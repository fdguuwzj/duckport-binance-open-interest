from __future__ import annotations

import asyncio
import gc
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import aiohttp
import pandas as pd

from binance_open_interest.api import BinanceApiError, BinanceOpenInterestRestApi
from binance_open_interest.config import (
    BACKFILL_DAYS,
    CONCURRENCY,
    HTTP_TIMEOUT_SECONDS,
    OPEN_INTEREST_INTERVAL,
    OPEN_INTEREST_INTERVAL_MINUTES,
    POLL_DELAY_SECONDS,
    REQUEST_INTERVAL_SECONDS,
    RETRY_BASE_SECONDS,
    RETRY_TIMES,
)
from binance_open_interest.duckport_client import DuckportClient
from binance_open_interest.log import logger
from binance_open_interest.model import OPEN_INTEREST_COLUMNS, parse_open_interest_rows

PERIOD = pd.Timedelta(minutes=OPEN_INTEREST_INTERVAL_MINUTES)
MAX_POINTS_PER_REQUEST = 500


def utc_timestamp(value) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True)


def latest_complete_time(now=None) -> pd.Timestamp:
    timestamp = utc_timestamp(now or pd.Timestamp.now(tz="UTC"))
    delayed = timestamp - pd.Timedelta(seconds=POLL_DELAY_SECONDS)
    return delayed.floor(f"{OPEN_INTEREST_INTERVAL_MINUTES}min")


def initial_start_time(now=None) -> pd.Timestamp:
    end = latest_complete_time(now)
    return (end - pd.Timedelta(days=BACKFILL_DAYS)).ceil(
        f"{OPEN_INTEREST_INTERVAL_MINUTES}min"
    )


def iter_request_windows(
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    start = utc_timestamp(start_time).ceil(f"{OPEN_INTEREST_INTERVAL_MINUTES}min")
    end = utc_timestamp(end_time).floor(f"{OPEN_INTEREST_INTERVAL_MINUTES}min")
    windows = []
    max_span = PERIOD * (MAX_POINTS_PER_REQUEST - 1)
    while start <= end:
        stop = min(end, start + max_span)
        windows.append((start, stop))
        start = stop + PERIOD
    return windows


def next_poll_time(now: datetime, delay_seconds: int = POLL_DELAY_SECONDS) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    timestamp = pd.Timestamp(now).tz_convert("UTC")
    boundary = timestamp.floor(f"{OPEN_INTEREST_INTERVAL_MINUTES}min")
    candidate = boundary + pd.Timedelta(seconds=max(0, delay_seconds))
    if candidate <= timestamp:
        candidate += PERIOD
    return candidate.to_pydatetime()


class RequestPacer:
    """Global request spacer shared by all concurrent symbol fetches."""

    def __init__(self, interval_seconds: float = REQUEST_INTERVAL_SECONDS):
        self.interval_seconds = interval_seconds
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next_allowed - now)
            self._next_allowed = max(now, self._next_allowed) + self.interval_seconds
        if delay:
            await asyncio.sleep(delay)

    async def defer(self, seconds: float) -> None:
        """Pause all subsequent requests after a server-side throttle response."""
        async with self._lock:
            self._next_allowed = max(self._next_allowed, time.monotonic() + seconds)


@dataclass
class FetchStats:
    requests: int = 0
    retries: int = 0


class OpenInterestFetcher:
    def __init__(
        self,
        api: BinanceOpenInterestRestApi,
        *,
        concurrency: int = CONCURRENCY,
        pacer: RequestPacer | None = None,
    ):
        self.api = api
        self._semaphore = asyncio.Semaphore(concurrency)
        self._pacer = pacer or RequestPacer()
        self.stats = FetchStats()

    async def trading_symbols(self) -> list[str]:
        payload = await self.api.exchange_info()
        return sorted(
            item["symbol"]
            for item in payload.get("symbols", [])
            if item.get("status") == "TRADING"
            and item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
        )

    async def _history_request(
        self,
        symbol: str,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> list[dict]:
        last_error = None
        for attempt in range(RETRY_TIMES + 1):
            try:
                async with self._semaphore:
                    await self._pacer.wait()
                    self.stats.requests += 1
                    return await self.api.open_interest_history(
                        symbol=symbol,
                        period=OPEN_INTEREST_INTERVAL,
                        startTime=int(start_time.timestamp() * 1000),
                        endTime=int(end_time.timestamp() * 1000),
                        limit=MAX_POINTS_PER_REQUEST,
                    )
            except Exception as exc:
                last_error = exc
                if attempt >= RETRY_TIMES:
                    break
                self.stats.retries += 1
                delay = min(60.0, RETRY_BASE_SECONDS * (2**attempt))
                if isinstance(exc, BinanceApiError) and exc.retry_after:
                    try:
                        delay = max(delay, float(exc.retry_after))
                    except ValueError:
                        pass
                if isinstance(exc, BinanceApiError) and exc.status in (418, 429):
                    await self._pacer.defer(delay)
                logger.warning(
                    f"OI request failed for {symbol} ({exc}); retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)
        raise RuntimeError(f"OI history failed for {symbol}") from last_error

    async def fetch_window(
        self,
        symbols: list[str],
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> pd.DataFrame:
        async def fetch_one(symbol: str) -> pd.DataFrame:
            rows = await self._history_request(symbol, start_time, end_time)
            return parse_open_interest_rows(rows, requested_symbol=symbol)

        results = await asyncio.gather(
            *(fetch_one(symbol) for symbol in symbols),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise RuntimeError(
                f"OI window failed for {len(failures)} / {len(symbols)} symbols"
            ) from failures[0]
        frames = results
        nonempty = [frame for frame in frames if not frame.empty]
        if not nonempty:
            return pd.DataFrame(columns=OPEN_INTEREST_COLUMNS)
        frame = pd.concat(nonempty, ignore_index=True)
        mask = (frame["open_time"] >= start_time) & (frame["open_time"] <= end_time)
        return frame.loc[mask].reset_index(drop=True)


class OpenInterestDataJobs:
    def __init__(self, client: DuckportClient, *, auto_start: bool = True):
        self.client = client
        self.duck_time = client.read_duck_time()
        logger.info(f"OI duck_time loaded: {self.duck_time}")
        if auto_start:
            threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self) -> None:
        asyncio.run(self.run_forever())

    async def run_forever(self) -> None:
        while True:
            try:
                await self.catch_up()
                break
            except Exception as exc:
                logger.error(f"OI startup catch-up failed: {exc}; retrying in 30s")
                gc.collect()
                await asyncio.sleep(30)

        while True:
            target = next_poll_time(datetime.now(timezone.utc))
            delay = max(0.0, (target - datetime.now(timezone.utc)).total_seconds())
            logger.info(f"Next OI poll at {target.isoformat()}")
            await asyncio.sleep(delay)
            try:
                await self.catch_up()
            except Exception as exc:
                logger.error(f"OI poll failed: {exc}")
                gc.collect()

    async def catch_up(self, now=None) -> None:
        stored = self.client.read_duck_time()
        if stored is not None:
            self.duck_time = max(self.duck_time, stored) if self.duck_time is not None else stored

        end_time = latest_complete_time(now)
        start_time = (
            self.duck_time + PERIOD
            if self.duck_time is not None
            else initial_start_time(now)
        )
        if start_time > end_time:
            logger.info(f"OI already current through {self.duck_time}")
            return

        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            fetcher = OpenInterestFetcher(BinanceOpenInterestRestApi(session))
            symbols = await fetcher.trading_symbols()
            if not symbols:
                raise RuntimeError("Binance exchangeInfo returned no trading USDT perpetual symbols")
            logger.info(
                f"OI catch-up: symbols={len(symbols)}, {start_time} -> {end_time}"
            )
            for window_start, window_end in iter_request_windows(start_time, end_time):
                before = fetcher.stats.requests
                frame = await fetcher.fetch_window(symbols, window_start, window_end)
                self.client.write_open_interest(frame, window_end)
                self.duck_time = window_end
                logger.info(
                    f"OI window complete: {window_start} -> {window_end}, "
                    f"rows={len(frame):,}, requests={fetcher.stats.requests - before}, "
                    f"retries={fetcher.stats.retries}"
                )
