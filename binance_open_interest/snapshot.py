"""Live 1-minute open-interest snapshots.

Binance does not publish 1-minute OI history. This loop samples
``GET /fapi/v1/openInterest`` once per minute and labels every symbol with
that minute. A missed minute cannot be replayed.
"""

from __future__ import annotations

import asyncio
import gc
import os
import signal
import sys
import threading

import aiohttp
import pandas as pd

from binance_open_interest.api import BinanceApiError, BinanceOpenInterestRestApi
from binance_open_interest.config import (
    DUCKPORT_ADDR,
    DUCKPORT_SCHEMA,
    HTTP_TIMEOUT_SECONDS,
    OPEN_INTEREST_1M_TABLE,
    RETRY_BASE_SECONDS,
    SNAPSHOT_CONCURRENCY,
    SNAPSHOT_DELAY_SECONDS,
    SNAPSHOT_RETRY_TIMES,
    SNAPSHOT_RPS,
)
from binance_open_interest.data_jobs import OpenInterestFetcher, RequestPacer
from binance_open_interest.duckport_client import DuckportClient
from binance_open_interest.log import logger
from binance_open_interest.model import build_snapshot_frame


def utc_timestamp(value) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def plan_snapshot(now, duck_time, delay_seconds: int = SNAPSHOT_DELAY_SECONDS):
    """Decide whether to sample the current minute or wait.

    Returns ``("capture", bar)`` or ``("sleep", until)``. Gaps between
    ``duck_time`` and the current minute are not replayed.
    """
    current = utc_timestamp(now)
    bar = current.floor("1min")
    ready = bar + pd.Timedelta(seconds=delay_seconds)
    if current < ready:
        return ("sleep", ready)
    if duck_time is not None and utc_timestamp(duck_time) >= bar:
        return ("sleep", bar + pd.Timedelta(minutes=1, seconds=delay_seconds))
    return ("capture", bar)


class OpenInterestSnapshotFetcher:
    def __init__(
        self,
        api: BinanceOpenInterestRestApi,
        *,
        concurrency: int = SNAPSHOT_CONCURRENCY,
        rps: float = SNAPSHOT_RPS,
        retry_times: int = SNAPSHOT_RETRY_TIMES,
    ):
        self.api = api
        self.retry_times = retry_times
        self._semaphore = asyncio.Semaphore(concurrency)
        self._pacer = RequestPacer(1.0 / rps)

    async def trading_symbols(self) -> list[str]:
        return await OpenInterestFetcher(self.api).trading_symbols()

    async def mark_prices(self) -> dict[str, str]:
        payload = await self.api.premium_index()
        prices = {}
        for item in payload:
            symbol = item.get("symbol")
            mark = item.get("markPrice")
            if symbol and mark not in (None, ""):
                prices[symbol] = str(mark)
        return prices

    async def _open_interest(self, symbol: str) -> dict:
        last_error = None
        for attempt in range(self.retry_times + 1):
            try:
                async with self._semaphore:
                    await self._pacer.wait()
                    return await self.api.open_interest(symbol)
            except Exception as exc:
                last_error = exc
                if attempt >= self.retry_times:
                    break
                delay = min(10.0, RETRY_BASE_SECONDS * (2**attempt))
                if isinstance(exc, BinanceApiError) and exc.retry_after:
                    try:
                        delay = max(delay, float(exc.retry_after))
                    except ValueError:
                        pass
                if isinstance(exc, BinanceApiError) and exc.status in (418, 429):
                    await self._pacer.defer(delay)
                await asyncio.sleep(delay)
        raise RuntimeError(f"OI snapshot failed for {symbol}") from last_error

    async def fetch_once(
        self,
        symbols: list[str],
        open_time: pd.Timestamp,
        mark_prices: dict[str, str],
    ) -> tuple[pd.DataFrame, list[str]]:
        async def fetch_one(symbol: str):
            try:
                payload = await self._open_interest(symbol)
            except Exception as exc:
                return symbol, exc
            return symbol, payload

        results = await asyncio.gather(*(fetch_one(symbol) for symbol in symbols))
        rows = []
        failed = []
        for symbol, payload in results:
            if isinstance(payload, Exception):
                failed.append(symbol)
                continue
            mark = mark_prices.get(symbol) or mark_prices.get(payload.get("symbol"))
            if mark is None or payload.get("openInterest") in (None, ""):
                failed.append(symbol)
                continue
            rows.append(
                {
                    "symbol": payload.get("symbol") or symbol,
                    "open_interest": payload.get("openInterest"),
                    "mark_price": mark,
                }
            )
        return build_snapshot_frame(open_time, rows), failed


class OpenInterestSnapshotJobs:
    def __init__(self, client: DuckportClient, *, auto_start: bool = True):
        self.client = client
        self.duck_time = client.read_duck_time()
        logger.info(f"OI 1m duck_time loaded: {self.duck_time}")
        logger.info("OI 1m snapshots start at the current minute; missed minutes are not backfilled")
        if auto_start:
            threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self) -> None:
        asyncio.run(self.run_forever())

    async def run_forever(self) -> None:
        while True:
            action, when = plan_snapshot(pd.Timestamp.now(tz="UTC"), self.duck_time)
            if action == "sleep":
                delay = max(0.0, (when - pd.Timestamp.now(tz="UTC")).total_seconds())
                logger.info(f"Next OI 1m snapshot at {when.isoformat()}")
                await asyncio.sleep(delay)
                continue
            try:
                await self.capture(when)
            except Exception as exc:
                logger.error(f"OI 1m snapshot failed: {exc}")
                gc.collect()
                await asyncio.sleep(2)

    async def capture(self, bar: pd.Timestamp) -> bool:
        deadline = utc_timestamp(bar) + pd.Timedelta(seconds=50)
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
        connector = aiohttp.TCPConnector(ssl=False)
        started = pd.Timestamp.now(tz="UTC")
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            fetcher = OpenInterestSnapshotFetcher(BinanceOpenInterestRestApi(session))
            symbols = await fetcher.trading_symbols()
            if not symbols:
                raise RuntimeError("Binance exchangeInfo returned no trading USDT perpetual symbols")
            estimated = len(symbols) / SNAPSHOT_RPS
            if estimated > 40:
                logger.warning(
                    f"OI 1m sweep of {len(symbols)} symbols at {SNAPSHOT_RPS} rps "
                    f"may exceed one minute ({estimated:.0f}s)"
                )
            mark_prices = await fetcher.mark_prices()
            pending = list(symbols)
            frames = []
            while pending and pd.Timestamp.now(tz="UTC") < deadline:
                frame, failed = await fetcher.fetch_once(pending, bar, mark_prices)
                if not frame.empty:
                    frames.append(frame)
                pending = failed
                if pending and pd.Timestamp.now(tz="UTC") < deadline:
                    logger.warning(
                        f"OI 1m retrying {len(pending)} symbols: {', '.join(pending[:8])}"
                    )
                    await asyncio.sleep(1)
            if pending:
                logger.warning(f"OI 1m missing {len(pending)} symbols for {bar}")
            if not frames:
                raise RuntimeError(f"OI 1m snapshot produced no rows for {bar}")
            frame = pd.concat(frames, ignore_index=True)
            self.client.write_open_interest(frame, bar)
            self.duck_time = utc_timestamp(bar)
            elapsed = (pd.Timestamp.now(tz="UTC") - started).total_seconds()
            logger.info(
                f"OI 1m snapshot complete: {bar}, rows={len(frame):,}, "
                f"failed={len(pending)}, elapsed={elapsed:.1f}s"
            )
            return True


def main() -> None:
    os.environ.setdefault("WATERMARK_MAX_LAG_SECONDS", "180")
    logger.info("Binance 1m open-interest ingestor starting")
    client = DuckportClient(
        addr=DUCKPORT_ADDR,
        schema=DUCKPORT_SCHEMA,
        table=os.getenv("OPEN_INTEREST_TABLE", OPEN_INTEREST_1M_TABLE),
    )

    def shutdown(signum, frame):
        logger.warning(f"Received signal {signum}; shutting down")
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        info = client.ping()
        logger.info(f"duckport-rs server: {info}")
    except Exception as exc:
        logger.error(f"Cannot reach duckport-rs at {DUCKPORT_ADDR}: {exc}")
        sys.exit(1)

    client.init_schema(pd.Timestamp.now(tz="UTC").floor("1min"))
    OpenInterestSnapshotJobs(client)
    try:
        signal.pause()
    except AttributeError:
        threading.Event().wait()


if __name__ == "__main__":
    main()
