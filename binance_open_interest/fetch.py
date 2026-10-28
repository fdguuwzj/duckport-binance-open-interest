"""Binance request helpers shared by the 5m and 1m collectors."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass

import pandas as pd

from binance_open_interest.api import BinanceApiError, BinanceOpenInterestRestApi
from binance_open_interest.config import (
    CONCURRENCY,
    HIST_REQUESTS_PER_5M,
    OPEN_INTEREST_INTERVAL,
    OPEN_INTEREST_INTERVAL_MINUTES,
    RETRY_BASE_SECONDS,
    RETRY_TIMES,
    SNAPSHOT_CONCURRENCY,
    SNAPSHOT_RETRY_TIMES,
    SNAPSHOT_RPS,
)
from binance_open_interest.log import logger
from binance_open_interest.model import build_snapshot_frame

PERIOD = pd.Timedelta(minutes=OPEN_INTEREST_INTERVAL_MINUTES)
MAX_POINTS_PER_REQUEST = 500
# openInterestHist rejects startTime older than 30 days ("startTime is invalid").
HISTORY_MAX_AGE = pd.Timedelta(days=30) - pd.Timedelta(hours=1)


async def trading_symbols(api: BinanceOpenInterestRestApi) -> list[str]:
    payload = await api.exchange_info()
    return sorted(
        item["symbol"]
        for item in payload.get("symbols", [])
        if item.get("status") == "TRADING"
        and item.get("contractType") in {"PERPETUAL", "TRADIFI_PERPETUAL"}
        and item.get("quoteAsset") == "USDT"
    )


def retry_delay(exc: BaseException, attempt: int, cap: float) -> float:
    delay = min(cap, RETRY_BASE_SECONDS * (2**attempt))
    if isinstance(exc, BinanceApiError) and exc.retry_after:
        try:
            delay = max(delay, float(exc.retry_after))
        except ValueError:
            pass
    return delay


def is_throttle(exc: BaseException) -> bool:
    return isinstance(exc, BinanceApiError) and exc.status in (418, 429)


def is_rejected(exc: BaseException) -> bool:
    """A 4xx other than throttling: retrying the same request cannot help."""
    return isinstance(exc, BinanceApiError) and 400 <= exc.status < 500 and not is_throttle(exc)


class RequestPacer:
    """Global request spacer shared by all concurrent symbol fetches."""

    def __init__(self, interval_seconds: float):
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


class WindowBudget:
    """Sliding-window request cap.

    ``/futures/data/openInterestHist`` allows 1000 requests per 5 minutes per
    IP. Requests may burst as long as no rolling window exceeds the cap, so a
    full-market sweep finishes in seconds instead of being spread evenly.
    """

    def __init__(
        self,
        max_requests: int = HIST_REQUESTS_PER_5M,
        window_seconds: float = 300.0,
        clock=time.monotonic,
    ):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._clock = clock
        self._sent: deque[float] = deque()
        self._paused_until = 0.0
        self._lock = asyncio.Lock()

    def _evict(self, now: float) -> None:
        while self._sent and now - self._sent[0] >= self.window_seconds:
            self._sent.popleft()

    def try_acquire(self) -> float:
        """Take one slot and return 0, or return how long to wait."""
        now = self._clock()
        self._evict(now)
        if now < self._paused_until:
            return self._paused_until - now
        if len(self._sent) >= self.max_requests:
            return self.window_seconds - (now - self._sent[0])
        self._sent.append(now)
        return 0.0

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                wait = self.try_acquire()
            if wait <= 0:
                return
            await asyncio.sleep(max(0.05, wait))

    def defer(self, seconds: float) -> None:
        self._paused_until = max(self._paused_until, self._clock() + seconds)


def iter_request_windows(
    first_close: pd.Timestamp,
    last_close: pd.Timestamp,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Split [first_close, last_close] into openInterestHist windows of 500 points."""
    start = pd.to_datetime(first_close, utc=True).ceil(f"{OPEN_INTEREST_INTERVAL_MINUTES}min")
    end = pd.to_datetime(last_close, utc=True).floor(f"{OPEN_INTEREST_INTERVAL_MINUTES}min")
    windows = []
    max_span = PERIOD * (MAX_POINTS_PER_REQUEST - 1)
    while start <= end:
        stop = min(end, start + max_span)
        windows.append((start, stop))
        start = stop + PERIOD
    return windows


@dataclass
class FetchStats:
    requests: int = 0
    retries: int = 0


class HistoryFetcher:
    """``/futures/data/openInterestHist`` (5m points, latest 30 days only)."""

    def __init__(
        self,
        api: BinanceOpenInterestRestApi,
        *,
        budget: WindowBudget | None = None,
        concurrency: int = CONCURRENCY,
        retry_times: int = RETRY_TIMES,
    ):
        self.api = api
        self.budget = budget or WindowBudget()
        self.retry_times = retry_times
        self._semaphore = asyncio.Semaphore(concurrency)
        self.stats = FetchStats()

    async def _request(
        self,
        symbol: str,
        first_close: pd.Timestamp,
        last_close: pd.Timestamp,
    ) -> list[dict]:
        last_error = None
        for attempt in range(self.retry_times + 1):
            try:
                async with self._semaphore:
                    await self.budget.acquire()
                    self.stats.requests += 1
                    return await self.api.open_interest_history(
                        symbol=symbol,
                        period=OPEN_INTEREST_INTERVAL,
                        startTime=int(first_close.timestamp() * 1000),
                        endTime=int(last_close.timestamp() * 1000),
                        limit=MAX_POINTS_PER_REQUEST,
                    )
            except Exception as exc:
                last_error = exc
                if attempt >= self.retry_times or is_rejected(exc):
                    break
                self.stats.retries += 1
                delay = retry_delay(exc, attempt, 60.0)
                if is_throttle(exc):
                    self.budget.defer(delay)
                logger.warning(
                    f"OI history request failed for {symbol} ({exc}); retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)
        raise RuntimeError(f"OI history failed for {symbol}") from last_error

    async def fetch(
        self,
        symbol: str,
        first_close: pd.Timestamp,
        last_close: pd.Timestamp,
        *,
        now: pd.Timestamp | None = None,
    ) -> list[dict]:
        """All published points whose timestamp lies in [first_close, last_close].

        Points older than Binance keeps are skipped, not requested.
        """
        now = pd.to_datetime(now or pd.Timestamp.now(tz="UTC"), utc=True)
        first_close = max(pd.to_datetime(first_close, utc=True), now - HISTORY_MAX_AGE)
        rows: list[dict] = []
        for window_start, window_end in iter_request_windows(first_close, last_close):
            rows.extend(await self._request(symbol, window_start, window_end))
        return rows


class SnapshotFetcher:
    """Live ``/fapi/v1/openInterest`` sweeps priced with ``premiumIndex`` marks."""

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
        return await trading_symbols(self.api)

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
                delay = retry_delay(exc, attempt, 10.0)
                if is_throttle(exc):
                    await self._pacer.defer(delay)
                await asyncio.sleep(delay)
        raise RuntimeError(f"OI snapshot failed for {symbol}") from last_error

    async def fetch_once(
        self,
        symbols: list[str],
        open_time: pd.Timestamp,
        mark_prices: dict[str, str],
    ) -> tuple[pd.DataFrame, list[str]]:
        """Sample every symbol once and label the rows ``open_time``.

        The response ``time`` is when open interest last changed, not when it
        was read, so any answer received after the close is the value at the
        close.
        """

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
