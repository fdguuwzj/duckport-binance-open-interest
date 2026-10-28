"""5-minute open interest with K-line labels.

Label T holds the open interest at the bar close C = T + 5m, the same way a
K-line labeled T closes at T + 5m.

* Live: right after every close C, sample ``/fapi/v1/openInterest`` for all
  trading symbols, write label C-5m and move the watermark in the same
  transaction (about C+20s for the full market).
* Repair: labels the live sweep could not sample (downtime, startup, failed
  symbols) come from ``/futures/data/openInterestHist`` once Binance has
  published them. Its point stamped C is stored under label C-5m. Binance's
  history has permanent holes for illiquid symbols; such labels stay missing.

While the startup gap is being repaired, live rows are written but the
watermark stays put, so ``duck_time`` never claims a range that is still
being filled. Repairs of single failed symbols land behind the watermark.
"""

from __future__ import annotations

import asyncio
import gc
import threading
from collections.abc import Iterable
from datetime import datetime, timezone

import aiohttp
import pandas as pd

from binance_open_interest.api import BinanceOpenInterestRestApi
from binance_open_interest.config import (
    BACKFILL_DAYS,
    BAR_CLOSE_DELAY_SECONDS,
    BAR_DEADLINE_SECONDS,
    HIST_PUBLISH_DELAY_SECONDS,
    HTTP_TIMEOUT_SECONDS,
    OPEN_INTEREST_INTERVAL_MINUTES,
    REPAIR_LOOKBACK_HOURS,
    REPAIR_MAX_ATTEMPTS,
)
from binance_open_interest.duckport_client import DuckportClient
from binance_open_interest.fetch import (
    PERIOD,
    HistoryFetcher,
    SnapshotFetcher,
    WindowBudget,
    trading_symbols,
)
from binance_open_interest.log import logger
from binance_open_interest.model import OPEN_INTEREST_COLUMNS, parse_open_interest_rows

FREQ = f"{OPEN_INTEREST_INTERVAL_MINUTES}min"
REPAIR_BATCH_SYMBOLS = 50


def utc_timestamp(value) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True)


def latest_close(now=None) -> pd.Timestamp:
    return utc_timestamp(now or pd.Timestamp.now(tz="UTC")).floor(FREQ)


def initial_start_time(now=None) -> pd.Timestamp:
    """First label of a fresh table: the oldest close history still serves, minus 5m."""
    oldest_close = (latest_close(now) - pd.Timedelta(days=BACKFILL_DAYS)).ceil(FREQ)
    return oldest_close - PERIOD


def label_range(first: pd.Timestamp, last: pd.Timestamp) -> list[pd.Timestamp]:
    if first > last:
        return []
    return list(pd.date_range(first, last, freq=FREQ))


def next_capture_time(now: datetime, delay_seconds: int = BAR_CLOSE_DELAY_SECONDS) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    timestamp = pd.Timestamp(now).tz_convert("UTC")
    candidate = timestamp.floor(FREQ) + pd.Timedelta(seconds=max(0, delay_seconds))
    if candidate <= timestamp:
        candidate += PERIOD
    return candidate.to_pydatetime()


class RepairQueue:
    """Missing ``(symbol, label)`` pairs waiting for openInterestHist."""

    def __init__(self, max_attempts: int = REPAIR_MAX_ATTEMPTS):
        self.max_attempts = max_attempts
        self._labels: dict[str, set[pd.Timestamp]] = {}
        self._attempts: dict[str, int] = {}

    def __len__(self) -> int:
        return sum(len(labels) for labels in self._labels.values())

    def add(self, symbol: str, labels: Iterable[pd.Timestamp]) -> None:
        labels = set(labels)
        if labels:
            self._labels.setdefault(symbol, set()).update(labels)

    def due(
        self,
        now: pd.Timestamp,
        publish_delay_seconds: int,
        *,
        after: pd.Timestamp | None = None,
    ) -> dict[str, list[pd.Timestamp]]:
        """Labels whose close plus the publish delay has passed (only > ``after``)."""
        latest_label = utc_timestamp(now) - pd.Timedelta(seconds=publish_delay_seconds) - PERIOD
        out = {}
        for symbol, labels in self._labels.items():
            ready = sorted(
                label
                for label in labels
                if label <= latest_label and (after is None or label > after)
            )
            if ready:
                out[symbol] = ready
        return out

    def next_due(self, publish_delay_seconds: int) -> pd.Timestamp | None:
        earliest = min((min(labels) for labels in self._labels.values()), default=None)
        if earliest is None:
            return None
        return earliest + PERIOD + pd.Timedelta(seconds=publish_delay_seconds)

    def has_after(self, label: pd.Timestamp | None) -> bool:
        if label is None:
            return bool(self._labels)
        return any(max(labels) > label for labels in self._labels.values())

    def resolve(self, symbol: str, labels: Iterable[pd.Timestamp]) -> None:
        remaining = self._labels.get(symbol)
        if remaining is None:
            return
        remaining.difference_update(labels)
        self._attempts.pop(symbol, None)
        if not remaining:
            del self._labels[symbol]

    def fail(self, symbol: str, labels: list[pd.Timestamp]) -> bool:
        """Count a failed fetch; drop the labels after too many. True if dropped."""
        attempts = self._attempts.get(symbol, 0) + 1
        self._attempts[symbol] = attempts
        if attempts < self.max_attempts:
            return False
        self.resolve(symbol, labels)
        return True


class OpenInterestDataJobs:
    def __init__(self, client: DuckportClient, *, auto_start: bool = True):
        self.client = client
        self.duck_time = client.read_duck_time()
        logger.info(f"OI duck_time loaded: {self.duck_time}")
        self.queue = RepairQueue()
        self.budget = WindowBudget()
        self.symbols: list[str] = []
        # Latest label every symbol has been written or queued for.
        self.covered_through: pd.Timestamp | None = None
        # The watermark stays here until every queued label after it is repaired.
        self.catchup_after: pd.Timestamp | None = self.duck_time
        self.caught_up = False
        self.last_live_label: pd.Timestamp | None = None
        if auto_start:
            threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self) -> None:
        asyncio.run(self.run_forever())

    @staticmethod
    def _session() -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
        return aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(ssl=False))

    async def run_forever(self) -> None:
        while True:
            try:
                async with self._session() as session:
                    symbols = await trading_symbols(BinanceOpenInterestRestApi(session))
                await self.plan_startup(symbols)
                break
            except Exception as exc:
                logger.error(f"OI startup planning failed: {exc}; retrying in 30s")
                gc.collect()
                await asyncio.sleep(30)
        await asyncio.gather(self.live_loop(), self.repair_loop())

    # ------------------------------------------------------------------ startup

    async def plan_startup(self, symbols: list[str], now=None) -> None:
        """Queue the gap after the watermark and holes inside the lookback window."""
        if not symbols:
            raise RuntimeError(
                "Binance exchangeInfo returned no trading USDT perpetual "
                "or TradFi perpetual symbols"
            )
        now = utc_timestamp(now or pd.Timestamp.now(tz="UTC"))
        self.symbols = list(symbols)
        last_label = latest_close(now) - PERIOD
        if self.duck_time is None:
            first_gap = initial_start_time(now)
            self.covered_through = last_label
        else:
            first_gap = self.duck_time + PERIOD
            self.covered_through = max(last_label, self.duck_time)
        gap = label_range(first_gap, last_label)
        for symbol in self.symbols:
            self.queue.add(symbol, gap)

        holes = 0
        if REPAIR_LOOKBACK_HOURS and self.duck_time is not None:
            window_start = max(
                (now - pd.Timedelta(hours=REPAIR_LOOKBACK_HOURS)).ceil(FREQ),
                initial_start_time(now),
            )
            if window_start <= self.duck_time:
                missing = await asyncio.to_thread(
                    self.client.missing_labels, self.symbols, window_start, self.duck_time
                )
                holes = len(missing)
                for symbol, labels in missing.groupby("symbol")["open_time"]:
                    self.queue.add(symbol, labels)
        self.caught_up = not self.queue.has_after(self.catchup_after)
        logger.info(
            f"OI startup: symbols={len(self.symbols)}, watermark={self.duck_time}, "
            f"gap labels={len(gap)} ({first_gap} -> {last_label}), "
            f"lookback holes={holes:,}, queued={len(self.queue):,}"
        )

    # --------------------------------------------------------------------- live

    async def live_loop(self) -> None:
        while True:
            target = next_capture_time(datetime.now(timezone.utc))
            delay = max(0.0, (target - datetime.now(timezone.utc)).total_seconds())
            await asyncio.sleep(delay)
            close = utc_timestamp(target).floor(FREQ)
            try:
                await self.capture(close)
            except Exception as exc:
                logger.error(f"OI live sweep failed for close {close}: {exc}")
                self._queue_skipped(close - PERIOD, include_last=True)
                gc.collect()

    def _queue_skipped(self, label: pd.Timestamp, *, include_last: bool = False) -> None:
        """Queue labels after ``covered_through`` up to ``label`` for every symbol."""
        first = self.covered_through + PERIOD if self.covered_through is not None else label
        last = label if include_last else label - PERIOD
        skipped = label_range(first, last)
        if skipped:
            logger.warning(f"OI live sweep skipped {len(skipped)} labels; queued for history")
            for symbol in self.symbols:
                self.queue.add(symbol, skipped)
        if self.covered_through is None or last > self.covered_through:
            self.covered_through = last

    async def capture(self, close: pd.Timestamp, *, fetcher=None, now_fn=None) -> pd.DataFrame:
        """Sample every symbol right after ``close`` and store label close-5m."""
        now_fn = now_fn or (lambda: pd.Timestamp.now(tz="UTC"))
        started = now_fn()
        if fetcher is None:
            async with self._session() as session:
                frame, pending = await self._sweep(
                    SnapshotFetcher(BinanceOpenInterestRestApi(session)), close, now_fn
                )
        else:
            frame, pending = await self._sweep(fetcher, close, now_fn)
        label = close - PERIOD
        await self._store_live(label, frame, pending)
        elapsed = (now_fn() - started).total_seconds()
        logger.info(
            f"OI live label {label} (close {close}): rows={len(frame):,}, "
            f"missing={len(pending)}, elapsed={elapsed:.1f}s, "
            f"watermark={'moved' if self.caught_up else 'held for catch-up'}"
        )
        return frame

    async def _sweep(self, fetcher, close: pd.Timestamp, now_fn):
        label = close - PERIOD
        deadline = close + pd.Timedelta(seconds=BAR_DEADLINE_SECONDS)
        symbols = await fetcher.trading_symbols()
        if not symbols:
            raise RuntimeError("Binance exchangeInfo returned no trading symbols")
        self.symbols = symbols
        marks = await fetcher.mark_prices()
        pending = list(symbols)
        frames = []
        while pending and now_fn() < deadline:
            frame, pending = await fetcher.fetch_once(pending, label, marks)
            if not frame.empty:
                frames.append(frame)
            if pending and now_fn() < deadline:
                logger.warning(f"OI live retrying {len(pending)} symbols: {', '.join(pending[:8])}")
                await asyncio.sleep(1)
        frame = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=OPEN_INTEREST_COLUMNS)
        )
        return frame, pending

    async def _store_live(self, label: pd.Timestamp, frame: pd.DataFrame, pending: list[str]) -> None:
        self._queue_skipped(label)
        for symbol in pending:
            self.queue.add(symbol, [label])
        if frame.empty:
            raise RuntimeError(f"OI live sweep produced no rows for {label}")
        through = label if self.caught_up else None
        await asyncio.to_thread(self.client.write_open_interest, frame, through)
        self.covered_through = max(self.covered_through, label)
        self.last_live_label = label
        if through is not None:
            self.duck_time = label

    # ------------------------------------------------------------------- repair

    async def repair_loop(self) -> None:
        while True:
            now = pd.Timestamp.now(tz="UTC")
            if self.caught_up:
                due = self.queue.due(now, HIST_PUBLISH_DELAY_SECONDS)
            else:
                # Older holes wait until the gap after the watermark is done,
                # so a long repair cannot hold the watermark back.
                due = self.queue.due(now, HIST_PUBLISH_DELAY_SECONDS, after=self.catchup_after)
                if not due:
                    await self.finish_catchup()
                    if not self.caught_up:
                        await asyncio.sleep(5)
                        continue
                    due = self.queue.due(now, HIST_PUBLISH_DELAY_SECONDS)
            if due:
                try:
                    await self.repair(due)
                except Exception as exc:
                    logger.error(f"OI history repair failed: {exc}")
                    gc.collect()
                    await asyncio.sleep(30)
                continue
            wake = self.queue.next_due(HIST_PUBLISH_DELAY_SECONDS)
            delay = 15.0 if wake is None else (wake - now).total_seconds()
            await asyncio.sleep(min(60.0, max(1.0, delay)))

    async def finish_catchup(self) -> None:
        """Move the watermark once nothing after the startup watermark is queued."""
        if self.caught_up or self.queue.has_after(self.catchup_after):
            return
        self.caught_up = True
        # Every label up to covered_through is now written or a permanent hole.
        through = self.covered_through
        if through is None or (self.duck_time is not None and through <= self.duck_time):
            return
        await asyncio.to_thread(self.client.replace_duck_time, through)
        self.duck_time = through
        logger.info(f"OI catch-up complete; watermark -> {through}")

    async def repair(self, due: dict[str, list[pd.Timestamp]], *, api=None, now=None) -> int:
        """Fetch history for due labels in symbol batches; return rows written."""
        written = 0
        symbols = sorted(due)
        logger.info(
            f"OI history repair: symbols={len(symbols)}, "
            f"labels={sum(len(v) for v in due.values()):,}, queued={len(self.queue):,}"
        )
        if api is None:
            async with self._session() as session:
                fetcher = HistoryFetcher(BinanceOpenInterestRestApi(session), budget=self.budget)
                written = await self._repair_batches(fetcher, symbols, due, now)
        else:
            fetcher = HistoryFetcher(api, budget=self.budget)
            written = await self._repair_batches(fetcher, symbols, due, now)
        logger.info(
            f"OI history repair done: rows={written:,}, requests={fetcher.stats.requests}, "
            f"retries={fetcher.stats.retries}, still queued={len(self.queue):,}"
        )
        return written

    async def _repair_batches(self, fetcher, symbols, due, now=None) -> int:
        written = 0
        for start in range(0, len(symbols), REPAIR_BATCH_SYMBOLS):
            batch = symbols[start : start + REPAIR_BATCH_SYMBOLS]
            frames = await asyncio.gather(
                *(self._repair_symbol(fetcher, symbol, due[symbol], now) for symbol in batch)
            )
            nonempty = [frame for frame in frames if frame is not None and not frame.empty]
            if nonempty:
                frame = pd.concat(nonempty, ignore_index=True)
                await asyncio.to_thread(self.client.write_open_interest, frame, None)
                written += len(frame)
            for symbol, frame in zip(batch, frames):
                if frame is not None:
                    # Labels history does not have are permanent upstream holes.
                    self.queue.resolve(symbol, due[symbol])
        return written

    async def _repair_symbol(
        self,
        fetcher: HistoryFetcher,
        symbol: str,
        labels: list[pd.Timestamp],
        now=None,
    ) -> pd.DataFrame | None:
        try:
            rows = await fetcher.fetch(symbol, labels[0] + PERIOD, labels[-1] + PERIOD, now=now)
        except Exception as exc:
            if self.queue.fail(symbol, labels):
                logger.error(f"OI history gave up on {symbol} ({len(labels)} labels): {exc}")
            return None
        frame = await asyncio.to_thread(
            parse_open_interest_rows, rows, requested_symbol=symbol, label_offset=-PERIOD
        )
        if frame.empty:
            return frame
        return frame[frame["open_time"].isin(labels)].reset_index(drop=True)
