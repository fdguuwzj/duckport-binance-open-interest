from datetime import datetime, timezone
from decimal import Decimal
import unittest
from unittest import mock

import pandas as pd

from binance_open_interest.data_jobs import (
    OpenInterestDataJobs,
    RepairQueue,
    initial_start_time,
    label_range,
    next_capture_time,
)
from binance_open_interest.api import BinanceApiError
from binance_open_interest.fetch import (
    MAX_POINTS_PER_REQUEST,
    HistoryFetcher,
    WindowBudget,
    iter_request_windows,
    trading_symbols,
)


def ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC")


def ms(value: str) -> int:
    return int(ts(value).timestamp() * 1000)


class FakeApi:
    def __init__(self, history=None):
        self.history = history or {}
        self.history_calls = []

    async def exchange_info(self):
        return {
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
                {"symbol": "AAPLUSDT", "status": "TRADING", "contractType": "TRADIFI_PERPETUAL", "quoteAsset": "USDT"},
                {"symbol": "ETHUSDC", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDC"},
                {"symbol": "OLDUSDT", "status": "SETTLING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
                {"symbol": "OLDTRADEUSDT", "status": "SETTLING", "contractType": "TRADIFI_PERPETUAL", "quoteAsset": "USDT"},
                {"symbol": "BTCUSDT_260925", "status": "TRADING", "contractType": "CURRENT_QUARTER", "quoteAsset": "USDT"},
            ]
        }

    async def open_interest_history(self, **params):
        self.history_calls.append(params)
        symbol = params["symbol"]
        if isinstance(self.history.get(symbol), Exception):
            raise self.history[symbol]
        return [
            row
            for row in self.history.get(symbol, [])
            if params["startTime"] <= row["timestamp"] <= params["endTime"]
        ]


def hist_row(symbol: str, close: str, oi: str) -> dict:
    return {
        "symbol": symbol,
        "sumOpenInterest": oi,
        "sumOpenInterestValue": "10",
        "CMCCirculatingSupply": "1",
        "timestamp": ms(close),
    }


class FakeClient:
    def __init__(self, duck_time=None, missing=None):
        self.duck_time = duck_time
        self.missing = missing if missing is not None else pd.DataFrame(columns=["symbol", "open_time"])
        self.writes = []
        self.watermarks = []

    def read_duck_time(self):
        return self.duck_time

    def missing_labels(self, symbols, start, end):
        return self.missing

    def write_open_interest(self, frame, complete_through):
        self.writes.append((frame.copy(), complete_through))

    def replace_duck_time(self, timestamp):
        self.watermarks.append(timestamp)


class FakeSnapshotFetcher:
    """Each symbol answers with the outcomes in ``answers`` in turn (True = success)."""

    def __init__(self, answers: dict[str, list[bool]]):
        self.answers = {symbol: list(values) for symbol, values in answers.items()}

    async def trading_symbols(self):
        return sorted(self.answers)

    async def mark_prices(self):
        return {symbol: "2" for symbol in self.answers}

    async def fetch_once(self, symbols, open_time, mark_prices):
        from binance_open_interest.model import build_snapshot_frame

        rows, failed = [], []
        for symbol in symbols:
            ok = self.answers[symbol].pop(0) if self.answers[symbol] else False
            if not ok:
                failed.append(symbol)
                continue
            rows.append({"symbol": symbol, "open_interest": "5", "mark_price": mark_prices[symbol]})
        return build_snapshot_frame(open_time, rows), failed


class HelperTests(unittest.IsolatedAsyncioTestCase):
    async def test_trading_symbols_filters_to_live_usdt_perpetuals(self):
        self.assertEqual(await trading_symbols(FakeApi()), ["AAPLUSDT", "BTCUSDT"])

    def test_windows_never_exceed_binance_limit(self):
        start = ts("2026-01-01 00:00:00")
        end = start + pd.Timedelta(minutes=5 * 1200)
        windows = iter_request_windows(start, end)
        self.assertEqual(len(windows), 3)
        for window_start, window_end in windows:
            points = int((window_end - window_start) / pd.Timedelta(minutes=5)) + 1
            self.assertLessEqual(points, MAX_POINTS_PER_REQUEST)
        self.assertEqual(windows[0][0], start)
        self.assertEqual(windows[-1][1], end)

    def test_capture_waits_for_close_delay(self):
        now = datetime(2026, 1, 1, 10, 5, 0, 400000, tzinfo=timezone.utc)
        self.assertEqual(next_capture_time(now, 1), datetime(2026, 1, 1, 10, 5, 1, tzinfo=timezone.utc))

    def test_capture_moves_to_next_close_after_delay(self):
        now = datetime(2026, 1, 1, 10, 5, 2, tzinfo=timezone.utc)
        self.assertEqual(next_capture_time(now, 1), datetime(2026, 1, 1, 10, 10, 1, tzinfo=timezone.utc))

    def test_initial_start_is_one_bar_before_oldest_served_close(self):
        # Latest close 10:05; 30 days earlier the oldest close is 01-01 10:05.
        start = initial_start_time(ts("2026-01-31 10:07:00"))
        self.assertEqual(start, ts("2026-01-01 10:00:00"))


class WindowBudgetTests(unittest.TestCase):
    def test_bursts_up_to_cap_then_waits_for_oldest_to_expire(self):
        now = [100.0]
        budget = WindowBudget(max_requests=3, window_seconds=300, clock=lambda: now[0])
        self.assertEqual([budget.try_acquire() for _ in range(3)], [0.0, 0.0, 0.0])
        now[0] = 160.0
        self.assertAlmostEqual(budget.try_acquire(), 240.0)
        now[0] = 400.0
        self.assertEqual(budget.try_acquire(), 0.0)

    def test_throttle_pauses_every_request(self):
        now = [0.0]
        budget = WindowBudget(max_requests=10, clock=lambda: now[0])
        budget.defer(30)
        self.assertAlmostEqual(budget.try_acquire(), 30.0)
        now[0] = 31.0
        self.assertEqual(budget.try_acquire(), 0.0)


class RepairQueueTests(unittest.TestCase):
    def test_labels_become_due_after_close_plus_publish_delay(self):
        queue = RepairQueue()
        queue.add("BTCUSDT", [ts("2026-01-01 10:00"), ts("2026-01-01 10:05")])
        # Label 10:00 closes at 10:05 and is published by 10:09.
        self.assertEqual(queue.due(ts("2026-01-01 10:08:59"), 240), {})
        self.assertEqual(queue.due(ts("2026-01-01 10:09"), 240), {"BTCUSDT": [ts("2026-01-01 10:00")]})
        self.assertEqual(queue.next_due(240), ts("2026-01-01 10:09"))

    def test_due_can_be_limited_to_labels_after_the_watermark(self):
        queue = RepairQueue()
        queue.add("BTCUSDT", [ts("2026-01-01 08:00"), ts("2026-01-01 10:00")])
        self.assertEqual(
            queue.due(ts("2026-01-02"), 240, after=ts("2026-01-01 09:00")),
            {"BTCUSDT": [ts("2026-01-01 10:00")]},
        )

    def test_failures_drop_labels_after_max_attempts(self):
        queue = RepairQueue(max_attempts=2)
        labels = [ts("2026-01-01 10:00")]
        queue.add("BTCUSDT", labels)
        self.assertFalse(queue.fail("BTCUSDT", labels))
        self.assertTrue(queue.fail("BTCUSDT", labels))
        self.assertEqual(len(queue), 0)

    def test_has_after_ignores_older_holes(self):
        queue = RepairQueue()
        queue.add("BTCUSDT", [ts("2026-01-01 09:00")])
        self.assertFalse(queue.has_after(ts("2026-01-01 10:00")))
        self.assertTrue(queue.has_after(None))
        queue.add("ETHUSDT", [ts("2026-01-01 10:05")])
        self.assertTrue(queue.has_after(ts("2026-01-01 10:00")))


class StartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_gap_after_watermark_and_lookback_holes_are_queued(self):
        missing = pd.DataFrame({"symbol": ["BTCUSDT"], "open_time": [ts("2026-01-31 08:00")]})
        jobs = OpenInterestDataJobs(
            FakeClient(duck_time=ts("2026-01-31 09:50"), missing=missing), auto_start=False
        )
        await jobs.plan_startup(["BTCUSDT", "ETHUSDT"], now=ts("2026-01-31 10:07"))
        # Latest close 10:05 -> latest label 10:00; gap is 09:55 and 10:00.
        due = jobs.queue.due(ts("2026-02-01"), 240)
        self.assertEqual(due["ETHUSDT"], [ts("2026-01-31 09:55"), ts("2026-01-31 10:00")])
        self.assertEqual(
            due["BTCUSDT"],
            [ts("2026-01-31 08:00"), ts("2026-01-31 09:55"), ts("2026-01-31 10:00")],
        )
        self.assertEqual(jobs.covered_through, ts("2026-01-31 10:00"))
        self.assertFalse(jobs.caught_up)

    async def test_current_watermark_with_only_old_holes_is_caught_up(self):
        missing = pd.DataFrame({"symbol": ["BTCUSDT"], "open_time": [ts("2026-01-31 08:00")]})
        jobs = OpenInterestDataJobs(
            FakeClient(duck_time=ts("2026-01-31 10:00"), missing=missing), auto_start=False
        )
        await jobs.plan_startup(["BTCUSDT"], now=ts("2026-01-31 10:07"))
        self.assertTrue(jobs.caught_up)
        self.assertEqual(len(jobs.queue), 1)


class LiveCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_sample_is_labeled_one_bar_earlier_and_moves_watermark(self):
        client = FakeClient(duck_time=ts("2026-01-01 10:00"))
        jobs = OpenInterestDataJobs(client, auto_start=False)
        await jobs.plan_startup(["BTCUSDT"], now=ts("2026-01-01 10:07"))
        self.assertTrue(jobs.caught_up)
        close = ts("2026-01-01 10:10")
        fetcher = FakeSnapshotFetcher({"BTCUSDT": [True]})
        frame = await jobs.capture(close, fetcher=fetcher, now_fn=lambda: ts("2026-01-01 10:10:02"))
        self.assertEqual(list(frame["open_time"]), [ts("2026-01-01 10:05")])
        self.assertEqual(frame.iloc[0]["open_interest_value"], Decimal("10"))
        self.assertEqual(client.writes[-1][1], ts("2026-01-01 10:05"))
        self.assertEqual(jobs.duck_time, ts("2026-01-01 10:05"))

    async def test_failed_symbols_are_retried_then_go_to_history(self):
        client = FakeClient(duck_time=ts("2026-01-01 10:00"))
        jobs = OpenInterestDataJobs(client, auto_start=False)
        await jobs.plan_startup(["BTCUSDT", "ETHUSDT"], now=ts("2026-01-01 10:07"))
        clock = iter([ts("2026-01-01 10:10:01")] * 4 + [ts("2026-01-01 10:11")] * 10)
        fetcher = FakeSnapshotFetcher(
            {
                # BTCUSDT fails once and succeeds on retry; ETHUSDT never answers.
                "BTCUSDT": [False, True],
                "ETHUSDT": [],
            }
        )
        frame = await jobs.capture(ts("2026-01-01 10:10"), fetcher=fetcher, now_fn=lambda: next(clock))
        self.assertEqual(list(frame["symbol"]), ["BTCUSDT"])
        self.assertEqual(jobs.queue.due(ts("2026-01-02"), 240), {"ETHUSDT": [ts("2026-01-01 10:05")]})

    async def test_watermark_holds_while_startup_gap_is_repaired(self):
        client = FakeClient(duck_time=ts("2026-01-01 09:00"))
        jobs = OpenInterestDataJobs(client, auto_start=False)
        await jobs.plan_startup(["BTCUSDT"], now=ts("2026-01-01 10:07"))
        fetcher = FakeSnapshotFetcher({"BTCUSDT": [True]})
        await jobs.capture(ts("2026-01-01 10:10"), fetcher=fetcher, now_fn=lambda: ts("2026-01-01 10:10:02"))
        self.assertIsNone(client.writes[-1][1])
        self.assertEqual(jobs.duck_time, ts("2026-01-01 09:00"))

    async def test_missed_closes_between_sweeps_are_queued(self):
        client = FakeClient(duck_time=ts("2026-01-01 10:00"))
        jobs = OpenInterestDataJobs(client, auto_start=False)
        await jobs.plan_startup(["BTCUSDT"], now=ts("2026-01-01 10:07"))
        fetcher = FakeSnapshotFetcher({"BTCUSDT": [True]})
        await jobs.capture(ts("2026-01-01 10:20"), fetcher=fetcher, now_fn=lambda: ts("2026-01-01 10:20:02"))
        self.assertEqual(
            jobs.queue.due(ts("2026-01-02"), 240)["BTCUSDT"],
            [ts("2026-01-01 10:05"), ts("2026-01-01 10:10")],
        )


class RepairTests(unittest.IsolatedAsyncioTestCase):
    async def test_history_point_at_close_is_stored_one_bar_earlier(self):
        client = FakeClient(duck_time=ts("2026-01-01 10:00"))
        jobs = OpenInterestDataJobs(client, auto_start=False)
        # No point at close 10:00, so label 09:55 is a permanent upstream hole.
        api = FakeApi(
            {
                "BTCUSDT": [
                    hist_row("BTCUSDT", "2026-01-01 09:55", "1"),
                    hist_row("BTCUSDT", "2026-01-01 10:05", "3"),
                ]
            }
        )
        jobs.queue.add("BTCUSDT", [ts("2026-01-01 09:50"), ts("2026-01-01 09:55"), ts("2026-01-01 10:00")])
        due = jobs.queue.due(ts("2026-01-02"), 240)
        written = await jobs.repair(due, api=api, now=ts("2026-01-01 11:00"))
        frame = client.writes[-1][0]
        self.assertEqual(written, 2)
        self.assertEqual(
            dict(zip(frame["open_time"], frame["open_interest"])),
            {ts("2026-01-01 09:50"): Decimal("1"), ts("2026-01-01 10:00"): Decimal("3")},
        )
        self.assertIsNone(client.writes[-1][1])
        self.assertEqual(api.history_calls[0]["startTime"], ms("2026-01-01 09:55"))
        self.assertEqual(api.history_calls[0]["endTime"], ms("2026-01-01 10:05"))
        self.assertEqual(len(jobs.queue), 0)

    async def test_points_older_than_binance_keeps_are_not_requested(self):
        api = FakeApi({"BTCUSDT": [hist_row("BTCUSDT", "2026-01-31 10:00", "7")]})
        fetcher = HistoryFetcher(api, budget=WindowBudget(), retry_times=0)
        rows = await fetcher.fetch(
            "BTCUSDT", ts("2025-12-01 00:00"), ts("2026-01-31 10:00"), now=ts("2026-01-31 10:10")
        )
        self.assertEqual(len(rows), 1)
        first_start = min(call["startTime"] for call in api.history_calls)
        self.assertGreaterEqual(first_start, ms("2026-01-01 11:10"))

    async def test_rejected_request_is_not_retried(self):
        api = FakeApi({"BTCUSDT": BinanceApiError(400, '{"msg":"parameter \'startTime\' is invalid."}')})
        fetcher = HistoryFetcher(api, budget=WindowBudget(), retry_times=5)
        with self.assertRaises(RuntimeError):
            await fetcher.fetch("BTCUSDT", ts("2026-01-31 09:00"), ts("2026-01-31 10:00"), now=ts("2026-01-31 10:10"))
        self.assertEqual(len(api.history_calls), 1)

    async def test_failed_symbol_stays_queued(self):
        client = FakeClient(duck_time=ts("2026-01-01 10:00"))
        jobs = OpenInterestDataJobs(client, auto_start=False)
        api = FakeApi({"BTCUSDT": RuntimeError("down")})
        jobs.queue.add("BTCUSDT", [ts("2026-01-01 09:50")])
        with mock.patch("binance_open_interest.fetch.asyncio.sleep", new=mock.AsyncMock()):
            written = await jobs.repair(
                jobs.queue.due(ts("2026-01-02"), 240), api=api, now=ts("2026-01-01 11:00")
            )
        self.assertEqual(written, 0)
        self.assertEqual(len(jobs.queue), 1)

    async def test_catchup_moves_watermark_to_covered_label(self):
        client = FakeClient(duck_time=ts("2026-01-01 09:50"))
        jobs = OpenInterestDataJobs(client, auto_start=False)
        await jobs.plan_startup(["BTCUSDT"], now=ts("2026-01-01 10:07"))
        await jobs.finish_catchup()
        self.assertEqual(client.watermarks, [])
        jobs.queue.resolve("BTCUSDT", label_range(ts("2026-01-01 09:55"), ts("2026-01-01 10:00")))
        await jobs.finish_catchup()
        self.assertTrue(jobs.caught_up)
        self.assertEqual(client.watermarks, [ts("2026-01-01 10:00")])
        self.assertEqual(jobs.duck_time, ts("2026-01-01 10:00"))


if __name__ == "__main__":
    unittest.main()
