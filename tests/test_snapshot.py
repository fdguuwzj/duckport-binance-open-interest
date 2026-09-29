from datetime import datetime, timezone
import unittest

import pandas as pd

from binance_open_interest.api import BinanceApiError
from binance_open_interest.snapshot import OpenInterestSnapshotFetcher, plan_snapshot


class FakeApi:
    def __init__(self):
        self.oi_calls = []

    async def exchange_info(self):
        return {
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
                {"symbol": "ETHUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
                {"symbol": "ETHUSDC", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDC"},
            ]
        }

    async def premium_index(self):
        return [
            {"symbol": "BTCUSDT", "markPrice": "100"},
            {"symbol": "ETHUSDT", "markPrice": "10"},
        ]

    async def open_interest(self, symbol):
        self.oi_calls.append(symbol)
        if symbol == "ETHUSDT":
            raise BinanceApiError(500, "down")
        return {"symbol": symbol, "openInterest": "3", "time": 1}


class SnapshotPlanTests(unittest.TestCase):
    def test_waits_until_delay_inside_the_current_minute(self):
        action, when = plan_snapshot(
            datetime(2026, 1, 1, 10, 1, 2, tzinfo=timezone.utc),
            None,
            5,
        )
        self.assertEqual(action, "sleep")
        self.assertEqual(when, pd.Timestamp("2026-01-01 10:01:05", tz="UTC"))

    def test_captures_current_minute_without_replaying_gaps(self):
        action, bar = plan_snapshot(
            datetime(2026, 1, 1, 10, 5, 10, tzinfo=timezone.utc),
            pd.Timestamp("2026-01-01 10:01:00", tz="UTC"),
            5,
        )
        self.assertEqual(action, "capture")
        self.assertEqual(bar, pd.Timestamp("2026-01-01 10:05:00", tz="UTC"))

    def test_sleeps_after_the_current_minute_is_stored(self):
        action, when = plan_snapshot(
            datetime(2026, 1, 1, 10, 1, 20, tzinfo=timezone.utc),
            pd.Timestamp("2026-01-01 10:01:00", tz="UTC"),
            5,
        )
        self.assertEqual(action, "sleep")
        self.assertEqual(when, pd.Timestamp("2026-01-01 10:02:05", tz="UTC"))


class SnapshotFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_once_keeps_successes_and_reports_failures(self):
        api = FakeApi()
        fetcher = OpenInterestSnapshotFetcher(api, concurrency=2, rps=1000, retry_times=0)
        self.assertEqual(await fetcher.trading_symbols(), ["BTCUSDT", "ETHUSDT"])
        frame, failed = await fetcher.fetch_once(
            ["BTCUSDT", "ETHUSDT"],
            pd.Timestamp("2026-01-01 10:31:40", tz="UTC"),
            await fetcher.mark_prices(),
        )
        self.assertEqual(list(frame["symbol"]), ["BTCUSDT"])
        self.assertEqual(frame.iloc[0]["open_interest_value"], frame.iloc[0]["open_interest"] * 100)
        self.assertEqual(failed, ["ETHUSDT"])
        self.assertEqual(frame.iloc[0]["open_time"], pd.Timestamp("2026-01-01 10:31:00", tz="UTC"))


if __name__ == "__main__":
    unittest.main()
