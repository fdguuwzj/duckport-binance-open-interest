from datetime import datetime, timezone
import unittest

import pandas as pd

from binance_open_interest.data_jobs import (
    MAX_POINTS_PER_REQUEST,
    OpenInterestFetcher,
    iter_request_windows,
    next_poll_time,
)


class FakeApi:
    async def exchange_info(self):
        return {
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
                {"symbol": "ETHUSDC", "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDC"},
                {"symbol": "OLDUSDT", "status": "SETTLING", "contractType": "PERPETUAL", "quoteAsset": "USDT"},
            ]
        }


class DataJobTests(unittest.IsolatedAsyncioTestCase):
    async def test_trading_symbols_filters_to_live_usdt_perpetuals(self):
        fetcher = OpenInterestFetcher(FakeApi())
        self.assertEqual(await fetcher.trading_symbols(), ["BTCUSDT"])

    def test_windows_never_exceed_binance_limit(self):
        start = pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
        end = start + pd.Timedelta(minutes=5 * 1200)
        windows = iter_request_windows(start, end)
        self.assertEqual(len(windows), 3)
        for window_start, window_end in windows:
            points = int((window_end - window_start) / pd.Timedelta(minutes=5)) + 1
            self.assertLessEqual(points, MAX_POINTS_PER_REQUEST)
        self.assertEqual(windows[0][0], start)
        self.assertEqual(windows[-1][1], end)

    def test_next_poll_uses_current_boundary_when_delay_not_elapsed(self):
        now = datetime(2026, 1, 1, 10, 5, 10, tzinfo=timezone.utc)
        self.assertEqual(
            next_poll_time(now, 30),
            datetime(2026, 1, 1, 10, 5, 30, tzinfo=timezone.utc),
        )

    def test_next_poll_advances_after_delay(self):
        now = datetime(2026, 1, 1, 10, 5, 31, tzinfo=timezone.utc)
        self.assertEqual(
            next_poll_time(now, 30),
            datetime(2026, 1, 1, 10, 10, 30, tzinfo=timezone.utc),
        )


if __name__ == "__main__":
    unittest.main()
