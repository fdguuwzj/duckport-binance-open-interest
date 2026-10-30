from decimal import Decimal
import unittest

import pandas as pd

from binance_open_interest.model import build_snapshot_frame, parse_open_interest_rows


class ModelTests(unittest.TestCase):
    def test_parse_rows_preserves_decimal_values_and_deduplicates(self):
        rows = [
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "123.12345678",
                "sumOpenInterestValue": "456789.00000001",
                "CMCCirculatingSupply": "21000000",
                "timestamp": 1_600_000_200_000,
            },
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "124.00000001",
                "sumOpenInterestValue": "456790.00000001",
                "CMCCirculatingSupply": "21000000",
                "timestamp": 1_600_000_200_000,
            },
        ]
        frame = parse_open_interest_rows(rows)
        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["open_interest"], Decimal("124.00000001"))
        self.assertEqual(str(frame.iloc[0]["symbol"]), "BTCUSDT")
        self.assertEqual(frame.iloc[0]["open_time"].second, 0)

    def test_requested_symbol_fills_missing_response_symbol(self):
        frame = parse_open_interest_rows(
            [
                {
                    "sumOpenInterest": "1",
                    "sumOpenInterestValue": "2",
                    "timestamp": 1_600_000_200_000,
                }
            ],
            requested_symbol="ETHUSDT",
        )
        self.assertEqual(frame.iloc[0]["symbol"], "ETHUSDT")

    def test_missing_optional_supply_stays_null(self):
        frame = parse_open_interest_rows(
            [
                {
                    "sumOpenInterest": "1",
                    "sumOpenInterestValue": "2",
                    "CMCCirculatingSupply": None,
                    "timestamp": 1_600_000_200_000,
                }
            ],
            requested_symbol="ETHUSDT",
        )
        self.assertIsNone(frame.iloc[0]["cmc_circulating_supply"])

    def test_snapshot_frame_prices_notional_on_the_scheduled_minute(self):
        frame = build_snapshot_frame(
            pd.Timestamp("2026-01-01 10:31:40", tz="UTC"),
            [
                {"symbol": "BTCUSDT", "open_interest": "2.5", "mark_price": "100.2"},
                {"symbol": "ETHUSDT", "open_interest": "4", "mark_price": ""},
                {"symbol": "SOLUSDC", "open_interest": "1", "mark_price": "20"},
            ],
        )
        self.assertEqual(list(frame["symbol"]), ["BTCUSDT"])
        self.assertEqual(frame.iloc[0]["open_interest"], Decimal("2.5"))
        self.assertEqual(frame.iloc[0]["open_interest_value"], Decimal("250.500000000000000000"))
        self.assertIsNone(frame.iloc[0]["cmc_circulating_supply"])
        self.assertEqual(frame.iloc[0]["open_time"], pd.Timestamp("2026-01-01 10:31:00", tz="UTC"))

    def test_snapshot_frame_notional_above_1e10(self):
        # BTCUSDT peaked near 1.28e10 USDT on 2025-10-05. 11 integer digits + 18 decimals exceed the
        # default 28-digit decimal context; one such symbol used to fail the whole sweep.
        frame = build_snapshot_frame(
            pd.Timestamp("2025-10-05 12:00:01", tz="UTC"),
            [
                {"symbol": "BTCUSDT", "open_interest": "103512.123", "mark_price": "123456.12345678"},
                {"symbol": "ETHUSDT", "open_interest": "2382399.455", "mark_price": "2326.67"},
            ],
        )
        self.assertEqual(list(frame["symbol"]), ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(frame.iloc[0]["open_interest_value"], Decimal("12779205436.361396543940000000"))
        self.assertEqual(frame.iloc[1]["open_interest_value"], Decimal("5543057339.96485"))


if __name__ == "__main__":
    unittest.main()
