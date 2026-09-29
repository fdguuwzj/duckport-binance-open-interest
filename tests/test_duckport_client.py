from decimal import Decimal
import unittest

import pandas as pd

from binance_open_interest.duckport_client import DuckportClient


class DuckportClientTests(unittest.TestCase):
    def test_arrow_schema_uses_decimal_and_timestamp(self):
        frame = pd.DataFrame(
            [
                {
                    "open_time": pd.Timestamp("2026-01-01", tz="UTC"),
                    "symbol": "BTCUSDT",
                    "open_interest": Decimal("1.12345678"),
                    "open_interest_value": Decimal("2.12345678"),
                    "cmc_circulating_supply": None,
                }
            ]
        )
        table = DuckportClient._open_interest_arrow(frame)
        self.assertEqual(str(table.schema.field("open_interest").type), "decimal128(38, 18)")
        self.assertEqual(table.column("open_interest")[0].as_py(), Decimal("1.123456780000000000"))

    def test_init_schema_registers_watermark(self):
        client = DuckportClient.__new__(DuckportClient)
        client.schema = "data"
        client.ingestor = "oi-test"
        client.max_lag_seconds = 900
        captured = []
        client.execute_transaction = captured.append
        client.init_schema(pd.Timestamp("2026-01-01", tz="UTC"))
        sql = "\n".join(captured[0])
        self.assertIn("data.usdt_perp_open_interest_5m", sql)
        self.assertIn("PRIMARY KEY (open_time, symbol)", sql)
        self.assertIn("'open_time'", sql)

    def test_one_minute_table_uses_its_own_watermark_and_minute_floor(self):
        client = DuckportClient.__new__(DuckportClient)
        client.schema = "data"
        client.ingestor = "oi-1m"
        client.max_lag_seconds = 180
        client.table = "usdt_perp_open_interest_1m"
        sql = client._watermark_sql("2026-01-01 10:31:00")
        self.assertIn("usdt_perp_open_interest_1m", sql)
        self.assertNotIn("usdt_perp_open_interest_5m", sql)
        self.assertEqual(client._time_floor(), "1min")
        frame = pd.DataFrame(
            [
                {
                    "open_time": pd.Timestamp("2026-01-01 10:31:40", tz="UTC"),
                    "symbol": "BTCUSDT",
                    "open_interest": Decimal("1"),
                    "open_interest_value": Decimal("2"),
                    "cmc_circulating_supply": None,
                }
            ]
        )
        table = client._open_interest_arrow(frame, time_floor=client._time_floor())
        self.assertEqual(
            pd.Timestamp(table.column("open_time")[0].as_py(), tz="UTC"),
            pd.Timestamp("2026-01-01 10:31:00", tz="UTC"),
        )


if __name__ == "__main__":
    unittest.main()
