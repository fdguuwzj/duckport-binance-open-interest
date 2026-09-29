from __future__ import annotations

import json
import math
import os
import threading
from decimal import Decimal
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from binance_open_interest.config import (
    DUCKPORT_APPEND_MAX_MESSAGE_BYTES,
    OPEN_INTEREST_TABLE,
)
from binance_open_interest.log import logger
from binance_open_interest.model import OPEN_INTEREST_COLUMNS, normalize_open_interest_frame


class DuckportClient:
    OPEN_INTEREST_COL_DEFS = [
        "open_time TIMESTAMP",
        "symbol VARCHAR",
        "open_interest DECIMAL(38,18)",
        "open_interest_value DECIMAL(38,18)",
        "cmc_circulating_supply DECIMAL(38,18)",
    ]
    OPEN_INTEREST_ARROW_SCHEMA = pa.schema(
        [
            ("open_time", pa.timestamp("ns")),
            ("symbol", pa.string()),
            ("open_interest", pa.decimal128(38, 18)),
            ("open_interest_value", pa.decimal128(38, 18)),
            ("cmc_circulating_supply", pa.decimal128(38, 18)),
        ]
    )
    _KEEPALIVE_OPTIONS = [
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
    ]

    def __init__(
        self,
        addr: str = "localhost:50051",
        schema: str = "data",
        table: str | None = None,
    ):
        self.location = f"grpc://{addr}"
        self.schema = schema
        self.ingestor = (
            os.getenv("INGESTOR_NAME")
            or os.getenv("INGESTOR_INSTANCE")
            or os.getenv("INGESTOR_EXEC")
            or "binance-open-interest"
        )
        self.max_lag_seconds = int(os.getenv("WATERMARK_MAX_LAG_SECONDS", "900"))
        self.append_max_message_bytes = int(
            os.getenv(
                "DUCKPORT_APPEND_MAX_MESSAGE_BYTES",
                str(DUCKPORT_APPEND_MAX_MESSAGE_BYTES),
            )
        )
        self.table = table or OPEN_INTEREST_TABLE
        self._write_lock = threading.Lock()
        self.client = flight.FlightClient(
            self.location,
            generic_options=self._KEEPALIVE_OPTIONS,
        )
        logger.info(f"DuckportClient connected: {self.location}, schema={schema}")

    @staticmethod
    def _sql_str(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _reconnect(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass
        self.client = flight.FlightClient(
            self.location,
            generic_options=self._KEEPALIVE_OPTIONS,
        )

    def ping(self) -> dict:
        results = list(self.client.do_action(flight.Action("duckport.ping", b"")))
        return json.loads(results[0].body.to_pybytes())

    def execute(self, sql: str) -> dict:
        action = flight.Action("duckport.execute", json.dumps({"sql": sql}).encode())
        try:
            results = list(self.client.do_action(action))
        except flight.FlightUnavailableError:
            self._reconnect()
            results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def execute_transaction(self, statements: list[str]) -> dict:
        body = json.dumps({"statements": statements}).encode()
        action = flight.Action("duckport.execute_transaction", body)
        try:
            results = list(self.client.do_action(action))
        except flight.FlightUnavailableError:
            self._reconnect()
            results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def append(self, schema: str, table: str, data: pa.Table) -> dict:
        descriptor = flight.FlightDescriptor.for_path("duckport.append", schema, table)
        try:
            writer, reader = self.client.do_put(descriptor, data.schema)
        except flight.FlightUnavailableError:
            self._reconnect()
            writer, reader = self.client.do_put(descriptor, data.schema)
        bytes_per_row = max(1, math.ceil(data.nbytes / max(1, len(data))))
        max_chunk = max(1, self.append_max_message_bytes // bytes_per_row)
        writer.write_table(data, max_chunksize=max_chunk)
        writer.done_writing()
        response = json.loads(bytes(reader.read()))
        writer.close()
        return response

    def read_table(self, schema: str, table: str) -> pa.Table:
        ticket = flight.Ticket(json.dumps({"schema": schema, "table": table}).encode())
        try:
            reader = self.client.do_get(ticket)
        except flight.FlightUnavailableError:
            self._reconnect()
            reader = self.client.do_get(ticket)
        return reader.read_all()

    def _table(self) -> str:
        return getattr(self, "table", OPEN_INTEREST_TABLE)

    def _time_floor(self) -> str:
        return "1min" if self._table().endswith("_1m") else "5min"

    def init_schema(self, start_time: pd.Timestamp) -> None:
        columns = ", ".join(self.OPEN_INTEREST_COL_DEFS)
        start_ts = pd.to_datetime(start_time, utc=True).strftime("%Y-%m-%d %H:%M:%S")
        table = self._table()
        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {self.schema}",
            f"CREATE TABLE IF NOT EXISTS {self.schema}.{table} "
            f"({columns}, PRIMARY KEY (open_time, symbol))",
            f"DROP TABLE IF EXISTS _staging_{table}",
            f"CREATE TABLE _staging_{table} ({columns})",
            f"INSERT INTO {self.schema}.watermark "
            f"(table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
            f"VALUES ('{table}', {self._sql_str(self.ingestor)}, {self.max_lag_seconds}, "
            f"'open_time', '{start_ts}', NULL, CURRENT_TIMESTAMP) "
            f"ON CONFLICT (table_name) DO UPDATE SET "
            f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
            f"time_column = excluded.time_column",
        ]
        self.execute_transaction(statements)
        logger.info(f"Schema '{self.schema}' initialised for {table}")

    @classmethod
    def _open_interest_arrow(cls, frame: pd.DataFrame, *, time_floor: str = "5min") -> pa.Table:
        normalized = normalize_open_interest_frame(frame, floor=time_floor)
        records = []
        for row in normalized.itertuples(index=False):
            timestamp = pd.to_datetime(row.open_time, utc=True).tz_localize(None).to_pydatetime()
            records.append(
                {
                    "open_time": timestamp,
                    "symbol": str(row.symbol),
                    "open_interest": Decimal(row.open_interest),
                    "open_interest_value": Decimal(row.open_interest_value),
                    "cmc_circulating_supply": (
                        None
                        if row.cmc_circulating_supply is None
                        else Decimal(row.cmc_circulating_supply)
                    ),
                }
            )
        return pa.Table.from_pylist(records, schema=cls.OPEN_INTEREST_ARROW_SCHEMA)

    def write_open_interest(
        self,
        frame: pd.DataFrame,
        complete_through: pd.Timestamp,
        *,
        time_floor: str | None = None,
    ) -> dict:
        if frame.empty:
            self.replace_duck_time(complete_through)
            return {"total_rows": 0}
        floor = self._time_floor() if time_floor is None else time_floor
        arrow = self._open_interest_arrow(frame[OPEN_INTEREST_COLUMNS], time_floor=floor)
        table = self._table()
        staging = f"_staging_{table}"
        target = f"{self.schema}.{table}"
        timestamp = pd.to_datetime(complete_through, utc=True).strftime("%Y-%m-%d %H:%M:%S")
        with self._write_lock:
            self.execute(f"TRUNCATE {staging}")
            append_response = self.append("main", staging, arrow)
            response = self.execute_transaction(
                [
                    f"INSERT INTO {target} SELECT * FROM {staging} "
                    f"ON CONFLICT (open_time, symbol) DO NOTHING",
                    self._watermark_sql(timestamp),
                    f"TRUNCATE {staging}",
                ]
            )
        logger.info(
            f"OI write complete: staged={append_response.get('rows_appended', len(arrow))}, "
            f"through={timestamp}"
        )
        return response

    def _watermark_sql(self, timestamp: str) -> str:
        return (
            f"INSERT INTO {self.schema}.watermark "
            f"(table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
            f"VALUES ('{self._table()}', {self._sql_str(self.ingestor)}, "
            f"{self.max_lag_seconds}, 'open_time', NULL, '{timestamp}', CURRENT_TIMESTAMP) "
            f"ON CONFLICT (table_name) DO UPDATE SET "
            f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
            f"duck_time = excluded.duck_time, updated_at = excluded.updated_at"
        )

    def replace_duck_time(self, timestamp: pd.Timestamp) -> None:
        value = pd.to_datetime(timestamp, utc=True).strftime("%Y-%m-%d %H:%M:%S")
        self.execute_transaction([self._watermark_sql(value)])

    def read_duck_time(self) -> Optional[pd.Timestamp]:
        try:
            frame = self.read_table(self.schema, "watermark").to_pandas()
            row = frame[frame["table_name"] == self._table()]
            if row.empty or pd.isna(row["duck_time"].iloc[0]):
                return None
            timestamp = pd.to_datetime(row["duck_time"].iloc[0], utc=True)
            return timestamp
        except Exception as exc:
            logger.warning(f"Cannot read OI watermark: {exc}")
            return None

    def close(self) -> None:
        self.client.close()
        logger.info("DuckportClient closed")
