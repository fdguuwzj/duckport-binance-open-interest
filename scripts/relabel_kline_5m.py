#!/usr/bin/env python3
"""One-off: move data.usdt_perp_open_interest_5m to K-line labels.

Before this change the table mixed two meanings under one label T:

* openInterestHist rows (``cmc_circulating_supply`` NOT NULL): open interest
  at T. They move to label T-5m.
* derivative_ticker imports (``cmc_circulating_supply`` NULL): the last tick
  in [T, T+5m), i.e. open interest at the close. They keep their label.

When a shifted history row lands on a label an import row already holds,
both describe the same close and the history row wins. The watermark moves
back 5 minutes. Every row the migration moves or removes is copied, with its
old label, to ``usdt_perp_open_interest_5m_pre_kline``.

History rows are moved one day per transaction, oldest first, so memory stays
small and an interrupted run resumes where it stopped.

Stop the 5m collector first (``duckport stop binance-open-interest-5m``).
Without ``--apply`` the script only prints what it would do.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

import pandas as pd
import pyarrow.flight as flight

TABLE = "usdt_perp_open_interest_5m"
BACKUP = f"{TABLE}_pre_kline"
MARKER = f"{TABLE}.label"
PROGRESS = f"{TABLE}.relabel_done_before"
FMT = "%Y-%m-%d %H:%M:%S"
COLUMNS = (
    "open_time TIMESTAMP, symbol VARCHAR, open_interest DECIMAL(38,18), "
    "open_interest_value DECIMAL(38,18), cmc_circulating_supply DECIMAL(38,18)"
)


class Client:
    def __init__(self, addr: str):
        self.client = flight.FlightClient(f"grpc://{addr}")

    def query(self, sql: str) -> list[dict]:
        ticket = flight.Ticket(json.dumps({"type": "duckport.query", "sql": sql}).encode())
        try:
            return self.client.do_get(ticket).read_all().to_pylist()
        except flight.FlightInternalError as exc:
            # duckport sends no data message for an empty result.
            if "never sent a data message" in str(exc):
                return []
            raise

    def transaction(self, statements: list[str]) -> dict:
        body = json.dumps({"statements": statements}).encode()
        result = list(self.client.do_action(flight.Action("duckport.execute_transaction", body)))
        return json.loads(result[0].body.to_pybytes())


def collector_running() -> bool:
    found = subprocess.run(
        ["pgrep", "-f", "ingestors/binance-open-interest-5m/"],
        capture_output=True,
    )
    return found.returncode == 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--addr", default="localhost:8816")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    db = Client(args.addr)

    marker = db.query(f"SELECT value FROM data.config_dict WHERE key = '{MARKER}'")
    if marker and marker[0]["value"] == "kline":
        print(f"{TABLE} already uses K-line labels; nothing to do")
        return 0
    progress = db.query(f"SELECT value FROM data.config_dict WHERE key = '{PROGRESS}'")
    done_before = progress[0]["value"] if progress else None
    backup_exists = bool(
        db.query(f"SELECT 1 FROM duckdb_tables() WHERE schema_name = 'data' AND table_name = '{BACKUP}'")
    )
    if backup_exists and done_before is None:
        print(f"data.{BACKUP} already exists; drop or rename it first", file=sys.stderr)
        return 1
    if done_before is not None:
        print(f"resuming: history rows before {done_before} are already moved")

    stats = db.query(
        f"""
        WITH h AS (SELECT open_time - INTERVAL 5 MINUTE AS open_time, symbol
                   FROM data.{TABLE} WHERE cmc_circulating_supply IS NOT NULL),
             i AS (SELECT open_time, symbol FROM data.{TABLE} WHERE cmc_circulating_supply IS NULL)
        SELECT (SELECT count(*) FROM data.{TABLE}) AS total,
               (SELECT count(*) FROM h) AS history_rows,
               (SELECT count(*) FROM i) AS import_rows,
               (SELECT count(*) FROM h JOIN i USING (open_time, symbol)) AS replaced_imports,
               (SELECT min(open_time) FROM h) AS history_first_label,
               (SELECT max(open_time) FROM h) AS history_last_label
        """
    )[0]
    watermark = db.query(f"SELECT duck_time FROM data.watermark WHERE table_name = '{TABLE}'")
    duck_time = watermark[0]["duck_time"] if watermark else None
    print(json.dumps({**stats, "duck_time": duck_time}, default=str, indent=2))
    print(f"rows after: {stats['total'] - stats['replaced_imports']:,}")

    if not args.apply:
        print("dry run; pass --apply to migrate")
        return 0
    if collector_running():
        print("binance-open-interest-5m is running; stop it first", file=sys.stderr)
        return 1

    if not backup_exists:
        db.transaction([f"CREATE TABLE data.{BACKUP} ({COLUMNS})"])
    bounds = db.query(
        f"SELECT min(open_time) AS lo, max(open_time) AS hi FROM data.{TABLE} "
        f"WHERE cmc_circulating_supply IS NOT NULL"
        + (f" AND open_time >= TIMESTAMP '{done_before}'" if done_before else "")
    )[0]
    if bounds["lo"] is not None:
        days = pd.date_range(
            pd.Timestamp(bounds["lo"]).floor("D"),
            pd.Timestamp(bounds["hi"]).floor("D") + pd.Timedelta(days=1),
            freq="D",
        )
        for lo, hi in zip(days[:-1], days[1:]):
            a, b = lo.strftime(FMT), hi.strftime(FMT)
            window = (
                f"cmc_circulating_supply IS NOT NULL "
                f"AND open_time >= TIMESTAMP '{a}' AND open_time < TIMESTAMP '{b}'"
            )
            # Import rows the shifted history rows will replace.
            collide = (
                f"cmc_circulating_supply IS NULL AND EXISTS (SELECT 1 FROM data.{TABLE} h "
                f"WHERE h.{window} AND h.symbol = o.symbol "
                f"AND h.open_time - INTERVAL 5 MINUTE = o.open_time)"
            )
            result = db.transaction(
                [
                    f"INSERT INTO data.{BACKUP} SELECT * FROM data.{TABLE} WHERE {window}",
                    f"INSERT INTO data.{BACKUP} SELECT * FROM data.{TABLE} o WHERE {collide}",
                    f"DELETE FROM data.{TABLE} o WHERE {collide}",
                    f"DELETE FROM data.{TABLE} WHERE {window}",
                    f"INSERT INTO data.{TABLE} SELECT open_time - INTERVAL 5 MINUTE, symbol, "
                    f"open_interest, open_interest_value, cmc_circulating_supply "
                    f"FROM data.{BACKUP} WHERE {window}",
                    f"INSERT INTO data.config_dict (key, value) VALUES ('{PROGRESS}', '{b}') "
                    f"ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                ]
            )
            print(f"  {a[:10]}: {result.get('rows_affected')}", flush=True)
    db.transaction(
        [
            f"UPDATE data.watermark SET duck_time = duck_time - INTERVAL 5 MINUTE, "
            f"updated_at = CURRENT_TIMESTAMP WHERE table_name = '{TABLE}'",
            f"INSERT INTO data.config_dict (key, value) VALUES ('{MARKER}', 'kline') "
            f"ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            f"DELETE FROM data.config_dict WHERE key = '{PROGRESS}'",
        ]
    )
    after = db.query(
        f"SELECT count(*) AS n, min(open_time) AS first, max(open_time) AS last FROM data.{TABLE}"
    )[0]
    print(json.dumps(after, default=str))
    print(f"moved/removed rows kept in data.{BACKUP}; drop it once the new labels are checked")
    return 0


if __name__ == "__main__":
    sys.exit(main())
