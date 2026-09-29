from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd


OPEN_INTEREST_COLUMNS = [
    "open_time",
    "symbol",
    "open_interest",
    "open_interest_value",
    "cmc_circulating_supply",
]


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value is pd.NA or value == "":
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def normalize_open_interest_frame(df: pd.DataFrame, *, floor: str = "5min") -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OPEN_INTEREST_COLUMNS)
    out = df.copy()
    for column in OPEN_INTEREST_COLUMNS:
        if column not in out:
            out[column] = pd.NA
    out = out[OPEN_INTEREST_COLUMNS]
    out["open_time"] = pd.to_datetime(out["open_time"], utc=True).dt.floor(floor)
    out["symbol"] = out["symbol"].astype(str)
    for column in (
        "open_interest",
        "open_interest_value",
        "cmc_circulating_supply",
    ):
        out[column] = out[column].map(_decimal_or_none)
    out = out.dropna(subset=["open_time", "symbol", "open_interest", "open_interest_value"])
    out = out[out["symbol"].str.endswith("USDT")]
    out = out.drop_duplicates(subset=["open_time", "symbol"], keep="last")
    return out.sort_values(["open_time", "symbol"]).reset_index(drop=True)


def parse_open_interest_rows(
    rows: list[dict[str, Any]],
    *,
    requested_symbol: str | None = None,
) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=OPEN_INTEREST_COLUMNS)
    records = []
    for row in rows:
        symbol = row.get("symbol") or requested_symbol
        records.append(
            {
                "open_time": pd.to_datetime(row.get("timestamp"), unit="ms", utc=True),
                "symbol": symbol,
                "open_interest": row.get("sumOpenInterest"),
                "open_interest_value": row.get("sumOpenInterestValue"),
                "cmc_circulating_supply": row.get("CMCCirculatingSupply"),
            }
        )
    return normalize_open_interest_frame(pd.DataFrame.from_records(records))


def build_snapshot_frame(open_time, rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Point-in-time OI rows labeled with one scheduled minute.

    ``open_interest_value`` is ``open_interest * mark_price``. Rows without a
    positive mark price are dropped. Circulating supply is not available on
    the live open-interest endpoint.
    """
    value_quantum = Decimal("1e-18")
    records = []
    for row in rows:
        symbol = row.get("symbol")
        raw_oi = row.get("open_interest")
        raw_mark = row.get("mark_price")
        if not symbol or raw_oi in (None, "") or raw_mark in (None, ""):
            continue
        try:
            open_interest = Decimal(str(raw_oi))
            mark_price = Decimal(str(raw_mark))
        except (InvalidOperation, ValueError):
            continue
        if mark_price <= 0:
            continue
        records.append(
            {
                "open_time": open_time,
                "symbol": symbol,
                "open_interest": open_interest,
                "open_interest_value": (open_interest * mark_price).quantize(value_quantum),
                "cmc_circulating_supply": None,
            }
        )
    if not records:
        return pd.DataFrame(columns=OPEN_INTEREST_COLUMNS)
    return normalize_open_interest_frame(pd.DataFrame.from_records(records), floor="1min")
