"""Environment-driven configuration for binance-open-interest."""

from __future__ import annotations

import os
import pathlib

from dotenv import load_dotenv

_env_file = os.getenv("INGESTOR_ENV_FILE", "config.env")
load_dotenv(_env_file, override=False)

_server_env = os.getenv(
    "DUCKPORT_SERVER_ENV",
    str(pathlib.Path.home() / ".duckport" / "server.env"),
)
load_dotenv(_server_env, override=False)
load_dotenv("/opt/duckport/server.env", override=False)


def _derive_duckport_addr() -> str:
    raw = os.getenv("DUCKPORT_LISTEN_ADDR", "0.0.0.0:50051")
    host, _, port = raw.rpartition(":")
    host = "localhost" if host in ("0.0.0.0", "", "*") else host
    return f"{host}:{port}"


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    value = int(os.getenv(name, str(default)))
    return min(maximum, max(minimum, value))


DUCKPORT_ADDR = _derive_duckport_addr()
DUCKPORT_SCHEMA = "data"
OPEN_INTEREST_TABLE = "usdt_perp_open_interest_5m"
OPEN_INTEREST_1M_TABLE = "usdt_perp_open_interest_1m"
OPEN_INTEREST_INTERVAL = "5m"
OPEN_INTEREST_INTERVAL_MINUTES = 5
# "5m" (default; "history" is the legacy name) or "snapshot" (1m table).
OI_MODE = os.getenv("OI_MODE", "5m").strip().lower()

# Live 1-minute snapshots use /fapi/v1/openInterest (weight 1 each).
# 40 requests/second finishes ~530 symbols in about 15 seconds.
SNAPSHOT_CONCURRENCY = _bounded_int("SNAPSHOT_CONCURRENCY", 32, 1, 64)
SNAPSHOT_RPS = min(50.0, max(5.0, float(os.getenv("SNAPSHOT_RPS", "40"))))
SNAPSHOT_DELAY_SECONDS = _bounded_int("SNAPSHOT_DELAY_SECONDS", 5, 0, 40)
SNAPSHOT_RETRY_TIMES = _bounded_int("SNAPSHOT_RETRY_TIMES", 2, 0, 5)

# 5m bars: label T holds the open interest at the bar close T+5m.
# The live sweep samples /fapi/v1/openInterest right after each close.
BAR_CLOSE_DELAY_SECONDS = _bounded_int("BAR_CLOSE_DELAY_SECONDS", 1, 0, 30)
# Symbols still unsampled this long after the close go to history repair.
BAR_DEADLINE_SECONDS = _bounded_int("BAR_DEADLINE_SECONDS", 30, 5, 120)

# Binance exposes at most the latest month through openInterestHist.
BACKFILL_DAYS = _bounded_int("BACKFILL_DAYS", 30, 1, 30)
CONCURRENCY = _bounded_int("CONCURRENCY", 8, 1, 64)
# openInterestHist allows 1000 requests per rolling 5 minutes per IP.
HIST_REQUESTS_PER_5M = _bounded_int("HIST_REQUESTS_PER_5M", 900, 50, 1000)
# Binance publishes a close C point gradually, measured up to C+3m.
HIST_PUBLISH_DELAY_SECONDS = _bounded_int("HIST_PUBLISH_DELAY_SECONDS", 240, 60, 1800)
# On startup, fill gaps inside this window from history.
REPAIR_LOOKBACK_HOURS = _bounded_int("REPAIR_LOOKBACK_HOURS", 24, 0, 720)
REPAIR_MAX_ATTEMPTS = _bounded_int("REPAIR_MAX_ATTEMPTS", 3, 1, 20)
HTTP_TIMEOUT_SECONDS = _bounded_int("HTTP_TIMEOUT_SECONDS", 30, 5, 120)
RETRY_TIMES = _bounded_int("RETRY_TIMES", 5, 0, 20)
RETRY_BASE_SECONDS = max(1.0, float(os.getenv("RETRY_BASE_SECONDS", "2")))

PROXY_URL = os.getenv("PROXY_URL", "").strip()
proxy = PROXY_URL or None

DUCKPORT_APPEND_MAX_MESSAGE_BYTES = int(
    os.getenv("DUCKPORT_APPEND_MAX_MESSAGE_BYTES", str(48 * 1024 * 1024))
)
