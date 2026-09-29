#!/usr/bin/env python3
from __future__ import annotations

import os
import signal
import sys
import threading

os.environ["TZ"] = "UTC"

from binance_open_interest.config import DUCKPORT_ADDR, DUCKPORT_SCHEMA, OI_MODE
from binance_open_interest.data_jobs import OpenInterestDataJobs, initial_start_time
from binance_open_interest.duckport_client import DuckportClient
from binance_open_interest.log import logger


def main() -> None:
    if OI_MODE == "snapshot":
        from binance_open_interest.snapshot import main as snapshot_main

        snapshot_main()
        return
    if OI_MODE != "history":
        logger.error(f"Unknown OI_MODE={OI_MODE!r}; expected history or snapshot")
        sys.exit(1)
    logger.info("Binance 5m open-interest ingestor starting")
    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA)

    def shutdown(signum, frame):
        logger.warning(f"Received signal {signum}; shutting down")
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        info = client.ping()
        logger.info(f"duckport-rs server: {info}")
    except Exception as exc:
        logger.error(f"Cannot reach duckport-rs at {DUCKPORT_ADDR}: {exc}")
        sys.exit(1)

    client.init_schema(initial_start_time())
    OpenInterestDataJobs(client)

    try:
        signal.pause()
    except AttributeError:
        threading.Event().wait()


if __name__ == "__main__":
    main()
