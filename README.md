# duckport-binance-open-interest

Binance USD-M perpetual 5-minute open-interest ingestor for `duckport-rs`.

It writes `data.usdt_perp_open_interest_5m` with a primary key on
`(open_time, symbol)`. On first startup it backfills up to the latest 30 days
available from Binance, then polls every five minutes. Writes and the
`data.watermark` update are committed atomically through duckport's staging
protocol.

## Table

| column | type |
|---|---|
| `open_time` | `TIMESTAMP` |
| `symbol` | `VARCHAR` |
| `open_interest` | `DECIMAL(38,18)` |
| `open_interest_value` | `DECIMAL(38,18)` |
| `cmc_circulating_supply` | `DECIMAL(38,18)` |

## Configuration

- `BACKFILL_DAYS`: initial history window, clamped to 1-30 days (default 30)
- `CONCURRENCY`: concurrent HTTP requests (default 8)
- `REQUEST_INTERVAL_SECONDS`: global spacing between OI history calls (default
  0.34s, about 882 calls per five minutes)
- `POLL_DELAY_SECONDS`: wait after each five-minute boundary (default 30s)
- `PROXY_URL`: optional HTTP proxy

The historical API is rate limited to 1000 requests per five minutes. Do not
set `REQUEST_INTERVAL_SECONDS` below `0.30` for a full-market collector.

## 1-minute snapshots

`OI_MODE=snapshot` writes `data.usdt_perp_open_interest_1m` from the live
`/fapi/v1/openInterest` endpoint. There is no historical backfill: the first
row is the minute the process starts sampling, and a missed minute stays
missing. `open_interest_value` is `open_interest * markPrice`.
`cmc_circulating_supply` is null.

Run it as a separate instance from the 5-minute collector. Useful settings:

- `SNAPSHOT_RPS`: global request rate, default 40
- `SNAPSHOT_DELAY_SECONDS`: wait after each minute boundary, default 5
- `WATERMARK_MAX_LAG_SECONDS`: default 180 for this mode
