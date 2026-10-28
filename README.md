# duckport-binance-open-interest

Binance USD-M perpetual 5-minute open-interest ingestor for `duckport-rs`.

It writes `data.usdt_perp_open_interest_5m` with a primary key on
`(open_time, symbol)`. Writes and the `data.watermark` update are committed
atomically through duckport's staging protocol.

## Label semantics

Rows use K-line labels: **label `T` holds the open interest at the bar close
`T+5m`**, the same instant a 5m K-line labeled `T` closes. Joining this table
to `usdt_perp_5m` on `(open_time, symbol)` therefore gives the open interest
at the end of that K-line, with no look-ahead.

Open interest is a stock, so every Binance source is a point-in-time value.
Binance has no open-interest K-line (no OHLC); `openInterestHist` is the only
5-minute history endpoint, and its point stamped `C` is the open interest at
`C`. This collector stores that point under label `C-5m`.

## How rows are collected

**Live (every bar).** One second after each close `C`, the collector reads
`GET /fapi/v1/openInterest` for every trading USDT perpetual and TradFi
perpetual, prices it with one `GET /fapi/v1/premiumIndex` call
(`open_interest_value = open_interest * markPrice`), and writes label `C-5m`
together with the watermark. A full-market sweep (about 750 symbols at 40
requests/s) finishes about 20 seconds after the close. Symbols that keep
failing for `BAR_DEADLINE_SECONDS` go to repair.

**Repair (history).** Labels the live sweep could not sample (downtime,
startup, failed symbols) are queued and fetched from
`/futures/data/openInterestHist` once Binance has published them, i.e.
`HIST_PUBLISH_DELAY_SECONDS` after the close. Each symbol is requested from
its oldest missing label, so a late point is never lost.

Why live comes from `/fapi/v1/openInterest` and not from history (measured
2026-10-25):

- History publishes a close gradually: the same point appeared at `C+30s`
  for some symbols and only at `C+3m` for others. Live sampling does not wait
  for it.
- History has permanent holes for illiquid and TradFi symbols (about 5% of
  expected points). The live endpoint answers for every trading symbol.
- The two sources differ by about 1e-4 relative at the same close (sampling
  moment). Repaired rows therefore match live rows closely but not exactly;
  `cmc_circulating_supply` is filled only on repaired rows (live is null).

## Watermark

`duck_time` is the latest label whose live sweep was written. After a
restart, the gap after the stored watermark is repaired first; live rows are
written meanwhile but the watermark stays put until that gap is done, so a
reader never sees a watermark over a range that is still filling. A later
repair of one failed symbol, or a hole in the startup lookback window, lands
behind the watermark. Labels history does not have stay missing.

## Table

| column | type |
|---|---|
| `open_time` | `TIMESTAMP` (label; value is at `open_time + 5m`) |
| `symbol` | `VARCHAR` |
| `open_interest` | `DECIMAL(38,18)` |
| `open_interest_value` | `DECIMAL(38,18)` |
| `cmc_circulating_supply` | `DECIMAL(38,18)` (history rows only) |

## Configuration

- `OI_MODE`: `5m` (default; `history` is accepted as the old name) or
  `snapshot` for the 1-minute table below
- `BAR_CLOSE_DELAY_SECONDS`: wait after each close before sampling (default 1)
- `BAR_DEADLINE_SECONDS`: live retries stop this long after the close
  (default 30)
- `SNAPSHOT_RPS`, `SNAPSHOT_CONCURRENCY`: live request rate and concurrency
  (default 40/s, 32)
- `BACKFILL_DAYS`: history window of a fresh table, 1-30 days (default 30)
- `CONCURRENCY`: concurrent history requests (default 8)
- `HIST_REQUESTS_PER_5M`: rolling 5-minute cap on history requests (default
  900; Binance allows 1000 per IP)
- `HIST_PUBLISH_DELAY_SECONDS`: repair a label this long after its close
  (default 240)
- `REPAIR_LOOKBACK_HOURS`: on startup, also repair missing labels in this
  window before the watermark (default 24, up to 720)
- `REPAIR_MAX_ATTEMPTS`: drop a symbol's queued labels after this many failed
  history fetches (default 3)
- `PROXY_URL`: optional HTTP proxy

Rate limits are per IP and shared with every process behind it. The live
sweep costs weight 1 per symbol plus 10 for `premiumIndex` against the 2400
weight/minute limit; a 1-minute snapshot instance on the same IP costs as
much every minute.

## Migrating a table written by 0.1.x

0.1.x stored history points under their own timestamp (open interest at `T`)
while derivative-ticker imports were already close values. Stop the
collector, then run:

```bash
duckport stop binance-open-interest-5m
python scripts/relabel_kline_5m.py           # dry run: counts only
python scripts/relabel_kline_5m.py --apply   # shift history rows by -5m
duckport start binance-open-interest-5m
```

History rows (`cmc_circulating_supply` not null) move to label `T-5m`;
import rows keep theirs, and where both land on one label the history row
wins. The watermark moves back 5 minutes and the old table is kept as
`usdt_perp_open_interest_5m_pre_kline`. Starting once with
`REPAIR_LOOKBACK_HOURS=720` refills the points 0.1.x lost within the last
30 days.

## 1-minute snapshots

`OI_MODE=snapshot` writes `data.usdt_perp_open_interest_1m` from the live
`/fapi/v1/openInterest` endpoint. There is no historical backfill: the first
row is the minute the process starts sampling, and a missed minute stays
missing. `open_interest_value` is `open_interest * markPrice`.
`cmc_circulating_supply` is null. Unlike the 5m table, label `M` is sampled
shortly after `M` itself (the start of the minute).

Run it as a separate instance from the 5-minute collector. Useful settings:

- `SNAPSHOT_RPS`: global request rate, default 40
- `SNAPSHOT_DELAY_SECONDS`: wait after each minute boundary, default 5
- `WATERMARK_MAX_LAG_SECONDS`: default 180 for this mode
