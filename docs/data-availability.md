# Data availability

Which raw data exists for the BTCUSDT USDⓈ-M perpetual, from when, at what
resolution, and which features can be computed for which period. Sources:
live capture (`mie ingest`, source `binance-um`, ADR-032) and the public
archive backfill (`mie archive-import`, source `binance-archive`, ADR-034).

**Gaps** come from the acceptance run of #12 (`mie archive-verify` lists
trade-id breaks and holes over 60 s per stream), window 2025-10-01 …
2026-09-30. The archive bucket listing is stale, so availability was
checked per file (ADR-034): all 9 daily streams for each of the 365 days
and the 12 monthly `fundingRate` files were published, **0 missing**
([results](https://github.com/Replikanti/mie/issues/12#issuecomment-6032650398)).

## Archive datasets (`data.binance.vision/data/futures/um/`)

| Dataset | Archive path | Raw stream | Resolution | Archive history start | Imported window | Normalized to | Gaps / caveats |
|---|---|---|---|---|---|---|---|
| Aggregate trades | `daily/aggTrades/BTCUSDT/` | `aggTrades` | per aggregate, ms | 2019-12-31 | 2025-10-01 … 2026-09-30 | `Trade` (`is_buyer_maker` ⇒ sell aggressor) | 0 id breaks, 0 holes over 60 s across the whole year (780 files, 603 218 226 rows). An aggregate's constituents can fall in another minute than its `transact_time`, so aggTrades bars differ from klines at minute boundaries (ADR-034 D8) |
| Klines 1m, 5m, 15m, 1h, 4h, 1d | `daily/klines/BTCUSDT/<interval>/` | `klines_1m` … `klines_1d` | the interval | 2019-12-31 | as above | `Kline` (cross-check only, never a bar source) | 365 files per interval, 0 missing, 0 integrity errors |
| Funding rate | `monthly/fundingRate/BTCUSDT/` | `fundingRate` | per settlement (8 h) | 2020-01 | months overlapping the window | `FundingSettlement` at `calc_time` | Monthly files only; `calc_time` has ms jitter. 12 months, 0 missing; 12 rows use exponent notation (`-1.8E-7`), parsed exactly since #55 |
| Metrics (open interest, ratios) | `daily/metrics/BTCUSDT/` | `metrics` | 5 min | 2021-12-01 | as above | `OpenInterest`, `resolution_ms` 300 000, ordered at `create_time` + 5 min (ADR-034 D3) | Rows unsorted in the file; the 23:55 sample is filed under the next day. Ratio columns stored raw only |
| Book depth bands | `daily/bookDepth/BTCUSDT/` | `bookDepth` | ~30 s, ±1–5 % bands | 2023-01-01 | as above | none (raw only, DuckDB research) | Percentage bands, not a book; band labels `-5` vs `-5.00` differ across the window |
| Individual trades | `daily/trades/BTCUSDT/` | `trades` (opt-in) | per trade, ms | 2019-12-31 | days of the kline cross-check only | `Trade` with the raw id, cross-check only | Not a replay stream. Imported for 10 days, including the two cross-check windows (2025-10-09 … 10-11, 2026-02-05 … 02-07). Single skipped trade ids (the klines skip them too, so bars are unaffected). **Upstream defect:** `BTCUSDT-trades-2025-10-10.zip` omits trades from 22:03 UTC to the end of the file; the aggTrades file and the klines contain them (ADR-031) |
| Book ticker | `daily/bookTicker/` | — | — | — | not imported | — | Ends 2024-03-30 |
| Liquidations | — | — | — | — | — | — | No USD-M archive dataset |

## Live-only data

| Data | Raw stream (source `binance-um`) | Resolution | From | Normalized to |
|---|---|---|---|---|
| Aggregate trades | `aggTrade` | per aggregate | start of live capture (#9) | `Trade` |
| Mark and index price, indicative funding | `markPrice` | 1 s | start of live capture | `MarkPrice` |
| Liquidation snapshots | `forceOrder` | ≤ 1/s per symbol (throttled lower bound) | start of live capture | `Liquidation` |
| Closed 1m klines | `kline_1m` | 1 min | start of live capture | `Kline` |
| Open interest | `openInterest` | 10 s poll | start of live capture | `OpenInterest`, `resolution_ms` 10 000 |
| Order book (diff depth + snapshots) | `depth`, `depthSnapshot` | diffs per 100 ms batch; `limit=1000` snapshots on every resync and a checkpoint every 60 s (ADR-038) | start of live depth capture (#10, first soak 2026-10-08) | `BookUpdate`, `BookSnapshot` |

## Features by period

| Feature family | Inputs | Computable from archive (window) | Computable from live | Notes |
|---|---|---|---|---|
| Event-time bars `bars.time.*` (1m … 1d) and ATR / ATR percentile (#16) | trades | yes, from aggTrades | yes | Bars come from trades only; klines are a cross-check (ADR-031) |
| Delta, CVD, volume at price | trades with aggressor | yes | yes | Aggressor from `is_buyer_maker` / `m` (ADR-023) |
| OI level, ΔOI, OI velocity (#19): `derivatives.oi.sample@1` (source resolution), `derivatives.oi.5m@1` (UTC 5-minute grid) | open interest | yes, 5 min | yes, 10 s | `resolution_ms` per value tells them apart; the native step never crosses resolutions, the grid is computable from both sources (ADR-042); archive values lag up to one interval |
| Funding settlements: `derivatives.funding.settled@1` | funding rate | yes | no settlement event; the indicative rate arrives with `MarkPrice` | Live settlements come from the archive once published (next day) |
| Indicative funding, mark/index price, basis, time to next funding: `derivatives.mark@1` | mark price | **no** | yes | Mark/index price klines are not imported; stays warming up in archive replays |
| Liquidation windows by side: `derivatives.liq.window.<5m\|15m\|1h>@1` | liquidations | **no** | yes | Lower bound only; warming up until the first liquidations-stream event, so archive zeros never read as a quiet market (ADR-042) |
| Order-book liquidity state (#18): `book.l2@1` (the L2 book), `book.depth@1` (depth and imbalance within 1, 2 and 5 bps), `book.clusters@1` (largest levels within 5 bps), `book.liquidity.window.<5m\|15m\|1h>@1` (liquidity added, cancelled and filled) | order book, trades | **no** | **from the start of live depth capture (#10) only** | Live only (ADR-043): every `book.*` value stays warming up in archive replays. bookDepth bands (raw, from 2023-01-01) are too coarse to substitute |
| Depth-dependent order flow (#22): absorption and exhaustion with passive liquidity | order book, trades | **no** | from the start of live depth capture (#10) only | Built on the `book.*` features |
