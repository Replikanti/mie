# Test fixtures

Public Binance USDⓈ-M market data for BTCUSDT; no keys, tokens or account
data. One verbatim payload per line (`*.jsonl`, and archive data rows in
`archive/*.csv`), exactly the bytes the raw store keeps.

| File | Provenance |
|---|---|
| `aggTrade.jsonl` | Live: 351 consecutive frames of `btcusdt@aggTrade` (ids contiguous), recorded with `mie ingest` on 2026-10-06 (about 2 min) |
| `markPrice.jsonl` | Live: `btcusdt@markPrice@1s`, same capture |
| `kline_1m.jsonl` | Live: `btcusdt@kline_1m`, same capture; two closed bars (`"x":true`), the rest open |
| `openInterest.jsonl` | Live: `GET /fapi/v1/openInterest?symbol=BTCUSDT` response bodies at 10 s slots, same capture |
| `forceOrder.jsonl` | **Synthetic**: no BTCUSDT liquidation fired during the capture. Line 1 is the example payload of the Binance documentation, line 2 a BUY-side variant with the same fields |
| `archive/BTCUSDT-{aggTrades,1m,metrics,bookDepth}-2026-09-30.csv` | Archive: the header, the first 20 and the last 5 data rows of each file from `data.binance.vision/data/futures/um/daily/…`, unzipped after its `.CHECKSUM` verified (2026-10-06). Metrics rows keep the file's own, unsorted order |
| `archive/BTCUSDT-1d-2026-09-30.csv` | Archive: the whole file (header and one bar) |
| `archive/BTCUSDT-fundingRate-2026-08.csv` | Archive: the header, the first 20 and the last 5 rows of `data/futures/um/monthly/fundingRate/…` |
| `archive/BTCUSDT-fundingRate-exponent.csv` | Archive: the header and all 12 rows of the monthly `fundingRate` files 2025-10 … 2026-09 whose rate is written with an exponent (`-1.8E-7`, `-6E-8`, `9.0E-7`, …), in file order, each file's `.CHECKSUM` verified (2026-10-07) |
| `depth.jsonl` | Live: 29 consecutive frames of `btcusdt@depth@100ms` on the `/public` route (one connection, `pu` chained), recorded with `mie ingest` on 2026-10-07 (`depth_snapshot_limit = 100`, `depth_checkpoint_interval_secs = 2`): from the session's first diff through the diff that straddles the second snapshot, plus one |
| `depthSnapshot.jsonl` | Live: the two `GET /fapi/v1/depth?symbol=BTCUSDT&limit=100` bodies of the same capture. Arrival order: line 1 after `depth` line 5, line 2 after `depth` line 27 (`tests/book_fixtures.rs` replays it) |
| `scenario-*.tsv` | **Synthetic** arrival sequences for `tests/pipeline_scenarios.rs`: `receive_seq \t stream \t session_id \t payload` |
| `scenario-book-*.tsv` | **Synthetic** order-book arrival sequences for `tests/book_scenarios.rs`, same format |

The live frames carry fields this adapter ignores (`nq`, `st`, `ap`, `ps`,
the snapshot's `E`, …) and the kline payloads keep Binance's spaces after
commas: normalization must accept both.
