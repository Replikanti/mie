# Test fixtures

Public Binance USDⓈ-M market data for BTCUSDT; no keys, tokens or account
data. One verbatim payload per line (`*.jsonl`), exactly the bytes the raw
store keeps.

| File | Provenance |
|---|---|
| `aggTrade.jsonl` | Live: 351 consecutive frames of `btcusdt@aggTrade` (ids contiguous), recorded with `mie ingest` on 2026-10-06 (about 2 min) |
| `markPrice.jsonl` | Live: `btcusdt@markPrice@1s`, same capture |
| `kline_1m.jsonl` | Live: `btcusdt@kline_1m`, same capture; two closed bars (`"x":true`), the rest open |
| `openInterest.jsonl` | Live: `GET /fapi/v1/openInterest?symbol=BTCUSDT` response bodies at 10 s slots, same capture |
| `forceOrder.jsonl` | **Synthetic**: no BTCUSDT liquidation fired during the capture. Line 1 is the example payload of the Binance documentation, line 2 a BUY-side variant with the same fields |
| `scenario-*.tsv` | **Synthetic** arrival sequences for `tests/pipeline_scenarios.rs`: `receive_seq \t stream \t session_id \t payload` |

The live frames carry fields this adapter ignores (`nq`, `st`, `ap`, …) and
the kline payloads keep Binance's spaces after commas: normalization must
accept both.
