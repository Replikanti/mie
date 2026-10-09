# Equivalence fixture: one recorded live session

The input of `tests/equivalence_offline.rs` (#13, ADR-041 D5): one real
`mie ingest` run, exported from its raw store and journal. Public Binance
USDⓈ-M market data only.

## Provenance

- Recorded 2026-10-09, 04:38:16–04:41:07 UTC (about 3 minutes; a 5-minute
  capture came to 830 KB, over the fixture's ~500 KB budget), with the
  binary of the PR that added the harness, against the live Binance
  endpoints of `ingest.example.toml`.
- Configuration: `ingest.example.toml` with
  `streams = ["aggTrade", "markPrice", "forceOrder", "kline_1m", "openInterest"]`
  (no depth, for size) and `[capture] state_checkpoint_interval_secs = 10`;
  every other key at its default (`hold_back_ms = 2000`,
  `oi_retime_allowance_ms = 10000`). A fresh raw root, so the run has no
  seeds. Ended with SIGTERM; `run_end` has exit code 0.
- Run `20261009T043816Z`: 1,741 records (aggTrade 1,179, kline_1m 375,
  markPrice 170, openInterest 17, forceOrder 0), 1,369 events delivered to
  the core, 0 domain rejections, 0 gaps, 18 state checkpoints, feature set
  `faac613e2a5ebbab`.

## Files

- `records.tsv`: every record of the run in `receive_seq` order, one per
  line after a header:
  `stream`, `receive_seq`, `receive_time_ns`, `session_id`,
  `event_time_ms`, `payload`, tab-separated. Payloads are verbatim; the
  exporter fails on a tab or a newline inside one.
- `journal.jsonl`: the run's `run_start`, `gap`, `domain_rejection`,
  `state_checkpoint` and `run_end` lines. Other lines are left out; `sealed`
  lines carry local paths.

## Re-recording

Re-recording is optional: after a feature-set change test (A) compares
events only, and the offline live-path test (B) keeps the state check.
To re-record:

1. Write an ingest config as above with a fresh `raw_root` and `journal`,
   and run `mie ingest --config <it>` for about 3 minutes; stop it with
   SIGTERM (for example `timeout -s TERM 180 mie ingest --config <it>`).
2. Check it: `mie equivalence --config <it> --from <today> --to <today>`
   must print `PASS`.
3. Export the run (its id is the `run_id` of the journal's `run_start`):

   ```sh
   MIE_FIXTURE_RAW_ROOT=<raw_root> MIE_FIXTURE_JOURNAL=<journal> \
   MIE_FIXTURE_RUN_ID=<run id> \
   MIE_FIXTURE_OUT=crates/mie-cli/tests/fixtures/equivalence \
   cargo test -p mie-cli --test equivalence_offline export_equivalence_fixture -- --ignored
   ```

4. Keep the directory under about 500 KB, update the counts above and the
   pinned `FIXTURE_*` constants of the test, and grep the fixture for local
   paths before committing.
