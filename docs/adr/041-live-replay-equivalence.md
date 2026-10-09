# ADR-041: Live/replay equivalence — Market State hash and checkpoints

- Status: proposed
- Date: 2026-10-09

## Context

ADR-019 requires live analysis and historical replay to drive the same
domain code and reach the same results. #13 makes that measurable: a live
session must be replayable from the raw store with zero divergences, and a
mutated event must be caught at the first checkpoint it affects.

The parts already exist. Live ingestion and replay share one drive policy
(`mie_app::drive_tolerant`, ADR-039 D9) and one pipeline: replay recomputes
each live run with the `Pipeline` live used and the run's journaled
parameters (ADR-032 D4, ADR-039 D1). Event-stream hash v1 identifies a
delivered event sequence (ADR-039 D7). What was missing is an identity for
the **state** the engine reaches, a record of the states live reached while
it ran, and a comparison.

Two facts shape the comparison:

- `mie ingest` starts a fresh `MarketStateEngine` for every run, while
  `mie replay` chains the runs of a window into one engine (ADR-039 D3) and
  restricts each run's output to the window (D4). A replay window that
  spans a restart therefore reaches other states than live did, by design.
- `MarketState` has no order-book fields yet: the domain `OrderBook`
  (ADR-038) joins the state with #18. Book events reach the engine and are
  covered by the event-stream hash.

## Decision

1. **Market State hash, encoding v1 (D1).** `MarketState::state_hash`
   (`mie-domain`, `state_hash` module) is FNV-1a 64 over an explicit
   encoding written with the ADR-029 `Fingerprinter` writers: the header
   `write_str("mie-market-state")`, `write_u32(1)`, then every public field
   of `MarketState` in declaration order — `feature_set`, `as_of`,
   `last_trade_price`, `bars`, `motion`, `atr`, `regime`, `flow`, `profile`,
   `structure`, `trade_count`. The rules:

   | Type | Encoding |
   |---|---|
   | `EventTime` | `write_i64` of the epoch ms |
   | `Price`, `Qty`, `Rate` | `write_i64` of the units (ADR-027) |
   | `u64` / `u32` / `bool` | `write_u64` / `write_u32` / `write_u8` (0, 1) |
   | `f64` | `write_u64` of the bit pattern (`-0.0` ≠ `0.0`) |
   | `Option<T>` | `write_u8` 0 for `None`; 1, then the value |
   | `FeatureValue<T>` | `write_u8` 0 `WarmingUp` + `observed`, `required`; 1 `Ready` + value; 2 `Unavailable` + reason (`InputInvalid` 0, `OutOfRange` 1) |
   | arrays, `Vec` | `write_len`, then the elements in stored order |
   | `FeatureKey` | the ADR-029 encoding: id string, version `u32` |
   | `FeatureSetVersion` | `write_u64` of the fingerprint |
   | enums | an explicit match with codes in declaration order: `Timeframe` M1 0 … D1 5; `RegimeLabel` Low 0 … Extreme 3; `Side` High 0, Low 1; `SweepOutcome` Pending 0, Sfp 1, Break 2 |
   | structs | every field, by exhaustive destructuring, in declaration order |

   Each encoder sits next to its type and destructures it without `..`, so
   a new field fails to compile until it is encoded. The hash covers all of
   `MarketState`, `trade_count` included: both sides of a comparison start
   from the same point (D4), so the counter is reproducible there even
   though it is not across replay windows. The engine's internal trackers
   (ATR window, flow, profile and structure trackers, the last event and
   ids) are excluded: the public state is the contract, and a divergence
   hidden in a tracker shows up in the public state at a later checkpoint.
2. **Versioning and comparability (D2).** Two state hashes are comparable
   only when their encoding (`STATE_HASH_ENCODING`) and their
   `FeatureSetVersion` are both equal. A new feature family — a new
   `MarketState` field — changes the feature-set version anyway (ADR-029);
   it extends the encoder in the same PR without an encoding bump, and
   updates the pinned golden values ("Adding a feature" step 6). Changing
   how an already-encoded field is written (its writer, an enum code or the
   order) bumps `STATE_HASH_ENCODING`.
3. **Checkpoints (D3).** One recorder (`mie_app::equivalence`) serves live
   and replay: `drive_checkpointed` is `drive_tolerant` plus a
   `CheckpointRecorder`. A checkpoint is recorded after the event that
   moves the engine's `as_of` into a later `as_of.div_euclid(interval)`
   bucket; the first accepted event only sets the bucket, a jump over
   several buckets records one checkpoint, and a rejected event — which
   does not move `as_of` — never records one. After the final event of a
   run without a provider failure a `last` checkpoint is recorded, unless
   that event already recorded one. A checkpoint holds the ordinal (events
   delivered so far, rejections included: the `run_end.events` count),
   `as_of`, the event-stream hash v1 of the delivered events and the state
   hash. The interval is event time, `[capture] state_checkpoint_interval_secs`,
   60 s by default (about 1,440 checkpoints per day). `mie ingest` journals
   each checkpoint as
   `{"type":"state_checkpoint","run_id","at_ms","ordinal","as_of","event_hash","state_hash","last"}`
   (`as_of` in ms or null, hashes as 16 hex digits), and `run_start` gains
   the comparability keys `state_checkpoint_interval_ms`,
   `state_hash_encoding`, `event_hash_encoding` and `feature_set`. They are
   not pipeline parameters; ADR-032 D4 is unchanged.
4. **The comparison unit is one clean live run (D4).** `mie equivalence`
   selects the runs that started in `[from, to)` and recomputes each one
   alone with `LiveReplay::run_replay`: the run's records over its extent,
   the D2 integrity rules of ADR-039, the run's `Pipeline` from its first
   record, **no chaining and no window** — exactly what that run's live
   provider delivered — into a fresh engine through `drive_checkpointed`
   at the run's journaled interval. The two checkpoint lists are compared
   in order, and the first difference is reported with its kind:
   `EventStream` (ordinal, `as_of`, event hash or end marker differ),
   `State` (equal events, different state) or `Missing` (one list ends
   early). The delivered events and domain rejections are also compared
   with `run_end`. Verdicts per run: `EQUIVALENT`; `DIVERGED` with the
   first divergent checkpoint; `EVENTS EQUIVALENT, STATE NOT COMPARABLE`
   when the run's feature set is not the binary's (state hashes are then
   ignored and nothing else); `NOT COMPARABLE` for a run without a clean
   `run_end` (a crashed run's sealed prefix is not what live delivered
   before the crash), without checkpoints (journaled before this ADR), with
   another encoding, or whose replay fails. The report has no wall-clock
   field, so a re-run prints identical bytes. The command exits 0 only
   when at least one run was compared and every selected run is
   `EQUIVALENT`.
5. **CI coverage (D5).** `crates/mie-cli/tests/equivalence_offline.rs`,
   run by the `check` job's `cargo test --workspace`, offline:
   (A) a recorded live session of about 5 minutes (aggTrade, markPrice,
   forceOrder, kline_1m, openInterest; no depth, to keep the fixture near
   0.5 MB; checkpoints every 10 s) written into a temporary raw store must
   be `EQUIVALENT`; after a later feature-set change the test asserts
   `EVENTS EQUIVALENT, STATE NOT COMPARABLE` instead, so the fixture need
   not be re-recorded; (B) the same payloads played through the offline
   `mie ingest` composition in two runs against one store must be
   `EQUIVALENT` with state comparison; (C) a mutated trade quantity, a
   flipped journaled state hash and a dropped record must fail at the
   first affected checkpoint or at the integrity check.

## Consequences

- Equivalence is a measured property: every live run journals the states
  it reached, and any later binary with the same encodings and feature set
  can check them against the raw store.
- The state hash encoding is a contract. Golden values pin it; explicit
  enum codes and exhaustive destructuring keep it from drifting silently.
- Every feature-set change updates the state-hash golden values and moves
  fixture test (A) to its events-only branch; state coverage then rests on
  test (B) and the soak.
- Every delivered event is hashed on the core thread (FNV over a few
  hundred bytes, a few KB for a depth diff), and the state once per
  interval. The soak's channel high-water and blocked time check the cost.
- A replay window spanning restarts still cannot be compared with live
  state; that would need warm-starting the live engine from history.
- One run is held in memory while it is compared (about 1.5 M records per
  24 h), as for `mie replay`.

## Alternatives considered

- **Compare live checkpoints with `mie replay` windows.** The chained
  engine and the window trimming would report a false divergence after
  every restart, and `LiveReplay::replay` fails on in-window records of
  runs missing from its run list.
- **Hash the engine's internal trackers.** That would tie the hash to
  implementation details that may change without a feature change; the
  public `MarketState` is the contract.
- **Hash `Debug` output or derived `Hash`.** Neither is stable across Rust
  releases (ADR-029).
- **Re-record the CI fixture in every feature PR.** It needs network access
  and adds about 0.5 MB of history each time; the offline live-path test
  keeps the state check instead.

## Accept when

A `mie ingest` soak of at least 24 hours with all seven streams, on the
merged binary and ended by SIGTERM, passes `mie equivalence` with every run
`EQUIVALENT` and state compared; the report of a second run is
byte-identical; and the soak's core channel high-water and blocked time
stay in line with the #9 soak.
