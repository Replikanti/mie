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
- The order book joined `MarketState` with #18 (ADR-043): the domain
  `OrderBook` (ADR-038) is the public feature `book.l2@1`, so its state
  hash covers every level, not only the derived book features. Before
  that, book events reached the engine only through the event-stream
  hash.

## Decision

1. **Market State hash, encoding v1 (D1).** `MarketState::state_hash`
   (`mie-domain`, `state_hash` module) is FNV-1a 64 over an explicit
   encoding written with the ADR-029 `Fingerprinter` writers: the header
   `write_str("mie-market-state")`, `write_u32(1)`, then every public field
   of `MarketState` in declaration order — `feature_set`, `as_of`,
   `last_trade_price`, `bars`, `motion`, `atr`, `regime`, `flow`, `profile`,
   `structure`, `derivatives`, `book`, `location`, `trade_count`. The rules:

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
   | enums | an explicit match with codes in declaration order: `Timeframe` M1 0 … D1 5; `RegimeLabel` Low 0 … Extreme 3; `Side` High 0, Low 1; `SweepOutcome` Pending 0, Sfp 1, Break 2; `book::Side` Bid 0, Ask 1; `LevelKind` Poc 0 … ValidatedReference 11; `AuctionState` InsideValue 0 … Acceptance 6; `LevelSide` High 0, Low 1, Bid 2, Ask 3; `Region` Below 0, In 1, Above 2; `Position` Below 0, LowEdge 1, In 2, HighEdge 3, Above 4; `Origin` Start 0, Acceptance 1 (ADR-044) |
   | `OrderBook` | the chain (`write_u8`: `Straddle` 0, `Chained` 1, `Invalid` 2; then the id as `u64` unless invalid), the trusted window (two `Option<Price>`), then the bids and the asks, each best first as `write_len` followed by price and quantity per level |
   | structs | every field, by exhaustive destructuring, in declaration order |

   Each encoder sits next to its type and destructures it without `..`, so
   a new field fails to compile until it is encoded. The hash covers all of
   `MarketState`, `trade_count` included: both sides of a comparison start
   from the same point (D4), so the counter is reproducible there even
   though it is not across replay windows. The engine's internal trackers
   (ATR window, flow, profile, structure, derivatives, liquidity and
   location trackers, the last event and ids) are excluded: the public state is the contract,
   and a divergence hidden in a tracker shows up in the public state at a
   later checkpoint. The order book is public state (`book.l2@1`), so it is
   hashed whole.
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
   with `run_end`. When the run's feature set is not the binary's, the
   engine may accept other events than live's did, and with them `as_of`,
   the end marker, where checkpoints fall and the domain rejections move;
   only the events are compared then: each journaled checkpoint's ordinal
   and event hash against the replay's event hash after as many events
   (`compare_events`), and the delivered events with `run_end`. Verdicts
   per run: `EQUIVALENT`; `DIVERGED` with the first divergent checkpoint;
   `EVENTS EQUIVALENT, STATE NOT COMPARABLE` when the run's feature set is
   not the binary's and its events match; `NOT COMPARABLE` for a run
   without a clean `run_end` (a crashed run's sealed prefix is not what
   live delivered before the crash), without checkpoints (journaled before
   this ADR), with another encoding, whose replay fails, or that delivered
   no event (no checkpoint to compare). The report has no wall-clock
   field, so a re-run prints identical bytes. The command exits 0 only
   when every selected run is `EQUIVALENT` and there is at least one, so
   at least one checkpoint was compared.
5. **CI coverage (D5).** `crates/mie-cli/tests/equivalence_offline.rs`,
   run by the `check` job's `cargo test --workspace`, offline:
   (A) a recorded live session of about 3 minutes (aggTrade, markPrice,
   forceOrder, kline_1m, openInterest; no depth, and short enough to keep
   the fixture under about 0.5 MB; checkpoints every 10 s) written into a
   temporary raw store must
   be `EQUIVALENT`; after a later feature-set change the test asserts
   `EVENTS EQUIVALENT, STATE NOT COMPARABLE` instead, so the fixture need
   not be re-recorded; (B) the same payloads played through the offline
   `mie ingest` composition in two runs against one store must be
   `EQUIVALENT` with state comparison; (C) a mutated trade quantity, a
   flipped journaled state hash and a dropped record must fail at the
   first affected checkpoint or at the integrity check; (G) the recorded
   live depth window (the adapter's `depth*.jsonl` fixture, one sync)
   played through the offline `mie ingest` composition with checkpoints
   every second must be `EQUIVALENT` with state compared, and a mutated
   level quantity in one recorded diff and a flipped state hash must
   diverge at the first affected checkpoint (ADR-043).

## Consequences

- Equivalence is a measured property: every live run journals the states
  it reached, and any later binary with the same encodings and feature set
  can check them against the raw store.
- The state hash encoding is a contract. Golden values pin it; explicit
  enum codes and exhaustive destructuring keep it from drifting silently.
- Every feature-set change updates the state-hash golden values and moves
  fixture test (A) to its events-only branch; state coverage then rests on
  test (B) and the live run of *Accept when*, within the limits stated
  there: each run starts a cold engine, so features with a long warm-up
  are covered by the CI tests alone.
- Every delivered event is hashed on the core thread (FNV over a few
  hundred bytes, a few KB for a depth diff), and the state once per
  interval. The live run's channel blocked time checks the cost (0 ms
  required); its channel high-water is reported, not gated (*Accept when*).
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

The criterion is a **coverage list**, not a duration. One or more
`mie ingest` runs on the merged binary, all seven streams, each ended by
SIGTERM (a clean `run_end`; without one the run is `NOT COMPARABLE`, D4),
together cover every item below, and `mie equivalence` over them reports
every run `EQUIVALENT` with state compared. A second `mie equivalence`
over the same runs prints a byte-identical report (the report has no
wall-clock field, D4). A missing coverage item means the run is extended
or repeated, never waived.

**Why not "at least 24 hours".** The earlier criterion was copied from
ADR-032, where 24 h existed to reach the 23 h planned rotation
(`max_connection_age_secs` 82 800 s). Neither soak reached a rotation:
#9 (about 30 h) ended its connections by a restart and an outage, and #10
(13 h 41 min) saw 12 unplanned disconnects on this host, each of which
reset the connection age (ADR-032 and ADR-038, *Acceptance*). A longer
soak therefore adds no divergence boundary that a targeted run lacks.
Rejected: a longer soak (the age
still resets); fault injection in the capture path (code surface that
exists only for the test, while the existing knob below already provokes
the gap).

**Coverage list.** Each item is a divergence boundary: a place where live
and a recompute from the raw store could part. Each is verified from the
run's journal and raw store, and the verification is printed in the
acceptance record.

1. **A restart, compared separately.** Two runs, the first ended by SIGTERM
   and the second started afterwards, each compared alone (D4: no
   chaining, because a fresh engine per run is what live did). The restart
   also yields a `Disconnected` seed gap per stream (ADR-032 D5).
2. **A `Disconnected` gap inside a run**, beyond the seed gap. Luck does not
   provide one on a schedule, so the run sets
   `[capture] max_connection_age_secs = 300`. This **deviates from the
   defaults** (82 800 s); `rotation_stagger_secs` stays 300 s. The value
   is the one that already exercised rotation in ADR-032 and ADR-038: all
   streams rotated, one recorded gap each, depth resynced in 0.4 s. With
   the default, no rotation falls inside a run of hours. Evidence: a
   `planned_rotation` journal record followed by a `gap` record with
   `reason` `Disconnected` on that stream.
3. **A book desync followed by a resync, and a `LateEvent` gap.** The
   depth rotation of item 2 gives the desync deterministically: a `book`
   record with `event` `desync`, then one with `event` `sync`. A `LateEvent`
   gap (`gap` record, `reason` `LateEvent`) is not provoked by any setting.
   Baselines for how likely one is: 1 051 late aggTrades in the ~30 h of #9
   (ADR-032, *Acceptance*), but 3 late events (aggTrade 1, depth 2) in the
   13 h 41 min of #10 (ADR-038, *Acceptance*), about 0.22 per hour, so about
   0.7 expected in 3 h. Late events come in bursts, so the odds of at least
   one in a short run are well below certain; the extend-or-repeat rule
   covers that.
4. **The 00:00 UTC boundary inside a run.** It is the raw store's date
   partition boundary (`date=YYYY-MM-DD`, ADR-030), the reset point of the
   UTC-day features (`profile.volume.utc_day@1`, `flow.cvd.utc_day@1`,
   `location.vwap.utc_day@1`; `profile.rs`, `flow.rs`, `location/vwap.rs`),
   the switch of `location.auction.prior_day@1` to the new prior day
   (ADR-044 D2), and a funding settlement time (every 8 h,
   `docs/data-availability.md`), the instant `next_funding_time` of the mark
   price moves on (ADR-042 D3).
5. **Liquidations present early enough for their windows to turn `Ready`
   in-run.** The windows count closed 1m bars from the first
   liquidations-stream event on (ADR-042 D5): `Samples(5)`, `Samples(15)`
   and `Samples(60)`. The 1h window therefore needs 60 closed minutes after
   the first liquidation in the run. Baseline: 17 late forceOrder events
   were 0.95 % of that stream in #9, so about 1 800 events in ~30 h (ADR-032,
   *Acceptance*), roughly one a minute, enough that the first one arrives
   within minutes. Evidence: the raw store's `forceOrder` records and the
   run's start time.

**What the run cannot cover.** Each run starts a cold engine
(`MarketStateEngine` per run, D4), so any feature whose warm-up is longer
than the run stays `WarmingUp` in both hashes and is compared as such. The
list, derived from the code (the values below are the constants in the tree):

- `volatility.atr.1h@1`: `ATR_LENGTH` = 14 closed hourly bars.
- `volatility.regime.1h@1`: `REGIME_WARM_UP` = 214 hourly samples
  (`ATR_LENGTH` 14 + `REGIME_LOOKBACK` 200, `volatility.rs`), about 8.9
  days.
- `profile.volume.composite_5d@1`: `COMPOSITE_SESSIONS` = 5 completed UTC
  days (`profile.rs`), which no run of hours reaches.
- `structure.swing.<tf>@1` and `structure.levels.<tf>@1`: `Samples(7)`
  closed bars of the timeframe (ADR-037 D10). That is 105 minutes at 15m,
  7 h at 1h, 28 h at 4h and 7 days at 1d; only the 15m timeframe can warm
  up in a run of about 3 h, and 4h and 1d cannot in any run of a day.
- `derivatives.funding.settled@1`: archive only, no live settlement event
  (ADR-042 D4), so it stays `WarmingUp` live and cannot be compared here.
- `location.auction.prior_day@1` (ADR-044): it needs a prior day, so a run
  reaches it only after its 00:00 UTC boundary (coverage item 4), and then
  against a partial prior day, the run's part before the boundary. The
  classifier is compared from there on; a full prior day is covered by the
  fixture tests only.

For these, coverage rests on the fixture tests (A), (B) and (C) of D5 and
on the pinned `state_hash` golden values, not on the live run. A feature
whose warm-up the run does reach (bars, flow, the profile, 15m structure,
the 5m/15m/1h liquidation windows, OI, mark price, the UTC-day VWAP and the
location level registry with its book clusters) is covered by it.

**Capacity gate.** The run's `stats` journal records carry the channel
`high_water` and `blocked_ms` (`journal.rs`).

- **Blocked time must be 0 ms**: the hard gate. Blocked time is the time
  senders spent blocked on a full channel (`ChannelStats::blocked_ns`); any
  blocking means the core thread could not keep up with the capture, and
  this ADR adds hashing to that thread (*Consequences*). It is 0 ms in both
  soaks: #9 (ADR-026, *Accept when*) and #10 (ADR-038, *Acceptance*). The
  channels hold 65 536 messages, about 87 times the largest high-water
  seen, so a single blocked millisecond is already far outside anything
  observed.
- **High-water is reported, not gated.** Baselines as context: #10, all
  seven streams as in this run, core 714 and inbound 488 of 65 536 (ADR-038,
  *Acceptance*); #9, trades only, at most 753 of 65 536 (ADR-032,
  *Acceptance*, ADR-026). 753 is the largest of two observations, not a
  derived limit, and a run of hours sees fewer bursts than a run of ~30 h.
  A hard bound at 753 would fail on an ordinary burst and prove nothing
  about capacity, while a bound near 65 536 would only trip when the
  channel is already full, which blocked time reports anyway. The value is
  recorded in the acceptance record.

**Example schedule (illustrative; the coverage list decides).** Start the
first run at about 22:30 UTC, restart at about 23:15, stop the second run
at about 01:30. Derivations: the restart at 23:15 leaves 45 minutes of
run 1 before the restart and 45 minutes of run 2 before 00:00, so both runs
hold pre-boundary state, and run 2 spends 90 minutes after 00:00; a 1h liquidation window in run 2 turns `Ready` about 60 minutes
after its first liquidation, that is at about 00:15 to 00:20 if the first
one arrives within a few minutes, which leaves about 70 minutes of `Ready`
1h window before 01:30, and the 5m and 15m windows are `Ready` across the
boundary. Run 1 (45 minutes) is too short for the 1h window to turn
`Ready`, which is why item 5 is checked in run 2. A schedule that does not
meet the list is extended, not accepted.
