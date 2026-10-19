# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is below 1.0, a breaking change bumps the minor version and
is marked **Breaking:** under `Changed`. Releases are cut as described in
[`docs/release.md`](docs/release.md).

## [Unreleased]

### Added

- **ADR-053 is `accepted` (#24 M4).** The *Accept when* measurement on the
  #12 year and the ADR-051 R0 live store passed: 603 323 345 events, 0
  mismatches, every archive-reachable trigger id fired. The measured gate
  pass rates (88.9–94.8 %) and shared-boundary shares (17.9 / 3.2 / 1.2 % of
  15m / 1h / 4h SFPs) are recorded in the ADR. No code changes.
- **OI expansion followed by rejection and the trigger measurement tool
  (ADR-053 D7, part of #24).** `trigger.oi_expansion_rejection.<15m|1h|4h|1d>@1`
  fire on the SFP of their timeframe and
  `trigger.oi_expansion_rejection.prior_day@1` on the failed auction, when
  the `derivatives.oi.5m@1` point at the rejection lies on a later
  boundary than the point at the excursion's start (the sweeping trade, or
  the close where the failed probe started or retargeted), with the same
  resolution and strictly higher open interest; no magnitude threshold.
  The point at an event is the ready point of the boundary 25 s before it
  (`oi_lag_ms`), so the archive and a continuous live feed select the same
  boundaries; this replaces the 360 000 ms age bound ADR-053 first stated.
  Each OI trigger directly follows its rejection's trigger with the same
  anchor, context and bias, and carries the rejection, both points and the
  change (`OiExpansion`, `OiRejection`, `TriggerEvidence::OiExpansion`).
  The OI values are compared by `Triggers::equal_but_registry`. ADR-053 is
  amended (D1, D7, D8, Consequences, *Accept when*) and stays `proposed`.
  New ignored tests `measure_triggers` and `measure_triggers_live` in
  `crates/mie-cli/tests/trigger_measure.rs` check the engine's triggers
  against an independent model of ADR-053 D3–D7 and report the M4
  measurements; their synthetic-tape twins run in CI. Every state hash and
  the feature-set version change again; the pre-#22, pre-#24 and pre-#96
  pins hold.
- **Candidate trigger rules (ADR-053 D4/D6, part of #24).**
  `trigger.cvd_divergence.<15m|1h|4h|1d>@1`,
  `trigger.effort_result.<5m|15m|1h>@1`, `trigger.exhaustion@1` and
  `trigger.absorption.<5m|15m|1h>@1` (live only) fire on the order-flow
  candidates of ADR-051 at a defined location: the context the rule reads
  must be ready and list a level of an admitted kind (every kind but
  liquidity clusters, prior sweeps and validated references, named in the
  `gate_kinds` parameter). A CVD divergence's gate reads the registry at
  its current swing without every level of either swing of its pair, on
  any structure timeframe, and its trigger carries that context; the other
  families gate on the candidate's own context. Each trigger carries the
  full candidate event as evidence; each value holds the last trigger with
  the candidate's hashed value and follows its candidate's validity.
  `TriggerEvidence` gains `Candidate` and is no longer `Copy`. ADR-053 is
  amended (D4, D6, D8) and stays `proposed`. Every state hash and the
  feature-set version change again; the pre-#22, pre-#24 and pre-#96 pins
  hold. The ignored `location_measure` rule-I check now compares only the
  bias and the auction and SFP trigger values, since the candidate trigger
  values depend on the selected registry by design.
- **Bias context and the auction and SFP trigger rules (ADR-053, part
  of #24).** `bias.context@1` records the 1h volatility regime and, per
  structure timeframe and side, the step of the last swing pair
  (`higher`, `equal`, `lower`): explicitly non-authorizing, with no
  aggregate or direction, and read by no trigger rule
  (`Bias::read(&MarketState)` is public). `trigger.failed_auction@1`,
  `trigger.failed_reclaim@1` and `trigger.breakout_acceptance@1` fire on
  the auction facts of `location.auction.prior_day@1`, read from each
  fact's own status and anchored at the crossed or accepted value edge;
  `trigger.sfp.<15m|1h|4h|1d>@1` fires on each SFP, anchored at the swept
  level, its location context without the swept swing's levels on any
  structure timeframe (same side and price, nested swing bar). A new
  last stage of the engine emits them through
  `MarketStateEngine::trigger_events`, each with the rule's feature key,
  its family, the evidence fact, a location context and a bias snapshot;
  the bias and the last trigger of each id are appended last to
  `MarketState` as `triggers`. Every state hash and the feature-set
  version change; the pre-#24 hashes are pinned. ADR-053 is `proposed`
  and also fixes the candidate (M2) and OI-expansion (M3) rules that
  follow.
- **`mie equivalence` compares warm-started runs on the history chain
  (ADR-052 D9, #118).** A run whose `warm_start` is warm is checked against
  one pass over the chain of the configuration's `[history]`: the parts
  before it must reproduce the journaled prefix event-stream hash and
  handoff state hash, else `DIVERGED (warm prefix)` with the journaled and
  the current prefix dataset version printed (never compared: a fast
  restart can move the version without a data change); its own events, the
  chain's recompute of the run, then drive the same engine with a fresh
  recorder and are compared as ADR-041 decision 4 compares a run. A warm
  run under another origin or cutover, or without `[history]`, is `NOT
  COMPARABLE`; under another feature set it compares the events only.
  The report gains `history origin <ms> cutover <ms>: one pass through run
  <id>` and a warm run line with its prefix events. Cold runs are compared
  and printed as before. New: `HistoryChain::to_end_of`,
  `HistoryChainStream::run_of_last_event` and `failed_run`,
  `LiveReplayStream::run_of_last_event` and `failed_run`, and
  `mie_app::equivalence::RunParts` and `compare_prefix`. ADR-041's status
  line records the superseded comparison unit.
- **`mie ingest` warm-starts from the history chain while the capture runs
  (ADR-052 D3, D6–D8, D13, #117).** With `[history]`, a run that starts at
  or after the cutover starts its capture at once, with the core fed the
  raw records (`CoreFeed::Records`, `BinanceLiveProvider::next_record`),
  and replays the chain through its predecessor on a `mie-warm-up` thread
  while the core queues the records. At the handoff the queue, then the
  live records, are recomputed with the chain seeds through the new
  `RunContinuation`, the one recompute the chained replay now uses too,
  joined to the prefix, and drive the warm engine through the new
  `mie_app::equivalence::drive_chain_checkpointed` with a fresh checkpoint
  recorder. New journal records: `warm_start` (the prefix's dataset
  version, event-stream hash, rejections and effective origin, the handoff
  state hash, the chain seeds, the queue's high-water in records and bytes,
  the long-warm-up features still warming up; or the cold reason
  `no-history`, `before-cutover` or `warm-up-failed` with its error),
  `warm_ready` (`ready_after_ms`) and `chain_restart`; `run_start` gains
  `history_origin_ms` and `history_cutover_ms`. A warm run's replay takes
  its seeds from `warm_start`. The memory guard reads the new optional
  `[history] max_run_records`, which has no default: without it every
  chain run starts cold, with the reason journaled. The same limit bounds
  the warm-up's record queue (what it leaves beside the largest replayed
  run); a queue past it abandons the warm-up. `chain_restart` is journaled
  and synced when the restart happens (`drive_chain_checkpointed` takes an
  `on_restart` callback). A failed, refused, overflowing or unfinished
  warm-up starts the run cold; the capture never waits for it.
  Runs without `[history]` capture and feed the core as before. `mie
  equivalence` compares warm runs from #118 on (below).
- **Research on the history chain: `research.replay_summary@2` and
  `mie experiment run --source history` (ADR-052 D10, #119).** With an
  ingest configuration that has `[history]`, the run opens the sample on
  the history chain and checks its data version, drives the chain's prefix
  `[origin, sample start)` into the engine without evaluating it, then
  replays the sample into the same engine as `@1` does, both through
  `mie_app::drive_chain`. The new outcome kind `history-replay-summary`
  records the origin, the effective origin, the cutover, the prefix's
  dataset version, event-stream hash and rejections, the engine key, and the
  state hashes at the sample's start and end. A `@2` result is keyed by
  experiment, pipeline and engine (feature-set version and state-hash
  encoding), stored as `<pipeline id>@<N>-<feature-set hex>-<state
  encoding>.result`, so a build with another engine records next to the
  old result. A sample before the origin fails. `--source live|archive`,
  `@1`, its keys and result text are unchanged.
- **The history chain and `mie replay --source history` (ADR-052, #116).**
  An optional `[history]` section of the ingest configuration (`origin`,
  `cutover`, `archive_config`; the archive configuration must share the raw
  root and the symbol) names the chain: the archive backfill from the origin
  up to the cutover (default streams, `aggTrades` folded), then the
  journaled capture runs that started at or after the cutover, the first
  continuing after the archive's last event as runs continue after each
  other. Every chain run is recomputed with the chain's newest event of
  each series as its seed when its journaled seed does not reach back that
  far, so every hole, the seam included, opens with a `Disconnected` gap,
  also when the archive's last days are missing. A run the cutover falls
  inside is refused by name. Every request
  replays from the origin and is restricted to its window, so adjacent
  windows concatenate exactly; the report has one line per segment, and the
  chain's dataset version covers both. An uncrossable hole (more than
  `MAX_BARS_PER_EVENT` one-minute bars) restarts a fresh engine through the
  new `mie_app::drive_chain`; each restart and the effective origin are
  printed. `--source live|archive` reports are unchanged; `mie experiment
  run` refused `--source history` until #119 (above).
- **Order-flow candidates: effort vs result, exhaustion and absorption
  (ADR-051, #22).** `candidate.effort_result.<5m|15m|1h>@1` judges each
  aggression window, once per closed minute, against its own previous
  1 440 evaluations: aggression at or above the 0.800 quantile with no more
  progress than the median move is a candidate. `B` = 1 440 and `q_hi` =
  0.800 were fixed by the pre-registered rules R1b and R1 on the hold-out
  year 2024-10-01 … 2025-09-30. `candidate.exhaustion@1` is a 1h expansion
  whose 5m aggression decayed to at most its median while the last close
  stays within 6 bps of the extreme; `candidate.absorption.<5m|15m|1h>@1`
  (live only) is effort vs result with the passive side opposite the
  aggressor replenished (fills and `added ≥ filled` within 5 bps; a depth
  outage, which books no fills, is not evaluated). Each emits on the
  rising edge, with a location context at the last close, through
  `MarketStateEngine::candidate_events`; values are part of
  `MarketState.candidates`. Every state hash and the feature-set version
  change. The `candidate_measure` tool checks all three against its model
  (R0), on archive replays and, with `measure_candidates_live`, on live
  captures.
- **Order-flow candidates: CVD and open-interest divergences (ADR-051,
  #22).** `candidate.divergence.cvd.<15m|1h|4h|1d>@1` and
  `candidate.divergence.oi.<15m|1h|4h|1d>@1` compare consecutive confirmed
  swings of one side at their anchors, the first trade that set each swing
  bar's extreme: a further extreme with the CVD the other way, or (on
  either side) with lower open interest, is a candidate. No magnitude
  threshold; each comparison carries its price, CVD and OI changes, the
  trades gaps between the anchors and the span. Values are appended last
  to `MarketState` as `candidates`; the facts, with a location context
  (registry levels at the swing price and the auction state), are exposed
  by `MarketStateEngine::candidate_events`. Candidates carry no direction
  and never gate on location.
- **`--trade-correction fold|none` (ADR-050, #102)** on `mie replay`,
  `mie experiment run` (both with `--source archive`) and
  `mie archive-kline-check` (with `--trade-source aggTrades`). `none`
  delivers `aggTrades` exactly as published and reproduces the dataset
  versions and event-stream hashes recorded before ADR-050; `mie replay`
  prints a fold summary, one line per corrected day and one per corrected
  1m minute.
- **`capture-report` check 7, kline reconciliation (ADR-048).** Closed
  `kline_1m` minutes and `aggTrade` records are reconciled in trade-id
  space per run; a block of minutes whose kline volume differs from its
  aggregate quantity fails the report, with one line per block and a
  totals row. A window with a trade lost inside an aggregate — which keeps
  the aggregate id consecutive and journals no gap — now FAILs, the #9 soak
  window included (2026-10-08, 43.250 BTC). Skipped unless both streams are
  configured.
- **Order-book liquidity state (ADR-043).** The engine owns the L2 book as
  `book.l2@1` and hashes it whole in the Market State; `book.depth@1` (best
  levels, depth and imbalance within 1, 2 and 5 bps of mid),
  `book.clusters@1` (the five largest levels per side within 5 bps with the
  median level quantity) and `book.liquidity.window.<5m|15m|1h>@1`
  (liquidity added, cancelled and filled per side and band, fills matched
  with taker trades). Live only.
- **Location and auction state (ADR-044).** `location.vwap.utc_day@1` (the
  UTC-day VWAP over closed minutes), `location.levels@1` (a level registry
  over the prior-day and 5-day profiles, the structure registries, the book
  clusters and the VWAP, with distance, touches, age, source, confluence
  and cluster strength per level, and Entered/Left zone events) and
  `location.auction.prior_day@1` (the auction state against the prior day's
  value: inside value, at the edge, outside, breakout, failed breakout,
  failed reclaim, acceptance after 60 closes beyond the edge). Transitions
  and zone events are exposed by `MarketStateEngine::location_events`.
  `crates/mie-cli/tests/location_measure.rs` measures the numbers behind
  the tolerance and the acceptance time over the archive.
- **7-day composite profile and `location.levels@2` (ADR-036 decision 2,
  ADR-044 D5, D10).** `profile.volume.composite_7d@1`, the bin-wise sum of
  the last 7 completed UTC days (a 7-day window holds every weekday once),
  and `location.levels@2`, the level registry over the prior-day and 7-day
  composite profiles. `MarketStateEngine::with_features` builds an engine
  for the default set with an older version selected instead, today
  `location.levels@1`, which stays computable; it refuses a set whose
  definitions differ from the catalog's. The experiment service builds its
  engine with it, so a spec on `location.levels@1` runs and reproduces
  (ADR-040 D4).

### Changed

- **Breaking: archive replays, experiments and the aggTrades kline check
  fold by default (ADR-050, #102).** Trades the `aggTrades` archive omits
  (2025-10-01 … 2026-06-09, ADR-047) are restored from imported `trades`
  files: each is added to the quantity of the aggregate right below it,
  also across a day boundary, or becomes a `MissingData` gap when its price
  or side differs, when an earlier trade of the same gap did not fold, or
  when the aggregate below lies in a day the replay does not have
  (`unresolved`); a gap the window ends inside is a trailing gap. The day
  after the window is read for the tail of the window's last aggregate and
  never delivered, so a folded quantity does not depend on where the window
  ends; when the store lacks that day's `trades` or `aggTrades`, the tail is
  a trailing gap and a `fold unchecked tail` report line. Where
  `trades` files exist for a window, its archive dataset version and
  event-stream hash change; experiment specs pinned to an earlier version
  run with `--trade-correction none`, and a mismatch under the fold prints
  that hint. `ArchiveReplay::new`, `with_defaults` and `open_requests` take
  an explicit `TradeCorrection`. Reports of `mie replay --source archive`
  and `archive-kline-check` name the correction in their first line.
- **Breaking: the order-book checkpoint audit has a per-side level floor
  and an exact window width (ADR-049, #98).** A checkpoint without a
  mismatch whose bid or ask side compared fewer than `MIN_SIDE_LEVELS` (1)
  levels is now `inconclusive` instead of `matched`; a mismatch still takes
  precedence. `capture-report` counts `inconclusive` on the order-book row,
  does not fail on it, and does not count it as `matched` for the presence
  rule. The window width is measured per side from the fresh best to the
  window edge, in hundredths of a bp, rounded up, and is 0 when the edge
  lies beyond the best price (before: the absolute distance, truncated to
  whole bps). In `book_checkpoint` journal lines, `window_bid_bps` and
  `window_ask_bps` are replaced by `window_bid_cbps` and `window_ask_cbps`,
  and `levels_bid` and `levels_ask` are added (`inconclusive` lines also
  carry `min_side_levels`). Older lines are judged by their `result`, and
  their whole-bp widths stay out of the window statistic. Over the #10 soak
  the audit now gives 809 `matched` and 3 `inconclusive` (an empty side
  after the price outran the previous window), still PASS.

- **The default level registry reads the 7-day composite (ADR-044 D5).**
  `location.levels@2` replaces `@1` in the default feature set, and
  `profile.volume.composite_7d@1` joins it; `composite_5d@1` stays computed.
  The feature-set version and the state-hash goldens move; no
  `STATE_HASH_ENCODING` bump (ADR-041 D2). Runs recorded before this change
  compare events only (ADR-041 D2).

- **`archive-kline-check` reports coverage and an INCONCLUSIVE verdict
  (ADR-045).** A coverage line (window bars compared, bars skipped as
  incomplete, klines without a bar) precedes the verdict. A run with no
  mismatch prints `INCONCLUSIVE` and exits 3 instead of passing when it
  compared fewer bars than the window
  holds (1855 per day), for example because trade or kline days are
  missing at the window's edges, or when it skipped more than the six
  partial-start bars (one per timeframe). Exit 0 (`PASS`) and 1 (`FAIL`)
  keep their meaning.

### Fixed

- **`archive-kline-check --trade-source trades` compared almost no bars
  (#85, ADR-045).** The archive `trades` id space is sparse by design (the
  exchange skips ids its klines do not count), so an id jump in that
  dataset is no longer a `SequenceBreak` gap. Live capture and archive
  `aggTrades` keep the strict rule.
- **Open-interest polling backs off from rate limits (ADR-046, #84).** On a
  418 or 429 the poller honours `Retry-After` (seconds, capped at 3 days);
  without the header it pauses 60 s after a 429 (one `REQUEST_WEIGHT`
  window) and 120 s after a 418 (the shortest ban). Before, the reconnect
  backoff was swallowed by the 10 s poll slot and polling went on into the
  rate limit. Successful polls stay on the 10 s grid.

### Security

- **`zip` 8.6 → 9.0 (RUSTSEC-2026-0367, -0368, -0369).** The three
  advisories concern `extract()` (SUID/SGID bits, symlink targets and
  permissions through symlinks outside the destination), which mie never
  calls: the archive reader opens the one CSV entry with `by_index(0)` and
  streams it. `cargo deny check` fails on them all the same. In 9.0 an
  entry's `name()` returns a `Result`; a name that does not decode is now a
  `FetchError::Zip` like any other unreadable archive.

## [0.1.0] - 2026-10-09

### Added

- **Hexagonal workspace with an enforced crate graph (ADR-025).** `mie-domain`
  (pure, deterministic core), `mie-ports`, `mie-app`, adapter crates and the
  `mie-cli` composition root, checked by `tools/check-architecture.sh`.
- **Domain core.** Exact decimal parsing and fixed-point prices and quantities
  (ADR-027), event time and a canonical multi-stream order (ADR-028), feature
  identity and versioning (ADR-029), event-time bars on six timeframes
  (ADR-031), ATR(14) percentile regime (ADR-033), order-flow CVD and rolling
  aggression windows (ADR-035), volume profile with POC, value area, HVN and
  LVN (ADR-036), market structure with swings, levels, sweeps and SFP
  (ADR-037) and derivatives context: open interest, funding and liquidations
  (ADR-042).
- **Immutable raw store (ADR-030).** Verbatim messages in sealed Parquet files
  with manifests and dataset versions.
- **Binance live capture: `mie ingest`, `mie capture-report` (ADR-032,
  ADR-038).** Raw-first persistence, shared normalization, feed gaps,
  canonical merge, and order-book sync from depth diffs and REST snapshots
  with audited checkpoints.
- **Binance public-archive backfill: `mie archive-import`,
  `mie archive-verify`, `mie archive-kline-check` (ADR-034).**
  Checksum-verified, exactly-once import (re-runs skip imported files, an
  interrupted file resumes) and a cross-check of bars against archive klines.
- **`mie replay` (ADR-039).** Replays a window of the raw store through the
  core; the report carries the dataset version and the event-stream hash, and
  the same window prints the same bytes.
- **`mie equivalence` (ADR-041).** Recomputes every capture run alone into a
  fresh engine and compares its state checkpoints with the ones `mie ingest`
  journaled; the first divergence is reported.
- **`mie experiment validate` / `mie experiment run` (ADR-040).** Experiment
  specs identified by the fingerprint of their canonical form, and an
  append-only research result store with SHA-256 trailers; a re-run on the
  same data version prints `reproduced` instead of `recorded`.
- **Release pipeline.** A pushed `vX.Y.Z` tag builds, tests and publishes the
  Linux x86_64 `mie` binary with a SHA-256 sidecar and these notes as a GitHub
  Release (`.github/workflows/release.yml`, `docs/release.md`).
