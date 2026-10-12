# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is below 1.0, a breaking change bumps the minor version and
is marked **Breaking:** under `Changed`. Releases are cut as described in
[`docs/release.md`](docs/release.md).

## [Unreleased]

### Added

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
