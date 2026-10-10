# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is below 1.0, a breaking change bumps the minor version and
is marked **Breaking:** under `Changed`. Releases are cut as described in
[`docs/release.md`](docs/release.md).

## [Unreleased]

### Added

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

### Fixed

- **Open-interest polling backs off from rate limits (ADR-045, #84).** On a
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
