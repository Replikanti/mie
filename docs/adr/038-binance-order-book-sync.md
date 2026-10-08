# ADR-038: Binance order-book sync — diffs and snapshots raw, sync as a pure state machine, audited checkpoints

- Status: proposed
- Date: 2026-10-07

## Context

#10 captures the BTCUSDT perpetual's order book, so passive liquidity
(added, removed, concentration, imbalance) can be rebuilt the same way in
live and in replay. Earlier decisions frame it:

- Raw data is stored verbatim and persisted before it is parsed. Live and
  replay share one normalization (ADR-003, ADR-019, ADR-022, ADR-030).
- Every event has one position in the canonical order. A `BookSnapshot`
  ranks before a `BookUpdate` of the same millisecond, so the diff that
  straddles a snapshot follows it. The engine requires strictly increasing
  snapshot and update ids, and a snapshot id at or above the last update id
  (ADR-028).
- Live capture runs on plain threads with one writer. The pipeline output is
  a pure function of the run's records in `receive_seq` order plus the run
  parameters journaled in `run_start` (ADR-032 D4). The hold-back merges the
  streams; anything at or below the last released event becomes a
  `LateEvent` gap (ADR-028 D6, ADR-032 D6).
- Feature state is order-flow first (ADR-021); the domain owns its models
  and keeps `BTreeMap`, never hash-ordered collections (ADR-025).

Binance publishes the book as a diff stream plus REST snapshots. The
documented sync procedure: buffer the diffs, fetch a snapshot, drop diffs
with `u < lastUpdateId`, require the first applied diff to straddle
`lastUpdateId`, then require every diff's `pu` to equal the previous diff's
`u`; otherwise start over.

## Decision

1. **Two raw streams.** `depth` is the diff stream `<symbol>@depth@100ms` on
   the `/public` WebSocket route (`wss://fstream.binance.com/public/ws`).
   `depthSnapshot` holds the bodies of REST `GET /fapi/v1/depth?symbol=
   <symbol>&limit=1000`. Both are stored verbatim. The snapshot body has no
   symbol; its raw stream key is its only instrument binding. Both feed the
   domain series `OrderBook`.
2. **Raw `event_time` is `T`**, the transaction time, for both streams, with
   the push time `E` as the fallback (ADR-032 D3).
3. **Fetch policy.** A snapshot is fetched when the capture thread asks for
   one because the book is unsynced (once per want id, through a capacity-1
   channel), and as a checkpoint 60 s after the last successful fetch. Two
   requests are at least 2 s apart; a 418 or 429 backs off exponentially, as
   for open interest. Non-2xx responses and transport failures are journaled
   (`depth_snapshot_fetch`) and never persisted. A `limit=1000` snapshot
   weighs 20; at the 2 s spacing the worst case is 600 per minute against
   Binance's 2400. The limit (one of 5, 10, 20, 50, 100, 500, 1000), the
   interval and the spacing are configuration. They decide only *when*
   snapshots exist, never what the pipeline does with them.
4. **Sync is a pure state machine in the pipeline** (`BookSequencer`): no
   clock, no I/O. Its inputs are the normalized records in `receive_seq`
   order, its seed (the later of the `depth` and `depthSnapshot` seeds) and
   the hold-back's last released event (`floor`), itself a function of the
   same records. So replay recomputes every book event, gap and rejection
   from the raw store plus the existing `run_start` parameters; no new run
   parameter is needed. The rules:
   1. A diff from a new capture session makes the book unsynced
      (`Disconnected`); the buffer becomes that diff.
   2. A synced diff whose `pu` is not the last emitted `u` makes it
      unsynced (`SequenceBreak`); the buffer becomes that diff.
   3. While unsynced, diffs are buffered; one that does not chain to the
      tail restarts the buffer. The buffer keeps the newest 1200 diffs (two
      minutes); drops are counted.
   4. A snapshot while unsynced is `Stale` if its `L` is at or below the last
      emitted snapshot id or below the last emitted update id (or not above
      the pending snapshot); otherwise it is the pending snapshot.
   5. Sync attempt after every unsynced record. Buffered diffs with `u < L`,
      or not above the last emitted `u`, are dropped as stale. With `d` the
      first remaining diff: `d.U > L` rejects the snapshot as `TooOld`;
      `d.T < S.T` as `TimeOrder`; a gap (or, without a gap, the snapshot)
      that would not sort above `floor` and above every book event emitted
      so far as `Late`. Otherwise the sequencer emits `FeedGap { OrderBook,
      start, end: S.T, cause }`, the `BookSnapshot` and the buffered diffs
      from `d` on. `start` is the time of the last emitted book event, else
      the seed, clamped to `end`; the gap is left out at the first sync of an
      unseeded run. Every rejection asks for another snapshot.
   6. A snapshot while synced is a **checkpoint** (decision 7).
   7. A desync clears the buffer and the pending snapshot.
   8. A diff that fails normalization desyncs (`MissingData`) and clears the
      buffer; a snapshot that fails is counted and, while unsynced, asks for
      another.
   9. The first cause of an unsynced period wins: each period yields
      exactly one `OrderBook` gap, delivered right before the snapshot that
      ends it (ADR-028 D4: continuity loss is announced on resumption).
   The constants (1200, and 64 in decision 7) are code, not run parameters:
   changing them is a versioned change of the sequencer.
5. **Late book events desync the book.** If the hold-back admits a book
   event as late (a `LateEvent` gap on `OrderBook`), the pipeline desyncs
   the book (`SequenceBreak`) and drops that record's remaining book events;
   the next resync announces the period with its own gap.
6. **The domain owns the L2 book** (`mie_domain::book::OrderBook`): a
   `BTreeMap` per side; a snapshot resets it; the first update must
   straddle the snapshot (`U <= L <= u`), every later one chain on `pu`; a
   violation or a negative quantity invalidates it and clears its levels; an
   `OrderBook` gap invalidates it until the next snapshot. A zero quantity
   removes a level. A snapshot is a reset, never liquidity flow. The
   **trusted window** is the snapshot's price range: bids at or above its
   deepest bid and asks at or below its deepest ask are fully known; beyond
   it only the levels changed since the snapshot are known, never their
   absence. It stays at the snapshot's bounds until the next reset, also
   when a snapshot returned fewer levels than requested. The engine is not
   wired to it yet (#18).
7. **Checkpoints re-anchor the book when provably consistent.** With `d*`
   the first diff with `u >= L` and `d_prev` the emitted diff before it, a
   checkpoint is emitted as a `BookSnapshot` (which sorts right before
   `d*`) only when `L` is above the last emitted snapshot id,
   `d_prev.T < S.T <= d*.T`, `d*.U <= L`, and the snapshot sorts above
   `floor`. A `d*` already emitted must be among the last 64 emitted diffs;
   a `d*` still to come is waited for, and a desync drops the wait. Every
   other checkpoint stays raw-only and is counted by reason. The canonical
   order and the engine's id rules then hold by construction. The trusted
   window follows the market instead of decaying until the next resync.
8. **The live checkpoint audit.** The capture thread mirrors the core's book
   over the released events with a domain `OrderBook`. At each re-anchored
   checkpoint it compares the book rebuilt from diffs since the previous
   anchor with the snapshot plus its straddling diff, within both trusted
   windows, and journals `matched` (levels, window depth in bps),
   `mismatched` (count and up to five examples), `unverifiable` (an
   `OrderBook` gap came between) or `invalidated` (the mirrored book broke
   without a gap: a provider bug). `mie capture-report` fails on any
   `mismatched` or `invalidated` checkpoint, and on a run whose depth spans
   more than twice its checkpoint interval without a `matched` one. This is
   #10's acceptance check; it needs no offline re-read of the raw depth.
   A `depth` reconnect counts as covered by any `OrderBook` gap spanning the
   old session's last record: by rule 9 a reconnect inside a period opened
   by a `pu` break or a malformed diff is announced as `SequenceBreak` or
   `MissingData`, not `Disconnected`.
9. **Recompute.** A recompute of a whole run (#11, #13) reads its records
   in `receive_seq` order plus `run_start`. A recompute that starts mid-run
   syncs at its first snapshot record. `mie replay` recomputes whole runs
   this way (ADR-039 D1), so book events, resync gaps and restart seed gaps
   replay with the other streams, and its window restricts the output.

### Documentation check (2026-10-07)

As for ADR-032, the Binance documentation site was not read; the checks
rest on live probes made on 2026-10-07.

- **Route.** `btcusdt@depth@100ms` delivers on `/public/ws`; on
  `/market/ws` it connected but delivered no frame in 4 s (as ADR-032's
  check found).
- **Update speed.** Frames in 5 s: `@depth` 10, `@depth@500ms` 5,
  `@depth@100ms` 23. `@depth@0ms` also connected and delivered a frame
  every ~25 ms (203 in 8 s, `pu` chained). It does not appear among the
  speeds this ADR relies on, so the capture stays on `@100ms`; a faster
  stream is a later, measured decision.
- **Payloads.** A diff carries `e` (`depthUpdate`), `E`, `T`, `s`, `ps`,
  `U`, `u`, `pu`, `b`, `a` (and `st` on some frames); levels are
  `[price, quantity]` string pairs, far beyond the top of the book. Over 50
  frames a diff averaged 3.5 KB (largest 24 KB). A snapshot carries
  `lastUpdateId`, `E`, `T`, `bids`, `asks`.
- **REST weight**, from `x-mbx-used-weight-1m`: 5 at `limit=100`, 10 at
  500, 20 at 1000. 25 snapshots at `limit=1000` took 271 to 642 ms each.
- **Snapshot `T` (load-bearing for rule 5b and decision 7).** Over 25
  `limit=1000` snapshots taken every ~2.8 s against a live diff stream
  (738 diffs, no `pu` break): the first diff with `u >= lastUpdateId`
  straddled it (`U <= L`) 25 times out of 25; its `T` was at or after the
  snapshot's `T` 25/25 (never equal); the diff before it was strictly
  earlier than the snapshot's `T` 25/25. A REST snapshot's `T` is therefore
  the transaction time of its `lastUpdateId`, as the sync rules assume.
- **Smoke run.** A 15 s `mie ingest` of `depth` and `depthSnapshot`
  (`limit=100`, checkpoints every 2 s) synced once, re-anchored 5
  checkpoints, matched all 5, and `mie capture-report` printed `PASS`. Its
  first sync window is the test fixture.

## Consequences

- Live and replay rebuild the identical book from the raw store; every
  resync, rejection and checkpoint decision is recomputable.
- Each unsynced period costs one `OrderBook` gap and the diffs until a
  straddling snapshot, typically one REST round trip.
- A slow REST path can make snapshots `Late` repeatedly: each retry waits
  the 2 s spacing. The counters and the soak show whether that happens.
- Depth is the heaviest stream: an estimated hundreds of MB a day after
  compression, plus 1440 snapshots a day at the default cadence. The
  capture thread does O(levels) work per diff for the audit.
- One more WebSocket connection and one more REST thread; the default
  stream set has seven positions, so the last planned reconnect lands at
  23 h + 6 × 5 min.
- The domain has an L2 book that nothing consumes yet; #18 wires it into
  the engine and builds liquidity features on it. Those must treat a
  snapshot as a reset.
- A report check now depends on journaled audit lines, not on re-reading
  depth: the journal must be complete for the window.

## Alternatives considered

- **Sync in the live I/O layer** (the fetcher thread decides when the book
  is synced). The result would depend on thread timing, not on persisted
  records, and replay could not reproduce it.
- **Raw-only checkpoints**, the issue's literal wording. The trusted window
  would shrink as price moves away from the last sync until the next
  resync, up to 23 h later.
- **Offline audit** by re-reading the raw depth files. Gigabytes a day for
  a check the capture can do on events it already holds.
- **Depth opt-in.** Depth is the order-flow input the design needs
  (ADR-021); the soak must measure its cost at the default.
- **A run parameter for the sync constants.** They change no market
  semantics; versioning them as code keeps `run_start` unchanged.

## Accept when

A ≥ 24 h live soak of the merged `mie ingest` with all seven streams
finishes, including the staggered 23 h rotations and one deliberate
restart. `mie capture-report` must print `PASS` over the window: no
mismatched or invalidated checkpoint, a matched checkpoint in every long
run, no domain rejection, late fraction 0. The soak's numbers, posted on
#10, settle the remaining values:

- depth messages per second (p50, p99) and raw bytes per day for `depth`
  and `depthSnapshot`;
- desyncs by cause and the time spent unsynced;
- snapshot fetch latency (p50, p99) and REST weight per minute;
- checkpoints emitted, skipped and matched, and the trusted-window width
  in bps;
- channel high-water marks and blocked time.
