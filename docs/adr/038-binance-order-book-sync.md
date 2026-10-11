# ADR-038: Binance order-book sync — diffs and snapshots raw, sync as a pure state machine, audited checkpoints

- Status: accepted
- Date: 2026-10-07
- Amended: 2026-10-11 (#73)

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
run, no domain rejection, late fraction at most 0.02 per stream
(`--max-late-fraction 0.02`, ADR-032 D11). The soak's numbers, posted on
#10, settle the remaining values:

- depth messages per second (p50, p99) and raw bytes per day for `depth`
  and `depthSnapshot`;
- desyncs by cause and the time spent unsynced;
- snapshot fetch latency (p50, p99) and REST weight per minute;
- checkpoints emitted, skipped and matched, and the trusted-window width
  in bps;
- channel high-water marks and blocked time.

## Acceptance

Accepted 2026-10-09. The #10 live soak ran 13 h 41 min in two runs
(2026-10-08 22:33 – 2026-10-09 12:14 UTC, main `99aed6b`, all seven streams,
the `ingest.example.toml` defaults) with one deliberate restart and 12
unplanned WebSocket disconnects. It deviates from "Accept when" in two
points, both decided by the user:

- **Duration.** It is 13 h 41 min, not 24 h. The home connection drops every
  few hours, so a 23 h connection age is unreachable, and continuing would
  not have exercised rotation. The US session (~13:30–20:00 UTC) and the
  16:00 funding were not observed, so the per-day figures below are
  extrapolated from a window covering the Asian and European sessions only.
  US-session sizes and rates are unmeasured.
- **Planned rotation.** No planned 23 h rotation fell inside the soak,
  because every disconnect reset connection age. The rotation path,
  including the depth resync after it, is covered by a separate short run
  with `max_connection_age_secs = 300` on all seven streams: the depth
  stream rotated, resynced in 0.4 s, and 7 of 7 checkpoints matched. This is
  how ADR-032 was accepted as well.

`mie capture-report --max-late-fraction 0.02` prints `PASS`: 812 of 812
checkpoints matched, none skipped, unverifiable, mismatched or invalidated,
no domain rejection, no normalize error. Late events: aggTrade 1, depth 2,
every other stream 0. The settled values:

- **Depth messages per second** (per wall-clock second, p50 / p99 / max):
  10 / 11 / 40. `@100ms` is a fixed cadence; a US-session p99 is unmeasured.
- **Raw bytes per day** (extrapolated from 13.7 h, Parquet on disk): `depth`
  about 640 MB, `depthSnapshot` about 20 MB, all seven streams about
  720 MB. The 5-minute `depth` seals ran at a p50 / p99 / max of 2.1 / 4.6 /
  7.7 MB, equivalent to 0.6 / 1.3 / 2.2 GB per day at that rate.
- **Desyncs**: `Disconnected` 4 (two are the initial syncs of the two runs,
  two are real depth disconnects), `SequenceBreak` 2. Both `SequenceBreak`
  desyncs coincide with a depth `LateEvent` gap (max depth lateness
  2594 ms against the 2000 ms hold-back), so they come from the hold-back
  rule, not from `pu` breaks. Time unsynced (wall clock): 4.2 s in total
  over 6 periods, at most 1.14 s. No snapshot was rejected and no buffer
  dropped diffs.
- **Snapshot fetches**: 818, all HTTP 200. Latency p50 347 ms, p99 828 ms,
  max 5465 ms. REST weight per minute, from the 20 per `limit=1000` fetch
  measured above (the journal does not record the header): average 19.9,
  maximum 40; 25.9 and 46 with the open-interest poll. The limit is 2400.
- **Checkpoints**: emitted 812, skipped 0, matched 812. The trusted window,
  taken at the narrower side, is min 0, p10 10, median 13 bps; 14 of 812
  are 5 bps or less, and all of them matched.
- **Channels**: high-water 488 inbound and 714 core of 65 536; blocked time
  0 ms.

Results: <https://github.com/Replikanti/mie/issues/10#issuecomment-6080717136>,
rotation: <https://github.com/Replikanti/mie/issues/10#issuecomment-6070194235>.

## Justification addendum (2026-10-11)

### a. Scope

This addendum follows the rule in [`README.md`](README.md): it explains
why the decisions and numbers above hold, where each comes from, what
breaks at a different value and when each value was set. It changes no
value, threshold, acceptance criterion or normative statement; every
section above is unchanged. The evidence is a measurement over the #10
soak (two runs, 13.68 h), posted on #73 with its decision rules fixed
beforehand (the plan comment) and its harness source:
<https://github.com/Replikanti/mie/issues/73#issuecomment-6106591720>
(results) and
<https://github.com/Replikanti/mie/issues/73#issuecomment-6106592277>
(harness), "the #73 measurement" below. Its self-check reproduced the
Acceptance figures exactly at the journaled hold-back.

The 2000 ms hold-back and the 0.02 late fraction belong to ADR-032; their
own justification is in that ADR's addendum. This one says why 2000 ms is
*used here*, in the order-book desync analysis (section c.7).

Every verdict below is KEEP. Where the measurement shows a limit of the
existing rule (the 0 bps window, the "twice the interval" check, the
depth lateness figure), the limit is stated and the rule is left as
decided; a change would need a superseding ADR.

### b. History of the values

All values below were written before any measurement of them, in the #10
plan (comment 6043270341, 2026-10-07 17:32 UTC) and carried by PR #59
(opened 2026-10-07 19:42 UTC, merged 2026-10-08 05:47 UTC, squash commit
2aa8736). That commit is also the first commit of this file: the header
says 2026-10-07, the date of the plan and of the documentation check, and
the file reached the repository on 2026-10-08. The ADR was accepted
on 2026-10-09 in PR #68 (merged 12:22 UTC, squash commit ef2f197).
*Accept when* promised that the soak's numbers "settle the remaining
values"; *Acceptance* reports the numbers and changes none of the values.
No derivation of any value below was recorded at either point.

| Value | Recorded in the plan and PR #59 | Reason recorded |
|---|---|---|
| Checkpoint 60 s | "fetched on demand and every 60 s"; `depth_checkpoint_interval = 60 s` | none |
| Spacing 2 s, weight ceiling | `depth_snapshot_min_spacing = 2 s`; "20 per fetch, at worst 600/min at 2 s spacing against Binance's 2400/min" | the ceiling arithmetic only; why 2 s: none |
| `limit` 1000 | `GET /fapi/v1/depth?limit=1000`; REST weight 20 noted in `ingest.example.toml` | none |
| 1200 buffered diffs | "`MAX_UNSYNCED_DIFFS = 1200`"; code comment "two minutes at the 100 ms cadence" | the comment restates the arithmetic (1200 × 100 ms), not why two minutes |
| 64 recent diffs | "`RECENT_DIFFS = 64`"; code comment "6.4 s at the 100 ms cadence" | the same: arithmetic, not a reason |
| "More than twice the interval" | "a run whose depth records span more than 2 × the checkpoint interval has no `Matched` checkpoint" | none |
| 0 bps trusted window | `bps()` "whole basis points, rounded down" (PR #59); the plan lists "trusted-window width in bps" and "the minimum and median window in bps" as soak numbers | none; no minimum width was ever specified |
| 2000 ms hold-back | taken from ADR-032 D6 | ADR-032's addendum; none specific to the book |

### c. The numbers, one by one

#### c.1 Checkpoint interval 60 s (decision 3)

- **Source.** Engineering default; no reason was recorded (section b).
  Measured in the #73 measurement: for each of the 812 matched
  checkpoints, the largest move of the `aggTrade` price from the anchor in
  the next T seconds, against the snapshot's own price range (13.9 to
  15.9 bps on the narrower side, median 15.3) and against the range the
  audit actually compared (median 13.6 bps).
- **Why this value.** At 60 s the price stays inside the snapshot's
  narrower side in 99.1 % of anchors and inside the audited range in
  96.2 %; it leaves the 5 bps outer band's margin (ADR-043) in 2.0 %. The
  alternatives:

  | T | Snapshots a day | REST weight a minute | Raw MB a day | Beyond the fresh narrow side | Beyond the audited narrow side | Beyond the outer-band margin |
  |---:|---:|---:|---:|---:|---:|---:|
  | 15 s | 5760 | 80 | 81.6 | 0.0 % | 0.9 % | 0.0 % |
  | 30 s | 2880 | 40 | 40.8 | 0.4 % | 1.8 % | 0.7 % |
  | **60 s** | **1440** | **20** | **20.4** | **0.9 %** | **3.8 %** | **2.0 %** |
  | 120 s | 720 | 10 | 10.2 | 2.5 % | 7.0 % | 7.5 % |
  | 300 s | 288 | 4 | 4.1 | 11.3 % | 19.1 % | 27.6 % |

  30 s buys 0.5 and 2.0 percentage points (fresh and audited) for twice
  the REST weight and raw size; 15 s buys 0.9 and 2.9 points for four
  times. Neither is dominated by 60 s, and neither is needed: the
  checkpoint is an audit and a re-anchor, not a live feed. The excursion
  is measured from the anchor price over the next T seconds, not as high
  minus low inside a calendar minute, so it is a different measure from
  ADR-043 M2; the two are not reconciled here.
- **Breaks below.** No correctness. Cost grows with 1/T: 15 s is 80 of
  the 2400 weight a minute (3.3 %) and 82 MB a day of raw snapshots,
  against 20 weight and 20 MB at 60 s. Below the 2 s spacing a checkpoint
  would wait for the spacing rule.
- **Breaks above.** Coverage. At 120 s the price leaves the outer-band
  margin in 7.5 % of anchors (3.8 times the 60 s rate); at 300 s in
  27.6 %, and at 300 s a run shorter than twice the interval (10 minutes)
  is not audited at all (section c.5).
- **Verdict: KEEP.** The cost is measured: 818 snapshots took 11.6 MB over
  13.68 h.

#### c.2 Spacing of at least 2 s, and `limit=1000` (decision 3)

- **Source.** Spacing: a derivation from an exchange limit. A
  `limit=1000` snapshot weighs 20 (probed 2026-10-07 through
  `x-mbx-used-weight-1m`); the budget is 2400 a minute (not re-verified);
  the open-interest poll adds 6 a minute (ADR-032 addendum). Limit: the
  largest of the allowed values 5, 10, 20, 50, 100, 500 and 1000.
- **Why 2 s.** It caps the worst case, repeated resyncs, at 30 fetches a
  minute: 30 × 20 + 6 = 606, which is 25.3 % of 2400. The other 75 % stays
  for traffic of the same IP that this ADR does not control. The
  alternatives: 1 s is 1206 (50.3 %); 5 s is 246 (10.3 %). No reason for
  exactly 2 s was recorded; the table above is the derivation. In the soak
  the spacing never bound: the shortest gap between two requests was
  5874 ms, and no 60 s window held more than 2 fetches (40 weight, 46 with
  open interest).
- **Why `limit=1000`.** The trusted window is the snapshot's price range,
  and it scales with the number of levels returned. Over the 818
  snapshots, keeping the first N levels, the narrower side measures a
  median of 15.3 bps at 1000, 7.0 bps at 500 and 1.5 bps at 100. ADR-043's
  5 bps outer band needs a margin above 5 bps: at `limit` 500, 67.1 % of
  anchors leave that margin within 60 s, at 1000 2.0 %. The REST probe of
  ADR-043 M1 gave 15 to 16 bps at 1000, consistent.
- **Breaks below.** Spacing: at 1 s a resync loop uses half the budget
  (1206 of 2400), and a 418 or 429 becomes likelier. Limit: at 500 the
  window is 7 bps and the outer band loses its margin (above); at 100 it
  is 1.5 bps, which serves only the 1 bps band (the test fixture's
  setting).
- **Breaks above.** Spacing: every retry after a rejected snapshot waits
  longer (5 s adds 3 s per retry); no rejection happened at 2000 ms
  hold-back, but the sweep shows 14 at 0 ms. Limit: there is no larger
  value. Weight: it is 20 at 1000; if the exchange raised it to 50, the
  worst case would be 30 × 50 + 6 = 1506 (62.8 %).
- **Verdict: KEEP.**

#### c.3 The 1200-diff buffer, "two minutes" (decision 4.3)

- **Source.** Arithmetic: 120 s at the fixed `@100ms` cadence is 1200
  diffs at the median rate of 10 a second (the rate is the soak's, p99 11,
  maximum 40). Measured in the #73 measurement.
- **Why this value.** It must hold every diff of one unsynced period. A
  period lasts about one fetch plus one retry spacing per attempt: at the
  slowest fetch (5.465 s) plus the 2 s spacing and 11 diffs a second that
  is 82 diffs, at the 40 a second burst 299. The soak's longest period
  lasted 1.14 s of wall clock, and the most diffs buffered in any period
  was 36 (3 % of 1200), with no drop.
  As a stress, replaying the same data with a hold-back of 0 ms gives
  unsynced periods of up to 117.8 s and a peak of 1155 diffs (96 %, no
  drop): two minutes is the length a period can reach before the buffer
  overflows. That setting is not a soak configuration.
- **Breaks below.** The oldest buffered diff is dropped and counted
  (`buffer_drops`). The first remaining diff then has `U > L`, the
  snapshot is rejected `TooOld` and fetched again after the spacing. The
  cost is more unsynced time, not wrong data.
- **Breaks above.** Memory only: 1200 × 3.5 KB (the average diff of the
  documentation check) is 4.2 MB, and at the 24 KB largest diff 28.8 MB.
  Nothing else depends on the size.
- **Verdict: KEEP.** "Two minutes" holds at the median rate; at the 40 a
  second burst the same 1200 diffs are 30 s.

#### c.4 Checkpoint search among the last 64 diffs (decision 7)

- **Source.** Arithmetic (64 × 100 ms = 6.4 s) and measurement. For each
  of the 812 checkpoints the harness replayed the diffs in `receive_seq`
  order. In 257 the straddling diff `d*` was already emitted when the
  snapshot record arrived; in 555 it was still to come and the checkpoint
  waited for it.
- **Why this value.** The distance of `d*` from the newest diff is the
  snapshot's age at arrival (receive time minus the snapshot `T`) times
  the diff rate. The age was at most 1472 ms (p50 160, p99 606 ms), so at
  most 6 of the 64 slots were needed, counting `d_prev` (p50 2, p99 6).
  **Margin: 58 of 64 slots (91 %).** Slowness of the fetch itself does not
  transfer: the three slowest (5465, 5371 and 1669 ms) had `d*` still to
  come when the response arrived. The naive bound 5.465 s × 11 a second =
  60 diffs, a margin of 4 (6 %), did not materialize. At the 40 a second
  burst the 64 diffs span only 1.6 s, against the worst observed age of
  1.47 s, which would still fit.
- **Breaks below.** A checkpoint whose `d*` is older than the window is
  skipped as `OutOfRecent`, counted and left raw-only. No correctness is
  lost and no gap is created. None was skipped in the soak.
- **Breaks above.** Memory only: each slot holds three 64-bit values (a
  time and two ids); nothing else depends on the size.
- **Verdict: KEEP.**

#### c.5 "More than twice its checkpoint interval" (decision 8)

- **Source.** Engineering default; no reason was recorded. Measured in the
  #73 measurement.
- **What the rule is.** A per-run presence check
  (`crates/mie-cli/src/report.rs`): a run whose depth records span more
  than twice the checkpoint interval must have at least one matched
  checkpoint. It does not test every 120 s window.
- **Why 2×.** The first checkpoint is due 60 s after the sync fetch, plus
  the fetch and the wait for `d*`. In the soak the first matched
  checkpoint came 61.0 s (run 1) and 60.7 s (run 2) after the run's first
  depth record. At 1× (60 s) a healthy run of 60 to 61 s would fail; at 2×
  the slack is 59 s. At 10× (10 minutes) a run could go ten minutes with
  nothing audited.
- **Breaks below.** At 1× the rule fails healthy runs (above).
- **Breaks above.** The audit-presence guarantee weakens in proportion; at
  10× a short broken run passes.
- **Limit of the rule.** It proves a run was audited at all, not that it
  was audited continuously. Between consecutive matched checkpoints the
  gaps were p50 60.35 s, p99 60.87 s and at most 112.6 s (94 % of 2×;
  seven gaps over 61 s). A desync restarts the 60 s timer from the sync
  fetch, so the next checkpoint is due up to one interval plus the resync
  time later; a continuity rule at 2× would not be guaranteed to hold.
- **Verdict: KEEP**, with that limit.

#### c.6 The 0 bps trusted window (decisions 6 and 8)

- **Source.** The rule is the audit as built: both books are compared
  inside the intersection of their trusted windows. `bps()` is the
  absolute distance between the best price and the window edge, truncated
  to whole basis points. So "0 bps" means under 1 bp, and the figure is a
  distance, not a width. There is no minimum width and no minimum level
  count.
- **What the soak shows.** Of the 812 matched checkpoints, 14 had the
  narrower side at 5 bps or less (truncated, as in *Acceptance*). In every
  one the narrow side was the side the price had moved toward in the
  preceding 60 s, by 8.8 to 19.2 bps; the previous anchor's window had
  decayed on that side. The fresh snapshots were never narrow (13.9 bps at
  least). Twelve of the 14 compared 57 to 423 levels on the narrow side.
  Two compared **zero** levels on it (2026-10-09 01:26:41 UTC, ask, 0.02
  bps, price +16.3 bps; 11:49:51 UTC, bid, 2.64 bps, price -19.2 bps): the
  price had moved further than the old window was wide, which is
  consistent with the old window's edge lying beyond the new best price,
  so no level of either book was inside it. `bps()` still reports a
  positive distance. Over all 812 checkpoints the narrower side compared a
  median of 894 levels (p10 699).
- **Does a match with a near-empty side prove anything for that side?**
  For each side separately: a side with 57 to 423 compared levels proves
  those levels equal; what lies beyond the window is unknown by design
  (decision 6: "never their absence"). A side with zero compared levels
  proves nothing for that side, although the checkpoint is counted as
  matched. That was the case in 2 of 812 (0.25 %); the other side of those
  two compared 1001 levels each.
- **Why keep it.** The audit's purpose in #10 was to show the rebuilt book
  equals the exchange's, inside what is fully known on both sides. The
  window narrows only through the decay of the previous anchor; the next
  checkpoint, 60 s later, is compared inside a fresh window again.
  Rejected here: a minimum width or level floor. It would need a policy
  for a failing checkpoint (unverifiable? skipped?). A floor of 1 level
  would have flagged 2 of the 812 checkpoints, a floor of 100 levels 3.
- **Breaks below / above.** There is no threshold to break. Without a
  minimum, a checkpoint can be called matched with an empty side (above).
  With a minimum, the checkpoints flagged are the ones counted for the
  floor above, and a price that moves further than the window in 60 s
  would flag its checkpoint every time.
- **Verdict: KEEP**, the rule as decided, with its limits stated: the
  truncation (a window under 1 bp counts as 0), no minimum width, no
  minimum level count, and 2 of 812 checkpoints with zero levels compared
  on one side. A minimum is a normative change and needs a superseding
  ADR.

#### c.7 The 2000 ms hold-back in the desync analysis (decision 5, *Acceptance*)

- **Source.** The value is ADR-032 D6's, justified there by the forceOrder
  prior. Here it matters for a second reason, measured in the #73
  measurement: a diff the hold-back admits as late desyncs the book
  (decision 5). The hold-back is one value for the merged canonical order
  (`HoldBack::new(hold_back_ms, ...)`), so depth cannot have its own;
  moving it moves every stream.
- **Why this value (for the book).** Replaying both runs with only the
  hold-back changed:

  | Hold-back | Depth `LateEvent` gaps | `SequenceBreak` desyncs | Wall time unsynced | Delay on every event |
  |---:|---:|---:|---:|---:|
  | 0 ms | 260 | 260 | 9608 s | 0 |
  | 500 ms | 53 | 53 | 1532 s | 0.5 s |
  | 1000 ms | 31 | 31 | 781 s | 1 s |
  | 1500 ms | 4 | 4 | 66.9 s | 1.5 s |
  | **2000 ms** | **2** | **2** | **4.2 s** | **2 s** |
  | 2594 ms | 2 | 2 | 3.4 s | 2.6 s |
  | 3000 ms | 0 | 0 | 2.0 s | 3 s |
  | 5000 ms | 0 | 0 | 2.0 s | 5 s |

  Every smaller value gives more book desyncs, so 2000 ms is not
  dominated. The choice among the values that are not dominated is a
  judgment, because mie has no live latency budget (there is no execution
  path, ADR-010).
- **What 2000 ms costs.** In 13.68 h: 2 desyncs, each announced by a
  `SequenceBreak` gap of 3.5 to 3.6 s of event time (1.1 s of wall clock
  unsynced), 2 depth `LateEvent` gaps, no snapshot rejected. Both late
  diffs reached the host 3.1 and 2.9 s after their `T` (depth receive lag:
  p50 133 ms, p99 261 ms, p99.9 1291 ms, max 3301 ms), while `aggTrade`
  and `markPrice` frames with later `T` had already arrived; the
  watermark was advanced by another stream, not by a depth burst.
  Raising the hold-back to 3000 ms would remove both desyncs, 2.2 s of
  unsynced wall time and 7 s of resync-gap event time, at 1 s more delay
  on every live event; to 2594 ms removes none (section d). The
  US-session rate, lag and lateness are unmeasured.
- **Breaks below.** More desyncs, as the table: at 1500 ms double, at
  1000 ms 15 times, at 0 ms the book is unsynced 19.5 % of the time.
- **Breaks above.** Delay on every event and every alert; the buffer
  grows with the hold-back (negligible, ADR-032 addendum section c).
- **Verdict: KEEP.** The cost is measured and stated above.

### d. Evidence figures, recomputed

- **Depth max lateness 2594 ms.** The maximum over the 776 `stats` lines of
  run `20261008T231852Z` of `streams.depth.max_lateness_ms`; the value is
  a running maximum per run and the last line and `run_end` carry the
  same number (run 1: 910 ms). Defined as the merge watermark minus the
  event's ordering time when it is pushed (ADR-032 addendum, section c).
  The replay at 2000 ms reproduces it. The second late diff (`T`
  2026-10-09 12:09:40.694 UTC, arrived 12:09:43.580) is 2594 ms behind the
  watermark by the harness's approximation of it. **It is a lower bound
  of the depth lateness, not the soak maximum**: diffs that arrive while
  the book is unsynced are buffered and reach the hold-back only after the
  resync; at hold-backs of 2594 ms and above, where the two late diffs no
  longer desync, the pipeline's depth `max_lateness_ms` is 3040 ms. The
  ADR-032 addendum's statement that lateness does not depend on the
  hold-back is about the streams that do not pass through the book
  sequencer; depth does, and the sequencer decides when its diffs reach the
  hold-back.
- **Time unsynced, 4.2 s in total, 1.14 s at most.** Wall clock from the
  `book` journal lines: each `desync` `at_ms` to the next `sync` `at_ms`
  in the same run: 755, 408, 401, 417 ms (`Disconnected`), 1141 and
  1086 ms (`SequenceBreak`), total 4208 ms. The event-time lengths of the
  `OrderBook` resync gaps are a different figure: 3420, 1636 and
  1571 ms (`Disconnected`), 3548 and 3588 ms (`SequenceBreak`).
- **183 ms and 459 ms.** The lengths of the two depth `LateEvent` gaps. A
  `LateEvent` gap runs from the late event's time to the last released
  event plus 1 ms (`holdback.rs`, `late_gap`), so they are measured
  against the last released event, not against the watermark (2439 and
  2594 ms). Each `SequenceBreak` gap starts at exactly the instant its
  `LateEvent` gap starts, the late diff's `T`
  (2026-10-09 04:48:01.564 and 12:09:40.694 UTC): "coincide" means the
  same start.
- **Limit.** Both runs cover 22:33 UTC to 12:14 UTC (13.68 h), the Asian
  and European sessions. The US session, the 16:00 UTC funding and every
  rate, p99 and lateness that depends on them are unmeasured, so each
  margin in section c holds for that window.

### e. Other numbers of this ADR

| Number | Where | Source | Breaks below | Breaks above |
|---|---|---|---|---|
| `@depth@100ms` | decision 1, documentation check | The speeds the documentation check probed on 2026-10-07 (`@depth` 10 frames in 5 s, `@500ms` 5, `@100ms` 23); `@0ms` also connected (203 frames in 8 s) and was not adopted. The soak's rate for `@100ms` is the one relied on here: 10 a second at the median, 11 at p99, 40 at the maximum | `@500ms`: fewer, coarser diffs; several book changes arrive merged in one | `@0ms`: about five times the frames a second in the same check (203 in 8 s against 23 in 5 s), same sync procedure; its raw size was not measured |
| 418/429 backoff | decision 3 | The same `backoff_*` values as the open-interest poller (250 ms doubling to 30 s); engineering default, not derived. The soak had no 418 or 429 (818 of 818 HTTP 200) | Retries during a ban deepen it (documented escalation, not re-verified) | A longer wait after a recovery, up to the 30 s cap |
| Weight 20 for `limit=1000` | decision 3 | Probed 2026-10-07 through `x-mbx-used-weight-1m`: 5, 10 and 20 for 100, 500 and 1000. Not re-verified | If the weight were lower, the ceiling would shrink | At 50 the worst case at 2 s spacing would be 1506 (62.8 %) |
| "At least 24 h" | *Accept when* | Copied from ADR-032 (#9 plan). Its only recorded reason was to include the 23 h planned reconnects; a restart or disconnect resets the connection age, and the host's network dropped every few hours, so the soak ran 13.7 h without one (*Acceptance*). The rotation was covered by a separate 300 s run | A run shorter than 23 h 25 min cannot contain the depth stream's planned rotation (section f.1) | Length alone does not guarantee a rotation: 12 unplanned disconnects reset the age |
| Late fraction 0.02 for depth | *Accept when* | ADR-032 D11 (its addendum, section b). Not binding here: depth had 2 late of 482 713 events (0.0004 %) | None known | The gate would still pass at about 9650 late depth events, 4800 times the soak's 2; no measurement ties 0.02 to a failure |

### f. Corrections of the original text

The text above stays as written. These sentences are superseded by the
figures here or are read as the code does:

1. **Consequences, "the last planned reconnect lands at 23 h + 6 × 5
   min".** The stagger is the stream's position in `streams` times 5
   minutes (`live.rs`). Depth is position 5, so its planned reconnect is at
   23 h + 5 × 5 min = 23 h 25 min; `depthSnapshot` is position 6 but a REST
   fetch and does not reconnect. The ADR-032 addendum has the same row.
2. **Alternatives, "up to 23 h later".** The next resync after a
   raw-only checkpoint is the next disconnect or the planned reconnect, so
   the bound is 23 h 25 min; in the soak, 12 unplanned disconnects came
   far earlier.
3. **Consequences, "an estimated hundreds of MB a day".** Confirmed:
   measured about 640 MB a day for `depth` and 720 MB for all seven streams
   (*Acceptance*).
4. **Decision 7, "must be among the last 64 emitted diffs".** The code
   needs the straddling diff and the diff before it both inside the 64
   (`CheckpointSkip::OutOfRecent` for the oldest slot), so `d*` must be
   among the last 63. In the soak at most 6 were used (section c.4).
5. **Decision 4.3, "(two minutes)".** Two minutes at the median 10 diffs a
   second; at the 40 a second burst, 30 s (section c.3).
6. **Decision 8, "a run whose depth spans more than twice its checkpoint
   interval without a matched one".** A per-run presence check, not a
   continuity rule (section c.5).
7. **Documentation check, "Frames in 5 s: `@100ms` 23".** The soak measured
   10 frames a second at the median, which is 50 in 5 s; the check's count
   is about half of that, so it was a presence probe, not a rate
   measurement. The rate this ADR relies on is the soak's.
8. **Acceptance, "14 of 812 are 5 bps or less, and all of them matched".**
   True as a count; two of the 14 compared zero levels on the narrow side
   (section c.6).
9. **Acceptance, "max depth lateness 2594 ms against the 2000 ms
   hold-back".** True at 2000 ms; a lower bound of the depth lateness
   (section d).
