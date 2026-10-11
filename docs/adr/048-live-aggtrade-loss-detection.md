# ADR-048: Live `aggTrade` loss inside aggregates — kline reconciliation as `capture-report` check 7

- Status: accepted
- Date: 2026-10-11

Relation to earlier ADRs: ADR-032 D5 ends with "On trades, the next id break
already reports a lost trade." The measurement below shows a lost trade that
causes no id break. ADR-032 is accepted and stays unedited, because an
addendum may not carry new normative content (`README.md`); the record is
here and cross-referenced, as ADR-047 did for ADR-031. ADR-047 left to #90
"whether to build any of these" detectors; this ADR is that decision for
live capture. The archive correction is #102 and is not decided here.

## Context

All numbers below come from the M0 measurement of #90, posted in full with
its scripts and the sha256 of every input file:
<https://github.com/Replikanti/mie/issues/90#issuecomment-6109027625>
(sections M0.3 and M0.4). M0 read the sealed raw stores of the #9,
#9-rotation, #10 and #13 captures (4 stores, 7 runs) and the Binance public
archive files of the same days. Arithmetic was exact (scaled integers).

### The live loss (M0.3(b))

Every live `aggTrade` record (2 511 816 unique aggregate ids, 0 duplicates,
UTC days 2026-10-07 … 10-10) was matched by `a` to the archive row of its
day. 97 live aggregates differ, all in `q` and `l` and in no other field.
All 97 come from run 20261008T035138Z (#9, after its restart), between
2026-10-08T12:50:16Z and 15:40:31Z, across 15 minutes. On every one the live
aggregate has the archive's `a`, `f`, `p`, `T` and `m` but a smaller `l` and
`q`: it starts at the same trade and ends early. The missing id ranges hold
466 ids, 465 of them trade rows of the archive `trades` file, 43.250 BTC.
The raw payloads are stored verbatim (ADR-030), so this is what the exchange
stream delivered; the archive is complete for these aggregates.

Nothing signalled it:

- **No gap was journaled** on `Trades` in that window, neither
  `Disconnected` nor `LateEvent`. The only journal entries there are
  `Liquidations` `LateEvent` gaps.
- **`capture-report` check 1 passed.** The aggregate id `a` stays
  consecutive, so aggregate-id continuity holds. ADR-032 D5's sentence
  above does not hold for a trade lost *inside* an aggregate.
- **The #9 soak acceptance contains the loss.** ADR-032's acceptance
  records `PASS` over the soak window (2026-10-07 15:50 – 2026-10-08 22:00
  UTC), which includes 12:50–15:43 on 2026-10-08.

### Detector C3 measured (M0.4)

C3 reconciles closed `kline_1m` frames with the aggregates in trade-id
space; its definition is D1 below.

- **Live** (M0.4(b)), per run: 2 631 blocks, 2 547 conclusive and 84
  inconclusive. 13 blocks are flagged, and all 13 contain the 97 truncated
  aggregates; in each, kline volume minus aggregate quantity equals the
  truncated volume exactly. Misses 0, false flags 0. 3 344 aggregates lie in
  no closed-kline minute, at run edges and in gaps.
- **Live klines are complete.** The 2 817 closed live klines were compared
  with the archive `trades` file of their day: `f`, `L`, `v` and `n` equal
  the minute's min id, max id, summed quantity and row count on 2 817 of
  2 817. The live kline is complete where the live aggregates are not.
- **Archive simulation** (M0.4(a)), five three-day windows centred on
  2026-02-05, 05-15, 06-09 (days with archive losses) and 06-10, 10-09
  (loss-free): 6 538 blocks, 0 misses, 0 false flags, block lengths 1 to 7
  minutes. Archive klines carry no trade ids, so `f`/`L` there came from the
  `trades` file.

The 13 flagged live blocks (UTC 2026-10-08):

| Block | kline v | aggregate q | difference |
|---|---|---|---|
| 12:50 | 4212.117 | 4206.15 | 5.967 |
| 15:21 | 1115.557 | 1115.407 | 0.15 |
| 15:23 | 3921.028 | 3913.251 | 7.777 |
| 15:24 | 860.927 | 859.539 | 1.388 |
| 15:25 | 1037.625 | 1032.032 | 5.593 |
| 15:31, 15:32 | 1926.219 | 1925.692 | 0.527 |
| 15:33 | 490.273 | 489.537 | 0.736 |
| 15:34 | 665.937 | 665.568 | 0.369 |
| 15:35 | 1862.232 | 1857.342 | 4.89 |
| 15:36 | 1757.667 | 1753.603 | 4.064 |
| 15:37 | 2309.86 | 2308.686 | 1.174 |
| 15:38 | 2373.994 | 2364.272 | 9.722 |
| 15:39 … 15:43 | 2582.572 | 2581.679 | 0.893 |

13 blocks, 18 minutes, total difference 43.250 BTC.

### The archive is no proxy for live (rule B2)

The pre-M0 plan of #90 fixed in advance that any live/archive difference
stops the use of the archive as a proxy for the live stream. It fired: the
live stream has a loss mode of its own, after the archive's defect window
(ADR-047: losses up to 2026-06-09) and while the archive is complete. So
ADR-047 speaks for the archive only, and its correction is #102.

## Decision

1. **C3 is `mie capture-report` check 7 (D1), defined exactly as M0
   measured it.** Per run of the window:
   - **Minutes.** Every `kline_1m` record with `k.x == true` gives the
     minute `k.t / 60000` with the trade-id interval `[k.f, k.L]` and the
     volume `k.v`. When several closed records exist for one minute, the
     last in `receive_seq` order counts. A minute with `k.n == 0` has no
     interval and joins no aggregate.
   - **Aggregates.** Every `aggTrade` record gives `(a, f, l, q, T)`,
     deduplicated by `a` (the last in `receive_seq` order counts).
   - **Membership.** An aggregate belongs to every minute whose `[f, L]`
     intersects its `[f, l]`. One that intersects none is an *orphan* at
     the minute `T / 60000`; so is an `aggTrade` record whose fields cannot
     be read, at its record time.
   - **Blocks.** Minutes that share an aggregate merge, transitively. Each
     aggregate's `q` counts once, in its block.
   - **Verdict.** A conclusive block passes when Σ `v` = Σ `q`, in ADR-027
     fixed point with checked arithmetic. Otherwise it is flagged with
     `difference = Σ v − Σ q`; a positive difference means trades are
     missing from the aggregates.

   Why exactly M0's definition: it is the one with a measured record (13 of
   13 losses, 0 false flags live and on 6 538 archive blocks). Any change —
   an adjacency rule, other gap streams, a tolerance — would be an
   unmeasured detector. Why 1-minute blocks: `kline_1m` is the only kline
   stream captured (ADR-032 D1), so it is the finest reference there is; a
   block is longer only where aggregates straddle minutes (M0: 1 to 7
   minutes on the archive, up to 5 among the live flags).

   Why the last record per minute and dedup by `a`: raw data is stored
   unchanged (ADR-022), so a record the source delivers twice is stored
   twice. Counting a duplicate twice would double its `v` or `q` and flag a
   false difference. The last record in `receive_seq`
   order is the one the store has most recently confirmed. For identical
   duplicates the choice does not matter: M0 measured 0 duplicate `a` and
   `nq` = `q` everywhere. Why an unreadable `aggTrade` is an orphan at its
   record time: its `[f, l]` and `q` are unknown, so its block would lack
   its `q` and be flagged with a positive difference, a false loss. Its
   record time is the only position the store still knows for it, and rule
   (d) of D3 makes that minute's block inconclusive instead. M0 saw 0
   unreadable records.

   Why an error, reported as a `FAIL` problem line, instead of a verdict:
   - **Non-increasing trade-id intervals.** The sweep that assigns
     aggregates to minutes requires the closed minutes' `[f, L]` to increase
     with the minute. Binance assigns trade ids in time order, so a
     violation means the kline records are corrupt or come from another
     instrument. Any block verdict computed over them would be meaningless,
     in either direction.
   - **`Qty` overflow.** It means the sums cannot be represented (ADR-027
     checked arithmetic, no wrapping).

   Both fail the report, and neither is silently skipped, for the reason
   given in D4: a report that passes over data it could not check is the
   #9 failure mode. M0 met neither case.
2. **Tolerance 0 (D2).** Both sums are exact integer counts of 10⁻⁸ BTC
   (ADR-027), so rounding cannot make honest data differ. Measured: 0 false
   flags at tolerance 0 on 6 538 archive-simulated and 2 547 conclusive live
   blocks. The smallest measured loss is one 0.001 BTC trade (archive,
   2025-10-01, M0.2); the smallest live block difference is 0.15 BTC
   (15:21 above). Any tolerance at or above a loss hides that loss, and no
   benign residual has been measured that a tolerance would have to absorb.
   A tolerance flag like `--max-late-fraction` is therefore not added: no
   benign deficit class exists to size it.
3. **Inconclusive blocks are not checked (D3).** A block is inconclusive
   when, first reason in this order:
   - (a) its minutes are not consecutive (a minute between them has no
     closed kline);
   - (b) `[first minute × 60000, last minute × 60000 + 59999]` is not inside
     the run's aggregate time span `[min T, max T]`;
   - (c) that interval overlaps a gap journaled for the run on `Trades` or
     `Klines`, of any reason;
   - (d) one of its minutes is an orphan's minute.

   Why: each rule marks a block whose ids are not fully covered by the
   klines or by the aggregates, for a reason a missing frame, the run
   boundary or a journaled gap already explains. M0: 84 of 2 631 live
   blocks were inconclusive, and none of the 13 losses fell into one. Why
   only `Trades` and `Klines` gaps: the only gaps journaled in the
   2026-10-08 loss window were `Liquidations` `LateEvent` gaps, and counting
   every stream's gaps would have made the loss blocks inconclusive and hidden
   the loss. Why any reason on those two streams: that is the definition M0
   measured; narrowing it to loss reasons (a `LateEvent` still has its raw
   record, ADR-032 D2) is unmeasured and not done here.
4. **A flagged block fails the report (D4).** One `FAIL` line per flagged
   block names its run, minutes (open times), kline volume, aggregate
   quantity and difference; a totals row counts blocks, conclusive,
   inconclusive by reason, flagged blocks and minutes, and the total
   difference. Why FAIL and not an informational row: `capture-report`
   verifies a window against its journal, checks 1 and 2 already fail on an
   unjournaled loss, and an informational row would let the next soak
   acceptance `PASS` over a loss, as #9's did. Accepting a known loss stays
   a human decision recorded on the issue. The check is skipped, with a
   `skipped` row, unless both `aggTrade` and `kline_1m` are configured.
5. **Report-level only (D5).** Check 7 reads the raw payloads already in the
   store. No event, bar, gap or feature changes; the domain event model and
   the live/replay normalization are untouched. Why: see the open question
   below. The code lives in `mie-cli` (`reconcile.rs`, pure), not in
   `mie-app`: it reasons about Binance trade-id ranges, an adapter concept,
   and every other `capture-report` check lives in `mie-cli`.

## Open question, not decided: in-band treatment

Marking or excluding affected bars, or an in-band gap, is not decided.

- A loss is known only when the minute's closed kline arrives, and for a
  merged block only when its last minute closes. That is long after the
  2000 ms hold-back (ADR-032 D6) released the minute's trades, so an in-band
  gap would be delivered into the past, which ADR-028 D6 forbids.
- It would make the trades pipeline depend on klines, and it would change
  the event stream that ADR-041 compares.
- Marking or excluding bars touches feature validity (ADR-029 D8).

If a consumer needs it, a new `needs-adr` issue decides it, starting from
check 7's per-block output.

## Consequences

- Detection happens when `capture-report` runs over a window, not
  continuously, and only where both `aggTrade` and `kline_1m` are captured.
- Localization is a block of minutes, not a trade.
- Inconclusive blocks are not checked: in M0, 84 of 2 631 live blocks.
- The kline is the reference. A loss present in both the kline and the
  aggregates is invisible, and a wrong kline flags a correct block (M0: 0
  such cases in 2 817 minutes).
- Inherited blind spot: an aggregate that straddles from a covered minute
  into a minute whose closed kline is missing, inside the run and outside
  every gap (a closed frame dropped within one session journals nothing,
  ADR-032 D5), leaves its block conclusive and flagged with a negative
  difference. M0 saw 0 such cases in 2 547 conclusive live blocks; the
  printed sign tells it apart from a loss. An adjacency rule would change
  the measured definition (D1).
- `capture-report` over the #9 soak window now prints `FAIL` with 13
  blocks, although ADR-032 recorded `PASS`. ADR-032 is not edited; the
  record is here.
- The live bars of 2026-10-08 12:50–15:43 are 43.250 BTC short. Replay
  reproduces them from the same raw records, so ADR-041 equivalence cannot
  see this loss class.
- The live-vs-archive comparison of M0.3(b) stays the manual cross-check
  that tells which side is wrong.
- Every future window with an upstream truncation fails. This is intended:
  the `FAIL` lines localize the loss.
- Memory: aggregate ranges are held per run for the window. All four M0
  stores together hold 2 511 816 aggregates, about 100 MB at roughly 40
  bytes each, which an offline report can afford.

## Alternatives considered

- **An informational row that leaves the verdict at `PASS`.** Rejected
  (D4): the #9 soak acceptance passed over exactly this loss.
- **C1, contiguity of the aggregates' trade-id ranges** (`f` = previous
  `l` + 1). Rejected: it cannot single the loss out. On 2026-02-05 the
  archive has 7 505 range gaps of which 5 hold a trade; on 2026-10-09,
  9 074 of which 0 do (M0.4(c)). The live truncation leaves a range gap
  (on 96 of 97 the next aggregate starts at the archive `l + 1`) that looks
  like the benign skipped-id gaps.
- **C2, kline volume against aggregate volume bucketed by time.**
  Rejected: at 1 minute only 200 of 6 610 bars with a non-zero residual
  contain a loss (3.03 % precision, M0.2), because an aggregate's trades
  can fall in another minute than its `T` (ADR-034 D8).
- **An in-band gap now.** Rejected: see the open question; it would be
  delivered into the past (ADR-028 D6).
- **The next-day archive cross-check as the detector.** Exact on
  2026-10-08, but a day late, it needs the network inside an offline report,
  and it trusts an archive that ADR-047 shows lost trades in 2025-10 …
  2026-06. It stays the manual cross-check.
- **`f` and `l` in the domain `Trade` and `Kline` events.** Rejected: an
  event-model change for a report-only need.
- **A tolerance flag.** Rejected (D2): no benign deficit class has been
  measured that could size it.
- **No detector.** Rejected: the loss happened during the first soak and
  nothing else signals it.

## Accept when

M2 reproduces M0: on the local #9 store, built from this PR, check 7 flags
exactly the 13 blocks of the table above (same minutes and differences,
total 43.250 BTC), all in run 20261008T035138Z, and 0 blocks on every other
run and store (#9-rotation, #10, #13), each store first verified against
M0's hash table. The conclusive and inconclusive totals are recorded next to
M0's 2 547 / 84; a difference is explained (M0 pooled gaps across stores,
check 7 uses each run's own journal). An extra flag or a missing block
stops the acceptance.

References: ADR-022, ADR-027, ADR-028, ADR-029, ADR-030, ADR-032, ADR-034, ADR-041,
ADR-047. Issues: #9, #10, #13, #88, #90, #102.

### Accepted: M2 result (2026-10-11)

Each store matched M0's hash table (parquet file counts 728 / 4 / 332 / 75
and the journal sha256). A release build of this PR, run over the window
2026-10-06 … 10-13:

| Store | Blocks | Conclusive | Inconclusive (edge / gap / orphan) | Flagged | Difference |
|---|---|---|---|---|---|
| #9 | 1666 | 1644 | 22 (2 / 20 / 0) | 13 (18 minutes) | 43.25000000 |
| #9-rotation | 9 | 5 | 4 (1 / 3 / 0) | 0 | 0 |
| #10 | 782 | 771 | 11 (2 / 9 / 0) | 0 | 0 |
| #13 | 174 | 127 | 47 (2 / 45 / 0) | 0 | 0 |

The 13 flagged blocks are those of the table above, with the same minutes
and differences, all in run 20261008T035138Z. The totals equal M0's
2 547 conclusive and 84 inconclusive, so per-run journals and M0's pooled
gaps gave the same split. Record: #90 M2 comment.
