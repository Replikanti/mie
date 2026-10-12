# ADR-050: The aggTrades fold — trades the `aggTrades` archive omits, restored from the `trades` archive at replay time

- Status: proposed
- Date: 2026-10-12

Relation to earlier ADRs: this ADR supersedes ADR-047 decision 2 (the loss
recorded as a caveat, no correction) and, in ADR-039 decision 5, the rule
that every `aggTrades` row is delivered exactly as normalized. Both stay
`accepted` otherwise; their status lines name this ADR. ADR-034 decision 2
is cross-referenced, not superseded: `trades` stays opt-in at import and is
never a delivered stream.

## Context

ADR-047 defined an **uncovered trade** of a day D: a row of
`BTCUSDT-trades-D.zip` whose id lies inside the day's aggregate span but
inside no aggregate's `first_trade_id ..= last_trade_id` range. It measured
2109 uncovered trades in 201 distinct 1m bars on 32 of 41 sampled days from
2025-10-01 to 2026-06-09, and 0 on 22 sampled days from 2026-06-10 on. It
recorded the loss as a caveat (D2) and left correction versus caveat to
#90.

### Rule A failed (#90 M0)

#90's M0 measured whether the caveat is acceptable, with a pre-registered
rule A per affected day
(<https://github.com/Replikanti/mie/issues/90#issuecomment-6109027625>,
section M0.2; exact arithmetic, every zip checked against its `.CHECKSUM`):

- **A1**: the largest lost volume (and lost taker-buy volume) in a touched
  1m bar is at most the largest boundary residual `|kline − aggTrades|` of
  the day's untouched 1m bars, the error ADR-031 already accepts.
- **A2**: the loss changes no more bars' high or low than the untouched
  bars already differ in.
- **A3**: the loss moves the day's VWAP by less than one tick.

A1 fails on 5 of the 32 days (2026-05-08, 05-15, 05-22, 06-01, 06-08), A3 on
3 (2026-05-15: 15.04 ticks, 05-22: 1.70, 06-01: 1.08); A2 holds on all 32.
The worst case is 2026-05-15 00:02, where one 1m bar lacks 105.803 BTC
against a 5.655 BTC boundary residual. The tick is 0.10, from
`GET /fapi/v1/exchangeInfo` `PRICE_FILTER` (retrieved 2026-10-11T11:59:30Z);
no price in any row of the 32 days is off that tick, so it held for the
whole window. A one-tick VWAP shift is the smallest move a price can make,
which is why A3 is set there: a larger tolerance would accept errors that
change what a VWAP-based level says. On 2026-10-11 the human decided: the
archive loss is **corrected, not caveated**; a per-minute or per-day
exclusion list is rejected (#102).

### Where the lost trades belong (#102 M0)

The attribution was measured before any code, with a pre-registered GO
rule (head-only = 0 and neither = 0 on all 32 days, and the live check
97/97), on the 32 days' `trades` and `aggTrades` zips (sha256 in the #90 M0
comment) and `measure.py`'s 2109 rows from #88:
<https://github.com/Replikanti/mie/issues/102#issuecomment-6115120769>.
For an uncovered trade `u`, `P` is the aggregate with the largest
`last_trade_id < u`, `N` the one with the smallest `first_trade_id > u`.

| Measured | Count | Of |
|---|---|---|
| tail: price and side equal `P`'s | 2109 | 2109 |
| head-only: equal `N`'s and not `P`'s | 0 | 2109 |
| neither | 0 | 2109 |
| equal to both `P` and `N` (part of tail) | 313 | 2109 |
| `u.time ≠ P.transact_time` | 2107 | 2109 |
| `minute(u.time) ≠ minute(P.transact_time)` | 26 (all on 2026-06-08) | 2109 |
| `P` and `N` inside the same day file | 2109 | 2109 |
| folded quantity, summed over the 32 per-day values | 541.622 BTC | — |

The population is ADR-047's: trades **inside** the day's aggregate span,
strictly between two ranges of the same day (the **inner** trades). An
**edge id**, a trade before the day's first range or after its last, is
outside it by definition, so "same day file 2109 of 2109" says nothing
about edges, and how often a lost trade sits at a day's edge was not
measured (#109 review).

**Live ground truth.** The only data in which the complete aggregate is
known is the live truncation of 2026-10-08 (#90 M0.3(b), ADR-048): 97 live
aggregates of run `20261008T035138Z` start at the archive aggregate's first
trade and end early. Folding the archive `trades` rows between the live `l`
and the next live `f` by the tail rule reproduces the archive row's `q` and
`l` exactly on **97 of 97**; no row in those ranges breaks the tail rule.

**Prediction.** Placing each of the 64 folded quantities of 2026-02-05 … 07
in `P.transact_time`'s bar and recomputing the residuals of all 5565 bars
of the ADR-031 window (the script reproduces the logged volume and
taker-buy mismatches exactly, 0 disagreements) predicts **1608** mismatches
for `archive-kline-check --trade-source aggTrades` after the fold: the
1658 of ADR-031 minus the 50 pure-gap bars ADR-047 attributed, with 0 new
mismatching bars. The 26 trades whose minute differs from `P`'s are all on
2026-06-08, outside that window.

**Why a tail.** Aggregate ids are consecutive over the whole imported year
(0 id breaks in 603 218 226 rows, `docs/data-availability.md`), so no whole
aggregate is missing; what is missing are trades at the end of an
aggregate's range. The live truncation has the same shape, measured on the
one dataset that holds both versions of the aggregate.

## Decision

1. **Correct, do not caveat (D1); supersedes ADR-047 D2.** Archive replays
   restore the uncovered trades.
   - Why: rule A failed (A1 on 5 days, A3 on 3, worst 15.04 ticks), so a
     caveat would hand consumers bars that are wrong by more than the
     error the pipeline accepts elsewhere, and the human decided on
     2026-10-11.
   - Rejected, the caveat: rule A fails, see above.
   - Rejected, an exclusion list: the human rejected it, and it would leave
     holes in 32 of the 41 sampled days of the window (78 %); on the 220
     unsampled days it could only list what is measured, which is nothing.
   - Rejected, re-planning without the defect window: it would drop 252 of
     the 365 backfill days, leaving 113, far below ADR-034's 12-month
     window, and live capture cannot stand in for it: its first event is
     2026-10-07, 0 days of overlap with 2025-10-01 … 2026-06-09 (#90
     M0.3(a)).

2. **The source is the `trades` archive (D2).** It is the only public
   dataset that holds the lost trades with their ids. Cost: 252 daily zips,
   8 437 684 717 bytes (8.44 GB) for 2025-10-01 … 2026-06-09, from HTTP
   `Content-Length` (#90 M0.2); the largest is 155 350 401 bytes
   (2026-02-05), which bounds the importer's staging, since a zip is
   deleted once imported (ADR-034 D5). Stored size, about 16–20 GB, by two derivations that agree:
   aggTrades take 14 815 MiB a year (ADR-034 addendum, `du` after #12),
   × 252/365 = 10 228 MiB, × the trades/aggTrades byte ratio 1.49–1.80
   measured on the 10 imported days (ADR-031 addendum) = 15 240–18 411 MiB
   (16.0–19.3 GB); and the aggTrades Parquet/zip ratio 36.8/17 = 2.16
   (ADR-034: one aggTrades day is 36.8 MB of Parquet, D5, from a 17 MB zip,
   Context) × 8.44 GB = 18.2 GB. If the true size is above
   20 GB the import's free-space gate (#102 M1: 25 GB = 20 GB + 5 GB, about
   7 days of live capture at 720 MB a day, ADR-038) stops it at a file
   boundary before live capture starves.
   - Rejected, a kline-derived fill: klines carry no trade ids, so a fill
     cannot tell a loss from the aggregation-boundary residual. Of the 6610
     1m bars with a nonzero volume residual on the 32 days, 200 hold a loss
     (3.03 %, #90 M0.2 C2); a fill would rewrite the other 6410, and the
     loss-free control days too (186 and 162 nonzero-residual bars on
     2026-06-10 and 2026-10-09, 0 losses).
   - Rejected, replaying `trades` instead of `aggTrades` on the window:
     those bars would be kline-exact, not aggTrades-equivalent, so 1584 of
     the 1658 mismatches of the February window (the boundary-only class of
     ADR-047) would change for reasons that are not the loss; the
     `trade_count` of a bar would switch from aggregates to raw trades;
     ADR-039 D5 refuses two streams in one trade-id space; and the sparse
     `trades` ids (ADR-045) turn off sequence-break detection.

3. **The tail fold (D3).** Per source day, both inputs in id order: a trade
   inside some range is covered; any other trade whose `P` is known is
   uncovered and is **folded into `P`** when its price and aggressor equal
   `P`'s **and every earlier uncovered trade between `P` and `N` was
   folded**. From the first trade of a gap that does not match on, the
   rest of that gap is unattributed (D4). `P` keeps its id, price,
   `transact_time` and side; only its quantity grows (exact `Qty`
   addition, an overflow fails the replay), and its restored range stays
   contiguous. A trade that matches both `P` and `N` (313 of 2109) is
   folded into `P`, because all 97 live truncations were tails. A repeated
   or regressing id is skipped and counted, as the live `StreamSequencer`
   drops it; a range with `first > last`, or one that overlaps the previous
   range, fails the replay naming the day and the aggregate.
   - **Edge ids.** After the day's last range, `P` is that range and `N`
     opens a later day: the trade is folded or gapped within the day.
     Before the day's first range, the trade is resolved against the
     previous source day's last range when the replay has that day (from
     its fold, or, for a day without `trades` files, from its `aggTrades`
     ranges): inside that range it is covered (ADR-047 D3's ids 706/707);
     above it, it is that range's tail, and whether the run above the
     range already broke is carried across midnight. Otherwise `P` is
     unknown and the trade is **unresolved**: counted and gapped (D4),
     never folded. The tail of the window's last aggregate can lie in the
     day after the window; the replay reads that day for it (the
     lookahead, D6), and when the store lacks it the tail is reported
     unchecked (D4).
   - Why edge ids are not ignored: ADR-047 D3 could ignore 706/707 because
     they were measured covered by the neighbouring day's range. A lost
     tail trade of a day's last aggregate is an edge id too: in the same
     day's file when it is timed before midnight, in the next day's when
     the aggregate spans midnight (the 706/707 shape with one trade lost).
     Ignored, it is dropped and the day reports `clean`, which breaks
     "anything it cannot fold surfaces as a gap and in the report"
     (Consequences). The tail rule needs only `P`, which is known in both
     cases.
   - Why only the run right above `P`: an aggregate is a contiguous id
     range, so once a trade of the gap is not `P`'s, no later id of that
     gap can be `P`'s either; folding it would give `P` a range that skips
     an id, a guessed composition (D4). M0 found 0 unattributed trades on
     the 32 days, so the restriction changes none of its numbers.
   - Why unresolved and not folded or ignored: without `P`'s row the fold
     cannot tell a trade covered by `P` (carried, not missing, ADR-047 D3)
     from one lost after it; folding would guess, ignoring would hide a
     possible loss.
   - Grounds: 2109 of 2109 tail, 0 head-only, 0 neither; 97 of 97 live
     truncations reproduced exactly; consecutive aggregate ids.
   - The folded quantity lands in `P.transact_time`'s bar, not the trade's
     own minute. 26 of the 2109 trades sit in another minute than `P`; the
     February prediction shows 0 new mismatches from the rule. Placing them
     in their own minute would need an event of their own (next bullet).
   - Rejected, one synthetic `Trade` per lost trade: consecutive aggregate
     ids leave no free id between two aggregates, and the engine requires
     strictly increasing trade ids (ADR-028 D5), so it needs a change to the
     domain event model for 2109 trades the fold places exactly.

4. **An unattributed or unresolved trade becomes a `MissingData` gap
   (D4).** An uncovered trade that does not fold (D3), and an unresolved
   edge id, is not folded. Per run the replay delivers one `FeedGap {
   stream: Trades, start: T(P), end: T(N), reason: MissingData }` right
   before `N` (from the window start when `P` lies before the window or is
   unknown), unless a gap already stands there. When `N` opens a later
   day, the gap goes before the first aggregate delivered above `P`. When
   the window ends before `N`, the gap is a trailing gap from the last
   delivered aggregate (or the window start) to the window end, the way a
   missing day or a live gap still open at the window's end is reported
   (ADR-039 D5); a window that ends before `T(P)` has none.
   - Why: a guessed fold would invent the aggregate's composition; a gap
     marks the touched bars `feed_gap` with no change to the event model or
     its hash encoding. M0 found 0 such inner trades on the 32 days, so
     this applies to the 220 unmeasured days of the window, to edge ids,
     and to a window's first day (unresolved, Consequences).
   - Why `MissingData` and not `SequenceBreak`: the aggregate-id sequence
     did not jump; data the source should hold is missing.
   - Why a trailing gap: a millisecond window (`--from`/`--to` in ms, an
     experiment's sample period) can end between `T(P)` and `T(N)`.
     Without it, the bars in the window after `P` would be unmarked while
     the report counts the trade (#109 review). A day-aligned window ends
     there only when `N` opens the next day: a gap after the last range
     of its last day, or the unchecked tail below.
   - **Unchecked tail.** When the window delivers the last aggregate of
     its last day, that day has `trades` files, and the store lacks the
     next day's `trades` or `aggTrades` (so the lookahead, D6, cannot
     fold it), a trade of the next day may be that aggregate's lost tail
     (the 706/707 shape with one trade lost) and nothing can tell. It is
     handled as an unresolved trade at the window end: a trailing gap from
     the aggregate to the window end, and a `fold unchecked tail <day>:
     aggregate <id>` line in the report (D7). When the run above the
     aggregate already broke, its gap is already there and nothing is
     added.
   - Why: #109 re-review N1. Ignored, a one-day window delivered the
     aggregate at 0.020 with the day `clean` where a two-day window
     delivered 0.025, and nothing marked the difference.
   - Why a line, not a count in the day's status: the day's own `trades`
     file is fully checked, and D7 counts an edge trade on the day of its
     `trades` file. What is unchecked is the next day's file, outside the
     window; the line names the aggregate and that day.
   - Why only the window's last day: inside the window the next day is in
     the report itself, `unchecked` without `trades` files and a missing
     day (a gap) without `aggTrades`.
   - Why not after an unchecked day: an unchecked day claims nothing about
     its aggregates (D6).
   - Why a gap although nothing may be lost: as for an unresolved trade,
     the replay cannot tell, so it marks rather than hides. A trailing gap
     is not delivered (ADR-039 D5), so it changes neither the event-stream
     hash nor the final state; it marks the report.
   - Rejected, failing the replay (human decision 4 at STOP 1 of #102): one
     unexpected trade on an unmeasured day would stop a 252-day replay.

5. **At replay time, from the verbatim files (D5).** The fold reads the
   sealed `aggTrades` and `trades` rows; nothing derived is written to the
   raw store. Grounds: raw data is immutable and the source of truth
   (ADR-003, ADR-022, ADR-030, CLAUDE.md invariant 4), and zips are deleted
   after import (ADR-034 D5), so a change of this rule needs a re-replay,
   not a re-download.
   - Rejected, an import-time folded stream: it puts derived data in the
     raw store.
   - Rejected, rewriting the stored rows: raw data is immutable.
   - Cost: when the first `aggTrades` file of a source day opens, that
     day's `aggTrades` parts and then its `trades` parts are read once more,
     each in `(min_event_time, path)` order and one sealed part at a time.
     A part holds at most 1 000 000 rows (the writer's `max_rows`, ADR-030
     D4), so memory stays at one part per input on top of the merge, whose
     12-month peak RSS was 362 MB (#11 acceptance). Time: about 54 min for
     the 252-day window — 20.5 min for the 12-month replay (#11
     acceptance) × 252/365 = 14.2 min, × 3.8 for the delivery pass (1), the
     fold's second `aggTrades` pass (1) and `trades` at up to 1.80 × the
     `aggTrades` bytes (ADR-031 addendum). The Accept-when run measures
     both; a slower replay costs time, not correctness. One more read is
     needed where a day with `trades` files follows a day of the replay
     that has none (D3's edge ids): that earlier day's `aggTrades` are read
     once for its last range. A window's first day has no previous day in
     the replay and reads nothing more (its head trades are unresolved,
     Consequences). Inside the import the read happens only for the
     control 2026-10-09 replayed with `--from 2026-10-08`; 2026-06-10's
     previous day, 2026-06-09, has `trades` files. The lookahead (D6)
     reads one more day per replay, the `aggTrades` and `trades` of the
     day after the window, when the store holds both: 1 of 253
     days for the Accept-when replay. Neither changes the estimate.

6. **Scope is the source day, with a one-range carry (D6).** Every `P` and
   `N` of the 2109 lay in the same day file as the trade (#102 M0), so a
   day's inner fold is complete when its files are. Only edge ids cross a
   day (D3). The fold of day D starts from D−1's last range and whether
   the run above it broke. D+1 is folded when D's first `aggTrades` file
   opens, so a head trade of D+1 that folds into D's last aggregate is
   known before that aggregate is delivered. Days fold in order, each
   once. On the window's last day D, D+1 lies outside the window: when the
   store holds D+1's `trades` and `aggTrades`, the replay reads them as
   well (the **lookahead** day), folds it like any D+1 and reports its
   status, but delivers none of it. The dataset version covers that day's
   files. `trades` stays opt-in at import (ADR-034 D2) and is imported for
   the 252-day defect window plus the controls 2026-06-10 and 2026-10-09
   (human decision 3 at STOP 1 of #102). A control D is judged in a
   replay from D−1 (Accept when, 5), so the store must also hold the
   `aggTrades` of D−1 and D, and for its kline check their `klines`: for
   2026-06-09 and 06-10 they are part of the year import; for 2026-10-08
   and 10-09, after its end (`aggTrades` 2025-10-01 … 2026-09-30,
   `docs/data-availability.md`), they are imported for #102 M3. Without
   D−1's `aggTrades`, D's head trades are unresolved and D−1 is a
   missing day, so the control would fail by construction, not by loss. A day without `trades` files is
   **unchecked** and delivered as normalized: the days from 2026-06-10 on,
   where ADR-047 D4 and #90 M0.1 measured 0 uncovered trades on 23 of 23
   sampled days, stay unchecked unless imported.
   - Why one range and a flag: they are all an edge id needs (`P`, and
     whether D3's run already broke), so the fold stays one sealed part at
     a time (D5).
   - Why fold D+1 ahead rather than hold D's last aggregate back: the
     replay is a streaming merge (ADR-039 D5) with a zero hold-back, so D's
     last aggregate is delivered as soon as it is merged; folding D+1 one
     day early costs no extra read, since every day is folded once anyway.
   - Why the lookahead (#109 re-review N1): without it the window's last
     aggregate was folded in a two-day window and not in a one-day one
     (0.025 against 0.020) for data the store holds, so a result depended
     on where its window ended. With it, a delivered aggregate's quantity
     does not depend on the window's end.
   - Why only when D+1 has `trades` files: without them there is nothing
     to fold, and folding D+1's `aggTrades` alone would add an `unchecked`
     day outside the window to every report. The tail is then unchecked
     (D4).
   - Why the version covers it: D+1's `trades` and `aggTrades` change the
     delivered quantity, so a version without them would name two
     outputs (ADR-039 D8). The selection takes every requested stream
     over D+1, so D+1's other files change the version too: a version
     that changes more often than needed, never one that misses a
     change.
   - Rejected, a gap for the window's last aggregate even when D+1's
     `trades` are in the store: it would keep the quantity dependent on
     the window's end for data the replay can read.

7. **Provenance is out of band (D7).** A consumer tells original from
   corrected data by three things, none of them in the event stream:
   - **The dataset version.** Under the fold, the `trades` files join the
     `aggTrades` selection, with the files of the lookahead day (D6), so
     the version covers them (ADR-039 D8, ADR-040 D5). When the store holds no `trades` file for the window, the fold
     reads nothing and delivers the data of `none`, so it keeps that
     version: every version recorded on a store without `trades` stays
     valid.
   - **The fold report.** `mie replay` prints a summary (days corrected,
     clean and unchecked as day ranges; aggregates, trades folded, quantity,
     unattributed trades, the edge part of the folds (trades and quantity)
     and unresolved trades of the corrected days), one `fold <day>` line
     per corrected day (the whole source day, edge ids counted on the day
     of their `trades` file; the lookahead day's status is listed with the
     window's days), a `fold unchecked tail` line when the window's last
     aggregate has one (D4), and one
     `fold minute <open ms>` line per 1m minute holding a folded aggregate
     delivered in the window, in a fixed order without a wall-clock field
     (ADR-039 D9). **A bar of any timeframe carries folded data iff it
     contains a reported minute.** `archive-kline-check` prints the summary
     line. The edge part is named apart because M0's numbers cover inner
     trades only (Context): Accept-when compares the inner part (folds
     minus edge folds) with them exactly.
   - **`--trade-correction none`**, which delivers `aggTrades` as published.
   - Rejected, an in-band flag on `Trade` or `Bar`: it needs event-stream
     hash encoding v2 (ADR-039 D7), a `Bar` state-hash change, and a field
     that live code never sets (human decision 1 at STOP 1 of #102).

8. **`fold` is the default (D8).** `mie replay --source archive`,
   `mie experiment run --source archive` and `mie archive-kline-check`
   fold unless given `--trade-correction none`. The flag is refused with
   `--source live` (live capture has no `trades` side input) and with
   `--trade-source trades` (nothing to correct). In code there is no hidden
   default: every `ArchiveReplay` and `open_requests` caller states the
   correction.
   - Why: the decision was "corrected, not caveated"; a correction that
     needs an opt-in leaves the caveat as the default.
   - Rejected, opt-in: see above. `none` keeps every earlier result
     reproducible instead (Consequences).

## Numbers and where they come from

| Number | Source | What a different value would change |
|---|---|---|
| 2109 uncovered trades, 201 1m bars, 32 of 41 days | #88 / ADR-047, reproduced by #90 M0.2 and #102 M0 | The Accept-when totals; a different count means a different input file (sha256) |
| 2109 / 0 / 0 tail, head-only, neither; 313 both | #102 M0 | Any head-only or neither on the 32 days would have stopped the design (pre-registered STOP) |
| 26 trades in another minute than `P` | #102 M0 | More such trades move more volume across minute boundaries; the February prediction bounds their effect there at 0 new mismatches |
| 97 / 97 live truncations | #90 M0.3(b), #102 M0 | Fewer would refute the tail shape on the one dataset with ground truth |
| 1658 / 50 / 24 / 1584, predicted 1608 | ADR-031, ADR-047, #102 M0 | The Accept-when kline check; another count after the fold means the fold or the prediction is wrong |
| tick 0.10 | `exchangeInfo` `PRICE_FILTER`, #90 M0 | A3's unit; a smaller tick makes more days fail A3 |
| 8.44 GB / 252 zips, largest 155 MB | HTTP `Content-Length`, #90 M0.2 | Download time and staging; staging holds one zip |
| 16–20 GB stored | D2's two derivations | Above 20 GB the 25 GB free-space gate of #102 M1 stops the import at a file boundary |
| 1 000 000 rows per part | writer `max_rows`, ADR-030 D4 | The fold's memory per input; a larger part raises it proportionally |
| about 54 min per 252-day replay | D5's derivation | Time only |
| 541.622 BTC folded on the 32 days | sum of #102 M0's per-day column | The Accept-when quantity check |
| edge ids: not measured in M0 | ADR-047's population is inner by definition (Context) | Nothing to predict: edge folds are recorded by the Accept-when run, not compared |

## Consequences

- **Archive dataset versions and event-stream hashes change under `fold`**
  for every window whose days have `trades` files and an `aggTrades`
  request; recorded results reproduce with `--trade-correction none`. The
  ADR-040 acceptance spec, which expects
  `stream 604001515:20929b98dfd17a44` (the ADR-039 12-month run), runs
  with `none` (ADR-040 is still proposed, so its Accept-when steps 1 and 2
  are edited in place). `mie experiment run`
  adds a hint to a data-version mismatch under `fold`.
- `mie replay` and `archive-kline-check` reports gain the correction in
  their header line and the fold lines, so their bytes differ from earlier
  reports in both modes; under `none` the dataset version and the
  event-stream hash are the earlier ones (pinned by a test).
- Event count, ids, prices, times and sides are unchanged by a fold; only
  folded aggregates' quantities differ, plus one gap per run of
  unattributed or unresolved trades (a trailing gap when the window ends
  inside one).
- A window's first day has no previous day in the replay. If its `trades`
  file starts inside the previous day's last aggregate (the 706/707 shape),
  those trades are unresolved: one gap at the window start and a nonzero
  `unresolved` count, though nothing may be lost. A window one day wider
  resolves them; the controls in Accept-when are judged that way.
- A window's last day reads the next day (the lookahead, D6), so a
  delivered aggregate's quantity does not depend on where the window ends.
  When the store lacks that day's `trades` or `aggTrades`, the last
  aggregate of a checked last day gets a trailing gap and a `fold
  unchecked tail` line (D4): every window that ends on the last imported
  `trades` day shows one, though nothing may be lost.
- The 2025-10-10 `trades` file is truncated at 22:03 UTC (ADR-031). The
  fold can only under-correct there: a trade absent from the file is never
  uncovered, so a loss in the missing part stays lost and the day status
  cannot show it.
- Negative: a folded replay reads `aggTrades` twice and `trades` once (D5),
  about 3.8 × the I/O of the window's `aggTrades`, plus the lookahead day.
- Negative: the tail rule is measured on the inner trades of 32 days; on
  the 220 unsampled days, and on edge ids everywhere, it is applied on
  that evidence. Anything it cannot fold surfaces as a gap and in the
  report, and the Accept-when run records the totals.
- Negative: on a partial window the `fold <day>` line describes the whole
  source day; only the minute lines are restricted to delivered
  aggregates. `archive-kline-check` reads one minute beyond each window
  end, so the neighbouring days appear in its summary.
- Live capture and live replay are unchanged.
- Not addressed: the live-stream truncation of 2026-10-08 and live
  detection (#90, ADR-048); importing `trades` beyond the defect window and
  the two controls; `archive-verify`.

## Alternatives considered

- **Caveat (ADR-047 D2 as it stood)** — D1: rule A fails.
- **Exclusion list** — D1: rejected by the human; holes in 32 of 41 sampled
  days.
- **Re-plan without the defect window** — D1: 113 of 365 days left, 0 days
  of live overlap.
- **Kline-derived fill** — D2: cannot separate the loss from the boundary
  residual; would rewrite 6410 loss-free bars and the controls.
- **Replay `trades` instead of `aggTrades`** — D2: changes 1584 bars for
  other reasons, `trade_count` semantics, one id space, sparse ids.
- **Synthetic trades** — D3: no free id between consecutive aggregates
  (ADR-028 D5).
- **Fail on an unattributed trade** — D4.
- **Fold at import time, or rewrite rows** — D5: derived data in the raw
  store; raw data is immutable.
- **In-band provenance flag** — D7: encoding v2, `Bar` state hash, a field
  live never sets.
- **Opt-in correction** — D8.

## Accept when

The #102 M3 acceptance run on the PR branch, after `trades` is imported for
2025-10-01 … 2026-06-09 and the controls 2026-06-10 and 2026-10-09, passes
all five:

1. **Coverage (rule A).** `mie replay --source archive --from 2025-10-01
   --to 2026-06-09` (fold) reports 0 unchecked days in the window. On each
   of the 32 measured days, trades folded minus edge trades folded equals
   the M0 count (2109 in total), the folded quantity minus the edge
   quantity equals the M0 quantity exactly (541.622 BTC in total), and
   unattributed is 0. With no uncovered trade left, rule A's lost set is
   empty, so A1, A2 and A3 pass on all 32 days. Unresolved is 0 on every
   day after 2025-10-01. The edge folds of every day and the totals of the
   220 unsampled days are recorded, unattributed and unresolved trades
   included.
   - Why the inner part: M0 measured inner trades only (Context), so the
     edge folds that D3 adds (#109 review) are not part of its 2109 trades
     or 541.622 BTC. Subtracting them keeps M0's numbers an exact check of
     the inner fold. Edge folds are recorded, not compared, because nothing
     predicts them.
   - Why unattributed 0 includes edge ids: an edge trade is the same kind
     of trade, the tail of `P`. One that does not match `P` would falsify
     D3 at the edge, and acceptance stops to examine it instead of
     accepting it as a gap.
   - Why unresolved 0 after the first day: every later day has its
     previous day in the replay, so its head trades resolve against that
     day's last range (D3). A nonzero count there means the carry failed,
     or a trade filed on D belongs below D−1's last aggregate, a shape no
     measured day has shown. Either stops acceptance. 2025-10-01's count,
     whose previous day is outside the replay (Consequences), is recorded.
   - The lookahead day 2026-06-10 (D6): the replay reads its `aggTrades`
     and `trades`, which are in the store, so the tail of 2026-06-09's
     last aggregate is checked and no `fold unchecked tail` line appears.
     06-10's status is listed with the window's days but is not one of
     them: it is not counted in the 252-day totals. An edge fold on it is
     06-09's last aggregate's tail and is recorded with the edge folds; an
     inner fold, unattributed or unresolved trade on it fails control 5.
     Before #109 re-review N1 this tail was not checked at all.
2. **Determinism.** The same replay a second time prints byte-identical
   output. With `--trade-correction none` the event count is the same (plus
   one gap per unattributed or unresolved run, if any) and the
   event-stream hash differs. Peak RSS is recorded.
3. **Kline check, February window.** `mie archive-kline-check --from
   2026-02-05 --to 2026-02-07` reproduces 1658 mismatches under `none` and
   gives exactly 1608 under `fold`, and the mismatch set under `fold` is a
   subset of the set under `none`.
4. **Kline check, the 32 days.** On each day, one 1-day check per mode: the
   mismatch set under `fold` is a subset of the set under `none`.
5. **Controls 2026-06-10 and 2026-10-09.** Precondition: the store holds
   the `trades` of D and the `aggTrades` and `klines` of D−1 and D (D6).
   Per control day D, a fold replay `--from D−1 --to D` reports D `clean`,
   with 0 unresolved, or `corrected` with only edge trades folded into
   D−1's last aggregate (0 inner folds, 0 unattributed, 0 unresolved). The
   kline-check mismatch lists of D are identical in both modes. The
   one-day replays of D in both modes have identical event-stream hashes
   and final states, unless the one-day fold reports unresolved head
   trades. In that case, the only difference is their one gap at the
   window start and the bars it marks.
   - Why D−1 in the window: D's first trades may lie in D−1's last
     aggregate (the 706/707 shape). A one-day replay cannot see that range,
     so it reports them unresolved by design (D3), which does not mean
     anything is lost. `clean` is judged where the range is in the
     replay. The kline check needs no wider window, because it already
     reads the minute before D (Consequences).
   - Why an edge fold into D−1's last aggregate passes: D7 counts it on
     D, the day of its `trades` file, but the aggregate it restores is
     D−1's (06-09 in the defect window, 10-08 unchecked), which lies
     outside the control's question: whether a loss-free day's own
     aggregates stay unchanged. AW1 sees the same fold on 06-10's
     lookahead status.
   - The `fold unchecked tail D` line and its trailing gap: the store has
     no `trades` of D+1 (2026-06-11, 10-10), so the tail of D's last
     aggregate cannot be checked (D4). It is expected and does not fail
     the control. Its gap is not delivered, so the hashes and final states
     above compare as before. Importing D+1 is not required: D7 counts
     such a trade on D+1, not on D, and D+1 would then be a checked last
     day whose own tail needs D+2, so the line would move by a day rather
     than go away.

References: ADR-003, ADR-022, ADR-028, ADR-030, ADR-031, ADR-034, ADR-038,
ADR-039, ADR-040, ADR-045, ADR-047, ADR-048. Issues: #88, #90, #102.
