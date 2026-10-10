# ADR-047: The `aggTrades` archive omits individual trades — measured extent, and what it changes in ADR-031's explained mismatches

- Status: accepted
- Date: 2026-10-10

Relation to earlier ADRs: ADR-031 (acceptance rule, "Accept when") and
ADR-045 ("Not addressed" bullet) stay `accepted` and are not edited; the
ADR process allows an addendum only for text that explains an existing
decision and "changes no ... normative statement" (`README.md`). A new class
of explained mismatch with a measured proportion is new content, so it is
recorded here and cross-referenced, as ADR-045 did for ADR-039. #90 decides
what to do about the archive and about live capture; this ADR records what
was measured and what it does and does not establish.

## Context

ADR-031 was accepted on the #12 backfill run (2026-02-05 … 02-07: 5565
bars compared, 1658 mismatches under `--trade-source aggTrades`, 0 under
`trades`). It called the 1658 "aggregation-boundary effects": an aggregate's
constituent trades can fall in another minute than its `transact_time`
(ADR-034 D8). ADR-045 noted, without explaining it, that the aggregates'
trade-id ranges of 2026-02-05 cover 19 583 973 ids, 2 413 more than the
`trades` file holds. #88 asked whether that difference, and the mismatches,
hide trades the `aggTrades` archive does not contain.

All numbers below were measured on 2026-10-10 by the main session on the
public archive files of `data.binance.vision` (USDⓈ-M, BTCUSDT daily
`trades`, `aggTrades`, `klines/1m`), each zip verified against its
published `.CHECKSUM` (sha256) before use. The scripts (`measure.py`,
`attribute.py`, `fetch.sh`), the sha256 of every file and the 64 uncovered
trades of 2026-02-05 … 07 are posted in full in the results comment of #88:
<https://github.com/Replikanti/mie/issues/88#issuecomment-6102401779>. They
are not committed: the measurement is a one-off over archive files, and the
archive-verify tooling is deliberately unchanged here (see Alternatives).

Definition. An **uncovered trade** of a day D is a row of
`BTCUSDT-trades-D.zip` whose id lies inside the day's aggregate span (first
`first_trade_id` … last `last_trade_id` of `BTCUSDT-aggTrades-D.zip`) but
inside no aggregate's `first_trade_id..last_trade_id` range: the trade exists
in the exchange's `trades` file and in no aggregate. Rows before the first
or after the last range of D are edge ids and are resolved against the
neighbouring day's files, not counted as uncovered.

### The 8 trades of 2026-02-05 and the 2 413

Files of 2026-02-05: `trades` sha256
`6b975b5429a24456c243038ea0a76a572293caa7daa39865a6d65af6d14025b4`,
`aggTrades` sha256
`b21d10ebecef8723c735e6fc1d992a7948437156a41c9db46e3a05540c4e2cb6`.
Eight trade rows (ids 7204381784, 7204770511, 7205587056, 7206514856 …
859, 7216236894; times 04:54:31Z … 19:49:31Z) are covered by no aggregate
range, and there are no others on that day. The identity measured for the day closes exactly:

```
range sum - trades rows = 2 413
  = 2 419 ids covered by an aggregate range but absent from the trades file
  +   2   aggregate ids beyond the last trade row of the day
  -   8   trade rows covered by no range
```

The identity closes on every one of the 63 measured days (below). The net
2 413 of ADR-045 therefore hides two opposite effects: 2 419 ids that lie in
aggregate ranges and have no `trades` row, and 8 trades that have a `trades` row and no aggregate. Only
the second is a loss of a trade from `aggTrades`.

### Edge ids

#88 named ids 7221144705 … 707 at the end of 2026-02-05. Id 705 is the last
row of `trades-2026-02-05` and is covered. Ids 706 and 707 are the first two
rows of `trades-2026-02-06` (00:00:00.012 and .028) and are covered by the
last aggregate range of 2026-02-05, which is the "2 aggregate ids beyond
the last trade row" above. They are **carried to the next day, not missing**.
The same two-way edge check (previous day's aggregates for the head,
next day's `trades` for the tail) was used for every measured day with
neighbours; the 2026-10-09 end-of-day edge needs the 2026-10-10 files and is
part of the late run below.

### Attribution of the 1658 mismatches (2026-02-05 … 07)

The mismatch list is `accept85-2026-02-05-aggTrades.log`, the #85
acceptance run (`main` @ f6f2738, aggTrades dataset hash `c1be9345…`).
`main` has had no commit since, so the current binary yields the same list.
The window has 64 uncovered trades (2026-02-05: 8, 02-06: 30, 02-07: 26).
Each trade was mapped to its UTC bar of every timeframe; for each such bar
the volume residual `kline volume − aggTrades volume − missing quantity` was
computed with exact decimals.

| Class of the 1658 mismatching bars | Bars | Share of 1658 |
|---|---|---|
| Touched by an uncovered trade, residual 0 after adding its quantity (pure gap) | 50 | 3.02 % |
| Touched, residual non-zero (gap plus boundary effect) | 24 | 1.45 % |
| Not touched by any uncovered trade (aggregation boundary only) | 1584 | 95.54 % |
| Touched bars that did **not** mismatch | 0 | — |

50 + 24 + 1584 = 1658. The last row says every bar an uncovered trade falls
in mismatches: a missing trade is never absorbed by the boundary effect.
Plausibility check of the script: the 64 trades sit in 17 distinct 1m bars,
each in at most 6 timeframes' bars, so at most 6 × 17 = 102 bars can be
touched; 74 were. (An earlier bound of 36 assumed the 8 trades of one day
and does not apply to the 3-day window.)

### Recurrence: 63 measured days

| Set | Days | Days with uncovered trades | Uncovered trades |
|---|---|---|---|
| Issue days 2026-02-05 … 07 | 3 | 3 | 64 |
| 2025-10-09 … 11 (ADR-031's other window) | 3 | 3 | 16 |
| 2026-10-09 | 1 | 0 | 0 |
| Pre-registered sample: 15th of each month, 2025-10 … 2026-09 | 12 | 7 | 545 |
| Widened sample: 1st, 8th, 22nd of each month, 2025-10 … 2026-09, plus 2026-10-01 and 2026-10-08 | 38 | 18 | 1407 |
| Cut-over narrowing 2026-06-09 … 14 | 6 | 1 | 77 |
| **Total** | **63** | **32** | **2109** |

Results, from the per-day counts in the #88 comment:

- Up to and including 2026-06-09: **32 of 41** measured days have uncovered
  trades, 2109 trades in total, peak 2026-05-08 … 06-09 at up to 478 a day
  (2026-05-15). Before 2026-05-08, 0 … 26 a day, with zero days among them (for
  example 2025-11-01, 2025-12-01 … 12-22, 2026-01-01).
- The last uncovered trade is at 2026-06-09T02:35:29Z. From **2026-06-10 on,
  0 of 22** measured days (2026-06-10 … 06-15, then the
  sampling dates through 2026-10-09).
- They come in bursts: the 2109 trades sit in 201 distinct minutes, up to
  178 in one minute (2026-05-15 00:02); 172 of them lie in second 31 of a
  minute; otherwise no fixed pattern was found.
- Control: 2025-10-10 shows the known defect of its `trades` file (761 722
  ids inside aggregate ranges absent from the file, and 13 aggregate ids
  beyond the truncated file that 2025-10-11 does not contain either), so the
  script sees both directions: a loss in `trades` and a loss in `aggTrades`.

### Sample design, and why

- **12 days, the 15th of each month (pre-registered).** Fixed before the
  run, so no day could be chosen for its outcome. 12 because the imported
  window is 12 months (ADR-034): one day per calendar month covers every
  month of the backfill once. The 15th is a mid-month day, so the
  neighbouring days that edge ids need lie in the same month, and the weekday changes from month to month, which removes a fixed
  weekday bias in part (not entirely: 12 days are 12 weekdays).
- **Widened to the 1st, 8th and 22nd after the first sample hit.** The
  decision recorded at STOP 1 of #88 was to widen only if any sample day
  showed uncovered trades. 7 of the 12 did, so the 1st/8th/22nd (with the
  15th, a 7-day grid) of each month were added, again by calendar and not by
  outcome, plus 2026-10-01 and 2026-10-08 at the end of the window.
- **Narrowed to 2026-06-09 … 14 (exploratory, not pre-registered).** The
  grid showed 2026-06-08 with 350 uncovered trades and 2026-06-15 with 0;
  the six days between them were measured to find the day the loss stops.
  That is how "from 2026-06-10" and the last trade at 02:35:29Z on 06-09
  were obtained, and the statement below is limited accordingly.
- **What 0 hits can and cannot show.** With zero events in `n` independent
  days, the 95 % upper bound of the per-day rate is `1 − 0.05^(1/n)`: 22.1 %
  for the 12-day sample alone (rule of three: 3/12 = 25 %), 12.7 % for the
  22 days since 2026-06-10 (rule of three: 3/22 = 13.6 %). To bound the rate
  below 5 % at 95 %, 59 zero-hit days would be needed (`ln 0.05 / ln 0.95`
  = 58.4). 95 % is the conventional level, not a project requirement; a
  different level moves the bound, not the conclusion. The days are not
  random draws and bursts cluster in time, so the bounds are indicative.
  The 22 days **detect recurrence, they do not prove rarity**: "no loss
  since 2026-06-10" is a measurement over 22 sampled days, consistent with
  an upstream fix around 2026-06-09, not a proof of one.

### Late run

`*-2026-10-10.zip` and `*-2026-10-11.zip` were not published when this was
measured (HTTP 404 at 20:59 UTC on 2026-10-10; daily files appear the
following day). They are a pending late run, to be measured after
publication and reported on #88, which stays open until then.

## Decision

1. **ADR-031's rule stays; its explained class is split (D1).** The
   pre-registered rule "an aggTrades mismatch is explained when the same bar
   matches under `--trade-source trades`" is unchanged and still gives 0
   unexplained for the February window, so ADR-031's acceptance stands. What
   changes is how the explained class is described: of the 1658, 1584
   (95.54 %) are aggregation-boundary effects only, 50 (3.02 %) are fully
   explained by trades missing from `aggTrades`, and 24 (1.45 %) by both. The
   statement "differ at aggregation boundaries ... as expected" is true for
   1584 bars and incomplete for 74 (4.46 %). Why not change the rule to
   exclude the 74: they match under `trades`, which is the property the rule
   tests, and the rule's job was to separate a pipeline defect from an
   upstream behaviour; the 74 are upstream.
2. **The loss is real and bounded in time, recorded as a caveat (D2).**
   Missing trades in the `aggTrades` archive are measured on 32 of 41 days
   up to 2026-06-09 and on 0 of 22 days from 2026-06-10. The aggregate-trades
   row of `docs/data-availability.md` carries that caveat. This ADR does not
   correct the archive or the pipeline: how large the effect is for a
   consumer (50 of 1658 mismatching bars are fully explained by the missing
   quantity on the issue window; no feature-level impact was measured) and
   whether to correct or only warn is decided in #90, with the numbers here.
   Why not correct now: the correction would be a design change of the
   backfill (merge `trades` into `aggTrades`, or replace the source), which
   needs its own plan and ADR and is out of #88's measurement scope.
3. **Edge ids are carried, not missing (D3).** Ids 7221144706 and 707 belong
   to `trades-2026-02-06` and are covered by 2026-02-05's last aggregate
   range. The question of #88 about them is closed.
4. **The recurrence statement is a measurement, not a guarantee (D4).** The
   cut-over at 2026-06-09/10 is stated with its bound (≤ 12.7 % per day at
   95 %). If the late run (2026-10-10 / 11) finds an uncovered trade, the
   statement is falsified and a new ADR supersedes D4 and re-scopes #90; if
   it finds none, the result is recorded on #88 and this ADR is not edited
   (an accepted ADR changes only its status line), the count of zero-hit
   days growing from 22 to 24.

## Consequences

- Archive-derived volume and trade counts of the `aggTrades` source are
  lower than the exchange's by the uncovered trades on the affected days
  (2025-10-01 … 2026-06-09). The measured size on the issue window: 64
  trades in 17 distinct 1m bars; on the worst measured day (2026-05-15) 478.
  The impact per derived feature is not measured here (#90).
- Negative: **`aggTrades` id continuity cannot detect this loss.** The
  continuity the pipeline checks is the aggregate id (`agg_trade_id`, column
  0: `normalize.rs` maps it to the event's trade id, which the sequencer
  checks, ADR-032 / ADR-039 D5). It stays consecutive when a trade is
  missing from the ranges: 0 breaks across the whole year in
  `docs/data-availability.md`, on days that have hundreds of uncovered
  trades. Only a comparison that involves the trade-id space sees the loss:
  against the `trades` file (as here) or against kline volume. A check that
  the ranges themselves are contiguous (`first_trade_id` of an aggregate
  equals the previous `last_trade_id` + 1) would flag an uncovered trade by
  definition, but no such check exists; whether to build any of these is
  #90's decision.
- Negative: **the live path is not measured.** Live capture (`aggTrade`
  stream, ADR-032) has no `trades` counterpart, so this measurement says
  nothing about whether the live feed loses the same trades. It stays an
  open risk, tracked in #90, whose scope includes whether to compare closed
  1m bars' kline volume with the captured `aggTrade` volume as a detector.
- Negative: "0 of 22 since 2026-06-10" is a sample. 22 days out of the
  122 from 2026-06-10 to 2026-10-09 inclusive were measured (6 consecutive
  days, then the 1st/8th/15th/22nd grid, 2026-10-08 and 10-09); it says
  nothing about the other 100 days, and the bound in D4 assumes independent days.
- The 2 413 of ADR-045 is explained: 2 419 − 8 + 2. ADR-045 stays correct
  (its statement is about the net). This measurement shows that the net
  contains two parts of opposite sign; it does not establish what the 2 419
  ids with no `trades` row are (the sparse id space of ADR-045 D5, or rows
  the file lacks), so ADR-045's "same class as the 2025-10-10 defect" is
  neither confirmed nor refuted for them.
- Not addressed: why the exchange's `aggTrades` files miss these trades,
  and whether it is a publishing or an engine behaviour. The measurement
  shows when, not why.

## Alternatives considered

- **A justification addendum to ADR-031 / ADR-045.** Rejected: an addendum
  may only explain existing decisions and change no normative statement
  (`README.md`). The split of the explained class and the recurrence
  measurement are new content.
- **A gap detector in `archive-verify` or `mie-app` now.** Rejected for
  this issue: #88 scopes a measurement, and whether detection is needed
  depends on #90's decision on correction versus caveat and on live
  capture, which this ADR hands over with the numbers.
- **Correct the archive now (merge `trades` into the aggregate stream).**
  Rejected here, see D2: a design change that needs its own plan.
- **Measure all 365 days.** Rejected: it multiplies multi-GB downloads for
  a question the 63-day sample already answers (recurrence and the
  cut-over), and the daily files for the sampled days are the only ones the
  decision needs; a detector in D2's follow-up would cover the rest.
- **Commit the measurement scripts.** Rejected: they run once against
  external files; the comment carries them in full, with per-file hashes,
  and a reproducible tool belongs to #90's decision on detection.

References: ADR-022, ADR-031, ADR-032, ADR-034, ADR-039, ADR-045. Issues: #88
(measurement, open for the late run), #90 (archive correction vs caveat, live
detection).
