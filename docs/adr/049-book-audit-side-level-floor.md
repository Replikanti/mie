# ADR-049: Order-book checkpoint audit — per-side level floor, `inconclusive` verdict, exact window width

- Status: accepted
- Date: 2026-10-11

Relation to earlier ADRs: this ADR supersedes two parts of ADR-038. The
first is decision 8's `matched` verdict: no minimum of compared levels per
side, and the window depth in whole bps. The second is the KEEP verdict of
ADR-038's 2026-10-11 justification addendum, section c.6. ADR-038's
decisions 1–7 and 9, the rest of decision 8, and every other number in it
stay in force. ADR-038 is accepted, so its text is not edited; only its
status line names this ADR (`docs/adr/README.md`, Lifecycle).

## Context

ADR-038 decision 8 audits each re-anchored checkpoint. It compares the book
rebuilt from diffs with the fresh snapshot plus its straddling diff, inside
the intersection of both trusted windows, and journals `matched` with the
total of levels compared and a window depth in bps. Nothing requires either
side to compare any level.

The measurement for ADR-038's justification addendum (#73, section 10:
<https://github.com/Replikanti/mie/issues/73#issuecomment-6106591720>)
found checkpoints whose window, on one side, held no level of either book.
The price had moved further within the 60 s interval than the previous
anchor's window was wide, so that window's edge lay beyond the new best
price. The checkpoint still counted as `matched`, and `bps()` reported the
distance from the best price to that edge as a positive window depth.
`bps()` takes the absolute distance and truncates it to whole bps, so an
edge beyond the best reads as a positive width, and any width under 1 bp
reads as 0. The addendum kept the rule (c.6, KEEP) and deferred a minimum
to a superseding ADR; #98 is that ADR plus the code change.

### The replay behind this ADR, and the rule that fired

The plan of #98 replayed the #10 soak (runs `20261008T223350Z` and
`20261008T231852Z`, 812 checkpoints) through the new audit. It fixed a
self-check and a decision rule before measuring. Results and harness:
<https://github.com/Replikanti/mie/issues/98#issuecomment-6113056026>.

- **Self-check: passed on 812 of 812.** The audit produced as many results
  as the journal has lines per run (44 + 768). The 809 unchanged verdicts
  reproduce the journaled level totals. Every journaled whole-bp width
  equals the exact width truncated, and every new width equals it rounded
  up to hundredths. The 14 rows of #73 section 10 reproduce side, level
  count and width.
- **The pre-registered rule fired.** It expected exactly 2 checkpoints to
  move from `matched` to `inconclusive` at a floor of 1. The replay moved
  **3**. The third is 2026-10-09 11:46:50 UTC. Its bids compared 974
  levels and its asks **0**: the ask edge lay 35.84 bps below the fresh
  best ask, after the best price rose 50.27 bps in the 60.3 s since the
  previous anchor. Its journal line reads `"levels":974`,
  `"window_ask_bps":35`, `matched`.
- **Why #73 counted 2.** Section 10 picked the "narrow side" of each
  checkpoint by the smaller `bps()` value. `bps()` read the empty ask side
  of 11:46:50 as 35 bps wide, so the bid side (19 bps, 974 levels) became
  the "narrow" one, and the empty side was never looked at. The figures
  "2 of 812" and "below 100 levels the narrow-side counts are {0, 0, 57}" in
  #73 and in ADR-038 addendum c.6 are therefore an **undercount**: they rank
  sides with the defect this ADR fixes. Ranked by the smaller of the two
  per-side level counts, the values below 100 are {0, 0, 0, 57}, and 3 of
  812 checkpoints (0.37 %) had an empty side. ADR-038's text keeps "2 of
  812"; this ADR is where the correction is recorded.
- **What the miss changes, and what it does not.** The two other stop
  conditions about the data held: every change was `matched` →
  `inconclusive`, and no side compared between 1 and 56 levels. The third
  case is the same defect as the other two: a window emptied by price
  movement. So the floor below, its derivation and every decision stand.
  Only the counts change.

| Checkpoint (UTC, 2026-10-09) | Empty side | Other side | Edge beyond the fresh best | Best-price move since the previous anchor |
|---|---|---|---:|---:|
| 01:26:41 | ask, 0 levels | bid, 1001 levels, 16.68 bps | 0.02 bps | +16.26 bps in 60.4 s |
| 11:46:50 | ask, 0 levels | bid, 974 levels, 19.22 bps | 35.84 bps | +50.27 bps in 60.3 s |
| 11:49:51 | bid, 0 levels | ask, 1001 levels, 16.51 bps | 2.64 bps | −19.63 bps in 60.3 s |

A fresh `limit=1000` snapshot is 13.94 bps wide on its narrower side at
least (#73 section 6). Each of the three moves is larger than that, and
the largest is inside the 60 s excursion maximum of 54.6 bps in #73
section 5.

## Decision

1. **Per-side counts (D1).** `mie_domain::book::BookComparison` reports
   `bid_levels` and `ask_levels`, the distinct prices compared on each side;
   `levels_compared()` is their sum. A `matched` verdict is a claim about
   each side, and a total cannot show an empty side: 974 + 0 reads as 974.
   `BookComparison` is in no state hash or encoding.
2. **A floor per side (D2).** `MIN_SIDE_LEVELS = 1`
   (`mie_adapter_binance::book_audit`). A comparison without a mismatch is
   `matched` only when each side compared at least that many levels;
   otherwise it is the new verdict `inconclusive`. The value is a code
   constant, not a run parameter: it decides what a verdict means, and a
   per-run value would make verdicts of two runs incomparable.
3. **Precedence (D3).** Any mismatch gives `mismatched`. Otherwise a side
   below the floor gives `inconclusive`. Otherwise the verdict is
   `matched`. A difference is evidence of a defect whatever the coverage of
   the other side, so a thin side never hides it. `unverifiable` and
   `invalidated` are unchanged; they arise before any comparison runs.
4. **Width semantics and unit (D4).** Each side reports the width of its
   compared window from the fresh book's best price to the window edge. The
   bid width is `best − edge` and the ask width `edge − best`. The width is
   0 when the edge lies beyond the best price, because no level is then
   inside the window. The unit is hundredths of a basis point of the best
   price (`cbps`), rounded up. It is absent when the side has no window or
   the fresh book has no best on it. The previous `bps()` (absolute distance,
   truncated to whole bps) is removed.
5. **Journal fields (D5).** A `book_checkpoint` line with `result` =
   `matched` or `inconclusive` writes `levels` (the total, kept so the
   field means the same as before), `levels_bid`, `levels_ask`,
   `window_bid_cbps` and `window_ask_cbps`. An `inconclusive` line also
   writes `min_side_levels`, the floor that judged it. The keys
   `window_bid_bps` and `window_ask_bps` are no longer written: the value
   changed meaning (rounded up instead of truncated, 0 beyond the best,
   hundredths instead of whole bps), so it gets a new name with the unit in
   it.
6. **`capture-report` check 6 (D6).** `inconclusive` checkpoints are
   counted on the order-book row and are not a failure. They do not count
   as `matched` for the presence rule, so a run whose depth spans more than
   twice its checkpoint interval still fails without a `matched` one. The
   window statistic reads only `window_*_cbps` from `matched` lines and
   prints bps with two decimals. A line from before this ADR is judged by
   its `result`; its whole-bp widths are left out of the statistic, so the
   two units are never mixed.

### Numbers

- **`MIN_SIDE_LEVELS = 1`.** Floors over the 812 checkpoints. "Flagged"
  means a journaled `matched` checkpoint whose smaller per-side count is
  below F.

  | F | flagged | `matched` left, run 1 (44) | `matched` left, run 2 (768) |
  |---:|---:|---:|---:|
  | 1 | 3 | 44 | 765 |
  | 50 | 3 | 44 | 765 |
  | 57 | 3 | 44 | 765 |
  | 58 | 4 | 44 | 764 |
  | 100 | 4 | 44 | 764 |
  | 699 (p10) | 80 | 44 | 688 |
  | 894 (median) | 406 | 25 | 381 |

  The smaller per-side count has min 0, p1 288, p10 699 and median 893
  (nearest rank); below 100 its values are 0, 0, 0 and 57.

  - Why 1: on one side, `matched` claims that the compared levels are
    equal. With 0 levels it claims nothing; with k ≥ 1 it claims exactly
    those k levels. 1 is the only value whose meaning does not depend on
    the data: "the comparison saw at least one level on that side" against
    "it saw nothing". The defect in #98 is the zero-evidence case, and only
    that.
  - What breaks at 0: the status quo. An empty side counts as `matched`,
    as on 3 of the 812 soak checkpoints.
  - What breaks at 2 to 57: nothing observable on this data, because every
    value in that range flags the same 3 checkpoints. That is the problem:
    such a value would have no derivation.
  - What breaks at 58 and above: 11:51:52 UTC is discarded, a real
    comparison of 57 levels at 1.16 bps. At p10 (699) 80 checkpoints go.
    At the median (894) half go, and run 1 keeps 25 of 44. A floor that
    high measures how far the price moved, not whether the audit saw
    anything. A larger value would also need larger test books: the fake
    transport's book holds one ask level.
  - Raising the floor later takes a superseding ADR on new data, which is
    the right path for a value the current data does not constrain.
- **Hundredths of a basis point, rounded up.** The tick is 0.10 USDT
  (`PRICE_FILTER`, ADR-036). At the best prices of the narrow checkpoints
  in #73 section 10 (81 833 to 83 317 USDT), one tick is 1.22 to 1.20 cbps,
  so the unit resolves a single tick up to 100 000 USDT, where one tick is
  exactly 1 cbps. Whole bps hid the case that showed the defect: an edge
  0.02 bps beyond the best read as "0 bps", like a real window under 1 bp.
  Rounding down in hundredths would report a one-tick window as 0 above
  100 000 USDT. Rounding up overstates by less than one hundredth and never
  reports a positive width as 0, so 0 means exactly "no level inside". The
  value is a `u32` and saturates instead of overflowing. It would saturate
  only above 42.9 million bps, which a window between two books of one
  price never reaches.
- **No threshold on the `inconclusive` count.** Not a fraction, and not a
  fail-on-any. On the soak, each of the 3 followed a move of the best price
  larger than a fresh window (16.26, 50.27 and 19.63 bps against at least
  13.94 bps). That is market movement within one 60 s interval, not a
  provider or sync defect. Failing on it would turn every fast session red
  with no defect behind it. A rate threshold needs a failure to tie its
  value to, and the soak has none: 3 of 812, all explained by the move.
  The presence rule already fails a run in which nothing matched.

## Consequences

- An empty side no longer counts as evidence. On the #10 soak the verdicts
  become **809 `matched`, 3 `inconclusive`**, and `capture-report` still
  prints PASS: both runs have `matched` checkpoints, and `inconclusive` is
  no failure.
- **Undetected case.** A defect that empties one side on only part of the
  checkpoints passes, as long as some checkpoints in the run still match.
  The presence rule is per run (ADR-038 addendum c.5), and `inconclusive`
  is counted, not judged. **The event that would justify a rate
  threshold:** a run where `inconclusive` occurs without a preceding move
  of the best price larger than the previous window. That is an empty side
  the market does not explain. The count on the order-book row and the
  per-side fields of each line are what show it.
- A side with exactly 1 compared level counts as matched and proves only
  that level. Nothing stops a near-empty side; the per-side counts in the
  journal now make it visible.
- **Narrow windows are inconclusive more often.** The recorded `limit=100`
  fixture (`crates/mie-adapter-binance/tests/fixtures/depth*.jsonl`, a 2 s
  checkpoint interval) has one checkpoint. It compares 101 bid levels and
  0 ask levels: in 2 s the best ask rose past the first snapshot's deepest
  ask; a `limit=100` window is about 1.5 bps wide on its narrower side
  (median, #73 section 6).
  Before this ADR it counted as `matched`. Now it is
  `inconclusive`, and `tests/book_fixtures.rs` pins that. The `matched`
  path stays covered by `tests/live_fake_transport.rs`. Live capture uses
  `limit=1000` (ADR-038 decision 3), where a fresh window is about 15 bps
  wide.
- **Journal schema change (Breaking).** Readers of `window_*_bps` must
  read `window_*_cbps`. On main the only readers are in this repository
  (`mie-cli`'s `journal.rs`, `report.rs` and their tests).
- **Old journals are not re-judged.** `capture-report` judges a line from
  before this ADR by its `result`, so the soak journal of #10 still reads
  812 `matched`. The 3 empty sides in it appear only in a replay with the
  new audit, which the harness in #98 does out of tree. There is no
  re-audit command.
- The domain's `BookComparison` changes shape (D1). No hash, encoding or
  golden value moves, because the comparison is not part of the Market
  State.
- ADR-038's decisions 1–7 and 9, the rest of decision 8 (`mismatched`,
  `unverifiable`, `invalidated`, the failures of `capture-report` and the
  presence rule), and every other number of ADR-038 and its addendum stay
  in force.

## Alternatives considered

- **No minimum** (ADR-038 c.6 KEEP). Rejected: a `matched` verdict with an
  empty side claims an equality nobody checked, on 3 of 812 soak
  checkpoints and on the recorded fixture.
- **A width floor instead of a level floor.** Rejected: a width is a
  distance from the fresh best, not a count of evidence. A wide but sparse
  window passes it, and an edge beyond the best has no natural width. The
  0.02 bps case shows that a width floor would also draw a line through
  real one-tick windows.
- **Silently dropping the checkpoint.** Rejected: it hides the event from
  the report and shrinks the counts without a trace. The undetected case
  above would then not even be counted.
- **Reusing `unverifiable`.** Rejected: `unverifiable` means that no
  comparison was possible because an `OrderBook` gap came between, and it
  carries a `GapReason`. A thin side is a comparison that ran but compared
  too little. The report must tell the two apart.
- **Failing the report on `inconclusive`.** Rejected: every observed case
  followed a price move larger than the window, so it would fail fast
  markets with no defect behind them (Numbers, last item).
- **A floor of about 50 or above.** Rejected: on this data, values up to
  57 cannot be told apart from 1, and values from 58 up discard real
  comparisons (Numbers, first item).
- **Keeping the key names `window_*_bps` with the new meaning.** Rejected:
  old and new journals would become silently incomparable, and journals
  are the evidence record that ADRs cite.
- **Marking ADR-038 `superseded by ADR-049` in full.** Rejected: its
  decisions 1–7 and 9 still stand (PR #97 keeps them). A full supersession
  would mark them as replaced. The status line names only the superseded
  parts, as ADR-028 and ADR-032 do.

## Why accepted without an `Accept when`

No live soak is needed. Capture, sync and the pipeline are unchanged. The
audit is a pure function of the released events, and #73 showed that a
replay of the audit equals the production audit on 812 of 812 checkpoints.
The replay in #98 runs the code of this ADR over the same events, with the
self-check above passing on all 812.
