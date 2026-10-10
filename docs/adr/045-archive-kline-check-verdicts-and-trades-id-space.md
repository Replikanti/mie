# ADR-045: Archive kline check — an INCONCLUSIVE verdict on low coverage, and a sparse trade-id space for the archive `trades` dataset

- Status: accepted
- Date: 2026-10-10

Relation to earlier ADRs: this ADR narrows ADR-039 D5 ("per raw stream the
live `StreamSequencer` drops repeats and turns a trade-id jump into
`SequenceBreak`") for one dataset, the archive `trades` files. ADR-039 and
ADR-031 stay `accepted` and are not edited; the ADR process has no
partial-supersede state, so the narrowing is recorded here and
cross-referenced from the code (`sequence.rs`, `archive/replay.rs`).
ADR-031's justification addendum ("Continuity") says a continuity rule of
its own for the `trades` id space is an ADR-028/ADR-032 level change; this
is that ADR, limited to the check-only dataset.

## Context

#85: `archive-kline-check --trade-source trades` prints `PASS` on near-zero
evidence. The re-run of the ADR-031 February window on 2026-10-10 (ADR-031
justification addendum, "Accept when") compared 5 complete bars, skipped
5566 of 5571 as incomplete and still printed `PASS`, because the verdict
was "at least one bar compared and every compared bar matched". The
`aggTrades` source reproduced the #12 report exactly (5565 compared, 1658
mismatches). Two questions follow: what verdict a run that compared almost
nothing deserves, and why nearly every bar was incomplete under `trades`.

### Why the bars were incomplete (measured)

Source: the Binance public archive files
`data/futures/um/daily/{trades,aggTrades,klines/1m}/BTCUSDT/*-2026-02-05.zip`,
the first day of the February window, sha256 equal to the published
`.CHECKSUM` of each zip, counted in one pass over the CSV rows.

| Quantity (2026-02-05, BTCUSDT) | `trades` | `aggTrades` |
|---|---|---|
| rows | 19 581 560 | 9 542 097 |
| id span (last − first + 1) | 19 591 772 | 9 542 097 |
| id discontinuities (`id != prev + 1`) | 9 881 | 0 |
| skipped ids in them | 10 212 (1 id: 9 554 breaks, 2 ids: 323, 3 ids: 4) | 0 |
| duplicates / id regressions / time going backwards | 0 / 0 / 0 | 0 / 0 / 0 |
| 1m bars containing at least one break | 1440 of 1440 | 0 of 1440 |

The sum of the 1440 kline `count` values of that day is 19 581 560, exactly
the `trades` row count: the exchange's own klines count the same trades,
so the skipped ids were never public trades. The import is faithful (its
ledger records 19 581 560 rows for that file). The pipeline reads the right
field (column 0, `id`), the file is strictly id-ordered with non-decreasing
time, and an archive replay pushes every event in one session, so every gap
of the re-run was a `SequenceBreak` from an id jump. With about 6.9 breaks
per minute (9 881 / 1 440) every 1m bar of the day contains one, and every
longer bar contains 1m bars, which is the 5566 of 5571. The rule "a trade
id other than `last + 1` is a gap" (ADR-039 D5, `sequence.rs`) is right for
live `aggTrade` and for archive `aggTrades` (0 breaks above, 0 over the 12
backfilled months, ADR-031 addendum) and wrong for the archive `trades` id
space, where the exchange itself skips ids.

## Decision

1. **Three verdicts (D1).** `KlineCheckReport::verdict()` returns `Pass`,
   `Fail` or `Inconclusive`, in this precedence: any mismatch fails; a run
   that compared no bar fails (unchanged: a window without the trade
   stream or the klines is no evidence); more than
   `MAX_EXPECTED_INCOMPLETE` bars skipped as incomplete is inconclusive;
   otherwise the run passes. `archive-kline-check` exits 0, 1 and 3
   respectively. Why a third verdict: `FAIL` would claim that bars
   disagree with the klines when none did, and `PASS` would claim evidence
   that does not exist. Why exit 3: 0 and 1 keep their meaning for scripts
   that already test them, and 2 is the CLI's usage-error code.
2. **Coverage next to the verdict (D2).** Right before the verdict line the
   check prints `coverage: compared C of C+S closed bars, S skipped as
   incomplete (expected at most 6)`. The counts are bars, not trades (#85
   says "skipped trade counts"): the check compares bars, and a bar is the
   unit a gap removes from the evidence. The `INCONCLUSIVE` line names the
   trade source and the gaps that make bars incomplete for it, and points
   at `archive-verify`, which lists them.
3. **Allowance: 6 incomplete bars (D3).** `MAX_EXPECTED_INCOMPLETE =
   Timeframe::ALL.len()` = 6. Derivation: in a clean window the only
   incomplete bars are the `partial_start` bars, one per timeframe: the bar
   containing the first consumed trades-stream event (ADR-031 decision 6),
   which opens in the 60 s trade margin before the window. Measured: the
   #12 runs skipped exactly 6 in both windows (`incomplete bars skipped:
   6`), the 2026-10-10 `aggTrades` re-run of the February window skipped 6
   (5565 of 5571 compared), and the offline fixture of this change skips 6
   under both sources. What another value breaks: at 0 every clean run is
   inconclusive. Above 6 a mid-window feed gap could pass: a gap marks every
   open bar it overlaps (ADR-031 decision 6), at least one per timeframe,
   so the smallest gap adds 6 incomplete bars; at 12 one short gap passes,
   at hundreds (a day has 1855 bars) a window whose gaps removed most of
   its 1h, 4h and 1d evidence passes. A percentage threshold was rejected
   for the same reason (below).
4. **The archive `trades` id space is sparse (D4).** The archive replay
   builds the `trades` sequencer with `allowing_trade_id_gaps()`: a trade id
   above `last + 1` is delivered without a `SequenceBreak` gap. Repeated and
   regressing ids are still dropped and counted, a session change is still
   a `Disconnected` gap, and source days missing from the store are still
   `MissingData` gaps. Live capture (`pipeline.rs`) and archive `aggTrades`
   keep the strict rule; the default `StreamSequencer::new` is unchanged.
   The bars are still built by the one domain path from the same `Trade`
   events (ADR-019); only the adapter-level continuity rule of one dataset
   differs. Scope: `trades` is excluded from default replays and refused
   next to `aggTrades` (ADR-039 D5), and the bars consumers see come from
   aggregate trades (ADR-031 addendum), so in practice the rule applies to
   the kline cross-check alone. Default replays keep their event-stream
   hashes.
5. **What still detects a lost trade in `trades` (D5).** The id jump was the
   signal for a lost trade. For this dataset it carries none (9 881 jumps
   on a day whose kline counts sum to its row count), and the check that
   reads the dataset detects losses by what it compares: volume,
   taker-buy volume and OHLC of every complete bar against the exchange
   kline. The one known defect of the dataset, `BTCUSDT-trades-2025-10-10`
   missing trades from 22:03 UTC, was found by that comparison (153 bars),
   not by an id jump (ADR-031 addendum).

## Consequences

- The February window is expected to compare 5565 bars and skip 6 under
  both sources again (`PASS`, exit 0; 0 mismatches under `trades`, the
  same 1658 under `aggTrades`), and the October window 5565 under `trades`
  with the 153 mismatches of the 2025-10-10 defect (exit 1). That
  regenerates the ADR-031 acceptance evidence that the 2026-10-10 re-run
  could not; the real-data output is posted on the PR of #85.
- A run with a real feed gap, or with a source day missing from the store,
  is `INCONCLUSIVE` (exit 3) instead of `PASS`. Scripts that test exit 1
  against 0 see no change for passing or mismatching runs; exit 3 is new.
- Negative: the id-jump signal is gone for `trades`. A loss that leaves
  every complete bar's volume, taker-buy volume and OHLC equal to the
  kline cannot be seen by this check. Such a loss would also have to keep
  the kline trade counts unaffected, since those are counted by the
  exchange; the check reports trade-count differences (not a mismatch for
  `aggTrades`, where they are expected).
- Negative: `INCONCLUSIVE` reports only how many bars were skipped, not
  which gap caused them; `archive-verify` and `mie replay` list the gaps.
- Not addressed: the aggregates' trade-id ranges of 2026-02-05 cover
  19 583 973 ids, 2 413 more than the `trades` file holds. It is the same
  class as the 2025-10-10 defect, it does not change the rule above, and
  the kline comparison is what detects such days.

## Alternatives considered

- **Remove the `trades` source from the check.** Rejected: ADR-031's
  acceptance rule ("an aggTrades mismatch is explained when the same bar
  matches under `--trade-source trades`") needs it for its 1658 explained
  mismatches, and the source exists for no other reason (`normalize.rs`).
- **Keep the strict rule and accept a permanent `INCONCLUSIVE` for
  `trades`.** Rejected: it leaves the `trades` source, which exists only
  for the kline cross-check, dead, and with it the explained-mismatch
  evidence of ADR-031.
- **Tolerate id gaps per bar** (count skipped ids per bar and pass a bar
  with at most N). Rejected: N would have no measurement behind it (the
  day above has up to 3 skipped ids per break and about 7 breaks per
  minute), it still turns a public-trade-free id into a gap, and the bar
  comparison already decides whether a bar's trades are complete.
- **A percentage coverage threshold** (for example 99 % of closed bars
  compared). Rejected: there is no measurement behind 99 %, and on a
  three-day window (5571 bars) 1 % is 55 bars, which lets several real
  feed gaps pass; the allowance of D3 is derived from what a clean window
  can skip.
