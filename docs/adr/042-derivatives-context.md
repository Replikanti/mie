# ADR-042: Derivatives context — open interest, funding and liquidations

- Status: proposed
- Date: 2026-10-09

## Context

The research brief (§8: "OI, ΔOI and OI velocity") and the Market State &
Regime brief ask for derivatives context on Market State: open interest
(OI), funding, mark price and liquidations. Issue #19 sets the scope:

- OI level, ΔOI and OI velocity at the live polling resolution and at the
  archive's 5-minute resolution for history. Each value exposes its
  resolution, so research never mixes the two silently;
- the funding rate, the mark price and the time to the next funding;
- liquidation flow by side over windows, as a lower bound;
- point in time: a value becomes visible at its observation time.

The raw data differs by source (`docs/data-availability.md`):

- live open interest is a 10 s REST poll (`resolution_ms` 10 000). It
  reaches the core re-timed to its delivery time, a few seconds after the
  exchange sampled it (ADR-032 D12);
- archive open interest is the 5-minute `metrics` series (`resolution_ms`
  300 000), ordered at `create_time + 5 min`, the end of its interval
  (ADR-034 D3). Its spacing is exact;
- mark price and the indicative funding rate are live only (1 s);
- funding settlements are archive only: live capture has no in-band
  settlement event;
- liquidations are live only, from a throttled stream, so every count is a
  lower bound.

Earlier decisions constrain the answer:

- One domain path for live and replay (ADR-019), in the canonical event
  order (ADR-028), on exact fixed point with explicit overflow (ADR-027).
- Every feature is a registered `id@version` with an append-only lock and
  a golden test per version (ADR-029). Raw data is the source of truth
  (ADR-022). A feature earns its place by measured contribution (ADR-013).
- Rolling windows over closed 1m bars, with the gap policy of ADR-035 D3:
  windows never go back to warming up and carry the OR of their minutes'
  flags (ADR-031, ADR-035).
- ADR-032 D12 left one question open: the exchange sampling time is not on
  `OpenInterest`; it was deferred until a feature consumes open interest.
- One source per replay (ADR-039 D6): live and archive data are not
  stitched in one replay.
- Nothing here may become a direction or a signal (ADR-012, ADR-023,
  ADR-024).

## Decision

1. **OI at the source resolution** (`derivatives.oi.sample@1`).
   - The value is the last OI sample: its ordering time, `open_interest`,
     `resolution_ms` and an optional step against the previous sample. The
     step holds `previous_time`, the exact `delta` (`Qty`) and
     `elapsed_ms`.
   - The step exists only when all of these hold:
     - a previous sample exists with the same `resolution_ms`, and that
       resolution is positive;
     - no `FeedGap` on the open-interest stream arrived since that sample;
     - `0 < elapsed_ms ≤ resolution_ms + step_tolerance_ms`, with
       `step_tolerance_ms` = 15 000 (derivation below).

     Archive spacing is exact (ADR-034 D3), so one missing 5-minute row
     (600 s) breaks the chain; live spacing jitter (2.0 to 18.9 s between
     consecutive samples on clean spans, measured below) does not.
   - **Resolution change.** The first sample at a new resolution has no
     step; the chain restarts there.
   - **Gap policy.** The level stays `Ready`: it is a real observation that
     carries its own time. A gap only breaks the next step.
   - **Velocity** is derived on demand, never stored:
     `(delta / 1e8) × 60 000 / elapsed_ms`, in BTC per minute; `None`
     without a step.
   - **Why `step_tolerance_ms` is 15 000.** At the live resolution the
     cut-off is `resolution_ms` 10 000 + 15 000 = 25 000 ms between two
     consecutive samples. The number answers one question: how far apart
     can two consecutive live samples be without a hole between them? Too
     low, and ordinary poll jitter breaks the ΔOI chain and drops steps
     that are real. Too high, and a hole (a missed poll or an outage the
     sequencer did not mark) is bridged as a step; the step still carries
     its `elapsed_ms`, so velocity stays exact, but it is no longer a
     10 s step. A `FeedGap` breaks the chain regardless of the tolerance,
     so the tolerance only governs unmarked spacing.

     The value was first set from reasoning (the 10 s re-time allowance of
     ADR-032 D12 plus unmeasured REST jitter) and a spacing range
     of 6–24 s that sat 1 s under the cut-off. It is now checked against
     the data of the #9 and #10 soaks (source: the raw stores of both
     soaks, the exchange `time` of each `openInterest` payload; measured
     2026-10-09, <https://github.com/Replikanti/mie/issues/69#issuecomment-6082384614>).
     15 783 samples over 4 runs (#9: 2 runs, #10: 2 runs); spacings are
     computed within a run, run boundaries excluded. No payload `time` differs
     from the stored event time and no exchange time is duplicated. The
     journals show 15 783 `oi_poll` persisted, 1 error, 0 not persisted,
     and 3 open-interest gaps, all `Disconnected`.

     | Spans | n | min | p50 | p90 | p99 | p99.9 | max | > 20 s | > 25 s |
     |---|---|---|---|---|---|---|---|---|---|
     | clean (no OI gap) | 15 772 | 1 975 ms | 10 422 | 13 306 | 16 312 | 17 320 | 18 928 | 0 | 0 |
     | overlapping an OI gap | 7 | 6 929 | 10 560 | 19 329 | 19 329 | 19 329 | 19 329 | 0 | 0 |
     | all | 15 779 | 1 975 | 10 422 | 13 311 | 16 313 | 17 329 | 19 329 | 0 | 0 |

     The poll interval is 10 000 ms (`OI_POLL_INTERVAL_MS`), so the p50 of
     10 422 ms is 422 ms above it, and the clean maximum of 18 928 ms is
     the poll interval plus 8 928 ms of jitter. **Rule:** `step_tolerance_ms`
     stays 15 000 only if the clean-span maximum is within the cut-off with
     a stated margin. It is: 25 000 − 18 928 = 6 072 ms of margin, and no
     spacing in any span exceeds 20 s. The number therefore stays and no
     `derivatives.oi.sample@2` follows. A different value would be a new
     version of the feature (ADR-029), that is code, and would need new
     evidence first.

     **What the table does not measure.** It is the spacing of the
     exchange `time`, which is what the raw store holds. The step compares
     the *delivery* times of ADR-032 D12: a late sample is delivered at
     `last released + 1`, shifted by at most `oi_retime_ms` (10 000 ms)
     from its exchange time, so a delivered spacing can differ from the
     table's by up to that shift in either direction. The margin above is
     therefore a margin on the exchange spacing. The delivered spacing is
     measured in *Accept when*, item 3.
2. **OI on the 5-minute UTC grid** (`derivatives.oi.5m@1`). Live (10 s) and
   archive (5 min) data both produce it, so a condition learned on archive
   history can be evaluated live.
   - At each boundary B, a multiple of 300 000 ms, the level is the latest
     OI sample with ordering time ≤ B. This is point in time by
     construction.
   - **Closing boundaries.** Boundaries close on OI events only. A sample
     at `t` closes `L = open of the 5m interval containing t` when L is
     newer than the last closed boundary. L takes the sample itself when
     `t == L` (archive samples land exactly on boundaries) and the previous
     sample otherwise. If `L − 300 000` is also newer than the last closed
     boundary, it is closed first with the previous sample, so L's delta
     still has its reference. A boundary without any sample at or before it
     is not closed. Only the last closed boundary is kept as the reference,
     so a jump across days costs O(1): no per-boundary loop and no time
     jump error.
   - **Freshness.** A boundary is `Ready` only when `B − sample_time ≤
     max_age_ms` (60 000). Otherwise it is `Unavailable` (`InputInvalid`),
     which is what a boundary after a feed hole becomes. Feed gaps need no
     separate rule.
   - **Value:** `boundary`, `open_interest`, `sample_time`, `resolution_ms`
     and `delta` against the previous boundary. `delta` exists only when
     the previous closed boundary is `B − 300 000`, was `Ready` and has the
     same `resolution_ms`. Velocity is derived as in decision 1, over
     `grid_ms`.
3. **Mark price and indicative funding** (`derivatives.mark@1`).
   - The value is the last `MarkPrice`: `time`, `mark_price`,
     `index_price`, the indicative `funding_rate` and `next_funding_time`.
   - Derived exactly, `None` on overflow:
     - `basis = mark − index`, as a `Price`;
     - `time to next funding = next_funding_time − at`, in ms. Consumers
       pass `MarketState::as_of`; the canonical text uses the value's own
       time.
   - The stream is live only, so the value stays warming up in archive
     replays. Gap policy: none; the value keeps its own time.
4. **Settled funding** (`derivatives.funding.settled@1`). The value is the
   last `FundingSettlement` (`time`, `rate`). It is archive only today.
5. **Liquidation windows** (`derivatives.liq.window.<5m|15m|1h>@1`).
   - **Windows.** A window is the last N closed 1m bars (N = 5, 15, 60) and
     steps with the 1m bars, as in ADR-035 D1, so it ends exactly where
     `flow.window.*` ends. A liquidation counts in the minute that contains
     its time; the developing minute is never included.
   - **Sides.** A `Sell` liquidation order closes a long and counts in
     `long_count` / `long_qty`; a `Buy` order closes a short and counts in
     `short_count` / `short_qty`. The quantity is `filled_qty`. Every value
     is a lower bound, because the stream is throttled.
   - **Warm-up gate.** The archive has no liquidations, so its zeros would
     look exactly like a quiet live window. The windows therefore stay
     warming up with 0 observed samples until the first liquidations-stream
     event (a liquidation or a gap). From the minute that contains that
     event, they count closed minutes; that minute is flagged
     `partial_start`.
   - **Gaps.** A liquidations gap becomes known only at its end (ADR-028),
     after minutes it overlaps may already have closed. On the gap event,
     every minute overlapping `[start, end]` is flagged `feed_gap`, and the
     current windows are recomputed: a window that overlaps the gap gains
     the flag. Windows never go back to warming up (ADR-035 D3 reasoning). A
     window's `feed_gap` and `partial_start` are the OR of its minutes'.
6. **Point in time.** A value becomes visible at the ordering time of the
   event that carries it (ADR-028): archive OI at `create_time + 5 min`
   (ADR-034 D3), live OI at its re-timed delivery time (ADR-032 D12). This
   settles the deferral in ADR-032 D12: features consume the delivery
   time, and the event model is unchanged.
7. **Exactness.** Sums and deltas use `Qty` and `u64` with checked
   arithmetic; time differences use checked `i64`. Floats are derived on
   demand in the orders above and never stored, so `MarketState` stays
   `Eq`. Overflow rejects the event as `StateError::Overflow` and leaves the
   state unchanged.
8. **Features** (the ids are permanent):
   - `derivatives.oi.sample@1`: parameters `gap_policy` `break_chain` and
     `step_tolerance_ms` 15 000; input open interest; warm-up `Samples(1)`,
     where a sample is an OI event.
   - `derivatives.oi.5m@1`: parameters `grid_ms` 300 000 and `max_age_ms`
     60 000; input open interest; warm-up `Samples(1)`, where a sample is a
     closed boundary.
   - `derivatives.mark@1`: no parameters; input mark price; warm-up
     `Samples(1)`.
   - `derivatives.funding.settled@1`: no parameters; input funding;
     warm-up `Samples(1)`.
   - `derivatives.liq.window.5m@1`, `.15m@1`, `.1h@1`: parameter
     `window_ms` 300 000 / 900 000 / 3 600 000; inputs liquidations and
     `bars.time.1m@1`; warm-up `Samples(N)`, where a sample is a closed 1m
     bar from the first liquidations-stream event on.

   `MarketState.derivatives` carries the seven values. Each names the
   feature that produced it. They are stepped on the same atomic path as
   bars and order flow: a rejected event changes none of them.

## Consequences

- The live-only features (mark price, liquidation windows) stay warming up
  in archive replays.
- The archive has only settled funding and live capture has only the
  indicative rate, so a funding condition learned on the archive is not
  evaluable live from Market State, and the reverse.
- The native live step inherits the re-time jitter of ADR-032 D12: its
  `elapsed_ms` measures delivery spacing, not exchange sampling spacing.
- **Grid lag risk.** Archive OI is ordered at the end of its interval
  (ADR-034 D3), but which instant the archive value measures is
  unverified. Archive-sourced grid values may lag live-sourced ones by one
  step. `resolution_ms` (300 000 vs 10 000) identifies the source; *Accept
  when* measures the lag, and its outcome table decides what follows.
- Liquidation gaps are over-marked: the sequencer reports a sparse
  stream's gap from its last event, not from the disconnect, so minutes
  before the outage can carry `feed_gap`.
- Nothing here emits a direction, a side bias or a signal (ADR-012,
  ADR-023, ADR-024). Divergences between CVD, OI and price belong to the
  order-flow candidates (#22).
- `MarketState.derivatives` joins the Market State hash (ADR-041) after
  `structure`, with an encoder next to each new type. Under ADR-041 a new
  feature family extends encoding v1 without a bump. Runs recorded with
  the previous feature set compare events only.
- Mark price (1/s) and OI (every 10 s) touch only a small copied head per
  event. The liquidation minutes are a fixed ring of 61 slots keyed by
  minute, copied only on a liquidations-stream event.

## Alternatives considered

- **The exchange sampling time on `OpenInterest`** (the deferral in
  ADR-032 D12). It would change the event model, the ADR-028 payload
  tie-break and event-stream hash v1 (ADR-039 D7) to fix a jitter of about
  10 s, which the 5-minute grid absorbs. Rejected.
- **One ΔOI chain across resolutions.** That is exactly the silent mixing
  issue #19 forbids. Rejected.
- **Inferring a settlement from the indicative rate** when
  `next_funding_time` advances. The indicative rate is an estimate, not the
  settled rate. Rejected.
- **Deriving `next_funding_time` from settlement spacing.** The funding
  interval is not guaranteed to stay fixed. Rejected.
- **Sliding event-time windows over liquidation events.** They could not
  be lined up with the order-flow windows. Rejected.
- **Liquidation notional** for `@1`. It can ship as `@2` if research
  justifies it (ADR-013). Rejected for `@1`.

## Accept when

1. **Archive versus live open interest.** Which instant does an archive
   `metrics` row measure: its `create_time` T or T + 5 min (the ordering
   time of ADR-034 D3)? This is measured, then recorded here; no outcome is
   decided in advance.

   - **Prerequisite (satisfied 2026-10-09).** Overlap needs archive rows
     for the days the live capture covers. The archive store ended at
     2026-09-30 (`docs/data-availability.md`, the window of #12) and live
     raw open interest starts at 2026-10-07 15:50 UTC (the start of the #9
     soak, ADR-032 *Acceptance*). `metrics` was therefore imported for
     2026-10-01 to 2026-10-08 with `mie archive-import --streams metrics
     --from 2026-10-01 --to 2026-10-08` (ADR-034; idempotent,
     checksum-verified): imported 8, failed 0, 2 304 rows (288 rows a day
     × 8). Source:
     <https://github.com/Replikanti/mie/issues/69#issuecomment-6082384614>.
     The contiguous range was chosen over the two overlap days alone
     because the import costs one small file a day. The import changes the
     archive dataset version; the acceptance record names the version
     used.
   - **Overlap.** The query runs over the days live capture and archive
     both cover, chosen by live coverage: the #9 soak (2026-10-07 15:50 to
     2026-10-08 22:00 UTC, about 30 h) and not the hole between the two
     soaks (2026-10-08 22:00 to 22:33). About 30 h is about 360 archive
     rows (12 a hour).
   - **Query (fixed here).** For each archive row, take its T. Compare
     `sum_open_interest` with the live OI sample nearest to T and with the
     one nearest to T + 5 min, by the live sample's raw-store event time
     (the exchange `time`, not the re-timed delivery time of ADR-032 D12).
     Before comparing, check that both series are in the same unit (BTC).
     Exclude a row when no live sample lies within one poll interval
     (10 000 ms) of T or of T + 5 min, and a row where the two nearest live
     samples carry the same value (it cannot discriminate), and report both
     exclusion counts with the number of rows left.
   - **Sample size.** A p99 differs from the maximum only with at least 100
     rows, and one day is 288 rows (a 5-minute series,
     `docs/data-availability.md`), which puts 3 rows above its p99. The p99 is reported when at least 100
     rows remain; below that, only the maximum.
   - **Decision rule.** The statistic is the share of rows whose nearest
     live instant is T + 5 min. If the archive carried no information
     about the instant, that share would be 50 % (a coin flip), with a
     standard deviation of √(0.25 / n); at n = 288 that is 0.0295, so a
     share of 59 % is 3 standard deviations above the coin flip. A smaller
     threshold would let noise decide, a larger one would leave real
     effects inconclusive. A share of at least 59 % means the archive
     measures T + 5 min. A share of at most 41 % means it measures T.
     Anything between is inconclusive. For another n after the exclusions,
     the same 3 standard deviations apply (50 % ± 1.5 / √n; for example
     60.6 % at n = 200), and the session running the query states n and
     the threshold used. Report the median and the p99 absolute difference
     at both instants.
   - **Outcomes (none is pre-decided):**

     | Outcome | What follows |
     |---|---|
     | Measures T + 5 min | ADR-034 D3 stands; the grid lag risk (*Consequences*) is closed. |
     | Measures T | Choose between amending the archive ordering (a code and golden-value change, and a new ADR superseding ADR-034 D3, per the lifecycle in `docs/adr/README.md`) and documenting the one-step lag with `resolution_ms` as the discriminator (the current text). The choice and its reason are recorded here. |
     | Inconclusive | Record the share, n and the threshold; no change; repeat with more days (each day adds 288 rows; later days are published by the archive with a delay). |

     The session that runs the acceptance records the result here and files
     any follow-up issue; the follow-up is not part of this ADR's text.
2. **Equivalence coverage of the derivatives family.** The live/replay
   run of #13 covers it with zero divergences. The run's coverage list is
   ADR-041's *Accept when*: all seven streams, so liquidations, open
   interest and mark price are present, with the liquidation windows
   turning `Ready` in-run (its item 5). `derivatives.funding.settled@1` is
   archive only (decision 4) and is not coverable live; ADR-041 lists it
   among the limits, and its coverage rests on the fixture tests and
   golden values.
3. **Delivered open-interest spacing.** From the OI events of the run in
   item 2 (delivery times, as the engine sees them), count consecutive
   events more than 25 000 ms apart without a feed gap between them; each
   is a missing step. Record the count, the number of OI events and the
   maximum spacing. The cut-off is the one derived in decision 1. A count
   of 0 confirms `step_tolerance_ms` on the delivered quantity the step
   uses; a count above 0 is recorded with the maximum, and the follow-up
   (which would be `derivatives.oi.sample@2`, ADR-029) is filed by the
   session that runs the acceptance.

References: ADR-012, ADR-013, ADR-019, ADR-022, ADR-023, ADR-024, ADR-027,
ADR-028, ADR-029, ADR-031, ADR-032, ADR-034, ADR-035, ADR-039, ADR-041.
