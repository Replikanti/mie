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
       `step_tolerance_ms` = 15 000: the 10 s re-time allowance of ADR-032
       D12 plus REST response jitter.

     Archive spacing is exact (ADR-034 D3), so one missing 5-minute row
     (600 s) breaks the chain; live re-time jitter (deliveries roughly
     6–24 s apart) does not.
   - **Resolution change.** The first sample at a new resolution has no
     step; the chain restarts there.
   - **Gap policy.** The level stays `Ready`: it is a real observation that
     carries its own time. A gap only breaks the next step.
   - **Velocity** is derived on demand, never stored:
     `(delta / 1e8) × 60 000 / elapsed_ms`, in BTC per minute; `None`
     without a step.
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
  when* measures the lag.
- Liquidation gaps are over-marked: the sequencer reports a sparse
  stream's gap from its last event, not from the disconnect, so minutes
  before the outage can carry `feed_gap`.
- Nothing here emits a direction, a side bias or a signal (ADR-012,
  ADR-023, ADR-024). Divergences between CVD, OI and price belong to the
  order-flow candidates (#22).
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

1. A DuckDB query over raw data, on at least one day covered by both live
   capture and the archive: for each archive row, compare
   `sum_open_interest` at `create_time` T with the live raw OI nearest to T
   and nearest to T + 5 min. Record here which instant the archive
   measures, with the median and p99 absolute difference. If it measures T,
   record the one-step lag of archive-sourced grid values. No code change
   follows: ADR-034 D3 stands.
2. The live/replay equivalence run of #13 covers the derivatives family
   with zero divergences.

References: ADR-012, ADR-013, ADR-019, ADR-022, ADR-023, ADR-024, ADR-027,
ADR-028, ADR-029, ADR-031, ADR-032, ADR-034, ADR-035, ADR-039.
