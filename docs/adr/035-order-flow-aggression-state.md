# ADR-035: Order-flow and aggression state

- Status: proposed
- Date: 2026-10-07

## Context

The Market State & Regime brief ("Order Flow / Aggression State") and the
research brief (§8) ask for a deterministic order-flow state that keeps
aggressor flow apart from passive liquidity and from the price response it
gets: aggressive buy and sell volume and delta, CVD, trade intensity,
large-trade activity, and price response to aggression. Issue #17 leaves
several definitions open:

- how CVD is anchored and what a feed gap does to it;
- which rolling windows exist and how they step;
- what a "large" trade is;
- how intensity and price response are measured.

Earlier decisions constrain the answer:

- Trades carry the aggressor side (ADR-021); raw data is the source of truth
  and every feature is reproducible from it (ADR-022). Aggression is not
  direction (ADR-023), and no feature here may become a signal (ADR-024).
- Quantities are exact fixed point at 1e-8; floats only for derived
  statistics in a defined order; arithmetic is explicit about overflow
  (ADR-027).
- One domain path for live and replay (ADR-019), in the canonical event
  order (ADR-028).
- Every feature is a registered `id@version` with an append-only lock and a
  golden test per version (ADR-029).
- Bars are built from trades on six UTC-aligned timeframes, and every bar
  already carries aggressive buy volume, sell volume and delta (ADR-031,
  decision 3). Incomplete bars carry `partial_start` or `feed_gap`.
- Live capture plans a sub-second reconnect gap on every stream once a day
  (ADR-032, decision 7). ADR-033 keeps incomplete bars with data in its
  series for the same reason.
- A feature earns its place by measured contribution (ADR-013).

Per-bar delta therefore ships already; this decision adds what spans bars.

## Decision

1. **Rolling windows: 5m, 15m and 1h over closed 1m bars.** A window is the
   last N closed 1m bars (N = 5, 15, 60) and steps once per closed 1m bar.
   There is no intrabar value: the developing minute is excluded. Because
   the windows sit on the 1m grid, a window that ends on a 5m, 15m or 1h
   boundary has exactly the volumes, delta and trade count of that
   timeframe's closed bar.
2. **CVD under two explicit anchors**, as two features:
   - **Continuous** (`flow.cvd.continuous@1`): the sum of signed aggressor
     quantity (buy `+qty`, sell `−qty`) since the first consumed trade, the
     run anchor. It continues through trades gaps and counts them. The
     value carries `anchor` and `gaps`. Its level is relative to the run,
     like the ATR seed (ADR-033), so it is not comparable across replay
     windows. The difference between two values is the exact market delta
     when both carry the same `anchor` and `gaps`.
   - **UTC day** (`flow.cvd.utc_day@1`): the CVD since 00:00 UTC. It equals
     the delta of the developing `bars.time.1d@1` bar by construction and
     carries that bar's coverage. Its value is reproducible from any replay
     window that starts at or before the day's open; its coverage follows
     ADR-031, so a window starting exactly at 00:00 reports `partial_start`,
     and only an earlier start yields `complete`.
3. **Gap policy.** Windows never go back to warming up. A window carries
   the OR of its minutes' coverage; an empty incomplete minute contributes
   zeros and its `feed_gap` flag. This is the ADR-031 bar policy, and
   ADR-033's reasoning applies: with a reset on every gap, a 60-minute
   window would be unavailable for an hour after each daily reconnect.
4. **Large print.** A print is large when `|price × qty| ≥ 100 000 USDT`,
   computed exactly in `i128`. The threshold is per print (`aggTrade`),
   absolute, inclusive, and the parameter `large_notional_usdt`. The value
   is provisional (*Accept when*). One `aggTrade` holds one taker order's
   fills at one price, so a taker order that sweeps k levels is k prints,
   each judged on its own size: sweeps are understated.
5. **Intensity**: trades per minute and volume per minute over the
   window's nominal span (N minutes), whatever its coverage. An incomplete
   window understates intensity, and its coverage flag says so.
6. **Price response.**
   - The reference price is the last trade price at the window's start:
     the close of the latest minute with trades before the window. `None`
     until the run has a trade before the window starts.
   - `displacement = last trade price at the window's end − reference`, as
     an exact `Price`.
   - `response = displacement / delta`, in USDT per BTC of net aggression.
     `None` when the delta is 0 or there is no displacement.
   - A negative response means price moved against the net aggression.
     That is a measurement; it is never read as a direction (ADR-023).
     Baselines and effort-versus-result judgments belong to the order-flow
     candidates (#22).
7. **Exactness.** Sums use `Qty` and `u64` with checked arithmetic; the
   notional is exact in `i128`. Floats are derived on demand in this order
   and never stored, so `MarketState` stays `Eq`:
   - `imbalance = delta / volume` (`None` when the volume is 0);
   - `trades_per_minute = trade_count / N`;
   - `volume_per_minute = (volume / 1e8) / N`;
   - `large_volume_share = (large buy + large sell volume) / volume`
     (`None` when the volume is 0);
   - `price_response = displacement / delta` (decision 6).

   Each ratio divides the `f64` conversions of the exact unit counts.
   Overflow rejects the event as `StateError::Overflow` and leaves the state
   unchanged.
8. **Features** (the ids are permanent):
   - `flow.cvd.continuous@1`: parameter `gap_policy` `continue_counted`;
     input trades; warm-up `Samples(1)`, where a sample is a trade.
   - `flow.cvd.utc_day@1`: parameter `session_ms` 86 400 000; input
     `bars.time.1d@1`; warm-up `Samples(1)`, where a sample is a
     trades-stream event.
   - `flow.window.5m@1`, `flow.window.15m@1`, `flow.window.1h@1`:
     parameters `large_notional_usdt` 100 000 and `window_ms` 300 000 /
     900 000 / 3 600 000; inputs trades (the large-print classification) and
     `bars.time.1m@1` (everything else); warm-up `Samples(N)`, where a
     sample is a closed 1m bar.

   `MarketState.flow` carries the five values. Each names the feature that
   produced it. They are stepped on the same atomic path as bars and
   volatility: a rejected event changes none of them.

## Consequences

- The windows lag by up to one minute plus the time to the next
  trades-stream event, which closes the minute. Nothing intrabar is
  visible.
- The continuous CVD level is not reproducible across replay windows; only
  differences under the same `anchor` and `gaps` are. Consumers that need a
  reproducible level use the UTC-day CVD.
- Taker orders that sweep several price levels are understated as large
  prints (decision 4).
- An incomplete window understates intensity; the coverage flag is the only
  warning (decision 5).
- The price response is undefined at zero net aggression, and a value near
  zero delta can be very large; it is a raw measurement for #22 to baseline.
- Nothing here emits a direction, a side bias or a signal (ADR-023,
  ADR-024). The order-flow candidates (#22) judge the measurements.
- The engine keeps the last 61 closed minutes in a fixed ring, copied only
  when a minute closes; per trade the work is one checked add and one
  `i128` multiply.

## Alternatives considered

- **Sliding windows over individual trades in event time.** Memory grows
  with the trade rate (hundreds of thousands of prints per hour at peaks),
  values change on every print, and the windows could no longer be checked
  against the bars they must agree with. Rejected.
- **Resetting the CVD at every gap.** ADR-032 plans a sub-second reconnect
  gap every day, so the CVD would reset at a random time each day.
  Rejected.
- **A UTC-day CVD alone.** Leaves divergences across days (#22) to every
  consumer. Rejected; both anchors ship.
- **A percentile of recent trade sizes as the large-print threshold.**
  Needs a warm-up, depends on the window, costs more per trade, and can
  ship later as its own id if research justifies it (ADR-013). Rejected for
  `@1`.
- **Merging same-millisecond, same-side prints** to recover sweeps. Merges
  distinct taker orders and delays the classification by one print.
  Rejected.
- **Resetting windows after a gap.** A 60-minute window would be
  unavailable for an hour after each daily reconnect (decision 3).
  Rejected.

## Accept when

On at least 30 consecutive backfilled days (#12), the share of prints and
the share of volume at or above the decision 4 threshold are measured and
recorded here. A print share between 0.1 % and 2 % keeps the threshold.
Otherwise a `@2` of the window ids with a recalibrated threshold supersedes
it.

References: ADR-013, ADR-019, ADR-021, ADR-022, ADR-023, ADR-024, ADR-027,
ADR-028, ADR-029, ADR-031, ADR-032, ADR-033.
