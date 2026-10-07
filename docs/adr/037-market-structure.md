# ADR-037: Market structure — swings, levels, sweeps and SFP

- Status: proposed
- Date: 2026-10-07

## Context

The research brief (§8, §10) and the Market State & Regime brief list
market structure among the Market State features: swing highs and lows,
sweeps and swing-failure patterns (SFP). Location maintains prior sweeps and
SFP / rejection zones among its levels. Issue #21 leaves the definitions
open:

- what a swing is, and when it becomes known;
- which levels are kept, for how long, and what counts as a touch;
- what a sweep is, and what turns a sweep into an SFP or a clean break;
- on which timeframes structure is built.

Earlier decisions constrain the answer:

- Raw data is the source of truth (ADR-022). Trades carry their exact price;
  prices are exact fixed point at 1e-8 (ADR-027).
- One domain path for live and replay (ADR-019), in the canonical event
  order (ADR-028). The core takes no dependencies and no hash-ordered
  collections (ADR-025).
- Every feature is a registered `id@version` with an append-only lock and a
  golden test per version (ADR-029).
- Bars close in event time on six UTC-aligned timeframes; every elapsed
  interval yields a bar, empty or not, and incomplete bars carry
  `partial_start` or `feed_gap` (ADR-031). An empty bar has no prices.
- Live capture plans a sub-second reconnect gap once a day (ADR-032), so
  ADR-033, ADR-035 and ADR-036 keep series running through gaps and carry
  the coverage instead of resetting.
- Bias is not a trigger (ADR-012); order flow feeds triggers, not strategy
  authority (ADR-024); aggression is not direction (ADR-023). A feature
  earns its place by measured contribution (ADR-013).

## Decision

1. **Timeframes.** Structure is built on the closed bars of 15m, 1h, 4h and
   1d (`STRUCTURE_TIMEFRAMES`), each independently. 1m and 5m are left out
   until research asks for them (ADR-013); adding one later means new ids,
   with no change to existing lock lines.
2. **Swing.** On the closed bars of one timeframe, bar `i` (with trades) is
   a swing high iff `high[i] > high[j]` for every bar with trades in
   `i-N..=i-1` and `high[i] >= high[j]` for every bar with trades in
   `i+1..=i+N` (`tie_rule` = `strict_left_weak_right`): a plateau of equal
   highs yields one swing, at its first bar. Swing lows mirror this.
   `N` = `swing_bars` = 3 on every timeframe. Empty bars count toward `N`
   and never qualify or disqualify a swing: no price is invented
   (ADR-031). An outside bar can be both a swing high and a swing low; the
   high is emitted first.
3. **Confirmation.** A swing at bar `i` has `confirmed_bar_end` = the end
   of bar `i+N`. It is emitted by the event that closes bar `i+N`, never
   earlier; that event's time is the swing's `known_at` (decision 8).
   Warm-up: `2N+1` = 7 closed bars of the timeframe.
4. **Coverage and gaps.** A swing carries the OR of the coverage of its
   `2N+1` window bars. Nothing resets on a gap, following ADR-033, ADR-035
   and ADR-036 (the daily reconnect gap of ADR-032).
5. **Registry.** Each timeframe keeps the active (unswept) structural highs
   and lows, each with `swing_time` (the swing bar's open),
   `confirmed_bar_end`, `touches` and `coverage`. Age is derived on demand
   (`now - confirmed_bar_end`), never stored.
   - **Touch**: a closed bar of the level's timeframe that opens at or after
     `confirmed_bar_end`, while the level is still active, whose extreme
     comes within `touch_tolerance_bps` = 5 of the level. For a high `L`:
     `high <= L && (L - high) · 10 000 <= |L| · 5`, exact in `i128`. Lows
     mirror this. The sweep bar never touches: the level is no longer
     active when it closes.
   - **Cap**: `max_levels` = 20 for each list — active highs, active lows
     and resolved swept levels — per timeframe. When a list is full, its
     oldest entry is evicted (by `confirmed_bar_end` for active levels, by
     sweep time for resolved ones, then by price). Pending sweeps are never
     evicted.
6. **Sweep** (`sweep_rule` = `trade_through_strict`): the first trade with
   `price > L` on an active high (`< L` on an active low). A trade at
   exactly `L` is not a sweep. At that trade the level leaves the active
   set and becomes a `PriorSweep` with outcome `Pending`. One trade can
   sweep several levels on several timeframes; the order is timeframe
   ascending, then the level price crosses first (highs by price
   ascending, lows by price descending), then `confirmed_bar_end`.
7. **SFP or clean break.** The window is `K` = `sfp_window_bars` = 2 bars
   of the level's timeframe, starting with the bar that contains the sweep
   trade.
   - **SFP** (`sfp_rule` = `close_at_or_inside`): the first window bar that
     closes at or inside the level (`close <= L` for a high, `>= L` for a
     low) resolves the sweep, timed at that bar's end.
   - **Break**: if the `K`-th bar closes without that, the sweep resolves
     as a clean break at its end.
   - Empty bars count toward `K` and never close inside.
   - **Extreme**: the highest high (lowest low) over the window bars up to
     the resolving bar. It is exact from bars, because no trade went beyond
     the level between the swing bar and the sweep.
   - An SFP adds an `SfpRejectionZone` `[L, extreme]` (`[extreme, L]` for a
     low).
   - Coverage: a sweep carries its level's coverage, and its resolution
     adds the OR of the window bars it read.

   Pending sweeps of one timeframe resolve in sweep order.
8. **Events** `StructureEvent::{Swing, Sweep, Sfp, Break}` are emitted in
   processing order: first the closed bars of the event, in `(end,
   timeframe)` order — for each bar, its resolutions, then its touches,
   then its swing confirmations — and then the trade's sweeps. Each event
   carries its timeframe, its feature key and its visibility time
   `known_at`, the time of the event that emitted it, which
   `StructureEvent::time` returns: the sweeping trade for a sweep; for a
   swing, an SFP or a break, the trade or trades gap that closed the
   confirming or resolving bar (ADR-031). The bar end stays a separate
   field — `confirmed_bar_end` for a swing, `resolved_bar_end` for an SFP
   or a break — and is never a visibility time: a closed bar becomes
   visible only at the next trades-stream event, so across a silent trades
   outage `known_at` trails the bar end by the outage's length. Labels,
   triggers and research joins key on `known_at`.
9. **Exactness.** Prices are compared exactly. Tolerance and window
   arithmetic use `i128` or checked `i64`, and `touches` is a checked
   `u32`. No float is computed or stored. An overflow rejects the event as
   `StateError::Overflow` and leaves the state unchanged.
10. **Features** (the ids are permanent). Per timeframe `<tf>`:
    - `structure.swing.<tf>@1`: the last swing high and low. Parameters
      `swing_bars` 3, `tie_rule` `strict_left_weak_right`, `timeframe_ms`;
      input `bars.time.<tf>@1`; warm-up `Samples(7)`, where a sample is a
      closed bar of the timeframe.
    - `structure.levels.<tf>@1`: the registry, touches, sweeps and SFPs.
      Parameters `max_levels` 20, `sfp_rule` `close_at_or_inside`,
      `sfp_window_bars` 2, `sweep_rule` `trade_through_strict`,
      `timeframe_ms`, `touch_tolerance_bps` 5; inputs trades,
      `bars.time.<tf>@1` and `structure.swing.<tf>@1`; warm-up
      `Samples(7)`.

    Splitting swings from levels lets the sweep, SFP and touch parameters
    be recalibrated without re-versioning the swings that divergences
    (#22) build on (ADR-029). `MarketState.structure` carries both values
    per timeframe; they are stepped on the same atomic path as bars,
    volatility, order flow and the volume profile. The engine exposes the
    events of the last accepted event. Each registry hands its levels to
    location (#23) as `StructureLevel`s.

An SFP here is a structure fact, never a signal: the `TriggerFamily::Sfp`
trigger rule, with location, bias and its own parameter version, belongs to
the bias/trigger issue (#24, ADR-012, ADR-024).

## Consequences

- The registry reflects only history since the replay start, so
  experiments record the replay window (#29).
- A trade-through inside a trades gap is unobservable: a level can survive
  a hole it was swept in. The bars of that hole show `feed_gap`.
- Swings are known `N` bars late: three hours on 1h, three days on 1d.
  Daily structure needs seven days of warm-up.
- The cap drops the oldest levels; a level evicted unswept is never swept
  later.
- `MarketState` stays `Clone + Eq` and holds vectors. Structure values
  change only when a bar of a structure timeframe closes or a level is
  swept; every other trade costs one comparison per timeframe.

## Alternatives considered

- **ZigZag / percent-reversal swings.** Their confirmation delay depends on
  the size of a future move — unbounded and threshold-dependent — so "no
  look-ahead" is harder to state and to test than with a fixed `N`-bar
  delay. Rejected.
- **Sweeps on closed 1m bars.** Loses the exact trade-through time and
  gains nothing: the bars already give the sweep extreme exactly
  (decision 7). Rejected.
- **Strict ties on both sides.** A plateau of equal highs would yield no
  swing at all, though it is a clear level. Rejected.
- **ATR-scaled touch tolerance.** Makes the levels depend on another
  feature's warm-up and state; a separate future version. Rejected for
  `@1`.
- **A single feature per timeframe.** Recalibrating `K` or the tolerance
  would re-version the swings and everything built on them. Rejected.

## Accept when

On at least 30 backfilled days (#12):

- swings per day per timeframe, the sweep-to-SFP ratio and the touch
  distribution are measured and recorded here;
- a manual chart spot check of at least 20 swings and 10 SFPs on 1h and 4h
  BTCUSDT perpetual matches, or every miss is explained.

Otherwise a `@2` recalibrates `N`, `K` or the tolerance.

References: ADR-012, ADR-013, ADR-019, ADR-022, ADR-023, ADR-024, ADR-025,
ADR-027, ADR-028, ADR-029, ADR-031, ADR-032, ADR-033, ADR-035, ADR-036.
