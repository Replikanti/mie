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

The four numeric parameters — `swing_bars` 3, `touch_tolerance_bps` 5,
`max_levels` 20 and `sfp_window_bars` 2 — were set when the engine landed
(#57), without any measurement. The audit of #80 asked where each comes
from. The reasons below were written afterwards; each says whether it is a
derivation, a measurement or a judgment, and no later measurement is
presented as the origin of a value.

### Measurements behind the numbers

Every measured number below cites one of these.

- **Tool.** `crates/mie-cli/tests/structure_measure.rs` (the ignored test
  `measure_structure`) replays the aggTrades of the #12 backfill through
  `MarketStateEngine`. Next to it, it runs an independent model of
  decisions 2–7, written from this text, on the engine's closed bars, with
  `N`, `K`, the tolerance and the cap as runtime parameters. At the
  engine's values the model must reproduce the engine (R0, Accept when);
  at other values it gives the sensitivity tables. The CI test
  `model_reproduces_the_engine_on_a_synthetic_tape` runs the same
  comparison on a seeded 40-day tape and asserts that every compared kind
  occurs, so R0 cannot pass vacuously.
- **Run.** 2025-10-01 … 2026-09-30, 365 UTC days, aggTrades only; halves
  split at 2026-04-01 12:00 UTC. 603 218 226 events, 0 domain rejections,
  wall time 1 166.6 s (19.4 min). Full output:
  <https://github.com/Replikanti/mie/issues/80#issuecomment-6103670815>.
- **Null.** Under a symmetric random walk, the walk stays on one side of
  its start for `N` steps with probability `u_N` = C(2N, N) / 4^N (Sparre
  Andersen); a swing needs that on both sides, so a bar is a swing high
  with probability `u_N²`: 0.250, 0.141, 0.0977, 0.0748, 0.0606 for
  `N` = 1 … 5. At `N` = 3 that is 9.4 swing highs a day on 15m, 2.3 on 1h,
  0.59 on 4h and 0.098 on 1d: about 36 per side a year on 1d, about 3 in
  30 days.
- **S1, swings** at `N` = 3, whole window:

  | | 15m | 1h | 4h | 1d |
  |---|---|---|---|---|
  | Swing highs per closed bar (null 9.77 %) | 9.85 % | 10.18 % | 10.00 % | 10.44 % |
  | Swing lows per closed bar | 9.98 % | 10.21 % | 10.05 % | 10.44 % |
  | Swing highs per day | 9.455 | 2.444 | 0.600 | 0.104 |
  | `known_at` − `confirmed_bar_end`, p50 / max | 114 / 3 893 ms | 106 / 3 668 ms | 96 / 3 316 ms | 72 / 2 730 ms |

  Across `N` = 1 … 5 the per-bar rate tracks the null on every timeframe;
  on 15m, swing highs per bar are 23.29, 13.94, 9.85, 7.70, 6.33 %
  against 25.00, 14.06, 9.77, 7.48, 6.06 %. At `N` = 1 there are 22.359
  swing highs a day on 15m; at `N` = 5 there are 0.058 a day on 1d (about
  21 in the year).
- **S2, level life** from `known_at` to the sweep, in bars of the
  timeframe, uncapped, whole window, p25 / p50 / p75: at `N` = 3, 15m
  2.9 / 11.1 / 47.8 (n 6 823), 1h 2.8 / 10.1 / 43.3 (n 1 714), 4h
  2.3 / 8.1 / 30.0 (n 402), 1d 1.4 / 6.6 / 24.7 (n 65); unswept at the
  window end 1.80, 4.03, 8.43 and 14.47 %. On 15m the p50 is 4.6 bars at
  `N` = 1 and 17.0 at `N` = 5.
- **S3, sweeps and outcomes** at the engine's parameters, whole window:

  | | 15m | 1h | 4h | 1d |
  |---|---|---|---|---|
  | Sweeps per day, highs / lows | 8.973 / 9.118 | 2.277 / 2.296 | 0.537 / 0.559 | 0.088 / 0.090 |
  | SFP share at `K` = 1 / 2 / 3 / 4 / 6 | 49.43 / 64.62 / 71.12 / 74.54 / 78.89 % | 50.15 / 63.69 / 68.12 / 71.36 / 76.63 % | 52.75 / 64.25 / 70.75 / 72.00 / 75.75 % | 53.23 / 70.97 / 80.65 / 80.65 / 83.87 % |
  | First close at or inside the level, window bar 1 / 2 / 3 | 3 264 / 1 003 / 429 | 837 / 226 / 74 | 211 / 46 / 26 | 33 / 11 / 6 |
  | Sweep position in its bar (elapsed fraction), p50: break at `K` = 1 but SFP at `K` = 2 | 0.514 (n 1 003) | 0.510 (n 226) | 0.587 (n 46) | 0.606 (n 11) |
  | The same, all sweeps | 0.433 (n 6 603) | 0.456 (n 1 669) | 0.542 (n 400) | 0.643 (n 65) |

- **S4, touches** at the engine's `N` and cap: share of levels touched at
  least once, whole window, by tolerance in bps:

  | Tolerance | 1 | 2 | 3 | 5 | 8 | 10 | 20 | Touches per level at 5, p90 / max | Levels |
  |---|---|---|---|---|---|---|---|---|---|
  | 15m | 6.62 % | 13.28 % | 19.72 % | 30.50 % | 44.13 % | 51.32 % | 71.73 % | 2 / 17 | 6 948 |
  | 1h | 2.07 % | 4.87 % | 7.89 % | 14.95 % | 24.52 % | 30.18 % | 50.17 % | 1 / 4 | 1 786 |
  | 4h | 1.14 % | 2.73 % | 3.64 % | 5.69 % | 11.16 % | 13.44 % | 23.92 % | 0 / 2 | 439 |
  | 1d | 1.32 % | 3.95 % | 3.95 % | 5.26 % | 6.58 % | 6.58 % | 7.89 % | 0 / 1 | 76 |

  On 1d no level of the first half was touched at any tolerance up to
  20 bps.
- **S5, the cap**, whole window:

  | | 15m | 1h | 4h | 1d |
  |---|---|---|---|---|
  | Uncapped active highs, p50 / p99 / max | 80 / 137 / 148 | 42 / 71 / 77 | 23 / 40 / 41 | 6 / 12 / 13 |
  | Uncapped active lows, p50 / p99 / max | 20 / 62 / 69 | 11 / 33 / 36 | 5 / 16 / 18 | 3 / 7 / 8 |
  | Evictions in the year at cap 20, active / resolved | 321 / 6 583 | 94 / 1 649 | 21 / 380 | 0 / 45 |
  | Sweeps lost at cap 10 / 20 / 40 | 507 / 220 / 89 | 114 / 45 / 4 | 22 / 2 / 0 | 0 / 0 / 0 |
  | Evicted active levels, distance from the last close, p10 / p50 bps | 556.7 / 1 051.7 | 929.8 / 2 520.6 | 4 011.4 / 5 270.5 | none evicted |
  | Resolved-list history depth at cap 20, p10 / p50 days | 0.80 / 1.10 | 3.19 / 4.28 | 12.87 / 18.12 | 35.43 / 97.31 |

  A lost sweep is one the uncapped registry records of a level that the
  capped one had already evicted; the uncapped registry records
  6 823 sweeps on 15m and 1 714 on 1h.

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

   Why `N` = 3 (a judgment; its consequences are derived and measured). `N`
   trades the confirmation delay, `N` bars (decision 3), against how many
   swings there are and how long a level lives. At `N` = 1 there are
   22.359 swing highs a day on 15m and a level lives p50 4.6 bars (S1,
   S2); at `N` = 5, 1d confirms five days late and yields 0.058 swing
   highs a day (about 21 a year), too few for any 1d ratio. 3 lies
   between; no measurement picks it over 2 or 4. One `N` on every
   timeframe: a swing is defined in bars of its own timeframe, and the
   null rate per bar does not depend on the timeframe; S1 measures
   9.85–10.44 % of bars as swing highs on all four, against the null's
   9.77 %. A per-timeframe `N` adds three parameters that no evidence here
   separates; whether structure contributes per timeframe is #31's
   question (ADR-013). History: set at #57 without a measurement.
3. **Confirmation.** A swing at bar `i` has `confirmed_bar_end` = the end
   of bar `i+N`. It is emitted by the event that closes bar `i+N`, never
   earlier; that event's time is the swing's `known_at` (decision 8).
   Warm-up: `2N+1` = 7 closed bars of the timeframe (a derivation from the
   window). On 1d that is one full weekly cycle, a corroboration and not the
   reason.
4. **Coverage and gaps.** A swing carries the OR of the coverage of its
   `2N+1` window bars. Nothing resets on a gap, following ADR-033, ADR-035
   and ADR-036 (the daily reconnect gap of ADR-032).
5. **Registry.** Each timeframe keeps the active (unswept) structural highs
   and lows, each with `swing_time` (the swing bar's open),
   `confirmed_bar_end`, the swing's `known_at`, `touches` and `coverage`.
   Age is derived on demand (`now - known_at`), never stored.
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

   Why 5 bps (post-hoc consistency; S4 measures what other values give).
   A touch says how close the price path came to the level within the bar
   without crossing it. That closeness is treated as a property of the
   level, not of the bar size (a judgment), so one value serves every
   timeframe, and it is in bps rather than USDT so it does not depend on
   the price level. The value predates every measurement. It is consistent
   with ADR-044 M1, measured later: the whole-year p75 of the 1m
   close-to-close move is 5.18 bps (halves 5.93 / 4.51), so a touch reads
   "the extreme stopped within one ordinary minute's move of sweeping the
   level". Measured consequence (S4): the share of levels touched at least
   once falls with the timeframe, 30.50 % on 15m, 14.95 % on 1h, 5.69 % on
   4h and 5.26 % on 1d (4 of 76 levels, none in the first half). On 15m,
   1 bps gives 6.62 % and 20 bps 71.73 %. R1 holds on every timeframe. A
   static per-timeframe tolerance scaled by √(timeframe) is the `@2`
   candidate if #31 finds that touches matter on 1d.

   Why a cap. In a trend, unswept levels on one side pile up without bound
   (S5: uncapped active highs on 15m p50 80, max 148, against lows p50 20),
   and the state that holds them is cloned, hashed (ADR-041) and handed to
   location, which rebuilds its registry every closed minute in
   `O(n log n)` (ADR-044). Evicting the oldest also evicts the farthest:
   evicted active levels lay p50 1 051.7 bps from the last close on 15m,
   2 520.6 on 1h and 5 270.5 on 4h (S5).

   Why 20 for the active lists (a measurement, R2). Set at #57 without a
   measurement. R2 asks that the sweeps lost to eviction move the SFP share
   by less than one standard error. It holds on 4h (lost share 0.0050
   against 0.0240) and 1d (no active level evicted) and **fails on 15m (0.0322
   against 0.0059) and 1h (0.0263 against 0.0118)**. Cap 40 still loses 89
   of 6 823 sweeps on 15m (1.30 %), above even the largest standard error
   possible at that sample (`n` = 6 823 − 89), sqrt(0.25 / 6 734) =
   0.61 %. 20 is therefore not justified on 15m and 1h (Accept when).

   Why the resolved list shares the cap (a judgment). The resolved list is
   location's history of prior sweeps and SFP zones, and it sits at the
   cap most of the time (ADR-044 M3: prior sweeps p50 80 = 4 timeframes ×
   20). At 20 it reaches back p50 1.10 days on 15m, 4.28 on 1h, 18.12
   on 4h and 97.31 on 1d (S5). No measurement says how far back a prior
   sweep stays relevant; the depth is recorded, and the value is a
   judgment.
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

   Why `K` = 2 (a derivation, checked by R3). The sweep trade can fall
   anywhere in its bar, including its last instant. With `K` = 1 such a
   sweep is judged by a close an instant later, so `K` = 1 sorts sweeps by
   their position in the bar. S3 shows it: the sweeps that `K` = 1 calls a
   break and `K` = 2 an SFP fell later in their bar than sweeps overall
   (elapsed fraction p50 0.514 against 0.433 on 15m, 0.510 against 0.456 on
   1h, 0.587 against 0.542 on 4h; the 11 such sweeps on 1d do not show it,
   0.606 against 0.643). `K` = 2 is the smallest window that gives every
   sweep at least one complete bar after the sweep trade. A larger `K`
   delays every break by one bar of the timeframe (on 1d, a day), and a
   later reclaim is a different fact, whose horizon belongs to the #24
   trigger rules (ADR-012, ADR-024). R3 checks that the window edge does
   not cut through the rejection mass: on every timeframe fewer first
   reclaims fall in window bar 3 than in bar 2 (S3). History: set at #57
   without a measurement.
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
    location (#23) as `StructureLevel`s, each timed by `known_at` with one
    meaning for every kind: the visibility time (decision 8) of the fact
    that gave the level its kind — the swing for a structural level, the
    sweep for a prior sweep, the SFP for a rejection zone. Its age is
    `now - known_at`; no bar end is handed over, so levels the engine
    learned of together have the same age.

An SFP here is a structure fact, never a signal: the `TriggerFamily::Sfp`
trigger rule, with location, bias and its own parameter version, belongs to
the bias/trigger issue (#24, ADR-012, ADR-024).

### Where each number comes from

| Number | Reason | Kind | Gate | Result |
|---|---|---|---|---|
| `swing_bars` 3, all timeframes | Decision 2: delay against swing count and level life; the null rate per bar does not depend on the timeframe; a per-timeframe `N` adds three parameters no evidence separates. Delay `N` bars: 45 min, 3 h, 12 h, 3 d. Warm-up `2N+1` = 7. Set at #57 without a measurement | judgment + derivation | R0, R1 | R0 valid, R1 PASS |
| `touch_tolerance_bps` 5 | Decision 5: a property of the level, in bps; consistent with ADR-044 M1's whole-year 1m p75 of 5.18 bps, which came later; S4 gives 1–20 bps | post-hoc consistency + measurement | R0, R1 | R0 valid, R1 PASS |
| `max_levels` 20 | Decision 5: a cap bounds the state; 20 on the active lists by R2 and S5; the resolved list's depth is recorded | measurement (R2) + judgment (resolved) | R0, R2 | R2 **FAIL** on 15m and 1h, PASS on 4h and 1d |
| `sfp_window_bars` 2 | Decision 7: the smallest window that gives every sweep a complete bar after the sweep trade; S3 | derivation + R3 | R0, R3 | R3 PASS |
| Accept when: the whole year, R0–R3 | Accept when: the sample follows from the null; no escape clause | — | — | — |
| Chart spot check | Dropped; R0 replaces it (Alternatives) | — | R0 | R0 valid |

## Consequences

- The registry reflects only history since the replay start, so
  experiments record the replay window (#29).
- A trade-through inside a trades gap is unobservable: a level can survive
  a hole it was swept in. The bars of that hole show `feed_gap`.
- Swings are known `N` bars late: 45 min on 15m, 3 h on 1h, 12 h on 4h and
  3 days on 1d. The confirming event adds p50 72–114 ms to that and at most
  3 893 ms in the year (S1). Daily structure needs seven days of warm-up.
- The cap drops the oldest levels; a level evicted unswept is never swept
  later. In the year, cap 20 evicted 321 active levels on 15m, 94 on 1h,
  21 on 4h and none on 1d, and lost 220, 45, 2 and 0 sweeps that an
  uncapped registry records (S5). By R2's bound, the lost sweeps move the
  15m and 1h SFP shares by at most 3.2 and 2.6 percentage points. The
  resolved list reaches back p50 1.10 days on 15m, 4.28 on 1h, 18.12 on
  4h and 97.31 on 1d.
- Bars come from aggTrades, which omit individual trades on some days
  (ADR-047: up to 478 a day on affected days, none in the 22 sampled days
  from 2026-06-10 on). A missing trade changes a bar's extreme or close, and with it a swing, a
  touch, a sweep or an SFP, only when it is that extreme or the bar's last
  trade. ADR-031 found the highs and lows of 15m and longer bars matching
  the exchange klines in both its windows. The engine and the measurement
  model read the same events, so R0 is unaffected.
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
- **`N` per timeframe.** Three more parameters. The per-bar swing rate is
  the same on every timeframe (S1), and nothing here measures which `N`
  serves which timeframe; contribution is #31's question (ADR-013).
  Rejected for `@1`.
- **A static per-timeframe tolerance scaled by √(timeframe).** It
  addresses the fall of the touched share from 30.50 % on 15m to 5.26 %
  on 1d (S4) without depending on another feature, unlike the ATR-scaled
  one. Not adopted: nothing yet says that touches matter on 1d. It is the
  `@2` candidate if #31 finds that they do.
- **`K` = 1.** Judges a sweep late in its bar by a close an instant later,
  so the outcome depends on the sweep's position in the bar (decision 7,
  S3); its SFP share is 49.43–53.23 % on the four timeframes. Rejected.
- **`K` = 3.** Delays every break by one more bar of the timeframe (on 1d,
  a day). First reclaims in window bar 3 are fewer than in bar 2 on every
  timeframe (R3), and a later reclaim is a different fact that belongs to
  the trigger rules (#24). Rejected.
- **No cap, or a larger cap.** Without a cap the state grows with the
  length of a trend (S5: 148 active highs on 15m in this year), and it is
  cloned, hashed and rebuilt into location every closed minute. A larger
  cap is what R2's failure on 15m and 1h points to, but cap 40 still
  fails 15m (89 lost sweeps, 1.30 %). The new value is the `@2`'s
  decision (Accept when).
- **Separate caps for active and resolved lists.** One more parameter per
  list, while no measured criterion exists for the resolved depth (decision
  5). Rejected for `@1`.
- **A manual chart spot check** (the earlier acceptance: at least 20
  swings and 10 SFPs on 1h and 4h matching, "or every miss explained").
  It had no oracle beyond this ADR's own definition applied by eye, no
  reason for its counts, and an escape clause. R0 replaces it: an
  independent model of this text, compared with the engine on every event
  of 365 days. Bars against the exchange are checked by ADR-031 and
  ADR-045. Dropped.

## Accept when

All four rules hold on the whole archive backfill of #12, 2025-10-01 …
2026-09-30 (365 UTC days, aggTrades only, 603 218 226 events,
`docs/data-availability.md`), measured by `structure_measure.rs`. Each
rule is judged per timeframe over the whole window; the halves, split at
2026-04-01 12:00 UTC, are recorded only. The rules were pre-registered on
#80 before any full-window run; changing a rule after a run voids that
run. There is no clause that lets a failure be explained away.

Why the whole year (a derivation from the null): at `N` = 3 the null gives
1d about 36 swing highs a year but about 3 in 30 days, so 30 days cannot
carry any 1d ratio. Even a year gives 1d only a few dozen swings and
sweeps per side; with `n` sweeps, the chance that one outcome is absent by
luck alone is about 2 · 0.5ⁿ, not negligible for a 1d half-year. Hence
whole-window rules.

- **R0, the run is valid.**
  - (a) Events = 603 218 226 and domain rejections = 0.
  - (b) At the engine's values (`SWING_BARS`, `SFP_WINDOW_BARS`,
    `TOUCH_TOLERANCE_BPS`, `MAX_LEVELS`), the model reproduces every
    engine `StructureEvent`. Swing: timeframe, side, price, `swing_time`,
    `confirmed_bar_end`. Sweep: timeframe and level identity, with the
    engine's sweep time inside the model's sweep bar. SFP and break:
    outcome, `resolved_bar_end`, `extreme`, `window_bars`.
  - (c) The model reproduces the registry's highs, lows and resolved
    sweeps (identity and `touches`) after every closed bar of each
    structure timeframe; levels swept by the closing event itself count as
    still present, because decision 8 applies that trade's sweeps after
    the close.

  Any mismatch voids the run; it is never read as a result.
- **R1, every structure fact occurs.** On every structure timeframe, each
  of these occurs at least once: a swing high, a swing low, a sweep of a
  high, a sweep of a low, an SFP, a break, and a level with `touches` ≥ 1.
  Why: a fact that never occurs in a year of BTC cannot be researched
  (#24, #26, #31), so its absence is a definition bug.
- **R2, the cap loses less than the noise.** Per timeframe, an uncapped
  registry runs next to the cap-20 one. `lost` = the sweeps the uncapped
  registry records of levels the capped one had already evicted; `n` and
  `p` = the cap-20 sweeps and their SFP share. PASS when
  `lost / (n + lost) ≤ sqrt(p (1 − p) / n)`. Why: the lost share bounds how
  far the cap can move the SFP share, `|p_uncapped − p_cap| ≤ lost share`
  whatever the lost sweeps' outcomes, and keeping that bound within one
  standard error keeps the cap's bias below the sampling noise of the
  statistic it feeds.
- **R3, the SFP window closes on a thinning tail.** On every timeframe,
  fewer sweeps have their first close at or inside the level in window
  bar 3 than in window bar 2. Why: if bar 3 had as many first reclaims as
  bar 2, the window edge would cut through the rejection mass and the
  SFP / break split would depend on an arbitrary boundary. Under the null
  the first-return probability falls with `k`, so a FAIL means structure
  that `K` = 2 misses.

On a FAIL of R1–R3, a `@2` of the affected ids recalibrates the failing
parameter (ADR-029 lock). A new `structure.levels.<tf>` or
`structure.swing.<tf>` version also re-versions `location.*` and re-runs
its measurement (ADR-044). An R0 mismatch caused by an engine defect takes
the same `@2` path.

Recorded, not criteria (per timeframe, whole window and halves): S1 swings
per day and per bar at `N` = 1 … 5 against `u_N²`, and the confirmation
delay; S2 level life at `N` = 1 … 5; S3 sweeps per day, the SFP share at
`K` = 1, 2, 3, 4, 6, the first-reclaim bar, and the sweep position in its
bar; S4 touches at 1–20 bps; S5 the uncapped list sizes, evictions, `lost`
at caps 10, 20 and 40, the evicted levels' distance, and the resolved
history depth; run facts (events, rejections, wall time).

Measured 2026-10-11, not met. R0 valid: 0 engine/model mismatches over
9 249 swings, 4 334 sweeps of highs, 4 403 sweeps of lows, 5 631 SFPs,
3 106 breaks and 46 327 registry comparisons; 0 domain rejections;
603 218 226 events as expected. R1 PASS on every timeframe (the fewest on
1d: 4 touches, 21 breaks, 32 sweeps of highs). R3 PASS on every
timeframe (window bar 2 → 3: 15m 1 003 → 429, 1h 226 → 74, 4h 46 → 26,
1d 11 → 6). **R2 FAIL on 15m and 1h**:

| R2 | `n` | `p` | `lost` | Lost share | sqrt(p (1 − p) / n) | Verdict |
|---|---|---|---|---|---|---|
| 15m | 6 603 | 0.6462 | 220 | 0.0322 | 0.0059 | FAIL |
| 1h | 1 669 | 0.6369 | 45 | 0.0263 | 0.0118 | FAIL |
| 4h | 400 | 0.6425 | 2 | 0.0050 | 0.0240 | PASS |
| 1d | 65 | 0.6769 | 0 | 0.0000 | 0.0580 | PASS |

Consequence: `max_levels` 20 is not justified on 15m and 1h, and this ADR
stays proposed. The `@2` of the affected ids — the `structure.levels.<tf>`
ids whose cap changes, at least 15m and 1h, and with them `location.*`
(ADR-044) — is a separate issue with its own plan. Until it lands, the
locked `@1` ids keep cap 20, with the bias stated in Consequences. Results:
<https://github.com/Replikanti/mie/issues/80#issuecomment-6103670815>.

References: ADR-012, ADR-013, ADR-019, ADR-022, ADR-023, ADR-024, ADR-025,
ADR-027, ADR-028, ADR-029, ADR-031, ADR-032, ADR-033, ADR-035, ADR-036,
ADR-041, ADR-044, ADR-045, ADR-047.
