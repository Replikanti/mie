# ADR-033: ATR-percentile regime on 1h bars, and bar motion

- Status: proposed
- Date: 2026-10-06

## Context

ADR-017 fixes the canonical regime scale: the ATR percentile mapped onto
LOW 0–25, MEDIUM 26–50, HIGH 51–75 and EXTREME 76–100, with the raw
percentile retained. The research brief (§8–§9) and the Market State &
Regime brief name ATR(14) and a 200-bar percentile lookback, and they call
for a cross-check against a TradingView/reference definition. Several things
are still open:

- which bars feed ATR(14);
- the ATR seed and smoothing;
- the percentile's rank method and its value range;
- what an empty or incomplete bar does to the series.

Earlier decisions constrain the answer:

- Bars are built from trades in event time on six timeframes. An interval
  without trades is an empty bar without a price. Incomplete bars carry
  `partial_start` or `feed_gap` (ADR-031).
- Prices are exact fixed point at 1e-8. Floats are kept for derived
  statistics, and arithmetic is explicit about overflow (ADR-027).
- Every feature is a registered `id@version` with an append-only lock and a
  golden test per version (ADR-029).
- Live capture plans a sub-second reconnect gap on every stream once a day
  (ADR-032, decision 7).
- A feature earns its place by measured contribution (ADR-013). The regime
  is context, never an entry signal (ADR-012).

## Decision

1. **Timeframe: 1h** (`Timeframe::H1`, `REGIME_TIMEFRAME`). 200 hourly bars
   (about 8.3 days) cover every trading session about 8 times. The
   percentile then compares volatility with the recent week rather than with
   time-of-day seasonality, which brief §9 lists as a separate, future
   regime dimension. Cost: 214 h (about 8.9 days) of history before the
   first regime.
2. **ATR(14)**, as Pine Script v5 `ta.atr(14)`. True range:
   - `TR = H − L` when there is no previous close (Pine's first-bar rule);
   - otherwise `TR = max(H − L, |H − Cprev|, |L − Cprev|)`.

   The first ATR comes at the 14th sample and is the mean of the first 14
   TRs. After that, `ATR_t = (13·ATR_{t−1} + TR_t) / 14` (Wilder smoothing).
   The value is held as a `Price`. Intermediates are `i128`, and the seed and
   every step round half to even to 1e-8 units. The deviation from the exact
   real-valued definition stays below 7 units: TRs are exact, so
   `e_14 ≤ 0.5` and `e_t ≤ 13/14 · e_{t−1} + 0.5`.
3. **Percentile**, as Pine Script v5 `ta.percentrank(atr, 200)`:
   `p_t = 100 · #{i ∈ 1..=200 : ATR_{t−i} ≤ ATR_t} / 200`. It ranks the
   current ATR against the previous 200 values; the current value is
   excluded and ties count as at-or-below. The values are `k/2` for
   `k ∈ 0..=200`, so the range is [0, 100]. Known quirk: a constant ATR
   window ranks 100 (EXTREME). That only happens on synthetic or dead data.
   The fixed-point Wilder step makes this reachable from decay too: once
   ATR ≤ 6 units (1e-8), `round(13·ATR/14) = ATR`, so a long run of
   identical TRs (e.g. 200+ trade-less complete hours, TR = 0) freezes ATR
   and the percentile climbs to 100, where Pine's strictly decaying ATR
   would rank 0. Not reachable on live BTCUSDT.
4. **Bands**: the upper-closed bands of `RegimeLabel::from_percentile` are
   confirmed: `[0, 25]`, `(25, 50]`, `(50, 75]`, `(75, 100]`. On the
   half-steps of decision 3 they equal round-half-up followed by the
   ADR-017 whole-number bands. ADR-017 is unchanged.
5. **Warm-up** is counted in samples (decision 6). ATR is ready after 14
   samples and the regime after 214 = 14 + 200.
6. **Series continuity**, per closed bar of the series' timeframe, shared by
   ATR/regime and motion. The anchor is the previous close; every bar with
   trades sets it to its own close afterwards.

   | Closed bar | Anchor | ATR / regime | Motion |
   |---|---|---|---|
   | trades, complete | yes | sample, TR with Cprev | value |
   | trades, complete | no | sample, TR = H − L | anchor only |
   | trades, incomplete (`partial_start` / `feed_gap`) | yes | sample on the observed OHLC | value, coverage carried |
   | trades, incomplete | no | anchor only | anchor only |
   | empty, complete | yes | sample, TR = 0 | value: change 0, range 0 |
   | empty, complete | no | skipped | skipped |
   | empty, incomplete | any | **break**: back to warming up from 0, anchor and window cleared | break |

   Incomplete bars that have trades are kept because of the daily reconnect
   gap (ADR-032): resetting on any `feed_gap` bar would mean a 214-hour
   warm-up never completes live. Only an interval with no data at all breaks
   a series. The empty-complete row matches the exchange-kline convention
   numerically (a flat bar has TR 0) without inventing a bar price
   (ADR-031).
7. **Motion per timeframe** (returns, velocity and range), from closed bars
   only:
   - `change = close − previous close` (exact `Price`);
   - `range = H − L` (exact; 0 for an empty bar);
   - `simple return = change / previous close`;
   - `velocity = simple return per minute`, i.e. return /
     (`timeframe_ms` / 60 000), comparable across timeframes.

   The floats are derived on demand from the exact fields and never stored,
   so `MarketState` stays `Eq`.
8. **Features**:
   - `bars.motion.<tf>@1` for each of the six timeframes: parameter
     `timeframe_ms`, input `bars.time.<tf>@1`, warm-up `Samples(1)` (a
     closed bar with an anchor), gap policy decision 6.
   - `volatility.atr.1h@1`: parameters `length` 14, `smoothing`
     `wilder_sma_seed`, `timeframe_ms` 3 600 000; input `bars.time.1h@1`;
     warm-up `Samples(14)`.
   - `volatility.regime.1h@1`: parameters `bands` `adr017_upper_closed`,
     `lookback` 200, `rank` `previous_at_or_below`, `timeframe_ms`
     3 600 000; input `volatility.atr.1h@1`; warm-up `Samples(214)`.

   `MarketState` carries `motion`, `atr` and `regime`. The regime holds its
   label, the raw percentile and the feature key that produced it. The
   values reflect the last bar the event closed, and they are committed
   atomically with the bars: a rejected event changes none of them.
9. **Reference**: the definitions are Pine Script v5 `ta.atr(14)` and
   `ta.percentrank(·, 200)`. CI checks the implementation against a golden
   fixture: real BTCUSDT perpetual 1h klines from the Binance public data
   archive (2024-07 and 2024-08), and the values an exact-arithmetic Python
   transcription of the Pine definitions computes on them
   (`tools/reference/atr_regime_reference.py`). A TradingView chart check
   with `tools/reference/atr_regime.pine` is the manual step of *Accept
   when*.

## Consequences

- The value at time T depends on the history consumed since the series
  (re)started. The seed's influence decays as (13/14)^k, so reproducing a
  value needs the same replay window; experiments record it (#29).
- `feed_gap` bars that have trades can understate TR silently.
- A fresh live session shows no regime for about 9 days unless it is seeded
  by replay (out of scope here).
- The regime changes at most once per hour.
- A constant ATR window ranks 100, including an ATR frozen by fixed-point
  rounding on dead data (decision 3).
- Motion and ATR are on closed bars only; an intrabar (developing-bar) value
  would be a new feature.
- `Regime` names its producing feature, so a later `@2` is visible on the
  state itself.

## Alternatives considered

- **5m, 15m or 4h as the regime timeframe.** On 5m, 200 bars are 16.7 h and
  the US open would read as EXTREME every day; on 15m the window is about
  two days; on 4h it is about 33 days and reacts slowly. Rejected.
- **A regime per timeframe (six-element vector).** Adds six locked ids with
  no measured contribution (ADR-013). Rejected; another timeframe can ship
  as its own id later.
- **Mid-rank ties** (`(below + equal/2) / n`). Not what the reference
  definition does, so the cross-check would lose its oracle. Rejected.
- **An inclusive window** (ranking against the last 200 values including the
  current one). Same reason. Rejected.
- **`f64` ATR.** Would drop `Eq` from `MarketState` and make the result
  depend on operation order; the fixed-point bound of decision 2 is
  provable instead. Rejected.
- **Resetting on any `feed_gap` bar.** The warm-up would never complete
  live (decision 6). Rejected.
- **The TA-Lib seed** (first TR skipped, since it has no previous close).
  Differs from the Pine reference for the first ~100 bars. Rejected.
- **A TradingView export as the CI source.** Manual and not reproducible by
  anyone without an account. Rejected; it remains the manual check.

## Accept when

The CI cross-check passes, and a manual TradingView spot check matches:
`tools/reference/atr_regime.pine` on the BINANCE:BTCUSDT.P 1h chart agrees
with the fixture to 1e-6 relative on ATR and exactly on the percentile, at
≥ 20 fixture rows past row 400. TradingView's series starts years earlier;
its seed difference there is (13/14)^400 ≈ 1e-13.

References: ADR-012, ADR-013, ADR-017, ADR-019, ADR-027, ADR-029, ADR-031,
ADR-032. This PR also accepts ADR-029: #15 registered bars and #16 a
volatility feature without changing encoding v1.
