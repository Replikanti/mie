# ADR-036: Volume profile

- Status: proposed
- Date: 2026-10-07

## Context

The research brief (§8, §10) and the Market State & Regime brief list the
volume profile among the Market State features and its levels among the
levels location maintains: POC, VAH, VAL, HVN and LVN. Issue #20 leaves the
definitions open:

- how prices are binned;
- which windows a profile covers and when it updates;
- what a feed gap does to it;
- how ties in the POC and in the value-area expansion break;
- what counts as a high- or low-volume node.

Earlier decisions constrain the answer:

- Raw data is the source of truth and every feature is reproducible from it
  (ADR-022). Trades carry their exact price and quantity.
- Prices and quantities are exact fixed point at 1e-8, and arithmetic is
  explicit about overflow (ADR-027).
- One domain path for live and replay (ADR-019), in the canonical event
  order (ADR-028). The core takes no dependencies and no hash-ordered
  collections (ADR-025).
- Every feature is a registered `id@version` with an append-only lock and a
  golden test per version (ADR-029).
- Bars close in event time on six UTC-aligned timeframes, and every elapsed
  interval yields a bar, empty or not (ADR-031). Incomplete bars carry
  `partial_start` or `feed_gap`.
- Live capture plans a sub-second reconnect gap once a day (ADR-032).
  ADR-033 and ADR-035 therefore keep series running through gaps and carry
  the coverage instead of resetting.
- Aggression is not direction (ADR-023); a feature earns its place by
  measured contribution (ADR-013).

## Decision

1. **Bins.** A fixed bin size of 10 USDT (`Price` 1 000 000 000 units, an
   even count, so bin midpoints are exact). The bin of a price is
   `k = units.div_euclid(bin_units)` and covers `[k·b, (k+1)·b)`. Every
   trade with a positive quantity adds its exact `Qty` to its bin; a
   zero-quantity trade adds nothing. Trades with `qty <= 0` are rejected at
   ingestion by the Binance normalizers (issue #54), so the zero rule stays
   only as defence in depth. The profile range is `[min k, max k]`
   over bins with volume; zero-volume bins inside the range are part of the
   dense histogram. Range guard `max_bins` = 10 000: a profile whose range
   spans more bins, or whose bin edges do not fit a `Price`, is
   `Unavailable { reason: OutOfRange }`. The event itself is not rejected.
   ATR-scaled bins are a separate future version.
2. **Windows.** Three profiles:
   - `profile.volume.utc_day@1`: the developing profile of the current UTC
     day (00:00 UTC, the anchor of `bars.time.1d@1` and
     `flow.cvd.utc_day@1`). Its levels are recomputed only when a 1m bar
     closes, from the trades of closed minutes: the developing minute is
     excluded (at most one minute of lag, no look-ahead). It restarts at
     every UTC day open and is `WarmingUp { observed: 0, required: 1 }`
     until the first closed minute of the day with volume.
   - `profile.volume.prior_day@1`: the completed profile of the last closed
     UTC day, fixed when its 1d bar closes. `Unavailable { reason:
     InputInvalid }` if that day had no volume.
   - `profile.volume.composite_5d@1`: the bin-wise sum of the last 5
     completed UTC days, recomputed once per day close. It warms up over 5
     closed days. Empty days count as days (ADR-031 yields a bar for every
     elapsed interval); five empty days give `Unavailable { reason:
     InputInvalid }`.

   Not in `@1`: rolling (e.g. 24 h) profiles, which need per-minute
   histograms to subtract; a developing composite; intraday sessions.
3. **Gap policy.** A trades gap never resets a profile. The value carries
   the OR of its inputs' coverage: the folded 1m bars of the day for the
   developing profile, the closed 1d bars for the completed ones. The
   reasoning is ADR-033's and ADR-035's (the planned daily reconnect gap).
4. **POC.** The bin with the most volume in the raw histogram. A tie goes
   to the bin closest to the range centre (minimise `|2k − (min + max)|`,
   exact); if still tied, the lower bin wins. Reported at the bin midpoint.
5. **Value area.** Single-bin expansion from the POC: compare the next bin
   above with the next bin below and add the larger. When they are equal,
   add both in the same step; when one side is exhausted, take the other.
   Stop once `va_volume · 100 ≥ total · value_area_pct` (70, exact in
   `i128`). VAL is the lower edge of the lowest value-area bin, VAH the
   upper (exclusive) edge of the highest, so value is `[VAL, VAH)`.
6. **HVN / LVN.** Nodes are found on the smoothed series
   `s[k] = Σ w_j · v[k+j]` with the integer triangular kernel 1-2-3-2-1
   (`node_smoothing` = `triangular_5`; bins outside the range count as 0).
   There is no division, so every comparison is exact.
   - A **peak (HVN)** is a maximal run of equal `s` with strictly lower
     neighbours on both sides. Beyond the range the neighbour is a virtual
     0 (nothing traded there), so a peak at the range edge is allowed.
   - A **valley (LVN)** is a maximal run of equal `s` with strictly higher
     neighbours inside the range on both sides. Valleys are interior only:
     the tails of a profile are never LVNs.
   - A run `[a, b]` is positioned at its lower-middle bin `a + (b − a) / 2`.
   - Prominence is topographic. HVN: `h − max(left_base, right_base)`,
     where a base is the minimum of `s` from the peak outward to the first
     strictly higher bin, or to the virtual 0 beyond the range; the global
     maximum's prominence is its height, so it is always an HVN. LVN:
     `min(left_top, right_top) − d`, where a top is the maximum of `s`
     outward to the first strictly lower bin or the range edge.
   - A node is kept when `prominence · 100 ≥ max(s) · node_prominence_pct`
     (10, exact). Each node carries its bin's midpoint and edges, the raw
     bin volume and `prominence_permille = floor(1000 · prominence /
     max(s))`. Nodes are sorted by price ascending.

   Bin size, smoothing and the 10 % threshold are provisional (*Accept
   when*).
7. **Exactness.** Bin volumes and totals are `Qty` with checked arithmetic;
   the smoothed series, prominence, thresholds and the value-area test are
   exact in `i128`. No float is computed or stored. A sum that leaves the
   `i64` range (a bin, a total, a composite) rejects the event as
   `StateError::Overflow` and leaves the state unchanged.
8. **Features** (the ids are permanent). Shared parameters: `bin_size`
   Price 10, `max_bins` 10 000, `node_prominence_pct` 10, `node_smoothing`
   `triangular_5`, `poc_rule` `max_volume_center_lower`, `session_ms`
   86 400 000, `value_area_pct` 70, `value_area_rule`
   `single_bin_ties_both`.
   - `profile.volume.utc_day@1`: inputs trades and `bars.time.1m@1`;
     warm-up `Samples(1)`, where a sample is a closed minute of the current
     day with volume.
   - `profile.volume.prior_day@1`: also `sessions` 1; inputs trades and
     `bars.time.1d@1`; warm-up `Samples(1)`, where a sample is a closed UTC
     day.
   - `profile.volume.composite_5d@1`: also `sessions` 5; inputs and
     samples as `prior_day`; warm-up `Samples(5)`.

   `MarketState.profile` carries the three values; each names the feature
   that produced it. They are stepped on the same atomic path as bars,
   volatility and order flow: a rejected event changes none of them. Each
   profile exposes its levels (POC, VAL, VAH, then every HVN and LVN) as
   the hand-off to the level registry (#23); a level's zone is its bin.

## Consequences

- The developing profile lags by up to one minute plus the time to the
  next trades-stream event, which closes the minute.
- Absolute bins are relatively coarser at low prices and finer at high
  prices; at today's BTC prices 10 USDT is about 0.015 %.
- A node is a single bin, not a zone of several bins; zone width belongs to
  the level registry (#23).
- VAH is exclusive: the value area is `[VAL, VAH)`, so a price equal to VAH
  is just outside value.
- `MarketState` stays `Clone + Eq` but now holds vectors. Profile values
  change only when a minute or a day closes, so the per-event cost is one
  sparse-map insert; consumers that clone the state per event pay a small
  extra copy.
- The completed profiles depend only on the trades of their days, so they
  are reproducible from any replay window that starts at or before the
  first included day's open (coverage per ADR-031).

## Alternatives considered

- **Volume at price from 1m bars**, spreading each bar's volume over its
  high–low range (TradingView-style). Invents a distribution, is inexact,
  and breaks ADR-022 and ADR-027 although trades carry the exact price.
  Rejected.
- **CBOT dual-row value-area expansion.** A TPO-era rule that compares two
  rows per side and overshoots by up to two bins; with fine volume bins,
  single-bin expansion is the norm. Rejected.
- **Float or ratio smoothing, or a Gaussian kernel.** Floats break exact,
  reproducible extrema; a Gaussian needs a width parameter and rounding.
  The integer triangular kernel keeps every comparison exact. Rejected.
- **TPO / market profile.** Time at price is a different measurement; it
  can ship later under its own id if research justifies it (ADR-013).
  Rejected for `@1`.
- **ATR-scaled or percentage bins.** Make bins depend on another feature's
  warm-up and state; a separate future version. Rejected for `@1`.
- **Rolling 24 h profiles.** Need per-minute histograms to subtract and
  cost far more memory. Rejected for `@1`.

## Accept when

On at least 30 consecutive backfilled days (#12):

- the median HVN and LVN counts per completed-day profile are measured and
  recorded here and lie in 1–8; otherwise a `@2` recalibrates the bin size,
  the smoothing or the threshold;
- POC, VAH and VAL of those days agree within two bins with an external
  session volume profile (e.g. TradingView Session Volume Profile, 10 USDT
  rows, 70 % value area), or each miss is explained.

References: ADR-013, ADR-019, ADR-022, ADR-023, ADR-025, ADR-027, ADR-028,
ADR-029, ADR-031, ADR-033, ADR-035.
