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

The design briefs name the five levels but give no number for any of them.
When #20 shipped `@1`, the bin size matched the row size of the external
oracle the old *Accept when* named (a circular reason), and the 5 days, the
70 %, the kernel and the 10 % threshold carried no stated reason. The audit
of #70 asked for the reasons; #79 measured them. Every `@1` value is locked
(ADR-029) and consumed by `location.levels@1` (ADR-044 D5), so a value
change means a new id or `@2` plus `location.*@2` and an ADR-044 re-run. No
value changed in #79.

### Measurements behind the numbers

Every number below cites one of these. The window W is the aggTrades
archive backfill of #12 (ADR-034), 2025-10-01 … 2026-09-30
(`docs/data-availability.md`), split into halves at 2026-04-01 00:00 UTC so
that one regime cannot carry a result. The rules (*Accept when*) were fixed
on #79 before any profile measurement was read. `w` is location's tolerance
`mie_domain::location::TOLERANCE_BPS` = 6 bps (ADR-044 D3), imported by the
tool, not copied: location consumes every level, and two levels within `w`
are one place to it (confluence and zones, ADR-044 D6–D7), so `w` is the
resolution at which profile levels are needed.

- **The tool.** `crates/mie-cli/tests/profile_measure.rs` replays W through
  one `MarketStateEngine` and recomputes every completed day with
  `shape_levels` at `ProfileShape::V1` and at the neighbouring values, one
  parameter at a time with the others at `@1`. It bins each trade at 5 USDT
  into even- and odd-aggregate-id day histograms and re-bins exactly
  (`div_euclid`) for the other bin sizes. Run on main after #93: 603 218 226
  events, 0 domain rejections, wall time 1 335.9 s (22.3 min). The tool's
  recomputation equals the engine's `prior_day` on 364 of 364 days and its
  `composite_5d` on 360 of 360.
- **The reference.** `tools/reference/volume_profile_reference.py`
  transcribes decisions 1 and 4–6 from this text (not from `profile.rs`) in
  standard-library Python and recomputes `prior_day` from the archive zips
  of D−1, D and D+1 for the 15th of each month (12 days). Its output equals
  the engine's line field by field on 12 of 12 days; those histograms and
  lines are the fixtures of `crates/mie-domain/tests/profile_reference.rs`.
- **Prices.** P_min 59 800.0 and P_max 126 208.5 USDT in the first half,
  57 758.6 and 87 385.1 in the second. A 10 USDT bin is 1.67 / 0.79 bps at
  the first half's P_min / P_max and 1.73 / 1.14 bps in the second.
- **Shape at `@1`** (first half / second half):

  | | Profiles | Span bins p50 / p99 / max | Value area bins min / p10 | Value area bps p10 / p50 / p90 | HVN p10 / p50 / p90 / max | LVN p10 / p50 / p90 / max |
  |---|---|---|---|---|---|---|
  | prior_day | 182 / 182 | 318 / 1 109 / 2 099 — 195 / 633 / 658 | 22 / 67 — 12 / 45 | 72.8 / 189.2 / 399.7 — 65.5 / 128.0 / 279.6 | 4 / 10 / 19 / 46 — 3 / 6 / 13 / 23 | 3 / 9 / 18 / 45 — 2 / 5 / 12 / 22 |
  | composite_5d | 178 / 182 | 725 / 2 267 / 2 470 — 442 / 1 543 / 1 684 | 123 / 198 — 76 / 136 | 231.7 / 456.1 / 839.4 — 193.0 / 300.1 / 599.3 | 8 / 15 / 30 / 68 — 6 / 12 / 19 / 50 | 7 / 14 / 29 / 67 — 5 / 11 / 18 / 49 |

  No profile in W is `OutOfRange` or `InputInvalid`. The composite POC lies
  more than `w` from the prior-day POC on 81.5 % / 85.2 % of days, so the
  composite is a second reference, not a copy of the prior day.
- **Node noise κ.** Each day's trades are split by aggregate-id parity into
  two half-profiles of the same price path; a node is reproduced when the
  other half has a node of the same kind within `w`. An aggregate is one
  taker order's fills at one price, so a node built by a few large orders
  lands in one half and fails; prints of at least 100 000 USDT carry 39–58 %
  of daily volume (ADR-035, decision 4), which makes this the noise that
  matters. Chance is the share of the day's bin range within `w` of any
  same-kind node of the other half, and `κ = (observed − chance) / (nodes −
  chance)`, pooled per half, kind and profile in both directions: 0 for
  random placement, 1 for perfect agreement. At `@1` (first half / second
  half): prior_day HVN 0.641 / 0.692, LVN 0.636 / 0.702; composite HVN
  0.655 / 0.712, LVN 0.682 / 0.715. It is measured on half the volume, so it
  understates full-day agreement.
- **Kernel resolution.** Two equal single-bin peaks `d` bins apart stay two
  HVNs exactly when `d ≥ r_k = (k + 3) / 2` for the triangular kernel of
  length `k` (pinned by `kernel_resolution_is_pinned` in `profile.rs`):
  none 2, t3 3, t5 4, t7 5, t9 6 bins. A profile then separates two places
  as close as location does when `r_k · bin ≤ w · P`, which holds above
  `P* = r_k · bin · 10 000 / w`.
- **Sensitivity** (first half / second half; p50 counts and κ of prior_day,
  then of composite_5d; κ as HVN, LVN):

  | Value | HVN p50 | LVN p50 | κ prior_day | κ composite | Days with `r_k · bin ≤ w · POC` |
  |---|---|---|---|---|---|
  | bin 5 | 19 / 12; 32 / 21 | 18 / 11; 31 / 20 | 0.577, 0.568 / 0.610, 0.602 | 0.621, 0.615 / 0.641, 0.652 | 100 / 100 % |
  | **bin 10** | 10 / 6; 15 / 12 | 9 / 5; 14 / 11 | 0.641, 0.636 / 0.692, 0.702 | 0.655, 0.682 / 0.712, 0.715 | 95.6 / 58.2 % |
  | bin 20 | 5 / 4; 9 / 6 | 4 / 3; 8 / 5 | 0.765, 0.767 / 0.793, 0.786 | 0.767, 0.782 / 0.789, 0.816 | 0 / 0 % |
  | bin 25 | 4 / 3; 7 / 6 | 3 / 2; 6 / 5 | 0.793, 0.763 / 0.826, 0.798 | 0.789, 0.805 / 0.826, 0.806 | 0 / 0 % |
  | bin 50 | 2 / 2; 4 / 3 | 1 / 1; 3 / 2 | 0.807, 0.794 / 0.765, 0.755 | 0.836, 0.833 / 0.790, 0.797 | 0 / 0 % |
  | kernel none | 28 / 18; 46 / 32 | 27 / 17; 45 / 31 | 0.572, 0.566 / 0.552, 0.548 | 0.605, 0.602 / 0.620, 0.642 | 100 / 100 % |
  | kernel t3 | 14 / 9; 24 / 16 | 13 / 8; 23 / 15 | 0.599, 0.591 / 0.634, 0.632 | 0.623, 0.640 / 0.645, 0.667 | 100 / 100 % |
  | **kernel t5** | 10 / 6; 15 / 12 | 9 / 5; 14 / 11 | 0.641, 0.636 / 0.692, 0.702 | 0.655, 0.682 / 0.712, 0.715 | 95.6 / 58.2 % |
  | kernel t7 | 7 / 5; 12 / 9 | 6 / 4; 11 / 8 | 0.682, 0.688 / 0.737, 0.745 | 0.694, 0.731 / 0.757, 0.767 | 66.5 / 3.8 % |
  | kernel t9 | 6 / 4; 10 / 7 | 5 / 3; 9 / 6 | 0.733, 0.729 / 0.773, 0.771 | 0.747, 0.756 / 0.777, 0.771 | 23.6 / 0.0 % |
  | prominence 5 % | 16 / 10; 29 / 20 | 15 / 9; 28 / 19 | 0.626, 0.637 / 0.683, 0.689 | 0.654, 0.655 / 0.702, 0.709 | — |
  | **prominence 10 %** | 10 / 6; 15 / 12 | 9 / 5; 14 / 11 | 0.641, 0.636 / 0.692, 0.702 | 0.655, 0.682 / 0.712, 0.715 | — |
  | prominence 15 % | 6 / 5; 9 / 8 | 5 / 4; 8 / 7 | 0.643, 0.659 / 0.690, 0.711 | 0.654, 0.670 / 0.721, 0.738 | — |
  | prominence 20 % | 5 / 4; 6 / 5 | 4 / 3; 5 / 4 | 0.647, 0.667 / 0.696, 0.723 | 0.650, 0.676 / 0.703, 0.728 | — |
  | prominence 30 % | 3 / 2; 3 / 3 | 2 / 1; 2 / 2 | 0.660, 0.667 / 0.698, 0.730 | 0.655, 0.684 / 0.701, 0.718 | — |

  Further, per bin size: the maximum prior_day span is 4 197 / 1 315 bins at
  5 USDT and the maximum composite span 4 939 / 3 368; the prior-day POC
  moves against 10 USDT by p50 2 / 2 USDT at 5 USDT, 5 / 5 at 20, 8 / 12 at
  25 and 30 / 30 at 50 (the maximum is thousands of USDT at every size: on
  days with two near-equal peaks the POC jumps between them). The prior-day
  LVN p10 is 3 / 2 at 10 % prominence, 1 / 1 at 15 % and 20 %, and 0 / 0 at
  30 %.
- **Value area** (prior_day, first half / second half):

  | Value area | Width bins min / p10 | Width bps p50 | Next-day 1m closes inside |
  |---|---|---|---|
  | 60 % | 18 / 54 — 10 / 37 | 147.3 / 102.3 | 36.2 / 36.4 % |
  | 68 % | 21 / 65 — 11 / 44 | 180.3 / 121.3 | 43.0 / 41.6 % |
  | **70 %** | 22 / 67 — 12 / 45 | 189.2 / 128.0 | 44.6 / 42.9 % |
  | 80 % | 25 / 79 — 14 / 56 | 228.7 / 154.0 | 52.4 / 50.1 % |

- **Composite length** (composite, first half / second half):

  | Days | Profiles | Span bins p50 / max | HVN p50 | LVN p50 | κ HVN, LVN | POC beyond `w` of prior-day POC |
  |---|---|---|---|---|---|---|
  | 3 | 180 / 182 | 560 / 2 267 — 338 / 1 543 | 13 / 10 | 12 / 9 | 0.649, 0.669 / 0.709, 0.701 | 72.8 / 74.2 % |
  | **5** | 178 / 182 | 725 / 2 470 — 442 / 1 684 | 15 / 12 | 14 / 11 | 0.655, 0.682 / 0.712, 0.715 | 81.5 / 85.2 % |
  | 7 | 176 / 182 | 832 / 2 470 — 506 / 1 715 | 16 / 13 | 15 / 12 | 0.657, 0.665 / 0.714, 0.715 | 82.4 / 89.6 % |
  | 14 | 169 / 182 | 1 108 / 3 080 — 739 / 1 900 | 20 / 16 | 19 / 15 | 0.662, 0.664 / 0.720, 0.721 | 87.6 / 93.4 % |

  No length produced an `OutOfRange` profile.
- **Weekly cycle C1.** Mean daily volume per UTC weekday, in BTC (first
  half / second half): Mon 194 837 / 176 287, Tue 198 521 / 166 846, Wed
  186 218 / 177 560, Thu 190 230 / 177 227, Fri 217 265 / 181 351, Sat
  90 097 / 70 114, Sun 117 264 / 90 005 (26 days each). Saturday and Sunday
  are below every weekday in both halves (C1 holds): a weekend day carries
  about half a weekday's volume.

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
   - *Why absolute bins:* the composite sums histograms bin by bin, so every
     day of it must share one grid. A percentage bin moves its grid with
     the price level, and two days at different prices would have no common
     bins to sum. An absolute bin is relatively coarser at low prices; the
     admissible range below bounds that.
   - *The admissible range:* from the 0.10 USDT tick size (Binance
     `PRICE_FILTER` of BTCUSDT, `exchangeInfo` on 2026-10-11; a finer bin
     only adds empty bins between traded prices) up to `w · P_min` = 6 bps × 57 758.6 =
     34.7 USDT (rule A2). VAL and VAH are bin edges, so the 70 % boundary
     is placed only to within one bin; a bin wider than `w` would make that
     error coarser than the tolerance the auction classifier applies to the
     same edges (ADR-044 D8). 50 USDT is 8.66 bps at P_min and fails A2.
   - *Why 10 inside the range:* the kernel resolves two places
     `r_5 · 10` = 40 USDT apart, which equals `w` at P* = 66 667 USDT;
     above that price the profile keeps apart every two equal peaks that
     location tells apart. That held on 95.6 % of first-half days and 58.2 % of
     second-half days. At 20 and 25 USDT it holds on no day of W (P* =
     133 333 and 166 667, above every price traded), so the profile merges
     peaks location would keep apart, and at 25 USDT the second-half
     prior-day LVN median falls to 2. At 5 USDT the resolution is finer
     than `w` everywhere, but κ falls by 0.06–0.10 on the prior day (0.568
     –0.610 against 0.636–0.702), node counts double (HVN p50 19 / 12
     against 10 / 6) and spans double (cost scales with 1 / bin, maximum
     composite span 4 939 bins against 2 470). 10 USDT is the coarsest of
     the measured sizes whose resolution reaches `w` within W; the choice
     between 5 and 10 is a judgment that weighs the extra resolution
     against the noise and cost, made in favour of 10.
   - *True history:* #20 chose 10 USDT to match the row size of the
     TradingView oracle the old *Accept when* named; this reason replaces
     that one.
   - *Why `max_bins` = 10 000:* a cost bound, not a filter. Each recompute
     holds the dense `Vec<Qty>` and the smoothed `Vec<i128>`, 24 B per bin,
     so at most 240 KB, and `utc_day` recomputes on every closed minute. At
     10 USDT the guard binds only for a range of at least 100 000 USDT, a
     corrupt print rather than a market. W's widest profile spans 2 470
     bins (composite, first half), so the margin is 4.0×; the widest
     14-day composite (3 080) and the widest 5 USDT composite (4 939) still
     fit. At 1 000 bins the guard would reject at least 1 % of prior days
     in the first half (p99 1 109, maximum 2 099) and every composite wider
     than 1 000 bins (p50 725, p99 2 267): it would filter real days, which
     rule A1 forbids. The bound scales with the bin: a 5 USDT `@2` needs
     the same price range in twice the bins.
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

   *Composite length.* The 5 came from the five-session week of exchange
   futures, where it has a reason (no weekend sessions). BTC trades seven
   days a week, and a weekend day carries about half a weekday's volume
   (C1, both halves). A 5-day window therefore holds three to five
   weekdays depending on the day it is computed (on Saturday it covers
   Monday–Friday, on Monday Wednesday–Sunday), so its mix of weekday and
   weekend volume, and with it the composite's POC and value area, changes
   with the weekday. A 7-day window holds every weekday exactly once. The
   measured cost of 7 is small: maximum span 2 470 / 1 715 bins against
   2 470 / 1 684, κ within 0.02, no `OutOfRange`, a warm-up two days longer (176
   against 178 first-half profiles).
   14 days would also remove the cycle, but widen the median span by half
   again (1 108 against 725 bins) and move the composite further from the
   prior day (POC beyond `w` on 87.6 / 93.4 % of days). 3 days does not remove the cycle.
   The composite therefore moves to 7 days, through a new id
   `profile.volume.composite_7d@1` (ids name their parameterization,
   ADR-029) together with `location.levels@2` and an ADR-044 M3 re-run.
   That is #96 and is pending; until it lands, this decision stays at 5
   days and `location.levels@1` keeps consuming `composite_5d@1`.
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
   - *Why 70:* the source is a convention, and the convention is the
     reason. The value area of J. Peter Steidlmayer's CBOT Market Profile
     is the range holding about one standard deviation of a normal
     distribution (68.3 %), rounded to 70 %; Dalton, Jones and Dalton,
     *Mind Over Markets* (updated edition, Wiley, 2013), describe the value
     area as the price range where about 70 % of the day's activity took
     place (cited without a page: the page was not verified against the
     print). VAH and VAL are named
     market levels, and the auction classifier reads "inside value" in that
     sense (ADR-044 D8), so a different share would name a different
     level.
   - *What breaks at another value:* 68 % (the unrounded standard
     deviation) narrows the median value area by 8.9 / 6.7 bps, about 4.5
     / 3.4 bps per edge (half the difference; the expansion is not
     symmetric), which is inside `w`: location cannot tell 68 from 70 at
     the median. 60 % and 80 % move the next-day share of 1m closes inside
     value from 44.6 / 42.9 % to 36.2 / 36.4 % and 52.4 / 50.1 %, and the
     median width by about ±20 %: a different reference, not a refinement.
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

   - *Why `triangular_5`:* the kernel trades noise against resolution. A
     narrower kernel keeps more noise: without smoothing κ drops to
     0.548–0.642 and the prior-day HVN median rises to 28 / 18 (t3: 0.591–
     0.667, 14 / 9). A wider kernel merges peaks that location tells apart:
     t5 resolves 4 bins (40 USDT, `w` above P* = 66 667 USDT, 95.6 / 58.2 %
     of days), t7 5 bins (P* = 83 333, 66.5 / 3.8 %) and t9 6 bins
     (P* = 100 000, 23.6 / 0.0 %). t5 is the widest kernel whose resolution
     stays within `w` on most of W; that κ rises further with t7 and t9 is
     bought with resolution location needs, and choosing t5 over them is a
     judgment made on that trade.
   - *Why 10 %:* prominence is a count knob, not a noise knob. κ moves by
     at most 0.031 between 10 % and any of 15, 20 and 30 % (0.636–0.715 at
     10 %, 0.655–0.730 at 30 %), while the counts fall: the prior-day LVN p10 is 3 / 2 at
     10 %, 1 / 1 at 15 % and 20 %, and 0 / 0 at 30 %, so above 10 % one day
     in ten offers the registry one LVN or none. Below 10 % κ falls in 7 of
     8 cells (5 %: 0.626–0.709) and the counts nearly double (prior-day
     HVN p50 16 / 10 against 10 / 6). 10 % is the highest measured
     threshold that keeps at least two LVNs on nine prior days in ten in
     both halves; a judgment within the measured range.
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
  prices: over W, 10 USDT was 1.73 bps at the lowest price (57 758.6 USDT)
  and 0.79 bps at the highest (126 208.5 USDT). Below about 16 667 USDT a
  10 USDT bin would exceed `w` and fail A2; below 66 667 USDT the kernel's
  40 USDT resolution is coarser than `w`, as it was at the POC of
  41.8 % of second-half days.
- The bin and the kernel are justified against ADR-044's `w`, so this
  upstream ADR now derives from a downstream one. A change of `w` re-runs
  `profile_measure.rs` and re-checks A2, A4 and the resolution share.
- Node counts per profile exceed the old 1–8 expectation (prior-day HVN
  p50 10 in the first half, composite 15 / 12); the registry sizes of
  ADR-044 M3 already reflect them.
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
- Until #96 lands, the composite's weekday mix varies with the day it is
  computed (decision 2).

## Alternatives considered

- **Volume at price from 1m bars**, spreading each bar's volume over its
  high–low range (TradingView-style). Invents a distribution, is inexact,
  and breaks ADR-022 and ADR-027 although trades carry the exact price.
  Rejected.
- **An external session volume profile as the oracle** (the old *Accept
  when*: TradingView Session Volume Profile, 10 USDT rows, POC, VAH and VAL
  within two bins "or each miss explained"). TradingView builds volume at
  price from bars, the method rejected above, so mismatches were built in
  and the criterion could neither pass nor fail. ADR-033 already rejected
  a TradingView export as a source (manual, not reproducible). Replaced by
  A5, an exact, committed, independent recomputation from the archive.
- **Median HVN and LVN counts in 1–8** (the old *Accept when*). The lower
  bound could never fail (decision 6 makes the global maximum an HVN on
  every ready profile) and the upper bound had no reason; what it stood for
  is noise, which A4 measures directly. Dropped before the data was read.
  Measured anyway: the medians lie outside 1–8 in 6 of 8 cells (prior_day
  HVN 10, LVN 9 in the first half; composite 15 / 14 and 12 / 11), so
  under the old rule `@1` would have been recalibrated toward fewer nodes,
  at the cost of the resolution decisions 1 and 6 defend.
- **Percentage bins** (and ATR-scaled bins). A grid that moves with the
  price level has no common bins across the days of a composite (decision
  1); ATR bins also make the profile depend on another feature's warm-up
  and state. Rejected for `@1`.
- **Other bin sizes** (5, 20, 25, 50 USDT), **kernels** (none, t3, t7, t9),
  **prominences** (5, 15, 20, 30 %), **value areas** (60, 68, 80 %) and
  **composite lengths** (3, 14 days): rejected for the measured reasons in
  decisions 1, 2, 5 and 6. 7 days is accepted in decision 2 and pending in
  #96.
- **CBOT dual-row value-area expansion.** A TPO-era rule that compares two
  rows per side and overshoots by up to two bins; with fine volume bins,
  single-bin expansion is the norm. Rejected.
- **Float or ratio smoothing, or a Gaussian kernel.** Floats break exact,
  reproducible extrema; a Gaussian needs a width parameter and rounding.
  The integer triangular kernel keeps every comparison exact. Rejected.
- **TPO / market profile.** Time at price is a different measurement; it
  can ship later under its own id if research justifies it (ADR-013).
  Rejected for `@1`.
- **Rolling 24 h profiles.** Need per-minute histograms to subtract and
  cost far more memory. Rejected for `@1`.

## Accept when

The rules were pre-registered on #79 before any profile measurement was
read; changing one after the run would void it. Window W and the halves as
in *Context*. Profiles judged: `prior_day` and `composite_5d`, the two
`location.levels@1` consumes (ADR-044 D5); `utc_day` applies the same
definitions to a partial day. Every rule must hold in both halves.

- **A0, the run is valid.** 603 218 226 events and 0 domain rejections, and
  on every completed day the tool's own recomputation equals the engine's
  `prior_day` and `composite_5d` field by field. A failure voids the run as
  a tool or engine bug; it is never read as a result.
- **A1, the guard never binds.** No `OutOfRange` among the prior-day and
  composite profiles of W: `max_bins` bounds the cost of a corrupt print and
  must not filter real days.
- **A2, value edges within the consumer's tolerance.**
  `bin_size · 10 000 ≤ P_min · w`: VAL and VAH are placed to within one
  bin, and that error must not be coarser than the tolerance the auction
  classifier applies to them.
- **A3, LVNs exist on a typical day.** The median LVN count per profile is
  at least 1: an id empty on most days cannot be evaluated (ADR-013) and
  gives the registry nothing. No HVN bound (decision 6 makes one HVN
  certain) and no upper bound (A4 measures what it stood for).
- **A4, nodes are structure, not noise.** κ ≥ 0.5 for HVN and LVN, for the
  prior day and the composite, in each half (8 cells). Below 0.5 the nodes
  are closer to random placement than to reproducible structure; the
  threshold is a judgment, and it is conservative because κ is measured on
  half the volume.
- **A5, independent exact recomputation.** On the 15th of each month of W
  (12 days), `tools/reference/volume_profile_reference.py` reads the zips
  of D−1, D and D+1 (each sha256 checked against the #12 import ledger),
  keeps the rows whose `transact_time` falls in D, drops repeated rows as
  `ArchiveReplay` does, and must equal the engine's `prior_day` line for D
  exactly in total, low, high, POC, POC volume, VAL, VAH, value-area volume
  and every HVN and LVN. Pass: 12 of 12. Both sides are exact (decision 7),
  so a tolerance could only hide bugs; the script shares neither author,
  language nor binning path with the engine's own randomized test. One day
  per month exercises binning at every month's price level, chosen by
  calendar, not by outcome (the ADR-047 calendar rule).
- **C1, weekly cycle** (decides the composite length, not acceptance of
  `@1`): holds if Saturday's and Sunday's mean daily volumes are each below
  the lowest Monday–Friday mean in both halves. If it holds, the composite
  moves to 7 days (decision 2) and this ADR is accepted when that
  follow-up merges; if not, 5 days stays as a stated judgment.

Met 2026-10-11 for A0–A5 at `@1`, on main after #93:

- A0 PASS: 603 218 226 events, 0 rejections, prior_day 364 of 364 and
  composite_5d 360 of 360 days equal.
- A1 PASS: 0 `OutOfRange`.
- A2 PASS: 10 · 10 000 ≤ 57 758.6 · 6 (100 000 ≤ 346 551.6). 5, 20 and
  25 USDT would also pass; 50 USDT fails.
- A3 PASS: median LVN count prior_day 9 / 5, composite 14 / 11.
- A4 PASS: all 8 cells between 0.636 and 0.715 (*Context*).
- A5 PASS: 12 of 12 days exact. The engine's binning, day attribution
  and archive deduplication were checked once, by this run; CI does not
  repeat them. What CI pins is the level computation:
  `crates/mie-domain/tests/profile_reference.rs` runs `shape_levels` on the
  reference's 12 histograms and requires the reference's levels exactly,
  plus each node's edges and raw bin volume against the histogram.
- C1 HOLDS: weekend days below every weekday in both halves.

Pending: #96 (`profile.volume.composite_7d@1`, `location.levels@2`,
ADR-044 M3 re-run). This ADR stays `proposed` until it merges. Full results
and the sha256 of the 36 archive zips:
<https://github.com/Replikanti/mie/issues/79#issuecomment-6106618884>.

References: ADR-013, ADR-019, ADR-022, ADR-023, ADR-025, ADR-027, ADR-028,
ADR-029, ADR-031, ADR-033, ADR-034, ADR-035, ADR-044, ADR-047.
