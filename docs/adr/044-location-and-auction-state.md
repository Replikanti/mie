# ADR-044: Location and auction state — UTC-day VWAP, level registry with score components, auction classifier against prior-day value

- Status: proposed
- Date: 2026-10-09

## Context

#23 asks where price is relative to meaningful levels, continuously and
whether or not a trade is authorized (brief §10–§11, ADR-012). It names a
level registry over every `LevelKind`, a level score (distance, touches,
age, source, confluence), an auction-state classification per
`AuctionState` with a measurable definition of each state (acceptance in
particular), and monitored-location events (entered or left a zone) for
egress and Conditional Behavior. Its acceptance criteria: tests per auction
state on synthetic paths, including failed breakout against acceptance near
the threshold, and location state on Market State, versioned and hashed.

What exists:

- Hand-off types from the upstream families: `ProfileLevel` (POC, VAL,
  VAH, HVN, LVN of a volume profile, ADR-036), `StructureLevel` (structural
  highs and lows, prior sweeps, SFP rejection zones per timeframe, ADR-037)
  and `SideClusters` (the five largest book levels per side within 5 bps,
  ADR-043). ADR-043 left the cluster threshold to this issue.
- Event-time bars on six timeframes (ADR-031); the developing, prior-day
  and 5-day composite volume profiles over UTC days (ADR-036).
- The Market State hash (ADR-041), the feature registry with its lock and
  goldens (ADR-029), exact fixed point (ADR-027), one domain path for live
  and replay (ADR-019), measured contribution before any weight (ADR-013).

The inputs of ADR-036 and ADR-037 (bin, 70 % value area, structure `N`,
`K` and tolerance) are under audit in #70. Location builds on their `@1`
versions; an upstream `@2` means a `location.*@2` and a re-run of the
measurement tool. Nothing changes silently.

### Measurements behind the numbers

Every number below cites one of these. The window is the archive backfill
of #12, 2025-10-01 … 2026-09-30 (`docs/data-availability.md`).

- **M0, plan-time probe** (an approximation: 1m kline closes, and a
  prior-day value area built by spreading each bar's volume uniformly over
  its 10-USDT bins, then expanding 70 % from the POC).
  - **M0a, the 1m close-to-close move** `|Δclose|` over 525 599 minutes, in
    bps: p50 2.56, p75 5.18, p90 9.08, p95 12.58, p99 23.0. 5.4 % of closes
    fall in the `w` = 5 edge bands. Value-area width: p10 67 bps, p50 150,
    p90 347; the narrowest 19 bps. 113 of 364 days open more than `w`
    outside the prior day's value.
  - **M0b, breakouts at `w` = 5:** 1 689 in 364 days, 87 % of them back
    inside value the same UTC day. Failure hazard per counted close: 5.6–7.4
    % over the first five closes, 0.88 % at closes 45–60, 0.49 % at 60–90,
    about 0.15 % from 180 on. At `w` = 3: 2 210 breakouts, first-close
    hazard 11.3 % (6.1 % at `w` = 5).

    | Closes beyond the edge (N) | 30 | 60 | 120 | 180 |
    |---|---|---|---|---|
    | Same-day failures that have already failed | 66 % | 79 % | 87 % | 91 % |
    | Held N closes, still returned the same day | 71 % | 61 % | 52 % | 45 % |

    Per half of the window, the share caught by 60 closes is 81 % and
    77 %; at `w` = 8 it drops to 72 % (68 % on the second half).
- **M1–M4, exact** — the measurement tool
  `crates/mie-cli/tests/location_measure.rs` replays the window's aggTrades
  through the engine and runs the exact classifier; the halves split at
  2026-04-01 12:00 UTC. Full-year run on the capture host, with `w` = 5 (the
  value before the D3 fixing rule): 603 218 226 events, 0 domain
  rejections, wall time 1 155.6 s (19.3 min). The #11 replay of the same
  window took 20.5 min over the default archive streams; this run reads
  aggTrades only and adds the location stage, so the two bound each other
  rather than compare like for like.
  - **M1, the 1m close-to-close move** `|Δclose|` from the engine's bars, in
    bps (p50 / p75 / p90 / p95 / p99):

    | | n | p50 | p75 | p90 | p95 | p99 |
    |---|---|---|---|---|---|---|
    | First half | 262 799 | 2.93 | 5.93 | 10.35 | 14.28 | 25.86 |
    | Second half | 262 799 | 2.25 | 4.51 | 7.74 | 10.67 | 19.36 |
    | Whole window | 525 598 | 2.56 | 5.18 | 9.08 | 12.58 | 23.04 |

    At `w` = 5, 4.59 % (first half) and 5.83 % (second half) of closes fall
    in the edge bands. Prior-day value-area width: p10 67.9 bps, p50 153.8,
    p90 352.1, narrowest 19.1 (364 days). 116 of 364 days open beyond an
    edge band.
  - **M2, breakouts** (probes out of prior-day value, acceptance off), first
    half / second half:

    | `w` | Breakouts | Failed the same day | Hazard, closes 1–5 | Hazard 45–59 | Caught by 30 | Caught by 60 | Caught by 120 | Held 60, then returned |
    |---|---|---|---|---|---|---|---|---|
    | 3 | 884 / 801 | 83.4 / 81.0 % | 11.8, 8.8, 7.5, 7.8, 5.6 / 9.9, 7.9, 7.7, 6.4, 4.3 % | 0.78 / 0.72 % | 75.0 / 72.0 % | 85.1 / 82.4 % | 91.2 / 88.4 % | 55.3 / 54.8 % |
    | 5 | 700 / 618 | 80.6 / 77.5 % | 7.9, 6.3, 4.2, 6.5, 4.6 / 5.0, 5.3, 6.1, 4.5, 3.1 % | 0.86 / 0.53 % | 68.4 / 64.3 % | 81.2 / 77.5 % | 88.3 / 84.6 % | 54.6 / 52.9 % |
    | 8 | 541 / 484 | 77.3 / 74.6 % | 4.4, 3.5, 5.1, 5.6, 2.3 / 2.9, 3.2, 4.4, 2.3, 2.1 % | 0.80 / 0.48 % | 61.2 / 56.2 % | 75.8 / 71.2 % | 84.9 / 80.1 % | 53.7 / 51.7 % |

    At `w` = 5 the hazard falls on to 0.45 % at closes 60–89 and 0.07 % at
    180–359 (first half). The exact counts are lower than M0b's (1 318 at
    `w` = 5 against 1 689): the exact value area and closes differ from the
    probe's approximation.
  - **M3, the registry** at `w` = 5: p50 / p99 levels per closed minute — POC,
    VAL, VAH 2/2 each, HVN 21/71, LVN 19/69, structural highs 48/69, lows
    29/61, prior sweeps 80/86, SFP zones 51/63, VWAP 1/1, clusters 0/0
    (archive), all 254/342. Zone events per day: 1 380.0 arrived, 33.1
    created, 1 400.1 departed, 12.9 retired.
  - **M4, auction states** at `w` = 5, `N_acc` = 60 (first half / second
    half, share of closes): inside value 38.4 / 36.2 %, at value edge 1.7 /
    2.2 %, outside value 15.6 / 16.0 %, breakout 6.9 / 6.7 %, failed breakout
    4.6 / 4.4 %, failed reclaim 2.9 / 3.0 %, acceptance 30.0 / 31.5 %. Every
    state occurs in each half; 24.0 / 23.6 transitions a day; the engine and
    the tool's classifier agreed on every close.

  The fixing rules decide whether a constant changes: `w` = the first-half
  M1 p75 rounded to whole bps (D3); `N_acc` per D9; and every
  `AuctionState` must occur in each half (M4) — a state that never occurs in
  six months of BTC cannot be researched, so its absence is a definition
  bug, fixed before merge. Constants change only before merge, because the
  lock lines are published at merge. The first-half p75 is 5.93 bps, so
  **`w` changed from 5 to 6** under the D3 rule. **Pending:** the re-run
  with `w` = 6 (M2 at `w` = 6 next to 3, 5 and 8; M3 and M4 at the fixed
  `w`) is recorded here, and D9 is re-checked on its first-half table.

## Decision

1. **Clock: closed 1m bars (ADR-031).** Location steps only on an event
   that closes a 1m bar. An empty bar (no `ohlc`) neither counts nor fails
   anything.
   - *Why:* it is the finest closed-bar clock, so time beyond a level has
     1-minute resolution and the noise model of D3 is per minute (M0a). It is
     also the clock of `profile.volume.utc_day@1` (ADR-036).
   - *Rejected:* per trade — single prints make the state flicker, and it
     means checking a few hundred levels on each of about 600 M trades a
     year (603 218 226 aggregates in the window, `docs/data-availability.md`);
     30m TPO periods — failures cluster in the first closes (5.6–7.4 % per
     close over the first five, M0b), which a 30-minute clock cannot see.
2. **Stage order and atomicity.**
   - The VWAP steps with the other trackers and can fail
     (`StateError::Overflow`). Everything else runs after the last commit,
     on the committed state of the same event, and cannot fail: comparisons
     are `i128` on `i64` inputs, and the counters count 1m closes since
     registration (`u32` holds about 8 000 years of minutes, so they never
     saturate in practice). A rejected event leaves the whole state
     unchanged.
   - The visibility time of every value and fact is the time of the event
     that closed the bar (ADR-037 D8 semantics); bar ends are separate
     fields.
   - When one event closes several 1m bars, or the 1d bar, the classifier
     first classifies the old day's closes against the reference it holds,
     then switches to the newly published prior-day profile.
   - *Rejected:* stepping location on copies like the other families.
     `book.clusters@1` exists only after the liquidity commit, so location
     would have to duplicate every commit.
3. **Location tolerance `w` = 6 bps of the level price.** Exact:
   `|p − L| · 10 000 ≤ |L| · 6`, in `i128`. One tolerance serves D5–D8.
   - *Fixing rule, pre-registered:* `w` = the first-half p75 of the exact
     1m close-to-close move (M1), rounded to whole bps. The first half fixes
     the number so that the second half stays a check rather than a source.
     The first-half p75 is 5.93 bps, so `w` = 6. The plan-time value was 5
     (M0a whole-window p75 5.18; the exact whole-window p75 is the same,
     5.18).
   - *Why the p75:* a probe starts beyond one side of an edge band and fails
     beyond its other side, a gap of `2w` = 12 bps. In the first half 10.35
     bps is the p90 and 14.28 the p95 (M1), so fewer than 10 % of minutes
     move that far and a probe rarely starts and fails within one minute.
     At `w` = 5 the gap of 10 bps lies below the first-half p90: more than
     10 % of those minutes could start and fail a probe at once.
   - *At `w` = 3 (≈ p50):* breakouts rise 26–30 % against `w` = 5 (884
     against 700 in the first half, 801 against 618 in the second) and the
     first-close hazard rises to 11.8 % against 7.9 % (first half, M2) —
     whipsaw pairs.
   - *At `w` = 8:* the two edge bands (`2 × 2w` = 32 bps) cover about half of
     a p10-wide value area (67.9 bps, M1), and the failures caught by 60
     closes drop to 75.8 % (71.2 % on the second half, M2). At `w` = 6 the
     bands cover 24 bps, about a third.
   - *Second half:* its p75 is 4.51 bps (M1); the market was calmer. One
     constant serves both halves by design (ADR-013: no regime-dependent
     tolerance until one is measured to help), and the re-run reports M2 at
     `w` = 6 for both halves.
   - *One tolerance:* it keeps `AtValueEdge` inside the VAH/VAL monitored
     zone, so "entered a monitored VAH zone" agrees with the classifier, and
     it adds no parameter nobody measured.
   - Parameter `tolerance` = 0.0006 (`Rate`).
4. **VWAP `location.vwap.utc_day@1`.**
   - `Σ(price · qty) / Σqty` over the trades of the current UTC day's closed
     minutes; the developing minute is excluded, as in the developing
     profile. The sums are exact `i128`, checked; an overflow rejects the
     event. Floor division to `Price`: the error is below 1e-8 USDT.
   - *Why the UTC day:* the auction reference (the prior-day profile) and the
     developing profile use it (ADR-036), and the 1d bar and the archive day
     close there (ADR-031, ADR-034). The perpetual has no exchange session;
     a VWAP anchored elsewhere would describe another auction than the value
     area it is compared with.
   - Warm-up `Samples(1)`, a closed minute of the day with volume; it
     restarts each UTC day. No bands and no prior-day VWAP in `@1`
     (ADR-013: none is measured yet).
5. **Level registry `location.levels@1`**, rebuilt at each closed 1m bar
   with trades from:
   - `profile.volume.prior_day@1` and `profile.volume.composite_5d@1`: POC,
     VAL, VAH, HVN and LVN (`VolumeProfile::levels`);
   - `structure.levels.<15m|1h|4h|1d>@1`: structural highs and lows, prior
     sweeps and SFP rejection zones (`LevelRegistry::levels`);
   - `book.clusters@1`: each top-5 candidate per side as `LiquidityCluster`,
     live only. No multiple-of-median threshold: ADR-043 M3 already puts the
     5th candidate at ≥ 80× the side median, and a threshold here would be a
     number nobody measured;
   - `location.vwap.utc_day@1`: one `Vwap` level.

   Not in `@1`: the developing profile's levels — its value area grows to
   contain price by construction, so "entering" it says nothing, and its
   constant re-identification would dominate the event stream;
   `ValidatedReference` — admitting such levels is #33's job; the kind
   stays, with no source.

   Zones are closed `[low, high]`. A level's identity carries its
   registration time, touches and in-zone flag across rebuilds: kind,
   source, side, anchor (the swing bar for structure levels, the day for the
   VWAP) and the price for every kind but the VWAP, whose price moves. The
   registry is ordered by (zone low, zone high, kind code, source,
   identity); no hash collections.
6. **Score = components, no scalar.** ADR-013 applies, and the weights are
   #31's measured output. Per level: `distance` (signed `Price` from the last
   close to the unpadded zone, 0 inside, bps derived on demand); `touches`
   (`Entered(Arrived)` since registration — one meaning for every kind; the
   ADR-037 touches stay on the structure registry); age (`now − known_at`,
   on demand); `source` (feature key and kind); `confluence` (levels from
   *other* sources whose zones lie within `w` of the level's price — same
   source excluded, because a POC is usually an HVN of its own profile);
   `strength` (cluster quantity and side median, exact; `None` otherwise).
   `nearest(n)` sorts by `|distance|` on demand.
   - *Rejected:* a weighted scalar score — its weights would be numbers
     nobody measured.
7. **Monitored-location events.** `LocationEvent::{Entered{Arrived|Created},
   Left{Departed|Retired}, Auction{from, to}}`, exposed by
   `MarketStateEngine::location_events()`. Not hashed; the in-zone flags and
   the auction state they follow from are.
   - A level is in zone while the closed bar's `[low, high]` overlaps its
     zone padded by `w`. *Why the range and not the close:* "entered" means
     price traded there, and Left then means a whole minute away — a
     debounce without a parameter.
   - Created and Retired mark levels that appear or vanish while price is in
     their zone, so every Entered is closed by exactly one Left.
   - Clusters are scored, not monitored: they lie within 5 bps of mid by
     construction, so their zones would always be occupied.
   - Order per bar: the auction transition, then the Lefts, then the
     Entereds, each in registry order.
8. **Auction state `location.auction.prior_day@1`** against the prior-day
   value area `[VAL, VAH)` (ADR-036).
   - *Why the prior day:* the developing day's value contains price by
     construction, which makes the classification tautological.
     `composite_5d` gets its own id if research asks for it (ADR-013).
   - Regions of a close `c` (exact, D3): Above `c > VAH + w`; Below
     `c < VAL − w`; In `VAL + w < c < VAH − w`; otherwise neutral, in an edge
     band.
   - The first close under a reference sets the accepted region `A`
     (origin `Start`); a neutral close counts by the side of the edge
     (`c ≥ VAH` Above, `c < VAL` Below, otherwise In).
   - Probes: a close in a region `R ≠ A` starts a probe toward `R`, or
     retargets an active probe toward another region and resets its count;
     the starting close counts. A close back in `A` fails the probe. A close
     counts when it is beyond the edge itself (`c ≥ VAH` upward, `c < VAL`
     downward, `VAL ≤ c < VAH` toward In); other neutral closes neither
     count nor fail. At `N_acc` counted closes `A := R`, origin
     `Acceptance`.
   - Labels, exhaustive, in priority order: an active probe toward Above or
     Below → `Breakout`; a failure → `FailedBreakout` (a probe out of value)
     or `FailedReclaim` (a probe toward In), held until `N_acc` closes count
     back toward `A` or a new probe starts; with an active probe toward In or
     `A` = In → `AtValueEdge` on a neutral close, otherwise `InsideValue`;
     `A` outside value → `Acceptance` (origin `Acceptance`) or
     `OutsideValue` (origin `Start`). The value carries the region, origin,
     the close's position, the probe and the failure, so every label can be
     traced.
   - The classifier resets at each new prior-day publication. It is
     `Unavailable(InputInvalid)` while the prior day is unavailable or
     `VAH − VAL` is at most both edge bands (no In region); that never
     happened in the window (narrowest value area 19.1 bps against 12 bps of
     bands at `w` = 6, M1).
   - *Why time, not volume:* a volume share needs a denominator (the
     session? the reference day?) that swings with time-of-day activity.
     Volume can come in a `@2`, with a measurement, if #31 asks for it.
9. **`N_acc` = 60 closes (1 h).** The hazard per counted close shows no
   knee: it falls smoothly from 4.2–7.9 % over the first five closes to
   0.86 % at closes 45–59, 0.45 % at 60–89 and below 0.1 % from 180 on (M2,
   `w` = 5, first half; M0b showed the same shape).
   - *At 60:* 81.2 % of same-day failures are already labelled
     `FailedBreakout` (77.5 % in the second half), and the hazard is 5–9×
     below each of the first five closes — the ratio M0b's 0.88 % against
     5.6–7.4 % had at plan time.
   - *At 30:* 31.6 % of failures would be labelled `Acceptance` first (68.4
     % caught).
   - *At 120:* 7 more points of failures are caught (88.3 %), but every
     `Acceptance` label waits 2 h.
   - *Stated limit:* 54.6 % of breakouts that held 60 closes still return
     inside prior-day value the same day (M2). Acceptance is descriptive
     ("held for an hour"), not predictive; whether it predicts anything is
     for #26/#31 to measure, and confirmation depth is a research variable
     (ADR-015).
   - The Market Profile convention of two 30-minute periods agrees with 60;
     corroboration only, not the reason.
   - Failure labels last the same `N_acc`: the evidence that would have
     accepted the probe also retires its failure label, so there is no extra
     parameter.
   - *Fixing rule:* `N_acc` stays 60 unless the exact first-half table (M2)
     contradicts either reason — about four in five same-day failures caught
     by 60 closes, and a hazard at 45–59 well below (about 5× or more) the
     first closes. If it does, the change stops and is reported on #23; no
     other value is picked silently. The `w` = 5 table supports 60; the
     re-check on the `w` = 6 re-run is pending (*Measurements*).
10. **Features** (the ids are permanent; `location` joins the families of
    "Adding a feature").

    | Feature | Parameters | Inputs | Warm-up |
    |---|---|---|---|
    | `location.vwap.utc_day@1` | `rounding` `floor`, `session_ms` 86 400 000 | trades, `bars.time.1m@1` | `Samples(1)` |
    | `location.levels@1` | `confluence_rule` `other_source_within_tolerance`, `tolerance` 0.0006, `zone_rule` `bar_range_overlap` | the eight D5 sources, `bars.time.1m@1` | `Samples(1)`: a closed 1m bar with trades |
    | `location.auction.prior_day@1` | `acceptance_closes` 60, `tolerance` 0.0006 | `profile.volume.prior_day@1`, `bars.time.1m@1` | `Samples(1)`: a closed 1m bar with trades under a ready reference |

    The classifier is a separate id so that recalibrating `N_acc` re-versions
    only the classifier (ADR-037 D10 reasoning). `MarketState.location` is
    declared after `book` and hashed (ADR-041 D1).

## Consequences

- The feature set and every state-hash golden move; runs recorded before
  this change compare events only (ADR-041 D2, D4).
- Each closed minute rebuilds the registry: `O(n log n)` over its levels
  (a few hundred: every structure registry holds at most 20 active highs,
  20 active lows and 20 resolved sweeps plus pending sweeps and SFP zones
  per timeframe, ADR-037 D5; p50 254 and p99 342 in the window, M3), plus
  two `i128` additions per trade for the VWAP. The full year replays in
  19.3 min with location against 20.5 min for the #11 replay (M1–M4 note on
  comparability).
- Zone events are many — about 1 380 arrivals and 1 400 departures a day
  at `w` = 5 (M3) — so #25 (episodes, cooldowns) and #38 (alert rate limits)
  consume them, not egress directly. The Created/Retired causes (about 46 a
  day) and the unmonitored clusters bound them.
- Archive registries hold no cluster levels (ADR-043: live only).
- Location emits facts only — no bias, no direction, no trigger (ADR-012).
  The trigger rules for failed auction, breakout with acceptance and failed
  reclaim belong to #24.

## Alternatives considered

- **Per-trade evaluation** (D1) and **step-on-copies staging** (D2):
  rejected for the reasons given there.
- **A weighted scalar score** (D6): rejected; weights are #31's output.
- **Volume-based acceptance** (D8): rejected for `@1`; no measured
  denominator.
- **The developing day or the 5-day composite as the auction reference**
  (D8): tautological for the developing day; the composite can get its own
  id when research asks.
- **A cluster threshold** (D5): rejected; it would be an unmeasured number.

## Accept when

#13's acceptance run (ADR-041 *Accept when*), on a binary with this feature
set, reports every run `EQUIVALENT` with the location state compared. Live
is the only thing the pre-merge measurement cannot settle: the cluster
levels and the live clock exist only in live capture.

References: ADR-012, ADR-013, ADR-015, ADR-019, ADR-027, ADR-029, ADR-031,
ADR-034, ADR-036, ADR-037, ADR-041, ADR-043.
