# ADR-051: Order-flow candidates — divergences on confirmed swings, effort vs result, exhaustion and absorption

- Status: proposed
- Date: 2026-10-12

## Context

#22 asks for deterministic candidate detectors for the composite order-flow
concepts of brief §8: CVD, OI and price divergences defined on confirmed
swing points; absorption (high aggression into a level, passive liquidity
replenished, little price progress); exhaustion (aggression decaying at an
extreme after expansion); and effort vs result (net aggression against
displacement relative to a rolling baseline). Each detector is versioned
with explicit parameters and emits candidate events that carry their inputs.
Its acceptance criteria: synthetic tape tests per detector that fire on the
constructed pattern and not on near misses, and candidate events that carry
location context and feature versions, never a direction or an execution
meaning.

Candidates are hypotheses, not signals. Conditional Behavior and research
decide whether they mean anything (ADR-012, ADR-013); aggression is not
direction (ADR-023).

What exists, and what each detector is built from:

- Confirmed swings per structure timeframe, `structure.swing.<tf>@1`
  (ADR-037). ADR-037 D10 split the swings out of the level registry so
  that the candidates of this issue could build on them alone.
- The continuous CVD `flow.cvd.continuous@1` and the rolling aggression
  windows `flow.window.<5m|15m|1h>@1` (ADR-035). ADR-035 decision 6 leaves
  "baselines and effort-versus-result judgments" to this issue.
- The 5-minute open-interest grid `derivatives.oi.5m@1`, produced by both
  the archive and live capture (ADR-042 D2).
- The book liquidity windows `book.liquidity.window.<5m|15m|1h>@1`, live
  only, on the same clock as the flow windows (ADR-043 D6).
- The level registry and the auction state (ADR-044), the Market State hash
  (ADR-041), the feature registry with its lock and goldens (ADR-029), and
  exact fixed point (ADR-027).

The design keeps tuned numbers to a minimum. Divergences are sign rules and
need no threshold. Effort vs result needs two numbers, a baseline length
`B` and one high quantile `q_hi`, fixed by two rules registered here in
advance. The rules were first run on the archive year of #12 (M2); since
#124 they run on a hold-out year that no run had read (D9), and the #12
year is the development year. Exhaustion and absorption are
built on effort vs result and add no tuned number of their own. The
alternative, an independent threshold per detector set by judgment (a "top
decile" effort, a 2× replenishment ratio, a minimum divergence size), means
about ten numbers nobody measured. ADR-043 decision 8 refused its cluster
threshold for that reason, and the ADR content rule of this repository
forbids it.

Delivery is in three steps under #22: M1 ships this ADR, the candidate
framework and the divergences; M2 ships the measurement tool that runs
R1b and R1 (below); M3 fixes `B` and `q_hi` from the hold-out run of D9 and
ships effort vs result, exhaustion and absorption. The M2 run on the #12
year stopped on R1b; D9 records that result, the revision of R1b's
statistic it led to (#124) and the hold-out year the revised rules run on.

## Decision

### The #22 / #24 boundary (normative for every candidate)

- **#22 ships Market State facts:** the `candidate.*@1` features (values
  in `MarketState.candidates`) and the `CandidateEvent`s
  (`MarketStateEngine::candidate_events()`). Every event carries its
  inputs with their feature keys and a location context (D3).
- **Candidates never carry a bias, a direction, a location gate or an
  execution meaning.** Nothing in #22 decides that a candidate is a
  trigger.
- **#24 owns the trigger rule of each `TriggerFamily`:** the rule that
  turns a candidate, plus location and bias, into a trigger event with its
  own parameter version. This is the split ADR-037 already made between
  the structure fact `StructureEvent::Sfp` and the trigger family
  `TriggerFamily::Sfp`.
- **Location gating belongs to #24** (ADR-012, brief §11). "At VAH" or
  "into a registry level" are trigger-rule conditions. A candidate records
  its location context and never filters on it, so one candidate can be
  tested against several locations.
- `crates/mie-domain/src/trigger.rs` changes only in its docs: the
  `CvdDivergence` (M1) and `Absorption`, `Exhaustion` and `EffortVsResult`
  (M3) variants link to their candidate fact, as `FailedAuction`, `Sfp` and
  `FailedReclaim` link to theirs. No `TriggerFamily` variant is added: the
  OI divergence feeds no variant directly, and `OiExpansionRejection`
  stays a #24 rule.
- **Naming.** No type, field or `Display` text says bullish, bearish, long,
  short, signal, reversal or continuation. The only sided fields are facts:
  `structure::Side` (which extreme), `event::Aggressor` (which taker side
  carried the net aggression, ADR-023) and `book::Side` (which passive
  side).

### D1. Facts and exposure

- `MarketState.candidates` is appended as the **last** field, after
  `trade_count`. *Why last:* ADR-041 D2 then holds unchanged. Every field
  encoded before it is written as before, with no encoding bump, and the
  new feature-set version makes old and new hashes incomparable. A test
  pins that the engine still reproduces the two encoding-v1 goldens pinned
  before #22 once the field is left out and the pre-#22 stamp is put back.
- `Aggressor` gets the state-hash code Buy 0, Sell 1 (declaration order),
  for the effort-vs-result values of M3.
- ADR-041 is accepted and is not edited; this decision records the
  extension of its field list.
- Candidate events are not hashed, the same as structure and location
  facts: the values they follow from are state.

### D2. The stage

- The candidate stage runs after the location stage in
  `MarketStateEngine::apply`, on the committed state of the same event, and
  cannot fail. *Why after location:* every event carries a location context
  (D3), read from the registry the same event rebuilt.
- Arithmetic is exact: `i128` on `i64` inputs. A derived difference that
  does not fit its type makes that value `Unavailable{OutOfRange}`; it
  never rejects the event. *Why:* an ordinary trade must not be rejected
  for every family because one derived difference is out of range (the
  reason of ADR-043 D9). For a divergence, the value of a family is
  `Unavailable{OutOfRange}` while the last comparison of either side did
  not fit, and the next comparison that fits makes it ready again.
- The tracker is mutated in place, never cloned per event; the stage reads
  only the committed state, so it cannot move any other field.
- Facts are emitted per closed bar in close order, per swing that bar
  confirmed, the CVD comparison before the OI one.

### D3. Location context (events only, never a feature input)

- A `LocationContext` holds the anchor price and two parts:
  - `levels`: a `FeatureValue` that names the registry read
    (`LevelSet.feature`, `location.levels@2` or `@1`) and lists the
    identity and zone (kind, source, side, anchor, price, low, high) of
    every registry level whose zone, padded by the registry's own
    tolerance, contains the anchor price. That is the in-zone test of
    `location/registry.rs` (ADR-044 D7) applied to the point `[p, p]`, a
    crate-private helper on the same code. Warming up or unavailable when
    the registry is.
  - `auction`: a copy of `state.location.auction`.
- For a divergence the anchor is the swing price, and every registry level
  derived from the candidate's own swing is left out, whatever its kind:
  same `structure.levels.<tf>` source, same side, anchored at the same
  swing bar. That is its structural level, and the prior sweep and SFP
  zone it becomes once a trade goes beyond it. *Why:* the context says
  where the candidate happened, and the candidate's own swing is the
  candidate, not a place. The event that confirms a swing also enters it
  into the registry, so every context would list it; and when the
  confirming trade itself trades beyond the swing price, structure sweeps
  the new level in the same event, so a kind-bound exclusion would list it
  as a prior sweep or not depending on that one trade's price. Levels of
  the same swing bar from other timeframes' registries stay: they are
  other swings. For the other candidates (M3) the anchor is the last
  close.
- *Why no location input:* if a candidate definition declared
  `location.levels@2`, then `catalog::current_set_with(&[location.levels@1])`
  would leave that candidate without its upstream. That breaks
  `with_features` for `location.levels@1` (ADR-044 D10), the
  `selectable_versions_are_older_than_current` test and the pre-#96 pin.
  The context names the registry version it read instead, and a test
  builds the `location.levels@1` engine.

### D4. Divergences

`candidate.divergence.cvd.<tf>@1` and `candidate.divergence.oi.<tf>@1`,
tf = 15m, 1h, 4h, 1d.

- **Timeframes.** The structure timeframes. #22 defines divergences on
  confirmed swings, and swings exist only there (ADR-037 D1).
- **Anchor.** For each structure timeframe the tracker records the first
  trade that sets the developing bar's high, and the first that sets its
  low. A strictly higher price (lower, for the low) replaces the record,
  so a tie keeps the first. At that trade it records
  `flow.cvd.continuous@1` after the trade (its `cvd` and `gaps`), the ready
  `derivatives.oi.5m@1` point if there is one and its boundary lies at most
  360 000 ms before the trade, and the trade time.
- **Why 360 000 ms for the OI point.** A grid point stays ready until an
  open-interest event closes the next boundary, so during an open-interest
  outage the ready point can be arbitrarily old, and an anchor would read
  an hours-old open interest as the open interest at the swing (the
  review of #120 found two such divergences on the golden tape, at 26 and
  96 minutes). The bound is one grid interval (300 000 ms: until the next
  boundary is due) plus the grid's `max_age_ms` of 60 000 ms. *Why the
  slack:* a live boundary closes only on the first 10 s poll after it, so
  for a few seconds of every interval the newest ready point is just over
  one interval old although the feed is healthy; without the slack those
  anchors would lose their point live but not in the archive, whose
  samples sit on the boundaries. *Why 60 000 ms and no more:* it is the lag
  `derivatives.oi.5m@1` itself accepts between a boundary and its sample
  (ADR-042 D2); a feed that has not delivered within it leaves the next
  boundary `Unavailable` anyway, so a point older than 360 000 ms means an
  outage, and the anchor has no point.
- **Lookup.** Closed bars' records sit in a ring of `SWING_BARS + 1` = 4
  per timeframe. *Why 4:* the swing bar is exactly `SWING_BARS` closed bars
  before the confirming bar, because every elapsed interval yields a bar,
  empty or not (ADR-031, ADR-037 D3); a shorter ring loses the swing bar, a
  longer one holds records nothing reads. The stage walks the event's
  closed bars in close order and, after each one, handles the
  `StructureEvent::Swing` facts it confirmed (`confirmed_bar_end ==
  bar.end()`); the trade comes last. This ordering lets a swing confirmed
  inside a multi-bar close (after a silence) find its bar. A confirmed
  swing whose bar has no matching record, which the ring rules out for
  every swing the engine itself confirms, breaks the side's chain of pairs
  instead of comparing across it.
- **Why the first trade at the extreme.** Then `cvd_new − cvd_prev` is
  exactly the net taker aggression between the two price extremes when
  the gap counts agree (ADR-035 D2). A divergence then reads "price
  reached a further extreme while net aggression since the previous
  extreme ran the other way". Rejected: the CVD at the swing bar's close,
  which mixes in the aggression after the extreme; and the CVD's own
  intrabar extreme, which needs per-bar CVD high/low state and belongs to
  no single price print.
- **Pairs.** Two consecutive anchored swings of one side and timeframe
  form a pair. It carries both anchors (swing time, known-at, price,
  anchor trade time, CVD, gap count, swing coverage), `price_change`,
  `cvd_change`, `gaps_between`, `span_ms` (anchor to anchor) and its
  coverage.
- **CVD rule** (signed, so it mirrors): high side `new.price > prev.price`
  and `cvd_new < cvd_prev`; low side `new.price < prev.price` and
  `cvd_new > cvd_prev`. Every comparison is strict, so an equal price or
  an equal CVD is no divergence.
- **OI rule** (unsigned, so it does not mirror): on either side, a further
  extreme (higher high or lower low) with `oi_new < oi_prev`. *Why
  unsigned:* open interest counts positions on both sides, so a new
  extreme reached on fewer open positions is the non-confirmation on
  either side. *Why the 5-minute grid:* both the archive and live capture
  produce it (ADR-042 D2), so the feature means the same in both. If
  either anchor has no grid point (none ready, or one older than the
  anchor bound above), the OI comparison has no change
  and is not divergent; the value records the missing point.
- **No magnitude threshold in `@1`.** Each comparison carries its changes,
  the gaps and the span, so #28 and #31 can threshold on measured
  contribution (ADR-013). A size floor would be a number nobody measured,
  and would force a `@2` once research finds the useful one.
- **Coverage.** The OR of both swings' coverage, plus `feed_gap` when
  `gaps_between > 0`: the CVD change then misses whatever traded inside
  the gaps.
- **Value and warm-up.** The value holds the last comparison of each side,
  `None` until that side's second anchored swing. Warm-up is `Samples(1)`,
  where a sample is an anchored confirmed swing. An event is emitted only
  for a divergent comparison, by the event that confirms the later swing,
  at the swing's `known_at`: there is no look-ahead beyond ADR-037's own
  confirmation delay.
- **Inputs.** `structure.swing.<tf>@1`, `bars.time.<tf>@1`,
  `flow.cvd.continuous@1` and trades; for OI also `derivatives.oi.5m@1`.
- **Parameters.** `anchor_rule` = `first_trade_at_extreme`,
  `compare_rule` = `strict`, `timeframe_ms`.

### D5. Effort vs result

`candidate.effort_result.<5m|15m|1h>@1` (M3).

- **Inputs.** `flow.window.<tf>@1`, the windows ADR-035 built for this
  judgment. A new horizon would be its own id.
- **Effort and progress.** `effort = |delta|`. `progress = displacement ×
  sign(delta)`: price movement in the direction of the net aggression, at
  most 0 when price did not move or moved against it. There is no
  evaluation when `delta == 0` or `displacement` is `None`.
- **When it evaluates.** Once per event that closes at least one 1m bar,
  on the windows after that event. A gap that closes k minutes in one
  event yields one evaluation; that window already carries `feed_gap`.
- **Baseline.** The previous `B` evaluations of the same window (the
  current one excluded), held in a ring. Quantiles are nearest-rank and
  exact on integers: the q-quantile is the ⌈q·B⌉-th smallest value, so
  q = 0.5 is the lower median ADR-043 uses.
- **The value carries** `effort`, `progress`, `effort_high` (the `q_hi`
  quantile of effort), `effort_median`, `move_high` and `move_median` (the
  same quantiles of `|displacement|`) and `active`.
- **Candidate condition.** `effort ≥ effort_high` and `progress ≤
  move_median`: aggression in the tail, with less progress than a typical
  window's absolute move. *Why the median for "little progress":* it is the
  parameter-free meaning of "less than a typical window"; any other
  quantile would be a second tuned number.
- **Emission.** On the rising edge only, when `active` turns from false to
  true. *Why:* overlapping windows keep a condition true for many minutes,
  and an edge rule needs no parameter. Cooldowns and episode
  de-duplication belong to #25.
- **Order of the two fixing rules: R1b fixes `B`, then R1 fixes `q_hi` at
  that `B`.** *Why:* `q_hi` is a quantile of the last `B` evaluations, so
  the same `q_hi` names a different tail under a different `B`; a `q_hi`
  read before its `B` is fixed means nothing. R1b passes a `B` only if it
  holds in every evaluable (window length, `q`) combination, so `B` does
  not depend on the `q_hi` R1 picks later. Rejected: a joint search over
  the 3 × 6 (`B`, `q`) pairs. It would let the episode floor choose `B`
  for the count it happens to yield rather than for what `B` is for, and
  it fits two numbers to one half.
- **Halves (both rules).** Split at the midpoint of the measurement year
  (D9): 2025-04-01 12:00 UTC for the hold-out year 2024-10-01 … 2025-09-30.
  *Why the midpoint:* it gives two equal halves of the 365-day window, so
  the half that sets a value and the half that checks it carry the same
  exposure (the split ADR-037 and ADR-044 use on the #12 year, at
  2026-04-01 12:00 UTC). The first half sets the value; the second half
  only checks it. One continuous replay; an episode belongs to the half of
  its emission time.
- **`B`: fixed by R1b (M3).** `B` exists so that a rank does not depend on
  which hours of the day filled the baseline. R1b measures exactly that.
  - **Candidates.** `B` ∈ {60, 240, 1440}: the spans of the 1h, 4h and 1d
    bars (ADR-031) in evaluations, one per minute. 60 and 240 are sub-day
    baselines, which R1b picks if the intraday activity cycle turns out
    not to distort a rank. 1440 is the shortest baseline that holds every
    UTC hour once.
  - **Reference.** `B_ref` = 10 080, one week. It holds every UTC hour 7
    times and every weekday once, so neither the hour nor the weekday that
    filled it decides a rank. It is the longest baseline in the grid.
  - **Statistic (revised for #124; M2 used the coefficient of variation,
    D9).** For each window length, each grid `q` and each half, `n_B(h)` and
    `n_ref(h)` are the episodes of `B` and of the reference emitted in UTC
    hour `h` (0–23), with `m(h) = n_B(h) + n_ref(h)`, `N_B = Σ n_B`,
    `N_ref = Σ n_ref` and `N = N_B + N_ref`. The statistic is Pearson's X²
    of the 2 × 24 table (`B`, reference) × hour:
    `X² = Σ_h (n_B(h)·N_ref − n_ref(h)·N_B)² / (m(h)·N_B·N_ref)`, which
    equals `Σ (O − E)²/E` over the 48 cells with `E_B(h) = m(h)·N_B/N` and
    `E_ref(h) = m(h)·N_ref/N`.
    - If `B` ranks every hour the way the reference does, each hour's
      `m(h)` episodes split between the two baselines with the same
      probability in every hour, whatever that hour's level, and X² follows
      χ² with (2 − 1)(24 − 1) = 23 degrees of freedom. A different number
      of hours or of baselines gives a different count; 23 is not a choice.
  - *Why the split, not the raw per-hour rate.* A short baseline makes raw
    rates nearly uniform by construction: `B` = 60 ranks each window
    against its own hour, so "high effort" fires about as often in the
    quietest hour as in the busiest. That is the behaviour `B` must avoid,
    so a uniformity test on raw rates would select the `B` this rule exists
    to reject. The hour's own level cancels in the split, so an hour-of-day
    profile the reference also has is never a failure, whatever that
    profile is.
  - *Why count-weighted, not the coefficient of variation of the ratios
    `n_B(h)/n_ref(h)` (M2's statistic).* In X² each hour enters with the
    information its count carries. The coefficient of variation gave an
    hour of 10 episodes the weight of an hour of 58, and at `n_ref(h)` = 1–3
    a ratio of Poisson counts is heavy-tailed, so its χ²-scaled null did not
    hold there. One sparse hour decided M2's verdict (D9).
  - *Why Pearson's X², not the likelihood-ratio G.* X² follows χ² more
    closely at small expected counts (Larntz 1978; Koehler and Larntz
    1980). On the #12 year's 1h, q = 0.975, second-half table of `B` = 1440,
    which the evaluability rule below excludes, the two statistics give
    p = 0.0097 and 0.00087, on either side of α/k: with G the verdict would
    depend on the formula, not on the data.
  - *Why UTC hours.* They resolve the daily activity cycle, and M2 counts
    by them. A finer bin divides the counts and adds noise without asking
    a finer question.
  - **Evaluability (Cochran 1954).** A combination is evaluable when no
    expected count is below 1 and at most 9 of its 48 expected counts
    (20 %) are below 5. Computed exactly in integers: `E_B(h) < c` ⇔
    `m(h)·N_B < c·N`, and the same for `N_ref`. A combination with
    `N_B = 0` or `N_ref = 0` is not evaluable, and so is one with an hour
    where `m(h) = 0` (its expected counts are 0).
    - *Why:* it is the standard condition under which X²'s χ² tail can be
      trusted; 9 is the largest count that is at most 20 % of 48.
    - *Looser:* the error of the asymptotic tail at α/k grows to the size of
      the G example above, so a verdict can flip on the approximation.
    - *Stricter (every expected count ≥ 5):* it removes M2's failing
      combination (6 of 48 below 5), which is the carve-out the maintainer
      ruled out (D9), and it leaves more window lengths without an
      evaluable combination, so R1b stops more often for want of data.
    - A non-evaluable combination is recorded and does not count against
      `B`. A window length without one stops R1b only for a candidate that
      fails no evaluable combination of that half (the order is under Stop
      rules, below).
  - **Threshold.** `X² ≤ χ²₂₃(1 − 0.05/k)`, where `k` is the number of
    evaluable (window length, `q`) combinations of that `B` and half, at
    most 3 × 6 = 18. The critical value is 35.172 at k = 1 and 46.258 at
    k = 18.
    - *Why 0.05:* the Bonferroni split keeps at 5 % the chance that a `B`
      which ranks like the reference fails anywhere in its half. 5 % is
      also the level of `n_min`'s 1.96 (R1), so the two rules share one
      error rate.
    - *What breaks at 0.01:* a distorting `B` passes more often, which is
      the error R1b exists to prevent. *At 0.10:* a `B` that ranks like the
      reference fails somewhere one time in ten, and R1b stops for nothing.
  - **Approximations.** Two pull in opposite directions.
    - Episodes cluster, so the true noise is larger than the multinomial
      one and the test is stricter.
    - `B` and the reference read the same tape, so their hour tables are
      positively correlated, while X²'s homogeneity test assumes two
      independent samples. The variance of the split is then smaller than
      the test assumes, and the test is conservative toward PASS. On the
      #12 year this side dominates near the reference: `B` = 1440's
      evaluable X² lie between 8.6 and 27.0, with means of 16.8 (first
      half) and 14.9 (second), well below the 23 degrees of freedom a
      χ²₂₃ variable averages, and its median p is 0.84 on the first half
      and 0.92 on the second, where 0.5 would be expected.
    - Consequence, recorded before the hold-out run: a hold-out PASS of
      `B` is weaker evidence than its nominal p says; a mild hour
      distortion at 1440 could pass. This is recorded, not corrected. The
      selection itself is unaffected: the test's job is to reject baselines
      that rank by hour, and on the #12 year `B` = 60 and 240 fail every
      evaluable combination with X² from 56.1 to 501.9 against critical
      values of at most 45.4 (D9).
  - **Counting.** An episode belongs to the UTC hour of its emission time.
    Only episodes emitted while the reference baseline is ready count, so
    every `B` is compared over the same minutes. The tool records, for
    every combination, X², p, the cells below 5 and below 1, the
    evaluability, k and the critical value.
  - **Selection.** `B` is the smallest candidate that passes in every
    evaluable combination on the first half. *Why the smallest:* a shorter
    baseline holds fewer values, warms up sooner (`Samples(B)`) and
    follows a change of volatility sooner; once the hour-of-day profile is
    kept, a longer baseline buys nothing R1b measures. The second half
    checks the chosen `B` with the same test.
  - **Stop rules.** The candidates are taken smallest first, and each `B`
    is decided by the first of these steps that applies:
    1. `B` fails an evaluable combination on the first half: `B` is out and
       the next candidate is taken. A window length of `B` without an
       evaluable combination does not matter then: a failing combination
       already rules `B` out.
    2. A window length has no evaluable combination on the first half:
       stop. A larger `B` is not taken: it would be picked while this one
       may still pass.
    3. `B` fails an evaluable combination on the second half: stop.
    4. A window length has no evaluable combination on the second half:
       stop.
    5. Otherwise `B` is fixed.

    If every candidate leaves at step 1, no candidate passes on the first
    half: stop. Every stop is reported on #22. `B_ref` is never picked by
    default: it cannot be tested against itself, and its 7× state and
    7-day warm-up are costs a human accepts, not a rule.
- **`q_hi`: fixed by R1 (M3), at the `B` R1b fixed.** `q_hi` is the
  highest value of {0.80, 0.85, 0.90, 0.95, 0.975, 0.99} at which, on the
  first half of the measurement year (D9), every window length yields at least
  `n_min` = 97 episodes in every floor cell (MC below).
  - **Cells.** #28 splits its comparisons by regime and by location (its
    scope: "regime/location splits", with location named as "VAH/VAL,
    structural extremes"). Its estimates therefore live in cells of a
    `volatility.regime.1h@1` label (4) × a location bin (3): 12 cells per
    window length. The bin is read from the episode's D3 context, the
    registry levels whose padded zone contains the anchor:
    - `value_edge`: the context lists a `Vah` or `Val` level, from either
      profile the registry reads (the prior day and the 7-day composite);
    - `structural_extreme`: it lists a `StructuralHigh` or `StructuralLow`
      level;
    - `neither`: it lists neither.
  - *Why these bins.* They are the two location factors #28 names, plus
    the remainder that #28's comparison with the unconditional baseline
    needs.
    - VAH and VAL form one bin, as `AuctionState::AtValueEdge` does
      (ADR-044), and highs and lows form one bin. #28's hypotheses are
      stated for both sides and every event carries its side, so a side
      split is a contrast #28 does not name, and it would halve every
      cell.
    - `value_edge` and `structural_extreme` may overlap; an episode in
      both counts in both, because #28 compares each factor with the
      baseline. A precedence between them would be a choice no source
      makes. A separate "both" cell would be the interaction, which #28
      does not name; as the rarest cell it would decide `q_hi` for a split
      nobody asked for.
    - Not bins: `PriorSweep`, a level price has already gone beyond and so
      no longer an extreme (at a p50 of 80 per closed minute, ADR-044 M3,
      it would sort episodes by proximity to old prices); SFP zones, which
      are #24's trigger context (ADR-037 D7); POC, HVN, LVN, the VWAP and
      clusters, which #28 does not list (they fall into `neither`); and the
      auction state. The auction state is a condition of #28's third
      hypothesis pair, not one of its location splits; the prior-day VAH
      and VAL it classifies against are registry levels already; and for a
      divergence it describes the last close, not the anchor.
  - **Selectability.** The bins are computed by the measurement tool from
    the recorded context, never by the engine. Location stays out of every
    definition's inputs, so the `location.levels@1` selection (D3) is
    untouched. R1 runs on the current set, where the context names
    `location.levels@2`.
  - **Excluded episodes.** Episodes before the regime warms up
    ("unlabelled") or while the registry is not ready ("unlocated") are
    counted and reported, but belong to no cell.
  - **Where 97 comes from.** It is the smallest n with a 95 % half-width of
    at most 10 percentage points on a proportion near 0.5: 1.96² · 0.25 /
    0.10² = 96.04. The half-width must hold in the cells #28 estimates,
    which are regime × location, not regime alone. ±10 pp is the coarsest
    contrast worth testing against the unconditional baseline; a wider
    half-width cannot tell a conditional rate from the baseline at the
    effect sizes #28 looks for, a narrower one multiplies the episodes
    needed by its inverse square. That 10 pp is a judgment.
  - **Minimum-cell rule (MC), fixed now and applied on the first half
    before `q_hi` is read.** A cell is *servable* if its count reaches 97
    at one or more grid values. Every grid value is read because the count
    need not fall monotonically as `q` rises: a shrinking active set can
    split one run into two rising edges.
    - A servable cell is a floor cell.
    - A non-servable `value_edge` or `structural_extreme` cell leaves the
      floor, and its bin's regime-pooled cell (all four labels) becomes a
      floor cell instead. *Why pool labels, not bins:* pooling bins erases
      the location factor under study, while pooling labels keeps the
      location-versus-baseline contrast and loses only its interaction
      with regime.
    - If the pooled cell is also non-servable, the bin is *unpowered* for
      that window length. It leaves the floor, the ADR names it, and #28
      reports it with its actual interval and no ±10 pp claim. No `q_hi`
      can change that, so it must not block the choice.
    - A non-servable `neither` cell stops R1. `neither` holds most
      episodes, so if it cannot carry the regime split, that window length
      cannot support the one split every episode has. That is a finding
      about #22's definition, not a rare cell.
    - The floor cells fixed on the first half are frozen; the second half
      checks them as they are.
  - **The grid.** The steps roughly halve the tail: 20, 15, 10, 5, 2.5 and
    1 %. A finer search would fit the number to the sample, the argument
    ADR-044 decision 9 made against moving a value only to reach a target.
  - **If only 0.80 passes.** R1 passes with `q_hi` = 0.80, and the ADR
    records that the floor binds at the bottom of the grid. There is no
    head-room left: #28 cannot split those cells further (by side or by
    auction state) at ±10 pp. The grid is not extended below 0.80 after
    seeing data, because that is the re-pick this rule forbids.
  - **Stop rules.** Stop and report on #22, with the per-cell table, if no
    grid value passes every floor cell on the first half, if a `neither`
    cell is non-servable, or if the second half misses a floor cell at the
    chosen `q_hi`. No other value, bin, `n_min` or window set is picked
    silently. The remedy (coarser bins, a wider half-width for #28, more
    data) is a human decision, recorded as a revision of this ADR before
    M3 fixes any constant.
- **Warm-up.** `Samples(B)`, where a sample is an evaluation.
- **Parameters.** `baseline_evaluations` = `B`, `effort_quantile` =
  `q_hi` (a `Rate`), `result_rule` = `progress_at_most_move_median`,
  `window_ms`.

### D6. Exhaustion

`candidate.exhaustion@1` (M3). Inputs: `candidate.effort_result.1h@1`,
`candidate.effort_result.5m@1` and `bars.time.1m@1`, so recalibrating
effort vs result re-versions exhaustion through its input versions. At the
evaluation ending at `t`, all three must hold:

- **Expansion.** The 1h evaluation ending at `t − 5 min` has `effort ≥
  effort_high` and `progress ≥ move_high`. It is looked up by its end; if
  it is missing, there is no evaluation.
- **Decay.** The 5m evaluation ending at `t` has `delta_5m ×
  sign(delta_1h) ≤ effort_median(5m)`: aggression in the expansion's
  direction is back to no more than typical, or has reversed.
- **At the extreme.** The last close is within 6 bps of the extreme of the
  65 closed minutes `(t − 65 min, t]` in the expansion's direction:
  `|extreme − close| · 10 000 ≤ |extreme| · 6`, exact.

Why each number:

- **The 5-minute offset** is the decay window's length. With it, expansion
  and decay cover disjoint minutes; otherwise the 1h effort would include
  the decaying minutes it is compared against.
- **65** is 60 + 5, derived from the two windows.
- **6 bps** means "within one ordinary minute's move of the extreme". The
  measured first-half p75 of the 1m close-to-close move is 5.93 bps
  (ADR-044 M1); that run is the measurement, the reason is this one. A
  smaller value demands that the close be the extreme print itself; a
  larger one accepts a pullback bigger than an ordinary minute, which is
  the start of a retracement rather than "at the extreme".
- **The 1h/5m pair.** 1h is the longest window and 5m the finest
  (ADR-035). A 15m decay probe would need a quarter of the hour to decay
  before it reports.

Emission is on the rising edge. Warm-up is `Samples(6)` 1h evaluations:
the expansion is read at `t − 5 min` and evaluations come one per minute,
so the first `t` with an expansion to read is the sixth 1h evaluation (the
one at `t − 5 min` and the five after it). Parameters: `decay_window_ms` = 300 000, `expansion_window_ms` =
3 600 000, `extreme_tolerance` = 0.0006.

### D7. Absorption

`candidate.absorption.<5m|15m|1h>@1` (M3), live only. Inputs:
`candidate.effort_result.<tf>@1` and `book.liquidity.window.<tf>@1`, which
run on the same clock (ADR-043 D6).

- **Condition.** The effort-vs-result condition holds for the window, and
  on the passive side opposite the aggressor (Buy aggression means Ask,
  Sell means Bid), inside the 5 bps band, `added ≥ filled`. The "level" of
  "aggression into a level" is the price where the fills rest and refill,
  an order-flow fact; registry-level gating is #24's.
- **Why `added ≥ filled`.** "Replenished" means at least as much resting
  size came back as aggression took. Any other ratio would be a tuned
  number. ADR-043 D5 books inferred fills into both `added` and `filled`,
  so `added − filled` does not depend on the inferred share; a test pins
  this.
- **Why 5 bps.** ADR-043 D8 puts its cluster candidates, the passive
  walls, inside 5 bps of mid by construction, and its *Accept when* holds
  that band to ≥ 90 % availability. The 1 bps band holds only 42–68 levels
  per side (ADR-043 M3), so it misses refills just behind the touch.
- **No evaluation** when the band's `beyond_window` is set (part of the
  flow was not observed), or when the book window is warming up or
  unavailable. Archive replays therefore keep it warming up.
- Emission is on the rising edge. Warm-up is `Samples(1)`: a minute with
  both inputs ready. Parameters: `band` = 0.0005, `replenish_rule` =
  `added_at_least_filled`.

### D8. Where each number comes from

| Number | Reason | Kind |
|---|---|---|
| Divergence timeframes 15m, 1h, 4h, 1d | Swings exist only on the structure timeframes (ADR-037 D1) | derivation |
| Effort windows 5m, 15m, 1h | The aggression windows ADR-035 built for this judgment; a new horizon is a new id | inherited input, own reason |
| OI point age at an anchor ≤ 360 000 ms | One grid interval plus the 60 s lag `derivatives.oi.5m@1` accepts (ADR-042 D2); less loses healthy live anchors, more reads an outage's stale point | derivation |
| Ring of 4 closed-bar records | `SWING_BARS + 1`: the swing bar is exactly `SWING_BARS` bars before the confirming one (ADR-031, ADR-037 D3) | derivation |
| R1b before R1 | `q_hi` names a tail only under a fixed `B` | derivation |
| `B` ∈ {60, 240, 1440} | The 1h, 4h and 1d bar spans in one-minute evaluations; 1440 is the shortest baseline holding every UTC hour | derivation; **value fixed by R1b (M3)** |
| `B_ref` 10 080 | One week: every hour 7×, every weekday once; the longest baseline in the grid | derivation |
| R1b's 24 UTC hours | The daily activity cycle's resolution; M2 counts by them | judgment |
| R1b's statistic: Pearson X² of the 2 × 24 table (`B`, reference) × hour | The count-weighted test of "the same hour profile, up to scale"; the hour's level cancels in the split, and each hour weighs by its count (M2's coefficient of variation did not, D9) | derivation |
| 23 degrees of freedom | (2 − 1) baselines × (24 − 1) hours | derivation |
| Critical value χ²₂₃(1 − 0.05/k): 35.172 at k = 1, 46.258 at k = 18 | The χ²₂₃ quantile at the Bonferroni-split family level | derivation |
| Evaluability: no expected count below 1, at most 9 of 48 below 5 | Cochran's rule (1954): where X²'s χ² tail can be trusted; 9 is the largest count ≤ 20 % of 48. Looser lets the tail error flip a verdict at α/k; stricter (all ≥ 5) carves out M2's failing combination and stops R1b for want of data | published rule + what breaks |
| Pearson's X² rather than G | X² follows χ² more closely at small expected counts (Larntz 1978; Koehler and Larntz 1980); G gives p 0.00087 where X² gives 0.0097 on a #12-year table, either side of α/k | published comparison |
| 5 % family level, k ≤ 18 | Bonferroni over at most 3 windows × 6 `q` evaluable combinations, so a `B` that ranks like the reference fails anywhere in a half at most 5 % of the time; the same level as `n_min`'s 1.96, so both rules share one error rate. 1 % lets a distorting `B` pass more often; 10 % stops R1b for nothing one time in ten | derivation + shared level |
| `q_hi` grid {0.80 … 0.99} | Tail roughly halved per step; finer would fit the sample | judgment + derivation; **value fixed by R1 (M3)** |
| `n_min` 97 | 1.96² · 0.25 / 0.10² = 96.04 in the regime × location cells #28 estimates | derivation from the 10 pp judgment |
| 10 pp half-width | The coarsest contrast worth testing against the baseline | judgment |
| Three location bins | The two location factors #28 names plus the remainder | derivation from #28's scope |
| Minimum-cell rule | Pool labels (keeps the location contrast), else unpowered; `neither` stops | derivation |
| The median for "little progress" | The parameter-free "less than a typical window" | derivation |
| 5 min offset, 65 min extreme window | Disjoint expansion and decay; 60 + 5 | derivation |
| 6 bps at the extreme | One ordinary minute's move (ADR-044 M1 first-half p75 5.93 bps) | measurement + own reason |
| 5 bps absorption band | ADR-043 D8's cluster band, ≥ 90 % available; 1 bps holds only 42–68 levels | inherited band, own reason |
| Replenishment `added ≥ filled` | "At least as much came back"; any other ratio is tuned | derivation |
| Halves split 2025-04-01 12:00 UTC | The measurement year's midpoint: two equal halves of the 365-day window | derivation |
| Measurement year 2024-10-01 … 2025-09-30 | The most recent year no run, fixture or constant had read (attested on #124), with the development year's 365 days, so the per-half counts the evaluability rule and R1's floor of 97 depend on are at the scale those rules were written for | derivation, with an attestation |
| 730 expected files | 365 days × 2 streams (`aggTrades`, `metrics`) | derivation |

### D9. The R1b revision and the hold-out year (#124)

1. **The M2 result is a valid STOP.** The M2 run of the measurement tool
   on the #12 year (2025-10-01 … 2026-09-30, [result on #22](https://github.com/Replikanti/mie/issues/22#issuecomment-6132263629))
   reproduced the engine with 0 mismatches (R0) and stopped R1b by its own
   rule: `B` = 1440 failed on the second half. Under the statistic M2 ran
   (the coefficient of variation `U_B` of the 24 ratios `n_B(h)/n_ref(h)`
   against `c_k · σ0`):

   | B | first half | second half |
   |---|---|---|
   | 60 | FAIL | FAIL |
   | 240 | FAIL | FAIL |
   | 1440 | PASS (k = 13) | FAIL (k = 14) |

   `B` = 1440's second half failed on one combination, 1h at q = 0.950:
   `U_B` = 1.0472 against `c_k · σ0` = 1.4046 × 0.5374 = 0.755. Its hour
   tables, UTC hours 0–23:

   - `n_B`: 5, 19, 8, 4, 13, 6, 12, 9, 9, 7, 5, 21, 6, 23, 29, 30, 39, 13,
     13, 16, 8, 12, 8, 12 (327 episodes);
   - `n_ref`: 3, 14, 8, 3, 3, 6, 12, 1, 4, 8, 5, 24, 8, 20, 24, 27, 19, 11,
     12, 11, 11, 8, 13, 2 (257 episodes).

   The failure is not a 13–16 UTC shift. Split by hour, the squared
   deviations of the ratios from their mean of 1.86 (which make `U_B²`)
   come from:

   - UTC 7, 9 against 1 (ratio 9.0): 59 %;
   - UTC 23, 12 against 2 (ratio 6.0): 20 %;
   - UTC 4, 13 against 3 (ratio 4.33): 7 %;
   - UTC 13–16, 23, 29, 30 and 39 against 20, 24, 27 and 19 (ratios 1.15,
     1.21, 1.11 and 2.05): 1.7 % together.

   The busy hours are busy for both baselines, so their ratio stays near
   the mean. The coefficient of variation gave every hour the same weight
   whatever its count (hour 7 holds 10 episodes, hour 16 holds 58), and a
   ratio of Poisson counts with a denominator of 1–3 is heavy-tailed, so
   `σ0`'s χ² scaling did not hold there: one sparse hour decided the
   verdict.
2. **No carve-out.** The failing combination stays in the test. Under the
   revised statistic (D5) it is evaluable: 6 of its 48 expected counts are
   below 5 and none is below 1.
3. **Where the revision came from.** The revised statistic was chosen on
   the #12 year only, after that year's result was seen. That year is the
   development year from here on and carries no evidential weight for the
   revision.
4. **Attestation.** No run, fixture or constant of #22 read 2024-10-01 …
   2025-09-30 before this revision; the check is posted on #124. The
   repository's only market data from before 2025-10 is the ATR reference
   fixture, which covers 2024-07-01 … 2024-08-31.
5. **The measurement year is 2024-10-01 … 2025-09-30.**
   - *Why this year:* no run, fixture or constant has read it. It has the
     same 365 days as the development year, so the per-half counts that
     the evaluability rule and R1's floor of 97 depend on are at the scale
     those rules were written for. It is also the most recent untouched
     year, the closest to the market the constants will run on.
   - *What breaks otherwise:* a shorter window makes more combinations
     non-evaluable and more R1 cells non-servable. An older year is just as
     untouched but further from the live market. Two years double the
     import and the replay and mix more regimes.
6. **Data.** `aggTrades` and `metrics` only, imported with
   `mie archive-import` (ADR-034). No `trades`, so the fold (ADR-050, D8
   default) leaves every day unchecked and delivers the aggregates as
   published.
   - *Why no `trades`:* R1b compares two baselines on one tape. The
     omission the fold repairs was measured on the #12 year at no more than
     478 uncovered trades a day (ADR-047), against about 1.65 M aggregates a
     day (603 218 226 rows over 365 days, `docs/data-availability.md`),
     which cannot move that comparison. R1's counts move only through the
     rare rising edge such a trade flips. A year of `trades` costs about
     23–29 GB stored (ADR-050's 16–20 GB per 252 days, scaled to 365) and
     hours of download.
   - The fold report's unchecked line is recorded with the result.
7. **Pre-measurement stop rules.** Any one of these stops the process and
   is reported on #124 and #22. The window is never shifted, shortened or
   filled.
   - a missing `aggTrades` or `metrics` daily file in the coverage probe
     (730 files expected: 365 days × 2 streams);
   - a file the importer rejects;
   - a trade-id break in `aggTrades` reported by `mie archive-verify`;
   - any other `mie archive-verify` failure over the window: a normalize
     error, a hash mismatch on read, a row count or ledger that disagrees
     with the store, or a file not imported — `archive-verify` must print
     PASS;
   - an error of the measurement run's archive replay caused by such data
     (a file that does not read, a record that does not normalize). The
     run panics on it before its report is printed, so no R0, R1b or R1
     outcome exists yet.

   Holes over 60 s without an id break are recorded, not stops: consecutive
   ids mean nothing is missing. *Why a human may remedy these stops:* they
   happen before any episode is counted, so a remedy cannot tune on R1b or
   R1 outcomes.
8. **One run.** R0, R1b and R1 run once on the hold-out year, at a recorded
   commit.
   - A re-run is allowed only after an infrastructure failure (the process
     killed, out of memory, out of disk). It uses the same commit and the
     same store, and the failed attempt is reported.
   - A result is never re-run to change it.
9. **Verdict.**
   - *Pass:* R0 with 0 mismatches and 0 rejections, R1b fixes `B`, and R1
     fixes `q_hi`. #22 M3 then uses those two values.
   - *Anything else:* M3 stays blocked, and the result is recorded here and
     on #22. Neither year is used to re-tune. A remedy is a new human
     decision, and it would need data that neither year can provide.
   - *What a PASS of R1b means:* the test is conservative toward PASS near
     the reference, because `B`'s and the reference's hour tables come from
     the same tape and are positively correlated while X² assumes
     independent samples (D5, Approximations). On the development year
     `B` = 1440 gives X² of 8.6–27.0, means 16.8 and 14.9 per half, well
     below the 23 a χ²₂₃ variable averages. A hold-out PASS of `B` is
     therefore weaker evidence than its nominal p. The selection of `B` is
     unaffected: `B` = 60 and 240 fail by X² 56.1–501.9 against critical
     values of at most 45.4.
10. **The use of the development-year re-run.** The revised statistic is a
    pure function of the per-hour tables M2 printed, so it was re-run on
    them without a replay (the tool's `r1b_from_a_logged_run` on the M2
    log). Diagnosis only, not a verdict:

    | B | half | evaluable (k) | X² of the evaluable combinations | critical | diagnosis |
    |---|---|---|---|---|---|
    | 60 | first | 14 | 106.2 – 465.0, all fail | 45.378 | fails |
    | 60 | second | 14 | 119.3 – 501.9, all fail | 45.378 | fails |
    | 240 | first | 14 | 68.4 – 341.9, all fail | 45.378 | fails |
    | 240 | second | 14 | 56.1 – 373.5, all fail | 45.378 | fails |
    | 1440 | first | 12 | 9.8 – 23.2, all pass | 44.833 | passes |
    | 1440 | second | 14 | 8.6 – 27.0, all pass | 45.378 | passes |

    The formerly failing combination (1440, second half, 1h, q = 0.950)
    gives X² = 27.0, p = 0.256. `B` = 1440's median p is 0.84 on the first
    half and 0.92 on the second.

    The re-run **may** be used for:
    1. checking that the implementation computes what this ADR says,
       through unit-test pins on three real tables;
    2. showing that the failing combination is still tested, not carved
       out;
    3. showing that the test still has power against hour-ranking
       baselines (60, 240);
    4. this diagnosis table, labelled as diagnosis.

    It **may not** be used for:
    1. fixing `B` or `q_hi`;
    2. meeting any *Accept when* item;
    3. claiming that R1b passes;
    4. choosing or adjusting any constant after the hold-out year is
       touched (the evaluability rule, the family level, the `B` set, the
       grid, `n_min`, the bins, the halves).

    R1 is not evaluated as a fixing rule on the development year. The
    revised rule was chosen after the #12-year result was seen, so that
    year carries no evidential weight for it. The hold-out run decides,
    once.

## Consequences

- `MarketState` grows by one field (`candidates`); every state hash and
  the feature-set version change, and old and new hashes are incomparable
  (ADR-041 D2). The pre-#22 encoding is pinned by a test.
- Divergences are known only when the later swing is confirmed: `N` bars
  late, as the swings themselves (ADR-037). 1d divergences need at least
  two anchored daily swings of one side, so weeks of replay.
- The per-trade cost is up to 8 comparisons (high and low on four
  timeframes) and, on a new extreme, one small record copy. Comparisons
  happen only when a swing is confirmed.
- The OI comparison reads a grid point at most 360 000 ms old at the
  anchor (D4); archive points are up to one 5-minute interval old, live
  ones are fresher. `resolution_ms` on each point tells them apart. An
  open-interest outage longer than that leaves the anchors inside it
  without a point, so no OI comparison crosses it.
- In archive replays the location context lists no cluster levels (ADR-043
  is live only), so contexts from archive and live differ by those levels.
- A divergence across a trades gap carries `feed_gap`: the CVD change
  misses whatever traded inside the gap.
- Effort vs result, exhaustion and absorption wait for the hold-out run
  (D9); until M3 the engine computes only the divergences. If that run
  stops, M3 stays blocked until a new human decision.
- R1b's test is lenient near the reference: both baselines read the same
  tape, so their hour tables are positively correlated and X² is
  conservative toward PASS (D5, D9). A mild hour distortion of `B` = 1440
  could pass; a baseline that ranks by hour fails with a wide margin.
- The hold-out year lives in its own raw store, with its own import ledger
  and staging. The development year's replays and their dataset versions
  are untouched, and the hold-out replay reads no development-year
  lookahead day (ADR-050 D6). That store lacks the 2025-09-30 23:55
  open-interest sample, which Binance files in the 2025-10-01 `metrics`
  file: the year's last 5-minute open-interest point is missing. It
  touches no R1b or R1 count, and it is recorded.
- Without a magnitude threshold, the divergence event rate is set by the
  swing rate and the share of pairs that diverge, which M2 records per
  timeframe on the archive year. #25 owns episode de-duplication.

## Alternatives considered

- **A magnitude floor on divergences** (a minimum price or CVD change).
  A number nobody measured; it would force a `@2` once #28/#31 measure the
  useful one. Rejected for `@1`.
- **The CVD at the swing bar's close as the anchor reading.** Mixes in the
  aggression after the extreme. Rejected.
- **The CVD's own intrabar extreme.** Needs per-bar CVD high/low state and
  belongs to no single price print. Rejected.
- **A signed OI rule** (mirroring the CVD rule). Open interest counts both
  sides' positions; a falling OI at a new low is the same non-confirmation
  as at a new high. Rejected.
- **Location as a feature input of the candidates.** Breaks the
  `location.levels@1` selection (D3). Rejected; the context records it.
- **Gating candidates on location** (only at VAH/VAL or registry levels).
  A trigger-rule condition (#24, ADR-012); it would hide the candidates
  #28 needs as its baseline. Rejected.
- **An independent tuned threshold per detector.** About ten unmeasured
  numbers. Rejected (Context).
- **A joint (`B`, `q`) search.** Lets the episode floor choose `B`, and
  fits two numbers to one half (D5). Rejected.
- **Raw per-hour rates or max/min for R1b.** Select the baseline R1b
  exists to reject, or have no simple null (D5). Rejected.
- **The coefficient of variation of the ratios `n_B(h)/n_ref(h)`** (M2's
  statistic). Weighs every hour alike whatever its count, and a ratio with
  a denominator of 1–3 is heavy-tailed; one sparse hour decided M2's
  verdict (D9). Rejected.
- **The coefficient of variation with a higher zero-floor.** Any floor that
  drops hours with `n_ref(h)` ≤ 3 drops exactly M2's failing combination:
  a carve-out by another name. Rejected.
- **The likelihood-ratio statistic G.** Follows χ² less closely at small
  expected counts; on a #12-year table it puts p on the other side of α/k
  than X² (D5). Rejected.
- **An exact conditional (Monte Carlo) test.** Valid at any count, but it
  needs a seeded sampler and a draw count, and its Monte Carlo error sits
  right at α/k. The evaluability rule makes the χ² tail adequate wherever
  the test is used. Rejected.
- **A time-of-day-aware baseline** (ranking each window against the same
  hour of earlier days). `B` exists so that a rank does not depend on which
  hours filled the baseline; a per-hour baseline makes raw rates uniform
  across hours, the `B` = 60 behaviour R1b rejects, so it fails against
  the one-week reference by construction. M2 shows no hour-of-day
  distortion at 1440 that would need correcting (D9). Rejected.
- **A different `B` set.** The M2 failure is a property of the statistic,
  not of `B` (D9), so a new set would be chosen on an artifact. Rejected.
- **Carving out 1h at q ≥ 0.950.** It would choose the test by its result
  on the year being tested. Rejected.
- **Taking the #12-year re-run as the verdict.** The revised statistic was
  chosen after that year's result was seen, so the year carries no
  evidential weight for it (D9). Rejected; the hold-out year decides.
- **A cooldown or hysteresis on emission.** Parameters; the rising edge
  needs none, and episodes are #25's. Rejected for `@1`.

## Accept when

- **R0.** An independent measurement model written from this text
  reproduces the engine with 0 mismatches:
  - divergences, effort vs result and exhaustion over the full #12 year
    (2025-10-01 … 2026-09-30), including the location bin of every
    effort-vs-result episode (done for the divergences in M2), and over
    the hold-out year (2024-10-01 … 2025-09-30, D9);
  - absorption over every live raw store with depth available at run time
    (the #10 soak, the #13 runs).
- **R1b.** The `B` rule passes on both halves of the hold-out year.
- **R1.** The `q_hi` rule passes on both halves of the hold-out year, at
  R1b's `B`.
- **Recorded, not criteria:**
  - divergence shares per timeframe: consecutive same-side swing pairs,
    the share at a further extreme, the CVD-divergent share and the
    OI-divergent share;
  - R1b's per-hour episode counts and, for every `B`, window length and
    `q`, per half: X², p, the cells below 5 and below 1, the evaluability,
    k and the critical value;
  - R1's per-cell counts per half for every grid `q` at the chosen `B`,
    each cell marked floor, pooled or unpowered, plus the unlabelled and
    unlocated counts;
  - exhaustion and absorption rates;
  - the year's replay wall time, against ADR-037's 1 166.6 s;
  - for the hold-out run: the coverage table, the `archive-verify`
    summary, the fold report's unchecked line, the dataset version and the
    commit.
- No clause lets a mismatch be explained away.

Every criterion is reachable once the hold-out import completes, and
otherwise settled by the D9 stop rules; the live stores exist from the #10
soak on.
