# ADR-064: P1 replication on 2020-01 … 2024-09 — a pre-registered grid of 48 contexts, per-label fees, a rotation null and Benjamini–Hochberg

- Status: proposed
- Date: 2026-11-09

## Context

#28 (ADR-058, report `docs/research/028-order-flow-interactions.md`) read
its absorption leads on the #12 development year 2025-10-01 … 2026-09-30.
P1-sell read `hypothesis` at 4h, and every 1d cell read `insufficient`:
one year holds at most 365 disjoint 1d windows, below n_min = 385
(ADR-058 D6). 1d is the horizon the owner's inputs point to: the taker
round trip is 2.1 / 1.1 / 0.53 R at 5m / 15m / 1h and 0.26 / 0.09 R at
4h / 1d (`docs/direction.md`, the #12 medians), so only at 4h and 1d does
μ = 0.10 R fit next to the fee. A longer window is the only way to read
1d, and the archive's `aggTrades` history from 2019-12-31
(`docs/data-availability.md`) gives 2020-01-01 … 2024-09-30, the days
before the calibration slice.

#205 asks for the pre-registration of that replication: everything that
must be fixed and merged before any outcome of the window is read. #206
reads the outcomes. The split exists because the pre-registration must be
on main before the read, so one PR cannot carry both. Four things have to
be decided here:

- **Which data, and how the tool proves it read only that.** ADR-058 D1
  guards one year by its provenance and published counts. The window has
  no published counts yet, and the calibration slice
  (2024-10-01 … 2024-12-31, `c20761ef…`) and the hold-out
  (2024-10-01 … 2025-09-30) must stay provably unread.
- **What exactly is tested.** #28 read 100 primary and 995 split cells
  with a chance count instead of a correction (ADR-058 D7). A replication
  that reads many cells again and picks the best repeats that problem.
  So the grid is fixed and small, and every context in it is decided with
  the false discovery rate of `docs/direction.md` T3.
- **Net, not gross.** #28 printed the fee per horizon at the median
  barrier width. A strategy pays the fee of each trade at that trade's
  own barrier width, and T2 asks for a net expectancy.
- **A null that respects the samples' dependence.** A held condition
  yields a member every minute, and outcomes are autocorrelated. T3 asks
  for a permutation null within time blocks; how labels move inside a
  block decides whether the null is valid.

The inputs are fixed by `docs/direction.md` and its addendum of
2026-11-08, written before this replication was designed or read:
confidence 0.95, μ = 0.10 R, Binance USDⓈ-M VIP 0 taker on both sides
(10 bps round trip), false discovery rate 0.05, maximum out-of-sample
drawdown 30 R, minimum frequency 2 per week for A and 1 per week for B.

## Decision

### Approach

- **Reuse, do not re-implement.** The replication is a module of the
  ADR-058 tool (`crates/mie-cli/tests/order_flow_study/replication.rs`)
  and calls its population, episodes, unit count, strata, effect E and
  day bootstrap unchanged. The replication is like-for-like only if the
  code that counted units and computed E in #28 does it again. The
  ADR-058 path stays byte-identical, pinned by a golden hash of its
  fixture report.
- **Queries** are `docs/research/205-p1-replication.sql`, views only, run
  after `028-order-flow-interactions.sql` on the same connection, which
  reaches no file but the dataset's four tables (ADR-058 *Approach*).
- **Two ignored entries.** `coverage_p1_replication` (#205) reads no
  outcome. `measure_p1_replication` (#206) reads them, and refuses to run
  until this ADR's pre-registration record pins the dataset.
- **Order.** This ADR and the code are committed before any window data
  exists. Only then is the archive imported, the dataset built and the
  coverage run, so the only program this PR runs on the window cannot read
  an outcome (D5).

### D1. Data and guard

- **Window.** Records = replay = `1577836800000..1727740800000` ms:
  2020-01-01 … 2024-09-30, 1 735 days, 247.857 weeks.
  - The start is the first whole UTC day of the `aggTrades` archive,
    whose history starts 2019-12-31.
  - The end is the day before the calibration slice (`docs/direction.md`).
  - Records = replay, as in #28: the warm-up costs the regime's 214 h and
    the 1d barrier's 15 days at the start. Replaying from 2019-12-31 would
    give the effort features at most one more day, and the regime needs
    8.9 days anyway. Rejected.
- **Streams.** `aggTrades` only, `--trade-correction fold`.
  - No grid input reads open interest or funding: P1 is ADR-051 D5
    `active` (effort against result, from `aggTrades`), the regime is the
    ATR percentile of 1h bars from `aggTrades` (ADR-033), and the labels
    come from trades. The `metrics` archive starts 2021-12-01, so adding it
    would split the window into two stream sets.
  - No `trades` files are imported for the window, so the fold is
    unchecked on every day, as on the hold-out year
    (`docs/data-availability.md`).
  - The dataset version therefore differs from #28's
    (`aggTrades,metrics`). OI and funding features and the OI triggers stay
    warming; no grid input reads them.
- **Store.** The window is imported into its own archive store (its own
  `raw_root`), as the hold-out has its own. The build cannot reach a
  hold-out or #12-year file.
- **Guard of both entries**, before any output:
  - `conditional::verify` passes;
  - `manifest.replay` and `manifest.records` both equal the window, and
    neither ends after the calibration start `1727740800000`;
  - the `source` line is `archive binance-archive/BTCUSDT streams
    aggTrades correction fold`;
  - engine `4c844593779da2e3-1`, `conditional.sampling@1
    2c71fff23e8b0415` and `conditional.labels@2 6f90bff079e746f2`,
    ADR-058 D1's, so features, sampling and labels are #28's;
  - `code` is a clean `git:` line: 40 lowercase hex digits, no `-dirty`;
  - `content` is not the calibration slice's
    `c20761ef7e96acd21e3f6dae04cb9f2f20efb34c058dda1e775264e608f892ac`.
- **Guard of the measurement only**, also before any output:
  - the pins of the pre-registration record: `data`, `content`, the
    contexts / baseline / trigger record counts, 3 effort rows per
    context, and per horizon the count of races that ran at 4h and 1d. The
    tool holds them as constants that are empty until the record is
    written, and an empty pin stops the measurement before it opens a
    dataset;
  - the SHA-256 of both query files and of this ADR's frozen text (D6);
  - the record set of the coverage chain's P1 rows equals the P1 rows of
    `study_members`;
  - the ADR-058 twin on the P1 rows: `study_baseline` and the P1 rows of
    `study_members` line by line, and the 48 contexts' units, with 0
    mismatches.
- **No `neither` count is pinned,** unlike ADR-058 D1: pinning it would
  need the races read, which coverage does not do.

### D2. Contexts (48)

- **Grid.** P1-buy and P1-sell exactly as ADR-058 D3, through the
  unchanged `study_members` view: ADR-051 D5 `active` on a covered effort
  window. Crossed with:
  - the effort window 5m / 15m / 1h;
  - the regime at decision, low / medium / high / extreme (ADR-033 bands);
    a member whose regime is `not_ready` is in no context, and is counted;
  - the horizons 4h and 1d.

  2 × 3 × 4 × 2 = **48 contexts**, in that fixed order (side, window,
  regime, horizon).
- **Episodes** are formed per side × window over the whole window, before
  the regime split (ADR-058 D6 and D8): an episode whose opener lies in
  another regime is kept in this one.
- **Why only 4h and 1d.** At 5m / 15m / 1h the taker round trip is 2.1 /
  1.1 / 0.53 R (*Context*), several times μ, and the largest primary |E|
  #28 read at any horizon was 0.1055 (ADR-058 *Acceptance*). 1d is the
  target. 4h is where P1-sell read `hypothesis` in #28, the lead being
  replicated.
- **Why per regime.** ADR-058 D4 standardizes by clock and location, not
  by regime, and its regime splits showed the confound. A context per
  regime compares each member with the baseline of its own regime, so the
  regime does not enter E.

### D3. Statistics

- **E.** E = T − T_b exactly as ADR-058 D4: 840 strata (New York hour of
  week × location-bin set), the baseline restricted to the context's
  regime, over every member whose race ran; a member whose stratum holds
  no baseline race of that regime is dropped and counted.
- **The race outcome x** of a member at H is +1 / −1 / 0 for hypothesis
  / counter / neither, ADR-058 D4's. E and T3 use x unchanged, so E stays
  comparable with #28.
- **The net outcome of a trade held to H**, which T2 and the information
  figures of D4 use, is s·g − f:
  - g is the gross outcome in R toward the hypothesis barrier. It is +1 /
    −1 when the hypothesis / counter barrier came first: the barriers are
    symmetric (ADR-056 D4′), so a barrier hit is ±1 R.
  - For `neither`, g is the mark to market at H, σ·m. Here m = (last −
    reference) / (upper − reference) is the window's last trade
    (`outcomes.last` at H) in half-widths toward the upper barrier, and σ
    is +1 for an upper hypothesis (P1-sell) and −1 for a lower one
    (P1-buy). A `neither` without a last trade is marked at the reference
    (m = 0).
  - *Why the mark and not x = 0:* a trade held to H without a barrier hit
    closes at H at the last price, so its result is the price move, not
    0 R. Scoring it 0 would make T2 blind to drift between the barriers: a
    context whose `neither` exits drift against s could pass T2 although
    its real expectancy per trade is at or below 0, and the reverse. T2 in
    `docs/direction.md` asks for the net expectancy of trades. ADR-058 D4's
    x is a race statistic, kept for E, where #28 defined it. (The #207
    review found the gap; amended before any outcome was read, see the
    record.)
  - f = 10 bps ÷ the member's own barrier half-width in bps,
    (upper − reference) / reference at H. The fee is per label, not the
    median of #28;
  - s = sign(E): +1 trades the hypothesis barrier, −1 the counter barrier.
    E = 0 or undefined gives no direction, and the context reads `no
    effect`.
- **T1.** `units` ≥ n_min = ⌈(z/μ)²⌉ = **385**, with z = Φ⁻¹(0.975) =
  1.959964 and μ = 0.10 R. The bound σ ≤ 1 on the gross race outcome holds
  for any distribution and needs no outcome. This restates ADR-058 D6's
  count and derivation; it changes neither.
- **T2.** The net expectancy over the units' representatives (ADR-058 D6
  stage 2: each episode's earliest member whose race ran, greedily thinned
  at H), in direction s.
  - *Why the representatives:* they are the trades of a strategy that
    holds one position at a time, and T1's n, the frequency and the
    drawdown are counted on the same sequence. Averaging over every
    member, as E does, would weight a long-held condition by its duration.
    A representative dropped from E (empty stratum) is still a trade and
    stays in T2.
  - Bootstrap: a circular block bootstrap of the UTC days that hold a
    study record (ADR-058 D5 `resample`), primary block **56 days**,
    sensitivity **28** and **112 days** (flag `block-sensitive` when either
    changes the T2 decision), **10 000** resamples, seed **205**, every
    block length from the same seed.
  - T2 passes when the nearest-rank (1 − c)/2 quantile of the resampled
    net expectancy is above 0. A resample that draws no unit is undefined,
    and more than (1 − c)/2 undefined resamples read `insufficient`
    (ADR-058 D5's rule).
- **T3, a permutation null within time blocks.**
  - **Blocks.** From 2020-01-01 00:00 UTC the whole weeks are cut into
    blocks of **8 weeks**: 30 blocks of 8 and one of 7 (247 weeks). The
    last 6 days (2024-09-25 … 09-30) are not a whole week and never
    rotate, which is conservative: they keep their observed labels in
    every draw.
  - **Draw.** One draw rotates each block's labels by k whole weeks, k
    uniform in 0 … L−1 and independent per block, circular within the
    block (L the block's length in weeks). The labels are the minute ends
    of the context's members.
  - **Pseudo-members** are the baseline records at the rotated minutes that
    are eligible for the context: regime r, the race at H ran, a stratum
    holding a baseline race. Each adds σ·(d − tilt(stratum)), with d = +1
    / −1 / 0 for upper / lower / neither, σ = +1 for an upper hypothesis
    (P1-sell) and −1 for a lower one (P1-buy), and the observed stratum
    tilts of D3 *E*. E* is their mean, so the identity rotation gives E
    itself (the tool asserts this to 1e-9).
  - **Count.** B = **1 000 000** draws, seed **205**. Draw i uses its own
    SplitMix64 stream, seeded with the i-th output of SplitMix64(205), and
    one draw's rotations serve all 48 contexts, so the result does not
    depend on how draws are split over threads.
  - **p-value.** Two-sided: p = min(1, 2·min(p⁺, p⁻)), with p⁺ = (1 +
    #{E* ≥ E})/(B + 1) and p⁻ = (1 + #{E* ≤ E})/(B + 1). A draw without
    an eligible pseudo-member counts toward both tails.
  - **Sensitivity.** Blocks of 4 weeks (61 of 4, one of 3, the same
    6-day tail); the flag `null-sensitive` is set when the BH decision
    differs from the 8-week one.
- **BH.** Benjamini–Hochberg at q = **0.05** over m = **48** (the
  addendum). A context below n_min, or without an E, enters with p = 1,
  so m never shrinks. Ties are broken in the fixed context order, which
  does not change which contexts are rejected. Reject p₍ₖ₎ ≤ k·q/48 up to
  the largest such k.

### D4. Reading and stop rules

- **Reading.** A context reads `replicated (hypothesis)` or `replicated
  (counter)`, by s, iff T1 ∧ T3 after BH ∧ T2 in direction s. Otherwise,
  the first that applies:
  - `not measurable`: units < 385;
  - `no effect`: not rejected by BH at 8-week blocks, or no direction;
  - `insufficient`: T2 has too many undefined resamples;
  - `not net positive`: T2's lower bound is at or below 0.
- **Information, not gates.** Printed for every measurable context; A and
  B are judged out of sample (#30, #33):
  - units per week, against A 2 and B 1;
  - net R per year = net expectancy × units per week × 52, the addendum's
    arithmetic (μ at 2 a week is 10.4 R a year);
  - the maximum drawdown of the cumulative net unit sequence in decision
    order, against 30 R;
  - ADR-058 D7's 56-day reading of E with ADR-058 D5's settings (10 000
    resamples, seed 28), for comparison with #28.
- **Stop rules.**
  - **S1.** The guard fails: nothing is printed. Fix the cause and rerun
    the same protocol.
  - **S2.** A context that is not measurable: #206 prints only its counts,
    no share, E, p or net figure.
  - **S3.** 0 replicated: P1 is recorded as not replicated at 4h / 1d on
    archive `aggTrades` 2020-01 … 2024-09, and no later re-run on this
    window with changed definitions counts as confirmatory.
  - **S4.** ≥ 1 replicated: the context is a RESEARCH candidate for the
    walk-forward on the hold-out (#30). Nothing is promoted, and the
    hold-out stays unread.

### D5. Coverage, without outcomes

- **Per context:** members whose race ran, dropped, episodes, units, units
  per week, measurable (units ≥ 385), and the median per-label fee in R
  over the units' representatives.
- **Per horizon:** baseline races that ran, the median baseline barrier
  half-width with the fee at it, and the disjoint-window ceiling (10 410
  at 4h, 1 735 at 1d).
- **Plus** the exclusion counts: per effort window the baseline records
  whose effort is not ready or incompletely covered, and per side × window
  the P1 rows whose regime is not ready.
- **Plus** the counts the measurement pins (D1) and the SHA-256 of both
  query files and of the frozen text.
- **Inputs.** Coverage reads only decision and minute-end times, effort
  flags, regime, location, barrier widths and `ran` = race ∈ {upper,
  lower, neither}.
  - `ran` is the input ADR-058 D6's gate already reads. It carries no
    direction and no neither against a barrier.
  - The marks of D3 (`rep_marks`) are outcomes and belong to the
    measurement chain only.
  - The coverage chain (`rep_ran`, `rep_coverage_*`) restates the P1
    condition from `study_records` and `study_effort`, and never selects
    from `study_members` or `study_baseline`, which carry outcomes.
  - Its Rust input types have no outcome field, so the compiler forbids an
    outcome read.
  - A coverage twin recounts every context's units from `contexts`,
    `effort` and its own projection of `ran`, and must show 0 mismatches
    before anything is printed.
- **Why coverage first.** It decides which contexts are measurable before
  any outcome exists in anyone's view, so S2 cannot be tuned. It also
  proves the import and build hold enough units before #206 spends the
  window.

### D6. Freeze

- **Frozen text.** Everything from `## Context` up to `## Pre-registration
  record` is frozen at the commit that adds this ADR. A test pins its
  SHA-256 and the SHA-256 of both query files, and the measurement checks
  the same hashes before it reads.
- **Frozen code.** The decision region of
  `crates/mie-cli/tests/order_flow_study/replication.rs`, between the
  markers `ADR-064 frozen decisions: begin` and `: end`, holds every
  constant and function that decides the replication. That covers the
  window and provenance, the grid, the fee, T2 with its marks, T3, BH, the
  reading, and the measurement that applies them with their settings. The
  same test pins the region's SHA-256 next to the texts' hashes. Both
  entries print it, and the measurement refuses to run when it differs.
  *Why:* hashing the texts alone left the numbers that decide the
  replication (q, the draws, the block lengths) editable after merge
  without any check failing (the #207 review). A region hash makes a
  change to any of them visible, in the tests and in the printed
  identity.
- **The run's commit.** #206 runs `measure_p1_replication` from the merge
  commit of #207 exactly, not from a later main. Its report shows the
  printed `replication.rs frozen decisions` SHA-256 equal to the pinned
  one. *Why:* the run also depends on code outside the region (the ADR-058
  path, the loaders, the twin), which only an exact commit freezes.
- **Changes after coverage** are allowed only to fix a review defect.
  Each is listed in the pre-registration record with its reason, and the
  coverage reruns if a count changes.
- **After merge** nothing in the frozen text changes. #206 adds only an
  `## Acceptance` section and the status line. This overrides the
  README's "a proposed ADR may be edited freely" for this ADR: a
  pre-registration edited after the coverage would no longer prove what
  was fixed before it.

### D7. Where each number comes from

| Number | Source | What breaks at another value |
|---|---|---|
| Window 2020-01-01 … 2024-09-30, 1 735 days | D1: the archive's first whole `aggTrades` day; the day before the calibration slice | Earlier: no archive data. Later: the calibration slice and the hold-out are read |
| 247.857 weeks | 1 735 ÷ 7 | — (arithmetic) |
| 48 contexts | D2: 2 sides × 3 windows × 4 regimes × 2 horizons | — (a count of what D2 names) |
| Horizons 4h and 1d only | D2: the taker round trip of 0.26 / 0.09 R at the #12 medians, against 0.53 R or more below 4h | Adding 5m–1h: 72 more contexts in m, each with a fee several times μ, which lowers BH's power for the contexts that can pass |
| n_min = 385 | D3: ⌈(1.959964 / 0.10)²⌉ = ⌈384.15⌉, T1 with σ ≤ 1 (ADR-058 D6) | Larger: fewer contexts measurable; smaller: contexts read that cannot resolve μ |
| 10 410 / 1 735 disjoint windows | D5: 1 735 days × 6 and × 1 | — (arithmetic; the units ceiling) |
| Fee 10 bps per label | `docs/direction.md`: the owner's tier today, VIP 0 taker on both sides, not the historical schedule, because the question is whether the edge is tradeable at the owner's cost | Lower: a pass the owner cannot trade; higher: edges rejected that the owner can trade |
| Fee per label, not per median | D3: each trade pays its own width; the half-width varies with ATR, so the median misstates the mean fee of a population concentrated in quiet or wild hours | — (the definition of a net outcome) |
| Bootstrap block 56 days | D3: ADR-056 measured the race-relevant volatility dependence at about 0.12 at 28 days and none at 56 (−0.06), on the #12 year; whole weeks keep the weekday cycle | Shorter: residual dependence left out, the interval too narrow; not whole weeks: some weekdays drawn more often |
| Blocks 28 and 112 days | D3: half and double the primary. 112 is added because one year cannot show a dependence longer than about a quarter year, and this window spans the 2021 and 2022 regimes | — (flags, not readings) |
| 10 000 T2 resamples | D3: the Monte Carlo error of the 2.5 % quantile's level, √(p(1 − p)/B) = ± 0.16 % at 10 000 against ± 0.49 % at 1 000 (ADR-058 D5's reasoning, for the same quantile) | 1 000: a 20 % relative error at the bound T2 reads |
| Null block 8 whole weeks | D3: whole weeks keep the UTC hour of week of every moved label (#155's weekly cycle, ADR-058 D4's strata); across a DST switch the New York hour moves by one, which the pseudo-member's own stratum tilt absorbs. The identity is 1 of L rotations per block, so under an effect the null keeps 1/L of the observed signal, a power cost of 50 / 25 / 12.5 % at L = 2 / 4 / 8. Longer blocks move labels across months of structural change that neither the regime restriction nor the strata hold | Shorter: more of the signal stays in the null (25 % at 4 weeks); longer: labels moved across structural change make the null wider than the noise |
| 4 weeks (null sensitivity) | D3: half the primary, still whole weeks | — (a flag) |
| Rotation, not a free shuffle | D3: a held condition yields a member every minute with overlapping windows. A free shuffle scatters members into independent minutes, so the null's variance is that of n members rather than of about `units`: too narrow, with false discoveries (tested on a synthetic null with clustered labels). A rotation keeps the runs and the outcomes' autocorrelation | — (the null's validity) |
| 8³⁰ · 7 ≈ 9 · 10²⁷ configurations | D3: 30 blocks of 8 and one of 7 | — (arithmetic: far more than B, so draws rarely repeat) |
| B = 10⁶ draws | D3: BH's strictest comparison is p ≤ q/48 = 1.04 · 10⁻³. There p = 2p⁺, so the Monte Carlo SD of p is √(2p/B) = 4.6 · 10⁻⁵ (4.4 % relative) at 10⁶, against 4.6 · 10⁻⁴ (44 %) at 10⁴. Per-block, per-rotation sums are precomputed, so a draw is one lookup per block and context: about 5 · 10⁹ lookups over both block lengths, seconds of CPU | 10⁴: a context at the BH boundary is decided by Monte Carlo noise |
| Two-sided, the smaller tail doubled | D3: T6 needs the counter-hypothesis testable, so a context can replicate in either direction. The rotation null is not symmetric about 0, because the identity share pulls it toward E, so a test on \|E\| would be invalid | One-sided: the counter direction untestable; \|E\|: wrong level |
| q = 0.05, m = 48 | `docs/direction.md` addendum 2026-11-08: its power simulation used this grid | — (the owner's input) |
| Seed 205 | D3: the issue number, named before the run | — (any fixed seed; a seed chosen after seeing results would be a choice) |
| Engine, sampling, labeling keys | D1: ADR-058 D1's, so features, sampling and labels are #28's | — (a difference is a different study) |
| A 2 / B 1 per week, 30 R | `docs/direction.md` addendum: information here, judged out of sample | — (not gates in this study) |
| 10.4 R a year at μ and 2 a week | 0.10 × 2 × 52, the addendum's arithmetic | — (arithmetic) |

There is no other number.

### Out of scope

- **Slippage.** It depends on the position size, which ADR-061 D8 keeps
  private, and at 1d it is small against the half-width. The walk-forward
  (#30) nets it.
- **Funding.** It needs the settlements inside each hold and a stream this
  dataset does not carry. The walk-forward (#30) nets it.
- So a T2 pass here is necessary for a tradeable edge, not sufficient.
- P2, P3, P4 and P1-trig; the 5m–1h horizons; the location, side and CVD
  splits.
- The walk-forward and the hold-out (#30, #33); the Research Lab's
  adoption of T3 and BH (#32), which may adopt or replace this code.
- Any change to the ADR-058 path, the `028` queries, n_min, P1, the
  regime or the labeling; the history chain (ADR-052); `trades` and
  `klines` imports; the waiting period for C.

## Consequences

- The replication is like-for-like with #28 in its population, units,
  strata and E, because the same code computes them. It differs in the
  stream set (`aggTrades` only) and therefore in the dataset version; no
  grid input reads the missing stream.
- The window holds at most 10 410 disjoint 4h and 1 735 disjoint 1d
  windows per context, so 1d can reach n_min where one year could not.
  Whether each context does is decided by coverage, before any outcome is
  read.
- Every T2 pass is gross of slippage and funding. A replicated context is
  a candidate for the walk-forward, not a strategy.
- The 8-week rotation keeps 1/8 of a real effect in the null, which costs
  power; the 4-week sensitivity shows whether a decision depends on it.
- T3 is two-sided. If the addendum's power simulation was one-sided, the
  power of the replication is somewhat lower than it states; this ADR
  does not re-simulate.
- `ran` leaks whether a barrier was hit before a censor on censored
  windows. It carries no direction, and it is the input ADR-058 D6's gate
  already reads.
- The measurement holds the P1 rows and the baseline of 4.75 years with
  the 4h and 1d races only. The #28 run peaked at 1.7 GB for one year;
  the main session caps the runs and records the peak.
- A frozen ADR cannot absorb a later idea. A better definition becomes a
  new pre-registration on new data, never a re-run here (S3).

## Alternatives considered

- **A separate study binary or CLI subcommand.** It would re-implement the
  ADR-058 D4–D6 statistic that #28 tested and accepted, and the
  replication is only like-for-like if the same code counts units and
  computes E. Rejected.
- **Re-reading all 100 primary cells of #28.** m = 100 halves BH's
  threshold for the contexts that can pass, and most cells have a fee far
  above μ. Rejected for the 48-context grid.
- **Streams `aggTrades,metrics` as in #28.** The `metrics` archive starts
  2021-12-01, so the window would split into two stream sets, and no grid
  input reads open interest. Rejected.
- **The median fee of #28.** A strategy pays each trade's fee at that
  trade's width. Rejected.
- **T2 over every member.** It weights long-held conditions by their
  duration and is not the trade sequence T1 and the frequency count.
  Rejected.
- **A free shuffle of labels.** Too narrow a null for clustered members
  (D7, tested). Rejected.
- **A circular shift by minutes instead of whole weeks.** It would move
  labels to other hours of the week, which the strata do not fully hold,
  and break the weekly cycle #155 measured. Rejected.
- **A null on |E|.** The rotation null is not symmetric about 0 (D7).
  Rejected.
- **Bonferroni over 48.** BH controls the false discovery rate the owner
  set; Bonferroni controls the family-wise rate, which the owner did not
  ask for, at a large cost in power. Rejected.
- **T2 with `neither` at 0 R** (ADR-058 D4's x). It ignores the price
  move of a trade that ends between the barriers (D3). Rejected for T2;
  kept for E.
- **Freezing only the texts, with #206 run from any later main.** The
  numbers in the code could change without a failing check (D6).
  Rejected.
- **Waiting for the Research Lab's T3 (#32).** It would block this
  replication behind #30. `docs/direction.md` rule 6 makes that document
  the spec until the Lab exists. Rejected; #32 adopts or replaces this.

## Accept when

#206, from the merge commit of this ADR's PR (#207) exactly:

- `measure_p1_replication` passes the guard of D1, including the pins of
  the pre-registration record, the frozen hashes, the record-set check
  and 0 twin mismatches;
- the report shows the commit it ran from, equal to that merge commit, and
  the printed `replication.rs frozen decisions` SHA-256, equal to the
  pinned one;
- `docs/research/205-p1-replication.md` records the dataset, the queries'
  hashes, the commands, the inputs and every table the measurement
  prints, `not measurable` contexts and null results included.

Null results do not block acceptance: S3 is an outcome, not a failure.
The acceptance PR adds an `## Acceptance` section and sets this ADR to
accepted.

## Pre-registration record

Written by #205's second commit, after the operational steps and before
any outcome of the window was read. All times 2026-11-09 UTC. Paths are
placeholders: `<mie>` is the release binary, `<test binary>` the release
build of `order_flow_study`, `<dataset>` the replication dataset, and the
two configs are the #12-year store's and the replication store's.

- **Code.** Every step ran from a clean checkout of
  `72371e363248db70b8a2b2d4445291859d7b311e`, the commit that added this
  ADR: `cargo build --release -p mie-cli` and `cargo test --release -p
  mie-cli --test order_flow_study --no-run`, under a 400 % CPU and 8 GB
  cap. The dataset's `code` line is that commit, without `-dirty`.
- **Engine-key probe.** One #12-year day, 2025-10-01, built from the
  #12-year store with #28's streams: engine `4c844593779da2e3-1`,
  `conditional.sampling@1 2c71fff23e8b0415`, `conditional.labels@2
  6f90bff079e746f2`, D1's, so the features, sampling and labels are #28's.
  1 439 records, `verify` PASS, nothing queried.
- **Dry run** of the import over the window: 1 735 files published, 0
  missing, 0 failed
  ([results](https://github.com/Replikanti/mie/issues/205#issuecomment-6729538103)).
- **Import**, year by year under an 8 GB cap, into the replication store
  (`streams = ["aggTrades"]`, its own `raw_root`): 366, 365, 365, 365 and
  274 files, 0 missing, 0 failed, 2 327 594 366 rows, 00:43 … 03:56. The
  store holds 66.0 GB (66 021 622 871 bytes) for the 1 735 days.
- **`archive-verify`** over the window: PASS. `aggTrades`: 3 223 files,
  2 327 594 366 rows, 0 normalize errors, 7 id breaks, 13 holes over 60 s
  on 9 days (listed in `docs/data-availability.md`), data
  `7e92345c663e72e096cbdf2921f60eaa8d9cbae291cd2f1f82bdd64306d27d10`. No
  hash, normalization or ledger error.
- **Build**, once, under a 400 % CPU and 8 GB cap: wall time 2:44:28
  (h:mm:ss), peak RSS 0.71 GB, 2 327 594 373 events, 0 rejections,
  2 638 166 records and labels. `conditional verify` PASS. The manifest:

  | Line | Value |
  |---|---|
  | `source` | `archive binance-archive/BTCUSDT streams aggTrades correction fold` |
  | `replay`, `records` | `1577836800000 1727740800000` |
  | `data` | `7e92345c663e72e096cbdf2921f60eaa8d9cbae291cd2f1f82bdd64306d27d10` |
  | `engine` | `4c844593779da2e3-1` |
  | `sampling` | `conditional.sampling@1 2c71fff23e8b0415` |
  | `labeling` | `conditional.labels@2 6f90bff079e746f2` |
  | `contexts.parquet` | 2 638 166 rows, `205a82462872954569738254ea9e0c9b053b1966f20a3fc0e2cac5f2ec802795` |
  | `outcomes.parquet` | 13 190 830 rows, `d0b88950bc709459e95436283177e0ace7a9c661d587657b0c07a3ac18eb62a7` |
  | `passages.parquet` | 75 907 rows, `effca270243585914bd86dc8b894142536f8f6141633e3c09079a5e1ac4974f9` |
  | `effort.parquet` | 7 914 498 rows, `eeeda049d5bb2b3d9d326c9edc92c84a713c026b36af4f971642d78c26cb637a` |
  | `content` | `e985ea3e9efa9cc9aaf15dbc013fe76eb76711e811a4d3f414e79b591402049f` |
  | `code` | `git:72371e363248db70b8a2b2d4445291859d7b311e` |

- **Coverage**, under a 400 % CPU and 12 GB cap: wall time 0:39.13
  (m:ss), peak RSS 2.6 GB, coverage twin mismatches 0. The output is
  [`docs/research/205-p1-replication-coverage.md`](../research/205-p1-replication-coverage.md).
  - **46 of 48 contexts are measurable**: all 24 at 4h and 22 of 24 at
    1d.
  - **Not measurable:** P1-buy w=1h high 1d, 354 units, and P1-buy w=1h
    extreme 1d, 288 units. S2 applies: #206 prints only their counts, and
    they enter BH with p = 1, so m stays 48 (D3).
  - No member of any context is dropped from E for an empty stratum.
  - The window's median baseline half-widths are 54.08 bps at 4h and
    144.98 bps at 1d, so the fee at the median is 0.18 R and 0.069 R,
    against 0.26 R and 0.09 R on the #12 year.
- **Against the addendum's assumption.** The `docs/direction.md` addendum
  of 2026-11-08 sized its power simulation with #28's units per year
  scaled to this window, which put about 470 units on P1-buy w=1h high at
  1d and every one of the 48 contexts at n_min or above. The coverage
  counts 354 units for that context and 288 for P1-buy w=1h extreme at 1d,
  both below 385. No input changes: q stays 0.05, n_min stays 385, the two
  contexts read `not measurable`, and the addendum's power figure for its
  example context does not describe this window.
- **The pins** the measurement asserts (D1): data `7e92345c…`, content
  `e985ea3e…`, contexts 2 638 166, baseline 2 498 200, trigger 139 966,
  effort rows 7 914 498 (3 per context), races that ran 2 634 304 at 4h
  and 2 613 978 at 1d.
- **Frozen SHA-256 at the first coverage** (from `72371e3`), as it
  printed them; the amendment below replaces the ADR's and the `205`
  queries': `028-order-flow-interactions.sql`
  `dd32d605081d6c80992655e16f453d143ee4e3ba5b17f4bbe399fcee0e7144a1`,
  `205-p1-replication.sql`
  `5d6d726da962d40d1d18b5295a13b366afa5ebea90fd1e7b49486022a2979014`,
  this ADR's frozen text
  `aeeadf25f10c3a8edb17a5f77c9b8ddb088dcef7c697e3a4fa2dd5aa513e3fe4`.
- **Changes after the first coverage.** The second commit set the pins,
  updated the tool's comments, its ignore reason and the documented test
  filter, added a test that the pins equal the coverage report's, and
  added the documents; it changed no frozen text and no count. The
  amendment below changes the frozen text, the queries and the code.
- **Deviations.** The first coverage invocation passed the filter
  `--exact coverage_p1_replication`, which matches no test (the entry's
  path is `replication::coverage_p1_replication`), so it ran nothing and
  printed nothing; it was rerun with the full path 14 s later. No other
  test entry, no query, no DuckDB client and no measurement ran on the
  window.
- **Command log**, every command run on the replication store or dataset,
  and the probe. The last line is the coverage rerun of the amendment
  below, with `<test binary>` rebuilt from `d9df826`:

  ```sh
  2026-11-09T00:21:29Z systemd-run --user --scope -q -p CPUQuota=400% -p MemoryMax=8G <mie> conditional build --config <#12-year archive config> --source archive --streams aggTrades,metrics --trade-correction fold --from 2025-10-01 --to 2025-10-01 --out <probe>
  2026-11-09T00:21:41Z <mie> archive-import --config <replication archive config> --streams aggTrades --from 2020-01-01 --to 2024-09-30 --dry-run
  2026-11-09T00:43:17Z systemd-run --user --scope -q -p MemoryMax=8G <mie> archive-import --config <replication archive config> --streams aggTrades --from 2020-01-01 --to 2020-12-31
  2026-11-09T01:10:44Z systemd-run --user --scope -q -p MemoryMax=8G <mie> archive-import --config <replication archive config> --streams aggTrades --from 2021-01-01 --to 2021-12-31
  2026-11-09T02:09:51Z systemd-run --user --scope -q -p MemoryMax=8G <mie> archive-import --config <replication archive config> --streams aggTrades --from 2022-01-01 --to 2022-12-31
  2026-11-09T02:51:36Z systemd-run --user --scope -q -p MemoryMax=8G <mie> archive-import --config <replication archive config> --streams aggTrades --from 2023-01-01 --to 2023-12-31
  2026-11-09T03:26:44Z systemd-run --user --scope -q -p MemoryMax=8G <mie> archive-import --config <replication archive config> --streams aggTrades --from 2024-01-01 --to 2024-09-30
  2026-11-09T03:56:05Z systemd-run --user --scope -q -p MemoryMax=8G <mie> archive-verify --config <replication archive config> --from 2020-01-01 --to 2024-09-30
  2026-11-09T04:32:15Z systemd-run --user --scope -q -p CPUQuota=400% -p MemoryMax=8G /usr/bin/time -v <mie> conditional build --config <replication archive config> --source archive --streams aggTrades --trade-correction fold --from 2020-01-01 --to 2024-09-30 --out <dataset>
  2026-11-09T07:16:43Z <mie> conditional verify <dataset>
  2026-11-09T07:16:46Z systemd-run --user --scope -q -p CPUQuota=400% -p MemoryMax=12G env MIE_OUTCOME_DATASET=<dataset> /usr/bin/time -v <test binary> --ignored --exact --nocapture coverage_p1_replication
  2026-11-09T07:17:00Z systemd-run --user --scope -q -p CPUQuota=400% -p MemoryMax=12G env MIE_OUTCOME_DATASET=<dataset> /usr/bin/time -v <test binary> --ignored --exact --nocapture replication::coverage_p1_replication
  2026-11-09T09:04:39Z systemd-run --user --scope -q -p CPUQuota=400% -p MemoryMax=12G env MIE_OUTCOME_DATASET=<dataset> /usr/bin/time -v <test binary> --ignored --exact --nocapture replication::coverage_p1_replication
  ```

### Amendment after review, before any outcome was read

Made 2026-11-09 on #207, after the [review](https://github.com/Replikanti/mie/pull/207#issuecomment-6735263891)
and the [QA report](https://github.com/Replikanti/mie/pull/207#issuecomment-6735947246),
**before any outcome of the window was read**: the only program that has
run on the replication dataset is the outcome-blind coverage. D6 allows a
change after the coverage to fix a review defect; these are the two review
findings.

- **Finding 1, the freeze did not cover the code.** The review set q to
  0.20, the draws to 1 000, the null blocks to 2 and 4 weeks and the T2
  block to 7 days, and every test stayed green: only the texts were
  hashed, and #206 could run from any later main.
  - D6 gains *Frozen code*: one marked decision region of
    `replication.rs` holds every constant and function that decides the
    replication, its SHA-256 is pinned by the same test as the texts', both
    entries print it, and the measurement refuses a different one. D6
    gains *The run's commit*: #206 runs from the merge commit of #207
    exactly.
  - *Accept when* requires that commit and the printed region hash.
  - The decision items moved into the region unchanged, apart from
    finding 2. A test asserts the D3 numbers and that each of the review's
    mutations, and changes to BH, the reading, the p-value and the mark,
    move the region's hash; applied to the file, each of the four review
    mutations fails `the_frozen_texts_equal_their_pins`.
- **Finding 2, T2 scored a `neither` exit at 0 R.** A trade held to H
  without a barrier hit closes at the last price.
  - D3 splits the race outcome x (ADR-058 D4's, for E and T3, unchanged)
    from T2's net outcome, whose gross for `neither` is the mark to market
    σ·m at H, with the reason. D5 notes that the marks are measurement
    data.
  - The queries gain `rep_marks` ((last − reference) / (upper −
    reference) at 4h and 1d), and `rep_members` carries `mark_4h` and
    `mark_1d`. The coverage chain is unchanged.
  - Fixture tests: a `neither` exit drifting with the hypothesis, against
    it and at the reference, for both sides and both directions s, and the
    marks read back from each label's last trade.
- *Alternatives considered* gains the two rejected options.
- **Hashes.** The `028` queries are unchanged
  (`dd32d605081d6c80992655e16f453d143ee4e3ba5b17f4bbe399fcee0e7144a1`).

  | Text | Before | After |
  |---|---|---|
  | ADR-064 frozen text | `aeeadf25f10c3a8edb17a5f77c9b8ddb088dcef7c697e3a4fa2dd5aa513e3fe4` | `6ed0f6afb3688c99ebc501eb1530e0cd088122fa3e7a1e4a067bc08fd6dcafae` |
  | `205-p1-replication.sql` | `5d6d726da962d40d1d18b5295a13b366afa5ebea90fd1e7b49486022a2979014` | `5e742fed2fba222bb29f501de22bddb3c5d2a399186c495fd574118df7ec74c3` |
  | `replication.rs` frozen decisions | — (not hashed) | `cafdbb1b139beba96739b18d7aa9bf83f83bf2cb64eeb21bf7dbae023e52d852` |

- **Coverage rerun**, from a release test binary of a clean checkout of
  `d9df826f21df86f4f38329ab4766fe11971b01ea`, the amended commit, under a
  400 % CPU and 12 GB cap, on the same dataset (content `e985ea3e…`, built
  from `72371e3`; the amendment changes no code the build runs). The
  command is the last line of the command log. Wall time 0:42.29 (m:ss),
  peak RSS 2.6 GB, 1 test ran, coverage twin mismatches 0. Against the
  first run only the hash lines changed: the `205` queries `5e742fed…`,
  the frozen text `6ed0f6af…` and the new `replication.rs frozen
  decisions` line `cafdbb1b…`. Every count, and 46 of 48 measurable, is
  identical, so the pins stay as they are.
  [`docs/research/205-p1-replication-coverage.md`](../research/205-p1-replication-coverage.md)
  now holds the rerun's output.
