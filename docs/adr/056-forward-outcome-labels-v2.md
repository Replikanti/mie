# ADR-056: Forward outcome labels v2 — barriers scaled by the ATR(14) of each horizon's own bars

- Status: proposed
- Date: 2026-10-26

## Context

ADR-055 decided the forward outcome labels of #26, `conditional.labels@1`
(fingerprint `604b624cc2522df9`). Its D4 scales the barrier pair of
horizon H by the one ATR there is, `volatility.atr.1h@1`, as
`reference ± b_H · ATR(1h)` with `b_H = √(π · H / 32 h)`, and predicts from
a driftless Brownian null that 0.92 % of the races are still open at H
(`Neither`). Its measurement (#26 M3, ADR-055 *Measurement*) met T0 and
completeness and missed that prediction by 7 to 18 times: on the #12 year
the `Neither` share was 16.338 / 12.548 / 10.036 / 9.281 / 6.191 % at
5m / 15m / 1h / 4h / 1d. At 5m and 15m it exceeds the 10.8 % that D4 lists
under "what breaks". ADR-055's Consequences pre-register what follows: a
large miss means a new labeling version from a stated reason, not a refit,
because no hold-out is left (ADR-053 D7). #151 asks for that version.

This ADR states the mechanism, derives the barrier rule of
`conditional.labels@2` from it, and pre-registers its prediction and the
miss criterion before any v2 run on real data. Every step below is
derived; no outside number is used.

### The mechanism

1. **Estimator noise is not the cause.**
   - Under the constant-σ Brownian null an hour's true range has mean
     √(8/π)σ and standard deviation √(4 ln 2 − 8/π)σ (Feller), so its
     coefficient of variation is 0.298.
   - Wilder smoothing of length L keeps 1/(2L − 1) of the variance of its
     inputs. At L = 14 that is 1/27, so the coefficient of variation of
     ATR(14) is 0.298 / √27 = 0.0573.
   - With that noise the null `Neither` share rises from 0.92 % to about
     1.03 % (ρ below lognormal with that spread, through the series of
     step 2), not to 6–16 %.
2. **The share is set by quiet windows.**
   - Let ρ = the window's diffusive σ ÷ the σ the barrier scale implies.
   - The probability that a race is still open at H is
     g(ρ) = (4/π) Σₙ≥₀ (−1)ⁿ/(2n+1) · exp(−(2n+1)² π² ρ² / 2) (the
     exit-time series of ADR-055 D4 with the band in units of the true σ).
   - g(1) = 0.92 %, g(0.8) = 5.4 %, g(0.7) = 11.3 %, g(0.6) = 21.5 %.
   - N(H) = E[g(ρ_H)] is therefore driven by the low tail of ρ_H, that is
     by how well the scale tracks the window's own volatility.
3. **Why v1 falls with H.**
   - For a time-changed Brownian motion, E[range over T | σ] =
     √(8/π) · √V_T, where V_T = ∫σ² over T.
   - Split an hour into k windows of H = 1h / k. Then
     √k · meanᵢ √Vᵢ ≤ √(Σᵢ Vᵢ) (Cauchy–Schwarz), with equality only for
     uniform variance.
   - So `ATR(1h) · √(H / 1h)` overstates the scale of the typical 5m or
     15m window: its band is too wide, which gives more `Neither`. By the
     same inequality it understates the scale of 4h and 1d windows, which
     gives fewer `Neither`.
   - The fixed 14 h memory of ATR(1h) adds a lag whose ratio to H runs
     from 168 (5m) to 0.58 (1d).
   - Both effects match the measured fall from 16.338 % to 6.191 %.
4. **The hypothesis this version tests:** BTC volatility has no preferred
   time scale between 5m and 15 days (14 daily bars). Then the law of ρ_H,
   for an estimator built from 14 bars of length H, does not depend on H,
   so N(H) is the same at every H.
5. **What can break it:** fixed-period seasonality (24 h, weekly) and
   jumps, which are not scale free. The pre-registered reading of a miss
   (*Accept when*) names which one.

Delivery is in two PRs under #151: this ADR (proposed, with the
prediction) with the code and tests; then the measurement of *Accept when*
recorded here, which settles the status. No accepted ADR is edited.
ADR-055's status line becomes `superseded by ADR-056`; nothing else in it
changes.

## Decision

### Approach

`conditional.labels@2` keeps all of ADR-055 except the barrier scale. Each
horizon's band is `reference ± b · ATR_H`, where ATR_H is ADR-033's
ATR(14) over the closed bars of the horizon's own timeframe and
b = √(π/32) at every horizon. The labeler computes ATR_H itself, with the
same domain bar and ATR code the engine runs, from the accepted events up
to and including the decision event. No engine feature, catalog entry,
lock, state hash or record changes.

*Why this rule:* v1's `ATR(1h) · √(H / 1h)` has a fixed bar length and a
fixed 14 h memory, so its error depends on H (mechanism, step 3). An
estimator whose bars and memory both scale with H removes that dependence
if volatility has no preferred time scale (step 4). At 1h it *is* v1's
rule, so v1's measured 1h share is a parameter-free prediction for every
horizon.

*Rejected:*

- **Refitting b_H to the measured shares.** That is a refit, and no
  hold-out is left (ADR-053 D7).
- **Per-timeframe ATR as engine features.** That changes Market State and
  the ADR-054 snapshot and needs a new sampling version, far beyond this
  issue. Labels are not Market State.

### What stays as in ADR-055

Each decision below is unchanged, with the reason the scale change does not
touch it:

- **D1, one label per record:** the join key reads the records, not the
  barriers.
- **D3, the horizons:** the scale is per horizon; which horizons exist is
  the trigger evidence's question (ADR-053), not the scale's.
- **D5, the tie rule:** it orders touches of whatever barriers there are.
- **D6, every censor rule:** censoring reads the trades stream, its gaps,
  its rejections and the end of the data, never a barrier.
- **D7, excursions and the last trade:** measured from the reference, not
  from a barrier.
- **D8, the passages:** their prices come from the trigger's evidence.
- **D9, thinning:** it reads decision times and episode flags only.

### D2′. Reference, path, window and what a label depends on

- Reference, path and window are ADR-055 D2's.
- A label now depends on three things: the trades strictly after the
  record's event, the record's reference, and the five ATR_H. The labeler
  computes the ATR_H from the accepted events up to and including the
  decision event. It still never takes the engine.
- `ForwardLabel.atr` stays the record's ATR(1h). Each horizon's scale is
  carried by its barriers: ATR_H = offset / b, recoverable to better than
  2 · 10⁻⁸ USDT (the offset's rounding of at most half a unit, divided by
  b = 0.31333, is 1.6 · 10⁻⁸ USDT). So `Display` and the label struct stay
  unchanged.

### D4′. Barriers (replaces ADR-055 D4)

- **Definition:** upper / lower = reference ± b · ATR_H.
  - ATR_H = ATR(14) per ADR-033 decisions 2 and 6 (Pine `ta.atr(14)`: the
    mean of the first 14 true ranges, then Wilder smoothing, each step
    rounded half to even to 1e-8, and the series-continuity table), over
    the closed ADR-031 bars of H's timeframe (`Timeframe::M5`, `M15`,
    `H1`, `H4`, `D1`; the horizons are bar timeframes by D3).
  - It is read after the decision event is applied, from closed bars only.
    The developing bar is never used, and the value is read once, when the
    label opens.
- **Value:** b = √(π/32) = 0.313329 at every H, stored as
  `SCALE_MULTIPLE_E5 = 31_333` per 10⁵. The offset is rounded half to even
  to 1e-8 USDT (the price unit, ADR-027).
  - Storage error < 0.5 / 31 333 = 1.6 · 10⁻⁵ relative.
  - 31 333 is v1's 1h multiple, so v2's 1h band is v1's.
- **Derivation:** under the null, E[ATR_H] = √(8H/π) σ (the mean range of
  a bar of length H, Feller). So a = b · ATR_H = √(H/4) σ, the mean exit
  time a²/σ² is H/4 at every H, and the null `Neither` share is still
  (4/π) e^(−π²/2) = 0.92 %. The *Accept when* prediction replaces that
  null as the expected value: the mechanism says the null is not the
  value, only that the value does not depend on H.
- **Why ATR(14) at every timeframe:**
  - At 1h it is the engine's `volatility.atr.1h@1`, the regime's
    estimator and v1's scale. That identity is what makes v1's measured
    1h share a prediction known before the run.
  - One length at every timeframe keeps memory ÷ horizon = 14 at every H,
    which is the invariance condition of the hypothesis.
- **What breaks at other values:**
  - **b.** A mean exit time of H/2 leaves 10.8 % of null races open, and H
    leaves 37 %: the race stops answering "which threshold first". H/6
    leaves 0.08 % but measures a sixth of the horizon. Any b ≠ 31 333 per
    10⁵ also breaks the 1h identity, so the prediction would need a model
    of how N depends on b: a fit.
  - **L ≠ 14 at 1h.** The same loss of the identity.
  - **A different L per horizon.** Breaks the invariance: memory ÷ horizon
    would depend on H again.
  - **A shorter L.** Raises the estimator noise: the coefficient of
    variation is 0.298 / √(2L − 1), so L = 7 gives 0.083 and L = 3 gives
    0.133.
  - **A longer L.** Raises the lag, the second v1 effect.
- **Warm-up:** a scale needs 15 bars of its timeframe after a cold start or
  a break: the first bar is partial and only anchors, then come 14
  samples (ADR-033 decision 6).
  - That is 75 min, 3 h 45 min, 15 h, 60 h and 15 d.
  - A scale that is not ready, an offset that rounds to 0, or a barrier
    outside the price range makes the race `Unavailable`. The excursions
    are still computed.
  - Consequences: the year's first 15 days have no 1d race (15 of 365
    days, about 4 % of the 1d labels); a live store shorter than 60 h has
    no 4h or 1d race.
- *Rejected:*
  - **Refit b_H.** No hold-out.
  - **Narrower bands, for example a mean exit time of H/16.** Under the
    dispersion of ρ the share still stays far above 1 % (g is convex in
    the low tail of ρ), and the race would measure a sixteenth of H.
  - **Realized variance from 1m returns.** It needs new choices (the step,
    the window) that ATR(14) already settles, and those choices would be
    made after seeing v1's shares.
  - **A median or otherwise robust ATR.** A new estimator with no domain
    precedent. It is the v3 candidate if jumps cause a miss.
  - **Engine features plus `conditional.sampling@2`.** Changes Market
    State; labels are not Market State.
  - **A barrier scale from the window itself.** Not known at decision, so
    no hypothesis could act on it.

### D10′. Provenance

- `LABELING_V2 = PipelineKey::new("conditional.labels", 2)`.
- `labeling_text(Labeling::V2)` names every rule: ADR-055's text with the
  `labeling` line and the `barriers` line replaced. The new `barriers`
  line names the scale (ATR(14) of ADR-033 over the closed ADR-031 bars of
  the horizon timeframe, read after the decision event, restarted on a bar
  error), the multiple 31 333 per 10⁵, the rounding and the three
  `Unavailable` causes. Its FNV-1a 64 fingerprint (ADR-029) is pinned by a
  golden test.
- `LABELING_V1`, its text and its fingerprint `604b624cc2522df9` stay, and
  stay executable through `Labeling::V1`, so every published v1 number can
  be re-derived. `Labeling::CURRENT` is v2.

### D11′. Placement and isolation

- The labeler holds its own `BarSet` and five `AtrSeries` (the ATR(14)
  stepper `AtrRegimeSeries` is now built on): O(1) memory beside ADR-055
  D11's.
- `observe` feeds them with every accepted event. `reject` never does: the
  engine commits an event atomically, so a rejected event changes no
  engine bar either, and the labeler's bars stay the engine's.
- An event the bars cannot take (`BarError`), or a bar a scale cannot take
  (an ATR overflow), can only come from an event the engine never took: a
  trailing trades gap, or a labeler started after the engine. It resets
  the bars and scales (or that scale) to warming up and counts in
  `LabelStats.scale_breaks`.
- The D11 guard in `tools/check-architecture.sh` is unchanged: the bars
  and scales are domain code, read by the labeler only.

### D12′. Where each number comes from

| Number | Source | What breaks at another value |
|---|---|---|
| Horizons 5m, 15m, 1h, 4h, 1d | ADR-055 D3: the bar timeframes trigger evidence is built on (ADR-053); the scale change does not touch which horizons exist | 1m: the sampling grid step, no evidence at that scale. Longer than 1d: no evidence spans it, and it lengthens the end-of-data censoring and the memory held |
| b = √(π/32), stored 31 333 per 10⁵ | D4′: null mean exit time H/4 at every H, from E[ATR_H] = √(8H/π) σ and the exit time a²/σ² | H/2: 10.8 % null timeouts; H: 37 %; H/6: a sixth of the horizon; any other value loses the 1h identity |
| Denominator 10⁵ | b stored to 5 decimals: relative error < 1.6 · 10⁻⁵ | 4 decimals: up to 1.6 · 10⁻⁴ relative, and the 1h multiple would no longer be v1's |
| Offset rounded half to even to 1e-8 USDT | The price unit (ADR-027); half to even has no bias | — |
| ATR length 14 at every timeframe | D4′: at 1h the engine's ATR(1h) (ADR-033), so the 1h identity holds; one length keeps memory ÷ horizon = 14 | Shorter: noisier (CV 0.298/√(2L − 1)); longer: more lag; per-horizon lengths: no invariance |
| Warm-up 15 bars (75 min / 3 h 45 min / 15 h / 60 h / 15 d) | ADR-033 decision 6: a partial first bar anchors, then 14 samples | — (follows from the length and the continuity table) |
| ATR_H recoverable to < 2 · 10⁻⁸ USDT | D2′: half a unit of offset rounding ÷ b | — (a property, not a parameter) |
| CV 0.298 of a true range, 0.0573 of ATR(14), 1/27 | Mechanism step 1: Feller's range moments; Wilder keeps 1/(2L − 1) of the variance | — (the reason estimator noise is not the cause) |
| g(ρ): 0.92 / 5.4 / 11.3 / 21.5 % at ρ = 1 / 0.8 / 0.7 / 0.6; 1.03 % | Mechanism step 2: the exit-time series; 1.03 % = E[g] under lognormal ρ with the 0.0573 spread | — (the reason quiet windows set the share) |
| Lag ratio 168 (5m) to 0.58 (1d) | Mechanism step 3: 14 h ÷ H | — (the second v1 effect) |
| 0.92 % null `Neither` share | (4/π) e^(−π²/2), the leading term at H = 4 · mean exit time | — (stated next to the prediction, not the prediction) |
| Prediction N(H) = 10.05 % at every H | *Accept when*: v1's 1h `Neither` count 56 372 over the 1h races that ran (561 695 labels − 899 `Unavailable` − at most 62 censored) | — (fixed by v1's published counts before the run; no constant is fitted) |
| Miss bound 1.62 on max_H N(H) ÷ min_H N(H) | *Accept when*: √(16.338 / 6.191) = 1.6245 rounded down: v2 must remove more than half of v1's log-spread (ln 2.639 = 0.970) | ≥ 2.64 accepts a rule no flatter than v1; below about 1.38 sampling noise at 1d alone could miss a true prediction |
| 1.38, 350 daily windows, SE 1.6 pp (16 % relative) | *Accept when*: about 350 disjoint daily windows after the warm-up; a 10 % share has SE √(0.1 · 0.9 / 350) = 1.6 pp; two SE in log, e^(2 · 0.16) ≈ 1.38 | — (the lower limit the bound must stay above) |
| ±0.08 in the sd of log ρ | *Accept when*: under lognormal ρ at median 1, where N = 10.05 % needs an sd of 0.53, ±0.08 between horizons gives a max ÷ min of about 1.56 to 1.62 | — (what the bound allows, stated to read it) |
| 1d passage window | ADR-055 D8: the longest horizon; unchanged by the scale | Shorter: levels of 1d evidence could not be revisited at their own scale |
| Thinning step = H | ADR-055 D9: disjoint windows of one stream; unchanged by the scale | Shorter: clear windows overlap; longer: fewer clear records than disjointness needs |
| Censor "strictly before c" | ADR-055 D6: event time has millisecond resolution and the order within the censor's millisecond is unknown (ADR-028) | "At or before c": a trade in a millisecond the gap covers would count |
| 1.4 × (bar labels stay rejected) | v2's 5m band, 2 · b · ATR_5m, is 2 b √5 = 1.40 times the null mean 1m range under the null of D4′, as v1's was | — (the reason ADR-055's rejection of 1m-bar labels still holds) |
| ~45 000 model-checked year labels | 36 096 trigger records + 8 760 whole UTC hours of baselines (*Accept when*, ADR-055's selection by decision time only) | Fewer: weaker T0; every label: a model as costly as the replay times the window |
| Development year 2025-10-01 … 2026-09-30 | The #12 year every earlier measurement used, so the 1h identity can be checked against ADR-055's published counts | Another year: no published v1 counts to check the identity against |
| 0 mismatches (T0), 0 scale mismatches, 0 scale breaks | The labeler and the model are deterministic code written from one text, so any difference is a bug | — |
| About 4 % of 1d labels without a race | The 15-day 1d warm-up over 365 days | — (a consequence, reported) |
| 60 h | The 4h warm-up: no 4h or 1d race on a shorter live store | — (a reachability limit, flagged below) |

There is no other number.

### Out of scope

- Engine, catalog, lock, state-hash or ADR-054 sampling changes, and new
  `volatility.atr.*` features.
- A warm-started labeler (#106, ADR-052): the scales start cold with the
  replay, like the engine.
- Any constant fitted to the year, and the calibration slice of
  `docs/direction.md`.
- Upper-vs-lower shares, excursion or time quantiles, and any split by
  trigger, regime or location.
- Re-running v1.
- #27, #28 and #38.
- Changes to the semantics of ADR-055 D1, D3 and D5–D9.

## Consequences

- Labels are still not Market State: no feature, catalog, lock, state-hash,
  record or engine golden changes. `AtrRegimeSeries` is rebuilt on the
  extracted `AtrSeries` with bit-identical output, pinned by the existing
  ATR and regime goldens and the Pine reference fixture.
- The labeler builds one more `BarSet` per accepted event, and so does the
  measurement's model. ADR-055's year run took 1:26:36 at a peak RSS of
  1.47 GB, well inside the 8 GB cap the measurement runs under.
- The year's first 15 days have no 1d race and its first 60 h no 4h race;
  on the R0 live store (one cleanly ended run shorter than 60 h) every 4h
  and 1d race is `Unavailable`.
- v1 stays reproducible: `Labeling::V1` keeps its text, fingerprint and
  label goldens.
- If the prediction misses, this ADR stays `proposed` with a `- Measured:`
  line and the pre-registered reading, and the next version follows from
  that reason, not from a refit.

## Alternatives considered

- **Amending ADR-055 instead of superseding it.** ADR-055 is the frozen
  record of labeling v1: its decision text is what fingerprint
  `604b624cc2522df9` names, and its D4 prediction is the one #26 M3
  refuted. Editing a proposed ADR in place is allowed, but it would put a
  second pre-registered prediction into the file whose first one failed,
  and which prediction preceded which run could then only be recovered
  from git history. One status line would cover two labeling versions, one
  of them refuted. Rejected: one labeling version maps to one ADR.
- **Refitting b_H to the measured shares.** No hold-out is left (ADR-053
  D7). Rejected.
- **Per-timeframe ATR engine features and `conditional.sampling@2`.**
  Changes Market State for a label-only need (D4′). Rejected.
- **Narrower bands, realized variance from 1m returns, a robust ATR, a
  scale from the window.** Still far above 1 %, new post-hoc choices, no
  precedent (the v3 candidate for jumps), or not known at decision (D4′).
  Rejected.

## Accept when

Pre-registered: this section is on main before any v2 run on real data.
The acceptance run uses `label_measure` (`measure_labels`,
`measure_labels_live`) from the merge commit of this ADR's code, release
build, on the #12 year 2025-10-01 … 2026-09-30 and on the ADR-051 R0 live
store.

- **T0 (asserted):** 0 mismatches against an independent model on both
  stores. The model builds its own bars and Wilder ATR from the ADR-031 and
  ADR-033 text and shares no bar or ATR code with the labeler. It checks
  every label on the live store, and on the year every trigger record's
  label plus the baselines at whole UTC hours (ADR-055's selection, chosen
  by decision time only).
- **Completeness (asserted):** exactly one label per record, in record
  order.
- **Scale cross-check (asserted):** the model's ATR_1h equals the record's
  `state.atr` at every record, in readiness and value, and
  `scale_breaks` is 0.
- **1h identity (checked against ADR-055's published counts):** v2's 1h
  `Neither` and `Unavailable` counts equal v1's: 56 372 and 899 on the
  year, 49 and 847 on the live store. A difference is a bug, not a result.
- **v1 reproducibility:** `conditional.labels@1` still derives fingerprint
  `604b624cc2522df9` (CI).
- **Prediction:** N(H) = `Neither` ÷ the races that ran (`Upper` + `Lower`
  + `Neither`). N(H) = N(1h) at every H, and N(1h) is known before the run:
  10.05 % (v1's 10.036 % of all labels, over the races that ran).
- **Miss criterion (the year):** max_H N(H) ÷ min_H N(H) > **1.62** means
  not accepted.
  - *Source:* √(16.338 / 6.191) = 1.6245, rounded down. The hypothesis
    says v1's horizon dependence comes from the estimator–horizon
    mismatch. "Comes from" is taken to mean that v2 removes more than half
    of v1's log-spread (ln 2.639 = 0.970, so at most 0.485 remains).
  - *What breaks:* a bound of 2.64 or more accepts a rule no flatter than
    v1. Below about 1.38, sampling noise at 1d alone could miss a true
    prediction: about 350 disjoint daily windows after the warm-up, so the
    standard error of a 10 % share is 1.6 pp (16 % relative), and the bound
    allows two standard errors, e^(2 · 0.16) ≈ 1.38.
  - Under a lognormal ρ at median 1, 1.62 allows about ±0.08 of difference
    in the standard deviation of log ρ between horizons.
- **Reading of a miss (pre-registered):**
  - N still falls with H (5m highest): **jumps**. A jump adds its size to
    one bar's true range whatever H is, while the diffusive range grows
    with √H, so jumps inflate ATR_H, and widen the band, most at short H.
  - The peak is at 1h or 4h: **24 h seasonality**. At those horizons the
    scale's memory (14 h, 56 h) spans about one daily cycle while the
    window sits in one phase of it, so ρ disperses most there; at 5m and
    15m the memory follows the phase, and a 1d window spans the cycle.
  - 1d highest: **weekly seasonality**. The 1d scale's memory spans two
    weeks while a 1d window sits on a weekday or a weekend day.

  The next version follows from that reason, not from a refit.
- **Reported, no threshold:**
  - per horizon, N as a share of all labels and of the races that ran,
    next to the prediction and v1's share;
  - censored windows by reason, `Unavailable`, `same_ms`, the clear
    counts, `reach_backs`, `scale_breaks`;
  - wall time and peak RSS.
- **Not reported (ADR-055's restriction, kept):** upper-vs-lower shares,
  excursion or time quantiles, and any split by trigger, regime or
  location.
- **Reachability, flagged now:** every criterion runs on the year.
  - The live store holds one cleanly ended run shorter than 60 h, so its
    4h and 1d races are all `Unavailable` (v1's live 4h and 1d had 0
    `Neither` because of censoring).
  - The live store gives T0, completeness, the scale cross-check and the
    1h identity. The miss criterion is evaluated on the year only.
- **No hold-out needed:** no constant of the rule is fitted. b comes from
  the null; 14 and the bars carry their own reasons above. v1's published
  shares enter only the prediction and the threshold.
