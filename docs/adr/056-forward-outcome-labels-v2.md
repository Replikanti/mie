# ADR-056: Forward outcome labels v2 — barriers scaled by the ATR(14) of each horizon's own bars

- Status: accepted
- Measured: 2026-10-28 (#151, see *Measurement*); accepted, T0 and
  completeness met on both stores and the prediction not missed (spread
  max ÷ min 1.5811 against the 1.62 bound)
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
derived. The noise numbers are measured, not simulated: labeling v1's
per-day counts on the #12 year (*The noise, measured on v1* below) give
the step SEs, the noise floor and the rates at which *Reading of a miss*
names a wrong mechanism. The review's simulations (PR #153), which these
numbers first came from, are kept only as superseded history.

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
5. **What can break it:** what is not scale free: jumps, fixed-period
   seasonality (intraday sessions, the 24 h cycle, the week), and a
   characteristic time of volatility clustering. The pre-registered
   reading of a miss (*Accept when*) maps every shape of N(H) to one of
   them, or to none.

### The noise, measured on v1

Measured before any v2 label exists, so the pre-registration stands: the
only real-data inputs are the engine's bars and labeling v1, whose counts
ADR-055 published.

- **Source.** `label_measure`'s `measure_label_noise` replays the #12 year
  (data provenance `5d9e27f862ac54dd93cff904458e9ee92767d5105a9d13a144351cba069f224d`,
  engine `4c844593779da2e3-1`, 603 323 345 events) through the engine, the
  sampler and a labeler under `conditional.labels@1` (fingerprint
  `604b624cc2522df9`); no v2 label is computed. Its v1 counts equal
  ADR-055's published ones (561 695 labels, 1h `Neither` 56 372,
  `Unavailable` 899). It writes the per-UTC-day `Neither` and race counts
  of 365 days; `measure_label_noise_from` reruns the analysis below from
  them with this ADR's bootstrap (56-day blocks, 10 000 resamples, seed
  151). The file also holds each day's 1d range, from which v2's 1d
  `Neither` could be approximated (a range against a Wilder(14) average
  of ranges is v2's 1d race); beyond the volatility below it stays
  unanalysed until the acceptance run, so no v2 outcome is read before
  it.
- **Persistence of daily volatility.** ln of the daily realized σ from
  the engine's 1m closes has a weekly cycle: Saturday's mean is 0.64 and
  Sunday's 0.35 below the Monday to Friday means, and the weekday means
  hold 28.8 % of its variance. Its raw autocorrelation, 0.562 / 0.327 /
  0.542 / 0.418 / 0.373 / 0.233 at 1 / 2 / 7 / 14 / 28 / 56 days, rises
  again at every whole week because of that cycle, not because of memory:
  a day off a whole week it is 0.318 / 0.250 at 6 / 8 days, 0.142 / 0.113
  at 27 / 29 and 0.004 / 0.012 at 55 / 57. With the weekday means removed,
  the AR(1) slope is φ = 0.7235 (lag-1 autocorrelation 0.725,
  block-bootstrap 95 % interval 0.610 … 0.781; the raw slope of 0.560 is
  pulled down by the Friday to Saturday and Sunday to Monday steps), and
  the autocorrelation is 0.725 / 0.591 / 0.359 / 0.177 / 0.124 / −0.063
  at 1 / 2 / 7 / 14 / 28 / 56 days (−0.097 at 42, −0.060 / −0.053 at
  55 / 57): the dependence left after the cycle is about 0.12 at 28 days
  and gone by 42 to 56. ln of the 1d log range, whose own noise
  attenuates φ (0.339 raw, 0.431 without weekday means), shows the same:
  its weekday means hold 26.0 % of its variance, and without them its
  autocorrelation is 0.267 / 0.063 / 0.068 / 0.002 at 7 / 14 / 28 / 56
  days.
- **SE of ln N(H)**, 5m / 15m / 1h / 4h / 1d: 0.045 / 0.049 / 0.057 /
  0.075 / 0.132, 195 to 642 times the binomial variance (design effect),
  that is, worth 870 to 2 900 independent races instead of 560 000.
- **The horizons are not independent.** Over the resamples, ln N(H) of
  neighbouring horizons correlate 0.985 / 0.944 / 0.897 / 0.738, and 5m
  with 1d 0.508. The steps correlate too: 5m→15m with 15m→1h +0.66,
  1h→4h with 4h→1d +0.61, 15m→1h with 4h→1d −0.30, the rest within
  ±0.18. Every SE below is taken over the same resamples, so these
  correlations are in it; nothing assumes independent horizons.
- **Step SEs** (5m→15m … 4h→1d): 0.0091 / 0.0191 / 0.0348 / 0.0915 in log.
- **Noise floor.** The widest pair is 1h with 1d, SE 0.116 of its
  log-difference, so two SE are a ratio of e^(2 · 0.116) = 1.26; v1's own
  5m / 1d spread has an SE of 0.115.
- **Block length.** The same counts at blocks of 1 / 14 / 28 / 56 / 91 /
  122 days give an SE of ln N(1d) of 0.103 / 0.091 / 0.111 / 0.131 /
  0.146 / 0.150 and a widest-pair 2-SE ratio of 1.200 / 1.191 / 1.230 /
  1.261 / 1.293 / 1.297 (`measure_label_noise_from`'s block-length
  sensitivity: this ADR's bootstrap, 10 000 resamples, seed 151, at each
  length; the widest pair is 1h with 1d at 14 to 56 days and 5m with 1d
  at 1, 91 and 122).
  The floor stays below 1.62 at every block tried.
- **Caveat: v1's 1d is not v2's 1d.** v1 scales every horizon by ATR(1h),
  whose memory is 14 h, so its 1d races lack the dependence that ATR_1d's
  14-day memory adds between consecutive days of v2's 1d races; and v1's
  1d share is 6.2 %, not 10 %. v2's 1d noise is therefore likely wider.
  The margin is why the band's numbers still hold: noise alone reaches the
  bound only when two SE reach ln 1.62 = 0.48, a pair SE of 0.24, which is
  2.1 times the measured 0.116. The review of this ADR (PR #153) checked
  that margin under v2's ATR_1d memory on synthetic years only (no v2
  label on real data): ln σ as the measured weekday offsets plus a fast
  and a slow AR(1) component fitted to the autocorrelation without them,
  a Wilder(14) ATR_1d, 400 years. v2's 1d SE of ln N came out 0.09 (an
  expected share per day) to 0.17 (one Bernoulli race per day), a 2-SE
  ratio of 1.20 to 1.41, below 1.62; an AR(1) at φ = 0.725 with the same
  SD gives the same. The 0.24 is 1.4 to 2.6 times those SEs, so v1's
  noise is a usable proxy for the margin. The acceptance run's band
  measures v2's own floor in any case.
- **Superseded history.** The review simulated AR(1) log daily σ with a
  Wilder(14) ATR_1d and found a 2-SE noise ratio of 1.31 / 1.43 / 1.50 /
  1.67 at φ = 0 / 0.8 / 0.9 / 0.95, and its shape simulation fixed
  independent per-horizon SEs (1d: 0.26 relative, the φ = 0.95 value).
  The measurement replaces both. Its model is not the reason: at the
  measured φ of 0.72 (without the weekly cycle) it gives about 1.4, as
  the calibrated rerun does, between its φ = 0.5 and 0.8 rows (1.34,
  1.43). The 1.50 and 1.67 belong to φ = 0.9 and 0.95, which the
  measured 0.72 excludes. The shape simulation's independent horizons
  are replaced because the measured horizons are correlated.

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
| Miss bound 1.62 on max_H N(H) ÷ min_H N(H) | *Accept when*: √(16.338 / 6.191) = 1.6245 rounded down: v2 must remove more than half of v1's log-spread (ln 2.639 = 0.970) | ≥ 2.64 accepts a rule no flatter than v1; alone, without the noise band, a true prediction can exceed it from 1d noise (next rows) |
| 1.38, 350 daily windows, SE 1.6 pp (16 % relative) | *Accept when*: the floor if the about 350 disjoint daily windows were independent: SE √(0.1 · 0.9 / 350) = 1.6 pp, two SE in log e^(2 · 0.16) ≈ 1.38 | — (not used: the windows are serially dependent, next row) |
| Noise floor 1.26; SE of ln N(H) 0.045 / 0.049 / 0.057 / 0.075 / 0.132 | *The noise, measured on v1*: labeling v1's per-day counts on the #12 year, this ADR's bootstrap; the widest pair (1h, 1d) has an SE of 0.116, e^(2 · 0.116) = 1.26 | — (measured, not thresholds; v2's 1d noise is likely wider, and the band measures it on the run; noise reaches 1.62 only at a pair SE of 0.24, 2.1 times the measured one) |
| φ = 0.7235 without weekday means (0.560 raw); autocorrelation without them 0.725 / 0.591 / 0.359 / 0.177 / 0.124 / −0.063 at 1 / 2 / 7 / 14 / 28 / 56 d | *The noise, measured on v1*: ln daily realized σ from the engine's 1m closes, less its weekday means (28.8 % of its variance). The raw 0.542 / 0.418 / 0.373 / 0.233 at 7 / 14 / 28 / 56 d are the weekly cycle: a day off a whole week the raw lags are 0.318 / 0.250 (6 / 8), 0.142 / 0.113 (27 / 29), 0.004 / 0.012 (55 / 57) | — (measured; the reason for the block length: about 0.12 left at 28 d, none at 56 d) |
| 1.31 / 1.43 / 1.50 / 1.67 (superseded) | The review of this ADR (PR #153) simulated 350-day years with AR(1) log daily σ and a Wilder(14) ATR_1d: the 2-SE noise ratio at persistence φ = 0 / 0.8 / 0.9 / 0.95 | — (history only: replaced by the measured floor 1.26. Its model is not the reason: at the measured φ of 0.72 without the weekly cycle it gives about 1.4, between its φ = 0.5 and 0.8 rows; the 1.50 and 1.67 belong to φ = 0.9 and 0.95, which the measured 0.72 excludes; the review's calibrated rerun gives v2's 1d a 2-SE ratio of 1.20 to 1.41) |
| Noise band: ln(max ÷ min) > 2 SE | *Accept when*: two standard errors, the width the derived floor used; the SE is the bootstrap's, so it carries the year's own serial dependence | ln(max ÷ min) ≥ 0 folds both directions, so the rate is two-sided: 4.6 % at 2 SE. 1 SE: a flat truth clears the band about one time in three (32 %), so the 1.62 bound alone would decide most noise misses. 3 SE: a spread has to be half again as far from noise before it counts. The pair is the observed max and min, which noise pulls apart, so the band is weaker than its nominal rate: a guard beside the 1.62 bound, not a test on its own |
| Step band: a step rises or falls only past 2 SE of its own log-difference | *Reading of a miss*: the same bootstrap as the spread's band, one adjacent pair at a time; a step equal in truth clears it in one direction 2.3 % of the time (one-sided normal) | 1 SE: 16 % per step one-sided, so a flat truth's four steps hold a spurious rise or fall about four times in five (one in six at 2 SE, steps taken as independent), and the readings return to the coin flips of raw signs. 3 SE: on the measured 4h→1d SE of 0.092 the week's ln 1.8 = 0.59 step (6.4 SE) clears either way, so 3 SE buys nothing on v1's noise, and on v2's likely wider 1d noise a real week step would have to be half again as far from noise before it counts |
| At most 2.3 % undefined resamples (230 of 10 000) | *Accept when*: the one-sided normal tail beyond the band's 2 SE (2.275 %, rounded up to the next per mille). An undefined resample stands for an infinite spread; up to the tail the band leaves outside anyway it is dropped. At the limit the SD of the rest is at most 6 % below the full one (a normal cut at +2 SD keeps an SD of 0.94), a small change beside the gap between the measured floor and the bound (2 SE = 0.23 against ln 1.62 = 0.48). On v1's counts no resample is undefined at any horizon (0 of 10 000); the limit guards v2's 1d, whose first 15 days have no race | 0, any undefined resample counting as outside: one resample of 10 000 decides the band. 4.6 % (both tails): cut at +1.69 SD the SD of the rest is 9.5 % low, and the band narrows by that much on exactly the years whose resamples come closest to an empty horizon |
| Block 56 UTC days | *Accept when*: a whole number of weeks, so every block holds each weekday eight times and keeps daily volatility's weekly cycle (Saturday 0.64 and Sunday 0.35 below the weekdays in ln σ, 28.8 % of its variance) intact inside it; and long enough to cut the dependence left without the cycle, whose autocorrelation is about 0.12 at 28 days and none at 56 (−0.063; *The noise, measured on v1*). 56 is also four memories of the 1d scale ((13/14)^56 = 1.6 % of ATR_1d's weight lies beyond it) and keeps 6.5 blocks a year; it raises the SE of ln N(1d) from 0.111 (28 d) to 0.131 | Not a whole number of weeks: a block holds some weekdays once more than others, so the resamples' weekday mix varies and the weekend dip adds variance the year does not have. 28: cuts the 0.12 still left at 28 days and narrows the band (SE of ln N(1d) 0.111); below 14 the band shrinks back towards the refuted binomial one (0.091 at 14 d). 91: 4 blocks a year, so the SE rests on four units (0.146). Dependence beyond 56 days, none measured, would still narrow the band; the 1.62 bound, 2.1 times the measured 2 SE in log, is what holds then (the floor is 1.30 even at 122-day blocks) |
| Day = UTC day of the decision | *Accept when*: the 1d horizon's bars are UTC days (ADR-031), and 1d noise is what the band is for | — (windows that cross midnight tie neighbouring days, which a block keeps together) |
| 10 000 resamples | *Accept when*: the Monte Carlo error of an SD from B draws is 1/√(2(B − 1)), 0.71 % at 10 000, at about 10⁷ additions | 1 000: 2.2 %, the band moves by that much with the seed; more buys nothing the 1.62 bound would notice |
| Scale memory 14 · H: 70 min (5m), 3.5 h (15m), 14 h, 56 h, 14 d | *Reading of a miss*: 14 bars of H's timeframe (D4′) | — (follows from the length) |
| Step SE 0.0091 / 0.0191 / 0.0348 / 0.0915 in log; step correlations up to +0.66 | *The noise, measured on v1*: the band's bootstrap of each adjacent pair, over the same resamples | — (measured, not thresholds; the run prints v2's own) |
| Wrong-mechanism rates 4.2 / 4.1 / 1.4 / 0.0 % of the misses; flat truth 14 misses in 200 000 | *What the rule achieves*: the shape evaluation on pseudo-years resampled from v1's real days | — (properties of the rule under v1's measured noise, cited, not thresholds) |
| Flat steps alone within 1.62 | *Reading of a miss*: the miss bound itself, so a named reading needs the steps that cleared to carry what makes the spread a miss | Any tighter bound sends a reading to *other* while its cleared steps still make the miss on their own; none: a spread carried by flat steps would be named by whichever short step cleared (the review's 10.7 / 10 / 10 / 10 / 6 % read jumps for 57.9 % of its misses under its superseded noise model, from a 1d low inside its ±0.56 4h→1d band) |
| 8 h funding cycle | ADR-041: the venue's funding settlement every 8 h, a fixed intraday time | — (a fact of the venue, named in the 15m reading) |
| Seed 151, SplitMix64 | *Accept when*: the issue's number, named before the run so it cannot be chosen by its outcome; SplitMix64 (Steele, Lea and Flood 2014) is fully specified and pinned by its published seed-0 output | — (any fixed seed; another moves the SE by about the 0.71 %) |
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
  that reason, not from a refit; in the *other* case, from a new mechanism
  argument.

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
- **Miss criterion (the year):** not accepted when both hold:
  max_H N(H) ÷ min_H N(H) > **1.62**, and the log-spread
  ln(max ÷ min) lies outside the **2-SE** noise band, that is, exceeds
  twice its standard error from the block bootstrap below. Either one
  alone does not miss.
  - *Source of 1.62:* √(16.338 / 6.191) = 1.6245, rounded down. The
    hypothesis says v1's horizon dependence comes from the
    estimator–horizon mismatch. "Comes from" is taken to mean that v2
    removes more than half of v1's log-spread (ln 2.639 = 0.970, so at
    most 0.485 remains).
  - *What breaks at 1.62:* a bound of 2.64 or more accepts a rule no
    flatter than v1. A bound alone, without the band, can miss a true
    prediction from noise, because the noise floor is not known in
    advance.
  - *Why the floor is not derived:* if the about 350 disjoint daily
    windows after the warm-up were independent, a 10 % share would have
    a standard error of 1.6 pp (16 % relative), and two of them in log,
    e^(2 · 0.16) ≈ 1.38, would sit safely below 1.62. They are not
    independent: consecutive days' 1d races share most of ATR_1d's
    14-day memory, and daily volatility is persistent, so their
    `Neither` outcomes are positively correlated and N(1d) varies more
    than a binomial share. Measured on v1 (*The noise, measured on v1*),
    daily volatility has a weekly cycle and, without it, an
    autocorrelation of 0.72 at one day that falls to 0.12 at 28 days and
    none at 56, and the floor of the widest pair is 1.26,
    below 1.62: ln 1.62 = 0.48 is 2.1 times its 2 SE of 0.23. v1's 1d lacks ATR_1d's
    memory, so v2's floor is not known in advance either: the band
    measures it on the year itself.
  - *The band:* `label_measure` counts `Neither` and the races that ran
    per horizon per UTC day of the decision, empty days included. A
    circular block bootstrap (Politis and Romano 1992) resamples those
    days in blocks of **56** consecutive days, wrapping at the end, from
    uniform starts until a resample holds as many days as the year, and
    sums the max and the min horizon over the same days, so their
    correlation stays in. Over **10 000** resamples from SplitMix64 at
    seed **151**, the SE is the standard deviation of
    ln(N*(max) ÷ N*(min)). A resample without a race or a `Neither` at
    either horizon has no log-spread. While such resamples are at most
    **2.3 %** of all (230 of 10 000), the SE is taken over the others;
    above that it is undefined and counts as outside the band, since a
    horizon that more than one resampled year in 43 lacks every `Neither`
    of (a resample needs every one of its blocks empty for that) is not at a
    flat 10 %. Each number's source and what breaks at another value are
    in D12′. The report prints the SE, the undefined resamples, the
    log-spread, both conditions and the miss.
  - Under a lognormal ρ at median 1, 1.62 allows about ±0.08 of difference
    in the standard deviation of log ρ between horizons.
- **Reading of a miss (pre-registered):** the shape of the five N(H),
  5m to 1d, read from its four steps after noise is taken out.
  - *Steps.* The step from H_i to H_(i+1) is a **rise** when
    ln N(H_(i+1)) − ln N(H_i) exceeds **2** standard errors of that
    log-difference, a **fall** when it is below minus two of them, and
    **flat** otherwise. The SE is the band's bootstrap applied to the pair:
    the same UTC days, 56-day blocks, 10 000 resamples, seed 151 and limit
    on undefined resamples, so all five SEs of a run come from the same
    resamples, and the steps' correlation (measured up to +0.66 on v1) is
    in them. An undefined SE counts as outside, and the step takes its
    sign. Raw signs are not used: a step between two horizons that are
    equal in truth gets a coin-flip sign, so the case a shape lands in
    would be chance at every such step.
  - *Where a mechanism shows.* The bands widen with H; measured on v1 one
    SE of the four steps is 0.0091, 0.0191, 0.0348 and 0.0915 in log. A
    mechanism that acts at every step therefore clears the
    short steps first, and a flat step at the short end with a departure
    further up reads as a mechanism that does not act at the short end.
  - *Cases.* Each case is a set of step sequences (U rise, D fall,
    F flat, 5m→15m first). The sets are disjoint, every one of the 81
    sequences falls in exactly one of them, and `label_measure` prints the
    steps and the case.

    | Case | Steps | Reading, and why these steps |
    |---|---|---|
    | Fall from 5m (4 sequences) | 5m→15m falls; falls only, then flat steps only (DFFF, DDFF, DDDF, DDDD) | **Jumps.** A jump adds its size to one bar's true range whatever H is, while the diffusive range grows with √H, so jumps inflate ATR_H, and widen the band, most at short H. No other listed mechanism makes N fall at 5m→15m. The falls fade into flat steps where the excess drops inside the widening bands. A fall after a flat step is not this signature: the jump excess only shrinks with H, so it cannot clear a wider band after it stayed inside a narrower one |
    | Rise below 1h (6) | 15m→1h rises; no step falls; 5m→15m or 4h→1d rises too (UUFF, UUUF, UUFU, UUUU, FUFU, FUUU) | **Finite volatility memory.** If volatility clustering has a characteristic time, the scale's memory of 14 bars (70 min at 5m, 14 days at 1d) outlasts it more the longer H is, so the scale is staler, ρ disperses more and N rises at every step. The week cannot act below 1h (next case). A 15m→1h rise alone is also the rising side of a 24 h peak at 1h whose falls stayed inside their bands, so a second rise is required where the 24 h cycle leaves N flat (5m→15m) or makes it fall (4h→1d) |
    | Rise above 1h only (2) | 5m→15m and 15m→1h flat; 1h→4h flat or rising; 4h→1d rises (FFFU, FFUU) | **The week.** It acts on the 1d window and at most the 4h window (a 4h scale's 56 h memory can span a weekend), so it cannot move N from 5m to 1h. The 4h→1d step has to rise: a 1h→4h rise alone is also the rising side of a 24 h peak at 4h whose fall stayed inside the widest band. A volatility memory long enough to leave the 5m to 1h scales fresh (beyond the 1h scale's 14 h) gives the same steps; the shape cannot tell it from the week, and this is the one case where a named reading can stand for another listed mechanism |
    | Peak at 15m (1) | UDFF | **Intraday sessions.** The 15m scale's memory of 3.5 h spans a session open or a funding time of the 8 h cycle while a 15m window sits on one side of it. Both sides must clear their bands (a lone rise is the previous ambiguity), and 1h on must be flat, since sessions act through the 15m scale only |
    | Peak at 1h or 4h (5) | 5m→15m flat; rises only at 15m→1h or 1h→4h, falls only at 1h→4h or 4h→1d, at least one of each, every rise before every fall (FUDF, FUDD, FUFD, FUUD, FFUD) | **24 h seasonality.** At those horizons the scale's memory (14 h, 56 h) spans about one daily cycle while the window sits in one phase of it, so ρ disperses most there; at 5m and 15m the memory follows the phase (so 5m→15m is flat), and a 1d window spans the cycle (so 1d falls back) |
    | Fall from 5m, rise below 1h (2) | 5m→15m falls; 15m→1h and 4h→1d rise; 1h→4h does not fall (DUFU, DUUU) | **Jumps and finite volatility memory together**, the first for the short end, the second for the long end; 4h→1d has to rise because a 24 h peak would make it fall. The next version addresses both, each from its own case's reason |
    | Fall from 5m, rise above 1h only (5) | 5m→15m falls; 15m→1h does not rise; no step falls after one that does not; 4h→1d rises (DFFU, DFUU, DDFU, DDUU, DDDU) | **Jumps and the week together**, each for its end, with the week case's ambiguity at the long end. A 1h→4h fall after a flat step (DFDU) is neither: the jump excess only shrinks with H, and the week leaves 1h→4h flat or rising. The next version addresses both |
    | Other (56) | Every other sequence, all four flat included | **No pre-registered reading.** ADR-056 is not accepted, and no reading is pre-registered for the miss. The next version needs its own new mechanism argument; it may name a listed mechanism, on that new argument and not on this table. A miss whose spread is shared by steps none of which clears its band, or lies in flat steps (next item), lands here |

  - *Where the miss lies.* A case names its reading only when the miss
    lies in the steps that cleared their bands: the series rebuilt from
    the flat steps alone, every cleared step set to 0 in log, has
    max ÷ min of at most 1.62. Otherwise the reading is *other*, whatever
    the steps. A flat step can carry the miss on its own when its band is
    wider than ln 1.62 = 0.48, and a case keyed on a short step that
    cleared would then name the mechanism of a miss made elsewhere. On
    v1's measured noise it cannot: the widest band, 4h→1d, is ±0.18, and
    all four flat steps at their band edges add up to 0.31. The rule
    guards v2's 1d, whose 14-day scale memory v1 lacks; the review's
    superseded noise model had a ±0.56 4h→1d band, under which a 5m 7 %
    high (inside the ±0.08 the bound allows) with a 1d 40 % low
    (10.7 / 10 / 10 / 10 / 6 %) read jumps for 57.9 % of its misses before
    this rule. `label_measure` prints the flat steps' ln(max ÷ min) next to
    the case.
  - *What the rule achieves.* Measured on v1's noise
    (`measure_label_noise_from`, *The noise, measured on v1*): 200 000
    pseudo-years per truth, each a block resample of the year's real days
    (56-day blocks, seed 151; the first 10 000 are the band's) with its
    ln N*(H) shifted by the truth's log minus the year's, so it keeps the
    year's noise, serial dependence and cross-horizon correlation; the
    miss by both conditions and the reading with the measured SEs fixed.
    Of the misses:
    - the week on 1d only (10 / 10 / 10 / 10 / 18 %, misses in 87.7 % of
      the draws): the week 91.7 %, other 4.1 %, a wrong mechanism 4.2 %;
    - the week on 4h and 1d (10 / 10 / 10 / 12 / 18 %, 83.3 %): the week
      92.5 %, other 3.4 %, a wrong mechanism 4.1 %;
    - jumps with a flat tail (18 / 13 / 10.5 / 10 / 10 %, 99.8 %): jumps
      98.4 %, other 0.2 %, a wrong mechanism 1.4 %;
    - a 5m excess inside the bound with a 1d low (10.7 / 10 / 10 / 10 /
      6 %, 82.1 %), a miss no listed mechanism makes: other 100.0 %, a
      wrong mechanism 0.0 %. Its 5m→15m step, ln 1.07 = 0.068, is 7.4 step
      SEs and falls in nearly every draw, but the 4h→1d fall that follows
      a flat step is no listed signature;
    - a flat truth misses in 14 of 200 000 draws (0.007 %), and 1 of
      those names a reading: the measured floor (1.26) lies far below
      1.62.

    These rates carry v1's noise around v2's truths (caveat in *The
    noise, measured on v1*); v2's wider 1d noise would lower the week's
    share and raise *other*.

  The next version follows from that reason, not from a refit.
- **Reported, no threshold:**
  - per horizon, N as a share of all labels and of the races that ran,
    next to the prediction and v1's share;
  - the spread, the bootstrap's SE, whether each miss condition holds,
    the miss, and the shape with its reading;
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

## Measurement

Measured 2026-10-28; **accepted**. Both runs used the `label_measure`
ignored tests (`measure_labels`, `measure_labels_live`) of main `5f9d9e6`
(the merge commit of this ADR's code, #153), release build, with the engine
key `4c844593779da2e3-1`, the sampling policy `conditional.sampling@1`,
fingerprint `2c71fff23e8b0415`, and the labeling policy
`conditional.labels@2`, fingerprint `6f90bff079e746f2`. The year run covered
2025-10-01 00:00 UTC (inclusive) to 2026-10-01 00:00 UTC (exclusive), the
window `1759276800000..1790812800000` ms, with
`MIE_MEASURE_EVENTS=603323345`; its provenance data version is
`5d9e27f862ac54dd93cff904458e9ee92767d5105a9d13a144351cba069f224d`. The live
run replayed ADR-051's R0 live store, the one cleanly ended run
`20261016T225606Z`, with the data version
`9348410a56c0d05d469f3f7e78c1c405d80db333d9d34df694b45ce256caca2d`. Every
number below is from these two runs. The prediction, the miss criterion and
the reading of a miss were on main before either run (*Accept when*).

**Criteria**, each checked against the printed reports:

- **T0 (asserted): met.** The year replayed 603 323 345 events, the
  expected count, with 0 rejections and no trailing gaps; 0 mismatches
  (order 0) in 44 855 of 561 695 labels checked (every trigger record's
  label plus the baseline labels at whole UTC hours). The live run: 2 042 225
  events, 0 rejections, 0 mismatches (order 0) in 2 185 of 2 185 labels.
- **Completeness (asserted): met.** 561 695 records and 561 695 labels in
  the year (`opened` 561 695, `emitted` 561 695); 2 185 records and 2 185
  labels on the live store (`opened` 2 185, `emitted` 2 185), 0 incomplete
  runs.
- **Scale cross-check (asserted): met.** The model's ATR_1h equals the
  record's `state.atr` in readiness and value: 0 mismatches, ready and equal
  at 560 796 records in the year and at 1 338 on the live store; `scale_breaks`
  0 on both.
- **1h identity: holds.** v2's 1h `Neither` and `Unavailable` counts are
  56 372 and 899 on the year and 49 and 847 on the live store, equal to
  ADR-055's published counts.
- **v1 reproducibility:** `conditional.labels@1` still derives
  `604b624cc2522df9`, pinned by `GOLDEN_LABELING_FINGERPRINT` in the label
  tests, which CI runs; this measurement did not re-run it.
- **Prediction and miss criterion (the year): not a miss.** N(H) is
  `Neither` ÷ the races that ran; predicted 10.05 % at every H. The spread
  max ÷ min over the five horizons is **1.5811** (4h over 5m), within the
  **1.62** bound. Its log-spread, 0.4581, lies outside the 2-SE band (the
  bootstrap's SE of ln(max ÷ min) is 0.0582, 0 undefined resamples; 56-day
  blocks, 10 000 resamples, seed 151), so one condition holds and the
  other does not; a miss needs both. The live store is not evaluated for the
  miss (*Reachability*).

**Verdict.** T0, completeness, the scale cross-check and the 1h identity
are met, and the prediction is not missed, so this ADR is **accepted**.

**Per horizon, the year** (561 695 labels). N is `Neither` as a share of
all labels and of the races that ran (`Upper` + `Lower` + `Neither`), next
to the prediction (10.05 % at every H) and v1's share of all labels.
Censored windows by reason `gap` / `stale_reference` / `rejected` /
`end_of_data`, as a share of the labels (counts in brackets where nonzero);
clear counts are baseline / trigger.

| H | Neither, of all labels | N, of races that ran | v1, of all labels | Censored | Unavailable | same_ms | Clear baseline / trigger |
|---|---|---|---|---|---|---|---|
| 5m | 43 925 (7.820 %) | 7.821 % (43 925 of 561 620) | 16.338 % | 0 / 0 / 0 / 0.001 % (5) | 74 (0.013 %) | 0 | 94 333 / 25 519 |
| 15m | 44 593 (7.939 %) | 7.942 % (44 593 of 561 465) | 12.548 % | 0 / 0 / 0 / 0.003 % (17) | 224 (0.040 %) | 0 | 33 758 / 22 828 |
| 1h | 56 372 (10.036 %) | 10.053 % (56 372 of 560 765) | 10.036 % | 0 / 0 / 0 / 0.011 % (62) | 899 (0.160 %) | 0 | 8 678 / 16 654 |
| 4h | 68 977 (12.280 %) | 12.366 % (68 977 of 557 794) | 9.281 % | 0 / 0 / 0 / 0.044 % (245) | 3 670 (0.653 %) | 0 | 2 185 / 9 815 |
| 1d | 56 383 (10.038 %) | 10.471 % (56 383 of 538 492) | 6.191 % | 0 / 0 / 0 / 0.276 % (1 548) | 22 724 (4.046 %) | 0 | 365 / 3 604 |

`reach_backs`: 0 and `scale_breaks`: 0 in the year.

**Per horizon, the live store** (2 185 labels), same columns (the 4h and
1d races are all `Unavailable`, so their N has no races):

| H | Neither, of all labels | N, of races that ran | v1, of all labels (year) | Censored | Unavailable | same_ms | Clear baseline / trigger |
|---|---|---|---|---|---|---|---|
| 5m | 213 (9.748 %) | 10.157 % (213 of 2 097) | 16.338 % | 0.915 % (20) / 0.275 % (6) / 0 / 0.229 % (5) | 73 (3.341 %) | 0 | 373 / 82 |
| 15m | 133 (6.087 %) | 6.873 % (133 of 1 935) | 12.548 % | 2.838 % (62) / 0.275 % (6) / 0 / 0.778 % (17) | 213 (9.748 %) | 0 | 134 / 74 |
| 1h | 49 (2.243 %) | 3.781 % (49 of 1 296) | 10.036 % | 11.533 % (252) / 0.275 % (6) / 0 / 2.929 % (64) | 847 (38.764 %) | 0 | 35 / 58 |
| 4h | 0 (0.000 %) | none (0 of 0) | 9.281 % | 40.092 % (876) / 0.275 % (6) / 0 / 4.851 % (106) | 2 185 (100.000 %) | 0 | 9 / 29 |
| 1d | 0 (0.000 %) | none (0 of 0) | 6.191 % | 94.874 % (2 073) / 0.275 % (6) / 0 / 4.851 % (106) | 2 185 (100.000 %) | 0 | 2 / 16 |

`reach_backs`: 497 on the live store. The live N is not comparable with the
year's: one run of under 60 h holds 3 UTC days, and most 1h labels are
censored (11.533 %) or `Unavailable` (38.764 %). The tool also printed a
spread of 2.6865 (5m over 1h) for the three live horizons with races and an
SE of 0 from the 3 days; neither is a criterion, the miss is evaluated on
the year only.

**Shape of N(H), the year.** The five steps against their own 2-SE bands
(the bootstrap applied to each pair; U rise, D fall, F flat):

| Step | ln change | SE | Step |
|---|---|---|---|
| 5m → 15m | +0.0154 | 0.0158 | F |
| 15m → 1h | +0.2356 | 0.0323 | U |
| 1h → 4h | +0.2071 | 0.0369 | U |
| 4h → 1d | −0.1664 | 0.0620 | D |

The sequence is FUUD. The series rebuilt from the flat step alone has
max ÷ min of 1.0155. The tool prints the case "peak at 1h or 4h: 24 h
seasonality".

**Cost.** The year run took a process wall time of 1:20:17 (the tool reports
4 817.3 s) at a peak RSS of 1 473 824 kB, for 603 323 345 events. The live
run took a process wall time of 1:09 (the tool reports 41.2 s) at a peak RSS
of 6 247 756 kB.

**Observations, recorded and not concluded.**

- N(H) is not flat. Three of the four steps clear their 2-SE bands (FUUD):
  N rises 15m → 1h and 1h → 4h and falls 4h → 1d. The five values run from
  7.821 % (5m) up to 12.366 % (4h) and back to 10.471 % (1d), against the
  predicted 10.05 % at every H.
- v2 removes 53 % of v1's log-spread: ln 1.5811 = 0.458 against v1's
  ln 2.639 = 0.970, which is why the spread passes the 1.62 bound (that
  bound asks for more than half). The noise band, separately, reports the
  remaining spread as larger than noise alone would give.
- The shape matches the pre-registered "peak at 1h or 4h: 24 h seasonality"
  reading. *Accept when* pre-registers that reading for a miss only. This
  run is not a miss, so it is recorded here as an observation and as the
  candidate for the next labeling version, not as a conclusion of this ADR.
  Whether the reading holds would need that version's own mechanism
  argument and its own pre-registered run.
- ADR-055's restriction stands: no upper-vs-lower shares, excursion or time
  quantiles, and no split by trigger, regime or location are reported
  (*Accept when*).
