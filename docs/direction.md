# Direction: what the first measured answer decides

- Status: draft. The directions, the criteria and the methods that set the
  statistical thresholds are registered here. The personal inputs are
  filled in by the owner before the first Research Lab result is read.
- Date: 2026-10-20

## Why this exists

MIE may legitimately answer **NO SUITABLE STRATEGY**
([Registry & Suitability](design/BTC_MIE_Strategy_Registry_and_Suitability.md)).
What the project becomes depends on that answer, and deciding it after the
results are in invites the same bias the Research Lab guards against: the
result gets read to fit the preferred outcome. So the decision rule is
written down first, like an experiment definition, and the first complete
pass of milestone 1 ([brief §19](design/BTC_Market_Intelligence_Engine_Research_Brief.md))
is evaluated against it.

Nothing here changes the design. Execution stays out of the MVP (ADR-010),
and LLMs stay research operators (ADR-011).

## The three directions

| | Direction | What MIE is | Partner it needs |
|---|---|---|---|
| **A** | Trade it | An internal tool. The Telegram egress is the operator's own alert channel. Execution would need its own ADR. | None required. At most capital, or a risk and execution partner. |
| **B** | Sell the intelligence | Context, location, trigger, invalidation and the measured outcome distribution behind each alert, delivered to traders. The differentiator is that every claim is measured and the system can say "no". | A trader with an existing audience and trust in the order-flow community. |
| **C** | Sell the research machine | The deterministic data plane, replay, measurement and append-only experiment store, offered to funds, prop desks and quant researchers. | Someone with relationships in crypto funds and market makers. |

If none of the three finds interest, MIE stays a personal research tool.
That is an acceptable outcome, not a failure of the rule.

## How thresholds are set

There are two kinds, and neither is guessed.

- **Statistical thresholds** follow from data. This document registers the
  method, not the number; the Research Lab computes the number on the
  calibration slice.
- **Personal inputs** cannot be measured from the market. They come from
  the owner's risk tolerance, time and product judgment, and are written
  into the table below before the first result is read.

### Calibration slice

The earliest contiguous part of the history is reserved for calibration:
variance estimates and null distributions are computed there and nowhere
else. It is excluded from every evaluation below, so the thresholds never
see the data they judge. Its dataset version and date range are recorded
in the inputs table.

### Net outcome

Every outcome is measured in R, net of:

- **fees** at the owner's fee tier;
- **slippage** measured from the reconstructed order book where live depth
  exists (ADR-038). For the archive period, where only coarse depth bands
  exist ([data availability](data-availability.md)), from a slippage model
  calibrated on the live book and recorded with the experiment;
- **funding** from the derivatives context (ADR-042).

### Methods

| ID | Quantity | Method |
|---|---|---|
| T1 | Minimum sample | n ≥ (z·σ/μ)², where σ is the standard deviation of net outcomes on the calibration slice, μ the smallest net expectancy worth trading (input) and z follows from the confidence level (input). n counts episodes, not samples (ADR-054 D4): samples within one episode are correlated. |
| T2 | Net expectancy | The lower bound of a block-bootstrap confidence interval of net expectancy, at the confidence level, is above zero. It resamples whole episodes (ADR-054 D4) in time-ordered blocks, so serially dependent trades are not treated as independent. |
| T3 | Effect against baseline | The baseline is the unconditional baseline sample (ADR-054) in the same location context, measured as the probability of each threshold being reached first. The null distribution comes from permuting context labels within time blocks. A context counts when its effect exceeds the null at the false discovery rate (input), corrected with Benjamini–Hochberg across every context tested. |
| T4 | Stability | The effect keeps its sign in every walk-forward window and still passes T3 out-of-sample. |
| T5 | Forward test | Forward-test net expectancy does not fall below the lower tail of the walk-forward window distribution, at the confidence level. |
| T6 | Counter-hypothesis | The opposite interpretation was tested and lost (Conditional Behavior, "Counter-hypothesis"). |

### Personal inputs

| Input | Value |
|---|---|
| Confidence level (sets z, T2, T5) | 0.95 |
| False discovery rate (T3) | to be set |
| Smallest net expectancy worth trading, in R per trade (T1) | 0.10 |
| Fee tier | Binance USDⓈ-M VIP 0, taker on both sides (10 bps round trip) |
| Maximum acceptable out-of-sample drawdown, in R | to be set |
| Minimum frequency for A, setups per week | to be set |
| Minimum frequency for B, alerts per week | to be set |
| Waiting period for outside interest in C | to be set |
| Calibration slice, dataset version and date range | `c20761ef7e96acd21e3f6dae04cb9f2f20efb34c058dda1e775264e608f892ac`, 2024-10-01 … 2024-12-31 |

Why these values (set 2026-10-30, before any #28 outcome was read):

- **Confidence 0.95.** T2 and T5 use it as a lower bound, where caution
  is wanted. Until the Research Lab applies the false discovery rate (T3),
  it also keeps the chance readings per family low: about 5 in 100 cells
  instead of 10 at 0.90.
- **μ = 0.10 R.** A smaller edge would sit inside the cost uncertainty.
  The barrier half-widths measured on the #12 year (medians 4.7 / 8.9 /
  19 / 39 / 109 bps at 5m / 15m / 1h / 4h / 1d; barrier widths only, no
  race outcome read) put the taker round trip at 2.1 / 1.1 / 0.53 / 0.26 /
  0.09 R. Taking one leg as maker instead moves it by about 3 bps, which
  is 0.08 R at the 4h median. This value is a judgment: the data show only
  that a smaller edge is not worth trading.
- **Fee tier VIP 0, taker on both sides.** This is the worst realistic
  case with no volume commitment. Maker legs are a sensitivity, not the
  default.
- **Calibration slice.** It is the earliest contiguous part of the history
  (§ Calibration slice): the first quarter of the hold-out year. The #12
  year stays whole, and the walk-forward and forward hold-out is
  2025-01-01 … 2025-09-30 plus all live data. The version is the slice's
  format-2 `content` hash, built and verified in #28 M3a.

The drawdown is recorded in R only. The capital and the risk per trade it
is derived from stay out of this public repository.

## The evidence that decides

The evidence comes from Conditional Behavior outputs and from candidates
moving through the Registry lifecycle
(`RESEARCH → BACKTESTED → WALK_FORWARD → PAPER → VALIDATED`), on versioned
data and features, excluding the calibration slice.

**A holds when** at least one candidate reaches `VALIDATED` after
walk-forward and the unseen forward test, and it passes T1, T2 and T5
out-of-sample, stays within the maximum drawdown, and its suitable setups
reach the minimum frequency for A.

**B holds when** A does not, or A's setups are too rare to trade, but
contexts that pass T1, T3, T4 and T6 together recur at least at the
minimum frequency for B. That is information that improves a discretionary
decision even when it is not a stand-alone strategy.

**C holds when** neither A nor B does, but the machine itself holds up:
live/replay equivalence passes on the full history chain (ADR-041,
ADR-052), an experiment runs end to end reproducibly from a clean checkout,
and someone outside the project asks to use it within the waiting period
after the results are published.

## Rules

1. The methods and the personal inputs are fixed before the first Research
   Lab result is read. Until then this document may be edited freely.
2. After that, a method or an input changes only through a dated addendum
   at the end of this file that states the reason. The original line
   stays, as in the ADR lifecycle.
3. If A and B both hold, A comes first. B is added on top only if
   publishing does not degrade A's execution, for example by crowding the
   same levels.
4. A partner is sought only once a direction holds, with the profile that
   direction names above.
5. Before charging anyone for B's output, check whether it counts as advice
   on crypto-assets under MiCA, which is a licensed service, or as general
   market information.
6. The Research Lab implements T1 to T6. Until it does, this document is
   their specification.
