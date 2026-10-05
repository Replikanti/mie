# BTC Market Intelligence Engine — Conditional Market Behavior
## Subsystem brief v4

**Architectural role:** Domain + application service

## Purpose

Measure how the market behaves after particular combinations of state, regime, location and trigger without prematurely turning observations into a strategy.

## Inputs

- Market State
- Regime
- Location / level context
- Trigger candidates or confirmed trigger definitions

## Order-Flow Conditional Behavior

Conditional analysis should explicitly test interactions between:

- aggressive buy/sell volume
- delta/CVD
- passive liquidity
- price displacement
- location (especially VAH/VAL and structural extremes)
- regime

Example hypotheses:

- high aggressive buying + weak price response → absorption/reversal
- high aggressive selling + weak price response → absorption/reversal
- high aggression + strong price response → continuation
- high aggression + failure to achieve acceptance → failed-auction continuation/reversal alternatives

The research layer must measure these outcomes rather than encode them as assumptions.

## Outputs

Measure distributions such as:

- upside threshold reached first
- downside threshold reached first
- MFE / MAE
- time-to-event
- level revisit probability
- invalidation probability
- excursion distribution
- outcome by volatility regime
- outcome by location
- outcome by trigger family

## Important separation

Conditional behavior is not a BUY/SELL classifier.

It answers:

> Given this measurable context, what tends to happen next?

Strategy Research later asks:

> Can this conditional behavior be converted into a repeatable, risk-adjusted strategy?

## Hexagonal role

The subsystem consumes domain ports rather than infrastructure.

The same conditional-behavior calculation must work from live state and historical replay.

## Research principle

Do not assume:

- failed auction always reverses
- absorption always reverses
- OI expansion always continues
- low ATR always precedes a breakout

Each relationship is an empirical hypothesis to be tested.

## Counter-hypothesis

For important observations, test the opposite interpretation as well.

Example:

`failed breakout → reversal`

must be compared with:

`failed breakout → delayed continuation`

The research engine should actively seek evidence that disproves a hypothesis.
