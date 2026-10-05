# BTC Market Intelligence Engine — Strategy Research Lab
## Subsystem brief v2

**Architectural role:** Application/research layer around the domain core

## Purpose

Discover, formalize, test, validate and monitor candidate strategies from measured market behavior.

The lab is not allowed to rewrite the domain's numerical truth.

## Research loop

```text
Knowledge
  ↓
Hypothesis
  ↓
Experiment
  ↓
Replay / Backtest
  ↓
Feature contribution
  ↓
Counter-hypothesis / Refutation
  ↓
Confirmation-cost analysis
  ↓
Validation
  ↓
Walk-forward
  ↓
Unseen forward test
  ↓
Strategy Registry
  ↓
Live degradation monitoring
```

## Experiment definition

Every hypothesis should specify:

- Market State filters
- Regime filters
- Location
- Trigger
- Entry
- Invalidation
- Target / management
- fees
- slippage
- funding
- latency assumptions
- sample period
- data version
- feature version

Results are immutable and carry provenance.

## Order-Flow Feature Contribution

Order-flow variables are first-class research features.

Experiments should be able to compare:

```text
Base:
Location + Failed Auction

Add:
+ aggressive buy/sell imbalance
+ delta/CVD
+ absorption
+ effort/result
+ passive liquidity response
```

Measure whether these features improve expectancy, robustness and entry quality.

Do not assume that a visually convincing Bookmap pattern has predictive value.

## Edge Contribution

Test a base hypothesis against incremental additions.

Example:

```text
Base:
VAH + failed auction

Test additions:
+ CVD divergence
+ OI condition
+ ATR regime
+ absorption
+ effort/result
+ SFP
```

Measure:

- sample size
- expectancy
- profit factor
- win rate
- MFE / MAE
- drawdown
- entry displacement
- time-to-target
- robustness by regime

More confirmation is not automatically better.

## Confirmation cost

Compare:

- early trigger
- trigger + one confirmation
- trigger + multiple confirmations
- candle-close confirmation

Account for worse entry price, missed trades and changed R/R.

## Agent / LLM role

Agents may:

- generate hypotheses
- plan experiments
- combine candidate features
- analyze results
- challenge/refute assumptions
- prioritize research

Agents may not become the authoritative source for numerical facts or performance metrics.

## Hexagonal architecture

The lab calls application/domain ports such as:

- `HistoricalDataProvider`
- `BacktestEngine`
- `StrategyRepository`
- `ResearchResultStore`

Concrete engines can be replaced without changing the research-domain rules.

## Anti-overfitting

Require:

- provenance
- train/validation separation
- walk-forward validation
- unseen forward testing
- minimum sample sizes
- leakage controls
- versioned features and data
