# BTC Market Intelligence Engine
## Research & Architecture Brief — v7

**Scope:** BTCUSDT perpetual MVP  
**Status:** Architecture baseline  
**Primary architectural principle:** Hexagonal Architecture (Ports & Adapters)

## 1. Vision

Build a deterministic market-intelligence and research system for BTCUSDT perpetual that first maps the market, learns conditional market behavior, and only afterwards discovers, validates and selects strategies.

The system is not a “super strategy”. It is a measurable research machine that can discover which combinations of market state, regime, location and trigger contain repeatable edge.

## 2. Core principle

**Market State → Regime → Conditional Behavior → Bias → Location → Trigger → Execution → Outcome → Research Feedback**

The system must distinguish:

- **Bias:** what the broader evidence suggests.
- **Location:** where the market is relative to meaningful levels.
- **Trigger:** what observable event authorizes execution.
- **Execution:** entry, invalidation, target and management.
- **Outcome:** what actually happened.
- **Research:** whether the hypothesis and its components add measurable edge.

No trading concept is accepted merely because it is intuitively plausible.

## 3. Explicit architectural principle — Hexagonal Architecture

The implementation should follow the principles of **Hexagonal Architecture / Ports & Adapters**.

Order Flow / Aggression is a domain-facing capability fed by exchange-native trade and depth adapters. Bookmap is explicitly not a runtime dependency.

The central MIE domain must not depend directly on Binance, TradingView, PostgreSQL, DuckDB, Telegram, LLM providers, exchange SDKs or other infrastructure.

### Domain core

The domain core contains the logic that should remain stable when infrastructure changes:

- Market State
- Regime
- Level / Location
- Bias
- Trigger
- Conditional Market Behavior
- Strategy hypotheses
- Strategy Suitability
- Risk
- Validation rules
- Edge / degradation evaluation

### Application layer

Application services orchestrate use cases without owning external technology details:

- analyze current market
- replay historical market state
- run research experiment
- run backtest
- run walk-forward validation
- evaluate strategy suitability
- record research results
- generate an observability report

### Inbound ports

Examples:

- `AnalyzeMarket`
- `ReplayMarket`
- `RunResearchExperiment`
- `EvaluateStrategy`
- `ValidateCandidate`
- `GenerateMarketReport`

### Outbound ports

Examples:

- `MarketDataProvider`
- `HistoricalDataProvider`
- `FeatureStore`
- `StrategyRepository`
- `BacktestEngine`
- `ExecutionGateway`
- `AlertGateway`
- `ResearchResultStore`

### Adapters

Initial adapters may include:

- Binance market-data adapter
- Binance historical/replay adapter
- Parquet adapter
- DuckDB adapter
- TradingView/reference-definition adapter
- Telegram adapter
- LLM/agent adapter
- backtest-engine adapter

The core must be runnable without any single external adapter. This enables live processing, historical replay, backtesting and forward testing to use the same domain logic.

## 4. Target architecture

```text
                         EXTERNAL WORLD
        Binance / TradingView / Parquet / Telegram / LLM
                           │
                     inbound/outbound
                        ADAPTERS
                           │
                     ┌─────▼─────┐
                     │   PORTS   │
                     └─────┬─────┘
                           │
              ╔════════════▼════════════╗
              ║       MIE CORE          ║
              ║                          ║
              ║ Market State             ║
              ║ Regime                   ║
              ║ Location / Auction       ║
              ║ Bias                     ║
              ║ Trigger                  ║
              ║ Conditional Behavior     ║
              ║ Strategy / Suitability   ║
              ║ Risk / Validation        ║
              ╚════════════▲════════════╝
                           │
                         PORTS
                           │
                         ADAPTERS
```

The architecture is deliberately dependency-inverted: infrastructure depends on domain-defined ports, not the other way around.

## 5. Live/replay path

```text
Binance → ingestion adapter → MarketDataPort
        → deterministic Market State
        → Regime
        → Location / Bias
        → Trigger monitoring
        → Strategy Suitability
        → Observability / Egress
```

Historical replay uses the same domain processing path:

```text
Parquet → HistoricalDataAdapter → same MarketDataPort
        → same Market State / Regime / Strategy logic
```

This is a critical design requirement: live and historical processing must not contain separate implementations of market logic.

## 6. Research path

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
Refutation
   ↓
Validation
   ↓
Walk-forward
   ↓
Unseen forward test
   ↓
Strategy Registry
   ↓
Live feedback
```

Agents/LLMs may propose hypotheses, plan experiments, synthesize results and challenge assumptions. Deterministic data, replay and validation remain authoritative.

## 7. Data Plane

Scope: BTCUSDT perpetual, one primary consumer.

Initial path:

`Binance → Rust ingestion → immutable raw Parquet → DuckDB replay/research`

Raw candidates:

- trades
- depth/order-book events
- klines/OHLCV
- open interest
- funding
- liquidations

Raw data is the source of truth. Derived features must be reproducible from raw data.

## 8. Market State

Canonical deterministic representation of current conditions.

Initial features:

- price, returns, velocity and range
- ATR(14)
- ATR percentile(200)
- OI, ΔOI and OI velocity
- trade delta and CVD
- bid/ask volume
- order-book imbalance
- liquidity added/removed and concentration
- absorption/exhaustion candidates
- volume profile: POC, VAH, VAL, HVN, LVN
- structural highs/lows
- SFP / sweep detection
- CVD/OI/price divergences
- effort-vs-result relationships
- multi-timeframe context

## 9. Regime

Initial ATR percentile regime:

- **LOW:** 0–25
- **MEDIUM:** 26–50
- **HIGH:** 51–75
- **EXTREME:** 76–100

The raw percentile is retained in addition to the label.

ATR percentile is contextual information, not a direct entry signal.

Future regime dimensions must be validated empirically: trend/range/transition, compression/expansion, directional efficiency, liquidity environment, OI, funding, session/time-of-day, location and regime duration.

## 10. Location and auction state

Maintain and score:

- POC
- VAH / VAL
- HVN / LVN
- structural highs/lows
- liquidity clusters
- prior sweeps
- SFP/rejection zones
- VWAP and other validated levels

For range trading, explicitly distinguish:

- inside value
- at value edge
- outside value
- breakout
- failed breakout / failed auction
- failed reclaim
- acceptance

The system continues to monitor a location even when no trade is authorized.

## 11. Bias / Location / Trigger separation

A directional bias does not authorize execution.

Example:

```text
Bias:
Range + price near VAH + HIGH volatility

Location:
VAH / upper value edge

Trigger candidates:
- failed auction
- SFP
- absorption
- aggression with no price progress
- CVD divergence
- OI expansion followed by rejection
- breakout + acceptance
```

The same bias and location can therefore be tested with multiple trigger families.

## 12. Conditional Market Behavior

Before strategy optimization, measure what tends to happen after a given Market State + Regime + Location + Trigger context.

Outputs can include:

- upside/downside threshold first
- MFE / MAE
- time-to-event
- level revisit probability
- invalidation probability
- excursion distribution
- outcome conditional on volatility regime
- outcome conditional on location and trigger

This layer is not a BUY/SELL classifier.

## 13. Strategy Research Lab

The lab continuously:

1. generates hypotheses
2. formalizes them
3. runs deterministic replay/backtests
4. evaluates feature contribution
5. performs counter-hypothesis/refutation tests
6. evaluates confirmation cost
7. validates survivors
8. runs walk-forward tests
9. runs unseen forward tests
10. registers validated candidates
11. monitors live degradation

No feature is assumed useful merely because it sounds plausible.

## 14. Strategy Registry & Suitability

Lifecycle:

`RESEARCH → BACKTESTED → WALK_FORWARD → PAPER → VALIDATED → ACTIVE → DEGRADING/SUSPENDED → DEPRECATED`

At runtime:

```text
Current Market State
        +
Current Regime
        +
Current Location
        +
Available validated triggers
        ↓
Strategy Suitability
```

The system may legitimately return:

**NO SUITABLE STRATEGY**

## 15. Edge decay and risk

A validated strategy can stop working.

Track:

- rolling expectancy
- profit factor
- win rate
- MFE/MAE
- drawdown
- performance by regime
- performance by location
- performance by trigger
- deviation from validation distributions

Risk is a separate subsystem and must be testable.

## 16. LLM / Agent boundary

LLMs are research operators and adapters, not numerical authorities.

Allowed:

- hypothesis generation
- research planning
- result synthesis
- challenge/refutation
- experiment prioritization

Not authoritative for:

- raw market facts
- deterministic feature values
- backtest metrics
- validation decisions

## 17. Egress

First practical egress: Telegram.

Example:

```text
Alert: BTC entered a monitored VAH zone.
Guide: Failed-auction short is only confirmed if the defined
acceptance/rejection conditions occur.
Invalidation: explicit level/condition.
```

Autonomous execution remains outside the MVP.

## 18. Two feedback loops

**Market loop**

`Binance → Market State → Regime → Observation → Outcome → Knowledge`

**Research loop**

`Knowledge → Hypothesis → Experiment → Validation → Registry → Suitability → Live feedback`

## 19. First milestone

`Binance → Rust → raw Parquet → deterministic Market State → ATR/OI/CVD/VP/structure → Regime → conditional outcome dataset → DuckDB replay/research → Strategy Research Lab → Telegram`

The first implementation should validate the architecture, not prematurely optimize a trading strategy.

## 20. Explicitly deferred

- multi-asset scale
- ClickHouse
- autonomous execution
- one fixed strategy
- premature parameter optimization
- LLM-driven numerical truth
- unvalidated discretionary concepts
- strategy selection based only on backtest performance
