# Architecture Decision Records — BTC Market Intelligence Engine
## Updated baseline — v4

**Scope:** BTCUSDT perpetual MVP

## ADR-001 — Deterministic market-data pipeline
**Status:** Accepted

Use `Binance raw market data → Rust deterministic feature engine → structured Market State`.

LLMs/agents may interpret structured state but are not the source of numerical market facts.

## ADR-002 — BTCUSDT perpetual only for MVP
**Status:** Accepted

The first implementation targets BTCUSDT perpetual and one primary consumer.

## ADR-003 — Raw data stored as Parquet
**Status:** Accepted

Persist immutable raw market data as Parquet, partitioned by source/instrument/time.

## ADR-004 — DuckDB before ClickHouse/PostgreSQL
**Status:** Accepted

Use DuckDB for analytical SQL and replay over Parquet. Add server databases only when workload proves the need.

## ADR-005 — Market State and Strategy are separate subsystems
**Status:** Accepted

Market State, Regime, Conditional Market Behavior and Strategy Research remain separate layers.

## ADR-006 — Conditional Market Behavior precedes Strategy Discovery
**Status:** Accepted

First measure conditional outcomes of Market State + Regime + Location. Only then formalize strategy hypotheses.

## ADR-007 — Automated research loop
**Status:** Accepted

Build a research loop for reproducible experiments, feature-contribution analysis, refutation, backtesting, walk-forward validation and forward testing.

## ADR-008 — Strategy Registry and lifecycle
**Status:** Accepted

Every candidate receives an ID, version, provenance and lifecycle state.

## ADR-009 — Strategy suitability instead of a universal best strategy
**Status:** Accepted

Evaluate validated strategies against current Market State + Regime + Location. The system may select no strategy.

## ADR-010 — Observability is not execution
**Status:** Accepted

First egress is informational. Autonomous execution remains outside the MVP.

## ADR-011 — Agent/LLM boundary
**Status:** Accepted

Agents/LLMs may generate hypotheses, plan research, synthesize results and perform challenge/refutation. Deterministic components remain authoritative.

## ADR-012 — Bias and trigger are separate
**Status:** Accepted

Bias does not authorize execution. A separate trigger must occur at a defined location.

## ADR-013 — Feature contribution must be measured
**Status:** Accepted

No feature is accepted because it is intuitively plausible. Incremental contribution must be measured.

## ADR-014 — Counter-hypothesis / refutation is mandatory
**Status:** Accepted

Important hypotheses must be tested against plausible alternative interpretations.

## ADR-015 — Confirmation has a measurable cost
**Status:** Accepted

Additional confirmation can improve win rate while worsening entry quality and expectancy. Confirmation depth must be tested.

## ADR-016 — Edge decay is a first-class state
**Status:** Accepted

Validated strategies can degrade. Rolling and regime-conditioned performance must be monitored.

## ADR-017 — Canonical ATR percentile scale
**Status:** Accepted

- LOW: 0–25
- MEDIUM: 26–50
- HIGH: 51–75
- EXTREME: 76–100

The raw percentile must be retained.

## ADR-018 — Hexagonal Architecture / Ports & Adapters
**Status:** Accepted

The MIE implementation follows Hexagonal Architecture.

The domain core must not depend directly on external infrastructure such as Binance SDKs, TradingView, databases, Telegram or LLM providers.

External systems connect through ports and adapters.

Examples of ports:

- `MarketDataProvider`
- `HistoricalDataProvider`
- `FeatureStore`
- `StrategyRepository`
- `BacktestEngine`
- `ResearchResultStore`
- `AlertGateway`

Infrastructure adapters implement these ports.

**Consequence:** live ingestion, historical replay, backtesting and forward testing can reuse the same domain logic while infrastructure components remain replaceable.

## ADR-019 — Same domain path for live and replay
**Status:** Accepted

Live and historical processing must use the same Market State, Regime, Location, Trigger and Strategy logic. Replay is an adapter/input difference, not a second implementation of the trading model.

## ADR-020 — LLMs remain outside the domain authority
**Status:** Accepted

LLMs may operate through research/application adapters. They cannot become authoritative for numerical market facts, deterministic feature calculations, validation metrics or strategy promotion.


## ADR-021 — Bookmap-like Order Flow without Bookmap dependency
**Status:** Accepted

The MIE must ingest exchange-native trades and depth/order-book events sufficient to reconstruct aggressive buy/sell flow and its interaction with passive liquidity.

Bookmap is a reference visualization/mental model, not a runtime dependency.

The domain must work with deterministic concepts such as:

- aggressive buy volume
- aggressive sell volume
- delta
- CVD
- trade intensity
- liquidity added/removed
- passive liquidity concentration
- price response to aggression
- absorption
- exhaustion
- effort vs result

These are domain features and hypotheses, not discretionary chart labels.

## ADR-022 — Preserve raw order-flow events
**Status:** Accepted

The Data Plane must preserve raw trades and depth/order-book events rather than reducing the source exclusively to OHLCV aggregates.

Derived order-flow features must be reproducible from versioned raw data and feature definitions.

## ADR-023 — Aggression is not direction
**Status:** Accepted

Aggressive buying/selling alone does not determine future price direction.

Research must evaluate aggression together with:

`Location + Passive Liquidity + Price Response + Regime + Auction State`

For example, strong aggressive buying with little price progress may represent absorption rather than continuation.

## ADR-024 — Order flow feeds triggers, not strategy authority
**Status:** Accepted

Order-flow features may contribute to Failed Auction, Absorption, Exhaustion, Effort-vs-Result, SFP, Failed Reclaim and Breakout Acceptance hypotheses.

They do not directly authorize execution without the defined trigger and validation pipeline.
