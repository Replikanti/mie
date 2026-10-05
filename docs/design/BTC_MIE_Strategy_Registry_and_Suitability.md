# BTC Market Intelligence Engine — Strategy Registry & Suitability
## Subsystem brief v4

**Architectural role:** Domain service + application boundary

## Purpose

Maintain versioned strategy candidates and determine which validated strategies are appropriate for the current market.

## Lifecycle

`RESEARCH → BACKTESTED → WALK_FORWARD → PAPER → VALIDATED → ACTIVE → DEGRADING/SUSPENDED → DEPRECATED`

Every candidate should carry:

- strategy ID
- version
- hypothesis
- feature definitions
- data/version provenance
- experiment IDs
- validation evidence
- regime suitability
- location suitability
- trigger suitability
- performance history

## Order-Flow Suitability

Validated strategies may include explicit order-flow requirements, for example:

- minimum aggression threshold
- delta direction
- absorption condition
- price-response condition
- liquidity state

Suitability must evaluate these requirements from deterministic Market State, not from a visual chart interpretation.

## Suitability

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

## Edge decay

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

Possible states:

`HEALTHY → WATCH → DEGRADING → SUSPENDED → DEPRECATED`

## Hexagonal boundary

The Registry owns strategy identity and lifecycle semantics.

Persistence is provided through a `StrategyRepository` port.

Suitability consumes domain state and validated strategy definitions; it does not know whether those definitions came from PostgreSQL, files, DuckDB or another storage adapter.

## Principle

There is no universal best strategy.

The correct output can be:

`NO SUITABLE STRATEGY`
