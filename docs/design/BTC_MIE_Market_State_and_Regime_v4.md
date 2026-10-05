# BTC Market Intelligence Engine — Market State & Regime
## Subsystem brief v2

**Architectural role:** Domain core

## Purpose

Turn deterministic market observations into a canonical representation of current market conditions.

The subsystem must not depend on Binance, TradingView, DuckDB or an LLM.

## Market State

## Order Flow / Aggression State

Market State includes a deterministic order-flow representation derived from raw trades and depth events.

Minimum state:

- aggressive buy volume
- aggressive sell volume
- delta
- CVD
- trade intensity
- large-trade activity
- bid/ask imbalance
- liquidity added/removed
- liquidity concentration
- price response to aggression

The state distinguishes aggressor flow from passive resting liquidity and from the resulting price response.

This enables deterministic candidates for absorption, exhaustion, initiative flow and effort-vs-result. These remain hypotheses until validated by Conditional Market Behavior and Research.

Bookmap-like visualization is not required; the underlying exchange data and reproducible features are.

Initial features:

- price, returns, velocity and range
- ATR(14)
- ATR percentile(200)
- OI / ΔOI / OI velocity
- trade delta and CVD
- bid/ask volume
- order-book imbalance
- liquidity added/removed and concentration
- absorption/exhaustion candidates
- volume profile: POC, VAH, VAL, HVN, LVN
- structural highs/lows
- SFP / sweeps
- CVD/OI/price divergences
- effort-vs-result
- multi-timeframe context

All features require deterministic definitions so live processing and historical replay produce equivalent results.

## ATR percentile regime

Canonical initial scale:

- **LOW:** 0–25
- **MEDIUM:** 26–50
- **HIGH:** 51–75
- **EXTREME:** 76–100

The raw percentile is always retained.

Example: `57` is **HIGH**, not MEDIUM.

ATR percentile is contextual information, not a direct entry signal.

## Location / auction state

The subsystem consumes/scorers levels including:

- POC
- VAH / VAL
- HVN / LVN
- structural highs/lows
- liquidity clusters
- prior sweeps
- SFP/rejection zones
- VWAP and validated reference levels

For range analysis distinguish:

- inside value
- at value edge
- outside value
- breakout
- failed breakout / failed auction
- failed reclaim
- acceptance

## Domain boundary

Market State and Regime describe the market.

They do not select a strategy and do not directly issue BUY/SELL decisions.

That separation allows multiple strategies and research hypotheses to consume the same deterministic state.
