# ADR-027: Fixed-point integer prices and quantities

- Status: proposed
- Date: 2026-10-05

## Context

Binance publishes prices and quantities as decimal strings. Market State
needs exact accumulation (delta, CVD, volume at price) and exact level
comparison (volume-profile bins, swing levels), identical between live and
replay (ADR-019, ADR-022). Binary floating point represents most decimal
prices inexactly, and accumulated rounding depends on evaluation order.

## Decision

`Price` and `Qty` are `i64` counts of `10^-8` units: one fixed scale for every
instrument, independent of the exchange tick and step sizes (which change
over time). Products such as notional widen to `i128`. Floating point is
allowed only in derived statistics (ratios, percentiles, distributions),
computed from the exact inputs in a defined order.

## Consequences

- Sums and comparisons are exact; replay reproduces live state bit for bit.
- Parsing must reject inputs with more than 8 decimal places instead of
  rounding them silently.
- Range: about ±9.2·10^10 whole units — far beyond any BTC price or quantity.
- Arithmetic helpers must be explicit about overflow (checked or widened).

## Alternatives considered

- **`f64` everywhere** — simplest and ecosystem-friendly, but sums become
  inexact and `==` on prices compares representation artifacts.
- **A decimal crate (`rust_decimal`)** — exact, but a core dependency
  (ADR-025) and slower in hot accumulation loops.
- **Per-instrument tick/step units** — compact, but ties stored values to
  exchange metadata that changes over time.

## Accept when

Raw Binance trades round-trip exactly (decimal string → `Price`/`Qty` → raw
store → replay) and the first order-flow features (delta, CVD) are computed
on them.
