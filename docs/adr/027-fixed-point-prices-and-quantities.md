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
over time). `Rate` — a dimensionless ratio published by the exchange, such as
a funding rate — uses the same `i64` count of `10^-8` units. Products such as
notional widen to `i128`. Floating point is allowed only in derived
statistics (ratios, percentiles, distributions), computed from the exact
inputs in a defined order.

All three types parse from decimal strings with one grammar,
`-?[0-9]+(\.[0-9]+)?` over ASCII:

- Rejected as malformed: the empty string, `+`, exponents, whitespace, digit
  separators, a missing digit on either side of the point (`.5`, `5.`) and
  non-ASCII digits.
- Accepted: leading zeros, and `-0`, which parses to 0. The sign is accepted
  for every type, matching `Display`, so negative quantities (delta) round-trip.
  Whether a given exchange field may be negative is a normalization check in
  the adapter, not part of the grammar.
- Exactness: digits beyond the 8th decimal place are accepted only when they
  are all `0`, because they do not change the value. Any non-zero digit there
  is an error, never a rounding.
- Range: a value outside `i64` units is an overflow error. `i64::MIN` parses.

## Consequences

- Sums and comparisons are exact; replay reproduces live state bit for bit.
- Parsing rejects any input it cannot hold exactly (a non-zero digit beyond
  the 8th decimal place, or overflow) instead of rounding it silently. Zero
  padding beyond 8 places is lossless and does not abort ingestion.
- `Display` and parsing round-trip for every value of every type.
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

The verbatim decimal strings in the raw store (ADR-030) normalize to the
same `Price`/`Qty` in live processing and in replay, and the first
order-flow features (delta, CVD) are computed on them.

Progress 2026-10-07: delta and CVD are computed on fixed point by ADR-035
(#17). The live/replay half waits for #11 and #13.
