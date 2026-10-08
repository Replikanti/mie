# ADR-026: Synchronous, pull-based market-data ports

- Status: proposed
- Date: 2026-10-05

## Context

The live Binance feed is network I/O and naturally asynchronous; historical
replay is a sequential read. ADR-019 demands one domain path for both, and
ADR-025 keeps async runtimes out of the core.

## Decision

`MarketDataProvider` is a synchronous pull interface: the core asks for the
next event and blocks until one exists. Adapters that do async I/O run their
own runtime internally (for example on a dedicated thread) and hand
normalized domain events to the port through a bounded channel. The core,
the ports and the application services contain no async code.

## Consequences

- Replay, backtests and tests drive the core with plain loops: fully
  deterministic, no executor scheduling in the result path.
- The live adapter owns buffering, backpressure and reconnects, and must
  surface feed gaps as data instead of swallowing them.
- One blocked core thread per consumer of a live stream — fine for one
  instrument and one primary consumer (ADR-002).

## Alternatives considered

- **Async ports (`async fn` in traits)** — couples every core crate to an
  executor and lets scheduling influence processing order.
- **Push-based callbacks** — inverts control into the adapter and makes
  backpressure and ordering harder to reason about.

## Accept when

The Binance live adapter feeds the core through this port at full BTCUSDT
trade and depth rates without dropped events or unbounded buffering.

Progress 2026-10-09: the #9 soak (about 30 h, trades without depth) fed the
core with channel blocked time 0 ms and high-water at most 753 of 65 536,
with no dropped events. Trade rates are therefore covered. Depth rates are
not yet: they need the #10 soak with all seven streams, and ADR-038 reports
them.
