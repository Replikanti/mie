# ADR-025: Crate graph enforces the hexagonal boundary

- Status: accepted
- Date: 2026-10-05

## Context

ADR-018 requires a domain core that never depends on infrastructure, and
ADR-019 requires live and replay processing to share that core. Inside a
single crate these rules hold only as long as every review catches every
stray `use`. Rust's crate graph can make them structural: a crate cannot use
what its `Cargo.toml` does not declare.

Determinism needs a second guard. Even without infrastructure dependencies,
core code could read the wall clock, touch the filesystem, or iterate a
`HashMap` (whose order is randomized per process) — and live and replay
results would silently diverge.

## Decision

The workspace is split by hexagonal role, with fixed dependency edges:

| Crate | Role | May depend on |
|---|---|---|
| `mie-domain` | domain core | nothing |
| `mie-ports` | inbound and outbound port traits | `mie-domain` |
| `mie-app` | application services (use cases) | `mie-domain`, `mie-ports` |
| `mie-adapter-<tech>` | one adapter per technology | `mie-domain`, `mie-ports`, external crates |
| `mie-cli` | composition root | anything; nothing depends on it |

The core crates (domain, ports, app) are pure: no external dependencies, and
no wall clock, filesystem, network, environment, threads, processes, async or
hash-ordered collections (`BTreeMap`/`BTreeSet` instead of
`HashMap`/`HashSet`). Time inside the core is event time from the input
stream.

`tools/check-architecture.sh` enforces both rule sets in CI.
Dev-dependencies are exempt from the graph rules.

## Consequences

- Infrastructure cannot leak into the core by accident; the build or the
  architecture check fails first.
- Adapters are replaceable one crate at a time (Binance, raw Parquet, DuckDB,
  Telegram, LLM operators, backtest engine).
- A utility crate the core needs (error derive, numeric helpers) requires
  amending this ADR — deliberate friction.
- More crates mean more `Cargo.toml` boilerplate; accepted for the guarantee.
- The purity scan is a pattern guard over source text, not a proof; review
  still applies.

## Alternatives considered

- **Single crate with module conventions** — cheapest, but the boundary
  rests on reviewer vigilance, while ADR-018 makes it a hard requirement.
- **Ports inside `mie-domain`** — fewer crates, but mixes use-case contracts
  with market semantics and invites I/O-shaped APIs into the domain.
- **One crate per subsystem** (market state, regime, research, …) —
  premature; the subsystems share one deterministic state model and would
  churn together.
