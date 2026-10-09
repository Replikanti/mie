# MIE — BTC Market Intelligence Engine

A deterministic market-intelligence and research system for the BTCUSDT
perpetual. It maps the market first, measures how the market behaves in each
measurable context, and only then discovers, validates and selects
strategies.

MIE is not a "super strategy". It is a measurable research machine that
finds out which combinations of market state, regime, location and trigger
carry repeatable edge — and it may legitimately answer
**NO SUITABLE STRATEGY**.

> **Status:** workspace skeleton. The design is complete in
> [`docs/design/`](docs/design/); implementation is tracked in
> [issues](https://github.com/Replikanti/mie/issues).

## Principles

**Market State → Regime → Conditional Behavior → Bias → Location → Trigger →
Execution → Outcome → Research Feedback**

- Deterministic data, replay and validation are authoritative. LLMs and agents
  are research operators, never the source of numerical truth (ADR-011,
  ADR-020).
- A bias does not authorize execution; only a trigger at a defined location
  can, through validation (ADR-012). Aggression is not direction (ADR-023).
- No feature is accepted because it sounds plausible: its contribution is
  measured (ADR-013), and important hypotheses face refutation (ADR-014).
- The first egress is informational (Telegram). Autonomous execution is
  outside the MVP (ADR-010).

## Architecture

Hexagonal — ports and adapters (ADR-018). The domain core knows nothing about
Binance, Parquet, DuckDB, Telegram or LLM providers, and live ingestion and
historical replay drive the same domain code (ADR-019). The crate graph
enforces the boundary (ADR-025):

```text
                 mie-cli             (composition root: `mie` binary)
                /       \
   mie-adapter-*         mie-app     (use-case services)
   (binance,    \       /
    parquet)     mie-ports           (inbound + outbound port traits)
                     |
                 mie-domain          (pure, deterministic core)
```

| Crate | Role |
|---|---|
| [`mie-domain`](crates/mie-domain) | Market observations, Market State, regime, location, trigger and strategy vocabulary. No dependencies, no I/O, no clock. |
| [`mie-ports`](crates/mie-ports) | Use-case (inbound) and infrastructure (outbound) contracts owned by the core. |
| [`mie-app`](crates/mie-app) | Use-case services that drive the domain through ports. |
| [`mie-adapter-parquet`](crates/mie-adapter-parquet) | Immutable raw store: verbatim messages in sealed Parquet files with manifests and dataset versions (ADR-030). |
| [`mie-adapter-binance`](crates/mie-adapter-binance) | Binance USDⓈ-M live capture: raw-first persistence, shared normalization, feed gaps, canonical merge (ADR-032); order-book sync from depth diffs and REST snapshots with audited checkpoints (ADR-038); public-archive backfill: checksum-verified, exactly-once import (ADR-034); replay of both from the raw store (ADR-039). |
| [`mie-adapter-fs`](crates/mie-adapter-fs) | Research result store: append-only, read-only result files with SHA-256 trailers, keyed by experiment and pipeline (ADR-040). |
| `mie-adapter-*` *(planned)* | DuckDB, Telegram, LLM operators, backtest engine. |
| [`mie-cli`](crates/mie-cli) | Composition root, the `mie` binary: wires adapters to ports. Today `ingest`, `capture-report`, `archive-import` / `archive-verify` / `archive-kline-check`, `replay`, `equivalence` and `experiment validate` / `experiment run`; report follows. |

First milestone data path (brief §19):

```text
Binance → Rust ingestion → immutable raw Parquet → deterministic Market State
(ATR / OI / CVD / volume profile / structure) → Regime → conditional outcome
dataset → DuckDB replay/research → Strategy Research Lab → Telegram
```

## Documents

| Document | Content |
|---|---|
| [Research & Architecture Brief](docs/design/BTC_Market_Intelligence_Engine_Research_Brief.md) | Vision, principles, architecture, first milestone, deferred scope |
| [ADR-001 … ADR-024](docs/design/BTC_MIE_ADRs.md) | Accepted baseline decisions |
| [Data Plane](docs/design/BTC_MIE_Data_Plane.md) | Ingestion, raw storage, replay, order-flow data contract |
| [Market State & Regime](docs/design/BTC_MIE_Market_State_and_Regime.md) | Features, order-flow state, ATR regime, location and auction state |
| [Conditional Market Behavior](docs/design/BTC_MIE_Conditional_Behavior.md) | Outcome measurement by context, counter-hypotheses |
| [Strategy Research Lab](docs/design/BTC_MIE_Strategy_Research_Lab.md) | Research loop, experiment definition, anti-overfitting |
| [Strategy Registry & Suitability](docs/design/BTC_MIE_Strategy_Registry_and_Suitability.md) | Lifecycle, runtime suitability, edge decay |
| [ADR-025 onwards](docs/adr/) | Decisions made during implementation |

## Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
tools/check-architecture.sh   # crate graph + core purity (ADR-025)
cargo deny check              # licenses, advisories, sources
```

Live capture (public market data, no credentials) into the raw store,
until SIGINT/SIGTERM, and the check of a capture window:

```sh
cargo run --release -p mie-cli -- ingest --config crates/mie-cli/ingest.example.toml
cargo run --release -p mie-cli -- capture-report \
    --config crates/mie-cli/ingest.example.toml --from <epoch ms> --to <epoch ms>
```

Historical backfill from the Binance public data archive (source
`binance-archive`, see [data availability](docs/data-availability.md)):
probe, import (re-runs skip imported files, an interrupted file resumes),
verify, and cross-check bars against the archive klines:

```sh
cargo run --release -p mie-cli -- archive-import \
    --config crates/mie-cli/archive.example.toml --from 2025-10-01 --to 2026-09-30 --dry-run
cargo run --release -p mie-cli -- archive-import \
    --config crates/mie-cli/archive.example.toml --from 2025-10-01 --to 2026-09-30
cargo run --release -p mie-cli -- archive-verify \
    --config crates/mie-cli/archive.example.toml --from 2025-10-01 --to 2026-09-30
cargo run --release -p mie-cli -- archive-kline-check \
    --config crates/mie-cli/archive.example.toml --from 2026-09-29 --to 2026-09-29 \
    --trade-source trades
```

Replay a window of the raw store through the core (ADR-039): live capture
runs are recomputed from the journal, the archive is merged canonically.
The report carries the dataset version and the event-stream hash; the same
window prints the same bytes:

```sh
cargo run --release -p mie-cli -- replay \
    --config crates/mie-cli/ingest.example.toml --from <epoch ms> --to <epoch ms>
cargo run --release -p mie-cli -- replay --source archive \
    --config crates/mie-cli/archive.example.toml --from 2025-10-01 --to 2026-09-30
```

Check live/replay equivalence (ADR-041): every capture run started in the
window is recomputed alone into a fresh engine, and its state checkpoints
(event-stream hash and Market State hash) are compared with the ones
`mie ingest` journaled; the first divergence is reported:

```sh
cargo run --release -p mie-cli -- equivalence \
    --config crates/mie-cli/ingest.example.toml --from <epoch ms> --to <epoch ms>
```

Validate an experiment spec, then run it over a replay of its sample and
record the result (ADR-040). The experiment id is the fingerprint of the
canonical spec; results are append-only, and a re-run on the same data
version prints `reproduced` instead of `recorded`:

```sh
cargo run --release -p mie-cli -- experiment validate crates/mie-cli/experiment.example.spec
cargo run --release -p mie-cli -- experiment run crates/mie-cli/experiment.example.spec \
    --source archive --config crates/mie-cli/archive.example.toml --results data/results
```

## Out of scope for now

Multi-asset scale, ClickHouse, autonomous execution, a single fixed strategy,
premature parameter optimization, LLM-driven numerical truth, unvalidated
discretionary concepts, and strategy selection on backtest performance alone
(brief §20).

## License

[MIT](LICENSE)
