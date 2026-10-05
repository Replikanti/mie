# CLAUDE.md — mie

BTC Market Intelligence Engine: a deterministic market-intelligence and
research system for the BTCUSDT perpetual. Rust workspace, hexagonal
architecture. Public repository (MIT).

## Sources of truth

- Design: `docs/design/` — research brief v7, five subsystem briefs, and the
  accepted baseline ADR-001 … ADR-024.
- Decisions since: `docs/adr/` (ADR-025 onwards, process in its README). A PR
  that makes a design decision adds or flips an ADR.
- Work: GitHub issues, labelled `area::*`, `P1`–`P3`, `size::*`, `epic`,
  `needs-adr`.

## Invariants

1. **Crate graph** (ADR-025): domain → nothing; ports → domain; app → domain
   and ports; `mie-adapter-<tech>` → domain, ports, external crates; only
   `mie-cli` wires adapters. `tools/check-architecture.sh` enforces it.
2. **Core purity** (domain, ports, app): no external dependencies, no wall
   clock, filesystem, network, environment, threads, async or
   `HashMap`/`HashSet`. "Now" is event time.
3. **One domain path** for live and replay (ADR-019). Replay is an adapter,
   never a second implementation of market logic.
4. **Raw data is immutable** and the source of truth (ADR-003, ADR-022).
   Every derived feature is reproducible from versioned raw data plus a
   versioned feature definition.
5. **LLMs are research operators**, never numerical authority (ADR-011,
   ADR-020). No execution path in the MVP (ADR-010): there is no
   `ExecutionGateway`.
6. Bias is not a trigger (ADR-012); aggression is not direction (ADR-023); a
   feature earns its place by measured contribution (ADR-013).

## Validation (CI equivalent — run before every commit)

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
tools/check-architecture.sh
cargo deny check
```

## Conventions

- English in code, comments, commits, PRs and issues.
- Branch and PR for every change; worktrees under `/worktrees/` (gitignored);
  never push to main. The PR body carries `Closes #N` or a
  `No-Issue: <reason>` line (CI-enforced).
- Domain items cite the design section or ADR they implement.
- Every new dependency is justified in a `Cargo.toml` comment; licenses per
  `deny.toml`. Core crates take none (ADR-025).
- Public repository: no local paths, credentials, API keys or private data.
