//! MIE composition root (ADR-025): the only crate that wires adapters to
//! ports. The `mie` binary dispatches to these commands:
//!
//! - `mie ingest --config <path>` ([`ingest`]): live Binance capture into
//!   the raw Parquet store, driving the core through the live provider;
//! - `mie capture-report --config <path> --from <ms> --to <ms>`
//!   ([`report`]): verifies a capture window (the soak acceptance of #9);
//! - `mie archive-import`, `mie archive-verify`, `mie archive-kline-check`
//!   ([`archive`]): backfill from the Binance public data archive (#12);
//! - `mie replay --config <path> --from … --to … [--source live|archive|history]`
//!   ([`replay`]): replays a window of raw data through the core with a
//!   deterministic report (#11);
//! - `mie equivalence --config <path> --from … --to …` ([`equivalence`]):
//!   recomputes each cleanly ended live run of the window and compares its
//!   state checkpoints with the ones live journaled (#13, ADR-041);
//! - `mie experiment validate <spec>` and `mie experiment run <spec> …`
//!   ([`experiment`]): validates an experiment spec, or runs it and records
//!   its result in the append-only result store (#29, ADR-040);
//! - `mie conditional build --config <path> --from … --to … --out <dir>`,
//!   `mie conditional verify <dir>`, `mie conditional query <dir> <view>`
//!   and `mie conditional views` ([`conditional`]): builds the conditional
//!   outcome dataset of a replay, verifies one, prints one of its DuckDB
//!   views, or prints the view definitions (#27, ADR-057; format 2 of #28,
//!   ADR-058);
//! - `mie alerts replay --config <path> --from … --to … --out <file>` and
//!   `mie alerts report --config <path> --from … --to …` ([`alerts`]): the
//!   alert dry-run of a replay window into a JSON Lines log, and the market
//!   report as of a window's end (#38, ADR-059);
//! - `mie store init --config <path>` and `mie store init-root --config
//!   <path> --root <path> --id <id>` ([`store`]): set up a tiered raw store,
//!   hot's marker and copy ledger, and each cold root's marker (#187,
//!   ADR-062 D4, D7);
//! - `mie store replicate --config <path> [--archive-config <path>]`
//!   ([`store::replicate`]): one replication pass to the copy targets, which
//!   copies and never removes (#189, ADR-062 D5, D11).
//!
//! The real network connectors are constructed in `main.rs` and nowhere
//! else; tests drive [`ingest::run`] with fakes.

pub mod alerts;
pub mod archive;
pub mod conditional;
pub mod config;
pub mod equivalence;
pub mod experiment;
pub mod ingest;
pub mod journal;
mod reconcile;
pub mod replay;
pub mod report;
pub mod store;
