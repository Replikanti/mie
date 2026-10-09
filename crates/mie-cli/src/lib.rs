//! MIE composition root (ADR-025): the only crate that wires adapters to
//! ports. The `mie` binary dispatches to these commands:
//!
//! - `mie ingest --config <path>` ([`ingest`]): live Binance capture into
//!   the raw Parquet store, driving the core through the live provider;
//! - `mie capture-report --config <path> --from <ms> --to <ms>`
//!   ([`report`]): verifies a capture window (the soak acceptance of #9);
//! - `mie archive-import`, `mie archive-verify`, `mie archive-kline-check`
//!   ([`archive`]): backfill from the Binance public data archive (#12);
//! - `mie replay --config <path> --from … --to … [--source live|archive]`
//!   ([`replay`]): replays a window of raw data through the core with a
//!   deterministic report (#11);
//! - `mie equivalence --config <path> --from … --to …` ([`equivalence`]):
//!   recomputes each cleanly ended live run of the window and compares its
//!   state checkpoints with the ones live journaled (#13, ADR-041);
//! - `mie experiment validate <spec>` and `mie experiment run <spec> …`
//!   ([`experiment`]): validates an experiment spec, or runs it and records
//!   its result in the append-only result store (#29, ADR-040).
//!
//! The real network connectors are constructed in `main.rs` and nowhere
//! else; tests drive [`ingest::run`] with fakes.

pub mod archive;
pub mod config;
pub mod equivalence;
pub mod experiment;
pub mod ingest;
pub mod journal;
pub mod replay;
pub mod report;
