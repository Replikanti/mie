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
//!   deterministic report (#11).
//!
//! The real network connectors are constructed in `main.rs` and nowhere
//! else; tests drive [`ingest::run`] with fakes.

pub mod archive;
pub mod config;
pub mod ingest;
pub mod journal;
pub mod replay;
pub mod report;
