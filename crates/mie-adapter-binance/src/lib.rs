//! Market-data adapter for the Binance USDⓈ-M BTCUSDT perpetual: live
//! capture and historical backfill from the public data archive (Data Plane
//! brief; ADR-026, ADR-028, ADR-030, ADR-032, ADR-034, ADR-038).
//!
//! The adapter implements the outbound
//! [`MarketDataProvider`](mie_ports::outbound::MarketDataProvider) port for
//! live data and imports archive files into the raw store through the
//! [`RawRecordSink`](mie_ports::raw::RawRecordSink) port. Only `mie-cli`
//! wires it (ADR-025).
//!
//! | Module | Role |
//! |---|---|
//! | [`stream`] | The captured feeds and their raw and domain names |
//! | [`normalize`] | Binance wire → domain events, shared with replay (#11) |
//! | [`sequence`] | Per-stream dedupe and feed gaps (disconnects, id breaks, restarts) |
//! | [`book_sync`] | Order-book sync: diffs, snapshots, resync gaps and checkpoints (ADR-038) |
//! | [`holdback`] | Canonical cross-stream merge with a bounded hold-back |
//! | [`pipeline`] | The above composed: raw records → canonical events |
//! | [`book_audit`] | Live checkpoint audit of the rebuilt book against fresh snapshots |
//! | [`transport`] | WebSocket, HTTP and clock seams with their real implementations |
//! | [`live`] | Capture threads, raw-first persistence and the live provider |
//! | [`replay`] | Replay of live capture runs: per-run recompute from the raw store (#11) |
//! | [`archive`] | Archive import and replay: catalog, checksummed downloads, import ledger, archive normalization, canonical merge |
//!
//! Live capture runs on plain threads, without an async runtime (ADR-026
//! leaves the adapter-internal model open): one per WebSocket stream (`ws`),
//! one open-interest poller (`rest`), one order-book snapshot fetcher
//! (`depth_rest`) and one capture thread, the only writer.
//!
//! Determinism: normalization is a pure function of the persisted payload,
//! and the pipeline — order-book sync included — of the persisted records
//! in `receive_seq` order plus the run parameters. The wall clock is used
//! only for capture metadata (receive time) and for scheduling (reconnects,
//! polls, when a depth snapshot is fetched), never for ordering (ADR-028
//! D1) or for what the pipeline does with a record (ADR-038).

pub mod archive;
pub mod book_audit;
pub mod book_sync;
mod depth_rest;
pub mod holdback;
pub mod live;
pub mod normalize;
pub mod pipeline;
pub mod replay;
mod rest;
pub mod sequence;
pub mod stream;
#[cfg(test)]
mod testing;
pub mod transport;
mod ws;

pub use book_audit::{BookAudit, CheckpointResult};
pub use book_sync::{BookSequencer, BookStats, BookTransition};
pub use live::{
    BinanceLiveProvider, CaptureEvent, CaptureHandle, CaptureObserver, CaptureSummary, LiveConfig,
    SnapshotTrigger, run_id, start,
};
pub use pipeline::{Pipeline, PipelineStats, Pushed, StreamStats};
pub use replay::{LiveReplay, LiveReplayStream, LiveRun, RUN_MARGIN_MS, RunStats};
pub use stream::{BinanceStream, DEPTH_SNAPSHOT_LIMITS, OI_POLL_INTERVAL_MS, WsRoute};
