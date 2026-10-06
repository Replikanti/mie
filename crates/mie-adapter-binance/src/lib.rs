//! Live market-data adapter for the Binance USDⓈ-M BTCUSDT perpetual
//! (Data Plane brief; ADR-026, ADR-028, ADR-030, ADR-032).
//!
//! The adapter implements the outbound
//! [`MarketDataProvider`](mie_ports::outbound::MarketDataProvider) port for
//! live data. Only `mie-cli` wires it (ADR-025).
//!
//! | Module | Role |
//! |---|---|
//! | [`stream`] | The captured feeds and their raw and domain names |
//! | [`normalize`] | Binance wire → domain events, shared with replay (#11) |
//! | [`sequence`] | Per-stream dedupe and feed gaps (disconnects, id breaks, restarts) |
//! | [`holdback`] | Canonical cross-stream merge with a bounded hold-back |
//! | [`pipeline`] | The three above composed: raw records → canonical events |
//! | [`transport`] | WebSocket, HTTP and clock seams with their real implementations |
//! | [`live`] | Capture threads, raw-first persistence and the live provider |
//!
//! Live capture runs on plain threads, without an async runtime (ADR-026
//! leaves the adapter-internal model open): one per WebSocket stream (`ws`),
//! one open-interest poller (`rest`) and one capture thread, the only writer.
//!
//! Determinism: normalization is a pure function of the persisted payload.
//! The wall clock is used only for capture metadata (receive time) and for
//! scheduling, never for ordering (ADR-028 D1).

pub mod holdback;
pub mod live;
pub mod normalize;
pub mod pipeline;
mod rest;
pub mod sequence;
pub mod stream;
pub mod transport;
mod ws;

pub use live::{
    BinanceLiveProvider, CaptureEvent, CaptureHandle, CaptureObserver, CaptureSummary, LiveConfig,
    run_id, start,
};
pub use pipeline::{Pipeline, PipelineStats, Pushed, StreamStats};
pub use stream::{BinanceStream, OI_POLL_INTERVAL_MS};
