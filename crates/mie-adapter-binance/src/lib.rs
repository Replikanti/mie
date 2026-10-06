//! Live market-data adapter for the Binance USDⓈ-M BTCUSDT perpetual
//! (Data Plane brief; ADR-026, ADR-028, ADR-030).
//!
//! The adapter implements the outbound
//! [`MarketDataProvider`](mie_ports::outbound::MarketDataProvider) port for
//! live data. Only `mie-cli` wires it (ADR-025).
//!
//! | Module | Role |
//! |---|---|
//! | [`stream`] | The captured feeds and their raw and domain names |
//! | [`normalize`] | Binance wire → domain events, shared with replay (#11) |
//!
//! Determinism: normalization is a pure function of the persisted payload.
//! The wall clock is used only for capture metadata (receive time) and for
//! scheduling, never for ordering (ADR-028 D1).

pub mod normalize;
pub mod stream;

pub use stream::{BinanceStream, OI_POLL_INTERVAL_MS};
