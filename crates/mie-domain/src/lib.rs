//! MIE domain core.
//!
//! Deterministic, infrastructure-free model of the BTCUSDT perpetual market,
//! as specified in `docs/design/` (ADR-005, ADR-018, ADR-019).
//!
//! Core purity (ADR-025, enforced by `tools/check-architecture.sh`): no
//! dependencies, no I/O, no wall clock, no randomness, no async, no
//! hash-ordered collections. "Now" is the time of the latest consumed event,
//! so the same input stream always yields the same state — whether it comes
//! from live ingestion or from historical replay.
//!
//! | Subsystem (design doc)                    | Module                       |
//! |-------------------------------------------|------------------------------|
//! | Market observations (Data Plane)          | [`event`], [`num`], [`time`] |
//! | Canonical event order (ADR-028)           | [`order`]                    |
//! | Market State                              | [`state`]                    |
//! | Event-time bars (ADR-031)                 | [`bars`]                     |
//! | Feature identity and versioning (ADR-029) | [`feature`], [`fingerprint`] |
//! | Motion, ATR and regime (ADR-033)          | [`volatility`]               |
//! | Regime (ADR-017, ADR-033)                 | [`regime`]                   |
//! | Location / auction state                  | [`location`]                 |
//! | Bias / trigger (ADR-012, ADR-024)         | [`trigger`]                  |
//! | Strategy lifecycle / edge health          | [`strategy`]                 |
//!
//! Conditional Market Behavior, risk and validation rules join as modules
//! here when their issues land — never as a dependency on a port or adapter.

pub mod bars;
pub mod event;
pub mod feature;
pub mod fingerprint;
pub mod location;
pub mod num;
pub mod order;
pub mod regime;
pub mod state;
pub mod strategy;
pub mod time;
pub mod trigger;
pub mod volatility;
