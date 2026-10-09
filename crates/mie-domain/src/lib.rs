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
//! | Event-stream hash (ADR-039)               | [`event_hash`]               |
//! | Canonical event order (ADR-028)           | [`order`]                    |
//! | Market State                              | [`state`]                    |
//! | Market State hash (ADR-041)               | [`state_hash`]               |
//! | Event-time bars (ADR-031)                 | [`bars`]                     |
//! | L2 order book (ADR-038)                   | [`book`]                     |
//! | Feature identity and versioning (ADR-029) | [`feature`], [`fingerprint`] |
//! | Motion, ATR and regime (ADR-033)          | [`volatility`]               |
//! | Order flow / aggression (ADR-035)         | [`flow`]                     |
//! | Volume profile (ADR-036)                  | [`profile`]                  |
//! | Market structure (ADR-037)                | [`structure`]                |
//! | Derivatives context (ADR-042)             | [`derivatives`]              |
//! | Regime (ADR-017, ADR-033)                 | [`regime`]                   |
//! | Location / auction state                  | [`location`]                 |
//! | Bias / trigger (ADR-012, ADR-024)         | [`trigger`]                  |
//! | Strategy lifecycle / edge health          | [`strategy`]                 |
//! | Experiments and results (ADR-040)         | [`research`]                 |
//!
//! Conditional Market Behavior, risk and validation rules join as modules
//! here when their issues land — never as a dependency on a port or adapter.

pub mod bars;
pub mod book;
pub mod derivatives;
pub mod event;
pub mod event_hash;
pub mod feature;
pub mod fingerprint;
pub mod flow;
pub mod location;
pub mod num;
pub mod order;
pub mod profile;
pub mod regime;
pub mod research;
pub mod state;
pub mod state_hash;
pub mod strategy;
pub mod structure;
pub mod time;
pub mod trigger;
pub mod volatility;
