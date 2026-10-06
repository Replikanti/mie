//! MIE ports: the contracts between the core and the outside world (ADR-018).
//!
//! Ports are owned by the core. Adapters implement them; the core never
//! depends on an adapter. Market-data ports are synchronous and pull-based,
//! and async I/O stays inside adapters (ADR-026, proposed).
//!
//! - [`inbound`]: use cases the outside world drives (CLI, scheduler).
//! - [`outbound`]: infrastructure the core drives (market data, stores, egress).
//! - [`raw`]: the immutable raw record store shared by producers and replay
//!   (ADR-030).

pub mod inbound;
pub mod outbound;
pub mod raw;
