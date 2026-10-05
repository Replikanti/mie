//! Domain-level market observations.
//!
//! This is what the `MarketDataProvider` port hands to the core — never
//! exchange SDK or wire types (Data Plane brief, design rule 5). The skeleton
//! carries trades only; depth/order-book, kline, open-interest, funding and
//! liquidation events, and explicit feed-gap markers, arrive with the Data
//! Plane issues.

use crate::num::{Price, Qty};
use crate::time::EventTime;

/// One market observation, in the order the core consumes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketEvent {
    /// An executed trade.
    Trade(Trade),
}

impl MarketEvent {
    /// The event time the core treats as "now" for this observation.
    pub fn time(&self) -> EventTime {
        match self {
            Self::Trade(trade) => trade.time,
        }
    }
}

/// An executed trade with its aggressor side (ADR-021).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    /// Exchange trade time.
    pub time: EventTime,
    /// Exchange-assigned trade id (unique per instrument).
    pub trade_id: u64,
    /// Execution price.
    pub price: Price,
    /// Executed quantity.
    pub qty: Qty,
    /// The side that crossed the spread.
    pub aggressor: Aggressor,
}

/// Which side initiated a trade by crossing the spread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Aggressor {
    /// An aggressive buy lifted the ask.
    Buy,
    /// An aggressive sell hit the bid.
    Sell,
}
