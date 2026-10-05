//! Market State: the canonical deterministic representation of current
//! conditions (brief §8, Market State & Regime brief).
//!
//! Walking-skeleton stage: the engine enforces the event-time contract and
//! tracks only the latest trade. Feature families (volatility, order flow,
//! order book, OI/funding, volume profile, structure) are added by the Market
//! State issues, each as a deterministic, versioned definition.

use crate::event::MarketEvent;
use crate::num::Price;
use crate::time::EventTime;
use std::fmt;

/// The market as known after the last consumed event.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarketState {
    /// Time of the last consumed event; `None` before the first one.
    pub as_of: Option<EventTime>,
    /// Price of the last trade.
    pub last_trade_price: Option<Price>,
    /// Number of trades consumed.
    pub trade_count: u64,
}

/// Builds [`MarketState`] incrementally from an ordered event stream.
///
/// Live ingestion and historical replay drive the same engine (ADR-019); the
/// engine never knows which of them is feeding it.
#[derive(Debug, Default)]
pub struct MarketStateEngine {
    state: MarketState,
}

impl MarketStateEngine {
    /// Creates an engine with an empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Consumes the next event.
    ///
    /// # Errors
    ///
    /// [`StateError::TimeWentBackwards`] if the event is older than the last
    /// consumed one; the state is left unchanged. Ordering is the market-data
    /// provider's job — the domain never reorders.
    pub fn apply(&mut self, event: &MarketEvent) -> Result<(), StateError> {
        let time = event.time();
        if let Some(as_of) = self.state.as_of.filter(|&as_of| time < as_of) {
            return Err(StateError::TimeWentBackwards { as_of, event: time });
        }
        match event {
            MarketEvent::Trade(trade) => {
                self.state.last_trade_price = Some(trade.price);
                self.state.trade_count += 1;
            }
            // No feature consumes these yet.
            MarketEvent::FeedGap(_)
            | MarketEvent::BookSnapshot(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::BookUpdate(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_) => {}
        }
        self.state.as_of = Some(time);
        Ok(())
    }

    /// The current state.
    pub fn state(&self) -> &MarketState {
        &self.state
    }
}

/// Why the engine rejected an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateError {
    /// The event is older than the last consumed event.
    TimeWentBackwards {
        /// Time of the last consumed event.
        as_of: EventTime,
        /// Time of the rejected event.
        event: EventTime,
    },
}

impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimeWentBackwards { as_of, event } => {
                write!(f, "event at {event} is older than the state as of {as_of}")
            }
        }
    }
}

impl std::error::Error for StateError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Aggressor, Trade};
    use crate::num::Qty;

    fn trade(millis: i64, trade_id: u64, price_units: i64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: EventTime::from_millis(millis),
            trade_id,
            price: Price::from_units(price_units),
            qty: Qty::from_units(100_000),
            aggressor: Aggressor::Buy,
        })
    }

    #[test]
    fn tracks_latest_trade() {
        let mut engine = MarketStateEngine::new();
        engine.apply(&trade(1_000, 1, 10)).unwrap();
        engine.apply(&trade(1_000, 2, 11)).unwrap();
        engine.apply(&trade(1_500, 3, 12)).unwrap();
        assert_eq!(
            engine.state(),
            &MarketState {
                as_of: Some(EventTime::from_millis(1_500)),
                last_trade_price: Some(Price::from_units(12)),
                trade_count: 3,
            }
        );
    }

    #[test]
    fn rejects_time_going_backwards_without_touching_state() {
        let mut engine = MarketStateEngine::new();
        engine.apply(&trade(2_000, 1, 10)).unwrap();
        let before = engine.state().clone();
        let err = engine.apply(&trade(1_999, 2, 11)).unwrap_err();
        assert_eq!(
            err,
            StateError::TimeWentBackwards {
                as_of: EventTime::from_millis(2_000),
                event: EventTime::from_millis(1_999),
            }
        );
        assert_eq!(engine.state(), &before);
    }
}
