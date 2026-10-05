//! Driven (outbound) ports.
//!
//! Present: live and historical market data. Planned, each landing with the
//! issue that first needs it: `FeatureStore`, `StrategyRepository`,
//! `BacktestEngine`, `ResearchResultStore`, `AlertGateway`. There is
//! deliberately no `ExecutionGateway`: the MVP is informational (ADR-010).

use mie_domain::event::MarketEvent;
use mie_domain::time::EventTime;
use std::fmt;

/// A source of domain-level market observations (brief §5, Data Plane brief).
///
/// Contract:
/// - events arrive in the order the core must consume them: non-decreasing
///   [`MarketEvent::time`], with a deterministic order among equal times;
/// - only domain types cross the port — never exchange SDK or wire types;
/// - `Ok(None)` means the stream has ended (replay exhausted, feed closed).
pub trait MarketDataProvider {
    /// Pulls the next event, blocking until one is available.
    ///
    /// # Errors
    ///
    /// [`ProviderError`] when the source fails or breaks this contract.
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError>;
}

/// Replays recorded market data through the same [`MarketDataProvider`]
/// contract as live data (ADR-019).
///
/// A replay exposes only the information that was available at each decision
/// moment and preserves the recorded event order.
pub trait HistoricalDataProvider {
    /// The event stream a replay yields.
    type Stream: MarketDataProvider;

    /// Opens a replay of `window`.
    ///
    /// # Errors
    ///
    /// [`ProviderError`] when the requested data cannot be opened.
    fn replay(&self, window: ReplayWindow) -> Result<Self::Stream, ProviderError>;
}

/// A half-open event-time window `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayWindow {
    /// First included event time.
    pub start: EventTime,
    /// First excluded event time.
    pub end: EventTime,
}

impl ReplayWindow {
    /// Whether `time` falls inside the window.
    pub fn contains(&self, time: EventTime) -> bool {
        self.start <= time && time < self.end
    }
}

/// Why a market-data provider could not deliver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderError {
    /// The underlying source failed (connection, file, decoding).
    Source(String),
    /// The source produced data that breaks the port contract.
    Contract(String),
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(detail) => write!(f, "market data source failed: {detail}"),
            Self::Contract(detail) => {
                write!(f, "market data source broke the port contract: {detail}")
            }
        }
    }
}

impl std::error::Error for ProviderError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_window_is_half_open() {
        let window = ReplayWindow {
            start: EventTime::from_millis(1_000),
            end: EventTime::from_millis(2_000),
        };
        assert!(!window.contains(EventTime::from_millis(999)));
        assert!(window.contains(EventTime::from_millis(1_000)));
        assert!(window.contains(EventTime::from_millis(1_999)));
        assert!(!window.contains(EventTime::from_millis(2_000)));
    }
}
