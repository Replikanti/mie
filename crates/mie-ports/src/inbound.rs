//! Driving (inbound) ports: the use cases the outside world invokes.
//!
//! Present: [`ReplayMarket`]. Planned, each landing with its issue:
//! `AnalyzeMarket`, `RunResearchExperiment`, `EvaluateStrategy`,
//! `ValidateCandidate`, `GenerateMarketReport`.

use crate::outbound::{ProviderError, ReplayWindow};
use mie_domain::state::{MarketState, StateError};
use std::fmt;

/// Replays recorded market data through the domain (brief §5).
pub trait ReplayMarket {
    /// Replays `window` and reports the resulting market state.
    ///
    /// # Errors
    ///
    /// [`UseCaseError`] when the data source fails or the domain rejects an
    /// event.
    fn replay(&self, window: ReplayWindow) -> Result<ReplayReport, UseCaseError>;
}

/// Outcome of a replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    /// Number of events consumed.
    pub events: u64,
    /// Market state after the last event.
    pub state: MarketState,
}

/// Why a use case failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UseCaseError {
    /// The market-data source failed.
    Provider(ProviderError),
    /// The domain rejected an event.
    Domain(StateError),
}

impl From<ProviderError> for UseCaseError {
    fn from(err: ProviderError) -> Self {
        Self::Provider(err)
    }
}

impl From<StateError> for UseCaseError {
    fn from(err: StateError) -> Self {
        Self::Domain(err)
    }
}

impl fmt::Display for UseCaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider(err) => write!(f, "{err}"),
            Self::Domain(err) => write!(f, "domain rejected an event: {err}"),
        }
    }
}

impl std::error::Error for UseCaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Provider(err) => Some(err),
            Self::Domain(err) => Some(err),
        }
    }
}
