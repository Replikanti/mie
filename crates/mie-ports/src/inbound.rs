//! Driving (inbound) ports: the use cases the outside world invokes.
//!
//! Present: [`ReplayMarket`], [`RunResearchExperiment`] (ADR-040).
//! Planned, each landing with its issue: `AnalyzeMarket`,
//! `EvaluateStrategy`, `ValidateCandidate`, `GenerateMarketReport`.

use crate::outbound::{ProviderError, ReplayWindow, ResultStoreError};
use crate::raw::DatasetVersion;
use mie_domain::event_hash::EventStreamHash;
use mie_domain::feature::FeatureKey;
use mie_domain::research::{DataVersion, ExperimentResult, ExperimentSpec, ResultKey};
use mie_domain::state::{MarketState, StateError};
use std::fmt;

/// Replays recorded market data through the domain (brief §5).
pub trait ReplayMarket {
    /// Replays `window` and reports the resulting market state.
    ///
    /// An event the domain rejects is counted in
    /// [`ReplayReport::domain_rejections`] and the replay continues, exactly
    /// as live ingestion does (ADR-039 D9).
    ///
    /// # Errors
    ///
    /// [`UseCaseError::Provider`] when the data source fails.
    fn replay(&self, window: ReplayWindow) -> Result<ReplayReport, UseCaseError>;
}

/// Outcome of a replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    /// Number of events delivered, rejected ones included.
    pub events: u64,
    /// The raw data the replay read (ADR-030, ADR-039 D8).
    pub dataset: DatasetVersion,
    /// The hash of the delivered event sequence (ADR-039 D7).
    pub stream_hash: EventStreamHash,
    /// Delivered events the domain rejected.
    pub domain_rejections: u64,
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

/// Runs an experiment through the deterministic research pipeline and
/// records its result (ADR-040). The pipeline is the only writer of results
/// (ADR-020).
pub trait RunResearchExperiment {
    /// Runs `spec` and records the result, or confirms the stored one.
    ///
    /// # Errors
    ///
    /// [`ResearchError`]; nothing is stored on any error.
    fn run(&mut self, spec: &ExperimentSpec) -> Result<ExperimentRun, ResearchError>;
}

/// What a run of an experiment did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExperimentRun {
    /// The result was new and is now stored.
    Recorded(ExperimentResult),
    /// The same result was already stored: the run reproduced it exactly.
    Reproduced(ExperimentResult),
}

impl ExperimentRun {
    /// The result, recorded or reproduced.
    pub fn result(&self) -> &ExperimentResult {
        match self {
            Self::Recorded(result) | Self::Reproduced(result) => result,
        }
    }
}

/// Why an experiment run failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResearchError {
    /// The market-data source failed.
    Provider(ProviderError),
    /// The replay opened other raw data than the spec names.
    DataVersionMismatch {
        /// The spec's data version.
        declared: DataVersion,
        /// The version the replay opened.
        opened: DataVersion,
    },
    /// The engine does not compute this member of the spec's feature set at
    /// its exact definition.
    FeatureUnavailable {
        /// The feature.
        feature: FeatureKey,
    },
    /// The sample period holds no event.
    EmptySample,
    /// The run produced another result than the one stored under its key:
    /// the run is not reproducible, and the stored result stays.
    Diverged {
        /// The key.
        key: ResultKey,
    },
    /// The result store failed.
    Store(ResultStoreError),
}

impl From<ProviderError> for ResearchError {
    fn from(err: ProviderError) -> Self {
        Self::Provider(err)
    }
}

impl From<ResultStoreError> for ResearchError {
    fn from(err: ResultStoreError) -> Self {
        Self::Store(err)
    }
}

impl fmt::Display for ResearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider(err) => write!(f, "{err}"),
            Self::DataVersionMismatch { declared, opened } => write!(
                f,
                "data version mismatch: the spec names {declared}, the sample opened {opened}"
            ),
            Self::FeatureUnavailable { feature } => write!(
                f,
                "feature {feature} is not computed by this build of the engine"
            ),
            Self::EmptySample => f.write_str("the sample period holds no event"),
            Self::Diverged { key } => write!(
                f,
                "result {key} diverged: the run differs from the stored result, which stays"
            ),
            Self::Store(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for ResearchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Provider(err) => Some(err),
            Self::Store(err) => Some(err),
            _ => None,
        }
    }
}
