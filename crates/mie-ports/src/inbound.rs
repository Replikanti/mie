//! Driving (inbound) ports: the use cases the outside world invokes.
//!
//! Present: [`ReplayMarket`], [`RunResearchExperiment`] (ADR-040; the
//! backtest's refusals [`ResearchError::Unresolved`],
//! [`ResearchError::FundingNotDelivered`], ADR-060 D13, and
//! [`ResearchError::BookNotDelivered`], ADR-061 D4; the slippage
//! calibration's [`ResearchError::BookNotInSet`] and
//! [`ResearchError::CalibrationOverflow`], ADR-061 D7),
//! [`GenerateMarketReport`] (ADR-059, proposed). Planned, each landing with
//! its issue: `AnalyzeMarket`, `EvaluateStrategy`, `ValidateCandidate`.

use crate::outbound::{ProviderError, ReplayWindow, ResultStoreError};
use crate::raw::DatasetVersion;
use mie_domain::alert::MarketReport;
use mie_domain::backtest::ResolutionError;
use mie_domain::event_hash::EventStreamHash;
use mie_domain::feature::FeatureKey;
use mie_domain::research::{DataVersion, ExperimentResult, ExperimentSpec, ResultKey};
use mie_domain::state::{MarketState, StateError};
use mie_domain::time::EventTime;
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

/// Reports the market as of the end of a window (brief §17, ADR-059 D2f).
pub trait GenerateMarketReport {
    /// Replays `window` and reports the market as of `window.end`: the
    /// state after the window's last event. A domain rejection is counted
    /// and the replay continues, as in [`ReplayMarket::replay`].
    ///
    /// # Errors
    ///
    /// [`UseCaseError::Provider`] when the data source fails.
    fn report(&self, window: ReplayWindow) -> Result<MarketReport, UseCaseError>;
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
    /// A sample on the history chain starts before the chain's origin, so
    /// no prefix window `[origin, sample start)` exists (ADR-052 D10).
    SampleBeforeOrigin {
        /// The sample's start.
        start: EventTime,
        /// The chain's origin.
        origin: EventTime,
    },
    /// The spec's rule references do not resolve under rule registry v1
    /// (ADR-060 D2): every error, in resolution order. Checked before
    /// anything is opened.
    Unresolved(Vec<ResolutionError>),
    /// `funding.recorded@1` on a sample whose settlements the history chain
    /// does not deliver (ADR-060 D7, D13): booking it would book zero
    /// funding by omission. Checked before anything is opened.
    FundingNotDelivered {
        /// The sample's end.
        sample_end: EventTime,
        /// `Some`: the chain delivers settlements only up to this time, its
        /// cutover, and the sample ends later. `None`: the chain's archive
        /// does not replay `fundingRate`, so it delivers none.
        until: Option<EventTime>,
    },
    /// A book slippage rule on a sample that ends at or before the history
    /// chain's cutover (ADR-061 D4): the archive has no order book (ADR-043
    /// D11), so every fill would be censored `no_book`. Checked before
    /// anything is opened.
    BookNotDelivered {
        /// The sample's end.
        sample_end: EventTime,
        /// The chain's cutover, where its live segment and the book begin.
        cutover: EventTime,
    },
    /// The slippage calibration on a spec whose feature set does not
    /// contain `book.l2@1` (ADR-061 D7): the book its fills are priced on
    /// would not be pinned by the spec. Checked before anything is opened.
    BookNotInSet,
    /// A count or a sum of the slippage calibration would leave its integer
    /// range, or the slippage of a fill at the trade at `at` has no
    /// fixed-point value, such as at a print at or below 0 (ADR-027,
    /// ADR-061 D7). Nothing is stored.
    CalibrationOverflow {
        /// The time of the event that overflowed: the trade, the book event
        /// that settled the held fills, or the sample's end.
        at: EventTime,
    },
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
            Self::SampleBeforeOrigin { start, origin } => write!(
                f,
                "the sample starts at {} ms, before the history origin {} ms (ADR-052 D10)",
                start.as_millis(),
                origin.as_millis()
            ),
            Self::Unresolved(errors) => {
                f.write_str("the spec does not resolve under rule registry v1 (ADR-060 D2):")?;
                for error in errors {
                    write!(f, "\n{error}")?;
                }
                Ok(())
            }
            Self::FundingNotDelivered {
                sample_end,
                until: Some(cutover),
            } => write!(
                f,
                "the sample ends at {} ms, after the history cutover {} ms: settlements are \
                 delivered only up to the cutover, so funding.recorded@1 would book zero \
                 funding by omission (ADR-060 D7); end the sample at or before the cutover, \
                 or state funding.none@1",
                sample_end.as_millis(),
                cutover.as_millis()
            ),
            Self::FundingNotDelivered { until: None, .. } => f.write_str(
                "the history chain's archive does not replay fundingRate, so no settlement is \
                 delivered: funding.recorded@1 would book zero funding by omission (ADR-060 \
                 D7); replay fundingRate in the archive segment, or state funding.none@1",
            ),
            Self::BookNotDelivered {
                sample_end,
                cutover,
            } => write!(
                f,
                "the sample ends at {} ms, at or before the history cutover {} ms: the archive \
                 has no order book (ADR-043 D11), so a book slippage rule would censor every \
                 fill no_book (ADR-061 D4); end the sample after the cutover, or state \
                 slippage.fixed@1",
                sample_end.as_millis(),
                cutover.as_millis()
            ),
            Self::BookNotInSet => f.write_str(
                "the spec's feature set does not contain book.l2@1: the slippage calibration \
                 prices its fills on that book (ADR-061 D7); add book.l2@1 to the features line",
            ),
            Self::CalibrationOverflow { at } => write!(
                f,
                "the slippage calibration leaves the 64-bit fixed-point range at the event at {} \
                 ms (ADR-027, ADR-061 D7)",
                at.as_millis()
            ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::research::SpecField;

    #[test]
    fn the_backtest_refusals_describe_themselves() {
        let errors = vec![
            ResolutionError::UnknownRule {
                field: SpecField::Location,
                rule: "location.at_level@1".to_owned(),
            },
            ResolutionError::StateFilterRule {
                field: SpecField::StateFilter,
                rule: "state.trend@1".to_owned(),
            },
        ];
        let text = ResearchError::Unresolved(errors.clone()).to_string();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines[0],
            "the spec does not resolve under rule registry v1 (ADR-060 D2):"
        );
        assert_eq!(lines[1..], [errors[0].to_string(), errors[1].to_string()]);

        assert_eq!(
            ResearchError::FundingNotDelivered {
                sample_end: EventTime::from_millis(2_000),
                until: Some(EventTime::from_millis(1_000)),
            }
            .to_string(),
            "the sample ends at 2000 ms, after the history cutover 1000 ms: settlements are \
             delivered only up to the cutover, so funding.recorded@1 would book zero funding \
             by omission (ADR-060 D7); end the sample at or before the cutover, or state \
             funding.none@1"
        );
        assert_eq!(
            ResearchError::FundingNotDelivered {
                sample_end: EventTime::from_millis(2_000),
                until: None,
            }
            .to_string(),
            "the history chain's archive does not replay fundingRate, so no settlement is \
             delivered: funding.recorded@1 would book zero funding by omission (ADR-060 D7); \
             replay fundingRate in the archive segment, or state funding.none@1"
        );
        assert_eq!(
            ResearchError::BookNotDelivered {
                sample_end: EventTime::from_millis(1_000),
                cutover: EventTime::from_millis(1_000),
            }
            .to_string(),
            "the sample ends at 1000 ms, at or before the history cutover 1000 ms: the archive \
             has no order book (ADR-043 D11), so a book slippage rule would censor every fill \
             no_book (ADR-061 D4); end the sample after the cutover, or state slippage.fixed@1"
        );
        for error in [
            ResearchError::Unresolved(errors),
            ResearchError::FundingNotDelivered {
                sample_end: EventTime::from_millis(2_000),
                until: None,
            },
            ResearchError::BookNotDelivered {
                sample_end: EventTime::from_millis(1_000),
                cutover: EventTime::from_millis(1_000),
            },
        ] {
            assert!(std::error::Error::source(&error).is_none());
        }
    }

    #[test]
    fn the_calibration_refusals_describe_themselves() {
        assert_eq!(
            ResearchError::BookNotInSet.to_string(),
            "the spec's feature set does not contain book.l2@1: the slippage calibration prices \
             its fills on that book (ADR-061 D7); add book.l2@1 to the features line"
        );
        let overflow = ResearchError::CalibrationOverflow {
            at: EventTime::from_millis(1_250),
        };
        assert_eq!(
            overflow.to_string(),
            "the slippage calibration leaves the 64-bit fixed-point range at the event at 1250 \
             ms (ADR-027, ADR-061 D7)"
        );
        for error in [ResearchError::BookNotInSet, overflow] {
            assert!(std::error::Error::source(&error).is_none());
        }
    }
}
