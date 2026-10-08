//! The research pipeline: runs an experiment spec and records its result
//! (ADR-040). The only writer of results (ADR-020).
//!
//! [`ExperimentService`] ships one pipeline, [`REPLAY_SUMMARY_V1`]: it
//! replays the spec's sample period through the domain and records what was
//! delivered (the event-stream hash, ADR-039 D7) and what the domain
//! rejected (ADR-039 D9). That is the provenance every later pipeline
//! builds on, and it proves reproduction end to end: a re-run on the same
//! data version yields the same result, and a stored result is never
//! replaced. The backtest (#30) adds its own pipeline and outcome kind.

use crate::{HashingProvider, drive_tolerant};
use mie_domain::research::{DataVersion, ExperimentResult, ExperimentSpec, Outcome, PipelineKey};
use mie_domain::state::MarketStateEngine;
use mie_ports::inbound::{ExperimentRun, ResearchError, RunResearchExperiment};
use mie_ports::outbound::{
    HistoricalDataProvider, Replay, ReplayWindow, ResearchResultStore, ResultStoreError,
};

/// `research.replay_summary@1`: the provenance-only pipeline (module docs).
pub const REPLAY_SUMMARY_V1: PipelineKey = PipelineKey::new("research.replay_summary", 1);

/// [`RunResearchExperiment`] over a historical data source and a result
/// store.
#[derive(Debug)]
pub struct ExperimentService<H, S> {
    history: H,
    store: S,
}

impl<H: HistoricalDataProvider, S: ResearchResultStore> ExperimentService<H, S> {
    /// Creates the service. It is the only holder of the writable store.
    pub fn new(history: H, store: S) -> Self {
        Self { history, store }
    }

    /// The result store, read-only.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Unwraps the data source and the store.
    pub fn into_parts(self) -> (H, S) {
        (self.history, self.store)
    }
}

impl<H: HistoricalDataProvider, S: ResearchResultStore> RunResearchExperiment
    for ExperimentService<H, S>
{
    /// Runs [`REPLAY_SUMMARY_V1`]:
    ///
    /// 1. every member of the spec's feature set must be computed by the
    ///    engine at the same key and definition — a subset check, so new
    ///    features in the default set never orphan an old spec;
    /// 2. the replay of the sample must open exactly the spec's data
    ///    version;
    /// 3. the sample is driven through the domain with the live rejection
    ///    policy and must deliver at least one event;
    /// 4. a result already stored under the key must be identical
    ///    ([`ExperimentRun::Reproduced`]); otherwise the run
    ///    [`Diverged`](ResearchError::Diverged) and nothing is written. A
    ///    new result is appended ([`ExperimentRun::Recorded`]).
    fn run(&mut self, spec: &ExperimentSpec) -> Result<ExperimentRun, ResearchError> {
        let mut engine = MarketStateEngine::new();
        for member in spec.features().definitions() {
            let computed = engine
                .feature_set()
                .definitions()
                .any(|d| d.key == member.key && d.fingerprint() == member.fingerprint());
            if !computed {
                return Err(ResearchError::FeatureUnavailable {
                    feature: member.key,
                });
            }
        }

        let window = ReplayWindow {
            start: spec.sample().start(),
            end: spec.sample().end(),
        };
        let Replay { stream, dataset } = self.history.replay(window)?;
        let opened = DataVersion::from(&dataset);
        if &opened != spec.data_version() {
            return Err(ResearchError::DataVersionMismatch {
                declared: spec.data_version().clone(),
                opened,
            });
        }
        let mut stream = HashingProvider::new(stream);
        let mut domain_rejections = 0;
        drive_tolerant(&mut stream, &mut engine, |_| domain_rejections += 1)?;
        let hash = stream.hash();
        if hash.events == 0 {
            return Err(ResearchError::EmptySample);
        }

        let result = ExperimentResult {
            spec: spec.clone(),
            pipeline: REPLAY_SUMMARY_V1,
            outcome: Outcome::ReplaySummary {
                stream: hash,
                domain_rejections,
            },
        };
        let key = result.key();
        if let Some(stored) = self.store.get(&key)? {
            return compare(stored, result);
        }
        match self.store.append(&result) {
            Ok(_) => Ok(ExperimentRun::Recorded(result)),
            // Another writer stored the key between `get` and `append`.
            Err(ResultStoreError::AlreadyStored {
                identical: true, ..
            }) => Ok(ExperimentRun::Reproduced(result)),
            Err(ResultStoreError::AlreadyStored {
                key,
                identical: false,
            }) => Err(ResearchError::Diverged { key }),
            Err(error) => Err(error.into()),
        }
    }
}

/// A stored result against a fresh run of the same key.
fn compare(
    stored: ExperimentResult,
    fresh: ExperimentResult,
) -> Result<ExperimentRun, ResearchError> {
    if stored.spec.canonical_text() != fresh.spec.canonical_text() {
        return Err(ResultStoreError::Collision {
            experiment: fresh.spec.id(),
        }
        .into());
    }
    if stored.canonical_text() == fresh.canonical_text() {
        Ok(ExperimentRun::Reproduced(stored))
    } else {
        Err(ResearchError::Diverged { key: fresh.key() })
    }
}
