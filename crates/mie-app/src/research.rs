//! The research pipeline: runs an experiment spec and records its result
//! (ADR-040). The only writer of results (ADR-020).
//!
//! [`ExperimentService`] ships two pipelines:
//!
//! - [`REPLAY_SUMMARY_V1`] ([`ExperimentService::new`]) replays the spec's
//!   sample period through the domain and records what was delivered (the
//!   event-stream hash, ADR-039 D7) and what the domain rejected (ADR-039
//!   D9). That is the provenance every later pipeline builds on, and it
//!   proves reproduction end to end: a re-run on the same data version
//!   yields the same result, and a stored result is never replaced.
//! - [`REPLAY_SUMMARY_V2`] ([`ExperimentService::on_chain`]) runs on the
//!   history chain (ADR-052 D10): it drives `[origin, sample start)` into
//!   the engine without evaluating it, then the sample as `@1` does, both
//!   through [`drive_chain`], and records the prefix's provenance and the
//!   state hashes at the sample's start and end. The engine computes all of
//!   [`catalog::CURRENT`], so those hashes depend on the build's feature
//!   set and state-hash encoding: the result is keyed by that engine too
//!   ([`EngineKey`]), and a build with another engine records its result
//!   next to the old one instead of diverging from it. Within one key, a
//!   different result is a determinism failure or a definition change
//!   without a version bump (ADR-029), and the run fails.
//!
//! The backtest (#30) adds its own pipeline and outcome kind.

use crate::{HashingProvider, drive_chain, drive_tolerant};
use mie_domain::event_hash::EventStreamHasher;
use mie_domain::feature::{FeatureKey, catalog};
use mie_domain::research::{
    DataVersion, EngineKey, ExperimentResult, ExperimentSpec, HistoryReplaySummary, Outcome,
    PipelineKey,
};
use mie_domain::state::{MarketStateEngine, UnsupportedFeatureSet};
use mie_domain::state_hash::STATE_HASH_ENCODING;
use mie_domain::time::EventTime;
use mie_ports::inbound::{ExperimentRun, ResearchError, RunResearchExperiment};
use mie_ports::outbound::{
    HistoricalDataProvider, Replay, ReplayWindow, ResearchResultStore, ResultStoreError,
};

/// `research.replay_summary@1`: the provenance-only pipeline (module docs).
pub const REPLAY_SUMMARY_V1: PipelineKey = PipelineKey::new("research.replay_summary", 1);

/// `research.replay_summary@2`: the provenance pipeline on the history
/// chain, with the prefix's provenance and the state hashes (module docs).
pub const REPLAY_SUMMARY_V2: PipelineKey = PipelineKey::new("research.replay_summary", 2);

/// The history chain's bounds (ADR-052 D1, D2), as the composition root
/// reads them from the configuration, so the core stays pure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainBounds {
    /// The chain's origin: the start of every prefix.
    pub origin: EventTime,
    /// The cutover between the archive and the live segment.
    pub cutover: EventTime,
}

/// [`RunResearchExperiment`] over a historical data source and a result
/// store.
#[derive(Debug)]
pub struct ExperimentService<H, S> {
    history: H,
    store: S,
    /// `Some`: `history` is the history chain, and the service runs
    /// [`REPLAY_SUMMARY_V2`].
    chain: Option<ChainBounds>,
}

impl<H: HistoricalDataProvider, S: ResearchResultStore> ExperimentService<H, S> {
    /// Creates the service running [`REPLAY_SUMMARY_V1`]. It is the only
    /// holder of the writable store.
    pub fn new(history: H, store: S) -> Self {
        Self {
            history,
            store,
            chain: None,
        }
    }

    /// Creates the service running [`REPLAY_SUMMARY_V2`] over `history`,
    /// the history chain within `bounds`. It is the only holder of the
    /// writable store.
    pub fn on_chain(history: H, store: S, bounds: ChainBounds) -> Self {
        Self {
            history,
            store,
            chain: Some(bounds),
        }
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
    /// Runs [`REPLAY_SUMMARY_V1`], or [`REPLAY_SUMMARY_V2`] for a service
    /// [`on_chain`](ExperimentService::on_chain):
    ///
    /// 1. the engine computes [`catalog::CURRENT`] with each member of the
    ///    spec's feature set listed in [`catalog::SELECTABLE`] in place of
    ///    the current version of its id (ADR-040 D4, ADR-044 D10), and
    ///    every member of the spec's set must be computed by that engine at
    ///    the same key and definition — a subset check, so new features in
    ///    the default set never orphan an old spec;
    /// 2. the replay of the sample must open exactly the spec's data
    ///    version; on the chain, a sample that starts before the origin is
    ///    refused first ([`ResearchError::SampleBeforeOrigin`]), and the
    ///    sample is opened and checked before the prefix is driven;
    /// 3. on the chain, the prefix `[origin, sample start)` drives the
    ///    engine through [`drive_chain`] (none when the sample starts at the
    ///    origin);
    /// 4. the sample is driven through the domain with the live rejection
    ///    policy ([`drive_tolerant`]; on the chain [`drive_chain`], into the
    ///    engine the prefix left) and must deliver at least one event;
    /// 5. a result already stored under the key must be identical
    ///    ([`ExperimentRun::Reproduced`]); otherwise the run
    ///    [`Diverged`](ResearchError::Diverged) and nothing is written. A
    ///    new result is appended ([`ExperimentRun::Recorded`]). A `@2` key
    ///    includes the engine, so another build's result is another key.
    fn run(&mut self, spec: &ExperimentSpec) -> Result<ExperimentRun, ResearchError> {
        let engine = engine_for(spec)?;
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
        let result = match self.chain {
            None => self.replay_summary(spec, engine)?,
            Some(bounds) => self.history_replay_summary(spec, engine, bounds)?,
        };
        self.record(result)
    }
}

impl<H: HistoricalDataProvider, S: ResearchResultStore> ExperimentService<H, S> {
    /// The [`REPLAY_SUMMARY_V1`] result of `spec` (steps 2 and 4 of
    /// [`RunResearchExperiment::run`]).
    fn replay_summary(
        &self,
        spec: &ExperimentSpec,
        mut engine: MarketStateEngine,
    ) -> Result<ExperimentResult, ResearchError> {
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

        Ok(ExperimentResult {
            spec: spec.clone(),
            pipeline: REPLAY_SUMMARY_V1,
            outcome: Outcome::ReplaySummary {
                stream: hash,
                domain_rejections,
            },
        })
    }

    /// The [`REPLAY_SUMMARY_V2`] result of `spec` on the chain within
    /// `bounds` (steps 2 to 4 of [`RunResearchExperiment::run`]).
    fn history_replay_summary(
        &self,
        spec: &ExperimentSpec,
        mut engine: MarketStateEngine,
        bounds: ChainBounds,
    ) -> Result<ExperimentResult, ResearchError> {
        let sample = ReplayWindow {
            start: spec.sample().start(),
            end: spec.sample().end(),
        };
        if sample.start < bounds.origin {
            return Err(ResearchError::SampleBeforeOrigin {
                start: sample.start,
                origin: bounds.origin,
            });
        }
        // The sample is opened first, so a data mismatch costs no prefix
        // drive. Opening selects files and computes the version; the
        // stream reads them only when driven.
        let Replay { stream, dataset } = self.history.replay(sample)?;
        let opened = DataVersion::from(&dataset);
        if &opened != spec.data_version() {
            return Err(ResearchError::DataVersionMismatch {
                declared: spec.data_version().clone(),
                opened,
            });
        }

        // The adapters refuse an empty window, so a sample at the origin
        // opens no prefix.
        let mut prefix_rejections = 0;
        let (prefix_data, prefix_stream, prefix_restarts) = if sample.start > bounds.origin {
            let prefix = ReplayWindow {
                start: bounds.origin,
                end: sample.start,
            };
            let Replay {
                stream: prefix_stream,
                dataset: prefix_dataset,
            } = self.history.replay(prefix)?;
            let mut prefix_stream = HashingProvider::new(prefix_stream);
            let drive = drive_chain(&mut prefix_stream, &mut engine, |_| {
                prefix_rejections += 1;
            })?;
            (
                Some(DataVersion::from(&prefix_dataset)),
                prefix_stream.hash(),
                drive.restarts,
            )
        } else {
            (None, EventStreamHasher::new().finish(), Vec::new())
        };
        let state_start = engine.state().state_hash();

        let mut stream = HashingProvider::new(stream);
        let mut rejections = 0;
        let drive = drive_chain(&mut stream, &mut engine, |_| rejections += 1)?;
        let hash = stream.hash();
        if hash.events == 0 {
            return Err(ResearchError::EmptySample);
        }

        Ok(ExperimentResult {
            spec: spec.clone(),
            pipeline: REPLAY_SUMMARY_V2,
            outcome: Outcome::HistoryReplaySummary(HistoryReplaySummary {
                origin: bounds.origin,
                effective_origin: drive.restarts.last().or(prefix_restarts.last()).copied(),
                cutover: bounds.cutover,
                prefix_data,
                prefix_stream,
                prefix_rejections,
                engine: EngineKey {
                    features: engine.feature_set().version(),
                    state_encoding: STATE_HASH_ENCODING,
                },
                state_start,
                stream: hash,
                rejections,
                state_end: engine.state().state_hash(),
            }),
        })
    }

    /// Step 5 of [`RunResearchExperiment::run`]: compares `result` with the
    /// one stored under its key, or appends it.
    fn record(&mut self, result: ExperimentResult) -> Result<ExperimentRun, ResearchError> {
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

/// The engine for `spec`: [`catalog::CURRENT`] with the spec's
/// [`catalog::SELECTABLE`] members in place of the current versions of
/// their ids. Any other member the engine does not compute is left to the
/// subset check, which names it.
///
/// # Errors
///
/// [`ResearchError::FeatureUnavailable`] if the selected versions do not
/// form a set the engine computes — impossible while the catalog tests
/// pass.
fn engine_for(spec: &ExperimentSpec) -> Result<MarketStateEngine, ResearchError> {
    let older: Vec<FeatureKey> = spec
        .features()
        .definitions()
        .map(|definition| definition.key)
        .filter(|key| catalog::SELECTABLE.contains(key))
        .collect();
    let Some(&first) = older.first() else {
        return Ok(MarketStateEngine::new());
    };
    let features = catalog::current_set_with(&older)
        .map_err(|_| ResearchError::FeatureUnavailable { feature: first })?;
    MarketStateEngine::with_features(features).map_err(|error| {
        let (UnsupportedFeatureSet::NotComputed { feature }
        | UnsupportedFeatureSet::Redefined { feature }
        | UnsupportedFeatureSet::Missing { feature }) = error;
        ResearchError::FeatureUnavailable { feature }
    })
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
