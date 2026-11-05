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
//!   through [`drive_chain`](crate::drive_chain), and records the prefix's
//!   provenance and the state hashes at the sample's start and end. The
//!   engine computes all of
//!   [`catalog::CURRENT`], so those hashes depend on the build's feature
//!   set and state-hash encoding: the result is keyed by that engine too
//!   ([`EngineKey`]), and a build with another engine records its result
//!   next to the old one instead of diverging from it. Within one key, a
//!   different result is a determinism failure or a definition change
//!   without a version bump (ADR-029), and the run fails.
//!
//! [`BacktestService`] runs [`BACKTEST_V1`] (ADR-060 D13): the spec's
//! hypothesis simulated on the history chain. It checks the feature set,
//! resolves the spec's rules under rule registry v1 and checks the funding
//! coverage, all before anything is opened, then hands the resolved plan
//! and the checked engine to a `BacktestEngine` (ADR-060 D14), which
//! drives the chain as `@2` does, through the same driver, so the result's
//! twelve provenance lines are `@2`'s. The result is keyed by the engine
//! like `@2`'s and recorded under the same rules.

use crate::{HashingProvider, drive_chain_with, drive_tolerant};
use mie_domain::backtest::{FundingRule, resolve};
use mie_domain::event::MarketEvent;
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
    BacktestEngine, BacktestRun, HistoricalDataProvider, MarketDataProvider, Replay, ReplayWindow,
    ResearchResultStore, ResultStoreError,
};
use std::cell::Cell;

/// `research.replay_summary@1`: the provenance-only pipeline (module docs).
pub const REPLAY_SUMMARY_V1: PipelineKey = PipelineKey::new("research.replay_summary", 1);

/// `research.replay_summary@2`: the provenance pipeline on the history
/// chain, with the prefix's provenance and the state hashes (module docs).
pub const REPLAY_SUMMARY_V2: PipelineKey = PipelineKey::new("research.replay_summary", 2);

/// `research.backtest@1`: the spec's hypothesis simulated on the history
/// chain (ADR-060 D13, module docs).
pub const BACKTEST_V1: PipelineKey = PipelineKey::new("research.backtest", 1);

/// The history chain's bounds (ADR-052 D1, D2), as the composition root
/// reads them from the configuration, so the core stays pure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainBounds {
    /// The chain's origin: the start of every prefix.
    pub origin: EventTime,
    /// The cutover between the archive and the live segment.
    pub cutover: EventTime,
}

/// Which samples the history chain delivers every funding settlement of,
/// or the gap of a missing one (ADR-060 D7), as the composition root reads
/// it from the configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FundingCoverage {
    /// The latest sample end the chain covers: its cutover, since live
    /// delivers no settlement event. `None`: the chain's archive does not
    /// replay `fundingRate`, so it covers no sample.
    pub until: Option<EventTime>,
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
    ///    engine through [`drive_chain`](crate::drive_chain) (none when the
    ///    sample starts at the origin);
    /// 4. the sample is driven through the domain with the live rejection
    ///    policy ([`drive_tolerant`]; on the chain
    ///    [`drive_chain`](crate::drive_chain), into the engine the prefix
    ///    left) and must deliver at least one event;
    /// 5. a result already stored under the key must be identical
    ///    ([`ExperimentRun::Reproduced`]); otherwise the run
    ///    [`Diverged`](ResearchError::Diverged) and nothing is written. A
    ///    new result is appended ([`ExperimentRun::Recorded`]). A `@2` key
    ///    includes the engine, so another build's result is another key.
    fn run(&mut self, spec: &ExperimentSpec) -> Result<ExperimentRun, ResearchError> {
        let engine = checked_engine(spec)?;
        let result = match self.chain {
            None => self.replay_summary(spec, engine)?,
            Some(bounds) => self.history_replay_summary(spec, engine, bounds)?,
        };
        record(&mut self.store, result)
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
        engine: MarketStateEngine,
        bounds: ChainBounds,
    ) -> Result<ExperimentResult, ResearchError> {
        let (summary, _) = drive_history(&self.history, spec, engine, bounds, |_| {})?;
        Ok(ExperimentResult {
            spec: spec.clone(),
            pipeline: REPLAY_SUMMARY_V2,
            outcome: Outcome::HistoryReplaySummary(summary),
        })
    }
}

/// [`RunResearchExperiment`] for [`BACKTEST_V1`] over a [`BacktestEngine`]
/// and a result store (ADR-060 D13, D14).
#[derive(Debug)]
pub struct BacktestService<B, S> {
    engine: B,
    store: S,
    funding: FundingCoverage,
    /// The chain's cutover: the book exists only in the live segment after
    /// it (ADR-043 D11, ADR-061 D4).
    cutover: EventTime,
}

impl<B: BacktestEngine, S: ResearchResultStore> BacktestService<B, S> {
    /// Creates the service over `engine`, which runs on a history chain
    /// whose settlements `funding` covers and whose live segment starts at
    /// `cutover`. It is the only holder of the writable store.
    pub fn new(engine: B, store: S, funding: FundingCoverage, cutover: EventTime) -> Self {
        Self {
            engine,
            store,
            funding,
            cutover,
        }
    }

    /// The result store, read-only.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Unwraps the engine and the store.
    pub fn into_parts(self) -> (B, S) {
        (self.engine, self.store)
    }
}

impl<B: BacktestEngine, S: ResearchResultStore> RunResearchExperiment for BacktestService<B, S> {
    /// Runs [`BACKTEST_V1`] (ADR-060 D13). The first three checks are pure,
    /// so a spec that fails one opens nothing:
    ///
    /// 1. the feature set, as [`ExperimentService`] checks it (ADR-040 D4);
    /// 2. the rules resolve under rule registry v1 (ADR-060 D2), else
    ///    [`ResearchError::Unresolved`] with every error;
    /// 3. under `funding.recorded@1`, the chain delivers the sample's
    ///    settlements: the sample ends at or before [`FundingCoverage`]'s
    ///    bound, else [`ResearchError::FundingNotDelivered`] (D7);
    /// 4. under a book slippage rule, the sample reaches the book: it ends
    ///    after the cutover, else [`ResearchError::BookNotDelivered`]
    ///    (ADR-061 D4);
    /// 5. the engine refuses a sample before the origin, opens the sample,
    ///    checks its data version (ADR-040 D5) and drives the prefix and
    ///    the sample (ADR-052 D10);
    /// 6. the result is keyed by the engine of its provenance and recorded
    ///    as [`ExperimentService`] records: `Recorded`, `Reproduced` or
    ///    `Diverged` (ADR-040 D7).
    fn run(&mut self, spec: &ExperimentSpec) -> Result<ExperimentRun, ResearchError> {
        let engine = checked_engine(spec)?;
        let plan = resolve(spec).map_err(ResearchError::Unresolved)?;
        if plan.costs().funding() == FundingRule::Recorded {
            let sample_end = spec.sample().end();
            if !self.funding.until.is_some_and(|until| sample_end <= until) {
                return Err(ResearchError::FundingNotDelivered {
                    sample_end,
                    until: self.funding.until,
                });
            }
        }
        // The archive has no book, so a sample that ends at or before the
        // cutover would censor every fill `no_book` (ADR-061 D4).
        if plan.costs().slippage().is_book() {
            let sample_end = spec.sample().end();
            if sample_end <= self.cutover {
                return Err(ResearchError::BookNotDelivered {
                    sample_end,
                    cutover: self.cutover,
                });
            }
        }
        let BacktestRun {
            summary,
            stats,
            records,
        } = self.engine.run(spec, &plan, engine)?;
        let result = ExperimentResult {
            spec: spec.clone(),
            pipeline: BACKTEST_V1,
            outcome: Outcome::Backtest {
                summary,
                stats,
                records,
            },
        };
        record(&mut self.store, result)
    }
}

/// One step of [`drive_history`], in delivery order: a chain restart comes
/// before the step of the event that caused it.
pub(crate) enum ChainStep<'a> {
    /// The engine was replaced by a fresh one after an uncrossable hole
    /// (ADR-052 D4).
    Restart,
    /// The engine accepted `event`; `engine` is its state as of the event.
    Accepted {
        /// The event.
        event: &'a MarketEvent,
        /// The engine after the event.
        engine: &'a MarketStateEngine,
        /// Whether the event is in the sample, not the prefix.
        sample: bool,
    },
    /// The engine rejected `event`, in the prefix or the sample alike: a
    /// rejection censors what is open, and the prefix opens nothing.
    Rejected {
        /// The event.
        event: &'a MarketEvent,
    },
}

/// Drives `spec`'s sample on the history chain within `bounds` (ADR-052
/// D10) and returns its provenance with the sample's stream, from which
/// a caller reads the trailing gaps: a sample that starts before the
/// origin is refused, the sample is opened and its data version checked,
/// the prefix `[origin, sample start)` drives `engine` (none when the
/// sample starts at the origin), then the sample, which must deliver an
/// event. Both drives go through the chain driver, and `step` sees every
/// delivered event and restart ([`ChainStep`]).
///
/// The one implementation of the chain provenance of `@2` and the
/// backtest (ADR-052 D10, ADR-060 D13).
pub(crate) fn drive_history<H: HistoricalDataProvider>(
    history: &H,
    spec: &ExperimentSpec,
    mut engine: MarketStateEngine,
    bounds: ChainBounds,
    mut step: impl FnMut(ChainStep<'_>),
) -> Result<(HistoryReplaySummary, H::Stream), ResearchError> {
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
    // drive. Opening selects files and computes the version; the stream
    // reads them only when driven.
    let Replay { stream, dataset } = history.replay(sample)?;
    let opened = DataVersion::from(&dataset);
    if &opened != spec.data_version() {
        return Err(ResearchError::DataVersionMismatch {
            declared: spec.data_version().clone(),
            opened,
        });
    }

    // The adapters refuse an empty window, so a sample at the origin
    // opens no prefix.
    let (prefix_data, prefix_stream, prefix_rejections, prefix_restarts) = if sample.start
        > bounds.origin
    {
        let prefix = ReplayWindow {
            start: bounds.origin,
            end: sample.start,
        };
        let Replay {
            stream: prefix_stream,
            dataset: prefix_dataset,
        } = history.replay(prefix)?;
        let mut prefix_stream = HashingProvider::new(prefix_stream);
        let (drive, rejections) = drive_stepped(&mut prefix_stream, &mut engine, false, &mut step)?;
        (
            Some(DataVersion::from(&prefix_dataset)),
            prefix_stream.hash(),
            rejections,
            drive,
        )
    } else {
        (None, EventStreamHasher::new().finish(), 0, Vec::new())
    };
    let state_start = engine.state().state_hash();

    let mut stream = HashingProvider::new(stream);
    let (restarts, rejections) = drive_stepped(&mut stream, &mut engine, true, &mut step)?;
    let hash = stream.hash();
    if hash.events == 0 {
        return Err(ResearchError::EmptySample);
    }

    let summary = HistoryReplaySummary {
        origin: bounds.origin,
        effective_origin: restarts.last().or(prefix_restarts.last()).copied(),
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
    };
    Ok((summary, stream.into_inner()))
}

/// One chain drive of [`drive_history`]: every event of `provider` into
/// `engine`, each delivered event and restart passed to `step` with
/// `sample`. Returns the restarts' times and the number of rejections.
///
/// The flags follow `drive_chain_alerts`: the rejection and the restart
/// callbacks only set them, and the observer, which runs after both for
/// the same event, steps the restart first, then the event.
fn drive_stepped<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    sample: bool,
    step: &mut impl FnMut(ChainStep<'_>),
) -> Result<(Vec<EventTime>, u64), ResearchError>
where
    P: MarketDataProvider + ?Sized,
{
    let rejected = Cell::new(false);
    let restarted = Cell::new(false);
    let mut rejections = 0;
    let drive = drive_chain_with(
        provider,
        engine,
        |_| rejected.set(true),
        |_| restarted.set(true),
        |event, engine| {
            if restarted.replace(false) {
                step(ChainStep::Restart);
            }
            if rejected.replace(false) {
                rejections += 1;
                step(ChainStep::Rejected { event });
            } else {
                step(ChainStep::Accepted {
                    event,
                    engine,
                    sample,
                });
            }
        },
    )?;
    Ok((drive.restarts, rejections))
}

/// The engine for `spec`, checked against its feature set (ADR-040 D4,
/// ADR-044 D10): [`engine_for`]'s, and every member of the spec's set must
/// be computed by it at the same key and definition — a subset check, so
/// new features in the default set never orphan an old spec.
///
/// # Errors
///
/// [`ResearchError::FeatureUnavailable`] naming the first member the engine
/// does not compute.
fn checked_engine(spec: &ExperimentSpec) -> Result<MarketStateEngine, ResearchError> {
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
    Ok(engine)
}

/// Step 5 of [`RunResearchExperiment::run`] for both services: compares
/// `result` with the one stored under its key, or appends it.
fn record<S: ResearchResultStore>(
    store: &mut S,
    result: ExperimentResult,
) -> Result<ExperimentRun, ResearchError> {
    let key = result.key();
    if let Some(stored) = store.get(&key)? {
        return compare(stored, result);
    }
    match store.append(&result) {
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
