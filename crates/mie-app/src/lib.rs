//! MIE application layer: use-case services that orchestrate the domain
//! through ports without owning any infrastructure (brief §3).
//!
//! [`drive_observed`] is the single place where market data meets the
//! domain; [`drive`] and the [`kline_check`] harness go through it. Live
//! analysis and historical replay both use it — replay is an input
//! difference, not a second implementation (ADR-019).
//!
//! [`drive_tolerant`] is the one rejection policy of long runs: live
//! ingestion and [`ReplayService`] both count a domain rejection and keep
//! driving (ADR-039 D9). [`HashingProvider`] identifies what a provider
//! delivered (ADR-039 D7).
//!
//! [`equivalence`] records the state checkpoints that live ingestion
//! journals and the equivalence harness recomputes, through
//! [`drive_tolerant_observed`], and compares two checkpoint lists (#13,
//! ADR-041).
//!
//! [`ExperimentService`] ([`research`]) runs an experiment spec and is the
//! only writer of research results (ADR-020, ADR-040).

pub mod equivalence;
pub mod kline_check;
pub mod research;

pub use research::{ExperimentService, REPLAY_SUMMARY_V1};

use mie_domain::event::MarketEvent;
use mie_domain::event_hash::{EventStreamHash, EventStreamHasher};
use mie_domain::state::{MarketStateEngine, StateError};
use mie_ports::inbound::{ReplayMarket, ReplayReport, UseCaseError};
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};

/// Feeds every event from `provider` into `engine` until the stream ends and
/// returns the number of events consumed.
///
/// # Errors
///
/// The first provider failure or domain rejection. Events consumed before it
/// stay applied to `engine`.
pub fn drive<P>(provider: &mut P, engine: &mut MarketStateEngine) -> Result<u64, UseCaseError>
where
    P: MarketDataProvider + ?Sized,
{
    drive_observed(provider, engine, |_, _| {})
}

/// [`drive`], calling `observe` with each event right after `engine`
/// accepted it, so an observer sees the state — and the bars the event
/// closed — exactly as of that event, never later.
///
/// # Errors
///
/// As [`drive`]; `observe` is not called for the event that failed.
pub fn drive_observed<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    mut observe: impl FnMut(&MarketEvent, &MarketStateEngine),
) -> Result<u64, UseCaseError>
where
    P: MarketDataProvider + ?Sized,
{
    let mut events = 0;
    while let Some(event) = provider.next_event()? {
        engine.apply(&event)?;
        observe(&event, engine);
        events += 1;
    }
    Ok(events)
}

/// Feeds every event from `provider` into `engine` until the stream ends,
/// like [`drive`], but a domain rejection does not stop it: `on_rejection`
/// gets the error and driving resumes. The engine leaves its state untouched
/// on a rejection, so the next event applies to the same state. Returns the
/// number of events delivered, rejected ones included.
///
/// This is the rejection policy of live ingestion and of replay alike
/// (ADR-019, ADR-039 D9): a long run must not stop on one bad event, and the
/// count makes every rejection visible.
///
/// # Errors
///
/// The first provider failure. Events consumed before it stay applied to
/// `engine`.
pub fn drive_tolerant<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    on_rejection: impl FnMut(&StateError),
) -> Result<u64, ProviderError>
where
    P: MarketDataProvider + ?Sized,
{
    drive_tolerant_observed(provider, engine, on_rejection, |_, _| {})
}

/// [`drive_tolerant`], calling `observe` after every delivered event,
/// accepted or rejected (after `on_rejection` for a rejected one), with the
/// engine's state as of that event. The equivalence checkpoints of live
/// ingestion and of their recompute are recorded here ([`equivalence`]).
///
/// # Errors
///
/// As [`drive_tolerant`]; `observe` is not called after a provider failure.
pub fn drive_tolerant_observed<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    mut on_rejection: impl FnMut(&StateError),
    mut observe: impl FnMut(&MarketEvent, &MarketStateEngine),
) -> Result<u64, ProviderError>
where
    P: MarketDataProvider + ?Sized,
{
    let mut events = 0;
    while let Some(event) = provider.next_event()? {
        events += 1;
        if let Err(rejected) = engine.apply(&event) {
            on_rejection(&rejected);
        }
        observe(&event, engine);
    }
    Ok(events)
}

/// A [`MarketDataProvider`] that hashes and counts every event it passes
/// on, in delivery order (ADR-039 D7).
#[derive(Debug)]
pub struct HashingProvider<P> {
    inner: P,
    hasher: EventStreamHasher,
}

impl<P> HashingProvider<P> {
    /// Wraps `inner`.
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            hasher: EventStreamHasher::new(),
        }
    }

    /// The hash of the events delivered so far.
    pub fn hash(&self) -> EventStreamHash {
        self.hasher.finish()
    }

    /// The wrapped provider.
    pub fn inner(&self) -> &P {
        &self.inner
    }

    /// Unwraps the provider.
    pub fn into_inner(self) -> P {
        self.inner
    }
}

impl<P: MarketDataProvider> MarketDataProvider for HashingProvider<P> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        let event = self.inner.next_event()?;
        if let Some(event) = &event {
            self.hasher.push(event);
        }
        Ok(event)
    }
}

/// [`ReplayMarket`] over any [`HistoricalDataProvider`].
#[derive(Debug)]
pub struct ReplayService<H> {
    history: H,
}

impl<H: HistoricalDataProvider> ReplayService<H> {
    /// Creates the service over a historical data source.
    pub fn new(history: H) -> Self {
        Self { history }
    }
}

impl<H: HistoricalDataProvider> ReplayMarket for ReplayService<H> {
    fn replay(&self, window: ReplayWindow) -> Result<ReplayReport, UseCaseError> {
        let Replay { stream, dataset } = self.history.replay(window)?;
        let mut stream = HashingProvider::new(stream);
        let mut engine = MarketStateEngine::new();
        let mut domain_rejections = 0;
        let events = drive_tolerant(&mut stream, &mut engine, |_| domain_rejections += 1)?;
        Ok(ReplayReport {
            events,
            dataset,
            stream_hash: stream.hash(),
            domain_rejections,
            state: engine.state().clone(),
        })
    }
}
