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
//! [`drive_chain`] drives the history chain (ADR-052 D4): the same policy,
//! except that an uncrossable hole restarts a fresh engine, which moves the
//! chain's effective origin.
//!
//! [`equivalence`] records the state checkpoints that live ingestion
//! journals and the equivalence harness recomputes, through
//! [`drive_tolerant_observed`], and compares two checkpoint lists (#13,
//! ADR-041); for warm-started runs it splits the history chain at run
//! boundaries and checks each run's prefix (ADR-052 D9).
//!
//! [`ExperimentService`] ([`research`]) runs an experiment spec and is the
//! only writer of research results (ADR-020, ADR-040): `@1` over a replay
//! of the sample, `@2` on the history chain, with the prefix driven first
//! (ADR-052 D10).

pub mod equivalence;
pub mod kline_check;
pub mod research;

pub use research::{ChainBounds, ExperimentService, REPLAY_SUMMARY_V1, REPLAY_SUMMARY_V2};

use mie_domain::event::MarketEvent;
use mie_domain::event_hash::{EventStreamHash, EventStreamHasher};
use mie_domain::state::{MarketStateEngine, StateError};
use mie_domain::time::EventTime;
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

/// What [`drive_chain`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainDrive {
    /// Events delivered, rejected ones included.
    pub events: u64,
    /// The time of each event that restarted the engine after an
    /// uncrossable hole, in delivery order. The last one is the chain's
    /// effective origin (ADR-052 D4); none: the origin itself.
    pub restarts: Vec<EventTime>,
}

/// [`drive_tolerant`] for the history chain (ADR-052 D4): a hole is
/// uncrossable exactly when the engine rejects the event after it with
/// [`StateError::TimeJump`] (more than `MAX_BARS_PER_EVENT` one-minute bars,
/// ADR-031 D11). Then `engine` is replaced by a fresh engine with the same
/// feature set, the event's time is recorded as a restart, and the event is
/// applied to the fresh engine (a rejection there goes to `on_rejection`):
/// ADR-031 D11's recovery, "replayed as two runs, each from a fresh engine".
/// The chain's effective origin is the last restart's time. Every other
/// rejection is counted as in [`drive_tolerant`].
///
/// The bound is the domain's, never derived a second time, and no state
/// from before the hole survives the restart.
///
/// # Errors
///
/// The first provider failure. Events consumed before it stay applied to
/// `engine`.
pub fn drive_chain<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    on_rejection: impl FnMut(&StateError),
) -> Result<ChainDrive, ProviderError>
where
    P: MarketDataProvider + ?Sized,
{
    drive_chain_observed(provider, engine, on_rejection, |_, _| {})
}

/// [`drive_chain`], calling `observe` after every delivered event, accepted
/// or rejected, with the engine's state as of that event: after a restart,
/// the fresh engine's.
///
/// # Errors
///
/// As [`drive_chain`]; `observe` is not called after a provider failure.
pub fn drive_chain_observed<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    on_rejection: impl FnMut(&StateError),
    observe: impl FnMut(&MarketEvent, &MarketStateEngine),
) -> Result<ChainDrive, ProviderError>
where
    P: MarketDataProvider + ?Sized,
{
    drive_chain_with(provider, engine, on_rejection, |_| {}, observe)
}

/// [`drive_chain_observed`], calling `on_restart` with the time of each
/// restart as it happens, right after the fresh engine took the event and
/// before `observe`: a consumer can record a restart even when the provider
/// fails later and the [`ChainDrive`] is lost.
pub(crate) fn drive_chain_with<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    mut on_rejection: impl FnMut(&StateError),
    mut on_restart: impl FnMut(EventTime),
    mut observe: impl FnMut(&MarketEvent, &MarketStateEngine),
) -> Result<ChainDrive, ProviderError>
where
    P: MarketDataProvider + ?Sized,
{
    let mut drive = ChainDrive::default();
    while let Some(event) = provider.next_event()? {
        drive.events += 1;
        match engine.apply(&event) {
            Ok(()) => {}
            Err(StateError::TimeJump { .. }) => {
                *engine = MarketStateEngine::with_features(engine.feature_set().clone())
                    .expect("an engine's own feature set is computable");
                drive.restarts.push(event.time());
                let applied = engine.apply(&event);
                on_restart(event.time());
                if let Err(rejected) = applied {
                    on_rejection(&rejected);
                }
            }
            Err(rejected) => on_rejection(&rejected),
        }
        observe(&event, engine);
    }
    Ok(drive)
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

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::bars::MAX_BARS_PER_EVENT;
    use mie_domain::event::{Aggressor, FeedGap, GapReason, Stream, Trade};
    use mie_domain::feature::catalog;
    use mie_domain::num::{Price, Qty};
    use std::collections::VecDeque;

    struct Feed(VecDeque<MarketEvent>);

    impl MarketDataProvider for Feed {
        fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
            Ok(self.0.pop_front())
        }
    }

    fn feed(events: &[MarketEvent]) -> Feed {
        Feed(events.iter().cloned().collect())
    }

    fn sized_trade(millis: i64, trade_id: u64, qty_units: i64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: EventTime::from_millis(millis),
            trade_id,
            price: Price::from_units(8_500_000_000_000),
            qty: Qty::from_units(qty_units),
            aggressor: Aggressor::Buy,
        })
    }

    fn trade(millis: i64, trade_id: u64) -> MarketEvent {
        sized_trade(millis, trade_id, 1_000_000)
    }

    fn trades_gap(start: i64, end: i64) -> MarketEvent {
        MarketEvent::FeedGap(FeedGap {
            stream: Stream::Trades,
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(end),
            reason: GapReason::MissingData,
        })
    }

    /// Drives `events` into `engine`; returns the drive and the rejections.
    fn chain(events: &[MarketEvent], engine: &mut MarketStateEngine) -> (ChainDrive, Vec<String>) {
        let mut rejections = Vec::new();
        let drive = drive_chain(&mut feed(events), engine, |e| {
            rejections.push(e.to_string())
        })
        .unwrap();
        (drive, rejections)
    }

    /// The first trade at 1 s opens the 1m bar [0, 60 s); the bound counts
    /// from that bar's end (ADR-031 D11).
    const FIRST: i64 = 1_000;
    const BAR_END: i64 = 60_000;

    fn bound_ms() -> i64 {
        i64::try_from(MAX_BARS_PER_EVENT).unwrap() * 60_000
    }

    #[test]
    fn a_restart_happens_exactly_at_the_domain_bound() {
        let crossings: [fn(i64) -> MarketEvent; 2] =
            [|at| trade(at, 2), |at| trades_gap(FIRST, at)];
        for crossing in crossings {
            // One millisecond short of the bound: the engine crosses it.
            let below = BAR_END + bound_ms() - 1;
            let mut engine = MarketStateEngine::new();
            let (drive, rejections) = chain(&[trade(FIRST, 1), crossing(below)], &mut engine);
            assert_eq!(
                drive,
                ChainDrive {
                    events: 2,
                    restarts: Vec::new()
                }
            );
            assert!(rejections.is_empty(), "{rejections:?}");

            // At the bound: the engine rejects it with TimeJump, so the chain
            // restarts there.
            let at = BAR_END + bound_ms();
            let mut plain = MarketStateEngine::new();
            plain.apply(&trade(FIRST, 1)).unwrap();
            assert!(matches!(
                plain.apply(&crossing(at)),
                Err(StateError::TimeJump { .. })
            ));
            let mut engine = MarketStateEngine::new();
            let (drive, rejections) = chain(&[trade(FIRST, 1), crossing(at)], &mut engine);
            assert_eq!(
                drive,
                ChainDrive {
                    events: 2,
                    restarts: vec![EventTime::from_millis(at)]
                }
            );
            assert!(rejections.is_empty(), "{rejections:?}");
        }
    }

    #[test]
    fn no_state_from_before_the_hole_survives_a_restart() {
        // The last trade before the hole develops the bar [60 s, 120 s).
        let at = 120_000 + bound_ms();
        let before = [trade(FIRST, 1), trade(FIRST + 30_000, 2), trade(90_000, 3)];
        let after = [
            trades_gap(90_000, at),
            trade(at, 4),
            trade(at + 70_000, 5),
            trade(at + 200_000, 6),
        ];
        let mut events = before.to_vec();
        events.extend(after.iter().cloned());
        let mut engine = MarketStateEngine::new();
        let (drive, rejections) = chain(&events, &mut engine);
        assert_eq!(drive.restarts, [EventTime::from_millis(at)]);
        assert_eq!(drive.events, events.len() as u64);
        assert!(rejections.is_empty(), "{rejections:?}");

        let mut fresh = MarketStateEngine::new();
        for event in &after {
            fresh.apply(event).unwrap();
        }
        assert_eq!(engine.state().state_hash(), fresh.state().state_hash());
        assert_eq!(engine.state().trade_count, 3);
    }

    #[test]
    fn a_restart_keeps_the_feature_set() {
        let levels_v1 = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key]).unwrap();
        assert_ne!(levels_v1, catalog::current_set());
        let mut engine = MarketStateEngine::with_features(levels_v1.clone()).unwrap();
        let at = BAR_END + bound_ms();
        let (drive, _) = chain(&[trade(FIRST, 1), trade(at, 2)], &mut engine);
        assert_eq!(drive.restarts.len(), 1);
        assert_eq!(engine.feature_set(), &levels_v1);
        assert_eq!(engine.state().feature_set, levels_v1.version());
    }

    #[test]
    fn another_rejection_is_counted_not_restarted() {
        // As the domain's CVD overflow test: the first day's bars hold
        // `i64::MAX` units, the next day's trade overflows the continuous
        // CVD.
        let events = [
            sized_trade(500, 1, i64::MAX),
            sized_trade(86_400_000, 2, 1),
            trade(86_400_001, 3),
        ];
        let mut engine = MarketStateEngine::new();
        let (drive, rejections) = chain(&events, &mut engine);
        assert!(drive.restarts.is_empty());
        assert_eq!(drive.events, 3);
        assert!(
            rejections.first().is_some_and(|r| r.contains("overflows")),
            "the overflow is counted: {rejections:?}"
        );
    }

    #[test]
    fn observe_sees_the_fresh_engine() {
        let at = BAR_END + bound_ms();
        let events = [trade(FIRST, 1), trade(FIRST + 1, 2), trade(at, 3)];
        let mut seen = Vec::new();
        let drive = drive_chain_observed(
            &mut feed(&events),
            &mut MarketStateEngine::new(),
            |e| panic!("{e}"),
            |event, engine| seen.push((event.time().as_millis(), engine.state().trade_count)),
        )
        .unwrap();
        assert_eq!(drive.restarts.len(), 1);
        assert_eq!(seen, [(FIRST, 1), (FIRST + 1, 2), (at, 1)]);
    }
}
