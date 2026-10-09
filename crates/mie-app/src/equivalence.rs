//! State checkpoints of the live/replay equivalence harness (#13, ADR-019,
//! ADR-041).
//!
//! Live ingestion and the harness's recompute of a live run drive their
//! engines through the same [`drive_checkpointed`], so both record their
//! checkpoints with the same [`CheckpointRecorder`] at the same events:
//!
//! - **Cadence.** A checkpoint is recorded after the event that moves the
//!   engine's `as_of` into a later `as_of.div_euclid(interval_ms)` bucket
//!   than the last one seen; the first accepted event only sets the bucket.
//!   `as_of` moves only on accepted events, so a rejected event — even a
//!   far-future one — never records a checkpoint or skews the cadence. A
//!   jump over several buckets records one checkpoint.
//! - **Content.** The number of events delivered so far, rejections
//!   included (`ordinal`, the `run_end.events` count), `as_of`, the
//!   event-stream hash v1 of those events (ADR-039 D7) and the Market State
//!   hash ([`StateHash`]).
//! - **End.** After the last event of a run that ended without a provider
//!   failure, a `last` checkpoint is recorded, unless the last event already
//!   recorded one.
//!
//! [`compare`] reports the first checkpoint at which two lists differ and
//! whether the event streams diverged or only the states. The cadence,
//! `as_of`, the end marker and the state all depend on what the engine
//! accepts, so a run recorded with another feature set is checked with
//! [`compare_events`] instead: each recorded checkpoint's ordinal and
//! event-stream hash against the replay's hash after as many events
//! ([`EventPrefixes`]), which no engine change can move (ADR-041).

use crate::drive_tolerant_observed;
use mie_domain::event::MarketEvent;
use mie_domain::event_hash::{EventStreamHash, EventStreamHasher};
use mie_domain::state::{MarketStateEngine, StateError};
use mie_domain::state_hash::StateHash;
use mie_domain::time::EventTime;
use mie_ports::outbound::{MarketDataProvider, ProviderError};
use std::fmt;

/// The state of a run after its first `ordinal` delivered events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateCheckpoint {
    /// Events delivered so far, rejected ones included.
    pub ordinal: u64,
    /// The engine's `as_of` after them.
    pub as_of: Option<EventTime>,
    /// Event-stream hash v1 of the delivered events.
    pub events: EventStreamHash,
    /// The Market State hash after them.
    pub state: StateHash,
    /// Whether this is the checkpoint after the run's last event.
    pub last: bool,
}

/// Records the checkpoints of one run (module docs).
#[derive(Debug, Clone)]
pub struct CheckpointRecorder {
    interval_ms: i64,
    hasher: EventStreamHasher,
    delivered: u64,
    /// The `as_of` bucket of the last accepted event seen.
    bucket: Option<i64>,
    /// The ordinal of the last recorded checkpoint.
    recorded: Option<u64>,
}

impl CheckpointRecorder {
    /// A recorder with a checkpoint every `interval_ms` of event time.
    ///
    /// # Panics
    ///
    /// If `interval_ms` is not positive.
    pub fn new(interval_ms: i64) -> Self {
        assert!(interval_ms > 0, "checkpoint interval {interval_ms} ms");
        Self {
            interval_ms,
            hasher: EventStreamHasher::new(),
            delivered: 0,
            bucket: None,
            recorded: None,
        }
    }

    /// Takes the next delivered event, right after `engine` accepted or
    /// rejected it, and returns the checkpoint it records, if any.
    pub fn observe(
        &mut self,
        event: &MarketEvent,
        engine: &MarketStateEngine,
    ) -> Option<StateCheckpoint> {
        self.hasher.push(event);
        self.delivered += 1;
        let bucket = engine
            .state()
            .as_of?
            .as_millis()
            .div_euclid(self.interval_ms);
        let crossed = self.bucket.is_some_and(|last| bucket > last);
        if self.bucket.is_none() || crossed {
            self.bucket = Some(bucket);
        }
        if !crossed {
            return None;
        }
        self.recorded = Some(self.delivered);
        Some(self.checkpoint(engine, false))
    }

    /// The `last` checkpoint after the run's final event: `None` when no
    /// event was delivered or the final event recorded a checkpoint.
    pub fn finish(&self, engine: &MarketStateEngine) -> Option<StateCheckpoint> {
        if self.delivered == 0 || self.recorded == Some(self.delivered) {
            return None;
        }
        Some(self.checkpoint(engine, true))
    }

    fn checkpoint(&self, engine: &MarketStateEngine, last: bool) -> StateCheckpoint {
        let state = engine.state();
        StateCheckpoint {
            ordinal: self.delivered,
            as_of: state.as_of,
            events: self.hasher.finish(),
            state: state.state_hash(),
            last,
        }
    }
}

/// [`drive_tolerant`](crate::drive_tolerant) with a [`CheckpointRecorder`]:
/// `on_checkpoint` gets every checkpoint as it is recorded, and the `last`
/// one when the provider ends without a failure. Returns the number of
/// events delivered, rejected ones included. Live ingestion and the
/// equivalence recompute both drive through it.
///
/// # Errors
///
/// The first provider failure; no `last` checkpoint is recorded then.
///
/// # Panics
///
/// If `interval_ms` is not positive.
pub fn drive_checkpointed<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    interval_ms: i64,
    on_rejection: impl FnMut(&StateError),
    mut on_checkpoint: impl FnMut(&StateCheckpoint),
) -> Result<u64, ProviderError>
where
    P: MarketDataProvider + ?Sized,
{
    let mut recorder = CheckpointRecorder::new(interval_ms);
    let events = drive_tolerant_observed(provider, engine, on_rejection, |event, engine| {
        if let Some(checkpoint) = recorder.observe(event, engine) {
            on_checkpoint(&checkpoint);
        }
    })?;
    if let Some(checkpoint) = recorder.finish(engine) {
        on_checkpoint(&checkpoint);
    }
    Ok(events)
}

/// How two checkpoint lists first differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivergenceKind {
    /// The ordinal, `as_of`, event-stream hash or end marker differs: the
    /// two sides delivered different events.
    EventStream,
    /// The events are equal, the Market State hash differs.
    State,
    /// One list ends before the other.
    Missing,
}

impl fmt::Display for DivergenceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EventStream => "event stream",
            Self::State => "state",
            Self::Missing => "missing checkpoint",
        })
    }
}

/// The first checkpoint at which two lists differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Divergence {
    /// Its position in the lists, from 0.
    pub index: usize,
    /// The live checkpoint; `None` when the live list ended before it.
    pub live: Option<StateCheckpoint>,
    /// The replay checkpoint; `None` when the replay list ended before it.
    pub replay: Option<StateCheckpoint>,
    /// How they differ.
    pub kind: DivergenceKind,
}

/// The result of [`compare`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    /// Every checkpoint matches.
    Equivalent {
        /// The number of checkpoints compared.
        checkpoints: usize,
    },
    /// The first difference.
    Diverged(Divergence),
}

/// Compares two checkpoint lists recorded with the same engine in order and
/// reports the first difference only. Runs recorded with another feature
/// set go through [`compare_events`] (module docs).
pub fn compare(live: &[StateCheckpoint], replay: &[StateCheckpoint]) -> Comparison {
    for index in 0..live.len().max(replay.len()) {
        let (l, r) = (live.get(index), replay.get(index));
        let kind = match (l, r) {
            (Some(l), Some(r)) => {
                if (l.ordinal, l.as_of, l.events, l.last) != (r.ordinal, r.as_of, r.events, r.last)
                {
                    DivergenceKind::EventStream
                } else if l.state != r.state {
                    DivergenceKind::State
                } else {
                    continue;
                }
            }
            _ => DivergenceKind::Missing,
        };
        return Comparison::Diverged(Divergence {
            index,
            live: l.copied(),
            replay: r.copied(),
            kind,
        });
    }
    Comparison::Equivalent {
        checkpoints: live.len(),
    }
}

/// A provider that also takes the event-stream hash of what it delivered
/// after each ordinal of a recorded checkpoint list, for [`compare_events`].
#[derive(Debug)]
pub struct EventPrefixes<'a, P: ?Sized> {
    inner: &'a mut P,
    hasher: EventStreamHasher,
    delivered: u64,
    /// The recorded ordinals, in list order.
    ordinals: Vec<u64>,
    /// The hash after each of the first `hashes.len()` ordinals.
    hashes: Vec<EventStreamHash>,
}

impl<'a, P: ?Sized> EventPrefixes<'a, P> {
    /// Wraps `inner`; hashes are taken at the ordinals of `recorded`.
    pub fn new(inner: &'a mut P, recorded: &[StateCheckpoint]) -> Self {
        Self {
            inner,
            hasher: EventStreamHasher::new(),
            delivered: 0,
            ordinals: recorded.iter().map(|c| c.ordinal).collect(),
            hashes: Vec::new(),
        }
    }

    /// The hashes taken so far, one per recorded ordinal reached in order.
    pub fn into_hashes(self) -> Vec<EventStreamHash> {
        self.hashes
    }
}

impl<P: MarketDataProvider + ?Sized> MarketDataProvider for EventPrefixes<'_, P> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        let event = self.inner.next_event()?;
        if let Some(event) = &event {
            self.hasher.push(event);
            self.delivered += 1;
            while self.ordinals.get(self.hashes.len()) == Some(&self.delivered) {
                self.hashes.push(self.hasher.finish());
            }
        }
        Ok(event)
    }
}

/// The first recorded checkpoint whose events the replay did not deliver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventDivergence {
    /// Its position in the recorded list, from 0.
    pub index: usize,
    /// The recorded checkpoint.
    pub live: StateCheckpoint,
    /// The replay's hash after `live.ordinal` events; `None` when the
    /// replay delivered fewer events.
    pub replay: Option<EventStreamHash>,
}

/// The result of [`compare_events`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventComparison {
    /// Every recorded checkpoint's events match.
    Equivalent {
        /// The number of checkpoints compared.
        checkpoints: usize,
    },
    /// The first difference.
    Diverged(EventDivergence),
}

/// Compares the delivered events only: each `live` checkpoint's event-stream
/// hash with the replay's hash after as many events (`replay`, from
/// [`EventPrefixes`] over the same list). `as_of`, the end marker, the state
/// and where checkpoints fall depend on the engine and are ignored, so a run
/// recorded with another feature set diverges here only when the events
/// differ (ADR-041). Events the replay delivers after the last recorded
/// checkpoint are left to the caller's total.
pub fn compare_events(live: &[StateCheckpoint], replay: &[EventStreamHash]) -> EventComparison {
    for (index, l) in live.iter().enumerate() {
        let r = replay.get(index).copied();
        if r != Some(l.events) {
            return EventComparison::Diverged(EventDivergence {
                index,
                live: *l,
                replay: r,
            });
        }
    }
    EventComparison::Equivalent {
        checkpoints: live.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HashingProvider;
    use mie_domain::event::{Aggressor, Trade};
    use mie_domain::fingerprint::Fingerprint;
    use mie_domain::num::{Price, Qty};
    use std::collections::VecDeque;

    struct Feed(VecDeque<MarketEvent>);

    impl MarketDataProvider for Feed {
        fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
            Ok(self.0.pop_front())
        }
    }

    fn trade(millis: i64, trade_id: u64, qty_units: i64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: EventTime::from_millis(millis),
            trade_id,
            price: Price::from_units(8_500_000_000_000),
            qty: Qty::from_units(qty_units),
            aggressor: Aggressor::Buy,
        })
    }

    /// Records the checkpoints of `events` at `interval_ms`, with the
    /// rejections.
    fn run(events: &[MarketEvent], interval_ms: i64) -> (Vec<StateCheckpoint>, u64) {
        let mut checkpoints = Vec::new();
        let mut rejections = 0;
        let delivered = drive_checkpointed(
            &mut Feed(events.iter().cloned().collect()),
            &mut MarketStateEngine::new(),
            interval_ms,
            |_| rejections += 1,
            |c| checkpoints.push(*c),
        )
        .unwrap();
        assert_eq!(delivered, events.len() as u64);
        (checkpoints, rejections)
    }

    fn hash_of(events: &[MarketEvent]) -> EventStreamHash {
        let mut provider = HashingProvider::new(Feed(events.iter().cloned().collect()));
        while provider.next_event().unwrap().is_some() {}
        provider.hash()
    }

    /// Trades at 1 s, 5 s, 12 s, 13 s, 45 s (a jump over three buckets) and
    /// 47 s.
    fn tape() -> Vec<MarketEvent> {
        [1_000, 5_000, 12_000, 13_000, 45_000, 47_000]
            .iter()
            .enumerate()
            .map(|(i, &t)| trade(t, 1 + i as u64, 1_000_000))
            .collect()
    }

    #[test]
    fn one_checkpoint_per_bucket_crossing_and_one_last() {
        let events = tape();
        let (checkpoints, rejections) = run(&events, 10_000);
        assert_eq!(rejections, 0);
        let at: Vec<(u64, Option<i64>, bool)> = checkpoints
            .iter()
            .map(|c| (c.ordinal, c.as_of.map(EventTime::as_millis), c.last))
            .collect();
        // The first event sets the bucket; 12 s and 45 s cross; 47 s is the
        // last.
        assert_eq!(
            at,
            [
                (3, Some(12_000), false),
                (5, Some(45_000), false),
                (6, Some(47_000), true)
            ]
        );
        for c in &checkpoints {
            let prefix = &events[..c.ordinal as usize];
            assert_eq!(c.events, hash_of(prefix));
            let mut engine = MarketStateEngine::new();
            for event in prefix {
                engine.apply(event).unwrap();
            }
            assert_eq!(c.state, engine.state().state_hash());
        }
    }

    #[test]
    fn a_final_event_that_records_a_checkpoint_is_not_repeated() {
        let events = &tape()[..5];
        let (checkpoints, _) = run(events, 10_000);
        assert_eq!(checkpoints.len(), 2);
        assert!(!checkpoints[1].last);
        assert_eq!(checkpoints[1].ordinal, 5);
        // No event, no checkpoint.
        assert!(run(&[], 10_000).0.is_empty());
        // A lone event records the last one.
        let (lone, _) = run(&tape()[..1], 10_000);
        assert_eq!(lone.len(), 1);
        assert!(lone[0].last);
        assert_eq!(lone[0].ordinal, 1);
    }

    #[test]
    fn rejected_events_count_but_never_record_or_skew_the_cadence() {
        let mut events = tape();
        // A duplicate after 5 s and a far-future trade that would close
        // more bars than the engine allows (a time jump), after 13 s.
        events.insert(2, events[1].clone());
        events.insert(5, trade(5_000_000_000, 99, 1_000_000));
        let (checkpoints, rejections) = run(&events, 10_000);
        assert_eq!(rejections, 2);
        let at: Vec<(u64, Option<i64>, bool)> = checkpoints
            .iter()
            .map(|c| (c.ordinal, c.as_of.map(EventTime::as_millis), c.last))
            .collect();
        assert_eq!(
            at,
            [
                (4, Some(12_000), false),
                (7, Some(45_000), false),
                (8, Some(47_000), true)
            ]
        );
        // The event hash covers the rejected events too.
        assert_eq!(checkpoints[2].events, hash_of(&events));
        assert_eq!(checkpoints[2].events.events, 8);
    }

    #[test]
    #[should_panic(expected = "checkpoint interval 0 ms")]
    fn the_interval_must_be_positive() {
        let _ = CheckpointRecorder::new(0);
    }

    #[test]
    fn compare_reports_the_first_difference() {
        let events = tape();
        let (base, _) = run(&events, 10_000);
        assert_eq!(
            compare(&base, &base),
            Comparison::Equivalent { checkpoints: 3 }
        );

        // One event changed after 12 s: the second checkpoint diverges.
        let mut perturbed = events.clone();
        perturbed[3] = trade(13_000, 4, 2_000_000);
        let (other, _) = run(&perturbed, 10_000);
        let Comparison::Diverged(d) = compare(&base, &other) else {
            panic!("equivalent")
        };
        assert_eq!((d.index, d.kind), (1, DivergenceKind::EventStream));
        assert_eq!((d.live, d.replay), (Some(base[1]), Some(other[1])));

        // Equal events, another state.
        let mut state = base.clone();
        state[2].state = StateHash::from_fingerprint(Fingerprint::from_raw(7));
        let Comparison::Diverged(d) = compare(&base, &state) else {
            panic!("equivalent")
        };
        assert_eq!((d.index, d.kind), (2, DivergenceKind::State));
        let mut moved = base.clone();
        moved[0].as_of = Some(EventTime::from_millis(12_001));
        let Comparison::Diverged(d) = compare(&base, &moved) else {
            panic!("equivalent")
        };
        assert_eq!((d.index, d.kind), (0, DivergenceKind::EventStream));

        // A shorter list.
        let Comparison::Diverged(d) = compare(&base, &base[..2]) else {
            panic!("equivalent")
        };
        assert_eq!(
            (d.index, d.kind, d.live, d.replay),
            (2, DivergenceKind::Missing, Some(base[2]), None)
        );
        let Comparison::Diverged(d) = compare(&[], &base) else {
            panic!("equivalent")
        };
        assert_eq!((d.index, d.live, d.replay), (0, None, Some(base[0])));
        assert_eq!(DivergenceKind::State.to_string(), "state");
    }

    /// The hashes [`EventPrefixes`] takes from `events` at the ordinals of
    /// `recorded`, after delivering all of them.
    fn prefixes(recorded: &[StateCheckpoint], events: &[MarketEvent]) -> Vec<EventStreamHash> {
        let mut feed = Feed(events.iter().cloned().collect());
        let mut provider = EventPrefixes::new(&mut feed, recorded);
        while provider.next_event().unwrap().is_some() {}
        provider.into_hashes()
    }

    #[test]
    fn compare_events_ignores_everything_the_engine_decides() {
        let events = tape();
        let (base, _) = run(&events, 10_000);
        let hashes = prefixes(&base, &events);
        assert_eq!(hashes, base.iter().map(|c| c.events).collect::<Vec<_>>());
        assert_eq!(
            compare_events(&base, &hashes),
            EventComparison::Equivalent { checkpoints: 3 }
        );

        // Another engine recorded the same events with other `as_of`
        // values, end marker and states, at another cadence: the events
        // still match.
        let mut engine_moved = base.clone();
        engine_moved[0].as_of = Some(EventTime::from_millis(12_001));
        engine_moved[1].state = StateHash::from_fingerprint(Fingerprint::from_raw(7));
        engine_moved[2].last = false;
        assert_eq!(
            compare_events(&engine_moved, &prefixes(&engine_moved, &events)),
            EventComparison::Equivalent { checkpoints: 3 }
        );
        let (dense, _) = run(&events, 1_000);
        assert!(dense.len() > base.len());
        assert_eq!(
            compare_events(&dense, &prefixes(&dense, &events)),
            EventComparison::Equivalent {
                checkpoints: dense.len()
            }
        );

        // One event changed after 12 s: the second checkpoint diverges.
        let mut perturbed = events.clone();
        perturbed[3] = trade(13_000, 4, 2_000_000);
        let EventComparison::Diverged(d) = compare_events(&base, &prefixes(&base, &perturbed))
        else {
            panic!("equivalent")
        };
        assert_eq!((d.index, d.live), (1, base[1]));
        assert_eq!(d.replay, Some(hash_of(&perturbed[..5])));

        // A replay that ends early has no hash at the last ordinal.
        let EventComparison::Diverged(d) = compare_events(&base, &prefixes(&base, &events[..5]))
        else {
            panic!("equivalent")
        };
        assert_eq!((d.index, d.replay), (2, None));
        assert_eq!(
            compare_events(&[], &[]),
            EventComparison::Equivalent { checkpoints: 0 }
        );
    }
}
