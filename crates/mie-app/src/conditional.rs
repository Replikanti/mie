//! Drives the context sampler of ADR-054 over a market-data provider
//! (ADR-054 D5, D6; #25).
//!
//! [`sample_contexts`] runs [`ContextSampler`] through the one rejection
//! policy of long runs, [`drive_tolerant_observed`] (ADR-039 D9), and
//! guards it: the sampler sees **accepted events only**. The driver calls
//! its observer after a rejected event too, while the engine's closed bars
//! and trigger events still show the previous event, so without the guard
//! every rejection would repeat the previous event's records (ADR-054 D6).
//!
//! [`sample_window`] opens a replay, names the raw data behind it in a
//! [`ContextProvenance`] (D5: the data version from [`Replay::dataset`],
//! the engine key of a fresh engine, the sampling policy) and streams the
//! window's records. Records go to a sink as they are taken; nothing is
//! collected (D6).

use crate::drive_tolerant_observed;
use mie_domain::conditional::{ContextProvenance, ContextRecord, ContextSampler, SamplingStats};
use mie_domain::research::DataVersion;
use mie_domain::state::{MarketStateEngine, StateError};
use mie_domain::time::EventTime;
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use std::cell::Cell;

/// What one sampling drive did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SamplingReport {
    /// Events delivered, rejected ones included.
    pub events: u64,
    /// Events the engine rejected; the sampler saw none of them.
    pub rejections: u64,
    /// The sampler's counts after the drive, over every record it emitted
    /// so far.
    pub stats: SamplingStats,
}

/// Feeds every event from `provider` into `engine` and every accepted one
/// into `sampler`, which passes its records to `emit` (ADR-054 D6). A
/// domain rejection does not stop the drive: `on_rejection` gets the error,
/// the sampler does not see the event, and driving resumes (ADR-039 D9).
///
/// The engine and the sampler are the caller's, so a drive can continue a
/// warm engine, a warm sampler, or both: a caller that warms the engine up
/// alone and then samples has the sampler observe from that point on, and
/// episodes reaching before it are truncated (ADR-054 D4).
///
/// # Errors
///
/// The first provider failure. Events consumed before it stay applied to
/// `engine` and `sampler`, and their records are emitted.
pub fn sample_contexts<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    sampler: &mut ContextSampler,
    mut on_rejection: impl FnMut(&StateError),
    mut emit: impl FnMut(ContextRecord),
) -> Result<SamplingReport, ProviderError>
where
    P: MarketDataProvider + ?Sized,
{
    // Set by a rejection, cleared by the observer that follows it: the
    // driver calls `on_rejection` and then `observe` for the same event.
    let rejected = Cell::new(false);
    let mut rejections = 0;
    let events = drive_tolerant_observed(
        provider,
        engine,
        |error| {
            rejected.set(true);
            rejections += 1;
            on_rejection(error);
        },
        |event, engine| {
            if !rejected.replace(false) {
                sampler.observe(event, engine, &mut emit);
            }
        },
    )?;
    Ok(SamplingReport {
        events,
        rejections,
        stats: sampler.stats(),
    })
}

/// Samples the replay of `window` from `history` into a fresh engine and a
/// fresh [`ContextSampler::new`]`(emit_from)` (ADR-054 D5, D6): events
/// before `emit_from` warm the engine and the episodes up without emitting
/// records. Returns the run's provenance and what the drive did.
///
/// # Errors
///
/// [`ProviderError`] when the replay cannot be opened or its stream fails.
pub fn sample_window<H>(
    history: &H,
    window: ReplayWindow,
    emit_from: Option<EventTime>,
    emit: impl FnMut(ContextRecord),
) -> Result<(ContextProvenance, SamplingReport), ProviderError>
where
    H: HistoricalDataProvider,
{
    let Replay {
        mut stream,
        dataset,
    } = history.replay(window)?;
    let mut engine = MarketStateEngine::new();
    let provenance = ContextProvenance::new(DataVersion::from(&dataset), &engine);
    let mut sampler = ContextSampler::new(emit_from);
    let report = sample_contexts(&mut stream, &mut engine, &mut sampler, |_| {}, emit)?;
    Ok((provenance, report))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::conditional::{SAMPLING_V1, Sample, policy_fingerprint};
    use mie_domain::event::{Aggressor, MarketEvent, Trade};
    use mie_domain::feature::catalog;
    use mie_domain::num::{Price, Qty};
    use mie_domain::research::EngineKey;
    use mie_domain::state_hash::STATE_HASH_ENCODING;
    use mie_ports::raw::DatasetVersion;
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

    /// A replay source over fixed events, with a fixed dataset version.
    struct History(Vec<MarketEvent>, DatasetVersion);

    impl HistoricalDataProvider for History {
        type Stream = Feed;

        fn replay(&self, window: ReplayWindow) -> Result<Replay<Feed>, ProviderError> {
            let events: Vec<MarketEvent> = self
                .0
                .iter()
                .filter(|event| window.contains(event.time()))
                .cloned()
                .collect();
            Ok(Replay {
                stream: feed(&events),
                dataset: self.1.clone(),
            })
        }
    }

    fn trade(millis: i64, trade_id: u64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: EventTime::from_millis(millis),
            trade_id,
            price: Price::from_units(8_500_000_000_000),
            qty: Qty::from_units(1_000_000),
            aggressor: Aggressor::Buy,
        })
    }

    /// Two trades in minute 0, the trade that closes it at 61 s, a repeat
    /// of that trade, and a trade later in minute 1.
    fn tape_with_a_repeat() -> Vec<MarketEvent> {
        vec![
            trade(1_000, 1),
            trade(30_000, 2),
            trade(61_000, 3),
            trade(61_000, 3),
            trade(62_000, 4),
        ]
    }

    #[test]
    fn a_rejected_event_yields_no_record() {
        let events = tape_with_a_repeat();

        // The hazard the guard exists for: after the rejected repeat the
        // engine still shows the bar the previous event closed, so an
        // unguarded sampler takes its baseline sample a second time.
        let mut engine = MarketStateEngine::new();
        let mut unguarded = ContextSampler::new(None);
        let mut unguarded_records = 0;
        for event in &events {
            let _ = engine.apply(event);
            unguarded.observe(event, &engine, |_| unguarded_records += 1);
        }
        assert_eq!(unguarded_records, 2, "the repeat would duplicate a record");

        let mut engine = MarketStateEngine::new();
        let mut sampler = ContextSampler::new(None);
        let mut records = Vec::new();
        let mut errors = Vec::new();
        let report = sample_contexts(
            &mut feed(&events),
            &mut engine,
            &mut sampler,
            |error| errors.push(error.to_string()),
            |record| records.push(record),
        )
        .unwrap();
        assert_eq!(report.events, 5);
        assert_eq!(report.rejections, 1);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert_eq!(records.len(), 1, "{records:#?}");
        let record = &records[0];
        assert_eq!(record.event, events[2].canonical_key());
        assert_eq!(
            record.sample,
            Sample::Baseline {
                minute_end: EventTime::from_millis(60_000)
            }
        );
        assert_eq!(report.stats, sampler.stats());
        assert_eq!(report.stats.records, 1);
        assert_eq!(engine.state().trade_count, 4);
    }

    #[test]
    fn a_drive_continues_the_callers_engine_and_sampler() {
        // One drive over the tape and one drive per event give the same
        // records and counts: nothing but the engine and the sampler
        // carries over between drives.
        let events = tape_with_a_repeat();
        let mut whole = Vec::new();
        let report = sample_contexts(
            &mut feed(&events),
            &mut MarketStateEngine::new(),
            &mut ContextSampler::new(None),
            |_| {},
            |record| whole.push(record),
        )
        .unwrap();

        let mut engine = MarketStateEngine::new();
        let mut sampler = ContextSampler::new(None);
        let mut piecewise = Vec::new();
        let (mut delivered, mut rejections) = (0, 0);
        for event in &events {
            let step = sample_contexts(
                &mut feed(std::slice::from_ref(event)),
                &mut engine,
                &mut sampler,
                |_| {},
                |record| piecewise.push(record),
            )
            .unwrap();
            delivered += step.events;
            rejections += step.rejections;
        }
        assert_eq!(piecewise, whole);
        assert_eq!((delivered, rejections), (report.events, report.rejections));
        assert_eq!(sampler.stats(), report.stats);
    }

    #[test]
    fn provenance_names_the_replay_dataset_and_engine() {
        let dataset = DatasetVersion::from_hex(&"ab".repeat(32)).unwrap();
        let history = History(tape_with_a_repeat(), dataset.clone());
        let window = ReplayWindow {
            start: EventTime::from_millis(0),
            end: EventTime::from_millis(120_000),
        };
        let mut records = Vec::new();
        let (provenance, report) =
            sample_window(&history, window, None, |record| records.push(record)).unwrap();
        assert_eq!(provenance.data, DataVersion::from(&dataset));
        assert_eq!(
            provenance.engine,
            EngineKey {
                features: catalog::current_set().version(),
                state_encoding: STATE_HASH_ENCODING,
            }
        );
        assert_eq!(provenance.policy, SAMPLING_V1);
        assert_eq!(provenance.policy_fingerprint, policy_fingerprint());
        assert_eq!(report.events, 5);
        assert_eq!(report.rejections, 1);
        assert_eq!(records.len(), 1);

        // `emit_from` after the closing trade: the drive still runs, the
        // record is not emitted.
        let (_, late) = sample_window(
            &history,
            window,
            Some(EventTime::from_millis(61_001)),
            |_| panic!("no record is emitted before emit_from"),
        )
        .unwrap();
        assert_eq!(late.events, 5);
        assert_eq!(late.stats.records, 0);
    }
}
