//! Drives the context sampler of ADR-054 and the forward labeler of
//! ADR-055 over a market-data provider (ADR-054 D5, D6, #25; ADR-055 D6,
//! D11, #26; the barrier scale of labeling v2: ADR-056, #151).
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
//!
//! [`label_contexts`] adds the [`ForwardLabeler`] to the same guarded
//! drive, in the ADR-055 D11 order: each accepted event goes to
//! [`ForwardLabeler::observe`] first, then to the sampler, whose records
//! are opened as labels before they reach their sink. A rejected event
//! goes to [`ForwardLabeler::reject`] only, which censors on a
//! trades-stream event and ignores the rest. [`finish_labels`] ends a
//! replay as ADR-055 D6 states: the provider's trailing gaps of the trades
//! stream to `observe`, in canonical order, no other stream's, then
//! [`ForwardLabeler::finish`]. [`label_window`] does both over a replay
//! window, with its [`LabelProvenance`], and labels only the records of a
//! record window, so the events after it give the last records their
//! lookahead. [`label_window_started`] is the same run with the provenance
//! handed to a sink before the first record, for the outcome dataset of
//! #27 (ADR-057).
//!
//! The labeler is built here and in tests only, never in live ingestion
//! (ADR-055 D11): labels read the trades after the decision, and this file
//! is the only one of `mie-app` that may name them
//! (`tools/check-architecture.sh`).

use crate::drive_tolerant_observed;
use mie_domain::conditional::label::{
    ForwardLabel, ForwardLabeler, LabelProvenance, LabelStats, Labeling,
};
use mie_domain::conditional::{ContextProvenance, ContextRecord, ContextSampler, SamplingStats};
use mie_domain::event::{FeedGap, MarketEvent, Stream};
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

/// What one labeling drive did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LabelReport {
    /// Events delivered, rejected ones included.
    pub events: u64,
    /// Events the engine rejected: the sampler saw none of them, the
    /// labeler had each reported to [`ForwardLabeler::reject`].
    pub rejections: u64,
    /// The sampler's counts after the drive.
    pub sampling: SamplingStats,
    /// The labeler's counts after the drive.
    pub labels: LabelStats,
}

/// Feeds every event from `provider` into `engine`, every accepted one into
/// `labeler` and `sampler`, and every record the sampler emits into
/// `labeler` (ADR-055 D11): an accepted event goes to
/// [`ForwardLabeler::observe`] first, which passes the labels it completes
/// to `on_label`, then to the sampler, whose records are opened as labels
/// and then passed to `on_record`. A domain rejection does not stop the
/// drive: `on_rejection` gets the error, the event goes to
/// [`ForwardLabeler::reject`] and to nothing else, and driving resumes
/// (ADR-039 D9). A rejected event therefore never opens or feeds a label;
/// one on the trades stream censors the labels not yet emitted (ADR-055
/// D6).
///
/// The drive does not end the labels: the engine, the sampler and the
/// labeler are the caller's, so a drive can continue warm ones, and one
/// drive per event is the same as one over the stream. End a replay with
/// [`finish_labels`] after its last event.
///
/// # Errors
///
/// The first provider failure. Events consumed before it stay applied to
/// `engine`, `sampler` and `labeler`, and their records and labels are
/// emitted.
pub fn label_contexts<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    sampler: &mut ContextSampler,
    labeler: &mut ForwardLabeler,
    on_rejection: impl FnMut(&StateError),
    on_record: impl FnMut(ContextRecord),
    on_label: impl FnMut(ForwardLabel),
) -> Result<LabelReport, ProviderError>
where
    P: MarketDataProvider + ?Sized,
{
    drive_labels(
        provider,
        engine,
        sampler,
        labeler,
        None,
        on_rejection,
        on_record,
        on_label,
    )
}

/// [`label_contexts`], with the sampler skipped at every event at or after
/// `sample_before`: such events take no record, and still feed the labels.
#[allow(clippy::too_many_arguments)]
fn drive_labels<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    sampler: &mut ContextSampler,
    labeler: &mut ForwardLabeler,
    sample_before: Option<EventTime>,
    mut on_rejection: impl FnMut(&StateError),
    mut on_record: impl FnMut(ContextRecord),
    mut on_label: impl FnMut(ForwardLabel),
) -> Result<LabelReport, ProviderError>
where
    P: MarketDataProvider + ?Sized,
{
    // Set by a rejection, cleared by the observer that follows it, as in
    // `sample_contexts`.
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
            if rejected.replace(false) {
                labeler.reject(event);
                return;
            }
            labeler.observe(event, &mut on_label);
            if sample_before.is_none_or(|before| event.time() < before) {
                sampler.observe(event, engine, |record| {
                    labeler.open(&record);
                    on_record(record);
                });
            }
        },
    )?;
    Ok(LabelReport {
        events,
        rejections,
        sampling: sampler.stats(),
        labels: labeler.stats(),
    })
}

/// Ends the labels of a replay after its last event (ADR-055 D6, D11):
/// passes exactly the [`Stream::Trades`] entries of `trailing_gaps` to
/// [`ForwardLabeler::observe`], in canonical order, then calls
/// [`ForwardLabeler::finish`], and passes every label to `on_label`.
///
/// `trailing_gaps` are the provider's gaps still open at its window's end,
/// which it reports out of band instead of delivering them (the
/// `HistoricalDataProvider` contract). A trades entry starts at the
/// stream's last delivered event, so it cuts the same windows as the end
/// of the data, and names the cut `Gap`, the outage the provider recorded,
/// instead of `EndOfData`. Another stream's trailing gap ends past the
/// data's end and would close windows the data never covered; labels read
/// trades only, so the labeler never takes it. The engine and the sampler
/// see none of them.
///
/// # Errors
///
/// [`ProviderError::Contract`], with the labeler untouched, when a trades
/// entry breaks that contract: it starts after its end (the engine's
/// [`StateError::InvalidGap`]), starts after the time up to which the
/// trades are known ([`ForwardLabeler::trades_known_until`]; an entry
/// before it in canonical order counts, at its end), or ends before that
/// time, sorting before a trades-stream event already observed. The
/// entries skip the engine, so these checks stand in for its own, and a
/// late start would close the windows between the known time and the
/// start as fully covered: trades nobody delivered. A start at or before
/// the known time only censors more, the D6 case of a gap opened at the
/// last delivered trade.
pub fn finish_labels(
    labeler: &mut ForwardLabeler,
    trailing_gaps: &[FeedGap],
    mut on_label: impl FnMut(ForwardLabel),
) -> Result<(), ProviderError> {
    let mut gaps: Vec<MarketEvent> = trailing_gaps
        .iter()
        .filter(|gap| gap.stream == Stream::Trades)
        .map(|gap| MarketEvent::FeedGap(*gap))
        .collect();
    gaps.sort();
    let entries = gaps.iter().filter_map(|gap| match gap {
        MarketEvent::FeedGap(gap) => Some(gap),
        _ => None,
    });
    // Every entry is checked before the labeler sees any: observing one
    // moves the known time to its end.
    let mut known = labeler.trades_known_until();
    for gap in entries {
        let broken = if gap.start > gap.end {
            Some(
                StateError::InvalidGap {
                    start: gap.start,
                    end: gap.end,
                }
                .to_string(),
            )
        } else {
            known.and_then(|known| {
                if gap.start > known {
                    Some(format!(
                        "it starts at {}, after the trades known up to {known}",
                        gap.start
                    ))
                } else if gap.end < known {
                    Some(format!(
                        "it ends at {}, before the trades known up to {known}",
                        gap.end
                    ))
                } else {
                    None
                }
            })
        };
        if let Some(detail) = broken {
            return Err(ProviderError::Contract(format!(
                "trailing trades gap {} to {}: {detail}",
                gap.start, gap.end
            )));
        }
        known = Some(gap.end);
    }
    for gap in &gaps {
        labeler.observe(gap, &mut on_label);
    }
    labeler.finish(on_label);
    Ok(())
}

/// Labels the replay of `window` from `history` under `labeling` (ADR-055
/// D6, D10, D11; ADR-056 for [`Labeling::V2`]): a fresh engine, a fresh
/// [`ContextSampler::new`]`(Some(records.start))` and a fresh
/// [`ForwardLabeler::new`]`(labeling)` observe every event of `window`, so
/// the labeler's own bars start where the engine's do, and the
/// records whose events fall in `records` are emitted to `on_record` and
/// labeled to `on_label`; records outside it are neither emitted nor
/// labeled. Events before `records.start` warm the engine and the
/// episodes up (ADR-054 D5), and every event up to `window.end` still
/// feeds the labels, so a caller that ends `records` a day before
/// `window` gives the last records their full lookahead instead of an
/// `EndOfData` censor.
///
/// `trailing_gaps` reads the stream's trailing gaps once it has ended
/// (they are provider-specific, reported out of band), and the replay ends
/// with [`finish_labels`]. Returns the run's provenance (the records' and
/// the labeling's) and what the drive did.
///
/// # Errors
///
/// [`ProviderError`] when the replay cannot be opened, its stream fails or
/// its trailing gaps break the [`finish_labels`] contract.
pub fn label_window<H>(
    history: &H,
    window: ReplayWindow,
    records: ReplayWindow,
    labeling: Labeling,
    trailing_gaps: impl FnOnce(&H::Stream) -> Vec<FeedGap>,
    on_record: impl FnMut(ContextRecord),
    on_label: impl FnMut(ForwardLabel),
) -> Result<(LabelProvenance, LabelReport), ProviderError>
where
    H: HistoricalDataProvider,
{
    label_window_started(
        history,
        window,
        records,
        labeling,
        trailing_gaps,
        |_| {},
        on_record,
        on_label,
    )
}

/// [`label_window`], with the run's provenance passed to `on_start` once
/// the replay has opened and before its first event, so a sink that stores
/// the provenance next to every row has it before the first record
/// (ADR-057 D3, #27). `on_start` is not called when the replay cannot be
/// opened.
///
/// # Errors
///
/// As [`label_window`].
#[allow(clippy::too_many_arguments)]
pub fn label_window_started<H>(
    history: &H,
    window: ReplayWindow,
    records: ReplayWindow,
    labeling: Labeling,
    trailing_gaps: impl FnOnce(&H::Stream) -> Vec<FeedGap>,
    on_start: impl FnOnce(&LabelProvenance),
    on_record: impl FnMut(ContextRecord),
    mut on_label: impl FnMut(ForwardLabel),
) -> Result<(LabelProvenance, LabelReport), ProviderError>
where
    H: HistoricalDataProvider,
{
    let Replay {
        mut stream,
        dataset,
    } = history.replay(window)?;
    let mut engine = MarketStateEngine::new();
    let provenance = LabelProvenance::new(
        ContextProvenance::new(DataVersion::from(&dataset), &engine),
        labeling,
    );
    on_start(&provenance);
    let mut sampler = ContextSampler::new(Some(records.start));
    let mut labeler = ForwardLabeler::new(labeling);
    let mut report = drive_labels(
        &mut stream,
        &mut engine,
        &mut sampler,
        &mut labeler,
        Some(records.end),
        |_| {},
        on_record,
        &mut on_label,
    )?;
    finish_labels(&mut labeler, &trailing_gaps(&stream), on_label)?;
    report.labels = labeler.stats();
    Ok((provenance, report))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::conditional::label::{
        Censor, CensorReason, LABELING_V2, Passage, Race, labeling_fingerprint,
    };
    use mie_domain::conditional::{SAMPLING_V1, Sample, policy_fingerprint};
    use mie_domain::event::{Aggressor, GapReason, MarketEvent, OpenInterest, Trade};
    use mie_domain::feature::catalog;
    use mie_domain::num::{Price, Qty, SCALE};
    use mie_domain::order::CanonicalKey;
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

    /// A replay source whose replays never open.
    struct Refusing;

    impl HistoricalDataProvider for Refusing {
        type Stream = Feed;

        fn replay(&self, _window: ReplayWindow) -> Result<Replay<Feed>, ProviderError> {
            Err(ProviderError::Source("no data".to_owned()))
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

    // ------------------------------------------------------------ labels

    const MINUTE: i64 = 60_000;
    const HOUR: i64 = 3_600_000;
    const DAY: i64 = 86_400_000;

    fn priced(millis: i64, trade_id: u64, units: i64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: EventTime::from_millis(millis),
            trade_id,
            price: Price::from_units(units),
            qty: Qty::from_units(1_000_000),
            aggressor: if trade_id.is_multiple_of(4) {
                Aggressor::Buy
            } else {
                Aggressor::Sell
            },
        })
    }

    fn trades_gap(stream: Stream, start: i64, end: i64) -> FeedGap {
        FeedGap {
            stream,
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(end),
            reason: GapReason::MissingData,
        }
    }

    /// `hours` hours of trades 5 s to 35 s apart around 85 000 USDT, a
    /// walk of up to ±20 USDT per trade, with even trade ids (so the id
    /// above any trade's is free), and an open-interest poll 3 s after
    /// every 5-minute boundary.
    fn walk(hours: i64) -> Vec<MarketEvent> {
        let mut seed: u64 = 0x6d69_6500_0026_0002;
        let mut next = |bound: u64| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            i64::try_from((seed >> 11) % bound).unwrap()
        };
        let mut events = Vec::new();
        let (mut time, mut units, mut trade_id) = (1_000, 85_000 * SCALE, 0);
        while time < hours * HOUR {
            time += 5_000 + next(30_000);
            units += (next(41) - 20) * SCALE;
            trade_id += 2;
            events.push(priced(time, trade_id, units));
        }
        for boundary in 1..hours * 12 {
            let time = boundary * 5 * MINUTE + 3_000;
            events.push(MarketEvent::OpenInterest(OpenInterest {
                time: EventTime::from_millis(time),
                open_interest: Qty::from_units(80_000 * SCALE + time),
                resolution_ms: 10_000,
            }));
        }
        events.sort();
        events
    }

    /// The first trade of `tape` at or after `from_ms`, with its index.
    fn trade_from(tape: &[MarketEvent], from_ms: i64) -> (usize, Trade) {
        tape.iter()
            .enumerate()
            .find_map(|(index, event)| match event {
                MarketEvent::Trade(trade) if trade.time.as_millis() >= from_ms => {
                    Some((index, *trade))
                }
                _ => None,
            })
            .expect("a trade")
    }

    /// `tape` with a trade right after the first trade at or after
    /// `from_ms` that repeats its id at 10 000 USDT more: the engine
    /// rejects it. Returns the tape and the repeated trade.
    fn with_a_repeated_trade(tape: &[MarketEvent], from_ms: i64) -> (Vec<MarketEvent>, Trade) {
        let (index, at) = trade_from(tape, from_ms);
        let mut tape = tape.to_vec();
        tape.insert(
            index + 1,
            priced(
                at.time.as_millis(),
                at.trade_id,
                at.price.units() + 10_000 * SCALE,
            ),
        );
        (tape, at)
    }

    /// What one labeling drive gave.
    struct Labeled {
        records: Vec<ContextRecord>,
        labels: Vec<ForwardLabel>,
        /// Per label, the index of the event whose observation emitted it
        /// (the tape's length for `finish_labels`); per-event drives only.
        emitted_at: Vec<usize>,
        report: LabelReport,
    }

    /// One [`label_contexts`] drive over `tape`, ended by [`finish_labels`]
    /// with `trailing`.
    fn label_whole(tape: &[MarketEvent], trailing: &[FeedGap]) -> Labeled {
        let mut engine = MarketStateEngine::new();
        let mut sampler = ContextSampler::new(None);
        let mut labeler = ForwardLabeler::new(Labeling::CURRENT);
        let (mut records, mut labels) = (Vec::new(), Vec::new());
        let mut report = label_contexts(
            &mut feed(tape),
            &mut engine,
            &mut sampler,
            &mut labeler,
            |_| {},
            |record| records.push(record),
            |label| labels.push(label),
        )
        .unwrap();
        finish_labels(&mut labeler, trailing, |label| labels.push(label)).unwrap();
        report.labels = labeler.stats();
        Labeled {
            records,
            labels,
            emitted_at: Vec::new(),
            report,
        }
    }

    /// One [`label_contexts`] drive per event of `tape`, ended by
    /// [`finish_labels`] with `trailing`.
    fn label_per_event(tape: &[MarketEvent], trailing: &[FeedGap]) -> Labeled {
        let mut engine = MarketStateEngine::new();
        let mut sampler = ContextSampler::new(None);
        let mut labeler = ForwardLabeler::new(Labeling::CURRENT);
        let (mut records, mut labels, mut emitted_at) = (Vec::new(), Vec::new(), Vec::new());
        let (mut events, mut rejections) = (0, 0);
        for (index, event) in tape.iter().enumerate() {
            let step = label_contexts(
                &mut feed(std::slice::from_ref(event)),
                &mut engine,
                &mut sampler,
                &mut labeler,
                |_| {},
                |record| records.push(record),
                |label| {
                    labels.push(label);
                    emitted_at.push(index);
                },
            )
            .unwrap();
            events += step.events;
            rejections += step.rejections;
        }
        finish_labels(&mut labeler, trailing, |label| {
            labels.push(label);
            emitted_at.push(tape.len());
        })
        .unwrap();
        Labeled {
            records,
            labels,
            emitted_at,
            report: LabelReport {
                events,
                rejections,
                sampling: sampler.stats(),
                labels: labeler.stats(),
            },
        }
    }

    /// `label` with every `EndOfData` censor renamed `Gap`.
    fn end_of_data_as_gap(label: &ForwardLabel) -> ForwardLabel {
        let mut label = label.clone();
        let rename = |censor: &mut Censor| {
            if censor.reason == CensorReason::EndOfData {
                censor.reason = CensorReason::Gap;
            }
        };
        for horizon in &mut label.horizons {
            if let Some(censor) = &mut horizon.censored {
                rename(censor);
            }
            if let Race::Censored(censor) = &mut horizon.race {
                rename(censor);
            }
        }
        for passage in [&mut label.level, &mut label.extreme].into_iter().flatten() {
            if let Passage::Censored(censor) = &mut passage.passage {
                rename(censor);
            }
        }
        label
    }

    #[test]
    fn rejected_events_never_open_or_feed_labels() {
        let tape = walk(3);
        let base = label_whole(&tape, &[]);
        assert_eq!(base.report.rejections, 0);
        assert!(base.records.len() > 100, "{}", base.records.len());
        assert_eq!(base.labels.len(), base.records.len());

        // A repeated open-interest poll: rejected, and nothing for the
        // labeler, since labels read trades only.
        let poll = tape
            .iter()
            .position(|event| {
                matches!(event, MarketEvent::OpenInterest(_)) && event.time().as_millis() >= HOUR
            })
            .unwrap();
        let mut repeated_poll = tape.clone();
        repeated_poll.insert(poll + 1, tape[poll].clone());
        let polled = label_whole(&repeated_poll, &[]);
        assert_eq!(polled.report.rejections, 1);
        assert_eq!(polled.records, base.records);
        assert_eq!(polled.labels, base.labels);

        // A trade that repeats the previous one's id 10 000 USDT higher:
        // rejected, so it opens no record and no label reads its price. On
        // the trades stream it censors every label not yet emitted at its
        // time, the last accepted trade's (ADR-055 D6); the labels opened
        // after it are untouched.
        let (repeated, at) = with_a_repeated_trade(&tape, 2 * HOUR);
        let run = label_whole(&repeated, &[]);
        assert_eq!(run.report.rejections, 1);
        assert_eq!(run.records, base.records);
        assert_eq!(run.labels.len(), run.records.len());
        let mut censored = 0;
        for (label, before) in run.labels.iter().zip(&base.labels) {
            assert_eq!((label.event, label.ordinal), (before.event, before.ordinal));
            for horizon in &label.horizons {
                assert!(horizon.up.size.units() < 5_000 * SCALE, "{label}");
                if let Some(censor) = horizon.censored
                    && censor.reason == CensorReason::Rejected
                {
                    assert!(label.event.time <= at.time, "{label}");
                    assert_eq!(censor.at, at.time, "{label}");
                    censored += 1;
                }
            }
            if label.event.time > at.time {
                assert_eq!(label, before);
            }
        }
        assert!(censored > 0, "the rejection censored no label");
    }

    #[test]
    fn a_label_drive_per_event_equals_one_over_the_stream() {
        // The measurement tool drives one event at a time: nothing but the
        // engine, the sampler and the labeler carries over between drives.
        let (tape, _) = with_a_repeated_trade(&walk(3), 2 * HOUR);
        let last = tape.last().unwrap().time().as_millis();
        let trailing = [trades_gap(Stream::Trades, last, last + HOUR)];
        let whole = label_whole(&tape, &trailing);
        let piecewise = label_per_event(&tape, &trailing);
        assert_eq!(piecewise.records, whole.records);
        assert_eq!(piecewise.labels, whole.labels);
        assert_eq!(piecewise.report, whole.report);
        assert_eq!(whole.report.events, tape.len() as u64);
        assert_eq!(whole.report.rejections, 1);
        let records = whole.records.len() as u64;
        assert_eq!(whole.report.sampling.records, records);
        assert_eq!(
            (whole.report.labels.opened, whole.report.labels.emitted),
            (records, records)
        );
    }

    #[test]
    fn finish_labels_takes_the_trades_trailing_gaps_only() {
        let tape = walk(3);
        let last_trade = tape
            .iter()
            .rev()
            .find(|event| matches!(event, MarketEvent::Trade(_)))
            .unwrap()
            .time()
            .as_millis();
        let end = 4 * HOUR;
        // A provider's trailing gaps: the trades stream's from its last
        // trade to the window end, and another stream's that ends a day
        // past the data.
        let trades = trades_gap(Stream::Trades, last_trade, end - 1);
        let other = trades_gap(Stream::OpenInterest, end - HOUR, end + DAY);
        let plain = label_whole(&tape, &[]);
        assert!(
            plain.labels.iter().any(|label| label.horizons[4]
                .censored
                .is_some_and(|censor| censor.reason == CensorReason::EndOfData
                    && censor.at.as_millis() == last_trade)),
            "the data end censors at the last trade"
        );
        // Another stream's trailing gap changes nothing.
        let with_other = label_whole(&tape, &[other]);
        assert_eq!(with_other.labels, plain.labels);
        // The trades stream's, listed after the other one, cuts the same
        // windows at the same time, and names the cut `Gap`.
        let with_trades = label_whole(&tape, &[other, trades]);
        let renamed: Vec<ForwardLabel> = plain.labels.iter().map(end_of_data_as_gap).collect();
        assert_ne!(renamed, plain.labels);
        assert_eq!(with_trades.labels, renamed);
        assert_eq!(with_trades.records, plain.records);
    }

    #[test]
    fn finish_labels_refuses_a_trades_trailing_gap_that_breaks_the_contract() {
        let tape = walk(3);
        let last_trade = tape
            .iter()
            .rev()
            .find(|event| matches!(event, MarketEvent::Trade(_)))
            .unwrap()
            .time()
            .as_millis();
        let plain = label_whole(&tape, &[]);
        let drive = || {
            let mut engine = MarketStateEngine::new();
            let mut sampler = ContextSampler::new(None);
            let mut labeler = ForwardLabeler::new(Labeling::CURRENT);
            let mut labels = Vec::new();
            label_contexts(
                &mut feed(&tape),
                &mut engine,
                &mut sampler,
                &mut labeler,
                |_| {},
                |_| {},
                |label| labels.push(label),
            )
            .unwrap();
            (labeler, labels)
        };
        // Past the last trade (a late start), inverted (the engine's
        // InvalidGap), and sorting before the last trade: each would skip
        // the engine's checks. The late start alone would close the
        // windows ending before it as fully covered, where the end of the
        // data censors them at the last trade.
        for gap in [
            trades_gap(
                Stream::Trades,
                last_trade + 30 * MINUTE,
                last_trade + 2 * HOUR,
            ),
            trades_gap(Stream::Trades, last_trade + 2 * HOUR, last_trade + HOUR),
            trades_gap(Stream::Trades, last_trade - HOUR, last_trade - 30 * MINUTE),
        ] {
            let (mut labeler, mut labels) = drive();
            let before = labeler.clone();
            let error = finish_labels(&mut labeler, &[gap], |label| labels.push(label))
                .expect_err("a broken trailing gap");
            assert!(matches!(error, ProviderError::Contract(_)), "{error}");
            // The labeler is untouched: ending it without the gap gives the
            // end-of-data labels.
            assert_eq!(labeler, before);
            finish_labels(&mut labeler, &[], |label| labels.push(label)).unwrap();
            assert_eq!(labels, plain.labels, "{gap:?}");
        }
        // One opened earlier than the last trade only censors more: let
        // through, like the D6 one opened at it.
        let early = trades_gap(Stream::Trades, last_trade - MINUTE, last_trade + HOUR);
        let censored = label_whole(&tape, &[early]);
        assert_eq!(censored.labels.len(), plain.labels.len());
        // Every trades entry is checked before the labeler sees one.
        let (mut labeler, _) = drive();
        let before = labeler.clone();
        let valid = trades_gap(Stream::Trades, last_trade, last_trade + HOUR);
        let late = trades_gap(Stream::Trades, last_trade + 2 * HOUR, last_trade + 3 * HOUR);
        assert!(finish_labels(&mut labeler, &[valid, late], |_| {}).is_err());
        assert_eq!(labeler, before);
    }

    #[test]
    fn label_window_names_the_data_the_engine_and_both_policies() {
        let dataset = DatasetVersion::from_hex(&"cd".repeat(32)).unwrap();
        let history = History(walk(3), dataset.clone());
        let window = ReplayWindow {
            start: EventTime::from_millis(0),
            end: EventTime::from_millis(4 * HOUR),
        };
        let (mut records, mut labels) = (Vec::new(), Vec::new());
        let (provenance, report) = label_window(
            &history,
            window,
            window,
            Labeling::V2,
            |_| Vec::new(),
            |record| records.push(record),
            |label| labels.push(label),
        )
        .unwrap();
        assert_eq!(provenance.context.data, DataVersion::from(&dataset));
        assert_eq!(
            provenance.context.engine,
            EngineKey {
                features: catalog::current_set().version(),
                state_encoding: STATE_HASH_ENCODING,
            }
        );
        assert_eq!(provenance.context.policy, SAMPLING_V1);
        assert_eq!(provenance.context.policy_fingerprint, policy_fingerprint());
        assert_eq!(provenance.labeling, LABELING_V2);
        assert_eq!(
            provenance.labeling_fingerprint,
            labeling_fingerprint(Labeling::V2)
        );
        assert_eq!(report.events, history.0.len() as u64);
        assert_eq!(report.rejections, 0);
        let count = records.len() as u64;
        assert_eq!(report.sampling.records, count);
        assert_eq!(
            (report.labels.opened, report.labels.emitted),
            (count, count)
        );
        // The same records and labels as one drive over the stream.
        let whole = label_whole(&history.0, &[]);
        assert_eq!(records, whole.records);
        assert_eq!(labels, whole.labels);
    }

    #[test]
    fn label_window_started_passes_the_provenance_before_the_first_record() {
        let dataset = DatasetVersion::from_hex(&"ab".repeat(32)).unwrap();
        let history = History(walk(3), dataset);
        let window = ReplayWindow {
            start: EventTime::from_millis(0),
            end: EventTime::from_millis(4 * HOUR),
        };
        // What reached the sinks, in order: the provenance, then a record
        // or a label per entry.
        let log = std::cell::RefCell::new(Vec::new());
        let (provenance, report) = label_window_started(
            &history,
            window,
            window,
            Labeling::V2,
            |_| Vec::new(),
            |provenance| log.borrow_mut().push(provenance.to_string()),
            |_| log.borrow_mut().push("record".to_owned()),
            |_| log.borrow_mut().push("label".to_owned()),
        )
        .unwrap();
        let log = log.into_inner();
        assert_eq!(log[0], provenance.to_string());
        assert_eq!(log.iter().filter(|e| e.starts_with("data=")).count(), 1);
        assert!(report.sampling.records > 0);
        assert_eq!(
            log.iter().filter(|e| *e == "record").count() as u64,
            report.sampling.records
        );

        // The same run as `label_window`'s.
        let (mut records, mut labels) = (0_u64, 0_u64);
        let (plain, plain_report) = label_window(
            &history,
            window,
            window,
            Labeling::V2,
            |_| Vec::new(),
            |_| records += 1,
            |_| labels += 1,
        )
        .unwrap();
        assert_eq!((plain, plain_report), (provenance, report));
        assert_eq!(records, report.sampling.records);
        assert_eq!(labels, report.labels.emitted);

        // No provenance when the replay cannot be opened.
        let mut started = false;
        let refused = label_window_started(
            &Refusing,
            window,
            window,
            Labeling::V2,
            |_| Vec::new(),
            |_| started = true,
            |_| {},
            |_| {},
        );
        assert!(refused.is_err());
        assert!(!started);
    }

    #[test]
    fn label_window_labels_the_records_window_only() {
        let dataset = DatasetVersion::from_hex(&"ef".repeat(32)).unwrap();
        let history = History(walk(3), dataset);
        let window = ReplayWindow {
            start: EventTime::from_millis(0),
            end: EventTime::from_millis(4 * HOUR),
        };
        let records = ReplayWindow {
            start: EventTime::from_millis(HOUR),
            end: EventTime::from_millis(2 * HOUR),
        };
        let run = |window: ReplayWindow, records: ReplayWindow| {
            let (mut taken, mut labels) = (Vec::new(), Vec::new());
            let (_, report) = label_window(
                &history,
                window,
                records,
                Labeling::V2,
                |_| Vec::new(),
                |record| taken.push(record),
                |label| labels.push(label),
            )
            .unwrap();
            (taken, labels, report)
        };
        let (all, all_labels, _) = run(window, window);
        let (taken, labels, report) = run(window, records);

        // Exactly the records of the window, as the full run took them,
        // each with one label.
        let expected: Vec<ContextRecord> = all
            .iter()
            .filter(|record| records.contains(record.event.time))
            .cloned()
            .collect();
        assert!(!expected.is_empty() && expected.len() < all.len());
        assert_eq!(taken, expected);
        assert_eq!(report.sampling.records, taken.len() as u64);
        assert_eq!(report.labels.opened, taken.len() as u64);
        assert_eq!(labels.len(), taken.len());

        // Their labels are the full run's but for the clear flags, which
        // start over at the window's first record (ADR-055 D9).
        let unflagged = |label: &ForwardLabel| {
            let mut label = label.clone();
            for horizon in &mut label.horizons {
                horizon.clear = false;
            }
            label
        };
        let expected: Vec<ForwardLabel> = all_labels
            .iter()
            .filter(|label| records.contains(label.event.time))
            .map(unflagged)
            .collect();
        assert_eq!(labels.iter().map(unflagged).collect::<Vec<_>>(), expected);
        assert!(matches!(taken[0].sample, Sample::Baseline { .. }));
        assert!(labels[0].horizons.iter().all(|horizon| horizon.clear));

        // The events after the records window feed the labels: the last
        // record's 15m window ends before the data does and is complete. A
        // replay that ends with the records window censors it at its last
        // trade.
        let last = labels.last().unwrap();
        assert_eq!(last.horizons[1].censored, None, "{last}");
        let (_, short, _) = run(
            ReplayWindow {
                start: window.start,
                end: records.end,
            },
            records,
        );
        let cut = short.last().unwrap();
        assert_eq!((cut.event, cut.ordinal), (last.event, last.ordinal));
        assert!(
            cut.horizons[1]
                .censored
                .is_some_and(|censor| censor.reason == CensorReason::EndOfData),
            "{cut}"
        );
    }

    #[test]
    fn labels_never_leak_into_records_through_the_driver() {
        // The #26 acceptance property, through the driver one event at a
        // time: mutating only events after a decision event changes labels,
        // never the records up to it or their clear flags, and leaves every
        // label emitted before the first mutated event as it was. A day of
        // data first, so labels have been emitted before the decision.
        let tape = walk(26);
        let base = label_per_event(&tape, &[]);
        assert_eq!(base.report.rejections, 0);
        let (index, at) = trade_from(&tape, 25 * HOUR);
        let decision = tape[index].canonical_key();
        assert!(base.records.iter().any(|record| record.event == decision));
        let time = at.time.as_millis();
        let shifted: Vec<MarketEvent> = tape
            .iter()
            .enumerate()
            .map(|(position, event)| match event {
                MarketEvent::Trade(trade) if position > index => MarketEvent::Trade(Trade {
                    price: Price::from_units(trade.price.units() + 1_000 * SCALE),
                    ..*trade
                }),
                other => other.clone(),
            })
            .collect();
        let inserted = |event: MarketEvent| {
            let mut tape = tape.clone();
            let position = tape.partition_point(|other| *other < event);
            assert!(position > index);
            tape.insert(position, event);
            tape
        };
        let variants: Vec<(&str, Vec<MarketEvent>)> = vec![
            ("truncated", tape[..=index].to_vec()),
            ("shifted", shifted),
            (
                "gap after",
                inserted(MarketEvent::FeedGap(trades_gap(
                    Stream::Trades,
                    time + 1,
                    time + 10 * MINUTE,
                ))),
            ),
            (
                "same ms",
                inserted(priced(
                    time,
                    at.trade_id + 1,
                    at.price.units() + 1_000 * SCALE,
                )),
            ),
        ];
        let decided = |key: CanonicalKey| key <= decision;
        let early: Vec<&ForwardLabel> = base
            .labels
            .iter()
            .zip(&base.emitted_at)
            .filter(|(_, emitted)| **emitted <= index)
            .map(|(label, _)| label)
            .collect();
        assert!(!early.is_empty());
        for (name, variant) in variants {
            let mutated = label_per_event(&variant, &[]);
            assert_eq!(mutated.report.rejections, 0, "{name}");
            let records = |run: &Labeled| -> Vec<ContextRecord> {
                run.records
                    .iter()
                    .filter(|record| decided(record.event))
                    .cloned()
                    .collect()
            };
            let before = records(&base);
            assert!(before.len() > 1_000, "{name} {}", before.len());
            assert_eq!(records(&mutated), before, "{name}");
            let labels = |run: &Labeled| -> Vec<ForwardLabel> {
                run.labels
                    .iter()
                    .filter(|label| decided(label.event))
                    .cloned()
                    .collect()
            };
            let (old, new) = (labels(&base), labels(&mutated));
            assert_eq!(old.len(), before.len(), "{name}");
            assert_eq!(new.len(), old.len(), "{name}");
            let flags = |labels: &[ForwardLabel]| -> Vec<[bool; 5]> {
                labels
                    .iter()
                    .map(|label| label.horizons.map(|horizon| horizon.clear))
                    .collect()
            };
            assert_eq!(flags(&new), flags(&old), "{name}");
            assert_ne!(new, old, "{name}: no label changed");
            for (position, label) in early.iter().enumerate() {
                assert_eq!(&mutated.labels[position], *label, "{name}");
            }
        }
    }
}
