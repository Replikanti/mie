//! Raw records → canonical domain events: normalization, per-stream
//! continuity, order-book sync and the canonical merge, composed.
//!
//! The `depth` and `depthSnapshot` records go through one
//! [`BookSequencer`] (ADR-038); every other stream through its own
//! [`StreamSequencer`]. If the hold-back turns a book event into a
//! `LateEvent` gap, the book desyncs (`SequenceBreak`) and the rest of that
//! record's book events are dropped: the next resync announces the period.
//!
//! [`Pipeline`] has no clock and does no I/O. Its output is a pure function
//! of its parameters and of the records it is given, in the order given:
//!
//! - the parameters of [`Pipeline::new`]: the symbol, `hold_back_ms`,
//!   `oi_retime_ms` (ADR-032 D12) and the per-stream seeds (the book is
//!   seeded with the later of the `depth` and `depthSnapshot` seeds). They
//!   are **not** in the raw store: live capture
//!   journals them in the run's `run_start` line, and a recompute must take
//!   them from there, never from configuration defaults;
//! - per record: the stream, the payload, the raw `event_time` and the
//!   capture `session_id`.
//!
//! Live capture feeds it records in processing order right after persisting
//! them. So replay (#11) and the equivalence harness (#13) recompute exactly
//! what live delivered — feed gaps and late-event gaps included — from the
//! raw store, ordered by `receive_seq` within a run, plus that run's
//! `run_start` parameters.

use crate::book_sync::{BookSequencer, BookStats, BookTransition};
use crate::holdback::{Admission, HoldBack};
use crate::normalize::{self, NormalizeError};
use crate::sequence::{Seed, StreamSequencer};
use crate::stream::BinanceStream;
use mie_domain::event::{GapReason, MarketEvent};
use mie_domain::time::EventTime;
use mie_ports::raw::RawRecord;
use std::collections::BTreeMap;

/// Counters of one stream.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamStats {
    /// Raw records pushed.
    pub records: u64,
    /// Domain events delivered, gaps excluded.
    pub events: u64,
    /// Records that failed normalization.
    pub normalize_errors: u64,
    /// Events dropped as repeats (exchange id or exact payload).
    pub duplicates: u64,
    /// Trades dropped because their id fell below the last delivered one.
    pub regressions: u64,
    /// Late samples delivered re-timed instead of as a gap (open interest,
    /// ADR-032 D12).
    pub retimed: u64,
    /// Delivered gaps on this stream, by reason.
    pub gaps: BTreeMap<GapReason, u64>,
    /// Largest distance in ms between the merge watermark and an event's
    /// ordering time when it arrived. Sizes the hold-back.
    pub max_lateness_ms: i64,
}

/// Counters of every stream that saw a record or a gap.
///
/// Book events count on `depth` (updates and gaps) and on `depthSnapshot`
/// (snapshots).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PipelineStats {
    /// Per-stream counters.
    pub streams: BTreeMap<BinanceStream, StreamStats>,
    /// Order-book sync counters (ADR-038).
    pub book: BookStats,
}

impl PipelineStats {
    /// Gaps delivered with `reason`, over all streams.
    pub fn gaps(&self, reason: GapReason) -> u64 {
        self.streams
            .values()
            .map(|s| s.gaps.get(&reason).copied().unwrap_or(0))
            .sum()
    }
}

/// What one [`Pipeline::push`] produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pushed {
    /// The events now due, in canonical order.
    pub events: Vec<MarketEvent>,
    /// Why the record did not normalize, if it did not. It is counted, and
    /// on an id-less stream a `MissingData` gap stands in for it; a book
    /// record desyncs the book instead (ADR-038).
    pub error: Option<NormalizeError>,
    /// Order-book sync-state changes the record caused, in order.
    pub book: Vec<BookTransition>,
}

/// The deterministic core of live capture and replay.
#[derive(Debug, Clone)]
pub struct Pipeline {
    symbol: String,
    sequencers: BTreeMap<BinanceStream, StreamSequencer>,
    book: BookSequencer,
    holdback: HoldBack,
    stats: PipelineStats,
}

impl Pipeline {
    /// A pipeline for `symbol` with a hold-back of `hold_back_ms` exchange
    /// milliseconds, re-timing open interest that is late by at most
    /// `oi_retime_ms` (ADR-032 D12), seeded with the previous run's last
    /// persisted event time per stream.
    pub fn new(
        symbol: &str,
        hold_back_ms: i64,
        oi_retime_ms: i64,
        seeds: &BTreeMap<BinanceStream, EventTime>,
    ) -> Self {
        let sequencers = BinanceStream::ALL
            .into_iter()
            .filter(|stream| !is_book(*stream))
            .map(|stream| {
                let seed = seeds
                    .get(&stream)
                    .map(|&last_event_time| Seed { last_event_time });
                (stream, StreamSequencer::new(stream.domain_stream(), seed))
            })
            .collect();
        let book_seed = [BinanceStream::Depth, BinanceStream::DepthSnapshot]
            .iter()
            .filter_map(|stream| seeds.get(stream).copied())
            .max();
        Self {
            symbol: symbol.to_owned(),
            sequencers,
            book: BookSequencer::new(book_seed),
            holdback: HoldBack::new(hold_back_ms, oi_retime_ms),
            stats: PipelineStats::default(),
        }
    }

    /// Takes the next record of `stream` and returns the events now due, in
    /// canonical order. The session is the record's capture `session_id`
    /// (empty for archive records).
    ///
    /// A record that does not normalize is counted and reported in
    /// [`Pushed::error`]; the pipeline stays usable. On an id-less stream
    /// (everything but trades) the lost sample becomes a `MissingData` gap
    /// ending at the record's raw `event_time`, so the loss is visible in
    /// band. On trades the next trade-id break reports it.
    pub fn push(&mut self, stream: BinanceStream, record: &RawRecord) -> Pushed {
        self.stream_stats(stream).records += 1;
        let session = record
            .capture
            .as_ref()
            .map_or("", |capture| capture.session_id.as_str());
        if is_book(stream) {
            return self.push_book(stream, session, record);
        }
        let Some(seq) = self.sequencers.get_mut(&stream) else {
            return Pushed::default();
        };
        let (sequenced, error) = match normalize::parse(stream, &self.symbol, &record.payload) {
            Ok(Some(event)) => (seq.push(session, event), None),
            Ok(None) => (Vec::new(), None),
            Err(error) => {
                let gap =
                    (stream != BinanceStream::AggTrade).then(|| seq.missing(record.event_time));
                self.stream_stats(stream).normalize_errors += 1;
                (gap.into_iter().collect(), Some(error))
            }
        };
        let mut events = Vec::new();
        for event in sequenced {
            self.note_lateness(stream, &event);
            let (admission, released) = self.holdback.push(event);
            match admission {
                Admission::Duplicate => self.stream_stats(stream).duplicates += 1,
                Admission::Retimed => self.stream_stats(stream).retimed += 1,
                Admission::Buffered | Admission::Late => {}
            }
            self.count(&released);
            events.extend(released);
        }
        Pushed {
            events,
            error,
            book: Vec::new(),
        }
    }

    /// A `depth` or `depthSnapshot` record through the book sequencer.
    fn push_book(&mut self, stream: BinanceStream, session: &str, record: &RawRecord) -> Pushed {
        let parsed = normalize::parse(stream, &self.symbol, &record.payload);
        let error = parsed.as_ref().err().cloned();
        if error.is_some() {
            self.stream_stats(stream).normalize_errors += 1;
        }
        let floor = self.holdback.last_released();
        let output = match stream {
            BinanceStream::DepthSnapshot => {
                let snapshot = parsed.and_then(|event| match event {
                    Some(MarketEvent::BookSnapshot(snapshot)) => Ok(snapshot),
                    other => Err(not_book(other.as_ref())),
                });
                self.book.push_snapshot(snapshot, floor)
            }
            _ => {
                let diff = parsed.and_then(|event| match event {
                    Some(MarketEvent::BookUpdate(update)) => Ok(update),
                    other => Err(not_book(other.as_ref())),
                });
                self.book.push_diff(session, diff, floor)
            }
        };
        let mut transitions = output.transitions;
        let mut events = Vec::new();
        for event in output.events {
            self.note_lateness(stream, &event);
            let (admission, released) = self.holdback.push(event);
            self.count(&released);
            events.extend(released);
            match admission {
                Admission::Late => {
                    // The book missed a slot: resync instead of delivering
                    // the rest of this record's book events.
                    transitions.extend(self.book.desync(GapReason::SequenceBreak));
                    break;
                }
                Admission::Duplicate => self.stream_stats(stream).duplicates += 1,
                Admission::Buffered | Admission::Retimed => {}
            }
        }
        Pushed {
            events,
            error,
            book: transitions,
        }
    }

    /// `Some(want id)` while the order book needs a snapshot; each id is
    /// requested once (ADR-038).
    pub fn book_snapshot_wanted(&self) -> Option<u64> {
        self.book.snapshot_wanted()
    }

    fn note_lateness(&mut self, stream: BinanceStream, event: &MarketEvent) {
        if let Some(watermark) = self.holdback.watermark() {
            let lateness = watermark
                .as_millis()
                .saturating_sub(event.time().as_millis());
            let stats = self.stream_stats(stream);
            stats.max_lateness_ms = stats.max_lateness_ms.max(lateness);
        }
    }

    /// Releases every buffered event, in canonical order.
    pub fn finish(&mut self) -> Vec<MarketEvent> {
        let out = self.holdback.finish();
        self.count(&out);
        out
    }

    /// The counters so far.
    pub fn stats(&self) -> PipelineStats {
        let mut stats = self.stats.clone();
        stats.book = self.book.stats().clone();
        for (&stream, seq) in &self.sequencers {
            if seq.duplicates() > 0 || seq.regressions() > 0 {
                let s = stats.streams.entry(stream).or_default();
                s.duplicates += seq.duplicates();
                s.regressions += seq.regressions();
            }
        }
        stats
    }

    /// Number of events waiting in the hold-back.
    pub fn buffered(&self) -> usize {
        self.holdback.len()
    }

    fn stream_stats(&mut self, stream: BinanceStream) -> &mut StreamStats {
        self.stats.streams.entry(stream).or_default()
    }

    fn count(&mut self, released: &[MarketEvent]) {
        for event in released {
            let stream = match event {
                MarketEvent::BookSnapshot(_) => BinanceStream::DepthSnapshot,
                _ => match BinanceStream::of_domain(event.stream()) {
                    Some(stream) => stream,
                    None => continue,
                },
            };
            let stats = self.stream_stats(stream);
            match event {
                MarketEvent::FeedGap(gap) => *stats.gaps.entry(gap.reason).or_default() += 1,
                _ => stats.events += 1,
            }
        }
    }
}

/// Whether `stream` feeds the order book.
fn is_book(stream: BinanceStream) -> bool {
    matches!(stream, BinanceStream::Depth | BinanceStream::DepthSnapshot)
}

/// A book stream normalized to something else: impossible by construction
/// of [`normalize::parse`], reported as an error all the same.
fn not_book(event: Option<&MarketEvent>) -> NormalizeError {
    NormalizeError::Unexpected(format!("not an order-book event: {event:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book_sync::BookTransition;
    use mie_domain::event::Stream;
    use mie_ports::raw::Capture;

    fn record(session: &str, seq: u64, payload: &str) -> RawRecord {
        RawRecord {
            event_time: EventTime::from_millis(0),
            capture: Some(Capture {
                receive_time_ns: 0,
                receive_seq: seq,
                session_id: session.to_owned(),
            }),
            payload: payload.as_bytes().to_vec(),
        }
    }

    fn agg(id: u64, time: i64) -> String {
        format!(
            r#"{{"e":"aggTrade","E":{time},"a":{id},"s":"BTCUSDT","p":"85000.10","q":"0.010","f":1,"l":1,"T":{time},"m":false}}"#
        )
    }

    #[test]
    fn normalize_errors_are_counted_and_do_not_stop_the_stream() {
        let mut pipe = Pipeline::new("BTCUSDT", 0, 0, &BTreeMap::new());
        let bad = pipe.push(BinanceStream::AggTrade, &record("s", 1, "{oops"));
        assert!(bad.error.is_some());
        // Trades get no stand-in gap: the next id break reports a loss.
        assert!(bad.events.is_empty());
        pipe.push(BinanceStream::AggTrade, &record("s", 2, &agg(1, 1_000)));
        let out = pipe
            .push(BinanceStream::AggTrade, &record("s", 3, &agg(2, 1_001)))
            .events;
        assert_eq!(out.len(), 1);
        let stats = pipe.stats();
        let agg_stats = &stats.streams[&BinanceStream::AggTrade];
        assert_eq!((agg_stats.records, agg_stats.normalize_errors), (3, 1));
        assert_eq!(agg_stats.events, 1);
        assert_eq!(pipe.buffered(), 1);
        assert_eq!(pipe.finish().len(), 1);
        assert_eq!(pipe.stats().streams[&BinanceStream::AggTrade].events, 2);
    }

    fn mark(time: i64) -> String {
        format!(
            r#"{{"e":"markPriceUpdate","E":{time},"s":"BTCUSDT","p":"85001.00000000","i":"85002.50000000","r":"0.00010000","T":1791273600000}}"#
        )
    }

    fn at(mut record: RawRecord, millis: i64) -> RawRecord {
        record.event_time = EventTime::from_millis(millis);
        record
    }

    #[test]
    fn an_unparsable_idless_sample_becomes_an_in_band_missing_data_gap() {
        let mut pipe = Pipeline::new("BTCUSDT", 0, 0, &BTreeMap::new());
        let mut out = Vec::new();
        out.extend(
            pipe.push(
                BinanceStream::MarkPrice,
                &at(record("s", 0, &mark(1_000)), 1_000),
            )
            .events,
        );
        // A decimal sent as a number: the 2000 ms sample is lost. The raw
        // record still carries its ordering time.
        let broken = mark(2_000).replace(r#""p":"85001.00000000""#, r#""p":85001.0"#);
        let pushed = pipe.push(
            BinanceStream::MarkPrice,
            &at(record("s", 1, &broken), 2_000),
        );
        assert!(matches!(pushed.error, Some(NormalizeError::Json(_))));
        out.extend(pushed.events);
        out.extend(
            pipe.push(
                BinanceStream::MarkPrice,
                &at(record("s", 2, &mark(3_000)), 3_000),
            )
            .events,
        );
        out.extend(pipe.finish());
        let gaps: Vec<_> = out
            .iter()
            .filter_map(|e| match e {
                MarketEvent::FeedGap(g) => {
                    Some((g.stream, g.start.as_millis(), g.end.as_millis(), g.reason))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            gaps,
            [(Stream::MarkPrice, 1_000, 2_000, GapReason::MissingData)]
        );
        assert_eq!(out.len(), 3);
        let mut engine = mie_domain::state::MarketStateEngine::new();
        for event in &out {
            engine.apply(event).unwrap();
        }
        let stats = pipe.stats();
        let mark_stats = &stats.streams[&BinanceStream::MarkPrice];
        assert_eq!(mark_stats.normalize_errors, 1);
        assert_eq!(mark_stats.gaps[&GapReason::MissingData], 1);
        assert_eq!(mark_stats.events, 2);
    }

    #[test]
    fn seeds_and_sessions_become_counted_gaps() {
        let seeds = BTreeMap::from([(BinanceStream::AggTrade, EventTime::from_millis(500))]);
        let mut pipe = Pipeline::new("BTCUSDT", 0, 0, &seeds);
        let mut out = Vec::new();
        for (i, (session, id, time)) in [("a", 1, 1_000), ("a", 2, 1_100), ("b", 3, 5_000)]
            .into_iter()
            .enumerate()
        {
            out.extend(
                pipe.push(
                    BinanceStream::AggTrade,
                    &record(session, i as u64, &agg(id, time)),
                )
                .events,
            );
        }
        out.extend(pipe.finish());
        let gaps: Vec<_> = out
            .iter()
            .filter_map(|e| match e {
                MarketEvent::FeedGap(g) => Some((g.stream, g.start.as_millis(), g.end.as_millis())),
                _ => None,
            })
            .collect();
        assert_eq!(
            gaps,
            [(Stream::Trades, 500, 1_000), (Stream::Trades, 1_100, 5_000)]
        );
        assert_eq!(pipe.stats().gaps(GapReason::Disconnected), 2);
    }

    fn depth(first: u64, last: u64, prev: u64, time: i64) -> String {
        format!(
            r#"{{"e":"depthUpdate","E":{},"T":{time},"s":"BTCUSDT","ps":"BTCUSDT","U":{first},"u":{last},"pu":{prev},"b":[["85000.10","{last}.000"]],"a":[]}}"#,
            time + 4
        )
    }

    fn depth_snapshot(last: u64, time: i64) -> String {
        format!(
            r#"{{"lastUpdateId":{last},"E":{},"T":{time},"bids":[["85000.10","1.000"]],"asks":[["85000.20","2.000"]]}}"#,
            time + 3
        )
    }

    /// Depth sync, a late diff and its resync, with mark prices moving the
    /// watermark; hold-back 0.
    fn late_book_records() -> Vec<(BinanceStream, RawRecord)> {
        let depth_rec = |seq, payload: String| (BinanceStream::Depth, record("d", seq, &payload));
        vec![
            depth_rec(1, depth(1, 10, 0, 1_000)),
            (
                BinanceStream::DepthSnapshot,
                record("r/1", 2, &depth_snapshot(5, 990)),
            ),
            (BinanceStream::MarkPrice, record("m", 3, &mark(2_000))),
            (BinanceStream::MarkPrice, record("m", 4, &mark(3_000))),
            // Chains, but its slot (1500) was released with the 2000 mark.
            depth_rec(5, depth(11, 20, 10, 1_500)),
            depth_rec(6, depth(21, 30, 20, 3_100)),
            (
                BinanceStream::DepthSnapshot,
                record("r/2", 7, &depth_snapshot(25, 3_050)),
            ),
            depth_rec(8, depth(31, 40, 30, 3_200)),
        ]
    }

    fn run_book(
        records: &[(BinanceStream, RawRecord)],
    ) -> (Vec<MarketEvent>, Vec<BookTransition>, Pipeline) {
        let mut pipe = Pipeline::new("BTCUSDT", 0, 0, &BTreeMap::new());
        let (mut events, mut transitions) = (Vec::new(), Vec::new());
        for (stream, record) in records {
            let pushed = pipe.push(*stream, record);
            assert_eq!(pushed.error, None);
            events.extend(pushed.events);
            transitions.extend(pushed.book);
        }
        events.extend(pipe.finish());
        (events, transitions, pipe)
    }

    #[test]
    fn a_late_book_update_desyncs_and_resyncs_once() {
        let records = late_book_records();
        let (out, transitions, pipe) = run_book(&records);
        let gaps: Vec<_> = out
            .iter()
            .filter_map(|e| match e {
                MarketEvent::FeedGap(g) => {
                    Some((g.stream, g.start.as_millis(), g.end.as_millis(), g.reason))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            gaps,
            [
                (Stream::OrderBook, 1_500, 2_001, GapReason::LateEvent),
                (Stream::OrderBook, 1_500, 3_050, GapReason::SequenceBreak),
            ]
        );
        assert_eq!(
            transitions,
            [
                BookTransition::Desynced(GapReason::Disconnected),
                BookTransition::Synced {
                    last_update_id: 5,
                    time: EventTime::from_millis(990)
                },
                BookTransition::Desynced(GapReason::SequenceBreak),
                BookTransition::Synced {
                    last_update_id: 25,
                    time: EventTime::from_millis(3_050)
                },
            ]
        );
        let mut engine = mie_domain::state::MarketStateEngine::new();
        let mut book = mie_domain::book::OrderBook::new();
        for (i, event) in out.iter().enumerate() {
            engine
                .apply(event)
                .unwrap_or_else(|e| panic!("event {i} rejected: {e}"));
            book.apply(event);
        }
        assert!(book.is_valid());
        assert_eq!(book.last_update_id(), Some(40));
        let stats = pipe.stats();
        assert_eq!(stats.book.syncs, 2);
        assert_eq!(stats.book.desyncs[&GapReason::SequenceBreak], 1);
        let depth_stats = &stats.streams[&BinanceStream::Depth];
        assert_eq!(depth_stats.records, 4);
        // Updates 10, 30, 40; the late one became a gap.
        assert_eq!(depth_stats.events, 3);
        assert_eq!(depth_stats.gaps[&GapReason::LateEvent], 1);
        assert_eq!(depth_stats.gaps[&GapReason::SequenceBreak], 1);
        assert_eq!(stats.streams[&BinanceStream::DepthSnapshot].events, 2);
        assert_eq!(pipe.book_snapshot_wanted(), None);

        // The same records give the same output.
        assert_eq!(run_book(&records).0, out);
    }

    #[test]
    fn a_book_normalize_error_is_counted_and_desyncs() {
        let mut pipe = Pipeline::new("BTCUSDT", 0, 0, &BTreeMap::new());
        pipe.push(
            BinanceStream::Depth,
            &record("d", 1, &depth(1, 10, 0, 1_000)),
        );
        let pushed = pipe.push(
            BinanceStream::Depth,
            &record("d", 2, r#"{"e":"depthUpdate"}"#),
        );
        assert!(pushed.error.is_some());
        assert!(pushed.events.is_empty());
        assert_eq!(pipe.book_snapshot_wanted(), None);
        pipe.push(
            BinanceStream::Depth,
            &record("d", 3, &depth(11, 20, 10, 1_100)),
        );
        assert!(pipe.book_snapshot_wanted().is_some());
        let stats = pipe.stats();
        assert_eq!(stats.streams[&BinanceStream::Depth].normalize_errors, 1);
        assert_eq!(stats.book.desyncs[&GapReason::Disconnected], 1);
    }

    #[test]
    fn a_reconnect_inside_an_unsynced_period_keeps_the_first_cause() {
        let mut pipe = Pipeline::new("BTCUSDT", 0, 0, &BTreeMap::new());
        let mut events = Vec::new();
        let mut transitions = Vec::new();
        let records = [
            (
                BinanceStream::Depth,
                record("d/1", 1, &depth(1, 10, 0, 1_000)),
            ),
            (
                BinanceStream::DepthSnapshot,
                record("r/1", 2, &depth_snapshot(5, 990)),
            ),
            (
                BinanceStream::Depth,
                record("d/1", 3, &depth(11, 20, 10, 1_100)),
            ),
            // 21..=30 missing: a pu break opens the unsynced period.
            (
                BinanceStream::Depth,
                record("d/1", 4, &depth(31, 40, 30, 1_300)),
            ),
            // The socket reconnects before the resync.
            (
                BinanceStream::Depth,
                record("d/2", 5, &depth(51, 60, 50, 1_500)),
            ),
            (
                BinanceStream::DepthSnapshot,
                record("r/2", 6, &depth_snapshot(55, 1_450)),
            ),
        ];
        for (stream, record) in &records {
            let pushed = pipe.push(*stream, record);
            events.extend(pushed.events);
            transitions.extend(pushed.book);
        }
        events.extend(pipe.finish());
        // One unsynced period, one gap, named by its first cause; it spans
        // the old session's last record (1300).
        let desyncs: Vec<_> = transitions
            .iter()
            .filter(|t| matches!(t, BookTransition::Desynced(_)))
            .collect();
        assert_eq!(
            desyncs,
            [
                &BookTransition::Desynced(GapReason::Disconnected),
                &BookTransition::Desynced(GapReason::SequenceBreak)
            ]
        );
        let gaps: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                MarketEvent::FeedGap(g) => {
                    Some((g.stream, g.start.as_millis(), g.end.as_millis(), g.reason))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            gaps,
            [(Stream::OrderBook, 1_100, 1_450, GapReason::SequenceBreak)]
        );
        let mut engine = mie_domain::state::MarketStateEngine::new();
        for event in &events {
            engine.apply(event).unwrap();
        }
        assert_eq!(
            pipe.stats().book.desyncs.get(&GapReason::Disconnected),
            Some(&1)
        );
    }
}
