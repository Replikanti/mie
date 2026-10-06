//! Raw records → canonical domain events: normalization, per-stream
//! continuity and the canonical merge, composed.
//!
//! [`Pipeline`] has no clock and does no I/O. Its output is a pure function
//! of its parameters and of the records it is given, in the order given:
//!
//! - the parameters of [`Pipeline::new`]: the symbol, `hold_back_ms` and
//!   the per-stream seeds. They are **not** in the raw store: live capture
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
    /// Delivered gaps on this stream, by reason.
    pub gaps: BTreeMap<GapReason, u64>,
    /// Largest distance in ms between the merge watermark and an event's
    /// ordering time when it arrived. Sizes the hold-back.
    pub max_lateness_ms: i64,
}

/// Counters of every stream that saw a record or a gap.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PipelineStats {
    /// Per-stream counters.
    pub streams: BTreeMap<BinanceStream, StreamStats>,
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
    /// on an id-less stream a `MissingData` gap stands in for it.
    pub error: Option<NormalizeError>,
}

/// The deterministic core of live capture and replay.
#[derive(Debug, Clone)]
pub struct Pipeline {
    symbol: String,
    sequencers: BTreeMap<BinanceStream, StreamSequencer>,
    holdback: HoldBack,
    stats: PipelineStats,
}

impl Pipeline {
    /// A pipeline for `symbol` with a hold-back of `hold_back_ms` exchange
    /// milliseconds, seeded with the previous run's last persisted event time
    /// per stream.
    pub fn new(
        symbol: &str,
        hold_back_ms: i64,
        seeds: &BTreeMap<BinanceStream, EventTime>,
    ) -> Self {
        let sequencers = BinanceStream::ALL
            .into_iter()
            .map(|stream| {
                let seed = seeds
                    .get(&stream)
                    .map(|&last_event_time| Seed { last_event_time });
                (stream, StreamSequencer::new(stream.domain_stream(), seed))
            })
            .collect();
        Self {
            symbol: symbol.to_owned(),
            sequencers,
            holdback: HoldBack::new(hold_back_ms),
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
            if let Some(watermark) = self.holdback.watermark() {
                let lateness = watermark
                    .as_millis()
                    .saturating_sub(event.time().as_millis());
                let stats = self.stream_stats(stream);
                stats.max_lateness_ms = stats.max_lateness_ms.max(lateness);
            }
            let (admission, released) = self.holdback.push(event);
            if admission == Admission::Duplicate {
                self.stream_stats(stream).duplicates += 1;
            }
            self.count(&released);
            events.extend(released);
        }
        Pushed { events, error }
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
            let Some(stream) = BinanceStream::of_domain(event.stream()) else {
                continue;
            };
            let stats = self.stream_stats(stream);
            match event {
                MarketEvent::FeedGap(gap) => *stats.gaps.entry(gap.reason).or_default() += 1,
                _ => stats.events += 1,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let mut pipe = Pipeline::new("BTCUSDT", 0, &BTreeMap::new());
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
        let mut pipe = Pipeline::new("BTCUSDT", 0, &BTreeMap::new());
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
        let mut pipe = Pipeline::new("BTCUSDT", 0, &seeds);
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
}
