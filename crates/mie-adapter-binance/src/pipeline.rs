//! Raw records → canonical domain events: normalization, per-stream
//! continuity and the canonical merge, composed.
//!
//! [`Pipeline`] has no clock and does no I/O. Its output is a pure function
//! of the records it is given, in the order given: the payload, the stream
//! and the capture session of each record. Live capture feeds it records in
//! processing order right after persisting them, so replay (#11) and the
//! equivalence harness (#13) recompute exactly what live delivered — feed
//! gaps and late-event gaps included — from the raw store, ordered by
//! `receive_seq`.

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
    /// # Errors
    ///
    /// The [`NormalizeError`] of a record that does not normalize. It is
    /// counted, and the pipeline stays usable: the next record continues
    /// normally.
    pub fn push(
        &mut self,
        stream: BinanceStream,
        record: &RawRecord,
    ) -> Result<Vec<MarketEvent>, NormalizeError> {
        self.stream_stats(stream).records += 1;
        let parsed = normalize::parse(stream, &self.symbol, &record.payload);
        let event = match parsed {
            Ok(Some(event)) => event,
            Ok(None) => return Ok(Vec::new()),
            Err(error) => {
                self.stream_stats(stream).normalize_errors += 1;
                return Err(error);
            }
        };
        let session = record
            .capture
            .as_ref()
            .map_or("", |capture| capture.session_id.as_str());
        let sequenced = self
            .sequencers
            .get_mut(&stream)
            .map(|seq| seq.push(session, event))
            .unwrap_or_default();
        let mut out = Vec::new();
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
            out.extend(released);
        }
        Ok(out)
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
        assert!(
            pipe.push(BinanceStream::AggTrade, &record("s", 1, "{oops"))
                .is_err()
        );
        pipe.push(BinanceStream::AggTrade, &record("s", 2, &agg(1, 1_000)))
            .unwrap();
        let out = pipe
            .push(BinanceStream::AggTrade, &record("s", 3, &agg(2, 1_001)))
            .unwrap();
        assert_eq!(out.len(), 1);
        let stats = pipe.stats();
        let agg_stats = &stats.streams[&BinanceStream::AggTrade];
        assert_eq!((agg_stats.records, agg_stats.normalize_errors), (3, 1));
        assert_eq!(agg_stats.events, 1);
        assert_eq!(pipe.buffered(), 1);
        assert_eq!(pipe.finish().len(), 1);
        assert_eq!(pipe.stats().streams[&BinanceStream::AggTrade].events, 2);
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
                .unwrap(),
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
