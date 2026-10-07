//! Replay of the archive backfill from the raw store (#11, ADR-038 D5).
//!
//! Archive rows carry exchange fields only, and every row is filed at its
//! ADR-028 ordering time (ADR-034 D3), so an archive replay is a streaming
//! canonical merge of the normalized rows — there is no capture run to
//! recompute:
//!
//! - **Selection.** Each requested stream is selected over its window
//!   widened to whole UTC days; the date partitions present tell which days
//!   the store holds. Only events inside the requested window are
//!   delivered. One dataset version per distinct window identifies the
//!   files.
//! - **Merge.** Files are opened in `(min_event_time, path)` order, each
//!   once its smallest event time is at or below the smallest head of the
//!   files already open, so about one file per stream is in memory. A file
//!   is read (hash-verified), normalized with the code import validation
//!   uses ([`super::normalize`]), restricted to the window and sorted; the
//!   open files are k-way merged in the canonical order (ADR-028). A row
//!   whose event's ordering time differs from its record's `event_time`
//!   breaks the contract; a row that does not normalize fails the replay.
//! - **Continuity.** Per raw stream, the live [`StreamSequencer`] drops
//!   repeats (overlapping files, exact duplicate rows) and turns a trade-id
//!   jump into a `SequenceBreak` gap; one zero hold-back
//!   [`HoldBack`] then places same-millisecond gaps in the canonical order.
//! - **Missing partitions.** A UTC day of a requested window without a
//!   sealed file of the stream is a `MissingData` gap from the stream's
//!   previous event (or the window start) to its first event after the
//!   missing days, delivered right before that event in place of any
//!   sequence-break gap. Missing days with no later event in the window are
//!   reported in [`trailing_gaps`](ArchiveReplayStream::trailing_gaps)
//!   only, as a live gap still open at the window's end would be.

use super::ARCHIVE_SOURCE;
use super::catalog::{ArchiveStream, DAY_MS, parse_day};
use super::normalize::parse;
use crate::holdback::HoldBack;
use crate::sequence::StreamSequencer;
use mie_domain::event::{FeedGap, GapReason, MarketEvent};
use mie_domain::time::EventTime;
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use mie_ports::raw::{DatasetVersion, RawRecordSource, RawSelection, RawStreamKey, SealedFile};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::fmt;

/// [`HistoricalDataProvider`] over the archive source of one instrument.
pub struct ArchiveReplay<'a> {
    source: &'a dyn RawRecordSource,
    symbol: String,
    streams: Vec<ArchiveStream>,
}

impl fmt::Debug for ArchiveReplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArchiveReplay")
            .field("symbol", &self.symbol)
            .field("streams", &self.streams)
            .finish_non_exhaustive()
    }
}

impl<'a> ArchiveReplay<'a> {
    /// A replay of `streams` of `symbol` from the archive source.
    pub fn new(source: &'a dyn RawRecordSource, symbol: &str, streams: &[ArchiveStream]) -> Self {
        Self {
            source,
            symbol: symbol.to_owned(),
            streams: streams.to_vec(),
        }
    }

    /// A replay of the `configured` streams that replay by default
    /// ([`ArchiveStream::replay_default`]): everything but `trades` and
    /// `bookDepth`.
    pub fn with_defaults(
        source: &'a dyn RawRecordSource,
        symbol: &str,
        configured: &[ArchiveStream],
    ) -> Self {
        let streams: Vec<_> = configured
            .iter()
            .copied()
            .filter(|s| s.replay_default())
            .collect();
        Self::new(source, symbol, &streams)
    }

    /// The replayed streams.
    pub fn streams(&self) -> &[ArchiveStream] {
        &self.streams
    }
}

impl<'a> HistoricalDataProvider for ArchiveReplay<'a> {
    type Stream = ArchiveReplayStream<'a>;

    fn replay(&self, window: ReplayWindow) -> Result<Replay<Self::Stream>, ProviderError> {
        let requests: Vec<_> = self.streams.iter().map(|&s| (s, window)).collect();
        let stream = open_requests(self.source, &self.symbol, &requests)?;
        let dataset = stream.versions[0].1.clone();
        Ok(Replay { stream, dataset })
    }
}

/// What the replay read of one stream.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveStreamStats {
    /// Files opened.
    pub files: u64,
    /// Rows inside the window, normalized.
    pub events: u64,
    /// UTC days (since 1970-01-01) of the widened window without a sealed
    /// file.
    pub missing_days: Vec<i64>,
}

/// A sealed file to merge, with the request it serves.
#[derive(Debug, Clone)]
struct Planned {
    file: SealedFile,
    stream: ArchiveStream,
    window: ReplayWindow,
}

/// One opened file: its remaining events in canonical order.
#[derive(Debug)]
struct Cursor {
    stream: ArchiveStream,
    events: std::vec::IntoIter<MarketEvent>,
}

fn source_error(detail: impl fmt::Display) -> ProviderError {
    ProviderError::Source(detail.to_string())
}

/// The whole UTC days `[first, end)` a window touches.
fn day_span(window: ReplayWindow) -> (i64, i64) {
    let first = window.start.as_millis().div_euclid(DAY_MS);
    let end = (window.end.as_millis() - 1).div_euclid(DAY_MS) + 1;
    (first, end)
}

/// Opens a replay of several streams, each over its own window (the kline
/// cross-check reads trades a minute beyond the klines' window).
///
/// # Errors
///
/// [`ProviderError::Contract`] for no request, an empty window, a stream
/// requested twice, `bookDepth` (no domain event) or `aggTrades` with
/// `trades` (one trade-id space); [`ProviderError::Source`] when the store
/// cannot be listed.
pub fn open_requests<'a>(
    source: &'a dyn RawRecordSource,
    symbol: &str,
    requests: &[(ArchiveStream, ReplayWindow)],
) -> Result<ArchiveReplayStream<'a>, ProviderError> {
    let contract = |detail: String| Err(ProviderError::Contract(detail));
    if requests.is_empty() {
        return contract("an archive replay needs at least one stream".to_owned());
    }
    let mut seen = BTreeSet::new();
    for &(stream, window) in requests {
        if window.start >= window.end {
            return contract(format!(
                "empty replay window [{}, {}) for {stream}",
                window.start, window.end
            ));
        }
        if stream.domain_stream().is_none() {
            return contract(format!("{stream} has no domain event to replay"));
        }
        if !seen.insert(stream) {
            return contract(format!("{stream} requested twice"));
        }
    }
    if seen.contains(&ArchiveStream::AggTrades) && seen.contains(&ArchiveStream::Trades) {
        return contract("aggTrades and trades share one trade-id space; replay one".to_owned());
    }

    let mut groups: Vec<(ReplayWindow, Vec<ArchiveStream>)> = Vec::new();
    for &(stream, window) in requests {
        match groups.iter_mut().find(|(w, _)| *w == window) {
            Some((_, streams)) => streams.push(stream),
            None => groups.push((window, vec![stream])),
        }
    }
    let mut versions = Vec::new();
    let mut planned = Vec::new();
    let mut stats: BTreeMap<ArchiveStream, ArchiveStreamStats> = BTreeMap::new();
    let mut missing: BTreeMap<ArchiveStream, VecDeque<i64>> = BTreeMap::new();
    let mut windows = BTreeMap::new();
    for (window, streams) in groups {
        let (first_day, end_day) = day_span(window);
        let keys = streams
            .iter()
            .map(|s| RawStreamKey::new(ARCHIVE_SOURCE, symbol, s.raw_name()))
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(source_error)?;
        let widened = ReplayWindow {
            start: EventTime::from_millis(first_day * DAY_MS),
            end: EventTime::from_millis(end_day * DAY_MS),
        };
        let selection = RawSelection::new(keys, widened).map_err(source_error)?;
        let dataset = source.select(&selection).map_err(source_error)?;
        let mut present: BTreeMap<ArchiveStream, BTreeSet<i64>> = BTreeMap::new();
        for file in dataset.files {
            let stream = ArchiveStream::from_raw_name(file.stream.stream())
                .ok_or_else(|| source_error(format!("unknown archive stream {}", file.stream)))?;
            if let Some(day) = parse_day(&file.date) {
                present.entry(stream).or_default().insert(day);
            }
            let overlaps = file.min_event_time < window.end && file.max_event_time >= window.start;
            if overlaps {
                planned.push(Planned {
                    file,
                    stream,
                    window,
                });
            }
        }
        for &stream in &streams {
            let days: Vec<i64> = (first_day..end_day)
                .filter(|day| !present.get(&stream).is_some_and(|p| p.contains(day)))
                .collect();
            missing.insert(stream, days.iter().copied().collect());
            stats.entry(stream).or_default().missing_days = days;
            windows.insert(stream, window);
        }
        versions.push((streams, dataset.version));
    }
    planned.sort_by(|a, b| {
        (a.file.min_event_time, &a.file.relative_path)
            .cmp(&(b.file.min_event_time, &b.file.relative_path))
    });
    let sequencers = windows
        .keys()
        .map(|&s| {
            let domain = s.domain_stream().expect("checked above");
            (s, StreamSequencer::new(domain, None))
        })
        .collect();
    Ok(ArchiveReplayStream {
        source,
        symbol: symbol.to_owned(),
        versions,
        planned,
        next_file: 0,
        cursors: Vec::new(),
        heap: BinaryHeap::new(),
        sequencers,
        missing,
        windows,
        holdback: HoldBack::new(0, 0),
        out: VecDeque::new(),
        finished: false,
        failed: None,
        stats,
        trailing: Vec::new(),
    })
}

/// The event stream of an archive replay: a lazy k-way merge over the
/// selected files (module docs).
pub struct ArchiveReplayStream<'a> {
    source: &'a dyn RawRecordSource,
    symbol: String,
    versions: Vec<(Vec<ArchiveStream>, DatasetVersion)>,
    planned: Vec<Planned>,
    next_file: usize,
    cursors: Vec<Cursor>,
    heap: BinaryHeap<Reverse<(MarketEvent, usize)>>,
    sequencers: BTreeMap<ArchiveStream, StreamSequencer>,
    missing: BTreeMap<ArchiveStream, VecDeque<i64>>,
    windows: BTreeMap<ArchiveStream, ReplayWindow>,
    holdback: HoldBack,
    out: VecDeque<MarketEvent>,
    finished: bool,
    failed: Option<ProviderError>,
    stats: BTreeMap<ArchiveStream, ArchiveStreamStats>,
    trailing: Vec<FeedGap>,
}

impl fmt::Debug for ArchiveReplayStream<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArchiveReplayStream")
            .field("symbol", &self.symbol)
            .field("files", &self.planned.len())
            .field("opened", &self.next_file)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl ArchiveReplayStream<'_> {
    /// The dataset version of each distinct request window, with its
    /// streams, in request order.
    pub fn dataset_versions(&self) -> &[(Vec<ArchiveStream>, DatasetVersion)] {
        &self.versions
    }

    /// Per-stream counters so far.
    pub fn stream_stats(&self) -> &BTreeMap<ArchiveStream, ArchiveStreamStats> {
        &self.stats
    }

    /// Missing days after a stream's last event in the window: not
    /// delivered (module docs). Complete once the stream has ended.
    pub fn trailing_gaps(&self) -> &[FeedGap] {
        &self.trailing
    }

    /// Opens the next planned file and adds its first event to the merge.
    fn open_next(&mut self) -> Result<(), ProviderError> {
        let Planned {
            file,
            stream,
            window,
        } = self.planned[self.next_file].clone();
        self.next_file += 1;
        let mut events = Vec::new();
        for record in self.source.read(&file).map_err(source_error)? {
            if !window.contains(record.event_time) {
                continue;
            }
            let parsed = parse(stream, &self.symbol, &record.payload).map_err(|e| {
                source_error(format!(
                    "{} at {}: {e}",
                    file.relative_path, record.event_time
                ))
            })?;
            let Some(event) = parsed else {
                continue;
            };
            if event.time() != record.event_time {
                return Err(ProviderError::Contract(format!(
                    "{}: a row ordered at {} is filed at {}",
                    file.relative_path,
                    event.time(),
                    record.event_time
                )));
            }
            events.push(event);
        }
        events.sort_unstable();
        let stats = self.stats.entry(stream).or_default();
        stats.files += 1;
        stats.events += events.len() as u64;
        let mut events = events.into_iter();
        if let Some(first) = events.next() {
            self.heap.push(Reverse((first, self.cursors.len())));
        }
        self.cursors.push(Cursor { stream, events });
        Ok(())
    }

    /// The next event of the canonical merge and its stream.
    fn next_merged(&mut self) -> Result<Option<(MarketEvent, ArchiveStream)>, ProviderError> {
        loop {
            let head = self.heap.peek().map(|Reverse((event, _))| event.time());
            let due = self
                .planned
                .get(self.next_file)
                .is_some_and(|p| head.is_none_or(|time| p.file.min_event_time <= time));
            if due {
                self.open_next()?;
                continue;
            }
            let Some(Reverse((event, index))) = self.heap.pop() else {
                return Ok(None);
            };
            let cursor = &mut self.cursors[index];
            match cursor.events.next() {
                Some(next) => self.heap.push(Reverse((next, index))),
                // Frees the file's buffer.
                None => cursor.events = Vec::new().into_iter(),
            }
            return Ok(Some((event, cursor.stream)));
        }
    }

    /// Sequences one merged event and queues what is due.
    fn admit(&mut self, event: MarketEvent, stream: ArchiveStream) {
        let sequencer = self
            .sequencers
            .get_mut(&stream)
            .expect("every merged stream has a sequencer");
        let previous = sequencer.last_time();
        let end = event.time();
        let mut sequenced = sequencer.push("", event);
        if sequenced.is_empty() {
            return;
        }
        let day = end.as_millis().div_euclid(DAY_MS);
        let missing = self.missing.entry(stream).or_default();
        let mut skipped = false;
        while missing.front().is_some_and(|&d| d < day) {
            missing.pop_front();
            skipped = true;
        }
        if skipped {
            let start = previous.unwrap_or(self.windows[&stream].start);
            sequenced.retain(|e| !matches!(e, MarketEvent::FeedGap(_)));
            sequenced.insert(
                0,
                MarketEvent::FeedGap(FeedGap {
                    stream: sequenced[0].stream(),
                    start: start.min(end),
                    end,
                    reason: GapReason::MissingData,
                }),
            );
        }
        for event in sequenced {
            let (_, released) = self.holdback.push(event);
            self.out.extend(released);
        }
    }

    /// Releases the hold-back and records the trailing missing days.
    fn finish(&mut self) {
        self.out.extend(self.holdback.finish());
        for (&stream, days) in &self.missing {
            let Some(&last) = days.back() else {
                continue;
            };
            let window = self.windows[&stream];
            let start = self.sequencers[&stream].last_time().unwrap_or(window.start);
            let end = EventTime::from_millis(((last + 1) * DAY_MS).min(window.end.as_millis()));
            self.trailing.push(FeedGap {
                stream: stream.domain_stream().expect("checked at open"),
                start: start.min(end),
                end,
                reason: GapReason::MissingData,
            });
        }
        self.finished = true;
    }
}

impl MarketDataProvider for ArchiveReplayStream<'_> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        loop {
            if let Some(event) = self.out.pop_front() {
                return Ok(Some(event));
            }
            if let Some(failed) = &self.failed {
                return Err(failed.clone());
            }
            if self.finished {
                return Ok(None);
            }
            match self.next_merged() {
                Ok(Some((event, stream))) => self.admit(event, stream),
                Ok(None) => self.finish(),
                Err(failed) => {
                    self.failed = Some(failed.clone());
                    return Err(failed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::normalize::record_time;
    use crate::testing::MemorySource;
    use mie_domain::event::Stream;
    use mie_domain::state::MarketStateEngine;
    use mie_ports::raw::RawRecord;

    fn day(label: &str) -> i64 {
        parse_day(label).unwrap()
    }

    fn record(stream: ArchiveStream, row: &str) -> RawRecord {
        RawRecord {
            event_time: record_time(stream, row.as_bytes()).unwrap(),
            capture: None,
            payload: row.as_bytes().to_vec(),
        }
    }

    fn agg_row(id: u64, time: i64) -> String {
        format!("{id},85000.10,0.010,{id},{id},{time},false")
    }

    /// Aggregate trades every `step` ms over one UTC day, ids from `first`.
    fn agg_day(source: &mut MemorySource, d: i64, first: u64, step: i64) -> u64 {
        let mut id = first;
        let mut records = Vec::new();
        let mut t = d * DAY_MS + 7;
        while t < (d + 1) * DAY_MS {
            records.push(record(ArchiveStream::AggTrades, &agg_row(id, t)));
            id += 1;
            t += step;
        }
        source.seal(ARCHIVE_SOURCE, "aggTrades", records);
        id
    }

    fn metrics_row(civil: &str, oi: &str) -> String {
        format!("{civil},BTCUSDT,{oi},7734131259.6348000000000000,1.45,1.93,1.38,0.41")
    }

    fn window(start: i64, end: i64) -> ReplayWindow {
        ReplayWindow {
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(end),
        }
    }

    fn drain(stream: &mut ArchiveReplayStream<'_>) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        while let Some(event) = stream.next_event().unwrap() {
            out.push(event);
        }
        out
    }

    fn gaps(events: &[MarketEvent]) -> Vec<(Stream, i64, i64, GapReason)> {
        events
            .iter()
            .filter_map(|e| match e {
                MarketEvent::FeedGap(g) => {
                    Some((g.stream, g.start.as_millis(), g.end.as_millis(), g.reason))
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn unsorted_rows_and_overlapping_files_merge_strictly_increasing() {
        let d = day("2026-09-30");
        let mut source = MemorySource::default();
        // Metrics rows are published unsorted; a second, overlapping file
        // repeats two of them.
        let rows = [
            metrics_row("2026-09-30 10:00:00", "93000.0000000000000000"),
            metrics_row("2026-09-30 02:30:00", "92849.1660000000000000"),
            metrics_row("2026-09-30 07:10:00", "93320.1460000000000000"),
        ];
        let first: Vec<_> = rows
            .iter()
            .map(|r| record(ArchiveStream::Metrics, r))
            .collect();
        source.seal(ARCHIVE_SOURCE, "metrics", first.clone());
        source.seal(ARCHIVE_SOURCE, "metrics", first[1..].to_vec());
        agg_day(&mut source, d, 1, 600_000);

        let replay = ArchiveReplay::new(
            &source,
            "BTCUSDT",
            &[ArchiveStream::AggTrades, ArchiveStream::Metrics],
        );
        let opened = replay.replay(window(d * DAY_MS, (d + 1) * DAY_MS)).unwrap();
        let mut stream = opened.stream;
        let out = drain(&mut stream);
        assert!(out.windows(2).all(|w| w[0] < w[1]));
        let oi: Vec<i64> = out
            .iter()
            .filter_map(|e| match e {
                MarketEvent::OpenInterest(oi) => Some(oi.time.as_millis()),
                _ => None,
            })
            .collect();
        // Delivered at the end of their five-minute interval (ADR-034).
        let at = |h: i64, m: i64| d * DAY_MS + (h * 60 + m) * 60_000 + 300_000;
        assert_eq!(oi, [at(2, 30), at(7, 10), at(10, 0)]);
        assert_eq!(out.len(), 3 + 144);
        assert!(gaps(&out).is_empty());
        assert_eq!(stream.stream_stats()[&ArchiveStream::Metrics].files, 2);
        assert_eq!(stream.dataset_versions().len(), 1);
        assert_eq!(stream.dataset_versions()[0].1, opened.dataset);
        let mut engine = MarketStateEngine::new();
        for event in &out {
            engine.apply(event).unwrap();
        }
    }

    #[test]
    fn a_missing_day_is_one_missing_data_gap_before_the_resumed_event() {
        let (d0, d1, d2, d3) = (
            day("2026-09-28"),
            day("2026-09-29"),
            day("2026-09-30"),
            day("2026-10-01"),
        );
        let mut source = MemorySource::default();
        let next = agg_day(&mut source, d0, 1, 3_600_000);
        // Day d1 was never imported; ids jump across it.
        agg_day(&mut source, d2, next + 5_000, 3_600_000);
        let replay = ArchiveReplay::new(&source, "BTCUSDT", &[ArchiveStream::AggTrades]);

        let mut stream = replay
            .replay(window(d0 * DAY_MS, d3 * DAY_MS))
            .unwrap()
            .stream;
        let out = drain(&mut stream);
        let last_d0 = d0 * DAY_MS + 7 + 23 * 3_600_000;
        let first_d2 = d2 * DAY_MS + 7;
        assert_eq!(
            gaps(&out),
            [(Stream::Trades, last_d0, first_d2, GapReason::MissingData)]
        );
        let at = out
            .iter()
            .position(|e| matches!(e, MarketEvent::FeedGap(_)))
            .unwrap();
        assert_eq!(out[at + 1].time().as_millis(), first_d2);
        assert_eq!(out.len(), 48 + 1);
        assert!(stream.trailing_gaps().is_empty());
        assert_eq!(
            stream.stream_stats()[&ArchiveStream::AggTrades].missing_days,
            [d1]
        );

        // A trailing missing day yields no delivered gap, only a trailing one.
        let mut stream = replay
            .replay(window(d2 * DAY_MS, (d3 + 1) * DAY_MS))
            .unwrap()
            .stream;
        let out = drain(&mut stream);
        assert!(gaps(&out).is_empty());
        assert_eq!(out.len(), 24);
        let trailing = stream.trailing_gaps();
        assert_eq!(trailing.len(), 1);
        assert_eq!(
            (
                trailing[0].start.as_millis(),
                trailing[0].end.as_millis(),
                trailing[0].reason
            ),
            (
                d2 * DAY_MS + 7 + 23 * 3_600_000,
                (d3 + 1) * DAY_MS,
                GapReason::MissingData
            )
        );

        // A missing first day: the gap runs from the window start.
        let mut stream = replay
            .replay(window(d1 * DAY_MS + 5, d3 * DAY_MS))
            .unwrap()
            .stream;
        let out = drain(&mut stream);
        assert_eq!(
            gaps(&out),
            [(
                Stream::Trades,
                d1 * DAY_MS + 5,
                first_d2,
                GapReason::MissingData
            )]
        );
    }

    #[test]
    fn a_trade_id_jump_is_a_sequence_break() {
        let d = day("2026-09-30");
        let mut source = MemorySource::default();
        let rows = [
            agg_row(10, d * DAY_MS + 1),
            agg_row(11, d * DAY_MS + 2),
            agg_row(15, d * DAY_MS + 3),
        ];
        source.seal(
            ARCHIVE_SOURCE,
            "aggTrades",
            rows.iter()
                .map(|r| record(ArchiveStream::AggTrades, r))
                .collect(),
        );
        let replay = ArchiveReplay::new(&source, "BTCUSDT", &[ArchiveStream::AggTrades]);
        let out = drain(
            &mut replay
                .replay(window(d * DAY_MS, (d + 1) * DAY_MS))
                .unwrap()
                .stream,
        );
        assert_eq!(
            gaps(&out),
            [(
                Stream::Trades,
                d * DAY_MS + 2,
                d * DAY_MS + 3,
                GapReason::SequenceBreak
            )]
        );
        assert_eq!(out.len(), 4);
    }

    #[test]
    fn misfiled_and_unparsable_rows_fail_the_replay() {
        let d = day("2026-09-30");
        let w = window(d * DAY_MS, (d + 1) * DAY_MS);
        let mut misfiled = MemorySource::default();
        let mut row = record(ArchiveStream::AggTrades, &agg_row(1, d * DAY_MS + 10));
        row.event_time = EventTime::from_millis(d * DAY_MS + 11);
        misfiled.seal(ARCHIVE_SOURCE, "aggTrades", vec![row]);
        let replay = ArchiveReplay::new(&misfiled, "BTCUSDT", &[ArchiveStream::AggTrades]);
        let mut stream = replay.replay(w).unwrap().stream;
        let err = stream.next_event().unwrap_err();
        assert!(
            matches!(&err, ProviderError::Contract(d) if d.contains("is filed at")),
            "{err}"
        );
        assert_eq!(stream.next_event(), Err(err));

        let mut broken = MemorySource::default();
        let mut row = record(ArchiveStream::AggTrades, &agg_row(1, d * DAY_MS + 10));
        row.payload = b"1,85000.10,0.0000000001,1,1,0,false".to_vec();
        broken.seal(ARCHIVE_SOURCE, "aggTrades", vec![row]);
        let replay = ArchiveReplay::new(&broken, "BTCUSDT", &[ArchiveStream::AggTrades]);
        let err = replay.replay(w).unwrap().stream.next_event().unwrap_err();
        assert!(
            matches!(&err, ProviderError::Source(d) if d.contains("aggTrades/date=")),
            "{err}"
        );
    }

    #[test]
    fn requests_without_one_trade_id_space_or_a_domain_event_are_refused() {
        let source = MemorySource::default();
        let w = window(0, DAY_MS);
        for requests in [
            vec![],
            vec![(ArchiveStream::BookDepth, w)],
            vec![(ArchiveStream::AggTrades, w), (ArchiveStream::Trades, w)],
            vec![(ArchiveStream::Metrics, w), (ArchiveStream::Metrics, w)],
            vec![(ArchiveStream::Metrics, window(5, 5))],
        ] {
            assert!(
                matches!(
                    open_requests(&source, "BTCUSDT", &requests),
                    Err(ProviderError::Contract(_))
                ),
                "{requests:?}"
            );
        }
        let defaults = ArchiveReplay::with_defaults(&source, "BTCUSDT", &ArchiveStream::ALL);
        assert!(!defaults.streams().contains(&ArchiveStream::Trades));
        assert!(!defaults.streams().contains(&ArchiveStream::BookDepth));
        assert_eq!(defaults.streams().len(), 9);
        // Two windows: one dataset version each.
        let two = open_requests(
            &source,
            "BTCUSDT",
            &[
                (ArchiveStream::Trades, window(0, 2 * DAY_MS)),
                (ArchiveStream::Metrics, w),
            ],
        )
        .unwrap();
        assert_eq!(two.dataset_versions().len(), 2);
    }
}
