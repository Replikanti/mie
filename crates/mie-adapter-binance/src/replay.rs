//! Replay of live capture runs from the raw store (#11, ADR-039 D1–D4).
//!
//! What live delivered is a function of each run's raw records in
//! `receive_seq` order plus the run's journaled pipeline parameters
//! (ADR-032 D4, [`crate::pipeline`]). So [`LiveReplay`] does not re-merge
//! stored records canonically; it **recomputes** every capture run that
//! touches the window with the same [`Pipeline`] live used, from the run's
//! first record, and restricts the output to the window afterwards. Only
//! that reproduces what live knew at each point: late events become the same
//! `LateEvent` gaps, open interest is re-timed as live delivered it
//! (ADR-032 D12), and the seeds reproduce the restart gaps.
//!
//! - **Run selection** (D2). A run's extent is
//!   `[started_at − RUN_MARGIN_MS, end + RUN_MARGIN_MS)`, where `end` is the
//!   run's end, else the next run's start (a crash), else open. The replay
//!   recomputes the runs whose extent overlaps the window, one at a time and
//!   in start order, each from the files of its streams that overlap its
//!   extent. An open run is read only up to the window's end plus the
//!   margin, which bounds what can still affect the window's output. One
//!   selection over the runs' streams and the union of their extents with
//!   the window gives the reported dataset version, so the version of a
//!   closed window stays the same while a later run goes on capturing, once
//!   that run has sealed past the window's end plus the margin.
//! - **Integrity** (D2). A run's records are those whose `session_id`
//!   starts with `<run_id>/`, sorted by `receive_seq`, which must count up
//!   from 0 without repeats. A run that ended cleanly must be complete: a
//!   hole, or a count other than its `run_end` record count, is an error. A
//!   crashed run is recomputed over its contiguous prefix; the records after
//!   its first hole are counted as ignored. Every capture record in the
//!   window must belong to a recomputed run: in-window records of a run
//!   whose `run_start` was lost — they cannot be recomputed without its
//!   parameters — fail the replay instead of vanishing from it. Such records
//!   read outside the window cannot affect it; they are counted
//!   ([`unattributed_outside`](LiveReplayStream::unattributed_outside)).
//! - **Run chaining** (D3). Each run's output continues after the last
//!   event delivered before it through a zero hold-back
//!   [`HoldBack`](crate::holdback::HoldBack): in-order events pass
//!   unchanged, and an event of a fast restart that is not above the last
//!   delivered one becomes a `LateEvent` gap or re-timed open interest,
//!   never an engine `OutOfOrder`.
//! - **Window** (D4). An event is delivered when its ordering time lies in
//!   the window; a gap counts at its `end`. A gap whose `end` is at or after
//!   the window's end is not delivered — live announced it only when the
//!   stream resumed — and is listed in
//!   [`trailing_gaps`](LiveReplayStream::trailing_gaps) instead.
//!
//! Memory: one run's records and output at a time (about 1.5 M records for
//! a 24 h run).

use crate::holdback::HoldBack;
use crate::pipeline::{Pipeline, PipelineStats};
use crate::stream::BinanceStream;
use mie_domain::event::{FeedGap, MarketEvent};
use mie_domain::time::EventTime;
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use mie_ports::raw::{RawRecord, RawRecordSource, RawSelection, RawStreamKey, SealedFile};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

/// How far a run's records may lie outside its wall-clock span: open
/// klines are filed at their close time (up to a minute ahead) and open
/// interest trails its poll by seconds.
pub const RUN_MARGIN_MS: i64 = 300_000;

/// One live capture run as its journal describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveRun {
    /// The run id, the prefix of its records' session ids.
    pub run_id: String,
    /// The exchange symbol the pipeline normalized for.
    pub symbol: String,
    /// The run's hold-back in exchange milliseconds.
    pub hold_back_ms: i64,
    /// The run's open-interest re-time allowance (ADR-032 D12).
    pub oi_retime_ms: i64,
    /// The run's restart seeds.
    pub seeds: BTreeMap<BinanceStream, EventTime>,
    /// The streams the run captured.
    pub streams: Vec<BinanceStream>,
    /// Wall-clock start, UTC ms.
    pub started_at_ms: i64,
    /// Wall-clock end, UTC ms; `None` when the run has no end (crash or
    /// still running).
    pub ended_at_ms: Option<i64>,
    /// The number of records a cleanly ended run appended; `None` for a run
    /// that crashed or ended with an error.
    pub clean_records: Option<u64>,
}

/// What the replay did with one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStats {
    /// The run.
    pub run_id: String,
    /// Records of the run found in the selection.
    pub records: u64,
    /// Records recomputed: the contiguous `receive_seq` prefix.
    pub replayed: u64,
    /// Records after the prefix of a crashed run, not recomputed.
    pub ignored: u64,
    /// Whether the run ended cleanly (and was therefore checked complete).
    pub clean: bool,
    /// The recompute's pipeline counters.
    pub pipeline: PipelineStats,
}

/// [`HistoricalDataProvider`] over the live capture runs of one source and
/// instrument.
pub struct LiveReplay<'a> {
    source: &'a dyn RawRecordSource,
    raw_source: String,
    symbol: String,
    runs: Vec<LiveRun>,
}

impl fmt::Debug for LiveReplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveReplay")
            .field("raw_source", &self.raw_source)
            .field("symbol", &self.symbol)
            .field("runs", &self.runs)
            .finish_non_exhaustive()
    }
}

impl<'a> LiveReplay<'a> {
    /// A replay of `runs` stored under the raw source `raw_source` (for
    /// example `binance-um`) and the instrument `symbol`. Runs of another
    /// symbol are not part of it.
    pub fn new(
        source: &'a dyn RawRecordSource,
        raw_source: &str,
        symbol: &str,
        mut runs: Vec<LiveRun>,
    ) -> Self {
        runs.retain(|run| run.symbol == symbol);
        runs.sort_by(|a, b| (a.started_at_ms, &a.run_id).cmp(&(b.started_at_ms, &b.run_id)));
        Self {
            source,
            raw_source: raw_source.to_owned(),
            symbol: symbol.to_owned(),
            runs,
        }
    }

    /// The runs, in start order.
    pub fn runs(&self) -> &[LiveRun] {
        &self.runs
    }

    /// Each run's extent `[start, end)` in exchange ms (module docs).
    fn extents(&self) -> Vec<(i64, i64)> {
        self.runs
            .iter()
            .enumerate()
            .map(|(i, run)| {
                let end = run
                    .ended_at_ms
                    .or_else(|| self.runs.get(i + 1).map(|next| next.started_at_ms));
                (
                    run.started_at_ms.saturating_sub(RUN_MARGIN_MS),
                    end.map_or(i64::MAX, |end| end.saturating_add(RUN_MARGIN_MS)),
                )
            })
            .collect()
    }
}

fn source_error(detail: impl fmt::Display) -> ProviderError {
    ProviderError::Source(detail.to_string())
}

impl<'a> HistoricalDataProvider for LiveReplay<'a> {
    type Stream = LiveReplayStream<'a>;

    fn replay(&self, window: ReplayWindow) -> Result<Replay<Self::Stream>, ProviderError> {
        if window.start >= window.end {
            return Err(ProviderError::Contract(format!(
                "empty replay window [{}, {})",
                window.start, window.end
            )));
        }
        let selected: Vec<(LiveRun, (i64, i64))> = self
            .runs
            .iter()
            .cloned()
            .zip(self.extents())
            .filter(|(_, (start, end))| {
                *start < window.end.as_millis() && window.start.as_millis() < *end
            })
            .map(|(run, (start, end))| {
                // An open run (no end, no successor) is read only up to the
                // window's end plus the margin: nothing it seals later can
                // change the window's output, so a closed window keeps its
                // dataset version while the run goes on capturing (D2).
                let end = if end == i64::MAX && run.clean_records.is_none() {
                    window.end.as_millis().saturating_add(RUN_MARGIN_MS)
                } else {
                    end
                };
                (run, (start, end))
            })
            .collect();
        let mut streams: BTreeSet<BinanceStream> = selected
            .iter()
            .flat_map(|(run, _)| run.streams.iter().copied())
            .collect();
        if streams.is_empty() {
            streams.extend(BinanceStream::ALL);
        }
        let keys = streams
            .iter()
            .map(|s| RawStreamKey::new(&self.raw_source, &self.symbol, s.raw_name()))
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(source_error)?;
        let start = selected
            .iter()
            .map(|(_, (start, _))| *start)
            .fold(window.start.as_millis(), i64::min);
        let end = selected
            .iter()
            .map(|(_, (_, end))| *end)
            .fold(window.end.as_millis(), i64::max);
        let selection = RawSelection::new(
            keys,
            ReplayWindow {
                start: EventTime::from_millis(start),
                end: EventTime::from_millis(end),
            },
        )
        .map_err(source_error)?;
        let dataset = self.source.select(&selection).map_err(source_error)?;
        let stream = LiveReplayStream {
            source: self.source,
            window,
            files: dataset.files,
            journaled: self.runs.iter().map(|run| run.run_id.clone()).collect(),
            selected: selected.iter().map(|(run, _)| run.run_id.clone()).collect(),
            read_paths: BTreeSet::new(),
            unattributed_checked: false,
            unattributed_outside: BTreeMap::new(),
            pending: selected.into(),
            out: VecDeque::new(),
            last: None,
            newest_oi: None,
            trailing: Vec::new(),
            stats: Vec::new(),
            failed: None,
        };
        Ok(Replay {
            stream,
            dataset: dataset.version,
        })
    }
}

/// The event stream of a [`LiveReplay`]: runs are loaded and recomputed
/// lazily, one at a time.
pub struct LiveReplayStream<'a> {
    source: &'a dyn RawRecordSource,
    window: ReplayWindow,
    files: Vec<SealedFile>,
    /// Every run of the journal.
    journaled: BTreeSet<String>,
    /// The runs this replay recomputes.
    selected: BTreeSet<String>,
    /// Files the runs read.
    read_paths: BTreeSet<String>,
    unattributed_checked: bool,
    /// Records of runs the journal does not know, outside the window.
    unattributed_outside: BTreeMap<String, u64>,
    pending: VecDeque<(LiveRun, (i64, i64))>,
    out: VecDeque<MarketEvent>,
    /// The last event of the chained output, delivered or not.
    last: Option<MarketEvent>,
    /// The newest open-interest time of the chained output.
    newest_oi: Option<EventTime>,
    trailing: Vec<FeedGap>,
    stats: Vec<RunStats>,
    failed: Option<ProviderError>,
}

impl fmt::Debug for LiveReplayStream<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveReplayStream")
            .field("window", &self.window)
            .field("pending_runs", &self.pending.len())
            .field("buffered", &self.out.len())
            .finish_non_exhaustive()
    }
}

impl LiveReplayStream<'_> {
    /// What the replay did with each run loaded so far, in start order.
    pub fn runs(&self) -> &[RunStats] {
        &self.stats
    }

    /// Capture records of runs the journal does not know that the replay
    /// read outside the window, per run id: they cannot affect it, so they
    /// are reported, not replayed (module docs).
    pub fn unattributed_outside(&self) -> &BTreeMap<String, u64> {
        &self.unattributed_outside
    }

    /// Gaps still open at the window's end: not delivered (module docs).
    pub fn trailing_gaps(&self) -> &[FeedGap] {
        &self.trailing
    }

    /// Loads, checks and recomputes one run, then queues its in-window
    /// events.
    fn load(&mut self, run: &LiveRun, extent: (i64, i64)) -> Result<(), ProviderError> {
        let records = self.read_run(run, extent)?;
        let found = records.len();
        let prefix = records
            .iter()
            .enumerate()
            .take_while(|(i, (seq, _, _))| *seq == *i as u64)
            .count();
        if let Some(expected) = run.clean_records {
            if let Some((next, _, _)) = records.get(prefix) {
                return Err(ProviderError::Source(format!(
                    "run {} ended cleanly but receive_seq [{prefix}, {next}) is missing from \
                     the raw store",
                    run.run_id
                )));
            }
            if found as u64 != expected {
                let detail = if (found as u64) < expected {
                    format!("receive_seq [{found}, {expected}) is missing from the raw store")
                } else {
                    format!("the raw store holds {found} records, run_end says {expected}")
                };
                return Err(ProviderError::Source(format!(
                    "run {} ended cleanly but {detail}",
                    run.run_id
                )));
            }
        }

        let mut pipeline =
            Pipeline::new(&run.symbol, run.hold_back_ms, run.oi_retime_ms, &run.seeds);
        let mut events = Vec::new();
        for (_, stream, record) in &records[..prefix] {
            events.extend(pipeline.push(*stream, record).events);
        }
        events.extend(pipeline.finish());
        drop(records);

        let events = match &self.last {
            Some(last) => {
                let mut hold = HoldBack::after(last.clone(), self.newest_oi, run.oi_retime_ms);
                let mut chained = Vec::with_capacity(events.len());
                for event in events {
                    chained.extend(hold.push(event).1);
                }
                chained.extend(hold.finish());
                chained
            }
            None => events,
        };
        if let Some(last) = events.last() {
            self.last = Some(last.clone());
        }
        for event in events {
            if let MarketEvent::OpenInterest(oi) = &event {
                self.newest_oi = Some(self.newest_oi.map_or(oi.time, |n| n.max(oi.time)));
            }
            if let MarketEvent::FeedGap(gap) = &event
                && gap.end >= self.window.end
            {
                if gap.start < self.window.end {
                    self.trailing.push(*gap);
                }
                continue;
            }
            if self.window.contains(event.time()) {
                self.out.push_back(event);
            }
        }
        self.stats.push(RunStats {
            run_id: run.run_id.clone(),
            records: found as u64,
            replayed: prefix as u64,
            ignored: (found - prefix) as u64,
            clean: run.clean_records.is_some(),
            pipeline: pipeline.stats(),
        });
        Ok(())
    }

    /// The run's records from the files of its streams that overlap its
    /// extent, sorted by `receive_seq`, which must not repeat. A record of a
    /// run the journal does not know fails the replay when it lies in the
    /// window; outside the window it is counted (D2).
    fn read_run(
        &mut self,
        run: &LiveRun,
        (start, end): (i64, i64),
    ) -> Result<Vec<(u64, BinanceStream, RawRecord)>, ProviderError> {
        let prefix = format!("{}/", run.run_id);
        let mut records = Vec::new();
        let mut unknown = Unknown::new();
        for file in &self.files {
            let Some(stream) = BinanceStream::from_raw_name(file.stream.stream()) else {
                return Err(ProviderError::Contract(format!(
                    "{}: unknown live stream {}",
                    file.relative_path, file.stream
                )));
            };
            let overlaps =
                file.min_event_time.as_millis() < end && file.max_event_time.as_millis() >= start;
            if !run.streams.contains(&stream) || !overlaps {
                continue;
            }
            // Adjacent runs share files; count each file's records once.
            let first_read = self.read_paths.insert(file.relative_path.clone());
            for record in self.source.read(file).map_err(source_error)? {
                let Some(capture) = &record.capture else {
                    return Err(ProviderError::Contract(format!(
                        "{}: a live record at {} has no capture metadata",
                        file.relative_path, record.event_time
                    )));
                };
                if capture.session_id.starts_with(&prefix) {
                    records.push((capture.receive_seq, stream, record));
                    continue;
                }
                let owner = run_of(&capture.session_id);
                if self.journaled.contains(owner) {
                    continue;
                }
                if self.window.contains(record.event_time) {
                    let (count, paths) = unknown.entry(owner.to_owned()).or_default();
                    *count += 1;
                    paths.insert(file.relative_path.clone());
                } else if first_read {
                    *self
                        .unattributed_outside
                        .entry(owner.to_owned())
                        .or_default() += 1;
                }
            }
        }
        if !unknown.is_empty() {
            return Err(unattributed(&unknown));
        }
        records.sort_by_key(|(seq, _, _)| *seq);
        if let Some(pair) = records.windows(2).find(|pair| pair[0].0 == pair[1].0) {
            return Err(ProviderError::Contract(format!(
                "run {}: receive_seq {} is stored twice",
                run.run_id, pair[0].0
            )));
        }
        Ok(records)
    }

    /// After the last run: the selected files overlapping the window that
    /// no run read must not hold a capture record in the window that no
    /// selected run claims — a run whose `run_start` the journal lost, or a
    /// record outside its run's extent (D2).
    fn check_unread_files(&mut self) -> Result<(), ProviderError> {
        self.unattributed_checked = true;
        let mut unknown = Unknown::new();
        let mut outside = BTreeMap::new();
        for file in &self.files {
            let overlaps =
                file.min_event_time < self.window.end && file.max_event_time >= self.window.start;
            if !overlaps || self.read_paths.contains(&file.relative_path) {
                continue;
            }
            for record in self.source.read(file).map_err(source_error)? {
                let Some(capture) = &record.capture else {
                    return Err(ProviderError::Contract(format!(
                        "{}: a live record at {} has no capture metadata",
                        file.relative_path, record.event_time
                    )));
                };
                let owner = run_of(&capture.session_id);
                let known = self.journaled.contains(owner);
                if !self.window.contains(record.event_time) {
                    if !known {
                        *self
                            .unattributed_outside
                            .entry(owner.to_owned())
                            .or_default() += 1;
                    }
                    continue;
                }
                if !known {
                    let (count, paths) = unknown.entry(owner.to_owned()).or_default();
                    *count += 1;
                    paths.insert(file.relative_path.clone());
                } else if !self.selected.contains(owner) {
                    *outside.entry(owner.to_owned()).or_insert(0_u64) += 1;
                }
            }
        }
        if !unknown.is_empty() {
            return Err(unattributed(&unknown));
        }
        if let Some((run, count)) = outside.first_key_value() {
            return Err(ProviderError::Source(format!(
                "{count} record(s) of run {run} lie in the window, more than \
                 {RUN_MARGIN_MS} ms outside the run's span"
            )));
        }
        Ok(())
    }
}

/// The run id of a capture session: the part before the first `/`.
fn run_of(session_id: &str) -> &str {
    session_id.split('/').next().unwrap_or_default()
}

/// In-window capture records of runs the journal does not know: count and
/// files, per run id.
type Unknown = BTreeMap<String, (u64, BTreeSet<String>)>;

/// The error for in-window capture records of runs the journal does not
/// know.
fn unattributed(unknown: &Unknown) -> ProviderError {
    let runs: Vec<String> = unknown
        .iter()
        .map(|(run, (count, paths))| {
            let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
            format!("{count} of run {run:?} in {}", paths.join(", "))
        })
        .collect();
    ProviderError::Source(format!(
        "the window holds capture records of runs without a run_start in the journal \
         ({}); without their run parameters they cannot be replayed as live delivered them",
        runs.join("; ")
    ))
}

impl MarketDataProvider for LiveReplayStream<'_> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        loop {
            if let Some(event) = self.out.pop_front() {
                return Ok(Some(event));
            }
            if let Some(failed) = &self.failed {
                return Err(failed.clone());
            }
            let Some((run, extent)) = self.pending.pop_front() else {
                if !self.unattributed_checked
                    && let Err(failed) = self.check_unread_files()
                {
                    self.failed = Some(failed.clone());
                    return Err(failed);
                }
                return Ok(None);
            };
            if let Err(failed) = self.load(&run, extent) {
                self.failed = Some(failed.clone());
                return Err(failed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::record_time;
    use crate::testing::MemorySource;
    use mie_domain::event::{GapReason, Stream};
    use mie_domain::state::MarketStateEngine;
    use mie_ports::raw::Capture;

    /// 2026-10-06T01:00:00Z.
    const T0: i64 = 1_791_248_400_000;
    const SOURCE: &str = "binance-um";

    fn agg(id: u64, time: i64) -> String {
        format!(
            r#"{{"e":"aggTrade","E":{},"a":{id},"s":"BTCUSDT","p":"85000.10","q":"0.010","f":1,"l":1,"T":{time},"m":false}}"#,
            time + 50
        )
    }

    fn mark(time: i64) -> String {
        format!(
            r#"{{"e":"markPriceUpdate","E":{time},"s":"BTCUSDT","p":"85001.00000000","i":"85002.50000000","r":"0.00010000","T":1791273600000}}"#
        )
    }

    fn oi(time: i64) -> String {
        format!(r#"{{"symbol":"BTCUSDT","openInterest":"95253.475","time":{time}}}"#)
    }

    /// The records of one capture run, in processing order.
    struct RunBuilder {
        run_id: String,
        records: Vec<(BinanceStream, RawRecord)>,
    }

    impl RunBuilder {
        fn new(run_id: &str) -> Self {
            Self {
                run_id: run_id.to_owned(),
                records: Vec::new(),
            }
        }

        fn push(&mut self, stream: BinanceStream, session: u32, payload: String) -> &mut Self {
            let event_time = record_time(stream, payload.as_bytes()).unwrap();
            let record = RawRecord {
                event_time,
                capture: Some(Capture {
                    receive_time_ns: event_time.as_millis() * 1_000_000,
                    receive_seq: self.records.len() as u64,
                    session_id: format!("{}/{}/{session}", self.run_id, stream.raw_name()),
                }),
                payload: payload.into_bytes(),
            };
            self.records.push((stream, record));
            self
        }

        /// The records of `stream`, without the `skip`ped `receive_seq`s.
        fn of(&self, stream: BinanceStream, skip: &[u64]) -> Vec<RawRecord> {
            self.records
                .iter()
                .filter(|(s, r)| {
                    *s == stream && !skip.contains(&r.capture.as_ref().unwrap().receive_seq)
                })
                .map(|(_, r)| r.clone())
                .collect()
        }

        /// Seals one file per stream.
        fn seal(&self, source: &mut MemorySource, skip: &[u64]) {
            for stream in BinanceStream::ALL {
                let records = self.of(stream, skip);
                if !records.is_empty() {
                    source.seal(SOURCE, stream.raw_name(), records);
                }
            }
        }

        fn run(&self, started: i64, ended: Option<i64>, clean: Option<u64>) -> LiveRun {
            LiveRun {
                run_id: self.run_id.clone(),
                symbol: "BTCUSDT".to_owned(),
                hold_back_ms: 750,
                oi_retime_ms: 10_000,
                seeds: BTreeMap::new(),
                streams: vec![
                    BinanceStream::AggTrade,
                    BinanceStream::MarkPrice,
                    BinanceStream::OpenInterest,
                ],
                started_at_ms: started,
                ended_at_ms: ended,
                clean_records: clean,
            }
        }

        /// What live delivered: the run's pipeline over its records.
        fn live(&self, run: &LiveRun) -> Vec<MarketEvent> {
            let mut pipeline =
                Pipeline::new(&run.symbol, run.hold_back_ms, run.oi_retime_ms, &run.seeds);
            let mut out = Vec::new();
            for (stream, record) in &self.records {
                out.extend(pipeline.push(*stream, record).events);
            }
            out.extend(pipeline.finish());
            out
        }
    }

    fn window(start: i64, end: i64) -> ReplayWindow {
        ReplayWindow {
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(end),
        }
    }

    fn drain(stream: &mut LiveReplayStream<'_>) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        while let Some(event) = stream.next_event().unwrap() {
            out.push(event);
        }
        out
    }

    fn replay(source: &MemorySource, runs: Vec<LiveRun>, w: ReplayWindow) -> Vec<MarketEvent> {
        let live = LiveReplay::new(source, SOURCE, "BTCUSDT", runs);
        drain(&mut live.replay(w).unwrap().stream)
    }

    /// One run over 20 s: trades every 100 ms with an id skip, mark prices
    /// every second, one mark price arriving 2 s late, open interest
    /// trailing its poll by 6 s, and a trade reconnect.
    fn busy_run() -> RunBuilder {
        let mut b = RunBuilder::new("20261006T010000Z");
        let mut id = 1_000;
        for i in 0..200_i64 {
            let t = T0 + 10 + 100 * i;
            if i == 70 {
                id += 3;
            }
            let session = if i < 120 { 1 } else { 2 };
            b.push(BinanceStream::AggTrade, session, agg(id, t));
            id += 1;
            if i % 10 == 0 {
                b.push(BinanceStream::MarkPrice, 1, mark(T0 + 100 * i));
            }
            if i == 95 {
                b.push(BinanceStream::MarkPrice, 1, mark(T0 + 7_250));
            }
            if i % 50 == 49 {
                b.push(BinanceStream::OpenInterest, 1, oi(T0 + 100 * i - 6_000));
            }
        }
        b
    }

    #[test]
    fn a_window_inside_a_run_is_the_full_run_restricted_to_it() {
        let b = busy_run();
        let mut source = MemorySource::default();
        b.seal(&mut source, &[]);
        let run = b.run(T0, Some(T0 + 21_000), Some(b.records.len() as u64));
        let full = replay(
            &source,
            vec![run.clone()],
            window(T0 - 3_600_000, T0 + 3_600_000),
        );
        assert_eq!(full, b.live(&run));
        assert!(full.windows(2).all(|w| w[0] < w[1]));
        let reasons: BTreeSet<GapReason> = full
            .iter()
            .filter_map(|e| match e {
                MarketEvent::FeedGap(g) => Some(g.reason),
                _ => None,
            })
            .collect();
        assert!(reasons.contains(&GapReason::SequenceBreak), "{reasons:?}");
        assert!(reasons.contains(&GapReason::Disconnected), "{reasons:?}");
        assert!(reasons.contains(&GapReason::LateEvent), "{reasons:?}");

        let inner = window(T0 + 5_000, T0 + 12_345);
        let expected: Vec<_> = full
            .iter()
            .filter(|e| inner.contains(e.time()))
            .cloned()
            .collect();
        assert!(expected.len() > 50);
        assert_eq!(replay(&source, vec![run.clone()], inner), expected);

        // Adjacent windows concatenate to their union.
        let mut halves = replay(&source, vec![run.clone()], window(T0, T0 + 9_999));
        halves.extend(replay(
            &source,
            vec![run.clone()],
            window(T0 + 9_999, T0 + 30_000),
        ));
        assert_eq!(halves, replay(&source, vec![run], window(T0, T0 + 30_000)));
    }

    #[test]
    fn a_fast_restart_continues_after_the_last_delivered_event() {
        let mut first = RunBuilder::new("20261006T010000Z");
        for i in 0..100_u64 {
            first.push(BinanceStream::AggTrade, 1, agg(1 + i, T0 + 200 * i as i64));
            if i % 5 == 0 {
                first.push(BinanceStream::MarkPrice, 1, mark(T0 + 200 * i as i64 + 1));
            }
        }
        first.push(BinanceStream::OpenInterest, 1, oi(T0 + 12_000));
        // Run 2 restarts within a second: its first mark price is older than
        // run 1's last delivered event, its open interest is newer than run
        // 1's but older than that event.
        let mut second = RunBuilder::new("20261006T010021Z");
        second.push(BinanceStream::MarkPrice, 1, mark(T0 + 19_000));
        second.push(BinanceStream::OpenInterest, 1, oi(T0 + 19_500));
        for i in 0..20_u64 {
            second.push(
                BinanceStream::AggTrade,
                1,
                agg(101 + i, T0 + 20_100 + 100 * i as i64),
            );
        }
        // Both runs share one file per stream, as one-hour parts do.
        let mut source = MemorySource::default();
        for stream in BinanceStream::ALL {
            let mut records = first.of(stream, &[]);
            records.extend(second.of(stream, &[]));
            if !records.is_empty() {
                source.seal(SOURCE, stream.raw_name(), records);
            }
        }
        let run1 = first.run(T0, Some(T0 + 20_500), Some(first.records.len() as u64));
        let mut run2 = second.run(
            T0 + 21_000,
            Some(T0 + 30_000),
            Some(second.records.len() as u64),
        );
        run2.seeds = BTreeMap::from([(
            BinanceStream::MarkPrice,
            EventTime::from_millis(T0 + 19_801),
        )]);
        let out = replay(&source, vec![run2, run1.clone()], window(T0, T0 + 60_000));

        assert!(out.windows(2).all(|w| w[0] < w[1]));
        let mut engine = MarketStateEngine::new();
        for event in &out {
            engine.apply(event).unwrap_or_else(|e| panic!("{e}"));
        }
        let live1 = first.live(&run1);
        let last1 = live1.last().unwrap().time().as_millis();
        assert_eq!(out[..live1.len()], live1[..]);
        let late_marks = out
            .iter()
            .filter(|e| {
                matches!(e, MarketEvent::FeedGap(g)
                    if g.stream == Stream::MarkPrice && g.reason == GapReason::LateEvent)
            })
            .count();
        assert!(late_marks >= 1, "{out:?}");
        let retimed: Vec<_> = out[live1.len()..]
            .iter()
            .filter_map(|e| match e {
                MarketEvent::OpenInterest(oi) => Some(oi.time.as_millis()),
                _ => None,
            })
            .collect();
        assert_eq!(retimed.len(), 1, "{out:?}");
        assert!(retimed[0] > last1);
        assert_eq!(
            out.iter()
                .filter(|e| matches!(e, MarketEvent::Trade(_)))
                .count(),
            120
        );
    }

    #[test]
    fn a_clean_run_must_be_complete_and_a_crashed_run_replays_its_prefix() {
        let b = busy_run();
        let n = b.records.len() as u64;
        let w = window(T0, T0 + 60_000);

        // A hole in a clean run.
        let mut holed = MemorySource::default();
        b.seal(&mut holed, &[3]);
        let live = LiveReplay::new(
            &holed,
            SOURCE,
            "BTCUSDT",
            vec![b.run(T0, Some(T0 + 21_000), Some(n))],
        );
        let err = drain_err(&mut live.replay(w).unwrap().stream);
        assert!(
            matches!(&err, ProviderError::Source(d) if d.contains("receive_seq [3, 4) is missing")),
            "{err}"
        );

        // A clean run whose tail is missing.
        let mut complete = MemorySource::default();
        b.seal(&mut complete, &[]);
        let live = LiveReplay::new(
            &complete,
            SOURCE,
            "BTCUSDT",
            vec![b.run(T0, Some(T0 + 21_000), Some(n + 2))],
        );
        let err = drain_err(&mut live.replay(w).unwrap().stream);
        assert!(
            matches!(&err, ProviderError::Source(d) if d.contains(&format!("[{n}, {})", n + 2))),
            "{err}"
        );

        // The same hole in a crashed run: the prefix is recomputed, the rest
        // counted.
        let crashed = b.run(T0, None, None);
        let live = LiveReplay::new(&holed, SOURCE, "BTCUSDT", vec![crashed.clone()]);
        let mut stream = live.replay(w).unwrap().stream;
        let out = drain(&mut stream);
        let stats = &stream.runs()[0];
        assert_eq!(
            (stats.records, stats.replayed, stats.ignored),
            (n - 1, 3, n - 4)
        );
        assert!(!stats.clean);
        let prefix = RunBuilder {
            run_id: b.run_id.clone(),
            records: b.records[..3].to_vec(),
        };
        let expected: Vec<_> = prefix
            .live(&crashed)
            .into_iter()
            .filter(|e| w.contains(e.time()))
            .collect();
        assert_eq!(out, expected);

        // A receive_seq stored twice breaks the contract.
        let mut twice = MemorySource::default();
        b.seal(&mut twice, &[]);
        twice.seal(
            SOURCE,
            "markPrice",
            b.of(BinanceStream::MarkPrice, &[])[..1].to_vec(),
        );
        let live = LiveReplay::new(
            &twice,
            SOURCE,
            "BTCUSDT",
            vec![b.run(T0, Some(T0 + 21_000), Some(n))],
        );
        let err = drain_err(&mut live.replay(w).unwrap().stream);
        assert!(
            matches!(&err, ProviderError::Contract(d) if d.contains("stored twice")),
            "{err}"
        );
    }

    fn drain_err(stream: &mut LiveReplayStream<'_>) -> ProviderError {
        loop {
            match stream.next_event() {
                Ok(Some(_)) => {}
                Ok(None) => panic!("the replay did not fail"),
                Err(err) => {
                    // The failure sticks.
                    assert_eq!(stream.next_event(), Err(err.clone()));
                    return err;
                }
            }
        }
    }

    #[test]
    fn a_gap_still_open_at_the_window_end_is_trailing_not_delivered() {
        let mut b = RunBuilder::new("20261006T010000Z");
        for i in 0..5_u64 {
            b.push(
                BinanceStream::AggTrade,
                1,
                agg(1 + i, T0 + 1_000 * i as i64),
            );
        }
        // Reconnected at 9 s: a Disconnected gap over [4 s, 9 s].
        b.push(BinanceStream::AggTrade, 2, agg(6, T0 + 9_000));
        let mut source = MemorySource::default();
        b.seal(&mut source, &[]);
        let run = b.run(T0, Some(T0 + 10_000), Some(6));
        let live = LiveReplay::new(&source, SOURCE, "BTCUSDT", vec![run]);
        let mut stream = live.replay(window(T0, T0 + 7_000)).unwrap().stream;
        let out = drain(&mut stream);
        assert_eq!(out.len(), 5);
        assert!(out.iter().all(|e| matches!(e, MarketEvent::Trade(_))));
        let trailing = stream.trailing_gaps();
        assert_eq!(trailing.len(), 1);
        assert_eq!(
            (
                trailing[0].start.as_millis(),
                trailing[0].end.as_millis(),
                trailing[0].reason
            ),
            (T0 + 4_000, T0 + 9_000, GapReason::Disconnected)
        );
        // A window that contains the resumption delivers the gap.
        let mut stream = live.replay(window(T0, T0 + 9_001)).unwrap().stream;
        assert_eq!(drain(&mut stream).len(), 7);
        assert!(stream.trailing_gaps().is_empty());
    }

    #[test]
    fn only_runs_overlapping_the_window_are_read() {
        let b = busy_run();
        let mut source = MemorySource::default();
        b.seal(&mut source, &[]);
        let run = b.run(T0, Some(T0 + 21_000), Some(b.records.len() as u64));
        let live = LiveReplay::new(&source, SOURCE, "BTCUSDT", vec![run]);
        // Ten minutes after the run's end, beyond its margin.
        let later = window(T0 + 21_000 + RUN_MARGIN_MS, T0 + 3_600_000);
        let replay = live.replay(later).unwrap();
        let mut stream = replay.stream;
        assert!(drain(&mut stream).is_empty());
        assert!(stream.runs().is_empty());
        assert_eq!(source.reads.get(), 0);
        // Another dataset than the run's window.
        let during = live.replay(window(T0, T0 + 1_000)).unwrap();
        assert_ne!(during.dataset, replay.dataset);
        assert!(matches!(
            live.replay(window(T0, T0)),
            Err(ProviderError::Contract(_))
        ));
    }

    #[test]
    fn records_of_a_run_without_run_start_fail_the_replay() {
        let mut journaled = RunBuilder::new("20261006T010000Z");
        for i in 0..10_u64 {
            journaled.push(
                BinanceStream::AggTrade,
                1,
                agg(1 + i, T0 + 1_000 * i as i64),
            );
        }
        // The next run's run_start never reached the journal.
        let mut lost = RunBuilder::new("20261006T010030Z");
        for i in 0..10_u64 {
            lost.push(
                BinanceStream::AggTrade,
                1,
                agg(100 + i, T0 + 31_000 + 1_000 * i as i64),
            );
        }
        let mut late = RunBuilder::new("20261006T020000Z");
        late.push(BinanceStream::AggTrade, 1, agg(500, T0 + 3_600_000));
        let mut source = MemorySource::default();
        let mut records = journaled.of(BinanceStream::AggTrade, &[]);
        records.extend(lost.of(BinanceStream::AggTrade, &[]));
        source.seal(SOURCE, "aggTrade", records);
        late.seal(&mut source, &[]);
        let runs = vec![
            journaled.run(T0, Some(T0 + 10_000), Some(10)),
            late.run(T0 + 3_600_000, Some(T0 + 3_601_000), Some(1)),
        ];
        let live = LiveReplay::new(&source, SOURCE, "BTCUSDT", runs);

        // Read along with the journaled run's files.
        let err = drain_err(&mut live.replay(window(T0, T0 + 60_000)).unwrap().stream);
        assert!(
            matches!(&err, ProviderError::Source(d)
                if d.contains("without a run_start") && d.contains("10 of run \"20261006T010030Z\"")),
            "{err}"
        );
        // A window holding the lost run alone: an error, not "no event".
        let lone = window(T0 + 320_000, T0 + 600_000);
        let mut source_lone = MemorySource::default();
        source_lone.seal(
            SOURCE,
            "aggTrade",
            lost.of(BinanceStream::AggTrade, &[])
                .into_iter()
                .map(|mut r| {
                    r.event_time = EventTime::from_millis(r.event_time.as_millis() + 300_000);
                    r
                })
                .collect(),
        );
        let live_lone = LiveReplay::new(&source_lone, SOURCE, "BTCUSDT", Vec::new());
        let err = drain_err(&mut live_lone.replay(lone).unwrap().stream);
        assert!(
            matches!(&err, ProviderError::Source(d) if d.contains("the window hold")),
            "{err}"
        );
        // Windows away from the lost run still replay.
        assert_eq!(
            replay(
                &source,
                live.runs().to_vec(),
                window(T0 + 3_000_000, T0 + 4_000_000)
            )
            .len(),
            1
        );
    }

    #[test]
    fn a_closed_window_keeps_its_dataset_version_while_a_later_run_captures() {
        let mut first = RunBuilder::new("20261006T010000Z");
        for i in 0..20_u64 {
            first.push(
                BinanceStream::AggTrade,
                1,
                agg(1 + i, T0 + 1_000 * i as i64),
            );
        }
        // A run started 60 s after the window's end, still capturing.
        let mut running = RunBuilder::new("20261006T010121Z");
        for i in 0..10_u64 {
            running.push(
                BinanceStream::AggTrade,
                1,
                agg(100 + i, T0 + 81_000 + 1_000 * i as i64),
            );
        }
        let mut source = MemorySource::default();
        first.seal(&mut source, &[]);
        source.seal(SOURCE, "aggTrade", running.of(BinanceStream::AggTrade, &[]));
        let runs = vec![
            first.run(T0, Some(T0 + 21_000), Some(20)),
            running.run(T0 + 81_000, None, None),
        ];
        let w = window(T0, T0 + 21_000);
        let open = |source: &MemorySource| {
            let live = LiveReplay::new(source, SOURCE, "BTCUSDT", runs.clone());
            let opened = live.replay(w).unwrap();
            let mut stream = opened.stream;
            (opened.dataset, drain(&mut stream))
        };
        let (version, events) = open(&source);
        assert_eq!(events.len(), 20);
        // The running run seals more files, beyond the window's end plus the
        // margin: the closed window's version and events stay.
        for hour in 1..4_i64 {
            let mut more = RunBuilder::new("20261006T010121Z");
            more.push(
                BinanceStream::AggTrade,
                1,
                agg(1_000 + hour as u64, T0 + hour * 3_600_000),
            );
            source.seal(SOURCE, "aggTrade", more.of(BinanceStream::AggTrade, &[]));
            assert_eq!(
                open(&source),
                (version.clone(), events.clone()),
                "hour {hour}"
            );
        }
    }

    #[test]
    fn an_unjournaled_neighbour_fails_only_the_windows_holding_its_records() {
        // Run R0's journal was lost (say the journal was rotated at the
        // restart); it ended 60 s before the journaled two-hour run R1.
        let mut lost = RunBuilder::new("20261006T005800Z");
        for i in 0..10_u64 {
            lost.push(
                BinanceStream::AggTrade,
                1,
                agg(1 + i, T0 - 70_000 + 1_000 * i as i64),
            );
        }
        let mut run = RunBuilder::new("20261006T010000Z");
        for i in 0..120_u64 {
            run.push(
                BinanceStream::AggTrade,
                1,
                agg(100 + i, T0 + 60_000 * i as i64),
            );
        }
        let mut source = MemorySource::default();
        // R0's records share R1's first one-hour part.
        let mut first_part = lost.of(BinanceStream::AggTrade, &[]);
        let r1 = run.of(BinanceStream::AggTrade, &[]);
        first_part.extend(r1[..60].iter().cloned());
        source.seal(SOURCE, "aggTrade", first_part);
        source.seal(SOURCE, "aggTrade", r1[60..].to_vec());
        let runs = vec![run.run(T0, Some(T0 + 7_200_000), Some(120))];
        let live = LiveReplay::new(&source, SOURCE, "BTCUSDT", runs.clone());

        // An hour into R1: R0's records cannot affect it.
        let inner = window(T0 + 3_600_000, T0 + 4_200_000);
        let mut stream = live.replay(inner).unwrap().stream;
        let out = drain(&mut stream);
        assert_eq!(out.len(), 10);
        let expected: Vec<_> = run
            .live(&runs[0])
            .into_iter()
            .filter(|e| inner.contains(e.time()))
            .collect();
        assert_eq!(out, expected);
        assert_eq!(
            stream.unattributed_outside(),
            &BTreeMap::from([("20261006T005800Z".to_owned(), 10)])
        );

        // A window holding R0's records still fails, naming R0 and its file.
        let err = drain_err(
            &mut live
                .replay(window(T0 - 120_000, T0 + 60_000))
                .unwrap()
                .stream,
        );
        assert!(
            matches!(&err, ProviderError::Source(d)
                if d.contains("10 of run \"20261006T005800Z\" in binance-um/BTCUSDT/aggTrade/date=")
                    && !d.contains("20261006T010000Z")),
            "{err}"
        );
    }
}
