//! Replay of the archive backfill from the raw store (#11, ADR-039 D5).
//!
//! Archive rows carry exchange fields only, and every row is filed at its
//! ADR-028 ordering time (ADR-034 D3), so an archive replay is a streaming
//! canonical merge of the normalized rows — there is no capture run to
//! recompute:
//!
//! - **Selection.** Each requested stream is selected over its window
//!   widened to whole UTC days. Only events inside the requested window are
//!   delivered. One dataset version per distinct window identifies the
//!   files.
//! - **Source days.** Which archive days the store holds is read from the
//!   sealed files' manifests, by **source day**: the UTC day of the archive
//!   file a row comes from, which is the day of its ordering time minus the
//!   stream's [`source_offset_ms`] (one five-minute step for `metrics`,
//!   whose rows are ordered at the end of their interval, ADR-034 D3; zero
//!   for every other stream). A file's smallest and largest event time give
//!   the source days it holds. Partition dates alone would not do: a
//!   metrics file's 23:55 row is ordered at 00:00 of the next day and lands
//!   in that day's partition, which would make a never-imported next day
//!   look present.
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
//!   The `trades` stream (kline cross-check only) is the exception: its id
//!   space is sparse by design, so an id jump there is no gap (ADR-045);
//!   dedupe and missing days still apply to it.
//! - **Missing days.** A source day of a requested window that no sealed
//!   file of the stream holds is a `MissingData` gap from the stream's
//!   previous event (or the window start) to its first event after the
//!   missing days, delivered right before that event in place of any
//!   sequence-break gap. Missing days with no later event in the window are
//!   reported in [`trailing_gaps`](ArchiveReplayStream::trailing_gaps)
//!   only, as a live gap still open at the window's end would be.
//! - **Fold** ([`TradeCorrection::Fold`], ADR-050). When `aggTrades` is
//!   requested, the `trades` files of the same days join its selection, so
//!   its dataset version covers them (unless the store holds none: then the
//!   data, and the version, are those of [`TradeCorrection::None`]). They
//!   are a side input, indexed by source day, never delivered: no gap, no
//!   sequencer, no missing day of their own. When the first `aggTrades` file
//!   of a source day opens, [`fold_day`] runs over that day's `aggTrades`
//!   parts and then its `trades` parts, each in `(min_event_time, path)`
//!   order and one sealed part at a time, for that day and the next one:
//!   the next day's head trades may fold into this day's last aggregate,
//!   so they are known before it is delivered. Days fold in order, each
//!   from the [`Carry`] of the day before when the replay has that day
//!   (the last range of a day without `trades` files is read for it). A
//!   delivered aggregate with a fold carries its folded quantity. An
//!   unattributed or unresolved trade becomes one `MissingData` gap per
//!   run, delivered before the next aggregate above it (from the window
//!   start when the aggregate below lies before the window), unless another
//!   gap already stands there; when the window ends first, it is a
//!   [`trailing_gaps`](ArchiveReplayStream::trailing_gaps) entry, as a live
//!   gap still open at the window's end would be. A day without `trades`
//!   files is unchecked and delivered as normalized.
//!   [`fold_report`](ArchiveReplayStream::fold_report) holds each day's
//!   status and, per 1m minute, the folded aggregates delivered in the
//!   window.

use super::ARCHIVE_SOURCE;
use super::catalog::{ArchiveStream, DAY_MS, day_label};
use super::fold::{AggRange, Carry, DayFold, Fold, FoldError, TradeCorrection, fold_day};
use super::normalize::{METRICS_RESOLUTION_MS, agg_range, parse};
use crate::holdback::HoldBack;
use crate::sequence::StreamSequencer;
use mie_domain::event::{FeedGap, GapReason, MarketEvent, Trade};
use mie_domain::num::Qty;
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
    correction: TradeCorrection,
}

impl fmt::Debug for ArchiveReplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArchiveReplay")
            .field("symbol", &self.symbol)
            .field("streams", &self.streams)
            .field("correction", &self.correction)
            .finish_non_exhaustive()
    }
}

impl<'a> ArchiveReplay<'a> {
    /// A replay of `streams` of `symbol` from the archive source, with the
    /// `aggTrades` fold `correction` (module docs).
    pub fn new(
        source: &'a dyn RawRecordSource,
        symbol: &str,
        streams: &[ArchiveStream],
        correction: TradeCorrection,
    ) -> Self {
        Self {
            source,
            symbol: symbol.to_owned(),
            streams: streams.to_vec(),
            correction,
        }
    }

    /// A replay of the `configured` streams that replay by default
    /// ([`ArchiveStream::replay_default`]): everything but `trades` and
    /// `bookDepth`.
    pub fn with_defaults(
        source: &'a dyn RawRecordSource,
        symbol: &str,
        configured: &[ArchiveStream],
        correction: TradeCorrection,
    ) -> Self {
        let streams: Vec<_> = configured
            .iter()
            .copied()
            .filter(|s| s.replay_default())
            .collect();
        Self::new(source, symbol, &streams, correction)
    }

    /// The replayed streams.
    pub fn streams(&self) -> &[ArchiveStream] {
        &self.streams
    }

    /// The `aggTrades` fold correction.
    pub fn correction(&self) -> TradeCorrection {
        self.correction
    }
}

impl<'a> HistoricalDataProvider for ArchiveReplay<'a> {
    type Stream = ArchiveReplayStream<'a>;

    fn replay(&self, window: ReplayWindow) -> Result<Replay<Self::Stream>, ProviderError> {
        let requests: Vec<_> = self.streams.iter().map(|&s| (s, window)).collect();
        let stream = open_requests(self.source, &self.symbol, &requests, self.correction)?;
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
    /// Source days (UTC days since 1970-01-01, module docs) of the widened
    /// window that no sealed file holds.
    pub missing_days: Vec<i64>,
}

/// What the fold found on one source day (ADR-050 D7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayStatus {
    /// Uncovered trades were found, folded into aggregates or
    /// unattributed, or trades whose aggregate below is unknown.
    Corrected {
        /// Aggregates that carry a fold of the day's trades (one of them
        /// may be the previous day's last).
        aggregates: u64,
        /// Trades folded into them.
        trades: u64,
        /// Their summed quantity.
        qty: Qty,
        /// Uncovered trades that fold nowhere (ADR-050 D4).
        unattributed: u64,
        /// Of `trades`, the edge ids (ADR-050 D3): after the day's last
        /// range, or before its first.
        edge_trades: u64,
        /// Their summed quantity.
        edge_qty: Qty,
        /// Edge ids whose aggregate below the replay does not have:
        /// gapped, not folded (ADR-050 D4).
        unresolved: u64,
    },
    /// The day's `trades` files were read and every trade is covered.
    Clean,
    /// No `trades` file holds the day: delivered as normalized.
    Unchecked,
}

/// The folded aggregates delivered in one 1m minute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinuteFold {
    /// Folded aggregates delivered in the minute.
    pub aggregates: u64,
    /// Trades folded into them.
    pub trades: u64,
    /// Their folded quantity.
    pub qty: Qty,
}

/// The provenance of a folding replay (ADR-050 D7): a bar of any timeframe
/// carries folded data iff it contains a minute of [`minutes`](Self::minutes).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FoldReport {
    /// Per source day (UTC days since 1970-01-01) whose `aggTrades` were
    /// opened, the fold status of the whole day.
    pub days: BTreeMap<i64, DayStatus>,
    /// Per 1m minute (open time, epoch ms), the folded aggregates delivered
    /// in the window.
    pub minutes: BTreeMap<i64, MinuteFold>,
}

/// The fold's side input and state (module docs).
#[derive(Debug, Default)]
struct FoldState {
    /// `aggTrades` files per source day, in `(min_event_time, path)` order.
    aggregates: BTreeMap<i64, Vec<SealedFile>>,
    /// `trades` files per source day, in the same order.
    trades: BTreeMap<i64, Vec<SealedFile>>,
    /// Folds of the computed days, by aggregate id.
    folds: BTreeMap<u64, Fold>,
    /// Gaps of unattributed or unresolved trades by the aggregate `N` above
    /// them: the times of `P` (if known) and `N`.
    gaps_before: BTreeMap<u64, (Option<EventTime>, EventTime)>,
    /// Gaps whose `N` opens a later day, by `P`: the time of `P`. They go
    /// before the first aggregate delivered above `P`.
    gaps_after: BTreeMap<u64, EventTime>,
    /// What each folded day hands to the next.
    carries: BTreeMap<i64, Option<Carry>>,
    report: FoldReport,
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

/// How far a row's ordering time lies after the start of the archive day
/// it comes from, at most: one resolution step for `metrics` (ordered at
/// the end of its five-minute interval, ADR-034 D3), zero otherwise.
pub fn source_offset_ms(stream: ArchiveStream) -> i64 {
    match stream {
        ArchiveStream::Metrics => i64::from(METRICS_RESOLUTION_MS),
        _ => 0,
    }
}

/// The source day of a row of `stream` ordered at `time` (module docs).
fn source_day(stream: ArchiveStream, time: EventTime) -> i64 {
    (time.as_millis() - source_offset_ms(stream)).div_euclid(DAY_MS)
}

/// The whole UTC days `[first, end)` a window touches.
fn day_span(window: ReplayWindow) -> (i64, i64) {
    let first = window.start.as_millis().div_euclid(DAY_MS);
    let end = (window.end.as_millis() - 1).div_euclid(DAY_MS) + 1;
    (first, end)
}

/// Opens a replay of several streams, each over its own window (the kline
/// cross-check reads trades a minute beyond the klines' window), with the
/// `aggTrades` fold `correction` (module docs).
///
/// # Errors
///
/// [`ProviderError::Contract`] for no request, an empty window, a stream
/// requested twice, `bookDepth` (no domain event) or `aggTrades` with
/// `trades` (one trade-id space, in both corrections);
/// [`ProviderError::Source`] when the store cannot be listed.
pub fn open_requests<'a>(
    source: &'a dyn RawRecordSource,
    symbol: &str,
    requests: &[(ArchiveStream, ReplayWindow)],
    correction: TradeCorrection,
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
    let mut fold: Option<FoldState> = None;
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
        let folding =
            correction == TradeCorrection::Fold && streams.contains(&ArchiveStream::AggTrades);
        let mut dataset = None;
        if folding {
            fold = Some(FoldState::default());
            // The side input joins the selection, so the version covers it.
            let trades =
                RawStreamKey::new(ARCHIVE_SOURCE, symbol, ArchiveStream::Trades.raw_name())
                    .map_err(source_error)?;
            let mut with_trades = keys.clone();
            with_trades.insert(trades.clone());
            let selection = RawSelection::new(with_trades, widened).map_err(source_error)?;
            let selected = source.select(&selection).map_err(source_error)?;
            // Without a `trades` file the fold reads nothing and delivers
            // the data of `none`, so it keeps that version.
            if selected.files.iter().any(|f| f.stream == trades) {
                dataset = Some(selected);
            }
        }
        let dataset = match dataset {
            Some(dataset) => dataset,
            None => {
                let selection = RawSelection::new(keys, widened).map_err(source_error)?;
                source.select(&selection).map_err(source_error)?
            }
        };
        let mut present: BTreeMap<ArchiveStream, BTreeSet<i64>> = BTreeMap::new();
        for file in dataset.files {
            let stream = ArchiveStream::from_raw_name(file.stream.stream())
                .ok_or_else(|| source_error(format!("unknown archive stream {}", file.stream)))?;
            // A sealed file lies in one date partition, so it holds at most
            // two source days: those of its first and its last row.
            let (first, last) = (
                source_day(stream, file.min_event_time),
                source_day(stream, file.max_event_time),
            );
            if folding && let Some(fold) = fold.as_mut() {
                let index = match stream {
                    ArchiveStream::AggTrades => Some(&mut fold.aggregates),
                    ArchiveStream::Trades => Some(&mut fold.trades),
                    _ => None,
                };
                if let Some(index) = index {
                    for day in first..=last {
                        index.entry(day).or_default().push(file.clone());
                    }
                }
                if stream == ArchiveStream::Trades {
                    continue;
                }
            }
            let held = present.entry(stream).or_default();
            held.insert(first);
            held.insert(last);
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
    if let Some(fold) = fold.as_mut() {
        for files in fold.aggregates.values_mut().chain(fold.trades.values_mut()) {
            files.sort_by(|a, b| {
                (a.min_event_time, &a.relative_path).cmp(&(b.min_event_time, &b.relative_path))
            });
        }
    }
    let sequencers = windows
        .keys()
        .map(|&s| {
            let domain = s.domain_stream().expect("checked above");
            let sequencer = StreamSequencer::new(domain, None);
            let sequencer = match s {
                // Sparse by design: an id jump is not a lost trade (ADR-045).
                ArchiveStream::Trades => sequencer.allowing_trade_id_gaps(),
                _ => sequencer,
            };
            (s, sequencer)
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
        fold,
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
    fold: Option<FoldState>,
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

    /// The fold's provenance (module docs); `None` unless the replay folds
    /// (`aggTrades` requested under [`TradeCorrection::Fold`]). Complete
    /// once the stream has ended.
    pub fn fold_report(&self) -> Option<&FoldReport> {
        self.fold.as_ref().map(|fold| &fold.report)
    }

    /// Folds every source day with `aggTrades` files up to `through` that
    /// is not folded yet, in day order, so each starts from the carry of
    /// the day before (module docs).
    fn fold_through(&mut self, through: i64) -> Result<(), ProviderError> {
        let Some(fold) = self.fold.as_ref() else {
            return Ok(());
        };
        let days: Vec<i64> = fold
            .aggregates
            .range(..=through)
            .map(|(&day, _)| day)
            .filter(|day| !fold.report.days.contains_key(day))
            .collect();
        for day in days {
            self.fold_once(day)?;
        }
        Ok(())
    }

    /// Folds source day `day` (module docs).
    fn fold_once(&mut self, day: i64) -> Result<(), ProviderError> {
        let source = self.source;
        let symbol = self.symbol.clone();
        let Some(fold) = self.fold.as_mut() else {
            return Ok(());
        };
        let Some(trade_files) = fold.trades.get(&day) else {
            fold.report.days.insert(day, DayStatus::Unchecked);
            return Ok(());
        };
        let label = day_label(day);
        let failed = |e: FoldError, label: &str| match e {
            FoldError::Input(_) => source_error(format!("fold of {label}: {e}")),
            FoldError::Contract(_) | FoldError::Overflow { .. } => {
                ProviderError::Contract(format!("fold of {label}: {e}"))
            }
        };
        let previous = day - 1;
        let carry = match (fold.carries.get(&previous), fold.aggregates.get(&previous)) {
            (Some(&carry), _) => carry,
            // A previous day without `trades` files: its last range only.
            (None, Some(files)) => {
                fold_day(
                    None,
                    day_ranges(source, files, previous),
                    std::iter::empty(),
                )
                .map_err(|e| failed(e, &day_label(previous)))?
                .carry
            }
            (None, None) => None,
        };
        let agg_files = fold.aggregates.get(&day).map_or(&[][..], Vec::as_slice);
        let folded = fold_day(
            carry,
            day_ranges(source, agg_files, day),
            day_trades(source, trade_files, day, &symbol),
        )
        .map_err(|e| failed(e, &label))?;
        let status = day_status(&folded).ok_or_else(|| {
            ProviderError::Contract(format!("fold of {label}: the day's quantity overflows"))
        })?;
        fold.report.days.insert(day, status);
        fold.carries.insert(day, folded.carry);
        for u in &folded.unattributed {
            match u.before {
                Some(n) => {
                    fold.gaps_before
                        .entry(n.agg_id)
                        .or_insert((Some(u.after.time), n.time));
                }
                None => {
                    fold.gaps_after.insert(u.after.agg_id, u.after.time);
                }
            }
        }
        if let Some(n) = folded.unresolved_before {
            // The aggregate below is unknown: the gap may start anywhere.
            fold.gaps_before.insert(n.agg_id, (None, n.time));
        }
        for (agg_id, add) in folded.folds {
            let merged = match fold.folds.get(&agg_id) {
                None => Some(add),
                // Folded from both sides of a midnight (ADR-050 D3).
                Some(f) => f.qty.checked_add(add.qty).map(|qty| Fold {
                    qty,
                    trades: f.trades + add.trades,
                    last_trade_id: f.last_trade_id.max(add.last_trade_id),
                }),
            };
            let merged = merged.ok_or_else(|| {
                ProviderError::Contract(format!(
                    "fold of {label}: the folded quantity of aggregate {agg_id} overflows"
                ))
            })?;
            fold.folds.insert(agg_id, merged);
        }
        Ok(())
    }

    /// Opens the next planned file and adds its first event to the merge.
    fn open_next(&mut self) -> Result<(), ProviderError> {
        let Planned {
            file,
            stream,
            window,
        } = self.planned[self.next_file].clone();
        self.next_file += 1;
        if stream == ArchiveStream::AggTrades && self.fold.is_some() {
            // The next day too: its head trades may fold into this file's
            // last aggregate (module docs).
            self.fold_through(source_day(stream, file.max_event_time) + 1)?;
        }
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
            let Some(mut event) = parsed else {
                continue;
            };
            if let (Some(fold), MarketEvent::Trade(trade)) = (&self.fold, &mut event)
                && stream == ArchiveStream::AggTrades
                && let Some(folded) = fold.folds.get(&trade.trade_id)
            {
                trade.qty = trade.qty.checked_add(folded.qty).ok_or_else(|| {
                    ProviderError::Contract(format!(
                        "{}: the folded quantity of aggregate {} overflows",
                        file.relative_path, trade.trade_id
                    ))
                })?;
            }
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
    fn admit(&mut self, event: MarketEvent, stream: ArchiveStream) -> Result<(), ProviderError> {
        let sequencer = self
            .sequencers
            .get_mut(&stream)
            .expect("every merged stream has a sequencer");
        let previous = sequencer.last_time();
        let end = event.time();
        let mut sequenced = sequencer.push("", event);
        if sequenced.is_empty() {
            return Ok(());
        }
        let day = source_day(stream, end);
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
        if stream == ArchiveStream::AggTrades
            && let Some(fold) = self.fold.as_mut()
            && let Some(MarketEvent::Trade(trade)) = sequenced.last().cloned()
        {
            // Gaps of unattributed or unresolved trades below this aggregate
            // (ADR-050 D4). One whose `N` lies below it ended before the
            // window; one without `N` goes before the first aggregate above
            // its `P`.
            let id = trade.trade_id;
            let gaps_after = fold.gaps_after.split_off(&id);
            let mut due = !fold.gaps_after.is_empty();
            fold.gaps_after = gaps_after;
            fold.gaps_before = fold.gaps_before.split_off(&id);
            due |= fold.gaps_before.remove(&id).is_some();
            let gapped = sequenced
                .iter()
                .any(|e| matches!(e, MarketEvent::FeedGap(_)));
            if due && !gapped {
                let start = previous.unwrap_or(self.windows[&stream].start);
                sequenced.insert(
                    0,
                    MarketEvent::FeedGap(FeedGap {
                        stream: trade_stream(),
                        start: start.min(end),
                        end,
                        reason: GapReason::MissingData,
                    }),
                );
            }
            if let Some(folded) = fold.folds.get(&trade.trade_id) {
                let ms = trade.time.as_millis();
                let minute = fold
                    .report
                    .minutes
                    .entry(ms - ms.rem_euclid(60_000))
                    .or_insert(MinuteFold {
                        aggregates: 0,
                        trades: 0,
                        qty: Qty::from_units(0),
                    });
                minute.aggregates += 1;
                minute.trades += folded.trades;
                minute.qty = minute.qty.checked_add(folded.qty).ok_or_else(|| {
                    ProviderError::Contract(format!(
                        "the folded quantity of minute {} overflows",
                        ms - ms.rem_euclid(60_000)
                    ))
                })?;
            }
        }
        for event in sequenced {
            let (_, released) = self.holdback.push(event);
            self.out.extend(released);
        }
        Ok(())
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
            let end = EventTime::from_millis(
                ((last + 1) * DAY_MS + source_offset_ms(stream)).min(window.end.as_millis()),
            );
            self.trailing.push(FeedGap {
                stream: stream.domain_stream().expect("checked at open"),
                start: start.min(end),
                end,
                reason: GapReason::MissingData,
            });
        }
        // A fold gap whose aggregate above lies beyond the window's end
        // (ADR-050 D4), unless a missing day already trails there.
        if let Some(fold) = &self.fold {
            let window = self.windows[&ArchiveStream::AggTrades];
            let open = fold
                .gaps_before
                .values()
                .any(|&(p, n)| p.is_none_or(|p| p < window.end) && n > window.start)
                || fold.gaps_after.values().any(|&p| p < window.end);
            let stream = trade_stream();
            if open && !self.trailing.iter().any(|gap| gap.stream == stream) {
                let start = self.sequencers[&ArchiveStream::AggTrades]
                    .last_time()
                    .unwrap_or(window.start);
                self.trailing.push(FeedGap {
                    stream,
                    start: start.min(window.end),
                    end: window.end,
                    reason: GapReason::MissingData,
                });
            }
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
            match self.next_merged().and_then(|merged| match merged {
                Some((event, stream)) => self.admit(event, stream),
                None => {
                    self.finish();
                    Ok(())
                }
            }) {
                Ok(()) => {}
                Err(failed) => {
                    self.failed = Some(failed.clone());
                    return Err(failed);
                }
            }
        }
    }
}

/// The domain series of every trade stream.
fn trade_stream() -> mie_domain::event::Stream {
    ArchiveStream::AggTrades
        .domain_stream()
        .expect("aggTrades has a domain event")
}

/// The rows of `stream` in `file` whose source day is `day`, each
/// converted by `convert` (`None` skips a row), as fold input.
fn read_day<T>(
    source: &dyn RawRecordSource,
    file: &SealedFile,
    stream: ArchiveStream,
    day: i64,
    mut convert: impl FnMut(&[u8]) -> Result<Option<T>, crate::normalize::NormalizeError>,
) -> Result<Vec<T>, FoldError> {
    let records = source
        .read(file)
        .map_err(|e| FoldError::Input(format!("{}: {e}", file.relative_path)))?;
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        if source_day(stream, record.event_time) != day {
            continue;
        }
        let converted = convert(&record.payload).map_err(|e| {
            FoldError::Input(format!(
                "{} at {}: {e}",
                file.relative_path, record.event_time
            ))
        })?;
        rows.extend(converted);
    }
    Ok(rows)
}

/// The `aggTrades` ranges of source day `day` in `files`, one sealed part
/// at a time, each part in id order.
fn day_ranges<'f>(
    source: &'f dyn RawRecordSource,
    files: &'f [SealedFile],
    day: i64,
) -> impl Iterator<Item = Result<AggRange, FoldError>> + 'f {
    parts(files, move |file| {
        read_day(source, file, ArchiveStream::AggTrades, day, |row| {
            agg_range(row).map(Some)
        })
        .map(|mut ranges: Vec<AggRange>| {
            ranges.sort_unstable_by_key(|r| r.agg_id);
            ranges
        })
    })
}

/// The `trades` rows of source day `day` in `files`, one sealed part at a
/// time, each part in id order.
fn day_trades<'f>(
    source: &'f dyn RawRecordSource,
    files: &'f [SealedFile],
    day: i64,
    symbol: &'f str,
) -> impl Iterator<Item = Result<Trade, FoldError>> + 'f {
    parts(files, move |file| {
        read_day(source, file, ArchiveStream::Trades, day, |row| {
            parse(ArchiveStream::Trades, symbol, row).map(|event| match event {
                Some(MarketEvent::Trade(trade)) => Some(trade),
                _ => None,
            })
        })
        .map(|mut trades: Vec<Trade>| {
            trades.sort_unstable_by_key(|t| t.trade_id);
            trades
        })
    })
}

/// The report status of a day's fold; `None` when its quantity overflows.
fn day_status(folded: &DayFold) -> Option<DayStatus> {
    if folded.uncovered == 0 && folded.unresolved == 0 {
        return Some(DayStatus::Clean);
    }
    let mut qty = Qty::from_units(0);
    for f in folded.folds.values() {
        qty = qty.checked_add(f.qty)?;
    }
    Some(DayStatus::Corrected {
        aggregates: folded.folds.len() as u64,
        trades: folded.folded_trades(),
        qty,
        unattributed: folded.unattributed.len() as u64,
        edge_trades: folded.edge_folded,
        edge_qty: folded.edge_qty,
        unresolved: folded.unresolved,
    })
}

/// The rows of `files`, read one file at a time by `read`; a failed read
/// ends the rows with its error.
fn parts<'f, T>(
    files: &'f [SealedFile],
    mut read: impl FnMut(&SealedFile) -> Result<Vec<T>, FoldError> + 'f,
) -> impl Iterator<Item = Result<T, FoldError>> + 'f
where
    T: 'f,
{
    files.iter().flat_map(move |file| {
        let (rows, error) = match read(file) {
            Ok(rows) => (rows, None),
            Err(error) => (Vec::new(), Some(error)),
        };
        rows.into_iter().map(Ok).chain(error.map(Err))
    })
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
        crate::archive::catalog::parse_day(label).unwrap()
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
            TradeCorrection::None,
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
        let replay = ArchiveReplay::new(
            &source,
            "BTCUSDT",
            &[ArchiveStream::AggTrades],
            TradeCorrection::None,
        );

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

    /// Seals day `d`'s metrics file as the Parquet writer files it: the
    /// rows up to 23:50 in partition `d`, the 23:55 row (ordered at 00:00 of
    /// the next day) in partition `d + 1`.
    fn metrics_day(source: &mut MemorySource, d: i64) {
        let civil =
            |h: i64, m: i64| format!("{} {h:02}:{m:02}:00", crate::archive::catalog::day_label(d));
        let in_day = (0..24)
            .map(|h| {
                record(
                    ArchiveStream::Metrics,
                    &metrics_row(&civil(h, 0), "93000.0000000000000000"),
                )
            })
            .collect();
        source.seal(ARCHIVE_SOURCE, "metrics", in_day);
        let spill = record(
            ArchiveStream::Metrics,
            &metrics_row(&civil(23, 55), "93001.0000000000000000"),
        );
        assert_eq!(spill.event_time.as_millis(), (d + 1) * DAY_MS);
        source.seal(ARCHIVE_SOURCE, "metrics", vec![spill]);
    }

    #[test]
    fn a_never_imported_metrics_day_is_missing_despite_the_spilled_row() {
        let (d0, d1, d2) = (day("2026-09-28"), day("2026-09-29"), day("2026-09-30"));
        let mut source = MemorySource::default();
        metrics_day(&mut source, d0);
        // Day d1 was never imported, yet its partition holds d0's 23:55 row.
        metrics_day(&mut source, d2);
        let replay = ArchiveReplay::new(
            &source,
            "BTCUSDT",
            &[ArchiveStream::Metrics],
            TradeCorrection::None,
        );
        let mut stream = replay
            .replay(window(d0 * DAY_MS, (d2 + 1) * DAY_MS))
            .unwrap()
            .stream;
        let out = drain(&mut stream);
        assert_eq!(
            stream.stream_stats()[&ArchiveStream::Metrics].missing_days,
            [d1]
        );
        // From d0's last row (its 23:55 sample, at d1 00:00) to d2's first.
        assert_eq!(
            gaps(&out),
            [(
                Stream::OpenInterest,
                d1 * DAY_MS,
                d2 * DAY_MS + 300_000,
                GapReason::MissingData
            )]
        );
        assert_eq!(out.len(), 2 * 25 - 1 + 1);
        assert!(stream.trailing_gaps().is_empty());

        // All three days imported: nothing missing, no gap.
        metrics_day(&mut source, d1);
        let replay = ArchiveReplay::new(
            &source,
            "BTCUSDT",
            &[ArchiveStream::Metrics],
            TradeCorrection::None,
        );
        let mut stream = replay
            .replay(window(d0 * DAY_MS, (d2 + 1) * DAY_MS))
            .unwrap()
            .stream;
        let out = drain(&mut stream);
        assert!(
            stream.stream_stats()[&ArchiveStream::Metrics]
                .missing_days
                .is_empty()
        );
        assert!(gaps(&out).is_empty(), "{:?}", gaps(&out));
        assert!(out.windows(2).all(|w| w[0] < w[1]));

        // A never-imported last day: trailing only, ending one step into the
        // next day, clamped to the window.
        let mut source = MemorySource::default();
        metrics_day(&mut source, d0);
        let replay = ArchiveReplay::new(
            &source,
            "BTCUSDT",
            &[ArchiveStream::Metrics],
            TradeCorrection::None,
        );
        let mut stream = replay
            .replay(window(d0 * DAY_MS, (d1 + 1) * DAY_MS))
            .unwrap()
            .stream;
        let out = drain(&mut stream);
        assert!(gaps(&out).is_empty());
        let trailing = stream.trailing_gaps();
        assert_eq!(
            (
                trailing.len(),
                trailing[0].start.as_millis(),
                trailing[0].end.as_millis()
            ),
            (1, d1 * DAY_MS, (d1 + 1) * DAY_MS)
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
        let replay = ArchiveReplay::new(
            &source,
            "BTCUSDT",
            &[ArchiveStream::AggTrades],
            TradeCorrection::None,
        );
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
        let replay = ArchiveReplay::new(
            &misfiled,
            "BTCUSDT",
            &[ArchiveStream::AggTrades],
            TradeCorrection::None,
        );
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
        let replay = ArchiveReplay::new(
            &broken,
            "BTCUSDT",
            &[ArchiveStream::AggTrades],
            TradeCorrection::None,
        );
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
            // Refused in both corrections: the fold reads `trades` as a side
            // input, it never delivers them next to `aggTrades`.
            for correction in [TradeCorrection::None, TradeCorrection::Fold] {
                assert!(
                    matches!(
                        open_requests(&source, "BTCUSDT", &requests, correction),
                        Err(ProviderError::Contract(_))
                    ),
                    "{requests:?} {correction}"
                );
            }
        }
        let defaults = ArchiveReplay::with_defaults(
            &source,
            "BTCUSDT",
            &ArchiveStream::ALL,
            TradeCorrection::Fold,
        );
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
            TradeCorrection::None,
        )
        .unwrap();
        assert_eq!(two.dataset_versions().len(), 2);
    }

    // ----------------------------------------------------------- the fold

    /// Minute 00:10 of day `d`.
    fn m(d: i64) -> i64 {
        d * DAY_MS + 600_000
    }

    fn agg(id: u64, price: &str, qty: &str, ids: (u64, u64), time: i64, maker: bool) -> String {
        format!("{id},{price},{qty},{},{},{time},{maker}", ids.0, ids.1)
    }

    fn raw_trade(id: u64, price: &str, qty: &str, time: i64, maker: bool) -> String {
        format!("{id},{price},{qty},0,{time},{maker}")
    }

    fn seal_rows(source: &mut MemorySource, stream: ArchiveStream, rows: &[String]) {
        let records = rows.iter().map(|r| record(stream, r)).collect();
        source.seal(ARCHIVE_SOURCE, stream.raw_name(), records);
    }

    /// Day `d`: aggregates 1 … 4 (ids from `first`) over trade ids
    /// 11 ..= 16 (offset by `10 * first`), and, with `with_trades`, its
    /// `trades` file, where id 14 lies between aggregates 2 and 3 at
    /// `price_14`. Returns the next aggregate id.
    fn fold_day_source(
        source: &mut MemorySource,
        d: i64,
        first: u64,
        with_trades: bool,
        price_14: &str,
    ) -> u64 {
        let (a, t) = (first, 10 * first);
        let m = m(d);
        seal_rows(
            source,
            ArchiveStream::AggTrades,
            &[
                agg(a, "85000.10", "0.010", (t + 11, t + 11), m + 1, false),
                agg(a + 1, "85000.20", "0.020", (t + 12, t + 13), m + 2, true),
                agg(a + 2, "85000.30", "0.030", (t + 15, t + 15), m + 3, false),
                agg(
                    a + 3,
                    "85000.40",
                    "0.040",
                    (t + 16, t + 16),
                    m + 300_000,
                    false,
                ),
            ],
        );
        if with_trades {
            seal_rows(
                source,
                ArchiveStream::Trades,
                &[
                    raw_trade(t + 11, "85000.10", "0.010", m + 1, false),
                    raw_trade(t + 12, "85000.20", "0.012", m + 2, true),
                    raw_trade(t + 13, "85000.20", "0.008", m + 2, true),
                    raw_trade(t + 14, price_14, "0.005", m + 2, true),
                    raw_trade(t + 15, "85000.30", "0.030", m + 3, false),
                    raw_trade(t + 16, "85000.40", "0.040", m + 300_000, false),
                ],
            );
        }
        a + 4
    }

    struct Opened {
        events: Vec<MarketEvent>,
        version: DatasetVersion,
        report: Option<FoldReport>,
        trailing: Vec<FeedGap>,
    }

    fn open(
        source: &MemorySource,
        streams: &[ArchiveStream],
        w: ReplayWindow,
        correction: TradeCorrection,
    ) -> Opened {
        let replay = ArchiveReplay::new(source, "BTCUSDT", streams, correction);
        let opened = replay.replay(w).unwrap();
        let mut stream = opened.stream;
        let events = drain(&mut stream);
        Opened {
            events,
            version: opened.dataset,
            report: stream.fold_report().cloned(),
            trailing: stream.trailing_gaps().to_vec(),
        }
    }

    fn qty(text: &str) -> Qty {
        text.parse().unwrap()
    }

    #[test]
    fn a_fold_changes_only_the_folded_quantity_and_the_version() {
        let d = day("2026-05-15");
        let mut source = MemorySource::default();
        fold_day_source(&mut source, d, 1, true, "85000.20");
        let w = window(d * DAY_MS, (d + 1) * DAY_MS);
        let none = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::None,
        );
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::Fold,
        );

        assert_eq!(fold.events.len(), none.events.len());
        let differing: Vec<_> = none
            .events
            .iter()
            .zip(&fold.events)
            .filter(|(a, b)| a != b)
            .collect();
        let [(MarketEvent::Trade(before), MarketEvent::Trade(after))] = differing[..] else {
            panic!("{differing:?}");
        };
        assert_eq!(after.trade_id, 2);
        assert_eq!((before.qty, after.qty), (qty("0.020"), qty("0.025")));
        assert_eq!(
            Trade {
                qty: before.qty,
                ..*after
            },
            *before,
            "id, price, time and side stay"
        );
        assert_ne!(fold.version, none.version, "the version covers trades");
        assert_eq!(none.report, None);
        assert_eq!(
            fold.report,
            Some(FoldReport {
                days: BTreeMap::from([(
                    d,
                    DayStatus::Corrected {
                        aggregates: 1,
                        trades: 1,
                        qty: qty("0.005"),
                        unattributed: 0,
                        edge_trades: 0,
                        edge_qty: Qty::from_units(0),
                        unresolved: 0
                    }
                )]),
                minutes: BTreeMap::from([(
                    m(d),
                    MinuteFold {
                        aggregates: 1,
                        trades: 1,
                        qty: qty("0.005")
                    }
                )]),
            })
        );
        assert!(gaps(&fold.events).is_empty());
    }

    #[test]
    fn a_replay_without_aggtrades_has_the_same_version_in_both_modes() {
        let d = day("2026-05-15");
        let mut source = MemorySource::default();
        fold_day_source(&mut source, d, 1, true, "85000.20");
        metrics_day(&mut source, d);
        let w = window(d * DAY_MS, (d + 1) * DAY_MS);
        let none = open(&source, &[ArchiveStream::Metrics], w, TradeCorrection::None);
        let fold = open(&source, &[ArchiveStream::Metrics], w, TradeCorrection::Fold);
        assert_eq!(fold.version, none.version);
        assert_eq!(fold.events, none.events);
        assert_eq!(fold.report, None);
    }

    #[test]
    fn a_day_without_trades_is_unchecked_and_delivered_as_normalized() {
        let (d0, d1) = (day("2026-05-15"), day("2026-05-16"));
        let mut source = MemorySource::default();
        let next = fold_day_source(&mut source, d0, 1, false, "85000.20");
        let d0_window = window(d0 * DAY_MS, (d0 + 1) * DAY_MS);
        let none = open(
            &source,
            &[ArchiveStream::AggTrades],
            d0_window,
            TradeCorrection::None,
        );
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            d0_window,
            TradeCorrection::Fold,
        );
        assert_eq!(fold.events, none.events);
        // No `trades` file at all: the data, and the version, of `none`.
        assert_eq!(fold.version, none.version);
        let unchecked = FoldReport {
            days: BTreeMap::from([(d0, DayStatus::Unchecked)]),
            minutes: BTreeMap::new(),
        };
        assert_eq!(fold.report.as_ref(), Some(&unchecked));

        // The next day has trades: it is corrected, d0 stays unchecked.
        fold_day_source(&mut source, d1, next, true, "85000.20");
        let both = window(d0 * DAY_MS, (d1 + 1) * DAY_MS);
        let none = open(
            &source,
            &[ArchiveStream::AggTrades],
            both,
            TradeCorrection::None,
        );
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            both,
            TradeCorrection::Fold,
        );
        assert_ne!(fold.version, none.version);
        let report = fold.report.unwrap();
        assert_eq!(report.days[&d0], DayStatus::Unchecked);
        assert!(matches!(
            report.days[&d1],
            DayStatus::Corrected { trades: 1, .. }
        ));
        let on_d0 = |events: &[MarketEvent]| -> Vec<MarketEvent> {
            events
                .iter()
                .filter(|e| e.time().as_millis() < d1 * DAY_MS)
                .cloned()
                .collect()
        };
        assert_eq!(on_d0(&fold.events), on_d0(&none.events));
        assert_eq!(report.minutes.keys().copied().collect::<Vec<_>>(), [m(d1)]);
    }

    /// The 1m bars a replay closed.
    fn closed_minutes(events: &[MarketEvent]) -> Vec<Bar> {
        let mut engine = MarketStateEngine::new();
        let mut minutes = Vec::new();
        for event in events {
            engine.apply(event).unwrap();
            minutes.extend(
                engine
                    .closed_bars()
                    .iter()
                    .filter(|bar| bar.timeframe == Timeframe::M1)
                    .cloned(),
            );
        }
        minutes
    }

    use mie_domain::bars::{Bar, Timeframe};

    #[test]
    fn an_unattributed_trade_is_a_missing_data_gap_before_the_aggregate_above() {
        let d = day("2026-05-15");
        let mut source = MemorySource::default();
        // Trade 14 trades at another price than aggregate 2: no fold.
        fold_day_source(&mut source, d, 1, true, "85000.25");
        let w = window(d * DAY_MS, (d + 1) * DAY_MS);
        let none = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::None,
        );
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::Fold,
        );
        let m = m(d);
        assert_eq!(
            gaps(&fold.events),
            [(Stream::Trades, m + 2, m + 3, GapReason::MissingData)]
        );
        let at = fold
            .events
            .iter()
            .position(|e| matches!(e, MarketEvent::FeedGap(_)))
            .unwrap();
        assert!(matches!(&fold.events[at + 1], MarketEvent::Trade(t) if t.trade_id == 3));
        assert_eq!(fold.events.len(), none.events.len() + 1);
        let report = fold.report.unwrap();
        assert_eq!(
            report.days[&d],
            DayStatus::Corrected {
                aggregates: 0,
                trades: 0,
                qty: Qty::from_units(0),
                unattributed: 1,
                edge_trades: 0,
                edge_qty: Qty::from_units(0),
                unresolved: 0
            }
        );
        assert!(report.minutes.is_empty());
        // The minute the gap touches is marked, as any feed gap is.
        let marked = |events: &[MarketEvent]| -> Vec<(i64, bool)> {
            closed_minutes(events)
                .iter()
                .map(|bar| (bar.open_time.as_millis(), bar.coverage.feed_gap))
                .filter(|&(open, _)| open == m)
                .collect()
        };
        assert_eq!(marked(&fold.events), [(m, true)]);
        assert_eq!(marked(&none.events), [(m, false)]);

        // A gap already standing before that aggregate is not doubled.
        let mut source = MemorySource::default();
        seal_rows(
            &mut source,
            ArchiveStream::AggTrades,
            &[
                agg(1, "85000.20", "0.020", (12, 13), m + 2, true),
                agg(5, "85000.30", "0.030", (15, 15), m + 3, false),
            ],
        );
        seal_rows(
            &mut source,
            ArchiveStream::Trades,
            &[
                raw_trade(12, "85000.20", "0.020", m + 2, true),
                raw_trade(14, "85000.25", "0.005", m + 2, true),
                raw_trade(15, "85000.30", "0.030", m + 3, false),
            ],
        );
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::Fold,
        );
        assert_eq!(
            gaps(&fold.events),
            [(Stream::Trades, m + 2, m + 3, GapReason::SequenceBreak)]
        );
    }

    #[test]
    fn folds_outside_the_window_are_neither_applied_nor_reported() {
        let d = day("2026-05-15");
        let mut source = MemorySource::default();
        fold_day_source(&mut source, d, 1, true, "85000.20");
        // From aggregate 3 on: the folded aggregate 2 lies before the window.
        let w = window(m(d) + 3, (d + 1) * DAY_MS);
        let none = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::None,
        );
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::Fold,
        );
        assert_eq!(fold.events, none.events);
        let report = fold.report.unwrap();
        assert!(report.minutes.is_empty());
        // The day line describes the whole source day.
        assert!(matches!(
            report.days[&d],
            DayStatus::Corrected { trades: 1, .. }
        ));
    }

    #[test]
    fn a_broken_range_contract_fails_the_fold_naming_day_and_aggregate() {
        let d = day("2026-05-15");
        let m = m(d);
        let mut source = MemorySource::default();
        seal_rows(
            &mut source,
            ArchiveStream::AggTrades,
            &[
                agg(1, "85000.20", "0.020", (12, 14), m + 2, true),
                agg(2, "85000.30", "0.030", (14, 15), m + 3, false),
            ],
        );
        seal_rows(
            &mut source,
            ArchiveStream::Trades,
            &[raw_trade(12, "85000.20", "0.020", m + 2, true)],
        );
        let replay = ArchiveReplay::new(
            &source,
            "BTCUSDT",
            &[ArchiveStream::AggTrades],
            TradeCorrection::Fold,
        );
        let err = replay
            .replay(window(d * DAY_MS, (d + 1) * DAY_MS))
            .unwrap()
            .stream
            .next_event()
            .unwrap_err();
        assert!(
            matches!(&err, ProviderError::Contract(detail)
                if detail.starts_with("fold of 2026-05-15: aggregate 2:")),
            "{err}"
        );
    }

    // ------------------------------------- #109 review findings (ADR-050)

    fn corrected(trades: u64, q: &str, unattributed: u64, edge: u64, unresolved: u64) -> DayStatus {
        let q = qty(q);
        DayStatus::Corrected {
            aggregates: u64::from(trades > 0),
            trades,
            qty: q,
            unattributed,
            edge_trades: edge,
            edge_qty: if edge > 0 { q } else { Qty::from_units(0) },
            unresolved,
        }
    }

    /// The quantity each delivered aggregate carries, by id.
    fn qty_by_id(events: &[MarketEvent]) -> BTreeMap<u64, Qty> {
        events
            .iter()
            .filter_map(|e| match e {
                MarketEvent::Trade(t) => Some((t.trade_id, t.qty)),
                _ => None,
            })
            .collect()
    }

    /// Day `d` ends with aggregate 2 over trades 12 ..= 13; `d + 1` starts
    /// with aggregate 3 over trade 15. Trade 14, at `price_14`, is filed on
    /// `d` at `m + 9`.
    fn end_of_day_source(d: i64, price_14: &str) -> MemorySource {
        let m = m(d);
        let mut source = MemorySource::default();
        seal_rows(
            &mut source,
            ArchiveStream::AggTrades,
            &[
                agg(1, "85000.10", "0.010", (11, 11), m + 1, false),
                agg(2, "85000.20", "0.020", (12, 13), m + 2, true),
            ],
        );
        seal_rows(
            &mut source,
            ArchiveStream::AggTrades,
            &[agg(
                3,
                "85000.30",
                "0.030",
                (15, 15),
                (d + 1) * DAY_MS + 5,
                false,
            )],
        );
        seal_rows(
            &mut source,
            ArchiveStream::Trades,
            &[
                raw_trade(11, "85000.10", "0.010", m + 1, false),
                raw_trade(12, "85000.20", "0.012", m + 2, true),
                raw_trade(13, "85000.20", "0.008", m + 2, true),
                raw_trade(14, price_14, "0.005", m + 9, true),
            ],
        );
        seal_rows(
            &mut source,
            ArchiveStream::Trades,
            &[raw_trade(
                15,
                "85000.30",
                "0.030",
                (d + 1) * DAY_MS + 5,
                false,
            )],
        );
        source
    }

    /// R1: a lost trade after the day's last aggregate, in the same day's
    /// `trades` file, is that aggregate's tail: folded, or gapped before
    /// the next day's first aggregate; never ignored with the day `Clean`.
    #[test]
    fn a_tail_trade_after_the_last_aggregate_of_a_day_is_folded_or_gapped() {
        let d = day("2026-05-15");
        let m = m(d);
        let w = window(d * DAY_MS, (d + 2) * DAY_MS);
        let source = end_of_day_source(d, "85000.20");
        let none = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::None,
        );
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::Fold,
        );
        assert_eq!(qty_by_id(&fold.events)[&2], qty("0.025"));
        assert_eq!(fold.events.len(), none.events.len());
        assert!(gaps(&fold.events).is_empty());
        let report = fold.report.unwrap();
        assert_eq!(report.days[&d], corrected(1, "0.005", 0, 1, 0));
        assert_eq!(report.days[&(d + 1)], DayStatus::Clean);

        // At another price: a gap from aggregate 2 to the next day's first.
        let source = end_of_day_source(d, "85000.25");
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::Fold,
        );
        assert_eq!(
            gaps(&fold.events),
            [(
                Stream::Trades,
                m + 2,
                (d + 1) * DAY_MS + 5,
                GapReason::MissingData
            )]
        );
        let report = fold.report.unwrap();
        assert_eq!(report.days[&d], corrected(0, "0", 1, 0, 0));

        // A window ending with the day: the gap trails the window.
        let w = window(d * DAY_MS, (d + 1) * DAY_MS);
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            w,
            TradeCorrection::Fold,
        );
        assert!(gaps(&fold.events).is_empty());
        assert_eq!(
            fold.trailing,
            [FeedGap {
                stream: Stream::Trades,
                start: EventTime::from_millis(m + 2),
                end: EventTime::from_millis((d + 1) * DAY_MS),
                reason: GapReason::MissingData,
            }]
        );
    }

    /// Day `d` ends with aggregate 2 over trades 12 ..= 13 at `late`, whose
    /// trade 13 is timed on `d + 1` (the ADR-047 706/707 shape); trade 14,
    /// lost from it, is timed on `d + 1` too. `d` has `trades` iff
    /// `d_trades`.
    fn cross_midnight_source(d: i64, d_trades: bool) -> MemorySource {
        let late = (d + 1) * DAY_MS - 10;
        let next = (d + 1) * DAY_MS;
        let mut source = MemorySource::default();
        seal_rows(
            &mut source,
            ArchiveStream::AggTrades,
            &[
                agg(1, "85000.10", "0.010", (11, 11), late - 5, false),
                agg(2, "85000.20", "0.020", (12, 13), late, true),
            ],
        );
        seal_rows(
            &mut source,
            ArchiveStream::AggTrades,
            &[
                agg(3, "85000.30", "0.030", (15, 15), next + 20, false),
                agg(4, "85000.40", "0.040", (16, 16), next + 30, false),
            ],
        );
        if d_trades {
            seal_rows(
                &mut source,
                ArchiveStream::Trades,
                &[
                    raw_trade(11, "85000.10", "0.010", late - 5, false),
                    raw_trade(12, "85000.20", "0.019999", late, true),
                ],
            );
        }
        seal_rows(
            &mut source,
            ArchiveStream::Trades,
            &[
                raw_trade(13, "85000.20", "0.000001", next + 12, true),
                raw_trade(14, "85000.20", "0.005", next + 15, true),
                raw_trade(15, "85000.30", "0.030", next + 20, false),
                raw_trade(16, "85000.40", "0.040", next + 30, false),
            ],
        );
        source
    }

    /// R2: a lost trade of the day's last aggregate filed in the next
    /// day's `trades` folds into it when the replay has that day, and is an
    /// unresolved gap when it does not.
    #[test]
    fn a_lost_trade_across_midnight_resolves_with_the_previous_day() {
        let d = day("2026-05-15");
        let next = (d + 1) * DAY_MS;
        let both = window(d * DAY_MS, (d + 2) * DAY_MS);
        let source = cross_midnight_source(d, true);
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            both,
            TradeCorrection::Fold,
        );
        assert_eq!(qty_by_id(&fold.events)[&2], qty("0.025"));
        assert!(gaps(&fold.events).is_empty());
        let report = fold.report.unwrap();
        assert_eq!(report.days[&d], DayStatus::Clean);
        assert_eq!(report.days[&(d + 1)], corrected(1, "0.005", 0, 1, 0));
        assert_eq!(
            report.minutes.keys().copied().collect::<Vec<_>>(),
            [next - 60_000]
        );

        // The previous day without `trades` files: its last range still
        // resolves the next day's head.
        let source = cross_midnight_source(d, false);
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            both,
            TradeCorrection::Fold,
        );
        assert_eq!(qty_by_id(&fold.events)[&2], qty("0.025"));
        let report = fold.report.unwrap();
        assert_eq!(report.days[&d], DayStatus::Unchecked);
        assert_eq!(report.days[&(d + 1)], corrected(1, "0.005", 0, 1, 0));

        // The previous day outside the replay: trades 13 and 14 cannot be
        // told covered from lost, so they are gapped and reported.
        let only = window(next, (d + 2) * DAY_MS);
        let none = open(
            &source,
            &[ArchiveStream::AggTrades],
            only,
            TradeCorrection::None,
        );
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            only,
            TradeCorrection::Fold,
        );
        assert_eq!(
            gaps(&fold.events),
            [(Stream::Trades, next, next + 20, GapReason::MissingData)]
        );
        assert_eq!(fold.events.len(), none.events.len() + 1);
        assert_eq!(
            fold.report.unwrap().days[&(d + 1)],
            corrected(0, "0", 0, 0, 2)
        );
    }

    /// R4: a millisecond window ending between `P` and `N` keeps the gap of
    /// an unattributed trade inside it, as a trailing gap.
    #[test]
    fn a_window_ending_between_the_aggregates_around_a_gap_trails_it() {
        let d = day("2026-05-15");
        let mut source = MemorySource::default();
        fold_day_source(&mut source, d, 1, true, "85000.25");
        let m = m(d);
        // Trade 14 is at m + 2, N (aggregate 3) at m + 3.
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            window(d * DAY_MS, m + 3),
            TradeCorrection::Fold,
        );
        assert!(gaps(&fold.events).is_empty());
        assert_eq!(
            fold.trailing,
            [FeedGap {
                stream: Stream::Trades,
                start: EventTime::from_millis(m + 2),
                end: EventTime::from_millis(m + 3),
                reason: GapReason::MissingData,
            }]
        );
        assert_eq!(fold.report.unwrap().days[&d], corrected(0, "0", 1, 0, 0));

        // A window that ends before the gap opens has none.
        let fold = open(
            &source,
            &[ArchiveStream::AggTrades],
            window(d * DAY_MS, m + 2),
            TradeCorrection::Fold,
        );
        assert!(fold.trailing.is_empty());
    }
}
