//! The history chain (ADR-052 D2, D5): the canonical event sequence from a
//! fixed origin, composed of the two replays that already exist instead of
//! a third implementation of either.
//!
//! - **Segments.** The archive segment is [`ArchiveReplay`] over
//!   `[origin, cutover)`, with the configured streams that replay by default
//!   and the `aggTrades` fold (ADR-050). The live segment is [`LiveReplay`]
//!   restricted to the journaled runs that started at or after the cutover
//!   (the *chain runs*), read from `L0`, the first chain run's start minus
//!   [`RUN_MARGIN_MS`]: by ADR-039 D2 no record of a run lies further before
//!   its start, so the first run is read whole. Runs that started before the
//!   cutover are outside the chain; their records are skipped.
//! - **The cutover must not fall inside a run.** [`HistoryChain::new`]
//!   refuses, naming it, every run that started before the cutover and whose
//!   ADR-039 D2 end — its `run_end`, else its journaled successor's start,
//!   else open — lies after it. The rule reads the journal alone, so it never
//!   depends on the window.
//! - **The seam.** The archive segment is drained first; its last event and
//!   newest open-interest time then seed the live segment's run chaining
//!   (ADR-039 D3), so the first chain run continues after the archive
//!   through the same zero hold-back `HoldBack` that joins runs: an overlap
//!   becomes a `LateEvent` gap or re-timed open interest, never an engine
//!   `OutOfOrder`. The output is strictly increasing.
//! - **Window** (ADR-039 D4). Every request replays the chain from the
//!   origin and restricts it afterwards: an event is delivered when its
//!   ordering time lies in the window, and a gap counts at its `end`. A gap
//!   whose `end` is at or after the window's end is withheld and listed in
//!   [`trailing_gaps`](HistoryChainStream::trailing_gaps) when it starts
//!   before it. The archive segment always spans `[origin, cutover)`, so
//!   adjacent windows concatenate exactly, and the gaps one window withholds
//!   are exactly those the next delivers that start before it. A window that
//!   ends at or before `L0` has no live segment.
//! - **The archive segment's own trailing gaps** (missing days at its end,
//!   the fold's unchecked tail at the cutover) are reported by its stream,
//!   never delivered: the seam is where the live segment takes over.
//! - **Prefix.** [`HistoryChain::before_run`] is the chain through a run's
//!   predecessor, unwindowed at the end, for the warm start and its
//!   equivalence check: the named run and every later one are skipped, never
//!   recomputed.
//! - **Dataset version** (ADR-052 D5, [`chain_dataset`]): SHA-256 over a
//!   versioned text holding the origin, the cutover and both segments'
//!   versions, so an archive correction and a newly sealed run both change
//!   it, while a closed window keeps it as a later run goes on capturing
//!   (ADR-039 D2).
//!
//! Uncrossable holes (ADR-052 D4) are not detected here: the provider's
//! sequence is the same whatever the engine makes of it, and the effective
//! origin is decided where the domain rejects the event after the hole
//! (`mie_app::drive_chain`).

use crate::archive::catalog::{ArchiveStream, DAY_MS};
use crate::archive::fetch::hex;
use crate::archive::fold::TradeCorrection;
use crate::archive::replay::{ArchiveReplay, ArchiveReplayStream};
use crate::replay::{LiveReplay, LiveReplayStream, LiveRun, RUN_MARGIN_MS};
use mie_domain::event::{FeedGap, MarketEvent};
use mie_domain::time::EventTime;
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use mie_ports::raw::{DatasetVersion, RawRecordSource};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;

/// The encoding of the chain's dataset-version text ([`chain_dataset`]). A
/// change to the text is a new encoding (ADR-052 D5).
pub const CHAIN_DATASET_ENCODING: u32 = 1;

/// The dataset version of a history chain (ADR-052 D5): SHA-256 over
///
/// ```text
/// mie-history-chain-dataset 1
/// origin <ms>
/// cutover <ms>
/// archive <hex>
/// live <hex or ->
/// ```
///
/// each line ending in `\n`, from the two segments' versions; `-` when the
/// request has no live segment.
pub fn chain_dataset(
    origin: EventTime,
    cutover: EventTime,
    archive: &DatasetVersion,
    live: Option<&DatasetVersion>,
) -> DatasetVersion {
    let text = format!(
        "mie-history-chain-dataset {CHAIN_DATASET_ENCODING}\norigin {}\ncutover {}\narchive {}\n\
         live {}\n",
        origin.as_millis(),
        cutover.as_millis(),
        archive.as_str(),
        live.map_or("-", DatasetVersion::as_str)
    );
    DatasetVersion::from_hex(&hex(&Sha256::digest(text.as_bytes())))
        .expect("a SHA-256 digest is 64 lowercase hex digits")
}

/// [`HistoricalDataProvider`] over the history chain of one instrument
/// (module docs).
pub struct HistoryChain<'a> {
    source: &'a dyn RawRecordSource,
    symbol: String,
    live_source: String,
    archive: ArchiveReplay<'a>,
    /// Every journaled run of the symbol, in start order.
    runs: Vec<LiveRun>,
    /// Each run's ADR-039 D2 end, aligned with `runs`.
    ends: Vec<Option<i64>>,
    /// The index of the first run that started at or after the cutover.
    first_chained: usize,
    origin: EventTime,
    cutover: EventTime,
}

impl fmt::Debug for HistoryChain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryChain")
            .field("symbol", &self.symbol)
            .field("live_source", &self.live_source)
            .field("origin", &self.origin)
            .field("cutover", &self.cutover)
            .field("runs", &self.runs.len())
            .field("first_chained", &self.first_chained)
            .finish_non_exhaustive()
    }
}

impl<'a> HistoryChain<'a> {
    /// The chain of `symbol` from `origin`: the archive's `archive_streams`
    /// (the configured ones; those that replay by default are replayed)
    /// before `cutover`, then the journaled `runs` of the raw source
    /// `live_source` that started at or after it, all read from `source`.
    ///
    /// # Errors
    ///
    /// [`ProviderError::Contract`] when the origin is not before the
    /// cutover, either is not 00:00 UTC (ADR-052 D1), or a run that started
    /// before the cutover ends after it (module docs); the error names that
    /// run.
    pub fn new(
        source: &'a dyn RawRecordSource,
        symbol: &str,
        archive_streams: &[ArchiveStream],
        live_source: &str,
        runs: Vec<LiveRun>,
        origin: EventTime,
        cutover: EventTime,
    ) -> Result<Self, ProviderError> {
        let contract = |detail: String| Err(ProviderError::Contract(detail));
        for (name, time) in [("origin", origin), ("cutover", cutover)] {
            if time.as_millis().rem_euclid(DAY_MS) != 0 {
                return contract(format!(
                    "the history {name} {time} is not 00:00 UTC (ADR-052 D1)"
                ));
            }
        }
        if origin >= cutover {
            return contract(format!(
                "the history origin {origin} is not before the cutover {cutover}"
            ));
        }
        let live = LiveReplay::new(source, live_source, symbol, runs);
        let runs = live.runs().to_vec();
        let ends = live.run_ends();
        let cut = cutover.as_millis();
        let first_chained = runs.partition_point(|run| run.started_at_ms < cut);
        for (run, end) in runs[..first_chained].iter().zip(&ends) {
            let how = match (end, run.ended_at_ms) {
                (Some(end), _) if *end <= cut => continue,
                (Some(end), Some(_)) => format!("ended at {end} ms, after it"),
                (Some(end), None) => format!(
                    "has no run_end, so it ends at its journaled successor's start {end} ms \
                     (ADR-039 D2), after it"
                ),
                (None, _) => "has neither a run_end nor a journaled successor, so it may still \
                              be capturing across it"
                    .to_owned(),
            };
            return contract(format!(
                "run {} started at {} ms, before the history cutover {cut} ms, and {how}: the \
                 cutover must not fall inside a run (ADR-052 D2)",
                run.run_id, run.started_at_ms
            ));
        }
        Ok(Self {
            source,
            symbol: symbol.to_owned(),
            live_source: live_source.to_owned(),
            archive: ArchiveReplay::with_defaults(
                source,
                symbol,
                archive_streams,
                TradeCorrection::Fold,
            ),
            runs,
            ends,
            first_chained,
            origin,
            cutover,
        })
    }

    /// The chain's origin.
    pub fn origin(&self) -> EventTime {
        self.origin
    }

    /// The cutover between the archive and the live segment.
    pub fn cutover(&self) -> EventTime {
        self.cutover
    }

    /// The archive segment's replay: its streams and correction.
    pub fn archive(&self) -> &ArchiveReplay<'a> {
        &self.archive
    }

    /// The chain runs: the journaled runs that started at or after the
    /// cutover, in start order.
    pub fn chain_runs(&self) -> &[LiveRun] {
        &self.runs[self.first_chained..]
    }

    /// The number of journaled runs that started before the cutover: not
    /// part of the chain.
    pub fn runs_before_cutover(&self) -> usize {
        self.first_chained
    }

    /// `L0`, where the live segment starts: the first chain run's start
    /// minus [`RUN_MARGIN_MS`]; `None` without a chain run.
    pub fn live_start(&self) -> Option<EventTime> {
        self.chain_runs()
            .first()
            .map(|run| EventTime::from_millis(run.started_at_ms.saturating_sub(RUN_MARGIN_MS)))
    }

    /// The chain through the predecessor of run `run_id`, unwindowed at the
    /// end: the archive segment, then every chain run that started before
    /// `run_id`, over the window `[origin, end + RUN_MARGIN_MS)` with `end`
    /// the predecessor's ADR-039 D2 end, which by D2 holds every record of
    /// the predecessor. `run_id` and every later run are skipped, never
    /// recomputed. For the first chain run the prefix is the archive segment
    /// alone, over `[origin, cutover)`.
    ///
    /// # Errors
    ///
    /// [`ProviderError::Contract`] when `run_id` is not a journaled run or
    /// started before the cutover; as [`replay`](Self::replay) otherwise.
    pub fn before_run(
        &self,
        run_id: &str,
    ) -> Result<Replay<HistoryChainStream<'a>>, ProviderError> {
        let index = self
            .runs
            .iter()
            .position(|run| run.run_id == run_id)
            .ok_or_else(|| {
                ProviderError::Contract(format!(
                    "run {run_id} is not a journaled run of {}/{}",
                    self.live_source, self.symbol
                ))
            })?;
        if index < self.first_chained {
            return Err(ProviderError::Contract(format!(
                "run {run_id} started before the history cutover {}: it is not part of the \
                 chain",
                self.cutover
            )));
        }
        let Some(live_start) = self.live_start().filter(|_| index > self.first_chained) else {
            return self.open(self.archive_window(), None);
        };
        let predecessor = index - 1;
        let end = self.ends[predecessor]
            .unwrap_or(self.runs[index].started_at_ms)
            .saturating_add(RUN_MARGIN_MS);
        let end = EventTime::from_millis(end);
        let ids = self.runs[self.first_chained..index]
            .iter()
            .map(|run| run.run_id.clone())
            .collect();
        self.open(
            ReplayWindow {
                start: self.origin,
                end,
            },
            Some((
                ids,
                ReplayWindow {
                    start: live_start,
                    end,
                },
            )),
        )
    }

    fn archive_window(&self) -> ReplayWindow {
        ReplayWindow {
            start: self.origin,
            end: self.cutover,
        }
    }

    /// Opens both segments and the stream over `window`.
    fn open(
        &self,
        window: ReplayWindow,
        live: Option<(BTreeSet<String>, ReplayWindow)>,
    ) -> Result<Replay<HistoryChainStream<'a>>, ProviderError> {
        let archive_window = self.archive_window();
        let archive = self.archive.replay(archive_window)?;
        let live = match live {
            Some((ids, live_window)) => {
                let replay = LiveReplay::new(
                    self.source,
                    &self.live_source,
                    &self.symbol,
                    self.runs.clone(),
                )
                .chaining_only(ids)
                .replay(live_window)?;
                Some(Segment {
                    stream: replay.stream,
                    window: live_window,
                    dataset: replay.dataset,
                    delivered: 0,
                })
            }
            None => None,
        };
        let dataset = chain_dataset(
            self.origin,
            self.cutover,
            &archive.dataset,
            live.as_ref().map(|segment| &segment.dataset),
        );
        Ok(Replay {
            stream: HistoryChainStream {
                window,
                archive: Segment {
                    stream: archive.stream,
                    window: archive_window,
                    dataset: archive.dataset,
                    delivered: 0,
                },
                live,
                archive_done: false,
                live_done: false,
                last: None,
                newest_oi: None,
                trailing: Vec::new(),
                failed: None,
            },
            dataset,
        })
    }
}

impl<'a> HistoricalDataProvider for HistoryChain<'a> {
    type Stream = HistoryChainStream<'a>;

    /// The chain restricted to `window` (module docs): the archive segment
    /// over `[origin, cutover)`, then, when a chain run exists and the
    /// window ends after `L0`, the live segment over `[L0, window end)`.
    fn replay(&self, window: ReplayWindow) -> Result<Replay<Self::Stream>, ProviderError> {
        if window.start >= window.end {
            return Err(ProviderError::Contract(format!(
                "empty replay window [{}, {})",
                window.start, window.end
            )));
        }
        let live = self.live_start().filter(|&l0| window.end > l0).map(|l0| {
            let ids = self
                .chain_runs()
                .iter()
                .map(|run| run.run_id.clone())
                .collect();
            (
                ids,
                ReplayWindow {
                    start: l0,
                    end: window.end,
                },
            )
        });
        self.open(window, live)
    }
}

/// One segment of a [`HistoryChainStream`]: its stream, window, dataset
/// version and the events the chain delivered from it.
#[derive(Debug)]
pub struct Segment<S> {
    stream: S,
    window: ReplayWindow,
    dataset: DatasetVersion,
    delivered: u64,
}

impl<S> Segment<S> {
    /// The segment's stream, for its own report (runs, streams, fold).
    pub fn stream(&self) -> &S {
        &self.stream
    }

    /// The window the segment was replayed over.
    pub fn window(&self) -> ReplayWindow {
        self.window
    }

    /// The segment's dataset version.
    pub fn dataset(&self) -> &DatasetVersion {
        &self.dataset
    }

    /// The events of the segment the chain delivered in its window so far.
    pub fn delivered(&self) -> u64 {
        self.delivered
    }
}

/// The event stream of a [`HistoryChain`]: the archive segment, then the
/// live segment chained after it, restricted to the window (module docs).
pub struct HistoryChainStream<'a> {
    window: ReplayWindow,
    archive: Segment<ArchiveReplayStream<'a>>,
    live: Option<Segment<LiveReplayStream<'a>>>,
    archive_done: bool,
    live_done: bool,
    /// The archive segment's last event, delivered or not.
    last: Option<MarketEvent>,
    /// The archive segment's newest open-interest time.
    newest_oi: Option<EventTime>,
    trailing: Vec<FeedGap>,
    failed: Option<ProviderError>,
}

impl fmt::Debug for HistoryChainStream<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryChainStream")
            .field("window", &self.window)
            .field("archive_done", &self.archive_done)
            .field("live", &self.live.is_some())
            .field("live_done", &self.live_done)
            .finish_non_exhaustive()
    }
}

impl<'a> HistoryChainStream<'a> {
    /// The window the chain is restricted to.
    pub fn window(&self) -> ReplayWindow {
        self.window
    }

    /// The archive segment.
    pub fn archive(&self) -> &Segment<ArchiveReplayStream<'a>> {
        &self.archive
    }

    /// The live segment; `None` when the request has none (module docs).
    pub fn live(&self) -> Option<&Segment<LiveReplayStream<'a>>> {
        self.live.as_ref()
    }

    /// Gaps of the chain still open at the window's end, not delivered
    /// (module docs): those of the archive segment as they come, then the
    /// live segment's once it has ended. Complete once the stream has ended.
    /// The archive segment's own trailing gaps at the cutover are its
    /// stream's ([`ArchiveReplayStream::trailing_gaps`]), not the chain's.
    pub fn trailing_gaps(&self) -> &[FeedGap] {
        &self.trailing
    }

    /// Whether `event` is delivered in the window (ADR-039 D4); a gap still
    /// open at the window's end is withheld and listed when it starts before
    /// it.
    fn admit(&mut self, event: &MarketEvent) -> bool {
        if let MarketEvent::FeedGap(gap) = event
            && gap.end >= self.window.end
        {
            if gap.start < self.window.end {
                self.trailing.push(*gap);
            }
            return false;
        }
        self.window.contains(event.time())
    }

    fn advance(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        while !self.archive_done {
            let Some(event) = self.archive.stream.next_event()? else {
                self.archive_done = true;
                if let (Some(live), Some(last)) = (self.live.as_mut(), self.last.clone()) {
                    live.stream.continue_after(last, self.newest_oi);
                }
                break;
            };
            if let MarketEvent::OpenInterest(oi) = &event {
                self.newest_oi = Some(self.newest_oi.map_or(oi.time, |n| n.max(oi.time)));
            }
            self.last = Some(event.clone());
            if self.admit(&event) {
                self.archive.delivered += 1;
                return Ok(Some(event));
            }
        }
        if self.live_done {
            return Ok(None);
        }
        loop {
            let Some(live) = self.live.as_mut() else {
                self.live_done = true;
                return Ok(None);
            };
            let Some(event) = live.stream.next_event()? else {
                self.trailing.extend_from_slice(live.stream.trailing_gaps());
                self.live_done = true;
                return Ok(None);
            };
            if self.admit(&event) {
                if let Some(live) = self.live.as_mut() {
                    live.delivered += 1;
                }
                return Ok(Some(event));
            }
        }
    }
}

impl MarketDataProvider for HistoryChainStream<'_> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        if let Some(failed) = &self.failed {
            return Err(failed.clone());
        }
        self.advance()
            .inspect_err(|failed| self.failed = Some(failed.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::ARCHIVE_SOURCE;
    use crate::archive::catalog::parse_day;
    use crate::archive::normalize::record_time as archive_time;
    use crate::stream::BinanceStream;
    use crate::testing::{MemorySource, RunBuilder, SOURCE, agg, mark, oi};
    use mie_domain::event::{GapReason, Stream};
    use mie_domain::state::MarketStateEngine;
    use mie_ports::raw::RawRecord;

    const MIN: i64 = 60_000;
    const HOUR: i64 = 3_600_000;
    /// 2026-10-05, the cutover of the fixture.
    fn cutover() -> i64 {
        parse_day("2026-10-05").unwrap() * DAY_MS
    }
    /// Five archive days before it.
    fn origin() -> i64 {
        cutover() - 5 * DAY_MS
    }
    /// The archive's last trade.
    fn archive_last() -> i64 {
        cutover() - 200
    }

    const ARCHIVE_STREAMS: [ArchiveStream; 2] = [ArchiveStream::AggTrades, ArchiveStream::Metrics];

    fn time(millis: i64) -> EventTime {
        EventTime::from_millis(millis)
    }

    fn window(start: i64, end: i64) -> ReplayWindow {
        ReplayWindow {
            start: time(start),
            end: time(end),
        }
    }

    fn archive_record(stream: ArchiveStream, row: &str) -> RawRecord {
        RawRecord {
            event_time: archive_time(stream, row.as_bytes()).unwrap(),
            capture: None,
            payload: row.as_bytes().to_vec(),
        }
    }

    /// Hourly aggregate trades over the archive day starting at `day_ms`,
    /// ids from `*id`.
    fn archive_day(source: &mut MemorySource, day_ms: i64, id: &mut u64, last: Option<i64>) {
        let mut records = Vec::new();
        for h in 0..24 {
            let row = format!(
                "{id},85000.10,0.010,{id},{id},{},false",
                day_ms + h * HOUR + 7
            );
            records.push(archive_record(ArchiveStream::AggTrades, &row));
            *id += 1;
        }
        if let Some(t) = last {
            let row = format!("{id},85000.20,0.010,{id},{id},{t},true");
            records.push(archive_record(ArchiveStream::AggTrades, &row));
            *id += 1;
        }
        source.seal(ARCHIVE_SOURCE, "aggTrades", records);
    }

    fn metrics_row(civil: &str) -> String {
        format!(
            "{civil},BTCUSDT,93000.0000000000000000,7734131259.6348000000000000,1.45,1.93,1.38,0.41"
        )
    }

    /// The archive: aggTrades on the first day and the two days before the
    /// cutover (the two days between are missing), the last trade 200 ms
    /// before the cutover; metrics on the last day, whose 23:55 row is
    /// ordered at 00:00 of the cutover day (ADR-034 D3).
    fn seal_archive(source: &mut MemorySource) {
        let mut id = 1;
        archive_day(source, origin(), &mut id, None);
        archive_day(source, cutover() - 2 * DAY_MS, &mut id, None);
        archive_day(source, cutover() - DAY_MS, &mut id, Some(archive_last()));
        let rows: Vec<RawRecord> = [
            "2026-10-04 12:00:00",
            "2026-10-04 23:50:00",
            "2026-10-04 23:55:00",
        ]
        .iter()
        .map(|civil| archive_record(ArchiveStream::Metrics, &metrics_row(civil)))
        .collect();
        source.seal(ARCHIVE_SOURCE, "metrics", rows);
    }

    /// The runs of the seam fixture (module docs of the test):
    /// - R0 before the cutover, ended 60 s before it, with a mark price no
    ///   chain run reads;
    /// - R1 right after the cutover, without mark prices, whose first trade
    ///   is older than the archive's last one and whose open interest trails
    ///   its poll;
    /// - R2 two hours later, with a reconnect;
    /// - R3 a fast restart after R2, whose first mark price is older than
    ///   R2's last event.
    struct Fixture {
        builders: Vec<RunBuilder>,
        runs: Vec<LiveRun>,
    }

    impl Fixture {
        fn new() -> Self {
            let c = cutover();
            let mut r0 = RunBuilder::new("20261004T233000Z");
            for i in 0..20 {
                r0.push(
                    BinanceStream::AggTrade,
                    1,
                    agg(5_000 + i as u64, c - 29 * MIN + i * MIN),
                );
            }
            r0.push(BinanceStream::MarkPrice, 1, mark(c - 100_000));

            let mut r1 = RunBuilder::new("20261005T000010Z");
            r1.push(BinanceStream::AggTrade, 1, agg(10_000, c - 1_000));
            r1.push(BinanceStream::OpenInterest, 1, oi(c - 2_000));
            for i in 0..30 {
                r1.push(
                    BinanceStream::AggTrade,
                    1,
                    agg(10_001 + i as u64, c + 10_000 + i * 30_000),
                );
            }

            let s2 = c + 2 * HOUR;
            let mut r2 = RunBuilder::new("20261005T020000Z");
            r2.push(BinanceStream::MarkPrice, 1, mark(s2 + 1_000));
            for i in 0..20 {
                r2.push(
                    BinanceStream::AggTrade,
                    1,
                    agg(20_000 + i as u64, s2 + 5_000 + i * 10_000),
                );
            }
            r2.push(BinanceStream::OpenInterest, 1, oi(s2 + 4 * MIN));
            for i in 0..10 {
                r2.push(
                    BinanceStream::AggTrade,
                    2,
                    agg(20_020 + i as u64, s2 + 6 * MIN + i * 10_000),
                );
            }

            let s3 = s2 + 10 * MIN + 1_000;
            let mut r3 = RunBuilder::new("20261005T021001Z");
            r3.push(BinanceStream::MarkPrice, 1, mark(s2 + 7 * MIN));
            for i in 0..10 {
                r3.push(
                    BinanceStream::AggTrade,
                    1,
                    agg(30_000 + i as u64, s3 + 5_000 + i * 10_000),
                );
            }

            let clean = |b: &RunBuilder| Some(b.records.len() as u64);
            let run0 = r0.run(c - 30 * MIN, Some(c - 60_000), clean(&r0));
            let mut run1 = r1.run(c + 10_000, Some(c + 20 * MIN), clean(&r1));
            run1.streams = vec![BinanceStream::AggTrade, BinanceStream::OpenInterest];
            let run2 = r2.run(s2, Some(s2 + 10 * MIN), clean(&r2));
            let run3 = r3.run(s3, Some(s3 + 10 * MIN), clean(&r3));
            Self {
                builders: vec![r0, r1, r2, r3],
                runs: vec![run0, run1, run2, run3],
            }
        }

        fn source(&self) -> MemorySource {
            let mut source = MemorySource::default();
            seal_archive(&mut source);
            for b in &self.builders {
                b.seal(&mut source, &[]);
            }
            source
        }
    }

    fn chain<'a>(source: &'a MemorySource, runs: Vec<LiveRun>) -> HistoryChain<'a> {
        HistoryChain::new(
            source,
            "BTCUSDT",
            &ARCHIVE_STREAMS,
            SOURCE,
            runs,
            time(origin()),
            time(cutover()),
        )
        .unwrap()
    }

    fn drain(stream: &mut HistoryChainStream<'_>) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        while let Some(event) = stream.next_event().unwrap() {
            out.push(event);
        }
        out
    }

    /// The events and trailing gaps of `chain` over `w`.
    fn replay(chain: &HistoryChain<'_>, w: ReplayWindow) -> (Vec<MarketEvent>, Vec<FeedGap>) {
        let mut stream = chain.replay(w).unwrap().stream;
        let events = drain(&mut stream);
        (events, stream.trailing_gaps().to_vec())
    }

    /// Far beyond the last run.
    fn far() -> i64 {
        cutover() + DAY_MS
    }

    fn gap_of(event: &MarketEvent) -> Option<FeedGap> {
        match event {
            MarketEvent::FeedGap(gap) => Some(*gap),
            _ => None,
        }
    }

    #[test]
    fn the_live_runs_continue_after_the_archive_at_the_seam() {
        let fixture = Fixture::new();
        let source = fixture.source();
        let chain = chain(&source, fixture.runs.clone());
        assert_eq!(chain.runs_before_cutover(), 1);
        assert_eq!(chain.chain_runs().len(), 3);
        assert_eq!(
            chain.live_start(),
            Some(time(cutover() + 10_000 - RUN_MARGIN_MS))
        );

        let mut stream = chain.replay(window(origin(), far())).unwrap().stream;
        let events = drain(&mut stream);
        assert!(
            stream.trailing_gaps().is_empty(),
            "{:?}",
            stream.trailing_gaps()
        );
        assert!(
            events.windows(2).all(|w| w[0] < w[1]),
            "strictly increasing"
        );
        let mut engine = MarketStateEngine::new();
        for event in &events {
            engine.apply(event).unwrap_or_else(|e| panic!("{e}"));
        }

        // The archive segment is ArchiveReplay over [origin, cutover).
        let archive = ArchiveReplay::with_defaults(
            &source,
            "BTCUSDT",
            &ARCHIVE_STREAMS,
            TradeCorrection::Fold,
        );
        let mut alone = archive.replay(window(origin(), cutover())).unwrap().stream;
        let mut expected = Vec::new();
        while let Some(event) = alone.next_event().unwrap() {
            expected.push(event);
        }
        let n = stream.archive().delivered() as usize;
        assert_eq!(events[..n], expected[..]);
        assert_eq!(expected.last().unwrap().time(), time(archive_last()));
        // The 23:55 metrics row is ordered at the cutover: not in the
        // archive segment.
        assert!(expected.iter().all(|e| e.time() < time(cutover())));
        let live = &events[n..];
        assert_eq!(stream.live().unwrap().delivered() as usize, live.len());

        // The seam: R1's first trade became a late gap, its open interest
        // was re-timed above the archive's last trade.
        let seam = time(archive_last() + 1);
        assert!(
            live.iter()
                .any(|e| gap_of(e).is_some_and(|g| g.stream == Stream::Trades
                    && g.reason == GapReason::LateEvent
                    && g.start == time(cutover() - 1_000)
                    && g.end == seam)),
            "{live:?}"
        );
        assert!(
            live.iter()
                .any(|e| matches!(e, MarketEvent::OpenInterest(oi) if oi.time == seam)),
            "{live:?}"
        );
        // R0 is not part of the chain: no event, no run stats.
        assert!(
            !events.iter().any(
                |e| matches!(e, MarketEvent::Trade(t) if (5_000..6_000).contains(&t.trade_id))
            )
        );
        assert!(!events.iter().any(|e| e.time() == time(cutover() - 100_000)));
        let ran: Vec<&str> = stream
            .live()
            .unwrap()
            .stream()
            .runs()
            .iter()
            .map(|r| r.run_id.as_str())
            .collect();
        assert_eq!(
            ran,
            ["20261005T000010Z", "20261005T020000Z", "20261005T021001Z"]
        );
        // R3, the fast restart, is chained after R2: its first mark price is
        // a late gap.
        assert!(
            live.iter()
                .any(|e| gap_of(e).is_some_and(|g| g.stream == Stream::MarkPrice
                    && g.reason == GapReason::LateEvent
                    && g.start == time(cutover() + 2 * HOUR + 7 * MIN))),
            "{live:?}"
        );
        assert_eq!(
            live.iter()
                .filter(|e| matches!(e, MarketEvent::Trade(_)))
                .count(),
            30 + 30 + 10
        );
    }

    #[test]
    fn a_run_the_cutover_falls_inside_is_refused() {
        let source = MemorySource::default();
        let c = cutover();
        let open = |id: &str, started: i64, ended: Option<i64>| {
            RunBuilder::new(id).run(started, ended, ended.map(|_| 0))
        };
        let refused = |runs: Vec<LiveRun>, id: &str| {
            let err = HistoryChain::new(
                &source,
                "BTCUSDT",
                &ARCHIVE_STREAMS,
                SOURCE,
                runs,
                time(origin()),
                time(c),
            )
            .unwrap_err();
            assert!(
                matches!(&err, ProviderError::Contract(d) if d.contains(&format!("run {id} "))),
                "{err}"
            );
        };
        // A clean run across the cutover.
        refused(vec![open("A", c - HOUR, Some(c + HOUR))], "A");
        // A crashed run before it whose successor starts after it.
        refused(
            vec![
                open("A", c - 2 * HOUR, None),
                open("B", c + HOUR, Some(c + 2 * HOUR)),
            ],
            "A",
        );
        // An open run before it.
        refused(vec![open("A", c - HOUR, None)], "A");
        // A crashed run whose successor starts before the cutover, and a run
        // that ends exactly at it.
        let accepted = HistoryChain::new(
            &source,
            "BTCUSDT",
            &ARCHIVE_STREAMS,
            SOURCE,
            vec![
                open("A", c - 3 * HOUR, None),
                open("B", c - 2 * HOUR, Some(c)),
                open("C", c + HOUR, None),
            ],
            time(origin()),
            time(c),
        )
        .unwrap();
        assert_eq!(accepted.runs_before_cutover(), 2);
        assert_eq!(accepted.chain_runs()[0].run_id, "C");

        // The origin and the cutover are UTC days, the origin first.
        for (o, cut) in [
            (origin() + 1, c),
            (origin(), c + HOUR),
            (c, c),
            (c + DAY_MS, c),
        ] {
            let err = HistoryChain::new(
                &source,
                "BTCUSDT",
                &ARCHIVE_STREAMS,
                SOURCE,
                Vec::new(),
                time(o),
                time(cut),
            )
            .unwrap_err();
            assert!(matches!(err, ProviderError::Contract(_)), "{err}");
        }
    }

    #[test]
    fn adjacent_windows_concatenate_to_their_union() {
        let fixture = Fixture::new();
        let source = fixture.source();
        let chain = chain(&source, fixture.runs.clone());
        let c = cutover();
        let s2 = c + 2 * HOUR;
        let (union, none) = replay(&chain, window(origin(), far()));
        assert!(none.is_empty());
        for split in [
            // Mid-archive-day, an archive day boundary, inside the missing
            // days.
            origin() + 12 * HOUR + 30 * MIN,
            c - 2 * DAY_MS,
            c - 4 * DAY_MS + 6 * HOUR,
            // Just before the cutover, between the archive's last event and
            // the first run's first event, before R1's first trade.
            c - 1,
            archive_last() + 1,
            c + 5_000,
            // Inside R2's reconnect, and at R3's fast restart.
            s2 + 4 * MIN + 30_000,
            s2 + 10 * MIN + 1_000,
        ] {
            let (mut head, trailing) = replay(&chain, window(origin(), split));
            let (tail, _) = replay(&chain, window(split, far()));
            let mut resumed: Vec<FeedGap> = tail
                .iter()
                .filter_map(gap_of)
                .filter(|g| g.start < time(split))
                .collect();
            head.extend(tail);
            assert_eq!(head, union, "split at {split}");
            let mut trailing = trailing;
            trailing.sort();
            resumed.sort();
            assert_eq!(trailing, resumed, "split at {split}");
        }
        // The splits that cut a gap: the missing days, the seam, the
        // reconnect.
        for split in [
            c - 4 * DAY_MS + 6 * HOUR,
            archive_last() + 1,
            s2 + 4 * MIN + 30_000,
        ] {
            let (_, trailing) = replay(&chain, window(origin(), split));
            assert!(!trailing.is_empty(), "split at {split}");
        }
    }

    #[test]
    fn the_prefix_before_a_run_is_the_chain_through_its_predecessor() {
        let fixture = Fixture::new();
        let source = fixture.source();
        let chain = chain(&source, fixture.runs.clone());
        let (union, _) = replay(&chain, window(origin(), far()));
        let r3_start = fixture.runs[3].started_at_ms;

        let mut prefix = chain.before_run("20261005T021001Z").unwrap().stream;
        let events = drain(&mut prefix);
        // Everything up to R3's first chained event, R3's late mark gap.
        let n = union
            .iter()
            .position(|e| {
                gap_of(e).is_some_and(|g| {
                    g.stream == Stream::MarkPrice && g.reason == GapReason::LateEvent
                })
            })
            .unwrap();
        assert_eq!(events, union[..n]);
        assert!(prefix.trailing_gaps().is_empty());
        let live = prefix.live().unwrap();
        assert_eq!(
            live.window(),
            window(
                cutover() + 10_000 - RUN_MARGIN_MS,
                fixture.runs[2].ended_at_ms.unwrap() + RUN_MARGIN_MS
            )
        );
        assert!(
            live.window().end.as_millis() > r3_start,
            "R3's first records lie in it"
        );
        let ran: Vec<&str> = live
            .stream()
            .runs()
            .iter()
            .map(|r| r.run_id.as_str())
            .collect();
        assert_eq!(ran, ["20261005T000010Z", "20261005T020000Z"]);

        // Before the first chain run: the archive segment alone.
        let mut first = chain.before_run("20261005T000010Z").unwrap();
        let events = drain(&mut first.stream);
        assert!(first.stream.live().is_none());
        assert_eq!(events.len() as u64, first.stream.archive().delivered());
        assert_eq!(events, union[..events.len()]);
        assert_eq!(events.last().unwrap().time(), time(archive_last()));

        // An unknown run, a run before the cutover.
        for id in ["20261006T000000Z", "20261004T233000Z"] {
            assert!(
                matches!(chain.before_run(id), Err(ProviderError::Contract(_))),
                "{id}"
            );
        }
    }

    #[test]
    fn the_current_run_is_never_part_of_its_prefix() {
        let fixture = Fixture::new();
        let mut source = fixture.source();
        // R4 is still capturing: journaled without run_end, with sealed
        // records, the first of them inside R3's margin.
        let s4 = fixture.runs[3].ended_at_ms.unwrap() + 2_000;
        let mut r4 = RunBuilder::new("20261005T022003Z");
        for i in 0..10 {
            r4.push(
                BinanceStream::AggTrade,
                1,
                agg(40_000 + i as u64, s4 + i * 1_000),
            );
        }
        r4.seal(&mut source, &[]);
        let mut runs = fixture.runs.clone();
        runs.push(r4.run(s4, None, None));
        let chain = chain(&source, runs);

        let mut prefix = chain.before_run("20261005T022003Z").unwrap().stream;
        let events = drain(&mut prefix);
        let ran: Vec<&str> = prefix
            .live()
            .unwrap()
            .stream()
            .runs()
            .iter()
            .map(|r| r.run_id.as_str())
            .collect();
        assert_eq!(
            ran,
            ["20261005T000010Z", "20261005T020000Z", "20261005T021001Z"]
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, MarketEvent::Trade(t) if t.trade_id >= 40_000))
        );
        // The chain through R3 of the store before R4 started.
        let before = fixture.source();
        let (all, _) = replay(
            &self::chain(&before, fixture.runs.clone()),
            window(origin(), far()),
        );
        assert_eq!(events, all);
    }

    #[test]
    fn the_dataset_version_covers_both_segments() {
        let fixture = Fixture::new();
        let w = window(origin(), cutover() + HOUR);
        let version = |source: &MemorySource, runs: Vec<LiveRun>| {
            chain(source, runs).replay(w).unwrap().dataset
        };
        let source = fixture.source();
        let base = version(&source, fixture.runs.clone());
        let opened = chain(&source, fixture.runs.clone()).replay(w).unwrap();
        assert_eq!(
            opened.dataset,
            chain_dataset(
                time(origin()),
                time(cutover()),
                opened.stream.archive().dataset(),
                Some(opened.stream.live().unwrap().dataset()),
            )
        );

        // A `trades` file on a fold day (ADR-050).
        let mut corrected = fixture.source();
        corrected.seal(
            ARCHIVE_SOURCE,
            "trades",
            vec![archive_record(
                ArchiveStream::Trades,
                &format!("9,85000.10,0.001,0,{},false", cutover() - 2 * DAY_MS + 7),
            )],
        );
        assert_ne!(version(&corrected, fixture.runs.clone()), base);

        // Another file of a chain run.
        let mut resealed = fixture.source();
        let mut extra = RunBuilder::new("20261005T000010Z");
        extra.push(BinanceStream::OpenInterest, 1, oi(cutover() + 5 * MIN));
        resealed.seal(
            SOURCE,
            "openInterest",
            extra.of(BinanceStream::OpenInterest, &[]),
        );
        assert_ne!(version(&resealed, fixture.runs.clone()), base);

        // A run started 60 s after a closed window's end goes on capturing,
        // past the window's end plus the margin: the window keeps its
        // version.
        let closed = window(origin(), fixture.runs[3].ended_at_ms.unwrap() + 10 * MIN);
        let s5 = closed.end.as_millis() + 60_000;
        let mut runs = fixture.runs.clone();
        let mut later = RunBuilder::new("20261005T024100Z");
        later.push(BinanceStream::AggTrade, 1, agg(50_000, s5 + 1_000));
        runs.push(later.run(s5, None, None));
        let mut growing = fixture.source();
        later.seal(&mut growing, &[]);
        let version =
            |source: &MemorySource| chain(source, runs.clone()).replay(closed).unwrap().dataset;
        let kept = version(&growing);
        for hour in 1..4 {
            let mut more = RunBuilder::new("20261005T024100Z");
            more.push(
                BinanceStream::AggTrade,
                1,
                agg(50_000 + hour as u64, s5 + hour * HOUR),
            );
            growing.seal(SOURCE, "aggTrade", more.of(BinanceStream::AggTrade, &[]));
            assert_eq!(version(&growing), kept, "hour {hour}");
        }
    }

    #[test]
    fn the_dataset_version_encoding_is_pinned() {
        let o = time(parse_day("2025-10-01").unwrap() * DAY_MS);
        let c = time(parse_day("2026-10-05").unwrap() * DAY_MS);
        let archive = DatasetVersion::from_hex(&"ab".repeat(32)).unwrap();
        let live = DatasetVersion::from_hex(&"cd".repeat(32)).unwrap();
        assert_eq!(
            chain_dataset(o, c, &archive, Some(&live)).as_str(),
            "5bd74b5b36bd46c3cb61f2d99d56e2796973653c7876b5626f869803f88354b5"
        );
        assert_eq!(
            chain_dataset(o, c, &archive, None).as_str(),
            "97fadd11b3b4a9806b9e5e58a8bc185527ad68c8d1788898d9d88fa05682ccbc"
        );
        assert_eq!(CHAIN_DATASET_ENCODING, 1);
    }
}
