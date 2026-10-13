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
//! - **Chain seeds** (ADR-052 D3). Every chain run is recomputed with the
//!   chain's newest event of each series as its seed, unless its journaled
//!   seed reaches back at least that far, so every hole in the chain opens
//!   with a `Disconnected` gap: between runs, and at the seam, where the
//!   archive segment's data of a series ends ([`seam_seeds`]). A series
//!   whose archive data ends with a trailing gap (missing days at its end,
//!   the fold's unchecked tail) ends where that gap starts, so the first
//!   run's seed gap covers the whole hole up to its first event of the
//!   series, and no bar across it reads as complete. A series the archive
//!   segment holds no event of (configured but never imported, say) gets
//!   no chain seed: the run keeps its journaled seed, and no seed is
//!   invented at the origin.
//! - **Window** (ADR-039 D4). Every request replays the chain from the
//!   origin and restricts it afterwards: an event is delivered when its
//!   ordering time lies in the window, and a gap counts at its `end`. A gap
//!   whose `end` is at or after the window's end is withheld and listed in
//!   [`trailing_gaps`](HistoryChainStream::trailing_gaps) when it starts
//!   before it. The archive segment always spans `[origin, cutover)`, so
//!   adjacent windows concatenate exactly, and the gaps one window withholds
//!   are those the next delivers that start before it, with one exception:
//!   a chain run whose extent starts at or after the window's end is not
//!   read, so its seed gaps, which start at the chain's last event before
//!   it, are delivered by the next window but not listed. Listing them would
//!   need that run's records, so a closed window's dataset version would
//!   move when the run starts. A window that ends at or before `L0` has no
//!   live segment.
//! - **The archive segment's own trailing gaps** (missing days at its end,
//!   the fold's unchecked tail at the cutover) are reported by its stream,
//!   not delivered as such: the first chain run's seed gaps carry them over
//!   the seam (chain seeds above). A series without a live stream
//!   (funding) has no seam to carry it over.
//! - **Prefix.** [`HistoryChain::before_run`] is the chain through a run's
//!   predecessor, unwindowed at the end, for the warm start: the named run
//!   and every later one are skipped, never recomputed. Once it has ended, its [`tail`](HistoryChainStream::tail)
//!   ([`ChainTail`]) is what the named run continues after: the seeds the
//!   chain recomputes it with ([`ChainTail::run_seeds`]) and the join to the
//!   prefix ([`ChainTail::continuation`]). The warm start (W3) pushes the
//!   live run's records through that continuation, so the live engine's
//!   input is the chain's recompute of the run by construction.
//! - **Through a run.** [`HistoryChain::to_end_of`] is the chain through a
//!   run, the prefix of its successor. The equivalence check of
//!   warm-started runs (ADR-052 D9) replays it once for all of them and
//!   splits it at run boundaries with
//!   [`run_of_last_event`](HistoryChainStream::run_of_last_event): the
//!   events before a run's first attributed event are the run's prefix, the
//!   run's own events are the chain's recompute of it.
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
use crate::replay::{LiveReplay, LiveReplayStream, LiveRun, RUN_MARGIN_MS, RunContinuation};
use crate::stream::BinanceStream;
use mie_domain::event::{FeedGap, MarketEvent, Stream};
use mie_domain::time::EventTime;
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use mie_ports::raw::{DatasetVersion, RawRecordSource};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
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

/// The chain seeds at the seam (ADR-052 D3), per domain series: where the
/// archive segment's data of the series ends. `newest` holds the archive
/// segment's newest event time per series, `trailing` its trailing gaps
/// ([`ArchiveReplayStream::trailing_gaps`]). A series with an event whose
/// data ends with a trailing gap (missing days at its end, the fold's
/// unchecked tail) ends where that gap starts, so the first chain run's
/// seed gap covers the whole hole up to its first event of the series. A
/// series without an event in the segment gets no seed: the chain holds no
/// earlier event of it, so the run keeps its journaled seed, and no seed is
/// invented at the origin.
///
/// The one computation of the seam seeds: the warm start (W3) journals the
/// same seeds for the first chain run ([`HistoryChainStream::tail`]).
pub(crate) fn seam_seeds(
    mut newest: BTreeMap<Stream, EventTime>,
    trailing: &[FeedGap],
) -> BTreeMap<Stream, EventTime> {
    for gap in trailing {
        if let Some(time) = newest.get_mut(&gap.stream) {
            *time = (*time).min(gap.start);
        }
    }
    newest
}

/// Where a chain prefix ends ([`HistoryChainStream::tail`]): what the next
/// chain run continues after (ADR-052 D2, D3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainTail {
    /// The prefix's last event, delivered or not; `None` for an empty
    /// prefix.
    pub last: Option<MarketEvent>,
    /// The prefix's newest open-interest time.
    pub newest_oi: Option<EventTime>,
    /// Per domain series, where the prefix's data of it ends: its newest
    /// event time, or at the seam where the archive's trailing gap of the
    /// series starts ([`seam_seeds`]).
    pub newest: BTreeMap<Stream, EventTime>,
}

impl ChainTail {
    /// The seeds the history chain recomputes the next run with, per
    /// captured stream: each non-book stream gets its domain series' chain
    /// seed, `depth` and `depthSnapshot` the order book's. A series the
    /// prefix holds no event of gets none: at the seam that is mark price,
    /// liquidations and the order book (ADR-052 D3).
    ///
    /// Journaled as the run's seeds, they are exactly the seeds the chain
    /// keeps: no journaled seed of a non-book stream is newer than the
    /// chain's, and none is missing where the chain has one, so the chain's
    /// per-stream rule changes none of them; the book keeps its journaled
    /// seed anyway.
    pub fn run_seeds(&self) -> BTreeMap<BinanceStream, EventTime> {
        BinanceStream::ALL
            .into_iter()
            .filter_map(|stream| {
                let seed = self.newest.get(&stream.domain_stream())?;
                Some((stream, *seed))
            })
            .collect()
    }

    /// The recompute of the next run with `seeds`, joined to the prefix
    /// through the zero hold-back join of ADR-039 D3, as the chain joins it
    /// (an empty prefix needs no join). `symbol`, `hold_back_ms` and
    /// `oi_retime_ms` are the run's.
    pub fn continuation(
        &self,
        symbol: &str,
        hold_back_ms: i64,
        oi_retime_ms: i64,
        seeds: &BTreeMap<BinanceStream, EventTime>,
    ) -> RunContinuation {
        RunContinuation::new(
            symbol,
            hold_back_ms,
            oi_retime_ms,
            seeds,
            self.last.clone().map(|last| (last, self.newest_oi)),
        )
    }
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
        let index = self.chain_index(run_id)?;
        if index == self.first_chained {
            return self.open(self.archive_window(), None);
        }
        let predecessor = index - 1;
        let end = self.ends[predecessor].unwrap_or(self.runs[index].started_at_ms);
        self.through(predecessor, end)
    }

    /// The chain through run `run_id`, unwindowed at the end: the archive
    /// segment, then every chain run up to and including `run_id`, over the
    /// window `[origin, end + RUN_MARGIN_MS)` with `end` the run's ADR-039
    /// D2 end (its `run_end`, else its journaled successor's start). When a
    /// successor exists, it is [`before_run`](Self::before_run) of that
    /// successor. Every later run is skipped, never recomputed. For one pass
    /// of the equivalence check over several warm-started runs (ADR-052
    /// D9), split at run boundaries by
    /// [`run_of_last_event`](HistoryChainStream::run_of_last_event).
    ///
    /// # Errors
    ///
    /// [`ProviderError::Contract`] when `run_id` is not a journaled run,
    /// started before the cutover, or has no end: neither a `run_end` nor a
    /// journaled successor, so it may still be capturing. As
    /// [`replay`](Self::replay) otherwise.
    pub fn to_end_of(&self, run_id: &str) -> Result<Replay<HistoryChainStream<'a>>, ProviderError> {
        let index = self.chain_index(run_id)?;
        let Some(end) = self.ends[index] else {
            return Err(ProviderError::Contract(format!(
                "run {run_id} has neither a run_end nor a journaled successor, so it may still \
                 be capturing: the chain through it has no end (ADR-039 D2)"
            )));
        };
        self.through(index, end)
    }

    /// The index in `runs` of the chain run `run_id`.
    fn chain_index(&self, run_id: &str) -> Result<usize, ProviderError> {
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
        Ok(index)
    }

    /// The archive segment, then the chain runs up to and including the one
    /// at `last` (a chain run), over `[origin, end + RUN_MARGIN_MS)` with
    /// `end` that run's ADR-039 D2 end: the one construction of
    /// [`before_run`](Self::before_run) and [`to_end_of`](Self::to_end_of).
    fn through(
        &self,
        last: usize,
        end: i64,
    ) -> Result<Replay<HistoryChainStream<'a>>, ProviderError> {
        let live_start = self
            .live_start()
            .expect("a chain run exists, so the live segment has a start");
        let end = EventTime::from_millis(end.saturating_add(RUN_MARGIN_MS));
        let ids = self.runs[self.first_chained..=last]
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
                from_live: false,
                last: None,
                newest_oi: None,
                newest: BTreeMap::new(),
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
    /// Whether the event delivered last came from the live segment.
    from_live: bool,
    /// The archive segment's last event, delivered or not.
    last: Option<MarketEvent>,
    /// The archive segment's newest open-interest time.
    newest_oi: Option<EventTime>,
    /// Per domain series, the archive segment's newest event time.
    newest: BTreeMap<Stream, EventTime>,
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

    /// The chain run whose recompute delivered the event that
    /// [`next_event`](MarketDataProvider::next_event) returned last: `None`
    /// for an archive event and before the first event
    /// ([`LiveReplayStream::run_of_last_event`]). Over
    /// [`HistoryChain::to_end_of`] it splits the stream at run boundaries:
    /// the events before a run's first attributed event are its prefix
    /// (ADR-052 D9).
    pub fn run_of_last_event(&self) -> Option<&str> {
        if !self.from_live {
            return None;
        }
        self.live.as_ref()?.stream.run_of_last_event()
    }

    /// The chain run whose read, check or recompute failed the stream
    /// ([`LiveReplayStream::failed_run`]): every event before that run's
    /// was delivered. `None` before a failure, for a failure of the archive
    /// segment and for the live segment's check after its last run.
    pub fn failed_run(&self) -> Option<&str> {
        self.live.as_ref()?.stream.failed_run()
    }

    /// Where the chain ends, once the stream has ended (`None` before): with
    /// a live segment, where its last chained run leaves the chained output;
    /// without one, the archive segment's last event and newest
    /// open-interest time, and [`seam_seeds`] of its newest events and
    /// trailing gaps, the seeds the first chain run gets at the seam. For
    /// [`HistoryChain::before_run`] it is what the named run continues
    /// after (module docs).
    pub fn tail(&self) -> Option<ChainTail> {
        if !self.live_done {
            return None;
        }
        Some(match &self.live {
            Some(live) => {
                let (last, newest_oi, newest) = live.stream.tail();
                ChainTail {
                    last: last.cloned(),
                    newest_oi,
                    newest: newest.clone(),
                }
            }
            None => ChainTail {
                last: self.last.clone(),
                newest_oi: self.newest_oi,
                newest: seam_seeds(self.newest.clone(), self.archive.stream.trailing_gaps()),
            },
        })
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

    /// Hands the archive segment over to the live segment (module docs):
    /// its last event and newest open-interest time continue the run
    /// chaining, and [`seam_seeds`] seed the first chain run.
    fn seam(&mut self) {
        let Some(live) = self.live.as_mut() else {
            return;
        };
        let newest = seam_seeds(
            std::mem::take(&mut self.newest),
            self.archive.stream.trailing_gaps(),
        );
        live.stream
            .continue_after(self.last.clone(), self.newest_oi, newest);
    }

    fn advance(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        while !self.archive_done {
            let Some(event) = self.archive.stream.next_event()? else {
                self.archive_done = true;
                self.seam();
                break;
            };
            if let MarketEvent::OpenInterest(oi) = &event {
                self.newest_oi = Some(self.newest_oi.map_or(oi.time, |n| n.max(oi.time)));
            }
            if !matches!(event, MarketEvent::FeedGap(_)) {
                self.newest.insert(event.stream(), event.time());
            }
            self.last = Some(event.clone());
            if self.admit(&event) {
                self.archive.delivered += 1;
                self.from_live = false;
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
                self.from_live = true;
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
    use crate::replay::with_chain_seeds;
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
            // Before L0, after the archive's newest open interest; inside the
            // R1 → R2 hole, beyond R2's margin.
            c - 295_000,
            c + HOUR,
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
            // The head lists what the tail delivers from before the split,
            // except (ADR-052 D2) the seed gaps of a chain run whose extent
            // starts after the split, which the head does not read.
            let unread = unread_from(&fixture.runs, split);
            let mut trailing = trailing;
            trailing.sort();
            resumed.sort();
            let (listed, missing): (Vec<FeedGap>, Vec<FeedGap>) =
                resumed.into_iter().partition(|g| trailing.contains(g));
            assert_eq!(trailing, listed, "split at {split}");
            assert!(
                missing
                    .iter()
                    .all(|g| g.reason == GapReason::Disconnected
                        && unread.is_some_and(|u| g.end >= u)),
                "split at {split}: {missing:?}"
            );
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

    /// Where the first chain run not read by a window ending at `split`
    /// starts its extent: the first whose start minus the margin is at or
    /// after `split`.
    fn unread_from(runs: &[LiveRun], split: i64) -> Option<EventTime> {
        runs.iter()
            .filter(|run| run.started_at_ms >= cutover())
            .map(|run| run.started_at_ms - RUN_MARGIN_MS)
            .find(|&start| start >= split)
            .map(time)
    }

    #[test]
    fn a_window_ending_in_a_hole_between_runs_does_not_list_the_next_seed_gap() {
        // Review repro 1 (PR #123): a split inside the R1 → R2 hole, more
        // than the margin before R2. R2's seed gaps reach back before the
        // split; the tail delivers them, the head cannot list them, because
        // it does not read R2 (ADR-052 D2): its version stays that of a store
        // without R2 (I6).
        let fixture = Fixture::new();
        let source = fixture.source();
        let chain = chain(&source, fixture.runs.clone());
        let split = cutover() + HOUR;
        let r1_last_trade = cutover() + 10_000 + 29 * 30_000;
        let (_, trailing) = replay(&chain, window(origin(), split));
        assert!(trailing.is_empty(), "{trailing:?}");
        let (tail, _) = replay(&chain, window(split, far()));
        let reaching_back: Vec<(Stream, i64, GapReason)> = tail
            .iter()
            .filter_map(gap_of)
            .filter(|g| g.start < time(split))
            .map(|g| (g.stream, g.start.as_millis(), g.reason))
            .collect();
        assert_eq!(
            reaching_back,
            [
                // From R1's last trade (ordered at R2's first trade), then
                // from its retimed open interest (at R2's first sample).
                (Stream::Trades, r1_last_trade, GapReason::Disconnected),
                (
                    Stream::OpenInterest,
                    archive_last() + 1,
                    GapReason::Disconnected
                ),
            ]
        );
        let mut without_later = MemorySource::default();
        seal_archive(&mut without_later);
        for b in &fixture.builders[..2] {
            b.seal(&mut without_later, &[]);
        }
        let head = window(origin(), split);
        assert_eq!(
            chain.replay(head).unwrap().dataset,
            self::chain(&without_later, fixture.runs[..2].to_vec())
                .replay(head)
                .unwrap()
                .dataset
        );
    }

    #[test]
    fn the_seam_marks_an_archive_that_ends_early() {
        // Review repro 2 (PR #123): the archive holds only its first day;
        // the four days before the cutover are missing, and the first chain
        // run is unseeded. Its first trade's seed gap covers the hole from
        // the archive's last trade.
        let mut source = MemorySource::default();
        let mut id = 1;
        archive_day(&mut source, origin(), &mut id, None);
        let archive_last_trade = origin() + 23 * HOUR + 7;
        let c = cutover();
        let mut r1 = RunBuilder::new("20261005T000010Z");
        for i in 0..10 {
            r1.push(
                BinanceStream::AggTrade,
                1,
                agg(10_000 + i as u64, c + 10_000 + i * 30_000),
            );
        }
        r1.seal(&mut source, &[]);
        let runs = vec![r1.run(c + 10_000, Some(c + 20 * MIN), Some(10))];
        let chain = chain(&source, runs.clone());
        let mut stream = chain.replay(window(origin(), far())).unwrap().stream;
        let events = drain(&mut stream);
        let archive_trailing: Vec<Stream> = stream
            .archive()
            .stream()
            .trailing_gaps()
            .iter()
            .map(|g| g.stream)
            .collect();
        assert_eq!(archive_trailing, [Stream::Trades, Stream::OpenInterest]);
        let trade_gaps: Vec<(i64, i64, GapReason)> = events
            .iter()
            .filter_map(gap_of)
            .filter(|g| g.stream == Stream::Trades)
            .map(|g| (g.start.as_millis(), g.end.as_millis(), g.reason))
            .collect();
        assert_eq!(
            trade_gaps,
            [(archive_last_trade, c + 10_000, GapReason::Disconnected)]
        );
        // Every bar across the hole is marked, none read as complete.
        let mut engine = MarketStateEngine::new();
        let mut closed = Vec::new();
        for event in &events {
            engine.apply(event).unwrap_or_else(|e| panic!("{e}"));
            closed.extend_from_slice(engine.closed_bars());
        }
        let across: Vec<_> = closed
            .iter()
            .filter(|bar| bar.open_time > time(archive_last_trade) && bar.end() <= time(c))
            .collect();
        assert!(across.len() > 4 * 1_440, "{}", across.len());
        assert!(across.iter().all(|bar| bar.coverage.feed_gap));

        // Partial loss: a run before the cutover captured after the
        // archive's last trade, and the first chain run's journaled seed is
        // that run's last trade. The chain does not hold it, so the seam gap
        // still starts at the archive's last trade.
        let mut r0 = RunBuilder::new("20261004T233000Z");
        r0.push(BinanceStream::AggTrade, 1, agg(5_000, c - 20 * MIN));
        r0.seal(&mut source, &[]);
        let mut seeded = runs[0].clone();
        seeded
            .seeds
            .insert(BinanceStream::AggTrade, time(c - 20 * MIN));
        let partial = self::chain(
            &source,
            vec![r0.run(c - 30 * MIN, Some(c - 10 * MIN), Some(1)), seeded],
        );
        let (events, _) = replay(&partial, window(origin(), far()));
        let trade_gaps: Vec<(i64, i64)> = events
            .iter()
            .filter_map(gap_of)
            .filter(|g| g.stream == Stream::Trades)
            .map(|g| (g.start.as_millis(), g.end.as_millis()))
            .collect();
        assert_eq!(trade_gaps, [(archive_last_trade, c + 10_000)]);
    }

    #[test]
    fn a_series_the_archive_never_held_keeps_the_journaled_seed() {
        // Review re-check (PR #123): metrics configured, none imported, and
        // the first chain run captures open interest. The chain holds no
        // open interest before the run, so no seed is invented at the origin
        // (ADR-052 D3): no year-long gap.
        let mut source = MemorySource::default();
        let mut id = 1;
        archive_day(
            &mut source,
            cutover() - DAY_MS,
            &mut id,
            Some(archive_last()),
        );
        let fixture = Fixture::new();
        fixture.builders[1].seal(&mut source, &[]);
        let chain = chain(&source, vec![fixture.runs[1].clone()]);
        let mut stream = chain.replay(window(origin(), far())).unwrap().stream;
        let events = drain(&mut stream);
        // The archive segment reports metrics missing from the origin.
        assert!(
            stream
                .archive()
                .stream()
                .trailing_gaps()
                .iter()
                .any(|g| g.stream == Stream::OpenInterest && g.start == time(origin()))
        );
        let oi_gaps: Vec<FeedGap> = events
            .iter()
            .filter_map(gap_of)
            .filter(|g| g.stream == Stream::OpenInterest)
            .collect();
        assert!(oi_gaps.is_empty(), "{oi_gaps:?}");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, MarketEvent::OpenInterest(_)))
        );
        // The trades seam is still marked from the archive's last trade.
        assert!(
            events
                .iter()
                .filter_map(gap_of)
                .any(|g| g.stream == Stream::Trades && g.reason == GapReason::Disconnected)
        );
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
        // Everything up to R3's first chained events, its mark-price gaps
        // (the seed gap and the late mark, both late after R2's last event).
        let n = union
            .iter()
            .position(|e| gap_of(e).is_some_and(|g| g.stream == Stream::MarkPrice))
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

    /// The events of `stream`, each with the chain run that delivered it.
    fn attributed(stream: &mut HistoryChainStream<'_>) -> Vec<(MarketEvent, Option<String>)> {
        let mut out = Vec::new();
        assert_eq!(stream.run_of_last_event(), None, "before the first event");
        while let Some(event) = stream.next_event().unwrap() {
            out.push((event, stream.run_of_last_event().map(str::to_owned)));
        }
        out
    }

    #[test]
    fn the_chain_through_a_run_is_the_prefix_of_its_successor() {
        let fixture = Fixture::new();
        let source = fixture.source();
        let chain = chain(&source, fixture.runs.clone());
        let (r1, r2, r3) = (
            fixture.runs[1].run_id.as_str(),
            fixture.runs[2].run_id.as_str(),
            fixture.runs[3].run_id.as_str(),
        );

        // Through R2: R3's prefix, event for event and in dataset version.
        let mut through = chain.to_end_of(r2).unwrap();
        let mut prefix = chain.before_run(r3).unwrap();
        assert_eq!(through.dataset, prefix.dataset);
        assert_eq!(drain(&mut through.stream), drain(&mut prefix.stream));
        assert_eq!(through.stream.window(), prefix.stream.window());

        // Through R3, the newest run: the chain up to its end plus the
        // margin.
        let end = fixture.runs[3].ended_at_ms.unwrap() + RUN_MARGIN_MS;
        let mut last = chain.to_end_of(r3).unwrap();
        let windowed = chain.replay(window(origin(), end)).unwrap();
        assert_eq!(last.dataset, windowed.dataset);
        let events = attributed(&mut last.stream);
        let (expected, _) = replay(&chain, window(origin(), end));
        assert_eq!(
            events.iter().map(|(e, _)| e.clone()).collect::<Vec<_>>(),
            expected
        );

        // The attribution: the archive's events, then R1's, R2's and R3's.
        let n = last.stream.archive().delivered() as usize;
        assert!(n > 0);
        assert!(events[..n].iter().all(|(_, run)| run.is_none()));
        let mut runs: Vec<&str> = events[n..]
            .iter()
            .map(|(_, run)| run.as_deref().expect("a live event is attributed"))
            .collect();
        runs.dedup();
        assert_eq!(runs, [r1, r2, r3]);
        // The events before each run's first one are its prefix.
        for run in [r1, r2, r3] {
            let first = events
                .iter()
                .position(|(_, by)| by.as_deref() == Some(run))
                .unwrap();
            let mut prefix = chain.before_run(run).unwrap().stream;
            let prefix = drain(&mut prefix);
            assert_eq!(
                events[..first]
                    .iter()
                    .map(|(e, _)| e.clone())
                    .collect::<Vec<_>>(),
                prefix,
                "{run}"
            );
        }
    }

    #[test]
    fn a_failed_run_is_named_after_every_event_before_it() {
        // R2 ended cleanly, but one of its records is missing.
        let fixture = Fixture::new();
        let mut source = MemorySource::default();
        seal_archive(&mut source);
        for (i, b) in fixture.builders.iter().enumerate() {
            b.seal(&mut source, if i == 2 { &[3] } else { &[] });
        }
        let chain = chain(&source, fixture.runs.clone());
        let mut stream = chain.to_end_of("20261005T021001Z").unwrap().stream;
        let mut delivered = Vec::new();
        let failed = loop {
            match stream.next_event() {
                Ok(Some(event)) => {
                    assert_eq!(stream.failed_run(), None);
                    delivered.push(event);
                }
                Ok(None) => panic!("the stream ended"),
                Err(failed) => break failed,
            }
        };
        assert!(
            matches!(&failed, ProviderError::Source(d) if d.contains("20261005T020000Z")),
            "{failed}"
        );
        assert_eq!(stream.failed_run(), Some("20261005T020000Z"));
        // R1 delivered all of its events before R2 failed: as many as the
        // prefix of R2 holds.
        assert_eq!(stream.run_of_last_event(), Some("20261005T000010Z"));
        let mut prefix = chain.before_run("20261005T020000Z").unwrap().stream;
        assert_eq!(delivered, drain(&mut prefix));
        // An archive-only prefix names no run.
        let mut archive = chain.before_run("20261005T000010Z").unwrap().stream;
        drain(&mut archive);
        assert_eq!(archive.failed_run(), None);
    }

    #[test]
    fn the_chain_through_an_open_or_unknown_run_is_refused() {
        let fixture = Fixture::new();
        let mut source = fixture.source();
        // R4 is still capturing: no run_end, no successor.
        let s4 = fixture.runs[3].ended_at_ms.unwrap() + 2_000;
        let mut r4 = RunBuilder::new("20261005T022003Z");
        r4.push(BinanceStream::AggTrade, 1, agg(40_000, s4 + 1_000));
        r4.seal(&mut source, &[]);
        let mut runs = fixture.runs.clone();
        runs.push(r4.run(s4, None, None));
        let chain = chain(&source, runs);
        for id in ["20261005T022003Z", "20261004T233000Z", "20261006T000000Z"] {
            assert!(
                matches!(chain.to_end_of(id), Err(ProviderError::Contract(_))),
                "{id}"
            );
        }
        // R3 ends at its run_end although a successor is capturing.
        assert!(chain.to_end_of("20261005T021001Z").is_ok());
    }

    /// `builder`'s records pushed through `continuation`, then finished.
    fn continue_with(continuation: &mut RunContinuation, builder: &RunBuilder) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        for (stream, record) in &builder.records {
            out.extend(continuation.push(*stream, record));
        }
        out.extend(continuation.finish());
        out
    }

    /// The run's seeds replaced by `tail`'s chain seeds, the continuation
    /// of the run after `tail`, and what it delivers over the run's records:
    /// what the warm start computes live.
    fn warm(tail: &ChainTail, run: &LiveRun, builder: &RunBuilder) -> (LiveRun, Vec<MarketEvent>) {
        let mut seeded = run.clone();
        seeded.seeds = tail.run_seeds();
        let mut continuation = tail.continuation(
            &run.symbol,
            run.hold_back_ms,
            run.oi_retime_ms,
            &seeded.seeds,
        );
        let out = continue_with(&mut continuation, builder);
        (seeded, out)
    }

    #[test]
    fn a_prefix_tail_continues_the_next_run_exactly_as_the_chain_does() {
        let fixture = Fixture::new();
        let source = fixture.source();
        let chain = chain(&source, fixture.runs.clone());
        let (union, _) = replay(&chain, window(origin(), far()));
        assert!(
            chain
                .replay(window(origin(), far()))
                .unwrap()
                .stream
                .tail()
                .is_none()
        );

        // The first chain run after the archive-only prefix.
        let mut prefix = chain.before_run("20261005T000010Z").unwrap().stream;
        assert!(prefix.tail().is_none(), "not before the stream has ended");
        let events = drain(&mut prefix);
        let tail = prefix.tail().unwrap();
        assert_eq!(tail.last.as_ref(), events.last());
        assert_eq!(tail.last.as_ref().unwrap().time(), time(archive_last()));
        // At the seam only the archive's series are seeded (ADR-052 D3).
        let seeds = tail.run_seeds();
        assert_eq!(
            seeds.keys().copied().collect::<Vec<_>>(),
            [BinanceStream::AggTrade, BinanceStream::OpenInterest]
        );
        assert_eq!(seeds[&BinanceStream::AggTrade], time(archive_last()));
        let (r1, out) = warm(&tail, &fixture.runs[1], &fixture.builders[1]);
        // R1's chained output in the full chain: up to R2's first event.
        let r2_first = union
            .iter()
            .position(|e| e.time() >= time(fixture.runs[2].started_at_ms))
            .unwrap();
        assert_eq!(out, union[events.len()..r2_first]);
        // Journaled, the chain seeds are what the chain recomputes R1 with.
        assert_eq!(with_chain_seeds(&r1, &tail.newest), r1);

        // R2 after the prefix through R1.
        let mut prefix = chain.before_run("20261005T020000Z").unwrap().stream;
        let events = drain(&mut prefix);
        let tail = prefix.tail().unwrap();
        let (r2, out) = warm(&tail, &fixture.runs[2], &fixture.builders[2]);
        assert_eq!(union[..events.len()], events[..]);
        assert_eq!(out, union[events.len()..events.len() + out.len()]);
        // R3, the fast restart, follows with its mark-price seed gap.
        assert!(
            gap_of(&union[events.len() + out.len()]).is_some_and(
                |g| g.stream == Stream::MarkPrice && g.reason == GapReason::Disconnected
            ),
            "{:?}",
            &union[events.len() + out.len()..]
        );
        assert_eq!(with_chain_seeds(&r2, &tail.newest), r2);
        // R1 captured no mark price, so R2's is unseeded; its trades are
        // seeded from R1's last trade.
        assert!(!r2.seeds.contains_key(&BinanceStream::MarkPrice));
        assert_eq!(
            r2.seeds[&BinanceStream::AggTrade],
            time(cutover() + 10_000 + 29 * 30_000)
        );
    }

    #[test]
    fn a_trailing_archive_gap_moves_the_tail_seed_to_its_start() {
        // As `the_seam_marks_an_archive_that_ends_early`: the archive holds
        // only its first day.
        let mut source = MemorySource::default();
        let mut id = 1;
        archive_day(&mut source, origin(), &mut id, None);
        let archive_last_trade = origin() + 23 * HOUR + 7;
        let c = cutover();
        let mut r1 = RunBuilder::new("20261005T000010Z");
        for i in 0..10 {
            r1.push(
                BinanceStream::AggTrade,
                1,
                agg(10_000 + i as u64, c + 10_000 + i * 30_000),
            );
        }
        r1.seal(&mut source, &[]);
        let run = r1.run(c + 10_000, Some(c + 20 * MIN), Some(10));
        let chain = chain(&source, vec![run.clone()]);
        let (union, _) = replay(&chain, window(origin(), far()));

        let mut prefix = chain.before_run(&run.run_id).unwrap().stream;
        let events = drain(&mut prefix);
        let trailing = prefix.archive().stream().trailing_gaps().to_vec();
        let trades_gap = trailing
            .iter()
            .find(|g| g.stream == Stream::Trades)
            .unwrap();
        let tail = prefix.tail().unwrap();
        assert_eq!(
            tail.run_seeds()[&BinanceStream::AggTrade],
            trades_gap.start.min(time(archive_last_trade))
        );
        assert_eq!(tail.newest[&Stream::Trades], time(archive_last_trade));
        let (seeded, out) = warm(&tail, &run, &r1);
        assert_eq!(out, union[events.len()..]);
        assert_eq!(with_chain_seeds(&seeded, &tail.newest), seeded);
        assert!(
            out.iter()
                .any(|e| gap_of(e).is_some_and(|g| g.stream == Stream::Trades
                    && g.reason == GapReason::Disconnected
                    && g.start == time(archive_last_trade)))
        );
    }

    #[test]
    fn the_book_takes_the_order_book_series_seed() {
        let tail = ChainTail {
            last: None,
            newest_oi: None,
            newest: BTreeMap::from([
                (Stream::OrderBook, time(5_000)),
                (Stream::MarkPrice, time(6_000)),
            ]),
        };
        assert_eq!(
            tail.run_seeds(),
            BTreeMap::from([
                (BinanceStream::MarkPrice, time(6_000)),
                (BinanceStream::Depth, time(5_000)),
                (BinanceStream::DepthSnapshot, time(5_000)),
            ])
        );
        // An empty prefix needs no join: the continuation is the pipeline.
        let mut b = RunBuilder::new("20261005T000010Z");
        b.push(BinanceStream::AggTrade, 1, agg(1, cutover() + 1_000));
        b.push(BinanceStream::AggTrade, 1, agg(2, cutover() + 2_000));
        let run = b.run(cutover(), None, None);
        let mut continuation =
            tail.continuation(&run.symbol, run.hold_back_ms, run.oi_retime_ms, &run.seeds);
        assert_eq!(continue_with(&mut continuation, &b), b.live(&run));
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

    /// The prefix before a second chain run that starts `gap_ms` after the
    /// first ended, with its first record 1 s after its start, twice: as
    /// the warm start (W3) journals it, while the
    /// second run is still capturing and has sealed nothing, and as an
    /// equivalence check sees it later, once the second run has sealed its
    /// files and ended. Each is the dataset version and the events.
    fn prefix_at_start_and_later(
        gap_ms: i64,
    ) -> (
        (DatasetVersion, Vec<MarketEvent>),
        (DatasetVersion, Vec<MarketEvent>),
    ) {
        let mut source = MemorySource::default();
        let mut id = 1;
        archive_day(&mut source, origin(), &mut id, None);
        let c = cutover();
        let mut r1 = RunBuilder::new("20261005T000010Z");
        for i in 0..10 {
            r1.push(
                BinanceStream::AggTrade,
                1,
                agg(10_000 + i as u64, c + 10_000 + i * 30_000),
            );
        }
        r1.seal(&mut source, &[]);
        let r1_end = c + 20 * MIN;
        let s2 = r1_end + gap_ms;
        let r1_run = r1.run(c + 10_000, Some(r1_end), Some(10));
        let id2 = if gap_ms < 5 * MIN {
            "20261005T002100Z"
        } else {
            "20261005T003000Z"
        };
        let mut r2 = RunBuilder::new(id2);
        for i in 0..10 {
            r2.push(
                BinanceStream::AggTrade,
                1,
                agg(20_000 + i as u64, s2 + 1_000 + i * 30_000),
            );
        }
        // At the warm start: the second run is journaled without an end and
        // has sealed nothing yet.
        let at_start = chain(&source, vec![r1_run.clone(), r2.run(s2, None, None)])
            .before_run(id2)
            .unwrap();
        let at_start = (at_start.dataset, drain(&mut { at_start.stream }));
        // Later: the second run sealed its files and ended cleanly.
        r2.seal(&mut source, &[]);
        let later = chain(
            &source,
            vec![r1_run, r2.run(s2, Some(s2 + 20 * MIN), Some(10))],
        )
        .before_run(id2)
        .unwrap();
        let later = (later.dataset, drain(&mut { later.stream }));
        (at_start, later)
    }

    #[test]
    fn a_run_sealing_records_inside_its_prefix_selection_moves_its_version_not_its_events() {
        // A restart 60 s after the predecessor's end, its first record 1 s
        // later, so before the predecessor's end plus the margin: once
        // sealed, that file falls into the prefix's selection (ADR-052
        // decisions 5 and 9). The start alone does not decide it; the
        // record's time does.
        let (at_start, later) = prefix_at_start_and_later(60_000);
        assert!(!at_start.1.is_empty());
        assert_eq!(at_start.1, later.1, "the prefix's events are the same");
        assert_ne!(at_start.0, later.0, "no data changed, the version did");
    }

    #[test]
    fn a_run_whose_records_lie_past_its_prefix_selection_keeps_its_version() {
        // A restart 10 min after the predecessor's end, its first record 1 s
        // later: past the predecessor's end plus the margin.
        let (at_start, later) = prefix_at_start_and_later(10 * MIN);
        assert_eq!(at_start.1, later.1);
        assert_eq!(at_start.0, later.0);
    }
}
