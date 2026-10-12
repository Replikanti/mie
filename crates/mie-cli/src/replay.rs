//! `mie replay`: replays a window of raw data through the core and prints a
//! deterministic report (#11, ADR-039).
//!
//! 1. Open the replay: [`LiveReplay`] over the journaled capture runs of the
//!    live source, [`ArchiveReplay`] over the archive backfill (one source
//!    per replay, ADR-039 D6), or [`HistoryChain`] over both, the archive up
//!    to the cutover and the live runs from it (ADR-052 D2, which supersedes
//!    ADR-039 D6 for the chain).
//! 2. Drive a fresh [`MarketStateEngine`] through
//!    [`mie_app::drive_tolerant`] — the drive of live ingestion — with the
//!    stream wrapped in a [`HashingProvider`]: a domain rejection is counted
//!    and driving resumes. The history chain is driven through
//!    [`mie_app::drive_chain`] instead: an uncrossable hole restarts a fresh
//!    engine, which is not a rejection (ADR-052 D4).
//! 3. Print the window, the source (with the archive's `aggTrades` fold
//!    correction, ADR-050), the dataset version, one line per run (live) or
//!    per stream (archive), the fold's provenance ([`fold_lines`]), the
//!    delivered events and gaps by stream and reason, trailing gaps,
//!    missing days, domain rejections, the final state and
//!    `event-stream <events>:<hex>`, then `PASS` or `FAIL: …`. The history
//!    chain prints one line per segment, each followed by that segment's
//!    lines as its own source prints them, and after the rejections one
//!    `restart at` line per restart and the `effective origin`.
//!
//! The report carries no wall-clock field, so replaying the same window of
//! the same store twice prints identical bytes. It passes when the replay
//! completed, delivered at least one event and the domain rejected none.

use crate::config::{ArchiveConfig, IngestConfig};
use crate::journal::read_runs;
use mie_adapter_binance::archive::catalog::DAY_MS;
use mie_adapter_binance::archive::catalog::day_label;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_binance::archive::replay::{ArchiveReplayStream, DayStatus, FoldReport};
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_binance::{HistoryChain, HistoryChainStream, LiveReplay, LiveReplayStream};
use mie_adapter_parquet::ParquetRawStore;
use mie_app::{HashingProvider, drive_chain, drive_tolerant};
use mie_domain::event::{FeedGap, GapReason, MarketEvent, Stream};
use mie_domain::event_hash::EventStreamHash;
use mie_domain::num::Qty;
use mie_domain::state::{MarketStateEngine, StateError};
use mie_domain::time::EventTime;
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use mie_ports::raw::DatasetVersion;
use std::collections::BTreeMap;
use std::io::Write;

/// Domain rejections printed as samples.
const MAX_REJECTION_SAMPLES: usize = 10;

/// The failure of `--source history` without a `[history]` section.
const NO_HISTORY: &str = "the ingest configuration has no [history] section (ADR-052 D6)";

/// Where a replay reads.
#[derive(Debug, Clone)]
pub enum ReplaySource {
    /// The live capture of an `mie ingest` configuration: its raw store and
    /// journal.
    Live(IngestConfig),
    /// The archive backfill of an `mie archive-import` configuration.
    Archive {
        /// The configuration.
        config: ArchiveConfig,
        /// Streams replacing the configured ones that replay by default
        /// (`--streams`).
        streams: Option<Vec<ArchiveStream>>,
        /// The `aggTrades` fold (`--trade-correction`, ADR-050).
        correction: TradeCorrection,
    },
    /// The history chain of an `mie ingest` configuration with `[history]`
    /// (ADR-052): its archive configuration's backfill up to the cutover,
    /// then its journaled runs from it.
    History(IngestConfig),
}

/// One `mie replay` invocation.
#[derive(Debug, Clone)]
pub struct ReplayRequest {
    /// The source.
    pub source: ReplaySource,
    /// The half-open event-time window.
    pub window: ReplayWindow,
}

/// What a replay produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayOutcome {
    /// Whether the replay passed (module docs).
    pub pass: bool,
    /// The dataset version, when the replay opened.
    pub dataset: Option<DatasetVersion>,
    /// The hash of the delivered events, when the replay opened.
    pub stream_hash: Option<EventStreamHash>,
    /// Events delivered, rejected ones included.
    pub events: u64,
    /// Delivered events the domain rejected.
    pub domain_rejections: u64,
    /// The provider failure, if any.
    pub error: Option<String>,
    /// The times (epoch ms) of the history chain's restarts after an
    /// uncrossable hole (ADR-052 D4); empty for `live` and `archive`.
    pub restarts: Vec<i64>,
}

impl ReplayOutcome {
    /// 0 when the replay passed, 1 otherwise.
    pub fn exit_code(&self) -> i32 {
        i32::from(!self.pass)
    }
}

/// Runs the replay and writes the report to `out`.
///
/// # Errors
///
/// A description when the report cannot be written. Every replay failure
/// (journal, store, contract) is reported in the output and the
/// [`ReplayOutcome`].
pub fn run(request: &ReplayRequest, out: &mut dyn Write) -> Result<ReplayOutcome, String> {
    let window = request.window;
    let mut report = Vec::new();
    let outcome = match &request.source {
        ReplaySource::Live(config) => {
            let source = &config.instrument.source;
            let symbol = &config.instrument.symbol;
            line(
                &mut report,
                format!("replay {} source live {source}/{symbol}", span(window)),
            );
            match read_runs(&config.paths.journal, source) {
                Ok(runs) => {
                    let store = ParquetRawStore::new(&config.paths.raw_root);
                    let runs = runs.iter().map(|run| run.live_run()).collect();
                    let live = LiveReplay::new(&store, source, symbol, runs);
                    drive(live.replay(window), &mut report, describe_live, false)
                }
                Err(error) => failed(&mut report, error),
            }
        }
        ReplaySource::Archive {
            config,
            streams,
            correction,
        } => {
            let symbol = &config.instrument.symbol;
            let store = ParquetRawStore::new(&config.paths.raw_root);
            match archive_replay(&store, config, streams.as_deref(), *correction) {
                Ok(replay) => {
                    let names: Vec<_> = replay.streams().iter().map(|s| s.raw_name()).collect();
                    line(
                        &mut report,
                        format!(
                            "replay {} source archive {ARCHIVE_SOURCE}/{symbol} streams {} \
                             correction {correction}",
                            span(window),
                            names.join(",")
                        ),
                    );
                    drive(replay.replay(window), &mut report, describe_archive, false)
                }
                Err(error) => failed(&mut report, error),
            }
        }
        ReplaySource::History(config) => history(config, window, &mut report),
    };
    out.write_all(&report)
        .and_then(|()| out.flush())
        .map_err(|e| format!("write the report: {e}"))?;
    Ok(outcome)
}

/// The `mie replay --source history` report of `config` over `window`.
fn history(config: &IngestConfig, window: ReplayWindow, report: &mut Vec<u8>) -> ReplayOutcome {
    let Some(section) = &config.history else {
        return failed(report, NO_HISTORY.to_owned());
    };
    line(
        report,
        format!(
            "replay {} source history origin {} cutover {}",
            span(window),
            section.origin,
            section.cutover
        ),
    );
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let chain = match history_chain(config, &store) {
        Ok(chain) => chain,
        Err(error) => return failed(report, error),
    };
    let source = &config.instrument.source;
    let symbol = &config.instrument.symbol;
    drive(
        chain.replay(window),
        report,
        |stream, report| describe_history(&chain, source, symbol, stream, report),
        true,
    )
}

/// The history chain of `config` over `store` (ADR-052 D2): the archive
/// configuration of its `[history]` section up to the cutover, then its
/// journaled runs from it. `mie replay --source history` and
/// `mie experiment run --source history` both build it here.
///
/// # Errors
///
/// A description when `config` has no `[history]` section, the archive
/// configuration or the journal cannot be read, or the chain refuses its
/// bounds or runs ([`HistoryChain::new`]).
pub(crate) fn history_chain<'a>(
    config: &IngestConfig,
    store: &'a ParquetRawStore,
) -> Result<HistoryChain<'a>, String> {
    let (history, archive) = match config.history_archive() {
        Ok(Some(loaded)) => loaded,
        Ok(None) => return Err(NO_HISTORY.to_owned()),
        Err(error) => return Err(error.to_string()),
    };
    let (origin, cutover, streams) =
        match (history.origin_ms(), history.cutover_ms(), archive.streams()) {
            (Ok(origin), Ok(cutover), Ok(streams)) => (origin, cutover, streams),
            (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => {
                return Err(error.to_string());
            }
        };
    let source = &config.instrument.source;
    let runs = read_runs(&config.paths.journal, source)?
        .iter()
        .map(|run| run.live_run())
        .collect();
    HistoryChain::new(
        store,
        &config.instrument.symbol,
        &streams,
        source,
        runs,
        EventTime::from_millis(origin),
        EventTime::from_millis(cutover),
    )
    .map_err(|error| error.to_string())
}

/// The archive replay of `config`: `streams` when given, else the
/// configured streams that replay by default.
///
/// # Errors
///
/// A description when the configured streams do not validate.
pub fn archive_replay<'a>(
    store: &'a ParquetRawStore,
    config: &ArchiveConfig,
    streams: Option<&[ArchiveStream]>,
    correction: TradeCorrection,
) -> Result<ArchiveReplay<'a>, String> {
    let symbol = &config.instrument.symbol;
    match streams {
        Some(streams) => Ok(ArchiveReplay::new(store, symbol, streams, correction)),
        None => config
            .streams()
            .map(|configured| ArchiveReplay::with_defaults(store, symbol, &configured, correction))
            .map_err(|e| e.to_string()),
    }
}

fn line(report: &mut Vec<u8>, text: String) {
    report.extend(text.into_bytes());
    report.push(b'\n');
}

fn span(window: ReplayWindow) -> String {
    format!(
        "[{}, {}) ms",
        window.start.as_millis(),
        window.end.as_millis()
    )
}

fn failed(report: &mut Vec<u8>, error: String) -> ReplayOutcome {
    line(report, format!("FAIL: {error}"));
    ReplayOutcome {
        pass: false,
        dataset: None,
        stream_hash: None,
        events: 0,
        domain_rejections: 0,
        error: Some(error),
        restarts: Vec::new(),
    }
}

/// Counts delivered events and gaps per domain stream.
struct Tally<P> {
    inner: P,
    streams: BTreeMap<Stream, (u64, BTreeMap<GapReason, u64>)>,
}

impl<P: MarketDataProvider> MarketDataProvider for Tally<P> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        let event = self.inner.next_event()?;
        if let Some(event) = &event {
            let (events, gaps) = self.streams.entry(event.stream()).or_default();
            match event {
                MarketEvent::FeedGap(gap) => *gaps.entry(gap.reason).or_default() += 1,
                _ => *events += 1,
            }
        }
        Ok(event)
    }
}

/// Drives an opened replay into a fresh engine and reports it; `chained`:
/// through [`drive_chain`], whose restarts are reported (ADR-052 D4), else
/// through [`drive_tolerant`].
fn drive<S: MarketDataProvider>(
    opened: Result<Replay<S>, ProviderError>,
    report: &mut Vec<u8>,
    describe: impl FnOnce(&S, &mut Vec<u8>),
    chained: bool,
) -> ReplayOutcome {
    let Replay { stream, dataset } = match opened {
        Ok(opened) => opened,
        Err(error) => return failed(report, error.to_string()),
    };
    line(report, format!("dataset {dataset}"));
    let mut hashing = HashingProvider::new(Tally {
        inner: stream,
        streams: BTreeMap::new(),
    });
    let mut engine = MarketStateEngine::new();
    let mut domain_rejections = 0_u64;
    let mut samples = Vec::new();
    let on_rejection = |rejected: &StateError| {
        domain_rejections += 1;
        if samples.len() < MAX_REJECTION_SAMPLES {
            samples.push(rejected.to_string());
        }
    };
    let driven = if chained {
        drive_chain(&mut hashing, &mut engine, on_rejection).map(|drive| drive.restarts)
    } else {
        drive_tolerant(&mut hashing, &mut engine, on_rejection).map(|_| Vec::new())
    };
    let stream_hash = hashing.hash();
    let tally = hashing.into_inner();
    describe(&tally.inner, report);
    for (stream, (events, gaps)) in &tally.streams {
        let gaps: Vec<String> = gaps
            .iter()
            .map(|(reason, n)| format!("{reason:?} {n}"))
            .collect();
        line(
            report,
            format!(
                "delivered {stream:?}: events {events}, gaps {}",
                if gaps.is_empty() {
                    "0".to_owned()
                } else {
                    gaps.join(", ")
                }
            ),
        );
    }
    line(report, format!("domain rejections: {domain_rejections}"));
    for sample in &samples {
        line(report, format!("  rejected: {sample}"));
    }
    let restarts: Vec<i64> = driven
        .as_ref()
        .map(|restarts| restarts.iter().map(|t| t.as_millis()).collect())
        .unwrap_or_default();
    if chained {
        for restart in &restarts {
            line(
                report,
                format!("restart at {restart} after an uncrossable hole (ADR-052 D4)"),
            );
        }
        line(
            report,
            format!(
                "effective origin {}",
                restarts
                    .last()
                    .map_or_else(|| "-".to_owned(), ToString::to_string)
            ),
        );
    }
    let state = engine.state();
    line(
        report,
        format!(
            "state: as of {}, trades {}, feature set {}",
            state
                .as_of
                .map_or_else(|| "-".to_owned(), |t| t.as_millis().to_string()),
            state.trade_count,
            state.feature_set
        ),
    );
    line(report, format!("event-stream {stream_hash}"));
    let error = driven.err().map(|e| e.to_string());
    let verdict = match (&error, domain_rejections, stream_hash.events) {
        (Some(error), _, _) => format!("FAIL: {error}"),
        (None, rejections, _) if rejections > 0 => {
            format!("FAIL: the domain rejected {rejections} event(s)")
        }
        (None, _, 0) => "FAIL: no event in the window".to_owned(),
        _ => "PASS".to_owned(),
    };
    let pass = verdict == "PASS";
    line(report, verdict);
    ReplayOutcome {
        pass,
        dataset: Some(dataset),
        stream_hash: Some(stream_hash),
        events: stream_hash.events,
        domain_rejections,
        error,
        restarts,
    }
}

fn trailing(report: &mut Vec<u8>, gaps: &[FeedGap]) {
    for gap in gaps {
        line(
            report,
            format!(
                "trailing gap {:?} [{}, {}] ms {:?} (open at the window end, not delivered)",
                gap.stream,
                gap.start.as_millis(),
                gap.end.as_millis(),
                gap.reason
            ),
        );
    }
}

fn describe_live(stream: &LiveReplayStream<'_>, report: &mut Vec<u8>) {
    describe_runs(stream, report);
    trailing(report, stream.trailing_gaps());
}

/// The run and unjournaled-run lines of a live replay.
fn describe_runs(stream: &LiveReplayStream<'_>, report: &mut Vec<u8>) {
    for run in stream.runs() {
        line(
            report,
            format!(
                "run {}: {}, records {}, replayed {}, ignored {}",
                run.run_id,
                if run.clean { "clean" } else { "not clean" },
                run.records,
                run.replayed,
                run.ignored
            ),
        );
    }
    for (run, count) in stream.unattributed_outside() {
        line(
            report,
            format!(
                "unjournaled run {run}: {count} record(s) outside the window, not replayed \
                 (no run_start in the journal)"
            ),
        );
    }
}

/// The segments of a history chain, each followed by its own source's
/// lines, then the chain's trailing gaps (ADR-052 D2). The archive
/// segment's own trailing gaps, open at the cutover and never delivered,
/// are among its lines.
fn describe_history(
    chain: &HistoryChain<'_>,
    source: &str,
    symbol: &str,
    stream: &HistoryChainStream<'_>,
    report: &mut Vec<u8>,
) {
    let archive = stream.archive();
    let names: Vec<_> = chain
        .archive()
        .streams()
        .iter()
        .map(|s| s.raw_name())
        .collect();
    line(
        report,
        format!(
            "segment archive {ARCHIVE_SOURCE}/{symbol} {}: dataset {}, streams {}, correction {}, \
             delivered {}",
            span(archive.window()),
            archive.dataset(),
            names.join(","),
            chain.archive().correction(),
            archive.delivered()
        ),
    );
    describe_archive(archive.stream(), report);
    match stream.live() {
        Some(live) => {
            line(
                report,
                format!(
                    "segment live {source}/{symbol} {}: dataset {}, runs chained {}, runs before \
                     the cutover {}, delivered {}",
                    span(live.window()),
                    live.dataset(),
                    live.stream().runs().len(),
                    chain.runs_before_cutover(),
                    live.delivered()
                ),
            );
            describe_runs(live.stream(), report);
        }
        None => line(report, "segment live: none".to_owned()),
    }
    trailing(report, stream.trailing_gaps());
}

fn describe_archive(stream: &ArchiveReplayStream<'_>, report: &mut Vec<u8>) {
    for (archive_stream, stats) in stream.stream_stats() {
        line(
            report,
            format!(
                "stream {archive_stream}: files {}, rows in window {}, missing days {}",
                stats.files,
                stats.events,
                stats.missing_days.len()
            ),
        );
        for &day in &stats.missing_days {
            line(report, format!("  missing day {}", day_label(day)));
        }
    }
    if let Some(fold) = stream.fold_report() {
        for text in fold_lines(fold, true) {
            line(report, text);
        }
    }
    trailing(report, stream.trailing_gaps());
}

/// Consecutive days as `first..last` ranges (a single day alone), comma
/// separated; `-` for none.
fn day_ranges(days: &[i64]) -> String {
    let mut ranges: Vec<(i64, i64)> = Vec::new();
    for &day in days {
        match ranges.last_mut() {
            Some((_, last)) if *last + 1 == day => *last = day,
            _ => ranges.push((day, day)),
        }
    }
    if ranges.is_empty() {
        return "-".to_owned();
    }
    ranges
        .iter()
        .map(|&(first, last)| {
            if first == last {
                day_label(first)
            } else {
                format!("{}..{}", day_label(first), day_label(last))
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// The provenance of a folding archive replay (ADR-050 D7), in a fixed
/// order and without a wall-clock field (ADR-039 D9):
///
/// - `fold summary: corrected <n> [<days>], clean <n> [<days>], unchecked
///   <n> [<days>]; aggregates <a>, trades folded <t>, qty <q>,
///   unattributed <u>, edge trades folded <e>, edge qty <eq>, unresolved
///   <r>` — the day statuses as day ranges and the totals of the corrected
///   days; the edge part of the folds (ADR-050 D3) is named apart, since
///   ADR-047 measured inner trades only, and unresolved trades are gapped,
///   not folded (D4);
/// - `fold unchecked tail <day>: aggregate <id>, …` when the window's last
///   aggregate is from a checked day and the store lacks the next day's
///   `trades` or `aggTrades` that would hold its tail (D4); its trailing
///   gap is printed with the others;
/// - with `detail`, one `fold <day>: …` line per corrected day (the whole
///   source day) and one `fold minute <open ms>: …` line per 1m minute
///   holding a folded aggregate delivered in the window. A bar of any
///   timeframe carries folded data iff it contains such a minute.
pub fn fold_lines(fold: &FoldReport, detail: bool) -> Vec<String> {
    let mut corrected = Vec::new();
    let mut clean = Vec::new();
    let mut unchecked = Vec::new();
    let (mut aggregates, mut trades, mut unattributed) = (0_u64, 0_u64, 0_u64);
    let (mut edge_trades, mut unresolved) = (0_u64, 0_u64);
    let mut qty = Some(Qty::from_units(0));
    let mut edge_qty = Some(Qty::from_units(0));
    let mut days = Vec::new();
    for (&day, status) in &fold.days {
        match *status {
            DayStatus::Corrected {
                aggregates: a,
                trades: t,
                qty: q,
                unattributed: u,
                edge_trades: e,
                edge_qty: eq,
                unresolved: r,
            } => {
                corrected.push(day);
                aggregates += a;
                trades += t;
                unattributed += u;
                edge_trades += e;
                unresolved += r;
                qty = qty.and_then(|sum| sum.checked_add(q));
                edge_qty = edge_qty.and_then(|sum| sum.checked_add(eq));
                days.push(format!(
                    "fold {}: aggregates {a}, trades {t}, qty {q}, unattributed {u}, edge \
                     trades {e}, edge qty {eq}, unresolved {r}",
                    day_label(day)
                ));
            }
            DayStatus::Clean => clean.push(day),
            DayStatus::Unchecked => unchecked.push(day),
        }
    }
    let total = |q: Option<Qty>| q.map_or_else(|| "overflow".to_owned(), |q| q.to_string());
    let (qty, edge_qty) = (total(qty), total(edge_qty));
    let mut lines = vec![format!(
        "fold summary: corrected {} [{}], clean {} [{}], unchecked {} [{}]; aggregates \
         {aggregates}, trades folded {trades}, qty {qty}, unattributed {unattributed}, edge \
         trades folded {edge_trades}, edge qty {edge_qty}, unresolved {unresolved}",
        corrected.len(),
        day_ranges(&corrected),
        clean.len(),
        day_ranges(&clean),
        unchecked.len(),
        day_ranges(&unchecked),
    )];
    if let Some(tail) = fold.unchecked_tail {
        lines.push(format!(
            "fold unchecked tail {}: aggregate {}, the store lacks the trades or aggTrades of {} \
             that would hold its tail",
            day_label(tail.day),
            tail.aggregate,
            day_label(tail.day + 1)
        ));
    }
    if detail {
        lines.extend(days);
        lines.extend(fold.minutes.iter().map(|(open, m)| {
            format!(
                "fold minute {open}: aggregates {}, trades {}, qty {}",
                m.aggregates, m.trades, m.qty
            )
        }));
    }
    lines
}

/// Parses a `--from` or `--to` bound: epoch milliseconds, or a UTC date
/// `YYYY-MM-DD` — the day's start for `--from`, the next day's start for
/// `--to` (the day is included, as in the archive commands).
///
/// # Errors
///
/// A description naming `flag` when `value` is neither.
pub fn parse_bound(flag: &str, value: &str, is_end: bool) -> Result<i64, String> {
    if let Some(day) = mie_adapter_binance::archive::catalog::parse_day(value) {
        return Ok((day + i64::from(is_end)) * DAY_MS);
    }
    value.parse().map_err(|_| {
        format!("{flag} {value:?} is neither epoch milliseconds nor a date YYYY-MM-DD")
    })
}
