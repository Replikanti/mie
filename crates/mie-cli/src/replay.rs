//! `mie replay`: replays a window of raw data through the core and prints a
//! deterministic report (#11, ADR-039).
//!
//! 1. Open the replay: [`LiveReplay`] over the journaled capture runs of the
//!    live source, or [`ArchiveReplay`] over the archive backfill (one
//!    source per replay, ADR-039 D6).
//! 2. Drive a fresh [`MarketStateEngine`] through
//!    [`mie_app::drive_tolerant`] — the drive of live ingestion — with the
//!    stream wrapped in a [`HashingProvider`]: a domain rejection is counted
//!    and driving resumes.
//! 3. Print the window, the source, the dataset version, one line per run
//!    (live) or per stream (archive), the delivered events and gaps by
//!    stream and reason, trailing gaps, missing days, domain rejections, the
//!    final state and `event-stream <events>:<hex>`, then `PASS` or
//!    `FAIL: …`.
//!
//! The report carries no wall-clock field, so replaying the same window of
//! the same store twice prints identical bytes. It passes when the replay
//! completed, delivered at least one event and the domain rejected none.

use crate::config::{ArchiveConfig, IngestConfig};
use crate::journal::read_runs;
use mie_adapter_binance::archive::catalog::DAY_MS;
use mie_adapter_binance::archive::catalog::day_label;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_binance::archive::replay::ArchiveReplayStream;
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_binance::{LiveReplay, LiveReplayStream};
use mie_adapter_parquet::ParquetRawStore;
use mie_app::{HashingProvider, drive_tolerant};
use mie_domain::event::{FeedGap, GapReason, MarketEvent, Stream};
use mie_domain::event_hash::EventStreamHash;
use mie_domain::state::MarketStateEngine;
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use mie_ports::raw::DatasetVersion;
use std::collections::BTreeMap;
use std::io::Write;

/// Domain rejections printed as samples.
const MAX_REJECTION_SAMPLES: usize = 10;

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
    },
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
                    drive(live.replay(window), &mut report, describe_live)
                }
                Err(error) => failed(&mut report, error),
            }
        }
        ReplaySource::Archive { config, streams } => {
            let symbol = &config.instrument.symbol;
            let store = ParquetRawStore::new(&config.paths.raw_root);
            let replay = match streams {
                Some(streams) => Ok(ArchiveReplay::new(&store, symbol, streams)),
                None => config
                    .streams()
                    .map(|configured| ArchiveReplay::with_defaults(&store, symbol, &configured))
                    .map_err(|e| e.to_string()),
            };
            match replay {
                Ok(replay) => {
                    let names: Vec<_> = replay.streams().iter().map(|s| s.raw_name()).collect();
                    line(
                        &mut report,
                        format!(
                            "replay {} source archive {ARCHIVE_SOURCE}/{symbol} streams {}",
                            span(window),
                            names.join(",")
                        ),
                    );
                    drive(replay.replay(window), &mut report, describe_archive)
                }
                Err(error) => failed(&mut report, error),
            }
        }
    };
    out.write_all(&report)
        .and_then(|()| out.flush())
        .map_err(|e| format!("write the report: {e}"))?;
    Ok(outcome)
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

/// Drives an opened replay into a fresh engine and reports it.
fn drive<S: MarketDataProvider>(
    opened: Result<Replay<S>, ProviderError>,
    report: &mut Vec<u8>,
    describe: impl FnOnce(&S, &mut Vec<u8>),
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
    let driven = drive_tolerant(&mut hashing, &mut engine, |rejected| {
        domain_rejections += 1;
        if samples.len() < MAX_REJECTION_SAMPLES {
            samples.push(rejected.to_string());
        }
    });
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
    trailing(report, stream.trailing_gaps());
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
