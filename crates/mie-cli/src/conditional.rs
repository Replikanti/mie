//! `mie conditional build` and `mie conditional verify`: the conditional
//! outcome dataset of #27 (ADR-057, proposed).
//!
//! `build` labels a replay through
//! [`label_window_started`](mie_app::conditional::label_window_started)
//! under [`Labeling::CURRENT`] and streams its records and labels into an
//! [`OutcomeWriter`] (ADR-057 D6): the archive or the live runs of a
//! configuration, one source per dataset. The history chain is refused: a
//! restart of `drive_chain` (ADR-052 D4) would need the sampler and the
//! labeler reset too, which ADR-054 *Out of scope* left to this issue and
//! ADR-057 D6 declines. The records window is `--from`/`--to`; the replay
//! window defaults to it, and `--replay-from`/`--replay-to` widen it for
//! warm-up and lookahead. No lookahead is built in: labels the data ends
//! before are censored `end_of_data` (ADR-055 D6).
//!
//! The writer cannot stop a labeling run, so a write error is kept, later
//! writes are skipped, and the build fails at the end without writing the
//! manifest: an unfinished directory is never a dataset (ADR-057 D5).
//!
//! The report prints the manifest, `records <n> labels <n>`, then `PASS`
//! or `FAIL: …`. `verify` re-checks a finished dataset and prints the same
//! manifest and verdict.
//!
//! `query` verifies a dataset the same way and then prints one of its
//! DuckDB views as TSV ([`OutcomeViews`], ADR-057 D7/D8); `views` prints
//! the view definitions for other DuckDB clients.

use crate::journal::read_runs;
use crate::replay::{ReplaySource, archive_replay};
use mie_adapter_binance::LiveReplay;
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_duckdb::{OutcomeViews, VIEWS_SQL};
use mie_adapter_parquet::ParquetRawStore;
use mie_adapter_parquet::outcome::{OutcomeDataset, OutcomeHeader, OutcomeManifest, OutcomeWriter};
use mie_app::conditional::{LabelReport, label_window_started};
use mie_domain::conditional::label::Labeling;
use mie_domain::event::FeedGap;
use mie_ports::outbound::{HistoricalDataProvider, ProviderError, ReplayWindow};
use std::cell::RefCell;
use std::io::Write;
use std::path::{Path, PathBuf};

/// The code version of this build (ADR-057 D4): `git:<40 hex>`, with
/// `-dirty`, or `unknown`, from `build.rs`.
pub const CODE_VERSION: &str = env!("MIE_CODE_VERSION");

/// One `mie conditional build` invocation.
#[derive(Debug, Clone)]
pub struct BuildRequest {
    /// The source: live or archive; history is refused.
    pub source: ReplaySource,
    /// The records window: the records taken and labeled.
    pub records: ReplayWindow,
    /// The replay window, containing `records`.
    pub replay: ReplayWindow,
    /// The dataset directory to create.
    pub out: PathBuf,
}

/// The sink the labeling run's closures share: the writer once the run
/// has started, and the first error.
struct Sink<'a> {
    out: &'a Path,
    source: String,
    request: &'a BuildRequest,
    writer: Option<OutcomeWriter>,
    error: Option<String>,
}

impl Sink<'_> {
    fn fail(&mut self, error: String) {
        self.error.get_or_insert(error);
    }
}

/// Runs the build and writes the report to `out`. Returns whether it
/// passed.
///
/// # Errors
///
/// A description when the report cannot be written. Every build failure is
/// reported in the output.
pub fn build(request: &BuildRequest, out: &mut dyn Write) -> Result<bool, String> {
    let mut report = Vec::new();
    let pass = match run_build(request) {
        Ok((manifest, records, labels)) => {
            report.extend(manifest.render().into_bytes());
            line(&mut report, &format!("records {records} labels {labels}"));
            line(&mut report, "PASS");
            true
        }
        Err(error) => {
            line(&mut report, &format!("FAIL: {error}"));
            false
        }
    };
    out.write_all(&report)
        .and_then(|()| out.flush())
        .map_err(|e| format!("write the report: {e}"))?;
    Ok(pass)
}

/// The manifest and the record and label counts of a finished build.
fn run_build(request: &BuildRequest) -> Result<(OutcomeManifest, u64, u64), String> {
    if request.out.exists() {
        return Err(format!(
            "{} exists; a dataset is never overwritten",
            request.out.display()
        ));
    }
    // The dataset directory is created only once the replay has opened;
    // an unusable parent would otherwise fail after the whole run.
    let parent = match request.out.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    if !parent.is_dir() {
        return Err(format!(
            "the parent of {} is not an existing directory",
            request.out.display()
        ));
    }
    let (records, replay) = (request.records, request.replay);
    if records.start >= records.end || replay.start > records.start || replay.end < records.end {
        return Err("the records window must be non-empty and inside the replay window".to_owned());
    }
    match &request.source {
        ReplaySource::Live(config) => {
            let source = &config.instrument.source;
            let symbol = &config.instrument.symbol;
            let runs = read_runs(&config.paths.journal, source)?;
            let store = ParquetRawStore::new(&config.paths.raw_root);
            let runs = runs.iter().map(|run| run.live_run()).collect();
            let live = LiveReplay::new(&store, source, symbol, runs);
            // The streams are the journaled runs' own; there is nothing to
            // correct (ADR-057 D3).
            let text = format!("live {source}/{symbol} streams journaled correction none");
            label_into(&live, request, text, |stream| {
                stream.trailing_gaps().to_vec()
            })
        }
        ReplaySource::Archive {
            config,
            streams,
            correction,
        } => {
            let store = ParquetRawStore::new(&config.paths.raw_root);
            let archive = archive_replay(&store, config, streams.as_deref(), *correction)?;
            let mut names: Vec<&str> = archive.streams().iter().map(|s| s.raw_name()).collect();
            names.sort_unstable();
            let text = format!(
                "archive {ARCHIVE_SOURCE}/{} streams {} correction {correction}",
                config.instrument.symbol,
                names.join(",")
            );
            label_into(&archive, request, text, |stream| {
                stream.trailing_gaps().to_vec()
            })
        }
        ReplaySource::History(_) => Err(
            "--source history is refused: a restart of the chain would need the sampler and \
             the labeler reset too (ADR-057 D6); build from archive or live"
                .to_owned(),
        ),
    }
}

/// Labels `history` over the request's windows into a new dataset.
fn label_into<H: HistoricalDataProvider>(
    history: &H,
    request: &BuildRequest,
    source: String,
    trailing_gaps: impl FnOnce(&H::Stream) -> Vec<FeedGap>,
) -> Result<(OutcomeManifest, u64, u64), String> {
    let sink = RefCell::new(Sink {
        out: &request.out,
        source,
        request,
        writer: None,
        error: None,
    });
    let ran: Result<(_, LabelReport), ProviderError> = label_window_started(
        history,
        request.replay,
        request.records,
        Labeling::CURRENT,
        trailing_gaps,
        |provenance| {
            let mut sink = sink.borrow_mut();
            let header = OutcomeHeader {
                source: sink.source.clone(),
                replay: sink.request.replay,
                records: sink.request.records,
                provenance: provenance.clone(),
                code: CODE_VERSION.to_owned(),
            };
            match OutcomeWriter::create(sink.out, header) {
                Ok(writer) => sink.writer = Some(writer),
                Err(error) => sink.fail(error.to_string()),
            }
        },
        |record| {
            let mut sink = sink.borrow_mut();
            if sink.error.is_none()
                && let Some(Err(error)) = sink.writer.as_mut().map(|w| w.record(&record))
            {
                sink.fail(error.to_string());
            }
        },
        |label| {
            let mut sink = sink.borrow_mut();
            if sink.error.is_none()
                && let Some(Err(error)) = sink.writer.as_mut().map(|w| w.label(&label))
            {
                sink.fail(error.to_string());
            }
        },
    );
    let sink = sink.into_inner();
    let (_, report) = ran.map_err(|error| error.to_string())?;
    if let Some(error) = sink.error {
        return Err(format!(
            "{error}; {} holds no dataset (no manifest)",
            request.out.display()
        ));
    }
    let writer = sink
        .writer
        .ok_or_else(|| "the labeling run never started".to_owned())?;
    let (records, labels) = (writer.records(), writer.labels());
    if records == 0 {
        return Err(format!(
            "no context record in the records window; {} holds no dataset (no manifest)",
            request.out.display()
        ));
    }
    let manifest = writer
        .finish(report.events, report.rejections)
        .map_err(|error| error.to_string())?;
    Ok((manifest, records, labels))
}

/// Verifies the dataset in `dir` and writes the report to `out`. Returns
/// whether it passed.
///
/// # Errors
///
/// A description when the report cannot be written.
pub fn verify(dir: &Path, out: &mut dyn Write) -> Result<bool, String> {
    let mut report = Vec::new();
    let pass = match OutcomeDataset::open(dir) {
        Ok(dataset) => {
            report.extend(dataset.manifest().render().into_bytes());
            line(&mut report, "PASS");
            true
        }
        Err(error) => {
            line(&mut report, &format!("FAIL: {error}"));
            false
        }
    };
    out.write_all(&report)
        .and_then(|()| out.flush())
        .map_err(|e| format!("write the report: {e}"))?;
    Ok(pass)
}

/// Verifies the dataset in `dir`, then writes `view` as TSV to `out`
/// (ADR-057 D7): a header line and one line per row, ordered by the
/// view's key columns. `n_min` is `outcome_summary`'s minimum clear-sample
/// count, required there and refused elsewhere. A failure prints
/// `FAIL: <reason>` instead, or after the rows written so far when the
/// query fails midway. Returns whether it passed.
///
/// # Errors
///
/// A description when the output cannot be written.
pub fn query(
    dir: &Path,
    view: &str,
    n_min: Option<u64>,
    out: &mut dyn Write,
) -> Result<bool, String> {
    // The views read the files by path; only a dataset whose manifest,
    // content hash and file hashes check out is handed to them.
    let ran = OutcomeDataset::open(dir)
        .map_err(|error| error.to_string())
        .and_then(|_| OutcomeViews::open(dir).map_err(|error| error.to_string()))
        .and_then(|views| {
            views
                .query_tsv(view, n_min, out)
                .map_err(|error| error.to_string())
        });
    match ran {
        Ok(_) => Ok(true),
        Err(error) => {
            let mut report = Vec::new();
            line(&mut report, &format!("FAIL: {error}"));
            out.write_all(&report)
                .and_then(|()| out.flush())
                .map_err(|e| format!("write the report: {e}"))?;
            Ok(false)
        }
    }
}

/// Writes the view definitions ([`VIEWS_SQL`]) to `out`.
///
/// # Errors
///
/// A description when the output cannot be written.
pub fn views(out: &mut dyn Write) -> Result<(), String> {
    out.write_all(VIEWS_SQL.as_bytes())
        .and_then(|()| out.flush())
        .map_err(|e| format!("write the views: {e}"))
}

fn line(report: &mut Vec<u8>, text: &str) {
    report.extend_from_slice(text.as_bytes());
    report.push(b'\n');
}
