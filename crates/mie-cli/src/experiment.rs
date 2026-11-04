//! `mie experiment validate|run` (#29, ADR-040): experiment specs in,
//! immutable results with provenance out.
//!
//! - [`validate`] parses a spec file (spec text v1, see
//!   [`mie_domain::research`]) and prints its experiment id and canonical
//!   text, or every error, one per line. A spec that parses goes through a
//!   second stage, resolution under the backtest's rule registry v1
//!   (ADR-060 D2, D13): `backtest: resolves under rule registry v1`, or one
//!   `backtest: <error>` line per resolution error. The second stage is
//!   informational: the verdict, and the exit code, stay the parse's, since
//!   a spec that parses is a valid input of the replay pipelines whether it
//!   resolves or not.
//! - [`run`] runs a spec through [`ExperimentService`] over a replay of the
//!   raw store and records the result in the append-only [`FsResultStore`].
//!   `live` and `archive` (as `mie replay`) run `research.replay_summary@1`;
//!   `history` runs `research.replay_summary@2` on the history chain of an
//!   ingest configuration with `[history]` (ADR-052 D10), whose origin and
//!   cutover come from the built chain. With [`ExperimentPipeline::Backtest`]
//!   it runs `research.backtest@1` on that chain instead, through
//!   [`BacktestService`] and the chain's [`ChainBacktest`] (ADR-060 D13,
//!   D14), and refuses any other source before the store is opened. The
//!   chain covers the funding of a sample that ends at or before its
//!   cutover, when its archive replays `fundingRate` (ADR-060 D7). It
//!   prints `recorded <key>` for a
//!   new result and `reproduced <key>` when the stored result was
//!   reproduced exactly; anything else is `FAIL: <reason>`. The composition
//!   root is the only place that hands the writable store to the pipeline
//!   (ADR-020). A data-version mismatch of an archive run that folds
//!   `aggTrades` (ADR-050) adds a hint: `--trade-correction none` opens the
//!   data versions recorded before ADR-050. The chain always folds, so it
//!   gets no hint.

use crate::journal::read_runs;
use crate::replay::{ReplaySource, archive_replay, history_chain};
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::{HistoryChainStream, LiveReplay};
use mie_adapter_fs::FsResultStore;
use mie_adapter_parquet::ParquetRawStore;
use mie_app::backtest::ChainBacktest;
use mie_app::{BacktestService, ChainBounds, ExperimentService, FundingCoverage};
use mie_domain::backtest::resolve;
use mie_domain::feature::catalog;
use mie_domain::research::{ExperimentSpec, SpecError};
use mie_ports::inbound::{ExperimentRun, ResearchError, RunResearchExperiment};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Which pipeline `mie experiment run` runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExperimentPipeline {
    /// `research.replay_summary@1`, or `@2` on the history chain.
    Summary,
    /// `research.backtest@1`, on the history chain only (ADR-060 D13).
    Backtest,
}

/// One `mie experiment run` invocation.
#[derive(Debug, Clone)]
pub struct ExperimentRequest {
    /// The spec file.
    pub spec: PathBuf,
    /// Where the sample is replayed from.
    pub source: ReplaySource,
    /// The result store's root directory.
    pub results: PathBuf,
    /// The pipeline.
    pub pipeline: ExperimentPipeline,
}

/// Validates the spec at `path` and writes the outcome to `out`. Returns
/// whether the spec is valid.
///
/// # Errors
///
/// A description when the file cannot be read or the output written.
pub fn validate(path: &Path, out: &mut dyn Write) -> Result<bool, String> {
    let mut report = Vec::new();
    let valid = match load(path)? {
        Ok(spec) => {
            line(&mut report, &format!("experiment {}", spec.id()));
            report.extend_from_slice(spec.canonical_text().as_bytes());
            match resolve(&spec) {
                Ok(_) => line(&mut report, "backtest: resolves under rule registry v1"),
                Err(errors) => {
                    for error in errors {
                        line(&mut report, &format!("backtest: {error}"));
                    }
                }
            }
            true
        }
        Err(errors) => {
            for error in errors {
                line(&mut report, &error.to_string());
            }
            false
        }
    };
    emit(out, &report)?;
    Ok(valid)
}

/// Runs the experiment of `request` and writes the outcome to `out`.
/// Returns whether a result was recorded or reproduced.
///
/// # Errors
///
/// A description when the spec file cannot be read or the output written.
/// Every run failure is reported in the output.
pub fn run(request: &ExperimentRequest, out: &mut dyn Write) -> Result<bool, String> {
    let mut report = Vec::new();
    let spec = match load(&request.spec)? {
        Ok(spec) => spec,
        Err(errors) => {
            for error in errors {
                line(&mut report, &error.to_string());
            }
            emit(out, &report)?;
            return Ok(false);
        }
    };
    let backtest = request.pipeline == ExperimentPipeline::Backtest;
    if backtest && !matches!(request.source, ReplaySource::History(_)) {
        line(
            &mut report,
            "FAIL: --pipeline backtest runs on --source history only (ADR-060 D13)",
        );
        emit(out, &report)?;
        return Ok(false);
    }
    let verdict = match FsResultStore::open(&request.results) {
        Ok(results) => match &request.source {
            ReplaySource::Live(config) => {
                match read_runs(&config.paths.journal, &config.instrument.source) {
                    Ok(runs) => {
                        let store = ParquetRawStore::new(&config.paths.raw_root);
                        let runs = runs.iter().map(|run| run.live_run()).collect();
                        let live = LiveReplay::new(
                            &store,
                            &config.instrument.source,
                            &config.instrument.symbol,
                            runs,
                        );
                        execute(ExperimentService::new(live, results), &spec, false)
                    }
                    Err(error) => Err(error),
                }
            }
            ReplaySource::Archive {
                config,
                streams,
                correction,
            } => {
                let store = ParquetRawStore::new(&config.paths.raw_root);
                let folding = *correction == TradeCorrection::Fold;
                archive_replay(&store, config, streams.as_deref(), *correction).and_then(|replay| {
                    execute(ExperimentService::new(replay, results), &spec, folding)
                })
            }
            ReplaySource::History(config) => {
                let store = ParquetRawStore::new(&config.paths.raw_root);
                history_chain(config, &store).and_then(|chain| {
                    // The bounds come from the built chain, so they cannot
                    // disagree with the provider.
                    let bounds = ChainBounds {
                        origin: chain.origin(),
                        cutover: chain.cutover(),
                    };
                    if !backtest {
                        return execute(
                            ExperimentService::on_chain(chain, results, bounds),
                            &spec,
                            false,
                        );
                    }
                    // Settlements come from the archive segment only: live
                    // delivers none (ADR-060 D7).
                    let funding = FundingCoverage {
                        until: chain
                            .archive()
                            .streams()
                            .contains(&ArchiveStream::FundingRate)
                            .then_some(chain.cutover()),
                    };
                    let engine =
                        ChainBacktest::new(chain, bounds, |stream: &HistoryChainStream<'_>| {
                            stream.trailing_gaps().to_vec()
                        });
                    execute(BacktestService::new(engine, results, funding), &spec, false)
                })
            }
        },
        Err(error) => Err(error.to_string()),
    };
    let pass = verdict.is_ok();
    match verdict {
        Ok(text) => line(&mut report, &text),
        Err(error) => line(&mut report, &format!("FAIL: {error}")),
    }
    emit(out, &report)?;
    Ok(pass)
}

/// Runs `spec` through `service`: the line to print, or why it failed.
/// `folding`: an archive replay that folds `aggTrades` (ADR-050), so a data
/// version from before the fold may be the cause of a mismatch.
fn execute(
    mut service: impl RunResearchExperiment,
    spec: &ExperimentSpec,
    folding: bool,
) -> Result<String, String> {
    let run = service.run(spec).map_err(|e| match e {
        ResearchError::DataVersionMismatch { .. } if folding => format!(
            "{e}\nhint: --trade-correction none reproduces data versions recorded before \
             ADR-050 (the aggTrades fold)"
        ),
        e => e.to_string(),
    })?;
    match run {
        ExperimentRun::Recorded(result) => Ok(format!("recorded {}", result.key())),
        ExperimentRun::Reproduced(result) => Ok(format!("reproduced {}", result.key())),
    }
}

/// Reads and parses the spec file.
fn load(path: &Path) -> Result<Result<ExperimentSpec, Vec<SpecError>>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read the spec {}: {e}", path.display()))?;
    Ok(ExperimentSpec::parse(&text, &catalog::registry()))
}

fn line(report: &mut Vec<u8>, text: &str) {
    report.extend_from_slice(text.as_bytes());
    report.push(b'\n');
}

fn emit(out: &mut dyn Write, report: &[u8]) -> Result<(), String> {
    out.write_all(report)
        .and_then(|()| out.flush())
        .map_err(|e| format!("write the report: {e}"))
}
