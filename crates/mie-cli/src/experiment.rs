//! `mie experiment validate|run` (#29, ADR-040): experiment specs in,
//! immutable results with provenance out.
//!
//! - [`validate`] parses a spec file (spec text v1, see
//!   [`mie_domain::research`]) and prints its experiment id and canonical
//!   text, or every error, one per line.
//! - [`run`] runs a spec through [`ExperimentService`] over a replay of the
//!   raw store (live or archive, as `mie replay`) and records the result in
//!   the append-only [`FsResultStore`]. It prints `recorded <key>` for a new
//!   result and `reproduced <key>` when the stored result was reproduced
//!   exactly; anything else is `FAIL: <reason>`. The composition root is the
//!   only place that hands the writable store to the pipeline (ADR-020).

use crate::journal::read_runs;
use crate::replay::ReplaySource;
use mie_adapter_binance::LiveReplay;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_fs::FsResultStore;
use mie_adapter_parquet::ParquetRawStore;
use mie_app::ExperimentService;
use mie_domain::feature::catalog;
use mie_domain::research::{ExperimentSpec, SpecError};
use mie_ports::inbound::{ExperimentRun, RunResearchExperiment};
use mie_ports::outbound::HistoricalDataProvider;
use std::io::Write;
use std::path::{Path, PathBuf};

/// One `mie experiment run` invocation.
#[derive(Debug, Clone)]
pub struct ExperimentRequest {
    /// The spec file.
    pub spec: PathBuf,
    /// Where the sample is replayed from.
    pub source: ReplaySource,
    /// The result store's root directory.
    pub results: PathBuf,
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
                        execute(live, results, &spec)
                    }
                    Err(error) => Err(error),
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
                replay.and_then(|replay| execute(replay, results, &spec))
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

/// Runs `spec` over `history` into `results`: the line to print, or why it
/// failed.
fn execute<H: HistoricalDataProvider>(
    history: H,
    results: FsResultStore,
    spec: &ExperimentSpec,
) -> Result<String, String> {
    let mut service = ExperimentService::new(history, results);
    match service.run(spec).map_err(|e| e.to_string())? {
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
