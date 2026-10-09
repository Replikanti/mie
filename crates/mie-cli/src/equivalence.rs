//! `mie equivalence`: the live/replay equivalence harness (#13, ADR-019,
//! ADR-041).
//!
//! The unit of comparison is one cleanly ended live run: `mie ingest`
//! starts a fresh engine for every run, so only a recompute of that run
//! alone, into a fresh engine, can reach the states live reached.
//!
//! 1. Select the journal's runs of the configured source and symbol that
//!    **started** in `[from, to)` (wall clock of `run_start`).
//! 2. Per run: a run without a clean `run_end`, without state checkpoints
//!    (journaled before #13) or recorded with another event-stream or state
//!    hash encoding than this binary's is NOT COMPARABLE. Otherwise
//!    recompute it with [`LiveReplay::run_replay`] — no chaining, no window
//!    — and drive a fresh [`MarketStateEngine`] through
//!    [`drive_checkpointed`] at the run's journaled interval, the drive live
//!    ingestion used. [`compare`] the replay's checkpoints with the
//!    journaled ones, the state hashes only when the run's feature set is
//!    this binary's (ADR-041); the delivered events and domain rejections
//!    are also compared with `run_end`.
//! 3. Print a header (window, source, encodings, current feature set), one
//!    line per run with its verdict — `EQUIVALENT`, `DIVERGED`,
//!    `EVENTS EQUIVALENT, STATE NOT COMPARABLE` or `NOT COMPARABLE` — the
//!    first divergence of a diverged run, then `PASS` or `FAIL: …`.
//!
//! The report carries no wall-clock field, so the same request prints the
//! same bytes. It passes when at least one run was compared and every
//! selected run is `EQUIVALENT`.

use crate::config::IngestConfig;
use crate::journal::{JournaledRun, read_runs};
use mie_adapter_binance::LiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_app::equivalence::{
    Comparison, Divergence, DivergenceKind, StateCheckpoint, compare, drive_checkpointed,
};
use mie_domain::event_hash;
use mie_domain::feature::{FeatureSetVersion, catalog};
use mie_domain::state::MarketStateEngine;
use mie_domain::state_hash::STATE_HASH_ENCODING;
use std::io::Write;

/// One `mie equivalence` invocation.
#[derive(Debug, Clone)]
pub struct EquivalenceRequest {
    /// The `mie ingest` configuration whose raw store and journal are read.
    pub config: IngestConfig,
    /// Runs started at or after this wall-clock time (UTC ms) are selected.
    pub from_ms: i64,
    /// Runs started before this wall-clock time (UTC ms) are selected.
    pub to_ms: i64,
}

/// The verdict on one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Every checkpoint and the run's totals match.
    Equivalent,
    /// The first difference.
    Diverged(Mismatch),
    /// The events match; the run was recorded with another feature set, so
    /// its state hashes cannot be compared (ADR-041).
    EventsOnly {
        /// The run's feature set.
        recorded: FeatureSetVersion,
        /// This binary's feature set.
        current: FeatureSetVersion,
    },
    /// The run cannot be compared, and why.
    NotComparable(String),
}

/// How a diverged run differs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mismatch {
    /// The first divergent checkpoint.
    Checkpoint(Divergence),
    /// Every checkpoint matches, but a `run_end` total does not.
    Total {
        /// The total: `events` or `domain_rejections`.
        name: &'static str,
        /// What `run_end` journaled.
        live: u64,
        /// What the replay counted.
        replay: u64,
    },
}

/// The verdict on one selected run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunVerdict {
    /// The run.
    pub run_id: String,
    /// Its verdict.
    pub verdict: Verdict,
}

/// What `mie equivalence` concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EquivalenceOutcome {
    /// Whether the harness passed (module docs).
    pub pass: bool,
    /// The selected runs, in journal order.
    pub runs: Vec<RunVerdict>,
}

impl EquivalenceOutcome {
    /// 0 when the harness passed, 1 otherwise.
    pub fn exit_code(&self) -> i32 {
        i32::from(!self.pass)
    }
}

/// Runs the harness and writes the report to `out`.
///
/// # Errors
///
/// A description when the report cannot be written. Every other failure
/// (journal, store, a run's replay) is reported in the output and the
/// [`EquivalenceOutcome`].
pub fn run(
    request: &EquivalenceRequest,
    out: &mut dyn Write,
) -> Result<EquivalenceOutcome, String> {
    let config = &request.config;
    let source = &config.instrument.source;
    let symbol = &config.instrument.symbol;
    let current = catalog::current_set().version();
    let mut report = Vec::new();
    line(
        &mut report,
        format!(
            "equivalence runs started in [{}, {}) ms source live {source}/{symbol}",
            request.from_ms, request.to_ms
        ),
    );
    line(
        &mut report,
        format!(
            "encodings: event-stream v{}, state v{STATE_HASH_ENCODING}; feature set {current}",
            event_hash::ENCODING_VERSION
        ),
    );
    let outcome = match read_runs(&config.paths.journal, source) {
        Ok(runs) => {
            let store = ParquetRawStore::new(&config.paths.raw_root);
            let live = LiveReplay::new(
                &store,
                source,
                symbol,
                runs.iter().map(JournaledRun::live_run).collect(),
            );
            let mut verdicts = Vec::new();
            for run in runs.iter().filter(|run| {
                run.parameters.symbol == *symbol
                    && (request.from_ms..request.to_ms).contains(&run.started_at_ms)
            }) {
                let verdict = judge(&live, run, current, &mut report);
                verdicts.push(RunVerdict {
                    run_id: run.parameters.run_id.clone(),
                    verdict,
                });
            }
            let verdict = if verdicts.is_empty() {
                "FAIL: no run started in the window".to_owned()
            } else {
                let failed = verdicts
                    .iter()
                    .filter(|v| v.verdict != Verdict::Equivalent)
                    .count();
                if failed == 0 {
                    "PASS".to_owned()
                } else {
                    format!("FAIL: {failed} of {} run(s) not equivalent", verdicts.len())
                }
            };
            let pass = verdict == "PASS";
            line(&mut report, verdict);
            EquivalenceOutcome {
                pass,
                runs: verdicts,
            }
        }
        Err(error) => {
            line(&mut report, format!("FAIL: {error}"));
            EquivalenceOutcome {
                pass: false,
                runs: Vec::new(),
            }
        }
    };
    out.write_all(&report)
        .and_then(|()| out.flush())
        .map_err(|e| format!("write the report: {e}"))?;
    Ok(outcome)
}

/// Recomputes and compares one run, and reports it.
fn judge(
    live: &LiveReplay<'_>,
    run: &JournaledRun,
    current: FeatureSetVersion,
    report: &mut Vec<u8>,
) -> Verdict {
    let run_id = &run.parameters.run_id;
    let not_comparable = |report: &mut Vec<u8>, reason: String| {
        line(report, format!("run {run_id}: NOT COMPARABLE: {reason}"));
        Verdict::NotComparable(reason)
    };
    let Some(end) = run.end.filter(|end| end.exit_code == 0) else {
        return not_comparable(report, "the run did not end cleanly".to_owned());
    };
    let Some(checkpointing) = run.checkpointing else {
        return not_comparable(
            report,
            "the run journaled no state checkpoints (recorded before ADR-041)".to_owned(),
        );
    };
    if checkpointing.event_encoding != event_hash::ENCODING_VERSION
        || checkpointing.state_encoding != STATE_HASH_ENCODING
    {
        return not_comparable(
            report,
            format!(
                "recorded with event-stream v{} and state v{} encodings",
                checkpointing.event_encoding, checkpointing.state_encoding
            ),
        );
    }
    let mut replay = match live.run_replay(run_id) {
        Ok(replay) => replay,
        Err(error) => return not_comparable(report, format!("replay failed: {error}")),
    };
    let mut engine = MarketStateEngine::new();
    let mut rejections = 0_u64;
    let mut checkpoints: Vec<StateCheckpoint> = Vec::new();
    let driven = drive_checkpointed(
        &mut replay,
        &mut engine,
        checkpointing.interval_ms,
        |_| rejections += 1,
        |checkpoint| checkpoints.push(*checkpoint),
    );
    let events = match driven {
        Ok(events) => events,
        Err(error) => return not_comparable(report, format!("replay failed: {error}")),
    };
    let compare_state = checkpointing.feature_set == current;
    let comparison = compare(&run.checkpoints, &checkpoints, compare_state);
    let verdict = match comparison {
        Comparison::Diverged(divergence) => Verdict::Diverged(Mismatch::Checkpoint(divergence)),
        Comparison::Equivalent { .. } => {
            let totals = [
                ("events", end.events, events),
                ("domain_rejections", end.domain_rejections, rejections),
            ];
            match totals
                .into_iter()
                .find(|(_, live, replay)| live.is_some_and(|live| live != *replay))
            {
                Some((name, live, replay)) => Verdict::Diverged(Mismatch::Total {
                    name,
                    live: live.unwrap_or_default(),
                    replay,
                }),
                None if compare_state => Verdict::Equivalent,
                None => Verdict::EventsOnly {
                    recorded: checkpointing.feature_set,
                    current,
                },
            }
        }
    };
    let stats = replay.stats();
    line(
        report,
        format!(
            "run {run_id}: records {}, events {events}, rejections {rejections}, checkpoints \
             {}, dataset {}: {}",
            stats.records,
            run.checkpoints.len(),
            replay.dataset(),
            headline(&verdict)
        ),
    );
    if let Verdict::Diverged(mismatch) = &verdict {
        detail(report, mismatch);
    }
    verdict
}

/// The verdict as the run line prints it.
fn headline(verdict: &Verdict) -> String {
    match verdict {
        Verdict::Equivalent => "EQUIVALENT".to_owned(),
        Verdict::Diverged(Mismatch::Checkpoint(d)) => {
            format!("DIVERGED at checkpoint {} ({})", d.index, d.kind)
        }
        Verdict::Diverged(Mismatch::Total { name, .. }) => format!("DIVERGED ({name})"),
        Verdict::EventsOnly { recorded, current } => {
            format!("EVENTS EQUIVALENT, STATE NOT COMPARABLE (feature set {recorded} ≠ {current})")
        }
        Verdict::NotComparable(reason) => format!("NOT COMPARABLE: {reason}"),
    }
}

/// The lines describing the first divergence.
fn detail(report: &mut Vec<u8>, mismatch: &Mismatch) {
    match mismatch {
        Mismatch::Checkpoint(d) => {
            let field = |c: &Option<StateCheckpoint>, f: &dyn Fn(&StateCheckpoint) -> String| {
                c.as_ref().map_or_else(|| "-".to_owned(), f)
            };
            let row = |name: &str, f: &dyn Fn(&StateCheckpoint) -> String| {
                format!(
                    "  {name:<10} live {:<20} replay {}",
                    field(&d.live, f),
                    field(&d.replay, f)
                )
            };
            line(
                report,
                format!(
                    "  first divergence: checkpoint {}, {}",
                    d.index,
                    match d.kind {
                        DivergenceKind::EventStream => "the delivered events differ",
                        DivergenceKind::State => "the events match, the state differs",
                        DivergenceKind::Missing => "one side has no such checkpoint",
                    }
                ),
            );
            line(report, row("ordinal", &|c| c.ordinal.to_string()));
            line(
                report,
                row("as_of", &|c| {
                    c.as_of
                        .map_or_else(|| "-".to_owned(), |t| t.as_millis().to_string())
                }),
            );
            line(
                report,
                row("event hash", &|c| c.events.fingerprint.to_string()),
            );
            line(report, row("state hash", &|c| c.state.to_string()));
            line(report, row("last", &|c| c.last.to_string()));
        }
        Mismatch::Total { name, live, replay } => {
            line(report, format!("  run_end {name} {live}, replay {replay}"))
        }
    }
}

fn line(report: &mut Vec<u8>, text: String) {
    report.extend(text.into_bytes());
    report.push(b'\n');
}
