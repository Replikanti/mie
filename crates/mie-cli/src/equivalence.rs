//! `mie equivalence`: the live/replay equivalence harness (#13, ADR-019,
//! ADR-041, ADR-052 D9).
//!
//! Select the journal's runs of the configured source and symbol that
//! **started** in `[from, to)` (wall clock of `run_start`). A selected run
//! is compared in one of two units, by how its core started its engine
//! (its `warm_start`).
//!
//! **The cold unit** (ADR-041 D4), for every run without a warm start:
//! journaled before ADR-052, without a history (`no-history`), before the
//! cutover (`before-cutover`), or after a failed warm-up (`warm-up-failed`).
//! `mie ingest` gave such a run a fresh engine, so only a recompute of that
//! run alone, into a fresh engine, can reach the states live reached.
//!
//! 1. A run without a clean `run_end`, without state checkpoints (journaled
//!    before #13) or recorded with another event-stream or state hash
//!    encoding than this binary's is NOT COMPARABLE. Otherwise recompute it
//!    with [`LiveReplay::run_replay`] — no chaining, no window — and drive a
//!    fresh [`MarketStateEngine`] through [`drive_checkpointed`] at the
//!    run's journaled interval, the drive live ingestion used.
//! 2. When the run's feature set is this binary's, [`compare`] the
//!    replay's checkpoints with the journaled ones and the delivered events
//!    and domain rejections with `run_end`. Otherwise (ADR-041) only what no
//!    engine change can move is compared: [`compare_events`] — each
//!    journaled checkpoint's event-stream hash against the replay's after
//!    as many events — and the delivered events with `run_end`. A run with
//!    no journaled checkpoint that matches its totals delivered no event;
//!    nothing was compared, so it is NOT COMPARABLE too.
//!
//! **The warm unit** (ADR-052 D9), for every run whose `warm_start` is
//! warm: its engine went through the history chain up to its predecessor
//! before it took the run's first event.
//!
//! 1. The cold unit's checks, then the history keys: the configuration must
//!    carry `[history]` with the origin and cutover the run journaled in
//!    `warm_start`. Otherwise the run is NOT COMPARABLE, naming both chains.
//! 2. One pass over the chain serves every warm run that passed them, in
//!    start order: [`HistoryChain::to_end_of`] the last of them, split at
//!    run boundaries by [`HistoryChainStream::run_of_last_event`] into
//!    [`RunParts`], driven into one [`MarketStateEngine`].
//! 3. Per warm run, the parts before it are driven through [`drive_chain`];
//!    their event-stream hash and the engine's state hash are checked
//!    against the journaled prefix and handoff with [`compare_prefix`] (the
//!    state only when the run's feature set is this binary's). A mismatch is
//!    `DIVERGED (warm prefix)`; the report prints the journaled and the
//!    current prefix dataset version beside it, never comparing them: a
//!    fast restart whose first records lie before its predecessor's end plus
//!    `RUN_MARGIN_MS` moves the version without a data change once it has
//!    sealed them (ADR-052 D5, D9).
//! 4. The run's own part, the chain's recompute of the run joined to the
//!    prefix through the ADR-039 D3 `HoldBack` (what the run's core
//!    consumed), is driven into the same engine through
//!    [`drive_chain_checkpointed`] with a fresh recorder, and compared as
//!    the cold unit compares a run. The part is driven even after a prefix
//!    mismatch, so the pass stays on the chain. A failure of the chain makes
//!    the warm run it reaches and every later one NOT COMPARABLE. A chain
//!    run that fails to load is charged to the first warm run at or after
//!    it, never to an earlier one, whose events were all delivered: a warm
//!    run's verdict does not depend on which later runs are selected.
//!
//! The report: a header (window, source, encodings, current feature set,
//! and the history chain's pass when one ran), one line per run in journal
//! order with its verdict — `EQUIVALENT`, `DIVERGED`, `DIVERGED (warm
//! prefix)`, `EVENTS EQUIVALENT, STATE NOT COMPARABLE` or `NOT COMPARABLE` —
//! the first divergence of a diverged run, then `PASS` or `FAIL: …`.
//!
//! The report carries no wall-clock field, so the same request prints the
//! same bytes. It passes when every selected run is `EQUIVALENT` and there
//! is at least one: an `EQUIVALENT` run compared at least one checkpoint.

use crate::config::IngestConfig;
use crate::journal::{Checkpointing, JournaledRun, RunEnd, WarmHandoff, WarmStart, read_runs};
use mie_adapter_binance::{HistoryChainStream, LiveReplay};
use mie_adapter_parquet::ParquetRawStore;
use mie_app::drive_chain;
use mie_app::equivalence::{
    Comparison, Divergence, DivergenceKind, EventComparison, EventDivergence, EventPrefixes,
    RunParts, StateCheckpoint, compare, compare_events, compare_prefix, drive_chain_checkpointed,
    drive_checkpointed,
};
use mie_domain::event_hash::{self, EventStreamHash};
use mie_domain::feature::{FeatureSetVersion, catalog};
use mie_domain::state::MarketStateEngine;
use mie_domain::state_hash::{STATE_HASH_ENCODING, StateHash};
use mie_ports::outbound::{MarketDataProvider, ProviderError};
use mie_ports::raw::DatasetVersion;
use std::collections::BTreeMap;
use std::io::Write;

/// Why a cleanly ended run without a checkpoint is not compared.
pub const NOTHING_COMPARED: &str = "no checkpoint to compare: the run delivered no event";

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
    /// The first checkpoint whose events differ, for a run recorded with
    /// another feature set.
    Events(EventDivergence),
    /// A warm-started run's prefix differs from its journaled handoff
    /// (ADR-052 D9).
    WarmPrefix(WarmPrefixDivergence),
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

/// How a warm-started run's prefix differs: what `warm_start` journaled
/// (`live`) against the chain's replay now (`replay`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmPrefixDivergence {
    /// The delivered events differ, or only the handoff state.
    pub kind: DivergenceKind,
    /// The journaled prefix's event-stream hash.
    pub live_stream: EventStreamHash,
    /// The replayed prefix's event-stream hash.
    pub replay_stream: EventStreamHash,
    /// The journaled handoff state hash.
    pub live_state: StateHash,
    /// The state hash after the replayed prefix.
    pub replay_state: StateHash,
    /// The prefix dataset version `warm_start` journaled.
    pub live_dataset: DatasetVersion,
    /// The prefix dataset version now: printed, never compared (module
    /// docs).
    pub replay_dataset: DatasetVersion,
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
            let selected: Vec<&JournaledRun> = runs
                .iter()
                .filter(|run| {
                    run.parameters.symbol == *symbol
                        && (request.from_ms..request.to_ms).contains(&run.started_at_ms)
                })
                .collect();
            // Verdicts first, printed in journal order afterwards: the warm
            // runs share one pass, whose header line precedes every run.
            let mut judged: Vec<Option<Judged>> = vec![None; selected.len()];
            let mut warm = Vec::new();
            for (slot, run) in selected.iter().enumerate() {
                match &run.warm_start {
                    Some(WarmStart::Warm(handoff)) => warm.push((slot, *run, handoff)),
                    _ => {
                        let mut lines = Vec::new();
                        let verdict = judge(&live, run, current, &mut lines);
                        judged[slot] = Some(Judged { verdict, lines });
                    }
                }
            }
            if let Some(pass) = warm_pass(config, &store, &warm, current, &mut judged) {
                line(&mut report, pass);
            }
            let mut verdicts = Vec::new();
            for (run, judged) in selected.iter().zip(judged) {
                let Judged { verdict, lines } = judged.expect("every selected run is judged");
                report.extend(lines);
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

/// One run's verdict and its report lines.
#[derive(Debug, Clone)]
struct Judged {
    verdict: Verdict,
    lines: Vec<u8>,
}

/// A `NOT COMPARABLE` run: its line and verdict.
fn not_comparable(run_id: &str, reason: String) -> Judged {
    let mut lines = Vec::new();
    line(
        &mut lines,
        format!("run {run_id}: NOT COMPARABLE: {reason}"),
    );
    Judged {
        verdict: Verdict::NotComparable(reason),
        lines,
    }
}

/// The checks of both units before any replay (module docs): a clean
/// `run_end`, journaled checkpoints, this binary's encodings. The reason
/// when one fails.
fn comparable(run: &JournaledRun) -> Result<(RunEnd, Checkpointing), String> {
    let Some(end) = run.end.filter(|end| end.exit_code == 0) else {
        return Err("the run did not end cleanly".to_owned());
    };
    let Some(checkpointing) = run.checkpointing else {
        return Err("the run journaled no state checkpoints (recorded before ADR-041)".to_owned());
    };
    if checkpointing.event_encoding != event_hash::ENCODING_VERSION
        || checkpointing.state_encoding != STATE_HASH_ENCODING
    {
        return Err(format!(
            "recorded with event-stream v{} and state v{} encodings",
            checkpointing.event_encoding, checkpointing.state_encoding
        ));
    }
    Ok((end, checkpointing))
}

/// Recomputes and compares one run in the cold unit, and reports it.
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
    let (end, checkpointing) = match comparable(run) {
        Ok(keys) => keys,
        Err(reason) => return not_comparable(report, reason),
    };
    let mut replay = match live.run_replay(run_id) {
        Ok(replay) => replay,
        Err(error) => return not_comparable(report, format!("replay failed: {error}")),
    };
    let compare_state = checkpointing.feature_set == current;
    let interval_ms = checkpointing.interval_ms;
    let (driven, prefixes) = if compare_state {
        (recompute(&mut replay, interval_ms), Vec::new())
    } else {
        let mut observed = EventPrefixes::new(&mut replay, &run.checkpoints);
        let driven = recompute(&mut observed, interval_ms);
        (driven, observed.into_hashes())
    };
    let recomputed = match driven {
        Ok(recomputed) => recomputed,
        Err(error) => return not_comparable(report, format!("replay failed: {error}")),
    };
    let verdict = verdict_of(run, &end, &checkpointing, current, &recomputed, &prefixes);
    let Recomputed {
        events, rejections, ..
    } = recomputed;
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

/// The verdict on a run's recompute, in both units (module docs): the
/// first divergent checkpoint (all of it with this binary's feature set,
/// else its events, the `prefixes` from [`EventPrefixes`]), then a
/// `run_end` total, then nothing compared, else `EQUIVALENT` or
/// `EVENTS EQUIVALENT, STATE NOT COMPARABLE`.
fn verdict_of(
    run: &JournaledRun,
    end: &RunEnd,
    checkpointing: &Checkpointing,
    current: FeatureSetVersion,
    recomputed: &Recomputed,
    prefixes: &[EventStreamHash],
) -> Verdict {
    let compare_state = checkpointing.feature_set == current;
    let checkpoint = if compare_state {
        match compare(&run.checkpoints, &recomputed.checkpoints) {
            Comparison::Diverged(divergence) => Some(Mismatch::Checkpoint(divergence)),
            Comparison::Equivalent { .. } => None,
        }
    } else {
        match compare_events(&run.checkpoints, prefixes) {
            EventComparison::Diverged(divergence) => Some(Mismatch::Events(divergence)),
            EventComparison::Equivalent { .. } => None,
        }
    };
    // Domain rejections depend on the engine: compared with state only.
    let totals = [
        ("events", end.events, recomputed.events, true),
        (
            "domain_rejections",
            end.domain_rejections,
            recomputed.rejections,
            compare_state,
        ),
    ];
    let total = || {
        totals
            .into_iter()
            .find(|&(_, live, replay, compared)| compared && live.is_some_and(|l| l != replay))
            .map(|(name, live, replay, _)| Mismatch::Total {
                name,
                live: live.unwrap_or_default(),
                replay,
            })
    };
    match checkpoint.or_else(total) {
        Some(mismatch) => Verdict::Diverged(mismatch),
        // Nothing was compared: no PASS on an empty run.
        None if run.checkpoints.is_empty() => Verdict::NotComparable(NOTHING_COMPARED.to_owned()),
        None if compare_state => Verdict::Equivalent,
        None => Verdict::EventsOnly {
            recorded: checkpointing.feature_set,
            current,
        },
    }
}

/// A warm run that passed the checks before the pass.
struct WarmRun<'r> {
    /// Its slot among the selected runs.
    slot: usize,
    run: &'r JournaledRun,
    handoff: &'r WarmHandoff,
    end: RunEnd,
    checkpointing: Checkpointing,
}

/// The history chain's bounds `[history]` has, or why they cannot be read.
fn configured_chain(config: &IngestConfig) -> Result<(i64, i64), String> {
    let Some(history) = &config.history else {
        return Err("the configuration has no [history]".to_owned());
    };
    let origin = history.origin_ms().map_err(|e| e.to_string())?;
    let cutover = history.cutover_ms().map_err(|e| e.to_string())?;
    Ok((origin, cutover))
}

/// The warm unit (module docs) over the selected warm runs `warm`, each
/// with its slot among the selected runs: fills their slots of `judged`
/// and returns the header line of the pass, when one ran.
fn warm_pass(
    config: &IngestConfig,
    store: &ParquetRawStore,
    warm: &[(usize, &JournaledRun, &WarmHandoff)],
    current: FeatureSetVersion,
    judged: &mut [Option<Judged>],
) -> Option<String> {
    let configured = configured_chain(config);
    let mut comparable_runs = Vec::new();
    for &(slot, run, handoff) in warm {
        let run_id = &run.parameters.run_id;
        let keys = comparable(run).and_then(|keys| {
            let journaled = format!(
                "the run warm-started from the history chain with origin {} ms and cutover {} \
                 ms",
                handoff.origin_ms, handoff.cutover_ms
            );
            match &configured {
                Ok(bounds) if *bounds == (handoff.origin_ms, handoff.cutover_ms) => Ok(keys),
                Ok((origin, cutover)) => Err(format!(
                    "{journaled}, the configured [history] has origin {origin} ms and cutover \
                     {cutover} ms"
                )),
                Err(error) => Err(format!("{journaled}, {error}")),
            }
        });
        match keys {
            Ok((end, checkpointing)) => comparable_runs.push(WarmRun {
                slot,
                run,
                handoff,
                end,
                checkpointing,
            }),
            Err(reason) => judged[slot] = Some(not_comparable(run_id, reason)),
        }
    }
    if comparable_runs.is_empty() {
        return None;
    }
    let fail_all = |judged: &mut [Option<Judged>], runs: &[WarmRun<'_>], reason: &str| {
        for warm in runs {
            judged[warm.slot] = Some(not_comparable(
                &warm.run.parameters.run_id,
                reason.to_owned(),
            ));
        }
    };
    let chain = match crate::replay::history_chain(config, store) {
        Ok(chain) => chain,
        Err(error) => {
            fail_all(judged, &comparable_runs, &format!("history chain: {error}"));
            return None;
        }
    };
    // Each warm run at its index among the chain runs, in chain order.
    let index: BTreeMap<String, usize> = chain
        .chain_runs()
        .iter()
        .enumerate()
        .map(|(i, run)| (run.run_id.clone(), i))
        .collect();
    let mut ordered = Vec::new();
    for warm in comparable_runs {
        match index.get(&warm.run.parameters.run_id) {
            Some(&k) => ordered.push((k, warm)),
            None => {
                let run_id = &warm.run.parameters.run_id;
                judged[warm.slot] = Some(not_comparable(
                    run_id,
                    format!("history chain: run {run_id} is not one of its runs"),
                ));
            }
        }
    }
    ordered.sort_by_key(|(k, _)| *k);
    let (_, last) = ordered.last()?;
    let last_id = last.run.parameters.run_id.clone();
    let through = match chain.to_end_of(&last_id) {
        Ok(through) => through,
        Err(error) => {
            let runs: Vec<WarmRun<'_>> = ordered.into_iter().map(|(_, warm)| warm).collect();
            fail_all(judged, &runs, &format!("history chain: {error}"));
            return None;
        }
    };
    let header = format!(
        "history origin {} cutover {}: one pass through run {last_id}",
        chain.origin().as_millis(),
        chain.cutover().as_millis()
    );
    // A run the chain does not know cannot occur (the stream recomputes
    // chain runs only): as an event's part it would end every view, as a
    // failure's it reaches the view that meets it. A failure is charged to
    // the run that failed, so a warm run before it keeps its full verdict,
    // the one a pass ending with it gives.
    let failures = index.clone();
    let mut parts = RunParts::new(
        through.stream,
        move |stream: &HistoryChainStream<'_>| {
            stream
                .run_of_last_event()
                .map(|run_id| index.get(run_id).copied().unwrap_or(usize::MAX))
        },
        move |stream: &HistoryChainStream<'_>| {
            stream
                .failed_run()
                .and_then(|run_id| failures.get(run_id).copied())
        },
    );
    let mut engine = MarketStateEngine::new();
    let mut failed: Option<String> = None;
    for (k, warm) in ordered {
        let run_id = &warm.run.parameters.run_id;
        if let Some(error) = &failed {
            judged[warm.slot] = Some(not_comparable(
                run_id,
                format!("chain replay failed: {error}"),
            ));
            continue;
        }
        let compare_state = warm.checkpointing.feature_set == current;
        if let Err(error) = drive_chain(&mut parts.before(k), &mut engine, |_| {}) {
            failed = Some(error.to_string());
            judged[warm.slot] = Some(not_comparable(
                run_id,
                format!("chain replay failed: {error}"),
            ));
            continue;
        }
        let prefix_stream = parts.hash();
        let prefix_state = engine.state().state_hash();
        // Opening the prefix only selects its files; nothing is read.
        let dataset = chain.before_run(run_id).map(|prefix| prefix.dataset);
        let prefix = compare_prefix(
            (warm.handoff.prefix_stream, warm.handoff.handoff_state),
            (prefix_stream, prefix_state),
            compare_state,
        );
        let interval_ms = warm.checkpointing.interval_ms;
        let mut view = parts.run(k);
        let (driven, prefixes) = if compare_state {
            (
                recompute_chained(&mut view, &mut engine, interval_ms),
                Vec::new(),
            )
        } else {
            let mut observed = EventPrefixes::new(&mut view, &warm.run.checkpoints);
            let driven = recompute_chained(&mut observed, &mut engine, interval_ms);
            (driven, observed.into_hashes())
        };
        let recomputed = match driven {
            Ok(recomputed) => recomputed,
            Err(error) => {
                failed = Some(error.to_string());
                judged[warm.slot] = Some(not_comparable(
                    run_id,
                    format!("chain replay failed: {error}"),
                ));
                continue;
            }
        };
        let dataset = match dataset {
            Ok(dataset) => dataset,
            Err(error) => {
                judged[warm.slot] = Some(not_comparable(run_id, format!("history chain: {error}")));
                continue;
            }
        };
        let verdict = match prefix {
            Some(kind) => Verdict::Diverged(Mismatch::WarmPrefix(WarmPrefixDivergence {
                kind,
                live_stream: warm.handoff.prefix_stream,
                replay_stream: prefix_stream,
                live_state: warm.handoff.handoff_state,
                replay_state: prefix_state,
                live_dataset: warm.handoff.prefix_dataset.clone(),
                replay_dataset: dataset.clone(),
            })),
            None => verdict_of(
                warm.run,
                &warm.end,
                &warm.checkpointing,
                current,
                &recomputed,
                &prefixes,
            ),
        };
        let records = parts
            .inner()
            .live()
            .and_then(|live| {
                live.stream()
                    .runs()
                    .iter()
                    .find(|stats| stats.run_id == *run_id)
            })
            .map_or(0, |stats| stats.records);
        let mut lines = Vec::new();
        line(
            &mut lines,
            format!(
                "run {run_id}: warm prefix events {}, records {records}, events {}, rejections \
                 {}, checkpoints {}, prefix dataset {dataset}: {}",
                prefix_stream.events,
                recomputed.events,
                recomputed.rejections,
                warm.run.checkpoints.len(),
                headline(&verdict)
            ),
        );
        if let Verdict::Diverged(mismatch) = &verdict {
            detail(&mut lines, mismatch);
        }
        judged[warm.slot] = Some(Judged { verdict, lines });
    }
    Some(header)
}

/// What the recompute of one run delivered.
struct Recomputed {
    events: u64,
    rejections: u64,
    checkpoints: Vec<StateCheckpoint>,
}

/// Drives `engine`, already through the run's prefix, through `provider`
/// as the core of a warm-started run did: the chain drive, with a fresh
/// recorder.
fn recompute_chained<P: MarketDataProvider + ?Sized>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    interval_ms: i64,
) -> Result<Recomputed, ProviderError> {
    let mut rejections = 0_u64;
    let mut checkpoints = Vec::new();
    let drive = drive_chain_checkpointed(
        provider,
        engine,
        interval_ms,
        |_| rejections += 1,
        |_| {},
        |checkpoint| checkpoints.push(*checkpoint),
    )?;
    Ok(Recomputed {
        events: drive.events,
        rejections,
        checkpoints,
    })
}

/// Drives a fresh engine through `provider` as live ingestion did.
fn recompute<P: MarketDataProvider + ?Sized>(
    provider: &mut P,
    interval_ms: i64,
) -> Result<Recomputed, ProviderError> {
    let mut rejections = 0_u64;
    let mut checkpoints = Vec::new();
    let events = drive_checkpointed(
        provider,
        &mut MarketStateEngine::new(),
        interval_ms,
        |_| rejections += 1,
        |checkpoint| checkpoints.push(*checkpoint),
    )?;
    Ok(Recomputed {
        events,
        rejections,
        checkpoints,
    })
}

/// The verdict as the run line prints it.
fn headline(verdict: &Verdict) -> String {
    match verdict {
        Verdict::Equivalent => "EQUIVALENT".to_owned(),
        Verdict::Diverged(Mismatch::Checkpoint(d)) => {
            format!("DIVERGED at checkpoint {} ({})", d.index, d.kind)
        }
        Verdict::Diverged(Mismatch::Events(d)) => {
            format!("DIVERGED at checkpoint {} (event stream)", d.index)
        }
        Verdict::Diverged(Mismatch::WarmPrefix(_)) => "DIVERGED (warm prefix)".to_owned(),
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
        Mismatch::Events(d) => {
            line(
                report,
                format!(
                    "  first divergence: checkpoint {}, the delivered events differ (events \
                     only)",
                    d.index
                ),
            );
            let replay = |f: &dyn Fn(&EventStreamHash) -> String| {
                d.replay.as_ref().map_or_else(|| "-".to_owned(), f)
            };
            line(
                report,
                format!(
                    "  {:<10} live {:<20} replay {}",
                    "ordinal",
                    d.live.ordinal,
                    replay(&|h| h.events.to_string())
                ),
            );
            line(
                report,
                format!(
                    "  {:<10} live {:<20} replay {}",
                    "event hash",
                    d.live.events.fingerprint.to_string(),
                    replay(&|h| h.fingerprint.to_string())
                ),
            );
        }
        Mismatch::WarmPrefix(d) => {
            line(
                report,
                format!(
                    "  first divergence: the warm prefix, {}",
                    match d.kind {
                        DivergenceKind::State => "the events match, the handoff state differs",
                        _ => "the delivered events differ",
                    }
                ),
            );
            let row = |name: &str, live: String, replay: String| {
                format!("  {name:<10} live {live:<20} replay {replay}")
            };
            line(
                report,
                row(
                    "events",
                    d.live_stream.events.to_string(),
                    d.replay_stream.events.to_string(),
                ),
            );
            line(
                report,
                row(
                    "event hash",
                    d.live_stream.fingerprint.to_string(),
                    d.replay_stream.fingerprint.to_string(),
                ),
            );
            line(
                report,
                row(
                    "state hash",
                    d.live_state.to_string(),
                    d.replay_state.to_string(),
                ),
            );
            line(
                report,
                row(
                    "dataset",
                    d.live_dataset.to_string(),
                    d.replay_dataset.to_string(),
                ),
            );
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
