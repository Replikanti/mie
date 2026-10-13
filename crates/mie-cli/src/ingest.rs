//! `mie ingest`: live Binance capture into the raw store, feeding the core.
//!
//! 1. Open the raw-store writer of the source (lock + crash recovery) and
//!    journal its recovery report.
//! 2. Seed each stream of the capture's pipeline with the largest event
//!    time sealed in the last 7 days, so the run opens with a restart gap
//!    per stream (ADR-032 D5).
//! 3. Journal `run_start` with the pipeline parameters, the comparability
//!    keys and the history chain's bounds, and sync it: replay needs it for
//!    every record the run seals (ADR-039 D2).
//! 4. Decide how the core's engine starts (ADR-052 D8): without `[history]`
//!    the run is cold (`no-history`); a run that started before the
//!    cutover is not part of the chain and is cold (`before-cutover`); any
//!    other run is a *chain run*, warm-started below.
//! 5. A cold run journals its `warm_start` reason, starts the capture and
//!    drives a fresh engine on this thread through
//!    [`mie_app::equivalence::drive_checkpointed`]: the rejection policy of
//!    [`mie_app::drive_tolerant`], which replay uses too (ADR-019, ADR-039
//!    D9), plus the state checkpoints the equivalence harness recomputes
//!    (ADR-041). An event the engine rejects is journaled and counted, and
//!    driving resumes: the engine leaves its state untouched on a
//!    rejection, and a soak must not stop on one. Every checkpoint is
//!    journaled as a `state_checkpoint` line, the last one after the final
//!    event of a run without a provider failure. A provider failure ends
//!    the run.
//! 6. A chain run (ADR-052 D3, D7) starts the capture at once, with the
//!    core fed the raw records ([`CoreFeed::Records`]), and builds the
//!    engine on a `mie-warm-up` thread, which owns its own read-only store:
//!    - the memory guard (D13): before anything is replayed, the largest
//!      chain run before this one, by its `run_end` record count or, for a
//!      run without a clean end, the rows of the sealed files over its
//!      ADR-039 D2 extent (an upper bound), must not exceed `[history]
//!      max_run_records`; without the key no warm-up starts;
//!    - the history chain through this run's predecessor
//!      ([`HistoryChain::before_run`]), driven into a fresh engine through
//!      [`mie_app::drive_chain`], hashed, and checked for cancellation on
//!      every event.
//!
//!    Meanwhile this thread moves the records into an in-memory queue,
//!    bounded by what `max_run_records` leaves beside the largest run the
//!    warm-up replays (all of it until the guard has run): the queued and
//!    the replayed records together never exceed the guard's budget. A
//!    queue past its limit abandons the warm-up (cancelled, not waited for)
//!    and starts the run cold. When the warm-up has finished (checked after
//!    every record), the handoff
//!    journals a warm `warm_start` with the chain seeds
//!    ([`ChainTail::run_seeds`]) and syncs it before the seeds are used.
//!    Then the warm engine is driven through
//!    [`mie_app::equivalence::drive_chain_checkpointed`] with a fresh
//!    recorder, so checkpoints count this run's events only: the queue, then
//!    the live records, are pushed through [`ChainTail::continuation`], the
//!    same [`RunContinuation`] the chained replay recomputes the run with,
//!    joined to the prefix through the chain's zero hold-back `HoldBack`.
//!    A fast-restart overlap therefore becomes a `LateEvent` gap or
//!    re-timed open interest, never an `OutOfOrder`. `warm_ready` records
//!    when the queue is drained; `chain_restart` records every uncrossable
//!    hole in the live part (ADR-052 D4) and is synced as it happens.
//!
//!    A warm-up that fails, panics, is refused by the guard, overflows the
//!    queue, or has not finished when the capture ends (the core then
//!    cancels it) gives a
//!    cold `warm-up-failed` run: the reason is journaled and synced, and a
//!    fresh engine is driven through `drive_checkpointed` from a
//!    [`RunContinuation`] without a join, seeded with `run_start`'s seeds,
//!    over the queue and then the live records: exactly what the capture's
//!    own pipeline delivers, so the ADR-041 D4 cold unit. The capture never
//!    waits for the warm-up and never stops because of it.
//!
//!    The capture's own pipeline keeps its lookback seeds and still writes
//!    the `gap`, `book` and `book_checkpoint` lines, requests snapshots and
//!    audits the book, so a warm run's `gap` lines describe that pipeline.
//! 7. On shutdown (SIGINT/SIGTERM sets the flag), join the capture, close
//!    the writer (sealing everything) and journal a `run_end` summary;
//!    `events` counts the events delivered to the engine.

use crate::config::IngestConfig;
use crate::journal::{
    ColdReason, ColdStart, Journal, JournalObserver, WarmHandoff, WarmStart, chain_restart_json,
    files_json, pipeline_json, read_runs, seeds_json, state_checkpoint_json, warm_ready_json,
    warm_start_json,
};
use crate::replay::history_chain;
use mie_adapter_binance::transport::{Clock, HttpGet, WsConnector};
use mie_adapter_binance::{
    BinanceLiveProvider, BinanceStream, CaptureSummary, ChainTail, CoreFeed, HistoryChain, LiveRun,
    RUN_MARGIN_MS, RunContinuation, run_id, start,
};
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_app::{HashingProvider, drive_chain};
use mie_domain::event::MarketEvent;
use mie_domain::event_hash::{self, EventStreamHash};
use mie_domain::feature::{FeatureKey, FeatureValue, catalog};
use mie_domain::state::{MarketState, MarketStateEngine};
use mie_domain::state_hash::STATE_HASH_ENCODING;
use mie_domain::time::EventTime;
use mie_ports::outbound::{MarketDataProvider, ProviderError, Replay, ReplayWindow};
use mie_ports::raw::{DatasetVersion, RawRecord, RawRecordSource, RawSelection, RawStreamKey};
use serde_json::{Value, json};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

const DAY_MS: i64 = 86_400_000;

/// How far back the seeds look.
const SEED_LOOKBACK_MS: i64 = 7 * DAY_MS;

/// The I/O the run uses; real ones in `main`, fakes in tests.
pub struct Transports {
    /// WebSocket connector.
    pub connector: Arc<dyn WsConnector>,
    /// HTTP client.
    pub http: Arc<dyn HttpGet>,
    /// Clock.
    pub clock: Arc<dyn Clock>,
}

/// Test hooks around the warm-up of a chain run ([`run_with_hold`]); [`run`]
/// passes none. Both run on the warm-up thread.
#[derive(Default)]
pub struct WarmUpHold {
    /// Runs before the warm-up reads anything; the warm-up waits for it.
    pub before: Option<Box<dyn FnOnce() + Send>>,
    /// Runs once the warm-up has its result (or panicked), before the
    /// thread ends.
    pub after: Option<Box<dyn FnOnce() + Send>>,
}

/// The result of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    /// The run's id.
    pub run_id: String,
    /// Events the core consumed (rejections included).
    pub events: u64,
    /// Events the engine rejected.
    pub domain_rejections: u64,
    /// The capture's final counters, when it ended cleanly.
    pub summary: Option<CaptureSummary>,
    /// What went wrong, if anything.
    pub error: Option<String>,
}

impl IngestOutcome {
    /// 0 for a clean run, 1 otherwise.
    pub fn exit_code(&self) -> i32 {
        i32::from(self.error.is_some())
    }
}

/// Counts what the core pulls.
struct Counting<'a, P> {
    inner: &'a mut P,
    events: u64,
}

impl<P: MarketDataProvider> MarketDataProvider for Counting<'_, P> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        let event = self.inner.next_event()?;
        self.events += u64::from(event.is_some());
        Ok(event)
    }
}

/// Runs one capture until `shutdown` is set or the capture fails.
///
/// # Errors
///
/// A description of a failure before the capture started (configuration,
/// journal, store lock, recovery, seeds). Failures after the start end the
/// run and are reported in the [`IngestOutcome`].
pub fn run(
    config: &IngestConfig,
    transports: Transports,
    shutdown: Arc<AtomicBool>,
) -> Result<IngestOutcome, String> {
    run_with_hold(config, transports, shutdown, WarmUpHold::default())
}

/// [`run`] with test hooks around the warm-up of a chain run.
///
/// # Errors
///
/// As [`run`].
pub fn run_with_hold(
    config: &IngestConfig,
    transports: Transports,
    shutdown: Arc<AtomicBool>,
    hold: WarmUpHold,
) -> Result<IngestOutcome, String> {
    let clock = Arc::clone(&transports.clock);
    let now = || clock.now_utc_ns();
    let run_id = run_id(now());
    let journal = Arc::new(Mutex::new(Journal::open(&config.paths.journal, &run_id)?));
    let log = |kind: &str, fields: Value| {
        let mut journal = journal.lock().unwrap_or_else(|p| p.into_inner());
        journal.write(now(), kind, fields);
    };
    let sync = || journal.lock().unwrap_or_else(|p| p.into_inner()).sync();

    let store = ParquetRawStore::new(&config.paths.raw_root);
    let writer = store
        .writer(&config.instrument.source, RotationPolicy::default())
        .map_err(|e| format!("open the raw store: {e}"))?;
    let recovery = writer.recovery();
    log(
        "recovery",
        json!({
            "clean": recovery.is_clean(),
            "rolled_forward": recovery.rolled_forward,
            "discarded": recovery
                .discarded
                .iter()
                .map(|(path, bytes)| json!({"path": path, "bytes": bytes}))
                .collect::<Vec<_>>(),
        }),
    );

    let streams = config.streams().map_err(|e| e.to_string())?;
    let checkpoint_interval_ms = config
        .state_checkpoint_interval_ms()
        .map_err(|e| e.to_string())?;
    let bounds = match &config.history {
        Some(history) => Some((
            history.origin_ms().map_err(|e| e.to_string())?,
            history.cutover_ms().map_err(|e| e.to_string())?,
        )),
        None => None,
    };
    // One start time for the seeds, `run_start` and the mode, so the mode
    // agrees with the history chain, which reads the start from `run_start`.
    let started_ns = now();
    let started_ms = started_ns.div_euclid(1_000_000);
    let seeds = seeds(&store, config, &streams, started_ms)?;
    let feature_set = MarketStateEngine::new().feature_set().version().to_string();
    journal.lock().unwrap_or_else(|p| p.into_inner()).write(
        started_ns,
        "run_start",
        json!({
            "symbol": config.instrument.symbol,
            "source": config.instrument.source,
            "streams": streams.iter().map(|s| s.raw_name()).collect::<Vec<_>>(),
            "hold_back_ms": config.capture.hold_back_ms,
            "oi_retime_ms": config.capture.oi_retime_allowance_ms,
            "seal_interval_secs": config.capture.seal_interval_secs,
            "depth_snapshot_limit": config.capture.depth_snapshot_limit,
            "depth_checkpoint_interval_ms": config
                .capture
                .depth_checkpoint_interval_secs
                .saturating_mul(1_000),
            "seeds": seeds_json(&seeds),
            "state_checkpoint_interval_ms": checkpoint_interval_ms,
            "state_hash_encoding": STATE_HASH_ENCODING,
            "event_hash_encoding": event_hash::ENCODING_VERSION,
            "feature_set": feature_set,
            "history_origin_ms": bounds.map(|(origin, _)| origin),
            "history_cutover_ms": bounds.map(|(_, cutover)| cutover),
        }),
    );

    // Durable before the first record is sealed: replay needs the run's
    // parameters for every record it seals (ADR-039 D2).
    sync();

    let cold = |reason: ColdReason, error: Option<String>| {
        WarmStart::Cold(ColdStart {
            reason,
            error,
            origin_ms: bounds.map(|(origin, _)| origin),
            cutover_ms: bounds.map(|(_, cutover)| cutover),
        })
    };
    let chained = match bounds {
        None => {
            log(
                "warm_start",
                warm_start_json(&cold(ColdReason::NoHistory, None)),
            );
            false
        }
        Some((_, cutover)) if started_ms < cutover => {
            log(
                "warm_start",
                warm_start_json(&cold(ColdReason::BeforeCutover, None)),
            );
            false
        }
        Some(_) => true,
    };

    let mut live = config
        .live_config(&run_id, seeds.clone())
        .map_err(|e| e.to_string())?;
    if chained {
        live.core_feed = CoreFeed::Records;
    }
    let (hold_back_ms, oi_retime_ms) = (live.hold_back_ms, live.oi_retime_ms);
    let observer = JournalObserver(Arc::clone(&journal));
    let (mut provider, handle) = start(
        live,
        writer,
        transports.connector,
        transports.http,
        Arc::clone(&clock),
        Box::new(observer),
        Arc::clone(&shutdown),
    )
    .map_err(|e| e.to_string())?;

    let mut domain_rejections = 0_u64;
    let on_rejection = |rejected: &mie_domain::state::StateError| {
        domain_rejections += 1;
        log("domain_rejection", json!({"error": rejected.to_string()}));
    };
    let on_checkpoint = |checkpoint: &mie_app::equivalence::StateCheckpoint| {
        log("state_checkpoint", state_checkpoint_json(checkpoint));
    };
    let (driven, events) = if chained {
        // The warm-up: the chain through this run's predecessor, on its own
        // thread, while the core queues the records. The queue's limit is
        // what `max_run_records` leaves beside the run the warm-up replays
        // (`QueueLimit`); without the key the warm-up cannot start.
        let cancel = Arc::new(AtomicBool::new(false));
        let max_run_records = config.history.as_ref().and_then(|h| h.max_run_records);
        let queue_limit = Arc::new(AtomicU64::new(max_run_records.unwrap_or(0)));
        let warm_up = match max_run_records {
            Some(max) => spawn_warm_up(config, &run_id, &cancel, &queue_limit, max, hold),
            None => Err(NOT_CONFIGURED.to_owned()),
        };
        let mut queue = VecDeque::new();
        let mut queued_bytes = 0_u64;
        let mut overflow = None;
        if let Ok(warm_up) = &warm_up {
            while !warm_up.is_finished() {
                match provider.next_record() {
                    Ok(Some((stream, record))) => {
                        queued_bytes += record.payload.len() as u64;
                        queue.push_back((stream, record));
                        let limit = queue_limit.load(Ordering::SeqCst);
                        if queue.len() as u64 > limit {
                            overflow = Some(format!(
                                "warm-up queue (ADR-052 D13): {} records queued, more than the \
                                 {limit} that [history] max_run_records {} leaves beside the run \
                                 the warm-up replays",
                                queue.len(),
                                max_run_records.unwrap_or(0),
                            ));
                            break;
                        }
                    }
                    // The capture ended or failed first: the warm-up is
                    // cancelled, and the drive below sees the end or the
                    // failure again after the queue.
                    Ok(None) | Err(_) => break,
                }
            }
        }
        cancel.store(true, Ordering::Relaxed);
        let warmed = match overflow {
            // Abandoned, not joined: the warm-up stops at its next prefix
            // event, and the core goes on draining the capture meanwhile.
            Some(error) => Err(error),
            None => warm_up.and_then(|warm_up| {
                warm_up.join().unwrap_or_else(|panic| {
                    Err(format!(
                        "the warm-up thread panicked: {}",
                        panic_message(panic.as_ref())
                    ))
                })
            }),
        };
        // The queue only grows during the warm-up: its length now is its
        // high-water mark.
        let queue_high_water = queue.len() as u64;
        match warmed {
            Ok(warm) => {
                let state = warm.engine.state();
                let run_seeds = warm.tail.run_seeds();
                let handoff = WarmHandoff {
                    origin_ms: warm.origin.as_millis(),
                    effective_origin_ms: warm.restarts.last().map(|t| t.as_millis()),
                    cutover_ms: warm.cutover.as_millis(),
                    prefix_dataset: warm.dataset.clone(),
                    prefix_last_event_ms: state.as_of.map(EventTime::as_millis),
                    prefix_stream: warm.stream,
                    prefix_rejections: warm.rejections,
                    handoff_state: state.state_hash(),
                    seeds: run_seeds.clone(),
                    queue_high_water,
                    queue_high_water_bytes: queued_bytes,
                    warming_up: warming_up(state),
                };
                log("warm_start", warm_start_json(&WarmStart::Warm(handoff)));
                // Durable before the seeds are used: a recompute of this run
                // needs them (ADR-052 D3).
                sync();
                let mut engine = warm.engine;
                let mut feed = RecordFeed {
                    queue,
                    live: &mut provider,
                    continuation: warm.tail.continuation(
                        &config.instrument.symbol,
                        hold_back_ms,
                        oi_retime_ms,
                        &run_seeds,
                    ),
                    out: VecDeque::new(),
                    ended: false,
                    on_drained: Some(Box::new(|| {
                        let ready_after_ms = now().div_euclid(1_000_000) - started_ms;
                        log("warm_ready", warm_ready_json(ready_after_ms));
                    })),
                };
                let mut counting = Counting {
                    inner: &mut feed,
                    events: 0,
                };
                // Each restart is journaled and synced as it happens, so a
                // later failure or crash does not lose it (ADR-052 D6).
                let driven = mie_app::equivalence::drive_chain_checkpointed(
                    &mut counting,
                    &mut engine,
                    checkpoint_interval_ms,
                    on_rejection,
                    |time| {
                        log("chain_restart", chain_restart_json(time));
                        sync();
                    },
                    on_checkpoint,
                );
                (driven.map(|_| ()), counting.events)
            }
            Err(error) => {
                log(
                    "warm_start",
                    warm_start_json(&cold(ColdReason::WarmUpFailed, Some(error))),
                );
                sync();
                let mut engine = MarketStateEngine::new();
                let mut feed = RecordFeed {
                    queue,
                    live: &mut provider,
                    continuation: RunContinuation::new(
                        &config.instrument.symbol,
                        hold_back_ms,
                        oi_retime_ms,
                        &seeds,
                        None,
                    ),
                    out: VecDeque::new(),
                    ended: false,
                    on_drained: None,
                };
                let mut counting = Counting {
                    inner: &mut feed,
                    events: 0,
                };
                let driven = mie_app::equivalence::drive_checkpointed(
                    &mut counting,
                    &mut engine,
                    checkpoint_interval_ms,
                    on_rejection,
                    on_checkpoint,
                );
                (driven.map(|_| ()), counting.events)
            }
        }
    } else {
        let mut engine = MarketStateEngine::new();
        let mut counting = Counting {
            inner: &mut provider,
            events: 0,
        };
        let driven = mie_app::equivalence::drive_checkpointed(
            &mut counting,
            &mut engine,
            checkpoint_interval_ms,
            on_rejection,
            on_checkpoint,
        );
        (driven.map(|_| ()), counting.events)
    };
    let mut error = driven.err().map(|failed| failed.to_string());
    drop(provider);
    shutdown.store(true, Ordering::Relaxed);

    let summary = match handle.join() {
        Ok((writer, summary)) => match writer.close() {
            Ok(files) => {
                if !files.is_empty() {
                    log("sealed", json!({"files": files_json(&files)}));
                }
                Some(summary)
            }
            Err(e) => {
                error.get_or_insert(format!("close the raw store: {e}"));
                Some(summary)
            }
        },
        Err(e) => {
            error.get_or_insert(e.to_string());
            Some(*e.summary)
        }
    };
    let outcome = IngestOutcome {
        run_id,
        events,
        domain_rejections,
        summary,
        error,
    };
    let stats = outcome.summary.as_ref().map(|s| &s.stats);
    let normalize_errors: u64 = stats.map_or(0, |s| {
        s.pipeline
            .streams
            .values()
            .map(|p| p.normalize_errors)
            .sum()
    });
    log(
        "run_end",
        json!({
            "exit_code": outcome.exit_code(),
            "error": outcome.error,
            "events": outcome.events,
            "domain_rejections": outcome.domain_rejections,
            "records": stats.map_or(0, |s| s.records),
            "time_fallbacks": stats.map_or(0, |s| s.time_fallbacks),
            "normalize_errors": normalize_errors,
            "sealed_files": stats.map_or(0, |s| s.sealed_files),
            "streams": stats.map_or(json!({}), |s| pipeline_json(&s.pipeline)),
        }),
    );
    let mut journal = journal.lock().unwrap_or_else(|p| p.into_inner());
    journal.sync();
    if let Some(write_error) = journal.write_error() {
        eprintln!("mie ingest: journal write failed: {write_error}");
    }
    Ok(outcome)
}

/// The largest sealed event time per stream within the lookback.
fn seeds(
    store: &ParquetRawStore,
    config: &IngestConfig,
    streams: &[BinanceStream],
    now_ms: i64,
) -> Result<BTreeMap<BinanceStream, EventTime>, String> {
    let mut keys = BTreeSet::new();
    let mut by_name = BTreeMap::new();
    for &stream in streams {
        let key = RawStreamKey::new(
            &config.instrument.source,
            &config.instrument.symbol,
            stream.raw_name(),
        )
        .map_err(|e| e.to_string())?;
        by_name.insert(stream.raw_name().to_owned(), stream);
        keys.insert(key);
    }
    // Open klines are stored at their future close time, so look ahead too.
    let window = ReplayWindow {
        start: EventTime::from_millis(now_ms.saturating_sub(SEED_LOOKBACK_MS).max(0)),
        end: EventTime::from_millis(now_ms.saturating_add(DAY_MS)),
    };
    let selection = RawSelection::new(keys, window).map_err(|e| e.to_string())?;
    let dataset = store
        .select(&selection)
        .map_err(|e| format!("read seeds from the raw store: {e}"))?;
    let mut seeds = BTreeMap::new();
    for file in dataset.files {
        if let Some(&stream) = by_name.get(file.stream.stream()) {
            let seed = seeds.entry(stream).or_insert(file.max_event_time);
            *seed = (*seed).max(file.max_event_time);
        }
    }
    Ok(seeds)
}

// ---------------------------------------------------------------------------
// The warm start of a chain run (ADR-052 D3, D7, D13).

/// What the warm-up hands over: the engine after the prefix and where the
/// prefix ends.
struct WarmUp {
    engine: MarketStateEngine,
    tail: ChainTail,
    origin: EventTime,
    cutover: EventTime,
    dataset: DatasetVersion,
    stream: EventStreamHash,
    rejections: u64,
    /// The prefix's restarts after uncrossable holes (ADR-052 D4).
    restarts: Vec<EventTime>,
}

/// Runs the hook it holds when dropped, so it also runs when the warm-up
/// panics.
struct AfterHook(Option<Box<dyn FnOnce() + Send>>);

impl Drop for AfterHook {
    fn drop(&mut self) {
        if let Some(hook) = self.0.take() {
            hook();
        }
    }
}

/// The error of a chain run without `[history] max_run_records`.
const NOT_CONFIGURED: &str = "memory guard (ADR-052 D13): [history] max_run_records is not \
                              configured, so the warm-up cannot bound what it holds in memory";

/// Spawns the `mie-warm-up` thread of run `run_id` (module docs), with the
/// memory guard's `max_run_records`. Once the guard knows the largest run
/// the warm-up will replay, the thread lowers `queue_limit` from `max` to
/// what `max` leaves beside that run (ADR-052 D13): the replayed run's
/// records and the queued ones together never exceed `max`.
fn spawn_warm_up(
    config: &IngestConfig,
    run_id: &str,
    cancel: &Arc<AtomicBool>,
    queue_limit: &Arc<AtomicU64>,
    max: u64,
    hold: WarmUpHold,
) -> Result<JoinHandle<Result<WarmUp, String>>, String> {
    let config = config.clone();
    let run_id = run_id.to_owned();
    let cancel = Arc::clone(cancel);
    let queue_limit = Arc::clone(queue_limit);
    thread::Builder::new()
        .name("mie-warm-up".to_owned())
        .spawn(move || {
            let _after = AfterHook(hold.after);
            if let Some(before) = hold.before {
                before();
            }
            warm_up(&config, &run_id, &cancel, &queue_limit, max)
        })
        .map_err(|e| format!("spawn the warm-up thread: {e}"))
}

/// The warm-up of run `run_id`: the memory guard, which also sets the
/// queue's limit, then the history chain through its predecessor into a
/// fresh engine.
fn warm_up(
    config: &IngestConfig,
    run_id: &str,
    cancel: &AtomicBool,
    queue_limit: &AtomicU64,
    max: u64,
) -> Result<WarmUp, String> {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let largest = memory_guard(config, &store, run_id, max)?;
    queue_limit.store(max - largest, Ordering::SeqCst);
    let chain: HistoryChain<'_> = history_chain(config, &store)?;
    let Replay { stream, dataset } = chain.before_run(run_id).map_err(|e| e.to_string())?;
    let mut hashing = HashingProvider::new(Cancellable {
        inner: stream,
        cancel,
    });
    let mut engine = MarketStateEngine::new();
    let mut rejections = 0_u64;
    let drive = drive_chain(&mut hashing, &mut engine, |_| rejections += 1)
        .map_err(|e| format!("replay the history chain through the predecessor: {e}"))?;
    let stream_hash = hashing.hash();
    let tail = hashing
        .into_inner()
        .inner
        .tail()
        .ok_or("the history chain's prefix did not end")?;
    Ok(WarmUp {
        engine,
        tail,
        origin: chain.origin(),
        cutover: chain.cutover(),
        dataset,
        stream: stream_hash,
        rejections,
        restarts: drive.restarts,
    })
}

/// The memory guard (ADR-052 D13): the largest chain run before `run_id`
/// must not hold more records than `max` (`[history] max_run_records`),
/// because the warm-up holds one run's records and output in memory at a
/// time. A run that ended cleanly counts its `run_end` records; any other
/// counts the rows of the sealed files of its streams that overlap its
/// ADR-039 D2 extent, an upper bound (adjacent runs share files). Returns
/// that largest count (0 without a chain run before `run_id`).
fn memory_guard(
    config: &IngestConfig,
    store: &ParquetRawStore,
    run_id: &str,
    max: u64,
) -> Result<u64, String> {
    let history = config
        .history
        .as_ref()
        .ok_or("memory guard (ADR-052 D13): no [history] section")?;
    let cutover = history.cutover_ms().map_err(|e| e.to_string())?;
    let source = &config.instrument.source;
    let symbol = &config.instrument.symbol;
    let mut runs: Vec<LiveRun> = read_runs(&config.paths.journal, source)?
        .iter()
        .map(|run| run.live_run())
        .filter(|run| run.symbol == *symbol)
        .collect();
    runs.sort_by(|a, b| (a.started_at_ms, &a.run_id).cmp(&(b.started_at_ms, &b.run_id)));
    let current = runs
        .iter()
        .position(|run| run.run_id == run_id)
        .ok_or_else(|| format!("run {run_id} is not in the journal"))?;
    let mut largest: Option<(&LiveRun, u64)> = None;
    for (index, run) in runs[..current].iter().enumerate() {
        if run.started_at_ms < cutover {
            continue;
        }
        let records = match run.clean_records {
            Some(records) => records,
            None => {
                // Not the last run: its successor's start ends it (D2).
                let end = run
                    .ended_at_ms
                    .unwrap_or(runs[index + 1].started_at_ms)
                    .saturating_add(RUN_MARGIN_MS);
                sealed_rows(store, source, symbol, run, end)?
            }
        };
        if largest.is_none_or(|(_, most)| records > most) {
            largest = Some((run, records));
        }
    }
    match largest {
        Some((run, records)) if records > max => Err(format!(
            "memory guard (ADR-052 D13): run {} holds {records} records, more than [history] \
             max_run_records {max}",
            run.run_id
        )),
        _ => Ok(largest.map_or(0, |(_, records)| records)),
    }
}

/// The rows of the sealed files of `run`'s streams that overlap
/// `[start − RUN_MARGIN_MS, end)`.
fn sealed_rows(
    store: &ParquetRawStore,
    source: &str,
    symbol: &str,
    run: &LiveRun,
    end: i64,
) -> Result<u64, String> {
    let keys = run
        .streams
        .iter()
        .map(|s| RawStreamKey::new(source, symbol, s.raw_name()))
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|e| e.to_string())?;
    let window = ReplayWindow {
        start: EventTime::from_millis(run.started_at_ms.saturating_sub(RUN_MARGIN_MS)),
        end: EventTime::from_millis(end),
    };
    let selection = RawSelection::new(keys, window).map_err(|e| e.to_string())?;
    let dataset = store
        .select(&selection)
        .map_err(|e| format!("memory guard (ADR-052 D13): {e}"))?;
    Ok(dataset.files.iter().map(|file| file.rows).sum())
}

/// A provider that fails once `cancel` is set: the warm-up stops at the
/// next event when the capture ends first.
struct Cancellable<'a, P> {
    inner: P,
    cancel: &'a AtomicBool,
}

impl<P: MarketDataProvider> MarketDataProvider for Cancellable<'_, P> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        if self.cancel.load(Ordering::Relaxed) {
            return Err(ProviderError::Source(
                "the run ended before the warm-up finished".to_owned(),
            ));
        }
        self.inner.next_event()
    }
}

/// The core's provider of a chain run after the warm-up: the queued
/// records, then the live ones, pushed through the run's
/// [`RunContinuation`]; `finish` at the live feed's end.
struct RecordFeed<'a> {
    queue: VecDeque<(BinanceStream, RawRecord)>,
    live: &'a mut BinanceLiveProvider,
    continuation: RunContinuation,
    out: VecDeque<MarketEvent>,
    ended: bool,
    /// Called once, when the queue's events have all been delivered and the
    /// first live record is awaited.
    on_drained: Option<Box<dyn FnOnce() + 'a>>,
}

impl MarketDataProvider for RecordFeed<'_> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        loop {
            if let Some(event) = self.out.pop_front() {
                return Ok(Some(event));
            }
            if self.ended {
                return Ok(None);
            }
            if let Some((stream, record)) = self.queue.pop_front() {
                self.out.extend(self.continuation.push(stream, &record));
                continue;
            }
            if let Some(drained) = self.on_drained.take() {
                drained();
            }
            match self.live.next_record()? {
                Some((stream, record)) => {
                    self.out.extend(self.continuation.push(stream, &record));
                }
                None => {
                    self.out.extend(self.continuation.finish());
                    self.ended = true;
                }
            }
        }
    }
}

/// The ids of ADR-041's *What the run cannot cover* features still warming
/// up in `state`, in a fixed order: the long warm-ups the warm start exists
/// for (ADR-052 D6). Structure counts per timeframe, swings then levels.
fn warming_up(state: &MarketState) -> Vec<String> {
    fn warming<T>(value: &FeatureValue<T>) -> bool {
        matches!(value, FeatureValue::WarmingUp { .. })
    }
    let mut ids = Vec::new();
    let mut check = |key: &FeatureKey, warm: bool| {
        if warm {
            ids.push(key.to_string());
        }
    };
    check(&catalog::VOLATILITY_ATR_1H_V1.key, warming(&state.atr));
    check(
        &catalog::VOLATILITY_REGIME_1H_V1.key,
        warming(&state.regime),
    );
    check(
        &catalog::PROFILE_VOLUME_COMPOSITE_5D_V1.key,
        warming(&state.profile.composite_5d),
    );
    check(
        &catalog::PROFILE_VOLUME_COMPOSITE_7D_V1.key,
        warming(&state.profile.composite_7d),
    );
    for ((timeframe, swing), (_, levels)) in catalog::STRUCTURE_SWING
        .iter()
        .zip(catalog::STRUCTURE_LEVELS.iter())
    {
        let structure = state.structure.get(*timeframe);
        check(&swing.key, structure.is_some_and(|s| warming(&s.swings)));
        check(&levels.key, structure.is_some_and(|s| warming(&s.levels)));
    }
    check(
        &catalog::DERIVATIVES_FUNDING_SETTLED_V1.key,
        warming(&state.derivatives.funding_settled),
    );
    check(
        &catalog::LOCATION_AUCTION_PRIOR_DAY_V1.key,
        warming(&state.location.auction),
    );
    ids
}

/// The text of a panic payload.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a non-text panic".to_owned())
}
