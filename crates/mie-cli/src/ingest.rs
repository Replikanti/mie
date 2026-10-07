//! `mie ingest`: live Binance capture into the raw store, feeding the core.
//!
//! 1. Open the raw-store writer of the source (lock + crash recovery) and
//!    journal its recovery report.
//! 2. Seed each stream with the largest event time sealed in the last
//!    7 days, so the run opens with a restart gap per stream.
//! 3. Start the live capture and drive the core on this thread through
//!    [`mie_app::drive_tolerant`], the drive replay uses too (ADR-019,
//!    ADR-039 D9). An event the engine rejects is journaled and counted,
//!    and driving resumes: the engine leaves its state untouched on a
//!    rejection, and a soak must not stop on one. A provider failure ends
//!    the run.
//! 4. On shutdown (SIGINT/SIGTERM sets the flag), join the capture, close
//!    the writer (sealing everything) and journal a `run_end` summary.

use crate::config::IngestConfig;
use crate::journal::{Journal, JournalObserver, files_json, pipeline_json};
use mie_adapter_binance::transport::{Clock, HttpGet, WsConnector};
use mie_adapter_binance::{BinanceStream, CaptureSummary, run_id, start};
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_domain::event::MarketEvent;
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::outbound::{MarketDataProvider, ProviderError, ReplayWindow};
use mie_ports::raw::{RawRecordSource, RawSelection, RawStreamKey};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

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
    let clock = Arc::clone(&transports.clock);
    let now = || clock.now_utc_ns();
    let run_id = run_id(now());
    let journal = Arc::new(Mutex::new(Journal::open(&config.paths.journal, &run_id)?));
    let log = |kind: &str, fields: Value| {
        let mut journal = journal.lock().unwrap_or_else(|p| p.into_inner());
        journal.write(now(), kind, fields);
    };

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
    let seeds = seeds(&store, config, &streams, now().div_euclid(1_000_000))?;
    log(
        "run_start",
        json!({
            "symbol": config.instrument.symbol,
            "source": config.instrument.source,
            "streams": streams.iter().map(|s| s.raw_name()).collect::<Vec<_>>(),
            "hold_back_ms": config.capture.hold_back_ms,
            "oi_retime_ms": config.capture.oi_retime_allowance_ms,
            "seal_interval_secs": config.capture.seal_interval_secs,
            "seeds": seeds
                .iter()
                .map(|(s, t)| (s.raw_name().to_owned(), json!(t.as_millis())))
                .collect::<serde_json::Map<_, _>>(),
        }),
    );

    // Durable before the first record is sealed: replay needs the run's
    // parameters for every record it seals (ADR-039 D2).
    journal.lock().unwrap_or_else(|p| p.into_inner()).sync();

    let live = config
        .live_config(&run_id, seeds)
        .map_err(|e| e.to_string())?;
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

    let mut engine = MarketStateEngine::new();
    let mut counting = Counting {
        inner: &mut provider,
        events: 0,
    };
    let mut domain_rejections = 0_u64;
    let driven = mie_app::drive_tolerant(&mut counting, &mut engine, |rejected| {
        domain_rejections += 1;
        log("domain_rejection", json!({"error": rejected.to_string()}));
    });
    let mut error = driven.err().map(|failed| failed.to_string());
    let events = counting.events;
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
