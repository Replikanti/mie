//! `mie archive-import`, `mie archive-verify` and `mie archive-kline-check`
//! (ADR-034): the archive backfill wired to the raw Parquet store.
//!
//! The importer writes through the writer of the fixed source
//! `binance-archive` with **day-span parts** (D4): `max_event_span_ms` is
//! one day, the row and byte limits stay the store's defaults. The live
//! capture's one-hour span would split a metrics or klines day into ~24
//! tiny files, and the unsorted metrics rows would fragment it further.

use crate::config::ArchiveConfig;
use mie_adapter_binance::archive::catalog::{DAY_MS, day_label};
use mie_adapter_binance::archive::fetch::{FetchPolicy, Fetcher};
use mie_adapter_binance::archive::import::{ImportOptions, ImportSummary, Importer};
use mie_adapter_binance::archive::ledger::{ImportLedger, LedgerDir};
use mie_adapter_binance::archive::normalize::{parse, record_time};
use mie_adapter_binance::archive::window::ArchiveWindowProvider;
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_binance::transport::{Clock, HttpDownload, HttpGet};
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_domain::event::MarketEvent;
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{
    RawRecord, RawRecordSink, RawRecordSource, RawSelection, RawStoreError, RawStreamKey,
    SealedFile,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// Longest silence inside a trade stream's day that `archive-verify`
/// accepts without listing it as a hole.
pub const HOLE_MS: i64 = 60_000;

/// Normalize-error samples printed per stream.
const MAX_SAMPLES: usize = 10;

/// The writer policy of the archive source (ADR-034 D4): day-span parts.
pub fn rotation_policy() -> RotationPolicy {
    RotationPolicy {
        max_event_span_ms: DAY_MS,
        ..RotationPolicy::default()
    }
}

/// The I/O of an import; real ones in `main`, fakes in tests.
pub struct ArchiveTransports {
    /// `.CHECKSUM` requests.
    pub http: Arc<dyn HttpGet>,
    /// Zip downloads.
    pub download: Arc<dyn HttpDownload>,
    /// Pacing and backoff.
    pub clock: Arc<dyn Clock>,
}

/// One `archive-import` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportRequest {
    /// First UTC day (days since 1970-01-01).
    pub from_day: i64,
    /// Last UTC day, inclusive.
    pub to_day: i64,
    /// Streams replacing the configured ones (`--streams`).
    pub streams: Option<Vec<ArchiveStream>>,
    /// Probe the checksums only.
    pub dry_run: bool,
}

/// A sink for dry runs: nothing may be written.
struct NoWrites;

impl RawRecordSink for NoWrites {
    fn append(&mut self, stream: &RawStreamKey, _: RawRecord) -> Result<(), RawStoreError> {
        Err(RawStoreError::Invalid(format!(
            "dry run: no record of {stream} may be written"
        )))
    }

    fn seal_all(&mut self) -> Result<Vec<SealedFile>, RawStoreError> {
        Ok(Vec::new())
    }
}

fn fetch_policy(config: &ArchiveConfig) -> FetchPolicy {
    let a = &config.archive;
    FetchPolicy {
        request_interval: Duration::from_millis(a.request_interval_ms),
        max_attempts: a.max_attempts,
        backoff_initial: Duration::from_millis(a.backoff_initial_ms),
        backoff_max: Duration::from_millis(a.backoff_max_ms),
    }
}

/// The import options of `request` under `config`.
///
/// # Errors
///
/// A description when the configured streams do not validate.
pub fn import_options(
    config: &ArchiveConfig,
    request: &ImportRequest,
) -> Result<ImportOptions, String> {
    let streams = match &request.streams {
        Some(streams) => streams.clone(),
        None => config.streams().map_err(|e| e.to_string())?,
    };
    Ok(ImportOptions {
        symbol: config.instrument.symbol.clone(),
        base_url: config.archive.base_url.clone(),
        ledger_dir: config.paths.import_ledger.clone(),
        staging_dir: config.paths.staging.clone(),
        streams,
        from_day: request.from_day,
        to_day: request.to_day,
        dry_run: request.dry_run,
    })
}

/// Runs `archive-import`, printing one line per file and the summary to
/// `out`.
///
/// # Errors
///
/// A description when the store cannot be opened or the run stops on an
/// integrity, store or ledger failure. Files reported before stay imported.
pub fn import(
    config: &ArchiveConfig,
    request: &ImportRequest,
    transports: &ArchiveTransports,
    shutdown: &AtomicBool,
    out: &mut dyn Write,
) -> Result<ImportSummary, String> {
    let options = import_options(config, request)?;
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let fetcher = Fetcher::new(
        transports.http.as_ref(),
        transports.download.as_ref(),
        transports.clock.as_ref(),
        fetch_policy(config),
    );
    if request.dry_run {
        let mut sink = NoWrites;
        let summary = Importer::new(&mut sink, &store, fetcher, options, shutdown)
            .run(&mut |report| {
                let _ = writeln!(out, "{report}");
            })
            .map_err(|e| e.to_string())?;
        let _ = writeln!(out, "{summary}");
        return Ok(summary);
    }
    let mut writer = store
        .writer(ARCHIVE_SOURCE, rotation_policy())
        .map_err(|e| format!("open the raw store: {e}"))?;
    let recovery = writer.recovery().clone();
    if !recovery.is_clean() {
        let _ = writeln!(
            out,
            "recovery: rolled forward {:?}, discarded {:?}",
            recovery.rolled_forward, recovery.discarded
        );
    }
    let result =
        Importer::new(&mut writer, &store, fetcher, options, shutdown).run(&mut |report| {
            let _ = writeln!(out, "{report}");
        });
    let closed = writer.close();
    let summary = result.map_err(|e| e.to_string())?;
    closed.map_err(|e| format!("close the raw store: {e}"))?;
    let _ = writeln!(out, "{summary}");
    Ok(summary)
}

/// The window of the inclusive day range.
fn day_window(from_day: i64, to_day: i64) -> ReplayWindow {
    ReplayWindow {
        start: EventTime::from_millis(from_day * DAY_MS),
        end: EventTime::from_millis((to_day + 1) * DAY_MS),
    }
}

fn key(config: &ArchiveConfig, stream: ArchiveStream) -> Result<RawStreamKey, String> {
    RawStreamKey::new(ARCHIVE_SOURCE, &config.instrument.symbol, stream.raw_name())
        .map_err(|e| e.to_string())
}

/// The sealed files of `key` in the partitions of the inclusive day range.
fn sealed(
    store: &ParquetRawStore,
    key: &RawStreamKey,
    from_day: i64,
    to_day: i64,
) -> Result<Vec<SealedFile>, String> {
    let selection = RawSelection::new(BTreeSet::from([key.clone()]), day_window(from_day, to_day))
        .map_err(|e| e.to_string())?;
    store
        .select(&selection)
        .map(|dataset| dataset.files)
        .map_err(|e| e.to_string())
}

/// The day of a ledger file path's `date=` segment.
fn path_day(relative_path: &str) -> Option<i64> {
    relative_path
        .split('/')
        .find_map(|segment| segment.strip_prefix("date="))
        .and_then(mie_adapter_binance::archive::catalog::parse_day)
}

/// What `archive-verify` found for one stream.
#[derive(Default)]
struct StreamCheck {
    files: u64,
    rows: u64,
    normalize_errors: u64,
    samples: Vec<String>,
    failures: Vec<String>,
    days: Vec<String>,
    breaks: Vec<String>,
    holes: Vec<String>,
}

/// Runs `archive-verify` over the inclusive day range, printing a report to
/// `out`. Returns whether it passed: every ledgered file is in the store,
/// hash-verified on read, with the ledger's row count, every record is
/// filed at its ordering time and normalizes, no sealed file is missing
/// from the ledgers, and every configured stream has a ledger for every
/// period of the range — a range that was not (fully) imported, an archive
/// file that was never published included, is no evidence and fails. Id
/// breaks and holes are listed, not failed: they feed the availability
/// matrix.
///
/// # Errors
///
/// A description when the ledgers or the store cannot be listed.
pub fn verify(
    config: &ArchiveConfig,
    from_day: i64,
    to_day: i64,
    out: &mut dyn Write,
) -> Result<bool, String> {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let ledgers = LedgerDir::new(&config.paths.import_ledger);
    let configured = config.streams().map_err(|e| e.to_string())?;
    let mut pass = true;
    if let Some(pending) = ledgers.pending().map_err(|e| e.to_string())? {
        pass = false;
        let _ = writeln!(out, "FAIL: import of {} is pending", pending.archive);
    }
    for stream in ArchiveStream::ALL {
        let all = ledgers.list(stream).map_err(|e| e.to_string())?;
        let in_range: Vec<&ImportLedger> = all
            .iter()
            .filter(|l| l.period.first_day() <= to_day && l.period.end_day() > from_day)
            .collect();
        if in_range.is_empty() && !configured.contains(&stream) {
            continue;
        }
        let key = key(config, stream)?;
        let mut check = check_stream(
            config, &store, stream, &key, &all, &in_range, from_day, to_day,
        )?;
        if configured.contains(&stream) {
            for period in stream.period_kind().periods(from_day, to_day) {
                if !all.iter().any(|ledger| ledger.period == period) {
                    check.failures.push(format!(
                        "not imported: {}",
                        stream.file_name(&config.instrument.symbol, period)
                    ));
                }
            }
        }
        let selection = RawSelection::new(BTreeSet::from([key]), day_window(from_day, to_day))
            .map_err(|e| e.to_string())?;
        let version = store.select(&selection).map_err(|e| e.to_string())?.version;
        let ok = check.failures.is_empty() && check.normalize_errors == 0;
        pass &= ok;
        let _ = writeln!(
            out,
            "stream {stream}: {} files {}, rows {}, normalize errors {}, id breaks {}, \
             holes>{}s {}, dataset {version}",
            if ok { "OK" } else { "FAIL" },
            check.files,
            check.rows,
            check.normalize_errors,
            check.breaks.len(),
            HOLE_MS / 1_000,
            check.holes.len()
        );
        for line in check
            .failures
            .iter()
            .chain(&check.samples)
            .chain(&check.days)
            .chain(&check.breaks)
            .chain(&check.holes)
        {
            let _ = writeln!(out, "  {line}");
        }
    }
    let _ = writeln!(out, "{}", if pass { "PASS" } else { "FAIL" });
    Ok(pass)
}

#[allow(clippy::too_many_arguments)]
fn check_stream(
    config: &ArchiveConfig,
    store: &ParquetRawStore,
    stream: ArchiveStream,
    key: &RawStreamKey,
    all: &[ImportLedger],
    in_range: &[&ImportLedger],
    from_day: i64,
    to_day: i64,
) -> Result<StreamCheck, String> {
    let mut check = StreamCheck::default();
    let symbol = &config.instrument.symbol;
    let is_trades = matches!(stream, ArchiveStream::AggTrades | ArchiveStream::Trades);

    // Ledger ↔ store: no sealed file outside every ledger.
    let ledgered: BTreeSet<&str> = all
        .iter()
        .flat_map(|l| l.files.iter().map(|f| f.relative_path.as_str()))
        .collect();
    for file in sealed(store, key, from_day, to_day + 1)? {
        if !ledgered.contains(file.relative_path.as_str()) {
            check
                .failures
                .push(format!("orphan {} is in no ledger", file.relative_path));
        }
    }

    let mut last_trade: Option<(u64, i64)> = None;
    for ledger in in_range {
        let mut by_day: BTreeMap<i64, Vec<SealedFile>> = BTreeMap::new();
        let mut records = Vec::new();
        let mut ok = true;
        for listed in &ledger.files {
            let Some(day) = path_day(&listed.relative_path) else {
                check
                    .failures
                    .push(format!("ledger path {}", listed.relative_path));
                ok = false;
                continue;
            };
            if let std::collections::btree_map::Entry::Vacant(entry) = by_day.entry(day) {
                entry.insert(sealed(store, key, day, day)?);
            }
            let found = by_day[&day]
                .iter()
                .find(|f| f.relative_path == listed.relative_path && f.sha256 == listed.sha256);
            match found {
                Some(file) => match store.read(file) {
                    Ok(read) => records.extend(read),
                    Err(e) => {
                        check.failures.push(format!("{}: {e}", file.relative_path));
                        ok = false;
                    }
                },
                None => {
                    check.failures.push(format!(
                        "{} of {} is missing or differs",
                        listed.relative_path, ledger.archive
                    ));
                    ok = false;
                }
            }
        }
        check.files += ledger.files.len() as u64;
        if ok && records.len() as u64 != ledger.rows {
            check.failures.push(format!(
                "{}: {} stored rows, ledger says {}",
                ledger.archive,
                records.len(),
                ledger.rows
            ));
        }
        check.rows += records.len() as u64;

        let mut times = Vec::with_capacity(records.len());
        for record in &records {
            let time = record.event_time.as_millis();
            times.push(time);
            if record_time(stream, &record.payload).ok() != Some(record.event_time) {
                check.failures.push(format!(
                    "{}: record at {time} ms is not filed at its ordering time",
                    ledger.archive
                ));
            }
            match parse(stream, symbol, &record.payload) {
                Ok(Some(MarketEvent::Trade(trade))) if is_trades => {
                    if let Some((id, at)) = last_trade
                        && trade.trade_id != id + 1
                    {
                        check.breaks.push(format!(
                            "id break {id} -> {} over [{at}, {}] ms",
                            trade.trade_id,
                            trade.time.as_millis()
                        ));
                    }
                    last_trade = Some((trade.trade_id, trade.time.as_millis()));
                }
                Ok(_) => {}
                Err(e) => {
                    check.normalize_errors += 1;
                    if check.samples.len() < MAX_SAMPLES {
                        check.samples.push(format!(
                            "normalize error in {} at {time} ms: {e}: {}",
                            ledger.archive,
                            String::from_utf8_lossy(&record.payload)
                        ));
                    }
                }
            }
        }
        times.sort_unstable();
        if is_trades {
            for pair in times.windows(2) {
                if pair[1] - pair[0] > HOLE_MS {
                    check
                        .holes
                        .push(format!("hole [{}, {}] ms", pair[0], pair[1]));
                }
            }
        }
        let (first, last) = (times.first(), times.last());
        check.days.push(match (first, last) {
            (Some(first), Some(last)) => format!(
                "{} {}: rows {}, first {first} ms, last {last} ms",
                ledger.period,
                stream,
                times.len()
            ),
            _ => format!("{} {}: rows 0", ledger.period, stream),
        });
    }
    Ok(check)
}

/// Runs `archive-kline-check` over the inclusive day range with trades from
/// `trade_stream`, printing the report and the dataset versions to `out`.
/// Returns whether at least one complete bar was compared and every
/// compared bar matched its kline: a window without the trade stream or
/// without klines is no evidence and fails.
///
/// # Errors
///
/// A description when the window cannot be loaded or the domain rejects an
/// event.
pub fn kline_check(
    config: &ArchiveConfig,
    from_day: i64,
    to_day: i64,
    trade_stream: ArchiveStream,
    out: &mut dyn Write,
) -> Result<bool, String> {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let window = day_window(from_day, to_day);
    let mut provider =
        ArchiveWindowProvider::open(&store, &config.instrument.symbol, trade_stream, window)
            .map_err(|e| e.to_string())?;
    let versions = provider.dataset_versions().to_vec();
    let mut engine = MarketStateEngine::new();
    let report = mie_app::kline_check::cross_check_klines(&mut provider, &mut engine)
        .map_err(|e| e.to_string())?;
    let _ = writeln!(
        out,
        "window {} .. {} (UTC days, inclusive), trades from {trade_stream}",
        day_label(from_day),
        day_label(to_day)
    );
    for (name, version) in &versions {
        let _ = writeln!(out, "dataset {name} {version}");
    }
    let _ = writeln!(out, "{report}");
    let pass = report.compared > 0 && report.all_matched();
    if report.compared == 0 {
        let _ = writeln!(
            out,
            "FAIL: no complete bar was compared; import {trade_stream} and klines for the \
             window and the minute around it"
        );
    } else {
        let _ = writeln!(out, "{}", if pass { "PASS" } else { "FAIL" });
    }
    Ok(pass)
}
