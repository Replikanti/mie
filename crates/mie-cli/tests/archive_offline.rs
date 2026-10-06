//! The archive backfill composition offline: a fake archive serving zips
//! built in the test, a real Parquet raw store and ledger in a temp dir, a
//! fake clock. No network anywhere.

mod common;

use common::TempDir;
use mie_adapter_binance::archive::catalog::{DAY_MS, Period, parse_day};
use mie_adapter_binance::archive::fetch::Fetcher;
use mie_adapter_binance::archive::import::{ImportError, ImportSummary, Importer};
use mie_adapter_binance::archive::ledger::{LedgerDir, Pending};
use mie_adapter_binance::archive::normalize::record_time;
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_binance::transport::{Clock, HttpDownload, HttpGet};
use mie_adapter_parquet::{ParquetRawStore, RawWriter, RotationPolicy};
use mie_cli::archive::{self, ArchiveTransports, ImportRequest};
use mie_cli::config::ArchiveConfig;
use mie_domain::bars::Timeframe;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{
    DatasetVersion, RawRecord, RawRecordSink, RawRecordSource, RawSelection, RawStoreError,
    RawStreamKey, SealedFile,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BASE: &str = "https://archive.invalid";
const SYMBOL: &str = "BTCUSDT";

fn day(text: &str) -> i64 {
    parse_day(text).unwrap()
}

// ---------------------------------------------------------------- fakes

/// Serves published zips and their `.CHECKSUM`s; 404 for anything else.
#[derive(Default)]
struct FakeArchive {
    zips: Mutex<BTreeMap<String, Vec<u8>>>,
    downloads: AtomicU64,
}

impl FakeArchive {
    fn publish(&self, stream: ArchiveStream, period: Period, csv: &str) {
        let path = stream.archive_path(SYMBOL, period);
        let entry = stream.file_name(SYMBOL, period).replace(".zip", ".csv");
        self.zips
            .lock()
            .unwrap()
            .insert(format!("{BASE}/{path}"), zip_of(&entry, csv.as_bytes()));
    }

    fn downloads(&self) -> u64 {
        self.downloads.load(Ordering::Relaxed)
    }
}

impl HttpGet for FakeArchive {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        let zip_url = url
            .strip_suffix(".CHECKSUM")
            .expect("only checksums are GET");
        let zips = self.zips.lock().unwrap();
        Ok(match zips.get(zip_url) {
            Some(zip) => {
                let name = zip_url.rsplit('/').next().unwrap();
                (200, format!("{}  {name}\n", sha256(zip)).into_bytes())
            }
            None => (404, b"Not Found".to_vec()),
        })
    }
}

impl HttpDownload for FakeArchive {
    fn download(&self, url: &str, sink: &mut dyn Write) -> Result<u16, String> {
        self.downloads.fetch_add(1, Ordering::Relaxed);
        match self.zips.lock().unwrap().get(url) {
            Some(zip) => {
                sink.write_all(zip).map_err(|e| e.to_string())?;
                Ok(200)
            }
            None => Ok(404),
        }
    }
}

/// Monotonic time that only sleeping advances.
#[derive(Default)]
struct FakeClock(Mutex<u64>);

impl Clock for FakeClock {
    fn now_utc_ns(&self) -> i64 {
        *self.0.lock().unwrap() as i64
    }

    fn monotonic_ns(&self) -> u64 {
        *self.0.lock().unwrap()
    }

    fn sleep(&self, duration: Duration) {
        *self.0.lock().unwrap() += duration.as_nanos() as u64;
    }
}

fn zip_of(entry: &str, data: &[u8]) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    writer.start_file(entry, options).unwrap();
    writer.write_all(data).unwrap();
    writer.finish().unwrap().into_inner()
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A sink that fails after `limit` appends, as a crash would.
struct FailAfter<'a> {
    inner: &'a mut RawWriter,
    left: u64,
}

impl RawRecordSink for FailAfter<'_> {
    fn append(&mut self, stream: &RawStreamKey, record: RawRecord) -> Result<(), RawStoreError> {
        if self.left == 0 {
            return Err(RawStoreError::Io("simulated crash".to_owned()));
        }
        self.left -= 1;
        self.inner.append(stream, record)
    }

    fn seal_all(&mut self) -> Result<Vec<SealedFile>, RawStoreError> {
        self.inner.seal_all()
    }
}

// ------------------------------------------------------- synthetic data

/// One synthetic trade: one constituent per aggregate.
#[derive(Clone, Copy)]
struct Tick {
    agg_id: u64,
    trade_id: u64,
    time: i64,
    price_tenths: i64,
    qty_thousandths: i64,
    maker: bool,
}

fn decimal(value: i64, places: u32) -> String {
    let scale = 10_i64.pow(places);
    format!(
        "{}.{:0width$}",
        value / scale,
        value % scale,
        width = places as usize
    )
}

/// A trade every `spacing_ms` over the day, ids continuing across days.
fn ticks(d: i64, spacing_ms: i64) -> Vec<Tick> {
    let per_day = DAY_MS / spacing_ms;
    (0..per_day)
        .map(|k| {
            let global = (d - day("2026-01-01")) * per_day + k;
            Tick {
                agg_id: 5_000_000 + global as u64,
                trade_id: 9_000_000 + global as u64,
                time: d * DAY_MS + k * spacing_ms + 7,
                price_tenths: 800_000 + (k * 37 + d) % 200,
                qty_thousandths: 1 + (k * 13) % 50,
                maker: k % 3 == 0,
            }
        })
        .collect()
}

fn agg_trades_csv(ticks: &[Tick]) -> String {
    let mut csv = format!("{}\n", ArchiveStream::AggTrades.expected_header());
    for t in ticks {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{}\n",
            t.agg_id,
            decimal(t.price_tenths, 1),
            decimal(t.qty_thousandths, 3),
            t.trade_id,
            t.trade_id,
            t.time,
            t.maker
        ));
    }
    csv
}

fn trades_csv(ticks: &[Tick]) -> String {
    let mut csv = format!("{}\n", ArchiveStream::Trades.expected_header());
    for t in ticks {
        csv.push_str(&format!(
            "{},{},{},0,{},{}\n",
            t.trade_id,
            decimal(t.price_tenths, 1),
            decimal(t.qty_thousandths, 3),
            t.time,
            t.maker
        ));
    }
    csv
}

/// Klines of `timeframe` consistent with `ticks`, one per bar with trades.
fn klines_csv(ticks: &[Tick], timeframe: Timeframe) -> String {
    let span = timeframe.millis();
    let mut bars: BTreeMap<i64, Vec<&Tick>> = BTreeMap::new();
    for t in ticks {
        bars.entry(t.time - t.time.rem_euclid(span))
            .or_default()
            .push(t);
    }
    let mut csv = format!("{}\n", ArchiveStream::Klines(timeframe).expected_header());
    for (open, trades) in bars {
        let prices: Vec<i64> = trades.iter().map(|t| t.price_tenths).collect();
        let volume: i64 = trades.iter().map(|t| t.qty_thousandths).sum();
        let buy: i64 = trades
            .iter()
            .filter(|t| !t.maker)
            .map(|t| t.qty_thousandths)
            .sum();
        csv.push_str(&format!(
            "{open},{},{},{},{},{},{},0,{},{},0,0\n",
            decimal(prices[0], 1),
            decimal(*prices.iter().max().unwrap(), 1),
            decimal(*prices.iter().min().unwrap(), 1),
            decimal(*prices.last().unwrap(), 1),
            decimal(volume, 3),
            open + span - 1,
            trades.len(),
            decimal(buy, 3),
        ));
    }
    csv
}

fn date_text(d: i64) -> String {
    mie_adapter_binance::archive::catalog::day_label(d)
}

/// Hourly samples in a shuffled order plus 23:55, which is ordered at the
/// next midnight.
fn metrics_csv(d: i64) -> String {
    let mut csv = format!("{}\n", ArchiveStream::Metrics.expected_header());
    let date = date_text(d);
    for i in 0..24 {
        let hour = (i * 7) % 24;
        csv.push_str(&format!(
            "{date} {hour:02}:00:00,{SYMBOL},{}.1230000000000000,1.0,1.0,1.0,1.0,1.0\n",
            90_000 + hour
        ));
    }
    csv.push_str(&format!(
        "{date} 23:55:00,{SYMBOL},90100.0000000000000000,1.0,1.0,1.0,1.0,1.0\n"
    ));
    csv
}

fn book_depth_csv(d: i64) -> String {
    let mut csv = format!("{}\n", ArchiveStream::BookDepth.expected_header());
    for s in [1, 31] {
        for pct in ["-1.00", "1.00"] {
            csv.push_str(&format!(
                "{} 00:00:{s:02},{pct},10.00000000,800000.00000000\n",
                date_text(d)
            ));
        }
    }
    csv
}

fn funding_csv(month: Period) -> String {
    let mut csv = format!("{}\n", ArchiveStream::FundingRate.expected_header());
    for d in month.first_day()..month.end_day() {
        for j in 0..3 {
            csv.push_str(&format!(
                "{},8,0.00010000\n",
                d * DAY_MS + j * 28_800_000 + j
            ));
        }
    }
    csv
}

/// Publishes every stream for the inclusive day range with trades every
/// `spacing_ms`; returns the CSV text per archive file name.
fn publish_all(
    archive: &FakeArchive,
    from: i64,
    to: i64,
    spacing_ms: i64,
) -> BTreeMap<String, String> {
    let mut published = BTreeMap::new();
    let mut put = |stream: ArchiveStream, period: Period, csv: String| {
        archive.publish(stream, period, &csv);
        published.insert(stream.file_name(SYMBOL, period), csv);
    };
    for d in from..=to {
        let ticks = ticks(d, spacing_ms);
        let period = Period::Day(d);
        put(ArchiveStream::AggTrades, period, agg_trades_csv(&ticks));
        put(ArchiveStream::Trades, period, trades_csv(&ticks));
        for timeframe in Timeframe::ALL {
            put(
                ArchiveStream::Klines(timeframe),
                period,
                klines_csv(&ticks, timeframe),
            );
        }
        put(ArchiveStream::Metrics, period, metrics_csv(d));
        put(ArchiveStream::BookDepth, period, book_depth_csv(d));
    }
    for month in ArchiveStream::FundingRate.period_kind().periods(from, to) {
        put(ArchiveStream::FundingRate, month, funding_csv(month));
    }
    published
}

// --------------------------------------------------------------- harness

fn config(dir: &Path, streams: &str) -> ArchiveConfig {
    ArchiveConfig::parse(&format!(
        r#"
[instrument]
symbol = "{SYMBOL}"
[paths]
raw_root = "{}"
import_ledger = "{}"
staging = "{}"
[archive]
base_url = "{BASE}"
streams = {streams}
"#,
        dir.join("raw").display(),
        dir.join("ledger").display(),
        dir.join("staging").display()
    ))
    .expect("valid test config")
}

const EVERY_STREAM: &str =
    r#"["aggTrades", "klines", "fundingRate", "metrics", "bookDepth", "trades"]"#;

fn transports(archive: &Arc<FakeArchive>) -> ArchiveTransports {
    ArchiveTransports {
        http: archive.clone(),
        download: archive.clone(),
        clock: Arc::new(FakeClock::default()),
    }
}

fn run_import(
    config: &ArchiveConfig,
    archive: &Arc<FakeArchive>,
    from: i64,
    to: i64,
    dry_run: bool,
) -> (ImportSummary, String) {
    let mut out = Vec::new();
    let request = ImportRequest {
        from_day: from,
        to_day: to,
        streams: None,
        dry_run,
    };
    let summary = archive::import(
        config,
        &request,
        &transports(archive),
        &AtomicBool::new(false),
        &mut out,
    )
    .expect("import runs");
    (summary, String::from_utf8(out).unwrap())
}

fn key(stream: ArchiveStream) -> RawStreamKey {
    RawStreamKey::new(ARCHIVE_SOURCE, SYMBOL, stream.raw_name()).unwrap()
}

fn files_of(config: &ArchiveConfig, stream: ArchiveStream) -> Vec<SealedFile> {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let selection = RawSelection::new(
        BTreeSet::from([key(stream)]),
        ReplayWindow {
            start: EventTime::from_millis(0),
            end: EventTime::from_millis(day("2030-01-01") * DAY_MS),
        },
    )
    .unwrap();
    store.select(&selection).unwrap().files
}

fn records_of(config: &ArchiveConfig, stream: ArchiveStream) -> Vec<RawRecord> {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    files_of(config, stream)
        .iter()
        .flat_map(|f| store.read(f).unwrap())
        .collect()
}

fn version(config: &ArchiveConfig) -> DatasetVersion {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let keys = ArchiveStream::ALL.into_iter().map(key).collect();
    let selection = RawSelection::new(
        keys,
        ReplayWindow {
            start: EventTime::from_millis(0),
            end: EventTime::from_millis(day("2030-01-01") * DAY_MS),
        },
    )
    .unwrap();
    store.select(&selection).unwrap().version
}

fn count_files(dir: &Path, suffix: &str) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .map(|e| e.unwrap().path())
        .map(|p| {
            if p.is_dir() {
                count_files(&p, suffix)
            } else {
                usize::from(p.to_string_lossy().ends_with(suffix))
            }
        })
        .sum()
}

fn data_rows(csv: &str) -> Vec<&str> {
    csv.lines().skip(1).collect()
}

// ----------------------------------------------------------------- tests

#[test]
fn three_days_of_every_stream_land_verbatim_under_the_archive_source() {
    let dir = TempDir::new("archive-import");
    let config = config(dir.path(), EVERY_STREAM);
    let archive = Arc::new(FakeArchive::default());
    let (from, to) = (day("2026-09-28"), day("2026-09-30"));
    let published = publish_all(&archive, from, to, 60_000);

    let (summary, out) = run_import(&config, &archive, from, to, false);
    // 3 days × (aggTrades, trades, 6 klines, metrics, bookDepth) + 1 month.
    assert_eq!(summary.imported, 31, "{out}");
    assert_eq!(summary.exit_code(), 0);
    assert_eq!(
        (summary.failed, summary.changed, summary.missing),
        (0, 0, 0)
    );
    assert_eq!(summary.downloads, 31);
    assert_eq!(count_files(&config.paths.import_ledger, ".import"), 31);
    assert!(!config.paths.import_ledger.join("PENDING").exists());
    assert_eq!(count_files(&config.paths.staging, ".zip"), 0);
    assert!(out.contains("imported BTCUSDT-aggTrades-2026-09-28.zip: 1440 rows, 1 files"));

    for stream in ArchiveStream::ALL {
        let mut expected: Vec<&str> = published
            .iter()
            .filter(|(name, _)| {
                stream
                    .period_kind()
                    .periods(from, to)
                    .iter()
                    .any(|p| stream.file_name(SYMBOL, *p) == **name)
            })
            .flat_map(|(_, csv)| data_rows(csv))
            .collect();
        let records = records_of(&config, stream);
        assert_eq!(
            summary.rows.get(stream.raw_name()).copied(),
            Some(expected.len() as u64),
            "{stream}"
        );
        for file in files_of(&config, stream) {
            assert!(
                file.relative_path.starts_with(&format!(
                    "source=binance-archive/instrument=BTCUSDT/stream={stream}/"
                )),
                "{}",
                file.relative_path
            );
        }
        let mut stored: Vec<&str> = Vec::new();
        for record in &records {
            assert_eq!(record.capture, None);
            let row = std::str::from_utf8(&record.payload).unwrap();
            assert_eq!(
                record_time(stream, &record.payload),
                Ok(record.event_time),
                "{row}"
            );
            stored.push(row);
        }
        expected.sort_unstable();
        stored.sort_unstable();
        assert_eq!(stored, expected, "{stream}: payloads are the CSV rows");
    }

    // The 23:55 metrics sample of the last day is filed under the next day.
    let metrics = files_of(&config, ArchiveStream::Metrics);
    assert!(
        metrics
            .iter()
            .any(|f| f.date == "2026-10-01" && f.rows == 1)
    );

    // Archive provenance never touches a live source.
    assert!(!config.paths.raw_root.join("source=binance-um").exists());
}

#[test]
fn a_re_run_skips_everything_without_downloading_or_sealing() {
    let dir = TempDir::new("archive-rerun");
    let config = config(dir.path(), EVERY_STREAM);
    let archive = Arc::new(FakeArchive::default());
    let (from, to) = (day("2026-09-29"), day("2026-09-30"));
    publish_all(&archive, from, to, 600_000);
    let (first, _) = run_import(&config, &archive, from, to, false);
    assert_eq!(first.imported, 21);
    let manifests = count_files(&config.paths.raw_root, ".manifest");
    let before = version(&config);
    let downloads = archive.downloads();

    let (second, out) = run_import(&config, &archive, from, to, false);
    assert_eq!(second.imported, 0, "{out}");
    assert_eq!(second.skipped, 21);
    assert_eq!(second.exit_code(), 0);
    assert_eq!(archive.downloads(), downloads, "no zip downloaded again");
    assert_eq!(count_files(&config.paths.raw_root, ".manifest"), manifests);
    assert_eq!(version(&config), before);

    // The dry run classifies without writing: imported days are skipped,
    // a published day not yet imported is reported, an unpublished one
    // is missing.
    publish_all(&archive, day("2026-10-01"), day("2026-10-01"), 600_000);
    let (dry, out) = run_import(&config, &archive, from, day("2026-10-02"), true);
    assert_eq!(dry.skipped, 21, "{out}");
    assert_eq!(dry.published, 11, "{out}");
    assert_eq!(dry.missing, 10, "{out}");
    assert_eq!(archive.downloads(), downloads);
    assert_eq!(count_files(&config.paths.raw_root, ".manifest"), manifests);
}

#[test]
fn an_interrupted_file_resumes_without_duplicates() {
    let dir = TempDir::new("archive-resume");
    let config = config(dir.path(), r#"["aggTrades"]"#);
    let archive = Arc::new(FakeArchive::default());
    let d = day("2026-09-30");
    let published = publish_all(&archive, d, d, 3_600_000);
    let csv = &published["BTCUSDT-aggTrades-2026-09-30.zip"];
    assert_eq!(data_rows(csv).len(), 24);
    let request = ImportRequest {
        from_day: d,
        to_day: d,
        streams: None,
        dry_run: false,
    };
    let options = archive::import_options(&config, &request).unwrap();
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let small_parts = RotationPolicy {
        max_rows: 5,
        ..archive::rotation_policy()
    };
    let clock = FakeClock::default();
    let shutdown = AtomicBool::new(false);

    // Crash after 13 appends: parts of 5 and 5 rows are sealed, 3 rows are
    // lost with the open part.
    {
        let mut writer = store.writer(ARCHIVE_SOURCE, small_parts).unwrap();
        let mut sink = FailAfter {
            inner: &mut writer,
            left: 13,
        };
        let fetcher = Fetcher::new(
            archive.as_ref(),
            archive.as_ref(),
            &clock,
            Default::default(),
        );
        let error = Importer::new(&mut sink, &store, fetcher, options.clone(), &shutdown)
            .run(&mut |_| {})
            .expect_err("the sink failed");
        assert!(matches!(error, ImportError::Store(_)), "{error}");
    }
    assert!(config.paths.import_ledger.join("PENDING").exists());
    assert_eq!(records_of(&config, ArchiveStream::AggTrades).len(), 10);

    // Restart: the 10 sealed rows are a prefix, the other 14 are appended.
    let mut writer = store.writer(ARCHIVE_SOURCE, small_parts).unwrap();
    let mut reports = Vec::new();
    let fetcher = Fetcher::new(
        archive.as_ref(),
        archive.as_ref(),
        &clock,
        Default::default(),
    );
    let summary = Importer::new(&mut writer, &store, fetcher, options, &shutdown)
        .run(&mut |r| reports.push(r.to_string()))
        .unwrap();
    writer.close().unwrap();
    assert_eq!(
        reports,
        [
            "resumed BTCUSDT-aggTrades-2026-09-30.zip: 10 rows sealed, 14 appended",
            "skipped BTCUSDT-aggTrades-2026-09-30.zip",
        ]
    );
    assert_eq!(summary.resumed, 1);
    assert!(!config.paths.import_ledger.join("PENDING").exists());
    let records = records_of(&config, ArchiveStream::AggTrades);
    let payloads: BTreeSet<&[u8]> = records.iter().map(|r| r.payload.as_slice()).collect();
    assert_eq!(records.len(), 24);
    assert_eq!(payloads.len(), 24, "no duplicate payloads");
    let ledger = LedgerDir::new(&config.paths.import_ledger)
        .read(ArchiveStream::AggTrades, "BTCUSDT-aggTrades-2026-09-30.zip")
        .unwrap()
        .unwrap();
    assert_eq!(ledger.rows, 24);
    assert_eq!(ledger.files.len(), 5);

    // And the archive-verify view agrees.
    let mut out = Vec::new();
    assert!(
        archive::verify(&config, d, d, &mut out).unwrap(),
        "{}",
        String::from_utf8_lossy(&out)
    );
}

#[test]
fn orphans_that_are_not_a_prefix_stop_the_run_before_any_append() {
    let dir = TempDir::new("archive-orphan");
    let config = config(dir.path(), r#"["aggTrades"]"#);
    let archive = Arc::new(FakeArchive::default());
    let d = day("2026-09-30");
    let published = publish_all(&archive, d, d, 3_600_000);
    let csv = &published["BTCUSDT-aggTrades-2026-09-30.zip"];
    let rows = data_rows(csv);
    let store = ParquetRawStore::new(&config.paths.raw_root);

    // A sealed part holding the second row first: not a prefix.
    {
        let mut writer = store
            .writer(ARCHIVE_SOURCE, archive::rotation_policy())
            .unwrap();
        let row = rows[1].as_bytes();
        writer
            .append(
                &key(ArchiveStream::AggTrades),
                RawRecord {
                    event_time: record_time(ArchiveStream::AggTrades, row).unwrap(),
                    capture: None,
                    payload: row.to_vec(),
                },
            )
            .unwrap();
        writer.close().unwrap();
    }
    let manifests = count_files(&config.paths.raw_root, ".manifest");

    // Without a pending import the orphan is a conflict as well.
    let (from, to) = (d, d);
    let request = ImportRequest {
        from_day: from,
        to_day: to,
        streams: None,
        dry_run: false,
    };
    let error = archive::import(
        &config,
        &request,
        &transports(&archive),
        &AtomicBool::new(false),
        &mut Vec::new(),
    )
    .expect_err("orphan without PENDING");
    assert!(error.contains("integrity conflict"), "{error}");
    assert!(!config.paths.import_ledger.join("PENDING").exists());

    let period = Period::Day(d);
    LedgerDir::new(&config.paths.import_ledger)
        .set_pending(&Pending {
            archive: ArchiveStream::AggTrades.archive_path(SYMBOL, period),
            sha256: sha256(
                &archive.zips.lock().unwrap()[&format!(
                    "{BASE}/{}",
                    ArchiveStream::AggTrades.archive_path(SYMBOL, period)
                )],
            ),
            stream: key(ArchiveStream::AggTrades),
            period,
        })
        .unwrap();
    let error = archive::import(
        &config,
        &request,
        &transports(&archive),
        &AtomicBool::new(false),
        &mut Vec::new(),
    )
    .expect_err("non-prefix orphan");
    assert!(error.contains("integrity conflict"), "{error}");
    assert!(error.contains("is not line"), "{error}");
    assert_eq!(count_files(&config.paths.raw_root, ".manifest"), manifests);
    assert!(config.paths.import_ledger.join("PENDING").exists());
}

#[test]
fn a_file_republished_upstream_is_a_conflict_and_the_store_stays_untouched() {
    let dir = TempDir::new("archive-changed");
    let config = config(dir.path(), r#"["bookDepth"]"#);
    let archive = Arc::new(FakeArchive::default());
    let d = day("2026-09-30");
    publish_all(&archive, d, d, 3_600_000);
    let (first, _) = run_import(&config, &archive, d, d, false);
    assert_eq!(first.imported, 1);
    let manifests = count_files(&config.paths.raw_root, ".manifest");
    let before = version(&config);

    let republished = book_depth_csv(d).replace("10.00000000", "11.00000000");
    archive.publish(ArchiveStream::BookDepth, Period::Day(d), &republished);
    let (second, out) = run_import(&config, &archive, d, d, false);
    assert_eq!(second.changed, 1, "{out}");
    assert_eq!(second.exit_code(), 1);
    assert!(
        out.contains("changed BTCUSDT-bookDepth-2026-09-30.zip"),
        "{out}"
    );
    assert_eq!(count_files(&config.paths.raw_root, ".manifest"), manifests);
    assert_eq!(version(&config), before);
}

#[test]
fn a_bad_header_or_a_microsecond_time_fails_the_file_and_appends_nothing() {
    let dir = TempDir::new("archive-invalid");
    let config = config(dir.path(), r#"["aggTrades"]"#);
    let archive = Arc::new(FakeArchive::default());
    let (d1, d2) = (day("2026-09-29"), day("2026-09-30"));
    let renamed = agg_trades_csv(&ticks(d1, 3_600_000)).replacen("quantity", "qty", 1);
    archive.publish(ArchiveStream::AggTrades, Period::Day(d1), &renamed);
    let ticks2 = ticks(d2, 3_600_000);
    let mut micros = ticks2.clone();
    micros[20].time *= 1_000;
    archive.publish(
        ArchiveStream::AggTrades,
        Period::Day(d2),
        &agg_trades_csv(&micros),
    );

    let (summary, out) = run_import(&config, &archive, d1, d2, false);
    assert_eq!(summary.failed, 2, "{out}");
    assert_eq!(summary.exit_code(), 1);
    assert!(
        out.contains("failed BTCUSDT-aggTrades-2026-09-29.zip: line 1"),
        "{out}"
    );
    assert!(out.contains("outside 2026-09-30 ± 1 day"), "{out}");
    assert_eq!(count_files(&config.paths.raw_root, ".parquet"), 0);
    assert_eq!(count_files(&config.paths.import_ledger, ".import"), 0);
    assert!(!config.paths.import_ledger.join("PENDING").exists());
}

#[test]
fn verify_passes_on_a_clean_import_and_fails_on_a_row_that_does_not_normalize() {
    let dir = TempDir::new("archive-verify");
    let config = config(dir.path(), EVERY_STREAM);
    let archive = Arc::new(FakeArchive::default());
    let (from, to) = (day("2026-09-29"), day("2026-09-30"));
    publish_all(&archive, from, to, 600_000);
    run_import(&config, &archive, from, to, false);
    let mut out = Vec::new();
    let pass = archive::verify(&config, from, to, &mut out).unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(pass, "{text}");
    assert!(text.ends_with("PASS\n"));
    assert!(
        text.contains("stream aggTrades: OK files 2, rows 288, normalize errors 0, id breaks 0"),
        "{text}"
    );
    assert!(text.contains("2026-09-30 aggTrades: rows 144"), "{text}");

    // A ninth significant decimal: stored verbatim (raw first), but it does
    // not normalize.
    let d = day("2026-10-01");
    let mut ticks = ticks(d, 600_000);
    ticks[3].qty_thousandths = 49_999;
    let csv = agg_trades_csv(&ticks).replace(",49.999,", ",0.000000001,");
    archive.publish(ArchiveStream::AggTrades, Period::Day(d), &csv);
    let config_agg = self::config(dir.path(), r#"["aggTrades"]"#);
    let (summary, _) = run_import(&config_agg, &archive, d, d, false);
    assert_eq!(summary.imported, 1);
    let mut out = Vec::new();
    let pass = archive::verify(&config_agg, d, d, &mut out).unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(!pass, "{text}");
    assert!(text.contains("normalize errors 1"), "{text}");
    assert!(text.ends_with("FAIL\n"));
}

#[test]
fn the_kline_check_matches_consistent_days_and_counts_an_altered_kline() {
    let dir = TempDir::new("archive-klines");
    let streams = r#"["aggTrades", "klines", "trades"]"#;
    let config = config(dir.path(), streams);
    let archive = Arc::new(FakeArchive::default());
    let (from, to) = (day("2026-09-28"), day("2026-09-30"));
    let published = publish_all(&archive, from, to, 30_000);
    run_import(&config, &archive, from, to, false);
    let middle = day("2026-09-29");

    for source in [ArchiveStream::AggTrades, ArchiveStream::Trades] {
        let mut out = Vec::new();
        let matched = archive::kline_check(&config, middle, middle, source, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(matched, "{text}");
        // 1440 + 288 + 96 + 24 + 6 + 1 bars of the day.
        assert!(
            text.contains("complete bars compared: 1855, matched: 1855, mismatched: 0"),
            "{text}"
        );
        assert!(text.contains(&format!("trades from {source}")), "{text}");
        assert!(text.contains("dataset klines "), "{text}");
    }

    // A second store where one 1m kline of the middle day is altered.
    let dir2 = TempDir::new("archive-klines-altered");
    let config2 = self::config(dir2.path(), streams);
    let altered = Arc::new(FakeArchive::default());
    publish_all(&altered, from, to, 30_000);
    let name = "BTCUSDT-1m-2026-09-29.zip";
    let csv = &published[name];
    let row = data_rows(csv)[100];
    let mut fields: Vec<String> = row.split(',').map(str::to_owned).collect();
    fields[5] = "999.000".to_owned();
    let changed = csv.replace(row, &fields.join(","));
    altered.publish(
        ArchiveStream::Klines(Timeframe::M1),
        Period::Day(middle),
        &changed,
    );
    run_import(&config2, &altered, from, to, false);
    let mut out = Vec::new();
    let matched =
        archive::kline_check(&config2, middle, middle, ArchiveStream::AggTrades, &mut out).unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(!matched);
    assert!(text.contains("mismatched: 1"), "{text}");
    assert!(text.contains("mismatch: 1m"), "{text}");
}
