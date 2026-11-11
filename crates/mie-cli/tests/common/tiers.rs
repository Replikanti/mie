//! The tiered-store fixture shared by `mie store adopt`, `mie store
//! verify` and `mie store repair` offline (ADR-062 D10, D11): a source
//! store with both sources and dates on both sides of the first hot date,
//! an initialized tiered store with two marked copy targets, their
//! configurations, and the AW1 report helper.

use super::{D0, HOUR, MapProbe, agg};
use mie_adapter_binance::archive::catalog::{DAY_MS, Period};
use mie_adapter_binance::archive::ledger::{LedgerDir, Pending};
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_binance::transport::{Clock, HttpDownload, HttpGet};
use mie_adapter_parquet::copy_ledger;
use mie_adapter_parquet::replica;
use mie_adapter_parquet::verification::CopyState;
use mie_adapter_parquet::{ParquetRawStore, RawWriter, RotationPolicy, TierContext};
use mie_cli::archive::{self, ArchiveTransports, ImportRequest};
use mie_cli::config::{ArchiveConfig, ColdRoot, IngestConfig};
use mie_cli::store::{self, AdoptRequest, DeviceProbe};
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::RawRecordSource;
use mie_ports::raw::{RawSelection, RawStreamKey};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const BASE: &str = "https://archive.invalid";
pub const SYMBOL: &str = "BTCUSDT";
/// The first hot date the fixtures pass in: partitions before it are
/// recorded removed from hot.
pub const FIRST_HOT: &str = "2026-10-05";
/// The live runs: one on an old date, one in the hot window.
pub const T1: i64 = D0 - 3 * DAY_MS + HOUR;
pub const T2: i64 = D0 + HOUR;
/// The AW1 windows: everything; one old day; old days up to the first hot
/// date. Every report of each touches a removed file.
pub const WINDOWS: [&str; 6] = [
    "2026-10-02",
    "2026-10-06",
    "2026-10-03",
    "2026-10-03",
    "2026-10-03",
    "2026-10-05",
];

// ---------------------------------------------------------------- fakes

/// Serves published zips and their `.CHECKSUM`s; 404 for anything else.
#[derive(Default)]
pub struct FakeArchive {
    zips: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl FakeArchive {
    pub fn publish(&self, stream: ArchiveStream, day: i64, csv: &str) {
        let period = Period::Day(day);
        let path = stream.archive_path(SYMBOL, period);
        let entry = stream.file_name(SYMBOL, period).replace(".zip", ".csv");
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        writer.start_file(entry, options).unwrap();
        writer.write_all(csv.as_bytes()).unwrap();
        let zip = writer.finish().unwrap().into_inner();
        self.zips
            .lock()
            .unwrap()
            .insert(format!("{BASE}/{path}"), zip);
    }
}

impl HttpGet for FakeArchive {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        let zip_url = url.strip_suffix(".CHECKSUM").expect("only checksums");
        Ok(match self.zips.lock().unwrap().get(zip_url) {
            Some(zip) => {
                let sha: String = Sha256::digest(zip)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect();
                let name = zip_url.rsplit('/').next().unwrap();
                (200, format!("{sha}  {name}\n").into_bytes())
            }
            None => (404, b"Not Found".to_vec()),
        })
    }
}

impl HttpDownload for FakeArchive {
    fn download(&self, url: &str, sink: &mut dyn Write) -> Result<u16, String> {
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
pub struct SleepClock(Mutex<u64>);

impl Clock for SleepClock {
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

pub fn agg_trades_csv(day: i64, first_id: u64) -> String {
    let mut csv = format!("{}\n", ArchiveStream::AggTrades.expected_header());
    for h in 0..24 {
        let id = first_id + h as u64;
        let time = day * DAY_MS + h * HOUR + 7;
        csv.push_str(&format!("{id},85000.10,0.010,{id},{id},{time},false\n"));
    }
    csv
}

/// Hourly metrics plus 23:55, which is filed at the next midnight
/// (ADR-034 D3).
pub fn metrics_csv(day: i64) -> String {
    let mut csv = format!("{}\n", ArchiveStream::Metrics.expected_header());
    let date = mie_adapter_binance::archive::catalog::day_label(day);
    for hour in 0..24 {
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

/// The fake archive of every fixture: `aggTrades` of 2026-10-02 to
/// 2026-10-05 and `metrics` of 2026-10-02 and 2026-10-03.
pub fn fake_archive() -> Arc<FakeArchive> {
    let archive = Arc::new(FakeArchive::default());
    let first = D0 / DAY_MS - 4;
    for (i, day) in (first..first + 4).enumerate() {
        archive.publish(
            ArchiveStream::AggTrades,
            day,
            &agg_trades_csv(day, 1 + 100 * i as u64),
        );
    }
    for day in [first, first + 1] {
        archive.publish(ArchiveStream::Metrics, day, &metrics_csv(day));
    }
    archive
}

pub fn transports(archive: &Arc<FakeArchive>) -> ArchiveTransports {
    ArchiveTransports {
        http: archive.clone(),
        download: archive.clone(),
        clock: Arc::new(SleepClock::default()),
    }
}

/// Imports `stream` for the inclusive day range.
pub fn import(
    config: &ArchiveConfig,
    archive: &Arc<FakeArchive>,
    stream: ArchiveStream,
    from_day: i64,
    to_day: i64,
) -> Result<String, String> {
    let mut out = Vec::new();
    let request = ImportRequest {
        from_day,
        to_day,
        streams: Some(vec![stream]),
        dry_run: false,
    };
    archive::import(
        config,
        &request,
        &transports(archive),
        &AtomicBool::new(false),
        &mut out,
    )
    .map(|_| String::from_utf8(out.clone()).unwrap())
    .map_err(|error| format!("{error}\n{}", String::from_utf8(out).unwrap()))
}

// ------------------------------------------------------------- fixtures

/// Writes an ingest configuration to `path` and parses it.
pub fn ingest_toml(path: &Path, raw: &Path, journal: &Path, cold: &[ColdRoot]) -> IngestConfig {
    let cold_roots = if cold.is_empty() {
        String::new()
    } else {
        format!("cold_roots = {}\n", super::cold_roots_toml(cold))
    };
    let text = format!(
        "[instrument]\nsymbol = \"BTCUSDT\"\nsource = \"binance-um\"\n[paths]\n\
         raw_root = \"{}\"\njournal = \"{}\"\n{cold_roots}[binance]\n\
         ws_base_url = \"wss://fake.invalid/ws\"\nrest_base_url = \"https://fake.invalid\"\n\
         streams = [\"aggTrade\"]\n[capture]\nhold_back_ms = 750\n",
        raw.display(),
        journal.display(),
    );
    std::fs::write(path, &text).unwrap();
    IngestConfig::parse(&text).unwrap()
}

/// Writes an archive configuration to `path` and parses it.
pub fn archive_toml(path: &Path, base: &Path, raw: &Path, cold: &[ColdRoot]) -> ArchiveConfig {
    let cold_roots = if cold.is_empty() {
        String::new()
    } else {
        format!("cold_roots = {}\n", super::cold_roots_toml(cold))
    };
    let text = format!(
        "[instrument]\nsymbol = \"{SYMBOL}\"\n[paths]\nraw_root = \"{}\"\n\
         import_ledger = \"{}\"\nstaging = \"{}\"\n{cold_roots}[archive]\n\
         base_url = \"{BASE}\"\nstreams = [\"aggTrades\", \"metrics\"]\n",
        raw.display(),
        base.join("ledger").display(),
        base.join("staging").display(),
    );
    std::fs::write(path, &text).unwrap();
    ArchiveConfig::parse(&text).unwrap()
}

/// The source store under `dir/src` (single root, both sources), the
/// tiered store under `dir/dst` (hot initialized, two marked copy
/// targets), and their configurations.
pub struct Fixture {
    pub dir: PathBuf,
    pub archive: Arc<FakeArchive>,
    /// `dir/src.toml`, `dir/src-archive.toml`.
    pub src_ingest: PathBuf,
    pub src_archive: PathBuf,
    /// `dir/dst/ingest.toml`, `dir/dst/archive.toml`.
    pub dst_ingest: PathBuf,
    pub dst_archive: PathBuf,
    pub config: IngestConfig,
    pub archive_config: ArchiveConfig,
}

impl Fixture {
    pub fn new(dir: &Path) -> Self {
        let src = dir.join("src");
        let src_config = ingest_toml(
            &dir.join("src.toml"),
            &src.join("raw"),
            &src.join("journal.jsonl"),
            &[],
        );
        let t1 = super::ingest(
            &src_config,
            T1,
            vec![vec![
                agg(100, T1 + 10),
                agg(101, T1 + 20),
                agg(102, T1 + 30),
            ]],
        );
        let t2 = super::ingest(
            &src_config,
            T2,
            vec![vec![agg(300, T2 + 5), agg(301, T2 + 6)]],
        );
        assert_eq!((t1.exit_code(), t2.exit_code()), (0, 0));
        let fake = fake_archive();
        let src_archive = archive_toml(&dir.join("src-archive.toml"), &src, &src.join("raw"), &[]);
        let first = D0 / DAY_MS - 4;
        import(
            &src_archive,
            &fake,
            ArchiveStream::AggTrades,
            first,
            first + 3,
        )
        .unwrap();
        import(&src_archive, &fake, ArchiveStream::Metrics, first, first).unwrap();

        let dst = dir.join("dst");
        std::fs::create_dir_all(&dst).unwrap();
        let cold = cold_roots(dir);
        let config = ingest_toml(
            &dst.join("ingest.toml"),
            &dst.join("raw"),
            &dst.join("journal.jsonl"),
            &cold,
        );
        let archive_config = archive_toml(&dst.join("archive.toml"), &dst, &dst.join("raw"), &cold);
        super::init_tiers(&config.paths.raw_root, &cold);
        Self {
            dir: dir.to_owned(),
            archive: fake,
            src_ingest: dir.join("src.toml"),
            src_archive: dir.join("src-archive.toml"),
            dst_ingest: dst.join("ingest.toml"),
            dst_archive: dst.join("archive.toml"),
            config,
            archive_config,
        }
    }

    pub fn src(&self) -> PathBuf {
        self.dir.join("src")
    }

    pub fn src_raw(&self) -> PathBuf {
        self.dir.join("src/raw")
    }

    pub fn hot(&self) -> PathBuf {
        self.config.paths.raw_root.clone()
    }

    /// The request adopting the whole source store.
    pub fn request(&self) -> AdoptRequest {
        AdoptRequest {
            from: self.src_raw(),
            journal: Some(self.src().join("journal.jsonl")),
            import_ledger: Some(self.src().join("ledger")),
            archive_config: Some(self.dst_archive.clone()),
        }
    }

    pub fn probe(&self) -> MapProbe {
        MapProbe(vec![
            (self.hot(), 1),
            (self.dir.join("cold-a"), 2),
            (self.dir.join("cold-b"), 3),
        ])
    }

    pub fn adopt(&self, request: &AdoptRequest) -> (bool, String) {
        adopt_with(&self.config, request, &self.probe())
    }

    pub fn tiered(&self) -> ParquetRawStore {
        store::raw_store(&self.hot(), &self.config.paths.cold_roots).unwrap()
    }

    /// The tiered writer of `source` on hot, with the fixture's hot window.
    pub fn writer(&self, source: &str) -> RawWriter {
        let ctx = TierContext {
            first_hot_date: FIRST_HOT.to_owned(),
            now_ms: Arc::new(|| D0 + 10),
            on_recall: None,
        };
        self.tiered()
            .tiered_writer(source, RotationPolicy::default(), ctx)
            .unwrap()
    }
}

/// The copy targets `dir/cold-a` and `dir/cold-b`.
pub fn cold_roots(dir: &Path) -> Vec<ColdRoot> {
    super::cold_roots(dir)
}

pub fn adopt_with(
    config: &IngestConfig,
    request: &AdoptRequest,
    probe: &dyn DeviceProbe,
) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::adopt(config, request, FIRST_HOT, probe, &|| D0 + 1, &mut out).unwrap();
    (pass, String::from_utf8(out).unwrap())
}

/// Every file below `root` with its bytes, sorted by relative path; lock
/// files (which the adoption's locks create) are left out.
pub fn files(root: &Path) -> Vec<(String, Vec<u8>)> {
    pub fn walk(dir: &Path, base: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, base, out);
            } else if !path.to_string_lossy().ends_with(".lock") {
                let rel = path.strip_prefix(base).unwrap().display().to_string();
                out.push((rel, std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// Every selection the acceptance compares: each date, the whole range and
/// a window across the first hot date, over every stream of the fixture.
pub fn selections() -> Vec<RawSelection> {
    let streams: BTreeSet<RawStreamKey> = [
        ("binance-um", "aggTrade"),
        (ARCHIVE_SOURCE, "aggTrades"),
        (ARCHIVE_SOURCE, "metrics"),
    ]
    .into_iter()
    .map(|(source, stream)| RawStreamKey::new(source, SYMBOL, stream).unwrap())
    .collect();
    let window = |start: i64, end: i64| {
        RawSelection::new(
            streams.clone(),
            ReplayWindow {
                start: EventTime::from_millis(start),
                end: EventTime::from_millis(end),
            },
        )
        .unwrap()
    };
    let mut out: Vec<RawSelection> = (-5..=1)
        .map(|d| window(D0 + d * DAY_MS, D0 + (d + 1) * DAY_MS))
        .collect();
    out.push(window(0, D0 + 2 * DAY_MS));
    out.push(window(D0 - 2 * DAY_MS, D0));
    out
}

pub fn date_of(path: &str) -> &str {
    path.split('/')
        .find_map(|segment| segment.strip_prefix("date="))
        .unwrap()
}

pub fn partition_of(path: &str) -> &str {
    path.rsplit_once('/').unwrap().0
}

/// The AW1 reports of `tools/tier-reports.sh` over the windows, paths
/// below `dir` stripped.
pub fn reports(dir: &Path, ingest: &Path, archive: &Path) -> String {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tools/tier-reports.sh");
    let output = Command::new("bash")
        .arg(script)
        .arg("--strip")
        .arg(dir)
        .arg(ingest)
        .arg(archive)
        .args(WINDOWS)
        .env("MIE", env!("CARGO_BIN_EXE_mie"))
        .env_remove("MIE_DEV_DEVICE_IDS")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "{stderr}");
    String::from_utf8(output.stdout).unwrap()
}

#[track_caller]
pub fn assert_pass((pass, out): &(bool, String)) {
    assert!(*pass && out.ends_with("PASS\n"), "{out}");
}

// ------------------------------------------- shared by rebuild and resolve

pub fn cold(fixture: &Fixture, name: &str) -> PathBuf {
    fixture.dir.join(name)
}

/// Days since 1970-01-01 of a `YYYY-MM-DD` date.
pub fn day_index(date: &str) -> i64 {
    mie_adapter_binance::archive::catalog::parse_day(date).unwrap()
}

/// Every selection's result (dataset version and files, or the error
/// text), in [`selections`] order.
pub fn selected(fixture: &Fixture) -> Vec<String> {
    let store = fixture.tiered();
    selections()
        .iter()
        .map(|selection| match store.select(selection) {
            Ok(dataset) => format!("{dataset:?}"),
            Err(error) => format!("error: {error}"),
        })
        .collect()
}

/// The held paths of hot's ledger.
pub fn held(fixture: &Fixture) -> BTreeSet<String> {
    let ledger = copy_ledger::read(&fixture.hot()).unwrap().unwrap();
    CopyState::fold(&ledger)
        .unwrap()
        .held()
        .into_iter()
        .map(|(path, _)| path)
        .collect()
}

/// The import request of one `metrics` day.
pub fn metrics_day(day: i64, dry_run: bool) -> ImportRequest {
    ImportRequest {
        from_day: day,
        to_day: day,
        streams: Some(vec![ArchiveStream::Metrics]),
        dry_run,
    }
}

pub fn run_import(fixture: &Fixture, request: &ImportRequest) -> Result<String, String> {
    let mut out = Vec::new();
    archive::import(
        &fixture.archive_config,
        request,
        &transports(&fixture.archive),
        &AtomicBool::new(false),
        &mut out,
    )
    .map(|_| String::from_utf8(out.clone()).unwrap())
}

/// The `metrics` file of 2026-10-03 imported into hot, then its entry
/// removed: its parts are the orphans an import that crashed between its
/// seal and its entry leaves. With `pending`, its `PENDING` is written too.
pub fn interrupted_import(fixture: &Fixture, pending: bool) -> (i64, Vec<String>) {
    let (day, parts, _) = interrupted_import_saving(fixture, pending);
    (day, parts)
}

/// [`interrupted_import`], also returning the removed entry's bytes.
pub fn interrupted_import_saving(fixture: &Fixture, pending: bool) -> (i64, Vec<String>, Vec<u8>) {
    let day = day_index("2026-10-03");
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    run_import(fixture, &metrics_day(day, false)).unwrap();
    let parts: Vec<String> = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .filter(|path| !before.contains(path))
        // The recall brought 2026-10-03's earlier parts back to hot.
        .filter(|path| !cold(fixture, "cold-a").join(path).exists())
        .collect();
    assert_eq!(parts.len(), 2, "{parts:?}");
    let ledgers = LedgerDir::new(&fixture.archive_config.paths.import_ledger);
    let name = ArchiveStream::Metrics.file_name(SYMBOL, Period::Day(day));
    let entry = ledgers.ledger_path(ArchiveStream::Metrics, &name);
    let bytes = fs::read(&entry).unwrap();
    fs::remove_file(&entry).unwrap();
    if pending {
        let path = ArchiveStream::Metrics.archive_path(SYMBOL, Period::Day(day));
        let (_, body) = fixture
            .archive
            .get(&format!("{BASE}/{path}.CHECKSUM"))
            .unwrap();
        let sha = String::from_utf8(body).unwrap()[..64].to_owned();
        ledgers
            .set_pending(&Pending {
                archive: path,
                sha256: sha,
                stream: RawStreamKey::new(ARCHIVE_SOURCE, SYMBOL, "metrics").unwrap(),
                period: Period::Day(day),
            })
            .unwrap();
    }
    (day, parts, bytes)
}
