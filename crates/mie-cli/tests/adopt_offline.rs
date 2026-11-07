//! `mie store adopt` offline (ADR-062 D10, D8, D6, #190): a store with
//! both sources and dates on both sides of the first hot date adopted into
//! a tiered store; two stores into one; the path rule (AC6); every refusal
//! writing nothing; the AW1 reports before and after; AW4 with the copy
//! targets emptied; the interim writer guard refusing an archive import
//! into a removed date (AC5).

mod common;

use common::{D0, HOUR, MapProbe, TempDir, agg};
use mie_adapter_binance::archive::catalog::{DAY_MS, Period};
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_binance::transport::{Clock, HttpDownload, HttpGet};
use mie_adapter_parquet::copy_ledger::{self, Kind};
use mie_adapter_parquet::replica;
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_cli::archive::{self, ArchiveTransports, ImportRequest};
use mie_cli::config::{ArchiveConfig, ColdRoot, IngestConfig};
use mie_cli::store::{self, AdoptRequest, DeviceProbe};
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawRecord, RawRecordSink, RawRecordSource, RawSelection, RawStreamKey};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BASE: &str = "https://archive.invalid";
const SYMBOL: &str = "BTCUSDT";
/// The first hot date the fixtures pass in: partitions before it are
/// recorded removed from hot.
const FIRST_HOT: &str = "2026-10-05";
/// The live runs: one on an old date, one in the hot window.
const T1: i64 = D0 - 3 * DAY_MS + HOUR;
const T2: i64 = D0 + HOUR;
/// The AW1 windows: everything; one old day; old days up to the first hot
/// date. Every report of each touches a removed file.
const WINDOWS: [&str; 6] = [
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
struct FakeArchive {
    zips: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl FakeArchive {
    fn publish(&self, stream: ArchiveStream, day: i64, csv: &str) {
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
struct SleepClock(Mutex<u64>);

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

fn agg_trades_csv(day: i64, first_id: u64) -> String {
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
fn metrics_csv(day: i64) -> String {
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
fn fake_archive() -> Arc<FakeArchive> {
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

fn transports(archive: &Arc<FakeArchive>) -> ArchiveTransports {
    ArchiveTransports {
        http: archive.clone(),
        download: archive.clone(),
        clock: Arc::new(SleepClock::default()),
    }
}

/// Imports `stream` for the inclusive day range.
fn import(
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
fn ingest_toml(path: &Path, raw: &Path, journal: &Path, cold: &[ColdRoot]) -> IngestConfig {
    let cold_roots = if cold.is_empty() {
        String::new()
    } else {
        format!("cold_roots = {}\n", common::cold_roots_toml(cold))
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
fn archive_toml(path: &Path, base: &Path, raw: &Path, cold: &[ColdRoot]) -> ArchiveConfig {
    let cold_roots = if cold.is_empty() {
        String::new()
    } else {
        format!("cold_roots = {}\n", common::cold_roots_toml(cold))
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
struct Fixture {
    dir: PathBuf,
    archive: Arc<FakeArchive>,
    /// `dir/src.toml`, `dir/src-archive.toml`.
    src_ingest: PathBuf,
    src_archive: PathBuf,
    /// `dir/dst/ingest.toml`, `dir/dst/archive.toml`.
    dst_ingest: PathBuf,
    dst_archive: PathBuf,
    config: IngestConfig,
    archive_config: ArchiveConfig,
}

impl Fixture {
    fn new(dir: &Path) -> Self {
        let src = dir.join("src");
        let src_config = ingest_toml(
            &dir.join("src.toml"),
            &src.join("raw"),
            &src.join("journal.jsonl"),
            &[],
        );
        let t1 = common::ingest(
            &src_config,
            T1,
            vec![vec![
                agg(100, T1 + 10),
                agg(101, T1 + 20),
                agg(102, T1 + 30),
            ]],
        );
        let t2 = common::ingest(
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
        common::init_tiers(&config.paths.raw_root, &cold);
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

    fn src(&self) -> PathBuf {
        self.dir.join("src")
    }

    fn src_raw(&self) -> PathBuf {
        self.dir.join("src/raw")
    }

    fn hot(&self) -> PathBuf {
        self.config.paths.raw_root.clone()
    }

    /// The request adopting the whole source store.
    fn request(&self) -> AdoptRequest {
        AdoptRequest {
            from: self.src_raw(),
            journal: Some(self.src().join("journal.jsonl")),
            import_ledger: Some(self.src().join("ledger")),
            archive_config: Some(self.dst_archive.clone()),
        }
    }

    fn probe(&self) -> MapProbe {
        MapProbe(vec![
            (self.hot(), 1),
            (self.dir.join("cold-a"), 2),
            (self.dir.join("cold-b"), 3),
        ])
    }

    fn adopt(&self, request: &AdoptRequest) -> (bool, String) {
        adopt_with(&self.config, request, &self.probe())
    }

    fn tiered(&self) -> ParquetRawStore {
        store::raw_store(&self.hot(), &self.config.paths.cold_roots).unwrap()
    }
}

/// The copy targets `dir/cold-a` and `dir/cold-b`.
fn cold_roots(dir: &Path) -> Vec<ColdRoot> {
    common::cold_roots(dir)
}

fn adopt_with(
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
fn files(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(dir: &Path, base: &Path, out: &mut Vec<(String, Vec<u8>)>) {
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
fn selections() -> Vec<RawSelection> {
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

fn date_of(path: &str) -> &str {
    path.split('/')
        .find_map(|segment| segment.strip_prefix("date="))
        .unwrap()
}

fn partition_of(path: &str) -> &str {
    path.rsplit_once('/').unwrap().0
}

/// AC1: the tiered `select` equals the source's for every window, hot
/// holds exactly the source's partitions from the first hot date on, every
/// file has k `copy` records, `removed` is recorded for exactly the older
/// partitions.
fn assert_adopted(fixture: &Fixture, source: &ParquetRawStore) {
    let tiered = fixture.tiered();
    for selection in selections() {
        assert_eq!(
            tiered.select(&selection).unwrap(),
            source.select(&selection).unwrap(),
            "{selection:?}"
        );
    }
    let paths = replica::list_sealed(source.root()).unwrap();
    let recent: Vec<String> = paths
        .iter()
        .filter(|path| date_of(path) >= FIRST_HOT)
        .cloned()
        .collect();
    assert!(!recent.is_empty() && recent.len() < paths.len());
    assert_eq!(replica::list_sealed(&fixture.hot()).unwrap(), recent);
    let records = copy_ledger::read(&fixture.hot()).unwrap().unwrap().records;
    let mut copies: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    let mut removed = BTreeSet::new();
    for record in &records {
        match &record.kind {
            Kind::Copy { path, root } => {
                copies.entry(path).or_default().insert(root);
            }
            Kind::Removed { partition } => {
                assert!(removed.insert(partition.clone()), "{partition} twice");
            }
            _ => {}
        }
    }
    for path in &paths {
        assert_eq!(
            copies.get(path.as_str()).map(BTreeSet::len),
            Some(2),
            "{path}"
        );
    }
    let old: BTreeSet<String> = paths
        .iter()
        .filter(|path| date_of(path) < FIRST_HOT)
        .map(|path| partition_of(path).to_owned())
        .collect();
    assert_eq!(removed, old);
}

#[track_caller]
fn assert_pass((pass, out): &(bool, String)) {
    assert!(*pass && out.ends_with("PASS\n"), "{out}");
}

// ------------------------------------------------------------------ tests

/// AC1: a store with both sources and dates inside and outside the hot
/// window is adopted; the source is byte-for-byte unchanged; hot's journal
/// and entries are the source's; a following pass copies and appends
/// nothing.
#[test]
fn an_adopted_store_selects_as_its_source_and_the_source_is_unchanged() {
    let dir = TempDir::new("adopt-ac1");
    let fixture = Fixture::new(dir.path());
    let source_tree = files(&fixture.src());
    let result = fixture.adopt(&fixture.request());
    assert_pass(&result);
    let out = &result.1;
    assert!(
        out.contains(
            "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-03: 1 file(s) to \
             cold-a, cold-b; removed from hot\n"
        ),
        "{out}"
    );
    assert!(
        out.contains(
            "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06: 1 file(s) to \
             cold-a, cold-b; hot\n"
        ),
        "{out}"
    );
    assert_adopted(&fixture, &ParquetRawStore::new(fixture.src_raw()));
    assert_eq!(files(&fixture.src()), source_tree);
    assert_eq!(
        std::fs::read(&fixture.config.paths.journal).unwrap(),
        std::fs::read(fixture.src().join("journal.jsonl")).unwrap()
    );
    assert_eq!(
        files(&fixture.dir.join("dst/ledger")),
        files(&fixture.src().join("ledger"))
    );

    let mut out = Vec::new();
    let pass = store::replicate(
        &fixture.config,
        Some(&fixture.dst_archive),
        &fixture.probe(),
        &|| D0 + 2,
        &mut out,
    )
    .unwrap();
    let out = String::from_utf8(out).unwrap();
    assert!(pass, "{out}");
    assert_eq!(
        out,
        "copied 0 file(s) and 0 import entries; appended 0 copy-ledger byte(s) and 0 journal \
         byte(s)\nPASS\n"
    );
}

/// The production shape (D10): the live store, then the archive store,
/// adopted into one tiered store, select as a single root holding both.
#[test]
fn two_stores_adopted_into_one_select_as_both() {
    let dir = TempDir::new("adopt-two");
    let fixture = Fixture::new(dir.path());
    // Split the source into a live store and an archive store.
    let (live, archive) = (dir.path().join("live"), dir.path().join("archive"));
    for (store, source) in [
        (&live, "source=binance-um"),
        (&archive, "source=binance-archive"),
    ] {
        copy_tree(
            &fixture.src_raw().join(source),
            &store.join("raw").join(source),
        );
    }
    copy_tree(&fixture.src().join("ledger"), &archive.join("ledger"));
    let live_request = AdoptRequest {
        from: live.join("raw"),
        journal: Some(fixture.src().join("journal.jsonl")),
        import_ledger: None,
        archive_config: None,
    };
    assert_pass(&fixture.adopt(&live_request));
    let archive_request = AdoptRequest {
        from: archive.join("raw"),
        journal: None,
        import_ledger: Some(archive.join("ledger")),
        archive_config: Some(fixture.dst_archive.clone()),
    };
    assert_pass(&fixture.adopt(&archive_request));
    assert_adopted(&fixture, &ParquetRawStore::new(fixture.src_raw()));
    // The sources had no `.lock` files, and the adoption created none.
    for store in [&live, &archive] {
        let locks: Vec<String> = files_with_locks(&store.join("raw"))
            .into_iter()
            .filter(|name| name.ends_with(".lock"))
            .collect();
        assert!(locks.is_empty(), "{locks:?}");
    }
}

/// Every file path below `root`, lock files included.
fn files_with_locks(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_owned()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path.display().to_string());
            }
        }
    }
    out
}

/// Copies the directory tree `from` to `to`.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else if entry.file_name() != ".lock" {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Seals `times` as `aggTrade` records of the live source into the single
/// root `raw`; returns the new data paths.
fn seal_live(raw: &Path, times: &[i64], tag: &str) -> Vec<String> {
    let before = replica::list_sealed(raw).unwrap_or_default();
    let key = RawStreamKey::new("binance-um", SYMBOL, "aggTrade").unwrap();
    let mut writer = ParquetRawStore::new(raw)
        .writer("binance-um", RotationPolicy::default())
        .unwrap();
    for (i, &time) in times.iter().enumerate() {
        writer
            .append(
                &key,
                RawRecord {
                    event_time: EventTime::from_millis(time),
                    capture: None,
                    payload: format!("{tag}-{i}").into_bytes(),
                },
            )
            .unwrap();
    }
    writer.close().unwrap();
    replica::list_sealed(raw)
        .unwrap()
        .into_iter()
        .filter(|path| !before.contains(path))
        .collect()
}

/// The trees an adoption must not change, lock files left out: hot (its
/// ledger included), both targets, the source, and hot's journal and
/// import ledger.
fn trees(fixture: &Fixture, source: &Path) -> Vec<Vec<(String, Vec<u8>)>> {
    [
        fixture.dir.join("dst"),
        fixture.dir.join("cold-a"),
        fixture.dir.join("cold-b"),
        source.to_owned(),
    ]
    .iter()
    .map(|root| files(root))
    .collect()
}

/// AC6, the path rule: an identical re-run is skipped and leaves hot's
/// copy ledger byte-identical; a store with one path of other bytes is
/// refused with `Integrity` naming the path and changes nothing; the
/// identical path plus a new date is adopted.
#[test]
fn the_path_rule_skips_identical_paths_and_refuses_other_bytes() {
    let dir = TempDir::new("adopt-path-rule");
    let fixture = Fixture::new(dir.path());
    assert_pass(&fixture.adopt(&fixture.request()));
    let ledger = std::fs::read(copy_ledger::ledger_path(&fixture.hot())).unwrap();
    let count = replica::list_sealed(&fixture.src_raw()).unwrap().len();
    let again = fixture.adopt(&fixture.request());
    assert_pass(&again);
    assert!(
        again.1.contains(&format!(
            "adopted 0 file(s), 0 byte(s), 0 stream-date(s); recorded 0 partition(s) removed \
             from hot; copied 0 file(s) to hot; skipped {count} identical file(s); copied 0 \
             import entries and 0 journal byte(s) to hot\n"
        )),
        "{}",
        again.1
    );
    assert_eq!(
        std::fs::read(copy_ledger::ledger_path(&fixture.hot())).unwrap(),
        ledger
    );

    // One path with other bytes, plus a new date.
    let other = dir.path().join("other");
    let adopted = replica::list_sealed(&fixture.src_raw())
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um") && date_of(p) == "2026-10-03")
        .unwrap();
    let clash = seal_live(&other, &[T1 + 50], "other");
    assert_eq!(clash, std::slice::from_ref(&adopted));
    seal_live(&other, &[D0 - 5 * DAY_MS], "new");
    let before = trees(&fixture, &other);
    let request = AdoptRequest {
        from: other.clone(),
        ..AdoptRequest::default()
    };
    let (pass, out) = fixture.adopt(&request);
    assert!(!pass, "{out}");
    assert!(
        out.contains(&format!(
            "FAIL: raw store integrity check failed: {adopted}: its manifest in --from differs \
             from the copy ledger's manifest record, copy target cold-a"
        )),
        "{out}"
    );
    assert_eq!(trees(&fixture, &other), before);

    // The identical path plus a new date.
    let same = dir.path().join("same");
    let manifest = format!("{}.manifest", adopted.strip_suffix(".parquet").unwrap());
    for file in [&adopted, &manifest] {
        let target = same.join(file);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::copy(fixture.src_raw().join(file), target).unwrap();
    }
    let new = seal_live(&same, &[D0 - 5 * DAY_MS], "new");
    let (pass, out) = fixture.adopt(&AdoptRequest {
        from: same,
        ..AdoptRequest::default()
    });
    assert!(pass, "{out}");
    assert!(
        out.contains("adopted 1 file(s)") && out.contains("skipped 1 identical file(s)"),
        "{out}"
    );
    let records = copy_ledger::read(&fixture.hot()).unwrap().unwrap().records;
    assert!(records.iter().any(|r| matches!(
        &r.kind,
        Kind::Removed { partition } if partition == partition_of(&new[0])
    )));
}

/// Every refusal writes nothing: hot (ledger, journal, import ledger), the
/// targets and the source are unchanged, lock files left out.
#[test]
fn every_refusal_writes_nothing() {
    type Setup =
        Box<dyn Fn(&Fixture, &mut AdoptRequest, &mut IngestConfig) -> Box<dyn std::any::Any>>;
    let none = || -> Box<dyn std::any::Any> { Box::new(()) };
    let cases: Vec<(&str, Setup, &str)> = vec![
        (
            "no cold roots",
            Box::new(move |_, _, config| {
                config.paths.cold_roots.clear();
                none()
            }),
            "paths.cold_roots is empty",
        ),
        (
            "hot without its marker",
            Box::new(move |fixture, _, _| {
                std::fs::remove_file(fixture.hot().join(".mie/root")).unwrap();
                none()
            }),
            "has no root marker although paths.cold_roots is configured",
        ),
        (
            "an archive configuration of another store",
            Box::new(move |fixture, request, _| {
                let path = fixture.dir.join("elsewhere.toml");
                archive_toml(
                    &path,
                    &fixture.dir,
                    &fixture.src_raw(),
                    &cold_roots(&fixture.dir),
                );
                request.archive_config = Some(path);
                none()
            }),
            "--archive-config",
        ),
        (
            "--import-ledger without an archive configuration",
            Box::new(move |_, request, _| {
                request.archive_config = None;
                none()
            }),
            "--import-ledger needs this store's archive configuration",
        ),
        (
            "--from missing",
            Box::new(move |fixture, request, _| {
                request.from = fixture.dir.join("nowhere");
                none()
            }),
            "--from",
        ),
        (
            "--from a file",
            Box::new(move |fixture, request, _| {
                request.from = fixture.src().join("journal.jsonl");
                none()
            }),
            "is not a directory",
        ),
        (
            "--from holding .mie",
            Box::new(move |fixture, _, _| {
                std::fs::create_dir(fixture.src_raw().join(".mie")).unwrap();
                none()
            }),
            "it is a root of a tiered store",
        ),
        (
            // Hot holds `.mie/`, which is refused first.
            "--from is hot",
            Box::new(move |fixture, request, _| {
                request.from = fixture.hot();
                none()
            }),
            "it is a root of a tiered store",
        ),
        (
            "--from contains hot",
            Box::new(move |fixture, request, _| {
                request.from = fixture.dir.join("dst");
                none()
            }),
            "overlaps the raw_root",
        ),
        (
            "--from inside a cold root",
            Box::new(move |fixture, request, _| {
                let inside = fixture.dir.join("cold-b/old");
                std::fs::create_dir(&inside).unwrap();
                request.from = inside;
                none()
            }),
            "overlaps the cold root",
        ),
        (
            "a writer on the source",
            Box::new(move |fixture, _, _| {
                Box::new(
                    ParquetRawStore::new(fixture.src_raw())
                        .writer("binance-um", RotationPolicy::default())
                        .unwrap(),
                )
            }),
            "a writer holds source binance-um of --from",
        ),
        (
            "a held pass lock",
            Box::new(move |fixture, _, _| {
                Box::new(replica::try_replication_lock(&fixture.hot()).unwrap())
            }),
            "another replication pass holds",
        ),
        (
            "a writer on hot",
            Box::new(move |fixture, _, _| {
                Box::new(
                    fixture
                        .tiered()
                        .writer(ARCHIVE_SOURCE, RotationPolicy::default())
                        .unwrap(),
                )
            }),
            "open the writer of source binance-archive on hot",
        ),
        (
            "no copy ledger",
            Box::new(move |fixture, _, _| {
                std::fs::remove_file(copy_ledger::ledger_path(&fixture.hot())).unwrap();
                none()
            }),
            "does not exist: the adoption records every copy in it",
        ),
        (
            "a target without its marker",
            Box::new(move |fixture, _, _| {
                std::fs::remove_file(fixture.dir.join("cold-b/.mie/root")).unwrap();
                none()
            }),
            "has no root marker: the adoption refuses",
        ),
        (
            "seal debris in the source",
            Box::new(move |fixture, _, _| {
                let path = replica::list_sealed(&fixture.src_raw()).unwrap().remove(0);
                let dir = fixture.src_raw().join(partition_of(&path));
                std::fs::write(dir.join(".part-00042.parquet.tmp"), b"x").unwrap();
                none()
            }),
            "holds the debris of an interrupted seal",
        ),
        (
            "a source manifest without its data",
            Box::new(move |fixture, _, _| {
                let path = replica::list_sealed(&fixture.src_raw())
                    .unwrap()
                    .pop()
                    .unwrap();
                std::fs::remove_file(fixture.src_raw().join(path)).unwrap();
                none()
            }),
            "the manifest is present without its data",
        ),
        (
            "source data that does not match its manifest",
            Box::new(move |fixture, _, _| {
                use std::os::unix::fs::PermissionsExt;
                let path = replica::list_sealed(&fixture.src_raw())
                    .unwrap()
                    .pop()
                    .unwrap();
                let data = fixture.src_raw().join(path);
                let mut bytes = std::fs::read(&data).unwrap();
                bytes[10] ^= 0x01;
                std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o644)).unwrap();
                std::fs::write(&data, bytes).unwrap();
                none()
            }),
            "the data does not match its manifest",
        ),
        (
            "a pending import in the source",
            Box::new(move |fixture, _, _| {
                std::fs::write(fixture.src().join("ledger/PENDING"), b"x").unwrap();
                none()
            }),
            "an import into the store to adopt is pending",
        ),
        (
            "an orphan archive part",
            Box::new(move |fixture, _, _| {
                let dir = fixture.src().join("ledger/aggTrades");
                let entry = std::fs::read_dir(&dir).unwrap().next().unwrap().unwrap();
                std::fs::remove_file(entry.path()).unwrap();
                none()
            }),
            "that no import entry in --import-ledger lists",
        ),
        (
            "archive parts without --import-ledger",
            Box::new(move |_, request, _| {
                request.import_ledger = None;
                none()
            }),
            "is an archive part, and no --import-ledger is given",
        ),
        (
            "hot's journal is not a prefix",
            Box::new(move |fixture, _, config| {
                std::fs::write(&config.paths.journal, b"{\"type\":\"other\"}\n").unwrap();
                let _ = fixture;
                none()
            }),
            "is not a byte prefix of --journal",
        ),
        (
            "an entry on hot with other bytes",
            Box::new(move |fixture, _, _| {
                let dir = fixture.src().join("ledger/aggTrades");
                let entry = std::fs::read_dir(&dir).unwrap().next().unwrap().unwrap();
                let dest = fixture.dir.join("dst/ledger/aggTrades");
                std::fs::create_dir_all(&dest).unwrap();
                std::fs::write(dest.join(entry.file_name()), b"other").unwrap();
                none()
            }),
            "differs from the one in --import-ledger",
        ),
    ];
    let dir = TempDir::new("adopt-refusals");
    let pristine = dir.path().join("pristine");
    std::fs::create_dir(&pristine).unwrap();
    Fixture::new(&pristine);
    for (name, setup, expected) in cases {
        let case = dir.path().join(name.replace(' ', "-"));
        copy_tree(&pristine, &case);
        // The configurations name the pristine paths: rewrite them.
        for toml in ["dst/ingest.toml", "dst/archive.toml"] {
            let path = case.join(toml);
            let text = std::fs::read_to_string(&path)
                .unwrap()
                .replace(&pristine.display().to_string(), &case.display().to_string());
            std::fs::write(path, text).unwrap();
        }
        let fixture = Fixture {
            dir: case.clone(),
            archive: fake_archive(),
            src_ingest: case.join("src.toml"),
            src_archive: case.join("src-archive.toml"),
            dst_ingest: case.join("dst/ingest.toml"),
            dst_archive: case.join("dst/archive.toml"),
            config: IngestConfig::load(&case.join("dst/ingest.toml")).unwrap(),
            archive_config: ArchiveConfig::load(&case.join("dst/archive.toml")).unwrap(),
        };
        let mut request = fixture.request();
        let mut config = fixture.config.clone();
        let held = setup(&fixture, &mut request, &mut config);
        let before = trees(&fixture, &fixture.src());
        let (pass, out) = adopt_with(&config, &request, &fixture.probe());
        drop(held);
        assert!(!pass, "{name}: {out}");
        assert!(
            out.contains("FAIL: ") && out.contains(expected),
            "{name}: {out}"
        );
        assert!(!out.contains("PASS"), "{name}: {out}");
        assert_eq!(trees(&fixture, &fixture.src()), before, "{name}");
    }

    // Shared devices, through the probe.
    let case = dir.path().join("shared-device");
    std::fs::create_dir(&case).unwrap();
    let fixture = Fixture::new(&case);
    let before = trees(&fixture, &fixture.src());
    let shared = MapProbe(vec![
        (fixture.hot(), 1),
        (case.join("cold-a"), 2),
        (case.join("cold-b"), 2),
    ]);
    let (pass, out) = adopt_with(&fixture.config, &fixture.request(), &shared);
    assert!(!pass && out.contains("both on device 2"), "{out}");
    assert_eq!(trees(&fixture, &fixture.src()), before);
}

/// The AW1 reports of `tools/tier-reports.sh` over the windows, paths
/// below `dir` stripped.
fn reports(dir: &Path, ingest: &Path, archive: &Path) -> String {
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

/// The `dataset <version>` lines of a report text.
fn datasets(text: &str) -> BTreeSet<&str> {
    text.lines()
        .filter_map(|line| line.trim_start().strip_prefix("dataset "))
        .collect()
}

/// Swaps `root` for an empty directory while `body` runs.
fn emptied<T>(roots: &[PathBuf], body: impl FnOnce() -> T) -> T {
    for root in roots {
        let aside = root.with_extension("aside");
        std::fs::rename(root, &aside).unwrap();
        std::fs::create_dir(root).unwrap();
    }
    let result = body();
    for root in roots {
        std::fs::remove_dir(root).unwrap();
        std::fs::rename(root.with_extension("aside"), root).unwrap();
    }
    result
}

/// AW1 offline: `mie replay --source live`, `mie replay --source archive`
/// and `mie equivalence` print byte-identical reports before (single-root
/// configurations over the source) and after the adoption (tiered
/// configurations). AW4 offline: with one copy target empty they are
/// still identical; with both empty every report fails with `Integrity`
/// naming a removed file, never a shorter report or another dataset
/// version.
#[test]
fn the_reports_are_identical_after_the_adoption_and_from_either_target() {
    let dir = TempDir::new("adopt-aw1");
    let fixture = Fixture::new(dir.path());
    let baseline = reports(dir.path(), &fixture.src_ingest, &fixture.src_archive);
    for needed in ["dataset ", "event-stream ", "EQUIVALENT", "\nPASS\n"] {
        assert!(baseline.contains(needed), "{needed:?} in {baseline}");
    }
    assert!(
        !baseline.contains(&dir.path().display().to_string()),
        "{baseline}"
    );
    assert_pass(&fixture.adopt(&fixture.request()));
    let after = reports(dir.path(), &fixture.dst_ingest, &fixture.dst_archive);
    assert_eq!(after, baseline);

    // AW4: either target alone serves every removed file.
    let (cold_a, cold_b) = (dir.path().join("cold-a"), dir.path().join("cold-b"));
    for root in [&cold_a, &cold_b] {
        let alone = emptied(std::slice::from_ref(root), || {
            reports(dir.path(), &fixture.dst_ingest, &fixture.dst_archive)
        });
        assert_eq!(alone, baseline, "{} empty", root.display());
    }
    let none = emptied(&[cold_a, cold_b], || {
        reports(dir.path(), &fixture.dst_ingest, &fixture.dst_archive)
    });
    let blocks: Vec<&str> = none.split("== ").filter(|b| !b.is_empty()).collect();
    assert_eq!(blocks.len(), 3 * WINDOWS.len() / 2, "{none}");
    for block in blocks {
        assert!(!block.ends_with("exit 0\n"), "{block}");
        assert!(
            block.contains("raw store integrity check failed")
                && block.contains("is missing on every root"),
            "{block}"
        );
        assert!(!block.lines().any(|l| l == "PASS"), "{block}");
    }
    let known = datasets(&baseline);
    assert!(datasets(&none).is_subset(&known), "{none}");
}

/// AC5: until the recall (#191), the writer guard refuses an archive import
/// whose rows land in a partition recorded as removed from hot: the
/// `metrics` file of 2026-10-03, whose partition holds the 23:55 row of
/// the previous file (ADR-034 D3). Hot's layout, ledger and import entries
/// stay byte-identical; `PENDING` remains (ADR-034 D4), and a second run is
/// refused the same way.
#[test]
fn an_import_into_a_removed_date_is_refused_and_writes_nothing() {
    let dir = TempDir::new("adopt-ac5");
    let fixture = Fixture::new(dir.path());
    assert_pass(&fixture.adopt(&fixture.request()));
    let partition = "source=binance-archive/instrument=BTCUSDT/stream=metrics/date=2026-10-03";
    let records = copy_ledger::read(&fixture.hot()).unwrap().unwrap().records;
    assert!(records.iter().any(|r| matches!(
        &r.kind,
        Kind::Removed { partition: p } if p == partition
    )));
    let hot = fixture.hot();
    let ledger_dir = fixture.dir.join("dst/ledger");
    let entries = |dir: &Path| -> Vec<(String, Vec<u8>)> {
        files(dir)
            .into_iter()
            .filter(|(name, _)| name != "PENDING")
            .collect()
    };
    let (layout, ledger, before_entries) = (
        files(&hot),
        std::fs::read(copy_ledger::ledger_path(&hot)).unwrap(),
        entries(&ledger_dir),
    );
    let day = D0 / DAY_MS - 3;
    for attempt in 0..2 {
        let error = import(
            &fixture.archive_config,
            &fixture.archive,
            ArchiveStream::Metrics,
            day,
            day,
        )
        .expect_err("refused");
        for text in [partition, "removed from hot", "recall (ADR-062 D6)", "#191"] {
            assert!(
                error.contains(text),
                "attempt {attempt}: {text:?} in {error}"
            );
        }
        assert_eq!(files(&hot), layout, "attempt {attempt}");
        assert_eq!(
            std::fs::read(copy_ledger::ledger_path(&hot)).unwrap(),
            ledger
        );
        assert_eq!(entries(&ledger_dir), before_entries);
        assert!(ledger_dir.join("PENDING").exists(), "attempt {attempt}");
    }
}

#[test]
fn usage_errors_exit_2() {
    for args in [
        vec!["store", "adopt", "--config", "x.toml"],
        vec!["store", "adopt"],
        vec!["store", "adopt", "--config", "x.toml", "--from"],
        vec![
            "store", "adopt", "--config", "x.toml", "--from", "y", "--root", "z",
        ],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_mie"))
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}");
    }
}
