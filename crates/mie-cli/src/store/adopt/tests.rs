//! The adoption under its hooks: a crash at every occurrence of every
//! step, then a re-run; recovery before the path rule; the locks held for
//! the run.

use super::*;
use crate::config::IngestConfig;
use mie_adapter_parquet::ParquetRawStore;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawRecord, RawRecordSink, RawRecordSource, RawSelection, RawStreamKey};
use std::cell::{Cell, RefCell};
use std::io;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

/// 2026-10-06T00:00:00Z.
const D0: i64 = 1_791_244_800_000;
const DAY: i64 = 86_400_000;
/// The first hot date of the fixture: D0 − 2 days.
const FIRST_HOT: &str = "2026-10-04";

/// A fresh directory, removed on drop.
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mie-cli-adopt-unit-{tag}-{}-{n}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Device ids by canonical path: hot 1, the copy targets 2 and 3.
struct Probe(Vec<(PathBuf, u64)>);

impl DeviceProbe for Probe {
    fn device_id(&self, root: &Path) -> io::Result<u64> {
        let wanted = fs::canonicalize(root)?;
        self.0
            .iter()
            .find(|(path, _)| fs::canonicalize(path).is_ok_and(|p| p == wanted))
            .map(|(_, id)| *id)
            .ok_or_else(|| io::Error::other(format!("{} unknown", root.display())))
    }
}

/// A closed single-root store under `src/`: live `aggTrade` parts on an
/// old date and on a hot-window date, archive `aggTrades` parts of two
/// archive files (old and hot-window) with their import entries, and a
/// journal. Under `dst/`, a tiered store with hot initialized and two
/// marked copy targets.
struct Fixture {
    dir: Dir,
    src: PathBuf,
    config: IngestConfig,
    hot: PathBuf,
    cold: [PathBuf; 2],
    probe: Probe,
}

impl Fixture {
    fn new(tag: &str) -> Self {
        let dir = Dir::new(tag);
        let src = dir.0.join("src");
        let dst = dir.0.join("dst");
        let hot = dst.join("raw");
        let cold = [dst.join("cold-a"), dst.join("cold-b")];
        let roots = format!(
            "cold_roots = [\n{{ path = \"{}\", copy_target = true }},\n\
             {{ path = \"{}\", copy_target = true }},\n]\n",
            cold[0].display(),
            cold[1].display()
        );
        let config = IngestConfig::parse(&format!(
            "[instrument]\nsymbol = \"BTCUSDT\"\nsource = \"binance-um\"\n[paths]\n\
             raw_root = \"{}\"\njournal = \"{}\"\n{roots}[binance]\n\
             ws_base_url = \"wss://fake.invalid/ws\"\nrest_base_url = \"https://fake.invalid\"\n\
             streams = [\"aggTrade\"]\n",
            hot.display(),
            dst.join("journal.jsonl").display(),
        ))
        .unwrap();
        fs::create_dir_all(&dst).unwrap();
        fs::write(
            dst.join("archive.toml"),
            format!(
                "[instrument]\nsymbol = \"BTCUSDT\"\n[paths]\nraw_root = \"{}\"\n\
                 import_ledger = \"{}\"\nstaging = \"{}\"\n{roots}",
                hot.display(),
                dst.join("ledger").display(),
                dst.join("staging").display(),
            ),
        )
        .unwrap();

        let raw = src.join("raw");
        seal(&raw, "binance-um", "aggTrade", D0 - 5 * DAY, 4);
        seal(&raw, "binance-um", "aggTrade", D0 + 3_600_000, 4);
        for (day, count) in [(D0 - 6 * DAY, 4), (D0 - DAY, 2)] {
            let parts = seal(&raw, ARCHIVE_SOURCE, "aggTrades", day, count);
            write_entry(&raw, &src.join("ledger"), &parts);
        }
        fs::write(
            src.join("journal.jsonl"),
            "{\"type\":\"run_start\",\"run_id\":\"r1\"}\n{\"type\":\"sealed\",\"run_id\":\"r1\"}\n",
        )
        .unwrap();

        fs::create_dir_all(&hot).unwrap();
        root_marker::create_meta(&hot, HOT_ID, true).unwrap();
        for (root, id) in cold.iter().zip(["cold-a", "cold-b"]) {
            fs::create_dir_all(root).unwrap();
            root_marker::create_meta(root, id, false).unwrap();
        }
        Self {
            probe: Probe(vec![
                (hot.clone(), 1),
                (cold[0].clone(), 2),
                (cold[1].clone(), 3),
            ]),
            dir,
            src,
            config,
            hot,
            cold,
        }
    }

    fn raw(&self) -> PathBuf {
        self.src.join("raw")
    }

    fn request(&self) -> AdoptRequest {
        AdoptRequest {
            from: self.raw(),
            journal: Some(self.src.join("journal.jsonl")),
            import_ledger: Some(self.src.join("ledger")),
            archive_config: Some(self.dir.0.join("dst/archive.toml")),
        }
    }

    fn adopt(&self) -> AdoptReport {
        run(
            &self.config,
            &self.request(),
            FIRST_HOT,
            &self.probe,
            &|| D0 + 1,
        )
    }

    fn records(&self) -> Vec<Record> {
        copy_ledger::read(&self.hot).unwrap().unwrap().records
    }

    fn tiered(&self) -> ParquetRawStore {
        ParquetRawStore::tiered(&self.hot, self.cold.to_vec())
    }
}

/// Seals `count` records of `stream` from `start`, one second apart, two
/// per part, into the single root `raw`; returns the new data paths.
fn seal(raw: &Path, source: &str, stream: &str, start: i64, count: i64) -> Vec<String> {
    let before = replica::list_sealed(raw).unwrap_or_default();
    let key = RawStreamKey::new(source, "BTCUSDT", stream).unwrap();
    let policy = RotationPolicy {
        max_rows: 2,
        ..RotationPolicy::default()
    };
    let mut writer = ParquetRawStore::new(raw).writer(source, policy).unwrap();
    for i in 0..count {
        writer
            .append(
                &key,
                RawRecord {
                    event_time: EventTime::from_millis(start + i * 1_000),
                    capture: None,
                    payload: format!("{stream}-{start}-{i}").into_bytes(),
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

/// Writes the import entry of the daily `aggTrades` file listing `parts`,
/// all of one date.
fn write_entry(raw: &Path, ledger: &Path, parts: &[String]) {
    let date = partition_of(&parts[0])
        .rsplit_once("date=")
        .unwrap()
        .1
        .to_owned();
    let name = format!("BTCUSDT-aggTrades-{date}.zip");
    let mut text = format!(
        "mie-archive-import 1\narchive data/futures/um/daily/aggTrades/BTCUSDT/{name}\n\
         sha256 {}\nstream binance-archive/BTCUSDT/aggTrades\nperiod {date}\nheader -\nrows {}\n",
        "ab".repeat(32),
        2 * parts.len()
    );
    for part in parts {
        let (_, file) = replica::sealed_manifest(raw, part).unwrap().unwrap();
        text.push_str(&format!("file {part} {} {}\n", file.rows, file.sha256));
    }
    let dir = ledger.join("aggTrades");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(format!("{name}.import")), text).unwrap();
}

/// Every file below `root` with its bytes, sorted; `.lock` files and the
/// copy ledger are left out.
fn files_under(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(dir: &Path, base: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, base, out);
            } else {
                let rel = path.strip_prefix(base).unwrap().display().to_string();
                if !rel.ends_with(".lock") && !rel.ends_with("copy-ledger") {
                    out.push((rel, fs::read(&path).unwrap()));
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// The selections compared with the source: each date, the whole range
/// and a window across the first hot date.
fn selections() -> Vec<RawSelection> {
    let streams: BTreeSet<RawStreamKey> = [
        RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap(),
        RawStreamKey::new(ARCHIVE_SOURCE, "BTCUSDT", "aggTrades").unwrap(),
    ]
    .into_iter()
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
    let mut out: Vec<RawSelection> = (-7..=1)
        .map(|d| window(D0 + d * DAY, D0 + (d + 1) * DAY))
        .collect();
    out.push(window(0, D0 + 2 * DAY));
    out.push(window(D0 - 3 * DAY, D0 - DAY));
    out
}

/// The `manifest` records per path, the `copy` records per path and root,
/// and the `removed` records per partition.
type Counts = (
    BTreeMap<String, usize>,
    BTreeMap<(String, String), usize>,
    BTreeMap<String, usize>,
);

fn counts(records: &[Record]) -> Counts {
    let (mut manifests, mut copies, mut removed) =
        (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
    for record in records {
        match &record.kind {
            Kind::Manifest { text } => {
                let path = replica::parse_manifest(text).unwrap().relative_path;
                *manifests.entry(path).or_default() += 1;
            }
            Kind::Copy { path, root } => {
                *copies.entry((path.clone(), root.clone())).or_default() += 1;
            }
            Kind::Removed { partition } => *removed.entry(partition.clone()).or_default() += 1,
            _ => {}
        }
    }
    (manifests, copies, removed)
}

/// Asserts the end state of a passed adoption (AC1 in small): `select`
/// equals the source's, one `manifest` per path, k `copy` records, one
/// `removed` per old partition, and hot holds the hot-window partitions
/// only.
fn assert_adopted(fixture: &Fixture) {
    let source = ParquetRawStore::new(fixture.raw());
    let tiered = fixture.tiered();
    for selection in selections() {
        assert_eq!(
            tiered.select(&selection).unwrap(),
            source.select(&selection).unwrap(),
            "{selection:?}"
        );
    }
    let paths = replica::list_sealed(&fixture.raw()).unwrap();
    let (manifests, copies, removed) = counts(&fixture.records());
    assert_eq!(manifests.len(), paths.len());
    assert!(manifests.values().all(|&n| n == 1), "{manifests:?}");
    assert_eq!(copies.len(), 2 * paths.len());
    assert!(copies.values().all(|&n| n == 1), "{copies:?}");
    let old: BTreeSet<&str> = paths
        .iter()
        .map(|p| partition_of(p))
        .filter(|p| date_of(p) < FIRST_HOT)
        .collect();
    assert_eq!(
        removed.keys().map(String::as_str).collect::<BTreeSet<_>>(),
        old
    );
    assert!(removed.values().all(|&n| n == 1), "{removed:?}");
    let on_hot: Vec<String> = paths
        .iter()
        .filter(|p| date_of(partition_of(p)) >= FIRST_HOT)
        .cloned()
        .collect();
    assert_eq!(replica::list_sealed(&fixture.hot).unwrap(), on_hot);
}

/// What the copy ledger knows: the partitions recorded removed, and per
/// partition the highest part a `copy` or `manifest` record names.
fn ledger_view(records: &[Record]) -> (BTreeSet<String>, BTreeMap<String, u32>) {
    let (mut removed, mut highest) = (BTreeSet::new(), BTreeMap::<String, u32>::new());
    for record in records {
        let path = match &record.kind {
            Kind::Removed { partition } => {
                removed.insert(partition.clone());
                continue;
            }
            Kind::Manifest { text } => replica::parse_manifest(text).unwrap().relative_path,
            Kind::Copy { path, .. } => path.clone(),
            _ => continue,
        };
        let part: u32 = path.rsplit_once("part-").unwrap().1[..5].parse().unwrap();
        let entry = highest
            .entry(partition_of(&path).to_owned())
            .or_insert(part);
        *entry = (*entry).max(part);
    }
    (removed, highest)
}

/// Between a crash and the re-run, an append into a partition the ledger
/// records as removed, or one where the ledger knows a part hot lacks, is
/// refused by the writer guard and creates nothing. Returns how many
/// appends of the second kind were refused.
fn assert_guard_refuses(fixture: &Fixture) -> usize {
    let (removed, highest) = ledger_view(&fixture.records());
    let mut partly = 0;
    let partitions: BTreeSet<String> = replica::list_sealed(&fixture.raw())
        .unwrap()
        .iter()
        .map(|p| partition_of(p).to_owned())
        .collect();
    for partition in partitions {
        let hot_next = replica::list_sealed(&fixture.hot)
            .unwrap()
            .iter()
            .filter(|p| partition_of(p) == partition)
            .count() as u32;
        let expected = if removed.contains(&partition) {
            "is recorded as removed from hot"
        } else if highest.get(&partition).is_some_and(|&h| h >= hot_next) {
            partly += 1;
            "hot's next part number is"
        } else {
            continue;
        };
        let segments: Vec<&str> = partition.split('/').collect();
        let value = |i: usize, prefix: &str| segments[i].strip_prefix(prefix).unwrap().to_owned();
        let (source, stream, date) = (value(0, "source="), value(2, "stream="), value(3, "date="));
        let key = RawStreamKey::new(&source, "BTCUSDT", &stream).unwrap();
        let day = mie_adapter_binance::archive::catalog::parse_day(&date).unwrap();
        let before = files_under(&fixture.hot);
        let mut writer = fixture
            .tiered()
            .writer(&source, RotationPolicy::default())
            .unwrap();
        let error = writer
            .append(
                &key,
                RawRecord {
                    event_time: EventTime::from_millis(day * DAY + 7),
                    capture: None,
                    payload: b"late".to_vec(),
                },
            )
            .unwrap_err();
        drop(writer);
        assert!(
            matches!(&error, RawStoreError::Integrity(m) if m.contains(expected) && m.contains(&partition)),
            "{partition}: {error}"
        );
        assert_eq!(files_under(&fixture.hot), before, "{partition}");
    }
    partly
}

/// How often each step occurs in an adoption without a crash.
fn step_counts() -> BTreeMap<String, usize> {
    let fixture = Fixture::new("adopt-count");
    let seen = Rc::new(RefCell::new(BTreeMap::<String, usize>::new()));
    {
        let seen = seen.clone();
        ON_STEP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |step| {
                *seen.borrow_mut().entry(format!("{step:?}")).or_default() += 1;
            }));
        });
    }
    let report = fixture.adopt();
    ON_STEP.with(|hook| hook.borrow_mut().take());
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    seen.take()
}

/// The acceptance item of #190: a crash at every occurrence of every
/// `AdoptStep`, then a re-run with the same request, passes and ends in the
/// state of an adoption that never crashed; in between, the guard refuses
/// appends into old and partly copied partitions.
#[test]
fn a_crash_at_every_step_is_completed_by_a_re_run() {
    let reference = Fixture::new("adopt-reference");
    let report = reference.adopt();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert_adopted(&reference);
    let roots = |fixture: &Fixture| {
        [&fixture.hot, &fixture.cold[0], &fixture.cold[1]].map(|root| files_under(root))
    };
    let expected = roots(&reference);
    let source_tree = files_under(&reference.src);

    let counts = step_counts();
    let mut partly = 0;
    for step in AdoptStep::ALL {
        let occurrences = counts.get(&format!("{step:?}")).copied().unwrap_or(0);
        assert!(occurrences > 0, "{step:?} never occurs in the fixture");
        for n in 0..occurrences {
            let fixture = Fixture::new("adopt-crash");
            let source = files_under(&fixture.src);
            let seen = Rc::new(Cell::new(0));
            {
                let seen = seen.clone();
                ON_STEP.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move |at| {
                        if at == step {
                            if seen.get() == n {
                                CRASH_AT.with(|crash| crash.set(Some(step)));
                            }
                            seen.set(seen.get() + 1);
                        }
                    }));
                });
            }
            let crashed = fixture.adopt();
            ON_STEP.with(|hook| hook.borrow_mut().take());
            assert_eq!(
                crashed.problems,
                ["simulated crash: the adoption stopped"],
                "{step:?} #{n}"
            );
            partly += assert_guard_refuses(&fixture);

            let report = fixture.adopt();
            assert_eq!(
                report.problems,
                Vec::<String>::new(),
                "{step:?} #{n}: {report:?}"
            );
            assert_adopted(&fixture);
            assert_eq!(roots(&fixture), expected, "{step:?} #{n}");
            assert_eq!(files_under(&fixture.src), source, "{step:?} #{n}");
        }
    }
    assert_eq!(source_tree, files_under(&reference.src));
    assert!(
        partly > 0,
        "no crash left a partly copied hot-window partition"
    );
}

/// The recovery runs first: an ingest's crashed seal on hot (published data
/// and its pending manifest) is rolled forward when the adoption takes the
/// writer, and the path rule then refuses the differing source file;
/// nothing on hot is deleted.
#[test]
fn recovery_runs_before_the_path_rule() {
    let fixture = Fixture::new("adopt-recovery");
    // Another seal of the hot-window live partition's first part.
    let other = Dir::new("adopt-recovery-other");
    let parts = seal(&other.0, "binance-um", "aggTrade", D0 + 7_200_000, 2);
    let path = parts[0].clone();
    let manifest = format!("{}.manifest", path.strip_suffix(".parquet").unwrap());
    let partition = partition_of(&path).to_owned();
    fs::create_dir_all(fixture.hot.join(&partition)).unwrap();
    fs::copy(other.0.join(&path), fixture.hot.join(&path)).unwrap();
    let pending = fixture
        .hot
        .join(&partition)
        .join(".part-00000.manifest.tmp");
    fs::copy(other.0.join(&manifest), &pending).unwrap();

    let report = fixture.adopt();
    assert!(
        report.lines.iter().any(|l| l
            .starts_with("recovery of source binance-um on hot: rolled forward")
            && l.contains(&path)),
        "{report:?}"
    );
    assert_eq!(report.problems.len(), 1, "{report:?}");
    assert!(
        report.problems[0].starts_with("raw store integrity check failed: ")
            && report.problems[0].contains(&path)
            && report.problems[0].contains("differs from hot"),
        "{report:?}"
    );
    // Rolled forward, not deleted as copy debris.
    assert_eq!(
        fs::read(fixture.hot.join(&path)).unwrap(),
        fs::read(other.0.join(&path)).unwrap()
    );
    assert_eq!(
        fs::read(fixture.hot.join(&manifest)).unwrap(),
        fs::read(other.0.join(&manifest)).unwrap()
    );
    // Nothing was adopted.
    let (manifests, copies, removed) = counts(&fixture.records());
    assert!(manifests.is_empty() && copies.is_empty() && removed.is_empty());
}

/// The pass lock and the hot writers are held for the whole run: another
/// pass and another writer of hot are refused while it runs.
#[test]
fn the_pass_lock_and_the_writers_are_held_for_the_run() {
    let fixture = Fixture::new("adopt-locks");
    let hot = fixture.hot.clone();
    let cold = fixture.cold.to_vec();
    let checked = Rc::new(Cell::new(0));
    {
        let checked = checked.clone();
        ON_STEP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |_| {
                assert!(matches!(
                    replica::try_replication_lock(&hot),
                    Err(RawStoreError::Locked(_))
                ));
                for source in ["binance-um", ARCHIVE_SOURCE] {
                    assert!(matches!(
                        ParquetRawStore::tiered(&hot, cold.clone())
                            .writer(source, RotationPolicy::default()),
                        Err(RawStoreError::Locked(_))
                    ));
                }
                checked.set(checked.get() + 1);
            }));
        });
    }
    let report = fixture.adopt();
    ON_STEP.with(|hook| hook.borrow_mut().take());
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert!(checked.get() > 0);
    // Released at the end.
    drop(replica::try_replication_lock(&fixture.hot).unwrap());
    fixture
        .tiered()
        .writer("binance-um", RotationPolicy::default())
        .unwrap();
}
