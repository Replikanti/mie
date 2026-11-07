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

/// Between a crash and the re-run, an append into a source partition that
/// the crashed adoption has *exposed* (the ledger knows one of its parts,
/// or a copy target holds one) while hot does not hold all its source
/// parts yet, or into one recorded as removed, is refused by the writer
/// guard and creates nothing: otherwise the capture could seal a path a
/// target holds, or will hold, with other bytes (D5). Exposure is read
/// from the roots and the ledger, not from the guard's own rule. Returns
/// how many exposed, incomplete partitions were tried.
fn assert_guard_refuses(fixture: &Fixture) -> usize {
    let (removed, highest) = ledger_view(&fixture.records());
    let mut partly = 0;
    let source = replica::list_sealed(&fixture.raw()).unwrap();
    let on_hot = replica::list_sealed(&fixture.hot).unwrap();
    let on_targets: Vec<String> = fixture
        .cold
        .iter()
        .flat_map(|root| replica::list_sealed(root).unwrap())
        .collect();
    let partitions: BTreeSet<String> = source.iter().map(|p| partition_of(p).to_owned()).collect();
    for partition in partitions {
        let in_partition = |p: &&String| partition_of(p) == partition;
        let exposed =
            highest.contains_key(&partition) || on_targets.iter().any(|p| in_partition(&p));
        let complete = source
            .iter()
            .filter(in_partition)
            .all(|p| on_hot.contains(p));
        let expected = if removed.contains(&partition) {
            "is recorded as removed from hot"
        } else if exposed && !complete {
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
    assert_guard_releases(&reference);

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
            assert_guard_releases(&fixture);
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

/// Review of #200, finding 1: a stop right after a copy target published a
/// hot-window part, before any `copy` record, must still leave the writer
/// guard refusing that partition: the ledger already knows every part of
/// it, so a restarted capture can never seal the published path with other
/// bytes, and the re-run completes the adoption.
#[test]
fn a_stop_right_after_a_target_publish_keeps_the_partition_guarded() {
    let fixture = Fixture::new("adopt-publish-window");
    let partition = "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06";
    let published = fixture.cold[0].join(partition).join("part-00000.manifest");
    {
        let published = published.clone();
        ON_STEP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |at| {
                if at == AdoptStep::AfterTargetPublish && published.exists() {
                    CRASH_AT.with(|crash| crash.set(Some(at)));
                }
            }));
        });
    }
    let crashed = fixture.adopt();
    ON_STEP.with(|hook| hook.borrow_mut().take());
    assert_eq!(crashed.problems, ["simulated crash: the adoption stopped"]);
    let path = format!("{partition}/part-00000.parquet");
    assert!(fixture.cold[0].join(&path).exists());
    assert!(!fixture.hot.join(&path).exists());

    // The capture restarts before the re-run.
    let before = files_under(&fixture.hot);
    let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
    let mut writer = fixture
        .tiered()
        .writer("binance-um", RotationPolicy::default())
        .unwrap();
    let error = writer
        .append(
            &key,
            RawRecord {
                event_time: EventTime::from_millis(D0 + 7_200_000),
                capture: None,
                payload: b"live-after-crash".to_vec(),
            },
        )
        .unwrap_err();
    drop(writer);
    assert!(
        matches!(&error, RawStoreError::Integrity(m) if m.contains(partition)
            && m.contains("hot's next part number is 0")),
        "{error}"
    );
    assert_eq!(files_under(&fixture.hot), before);

    let report = fixture.adopt();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert_adopted(&fixture);
    assert_guard_releases(&fixture);
}

/// Review of #200, finding 2: a source manifest without its data, or data
/// that does not match its manifest, is refused in the check phase, before
/// any record or copy is written.
#[test]
fn a_damaged_source_part_is_refused_before_anything_is_written() {
    for damage in ["data missing", "data differs"] {
        let fixture = Fixture::new("adopt-damaged-source");
        let last = replica::list_sealed(&fixture.raw()).unwrap().pop().unwrap();
        let data = fixture.raw().join(&last);
        if damage == "data missing" {
            fs::remove_file(&data).unwrap();
        } else {
            use std::os::unix::fs::PermissionsExt;
            let mut bytes = fs::read(&data).unwrap();
            bytes[10] ^= 0x01;
            fs::set_permissions(&data, fs::Permissions::from_mode(0o644)).unwrap();
            fs::write(&data, bytes).unwrap();
        }
        assert_eq!(
            replica::list_unsealed(&fixture.raw()).unwrap(),
            Vec::<String>::new()
        );
        let before = [&fixture.hot, &fixture.cold[0], &fixture.cold[1]].map(|r| files_under(r));
        let ledger = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
        let report = fixture.adopt();
        assert_eq!(report.problems.len(), 1, "{damage}: {report:?}");
        let expected = if damage == "data missing" {
            "the manifest is present without its data"
        } else {
            "the data does not match its manifest"
        };
        assert!(
            report.problems[0].contains(&last)
                && report.problems[0].contains(expected)
                && report.problems[0].contains("nothing was written"),
            "{damage}: {report:?}"
        );
        assert_eq!(
            fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap(),
            ledger,
            "{damage}"
        );
        assert_eq!(
            [&fixture.hot, &fixture.cold[0], &fixture.cold[1]].map(|r| files_under(r)),
            before,
            "{damage}"
        );
    }
}

/// Review of #200, re-checks: a run that stopped after its `manifest`
/// records, re-run against a store without one of the recorded files (a
/// backup, another `--from`), refuses and writes nothing, whether the file
/// is on no root or published on one copy target only without its `copy`
/// record. Passing would leave a record that keeps the partition guarded
/// for good. With the store it read, the re-run completes and the guard
/// releases the partition.
#[test]
fn a_re_run_against_a_store_without_a_recorded_file_is_refused() {
    let partition = "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06";
    for case in ["on no root", "on one copy target"] {
        let fixture = Fixture::new("adopt-shrunk-source");
        let published = fixture.cold[0].join(partition).join("part-00001.manifest");
        {
            let published = published.clone();
            ON_STEP.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |at| {
                    if at == AdoptStep::AfterTargetPublish
                        && (case == "on no root" || published.exists())
                    {
                        CRASH_AT.with(|crash| crash.set(Some(at)));
                    }
                }));
            });
        }
        let crashed = fixture.adopt();
        ON_STEP.with(|hook| hook.borrow_mut().take());
        assert_eq!(
            crashed.problems,
            ["simulated crash: the adoption stopped"],
            "{case}"
        );
        assert_eq!(published.exists(), case == "on one copy target", "{case}");

        let aside = fixture.dir.0.join("aside");
        fs::create_dir_all(&aside).unwrap();
        for name in ["part-00001.parquet", "part-00001.manifest"] {
            let path = fixture.raw().join(partition).join(name);
            fs::copy(&path, aside.join(name)).unwrap();
            fs::remove_file(path).unwrap();
        }
        let missing = format!("{partition}/part-00001.parquet");
        let roots = |fixture: &Fixture| {
            [&fixture.hot, &fixture.cold[0], &fixture.cold[1]].map(|root| files_under(root))
        };
        let (before, ledger) = (
            roots(&fixture),
            fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap(),
        );
        for attempt in 0..2 {
            let report = fixture.adopt();
            assert_eq!(report.problems.len(), 1, "{case} #{attempt}: {report:?}");
            assert!(
                report.problems[0].contains(&missing)
                    && report.problems[0]
                        .contains("which --from does not list and which is not complete")
                    && report.problems[0].contains("Nothing was written"),
                "{case} #{attempt}: {report:?}"
            );
            assert_eq!(roots(&fixture), before, "{case} #{attempt}");
            assert_eq!(
                fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap(),
                ledger,
                "{case} #{attempt}"
            );
        }
        // The guard still refuses the partition and names the way out.
        let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
        let mut writer = fixture
            .tiered()
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        let error = writer
            .append(
                &key,
                RawRecord {
                    event_time: EventTime::from_millis(D0 + 7_200_000),
                    capture: None,
                    payload: b"live".to_vec(),
                },
            )
            .unwrap_err();
        drop(writer);
        assert!(
            error.to_string().contains("with the store it read"),
            "{case}: {error}"
        );

        // With the store the stopped run read, the re-run completes.
        for name in ["part-00001.parquet", "part-00001.manifest"] {
            fs::copy(aside.join(name), fixture.raw().join(partition).join(name)).unwrap();
        }
        let report = fixture.adopt();
        assert_eq!(report.problems, Vec::<String>::new(), "{case}: {report:?}");
        assert_adopted(&fixture);
        assert_guard_releases(&fixture);
    }
}

/// The invariant a PASS guarantees (ADR-062 D10): every adopted partition
/// on hot's side, which hot then holds completely, takes a new part. Each
/// such partition gets one record appended past its parts.
fn assert_guard_releases(fixture: &Fixture) {
    let source = replica::list_sealed(&fixture.raw()).unwrap();
    let on_hot = replica::list_sealed(&fixture.hot).unwrap();
    let partitions: BTreeSet<&str> = source
        .iter()
        .map(|p| partition_of(p))
        .filter(|p| date_of(p) >= FIRST_HOT)
        .collect();
    assert!(!partitions.is_empty());
    for partition in partitions {
        assert!(
            source
                .iter()
                .filter(|p| partition_of(p) == partition)
                .all(|p| on_hot.contains(p)),
            "{partition}: hot holds every adopted part"
        );
        let segments: Vec<&str> = partition.split('/').collect();
        let value = |i: usize, prefix: &str| segments[i].strip_prefix(prefix).unwrap().to_owned();
        let (source, stream, date) = (value(0, "source="), value(2, "stream="), value(3, "date="));
        let key = RawStreamKey::new(&source, "BTCUSDT", &stream).unwrap();
        let day = mie_adapter_binance::archive::catalog::parse_day(&date).unwrap();
        let mut writer = fixture
            .tiered()
            .writer(&source, RotationPolicy::default())
            .unwrap();
        writer
            .append(
                &key,
                RawRecord {
                    event_time: EventTime::from_millis(day * DAY + 20 * 3_600_000),
                    capture: None,
                    payload: b"after the adoption".to_vec(),
                },
            )
            .unwrap_or_else(|e| panic!("{partition}: the guard still refuses after a PASS: {e}"));
        writer.close().unwrap();
    }
}

/// Review of #200, findings 5 and 6: a copy target added (or a failed one
/// replaced under a new id) after two adoptions holds no copy of the files
/// that left hot, and the pass copies from hot only. That gap does not
/// block the writes: re-adopting the first store fills its own files on
/// the new target and fails naming only the second store's files with the
/// remedy (`mie store repair`, #192, or a re-adoption); re-adopting the
/// second store then passes, and so does a third store. A file of that
/// class never reaches the writer: its partition stays recorded removed.
#[test]
fn a_target_added_after_an_adoption_is_named_with_its_remedy() {
    let fixture = Fixture::new("adopt-new-target");
    let first = fixture.adopt();
    assert_eq!(first.problems, Vec::<String>::new(), "{first:?}");
    // A second store, adopted while there are two targets.
    let other = Dir::new("adopt-new-target-other");
    seal(&other.0, "binance-um", "aggTrade", D0 - 4 * DAY, 2);
    let other_request = AdoptRequest {
        from: other.0.clone(),
        archive_config: Some(fixture.dir.0.join("dst/archive.toml")),
        ..AdoptRequest::default()
    };
    let second = run(
        &fixture.config,
        &other_request,
        FIRST_HOT,
        &fixture.probe,
        &|| D0 + 2,
    );
    assert_eq!(second.problems, Vec::<String>::new(), "{second:?}");

    // The operator adds cold-c.
    let dst = fixture.dir.0.join("dst");
    let cold_c = dst.join("cold-c");
    fs::create_dir_all(&cold_c).unwrap();
    root_marker::create_meta(&cold_c, "cold-c", false).unwrap();
    let roots = format!(
        "cold_roots = [\n{{ path = \"{}\", copy_target = true }},\n\
         {{ path = \"{}\", copy_target = true }},\n{{ path = \"{}\", copy_target = true }},\n]\n",
        fixture.cold[0].display(),
        fixture.cold[1].display(),
        cold_c.display()
    );
    let config = IngestConfig::parse(&format!(
        "[instrument]\nsymbol = \"BTCUSDT\"\nsource = \"binance-um\"\n[paths]\n\
         raw_root = \"{}\"\njournal = \"{}\"\n{roots}[binance]\n\
         ws_base_url = \"wss://fake.invalid/ws\"\nrest_base_url = \"https://fake.invalid\"\n\
         streams = [\"aggTrade\"]\n",
        fixture.hot.display(),
        dst.join("journal.jsonl").display(),
    ))
    .unwrap();
    let archive = dst.join("archive3.toml");
    fs::write(
        &archive,
        format!(
            "[instrument]\nsymbol = \"BTCUSDT\"\n[paths]\nraw_root = \"{}\"\n\
             import_ledger = \"{}\"\nstaging = \"{}\"\n{roots}",
            fixture.hot.display(),
            dst.join("ledger").display(),
            dst.join("staging").display(),
        ),
    )
    .unwrap();
    let probe = Probe(vec![
        (fixture.hot.clone(), 1),
        (fixture.cold[0].clone(), 2),
        (fixture.cold[1].clone(), 3),
        (cold_c.clone(), 4),
    ]);
    let mut out = Vec::new();
    crate::store::replicate(&config, Some(&archive), &probe, &|| D0 + 3, &mut out).unwrap();
    let old_of = |raw: &Path| -> Vec<String> {
        replica::list_sealed(raw)
            .unwrap()
            .into_iter()
            .filter(|p| date_of(partition_of(p)) < FIRST_HOT)
            .collect()
    };
    let (old_a, old_b) = (old_of(&fixture.raw()), old_of(&other.0));
    assert!(!old_a.is_empty() && !old_b.is_empty());

    // Re-adopting the first store fills its files and names the second's.
    let mut original = fixture.request();
    original.archive_config = Some(archive.clone());
    let again = run(&config, &original, FIRST_HOT, &probe, &|| D0 + 4);
    // Only the end-of-run check fails this run (the repair class does not
    // block the writes): the run must FAIL, never PASS with files short.
    let mut out = Vec::new();
    let passed =
        crate::store::adopt(&config, &original, FIRST_HOT, &probe, &|| D0 + 4, &mut out).unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(!passed && !text.contains("\nPASS\n"), "{text}");
    assert!(text.contains("FAIL: the copy ledger records "), "{text}");
    for path in &old_a {
        assert!(cold_c.join(path).exists(), "{path}");
    }
    let repair: Vec<&String> = again
        .problems
        .iter()
        .filter(|p| p.contains("Fill it with `mie store repair` (#192)"))
        .collect();
    assert_eq!(repair.len(), 1, "{again:?}");
    for path in &old_b {
        assert!(!cold_c.join(path).exists(), "{path}");
        assert!(
            repair[0].contains(&format!(
                "{path} (no copy record on cold-c, only on cold-a, cold-b)"
            )),
            "{path}: {again:?}"
        );
    }
    for path in &old_a {
        assert!(!repair[0].contains(path.as_str()), "{path}: {again:?}");
    }
    assert!(
        repair[0].contains("re-adopt the original store"),
        "{again:?}"
    );
    // That class never reaches the writer: its partitions stay removed.
    let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
    let mut writer = ParquetRawStore::tiered(
        &fixture.hot,
        config
            .paths
            .cold_roots
            .iter()
            .map(|r| r.path.clone())
            .collect(),
    )
    .writer("binance-um", RotationPolicy::default())
    .unwrap();
    let error = writer
        .append(
            &key,
            RawRecord {
                event_time: EventTime::from_millis(D0 - 4 * DAY + 60_000),
                capture: None,
                payload: b"late".to_vec(),
            },
        )
        .unwrap_err();
    drop(writer);
    assert!(
        error
            .to_string()
            .contains("is recorded as removed from hot"),
        "{error}"
    );

    // Re-adopting the second store completes cold-c, and a third store
    // is then adopted.
    let mut second = other_request.clone();
    second.archive_config = Some(archive.clone());
    let report = run(&config, &second, FIRST_HOT, &probe, &|| D0 + 5);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    for path in &old_b {
        assert!(cold_c.join(path).exists(), "{path}");
    }
    let third = Dir::new("adopt-new-target-third");
    seal(&third.0, "binance-um", "aggTrade", D0 - 3 * DAY, 2);
    let report = run(
        &config,
        &AdoptRequest {
            from: third.0.clone(),
            archive_config: Some(archive.clone()),
            ..AdoptRequest::default()
        },
        FIRST_HOT,
        &probe,
        &|| D0 + 6,
    );
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
}

/// The ingest configuration, an archive configuration and a probe of the
/// fixture's hot with the copy targets `ids`, created and marked under
/// `dst/` when they do not exist yet.
fn with_targets(fixture: &Fixture, ids: &[&str]) -> (IngestConfig, PathBuf, Probe) {
    let dst = fixture.dir.0.join("dst");
    let mut roots = String::from("cold_roots = [\n");
    let mut probe = vec![(fixture.hot.clone(), 1)];
    for (i, id) in ids.iter().enumerate() {
        let root = dst.join(id);
        if !root.exists() {
            fs::create_dir_all(&root).unwrap();
            root_marker::create_meta(&root, id, false).unwrap();
        }
        roots.push_str(&format!(
            "{{ path = \"{}\", copy_target = true }},\n",
            root.display()
        ));
        probe.push((root, 10 + i as u64));
    }
    roots.push_str("]\n");
    let config = IngestConfig::parse(&format!(
        "[instrument]\nsymbol = \"BTCUSDT\"\nsource = \"binance-um\"\n[paths]\n\
         raw_root = \"{}\"\njournal = \"{}\"\n{roots}[binance]\n\
         ws_base_url = \"wss://fake.invalid/ws\"\nrest_base_url = \"https://fake.invalid\"\n\
         streams = [\"aggTrade\"]\n",
        fixture.hot.display(),
        dst.join("journal.jsonl").display(),
    ))
    .unwrap();
    let archive = dst.join(format!("archive-{}.toml", ids.join("-")));
    fs::write(
        &archive,
        format!(
            "[instrument]\nsymbol = \"BTCUSDT\"\n[paths]\nraw_root = \"{}\"\n\
             import_ledger = \"{}\"\nstaging = \"{}\"\n{roots}",
            fixture.hot.display(),
            dst.join("ledger").display(),
            dst.join("staging").display(),
        ),
    )
    .unwrap();
    (config, archive, Probe(probe))
}

/// Review of #200, finding 7: after two adoptions every copy target is
/// replaced under new ids, so the removed files' copies are recorded only
/// on roots no longer configured. That gap does not block the writes
/// either, and its message names the only remedy (re-adopting each original
/// store; the repair has no good copy to copy from). Re-adopting A fills
/// A's files and fails naming B's; re-adopting B passes, and so does A.
#[test]
fn every_target_replaced_after_two_adoptions_is_filled_by_re_adoptions() {
    let fixture = Fixture::new("adopt-all-replaced");
    assert_eq!(fixture.adopt().problems, Vec::<String>::new());
    let other = Dir::new("adopt-all-replaced-other");
    seal(&other.0, "binance-um", "aggTrade", D0 - 4 * DAY, 2);
    let request_b = AdoptRequest {
        from: other.0.clone(),
        archive_config: Some(fixture.dir.0.join("dst/archive.toml")),
        ..AdoptRequest::default()
    };
    let b = run(
        &fixture.config,
        &request_b,
        FIRST_HOT,
        &fixture.probe,
        &|| D0 + 2,
    );
    assert_eq!(b.problems, Vec::<String>::new(), "{b:?}");

    let (config, archive, probe) = with_targets(&fixture, &["cold-c", "cold-d"]);
    let new = config
        .paths
        .cold_roots
        .iter()
        .map(|root| root.path.clone())
        .collect::<Vec<_>>();
    let old_of = |raw: &Path| -> Vec<String> {
        replica::list_sealed(raw)
            .unwrap()
            .into_iter()
            .filter(|p| date_of(partition_of(p)) < FIRST_HOT)
            .collect()
    };
    let (old_a, old_b) = (old_of(&fixture.raw()), old_of(&other.0));
    let mut request_a = fixture.request();
    request_a.archive_config = Some(archive.clone());
    let request_b = AdoptRequest {
        archive_config: Some(archive.clone()),
        ..request_b
    };

    let mut out = Vec::new();
    let passed =
        crate::store::adopt(&config, &request_a, FIRST_HOT, &probe, &|| D0 + 3, &mut out).unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(!passed && !text.contains("\nPASS\n"), "{text}");
    for root in &new {
        for path in &old_a {
            assert!(root.join(path).exists(), "{path}");
        }
        for path in &old_b {
            assert!(!root.join(path).exists(), "{path}");
        }
    }
    let fail: Vec<&str> = text.lines().filter(|l| l.starts_with("FAIL: ")).collect();
    assert_eq!(fail.len(), 1, "{text}");
    for path in &old_b {
        assert!(
            fail[0].contains(&format!(
                "{path} (no copy record on cold-c, cold-d, recorded on roots no longer \
                 configured: cold-a, cold-b)"
            )),
            "{path}: {text}"
        );
    }
    for text_part in [
        "no configured copy target holds a recorded copy",
        "`mie store repair` cannot help",
        "re-adopt each original store in turn",
    ] {
        assert!(fail[0].contains(text_part), "{text_part:?} in {text}");
    }
    assert!(!fail[0].contains("an adoption that stopped"), "{text}");

    let report = run(&config, &request_b, FIRST_HOT, &probe, &|| D0 + 4);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let report = run(&config, &request_a, FIRST_HOT, &probe, &|| D0 + 5);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    for root in &new {
        for path in old_a.iter().chain(&old_b) {
            assert!(root.join(path).exists(), "{path}");
        }
    }
}
