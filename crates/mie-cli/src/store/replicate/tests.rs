//! The pass under its hooks: a crash at every pass step, the seal race and
//! a failed append that truncated.

use super::*;
use crate::config::IngestConfig;
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_domain::time::EventTime;
use mie_ports::raw::{RawRecord, RawRecordSink, RawStreamKey};
use std::cell::RefCell;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

/// 2026-10-06T00:00:00Z.
const D0: i64 = 1_791_244_800_000;

/// A fresh directory, removed on drop.
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mie-cli-replicate-unit-{tag}-{}-{n}",
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

/// Records every truncation it is handed.
#[derive(Default)]
struct Seen(Vec<(PathBuf, Truncation)>);

impl PassObserver for Seen {
    fn ledger_truncated(&mut self, ledger: &Path, truncation: Truncation) {
        self.0.push((ledger.to_owned(), truncation));
    }
}

/// A tiered store: hot with live parts of `aggTrade` (three parts) and two
/// archive parts of one archive file, a journal, an archive configuration
/// and the import entry of the archive file, and two marked copy targets.
struct Fixture {
    dir: Dir,
    config: IngestConfig,
    hot: PathBuf,
    cold: [PathBuf; 2],
    probe: Probe,
}

impl Fixture {
    fn new(tag: &str) -> Self {
        let dir = Dir::new(tag);
        let hot = dir.0.join("raw");
        let cold = [dir.0.join("cold-a"), dir.0.join("cold-b")];
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
            dir.0.join("journal.jsonl").display(),
        ))
        .unwrap();
        fs::write(
            dir.0.join("archive.toml"),
            format!(
                "[instrument]\nsymbol = \"BTCUSDT\"\n[paths]\nraw_root = \"{}\"\n\
                 import_ledger = \"{}\"\nstaging = \"{}\"\n{roots}",
                hot.display(),
                dir.0.join("ledger").display(),
                dir.0.join("staging").display(),
            ),
        )
        .unwrap();
        let fixture = Self {
            probe: Probe(vec![
                (hot.clone(), 1),
                (cold[0].clone(), 2),
                (cold[1].clone(), 3),
            ]),
            dir,
            config,
            hot,
            cold,
        };
        fs::create_dir_all(&fixture.hot).unwrap();
        fixture.seal("binance-um", "aggTrade", D0, 6);
        let parts = fixture.seal("binance-archive", "aggTrades", D0 - 86_400_000, 4);
        root_marker::create_meta(&fixture.hot, HOT_ID, true).unwrap();
        for (root, id) in fixture.cold.iter().zip(["cold-a", "cold-b"]) {
            fs::create_dir_all(root).unwrap();
            root_marker::create_meta(root, id, false).unwrap();
        }
        fixture.write_entry(&parts);
        fs::write(
            &fixture.config.paths.journal,
            "{\"type\":\"run_start\"}\n{\"type\":\"sealed\"}\n",
        )
        .unwrap();
        fixture
    }

    /// Seals `count` records of `stream` from `start` on hot.
    fn seal(&self, source: &str, stream: &str, start: i64, count: i64) -> Vec<String> {
        seal(&self.hot, source, stream, start, count)
    }

    /// Writes the import entry listing `parts`.
    fn write_entry(&self, parts: &[String]) {
        let mut text = format!(
            "mie-archive-import 1\n\
             archive data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-05.zip\n\
             sha256 {}\nstream binance-archive/BTCUSDT/aggTrades\nperiod 2026-10-05\n\
             header -\nrows 4\n",
            "ab".repeat(32)
        );
        for part in parts {
            let (_, file) = replica::sealed_manifest(&self.hot, part).unwrap().unwrap();
            text.push_str(&format!("file {part} {} {}\n", file.rows, file.sha256));
        }
        let dir = self.dir.0.join("ledger").join("aggTrades");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("BTCUSDT-aggTrades-2026-10-05.zip.import"), text).unwrap();
    }

    fn archive(&self) -> PathBuf {
        self.dir.0.join("archive.toml")
    }

    /// One pass through `ledger`.
    fn pass_with(&self, ledger: &dyn LedgerAppend, seen: &mut Seen) -> PassReport {
        let archive = self.archive();
        run(
            &self.config,
            Some(&archive),
            &self.probe,
            &|| D0 + 1,
            ledger,
            seen,
        )
    }

    fn pass(&self) -> PassReport {
        self.pass_with(&CopyLedger::at(&self.hot), &mut Seen::default())
    }

    /// Hot's copy-ledger records.
    fn records(&self) -> Vec<Record> {
        copy_ledger::read(&self.hot).unwrap().unwrap().records
    }

    /// The copy target of `id`.
    fn target(&self, id: &str) -> &Path {
        match id {
            "cold-a" => &self.cold[0],
            "cold-b" => &self.cold[1],
            other => panic!("unknown root {other}"),
        }
    }
}

/// Seals `count` records of `stream` from `start` on `hot`, two per part,
/// and returns the data paths of the new parts.
fn seal(hot: &Path, source: &str, stream: &str, start: i64, count: i64) -> Vec<String> {
    let before = replica::list_sealed(hot).unwrap();
    let key = RawStreamKey::new(source, "BTCUSDT", stream).unwrap();
    let policy = RotationPolicy {
        max_rows: 2,
        ..RotationPolicy::default()
    };
    let mut writer = ParquetRawStore::tiered(hot, Vec::new())
        .writer(source, policy)
        .unwrap();
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
    replica::list_sealed(hot)
        .unwrap()
        .into_iter()
        .filter(|path| !before.contains(path))
        .collect()
}

/// The `manifest` records per data path and the `copy` records per path
/// and root.
fn counts(records: &[Record]) -> (BTreeMap<String, usize>, BTreeMap<(String, String), usize>) {
    let (mut manifests, mut copies) = (BTreeMap::new(), BTreeMap::new());
    for record in records {
        match &record.kind {
            Kind::Manifest { text } => {
                let path = replica::parse_manifest(text).unwrap().relative_path;
                *manifests.entry(path).or_default() += 1;
            }
            Kind::Copy { path, root } => {
                *copies.entry((path.clone(), root.clone())).or_default() += 1;
            }
            _ => {}
        }
    }
    (manifests, copies)
}

/// The manifest path of a data path.
fn manifest_of(data_path: &str) -> String {
    format!("{}.manifest", data_path.strip_suffix(".parquet").unwrap())
}

#[test]
fn a_pass_copies_everything_and_a_second_pass_nothing() {
    let fixture = Fixture::new("pass");
    let report = fixture.pass();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let paths = replica::list_sealed(&fixture.hot).unwrap();
    assert_eq!(paths.len(), 5);
    let (manifests, copies) = counts(&fixture.records());
    for path in &paths {
        assert_eq!(manifests.get(path), Some(&1), "{path}");
        for (root, id) in fixture.cold.iter().zip(["cold-a", "cold-b"]) {
            assert_eq!(copies.get(&(path.clone(), id.to_owned())), Some(&1));
            for file in [path.clone(), manifest_of(path)] {
                assert_eq!(
                    fs::read(fixture.hot.join(&file)).unwrap(),
                    fs::read(root.join(&file)).unwrap(),
                    "{file}"
                );
            }
        }
    }
    let hot_ledger = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    let report = fixture.pass();
    assert_eq!(report.problems, Vec::<String>::new());
    assert_eq!(
        report.lines,
        [
            "copied 0 file(s) and 0 import entries; appended 0 copy-ledger byte(s) and 0 journal \
          byte(s)"
        ]
    );
    assert_eq!(
        fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap(),
        hot_ledger
    );
}

/// The acceptance item of #189: a crash at every pass step leaves no
/// `copy` record naming a root that lacks the published manifest, and the
/// next pass ends with exactly one `manifest` record per file and one
/// `copy` record per file and target.
#[test]
fn a_crash_at_every_pass_step_is_completed_by_the_next_pass() {
    for step in PassStep::ALL {
        let fixture = Fixture::new("pass-crash");
        CRASH_AT.with(|crash| crash.set(Some(step)));
        let crashed = fixture.pass();
        assert_eq!(
            CRASH_AT.with(std::cell::Cell::get),
            None,
            "{step:?} was reached"
        );
        let lines = crashed
            .lines
            .iter()
            .filter(|l| l.starts_with("copied 0"))
            .count();
        assert_eq!(lines, 0, "{step:?}: the crashed pass did not finish");
        for record in fixture.records() {
            if let Kind::Copy { path, root } = record.kind {
                assert!(
                    fixture.target(&root).join(manifest_of(&path)).exists(),
                    "{step:?}: a copy record for {path} on {root} without its manifest"
                );
            }
        }

        let report = fixture.pass();
        assert_eq!(report.problems, Vec::<String>::new(), "{step:?}");
        let paths = replica::list_sealed(&fixture.hot).unwrap();
        let (manifests, copies) = counts(&fixture.records());
        assert_eq!(manifests.len(), paths.len(), "{step:?}");
        assert!(
            manifests.values().all(|&n| n == 1),
            "{step:?}: {manifests:?}"
        );
        assert_eq!(copies.len(), 2 * paths.len(), "{step:?}");
        assert!(copies.values().all(|&n| n == 1), "{step:?}: {copies:?}");
        // And the pass after that is idle.
        let idle = fixture.pass();
        assert_eq!(idle.lines.len(), 1, "{step:?}: {idle:?}");
    }
}

/// The acceptance item of #189: a seal and its `sealed` journal line that
/// land between the listing and the journal step stay off the target until
/// the next pass.
#[test]
fn a_seal_during_the_pass_reaches_the_targets_with_the_next_pass() {
    let fixture = Fixture::new("pass-seal-race");
    let journal = fixture.config.paths.journal.clone();
    let before = fs::read(&journal).unwrap();
    let new_parts = std::rc::Rc::new(RefCell::new(Vec::new()));
    {
        let (hot, journal, new_parts) = (fixture.hot.clone(), journal.clone(), new_parts.clone());
        AFTER_LISTING.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                *new_parts.borrow_mut() = seal(&hot, "binance-um", "aggTrade", D0 + 3_600_000, 2);
                let mut text = fs::read(&journal).unwrap();
                text.extend_from_slice(b"{\"type\":\"sealed\",\"late\":true}\n");
                fs::write(&journal, text).unwrap();
            }));
        });
    }
    let report = fixture.pass();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let late = new_parts.borrow().clone();
    assert_eq!(late.len(), 1);
    for root in &fixture.cold {
        assert!(!root.join(&late[0]).exists());
        assert_eq!(
            fs::read(root.join(".mie/journal/journal.jsonl")).unwrap(),
            before,
            "the late `sealed` line stays off the copy"
        );
    }
    let report = fixture.pass();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    for root in &fixture.cold {
        assert!(root.join(&late[0]).exists());
        assert_eq!(
            fs::read(root.join(".mie/journal/journal.jsonl")).unwrap(),
            fs::read(&journal).unwrap()
        );
    }
}

/// An appender whose first append fails after truncating.
struct Truncating<'a> {
    inner: &'a CopyLedger,
    failed: std::cell::Cell<bool>,
}

impl LedgerAppend for Truncating<'_> {
    fn path(&self) -> &Path {
        self.inner.path()
    }

    fn append(&self, record: &Record) -> Result<AppendOutcome, AppendError> {
        if self.failed.replace(true) {
            return self.inner.append(record);
        }
        Err(AppendError {
            error: RawStoreError::Io("injected: no space left on device".to_owned()),
            truncation: Some(Truncation {
                bytes: 17,
                offset: 4_242,
            }),
        })
    }
}

/// The acceptance item of #189 (review of #197, finding 5): a failed
/// append that truncated hands the truncation to the observer, and the pass
/// returns the failure.
#[test]
fn a_failed_append_that_truncated_reaches_the_observer() {
    let fixture = Fixture::new("pass-truncation");
    let ledger = CopyLedger::at(&fixture.hot);
    let appender = Truncating {
        inner: &ledger,
        failed: std::cell::Cell::new(false),
    };
    let mut seen = Seen::default();
    let report = fixture.pass_with(&appender, &mut seen);
    assert_eq!(
        seen.0,
        [(
            ledger.path().to_owned(),
            Truncation {
                bytes: 17,
                offset: 4_242
            }
        )]
    );
    assert_eq!(report.problems.len(), 1, "{report:?}");
    assert!(
        report.problems[0].contains("the manifest append")
            && report.problems[0].contains("the pass stops")
            && report.problems[0].contains("no space left"),
        "{report:?}"
    );
    assert_eq!(
        truncation_line(ledger.path(), seen.0[0].1),
        format!(
            "copy ledger {}: truncated an incomplete trailing record of 17 bytes at byte 4242 \
             before an append (ADR-062 D7)",
            ledger.path().display()
        )
    );
}

/// Every file below `root` with its bytes, sorted by relative path.
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
                out.push((rel, fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// Makes `path` writable again, so a test can replace it.
fn make_writable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
}

/// Review of #199, finding 1: when hot's manifest differs from the ledger's
/// `manifest` record, the pass copies nothing, so two targets never hold
/// two versions of one path.
#[test]
fn a_hot_manifest_that_differs_from_the_ledger_is_never_copied() {
    let fixture = Fixture::new("pass-d5-guard");
    // Pass 1 with cold-b unmarked: only cold-a gets copies and the ledger
    // gets the manifest records.
    let marker_b = root_marker::marker_path(&fixture.cold[1]);
    let saved = fs::read(&marker_b).unwrap();
    fs::remove_file(&marker_b).unwrap();
    fixture.pass();
    fs::write(&marker_b, &saved).unwrap();
    // Another valid seal of the same part number replaces hot's.
    let other = Dir::new("pass-d5-guard-other");
    let path = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    let alt = seal(&other.0, "binance-um", "aggTrade", D0 + 500, 6);
    assert!(alt.contains(&path), "{alt:?} vs {path}");
    for file in [path.clone(), manifest_of(&path)] {
        let dst = fixture.hot.join(&file);
        make_writable(&dst);
        fs::remove_file(&dst).unwrap();
        fs::copy(other.0.join(&file), &dst).unwrap();
    }
    let cold_a = files_under(&fixture.cold[0]);
    let report = fixture.pass();
    assert!(
        !fixture.cold[1].join(&path).exists() && !fixture.cold[1].join(manifest_of(&path)).exists(),
        "the divergent version reached cold-b"
    );
    assert!(
        report.problems.iter().any(|p| p.starts_with(&format!("{path}: 1 of 2 copies"))
            && p.contains("hot's manifest differs from the copy ledger's manifest record; nothing is copied")),
        "{report:?}"
    );
    // cold-a's copy of the path is the recorded version, untouched.
    let after: Vec<_> = files_under(&fixture.cold[0])
        .into_iter()
        .filter(|(name, _)| name.starts_with("source="))
        .collect();
    let before: Vec<_> = cold_a
        .into_iter()
        .filter(|(name, _)| name.starts_with("source="))
        .collect();
    assert_eq!(after, before);
}

/// Review of #199, finding 2: one unreadable hot file is named, and every
/// other file and every target goes on.
#[test]
fn an_unreadable_hot_file_stops_only_that_file() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new("pass-hot-eacces");
    let paths = replica::list_sealed(&fixture.hot).unwrap();
    let bad = fixture.hot.join(&paths[0]);
    fs::set_permissions(&bad, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read(&bad).is_ok() {
        // Root reads through any permission; the case cannot be built.
        return;
    }
    let report = fixture.pass();
    fs::set_permissions(&bad, fs::Permissions::from_mode(0o444)).unwrap();
    assert!(
        !report.problems.iter().any(|p| p.contains("stopped")),
        "{report:?}"
    );
    assert!(
        report.problems.iter().any(|p| p.starts_with(&paths[0])
            && p.contains("reading it on hot failed")
            && p.contains("Permission denied")),
        "{report:?}"
    );
    let (_, copies) = counts(&fixture.records());
    assert_eq!(copies.len(), 2 * (paths.len() - 1), "{copies:?}");
    for root in &fixture.cold {
        assert!(copy_ledger::ledger_path(root).exists());
        // The unreadable file is an archive part, which never holds the
        // journal.
        assert!(root.join(".mie/journal/journal.jsonl").exists());
    }
}

/// Review of #199, finding 3 (operator decision): the journal tail is held
/// on a target while a listed live file is not on it, so a `sealed` line on
/// a copy always names a file that copy holds.
#[test]
fn the_journal_tail_waits_for_every_live_file_on_its_target() {
    let fixture = Fixture::new("pass-journal-hold");
    let path = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    let debris = fixture.cold[0].join(&path);
    fs::create_dir_all(debris.parent().unwrap()).unwrap();
    fs::write(&debris, b"not the part").unwrap();
    let report = fixture.pass();
    assert!(!fixture.cold[0].join(manifest_of(&path)).exists());
    assert!(!fixture.cold[0].join(".mie/journal/journal.jsonl").exists());
    assert!(
        report.problems.iter().any(|p| p.starts_with(
            "the journal copy on cold-a is held: 1 listed live file(s) are not on it"
        ) && p.contains(&path)),
        "{report:?}"
    );
    assert_eq!(
        fs::read(fixture.cold[1].join(".mie/journal/journal.jsonl")).unwrap(),
        fs::read(&fixture.config.paths.journal).unwrap()
    );

    // Once the file is on cold-a, the journal follows.
    fs::remove_file(&debris).unwrap();
    let report = fixture.pass();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert_eq!(
        fs::read(fixture.cold[0].join(".mie/journal/journal.jsonl")).unwrap(),
        fs::read(&fixture.config.paths.journal).unwrap()
    );
}

/// Review of #199, finding 4 (operator decision): a target that loses its
/// mount after step 0 gets nothing written into the bare mount point.
#[test]
fn nothing_is_written_into_a_target_that_lost_its_mount() {
    let fixture = Fixture::new("pass-unmounted");
    let a = fixture.cold[0].clone();
    let hidden = fixture.dir.0.join("cold-a.mounted");
    AFTER_LISTING.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            fs::rename(&a, &hidden).unwrap();
            fs::create_dir(&a).unwrap();
        }))
    });
    let report = fixture.pass();
    assert_eq!(files_under(&fixture.cold[0]), Vec::new(), "{report:?}");
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains("(cold-a): stopped") && p.contains("its marker is gone")),
        "{report:?}"
    );
    let (_, copies) = counts(&fixture.records());
    assert!(
        copies.keys().all(|(_, root)| root == "cold-b"),
        "{copies:?}"
    );
    assert_eq!(
        copies.len(),
        replica::list_sealed(&fixture.hot).unwrap().len()
    );
}

/// QA of #199: a target whose marker disappears or changes mid-pass (right
/// after a copy was published on it) gets no record and no file after that
/// point, is reported stopped, and the command exits 1.
#[test]
fn a_marker_lost_or_changed_mid_pass_stops_its_target() {
    for change in ["removed", "changed"] {
        let fixture = Fixture::new("pass-marker-mid");
        let cold_a = fixture.cold[0].clone();
        let at_change = std::rc::Rc::new(RefCell::new(None));
        {
            let at_change = at_change.clone();
            ON_STEP.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |step| {
                    if step != PassStep::AfterPublish || at_change.borrow().is_some() {
                        return;
                    }
                    let marker = root_marker::marker_path(&cold_a);
                    if change == "removed" {
                        fs::remove_file(&marker).unwrap();
                    } else {
                        fs::write(&marker, root_marker::render("cold-x").unwrap()).unwrap();
                    }
                    *at_change.borrow_mut() = Some(files_under(&cold_a));
                }));
            });
        }
        let archive = fixture.archive();
        let mut out = Vec::new();
        let pass = crate::store::replicate(
            &fixture.config,
            Some(&archive),
            &fixture.probe,
            &|| D0 + 1,
            &mut out,
        )
        .unwrap();
        ON_STEP.with(|hook| hook.borrow_mut().take());
        let out = String::from_utf8(out).unwrap();
        assert!(!pass, "{change}: exit 1: {out}");
        let snapshot = at_change.borrow().clone().expect("the marker was changed");
        assert_eq!(files_under(&fixture.cold[0]), snapshot, "{change}: {out}");
        for record in fixture.records() {
            match record.kind {
                Kind::Copy { root, path } => assert_ne!(root, "cold-a", "{change}: {path}"),
                Kind::LedgerCopy { root, .. } => assert_ne!(root, "cold-a", "{change}"),
                _ => {}
            }
        }
        assert!(
            out.contains(&format!(
                "FAIL: copy target {} (cold-a): stopped for the rest of this pass",
                fixture.cold[0].display()
            )),
            "{change}: {out}"
        );
        // cold-b completes.
        let (_, copies) = counts(&fixture.records());
        assert_eq!(
            copies.len(),
            replica::list_sealed(&fixture.hot).unwrap().len(),
            "{change}"
        );
    }
}
