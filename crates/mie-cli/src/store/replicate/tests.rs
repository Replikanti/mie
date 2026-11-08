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
pub(crate) const D0: i64 = 1_791_244_800_000;

/// A fresh directory, removed on drop.
pub(crate) struct Dir(pub(crate) PathBuf);

impl Dir {
    pub(crate) fn new(tag: &str) -> Self {
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
pub(crate) struct Probe(pub(crate) Vec<(PathBuf, u64)>);

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
pub(crate) struct Seen(pub(crate) Vec<(PathBuf, Truncation)>);

impl PassObserver for Seen {
    fn ledger_truncated(&mut self, ledger: &Path, truncation: Truncation) {
        self.0.push((ledger.to_owned(), truncation));
    }
}

/// A tiered store: hot with live parts of `aggTrade` (three parts) and two
/// archive parts of one archive file, a journal, an archive configuration
/// and the import entry of the archive file, and two marked copy targets.
pub(crate) struct Fixture {
    pub(crate) dir: Dir,
    pub(crate) config: IngestConfig,
    pub(crate) hot: PathBuf,
    pub(crate) cold: [PathBuf; 2],
    pub(crate) probe: Probe,
}

impl Fixture {
    pub(crate) fn new(tag: &str) -> Self {
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
    pub(crate) fn seal(&self, source: &str, stream: &str, start: i64, count: i64) -> Vec<String> {
        seal(&self.hot, source, stream, start, count)
    }

    /// Writes the import entry listing `parts`.
    pub(crate) fn write_entry(&self, parts: &[String]) {
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

    pub(crate) fn archive(&self) -> PathBuf {
        self.dir.0.join("archive.toml")
    }

    /// One pass through `ledger`.
    pub(crate) fn pass_with(&self, ledger: &dyn LedgerAppend, seen: &mut Seen) -> PassReport {
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

    pub(crate) fn pass(&self) -> PassReport {
        self.pass_with(&CopyLedger::at(&self.hot), &mut Seen::default())
    }

    /// Hot's copy-ledger records.
    pub(crate) fn records(&self) -> Vec<Record> {
        copy_ledger::read(&self.hot).unwrap().unwrap().records
    }

    /// The copy target of `id`.
    pub(crate) fn target(&self, id: &str) -> &Path {
        match id {
            "cold-a" => &self.cold[0],
            "cold-b" => &self.cold[1],
            other => panic!("unknown root {other}"),
        }
    }
}

/// Seals `count` records of `stream` from `start` on `hot`, two per part,
/// and returns the data paths of the new parts.
pub(crate) fn seal(hot: &Path, source: &str, stream: &str, start: i64, count: i64) -> Vec<String> {
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
pub(crate) fn counts(
    records: &[Record],
) -> (BTreeMap<String, usize>, BTreeMap<(String, String), usize>) {
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
pub(crate) fn manifest_of(data_path: &str) -> String {
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
    let path = first_live(&fixture);
    let (before, _) = journal_naming(&fixture, &path);
    let debris = fixture.cold[0].join(&path);
    fs::create_dir_all(debris.parent().unwrap()).unwrap();
    fs::write(&debris, b"not the part").unwrap();
    let report = fixture.pass();
    assert!(!fixture.cold[0].join(manifest_of(&path)).exists());
    assert_eq!(
        fs::read(fixture.cold[0].join(".mie/journal/journal.jsonl")).unwrap(),
        before,
        "cold-a's copy stops before the line that names {path}"
    );
    assert!(
        report.problems.iter().any(|p| p.starts_with(&format!(
            "the journal copy on cold-a stops before byte {} of the journal",
            before.len()
        )) && p.contains(&path)),
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

/// The first live part of the fixture.
fn first_live(fixture: &Fixture) -> String {
    replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap()
}

/// Rewrites the fixture's journal as a line, a `sealed` line naming
/// `path`, and a line after it; returns the bytes before the `sealed` line
/// and the whole journal.
fn journal_naming(fixture: &Fixture, path: &str) -> (Vec<u8>, Vec<u8>) {
    let before = b"{\"type\":\"run_start\",\"run_id\":\"r1\"}\n".to_vec();
    let mut journal = before.clone();
    journal.extend_from_slice(
        format!(
            "{{\"type\":\"sealed\",\"run_id\":\"r1\",\"files\":[{{\"path\":\"{path}\",\"rows\":2}}]}}\n"
        )
        .as_bytes(),
    );
    journal.extend_from_slice(b"{\"type\":\"run_end\",\"run_id\":\"r1\"}\n");
    fs::write(&fixture.config.paths.journal, &journal).unwrap();
    (before, journal)
}

/// The coordinator's bound on the hold (#199): one unreadable live part on
/// hot stops every target's journal copy before the `sealed` line that
/// names it, and no earlier: every line before it reaches both targets.
#[test]
fn an_unreadable_live_part_bounds_the_journal_copies_at_its_sealed_line() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new("pass-journal-bound");
    let path = first_live(&fixture);
    let (before, _) = journal_naming(&fixture, &path);
    let bad = fixture.hot.join(&path);
    fs::set_permissions(&bad, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read(&bad).is_ok() {
        // Root reads through any permission; the case cannot be built.
        return;
    }
    let report = fixture.pass();
    fs::set_permissions(&bad, fs::Permissions::from_mode(0o444)).unwrap();
    for (root, id) in fixture.cold.iter().zip(["cold-a", "cold-b"]) {
        assert_eq!(
            fs::read(root.join(".mie/journal/journal.jsonl")).unwrap(),
            before,
            "{id}: the lines before the `sealed` line, nothing from it on"
        );
        assert!(
            report.problems.iter().any(|p| p.starts_with(&format!(
                "the journal copy on {id} stops before byte {}",
                before.len()
            ))),
            "{report:?}"
        );
    }
    assert!(
        !report.problems.iter().any(|p| p.contains("stopped")),
        "{report:?}"
    );
    // Every other file reached both targets.
    let (_, copies) = counts(&fixture.records());
    let paths = replica::list_sealed(&fixture.hot).unwrap();
    assert_eq!(copies.len(), 2 * (paths.len() - 1), "{copies:?}");

    // Readable again: the next pass completes the journal copies.
    let report = fixture.pass();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    for root in &fixture.cold {
        assert_eq!(
            fs::read(root.join(".mie/journal/journal.jsonl")).unwrap(),
            fs::read(&fixture.config.paths.journal).unwrap()
        );
    }
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

// ------------------------------------- the hot removal's side (#191)

impl Fixture {
    /// One `mie ingest` pass with the plan, the hot window starting on
    /// `first_hot_date`.
    fn planned(&self, first_hot_date: &str) -> (PassReport, Option<RemovalPlan>) {
        let archive = archive_config(&self.config, Some(&self.archive())).unwrap();
        run_planned(
            &self.config,
            archive,
            &self.probe,
            &|| D0 + 2,
            &CopyLedger::at(&self.hot),
            &mut Seen::default(),
            first_hot_date,
        )
    }

    /// The ingest configuration with a third copy target `cold-c`, its
    /// archive configuration and probe, created and marked.
    fn with_cold_c(&self) -> (IngestConfig, PathBuf, Probe) {
        let cold_c = self.dir.0.join("cold-c");
        fs::create_dir_all(&cold_c).unwrap();
        root_marker::create_meta(&cold_c, "cold-c", false).unwrap();
        let roots = format!(
            "cold_roots = [\n{{ path = \"{}\", copy_target = true }},\n\
             {{ path = \"{}\", copy_target = true }},\n\
             {{ path = \"{}\", copy_target = true }},\n]\n",
            self.cold[0].display(),
            self.cold[1].display(),
            cold_c.display()
        );
        let config = IngestConfig::parse(&format!(
            "[instrument]\nsymbol = \"BTCUSDT\"\nsource = \"binance-um\"\n[paths]\n\
             raw_root = \"{}\"\njournal = \"{}\"\n{roots}[binance]\n\
             ws_base_url = \"wss://fake.invalid/ws\"\nrest_base_url = \"https://fake.invalid\"\n\
             streams = [\"aggTrade\"]\n",
            self.hot.display(),
            self.config.paths.journal.display(),
        ))
        .unwrap();
        let archive = self.dir.0.join("archive-c.toml");
        fs::write(
            &archive,
            format!(
                "[instrument]\nsymbol = \"BTCUSDT\"\n[paths]\nraw_root = \"{}\"\n\
                 import_ledger = \"{}\"\nstaging = \"{}\"\n{roots}",
                self.hot.display(),
                self.dir.0.join("ledger").display(),
                self.dir.0.join("staging").display(),
            ),
        )
        .unwrap();
        let probe = Probe(vec![
            (self.hot.clone(), 1),
            (self.cold[0].clone(), 2),
            (self.cold[1].clone(), 3),
            (cold_c, 4),
        ]);
        (config, archive, probe)
    }
}

/// The #199 review carry-over: once the hot removal took a file off hot, a
/// target added later never gets it from hot, so its journal copy stops
/// before the `sealed` line that names it, keyed on the ledger's paths;
/// the other targets hold the whole journal.
#[test]
fn a_removed_file_holds_a_new_targets_journal_copy_before_its_sealed_line() {
    let fixture = Fixture::new("pass-removed-journal");
    let path = first_live(&fixture);
    let (before, journal) = journal_naming(&fixture, &path);
    let report = fixture.pass();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");

    // The real remover, with the hot window after the file's date.
    let (report, plan) = fixture.planned("2026-10-07");
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let plan = plan.expect("a plan");
    let removed =
        removal::remove_with_lock(&fixture.hot, "binance-um", &plan, "2026-10-07", D0 + 3);
    let partition = path.rsplit_once('/').unwrap().0;
    assert_eq!(removed.removed, [partition], "{removed:?}");
    assert!(!fixture.hot.join(&path).exists());

    // cold-c joins.
    let (config, archive, probe) = fixture.with_cold_c();
    let report = run(
        &config,
        Some(&archive),
        &probe,
        &|| D0 + 4,
        &CopyLedger::at(&fixture.hot),
        &mut Seen::default(),
    );
    let cold_c = fixture.dir.0.join("cold-c");
    assert_eq!(
        fs::read(cold_c.join(".mie/journal/journal.jsonl")).unwrap(),
        before
    );
    assert!(
        report.problems.iter().any(|p| p.starts_with(&format!(
            "the journal copy on cold-c stops before byte {} of the journal",
            before.len()
        )) && p.contains(&path)),
        "{report:?}"
    );
    for root in &fixture.cold {
        assert_eq!(
            fs::read(root.join(".mie/journal/journal.jsonl")).unwrap(),
            journal
        );
    }
}

/// The substring rule the token match replaces: the first line whose
/// bytes contain one of `paths`.
fn substring_rule(journal: &[u8], paths: &BTreeSet<&str>) -> Option<usize> {
    let mut offset = 0;
    for line in journal.split_inclusive(|&b| b == b'\n') {
        if paths
            .iter()
            .any(|p| line.windows(p.len()).any(|w| w == p.as_bytes()))
        {
            return Some(offset);
        }
        offset += line.len();
    }
    None
}

#[test]
fn first_line_naming_matches_by_layout_tokens() {
    let dir = Dir::new("token-match");
    let part = |n: u32| {
        format!(
            "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-0000{n}.parquet"
        )
    };
    let tmp = "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/\
               .part-00003.parquet.tmp";
    let lines = [
        "{\"type\":\"run_start\",\"run_id\":\"r1\"}".to_owned(),
        // A temp name and a path that only shares a prefix: no match.
        format!(
            "{{\"type\":\"recovery\",\"rolled_forward\":[],\"discarded\":[{{\"path\":\"{tmp}\",\"bytes\":9}}]}}"
        ),
        format!(
            "{{\"type\":\"sealed\",\"files\":[{{\"path\":\"{}\"}}]}}",
            part(1).replace(".parquet", ".manifest")
        ),
        format!(
            "{{\"type\":\"recall\",\"partition\":\"x\",\"damaged\":[{{\"path\":\"{}\",\"root\":\"r\"}}]}}",
            part(4)
        ),
        format!(
            "{{\"type\":\"recovery\",\"rolled_forward\":[\"{}\"]}}",
            part(2)
        ),
        format!(
            "{{\"type\":\"sealed\",\"files\":[{{\"path\":\"{}\",\"rows\":2}},{{\"path\":\"{}\"}}]}}",
            part(0),
            part(1)
        ),
    ];
    let journal: Vec<u8> = lines
        .iter()
        .flat_map(|l| format!("{l}\n").into_bytes())
        .collect();
    let path = dir.0.join("journal.jsonl");
    fs::write(&path, &journal).unwrap();
    let offset_of = |i: usize| -> u64 { lines[..i].iter().map(|l| l.len() as u64 + 1).sum() };
    let len = journal.len() as u64;

    let one = |p: &str| -> Option<(u64, String)> {
        let owned = p.to_owned();
        let set: BTreeSet<&str> = [owned.as_str()].into();
        first_line_naming(&path, 0, len, &set).unwrap()
    };
    // `recovery.rolled_forward` and `sealed` match.
    assert_eq!(one(&part(2)), Some((offset_of(4), part(2))));
    assert_eq!(one(&part(0)), Some((offset_of(5), part(0))));
    // A `sealed` line with two files; the manifest name before it does
    // not match.
    assert_eq!(one(&part(1)), Some((offset_of(5), part(1))));
    // The temp file's data path is never named by its temp name.
    assert_eq!(one(&part(3)), None);
    // From a later offset only later lines count.
    let second = part(2);
    let set: BTreeSet<&str> = [second.as_str()].into();
    assert_eq!(
        first_line_naming(&path, offset_of(5), len, &set).unwrap(),
        None
    );

    // Against the substring rule, over every subset of the paths.
    let all: Vec<String> = (0..5).map(part).collect();
    for mask in 0..(1u32 << all.len()) {
        let set: BTreeSet<&str> = all
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, p)| p.as_str())
            .collect();
        let found = first_line_naming(&path, 0, len, &set)
            .unwrap()
            .map(|(offset, _)| offset as usize);
        assert_eq!(found, substring_rule(&journal, &set), "{set:?}");
    }
}

#[test]
fn a_planned_pass_plans_only_complete_partitions() {
    let partition = |fixture: &Fixture| first_live(fixture).rsplit_once('/').unwrap().0.to_owned();

    // A target skipped (no marker): no plan.
    let fixture = Fixture::new("plan-skipped");
    fs::remove_file(root_marker::marker_path(&fixture.cold[1])).unwrap();
    let (report, plan) = fixture.planned("2026-10-07");
    assert!(!report.targets_complete, "{report:?}");
    assert!(plan.is_none());

    // A target stopped mid-pass: no plan.
    let fixture = Fixture::new("plan-stopped");
    let cold_b = fixture.cold[1].clone();
    ON_STEP.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |step| {
            if step == PassStep::AfterPublish {
                let _ = fs::remove_file(root_marker::marker_path(&cold_b));
            }
        }));
    });
    let (report, plan) = fixture.planned("2026-10-07");
    ON_STEP.with(|hook| hook.borrow_mut().take());
    assert!(!report.targets_complete, "{report:?}");
    assert!(plan.is_none());

    // Complete: the pass's own ledger copies cover its copy records.
    let fixture = Fixture::new("plan-complete");
    let (report, plan) = fixture.planned("2026-10-07");
    assert!(report.targets_complete, "{report:?}");
    assert_eq!(
        report.target_ids,
        ["cold-a", "cold-b"].map(str::to_owned).into()
    );
    let plan = plan.expect("a plan");
    let names: Vec<&str> = plan
        .candidates
        .iter()
        .map(|c| c.partition.as_str())
        .collect();
    assert!(names.contains(&partition(&fixture).as_str()), "{plan:?}");

    // A new part whose copy records lie beyond both ledger copies (the
    // pass stopped before its `ledger-copy` records) is not planned; the
    // next pass covers it, and its partition is planned again.
    seal(&fixture.hot, "binance-um", "aggTrade", D0 + 60_000, 2);
    CRASH_AT.with(|crash| crash.set(Some(PassStep::AfterLedgerExtend)));
    let (_, plan) = fixture.planned("2026-10-07");
    CRASH_AT.with(|crash| crash.set(None));
    assert!(plan.is_none(), "a stopped pass gives no plan");
    let ledger = copy_ledger::read(&fixture.hot).unwrap().unwrap();
    let scan = removal::scan_hot(&fixture.hot, "2026-10-07").unwrap();
    let ids: BTreeSet<String> = ["cold-a", "cold-b"].map(str::to_owned).into();
    let direct = removal::plan(&ledger, &scan, &ids, 2, "2026-10-07").unwrap();
    assert!(
        direct
            .candidates
            .iter()
            .all(|c| c.partition != partition(&fixture)),
        "{direct:?}"
    );
    let (_, plan) = fixture.planned("2026-10-07");
    let plan = plan.expect("a plan");
    assert!(
        plan.candidates
            .iter()
            .any(|c| c.partition == partition(&fixture)),
        "{plan:?}"
    );

    // `run` never plans and never removes.
    let report = fixture.pass();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert!(fixture.hot.join(partition(&fixture)).exists());
}

/// Review of #202, finding 1: a target whose copy-ledger copy is refused
/// ("needs the repair") holds no verified ledger copy, so its older
/// `ledger-copy` record must not count for rule (f). The pass is not
/// complete, gives no plan, and nothing leaves hot.
#[test]
fn a_refused_ledger_copy_gives_no_plan_and_removes_nothing() {
    let fixture = Fixture::new("plan-refused-ledger-copy");
    let partition = first_live(&fixture).rsplit_once('/').unwrap().0.to_owned();
    let report = fixture.pass();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    // cold-b's ledger copy is damaged in its last record.
    let copy = copy_ledger::ledger_path(&fixture.cold[1]);
    make_writable(&copy);
    let mut bytes = fs::read(&copy).unwrap();
    let at = bytes.len() - 5;
    bytes[at] ^= 0x01;
    fs::write(&copy, &bytes).unwrap();

    let (report, plan) = fixture.planned("2026-10-07");
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains("the copy-ledger copy on cold-b") && p.contains("refused")),
        "{report:?}"
    );
    assert!(!report.targets_complete, "{report:?}");
    assert!(plan.is_none(), "{plan:?}");
    assert!(fixture.hot.join(&partition).exists());
}

/// Review of #202, finding 2: hot holding another version of a recorded
/// path (a D5 breach that `select` reports) is never removed silently: the
/// plan leaves the partition on hot and the pass names the conflict.
#[test]
fn a_hot_manifest_differing_from_the_ledger_is_never_removed() {
    let fixture = Fixture::new("plan-d5-conflict");
    let report = fixture.pass();
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let other = Dir::new("plan-d5-conflict-other");
    let path = first_live(&fixture);
    let alt = seal(&other.0, "binance-um", "aggTrade", D0 + 500, 6);
    assert!(alt.contains(&path));
    for file in [path.clone(), manifest_of(&path)] {
        let dst = fixture.hot.join(&file);
        make_writable(&dst);
        fs::remove_file(&dst).unwrap();
        fs::copy(other.0.join(&file), &dst).unwrap();
    }
    let partition = path.rsplit_once('/').unwrap().0.to_owned();
    let (report, plan) = fixture.planned("2026-10-07");
    let plan = plan.expect("a plan");
    assert!(
        plan.candidates.iter().all(|c| c.partition != partition),
        "{plan:?}"
    );
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains(&path) && p.contains("ADR-062 D5")),
        "{report:?}"
    );
    let removed =
        removal::remove_with_lock(&fixture.hot, "binance-um", &plan, "2026-10-07", D0 + 3);
    assert!(removed.removed.is_empty(), "{removed:?}");
    assert!(fixture.hot.join(&path).exists());
}

/// L2 of #192: an open copy-ledger finding holds the target's ledger copy
/// (no extension, no `ledger-copy` record, the pass not complete for the
/// hot removal) while the other target goes on; the closing pass of a
/// repair, which names the target as cleared, extends it and appends the
/// record that ends the finding's void.
#[test]
fn an_open_copy_ledger_finding_holds_the_ledger_copy_until_cleared() {
    let fixture = Fixture::new("pass-l2");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + 2,
            kind: Kind::Finding {
                root: "cold-a".to_owned(),
                code: copy_ledger::FindingCode::Chain,
                subject: copy_ledger::Subject::CopyLedger,
            },
        })
        .unwrap();
    let copy_a = copy_ledger::ledger_path(&fixture.cold[0]);
    let before = fs::read(&copy_a).unwrap();
    let ledger_copies = |fixture: &Fixture, id: &str| {
        fixture
            .records()
            .iter()
            .filter(|r| matches!(&r.kind, Kind::LedgerCopy { root, .. } if root == id))
            .count()
    };
    let (a_before, b_before) = (
        ledger_copies(&fixture, "cold-a"),
        ledger_copies(&fixture, "cold-b"),
    );
    let report = fixture.pass();
    assert!(!report.targets_complete, "{report:?}");
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains("cold-a") && p.contains("open copy-ledger finding")),
        "{report:?}"
    );
    assert_eq!(fs::read(&copy_a).unwrap(), before);
    assert_eq!(ledger_copies(&fixture, "cold-a"), a_before);
    assert_eq!(ledger_copies(&fixture, "cold-b"), b_before + 1);

    let lock = replica::try_replication_lock(&fixture.hot).unwrap();
    let archive = fixture.archive();
    let report = run_locked(
        &fixture.config,
        Some(&archive),
        &fixture.probe,
        &|| D0 + 3,
        &CopyLedger::at(&fixture.hot),
        &mut Seen::default(),
        lock,
        &["cold-a".to_owned()].into(),
    );
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert!(report.targets_complete);
    assert_eq!(ledger_copies(&fixture, "cold-a"), a_before + 1);
    // The finding is closed: the next ordinary pass passes.
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
}
