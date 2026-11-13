//! The restore end to end on the replication fixture (#195): every
//! refusal and stop, the journal, the import ledger, the install, the
//! copy-back, the commit and its crash points. The names follow the plan's
//! edge cases.

use super::*;
use crate::store::replicate::tests::{D0, Fixture};
use crate::store::replicate::{CRASH_AT, PassStep};
use mie_adapter_parquet::copy_ledger::LedgerCopyCheck;
use mie_adapter_parquet::{ParquetRawStore, rebuilt};
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawRecordSource, RawSelection, RawStreamKey};
use std::cell::RefCell;
use std::os::unix::fs::PermissionsExt;
use std::rc::Rc;

/// The run's `at_ms`.
const AT: i64 = D0 + 3 * 3_600_000;
/// The fixture's hot window: its archive parts (2026-10-05) and live parts
/// (2026-10-06) are inside it.
const FIRST_HOT: &str = "2026-10-05";
const LIVE: &str = "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06";
const ARCHIVE: &str = "source=binance-archive/instrument=BTCUSDT/stream=aggTrades/date=2026-10-05";
const ENTRY: &str = "aggTrades/BTCUSDT-aggTrades-2026-10-05.zip.import";

fn restore_at(fixture: &Fixture, flags: Flags, first_hot: &str, at: i64) -> RestoreReport {
    run(
        &fixture.config,
        Some(&fixture.archive()),
        flags,
        first_hot,
        &fixture.probe,
        &|| at,
    )
}

fn restore(fixture: &Fixture) -> RestoreReport {
    restore_at(fixture, Flags::default(), FIRST_HOT, AT)
}

/// A fixture after one passing replication pass.
fn replicated(tag: &str) -> Fixture {
    let fixture = Fixture::new(tag);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    fixture
}

/// The hot disk lost: `raw_root` emptied (the directory stays: the
/// replacement is mounted), the journal and the import-ledger directory
/// gone, as in the default layout where all three share the hot disk.
fn lose_hot(fixture: &Fixture) {
    make_writable(&fixture.hot);
    fs::remove_dir_all(&fixture.hot).unwrap();
    fs::create_dir(&fixture.hot).unwrap();
    let _ = fs::remove_file(&fixture.config.paths.journal);
    let ledger = fixture.dir.0.join("ledger");
    make_writable(&ledger);
    let _ = fs::remove_dir_all(ledger);
}

/// Makes every file below `dir` writable, so it can be removed or damaged.
fn make_writable(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() && !path.is_symlink() {
            make_writable(&path);
        } else if !path.is_symlink() {
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        }
    }
}

/// Every file below the fixture with its bytes; lock files left out (the
/// locks create them).
fn snapshot(fixture: &Fixture) -> Vec<(PathBuf, Vec<u8>)> {
    fn walk(dir: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, out);
            } else if !path.to_string_lossy().ends_with(".lock") {
                let bytes = fs::read(&path).unwrap_or_default();
                out.push((path, bytes));
            }
        }
    }
    let mut out = Vec::new();
    walk(&fixture.dir.0, &mut out);
    out.sort();
    out
}

#[track_caller]
fn assert_restored(report: &RestoreReport) {
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    assert!(report.restored, "{report:#?}");
    assert_eq!(
        report.lines.last().map(String::as_str),
        Some("restored: hot marker written; mie ingest may start"),
        "{report:#?}"
    );
}

#[track_caller]
fn assert_problem(report: &RestoreReport, needles: &[&str]) {
    assert!(
        report
            .problems
            .iter()
            .any(|p| needles.iter().all(|n| p.contains(n))),
        "no problem with {needles:?}: {report:#?}"
    );
}

#[track_caller]
fn assert_not_restored(report: &RestoreReport, needles: &[&str]) {
    assert_problem(report, needles);
    assert!(!report.restored, "{report:#?}");
    assert_eq!(
        report.lines.last().map(String::as_str),
        Some("not restored: no hot marker written"),
        "{report:#?}"
    );
}

/// The hot files of the fixture below `raw_root`, the journal and the
/// import ledger, relative; lock files and `.mie/damaged/` left out.
fn hot_state(fixture: &Fixture) -> Vec<(String, Vec<u8>)> {
    snapshot(fixture)
        .into_iter()
        .filter_map(|(path, bytes)| {
            let rel = path
                .strip_prefix(&fixture.dir.0)
                .unwrap()
                .display()
                .to_string();
            let hot =
                rel.starts_with("raw/") || rel.starts_with("ledger/") || rel == "journal.jsonl";
            (hot && !rel.contains("/.mie/damaged/") && !rel.ends_with("copy-ledger"))
                .then_some((rel, bytes))
        })
        .collect()
}

/// Hot's ledger records with `at_ms` set aside.
fn records_without_time(fixture: &Fixture) -> Vec<Kind> {
    fixture.records().into_iter().map(|r| r.kind).collect()
}

/// Appends `kind` to hot's ledger.
fn append(fixture: &Fixture, kind: Kind) {
    CopyLedger::at(&fixture.hot)
        .append(&Record { at_ms: D0, kind })
        .unwrap();
}

/// A selection of every stream of the fixture over `[start, end)`.
fn window(start: i64, end: i64) -> RawSelection {
    let streams = [
        RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap(),
        RawStreamKey::new(ARCHIVE_SOURCE, "BTCUSDT", "aggTrades").unwrap(),
    ]
    .into_iter()
    .collect();
    RawSelection::new(
        streams,
        ReplayWindow {
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(end),
        },
    )
    .unwrap()
}

fn tiered(fixture: &Fixture) -> ParquetRawStore {
    ParquetRawStore::tiered(&fixture.hot, fixture.cold.to_vec())
}

/// Every selection compared, over the store of `hot` and `cold`: the two
/// days and both together.
fn selected(hot: &Path, cold: &[PathBuf]) -> Vec<String> {
    let day = 86_400_000;
    let store = ParquetRawStore::tiered(hot, cold.to_vec());
    [
        window(D0 - day, D0),
        window(D0, D0 + day),
        window(D0 - day, D0 + day),
    ]
    .iter()
    .map(|w| match store.select(w) {
        Ok(dataset) => format!("{dataset:?}"),
        Err(error) => format!("error: {error}"),
    })
    .collect()
}

/// Writes the fixture's archive configuration again from its ingest
/// configuration's `cold_roots`.
fn rewrite_archive(fixture: &Fixture) {
    let roots: Vec<String> = fixture
        .config
        .paths
        .cold_roots
        .iter()
        .map(|root| {
            format!(
                "{{ path = \"{}\", copy_target = {} }},\n",
                root.path.display(),
                root.copy_target
            )
        })
        .collect();
    fs::write(
        fixture.archive(),
        format!(
            "[instrument]\nsymbol = \"BTCUSDT\"\n[paths]\nraw_root = \"{}\"\n\
             import_ledger = \"{}\"\nstaging = \"{}\"\ncold_roots = [\n{}]\n",
            fixture.config.paths.raw_root.display(),
            fixture.dir.0.join("ledger").display(),
            fixture.dir.0.join("staging").display(),
            roots.concat()
        ),
    )
    .unwrap();
}

/// Adds the read-only root `cold-r` first in read order, holding a copy of
/// cold-a's layout and its own marker (device 4); returns its path.
fn with_read_only_root(fixture: &mut Fixture) -> PathBuf {
    fn copy_tree(from: &Path, to: &Path) {
        fs::create_dir_all(to).unwrap();
        for entry in fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            if name == ".mie" {
                continue;
            }
            let path = entry.path();
            if path.is_dir() {
                copy_tree(&path, &to.join(&name));
            } else {
                fs::copy(&path, to.join(&name)).unwrap();
            }
        }
    }
    let root = fixture.dir.0.join("cold-r");
    copy_tree(&fixture.cold[0], &root);
    root_marker::create_meta(&root, "cold-r", false).unwrap();
    fixture.config.paths.cold_roots.insert(
        0,
        crate::config::ColdRoot {
            path: root.clone(),
            copy_target: false,
        },
    );
    fixture.probe.0.push((root.clone(), 4));
    rewrite_archive(fixture);
    root
}

/// The data path of the live part `n`.
fn live(n: u32) -> String {
    format!("{LIVE}/part-{n:05}.parquet")
}

/// Flips the first byte of the data of `path` on `root`.
fn damage(root: &Path, path: &str) {
    let file = root.join(path);
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
    let mut bytes = fs::read(&file).unwrap();
    bytes[0] ^= 0x01;
    fs::write(&file, bytes).unwrap();
}

// ---------------------------------------------------------------- H0, H1

/// An empty `cold_roots` and a missing `raw_root` are refused, nothing
/// created.
#[test]
fn h0_refusals_write_nothing() {
    let fixture = replicated("restore-h0");
    lose_hot(&fixture);
    let before = snapshot(&fixture);
    let mut single = fixture.config.clone();
    single.paths.cold_roots.clear();
    let report = run(
        &single,
        None,
        Flags::default(),
        FIRST_HOT,
        &fixture.probe,
        &|| AT,
    );
    assert_not_restored(&report, &["cold_roots is empty"]);
    assert_eq!(snapshot(&fixture), before);

    fs::remove_dir(&fixture.hot).unwrap();
    let report = restore(&fixture);
    assert_not_restored(&report, &["does not exist", "mount or create"]);
    assert!(!fixture.hot.exists());
}

/// A hot that still carries its marker is refused naming #217, nothing
/// written; so is a second run after a commit (`m_second_run_is_refused`).
#[test]
fn h0_a_marked_hot_is_refused() {
    let fixture = replicated("restore-marked");
    let before = snapshot(&fixture);
    let report = restore(&fixture);
    assert_not_restored(&report, &["carries hot's marker", "#217", "lost hot only"]);
    assert_eq!(snapshot(&fixture), before);
}

#[test]
fn m_second_run_is_refused() {
    let fixture = replicated("restore-second");
    lose_hot(&fixture);
    assert_restored(&restore(&fixture));
    let before = snapshot(&fixture);
    let report = restore_at(&fixture, Flags::default(), FIRST_HOT, AT + 1);
    assert_not_restored(&report, &["#217"]);
    assert_eq!(snapshot(&fixture), before);
}

/// A cold root's marker on `raw_root`, a root without a marker (read-only
/// included), hot's id on a cold root, one id on two roots, two roots on
/// one device: refused, naming them, nothing written.
#[test]
fn h0_roots_ids_and_devices() {
    let fixture = replicated("restore-roots");
    lose_hot(&fixture);
    let before = snapshot(&fixture);
    let marker = |root: &Path, id: &str| {
        let path = root_marker::marker_path(root);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).ok();
        fs::write(path, root_marker::render(id).unwrap()).unwrap();
    };

    root_marker::create_meta(&fixture.hot, "cold-a", false).unwrap();
    assert_not_restored(&restore(&fixture), &["marker of cold root"]);
    fs::remove_dir_all(root_marker::meta_dir(&fixture.hot)).unwrap();

    let mut read_only = Fixture::new("restore-roots-ro");
    assert_eq!(read_only.pass().problems, Vec::<String>::new());
    lose_hot(&read_only);
    let extra = with_read_only_root(&mut read_only);
    fs::remove_file(root_marker::marker_path(&extra)).unwrap();
    let report = restore(&read_only);
    assert_not_restored(&report, &["read-only root", "has no root marker"]);

    let original = fs::read(root_marker::marker_path(&fixture.cold[1])).unwrap();
    for (id, needle) in [
        ("hot", "carries hot's marker"),
        ("cold-a", "both carry the id"),
    ] {
        marker(&fixture.cold[1], id);
        assert_not_restored(&restore(&fixture), &[needle, "cold-b"]);
    }
    fs::write(root_marker::marker_path(&fixture.cold[1]), &original).unwrap();

    let mut same = Fixture::new("restore-roots-device");
    assert_eq!(same.pass().problems, Vec::<String>::new());
    lose_hot(&same);
    same.probe.0[2].1 = 2;
    assert_not_restored(&restore(&same), &["both on device 2"]);
    assert_eq!(snapshot(&fixture), before);
}

/// Every whole-run lock held elsewhere refuses the restore without waiting;
/// only lock files are written.
#[test]
fn h1_locks_are_taken_without_waiting() {
    let fixture = replicated("restore-locks");
    lose_hot(&fixture);
    fs::create_dir(root_marker::meta_dir(&fixture.hot)).unwrap();
    let before = snapshot(&fixture);
    let hot = fixture.hot.clone();
    type Take<'a> = Box<dyn Fn() -> Box<dyn std::any::Any> + 'a>;
    let cases: [(&str, Take); 5] = [
        (
            "copy-ledger lock",
            Box::new(|| Box::new(CopyLedger::at(&hot).try_hold().unwrap())),
        ),
        (
            "pass lock",
            Box::new(|| Box::new(replica::try_replication_lock(&hot).unwrap())),
        ),
        (
            "verify lock",
            Box::new(|| Box::new(replica::try_verify_lock(&hot).unwrap())),
        ),
        (
            "source binance-um",
            Box::new(|| Box::new(removal::take_source_lock(&hot, "binance-um").unwrap())),
        ),
        (
            "source binance-archive",
            Box::new(|| Box::new(removal::take_source_lock(&hot, ARCHIVE_SOURCE).unwrap())),
        ),
    ];
    for (what, take) in cases {
        let held = take();
        let report = restore(&fixture);
        assert_not_restored(&report, &[what, "without waiting"]);
        drop(held);
        assert_eq!(snapshot(&fixture), before, "{what}");
    }
}

/// A `paths.journal` without a file name is refused.
#[test]
fn h0_journal_without_file_name() {
    let mut fixture = replicated("restore-journal-name");
    lose_hot(&fixture);
    fixture.config.paths.journal = PathBuf::from("/");
    assert_not_restored(&restore(&fixture), &["has no file name"]);
}

/// An import-ledger path that is a link to a missing directory is refused.
#[test]
fn h0_dangling_import_ledger() {
    let fixture = replicated("restore-dangling");
    lose_hot(&fixture);
    std::os::unix::fs::symlink(
        fixture.dir.0.join("unmounted"),
        fixture.dir.0.join("ledger"),
    )
    .unwrap();
    let before = snapshot(&fixture);
    assert_not_restored(&restore(&fixture), &["link to a missing directory"]);
    assert_eq!(snapshot(&fixture), before);
}

// ------------------------------------------------------------------- H3

/// The journal copies of the fixture (both targets hold the whole one).
fn journal_copy(fixture: &Fixture, i: usize) -> PathBuf {
    fixture.cold[i].join(".mie/journal/journal.jsonl")
}

/// Hot's surviving journal is the longest: kept byte-identical.
#[test]
fn j_surviving_hot_journal_is_kept() {
    let fixture = replicated("restore-j-kept");
    let journal = fixture.config.paths.journal.clone();
    let mut longer = fs::read(&journal).unwrap();
    longer.extend_from_slice(b"{\"type\":\"run_end\"}\n");
    lose_hot(&fixture);
    fs::write(&journal, &longer).unwrap();
    let report = restore(&fixture);
    assert_restored(&report);
    assert_eq!(fs::read(&journal).unwrap(), longer);
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.contains("extended by 0 byte(s)")),
        "{report:#?}"
    );
}

/// Hot's surviving journal is a strict prefix: extended, its first bytes
/// untouched.
#[test]
fn j_surviving_hot_prefix_is_extended() {
    let fixture = replicated("restore-j-prefix");
    let journal = fixture.config.paths.journal.clone();
    let full = fs::read(&journal).unwrap();
    lose_hot(&fixture);
    fs::write(&journal, &full[..10]).unwrap();
    assert_restored(&restore(&fixture));
    assert_eq!(fs::read(&journal).unwrap(), full);
}

/// Hot's surviving journal diverges: the restore stops, nothing written.
#[test]
fn j_surviving_hot_divergence_stops() {
    let fixture = replicated("restore-j-diverges");
    let journal = fixture.config.paths.journal.clone();
    let mut bad = fs::read(&journal).unwrap()[..10].to_vec();
    bad[3] ^= 0x01;
    lose_hot(&fixture);
    fs::write(&journal, &bad).unwrap();
    let before = snapshot(&fixture);
    assert_not_restored(&restore(&fixture), &["hot's journal", "byte 3", "differ"]);
    assert_eq!(snapshot(&fixture), before);
}

/// Copies that end inside a line: hot gets one `\n` after them, every copy
/// stays a prefix, and the next `run_start` parses as a line of its own.
#[test]
fn j_cut_last_line_is_terminated() {
    let fixture = Fixture::new("restore-j-cut");
    let journal = fixture.config.paths.journal.clone();
    let mut cut = fs::read(&journal).unwrap();
    cut.extend_from_slice(b"{\"type\":\"run_end\",\"run");
    fs::write(&journal, &cut).unwrap();
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    lose_hot(&fixture);
    assert_restored(&restore(&fixture));
    let mut expected = cut.clone();
    expected.push(b'\n');
    assert_eq!(fs::read(&journal).unwrap(), expected);
    for i in 0..2 {
        let copy = fs::read(journal_copy(&fixture, i)).unwrap();
        assert!(expected.starts_with(&copy), "copy {i}");
    }
    let mut file = fs::OpenOptions::new().append(true).open(&journal).unwrap();
    file.write_all(b"{\"type\":\"run_start\",\"run_id\":\"next\"}\n")
        .unwrap();
    let text = fs::read_to_string(&journal).unwrap();
    let last = text.lines().last().unwrap();
    assert!(
        serde_json::from_str::<serde_json::Value>(last).is_ok(),
        "{last}"
    );
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
}

/// A target journal directory without the configured name stops the
/// restore.
#[test]
fn j_renamed_journal_path_stops() {
    let mut fixture = replicated("restore-j-renamed");
    lose_hot(&fixture);
    fixture.config.paths.journal = fixture.dir.0.join("renamed.jsonl");
    assert_not_restored(
        &restore(&fixture),
        &["journal.jsonl", "renamed.jsonl", "renamed paths.journal"],
    );
}

/// No candidate anywhere: no journal, said so; the restore completes.
#[test]
fn j_no_journal_anywhere() {
    let fixture = replicated("restore-j-none");
    lose_hot(&fixture);
    for i in 0..2 {
        fs::remove_file(journal_copy(&fixture, i)).unwrap();
    }
    let report = restore(&fixture);
    assert_restored(&report);
    assert!(!fixture.config.paths.journal.exists());
    assert!(
        report.lines.iter().any(|l| l.contains("no candidate")),
        "{report:#?}"
    );
}

// ------------------------------------------------------------------- H4

/// A target's import-ledger segment directory.
fn segment(fixture: &Fixture, i: usize) -> PathBuf {
    fixture.cold[i].join(".mie/import-ledger/aggTrades")
}

/// Names with a look-alike hyphen, a trailing blank, a zero-width space or
/// a leading `.` are named and never written.
#[test]
fn i_malformed_names_are_never_restored() {
    let fixture = replicated("restore-i-malformed");
    lose_hot(&fixture);
    let names = [
        "BTCUSDT\u{2010}aggTrades-2026-10-05.zip.import",
        "BTCUSDT-aggTrades-2026-10-05.zip.import ",
        "BTCUSDT-aggTrades-2026-10-05\u{200b}.zip.import",
        ".BTCUSDT-aggTrades-2026-10-05.zip.import",
    ];
    for name in names {
        fs::write(segment(&fixture, 0).join(name), b"x").unwrap();
    }
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    let hot = fixture.dir.0.join("ledger/aggTrades");
    for name in names {
        assert!(!hot.join(name).exists(), "{name:?}");
        assert_problem(
            &report,
            &[&format!("{:?}", format!("aggTrades/{name}")), "cold-a"],
        );
    }
    assert!(hot.join("BTCUSDT-aggTrades-2026-10-05.zip.import").exists());
}

/// Generation `u32::MAX` is restored; one more is malformed.
#[test]
fn i_generation_bounds() {
    let fixture = replicated("restore-i-generation");
    let e0 =
        fs::read(segment(&fixture, 0).join("BTCUSDT-aggTrades-2026-10-05.zip.import")).unwrap();
    lose_hot(&fixture);
    let (max, over) = (
        "BTCUSDT-aggTrades-2026-10-05.zip.4294967295.import",
        "BTCUSDT-aggTrades-2026-10-05.zip.4294967296.import",
    );
    for i in 0..2 {
        fs::write(segment(&fixture, i).join(max), &e0).unwrap();
        fs::write(segment(&fixture, i).join(over), &e0).unwrap();
    }
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    let hot = fixture.dir.0.join("ledger/aggTrades");
    assert_eq!(fs::read(hot.join(max)).unwrap(), e0);
    assert!(!hot.join(over).exists());
    assert_problem(&report, &[over, "no import-ledger name"]);
}

/// A surviving `PENDING` is left in place and named; the identical entries
/// are written read-only.
#[test]
fn i_surviving_pending_is_left_and_named() {
    let fixture = replicated("restore-i-pending");
    lose_hot(&fixture);
    let dir = fixture.dir.0.join("ledger");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("PENDING"), b"pending bytes").unwrap();
    let report = restore(&fixture);
    assert_restored(&report);
    assert_eq!(fs::read(dir.join("PENDING")).unwrap(), b"pending bytes");
    assert!(
        report.lines.iter().any(|l| l.contains("PENDING")),
        "{report:#?}"
    );
    let entry = dir.join(ENTRY);
    assert_eq!(
        fs::read(&entry).unwrap(),
        fs::read(segment(&fixture, 0).join("BTCUSDT-aggTrades-2026-10-05.zip.import")).unwrap()
    );
    assert!(fs::metadata(&entry).unwrap().permissions().readonly());
}

// ------------------------------------------------------------------- H5

/// A ledger copy that is no prefix of the install is moved aside on its
/// root; the install's gate holds its length.
#[test]
fn l_non_prefix_copies_move_aside_and_the_gate_is_written() {
    let fixture = replicated("restore-l-aside");
    lose_hot(&fixture);
    let copy = copy_ledger::ledger_path(&fixture.cold[0]);
    let mut bytes = fs::read(&copy).unwrap();
    let at = copy_ledger::HEADER.len() + 3;
    bytes[at] ^= 0x01;
    fs::set_permissions(&copy, fs::Permissions::from_mode(0o644)).ok();
    fs::write(&copy, &bytes).unwrap();
    let report = restore(&fixture);
    assert_restored(&report);
    let aside = replica::damaged_dir(&fixture.cold[0], AT).join("copy-ledger");
    assert_eq!(fs::read(&aside).unwrap(), bytes);
    assert_eq!(
        copy_ledger::check_copy(&fixture.hot, &fixture.cold[1]).unwrap(),
        LedgerCopyCheck::Good
    );
    // l_gate_written.
    let installed = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    assert_eq!(
        rebuilt::read(&fixture.hot).unwrap(),
        Some(installed.len() as u64)
    );
}

/// An existing `.mie/damaged/<at_ms>/` stops the restore before any
/// write.
#[test]
fn l_existing_evidence_stops() {
    let fixture = replicated("restore-l-evidence");
    lose_hot(&fixture);
    fs::create_dir_all(replica::damaged_dir(&fixture.hot, AT)).unwrap();
    let report = restore(&fixture);
    assert_not_restored(&report, &["evidence is never overwritten"]);
    assert!(!fixture.config.paths.journal.exists());
    assert!(!copy_ledger::ledger_path(&fixture.hot).exists());
}

/// A recorded cold copy is never read in H2: a damaged copy of a removed
/// partition outside the copy-back gives no line.
#[test]
fn d_recorded_copies_are_not_reread() {
    let fixture = replicated("restore-d-recorded");
    append(
        &fixture,
        Kind::Removed {
            partition: ARCHIVE.to_owned(),
        },
    );
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    lose_hot(&fixture);
    let part = format!("{ARCHIVE}/part-00000.parquet");
    damage(&fixture.cold[0], &part);
    let report = restore_at(&fixture, Flags::default(), "2026-10-06", AT);
    assert_restored(&report);
    assert!(!fixture.hot.join(ARCHIVE).exists());
}

/// A journal-named path that no root holds: without `--record-lost` the
/// restore stops before any write; with it the path is lost and a
/// covering `select` fails naming it.
#[test]
fn d_lost_needs_the_flag() {
    let fixture = Fixture::new("restore-d-lost");
    let ghost = live(9);
    let journal = fixture.config.paths.journal.clone();
    let mut text = fs::read(&journal).unwrap();
    text.extend_from_slice(
        format!("{{\"type\":\"sealed\",\"run_id\":\"r1\",\"files\":[{{\"path\":\"{ghost}\"}}]}}\n")
            .as_bytes(),
    );
    fs::write(&journal, text).unwrap();
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert_not_restored(&report, &[&ghost, "--record-lost", "before any write"]);
    assert!(!journal.exists());
    assert!(!copy_ledger::ledger_path(&fixture.hot).exists());

    let flags = Flags { record_lost: true };
    let report = restore_at(&fixture, flags, FIRST_HOT, AT);
    assert!(report.restored, "{report:#?}");
    assert_problem(&report, &[&ghost, "no root holds it"]);
    let error = tiered(&fixture)
        .select(&window(D0, D0 + 86_400_000))
        .unwrap_err();
    assert!(error.to_string().contains(&ghost), "{error}");
}

/// The held variant: a pass that stopped between an archive file's parts
/// and its entry, then the hot loss. The restore completes, both parts
/// held `unlisted` and not copied back; the importer refuses; a covering
/// `select` fails naming them.
#[test]
fn held_variant_through_before_entries() {
    let fixture = Fixture::new("restore-held");
    CRASH_AT.with(|crash| crash.set(Some(PassStep::BeforeEntries)));
    let _ = fixture.pass();
    CRASH_AT.with(|crash| crash.set(None));
    let parts: Vec<String> = replica::list_sealed(&fixture.cold[0])
        .unwrap()
        .into_iter()
        .filter(|path| path.starts_with(ARCHIVE))
        .collect();
    assert_eq!(parts.len(), 2, "{parts:?}");
    assert!(!segment(&fixture, 0).exists());
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    for part in &parts {
        assert_problem(&report, &[part, "held (unlisted)"]);
        // c_only_ordinary_paths_are_copied_back.
        assert!(!fixture.hot.join(part).exists());
    }
    assert!(fixture.hot.join(live(0)).exists());
    let ledger = crate::store::check_copy_ledger(&fixture.hot, &fixture.config.paths.cold_roots)
        .unwrap()
        .unwrap();
    let refusal = crate::store::check_holds(&ledger).unwrap_err();
    assert!(refusal.contains("2 archive part(s) are held"), "{refusal}");
    let error = tiered(&fixture)
        .select(&window(D0 - 86_400_000, D0))
        .unwrap_err();
    assert!(error.to_string().contains(&parts[0]), "{error}");
}

// ------------------------------------------------------------------- H6

/// A window partition the start records removed is copied back, then
/// `returned`.
#[test]
fn c_window_partition_returns() {
    let fixture = replicated("restore-c-returns");
    append(
        &fixture,
        Kind::Removed {
            partition: LIVE.to_owned(),
        },
    );
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert_restored(&report);
    for n in 0..3 {
        assert!(fixture.hot.join(live(n)).exists(), "part {n}");
    }
    let records = records_without_time(&fixture);
    assert_eq!(
        records.last(),
        Some(&Kind::Returned {
            partition: LIVE.to_owned()
        })
    );
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.starts_with(&format!("copy-back {LIVE}: 3 copied"))
                && l.ends_with("returned")),
        "{report:#?}"
    );
}

/// The first copy damaged: the next root's is used and the damage named,
/// for verify, then repair; nothing is held.
#[test]
fn c_damaged_copy_is_passed_over() {
    let fixture = replicated("restore-c-damaged");
    lose_hot(&fixture);
    damage(&fixture.cold[0], &live(0));
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(
        &report,
        &[
            &live(0),
            "cold-a",
            "passed over",
            "`mie store verify`, then `mie store repair`",
        ],
    );
    assert_eq!(report.problems.len(), 1, "{report:#?}");
    assert_eq!(
        fs::read(fixture.hot.join(live(0))).unwrap(),
        fs::read(fixture.cold[1].join(live(0))).unwrap()
    );
}

/// Every copy damaged: not copied, still recorded, `Integrity` on read; the
/// restore completes.
#[test]
fn c_no_good_copy_follows_the_lost_file_rule() {
    let fixture = replicated("restore-c-no-good");
    lose_hot(&fixture);
    for root in &fixture.cold {
        damage(root, &live(1));
    }
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(
        &report,
        &[
            &live(1),
            "no cold root holds a verified copy",
            "lost-file rule",
        ],
    );
    assert!(!fixture.hot.join(live(1)).exists());
    assert!(
        fixture
            .records()
            .iter()
            .any(|r| matches!(&r.kind, Kind::Manifest { text } if text.contains(&live(1))))
    );
    let store = tiered(&fixture);
    let dataset = store.select(&window(D0, D0 + 86_400_000)).unwrap();
    let file = dataset
        .files
        .iter()
        .find(|file| file.relative_path == live(1))
        .unwrap();
    assert!(
        matches!(store.read(file), Err(RawStoreError::Integrity(_))),
        "{:?}",
        store.read(file)
    );
}

/// Hot holds other bytes under a path: the restore stops before the
/// marker.
#[test]
fn c_hot_side_error_stops_before_the_marker() {
    let fixture = replicated("restore-c-hot-side");
    let manifest = format!("{LIVE}/part-00002.manifest");
    let mut other = fs::read_to_string(fixture.cold[0].join(&manifest)).unwrap();
    other = other.replacen("rows 2", "rows 3", 1);
    lose_hot(&fixture);
    let dir = fixture.hot.join(LIVE);
    fs::create_dir_all(&dir).unwrap();
    fs::write(fixture.hot.join(&manifest), other).unwrap();
    let report = restore(&fixture);
    assert_not_restored(&report, &[&live(2), "hot's side"]);
}

/// A read-only root first in read order: its verified copy is used, the
/// next root's (damaged) copy never read; damaged there too, the first
/// verified root is used.
#[test]
fn c_read_order() {
    let mut fixture = replicated("restore-c-order");
    lose_hot(&fixture);
    let read_only = with_read_only_root(&mut fixture);
    damage(&fixture.cold[0], &live(0));
    let report = restore(&fixture);
    assert_restored(&report);
    assert_eq!(
        fs::read(fixture.hot.join(live(0))).unwrap(),
        fs::read(read_only.join(live(0))).unwrap()
    );

    let mut fixture = replicated("restore-c-order-2");
    lose_hot(&fixture);
    let read_only = with_read_only_root(&mut fixture);
    damage(&read_only, &live(0));
    damage(&fixture.cold[0], &live(0));
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_eq!(report.problems.len(), 2, "{report:#?}");
    assert_eq!(
        fs::read(fixture.hot.join(live(0))).unwrap(),
        fs::read(fixture.cold[1].join(live(0))).unwrap()
    );
}

/// AW3: a `select` over every window mid-copy-back answers the pre-loss
/// file list and dataset version.
#[test]
fn c_select_during_copy_back_holds_every_file() {
    let fixture = replicated("restore-c-aw3");
    let before = selected(&fixture.hot, &fixture.cold);
    assert!(before.iter().all(|s| !s.starts_with("error")), "{before:?}");
    lose_hot(&fixture);
    let seen: Rc<RefCell<Option<Vec<String>>>> = Rc::default();
    {
        let (seen, hot, cold) = (seen.clone(), fixture.hot.clone(), fixture.cold.clone());
        DURING_COPY_BACK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                *seen.borrow_mut() = Some(selected(&hot, &cold));
            }));
        });
    }
    let report = restore(&fixture);
    DURING_COPY_BACK.with(|hook| *hook.borrow_mut() = None);
    assert_restored(&report);
    assert_eq!(seen.borrow().as_ref(), Some(&before));
    assert_eq!(selected(&fixture.hot, &fixture.cold), before);
}

// ------------------------------------------------------------------- H7

/// A crash after each write step: `mie ingest` and the importer refuse,
/// naming the restore and its re-run, and a re-run passes with the same
/// files, entries and ledger records as an uncrashed run (`at_ms` aside).
#[test]
fn m_crash_at_every_step_resumes() {
    let prepare = |tag: &str| {
        let fixture = replicated(tag);
        append(
            &fixture,
            Kind::Removed {
                partition: LIVE.to_owned(),
            },
        );
        assert_eq!(fixture.pass().problems, Vec::<String>::new());
        lose_hot(&fixture);
        fixture
    };
    let reference = prepare("restore-m-reference");
    assert_restored(&restore(&reference));
    let expected = (hot_state(&reference), records_without_time(&reference));
    for step in Step::ALL {
        let fixture = prepare(&format!("restore-m-{step:?}"));
        AFTER_STEP.with(|crash| crash.set(Some(step)));
        let report = restore(&fixture);
        AFTER_STEP.with(|crash| crash.set(None));
        assert_not_restored(&report, &["simulated crash"]);
        let refusal = check_hot(&fixture.hot, &fixture.config.paths.cold_roots).unwrap_err();
        assert!(
            refusal.contains("mie store restore") && refusal.contains("interrupted restore"),
            "{step:?}: {refusal}"
        );
        let report = restore_at(&fixture, Flags::default(), FIRST_HOT, AT + 1);
        assert_restored(&report);
        assert_eq!(
            (hot_state(&fixture), records_without_time(&fixture)),
            expected,
            "{step:?}"
        );
    }
}

/// A marker gone before the commit stops the restore.
#[test]
fn m_marker_lost_before_commit() {
    let fixture = replicated("restore-m-marker");
    lose_hot(&fixture);
    let cold = fixture.cold[1].clone();
    DURING_COPY_BACK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            fs::remove_file(root_marker::marker_path(&cold)).unwrap();
        }));
    });
    let report = restore(&fixture);
    DURING_COPY_BACK.with(|hook| *hook.borrow_mut() = None);
    assert_not_restored(
        &report,
        &["cold-b", "marker is gone", "before hot's marker"],
    );
}

/// The marker renders and parses back to `hot`, and the install parses
/// whole.
#[test]
fn m_round_trips() {
    let fixture = replicated("restore-m-round");
    lose_hot(&fixture);
    assert_restored(&restore(&fixture));
    assert_eq!(
        root_marker::read(&fixture.hot).unwrap().as_deref(),
        Some(HOT_ID)
    );
    let ledger = copy_ledger::read(&fixture.hot).unwrap().unwrap();
    assert_eq!(ledger.ignored_bytes, 0);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
}

/// `mie store restore`'s output: `restored` with `FAIL:` lines exits 1,
/// without them `PASS`; a stop says `not restored`.
#[test]
fn m_report_and_exit() {
    let print = |fixture: &Fixture, at: i64| {
        let mut out = Vec::new();
        let pass = crate::store::restore(
            &fixture.config,
            Some(&fixture.archive()),
            Flags::default(),
            FIRST_HOT,
            &fixture.probe,
            &|| at,
            &mut out,
        )
        .unwrap();
        (pass, String::from_utf8(out).unwrap())
    };
    let fixture = replicated("restore-m-pass");
    lose_hot(&fixture);
    let (pass, out) = print(&fixture, AT);
    assert!(
        pass && out.ends_with("restored: hot marker written; mie ingest may start\nPASS\n"),
        "{out}"
    );
    let (pass, out) = print(&fixture, AT + 1);
    assert!(
        !pass && out.contains("not restored: no hot marker written\nFAIL: "),
        "{out}"
    );

    let damaged = replicated("restore-m-fail");
    lose_hot(&damaged);
    damage(&damaged.cold[0], &live(0));
    let (pass, out) = print(&damaged, AT);
    assert!(!pass, "{out}");
    assert!(
        out.contains("restored: hot marker written") && !out.contains("PASS"),
        "{out}"
    );
    assert!(root_marker::read(&damaged.hot).unwrap().is_some());
}

/// `.mie/` without a marker: `check_hot` and `mie store init` name the
/// re-run of the restore.
#[test]
fn m_interrupted_restore_is_named() {
    let fixture = replicated("restore-m-interrupted");
    lose_hot(&fixture);
    AFTER_STEP.with(|crash| crash.set(Some(Step::Install)));
    let _ = restore(&fixture);
    AFTER_STEP.with(|crash| crash.set(None));
    let refusal = check_hot(&fixture.hot, &fixture.config.paths.cold_roots).unwrap_err();
    assert!(refusal.contains("interrupted restore"), "{refusal}");
    let mut out = Vec::new();
    assert!(!crate::store::init(&fixture.config, &mut out).unwrap());
    let out = String::from_utf8(out).unwrap();
    assert!(
        out.contains("already exists")
            && out.contains("interrupted restore")
            && out.contains("mie store restore"),
        "{out}"
    );
}

// ------------------------------------------------------------ path forms

/// A `raw_root` that is a symlink to the new directory, and a relative
/// `raw_root`: restored through them, the device check on the target.
#[test]
fn p_relative_and_linked_raw_root() {
    let mut fixture = replicated("restore-p-link");
    lose_hot(&fixture);
    let target = fixture.dir.0.join("new-hot");
    fs::remove_dir(&fixture.hot).unwrap();
    fs::create_dir(&target).unwrap();
    std::os::unix::fs::symlink(&target, &fixture.hot).unwrap();
    assert_restored(&restore(&fixture));
    assert!(target.join(live(0)).exists());
    assert_eq!(root_marker::read(&target).unwrap().as_deref(), Some(HOT_ID));

    let relative = replicated("restore-p-relative");
    lose_hot(&relative);
    let cwd = std::env::current_dir().unwrap();
    let up = "../".repeat(cwd.components().count() - 1);
    let rel = PathBuf::from(format!(
        "{up}{}",
        relative.hot.strip_prefix("/").unwrap().display()
    ));
    assert!(rel.is_relative());
    fixture = relative;
    fixture.config.paths.raw_root = rel;
    rewrite_archive(&fixture);
    let report = restore(&fixture);
    assert_restored(&report);
    assert_eq!(
        root_marker::read(&fixture.dir.0.join("raw"))
            .unwrap()
            .as_deref(),
        Some(HOT_ID)
    );
}

/// H5 b and H6 together: a recorded live path whose copy on cold-a carries
/// another manifest text: the start's text wins, cold-a's copy is named
/// for verify, then repair, and the copy-back passes over it and copies
/// cold-b's.
#[test]
fn d_live_conflict_with_record_is_passed_over() {
    let fixture = replicated("restore-d-conflict");
    lose_hot(&fixture);
    let manifest = fixture.cold[0].join(format!("{LIVE}/part-00000.manifest"));
    let text = fs::read_to_string(&manifest).unwrap();
    fs::set_permissions(&manifest, fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(&manifest, text.replacen("rows 2", "rows 3", 1)).unwrap();
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(
        &report,
        &[
            &live(0),
            "cold-a",
            "differs from the copy ledger's manifest record",
        ],
    );
    assert_eq!(
        fs::read_to_string(fixture.hot.join(format!("{LIVE}/part-00000.manifest"))).unwrap(),
        text
    );
}

/// H4 on disk: a surviving hot copy of the losing variant is moved to
/// `.mie/damaged/<at_ms>/import-ledger/`, never deleted, and the decided
/// variant takes its place.
#[test]
fn i_surviving_losing_entry_is_set_aside() {
    let fixture = replicated("restore-i-aside");
    let good =
        fs::read(segment(&fixture, 0).join("BTCUSDT-aggTrades-2026-10-05.zip.import")).unwrap();
    lose_hot(&fixture);
    let hot_entry = fixture.dir.0.join("ledger").join(ENTRY);
    fs::create_dir_all(hot_entry.parent().unwrap()).unwrap();
    let text = String::from_utf8(good.clone()).unwrap();
    let sha = text
        .lines()
        .find_map(|line| line.strip_prefix("file "))
        .and_then(|line| line.rsplit(' ').next())
        .unwrap()
        .to_owned();
    let losing = text.replacen(&sha, &"0".repeat(64), 1);
    fs::write(&hot_entry, &losing).unwrap();
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_eq!(fs::read(&hot_entry).unwrap(), good);
    let aside = replica::damaged_dir(&fixture.hot, AT)
        .join(IMPORT_LEDGER_DIR)
        .join(ENTRY);
    assert_eq!(fs::read_to_string(aside).unwrap(), losing);
    assert!(crate::store::check_holds(&copy_ledger::read(&fixture.hot).unwrap().unwrap()).is_ok());
}
