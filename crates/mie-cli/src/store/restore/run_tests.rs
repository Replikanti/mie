//! The restore end to end on the replication fixture (#195): every
//! refusal and stop, the journal, the import ledger, the install, the
//! copy-back, the commit and its crash points. The names follow the plan's
//! edge cases.

use super::*;
use crate::store::replicate::tests::{D0, Fixture, manifest_of};
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

    let flags = Flags {
        record_lost: true,
        ..Flags::default()
    };
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
    let damaged = fs::read(fixture.cold[0].join(live(0))).unwrap();
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    // Round 3: k1 never moves a recorded copy.
    assert_eq!(fs::read(fixture.cold[0].join(live(0))).unwrap(), damaged);
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
    let layouts: Vec<BTreeMap<String, Vec<u8>>> =
        fixture.cold.iter().map(|root| live_files(root)).collect();
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    // Round 3: `live(1)` has no copy-back, so no (k): no cold layout changed.
    let after: Vec<BTreeMap<String, Vec<u8>>> =
        fixture.cold.iter().map(|root| live_files(root)).collect();
    assert_eq!(after, layouts);
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

/// The live partition of 2026-10-05: inside the hot window, sorted before
/// LIVE.
const EARLY: &str = "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-05";

/// Seals one part in [`EARLY`] on hot, after the last pass: no record
/// names it on the targets. Returns its data path.
fn early_path(fixture: &Fixture) -> String {
    let paths = fixture.seal("binance-um", "aggTrade", D0 - 86_400_000 + 6_000, 2);
    assert_eq!(paths, vec![format!("{EARLY}/part-00000.parquet")]);
    paths[0].clone()
}

/// A crash after each write step: `mie ingest` and the importer refuse,
/// naming the restore and its re-run, and a re-run passes with the same
/// files, entries and ledger records as an uncrashed run (`at_ms` aside),
/// and leaves the targets' ledger copies as it does: cold-a's non-prefix
/// copy moved aside, also after a crash between the replace and that
/// move (review of #218, finding 2).
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
        // A non-prefix ledger copy on cold-a, which the install moves
        // aside (review of #218, finding 2).
        let copy = copy_ledger::ledger_path(&fixture.cold[0]);
        let mut bytes = fs::read(&copy).unwrap();
        bytes[copy_ledger::HEADER.len() + 3] ^= 0x01;
        fs::set_permissions(&copy, fs::Permissions::from_mode(0o644)).unwrap();
        fs::write(&copy, &bytes).unwrap();
        // A partial `D~` of the replicated window path `live(0)` on cold-a,
        // which the set-aside moves into `.mie/damaged/` (H6 k).
        let data = fs::read(fixture.cold[0].join(live(0))).unwrap();
        fs::write(
            replica::temp_path(&fixture.cold[0], &live(0), replica::TempName::Data).unwrap(),
            &data[..data.len() / 2],
        )
        .unwrap();
        // Round 3: row 3 (`M`+`D~`) on cold-a of an unrecorded window path
        // that sorts before LIVE, so the first move is a k1 move after its
        // copy-back.
        let early = early_path(&fixture);
        let (data, text) = (
            fs::read(fixture.hot.join(&early)).unwrap(),
            fs::read(fixture.hot.join(manifest_of(&early))).unwrap(),
        );
        lose_hot(&fixture);
        let a = &fixture.cold[0];
        fs::create_dir_all(a.join(&early).parent().unwrap()).unwrap();
        fs::write(
            replica::temp_path(a, &early, replica::TempName::Data).unwrap(),
            &data,
        )
        .unwrap();
        fs::write(a.join(manifest_of(&early)), &text).unwrap();
        fixture
    };
    // The partial `D~` is gone from cold-a's layout and kept, byte for
    // byte, below exactly one `.mie/damaged/<at_ms>/`; so are row 3's `M`
    // and `D~`.
    let set_aside = |fixture: &Fixture, step: &str| {
        let a = &fixture.cold[0];
        let partial = replica::temp_path(a, &live(0), replica::TempName::Data).unwrap();
        let early = format!("{EARLY}/part-00000.parquet");
        let early_m = a.join(manifest_of(&early));
        let early_d = replica::temp_path(a, &early, replica::TempName::Data).unwrap();
        let data = fs::read(a.join(live(0))).unwrap();
        let hot_early = fs::read(fixture.hot.join(&early)).unwrap();
        for (file, bytes) in [
            (&partial, data[..data.len() / 2].to_vec()),
            (&early_d, hot_early),
            (
                &early_m,
                fs::read(fixture.hot.join(manifest_of(&early))).unwrap(),
            ),
        ] {
            assert!(!file.exists(), "{step}: {}", file.display());
            let rel = file.strip_prefix(a).unwrap().to_owned();
            let found: Vec<Vec<u8>> = [AT, AT + 1]
                .iter()
                .filter_map(|at| fs::read(replica::damaged_dir(a, *at).join(&rel)).ok())
                .collect();
            assert_eq!(found, vec![bytes], "{step}: {}", file.display());
        }
    };
    // `hot_state` leaves every copy ledger out, so the targets' copies are
    // compared by their check against hot's ledger.
    let copies = |fixture: &Fixture| -> Vec<LedgerCopyCheck> {
        fixture
            .cold
            .iter()
            .map(|cold| copy_ledger::check_copy(&fixture.hot, cold).unwrap())
            .collect()
    };
    // The restore commits; its only `FAIL:` is row 3's "missing" for
    // cold-a's copy of the early path, while that copy is still there.
    let committed = |report: &RestoreReport, step: &str| {
        assert!(report.restored, "{step}: {report:#?}");
        for problem in &report.problems {
            assert!(
                problem.contains(EARLY) && problem.contains("missing"),
                "{step}: {report:#?}"
            );
        }
    };
    let reference = prepare("restore-m-reference");
    committed(&restore(&reference), "reference");
    set_aside(&reference, "reference");
    let expected = (
        hot_state(&reference),
        records_without_time(&reference),
        copies(&reference),
    );
    assert_eq!(
        expected.2,
        vec![LedgerCopyCheck::Absent, LedgerCopyCheck::Good]
    );
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
        committed(&report, &format!("{step:?}"));
        assert_eq!(
            (
                hot_state(&fixture),
                records_without_time(&fixture),
                copies(&fixture)
            ),
            expected,
            "{step:?}"
        );
        set_aside(&fixture, &format!("{step:?}"));
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

// ------------------------------------- paths a cold root holds (review of #218)

/// One capture of two rows on the live date after the restore, by the
/// tiered writer `mie ingest` opens; returns hot's live data paths.
fn capture_after_restore(fixture: &Fixture) -> Vec<String> {
    use mie_adapter_parquet::{RotationPolicy, TierContext};
    use mie_ports::raw::{RawRecord, RawRecordSink};
    let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
    let policy = RotationPolicy {
        max_rows: 2,
        ..RotationPolicy::default()
    };
    let ctx = TierContext {
        first_hot_date: FIRST_HOT.to_owned(),
        now_ms: std::sync::Arc::new(|| AT + 10),
        on_recall: None,
    };
    let mut writer = tiered(fixture)
        .tiered_writer("binance-um", policy, ctx)
        .unwrap();
    for i in 0..2 {
        writer
            .append(
                &key,
                RawRecord {
                    event_time: EventTime::from_millis(D0 + 50_000 + i * 1_000),
                    capture: None,
                    payload: format!("new-{i}").into_bytes(),
                },
            )
            .unwrap();
    }
    writer.close().unwrap();
    replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .filter(|path| path.starts_with(LIVE))
        .collect()
}

/// A fixture whose hot sealed `live(3)` after the pass, then lost hot;
/// `place` puts what a cold root holds of `live(3)` before the loss.
fn sealed_then_lost(tag: &str, place: &dyn Fn(&Fixture)) -> Fixture {
    let fixture = replicated(tag);
    assert_eq!(
        fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2),
        vec![live(3)]
    );
    place(&fixture);
    lose_hot(&fixture);
    fixture
}

/// The data of `live(3)` copied from hot to `root`, without its manifest.
fn data_only(fixture: &Fixture, root: &Path) {
    fs::copy(fixture.hot.join(live(3)), root.join(live(3))).unwrap();
}

/// Review of #218, finding 1: a replication copy that crashed between its
/// data and its manifest rename, then the hot loss. cold-a holds
/// `live(3)`'s data without a manifest, and no record names it. No root
/// holds a sealed copy and nothing can complete this one, so it is a lost
/// candidate: without `--record-lost` the restore stops before any write;
/// with it the path is recorded lost (origin `verify`), the data stays
/// where it is, and the next capture does not reuse the path (D5, D6).
#[test]
fn d_cold_orphan_data_is_a_lost_candidate() {
    let fixture = sealed_then_lost("restore-d-orphan", &|f| data_only(f, &f.cold[0]));
    let old = fs::read(fixture.cold[0].join(live(3))).unwrap();
    let report = restore(&fixture);
    assert_not_restored(&report, &[&live(3), "--record-lost", "before any write"]);
    assert_problem(
        &report,
        &[&live(3), "data file without its manifest on cold-a"],
    );
    assert!(!copy_ledger::ledger_path(&fixture.hot).exists());

    let report = restore_at(
        &fixture,
        Flags {
            record_lost: true,
            ..Flags::default()
        },
        FIRST_HOT,
        AT,
    );
    assert!(report.restored, "{report:#?}");
    assert!(
        records_without_time(&fixture).contains(&Kind::Lost {
            path: live(3),
            origin: copy_ledger::LostOrigin::Verify,
            sha256: None,
        }),
        "{report:#?}"
    );
    let parts = capture_after_restore(&fixture);
    assert!(parts.contains(&live(4)), "{parts:?}");
    assert!(!parts.contains(&live(3)), "{parts:?}");
    assert_eq!(fs::read(fixture.cold[0].join(live(3))).unwrap(), old);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
}

/// Finding 1's second case: cold-a holds `live(3)` with a manifest that
/// does not parse, and no other root or record has its text. A lost
/// candidate as above; its copy is also named as damaged.
#[test]
fn d_cold_bad_manifest_is_a_lost_candidate() {
    let fixture = sealed_then_lost("restore-d-badtext", &|f| {
        data_only(f, &f.cold[0]);
        fs::write(f.cold[0].join(manifest_of(&live(3))), "no manifest\n").unwrap();
    });
    let report = restore(&fixture);
    assert_not_restored(&report, &[&live(3), "--record-lost"]);
    assert_problem(
        &report,
        &[&live(3), "manifest on cold-a that does not parse"],
    );

    let report = restore_at(
        &fixture,
        Flags {
            record_lost: true,
            ..Flags::default()
        },
        FIRST_HOT,
        AT,
    );
    assert!(report.restored, "{report:#?}");
    assert_problem(&report, &[&live(3), "on cold-a", "does not verify"]);
    let parts = capture_after_restore(&fixture);
    assert!(parts.contains(&live(4)), "{parts:?}");
    assert!(!parts.contains(&live(3)), "{parts:?}");
}

/// The same class on a read-only root: a sealed `live(3)` that only the
/// read-only root holds and no record names gets its `manifest` record and
/// is copied back, so the numbering counts it and hot holds it again.
#[test]
fn c_unrecorded_read_only_copy_is_recorded_and_copied_back() {
    let mut fixture = replicated("restore-c-readonly");
    let read_only = with_read_only_root(&mut fixture);
    assert_eq!(
        fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2),
        vec![live(3)]
    );
    let data = fs::read(fixture.hot.join(live(3))).unwrap();
    let text = fs::read_to_string(fixture.hot.join(manifest_of(&live(3)))).unwrap();
    fs::write(read_only.join(live(3)), &data).unwrap();
    fs::write(read_only.join(manifest_of(&live(3))), &text).unwrap();
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert_restored(&report);
    assert!(records_without_time(&fixture).contains(&Kind::Manifest { text }));
    assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data);
    let parts = capture_after_restore(&fixture);
    assert!(parts.contains(&live(4)), "{parts:?}");
    assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data);
}

/// Review of #218, finding 2: a crash after the install's replace and
/// before the target copies are moved aside. The re-run keeps hot's
/// ledger (deviation 4) and still moves the non-prefix copy aside, as an
/// uncrashed run does.
#[test]
fn l_resume_after_replace_moves_non_prefix_copy_aside() {
    let fixture = replicated("restore-l-resume");
    lose_hot(&fixture);
    let copy = copy_ledger::ledger_path(&fixture.cold[0]);
    let mut bytes = fs::read(&copy).unwrap();
    let at = copy_ledger::HEADER.len() + 3;
    bytes[at] ^= 0x01;
    fs::set_permissions(&copy, fs::Permissions::from_mode(0o644)).ok();
    fs::write(&copy, &bytes).unwrap();
    AFTER_STEP.with(|crash| crash.set(Some(Step::Install)));
    let report = restore(&fixture);
    AFTER_STEP.with(|crash| crash.set(None));
    assert!(!report.restored, "{report:#?}");
    // Undo the move-aside rename: the state of a crash between
    // `hold.replace` and the move-aside.
    let aside = replica::damaged_dir(&fixture.cold[0], AT).join("copy-ledger");
    fs::rename(&aside, &copy).unwrap();
    let report = restore_at(&fixture, Flags::default(), FIRST_HOT, AT + 1);
    assert_restored(&report);
    assert!(
        report
            .lines
            .iter()
            .any(|line| line.contains("already holds the derived ledger")),
        "{report:#?}"
    );
    let aside = replica::damaged_dir(&fixture.cold[0], AT + 1).join("copy-ledger");
    assert_eq!(fs::read(&aside).unwrap(), bytes);
    assert_eq!(
        copy_ledger::check_copy(&fixture.hot, &fixture.cold[0]).unwrap(),
        LedgerCopyCheck::Absent
    );
}

// --------------------------- crash debris on cold roots (#195 amendment, B6a)

/// The temp name `name` of `live(n)` on `root`.
fn tmp(root: &Path, n: u32, name: replica::TempName) -> PathBuf {
    replica::temp_path(root, &live(n), name).unwrap()
}

/// The sealed data and manifest text of `live(n)` on the fixture's hot.
fn sealed_of(fixture: &Fixture, n: u32) -> (Vec<u8>, String) {
    (
        fs::read(fixture.hot.join(live(n))).unwrap(),
        fs::read_to_string(fixture.hot.join(manifest_of(&live(n)))).unwrap(),
    )
}

/// Row 5: the state of a D5 copy that crashed after `PublishData`: the
/// final data and its verified pending manifest.
fn publish_data_crash(fixture: &Fixture, root: &Path) {
    let (data, text) = sealed_of(fixture, 3);
    fs::write(root.join(live(3)), &data).unwrap();
    fs::write(tmp(root, 3, replica::TempName::Manifest), &text).unwrap();
}

/// Row 6: the state after `Verify` or `MarkReadOnly`: both temp names,
/// verified, nothing published.
fn verified_temps(fixture: &Fixture, root: &Path) {
    let (data, text) = sealed_of(fixture, 3);
    fs::write(tmp(root, 3, replica::TempName::Data), &data).unwrap();
    fs::write(tmp(root, 3, replica::TempName::Manifest), &text).unwrap();
}

/// A tiered `select` of the live day.
fn select_live_day(fixture: &Fixture) -> Result<String, RawStoreError> {
    tiered(fixture)
        .select(&window(D0, D0 + 86_400_000))
        .map(|dataset| format!("{dataset:?}"))
}

/// Round 2 finding 1 (row 6): `D~` and `M~` on cold-a, both verified, no
/// record. The restore passes without `--record-lost`, records `M~`'s
/// text and copies the part back from `D~`; the next capture seals
/// `part-00004`; the next pass is clean and seals cold-a's copy with the
/// same bytes. Hot or cold-a holds the old bytes at every point.
#[test]
fn x_verified_temps_roll_forward() {
    let fixture = sealed_then_lost("restore-x-temps", &|f| verified_temps(f, &f.cold[0]));
    let data = fs::read(tmp(&fixture.cold[0], 3, replica::TempName::Data)).unwrap();
    let text = fs::read_to_string(tmp(&fixture.cold[0], 3, replica::TempName::Manifest)).unwrap();
    let report = restore(&fixture);
    assert_restored(&report);
    assert!(
        records_without_time(&fixture).contains(&Kind::Manifest { text: text.clone() }),
        "{report:#?}"
    );
    assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data);
    assert_eq!(
        fs::read_to_string(fixture.hot.join(manifest_of(&live(3)))).unwrap(),
        text
    );
    assert!(select_live_day(&fixture).is_ok());
    let parts = capture_after_restore(&fixture);
    assert!(parts.contains(&live(4)), "{parts:?}");
    assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(fs::read(fixture.cold[0].join(live(3))).unwrap(), data);
    assert_eq!(
        fs::read_to_string(fixture.cold[0].join(manifest_of(&live(3)))).unwrap(),
        text
    );
}

/// Round 2 finding 2 (row 5): `D` and a verified `M~` on cold-a, the
/// state a copy that crashed after `PublishData` leaves. Rolled forward:
/// no `lost` record, the day selects, the next capture seals
/// `part-00004`, and the next pass publishes cold-a's manifest.
#[test]
fn x_publish_data_crash_rolls_forward() {
    let fixture = sealed_then_lost("restore-x-publish", &|f| publish_data_crash(f, &f.cold[0]));
    let (data, text) = (
        fs::read(fixture.cold[0].join(live(3))).unwrap(),
        fs::read_to_string(tmp(&fixture.cold[0], 3, replica::TempName::Manifest)).unwrap(),
    );
    let report = restore(&fixture);
    assert_restored(&report);
    let records = records_without_time(&fixture);
    assert!(records.contains(&Kind::Manifest { text: text.clone() }));
    assert!(
        !records
            .iter()
            .any(|k| matches!(k, Kind::Lost { path, .. } if *path == live(3))),
        "{records:?}"
    );
    assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data);
    assert!(select_live_day(&fixture).is_ok());
    let parts = capture_after_restore(&fixture);
    assert!(parts.contains(&live(4)), "{parts:?}");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(fs::read(fixture.cold[0].join(live(3))).unwrap(), data);
    assert_eq!(
        fs::read_to_string(fixture.cold[0].join(manifest_of(&live(3)))).unwrap(),
        text
    );
}

/// `mie store rebuild --replace-hot-ledger` over the fixture at `at`.
fn rebuild_at(fixture: &Fixture, record_lost: bool, at: i64) -> RebuildReport {
    rebuild::run(
        &fixture.config,
        Some(&fixture.archive()),
        rebuild::Flags {
            record_lost,
            replace_hot_ledger: true,
            ..rebuild::Flags::default()
        },
        FIRST_HOT,
        &fixture.probe,
        &|| at,
    )
}

/// Whether hot's ledger records `live(n)` lost.
fn recorded_lost(fixture: &Fixture, n: u32) -> bool {
    records_without_time(fixture)
        .iter()
        .any(|k| matches!(k, Kind::Lost { path, .. } if *path == live(n)))
}

/// Round 2 finding 3a: row 8 restored with `--record-lost`, then a
/// rebuild before any pass has replicated the `lost` record. The rebuild
/// finds the path again: without `--record-lost` it writes nothing and
/// names the path and the flag; with it, it installs `lost`, the next
/// capture seals `part-00004` and the pass is clean.
#[test]
fn x_rebuild_before_the_first_pass_keeps_the_path_counted() {
    let fixture = sealed_then_lost("restore-x-3a", &|f| data_only(f, &f.cold[0]));
    let report = restore_at(
        &fixture,
        Flags {
            record_lost: true,
            ..Flags::default()
        },
        FIRST_HOT,
        AT,
    );
    assert!(report.restored, "{report:#?}");
    let ledger = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    let rebuilt = rebuild_at(&fixture, false, AT + 100);
    assert!(
        rebuilt.lines.iter().any(|l| l == "nothing written"),
        "{rebuilt:#?}"
    );
    assert!(
        rebuilt
            .problems
            .iter()
            .any(|p| p.contains(&live(3)) && p.contains("--record-lost")),
        "{rebuilt:#?}"
    );
    assert_eq!(
        fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap(),
        ledger
    );
    let rebuilt = rebuild_at(&fixture, true, AT + 200);
    assert!(
        rebuilt.lines.iter().any(|l| l.starts_with("wrote ")),
        "{rebuilt:#?}"
    );
    assert!(recorded_lost(&fixture, 3), "{rebuilt:#?}");
    let parts = capture_after_restore(&fixture);
    assert!(parts.contains(&live(4)), "{parts:?}");
    assert!(!parts.contains(&live(3)), "{parts:?}");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
}

/// Round 2 finding 3b: row 8 restored with `--record-lost`, a pass that
/// replicates the `lost` record, then a rebuild: it passes, with no
/// "holds it again", a report line naming the debris left in place, and
/// cold-a's data unchanged.
#[test]
fn x_rebuild_after_a_pass_passes_with_debris_left() {
    let fixture = sealed_then_lost("restore-x-3b", &|f| data_only(f, &f.cold[0]));
    let old = fs::read(fixture.cold[0].join(live(3))).unwrap();
    let report = restore_at(
        &fixture,
        Flags {
            record_lost: true,
            ..Flags::default()
        },
        FIRST_HOT,
        AT,
    );
    assert!(report.restored, "{report:#?}");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let rebuilt = rebuild_at(&fixture, false, AT + 100);
    assert_eq!(rebuilt.problems, Vec::<String>::new(), "{rebuilt:#?}");
    assert!(
        rebuilt
            .lines
            .iter()
            .any(|l| l.starts_with(&format!("{}: recorded lost;", live(3)))
                && l.contains("left in place")),
        "{rebuilt:#?}"
    );
    assert!(recorded_lost(&fixture, 3));
    assert_eq!(fs::read(fixture.cold[0].join(live(3))).unwrap(), old);
}

/// The roots with a current `copy` record of `path` in hot's ledger,
/// sorted.
fn copies(fixture: &Fixture, path: &str) -> Vec<String> {
    let ledger = copy_ledger::read(&fixture.hot).unwrap().unwrap();
    let state = CopyState::fold(&ledger).unwrap();
    let mut roots: Vec<String> = state
        .copies_of(path)
        .filter(|(_, copy)| copy.latest.is_some())
        .map(|(root, _)| root.to_owned())
        .collect();
    roots.sort();
    roots
}

/// Where H6 k moves the file `name` of the live partition on `root` at
/// `at`.
fn aside_of(root: &Path, at: i64, name: &str) -> PathBuf {
    replica::damaged_dir(root, at).join(LIVE).join(name)
}

/// Row 3, round 3: `M` with its data only under `D~` (a power loss undid
/// the data's rename), no other root holds the path. The restore copies
/// the part back from `D~`, names cold-a's copy `missing` without naming
/// the repair, which cannot fix it, and then moves `M` and `D~` aside
/// (H6 k1), so the next pass copies the path from hot: 0 of 2 copies
/// before it, 2 of 2 after it, cold-a's sealed copy equal to the old
/// `D~`.
#[test]
fn x_reordered_renames_roll_forward_from_the_temp_data() {
    let fixture = sealed_then_lost("restore-x-reordered", &|f| {
        let (data, text) = sealed_of(f, 3);
        fs::write(tmp(&f.cold[0], 3, replica::TempName::Data), &data).unwrap();
        fs::write(f.cold[0].join(manifest_of(&live(3))), &text).unwrap();
    });
    let a = fixture.cold[0].clone();
    let data = fs::read(tmp(&a, 3, replica::TempName::Data)).unwrap();
    let text = fs::read_to_string(a.join(manifest_of(&live(3)))).unwrap();
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(&report, &[&live(3), "on cold-a", "missing"]);
    assert!(
        !report
            .problems
            .iter()
            .any(|p| p.contains("missing") && p.contains("mie store repair")),
        "{report:#?}"
    );
    assert!(records_without_time(&fixture).contains(&Kind::Manifest { text: text.clone() }));
    assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data);
    // k1: `M` and `D~` are below `.mie/damaged/<AT>/`, byte-identical, and
    // gone from the layout; one line names both.
    for (name, bytes) in [
        ("part-00003.manifest", text.as_bytes()),
        (".part-00003.parquet.tmp", data.as_slice()),
    ] {
        assert!(!a.join(LIVE).join(name).exists(), "{name}");
        assert_eq!(fs::read(aside_of(&a, AT, name)).unwrap(), bytes, "{name}");
    }
    assert!(
        report.lines.iter().any(|l| l.contains(&live(3))
            && l.contains("cold-a")
            && l.contains("part-00003.manifest")
            && l.contains(".part-00003.parquet.tmp")
            && l.contains("moved")),
        "{report:#?}"
    );
    let parts = capture_after_restore(&fixture);
    assert!(parts.contains(&live(4)), "{parts:?}");
    assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data);
    assert_eq!(copies(&fixture, &live(3)), Vec::<String>::new());
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(copies(&fixture, &live(3)), vec!["cold-a", "cold-b"]);
    assert_eq!(fs::read(a.join(live(3))).unwrap(), data);
}

/// Mode 000: the stand-in for a file that cannot be read (an EIO sector).
fn unreadable(file: &Path) {
    fs::set_permissions(file, fs::Permissions::from_mode(0o000)).unwrap();
}

/// Whether the tests run with rights that read a mode-000 file (root):
/// the unreadable-file tests are skipped then.
fn reads_mode_000(dir: &Path) -> bool {
    let probe = dir.join(".mode-000-probe");
    fs::write(&probe, b"x").unwrap();
    unreadable(&probe);
    let readable = fs::read(&probe).is_ok();
    fs::remove_file(&probe).unwrap();
    readable
}

/// The bytes of `file` whatever its mode, and its mode bits.
fn bytes_and_mode(file: &Path) -> (Vec<u8>, u32) {
    let mode = fs::metadata(file).unwrap().permissions().mode() & 0o777;
    fs::set_permissions(file, fs::Permissions::from_mode(0o644)).unwrap();
    let bytes = fs::read(file).unwrap();
    fs::set_permissions(file, fs::Permissions::from_mode(mode)).unwrap();
    (bytes, mode)
}

/// H6 k1, round 3 and amendment 2: cold-b holds a sealed, verified
/// `live(3)` (so the copy-back succeeds and B4 records `copy` on cold-b
/// only), and cold-a one form per case. k1 moves exactly what the next
/// pass's D5 copy would refuse: afterwards `copy_sealed` from hot to
/// cold-a succeeds, and the next pass is clean, 1 of 2 copies before it
/// and 2 of 2 after it. Moved names are byte-identical below
/// `.mie/damaged/<AT>/`, a kept `D` is unchanged.
#[test]
fn k_leaves_exactly_what_the_pass_accepts() {
    const D: &str = "part-00003.parquet";
    const M: &str = "part-00003.manifest";
    const D_TMP: &str = ".part-00003.parquet.tmp";
    type Place = fn(&[u8], &str) -> Vec<(&'static str, Vec<u8>, bool)>;
    let cases: [(&str, bool, Place); 9] = [
        ("M + D one byte short", true, |data, text| {
            vec![
                (D, data[..data.len() - 1].to_vec(), false),
                (M, text.as_bytes().to_vec(), false),
            ]
        }),
        ("M + D with a flipped byte", true, |data, text| {
            let mut bytes = data.to_vec();
            bytes[0] ^= 0x01;
            vec![(D, bytes, false), (M, text.as_bytes().to_vec(), false)]
        }),
        ("M alone", true, |_, text| {
            vec![(M, text.as_bytes().to_vec(), false)]
        }),
        ("M + verifying D~ (row 3)", true, |data, text| {
            vec![
                (M, text.as_bytes().to_vec(), false),
                (D_TMP, data.to_vec(), false),
            ]
        }),
        ("M that does not parse + D", true, |data, _| {
            vec![
                (D, data.to_vec(), false),
                (M, b"no manifest\n".to_vec(), false),
            ]
        }),
        ("D that verifies, no M", false, |data, _| {
            vec![(D, data.to_vec(), false)]
        }),
        ("D with a flipped byte, no M", true, |data, _| {
            let mut bytes = data.to_vec();
            bytes[0] ^= 0x01;
            vec![(D, bytes, false)]
        }),
        ("an unreadable M", true, |data, text| {
            vec![
                (D, data.to_vec(), false),
                (M, text.as_bytes().to_vec(), true),
            ]
        }),
        ("an unreadable D beside a good M", true, |data, text| {
            vec![
                (D, data.to_vec(), true),
                (M, text.as_bytes().to_vec(), false),
            ]
        }),
    ];
    for (i, (case, moved, place)) in cases.into_iter().enumerate() {
        let placed: RefCell<Vec<(&'static str, Vec<u8>, bool)>> = RefCell::default();
        let fixture = sealed_then_lost(&format!("restore-k1-{i}"), &|f| {
            let (data, text) = sealed_of(f, 3);
            let b = &f.cold[1];
            fs::write(b.join(live(3)), &data).unwrap();
            fs::write(b.join(manifest_of(&live(3))), &text).unwrap();
            let forms = place(&data, &text);
            for (name, bytes, _) in &forms {
                fs::write(f.cold[0].join(LIVE).join(name), bytes).unwrap();
            }
            *placed.borrow_mut() = forms;
        });
        let placed = placed.into_inner();
        if placed.iter().any(|(_, _, mode_000)| *mode_000) {
            if reads_mode_000(&fixture.dir.0) {
                continue;
            }
            for (name, _, mode_000) in &placed {
                if *mode_000 {
                    unreadable(&fixture.cold[0].join(LIVE).join(name));
                }
            }
        }
        let a = fixture.cold[0].clone();
        let report = restore(&fixture);
        assert!(report.restored, "{case}: {report:#?}");
        for (name, bytes, mode_000) in &placed {
            let file = a.join(LIVE).join(name);
            let aside = aside_of(&a, AT, name);
            if moved {
                assert!(!file.exists(), "{case}: {name}");
                let (found, mode) = bytes_and_mode(&aside);
                assert_eq!(&found, bytes, "{case}: {name}");
                assert_eq!(mode == 0, *mode_000, "{case}: {name}");
            } else {
                assert!(!aside.exists(), "{case}: {name}");
                assert_eq!(&fs::read(&file).unwrap(), bytes, "{case}: {name}");
            }
        }
        if moved {
            let found = report
                .lines
                .iter()
                .chain(&report.problems)
                .any(|l| l.contains(&live(3)) && l.contains("cold-a") && l.contains("moved"));
            assert!(found, "{case}: {report:#?}");
        }
        if placed.iter().any(|(_, _, mode_000)| *mode_000) {
            assert_problem(
                &report,
                &[
                    "cold-a",
                    "Permission denied",
                    "failing disk",
                    "`mie store verify`, then `mie store repair`",
                ],
            );
        }
        assert_eq!(copies(&fixture, &live(3)), vec!["cold-b"], "{case}");
        replica::copy_sealed(&fixture.hot, &a, &live(3))
            .unwrap_or_else(|e| panic!("{case}: {e:?}"));
        assert_eq!(fixture.pass().problems, Vec::<String>::new(), "{case}");
        assert_eq!(
            copies(&fixture, &live(3)),
            vec!["cold-a", "cold-b"],
            "{case}"
        );
    }
}

/// k1 never moves a self-consistent other file (D5): the start records
/// `live(3)` and a `copy` on cold-b only, and cold-a holds its own `M`
/// with another text beside a `D` that matches it. The restore commits,
/// its `FAIL:` names D5 and says the file is not moved, cold-a stays
/// byte-identical, and the next pass still reports cold-a's copy, 1 of 2
/// copies (#219 case 2).
#[test]
fn k_self_consistent_other_file_is_never_moved() {
    let fixture = replicated("restore-k1-other-file");
    assert_eq!(
        fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2),
        vec![live(3)]
    );
    let (data, text) = sealed_of(&fixture, 3);
    let a = fixture.cold[0].clone();
    let other = text.replacen("rows ", "rows 9", 1);
    assert_ne!(other, text);
    fs::write(a.join(live(3)), &data).unwrap();
    fs::write(a.join(manifest_of(&live(3))), &other).unwrap();
    let pass = fixture.pass();
    assert!(
        pass.problems.iter().any(|p| p.contains(&live(3))),
        "{pass:#?}"
    );
    assert_eq!(copies(&fixture, &live(3)), vec!["cold-b"]);
    lose_hot(&fixture);
    let before = live_files(&a);
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(&report, &[&live(3), "cold-a", "D5", "not moved"]);
    assert_eq!(live_files(&a), before);
    assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data);
    let pass = fixture.pass();
    assert!(
        pass.problems
            .iter()
            .any(|p| p.contains(&live(3)) && p.contains("cold-a")),
        "{pass:#?}"
    );
    assert_eq!(copies(&fixture, &live(3)), vec!["cold-b"]);
}

/// k1: an aside name already taken stops the restore before hot's marker,
/// nothing below `.mie/damaged/` overwritten; a re-run at `AT + 1`
/// commits and moves into `.mie/damaged/<AT + 1>/`.
#[test]
fn k_aside_collision_stops_before_the_marker() {
    let fixture = sealed_then_lost("restore-k1-collision", &|f| {
        let (data, text) = sealed_of(f, 3);
        fs::write(tmp(&f.cold[0], 3, replica::TempName::Data), &data).unwrap();
        fs::write(f.cold[0].join(manifest_of(&live(3))), &text).unwrap();
    });
    let a = fixture.cold[0].clone();
    let taken = aside_of(&a, AT, "part-00003.manifest");
    fs::create_dir_all(taken.parent().unwrap()).unwrap();
    fs::write(&taken, b"other bytes").unwrap();
    let (data, text) = (
        fs::read(tmp(&a, 3, replica::TempName::Data)).unwrap(),
        fs::read(a.join(manifest_of(&live(3)))).unwrap(),
    );
    let report = restore(&fixture);
    assert_not_restored(&report, &[&live(3), "a new `at_ms`"]);
    assert_eq!(fs::read(&taken).unwrap(), b"other bytes");
    assert!(!root_marker::marker_path(&fixture.hot).exists());
    let report = restore_at(&fixture, Flags::default(), FIRST_HOT, AT + 1);
    assert!(report.restored, "{report:#?}");
    assert_eq!(fs::read(&taken).unwrap(), b"other bytes");
    assert_eq!(
        fs::read(aside_of(&a, AT + 1, "part-00003.manifest")).unwrap(),
        text
    );
    assert_eq!(
        fs::read(aside_of(&a, AT + 1, ".part-00003.parquet.tmp")).unwrap(),
        data
    );
}

/// Row 7 (amendment 2, rule 1′): a parseable `M~` whose data `D~` does
/// not verify gives no text, so the path is a lost candidate: the restore
/// stops before any write naming `--record-lost`; with it, `lost` is
/// recorded, a `select` of the day fails with `Integrity` naming the
/// path, and the next capture seals `part-00004`.
#[test]
fn x_pending_text_without_good_data_is_counted() {
    let fixture = sealed_then_lost("restore-x-pending-bad", &|f| {
        verified_temps(f, &f.cold[0]);
        let data = tmp(&f.cold[0], 3, replica::TempName::Data);
        let mut bytes = fs::read(&data).unwrap();
        bytes[0] ^= 0x01;
        fs::write(&data, bytes).unwrap();
    });
    let report = restore(&fixture);
    assert_not_restored(&report, &[&live(3), "--record-lost", "before any write"]);
    assert!(!copy_ledger::ledger_path(&fixture.hot).exists());
    let report = restore_at(
        &fixture,
        Flags {
            record_lost: true,
            ..Flags::default()
        },
        FIRST_HOT,
        AT,
    );
    assert!(report.restored, "{report:#?}");
    assert!(recorded_lost(&fixture, 3), "{report:#?}");
    match select_live_day(&fixture) {
        Err(RawStoreError::Integrity(why)) => assert!(why.contains(&live(3)), "{why}"),
        other => panic!("{other:?}"),
    }
    let parts = capture_after_restore(&fixture);
    assert!(parts.contains(&live(4)), "{parts:?}");
    assert!(!parts.contains(&live(3)), "{parts:?}");
}

/// Restores row 9 or 10 (`place` on cold-a): without `--record-lost` the
/// restore stops before any write naming the form; with it the part is
/// recorded lost, the debris stays byte-identical and the next capture
/// seals `part-00004`.
fn debris_is_a_lost_candidate(tag: &str, place: &dyn Fn(&Fixture), form: &str) {
    let fixture = sealed_then_lost(tag, place);
    let before = snapshot(&fixture)
        .into_iter()
        .filter(|(path, _)| path.starts_with(&fixture.cold[0]))
        .collect::<Vec<_>>();
    let report = restore(&fixture);
    assert_not_restored(&report, &[&live(3), "--record-lost", "before any write"]);
    assert_problem(&report, &[&live(3), form]);
    assert!(!copy_ledger::ledger_path(&fixture.hot).exists());
    let report = restore_at(
        &fixture,
        Flags {
            record_lost: true,
            ..Flags::default()
        },
        FIRST_HOT,
        AT,
    );
    assert!(report.restored, "{report:#?}");
    assert!(recorded_lost(&fixture, 3), "{report:#?}");
    let parts = capture_after_restore(&fixture);
    assert!(parts.contains(&live(4)), "{parts:?}");
    assert!(!parts.contains(&live(3)), "{parts:?}");
    for (path, bytes) in before {
        if path.starts_with(fixture.cold[0].join(LIVE)) {
            assert_eq!(fs::read(&path).unwrap(), bytes, "{}", path.display());
        }
    }
}

/// Row 9: `D~` alone on cold-a: a lost candidate.
#[test]
fn x_temp_data_alone_is_a_lost_candidate() {
    debris_is_a_lost_candidate(
        "restore-x-temp-data",
        &|f| {
            let (data, _) = sealed_of(f, 3);
            fs::write(tmp(&f.cold[0], 3, replica::TempName::Data), data).unwrap();
        },
        "the data temp file on cold-a",
    );
}

/// Row 10: `D~` with a pending manifest that does not parse (its final
/// `\n` missing): a lost candidate.
#[test]
fn x_torn_pending_manifest_is_debris() {
    debris_is_a_lost_candidate(
        "restore-x-torn",
        &|f| {
            let (data, text) = sealed_of(f, 3);
            fs::write(tmp(&f.cold[0], 3, replica::TempName::Data), data).unwrap();
            fs::write(
                tmp(&f.cold[0], 3, replica::TempName::Manifest),
                text.strip_suffix('\n').unwrap(),
            )
            .unwrap();
        },
        "pending manifest on cold-a that does not parse",
    );
}

/// Row 16: a repair's evidence of the part below cold-a's
/// `.mie/damaged/<dir>/`, nothing else: a lost candidate named by it; the
/// evidence stays byte-identical.
#[test]
fn x_damaged_evidence_names_the_path() {
    let fixture = sealed_then_lost("restore-x-evidence", &|f| {
        let (data, text) = sealed_of(f, 3);
        let aside = replica::damaged_dir(&f.cold[0], 123).join(LIVE);
        fs::create_dir_all(&aside).unwrap();
        fs::write(aside.join("part-00003.parquet"), data).unwrap();
        fs::write(aside.join("part-00003.manifest"), text).unwrap();
    });
    let aside = replica::damaged_dir(&fixture.cold[0], 123);
    let evidence = snapshot(&fixture)
        .into_iter()
        .filter(|(path, _)| path.starts_with(&aside))
        .collect::<Vec<_>>();
    assert_eq!(evidence.len(), 2);
    let report = restore(&fixture);
    assert_not_restored(&report, &[&live(3), "--record-lost"]);
    assert_problem(&report, &[&live(3), ".mie/damaged/123/ on cold-a"]);
    let report = restore_at(
        &fixture,
        Flags {
            record_lost: true,
            ..Flags::default()
        },
        FIRST_HOT,
        AT,
    );
    assert!(report.restored, "{report:#?}");
    assert!(recorded_lost(&fixture, 3));
    let parts = capture_after_restore(&fixture);
    assert!(parts.contains(&live(4)), "{parts:?}");
    for (path, bytes) in evidence {
        assert_eq!(fs::read(&path).unwrap(), bytes, "{}", path.display());
    }
}

/// The files of `root`'s live partition by name.
fn live_files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    sorted_names(&root.join(LIVE))
        .unwrap()
        .into_iter()
        .map(|name| {
            let bytes = fs::read(root.join(LIVE).join(&name)).unwrap();
            (name, bytes)
        })
        .collect()
}

/// H6 k with rows 12 and 13 and a partial `D~` beside a replicated path.
/// cold-a holds `live(3)` sealed plus verifiable temp names (row 13);
/// cold-b a pending manifest with another text beside a partial `D~` (row
/// 12); cold-a a partial `D~` of the replicated `live(0)`. After the
/// copy-back the unverifiable temp names are moved into
/// `.mie/damaged/<at_ms>/` with their bytes, the other text named, the
/// verifiable ones kept; the next pass is clean.
#[test]
fn x_set_aside_keeps_unverifiable_temps() {
    let fixture = sealed_then_lost("restore-x-set-aside", &|f| {
        let (data, text) = sealed_of(f, 3);
        let (a, b) = (&f.cold[0], &f.cold[1]);
        fs::write(a.join(live(3)), &data).unwrap();
        fs::write(a.join(manifest_of(&live(3))), &text).unwrap();
        fs::write(tmp(a, 3, replica::TempName::Data), &data).unwrap();
        fs::write(tmp(a, 3, replica::TempName::Manifest), &text).unwrap();
        fs::write(
            tmp(b, 3, replica::TempName::Manifest),
            text.replacen("rows ", "rows 9", 1),
        )
        .unwrap();
        fs::write(tmp(b, 3, replica::TempName::Data), &data[..data.len() - 1]).unwrap();
        let (zero, _) = sealed_of(f, 0);
        fs::write(tmp(a, 0, replica::TempName::Data), &zero[..zero.len() / 2]).unwrap();
    });
    let (a, b) = (fixture.cold[0].clone(), fixture.cold[1].clone());
    let (before_a, before_b) = (live_files(&a), live_files(&b));
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(
        &report,
        &[&live(3), "on cold-b (its pending manifest)", "differs"],
    );
    let moved = |root: &Path, n: u32, name: replica::TempName| {
        let file = tmp(root, n, name);
        let rel = file.strip_prefix(root).unwrap().to_owned();
        (file, replica::damaged_dir(root, AT).join(rel))
    };
    // Kept: cold-a's verifiable temp names of live(3).
    for name in [replica::TempName::Data, replica::TempName::Manifest] {
        let (file, aside) = moved(&a, 3, name);
        assert!(file.exists() && !aside.exists(), "{}", file.display());
    }
    // Moved, bytes unchanged.
    for (root, n, name, before) in [
        (&b, 3, replica::TempName::Manifest, &before_b),
        (&b, 3, replica::TempName::Data, &before_b),
        (&a, 0, replica::TempName::Data, &before_a),
    ] {
        let (file, aside) = moved(root, n, name);
        assert!(!file.exists(), "{}", file.display());
        let name = file.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(
            fs::read(&aside).unwrap(),
            before[&name],
            "{}",
            aside.display()
        );
    }
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.contains(".part-00000.parquet.tmp") && l.contains("moved")),
        "{report:#?}"
    );
    assert_problem(&report, &[".part-00003.manifest.tmp", "another text"]);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
}

/// Rows 5 and 6 on a read-only root, which no other root holds: both
/// parts are recorded and copied back from its crash forms. Round 3 adds
/// row 2 (`M` with damaged `D`; cold-b holds a good copy, so the
/// copy-back passes over the read-only root, whose `FAIL:` names no
/// command) and row 3 (`M`+`D~`, copied back from `D~`). The root's names,
/// bytes and modes stay unchanged: k1 never writes a read-only root.
#[test]
fn x_read_only_crash_forms_are_read_never_written() {
    use std::os::unix::fs::PermissionsExt;
    let mut fixture = replicated("restore-x-read-only");
    let read_only = with_read_only_root(&mut fixture);
    for (n, start) in [(3, 6_000), (4, 9_000), (5, 12_000), (6, 15_000)] {
        assert_eq!(
            fixture.seal("binance-um", "aggTrade", D0 + start, 2),
            vec![live(n)]
        );
    }
    let (data3, text3) = sealed_of(&fixture, 3);
    let (data4, text4) = sealed_of(&fixture, 4);
    let (data5, text5) = sealed_of(&fixture, 5);
    let (data6, text6) = sealed_of(&fixture, 6);
    fs::write(read_only.join(live(3)), &data3).unwrap();
    fs::write(tmp(&read_only, 3, replica::TempName::Manifest), &text3).unwrap();
    fs::write(tmp(&read_only, 4, replica::TempName::Data), &data4).unwrap();
    fs::write(tmp(&read_only, 4, replica::TempName::Manifest), &text4).unwrap();
    // Row 2 on the read-only root, a good copy on cold-b.
    let mut bad5 = data5.clone();
    bad5[0] ^= 0x01;
    fs::write(read_only.join(live(5)), &bad5).unwrap();
    fs::write(read_only.join(manifest_of(&live(5))), &text5).unwrap();
    fs::write(fixture.cold[1].join(live(5)), &data5).unwrap();
    fs::write(fixture.cold[1].join(manifest_of(&live(5))), &text5).unwrap();
    // Row 3 on the read-only root alone.
    fs::write(tmp(&read_only, 6, replica::TempName::Data), &data6).unwrap();
    fs::write(read_only.join(manifest_of(&live(6))), &text6).unwrap();
    let modes = |root: &Path| -> Vec<(String, Vec<u8>, u32)> {
        live_files(root)
            .into_iter()
            .map(|(name, bytes)| {
                let mode = fs::metadata(root.join(LIVE).join(&name))
                    .unwrap()
                    .permissions()
                    .mode();
                (name, bytes, mode)
            })
            .collect()
    };
    let before = modes(&read_only);
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_eq!(report.problems.len(), 1, "{report:#?}");
    assert_problem(&report, &[&live(5), "cold-r", "never written"]);
    assert!(
        !report.problems[0].contains("mie store repair"),
        "{report:#?}"
    );
    let records = records_without_time(&fixture);
    for (text, data, n) in [
        (&text3, &data3, 3),
        (&text4, &data4, 4),
        (&text5, &data5, 5),
        (&text6, &data6, 6),
    ] {
        assert!(
            records.contains(&Kind::Manifest { text: text.clone() }),
            "{n}"
        );
        assert_eq!(&fs::read(fixture.hot.join(live(n))).unwrap(), data, "{n}");
    }
    assert_eq!(modes(&read_only), before);
}

/// The state of H6 J: LIVE recorded removed and replicated, then hot
/// sealed `live(3)`, whose only cold forms are verified `D~`+`M~` on
/// cold-a (row 6), then the hot loss. Returns the fixture and the data.
fn removed_with_unrecorded_path(tag: &str) -> (Fixture, Vec<u8>) {
    let fixture = replicated(tag);
    append(
        &fixture,
        Kind::Removed {
            partition: LIVE.to_owned(),
        },
    );
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(
        fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2),
        vec![live(3)]
    );
    verified_temps(&fixture, &fixture.cold[0]);
    let (data, _) = sealed_of(&fixture, 3);
    lose_hot(&fixture);
    (fixture, data)
}

/// The first hot date of the J tests: LIVE (2026-10-06) lies before it.
const J_FIRST_HOT: &str = "2026-10-07";

/// H6 J (amendment 2): an unrecorded path recorded from a cold text in a
/// partition the start records removed and that lies before the hot
/// window is copied back on its own: hot holds `live(3)` and none of
/// parts 0–2, and there is no `returned`.
#[test]
fn x_removed_partition_with_an_unrecorded_path_is_copied_back() {
    let (fixture, data) = removed_with_unrecorded_path("restore-x-removed");
    let report = restore_at(&fixture, Flags::default(), J_FIRST_HOT, AT);
    assert_restored(&report);
    assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data);
    for n in 0..3 {
        assert!(!fixture.hot.join(live(n)).exists(), "part {n}");
    }
    assert!(
        !records_without_time(&fixture).contains(&Kind::Returned {
            partition: LIVE.to_owned()
        }),
        "{report:#?}"
    );
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.starts_with(&format!("copy-back {}", live(3)))),
        "{report:#?}"
    );
}

/// Whether the hot removal's plan lists LIVE as an incomplete removal,
/// with the hot window starting on [`J_FIRST_HOT`].
fn live_is_incomplete(fixture: &Fixture) -> bool {
    let ledger = copy_ledger::read(&fixture.hot).unwrap().unwrap();
    let scan = removal::scan_hot(&fixture.hot, J_FIRST_HOT).unwrap();
    let targets = BTreeSet::from(["cold-a".to_owned(), "cold-b".to_owned()]);
    let gate = rebuilt::read(&fixture.hot).unwrap();
    removal::plan(&ledger, &scan, &targets, 2, J_FIRST_HOT, gate)
        .unwrap()
        .incomplete
        .iter()
        .any(|candidate| candidate.partition == LIVE)
}

/// Review round 3, finding 1: J is derived from the installed ledger, so a
/// crash after any write step and a re-run still copy `live(3)` back:
/// hot's `live(3)` equals cold-a's old `D~`, no `returned` for LIVE, a
/// read of it succeeds; the next pass records `copy` on cold-a and cold-b
/// (0 of 2, then 2 of 2), and only then the hot removal lists LIVE as
/// incomplete.
#[test]
fn j_crash_at_every_step_still_copies_back() {
    for step in Step::ALL {
        let (fixture, data) = removed_with_unrecorded_path(&format!("restore-j-crash-{step:?}"));
        AFTER_STEP.with(|crash| crash.set(Some(step)));
        let first = restore_at(&fixture, Flags::default(), J_FIRST_HOT, AT);
        let reached = AFTER_STEP.with(std::cell::Cell::get).is_none();
        AFTER_STEP.with(|crash| crash.set(None));
        if reached {
            assert_not_restored(&first, &["simulated crash"]);
            let report = restore_at(&fixture, Flags::default(), J_FIRST_HOT, AT + 1);
            assert_restored(&report);
        } else {
            // This state never reaches the step (nothing to set aside).
            assert_restored(&first);
        }
        assert_eq!(
            fs::read(fixture.hot.join(live(3))).unwrap(),
            data,
            "{step:?}"
        );
        assert!(
            !records_without_time(&fixture).contains(&Kind::Returned {
                partition: LIVE.to_owned()
            }),
            "{step:?}"
        );
        let store = tiered(&fixture);
        let dataset = store.select(&window(D0, D0 + 86_400_000)).unwrap();
        let file = dataset
            .files
            .iter()
            .find(|file| file.relative_path == live(3))
            .unwrap();
        assert!(store.read(file).is_ok(), "{step:?}");
        assert_eq!(copies(&fixture, &live(3)), Vec::<String>::new(), "{step:?}");
        assert!(!live_is_incomplete(&fixture), "{step:?}");
        assert_eq!(fixture.pass().problems, Vec::<String>::new(), "{step:?}");
        assert_eq!(
            copies(&fixture, &live(3)),
            vec!["cold-a", "cold-b"],
            "{step:?}"
        );
        assert!(live_is_incomplete(&fixture), "{step:?}");
    }
}

/// H6 J: a path an earlier rebuild recorded on an intact hot (from cold-a's
/// verified `M~`, in the removed LIVE), replicated by a pass, then the hot
/// loss: the restore copies it back through J.
#[test]
fn j_path_recorded_by_an_earlier_rebuild_is_copied_back() {
    let fixture = replicated("restore-j-rebuilt");
    append(
        &fixture,
        Kind::Removed {
            partition: LIVE.to_owned(),
        },
    );
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(
        fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2),
        vec![live(3)]
    );
    verified_temps(&fixture, &fixture.cold[0]);
    let (data, text) = sealed_of(&fixture, 3);
    make_writable(&fixture.hot.join(LIVE));
    fs::remove_dir_all(fixture.hot.join(LIVE)).unwrap();
    let rebuilt = rebuild_at(&fixture, false, AT - 100);
    assert!(
        rebuilt.lines.iter().any(|l| l.starts_with("wrote ")),
        "{rebuilt:#?}"
    );
    assert!(records_without_time(&fixture).contains(&Kind::Manifest { text }));
    fixture.pass();
    lose_hot(&fixture);
    let report = restore_at(&fixture, Flags::default(), J_FIRST_HOT, AT);
    assert_restored(&report);
    assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data);
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.starts_with(&format!("copy-back {}", live(3)))),
        "{report:#?}"
    );
}

/// The J state with row 3 on cold-a (`M` + a verifying `D~`, the only
/// text form of `live(3)`) and the damaged form `damage` puts on cold-b,
/// which holds no text form of it. Returns the fixture, the sealed data
/// and the bytes of cold-b's damaged form.
fn j_row3_beside(tag: &str, damage: fn(&Fixture, &[u8]) -> PathBuf) -> (Fixture, Vec<u8>, Vec<u8>) {
    let fixture = replicated(tag);
    append(
        &fixture,
        Kind::Removed {
            partition: LIVE.to_owned(),
        },
    );
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(
        fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2),
        vec![live(3)]
    );
    let (data, text) = sealed_of(&fixture, 3);
    fs::write(tmp(&fixture.cold[0], 3, replica::TempName::Data), &data).unwrap();
    fs::write(fixture.cold[0].join(manifest_of(&live(3))), &text).unwrap();
    let file = damage(&fixture, &data);
    let bytes = fs::read(file).unwrap();
    lose_hot(&fixture);
    (fixture, data, bytes)
}

/// Review round 4 (rule 6): a J path whose only text form is row 3 on
/// cold-a, and cold-b holds a truncated `D` with no `M`, or a half-written
/// `D~`. A crash at any step, the set-aside after (k) moved cold-a's forms
/// included, and a re-run end as the uncrashed run does: hot holds
/// `live(3)`, cold-b's damaged form is gone from its layout and kept byte
/// for byte below exactly one `.mie/damaged/<at_ms>/`, and the next pass
/// records the path on cold-a and cold-b (2 of 2), a second pass clean.
#[test]
fn j_crash_after_row_3_set_aside_still_reaches_every_target() {
    type Damage = fn(&Fixture, &[u8]) -> PathBuf;
    let damages: [(&str, Damage); 2] = [
        ("truncated D", |fixture, data| {
            let file = fixture.cold[1].join(live(3));
            fs::write(&file, &data[..data.len() - 1]).unwrap();
            file
        }),
        ("half D~", |fixture, data| {
            let file = tmp(&fixture.cold[1], 3, replica::TempName::Data);
            fs::write(&file, &data[..data.len() / 2]).unwrap();
            file
        }),
    ];
    for (i, (name, damage)) in damages.into_iter().enumerate() {
        for step in [None].into_iter().chain(Step::ALL.map(Some)) {
            let case = format!("{name}, crash at {step:?}");
            let (fixture, data, bytes) =
                j_row3_beside(&format!("restore-j-row3-{i}-{step:?}"), damage);
            let mut at = AT;
            if let Some(step) = step {
                AFTER_STEP.with(|crash| crash.set(Some(step)));
                let first = restore_at(&fixture, Flags::default(), J_FIRST_HOT, at);
                let reached = AFTER_STEP.with(std::cell::Cell::get).is_none();
                AFTER_STEP.with(|crash| crash.set(None));
                if reached {
                    assert_not_restored(&first, &["simulated crash"]);
                    at += 1;
                } else {
                    assert!(first.restored, "{case}: {first:#?}");
                }
            }
            if at != AT || step.is_none() {
                let report = restore_at(&fixture, Flags::default(), J_FIRST_HOT, at);
                assert!(report.restored, "{case}: {report:#?}");
            }
            assert_eq!(fs::read(fixture.hot.join(live(3))).unwrap(), data, "{case}");
            let b = &fixture.cold[1];
            let in_layout: Vec<String> = fs::read_dir(b.join(LIVE))
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .filter(|name| name.contains("part-00003"))
                .collect();
            assert_eq!(in_layout, Vec::<String>::new(), "{case}");
            let damaged = b.join(".mie").join("damaged");
            let kept = snapshot(&fixture)
                .into_iter()
                .filter(|(path, held)| path.starts_with(&damaged) && *held == bytes)
                .count();
            assert_eq!(kept, 1, "{case}");
            assert_eq!(fixture.pass().problems, Vec::<String>::new(), "{case}");
            assert_eq!(
                copies(&fixture, &live(3)),
                vec!["cold-a", "cold-b"],
                "{case}"
            );
            assert_eq!(fixture.pass().problems, Vec::<String>::new(), "{case}");
        }
    }
}

/// Review round 5 (U, a hot-side error stops): the state of
/// [`j_crash_after_row_3_set_aside_still_reaches_every_target`] with a
/// truncated `D` on cold-b, crashed at `Step::SetAside`, then hot's data
/// file of `live(3)` unreadable (mode 000). For a J path (LIVE before the
/// window) and a planned one (LIVE inside it), the re-run stops before
/// hot's marker and names the path and hot's read error, never "no cold
/// root holds a verified copy". Once hot reads it again, a re-run commits
/// and ends as the uncrashed run: cold-b's `D` kept below `.mie/damaged/`,
/// 2 of 2 copies after the next pass.
#[test]
fn u_unreadable_hot_copy_after_set_aside_crash_stops() {
    for (label, first_hot) in [("J", J_FIRST_HOT), ("planned", FIRST_HOT)] {
        let (fixture, data, bytes) =
            j_row3_beside(&format!("restore-u-hot-copy-{label}"), |fixture, data| {
                let file = fixture.cold[1].join(live(3));
                fs::write(&file, &data[..data.len() - 1]).unwrap();
                file
            });
        if reads_mode_000(&fixture.dir.0) {
            return;
        }
        AFTER_STEP.with(|crash| crash.set(Some(Step::SetAside)));
        let first = restore_at(&fixture, Flags::default(), first_hot, AT);
        assert!(AFTER_STEP.with(std::cell::Cell::get).is_none(), "{label}");
        AFTER_STEP.with(|crash| crash.set(None));
        assert_not_restored(&first, &["simulated crash"]);
        let hot_data = fixture.hot.join(live(3));
        let mode = fs::metadata(&hot_data).unwrap().permissions();
        unreadable(&hot_data);
        let stopped = restore_at(&fixture, Flags::default(), first_hot, AT + 1);
        assert_not_restored(
            &stopped,
            &[&live(3), "Permission denied", "completes once hot reads it"],
        );
        assert!(
            !stopped
                .problems
                .iter()
                .any(|p| p.contains("no cold root holds a verified copy")),
            "{label}: {stopped:#?}"
        );
        assert!(!root_marker::marker_path(&fixture.hot).exists(), "{label}");
        fs::set_permissions(&hot_data, mode).unwrap();
        let report = restore_at(&fixture, Flags::default(), first_hot, AT + 2);
        assert!(report.restored, "{label}: {report:#?}");
        assert_eq!(fs::read(&hot_data).unwrap(), data, "{label}");
        assert!(!fixture.cold[1].join(live(3)).exists(), "{label}");
        let damaged = fixture.cold[1].join(".mie").join("damaged");
        let kept = snapshot(&fixture)
            .into_iter()
            .filter(|(path, held)| path.starts_with(&damaged) && *held == bytes)
            .count();
        assert_eq!(kept, 1, "{label}");
        assert_eq!(fixture.pass().problems, Vec::<String>::new(), "{label}");
        assert_eq!(
            copies(&fixture, &live(3)),
            vec!["cold-a", "cold-b"],
            "{label}"
        );
        assert_eq!(fixture.pass().problems, Vec::<String>::new(), "{label}");
    }
}

/// The reviewer's state of round 3, finding 2: LIVE replicated and removed
/// from hot, then part 1 gone from both cold roots; with `garbage` a
/// half-written `D~` of part 1 on cold-a (a crashed re-copy). Hot's ledger
/// is then gone; hot is otherwise intact.
fn removed_then_garbage(tag: &str, garbage: bool) -> Fixture {
    let fixture = replicated(tag);
    append(
        &fixture,
        Kind::Removed {
            partition: LIVE.to_owned(),
        },
    );
    make_writable(&fixture.hot.join(LIVE));
    fs::remove_dir_all(fixture.hot.join(LIVE)).unwrap();
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    for root in &fixture.cold {
        make_writable(&root.join(LIVE));
        fs::remove_file(root.join(live(1))).unwrap();
        fs::remove_file(root.join(manifest_of(&live(1)))).unwrap();
    }
    if garbage {
        let whole = fs::read(fixture.cold[1].join(live(0))).unwrap();
        fs::write(
            tmp(&fixture.cold[0], 1, replica::TempName::Data),
            &whole[..whole.len() / 2],
        )
        .unwrap();
    }
    let ledger = copy_ledger::ledger_path(&fixture.hot);
    make_writable(ledger.parent().unwrap());
    fs::remove_file(&ledger).unwrap();
    fixture
}

/// Review round 3, finding 2 (rule 4′), the rebuild on an intact hot: with
/// and without the half `D~`, the same lost candidate (`live(1)`, origin
/// `ledger`) and nothing written; with `--record-lost`, `lost` is
/// recorded and the `D~` stays byte-identical.
#[test]
fn rebuild_keeps_the_lost_candidate_beside_garbage_temps() {
    for garbage in [false, true] {
        let fixture = removed_then_garbage(&format!("restore-r4-rebuild-{garbage}"), garbage);
        let report = rebuild_at(&fixture, false, AT);
        assert!(
            report.lines.iter().any(|l| l == "nothing written"),
            "{garbage}: {report:#?}"
        );
        assert!(
            report
                .lines
                .iter()
                .any(|l| l.starts_with(&format!("lost candidate: {}: origin ledger", live(1)))),
            "{garbage}: {report:#?}"
        );
        assert!(!copy_ledger::ledger_path(&fixture.hot).exists());
        let partial = tmp(&fixture.cold[0], 1, replica::TempName::Data);
        let before = fs::read(&partial).ok();
        let report = rebuild_at(&fixture, true, AT + 1);
        assert!(
            report.lines.iter().any(|l| l.starts_with("wrote ")),
            "{garbage}: {report:#?}"
        );
        assert!(recorded_lost(&fixture, 1), "{garbage}");
        assert_eq!(fs::read(&partial).ok(), before, "{garbage}");
    }
}

/// Review round 3, finding 2 (rule 4′), the restore, LIVE outside the
/// copy-back plan: with and without the half `D~`, the same stop naming
/// `live(1)` and `--record-lost`; with the flag, `lost` is recorded and
/// the `D~` stays byte-identical.
#[test]
fn restore_keeps_the_lost_candidate_outside_the_plan() {
    for garbage in [false, true] {
        let fixture = removed_then_garbage(&format!("restore-r4-restore-{garbage}"), garbage);
        lose_hot(&fixture);
        let report = restore_at(&fixture, Flags::default(), "2026-10-08", AT);
        assert_not_restored(&report, &[&live(1), "--record-lost"]);
        let partial = tmp(&fixture.cold[0], 1, replica::TempName::Data);
        let before = fs::read(&partial).ok();
        let flags = Flags {
            record_lost: true,
            ..Flags::default()
        };
        let report = restore_at(&fixture, flags, "2026-10-08", AT);
        assert!(report.restored, "{garbage}: {report:#?}");
        assert!(recorded_lost(&fixture, 1), "{garbage}");
        assert_eq!(fs::read(&partial).ok(), before, "{garbage}");
    }
}

/// The live partition of day `day` (0 = the fixture's live date).
fn live_on(day: i64) -> String {
    let date = ["2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09"][day as usize];
    format!("source=binance-um/instrument=BTCUSDT/stream=aggTrade/date={date}")
}

/// One capture of two rows on each day of `days`, by the tiered writer;
/// returns hot's live data paths.
fn capture_on(fixture: &Fixture, days: &[i64]) -> Vec<String> {
    use mie_adapter_parquet::{RotationPolicy, TierContext};
    use mie_ports::raw::{RawRecord, RawRecordSink};
    let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
    let policy = RotationPolicy {
        max_rows: 2,
        ..RotationPolicy::default()
    };
    let ctx = TierContext {
        first_hot_date: FIRST_HOT.to_owned(),
        now_ms: std::sync::Arc::new(|| AT + 10),
        on_recall: None,
    };
    let mut writer = tiered(fixture)
        .tiered_writer("binance-um", policy, ctx)
        .unwrap();
    for day in days {
        for i in 0..2 {
            writer
                .append(
                    &key,
                    RawRecord {
                        event_time: EventTime::from_millis(
                            D0 + day * 86_400_000 + 50_000 + i * 1_000,
                        ),
                        capture: None,
                        payload: format!("new-{day}-{i}").into_bytes(),
                    },
                )
                .unwrap();
        }
    }
    writer.close().unwrap();
    replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .filter(|path| path.starts_with("source=binance-um/"))
        .collect()
}

/// Both modes agree (round 2 finding 3): rows 1, 5, 6 and 8 in four
/// partitions, then the restore and a capture; the restore, a rebuild
/// before any pass and a capture; the restore, a pass, a rebuild and a
/// capture. Every capture takes the same next part numbers, and none
/// reuses a path a cold root holds.
#[test]
fn x_rebuild_agrees_with_the_restore() {
    let prepare = |tag: &str| {
        let fixture = replicated(tag);
        let mut placed = Vec::new();
        for (day, row) in [(1, 1), (2, 5), (3, 6), (0, 8)] {
            let paths = fixture.seal("binance-um", "aggTrade", D0 + day * 86_400_000 + 6_000, 2);
            let [path] = &paths[..] else {
                panic!("{paths:?}");
            };
            let data = fs::read(fixture.hot.join(path)).unwrap();
            let text = fs::read_to_string(fixture.hot.join(manifest_of(path))).unwrap();
            let a = &fixture.cold[0];
            let d_tmp = replica::temp_path(a, path, replica::TempName::Data).unwrap();
            let m_tmp = replica::temp_path(a, path, replica::TempName::Manifest).unwrap();
            fs::create_dir_all(a.join(path).parent().unwrap()).unwrap();
            match row {
                1 => {
                    fs::write(a.join(path), &data).unwrap();
                    fs::write(a.join(manifest_of(path)), &text).unwrap();
                }
                5 => {
                    fs::write(a.join(path), &data).unwrap();
                    fs::write(&m_tmp, &text).unwrap();
                }
                6 => {
                    fs::write(&d_tmp, &data).unwrap();
                    fs::write(&m_tmp, &text).unwrap();
                }
                _ => fs::write(a.join(path), &data).unwrap(),
            }
            placed.push(path.clone());
        }
        lose_hot(&fixture);
        let report = restore_at(
            &fixture,
            Flags {
                record_lost: true,
                ..Flags::default()
            },
            FIRST_HOT,
            AT,
        );
        assert!(report.restored, "{tag}: {report:#?}");
        (fixture, placed)
    };
    let next = |fixture: &Fixture, placed: &[String], tag: &str| -> Vec<String> {
        let before = replica::list_sealed(&fixture.hot).unwrap();
        let after = capture_on(fixture, &[0, 1, 2, 3]);
        let new: Vec<String> = after.into_iter().filter(|p| !before.contains(p)).collect();
        for path in placed {
            assert!(!new.contains(path), "{tag}: {path} reused: {new:?}");
        }
        new
    };
    let (alone, placed) = prepare("restore-x-agree-alone");
    let expected = next(&alone, &placed, "restore");
    assert_eq!(
        expected,
        vec![
            format!("{}/part-00004.parquet", live_on(0)),
            format!("{}/part-00001.parquet", live_on(1)),
            format!("{}/part-00001.parquet", live_on(2)),
            format!("{}/part-00001.parquet", live_on(3)),
        ]
    );
    let (before_pass, placed) = prepare("restore-x-agree-before");
    let rebuilt = rebuild_at(&before_pass, true, AT + 100);
    assert!(
        rebuilt.lines.iter().any(|l| l.starts_with("wrote ")),
        "{rebuilt:#?}"
    );
    assert_eq!(
        next(&before_pass, &placed, "rebuild before a pass"),
        expected
    );
    let (after_pass, placed) = prepare("restore-x-agree-after");
    assert_eq!(after_pass.pass().problems, Vec::<String>::new());
    let rebuilt = rebuild_at(&after_pass, false, AT + 100);
    assert!(
        rebuilt.lines.iter().any(|l| l.starts_with("wrote ")),
        "{rebuilt:#?}"
    );
    assert_eq!(next(&after_pass, &placed, "rebuild after a pass"), expected);
}

// ------------------------------------- unreadable inputs (amendment 2, U)

/// `report`'s lines and problems with `from` replaced by `to`: two twin
/// fixtures compared line for line.
fn normalized(report: &RestoreReport, from: &Path, to: &Path) -> (Vec<String>, Vec<String>) {
    let (from, to) = (from.display().to_string(), to.display().to_string());
    let fix = |lines: &[String]| -> Vec<String> {
        lines.iter().map(|line| line.replace(&from, &to)).collect()
    };
    (fix(&report.lines), fix(&report.problems))
}

/// Review round 3, finding 3: an unreadable leftover temp name (mode 000)
/// beside a sealed copy on cold-a never stops the restore. The first run
/// commits with exit 1; its `FAIL:` names the file, the read error, verify
/// and repair; the file is below `.mie/damaged/<AT>/<partition>/` with its
/// 7 bytes and mode 000; `--record-lost` changes nothing, and the next
/// pass is clean. The same holds for an unreadable `M~`.
#[test]
fn u_unreadable_temp_is_moved_never_a_stop() {
    for (i, name) in [replica::TempName::Data, replica::TempName::Manifest]
        .into_iter()
        .enumerate()
    {
        let prepare = |tag: String| {
            let fixture = replicated(&tag);
            let file = tmp(&fixture.cold[0], 1, name);
            fs::write(&file, b"partial").unwrap();
            unreadable(&file);
            lose_hot(&fixture);
            (fixture, file)
        };
        let (fixture, file) = prepare(format!("restore-u-temp-{i}"));
        if reads_mode_000(&fixture.dir.0) {
            return;
        }
        let report = restore(&fixture);
        assert!(report.restored, "{name:?}: {report:#?}");
        let file_name = file.file_name().unwrap().to_string_lossy().into_owned();
        let aside = aside_of(&fixture.cold[0], AT, &file_name);
        assert_problem(
            &report,
            &[
                &file_name,
                "cold-a",
                "Permission denied",
                &aside.display().to_string(),
                "failing disk",
                "`mie store verify`, then `mie store repair`",
            ],
        );
        assert!(!file.exists(), "{name:?}");
        assert_eq!(bytes_and_mode(&aside), (b"partial".to_vec(), 0), "{name:?}");
        let (twin, _) = prepare(format!("restore-u-temp-{i}-flag"));
        let flagged = restore_at(
            &twin,
            Flags {
                record_lost: true,
                ..Flags::default()
            },
            FIRST_HOT,
            AT,
        );
        assert_eq!(
            normalized(&flagged, &twin.dir.0, &fixture.dir.0),
            normalized(&report, &fixture.dir.0, &fixture.dir.0),
            "{name:?}"
        );
        assert_eq!(fixture.pass().problems, Vec::<String>::new(), "{name:?}");
    }
}

/// An unreadable `M~` never stops either command: beside a path hot
/// holds, the rebuild's records equal a twin's without it; as the only
/// form of an unrecorded path it is a lost candidate named "cannot be
/// read", and with `--record-lost` the path is recorded lost and the file
/// stays untouched.
#[test]
fn u_unreadable_pending_manifest_never_stops() {
    let fixture = replicated("restore-u-pending-hot");
    if reads_mode_000(&fixture.dir.0) {
        return;
    }
    let file = tmp(&fixture.cold[0], 1, replica::TempName::Manifest);
    fs::write(&file, b"partial").unwrap();
    unreadable(&file);
    let report = rebuild_at(&fixture, false, AT);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    let twin = replicated("restore-u-pending-hot-twin");
    let twin_report = rebuild_at(&twin, false, AT);
    assert_eq!(twin_report.problems, Vec::<String>::new());
    assert_eq!(records_without_time(&fixture), records_without_time(&twin));

    // The only form of an unrecorded path, in both commands.
    let place = |f: &Fixture| {
        let file = tmp(&f.cold[0], 3, replica::TempName::Manifest);
        fs::write(&file, sealed_of(f, 3).1).unwrap();
        unreadable(&file);
    };
    let fixture = sealed_then_lost("restore-u-pending-only", &place);
    let file = tmp(&fixture.cold[0], 3, replica::TempName::Manifest);
    let before = bytes_and_mode(&file);
    let report = restore(&fixture);
    assert_not_restored(&report, &[&live(3), "cannot be read", "--record-lost"]);
    let flags = Flags {
        record_lost: true,
        ..Flags::default()
    };
    let report = restore_at(&fixture, flags, FIRST_HOT, AT);
    assert!(report.restored, "{report:#?}");
    assert!(recorded_lost(&fixture, 3));
    assert_eq!(bytes_and_mode(&file), before);

    let fixture = replicated("restore-u-pending-only-rebuild");
    assert_eq!(
        fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2),
        vec![live(3)]
    );
    place(&fixture);
    let file = tmp(&fixture.cold[0], 3, replica::TempName::Manifest);
    for name in [live(3), manifest_of(&live(3))] {
        make_writable(&fixture.hot.join(LIVE));
        fs::remove_file(fixture.hot.join(name)).unwrap();
    }
    let before = bytes_and_mode(&file);
    let report = rebuild_at(&fixture, false, AT);
    assert!(
        report.lines.iter().any(|l| l == "nothing written"),
        "{report:#?}"
    );
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains(&live(3)) && p.contains("cannot be read")),
        "{report:#?}"
    );
    let report = rebuild_at(&fixture, true, AT + 1);
    assert!(
        report.lines.iter().any(|l| l.starts_with("wrote ")),
        "{report:#?}"
    );
    assert!(recorded_lost(&fixture, 3));
    assert_eq!(bytes_and_mode(&file), before);
}

/// One case per row of U's table, each in the restore and, on the same
/// state with hot intact, in the rebuild, which stops as main does.
#[test]
fn u_unreadable_inputs_in_the_restore() {
    let probe = replicated("restore-u-inputs-probe");
    if reads_mode_000(&probe.dir.0) {
        return;
    }
    let stops = |fixture: &Fixture, case: &str| {
        let report = rebuild_at(fixture, false, AT - 1_000);
        assert!(
            report.lines.iter().any(|l| l == "nothing written")
                && report
                    .problems
                    .iter()
                    .any(|p| p.contains("Permission denied")),
            "{case}: {report:#?}"
        );
    };

    // A cold `M`, recorded: a damaged copy passed over, never moved.
    let fixture = replicated("restore-u-m-recorded");
    let m = fixture.cold[0].join(manifest_of(&live(0)));
    unreadable(&m);
    stops(&fixture, "recorded M");
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(&report, &[&live(0), "cold-a", "cannot be read"]);
    assert!(m.exists());
    assert_eq!(
        fs::read(fixture.hot.join(live(0))).unwrap(),
        fs::read(fixture.cold[1].join(live(0))).unwrap()
    );

    // A cold `M`, unrecorded and alone: a lost candidate (row 4).
    let fixture = replicated("restore-u-m-unrecorded");
    assert_eq!(
        fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2),
        vec![live(3)]
    );
    data_only(&fixture, &fixture.cold[0]);
    let m = fixture.cold[0].join(manifest_of(&live(3)));
    fs::write(&m, sealed_of(&fixture, 3).1).unwrap();
    unreadable(&m);
    stops(&fixture, "unrecorded M");
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert_not_restored(&report, &[&live(3), "--record-lost"]);
    assert_problem(&report, &[&live(3), "cannot be read"]);

    // A target's ledger copy: rejected, named, not moved.
    let fixture = replicated("restore-u-ledger-copy");
    let copy = copy_ledger::ledger_path(&fixture.cold[0]);
    unreadable(&copy);
    stops(&fixture, "ledger copy");
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(&report, &["cold-a", "cannot be read"]);
    assert!(copy.exists());

    // An entry copy with a readable twin: hot gets the twin's bytes, the
    // unreadable copy is named for verify, then repair.
    let fixture = replicated("restore-u-entry-twin");
    let entry = segment(&fixture, 0).join(ENTRY.rsplit_once('/').unwrap().1);
    unreadable(&entry);
    stops(&fixture, "entry with twin");
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(
        &report,
        &[
            "cold-a",
            "cannot be read",
            "`mie store verify`, then `mie store repair`",
        ],
    );
    assert_eq!(
        fs::read(fixture.dir.0.join("ledger").join(ENTRY)).unwrap(),
        fs::read(segment(&fixture, 1).join(ENTRY.rsplit_once('/').unwrap().1)).unwrap()
    );

    // An entry copy with no readable twin: the name is absent, its parts
    // are held `unlisted`, and the `FAIL:` names `mie store resolve`.
    let fixture = replicated("restore-u-entry-alone");
    for i in 0..2 {
        unreadable(&segment(&fixture, i).join(ENTRY.rsplit_once('/').unwrap().1));
    }
    stops(&fixture, "entry alone");
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(&report, &["cannot be read", "mie store resolve"]);
    assert_problem(&report, &["held (unlisted)"]);
    assert!(!fixture.dir.0.join("ledger").join(ENTRY).exists());

    // A quarantined manifest: counted without text, a `FAIL:`.
    let fixture = replicated("restore-u-quarantine");
    let part = replica::list_sealed(&fixture.cold[0])
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with(ARCHIVE))
        .unwrap();
    let quarantined = replica::quarantine_dir(&fixture.cold[0], "q-1").join(manifest_of(&part));
    fs::create_dir_all(quarantined.parent().unwrap()).unwrap();
    fs::write(&quarantined, b"mie-manifest\n").unwrap();
    unreadable(&quarantined);
    let report = rebuild_at(&fixture, false, AT - 1_000);
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains("cannot read the quarantine")),
        "{report:#?}"
    );
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert!(
        !report
            .problems
            .iter()
            .any(|p| p.contains("cannot read the quarantine")),
        "{report:#?}"
    );
    assert_problem(&report, &["cannot be read", "q-1"]);

    // A journal candidate of at most C's length: left out with a `FAIL:`.
    let fixture = replicated("restore-u-journal-short");
    let short = journal_copy(&fixture, 0);
    let bytes = fs::read(&short).unwrap();
    fs::set_permissions(&short, fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(&short, &bytes[..bytes.len() / 2]).unwrap();
    unreadable(&short);
    lose_hot(&fixture);
    let report = restore(&fixture);
    assert!(report.restored, "{report:#?}");
    assert_problem(&report, &["journal candidate", "cold-a", "cannot be read"]);
    assert_eq!(fs::read(&fixture.config.paths.journal).unwrap(), bytes);

    // Longer than C: the stop names the flag and a plain re-run; nothing
    // is written.
    let fixture = replicated("restore-u-journal-long");
    let b = journal_copy(&fixture, 1);
    let bytes = fs::read(&b).unwrap();
    fs::set_permissions(&b, fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(&b, &bytes[..bytes.len() - 5]).unwrap();
    unreadable(&journal_copy(&fixture, 0));
    lose_hot(&fixture);
    let before = snapshot(&fixture);
    let report = restore(&fixture);
    assert_not_restored(
        &report,
        &[
            "cold-a",
            "cannot be read",
            "its last 5 byte(s)",
            "a plain re-run",
            "--skip-unreadable-journal",
        ],
    );
    assert_eq!(snapshot(&fixture), before);
}

/// The flags with `--skip-unreadable-journal`.
fn skip_flag() -> Flags {
    Flags {
        skip_unreadable_journal: true,
        ..Flags::default()
    }
}

/// cold-a's journal copy one byte longer than cold-b's (C, cut inside its
/// last line), cold-a's unreadable; then the hot loss. Returns the
/// fixture and cold-a's bytes.
fn journal_one_byte_longer(tag: &str) -> (Fixture, Vec<u8>) {
    let fixture = replicated(tag);
    let (a, b) = (journal_copy(&fixture, 0), journal_copy(&fixture, 1));
    let full = fs::read(&a).unwrap();
    fs::set_permissions(&b, fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(&b, &full[..full.len() - 1]).unwrap();
    unreadable(&a);
    lose_hot(&fixture);
    (fixture, full)
}

/// H3′, `--skip-unreadable-journal`: an unreadable journal copy one byte
/// longer than C stops a plain run with nothing written; after a
/// transient error a plain re-run takes it as C. With the flag it is left
/// out: exit 1, hot's journal from C (plus H3's `\n`), one `FAIL:` naming
/// the copy, its size, "its last 1 byte(s)" and the aside path, which
/// holds the copy with its bytes and mode 000; the next pass rewrites
/// cold-a's copy from byte 0, and a second pass is clean. The flag
/// changes nothing without an unreadable candidate, or with one of C's
/// length; both copies unreadable stop ("none reads"), and with the flag
/// the journal stays absent with both copies moved. Hot's own unreadable
/// journal goes to `<journal>.unreadable-<AT>`: by the flag as the
/// longest, by U at C's length.
#[test]
fn u_skip_unreadable_journal() {
    let probe = replicated("restore-u-skip-probe");
    if reads_mode_000(&probe.dir.0) {
        return;
    }
    let journal = |f: &Fixture| f.config.paths.journal.clone();
    // Without the flag: the stop, then a transient error cleared.
    let (fixture, full) = journal_one_byte_longer("restore-u-skip-plain");
    let before = snapshot(&fixture);
    let report = restore(&fixture);
    assert_not_restored(
        &report,
        &["cold-a", "its last 1 byte(s)", "--skip-unreadable-journal"],
    );
    assert_eq!(snapshot(&fixture), before);
    fs::set_permissions(journal_copy(&fixture, 0), fs::Permissions::from_mode(0o444)).unwrap();
    let report = restore(&fixture);
    assert_restored(&report);
    assert_eq!(fs::read(journal(&fixture)).unwrap(), full);

    // With the flag after the stop.
    let (fixture, full) = journal_one_byte_longer("restore-u-skip-flag");
    assert!(!restore(&fixture).restored);
    let report = restore_at(&fixture, skip_flag(), FIRST_HOT, AT);
    assert!(report.restored, "{report:#?}");
    let b = fs::read(journal_copy(&fixture, 1)).unwrap();
    let mut expected = b.clone();
    expected.push(b'\n');
    assert_eq!(fs::read(journal(&fixture)).unwrap(), expected);
    let aside = replica::damaged_dir(&fixture.cold[0], AT).join(".mie/journal/journal.jsonl");
    let journal_fails: Vec<&String> = report
        .problems
        .iter()
        .filter(|p| p.contains("journal candidate"))
        .collect();
    assert_eq!(journal_fails.len(), 1, "{report:#?}");
    for needle in [
        "cold-a".to_owned(),
        format!("({} bytes)", full.len()),
        "its last 1 byte(s)".to_owned(),
        aside.display().to_string(),
        "`mie store verify`, then `mie store repair`".to_owned(),
    ] {
        assert!(journal_fails[0].contains(&needle), "{needle}: {report:#?}");
    }
    assert!(!journal_copy(&fixture, 0).exists());
    assert_eq!(bytes_and_mode(&aside), (full.clone(), 0));
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    for i in 0..2 {
        assert_eq!(
            fs::read(journal_copy(&fixture, i)).unwrap(),
            expected,
            "{i}"
        );
    }
    assert_eq!(fixture.pass().problems, Vec::<String>::new());

    // The flag with no unreadable candidate, or one of C's length: the
    // report equals the plain run's.
    for (i, shorter) in [false, true].into_iter().enumerate() {
        let prepare = |tag: String| {
            let fixture = replicated(&tag);
            if shorter {
                let a = journal_copy(&fixture, 0);
                fs::set_permissions(&a, fs::Permissions::from_mode(0o000)).unwrap();
            }
            lose_hot(&fixture);
            fixture
        };
        let plain = prepare(format!("restore-u-skip-same-{i}"));
        let flagged = prepare(format!("restore-u-skip-same-{i}-flag"));
        let plain_report = restore(&plain);
        let flagged_report = restore_at(&flagged, skip_flag(), FIRST_HOT, AT);
        assert_eq!(
            normalized(&flagged_report, &flagged.dir.0, &plain.dir.0),
            normalized(&plain_report, &plain.dir.0, &plain.dir.0),
            "{i}"
        );
    }

    // Both copies unreadable: "none reads"; with the flag no journal, two
    // `FAIL:` lines and both copies moved.
    let fixture = replicated("restore-u-skip-none");
    let full = fs::read(journal_copy(&fixture, 0)).unwrap();
    for i in 0..2 {
        unreadable(&journal_copy(&fixture, i));
    }
    lose_hot(&fixture);
    assert_not_restored(&restore(&fixture), &["none reads"]);
    let report = restore_at(&fixture, skip_flag(), FIRST_HOT, AT);
    assert!(report.restored, "{report:#?}");
    assert!(!journal(&fixture).exists());
    assert!(
        report.lines.iter().any(|l| l.contains("stays absent")),
        "{report:#?}"
    );
    assert_eq!(
        report
            .problems
            .iter()
            .filter(|p| p.contains("journal candidate"))
            .count(),
        2,
        "{report:#?}"
    );
    for root in &fixture.cold {
        let aside = replica::damaged_dir(root, AT).join(".mie/journal/journal.jsonl");
        assert_eq!(bytes_and_mode(&aside), (full.clone(), 0));
    }

    // Hot's own unreadable journal: by the flag as the longest, by U at
    // C's length; hot's journal then comes from C.
    for (i, longer) in [true, false].into_iter().enumerate() {
        let fixture = replicated(&format!("restore-u-skip-hot-{i}"));
        let full = fs::read(journal_copy(&fixture, 0)).unwrap();
        lose_hot(&fixture);
        let mut own = full.clone();
        if longer {
            own.extend_from_slice(b"{\"type\":\"run_end\"}\n");
        }
        fs::write(journal(&fixture), &own).unwrap();
        unreadable(&journal(&fixture));
        let flags = if longer {
            skip_flag()
        } else {
            Flags::default()
        };
        let report = restore_at(&fixture, flags, FIRST_HOT, AT);
        assert!(report.restored, "{i}: {report:#?}");
        let aside = PathBuf::from(format!("{}.unreadable-{AT}", journal(&fixture).display()));
        assert_problem(&report, &["hot's journal", &aside.display().to_string()]);
        assert_eq!(bytes_and_mode(&aside), (own, 0), "{i}");
        assert_eq!(fs::read(journal(&fixture)).unwrap(), full, "{i}");
    }
}

/// H3′ across a crash: a flagged run that crashes at `Step::JournalAside`,
/// or at `Step::Journal`, then a re-run without the flag commits with the
/// same hot journal, ledger records and copy-back as an uncrashed flagged
/// run, with no line for the moved copy.
#[test]
fn u_skip_unreadable_journal_crash_resumes() {
    let probe = replicated("restore-u-skip-crash-probe");
    if reads_mode_000(&probe.dir.0) {
        return;
    }
    let (reference, _) = journal_one_byte_longer("restore-u-skip-crash-reference");
    let report = restore_at(&reference, skip_flag(), FIRST_HOT, AT);
    assert!(report.restored, "{report:#?}");
    let expected = (hot_state(&reference), records_without_time(&reference));
    for step in [Step::JournalAside, Step::Journal] {
        let (fixture, _) = journal_one_byte_longer(&format!("restore-u-skip-crash-{step:?}"));
        AFTER_STEP.with(|crash| crash.set(Some(step)));
        let report = restore_at(&fixture, skip_flag(), FIRST_HOT, AT);
        AFTER_STEP.with(|crash| crash.set(None));
        assert_not_restored(&report, &["simulated crash"]);
        let report = restore_at(&fixture, Flags::default(), FIRST_HOT, AT + 1);
        assert!(report.restored, "{step:?}: {report:#?}");
        assert!(
            !report
                .problems
                .iter()
                .chain(&report.lines)
                .any(|l| l.contains("journal candidate") && l.contains("cannot be read")),
            "{step:?}: {report:#?}"
        );
        assert_eq!(
            (hot_state(&fixture), records_without_time(&fixture)),
            expected,
            "{step:?}"
        );
    }
}
