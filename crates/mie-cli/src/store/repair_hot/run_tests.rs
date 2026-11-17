//! The hot repair end to end on the replication fixture (#217): the
//! refusals, the locks, each journal state, `--keep-hot-journal`, the
//! import ledger, a crash at each step and the re-run, the evidence, the
//! path forms, refused writes, the report and the exit code. The names
//! follow the plan's edge cases.

use super::tests::{checked_lines, line, records};
use super::*;
use crate::store::replicate::tests::{D0, Fixture};
use std::os::unix::fs::PermissionsExt;

/// The run's `at_ms`.
const AT: i64 = D0 + 3 * 3_600_000;
/// The fixture's import entry.
const ENTRY: &str = "aggTrades/BTCUSDT-aggTrades-2026-10-05.zip.import";

/// A fixture whose journal is `bytes`, after one passing pass, so both
/// copies equal it.
fn with_journal(tag: &str, bytes: &[u8]) -> Fixture {
    let fixture = Fixture::new(tag);
    fs::write(&fixture.config.paths.journal, bytes).unwrap();
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    for id in ["cold-a", "cold-b"] {
        assert_eq!(fs::read(copy_of(&fixture, id)).unwrap(), bytes);
    }
    fixture
}

fn copy_of(fixture: &Fixture, id: &str) -> PathBuf {
    fixture.target(id).join(".mie/journal/journal.jsonl")
}

fn run_with(fixture: &Fixture, scope: Scope, flags: Flags, at: i64) -> RepairHotReport {
    run(
        &fixture.config,
        Some(&fixture.archive()),
        scope,
        flags,
        &fixture.probe,
        &|| at,
    )
}

fn journal(fixture: &Fixture, at: i64) -> RepairHotReport {
    run_with(fixture, Scope::Journal, Flags::default(), at)
}

fn keep_hot(fixture: &Fixture, at: i64) -> RepairHotReport {
    run_with(
        fixture,
        Scope::Journal,
        Flags {
            keep_hot_journal: true,
        },
        at,
    )
}

fn ledger(fixture: &Fixture, at: i64) -> RepairHotReport {
    run_with(fixture, Scope::ImportLedger, Flags::default(), at)
}

/// Every file below the fixture with its bytes (an unreadable one empty);
/// lock files left out.
fn snapshot(fixture: &Fixture) -> Vec<(PathBuf, Vec<u8>)> {
    fn walk(dir: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() && !path.is_symlink() {
                walk(&path, out);
            } else if !path.to_string_lossy().ends_with(".lock") {
                out.push((path.clone(), fs::read(&path).unwrap_or_default()));
            }
        }
    }
    let mut out = Vec::new();
    walk(&fixture.dir.0, &mut out);
    out.sort();
    out
}

#[track_caller]
fn assert_pass(report: &RepairHotReport) {
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
}

#[track_caller]
fn assert_problem(report: &RepairHotReport, needles: &[&str]) {
    assert!(
        report
            .problems
            .iter()
            .any(|p| needles.iter().all(|n| p.contains(n))),
        "no problem with {needles:?}: {report:#?}"
    );
}

#[track_caller]
fn assert_line(report: &RepairHotReport, needles: &[&str]) {
    assert!(
        report
            .lines
            .iter()
            .any(|l| needles.iter().all(|n| l.contains(n))),
        "no line with {needles:?}: {report:#?}"
    );
}

#[track_caller]
fn assert_nothing_written(report: &RepairHotReport) {
    assert_eq!(report.written, 0, "{report:#?}");
    assert_eq!(
        report.lines.last().map(String::as_str),
        Some("nothing written"),
        "{report:#?}"
    );
}

/// Flips one bit of byte `at` of `path`.
fn flip(path: &Path, at: usize) {
    let mut bytes = fs::read(path).unwrap();
    bytes[at] ^= 0x01;
    fs::write(path, bytes).unwrap();
}

fn mode(path: &Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

/// The fixture's journal: 1 000 bytes of run `A`'s records.
fn thousand() -> Vec<u8> {
    records("A", 1000)
}

/// Hot's import-ledger file `name`.
fn hot_file(fixture: &Fixture, name: &str) -> PathBuf {
    fixture.dir.0.join("ledger").join(name)
}

fn target_file(fixture: &Fixture, id: &str, name: &str) -> PathBuf {
    fixture.target(id).join(".mie/import-ledger").join(name)
}

/// Overwrites a read-only import-ledger file.
fn overwrite(path: &Path, bytes: &[u8]) {
    mode(path, 0o644);
    fs::write(path, bytes).unwrap();
}

// ------------------------------------------------------------------ X0

/// Every refusal of X0 writes nothing.
#[test]
fn x0_refusals_write_nothing() {
    let fixture = with_journal("hot-x0", &thousand());
    let before = snapshot(&fixture);
    let report = run_with(
        &fixture,
        Scope::ImportLedger,
        Flags {
            keep_hot_journal: true,
        },
        AT,
    );
    assert_problem(
        &report,
        &["--keep-hot-journal applies to --scope journal only"],
    );
    assert_nothing_written(&report);

    let mut single = fixture.config.clone();
    single.paths.cold_roots.clear();
    let report = run(
        &single,
        None,
        Scope::Journal,
        Flags::default(),
        &fixture.probe,
        &|| AT,
    );
    assert_problem(&report, &["cold_roots is empty"]);

    let mut nameless = fixture.config.clone();
    nameless.paths.journal = PathBuf::from("/");
    let report = run(
        &nameless,
        None,
        Scope::Journal,
        Flags::default(),
        &fixture.probe,
        &|| AT,
    );
    assert_problem(&report, &["has no file name"]);

    let report = run(
        &fixture.config,
        None,
        Scope::ImportLedger,
        Flags::default(),
        &fixture.probe,
        &|| AT,
    );
    assert_problem(&report, &["no archive configuration"]);
    assert_eq!(snapshot(&fixture), before);

    // A dangling journal link, a dangling import-ledger link.
    let journal = fixture.config.paths.journal.clone();
    let moved = fixture.dir.0.join("journal.moved");
    fs::rename(&journal, &moved).unwrap();
    std::os::unix::fs::symlink(fixture.dir.0.join("unmounted/journal.jsonl"), &journal).unwrap();
    assert_problem(
        &journal_run(&fixture),
        &["link to a missing path", "Mount it"],
    );
    fs::remove_file(&journal).unwrap();
    fs::rename(&moved, &journal).unwrap();
    let dir = fixture.dir.0.join("ledger");
    let away = fixture.dir.0.join("ledger.away");
    fs::rename(&dir, &away).unwrap();
    std::os::unix::fs::symlink(fixture.dir.0.join("unmounted/ledger"), &dir).unwrap();
    assert_problem(&ledger(&fixture, AT), &["link to a missing path"]);
    fs::remove_file(&dir).unwrap();
    fs::rename(&away, &dir).unwrap();

    // A listing error on hot's import ledger.
    mode(&dir.join("aggTrades"), 0o000);
    let report = ledger(&fixture, AT);
    mode(&dir.join("aggTrades"), 0o755);
    assert_problem(&report, &["cannot be listed"]);
    assert_nothing_written(&report);

    // Hot's copy ledger missing names the rebuild.
    let path = copy_ledger::ledger_path(&fixture.hot);
    let saved = fs::read(&path).unwrap();
    fs::remove_file(&path).unwrap();
    assert_problem(
        &ledger(&fixture, AT),
        &["does not exist", "mie store rebuild"],
    );
    fs::write(&path, saved).unwrap();

    // A hot without its marker names the restore.
    let marker = root_marker::marker_path(&fixture.hot);
    let saved = fs::read(&marker).unwrap();
    fs::remove_file(&marker).unwrap();
    assert_problem(&journal_run(&fixture), &["mie store restore"]);
    fs::write(&marker, saved).unwrap();
    assert_eq!(snapshot(&fixture), before);
}

fn journal_run(fixture: &Fixture) -> RepairHotReport {
    journal(fixture, AT)
}

/// Every lock is taken without waiting: a held live writer lock refuses
/// the journal scope naming `mie ingest`, a held archive lock the
/// import-ledger scope, a held pass or verify lock both.
#[test]
fn x0_locks_are_taken_without_waiting() {
    let fixture = with_journal("hot-locks", &thousand());
    flip(&fixture.config.paths.journal, 10);
    let before = snapshot(&fixture);
    {
        let _live = removal::take_source_lock(&fixture.hot, "binance-um").unwrap();
        let report = journal(&fixture, AT);
        assert_problem(
            &report,
            &["writer lock of source binance-um", "stop `mie ingest`"],
        );
        assert_nothing_written(&report);
    }
    {
        let _archive = removal::take_source_lock(&fixture.hot, ARCHIVE_SOURCE).unwrap();
        assert_problem(
            &ledger(&fixture, AT),
            &["writer lock of source binance-archive", "the importer"],
        );
    }
    {
        let _pass = replica::try_replication_lock(&fixture.hot).unwrap();
        assert_problem(&journal(&fixture, AT), &["the pass lock"]);
        assert_problem(&ledger(&fixture, AT), &["the pass lock"]);
    }
    {
        let _verify = replica::try_verify_lock(&fixture.hot).unwrap();
        assert_problem(&journal(&fixture, AT), &["the verify lock"]);
    }
    assert_eq!(snapshot(&fixture), before);
}

/// The journal lock (review of #226, finding 3): `mie ingest` drops the
/// source's writer lock when its store closes, then still writes `sealed`,
/// `run_end` and the final pass's `replication`. Its open journal holds
/// the journal lock until then, so the journal scope refuses while it is
/// open, and while the lock is held otherwise, with nothing written; the
/// import-ledger scope does not take it. A journal does not open while
/// the lock is held, and opens again once the run ends.
#[test]
fn x0_the_journal_lock_excludes_every_journal_write() {
    let fixture = with_journal("hot-journal-lock", &thousand());
    flip(&fixture.config.paths.journal, 10);
    let before = snapshot(&fixture);
    let live = crate::journal::Journal::open(&fixture.config.paths.journal, "run-A").unwrap();
    let report = journal(&fixture, AT);
    assert_problem(&report, &["the journal lock", "stop `mie ingest`"]);
    assert_nothing_written(&report);
    assert_pass(&ledger(&fixture, AT));
    drop(live);
    {
        let _held = replica::try_journal_lock(&fixture.config.paths.journal).unwrap();
        let report = journal(&fixture, AT);
        assert_problem(&report, &["the journal lock"]);
        assert_nothing_written(&report);
        let refused = crate::journal::Journal::open(&fixture.config.paths.journal, "run-B");
        assert!(
            refused.is_err_and(|e| e.contains("journal lock")),
            "a journal opened under a held lock"
        );
    }
    assert_eq!(snapshot(&fixture), before);
    assert_pass(&journal(&fixture, AT));
    assert_eq!(fs::read(&fixture.config.paths.journal).unwrap(), thousand());
}

/// The import-ledger scope runs beside a live writer.
#[test]
fn x0_import_ledger_scope_runs_beside_a_live_writer() {
    let fixture = with_journal("hot-live", &thousand());
    let _live = removal::take_source_lock(&fixture.hot, "binance-um").unwrap();
    let report = ledger(&fixture, AT);
    assert_pass(&report);
    assert_nothing_written(&report);
}

// ------------------------------------------------------------------ X1

/// A copy target without its marker is skipped, a FAIL, and is no
/// witness: one copy left is at most *behind*, a difference *one-witness*.
#[test]
fn x1_target_without_marker_is_skipped() {
    let fixture = with_journal("hot-skip", &thousand());
    fs::remove_file(root_marker::marker_path(fixture.target("cold-b"))).unwrap();
    let report = journal(&fixture, AT);
    assert_problem(&report, &["cold-b", "has no root marker", "skipped"]);
    assert_line(&report, &["decision agree"]);
    flip(&fixture.config.paths.journal, 10);
    let before = snapshot(&fixture);
    let report = journal(&fixture, AT);
    assert_line(&report, &["decision one-witness"]);
    assert_problem(&report, &["only one copy reaches it"]);
    assert_eq!(snapshot(&fixture), before);
}

/// An unreadable copy is no witness, named with the read-error sentence.
#[test]
fn x1_unreadable_copy_is_no_witness() {
    let fixture = with_journal("hot-unreadable-copy", &thousand());
    mode(&copy_of(&fixture, "cold-b"), 0o000);
    flip(&fixture.config.paths.journal, 10);
    let report = journal(&fixture, AT);
    mode(&copy_of(&fixture, "cold-b"), 0o644);
    assert_problem(
        &report,
        &["cold-b", "cannot be read", "no witness", READ_ERROR],
    );
    assert_line(&report, &["decision one-witness"]);
    assert_nothing_written(&report);
}

// ------------------------------------------------------------------ X2

/// No copy under the configured name: *agree*, nothing written, the other
/// names on the targets listed.
#[test]
fn x2_no_copy_under_the_configured_name() {
    let fixture = with_journal("hot-no-copy", &thousand());
    for id in ["cold-a", "cold-b"] {
        fs::remove_file(copy_of(&fixture, id)).unwrap();
    }
    fs::write(
        fixture.target("cold-a").join(".mie/journal/old.jsonl"),
        b"x\n",
    )
    .unwrap();
    let report = journal(&fixture, AT);
    assert_pass(&report);
    assert_line(
        &report,
        &[
            "no copy target holds a journal copy named journal.jsonl",
            "old.jsonl",
        ],
    );
    assert_line(&report, &["decision agree"]);
    assert_nothing_written(&report);
}

/// Hot's journal absent, copies of 300 bytes: created with those bytes.
#[test]
fn x2_behind_extends_with_one_witness() {
    let copy = records("A", 300);
    let fixture = with_journal("hot-behind", &copy);
    fs::remove_file(&fixture.config.paths.journal).unwrap();
    let report = journal(&fixture, AT);
    assert_pass(&report);
    assert_line(&report, &["decision behind"]);
    assert_eq!(fs::read(&fixture.config.paths.journal).unwrap(), copy);
    assert_eq!(report.written, 1);
    assert_eq!(
        report.lines.last().map(String::as_str),
        Some("repaired: 1 file(s) written")
    );
    // A second run agrees.
    let report = journal(&fixture, AT + 1);
    assert_pass(&report);
    assert_nothing_written(&report);
}

/// A written journal that ends inside a line gets one `\n`: C ending in
/// `}` gives 1 001 bytes, C ending in `\n` 1 000.
#[test]
fn x2_cut_last_line_gets_one_newline() {
    let mut cut = thousand();
    cut.truncate(999);
    cut.push(b'}');
    assert_eq!(cut.len(), 1000);
    let fixture = with_journal("hot-cut", &cut);
    flip(&fixture.config.paths.journal, 10);
    let report = journal(&fixture, AT);
    assert_pass(&report);
    assert_line(&report, &["decision damaged"]);
    let mut expected = cut.clone();
    expected.push(b'\n');
    assert_eq!(fs::read(&fixture.config.paths.journal).unwrap(), expected);

    let whole = thousand();
    let fixture = with_journal("hot-whole", &whole);
    flip(&fixture.config.paths.journal, 10);
    assert_pass(&journal(&fixture, AT));
    assert_eq!(fs::read(&fixture.config.paths.journal).unwrap(), whole);
}

/// The copies-differ stops name what each needs: *copy-damaged* verify,
/// then repair; *two-fault* `--keep-hot-journal` and not the repair, with
/// no configuration line at k = 2.
#[test]
fn x2_copies_differ_stop_texts() {
    let fixture = with_journal("hot-copies", &thousand());
    flip(&copy_of(&fixture, "cold-b"), 300);
    let before = snapshot(&fixture);
    let report = journal(&fixture, AT);
    assert_line(&report, &["decision copy-damaged"]);
    assert_problem(
        &report,
        &[
            "the faults are on the copies",
            "`mie store verify`, then `mie store repair`",
            "cold-b",
        ],
    );
    assert_nothing_written(&report);
    flip(&fixture.config.paths.journal, 700);
    let before_two = snapshot(&fixture);
    let report = journal(&fixture, AT);
    assert_line(&report, &["decision two-fault"]);
    assert_problem(
        &report,
        &["two faults", "--keep-hot-journal", "is not the next step"],
    );
    assert!(
        report
            .problems
            .iter()
            .all(|p| !p.contains("--config lists only")),
        "{report:#?}"
    );
    assert_nothing_written(&report);
    assert_eq!(snapshot(&fixture), before_two);
    let _ = before;
}

// ------------------------------------------------------------------ X3

/// The flag never overrides *agree*, *behind*, *damaged* or *hot-ahead*.
#[test]
fn x3_keep_hot_never_overrides_evidence() {
    let fixture = with_journal("hot-keep-refused", &thousand());
    let check = |fixture: &Fixture, state: &str| {
        let before = snapshot(fixture);
        let report = keep_hot(fixture, AT);
        assert_problem(
            &report,
            &["--keep-hot-journal is refused in the state", state],
        );
        assert_nothing_written(&report);
        assert_eq!(snapshot(fixture), before, "{state}");
    };
    check(&fixture, "agree");
    flip(&fixture.config.paths.journal, 10);
    check(&fixture, "damaged");
    let saved = fs::read(&fixture.config.paths.journal).unwrap();
    fs::remove_file(&fixture.config.paths.journal).unwrap();
    check(&fixture, "behind");
    let mut ahead = saved;
    ahead.extend(records("A", 200));
    fs::write(&fixture.config.paths.journal, &ahead).unwrap();
    check(&fixture, "hot-ahead");
}

/// The flag rebuilds only the copies that are no prefix of hot's journal;
/// the marker is re-read before each rebuild, and a missing one stops the
/// run before that rebuild.
#[test]
fn x3_keep_hot_rebuilds_only_differing_copies() {
    let fixture = with_journal("hot-keep", &thousand());
    flip(&copy_of(&fixture, "cold-b"), 300);
    let a = fs::read(copy_of(&fixture, "cold-a")).unwrap();
    let b = fs::read(copy_of(&fixture, "cold-b")).unwrap();
    let report = keep_hot(&fixture, AT);
    assert_pass(&report);
    assert_eq!(report.written, 1);
    assert_eq!(fs::read(copy_of(&fixture, "cold-a")).unwrap(), a);
    assert_eq!(fs::read(copy_of(&fixture, "cold-b")).unwrap(), &a[..300]);
    let aside =
        replica::damaged_dir(fixture.target("cold-b"), AT).join(".mie/journal/journal.jsonl");
    assert_eq!(fs::read(aside).unwrap(), b);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(fs::read(copy_of(&fixture, "cold-b")).unwrap(), a);

    // A fork: both copies hold line X, hot line Y; cold-b's marker goes
    // before the writes.
    let fixture = forked("hot-keep-marker");
    let marker = root_marker::marker_path(fixture.target("cold-b"));
    let gone = marker.clone();
    BEFORE_WRITES.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || fs::remove_file(gone).unwrap()));
    });
    let b = fs::read(copy_of(&fixture, "cold-b")).unwrap();
    let report = keep_hot(&fixture, AT);
    assert_problem(
        &report,
        &["cold-b", "marker is gone or changed", "not rebuilt"],
    );
    assert_eq!(report.written, 1);
    assert_eq!(fs::read(copy_of(&fixture, "cold-b")).unwrap(), b);
}

/// A store whose copies hold one more line X than the common journal, and
/// hot another, longer line Y: a fork of unequal length.
fn forked(tag: &str) -> Fixture {
    let common = thousand();
    let fixture = with_journal(tag, &common);
    let x = line("A", "", 120);
    for id in ["cold-a", "cold-b"] {
        let mut bytes = common.clone();
        bytes.extend(&x);
        fs::write(copy_of(&fixture, id), bytes).unwrap();
    }
    let mut hot = common;
    hot.extend(line("B", "", 180));
    fs::write(&fixture.config.paths.journal, hot).unwrap();
    fixture
}

// ------------------------------------------------------------------ X4

/// On hot: a rotten entry is replaced from the targets, read-only, its old
/// bytes linked aside; a malformed `.import` name the readers fail closed
/// on is moved aside, the names they skip stay.
#[test]
fn x4_run_writes_hot_and_moves_malformed_names() {
    let fixture = with_journal("hot-x4", &thousand());
    let entry = hot_file(&fixture, ENTRY);
    let good = fs::read(&entry).unwrap();
    overwrite(&entry, b"rotten\n");
    let segment = fixture.dir.0.join("ledger/aggTrades");
    let file = "BTCUSDT-aggTrades-2026-10-05.zip";
    for name in [
        format!("{file}.01.import"),
        format!("{file}.4294967296.import"),
    ] {
        fs::write(segment.join(name), b"x\n").unwrap();
    }
    for name in [format!(".{file}.import"), "notes.txt".to_owned()] {
        fs::write(segment.join(name), b"x\n").unwrap();
    }
    fs::write(segment.join(format!("{file}.4294967295.import")), b"x\n").unwrap();
    let report = ledger(&fixture, AT);
    // The valid name of generation 4294967295 is handled: it does not
    // parse, and no copy target holds it, so it stops the run.
    assert_problem(
        &report,
        &[&format!("{file}.4294967295.import"), "no copy is good"],
    );
    assert_nothing_written(&report);
    fs::remove_file(segment.join(format!("{file}.4294967295.import"))).unwrap();

    let report = ledger(&fixture, AT);
    assert_pass(&report);
    assert_eq!(fs::read(&entry).unwrap(), good);
    assert!(fs::metadata(&entry).unwrap().permissions().readonly());
    let evidence = fixture.dir.0.join(format!("ledger.damaged-{AT}"));
    assert_eq!(fs::read(evidence.join(ENTRY)).unwrap(), b"rotten\n");
    for name in [
        format!("{file}.01.import"),
        format!("{file}.4294967296.import"),
    ] {
        assert!(!segment.join(&name).exists(), "{name}");
        assert_eq!(
            fs::read(evidence.join("aggTrades").join(&name)).unwrap(),
            b"x\n"
        );
    }
    for name in [format!(".{file}.import"), "notes.txt".to_owned()] {
        assert!(segment.join(&name).exists(), "{name}");
    }
    assert_line(
        &report,
        &["\"aggTrades/notes.txt\" on hot", "left in place"],
    );
    assert_eq!(report.written, 3);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
}

/// Superseded entries on the fixture: hot's rotten E0 is restored from a
/// target copy S1 names; with none that S1 names, it stops.
#[test]
fn x4_run_restores_a_superseded_entry() {
    let fixture = with_journal("hot-superseded", &thousand());
    let names = fixture.supersede(true);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let e0 = hot_file(&fixture, &names[0]);
    let good = fs::read(&e0).unwrap();
    for id in ["cold-a", "cold-b"] {
        assert!(target_file(&fixture, id, &names[1]).exists(), "{id}");
    }
    // The pass copies no superseded entry (#210 C3): the targets hold E0
    // only from before; put it there as an earlier pass left it.
    for id in ["cold-a", "cold-b"] {
        let copy = target_file(&fixture, id, &names[0]);
        if !copy.exists() {
            fs::write(&copy, &good).unwrap();
        }
    }
    overwrite(&e0, b"rotten\n");
    let report = ledger(&fixture, AT);
    assert_pass(&report);
    assert_eq!(fs::read(&e0).unwrap(), good);

    // No target copy that S1 names: a stop, nothing written.
    overwrite(&e0, b"rotten\n");
    for id in ["cold-a", "cold-b"] {
        let copy = target_file(&fixture, id, &names[0]);
        let text = String::from_utf8(fs::read(&copy).unwrap()).unwrap();
        overwrite(&copy, text.replace("rows 4", "rows 5").as_bytes());
    }
    let report = ledger(&fixture, AT + 1);
    assert_problem(
        &report,
        &[
            &names[0],
            "no supersede record names",
            "Nothing needs to be recreated",
        ],
    );
    assert_nothing_written(&report);
}

// ------------------------------------------------------------------ X5

/// An existing evidence name stops the run with nothing written; the next
/// millisecond completes it.
#[test]
fn x5_existing_evidence_name_stops() {
    let fixture = with_journal("hot-evidence", &thousand());
    flip(&fixture.config.paths.journal, 10);
    let aside = beside(&fixture.config.paths.journal, AT);
    fs::write(&aside, b"earlier\n").unwrap();
    let before = snapshot(&fixture);
    let report = journal(&fixture, AT);
    assert_problem(&report, &["evidence is never overwritten", "a re-run"]);
    assert_nothing_written(&report);
    assert_eq!(snapshot(&fixture), before);
    let report = journal(&fixture, AT + 1);
    assert_pass(&report);
    assert_line(
        &report,
        &["evidence of an earlier run", &aside.display().to_string()],
    );
    assert_eq!(fs::read(&aside).unwrap(), b"earlier\n");
    assert_eq!(
        fs::read(&fixture.config.paths.journal).unwrap(),
        fs::read(copy_of(&fixture, "cold-a")).unwrap()
    );
}

/// A refused write stops the run, naming it, with nothing written after
/// it: hot's journal directory read-only, then cold-b's journal directory
/// read-only under `--keep-hot-journal`.
#[test]
fn x5_refused_write_stops_before_the_next() {
    let fixture = with_journal("hot-refused", &thousand());
    flip(&fixture.config.paths.journal, 10);
    // The journal's lock file, as the `mie ingest` that wrote it leaves it.
    drop(replica::try_journal_lock(&fixture.config.paths.journal).unwrap());
    let before = snapshot(&fixture);
    mode(&fixture.dir.0, 0o555);
    let report = journal(&fixture, AT);
    mode(&fixture.dir.0, 0o755);
    assert_problem(
        &report,
        &["replace hot's journal", "hot still holds its journal"],
    );
    assert_nothing_written(&report);
    assert_eq!(snapshot(&fixture), before);

    let fixture = forked("hot-refused-keep");
    let dir = fixture.target("cold-b").join(".mie/journal");
    let b = fs::read(dir.join("journal.jsonl")).unwrap();
    mode(&dir, 0o555);
    let report = keep_hot(&fixture, AT);
    mode(&dir, 0o755);
    assert_problem(
        &report,
        &["rebuild the journal copy on cold-b", "the run stops"],
    );
    assert_eq!(report.written, 1);
    assert_eq!(fs::read(dir.join("journal.jsonl")).unwrap(), b);
}

/// A crash after each step leaves hot holding the old or the new file,
/// never none; a re-run, with a new `at_ms`, ends with the bytes of a run
/// without a crash, and names the extra evidence link.
#[test]
fn x5_crash_at_every_step_resumes() {
    let whole = thousand();
    for step in [Step::Linked, Step::Temp, Step::Renamed] {
        let fixture = with_journal(&format!("hot-crash-{step:?}"), &whole);
        let journal_path = fixture.config.paths.journal.clone();
        flip(&journal_path, 10);
        let damaged = fs::read(&journal_path).unwrap();
        AFTER_STEP.with(|crash| crash.set(Some(step)));
        let report = journal(&fixture, AT);
        assert_problem(&report, &["simulated crash"]);
        let now = fs::read(&journal_path).unwrap();
        assert!(now == damaged || now == whole, "{step:?}");
        assert_eq!(
            fs::read(beside(&journal_path, AT)).unwrap(),
            damaged,
            "{step:?}"
        );
        let report = journal(&fixture, AT + 1);
        assert_pass(&report);
        assert_eq!(fs::read(&journal_path).unwrap(), whole, "{step:?}");
        assert_line(
            &report,
            &["evidence of an earlier run", &format!("damaged-{AT}")],
        );
    }

    for step in [
        Step::MalformedMoved,
        Step::Linked,
        Step::Temp,
        Step::Renamed,
    ] {
        let fixture = with_journal(&format!("hot-crash-ledger-{step:?}"), &whole);
        let entry = hot_file(&fixture, ENTRY);
        let good = fs::read(&entry).unwrap();
        overwrite(&entry, b"rotten\n");
        let malformed = fixture
            .dir
            .0
            .join("ledger/aggTrades/BTCUSDT-aggTrades-2026-10-05.zip.01.import");
        fs::write(&malformed, b"x\n").unwrap();
        AFTER_STEP.with(|crash| crash.set(Some(step)));
        assert_problem(&ledger(&fixture, AT), &["simulated crash"]);
        assert!(entry.exists(), "{step:?}");
        let report = ledger(&fixture, AT + 1);
        assert_pass(&report);
        assert_eq!(fs::read(&entry).unwrap(), good, "{step:?}");
        assert!(!malformed.exists(), "{step:?}");
    }

    // *behind*: a crash after the extension, before its closing `\n`,
    // leaves hot equal to C; the re-run writes the `\n` (review of #226,
    // finding 5), and a third run agrees.
    let mut cut = thousand();
    cut.extend_from_slice(b"{\"type\":\"stats\",\"run_id\":\"A\",\"at");
    let fixture = with_journal("hot-crash-behind", &cut);
    let journal_path = fixture.config.paths.journal.clone();
    fs::write(&journal_path, &cut[..500]).unwrap();
    AFTER_STEP.with(|crash| crash.set(Some(Step::Extended)));
    assert_problem(&journal(&fixture, AT), &["simulated crash"]);
    assert_eq!(fs::read(&journal_path).unwrap(), cut);
    let report = journal(&fixture, AT + 1);
    assert_pass(&report);
    assert_line(&report, &["decision behind"]);
    assert_eq!(report.written, 1);
    let mut whole = cut.clone();
    whole.push(b'\n');
    assert_eq!(fs::read(&journal_path).unwrap(), whole);
    let report = journal(&fixture, AT + 2);
    assert_pass(&report);
    assert_nothing_written(&report);

    let fixture = forked("hot-crash-keep");
    let hot = fs::read(&fixture.config.paths.journal).unwrap();
    AFTER_STEP.with(|crash| crash.set(Some(Step::CopyRebuilt)));
    assert_problem(&keep_hot(&fixture, AT), &["simulated crash"]);
    assert_pass(&keep_hot(&fixture, AT + 1));
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    for id in ["cold-a", "cold-b"] {
        assert_eq!(fs::read(copy_of(&fixture, id)).unwrap(), hot, "{id}");
    }
}

/// A path relative to the working directory, as every command reads it.
fn relative(path: &Path) -> PathBuf {
    let cwd = std::env::current_dir().unwrap();
    let mut out = PathBuf::new();
    for _ in cwd.components().skip(1) {
        out.push("..");
    }
    out.join(path.strip_prefix("/").unwrap())
}

/// A `paths.journal` that is a link into another directory: the link's
/// target is replaced, the link stays a link, and the evidence lies beside
/// the target; relative `paths.journal` and `raw_root` resolve as every
/// other command resolves them.
#[test]
fn x5_symlinked_journal_is_replaced_at_its_target() {
    let whole = thousand();
    let mut fixture = with_journal("hot-link", &whole);
    let link = fixture.config.paths.journal.clone();
    let target = fixture.dir.0.join("elsewhere/j.jsonl");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::rename(&link, &target).unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();
    flip(&target, 10);
    let damaged = fs::read(&target).unwrap();
    fixture.config.paths.journal = relative(&link);
    fixture.config.paths.raw_root = relative(&fixture.hot);
    let report = journal(&fixture, AT);
    assert_pass(&report);
    assert!(
        fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(fs::read(&target).unwrap(), whole);
    assert_eq!(fs::read(beside(&target, AT)).unwrap(), damaged);
}

// ------------------------------------------------------------------ X7

/// Writes `bytes` over the journal copy on `id`.
fn set_copy(fixture: &Fixture, id: &str, bytes: &[u8]) {
    let path = copy_of(fixture, id);
    mode(&path, 0o644);
    fs::write(path, bytes).unwrap();
}

/// The plan's geometry with hot longer (ADR-066 D4): hot 1 200 B in six
/// checked lines, cold-a 1 000 B, cold-b 400 B, hot flipped at 450 and at
/// 1 050. *line-damaged*: hot becomes cold-a's 1 000 B followed by hot's
/// bytes past them, the flip at 1 050 included and named, and the former
/// bytes are kept beside the journal.
#[test]
fn x7_hot_longer_keeps_its_tail() {
    let six = checked_lines("A", 6);
    let fixture = with_journal("hot-x7-tail", &six);
    set_copy(&fixture, "cold-a", &six[..1000]);
    set_copy(&fixture, "cold-b", &six[..400]);
    let journal_path = fixture.config.paths.journal.clone();
    flip(&journal_path, 450);
    flip(&journal_path, 1050);
    let damaged = fs::read(&journal_path).unwrap();
    let report = journal(&fixture, AT);
    assert_pass(&report);
    assert_line(&report, &["decision line-damaged"]);
    assert_line(
        &report,
        &[
            "repaired from the copy on cold-a",
            "byte(s) 400",
            "ADR-066 D4",
        ],
    );
    assert_line(
        &report,
        &["hot's line from byte 1000 fails its check", "kept"],
    );
    let mut expected = six[..1000].to_vec();
    expected.extend_from_slice(&damaged[1000..]);
    assert_eq!(fs::read(&journal_path).unwrap(), expected);
    assert_eq!(fs::read(beside(&journal_path, AT)).unwrap(), damaged);
    assert_eq!(report.written, 1);
}

/// Hot 1 000 B flipped at 450 under a C cut 50 bytes into a sixth line:
/// hot becomes C plus one `\n` (H3's), as *damaged* writes it.
#[test]
fn x7_c_cut_after_the_damaged_line() {
    let six = checked_lines("A", 6);
    let fixture = with_journal("hot-x7-cut", &six[..1000]);
    set_copy(&fixture, "cold-a", &six[..1050]);
    set_copy(&fixture, "cold-b", &six[..1050]);
    flip(&fixture.config.paths.journal, 450);
    let report = journal(&fixture, AT);
    assert_pass(&report);
    assert_line(&report, &["decision line-damaged"]);
    let mut expected = six[..1050].to_vec();
    expected.push(b'\n');
    assert_eq!(fs::read(&fixture.config.paths.journal).unwrap(), expected);
}

/// Hot verifies where the copies' line 3 fails its check: *copy-damaged*,
/// naming `mie store verify` then `mie store repair` and the line
/// verdicts; nothing is written.
#[test]
fn x7_copy_line_fails_names_the_repair() {
    let five = checked_lines("A", 5);
    let fixture = with_journal("hot-x7-copy", &five);
    let mut bad = five.clone();
    bad[450] ^= 0x01;
    set_copy(&fixture, "cold-a", &bad);
    set_copy(&fixture, "cold-b", &bad[..400]);
    let before = snapshot(&fixture);
    let report = journal(&fixture, AT);
    assert_line(&report, &["decision copy-damaged"]);
    assert_problem(
        &report,
        &[
            "verifies its check",
            "mie store verify",
            "mie store repair",
            "hot's line from byte 400 verifies, C's line holding byte 450 fails its check",
        ],
    );
    assert_nothing_written(&report);
    assert_eq!(snapshot(&fixture), before);
}

/// A fork found by the checks stops as *diverged*, naming the verifying
/// hot line and C's verdict there, and writes nothing.
#[test]
fn x7_fork_by_checks_names_the_verifying_line() {
    let five = checked_lines("A", 5);
    let mut hot = five[..400].to_vec();
    hot.extend_from_slice(&five[200..400]);
    hot.extend_from_slice(&five[600..]);
    let fixture = with_journal("hot-x7-fork", &five);
    fs::write(&fixture.config.paths.journal, &hot).unwrap();
    let before = snapshot(&fixture);
    let report = journal(&fixture, AT);
    assert_line(&report, &["decision diverged"]);
    assert_problem(
        &report,
        &[
            "hot's line from byte 400 verifies its check",
            "C's line that holds byte",
            "verifies",
            "never overwrites a hot line that verifies",
            "--keep-hot-journal",
        ],
    );
    assert_nothing_written(&report);
    assert_eq!(snapshot(&fixture), before);
}

/// Unchecked lines against one copy: the *one-witness* stop says that the
/// differing lines carry no check, not that the journal has no chain.
#[test]
fn x7_one_witness_names_the_missing_check() {
    let fixture = with_journal("hot-x7-one", &thousand());
    set_copy(&fixture, "cold-b", &thousand()[..400]);
    flip(&fixture.config.paths.journal, 450);
    let report = journal(&fixture, AT);
    assert_line(&report, &["decision one-witness"]);
    assert_problem(
        &report,
        &["the differing lines carry no check", "carries no check"],
    );
    assert!(
        report.problems.iter().all(|p| !p.contains("hash chain")),
        "{report:#?}"
    );
}

/// `paths.journal` as a link: *line-damaged* replaces the link's target,
/// as *damaged* does.
#[test]
fn x7_line_damaged_follows_a_linked_journal() {
    let five = checked_lines("A", 5);
    let mut fixture = with_journal("hot-x7-link", &five);
    set_copy(&fixture, "cold-b", &five[..400]);
    let link = fixture.config.paths.journal.clone();
    let target = fixture.dir.0.join("elsewhere/j.jsonl");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::rename(&link, &target).unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();
    flip(&target, 450);
    let damaged = fs::read(&target).unwrap();
    fixture.config.paths.journal = relative(&link);
    fixture.config.paths.raw_root = relative(&fixture.hot);
    let report = journal(&fixture, AT);
    assert_pass(&report);
    assert_line(&report, &["decision line-damaged"]);
    assert!(
        fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(fs::read(&target).unwrap(), five);
    assert_eq!(fs::read(beside(&target, AT)).unwrap(), damaged);
}

// ------------------------------------------------------------------ X6

/// The command's report: PASS and `true`, or FAIL lines and `false`.
#[test]
fn x6_report_and_exit() {
    let fixture = with_journal("hot-report", &thousand());
    let mut out = Vec::new();
    let pass = crate::store::repair_hot(
        &fixture.config,
        Some(&fixture.archive()),
        Scope::Journal,
        Flags::default(),
        &fixture.probe,
        &|| AT,
        &mut out,
    )
    .unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(pass, "{text}");
    assert!(text.contains("next: `mie store verify`"), "{text}");
    assert!(text.ends_with("nothing written\nPASS\n"), "{text}");
    flip(&fixture.config.paths.journal, 10);
    flip(&copy_of(&fixture, "cold-b"), 10);
    let mut out = Vec::new();
    let pass = crate::store::repair_hot(
        &fixture.config,
        Some(&fixture.archive()),
        Scope::Journal,
        Flags::default(),
        &fixture.probe,
        &|| AT,
        &mut out,
    )
    .unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(!pass, "{text}");
    assert!(text.lines().any(|l| l.starts_with("FAIL: ")), "{text}");
    assert!(!text.contains("PASS"), "{text}");
}

/// ADR-066 D8 in the journal scope: hot's line 3 fails its check (a flip
/// at 450), cold-a's line 3 fails differently (460), cold-b holds hot's
/// bytes. With the line recorded lost, R7 rebuilds cold-a, and the journal
/// scope reads the same record: `r7_refuses` agrees, the state is
/// *copy-damaged* naming `mie store repair`, and the repair then makes
/// cold-a hot's bytes. Without the record, or without a readable copy
/// ledger (named in the report), R7 refuses and the state is *two-fault*.
#[test]
fn x7_journal_scope_uses_the_same_records_as_r7() {
    let good = checked_lines("A", 5);
    let mut hot = good.clone();
    hot[450] ^= 0x01;
    let mut cold_a = good.clone();
    cold_a[460] ^= 0x01;
    let fixture = with_journal("x7-records", &good);
    fs::write(&fixture.config.paths.journal, &hot).unwrap();
    fs::write(copy_of(&fixture, "cold-a"), &cold_a).unwrap();
    fs::write(copy_of(&fixture, "cold-b"), &hot).unwrap();
    let report = journal(&fixture, AT);
    assert_line(&report, &["decision two-fault"]);
    assert_nothing_written(&report);

    copy_ledger::CopyLedger::at(&fixture.hot)
        .append(&copy_ledger::Record {
            at_ms: D0,
            kind: copy_ledger::Kind::LostLine {
                journal: "journal.jsonl".to_owned(),
                line: 3,
                offset: 400,
                sha256: copy_ledger::line_sha256(&hot[400..599]),
            },
        })
        .unwrap();
    let report = journal(&fixture, AT);
    assert_line(&report, &["lost-line records of hot's journal", ": 1"]);
    assert_line(&report, &["decision copy-damaged"]);
    assert_problem(
        &report,
        &["`mie store verify`, then `mie store repair`", "cold-a"],
    );
    assert_nothing_written(&report);

    // The copy ledger out of reach: no record is used, and the report
    // says so.
    let ledger = copy_ledger::ledger_path(&fixture.hot);
    let away = ledger.with_extension("away");
    fs::rename(&ledger, &away).unwrap();
    let report = journal(&fixture, AT);
    fs::rename(&away, &ledger).unwrap();
    assert_line(&report, &["cannot be read", "no lost-line record is used"]);
    assert_line(&report, &["decision two-fault"]);

    // R7 agrees: the verification finds cold-a, the repair rebuilds it.
    let ledger_append = copy_ledger::CopyLedger::at(&fixture.hot);
    let verified = crate::store::verify::run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| D0 + 3_600_000,
        &ledger_append,
    );
    assert!(
        verified.problems.iter().any(|p| p.contains("(cold-a)")),
        "{verified:#?}"
    );
    let repaired = crate::store::repair::run(
        &fixture.config,
        Some(&fixture.archive()),
        crate::store::repair::Flags::default(),
        &fixture.probe,
        &|| D0 + 2 * 3_600_000,
        &ledger_append,
    );
    assert_eq!(repaired.problems, Vec::<String>::new(), "{repaired:#?}");
    assert_eq!(fs::read(copy_of(&fixture, "cold-a")).unwrap(), hot);
    let report = journal(&fixture, AT + 1);
    assert_line(&report, &["decision agree"]);
}
