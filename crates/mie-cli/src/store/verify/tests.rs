//! AW11's deterministic interleaving (#192): a replication pass between
//! the verification's ledger snapshot and its first append.

use super::*;
use crate::store::replicate::tests::{D0, Fixture, Probe, Seen};
use mie_adapter_parquet::copy_ledger::CopyLedger;
use std::time::SystemTime;

/// The wall clock, so a part sealed now is within G of it.
fn wall_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// AW11, deterministic: the hook runs a replication pass after S is read
/// and before verify appends anything; the pass copies a newly sealed part
/// and appends `copy` and `ledger-copy` records. The chain then verifies
/// on hot and on every copy, every copy is a byte prefix of hot, and the
/// verification passes, with its records after the pass's.
#[test]
fn aw11_a_pass_between_the_snapshot_and_the_first_append_keeps_every_chain_valid() {
    let fixture = Fixture::new("verify-aw11");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-um", "aggTrade", D0 + 3_600_000, 2);
    assert!(!new.is_empty());
    let (config, archive, hot) = (
        fixture.config.clone(),
        fixture.archive(),
        fixture.hot.clone(),
    );
    let devices = fixture.probe.0.clone();
    AFTER_SNAPSHOT.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let report = replicate::run(
                &config,
                Some(&archive),
                &Probe(devices),
                &|| D0 + 2,
                &CopyLedger::at(&hot),
                &mut Seen::default(),
            );
            assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
        }));
    });
    let now = wall_now();
    let report = run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| now,
        &CopyLedger::at(&fixture.hot),
    );
    assert!(
        AFTER_SNAPSHOT.with(|hook| hook.borrow().is_none()),
        "the hook ran"
    );
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");

    let hot_bytes = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    assert_eq!(copy_ledger::check_chain(&hot_bytes).failure, None);
    for root in &fixture.cold {
        let copy = fs::read(copy_ledger::ledger_path(root)).unwrap();
        assert_eq!(copy_ledger::check_chain(&copy).failure, None);
        assert!(hot_bytes.starts_with(&copy));
    }
    let records = fixture.records();
    let last_copy = records
        .iter()
        .rposition(|r| matches!(&r.kind, Kind::Copy { path, .. } if new.contains(path)))
        .expect("the hook's pass recorded the new part");
    let first_verify = records
        .iter()
        .position(|r| matches!(r.kind, Kind::Verify { .. }))
        .unwrap();
    assert!(last_copy < first_verify);
    let verify_records: Vec<&Record> = records
        .iter()
        .filter(|r| matches!(r.kind, Kind::Verify { .. }))
        .collect();
    assert_eq!(verify_records.len(), 2);

    // A later verification sees the new part's records and passes too.
    let report = run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| now + 1,
        &CopyLedger::at(&fixture.hot),
    );
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
}

/// Sets `path`'s mtime to `ms` since the epoch, making it writable first.
pub(crate) fn set_mtime(path: &Path, ms: i64) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
    let time = UNIX_EPOCH + std::time::Duration::from_millis(ms as u64);
    fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(time)
        .unwrap();
}

/// Flips one byte of `path`, making it writable first.
pub(crate) fn flip_byte(path: &Path, offset: usize) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
    let mut bytes = fs::read(path).unwrap();
    bytes[offset] ^= 0x01;
    fs::write(path, bytes).unwrap();
}

/// #210 C4, with G = 600 000 ms: a supersede record whose mtime on hot is
/// now − 599 999 is not checked; now − 600 000 and now + 1 are; a flipped
/// byte is `entry`, an absent copy `missing`. A superseded E0 that differs
/// on a target gets no finding, and an earlier open finding about it
/// closes.
#[test]
fn verify_checks_supersede_records_and_skips_superseded_entries() {
    let fixture = Fixture::new("verify-supersede");
    let [e0, s1, _] = fixture.supersede(true);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let hot_s1 = fixture.dir.0.join("ledger").join(&s1);
    let cold_a = |name: &str| fixture.cold[0].join(".mie/import-ledger").join(name);
    flip_byte(&cold_a(&s1), 3);
    // A superseded E0 that differs on cold-a, with an open finding.
    let e0_copy = cold_a(&e0);
    fs::write(&e0_copy, b"another E0\n").unwrap();
    let now = D0 + 10 * 3_600_000;
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: now - 1,
            kind: Kind::Finding {
                root: "cold-a".to_owned(),
                code: FindingCode::Entry,
                subject: Subject::Entry(e0.clone()),
            },
        })
        .unwrap();
    let verify = |now: i64| {
        run(
            &fixture.config,
            Some(&fixture.archive()),
            "24",
            &fixture.probe,
            &|| now,
            &CopyLedger::at(&fixture.hot),
        )
    };
    let about = |report: &VerifyReport, name: &str, what: &str| {
        report
            .problems
            .iter()
            .any(|p| p.contains(name) && p.contains(what))
    };
    set_mtime(&hot_s1, now - 599_999);
    let report = verify(now);
    assert!(!about(&report, &s1, "import entry"), "{report:?}");
    assert!(
        !report.problems.iter().any(|p| p.contains(&e0)),
        "{report:?}"
    );
    let ledger = copy_ledger::read(&fixture.hot).unwrap().unwrap();
    let state = CopyState::fold(&ledger).unwrap();
    assert!(state.open_entry_findings("cold-a").is_empty());
    for (at, offset) in [(now + 1, -600_000), (now + 2, 1)] {
        set_mtime(&hot_s1, at + offset);
        let report = verify(at);
        assert!(about(&report, &s1, "entry: the copy differs"), "{report:?}");
    }
    fs::remove_file(cold_a(&s1)).unwrap();
    set_mtime(&hot_s1, now - 600_000);
    let report = verify(now + 3);
    assert!(about(&report, &s1, "missing"), "{report:?}");
}

/// The first live path of `fixture` and hot's manifest text of it with one
/// row more: a self-consistent other file's text over the same data.
fn live_and_other(fixture: &Fixture) -> (String, String) {
    let path = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    let text = fs::read_to_string(fixture.hot.join(manifest_name(&path))).unwrap();
    let rows = text
        .lines()
        .find_map(|line| line.strip_prefix("rows "))
        .unwrap()
        .to_owned();
    let other = text.replace(
        &format!("\nrows {rows}\n"),
        &format!("\nrows {}\n", rows.parse::<u64>().unwrap() + 1),
    );
    (path, other)
}

fn manifest_name(path: &str) -> String {
    format!("{}.manifest", path.strip_suffix(".parquet").unwrap())
}

fn verify_at(fixture: &Fixture, now: i64) -> VerifyReport {
    run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| now,
        &CopyLedger::at(&fixture.hot),
    )
}

/// ADR-067 D6: a `displaced` record of a copy target whose manifest of the
/// path is no longer the record's text is a note naming the path, rows,
/// SHA-256 and directory, and the verification passes.
#[test]
fn v4_a_displaced_record_is_a_note() {
    let fixture = Fixture::new("verify-displaced-note");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let (path, other) = live_and_other(&fixture);
    let parsed = replica::parse_manifest(&other).unwrap();
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + 10,
            kind: Kind::Displaced {
                root: "cold-b".to_owned(),
                dir: D0 + 10,
                text: other,
            },
        })
        .unwrap();
    let report = verify_at(&fixture, D0 + 3_600_000);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let dir = replica::damaged_dir(&fixture.cold[1], D0 + 10);
    assert!(
        report.lines.iter().any(|l| l.contains(&path)
            && l.contains("cold-b")
            && l.contains(&format!("rows {}", parsed.rows))
            && l.contains(&parsed.sha256)
            && l.contains(&dir.display().to_string())),
        "{report:?}"
    );
}

/// ADR-067 D6: while the target's manifest of the path still equals the
/// record's text (a move that crashed after its record), the note is a
/// `FAIL` naming `mie store repair`.
#[test]
fn v4_an_incomplete_move_fails_naming_the_repair() {
    let fixture = Fixture::new("verify-displaced-incomplete");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let (path, other) = live_and_other(&fixture);
    let manifest = fixture.cold[1].join(manifest_name(&path));
    let mut permissions = fs::metadata(&manifest).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&manifest, permissions).unwrap();
    fs::write(&manifest, &other).unwrap();
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + 10,
            kind: Kind::Displaced {
                root: "cold-b".to_owned(),
                dir: D0 + 10,
                text: other,
            },
        })
        .unwrap();
    let report = verify_at(&fixture, D0 + 3_600_000);
    assert!(
        report.problems.iter().any(|p| p.contains(&path)
            && p.contains("cold-b")
            && p.contains("did not complete")
            && p.contains("`mie store repair`")),
        "{report:?}"
    );
}

/// Makes the manifest of `path` on `root` writable and writes `text` to it.
fn overwrite_manifest(root: &Path, path: &str, text: &str) {
    let manifest = root.join(manifest_name(path));
    let mut permissions = fs::metadata(&manifest).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&manifest, permissions).unwrap();
    fs::write(&manifest, text).unwrap();
}

/// Appends the `displaced` record of `text` on cold-b, directory `D0 + 10`.
fn record_displaced(fixture: &Fixture, text: &str) {
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + 10,
            kind: Kind::Displaced {
                root: "cold-b".to_owned(),
                dir: D0 + 10,
                text: text.to_owned(),
            },
        })
        .unwrap();
}

/// ADR-067 D6: when the damaged directory holds the record's manifest, the
/// note says the move put the file there.
#[test]
fn v4_a_completed_move_is_a_note_naming_the_damaged_directory() {
    let fixture = Fixture::new("verify-displaced-moved");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let (path, other) = live_and_other(&fixture);
    record_displaced(&fixture, &other);
    let dir = replica::damaged_dir(&fixture.cold[1], D0 + 10);
    let aside = dir.join(manifest_name(&path));
    fs::create_dir_all(aside.parent().unwrap()).unwrap();
    fs::write(&aside, &other).unwrap();
    let report = verify_at(&fixture, D0 + 3_600_000);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert!(
        report.lines.iter().any(|l| l.contains(&path)
            && l.contains("cold-b")
            && l.contains("`mie store repair --move-other-files` moved")
            && l.contains(&format!("into {}", dir.display()))),
        "{report:?}"
    );
}

/// ADR-067 D6 (#234 review, round 2): the recorded move never happened,
/// and a later replace moved the target's files of the path into the
/// root's quarantine. The note names the quarantine and never says the
/// file was moved into the damaged directory, which does not hold it.
#[test]
fn v4_a_displaced_file_a_replace_moved_is_named_in_its_quarantine() {
    let fixture = Fixture::new("verify-displaced-replaced");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let (path, other) = live_and_other(&fixture);
    let parsed = replica::parse_manifest(&other).unwrap();
    let target = &fixture.cold[1];
    overwrite_manifest(target, &path, &other);
    record_displaced(&fixture, &other);
    // The replace: the target's files of the path move into its
    // quarantine, and the ledger records `quarantined` and `replaced`.
    let id = "q-1000";
    let quarantine = replica::quarantine_dir(target, id);
    for name in [path.clone(), manifest_name(&path)] {
        let to = quarantine.join(&name);
        fs::create_dir_all(to.parent().unwrap()).unwrap();
        fs::rename(target.join(&name), &to).unwrap();
    }
    for kind in [
        Kind::Quarantined {
            path: path.clone(),
            quarantine: id.to_owned(),
        },
        Kind::Replaced {
            path: path.clone(),
            entry: "aggTrades/BTCUSDT-aggTrades-2026-10-01.zip.1.import".to_owned(),
        },
    ] {
        CopyLedger::at(&fixture.hot)
            .append(&Record {
                at_ms: D0 + 20,
                kind,
            })
            .unwrap();
    }
    let report = verify_at(&fixture, D0 + 3_600_000);
    let notes: Vec<&String> = report
        .lines
        .iter()
        .filter(|l| l.contains(&path) && l.contains("cold-b") && l.contains(&parsed.sha256))
        .collect();
    assert!(!notes.is_empty(), "{report:?}");
    assert!(
        notes
            .iter()
            .all(|l| !l.contains("`mie store repair --move-other-files` moved")),
        "{notes:?}"
    );
    assert!(
        notes.iter().any(|l| l.starts_with("note: ")
            && l.contains("replace")
            && l.contains(&quarantine.display().to_string())),
        "{notes:?}"
    );
    assert!(
        report.problems.iter().all(|p| !p.contains(&parsed.sha256)),
        "{report:?}"
    );
}

/// ADR-067 D6: neither the path nor the damaged directory holds the
/// record's manifest: the note says so and names no place it did not
/// check.
#[test]
fn v4_a_displaced_file_found_nowhere_names_no_location() {
    let fixture = Fixture::new("verify-displaced-nowhere");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let (path, other) = live_and_other(&fixture);
    record_displaced(&fixture, &other);
    let report = verify_at(&fixture, D0 + 3_600_000);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let dir = replica::damaged_dir(&fixture.cold[1], D0 + 10);
    assert!(
        report.lines.iter().any(|l| l.starts_with("note: ")
            && l.contains(&path)
            && l.contains("no longer under the path")
            && l.contains(&format!("{} does not hold", dir.display()))
            && !l.contains("moved")
            && !l.contains("deleted")),
        "{report:?}"
    );
}
