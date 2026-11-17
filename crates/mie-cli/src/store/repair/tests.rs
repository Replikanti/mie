//! The repair under its crash hooks (#192): a crash at every repair step,
//! then a re-run, ends where an uninterrupted run ends.

use super::*;
use crate::store::replicate::tests::{D0, Fixture};
use crate::store::verify;
use mie_adapter_parquet::copy_ledger::CopyLedger;
use std::os::unix::fs::PermissionsExt;

const HOUR: i64 = 3_600_000;

fn flip(path: &Path, offset: usize) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
    let mut bytes = fs::read(path).unwrap();
    bytes[offset] ^= 0x01;
    fs::write(path, bytes).unwrap();
}

/// A replicated fixture with a damaged data copy, ledger copy and journal
/// copy on cold-a, a damaged hot archive part and a self-consistent other
/// file under a live path on cold-b, found by a verification.
fn damaged(tag: &str) -> Fixture {
    let fixture = Fixture::new(tag);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let sealed = replica::list_sealed(&fixture.hot).unwrap();
    let live = sealed
        .iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    flip(&fixture.cold[0].join(live), 30);
    let archive_part = sealed
        .iter()
        .find(|p| p.starts_with("source=binance-archive"))
        .unwrap();
    flip(&fixture.hot.join(archive_part), 30);
    let ledger = copy_ledger::ledger_path(&fixture.cold[0]);
    let bytes = fs::read(&ledger).unwrap();
    let mut start = copy_ledger::HEADER.len();
    start += bytes[start..].iter().position(|&b| b == b'\n').unwrap() + 1;
    flip(&ledger, start + 2);
    flip(&fixture.cold[0].join(".mie/journal/journal.jsonl"), 5);
    // A self-consistent other file under the live path on cold-b (ADR-067).
    plant_other_file(&fixture.cold[1], live, &other_text(&fixture.hot, live));
    let report = verify::run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| D0 + HOUR,
        &CopyLedger::at(&fixture.hot),
    );
    assert!(report.problems.len() >= 3, "{report:?}");
    fixture
}

fn repair(fixture: &Fixture, now: i64) -> RepairReport {
    run(
        &fixture.config,
        Some(&fixture.archive()),
        Flags::default(),
        &fixture.probe,
        &|| now,
        &CopyLedger::at(&fixture.hot),
    )
}

/// [`repair`] with `--move-other-files` (ADR-067).
fn repair_moving(fixture: &Fixture, now: i64) -> RepairReport {
    run(
        &fixture.config,
        Some(&fixture.archive()),
        moving(),
        &fixture.probe,
        &|| now,
        &CopyLedger::at(&fixture.hot),
    )
}

fn moving() -> Flags {
    Flags {
        move_other_files: true,
        ..Flags::default()
    }
}

/// `root`'s manifest text of `path` with one row more: a self-consistent
/// other file under the path when written beside the same data.
fn other_text(root: &Path, path: &str) -> String {
    let text = fs::read_to_string(root.join(manifest_of(path))).unwrap();
    let rows = text
        .lines()
        .find_map(|line| line.strip_prefix("rows "))
        .unwrap()
        .to_owned();
    text.replace(
        &format!("\nrows {rows}\n"),
        &format!("\nrows {}\n", rows.parse::<u64>().unwrap() + 1),
    )
}

/// Writes `text` as the manifest of `path` on `root`, beside the data
/// there (copied from the root's own data, else left as it is).
fn plant_other_file(root: &Path, path: &str, text: &str) {
    let manifest = root.join(manifest_of(path));
    if manifest.exists() {
        fs::set_permissions(&manifest, fs::Permissions::from_mode(0o644)).unwrap();
    }
    fs::create_dir_all(manifest.parent().unwrap()).unwrap();
    fs::write(&manifest, text).unwrap();
}

/// The `displaced` records of hot's ledger, as `(root, dir, text)`.
fn displaced(fixture: &Fixture) -> Vec<(String, i64, String)> {
    fixture
        .records()
        .into_iter()
        .filter_map(|r| match r.kind {
            Kind::Displaced { root, dir, text } => Some((root, dir, text)),
            _ => None,
        })
        .collect()
}

/// Every file below `root` with its bytes, by relative path; the copy
/// ledger (whose records differ by the crashed run's own), the damaged
/// copies and the lock files left out.
fn tree(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(dir: &Path, base: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            let rel = path.strip_prefix(base).unwrap().display().to_string();
            if rel == ".mie/damaged" || rel == ".mie/copy-ledger" || rel.ends_with(".lock") {
                continue;
            }
            if path.is_dir() {
                walk(&path, base, out);
            } else {
                out.push((rel, fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

fn trees(fixture: &Fixture) -> Vec<Vec<(String, Vec<u8>)>> {
    std::iter::once(&fixture.hot)
        .chain(&fixture.cold)
        .map(|root| tree(root))
        .collect()
}

/// A crash at every repair step, then a re-run, leaves every root as an
/// uninterrupted run does, every ledger copy a valid prefix of hot's, and
/// a verification that passes.
///
/// The repairs run with `--move-other-files`: after a crash between the
/// `displaced` record and its move, the re-run completes the move into the
/// record's directory, the crashed run's, and appends no second record.
#[test]
fn a_crash_at_every_repair_step_is_completed_by_the_next_run() {
    let reference = damaged("repair-crash-reference");
    let report = repair_moving(&reference, D0 + 2 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let expected = trees(&reference);
    for step in RepairStep::ALL {
        let fixture = damaged(&format!("repair-crash-{step:?}"));
        CRASH_AT.with(|crash| crash.set(Some(step)));
        let crashed = repair_moving(&fixture, D0 + 2 * HOUR);
        assert!(
            CRASH_AT.with(std::cell::Cell::get).is_none(),
            "{step:?} never reached"
        );
        assert!(
            crashed
                .problems
                .iter()
                .any(|p| p.contains("simulated crash")),
            "{step:?}: {crashed:?}"
        );
        let report = repair_moving(&fixture, D0 + 3 * HOUR);
        assert_eq!(
            report.problems,
            Vec::<String>::new(),
            "{step:?}: {report:?}"
        );
        assert_eq!(trees(&fixture), expected, "{step:?}");
        // The other file sits in exactly one run's directory, the one its
        // only record names.
        let records = displaced(&fixture);
        assert_eq!(records.len(), 1, "{step:?}");
        let (root, dir, text) = &records[0];
        assert_eq!(root, "cold-b", "{step:?}");
        let live = replica::list_sealed(&fixture.hot)
            .unwrap()
            .into_iter()
            .find(|p| p.starts_with("source=binance-um"))
            .unwrap();
        let moved = |at: i64| replica::damaged_dir(&fixture.cold[1], at).join(manifest_of(&live));
        assert_eq!(fs::read_to_string(moved(*dir)).unwrap(), *text, "{step:?}");
        let other_run = if *dir == D0 + 2 * HOUR { 3 } else { 2 };
        assert!(!moved(D0 + other_run * HOUR).exists(), "{step:?}");
        if step == RepairStep::AfterDisplacedRecord {
            assert_eq!(*dir, D0 + 2 * HOUR, "{step:?}");
        }
        let hot = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
        for root in &fixture.cold {
            let copy = fs::read(copy_ledger::ledger_path(root)).unwrap();
            assert_eq!(copy_ledger::check_chain(&copy).failure, None, "{step:?}");
            assert!(hot.starts_with(&copy), "{step:?}");
        }
        let verified = verify::run(
            &fixture.config,
            Some(&fixture.archive()),
            "24",
            &fixture.probe,
            &|| D0 + 4 * HOUR,
            &CopyLedger::at(&fixture.hot),
        );
        assert_eq!(verified.problems, Vec::<String>::new(), "{step:?}");
    }
}

/// The fixture's first live file, after its partition left hot: recorded
/// removed and deleted from hot, as the hot removal leaves it.
fn removed_live_file(fixture: &Fixture) -> String {
    let live = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    let partition = partition_of(&live).to_owned();
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + 2,
            kind: Kind::Removed {
                partition: partition.clone(),
            },
        })
        .unwrap();
    fs::remove_dir_all(fixture.hot.join(&partition)).unwrap();
    live
}

/// Review of #203, finding 2 (R9), the unmount: cold-b presents its marker
/// when the run starts and holds the only good copy of a file that left
/// hot, then is unmounted mid-run (its mount point an empty directory, so
/// the copy checks as missing). R9 re-reads the marker of every root that
/// records a copy before it calls a file lost, so the file is "not
/// repairable in this run", and `--record-lost` appends nothing.
#[test]
fn a_target_unmounted_mid_run_makes_a_file_not_repairable_never_lost() {
    let fixture = Fixture::new("repair-unmounted");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let live = removed_live_file(&fixture);
    flip(&fixture.cold[0].join(&live), 30);
    let found = verify::run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| D0 + HOUR,
        &CopyLedger::at(&fixture.hot),
    );
    assert!(!found.problems.is_empty(), "{found:?}");
    let cold_b = fixture.cold[1].clone();
    let hidden = fixture.dir.0.join("cold-b.mounted");
    {
        let (cold_b, hidden) = (cold_b.clone(), hidden.clone());
        BEFORE_WORK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                fs::rename(&cold_b, &hidden).unwrap();
                fs::create_dir(&cold_b).unwrap();
            }));
        });
    }
    let report = run(
        &fixture.config,
        Some(&fixture.archive()),
        Flags {
            record_lost: true,
            ..Flags::default()
        },
        &fixture.probe,
        &|| D0 + 2 * HOUR,
        &CopyLedger::at(&fixture.hot),
    );
    fs::remove_dir(&cold_b).unwrap();
    fs::rename(&hidden, &cold_b).unwrap();
    assert!(
        report.problems.iter().any(|p| p.contains(&live)
            && p.contains("not repairable in this run")
            && p.contains("cold-b")),
        "{report:?}"
    );
    assert!(
        !report.problems.iter().any(|p| p.contains("is lost")),
        "{report:?}"
    );
    assert!(
        !fixture
            .records()
            .iter()
            .any(|r| matches!(r.kind, Kind::Lost { .. })),
        "a file whose good copy sits on an unmounted target was recorded lost"
    );
}

/// Review of #203, finding 4 (R4): the hot removal records a partition
/// removed and deletes it after the repair read the ledger and before R4
/// lists hot. R4 re-reads the ledger under the source's writer lock and
/// skips the partition: nothing is re-created on hot in a partition the
/// ledger records removed.
#[test]
fn a_partition_removed_during_the_run_is_not_recreated_on_hot() {
    let fixture = Fixture::new("repair-removed-mid-run");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let live = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    let partition = partition_of(&live).to_owned();
    {
        let (hot, partition) = (fixture.hot.clone(), partition.clone());
        BEFORE_WORK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                // The remover's order: the record, then the deletion.
                CopyLedger::at(&hot)
                    .append(&Record {
                        at_ms: D0 + 2,
                        kind: Kind::Removed {
                            partition: partition.clone(),
                        },
                    })
                    .unwrap();
                fs::remove_dir_all(hot.join(&partition)).unwrap();
            }));
        });
    }
    let report = repair(&fixture, D0 + 2 * HOUR);
    assert!(
        !fixture.hot.join(&partition).exists(),
        "R4 re-created a partition the ledger records removed: {report:?}"
    );
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.contains(&live) && l.contains("recorded removed")),
        "{report:?}"
    );
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
}

/// #210 C5: R8 replaces a damaged supersede-record copy from hot, moving
/// the damaged copy to `damaged/`; a finding on a superseded E0 is one
/// line and no problem.
#[test]
fn repair_handles_supersede_records_and_superseded_entries() {
    use crate::store::verify::tests::{flip_byte, set_mtime};
    let fixture = Fixture::new("repair-supersede");
    let [e0, s1, _] = fixture.supersede(true);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let hot_s1 = fixture.dir.0.join("ledger").join(&s1);
    let cold_a = |name: &str| fixture.cold[0].join(".mie/import-ledger").join(name);
    flip_byte(&cold_a(&s1), 3);
    set_mtime(&hot_s1, D0);
    let report = verify::run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| D0 + HOUR,
        &CopyLedger::at(&fixture.hot),
    );
    assert!(
        report.problems.iter().any(|p| p.contains(&s1)),
        "{report:?}"
    );
    // A finding on the superseded E0, after that verification.
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + HOUR + 1,
            kind: Kind::Finding {
                root: "cold-a".to_owned(),
                code: copy_ledger::FindingCode::Entry,
                subject: copy_ledger::Subject::Entry(e0.clone()),
            },
        })
        .unwrap();
    let damaged = fs::read(cold_a(&s1)).unwrap();
    let report = repair(&fixture, D0 + 2 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert_eq!(fs::read(cold_a(&s1)).unwrap(), fs::read(&hot_s1).unwrap());
    let aside = replica::damaged_dir(&fixture.cold[0], D0 + 2 * HOUR)
        .join(".mie/import-ledger")
        .join(&s1);
    assert_eq!(fs::read(aside).unwrap(), damaged);
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.contains(&e0) && l.contains("superseded by") && l.contains(&s1)),
        "{report:?}"
    );
}

// ------------------------------------------------ R2b (#219, case 1)

use crate::store::replicate::tests::{counts, manifest_of};
use mie_adapter_parquet::copy_ledger::HoldReason;
use mie_adapter_parquet::replica::TempName;

/// What a test plants under a name: bytes, a dangling symlink, a
/// directory, or bytes at mode 000.
#[derive(Clone)]
enum Plant {
    Bytes(Vec<u8>),
    Dangling,
    Dir,
    Unreadable(Vec<u8>),
}

fn plant(root: &Path, name: &str, what: &Plant) {
    let file = root.join(name);
    fs::create_dir_all(file.parent().unwrap()).unwrap();
    match what {
        Plant::Bytes(bytes) => fs::write(&file, bytes).unwrap(),
        Plant::Dangling => std::os::unix::fs::symlink("/nonexistent/mie-r2b", &file).unwrap(),
        Plant::Dir => fs::create_dir_all(&file).unwrap(),
        Plant::Unreadable(bytes) => {
            fs::write(&file, bytes).unwrap();
            fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).unwrap();
        }
    }
}

/// The relative name of a temp name of `path`.
fn temp_of(path: &str, name: TempName) -> String {
    let base = Path::new("/r");
    replica::temp_path(base, path, name)
        .unwrap()
        .strip_prefix(base)
        .unwrap()
        .display()
        .to_string()
}

/// The names a form plants, with what each holds.
type Planted = Vec<(String, Plant)>;

/// The judgment classes the differential test expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Absent,
    Accepted,
    OtherFile,
    Refused,
    Unreadable,
    Stuck,
}

/// The differential test of R2b's judgment (`r2b_form`: the restore's
/// H6 k1 test `restore::unrecorded_copy`, then the temp names the pass
/// cannot remove) against the closing pass's D5 copy from hot: every form
/// is planted on two twin directories, judged on one and copied onto the
/// other. Absent or accepted ⇔ the copy succeeds; refused ⇔ `Integrity`;
/// a self-consistent other file ⇒ `Integrity`; a judgment error ⇔ an I/O
/// error of the copy; a temp name the pass cannot remove beside an absent
/// or accepted form ⇔ an I/O error of the copy. Look-alike names beside
/// the temp names stay byte-identical: R2b derives every name from hot's
/// listing.
#[test]
fn r2b_moves_exactly_what_the_pass_refuses() {
    let fixture = Fixture::new("r2b-differential");
    let sealed = replica::list_sealed(&fixture.hot).unwrap();
    let path = sealed
        .iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap()
        .clone();
    let archive_part = sealed
        .iter()
        .find(|p| p.starts_with("source=binance-archive"))
        .unwrap()
        .clone();
    let data = fs::read(fixture.hot.join(&path)).unwrap();
    let text = fs::read_to_string(fixture.hot.join(manifest_of(&path))).unwrap();
    let elsewhere = fs::read_to_string(fixture.hot.join(manifest_of(&archive_part))).unwrap();
    let (m, d) = (manifest_of(&path), path.clone());
    let (dt, mt) = (
        temp_of(&path, TempName::Data),
        temp_of(&path, TempName::Manifest),
    );
    let bytes = |b: &[u8]| Plant::Bytes(b.to_vec());
    let mut flipped = data.clone();
    flipped[30] ^= 0x01;
    let mut long = data.clone();
    long.push(0);
    let short = data[..data.len() - 1].to_vec();
    let no_sha: String = text
        .lines()
        .filter(|line| !line.starts_with("sha256 "))
        .map(|line| format!("{line}\n"))
        .collect();
    let rows = text
        .lines()
        .find_map(|line| line.strip_prefix("rows "))
        .unwrap()
        .to_owned();
    let other = text.replace(
        &format!("rows {rows}\n"),
        &format!("rows {}\n", rows.parse::<u64>().unwrap() + 1),
    );
    let file_name = path.rsplit('/').next().unwrap();
    let dir_of = |name: &str| format!("{}/{name}", path.rsplit_once('/').unwrap().0);
    let look_alikes = [
        dir_of(&file_name.replacen('-', "\u{2010}", 1)),
        dir_of(&file_name.replace(".parquet", "\u{200b}.parquet")),
        dir_of(&format!("{file_name} ")),
    ];
    // ADR-067: a manifest whose path line spells the path with a
    // look-alike hyphen or a zero-width character, with one row more and
    // the same data, never parses to the path's own location, so it is
    // case 1 (#219), moved without a record or the flag, never an other
    // file.
    let look_alike_path = |from: &str, to: &str| {
        other.replacen(
            &format!("path {path}\n"),
            &format!("path {}\n", path.replacen(from, to, 1)),
            1,
        )
    };
    let hyphen = look_alike_path("part-", "part\u{2011}");
    let zero_width = look_alike_path(".parquet", "\u{200b}.parquet");
    assert!(hyphen != other && zero_width != other);
    let t = text.as_bytes();
    let cases: Vec<(&str, Class, Planted)> = vec![
        ("nothing", Class::Absent, vec![]),
        (
            "temp names only, with look-alikes",
            Class::Absent,
            vec![(dt.clone(), bytes(&data)), (mt.clone(), bytes(t))]
                .into_iter()
                .chain(
                    look_alikes
                        .iter()
                        .map(|n| (n.clone(), bytes(b"look-alike"))),
                )
                .collect(),
        ),
        (
            "sealed and verifying",
            Class::Accepted,
            vec![(m.clone(), bytes(t)), (d.clone(), bytes(&data))],
        ),
        (
            "verifying data without a manifest",
            Class::Accepted,
            vec![(d.clone(), bytes(&data))],
        ),
        (
            "a manifest without its data",
            Class::Refused,
            vec![(m.clone(), bytes(t))],
        ),
        (
            "data one byte short",
            Class::Refused,
            vec![(m.clone(), bytes(t)), (d.clone(), bytes(&short))],
        ),
        (
            "data one byte long",
            Class::Refused,
            vec![(m.clone(), bytes(t)), (d.clone(), bytes(&long))],
        ),
        (
            "one flipped data byte",
            Class::Refused,
            vec![(m.clone(), bytes(t)), (d.clone(), bytes(&flipped))],
        ),
        (
            "a manifest beside a verifying data temp file",
            Class::Refused,
            vec![(m.clone(), bytes(t)), (dt.clone(), bytes(&data))],
        ),
        (
            "a manifest that does not parse",
            Class::Refused,
            vec![
                (m.clone(), bytes(no_sha.as_bytes())),
                (d.clone(), bytes(&data)),
            ],
        ),
        (
            "a manifest that is not UTF-8",
            Class::Refused,
            vec![
                (m.clone(), bytes(&[0xff, 0xfe, 0x0a])),
                (d.clone(), bytes(&data)),
            ],
        ),
        (
            "a manifest describing another location",
            Class::Refused,
            vec![
                (m.clone(), bytes(elsewhere.as_bytes())),
                (d.clone(), bytes(&data)),
            ],
        ),
        (
            "data without a manifest, one byte flipped",
            Class::Refused,
            vec![(d.clone(), bytes(&flipped))],
        ),
        (
            "data without a manifest, one byte short",
            Class::Refused,
            vec![(d.clone(), bytes(&short))],
        ),
        (
            "a dangling symlink at the data path",
            Class::Refused,
            vec![(d.clone(), Plant::Dangling)],
        ),
        (
            "a manifest beside a dangling symlink at the data path",
            Class::Refused,
            vec![(m.clone(), bytes(t)), (d.clone(), Plant::Dangling)],
        ),
        (
            "a self-consistent other file",
            Class::OtherFile,
            vec![
                (m.clone(), bytes(other.as_bytes())),
                (d.clone(), bytes(&data)),
            ],
        ),
        (
            "an other text whose path line has a look-alike hyphen",
            Class::Refused,
            vec![
                (m.clone(), bytes(hyphen.as_bytes())),
                (d.clone(), bytes(&data)),
            ],
        ),
        (
            "an other text whose path line has a zero-width character",
            Class::Refused,
            vec![
                (m.clone(), bytes(zero_width.as_bytes())),
                (d.clone(), bytes(&data)),
            ],
        ),
        (
            "a directory at the data path",
            Class::Unreadable,
            vec![(d.clone(), Plant::Dir)],
        ),
        (
            "a manifest beside a directory at the data path",
            Class::Unreadable,
            vec![(m.clone(), bytes(t)), (d.clone(), Plant::Dir)],
        ),
        (
            "unreadable data",
            Class::Unreadable,
            vec![(d.clone(), Plant::Unreadable(data.clone()))],
        ),
        (
            "a manifest beside unreadable data",
            Class::Unreadable,
            vec![
                (m.clone(), bytes(t)),
                (d.clone(), Plant::Unreadable(data.clone())),
            ],
        ),
        (
            "a directory at the data temp name",
            Class::Stuck,
            vec![(dt.clone(), Plant::Dir)],
        ),
        (
            "a directory at the manifest temp name beside a data temp file",
            Class::Stuck,
            vec![(dt.clone(), bytes(&data)), (mt.clone(), Plant::Dir)],
        ),
        (
            "sealed and verifying, beside a directory at the data temp name",
            Class::Stuck,
            vec![
                (m.clone(), bytes(t)),
                (d.clone(), bytes(&data)),
                (dt.clone(), Plant::Dir),
            ],
        ),
        (
            "verifying data without a manifest, beside a directory at the manifest temp name",
            Class::Stuck,
            vec![(d.clone(), bytes(&data)), (mt.clone(), Plant::Dir)],
        ),
        (
            "a manifest without its data, beside a directory at the data temp name",
            Class::Refused,
            vec![(m.clone(), bytes(t)), (dt.clone(), Plant::Dir)],
        ),
        (
            "a self-consistent other file, beside a directory at the manifest temp name",
            Class::OtherFile,
            vec![
                (m.clone(), bytes(other.as_bytes())),
                (d.clone(), bytes(&data)),
                (mt.clone(), Plant::Dir),
            ],
        ),
        (
            "dangling symlinks at both temp names",
            Class::Absent,
            vec![(dt.clone(), Plant::Dangling), (mt.clone(), Plant::Dangling)],
        ),
        (
            "unreadable temp files beside a verifying sealed copy",
            Class::Accepted,
            vec![
                (m.clone(), bytes(t)),
                (d.clone(), bytes(&data)),
                (dt.clone(), Plant::Unreadable(data.clone())),
                (mt.clone(), Plant::Unreadable(t.to_vec())),
            ],
        ),
    ];
    for (n, (form, class, planted)) in cases.into_iter().enumerate() {
        let judged_root = fixture.dir.0.join(format!("judged-{n}"));
        let passed_root = fixture.dir.0.join(format!("passed-{n}"));
        for root in [&judged_root, &passed_root] {
            fs::create_dir_all(root).unwrap();
            for (name, what) in &planted {
                plant(root, name, what);
            }
        }
        let judged = r2b_form(&judged_root, &path, &text);
        let got = match &judged {
            Ok(Form::Judged(UnrecordedCopy::Absent)) => Class::Absent,
            Ok(Form::Judged(UnrecordedCopy::Accepted)) => Class::Accepted,
            Ok(Form::Judged(UnrecordedCopy::OtherFile)) => Class::OtherFile,
            Ok(Form::Judged(UnrecordedCopy::Refused(_))) => Class::Refused,
            Ok(Form::Stuck { .. }) => Class::Stuck,
            Err(RawStoreError::Io(_)) => Class::Unreadable,
            Err(other) => panic!("{form}: {other:?}"),
        };
        assert_eq!(got, class, "{form}: {judged:?}");
        let copied = replica::copy_sealed(&fixture.hot, &passed_root, &path);
        match (class, &copied) {
            (Class::Absent | Class::Accepted, Ok(_))
            | (
                Class::Refused | Class::OtherFile,
                Err(CopyError {
                    side: Side::Destination,
                    error: RawStoreError::Integrity(_),
                }),
            )
            | (
                Class::Unreadable | Class::Stuck,
                Err(CopyError {
                    side: Side::Destination,
                    error: RawStoreError::Io(_),
                }),
            ) => {}
            _ => panic!("{form}: judged {judged:?}, the pass's D5 copy gave {copied:?}"),
        }
        for root in [&judged_root, &passed_root] {
            for name in &look_alikes {
                if planted.iter().any(|(planted, _)| planted == name) {
                    assert_eq!(fs::read(root.join(name)).unwrap(), b"look-alike", "{form}");
                }
            }
        }
    }
}

#[test]
fn r2b_takes_ordinary_held_and_unmanifested_paths_only() {
    assert!(r2b_takes(None));
    for (verdict, taken) in [
        (PathVerdict::Ordinary, true),
        (PathVerdict::NoManifest, true),
        (PathVerdict::Held, true),
        (PathVerdict::Lost, false),
        (PathVerdict::Conflict, false),
        (PathVerdict::Replaced, false),
        (PathVerdict::Pending, false),
    ] {
        assert_eq!(r2b_takes(Some(verdict)), taken, "{verdict:?}");
    }
}

/// How the state arises on main (#219, item 2): after a full pass a new
/// live part P is sealed, and a pass crashes after publishing P on cold-a,
/// before any record. Then cold-a's P gets one flipped data byte and cold-b
/// a manifest of P without its data. Returns P and cold-a's damaged bytes.
fn crashed_then_damaged(tag: &str) -> (Fixture, String, Vec<u8>) {
    let fixture = Fixture::new(tag);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2);
    assert_eq!(new.len(), 1, "{new:?}");
    let path = new[0].clone();
    replicate::CRASH_AT.with(|crash| crash.set(Some(replicate::PassStep::AfterPublish)));
    fixture.pass();
    assert!(replicate::CRASH_AT.with(std::cell::Cell::get).is_none());
    let (cold_a, cold_b) = (&fixture.cold[0], &fixture.cold[1]);
    assert!(cold_a.join(&path).exists() && !cold_b.join(&path).exists());
    flip(&cold_a.join(&path), 30);
    let damaged = fs::read(cold_a.join(&path)).unwrap();
    plant(
        cold_b,
        &manifest_of(&path),
        &Plant::Bytes(fs::read(fixture.hot.join(manifest_of(&path))).unwrap()),
    );
    for root in [cold_a, cold_b] {
        assert!(
            matches!(
                replica::copy_sealed(&fixture.hot, root, &path),
                Err(CopyError {
                    error: RawStoreError::Integrity(_),
                    ..
                })
            ),
            "{}",
            root.display()
        );
    }
    (fixture, path, damaged)
}

fn verified(fixture: &Fixture, now: i64) -> Vec<String> {
    verify::run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| now,
        &CopyLedger::at(&fixture.hot),
    )
    .problems
}

/// The issue's item 2, cleared: both forms move byte-identical into
/// `damaged/<at>/` (cold-a, the first target, and cold-b, the last; hot
/// checked once), the closing pass appends P's `manifest` record and then
/// one `copy` record per target, and a later pass and verification are
/// clean.
#[test]
fn r2b_a_pass_crash_after_publish_then_damage_is_cleared() {
    let (fixture, path, damaged) = crashed_then_damaged("r2b-crash-publish");
    let text = fs::read(fixture.hot.join(manifest_of(&path))).unwrap();
    let report = repair(&fixture, D0 + 2 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let aside = |root: &Path| replica::damaged_dir(root, D0 + 2 * HOUR);
    assert_eq!(
        fs::read(aside(&fixture.cold[0]).join(&path)).unwrap(),
        damaged
    );
    for root in &fixture.cold {
        assert_eq!(
            fs::read(aside(root).join(manifest_of(&path))).unwrap(),
            text
        );
        assert_eq!(
            fs::read(root.join(&path)).unwrap(),
            fs::read(fixture.hot.join(&path)).unwrap()
        );
    }
    assert!(!aside(&fixture.cold[1]).join(&path).exists());
    let records = fixture.records();
    let (manifests, copies) = counts(&records);
    assert_eq!(manifests.get(&path), Some(&1));
    for id in ["cold-a", "cold-b"] {
        assert_eq!(copies.get(&(path.clone(), id.to_owned())), Some(&1), "{id}");
    }
    let manifest = records
        .iter()
        .position(|r| matches!(&r.kind, Kind::Manifest { text } if text.contains(&path)))
        .unwrap();
    let first_copy = records
        .iter()
        .position(|r| matches!(&r.kind, Kind::Copy { path: p, .. } if *p == path))
        .unwrap();
    assert!(manifest < first_copy);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(verified(&fixture, D0 + 3 * HOUR), Vec::<String>::new());
}

/// A crash right after R2b's first move-aside: the next run moves the rest
/// and ends where an uninterrupted run ends, every ledger copy a valid
/// prefix of hot's, and a verification passes.
#[test]
fn r2b_a_crash_after_the_move_aside_is_completed_by_the_next_run() {
    let (reference, _, _) = crashed_then_damaged("r2b-crash-reference");
    let report = repair(&reference, D0 + 2 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let (fixture, path, damaged) = crashed_then_damaged("r2b-crash-aside");
    CRASH_AT.with(|crash| crash.set(Some(RepairStep::AfterMoveAside)));
    let crashed = repair(&fixture, D0 + 2 * HOUR);
    assert!(CRASH_AT.with(std::cell::Cell::get).is_none());
    assert!(
        crashed
            .problems
            .iter()
            .any(|p| p.contains("simulated crash")),
        "{crashed:?}"
    );
    assert_eq!(
        fs::read(replica::damaged_dir(&fixture.cold[0], D0 + 2 * HOUR).join(&path)).unwrap(),
        damaged
    );
    let report = repair(&fixture, D0 + 3 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert_eq!(trees(&fixture), trees(&reference));
    let hot = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    for root in &fixture.cold {
        let copy = fs::read(copy_ledger::ledger_path(root)).unwrap();
        assert_eq!(copy_ledger::check_chain(&copy).failure, None);
        assert!(hot.starts_with(&copy));
    }
    assert_eq!(verified(&fixture, D0 + 4 * HOUR), Vec::<String>::new());
}

/// R2b never judges a lost path (R9's) nor a path whose partition is
/// recorded removed while still on hot (the hot removal's resume): cold-a's
/// damaged copy stays in place.
#[test]
fn r2b_lost_and_removed_paths_are_never_touched() {
    for kind in ["lost", "removed"] {
        let (fixture, path, damaged) = crashed_then_damaged(&format!("r2b-untouched-{kind}"));
        let sha = replica::sealed_manifest(&fixture.hot, &path)
            .unwrap()
            .unwrap()
            .1
            .sha256;
        let record = match kind {
            "lost" => Kind::Lost {
                path: path.clone(),
                origin: LostOrigin::Verify,
                sha256: Some(sha),
            },
            _ => Kind::Removed {
                partition: partition_of(&path).to_owned(),
            },
        };
        CopyLedger::at(&fixture.hot)
            .append(&Record {
                at_ms: D0 + HOUR,
                kind: record,
            })
            .unwrap();
        let report = repair(&fixture, D0 + 2 * HOUR);
        assert_eq!(
            fs::read(fixture.cold[0].join(&path)).unwrap(),
            damaged,
            "{kind}: {report:?}"
        );
        assert!(
            !replica::damaged_dir(&fixture.cold[0], D0 + 2 * HOUR)
                .join(&path)
                .exists(),
            "{kind}"
        );
        assert!(
            !report.lines.iter().any(|l| l.contains("no copy record")),
            "{kind}: {report:?}"
        );
    }
}

/// The closing pass copies an archive part only once a current import
/// entry on hot lists it (D11), so R2b moves one only then: (a) a new
/// unlisted part's manifest without data on cold-a stays, named; (b) a
/// listed part held `entry-conflict`, which cold-b holds with one flipped
/// data byte and no record (its marker was hidden for the first pass), is
/// moved, and the closing pass records `copy <part> cold-b`.
#[test]
fn r2b_archive_parts_follow_the_pass_gate() {
    let fixture = Fixture::new("r2b-archive-unlisted");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-archive", "aggTrades", D0 - 86_400_000 + 10_000, 2);
    assert_eq!(new.len(), 1, "{new:?}");
    let part = new[0].clone();
    let manifest = fs::read(fixture.hot.join(manifest_of(&part))).unwrap();
    plant(
        &fixture.cold[0],
        &manifest_of(&part),
        &Plant::Bytes(manifest.clone()),
    );
    let report = repair(&fixture, D0 + 2 * HOUR);
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains(&part) && p.contains("cold-a") && p.contains("import entry")),
        "{report:?}"
    );
    assert_eq!(
        fs::read(fixture.cold[0].join(manifest_of(&part))).unwrap(),
        manifest
    );
    assert!(!replica::damaged_dir(&fixture.cold[0], D0 + 2 * HOUR).exists());

    // (a′) ADR-067: a self-consistent other file under the unlisted part
    // stays too, with the flag, named, and nothing is recorded.
    let fixture = Fixture::new("r2b-archive-unlisted-other");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-archive", "aggTrades", D0 - 86_400_000 + 10_000, 2);
    let part = new[0].clone();
    plant(
        &fixture.cold[0],
        &part,
        &Plant::Bytes(fs::read(fixture.hot.join(&part)).unwrap()),
    );
    plant_other_file(&fixture.cold[0], &part, &other_text(&fixture.hot, &part));
    let before = fs::read(fixture.cold[0].join(manifest_of(&part))).unwrap();
    let report = repair_moving(&fixture, D0 + 2 * HOUR);
    assert!(
        report.problems.iter().any(|p| p.contains(&part)
            && p.contains("cold-a")
            && p.contains("self-consistent other file")
            && p.contains("import entry")),
        "{report:?}"
    );
    assert_eq!(
        fs::read(fixture.cold[0].join(manifest_of(&part))).unwrap(),
        before
    );
    assert!(!replica::damaged_dir(&fixture.cold[0], D0 + 2 * HOUR).exists());
    assert!(displaced(&fixture).is_empty());

    let fixture = Fixture::new("r2b-archive-held");
    let marker = root_marker::marker_path(&fixture.cold[1]);
    let hidden = fixture.dir.0.join("cold-b.marker");
    fs::rename(&marker, &hidden).unwrap();
    fixture.pass();
    fs::rename(&hidden, &marker).unwrap();
    let part = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-archive"))
        .unwrap();
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + HOUR,
            kind: Kind::Hold {
                path: part.clone(),
                reason: HoldReason::EntryConflict,
            },
        })
        .unwrap();
    let state = CopyState::fold(&copy_ledger::read(&fixture.hot).unwrap().unwrap()).unwrap();
    assert_eq!(state.verdict(&part), Some(PathVerdict::Held));
    let mut data = fs::read(fixture.hot.join(&part)).unwrap();
    data[30] ^= 0x01;
    plant(&fixture.cold[1], &part, &Plant::Bytes(data.clone()));
    plant(
        &fixture.cold[1],
        &manifest_of(&part),
        &Plant::Bytes(fs::read(fixture.hot.join(manifest_of(&part))).unwrap()),
    );
    let report = repair(&fixture, D0 + 2 * HOUR);
    assert_eq!(
        fs::read(replica::damaged_dir(&fixture.cold[1], D0 + 2 * HOUR).join(&part)).unwrap(),
        data,
        "{report:?}"
    );
    assert_eq!(
        fs::read(fixture.cold[1].join(&part)).unwrap(),
        fs::read(fixture.hot.join(&part)).unwrap()
    );
    let (_, copies) = counts(&fixture.records());
    assert_eq!(copies.get(&(part, "cold-b".to_owned())), Some(&1));
}

/// A copy target whose marker goes after the run's marker reads (an
/// unmount mid-run) is never written: R2b moves nothing on it and names
/// the marker.
#[test]
fn r2b_a_target_whose_marker_goes_mid_run_is_not_written() {
    let (fixture, path, damaged) = crashed_then_damaged("r2b-marker-gone");
    let marker = root_marker::marker_path(&fixture.cold[0]);
    let hidden = fixture.dir.0.join("cold-a.marker");
    {
        let (marker, hidden) = (marker.clone(), hidden.clone());
        BEFORE_WORK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || fs::rename(&marker, &hidden).unwrap()));
        });
    }
    let report = repair(&fixture, D0 + 2 * HOUR);
    fs::rename(&hidden, &marker).unwrap();
    assert_eq!(fs::read(fixture.cold[0].join(&path)).unwrap(), damaged);
    assert!(!replica::damaged_dir(&fixture.cold[0], D0 + 2 * HOUR).exists());
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains("cold-a") && p.contains("marker")),
        "{report:?}"
    );
}

/// The evidence name R2b would move to is already taken at the run's
/// `at_ms`: the move refuses and is named, both files stay byte-identical,
/// and a run at a new `at_ms` completes.
#[test]
fn r2b_an_evidence_name_taken_at_the_same_at_ms_is_never_overwritten() {
    let (fixture, path, damaged) = crashed_then_damaged("r2b-evidence-taken");
    let at = D0 + 2 * HOUR;
    let taken = replica::damaged_dir(&fixture.cold[0], at).join(&path);
    plant(
        &fixture.cold[0],
        &taken
            .strip_prefix(&fixture.cold[0])
            .unwrap()
            .display()
            .to_string(),
        &Plant::Bytes(b"evidence".to_vec()),
    );
    let report = repair(&fixture, at);
    assert!(
        report.problems.iter().any(|p| p.contains(&path)
            && p.contains("cold-a")
            && p.contains("moving it aside failed")),
        "{report:?}"
    );
    assert_eq!(fs::read(&taken).unwrap(), b"evidence");
    assert_eq!(fs::read(fixture.cold[0].join(&path)).unwrap(), damaged);
    let report = repair(&fixture, at + 1);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert_eq!(
        fs::read(replica::damaged_dir(&fixture.cold[0], at + 1).join(&path)).unwrap(),
        damaged
    );
    assert_eq!(fs::read(&taken).unwrap(), b"evidence");
}

/// A temp name of P that the closing pass's D5 copy cannot remove (a
/// directory: the copy removes both temp names by unlink before it copies
/// or records) is moved aside alone, with a `FAIL:` (#219 review): on
/// cold-a beside a published, verifying copy no record covers (a pass that
/// crashed after publishing), on cold-b with neither `D` nor `M`. The
/// verified copy on cold-a stays in place and byte-identical, each
/// directory moves with its content into `damaged/<at>/`, the closing pass
/// records `copy P` on both targets, and a later pass and verification are
/// clean. Without the move, every pass stops cold-b on EISDIR.
#[test]
fn r2b_a_temp_name_the_pass_cannot_remove_is_moved_aside() {
    let fixture = Fixture::new("r2b-stuck-temp");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2);
    assert_eq!(new.len(), 1, "{new:?}");
    let path = new[0].clone();
    replicate::CRASH_AT.with(|crash| crash.set(Some(replicate::PassStep::AfterPublish)));
    fixture.pass();
    assert!(replicate::CRASH_AT.with(std::cell::Cell::get).is_none());
    let (cold_a, cold_b) = (&fixture.cold[0], &fixture.cold[1]);
    assert!(cold_a.join(&path).exists() && !cold_b.join(&path).exists());
    let published = [
        fs::read(cold_a.join(&path)).unwrap(),
        fs::read(cold_a.join(manifest_of(&path))).unwrap(),
    ];
    let stuck = [
        (cold_a, temp_of(&path, TempName::Data)),
        (cold_b, temp_of(&path, TempName::Manifest)),
    ];
    for (root, name) in &stuck {
        plant(root, name, &Plant::Dir);
        plant(
            root,
            &format!("{name}/inside"),
            &Plant::Bytes(b"evidence".to_vec()),
        );
    }
    let at = D0 + 2 * HOUR;
    let report = repair(&fixture, at);
    for (root, name) in &stuck {
        let id = root.file_name().unwrap().to_str().unwrap();
        assert!(
            report.problems.iter().any(|p| p.contains(&path)
                && p.contains(id)
                && p.contains(name.as_str())
                && p.contains("cannot remove")
                && p.contains("moved aside")),
            "{id}: {report:?}"
        );
        assert!(!root.join(name).exists(), "{id}");
        let aside = replica::damaged_dir(root, at).join(name);
        assert_eq!(fs::read(aside.join("inside")).unwrap(), b"evidence", "{id}");
        assert_eq!(fs::read_dir(&aside).unwrap().count(), 1, "{id}");
    }
    assert_eq!(report.problems.len(), stuck.len(), "{report:?}");
    assert_eq!(fs::read(cold_a.join(&path)).unwrap(), published[0]);
    assert_eq!(
        fs::read(cold_a.join(manifest_of(&path))).unwrap(),
        published[1]
    );
    assert!(!replica::damaged_dir(cold_a, at).join(&path).exists());
    assert!(
        !replica::damaged_dir(cold_a, at)
            .join(manifest_of(&path))
            .exists()
    );
    assert_eq!(
        fs::read(cold_b.join(&path)).unwrap(),
        fs::read(fixture.hot.join(&path)).unwrap()
    );
    let (_, copies) = counts(&fixture.records());
    for id in ["cold-a", "cold-b"] {
        assert_eq!(copies.get(&(path.clone(), id.to_owned())), Some(&1), "{id}");
    }
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(repair(&fixture, at + HOUR).problems, Vec::<String>::new());
    assert_eq!(verified(&fixture, at + 2 * HOUR), Vec::<String>::new());
}

/// R2b's gates hold for a temp name the pass cannot remove: an archive
/// part no current import entry lists is not copied by the closing pass,
/// so a directory at its data temp name on cold-a is named and stays.
#[test]
fn r2b_a_stuck_temp_name_of_an_unlisted_archive_part_stays() {
    let fixture = Fixture::new("r2b-stuck-unlisted");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-archive", "aggTrades", D0 - 86_400_000 + 10_000, 2);
    assert_eq!(new.len(), 1, "{new:?}");
    let part = new[0].clone();
    let name = temp_of(&part, TempName::Data);
    plant(&fixture.cold[0], &name, &Plant::Dir);
    let report = repair(&fixture, D0 + 2 * HOUR);
    assert!(
        report.problems.iter().any(|p| p.contains(&part)
            && p.contains("cold-a")
            && p.contains("cannot remove")
            && p.contains("import entry")),
        "{report:?}"
    );
    assert!(fixture.cold[0].join(&name).is_dir());
    assert!(!replica::damaged_dir(&fixture.cold[0], D0 + 2 * HOUR).exists());
}

/// The temp names `damaged/<at>/` holds on `root`, by relative path: R2b
/// moves nothing else of a path whose form it does not move.
fn moved_names(root: &Path, at: i64) -> Vec<String> {
    let aside = replica::damaged_dir(root, at);
    let mut names = Vec::new();
    let mut dirs = vec![aside.clone()];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".tmp") || !entry.file_type().unwrap().is_dir() {
                let relative = entry.path().strip_prefix(&aside).unwrap().to_owned();
                names.push(relative.display().to_string());
            } else {
                dirs.push(entry.path());
            }
        }
    }
    names.sort();
    names
}

/// A temp name the pass cannot remove is no sealed file, so R2b moves it
/// whatever hot's copy of the path is (#219 review, round 2): with one
/// flipped data byte in hot's P (its manifest unchanged, so the pass still
/// reaches its copy), a directory at P's data temp name on cold-a, the
/// first copy target, is moved aside, while cold-b's manifest of P
/// without its data, a form the pass refuses, stays in place, named, as
/// with any bad hot copy. The closing pass then stops no target, and a
/// later healthy part Q reaches both. Without the move, every pass stops
/// cold-a on EISDIR before Q.
#[test]
fn r2b_a_stuck_temp_name_moves_although_hot_is_bad() {
    let fixture = Fixture::new("r2b-stuck-hot-bad");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2);
    assert_eq!(new.len(), 1, "{new:?}");
    let path = new[0].clone();
    let manifest = manifest_of(&path);
    let (cold_a, cold_b) = (&fixture.cold[0], &fixture.cold[1]);
    let name = temp_of(&path, TempName::Data);
    plant(cold_a, &name, &Plant::Dir);
    let text = fs::read(fixture.hot.join(&manifest)).unwrap();
    plant(cold_b, &manifest, &Plant::Bytes(text.clone()));
    flip(&fixture.hot.join(&path), 30);
    let at = D0 + 2 * HOUR;
    let report = repair(&fixture, at);
    assert!(
        report.problems.iter().any(|p| p.contains(&path)
            && p.contains("cold-a")
            && p.contains(name.as_str())
            && p.contains("moved aside")
            && p.contains("not good (sha256)")),
        "{report:?}"
    );
    assert!(
        report.problems.iter().any(|p| p.contains(&path)
            && p.contains("cold-b")
            && p.contains("not moved: hot's copy is not good")),
        "{report:?}"
    );
    assert!(
        !report.problems.iter().any(|p| p.contains("stopped")),
        "{report:?}"
    );
    assert!(!cold_a.join(&name).exists());
    assert_eq!(moved_names(cold_a, at), vec![name.clone()]);
    assert_eq!(fs::read(cold_b.join(&manifest)).unwrap(), text);
    assert!(!replica::damaged_dir(cold_b, at).exists());
    let q = fixture.seal("binance-um", "aggTrade", D0 + 9_000, 2);
    assert_eq!(q.len(), 1, "{q:?}");
    let pass = fixture.pass();
    assert!(
        !pass.problems.iter().any(|p| p.contains("stopped")),
        "{pass:?}"
    );
    for root in &fixture.cold {
        assert_eq!(
            fs::read(root.join(&q[0])).unwrap(),
            fs::read(fixture.hot.join(&q[0])).unwrap(),
            "{}",
            root.display()
        );
    }
}

/// A path recorded lost is R9's, so R2b never judges its form, but the
/// closing pass still copies it from hot when hot lists it with a
/// manifest the ledger does not contradict, and so still meets its temp
/// names (#219 review, round 2): a directory at its data temp name on
/// cold-a, the first copy target, is moved aside alone, nothing sealed
/// of it moves, and a later healthy part Q reaches both targets. Without
/// the move, every pass stops cold-a on EISDIR before Q.
#[test]
fn r2b_a_stuck_temp_name_of_a_lost_path_moves_alone() {
    let fixture = Fixture::new("r2b-stuck-lost");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2);
    assert_eq!(new.len(), 1, "{new:?}");
    let path = new[0].clone();
    let sha = replica::sealed_manifest(&fixture.hot, &path)
        .unwrap()
        .unwrap()
        .1
        .sha256;
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + HOUR,
            kind: Kind::Lost {
                path: path.clone(),
                origin: LostOrigin::Verify,
                sha256: Some(sha),
            },
        })
        .unwrap();
    let cold_a = &fixture.cold[0];
    let name = temp_of(&path, TempName::Data);
    plant(cold_a, &name, &Plant::Dir);
    let at = D0 + 2 * HOUR;
    let report = repair(&fixture, at);
    assert!(
        report.problems.iter().any(|p| p.contains(&path)
            && p.contains("cold-a")
            && p.contains(name.as_str())
            && p.contains("moved aside")),
        "{report:?}"
    );
    assert!(
        !report.problems.iter().any(|p| p.contains("stopped")),
        "{report:?}"
    );
    assert!(!cold_a.join(&name).exists());
    assert_eq!(moved_names(cold_a, at), vec![name.clone()]);
    let q = fixture.seal("binance-um", "aggTrade", D0 + 9_000, 2);
    assert_eq!(q.len(), 1, "{q:?}");
    let pass = fixture.pass();
    assert!(
        !pass.problems.iter().any(|p| p.contains("stopped")),
        "{pass:?}"
    );
    for root in &fixture.cold {
        assert!(root.join(&q[0]).exists(), "{}", root.display());
    }
}

/// A new part P sealed after a clean pass, then one pass with cold-a's
/// marker hidden: cold-b holds P with its `copy` record, cold-a holds no
/// record of it.
fn unrecorded_on_cold_a(tag: &str) -> (Fixture, String) {
    let fixture = Fixture::new(tag);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2);
    assert_eq!(new.len(), 1, "{new:?}");
    let marker = root_marker::marker_path(&fixture.cold[0]);
    let hidden = fixture.dir.0.join("cold-a.marker");
    fs::rename(&marker, &hidden).unwrap();
    fixture.pass();
    fs::rename(&hidden, &marker).unwrap();
    let path = new[0].clone();
    let (_, copies) = counts(&fixture.records());
    assert_eq!(copies.get(&(path.clone(), "cold-a".to_owned())), None);
    assert_eq!(copies.get(&(path.clone(), "cold-b".to_owned())), Some(&1));
    (fixture, path)
}

/// A hot removal that crashes after its `removed` record and before it
/// deletes the files leaves a partition recorded removed whose files hot
/// still holds, and the closing pass still copies them to a copy target
/// without a `copy` record of them (#219 review, round 3). A directory at
/// the data temp name of such a path P on cold-a is moved aside alone, and
/// nothing sealed of P there is judged or moved. The closing pass then
/// stops no target: P and a later healthy part Q in another partition
/// reach cold-a, and a later pass, repair and verification are clean.
/// Without the move, every pass stops cold-a on EISDIR, so the removal
/// never completes and Q never reaches cold-a.
#[test]
fn r2b_a_stuck_temp_name_of_a_removed_partition_moves_alone() {
    let (fixture, path) = unrecorded_on_cold_a("r2b-stuck-removed");
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + HOUR,
            kind: Kind::Removed {
                partition: partition_of(&path).to_owned(),
            },
        })
        .unwrap();
    let cold_a = &fixture.cold[0];
    let name = temp_of(&path, TempName::Data);
    plant(cold_a, &name, &Plant::Dir);
    let q = fixture.seal("binance-um", "aggTrade", D0 + 2 * 86_400_000, 2);
    assert_eq!(q.len(), 1, "{q:?}");
    assert_ne!(partition_of(&q[0]), partition_of(&path));
    let at = D0 + 2 * HOUR;
    let report = repair(&fixture, at);
    assert!(
        report.problems.iter().any(|p| p.contains(&path)
            && p.contains("cold-a")
            && p.contains(name.as_str())
            && p.contains("recorded removed")
            && p.contains("moved aside")),
        "{report:?}"
    );
    assert!(
        !report.problems.iter().any(|p| p.contains("stopped")),
        "{report:?}"
    );
    assert!(!cold_a.join(&name).exists());
    assert_eq!(moved_names(cold_a, at), vec![name.clone()]);
    for root in &fixture.cold {
        assert_eq!(
            fs::read(root.join(&q[0])).unwrap(),
            fs::read(fixture.hot.join(&q[0])).unwrap(),
            "{}",
            root.display()
        );
    }
    assert_eq!(
        fs::read(cold_a.join(&path)).unwrap(),
        fs::read(fixture.cold[1].join(&path)).unwrap()
    );
    let (_, copies) = counts(&fixture.records());
    assert_eq!(copies.get(&(path.clone(), "cold-a".to_owned())), Some(&1));
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(repair(&fixture, at + HOUR).problems, Vec::<String>::new());
    assert_eq!(verified(&fixture, at + 2 * HOUR), Vec::<String>::new());
}

/// R2b's report of a moved temp name says what the pass does with it
/// (#219 review, round 3), and the move itself is the same in every case:
/// (a) an absent form the pass reaches: every pass stops the target on
/// it; (b) hot's manifest differs from the copy ledger's `manifest`
/// record, so the pass's D5 guard copies nothing of the path and never
/// reaches the name now; (c) a lost path whose form on the target is not
/// judged (a manifest without its data), which the pass meets before the
/// name.
#[test]
fn r2b_the_report_of_a_moved_temp_name_says_whether_the_pass_reaches_it() {
    for case in ["reached", "guard", "lost-form"] {
        let (fixture, path) = unrecorded_on_cold_a(&format!("r2b-stuck-say-{case}"));
        let cold_a = &fixture.cold[0];
        match case {
            "guard" => {
                let hot_manifest = fixture.hot.join(manifest_of(&path));
                let text = fs::read_to_string(&hot_manifest).unwrap();
                let sha = replica::parse_manifest(&text).unwrap().sha256;
                let other = format!(
                    "{}{}",
                    if sha.starts_with('a') { 'b' } else { 'a' },
                    &sha[1..]
                );
                let other_text = text.replace(&sha, &other);
                assert!(replica::parse_manifest(&other_text).is_ok());
                fs::set_permissions(&hot_manifest, fs::Permissions::from_mode(0o644)).unwrap();
                fs::write(&hot_manifest, other_text).unwrap();
            }
            "lost-form" => {
                let sha = replica::sealed_manifest(&fixture.hot, &path)
                    .unwrap()
                    .unwrap()
                    .1
                    .sha256;
                CopyLedger::at(&fixture.hot)
                    .append(&Record {
                        at_ms: D0 + HOUR,
                        kind: Kind::Lost {
                            path: path.clone(),
                            origin: LostOrigin::Verify,
                            sha256: Some(sha),
                        },
                    })
                    .unwrap();
                let text = fs::read(fixture.hot.join(manifest_of(&path))).unwrap();
                plant(cold_a, &manifest_of(&path), &Plant::Bytes(text));
            }
            _ => {}
        }
        let name = temp_of(&path, TempName::Data);
        plant(cold_a, &name, &Plant::Dir);
        let at = D0 + 2 * HOUR;
        let report = repair(&fixture, at);
        let said: Vec<&String> = report
            .problems
            .iter()
            .filter(|p| p.contains(&path) && p.contains("cold-a") && p.contains("moved aside"))
            .collect();
        assert_eq!(said.len(), 1, "{case}: {report:?}");
        let every = said[0].contains("every pass stops this target on it");
        assert_eq!(every, case == "reached", "{case}: {}", said[0]);
        match case {
            "guard" => assert!(
                said[0].contains("no pass copies it now")
                    && said[0].contains("differs from the copy ledger's manifest record"),
                "{case}: {}",
                said[0]
            ),
            "lost-form" => assert!(
                said[0].contains("whenever its D5 copy accepts the form there"),
                "{case}: {}",
                said[0]
            ),
            _ => {}
        }
        assert!(!cold_a.join(&name).exists(), "{case}");
        assert_eq!(moved_names(cold_a, at), vec![name.clone()], "{case}");
    }
}

// ------------------------------------------------- D5 (ADR-066, #224)

/// One journal line of exactly `len` bytes, its `\n` included: a record
/// carrying `"n":n` and a `pad` of `*`, with the line check (ADR-066 D1)
/// when `checked`.
fn journal_line(n: usize, len: usize, checked: bool) -> Vec<u8> {
    let object_len = if checked { len - 21 } else { len - 1 };
    let mut object =
        format!("{{\"type\":\"stats\",\"run_id\":\"A\",\"at_ms\":1,\"n\":{n},\"pad\":\"")
            .into_bytes();
    object.resize(object_len - 2, b'*');
    object.extend_from_slice(b"\"}");
    let mut line = if checked {
        crate::journal::with_check(&object)
    } else {
        object
    };
    line.push(b'\n');
    assert_eq!(line.len(), len);
    line
}

/// The plan's journal: five 200-byte lines (starts 0, 200, 400, 600, 800),
/// checked or not.
fn five(checked: bool) -> Vec<u8> {
    (1..=5)
        .flat_map(|n| journal_line(n, 200, checked))
        .collect()
}

fn flipped(bytes: &[u8], at: &[usize]) -> Vec<u8> {
    let mut bytes = bytes.to_vec();
    for &i in at {
        bytes[i] ^= 0x01;
    }
    bytes
}

/// Hot and its witnesses written to files of a fresh directory.
struct Journals {
    _dir: crate::store::replicate::tests::Dir,
    hot: PathBuf,
    witnesses: Vec<PathBuf>,
}

impl Journals {
    fn new(hot: &[u8], witnesses: &[&[u8]]) -> Self {
        let dir = crate::store::replicate::tests::Dir::new("repair-d5");
        let path = dir.0.join("hot");
        fs::write(&path, hot).unwrap();
        let witnesses = witnesses
            .iter()
            .enumerate()
            .map(|(i, bytes)| {
                let path = dir.0.join(format!("copy-{i}"));
                fs::write(&path, bytes).unwrap();
                path
            })
            .collect();
        Self {
            _dir: dir,
            hot: path,
            witnesses,
        }
    }

    fn refs(&self) -> Vec<&Path> {
        self.witnesses.iter().map(PathBuf::as_path).collect()
    }

    /// D5 from `from`, unbounded.
    fn contest(&self, from: u64) -> Option<Contest> {
        journal_contest(&self.hot, &self.refs(), from, u64::MAX, &[]).unwrap()
    }

    /// ADR-065 D8 from `from`.
    fn d8(&self, from: u64) -> Option<Contested> {
        replica::first_contested_file(&self.hot, &self.refs(), from).unwrap()
    }
}

/// The plan's threshold arithmetic at k = 2, hot 1 000 B contradicted at
/// 450. Without checks: hot and cold-b against cold-a, 2·2 = 4 > 3, hot
/// wins; cold-b only 400 B, hot against cold-a alone, 2·1 = 2 ≤ 2,
/// refused. With checks, the same 400-B geometry: hot's line Intact and
/// cold-a's Failed, settled for hot with no count.
#[test]
fn d5_hot_intact_settles_one_against_one() {
    let legacy = five(false);
    let damaged = flipped(&legacy, &[450]);
    let two = Journals::new(&legacy, &[&damaged, &legacy]);
    assert_eq!(two.contest(400), None);
    let one = Journals::new(&legacy, &[&damaged, &legacy[..400]]);
    let refused = Contested {
        at: 450,
        covering: 2,
        with_hot: 1,
    };
    assert_eq!(one.d8(400), Some(refused));
    assert_eq!(one.contest(400), Some(Contest::Majority(refused)));

    let hot = five(true);
    let cold_a = flipped(&hot, &[450]);
    let checked = Journals::new(&hot, &[&cold_a, &hot[..400]]);
    assert_eq!(checked.d8(400).map(|c| c.at), Some(450));
    assert_eq!(checked.contest(400), None);
    assert_eq!(checked.contest(0), None);
}

/// Hot's line 3 fails its check, cold-a holds hot's bytes, cold-b
/// contradicts: hot holds 2 of 3, a majority, and D5 still refuses.
#[test]
fn d5_hot_line_failing_refuses_despite_a_majority() {
    let good = five(true);
    let hot = flipped(&good, &[450]);
    let journals = Journals::new(&hot, &[&hot, &good]);
    assert_eq!(journals.d8(0), None);
    assert_eq!(
        journals.contest(0),
        Some(Contest::HotLineFails {
            at: 450,
            line_start: 400,
            recorded: false
        })
    );
}

/// Hot's line 3 and cold-a's differ and both verify: a fork, whoever holds
/// a majority.
#[test]
fn d5_both_lines_verify_is_a_fork() {
    let good = five(true);
    let mut hot = good[..400].to_vec();
    hot.extend(journal_line(9, 200, true));
    hot.extend_from_slice(&good[600..]);
    // `{"type":"stats","run_id":"A","at_ms":1,"n":` is 43 bytes.
    let journals = Journals::new(&hot, &[&good, &hot]);
    assert_eq!(journals.d8(0), None);
    assert_eq!(
        journals.contest(0),
        Some(Contest::Fork {
            at: 443,
            witness: 0,
            hot: LineCheck::Intact
        })
    );
}

/// A witness line without its check (its member's name damaged) makes no
/// claim: ADR-065 D8's majority decides the line, one against one refused,
/// two against one for hot settled.
#[test]
fn d5_unchecked_witness_line_keeps_the_majority() {
    let hot = five(true);
    // Line 3's member starts at 578 (its `,`); 580 is the first `c`.
    let mut cold_a = hot.clone();
    cold_a[580] = b'C';
    let one = Journals::new(&hot, &[&cold_a]);
    assert_eq!(
        one.contest(0),
        Some(Contest::Majority(Contested {
            at: 580,
            covering: 2,
            with_hot: 1
        }))
    );
    let two = Journals::new(&hot, &[&cold_a, &hot]);
    assert_eq!(two.contest(0), None);
}

/// A copy's line that verifies is never overwritten by hot bytes without a
/// check, however many journals hold them.
#[test]
fn d5_verifying_copy_line_against_unchecked_hot_refuses() {
    let good = five(true);
    let mut hot = good.clone();
    hot[580] = b'C';
    let journals = Journals::new(&hot, &[&good, &hot]);
    assert_eq!(journals.d8(0), None);
    assert_eq!(
        journals.contest(0),
        Some(Contest::Fork {
            at: 580,
            witness: 0,
            hot: LineCheck::Unchecked
        })
    );
}

/// A witness whose `\n` at 599 is flipped holds one merged line that fails
/// its check against hot's Intact line: settled for hot, one against one.
/// Without checks the same flip is refused.
#[test]
fn d5_witness_newline_flip_is_settled_for_hot() {
    let hot = five(true);
    let cold_a = flipped(&hot, &[599]);
    let journals = Journals::new(&hot, &[&cold_a, &hot[..400]]);
    assert_eq!(journals.contest(400), None);
    let legacy = five(false);
    let journals = Journals::new(&legacy, &[&flipped(&legacy, &[599]), &legacy[..400]]);
    assert_eq!(journals.contest(400).map(|_| ()), Some(()));
}

/// A byte past hot's end is refused, as ADR-065 D8, checked or not.
#[test]
fn d5_past_hot_end_refuses() {
    for checked in [true, false] {
        let copy = five(checked);
        let journals = Journals::new(&copy[..800], &[&copy, &copy[..900]]);
        let expected = Contested {
            at: 800,
            covering: 2,
            with_hot: 0,
        };
        assert_eq!(journals.d8(0), Some(expected));
        assert_eq!(journals.contest(0), Some(Contest::Majority(expected)));
    }
}

/// A witness that cannot be read refuses (an error on the destination
/// side, never a panic); an absent one holds no byte; an unreadable hot is
/// an error on the source side.
#[test]
fn d5_unreadable_witness_refuses() {
    let hot = five(true);
    let journals = Journals::new(&hot, &[&flipped(&hot, &[450])]);
    let dir = journals.hot.parent().unwrap().to_owned();
    let error = journal_contest(&journals.hot, &[&dir], 0, u64::MAX, &[]).unwrap_err();
    assert_eq!(error.side, Side::Destination, "{error}");
    let absent = dir.join("absent");
    assert_eq!(
        journal_contest(&journals.hot, &[&absent], 0, u64::MAX, &[]).unwrap(),
        None
    );
    let error = journal_contest(&absent, &journals.refs(), 0, u64::MAX, &[]).unwrap_err();
    assert_eq!(error.side, Side::Source, "{error}");
}

/// On journals without checked lines D5 is exactly ADR-065 D8: 200
/// damage patterns from a seeded LCG (flips, `\n` flips, cuts and
/// extensions of hot and two witnesses, from a random line start).
#[test]
fn d5_unchecked_journal_matches_first_contested_file() {
    let mut state: u64 = 0x2024_0224;
    let mut next = |n: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % n
    };
    let base: Vec<u8> = (1..=8)
        .flat_map(|n| journal_line(n, 80 + 23 * n, false))
        .collect();
    let mut decided = [0usize; 2];
    for case in 0..200 {
        let variant = |bytes: &[u8], next: &mut dyn FnMut(u64) -> u64| {
            let mut bytes = bytes.to_vec();
            for _ in 0..next(3) {
                let at = next(bytes.len() as u64) as usize;
                bytes[at] = if next(4) == 0 {
                    b'\n'
                } else {
                    bytes[at] ^ 0x01
                };
            }
            match next(4) {
                0 => bytes.truncate(next(bytes.len() as u64) as usize),
                1 => bytes.extend(journal_line(9, 90, false)),
                _ => {}
            }
            bytes
        };
        let hot = variant(&base, &mut next);
        let a = variant(&base, &mut next);
        let b = variant(&base, &mut next);
        let journals = Journals::new(&hot, &[&a, &b]);
        let from = if hot.is_empty() {
            0
        } else {
            let at = next(hot.len() as u64) as usize;
            hot[..at]
                .iter()
                .rposition(|&byte| byte == b'\n')
                .map_or(0, |i| i as u64 + 1)
        };
        let d8 = journals.d8(from);
        let d5 = match journals.contest(from) {
            None => None,
            Some(Contest::Majority(contested)) => Some(contested),
            Some(other) => panic!("case {case}: {other:?} on journals without checks"),
        };
        assert_eq!(d5, d8, "case {case} from {from}");
        decided[usize::from(d8.is_some())] += 1;
    }
    // Both outcomes occur, so the comparison is not vacuous.
    assert!(decided[0] > 10 && decided[1] > 10, "{decided:?}");
}

/// R7 over files: cold-a's journal copy fails its check in line 3, cold-b's
/// is 400 bytes (no witness there): one against one, which ADR-065 D8
/// refuses and D5 rebuilds, since hot's line verifies. On the same
/// journal without checks R7 still refuses.
#[test]
fn r7_rebuilds_a_failing_copy_line_one_against_one() {
    for checked in [true, false] {
        let fixture = Fixture::new(if checked {
            "r7-d5-checked"
        } else {
            "r7-d5-legacy"
        });
        let journal = five(checked);
        fs::write(&fixture.config.paths.journal, &journal).unwrap();
        assert_eq!(fixture.pass().problems, Vec::<String>::new());
        let copy_a = fixture.cold[0].join(".mie/journal/journal.jsonl");
        let copy_b = fixture.cold[1].join(".mie/journal/journal.jsonl");
        flip(&copy_a, 450);
        fs::write(&copy_b, &journal[..400]).unwrap();
        let report = verify::run(
            &fixture.config,
            Some(&fixture.archive()),
            "24",
            &fixture.probe,
            &|| D0 + HOUR,
            &CopyLedger::at(&fixture.hot),
        );
        assert!(
            report
                .problems
                .iter()
                .any(|p| p.contains("(cold-a)") && p.contains("prefix: byte 450")),
            "{report:?}"
        );
        let report = repair(&fixture, D0 + 2 * HOUR);
        if checked {
            assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
            assert!(
                report
                    .lines
                    .iter()
                    .any(|l| l.contains("settled for hot") && l.contains("rebuilt up to byte 400")),
                "{report:?}"
            );
            assert_eq!(fs::read(&copy_a).unwrap(), journal);
        } else {
            assert!(
                report
                    .problems
                    .iter()
                    .any(|p| p.contains("cold-a") && p.contains("one against one")),
                "{report:?}"
            );
        }
    }
}

/// Hot's journal fails its check in line 3 where both copies hold the
/// line intact: R7 refuses each copy, naming hot as the suspect and `mie
/// store repair-hot --scope journal`.
#[test]
fn r7_refuses_while_hot_line_fails_its_check() {
    let fixture = Fixture::new("r7-d5-hot-fails");
    let journal = five(true);
    fs::write(&fixture.config.paths.journal, &journal).unwrap();
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    flip(&fixture.config.paths.journal, 450);
    let report = verify::run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| D0 + HOUR,
        &CopyLedger::at(&fixture.hot),
    );
    assert!(!report.problems.is_empty(), "{report:?}");
    let report = repair(&fixture, D0 + 2 * HOUR);
    for root in ["cold-a", "cold-b"] {
        assert!(
            report.problems.iter().any(|p| p.contains(root)
                && p.contains("refused")
                && p.contains("hot's line from byte 400")
                && p.contains("fails its check")
                && p.contains("mie store repair-hot --scope journal")),
            "{root}: {report:?}"
        );
    }
    for copy in &fixture.cold {
        assert_eq!(
            fs::read(copy.join(".mie/journal/journal.jsonl")).unwrap(),
            journal
        );
    }
}

// ------------------------------------- the lost-line record (ADR-066 D8)

/// The plan's journal of R7a: six 200-byte checked lines (starts 0, 200,
/// …, 1 000).
fn six() -> Vec<u8> {
    (1..=6).flat_map(|n| journal_line(n, 200, true)).collect()
}

/// The `lost-line` record of hot's line from `offset`, whose bytes with its
/// `\n` are `line`.
fn lost(number: u64, offset: u64, line: &[u8]) -> LostLine {
    LostLine {
        at_ms: D0,
        line: number,
        offset,
        sha256: copy_ledger::line_sha256(line.strip_suffix(b"\n").unwrap()),
    }
}

/// A fixture whose hot journal and both copies are `bytes`, after one
/// passing pass.
fn with_journal(tag: &str, bytes: &[u8]) -> Fixture {
    let fixture = Fixture::new(tag);
    fs::write(&fixture.config.paths.journal, bytes).unwrap();
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    for cold in fixture.cold.iter().filter(|_| !bytes.is_empty()) {
        assert_eq!(fs::read(copy_of(cold)).unwrap(), bytes);
    }
    fixture
}

fn copy_of(cold: &Path) -> PathBuf {
    cold.join(".mie/journal/journal.jsonl")
}

/// Writes `bytes` over the file at `path`, read-only or not.
fn overwrite(path: &Path, bytes: &[u8]) {
    if path.exists() {
        fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
    }
    fs::write(path, bytes).unwrap();
}

fn repair_flags(fixture: &Fixture, now: i64, record_lost_lines: bool) -> RepairReport {
    run(
        &fixture.config,
        Some(&fixture.archive()),
        Flags {
            record_lost_lines,
            ..Flags::default()
        },
        &fixture.probe,
        &|| now,
        &CopyLedger::at(&fixture.hot),
    )
}

fn verify_at(fixture: &Fixture, now: i64) -> verify::VerifyReport {
    verify::run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| now,
        &CopyLedger::at(&fixture.hot),
    )
}

/// Every `lost-line` record of hot's ledger: (at_ms, journal, line,
/// offset, sha256).
fn lost_records(fixture: &Fixture) -> Vec<(i64, String, u64, u64, String)> {
    fixture
        .records()
        .into_iter()
        .filter_map(|record| match record.kind {
            Kind::LostLine {
                journal,
                line,
                offset,
                sha256,
            } => Some((record.at_ms, journal, line, offset, sha256)),
            _ => None,
        })
        .collect()
}

#[track_caller]
fn assert_any(items: &[String], needles: &[&str]) {
    assert!(
        items.iter().any(|i| needles.iter().all(|n| i.contains(n))),
        "nothing with {needles:?} in {items:#?}"
    );
}

/// D5 with a record: hot's line 3 fails its check (a flip at 450) and is
/// recorded; cold-a's line 3 fails differently (460), cold-b holds hot's
/// bytes. Without the record D5 refuses (hot's line fails); with it the
/// line is settled for hot. Over files: verify's V4 names the record and
/// `mie store repair`, R7 makes cold-a hot's bytes, and the next verify
/// passes with the line as a note.
#[test]
fn d5_a_recorded_line_settles_a_failing_copy_line() {
    let good = five(true);
    let hot = flipped(&good, &[450]);
    let cold_a = flipped(&good, &[460]);
    let journals = Journals::new(&hot, &[&cold_a, &hot]);
    let record = lost(3, 400, &hot[400..600]);
    assert_eq!(
        journals.contest(0),
        Some(Contest::HotLineFails {
            at: 450,
            line_start: 400,
            recorded: false
        })
    );
    let refs = journals.refs();
    assert_eq!(
        journal_contest(
            &journals.hot,
            &refs,
            0,
            u64::MAX,
            std::slice::from_ref(&record)
        )
        .unwrap(),
        None
    );

    let fixture = with_journal("d5-recorded", &good);
    overwrite(&fixture.config.paths.journal, &hot);
    overwrite(&copy_of(&fixture.cold[0]), &cold_a);
    overwrite(&copy_of(&fixture.cold[1]), &hot);
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0,
            kind: Kind::LostLine {
                journal: "journal.jsonl".to_owned(),
                line: 3,
                offset: 400,
                sha256: record.sha256.clone(),
            },
        })
        .unwrap();
    let report = verify_at(&fixture, D0 + HOUR);
    assert_any(
        &report.problems,
        &[
            "(cold-a)",
            "prefix: byte 450",
            "recorded lost",
            "`mie store repair` makes the copy hot's bytes",
        ],
    );
    assert_any(
        &report.lines,
        &[
            "note:",
            "line 3 at byte 400",
            &format!("recorded lost at {D0}"),
        ],
    );
    let report = repair_flags(&fixture, D0 + 2 * HOUR, false);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    assert_any(&report.lines, &["line 3 at byte 400", "recorded lost at"]);
    assert_any(
        &report.lines,
        &["settled for hot", "rebuilt up to byte 400"],
    );
    for cold in &fixture.cold {
        assert_eq!(fs::read(copy_of(cold)).unwrap(), hot);
    }
    let report = verify_at(&fixture, D0 + 3 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
}

/// D5 with a record: a copy's line that verifies still refuses (a fork,
/// never overwritten), and so does a contradicting line without a check,
/// which a record does not settle.
#[test]
fn d5_a_recorded_line_still_refuses_a_verifying_copy_line() {
    let good = five(true);
    let hot = flipped(&good, &[450]);
    let record = [lost(3, 400, &hot[400..600])];
    let journals = Journals::new(&hot, &[&good, &hot]);
    assert_eq!(
        journal_contest(&journals.hot, &journals.refs(), 0, u64::MAX, &record).unwrap(),
        Some(Contest::Fork {
            at: 450,
            witness: 0,
            hot: LineCheck::Failed
        })
    );
    // cold-a holds hot's bytes with its member's name damaged at 580: its
    // line carries no check.
    let mut unchecked = hot.clone();
    unchecked[580] = b'C';
    let journals = Journals::new(&hot, &[&unchecked, &hot]);
    assert_eq!(
        journal_contest(&journals.hot, &journals.refs(), 0, u64::MAX, &record).unwrap(),
        Some(Contest::HotLineFails {
            at: 580,
            line_start: 400,
            recorded: true
        })
    );
}

/// A stale record (another digest, another offset) settles nothing: hot's
/// failed line refuses as without it.
#[test]
fn d5_a_stale_record_does_not_settle() {
    let good = five(true);
    let hot = flipped(&good, &[450]);
    let cold_a = flipped(&good, &[460]);
    let journals = Journals::new(&hot, &[&cold_a, &hot]);
    let refused = Some(Contest::HotLineFails {
        at: 450,
        line_start: 400,
        recorded: false,
    });
    for stale in [
        // The line's bytes before a further flip.
        lost(3, 400, &flipped(&hot, &[451])[400..600]),
        // The intact line's bytes (restored, then damaged again).
        lost(3, 400, &good[400..600]),
        // The right bytes at another offset.
        lost(4, 600, &hot[400..600]),
    ] {
        assert_eq!(
            journal_contest(&journals.hot, &journals.refs(), 0, u64::MAX, &[stale]).unwrap(),
            refused
        );
    }
}

/// The plan's geometry: hot 1 200 B in 200-B lines, both copies 1 000 B,
/// hot flipped at 1 050 in line 6, which no copy holds. With the flag the
/// repair appends exactly `lost-line <at_ms> journal.jsonl 6 1000
/// <sha256>`, and the next verify passes, listing the line as a note.
#[test]
fn record_lost_lines_records_a_line_no_copy_holds() {
    let six = six();
    let fixture = with_journal("r7a-records", &six[..1000]);
    let hot = flipped(&six, &[1050]);
    overwrite(&fixture.config.paths.journal, &hot);
    let report = verify_at(&fixture, D0 + HOUR);
    assert_any(
        &report.problems,
        &[
            "hot's journal line 6 at byte 1000 fails its check",
            "not recorded lost",
            "`mie store repair-hot --scope journal`",
            "`mie store repair --record-lost-lines`",
        ],
    );
    let at = D0 + 2 * HOUR;
    let report = repair_flags(&fixture, at, true);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    assert_any(
        &report.lines,
        &[
            "line 6 at byte 1000",
            "record is appended (--record-lost-lines)",
        ],
    );
    let sha = copy_ledger::line_sha256(&hot[1000..1199]);
    assert_eq!(
        lost_records(&fixture),
        [(at, "journal.jsonl".to_owned(), 6, 1000, sha.clone())]
    );
    let ledger = fs::read_to_string(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    assert!(
        ledger.contains(&format!("\nlost-line {at} journal.jsonl 6 1000 {sha} ")),
        "{ledger}"
    );
    let report = verify_at(&fixture, D0 + 3 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    assert_any(
        &report.lines,
        &[
            "note:",
            "line 6 at byte 1000",
            &format!("recorded lost at {at}"),
        ],
    );
    assert_any(
        &report.lines,
        &["6 complete line(s) checked: 5 intact, 0 without the check, 1 failed (1 recorded lost)"],
    );
}

/// Without the flag the repair names the recordable line, fails, and
/// appends nothing.
#[test]
fn record_lost_lines_without_the_flag_records_nothing() {
    let six = six();
    let fixture = with_journal("r7a-no-flag", &six[..1000]);
    overwrite(&fixture.config.paths.journal, &flipped(&six, &[1050]));
    let before = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    let report = repair_flags(&fixture, D0 + 2 * HOUR, false);
    assert_any(
        &report.problems,
        &[
            "line 6 at byte 1000",
            "can be recorded",
            "Nothing is recorded",
            "`mie store repair --record-lost-lines`",
        ],
    );
    assert!(lost_records(&fixture).is_empty());
    let after = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    assert!(after.starts_with(&before));
    assert!(lost_records(&fixture).is_empty());
}

/// D8's condition 4, the plan's table: hot 1 200 B, its line 6 failed at
/// [1 000, 1 199), its `\n` at 1 199; cold-b 1 000 B, cold-a as listed.
#[test]
fn record_lost_lines_judges_each_copy_span() {
    let six = six();
    let hot = flipped(&six, &[1050]);
    let mut other = hot[..1100].to_vec();
    other[1020] ^= 0x01;
    // Line 6 intact, its `\n` flipped: one merged line [1 000, 1 400) with
    // line 7 that fails its check and holds the true line's bytes.
    let mut merged = six.clone();
    merged[1199] ^= 0x01;
    merged.extend(journal_line(7, 200, true));
    let cases: [(&str, Vec<u8>, bool); 7] = [
        ("absent", six[..1000].to_vec(), true),
        ("equal-cut", hot[..1100].to_vec(), true),
        ("other-cut", other, false),
        ("other-failed", flipped(&six, &[1060]), true),
        ("intact", six.clone(), false),
        // A `\n` at 1 100: a line [1 000, 1 101) without a check.
        (
            "split",
            {
                let mut split = six.clone();
                split[1100] = b'\n';
                split
            },
            false,
        ),
        ("merged", merged, false),
    ];
    for (name, cold_a, recordable) in cases {
        let fixture = with_journal(&format!("r7a-span-{name}"), &six[..1000]);
        overwrite(&fixture.config.paths.journal, &hot);
        overwrite(&copy_of(&fixture.cold[0]), &cold_a);
        let report = repair_flags(&fixture, D0 + 2 * HOUR, false);
        if recordable {
            assert_any(
                &report.problems,
                &["line 6 at byte 1000", "can be recorded"],
            );
        } else {
            assert_any(
                &report.problems,
                &[
                    "line 6 at byte 1000",
                    "not recordable",
                    "cold-a",
                    "`mie store repair-hot --scope journal`",
                ],
            );
            assert!(
                !report
                    .problems
                    .iter()
                    .any(|p| p.contains("can be recorded")),
                "{name}: {report:#?}"
            );
        }
        let report = repair_flags(&fixture, D0 + 3 * HOUR, true);
        assert_eq!(
            lost_records(&fixture).len(),
            usize::from(recordable),
            "{name}: {report:#?}"
        );
    }
}

/// D8's conditions 2 and 3: a copy target without its marker, or a journal
/// copy that cannot be read, makes the line "not recordable in this run",
/// and nothing is appended.
#[test]
fn record_lost_lines_refuses_while_a_target_lacks_its_marker() {
    let six = six();
    let fixture = with_journal("r7a-marker", &six[..1000]);
    overwrite(&fixture.config.paths.journal, &flipped(&six, &[1050]));
    let marker = root_marker::marker_path(&fixture.cold[1]);
    let saved = fs::read(&marker).unwrap();
    fs::remove_file(&marker).unwrap();
    let report = repair_flags(&fixture, D0 + 2 * HOUR, true);
    assert_any(
        &report.problems,
        &[
            "line 6 at byte 1000",
            "not recordable in this run",
            "presented no marker",
        ],
    );
    assert!(lost_records(&fixture).is_empty());
    fs::write(&marker, saved).unwrap();

    // The marker gone between the start of the run and the decision.
    BEFORE_WORK.with(|hook| {
        let marker = marker.clone();
        *hook.borrow_mut() = Some(Box::new(move || {
            fs::rename(&marker, marker.with_extension("away")).unwrap();
        }));
    });
    let report = repair_flags(&fixture, D0 + 3 * HOUR, true);
    fs::rename(marker.with_extension("away"), &marker).unwrap();
    assert_any(
        &report.problems,
        &[
            "line 6 at byte 1000",
            "not recordable in this run",
            "no longer presents its marker",
        ],
    );
    assert!(lost_records(&fixture).is_empty());

    // cold-a's journal copy, 1 200 bytes, cannot be read.
    let copy = copy_of(&fixture.cold[0]);
    assert_eq!(fs::metadata(&copy).unwrap().len(), 1200);
    fs::set_permissions(&copy, fs::Permissions::from_mode(0o000)).unwrap();
    let report = repair_flags(&fixture, D0 + 4 * HOUR, true);
    fs::set_permissions(&copy, fs::Permissions::from_mode(0o644)).unwrap();
    assert_any(
        &report.problems,
        &[
            "line 6 at byte 1000",
            "not recordable in this run",
            "cannot be read",
        ],
    );
    assert!(lost_records(&fixture).is_empty());
}

/// A journal name that is not UTF-8 cannot be named by a record: refused
/// before any append.
#[test]
fn record_lost_lines_refuses_a_non_utf8_journal_name() {
    use std::os::unix::ffi::OsStrExt;
    let mut fixture = Fixture::new("r7a-non-utf8");
    let name = std::ffi::OsStr::from_bytes(b"journal-\xff.jsonl");
    fixture.config.paths.journal = fixture.dir.0.join(name);
    let six = six();
    fs::write(&fixture.config.paths.journal, &six[..1000]).unwrap();
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    overwrite(&fixture.config.paths.journal, &flipped(&six, &[1050]));
    let before = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    let report = repair_flags(&fixture, D0 + 2 * HOUR, true);
    assert_any(
        &report.problems,
        &["line 6 at byte 1000", "is not UTF-8", "nothing is recorded"],
    );
    let after = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    assert!(after.starts_with(&before));
    assert!(lost_records(&fixture).is_empty());
}

/// Line 1 at byte 0 and the last complete line are recordable; the last
/// line without its `\n`, a damaged checked line that may be in the middle
/// of being written, never is. The copies hold nothing of the journal.
#[test]
fn record_lost_lines_head_and_tail() {
    let six = six();
    let mut hot = flipped(&six, &[50, 1150]);
    let mut partial = journal_line(7, 200, true);
    partial.pop();
    partial[30] ^= 0x01;
    hot.extend_from_slice(&partial);
    let fixture = with_journal("r7a-head-tail", b"");
    overwrite(&fixture.config.paths.journal, &hot);
    let at = D0 + 2 * HOUR;
    let report = repair_flags(&fixture, at, true);
    assert!(
        !report.problems.iter().any(|p| p.contains("line 7")),
        "{report:#?}"
    );
    let records: Vec<(u64, u64)> = lost_records(&fixture)
        .into_iter()
        .map(|(_, _, line, offset, _)| (line, offset))
        .collect();
    assert_eq!(records, [(1, 0), (6, 1000)]);
}

/// Two byte-identical damaged lines at different offsets get two records,
/// keyed by offset.
#[test]
fn record_lost_lines_keys_by_offset() {
    let damaged = flipped(&journal_line(1, 200, true), &[50]);
    let hot = [damaged.clone(), damaged.clone(), journal_line(3, 200, true)].concat();
    let fixture = with_journal("r7a-keys", b"");
    overwrite(&fixture.config.paths.journal, &hot);
    repair_flags(&fixture, D0 + 2 * HOUR, true);
    let records = lost_records(&fixture);
    assert_eq!(
        records
            .iter()
            .map(|(_, _, line, offset, _)| (*line, *offset))
            .collect::<Vec<_>>(),
        [(1, 0), (2, 200)]
    );
    assert_eq!(records[0].4, records[1].4);
}

/// A second run appends nothing: the line a record matches is reported
/// recorded.
#[test]
fn record_lost_lines_is_idempotent() {
    let six = six();
    let fixture = with_journal("r7a-idempotent", &six[..1000]);
    overwrite(&fixture.config.paths.journal, &flipped(&six, &[1050]));
    repair_flags(&fixture, D0 + 2 * HOUR, true);
    assert_eq!(lost_records(&fixture).len(), 1);
    let report = repair_flags(&fixture, D0 + 3 * HOUR, true);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    assert_any(
        &report.lines,
        &[
            "line 6 at byte 1000",
            &format!("recorded lost at {}", D0 + 2 * HOUR),
        ],
    );
    assert_eq!(lost_records(&fixture).len(), 1);
}

/// Lines without the check (old, torn, damaged in the member) and an
/// unterminated last line are never recorded, and name nothing.
#[test]
fn record_lost_lines_never_records_unchecked_or_unterminated_lines() {
    let mut hot = flipped(&five(false), &[50, 450]);
    // A torn checked line that #225 ended with `\n`.
    let torn = journal_line(6, 200, true);
    hot.extend_from_slice(&torn[..120]);
    hot.push(b'\n');
    // A checked line whose member's name is damaged.
    let mut member = journal_line(7, 200, true);
    member[182] = b'C';
    hot.extend_from_slice(&member);
    // A damaged checked line without its `\n`.
    let mut partial = flipped(&journal_line(8, 200, true), &[40]);
    partial.pop();
    hot.extend_from_slice(&partial);
    let fixture = with_journal("r7a-unchecked", b"");
    overwrite(&fixture.config.paths.journal, &hot);
    let report = repair_flags(&fixture, D0 + 2 * HOUR, true);
    assert!(
        !report
            .problems
            .iter()
            .any(|p| p.contains("fails its check")),
        "{report:#?}"
    );
    assert!(lost_records(&fixture).is_empty());
    let report = verify_at(&fixture, D0 + 3 * HOUR);
    assert_any(
        &report.lines,
        &[
            "7 complete line(s) checked: 0 intact, 7 without the check, 0 failed",
            "its last 199 byte(s)",
        ],
    );
}

/// The record writes no journal byte, so it runs beside a capture: an open
/// `Journal` holds the journal lock, and the repair still records.
#[test]
fn record_lost_lines_runs_beside_a_capture() {
    let six = six();
    let fixture = with_journal("r7a-capture", &six[..1000]);
    overwrite(&fixture.config.paths.journal, &flipped(&six, &[1050]));
    let journal = crate::journal::Journal::open(&fixture.config.paths.journal, "live").unwrap();
    assert!(replica::try_journal_lock(&fixture.config.paths.journal).is_err());
    repair_flags(&fixture, D0 + 2 * HOUR, true);
    drop(journal);
    assert_eq!(lost_records(&fixture).len(), 1);
}

// ------------------------------------------- review of #233 (ADR-066 D8)

/// Appends the `lost-line` record of hot's line 3 of [`five`] (bytes
/// `[400, 600)` of `hot`).
fn record_line_3(fixture: &Fixture, hot: &[u8]) {
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0,
            kind: Kind::LostLine {
                journal: "journal.jsonl".to_owned(),
                line: 3,
                offset: 400,
                sha256: copy_ledger::line_sha256(&hot[400..599]),
            },
        })
        .unwrap();
}

/// A record settles hot's line only against copy lines that fail their
/// check over the same span, as recording judges them (D8 condition 4).
/// cold-a holds line 3 as written with its `\n` at 599 flipped: one merged
/// line [400, 800) that fails its check and holds the true line's bytes.
/// D5 refuses (hot's line recorded, the copy's line of another span), V4
/// does not point to `mie store repair`, and R7 leaves cold-a untouched.
#[test]
fn d5_a_recorded_line_does_not_settle_a_copy_line_of_another_span() {
    let good = five(true);
    let hot = flipped(&good, &[450]);
    let merged = flipped(&good, &[599]);
    let record = [lost(3, 400, &hot[400..600])];
    let journals = Journals::new(&hot, &[&merged]);
    assert_eq!(
        journal_contest(&journals.hot, &journals.refs(), 0, u64::MAX, &record).unwrap(),
        Some(Contest::HotLineFails {
            at: 450,
            line_start: 400,
            recorded: true
        })
    );
    // The same span still settles (the line fails differently).
    let same = flipped(&good, &[460]);
    let journals = Journals::new(&hot, &[&same]);
    assert_eq!(
        journal_contest(&journals.hot, &journals.refs(), 0, u64::MAX, &record).unwrap(),
        None
    );

    let fixture = with_journal("d5-recorded-span", &good);
    overwrite(&fixture.config.paths.journal, &hot);
    overwrite(&copy_of(&fixture.cold[0]), &merged);
    overwrite(&copy_of(&fixture.cold[1]), &hot);
    record_line_3(&fixture, &hot);
    let report = verify_at(&fixture, D0 + HOUR);
    assert_any(&report.problems, &["(cold-a)", "prefix: byte 450"]);
    assert!(
        !report
            .problems
            .iter()
            .any(|p| p.contains("makes the copy hot's bytes")),
        "{report:#?}"
    );
    let report = repair_flags(&fixture, D0 + 2 * HOUR, false);
    assert_any(
        &report.problems,
        &[
            "cold-a",
            "refused",
            "is recorded lost",
            "of the same span",
            "mie store repair-hot --scope journal",
        ],
    );
    assert_eq!(fs::read(copy_of(&fixture.cold[0])).unwrap(), merged);
}

/// R7's fork refusal follows hot's verdict: against a copy line that
/// verifies, a recorded hot line is named as failing its check (repair-hot
/// restores it from the copy), never as carrying no check.
#[test]
fn r7_fork_text_names_a_recorded_hot_line_as_failing() {
    let good = five(true);
    let hot = flipped(&good, &[450]);
    let fixture = with_journal("r7-fork-recorded", &good);
    overwrite(&fixture.config.paths.journal, &hot);
    overwrite(&copy_of(&fixture.cold[1]), &hot);
    record_line_3(&fixture, &hot);
    let report = verify_at(&fixture, D0 + HOUR);
    assert_any(&report.problems, &["(cold-a)", "prefix: byte 450"]);
    let report = repair_flags(&fixture, D0 + 2 * HOUR, false);
    assert_any(
        &report.problems,
        &[
            "cold-a",
            "refused",
            "verifies its check",
            "hot's line there fails its check and is recorded lost",
            "mie store repair-hot --scope journal",
        ],
    );
    assert!(
        !report
            .problems
            .iter()
            .any(|p| p.contains("carries no check, and a line that verifies")),
        "{report:#?}"
    );
    assert_eq!(fs::read(copy_of(&fixture.cold[0])).unwrap(), good);
}

/// D8 condition 2 at the decision: a target unmounted after its journal
/// copy was read (its marker and its copy gone together) refuses the
/// record, since the markers are read again after the copies.
#[test]
fn record_lost_lines_refuses_a_target_unmounted_after_the_copy_reads() {
    let six = six();
    let fixture = with_journal("r7a-unmount", &six[..1000]);
    overwrite(&fixture.config.paths.journal, &flipped(&six, &[1050]));
    let cold_b = fixture.cold[1].clone();
    let away = cold_b.with_extension("away");
    BEFORE_MARKER_REREAD.with(|hook| {
        let (cold_b, away) = (cold_b.clone(), away.clone());
        *hook.borrow_mut() = Some(Box::new(move || {
            fs::rename(&cold_b, &away).unwrap();
            fs::create_dir(&cold_b).unwrap();
        }));
    });
    let report = repair_flags(&fixture, D0 + 2 * HOUR, true);
    BEFORE_MARKER_REREAD.with(|hook| assert!(hook.borrow().is_none(), "the hook never ran"));
    fs::remove_dir(&cold_b).unwrap();
    fs::rename(&away, &cold_b).unwrap();
    assert_any(
        &report.problems,
        &[
            "line 6 at byte 1000",
            "not recordable in this run",
            "no longer presents its marker",
        ],
    );
    assert!(lost_records(&fixture).is_empty());
}

// ------------------------------------------- R5a, R5b (ADR-067, #227)

/// A replicated fixture whose cold-b holds `text` as the manifest of the
/// first live path beside its data, found by a verification. Returns the
/// path.
fn other_on_cold_b(tag: &str, text: impl Fn(&Path, &str) -> String) -> (Fixture, String) {
    let fixture = Fixture::new(tag);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let path = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    plant_other_file(&fixture.cold[1], &path, &text(&fixture.hot, &path));
    assert!(!verified(&fixture, D0 + HOUR).is_empty());
    (fixture, path)
}

/// The inodes of the data file and manifest of `path` on `root`.
fn inodes(root: &Path, path: &str) -> [u64; 2] {
    use std::os::unix::fs::MetadataExt;
    [path.to_owned(), manifest_of(path)].map(|name| fs::metadata(root.join(name)).unwrap().ino())
}

fn append_displaced(fixture: &Fixture, root: &str, dir: i64, text: &str) {
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: dir,
            kind: Kind::Displaced {
                root: root.to_owned(),
                dir,
                text: text.to_owned(),
            },
        })
        .unwrap();
}

/// R5b: a crash between the move's two renames leaves the other file's
/// data in the record's directory and its manifest in place. The next run,
/// without the flag, moves the manifest to its data (never into its own
/// directory, never judged as a manifest without its data), appends no
/// record, and repairs the path from hot.
#[test]
fn r5b_a_manifest_left_between_the_renames_joins_its_data() {
    let (fixture, path) = other_on_cold_b("r5b-between", other_text);
    let cold_b = &fixture.cold[1];
    let text = fs::read_to_string(cold_b.join(manifest_of(&path))).unwrap();
    let recorded = D0 + HOUR + 1;
    append_displaced(&fixture, "cold-b", recorded, &text);
    let aside = replica::damaged_dir(cold_b, recorded);
    fs::create_dir_all(aside.join(&path).parent().unwrap()).unwrap();
    fs::rename(cold_b.join(&path), aside.join(&path)).unwrap();
    let report = repair(&fixture, D0 + 2 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert_eq!(
        fs::read_to_string(aside.join(manifest_of(&path))).unwrap(),
        text
    );
    assert!(aside.join(&path).exists());
    assert!(!replica::damaged_dir(cold_b, D0 + 2 * HOUR).exists());
    assert_eq!(displaced(&fixture).len(), 1);
    assert_eq!(
        fs::read(cold_b.join(&path)).unwrap(),
        fs::read(fixture.hot.join(&path)).unwrap()
    );
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.contains(&path) && l.contains("completed the recorded move")),
        "{report:?}"
    );
}

/// The adversarial case of ADR-067 D4: an other file with the rows and the
/// SHA-256 of the recorded file, its text differing only in
/// `max_event_time_ms`, moves with the flag; afterwards the canonical copy
/// the repair placed there is never moved, with the flag or without, since
/// a record matches only a byte-equal text. A second D5 violation later
/// (another text) needs the flag again and gets its own record, and the
/// same other file on both targets gets one record per root.
#[test]
fn r5a_same_rows_and_sha_other_text_never_moves_the_canonical_copy() {
    let later_max = |hot: &Path, path: &str| {
        let text = fs::read_to_string(hot.join(manifest_of(path))).unwrap();
        let max = text
            .lines()
            .find_map(|line| line.strip_prefix("max_event_time_ms "))
            .unwrap()
            .to_owned();
        text.replace(
            &format!("max_event_time_ms {max}\n"),
            &format!("max_event_time_ms {}\n", max.parse::<i64>().unwrap() + 1),
        )
    };
    let (fixture, path) = other_on_cold_b("r5a-same-sha", later_max);
    let cold_b = fixture.cold[1].clone();
    let other = fs::read_to_string(cold_b.join(manifest_of(&path))).unwrap();
    let canonical = fs::read_to_string(fixture.hot.join(manifest_of(&path))).unwrap();
    let field = |text: &str, name: &str| {
        text.lines()
            .find(|line| line.starts_with(&format!("{name} ")))
            .unwrap()
            .to_owned()
    };
    assert_ne!(other, canonical);
    for name in ["rows", "bytes", "sha256"] {
        assert_eq!(field(&other, name), field(&canonical, name));
    }
    let report = repair_moving(&fixture, D0 + 2 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert_eq!(
        displaced(&fixture),
        [("cold-b".to_owned(), D0 + 2 * HOUR, other.clone())]
    );
    let placed = inodes(&cold_b, &path);
    for (n, flags) in [Flags::default(), moving()].into_iter().enumerate() {
        // An open finding makes R1, and so R5b, visit the canonical copy.
        CopyLedger::at(&fixture.hot)
            .append(&Record {
                at_ms: D0 + (2 + n as i64) * HOUR + 1,
                kind: Kind::Finding {
                    root: "cold-b".to_owned(),
                    code: copy_ledger::FindingCode::Stale,
                    subject: copy_ledger::Subject::File(path.clone()),
                },
            })
            .unwrap();
        let report = run(
            &fixture.config,
            Some(&fixture.archive()),
            flags,
            &fixture.probe,
            &|| D0 + (3 + n as i64) * HOUR,
            &CopyLedger::at(&fixture.hot),
        );
        assert_eq!(report.problems, Vec::<String>::new(), "{n}: {report:?}");
        assert!(
            report
                .lines
                .iter()
                .any(|l| l.contains(&path) && l.contains("re-verified good")),
            "{n}: {report:?}"
        );
        assert_eq!(inodes(&cold_b, &path), placed, "{n}");
        assert_eq!(displaced(&fixture).len(), 1, "{n}");
    }

    // A second D5 violation: another text under the path, on both
    // targets. Without the flag it is refused; with it, one record per
    // root, each with that text.
    let second = other_text(&fixture.hot, &path);
    for root in &fixture.cold {
        plant_other_file(root, &path, &second);
    }
    assert!(!verified(&fixture, D0 + 5 * HOUR).is_empty());
    let report = repair(&fixture, D0 + 6 * HOUR);
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains(&path) && p.contains("--move-other-files")),
        "{report:?}"
    );
    assert_eq!(displaced(&fixture).len(), 1);
    let report = repair_moving(&fixture, D0 + 7 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let records = displaced(&fixture);
    assert_eq!(
        records[1..],
        [
            ("cold-a".to_owned(), D0 + 7 * HOUR, second.clone()),
            ("cold-b".to_owned(), D0 + 7 * HOUR, second)
        ]
    );
    for root in &fixture.cold {
        assert_eq!(
            fs::read_to_string(root.join(manifest_of(&path))).unwrap(),
            canonical
        );
    }
    assert_eq!(verified(&fixture, D0 + 8 * HOUR), Vec::<String>::new());
}

/// R5a gate 5: a target whose marker is gone after the run's marker reads
/// gets no record and no move, and the marker is named.
#[test]
fn r5a_marker_gone_before_the_record_writes_nothing() {
    let (fixture, path) = other_on_cold_b("r5a-marker", other_text);
    let cold_b = fixture.cold[1].clone();
    let before = inodes(&cold_b, &path);
    let marker = root_marker::marker_path(&cold_b);
    let hidden = fixture.dir.0.join("cold-b.marker");
    {
        let (marker, hidden) = (marker.clone(), hidden.clone());
        BEFORE_WORK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || fs::rename(&marker, &hidden).unwrap()));
        });
    }
    let report = repair_moving(&fixture, D0 + 2 * HOUR);
    fs::rename(&hidden, &marker).unwrap();
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains("cold-b") && p.contains("marker")),
        "{report:?}"
    );
    assert_eq!(inodes(&cold_b, &path), before);
    assert!(displaced(&fixture).is_empty());
    assert!(!replica::damaged_dir(&cold_b, D0 + 2 * HOUR).exists());
}

/// R5a gate 6: a name of the path already taken below the run's
/// `.mie/damaged/<at_ms>/` (planted by hand, or the same millisecond)
/// records nothing and moves nothing; a run at a later `at_ms` moves the
/// file. The completion variant: the record's own directory already holds
/// the data name, so R5b's move refuses, names it, and overwrites nothing.
#[test]
fn r5a_a_taken_evidence_name_records_nothing() {
    let (fixture, path) = other_on_cold_b("r5a-taken", other_text);
    let cold_b = fixture.cold[1].clone();
    let at = D0 + 2 * HOUR;
    let before = inodes(&cold_b, &path);
    for name in [
        path.clone(),
        manifest_of(&path),
        temp_of(&path, TempName::Data),
        temp_of(&path, TempName::Manifest),
    ] {
        let taken = replica::damaged_dir(&cold_b, at).join(&name);
        plant(
            &cold_b,
            &rel(&cold_b, &taken),
            &Plant::Bytes(b"evidence".to_vec()),
        );
        let report = repair_moving(&fixture, at);
        assert!(
            report
                .problems
                .iter()
                .any(|p| p.contains(&path) && p.contains("cold-b") && p.contains("already")),
            "{name}: {report:?}"
        );
        assert_eq!(inodes(&cold_b, &path), before, "{name}");
        assert!(displaced(&fixture).is_empty(), "{name}");
        assert_eq!(fs::read(&taken).unwrap(), b"evidence", "{name}");
        fs::remove_file(&taken).unwrap();
    }
    let report = repair_moving(&fixture, at + 1);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    assert_eq!(
        inodes(&replica::damaged_dir(&cold_b, at + 1), &path),
        before
    );

    // Completion: a record whose directory already holds the data name.
    let (fixture, path) = other_on_cold_b("r5b-taken", other_text);
    let cold_b = fixture.cold[1].clone();
    let text = fs::read_to_string(cold_b.join(manifest_of(&path))).unwrap();
    let recorded = D0 + HOUR + 1;
    append_displaced(&fixture, "cold-b", recorded, &text);
    let taken = replica::damaged_dir(&cold_b, recorded).join(&path);
    plant(
        &cold_b,
        &rel(&cold_b, &taken),
        &Plant::Bytes(b"evidence".to_vec()),
    );
    let before = inodes(&cold_b, &path);
    let report = repair(&fixture, D0 + 2 * HOUR);
    assert!(
        report.problems.iter().any(|p| p.contains(&path)
            && p.contains("cold-b")
            && p.contains("completing the recorded move")),
        "{report:?}"
    );
    assert_eq!(inodes(&cold_b, &path), before);
    assert_eq!(fs::read(&taken).unwrap(), b"evidence");
    assert_eq!(displaced(&fixture).len(), 1);
}

/// `name` relative to `root`, as [`plant`] takes it.
fn rel(root: &Path, name: &Path) -> String {
    name.strip_prefix(root).unwrap().display().to_string()
}

/// An appender that fails every `displaced` append.
struct FailingDisplaced<'a>(&'a CopyLedger);

impl LedgerAppend for FailingDisplaced<'_> {
    fn path(&self) -> &Path {
        self.0.path()
    }

    fn append(
        &self,
        record: &Record,
    ) -> Result<copy_ledger::AppendOutcome, copy_ledger::AppendError> {
        if matches!(record.kind, Kind::Displaced { .. }) {
            return Err(copy_ledger::AppendError {
                error: RawStoreError::Io("injected: no space left on device".to_owned()),
                truncation: None,
            });
        }
        self.0.append(record)
    }

    fn append_unless(
        &self,
        record: &Record,
        offset: u64,
        chain: &str,
        veto: &dyn Fn(&[Record]) -> Option<String>,
    ) -> Result<copy_ledger::Guarded, copy_ledger::AppendError> {
        self.0.append_unless(record, offset, chain, veto)
    }
}

/// The record comes before the move: a failed `displaced` append stops
/// the run, and the other file stays in place.
#[test]
fn r5a_a_failed_append_moves_nothing() {
    let (fixture, path) = other_on_cold_b("r5a-append-fails", other_text);
    let cold_b = fixture.cold[1].clone();
    let before = inodes(&cold_b, &path);
    let ledger = CopyLedger::at(&fixture.hot);
    let report = run(
        &fixture.config,
        Some(&fixture.archive()),
        moving(),
        &fixture.probe,
        &|| D0 + 2 * HOUR,
        &FailingDisplaced(&ledger),
    );
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains("the displaced append") && p.contains("the repair stops")),
        "{report:?}"
    );
    assert_eq!(inodes(&cold_b, &path), before);
    assert!(displaced(&fixture).is_empty());
    assert!(!replica::damaged_dir(&cold_b, D0 + 2 * HOUR).exists());
}

/// A partition recorded removed while hot still holds it (a hot removal
/// that crashed before its delete): R2b never judges its forms, so an
/// other file on a target without a `copy` record of the path stays and
/// nothing is recorded, even with the flag.
#[test]
fn r5a_removed_partition_on_hot_is_not_judged() {
    let fixture = Fixture::new("r5a-removed");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-um", "aggTrade", D0 + 6_000, 2);
    assert_eq!(new.len(), 1, "{new:?}");
    let path = new[0].clone();
    let cold_b = fixture.cold[1].clone();
    plant(
        &cold_b,
        &path,
        &Plant::Bytes(fs::read(fixture.hot.join(&path)).unwrap()),
    );
    plant_other_file(&cold_b, &path, &other_text(&fixture.hot, &path));
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + HOUR,
            kind: Kind::Removed {
                partition: partition_of(&path).to_owned(),
            },
        })
        .unwrap();
    let before = inodes(&cold_b, &path);
    let report = repair_moving(&fixture, D0 + 2 * HOUR);
    assert_eq!(inodes(&cold_b, &path), before, "{report:?}");
    assert!(displaced(&fixture).is_empty(), "{report:?}");
    assert!(
        !replica::damaged_dir(&cold_b, D0 + 2 * HOUR)
            .join(&path)
            .exists()
    );
}

/// A replicated fixture whose cold-b holds a self-consistent other file
/// under the first path of `source`, found by a verification, with its
/// `displaced` record appended and the move not done (a crash after the
/// record, or a failed rename). Returns the path and the record's text.
fn incomplete_move(tag: &str, source: &str) -> (Fixture, String, String) {
    let fixture = Fixture::new(tag);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let path = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with(&format!("source={source}/")))
        .unwrap();
    let text = other_text(&fixture.hot, &path);
    plant_other_file(&fixture.cold[1], &path, &text);
    assert!(!verified(&fixture, D0 + HOUR).is_empty());
    append_displaced(&fixture, "cold-b", D0 + HOUR + 1, &text);
    (fixture, path, text)
}

/// R5b under gate 1 (review of #234, finding 2): a path held after its
/// `displaced` record was appended. R1 visits it (its finding is open),
/// but the recorded move is not completed before `mie store resolve`
/// decides the hold: the repair names the hold resolution and moves
/// nothing, and verify's `FAIL:` names `mie store resolve` first.
#[test]
fn r5b_a_held_path_is_not_completed_before_resolve() {
    let (fixture, path, text) = incomplete_move("r5b-held", "binance-archive");
    let cold_b = fixture.cold[1].clone();
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + HOUR + 2,
            kind: Kind::Hold {
                path: path.clone(),
                reason: copy_ledger::HoldReason::EntryConflict,
            },
        })
        .unwrap();
    let report = verify_at(&fixture, D0 + HOUR + 3);
    assert_any(
        &report.problems,
        &[
            &path,
            "cold-b",
            "did not complete",
            "the path is held",
            "`mie store resolve`",
        ],
    );
    let before = inodes(&cold_b, &path);
    let report = repair_moving(&fixture, D0 + 2 * HOUR);
    assert!(
        !replica::damaged_dir(&cold_b, D0 + HOUR + 1)
            .join(manifest_of(&path))
            .exists(),
        "a held path's other file moved before the resolution: {report:?}"
    );
    assert_eq!(inodes(&cold_b, &path), before, "{report:?}");
    assert_eq!(
        fs::read_to_string(cold_b.join(manifest_of(&path))).unwrap(),
        text
    );
    assert_any(
        &report.problems,
        &[&path, "cold-b", "the path is held", "`mie store resolve`"],
    );
    assert_eq!(displaced(&fixture).len(), 1, "{report:?}");
}

/// R5b and the lost-file rule (review of #234, finding 2): a path
/// recorded lost after its `displaced` record was appended. No repair
/// rule visits a lost path (nothing is written for it, ADR-062 D11), so
/// verify never fails naming a repair that never acts: the incomplete
/// move is a note naming the lost record, and a repair with the flag
/// leaves the file in place and appends nothing.
#[test]
fn r5b_a_lost_path_is_a_note_and_never_moved() {
    let (fixture, path, text) = incomplete_move("r5b-lost", "binance-um");
    let cold_b = fixture.cold[1].clone();
    let sha =
        replica::parse_manifest(&fs::read_to_string(fixture.hot.join(manifest_of(&path))).unwrap())
            .unwrap()
            .sha256;
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + HOUR + 2,
            kind: Kind::Lost {
                path: path.clone(),
                origin: LostOrigin::Verify,
                sha256: Some(sha),
            },
        })
        .unwrap();
    let report = verify_at(&fixture, D0 + HOUR + 3);
    assert!(
        !report
            .problems
            .iter()
            .any(|p| p.contains(&path) && p.contains("did not complete")),
        "{report:?}"
    );
    assert_any(
        &report.lines,
        &[&path, "cold-b", "did not complete", "recorded lost"],
    );
    let before = inodes(&cold_b, &path);
    let len = fixture.records().len();
    let report = repair_moving(&fixture, D0 + 2 * HOUR);
    assert_eq!(inodes(&cold_b, &path), before, "{report:?}");
    assert_eq!(
        fs::read_to_string(cold_b.join(manifest_of(&path))).unwrap(),
        text
    );
    assert!(
        !fixture.records()[len..].iter().any(|r| match &r.kind {
            Kind::Displaced { .. } => true,
            Kind::Copy { path: copied, .. } => *copied == path,
            _ => false,
        }),
        "{report:?}"
    );
}
