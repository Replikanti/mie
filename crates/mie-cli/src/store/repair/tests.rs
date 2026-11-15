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
/// copy on cold-a and a damaged hot archive part, found by a verification.
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
        false,
        &fixture.probe,
        &|| now,
        &CopyLedger::at(&fixture.hot),
    )
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
#[test]
fn a_crash_at_every_repair_step_is_completed_by_the_next_run() {
    let reference = damaged("repair-crash-reference");
    let report = repair(&reference, D0 + 2 * HOUR);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
    let expected = trees(&reference);
    for step in RepairStep::ALL {
        let fixture = damaged(&format!("repair-crash-{step:?}"));
        CRASH_AT.with(|crash| crash.set(Some(step)));
        let crashed = repair(&fixture, D0 + 2 * HOUR);
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
        let report = repair(&fixture, D0 + 3 * HOUR);
        assert_eq!(
            report.problems,
            Vec::<String>::new(),
            "{step:?}: {report:?}"
        );
        assert_eq!(trees(&fixture), expected, "{step:?}");
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
        true,
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
}

/// The differential test of R2b's judgment (`restore::unrecorded_copy`,
/// shared with the restore's H6 k1) against the closing pass's D5 copy
/// from hot: every form is planted on two twin directories, judged on
/// one and copied onto the other. Absent or accepted ⇔ the copy
/// succeeds; refused ⇔ `Integrity`; a self-consistent other file ⇒
/// `Integrity`; a judgment error ⇔ an I/O error of the copy. Look-alike
/// names beside the temp names stay byte-identical: R2b derives every
/// name from hot's listing.
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
        let judged = restore::unrecorded_copy(&judged_root, &path, &text);
        let got = match &judged {
            Ok(UnrecordedCopy::Absent) => Class::Absent,
            Ok(UnrecordedCopy::Accepted) => Class::Accepted,
            Ok(UnrecordedCopy::OtherFile) => Class::OtherFile,
            Ok(UnrecordedCopy::Refused(_)) => Class::Refused,
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
                Class::Unreadable,
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
