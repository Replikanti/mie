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
