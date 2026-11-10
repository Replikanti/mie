//! The rebuild end to end on the replication fixture (#193): refusals,
//! locks, the read order, the install and the stops before it.

use super::*;
use crate::store::replicate::tests::{D0, Fixture};
use mie_adapter_parquet::rebuilt;
use std::os::unix::fs::PermissionsExt;

/// The run's `at_ms`.
const AT: i64 = D0 + 3 * 3_600_000;

fn rebuild_with(fixture: &Fixture, flags: Flags, first_hot: &str) -> RebuildReport {
    run(
        &fixture.config,
        Some(&fixture.archive()),
        flags,
        first_hot,
        &fixture.probe,
        &|| AT,
    )
}

fn rebuild(fixture: &Fixture, flags: Flags) -> RebuildReport {
    rebuild_with(fixture, flags, "2026-10-05")
}

/// A replicated fixture whose hot ledger is gone.
fn lost_ledger(tag: &str) -> Fixture {
    let fixture = Fixture::new(tag);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    fs::remove_file(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    fixture
}

/// Every file under hot and both targets with its bytes; lock files left
/// out (the locks create them).
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
fn assert_pass(report: &RebuildReport) {
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    assert!(
        report.lines.last().is_some_and(|l| l.starts_with("wrote ")),
        "{report:#?}"
    );
}

#[track_caller]
fn assert_stopped(report: &RebuildReport, needles: &[&str]) {
    assert!(
        report
            .problems
            .iter()
            .any(|p| needles.iter().all(|n| p.contains(n))),
        "no problem with {needles:?}: {report:#?}"
    );
    assert_eq!(
        report.lines.last().map(String::as_str),
        Some("nothing written"),
        "{report:#?}"
    );
}

fn chmod(path: &Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

/// Whether permissions bind this process (they do not for root).
fn permissions_bind(dir: &Path) -> bool {
    let probe = dir.join("perm-probe");
    fs::create_dir(&probe).unwrap();
    chmod(&probe, 0o000);
    let binds = fs::read_dir(&probe).is_err();
    chmod(&probe, 0o755);
    fs::remove_dir(&probe).unwrap();
    binds
}

/// The ledger after a lost hot ledger is the longest copy, unchanged, and
/// the gate holds its length.
#[test]
fn a_lost_ledger_is_rebuilt_from_its_longest_copy() {
    let fixture = lost_ledger("rebuild-lost");
    // cold-b's copy is longer: the pass extends cold-a's first, then
    // appends its `ledger-copy` record, which cold-b's copy then holds.
    let (a, b) = (
        fs::read(copy_ledger::ledger_path(&fixture.cold[0])).unwrap(),
        fs::read(copy_ledger::ledger_path(&fixture.cold[1])).unwrap(),
    );
    assert!(b.len() > a.len() && b.starts_with(&a));
    let copy = b;
    let report = rebuild(&fixture, Flags::default());
    assert_pass(&report);
    let ledger = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    assert!(ledger == copy, "{report:#?}");
    assert!(
        report.lines[0].starts_with("start: the ledger copy on cold-b"),
        "{report:#?}"
    );
    assert_eq!(
        rebuilt::read(&fixture.hot).unwrap(),
        Some(ledger.len() as u64)
    );
}

/// The rebuilt ledger parses, and the next pass copies it and passes.
#[test]
fn the_rebuilt_ledger_round_trips_and_the_next_pass_copies_it() {
    let fixture = lost_ledger("rebuild-round-trip");
    for root in &fixture.cold {
        fs::remove_file(copy_ledger::ledger_path(root)).unwrap();
    }
    let report = rebuild(&fixture, Flags::default());
    assert_pass(&report);
    assert!(report.lines.iter().any(|l| l.starts_with("started empty")));
    assert!(copy_ledger::read(&fixture.hot).unwrap().is_some());
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    for root in &fixture.cold {
        assert_eq!(
            copy_ledger::check_copy(&fixture.hot, root).unwrap(),
            LedgerCopyCheck::Good
        );
    }
}

/// B0: a readable hot ledger is refused without the flag and linked aside
/// with it; an unreadable one proceeds without it.
#[test]
fn hot_ledger_states() {
    let fixture = Fixture::new("rebuild-hot-ledger");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let before = snapshot(&fixture);
    let report = rebuild(&fixture, Flags::default());
    assert_stopped(&report, &["readable", "--replace-hot-ledger"]);
    assert_eq!(snapshot(&fixture), before);

    let old = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    let flags = Flags {
        replace_hot_ledger: true,
        ..Flags::default()
    };
    let report = rebuild(&fixture, flags);
    assert_pass(&report);
    let aside = replica::damaged_dir(&fixture.hot, AT).join("copy-ledger");
    assert_eq!(fs::read(&aside).unwrap(), old);

    for (tag, damage) in [
        ("corrupt", b"mie-copy-ledger 9\n".to_vec()),
        ("integrity", {
            let mut bytes = old.clone();
            bytes[copy_ledger::HEADER.len() + 3] ^= 0x01;
            bytes
        }),
    ] {
        let fixture = Fixture::new(&format!("rebuild-hot-ledger-{tag}"));
        assert_eq!(fixture.pass().problems, Vec::<String>::new());
        fs::write(copy_ledger::ledger_path(&fixture.hot), &damage).unwrap();
        let report = rebuild(&fixture, Flags::default());
        assert_pass(&report);
        let aside = replica::damaged_dir(&fixture.hot, AT).join("copy-ledger");
        assert_eq!(fs::read(&aside).unwrap(), damage, "{tag}");
    }
}

/// Every whole-run lock refuses the rebuild with nothing written; the live
/// source's lock (`mie ingest`) does not.
#[test]
fn the_locks_refuse_the_rebuild() {
    let fixture = lost_ledger("rebuild-locks");
    let before = snapshot(&fixture);
    let hot = fixture.hot.clone();
    /// A lock's name and how to take it; the value holds it.
    type Take<'a> = Box<dyn Fn() -> Box<dyn std::any::Any> + 'a>;
    let cases: [(&str, Take); 4] = [
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
            "archive source's writer lock",
            Box::new(|| {
                Box::new(
                    removal::try_source_lock(&hot, ARCHIVE_SOURCE)
                        .unwrap()
                        .unwrap(),
                )
            }),
        ),
    ];
    for (name, take) in cases {
        let held = take();
        let report = rebuild(&fixture, Flags::default());
        assert_stopped(&report, &[name]);
        assert_eq!(snapshot(&fixture), before, "{name}");
        drop(held);
    }
    let live = removal::try_source_lock(&hot, "binance-um")
        .unwrap()
        .unwrap();
    assert_pass(&rebuild(&fixture, Flags::default()));
    drop(live);
}

/// B0: archive parts with no archive configuration are refused.
#[test]
fn archive_paths_need_the_archive_config() {
    let fixture = lost_ledger("rebuild-no-archive");
    let before = snapshot(&fixture);
    let report = run(
        &fixture.config,
        None,
        Flags::default(),
        "2026-10-05",
        &fixture.probe,
        &|| AT,
    );
    assert_stopped(&report, &["binance-archive", "--archive-config"]);
    assert_eq!(snapshot(&fixture), before);
}

/// B0: an archive configuration of another store is refused.
#[test]
fn another_stores_archive_config_is_refused() {
    let fixture = lost_ledger("rebuild-other-archive");
    let other = fixture.dir.0.join("other.toml");
    let text = fs::read_to_string(fixture.archive())
        .unwrap()
        .replace("raw\"", "elsewhere\"");
    fs::write(&other, text).unwrap();
    let before = snapshot(&fixture);
    let report = run(
        &fixture.config,
        Some(&other),
        Flags::default(),
        "2026-10-05",
        &fixture.probe,
        &|| AT,
    );
    assert_eq!(
        report.lines.last().map(String::as_str),
        Some("nothing written")
    );
    assert!(!report.problems.is_empty());
    assert_eq!(snapshot(&fixture), before);
}

/// B0: a copy target or a read-only root without its marker stops the
/// rebuild before anything is read or written.
#[test]
fn a_root_without_its_marker_stops_the_rebuild() {
    let fixture = lost_ledger("rebuild-no-marker");
    fs::remove_file(root_marker::marker_path(&fixture.cold[1])).unwrap();
    let before = snapshot(&fixture);
    let report = rebuild(&fixture, Flags::default());
    assert_stopped(&report, &["copy target", "cold-b", "no root marker"]);
    assert_eq!(snapshot(&fixture), before);

    let fixture = lost_ledger("rebuild-no-marker-ro");
    let read_only = fixture.dir.0.join("cold-ro");
    fs::create_dir_all(&read_only).unwrap();
    let mut config = fixture.config.clone();
    config.paths.cold_roots.push(crate::config::ColdRoot {
        path: read_only,
        copy_target: false,
    });
    let before = snapshot(&fixture);
    let report = run(
        &config,
        None,
        Flags::default(),
        "2026-10-05",
        &fixture.probe,
        &|| AT,
    );
    assert_stopped(&report, &["read-only root", "cold-ro", "no root marker"]);
    assert_eq!(snapshot(&fixture), before);
}

/// B9 step 2: a marker that disappears between the listing and the
/// install stops the rebuild with nothing written, no `lost` record
/// included.
#[test]
fn a_marker_lost_during_the_run_stops_before_any_write() {
    let fixture = lost_ledger("rebuild-marker-mid-run");
    let marker = root_marker::marker_path(&fixture.cold[0]);
    let moved = fixture.dir.0.join("marker-moved");
    let (from, to) = (marker.clone(), moved.clone());
    BEFORE_INSTALL.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || fs::rename(&from, &to).unwrap()));
    });
    let report = rebuild(
        &fixture,
        Flags {
            record_lost: true,
            ..Flags::default()
        },
    );
    assert_stopped(&report, &["cold-a", "marker is gone"]);
    assert!(!copy_ledger::ledger_path(&fixture.hot).exists());
    assert!(!rebuilt::gate_path(&fixture.hot).exists());
    fs::rename(&moved, &marker).unwrap();
}

/// B1: an unreadable partition directory stops the rebuild naming the
/// root, never a lost candidate.
#[test]
fn an_io_error_on_a_listing_stops_the_rebuild() {
    let fixture = lost_ledger("rebuild-listing-io");
    if !permissions_bind(&fixture.dir.0) {
        return;
    }
    let part = replica::list_sealed(&fixture.cold[1]).unwrap().remove(0);
    let partition = fixture.cold[1].join(part.rsplit_once('/').unwrap().0);
    chmod(&partition, 0o000);
    let report = rebuild(
        &fixture,
        Flags {
            record_lost: true,
            ..Flags::default()
        },
    );
    chmod(&partition, 0o755);
    assert_stopped(
        &report,
        &["cannot list", &fixture.cold[1].display().to_string()],
    );
    assert!(
        !report
            .problems
            .iter()
            .any(|p| p.contains("no root holds it"))
    );
    assert!(!copy_ledger::ledger_path(&fixture.hot).exists());
}

/// B1, B5: unreadable data on one root while another verifies is a
/// damaged copy with a `FAIL:` line and no record, never an absence.
#[test]
fn unreadable_data_is_damage_not_absence() {
    let fixture = lost_ledger("rebuild-unreadable-data");
    if !permissions_bind(&fixture.dir.0) {
        return;
    }
    for root in &fixture.cold {
        fs::remove_file(copy_ledger::ledger_path(root)).unwrap();
    }
    let part = replica::list_sealed(&fixture.cold[0])
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    let data = fixture.cold[0].join(&part);
    chmod(&data, 0o000);
    let report = rebuild(&fixture, Flags::default());
    chmod(&data, 0o444);
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains(&part) && p.contains("cold-a") && p.contains("cannot be read")),
        "{report:#?}"
    );
    assert!(
        !report
            .problems
            .iter()
            .any(|p| p.contains("no root holds it"))
    );
    let records = fixture.records();
    let copies: Vec<&str> = records
        .iter()
        .filter_map(|r| match &r.kind {
            Kind::Copy { path, root } if *path == part => Some(root.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(copies, vec!["cold-b"]);
}

/// B1: a part sealed after the journal was read is listed, never lost.
#[test]
fn journal_is_read_before_the_roots_are_listed() {
    let fixture = lost_ledger("rebuild-read-order");
    let hot = fixture.hot.clone();
    let journal = fixture.config.paths.journal.clone();
    let sealed = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let seen = sealed.clone();
    AFTER_JOURNAL.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let parts = crate::store::replicate::tests::seal(
                &hot,
                "binance-um",
                "aggTrade",
                D0 + 7_200_000,
                2,
            );
            let mut text = fs::read_to_string(&journal).unwrap();
            text.push_str(&format!(
                "{{\"type\":\"sealed\",\"run_id\":\"r9\",\"files\":[{{\"path\":\"{}\"}}]}}\n",
                parts[0]
            ));
            fs::write(&journal, text).unwrap();
            *seen.borrow_mut() = parts;
        }));
    });
    let report = rebuild(
        &fixture,
        Flags {
            record_lost: true,
            ..Flags::default()
        },
    );
    assert_eq!(sealed.borrow().len(), 1);
    assert_pass(&report);
    assert!(!report.lines.iter().any(|l| l.starts_with("lost:")));
}

/// B8: a removal's hidden leftover is no directory, so its partition is
/// recorded removed; a partition the start records removed that is still
/// on hot gets no record.
#[test]
fn removal_leftovers_and_incomplete_removals() {
    let fixture = Fixture::new("rebuild-leftovers");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let live = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    let live_partition = live.rsplit_once('/').unwrap().0.to_owned();
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + 2,
            kind: Kind::Removed {
                partition: live_partition.clone(),
            },
        })
        .unwrap();
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    fs::remove_file(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    let archive = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-archive"))
        .unwrap();
    let partition = archive.rsplit_once('/').unwrap().0.to_owned();
    let (stream, date) = partition.rsplit_once('/').unwrap();
    fs::rename(
        fixture.hot.join(&partition),
        fixture.hot.join(stream).join(format!(".removed-{date}")),
    )
    .unwrap();
    let report = rebuild_with(&fixture, Flags::default(), "2026-10-06");
    assert_pass(&report);
    let removed: Vec<String> = fixture
        .records()
        .into_iter()
        .filter(|r| r.at_ms == AT)
        .filter_map(|r| match r.kind {
            Kind::Removed { partition } => Some(partition),
            _ => None,
        })
        .collect();
    assert_eq!(removed, vec![partition]);
}

/// B2, B9 step 6: a ledger copy that is no prefix of the rebuilt ledger
/// is moved aside, and the next pass copies the ledger from byte 0.
#[test]
fn a_copy_that_is_no_prefix_is_moved_aside_after_the_install() {
    let fixture = lost_ledger("rebuild-move-aside");
    let b = copy_ledger::ledger_path(&fixture.cold[1]);
    let bytes = fs::read(&b).unwrap();
    let ledger = copy_ledger::parse(&b, &bytes).unwrap();
    // cold-b: the first record of cold-a's copy, then one of its own.
    let mut other = LedgerBuilder::from_prefix(&b, &bytes[..ledger.ends[0] as usize]).unwrap();
    other
        .push(&Record {
            at_ms: D0,
            kind: Kind::Verify {
                root: "cold-b".to_owned(),
                prefix_bytes: 1,
                findings: 0,
            },
        })
        .unwrap();
    let other = other.finish();
    fs::write(&b, &other).unwrap();
    let report = rebuild(&fixture, Flags::default());
    assert_pass(&report);
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.contains("cold-b") && l.contains("moved aside"))
    );
    let aside = replica::damaged_dir(&fixture.cold[1], AT).join("copy-ledger");
    assert_eq!(fs::read(&aside).unwrap(), other);
    assert!(!b.exists());
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    assert_eq!(
        copy_ledger::check_copy(&fixture.hot, &fixture.cold[1]).unwrap(),
        LedgerCopyCheck::Good
    );
}

/// B9 step 3: an existing evidence name on a target stops the install.
#[test]
fn existing_evidence_on_a_target_stops_the_install() {
    let fixture = lost_ledger("rebuild-evidence");
    let aside = replica::damaged_dir(&fixture.cold[0], AT).join("copy-ledger");
    fs::create_dir_all(aside.parent().unwrap()).unwrap();
    fs::write(&aside, b"earlier").unwrap();
    let report = rebuild(&fixture, Flags::default());
    assert_stopped(&report, &["evidence is never overwritten"]);
    assert_eq!(fs::read(&aside).unwrap(), b"earlier");
    assert!(!copy_ledger::ledger_path(&fixture.hot).exists());
}

/// B3: one live path with two parseable texts on two roots stops the
/// rebuild with nothing written.
#[test]
fn a_live_manifest_conflict_stops_the_rebuild() {
    let fixture = lost_ledger("rebuild-live-conflict");
    let part = replica::list_sealed(&fixture.cold[1])
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    let manifest = fixture.cold[1].join(crate::store::replicate::tests::manifest_of(&part));
    chmod(&manifest, 0o644);
    let text = fs::read_to_string(&manifest).unwrap();
    let other = text.replacen("\nrows 2\n", "\nrows 3\n", 1);
    assert_ne!(other, text);
    fs::write(&manifest, other).unwrap();
    let before = snapshot(&fixture);
    let report = rebuild(&fixture, Flags::default());
    assert_stopped(&report, &[&part, "cold-b", "different manifest texts"]);
    assert_eq!(snapshot(&fixture), before);
}
