//! The hot removal against the single-root reference (ADR-062 D6): each
//! eligibility rule alone, a crash at every step, and a remover that never
//! waits.

use super::{
    CRASH_AFTER, RemovalPlan, RemovalReport, RemovalStep, plan, remove_with_lock, scan_hot,
};
use crate::copy_ledger::{self, CopyLedger, HoldReason, Kind, LostOrigin, Resolution};
use crate::layout::{self, DAY_MS, PartFile};
use crate::reader::ParquetRawStore;
use crate::testutil::{FIXTURE_D0 as D0, TieredFixture, record, stream};
use crate::writer::RotationPolicy;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawRecordSink, RawRecordSource, RawSelection, RawStreamKey};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

const SOURCE: &str = "binance-um";

fn first_hot() -> String {
    TieredFixture::date(3)
}

fn all() -> RawSelection {
    RawSelection::new(
        TieredFixture::streams().into_iter().collect(),
        ReplayWindow {
            start: EventTime::from_millis(0),
            end: EventTime::from_millis(layout::MAX_EVENT_TIME_MS),
        },
    )
    .unwrap()
}

fn key() -> RawStreamKey {
    stream("aggTrade")
}

fn partition() -> String {
    TieredFixture::partition(&key(), 0)
}

/// The plan over the fixture's current state.
fn plan_now(fx: &TieredFixture, targets: &BTreeSet<String>, k: usize, first: &str) -> RemovalPlan {
    let ledger = copy_ledger::read(&fx.hot).unwrap().unwrap();
    plan(&ledger, &scan_hot(&fx.hot).unwrap(), targets, k, first).unwrap()
}

fn remove(fx: &TieredFixture, plan: &RemovalPlan) -> RemovalReport {
    remove_with_lock(&fx.hot, SOURCE, plan, &first_hot(), D0 + 40 * DAY_MS)
}

/// The `removed` records of `partition`.
fn removed_records(fx: &TieredFixture, partition: &str) -> usize {
    copy_ledger::read(&fx.hot)
        .unwrap()
        .unwrap()
        .records
        .iter()
        .filter(|r| matches!(&r.kind, Kind::Removed { partition: p } if p == partition))
        .count()
}

/// A fixture whose day-0 `aggTrade` partition is copied to both targets
/// and covered by both ledger copies: eligible.
fn eligible(tag: &str) -> TieredFixture {
    let fx = TieredFixture::new(tag);
    fx.copy(&partition());
    fx.ledger_copies();
    fx
}

/// Seals one more part into the day-0 partition through a single-root
/// writer on hot (a file the ledger does not know).
fn seal_extra(fx: &TieredFixture, key: &RawStreamKey) {
    let mut writer = ParquetRawStore::new(&fx.hot)
        .writer(key.source(), RotationPolicy::default())
        .unwrap();
    writer.append(key, record(D0 + 10, 1)).unwrap();
    writer.close().unwrap();
}

/// Every reference file reads through the union, the ledger verifies, and
/// `select` equals the reference's.
fn assert_intact(fx: &TieredFixture, what: &str) {
    let (store, reference) = (fx.store(), fx.reference());
    let expected = reference.select(&all()).unwrap();
    for file in &expected.files {
        assert_eq!(store.read(file), reference.read(file), "{what}");
    }
    assert!(copy_ledger::read(&fx.hot).is_ok(), "{what}");
    assert_eq!(store.select(&all()).unwrap(), expected, "{what}");
}

#[test]
fn each_eligibility_condition_alone_blocks_removal() {
    let targets = TieredFixture::target_ids();

    // The control row: removed, with its record before the rename.
    let fx = eligible("removal-control");
    let plan = plan_now(&fx, &targets, 2, &first_hot());
    assert_eq!(
        plan.candidates
            .iter()
            .map(|c| c.partition.as_str())
            .collect::<Vec<_>>(),
        vec![partition().as_str()]
    );
    assert_eq!(plan.candidates[0].files.len(), 3);
    let report = remove(&fx, &plan);
    assert_eq!(report.removed, vec![partition()], "{report:?}");
    assert_eq!(removed_records(&fx, &partition()), 1);
    assert!(!layout::resolve(&fx.hot, &partition()).exists());
    assert_intact(&fx, "control");

    /// One row: what it breaks, run against an eligible fixture. `None`
    /// when the plan itself must leave the partition out; `Some(reason)`
    /// when the plan names it and the remover refuses it with `reason`.
    type Row = (
        &'static str,
        Box<dyn Fn(&TieredFixture) -> RemovalPlan>,
        Option<&'static str>,
    );
    let path = |n: u32| format!("{}/{}", partition(), PartFile::Data.name(n));
    let rows: Vec<Row> = vec![
        // (a) The date equals the first hot date.
        (
            "the window",
            Box::new(move |fx| {
                plan_now(fx, &TieredFixture::target_ids(), 2, &TieredFixture::date(0))
            }),
            None,
        ),
        // A `.tmp` file: planned, then refused in the seal path.
        (
            "a .tmp file",
            Box::new(|fx| {
                let plan = plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot());
                fs::write(
                    layout::resolve(&fx.hot, &partition()).join(PartFile::DataTmp.name(9)),
                    b"debris",
                )
                .unwrap();
                plan
            }),
            Some("seal debris"),
        ),
        // (e) k - 1 copy records: a file with only cold-a's.
        (
            "k - 1 copies",
            Box::new(|fx| {
                seal_extra(fx, &key());
                fx.copy_path(&format!("{}/part-00003.parquet", partition()), &["cold-a"]);
                fx.ledger_copies();
                plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot())
            }),
            None,
        ),
        // (f) Copy records beyond one target's verified ledger-copy prefix.
        (
            "copy records beyond the ledger-copy prefix",
            Box::new(|fx| {
                seal_extra(fx, &key());
                fx.copy_path(
                    &format!("{}/part-00003.parquet", partition()),
                    &["cold-a", "cold-b"],
                );
                fx.ledger_copy(0);
                plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot())
            }),
            None,
        ),
        // (c) A target id missing from the pass (a replaced target).
        (
            "a replaced target",
            Box::new(|fx| {
                let targets: BTreeSet<String> = ["cold-a", "cold-c"].map(str::to_owned).into();
                plan_now(fx, &targets, 2, &first_hot())
            }),
            None,
        ),
        // (c) A target that did not present its marker.
        (
            "a target out",
            Box::new(|fx| {
                let targets: BTreeSet<String> = ["cold-a"].map(str::to_owned).into();
                plan_now(fx, &targets, 2, &first_hot())
            }),
            None,
        ),
        // (d) A lost, a held and a pending path.
        (
            "a lost path",
            Box::new(move |fx| {
                fx.append(Kind::Lost {
                    path: path(7),
                    origin: LostOrigin::Journal,
                    sha256: None,
                });
                fx.ledger_copies();
                plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot())
            }),
            None,
        ),
        (
            "a held path",
            Box::new(move |fx| {
                fx.append(Kind::Hold {
                    path: path(1),
                    reason: HoldReason::Unlisted,
                });
                fx.ledger_copies();
                plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot())
            }),
            None,
        ),
        (
            "a pending path",
            Box::new(move |fx| {
                fx.append(Kind::HoldCleared {
                    path: path(1),
                    resolution: Resolution::Replace,
                });
                fx.ledger_copies();
                plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot())
            }),
            None,
        ),
        // (e) A sealed file the ledger does not know (an archive orphan:
        // never replicated, so never copied).
        (
            "a file without copy records",
            Box::new(|fx| {
                seal_extra(fx, &key());
                plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot())
            }),
            None,
        ),
        // A file sealed after the plan.
        (
            "a file sealed after the plan",
            Box::new(|fx| {
                let plan = plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot());
                seal_extra(fx, &key());
                plan
            }),
            Some("sealed after the plan"),
        ),
        // A record after the plan that names the partition's files.
        (
            "a record after the plan",
            Box::new(move |fx| {
                let plan = plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot());
                fx.append(Kind::Hold {
                    path: path(0),
                    reason: HoldReason::Unlisted,
                });
                plan
            }),
            Some("after the plan names it"),
        ),
        // A `returned` record after the plan.
        (
            "a returned record after the plan",
            Box::new(|fx| {
                let plan = plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot());
                fx.append(Kind::Returned {
                    partition: partition(),
                });
                plan
            }),
            Some("after the plan names it"),
        ),
    ];
    for (name, setup, refused) in rows {
        let fx = eligible("removal-row");
        let plan = setup(&fx);
        let planned = plan.candidates.iter().any(|c| c.partition == partition());
        assert_eq!(planned, refused.is_some(), "{name}: {plan:?}");
        let report = remove(&fx, &plan);
        assert!(report.removed.is_empty(), "{name}: {report:?}");
        if let Some(reason) = refused {
            assert!(
                report
                    .skipped
                    .iter()
                    .any(|(p, why)| *p == partition() && why.contains(reason)),
                "{name}: {report:?}"
            );
        }
        assert_eq!(removed_records(&fx, &partition()), 0, "{name}");
        assert!(layout::resolve(&fx.hot, &partition()).exists(), "{name}");
    }

    // A ledger that no longer holds the plan's chain at the plan's length
    // (replaced, as by a rebuild): the pass removes nothing.
    let fx = eligible("removal-replaced-ledger");
    let plan = plan_now(&fx, &targets, 2, &first_hot());
    let ledger = copy_ledger::ledger_path(&fx.hot);
    let mut bytes = fs::read(&ledger).unwrap();
    let at = usize::try_from(plan.ledger_len).unwrap() - 2;
    bytes[at] = if bytes[at] == b'0' { b'1' } else { b'0' };
    fs::write(&ledger, &bytes).unwrap();
    let report = remove(&fx, &plan);
    assert!(report.removed.is_empty(), "{report:?}");
    let error = report.error.expect("an error");
    assert!(error.contains("changed since the plan"), "{error}");
    assert!(layout::resolve(&fx.hot, &partition()).exists());

    // An archive orphan in its own partition is never a candidate.
    let fx = eligible("removal-orphan");
    let archive = RawStreamKey::new("binance-archive", "BTCUSDT", "metrics").unwrap();
    seal_extra(&fx, &archive);
    let plan = plan_now(&fx, &targets, 2, &first_hot());
    assert!(plan.names_source(SOURCE));
    assert!(!plan.names_source("binance-archive"), "{plan:?}");
}

#[test]
fn a_crash_at_every_removal_step_keeps_every_file() {
    let targets = TieredFixture::target_ids();
    for step in RemovalStep::ALL {
        let fx = eligible("removal-crash");
        let plan = plan_now(&fx, &targets, 2, &first_hot());
        CRASH_AFTER.with(|crash| crash.set(Some(step)));
        let report = remove(&fx, &plan);
        CRASH_AFTER.with(|crash| crash.set(None));
        assert!(report.error.is_some(), "{step:?}: {report:?}");
        assert!(report.removed.is_empty(), "{step:?}");
        assert_eq!(removed_records(&fx, &partition()), 1, "{step:?}");
        assert_intact(&fx, &format!("{step:?}"));

        // The next pass completes it, with no second record.
        let plan = plan_now(&fx, &targets, 2, &first_hot());
        assert!(plan.candidates.is_empty(), "{step:?}: {plan:?}");
        let report = remove(&fx, &plan);
        assert!(report.error.is_none(), "{step:?}: {report:?}");
        match step {
            RemovalStep::AfterRecord => {
                assert_eq!(report.completed, vec![partition()], "{step:?}");
            }
            RemovalStep::AfterRename => {
                let (parent, name) = partition()
                    .rsplit_once('/')
                    .map(|(a, b)| (a.to_owned(), b.to_owned()))
                    .unwrap();
                assert_eq!(report.completed, vec![format!("{parent}/.removed-{name}")]);
            }
            RemovalStep::AfterDelete => assert!(report.is_empty(), "{report:?}"),
        }
        assert_eq!(removed_records(&fx, &partition()), 1, "{step:?}");
        let stream_dir = layout::resolve(&fx.hot, &layout::stream_dir(&key()));
        let names = crate::sorted_entries(&stream_dir).unwrap();
        assert!(
            names.iter().all(|n| !n.starts_with(".removed-")),
            "{step:?}: {names:?}"
        );
        assert!(!layout::resolve(&fx.hot, &partition()).exists());
        assert_intact(&fx, &format!("{step:?} completed"));
    }
}

/// Every file and its bytes below `root`.
fn files_under(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, Vec<u8>)>) {
        for name in crate::sorted_entries(dir).unwrap() {
            let path = dir.join(&name);
            let rel = format!("{prefix}{name}");
            if path.is_dir() {
                walk(&path, &format!("{rel}/"), out);
            } else if !name.ends_with(".lock") {
                out.push((rel, fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, "", &mut out);
    out
}

#[test]
fn the_remover_never_waits() {
    let targets = TieredFixture::target_ids();

    // A writer holds the source lock: `locked`, nothing changes.
    let fx = eligible("removal-source-lock");
    let plan = plan_now(&fx, &targets, 2, &first_hot());
    let before = files_under(&fx.hot);
    let writer = ParquetRawStore::new(&fx.hot)
        .writer(SOURCE, RotationPolicy::default())
        .unwrap();
    let report = remove(&fx, &plan);
    assert!(report.locked, "{report:?}");
    assert!(report.removed.is_empty());
    assert_eq!(files_under(&fx.hot), before);
    drop(writer);

    // Another holder has the ledger lock: skipped, naming the reason.
    let hold = CopyLedger::at(&fx.hot).try_hold().unwrap();
    let report = remove(&fx, &plan);
    assert!(report.removed.is_empty(), "{report:?}");
    assert!(
        report
            .skipped
            .iter()
            .any(|(p, why)| *p == partition() && why.contains("copy-ledger lock is held")),
        "{report:?}"
    );
    assert_eq!(files_under(&fx.hot), before);
    drop(hold);

    // Free again: removed.
    assert_eq!(remove(&fx, &plan).removed, vec![partition()]);
}

#[test]
fn a_writer_removes_only_partitions_without_an_open_part() {
    let fx = eligible("removal-open-part");
    let plan = plan_now(&fx, &TieredFixture::target_ids(), 2, &first_hot());
    let mut writer = fx
        .store()
        .tiered_writer(
            SOURCE,
            RotationPolicy::default(),
            TieredFixture::context(&first_hot()),
        )
        .unwrap();
    // An open part of the partition: skipped, the part is not lost.
    writer.append(&key(), record(D0 + 10, 5)).unwrap();
    let report = writer.remove_partitions(&plan, &first_hot(), D0 + 40 * DAY_MS);
    assert!(report.removed.is_empty(), "{report:?}");
    assert!(
        report
            .skipped
            .iter()
            .any(|(_, why)| why.contains("open part")),
        "{report:?}"
    );
    let sealed = writer.close().unwrap();
    assert_eq!(sealed[0].part, 3);
}
