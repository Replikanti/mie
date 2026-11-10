//! The hot removal against the single-root reference (ADR-062 D6): each
//! eligibility rule alone, a crash at every step, and a remover that never
//! waits.

use super::{
    CRASH_AFTER, RemovalPlan, RemovalReport, RemovalStep, plan, remove_with_lock, scan_hot,
};
use crate::copy_ledger::{
    self, CopyLedger, FindingCode, HoldReason, Kind, LostOrigin, Resolution, Subject,
};
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
    plan(
        &ledger,
        &scan_hot(&fx.hot, first).unwrap(),
        targets,
        k,
        first,
        None,
    )
    .unwrap()
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

/// Replaces hot's manifest of `path` with another valid text (one more
/// row): hot then holds another version of a recorded path (D5).
fn rewrite_hot_manifest(fx: &TieredFixture, path: &str) {
    let file = layout::resolve(&fx.hot, &TieredFixture::manifest_of(path));
    let mut manifest = crate::manifest::parse(&fs::read_to_string(&file).unwrap()).unwrap();
    manifest.rows += 1;
    let mut permissions = fs::metadata(&file).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&file, permissions).unwrap();
    fs::write(&file, crate::manifest::render(&manifest)).unwrap();
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
        // Review of #202, finding 2: hot's manifest differs from the
        // ledger's `manifest` record (D5): never planned.
        (
            "hot's manifest differs from the ledger's",
            Box::new(move |fx| {
                rewrite_hot_manifest(fx, &path(0));
                plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot())
            }),
            None,
        ),
        // ... and when it changes after the plan, the re-check refuses it.
        (
            "hot's manifest changed after the plan",
            Box::new(move |fx| {
                let plan = plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot());
                rewrite_hot_manifest(fx, &path(0));
                plan
            }),
            Some("differs from the copy ledger's manifest record"),
        ),
        // Review of #202, finding 3: a damaged-copy finding before the
        // plan takes that copy out of k.
        (
            "a finding before the plan",
            Box::new(move |fx| {
                fx.append(Kind::Finding {
                    root: "cold-b".to_owned(),
                    code: FindingCode::Sha256,
                    subject: Subject::File(path(1)),
                });
                fx.ledger_copies();
                plan_now(fx, &TieredFixture::target_ids(), 2, &first_hot())
            }),
            None,
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

/// Review of #202: the plan names a D5 conflict, and a copy re-recorded
/// after a finding counts again (the repair's `copy` record, #192).
#[test]
fn conflicts_are_reported_and_a_copy_after_a_finding_counts_again() {
    let path = format!("{}/{}", partition(), PartFile::Data.name(0));
    let fx = eligible("removal-conflict");
    rewrite_hot_manifest(&fx, &path);
    let plan = plan_now(&fx, &TieredFixture::target_ids(), 2, &first_hot());
    assert!(plan.candidates.is_empty(), "{plan:?}");
    assert!(
        plan.conflicts
            .iter()
            .any(|c| c.contains(&path) && c.contains("ADR-062 D5")),
        "{plan:?}"
    );

    let fx = eligible("removal-finding-then-copy");
    fx.append(Kind::Finding {
        root: "cold-b".to_owned(),
        code: FindingCode::Missing,
        subject: Subject::File(path.clone()),
    });
    fx.ledger_copies();
    assert!(
        plan_now(&fx, &TieredFixture::target_ids(), 2, &first_hot())
            .candidates
            .is_empty()
    );
    fx.append(Kind::Copy {
        path: path.clone(),
        root: "cold-b".to_owned(),
    });
    fx.ledger_copies();
    let plan = plan_now(&fx, &TieredFixture::target_ids(), 2, &first_hot());
    assert_eq!(plan.candidates.len(), 1, "{plan:?}");
}

/// L1 of #192 in the plan: a copy-ledger finding on one target voids its
/// `ledger-copy` record, so there is no prefix P and no candidate, until a
/// later `ledger-copy` record for it.
#[test]
fn a_copy_ledger_finding_leaves_no_plan_until_a_later_ledger_copy() {
    let fx = eligible("removal-l1-plan");
    let targets = TieredFixture::target_ids();
    assert_eq!(plan_now(&fx, &targets, 2, &first_hot()).candidates.len(), 1);
    fx.append(Kind::Finding {
        root: "cold-a".to_owned(),
        code: FindingCode::Chain,
        subject: Subject::CopyLedger,
    });
    let plan = plan_now(&fx, &targets, 2, &first_hot());
    assert!(plan.candidates.is_empty(), "{plan:?}");
    fx.ledger_copy(0);
    assert_eq!(plan_now(&fx, &targets, 2, &first_hot()).candidates.len(), 1);
}

/// L1 of #192 in the re-check: a copy-ledger finding in the tail since
/// the plan names every candidate, which stays on hot.
#[test]
fn a_copy_ledger_finding_after_the_plan_skips_every_candidate() {
    let fx = eligible("removal-l1-recheck");
    let plan = plan_now(&fx, &TieredFixture::target_ids(), 2, &first_hot());
    assert_eq!(plan.candidates.len(), 1);
    fx.append(Kind::Finding {
        root: "cold-b".to_owned(),
        code: FindingCode::Prefix,
        subject: Subject::CopyLedger,
    });
    let report = remove(&fx, &plan);
    assert!(report.removed.is_empty(), "{report:?}");
    assert_eq!(report.skipped.len(), 1, "{report:?}");
    assert!(
        report.skipped[0]
            .1
            .contains("a finding record after the plan"),
        "{report:?}"
    );
    assert!(layout::resolve(&fx.hot, &partition()).exists());
}

/// `ledger` with one `verify` record per `(root, prefix)` appended in
/// memory, each one byte long: the plan reads only the records and their
/// ends.
fn with_verifies(ledger: &copy_ledger::Ledger, verifies: &[(&str, u64)]) -> copy_ledger::Ledger {
    let mut ledger = ledger.clone();
    for (root, prefix_bytes) in verifies {
        ledger.records.push(copy_ledger::Record {
            at_ms: D0 + 50 * DAY_MS,
            kind: Kind::Verify {
                root: (*root).to_owned(),
                prefix_bytes: *prefix_bytes,
                findings: 0,
            },
        });
        ledger.verified_len += 1;
        ledger.ends.push(ledger.verified_len);
    }
    ledger
}

/// B10 of #193: with the gate `Some(L)` nothing is planned until every
/// target id has a `verify` record whose prefix covers L, and L lies
/// inside the read; `None` plans as before. L = 1 000: prefix 999 waits,
/// 1 000 and 1 001 plan, one of two targets waits.
#[test]
fn the_plan_waits_for_a_verify_covering_the_rebuilt_ledger() {
    let fx = eligible("removal-gate");
    let targets = TieredFixture::target_ids();
    let ledger = copy_ledger::read(&fx.hot).unwrap().unwrap();
    assert!(ledger.verified_len >= 1_002, "{}", ledger.verified_len);
    let scan = scan_hot(&fx.hot, &first_hot()).unwrap();
    let planned = |ledger: &copy_ledger::Ledger, gate: Option<u64>| {
        plan(ledger, &scan, &targets, 2, &first_hot(), gate).unwrap()
    };

    let control = planned(&ledger, None);
    assert_eq!(control.candidates.len(), 1);
    assert_eq!(control.waiting, None);

    /// The `verify` records appended, the gate, and whether it plans.
    type Case<'a> = (&'a [(&'a str, u64)], Option<u64>, bool);
    let cases: [Case; 8] = [
        (&[], Some(1_000), false),
        (&[("cold-a", 999), ("cold-b", 999)], Some(1_000), false),
        (&[("cold-a", 1_000), ("cold-b", 1_000)], Some(1_000), true),
        (&[("cold-a", 1_001), ("cold-b", 1_001)], Some(1_000), true),
        (&[("cold-a", 1_000)], Some(1_000), false),
        (&[("cold-a", 999), ("cold-b", 1_000)], Some(1_000), false),
        // L beyond the read: a ledger shorter than the rebuilt one.
        (
            &[("cold-a", u64::MAX), ("cold-b", u64::MAX)],
            Some(u64::MAX),
            false,
        ),
        (&[], None, true),
    ];
    for (verifies, gate, plans) in cases {
        let ledger = with_verifies(&ledger, verifies);
        let plan = planned(&ledger, gate);
        assert_eq!(
            plan.candidates == control.candidates,
            plans,
            "{verifies:?} {gate:?}: {plan:?}"
        );
        assert_eq!(plan.candidates.is_empty(), !plans, "{verifies:?} {gate:?}");
        assert_eq!(plan.waiting.is_some(), !plans, "{verifies:?} {gate:?}");
    }
}

/// Review of #209, finding 1: a plan made before a copy-ledger rebuild is
/// dropped by the remover, under its locks, once the rebuild installed its
/// gate, although the plan's prefix still matches the rebuilt ledger; a
/// gate that cannot be parsed removes nothing and is an error; a plan made
/// under the current gate, once a verification covers it, removes.
#[test]
fn a_plan_made_before_a_rebuild_is_dropped_by_the_remover() {
    let fx = eligible("removal-stale-plan");
    let targets = TieredFixture::target_ids();
    // The pass plans (no gate yet) and hands the plan to the seal path.
    let stale = plan_now(&fx, &targets, 2, &first_hot());
    assert_eq!(stale.candidates.len(), 1);
    // `mie store rebuild` runs before the next seal: start = a ledger copy
    // equal to hot's prefix, plus one derived record, installed with its
    // gate.
    {
        let hold = CopyLedger::at(&fx.hot).try_hold().unwrap();
        let bytes = fs::read(copy_ledger::ledger_path(&fx.hot)).unwrap();
        let mut builder =
            copy_ledger::LedgerBuilder::from_prefix(Path::new("start"), &bytes).unwrap();
        builder
            .push(&copy_ledger::Record {
                at_ms: D0 + 41 * DAY_MS,
                kind: Kind::Verify {
                    root: "cold-a".to_owned(),
                    prefix_bytes: 0,
                    findings: 0,
                },
            })
            .unwrap();
        hold.replace(&builder.finish(), None).unwrap();
    }
    let gate = crate::rebuilt::read(&fx.hot).unwrap();
    assert!(gate.is_some());
    // A fresh plan waits, as rule (g) says ...
    let ledger = copy_ledger::read(&fx.hot).unwrap().unwrap();
    let scan = scan_hot(&fx.hot, &first_hot()).unwrap();
    let fresh = plan(&ledger, &scan, &targets, 2, &first_hot(), gate).unwrap();
    assert!(
        fresh.candidates.is_empty() && fresh.waiting.is_some(),
        "{fresh:?}"
    );
    // ... and the seal path drops the stale plan.
    let report = remove(&fx, &stale);
    assert!(report.removed.is_empty(), "{report:?}");
    assert_eq!(report.skipped.len(), 1, "{report:?}");
    assert!(report.skipped[0].1.contains("rebuilt"), "{report:?}");
    assert!(layout::resolve(&fx.hot, &partition()).exists());
    assert_eq!(removed_records(&fx, &partition()), 0);

    // A malformed gate: nothing is removed, and the pass reports it.
    let gate_path = crate::rebuilt::gate_path(&fx.hot);
    let good = fs::read(&gate_path).unwrap();
    fs::write(&gate_path, b"mie-rebuilt 1\nprefix 01\n").unwrap();
    let report = remove(&fx, &stale);
    assert!(report.removed.is_empty(), "{report:?}");
    assert!(
        report.error.as_deref().is_some_and(|e| e.contains("gate")),
        "{report:?}"
    );
    assert!(layout::resolve(&fx.hot, &partition()).exists());
    fs::write(&gate_path, good).unwrap();

    // Once a verification of both targets covers the rebuilt ledger, a plan
    // made under that gate removes.
    for root in ["cold-a", "cold-b"] {
        fx.append(Kind::Verify {
            root: root.to_owned(),
            prefix_bytes: gate.unwrap(),
            findings: 0,
        });
    }
    fx.ledger_copies();
    let ledger = copy_ledger::read(&fx.hot).unwrap().unwrap();
    let scan = scan_hot(&fx.hot, &first_hot()).unwrap();
    let covered = plan(&ledger, &scan, &targets, 2, &first_hot(), gate).unwrap();
    assert_eq!(covered.waiting, None, "{covered:?}");
    let report = remove(&fx, &covered);
    assert_eq!(report.removed, vec![partition()], "{report:?}");
}

/// Re-review of #209 (a pin of finding 1's argument): a plan made under
/// gate L, then a second rebuild that installs a ledger of the same length
/// L, byte-identical to the first: the gate is equal, and the plan is still
/// dropped, since its prefix lies past L.
#[test]
fn a_second_rebuild_of_the_same_length_drops_the_plan() {
    let fx = eligible("removal-same-len");
    let targets = TieredFixture::target_ids();
    let build = |fx: &TieredFixture, bytes: &[u8]| {
        let mut builder =
            copy_ledger::LedgerBuilder::from_prefix(Path::new("start"), bytes).unwrap();
        builder
            .push(&copy_ledger::Record {
                at_ms: D0 + 41 * DAY_MS,
                kind: Kind::Verify {
                    root: "cold-a".to_owned(),
                    prefix_bytes: 0,
                    findings: 0,
                },
            })
            .unwrap();
        let out = builder.finish();
        let hold = CopyLedger::at(&fx.hot).try_hold().unwrap();
        hold.replace(&out, None).unwrap();
        out
    };
    let start = fs::read(copy_ledger::ledger_path(&fx.hot)).unwrap();
    let first = build(&fx, &start);
    let gate = crate::rebuilt::read(&fx.hot).unwrap();
    for root in ["cold-a", "cold-b"] {
        fx.append(Kind::Verify {
            root: root.to_owned(),
            prefix_bytes: gate.unwrap(),
            findings: 0,
        });
    }
    fx.ledger_copies();
    let ledger = copy_ledger::read(&fx.hot).unwrap().unwrap();
    let scan = scan_hot(&fx.hot, &first_hot()).unwrap();
    let covered = plan(&ledger, &scan, &targets, 2, &first_hot(), gate).unwrap();
    assert_eq!(covered.candidates.len(), 1, "{covered:?}");
    let second = build(&fx, &start);
    assert_eq!(first, second);
    assert_eq!(crate::rebuilt::read(&fx.hot).unwrap(), gate);
    let report = remove(&fx, &covered);
    assert!(report.removed.is_empty(), "{report:?}");
    assert!(layout::resolve(&fx.hot, &partition()).exists());
}
