//! The rebuild's pure derivation (B2 to B8) over constructed inputs, and
//! the journal read (B1).

use super::*;
use crate::store::archive_checks::tests::{E1, NAME, S1, entry, part, sha, supersede, text};
use mie_adapter_parquet::copy_ledger::{FindingCode, LedgerBuilder, Subject};

/// The run's `at_ms`: 2026-10-06T03:00:00Z.
const AT: i64 = 1_791_255_600_000;

fn live(date: &str, n: u32) -> String {
    format!("source=binance-um/instrument=BTCUSDT/stream=aggTrade/date={date}/part-{n:05}.parquet")
}

fn partition(path: &str) -> String {
    path.rsplit_once('/').unwrap().0.to_owned()
}

/// Inputs over hot and two copy targets, all empty, no ledger copy.
fn inputs() -> RebuildInputs {
    RebuildInputs {
        mode: Mode::Rebuild,
        at_ms: AT,
        first_hot_date: "2026-10-05".to_owned(),
        record_lost: false,
        archive_configured: true,
        record_unlisted: false,
        import_ledger: Some("the import ledger".to_owned()),
        missing_import_ledger: None,
        ledger_copies: vec![
            LedgerCopyRead {
                id: "cold-a".to_owned(),
                bytes: None,
                error: None,
            },
            LedgerCopyRead {
                id: "cold-b".to_owned(),
                bytes: None,
                error: None,
            },
        ],
        journal: JournalRead::default(),
        entries: BTreeMap::new(),
        pending: None,
        hot: HotRead::default(),
        roots: ["cold-a", "cold-b"]
            .map(|id| RootRead {
                id: id.to_owned(),
                copy_target: true,
                files: BTreeMap::new(),
                orphan_data: BTreeSet::new(),
                temps: BTreeMap::new(),
                evidence: BTreeMap::new(),
                quarantine: QuarantineListing::default(),
            })
            .into(),
    }
}

fn good(text: &str) -> RootCopy {
    RootCopy::Text {
        text: text.to_owned(),
        data: DataCheck::Good,
    }
}

fn on_hot(inputs: &mut RebuildInputs, path: &str, text: &str) {
    inputs.hot.files.insert(
        path.to_owned(),
        RootCopy::Text {
            text: text.to_owned(),
            data: DataCheck::NotChecked,
        },
    );
    inputs.hot.partitions.insert(partition(path));
}

fn on(inputs: &mut RebuildInputs, root: usize, path: &str, copy: RootCopy) {
    inputs.roots[root].files.insert(path.to_owned(), copy);
}

fn record(kind: Kind) -> Record {
    Record {
        at_ms: AT - 1,
        kind,
    }
}

/// A ledger of `kinds` after the header.
fn ledger(kinds: &[Kind]) -> Vec<u8> {
    let mut builder = LedgerBuilder::empty();
    for kind in kinds {
        builder.push(&record(kind.clone())).unwrap();
    }
    builder.finish()
}

fn manifest(text: &str) -> Kind {
    Kind::Manifest {
        text: text.to_owned(),
    }
}

fn copy(path: &str, root: &str) -> Kind {
    Kind::Copy {
        path: path.to_owned(),
        root: root.to_owned(),
    }
}

fn removed(partition: &str) -> Kind {
    Kind::Removed {
        partition: partition.to_owned(),
    }
}

fn start_on(inputs: &mut RebuildInputs, i: usize, bytes: Vec<u8>) {
    inputs.ledger_copies[i].bytes = Some(bytes);
}

/// The records the derivation added, by kind and content.
fn added(derived: &Derived) -> Vec<Kind> {
    derived
        .added
        .iter()
        .map(|record| record.kind.clone())
        .collect()
}

fn derive_ok(inputs: &RebuildInputs) -> Derived {
    match derive(inputs) {
        Ok(derived) => derived,
        Err(Stop(why)) => panic!("stopped: {why}"),
    }
}

#[track_caller]
fn assert_problem(derived: &Derived, needles: &[&str]) {
    assert!(
        derived
            .problems
            .iter()
            .any(|p| needles.iter().all(|n| p.contains(n))),
        "no problem with {needles:?} in {:#?}",
        derived.problems
    );
}

/// Records that name no path: neither a lost source nor a copy.
fn filler(n: usize) -> Vec<Kind> {
    (0..n)
        .map(|i| Kind::Verify {
            root: "cold-a".to_owned(),
            prefix_bytes: 18 + i as u64,
            findings: 0,
        })
        .collect()
}

/// A start copy of complete records plus 37 torn bytes starts at the
/// complete length, and with nothing to add the rebuilt ledger equals it.
#[test]
fn a_torn_last_record_is_dropped_from_the_start() {
    let mut inputs = inputs();
    let complete = ledger(&filler(12));
    let mut torn = complete.clone();
    torn.extend_from_slice(&b"verify 1 cold-a 18 0 0123456789abcdef0123"[..37]);
    start_on(&mut inputs, 0, torn);
    let derived = derive_ok(&inputs);
    assert_eq!(derived.start.root.as_deref(), Some("cold-a"));
    assert_eq!(derived.start.len, complete.len() as u64);
    assert_eq!(derived.start.records, 12);
    assert_eq!(derived.ledger, complete);
    assert!(derived.added.is_empty());
}

/// B2: the longest verified prefix; a copy failing its chain at record 5
/// is rejected naming the record; on a tie, configuration order; no
/// candidate is the header alone, said so.
#[test]
fn start_choice_longest_verified_then_configuration_order() {
    let short = ledger(&filler(10));
    let mut broken = ledger(&filler(14));
    let offset = {
        let mut offset = copy_ledger::HEADER.len();
        for _ in 0..4 {
            offset += broken[offset..].iter().position(|&b| b == b'\n').unwrap() + 1;
        }
        offset
    };
    broken[offset + 2] ^= 0x01;
    let mut inputs = inputs();
    start_on(&mut inputs, 0, broken);
    start_on(&mut inputs, 1, short.clone());
    let derived = derive_ok(&inputs);
    assert_eq!(derived.start.root.as_deref(), Some("cold-b"));
    assert_eq!(derived.start.len, short.len() as u64);
    assert!(
        derived
            .rejected
            .iter()
            .any(|r| r.contains("cold-a") && r.contains("record 5")),
        "{:?}",
        derived.rejected
    );

    // Two verifying copies of one length with other bytes: the first.
    let other = ledger(&filler(10).into_iter().rev().collect::<Vec<_>>());
    assert_eq!(other.len(), short.len());
    let mut inputs = super::tests::inputs();
    start_on(&mut inputs, 0, other.clone());
    start_on(&mut inputs, 1, short.clone());
    assert_eq!(derive_ok(&inputs).ledger, other);

    // The longer one wins whatever its position.
    let long = ledger(&filler(11));
    let mut inputs = super::tests::inputs();
    start_on(&mut inputs, 0, short);
    start_on(&mut inputs, 1, long.clone());
    assert_eq!(derive_ok(&inputs).ledger, long);

    // None: the header alone, said so; every rejection named.
    let mut inputs = super::tests::inputs();
    start_on(&mut inputs, 1, b"mie-copy-ledger 9\n".to_vec());
    let derived = derive_ok(&inputs);
    assert_eq!(derived.start.root, None);
    assert_eq!(derived.ledger, copy_ledger::HEADER.as_bytes());
    assert_eq!(derived.rejected.len(), 2, "{:?}", derived.rejected);
    assert!(
        derived
            .rejected
            .iter()
            .any(|r| r.contains("cold-a") && r.contains("absent"))
    );
    assert!(derived.lines.iter().any(|l| l.contains("started empty")));
}

/// A store whose start copy already records every copy and removal: the
/// rebuild adds nothing and the ledger byte-equals the start.
#[test]
fn a_rebuild_from_a_current_copy_adds_nothing() {
    let path = live("2026-10-02", 0);
    let t = text(&path, &sha("a"));
    let start = ledger(&[
        manifest(&t),
        copy(&path, "cold-a"),
        copy(&path, "cold-b"),
        removed(&partition(&path)),
    ]);
    let mut inputs = inputs();
    on(&mut inputs, 0, &path, good(&t));
    on(&mut inputs, 1, &path, good(&t));
    inputs.journal.names.push(JournalName {
        path: path.clone(),
        line: 3,
        run_id: "r1".to_owned(),
        recovery: false,
    });
    start_on(&mut inputs, 0, start.clone());
    start_on(&mut inputs, 1, start.clone());
    let derived = derive_ok(&inputs);
    assert!(derived.added.is_empty(), "{:?}", derived.added);
    assert_eq!(derived.ledger, start);
    assert_eq!(derived.problems, Vec::<String>::new());
}

/// Equal inputs give byte-identical ledgers, in the B9 order: per path
/// `manifest` then `copy` per target, then `lost`, `removed`, `hold`.
#[test]
fn records_are_appended_in_a_fixed_order() {
    let (p1, p2) = (live("2026-10-02", 0), live("2026-10-02", 1));
    let gone = live("2026-10-02", 2);
    let archive = part("aggTrades", "2026-10-02", 0);
    let mut inputs = inputs();
    inputs.record_lost = true;
    inputs.record_unlisted = true;
    for path in [&p2, &p1, &archive] {
        let t = text(path, &sha(path));
        on(&mut inputs, 1, path, good(&t));
        on(&mut inputs, 0, path, good(&t));
    }
    inputs.journal.names.push(JournalName {
        path: gone.clone(),
        line: 7,
        run_id: "r1".to_owned(),
        recovery: false,
    });
    let first = derive_ok(&inputs);
    let second = derive_ok(&inputs);
    assert_eq!(first.ledger, second.ledger);
    let names: Vec<String> = added(&first)
        .iter()
        .map(|kind| match kind {
            Kind::Manifest { text } => format!(
                "manifest {}",
                replica::parse_manifest(text).unwrap().relative_path
            ),
            Kind::Copy { path, root } => format!("copy {path} {root}"),
            Kind::Lost { path, .. } => format!("lost {path}"),
            Kind::Removed { partition } => format!("removed {partition}"),
            Kind::Hold { path, .. } => format!("hold {path}"),
            other => format!("{other:?}"),
        })
        .collect();
    let mut expected = Vec::new();
    for path in [&archive, &p1, &p2] {
        expected.push(format!("manifest {path}"));
        expected.push(format!("copy {path} cold-a"));
        expected.push(format!("copy {path} cold-b"));
    }
    expected.push(format!("lost {gone}"));
    expected.push(format!("removed {}", partition(&archive)));
    expected.push(format!("removed {}", partition(&p1)));
    expected.push(format!("hold {archive}"));
    assert_eq!(names, expected);
    assert!(first.added.iter().all(|record| record.at_ms == AT));
}

/// A `copy` record the start holds is never repeated, with or without an
/// open finding after it, and the finding stays open.
#[test]
fn a_copy_the_start_records_gets_no_second_record() {
    let path = live("2026-10-06", 0);
    let t = text(&path, &sha("a"));
    let mut inputs = inputs();
    on_hot(&mut inputs, &path, &t);
    on(&mut inputs, 0, &path, good(&t));
    on(&mut inputs, 1, &path, good(&t));
    start_on(
        &mut inputs,
        0,
        ledger(&[manifest(&t), copy(&path, "cold-a")]),
    );
    let derived = derive_ok(&inputs);
    assert_eq!(added(&derived), vec![copy(&path, "cold-b")]);
}

#[test]
fn an_open_finding_is_not_closed_by_the_rebuild() {
    let path = live("2026-10-06", 0);
    let t = text(&path, &sha("a"));
    let mut inputs = inputs();
    on_hot(&mut inputs, &path, &t);
    on(&mut inputs, 0, &path, good(&t));
    on(&mut inputs, 1, &path, good(&t));
    start_on(
        &mut inputs,
        0,
        ledger(&[
            manifest(&t),
            copy(&path, "cold-a"),
            Kind::Finding {
                root: "cold-a".to_owned(),
                code: FindingCode::Sha256,
                subject: Subject::File(path.clone()),
            },
        ]),
    );
    let derived = derive_ok(&inputs);
    assert_eq!(added(&derived), vec![copy(&path, "cold-b")]);
    let rebuilt = copy_ledger::parse(Path::new("rebuilt"), &derived.ledger).unwrap();
    let state = CopyState::fold(&rebuilt).unwrap();
    assert_eq!(
        state.copy(&path, "cold-a").unwrap().open,
        Some(FindingCode::Sha256)
    );
}

/// Two parseable texts are a conflict: a live one stops the rebuild,
/// naming the path, the roots and the start copy; a text that does not
/// parse or names another location is a damaged copy, never a conflict.
#[test]
fn distinct_parseable_texts_are_a_conflict_unparseable_is_damage() {
    let path = live("2026-10-06", 0);
    let (t1, t2) = (text(&path, &sha("1")), text(&path, &sha("2")));
    let other = live("2026-10-06", 1);
    let misplaced = text(&other, &sha("1"));

    let mut inputs = inputs();
    on_hot(&mut inputs, &path, &t1);
    on(
        &mut inputs,
        0,
        &path,
        RootCopy::BadText("does not parse".to_owned()),
    );
    on(&mut inputs, 1, &path, good(&t1));
    let derived = derive_ok(&inputs);
    assert_eq!(added(&derived), vec![manifest(&t1), copy(&path, "cold-b")]);
    assert_problem(
        &derived,
        &[&path, "cold-a", "mie store verify", "mie store repair"],
    );
    // `BadText` is what the read makes of a misplaced text.
    assert_eq!(
        classify_text(&path, misplaced.as_bytes()),
        RootCopy::BadText(format!(
            "the manifest describes {other}, not its own location"
        ))
    );

    let mut inputs = super::tests::inputs();
    on(&mut inputs, 0, &path, good(&t1));
    on(&mut inputs, 1, &path, good(&t2));
    let Err(Stop(why)) = derive(&inputs) else {
        panic!("a live conflict must stop the rebuild");
    };
    assert!(
        why.contains(&path) && why.contains("cold-a") && why.contains("cold-b"),
        "{why}"
    );

    let mut inputs = super::tests::inputs();
    on(&mut inputs, 0, &path, good(&t2));
    start_on(&mut inputs, 1, ledger(&[manifest(&t1)]));
    let Err(Stop(why)) = derive(&inputs) else {
        panic!("a conflict with the start must stop the rebuild");
    };
    assert!(why.contains(&path) && why.contains("start"), "{why}");
}

/// An archive path with two texts is held `manifest-conflict`, never a
/// stop, and from an empty start it gets the text its current entry
/// lists, with `copy` records for verified copies of that text only.
#[test]
fn an_archive_manifest_conflict_becomes_a_hold() {
    let path = part("aggTrades", "2026-10-06", 0);
    let (t1, t2) = (text(&path, &sha("1")), text(&path, &sha("2")));
    let mut inputs = inputs();
    on_hot(&mut inputs, &path, &t1);
    on(&mut inputs, 0, &path, good(&t1));
    on(&mut inputs, 1, &path, good(&t2));
    let derived = derive_ok(&inputs);
    assert_eq!(
        derived.holds,
        vec![(path.clone(), HoldReason::ManifestConflict)]
    );
    assert!(added(&derived).contains(&Kind::Hold {
        path: path.clone(),
        reason: HoldReason::ManifestConflict
    }));
    assert!(
        !added(&derived)
            .iter()
            .any(|k| matches!(k, Kind::Manifest { .. } | Kind::Copy { .. }))
    );
}

#[test]
fn a_held_conflict_records_the_entry_text() {
    let path = part("aggTrades", "2026-10-06", 0);
    let (t1, t2) = (text(&path, &sha("1")), text(&path, &sha("2")));
    let mut inputs = inputs();
    on_hot(&mut inputs, &path, &t1);
    on(&mut inputs, 0, &path, good(&t2));
    on(&mut inputs, 1, &path, good(&t1));
    let listing = entry(&[(&path, &sha("1"))]);
    inputs.entries.insert(
        "aggTrades/x.zip.import".to_owned(),
        vec![EntryCopy {
            root: "hot".to_owned(),
            bytes: listing,
        }],
    );
    let derived = derive_ok(&inputs);
    assert_eq!(
        added(&derived),
        vec![
            manifest(&t1),
            copy(&path, "cold-b"),
            Kind::Hold {
                path: path.clone(),
                reason: HoldReason::ManifestConflict
            }
        ]
    );
}

/// Names outside the layout's grammar are reported, never recorded, and
/// the rebuild still derives its ledger.
#[test]
fn journal_paths_outside_the_layout_are_reported_never_recorded() {
    let good_path = live("2026-10-06", 0);
    let hostile = [
        good_path.replacen("source=", "ｓource=", 1),
        format!("{good_path}\u{200b}"),
        format!("{good_path} "),
        format!("{good_path}\t"),
        "source=binance-um/../x".to_owned(),
        format!("/{good_path}"),
    ];
    let mut inputs = inputs();
    inputs.record_lost = true;
    for (i, path) in hostile.iter().enumerate() {
        inputs.journal.names.push(JournalName {
            path: path.clone(),
            line: i as u64 + 1,
            run_id: "r1".to_owned(),
            recovery: i % 2 == 1,
        });
    }
    let derived = derive_ok(&inputs);
    assert!(derived.added.is_empty(), "{:?}", derived.added);
    assert!(derived.lost.is_empty());
    for path in &hostile {
        assert_problem(&derived, &[&format!("{path:?}"), "never recorded"]);
    }
}

/// One `lost` record per path whatever names it, with origin ledger >
/// import > journal > recovery, and every source named.
#[test]
fn lost_origin_precedence() {
    let (p, q, r, s) = (
        live("2026-10-06", 0),
        part("aggTrades", "2026-10-02", 0),
        live("2026-10-06", 2),
        live("2026-10-06", 3),
    );
    let r_text = text(&r, &sha("r"));
    let mut inputs = inputs();
    inputs.record_lost = true;
    let name = |path: &str, line: u64, recovery: bool| JournalName {
        path: path.to_owned(),
        line,
        run_id: "r1".to_owned(),
        recovery,
    };
    inputs.journal.names = vec![
        name(&p, 3, false),
        name(&p, 4, true),
        name(&q, 5, false),
        name(&r, 6, false),
        name(&s, 7, true),
    ];
    inputs.entries.insert(
        "aggTrades/x.zip.import".to_owned(),
        vec![EntryCopy {
            root: "hot".to_owned(),
            bytes: entry(&[(&q, &sha("q"))]),
        }],
    );
    start_on(&mut inputs, 0, ledger(&[manifest(&r_text)]));
    let derived = derive_ok(&inputs);
    let lost: Vec<(String, LostOrigin, Option<String>)> = added(&derived)
        .into_iter()
        .filter_map(|kind| match kind {
            Kind::Lost {
                path,
                origin,
                sha256,
            } => Some((path, origin, sha256)),
            _ => None,
        })
        .collect();
    assert_eq!(
        lost,
        vec![
            (q.clone(), LostOrigin::Import, Some(sha("q"))),
            (p.clone(), LostOrigin::Journal, None),
            (r.clone(), LostOrigin::Ledger, Some(sha("r"))),
            (s.clone(), LostOrigin::Recovery, None),
        ]
    );
    let p_lost = derived.lost.iter().find(|l| l.path == p).unwrap();
    assert!(
        p_lost
            .sources
            .iter()
            .any(|s| s.contains("line 3") && s.contains("r1"))
            && p_lost.sources.iter().any(|s| s.contains("line 4")),
        "{:?}",
        p_lost.sources
    );
    let r_lost = derived.lost.iter().find(|l| l.path == r).unwrap();
    assert!(
        r_lost.sources.iter().any(|s| s.contains("line 6")),
        "{:?}",
        r_lost.sources
    );

    // Without --record-lost: the same candidates, nothing recorded.
    inputs.record_lost = false;
    let derived = derive_ok(&inputs);
    assert_eq!(derived.lost.len(), 4);
    assert!(derived.lost.iter().all(|l| !l.recorded));
    assert!(
        !added(&derived)
            .iter()
            .any(|k| matches!(k, Kind::Lost { .. }))
    );
}

/// A path the start records lost that a root holds again stays lost: no
/// record for it, and it is named.
#[test]
fn a_path_the_start_records_lost_stays_lost() {
    let path = live("2026-10-06", 0);
    let t = text(&path, &sha("a"));
    let mut inputs = inputs();
    inputs.record_lost = true;
    on(&mut inputs, 0, &path, good(&t));
    inputs.hot.partitions.insert(partition(&path));
    start_on(
        &mut inputs,
        0,
        ledger(&[Kind::Lost {
            path: path.clone(),
            origin: LostOrigin::Journal,
            sha256: None,
        }]),
    );
    let derived = derive_ok(&inputs);
    assert!(derived.added.is_empty(), "{:?}", derived.added);
    assert_problem(&derived, &[&path, "stays lost"]);
}

/// B7′ (#210): a start hold the sources no longer derive is cleared only on
/// positive evidence in the import ledger; every other one stays held and
/// is named; a cleared hold the sources derive again gets a new `hold`.
#[test]
fn a_start_hold_is_cleared_only_on_positive_evidence() {
    let path = part("aggTrades", "2026-10-06", 0);
    let t = text(&path, &sha("a"));
    let listing = entry(&[(&path, &sha("a"))]);
    let hot_copy = |bytes: &[u8]| {
        vec![EntryCopy {
            root: "hot".to_owned(),
            bytes: bytes.to_vec(),
        }]
    };
    let base = |reason: HoldReason, entries: &[(&str, Vec<EntryCopy>)]| {
        let mut inputs = inputs();
        on_hot(&mut inputs, &path, &t);
        for (name, copies) in entries {
            inputs.entries.insert((*name).to_owned(), copies.clone());
        }
        start_on(
            &mut inputs,
            0,
            ledger(&[Kind::Hold {
                path: path.clone(),
                reason,
            }]),
        );
        inputs
    };
    let held_after = |derived: &Derived| {
        let rebuilt = copy_ledger::parse(Path::new("rebuilt"), &derived.ledger).unwrap();
        CopyState::fold(&rebuilt).unwrap().hold_reason(&path)
    };
    let cleared = Kind::HoldCleared {
        path: path.clone(),
        resolution: Resolution::Readmit,
    };

    // An `unlisted` start hold with a current E0 listing it: cleared.
    let derived = derive_ok(&base(HoldReason::Unlisted, &[(NAME, hot_copy(&listing))]));
    assert_eq!(added(&derived), vec![cleared.clone()]);
    assert!(
        derived
            .lines
            .contains(&format!("hold-cleared {path} readmit")),
        "{:?}",
        derived.lines
    );
    assert!(
        !derived.problems.iter().any(|p| p.contains(&path)),
        "{:?}",
        derived.problems
    );
    assert_eq!(held_after(&derived), None);

    // An `entry-conflict` start hold with E1 (written by a resolve) current:
    // cleared.
    let derived = derive_ok(&base(
        HoldReason::EntryConflict,
        &[
            (NAME, hot_copy(&entry(&[(&path, &sha("other"))]))),
            (S1, hot_copy(&supersede())),
            (E1, hot_copy(&listing)),
        ],
    ));
    assert_eq!(added(&derived), vec![cleared.clone()]);
    assert_eq!(held_after(&derived), None);

    // An `entry-conflict` start hold whose E0 copies merely agree now: it
    // stays, named.
    let derived = derive_ok(&base(
        HoldReason::EntryConflict,
        &[(NAME, hot_copy(&listing))],
    ));
    assert!(derived.added.is_empty(), "{:?}", derived.added);
    assert_problem(&derived, &[&path, "stays held", "mie store resolve"]);
    assert_eq!(held_after(&derived), Some(HoldReason::EntryConflict));

    // A `manifest-conflict` start hold: it stays, even with a current entry.
    let derived = derive_ok(&base(
        HoldReason::ManifestConflict,
        &[
            (NAME, hot_copy(&entry(&[(&path, &sha("other"))]))),
            (S1, hot_copy(&supersede())),
            (E1, hot_copy(&listing)),
        ],
    ));
    assert!(derived.added.is_empty(), "{:?}", derived.added);
    assert_problem(&derived, &[&path, "stays held"]);
    assert_eq!(held_after(&derived), Some(HoldReason::ManifestConflict));

    // No entry at all: an absence never clears an `unlisted` hold. The
    // sources derive it again, so it is no leftover either.
    let derived = derive_ok(&base(HoldReason::Unlisted, &[]));
    assert!(derived.added.is_empty(), "{:?}", derived.added);
    assert_eq!(held_after(&derived), Some(HoldReason::Unlisted));

    // A cleared start hold that the sources derive again: a new `hold`.
    let mut inputs = inputs();
    inputs.record_unlisted = true;
    on_hot(&mut inputs, &path, &t);
    start_on(
        &mut inputs,
        0,
        ledger(&[
            Kind::Hold {
                path: path.clone(),
                reason: HoldReason::Unlisted,
            },
            cleared.clone(),
        ]),
    );
    let derived = derive_ok(&inputs);
    assert_eq!(
        added(&derived),
        vec![Kind::Hold {
            path: path.clone(),
            reason: HoldReason::Unlisted
        }]
    );
    assert_eq!(held_after(&derived), Some(HoldReason::Unlisted));
}

/// B8 at the hot window's first date (now 2026-11-10, first hot date
/// 2026-11-03): the day before is recorded removed quietly, the first hot
/// date is recorded and named; a partition the start records removed that
/// is still on hot gets no record.
#[test]
fn hot_window_boundary() {
    let (before, first) = (live("2026-11-02", 0), live("2026-11-03", 0));
    let back = live("2026-10-20", 0);
    let mut inputs = inputs();
    inputs.first_hot_date = "2026-11-03".to_owned();
    for path in [&before, &first, &back] {
        let t = text(path, &sha(path));
        on(&mut inputs, 0, path, good(&t));
        on(&mut inputs, 1, path, good(&t));
    }
    let t = text(&back, &sha(&back));
    on_hot(&mut inputs, &back, &t);
    start_on(
        &mut inputs,
        0,
        ledger(&[
            manifest(&t),
            copy(&back, "cold-a"),
            copy(&back, "cold-b"),
            removed(&partition(&back)),
        ]),
    );
    let derived = derive_ok(&inputs);
    let removals: Vec<Kind> = added(&derived)
        .into_iter()
        .filter(|kind| matches!(kind, Kind::Removed { .. }))
        .collect();
    assert_eq!(
        removals,
        vec![removed(&partition(&before)), removed(&partition(&first))]
    );
    assert_problem(&derived, &[&partition(&first), "hot window", "restart"]);
    assert!(
        !derived
            .problems
            .iter()
            .any(|p| p.contains(&partition(&before))),
        "{:?}",
        derived.problems
    );
}

/// B1: a line cut at the length read is not parsed; a complete line that
/// is no JSON is counted and skipped; `sealed` and `recovery` names keep
/// their line numbers and run ids.
#[test]
fn journal_line_cut_at_the_read_length_is_ignored() {
    let p = live("2026-10-06", 0);
    let q = live("2026-10-06", 1);
    let bytes = format!(
        "{{\"type\":\"run_start\",\"run_id\":\"r1\"}}\n\
         {{\"type\":\"sealed\",\"run_id\":\"r1\",\"files\":[{{\"path\":\"{p}\"}}]}}\n\
         {{\"type\":\"sealed\",\"run_id\":\"r1\",\"files\":[{{\"path\":\"{q}\"}}]}}"
    );
    let read = parse_journal(bytes.as_bytes());
    assert_eq!(read.bytes, bytes.len() as u64);
    assert_eq!(read.non_json, 0);
    assert_eq!(
        read.names,
        vec![JournalName {
            path: p,
            line: 2,
            run_id: "r1".to_owned(),
            recovery: false,
        }]
    );
}

#[test]
fn non_json_journal_lines_are_counted_not_parsed() {
    let p = live("2026-10-06", 0);
    let q = live("2026-10-06", 1);
    let bytes = format!(
        "{{\"type\":\"sealed\",\"run_id\":\"r1\",\"files\":[{{\"path\":\"{p}\"}}\n\
         not json {q}\n\
         \u{fffd}\n\
         {{\"type\":\"recovery\",\"run_id\":\"r2\",\"rolled_forward\":[\"{q}\"],\"discarded\":[]}}\n"
    );
    let read = parse_journal(bytes.as_bytes());
    assert_eq!(read.non_json, 3);
    assert_eq!(
        read.names,
        vec![JournalName {
            path: q,
            line: 4,
            run_id: "r2".to_owned(),
            recovery: true,
        }]
    );
}

/// Review of #209, finding 2: a missing hot import-ledger directory stops
/// the rebuild when an archive part exists on a root or in the start, or a
/// copy target holds import entries; otherwise it is no stop.
#[test]
fn a_missing_import_ledger_stops_only_where_it_could_hide_entries() {
    let missing = Some("the configured import ledger".to_owned());
    let mut quiet = inputs();
    quiet.missing_import_ledger = missing.clone();
    on(
        &mut quiet,
        0,
        &live("2026-10-06", 0),
        good(&text(&live("2026-10-06", 0), &sha("a"))),
    );
    quiet
        .hot
        .partitions
        .insert(partition(&live("2026-10-06", 0)));
    derive_ok(&quiet);

    let archive = part("aggTrades", "2026-10-06", 0);
    let mut on_hot_only = inputs();
    on_hot_only.missing_import_ledger = missing.clone();
    on_hot(&mut on_hot_only, &archive, &text(&archive, &sha("a")));
    let mut in_start = inputs();
    in_start.missing_import_ledger = missing.clone();
    start_on(
        &mut in_start,
        0,
        ledger(&[Kind::Hold {
            path: archive.clone(),
            reason: HoldReason::Unlisted,
        }]),
    );
    let mut target_entries = inputs();
    target_entries.missing_import_ledger = missing;
    target_entries.entries.insert(
        NAME.to_owned(),
        vec![EntryCopy {
            root: "cold-a".to_owned(),
            bytes: entry(&[]),
        }],
    );
    for (case, inputs) in [
        ("hot", on_hot_only),
        ("start", in_start),
        ("target entries", target_entries),
    ] {
        let Err(Stop(why)) = derive(&inputs) else {
            panic!("{case}: a missing import ledger must stop the rebuild");
        };
        assert!(
            why.contains("the configured import ledger") && why.contains("does not exist"),
            "{case}: {why}"
        );
    }
}

/// Re-review of #209: a new `unlisted` hold is written only with
/// `--record-unlisted`; without it the candidate is named, the import
/// ledger's counts too, and nothing may be installed. A conflict hold
/// needs no flag.
#[test]
fn unlisted_holds_need_the_flag() {
    let path = part("aggTrades", "2026-10-06", 0);
    let mut inputs = inputs();
    on_hot(&mut inputs, &path, &text(&path, &sha("a")));
    inputs.entries.insert(
        NAME.to_owned(),
        vec![EntryCopy {
            root: "cold-a".to_owned(),
            bytes: entry(&[]),
        }],
    );
    let derived = derive_ok(&inputs);
    assert!(derived.added.is_empty(), "{:?}", derived.added);
    assert_eq!(derived.unrecorded_unlisted, vec![path.clone()]);
    assert_problem(&derived, &[&path, "--record-unlisted"]);
    assert_problem(
        &derived,
        &[
            "the import ledger holds 0 entries",
            "hold 1 entries that hot lacks",
        ],
    );
    inputs.record_unlisted = true;
    let derived = derive_ok(&inputs);
    assert!(derived.unrecorded_unlisted.is_empty());
    // Re-review 2 of #209, finding 1: the flagged run, which writes the
    // holds, names the import ledger's counts too.
    assert_problem(
        &derived,
        &[
            "the import ledger holds 0 entries",
            "hold 1 entries that hot lacks",
        ],
    );
    assert_eq!(
        added(&derived),
        vec![Kind::Hold {
            path,
            reason: HoldReason::Unlisted
        }]
    );
}

/// Re-review 2 of #209, finding 2: with `--record-lost` alone and an
/// unrecorded `unlisted` candidate the run installs nothing, so no line
/// may say a record was recorded or added; the derivation's lines speak of
/// derived records, and only the install reports what it wrote.
#[test]
fn a_derivation_never_claims_a_record_was_written() {
    let gone = live("2026-10-06", 5);
    let archive = part("aggTrades", "2026-10-06", 0);
    let mut inputs = inputs();
    inputs.record_lost = true;
    inputs.journal.names.push(JournalName {
        path: gone.clone(),
        line: 2,
        run_id: "r1".to_owned(),
        recovery: false,
    });
    on_hot(&mut inputs, &archive, &text(&archive, &sha("a")));
    let derived = derive_ok(&inputs);
    assert_eq!(derived.unrecorded_unlisted, vec![archive]);
    assert_eq!(derived.lost.len(), 1);
    for line in derived.lines.iter().chain(&derived.problems) {
        assert!(
            !line.contains("recorded lost")
                && !line.contains("records added")
                && !line.contains("from now on"),
            "{line}"
        );
    }
    assert!(
        derived
            .lines
            .iter()
            .any(|l| l.starts_with("records derived: ") && l.contains("lost 1")),
        "{:?}",
        derived.lines
    );
    assert_problem(
        &derived,
        &[&gone, "--record-lost", "only if this run installs"],
    );
}

/// Q1 to Q6 (#211): a replaced generation whose part sits in hot's
/// quarantine and on cold-a's: the start's hold on it neither stays held
/// nor gives a B7′ line, the start's records name it and no lost candidate
/// comes of it; the quarantine's records follow the others in Q6's order,
/// and equal inputs give byte-identical ledgers.
#[test]
fn equal_inputs_give_byte_identical_ledgers_with_quarantine() {
    use mie_adapter_binance::archive::ledger::{Quarantine, SupersedeRecord};
    use mie_adapter_parquet::replica::QuarantinedCopy;
    let p = part("aggTrades", "2026-10-01", 0);
    let live_path = live("2026-10-06", 0);
    let t = text(&p, &sha("p"));
    let record = SupersedeRecord {
        archive: "data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-01.zip"
            .to_owned(),
        stream: mie_ports::raw::RawStreamKey::new("binance-archive", "BTCUSDT", "aggTrades")
            .unwrap(),
        period: mie_adapter_binance::archive::catalog::Period::Day(
            mie_adapter_binance::archive::catalog::parse_day("2026-10-01").unwrap(),
        ),
        entries: Vec::new(),
        quarantine: Some(Quarantine {
            id: "q-1".to_owned(),
            moved: vec![p.clone()],
        }),
    };
    let mut inputs = inputs();
    inputs.entries.insert(
        S1.to_owned(),
        vec![EntryCopy {
            root: "hot".to_owned(),
            bytes: record.render().into_bytes(),
        }],
    );
    let quarantined = QuarantinedCopy {
        text: Some(t.clone()),
        data: true,
        unreadable: None,
    };
    for listing in std::iter::once(&mut inputs.hot.quarantine)
        .chain(std::iter::once(&mut inputs.roots[0].quarantine))
    {
        listing.ids.insert(
            "q-1".to_owned(),
            BTreeMap::from([(p.clone(), quarantined.clone())]),
        );
    }
    let lt = text(&live_path, &sha("live"));
    on_hot(&mut inputs, &live_path, &lt);
    on(&mut inputs, 0, &live_path, good(&lt));
    start_on(
        &mut inputs,
        0,
        ledger(&[Kind::Hold {
            path: p.clone(),
            reason: HoldReason::Unlisted,
        }]),
    );
    let derived = derive_ok(&inputs);
    assert!(derived.lost.is_empty(), "{derived:#?}");
    assert!(derived.holds.is_empty(), "{derived:#?}");
    assert_eq!(derived.problems, Vec::<String>::new(), "{derived:#?}");
    assert_eq!(
        added(&derived),
        vec![
            manifest(&lt),
            copy(&live_path, "cold-a"),
            manifest(&t),
            Kind::HoldCleared {
                path: p.clone(),
                resolution: Resolution::Replace
            },
            Kind::Quarantined {
                path: p.clone(),
                quarantine: "q-1".to_owned()
            },
        ]
    );
    assert!(
        derived
            .lines
            .iter()
            .any(|l| l.contains("quarantined 1, replaced 0")),
        "{derived:#?}"
    );
    assert!(
        derived
            .lines
            .iter()
            .any(|l| l == "quarantine q-1: 1 path(s), 1 complete, 0 incomplete")
    );
    assert_eq!(derive_ok(&inputs).ledger, derived.ledger);
}

// ------------------------------------------- the hot restore's mode (#195)

/// `inputs` in the hot restore's mode with hot's ledger `hot_ledger` and
/// the copy-back plan `copy_back`.
fn restoring(inputs: &mut RebuildInputs, hot_ledger: Option<Vec<u8>>, copy_back: &[&str]) {
    inputs.mode = Mode::Restore(RestoreMode {
        hot_ledger,
        copy_back: copy_back.iter().map(|p| (*p).to_owned()).collect(),
    });
}

/// H5 a: hot's surviving ledger is a start candidate, and the longest
/// verified prefix wins: hot's 15 records against the targets' 10 and 12.
#[test]
fn l_surviving_hot_ledger_is_a_candidate() {
    let mut inputs = inputs();
    start_on(&mut inputs, 0, ledger(&filler(10)));
    start_on(&mut inputs, 1, ledger(&filler(12)));
    let hot = ledger(&filler(15));
    // The rebuild never reads hot's ledger as a candidate.
    assert_eq!(derive_ok(&inputs).start.root.as_deref(), Some("cold-b"));
    restoring(&mut inputs, Some(hot.clone()), &[]);
    let derived = derive_ok(&inputs);
    assert_eq!(derived.start.root.as_deref(), Some("hot"));
    assert_eq!(derived.start.len, hot.len() as u64);
    assert_eq!(derived.ledger, hot);
}

/// H5 a: on equal verified lengths hot wins, then configuration order.
#[test]
fn l_tie_order() {
    let same = ledger(&filler(12));
    let mut inputs = inputs();
    start_on(&mut inputs, 0, same.clone());
    start_on(&mut inputs, 1, same.clone());
    restoring(&mut inputs, Some(same.clone()), &[]);
    assert_eq!(derive_ok(&inputs).start.root.as_deref(), Some("hot"));
    restoring(&mut inputs, None, &[]);
    assert_eq!(derive_ok(&inputs).start.root.as_deref(), Some("cold-a"));
}

/// H5 a: a torn trailing record on the longest copy (hot's) counts with
/// its verified prefix, which then ties with a target's and wins as hot.
#[test]
fn l_torn_trailing_record() {
    let complete = ledger(&filler(12));
    let mut torn = complete.clone();
    torn.extend_from_slice(&b"verify 1 cold-a 18 0 0123456789abcdef0123"[..37]);
    let mut inputs = inputs();
    start_on(&mut inputs, 1, complete.clone());
    restoring(&mut inputs, Some(torn), &[]);
    let derived = derive_ok(&inputs);
    assert_eq!(derived.start.root.as_deref(), Some("hot"));
    assert_eq!(derived.start.len, complete.len() as u64);
    assert_eq!(derived.ledger, complete);
}

/// H5 a: no candidate verifies: the start is the header alone, said so.
#[test]
fn l_empty_start() {
    let mut inputs = inputs();
    start_on(&mut inputs, 0, b"mie-copy-ledger 9\n".to_vec());
    restoring(&mut inputs, Some(b"not a ledger".to_vec()), &[]);
    let derived = derive_ok(&inputs);
    assert_eq!(derived.start.root, None);
    assert!(
        derived.lines[0].starts_with("started empty"),
        "{derived:#?}"
    );
    assert!(
        derived.rejected.iter().any(|r| r.contains("on hot")),
        "{:?}",
        derived.rejected
    );
}

/// H5 b: a live path with two texts and a start record: the start's text
/// wins, the other copy is a damaged copy with a `FAIL:` line and no
/// record. The rebuild stops on the same inputs.
#[test]
fn d_live_conflict_with_record_uses_it() {
    let path = live("2026-10-06", 0);
    let (sealed, other) = (text(&path, &sha("a")), text(&path, &sha("b")));
    let mut inputs = inputs();
    start_on(
        &mut inputs,
        0,
        ledger(&[manifest(&sealed), copy(&path, "cold-a")]),
    );
    on(&mut inputs, 0, &path, good(&sealed));
    on(&mut inputs, 1, &path, good(&other));
    assert!(derive(&inputs).is_err(), "the rebuild stops");
    restoring(&mut inputs, None, &[&partition(&path)]);
    let derived = derive_ok(&inputs);
    assert_eq!(added(&derived), Vec::<Kind>::new());
    assert_problem(
        &derived,
        &[
            &path,
            "cold-b",
            "differs from the copy ledger's manifest record",
            "another self-consistent file under the path (D5)",
        ],
    );
}

/// H5 b: without a start record a live path with two texts stops the
/// restore, nothing derived.
#[test]
fn d_live_conflict_without_record_stops() {
    let path = live("2026-10-06", 0);
    let mut inputs = inputs();
    on(&mut inputs, 0, &path, good(&text(&path, &sha("a"))));
    on(&mut inputs, 1, &path, good(&text(&path, &sha("b"))));
    restoring(&mut inputs, None, &[&partition(&path)]);
    let Err(Stop(why)) = derive(&inputs) else {
        panic!("no stop");
    };
    assert!(
        why.contains(&path) && why.contains("different manifest texts"),
        "{why}"
    );
}

/// H5 b leaves archive paths alone: two texts and a start record are a
/// `manifest-conflict` hold, both copies left, nothing recorded for them.
#[test]
fn d_archive_conflict_is_held_not_copied() {
    let path = part("aggTrades", "2026-10-06", 0);
    let (sealed, other) = (text(&path, &sha("a")), text(&path, &sha("b")));
    let mut inputs = inputs();
    start_on(&mut inputs, 0, ledger(&[manifest(&sealed)]));
    on(&mut inputs, 0, &path, good(&sealed));
    on(&mut inputs, 1, &path, good(&other));
    restoring(&mut inputs, None, &[&partition(&path)]);
    let derived = derive_ok(&inputs);
    assert_eq!(
        derived.holds,
        vec![(path.clone(), HoldReason::ManifestConflict)]
    );
    assert!(
        !derived
            .problems
            .iter()
            .any(|p| p.contains("differs from the copy ledger's manifest record")),
        "{:#?}",
        derived.problems
    );
}

/// An unrecorded cold copy with a bad SHA-256: no `copy` record, a `FAIL:`
/// line; the good one is recorded.
#[test]
fn d_unrecorded_damaged_copy() {
    let path = live("2026-10-06", 0);
    let sealed = text(&path, &sha("a"));
    let mut inputs = inputs();
    on(&mut inputs, 0, &path, good(&sealed));
    on(
        &mut inputs,
        1,
        &path,
        RootCopy::Text {
            text: sealed.clone(),
            data: DataCheck::Sha256,
        },
    );
    restoring(&mut inputs, None, &[&partition(&path)]);
    let derived = derive_ok(&inputs);
    assert_eq!(
        added(&derived),
        vec![manifest(&sealed), copy(&path, "cold-a")]
    );
    assert_problem(
        &derived,
        &[
            &path,
            "cold-b",
            "sha256",
            "does not check this unrecorded copy",
        ],
    );
}

/// H5 c: in the restore an `unlisted` hold is written without the flag,
/// and the rebuild's import-ledger evidence line is not printed: the
/// restore restores that ledger itself.
#[test]
fn d_unlisted_parts_are_held() {
    let path = part("aggTrades", "2026-10-06", 0);
    let mut inputs = inputs();
    on(&mut inputs, 0, &path, good(&text(&path, &sha("a"))));
    restoring(&mut inputs, None, &[&partition(&path)]);
    let derived = derive_ok(&inputs);
    assert!(derived.unrecorded_unlisted.is_empty());
    assert!(added(&derived).contains(&Kind::Hold {
        path: path.clone(),
        reason: HoldReason::Unlisted
    }));
    assert!(
        !derived
            .problems
            .iter()
            .any(|p| p.contains("--record-unlisted")),
        "{:#?}",
        derived.problems
    );
}

/// H5 d: a journal-named path no root holds is a lost candidate, recorded
/// only with `--record-lost`; the restore's text names its own flag.
#[test]
fn d_lost_needs_the_flag() {
    let path = live("2026-10-06", 4);
    let mut inputs = inputs();
    inputs.journal.names = vec![JournalName {
        path: path.clone(),
        line: 3,
        run_id: "r1".to_owned(),
        recovery: false,
    }];
    restoring(&mut inputs, None, &[]);
    let derived = derive_ok(&inputs);
    assert_eq!(derived.lost.len(), 1);
    assert!(!derived.lost[0].recorded);
    assert!(added(&derived).is_empty());
    assert_problem(&derived, &[&path, "`mie store restore --record-lost`"]);
    inputs.record_lost = true;
    let derived = derive_ok(&inputs);
    assert!(derived.lost[0].recorded);
    assert!(matches!(&added(&derived)[..], [Kind::Lost { path: p, .. }] if *p == path));
}

/// B6 in the restore: a journal path with a look-alike hyphen, `..` or an
/// absolute path is reported, never recorded.
#[test]
fn d_names_outside_the_layout() {
    let mut inputs = inputs();
    inputs.record_lost = true;
    let bad = [
        "source=binance\u{2010}um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00000.parquet",
        "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/../part-00000.parquet",
        "/source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00000.parquet",
    ];
    inputs.journal.names = bad
        .iter()
        .enumerate()
        .map(|(i, path)| JournalName {
            path: (*path).to_owned(),
            line: i as u64 + 1,
            run_id: "r1".to_owned(),
            recovery: false,
        })
        .collect();
    restoring(&mut inputs, None, &[]);
    let derived = derive_ok(&inputs);
    assert!(added(&derived).is_empty(), "{:?}", derived.added);
    for path in bad {
        assert_problem(
            &derived,
            &[&format!("{path:?}"), "no data path of the layout"],
        );
    }
}

/// H5 e: a partition the copy-back plans to bring back counts as on hot,
/// so it gets no `removed` record and no hot-damage line; without it (the
/// rebuild) it does. A recorded copy the restore did not read counts as
/// verified for B8.
#[test]
fn b8_counts_the_copy_back_as_on_hot() {
    let (window, old) = (live("2026-10-06", 0), live("2026-10-01", 0));
    let mut inputs = inputs();
    for path in [&window, &old] {
        on(&mut inputs, 0, path, good(&text(path, &sha(path))));
    }
    let derived = derive_ok(&inputs);
    assert!(added(&derived).contains(&removed(&partition(&window))));
    assert!(added(&derived).contains(&removed(&partition(&old))));
    restoring(&mut inputs, None, &[&partition(&window)]);
    let derived = derive_ok(&inputs);
    assert!(!added(&derived).contains(&removed(&partition(&window))));
    assert!(added(&derived).contains(&removed(&partition(&old))));
    assert!(
        !derived.problems.iter().any(|p| p.contains("hot damage")),
        "{:#?}",
        derived.problems
    );
    // A recorded, unread copy of the old partition.
    let old_text = text(&old, &sha(&old));
    start_on(
        &mut inputs,
        0,
        ledger(&[manifest(&old_text), copy(&old, "cold-a")]),
    );
    on(
        &mut inputs,
        0,
        &old,
        RootCopy::Text {
            text: old_text,
            data: DataCheck::NotChecked,
        },
    );
    let derived = derive_ok(&inputs);
    assert!(added(&derived).contains(&removed(&partition(&old))));
}

/// H5 f: a missing hot import ledger stops the rebuild while the targets
/// hold entries, never the restore, which restores it.
#[test]
fn a_missing_hot_import_ledger_does_not_stop_the_restore() {
    let path = part("aggTrades", "2026-10-06", 0);
    let sealed = text(&path, &sha("a"));
    let mut inputs = inputs();
    inputs.missing_import_ledger = Some("the import ledger".to_owned());
    inputs.entries.insert(
        NAME.to_owned(),
        vec![EntryCopy {
            root: "cold-a".to_owned(),
            bytes: entry(&[(&path, &sha("a"))]),
        }],
    );
    on(&mut inputs, 0, &path, good(&sealed));
    assert!(derive(&inputs).is_err());
    restoring(&mut inputs, None, &[&partition(&path)]);
    let derived = derive_ok(&inputs);
    assert_eq!(
        derived.chosen_entries.get(NAME),
        Some(&entry(&[(&path, &sha("a"))]))
    );
    assert!(derived.holds.is_empty(), "{:?}", derived.holds);
}

/// B6a (formerly H5 h, review of #218): a path cold roots hold only as
/// debris (data without its manifest on cold-a, a manifest that does not
/// parse on cold-b) and no record names is a lost candidate, origin
/// `verify`, in both modes; with hot holding both paths, none.
#[test]
fn h_cold_debris_is_a_lost_candidate_when_hot_lacks_the_path() {
    let orphan = live("2026-10-06", 3);
    let bad = live("2026-10-06", 4);
    let mut inputs = inputs();
    inputs.roots[0].orphan_data.insert(orphan.clone());
    on(
        &mut inputs,
        1,
        &bad,
        RootCopy::BadText("the manifest does not parse: x".to_owned()),
    );
    let mut held = inputs.clone();
    on_hot(&mut held, &orphan, &text(&orphan, &sha("a")));
    on_hot(&mut held, &bad, &text(&bad, &sha("b")));
    assert_eq!(derive_ok(&held).lost, Vec::<LostFile>::new());
    for restore in [false, true] {
        if restore {
            restoring(&mut inputs, None, &[]);
        }
        let derived = derive_ok(&inputs);
        let lost: Vec<(&str, LostOrigin, bool)> = derived
            .lost
            .iter()
            .map(|file| (file.path.as_str(), file.origin, file.recorded))
            .collect();
        assert_eq!(
            lost,
            vec![
                (orphan.as_str(), LostOrigin::Verify, false),
                (bad.as_str(), LostOrigin::Verify, false),
            ],
            "restore {restore}"
        );
        assert!(
            derived
                .problems
                .iter()
                .any(|p| p.contains(&orphan) && p.contains("the path must stay counted")),
            "{:?}",
            derived.problems
        );
    }
    inputs.record_lost = true;
    let derived = derive_ok(&inputs);
    assert!(derived.added.iter().any(|record| record.kind
        == Kind::Lost {
            path: orphan.clone(),
            origin: LostOrigin::Verify,
            sha256: None,
        }));
}

/// B6a: debris beside a sealed copy, or under a path a start record
/// names, is no lost candidate: the copy or the record keeps the path
/// counted.
#[test]
fn h_debris_beside_a_copy_or_a_record_is_no_candidate() {
    let path = live("2026-10-06", 3);
    let mut beside = inputs();
    beside.roots[0].orphan_data.insert(path.clone());
    on(&mut beside, 1, &path, good(&text(&path, &sha("a"))));
    restoring(&mut beside, None, &[]);
    assert_eq!(derive_ok(&beside).lost, Vec::<LostFile>::new());

    let mut inputs = inputs();
    inputs.roots[0].orphan_data.insert(path.clone());
    start_on(
        &mut inputs,
        1,
        ledger(&[Kind::Manifest {
            text: text(&path, &sha("a")),
        }]),
    );
    restoring(&mut inputs, None, &[]);
    assert_eq!(derive_ok(&inputs).lost, Vec::<LostFile>::new());
}

/// B6a rule 2 (formerly H5 h): an unrecorded sealed copy on a read-only
/// root gets its `manifest` record when hot lacks the path, in both modes
/// (so the restore's copy-back brings it to hot), never while hot holds
/// it.
#[test]
fn h_unrecorded_read_only_copy_gets_its_manifest_when_hot_lacks_it() {
    let path = live("2026-10-06", 3);
    let manifest = text(&path, &sha("a"));
    let mut inputs = inputs();
    inputs.roots[0].copy_target = false;
    on(
        &mut inputs,
        0,
        &path,
        RootCopy::Text {
            text: manifest.clone(),
            data: DataCheck::NotChecked,
        },
    );
    let mut held = inputs.clone();
    on_hot(&mut held, &path, &manifest);
    assert_eq!(derive_ok(&held).added, Vec::<Record>::new());
    for restore in [false, true] {
        if restore {
            restoring(&mut inputs, None, &[&partition(&path)]);
        }
        let added: Vec<Kind> = derive_ok(&inputs)
            .added
            .into_iter()
            .map(|record| record.kind)
            .collect();
        assert_eq!(
            added,
            vec![Kind::Manifest {
                text: manifest.clone()
            }],
            "restore {restore}"
        );
    }
}

// ------------------------------------- B6a: cold-only paths (#195 amendment)

/// The part path of the B6a cases: `part-00003` of the live date.
fn p3() -> String {
    live("2026-10-06", 3)
}

/// Gives cold root `root` the temp forms of `path`: `D~` with `data_tmp`,
/// and the pending manifest `pending`.
fn temps(
    inputs: &mut RebuildInputs,
    root: usize,
    path: &str,
    data_tmp: bool,
    pending: Option<RootCopy>,
) {
    inputs.roots[root]
        .temps
        .insert(path.to_owned(), TempForms { data_tmp, pending });
}

/// A pending manifest with `text` whose data checked `data`.
fn pending(text: &str, data: DataCheck) -> Option<RootCopy> {
    Some(RootCopy::Text {
        text: text.to_owned(),
        data,
    })
}

/// The three settings every `b6a_*` case runs in: the restore, the
/// rebuild with an empty hot, and the rebuild with hot holding `path`
/// with `hot_text` (which must give today's output).
fn settings(
    base: &RebuildInputs,
    path: &str,
    hot_text: &str,
) -> [(&'static str, RebuildInputs); 3] {
    let mut restore = base.clone();
    restoring(&mut restore, None, &[&partition(path)]);
    let rebuild = base.clone();
    let mut hot = base.clone();
    on_hot(&mut hot, path, hot_text);
    [
        ("restore", restore),
        ("rebuild", rebuild),
        ("hot holds it", hot),
    ]
}

/// `inputs` without the B6a reads (temps and evidence): what the
/// derivation saw before B6a.
fn without_b6a_reads(inputs: &RebuildInputs) -> RebuildInputs {
    let mut inputs = inputs.clone();
    inputs.hot.evidence.clear();
    for root in &mut inputs.roots {
        root.temps.clear();
        root.evidence.clear();
    }
    inputs
}

/// The records added for `path` (manifest, copy, lost), in order.
fn records_of(derived: &Derived, path: &str) -> Vec<Kind> {
    added(derived)
        .into_iter()
        .filter(|kind| named_path(kind).as_deref() == Some(path))
        .collect()
}

/// The lost candidates as `(path, origin)`.
fn lost_of(derived: &Derived) -> Vec<(String, LostOrigin)> {
    derived
        .lost
        .iter()
        .map(|file| (file.path.clone(), file.origin))
        .collect()
}

#[track_caller]
fn assert_no_problem(derived: &Derived, needles: &[&str]) {
    assert!(
        !derived
            .problems
            .iter()
            .any(|p| needles.iter().all(|n| p.contains(n))),
        "a problem with {needles:?} in {:#?}",
        derived.problems
    );
}

/// With hot holding the path, the B6a reads change nothing: the output
/// equals the derivation over the same inputs without them.
#[track_caller]
fn assert_hot_unchanged(hot: &RebuildInputs) {
    assert_eq!(derive_ok(hot), derive_ok(&without_b6a_reads(hot)));
}

/// Row 1: a sealed copy, `D`+`M`, the data verified on a copy target:
/// `manifest` and `copy`, in every setting; on a read-only root (data not
/// checked in the restore) `manifest` alone, which only hot's copy
/// replaced before B6a.
#[test]
fn b6a_01_sealed_copy() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let mut base = inputs();
    on(&mut base, 0, &p, good(&t));
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        assert_eq!(
            records_of(&derived, &p),
            vec![manifest(&t), copy(&p, "cold-a")],
            "{setting}"
        );
        assert!(derived.lost.is_empty(), "{setting}");
    }
    let mut base = inputs();
    base.roots[1].copy_target = false;
    on(&mut base, 1, &p, good(&t));
    for (setting, mut inputs) in settings(&base, &p, &t) {
        if setting == "restore" {
            on(
                &mut inputs,
                1,
                &p,
                RootCopy::Text {
                    text: t.clone(),
                    data: DataCheck::NotChecked,
                },
            );
        }
        let derived = derive_ok(&inputs);
        let expected = if setting == "hot holds it" {
            vec![]
        } else {
            vec![manifest(&t)]
        };
        assert_eq!(records_of(&derived, &p), expected, "{setting}");
        assert!(derived.lost.is_empty(), "{setting}");
        if setting == "hot holds it" {
            assert_hot_unchanged(&inputs);
        }
    }
}

/// Row 2: a parseable `M` whose data fails (here its SHA-256), and an
/// absent `D` beside a `D~` that does not verify: held with its text, a
/// `FAIL:` naming verify, then repair; never a lost candidate.
#[test]
fn b6a_02_sealed_text_without_good_data() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    for (data, why, data_tmp) in [
        (DataCheck::Sha256, "sha256", false),
        (DataCheck::Missing, "missing", true),
    ] {
        let mut base = inputs();
        on(
            &mut base,
            0,
            &p,
            RootCopy::Text {
                text: t.clone(),
                data: data.clone(),
            },
        );
        temps(&mut base, 0, &p, data_tmp, None);
        for (setting, inputs) in settings(&base, &p, &t) {
            let derived = derive_ok(&inputs);
            assert_eq!(
                records_of(&derived, &p),
                vec![manifest(&t)],
                "{setting} {why}"
            );
            assert!(derived.lost.is_empty(), "{setting} {why}");
            // Round 3: in the restore an unrecorded copy's remedy is the
            // set-aside, not the repair.
            let remedy = if setting == "restore" {
                "does not check this unrecorded copy"
            } else {
                "mie store repair"
            };
            assert_problem(&derived, &[&p, "on cold-a", why, remedy]);
            if setting == "hot holds it" {
                assert_hot_unchanged(&inputs);
            }
        }
    }
}

/// Row 3: `M` with its data only under `D~` (a power loss kept the
/// manifest's rename and lost the data's): the `manifest` record, the
/// copy named `missing` for the repair; the restore's copy-back reads
/// `D~` (run level).
#[test]
fn b6a_03_manifest_with_temp_data() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let mut base = inputs();
    on(
        &mut base,
        0,
        &p,
        RootCopy::Text {
            text: t.clone(),
            data: DataCheck::Missing,
        },
    );
    temps(&mut base, 0, &p, true, None);
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        assert_eq!(records_of(&derived, &p), vec![manifest(&t)], "{setting}");
        assert!(derived.lost.is_empty(), "{setting}");
        // Round 3: the repair cannot fix this unrecorded copy; in the
        // restore H6 k1 sets it aside.
        let remedy = if setting == "restore" {
            "does not check this unrecorded copy"
        } else {
            "mie store repair"
        };
        assert_problem(&derived, &[&p, "on cold-a", "missing", remedy]);
        if setting == "hot holds it" {
            assert_hot_unchanged(&inputs);
        }
    }
}

/// Row 4: a manifest that does not parse, any data: a lost candidate,
/// origin `verify`, plus the damaged copy's `FAIL:`, in both modes; with
/// hot holding the path only the damaged copy.
#[test]
fn b6a_04_bad_manifest() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let mut base = inputs();
    on(
        &mut base,
        0,
        &p,
        RootCopy::BadText("the manifest does not parse: x".to_owned()),
    );
    base.roots[0].orphan_data.insert(p.clone());
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        assert_problem(
            &derived,
            &[&p, "on cold-a", "does not parse", "does not verify"],
        );
        if setting == "hot holds it" {
            assert!(derived.lost.is_empty());
            assert_eq!(records_of(&derived, &p), vec![manifest(&t)]);
            assert_hot_unchanged(&inputs);
        } else {
            assert_eq!(
                lost_of(&derived),
                vec![(p.clone(), LostOrigin::Verify)],
                "{setting}"
            );
            assert_eq!(records_of(&derived, &p), vec![], "{setting}");
            assert_problem(
                &derived,
                &[
                    &p,
                    "manifest on cold-a that does not parse",
                    "--record-lost",
                ],
            );
        }
    }
}

/// Row 5: `D` with a verified pending manifest (a copy crashed after
/// `PublishData`): rolled forward, the `manifest` record with `M~`'s text
/// and no `copy` record, in both modes; no "only a pending manifest"
/// line, since a cold root holds `D` (rule 6, amendment 2).
#[test]
fn b6a_05_data_with_pending_manifest() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let mut base = inputs();
    base.roots[0].orphan_data.insert(p.clone());
    temps(&mut base, 0, &p, false, pending(&t, DataCheck::Good));
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        assert!(derived.lost.is_empty(), "{setting}: {:?}", derived.lost);
        match setting {
            "hot holds it" => {
                assert_eq!(records_of(&derived, &p), vec![]);
                assert_eq!(derived.problems, Vec::<String>::new());
                assert_hot_unchanged(&inputs);
            }
            "restore" => {
                assert_eq!(records_of(&derived, &p), vec![manifest(&t)]);
                assert_no_problem(&derived, &[&p]);
            }
            _ => {
                // Rule 6 (amendment 2): the tiered `read` serves cold-a's
                // `D`, so no "only a pending manifest" line.
                assert_eq!(records_of(&derived, &p), vec![manifest(&t)]);
                assert_no_problem(&derived, &[&p, "from the pending manifest"]);
            }
        }
    }
}

/// Row 6: `D~` and `M~`, both verified (a copy crashed after `Verify` or
/// `MarkReadOnly`): rolled forward as row 5.
#[test]
fn b6a_06_verified_temps() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let mut base = inputs();
    temps(&mut base, 0, &p, true, pending(&t, DataCheck::Good));
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        assert!(derived.lost.is_empty(), "{setting}: {:?}", derived.lost);
        if setting == "hot holds it" {
            assert_eq!(records_of(&derived, &p), vec![]);
            assert_eq!(derived.problems, Vec::<String>::new());
            assert_hot_unchanged(&inputs);
        } else {
            assert_eq!(records_of(&derived, &p), vec![manifest(&t)], "{setting}");
            assert_no_problem(&derived, &[&p, "--record-lost"]);
        }
    }
}

/// Row 7 (amendment 2, rule 1′): a parseable `M~` whose data (`D`, else
/// `D~`) fails gives no text, so the path is a lost candidate, origin
/// `verify`, its line naming the form and the failure; with hot holding
/// the path nothing changes.
#[test]
fn b6a_07_pending_text_without_good_data() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    for (data, data_tmp, orphan) in [
        (DataCheck::Sha256, true, false),
        (DataCheck::Size, false, true),
        (DataCheck::Missing, false, false),
    ] {
        let mut base = inputs();
        temps(&mut base, 0, &p, data_tmp, pending(&t, data.clone()));
        if orphan {
            base.roots[0].orphan_data.insert(p.clone());
        }
        for (setting, inputs) in settings(&base, &p, &t) {
            let derived = derive_ok(&inputs);
            if setting == "hot holds it" {
                assert!(derived.lost.is_empty());
                assert_eq!(records_of(&derived, &p), vec![]);
                assert_eq!(derived.problems, Vec::<String>::new());
                assert_hot_unchanged(&inputs);
            } else {
                assert_eq!(
                    lost_of(&derived),
                    vec![(p.clone(), LostOrigin::Verify)],
                    "{setting} {data:?}"
                );
                assert_eq!(records_of(&derived, &p), vec![], "{setting}");
                assert_problem(
                    &derived,
                    &[&p, "pending manifest on cold-a whose data does not verify"],
                );
            }
        }
    }
}

/// Row 8: `D` alone: a lost candidate, origin `verify`, in both modes.
#[test]
fn b6a_08_data_alone() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let mut base = inputs();
    base.roots[0].orphan_data.insert(p.clone());
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        if setting == "hot holds it" {
            assert!(derived.lost.is_empty());
            assert_hot_unchanged(&inputs);
        } else {
            assert_eq!(
                lost_of(&derived),
                vec![(p.clone(), LostOrigin::Verify)],
                "{setting}"
            );
            assert_problem(
                &derived,
                &[
                    &p,
                    "data file without its manifest on cold-a",
                    "--record-lost",
                ],
            );
        }
    }
}

/// Row 9: `D~` alone: a lost candidate, origin `verify`, in both modes.
#[test]
fn b6a_09_temp_data_alone() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let mut base = inputs();
    temps(&mut base, 1, &p, true, None);
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        if setting == "hot holds it" {
            assert!(derived.lost.is_empty());
            assert_hot_unchanged(&inputs);
        } else {
            assert_eq!(
                lost_of(&derived),
                vec![(p.clone(), LostOrigin::Verify)],
                "{setting}"
            );
            assert_problem(
                &derived,
                &[&p, "the data temp file on cold-b", "--record-lost"],
            );
        }
    }
}

/// Row 10: a pending manifest that does not parse, alone or beside `D` or
/// `D~`: a lost candidate. Its text equal to the path's except for the
/// final `\n` does not parse.
#[test]
fn b6a_10_torn_pending_manifest() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let torn = classify_text(&p, t.strip_suffix('\n').unwrap().as_bytes());
    assert!(matches!(torn, RootCopy::BadText(_)), "{torn:?}");
    for (data_tmp, orphan) in [(false, false), (true, false), (false, true)] {
        let mut base = inputs();
        temps(&mut base, 0, &p, data_tmp, Some(torn.clone()));
        if orphan {
            base.roots[0].orphan_data.insert(p.clone());
        }
        for (setting, inputs) in settings(&base, &p, &t) {
            let derived = derive_ok(&inputs);
            if setting == "hot holds it" {
                assert!(derived.lost.is_empty());
                assert_hot_unchanged(&inputs);
            } else {
                assert_eq!(
                    lost_of(&derived),
                    vec![(p.clone(), LostOrigin::Verify)],
                    "{setting}"
                );
                assert_eq!(records_of(&derived, &p), vec![], "{setting}");
                assert_problem(
                    &derived,
                    &[&p, "pending manifest on cold-a that does not parse"],
                );
            }
        }
    }
}

/// Row 11: parseable pending texts that differ across roots, no `M` and no
/// start text: never B3's stop. A live path is a lost candidate; an
/// archive path takes the text whose SHA-256 the current entry lists, and
/// the other pending copy is named.
#[test]
fn b6a_11_disagreeing_pending_texts() {
    let p = p3();
    let (ta, tb) = (text(&p, &sha("a")), text(&p, &sha("b")));
    let mut base = inputs();
    temps(&mut base, 0, &p, true, pending(&ta, DataCheck::Good));
    temps(&mut base, 1, &p, true, pending(&tb, DataCheck::Good));
    for (setting, inputs) in settings(&base, &p, &ta) {
        let derived = derive_ok(&inputs);
        if setting == "hot holds it" {
            assert!(derived.lost.is_empty());
            assert_hot_unchanged(&inputs);
        } else {
            assert_eq!(
                lost_of(&derived),
                vec![(p.clone(), LostOrigin::Verify)],
                "{setting}"
            );
            assert_eq!(records_of(&derived, &p), vec![], "{setting}");
        }
    }
    let a = part("aggTrades", "2026-10-01", 3);
    let (ta, tb) = (text(&a, &sha("a")), text(&a, &sha("b")));
    let mut base = inputs();
    temps(&mut base, 0, &a, true, pending(&ta, DataCheck::Good));
    temps(&mut base, 1, &a, true, pending(&tb, DataCheck::Good));
    base.entries.insert(
        NAME.to_owned(),
        vec![EntryCopy {
            root: "hot".to_owned(),
            bytes: entry(&[(&a, &sha("b"))]),
        }],
    );
    for (setting, inputs) in settings(&base, &a, &tb) {
        let derived = derive_ok(&inputs);
        assert!(derived.lost.is_empty(), "{setting}: {:?}", derived.lost);
        assert!(derived.holds.is_empty(), "{setting}: {:?}", derived.holds);
        if setting == "hot holds it" {
            assert_eq!(records_of(&derived, &a), vec![]);
            assert_hot_unchanged(&inputs);
        } else {
            assert_eq!(records_of(&derived, &a), vec![manifest(&tb)], "{setting}");
            assert_problem(
                &derived,
                &[&a, "on cold-a (its pending manifest)", "differs"],
            );
        }
    }
}

/// Row 12: a parseable pending manifest whose text differs from the
/// path's decided text (an `M` on another root): the decided text wins,
/// the pending form is a damaged copy named in both modes; with hot
/// holding the path nothing changes.
#[test]
fn b6a_12_pending_text_against_decided_text() {
    let p = p3();
    let (t, other) = (text(&p, &sha("a")), text(&p, &sha("b")));
    let mut base = inputs();
    on(&mut base, 0, &p, good(&t));
    temps(&mut base, 1, &p, true, pending(&other, DataCheck::Good));
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        assert!(derived.lost.is_empty(), "{setting}");
        assert_eq!(
            records_of(&derived, &p),
            vec![manifest(&t), copy(&p, "cold-a")],
            "{setting}"
        );
        if setting == "hot holds it" {
            assert_no_problem(&derived, &["pending manifest"]);
            assert_hot_unchanged(&inputs);
        } else {
            assert_problem(
                &derived,
                &[&p, "on cold-b (its pending manifest)", "differs"],
            );
        }
    }
}

/// Row 13: `D`+`M` with leftover temp names, the pending text equal to
/// the manifest's: `M` decides, the temp names add nothing.
#[test]
fn b6a_13_temps_beside_a_sealed_copy() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let mut base = inputs();
    on(&mut base, 0, &p, good(&t));
    temps(&mut base, 0, &p, true, pending(&t, DataCheck::NotChecked));
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        assert_eq!(
            records_of(&derived, &p),
            vec![manifest(&t), copy(&p, "cold-a")],
            "{setting}"
        );
        assert!(derived.lost.is_empty(), "{setting}");
        assert_no_problem(&derived, &[&p, "pending manifest"]);
        assert_eq!(
            derive_ok(&inputs),
            derive_ok(&without_b6a_reads(&inputs)),
            "{setting}"
        );
    }
}

/// Row 15: a part half moved into `.quarantine/<id>/` (its manifest left
/// in the layout) with leftover temp names and evidence: Q's rules decide
/// it, and the B6a reads change nothing.
#[test]
fn b6a_15_half_moved_quarantine_part() {
    use mie_adapter_parquet::replica::QuarantinedCopy;
    let a = part("aggTrades", "2026-10-01", 3);
    let t = text(&a, &sha("a"));
    let mut base = inputs();
    base.roots[0].quarantine.ids.insert(
        "q-1".to_owned(),
        BTreeMap::from([(
            a.clone(),
            QuarantinedCopy {
                text: None,
                data: true,
                unreadable: None,
            },
        )]),
    );
    on(
        &mut base,
        0,
        &a,
        RootCopy::Text {
            text: t.clone(),
            data: DataCheck::Missing,
        },
    );
    temps(&mut base, 0, &a, true, pending(&t, DataCheck::NotChecked));
    temps(&mut base, 1, &a, true, None);
    base.roots[1]
        .evidence
        .insert(a.clone(), BTreeSet::from(["7".to_owned()]));
    for (setting, inputs) in settings(&base, &a, &t) {
        let derived = derive_ok(&inputs);
        assert!(derived.lost.is_empty(), "{setting}: {:?}", derived.lost);
        assert_eq!(derived, derive_ok(&without_b6a_reads(&inputs)), "{setting}");
    }
}

/// Row 16: part names below `.mie/damaged/<dir>/` name the path but never
/// hold it: an unrecorded path with nothing else is a lost candidate,
/// origin `verify`, one source per directory.
#[test]
fn b6a_16_damaged_evidence() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let mut base = inputs();
    base.roots[0].evidence.insert(
        p.clone(),
        BTreeSet::from(["111".to_owned(), "222".to_owned()]),
    );
    base.hot
        .evidence
        .insert(p.clone(), BTreeSet::from(["333".to_owned()]));
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        if setting == "hot holds it" {
            assert!(derived.lost.is_empty());
            assert_hot_unchanged(&inputs);
            continue;
        }
        assert_eq!(
            lost_of(&derived),
            vec![(p.clone(), LostOrigin::Verify)],
            "{setting}"
        );
        let sources = &derived.lost[0].sources;
        assert_eq!(sources.len(), 3, "{sources:?}");
        for (dir, root) in [("111", "cold-a"), ("222", "cold-a"), ("333", "hot")] {
            assert!(
                sources
                    .iter()
                    .any(|s| s.contains(&format!(".mie/damaged/{dir}/ on {root}"))),
                "{sources:?}"
            );
        }
    }
}

/// Rule 4′ (amendment 2): a path the start records, which hot lacks, is
/// held by a crash form only when it is a verified crash copy: on a root
/// without `M`, an `M~` byte-equal to the start's text whose data
/// verifies, a read-only root's included. In the rebuild it then gets
/// rule 6's "only a pending manifest" line. A `D~` alone, even one that
/// verifies, holds nothing: a lost candidate, origin `ledger`.
#[test]
fn b6a_recorded_path_is_held_only_by_a_verified_crash_copy() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    for read_only in [false, true] {
        let mut base = inputs();
        base.roots[0].copy_target = !read_only;
        start_on(&mut base, 1, ledger(&[manifest(&t)]));
        temps(&mut base, 0, &p, true, pending(&t, DataCheck::Good));
        for (setting, inputs) in settings(&base, &p, &t) {
            let derived = derive_ok(&inputs);
            assert!(derived.lost.is_empty(), "{setting}: {:?}", derived.lost);
            assert_eq!(records_of(&derived, &p), vec![], "{setting}");
            if setting == "rebuild" {
                assert_problem(&derived, &[&p, "from the pending manifest"]);
            } else {
                assert_no_problem(&derived, &[&p, "from the pending manifest"]);
            }
        }
    }
    let mut base = inputs();
    start_on(&mut base, 1, ledger(&[manifest(&t)]));
    temps(&mut base, 0, &p, true, None);
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        if setting == "hot holds it" {
            assert!(derived.lost.is_empty(), "{setting}");
        } else {
            assert_eq!(
                lost_of(&derived),
                vec![(p.clone(), LostOrigin::Ledger)],
                "{setting}: D~ alone"
            );
        }
    }
}

/// Review round 3, finding 2 (rule 4′): a recorded path that hot lacks,
/// in a removed partition outside the copy-back plan, whose cold roots
/// hold only one unverified crash form, (a) a half `D~`, (b) a torn `M~`,
/// (c) an `M~` with another text, (d) an unreadable `M~`, (e) a `D~` alone
/// that verifies, (f) an `M~` equal to the text whose `D~` is one byte
/// short: a lost candidate, origin `ledger`, whose line names the forms;
/// apart from the lines naming the path, the output equals main's (the
/// derivation without the B6a reads). (g) an `M~` equal to the text with a
/// verifying `D~` holds it, and the rebuild names it.
#[test]
fn rule4_unverified_crash_forms_never_hold() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let other = text(&p, &sha("b"));
    let start = ledger(&[
        manifest(&t),
        copy(&p, "cold-a"),
        copy(&p, "cold-b"),
        removed(&partition(&p)),
    ]);
    let cases: [(&str, bool, Option<RootCopy>, &str); 6] = [
        ("(a) a half D~", true, None, "the data temp file on cold-a"),
        (
            "(b) a torn M~",
            false,
            Some(RootCopy::BadText(
                "the pending manifest does not parse: torn".to_owned(),
            )),
            "pending manifest on cold-a that does not parse",
        ),
        (
            "(c) an M~ with another text",
            false,
            pending(&other, DataCheck::Good),
            "a pending manifest on cold-a",
        ),
        (
            "(d) an unreadable M~",
            false,
            Some(RootCopy::BadText(
                "the pending manifest cannot be read: Permission denied".to_owned(),
            )),
            "cannot be read",
        ),
        (
            "(e) a D~ alone that verifies",
            true,
            None,
            "the data temp file on cold-a",
        ),
        (
            "(f) an M~ equal to the text, its D~ one byte short",
            true,
            pending(&t, DataCheck::Size),
            "whose data does not verify",
        ),
    ];
    let without_path = |lines: &[String]| -> Vec<String> {
        lines.iter().filter(|l| !l.contains(&p)).cloned().collect()
    };
    for (case, data_tmp, pending_form, named) in cases {
        let mut base = inputs();
        start_on(&mut base, 1, start.clone());
        temps(&mut base, 0, &p, data_tmp, pending_form);
        let mut restore = base.clone();
        restoring(&mut restore, None, &[]);
        for (setting, inputs) in [("restore", restore), ("rebuild", base)] {
            let derived = derive_ok(&inputs);
            let main = derive_ok(&without_b6a_reads(&inputs));
            assert_eq!(
                lost_of(&derived),
                vec![(p.clone(), LostOrigin::Ledger)],
                "{case} {setting}"
            );
            assert_eq!(lost_of(&derived), lost_of(&main), "{case} {setting}");
            assert!(
                derived.lines.iter().any(|l| l
                    .starts_with(&format!("lost candidate: {p}: origin ledger"))
                    && l.contains(named)),
                "{case} {setting}: {:#?}",
                derived.lines
            );
            assert_eq!(derived.ledger, main.ledger, "{case} {setting}");
            assert_eq!(derived.added, main.added, "{case} {setting}");
            assert_eq!(derived.holds, main.holds, "{case} {setting}");
            assert_eq!(
                without_path(&derived.lines),
                without_path(&main.lines),
                "{case} {setting}"
            );
            assert_eq!(
                without_path(&derived.problems),
                without_path(&main.problems),
                "{case} {setting}"
            );
        }
    }
    // (g) a verified crash copy holds it.
    let mut base = inputs();
    start_on(&mut base, 1, start.clone());
    temps(&mut base, 0, &p, true, pending(&t, DataCheck::Good));
    let mut restore = base.clone();
    restoring(&mut restore, None, &[]);
    for (setting, inputs) in [("restore", restore), ("rebuild", base)] {
        let derived = derive_ok(&inputs);
        assert!(derived.lost.is_empty(), "(g) {setting}: {:?}", derived.lost);
        if setting == "rebuild" {
            assert_problem(&derived, &[&p, "from the pending manifest"]);
        }
    }
}

/// Rule 5: a path the start records lost whose roots hold only crash
/// forms is a report line, not a `FAIL:`; a parseable `M` on a cold root,
/// or hot holding it, still gives the `FAIL:`.
#[test]
fn b6a_lost_path_with_crash_forms_is_a_line() {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let lost = Kind::Lost {
        path: p.clone(),
        origin: LostOrigin::Verify,
        sha256: None,
    };
    let mut base = inputs();
    start_on(&mut base, 1, ledger(std::slice::from_ref(&lost)));
    temps(&mut base, 0, &p, true, pending(&t, DataCheck::NotChecked));
    base.roots[1].orphan_data.insert(p.clone());
    for (setting, inputs) in settings(&base, &p, &t) {
        let derived = derive_ok(&inputs);
        assert!(derived.lost.is_empty(), "{setting}");
        assert_eq!(records_of(&derived, &p), vec![], "{setting}");
        if setting == "hot holds it" {
            assert_problem(&derived, &[&p, "holds it again"]);
            assert_hot_unchanged(&inputs);
        } else {
            assert_no_problem(&derived, &[&p, "holds it again"]);
            assert!(
                derived
                    .lines
                    .iter()
                    .any(|l| l.starts_with(&format!("{p}: recorded lost;"))
                        && l.contains("left in place")),
                "{setting}: {:#?}",
                derived.lines
            );
        }
    }
    let mut sealed = base.clone();
    sealed.roots[1].copy_target = false;
    on(
        &mut sealed,
        1,
        &p,
        RootCopy::Text {
            text: t.clone(),
            data: DataCheck::NotChecked,
        },
    );
    for (setting, inputs) in settings(&sealed, &p, &t) {
        assert_problem(&derive_ok(&inputs), &[&p, "holds it again"]);
        let _ = setting;
    }
}

/// The inputs of state-table row `row` (1 to 16; 14, an empty `date=`
/// directory, has none) for the path `p3()`.
fn row_inputs(row: u32) -> Option<RebuildInputs> {
    let (p, t) = (p3(), text(&p3(), &sha("a")));
    let other = text(&p, &sha("b"));
    let mut base = inputs();
    match row {
        1 => on(&mut base, 0, &p, good(&t)),
        2 => on(
            &mut base,
            0,
            &p,
            RootCopy::Text {
                text: t.clone(),
                data: DataCheck::Sha256,
            },
        ),
        3 => {
            on(
                &mut base,
                0,
                &p,
                RootCopy::Text {
                    text: t.clone(),
                    data: DataCheck::Missing,
                },
            );
            temps(&mut base, 0, &p, true, None);
        }
        4 => {
            on(&mut base, 0, &p, RootCopy::BadText("x".to_owned()));
            base.roots[0].orphan_data.insert(p.clone());
        }
        5 => {
            base.roots[0].orphan_data.insert(p.clone());
            temps(&mut base, 0, &p, false, pending(&t, DataCheck::Good));
        }
        6 => temps(&mut base, 0, &p, true, pending(&t, DataCheck::Good)),
        7 => temps(&mut base, 0, &p, true, pending(&t, DataCheck::Sha256)),
        8 => {
            base.roots[0].orphan_data.insert(p.clone());
        }
        9 => temps(&mut base, 0, &p, true, None),
        10 => temps(
            &mut base,
            0,
            &p,
            false,
            Some(RootCopy::BadText("torn".to_owned())),
        ),
        11 => {
            temps(&mut base, 0, &p, true, pending(&t, DataCheck::Good));
            temps(&mut base, 1, &p, true, pending(&other, DataCheck::Good));
        }
        12 => {
            on(&mut base, 0, &p, good(&t));
            temps(&mut base, 1, &p, true, pending(&other, DataCheck::Good));
        }
        13 => {
            on(&mut base, 0, &p, good(&t));
            temps(&mut base, 0, &p, true, pending(&t, DataCheck::NotChecked));
        }
        15 => {
            use mie_adapter_parquet::replica::QuarantinedCopy;
            let a = part("aggTrades", "2026-10-01", 3);
            let ta = text(&a, &sha("a"));
            base.roots[0].quarantine.ids.insert(
                "q-1".to_owned(),
                BTreeMap::from([(
                    a.clone(),
                    QuarantinedCopy {
                        text: None,
                        data: true,
                        unreadable: None,
                    },
                )]),
            );
            on(
                &mut base,
                0,
                &a,
                RootCopy::Text {
                    text: ta,
                    data: DataCheck::Missing,
                },
            );
            temps(&mut base, 1, &a, true, None);
        }
        16 => {
            base.roots[0]
                .evidence
                .insert(p.clone(), BTreeSet::from(["111".to_owned()]));
        }
        _ => return None,
    }
    Some(base)
}

/// Rule 6 (amendment 2): for every row in both modes (with
/// `--record-lost`, so a row with a candidate installs), a second
/// derivation that starts from the first one's ledger (the restore: as
/// hot's ledger; the rebuild: as a ledger copy) adds no record, has no
/// lost candidate, names the same paths in `FAIL:` lines (apart from the
/// first run's lost candidates, now report lines under rule 5) and, in
/// the restore, copies back the same paths.
#[test]
fn b6a_every_row_rederives_after_its_install() {
    for row in 1..=16 {
        let Some(mut base) = row_inputs(row) else {
            continue;
        };
        base.record_lost = true;
        let paths = [p3(), part("aggTrades", "2026-10-01", 3)];
        let mut restore = base.clone();
        restoring(&mut restore, None, &[&partition(&p3())]);
        for (setting, first_inputs) in [("restore", restore), ("rebuild", base)] {
            let first = derive_ok(&first_inputs);
            let mut again = first_inputs.clone();
            match &mut again.mode {
                Mode::Restore(mode) => mode.hot_ledger = Some(first.ledger.clone()),
                Mode::Rebuild => again.ledger_copies[0].bytes = Some(first.ledger.clone()),
            }
            let second = derive_ok(&again);
            assert_eq!(added(&second), vec![], "row {row} {setting}");
            assert!(second.lost.is_empty(), "row {row} {setting}");
            let lost: BTreeSet<&str> = first.lost.iter().map(|f| f.path.as_str()).collect();
            let failing = |derived: &Derived, skip_lost: bool| -> BTreeSet<String> {
                paths
                    .iter()
                    .filter(|path| !(skip_lost && lost.contains(path.as_str())))
                    .filter(|path| derived.problems.iter().any(|l| l.contains(path.as_str())))
                    .cloned()
                    .collect()
            };
            assert_eq!(
                failing(&second, true),
                failing(&first, true),
                "row {row} {setting}: {:#?} {:#?}",
                first.problems,
                second.problems
            );
            if setting == "restore" {
                let ordinary = |ledger: &[u8]| -> BTreeSet<String> {
                    let parsed = copy_ledger::parse(Path::new("l"), ledger).unwrap();
                    let state = CopyState::fold(&parsed).unwrap();
                    state
                        .paths()
                        .filter(|(path, verdict)| {
                            *verdict == PathVerdict::Ordinary
                                && partition_of(path) == partition(&p3())
                        })
                        .map(|(path, _)| path.to_owned())
                        .collect()
                };
                assert_eq!(
                    ordinary(&second.ledger),
                    ordinary(&first.ledger),
                    "row {row}"
                );
            }
        }
    }
}
