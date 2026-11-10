//! The rebuild's pure derivation (B2 to B8) over constructed inputs, and
//! the journal read (B1).

use super::*;
use crate::store::archive_checks::tests::{entry, part, sha, text};
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
        at_ms: AT,
        first_hot_date: "2026-10-05".to_owned(),
        record_lost: false,
        archive_configured: true,
        ledger_copies: vec![
            LedgerCopyRead {
                id: "cold-a".to_owned(),
                bytes: None,
            },
            LedgerCopyRead {
                id: "cold-b".to_owned(),
                bytes: None,
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

/// A hold the start carries that the sources no longer derive stays held
/// (no clearing record exists before #194) and is named.
#[test]
fn a_start_hold_the_sources_no_longer_derive_stays_held_and_is_named() {
    let path = part("aggTrades", "2026-10-06", 0);
    let t = text(&path, &sha("a"));
    let mut inputs = inputs();
    on_hot(&mut inputs, &path, &t);
    inputs.entries.insert(
        "aggTrades/x.zip.import".to_owned(),
        vec![EntryCopy {
            root: "hot".to_owned(),
            bytes: entry(&[(&path, &sha("a"))]),
        }],
    );
    start_on(
        &mut inputs,
        0,
        ledger(&[Kind::Hold {
            path: path.clone(),
            reason: HoldReason::Unlisted,
        }]),
    );
    let derived = derive_ok(&inputs);
    assert!(derived.added.is_empty(), "{:?}", derived.added);
    assert_problem(&derived, &[&path, "stays held", "#194"]);
    let rebuilt = copy_ledger::parse(Path::new("rebuilt"), &derived.ledger).unwrap();
    assert_eq!(
        CopyState::fold(&rebuilt).unwrap().hold_reason(&path),
        Some(HoldReason::Unlisted)
    );
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
