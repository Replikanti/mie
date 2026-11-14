//! The replace's pure parts (RQ2 to RQ5) as tables, and the run under its
//! hook on the replication fixture: no selection shortens between steps,
//! and the resume completes a half-moved part and never overwrites.

use super::super::tests::{ARCHIVE, F, file, inputs, listing, record};
use super::*;
use crate::archive::ArchiveTransports;
use crate::store::archive_checks::tests::part;
use crate::store::replicate::tests::{D0, Fixture, seal};
use mie_adapter_binance::transport::{Clock, HttpDownload, HttpGet};
use mie_adapter_parquet::ParquetRawStore;
use mie_adapter_parquet::copy_ledger::HoldReason;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{DatasetVersion, RawRecordSource, RawSelection, RawStreamKey};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

const ID: &str = "q-1791259200000";

/// A copy target or hot whose layout holds the data file.
fn holder(root: &str) -> Holder {
    Holder {
        root: root.to_owned(),
        read_only: false,
        data: true,
        text: None,
    }
}

/// Every path in `held` held by hot.
fn on_hot(paths: &[&str]) -> BTreeMap<String, Vec<Holder>> {
    paths
        .iter()
        .map(|path| ((*path).to_owned(), vec![holder("hot")]))
        .collect()
}

#[track_caller]
fn refused(inputs: &PlanInputs, holders: &BTreeMap<String, Vec<Holder>>, needles: &[&str]) {
    match plan_replace(inputs, holders, ID) {
        Ok(plan) => panic!("planned {plan:?}"),
        Err(why) => assert!(needles.iter().all(|n| why.contains(n)), "{why}"),
    }
}

/// RQ2 to RQ4 as a table: no entry, the generation arithmetic, siblings
/// and two ledger texts (read-only holders:
/// [`rq2_refuses_only_a_path_read_only_roots_alone_hold`]).
#[test]
fn replace_plan_table() {
    let (a, b, c) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-01", 1),
        part("aggTrades", "2026-10-01", 2),
    );
    let e0 = format!("{F}.import");
    let (s1, e1) = (format!("{F}.1.supersede.import"), format!("{F}.1.import"));

    // F has no entry: S1 with no `entry` line, M = parts(F).
    let plan = plan_replace(&inputs(Vec::new(), &[&a]), &on_hot(&[&a]), ID).unwrap();
    assert_eq!(plan.generation, 1);
    assert!(plan.record.entries.is_empty());
    assert_eq!(plan.moved(), std::slice::from_ref(&a));
    assert_eq!(plan.record.quarantine.as_ref().unwrap().id, ID);

    // E0 → S1, naming E0; its sibling b moves too.
    let entry = listing("zip", &[&a, &b]);
    let files = vec![file("hot", &e0, &entry), file("cold-a", &e0, &entry)];
    let plan = plan_replace(&inputs(files.clone(), &[&a]), &on_hot(&[&a, &b]), ID).unwrap();
    assert_eq!(plan.generation, 1);
    assert_eq!(plan.record.entries.len(), 1);
    assert_eq!(plan.moved(), &[a.clone(), b.clone()]);
    assert_eq!(plan.listed[&a].len(), 1);

    // E0, S1 (`quarantine -`), E1 → S2, naming E1.
    let mut later = files.clone();
    later.push(file("hot", &s1, &record(None)));
    later.push(file("hot", &e1, &entry));
    let plan = plan_replace(&inputs(later, &[&a]), &on_hot(&[&a, &b]), ID).unwrap();
    assert_eq!(plan.generation, 2);
    assert!(plan.record.entries[0].name.ends_with(&e1));

    // The highest generation at u32::MAX is refused, never wrapped.
    let max = format!("{F}.{}.import", u32::MAX);
    let mut top = files.clone();
    top.push(file("hot", &max, &entry));
    refused(
        &inputs(top, &[&a]),
        &on_hot(&[&a, &b]),
        &["exceed", "never wraps"],
    );

    // A listed path no root holds and the ledger does not record: named,
    // not moved.
    let plan = plan_replace(&inputs(files.clone(), &[&a]), &on_hot(&[&a]), ID).unwrap();
    assert_eq!(plan.moved(), std::slice::from_ref(&a));
    assert!(
        plan.lines
            .iter()
            .any(|l| l.contains(&b) && l.contains("not moved"))
    );
    // One the ledger records that no root holds: refused.
    let mut recorded = inputs(files.clone(), &[&a]);
    recorded.verdicts.insert(b.clone(), PathVerdict::Ordinary);
    refused(&recorded, &on_hot(&[&a]), &[&b, "no root holds it"]);
    // A sibling lost, pending, replaced or held: refused.
    for verdict in [
        PathVerdict::Lost,
        PathVerdict::Pending,
        PathVerdict::Replaced,
        PathVerdict::Held,
        PathVerdict::NoManifest,
    ] {
        let mut sibling = inputs(files.clone(), &[&a]);
        sibling.verdicts.insert(b.clone(), verdict);
        refused(&sibling, &on_hot(&[&a, &b]), &[&b, "not ordinary"]);
    }
    // Two manifest texts in the copy ledger for a moved path: refused.
    let mut two = inputs(files.clone(), &[&a]);
    two.verdicts.insert(a.clone(), PathVerdict::Conflict);
    refused(&two, &on_hot(&[&a, &b]), &[&a, "two manifest texts"]);
    // A part no root holds: refused.
    refused(
        &inputs(files, &[&a, &c]),
        &on_hot(&[&a, &b]),
        &[&c, "no root holds it"],
    );
    let _ = ARCHIVE;
}

/// RQ2 as amended (#216): with h the holders of a moved path that are hot
/// or a copy target and r its read-only holders, each counted only when
/// its layout holds the data file (review of #221, finding 2), (h, r) =
/// (0, 1) and (0, 2) refuse the whole replace, naming the path and every
/// read-only data holder in holder order; (1, 1), (2, 1) and (1, 0) plan;
/// (0, 0) is a part no root holds, as before, or, with manifests alone,
/// the rule before the review.
#[test]
fn rq2_refuses_only_a_path_read_only_roots_alone_hold() {
    let (a, b) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-01", 1),
    );
    let e0 = format!("{F}.import");
    let entry = listing("zip", &[&a, &b]);
    let files = vec![file("hot", &e0, &entry), file("cold-a", &e0, &entry)];
    let read_only = |root: &str| Holder {
        read_only: true,
        ..holder(root)
    };
    let with = |held_a: Vec<Holder>, held_b: Vec<Holder>| {
        BTreeMap::from([(a.clone(), held_a), (b.clone(), held_b)])
    };
    let planned = |holders: &BTreeMap<String, Vec<Holder>>| {
        let plan = plan_replace(&inputs(files.clone(), &[&a]), holders, ID)
            .unwrap_or_else(|why| panic!("{why}"));
        assert_eq!(plan.moved(), &[a.clone(), b.clone()]);
    };

    // (1, 0), (1, 1) with hot, (1, 1) with a copy target, (2, 1).
    planned(&with(vec![holder("hot")], vec![holder("hot")]));
    planned(&with(
        vec![holder("hot")],
        vec![holder("hot"), read_only("cold-r")],
    ));
    planned(&with(
        vec![holder("hot")],
        vec![holder("cold-a"), read_only("cold-r")],
    ));
    planned(&with(
        vec![holder("hot")],
        vec![holder("hot"), holder("cold-a"), read_only("cold-r")],
    ));
    // One directory under two spellings, once a copy target, once
    // read-only: h = 1.
    planned(&with(
        vec![holder("hot")],
        vec![holder("cold-c"), read_only("cold-c")],
    ));
    // Hot and a read-only root hold both paths.
    planned(&with(
        vec![holder("hot"), read_only("cold-r")],
        vec![holder("hot"), read_only("cold-r")],
    ));

    // (0, 1): the sibling b, the last path of M, while hot holds a.
    refused(
        &inputs(files.clone(), &[&a]),
        &with(vec![holder("hot")], vec![read_only("cold-r")]),
        &[
            &b,
            "only",
            "read-only root(s) cold-r;",
            "copy_target = true",
        ],
    );
    // (0, 1): the part a.
    refused(
        &inputs(files.clone(), &[&a]),
        &with(vec![read_only("cold-r")], vec![holder("hot")]),
        &[&a, "only", "read-only root(s) cold-r;", "mie store repair"],
    );
    // (0, 2): both named in holder order.
    refused(
        &inputs(files.clone(), &[&a]),
        &with(
            vec![holder("hot")],
            vec![read_only("cold-r"), read_only("cold-s")],
        ),
        &[&b, "read-only root(s) cold-r, cold-s;"],
    );
    // (0, 0): a part no root holds.
    refused(
        &inputs(files.clone(), &[&a]),
        &with(Vec::new(), vec![holder("hot")]),
        &[&a, "no root holds it"],
    );

    // Review of #221, finding 2: only a data file counts. A copy target
    // or hot holding the manifest alone is no writable holder: (0, 1).
    let manifest = |holder: Holder| Holder {
        data: false,
        ..holder
    };
    for writable in ["hot", "cold-a"] {
        refused(
            &inputs(files.clone(), &[&a]),
            &with(
                vec![holder("hot")],
                vec![manifest(holder(writable)), read_only("cold-r")],
            ),
            &[&b, "read-only root(s) cold-r;", "data file"],
        );
    }
    // A read-only root holding the manifest alone is no r: the data on a
    // copy target plans, and among two read-only roots only the data
    // holder is named.
    planned(&with(
        vec![holder("hot")],
        vec![holder("cold-a"), manifest(read_only("cold-r"))],
    ));
    refused(
        &inputs(files.clone(), &[&a]),
        &with(
            vec![holder("hot")],
            vec![manifest(read_only("cold-r")), read_only("cold-s")],
        ),
        &[&b, "read-only root(s) cold-s;"],
    );
    // No holder has the data file (h = 0, r = 0): the rule before the
    // review stands. Manifests on read-only roots alone are refused, a
    // manifest on hot or a copy target plans.
    refused(
        &inputs(files.clone(), &[&a]),
        &with(vec![holder("hot")], vec![manifest(read_only("cold-r"))]),
        &[&b, "read-only root(s) cold-r;"],
    );
    planned(&with(
        vec![holder("hot")],
        vec![manifest(holder("cold-a")), manifest(read_only("cold-r"))],
    ));
}

/// A part of `rows` events in `[min, max]` ms.
fn moved(path: &str, min: i64, max: i64) -> MovedFile {
    let mut manifest = super::super::tests::manifest(path);
    manifest.min_event_time = EventTime::from_millis(min);
    manifest.max_event_time = EventTime::from_millis(max);
    MovedFile {
        path: path.to_owned(),
        manifests: vec![manifest],
    }
}

/// A warm run's handoff over the prefix `[origin, cutover)`.
fn warm(origin: i64, cutover: i64) -> WarmHandoff {
    WarmHandoff {
        origin_ms: origin,
        effective_origin_ms: None,
        cutover_ms: cutover,
        prefix_dataset: DatasetVersion::from_hex(&"ab".repeat(32)).unwrap(),
        prefix_last_event_ms: None,
        prefix_stream: mie_domain::event_hash::EventStreamHash {
            events: 0,
            fingerprint: mie_domain::fingerprint::Fingerprint::from_raw(0),
        },
        prefix_rejections: 0,
        handoff_state: mie_domain::state_hash::StateHash::from_fingerprint(
            mie_domain::fingerprint::Fingerprint::from_raw(0),
        ),
        seeds: BTreeMap::new(),
        queue_high_water: 0,
        queue_high_water_bytes: 0,
        warming_up: Vec::new(),
    }
}

/// RQ5: a prefix `[origin, cutover)` covers a part with events `[min,
/// max]` iff min < cutover and max ≥ origin; a run covering two parts is
/// named once with both.
#[test]
fn cost_overlap_bounds() {
    let a = part("aggTrades", "2026-10-01", 0);
    let b = part("aggTrades", "2026-10-01", 1);
    let named = |prefix: (i64, i64)| {
        let lines = cost(
            &[moved(&a, 1_000, 2_000)],
            &Journal::Runs(vec![("r".to_owned(), warm(prefix.0, prefix.1))]),
        );
        lines.iter().any(|l| l.starts_with("cost: warm run r"))
    };
    assert!(named((2_000, 3_000)), "max = origin, inclusive");
    assert!(!named((0, 1_000)), "min = cutover, exclusive");
    assert!(named((0, 1_001)));
    assert!(!named((2_001, 3_000)));
    let lines = cost(
        &[moved(&a, 1_000, 2_000), moved(&b, 3_000, 4_000)],
        &Journal::Runs(vec![("r".to_owned(), warm(0, 10_000))]),
    );
    let runs: Vec<&String> = lines
        .iter()
        .filter(|l| l.starts_with("cost: warm run "))
        .collect();
    assert_eq!(runs.len(), 1, "{lines:?}");
    assert!(runs[0].contains(&a) && runs[0].contains(&b) && runs[0].contains(&"ab".repeat(32)));
    assert_eq!(
        lines.last().unwrap(),
        "cost: 2 moved path(s), 1 journaled warm run(s) whose prefix_dataset changes"
    );
    let lines = cost(&[moved(&a, 1_000, 2_000)], &Journal::Missing);
    assert!(lines.contains(&"cost: no journal: no warm run recorded".to_owned()));
}

/// RQ5 (review of #215, finding 2): a moved path without a manifest
/// covers its whole stream-day, so every warm run whose prefix overlaps
/// that day is named; a run that ends at the day's start is not.
#[test]
fn a_manifest_less_moved_path_names_the_runs_over_its_day() {
    let a = part("aggTrades", "2026-10-01", 0);
    let day = mie_adapter_binance::archive::catalog::parse_day("2026-10-01").unwrap() * 86_400_000;
    let bare = MovedFile {
        path: a.clone(),
        manifests: Vec::new(),
    };
    let named = |prefix: (i64, i64)| {
        cost(
            std::slice::from_ref(&bare),
            &Journal::Runs(vec![("r".to_owned(), warm(prefix.0, prefix.1))]),
        )
        .iter()
        .any(|l| l.starts_with("cost: warm run r") && l.contains(&a))
    };
    assert!(
        named((day + 23 * 3_600_000, day + 86_400_000 * 2)),
        "late in the day"
    );
    assert!(named((day - 86_400_000, day + 1)), "its first millisecond");
    assert!(
        named((day + 86_400_000 - 1, day + 86_400_000 * 2)),
        "its last millisecond"
    );
    assert!(!named((day - 86_400_000, day)), "ends at its start");
    assert!(
        !named((day + 86_400_000, day + 86_400_000 * 2)),
        "starts the next day"
    );
}

/// RQ5: only `warm_start` lines with `warm: true` are read; a cold start
/// and a non-JSON line name no run, a missing journal is `None`, an
/// unreadable one is an error.
#[test]
fn cost_reads_only_warm_starts() {
    let fixture = Fixture::new("replace-journal");
    let path = fixture.dir.0.join("journal-cost.jsonl");
    assert_eq!(crate::journal::warm_handoffs(&path).unwrap(), None);
    let mut line = crate::journal::warm_start_json(&crate::journal::WarmStart::Warm(warm(1, 2)));
    line["type"] = "warm_start".into();
    line["run_id"] = "w".into();
    let cold = serde_json::json!({
        "type": "warm_start", "run_id": "c", "warm": false, "cold": "no-history", "error": null
    });
    fs::write(
        &path,
        format!("{line}\nnot json\n{cold}\n{{\"type\":\"run_start\"}}\n"),
    )
    .unwrap();
    let runs = crate::journal::warm_handoffs(&path).unwrap().unwrap();
    assert_eq!(runs, vec![("w".to_owned(), warm(1, 2))]);
    let dir = fixture.dir.0.join("a-directory");
    fs::create_dir(&dir).unwrap();
    assert!(crate::journal::warm_handoffs(&dir).is_err());
}

// ------------------------------------------------------------- the run

/// Transports that fail every request: the replace makes none.
struct NoNetwork;

impl HttpGet for NoNetwork {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        panic!("the replace requested {url}")
    }
}

impl HttpDownload for NoNetwork {
    fn download(&self, url: &str, _: &mut dyn std::io::Write) -> Result<u16, String> {
        panic!("the replace downloaded {url}")
    }
}

impl Clock for NoNetwork {
    fn now_utc_ns(&self) -> i64 {
        0
    }
    fn monotonic_ns(&self) -> u64 {
        0
    }
    fn sleep(&self, _: std::time::Duration) {}
}

/// The run's `at_ms`.
const AT: i64 = D0 + 4 * 3_600_000;

fn resolve(fixture: &Fixture, replace: bool, at: i64) -> ResolveReport {
    let transports = ArchiveTransports {
        http: Arc::new(NoNetwork),
        download: Arc::new(NoNetwork),
        clock: Arc::new(NoNetwork),
    };
    super::super::run(
        &fixture.config,
        Some(&fixture.archive()),
        Some("data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-05.zip"),
        replace,
        &transports,
        &fixture.probe,
        &|| at,
    )
}

/// The replication fixture after its pass, plus a third archive part of
/// the same day that only hot holds, listed by hot's E0; the first part
/// held. Returns the three parts.
fn replace_fixture(tag: &str) -> (Fixture, Vec<String>) {
    let fixture = Fixture::new(tag);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let mut parts: Vec<String> = replica::list_sealed(&fixture.hot)
        .unwrap()
        .into_iter()
        .filter(|p| p.contains("binance-archive"))
        .collect();
    parts.extend(seal(
        &fixture.hot,
        "binance-archive",
        "aggTrades",
        D0 - 86_400_000 + 10_000,
        1,
    ));
    assert_eq!(parts.len(), 3, "{parts:?}");
    let entry = fixture
        .dir
        .0
        .join("ledger/aggTrades/BTCUSDT-aggTrades-2026-10-05.zip.import");
    fs::remove_file(&entry).unwrap();
    fixture.write_entry(&parts);
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: D0 + 2,
            kind: Kind::Hold {
                path: parts[0].clone(),
                reason: HoldReason::Unlisted,
            },
        })
        .unwrap();
    (fixture, parts)
}

/// One selection of the archive stream over `[start, end)`.
fn window(start: i64, end: i64) -> RawSelection {
    RawSelection::new(
        BTreeSet::from([RawStreamKey::new("binance-archive", "BTCUSDT", "aggTrades").unwrap()]),
        ReplayWindow {
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(end),
        },
    )
    .unwrap()
}

/// RQ6: a stop after any root's renames never answers a selection
/// shorter: after every root (cold-a, cold-b, hot), each selection of one
/// part's events, of the unreplicated sibling only hot held among them,
/// and of the whole day is an error or the answer before the replace.
#[test]
fn records_precede_renames_so_no_selection_shortens() {
    let (fixture, parts) = replace_fixture("replace-hook");
    let store = ParquetRawStore::tiered(&fixture.hot, fixture.cold.to_vec());
    let windows: Vec<RawSelection> = parts
        .iter()
        .map(|path| {
            let (_, file) = replica::sealed_manifest(&fixture.hot, path)
                .unwrap()
                .unwrap();
            window(
                file.min_event_time.as_millis(),
                file.max_event_time.as_millis() + 1,
            )
        })
        .chain(std::iter::once(window(D0 - 86_400_000, D0)))
        .collect();
    let baseline: Vec<_> = windows.iter().map(|w| store.select(w)).collect();
    assert!(baseline[2].as_ref().is_ok_and(|d| d.files.len() == 1));
    let seen: Rc<RefCell<Vec<String>>> = Rc::default();
    let failures: Rc<RefCell<Vec<String>>> = Rc::default();
    {
        let (seen, failures) = (seen.clone(), failures.clone());
        let (windows, baseline, store) = (windows.clone(), baseline.clone(), store.clone());
        AFTER_ROOT.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |root: &str| {
                seen.borrow_mut().push(root.to_owned());
                for (w, before) in windows.iter().zip(&baseline) {
                    let now = store.select(w);
                    if now.is_ok() && now != *before {
                        failures.borrow_mut().push(format!(
                            "after {root}: {w:?} answered {now:?}, was {before:?}"
                        ));
                    }
                }
            }));
        });
    }
    let report = resolve(&fixture, true, AT);
    AFTER_ROOT.with(|hook| *hook.borrow_mut() = None);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    assert_eq!(*seen.borrow(), vec!["cold-a", "cold-b", "hot"]);
    assert_eq!(*failures.borrow(), Vec::<String>::new());
    for w in &windows {
        assert!(store.select(w).is_err());
    }
}

/// RQ7: hot's data moved and its manifest not (a stop between the two
/// renames): the resume moves the manifest and the replace is complete.
#[test]
fn resume_completes_a_half_moved_part() {
    let (fixture, parts) = replace_fixture("replace-half-moved");
    let report = resolve(&fixture, true, AT);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    let id = format!("q-{AT}");
    let manifest = format!("{}.manifest", parts[0].strip_suffix(".parquet").unwrap());
    let dir = replica::quarantine_dir(&fixture.hot, &id);
    fs::rename(dir.join(&manifest), fixture.hot.join(&manifest)).unwrap();
    let report = resolve(&fixture, false, AT + 1);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    assert!(
        report.lines.iter().any(|l| l.starts_with("resume: ")),
        "{report:#?}"
    );
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.starts_with(&format!("moved {manifest} on hot"))),
        "{report:#?}"
    );
    assert!(!fixture.hot.join(&manifest).exists());
    assert!(dir.join(&manifest).exists());
    let report = resolve(&fixture, false, AT + 2);
    assert!(
        report.lines.iter().any(|l| l.contains("is complete")),
        "{report:#?}"
    );
}

/// RQ7: a name in both the layout and the quarantine is `Integrity`; the
/// resume stops and nothing is overwritten.
#[test]
fn resume_never_overwrites_a_quarantined_copy() {
    let (fixture, parts) = replace_fixture("replace-no-overwrite");
    let report = resolve(&fixture, true, AT);
    assert_eq!(report.problems, Vec::<String>::new(), "{report:#?}");
    let id = format!("q-{AT}");
    let dir = replica::quarantine_dir(&fixture.hot, &id);
    let quarantined = fs::read(dir.join(&parts[1])).unwrap();
    fs::write(fixture.hot.join(&parts[1]), b"another copy").unwrap();
    let report = resolve(&fixture, false, AT + 1);
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.contains(&parts[1]) && p.contains("refusing to replace")),
        "{report:#?}"
    );
    assert_eq!(fs::read(dir.join(&parts[1])).unwrap(), quarantined);
    assert_eq!(
        fs::read(fixture.hot.join(&parts[1])).unwrap(),
        b"another copy"
    );
}
