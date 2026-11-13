//! The archive checks as a table (#193 B7): the `PENDING` window, entry
//! resolution and the hold reasons' precedence.

use super::*;
use mie_adapter_binance::archive::catalog::{Period, parse_day};
use mie_adapter_parquet::replica;

const AGG: &str = "binance-archive/BTCUSDT/aggTrades";

pub(crate) fn part(stream: &str, date: &str, n: u32) -> String {
    format!(
        "source=binance-archive/instrument=BTCUSDT/stream={stream}/date={date}/part-{n:05}.parquet"
    )
}

pub(crate) fn sha(tag: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(tag.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A valid manifest text of `path` whose data has the SHA-256 `sha`.
pub(crate) fn text(path: &str, sha: &str) -> String {
    let segments: Vec<&str> = path.split('/').collect();
    let value = |i: usize, prefix: &str| segments[i].strip_prefix(prefix).unwrap().to_owned();
    let date = value(3, "date=");
    let part: u32 = segments[4]
        .strip_prefix("part-")
        .and_then(|p| p.strip_suffix(".parquet"))
        .unwrap()
        .parse()
        .unwrap();
    let start = parse_day(&date).unwrap() * 86_400_000;
    let text = format!(
        "mie-raw-manifest 1\npath {path}\nsource {}\ninstrument {}\nstream {}\ndate {date}\n\
         part {part}\nrows 1\nbytes 10\nmin_event_time_ms {start}\nmax_event_time_ms {start}\n\
         sha256 {sha}\n",
        value(0, "source="),
        value(1, "instrument="),
        value(2, "stream="),
    );
    replica::parse_manifest(&text).unwrap();
    text
}

/// An import entry listing `files` as `(path, sha256)`.
pub(crate) fn entry(files: &[(&str, &str)]) -> Vec<u8> {
    let mut text = format!(
        "mie-archive-import 1\n\
         archive data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-01.zip\n\
         sha256 {}\nstream {AGG}\nperiod 2026-10-01\nheader -\nrows {}\n",
        sha("zip"),
        files.len()
    );
    let mut sorted = files.to_vec();
    sorted.sort();
    for (path, sha) in sorted {
        text.push_str(&format!("file {path} 1 {sha}\n"));
    }
    text.into_bytes()
}

pub(crate) const NAME: &str = "aggTrades/BTCUSDT-aggTrades-2026-10-01.zip.import";

/// Inputs where each of `paths` is on hot and cold-a with the text of
/// SHA-256 `sha(path)`.
fn on_roots(paths: &[String]) -> BTreeMap<String, PartTexts> {
    paths
        .iter()
        .map(|path| {
            let text = text(path, &sha(path));
            (
                path.clone(),
                PartTexts {
                    roots: vec![
                        ("hot".to_owned(), text.clone()),
                        ("cold-a".to_owned(), text),
                    ],
                    start: Vec::new(),
                },
            )
        })
        .collect()
}

fn copies(list: &[(&str, &[u8])]) -> Vec<EntryCopy> {
    list.iter()
        .map(|(root, bytes)| EntryCopy {
            root: (*root).to_owned(),
            bytes: bytes.to_vec(),
        })
        .collect()
}

fn pending(stream: &str, period: Period) -> Pending {
    let key: Vec<&str> = stream.split('/').collect();
    Pending {
        archive: "data/futures/um/x.zip".to_owned(),
        sha256: sha("pending"),
        stream: mie_ports::raw::RawStreamKey::new(key[0], key[1], key[2]).unwrap(),
        period,
    }
}

fn held(findings: &ArchiveFindings) -> Vec<(String, HoldReason)> {
    findings
        .holds
        .iter()
        .map(|(path, reason)| (path.clone(), *reason))
        .collect()
}

/// A daily `PENDING` exempts `[day − 1, day + 1]` of its stream only; a
/// monthly one `[first − 1, last + 1]`; both ends inclusive, as the
/// importer's orphan window.
#[test]
fn pending_window_bounds() {
    let dates = [
        "2026-09-29",
        "2026-09-30",
        "2026-10-01",
        "2026-10-02",
        "2026-10-03",
    ];
    let mut paths: Vec<String> = dates.iter().map(|d| part("aggTrades", d, 0)).collect();
    paths.push(part("metrics", "2026-10-01", 0));
    let day = parse_day("2026-10-01").unwrap();
    let findings = check(&ArchiveInputs {
        entries: BTreeMap::new(),
        parts: on_roots(&paths),
        pending: Some(Ok(pending(AGG, Period::Day(day)))),
    });
    assert_eq!(
        held(&findings),
        vec![
            (part("aggTrades", "2026-09-29", 0), HoldReason::Unlisted),
            (part("aggTrades", "2026-10-03", 0), HoldReason::Unlisted),
            (part("metrics", "2026-10-01", 0), HoldReason::Unlisted),
        ]
    );
    assert_eq!(
        findings.exempt,
        dates[1..4]
            .iter()
            .map(|d| part("aggTrades", d, 0))
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(findings.problems, Vec::<String>::new());

    let dates = [
        "2026-08-30",
        "2026-08-31",
        "2026-09-01",
        "2026-09-30",
        "2026-10-01",
        "2026-10-02",
    ];
    let paths: Vec<String> = dates.iter().map(|d| part("fundingRate", d, 0)).collect();
    let findings = check(&ArchiveInputs {
        entries: BTreeMap::new(),
        parts: on_roots(&paths),
        pending: Some(Ok(pending(
            "binance-archive/BTCUSDT/fundingRate",
            Period::Month {
                year: 2026,
                month: 9,
            },
        ))),
    });
    assert_eq!(
        held(&findings),
        vec![
            (part("fundingRate", "2026-08-30", 0), HoldReason::Unlisted),
            (part("fundingRate", "2026-10-02", 0), HoldReason::Unlisted),
        ]
    );
    assert_eq!(findings.exempt.len(), 4);
}

/// A `PENDING` that cannot be read or parsed exempts nothing and is named.
#[test]
fn a_malformed_pending_exempts_nothing() {
    let paths = vec![part("aggTrades", "2026-10-01", 0)];
    let findings = check(&ArchiveInputs {
        entries: BTreeMap::new(),
        parts: on_roots(&paths),
        pending: Some(Err(
            "PENDING: first line is not \"mie-archive-pending 1\"".to_owned()
        )),
    });
    assert_eq!(
        held(&findings),
        vec![(paths[0].clone(), HoldReason::Unlisted)]
    );
    assert!(findings.exempt.is_empty());
    assert!(
        findings
            .problems
            .iter()
            .any(|p| p.contains("PENDING") && p.contains("exempts nothing")),
        "{:?}",
        findings.problems
    );
    // No `PENDING` at all: held, and no problem about it.
    let findings = check(&ArchiveInputs {
        entries: BTreeMap::new(),
        parts: on_roots(&paths),
        pending: None,
    });
    assert_eq!(held(&findings).len(), 1);
    assert_eq!(findings.problems, Vec::<String>::new());
}

/// Entry copies: identical → current; one variant matching the manifests →
/// current, the others named; none matching, or two → `entry-conflict` on
/// every listed part, absent ones reported and never held; every variant
/// unparseable → named, its parts unlisted; on targets only → current and
/// named.
#[test]
fn entry_resolution_table() {
    let (a, b) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-01", 1),
    );
    let absent = part("aggTrades", "2026-10-01", 2);
    let parts = on_roots(&[a.clone(), b.clone()]);
    let good = entry(&[(&a, &sha(&a)), (&b, &sha(&b))]);
    let wrong = entry(&[(&a, &sha("other")), (&b, &sha(&b))]);
    let wrong_too = entry(&[(&a, &sha(&a)), (&b, &sha("x")), (&absent, &sha("y"))]);
    let subset = entry(&[(&a, &sha(&a))]);
    let run = |list: &[(&str, &[u8])]| {
        check(&ArchiveInputs {
            entries: BTreeMap::from([(NAME.to_owned(), copies(list))]),
            parts: parts.clone(),
            pending: None,
        })
    };

    // Identical copies.
    let found = run(&[("hot", &good), ("cold-a", &good), ("cold-b", &good)]);
    assert_eq!(held(&found), vec![]);
    assert_eq!(found.problems, Vec::<String>::new());
    assert_eq!(found.current[NAME].len(), 2);

    // One matching variant: current, the other copy named.
    for list in [
        [("hot", &good), ("cold-a", &wrong), ("cold-b", &good)],
        [("hot", &wrong), ("cold-a", &good), ("cold-b", &good)],
    ] {
        let list: Vec<(&str, &[u8])> = list.iter().map(|(r, b)| (*r, b.as_slice())).collect();
        let found = run(&list);
        assert_eq!(held(&found), vec![], "{list:?}");
        assert_eq!(found.current[NAME].len(), 2);
        let named = list
            .iter()
            .find(|(_, bytes)| *bytes == wrong.as_slice())
            .unwrap()
            .0;
        assert!(
            found
                .problems
                .iter()
                .any(|p| p.contains(NAME) && p.contains(named) && p.contains("differs")),
            "{:?}",
            found.problems
        );
    }

    // No variant matches: every listed part present is held, the absent
    // one reported.
    let found = run(&[("hot", &wrong), ("cold-a", &wrong_too)]);
    assert_eq!(
        held(&found),
        vec![
            (a.clone(), HoldReason::EntryConflict),
            (b.clone(), HoldReason::EntryConflict),
        ]
    );
    assert!(found.current.is_empty());
    assert!(
        found
            .problems
            .iter()
            .any(|p| p.contains(&absent) && p.contains("no root holds it")),
        "{:?}",
        found.problems
    );

    // Two variants that both match: still a conflict.
    let found = run(&[("hot", &good), ("cold-a", &subset)]);
    assert_eq!(held(&found).len(), 2);
    assert!(
        held(&found)
            .iter()
            .all(|(_, r)| *r == HoldReason::EntryConflict)
    );

    // Every variant unparseable: named, and its parts are unlisted.
    let found = run(&[("hot", b"garbage\n"), ("cold-a", b"other garbage\n")]);
    assert_eq!(
        held(&found),
        vec![
            (a.clone(), HoldReason::Unlisted),
            (b.clone(), HoldReason::Unlisted),
        ]
    );
    assert!(
        found.problems.iter().any(|p| p.contains(NAME)),
        "{:?}",
        found.problems
    );

    // On targets only: current, and hot's absence named.
    let found = run(&[("cold-a", &good), ("cold-b", &good)]);
    assert_eq!(held(&found), vec![]);
    assert_eq!(found.current[NAME].len(), 2);
    assert!(
        found
            .problems
            .iter()
            .any(|p| p.contains(NAME) && p.contains("hot")),
        "{:?}",
        found.problems
    );
}

/// One reason per path: `manifest-conflict` > `entry-conflict` >
/// `unlisted`; the start's texts count for the conflict.
#[test]
fn hold_reason_precedence() {
    let (a, b, c) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-01", 1),
        part("aggTrades", "2026-10-01", 2),
    );
    let mut parts = on_roots(&[a.clone(), b.clone(), c.clone()]);
    // a: two texts on two roots; b: a second text in the start only.
    parts.get_mut(&a).unwrap().roots[1].1 = text(&a, &sha("second"));
    parts.get_mut(&b).unwrap().start = vec![text(&b, &sha("start"))];
    // A conflicting entry lists a and b, nothing lists c.
    let one = entry(&[(&a, &sha("x")), (&b, &sha("y"))]);
    let two = entry(&[(&a, &sha("z")), (&b, &sha("w"))]);
    let findings = check(&ArchiveInputs {
        entries: BTreeMap::from([(NAME.to_owned(), copies(&[("hot", &one), ("cold-a", &two)]))]),
        parts,
        pending: None,
    });
    assert_eq!(
        held(&findings),
        vec![
            (a, HoldReason::ManifestConflict),
            (b, HoldReason::ManifestConflict),
            (c.clone(), HoldReason::Unlisted),
        ]
    );

    let mut parts = on_roots(std::slice::from_ref(&c));
    parts.get_mut(&c).unwrap().start = vec![];
    let findings = check(&ArchiveInputs {
        entries: BTreeMap::from([(
            NAME.to_owned(),
            copies(&[
                ("hot", &entry(&[(&c, &sha("x"))])),
                ("cold-a", &entry(&[(&c, &sha("z"))])),
            ]),
        )]),
        parts,
        pending: None,
    });
    assert_eq!(held(&findings), vec![(c, HoldReason::EntryConflict)]);
}

pub(crate) const E1: &str = "aggTrades/BTCUSDT-aggTrades-2026-10-01.zip.1.import";
pub(crate) const S1: &str = "aggTrades/BTCUSDT-aggTrades-2026-10-01.zip.1.supersede.import";

/// A supersede record of the fixture's archive file, with no `entry` line.
pub(crate) fn supersede() -> Vec<u8> {
    format!(
        "mie-archive-supersede 1\n\
         archive data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-01.zip\n\
         stream {AGG}\nperiod 2026-10-01\nquarantine -\n"
    )
    .into_bytes()
}

/// The copies of one name, as `(root, bytes)`.
type Copies<'a> = &'a [(&'a str, &'a [u8])];

/// #210 C7: only the current generation's entry goes through entry
/// resolution; supersede records count by generation and are named when
/// they are on the targets only or their copies differ.
#[test]
fn supersede_records_join_entry_resolution() {
    let (a, b) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-01", 1),
    );
    let parts = on_roots(&[a.clone(), b.clone()]);
    let good = entry(&[(&a, &sha(&a)), (&b, &sha(&b))]);
    let wrong = entry(&[(&a, &sha("other")), (&b, &sha(&b))]);
    let wrong_too = entry(&[(&a, &sha("x")), (&b, &sha("y"))]);
    let record = supersede();
    let run = |entries: &[(&str, Copies<'_>)]| {
        check(&ArchiveInputs {
            entries: entries
                .iter()
                .map(|(name, list)| ((*name).to_owned(), copies(list)))
                .collect(),
            parts: parts.clone(),
            pending: None,
        })
    };

    // E0 variants that conflict, superseded by S1 and an identical E1:
    // E1 is current, nothing is held, and E0 is no conflict.
    let found = run(&[
        (NAME, &[("hot", &wrong), ("cold-a", &wrong_too)]),
        (S1, &[("hot", &record), ("cold-a", &record)]),
        (E1, &[("hot", &good), ("cold-a", &good)]),
    ]);
    assert_eq!(held(&found), vec![]);
    assert_eq!(found.current.keys().collect::<Vec<_>>(), vec![E1]);
    assert_eq!(found.problems, Vec::<String>::new());

    // E0 and S1 without E1: no current entry, the parts are unlisted.
    let found = run(&[
        (NAME, &[("hot", &good), ("cold-a", &good)]),
        (S1, &[("hot", &record)]),
    ]);
    assert!(found.current.is_empty());
    assert_eq!(
        held(&found),
        vec![
            (a.clone(), HoldReason::Unlisted),
            (b.clone(), HoldReason::Unlisted),
        ]
    );

    // E1 variants that conflict: entry-conflict on E1's parts.
    let found = run(&[
        (NAME, &[("hot", &good)]),
        (S1, &[("hot", &record)]),
        (E1, &[("hot", &wrong), ("cold-a", &wrong_too)]),
    ]);
    assert_eq!(
        held(&found),
        vec![
            (a.clone(), HoldReason::EntryConflict),
            (b.clone(), HoldReason::EntryConflict),
        ]
    );
    assert!(
        found.problems.iter().any(|p| p.contains(E1)),
        "{:?}",
        found.problems
    );

    // A target-only S1 counts and is named.
    let found = run(&[(NAME, &[("hot", &good)]), (S1, &[("cold-a", &record)])]);
    assert!(found.current.is_empty());
    assert_eq!(held(&found).len(), 2);
    assert!(
        found
            .problems
            .iter()
            .any(|p| p.contains(S1) && p.contains("cold-a only")),
        "{:?}",
        found.problems
    );

    // Differing record copies are named; the generation still counts.
    let found = run(&[
        (NAME, &[("hot", &good)]),
        (S1, &[("hot", &record), ("cold-a", b"damaged\n")]),
        (E1, &[("hot", &good)]),
    ]);
    assert_eq!(found.current.keys().collect::<Vec<_>>(), vec![E1]);
    assert!(
        found
            .problems
            .iter()
            .any(|p| p.contains(S1) && p.contains("differ")),
        "{:?}",
        found.problems
    );

    // A malformed name is named and counts for nothing.
    let bad = "aggTrades/BTCUSDT-aggTrades-2026-10-01.zip.01.import";
    let found = run(&[(NAME, &[("hot", &good)]), (bad, &[("hot", &good)])]);
    assert_eq!(found.current.keys().collect::<Vec<_>>(), vec![NAME]);
    assert!(
        found.problems.iter().any(|p| p.contains(bad)),
        "{:?}",
        found.problems
    );
}

// ----------------------------------------------------------- quarantine

use mie_adapter_binance::archive::ledger::{Quarantine, SupersededEntry};
use mie_adapter_parquet::copy_ledger::{LedgerBuilder, Record};
use mie_adapter_parquet::replica::QuarantinedCopy;

const QID: &str = "q-1791259200000";

/// S1 of the fixture's archive file moving `moved` (sorted) under
/// [`QID`], naming `entries`.
fn moving(moved: &[&str], entries: Vec<SupersededEntry>) -> Vec<u8> {
    let mut moved: Vec<String> = moved.iter().map(|p| (*p).to_owned()).collect();
    moved.sort();
    SupersedeRecord {
        archive: "data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-01.zip"
            .to_owned(),
        stream: RawStreamKey::new("binance-archive", "BTCUSDT", "aggTrades").unwrap(),
        period: Period::Day(parse_day("2026-10-01").unwrap()),
        entries,
        quarantine: Some(Quarantine {
            id: QID.to_owned(),
            moved,
        }),
    }
    .render()
    .into_bytes()
}

/// The start's copy state from `kinds`.
fn start(kinds: Vec<Kind>) -> CopyState {
    let mut builder = LedgerBuilder::empty();
    for kind in kinds {
        builder.push(&Record { at_ms: 1, kind }).unwrap();
    }
    let ledger =
        mie_adapter_parquet::copy_ledger::parse(std::path::Path::new("start"), &builder.finish())
            .unwrap();
    CopyState::fold(&ledger).unwrap()
}

/// A root's quarantine `QID` holding `parts` with their texts.
fn quarantined_on(root: &str, parts: &[(&str, Option<&str>)]) -> RootQuarantine {
    let mut listing = QuarantineListing::default();
    listing.ids.insert(
        QID.to_owned(),
        parts
            .iter()
            .map(|(path, text)| {
                (
                    (*path).to_owned(),
                    QuarantinedCopy {
                        text: text.map(str::to_owned),
                        data: true,
                        unreadable: None,
                    },
                )
            })
            .collect(),
    );
    RootQuarantine {
        root: root.to_owned(),
        listing,
    }
}

fn run_quarantine(
    entries: &BTreeMap<String, Vec<EntryCopy>>,
    roots: &[RootQuarantine],
    layout: &BTreeMap<String, Vec<(String, Option<String>)>>,
    start: &CopyState,
) -> QuarantineFindings {
    quarantine(&QuarantineInputs {
        entries,
        roots,
        layout,
        start,
    })
}

fn record_of(bytes: Vec<u8>) -> BTreeMap<String, Vec<EntryCopy>> {
    BTreeMap::from([(S1.to_owned(), copies(&[("hot", &bytes)]))])
}

#[track_caller]
fn has_problem(found: &QuarantineFindings, needles: &[&str]) {
    assert!(
        found
            .problems
            .iter()
            .any(|p| needles.iter().all(|n| p.contains(n))),
        "no problem with {needles:?}: {:#?}",
        found.problems
    );
}

/// Q1/Q2: an id directory named with a look-alike hyphen, a trailing blank
/// or a zero-width character is named and its files count for nothing.
#[test]
fn quarantine_names_outside_the_id_grammar_are_named_not_recorded() {
    let mut root = RootQuarantine {
        root: "hot".to_owned(),
        listing: QuarantineListing::default(),
    };
    root.listing.bad_names = vec![
        "q\u{2010}1".to_owned(),
        "q-1 ".to_owned(),
        "q\u{200b}-1".to_owned(),
    ];
    let found = run_quarantine(
        &BTreeMap::new(),
        &[root],
        &BTreeMap::new(),
        &start(Vec::new()),
    );
    assert_eq!(found.problems.len(), 3, "{found:#?}");
    assert!(found.records.is_empty() && found.paths.is_empty());
}

/// Q2: a moved path with a fullwidth digit makes its record unparseable
/// (no path of it counts); a non-canonical ASCII path and a live path are
/// named and never recorded, and the derivation goes on.
#[test]
fn moved_paths_outside_the_layout_are_named_never_recorded() {
    let good = part("aggTrades", "2026-10-01", 0);
    let odd =
        "source=binance-archive/instrument=BTCUSDT/stream=aggTrades/date=2026-10-01/part-7.parquet";
    let live =
        "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-01/part-00000.parquet";
    let wide = good.replace("2026-10-01", "2026-10-0\u{ff11}");
    let entries = BTreeMap::from([
        (
            S1.to_owned(),
            copies(&[("hot", &moving(&[&good, odd, live], Vec::new()))]),
        ),
        (
            "aggTrades/BTCUSDT-aggTrades-2026-10-01.zip.2.supersede.import".to_owned(),
            copies(&[("hot", &moving(&[&wide], Vec::new()))]),
        ),
    ]);
    let found = run_quarantine(&entries, &[], &BTreeMap::new(), &start(Vec::new()));
    assert_eq!(found.paths.keys().collect::<Vec<_>>(), vec![&good]);
    has_problem(&found, &[odd, "never recorded"]);
    has_problem(&found, &[live, "never recorded"]);
    assert!(found.records.iter().all(|kind| match kind {
        Kind::Quarantined { path, .. } => *path == good,
        _ => true,
    }));
}

/// Q4/Q5: a moved path that no root's layout or quarantine holds is still
/// recorded quarantined, and named: its bytes are gone.
#[test]
fn a_quarantined_path_gone_from_every_root_is_still_quarantined() {
    let p = part("aggTrades", "2026-10-01", 0);
    let found = run_quarantine(
        &record_of(moving(&[&p], Vec::new())),
        &[],
        &BTreeMap::new(),
        &start(Vec::new()),
    );
    assert_eq!(
        found.records,
        vec![Kind::Quarantined {
            path: p.clone(),
            quarantine: QID.to_owned()
        }]
    );
    has_problem(&found, &[&p, "bytes are gone"]);
}

/// Q2/Q5: a quarantined file no record names takes its directory's id and
/// is named.
#[test]
fn an_unnamed_quarantine_file_is_quarantined_and_named() {
    let p = part("aggTrades", "2026-10-01", 0);
    let t = text(&p, &sha("a"));
    let found = run_quarantine(
        &BTreeMap::new(),
        &[quarantined_on("cold-a", &[(&p, Some(&t))])],
        &BTreeMap::new(),
        &start(Vec::new()),
    );
    assert_eq!(found.paths[&p].record, None);
    assert_eq!(
        found.records,
        vec![
            Kind::Manifest { text: t },
            Kind::Quarantined {
                path: p.clone(),
                quarantine: QID.to_owned()
            }
        ]
    );
    has_problem(&found, &[&p, "no supersede record names it"]);
}

/// Q4/Q5: a start hold on a complete Q path is cleared by `replace`; on
/// an incomplete one (hot's layout still holds it) it stays open and the
/// stop is named; `replaced` follows an entry of generation ≥ 1, never an
/// incomplete path.
#[test]
fn start_holds_on_quarantined_paths() {
    let (done, stopped) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-01", 1),
    );
    let mut entries = record_of(moving(&[&done, &stopped], Vec::new()));
    entries.insert(E1.to_owned(), copies(&[("hot", &entry(&[]))]));
    let layout = BTreeMap::from([(
        stopped.clone(),
        vec![("hot".to_owned(), Some(text(&stopped, &sha("s"))))],
    )]);
    let holds = vec![
        Kind::Hold {
            path: done.clone(),
            reason: HoldReason::Unlisted,
        },
        Kind::Hold {
            path: stopped.clone(),
            reason: HoldReason::Unlisted,
        },
        Kind::Manifest {
            text: text(&done, &sha("d")),
        },
        Kind::Manifest {
            text: text(&stopped, &sha("s")),
        },
    ];
    let found = run_quarantine(
        &entries,
        &[quarantined_on("hot", &[(&done, None)])],
        &layout,
        &start(holds),
    );
    assert_eq!(
        found.records,
        vec![
            Kind::HoldCleared {
                path: done.clone(),
                resolution: Resolution::Replace
            },
            Kind::Quarantined {
                path: done.clone(),
                quarantine: QID.to_owned()
            },
            Kind::Quarantined {
                path: stopped.clone(),
                quarantine: QID.to_owned()
            },
            Kind::Replaced {
                path: done.clone(),
                entry: E1.to_owned()
            },
        ]
    );
    has_problem(&found, &[&stopped, "stopped between renames"]);
    assert!(found.paths[&done].complete && !found.paths[&stopped].complete);
    assert!(
        found
            .lines
            .iter()
            .any(|l| l.contains("1 complete, 1 incomplete"))
    );
}

/// Q4, the text rule: two texts in quarantine give the one whose SHA-256
/// the superseded entry lists, as a full start has it.
#[test]
fn the_quarantine_manifest_text_follows_the_superseded_entry() {
    let p = part("aggTrades", "2026-10-01", 0);
    let (listed, other) = (text(&p, &sha("listed")), text(&p, &sha("other")));
    let e0 = entry(&[(&p, &sha("listed"))]);
    let named = SupersededEntry::of(NAME.to_owned(), &e0);
    let mut entries = record_of(moving(&[&p], vec![named]));
    entries.insert(NAME.to_owned(), copies(&[("hot", &e0)]));
    for order in [[&listed, &other], [&other, &listed]] {
        let found = run_quarantine(
            &entries,
            &[
                quarantined_on("hot", &[(&p, Some(order[0]))]),
                quarantined_on("cold-a", &[(&p, Some(order[1]))]),
            ],
            &BTreeMap::new(),
            &start(Vec::new()),
        );
        assert_eq!(
            found.records[0],
            Kind::Manifest {
                text: listed.clone()
            }
        );
    }
    // Without the entry's evidence, two texts give none.
    let found = run_quarantine(
        &record_of(moving(&[&p], Vec::new())),
        &[
            quarantined_on("hot", &[(&p, Some(&listed))]),
            quarantined_on("cold-a", &[(&p, Some(&other))]),
        ],
        &BTreeMap::new(),
        &start(Vec::new()),
    );
    assert!(
        found
            .records
            .iter()
            .all(|kind| !matches!(kind, Kind::Manifest { .. }))
    );
}
