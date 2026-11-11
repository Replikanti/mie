//! The pure parts of `mie store resolve` (#210): proposals (RA3),
//! attribution (RA4) and the plan table (RA5).

use super::*;
use crate::store::archive_checks::tests::{entry, part, sha, text};
use mie_adapter_binance::archive::catalog::parse_day;

const SYMBOL: &str = "BTCUSDT";
const F: &str = "BTCUSDT-aggTrades-2026-10-01.zip";
const ARCHIVE: &str = "data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-01.zip";

fn manifest(path: &str) -> SealedFile {
    replica::parse_manifest(&text(path, &sha(path))).unwrap()
}

fn held(path: &str, reason: HoldReason) -> HeldPart {
    let t = text(path, &sha(path));
    HeldPart {
        path: path.to_owned(),
        reason,
        hot_text: Some(t.clone()),
        ledger_text: Some(t),
    }
}

/// An entry of F listing `paths` with their manifests' SHA-256; `zip`
/// tags the published SHA-256, so two tags are two variants.
fn listing(zip: &str, paths: &[&str]) -> Vec<u8> {
    let files: Vec<(String, String)> = paths.iter().map(|p| ((*p).to_owned(), sha(p))).collect();
    let refs: Vec<(&str, &str)> = files
        .iter()
        .map(|(p, s)| (p.as_str(), s.as_str()))
        .collect();
    let text = String::from_utf8(entry(&refs)).unwrap();
    text.replace(&sha("zip"), &sha(zip)).into_bytes()
}

fn record(quarantine: Option<&str>) -> Vec<u8> {
    SupersedeRecord {
        archive: ARCHIVE.to_owned(),
        stream: key(),
        period: Period::Day(parse_day("2026-10-01").unwrap()),
        entries: Vec::new(),
        quarantine: quarantine.map(|id| mie_adapter_binance::archive::ledger::Quarantine {
            id: id.to_owned(),
            moved: vec![part("aggTrades", "2026-10-01", 0)],
        }),
    }
    .render()
    .into_bytes()
}

fn key() -> RawStreamKey {
    RawStreamKey::new(ARCHIVE_SOURCE, SYMBOL, "aggTrades").unwrap()
}

fn file(root: &str, name: &str, bytes: &[u8]) -> FileOf {
    FileOf {
        root: root.to_owned(),
        name: LedgerName::parse(name).unwrap(),
        bytes: bytes.to_vec(),
    }
}

fn inputs(files: Vec<FileOf>, parts: &[&str]) -> PlanInputs {
    PlanInputs {
        segment: "aggTrades".to_owned(),
        archive: ARCHIVE.to_owned(),
        stream: key(),
        period: Period::Day(parse_day("2026-10-01").unwrap()),
        files,
        malformed: Vec::new(),
        parts: parts.iter().map(|p| manifest(p)).collect(),
        verdicts: BTreeMap::new(),
        located: BTreeMap::new(),
    }
}

#[track_caller]
fn refused(inputs: &PlanInputs, needles: &[&str]) {
    match plan(inputs) {
        Ok(planned) => panic!("planned {planned:?}"),
        Err(why) => assert!(needles.iter().all(|n| why.contains(n)), "{why}"),
    }
}

/// RA5, cases (a) to (d) and every refusal.
#[test]
fn plan_table() {
    let (a, b) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-01", 1),
    );
    let e0 = format!("{F}.import");
    let (e1, s1) = (format!("{F}.1.import"), format!("{F}.1.supersede.import"));
    let both = [a.as_str(), b.as_str()];

    // (a) No file: the missing entry, the check set is parts(F).
    let planned = plan(&inputs(Vec::new(), &both)).unwrap();
    assert_eq!(planned.case, Case::Missing);
    assert_eq!((planned.record.is_some(), planned.entry), (false, Some(0)));
    assert_eq!(planned.check_set, vec![a.clone(), b.clone()]);

    // (b) E0 and S1 with `quarantine -`: an interrupted re-admit writes E1.
    let v1 = listing("v1", &both);
    let planned = plan(&inputs(
        vec![
            file("hot", &e0, &v1),
            file("hot", &s1, &record(None)),
            file("cold-a", &s1, &record(None)),
        ],
        &both,
    ))
    .unwrap();
    assert_eq!(planned.case, Case::Interrupted);
    assert_eq!((planned.record.is_some(), planned.entry), (false, Some(1)));
    // S1 with a quarantine id: a pending replace, #211.
    refused(
        &inputs(
            vec![
                file("hot", &e0, &v1),
                file("hot", &s1, &record(Some("q-1"))),
            ],
            &both,
        ),
        &["q-1", "#211"],
    );

    // (c) The latest entry lists the held parts: no import-ledger write.
    let planned = plan(&inputs(
        vec![
            file("hot", &e0, &listing("v0", &both)),
            file("cold-a", &e0, &listing("v0", &both)),
            file("hot", &s1, &record(None)),
            file("hot", &e1, &v1),
        ],
        &[a.as_str()],
    ))
    .unwrap();
    assert_eq!(planned.case, Case::Listed);
    assert_eq!((planned.record.is_some(), planned.entry), (false, None));
    assert_eq!(planned.check_set, vec![a.clone(), b.clone()]);
    // ...unless another path it lists is not ordinary: then supersede.
    let mut held_b = inputs(
        vec![file("hot", &s1, &record(None)), file("hot", &e1, &v1)],
        &[a.as_str()],
    );
    held_b.verdicts.insert(b.clone(), PathVerdict::Held);
    held_b.located.insert(b.clone(), vec!["hot".to_owned()]);
    refused(&held_b, &[&b, "holds or records it"]);

    // (d) Two variants of E0: S1 names both, sorted, then E1.
    let v2 = listing("v2", &both);
    let planned = plan(&inputs(
        vec![
            file("hot", &e0, &v1),
            file("cold-a", &e0, &v2),
            file("cold-b", &e0, &v1),
        ],
        &both,
    ))
    .unwrap();
    assert_eq!(planned.case, Case::Supersede);
    assert_eq!(planned.entry, Some(1));
    let (n, record_d) = planned.record.unwrap();
    assert_eq!(n, 1);
    let mut expected = vec![
        SupersededEntry::of(format!("aggTrades/{e0}"), &v1),
        SupersededEntry::of(format!("aggTrades/{e0}"), &v2),
    ];
    expected.sort();
    assert_eq!(record_d.entries, expected);
    assert_eq!(record_d.quarantine, None);
    assert_eq!(planned.check_set, vec![a.clone(), b.clone()]);
    // E0, S1, E1 present and E1 does not list the parts: n = 2.
    let planned = plan(&inputs(
        vec![
            file("hot", &e0, &v1),
            file("hot", &s1, &record(None)),
            file("hot", &e1, &listing("v3", &[a.as_str()])),
        ],
        &both,
    ))
    .unwrap();
    assert_eq!((planned.case, planned.entry), (Case::Supersede, Some(2)));
    assert_eq!(planned.record.unwrap().0, 2);

    // A file of F on a target only.
    refused(
        &inputs(
            vec![file("hot", &e0, &v1), file("cold-a", &s1, &record(None))],
            &both,
        ),
        &[&s1, "cold-a only", "mie store repair"],
    );
    // Differing supersede-record copies.
    refused(
        &inputs(
            vec![
                file("hot", &e0, &v1),
                file("hot", &s1, &record(None)),
                file("cold-a", &s1, b"damaged\n"),
            ],
            &both,
        ),
        &[&s1, "differ"],
    );
    // A malformed name of F.
    let mut malformed = inputs(vec![file("hot", &e0, &v1)], &both);
    malformed.malformed = vec![format!("{F}.01.import on hot")];
    refused(&malformed, &[&format!("{F}.01.import"), "malformed"]);
    // The generation never wraps.
    let top = format!("{F}.{}.import", u32::MAX);
    let wrong = String::from_utf8(listing("top", &[a.as_str()]))
        .unwrap()
        .into_bytes();
    refused(
        &inputs(
            vec![
                file(
                    "hot",
                    &format!("{F}.{}.supersede.import", u32::MAX),
                    &record(None),
                ),
                file("hot", &top, &wrong),
            ],
            &both,
        ),
        &["exceed", "4294967295"],
    );

    // No orphan: a superseded variant lists c, which the ledger records
    // lost, or which hot holds: refused; held by nothing: named.
    let c = part("aggTrades", "2026-10-01", 2);
    let with_c = listing("vc", &[a.as_str(), b.as_str(), c.as_str()]);
    let orphan = |located: Option<&str>| {
        let mut inputs = inputs(
            vec![file("hot", &e0, &with_c), file("cold-a", &e0, &v1)],
            &both,
        );
        if let Some(place) = located {
            inputs.located.insert(c.clone(), vec![place.to_owned()]);
        }
        inputs
    };
    refused(&orphan(Some("the copy ledger (Lost)")), &[&c, "Lost"]);
    refused(&orphan(Some("hot")), &[&c, "hot"]);
    let planned = plan(&orphan(None)).unwrap();
    assert_eq!(planned.case, Case::Supersede);
    assert!(
        planned
            .lines
            .iter()
            .any(|l| l.contains(&c) && l.contains("no root holds it")),
        "{:?}",
        planned.lines
    );
}

/// RA4: parts(F) and its refusals.
#[test]
fn attribution_refusals() {
    let (a, b) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-02", 0),
    );
    let holds = vec![
        held(&a, HoldReason::Unlisted),
        held(&b, HoldReason::Unlisted),
    ];
    let parts = attribute(SYMBOL, ARCHIVE, &holds, None).unwrap();
    assert_eq!(parts, vec![manifest(&a)]);

    // None proposes the file: the output names what the holds propose.
    let other = ARCHIVE.replace("2026-10-01", "2026-09-30");
    let why = attribute(SYMBOL, &other, &holds, None).unwrap_err();
    assert!(
        why.contains(ARCHIVE) && why.contains("2026-10-02.zip"),
        "{why}"
    );
    // A manifest-conflict hold names #211.
    let why = attribute(
        SYMBOL,
        ARCHIVE,
        &[held(&a, HoldReason::ManifestConflict)],
        None,
    )
    .unwrap_err();
    assert!(
        why.contains("manifest-conflict") && why.contains("#211"),
        "{why}"
    );
    // No manifest on hot or in the ledger: no proposal, named.
    let bare = HeldPart {
        hot_text: None,
        ledger_text: None,
        ..held(&a, HoldReason::Unlisted)
    };
    let why = attribute(SYMBOL, ARCHIVE, &[bare], None).unwrap_err();
    assert!(why.contains(&a) && why.contains("no manifest"), "{why}");
    // Hot's text differs from the ledger's (D5).
    let differing = HeldPart {
        hot_text: Some(text(&a, &sha("other"))),
        ..held(&a, HoldReason::Unlisted)
    };
    let why = attribute(SYMBOL, ARCHIVE, &[differing], None).unwrap_err();
    assert!(why.contains(&a) && why.contains("D5"), "{why}");
    // `PENDING` names the file.
    let why = attribute(SYMBOL, ARCHIVE, &holds, Some(ARCHIVE)).unwrap_err();
    assert!(why.contains("PENDING"), "{why}");
}

/// RA3: a part whose first and last ordering times map to two files gets
/// no proposal.
#[test]
fn a_part_spanning_two_periods_gets_no_proposal() {
    let a = part("aggTrades", "2026-10-01", 0);
    let mut file = manifest(&a);
    assert_eq!(propose(SYMBOL, &a, Some(&file)), Ok(ARCHIVE.to_owned()));
    file.max_event_time =
        mie_domain::time::EventTime::from_millis(parse_day("2026-10-02").unwrap() * 86_400_000);
    let why = propose(SYMBOL, &a, Some(&file)).unwrap_err();
    assert!(why.contains("two files"), "{why}");
    // A live part and another symbol's part: no proposal, named.
    let live =
        "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-01/part-00000.parquet";
    assert!(propose(SYMBOL, live, Some(&file)).is_err());
    assert!(
        propose("ETHUSDT", &a, Some(&manifest(&a)))
            .unwrap_err()
            .contains("ETHUSDT")
    );
    let (lines, none) = proposals(SYMBOL, &[held(&a, HoldReason::Unlisted)]);
    assert!(!none);
    assert_eq!(
        lines,
        vec![format!("held {a} (unlisted): --file {ARCHIVE}")]
    );
    assert_eq!(proposals(SYMBOL, &[]), (Vec::new(), true));
}
