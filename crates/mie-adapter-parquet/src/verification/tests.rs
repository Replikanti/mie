//! The copy state (#192): the remover's open-finding rule, L1, and the
//! journal, entry and verification-pass rules.

use super::{CopyState, PathVerdict, open_ledger_findings};
use crate::copy_ledger::{self, CopyLedger, FindingCode, Kind, Record, Subject};
use crate::layout::PartFile;
use crate::testutil::{FIXTURE_D0 as D0, TieredFixture, stream};

fn path(n: u32) -> String {
    format!(
        "{}/{}",
        TieredFixture::partition(&stream("aggTrade"), 0),
        PartFile::Data.name(n)
    )
}

fn state(fx: &TieredFixture) -> CopyState {
    CopyState::fold(&copy_ledger::read(&fx.hot).unwrap().unwrap()).unwrap()
}

fn finding(root: &str, code: FindingCode, subject: Subject) -> Kind {
    Kind::Finding {
        root: root.to_owned(),
        code,
        subject,
    }
}

/// Appends `kind` at exactly `at_ms` (a verification pass shares one).
fn append_at(fx: &TieredFixture, at_ms: i64, kind: Kind) {
    CopyLedger::at(&fx.hot)
        .append(&Record { at_ms, kind })
        .unwrap();
}

/// The remover's rule: a finding voids a copy until a later `copy`
/// record, whose end then counts; every finding code voids, and only a
/// `copy` record closes it.
#[test]
fn a_finding_voids_a_copy_until_a_later_copy_record() {
    let fx = TieredFixture::new("verification-open");
    fx.copy_path(&path(0), &["cold-a", "cold-b"]);
    let copied = state(&fx);
    let first = copied.copy(&path(0), "cold-a").unwrap();
    assert!(first.valid_end.is_some() && first.open.is_none());
    assert_eq!(first.valid_end, first.latest.map(|(end, _)| end));
    assert_eq!(copied.verdict(&path(0)), Some(PathVerdict::Ordinary));
    assert!(copied.ledger_text(&path(0)).is_some());

    for code in [FindingCode::Sha256, FindingCode::Stale] {
        fx.append(finding("cold-a", code, Subject::File(path(0))));
        let voided = state(&fx);
        let copy = voided.copy(&path(0), "cold-a").unwrap();
        assert_eq!((copy.valid_end, copy.open), (None, Some(code)));
        // The other root's copy is untouched.
        assert!(voided.copy(&path(0), "cold-b").unwrap().valid_end.is_some());
        fx.append(Kind::Copy {
            path: path(0),
            root: "cold-a".to_owned(),
        });
        let again = state(&fx);
        let copy = again.copy(&path(0), "cold-a").unwrap();
        assert!(copy.open.is_none());
        assert_eq!(copy.valid_end, copy.latest.map(|(end, _)| end));
    }
    // A finding about a copy no record names opens it without a copy.
    fx.append(finding(
        "cold-a",
        FindingCode::Missing,
        Subject::File(path(1)),
    ));
    let copy = *state(&fx).copy(&path(1), "cold-a").unwrap();
    assert_eq!(
        (copy.valid_end, copy.latest, copy.open),
        (None, None, Some(FindingCode::Missing))
    );
}

/// L1: a copy-ledger finding voids the root's `ledger-copy` record until
/// a later one, and the replication pass reads the same open set.
#[test]
fn a_copy_ledger_finding_voids_the_ledger_copy_until_a_later_record() {
    let fx = TieredFixture::new("verification-l1");
    fx.copy_path(&path(0), &["cold-a", "cold-b"]);
    fx.ledger_copies();
    let before = state(&fx);
    assert!(before.ledger_copy("cold-a").is_some() && !before.open_ledger_finding("cold-a"));
    fx.append(finding("cold-a", FindingCode::Chain, Subject::CopyLedger));
    let voided = state(&fx);
    assert_eq!(voided.ledger_copy("cold-a"), None);
    assert!(voided.has_ledger_copy_record("cold-a"));
    assert!(voided.open_ledger_finding("cold-a"));
    assert!(voided.ledger_copy("cold-b").is_some());
    let records = copy_ledger::read(&fx.hot).unwrap().unwrap().records;
    assert_eq!(open_ledger_findings(&records), ["cold-a".to_owned()].into());
    fx.ledger_copy(0);
    let again = state(&fx);
    assert!(again.ledger_copy("cold-a").is_some() && !again.open_ledger_finding("cold-a"));
    let records = copy_ledger::read(&fx.hot).unwrap().unwrap().records;
    assert!(open_ledger_findings(&records).is_empty());
}

/// A journal or entry finding stays open through its own pass and closes
/// with a later pass that did not find it again; a pass groups the
/// findings of its root and `at_ms` only.
#[test]
fn journal_and_entry_findings_close_with_a_later_clean_pass() {
    let fx = TieredFixture::new("verification-tails");
    fx.copy_path(&path(0), &["cold-a"]);
    let entry = "aggTrades/x.zip.import".to_owned();
    let t = D0 + 100;
    // A stopped pass: a finding at t - 1 that no `verify` record follows.
    append_at(
        &fx,
        t - 1,
        finding("cold-a", FindingCode::Sha256, Subject::File(path(0))),
    );
    append_at(
        &fx,
        t,
        finding("cold-a", FindingCode::Prefix, Subject::Journal),
    );
    append_at(
        &fx,
        t,
        finding("cold-a", FindingCode::Entry, Subject::Entry(entry.clone())),
    );
    append_at(
        &fx,
        t,
        Kind::Verify {
            root: "cold-a".to_owned(),
            prefix_bytes: 10,
            findings: 2,
        },
    );
    let open = state(&fx);
    assert!(open.open_journal_finding("cold-a"));
    assert_eq!(
        open.open_entry_findings("cold-a"),
        std::slice::from_ref(&entry)
    );
    assert!(!open.open_journal_finding("cold-b"));
    let passes = open.verifies("cold-a");
    assert_eq!(passes.len(), 1);
    // The stopped pass's file finding belongs to no pass.
    assert!(passes[0].files.is_empty(), "{passes:?}");

    // The next pass finds the entry again, not the journal.
    append_at(
        &fx,
        t + 1,
        finding("cold-a", FindingCode::Entry, Subject::Entry(entry.clone())),
    );
    append_at(
        &fx,
        t + 1,
        Kind::Verify {
            root: "cold-a".to_owned(),
            prefix_bytes: 20,
            findings: 1,
        },
    );
    let later = state(&fx);
    assert!(!later.open_journal_finding("cold-a"));
    assert_eq!(later.open_entry_findings("cold-a"), [entry]);
    append_at(
        &fx,
        t + 2,
        Kind::Verify {
            root: "cold-a".to_owned(),
            prefix_bytes: 30,
            findings: 0,
        },
    );
    assert!(state(&fx).open_entry_findings("cold-a").is_empty());
}

/// The last good verification is the later of the latest `copy` record
/// and the latest pass that covers it and recorded nothing about the
/// file.
#[test]
fn the_last_good_verification_needs_a_covering_pass_without_a_finding() {
    let fx = TieredFixture::new("verification-last-good");
    fx.copy_path(&path(0), &["cold-a"]);
    fx.copy_path(&path(1), &["cold-a"]);
    let copied = state(&fx);
    let (end, at) = copied.copy(&path(0), "cold-a").unwrap().latest.unwrap();
    assert_eq!(copied.last_good_verification(&path(0), "cold-a"), Some(at));
    assert_eq!(copied.last_good_verification(&path(0), "cold-b"), None);
    let verify = |fx: &TieredFixture, at_ms: i64, prefix_bytes: u64| {
        append_at(
            fx,
            at_ms,
            Kind::Verify {
                root: "cold-a".to_owned(),
                prefix_bytes,
                findings: 0,
            },
        );
    };
    // A pass that does not cover the record does not count.
    verify(&fx, D0 + 1_000, end - 1);
    assert_eq!(
        state(&fx).last_good_verification(&path(0), "cold-a"),
        Some(at)
    );
    verify(&fx, D0 + 2_000, end);
    assert_eq!(
        state(&fx).last_good_verification(&path(0), "cold-a"),
        Some(D0 + 2_000)
    );
    // A later pass with a finding about path(1) only still counts for
    // path(0), not for path(1), whose copy record the pass at D0 + 2000
    // does not cover.
    let t = D0 + 3_000;
    append_at(
        &fx,
        t,
        finding("cold-a", FindingCode::Size, Subject::File(path(1))),
    );
    verify(&fx, t, end + 10_000);
    let after = state(&fx);
    assert_eq!(after.last_good_verification(&path(0), "cold-a"), Some(t));
    let (_, at1) = after.copy(&path(1), "cold-a").unwrap().latest.unwrap();
    assert_eq!(after.last_good_verification(&path(1), "cold-a"), Some(at1));
    assert!(after.has_copies_on("cold-a") && !after.has_copies_on("cold-b"));
}
