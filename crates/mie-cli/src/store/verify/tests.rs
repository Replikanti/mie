//! AW11's deterministic interleaving (#192): a replication pass between
//! the verification's ledger snapshot and its first append.

use super::*;
use crate::store::replicate::tests::{D0, Fixture, Probe, Seen};
use mie_adapter_parquet::copy_ledger::CopyLedger;
use std::time::SystemTime;

/// The wall clock, so a part sealed now is within G of it.
fn wall_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// AW11, deterministic: the hook runs a replication pass after S is read
/// and before verify appends anything; the pass copies a newly sealed part
/// and appends `copy` and `ledger-copy` records. The chain then verifies
/// on hot and on every copy, every copy is a byte prefix of hot, and the
/// verification passes, with its records after the pass's.
#[test]
fn aw11_a_pass_between_the_snapshot_and_the_first_append_keeps_every_chain_valid() {
    let fixture = Fixture::new("verify-aw11");
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let new = fixture.seal("binance-um", "aggTrade", D0 + 3_600_000, 2);
    assert!(!new.is_empty());
    let (config, archive, hot) = (
        fixture.config.clone(),
        fixture.archive(),
        fixture.hot.clone(),
    );
    let devices = fixture.probe.0.clone();
    AFTER_SNAPSHOT.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            let report = replicate::run(
                &config,
                Some(&archive),
                &Probe(devices),
                &|| D0 + 2,
                &CopyLedger::at(&hot),
                &mut Seen::default(),
            );
            assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
        }));
    });
    let now = wall_now();
    let report = run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| now,
        &CopyLedger::at(&fixture.hot),
    );
    assert!(
        AFTER_SNAPSHOT.with(|hook| hook.borrow().is_none()),
        "the hook ran"
    );
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");

    let hot_bytes = fs::read(copy_ledger::ledger_path(&fixture.hot)).unwrap();
    assert_eq!(copy_ledger::check_chain(&hot_bytes).failure, None);
    for root in &fixture.cold {
        let copy = fs::read(copy_ledger::ledger_path(root)).unwrap();
        assert_eq!(copy_ledger::check_chain(&copy).failure, None);
        assert!(hot_bytes.starts_with(&copy));
    }
    let records = fixture.records();
    let last_copy = records
        .iter()
        .rposition(|r| matches!(&r.kind, Kind::Copy { path, .. } if new.contains(path)))
        .expect("the hook's pass recorded the new part");
    let first_verify = records
        .iter()
        .position(|r| matches!(r.kind, Kind::Verify { .. }))
        .unwrap();
    assert!(last_copy < first_verify);
    let verify_records: Vec<&Record> = records
        .iter()
        .filter(|r| matches!(r.kind, Kind::Verify { .. }))
        .collect();
    assert_eq!(verify_records.len(), 2);

    // A later verification sees the new part's records and passes too.
    let report = run(
        &fixture.config,
        Some(&fixture.archive()),
        "24",
        &fixture.probe,
        &|| now + 1,
        &CopyLedger::at(&fixture.hot),
    );
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
}

/// Sets `path`'s mtime to `ms` since the epoch, making it writable first.
pub(crate) fn set_mtime(path: &Path, ms: i64) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
    let time = UNIX_EPOCH + std::time::Duration::from_millis(ms as u64);
    fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(time)
        .unwrap();
}

/// Flips one byte of `path`, making it writable first.
pub(crate) fn flip_byte(path: &Path, offset: usize) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
    let mut bytes = fs::read(path).unwrap();
    bytes[offset] ^= 0x01;
    fs::write(path, bytes).unwrap();
}

/// #210 C4, with G = 600 000 ms: a supersede record whose mtime on hot is
/// now − 599 999 is not checked; now − 600 000 and now + 1 are; a flipped
/// byte is `entry`, an absent copy `missing`. A superseded E0 that differs
/// on a target gets no finding, and an earlier open finding about it
/// closes.
#[test]
fn verify_checks_supersede_records_and_skips_superseded_entries() {
    let fixture = Fixture::new("verify-supersede");
    let [e0, s1, _] = fixture.supersede(true);
    assert_eq!(fixture.pass().problems, Vec::<String>::new());
    let hot_s1 = fixture.dir.0.join("ledger").join(&s1);
    let cold_a = |name: &str| fixture.cold[0].join(".mie/import-ledger").join(name);
    flip_byte(&cold_a(&s1), 3);
    // A superseded E0 that differs on cold-a, with an open finding.
    let e0_copy = cold_a(&e0);
    fs::write(&e0_copy, b"another E0\n").unwrap();
    let now = D0 + 10 * 3_600_000;
    CopyLedger::at(&fixture.hot)
        .append(&Record {
            at_ms: now - 1,
            kind: Kind::Finding {
                root: "cold-a".to_owned(),
                code: FindingCode::Entry,
                subject: Subject::Entry(e0.clone()),
            },
        })
        .unwrap();
    let verify = |now: i64| {
        run(
            &fixture.config,
            Some(&fixture.archive()),
            "24",
            &fixture.probe,
            &|| now,
            &CopyLedger::at(&fixture.hot),
        )
    };
    let about = |report: &VerifyReport, name: &str, what: &str| {
        report
            .problems
            .iter()
            .any(|p| p.contains(name) && p.contains(what))
    };
    set_mtime(&hot_s1, now - 599_999);
    let report = verify(now);
    assert!(!about(&report, &s1, "import entry"), "{report:?}");
    assert!(
        !report.problems.iter().any(|p| p.contains(&e0)),
        "{report:?}"
    );
    let ledger = copy_ledger::read(&fixture.hot).unwrap().unwrap();
    let state = CopyState::fold(&ledger).unwrap();
    assert!(state.open_entry_findings("cold-a").is_empty());
    for (at, offset) in [(now + 1, -600_000), (now + 2, 1)] {
        set_mtime(&hot_s1, at + offset);
        let report = verify(at);
        assert!(about(&report, &s1, "entry: the copy differs"), "{report:?}");
    }
    fs::remove_file(cold_a(&s1)).unwrap();
    set_mtime(&hot_s1, now - 600_000);
    let report = verify(now + 3);
    assert!(about(&report, &s1, "missing"), "{report:?}");
}
