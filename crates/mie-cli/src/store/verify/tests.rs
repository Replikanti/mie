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
