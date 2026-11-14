//! Every store lock is free right after its guard drops, while another
//! thread of the process starts child processes (#220).
//!
//! A store lock is a `flock`, which belongs to the open file description,
//! and a child process holds a copy of every descriptor of its parent from
//! its fork until its exec (`O_CLOEXEC` closes them only at the exec). A
//! guard that only closed its descriptor left the lock held by such a copy
//! for that window, so the next take in the parent saw `Locked`; under load
//! the window stretches from microseconds to milliseconds. One thread here
//! starts [`SPAWNS`] no-op children while the test thread takes, drops and
//! takes again every store lock in a rotation, so many drops land inside a
//! child's window.

mod common;

use common::TempDir;
use mie_adapter_parquet::copy_ledger::{Kind, Record};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::{CopyLedger, ParquetRawStore, RotationPolicy, removal, replica};
use mie_ports::raw::RawStoreError;
use std::collections::BTreeMap;
use std::process::{Command, Stdio};
use std::time::Instant;

/// The children started per run. A spawn that lands a drop in its window
/// fails the run before the fix; even at 2 % per spawn, 200 spawns miss
/// with probability 0.98^200 = 1.8 %.
const SPAWNS: usize = 200;

const PATH: &str =
    "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00000.parquet";

/// The no-op child: it exits as soon as it has exec'd.
#[test]
#[ignore = "child process of every_store_lock_is_free_right_after_its_drop_while_children_spawn"]
fn spawned_child() {}

/// Counts a take: `true` when it holds the lock, `false` (and one more
/// `Locked` for `step`) when it was refused. Any other error fails the
/// test.
fn tally<T>(
    step: &'static str,
    result: Result<T, RawStoreError>,
    locked: &mut BTreeMap<&'static str, u64>,
) -> bool {
    match result {
        Ok(guard) => {
            drop(guard);
            true
        }
        Err(RawStoreError::Locked(_)) => {
            *locked.entry(step).or_default() += 1;
            false
        }
        Err(error) => panic!("{step}: {error}"),
    }
}

#[test]
fn every_store_lock_is_free_right_after_its_drop_while_children_spawn() {
    // A hot root with its marker and a header-only ledger, and a plain root
    // for the writer (a plain store's writer needs no hot window).
    let hot = TempDir::new("flock-fork-hot");
    root_marker::create_meta(hot.path(), HOT_ID, true).unwrap();
    let plain = TempDir::new("flock-fork-plain");
    let hot = hot.path();
    let store = ParquetRawStore::new(plain.path());
    let ledger = CopyLedger::at(hot);
    let started = Instant::now();
    let spawner = std::thread::spawn(|| {
        let exe = std::env::current_exe().unwrap();
        for _ in 0..SPAWNS {
            let status = Command::new(&exe)
                .args(["spawned_child", "--exact", "--ignored"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("start the child");
            assert!(status.success(), "the child failed: {status}");
        }
    });
    let mut locked = BTreeMap::new();
    let mut cycles = 0_u64;
    let mut at = 0_i64;
    while !spawner.is_finished() {
        // Each step: take, drop, take again (refused only if the drop left
        // the lock held), drop.
        for _ in 0..2 {
            if !tally(
                "replication",
                replica::try_replication_lock(hot),
                &mut locked,
            ) {
                break;
            }
        }
        for _ in 0..2 {
            if !tally("verify", replica::try_verify_lock(hot), &mut locked) {
                break;
            }
        }
        for _ in 0..2 {
            let take = removal::take_source_lock(hot, "binance-um");
            if !tally("source", take, &mut locked) {
                break;
            }
        }
        for _ in 0..2 {
            if !tally("ledger hold", ledger.try_hold(), &mut locked) {
                break;
            }
        }
        // The blocking take of an append, then a hold.
        let record = Record {
            at_ms: at,
            kind: Kind::Copy {
                path: PATH.to_owned(),
                root: "cold-a".to_owned(),
            },
        };
        at += 1;
        ledger.append(&record).map_err(|e| e.error).unwrap();
        tally("hold after append", ledger.try_hold(), &mut locked);
        // A writer dropped without `close` (the shape of the `aw10` flake),
        // then the source's lock.
        let writer = store.writer("binance-archive", RotationPolicy::default());
        if tally("writer", writer, &mut locked) {
            let take = removal::take_source_lock(plain.path(), "binance-archive");
            tally("source after writer", take, &mut locked);
        }
        cycles += 1;
    }
    spawner.join().unwrap();
    println!(
        "{cycles} cycles beside {SPAWNS} children in {:?}, locked: {locked:?}",
        started.elapsed()
    );
    assert!(cycles > 0, "no cycle ran while the children started");
    assert!(
        locked.is_empty(),
        "a dropped lock was still held: {locked:?}"
    );
}
