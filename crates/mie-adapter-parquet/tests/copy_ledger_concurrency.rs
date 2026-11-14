//! The copy ledger under concurrent appenders (ADR-062 D7): one appender at
//! a time through the lock file, threads and processes alike, readers
//! without a lock, and the whole-run hold.
//!
//! The counts are what makes a lock-free appender fail: two appenders that
//! read the same tail write two records chained to it, so the next append
//! either truncates one of them as a broken trailing record (a record goes
//! missing) or finds damage before the last record and fails. 4 threads ×
//! 200 appends and 2 processes × 200 appends give hundreds of contended
//! appends per run.

mod common;

use common::TempDir;
use mie_adapter_parquet::CopyLedger;
use mie_adapter_parquet::copy_ledger::{self, Kind, Record};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_ports::raw::RawStoreError;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const ROOT_VAR: &str = "MIE_COPY_LEDGER_TEST_ROOT";
const APPENDS: i64 = 200;
const PATH: &str =
    "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00000.parquet";

/// Appender `who`'s record number `i`: its root id names the appender, its
/// time the position in that appender's sequence.
fn nth(who: &str, i: i64) -> Record {
    Record {
        at_ms: i,
        kind: Kind::Copy {
            path: PATH.to_owned(),
            root: who.to_owned(),
        },
    }
}

/// A hot root with its marker and a header-only ledger.
fn hot(tag: &str) -> TempDir {
    let dir = TempDir::new(tag);
    root_marker::create_meta(dir.path(), HOT_ID, true).unwrap();
    dir
}

/// Checks the whole chain and that every appender's records appear in its
/// own order, `count` each.
fn assert_sequences(root: &Path, appenders: &[&str], count: i64) {
    let bytes = std::fs::read(copy_ledger::ledger_path(root)).unwrap();
    let check = copy_ledger::check_chain(&bytes);
    assert_eq!(check.failure, None);
    assert_eq!(check.tail_bytes, 0);
    assert_eq!(check.records, appenders.len() as u64 * count as u64);
    let ledger = copy_ledger::read(root).unwrap().unwrap();
    for who in appenders {
        let times: Vec<i64> = ledger
            .records
            .iter()
            .filter(|r| matches!(&r.kind, Kind::Copy { root, .. } if root == who))
            .map(|r| r.at_ms)
            .collect();
        assert_eq!(times, (0..count).collect::<Vec<_>>(), "{who}");
    }
}

fn wait_for(path: &Path, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("the child did not create {} within 30 s", path.display());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn spawn(test: &str, root: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args([test, "--exact", "--ignored", "--nocapture"])
        .env(ROOT_VAR, root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

#[test]
fn concurrent_threads_leave_a_valid_chain_and_readers_never_fail() {
    let dir = hot("ledger-threads");
    let root = dir.path().to_path_buf();
    let done = Arc::new(AtomicBool::new(false));
    let reader = {
        let (root, done) = (root.clone(), Arc::clone(&done));
        std::thread::spawn(move || {
            let mut reads = 0_u64;
            while !done.load(Ordering::Relaxed) {
                let ledger = copy_ledger::read(&root).expect("a reader never fails");
                assert!(ledger.is_some());
                reads += 1;
            }
            reads
        })
    };
    let names = ["t0", "t1", "t2", "t3"];
    let writers: Vec<_> = names
        .iter()
        .map(|&who| {
            let root = root.clone();
            std::thread::spawn(move || {
                // Its own appender: every append opens its own lock
                // descriptor, so the lock excludes the threads.
                let ledger = CopyLedger::at(&root);
                for i in 0..APPENDS {
                    let outcome = ledger.append(&nth(who, i)).unwrap();
                    assert_eq!(outcome.truncation, None, "{who} {i}");
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    done.store(true, Ordering::Relaxed);
    assert!(reader.join().unwrap() > 0);
    assert_sequences(&root, &names, APPENDS);
}

/// The child of `concurrent_processes_leave_a_valid_chain`: waits for the
/// parent's start signal, then appends its sequence.
#[test]
#[ignore = "child process of concurrent_processes_leave_a_valid_chain"]
fn child_appender() {
    let Some(root) = std::env::var_os(ROOT_VAR) else {
        return;
    };
    let root = Path::new(&root);
    std::fs::write(root.join("ready"), b"").unwrap();
    while !root.join("go").exists() {
        std::thread::sleep(Duration::from_millis(1));
    }
    let ledger = CopyLedger::at(root);
    for i in 0..APPENDS {
        ledger.append(&nth("child", i)).unwrap();
    }
}

#[test]
fn concurrent_processes_leave_a_valid_chain() {
    let dir = hot("ledger-processes");
    let root = dir.path();
    let mut child = spawn("child_appender", root);
    wait_for(&root.join("ready"), &mut child);
    std::fs::write(root.join("go"), b"").unwrap();
    let ledger = CopyLedger::at(root);
    for i in 0..APPENDS {
        ledger.append(&nth("parent", i)).unwrap();
    }
    assert!(child.wait().unwrap().success());
    assert_sequences(root, &["parent", "child"], APPENDS);
}

/// The child of `the_whole_run_hold_refuses_while_another_process_holds_it`:
/// takes the hold and keeps it until the parent's release signal.
#[test]
#[ignore = "child process of the_whole_run_hold_refuses_while_another_process_holds_it"]
fn child_holder() {
    let Some(root) = std::env::var_os(ROOT_VAR) else {
        return;
    };
    let root = Path::new(&root);
    let hold = CopyLedger::at(root).try_hold().unwrap();
    hold.append(&nth("child", 0)).unwrap();
    std::fs::write(root.join("held"), b"").unwrap();
    while !root.join("release").exists() {
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(hold);
}

#[test]
fn the_whole_run_hold_refuses_while_another_process_holds_it() {
    let dir = hot("ledger-hold");
    let root = dir.path();
    let mut child = spawn("child_holder", root);
    wait_for(&root.join("held"), &mut child);
    let ledger = CopyLedger::at(root);
    let refused = ledger.try_hold();
    assert!(
        matches!(refused, Err(RawStoreError::Locked(_))),
        "{refused:?}"
    );
    std::fs::write(root.join("release"), b"").unwrap();
    assert!(child.wait().unwrap().success());
    let hold = ledger.try_hold().unwrap();
    hold.append(&nth("parent", 0)).unwrap();

    // Two handles in one process exclude each other the same way.
    assert!(matches!(
        CopyLedger::at(root).try_hold(),
        Err(RawStoreError::Locked(_))
    ));
    drop(hold);
    CopyLedger::at(root).try_hold().unwrap();
    assert_sequences(root, &["child", "parent"], 1);
}

/// Review of #197, finding 1: an append that waits behind a whole-run hold
/// opens the ledger only once it has the lock, so a rebuild that replaced
/// the ledger by rename meanwhile (ADR-062 D7 step 6) cannot swallow the
/// record into the replaced file.
#[test]
fn an_append_waiting_behind_a_hold_writes_the_replacing_ledger() {
    let dir = hot("ledger-replaced");
    let root = dir.path().to_path_buf();
    let ledger = CopyLedger::at(&root);
    ledger.append(&nth("first", 0)).unwrap();
    let hold = ledger.try_hold().unwrap();
    let waiter = {
        let root = root.clone();
        std::thread::spawn(move || CopyLedger::at(&root).append(&nth("waiter", 0)))
    };
    // Give the waiter time to block on the lock.
    std::thread::sleep(Duration::from_millis(300));
    // The holder's rebuild: a temp file renamed over the ledger.
    let path = copy_ledger::ledger_path(&root);
    let tmp = path.with_extension("rebuild.tmp");
    std::fs::copy(&path, &tmp).unwrap();
    std::fs::rename(&tmp, &path).unwrap();
    drop(hold);
    waiter.join().unwrap().unwrap();
    assert_sequences(&root, &["first", "waiter"], 1);
}
