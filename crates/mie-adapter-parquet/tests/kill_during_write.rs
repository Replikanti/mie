//! Acceptance (#8): killing a writer mid-write leaves only valid sealed
//! files.
//!
//! The test re-runs its own binary as a child that appends a deterministic
//! sequence forever, SIGKILLs it at several points, and checks the end
//! state: every visible `*.parquet` is complete, and after recovery every
//! file is sealed, verified, and together they hold an exact prefix of the
//! sequence.

mod common;

use common::{D0, DAY, TempDir, selection, stream};
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_domain::time::EventTime;
use mie_ports::raw::{Capture, RawRecord, RawRecordSink, RawRecordSource};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const ROOT_VAR: &str = "MIE_RAW_KILL_TEST_ROOT";
/// Upper bound on the child's sequence, so an orphaned child ends by itself.
const MAX_RECORDS: u64 = 20_000_000;
/// Delays between the first sealed manifest and the kill, in milliseconds.
const KILL_DELAYS_MS: [u64; 8] = [0, 1, 3, 7, 15, 30, 60, 120];

fn policy() -> RotationPolicy {
    RotationPolicy {
        max_rows: 97,
        ..RotationPolicy::default()
    }
}

/// Record `i` of the child's sequence.
fn nth(i: u64) -> RawRecord {
    RawRecord {
        event_time: EventTime::from_millis(D0 + i as i64),
        capture: Some(Capture {
            receive_time_ns: i as i64,
            receive_seq: i,
            session_id: "kill-test".to_owned(),
        }),
        payload: i.to_string().into_bytes(),
    }
}

/// The child process: appends the sequence until it is killed. Does nothing
/// when run without the parent's environment.
#[test]
#[ignore = "child process of writer_killed_mid_write_leaves_only_sealed_files"]
fn child_writer() {
    let Some(root) = std::env::var_os(ROOT_VAR) else {
        return;
    };
    let store = ParquetRawStore::new(root);
    let mut writer = store.writer("binance-um", policy()).unwrap();
    let key = stream("aggTrade");
    for i in 0..MAX_RECORDS {
        writer.append(&key, nth(i)).unwrap();
    }
}

fn has_file_with_suffix(dir: &Path, suffix: &str) -> bool {
    common::files_below(dir).iter().any(|f| f.ends_with(suffix))
}

#[test]
fn writer_killed_mid_write_leaves_only_sealed_files() {
    let exe = std::env::current_exe().unwrap();
    for delay in KILL_DELAYS_MS {
        let dir = TempDir::new("kill");
        let mut child = Command::new(&exe)
            .args(["child_writer", "--exact", "--ignored", "--nocapture"])
            .env(ROOT_VAR, dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        while !has_file_with_suffix(dir.path(), ".manifest") {
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("the child sealed no file within 10 s");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        std::thread::sleep(Duration::from_millis(delay));
        child.kill().unwrap();
        child.wait().unwrap();

        // Before recovery: every visible data file is a complete Parquet file.
        let visible: Vec<String> = common::files_below(dir.path())
            .into_iter()
            .filter(|f| f.ends_with(".parquet"))
            .collect();
        assert!(!visible.is_empty());
        for rel in &visible {
            let data = bytes::Bytes::from(std::fs::read(dir.path().join(rel)).unwrap());
            let reader = ParquetRecordBatchReaderBuilder::try_new(data)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                batch.unwrap();
            }
        }

        // Recovery leaves only sealed files that hold a prefix of the sequence.
        let store = ParquetRawStore::new(dir.path());
        let writer = store.writer("binance-um", policy()).unwrap();
        let report = writer.recovery().clone();
        writer.close().unwrap();
        let all = common::files_below(dir.path());
        assert!(
            all.iter().all(|f| !f.ends_with(".tmp")),
            "{delay} ms: {all:?}"
        );
        for rel in all.iter().filter(|f| f.ends_with(".parquet")) {
            let manifest = rel.replace(".parquet", ".manifest");
            assert!(all.contains(&manifest), "{delay} ms: {rel} is not sealed");
        }
        let key = stream("aggTrade");
        let dataset = store.select(&selection(&[&key], D0, D0 + DAY)).unwrap();
        let mut next = 0u64;
        for file in &dataset.files {
            for record in store.read(file).unwrap() {
                assert_eq!(record, nth(next), "{delay} ms: {}", file.relative_path);
                next += 1;
            }
        }
        assert!(next >= 97, "{delay} ms: only {next} records survived");
        assert!(report.discarded.len() <= 1, "{delay} ms: {report:?}");
    }
}
