//! Shared helpers of the integration tests.

#![allow(dead_code)]

use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{Capture, RawRecord, RawSelection, RawStreamKey};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Milliseconds per UTC day.
pub const DAY: i64 = 86_400_000;
/// 2026-10-06T00:00:00Z.
pub const D0: i64 = 1_791_244_800_000;

/// A fresh directory under the system temp dir, removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mie-adapter-parquet-it-{tag}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `binance-um` / `BTCUSDT` / `name`.
pub fn stream(name: &str) -> RawStreamKey {
    RawStreamKey::new("binance-um", "BTCUSDT", name).expect("valid test stream")
}

/// Record `i` at `time`: payload `i` in decimal, capture metadata on two
/// records out of three.
pub fn record(time: i64, i: u64) -> RawRecord {
    RawRecord {
        event_time: EventTime::from_millis(time),
        capture: (!i.is_multiple_of(3)).then(|| Capture {
            receive_time_ns: time * 1_000_000 + 17,
            receive_seq: i,
            session_id: "session-1".to_owned(),
        }),
        payload: i.to_string().into_bytes(),
    }
}

pub fn window(start: i64, end: i64) -> ReplayWindow {
    ReplayWindow {
        start: EventTime::from_millis(start),
        end: EventTime::from_millis(end),
    }
}

pub fn selection(streams: &[&RawStreamKey], start: i64, end: i64) -> RawSelection {
    let streams: BTreeSet<_> = streams.iter().map(|s| (*s).clone()).collect();
    RawSelection::new(streams, window(start, end)).expect("valid selection")
}

/// Every regular file below `dir`, as paths relative to `dir` with `/`.
pub fn files_below(dir: &Path) -> Vec<String> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<String>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .expect("list")
            .map(|e| e.expect("entry").file_name().into_string().expect("utf-8"))
            .collect();
        entries.sort();
        for name in entries {
            let path = dir.join(&name);
            if path.is_dir() {
                walk(&path, &format!("{prefix}{name}/"), out);
            } else {
                out.push(format!("{prefix}{name}"));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, "", &mut out);
    out
}
