//! Test helpers: self-cleaning temp directories and deterministic records.

use mie_domain::time::EventTime;
use mie_ports::raw::{Capture, RawRecord, RawStreamKey};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A fresh directory under the system temp dir, removed on drop.
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mie-adapter-parquet-unit-{tag}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `binance-um` / `BTCUSDT` / `name`.
pub(crate) fn stream(name: &str) -> RawStreamKey {
    RawStreamKey::new("binance-um", "BTCUSDT", name).expect("valid test stream")
}

/// Record `i` at `time`: payload `i` in decimal, capture metadata on two
/// records out of three.
pub(crate) fn record(time: i64, i: i64) -> RawRecord {
    RawRecord {
        event_time: EventTime::from_millis(time),
        capture: (i % 3 != 0).then(|| Capture {
            receive_time_ns: time.saturating_mul(1_000_000).saturating_add(17),
            receive_seq: i.unsigned_abs(),
            session_id: "session-1".to_owned(),
        }),
        payload: i.to_string().into_bytes(),
    }
}
