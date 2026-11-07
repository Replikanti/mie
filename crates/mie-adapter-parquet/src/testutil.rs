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

/// 2026-10-06T00:00:00Z, the first date of [`TieredFixture`].
pub(crate) const FIXTURE_D0: i64 = 1_791_244_800_000;

/// Dates in a [`TieredFixture`].
pub(crate) const FIXTURE_DATES: i64 = 4;

/// Records per stream and date in a [`TieredFixture`], 3 h apart from
/// 00:00 (the last at 15:00), two per part.
pub(crate) const FIXTURE_RECORDS_PER_DATE: i64 = 6;

/// A tiered store on disk, the stand-in for the replicator of #189 and the
/// remover of #191 (ADR-062 D5, D6): hot with its marker and a header-only
/// copy ledger, two cold roots with their markers, and a reference root
/// holding a copy of everything hot held before any move, read as a single
/// root.
///
/// Hot holds two streams over [`FIXTURE_DATES`] dates, three parts per
/// stream and date. Cloning shares the directories, so a test hook can
/// move data while a `select` runs.
#[derive(Clone)]
pub(crate) struct TieredFixture {
    _dir: std::rc::Rc<TempDir>,
    pub(crate) hot: PathBuf,
    pub(crate) cold: [PathBuf; 2],
    reference: PathBuf,
    at_ms: std::rc::Rc<std::cell::Cell<i64>>,
}

impl TieredFixture {
    pub(crate) fn new(tag: &str) -> Self {
        use crate::reader::ParquetRawStore;
        use crate::writer::RotationPolicy;
        use mie_ports::raw::RawRecordSink;

        let dir = TempDir::new(tag);
        let hot = dir.path().join("hot");
        let cold = [dir.path().join("cold-a"), dir.path().join("cold-b")];
        let reference = dir.path().join("reference");
        let policy = RotationPolicy {
            max_rows: 2,
            max_event_span_ms: crate::layout::DAY_MS,
            ..RotationPolicy::default()
        };
        let mut writer = ParquetRawStore::new(&hot)
            .writer("binance-um", policy)
            .expect("open the fixture writer");
        let mut i = 0;
        for day in 0..FIXTURE_DATES {
            for k in 0..FIXTURE_RECORDS_PER_DATE {
                let time = FIXTURE_D0 + day * crate::layout::DAY_MS + k * 3 * 3_600_000;
                for key in Self::streams() {
                    writer.append(&key, record(time, i)).expect("append");
                    i += 1;
                }
            }
        }
        writer.close().expect("seal the fixture");
        copy_tree(&hot, &reference);
        crate::root_marker::create_meta(&hot, crate::root_marker::HOT_ID, true).expect("init hot");
        for (root, id) in cold.iter().zip(["cold-a", "cold-b"]) {
            std::fs::create_dir_all(root).expect("create a cold root");
            crate::root_marker::create_meta(root, id, false).expect("init a cold root");
        }
        Self {
            _dir: std::rc::Rc::new(dir),
            hot,
            cold,
            reference,
            at_ms: std::rc::Rc::new(std::cell::Cell::new(FIXTURE_D0)),
        }
    }

    /// `aggTrade` and `markPrice`.
    pub(crate) fn streams() -> [RawStreamKey; 2] {
        [stream("aggTrade"), stream("markPrice")]
    }

    /// The date of day `day` of the fixture.
    pub(crate) fn date(day: i64) -> String {
        crate::layout::civil_date(FIXTURE_D0 + day * crate::layout::DAY_MS).expect("a date")
    }

    /// The partition directory of `key` on day `day`.
    pub(crate) fn partition(key: &RawStreamKey, day: i64) -> String {
        crate::layout::partition_dir(key, &Self::date(day))
    }

    /// The tiered store.
    pub(crate) fn store(&self) -> crate::reader::ParquetRawStore {
        crate::reader::ParquetRawStore::tiered(&self.hot, self.cold.to_vec())
    }

    /// The single-root reference store.
    pub(crate) fn reference(&self) -> crate::reader::ParquetRawStore {
        crate::reader::ParquetRawStore::new(&self.reference)
    }

    /// Appends one record to hot's copy ledger.
    pub(crate) fn append(&self, kind: crate::copy_ledger::Kind) {
        let at_ms = self.at_ms.get() + 1;
        self.at_ms.set(at_ms);
        crate::copy_ledger::CopyLedger::at(&self.hot)
            .append(&crate::copy_ledger::Record { at_ms, kind })
            .expect("append to the fixture ledger");
    }

    /// The data paths whose manifests `root` holds in `partition`, sorted.
    pub(crate) fn data_paths(root: &Path, partition: &str) -> Vec<String> {
        use crate::layout::PartFile;
        crate::sorted_entries(&crate::layout::resolve(root, partition))
            .expect("list a partition")
            .iter()
            .filter_map(|name| match PartFile::classify(name) {
                Some((PartFile::Manifest, part)) => {
                    Some(format!("{partition}/{}", PartFile::Data.name(part)))
                }
                _ => None,
            })
            .collect()
    }

    /// The manifest path of a data path.
    pub(crate) fn manifest_of(data_path: &str) -> String {
        data_path
            .strip_suffix(".parquet")
            .map(|base| format!("{base}.manifest"))
            .expect("a data path")
    }

    /// Copies every file of hot's `partition` to both cold roots and
    /// records it: one `manifest` record per file, then one `copy` record
    /// per root (D5, the copy protocol).
    pub(crate) fn copy(&self, partition: &str) {
        for path in Self::data_paths(&self.hot, partition) {
            let manifest = Self::manifest_of(&path);
            for root in &self.cold {
                for file in [&path, &manifest] {
                    copy_file(&self.hot, root, file);
                }
            }
            let text = std::fs::read_to_string(crate::layout::resolve(&self.hot, &manifest))
                .expect("read a manifest");
            self.append(crate::copy_ledger::Kind::Manifest { text });
            for root in ["cold-a", "cold-b"] {
                self.append(crate::copy_ledger::Kind::Copy {
                    path: path.clone(),
                    root: root.to_owned(),
                });
            }
        }
    }

    /// Removes `partition` from hot in D6's order: the `removed` record,
    /// then the rename to a hidden name, then the deletion.
    pub(crate) fn remove(&self, partition: &str) {
        self.append(crate::copy_ledger::Kind::Removed {
            partition: partition.to_owned(),
        });
        let dir = crate::layout::resolve(&self.hot, partition);
        let (parent, name) = partition.rsplit_once('/').expect("a partition");
        let hidden = crate::layout::resolve(&self.hot, parent).join(format!(".removed-{name}"));
        std::fs::rename(&dir, &hidden).expect("rename the partition");
        std::fs::remove_dir_all(&hidden).expect("delete the partition");
    }

    /// Copies the first `n` files of a removed `partition` back to hot from
    /// cold-a and appends `returned`: a recall stopped part-way.
    pub(crate) fn return_some(&self, partition: &str, n: usize) {
        for path in Self::data_paths(&self.cold[0], partition).iter().take(n) {
            copy_file(&self.cold[0], &self.hot, path);
            copy_file(&self.cold[0], &self.hot, &Self::manifest_of(path));
        }
        self.append(crate::copy_ledger::Kind::Returned {
            partition: partition.to_owned(),
        });
    }

    /// Moves every copy of `path` (data and manifest), on hot and both cold
    /// roots, to `<root>/.quarantine/<id>/<path>`.
    pub(crate) fn quarantine(&self, path: &str, id: &str) {
        for root in std::iter::once(&self.hot).chain(&self.cold) {
            for file in [path.to_owned(), Self::manifest_of(path)] {
                let from = crate::layout::resolve(root, &file);
                if !from.exists() {
                    continue;
                }
                let to = crate::layout::resolve(&root.join(".quarantine").join(id), &file);
                std::fs::create_dir_all(to.parent().expect("a parent")).expect("create");
                std::fs::rename(&from, &to).expect("quarantine a copy");
            }
        }
    }
}

/// Copies the relative `file` from `from` to `to`, creating directories.
fn copy_file(from: &Path, to: &Path, file: &str) {
    let target = crate::layout::resolve(to, file);
    std::fs::create_dir_all(target.parent().expect("a parent")).expect("create");
    std::fs::copy(crate::layout::resolve(from, file), &target).expect("copy a file");
}

/// Copies the directory tree `from` to `to`.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create");
    for entry in std::fs::read_dir(from).expect("list") {
        let entry = entry.expect("list");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy");
        }
    }
}
