//! The append-only writer: open parts, rotation and the seal protocol
//! (ADR-030).

use crate::layout::{self, PartFile};
use crate::manifest;
use crate::recovery::{self, RecoveryReport};
use crate::schema;
use crate::{io_error, sorted_entries, sync_dir};
use mie_domain::time::EventTime;
use mie_ports::raw::{
    RawRecord, RawRecordSink, RawStoreError, RawStreamKey, SealedFile, validate_segment,
};
use parquet::arrow::ArrowWriter;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Records buffered before they are handed to the Parquet encoder as one
/// batch.
const BATCH_RECORDS: usize = 8_192;

/// When an open part is sealed (ADR-030 D4).
///
/// A part is sealed before an append that would take it past `max_rows`
/// records, `max_payload_bytes` of payload or an event-time span of
/// `max_event_span_ms`. A date's open part is sealed once a record of the
/// same stream arrives `date_grace_ms` or more after the end of that date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RotationPolicy {
    /// Most records per part. At least 1.
    pub max_rows: u64,
    /// Most payload bytes per part; a single larger record still gets a part
    /// of its own. At least 1.
    pub max_payload_bytes: u64,
    /// Largest `max - min` event time within a part, in milliseconds.
    pub max_event_span_ms: i64,
    /// How long after midnight UTC a date's part stays open for stragglers,
    /// in event-time milliseconds.
    pub date_grace_ms: i64,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self {
            max_rows: 1_000_000,
            max_payload_bytes: 128 * 1024 * 1024,
            max_event_span_ms: 3_600_000,
            date_grace_ms: 60_000,
        }
    }
}

/// The steps of the seal protocol, in order (ADR-030). Tests stop a seal
/// after any of them to simulate a crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SealStep {
    /// The Parquet footer is written.
    CloseWriter,
    /// The data temp file is fsynced.
    SyncData,
    /// The manifest temp file is written and fsynced, and so is the
    /// directory.
    WriteManifest,
    /// Both temp files are read-only.
    MarkReadOnly,
    /// The data file has its final name.
    RenameData,
    /// The manifest has its final name: the part is sealed.
    RenameManifest,
    /// The directory is fsynced.
    SyncDir,
}

#[cfg(test)]
impl SealStep {
    /// Every step, in protocol order.
    pub(crate) const ALL: [Self; 7] = [
        Self::CloseWriter,
        Self::SyncData,
        Self::WriteManifest,
        Self::MarkReadOnly,
        Self::RenameData,
        Self::RenameManifest,
        Self::SyncDir,
    ];
}

/// Hashes and counts every byte on its way to the file, so sealing needs no
/// second pass over the data.
struct HashingWriter {
    file: File,
    hasher: Sha256,
    bytes: u64,
}

impl Write for HashingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// A part being written.
struct OpenPart {
    part: u32,
    /// First millisecond after the part's date.
    date_end_ms: i64,
    writer: ArrowWriter<HashingWriter>,
    buffer: Vec<RawRecord>,
    rows: u64,
    payload_bytes: u64,
    min_ms: i64,
    max_ms: i64,
}

/// The raw-store writer of one source (ADR-030).
///
/// Holds the source's lock for its lifetime. Records become visible only
/// when their part is sealed: on rotation, on [`RawRecordSink::seal_all`] or
/// on [`RawWriter::close`]. Dropping the writer seals nothing; the unsealed
/// parts are discarded by the next writer's recovery. After any I/O failure
/// the writer is poisoned and every later call fails.
pub struct RawWriter {
    root: PathBuf,
    source: String,
    policy: RotationPolicy,
    lock: File,
    open: BTreeMap<(RawStreamKey, String), OpenPart>,
    sealed: Vec<SealedFile>,
    poisoned: bool,
    recovery: RecoveryReport,
    #[cfg(test)]
    pub(crate) crash_after: Option<SealStep>,
}

impl std::fmt::Debug for RawWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawWriter")
            .field("root", &self.root)
            .field("source", &self.source)
            .field("policy", &self.policy)
            .field("open_parts", &self.open.len())
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl RawWriter {
    /// Creates the source directory, takes the source lock and runs
    /// recovery.
    pub(crate) fn open(
        root: &Path,
        source: &str,
        policy: RotationPolicy,
    ) -> Result<Self, RawStoreError> {
        validate_segment(source)?;
        if policy.max_rows == 0
            || policy.max_payload_bytes == 0
            || policy.max_event_span_ms < 0
            || policy.date_grace_ms < 0
        {
            return Err(RawStoreError::Invalid(format!(
                "rotation policy {policy:?}: limits must be positive and spans non-negative"
            )));
        }
        let source_dir = root.join(layout::source_dir(source));
        fs::create_dir_all(&source_dir).map_err(io_error("create", &source_dir))?;
        sync_dir(root)?;
        let lock_path = source_dir.join(".lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(io_error("open", &lock_path))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(RawStoreError::Locked(format!(
                    "another writer holds source {source} in {}",
                    root.display()
                )));
            }
            Err(TryLockError::Error(error)) => return Err(io_error("lock", &lock_path)(error)),
        }
        let recovery = recovery::recover(root, source)?;
        Ok(Self {
            root: root.to_path_buf(),
            source: source.to_owned(),
            policy,
            lock,
            open: BTreeMap::new(),
            sealed: Vec::new(),
            poisoned: false,
            recovery,
            #[cfg(test)]
            crash_after: None,
        })
    }

    /// What recovery did when this writer opened.
    pub fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// Seals every open part and releases the source lock. Returns the files
    /// sealed since the previous [`RawRecordSink::seal_all`].
    ///
    /// # Errors
    ///
    /// [`RawStoreError`] when sealing fails; the lock is released with the
    /// dropped writer either way.
    pub fn close(mut self) -> Result<Vec<SealedFile>, RawStoreError> {
        let sealed = self.seal_all()?;
        self.lock
            .unlock()
            .map_err(|e| RawStoreError::Io(format!("unlock source {}: {e}", self.source)))?;
        Ok(sealed)
    }

    fn check_poisoned(&self) -> Result<(), RawStoreError> {
        if self.poisoned {
            Err(RawStoreError::Io(
                "the writer failed earlier and accepts no more calls".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    /// Poisons the writer when a storage step failed.
    fn poison_on_error<T>(&mut self, result: Result<T, RawStoreError>) -> Result<T, RawStoreError> {
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn store(
        &mut self,
        stream: &RawStreamKey,
        date: String,
        record: RawRecord,
    ) -> Result<(), RawStoreError> {
        let time = record.event_time.as_millis();

        // Date grace: older dates of the same stream that this record has
        // left behind by the grace period are complete.
        let expired: Vec<_> = self
            .open
            .range((stream.clone(), String::new())..)
            .take_while(|((key, _), _)| key == stream)
            .filter(|((_, part_date), part)| {
                *part_date != date
                    && time >= part.date_end_ms.saturating_add(self.policy.date_grace_ms)
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            self.seal(&key)?;
        }

        let key = (stream.clone(), date);
        if let Some(part) = self.open.get(&key)
            && self.must_rotate(part, &record)
        {
            self.seal(&key)?;
        }
        if !self.open.contains_key(&key) {
            let part = self.open_part(&key.0, &key.1, time)?;
            self.open.insert(key.clone(), part);
        }
        let part = self.open.get_mut(&key).expect("the part was just opened");
        part.rows += 1;
        part.payload_bytes += record.payload.len() as u64;
        part.min_ms = part.min_ms.min(time);
        part.max_ms = part.max_ms.max(time);
        part.buffer.push(record);
        if part.buffer.len() >= BATCH_RECORDS {
            write_buffer(part)?;
        }
        Ok(())
    }

    /// Whether `part` must be sealed before `record` is appended to it.
    fn must_rotate(&self, part: &OpenPart, record: &RawRecord) -> bool {
        let time = record.event_time.as_millis();
        let span = part.max_ms.max(time) - part.min_ms.min(time);
        part.rows >= self.policy.max_rows
            || part.payload_bytes + record.payload.len() as u64 > self.policy.max_payload_bytes
            || span > self.policy.max_event_span_ms
    }

    /// Opens the next part of a partition: number = highest existing + 1.
    fn open_part(
        &self,
        stream: &RawStreamKey,
        date: &str,
        first_ms: i64,
    ) -> Result<OpenPart, RawStoreError> {
        let partition = layout::resolve(&self.root, &layout::partition_dir(stream, date));
        fs::create_dir_all(&partition).map_err(io_error("create", &partition))?;
        // Make the new directory entries durable up to the source directory.
        let mut dir = partition.as_path();
        for _ in 0..3 {
            dir = dir
                .parent()
                .expect("a partition is four levels below the root");
            sync_dir(dir)?;
        }
        let part = sorted_entries(&partition)?
            .iter()
            .filter_map(|name| PartFile::classify(name))
            .map(|(_, part)| part + 1)
            .max()
            .unwrap_or(0);
        if part > layout::MAX_PART {
            return Err(RawStoreError::Invalid(format!(
                "partition {} has no free part number",
                partition.display()
            )));
        }
        let path = partition.join(PartFile::DataTmp.name(part));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(io_error("create", &path))?;
        let sink = HashingWriter {
            file,
            hasher: Sha256::new(),
            bytes: 0,
        };
        let writer = ArrowWriter::try_new(
            sink,
            schema::schema(),
            Some(schema::writer_properties(stream)),
        )
        .map_err(|e| RawStoreError::Io(format!("start {}: {e}", path.display())))?;
        Ok(OpenPart {
            part,
            date_end_ms: layout::next_day_start_ms(first_ms),
            writer,
            buffer: Vec::new(),
            rows: 0,
            payload_bytes: 0,
            min_ms: first_ms,
            max_ms: first_ms,
        })
    }

    /// Runs the seal protocol on one open part (ADR-030).
    fn seal(&mut self, key: &(RawStreamKey, String)) -> Result<(), RawStoreError> {
        let (stream, date) = key;
        let mut part = self.open.remove(key).expect("only open parts are sealed");
        let partition = layout::resolve(&self.root, &layout::partition_dir(stream, date));
        let data_tmp = partition.join(PartFile::DataTmp.name(part.part));
        let manifest_tmp = partition.join(PartFile::ManifestTmp.name(part.part));
        let data_final = partition.join(PartFile::Data.name(part.part));
        let manifest_final = partition.join(PartFile::Manifest.name(part.part));

        write_buffer(&mut part)?;
        let sink = part
            .writer
            .into_inner()
            .map_err(|e| RawStoreError::Io(format!("finish {}: {e}", data_tmp.display())))?;
        self.reached(SealStep::CloseWriter)?;

        sink.file.sync_all().map_err(io_error("sync", &data_tmp))?;
        self.reached(SealStep::SyncData)?;

        let sealed = SealedFile {
            relative_path: layout::file_path(stream, date, PartFile::Data, part.part),
            stream: stream.clone(),
            date: date.clone(),
            part: part.part,
            rows: part.rows,
            bytes: sink.bytes,
            min_event_time: EventTime::from_millis(part.min_ms),
            max_event_time: EventTime::from_millis(part.max_ms),
            sha256: manifest::hex(&sink.hasher.finalize()),
        };
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&manifest_tmp)
            .map_err(io_error("create", &manifest_tmp))?;
        file.write_all(manifest::render(&sealed).as_bytes())
            .and_then(|()| file.sync_all())
            .map_err(io_error("write", &manifest_tmp))?;
        sync_dir(&partition)?;
        self.reached(SealStep::WriteManifest)?;

        recovery::mark_read_only(&data_tmp)?;
        recovery::mark_read_only(&manifest_tmp)?;
        self.reached(SealStep::MarkReadOnly)?;

        recovery::rename_no_replace(&data_tmp, &data_final)?;
        self.reached(SealStep::RenameData)?;

        recovery::rename_no_replace(&manifest_tmp, &manifest_final)?;
        self.reached(SealStep::RenameManifest)?;

        sync_dir(&partition)?;
        self.reached(SealStep::SyncDir)?;

        self.sealed.push(sealed);
        Ok(())
    }

    /// Marks the end of a seal step; under test, simulates a crash there.
    fn reached(&self, step: SealStep) -> Result<(), RawStoreError> {
        #[cfg(test)]
        if self.crash_after == Some(step) {
            return Err(RawStoreError::Io(format!("simulated crash after {step:?}")));
        }
        let _ = step;
        Ok(())
    }
}

/// Hands the buffered records to the Parquet encoder.
fn write_buffer(part: &mut OpenPart) -> Result<(), RawStoreError> {
    if part.buffer.is_empty() {
        return Ok(());
    }
    part.writer
        .write(&schema::to_batch(&part.buffer))
        .map_err(|e| RawStoreError::Io(format!("encode part {}: {e}", part.part)))?;
    part.buffer.clear();
    Ok(())
}

impl RawRecordSink for RawWriter {
    fn append(&mut self, stream: &RawStreamKey, record: RawRecord) -> Result<(), RawStoreError> {
        self.check_poisoned()?;
        if stream.source() != self.source {
            return Err(RawStoreError::Invalid(format!(
                "stream {stream} does not belong to this writer's source {}",
                self.source
            )));
        }
        let date = layout::civil_date(record.event_time.as_millis())?;
        let result = self.store(stream, date, record);
        self.poison_on_error(result)
    }

    fn seal_all(&mut self) -> Result<Vec<SealedFile>, RawStoreError> {
        self.check_poisoned()?;
        let keys: Vec<_> = self.open.keys().cloned().collect();
        for key in keys {
            let result = self.seal(&key);
            self.poison_on_error(result)?;
        }
        Ok(std::mem::take(&mut self.sealed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::ParquetRawStore;
    use crate::testutil::{TempDir, record, stream};
    use mie_ports::outbound::ReplayWindow;
    use mie_ports::raw::{RawRecordSource, RawSelection};
    use std::collections::BTreeSet;

    const DAY: i64 = layout::DAY_MS;
    /// 2026-10-06T00:00:00Z.
    const D0: i64 = 1_791_244_800_000;

    fn everything(store: &ParquetRawStore, streams: &[&RawStreamKey]) -> Vec<SealedFile> {
        let selection = RawSelection::new(
            streams
                .iter()
                .map(|s| (*s).clone())
                .collect::<BTreeSet<_>>(),
            ReplayWindow {
                start: EventTime::from_millis(0),
                end: EventTime::from_millis(layout::MAX_EVENT_TIME_MS),
            },
        )
        .unwrap();
        store.select(&selection).unwrap().files
    }

    fn read_all(store: &ParquetRawStore, files: &[SealedFile]) -> Vec<RawRecord> {
        files
            .iter()
            .flat_map(|file| store.read(file).unwrap())
            .collect()
    }

    /// Every file and directory below `root`, with file contents, sorted.
    fn snapshot(root: &Path) -> Vec<(String, Option<Vec<u8>>)> {
        fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, Option<Vec<u8>>)>) {
            for name in sorted_entries(dir).unwrap() {
                let path = dir.join(&name);
                let rel = format!("{prefix}{name}");
                if path.is_dir() {
                    out.push((rel.clone(), None));
                    walk(&path, &format!("{rel}/"), out);
                } else {
                    out.push((rel, Some(fs::read(&path).unwrap())));
                }
            }
        }
        let mut out = Vec::new();
        walk(root, "", &mut out);
        out
    }

    fn records(count: i64, start_ms: i64) -> Vec<RawRecord> {
        (0..count).map(|i| record(start_ms + i, i)).collect()
    }

    #[test]
    fn a_crash_at_every_seal_step_recovers_deterministically() {
        let key = stream("aggTrade");
        let input = records(10, D0);
        for step in SealStep::ALL {
            let dir = TempDir::new("crash-step");
            let store = ParquetRawStore::new(dir.path());
            let mut writer = store
                .writer("binance-um", RotationPolicy::default())
                .unwrap();
            writer.crash_after = Some(step);
            for r in &input {
                writer.append(&key, r.clone()).unwrap();
            }
            assert!(
                matches!(writer.seal_all(), Err(RawStoreError::Io(_))),
                "{step:?}"
            );
            // Poisoned: the crashed writer accepts nothing more.
            assert!(writer.append(&key, input[0].clone()).is_err());
            drop(writer);

            let writer = store
                .writer("binance-um", RotationPolicy::default())
                .unwrap();
            let report = writer.recovery().clone();
            let data_rel = layout::file_path(&key, "2026-10-06", PartFile::Data, 0);
            let tmp_rel = layout::file_path(&key, "2026-10-06", PartFile::DataTmp, 0);
            match step {
                SealStep::CloseWriter | SealStep::SyncData => {
                    assert!(report.rolled_forward.is_empty(), "{step:?}");
                    assert_eq!(report.discarded.len(), 1, "{step:?}");
                    assert_eq!(report.discarded[0].0, tmp_rel);
                    assert!(report.discarded[0].1 > 0);
                    assert!(everything(&store, &[&key]).is_empty());
                }
                SealStep::WriteManifest | SealStep::MarkReadOnly | SealStep::RenameData => {
                    assert_eq!(report.rolled_forward, vec![data_rel.clone()], "{step:?}");
                    assert!(report.discarded.is_empty());
                    let files = everything(&store, &[&key]);
                    assert_eq!(read_all(&store, &files), input, "{step:?}");
                }
                SealStep::RenameManifest | SealStep::SyncDir => {
                    assert!(report.is_clean(), "{step:?}: {report:?}");
                    let files = everything(&store, &[&key]);
                    assert_eq!(read_all(&store, &files), input, "{step:?}");
                }
            }
            for file in everything(&store, &[&key]) {
                let data = layout::resolve(dir.path(), &file.relative_path);
                assert!(fs::metadata(&data).unwrap().permissions().readonly());
            }
            // Recovery is idempotent: a second pass changes nothing.
            let before = snapshot(dir.path());
            drop(writer);
            let again = store
                .writer("binance-um", RotationPolicy::default())
                .unwrap();
            assert!(again.recovery().is_clean(), "{step:?}");
            assert_eq!(snapshot(dir.path()), before, "{step:?}");
        }
    }

    #[test]
    fn a_crash_mid_part_discards_the_part() {
        let dir = TempDir::new("crash-mid-part");
        let store = ParquetRawStore::new(dir.path());
        let key = stream("aggTrade");
        let mut writer = store
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        for r in records(BATCH_RECORDS as i64 + 5, D0) {
            writer.append(&key, r).unwrap();
        }
        // Dropping is a kill without footer: nothing is sealed.
        drop(writer);
        let partition = layout::resolve(dir.path(), &layout::partition_dir(&key, "2026-10-06"));
        assert_eq!(
            sorted_entries(&partition).unwrap(),
            vec![PartFile::DataTmp.name(0)]
        );

        let writer = store
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        assert!(writer.recovery().rolled_forward.is_empty());
        assert_eq!(writer.recovery().discarded.len(), 1);
        assert!(sorted_entries(&partition).unwrap().is_empty());
        assert!(everything(&store, &[&key]).is_empty());
    }

    fn part_sizes(policy: RotationPolicy, input: &[RawRecord]) -> Vec<u64> {
        let dir = TempDir::new("rotation");
        let store = ParquetRawStore::new(dir.path());
        let key = stream("aggTrade");
        let mut writer = store.writer("binance-um", policy).unwrap();
        for r in input {
            writer.append(&key, r.clone()).unwrap();
        }
        let sealed = writer.close().unwrap();
        let files = everything(&store, &[&key]);
        assert_eq!(sealed, files);
        assert_eq!(read_all(&store, &files), input);
        files.iter().map(|f| f.rows).collect()
    }

    #[test]
    fn rotation_seals_at_exact_boundaries() {
        let rows = RotationPolicy {
            max_rows: 3,
            ..RotationPolicy::default()
        };
        assert_eq!(part_sizes(rows, &records(3, D0)), vec![3]);
        assert_eq!(part_sizes(rows, &records(4, D0)), vec![3, 1]);
        assert_eq!(part_sizes(rows, &records(7, D0)), vec![3, 3, 1]);

        let bytes = RotationPolicy {
            max_payload_bytes: 10,
            ..RotationPolicy::default()
        };
        let sized = |n: i64| -> Vec<RawRecord> {
            (0..n)
                .map(|i| RawRecord {
                    payload: vec![b'p'; 5],
                    ..record(D0 + i, i)
                })
                .collect()
        };
        assert_eq!(part_sizes(bytes, &sized(2)), vec![2]);
        assert_eq!(part_sizes(bytes, &sized(3)), vec![2, 1]);
        // One record larger than the limit still gets a part of its own.
        let big = RawRecord {
            payload: vec![b'p'; 11],
            ..record(D0, 0)
        };
        assert_eq!(part_sizes(bytes, &[big]), vec![1]);

        let span = RotationPolicy {
            max_event_span_ms: 100,
            ..RotationPolicy::default()
        };
        assert_eq!(
            part_sizes(span, &[record(D0, 0), record(D0 + 100, 1)]),
            vec![2]
        );
        assert_eq!(
            part_sizes(
                span,
                &[record(D0, 0), record(D0 + 100, 1), record(D0 + 101, 2)]
            ),
            vec![2, 1]
        );
        // The span counts backwards too: an older record can exceed it.
        assert_eq!(
            part_sizes(
                span,
                &[record(D0 + 200, 0), record(D0 + 100, 1), record(D0 + 99, 2)]
            ),
            vec![2, 1]
        );
    }

    #[test]
    fn date_grace_keeps_stragglers_in_their_date() {
        let dir = TempDir::new("date-grace");
        let store = ParquetRawStore::new(dir.path());
        let trades = stream("aggTrade");
        let depth = stream("depth");
        let mut writer = store
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        let day0 = |n| layout::file_path(&trades, "2026-10-06", PartFile::Manifest, n);
        let sealed = |rel: &str| layout::resolve(dir.path(), rel).exists();

        writer.append(&trades, record(D0 + DAY - 10, 0)).unwrap();
        writer
            .append(&trades, record(D0 + DAY + 59_999, 1))
            .unwrap();
        // Within the grace: the straggler joins its own date's open part.
        writer.append(&trades, record(D0 + DAY - 5, 2)).unwrap();
        // Another stream passing the grace does not seal this stream's date.
        writer
            .append(&depth, record(D0 + DAY + 120_000, 3))
            .unwrap();
        assert!(!sealed(&day0(0)));
        // The same stream reaching the grace seals the old date.
        writer
            .append(&trades, record(D0 + DAY + 60_000, 4))
            .unwrap();
        assert!(sealed(&day0(0)));
        // A later straggler opens a new part of the old date.
        writer.append(&trades, record(D0 + DAY - 1, 5)).unwrap();
        writer.close().unwrap();
        assert!(sealed(&day0(1)));

        let files = everything(&store, &[&trades]);
        let shape: Vec<_> = files
            .iter()
            .map(|f| (f.date.as_str(), f.part, f.rows))
            .collect();
        assert_eq!(
            shape,
            vec![
                ("2026-10-06", 0, 2),
                ("2026-10-06", 1, 1),
                ("2026-10-07", 0, 2)
            ]
        );
        let payloads: Vec<_> = read_all(&store, &files)
            .into_iter()
            .map(|r| r.payload)
            .collect();
        assert_eq!(
            payloads,
            ["0", "2", "5", "1", "4"].map(|p| p.as_bytes().to_vec())
        );
    }

    #[test]
    fn empty_parts_are_never_written() {
        let dir = TempDir::new("empty");
        let store = ParquetRawStore::new(dir.path());
        let mut writer = store
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        assert_eq!(writer.seal_all().unwrap(), vec![]);
        assert_eq!(writer.close().unwrap(), vec![]);
        let source = dir.path().join("source=binance-um");
        assert_eq!(sorted_entries(&source).unwrap(), vec![".lock".to_owned()]);
    }

    #[test]
    fn part_numbers_continue_after_reopen() {
        let dir = TempDir::new("numbering");
        let store = ParquetRawStore::new(dir.path());
        let key = stream("aggTrade");
        for i in 0..3 {
            let mut writer = store
                .writer("binance-um", RotationPolicy::default())
                .unwrap();
            writer.append(&key, record(D0 + i, i)).unwrap();
            let sealed = writer.close().unwrap();
            assert_eq!(sealed.len(), 1);
            assert_eq!(sealed[0].part, u32::try_from(i).unwrap());
        }
        let files = everything(&store, &[&key]);
        assert_eq!(
            files.iter().map(|f| f.part).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(read_all(&store, &files), records(3, D0));
    }

    #[test]
    fn sealing_never_overwrites_an_existing_file() {
        let dir = TempDir::new("no-overwrite");
        let store = ParquetRawStore::new(dir.path());
        let key = stream("aggTrade");
        let mut writer = store
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        writer.append(&key, record(D0, 0)).unwrap();
        let squatter = layout::resolve(
            dir.path(),
            &layout::file_path(&key, "2026-10-06", PartFile::Data, 0),
        );
        fs::write(&squatter, b"not ours").unwrap();
        assert!(matches!(
            writer.seal_all(),
            Err(RawStoreError::Integrity(_))
        ));
        assert_eq!(fs::read(&squatter).unwrap(), b"not ours");
        // A failed seal poisons the writer.
        assert!(matches!(
            writer.append(&key, record(D0, 1)),
            Err(RawStoreError::Io(_))
        ));
    }

    #[test]
    fn sealed_files_are_read_only() {
        let dir = TempDir::new("read-only");
        let store = ParquetRawStore::new(dir.path());
        let key = stream("aggTrade");
        let mut writer = store
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        writer.append(&key, record(D0, 0)).unwrap();
        writer.close().unwrap();
        for kind in [PartFile::Data, PartFile::Manifest] {
            let path = layout::resolve(dir.path(), &layout::file_path(&key, "2026-10-06", kind, 0));
            assert!(
                fs::metadata(&path).unwrap().permissions().readonly(),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn one_writer_per_source() {
        let dir = TempDir::new("lock");
        let store = ParquetRawStore::new(dir.path());
        let first = store
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        assert!(matches!(
            store.writer("binance-um", RotationPolicy::default()),
            Err(RawStoreError::Locked(_))
        ));
        let other = store
            .writer("binance-archive", RotationPolicy::default())
            .unwrap();
        first.close().unwrap();
        other.close().unwrap();
        store
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
    }

    #[test]
    fn invalid_input_is_rejected_without_poisoning() {
        let dir = TempDir::new("invalid");
        let store = ParquetRawStore::new(dir.path());
        assert!(matches!(
            store.writer("binance/um", RotationPolicy::default()),
            Err(RawStoreError::Invalid(_))
        ));
        let zero = RotationPolicy {
            max_rows: 0,
            ..RotationPolicy::default()
        };
        assert!(matches!(
            store.writer("binance-um", zero),
            Err(RawStoreError::Invalid(_))
        ));

        let mut writer = store
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        let foreign = RawStreamKey::new("bybit", "BTCUSDT", "aggTrade").unwrap();
        assert!(matches!(
            writer.append(&foreign, record(D0, 0)),
            Err(RawStoreError::Invalid(_))
        ));
        let key = stream("aggTrade");
        for bad in [-1, layout::MAX_EVENT_TIME_MS + 1] {
            assert!(matches!(
                writer.append(&key, record(bad, 0)),
                Err(RawStoreError::Invalid(_))
            ));
        }
        writer.append(&key, record(D0, 0)).unwrap();
        assert_eq!(writer.close().unwrap().len(), 1);
    }

    #[test]
    fn identical_input_gives_identical_bytes() {
        let keys = [stream("aggTrade"), stream("depth")];
        let input: Vec<_> = (0..20_000)
            .map(|i| (&keys[(i % 2) as usize], record(D0 + DAY - 10_000 + i, i)))
            .collect();
        let write = |tag: &str| {
            let dir = TempDir::new(tag);
            let store = ParquetRawStore::new(dir.path());
            let policy = RotationPolicy {
                max_rows: 7_000,
                ..RotationPolicy::default()
            };
            let mut writer = store.writer("binance-um", policy).unwrap();
            for (key, r) in &input {
                writer.append(key, r.clone()).unwrap();
            }
            writer.close().unwrap();
            let selection = RawSelection::new(
                keys.iter().cloned().collect(),
                ReplayWindow {
                    start: EventTime::from_millis(D0),
                    end: EventTime::from_millis(D0 + 2 * DAY),
                },
            )
            .unwrap();
            let version = store.select(&selection).unwrap().version;
            let mut tree = snapshot(dir.path());
            tree.retain(|(name, _)| !name.ends_with(".lock"));
            (tree, version)
        };
        let (a, version_a) = write("determinism-a");
        let (b, version_b) = write("determinism-b");
        assert!(a.iter().filter(|(n, _)| n.ends_with(".parquet")).count() >= 4);
        assert_eq!(a, b);
        assert_eq!(version_a, version_b);
    }
}
