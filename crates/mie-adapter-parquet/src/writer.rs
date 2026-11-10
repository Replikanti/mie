//! The append-only writer: open parts, rotation and the seal protocol
//! (ADR-030).

use crate::copy_ledger::{self, CopyLedger, Kind, Record, Truncation};
use crate::layout::{self, PartFile};
use crate::manifest;
use crate::recovery::{self, RecoveryReport};
use crate::removal::{self, RemovalPlan, RemovalReport};
use crate::replica;
use crate::schema;
use crate::tiers::{LedgerView, TierState, Verdict};
use crate::{io_error, sorted_entries, sync_dir};
use mie_domain::time::EventTime;
use mie_ports::raw::{
    RawRecord, RawRecordSink, RawStoreError, RawStreamKey, SealedFile, validate_segment,
};
use parquet::arrow::ArrowWriter;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
/// second pass over the data. The outcome dataset writer seals its tables
/// with it too.
pub(crate) struct HashingWriter {
    pub(crate) file: File,
    pub(crate) hasher: Sha256,
    pub(crate) bytes: u64,
}

impl HashingWriter {
    /// A writer at the start of `file`.
    pub(crate) fn new(file: File) -> Self {
        Self {
            file,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }
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

/// An observer of a recall's steps ([`TierContext::on_recall`]).
pub type RecallObserver = Arc<dyn Fn(&RecallEvent) + Send + Sync>;

/// What a tiered store's writer needs beyond the store (ADR-062 D6).
#[derive(Clone)]
pub struct TierContext {
    /// The first date of the hot window when the writer opens (D8). The
    /// degraded writer opens only dates on or after it.
    pub first_hot_date: String,
    /// The wall clock, UTC epoch milliseconds, for the `returned` records
    /// of a recall.
    pub now_ms: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// Sees every step of a recall, synchronously, before the next one.
    pub on_recall: Option<RecallObserver>,
}

impl std::fmt::Debug for TierContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TierContext")
            .field("first_hot_date", &self.first_hot_date)
            .finish_non_exhaustive()
    }
}

/// One step of a recall (ADR-062 D6), as [`TierContext::on_recall`] sees
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecallEvent {
    /// One sealed file is back on hot.
    Copied {
        /// The partition directory.
        partition: String,
        /// The data path.
        path: String,
        /// The cold root it was copied from.
        root: PathBuf,
    },
    /// A damaged cold copy was passed over; the next root was tried (#191
    /// STOP 1 decision 1).
    Damaged {
        /// The partition directory.
        partition: String,
        /// The data path.
        path: String,
        /// The cold root that holds the damaged copy.
        root: PathBuf,
        /// What is wrong with it.
        error: String,
    },
    /// The `returned` append truncated the copy ledger's incomplete
    /// trailing record (D7: every truncation is journaled).
    LedgerTruncated(Truncation),
    /// The `returned` record is appended: the partition is on hot again.
    Returned {
        /// The partition directory.
        partition: String,
        /// The files copied back (or found on hot from a crashed recall).
        files: usize,
    },
}

/// The tier side of a writer.
struct Tiered {
    cold_roots: Vec<PathBuf>,
    ctx: TierContext,
    /// The ledger snapshot; `None` is the degraded writer (no readable
    /// ledger, D6 last bullet).
    state: Option<TierState>,
}

/// The raw-store writer of one source (ADR-030).
///
/// Holds the source's lock for its lifetime. Records become visible only
/// when their part is sealed: on rotation, on [`RawRecordSink::seal_all`] or
/// on [`RawWriter::close`]. Dropping the writer seals nothing; the unsealed
/// parts are discarded by the next writer's recovery. After any I/O failure
/// the writer is poisoned and every later call fails.
///
/// On a tiered store (ADR-062 D6) the writer reads the copy ledger once
/// when it opens and keeps that snapshot current with its own seals,
/// recalls and removals:
///
/// - **Union part numbering.** A new part takes the highest part number
///   hot or the ledger knows for the partition, lost, held and quarantined
///   entries included, plus one, so a path is never reused.
/// - **The recall.** An append to a partition the ledger records as
///   removed from hot first copies every sealed file of it back from a
///   cold copy by the D5 copy ([`replica::copy_back`]), verified, then
///   appends `returned`; only then is the part opened. Any failure poisons
///   the writer and appends nothing.
/// - **The degraded writer.** Without a readable ledger (missing, or
///   failing its chain) the writer opens only dates on or after the first
///   hot date it was opened with, refusing an earlier one with
///   [`RawStoreError::Integrity`] before anything is created.
///
/// [`RawWriter::remove_partitions`] is the hot removal of the writer's own
/// source, under the lock it holds.
pub struct RawWriter {
    root: PathBuf,
    source: String,
    policy: RotationPolicy,
    lock: File,
    /// The tier side of a tiered store.
    tiers: Option<Tiered>,
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
    /// recovery. With `tiers` (the cold roots in read order and the
    /// context), it then reads the copy ledger once (the type docs); a
    /// single-root store never reads it.
    pub(crate) fn open(
        root: &Path,
        source: &str,
        policy: RotationPolicy,
        tiers: Option<(Vec<PathBuf>, TierContext)>,
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
        let tiers = match tiers {
            Some((cold_roots, ctx)) => {
                let state = match copy_ledger::read(root) {
                    // Once the ledger parsed, its fold cannot fail; if it
                    // does, the writer fails closed.
                    Ok(Some(ledger)) => Some(TierState::fold(&ledger, source)?),
                    // Missing or unreadable: the degraded writer (D6, D7).
                    Ok(None) | Err(_) => None,
                };
                Some(Tiered {
                    cold_roots,
                    ctx,
                    state,
                })
            }
            None => None,
        };
        Ok(Self {
            root: root.to_path_buf(),
            source: source.to_owned(),
            policy,
            lock,
            tiers,
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

    /// Opens the next part of a partition: number = highest existing + 1,
    /// on a tiered store over the union of hot and the ledger snapshot
    /// (ADR-062 D6), after recalling a removed partition. The degraded
    /// writer's refusal and the number are decided before anything is
    /// created, so a refusal leaves the store untouched.
    fn open_part(
        &mut self,
        stream: &RawStreamKey,
        date: &str,
        first_ms: i64,
    ) -> Result<OpenPart, RawStoreError> {
        let relative = layout::partition_dir(stream, date);
        let partition = layout::resolve(&self.root, &relative);
        let mut removed = false;
        if let Some(tiers) = &self.tiers {
            match &tiers.state {
                None if date < tiers.ctx.first_hot_date.as_str() => {
                    return Err(RawStoreError::Integrity(format!(
                        "partition {relative}: the copy ledger {} is missing or unreadable, so \
                         the writer opens only hot-window dates, from {} on (ADR-062 D6, D7); the \
                         copy-ledger rebuild restores it. The writer writes nothing",
                        copy_ledger::ledger_path(&self.root).display(),
                        tiers.ctx.first_hot_date
                    )));
                }
                Some(state) => removed = state.is_removed(&relative),
                None => {}
            }
        }
        if removed {
            self.recall(&relative)?;
        }
        // A missing directory lists as empty.
        let hot_next = sorted_entries(&partition)?
            .iter()
            .filter_map(|name| PartFile::classify(name))
            .map(|(_, part)| part + 1)
            .max()
            .unwrap_or(0);
        let part = match self.tiers.as_ref().and_then(|tiers| tiers.state.as_ref()) {
            Some(state) => state.next_part(&relative, hot_next),
            None => hot_next,
        };
        if part > layout::MAX_PART {
            return Err(RawStoreError::Invalid(format!(
                "partition {} has no free part number",
                partition.display()
            )));
        }
        fs::create_dir_all(&partition).map_err(io_error("create", &partition))?;
        // Make the new directory entries durable up to the source directory.
        let mut dir = partition.as_path();
        for _ in 0..3 {
            dir = dir
                .parent()
                .expect("a partition is four levels below the root");
            sync_dir(dir)?;
        }
        let path = partition.join(PartFile::DataTmp.name(part));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(io_error("create", &path))?;
        let sink = HashingWriter::new(file);
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

        if let Some(state) = self.tiers.as_mut().and_then(|tiers| tiers.state.as_mut()) {
            state.record_part(&layout::partition_dir(stream, date), part.part);
        }
        self.sealed.push(sealed);
        Ok(())
    }

    /// Recalls the removed partition `partition` (relative) to hot, under
    /// the source lock this writer holds (ADR-062 D6, the type docs).
    ///
    /// 1. The copy ledger is read fresh: a recall is rare, so a full read
    ///    is acceptable here. When it no longer records the partition as
    ///    removed, nothing is copied.
    /// 2. Every recorded path of the partition with an ordinary verdict is
    ///    copied back by [`replica::copy_back`]. A held path is refused
    ///    (`mie store resolve`), and so is a conflicting one or one without
    ///    its manifest; lost, replaced and replacement-pending paths are not
    ///    copied, because no root holds them in the layout, but the union
    ///    numbering still counts them.
    /// 3. `returned` is appended at `now_ms`.
    ///
    /// Any failure appends nothing: the partition stays recorded as
    /// removed, and the next recall resumes it (files already on hot are
    /// `AlreadyPresent`) or the remover's completion step removes it again.
    fn recall(&mut self, partition: &str) -> Result<(), RawStoreError> {
        let ledger_path = copy_ledger::ledger_path(&self.root);
        let ledger = copy_ledger::read(&self.root)?.ok_or_else(|| {
            RawStoreError::Integrity(format!(
                "partition {partition}: the copy ledger {} disappeared, so the recall cannot \
                 list the partition (ADR-062 D6)",
                ledger_path.display()
            ))
        })?;
        let tiers = self.tiers.as_mut().expect("only a tiered writer recalls");
        if !crate::tiers::removed_partitions(&ledger).contains(partition) {
            if let Some(state) = tiers.state.as_mut() {
                state.mark_returned(partition);
            }
            return Ok(());
        }
        let view = LedgerView::fold(&ledger)?;
        let emit = |event: RecallEvent| {
            if let Some(on_recall) = &tiers.ctx.on_recall {
                on_recall(&event);
            }
        };
        let mut files = 0;
        for (path, verdict) in view.partition(partition) {
            let text = match verdict {
                Verdict::Ordinary(text) => text,
                Verdict::Lost | Verdict::Replaced | Verdict::Pending => continue,
                Verdict::Held(reason) => {
                    return Err(RawStoreError::Integrity(format!(
                        "partition {partition}: {path} is held ({}), so the recall cannot copy \
                         the partition back; resolve the hold with `mie store resolve` first \
                         (ADR-062 D6, D11)",
                        reason.as_str()
                    )));
                }
                Verdict::Conflict => {
                    return Err(RawStoreError::Integrity(format!(
                        "partition {partition}: the copy ledger holds two different manifest \
                         texts for {path}, so the recall stops (ADR-062 D5)"
                    )));
                }
                Verdict::NoManifest => {
                    return Err(RawStoreError::Integrity(format!(
                        "partition {partition}: {path} is recorded in the copy ledger without \
                         its manifest, so the recall cannot copy it back (ADR-062 D7)"
                    )));
                }
            };
            let copied = replica::copy_back(
                &tiers.cold_roots,
                &self.root,
                &path,
                &text,
                &mut |damaged| {
                    emit(RecallEvent::Damaged {
                        partition: partition.to_owned(),
                        path: path.clone(),
                        root: damaged.root.clone(),
                        error: damaged.error.to_string(),
                    });
                },
            )?;
            files += 1;
            emit(RecallEvent::Copied {
                partition: partition.to_owned(),
                path: path.clone(),
                root: copied.root,
            });
        }
        let record = Record {
            at_ms: (tiers.ctx.now_ms)(),
            kind: Kind::Returned {
                partition: partition.to_owned(),
            },
        };
        let (truncation, result) = match CopyLedger::at(&self.root).append(&record) {
            Ok(outcome) => (outcome.truncation, Ok(())),
            Err(error) => (error.truncation, Err(error.error)),
        };
        if let Some(truncation) = truncation {
            emit(RecallEvent::LedgerTruncated(truncation));
        }
        result?;
        if let Some(state) = tiers.state.as_mut() {
            state.mark_returned(partition);
        }
        emit(RecallEvent::Returned {
            partition: partition.to_owned(),
            files,
        });
        Ok(())
    }

    /// The hot removal of this writer's source by `plan` (ADR-062 D6),
    /// under the source lock the writer holds: the remover of `mie ingest`
    /// runs it in the seal path. A partition with an open part is skipped;
    /// a partition removed is marked in the writer's snapshot, so a later
    /// append to it recalls it. A single-root or degraded writer removes
    /// nothing.
    pub fn remove_partitions(
        &mut self,
        plan: &RemovalPlan,
        first_hot_date: &str,
        at_ms: i64,
    ) -> RemovalReport {
        let degraded = self
            .tiers
            .as_ref()
            .is_none_or(|tiers| tiers.state.is_none());
        if degraded || self.poisoned {
            return RemovalReport {
                source: self.source.clone(),
                ..RemovalReport::default()
            };
        }
        let busy: BTreeSet<String> = self
            .open
            .keys()
            .map(|(stream, date)| layout::partition_dir(stream, date))
            .collect();
        let report =
            removal::remove_locked(&self.root, &self.source, plan, &busy, first_hot_date, at_ms);
        if let Some(state) = self.tiers.as_mut().and_then(|tiers| tiers.state.as_mut()) {
            for partition in &report.removed {
                state.mark_removed(partition);
            }
        }
        report
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
    fn default_policy_is_adr_030_d4() {
        // ADR-030 D4: 1 000 000 rows, 128 MiB of payload, 1 h of event-time
        // span, and a 60 s date grace. Live capture writes with this policy.
        assert_eq!(
            RotationPolicy::default(),
            RotationPolicy {
                max_rows: 1_000_000,
                max_payload_bytes: 134_217_728,
                max_event_span_ms: 3_600_000,
                date_grace_ms: 60_000,
            }
        );
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

    // ------------------------- tiers: numbering, recall, degraded (#191)

    use crate::copy_ledger::{self, HoldReason, Kind, LostOrigin};
    use crate::removal;
    use crate::replica::{self, CopyStep};
    use crate::testutil::{FIXTURE_D0, TieredFixture};
    use std::sync::{Arc, Mutex};

    /// The first hot date of the tests: the fixture's last date, so days 0
    /// to 2 lie before the hot window.
    fn first_hot() -> String {
        TieredFixture::date(3)
    }

    /// A tiered writer of `binance-um` over the fixture, with `on_recall`.
    fn tiered(
        fixture: &TieredFixture,
        on_recall: Option<RecallObserver>,
    ) -> Result<RawWriter, RawStoreError> {
        let mut ctx = TieredFixture::context(&first_hot());
        ctx.on_recall = on_recall;
        fixture
            .store()
            .tiered_writer("binance-um", RotationPolicy::default(), ctx)
    }

    /// Appends one record at `time` to `key` through a fresh tiered writer
    /// of the fixture and closes it.
    fn write_one(
        fixture: &TieredFixture,
        key: &RawStreamKey,
        time: i64,
    ) -> Result<Vec<SealedFile>, RawStoreError> {
        let mut writer = tiered(fixture, None)?;
        writer.append(key, record(time, 99))?;
        writer.close()
    }

    /// Every entry below `root` but the source locks, which a writer
    /// opening touches.
    fn tree(root: &Path) -> Vec<(String, Option<Vec<u8>>)> {
        let mut tree = snapshot(root);
        tree.retain(|(name, _)| !name.ends_with(".lock"));
        tree
    }

    /// Collects every recall event.
    fn collector() -> (Arc<Mutex<Vec<RecallEvent>>>, RecallObserver) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&events);
        let on_recall: RecallObserver =
            Arc::new(move |event: &RecallEvent| sink.lock().unwrap().push(event.clone()));
        (events, on_recall)
    }

    /// The whole selection of both fixture streams.
    fn all_files(store: &ParquetRawStore) -> Vec<SealedFile> {
        everything(
            store,
            &[&TieredFixture::streams()[0], &TieredFixture::streams()[1]],
        )
    }

    /// A real removal of `partition` through `writer`: the ledger copies,
    /// the plan, then [`RawWriter::remove_partitions`].
    fn remove_through(fixture: &TieredFixture, writer: &mut RawWriter, partition: &str) {
        fixture.ledger_copies();
        let ledger = copy_ledger::read(&fixture.hot).unwrap().unwrap();
        let scan = removal::scan_hot(&fixture.hot, &first_hot()).unwrap();
        let plan = removal::plan(
            &ledger,
            &scan,
            &TieredFixture::target_ids(),
            2,
            &first_hot(),
            None,
        )
        .unwrap();
        let report = writer.remove_partitions(&plan, &first_hot(), FIXTURE_D0 + 40 * DAY);
        assert_eq!(report.removed, vec![partition.to_owned()], "{report:?}");
        assert!(!layout::resolve(&fixture.hot, partition).exists());
    }

    #[test]
    fn union_numbering_counts_the_ledger_when_hot_lost_a_partition() {
        let key = stream("aggTrade");
        let partition = TieredFixture::partition(&key, 0);
        // Hot lost a recorded partition (not recorded as removed): the
        // ledger's parts 0 to 2 continue the numbering.
        let fixture = TieredFixture::new("union-lost");
        fixture.copy(&partition);
        fs::remove_dir_all(layout::resolve(&fixture.hot, &partition)).unwrap();
        let sealed = write_one(&fixture, &key, FIXTURE_D0 + 10).unwrap();
        assert_eq!(sealed[0].part, 3);

        // Each kind alone raises the next number: lost, held and
        // quarantined entries (written directly until #193 and #194).
        let path = |part: u32| format!("{partition}/{}", PartFile::Data.name(part));
        let rows: [(&str, Kind); 3] = [
            (
                "lost",
                Kind::Lost {
                    path: path(7),
                    origin: LostOrigin::Verify,
                    sha256: None,
                },
            ),
            (
                "hold",
                Kind::Hold {
                    path: path(7),
                    reason: HoldReason::Unlisted,
                },
            ),
            (
                "quarantined",
                Kind::Quarantined {
                    path: path(7),
                    quarantine: "q-0001".to_owned(),
                },
            ),
        ];
        for (name, kind) in rows {
            let fixture = TieredFixture::new("union-kind");
            fixture.append(kind);
            let sealed = write_one(&fixture, &key, FIXTURE_D0 + 10).unwrap();
            assert_eq!(sealed[0].part, 8, "{name}");
            // The single-root store over the same root numbers from hot.
            let single = ParquetRawStore::new(&fixture.hot);
            let mut writer = single
                .writer("binance-um", RotationPolicy::default())
                .unwrap();
            writer.append(&key, record(FIXTURE_D0 + 11, 1)).unwrap();
            assert_eq!(writer.close().unwrap()[0].part, 9, "{name}");
        }
    }

    #[test]
    fn an_append_to_a_removed_partition_recalls_it_first() {
        let fixture = TieredFixture::new("recall");
        let key = stream("aggTrade");
        let partition = TieredFixture::partition(&key, 0);
        let reference = everything(&fixture.reference(), &[&key]);
        fixture.copy(&partition);
        let recorded = TieredFixture::data_paths(&fixture.hot, &partition);

        // The `returned` record follows every file: checked as each event
        // arrives.
        let events = Arc::new(Mutex::new(Vec::new()));
        let (sink, hot) = (Arc::clone(&events), fixture.hot.clone());
        let check_partition = partition.clone();
        let on_recall: RecallObserver = Arc::new(move |event: &RecallEvent| {
            let ledger = copy_ledger::read(&hot).unwrap().unwrap();
            let removed = crate::tiers::removed_partitions(&ledger).contains(&check_partition);
            match event {
                RecallEvent::Returned { .. } => assert!(!removed),
                _ => assert!(removed, "{event:?} before `returned`"),
            }
            sink.lock().unwrap().push(event.clone());
        });
        let mut writer = tiered(&fixture, Some(on_recall)).unwrap();
        remove_through(&fixture, &mut writer, &partition);

        for round in 0..2 {
            events.lock().unwrap().clear();
            writer
                .append(&key, record(FIXTURE_D0 + 10 + round, round))
                .unwrap();
            let sealed = writer.seal_all().unwrap();
            let expected_files = recorded.len() + round as usize;
            let got = events.lock().unwrap().clone();
            let copied: Vec<&str> = got
                .iter()
                .filter_map(|event| match event {
                    RecallEvent::Copied { path, root, .. } => {
                        assert_eq!(root, &fixture.cold[0]);
                        Some(path.as_str())
                    }
                    _ => None,
                })
                .collect();
            let mut in_order: Vec<String> = TieredFixture::data_paths(&fixture.hot, &partition);
            in_order.retain(|path| *path != sealed[0].relative_path);
            assert_eq!(copied, in_order, "round {round}: every file, in path order");
            assert_eq!(copied.len(), expected_files);
            assert_eq!(
                got.last(),
                Some(&RecallEvent::Returned {
                    partition: partition.clone(),
                    files: expected_files,
                })
            );
            // The new part takes the highest plus one.
            assert_eq!(sealed[0].part, 3 + u32::try_from(round).unwrap());
            let mut expected = reference.clone();
            let store = fixture.store();
            let files = everything(&store, &[&key]);
            for file in &files {
                if !expected.contains(file) {
                    expected.push(file.clone());
                }
            }
            expected.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
            assert_eq!(files, expected);
            assert_eq!(files.len(), reference.len() + 1 + round as usize);

            // Once the new part is copied and the ledger copies cover it,
            // a new plan removes the partition again.
            fixture.copy_path(&sealed[0].relative_path, &["cold-a", "cold-b"]);
            remove_through(&fixture, &mut writer, &partition);
        }
        writer.close().unwrap();
    }

    /// A fixture with `partition` copied and removed by the stand-in.
    fn removed_fixture(tag: &str) -> (TieredFixture, RawStreamKey, String) {
        let fixture = TieredFixture::new(tag);
        let key = stream("aggTrade");
        let partition = TieredFixture::partition(&key, 0);
        fixture.copy(&partition);
        fixture.remove(&partition);
        (fixture, key, partition)
    }

    /// The `n`-th recorded data path of `partition` on cold-a.
    fn cold_path(fixture: &TieredFixture, partition: &str, n: usize) -> String {
        TieredFixture::data_paths(&fixture.cold[0], partition)[n].clone()
    }

    fn writable(path: &Path) {
        let mut permissions = fs::metadata(path).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        permissions.set_readonly(false);
        fs::set_permissions(path, permissions).unwrap();
    }

    #[test]
    fn the_recall_reads_from_the_first_root_with_a_verified_copy() {
        // Absent on cold-a: from cold-b.
        let (fixture, key, partition) = removed_fixture("recall-absent");
        let path = cold_path(&fixture, &partition, 1);
        for file in [path.clone(), TieredFixture::manifest_of(&path)] {
            fs::remove_file(layout::resolve(&fixture.cold[0], &file)).unwrap();
        }
        let (events, on_recall) = collector();
        let mut writer = tiered(&fixture, Some(on_recall)).unwrap();
        writer.append(&key, record(FIXTURE_D0 + 10, 0)).unwrap();
        writer.close().unwrap();
        let from: Vec<(String, PathBuf)> = events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                RecallEvent::Copied { path, root, .. } => Some((path.clone(), root.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(from.len(), 3);
        for (copied, root) in from {
            let expected = if copied == path { 1 } else { 0 };
            assert_eq!(root, fixture.cold[expected], "{copied}");
        }

        // Damaged on cold-a: reported, then copied from cold-b (#191 STOP 1
        // decision 1).
        let (fixture, key, partition) = removed_fixture("recall-damaged");
        let path = cold_path(&fixture, &partition, 0);
        let data = layout::resolve(&fixture.cold[0], &path);
        writable(&data);
        let mut bytes = fs::read(&data).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0x01;
        fs::write(&data, bytes).unwrap();
        let (events, on_recall) = collector();
        let mut writer = tiered(&fixture, Some(on_recall)).unwrap();
        writer.append(&key, record(FIXTURE_D0 + 10, 0)).unwrap();
        writer.close().unwrap();
        let events = events.lock().unwrap().clone();
        match &events[..2] {
            [
                RecallEvent::Damaged {
                    path: damaged,
                    root: damaged_root,
                    error,
                    ..
                },
                RecallEvent::Copied {
                    path: copied, root, ..
                },
            ] => {
                assert_eq!((damaged, copied), (&path, &path));
                assert_eq!(damaged_root, &fixture.cold[0]);
                assert_eq!(root, &fixture.cold[1]);
                assert!(error.contains("does not match its manifest"), "{error}");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            fs::read(layout::resolve(&fixture.hot, &path)).unwrap(),
            fs::read(layout::resolve(&fixture.cold[1], &path)).unwrap()
        );

        // A cold manifest text that differs from the ledger's: Integrity,
        // hot unchanged, the writer poisoned.
        let (fixture, key, partition) = removed_fixture("recall-manifest");
        let path = cold_path(&fixture, &partition, 0);
        let manifest_path = layout::resolve(&fixture.cold[0], &TieredFixture::manifest_of(&path));
        let mut changed = manifest::parse(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
        changed.rows += 1;
        writable(&manifest_path);
        fs::write(&manifest_path, manifest::render(&changed)).unwrap();
        let before = tree(&fixture.hot);
        let mut writer = tiered(&fixture, None).unwrap();
        let error = writer.append(&key, record(FIXTURE_D0 + 10, 0)).unwrap_err();
        let RawStoreError::Integrity(message) = &error else {
            panic!("{error:?}")
        };
        assert!(
            message.contains("differs from the copy ledger's"),
            "{message}"
        );
        assert!(message.contains(&path), "{message}");
        assert_eq!(tree(&fixture.hot), before);
        assert!(matches!(
            writer.append(&key, record(FIXTURE_D0 + 11, 1)),
            Err(RawStoreError::Io(_))
        ));
        drop(writer);

        // Missing on every root: Integrity naming the file and both roots.
        let (fixture, key, partition) = removed_fixture("recall-missing");
        let path = cold_path(&fixture, &partition, 2);
        for root in &fixture.cold {
            for file in [path.clone(), TieredFixture::manifest_of(&path)] {
                fs::remove_file(layout::resolve(root, &file)).unwrap();
            }
        }
        let error = write_one(&fixture, &key, FIXTURE_D0 + 10).unwrap_err();
        let RawStoreError::Integrity(message) = &error else {
            panic!("{error:?}")
        };
        for text in [
            path.clone(),
            fixture.cold[0].display().to_string(),
            fixture.cold[1].display().to_string(),
        ] {
            assert!(message.contains(&text), "{text:?} in {message}");
        }
        assert!(
            copy_ledger::read(&fixture.hot)
                .unwrap()
                .unwrap()
                .records
                .iter()
                .all(|r| !matches!(r.kind, Kind::Returned { .. })),
            "nothing is appended"
        );

        // A held path: Integrity naming `mie store resolve`.
        let (fixture, key, partition) = removed_fixture("recall-held");
        fixture.append(Kind::Hold {
            path: cold_path(&fixture, &partition, 1),
            reason: HoldReason::ManifestConflict,
        });
        let error = write_one(&fixture, &key, FIXTURE_D0 + 10).unwrap_err();
        let RawStoreError::Integrity(message) = &error else {
            panic!("{error:?}")
        };
        assert!(message.contains("mie store resolve"), "{message}");
    }

    #[test]
    fn a_crash_at_every_recall_step_keeps_every_file_and_the_ledger() {
        /// Where the recall stops.
        #[derive(Debug, Clone, Copy)]
        enum Crash {
            /// At a copy step of file `n`.
            Copy(usize, CopyStep),
            /// After the last copy, at the `returned` append.
            Returned,
        }
        let mut crashes: Vec<Crash> = Vec::new();
        for n in [0, 2] {
            crashes.extend(CopyStep::ALL.iter().map(|&step| Crash::Copy(n, step)));
        }
        crashes.push(Crash::Returned);
        for crash in crashes {
            let (fixture, key, partition) = removed_fixture("recall-crash");
            let files = TieredFixture::data_paths(&fixture.cold[0], &partition);
            // Arm the crash before file n: at once for file 0, from the
            // event of file n − 1 otherwise.
            let arm: RecallObserver = {
                let files = files.clone();
                Arc::new(move |event: &RecallEvent| {
                    let RecallEvent::Copied { path, .. } = event else {
                        return;
                    };
                    let index = files.iter().position(|p| p == path).unwrap();
                    match crash {
                        Crash::Copy(n, step) if n > 0 && index == n - 1 => {
                            replica::CRASH_AFTER.with(|c| c.set(Some(step)));
                        }
                        Crash::Returned if index == files.len() - 1 => {
                            copy_ledger::FAIL_NEXT_WRITE.with(|f| f.set(true));
                        }
                        _ => {}
                    }
                })
            };
            if let Crash::Copy(0, step) = crash {
                replica::CRASH_AFTER.with(|c| c.set(Some(step)));
            }
            let mut writer = tiered(&fixture, Some(arm)).unwrap();
            assert!(
                writer.append(&key, record(FIXTURE_D0 + 10, 0)).is_err(),
                "{crash:?}"
            );
            drop(writer);
            replica::CRASH_AFTER.with(|c| c.set(None));

            // Every file is complete on at least one root, the ledger
            // verifies, and the selection is the reference's.
            let store = fixture.store();
            let reference = fixture.reference();
            let expected = all_files(&reference);
            for file in &expected {
                assert_eq!(store.read(file), reference.read(file), "{crash:?}");
            }
            let ledger = copy_ledger::read(&fixture.hot).unwrap().unwrap();
            assert!(
                crate::tiers::removed_partitions(&ledger).contains(&partition),
                "{crash:?}: nothing appended"
            );
            assert_eq!(all_files(&store), expected, "{crash:?}");

            // A new writer's recovery plus the same append completes the
            // recall with the same part number.
            let sealed = write_one(&fixture, &key, FIXTURE_D0 + 10).unwrap();
            assert_eq!(sealed[0].part, 3, "{crash:?}");
            assert_eq!(
                TieredFixture::data_paths(&fixture.hot, &partition).len(),
                4,
                "{crash:?}"
            );
            let ledger = copy_ledger::read(&fixture.hot).unwrap().unwrap();
            assert!(!crate::tiers::removed_partitions(&ledger).contains(&partition));
        }
    }

    #[test]
    fn the_degraded_writer_opens_only_hot_window_dates() {
        for damage in ["missing", "chain"] {
            let (fixture, key, partition) = removed_fixture("degraded");
            let ledger = copy_ledger::ledger_path(&fixture.hot);
            if damage == "missing" {
                fs::remove_file(&ledger).unwrap();
            } else {
                let mut bytes = fs::read(&ledger).unwrap();
                bytes[copy_ledger::HEADER.len() + 3] ^= 0x01;
                fs::write(&ledger, bytes).unwrap();
                assert!(copy_ledger::read(&fixture.hot).is_err());
            }
            // A date on the first hot date is written as today.
            let hot_day = FIXTURE_D0 + 3 * DAY + 10;
            let sealed = write_one(&fixture, &key, hot_day).unwrap();
            assert_eq!(sealed[0].part, 3, "{damage}");

            // An earlier date is refused before anything is created, and
            // the writer is poisoned.
            let before = tree(&fixture.hot);
            let mut writer = tiered(&fixture, None).unwrap();
            let error = writer.append(&key, record(FIXTURE_D0 + 10, 0)).unwrap_err();
            let RawStoreError::Integrity(message) = &error else {
                panic!("{damage}: {error:?}")
            };
            for text in [partition.as_str(), "only hot-window dates", "rebuild"] {
                assert!(message.contains(text), "{damage}: {text:?} in {message}");
            }
            assert_eq!(tree(&fixture.hot), before, "{damage}");
            assert!(matches!(
                writer.append(&key, record(hot_day, 1)),
                Err(RawStoreError::Io(_))
            ));
        }
    }

    #[test]
    fn a_single_root_store_never_reads_the_ledger() {
        let (fixture, key, partition) = removed_fixture("single-root");
        let mut writer = ParquetRawStore::new(&fixture.hot)
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        writer.append(&key, record(FIXTURE_D0 + 10, 0)).unwrap();
        let sealed = writer.close().unwrap();
        assert_eq!(
            sealed[0].relative_path,
            format!("{partition}/part-00000.parquet")
        );
        // `writer()` refuses a tiered store: it has no window and no clock.
        assert!(matches!(
            fixture
                .store()
                .writer("binance-um", RotationPolicy::default()),
            Err(RawStoreError::Invalid(_))
        ));
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
