//! The store entry point: writers, dataset selection and verified reads
//! (ADR-030), over one root or over tiers (ADR-062 D7, the `tiers` module).

use crate::layout::{self, PartFile};
use crate::manifest;
use crate::schema;
use crate::sorted_entries;
use crate::tiers;
use crate::writer::{RawWriter, RotationPolicy, TierContext};
use bytes::Bytes;
use mie_ports::raw::{
    RawDataset, RawRecord, RawRecordSource, RawSelection, RawStoreError, RawStreamKey, SealedFile,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};

/// A raw store: one root, or hot plus ordered cold roots (ADR-062 D7, the
/// crate docs' *Tiered store*).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetRawStore {
    root: PathBuf,
    tiers: Option<Tiers>,
}

/// The cold roots of a tiered store, in read order; never empty.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Tiers {
    cold_roots: Vec<PathBuf>,
}

impl ParquetRawStore {
    /// A store rooted at `root`. Does no I/O: the directory is created by the
    /// first writer.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            tiers: None,
        }
    }

    /// A tiered store: hot at `hot`, then `cold_roots` in read order
    /// (ADR-062 D7). Does no I/O, so it never touches a cold mount. With an
    /// empty `cold_roots` it is the value [`new`](Self::new) gives, and runs
    /// the single-root code.
    pub fn tiered(hot: impl Into<PathBuf>, cold_roots: Vec<PathBuf>) -> Self {
        Self {
            root: hot.into(),
            tiers: (!cold_roots.is_empty()).then_some(Tiers { cold_roots }),
        }
    }

    /// The cold roots in read order; empty for a single-root store.
    pub fn cold_roots(&self) -> &[PathBuf] {
        self.tiers.as_ref().map_or(&[], |tiers| &tiers.cold_roots)
    }

    /// Whether the store has cold roots.
    pub fn is_tiered(&self) -> bool {
        self.tiers.is_some()
    }

    /// The store's root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Opens the writer of `source` on a single-root store: creates its
    /// directory, takes its lock and recovers interrupted seals
    /// ([`RawWriter::recovery`]).
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Invalid`] on a tiered store, whose writer needs its
    /// hot window and clock ([`tiered_writer`](Self::tiered_writer)), so no
    /// path opens a tiered writer without them; [`RawStoreError::Locked`]
    /// when another writer holds the source; [`RawStoreError::Invalid`] for
    /// a bad source name or policy; the recovery errors of ADR-030.
    pub fn writer(&self, source: &str, policy: RotationPolicy) -> Result<RawWriter, RawStoreError> {
        if self.is_tiered() {
            return Err(RawStoreError::Invalid(format!(
                "{} is a tiered store: its writer needs the hot window and a clock \
                 (tiered_writer, ADR-062 D6)",
                self.root.display()
            )));
        }
        RawWriter::open(&self.root, source, policy, None)
    }

    /// Opens the writer of `source` on a tiered store (ADR-062 D6,
    /// [`RawWriter`]): as [`writer`](Self::writer), then it reads the copy
    /// ledger once for the union part numbering and the recall, or, without
    /// a readable ledger, opens as the degraded writer, which writes only
    /// dates from `ctx.first_hot_date` on. On a single-root store it is
    /// [`writer`](Self::writer) and `ctx` is unused.
    ///
    /// # Errors
    ///
    /// As [`writer`](Self::writer), except the tiered refusal.
    pub fn tiered_writer(
        &self,
        source: &str,
        policy: RotationPolicy,
        ctx: TierContext,
    ) -> Result<RawWriter, RawStoreError> {
        let tiers = self
            .tiers
            .as_ref()
            .map(|tiers| (tiers.cold_roots.clone(), ctx));
        RawWriter::open(&self.root, source, policy, tiers)
    }

    /// The sealed files of `stream` on `date`, from their manifests.
    fn sealed_files(
        &self,
        stream: &RawStreamKey,
        date: &str,
    ) -> Result<Vec<SealedFile>, RawStoreError> {
        let partition = layout::resolve(&self.root, &layout::partition_dir(stream, date));
        let mut files = Vec::new();
        for name in sorted_entries(&partition)? {
            let Some((PartFile::Manifest, part)) = PartFile::classify(&name) else {
                continue;
            };
            let path = partition.join(&name);
            let text = fs::read_to_string(&path).map_err(|e| {
                RawStoreError::Corrupt(format!("read manifest {}: {e}", path.display()))
            })?;
            let file = manifest::parse(&text)?;
            if file.stream != *stream || file.date != date || file.part != part {
                return Err(RawStoreError::Corrupt(format!(
                    "manifest {} describes {}, not its own location",
                    path.display(),
                    file.relative_path
                )));
            }
            files.push(file);
        }
        Ok(files)
    }
}

impl ParquetRawStore {
    /// The selection over hot's listing alone, as a single-root store
    /// answers it; it never reads the copy ledger. In a tiered store only
    /// the capture start's seed scan uses it, as its fallback when the union
    /// `select` fails (ADR-062 D7 *Seeds*): elsewhere it could answer
    /// shorter than the union without an error.
    ///
    /// # Errors
    ///
    /// As [`RawRecordSource::select`] of a single-root store.
    pub fn select_hot(&self, selection: &RawSelection) -> Result<RawDataset, RawStoreError> {
        let window = selection.window();
        let (start, end) = (window.start.as_millis(), window.end.as_millis());
        // Only the date partitions the window touches, clamped to the
        // supported range.
        let first = start.max(0);
        let last = (end - 1).min(layout::MAX_EVENT_TIME_MS);
        let mut files = Vec::new();
        if first <= last {
            let (first_date, last_date) = (layout::civil_date(first)?, layout::civil_date(last)?);
            for stream in selection.streams() {
                let stream_dir = layout::resolve(&self.root, &layout::stream_dir(stream));
                for name in sorted_entries(&stream_dir)? {
                    let Some(date) = name.strip_prefix("date=") else {
                        continue;
                    };
                    if layout::parse_date(date).is_none()
                        || date < first_date.as_str()
                        || date > last_date.as_str()
                    {
                        continue;
                    }
                    files.extend(self.sealed_files(stream, date)?.into_iter().filter(|f| {
                        f.min_event_time.as_millis() < end && f.max_event_time.as_millis() >= start
                    }));
                }
            }
        }
        files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        let version = manifest::dataset_version(window, selection.streams(), &files);
        Ok(RawDataset { version, files })
    }

    /// The tiered `read`: hot, then each cold root in order (ADR-062 D7).
    fn read_tiered(
        &self,
        file: &SealedFile,
        tiers: &Tiers,
    ) -> Result<Vec<RawRecord>, RawStoreError> {
        let roots = std::iter::once(&self.root).chain(&tiers.cold_roots);
        for root in roots.clone() {
            let path = layout::resolve(root, &file.relative_path);
            match fs::read(&path) {
                // Absent on this root: the next one may hold it.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(RawStoreError::Io(format!(
                        "read {} on root {}: {e}",
                        path.display(),
                        root.display()
                    )));
                }
                // The first present copy decides: a bad copy is an error,
                // never a fallback to the next one (D5).
                Ok(data) => return decode_verified(data, file),
            }
        }
        let tried: Vec<String> = roots.map(|root| root.display().to_string()).collect();
        Err(RawStoreError::Integrity(format!(
            "{} is missing on every root: {}",
            file.relative_path,
            tried.join(", ")
        )))
    }
}

impl RawRecordSource for ParquetRawStore {
    fn select(&self, selection: &RawSelection) -> Result<RawDataset, RawStoreError> {
        match &self.tiers {
            None => self.select_hot(selection),
            Some(_) => tiers::select(&self.root, selection),
        }
    }

    fn read(&self, file: &SealedFile) -> Result<Vec<RawRecord>, RawStoreError> {
        let (stream, date, part) = layout::parse_data_path(&file.relative_path)?;
        if stream != file.stream || date != file.date || part != file.part {
            return Err(RawStoreError::Invalid(format!(
                "{} does not match the file's stream, date and part",
                file.relative_path
            )));
        }
        if let Some(tiers) = &self.tiers {
            return self.read_tiered(file, tiers);
        }
        let path = layout::resolve(&self.root, &file.relative_path);
        let data = fs::read(&path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                RawStoreError::Integrity(format!("{} is missing", file.relative_path))
            }
            _ => RawStoreError::Io(format!("read {}: {e}", path.display())),
        })?;
        decode_verified(data, file)
    }
}

/// Verifies `data` against `file`'s manifest and decodes exactly those
/// bytes, checking the schema metadata, the row count and the event-time
/// range.
fn decode_verified(data: Vec<u8>, file: &SealedFile) -> Result<Vec<RawRecord>, RawStoreError> {
    manifest::verify(&data, file)?;

    // Decode the very bytes that were just verified.
    let corrupt = |e: &dyn std::fmt::Display| {
        RawStoreError::Corrupt(format!("decode {}: {e}", file.relative_path))
    };
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data)).map_err(|e| corrupt(&e))?;
    let stored = builder.metadata().file_metadata().key_value_metadata();
    for (key, expected) in schema::key_value_metadata(&file.stream) {
        let found = stored
            .and_then(|pairs| pairs.iter().find(|kv| kv.key == key))
            .and_then(|kv| kv.value.as_deref());
        if found != Some(expected) {
            return Err(RawStoreError::Integrity(format!(
                "{}: metadata {key} is {found:?}, expected {expected:?}",
                file.relative_path
            )));
        }
    }
    let mut records = Vec::new();
    for batch in builder.build().map_err(|e| corrupt(&e))? {
        records.extend(schema::from_batch(&batch.map_err(|e| corrupt(&e))?)?);
    }

    let times = records.iter().map(|r| r.event_time);
    let (min, max) = (times.clone().min(), times.max());
    if records.len() as u64 != file.rows
        || min != Some(file.min_event_time)
        || max != Some(file.max_event_time)
    {
        return Err(RawStoreError::Integrity(format!(
            "{}: {} rows over [{min:?}, {max:?}] do not match the manifest",
            file.relative_path,
            records.len()
        )));
    }
    Ok(records)
}
