//! The store entry point: writers, dataset selection and verified reads
//! (ADR-030).

use crate::layout::{self, PartFile};
use crate::manifest;
use crate::schema;
use crate::sorted_entries;
use crate::writer::{RawWriter, RotationPolicy};
use bytes::Bytes;
use mie_ports::raw::{
    RawDataset, RawRecord, RawRecordSource, RawSelection, RawStoreError, RawStreamKey, SealedFile,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};

/// A raw store rooted at one directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetRawStore {
    root: PathBuf,
}

impl ParquetRawStore {
    /// A store rooted at `root`. Does no I/O: the directory is created by the
    /// first writer.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The store's root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Opens the writer of `source`: creates its directory, takes its lock
    /// and recovers interrupted seals ([`RawWriter::recovery`]).
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Locked`] when another writer holds the source,
    /// [`RawStoreError::Invalid`] for a bad source name or policy, and the
    /// recovery errors of ADR-030.
    pub fn writer(&self, source: &str, policy: RotationPolicy) -> Result<RawWriter, RawStoreError> {
        RawWriter::open(&self.root, source, policy)
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

impl RawRecordSource for ParquetRawStore {
    fn select(&self, selection: &RawSelection) -> Result<RawDataset, RawStoreError> {
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

    fn read(&self, file: &SealedFile) -> Result<Vec<RawRecord>, RawStoreError> {
        let (stream, date, part) = layout::parse_data_path(&file.relative_path)?;
        if stream != file.stream || date != file.date || part != file.part {
            return Err(RawStoreError::Invalid(format!(
                "{} does not match the file's stream, date and part",
                file.relative_path
            )));
        }
        let path = layout::resolve(&self.root, &file.relative_path);
        let data = fs::read(&path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                RawStoreError::Integrity(format!("{} is missing", file.relative_path))
            }
            _ => RawStoreError::Io(format!("read {}: {e}", path.display())),
        })?;
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
}
