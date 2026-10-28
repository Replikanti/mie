//! Opening a finished dataset: verified before anything reads it
//! (ADR-057 D3, D5).

use super::manifest::OutcomeManifest;
use super::schema;
use super::{MANIFEST_FILE, OutcomeError, TABLES};
use crate::manifest::sha256_hex;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};

/// A verified outcome dataset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeDataset {
    dir: PathBuf,
    manifest: OutcomeManifest,
}

impl OutcomeDataset {
    /// Opens the dataset in `dir` and verifies it: the manifest parses and
    /// its `content` line is the hash of the lines before it; every table
    /// has the manifest's SHA-256, row count, format metadata and schema;
    /// `outcomes` has five rows per `contexts` row.
    ///
    /// # Errors
    ///
    /// [`OutcomeError::Incomplete`] without `dataset.manifest`,
    /// [`OutcomeError::Integrity`] for any mismatch,
    /// [`OutcomeError::Io`] when a file cannot be read.
    pub fn open(dir: &Path) -> Result<Self, OutcomeError> {
        let path = dir.join(MANIFEST_FILE);
        let text = match fs::read(&path) {
            Ok(bytes) => String::from_utf8(bytes)
                .map_err(|_| OutcomeError::Integrity(format!("{} is not UTF-8", path.display())))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(OutcomeError::Incomplete(format!(
                    "{} has no {MANIFEST_FILE}: the build did not finish",
                    dir.display()
                )));
            }
            Err(error) => return Err(super::io_error("read", &path)(error)),
        };
        let manifest = OutcomeManifest::parse(&text)?;
        if manifest.compute_content() != manifest.content {
            return Err(OutcomeError::Integrity(
                "the content hash does not match the manifest".to_owned(),
            ));
        }
        let schemas = [
            schema::contexts_schema(),
            schema::outcomes_schema(),
            schema::passages_schema(),
        ];
        for ((file, name), expected) in manifest.files.iter().zip(TABLES).zip(schemas) {
            verify_table(&dir.join(name), name, file.rows, &file.sha256, &expected)?;
        }
        let rows = |name: &str| manifest.rows(name).unwrap_or(0);
        if rows(TABLES[1]) != 5 * rows(TABLES[0]) {
            return Err(OutcomeError::Integrity(format!(
                "{} outcome rows for {} contexts: not five per record",
                rows(TABLES[1]),
                rows(TABLES[0])
            )));
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            manifest,
        })
    }

    /// The dataset directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The verified manifest.
    pub fn manifest(&self) -> &OutcomeManifest {
        &self.manifest
    }

    /// The path of the table `name` (one of [`TABLES`]).
    pub fn table(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

/// Checks one table against its manifest line and its schema.
fn verify_table(
    path: &Path,
    name: &str,
    rows: u64,
    sha256: &str,
    expected: &SchemaRef,
) -> Result<(), OutcomeError> {
    let integrity = |detail: String| OutcomeError::Integrity(format!("{name}: {detail}"));
    let data = fs::read(path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => integrity("missing".to_owned()),
        _ => OutcomeError::Io(format!("read {}: {e}", path.display())),
    })?;
    if sha256_hex(&data) != sha256 {
        return Err(integrity("SHA-256 does not match the manifest".to_owned()));
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data))
        .map_err(|e| integrity(format!("decode: {e}")))?;
    let metadata = builder.metadata().file_metadata();
    let format = metadata
        .key_value_metadata()
        .and_then(|pairs| pairs.iter().find(|kv| kv.key == schema::KEY_FORMAT))
        .and_then(|kv| kv.value.as_deref());
    if format != Some(schema::FORMAT_VERSION) {
        return Err(integrity(format!(
            "metadata {} is {format:?}, expected {:?}",
            schema::KEY_FORMAT,
            schema::FORMAT_VERSION
        )));
    }
    if u64::try_from(metadata.num_rows()).ok() != Some(rows) {
        return Err(integrity(format!(
            "{} rows, the manifest says {rows}",
            metadata.num_rows()
        )));
    }
    if builder.schema().fields() != expected.fields() {
        return Err(integrity("schema is not the dataset format 1".to_owned()));
    }
    Ok(())
}
