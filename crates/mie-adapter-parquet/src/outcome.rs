//! The conditional outcome dataset: context records and their forward
//! outcome labels as versioned, content-hashed Parquet (#27, ADR-057;
//! format 2 of #28, ADR-058, proposed).
//!
//! One dataset is one directory written by [`OutcomeWriter`] from one
//! labeling run (`mie_app::conditional::label_window_started`):
//!
//! - `contexts.parquet`: one row per context record (ADR-054 D1), in record
//!   order, numbered by `record` from 0;
//! - `outcomes.parquet`: one row per record and horizon (ADR-055 D3–D7,
//!   D9), in record order and then in horizon order;
//! - `passages.parquet`: one row per present `level` or `extreme` passage
//!   of a trigger record (ADR-055 D8);
//! - `effort.parquet`: one row per record and effort window (`5m`, `15m`,
//!   `1h`), the record's effort-vs-result evaluations (ADR-051 D5) as the
//!   order-flow studies read them (ADR-058 D2);
//! - `dataset.manifest`: the provenance, the row count and SHA-256 of each
//!   file, the content hash and the code version (D3).
//!
//! Format 2 (ADR-058) also stores the auction side and the UTC-day CVD on
//! `contexts`. A format-1 dataset is refused: it has to be rebuilt.
//!
//! `record` joins the four tables; the ADR-055 D1 key (`decision_time`,
//! `event_kind`, `event_seq`, `ordinal`) stays on `contexts`. Every row
//! carries the run's provenance (ADR-054 D5, ADR-055 D10) in six constant
//! columns. Prices and quantities are `Decimal128(18, 8)`, exact in their
//! ADR-027 units, and times `Timestamp(ms, "UTC")`; enums are their
//! snake_case names.
//!
//! The manifest is written last, after every table is sealed (fsynced,
//! read-only, renamed from a hidden temp name): a directory is a dataset if
//! and only if `dataset.manifest` exists (ADR-030's rule). Its `content`
//! line hashes every manifest byte before it, so two builds from the same
//! data, windows, source, engine and policies write the same `content`,
//! whatever the code version on the line after it.
//!
//! Labels are read here and in `mie-cli` only: an adapter is outside the
//! core the ADR-055 D11 guard covers, and no feature reads this crate.

mod dataset;
mod manifest;
mod schema;
mod writer;

#[cfg(test)]
mod tests;

pub use dataset::OutcomeDataset;
pub use manifest::{OutcomeFile, OutcomeManifest};
pub use writer::{OutcomeHeader, OutcomeWriter};

use mie_ports::raw::RawStoreError;
use std::fmt;
use std::path::Path;

/// File name of the manifest.
pub const MANIFEST_FILE: &str = "dataset.manifest";

/// The tables of a dataset, in manifest order.
pub const TABLES: [&str; 4] = [
    "contexts.parquet",
    "outcomes.parquet",
    "passages.parquet",
    "effort.parquet",
];

/// Why an outcome dataset could not be written or read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeError {
    /// The output directory exists already: a dataset is never overwritten.
    Exists(String),
    /// The caller broke the writer's contract: a label that does not match
    /// the next unlabeled record, a record left without a label, a header
    /// that cannot be written on one manifest line.
    Contract(String),
    /// A value does not fit its column: a price or quantity beyond
    /// `Decimal128(18, 8)`. Never rounded.
    Range(String),
    /// The directory holds no `dataset.manifest`: the build did not finish.
    Incomplete(String),
    /// A file does not match the manifest, or the manifest does not match
    /// itself (tampering or corruption).
    Integrity(String),
    /// The underlying storage failed.
    Io(String),
}

impl fmt::Display for OutcomeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exists(detail) => write!(f, "outcome dataset exists: {detail}"),
            Self::Contract(detail) => write!(f, "outcome writer contract broken: {detail}"),
            Self::Range(detail) => write!(f, "outcome value out of range: {detail}"),
            Self::Incomplete(detail) => write!(f, "outcome dataset incomplete: {detail}"),
            Self::Integrity(detail) => {
                write!(f, "outcome dataset integrity check failed: {detail}")
            }
            Self::Io(detail) => write!(f, "outcome dataset I/O failed: {detail}"),
        }
    }
}

impl std::error::Error for OutcomeError {}

impl From<RawStoreError> for OutcomeError {
    /// The shared file helpers report every failure as a raw-store error;
    /// here they are storage failures.
    fn from(error: RawStoreError) -> Self {
        Self::Io(error.to_string())
    }
}

/// Maps an I/O failure on `path` to [`OutcomeError::Io`].
fn io_error(action: &str, path: &Path) -> impl FnOnce(std::io::Error) -> OutcomeError {
    let context = format!("{action} {}", path.display());
    move |error| OutcomeError::Io(format!("{context}: {error}"))
}
