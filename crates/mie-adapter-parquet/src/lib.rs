//! Raw-store adapter: immutable, verbatim Parquet files (ADR-003, ADR-022,
//! ADR-030).
//!
//! Implements the raw store ports of [`mie_ports::raw`]:
//!
//! - [`RawWriter`] ([`RawRecordSink`](mie_ports::raw::RawRecordSink)) buffers
//!   records into open parts and seals them with a temp file, fsync, a
//!   durable pending manifest and an atomic rename. A sealed file never
//!   changes again, and a killed writer never leaves a partial one visible.
//! - [`ParquetRawStore`] ([`RawRecordSource`](mie_ports::raw::RawRecordSource))
//!   resolves a replay window to its sealed files and dataset version, and
//!   reads each file only after verifying it against its manifest.
//!
//! Every message is stored verbatim: the exact payload bytes plus capture
//! metadata in one exchange-agnostic envelope (schema v1). Parsing and
//! fixed-point conversion (ADR-027) belong to normalization, shared by live
//! and replay. Only `mie-cli` wires this adapter (ADR-025).
//!
//! The layout is Hive-partitioned, so DuckDB reads sealed files directly:
//!
//! ```sql
//! SELECT event_time, json_extract_string(decode(payload), '$.p') AS price
//! FROM read_parquet(
//!     '<root>/source=binance-um/instrument=BTCUSDT/stream=aggTrade/*/*.parquet',
//!     hive_partitioning = true)
//! WHERE date = '2026-10-06'
//! ORDER BY event_time;
//! ```
//!
//! Files in progress are hidden (`.part-NNNNN.parquet.tmp`) and never match
//! `*.parquet`.

mod layout;
mod manifest;
mod reader;
mod recovery;
mod schema;
mod writer;

#[cfg(test)]
mod testutil;

pub use reader::ParquetRawStore;
pub use recovery::RecoveryReport;
pub use writer::{RawWriter, RotationPolicy};

use mie_ports::raw::RawStoreError;
use std::fs::{self, File};
use std::path::Path;

/// Maps an I/O failure on `path` to [`RawStoreError::Io`].
fn io_error(action: &str, path: &Path) -> impl FnOnce(std::io::Error) -> RawStoreError {
    let context = format!("{action} {}", path.display());
    move |error| RawStoreError::Io(format!("{context}: {error}"))
}

/// Fsyncs a directory, making the entries created or renamed in it durable.
fn sync_dir(dir: &Path) -> Result<(), RawStoreError> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(io_error("sync directory", dir))
}

/// Names of the entries of `dir`, sorted, so every walk is deterministic.
/// A missing directory has no entries. Names that are not UTF-8 are skipped:
/// the store never creates them.
fn sorted_entries(dir: &Path) -> Result<Vec<String>, RawStoreError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error("list", dir)(error)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(io_error("list", dir))?;
        if let Ok(name) = entry.file_name().into_string() {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}
