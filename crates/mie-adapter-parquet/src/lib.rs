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
//!   reads each file only after verifying it against its manifest. It is a
//!   single-root store ([`ParquetRawStore::new`]) or a tiered one
//!   ([`ParquetRawStore::tiered`], below); with no cold root the two are the
//!   same value and run the same code.
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
//!
//! A tiered store (ADR-062) adds two on-disk formats, both under a root's
//! `.mie/` directory, outside the layout:
//!
//! - [`root_marker`]: each root's id, written once when the root is
//!   initialized, and the atomic creation of `.mie/`;
//! - [`copy_ledger`] ([`CopyLedger`]): hot's append-only, hash-chained
//!   record of copies, removals and verification passes, one appender at a
//!   time under a lock file, readers lock-free.
//!
//! [`replica`] holds the copy primitives of the replication pass (ADR-062
//! D5, D11): the D5 copy of a sealed part under the writer's own temp names
//! and step order, the copy of an immutable file, the tail append of an
//! append-only file and the pass lock. `mie-cli` orchestrates the pass.
//!
//! [`verification`] folds one ledger read into the copy state that the hot
//! removal, the replication pass, `mie store verify` and `mie store repair`
//! all read (ADR-062 D6, D11).
//!
//! [`rebuilt`] is the gate `mie store rebuild` leaves on hot: the hot
//! removal waits until a verification of every copy target covers the
//! rebuilt ledger (ADR-062 D7 step 6).
//!
//! With no cold root configured none of them is ever created.
//!
//! # Tiered store
//!
//! A tiered store (ADR-062 D7) is hot (the writer's root) plus an ordered
//! list of cold roots, with hot's copy ledger as the listing of everything
//! that left hot. Its `select` lists hot completely first, then reads the
//! copy ledger from disk, fresh for every call, merges both by relative
//! path and checks them (D5, the lost, held and quarantined files of D11)
//! before the event-time filter. It never lists a cold root, so a hung or
//! unmounted cold mount cannot block it or shorten a selection. Its `read`
//! tries hot, then each cold root in order, skips a root only when the file
//! is absent there, and verifies the first copy it finds; a bad copy is an
//! error, never a fallback to the next one. A file absent from every root
//! and a missing copy ledger are both `Integrity` errors.
//!
//! A tiered store's writer ([`ParquetRawStore::tiered_writer`], ADR-062
//! D6) reads the copy ledger once when it opens. It numbers a new part from
//! the union of hot and the ledger (lost, held and quarantined entries
//! included), so a path is never reused; an append to a partition the
//! ledger records as removed from hot first **recalls** it, copying every
//! sealed file back from a cold copy ([`replica::copy_back`]) and appending
//! `returned`; without a readable ledger it is the degraded writer, which
//! opens only hot-window dates. [`removal`] plans and carries out the hot
//! removal: a partition before the hot window leaves hot once the ledger
//! records k verified copies of every file in it inside the prefix k
//! ledger copies hold, under the source's writer lock and the ledger lock,
//! with its `removed` record before the rename to a hidden name.
//!
//! Once partitions leave hot, an ad-hoc DuckDB query reads a copy target
//! instead of hot, since a copy target holds every file
//! (`<copy target>/source=…/*/*.parquet`) and hot only the recent dates.
//!
//! [`outcome`] writes the conditional outcome dataset of #27 (ADR-057): the
//! context records and forward labels of one labeling run as three
//! content-hashed Parquet tables, sealed with the same protocol and read
//! back only after verification.
//!
//! [`flock`] holds every store lock, and its guard unlocks explicitly when
//! dropped, so a child process another thread starts cannot keep a dropped
//! lock held until its exec (#220).

pub mod copy_ledger;
pub mod flock;
mod layout;
mod manifest;
pub mod outcome;
mod reader;
pub mod rebuilt;
mod recovery;
pub mod removal;
pub mod replica;
pub mod root_marker;
mod schema;
mod tiers;
pub mod verification;
mod writer;

#[cfg(test)]
mod testutil;

pub use copy_ledger::{CopyLedger, LedgerHold};
pub use reader::{ImportView, ParquetRawStore};
pub use recovery::RecoveryReport;
pub use writer::{RawWriter, RecallEvent, RecallObserver, RotationPolicy, TierContext};

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
    list_entries(dir).map(Option::unwrap_or_default)
}

/// [`sorted_entries`], but `None` for a missing directory, so a caller can
/// tell a directory that vanished from an empty one (the tiered `select`,
/// ADR-062 D7).
fn list_entries(dir: &Path) -> Result<Option<Vec<String>>, RawStoreError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
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
    Ok(Some(names))
}
