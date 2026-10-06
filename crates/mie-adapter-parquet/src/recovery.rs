//! Crash recovery: finishes or discards interrupted seals (ADR-030 D5).
//!
//! Runs under the source lock whenever a writer opens, partition by
//! partition in sorted order. It is idempotent: a second run finds nothing
//! to do. Per part number:
//!
//! - final data plus a pending manifest: verify the hash, roll forward;
//! - data temp file plus a valid, matching pending manifest: verify the hash,
//!   roll forward;
//! - data temp file without one: delete both, report them as discarded;
//! - orphan pending manifest: delete it;
//! - a manifest without its data, data without any manifest, or a hash
//!   mismatch during roll-forward: [`RawStoreError::Integrity`], and nothing
//!   is deleted.
//!
//! Files the store does not own are ignored.

use crate::layout::{self, PartFile};
use crate::manifest;
use crate::{io_error, sorted_entries, sync_dir};
use mie_ports::raw::{RawStoreError, RawStreamKey, SealedFile, validate_segment};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

/// What recovery did when a writer opened.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Relative paths of the parts whose interrupted seal was completed.
    pub rolled_forward: Vec<String>,
    /// Relative paths and sizes of the unsealed data temp files deleted. The
    /// records in them were never sealed; live capture reports the loss as a
    /// feed gap (#9).
    pub discarded: Vec<(String, u64)>,
}

impl RecoveryReport {
    /// Whether recovery found nothing to do.
    pub fn is_clean(&self) -> bool {
        self.rolled_forward.is_empty() && self.discarded.is_empty()
    }
}

/// Recovers every partition of `source` below `root`.
pub(crate) fn recover(root: &Path, source: &str) -> Result<RecoveryReport, RawStoreError> {
    let mut report = RecoveryReport::default();
    let source_dir = root.join(layout::source_dir(source));
    for instrument in segment_dirs(&source_dir, "instrument=")? {
        let instrument_dir = source_dir.join(format!("instrument={instrument}"));
        for stream in segment_dirs(&instrument_dir, "stream=")? {
            let key = RawStreamKey::new(source, &instrument, &stream)?;
            let stream_dir = instrument_dir.join(format!("stream={stream}"));
            for name in sorted_entries(&stream_dir)? {
                let Some(date) = name.strip_prefix("date=") else {
                    continue;
                };
                if layout::parse_date(date).is_some() && stream_dir.join(&name).is_dir() {
                    recover_partition(root, &key, date, &mut report)?;
                }
            }
        }
    }
    Ok(report)
}

/// The values of the `<prefix><value>` subdirectories of `dir` whose value
/// is a valid segment, sorted.
fn segment_dirs(dir: &Path, prefix: &str) -> Result<Vec<String>, RawStoreError> {
    Ok(sorted_entries(dir)?
        .into_iter()
        .filter_map(|name| name.strip_prefix(prefix).map(str::to_owned))
        .filter(|value| validate_segment(value).is_ok())
        .filter(|value| dir.join(format!("{prefix}{value}")).is_dir())
        .collect())
}

fn recover_partition(
    root: &Path,
    key: &RawStreamKey,
    date: &str,
    report: &mut RecoveryReport,
) -> Result<(), RawStoreError> {
    let partition_rel = layout::partition_dir(key, date);
    let partition = layout::resolve(root, &partition_rel);
    let mut parts: BTreeMap<u32, BTreeSet<PartFile>> = BTreeMap::new();
    for name in sorted_entries(&partition)? {
        if let Some((kind, part)) = PartFile::classify(&name) {
            parts.entry(part).or_default().insert(kind);
        }
    }
    for (part, kinds) in parts {
        let path = |kind: PartFile| partition.join(kind.name(part));
        let rel = |kind: PartFile| format!("{partition_rel}/{}", kind.name(part));
        let has = |kind: PartFile| kinds.contains(&kind);

        if has(PartFile::Manifest) {
            if !has(PartFile::Data) {
                return Err(RawStoreError::Integrity(format!(
                    "{} has no data file",
                    rel(PartFile::Manifest)
                )));
            }
            // Sealed. The protocol leaves no temp file next to a sealed
            // part; any found is debris and is removed.
            if has(PartFile::DataTmp) {
                discard(&path(PartFile::DataTmp), rel(PartFile::DataTmp), report)?;
            }
            if has(PartFile::ManifestTmp) {
                remove(&path(PartFile::ManifestTmp))?;
            }
        } else if has(PartFile::Data) {
            // Crash between the two renames: the pending manifest must vouch
            // for the visible data file.
            let pending = has(PartFile::ManifestTmp)
                .then(|| pending_manifest(&path(PartFile::ManifestTmp), key, date, part))
                .flatten()
                .ok_or_else(|| {
                    RawStoreError::Integrity(format!(
                        "{} has no valid manifest",
                        rel(PartFile::Data)
                    ))
                })?;
            verify_file(&path(PartFile::Data), &pending)?;
            rename_no_replace(&path(PartFile::ManifestTmp), &path(PartFile::Manifest))?;
            if has(PartFile::DataTmp) {
                discard(&path(PartFile::DataTmp), rel(PartFile::DataTmp), report)?;
            }
            sync_dir(&partition)?;
            report.rolled_forward.push(rel(PartFile::Data));
        } else if has(PartFile::DataTmp) {
            let pending = has(PartFile::ManifestTmp)
                .then(|| pending_manifest(&path(PartFile::ManifestTmp), key, date, part))
                .flatten();
            if let Some(pending) = pending {
                // The manifest was durable: the data was complete and fsynced
                // before it, so the seal is finished.
                verify_file(&path(PartFile::DataTmp), &pending)?;
                mark_read_only(&path(PartFile::DataTmp))?;
                mark_read_only(&path(PartFile::ManifestTmp))?;
                rename_no_replace(&path(PartFile::DataTmp), &path(PartFile::Data))?;
                rename_no_replace(&path(PartFile::ManifestTmp), &path(PartFile::Manifest))?;
                report.rolled_forward.push(rel(PartFile::Data));
            } else {
                discard(&path(PartFile::DataTmp), rel(PartFile::DataTmp), report)?;
                if has(PartFile::ManifestTmp) {
                    remove(&path(PartFile::ManifestTmp))?;
                }
            }
            sync_dir(&partition)?;
        } else if has(PartFile::ManifestTmp) {
            remove(&path(PartFile::ManifestTmp))?;
            sync_dir(&partition)?;
        }
    }
    Ok(())
}

/// Parses a pending manifest; `None` when it is unreadable, malformed or
/// describes another part (an interrupted write of the manifest itself).
fn pending_manifest(path: &Path, key: &RawStreamKey, date: &str, part: u32) -> Option<SealedFile> {
    let text = fs::read_to_string(path).ok()?;
    manifest::parse(&text)
        .ok()
        .filter(|m| m.stream == *key && m.date == date && m.part == part)
}

/// Checks a data file's size and SHA-256 against its manifest.
fn verify_file(path: &Path, expected: &SealedFile) -> Result<(), RawStoreError> {
    let data = fs::read(path).map_err(io_error("read", path))?;
    manifest::verify(&data, expected)
}

/// Deletes an unsealed data temp file and reports it.
fn discard(path: &Path, rel: String, report: &mut RecoveryReport) -> Result<(), RawStoreError> {
    let size = fs::metadata(path).map_err(io_error("inspect", path))?.len();
    remove(path)?;
    report.discarded.push((rel, size));
    Ok(())
}

fn remove(path: &Path) -> Result<(), RawStoreError> {
    fs::remove_file(path).map_err(io_error("remove", path))
}

/// Clears every write permission of a file.
pub(crate) fn mark_read_only(path: &Path) -> Result<(), RawStoreError> {
    let mut permissions = fs::metadata(path)
        .map_err(io_error("inspect", path))?
        .permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions).map_err(io_error("protect", path))
}

/// Renames `from` to `to`, refusing to replace an existing `to`: a sealed
/// file is never overwritten. The writer lock excludes concurrent writers,
/// so checking first is enough.
pub(crate) fn rename_no_replace(from: &Path, to: &Path) -> Result<(), RawStoreError> {
    if fs::symlink_metadata(to).is_ok() {
        return Err(RawStoreError::Integrity(format!(
            "refusing to replace existing {}",
            to.display()
        )));
    }
    fs::rename(from, to).map_err(io_error("rename", from))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{TempDir, stream};

    fn touch(dir: &Path, name: &str, data: &[u8]) {
        fs::create_dir_all(dir).unwrap();
        fs::write(dir.join(name), data).unwrap();
    }

    #[test]
    fn unknown_files_are_ignored_and_orphans_removed() {
        let dir = TempDir::new("recovery-orphans");
        let key = stream("aggTrade");
        let partition = layout::resolve(dir.path(), &layout::partition_dir(&key, "2026-10-06"));
        touch(&partition, "README", b"keep");
        touch(&partition, ".part-00003.manifest.tmp", b"half a manif");
        touch(&dir.path().join("source=binance-um/notes"), "x", b"keep");
        touch(
            &dir.path()
                .join("source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=bad"),
            "part-00000.manifest",
            b"",
        );

        let report = recover(dir.path(), "binance-um").unwrap();
        assert!(report.is_clean());
        assert_eq!(
            sorted_entries(&partition).unwrap(),
            vec!["README".to_owned()]
        );
    }

    #[test]
    fn missing_data_or_manifest_is_an_integrity_error() {
        for (name, data) in [
            ("part-00000.manifest", &b"x"[..]),
            ("part-00000.parquet", b"PAR1"),
        ] {
            let dir = TempDir::new("recovery-integrity");
            let key = stream("aggTrade");
            let partition = layout::resolve(dir.path(), &layout::partition_dir(&key, "2026-10-06"));
            touch(&partition, name, data);
            assert!(
                matches!(
                    recover(dir.path(), "binance-um"),
                    Err(RawStoreError::Integrity(_))
                ),
                "{name}"
            );
            // Nothing is deleted.
            assert_eq!(sorted_entries(&partition).unwrap(), vec![name.to_owned()]);
        }
    }
}
