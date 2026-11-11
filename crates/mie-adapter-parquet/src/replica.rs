//! The copy primitives of a tiered raw store (ADR-062 D5, D11).
//!
//! Format-agnostic building blocks of the replication pass, which
//! `mie-cli` orchestrates: this module never parses the journal or the
//! import ledger (ADR-025).
//!
//! - [`list_sealed`]: the sealed parts of a root's layout;
//! - [`list_unsealed`]: the seal debris of a root's layout;
//! - [`copy_sealed`]: the D5 copy of one sealed part;
//! - [`copy_back`]: the copy of one sealed part from the first cold root
//!   with a verified copy back onto hot (the recall, the hot restore);
//! - [`copy_immutable`]: the copy of an immutable small file (an
//!   import-ledger entry);
//! - [`extend_prefix`]: the D11 tail append of a line-structured,
//!   append-only file (the journal, the copy ledger);
//! - [`try_replication_lock`]: the pass lock on hot;
//! - [`check_sealed_copy`]: the verification's classification of one copy
//!   against the copy ledger's manifest text (#192);
//! - [`move_aside`], [`rebuild_prefix`]: the repair's move of a damaged
//!   copy into `.mie/damaged/` and its rebuild of a tail copy (#192); the
//!   replace's move of a superseded part into [`quarantine_dir`] (#211);
//! - [`list_quarantine`]: a root's `.quarantine/` directory, as the
//!   rebuild reads it (ADR-062 D7 step 5a, #211);
//! - [`first_difference`], [`line_start`]: where a tail copy first differs
//!   from hot's file, and where the line holding a byte starts;
//! - [`try_verify_lock`]: the verification lock on hot (#192).
//!
//! Every primitive takes its source and destination explicitly, so the
//! adoption of another store, the recall onto hot and the repair reuse them
//! unchanged. The caller guarantees that no writer writes the destination
//! partition: a copy target has none, and on hot the caller holds the
//! source lock.
//!
//! # The copy of a sealed part
//!
//! The copy uses the writer's own temp names (`.part-NNNNN.parquet.tmp`,
//! `.part-NNNNN.manifest.tmp`) and the seal's order of steps: data, then
//! the manifest, both fsynced; both read back and verified; both
//! read-only; the data renamed into place, then the manifest, then the
//! directory fsynced. A crashed copy on any root therefore leaves exactly
//! the intermediate states of a crashed seal, and ADR-030 recovery already
//! defines each of them: a data temp file alone is discarded, a data temp
//! file or published data with a pending manifest is rolled forward. That
//! matters once a copy lands on hot (the recall and the hot restore). Copy
//! temp names of their own would be debris that writer recovery ignores.
//!
//! The bytes are streamed verbatim through a bounded buffer: a copy never
//! recompresses, merges parts or re-encodes (D5).

use crate::layout::{self, DAMAGED_DIR, META_DIR, PartFile, REPLICATE_LOCK, VERIFY_LOCK};
use crate::manifest::{self, hex};
use crate::recovery::{mark_read_only, rename_no_replace};
use crate::{io_error, list_entries, sync_dir};
use mie_ports::raw::{RawStoreError, RawStreamKey, SealedFile, validate_segment};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// The bytes streamed per read and write. Unit tests use a buffer shorter
/// than a test part, so every copy streams through several chunks.
const CHUNK: usize = if cfg!(test) { 97 } else { 1 << 20 };

/// What a copy did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyOutcome {
    /// The destination now holds the file; this call published it.
    Copied,
    /// The destination already held a byte-identical copy.
    AlreadyPresent,
}

/// The side of a copy that failed: a failure to read the source (hot) is
/// about one file, a failure on the destination is about the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// Reading the source failed, or the source does not match its own
    /// manifest.
    Source,
    /// Writing, reading or checking the destination failed.
    Destination,
}

/// A failed copy and the side it failed on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyError {
    /// Where it failed.
    pub side: Side,
    /// Why.
    pub error: RawStoreError,
}

impl CopyError {
    /// A failure on the source side.
    pub fn source(error: RawStoreError) -> Self {
        Self {
            side: Side::Source,
            error,
        }
    }
}

impl From<RawStoreError> for CopyError {
    /// A failure on the destination side.
    fn from(error: RawStoreError) -> Self {
        Self {
            side: Side::Destination,
            error,
        }
    }
}

impl std::fmt::Display for CopyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let side = match self.side {
            Side::Source => "source",
            Side::Destination => "destination",
        };
        write!(f, "{side}: {}", self.error)
    }
}

impl std::error::Error for CopyError {}

/// The steps of [`copy_sealed`] and [`copy_immutable`], in order. Tests
/// stop a copy after any of them to simulate a crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyStep {
    /// The data temp file is written and fsynced.
    WriteData,
    /// The manifest temp file is written and fsynced, and so is the
    /// directory.
    WriteManifest,
    /// Both temp files are read back and verified.
    Verify,
    /// Both temp files are read-only.
    MarkReadOnly,
    /// The data file has its final name.
    PublishData,
    /// The manifest has its final name: the copy is sealed.
    PublishManifest,
    /// The directory is fsynced.
    SyncDir,
}

#[cfg(test)]
impl CopyStep {
    /// Every step, in protocol order.
    pub(crate) const ALL: [Self; 7] = [
        Self::WriteData,
        Self::WriteManifest,
        Self::Verify,
        Self::MarkReadOnly,
        Self::PublishData,
        Self::PublishManifest,
        Self::SyncDir,
    ];
}

/// The steps of [`extend_prefix`]. Tests stop an append after either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailStep {
    /// Part of the missing bytes is written, not all of them.
    PartialWrite,
    /// The appended bytes are written and fsynced, before the read-back.
    Sync,
}

#[cfg(test)]
thread_local! {
    /// Test hook: the next copy on this thread stops after this step.
    pub(crate) static CRASH_AFTER: std::cell::Cell<Option<CopyStep>> =
        const { std::cell::Cell::new(None) };
    /// Test hook: the next tail append on this thread stops after this
    /// step.
    pub(crate) static TAIL_CRASH_AFTER: std::cell::Cell<Option<TailStep>> =
        const { std::cell::Cell::new(None) };
}

/// Marks the end of a copy step; under test, simulates a crash there once.
fn reached(step: CopyStep) -> Result<(), RawStoreError> {
    #[cfg(test)]
    if CRASH_AFTER.with(|crash| crash.get() == Some(step)) {
        CRASH_AFTER.with(|crash| crash.set(None));
        return Err(RawStoreError::Io(format!("simulated crash after {step:?}")));
    }
    let _ = step;
    Ok(())
}

/// Marks the end of a tail-append step; under test, simulates a crash
/// there once.
fn tail_reached(step: TailStep) -> Result<(), RawStoreError> {
    #[cfg(test)]
    if TAIL_CRASH_AFTER.with(|crash| crash.get() == Some(step)) {
        TAIL_CRASH_AFTER.with(|crash| crash.set(None));
        return Err(RawStoreError::Io(format!("simulated crash after {step:?}")));
    }
    let _ = step;
    Ok(())
}

/// The relative data paths of every sealed part in the layout of `root`
/// (`source=*/instrument=*/stream=*/date=*/part-NNNNN.manifest`), sorted.
///
/// `.mie/`, `.quarantine/` and every name outside the layout are skipped.
/// A partition or a manifest that vanishes during the walk is skipped:
/// only the hot removal removes, and a removed partition already has its k
/// copies (ADR-062 D6).
///
/// # Errors
///
/// [`RawStoreError::Io`] when a directory cannot be listed.
pub fn list_sealed(root: &Path) -> Result<Vec<String>, RawStoreError> {
    let mut paths = Vec::new();
    for_each_partition(root, |key, date, names| {
        for name in names {
            if let Some((PartFile::Manifest, part)) = PartFile::classify(name) {
                paths.push(layout::file_path(key, date, PartFile::Data, part));
            }
        }
    })?;
    // The walk sorts each directory level, which is not the order of the
    // whole paths (`stream=a-b/` sorts before `stream=a/`).
    paths.sort();
    Ok(paths)
}

/// The relative paths of the seal debris in the layout of `root`, sorted:
/// every data temp file (`.part-NNNNN.parquet.tmp`), every pending manifest
/// (`.part-NNNNN.manifest.tmp`) and every data file without its manifest.
///
/// These are the states of an interrupted seal or copy that ADR-030
/// recovery rolls forward or discards, and that [`list_sealed`] skips
/// silently. The adoption of another store (`mie store adopt`, ADR-062
/// D10) refuses a source that holds any: it never writes the source, so it
/// cannot recover it, and copying only the sealed parts would leave the
/// rest behind without a word. The walk is [`list_sealed`]'s.
///
/// # Errors
///
/// [`RawStoreError::Io`] when a directory cannot be listed.
pub fn list_unsealed(root: &Path) -> Result<Vec<String>, RawStoreError> {
    let mut paths = Vec::new();
    for_each_partition(root, |key, date, names| {
        let sealed: Vec<u32> = names
            .iter()
            .filter_map(|name| match PartFile::classify(name) {
                Some((PartFile::Manifest, part)) => Some(part),
                _ => None,
            })
            .collect();
        for name in names {
            let debris = match PartFile::classify(name) {
                Some((PartFile::DataTmp | PartFile::ManifestTmp, _)) => true,
                Some((PartFile::Data, part)) => !sealed.contains(&part),
                Some((PartFile::Manifest, _)) | None => false,
            };
            if debris {
                paths.push(format!("{}/{name}", layout::partition_dir(key, date)));
            }
        }
    })?;
    paths.sort();
    Ok(paths)
}

/// Calls `visit` with the stream, the date and the sorted entry names of
/// every partition in the layout of `root` (the walk of [`list_sealed`]).
fn for_each_partition(
    root: &Path,
    mut visit: impl FnMut(&RawStreamKey, &str, &[String]),
) -> Result<(), RawStoreError> {
    for source in layout_values(root, "source=")? {
        let source_dir = root.join(format!("source={source}"));
        for instrument in layout_values(&source_dir, "instrument=")? {
            let instrument_dir = source_dir.join(format!("instrument={instrument}"));
            for stream in layout_values(&instrument_dir, "stream=")? {
                let Ok(key) = RawStreamKey::new(&source, &instrument, &stream) else {
                    continue;
                };
                let stream_dir = instrument_dir.join(format!("stream={stream}"));
                for date in layout_values(&stream_dir, "date=")? {
                    if layout::parse_date(&date).is_none() {
                        continue;
                    }
                    let partition = layout::resolve(root, &layout::partition_dir(&key, &date));
                    let Some(names) = list_entries(&partition)? else {
                        continue;
                    };
                    visit(&key, &date, &names);
                }
            }
        }
    }
    Ok(())
}

/// The values of the `<prefix><value>` subdirectories of `dir` whose value
/// is a valid segment, sorted. A missing directory, or an entry that
/// vanishes before it is inspected, has none.
pub(crate) fn layout_values(dir: &Path, prefix: &str) -> Result<Vec<String>, RawStoreError> {
    let Some(names) = list_entries(dir)? else {
        return Ok(Vec::new());
    };
    let mut values = Vec::new();
    for name in names {
        let Some(value) = name.strip_prefix(prefix) else {
            continue;
        };
        if validate_segment(value).is_err() {
            continue;
        }
        let path = dir.join(&name);
        match fs::metadata(&path) {
            Ok(metadata) if metadata.is_dir() => values.push(value.to_owned()),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("inspect", &path)(error)),
        }
    }
    Ok(values)
}

/// The manifest of the sealed part `data_path` on `root`: its text
/// verbatim and its parse, checked to describe its own location, as the
/// tiered `select` does. `None` when the manifest does not exist.
///
/// # Errors
///
/// [`RawStoreError::Invalid`] for a path outside the layout;
/// [`RawStoreError::Corrupt`] for a malformed or misplaced manifest;
/// [`RawStoreError::Io`] when it cannot be read.
pub fn sealed_manifest(
    root: &Path,
    data_path: &str,
) -> Result<Option<(String, SealedFile)>, RawStoreError> {
    let (key, date, part) = layout::parse_data_path(data_path)?;
    let path = layout::resolve(
        root,
        &layout::file_path(&key, &date, PartFile::Manifest, part),
    );
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("read", &path)(error)),
    };
    let file = manifest::parse(&text).map_err(|error| match error {
        RawStoreError::Corrupt(detail) => {
            RawStoreError::Corrupt(format!("{}: {detail}", path.display()))
        }
        other => other,
    })?;
    if file.stream != key || file.date != date || file.part != part {
        return Err(RawStoreError::Corrupt(format!(
            "manifest {} describes {}, not its own location",
            path.display(),
            file.relative_path
        )));
    }
    Ok(Some((text, file)))
}

/// Parses a manifest text strictly (ADR-030): the text of a copy-ledger
/// `manifest` record, say, whose data path the pass needs.
///
/// # Errors
///
/// [`RawStoreError::Corrupt`] for anything the manifest renderer cannot
/// produce.
pub fn parse_manifest(text: &str) -> Result<SealedFile, RawStoreError> {
    manifest::parse(text)
}

/// Verifies the data of the sealed part `file` on `root` against its
/// manifest, size first, then SHA-256, streamed through a bounded buffer.
///
/// The adoption of another store (ADR-062 D10) checks every source part
/// this way before it writes anything, so a damaged source is refused
/// with nothing written rather than found mid-run.
///
/// # Errors
///
/// [`RawStoreError::Integrity`] when the data file is missing or does not
/// match; [`RawStoreError::Io`] when it cannot be read.
pub fn verify_sealed(root: &Path, file: &SealedFile) -> Result<(), RawStoreError> {
    let path = layout::resolve(root, &file.relative_path);
    match matches_manifest(&path, file) {
        Ok(true) => Ok(()),
        Ok(false) => Err(RawStoreError::Integrity(format!(
            "{} on {}: the data does not match its manifest",
            file.relative_path,
            root.display()
        ))),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            Err(RawStoreError::Integrity(format!(
                "{} on {}: the manifest is present without its data",
                file.relative_path,
                root.display()
            )))
        }
        Err(error) => Err(io_error("read", &path)(error)),
    }
}

/// The size and SHA-256 of a file, streamed through a bounded buffer.
fn hash_file(path: &Path) -> std::io::Result<(u64, String)> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; CHUNK];
    let mut bytes = 0;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
        bytes += n as u64;
    }
    Ok((bytes, hex(&hasher.finalize())))
}

/// Checks the file at `path` against its manifest, size first, then
/// SHA-256. `Ok(false)` on a mismatch; an absent file is an I/O error.
fn matches_manifest(path: &Path, expected: &SealedFile) -> std::io::Result<bool> {
    hash_file(path).map(|(bytes, sha)| bytes == expected.bytes && sha == expected.sha256)
}

/// Whether `path` exists (any entry, a dangling symlink included).
fn exists(path: &Path) -> Result<bool, RawStoreError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("inspect", path)(error)),
    }
}

/// Removes `path` if it exists.
fn remove_if_present(path: &Path) -> Result<(), RawStoreError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("remove", path)(error)),
    }
}

/// Creates `dir` and every missing parent, fsyncing each parent whose
/// entry was created. An existing ancestor ends the walk: a copy target's
/// root is never created here.
fn ensure_dir(dir: &Path) -> Result<(), RawStoreError> {
    if exists(dir)? {
        return Ok(());
    }
    let parent = dir
        .parent()
        .ok_or_else(|| RawStoreError::Io(format!("{} has no parent", dir.display())))?;
    ensure_dir(parent)?;
    match fs::create_dir(dir) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::AlreadyExists => return Ok(()),
        Err(error) => return Err(io_error("create", dir)(error)),
    }
    sync_dir(parent)
}

/// Writes `text` to the new file `path` and fsyncs it.
fn write_new(path: &Path, bytes: &[u8]) -> Result<(), RawStoreError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io_error("create", path))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(io_error("write", path))
}

/// Copies the sealed part `data_path` from `source_root` to `dest_root` by
/// the D5 protocol (module docs) and returns the outcome and the manifest
/// text, verbatim.
///
/// 1. The source manifest is read and must describe its own location.
/// 2. The destination's state decides: a manifest present must equal the
///    source's byte for byte and its data must verify, giving
///    [`CopyOutcome::AlreadyPresent`]; anything else is
///    [`RawStoreError::Integrity`]. Data present without a manifest (a copy
///    that crashed between the two renames) is verified against the source
///    manifest and the copy publishes the manifest; data that fails is
///    `Integrity`, and the file is never overwritten. Temp debris of a
///    crashed copy is removed.
/// 3. The missing directories are created, each parent whose entry was
///    created fsynced.
/// 4. The source data is streamed into the data temp file and fsynced
///    ([`CopyStep::WriteData`]); a source that does not match its own
///    manifest is `Integrity` naming the source root, and nothing is
///    published.
/// 5. The manifest temp file gets the source text verbatim and is
///    fsynced, and so is the directory ([`CopyStep::WriteManifest`]).
/// 6. Both temp files are read back and verified ([`CopyStep::Verify`])
///    and marked read-only ([`CopyStep::MarkReadOnly`]).
/// 7. The data is renamed into place ([`CopyStep::PublishData`]), then the
///    manifest ([`CopyStep::PublishManifest`]), then the directory is
///    fsynced ([`CopyStep::SyncDir`]).
///
/// # Errors
///
/// A [`CopyError`] on [`Side::Source`] for a path outside the layout
/// ([`RawStoreError::Invalid`]), a source without its manifest or not
/// matching it ([`RawStoreError::Integrity`]), a malformed or misplaced
/// source manifest ([`RawStoreError::Corrupt`]) and a source that cannot be
/// read ([`RawStoreError::Io`]); on [`Side::Destination`] for a destination
/// that differs ([`RawStoreError::Integrity`], naming the root and the
/// path) and any I/O failure there.
pub fn copy_sealed(
    source_root: &Path,
    dest_root: &Path,
    data_path: &str,
) -> Result<(CopyOutcome, String), CopyError> {
    let (key, date, part) = layout::parse_data_path(data_path).map_err(CopyError::source)?;
    let (text, file) = sealed_manifest(source_root, data_path)
        .map_err(CopyError::source)?
        .ok_or_else(|| {
            CopyError::source(RawStoreError::Integrity(format!(
                "{data_path}: the source root {} holds no manifest for it",
                source_root.display()
            )))
        })?;
    let source_data = layout::resolve(source_root, data_path);
    let partition = layout::resolve(dest_root, &layout::partition_dir(&key, &date));
    let data_final = partition.join(PartFile::Data.name(part));
    let manifest_final = partition.join(PartFile::Manifest.name(part));
    let data_tmp = partition.join(PartFile::DataTmp.name(part));
    let manifest_tmp = partition.join(PartFile::ManifestTmp.name(part));
    let differs = |what: &str| {
        RawStoreError::Integrity(format!(
            "{data_path} on {}: {what}; the same path must be byte-identical on every root \
             (ADR-062 D5)",
            dest_root.display()
        ))
    };
    let verify_dest_data = || match matches_manifest(&data_final, &file) {
        Ok(true) => Ok(()),
        Ok(false) => Err(differs("its data does not match the manifest")),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            Err(differs("its manifest is present without its data"))
        }
        Err(error) => Err(io_error("read", &data_final)(error)),
    };

    if exists(&manifest_final)? {
        let present = fs::read(&manifest_final).map_err(io_error("read", &manifest_final))?;
        if present != text.as_bytes() {
            return Err(differs("its manifest differs from the source's").into());
        }
        verify_dest_data()?;
        remove_if_present(&data_tmp)?;
        remove_if_present(&manifest_tmp)?;
        return Ok((CopyOutcome::AlreadyPresent, text));
    }

    if exists(&data_final)? {
        // A copy that crashed between the two renames: the data must vouch
        // for itself before its manifest is published.
        verify_dest_data()?;
        remove_if_present(&data_tmp)?;
        remove_if_present(&manifest_tmp)?;
        publish_manifest(&partition, &manifest_tmp, &manifest_final, &text)?;
        return Ok((CopyOutcome::Copied, text));
    }

    // Debris of a crashed copy; the copy starts over.
    remove_if_present(&data_tmp)?;
    remove_if_present(&manifest_tmp)?;
    ensure_dir(&partition)?;

    stream_copy(&source_data, &data_tmp, &file, source_root, data_path)?;
    reached(CopyStep::WriteData)?;

    write_new(&manifest_tmp, text.as_bytes())?;
    sync_dir(&partition)?;
    reached(CopyStep::WriteManifest)?;

    let data_ok = matches_manifest(&data_tmp, &file).map_err(io_error("read", &data_tmp))?;
    let manifest_ok =
        fs::read(&manifest_tmp).map_err(io_error("read", &manifest_tmp))? == text.as_bytes();
    if !data_ok || !manifest_ok {
        remove_if_present(&data_tmp)?;
        remove_if_present(&manifest_tmp)?;
        return Err(RawStoreError::Integrity(format!(
            "{data_path} on {}: the copy read back does not match the source; nothing was \
             published",
            dest_root.display()
        ))
        .into());
    }
    reached(CopyStep::Verify)?;

    mark_read_only(&data_tmp)?;
    mark_read_only(&manifest_tmp)?;
    reached(CopyStep::MarkReadOnly)?;

    rename_no_replace(&data_tmp, &data_final)?;
    reached(CopyStep::PublishData)?;

    rename_no_replace(&manifest_tmp, &manifest_final)?;
    reached(CopyStep::PublishManifest)?;

    sync_dir(&partition)?;
    reached(CopyStep::SyncDir)?;
    Ok((CopyOutcome::Copied, text))
}

/// A cold copy that [`copy_back`] found damaged and passed over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DamagedCopy {
    /// The cold root that holds it.
    pub root: PathBuf,
    /// What is wrong with it.
    pub error: RawStoreError,
}

/// What [`copy_back`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopiedBack {
    /// The cold root the file was copied from.
    pub root: PathBuf,
    /// [`CopyOutcome::AlreadyPresent`] when hot already held it (a crashed
    /// recall).
    pub outcome: CopyOutcome,
    /// The damaged copies passed over before it, in read order.
    pub damaged: Vec<DamagedCopy>,
}

/// Copies the sealed file `data_path` back onto `hot` by the D5 copy
/// ([`copy_sealed`]), from the first cold root in `cold_roots` (read order)
/// that holds a verified copy: the recall of ADR-062 D6, and the hot
/// restore's step 3 (D11). `ledger_text` is the copy ledger's manifest
/// text of the path. The caller holds the source's writer lock.
///
/// Per root, in order:
///
/// - no manifest there: the copy is absent, and the next root is tried, as
///   in the tiered `read`;
/// - a manifest whose text differs from `ledger_text`: two versions of one
///   path (D5), [`RawStoreError::Integrity`], and no other root is tried;
/// - data that fails its manifest, or a copy that cannot be read: a damaged
///   copy. It is handed to `observe` at once and the next root is tried
///   (#191 STOP 1 decision 1: a late import never stops on a fault a good
///   copy already covers; #192's verification finds the damage again);
/// - otherwise the copy onto hot. A copy hot already holds (a crashed
///   recall) is [`CopyOutcome::AlreadyPresent`].
///
/// # Errors
///
/// [`RawStoreError::Integrity`] for a differing manifest, and when no root
/// holds a verified copy, naming the file and every root tried; any error
/// on hot's side of the copy as [`copy_sealed`] gives it.
pub fn copy_back(
    cold_roots: &[PathBuf],
    hot: &Path,
    data_path: &str,
    ledger_text: &str,
    observe: &mut dyn FnMut(&DamagedCopy),
) -> Result<CopiedBack, RawStoreError> {
    let (key, date, part) = layout::parse_data_path(data_path)?;
    let manifest_rel = layout::file_path(&key, &date, PartFile::Manifest, part);
    let mut damaged = Vec::new();
    for root in cold_roots {
        let path = layout::resolve(root, &manifest_rel);
        let mut damage = |error: RawStoreError, damaged: &mut Vec<DamagedCopy>| {
            let copy = DamagedCopy {
                root: root.clone(),
                error,
            };
            observe(&copy);
            damaged.push(copy);
        };
        match fs::read(&path) {
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => {
                damage(io_error("read", &path)(error), &mut damaged);
                continue;
            }
            Ok(text) if text != ledger_text.as_bytes() => {
                return Err(RawStoreError::Integrity(format!(
                    "{data_path} on {}: its manifest differs from the copy ledger's manifest \
                     record; the same path must be byte-identical on every root (ADR-062 D5), \
                     so the recall stops",
                    root.display()
                )));
            }
            Ok(_) => {}
        }
        match copy_sealed(root, hot, data_path) {
            Ok((outcome, _)) => {
                return Ok(CopiedBack {
                    root: root.clone(),
                    outcome,
                    damaged,
                });
            }
            // The cold copy fails its manifest or cannot be read.
            Err(CopyError {
                side: Side::Source,
                error,
            }) => damage(error, &mut damaged),
            Err(CopyError { error, .. }) => return Err(error),
        }
    }
    let tried: Vec<String> = cold_roots
        .iter()
        .map(|root| root.display().to_string())
        .collect();
    Err(RawStoreError::Integrity(format!(
        "{data_path}: no cold root holds a verified copy to recall it from (tried {}); the \
         partition stays recorded as removed (ADR-062 D6)",
        tried.join(", ")
    )))
}

/// Streams `source` into the new file `dest` through a bounded buffer,
/// hashing on the way, and fsyncs it. A source that does not match its
/// manifest leaves no `dest`.
fn stream_copy(
    source: &Path,
    dest: &Path,
    expected: &SealedFile,
    source_root: &Path,
    data_path: &str,
) -> Result<(), CopyError> {
    let mut input = match File::open(source) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(CopyError::source(RawStoreError::Integrity(format!(
                "{data_path}: the source root {} holds its manifest without its data",
                source_root.display()
            ))));
        }
        Err(error) => return Err(CopyError::source(io_error("open", source)(error))),
    };
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)
        .map_err(io_error("create", dest))?;
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0; CHUNK];
    loop {
        let n = input
            .read(&mut buffer)
            .map_err(|error| CopyError::source(io_error("read", source)(error)))?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
        bytes += n as u64;
        output
            .write_all(&buffer[..n])
            .map_err(io_error("write", dest))?;
    }
    if bytes != expected.bytes || hex(&hasher.finalize()) != expected.sha256 {
        drop(output);
        remove_if_present(dest)?;
        return Err(CopyError::source(RawStoreError::Integrity(format!(
            "{data_path} on the source root {}: the data does not match its manifest; nothing \
             was copied",
            source_root.display()
        ))));
    }
    Ok(output.sync_all().map_err(io_error("sync", dest))?)
}

/// Publishes the manifest `text` next to data that is already in place:
/// the temp file, fsync, read-back, read-only, rename, directory fsync.
fn publish_manifest(
    partition: &Path,
    manifest_tmp: &Path,
    manifest_final: &Path,
    text: &str,
) -> Result<(), RawStoreError> {
    write_new(manifest_tmp, text.as_bytes())?;
    sync_dir(partition)?;
    reached(CopyStep::WriteManifest)?;
    if fs::read(manifest_tmp).map_err(io_error("read", manifest_tmp))? != text.as_bytes() {
        remove_if_present(manifest_tmp)?;
        return Err(RawStoreError::Integrity(format!(
            "{}: the manifest read back does not match the source",
            manifest_tmp.display()
        )));
    }
    reached(CopyStep::Verify)?;
    mark_read_only(manifest_tmp)?;
    reached(CopyStep::MarkReadOnly)?;
    rename_no_replace(manifest_tmp, manifest_final)?;
    reached(CopyStep::PublishManifest)?;
    sync_dir(partition)?;
    reached(CopyStep::SyncDir)
}

/// The temp name of an immutable file: `.<name>.tmp`, the import ledger's
/// own convention (ADR-034 D4).
fn immutable_tmp(dest: &Path) -> Result<PathBuf, RawStoreError> {
    let name = dest
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| RawStoreError::Invalid(format!("{} has no file name", dest.display())))?;
    Ok(dest.with_file_name(format!(".{name}.tmp")))
}

/// Copies the immutable file `source` (an import-ledger entry) to `dest`.
///
/// An existing `dest` must be byte-identical
/// ([`CopyOutcome::AlreadyPresent`]); anything else is
/// [`RawStoreError::Integrity`]. Otherwise: the temp file `.<name>.tmp`
/// ([`CopyStep::WriteData`], fsynced), read back and compared
/// ([`CopyStep::Verify`]), read-only ([`CopyStep::MarkReadOnly`]), renamed
/// without replacing ([`CopyStep::PublishData`]) and the directory fsynced
/// ([`CopyStep::SyncDir`]). Missing directories are created.
///
/// # Errors
///
/// A [`CopyError`] on [`Side::Source`] when the source cannot be read (a
/// missing source included); on [`Side::Destination`] for a differing
/// destination ([`RawStoreError::Integrity`]) and any I/O failure there.
pub fn copy_immutable(source: &Path, dest: &Path) -> Result<CopyOutcome, CopyError> {
    let bytes =
        fs::read(source).map_err(|error| CopyError::source(io_error("read", source)(error)))?;
    match fs::read(dest) {
        Ok(present) if present == bytes => return Ok(CopyOutcome::AlreadyPresent),
        Ok(_) => {
            return Err(RawStoreError::Integrity(format!(
                "{} differs from {}; an immutable file is byte-identical on every root \
                 (ADR-062 D5)",
                dest.display(),
                source.display()
            ))
            .into());
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(io_error("read", dest)(error).into()),
    }
    let dir = dest
        .parent()
        .ok_or_else(|| RawStoreError::Invalid(format!("{} has no directory", dest.display())))?;
    let tmp = immutable_tmp(dest)?;
    remove_if_present(&tmp)?;
    ensure_dir(dir)?;
    write_new(&tmp, &bytes)?;
    reached(CopyStep::WriteData)?;
    if fs::read(&tmp).map_err(io_error("read", &tmp))? != bytes {
        remove_if_present(&tmp)?;
        return Err(RawStoreError::Integrity(format!(
            "{}: the copy read back does not match {}",
            tmp.display(),
            source.display()
        ))
        .into());
    }
    reached(CopyStep::Verify)?;
    mark_read_only(&tmp)?;
    reached(CopyStep::MarkReadOnly)?;
    rename_no_replace(&tmp, dest)?;
    reached(CopyStep::PublishData)?;
    sync_dir(dir)?;
    reached(CopyStep::SyncDir)?;
    Ok(CopyOutcome::Copied)
}

/// The source of a tail append: a file read at offsets (the journal), or
/// bytes already in memory (the copy ledger, from the read the ledger copy
/// is computed from).
#[derive(Debug, Clone, Copy)]
pub enum TailSource<'a> {
    /// A file on disk.
    File(&'a Path),
    /// Bytes in memory.
    Bytes(&'a [u8]),
}

impl TailSource<'_> {
    /// What the source is, for messages.
    fn describe(&self) -> String {
        match self {
            Self::File(path) => path.display().to_string(),
            Self::Bytes(_) => "hot's copy ledger".to_owned(),
        }
    }

    /// The `len` bytes at `offset`; a failure is on the source side.
    fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>, CopyError> {
        self.read_at_inner(offset, len).map_err(CopyError::source)
    }

    fn read_at_inner(&self, offset: u64, len: usize) -> Result<Vec<u8>, RawStoreError> {
        match self {
            Self::File(path) => {
                let mut file = File::open(path).map_err(io_error("open", path))?;
                read_at(&mut file, path, offset, len)
            }
            Self::Bytes(bytes) => {
                let start = usize::try_from(offset).ok().filter(|&s| s <= bytes.len());
                start
                    .and_then(|start| bytes.get(start..start + len))
                    .map(<[u8]>::to_vec)
                    .ok_or_else(|| {
                        RawStoreError::Io(format!(
                            "hot's copy ledger: {len} bytes at byte {offset} are past its end"
                        ))
                    })
            }
        }
    }
}

/// Reads `len` bytes of `file` at `offset`.
fn read_at(
    file: &mut File,
    path: &Path,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>, RawStoreError> {
    let mut buffer = vec![0; len];
    file.seek(SeekFrom::Start(offset))
        .and_then(|_| file.read_exact(&mut buffer))
        .map_err(io_error("read", path))?;
    Ok(buffer)
}

/// Where the last complete (`\n`-terminated) line of the first `len`
/// bytes of `file` starts: after the line end before it, or 0. With no
/// line end at all it is 0, so a partial first line is compared whole.
fn last_line_start(file: &mut File, path: &Path, len: u64) -> Result<u64, RawStoreError> {
    let step = CHUNK as u64;
    // The line end that closes the last complete line, then the one before.
    let mut seen = 0;
    let mut end = len;
    while end > 0 {
        let start = end.saturating_sub(step);
        let chunk = read_at(file, path, start, (end - start) as usize)?;
        for (i, &byte) in chunk.iter().enumerate().rev() {
            if byte == b'\n' {
                seen += 1;
                if seen == 2 {
                    return Ok(start + i as u64 + 1);
                }
            }
        }
        end = start;
    }
    Ok(0)
}

/// Extends `dest` to the byte prefix `[0, end)` of `source` (the D11 tail
/// append of a line-structured, append-only file) and returns the bytes
/// appended.
///
/// A missing `dest` is created (its directory fsynced). A `dest` longer
/// than `end` is refused. Before anything is written, the bytes of `dest`
/// from the start of its last complete line to its end, a partial tail
/// included, are compared with the source's bytes at the same offsets; a
/// mismatch is [`RawStoreError::Integrity`] naming the file and the first
/// differing offset, and `dest` stays untouched. Then `[len(dest), end)` is
/// appended in bounded chunks ([`TailStep::PartialWrite`] stops after the
/// first of two halves), fsynced ([`TailStep::Sync`]), read back and
/// compared. A crash leaves a shorter byte prefix of the source, which the
/// next call extends. With `end` equal to the length it only compares.
///
/// # Errors
///
/// A [`CopyError`] on [`Side::Source`] when the source cannot be read (a
/// source shorter than `end` included); on [`Side::Destination`] for a
/// `dest` longer than `end` or not a prefix of the source at its tail and
/// for a read-back mismatch ([`RawStoreError::Integrity`]), and any I/O
/// failure on `dest`.
pub fn extend_prefix(source: TailSource<'_>, dest: &Path, end: u64) -> Result<u64, CopyError> {
    if !exists(dest)? {
        let dir = dest.parent().ok_or_else(|| {
            RawStoreError::Invalid(format!("{} has no directory", dest.display()))
        })?;
        ensure_dir(dir)?;
        match OpenOptions::new().write(true).create_new(true).open(dest) {
            Ok(file) => file.sync_all().map_err(io_error("sync", dest))?,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error("create", dest)(error).into()),
        }
        sync_dir(dir)?;
    }
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(dest)
        .map_err(io_error("open", dest))?;
    let len = file.metadata().map_err(io_error("stat", dest))?.len();
    if len > end {
        return Err(RawStoreError::Integrity(format!(
            "{}: {len} bytes, longer than the {end} bytes of {} it copies; a copy is a byte \
             prefix of hot's file (ADR-062 D11)",
            dest.display(),
            source.describe()
        ))
        .into());
    }

    // The copy's last record (and any partial tail) against the source.
    let mut offset = last_line_start(&mut file, dest, len)?;
    while offset < len {
        let n = (len - offset).min(CHUNK as u64) as usize;
        let mine = read_at(&mut file, dest, offset, n)?;
        let theirs = source.read_at(offset, n)?;
        if let Some(i) = mine.iter().zip(&theirs).position(|(a, b)| a != b) {
            return Err(RawStoreError::Integrity(format!(
                "{}: byte {} differs from {}; the copy's last record is not hot's record at that \
                 offset, so it is not a byte prefix of hot's file (ADR-062 D11)",
                dest.display(),
                offset + i as u64,
                source.describe()
            ))
            .into());
        }
        offset += n as u64;
    }
    if len == end {
        return Ok(0);
    }

    // Append [len, end) in bounded chunks: the first half, then the rest.
    let half = len + ((end - len) / 2).max(1);
    append_range(&mut file, dest, source, len, half)?;
    tail_reached(TailStep::PartialWrite)?;
    append_range(&mut file, dest, source, half, end)?;
    file.sync_all().map_err(io_error("sync", dest))?;
    tail_reached(TailStep::Sync)?;

    // Read the appended range back.
    let mut offset = len;
    while offset < end {
        let n = (end - offset).min(CHUNK as u64) as usize;
        if read_at(&mut file, dest, offset, n)? != source.read_at(offset, n)? {
            return Err(RawStoreError::Integrity(format!(
                "{}: the bytes appended at {offset} read back differently from {}",
                dest.display(),
                source.describe()
            ))
            .into());
        }
        offset += n as u64;
    }
    Ok(end - len)
}

/// Appends the source's `[from, to)` to `file` in bounded chunks.
fn append_range(
    file: &mut File,
    dest: &Path,
    source: TailSource<'_>,
    from: u64,
    to: u64,
) -> Result<(), CopyError> {
    let mut offset = from;
    while offset < to {
        let n = (to - offset).min(CHUNK as u64) as usize;
        let chunk = source.read_at(offset, n)?;
        file.write_all(&chunk)
            .map_err(io_error("append to", dest))?;
        offset += n as u64;
    }
    Ok(())
}

/// The replication pass lock on hot; dropping it releases the lock.
#[derive(Debug)]
pub struct ReplicationLock {
    _file: File,
}

/// The pass lock's path on the store rooted at `raw_root`.
pub fn replication_lock_path(raw_root: &Path) -> PathBuf {
    raw_root.join(META_DIR).join(REPLICATE_LOCK)
}

/// Takes the pass lock `<raw_root>/.mie/replicate.lock` without waiting
/// (ADR-062 D7 *Formats and layout*): one replication pass at a time, since
/// two passes would both read a copy's length and append the same tail
/// twice.
///
/// # Errors
///
/// [`RawStoreError::Locked`] when another pass holds it;
/// [`RawStoreError::Io`] when the lock file cannot be opened.
pub fn try_replication_lock(raw_root: &Path) -> Result<ReplicationLock, RawStoreError> {
    let path = replication_lock_path(raw_root);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(io_error("open", &path))?;
    match file.try_lock() {
        Ok(()) => Ok(ReplicationLock { _file: file }),
        Err(TryLockError::WouldBlock) => Err(RawStoreError::Locked(format!(
            "another replication pass holds {}",
            path.display()
        ))),
        Err(TryLockError::Error(error)) => Err(io_error("lock", &path)(error)),
    }
}

/// The verification lock on hot; dropping it releases the lock.
#[derive(Debug)]
pub struct VerifyLock {
    _file: File,
}

/// The verification lock's path on the store rooted at `raw_root`.
pub fn verify_lock_path(raw_root: &Path) -> PathBuf {
    raw_root.join(META_DIR).join(VERIFY_LOCK)
}

/// Takes the verification lock `<raw_root>/.mie/verify.lock` without
/// waiting (ADR-062 D11, #192): `mie store verify` and `mie store repair`
/// hold it for their whole run, so a verification never records a finding
/// about a copy that a repair made after the verification read the ledger.
///
/// # Errors
///
/// [`RawStoreError::Locked`] when another verification or repair holds it;
/// [`RawStoreError::Io`] when the lock file cannot be opened.
pub fn try_verify_lock(raw_root: &Path) -> Result<VerifyLock, RawStoreError> {
    let path = verify_lock_path(raw_root);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(io_error("open", &path))?;
    match file.try_lock() {
        Ok(()) => Ok(VerifyLock { _file: file }),
        Err(TryLockError::WouldBlock) => Err(RawStoreError::Locked(format!(
            "another mie store verify or mie store repair holds {}",
            path.display()
        ))),
        Err(TryLockError::Error(error)) => Err(io_error("lock", &path)(error)),
    }
}

/// What [`check_sealed_copy`] found, in its precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyCheck {
    /// The data and its manifest match the ledger's manifest text.
    Good,
    /// No data: no file at all, or a manifest without its data.
    Missing,
    /// No manifest next to the data, or one whose text differs from the
    /// ledger's. `other_file` when the data matches that differing
    /// manifest: two sealed files under one path (D5), never a damaged
    /// copy of the ledger's file.
    Manifest {
        /// The root holds a self-consistent other file.
        other_file: bool,
    },
    /// The data's size differs from the ledger's manifest.
    Size,
    /// The data's SHA-256 differs from the ledger's manifest.
    Sha256,
}

/// Classifies the copy of the sealed part `data_path` on `root` against
/// `ledger_text`, the copy ledger's manifest text of the path (ADR-062 D11,
/// #192 V4), in this precedence: [`CopyCheck::Missing`], then
/// [`CopyCheck::Manifest`], then [`CopyCheck::Size`], then
/// [`CopyCheck::Sha256`]. The data is always checked against the ledger's
/// text, size before hash, streamed through a bounded buffer. A data path
/// that is no regular file (a directory, say) is an I/O error, never a
/// finding.
///
/// # Errors
///
/// [`RawStoreError::Invalid`] for a path outside the layout;
/// [`RawStoreError::Corrupt`] for a ledger text that does not parse;
/// [`RawStoreError::Io`] for any I/O failure on `root` other than an absent
/// file.
pub fn check_sealed_copy(
    root: &Path,
    data_path: &str,
    ledger_text: &str,
) -> Result<CopyCheck, RawStoreError> {
    let expected = manifest::parse(ledger_text)?;
    let (key, date, part) = layout::parse_data_path(data_path)?;
    let data = layout::resolve(root, data_path);
    let manifest_path = layout::resolve(
        root,
        &layout::file_path(&key, &date, PartFile::Manifest, part),
    );
    let mut file = match File::open(&data) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(CopyCheck::Missing),
        Err(error) => return Err(io_error("open", &data)(error)),
    };
    let metadata = file.metadata().map_err(io_error("inspect", &data))?;
    if !metadata.is_file() {
        return Err(RawStoreError::Io(format!(
            "{} is not a regular file",
            data.display()
        )));
    }
    let present = match fs::read(&manifest_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(CopyCheck::Manifest { other_file: false });
        }
        Err(error) => return Err(io_error("read", &manifest_path)(error)),
    };
    if present != ledger_text.as_bytes() {
        let own = std::str::from_utf8(&present)
            .ok()
            .and_then(|text| manifest::parse(text).ok())
            .filter(|own| own.stream == key && own.date == date && own.part == part);
        let other_file = match own {
            Some(own) => metadata.len() == own.bytes && hash_open(&mut file, &data)? == own.sha256,
            None => false,
        };
        return Ok(CopyCheck::Manifest { other_file });
    }
    if metadata.len() != expected.bytes {
        return Ok(CopyCheck::Size);
    }
    Ok(if hash_open(&mut file, &data)? == expected.sha256 {
        CopyCheck::Good
    } else {
        CopyCheck::Sha256
    })
}

/// The SHA-256 of an open file from its start, streamed.
fn hash_open(file: &mut File, path: &Path) -> Result<String, RawStoreError> {
    file.seek(SeekFrom::Start(0))
        .map_err(io_error("read", path))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; CHUNK];
    loop {
        let n = file.read(&mut buffer).map_err(io_error("read", path))?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(hex(&hasher.finalize()))
}

/// The directory a repair run at `at_ms` moves the damaged copies of
/// `root` into: `<root>/.mie/damaged/<at_ms>` (ADR-062 D7, D11, #192).
pub fn damaged_dir(root: &Path, at_ms: i64) -> PathBuf {
    root.join(META_DIR)
        .join(DAMAGED_DIR)
        .join(at_ms.to_string())
}

/// Moves the copy of the sealed part `data_path` on `root` aside into
/// `aside` (a [`damaged_dir`] or a [`quarantine_dir`] of the same root, so
/// the same filesystem and atomic renames), under the same relative path,
/// and returns the names moved (#192 R5, #211 W3). The data file goes
/// first, then its manifest, then the seal temp names of the path: a
/// concurrent `read` skips a root without the data, and a crash after the first rename leaves a manifest without
/// data, which the verification reports as `missing` and the next repair
/// completes. Every rename refuses to replace; the directories are
/// fsynced. A name that does not exist is skipped, so a re-run completes
/// a part whose data moved and whose manifest did not; a name present both
/// in the layout and in `aside` is `Integrity` and nothing is overwritten.
///
/// # Errors
///
/// [`RawStoreError::Invalid`] for a path outside the layout;
/// [`RawStoreError::Integrity`] when `aside` already holds a name;
/// [`RawStoreError::Io`] on I/O failure.
pub fn move_aside(
    root: &Path,
    data_path: &str,
    aside: &Path,
) -> Result<Vec<String>, RawStoreError> {
    let (key, date, part) = layout::parse_data_path(data_path)?;
    let partition_rel = layout::partition_dir(&key, &date);
    let partition = layout::resolve(root, &partition_rel);
    let dest = layout::resolve(aside, &partition_rel);
    let mut moved = Vec::new();
    for kind in [
        PartFile::Data,
        PartFile::Manifest,
        PartFile::DataTmp,
        PartFile::ManifestTmp,
    ] {
        let name = kind.name(part);
        let from = partition.join(&name);
        if !exists(&from)? {
            continue;
        }
        ensure_dir(&dest)?;
        rename_no_replace(&from, &dest.join(&name))?;
        sync_dir(&dest)?;
        sync_dir(&partition)?;
        moved.push(format!("{partition_rel}/{name}"));
    }
    Ok(moved)
}

/// Moves the single file `path` (an import-ledger entry copy, a tail copy)
/// to `aside`, a path under a [`damaged_dir`] of the same root (#192 R5):
/// the parent directories are created, the rename refuses to replace, and
/// both directories are fsynced. A missing `path` is no error.
///
/// # Errors
///
/// [`RawStoreError::Integrity`] when `aside` exists; [`RawStoreError::Io`]
/// on I/O failure.
pub fn move_file_aside(path: &Path, aside: &Path) -> Result<bool, RawStoreError> {
    if !exists(path)? {
        return Ok(false);
    }
    let (Some(dir), Some(aside_dir)) = (path.parent(), aside.parent()) else {
        return Err(RawStoreError::Invalid(format!(
            "{} or {} has no directory",
            path.display(),
            aside.display()
        )));
    };
    ensure_dir(aside_dir)?;
    rename_no_replace(path, aside)?;
    sync_dir(aside_dir)?;
    sync_dir(dir)?;
    Ok(true)
}

/// The directory below every root that holds the quarantined parts of a
/// replace (ADR-062 D11): outside the layout, so `select`, recovery and
/// every walk of the layout skip it, and never deleted by mie.
pub const QUARANTINE_DIR: &str = ".quarantine";

/// The directory the replace with quarantine id `id` moves the parts of
/// `root` into: `<root>/.quarantine/<id>` (ADR-062 D11, #211 W3). The
/// parts keep their relative path below it.
pub fn quarantine_dir(root: &Path, id: &str) -> PathBuf {
    root.join(QUARANTINE_DIR).join(id)
}

/// One part found in a quarantine directory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuarantinedCopy {
    /// Its manifest's text when the manifest is there and is UTF-8; it is
    /// not parsed here.
    pub text: Option<String>,
    /// Whether its data file is there. The data is never read: quarantined
    /// bytes are evidence.
    pub data: bool,
}

/// A root's `.quarantine/` directory (ADR-062 D7 step 5a, #211 Q1).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuarantineListing {
    /// Per id directory whose name is a valid segment, the parts below it
    /// by data path.
    pub ids:
        std::collections::BTreeMap<String, std::collections::BTreeMap<String, QuarantinedCopy>>,
    /// The names in `.quarantine/` that are no valid segment (a look-alike
    /// hyphen, a trailing blank, a zero-width character): reported, and
    /// they count for nothing.
    pub bad_names: Vec<String>,
}

/// Lists the quarantine directory of `root` (#211 Q1): every id directory
/// and below it every part of the layout, with its manifest text or its
/// data alone. A missing `.quarantine/` is empty. Seal temp names and
/// names outside the layout are skipped.
///
/// # Errors
///
/// [`RawStoreError::Integrity`] when `.quarantine` exists and is no
/// directory (a symlink included); [`RawStoreError::Io`] when a directory
/// or a manifest cannot be read: a skipped directory would read as parts
/// gone.
pub fn list_quarantine(root: &Path) -> Result<QuarantineListing, RawStoreError> {
    let dir = root.join(QUARANTINE_DIR);
    match fs::symlink_metadata(&dir) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(RawStoreError::Integrity(format!(
                "{} is no directory",
                dir.display()
            )));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(QuarantineListing::default());
        }
        Err(error) => return Err(io_error("inspect", &dir)(error)),
    }
    let mut listing = QuarantineListing::default();
    for name in list_entries(&dir)?.unwrap_or_default() {
        if validate_segment(&name).is_err() {
            listing.bad_names.push(name);
            continue;
        }
        let id_dir = dir.join(&name);
        let mut found: Vec<(String, PartFile)> = Vec::new();
        for_each_partition(&id_dir, |key, date, names| {
            for entry in names {
                if let Some((kind @ (PartFile::Data | PartFile::Manifest), part)) =
                    PartFile::classify(entry)
                {
                    found.push((layout::file_path(key, date, PartFile::Data, part), kind));
                }
            }
        })?;
        let mut parts = std::collections::BTreeMap::new();
        for (path, kind) in found {
            let copy: &mut QuarantinedCopy = parts.entry(path.clone()).or_default();
            match kind {
                PartFile::Data => copy.data = true,
                _ => {
                    let (key, date, part) = layout::parse_data_path(&path)?;
                    let manifest = layout::resolve(
                        &id_dir,
                        &layout::file_path(&key, &date, PartFile::Manifest, part),
                    );
                    let bytes = fs::read(&manifest).map_err(io_error("read", &manifest))?;
                    copy.text = String::from_utf8(bytes).ok();
                }
            }
        }
        listing.ids.insert(name, parts);
    }
    Ok(listing)
}

/// The first byte in `[0, len)` where the file `copy` differs from
/// `source`, compared in bounded chunks with each chunk of the copy read
/// before the source's; `None` when they agree. The caller makes sure both
/// are at least `len` bytes long.
///
/// # Errors
///
/// A [`CopyError`] on [`Side::Destination`] when the copy cannot be read,
/// on [`Side::Source`] when the source cannot.
pub fn first_difference(
    copy: &Path,
    source: TailSource<'_>,
    len: u64,
) -> Result<Option<u64>, CopyError> {
    let mut file = File::open(copy).map_err(io_error("open", copy))?;
    let mut offset = 0;
    while offset < len {
        let n = (len - offset).min(CHUNK as u64) as usize;
        let mine = read_at(&mut file, copy, offset, n)?;
        let theirs = source.read_at(offset, n)?;
        if let Some(i) = mine.iter().zip(&theirs).position(|(a, b)| a != b) {
            return Ok(Some(offset + i as u64));
        }
        offset += n as u64;
    }
    Ok(None)
}

/// Where the line holding byte `offset` of `source` starts: after the last
/// `\n` before it, or 0.
///
/// # Errors
///
/// A [`CopyError`] on [`Side::Source`] when the source cannot be read.
pub fn line_start(source: TailSource<'_>, offset: u64) -> Result<u64, CopyError> {
    let mut end = offset;
    while end > 0 {
        let start = end.saturating_sub(CHUNK as u64);
        let chunk = source.read_at(start, (end - start) as usize)?;
        if let Some(i) = chunk.iter().rposition(|&b| b == b'\n') {
            return Ok(start + i as u64 + 1);
        }
        end = start;
    }
    Ok(0)
}

/// The steps of [`rebuild_prefix`]. Tests stop a rebuild after either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildStep {
    /// The damaged copy is moved aside.
    MoveAside,
    /// The new copy is written to its temp name, fsynced and read back.
    WriteTemp,
}

#[cfg(test)]
thread_local! {
    /// Test hook: the next rebuild on this thread stops after this step.
    pub(crate) static REBUILD_CRASH_AFTER: std::cell::Cell<Option<RebuildStep>> =
        const { std::cell::Cell::new(None) };
}

/// Marks the end of a rebuild step; under test, simulates a crash there
/// once.
fn rebuild_reached(step: RebuildStep) -> Result<(), RawStoreError> {
    #[cfg(test)]
    if REBUILD_CRASH_AFTER.with(|crash| crash.get() == Some(step)) {
        REBUILD_CRASH_AFTER.with(|crash| crash.set(None));
        return Err(RawStoreError::Io(format!("simulated crash after {step:?}")));
    }
    let _ = step;
    Ok(())
}

/// Replaces the tail copy `dest` (a journal or copy-ledger copy on a copy
/// target) by the byte prefix `[0, upto)` of `source` (#192 R6, R7). The
/// damaged copy is moved to `aside` first ([`RebuildStep::MoveAside`]; the
/// rename refuses to replace, its directories fsynced); then the prefix is
/// written to the temp name `.<name>.rebuild.tmp` beside `dest` in bounded
/// chunks, fsynced and read back ([`RebuildStep::WriteTemp`]), renamed into
/// place without replacing, and the directory fsynced. A crash leaves the
/// copy absent, which the next replication pass creates from hot, or a
/// stale temp name, which the next rebuild removes. A missing `dest` is no
/// error: there is nothing to move aside.
///
/// # Errors
///
/// A [`CopyError`] on [`Side::Source`] when the source cannot be read (one
/// shorter than `upto` included); on [`Side::Destination`] for a read-back
/// mismatch ([`RawStoreError::Integrity`]) and any I/O failure there.
pub fn rebuild_prefix(
    source: TailSource<'_>,
    dest: &Path,
    upto: u64,
    aside: &Path,
) -> Result<(), CopyError> {
    let dir = dest
        .parent()
        .ok_or_else(|| RawStoreError::Invalid(format!("{} has no directory", dest.display())))?;
    let name = dest
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| RawStoreError::Invalid(format!("{} has no file name", dest.display())))?;
    let tmp = dir.join(format!(".{name}.rebuild.tmp"));
    remove_if_present(&tmp)?;
    move_file_aside(dest, aside)?;
    rebuild_reached(RebuildStep::MoveAside)?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&tmp)
        .map_err(io_error("create", &tmp))?;
    append_range(&mut file, &tmp, source, 0, upto)?;
    file.sync_all().map_err(io_error("sync", &tmp))?;
    let mut offset = 0;
    while offset < upto {
        let n = (upto - offset).min(CHUNK as u64) as usize;
        if read_at(&mut file, &tmp, offset, n)? != source.read_at(offset, n)? {
            drop(file);
            remove_if_present(&tmp)?;
            return Err(RawStoreError::Integrity(format!(
                "{}: the rebuilt copy read back differently from {}",
                tmp.display(),
                source.describe()
            ))
            .into());
        }
        offset += n as u64;
    }
    drop(file);
    rebuild_reached(RebuildStep::WriteTemp)?;
    rename_no_replace(&tmp, dest)?;
    sync_dir(dir)?;
    Ok(())
}
