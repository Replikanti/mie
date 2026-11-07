//! The union `select` of a tiered store (ADR-062 D7).
//!
//! The order is fixed: hot is listed completely first, then the copy ledger
//! is read from disk, fresh for this call. A file's records precede the
//! removal of its partition, and the removal record precedes the rename
//! (D6), so a file missing from the hot listing is in any ledger read after
//! it. Both sources are merged by relative path and checked over the
//! selected streams and dates *before* the event-time filter, so a
//! manifest conflict (D5) can never drop a file from one side silently.
//! Only then are the ordinary files filtered by event time and hashed into
//! the dataset version, which is the version a single root holding every
//! file yields.
//!
//! # The ledger's view of a path
//!
//! The ledger's records are folded in file order into one [`Entry`] per
//! data path:
//!
//! - `manifest` records give the manifest; two different texts for one
//!   path are a conflict (D5);
//! - `lost` marks the path lost (D11, the lost-file rule);
//! - `hold` holds it; `hold-cleared readmit` clears the hold;
//!   `hold-cleared replace` clears it and marks a replacement pending;
//! - `quarantined` marks a replacement pending under its quarantine id;
//! - `replaced` records the re-import's entry that replaces it.
//!
//! `copy`, `removed`, `returned`, `ledger-copy`, `finding` and `verify`
//! records do not change the file list: every recorded file counts,
//! whatever its partition's state.
//!
//! A path's verdict, in this precedence: lost, then conflict, then held,
//! then replaced (not listed; still listed on hot is an error), then
//! replacement pending, otherwise an ordinary file. A `hold-cleared
//! replace` never makes a path ordinary again: the selection fails until
//! the re-import's `replaced` record exists (D5).
//!
//! A non-ordinary path fails a selection that covers it. With a manifest
//! (from hot or the ledger) it is covered by the same event-time overlap as
//! an ordinary file; without one (a `lost` record of journal or import
//! origin, which carries no event times) every selection of its stream and
//! date covers it. Errors are evaluated in path order and the first one is
//! returned, so the message is deterministic.

use crate::copy_ledger::{self, HoldReason, Kind, Ledger, Resolution};
use crate::layout::{self, PartFile};
use crate::manifest;
use crate::{list_entries, sorted_entries};
use mie_ports::raw::{RawDataset, RawSelection, RawStoreError, RawStreamKey, SealedFile};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[cfg(test)]
mod tests;

/// A test hook that sees a relative path.
#[cfg(test)]
type PathHook = std::cell::RefCell<Option<Box<dyn FnMut(&str)>>>;

#[cfg(test)]
thread_local! {
    /// Test hook: runs once between the hot listing and the ledger read.
    pub(crate) static AFTER_HOT_LISTING: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    /// Test hook: runs with each partition directory (relative) between
    /// its `date=` listing and the listing of its own files.
    pub(crate) static BEFORE_PARTITION_FILES: PathHook = std::cell::RefCell::new(None);
    /// Test hook: runs with each listed data path (relative) before its
    /// manifest is read.
    pub(crate) static BEFORE_MANIFEST_READ: PathHook = std::cell::RefCell::new(None);
}

/// A manifest as one source holds it: its text verbatim and its parse.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Manifest {
    text: String,
    file: SealedFile,
}

/// A replacement that is pending: the selection fails until the re-import
/// records it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Pending {
    /// `hold-cleared replace`, before the part is quarantined.
    Cleared,
    /// `quarantined` under this id.
    Quarantined(String),
}

/// What the copy ledger records of one data path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Entry {
    manifest: Option<Manifest>,
    /// Two `manifest` records with different texts (D5).
    conflict: bool,
    lost: bool,
    held: Option<HoldReason>,
    pending: Option<Pending>,
    /// The import entry of the `replaced` record.
    replaced: Option<String>,
}

/// The copy ledger folded per data path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct LedgerView {
    entries: BTreeMap<String, Entry>,
}

impl LedgerView {
    /// Folds the records in file order (the module docs).
    fn fold(ledger: &Ledger) -> Result<Self, RawStoreError> {
        let mut entries: BTreeMap<String, Entry> = BTreeMap::new();
        for record in &ledger.records {
            match &record.kind {
                Kind::Manifest { text } => {
                    let file = manifest::parse(text)?;
                    let entry = entries.entry(file.relative_path.clone()).or_default();
                    match &entry.manifest {
                        Some(known) if known.text != *text => entry.conflict = true,
                        Some(_) => {}
                        None => {
                            entry.manifest = Some(Manifest {
                                text: text.clone(),
                                file,
                            });
                        }
                    }
                }
                Kind::Lost { path, .. } => entries.entry(path.clone()).or_default().lost = true,
                Kind::Hold { path, reason } => {
                    entries.entry(path.clone()).or_default().held = Some(*reason);
                }
                Kind::HoldCleared { path, resolution } => {
                    let entry = entries.entry(path.clone()).or_default();
                    entry.held = None;
                    if *resolution == Resolution::Replace {
                        entry.pending.get_or_insert(Pending::Cleared);
                    }
                }
                Kind::Quarantined { path, quarantine } => {
                    entries.entry(path.clone()).or_default().pending =
                        Some(Pending::Quarantined(quarantine.clone()));
                }
                Kind::Replaced { path, entry } => {
                    entries.entry(path.clone()).or_default().replaced = Some(entry.clone());
                }
                Kind::Copy { .. }
                | Kind::Removed { .. }
                | Kind::Returned { .. }
                | Kind::LedgerCopy { .. }
                | Kind::Finding { .. }
                | Kind::Verify { .. } => {}
            }
        }
        Ok(Self { entries })
    }
}

/// The interim writer guard of a tiered store (ADR-062 D6, #190 until
/// #191): what the copy ledger knows of one source's partitions, read once
/// when the writer opens.
///
/// Until #191 brings the recall and union part numbering, the writer
/// numbers a part from hot's listing alone. Two appends would then reuse a
/// path that a copy target already holds (D5): an append into a partition
/// the ledger records as removed from hot (an adopted old date), and an
/// append into a partition whose recorded parts hot does not hold yet (a
/// crashed adoption that published a file on the targets before hot). The
/// guard refuses both before anything is created.
///
/// The snapshot taken at writer open is enough: every writer of `removed`
/// records holds the source's writer lock (only `mie store adopt` until
/// #191), so none runs while this writer is open. `manifest` and `copy`
/// records that the replication pass appends meanwhile name parts hot
/// already holds, so they never raise a partition's highest part above
/// hot's.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TierGuard {
    /// The ledger, for messages.
    ledger: std::path::PathBuf,
    /// Partition directories whose latest `removed` / `returned` record is
    /// `removed`.
    removed: BTreeSet<String>,
    /// Per partition directory, the highest part number any record names.
    highest: BTreeMap<String, u32>,
}

impl TierGuard {
    /// Folds `ledger` (the copy ledger of the store rooted at `hot`) for
    /// the partitions of `source`.
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Corrupt`] for a `manifest` text that does not parse
    /// and [`RawStoreError::Invalid`] for a path outside the layout; the
    /// ledger parser has already refused both, so either means the writer
    /// fails closed.
    pub(crate) fn fold(hot: &Path, ledger: &Ledger, source: &str) -> Result<Self, RawStoreError> {
        let prefix = format!("{}/", layout::source_dir(source));
        let mut guard = Self {
            ledger: copy_ledger::ledger_path(hot),
            ..Self::default()
        };
        for record in &ledger.records {
            let path = match &record.kind {
                Kind::Manifest { text } => manifest::parse(text)?.relative_path,
                Kind::Copy { path, .. }
                | Kind::Lost { path, .. }
                | Kind::Hold { path, .. }
                | Kind::HoldCleared { path, .. }
                | Kind::Quarantined { path, .. }
                | Kind::Replaced { path, .. } => path.clone(),
                Kind::Removed { partition } => {
                    if partition.starts_with(&prefix) {
                        guard.removed.insert(partition.clone());
                    }
                    continue;
                }
                Kind::Returned { partition } => {
                    guard.removed.remove(partition);
                    continue;
                }
                Kind::LedgerCopy { .. } | Kind::Finding { .. } | Kind::Verify { .. } => continue,
            };
            if !path.starts_with(&prefix) {
                continue;
            }
            let (stream, date, part) = layout::parse_data_path(&path)?;
            let highest = guard
                .highest
                .entry(layout::partition_dir(&stream, &date))
                .or_insert(part);
            *highest = (*highest).max(part);
        }
        Ok(guard)
    }

    /// Refuses a new part `part` (hot's next number) in the partition
    /// directory `partition`, naming the cause (the type docs).
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Integrity`] for a partition the ledger records as
    /// removed from hot, and for one whose highest recorded part is `part`
    /// or above.
    pub(crate) fn check(&self, partition: &str, part: u32) -> Result<(), RawStoreError> {
        if self.removed.contains(partition) {
            return Err(RawStoreError::Integrity(format!(
                "partition {partition} is recorded as removed from hot in the copy ledger {}: an \
                 append to it needs the recall (ADR-062 D6), which #191 adds; until then the \
                 writer refuses it and writes nothing",
                self.ledger.display()
            )));
        }
        match self.highest.get(partition) {
            Some(&recorded) if recorded >= part => Err(RawStoreError::Integrity(format!(
                "partition {partition}: the copy ledger {} records part {recorded} of it, but \
                 hot's next part number is {part}, so the new part would reuse a recorded path \
                 (ADR-062 D5). Hot must first hold every recorded part: re-run the adoption that \
                 recorded it (`mie store adopt`); union part numbering comes with #191. The \
                 writer writes nothing",
                self.ledger.display()
            ))),
            _ => Ok(()),
        }
    }
}

/// What the hot listing found.
#[derive(Debug, Default)]
struct HotListing {
    /// Listed files by data path.
    files: BTreeMap<String, Manifest>,
    /// Partitions (relative) whose `date=` entry was listed and whose
    /// directory was gone when its files were listed.
    vanished_partitions: Vec<String>,
    /// Data paths whose manifest was listed and gone when it was read.
    vanished_files: Vec<String>,
}

/// The union `select` of the tiered store with hot at `hot` (module docs).
/// It never touches a cold root.
pub(crate) fn select(hot: &Path, selection: &RawSelection) -> Result<RawDataset, RawStoreError> {
    let window = selection.window();
    let (start, end) = (window.start.as_millis(), window.end.as_millis());
    // Only the date partitions the window touches, clamped to the supported
    // range, as in the single-root `select`.
    let first = start.max(0);
    let last = (end - 1).min(layout::MAX_EVENT_TIME_MS);
    let dates = if first <= last {
        Some((layout::civil_date(first)?, layout::civil_date(last)?))
    } else {
        None
    };

    // a. Hot, completely.
    let listing = match &dates {
        Some(dates) => list_hot(hot, selection.streams(), dates)?,
        None => HotListing::default(),
    };
    #[cfg(test)]
    if let Some(hook) = AFTER_HOT_LISTING.with(|hook| hook.borrow_mut().take()) {
        hook();
    }

    // b. Only then the ledger, fresh. A missing ledger fails every window:
    // read as empty it would drop every removed partition silently.
    let path = copy_ledger::ledger_path(hot);
    let ledger = copy_ledger::read(hot)?.ok_or_else(|| {
        RawStoreError::Integrity(format!(
            "copy ledger {} does not exist: a tiered store (paths.cold_roots configured) \
             requires it; `mie store init` creates it, and the copy-ledger rebuild restores it \
             (ADR-062 D7)",
            path.display()
        ))
    })?;
    let view = LedgerView::fold(&ledger)?;

    // c.–f. Merge, check, filter.
    let mut files = match &dates {
        Some(dates) => merge(&listing, &view, selection.streams(), dates, start, end)?,
        None => Vec::new(),
    };
    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    let version = manifest::dataset_version(window, selection.streams(), &files);
    Ok(RawDataset { version, files })
}

/// Lists every selected partition of hot in `[first_date, last_date]`,
/// with no event-time filter, keeping each manifest's text.
fn list_hot(
    hot: &Path,
    streams: &BTreeSet<RawStreamKey>,
    (first_date, last_date): &(String, String),
) -> Result<HotListing, RawStoreError> {
    let mut listing = HotListing::default();
    for stream in streams {
        let stream_dir = layout::resolve(hot, &layout::stream_dir(stream));
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
            let relative = layout::partition_dir(stream, date);
            #[cfg(test)]
            BEFORE_PARTITION_FILES.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook(&relative);
                }
            });
            let partition = layout::resolve(hot, &relative);
            let Some(names) = list_entries(&partition)? else {
                listing.vanished_partitions.push(relative);
                continue;
            };
            for name in names {
                let Some((PartFile::Manifest, part)) = PartFile::classify(&name) else {
                    continue;
                };
                let data_path = layout::file_path(stream, date, PartFile::Data, part);
                #[cfg(test)]
                BEFORE_MANIFEST_READ.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().as_mut() {
                        hook(&data_path);
                    }
                });
                let path = partition.join(&name);
                let text = match fs::read_to_string(&path) {
                    Ok(text) => text,
                    // The D6 rename can land between the listing and the
                    // read: the ledger read that follows names the file.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        listing.vanished_files.push(data_path);
                        continue;
                    }
                    Err(e) => {
                        return Err(RawStoreError::Corrupt(format!(
                            "read manifest {}: {e}",
                            path.display()
                        )));
                    }
                };
                let file = manifest::parse(&text)?;
                if file.stream != *stream || file.date != date || file.part != part {
                    return Err(RawStoreError::Corrupt(format!(
                        "manifest {} describes {}, not its own location",
                        path.display(),
                        file.relative_path
                    )));
                }
                listing.files.insert(data_path, Manifest { text, file });
            }
        }
    }
    Ok(listing)
}

/// Merges the hot listing and the ledger view over the selected streams
/// and dates, applies the checks of the module docs, and returns the
/// ordinary files that overlap `[start, end)`.
fn merge(
    listing: &HotListing,
    view: &LedgerView,
    streams: &BTreeSet<RawStreamKey>,
    (first_date, last_date): &(String, String),
    start: i64,
    end: i64,
) -> Result<Vec<SealedFile>, RawStoreError> {
    let in_range = |path: &str| {
        layout::parse_data_path(path).is_ok_and(|(stream, date, _)| {
            streams.contains(&stream)
                && date.as_str() >= first_date.as_str()
                && date.as_str() <= last_date.as_str()
        })
    };
    let covers = |file: &SealedFile| {
        file.min_event_time.as_millis() < end && file.max_event_time.as_millis() >= start
    };
    let paths: BTreeSet<&str> = listing
        .files
        .keys()
        .map(String::as_str)
        .chain(
            view.entries
                .keys()
                .map(String::as_str)
                .filter(|path| in_range(path)),
        )
        .collect();

    // (path order key, message); the smallest key is returned.
    let mut errors: Vec<(String, String)> = Vec::new();
    let mut files = Vec::new();
    let no_entry = Entry::default();
    for path in paths {
        let hot = listing.files.get(path);
        let entry = view.entries.get(path).unwrap_or(&no_entry);
        let mut fail = |message: String| errors.push((path.to_owned(), message));
        let conflict = match (hot, &entry.manifest) {
            (Some(hot), Some(ledger)) if hot.text != ledger.text => Some(format!(
                "{path}: hot's manifest differs from the copy ledger's manifest record; the same \
                 path must carry byte-identical manifests everywhere (ADR-062 D5)"
            )),
            _ if entry.conflict => Some(format!(
                "{path}: the copy ledger holds two different manifest texts for it; the same \
                 path must carry byte-identical manifests everywhere (ADR-062 D5)"
            )),
            _ => None,
        };
        let manifest = hot.or(entry.manifest.as_ref());
        // Without any manifest the path covers its whole stream and date.
        let covered = manifest.is_none_or(|m| covers(&m.file));
        if entry.lost {
            if covered {
                fail(format!(
                    "{path} is lost: the copy ledger records no good copy of it left, and a \
                     selection that covers it fails (ADR-062 D11, the lost-file rule)"
                ));
            } else if let Some(conflict) = conflict {
                fail(conflict);
            }
            continue;
        }
        if let Some(conflict) = conflict {
            fail(conflict);
            continue;
        }
        if let Some(reason) = entry.held {
            if covered {
                fail(format!(
                    "{path} is held ({}): resolve the hold with `mie store resolve` before a \
                     selection covers it (ADR-062 D11)",
                    reason.as_str()
                ));
            }
            continue;
        }
        if let Some(replacement) = &entry.replaced {
            if hot.is_some() {
                fail(format!(
                    "{path} is replaced by import entry {replacement} but still listed on hot \
                     (ADR-062 D11)"
                ));
            }
            continue;
        }
        if let Some(pending) = &entry.pending {
            if covered {
                let state = match pending {
                    Pending::Cleared => "its hold was resolved by replace".to_owned(),
                    Pending::Quarantined(id) => format!("it is quarantined under {id}"),
                };
                fail(format!(
                    "{path}: {state}, and no replacement is recorded yet; a selection that \
                     covers it fails until the re-import of its archive file records the \
                     replacement (ADR-062 D11)"
                ));
            }
            continue;
        }
        match manifest {
            Some(m) if covers(&m.file) => files.push(m.file.clone()),
            Some(_) => {}
            None => fail(format!(
                "{path} is recorded in the copy ledger without its manifest (ADR-062 D7)"
            )),
        }
    }

    // Every vanished partition and file must be named by the ledger.
    let recorded_partitions: BTreeSet<&str> = view
        .entries
        .iter()
        .filter(|(_, entry)| entry.manifest.is_some() || entry.lost)
        .filter_map(|(path, _)| path.rsplit_once('/').map(|(partition, _)| partition))
        .collect();
    for partition in &listing.vanished_partitions {
        if !recorded_partitions.contains(partition.as_str()) {
            errors.push((
                partition.clone(),
                format!(
                    "partition {partition} disappeared from hot while it was listed and the \
                     copy ledger records none of its files (ADR-062 D7)"
                ),
            ));
        }
    }
    for path in &listing.vanished_files {
        if !view.entries.contains_key(path) {
            errors.push((
                path.clone(),
                format!(
                    "{path} disappeared from hot while it was listed and the copy ledger does \
                     not record it (ADR-062 D7)"
                ),
            ));
        }
    }
    match errors.into_iter().min_by(|a, b| a.0.cmp(&b.0)) {
        Some((_, message)) => Err(RawStoreError::Integrity(message)),
        None => Ok(files),
    }
}
