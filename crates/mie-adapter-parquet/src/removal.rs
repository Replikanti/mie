//! The hot removal of a tiered raw store (ADR-062 D6).
//!
//! A partition leaves hot once it is safe elsewhere. The work is split in
//! two, so the capture thread never folds the whole copy ledger:
//!
//! - [`plan`] is a pure function over a ledger read and one walk of hot
//!   ([`scan_hot`]). The replication pass runs it after its own work, on
//!   its own thread, from a ledger read it already pays for. A partition is
//!   a candidate when every rule of [`plan`] holds.
//! - [`remove_with_lock`] and [`RawWriter::remove_partitions`](crate::RawWriter::remove_partitions)
//!   carry it out. The caller holds the source's writer lock (through the
//!   writer of `mie ingest`, or by trying the lock here), and the remover
//!   takes the copy-ledger lock with [`CopyLedger::try_hold`], which never
//!   waits. Under both locks it re-checks the plan cheaply: the ledger's
//!   tail since the plan ([`LedgerHold::records_after`](crate::LedgerHold::records_after)) names neither the
//!   partition nor its files, the date is still before the hot window, and
//!   the directory holds no seal debris and no file the plan did not see.
//!   Only then does it append `removed` and fsync, rename the directory to
//!   the hidden name `.removed-date=YYYY-MM-DD` beside it (same directory,
//!   so the same filesystem and an atomic rename; recovery, `select` and
//!   the replica walks read only `date=` entries), fsync the stream
//!   directory, delete the hidden directory and fsync again.
//!
//! A crash after the record leaves the partition *incomplete* (recorded
//! removed, its directory still on hot) or a hidden leftover; the next pass
//! finishes both without a second record. A recall that failed before its
//! `returned` record leaves an incomplete partition too.

use crate::copy_ledger::{CopyLedger, Kind, Ledger, Record, Subject, Truncation};
use crate::layout::{self, PartFile};
use crate::replica::layout_values;
use crate::tiers::{self, LedgerView, Verdict};
use crate::{io_error, list_entries, sync_dir};
use mie_ports::raw::RawStoreError;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::ErrorKind;
use std::path::Path;

/// The prefix of a removed partition's hidden name.
const HIDDEN_PREFIX: &str = ".removed-";

/// One partition directory of hot, as [`scan_hot`] found it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionScan {
    /// The data paths of its sealed parts, sorted.
    pub sealed: Vec<String>,
    /// Whether it holds seal debris: a `.tmp` file, or data without its
    /// manifest.
    pub debris: bool,
    /// For a partition before the hot window, hot's manifest text of each
    /// sealed part by data path, `None` when it cannot be read; empty for
    /// the others, which the plan never considers.
    pub manifests: BTreeMap<String, Option<String>>,
}

/// One walk of hot's layout.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HotScan {
    /// Per partition directory (relative).
    pub partitions: BTreeMap<String, PartitionScan>,
    /// The hidden `.removed-date=…` directories a crashed removal left
    /// (relative), sorted.
    pub leftovers: Vec<String>,
}

/// Walks hot's layout once, as [`replica::list_sealed`](crate::replica::list_sealed)
/// does: per partition its sealed data paths and whether seal debris is
/// there, and every hidden directory of a crashed removal. For partitions
/// dated before `first_hot_date`, the only ones the plan considers, it also
/// reads each sealed part's manifest text, so the plan can compare it with
/// the ledger's `manifest` record (D5; review of #202, finding 2).
///
/// # Errors
///
/// [`RawStoreError::Io`] when a directory cannot be listed.
pub fn scan_hot(hot: &Path, first_hot_date: &str) -> Result<HotScan, RawStoreError> {
    let mut scan = HotScan::default();
    for source in layout_values(hot, "source=")? {
        let source_dir = hot.join(format!("source={source}"));
        for instrument in layout_values(&source_dir, "instrument=")? {
            let instrument_dir = source_dir.join(format!("instrument={instrument}"));
            for stream in layout_values(&instrument_dir, "stream=")? {
                let stream_rel = format!("source={source}/instrument={instrument}/stream={stream}");
                let stream_dir = instrument_dir.join(format!("stream={stream}"));
                let Some(names) = list_entries(&stream_dir)? else {
                    continue;
                };
                for name in names {
                    if let Some(date) = name.strip_prefix(HIDDEN_PREFIX) {
                        if date
                            .strip_prefix("date=")
                            .is_some_and(|d| layout::parse_date(d).is_some())
                        {
                            scan.leftovers.push(format!("{stream_rel}/{name}"));
                        }
                        continue;
                    }
                    let Some(date) = name.strip_prefix("date=") else {
                        continue;
                    };
                    if layout::parse_date(date).is_none() {
                        continue;
                    }
                    let partition = format!("{stream_rel}/{name}");
                    let Some(files) = list_entries(&stream_dir.join(&name))? else {
                        continue;
                    };
                    let mut found = scan_partition(&partition, &files);
                    if date < first_hot_date {
                        found.manifests = read_manifests(hot, &found.sealed);
                    }
                    scan.partitions.insert(partition, found);
                }
            }
        }
    }
    scan.leftovers.sort();
    Ok(scan)
}

/// Hot's manifest text of each data path in `sealed`, `None` when it
/// cannot be read.
fn read_manifests(hot: &Path, sealed: &[String]) -> BTreeMap<String, Option<String>> {
    sealed
        .iter()
        .map(|path| (path.clone(), read_manifest(hot, path)))
        .collect()
}

/// Hot's manifest text of the data path `path`.
fn read_manifest(hot: &Path, path: &str) -> Option<String> {
    let (key, date, part) = layout::parse_data_path(path).ok()?;
    let manifest = layout::file_path(&key, &date, PartFile::Manifest, part);
    fs::read_to_string(layout::resolve(hot, &manifest)).ok()
}

/// The D5 message for hot's manifest of `path` differing from the ledger's.
fn differs(path: &str) -> String {
    format!(
        "{path}: hot's manifest differs from the copy ledger's manifest record; the same path \
         must carry byte-identical manifests everywhere, so its partition is not removed from \
         hot (ADR-062 D5)"
    )
}

/// The sealed parts and the debris among the entry `names` of `partition`.
fn scan_partition(partition: &str, names: &[String]) -> PartitionScan {
    let manifests: BTreeSet<u32> = names
        .iter()
        .filter_map(|name| match PartFile::classify(name) {
            Some((PartFile::Manifest, part)) => Some(part),
            _ => None,
        })
        .collect();
    let mut scan = PartitionScan::default();
    for name in names {
        match PartFile::classify(name) {
            Some((PartFile::Manifest, part)) => scan
                .sealed
                .push(format!("{partition}/{}", PartFile::Data.name(part))),
            Some((PartFile::Data, part)) if !manifests.contains(&part) => scan.debris = true,
            Some((PartFile::DataTmp | PartFile::ManifestTmp, _)) => scan.debris = true,
            Some((PartFile::Data, _)) | None => {}
        }
    }
    scan.sealed.sort();
    scan
}

/// A partition the plan found eligible, with its file set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The partition directory (relative).
    pub partition: String,
    /// Its file set: hot's sealed files plus every path the ledger records
    /// in it, replaced paths excluded; sorted.
    pub files: Vec<String>,
    /// The ledger's manifest text of every file in the set: hot's manifests
    /// must still equal them under the locks (D5).
    pub manifests: BTreeMap<String, String>,
}

/// What [`plan`] decided, against one ledger read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovalPlan {
    /// The ledger's verified length at the plan's read.
    pub ledger_len: u64,
    /// The chain hash of the last record of that read.
    pub chain: String,
    /// The partitions to remove.
    pub candidates: Vec<Candidate>,
    /// Partitions recorded as removed whose directory is still on hot (a
    /// crash between the record and the rename, or a recall that failed
    /// before `returned`); they pass the same file checks as a candidate.
    pub incomplete: Vec<Candidate>,
    /// The hidden directories a crashed removal left (relative).
    pub leftovers: Vec<String>,
    /// Every path whose hot manifest differs from the ledger's `manifest`
    /// record (D5), as a message: its partition stays on hot, and the pass
    /// reports it.
    pub conflicts: Vec<String>,
}

impl RemovalPlan {
    /// Whether the plan names anything of the source `source`.
    pub fn names_source(&self, source: &str) -> bool {
        let prefix = format!("{}/", layout::source_dir(source));
        self.candidates
            .iter()
            .chain(&self.incomplete)
            .map(|c| &c.partition)
            .chain(&self.leftovers)
            .any(|path| path.starts_with(&prefix))
    }
}

/// The removal plan over `ledger` and hot's `scan` (ADR-062 D6), for k
/// copy targets of which the ids `targets` presented their marker in the
/// planning pass, with the hot window starting at `first_hot_date`. A
/// partition on hot is a candidate iff all of these hold:
///
/// - (a) its date is before `first_hot_date` (D6, D8);
/// - (b) the ledger does not record it as removed;
/// - (c) `targets.len() == k`: every configured copy target presented its
///   marker (#191 STOP 1, confirmation 3), so a target replaced under a new
///   id never counts with the old one;
/// - (d) its file set (hot's sealed files plus every path the ledger
///   records in it, replaced paths excluded) has no lost, held, conflicting
///   or replacement-pending path;
/// - (e) every file in it has a `manifest` record whose text hot's
///   manifest equals (D5; a difference is reported in
///   [`RemovalPlan::conflicts`]) and, for every id in `targets`, a `copy`
///   record with no `finding` about that copy on that root after it (a
///   damaged copy never counts toward k, the re-check's own rule);
/// - (f) that `manifest` record and those `copy` records end at or before
///   P, the smallest of the latest `ledger-copy` prefix lengths of the ids
///   in `targets`: they lie in the prefix that k verified ledger copies
///   hold.
///
/// A partition with debris can still be planned: the `.tmp` rule must be
/// checked under the source lock, so the remover refuses it there.
/// Incomplete partitions (recorded removed, still on hot) need (c) to (f).
///
/// # Errors
///
/// [`RawStoreError::Corrupt`] for a `manifest` record that does not parse;
/// the ledger parser has already refused it.
pub fn plan(
    ledger: &Ledger,
    scan: &HotScan,
    targets: &BTreeSet<String>,
    k: usize,
    first_hot_date: &str,
) -> Result<RemovalPlan, RawStoreError> {
    let mut plan = RemovalPlan {
        ledger_len: ledger.verified_len,
        chain: ledger.last_chain.clone(),
        candidates: Vec::new(),
        incomplete: Vec::new(),
        leftovers: scan.leftovers.clone(),
        conflicts: Vec::new(),
    };
    if targets.len() != k || k == 0 {
        return Ok(plan);
    }
    let view = LedgerView::fold(ledger)?;
    let removed = tiers::removed_partitions(ledger);
    // Where each path's first `manifest` record and each id's first `copy`
    // record end, and P. A `finding` about a file on a root voids that
    // root's copy until a later `copy` record (review of #202, finding 3).
    let mut manifest_end: BTreeMap<String, u64> = BTreeMap::new();
    let mut copy_end: BTreeMap<(String, String), Option<u64>> = BTreeMap::new();
    let mut ledger_copy: BTreeMap<&str, u64> = BTreeMap::new();
    for (record, &end) in ledger.records.iter().zip(&ledger.ends) {
        match &record.kind {
            Kind::Manifest { text } => {
                let path = crate::manifest::parse(text)?.relative_path;
                manifest_end.entry(path).or_insert(end);
            }
            Kind::Copy { path, root } => {
                copy_end
                    .entry((path.clone(), root.clone()))
                    .or_insert(None)
                    .get_or_insert(end);
            }
            Kind::Finding {
                root,
                subject: Subject::File(path),
                ..
            } => {
                copy_end.insert((path.clone(), root.clone()), None);
            }
            Kind::LedgerCopy {
                root, prefix_bytes, ..
            } => {
                ledger_copy.insert(root.as_str(), *prefix_bytes);
            }
            _ => {}
        }
    }
    let Some(prefix) = targets
        .iter()
        .map(|id| ledger_copy.get(id.as_str()).copied())
        .collect::<Option<Vec<u64>>>()
        .and_then(|lengths| lengths.into_iter().min())
    else {
        return Ok(plan);
    };
    let inside = |end: Option<u64>| end.is_some_and(|end| end <= prefix);

    for (partition, on_hot) in &scan.partitions {
        let Ok((_, date)) = layout::parse_partition_dir(partition) else {
            continue;
        };
        let recorded_removed = removed.contains(partition);
        if !recorded_removed && date.as_str() >= first_hot_date {
            continue;
        }
        let recorded = view.partition(partition);
        let mut files: BTreeSet<String> = on_hot.sealed.iter().cloned().collect();
        let mut manifests: BTreeMap<String, String> = BTreeMap::new();
        let mut safe = true;
        for (path, verdict) in &recorded {
            match verdict {
                // A replaced path still on hot is the error `select`
                // reports; the partition stays until it is resolved.
                Verdict::Replaced => safe &= !on_hot.sealed.contains(path),
                Verdict::Ordinary(text) => {
                    files.insert(path.clone());
                    manifests.insert(path.clone(), text.clone());
                }
                Verdict::NoManifest
                | Verdict::Lost
                | Verdict::Conflict
                | Verdict::Held(_)
                | Verdict::Pending => safe = false,
            }
        }
        // Hot's version of every path it holds is the ledger's (D5).
        for path in &on_hot.sealed {
            match (on_hot.manifests.get(path), manifests.get(path)) {
                (Some(Some(hot)), Some(ledger)) if hot == ledger => {}
                (Some(Some(_)), Some(_)) => {
                    plan.conflicts.push(differs(path));
                    safe = false;
                }
                // Unreadable, or not recorded (rule e fails anyway).
                _ => safe = false,
            }
        }
        safe &= files.iter().all(|path| {
            inside(manifest_end.get(path).copied())
                && targets
                    .iter()
                    .all(|id| inside(copy_end.get(&(path.clone(), id.clone())).copied().flatten()))
        });
        if !safe || files.is_empty() {
            continue;
        }
        let candidate = Candidate {
            partition: partition.clone(),
            files: files.into_iter().collect(),
            manifests,
        };
        if recorded_removed {
            plan.incomplete.push(candidate);
        } else {
            plan.candidates.push(candidate);
        }
    }
    Ok(plan)
}

/// The writer lock of a source, taken by the remover; dropping it releases
/// the lock.
#[derive(Debug)]
pub struct SourceLock {
    _file: File,
}

/// Tries the writer lock `source=<source>/.lock` of hot, without creating
/// it. `None` when the source has no lock file (no writer ever ran, so it
/// has no partition).
///
/// # Errors
///
/// [`RawStoreError::Locked`] when a writer (an import) holds it;
/// [`RawStoreError::Io`] when it cannot be opened or locked.
pub fn try_source_lock(hot: &Path, source: &str) -> Result<Option<SourceLock>, RawStoreError> {
    let path = hot.join(layout::source_dir(source)).join(".lock");
    let file = match OpenOptions::new().write(true).open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("open", &path)(error)),
    };
    match file.try_lock() {
        Ok(()) => Ok(Some(SourceLock { _file: file })),
        Err(TryLockError::WouldBlock) => Err(RawStoreError::Locked(format!(
            "a writer holds source {source} in {}",
            hot.display()
        ))),
        Err(TryLockError::Error(error)) => Err(io_error("lock", &path)(error)),
    }
}

/// What one remove pass over one source did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemovalReport {
    /// The source.
    pub source: String,
    /// The partitions removed, each with its `removed` record.
    pub removed: Vec<String>,
    /// The incomplete removals finished (no new record) and the leftovers
    /// deleted.
    pub completed: Vec<String>,
    /// The partitions of the plan left on hot, with the reason.
    pub skipped: Vec<(String, String)>,
    /// Truncations of the ledger's trailing record by the removal appends.
    pub truncations: Vec<Truncation>,
    /// The source's writer lock was held by another writer: nothing ran.
    pub locked: bool,
    /// The error that ended the pass early, if any.
    pub error: Option<String>,
}

impl RemovalReport {
    /// Whether the pass did or found anything worth a journal line.
    pub fn is_empty(&self) -> bool {
        self.removed.is_empty()
            && self.completed.is_empty()
            && self.skipped.is_empty()
            && self.truncations.is_empty()
            && !self.locked
            && self.error.is_none()
    }
}

/// The steps of a removal, in order. Tests stop a removal after any of
/// them to simulate a crash.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemovalStep {
    /// The `removed` record is appended and fsynced.
    AfterRecord,
    /// The partition has its hidden name.
    AfterRename,
    /// The hidden directory is deleted.
    AfterDelete,
}

#[cfg(test)]
impl RemovalStep {
    /// Every step, in protocol order.
    pub(crate) const ALL: [Self; 3] = [Self::AfterRecord, Self::AfterRename, Self::AfterDelete];
}

#[cfg(test)]
thread_local! {
    /// Test hook: the next removal on this thread stops after this step.
    pub(crate) static CRASH_AFTER: std::cell::Cell<Option<RemovalStep>> =
        const { std::cell::Cell::new(None) };
}

/// Marks the end of a removal step; under test, simulates a crash there
/// once.
fn reached(step: RemovalStep) -> Result<(), String> {
    #[cfg(test)]
    if CRASH_AFTER.with(|crash| crash.get() == Some(step)) {
        CRASH_AFTER.with(|crash| crash.set(None));
        return Err(format!("simulated crash after {step:?}"));
    }
    let _ = step;
    Ok(())
}

/// Tries the writer lock of `source` and, holding it, runs the plan's
/// entries for `source` (`remove_locked`). An import that holds the
/// lock skips the source for this pass: the report is `locked`.
pub fn remove_with_lock(
    hot: &Path,
    source: &str,
    plan: &RemovalPlan,
    first_hot_date: &str,
    at_ms: i64,
) -> RemovalReport {
    let mut report = RemovalReport {
        source: source.to_owned(),
        ..RemovalReport::default()
    };
    match try_source_lock(hot, source) {
        Ok(Some(lock)) => {
            let report = remove_locked(hot, source, plan, &BTreeSet::new(), first_hot_date, at_ms);
            drop(lock);
            report
        }
        Ok(None) => report,
        Err(RawStoreError::Locked(_)) => {
            report.locked = true;
            report
        }
        Err(error) => {
            report.error = Some(error.to_string());
            report
        }
    }
}

/// The plan's entries for `source`, under its writer lock, which the
/// caller holds (through a [`RawWriter`](crate::RawWriter) or a
/// [`SourceLock`]). `busy` names partitions with an open part, which are
/// skipped. The module docs give the steps.
pub(crate) fn remove_locked(
    hot: &Path,
    source: &str,
    plan: &RemovalPlan,
    busy: &BTreeSet<String>,
    first_hot_date: &str,
    at_ms: i64,
) -> RemovalReport {
    let mut report = RemovalReport {
        source: source.to_owned(),
        ..RemovalReport::default()
    };
    if !plan.names_source(source) {
        return report;
    }
    let prefix = format!("{}/", layout::source_dir(source));
    let ours = |path: &&String| path.starts_with(&prefix);
    let hold = match CopyLedger::at(hot).try_hold() {
        Ok(hold) => hold,
        // A whole-run holder (the rebuild, the hot restore) or an append in
        // flight: the next seal pass tries again.
        Err(error) => {
            for candidate in plan.candidates.iter().chain(&plan.incomplete) {
                if ours(&&candidate.partition) {
                    report.skipped.push((
                        candidate.partition.clone(),
                        format!("the copy-ledger lock is held ({error}); retried at the next pass"),
                    ));
                }
            }
            return report;
        }
    };
    let tail = match hold.records_after(plan.ledger_len, &plan.chain) {
        Ok(tail) => tail,
        Err(error) => {
            report.error = Some(error.to_string());
            return report;
        }
    };

    for (candidate, new) in plan
        .candidates
        .iter()
        .map(|c| (c, true))
        .chain(plan.incomplete.iter().map(|c| (c, false)))
    {
        if !ours(&&candidate.partition) {
            continue;
        }
        let partition = &candidate.partition;
        if let Err(reason) = recheck(hot, candidate, &tail, busy, new.then_some(first_hot_date)) {
            report.skipped.push((partition.clone(), reason));
            continue;
        }
        if new {
            let record = Record {
                at_ms,
                kind: Kind::Removed {
                    partition: partition.clone(),
                },
            };
            match hold.append(&record) {
                Ok(outcome) => report.truncations.extend(outcome.truncation),
                Err(error) => {
                    report.truncations.extend(error.truncation);
                    report.error = Some(format!(
                        "the removed append for {partition} failed, so the pass stops: {}",
                        error.error
                    ));
                    return report;
                }
            }
            if let Err(error) = reached(RemovalStep::AfterRecord) {
                report.error = Some(error);
                return report;
            }
        }
        match rename_and_delete(hot, partition) {
            Ok(()) => {
                if new {
                    report.removed.push(partition.clone());
                } else {
                    report.completed.push(partition.clone());
                }
            }
            Err(error) => {
                report.error = Some(error);
                return report;
            }
        }
    }
    for leftover in plan.leftovers.iter().filter(ours) {
        let path = layout::resolve(hot, leftover);
        match delete_dir(&path) {
            Ok(()) => report.completed.push(leftover.clone()),
            Err(error) => {
                report.error = Some(error.to_string());
                return report;
            }
        }
    }
    report
}

/// The re-check of one planned partition under both locks. `first_hot_date`
/// is `None` for an incomplete removal, whose record is already written.
fn recheck(
    hot: &Path,
    candidate: &Candidate,
    tail: &[Record],
    busy: &BTreeSet<String>,
    first_hot_date: Option<&str>,
) -> Result<(), String> {
    let partition = &candidate.partition;
    if busy.contains(partition) {
        return Err("the writer has an open part in it".to_owned());
    }
    if let Some(first) = first_hot_date {
        let (_, date) = layout::parse_partition_dir(partition).map_err(|e| e.to_string())?;
        if date.as_str() >= first {
            return Err(format!(
                "its date is inside the hot window, which starts on {first}"
            ));
        }
    }
    let files: BTreeSet<&str> = candidate.files.iter().map(String::as_str).collect();
    for record in tail {
        let named = match &record.kind {
            Kind::Manifest { .. } | Kind::Copy { .. } | Kind::LedgerCopy { .. } => false,
            Kind::Removed { partition: p } | Kind::Returned { partition: p } => p == partition,
            Kind::Lost { path, .. }
            | Kind::Hold { path, .. }
            | Kind::HoldCleared { path, .. }
            | Kind::Quarantined { path, .. }
            | Kind::Replaced { path, .. } => {
                files.contains(path.as_str()) || path.starts_with(&format!("{partition}/"))
            }
            Kind::Finding { subject, .. } => match subject {
                Subject::File(path) => {
                    files.contains(path.as_str()) || path.starts_with(&format!("{partition}/"))
                }
                _ => false,
            },
            Kind::Verify { .. } => false,
        };
        if named {
            return Err(format!(
                "a {} record after the plan names it or its files",
                record.kind.name()
            ));
        }
    }
    let dir = layout::resolve(hot, partition);
    let Some(names) = list_entries(&dir).map_err(|e| e.to_string())? else {
        return Err("its directory is gone".to_owned());
    };
    let scan = scan_partition(partition, &names);
    if scan.debris {
        return Err("it holds seal debris (a .tmp file, or data without its manifest)".to_owned());
    }
    if let Some(extra) = scan
        .sealed
        .iter()
        .find(|path| !files.contains(path.as_str()))
    {
        return Err(format!("{extra} was sealed after the plan"));
    }
    // Hot's version is still the ledger's (D5): a few small reads.
    for path in &scan.sealed {
        if read_manifest(hot, path).as_ref() != candidate.manifests.get(path) {
            return Err(differs(path));
        }
    }
    Ok(())
}

/// Renames `partition` to its hidden name, fsyncs, deletes it, fsyncs.
fn rename_and_delete(hot: &Path, partition: &str) -> Result<(), String> {
    let (parent, name) = partition
        .rsplit_once('/')
        .ok_or_else(|| format!("{partition} is not a partition directory"))?;
    let stream_dir = layout::resolve(hot, parent);
    let dir = stream_dir.join(name);
    let hidden = stream_dir.join(format!("{HIDDEN_PREFIX}{name}"));
    delete_dir(&hidden).map_err(|e| e.to_string())?;
    fs::rename(&dir, &hidden).map_err(|e| io_error("rename", &dir)(e).to_string())?;
    sync_dir(&stream_dir).map_err(|e| e.to_string())?;
    reached(RemovalStep::AfterRename)?;
    delete_dir(&hidden).map_err(|e| e.to_string())?;
    reached(RemovalStep::AfterDelete)
}

/// Deletes the directory `path` if it exists, and fsyncs its parent.
fn delete_dir(path: &Path) -> Result<(), RawStoreError> {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error("delete", path)(error)),
    }
    match path.parent() {
        Some(parent) => sync_dir(parent),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests;
