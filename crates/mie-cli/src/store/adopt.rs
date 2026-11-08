//! The adoption of an existing store (ADR-062 D10, D8, D5): `mie store
//! adopt`.
//!
//! Existing data reaches the copy targets without passing through hot: an
//! initialized tiered store adopts a closed single-root store (the #22
//! store, the archive store, a backfill imported into a scratch store). The
//! source store is only read, never written. The run has two phases under
//! locks: a check phase that decides every path before anything is
//! written, then the writes in a fixed order.
//!
//! 0. **Refusals, nothing written.** An empty `cold_roots`, a hot without
//!    its marker, an archive configuration of another store, an
//!    `--import-ledger` without an archive configuration, a `--from` that
//!    is missing, not a directory, holds `.mie/` (a tiered hot is partial
//!    by construction) or equals, contains or lies inside hot or a cold
//!    root, and a writer active on the source. The source's `.lock` files
//!    are opened read-only and only tried, never created.
//! 1. **Locks, never waiting, held for the run.** The pass lock, then the
//!    hot writer of every source the run writes, which runs ADR-030
//!    recovery first: a crashed seal on hot is rolled forward before the
//!    path rule looks at it, instead of being deleted as copy debris. A
//!    crashed adoption's own hot copies are crashed seals too (D5).
//! 2. **Checks, still nothing written.** Hot's copy ledger is readable;
//!    every copy target presents its marker (refused, not skipped as in
//!    the pass: every adopted file needs k copies); ids are distinct and not
//!    `hot`, and the devices too; the source holds no seal debris and no
//!    `PENDING`, and every source part's data matches its manifest (so a
//!    damaged source is refused with nothing written); every recorded file
//!    the source does not list is already *complete* (the one predicate a
//!    PASS also asserts at the end: on hot unless its partition is
//!    recorded removed, and recorded on every copy target), so a re-run of
//!    a stopped adoption against another store can never pass and leave a
//!    recorded file incomplete for good. A file that left hot with at least one
//!    recorded copy on any root id but lacks one on a configured target
//!    (a target added or replaced, every target replaced, or a stopped
//!    adoption) does not block the writes (its partition is recorded
//!    removed, and the run never writes its path); the end of the run fails
//!    naming it with its remedy (`mie store repair` when a configured
//!    target holds a copy, otherwise only a re-adoption of each original
//!    store); every archive part has the entry that lists it with its
//!    manifest's SHA-256, and every listed part is in the source or already
//!    in the store. The **path rule** (D10, D5): each source path's
//!    manifest is compared with the ledger's `manifest` record, hot's
//!    manifest and each target's manifest wherever they exist. Equal
//!    everywhere is an identical path, resumed or skipped; anything else,
//!    and a path the ledger records as lost, held, quarantined or replaced,
//!    is `Integrity`. Each source entry is absent from hot's import ledger
//!    or byte-identical to it, and hot's journal is absent or a byte prefix
//!    of the source journal (so a re-run resumes an interrupted copy).
//! 3. **Writes, in this order, stopping at the first failure:**
//!    1. `removed` for every source partition older than the first hot
//!       date (D8) that has no directory on hot and is not recorded
//!       removed yet. First, so an append to those dates recalls the
//!       partition from the first byte on instead of numbering from hot's
//!       empty listing (D6). Adoption deletes nothing: the record only says
//!       that hot holds no copy.
//!    2. The `manifest` record of every source file the ledger does not
//!       know yet, before any copy. From the first published byte on, the
//!       ledger then knows every part of every adopted partition, and the
//!       writer's union part numbering (D6) numbers a new part above all of
//!       them, so a capture that restarts after a crash can never seal a
//!       path a target holds, or will hold, with other bytes (review of
//!       #200).
//!    3. Per file in path order: to every copy target without a `copy`
//!       record (marker re-read before and after the copy, then the `copy`
//!       record), then to hot when its partition is on hot's side.
//!    4. The source's import entries to hot's import ledger, after all
//!       their parts.
//!    5. The source journal to hot's journal, as a byte-prefix extension.
//!    6. One replication pass under the held pass lock, so the ledger, the
//!       journal and the entries have k copies before the run returns.
//!
//! A re-run with the same arguments resumes: identical paths are only
//! completed. Stopping at the first failure is deliberate: a store cannot
//! be half adopted on purpose, and going on would only copy more bytes
//! before the same failure.

use super::replicate::{self, Entry, Fold, LedgerAppend, PassObserver};
use super::{DeviceProbe, check_devices, check_hot, exists, raw_store, truncation_line};
use crate::config::IngestConfig;
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_binance::archive::ledger::PENDING_FILE;
use mie_adapter_parquet::copy_ledger::{self, AppendError, CopyLedger, Kind, Record, Truncation};
use mie_adapter_parquet::replica::{self, CopyError, CopyOutcome, Side, TailSource};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::{RawWriter, RotationPolicy, TierContext};
use mie_ports::raw::{RawStoreError, SealedFile, validate_segment};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, TryLockError};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// What to adopt.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdoptRequest {
    /// The root of the store to adopt.
    pub from: PathBuf,
    /// Its capture journal, copied to the configured journal on hot.
    pub journal: Option<PathBuf>,
    /// Its import ledger, whose entries are copied to hot's import ledger.
    pub import_ledger: Option<PathBuf>,
    /// The archive configuration of this store (else `[history]
    /// archive_config`).
    pub archive_config: Option<PathBuf>,
}

/// What one adoption did: its report lines and every problem that makes it
/// fail.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AdoptReport {
    /// What was adopted and copied, in order, then the pass's lines.
    pub lines: Vec<String>,
    /// Every refusal and failure; empty when the adoption passed.
    pub problems: Vec<String>,
}

/// The steps a test can stop the adoption after, as a crash would.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdoptStep {
    /// Every `removed` record was appended.
    AfterRemovedRecords,
    /// A file was published on a copy target, before its `copy` record.
    AfterTargetPublish,
    /// A `manifest` record was appended, before any copy.
    AfterManifestRecord,
    /// A `copy` record was appended.
    AfterCopyRecord,
    /// A file was copied to hot.
    AfterHotCopy,
    /// An import entry was copied to hot.
    AfterEntry,
    /// The journal was copied to hot.
    AfterJournal,
}

#[cfg(test)]
impl AdoptStep {
    pub(crate) const ALL: [Self; 7] = [
        Self::AfterRemovedRecords,
        Self::AfterTargetPublish,
        Self::AfterManifestRecord,
        Self::AfterCopyRecord,
        Self::AfterHotCopy,
        Self::AfterEntry,
        Self::AfterJournal,
    ];
}

/// A test hook that sees every adoption step.
#[cfg(test)]
type StepHook = Box<dyn FnMut(AdoptStep)>;

#[cfg(test)]
thread_local! {
    /// Test hook: the adoption stops at the first occurrence of this step.
    pub(crate) static CRASH_AT: std::cell::Cell<Option<AdoptStep>> =
        const { std::cell::Cell::new(None) };
    /// Test hook: runs at every adoption step, before a crash there.
    pub(crate) static ON_STEP: std::cell::RefCell<Option<StepHook>> =
        std::cell::RefCell::new(None);
}

/// Why the adoption stopped early.
enum Stop {
    /// A refusal or a failure, already in the report.
    Failed,
    /// A simulated crash (tests).
    #[cfg(test)]
    Crash,
}

/// Marks a step; under test, runs the step hook and stops there once.
fn reached(step: AdoptStep) -> Result<(), Stop> {
    #[cfg(test)]
    ON_STEP.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(step);
        }
    });
    #[cfg(test)]
    if CRASH_AT.with(std::cell::Cell::get) == Some(step) {
        CRASH_AT.with(|crash| crash.set(None));
        return Err(Stop::Crash);
    }
    let _ = step;
    Ok(())
}

/// Collects every copy-ledger truncation as a report line.
#[derive(Default)]
struct Truncations(Vec<String>);

impl PassObserver for Truncations {
    fn ledger_truncated(&mut self, ledger: &Path, truncation: Truncation) {
        self.0.push(truncation_line(ledger, truncation));
    }
}

/// A copy target that presented its marker.
pub(super) struct Target {
    pub(super) path: PathBuf,
    pub(super) id: String,
}

/// One sealed file of the source, as the check phase decided it.
struct SourceFile {
    path: String,
    partition: String,
    text: String,
    file: SealedFile,
}

/// Everything the check phase decided; the writes only carry it out.
struct Plan {
    targets: Vec<Target>,
    files: Vec<SourceFile>,
    /// Partitions recorded `removed` by write 1.
    to_remove: Vec<String>,
    /// Partitions whose files also go to hot.
    hot_side: BTreeSet<String>,
    /// The source's entries, with hot's import ledger they go to.
    entries: Vec<Entry>,
    hot_import_ledger: Option<PathBuf>,
    /// The source journal and its length.
    journal: Option<(PathBuf, u64)>,
}

/// The adoption in progress.
struct Run<'a> {
    config: &'a IngestConfig,
    request: &'a AdoptRequest,
    first_hot_date: &'a str,
    probe: &'a dyn DeviceProbe,
    now_ms: &'a dyn Fn() -> i64,
    ledger: CopyLedger,
    truncations: Truncations,
    fold: Fold,
    report: AdoptReport,
}

/// Runs one adoption (module docs). Every outcome is in the report.
pub fn run(
    config: &IngestConfig,
    request: &AdoptRequest,
    first_hot_date: &str,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
) -> AdoptReport {
    let mut run = Run {
        config,
        request,
        first_hot_date,
        probe,
        now_ms,
        ledger: CopyLedger::at(&config.paths.raw_root),
        truncations: Truncations::default(),
        fold: Fold::default(),
        report: AdoptReport::default(),
    };
    match run.adopt() {
        Ok(()) | Err(Stop::Failed) => {}
        #[cfg(test)]
        Err(Stop::Crash) => run
            .report
            .problems
            .push("simulated crash: the adoption stopped".to_owned()),
    }
    let mut report = run.report;
    report.lines.extend(run.truncations.0);
    report
}

/// The integrity error text of a path that differs (D5).
fn integrity(detail: String) -> String {
    RawStoreError::Integrity(detail).to_string()
}

/// The `source=<s>` values of `root`'s layout that are directories.
fn source_names(root: &Path) -> Result<Vec<String>, String> {
    let names = replicate::sorted_names(root)
        .map_err(|e| format!("--from {}: cannot list it: {e}", root.display()))?;
    Ok(names
        .into_iter()
        .filter_map(|name| name.strip_prefix("source=").map(str::to_owned))
        .filter(|source| {
            validate_segment(source).is_ok() && root.join(format!("source={source}")).is_dir()
        })
        .collect())
}

/// The partition directory of a data path.
pub(super) fn partition_of(path: &str) -> &str {
    path.rsplit_once('/')
        .map_or(path, |(partition, _)| partition)
}

/// The date of a partition directory.
fn date_of(partition: &str) -> &str {
    partition.rsplit_once("/date=").map_or("", |(_, date)| date)
}

/// Whether the first `len` bytes of `a` and `b` are equal, streamed.
fn same_prefix(a: &Path, b: &Path, len: u64) -> std::io::Result<bool> {
    let (mut a, mut b) = (File::open(a)?, File::open(b)?);
    let (mut left, mut buf_a, mut buf_b) = (len, vec![0; 1 << 16], vec![0; 1 << 16]);
    while left > 0 {
        let n = (left.min(buf_a.len() as u64)) as usize;
        a.read_exact(&mut buf_a[..n])?;
        b.read_exact(&mut buf_b[..n])?;
        if buf_a[..n] != buf_b[..n] {
            return Ok(false);
        }
        left -= n as u64;
    }
    Ok(true)
}

impl Run<'_> {
    fn line(&mut self, line: String) {
        self.report.lines.push(line);
    }

    /// Records a refusal or failure and stops.
    fn fail(&mut self, problem: String) -> Stop {
        self.report.problems.push(problem);
        Stop::Failed
    }

    /// Appends one record; a truncation is reported first, also when the
    /// append fails, and a failed append stops the run.
    fn append(&mut self, kind: Kind) -> Result<(), Stop> {
        let record = Record {
            at_ms: (self.now_ms)(),
            kind,
        };
        let (truncation, result) = match self.ledger.append(&record) {
            Ok(outcome) => (outcome.truncation, Ok(())),
            Err(AppendError { error, truncation }) => (truncation, Err(error)),
        };
        if let Some(truncation) = truncation {
            let path = LedgerAppend::path(&self.ledger).to_owned();
            self.truncations.ledger_truncated(&path, truncation);
        }
        result.map_err(|error| {
            let message = format!(
                "the {} append to the copy ledger {} failed, so the adoption stops: {error}",
                record.kind.name(),
                LedgerAppend::path(&self.ledger).display()
            );
            self.fail(message)
        })
    }

    /// Whether `target` still presents its marker; a missing or changed
    /// marker stops the run, since records for an unmounted mount point
    /// would name copies the returning mount hides (D11).
    fn marker_present(&mut self, target: &Target) -> Result<(), Stop> {
        match root_marker::read(&target.path) {
            Ok(Some(id)) if id == target.id => Ok(()),
            Ok(Some(other)) => Err(self.fail(format!(
                "copy target {} ({}): its marker now carries the id {other:?}, so the adoption \
                 stops; re-run it once the target is back (ADR-062 D11)",
                target.path.display(),
                target.id
            ))),
            Ok(None) => Err(self.fail(format!(
                "copy target {} ({}): its marker is gone (an unmounted mount point?), so the \
                 adoption stops; re-run it once the target is back (ADR-062 D11)",
                target.path.display(),
                target.id
            ))),
            Err(error) => Err(self.fail(format!(
                "copy target {} ({}): its marker cannot be read ({error}), so the adoption stops",
                target.path.display(),
                target.id
            ))),
        }
    }

    fn adopt(&mut self) -> Result<(), Stop> {
        self.refusals()?;
        // 1. Locks, held until the run ends.
        let hot = self.config.paths.raw_root.clone();
        let lock = replica::try_replication_lock(&hot).map_err(|e| self.fail(e.to_string()))?;
        let writers = self.writers()?;
        // 2. Checks.
        let plan = self.check()?;
        // 3. Writes.
        let result = self.write(&plan, lock);
        for (source, writer) in writers {
            if let Err(error) = writer.close() {
                self.report.problems.push(format!(
                    "close the writer of source {source} on hot: {error}"
                ));
            }
        }
        result
    }

    /// Step 0: the refusals before anything is written.
    fn refusals(&mut self) -> Result<(), Stop> {
        let config = self.config;
        let hot = config.paths.raw_root.as_path();
        let cold_roots = &config.paths.cold_roots;
        if cold_roots.is_empty() {
            return Err(self.fail(
                "paths.cold_roots is empty: a single-root store has no copy targets to adopt \
                 into (ADR-062 D10)"
                    .to_owned(),
            ));
        }
        check_hot(hot, cold_roots).map_err(|e| self.fail(e))?;
        let archive = replicate::archive_config(config, self.request.archive_config.as_deref())
            .map_err(|e| self.fail(e))?;
        if self.request.import_ledger.is_some() && archive.is_none() {
            return Err(self.fail(
                "--import-ledger needs this store's archive configuration (--archive-config, \
                 else [history] archive_config): it names hot's import ledger, which the \
                 entries are copied to"
                    .to_owned(),
            ));
        }
        let from = self.request.from.as_path();
        match fs::metadata(from) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(self.fail(format!("--from {} is not a directory", from.display())));
            }
            Err(error) => {
                return Err(self.fail(format!("--from {}: {error}", from.display())));
            }
        }
        let meta = root_marker::meta_dir(from);
        if exists(&meta).map_err(|e| self.fail(e))? {
            return Err(self.fail(format!(
                "--from {} holds {}: it is a root of a tiered store, whose hot holds only the hot \
                 window, so its layout is partial by construction. Adopt the single-root store \
                 itself (ADR-062 D10)",
                from.display(),
                meta.display()
            )));
        }
        let from_canonical = fs::canonicalize(from)
            .map_err(|e| self.fail(format!("--from {}: {e}", from.display())))?;
        let roots = std::iter::once(("raw_root", hot)).chain(
            cold_roots
                .iter()
                .map(|root| ("cold root", root.path.as_path())),
        );
        for (what, root) in roots {
            let canonical = fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
            if from_canonical.starts_with(&canonical) || canonical.starts_with(&from_canonical) {
                return Err(self.fail(format!(
                    "--from {} overlaps the {what} {}: the store to adopt must lie outside hot \
                     and every cold root, which the adoption writes and which must never be \
                     read as its source (ADR-062 D10)",
                    from.display(),
                    root.display()
                )));
            }
        }
        // A writer on the source: its `.lock` is only tried, read-only, and
        // a missing one is never created, so the source stays untouched.
        for source in source_names(from).map_err(|e| self.fail(e))? {
            let path = from.join(format!("source={source}")).join(".lock");
            let file = match File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(self.fail(format!("--from: open {}: {error}", path.display())));
                }
            };
            match file.try_lock() {
                Ok(()) => drop(file),
                Err(TryLockError::WouldBlock) => {
                    return Err(self.fail(format!(
                        "a writer holds source {source} of --from ({}): only a closed store is \
                         adopted; stop its capture or import first",
                        path.display()
                    )));
                }
                Err(TryLockError::Error(error)) => {
                    return Err(self.fail(format!("--from: lock {}: {error}", path.display())));
                }
            }
        }
        Ok(())
    }

    /// The hot writer of every source the run writes, never appended to:
    /// held for the source lock (D6), and opened for its recovery.
    fn writers(&mut self) -> Result<Vec<(String, RawWriter)>, Stop> {
        let config = self.config;
        let store = raw_store(&config.paths.raw_root, &config.paths.cold_roots)
            .map_err(|e| self.fail(e))?;
        let mut sources: BTreeSet<String> = source_names(&self.request.from)
            .map_err(|e| self.fail(e))?
            .into_iter()
            .collect();
        if self.request.journal.is_some() {
            sources.insert(config.instrument.source.clone());
        }
        if self.request.import_ledger.is_some() {
            sources.insert(ARCHIVE_SOURCE.to_owned());
        }
        let mut writers = Vec::new();
        // They never append, so the clock is only a stand-in for a recall
        // that never runs.
        let opened_ms = (self.now_ms)();
        for source in sources {
            let ctx = TierContext {
                first_hot_date: self.first_hot_date.to_owned(),
                now_ms: Arc::new(move || opened_ms),
                on_recall: None,
            };
            let writer = store
                .tiered_writer(&source, RotationPolicy::default(), ctx)
                .map_err(|e| {
                    self.fail(format!("open the writer of source {source} on hot: {e}"))
                })?;
            let recovery = writer.recovery();
            if !recovery.is_clean() {
                let line = format!(
                    "recovery of source {source} on hot: rolled forward {:?}, discarded {:?}",
                    recovery.rolled_forward, recovery.discarded
                );
                self.line(line);
            }
            writers.push((source, writer));
        }
        Ok(writers)
    }

    /// Step 2: every check, nothing written. Returns the plan.
    fn check(&mut self) -> Result<Plan, Stop> {
        let config = self.config;
        let hot = config.paths.raw_root.as_path();
        let from = self.request.from.clone();

        // Hot's ledger, folded.
        let ledger_path = copy_ledger::ledger_path(hot);
        let rebuild = "the adoption records every copy in it; the copy-ledger rebuild restores it \
                       (ADR-062 D7)";
        let records = match copy_ledger::read(hot) {
            Ok(Some(ledger)) => ledger.records,
            Ok(None) => {
                return Err(self.fail(format!(
                    "copy ledger {} does not exist: {rebuild}",
                    ledger_path.display()
                )));
            }
            Err(error) => {
                return Err(self.fail(format!(
                    "copy ledger {} cannot be read ({error}): {rebuild}",
                    ledger_path.display()
                )));
            }
        };
        self.fold = Fold::new(&records)
            .map_err(|e| self.fail(format!("copy ledger {}: {e}", ledger_path.display())))?;
        let (removed, marked) = ledger_states(&records);

        // The copy targets: every one must present its marker.
        let mut targets: Vec<Target> = Vec::new();
        for root in config
            .paths
            .cold_roots
            .iter()
            .filter(|root| root.copy_target)
        {
            match root_marker::read(&root.path) {
                Ok(Some(id)) if id == HOT_ID => {
                    return Err(self.fail(format!(
                        "copy target {} carries hot's marker (id {HOT_ID:?}): the same root under \
                         two names, or a copied marker (ADR-062 D7)",
                        root.path.display()
                    )));
                }
                Ok(Some(id)) => {
                    if let Some(other) = targets.iter().find(|target| target.id == id) {
                        return Err(self.fail(format!(
                            "copy targets {} and {} both carry the id {id:?}: two spellings of \
                             one root, or a copied marker (ADR-062 D7)",
                            other.path.display(),
                            root.path.display()
                        )));
                    }
                    targets.push(Target {
                        path: root.path.clone(),
                        id,
                    });
                }
                Ok(None) => {
                    return Err(self.fail(format!(
                        "copy target {} has no root marker: the adoption refuses, since every \
                         adopted file needs a copy on every copy target, and an unmounted mount \
                         point is an empty directory (ADR-062 D10, D11)",
                        root.path.display()
                    )));
                }
                Err(error) => {
                    return Err(self.fail(format!(
                        "copy target {}: its root marker cannot be read ({error})",
                        root.path.display()
                    )));
                }
            }
        }
        let paths: Vec<&Path> = targets.iter().map(|target| target.path.as_path()).collect();
        check_devices(hot, &paths, self.probe).map_err(|e| self.fail(e))?;

        // The source: no seal debris, no pending import.
        let unsealed = replica::list_unsealed(&from)
            .map_err(|e| self.fail(format!("--from {}: {e}", from.display())))?;
        if !unsealed.is_empty() {
            return Err(self.fail(format!(
                "--from {} holds the debris of an interrupted seal: {}. The adoption never \
                 writes the store it adopts, so it cannot recover it: open that store's writer \
                 once (any start of its capture or import runs the ADR-030 recovery), then adopt \
                 again",
                from.display(),
                unsealed.join(", ")
            )));
        }
        let entries = match &self.request.import_ledger {
            Some(ledger) => self.source_entries(ledger)?,
            None => Vec::new(),
        };

        // The source's sealed files, their manifests, and their data
        // verified against them.
        let listed = replica::list_sealed(&from)
            .map_err(|e| self.fail(format!("--from {}: {e}", from.display())))?;
        let mut files = Vec::new();
        for path in listed {
            let (text, file) = match replica::sealed_manifest(&from, &path) {
                Ok(Some(found)) => found,
                Ok(None) => {
                    return Err(self.fail(format!(
                        "--from {}: the manifest of {path} disappeared while it was read",
                        from.display()
                    )));
                }
                Err(error) => {
                    return Err(self.fail(format!("--from {}: {error}", from.display())));
                }
            };
            // The data must match its manifest now, before any write: a
            // damaged source found while it is copied would stop the run
            // with records and copies already written.
            replica::verify_sealed(&from, &file).map_err(|error| {
                self.fail(format!(
                    "--from {}: {error}; the store is not adopted and nothing was written",
                    from.display()
                ))
            })?;
            files.push(SourceFile {
                partition: partition_of(&path).to_owned(),
                path,
                text,
                file,
            });
        }
        self.check_archive(&files, &entries)?;

        // The path rule (D10, D5).
        for file in &files {
            self.path_rule(file, &targets, &marked)?;
        }

        // Which partitions go to hot, and which are recorded removed.
        let partitions: BTreeSet<&str> = files.iter().map(|f| f.partition.as_str()).collect();
        let (mut to_remove, mut hot_side) = (Vec::new(), BTreeSet::new());
        for partition in partitions {
            if removed.contains(partition) {
                continue;
            }
            let on_hot = exists(&hot.join(partition)).map_err(|e| self.fail(e))?;
            if date_of(partition) >= self.first_hot_date || on_hot {
                hot_side.insert(partition.to_owned());
            } else {
                to_remove.push(partition.to_owned());
            }
        }

        // Every recorded file the source does not list must already be in
        // the end state an adoption guarantees: with `removed` as this run
        // will leave it.
        let mut removed_after = removed.clone();
        removed_after.extend(to_remove.iter().cloned());
        self.check_unlisted(&files, &targets, &removed_after, &marked)?;

        // Hot's side: entries and the journal.
        let hot_import_ledger = match &self.request.import_ledger {
            Some(_) => replicate::archive_config(config, self.request.archive_config.as_deref())
                .map_err(|e| self.fail(e))?
                .map(|archive| archive.paths.import_ledger),
            None => None,
        };
        if let Some(hot_ledger) = &hot_import_ledger {
            for entry in &entries {
                let dest = hot_ledger.join(&entry.segment).join(&entry.file_name);
                let source = fs::read(&entry.path)
                    .map_err(|e| self.fail(format!("read {}: {e}", entry.path.display())))?;
                match fs::read(&dest) {
                    Ok(present) if present == source => {}
                    Ok(_) => {
                        return Err(self.fail(integrity(format!(
                            "import entry {} on hot ({}) differs from the one in --import-ledger \
                             ({}); an import entry is byte-identical everywhere, so the store is \
                             not adopted and nothing was written (ADR-062 D5, D10)",
                            entry.name,
                            dest.display(),
                            entry.path.display()
                        ))));
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(self.fail(format!("read {}: {error}", dest.display())));
                    }
                }
            }
        }
        let journal = match &self.request.journal {
            Some(source) => Some(self.check_journal(source)?),
            None => None,
        };
        Ok(Plan {
            targets,
            files,
            to_remove,
            hot_side,
            entries,
            hot_import_ledger,
            journal,
        })
    }

    /// The source import ledger's entries; `PENDING` and an unreadable
    /// entry are refused.
    fn source_entries(&mut self, ledger: &Path) -> Result<Vec<Entry>, Stop> {
        if !ledger.is_dir() {
            return Err(self.fail(format!(
                "--import-ledger {} is not a directory",
                ledger.display()
            )));
        }
        let pending = ledger.join(PENDING_FILE);
        if exists(&pending).map_err(|e| self.fail(e))? {
            return Err(self.fail(format!(
                "--import-ledger holds {}: an import into the store to adopt is pending (ADR-034 \
                 D4); finish it with that store's `mie archive-import` first",
                pending.display()
            )));
        }
        let (entries, problems) = replicate::hot_entries(ledger);
        if !problems.is_empty() {
            self.report.problems.extend(problems);
            return Err(Stop::Failed);
        }
        Ok(entries)
    }

    /// Every archive part has an entry that lists it with its manifest's
    /// SHA-256 (D11 *Archive parts wait for their import entry*), and every
    /// part an entry lists is in the source or already in the store.
    fn check_archive(&mut self, files: &[SourceFile], entries: &[Entry]) -> Result<(), Stop> {
        for file in files.iter().filter(|f| replicate::is_archive_part(&f.path)) {
            if self.request.import_ledger.is_none() {
                return Err(self.fail(format!(
                    "{} is an archive part, and no --import-ledger is given: an archive part is \
                     adopted only together with the import entry that lists it (ADR-062 D11)",
                    file.path
                )));
            }
            let listed = entries.iter().any(|entry| {
                entry
                    .files
                    .contains(&(file.path.clone(), file.file.sha256.clone()))
            });
            if !listed {
                return Err(self.fail(format!(
                    "{} is an archive part that no import entry in --import-ledger lists with its \
                     manifest's SHA-256: an orphan part (an import that crashed before its \
                     entry?) is never adopted (ADR-062 D11)",
                    file.path
                )));
            }
        }
        let hot = self.config.paths.raw_root.clone();
        for entry in entries {
            for (path, sha) in &entry.files {
                if let Some(file) = files.iter().find(|f| f.path == *path) {
                    if file.file.sha256 != *sha {
                        return Err(self.fail(integrity(format!(
                            "import entry {} lists {path} with another SHA-256 than its manifest \
                             in --from (ADR-062 D11)",
                            entry.name
                        ))));
                    }
                    continue;
                }
                // Not in the source: an earlier adoption may have brought it.
                let known = match self.fold.manifests.get(path) {
                    Some(text) => replica::parse_manifest(text).ok().map(|f| f.sha256),
                    None => match replica::sealed_manifest(&hot, path) {
                        Ok(found) => found.map(|(_, f)| f.sha256),
                        Err(error) => {
                            return Err(self.fail(format!("hot: {error}")));
                        }
                    },
                };
                if known.as_deref() != Some(sha.as_str()) {
                    return Err(self.fail(format!(
                        "import entry {} lists {path}, which neither --from nor this store holds \
                         with that SHA-256: the entry cannot be adopted without its parts \
                         (ADR-062 D11)",
                        entry.name
                    )));
                }
            }
        }
        Ok(())
    }

    /// The refusal of a recorded file the source does not list and that is
    /// not [`complete`]: an adoption that stopped recorded it and this run
    /// reads another store, or a replication pass has not finished it.
    /// Passing would leave a recorded file that no root completes, or a
    /// file below k, for good. Nothing is written.
    fn check_unlisted(
        &mut self,
        files: &[SourceFile],
        targets: &[Target],
        removed: &BTreeSet<String>,
        marked: &BTreeMap<String, &'static str>,
    ) -> Result<(), Stop> {
        let listed: BTreeSet<&str> = files.iter().map(|f| f.path.as_str()).collect();
        let hot = self.config.paths.raw_root.clone();
        let (mut stopped, mut repair, mut pass) = (Vec::new(), Vec::new(), Vec::new());
        for path in self.fold.manifests.keys() {
            if listed.contains(path.as_str()) || marked.contains_key(path) {
                continue;
            }
            match complete(&hot, &self.fold, path, targets, removed) {
                Ok(None) => {}
                Ok(Some(gap)) if gap.needs_refill() => {
                    repair.push((format!("{path} ({gap})"), gap.repairable()));
                }
                Ok(Some(gap)) if gap.needs_pass() => pass.push(format!("{path} ({gap})")),
                Ok(Some(gap)) => stopped.push(format!("{path} ({gap})")),
                Err(error) => stopped.push(format!("{path} ({error})")),
            }
        }
        let tail = "until then the writer refuses new parts in those partitions. Nothing was \
                    written (ADR-062 D6, D10)";
        let mut problems = Vec::new();
        if !stopped.is_empty() {
            problems.push(format!(
                "the copy ledger records {}, which --from does not list and which is not complete \
                 (on hot unless its partition is recorded removed, and recorded on every copy \
                 target): an adoption that stopped recorded it and --from is not the store it \
                 read. Re-run the adoption with the store it read; {tail}",
                stopped.join(", ")
            ));
        }
        // A file that only needs the repair does not block the writes: its
        // partition is recorded removed (so an append recalls it, and the
        // recall names a file no root holds), and this run never writes its
        // path. Blocking would
        // deadlock two adopted stores that each wait for the other's files
        // on a new target; the end-of-run check names it and fails the run.
        if !stopped.is_empty() {
            problems.extend(refill_problems(&repair));
        }
        if !pass.is_empty() {
            problems.push(format!(
                "the copy ledger records {}, which --from does not list and which hot holds \
                 without a copy record on every copy target: run `mie store replicate` first. \
                 Nothing was written (ADR-062 D10, D11)",
                pass.join(", ")
            ));
        }
        if problems.is_empty() {
            return Ok(());
        }
        self.report.problems.extend(problems);
        Err(Stop::Failed)
    }

    /// The path rule for one source file (module docs).
    fn path_rule(
        &mut self,
        file: &SourceFile,
        targets: &[Target],
        marked: &BTreeMap<String, &'static str>,
    ) -> Result<(), Stop> {
        let path = &file.path;
        if let Some(state) = marked.get(path) {
            return Err(self.fail(integrity(format!(
                "{path}: the copy ledger records it as {state}, so it is never adopted again; \
                 the store is not adopted and nothing was written (ADR-062 D10, D11)"
            ))));
        }
        let mut differs = Vec::new();
        if self.fold.conflicts.contains(path) {
            differs.push("the copy ledger (two manifest texts)".to_owned());
        } else if self
            .fold
            .manifests
            .get(path)
            .is_some_and(|known| *known != file.text)
        {
            differs.push("the copy ledger's manifest record".to_owned());
        }
        let hot = self.config.paths.raw_root.clone();
        let roots = std::iter::once(("hot".to_owned(), hot)).chain(targets.iter().map(|t| {
            (
                format!("copy target {} ({})", t.id, t.path.display()),
                t.path.clone(),
            )
        }));
        for (name, root) in roots {
            match replica::sealed_manifest(&root, path) {
                Ok(Some((text, _))) if text != file.text => differs.push(name),
                Ok(_) => {}
                Err(error) => return Err(self.fail(format!("{name}: {error}"))),
            }
        }
        if differs.is_empty() {
            return Ok(());
        }
        Err(self.fail(integrity(format!(
            "{path}: its manifest in --from differs from {}; the same path must carry \
             byte-identical manifests everywhere, so the store is not adopted and nothing was \
             written (ADR-062 D5, D10)",
            differs.join(", ")
        ))))
    }

    /// The journal rule: hot's journal is absent or a byte prefix of the
    /// source journal. Returns the source and its length.
    fn check_journal(&mut self, source: &Path) -> Result<(PathBuf, u64), Stop> {
        let length = match fs::metadata(source) {
            Ok(metadata) if metadata.is_file() => metadata.len(),
            Ok(_) => {
                return Err(self.fail(format!("--journal {} is not a file", source.display())));
            }
            Err(error) => {
                return Err(self.fail(format!("--journal {}: {error}", source.display())));
            }
        };
        let hot_journal = self.config.paths.journal.clone();
        let present = match fs::metadata(&hot_journal) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok((source.into(), length));
            }
            Err(error) => {
                return Err(self.fail(format!("journal {}: {error}", hot_journal.display())));
            }
        };
        let prefix = present <= length
            && same_prefix(&hot_journal, source, present)
                .map_err(|e| self.fail(format!("compare the journals: {e}")))?;
        if !prefix {
            return Err(self.fail(format!(
                "journal {} ({present} bytes) is not a byte prefix of --journal {} ({length} \
                 bytes): the journal is copied only into an absent or empty one, or one an \
                 interrupted adoption of this journal began (ADR-062 D10)",
                hot_journal.display(),
                source.display()
            )));
        }
        Ok((source.into(), length))
    }

    /// Step 3: the writes, then the pass.
    fn write(&mut self, plan: &Plan, lock: replica::ReplicationLock) -> Result<(), Stop> {
        let config = self.config;
        let hot = config.paths.raw_root.clone();
        let from = self.request.from.clone();

        // 1. `removed` first.
        for partition in &plan.to_remove {
            self.append(Kind::Removed {
                partition: partition.clone(),
            })?;
        }
        reached(AdoptStep::AfterRemovedRecords)?;

        // 2. The `manifest` record of every file the ledger does not know,
        // before any copy: from the first publish on, the writer's union
        // numbering knows every part of every adopted partition (D6).
        let mut recorded = BTreeSet::new();
        for file in &plan.files {
            if self.fold.manifests.contains_key(&file.path) {
                continue;
            }
            self.append(Kind::Manifest {
                text: file.text.clone(),
            })?;
            self.fold
                .manifests
                .insert(file.path.clone(), file.text.clone());
            recorded.insert(file.path.as_str());
            reached(AdoptStep::AfterManifestRecord)?;
        }

        // 3. Every file: the targets, then hot.
        let (mut adopted, mut bytes, mut identical, mut to_hot) = (0u64, 0u64, 0u64, 0u64);
        let mut per_partition: BTreeMap<&str, u64> = BTreeMap::new();
        for file in &plan.files {
            let path = file.path.as_str();
            let mut wrote = recorded.contains(path);
            for target in &plan.targets {
                if self.fold.has_copy(path, &target.id) {
                    continue;
                }
                self.marker_present(target)?;
                replica::copy_sealed(&from, &target.path, path).map_err(|error| {
                    let side = match error.side {
                        Side::Source => "--from".to_owned(),
                        Side::Destination => format!("copy target {}", target.id),
                    };
                    self.fail(format!(
                        "copying {path} to copy target {} failed on {side}, so the adoption \
                         stops; a re-run resumes it: {}",
                        target.id, error.error
                    ))
                })?;
                reached(AdoptStep::AfterTargetPublish)?;
                self.marker_present(target)?;
                self.append(Kind::Copy {
                    path: path.to_owned(),
                    root: target.id.clone(),
                })?;
                self.fold
                    .copies
                    .entry(path.to_owned())
                    .or_default()
                    .insert(target.id.clone());
                reached(AdoptStep::AfterCopyRecord)?;
                wrote = true;
            }
            if plan.hot_side.contains(&file.partition) {
                match replica::copy_sealed(&from, &hot, path) {
                    Ok((CopyOutcome::Copied, _)) => {
                        to_hot += 1;
                        wrote = true;
                        reached(AdoptStep::AfterHotCopy)?;
                    }
                    Ok((CopyOutcome::AlreadyPresent, _)) => {}
                    Err(CopyError { side, error }) => {
                        let side = match side {
                            Side::Source => "--from",
                            Side::Destination => "hot",
                        };
                        return Err(self.fail(format!(
                            "copying {path} to hot failed on {side}, so the adoption stops; a \
                             re-run resumes it: {error}"
                        )));
                    }
                }
            }
            if wrote {
                adopted += 1;
                bytes += file.file.bytes;
                *per_partition.entry(&file.partition).or_default() += 1;
            } else {
                identical += 1;
            }
        }

        // 4. The entries, after all their parts.
        let mut entries = 0u64;
        if let Some(hot_ledger) = &plan.hot_import_ledger {
            for entry in &plan.entries {
                let dest = hot_ledger.join(&entry.segment).join(&entry.file_name);
                match replica::copy_immutable(&entry.path, &dest) {
                    Ok(CopyOutcome::Copied) => {
                        entries += 1;
                        reached(AdoptStep::AfterEntry)?;
                    }
                    Ok(CopyOutcome::AlreadyPresent) => {}
                    Err(error) => {
                        return Err(self.fail(format!(
                            "copying import entry {} to hot ({}) failed, so the adoption stops; \
                             a re-run resumes it: {error}",
                            entry.name,
                            dest.display()
                        )));
                    }
                }
            }
        }

        // 5. The journal.
        let mut journal_bytes = 0;
        if let Some((source, length)) = &plan.journal {
            let dest = config.paths.journal.clone();
            journal_bytes = replica::extend_prefix(TailSource::File(source), &dest, *length)
                .map_err(|error| {
                    self.fail(format!(
                        "copying the journal {} to {} failed, so the adoption stops; a re-run \
                         resumes it: {error}",
                        source.display(),
                        dest.display()
                    ))
                })?;
            reached(AdoptStep::AfterJournal)?;
        }

        // The report.
        let ids: Vec<&str> = plan.targets.iter().map(|t| t.id.as_str()).collect();
        for (partition, count) in &per_partition {
            let side = if plan.hot_side.contains(*partition) {
                "hot"
            } else {
                "removed from hot"
            };
            self.line(format!(
                "{partition}: {count} file(s) to {}; {side}",
                ids.join(", ")
            ));
        }
        self.line(format!(
            "adopted {adopted} file(s), {bytes} byte(s), {} stream-date(s); recorded {} \
             partition(s) removed from hot; copied {to_hot} file(s) to hot; skipped {identical} \
             identical file(s); copied {entries} import entr{} and {journal_bytes} journal \
             byte(s) to hot",
            per_partition.len(),
            plan.to_remove.len(),
            if entries == 1 { "y" } else { "ies" }
        ));

        // 6. One pass under the held lock.
        let pass = replicate::run_locked(
            config,
            self.request.archive_config.as_deref(),
            self.probe,
            self.now_ms,
            &self.ledger,
            &mut self.truncations,
            lock,
            &BTreeSet::new(),
        );
        self.report.lines.extend(pass.lines);
        self.report.problems.extend(pass.problems);
        // Also after a failed pass, so the repair class is named whatever
        // else is short.
        self.assert_complete(&plan.targets);
        Ok(())
    }

    /// The end of a run that is about to pass: every recorded file is
    /// [`complete`], so a PASS always leaves every adopted partition that
    /// hot holds complete there and every file at k copies (D6). A gap is a
    /// problem, and the run fails.
    fn assert_complete(&mut self, targets: &[Target]) {
        let hot = self.config.paths.raw_root.clone();
        let records = match copy_ledger::read(&hot) {
            Ok(Some(ledger)) => ledger.records,
            Ok(None) => {
                self.report
                    .problems
                    .push("the copy ledger disappeared during the adoption".to_owned());
                return;
            }
            Err(error) => {
                self.report.problems.push(format!(
                    "the copy ledger cannot be read after the adoption: {error}"
                ));
                return;
            }
        };
        let fold = match Fold::new(&records) {
            Ok(fold) => fold,
            Err(error) => {
                self.report
                    .problems
                    .push(format!("the copy ledger: {error}"));
                return;
            }
        };
        let (removed, marked) = ledger_states(&records);
        let mut repair = Vec::new();
        for path in fold.manifests.keys() {
            if marked.contains_key(path) {
                continue;
            }
            let gap = match complete(&hot, &fold, path, targets, &removed) {
                Ok(None) => continue,
                Ok(Some(gap)) if gap.needs_refill() => {
                    repair.push((format!("{path} ({gap})"), gap.repairable()));
                    continue;
                }
                Ok(Some(gap)) => gap.to_string(),
                Err(error) => error,
            };
            self.report.problems.push(format!(
                "after the adoption {path} is not complete ({gap}) (ADR-062 D10)"
            ));
        }
        self.report.problems.extend(refill_problems(&repair));
    }
}

/// The problems naming the files that left hot and lack a copy on a
/// configured target (`Gap::needs_refill`), each with whether a configured
/// target holds a recorded copy. Two causes, two remedies.
fn refill_problems(files: &[(String, bool)]) -> Vec<String> {
    let names = |repairable: bool| {
        files
            .iter()
            .filter(|(_, r)| *r == repairable)
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let (repairable, readopt) = (names(true), names(false));
    let mut problems = Vec::new();
    if !repairable.is_empty() {
        problems.push(format!(
            "the copy ledger records {}, which left hot with a recorded copy on a configured copy \
             target but none on the target named: a target was added or replaced after it was \
             adopted, or an adoption stopped before it reached every target, and the replication \
             pass copies from hot only. Fill it with `mie store repair`, which copies it from \
             the target that holds it, or re-adopt the original store it came from; this run \
             adopted what it read, and only these files are short (ADR-062 D10, D11)",
            repairable
        ));
    }
    if !readopt.is_empty() {
        problems.push(format!(
            "the copy ledger records {}, which left hot, and no configured copy target holds a \
             recorded copy: its copies are recorded only on roots that are no longer configured \
             (every copy target replaced or lost). `mie store repair` cannot help, since no \
             configured target has a good copy; the only remedy is to re-adopt each original \
             store in turn, which copies its files to every configured target. This run adopted \
             what it read, and only these files are short (ADR-062 D10, D11)",
            readopt
        ));
    }
    problems
}

/// The partitions recorded removed (the later of `removed` and `returned`
/// decides) and the paths recorded lost, held, quarantined or replaced.
pub(super) fn ledger_states(
    records: &[Record],
) -> (BTreeSet<String>, BTreeMap<String, &'static str>) {
    let mut removed = BTreeSet::new();
    let mut marked: BTreeMap<String, &'static str> = BTreeMap::new();
    for record in records {
        match &record.kind {
            Kind::Removed { partition } => {
                removed.insert(partition.clone());
            }
            Kind::Returned { partition } => {
                removed.remove(partition);
            }
            Kind::Lost { path, .. } => {
                marked.insert(path.clone(), "lost");
            }
            Kind::Hold { path, .. } => {
                marked.entry(path.clone()).or_insert("held");
            }
            Kind::Quarantined { path, .. } => {
                marked.insert(path.clone(), "quarantined");
            }
            Kind::Replaced { path, .. } => {
                marked.insert(path.clone(), "replaced");
            }
            _ => {}
        }
    }
    (removed, marked)
}

/// What a recorded file lacks of [`complete`].
pub(super) struct Gap {
    /// Its partition is not recorded removed and hot lacks it.
    pub(super) off_hot: bool,
    /// Hot holds it.
    pub(super) on_hot: bool,
    /// The copy targets with a `copy` record of it.
    pub(super) recorded: Vec<String>,
    /// The copy targets without one.
    pub(super) missing: Vec<String>,
    /// Root ids with a `copy` record of it that are not configured copy
    /// targets any more (replaced or lost targets).
    pub(super) elsewhere: Vec<String>,
}

impl Gap {
    /// Only copies on some targets are missing, hot does not hold the file
    /// (its partition is recorded removed), and the ledger records at least
    /// one copy of it on any root id: it left hot after a complete or
    /// partial adoption, and a target was added or replaced since, or the
    /// adoption stopped before it reached every target. The pass copies
    /// from hot only, so only the target-to-target repair (`mie store repair`, when a
    /// configured target holds a recorded copy) or a re-adoption of its
    /// original store fills it. This class never blocks the writes.
    pub(super) fn needs_refill(&self) -> bool {
        !self.off_hot
            && !self.on_hot
            && !self.missing.is_empty()
            && !(self.recorded.is_empty() && self.elsewhere.is_empty())
    }

    /// Whether a configured copy target holds a recorded copy, so the
    /// repair can copy from it.
    pub(super) fn repairable(&self) -> bool {
        !self.recorded.is_empty()
    }

    /// Only copies are missing and hot holds the file: the pass fills them.
    fn needs_pass(&self) -> bool {
        !self.off_hot && self.on_hot && !self.missing.is_empty()
    }
}

impl std::fmt::Display for Gap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if self.off_hot {
            parts.push("not on hot, and its partition is not recorded removed".to_owned());
        }
        if !self.missing.is_empty() {
            let mut on = if self.recorded.is_empty() {
                String::new()
            } else {
                format!(", only on {}", self.recorded.join(", "))
            };
            if !self.elsewhere.is_empty() {
                on.push_str(&format!(
                    ", recorded on roots no longer configured: {}",
                    self.elsewhere.join(", ")
                ));
            }
            parts.push(format!("no copy record on {}{on}", self.missing.join(", ")));
        }
        write!(f, "{}", parts.join("; "))
    }
}

/// The one completeness predicate of a recorded file (ADR-062 D10): the
/// end state a passed adoption guarantees. It is complete when it is on
/// hot unless its partition is recorded removed, and when the ledger
/// records a copy of it on every copy target. `Ok(None)` when complete,
/// otherwise what is missing.
///
/// Why this predicate: on hot, hot holds every recorded part of its
/// partition, so a hot-window partition is whole there; on every target,
/// it has its k copies. A file on a target without a `copy` record does not count:
/// nothing would ever record it.
pub(super) fn complete(
    hot: &Path,
    fold: &Fold,
    path: &str,
    targets: &[Target],
    removed: &BTreeSet<String>,
) -> Result<Option<Gap>, String> {
    let on_hot = match replica::sealed_manifest(hot, path) {
        Ok(found) => found.is_some(),
        Err(error) => return Err(format!("hot: {error}")),
    };
    let (recorded, missing): (Vec<&Target>, Vec<&Target>) = targets
        .iter()
        .partition(|target| fold.has_copy(path, &target.id));
    let elsewhere = fold
        .copies
        .get(path)
        .into_iter()
        .flatten()
        .filter(|id| !targets.iter().any(|target| target.id == **id))
        .cloned()
        .collect();
    let gap = Gap {
        off_hot: !on_hot && !removed.contains(partition_of(path)),
        on_hot,
        recorded: recorded.iter().map(|t| t.id.clone()).collect(),
        missing: missing.iter().map(|t| t.id.clone()).collect(),
        elsewhere,
    };
    Ok((gap.off_hot || !gap.missing.is_empty()).then_some(gap))
}
