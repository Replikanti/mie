//! The replication pass (ADR-062 D5, D9, D11): `mie store replicate`.
//!
//! One pass copies everything hot holds that a copy target lacks, in this
//! order, and never removes anything:
//!
//! 0. **Refusals and reads.** An empty `cold_roots`, a hot without its
//!    marker, an archive configuration that does not describe this store,
//!    and a pass lock held by another pass are refused. A missing or
//!    unreadable copy ledger on hot means the pass does nothing and says
//!    so (D7 degraded start). Each copy target's marker is read: a target
//!    without one, or with an unreadable one, is skipped for the pass and
//!    named, and nothing is created in it. A marker with hot's id, an id two
//!    targets share, and two roots on one device are refused, naming both
//!    roots. Then L, the journal's length, is read. Nothing is written
//!    before this point but the lock file.
//! 1. **Sealed files.** Every sealed part on hot without a copy record for
//!    a target is copied to it by the D5 protocol, and the `manifest`
//!    record (with the first copy) and the `copy` record are appended. Hot's
//!    manifest is checked against the ledger's `manifest` record before
//!    anything is copied (D5). A live part is always eligible; an archive
//!    part only once an import entry on hot lists it with its manifest's
//!    SHA-256 (D11 *Archive parts wait for their import entry*).
//! 2. **Import-ledger entries.** An entry goes to a target only once every
//!    part it lists has a copy record for that target, so per archive file
//!    its parts precede its entry. `PENDING` is never copied.
//! 3. **The copy-ledger tail**, to hot's verified prefix, then one
//!    `ledger-copy` record per target whose copy was not up to date. After
//!    steps 1 and 2, so this pass's copy records lie inside the prefix.
//! 4. **The journal tail** `[len(copy), L)`. L was read before step 1
//!    listed hot, and when a live file (from hot's listing, or from the
//!    ledger's `manifest` and `copy` records) has no copy record for a
//!    target, that target's copy stops before the first line that names it,
//!    so a `sealed` line on a copy always names a file that is already on
//!    that copy. The ledger's paths count because the hot removal (#191,
//!    ADR-062 D6) takes files off hot: a target added after a removal never
//!    gets them from hot, and its journal copy waits at the first removed
//!    file's line until #192's repair fills it.
//!
//! **The target's marker is re-read before every write to it** (a sealed
//! copy, an entry, the ledger and journal tails) and before every record
//! about it: a target whose marker is gone or changed stops for the rest of
//! the pass (D11; review of #199, finding 4). The residual window is the
//! time between a re-read and the write that follows it.
//!
//! A failure to read hot (a part, its manifest, an entry, the journal) is
//! about that one source and is named, and the pass goes on with the other
//! files and targets. An `Integrity`, `Corrupt` or `Invalid` error on a
//! target's copy of one file or entry is named and the pass goes on; any
//! other error on a target stops that target for the rest of the pass; a
//! failed ledger append stops the pass. Every append hands a
//! truncation of the ledger's trailing record to the [`PassObserver`]
//! before anything else, a failed append included (#197 review finding 5):
//! `mie store replicate` prints it as a report line of its own, and `mie
//! ingest` journals it (`copy_ledger_truncated`). The standalone command never writes the
//! capture journal: a second process appending to it while `mie ingest`
//! writes it through a buffered writer could land inside a half-flushed
//! line (#189 STOP 1).
//!
//! The pass passes only when every configured copy target was written,
//! every listed file has copy records for k distinct root ids (k = the
//! number of configured copy targets), every entry is on every target, and
//! every target's ledger copy and journal copy is current.
//!
//! It is also D9's copy-only mode: a pre-cutover store initialized with its
//! own `cold_roots` gets whole-store copies, and nothing moves.
//!
//! [`run_planned`] is the pass of `mie ingest` (D11, after every seal
//! pass): when every configured copy target presented its marker and none
//! stopped, it ends with a fresh ledger read, one walk of hot and the hot
//! removal's plan ([`removal::plan`]), which the seal path carries out.
//! `mie store replicate` and the adoption never plan and never remove.

use super::{DeviceProbe, check_devices, check_hot};
use crate::config::{ArchiveConfig, IngestConfig};
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_binance::archive::ledger::ImportLedger;
use mie_adapter_parquet::copy_ledger::{
    self, AppendError, AppendOutcome, CopyLedger, Kind, Record, Truncation,
};
use mie_adapter_parquet::removal::{self, RemovalPlan};
use mie_adapter_parquet::replica::{self, CopyError, CopyOutcome, Side, TailSource};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_ports::raw::{RawStoreError, validate_segment};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// Where a copy target keeps its copies of hot's metadata (ADR-062 D7
/// *Formats and layout*).
const META_DIR: &str = ".mie";
const JOURNAL_DIR: &str = "journal";
const IMPORT_LEDGER_DIR: &str = "import-ledger";

/// The appender the pass writes the copy ledger through: [`CopyLedger`] in
/// production, a failing one in tests.
pub trait LedgerAppend {
    /// The ledger's path.
    fn path(&self) -> &Path;

    /// Appends `record`.
    ///
    /// # Errors
    ///
    /// As [`CopyLedger::append`].
    fn append(&self, record: &Record) -> Result<AppendOutcome, AppendError>;
}

impl LedgerAppend for CopyLedger {
    fn path(&self) -> &Path {
        CopyLedger::path(self)
    }

    fn append(&self, record: &Record) -> Result<AppendOutcome, AppendError> {
        CopyLedger::append(self, record)
    }
}

/// What the pass reports as it happens.
pub trait PassObserver {
    /// An append truncated the incomplete trailing record of `ledger`. Called
    /// before the append's result is acted on, a failed append included.
    fn ledger_truncated(&mut self, ledger: &Path, truncation: Truncation);
}

/// The report line of a truncation.
pub fn truncation_line(ledger: &Path, truncation: Truncation) -> String {
    format!(
        "copy ledger {}: truncated an incomplete trailing record of {} bytes at byte {} before \
         an append (ADR-062 D7)",
        ledger.display(),
        truncation.bytes,
        truncation.offset
    )
}

/// The steps a test can stop the pass after, as a crash would. Each name
/// says after what the pass stops.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PassStep {
    /// A sealed part was published on a target, before its records.
    AfterPublish,
    /// The `manifest` record was appended, before the `copy` record.
    AfterManifestRecord,
    /// An import entry was copied to a target.
    AfterEntryCopy,
    /// A ledger copy was extended, before its `ledger-copy` record.
    AfterLedgerExtend,
}

#[cfg(test)]
impl PassStep {
    pub(crate) const ALL: [Self; 4] = [
        Self::AfterPublish,
        Self::AfterManifestRecord,
        Self::AfterEntryCopy,
        Self::AfterLedgerExtend,
    ];
}

/// A test hook that sees every pass step.
#[cfg(test)]
type StepHook = Box<dyn FnMut(PassStep)>;

#[cfg(test)]
thread_local! {
    /// Test hook: the pass stops at the first occurrence of this step.
    pub(crate) static CRASH_AT: std::cell::Cell<Option<PassStep>> =
        const { std::cell::Cell::new(None) };
    /// Test hook: runs once between step 1's listing and the rest of the
    /// pass (the seal race of D11).
    pub(crate) static AFTER_LISTING: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
    /// Test hook: runs at every pass step, before a crash there.
    pub(crate) static ON_STEP: std::cell::RefCell<Option<StepHook>> =
        std::cell::RefCell::new(None);
}

/// What one pass did: its report lines, and every problem that makes it
/// fail.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PassReport {
    /// What was copied and appended, in order.
    pub lines: Vec<String>,
    /// Every refusal, skipped or stopped target, file or entry below k and
    /// error; empty when the pass passed.
    pub problems: Vec<String>,
    /// Sealed files copied (copies made, over every target).
    pub files_copied: usize,
    /// Import entries copied.
    pub entries_copied: usize,
    /// Bytes appended to the copy-ledger copies.
    pub ledger_bytes: u64,
    /// Bytes appended to the journal copies.
    pub journal_bytes: u64,
    /// The ids of the copy targets that presented their marker.
    pub target_ids: BTreeSet<String>,
    /// Every configured copy target presented its marker and none was
    /// stopped, and the pass ran to its end.
    pub targets_complete: bool,
}

/// Why the pass stopped early.
enum Stop {
    /// A refusal or a failed ledger append, already in the report.
    Failed,
    /// A simulated crash (tests).
    #[cfg(test)]
    Crash,
}

/// A configured copy target that presented its marker.
struct Target {
    path: PathBuf,
    id: String,
    /// Whether it stopped for the rest of the pass.
    stopped: bool,
}

/// The copy ledger as the pass knows it: the first manifest text per data
/// path, the paths with conflicting texts, and the root ids of each path's
/// copy records.
#[derive(Default)]
pub(super) struct Fold {
    pub(super) manifests: BTreeMap<String, String>,
    pub(super) conflicts: BTreeSet<String>,
    pub(super) copies: BTreeMap<String, BTreeSet<String>>,
    /// The paths of `lost` records: no copy of them can reach a target.
    pub(super) lost: BTreeSet<String>,
}

impl Fold {
    pub(super) fn new(records: &[Record]) -> Result<Self, RawStoreError> {
        let mut fold = Self::default();
        for record in records {
            match &record.kind {
                Kind::Manifest { text } => {
                    let path = replica::parse_manifest(text)?.relative_path;
                    match fold.manifests.get(&path) {
                        Some(known) if known != text => {
                            fold.conflicts.insert(path);
                        }
                        Some(_) => {}
                        None => {
                            fold.manifests.insert(path, text.clone());
                        }
                    }
                }
                Kind::Copy { path, root } => {
                    fold.copies
                        .entry(path.clone())
                        .or_default()
                        .insert(root.clone());
                }
                Kind::Lost { path, .. } => {
                    fold.lost.insert(path.clone());
                }
                _ => {}
            }
        }
        Ok(fold)
    }

    pub(super) fn has_copy(&self, path: &str, id: &str) -> bool {
        self.copies.get(path).is_some_and(|ids| ids.contains(id))
    }

    fn copy_count(&self, path: &str) -> usize {
        self.copies.get(path).map_or(0, BTreeSet::len)
    }
}

/// One import-ledger entry on hot (or, for the adoption, in the import
/// ledger of the store it adopts).
pub(super) struct Entry {
    /// `<segment>/<name>.import`.
    pub(super) name: String,
    pub(super) segment: String,
    pub(super) file_name: String,
    pub(super) path: PathBuf,
    /// The data paths it lists, with their SHA-256.
    pub(super) files: Vec<(String, String)>,
}

/// The pass in progress.
struct Pass<'a> {
    hot: &'a Path,
    /// k: the number of configured copy targets.
    k: usize,
    archive: Option<ArchiveConfig>,
    journal: &'a Path,
    /// L, read before step 1; `None` without a journal.
    journal_len: Option<u64>,
    _lock: replica::ReplicationLock,
    ledger: &'a dyn LedgerAppend,
    observer: &'a mut dyn PassObserver,
    now_ms: &'a dyn Fn() -> i64,
    targets: Vec<Target>,
    fold: Fold,
    report: PassReport,
    /// Per file or entry, why it is short of a copy.
    reasons: BTreeMap<String, Vec<String>>,
}

impl Pass<'_> {
    fn line(&mut self, line: String) {
        self.report.lines.push(line);
    }

    fn problem(&mut self, problem: String) {
        self.report.problems.push(problem);
    }

    fn reason(&mut self, subject: &str, reason: String) {
        self.reasons
            .entry(subject.to_owned())
            .or_default()
            .push(reason);
    }

    /// Stops target `i` for the rest of the pass.
    fn stop_target(&mut self, i: usize, why: String) {
        let target = &mut self.targets[i];
        target.stopped = true;
        let message = format!(
            "copy target {} ({}): stopped for the rest of this pass: {why}",
            target.path.display(),
            target.id
        );
        self.problem(message);
    }

    /// Whether target `i` still presents its marker; stops it otherwise.
    /// Why before each record: a mount that disappears mid-pass leaves an
    /// empty directory on its parent's filesystem, and records written for
    /// it would name copies that the returning mount hides.
    fn marker_present(&mut self, i: usize) -> bool {
        let target = &self.targets[i];
        match root_marker::read(&target.path) {
            Ok(Some(id)) if id == target.id => true,
            Ok(Some(other)) => {
                self.stop_target(i, format!("its marker now carries the id {other:?}"));
                false
            }
            Ok(None) => {
                self.stop_target(
                    i,
                    "its marker is gone (an unmounted mount point?); nothing more is recorded \
                     for it (ADR-062 D11)"
                        .to_owned(),
                );
                false
            }
            Err(error) => {
                self.stop_target(i, format!("its marker cannot be read: {error}"));
                false
            }
        }
    }

    /// Appends one record. A truncation goes to the observer first, also
    /// when the append fails; a failed append stops the pass.
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
            self.observer
                .ledger_truncated(self.ledger.path(), truncation);
        }
        result.map_err(|error| {
            let message = format!(
                "the {} append to the copy ledger {} failed, so the pass stops: {error}",
                record.kind.name(),
                self.ledger.path().display()
            );
            self.problem(message);
            Stop::Failed
        })
    }
}

/// Marks a step of the pass; under test, runs the step hook and stops the
/// pass there once.
fn reached(step: PassStep) -> Result<(), Stop> {
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

/// Whether an error concerns one file or entry only (`Integrity`,
/// `Corrupt`, `Invalid`): named, and the pass goes on with the target.
fn is_local(error: &RawStoreError) -> bool {
    matches!(
        error,
        RawStoreError::Integrity(_) | RawStoreError::Corrupt(_) | RawStoreError::Invalid(_)
    )
}

/// The archive configuration of the pass: `--archive-config`, otherwise
/// `[history] archive_config`, otherwise none.
pub fn archive_config(
    config: &IngestConfig,
    given: Option<&Path>,
) -> Result<Option<ArchiveConfig>, String> {
    match given {
        Some(path) => config
            .archive_config_at(path)
            .map(Some)
            .map_err(|e| e.to_string()),
        None => config
            .history_archive()
            .map(|archive| archive.map(|(_, archive)| archive))
            .map_err(|e| e.to_string()),
    }
}

/// Runs one pass (module docs). Every outcome is in the report.
pub fn run(
    config: &IngestConfig,
    archive: Option<&Path>,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    ledger: &dyn LedgerAppend,
    observer: &mut dyn PassObserver,
) -> PassReport {
    let archive = archive_config(config, archive);
    run_with(config, archive, probe, now_ms, ledger, observer, None)
}

/// The pass of `mie ingest` (module docs): [`run`] with the archive
/// configuration already resolved, then, when every configured copy target
/// presented its marker and none stopped, the hot removal's plan from a
/// fresh ledger read and one walk of hot, with the hot window starting on
/// `first_hot_date`. A failed read or walk is a problem and gives no plan.
pub fn run_planned(
    config: &IngestConfig,
    archive: Option<ArchiveConfig>,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    ledger: &dyn LedgerAppend,
    observer: &mut dyn PassObserver,
    first_hot_date: &str,
) -> (PassReport, Option<RemovalPlan>) {
    let mut report = run_with(config, Ok(archive), probe, now_ms, ledger, observer, None);
    if !report.targets_complete {
        return (report, None);
    }
    let hot = config.paths.raw_root.as_path();
    let k = config
        .paths
        .cold_roots
        .iter()
        .filter(|root| root.copy_target)
        .count();
    let planned = match copy_ledger::read(hot) {
        Ok(Some(read)) => removal::scan_hot(hot)
            .and_then(|scan| removal::plan(&read, &scan, &report.target_ids, k, first_hot_date))
            .map_err(|error| error.to_string()),
        Ok(None) => Err(format!(
            "copy ledger {} does not exist",
            copy_ledger::ledger_path(hot).display()
        )),
        Err(error) => Err(error.to_string()),
    };
    match planned {
        Ok(plan) => (report, Some(plan)),
        Err(error) => {
            report.problems.push(format!(
                "the hot removal's plan failed, so nothing is removed after this pass: {error}"
            ));
            (report, None)
        }
    }
}

/// [`run`] under a pass lock the caller already holds: `mie store adopt`
/// ends with one pass and holds the lock for its whole run (ADR-062 D10).
pub(crate) fn run_locked(
    config: &IngestConfig,
    archive: Option<&Path>,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    ledger: &dyn LedgerAppend,
    observer: &mut dyn PassObserver,
    lock: replica::ReplicationLock,
) -> PassReport {
    let archive = archive_config(config, archive);
    run_with(config, archive, probe, now_ms, ledger, observer, Some(lock))
}

/// The pass, taking the lock at its place in step 0 unless one is given.
fn run_with(
    config: &IngestConfig,
    archive: Result<Option<ArchiveConfig>, String>,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    ledger: &dyn LedgerAppend,
    observer: &mut dyn PassObserver,
    lock: Option<replica::ReplicationLock>,
) -> PassReport {
    let mut report = PassReport::default();
    let Some(mut pass) = start(
        config,
        archive,
        probe,
        now_ms,
        ledger,
        observer,
        lock,
        &mut report,
    ) else {
        return report;
    };
    // A stop is already in the report; a crash leaves it as it was.
    let _ = pass.copy_everything();
    pass.report
}

/// Step 0. `None` with the refusal in `report`. A held `lock` is used
/// as is; without one the pass lock is taken after the configuration
/// refusals, as before.
#[allow(clippy::too_many_arguments)]
fn start<'a>(
    config: &'a IngestConfig,
    archive: Result<Option<ArchiveConfig>, String>,
    probe: &dyn DeviceProbe,
    now_ms: &'a dyn Fn() -> i64,
    ledger: &'a dyn LedgerAppend,
    observer: &'a mut dyn PassObserver,
    lock: Option<replica::ReplicationLock>,
    report: &mut PassReport,
) -> Option<Pass<'a>> {
    let mut refuse = |problem: String| {
        report.problems.push(problem);
        None
    };
    let hot = config.paths.raw_root.as_path();
    let cold_roots = &config.paths.cold_roots;
    if cold_roots.is_empty() {
        return refuse(
            "paths.cold_roots is empty: a single-root store has no copy targets (ADR-062 D11)"
                .to_owned(),
        );
    }
    if let Err(error) = check_hot(hot, cold_roots) {
        return refuse(error);
    }
    let archive = match archive {
        Ok(archive) => archive,
        Err(error) => return refuse(error),
    };
    let lock = match lock.map_or_else(|| replica::try_replication_lock(hot), Ok) {
        Ok(lock) => lock,
        Err(error) => return refuse(error.to_string()),
    };
    let ledger_path = copy_ledger::ledger_path(hot);
    let degraded = "the pass does nothing (the degraded start, ADR-062 D7); the copy-ledger \
                    rebuild restores the ledger";
    let records = match copy_ledger::read(hot) {
        Ok(Some(ledger)) => ledger.records,
        Ok(None) => {
            return refuse(format!(
                "copy ledger {} does not exist: {degraded}",
                ledger_path.display()
            ));
        }
        Err(error) => {
            return refuse(format!(
                "copy ledger {} cannot be read ({error}): {degraded}",
                ledger_path.display()
            ));
        }
    };
    let fold = match Fold::new(&records) {
        Ok(fold) => fold,
        Err(error) => {
            return refuse(format!(
                "copy ledger {}: {error}: {degraded}",
                ledger_path.display()
            ));
        }
    };

    let mut targets: Vec<Target> = Vec::new();
    let mut skipped = Vec::new();
    for root in cold_roots.iter().filter(|root| root.copy_target) {
        match root_marker::read(&root.path) {
            Ok(Some(id)) if id == HOT_ID => {
                return refuse(format!(
                    "copy target {} carries hot's marker (id {HOT_ID:?}), the id of raw_root {}: \
                     the same root under two names, or a copied marker (ADR-062 D7)",
                    root.path.display(),
                    hot.display()
                ));
            }
            Ok(Some(id)) => {
                if let Some(other) = targets.iter().find(|target| target.id == id) {
                    return refuse(format!(
                        "copy targets {} and {} both carry the id {id:?}: two spellings of one \
                         root, or a copied marker (ADR-062 D7)",
                        other.path.display(),
                        root.path.display()
                    ));
                }
                targets.push(Target {
                    path: root.path.clone(),
                    id,
                    stopped: false,
                });
            }
            Ok(None) => skipped.push(format!(
                "copy target {} has no root marker: skipped for this pass and nothing is written \
                 into it (an unmounted mount point is an empty directory; ADR-062 D11)",
                root.path.display()
            )),
            Err(error) => skipped.push(format!(
                "copy target {}: its root marker cannot be read ({error}): skipped for this pass \
                 and nothing is written into it (ADR-062 D11)",
                root.path.display()
            )),
        }
    }
    let marked: Vec<&Path> = targets.iter().map(|target| target.path.as_path()).collect();
    if let Err(error) = check_devices(hot, &marked, probe) {
        return refuse(error);
    }
    let journal = config.paths.journal.as_path();
    let journal_len = match fs::metadata(journal) {
        Ok(metadata) => Some(metadata.len()),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => {
            return refuse(format!(
                "journal {}: cannot read its length: {error}",
                journal.display()
            ));
        }
    };
    report.problems.extend(skipped);
    Some(Pass {
        hot,
        k: cold_roots.iter().filter(|root| root.copy_target).count(),
        archive,
        journal,
        journal_len,
        _lock: lock,
        ledger,
        observer,
        now_ms,
        targets,
        fold,
        report: std::mem::take(report),
        reasons: BTreeMap::new(),
    })
}

/// Whether a data path belongs to the archive source.
pub(super) fn is_archive_part(path: &str) -> bool {
    path.strip_prefix("source=")
        .and_then(|rest| rest.split_once('/'))
        .is_some_and(|(source, _)| source == ARCHIVE_SOURCE)
}

/// The import-ledger entries on hot, `<segment>/<name>.import`, sorted;
/// `PENDING`, hidden temp files and anything else are never listed. An
/// entry that cannot be read or parsed is returned as a problem.
pub(super) fn hot_entries(import_ledger: &Path) -> (Vec<Entry>, Vec<String>) {
    let (mut entries, mut problems) = (Vec::new(), Vec::new());
    let segments = match sorted_names(import_ledger) {
        Ok(names) => names,
        Err(error) => {
            problems.push(format!(
                "import ledger {}: cannot list it ({error}); no entry and no archive part is \
                 replicated in this pass",
                import_ledger.display()
            ));
            return (entries, problems);
        }
    };
    for segment in segments {
        let dir = import_ledger.join(&segment);
        if validate_segment(&segment).is_err() || !dir.is_dir() {
            continue;
        }
        let names = match sorted_names(&dir) {
            Ok(names) => names,
            Err(error) => {
                problems.push(format!("import ledger {}: {error}", dir.display()));
                continue;
            }
        };
        for file_name in names {
            if file_name.starts_with('.') || !file_name.ends_with(".import") {
                continue;
            }
            let name = format!("{segment}/{file_name}");
            let path = dir.join(&file_name);
            let parsed = fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|text| ImportLedger::parse(&text).map_err(|e| e.to_string()));
            match parsed {
                Ok(ledger) => entries.push(Entry {
                    name,
                    segment: segment.clone(),
                    file_name,
                    path,
                    files: ledger
                        .files
                        .into_iter()
                        .map(|file| (file.relative_path, file.sha256))
                        .collect(),
                }),
                Err(error) => problems.push(format!(
                    "import entry {name} ({}): {error}; neither it nor the parts it lists are \
                     replicated",
                    path.display()
                )),
            }
        }
    }
    (entries, problems)
}

/// The byte offset and the path of the first line of `journal` in
/// `[from, end)` (from the start of the line that holds byte `from`) that
/// names one of `paths`, or `None`. A line cut at `end` counts with the
/// bytes before `end`. A data path's text occurs in a journal line only
/// where the line names that file (`sealed`, `recovery` `rolled_forward`):
/// a temp file's name differs from it in the byte before `part-`.
///
/// The match is by token ([`line_names`]), not a substring scan per path:
/// with the ledger's paths, a new target's set holds every recorded live
/// file (about 670 000 after a year), and a scan per path would take
/// minutes per pass.
pub(super) fn first_line_naming(
    journal: &Path,
    from: u64,
    end: u64,
    paths: &BTreeSet<&str>,
) -> std::io::Result<Option<(u64, String)>> {
    use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
    let mut file = fs::File::open(journal)?;
    // The start of the line that holds byte `from`.
    let mut start = from.min(end);
    let mut cursor = start;
    'back: while cursor > 0 {
        let chunk_start = cursor.saturating_sub(65_536);
        let mut chunk = vec![0; (cursor - chunk_start) as usize];
        file.seek(SeekFrom::Start(chunk_start))?;
        file.read_exact(&mut chunk)?;
        for (i, &byte) in chunk.iter().enumerate().rev() {
            if byte == b'\n' {
                start = chunk_start + i as u64 + 1;
                break 'back;
            }
        }
        cursor = chunk_start;
        start = 0;
    }
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::new(file);
    let mut offset = start;
    let mut line = Vec::new();
    while offset < end {
        line.clear();
        let n = reader.read_until(b'\n', &mut line)? as u64;
        if n == 0 {
            break;
        }
        let seen = &line[..n.min(end - offset) as usize];
        if let Some(path) = line_names(seen, paths) {
            return Ok(Some((offset, path.to_owned())));
        }
        offset += n;
    }
    Ok(None)
}

/// The first path of `paths` that `line` names, by token: for each
/// occurrence of `source=`, the longest run of layout-path bytes
/// (`[A-Za-z0-9._=/-]`) from there, and every prefix of that run that ends
/// with `.parquet`, looked up in `paths`. It finds the lines today's
/// substring rule found for every path the layout can write: a data path
/// consists of those bytes and starts with `source=`, so each occurrence is
/// a prefix of such a run. The cost per line is O(line length · log n)
/// instead of O(paths × line length).
pub(super) fn line_names<'a>(line: &[u8], paths: &BTreeSet<&'a str>) -> Option<&'a str> {
    const START: &[u8] = b"source=";
    const SUFFIX: &[u8] = b".parquet";
    let layout_byte =
        |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'=' | b'/' | b'-');
    let mut from = 0;
    while let Some(at) = line[from..]
        .windows(START.len())
        .position(|window| window == START)
    {
        let start = from + at;
        let run_len = line[start..]
            .iter()
            .position(|&b| !layout_byte(b))
            .unwrap_or(line.len() - start);
        let run = &line[start..start + run_len];
        for end in SUFFIX.len()..=run.len() {
            if run[..end].ends_with(SUFFIX)
                && let Ok(candidate) = std::str::from_utf8(&run[..end])
                && let Some(path) = paths.get(candidate)
            {
                return Some(path);
            }
        }
        from = start + 1;
    }
    None
}

/// The UTF-8 entry names of `dir`, sorted; a missing directory has none.
pub(super) fn sorted_names(dir: &Path) -> std::io::Result<Vec<String>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut names = Vec::new();
    for entry in entries {
        if let Ok(name) = entry?.file_name().into_string() {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

impl Pass<'_> {
    /// Steps 1 to 4, then the result (module docs).
    fn copy_everything(&mut self) -> Result<(), Stop> {
        let paths = match replica::list_sealed(self.hot) {
            Ok(paths) => paths,
            Err(error) => {
                self.problem(format!("cannot list hot's sealed files: {error}"));
                return Err(Stop::Failed);
            }
        };
        #[cfg(test)]
        if let Some(hook) = AFTER_LISTING.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
        let (entries, problems) = match &self.archive {
            Some(archive) => hot_entries(&archive.paths.import_ledger),
            None => (Vec::new(), Vec::new()),
        };
        self.report.problems.extend(problems);
        let mut copied = 0usize;
        for path in &paths {
            copied += self.copy_file(path, &entries)?;
        }
        let mut entries_copied = 0usize;
        for entry in &entries {
            entries_copied += self.copy_entry(entry)?;
        }
        let ledger_bytes = self.copy_ledger_tail()?;
        let journal_bytes = self.copy_journal_tail(&paths);
        self.line(format!(
            "copied {copied} file(s) and {entries_copied} import entr{}; appended {ledger_bytes} \
             copy-ledger byte(s) and {journal_bytes} journal byte(s)",
            if entries_copied == 1 { "y" } else { "ies" }
        ));
        self.report.files_copied = copied;
        self.report.entries_copied = entries_copied;
        self.report.ledger_bytes = ledger_bytes;
        self.report.journal_bytes = journal_bytes;
        self.report.target_ids = self.targets.iter().map(|t| t.id.clone()).collect();
        self.report.targets_complete =
            self.targets.len() == self.k && self.targets.iter().all(|t| !t.stopped);
        self.result(&paths, &entries);
        Ok(())
    }

    /// Step 1 for one sealed file: returns how many copies it made.
    fn copy_file(&mut self, path: &str, entries: &[Entry]) -> Result<usize, Stop> {
        let missing: Vec<usize> = (0..self.targets.len())
            .filter(|&i| !self.fold.has_copy(path, &self.targets[i].id))
            .collect();
        if missing.is_empty() {
            return Ok(0);
        }
        if is_archive_part(path) && !self.archive_part_eligible(path, entries) {
            return Ok(0);
        }
        // The D5 guard runs before anything is published: a hot manifest
        // that differs from the ledger's `manifest` record would otherwise
        // put two versions of one path on two targets (review of #199,
        // finding 1).
        let hot_text = match replica::sealed_manifest(self.hot, path) {
            Ok(Some((text, _))) => text,
            // Gone since the listing: only the hot removal removes, and a
            // removed partition has its copies.
            Ok(None) => return Ok(0),
            Err(error) => {
                self.reason(path, format!("reading its manifest on hot failed: {error}"));
                return Ok(0);
            }
        };
        if !self.matches_ledger(path, &hot_text) {
            return Ok(0);
        }
        let mut copied = 0;
        for i in missing {
            if self.targets[i].stopped || !self.marker_present(i) {
                continue;
            }
            let (outcome, text) = match replica::copy_sealed(self.hot, &self.targets[i].path, path)
            {
                Ok(done) => done,
                // A failure to read hot is about this file, not about the
                // target: the other files and targets go on (review of #199,
                // finding 2).
                Err(CopyError {
                    side: Side::Source,
                    error,
                }) => {
                    self.reason(path, format!("reading it on hot failed: {error}"));
                    return Ok(copied);
                }
                Err(CopyError { error, .. }) if is_local(&error) => {
                    let id = self.targets[i].id.clone();
                    self.reason(path, format!("the copy to {id} failed: {error}"));
                    continue;
                }
                Err(CopyError { error, .. }) => {
                    self.stop_target(i, format!("copying {path}: {error}"));
                    continue;
                }
            };
            reached(PassStep::AfterPublish)?;
            if text != hot_text {
                self.reason(
                    path,
                    "its manifest on hot changed during the pass; nothing is recorded (ADR-062 D5)"
                        .to_owned(),
                );
                continue;
            }
            if !self.marker_present(i) {
                continue;
            }
            if !self.fold.manifests.contains_key(path) {
                self.append(Kind::Manifest { text: text.clone() })?;
                self.fold.manifests.insert(path.to_owned(), text);
                reached(PassStep::AfterManifestRecord)?;
                if !self.marker_present(i) {
                    continue;
                }
            }
            let id = self.targets[i].id.clone();
            self.append(Kind::Copy {
                path: path.to_owned(),
                root: id.clone(),
            })?;
            self.fold
                .copies
                .entry(path.to_owned())
                .or_default()
                .insert(id.clone());
            match outcome {
                CopyOutcome::Copied => {
                    copied += 1;
                    self.line(format!("copied {path} to {id}"));
                }
                CopyOutcome::AlreadyPresent => {
                    self.line(format!("recorded {path} on {id}: already present"));
                }
            }
        }
        Ok(copied)
    }

    /// The D5 guard: hot's manifest text must equal the ledger's `manifest`
    /// record for the path, if there is one, and the ledger must not hold
    /// two texts for it. Otherwise the reason is recorded.
    fn matches_ledger(&mut self, path: &str, hot_text: &str) -> bool {
        if self.fold.conflicts.contains(path) {
            self.reason(
                path,
                "the copy ledger holds two different manifest texts for it; nothing is copied \
                 (ADR-062 D5)"
                    .to_owned(),
            );
            return false;
        }
        match self.fold.manifests.get(path) {
            Some(known) if known != hot_text => {
                self.reason(
                    path,
                    "hot's manifest differs from the copy ledger's manifest record; nothing is \
                     copied (ADR-062 D5)"
                        .to_owned(),
                );
                false
            }
            _ => true,
        }
    }

    /// Whether an archive part may be copied: an import entry on hot lists
    /// it with its manifest's SHA-256 (D11). Otherwise the reason is
    /// recorded.
    fn archive_part_eligible(&mut self, path: &str, entries: &[Entry]) -> bool {
        if self.archive.is_none() {
            self.reason(
                path,
                "an archive part, and no archive configuration is given (--archive-config or \
                 [history] archive_config), so its import entry cannot be checked"
                    .to_owned(),
            );
            return false;
        }
        let listed: Vec<(&str, &str)> = entries
            .iter()
            .flat_map(|entry| {
                entry
                    .files
                    .iter()
                    .filter(|(file, _)| file == path)
                    .map(|(_, sha)| (entry.name.as_str(), sha.as_str()))
            })
            .collect();
        if listed.is_empty() {
            self.reason(
                path,
                "waiting for its import entry: an archive part is replicated only once the \
                 entry that lists it exists on hot (ADR-062 D11)"
                    .to_owned(),
            );
            return false;
        }
        let sha = match replica::sealed_manifest(self.hot, path) {
            Ok(Some((_, file))) => file.sha256,
            // Gone since the listing: only the hot removal removes, and a
            // removed partition has its copies.
            Ok(None) => return false,
            Err(error) => {
                self.reason(path, format!("its manifest on hot: {error}"));
                return false;
            }
        };
        if listed.iter().any(|(_, listed_sha)| *listed_sha == sha) {
            return true;
        }
        let names: Vec<&str> = listed.iter().map(|(name, _)| *name).collect();
        self.reason(
            path,
            format!(
                "integrity: import entry {} lists it with another SHA-256 than its manifest's; \
                 it is not copied (ADR-062 D11)",
                names.join(", ")
            ),
        );
        false
    }

    /// Step 2 for one entry: returns how many copies it made.
    fn copy_entry(&mut self, entry: &Entry) -> Result<usize, Stop> {
        let mut copied = 0;
        for i in 0..self.targets.len() {
            if self.targets[i].stopped {
                continue;
            }
            let dir = self.targets[i]
                .path
                .join(META_DIR)
                .join(IMPORT_LEDGER_DIR)
                .join(&entry.segment);
            // Presence comes from a listing: re-reading every entry on
            // every pass would read the whole import ledger from every
            // target; comparing the bytes is the verification's job.
            let present = match sorted_names(&dir) {
                Ok(names) => names.contains(&entry.file_name),
                Err(error) => {
                    self.stop_target(i, format!("listing {}: {error}", dir.display()));
                    continue;
                }
            };
            if present {
                continue;
            }
            let id = self.targets[i].id.clone();
            let waiting: Vec<&str> = entry
                .files
                .iter()
                .map(|(file, _)| file.as_str())
                .filter(|file| !self.fold.has_copy(file, &id))
                .collect();
            if !waiting.is_empty() {
                let reason = format!(
                    "waiting for {} of its part(s) on {id}, which precede it (ADR-062 D11)",
                    waiting.len()
                );
                self.reason(&entry.name, reason);
                continue;
            }
            if !self.marker_present(i) {
                continue;
            }
            match replica::copy_immutable(&entry.path, &dir.join(&entry.file_name)) {
                Ok(_) => {
                    copied += 1;
                    self.line(format!("copied import entry {} to {id}", entry.name));
                    reached(PassStep::AfterEntryCopy)?;
                }
                Err(CopyError {
                    side: Side::Source,
                    error,
                }) => {
                    self.reason(&entry.name, format!("reading it on hot failed: {error}"));
                    return Ok(copied);
                }
                Err(CopyError { error, .. }) if is_local(&error) => {
                    self.reason(&entry.name, format!("the copy to {id} failed: {error}"));
                }
                Err(CopyError { error, .. }) => {
                    self.stop_target(i, format!("copying import entry {}: {error}", entry.name));
                }
            }
        }
        Ok(copied)
    }

    /// Step 3: returns the bytes appended to the ledger copies.
    fn copy_ledger_tail(&mut self) -> Result<u64, Stop> {
        let mut appended = 0;
        for i in 0..self.targets.len() {
            if self.targets[i].stopped {
                continue;
            }
            if !self.marker_present(i) {
                continue;
            }
            let (path, id) = (self.targets[i].path.clone(), self.targets[i].id.clone());
            let copy = match copy_ledger::extend_copy(self.hot, &path, &id) {
                Ok(copy) => copy,
                Err(CopyError {
                    side: Side::Source,
                    error,
                }) => {
                    self.problem(format!(
                        "hot's copy ledger cannot be read for the ledger copies: {error}"
                    ));
                    return Ok(appended);
                }
                Err(CopyError { error, .. }) if is_local(&error) => {
                    self.problem(format!(
                        "the copy-ledger copy on {id} ({}) is refused: {error}; it needs the \
                         repair (ADR-062 D11)",
                        copy_ledger::ledger_path(&path).display()
                    ));
                    continue;
                }
                Err(CopyError { error, .. }) => {
                    self.stop_target(i, format!("extending its copy-ledger copy: {error}"));
                    continue;
                }
            };
            appended += copy.appended_bytes;
            if copy.current {
                continue;
            }
            reached(PassStep::AfterLedgerExtend)?;
            if !self.marker_present(i) {
                continue;
            }
            self.append(Kind::LedgerCopy {
                root: id.clone(),
                prefix_bytes: copy.prefix_bytes,
                prefix_sha256: copy.prefix_sha256,
            })?;
            self.line(format!(
                "copy-ledger copy on {id}: appended {} byte(s), now {} bytes",
                copy.appended_bytes, copy.prefix_bytes
            ));
        }
        Ok(appended)
    }

    /// Step 4: returns the bytes appended to the journal copies.
    ///
    /// A `sealed` line on a copy must always name a file that copy holds
    /// (ADR-062 D11; review of #199, finding 3). So when a live file of the
    /// listing, or of the ledger's `manifest` and `copy` records, has no
    /// copy record for a target, that target's journal copy advances only up
    /// to the first line that names such a file, never through it;
    /// everything before that line is still appended. The ledger's paths
    /// count because the hot removal takes files off hot, and a target added
    /// after it never gets them from hot (#199 review carry-over): its copy
    /// waits until #192's repair fills the target. Archive parts are never
    /// checked: only the capture writes `sealed` lines. Lost paths are not
    /// added: no copy of them can ever reach a target (#192, #193 decide).
    fn copy_journal_tail(&mut self, paths: &[String]) -> u64 {
        let Some(len) = self.journal_len else {
            return 0;
        };
        let Some(name) = self.journal.file_name().map(ToOwned::to_owned) else {
            self.problem(format!(
                "journal {} has no file name",
                self.journal.display()
            ));
            return 0;
        };
        let mut appended = 0;
        for i in 0..self.targets.len() {
            if self.targets[i].stopped {
                continue;
            }
            let dest = self.targets[i]
                .path
                .join(META_DIR)
                .join(JOURNAL_DIR)
                .join(&name);
            let id = self.targets[i].id.clone();
            let copy_len = fs::metadata(&dest).map_or(0, |metadata| metadata.len());
            let found = {
                let fold = &self.fold;
                let recorded = fold
                    .manifests
                    .keys()
                    .chain(fold.copies.keys())
                    .filter(|path| !fold.lost.contains(*path));
                let short: BTreeSet<&str> = paths
                    .iter()
                    .chain(recorded)
                    .filter(|path| !is_archive_part(path) && !fold.has_copy(path, &id))
                    .map(String::as_str)
                    .collect();
                if short.is_empty() {
                    Ok(None)
                } else {
                    first_line_naming(self.journal, copy_len, len, &short)
                }
            };
            let mut end = len;
            match found {
                Ok(None) => {}
                Ok(Some((offset, path))) => {
                    self.problem(format!(
                        "the journal copy on {id} stops before byte {offset} of the journal: \
                         the line there names {path}, which is not on {id}, and a `sealed` \
                         line on a copy always names a file that copy holds (ADR-062 D11)"
                    ));
                    // A copy already past the line is only compared.
                    end = offset.max(copy_len.min(len));
                }
                Err(error) => {
                    self.problem(format!(
                        "the journal {} cannot be read: {error}",
                        self.journal.display()
                    ));
                    return appended;
                }
            }
            if !self.marker_present(i) {
                continue;
            }
            match replica::extend_prefix(TailSource::File(self.journal), &dest, end) {
                Ok(0) => {}
                Ok(bytes) => {
                    appended += bytes;
                    self.line(format!(
                        "journal copy on {id}: appended {bytes} byte(s), now {end} bytes"
                    ));
                }
                Err(CopyError {
                    side: Side::Source,
                    error,
                }) => {
                    self.problem(format!(
                        "the journal {} cannot be read: {error}",
                        self.journal.display()
                    ));
                    return appended;
                }
                Err(CopyError { error, .. }) if is_local(&error) => self.problem(format!(
                    "the journal copy on {id} ({}) is refused: {error}; it needs the repair \
                     (ADR-062 D11)",
                    dest.display()
                )),
                Err(CopyError { error, .. }) => {
                    self.stop_target(i, format!("extending its journal copy: {error}"));
                }
            }
        }
        appended
    }

    /// Names every file and entry below k.
    fn result(&mut self, paths: &[String], entries: &[Entry]) {
        let (k, configured) = (self.k, self.targets.len());
        let short_targets = configured < k || self.targets.iter().any(|target| target.stopped);
        for path in paths {
            let count = self.fold.copy_count(path);
            let reasons = self.reasons.remove(path).unwrap_or_default();
            if count >= k && reasons.is_empty() {
                continue;
            }
            let why = if !reasons.is_empty() {
                reasons.join("; ")
            } else if short_targets {
                "a copy target was skipped or stopped in this pass".to_owned()
            } else {
                "not copied".to_owned()
            };
            self.problem(format!("{path}: {count} of {k} copies: {why}"));
        }
        for entry in entries {
            let reasons = self.reasons.remove(&entry.name).unwrap_or_default();
            if reasons.is_empty() && !short_targets {
                continue;
            }
            let why = if reasons.is_empty() {
                "a copy target was skipped or stopped in this pass".to_owned()
            } else {
                reasons.join("; ")
            };
            self.problem(format!(
                "import entry {} is not on every copy target: {why}",
                entry.name
            ));
        }
    }
}
