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
//!    part only once a current import entry on hot lists it with its
//!    manifest's SHA-256 (D11 *Archive parts wait for their import
//!    entry*).
//! 2. **Import-ledger entries.** An entry goes to a target only once every
//!    part it lists has a copy record for that target, so per archive file
//!    its parts precede its entry. `PENDING` is never copied. Only current
//!    entries are copied, and supersede records, with no part gate (#210
//!    C3): a superseded entry is never copied and authorizes no part, and
//!    an archive file one of whose import-ledger files is malformed,
//!    unreadable or bound to another file has none of its files copied and
//!    none of its parts authorized in the pass.
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
//!    file's line until `mie store repair` fills it.
//!
//! **A target with an open copy-ledger finding** (a `finding R …
//! copy-ledger` after its latest `ledger-copy R` record) gets no ledger
//! copy extension and no `ledger-copy` record in step 3 (L2 of #192): such
//! a record would end the finding's void without any repair. The pass
//! names it as needing `mie store repair` and is not complete for the hot
//! removal. Only the closing pass of a repair, which names the targets it
//! repaired, extends them. A finding `mie store verify` records after the
//! pass read the ledger (it takes no pass lock) counts too: step 3 re-reads
//! the records after its read before it extends a copy, and appends the
//! `ledger-copy` record only under the same check inside one hold of the
//! ledger lock (review of #203, finding 1).
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
use mie_adapter_binance::archive::ledger::{LedgerKind, LedgerName, classify};
use mie_adapter_parquet::copy_ledger::{
    self, AppendError, AppendOutcome, CopyLedger, Guarded, Kind, Record, Subject, Truncation,
};
use mie_adapter_parquet::rebuilt;
use mie_adapter_parquet::removal::{self, RemovalPlan};
use mie_adapter_parquet::replica::{self, CopyError, CopyOutcome, Side, TailSource};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::verification;
use mie_ports::raw::{RawStoreError, validate_segment};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

#[cfg(test)]
pub(crate) mod tests;

/// Where a copy target keeps its copies of hot's metadata (ADR-062 D7
/// *Formats and layout*).
pub(super) const META_DIR: &str = ".mie";
pub(super) const JOURNAL_DIR: &str = "journal";
pub(super) const IMPORT_LEDGER_DIR: &str = "import-ledger";

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

    /// Appends `record` unless `veto`, given the records after byte
    /// `offset` as they stand under the same lock, refuses it.
    ///
    /// # Errors
    ///
    /// As [`CopyLedger::append_unless`].
    fn append_unless(
        &self,
        record: &Record,
        offset: u64,
        chain: &str,
        veto: &dyn Fn(&[Record]) -> Option<String>,
    ) -> Result<Guarded, AppendError>;
}

impl LedgerAppend for CopyLedger {
    fn path(&self) -> &Path {
        CopyLedger::path(self)
    }

    fn append(&self, record: &Record) -> Result<AppendOutcome, AppendError> {
        CopyLedger::append(self, record)
    }

    fn append_unless(
        &self,
        record: &Record,
        offset: u64,
        chain: &str,
        veto: &dyn Fn(&[Record]) -> Option<String>,
    ) -> Result<Guarded, AppendError> {
        CopyLedger::append_unless(self, record, offset, chain, veto)
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
    /// Step 1 is done, before the first entry copy: a crash here leaves an
    /// archive file's parts on the targets without its entry (#195, the
    /// held variant of the hot restore). Not in [`PassStep::ALL`].
    #[cfg(test)]
    BeforeEntries,
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
    /// Every configured copy target presented its marker, none was stopped,
    /// every one's ledger copy was extended or current (a refused copy holds
    /// no verified prefix), and the pass ran to its end.
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
    /// The root ids with an open copy-ledger finding (L2 of #192).
    pub(super) open_ledger: BTreeSet<String>,
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
        fold.open_ledger = verification::open_ledger_findings(records);
        Ok(fold)
    }

    pub(super) fn has_copy(&self, path: &str, id: &str) -> bool {
        self.copies.get(path).is_some_and(|ids| ids.contains(id))
    }

    fn copy_count(&self, path: &str) -> usize {
        self.copies.get(path).map_or(0, BTreeSet::len)
    }
}

/// One import-ledger file on hot (or, for the adoption, in the import
/// ledger of the store it adopts) that is copied like an entry: a current
/// entry or a supersede record (ADR-062 D11, proposed; #210 C3).
pub(super) struct Entry {
    /// `<segment>/<name>.import`.
    pub(super) name: String,
    pub(super) segment: String,
    pub(super) file_name: String,
    pub(super) path: PathBuf,
    /// The data paths it lists, with their SHA-256; none for a supersede
    /// record, which authorizes no part and waits for none.
    pub(super) files: Vec<(String, String)>,
    /// Its generation (G1).
    pub(super) generation: u32,
    /// An entry or a supersede record.
    pub(super) kind: LedgerKind,
}

/// The import ledger as the copy commands read it (#210 C3): every current
/// entry and supersede record, the superseded entries, and every problem.
#[derive(Default)]
pub(super) struct HotEntries {
    /// Current entries and supersede records, by name.
    pub(super) entries: Vec<Entry>,
    /// Every superseded entry `<segment>/<name>.import`, with the supersede
    /// record of the highest generation of its archive file.
    pub(super) superseded: BTreeMap<String, String>,
    /// Listing, read, name, parse and binding failures.
    pub(super) problems: Vec<String>,
}

/// A check over the records after the pass's ledger read: `Some` with
/// the reason it refuses an append.
type Veto<'a> = &'a dyn Fn(&[Record]) -> Option<String>;

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
    /// The end of the verified prefix the pass read, and the chain hash of
    /// its last record: step 3 re-reads the records after it (L2).
    read_len: u64,
    read_chain: String,
    report: PassReport,
    /// Per file or entry, why it is short of a copy.
    reasons: BTreeMap<String, Vec<String>>,
    /// A ledger copy was refused or hot's ledger could not be read for
    /// them: a target without a verified ledger copy this pass, so the
    /// pass is not complete for the hot removal's rule (f) (review of #202,
    /// finding 1).
    ledger_copy_failed: bool,
    /// The root ids whose ledger copy a repair rebuilt: exempt from L2.
    cleared: &'a BTreeSet<String>,
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
        self.append_guarded(kind, None).map(|_| ())
    }

    /// [`append`](Self::append), or with a `veto` the guarded append of
    /// [`LedgerAppend::append_unless`] over the records after the pass's
    /// read; `Some` with the veto's reason when it refused the record.
    fn append_guarded(
        &mut self,
        kind: Kind,
        veto: Option<Veto<'_>>,
    ) -> Result<Option<String>, Stop> {
        let record = Record {
            at_ms: (self.now_ms)(),
            kind,
        };
        let appended = match veto {
            None => self.ledger.append(&record).map(Guarded::Appended),
            Some(veto) => self
                .ledger
                .append_unless(&record, self.read_len, &self.read_chain, veto),
        };
        let (truncation, result) = match appended {
            Ok(Guarded::Appended(outcome)) => (outcome.truncation, Ok(None)),
            Ok(Guarded::Vetoed(reason)) => (None, Ok(Some(reason))),
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
    run_with(
        config,
        archive,
        probe,
        now_ms,
        ledger,
        observer,
        None,
        &BTreeSet::new(),
    )
}

/// The pass of `mie ingest` (module docs): [`run`] with the archive
/// configuration already resolved, then, when every configured copy target
/// presented its marker and none stopped, the hot removal's plan from a
/// fresh ledger read and one walk of hot, with the hot window starting on
/// `first_hot_date`. A failed read or walk is a problem and gives no plan.
/// After a copy-ledger rebuild the plan reads its gate (`.mie/rebuilt`,
/// #193 B10) after the ledger and waits for a verification of every target
/// that covers the rebuilt ledger; a gate that cannot be read or parsed
/// gives no plan.
pub fn run_planned(
    config: &IngestConfig,
    archive: Option<ArchiveConfig>,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    ledger: &dyn LedgerAppend,
    observer: &mut dyn PassObserver,
    first_hot_date: &str,
) -> (PassReport, Option<RemovalPlan>) {
    let mut report = run_with(
        config,
        Ok(archive),
        probe,
        now_ms,
        ledger,
        observer,
        None,
        &BTreeSet::new(),
    );
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
    // The ledger first, the rebuild's gate second: the rebuild writes the
    // gate before it renames the ledger into place, so a read that sees the
    // rebuilt ledger always finds its gate (#193 B10).
    let planned = match copy_ledger::read(hot) {
        Ok(Some(read)) => rebuilt::read(hot)
            .map_err(|error| format!("the copy-ledger rebuild's gate: {error}"))
            .and_then(|gate| {
                removal::scan_hot(hot, first_hot_date)
                    .and_then(|scan| {
                        removal::plan(&read, &scan, &report.target_ids, k, first_hot_date, gate)
                    })
                    .map_err(|error| error.to_string())
            }),
        Ok(None) => Err(format!(
            "copy ledger {} does not exist",
            copy_ledger::ledger_path(hot).display()
        )),
        Err(error) => Err(error.to_string()),
    };
    match planned {
        Ok(plan) => {
            // A D5 conflict keeps its partition on hot and fails the pass.
            report.problems.extend(plan.conflicts.iter().cloned());
            (report, Some(plan))
        }
        Err(error) => {
            report.problems.push(format!(
                "the hot removal's plan failed, so nothing is removed after this pass: {error}"
            ));
            (report, None)
        }
    }
}

/// [`run`] under a pass lock the caller already holds: `mie store adopt`
/// and `mie store repair` end with one pass and hold the lock for their
/// whole run (ADR-062 D10, D11). `cleared` names the copy targets whose
/// ledger copy the repair rebuilt, which L2 then lets through; the adoption
/// passes an empty set.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_locked(
    config: &IngestConfig,
    archive: Option<&Path>,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    ledger: &dyn LedgerAppend,
    observer: &mut dyn PassObserver,
    lock: replica::ReplicationLock,
    cleared: &BTreeSet<String>,
) -> PassReport {
    let archive = archive_config(config, archive);
    run_with(
        config,
        archive,
        probe,
        now_ms,
        ledger,
        observer,
        Some(lock),
        cleared,
    )
}

/// The pass, taking the lock at its place in step 0 unless one is given.
#[allow(clippy::too_many_arguments)]
fn run_with(
    config: &IngestConfig,
    archive: Result<Option<ArchiveConfig>, String>,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    ledger: &dyn LedgerAppend,
    observer: &mut dyn PassObserver,
    lock: Option<replica::ReplicationLock>,
    cleared: &BTreeSet<String>,
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
        cleared,
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
    cleared: &'a BTreeSet<String>,
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
    let (records, read_len, read_chain) = match copy_ledger::read(hot) {
        Ok(Some(ledger)) => (ledger.records, ledger.verified_len, ledger.last_chain),
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
        read_len,
        read_chain,
        report: std::mem::take(report),
        reasons: BTreeMap::new(),
        ledger_copy_failed: false,
        cleared,
    })
}

/// L2 for a finding the pass's fold never saw: the first copy-ledger
/// finding on `id` among the records after the pass's ledger read. Only
/// the pass appends `ledger-copy` records (it holds the pass lock), so
/// such a finding is still open.
fn finding_since_read(tail: &[Record], id: &str) -> Option<String> {
    tail.iter().find_map(|record| match &record.kind {
        Kind::Finding {
            root,
            code,
            subject: Subject::CopyLedger,
        } if root == id => Some(format!(
            "a `finding {id} {} copy-ledger` record appended at {} after this pass read the \
             copy ledger",
            code.as_str(),
            record.at_ms
        )),
        _ => None,
    })
}

/// The problem of a ledger copy held by a finding recorded during the pass.
fn held_by(id: &str, target: &Path, reason: &str) -> String {
    format!(
        "the copy-ledger copy on {id} ({}) has an open copy-ledger finding ({reason}): it gets \
         no ledger-copy record until `mie store repair` repairs it (ADR-062 D6, D11)",
        copy_ledger::ledger_path(target).display()
    )
}

/// Whether a data path belongs to the archive source.
pub(super) fn is_archive_part(path: &str) -> bool {
    path.strip_prefix("source=")
        .and_then(|rest| rest.split_once('/'))
        .is_some_and(|(source, _)| source == ARCHIVE_SOURCE)
}

/// One import-entry file of an import-ledger directory.
pub(super) struct EntryFile {
    /// `<segment>/<name>.import`.
    pub(super) name: String,
    pub(super) segment: String,
    pub(super) file_name: String,
    pub(super) path: PathBuf,
}

/// The import-entry files of the import-ledger directory `import_ledger`
/// (hot's, or a copy target's `.mie/import-ledger/`),
/// `<segment>/<name>.import`, sorted; `PENDING`, hidden temp files and
/// anything else are never listed. Every listing error is returned, with
/// the files of the directories that could be listed: the pass names it
/// and goes on, the rebuild stops on it (#193 B1).
pub(super) fn entry_files(import_ledger: &Path) -> (Vec<EntryFile>, Vec<String>) {
    let (mut files, mut errors) = (Vec::new(), Vec::new());
    let segments = match sorted_names(import_ledger) {
        Ok(names) => names,
        Err(error) => {
            errors.push(format!(
                "import ledger {}: cannot list it ({error})",
                import_ledger.display()
            ));
            return (files, errors);
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
                errors.push(format!("import ledger {}: {error}", dir.display()));
                continue;
            }
        };
        for file_name in names {
            if file_name.starts_with('.') || !file_name.ends_with(".import") {
                continue;
            }
            files.push(EntryFile {
                name: format!("{segment}/{file_name}"),
                segment: segment.clone(),
                path: dir.join(&file_name),
                file_name,
            });
        }
    }
    (files, errors)
}

/// The archive file a malformed import-ledger name belongs to, when it
/// can tell: the name up to its first `.zip.`.
fn archive_file_of(file_name: &str) -> Option<&str> {
    file_name
        .find(".zip.")
        .map(|at| &file_name[..at + ".zip".len()])
}

/// The files of one archive file in one directory, or why it fails closed.
type Group = Result<Vec<(EntryFile, LedgerName)>, String>;

/// The import ledger at `import_ledger` (hot's, or the store an adoption
/// reads), classified per archive file (#210 G1–G4, C3): the current
/// entries and the supersede records are returned to be copied, the
/// superseded entries are named. `PENDING`, hidden temp files and other
/// names are never listed. A file of an archive file that is malformed,
/// cannot be read or parsed, or names another archive file fails that
/// archive file closed: a problem names it, and none of its files is
/// returned, so none is copied and none of its parts is authorized.
pub(super) fn hot_entries(import_ledger: &Path) -> HotEntries {
    let (files, listing) = entry_files(import_ledger);
    let mut found = HotEntries {
        problems: listing
            .into_iter()
            .map(|error| {
                if error.contains("cannot list it") {
                    format!("{error}; no entry and no archive part is replicated in this pass")
                } else {
                    error
                }
            })
            .collect(),
        ..HotEntries::default()
    };
    // Per (segment, archive file): its files, or why it fails closed.
    let mut groups: BTreeMap<(String, String), Group> = BTreeMap::new();
    for file in files {
        match LedgerName::parse(&file.file_name) {
            Ok(name) => {
                if let Ok(list) = groups
                    .entry((file.segment.clone(), name.file.clone()))
                    .or_insert_with(|| Ok(Vec::new()))
                {
                    list.push((file, name));
                }
            }
            Err(error) => {
                let why = format!(
                    "import-ledger file {} ({}): {error}",
                    file.name,
                    file.path.display()
                );
                match archive_file_of(&file.file_name) {
                    Some(archive_file) => {
                        groups.insert((file.segment.clone(), archive_file.to_owned()), Err(why));
                    }
                    None => found
                        .problems
                        .push(format!("{why}; it is not replicated (ADR-062 D11)")),
                }
            }
        }
    }
    for ((segment, archive_file), group) in groups {
        let fail = |why: String| {
            format!(
                "{why}; no import-ledger file of {segment}/{archive_file} is replicated \
                 and none of its parts is authorized in this pass (ADR-062 D11)"
            )
        };
        let files = match group {
            Ok(files) => files,
            Err(why) => {
                found.problems.push(fail(why));
                continue;
            }
        };
        let mut pairs = Vec::new();
        let mut unreadable = None;
        for (file, name) in &files {
            match fs::read(&file.path) {
                Ok(bytes) => pairs.push((name.clone(), bytes)),
                Err(error) => {
                    unreadable = Some(format!(
                        "import-ledger file {} ({}): {error}",
                        file.name,
                        file.path.display()
                    ));
                    break;
                }
            }
        }
        if let Some(why) = unreadable {
            found.problems.push(fail(why));
            continue;
        }
        let classified = match classify(&segment, &pairs) {
            Ok(classified) => classified,
            Err(error) => {
                found.problems.push(fail(format!(
                    "import ledger {}: {error}",
                    import_ledger.display()
                )));
                continue;
            }
        };
        let latest_record = classified
            .records
            .last()
            .map(|(name, _)| format!("{segment}/{}", name.render()));
        for name in &classified.superseded {
            found.superseded.insert(
                format!("{segment}/{}", name.render()),
                latest_record.clone().unwrap_or_default(),
            );
        }
        let current = classified.current.as_ref().map(|(name, _)| name);
        for (file, name) in files {
            let listed = match (name.kind, Some(&name) == current) {
                (LedgerKind::Entry, true) => classified
                    .current
                    .as_ref()
                    .map(|(_, ledger)| {
                        ledger
                            .files
                            .iter()
                            .map(|f| (f.relative_path.clone(), f.sha256.clone()))
                            .collect()
                    })
                    .unwrap_or_default(),
                (LedgerKind::Supersede, _) => Vec::new(),
                (LedgerKind::Entry, false) => continue,
            };
            found.entries.push(Entry {
                name: file.name,
                segment: file.segment,
                file_name: file.file_name,
                path: file.path,
                files: listed,
                generation: name.generation,
                kind: name.kind,
            });
        }
    }
    found.entries.sort_by(|a, b| a.name.cmp(&b.name));
    found
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
        let HotEntries {
            entries, problems, ..
        } = match &self.archive {
            Some(archive) => hot_entries(&archive.paths.import_ledger),
            None => HotEntries::default(),
        };
        self.report.problems.extend(problems);
        let mut copied = 0usize;
        for path in &paths {
            copied += self.copy_file(path, &entries)?;
        }
        #[cfg(test)]
        reached(PassStep::BeforeEntries)?;
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
        self.report.targets_complete = self.targets.len() == self.k
            && self.targets.iter().all(|t| !t.stopped)
            && !self.ledger_copy_failed;
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
            // L2 (#192): an open copy-ledger finding blocks the extension
            // and its record until a repair rebuilt the copy.
            if self.fold.open_ledger.contains(&id) && !self.cleared.contains(&id) {
                self.ledger_copy_failed = true;
                self.problem(format!(
                    "the copy-ledger copy on {id} ({}) has an open copy-ledger finding: it is \
                     not extended and gets no ledger-copy record until `mie store repair` \
                     repairs it (ADR-062 D6, D11)",
                    copy_ledger::ledger_path(&path).display()
                ));
                continue;
            }
            // L2 again, for a finding recorded after the pass read the
            // ledger: `mie store verify` takes no pass lock (V10), and the
            // fold above never saw it (review of #203, finding 1).
            match copy_ledger::records_after(self.hot, self.read_len, &self.read_chain) {
                Ok(tail) => {
                    if let Some(reason) = finding_since_read(&tail, &id) {
                        self.ledger_copy_failed = true;
                        self.problem(held_by(&id, &path, &reason));
                        continue;
                    }
                }
                Err(error) => {
                    self.ledger_copy_failed = true;
                    self.problem(format!(
                        "hot's copy ledger cannot be re-read for the ledger copies: {error}"
                    ));
                    return Ok(appended);
                }
            }
            let copy = match copy_ledger::extend_copy(self.hot, &path, &id) {
                Ok(copy) => copy,
                Err(CopyError {
                    side: Side::Source,
                    error,
                }) => {
                    self.ledger_copy_failed = true;
                    self.problem(format!(
                        "hot's copy ledger cannot be read for the ledger copies: {error}"
                    ));
                    return Ok(appended);
                }
                Err(CopyError { error, .. }) if is_local(&error) => {
                    self.ledger_copy_failed = true;
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
            // The check and the record under one hold of the ledger lock: a
            // finding recorded after the extension is still seen.
            let veto = |tail: &[Record]| finding_since_read(tail, &id);
            let vetoed = self.append_guarded(
                Kind::LedgerCopy {
                    root: id.clone(),
                    prefix_bytes: copy.prefix_bytes,
                    prefix_sha256: copy.prefix_sha256,
                },
                Some(&veto),
            )?;
            if let Some(reason) = vetoed {
                self.ledger_copy_failed = true;
                self.problem(held_by(&id, &path, &reason));
                continue;
            }
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
    /// waits until `mie store repair` fills the target. Archive parts are never
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
