//! The repair (ADR-062 D11 *On a finding*, #192): `mie store repair`.
//!
//! An explicit step, never run by the verification: several bad copies at
//! once point at a shared cause, such as a controller that returns bad
//! bytes, so an operator looks before anything is rewritten. Its work list
//! is what the copy ledger records (the findings `mie store verify` left
//! open) plus the copies a configured target lacks without any record (a
//! target added or replaced after files left hot, the comment of
//! 2026-11-07 on #192). Every work item is re-checked first, so a copy that
//! came back (a remount) is only re-recorded. The rules, as numbered in the
//! plan of #192 (the tests cite them):
//!
//! - **R0 Refusals and locks.** The refusals of the verification, without
//!   P. Then the verification lock and the pass lock, both without waiting,
//!   held for the whole run: replication pauses while it runs. A copy target
//!   without its marker is named, skipped and never written.
//! - **R1 Data copies with an open finding** (a finding after the latest
//!   `copy` record, the remover's rule): good now, a fresh `copy` record and
//!   no byte written; a self-consistent other file, refused and never moved
//!   (D5); otherwise a good source (R3), the bad copy moved aside (R5), the
//!   D5 copy from the source, its published text checked against the
//!   ledger's, the marker re-read, and a fresh `copy` record. That record
//!   ends the finding's void, so a repaired partition becomes removable
//!   again (the comment of 2026-11-08).
//! - **R2 Refill:** a path that left hot (its partition recorded removed)
//!   with a recorded copy on some configured target but none on R is copied
//!   to R from a source and recorded. The class is the adoption's own
//!   (`adopt::complete`), so repair and adopt never disagree. Files on hot
//!   that lack a target are left to the closing pass.
//! - **R3 Sources, in order:** hot, then every cold root in configuration
//!   order (copy targets and read-only roots alike), never the destination.
//!   A source counts only when its manifest text equals the ledger's byte
//!   for byte and its data verifies. A root whose copy cannot be read is
//!   no source and is named (review of #203, finding 2).
//! - **R4 Hot copies:** a bad hot copy, or a recorded path hot lacks
//!   although its partition is not recorded removed, is replaced from the
//!   first good cold copy after R5's move-aside, under the source's writer
//!   lock, taken without waiting. A writer holding it (`mie ingest` for the
//!   live source, an import for the archive) leaves the copy unrepaired and
//!   named. Under the lock the ledger is read again, and a path whose
//!   partition the hot removal recorded removed since is skipped (review of
//!   #203, finding 4). Hot copies get no record: they never count (D11).
//! - **R5 Move-aside** into `<root>/.mie/damaged/<run at_ms>/<relative
//!   path>`: the data first, then its manifest and the seal temp names,
//!   each an atomic rename on the same filesystem. Nothing in mie deletes or
//!   lists it: the bytes are the evidence of a shared cause, the rename
//!   costs no I/O, and an overwrite that crashes leaves neither version.
//! - **R6 Ledger copies with an open copy-ledger finding:** when the copy's
//!   chain fails at or before the first byte f where it differs from hot,
//!   it is moved aside and rebuilt as hot's verified bytes up to f, and the
//!   closing pass extends it; when only its unterminated tail differs (a
//!   crashed extension), it is rebuilt the same way up to its last complete
//!   record (review of #203, finding 3); when its complete records verify
//!   but differ from hot's, or run past hot's verified prefix, both chains
//!   are valid and one of them holds records the other lacks, so it is
//!   refused, naming `mie store rebuild --replace-hot-ledger`, which
//!   supersedes both chains (#193).
//! - **R7 Journal copies with an open journal finding:** let d be the first
//!   byte where the copy differs from hot. Repaired only when another
//!   target's copy is longer than d and equals hot over `[0, d]` (two of
//!   three agree): the copy is moved aside and rebuilt up to the start of
//!   the line that holds d, and the closing pass extends it under its usual
//!   journal hold. Otherwise refused: the journal has no chain, so one copy
//!   against hot cannot be attributed; when every other copy agrees with
//!   this one against hot, hot's journal is named as the suspect, for the
//!   hot restore (#195).
//! - **R8 Entry copies with an open entry finding:** replaced from hot only
//!   when hot's entry lists every file with the SHA-256 of the ledger's
//!   `manifest` record (else hot's manifest); otherwise refused as an entry
//!   conflict, for the hold resolution (#194). An absent copy is left to the
//!   closing pass.
//! - **R9 The lost-file rule:** a path with no good source, while every
//!   root the ledger records a copy of it on presents its marker again at
//!   that moment and every root tried was read without error, is reported
//!   lost, and nothing is re-created or written to any root; otherwise it
//!   is "not repairable in this run" (review of #203, finding 2). Only with `--record-lost` does the run also append `lost <at_ms>
//!   <path> verify <sha256>`, which fails every selection that covers the
//!   path for good; the operator passes it after a run without it reported
//!   the path (#192, STOP 1 decision 1). A recorded copy on a root that is
//!   absent or no longer configured makes the path "not repairable in this
//!   run", never lost.
//! - **R10 The closing pass:** one replication pass under the held pass
//!   lock, with the targets R6 repaired exempt from the replicator's
//!   copy-ledger hold, so the fresh `copy` records land inside the prefix k
//!   ledger copies hold and a refilled target's journal copy moves past the
//!   refilled files. The repair passes only when every work item was
//!   repaired or re-verified, nothing was refused or lost, and the closing
//!   pass passed.

use super::adopt::{self, Target};
use super::replicate::{
    self, Entry, Fold, IMPORT_LEDGER_DIR, JOURNAL_DIR, LedgerAppend, META_DIR, PassObserver,
    hot_entries,
};
use super::{DeviceProbe, check_devices, check_hot, truncation_line};
use crate::config::{ArchiveConfig, IngestConfig};
use mie_adapter_parquet::copy_ledger::{
    self, AppendError, Kind, LedgerCopyCheck, LostOrigin, Record, Truncation,
};
use mie_adapter_parquet::removal;
use mie_adapter_parquet::replica::{self, CopyCheck, CopyError, Side, TailSource};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::verification::{CopyState, PathVerdict};
use mie_ports::raw::RawStoreError;
use std::collections::BTreeSet;
use std::fs;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// The steps a test can stop the repair after, as a crash would.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepairStep {
    /// A damaged copy was moved aside, before the new copy.
    AfterMoveAside,
    /// A data copy was published, before its `copy` record.
    AfterPublish,
    /// A tail copy (ledger or journal) was rebuilt, before the closing
    /// pass.
    AfterTailRebuild,
}

#[cfg(test)]
impl RepairStep {
    pub(crate) const ALL: [Self; 3] = [
        Self::AfterMoveAside,
        Self::AfterPublish,
        Self::AfterTailRebuild,
    ];
}

#[cfg(test)]
thread_local! {
    /// Test hook: the repair stops at the first occurrence of this step.
    pub(crate) static CRASH_AT: std::cell::Cell<Option<RepairStep>> =
        const { std::cell::Cell::new(None) };
    /// Test hook: runs once after the run's ledger read and marker reads,
    /// before R1 (a concurrent removal, an unmount mid-run).
    pub(crate) static BEFORE_WORK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

/// Why the repair stopped early.
enum Stop {
    /// A failed ledger append, already in the report.
    Failed,
    /// A simulated crash (tests).
    #[cfg(test)]
    Crash,
}

/// Marks a step; under test, stops there once.
fn reached(step: RepairStep) -> Result<(), Stop> {
    #[cfg(test)]
    if CRASH_AT.with(std::cell::Cell::get) == Some(step) {
        CRASH_AT.with(|crash| crash.set(None));
        return Err(Stop::Crash);
    }
    let _ = step;
    Ok(())
}

/// What one repair did: its report lines and every problem that makes it
/// fail.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RepairReport {
    /// What was repaired, re-verified and recorded, then the closing pass's
    /// lines and every copy-ledger truncation.
    pub lines: Vec<String>,
    /// Every refusal, lost file, unrepaired item and closing-pass problem;
    /// empty when the repair passed.
    pub problems: Vec<String>,
}

/// Collects every copy-ledger truncation as a report line.
#[derive(Default)]
struct Truncations(Vec<String>);

impl PassObserver for Truncations {
    fn ledger_truncated(&mut self, ledger: &Path, truncation: Truncation) {
        self.0.push(truncation_line(ledger, truncation));
    }
}

/// A cold root that presented its marker.
struct Marked {
    path: PathBuf,
    id: String,
    copy_target: bool,
}

/// What R3 found for one path.
struct Sources {
    /// The first root with a good copy.
    good: Option<PathBuf>,
    /// The roots tried before it whose copy could not be read, each with
    /// its error.
    unread: Vec<String>,
}

/// The repair in progress.
struct Repair<'a> {
    config: &'a IngestConfig,
    hot: &'a Path,
    archive: Option<ArchiveConfig>,
    record_lost: bool,
    now_ms: &'a dyn Fn() -> i64,
    /// The run's `at_ms`: the name of its `.mie/damaged/` directories.
    run_ms: i64,
    ledger: &'a dyn LedgerAppend,
    truncations: Truncations,
    state: CopyState,
    fold: Fold,
    removed: BTreeSet<String>,
    /// Every cold root that presented its marker, in configuration order.
    marked: Vec<Marked>,
    entries: Vec<Entry>,
    /// The copy targets whose ledger copy R6 repaired (exempt from L2).
    cleared: BTreeSet<String>,
    /// The paths already judged by R9 in this run.
    judged: BTreeSet<String>,
    report: RepairReport,
}

/// Runs one repair (module docs). Every outcome is in the report.
pub fn run(
    config: &IngestConfig,
    archive: Option<&Path>,
    record_lost: bool,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    ledger: &dyn LedgerAppend,
) -> RepairReport {
    let mut report = RepairReport::default();
    let hot = config.paths.raw_root.as_path();
    let cold_roots = &config.paths.cold_roots;
    let mut refuse = |problem: String| {
        report.problems.push(problem);
    };
    if cold_roots.is_empty() {
        refuse(
            "paths.cold_roots is empty: a single-root store has no copies to repair (ADR-062 D11)"
                .to_owned(),
        );
        return report;
    }
    if let Err(error) = check_hot(hot, cold_roots) {
        refuse(error);
        return report;
    }
    let archive_config = match replicate::archive_config(config, archive) {
        Ok(archive) => archive,
        Err(error) => {
            refuse(error);
            return report;
        }
    };
    let _verify_lock = match replica::try_verify_lock(hot) {
        Ok(lock) => lock,
        Err(error) => {
            refuse(error.to_string());
            return report;
        }
    };
    let pass_lock = match replica::try_replication_lock(hot) {
        Ok(lock) => lock,
        Err(error) => {
            refuse(error.to_string());
            return report;
        }
    };
    let ledger_path = copy_ledger::ledger_path(hot);
    let snapshot = match copy_ledger::read(hot) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            refuse(format!(
                "copy ledger {} does not exist: the copy-ledger rebuild restores it (ADR-062 D7)",
                ledger_path.display()
            ));
            return report;
        }
        Err(error) => {
            refuse(format!("hot's copy ledger: {error}"));
            return report;
        }
    };
    let folded = CopyState::fold(&snapshot)
        .and_then(|state| Fold::new(&snapshot.records).map(|fold| (state, fold)));
    let (state, fold) = match folded {
        Ok(folded) => folded,
        Err(error) => {
            refuse(format!("copy ledger {}: {error}", ledger_path.display()));
            return report;
        }
    };
    let (removed, _) = adopt::ledger_states(&snapshot.records);

    let mut marked: Vec<Marked> = Vec::new();
    let mut skipped = Vec::new();
    for cold in cold_roots {
        match root_marker::read(&cold.path) {
            Ok(Some(id)) if id == HOT_ID => {
                refuse(format!(
                    "cold root {} carries hot's marker (id {HOT_ID:?}), the id of raw_root {}: \
                     the same root under two names, or a copied marker (ADR-062 D7)",
                    cold.path.display(),
                    hot.display()
                ));
                return report;
            }
            Ok(Some(id)) => {
                if let Some(other) = marked.iter().find(|root| root.id == id) {
                    refuse(format!(
                        "cold roots {} and {} both carry the id {id:?}: two spellings of one \
                         root, or a copied marker (ADR-062 D7)",
                        other.path.display(),
                        cold.path.display()
                    ));
                    return report;
                }
                marked.push(Marked {
                    path: cold.path.clone(),
                    id,
                    copy_target: cold.copy_target,
                });
            }
            Ok(None) if cold.copy_target => skipped.push(format!(
                "copy target {} has no root marker: skipped, and nothing is written into it (an \
                 unmounted mount point is an empty directory; ADR-062 D11)",
                cold.path.display()
            )),
            Err(error) if cold.copy_target => skipped.push(format!(
                "copy target {}: its root marker cannot be read ({error}): skipped, and nothing \
                 is written into it (ADR-062 D11)",
                cold.path.display()
            )),
            // A read-only root without a marker is no source.
            Ok(None) | Err(_) => {}
        }
    }
    let targets: Vec<&Path> = marked
        .iter()
        .filter(|root| root.copy_target)
        .map(|root| root.path.as_path())
        .collect();
    if let Err(error) = check_devices(hot, &targets, probe) {
        refuse(error);
        return report;
    }
    report.problems.extend(skipped);
    let mut problems = Vec::new();
    let entries = match &archive_config {
        Some(archive) => {
            let (entries, entry_problems) = hot_entries(&archive.paths.import_ledger);
            problems.extend(entry_problems);
            entries
        }
        None => Vec::new(),
    };
    report.problems.extend(problems);

    let mut repair = Repair {
        config,
        hot,
        archive: archive_config,
        record_lost,
        now_ms,
        run_ms: now_ms(),
        ledger,
        truncations: Truncations::default(),
        state,
        fold,
        removed,
        marked,
        entries,
        cleared: BTreeSet::new(),
        judged: BTreeSet::new(),
        report,
    };
    #[cfg(test)]
    if let Some(hook) = BEFORE_WORK.with(|hook| hook.borrow_mut().take()) {
        hook();
    }
    match repair.repair_all() {
        Ok(()) => {
            // R10: the closing pass under the held pass lock.
            let pass = replicate::run_locked(
                config,
                archive,
                probe,
                now_ms,
                ledger,
                &mut repair.truncations,
                pass_lock,
                &repair.cleared,
            );
            repair.report.lines.extend(pass.lines);
            repair.report.problems.extend(pass.problems);
        }
        Err(Stop::Failed) => {}
        #[cfg(test)]
        Err(Stop::Crash) => repair
            .report
            .problems
            .push("simulated crash: the repair stopped".to_owned()),
    }
    let mut report = repair.report;
    report.lines.extend(repair.truncations.0);
    report
}

/// The partition directory of a data path.
fn partition_of(path: &str) -> &str {
    adopt::partition_of(path)
}

/// The source of a data path: its `source=` value.
fn source_of(path: &str) -> &str {
    path.strip_prefix("source=")
        .and_then(|rest| rest.split_once('/'))
        .map_or("", |(source, _)| source)
}

/// The byte at `offset` of `path`, `None` past its end.
fn byte_at(path: &Path, offset: u64) -> std::io::Result<Option<u8>> {
    let mut file = fs::File::open(path)?;
    if file.metadata()?.len() <= offset {
        return Ok(None);
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut byte = [0u8];
    file.read_exact(&mut byte)?;
    Ok(Some(byte[0]))
}

/// Where a copy's relative `path` goes in the damaged directory of `root`.
fn aside_of(root: &Path, run_ms: i64, relative: &Path) -> PathBuf {
    replica::damaged_dir(root, run_ms).join(relative)
}

impl Repair<'_> {
    fn line(&mut self, line: String) {
        self.report.lines.push(line);
    }

    fn problem(&mut self, problem: String) {
        self.report.problems.push(problem);
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
            let path = self.ledger.path().to_owned();
            self.truncations.ledger_truncated(&path, truncation);
        }
        result.map_err(|error| {
            let message = format!(
                "the {} append to the copy ledger {} failed, so the repair stops: {error}",
                record.kind.name(),
                self.ledger.path().display()
            );
            self.problem(message);
            Stop::Failed
        })
    }

    /// Whether the marked root `i` still presents its marker; names it
    /// otherwise.
    fn marker_present(&mut self, i: usize) -> bool {
        let root = &self.marked[i];
        let why = match root_marker::read(&root.path) {
            Ok(Some(id)) if id == root.id => return true,
            Ok(Some(other)) => format!("its marker now carries the id {other:?}"),
            Ok(None) => "its marker is gone (an unmounted mount point?)".to_owned(),
            Err(error) => format!("its marker cannot be read: {error}"),
        };
        let message = format!(
            "copy target {} ({}): {why}; nothing more is recorded for it (ADR-062 D11)",
            root.path.display(),
            root.id
        );
        self.problem(message);
        false
    }

    /// The name of the marked root `i`.
    fn name(&self, i: usize) -> String {
        let root = &self.marked[i];
        format!("{} ({})", root.path.display(), root.id)
    }

    /// The marked copy targets, as the adoption's completeness check
    /// takes them.
    fn adopt_targets(&self) -> Vec<Target> {
        self.marked
            .iter()
            .filter(|root| root.copy_target)
            .map(|root| Target {
                path: root.path.clone(),
                id: root.id.clone(),
            })
            .collect()
    }

    /// R1 to R8, in order.
    fn repair_all(&mut self) -> Result<(), Stop> {
        let targets: Vec<usize> = (0..self.marked.len())
            .filter(|&i| self.marked[i].copy_target)
            .collect();
        // R1.
        for &i in &targets {
            let id = self.marked[i].id.clone();
            let open: Vec<(String, String)> = self
                .state
                .copied_paths()
                .filter(|path| {
                    self.state
                        .copy(path, &id)
                        .is_some_and(|copy| copy.latest.is_some() && copy.open.is_some())
                        && matches!(
                            self.state.verdict(path),
                            Some(PathVerdict::Ordinary | PathVerdict::Held)
                        )
                })
                .filter_map(|path| {
                    self.state
                        .ledger_text(path)
                        .map(|text| (path.to_owned(), text.to_owned()))
                })
                .collect();
            for (path, text) in open {
                self.repair_data(i, &path, &text, false)?;
            }
        }
        // R2.
        let adopt_targets = self.adopt_targets();
        let refill: Vec<(String, String, Vec<String>)> = self
            .fold
            .manifests
            .iter()
            .filter(|(path, _)| self.state.verdict(path) == Some(PathVerdict::Ordinary))
            .filter_map(|(path, text)| {
                match adopt::complete(self.hot, &self.fold, path, &adopt_targets, &self.removed) {
                    Ok(Some(gap)) if gap.needs_refill() && gap.repairable() => {
                        Some((path.clone(), text.clone(), gap.missing))
                    }
                    _ => None,
                }
            })
            .collect();
        for (path, text, missing) in refill {
            for id in missing {
                if let Some(i) = self.marked.iter().position(|root| root.id == id) {
                    self.repair_data(i, &path, &text, true)?;
                }
            }
        }
        // R4.
        self.repair_hot()?;
        // R6, R7, R8.
        for &i in &targets {
            if self.state.open_ledger_finding(&self.marked[i].id) {
                self.repair_ledger_copy(i)?;
            }
            if self.state.open_journal_finding(&self.marked[i].id) {
                self.repair_journal_copy(i, &targets)?;
            }
            for entry in self.state.open_entry_findings(&self.marked[i].id) {
                self.repair_entry_copy(i, &entry)?;
            }
        }
        Ok(())
    }

    /// The first good source of `path` (R3), never `exclude`, and every
    /// root tried before it whose copy could not be read. A read error is
    /// no verdict on the copy (EIO from a failing disk may hide intact
    /// bytes), so R9 never calls a path lost past one (review of #203,
    /// finding 2).
    fn source(&self, path: &str, text: &str, exclude: &Path) -> Sources {
        let mut unread = Vec::new();
        for root in std::iter::once(self.hot)
            .chain(self.marked.iter().map(|root| root.path.as_path()))
            .filter(|root| *root != exclude)
        {
            match replica::check_sealed_copy(root, path, text) {
                Ok(CopyCheck::Good) => {
                    return Sources {
                        good: Some(root.to_path_buf()),
                        unread,
                    };
                }
                Ok(_) => {}
                Err(error) => unread.push(format!("{} cannot be read ({error})", root.display())),
            }
        }
        Sources { good: None, unread }
    }

    /// The good source of `path` from `sources`, naming the roots that
    /// could not be read on the way; R9 when there is none.
    fn good_source(&mut self, path: &str, text: &str, sources: Sources) -> Option<PathBuf> {
        let Some(good) = sources.good else {
            self.lost(path, text, &sources.unread);
            return None;
        };
        for unread in sources.unread {
            self.line(format!(
                "{path}: {unread}; it is skipped as a source and {} is used",
                good.display()
            ));
        }
        Some(good)
    }

    /// R1 (`refill` false) or R2 (`refill` true) for the copy of `path` on
    /// the marked target `i`.
    fn repair_data(&mut self, i: usize, path: &str, text: &str, refill: bool) -> Result<(), Stop> {
        let root = self.marked[i].path.clone();
        let id = self.marked[i].id.clone();
        let name = self.name(i);
        let check = match replica::check_sealed_copy(&root, path, text) {
            Ok(check) => check,
            Err(error) => {
                self.problem(format!("{path} on {name}: cannot be checked: {error}"));
                return Ok(());
            }
        };
        match check {
            CopyCheck::Good => {
                if !self.marker_present(i) {
                    return Ok(());
                }
                self.append(Kind::Copy {
                    path: path.to_owned(),
                    root: id.clone(),
                })?;
                self.line(format!(
                    "{path} on {name}: re-verified good, recorded again; no byte written"
                ));
                return Ok(());
            }
            CopyCheck::Manifest { other_file: true } => {
                self.problem(format!(
                    "{path} on {name}: refused: it holds a self-consistent other file under the \
                     same path (its own manifest differs from the copy ledger's and its data \
                     matches it), two sealed files under one path, never a damaged copy; it is \
                     not moved (ADR-062 D5)"
                ));
                return Ok(());
            }
            _ => {}
        }
        let sources = self.source(path, text, &root);
        let Some(source) = self.good_source(path, text, sources) else {
            return Ok(());
        };
        if !self.marker_present(i) {
            return Ok(());
        }
        // R5: whatever the root holds of the path (bad data, a manifest
        // without its data, seal debris) is moved aside first.
        let aside = replica::damaged_dir(&root, self.run_ms);
        match replica::move_aside(&root, path, &aside) {
            Ok(moved) if !moved.is_empty() => {
                self.line(format!(
                    "{path} on {name}: moved {} aside into {}",
                    moved.join(", "),
                    aside.display()
                ));
                reached(RepairStep::AfterMoveAside)?;
            }
            Ok(_) => {}
            Err(error) => {
                self.problem(format!(
                    "{path} on {name}: moving the damaged copy aside failed: {error}"
                ));
                return Ok(());
            }
        }
        let published = match replica::copy_sealed(&source, &root, path) {
            Ok((_, published)) => published,
            Err(CopyError { side, error }) => {
                let side = match side {
                    Side::Source => format!("the source {}", source.display()),
                    Side::Destination => name.clone(),
                };
                self.problem(format!(
                    "{path} on {name}: the copy from {} failed on {side}: {error}",
                    source.display()
                ));
                return Ok(());
            }
        };
        reached(RepairStep::AfterPublish)?;
        if published != text {
            self.problem(format!(
                "{path} on {name}: the published manifest differs from the copy ledger's; nothing \
                 is recorded (ADR-062 D5)"
            ));
            return Ok(());
        }
        if !self.marker_present(i) {
            return Ok(());
        }
        self.append(Kind::Copy {
            path: path.to_owned(),
            root: id,
        })?;
        let what = if refill { "refilled" } else { "repaired" };
        self.line(format!(
            "{path} on {name}: {what} from {} and recorded",
            source.display()
        ));
        Ok(())
    }

    /// R9 for `path`, which has no good source; `unread` names the roots
    /// whose copy could not be read.
    fn lost(&mut self, path: &str, text: &str, unread: &[String]) {
        if !self.judged.insert(path.to_owned()) {
            return;
        }
        let presented: BTreeSet<&str> = self.marked.iter().map(|root| root.id.as_str()).collect();
        let elsewhere: Vec<String> = self
            .state
            .copies_of(path)
            .filter(|(_, copy)| copy.latest.is_some())
            .map(|(id, _)| id)
            .filter(|id| !presented.contains(id))
            .map(str::to_owned)
            .collect();
        if !elsewhere.is_empty() {
            self.problem(format!(
                "{path}: not repairable in this run: no presented root holds a good copy, and the \
                 copy ledger records copies on {}, which did not present a marker or are no \
                 longer configured; it is not lost (ADR-062 D11)",
                elsewhere.join(", ")
            ));
            return;
        }
        // Every root that records a copy must present its marker now, not
        // only when the run started: a target unmounted mid-run leaves an
        // empty mount point, where every copy checks as missing (review of
        // #203, finding 2).
        let recorded: BTreeSet<&str> = self
            .state
            .copies_of(path)
            .filter(|(_, copy)| copy.latest.is_some())
            .map(|(id, _)| id)
            .collect();
        let mut doubts: Vec<String> = unread.to_vec();
        for root in self
            .marked
            .iter()
            .filter(|root| recorded.contains(root.id.as_str()))
        {
            match root_marker::read(&root.path) {
                Ok(Some(id)) if id == root.id => {}
                _ => doubts.push(format!(
                    "{} ({}) no longer presents its marker (an unmounted mount point?)",
                    root.path.display(),
                    root.id
                )),
            }
        }
        if !doubts.is_empty() {
            self.problem(format!(
                "{path}: not repairable in this run: no root yielded a good copy, but {}; it is \
                 not lost, and nothing is recorded. Run the repair again once every root reads \
                 (ADR-062 D11)",
                doubts.join(", ")
            ));
            return;
        }
        let sha = replica::parse_manifest(text).ok().map(|file| file.sha256);
        let recorded = if self.record_lost {
            let kind = Kind::Lost {
                path: path.to_owned(),
                origin: LostOrigin::Verify,
                sha256: sha,
            };
            match self.append(kind) {
                Ok(()) => {
                    " The lost record is appended (--record-lost): every selection that \
                           covers it fails from now on."
                }
                Err(_) => " The lost record could not be appended.",
            }
        } else {
            " Nothing is recorded; after checking for a shared cause, `mie store repair \
             --record-lost` records it."
        };
        self.problem(format!(
            "{path} is lost: no root holds a good copy (hot, then every cold root tried, each read \
             without error), and every root the copy ledger records a copy on presented its \
             marker, again at this check. Nothing is re-created and nothing is written to any \
             root (ADR-062 D11, the lost-file rule).{recorded}"
        ));
    }

    /// R4: hot's copies.
    fn repair_hot(&mut self) -> Result<(), Stop> {
        let listing = match replica::list_sealed(self.hot) {
            Ok(listing) => listing,
            Err(error) => {
                self.problem(format!("cannot list hot's sealed files: {error}"));
                return Ok(());
            }
        };
        let listed: BTreeSet<&str> = listing.iter().map(String::as_str).collect();
        let mut work: Vec<(String, String)> = Vec::new();
        for path in &listing {
            let Some(text) = self.state.ledger_text(path) else {
                continue;
            };
            if !matches!(
                self.state.verdict(path),
                Some(PathVerdict::Ordinary | PathVerdict::Held)
            ) {
                continue;
            }
            match replica::check_sealed_copy(self.hot, path, text) {
                Ok(CopyCheck::Good) => {}
                Ok(CopyCheck::Missing) if self.removed.contains(partition_of(path)) => {}
                Ok(CopyCheck::Manifest { other_file: true }) => self.problem(format!(
                    "{path} on hot: refused: hot holds a self-consistent other file under the \
                     same path, two sealed files under one path; it is not moved (ADR-062 D5)"
                )),
                Ok(_) => work.push((path.clone(), text.to_owned())),
                Err(error) => self.problem(format!("{path} on hot: cannot be checked: {error}")),
            }
        }
        let mut unread = Vec::new();
        for (path, verdict) in self.state.paths() {
            if verdict == PathVerdict::Ordinary
                && !listed.contains(path)
                && !self.removed.contains(partition_of(path))
                && let Some(text) = self.state.ledger_text(path)
            {
                match replica::sealed_manifest(self.hot, path) {
                    Ok(Some(_)) => {}
                    Ok(None) => work.push((path.to_owned(), text.to_owned())),
                    // A read error is no absence: never R9 over it.
                    Err(error) => unread.push(format!("{path} on hot: cannot be checked: {error}")),
                }
            }
        }
        for problem in unread {
            self.problem(problem);
        }
        work.sort();
        for (path, text) in work {
            self.repair_hot_copy(&path, &text)?;
        }
        Ok(())
    }

    /// R4 for one hot copy.
    fn repair_hot_copy(&mut self, path: &str, text: &str) -> Result<(), Stop> {
        let sources = self.source(path, text, self.hot);
        let Some(source) = self.good_source(path, text, sources) else {
            return Ok(());
        };
        let lock = match removal::try_source_lock(self.hot, source_of(path)) {
            Ok(lock) => lock,
            Err(RawStoreError::Locked(_)) => {
                self.problem(format!(
                    "{path} on hot: not repaired: a writer holds source {} (`mie ingest` for the \
                     live source, an import for the archive); stop it briefly and run the repair \
                     again (ADR-062 D11)",
                    source_of(path)
                ));
                return Ok(());
            }
            Err(error) => {
                self.problem(format!("{path} on hot: the writer lock: {error}"));
                return Ok(());
            }
        };
        // The hot removal records `removed` and deletes under this lock,
        // without the pass lock, so a partition the run's ledger read saw
        // on hot may have left it since: re-read under the lock (review of
        // #203, finding 4).
        match copy_ledger::read(self.hot) {
            Ok(Some(ledger)) => {
                if adopt::ledger_states(&ledger.records)
                    .0
                    .contains(partition_of(path))
                {
                    self.line(format!(
                        "{path} on hot: skipped: its partition was recorded removed during this \
                         run, so nothing is re-created on hot (ADR-062 D6, D11)"
                    ));
                    return Ok(());
                }
            }
            Ok(None) => {
                self.problem(format!(
                    "{path} on hot: not repaired: hot's copy ledger is gone"
                ));
                return Ok(());
            }
            Err(error) => {
                self.problem(format!(
                    "{path} on hot: not repaired: hot's copy ledger cannot be re-read: {error}"
                ));
                return Ok(());
            }
        }
        let aside = replica::damaged_dir(self.hot, self.run_ms);
        match replica::move_aside(self.hot, path, &aside) {
            Ok(moved) if !moved.is_empty() => {
                self.line(format!(
                    "{path} on hot: moved {} aside into {}",
                    moved.join(", "),
                    aside.display()
                ));
                reached(RepairStep::AfterMoveAside)?;
            }
            Ok(_) => {}
            Err(error) => {
                self.problem(format!(
                    "{path} on hot: moving the damaged copy aside failed: {error}"
                ));
                return Ok(());
            }
        }
        match replica::copy_back(
            std::slice::from_ref(&source),
            self.hot,
            path,
            text,
            &mut |_| {},
        ) {
            Ok(_) => self.line(format!(
                "{path} on hot: repaired from {} (hot copies get no record)",
                source.display()
            )),
            Err(error) => self.problem(format!(
                "{path} on hot: the copy from {} failed: {error}",
                source.display()
            )),
        }
        drop(lock);
        reached(RepairStep::AfterPublish)
    }

    /// R6 for the ledger copy of the marked target `i`.
    fn repair_ledger_copy(&mut self, i: usize) -> Result<(), Stop> {
        let root = self.marked[i].path.clone();
        let id = self.marked[i].id.clone();
        let name = self.name(i);
        let copy = copy_ledger::ledger_path(&root);
        let check = match copy_ledger::check_copy(self.hot, &root) {
            Ok(check) => check,
            Err(error) => {
                self.problem(format!(
                    "the copy ledger on {name}: cannot be checked: {error}"
                ));
                return Ok(());
            }
        };
        match check {
            LedgerCopyCheck::Good | LedgerCopyCheck::Absent => {
                self.cleared.insert(id);
                self.line(format!(
                    "the copy ledger on {name}: {}; the closing pass extends it and records it",
                    if check == LedgerCopyCheck::Good {
                        "re-verified good"
                    } else {
                        "absent"
                    }
                ));
                return Ok(());
            }
            LedgerCopyCheck::Diverged { .. } | LedgerCopyCheck::Longer { .. } => {
                self.problem(format!(
                    "the copy ledger on {name} ({}): refused: its hash chain verifies but it \
                     differs from hot's or is longer than hot's verified prefix, so both chains are \
                     valid and one holds records the other lacks (a damaged last record on hot \
                     that an append truncated, say); overwriting it could destroy the last copy of \
                     a record. `mie store rebuild --replace-hot-ledger` decides (ADR-062 D7, D11)",
                    copy.display()
                ));
                return Ok(());
            }
            LedgerCopyCheck::Chain { .. } | LedgerCopyCheck::Tail { .. } => {}
        }
        let read = |path: &Path| fs::read(path).map_err(|e| format!("{}: {e}", path.display()));
        let (mine, hot_bytes) = match (read(&copy), read(&copy_ledger::ledger_path(self.hot))) {
            (Ok(mine), Ok(hot_bytes)) => (mine, hot_bytes),
            (Err(error), _) | (_, Err(error)) => {
                self.problem(format!("the copy ledger on {name}: {error}"));
                return Ok(());
            }
        };
        let verified = match copy_ledger::parse(&copy_ledger::ledger_path(self.hot), &hot_bytes) {
            Ok(ledger) => hot_bytes.len() as u64 - ledger.ignored_bytes,
            Err(error) => {
                self.problem(format!("hot's copy ledger: {error}"));
                return Ok(());
            }
        };
        let (upto, why) = match check {
            // Every complete record equals hot's; only the unterminated
            // tail differs, and a tail is no record (review of #203).
            LedgerCopyCheck::Tail { offset } => (
                offset.min(verified),
                format!(
                    "its unterminated tail from byte {offset} differs from hot's (a crashed \
                     extension), every complete record equals hot's"
                ),
            ),
            LedgerCopyCheck::Chain { record, offset } => {
                let first = mine
                    .iter()
                    .zip(&hot_bytes)
                    .position(|(a, b)| a != b)
                    .map_or(mine.len().min(hot_bytes.len()) as u64, |at| at as u64);
                if offset > first {
                    self.problem(format!(
                        "the copy ledger on {name} ({}): refused: its chain fails at record \
                         {record} (byte {offset}), after byte {first} where it first differs from \
                         hot's, so a record that verifies differs from hot's; `mie store rebuild \
                         --replace-hot-ledger` decides (ADR-062 D7)",
                        copy.display()
                    ));
                    return Ok(());
                }
                (
                    first.min(verified),
                    format!("its chain failed at record {record} (byte {offset})"),
                )
            }
            _ => return Ok(()),
        };
        let aside = aside_of(&root, self.run_ms, Path::new(".mie/copy-ledger"));
        match replica::rebuild_prefix(TailSource::Bytes(&hot_bytes), &copy, upto, &aside) {
            Ok(()) => {
                self.cleared.insert(id);
                self.line(format!(
                    "the copy ledger on {name}: {why}; moved aside to {} and rebuilt from hot's \
                     first {upto} byte(s), the closing pass extends it",
                    aside.display()
                ));
                reached(RepairStep::AfterTailRebuild)
            }
            Err(error) => {
                self.problem(format!(
                    "the copy ledger on {name}: the rebuild failed: {error}"
                ));
                Ok(())
            }
        }
    }

    /// The journal copy of root `i`.
    fn journal_copy(&self, i: usize) -> Option<PathBuf> {
        self.config.paths.journal.file_name().map(|name| {
            self.marked[i]
                .path
                .join(META_DIR)
                .join(JOURNAL_DIR)
                .join(name)
        })
    }

    /// The first byte where the copy `copy` differs from hot's journal,
    /// `None` when it is a prefix of it.
    fn journal_difference(&self, copy: &Path) -> Result<Option<u64>, String> {
        let journal = self.config.paths.journal.as_path();
        let copy_len = fs::metadata(copy)
            .map_err(|e| format!("{}: {e}", copy.display()))?
            .len();
        let hot_len = fs::metadata(journal).map_or(0, |metadata| metadata.len());
        if copy_len > hot_len {
            return Ok(Some(hot_len));
        }
        replica::first_difference(copy, TailSource::File(journal), copy_len)
            .map_err(|e| e.to_string())
    }

    /// R7 for the journal copy of the marked target `i`.
    fn repair_journal_copy(&mut self, i: usize, targets: &[usize]) -> Result<(), Stop> {
        let name = self.name(i);
        let Some(copy) = self.journal_copy(i) else {
            return Ok(());
        };
        if !copy.exists() {
            self.line(format!(
                "the journal on {name}: absent; the closing pass copies it"
            ));
            return Ok(());
        }
        let first = match self.journal_difference(&copy) {
            Ok(None) => {
                self.line(format!(
                    "the journal on {name}: re-verified, a byte prefix of hot's"
                ));
                return Ok(());
            }
            Ok(Some(first)) => first,
            Err(error) => {
                self.problem(format!("the journal on {name}: {error}"));
                return Ok(());
            }
        };
        let journal = self.config.paths.journal.clone();
        // Another target's copy longer than d that equals hot over [0, d].
        let mut agree = false;
        let mut others = Vec::new();
        for &j in targets.iter().filter(|&&j| j != i) {
            let Some(other) = self.journal_copy(j) else {
                continue;
            };
            let long = fs::metadata(&other).is_ok_and(|metadata| metadata.len() > first);
            if !long {
                continue;
            }
            match replica::first_difference(&other, TailSource::File(&journal), first + 1) {
                Ok(None) => agree = true,
                Ok(Some(_)) => {}
                Err(_) => continue,
            }
            others.push(other);
        }
        if !agree {
            let mine = byte_at(&copy, first).ok().flatten();
            let theirs = byte_at(&journal, first).ok().flatten();
            let suspect = !others.is_empty()
                && others
                    .iter()
                    .all(|other| byte_at(other, first).ok().flatten() == mine)
                && mine != theirs;
            let tail = if suspect {
                " Every other copy agrees with this one against hot, so hot's journal is the \
                 suspect: the hot restore (#195) restores it from the copies, never this repair"
            } else {
                " No other copy agrees with hot over [0, d], and the journal has no hash chain, \
                 so the difference cannot be attributed"
            };
            self.problem(format!(
                "the journal on {name} ({}): refused: it differs from hot's journal at byte \
                 {first}.{tail} (ADR-062 D11)",
                copy.display()
            ));
            return Ok(());
        }
        let upto = match replica::line_start(TailSource::File(&journal), first) {
            Ok(upto) => upto,
            Err(error) => {
                self.problem(format!("hot's journal: {error}"));
                return Ok(());
            }
        };
        let relative = PathBuf::from(META_DIR)
            .join(JOURNAL_DIR)
            .join(copy.file_name().unwrap_or_default());
        let aside = aside_of(&self.marked[i].path, self.run_ms, &relative);
        match replica::rebuild_prefix(TailSource::File(&journal), &copy, upto, &aside) {
            Ok(()) => {
                self.line(format!(
                    "the journal on {name}: it differed from hot's at byte {first}, where another \
                     copy agrees with hot; moved aside to {} and rebuilt up to byte {upto}, the \
                     start of that line; the closing pass extends it",
                    aside.display()
                ));
                reached(RepairStep::AfterTailRebuild)
            }
            Err(error) => {
                self.problem(format!(
                    "the journal on {name}: the rebuild failed: {error}"
                ));
                Ok(())
            }
        }
    }

    /// R8 for the copy of `entry` on the marked target `i`.
    fn repair_entry_copy(&mut self, i: usize, entry: &str) -> Result<(), Stop> {
        let name = self.name(i);
        let Some((segment, file_name)) = entry.split_once('/') else {
            return Ok(());
        };
        let dest = self.marked[i]
            .path
            .join(META_DIR)
            .join(IMPORT_LEDGER_DIR)
            .join(segment)
            .join(file_name);
        let Some(hot_entry) = self.entries.iter().find(|e| e.name == entry) else {
            let why = if self.archive.is_none() {
                "no archive configuration is given, so hot's entry is not read"
            } else {
                "hot holds no readable entry of that name"
            };
            self.problem(format!(
                "import entry {entry} on {name}: refused: {why} (ADR-062 D11)"
            ));
            return Ok(());
        };
        let (hot_path, files) = (hot_entry.path.clone(), hot_entry.files.clone());
        let hot_bytes = match fs::read(&hot_path) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.problem(format!("import entry {entry} on hot: {error}"));
                return Ok(());
            }
        };
        match fs::read(&dest) {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                self.line(format!(
                    "import entry {entry} on {name}: absent; the closing pass copies it"
                ));
                return Ok(());
            }
            Err(error) => {
                self.problem(format!("import entry {entry} on {name}: {error}"));
                return Ok(());
            }
            Ok(bytes) if bytes == hot_bytes => {
                self.line(format!(
                    "import entry {entry} on {name}: re-verified, byte-identical to hot's"
                ));
                return Ok(());
            }
            Ok(_) => {}
        }
        let mut wrong = Vec::new();
        for (file, sha) in &files {
            let expected = match self.state.ledger_text(file) {
                Some(text) => replica::parse_manifest(text).ok().map(|f| f.sha256),
                None => replica::sealed_manifest(self.hot, file)
                    .ok()
                    .flatten()
                    .map(|(_, f)| f.sha256),
            };
            if expected.as_deref() != Some(sha.as_str()) {
                wrong.push(file.clone());
            }
        }
        if !wrong.is_empty() {
            self.problem(format!(
                "import entry {entry} on {name}: refused: hot's entry lists {} with another \
                 SHA-256 than the copy ledger's manifest record (else hot's manifest), so hot's \
                 entry is no good source: an entry conflict, for the hold resolution (#194; \
                 ADR-062 D11)",
                wrong.join(", ")
            ));
            return Ok(());
        }
        if !self.marker_present(i) {
            return Ok(());
        }
        let relative = PathBuf::from(META_DIR)
            .join(IMPORT_LEDGER_DIR)
            .join(segment)
            .join(file_name);
        let aside = aside_of(&self.marked[i].path, self.run_ms, &relative);
        if let Err(error) = replica::move_file_aside(&dest, &aside) {
            self.problem(format!(
                "import entry {entry} on {name}: moving it aside failed: {error}"
            ));
            return Ok(());
        }
        reached(RepairStep::AfterMoveAside)?;
        match replica::copy_immutable(&hot_path, &dest) {
            Ok(_) => self.line(format!(
                "import entry {entry} on {name}: moved aside to {} and replaced from hot",
                aside.display()
            )),
            Err(error) => self.problem(format!(
                "import entry {entry} on {name}: the copy from hot failed: {error}"
            )),
        }
        Ok(())
    }
}
