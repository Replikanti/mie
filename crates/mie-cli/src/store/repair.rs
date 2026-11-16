//! The repair (ADR-062 D11 *On a finding*, #192): `mie store repair`.
//!
//! An explicit step, never run by the verification: several bad copies at
//! once point at a shared cause, such as a controller that returns bad
//! bytes, so an operator looks before anything is rewritten. Its work list
//! is what the copy ledger records (the findings `mie store verify` left
//! open) plus the copies a configured target lacks without any record (a
//! target added or replaced after files left hot, the comment of
//! 2026-11-07 on #192) and the forms a target holds without a record of
//! files hot holds (#219). Every work item is re-checked first, so a copy
//! that came back (a remount) is only re-recorded. The rules, as numbered
//! in the plan of #192 plus R2b (#219; the tests cite them):
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
//!   that lack a target are left to the closing pass, after R2b.
//! - **R2b Unrecorded copies of hot's files** (#219): for every path hot
//!   holds sealed whose partition is not recorded removed, whose verdict is
//!   none, ordinary, without `manifest` record or held, and every marked
//!   copy target without a `copy` record of it that holds its data file or
//!   manifest: that form is judged against the text (the ledger's
//!   `manifest` record, else hot's manifest, as V3 reads it) by the test
//!   the closing pass's D5 copy from hot applies, the restore's H6 k1
//!   (`restore::unrecorded_copy`). A copy the pass accepts stays, and the
//!   closing pass records it or publishes its manifest; a self-consistent
//!   other file is refused and never moved, as in R1 (D5; #227). Every
//!   other form, an unreadable one included (a `FAIL:`, #219 STOP 1
//!   decision 2), is moved aside (R5), and the closing pass copies the
//!   path from hot and records it; R2b itself writes no record and copies
//!   nothing. It moves only when hot's copy checks good against the text,
//!   and an archive part only when a current import entry on hot lists it
//!   with the text's SHA-256, since the closing pass copies nothing else;
//!   otherwise the form is named and stays. A temp name of the path that
//!   the pass's copy cannot remove (a directory: that copy removes both
//!   temp names by unlink before it copies, publishes or records, so every
//!   pass would stop the target on it) beside an absent form or a copy the
//!   pass accepts is moved aside alone, with a `FAIL:` (#219 review); the
//!   accepted copy stays in place. That move keeps the marker and archive
//!   gates but not hot's: a temp name is no sealed file and no copy, and
//!   the pass meets it whatever hot's data is (#219 review, round 2). For
//!   the same reason a lost path's temp names the pass cannot remove move
//!   alone, while its form is never judged, and so do those of a path hot
//!   still holds in a partition recorded removed (a hot removal that
//!   crashed between its `removed` record and its delete; the closing pass
//!   copies such a path to a target without its `copy` record, and a
//!   target it stops keeps the removal from completing), whose form is the
//!   removal's and never judged (#219 review, round 3). The report of such
//!   a move says whether the pass reaches the name now: not while its D5
//!   guard copies nothing of the path, and for an unjudged form only once
//!   its D5 copy accepts that form. A form the pass refuses moves with its
//!   temp names.
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
//!   byte where the copy differs from hot (L_h when it is longer), u the
//!   start of hot's line that holds d. The rebuild moves the copy aside and
//!   writes hot's bytes up to u, and the closing pass extends it from hot
//!   to L_h under its usual journal hold, so the repair writes hot's bytes
//!   into the copy from u on. It is done only when every byte p ≥ u where
//!   a journal that holds a byte at p contradicts hot (or holds one past
//!   hot's end) is settled for hot ([`journal_contest`], ADR-066 D5): p
//!   below L_h; hot's line that holds p not Failed under its check
//!   (ADR-066 D2), even where hot holds a majority; no contradicting
//!   journal's line Intact (both lines verifying is a fork, and a line that
//!   verifies is never overwritten by bytes that do not); then hot's line
//!   Intact with every contradicting line Failed settles the line with no
//!   count, and any other line, where a check is missing, is ADR-065 D8's
//!   strict majority unchanged: more than half of the journals that hold a
//!   byte at p, hot included, hold hot's byte. So a journal without
//!   checked lines gets exactly D8. The journals are hot, this copy and
//!   every other target's copy, each as it was before this run (a copy
//!   rebuilt earlier in the run counts by its evidence copy); one that
//!   cannot be read refuses. Otherwise refused, naming the byte and why:
//!   hot's line fails its check (hot's journal is the suspect); a copy's
//!   line verifies; or, without checks, one copy against hot (one against
//!   one) cannot be attributed, and when every other copy agrees with this
//!   one against hot at d, hot's journal is named as the suspect. The
//!   repair never rewrites hot, and `mie store restore` is for a lost hot
//!   only: with `mie ingest` stopped, `mie store repair-hot --scope
//!   journal` decides hot's journal (ADR-065, ADR-066 D4).
//! - **R8 Entry copies with an open entry finding:** replaced from hot only
//!   when hot's entry lists every file with the SHA-256 of the ledger's
//!   `manifest` record (else hot's manifest, [`entry_mismatches`], the test
//!   `mie store repair-hot` shares); otherwise refused, naming `mie store
//!   repair-hot --scope import-ledger`, which restores hot's entry when a
//!   copy target holds one that passes the test, then the hold resolution
//!   (`mie store resolve`, ADR-062 D11).
//!   An absent copy is left to the closing pass. A supersede record's copy
//!   is replaced from hot when hot's record parses and names its own file
//!   (it lists no part); a finding on a superseded entry is one line and no
//!   problem, and the next verification closes it (#210 C5).
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
use super::restore::{self, UnrecordedCopy};
use super::{DeviceProbe, check_devices, check_hot, truncation_line};
use crate::config::{ArchiveConfig, IngestConfig};
use crate::journal::{LineCheck, MAX_CHECKED_LINE, check_line};
use mie_adapter_parquet::copy_ledger::{
    self, AppendError, Kind, LedgerCopyCheck, LostOrigin, Record, Truncation,
};
use mie_adapter_parquet::removal;
use mie_adapter_parquet::replica::{
    self, Contested, CopyCheck, CopyError, Side, TailSource, TempName,
};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::verification::{CopyState, PathVerdict};
use mie_ports::raw::RawStoreError;
use std::collections::{BTreeMap, BTreeSet};
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
    /// Every superseded entry, with the record that supersedes it (#210
    /// C5).
    superseded: BTreeMap<String, String>,
    /// The copy targets whose ledger copy R6 repaired (exempt from L2).
    cleared: BTreeSet<String>,
    /// The paths already judged by R9 in this run.
    judged: BTreeSet<String>,
    /// Per marked root whose journal copy R7 rebuilt in this run, where
    /// its former bytes lie: D8 counts every copy as it was before the run.
    journal_before: BTreeMap<usize, PathBuf>,
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
    let (entries, superseded) = match &archive_config {
        Some(archive) => {
            let found = hot_entries(&archive.paths.import_ledger);
            problems.extend(
                found
                    .problems
                    .into_iter()
                    .map(super::repair_hot::with_remedy),
            );
            (found.entries, found.superseded)
        }
        None => (Vec::new(), BTreeMap::new()),
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
        superseded,
        cleared: BTreeSet::new(),
        judged: BTreeSet::new(),
        journal_before: BTreeMap::new(),
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

/// Whether R2b judges the unrecorded copies of a path of this verdict:
/// none (no `manifest` record), ordinary, recorded without a `manifest`
/// record, or held (as R1, R4 and V4 take held paths). A lost path is
/// R9's, which writes nothing for it; a conflict is two texts (D5); a
/// replaced or pending one belongs to the replace resolution (#211).
fn r2b_takes(verdict: Option<PathVerdict>) -> bool {
    matches!(
        verdict,
        None | Some(PathVerdict::Ordinary | PathVerdict::NoManifest | PathVerdict::Held)
    )
}

/// R2b's judgment of the form a copy target holds of a path (#219).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Form {
    /// The restore's H6 k1 test (`restore::unrecorded_copy`), when the
    /// closing pass's copy can remove both temp names of the path.
    Judged(UnrecordedCopy),
    /// Absent (`accepted` false) or accepted, beside temp names the
    /// closing pass's copy cannot remove ([`stuck_temps`]).
    Stuck {
        stuck: Vec<(PathBuf, String)>,
        accepted: bool,
    },
}

/// R2b's judgment of the form `root` holds of `path` against `text`: the
/// restore's H6 k1 test, and for a form it leaves in place (absent or
/// accepted) the temp names the pass's copy cannot remove. A form the
/// pass refuses is moved by R5 with its temp names, and a self-consistent
/// other file is refused before the pass reaches them.
///
/// # Errors
///
/// The H6 k1 test's: a copy that cannot be read.
fn r2b_form(root: &Path, path: &str, text: &str) -> Result<Form, RawStoreError> {
    let judged = restore::unrecorded_copy(root, path, text)?;
    if matches!(judged, UnrecordedCopy::Absent | UnrecordedCopy::Accepted) {
        let stuck = stuck_temps(root, path);
        if !stuck.is_empty() {
            return Ok(Form::Stuck {
                stuck,
                accepted: judged == UnrecordedCopy::Accepted,
            });
        }
    }
    Ok(Form::Judged(judged))
}

/// The temp names of the part `path` on `root` that the closing pass's D5
/// copy cannot remove, each with what it is. That copy removes both temp
/// names, by unlink(2), before it copies, before it publishes the manifest
/// of verifying data and before it records a copy it accepts
/// (`replica::copy_sealed`); unlink refuses a directory (EISDIR), which
/// the copy meets as an I/O error that stops the target for the rest of
/// the pass, in every pass. A name that cannot be inspected counts too, as
/// in `restore::unrecorded_copy`, and its failed move names the error. Every
/// other entry there, a dangling symlink or an unreadable file included,
/// the copy unlinks as debris, unless the partition directory refuses the
/// unlink (no write permission, EACCES): no move can clear that, since a
/// rename needs the same permission, and the pass names the error itself.
fn stuck_temps(root: &Path, path: &str) -> Vec<(PathBuf, String)> {
    let mut stuck = Vec::new();
    for name in [TempName::Data, TempName::Manifest] {
        // Hot's listing holds layout paths only.
        let Ok(file) = replica::temp_path(root, path, name) else {
            continue;
        };
        match fs::symlink_metadata(&file) {
            Ok(metadata) if metadata.is_dir() => stuck.push((file, "a directory".to_owned())),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => stuck.push((file, format!("it cannot be inspected: {error}"))),
        }
    }
    stuck
}

/// What R2b found on a copy target, for the move and its report.
enum Found {
    /// A form the pass's D5 copy refuses, with why.
    Refused(String),
    /// A form that cannot be read, with the error.
    Unreadable(String),
    /// Temp names the pass's copy cannot remove, from [`stuck_temps`],
    /// and what is beside them.
    Stuck(Vec<(PathBuf, String)>, Beside),
}

/// What is beside the temp names R2b moves alone ([`Found::Stuck`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Beside {
    /// Neither the data nor the manifest of the path.
    Absent,
    /// A copy the pass accepts, which stays in place.
    Accepted,
    /// A form never judged: the path is recorded lost (R9's), and only its
    /// temp names move (#219 review, round 2).
    Lost,
    /// A form never judged: the path's partition is recorded removed while
    /// hot still holds it (a hot removal that crashed before its delete),
    /// so the form is the removal's, and only its temp names move (#219
    /// review, round 3).
    Removed,
}

/// Whether the closing pass reaches a temp name R2b moves alone, for its
/// report (#219 review, round 3). The move is the same in every case.
enum Reach {
    /// The pass's D5 copy of the path to the target meets the name first,
    /// in every pass.
    Every,
    /// The pass's D5 guard copies nothing of the path now, with why.
    Guarded(String),
    /// The form beside the name is not judged (a lost path or a removed
    /// partition), and the pass's D5 copy meets the name only once it
    /// accepts that form.
    Unjudged,
}

impl Reach {
    /// What the pass does with the name, as the report says it.
    fn says(&self) -> String {
        match self {
            Self::Every => "so every pass stops this target on it".to_owned(),
            Self::Guarded(why) => format!(
                "so a pass that copies the path to this target stops the target on it, but no \
                 pass copies it now: {why}"
            ),
            Self::Unjudged => "so a pass stops this target on it whenever its D5 copy accepts \
                               the form there, which is not judged"
                .to_owned(),
        }
    }
}

/// Whether `root` holds the data file or the manifest of `path`, without
/// judging either: a name that cannot be inspected counts as held.
fn form_present(root: &Path, path: &str) -> bool {
    let manifest = format!("{}.manifest", path.strip_suffix(".parquet").unwrap_or(path));
    [root.join(path), root.join(manifest)].iter().any(
        |file| !matches!(fs::symlink_metadata(file), Err(e) if e.kind() == ErrorKind::NotFound),
    )
}

/// How R2b's report of the moved temp names of a path whose form is
/// `beside` ends, with hot's copy `hot` checked against the text.
fn stuck_then(beside: Beside, hot: &Result<CopyCheck, RawStoreError>) -> String {
    let good = matches!(hot, Ok(CopyCheck::Good));
    let repairs = "R4 repairs hot when the copy ledger records its text and a cold root holds a \
                   good copy";
    match beside {
        Beside::Lost | Beside::Removed => {
            "nothing else of the path there is judged or moved".to_owned()
        }
        Beside::Accepted if good => {
            "the copy there stays in place, and the closing pass records it".to_owned()
        }
        Beside::Absent if good => {
            "the closing pass copies the path from hot and records it".to_owned()
        }
        Beside::Accepted => format!(
            "the copy there stays in place; hot's copy is not good ({}), and {repairs}",
            hot_check(hot)
        ),
        Beside::Absent => format!(
            "hot's copy is not good ({}), so the closing pass copies nothing of the path from \
             hot; {repairs}; then run the repair again",
            hot_check(hot)
        ),
    }
}

/// What a hot copy that does not check good is, for R2b's report.
fn hot_check(check: &Result<CopyCheck, RawStoreError>) -> String {
    match check {
        Ok(CopyCheck::Good) => "good".to_owned(),
        Ok(CopyCheck::Missing) => "missing".to_owned(),
        Ok(CopyCheck::Manifest { .. }) => "its manifest is not the text".to_owned(),
        Ok(CopyCheck::Size) => "size".to_owned(),
        Ok(CopyCheck::Sha256) => "sha256".to_owned(),
        Err(error) => format!("it cannot be read: {error}"),
    }
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
        // R2b.
        self.repair_unrecorded(&targets)?;
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

    /// R2b: the forms the marked copy targets `targets` hold of files hot
    /// holds, without a `copy` record (module docs).
    fn repair_unrecorded(&mut self, targets: &[usize]) -> Result<(), Stop> {
        if targets.is_empty() {
            return Ok(());
        }
        let listing = match replica::list_sealed(self.hot) {
            Ok(listing) => listing,
            Err(error) => {
                self.problem(format!(
                    "cannot list hot's sealed files, so no unrecorded copy on a copy target is \
                     checked: {error}"
                ));
                return Ok(());
            }
        };
        for path in &listing {
            let verdict = self.state.verdict(path);
            // A lost path's form is R9's, and a form in a partition
            // recorded removed is the hot removal's, but the closing pass
            // still copies such a path while hot holds it, and so still
            // meets its temp names (#219 review, rounds 2 and 3).
            let lost = verdict == Some(PathVerdict::Lost);
            if !(lost || r2b_takes(verdict)) {
                continue;
            }
            let alone = if self.removed.contains(partition_of(path)) {
                Some(Beside::Removed)
            } else if lost {
                Some(Beside::Lost)
            } else {
                None
            };
            let missing: Vec<usize> = targets
                .iter()
                .copied()
                .filter(|&i| !self.fold.has_copy(path, &self.marked[i].id))
                .collect();
            if missing.is_empty() {
                continue;
            }
            let (text, origin) = match self.state.ledger_text(path) {
                Some(text) => (text.to_owned(), "the copy ledger's"),
                None => match replica::sealed_manifest(self.hot, path) {
                    Ok(Some((text, _))) => (text, "hot's"),
                    // Gone or unreadable: the closing pass names it.
                    Ok(None) | Err(_) => continue,
                },
            };
            // Each candidate with what R2b found there.
            let mut candidates: Vec<(usize, Found)> = Vec::new();
            for i in missing {
                let name = self.name(i);
                if let Some(beside) = alone {
                    let stuck = stuck_temps(&self.marked[i].path, path);
                    if !stuck.is_empty() {
                        candidates.push((i, Found::Stuck(stuck, beside)));
                    }
                    continue;
                }
                match r2b_form(&self.marked[i].path, path, &text) {
                    Ok(Form::Judged(UnrecordedCopy::Absent | UnrecordedCopy::Accepted)) => {}
                    Ok(Form::Stuck { stuck, accepted }) => {
                        let beside = if accepted {
                            Beside::Accepted
                        } else {
                            Beside::Absent
                        };
                        candidates.push((i, Found::Stuck(stuck, beside)));
                    }
                    Ok(Form::Judged(UnrecordedCopy::OtherFile)) => self.problem(format!(
                        "{path} on {name}: refused: it holds no recorded copy but a \
                         self-consistent other file under the same path (its own manifest \
                         differs from {origin} and its data matches it), two sealed files under \
                         one path, never a damaged copy; it is not moved, and the path stays one \
                         copy short on this target until #227 decides it (ADR-062 D5)"
                    )),
                    Ok(Form::Judged(UnrecordedCopy::Refused(why))) => {
                        candidates.push((i, Found::Refused(why)));
                    }
                    Err(error) => candidates.push((i, Found::Unreadable(error.to_string()))),
                }
            }
            if candidates.is_empty() {
                continue;
            }
            let found = |judged: &Found| match judged {
                Found::Refused(why) => format!("its copy does not verify ({why})"),
                Found::Unreadable(error) => format!("its copy cannot be read ({error})"),
                Found::Stuck(stuck, beside) => {
                    let names = stuck
                        .iter()
                        .map(|(file, what)| {
                            format!(
                                "{}/{} ({what})",
                                partition_of(path),
                                file.file_name().unwrap_or_default().to_string_lossy()
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    match beside {
                        Beside::Lost => format!(
                            "the path is recorded lost, so its form there is not judged (R9's), \
                             but the pass's D5 copy cannot remove {names}"
                        ),
                        Beside::Removed => format!(
                            "its partition is recorded removed, so its form there is not judged \
                             (the hot removal's), but the pass's D5 copy cannot remove {names}"
                        ),
                        Beside::Accepted => format!(
                            "a copy the pass accepts, beside {names}, which the pass's D5 copy \
                             cannot remove"
                        ),
                        Beside::Absent => format!(
                            "neither its data nor its manifest, beside {names}, which the pass's \
                             D5 copy cannot remove"
                        ),
                    }
                }
            };
            if replicate::is_archive_part(path) && !self.listed(path, &text) {
                let why = if self.archive.is_none() {
                    "no archive configuration is given, so its import entry cannot be checked"
                } else {
                    "no current import entry on hot lists it with its manifest's SHA-256"
                };
                for (i, judged) in &candidates {
                    let name = self.name(*i);
                    self.problem(format!(
                        "{path} on {name}: no copy record, and {}; not moved: an archive part, \
                         and {why}, so the closing pass does not copy it; it is moved once a \
                         current import entry on hot lists it (ADR-062 D11)",
                        found(judged)
                    ));
                }
                continue;
            }
            // A form moves only once hot's copy checks good, since the
            // closing pass replaces it from hot only. A temp name the pass
            // cannot remove is no sealed file and no copy, and the pass
            // meets it whatever hot's data is, so it moves regardless
            // (#219 review, round 2).
            let hot = replica::check_sealed_copy(self.hot, path, &text);
            let good = matches!(hot, Ok(CopyCheck::Good));
            let guard = self.pass_guard(path);
            for (i, judged) in candidates {
                if good || matches!(judged, Found::Stuck(..)) {
                    self.move_unrecorded(
                        i,
                        path,
                        &found(&judged),
                        &judged,
                        &hot,
                        guard.as_deref(),
                    )?;
                    continue;
                }
                let name = self.name(i);
                self.problem(format!(
                    "{path} on {name}: no copy record, and {}; not moved: hot's copy is not good \
                     ({}), and the closing pass copies from hot only. R4 repairs hot when the \
                     copy ledger records its text and a cold root holds a good copy; then run \
                     the repair again (ADR-062 D11)",
                    found(&judged),
                    hot_check(&hot)
                ));
            }
        }
        Ok(())
    }

    /// Whether a current import entry on hot lists the archive part
    /// `path` with the SHA-256 of `text`: the closing pass's gate (D11).
    fn listed(&self, path: &str, text: &str) -> bool {
        let Ok(sha) = replica::parse_manifest(text).map(|file| file.sha256) else {
            return false;
        };
        self.entries.iter().any(|entry| {
            entry
                .files
                .iter()
                .any(|(file, listed)| file == path && *listed == sha)
        })
    }

    /// Why the closing pass's D5 guard copies nothing of `path` now, if it
    /// does, as `replicate`'s `copy_file` decides before any copy: two
    /// texts in the copy ledger, a hot manifest that is gone or cannot be
    /// read, or one that differs from the ledger's `manifest` record. For
    /// R2b's report only (#219 review, round 3).
    fn pass_guard(&self, path: &str) -> Option<String> {
        if self.fold.conflicts.contains(path) {
            return Some("the copy ledger holds two different manifest texts for it".to_owned());
        }
        match replica::sealed_manifest(self.hot, path) {
            Ok(Some((hot_text, _))) => match self.fold.manifests.get(path) {
                Some(known) if *known != hot_text => {
                    Some("hot's manifest differs from the copy ledger's manifest record".to_owned())
                }
                _ => None,
            },
            Ok(None) => Some("hot holds no manifest of it".to_owned()),
            Err(error) => Some(format!("its manifest on hot cannot be read: {error}")),
        }
    }

    /// R2b's move-aside (R5) of the unrecorded copy of `path` on the
    /// marked target `i`, which `found` describes and `judged` is, with
    /// hot's copy `hot` checked against the text; an unreadable one is a
    /// problem (#219 STOP 1 decision 2), and so are temp names the pass's
    /// copy cannot remove (#219 review), which move alone: a copy the pass
    /// accepts beside them stays in place, and so does the form of a lost
    /// path or of a partition recorded removed. `guard` is why the pass
    /// copies nothing of the path now ([`Self::pass_guard`]), for the
    /// report of moved temp names.
    fn move_unrecorded(
        &mut self,
        i: usize,
        path: &str,
        found: &str,
        judged: &Found,
        hot: &Result<CopyCheck, RawStoreError>,
        guard: Option<&str>,
    ) -> Result<(), Stop> {
        if !self.marker_present(i) {
            return Ok(());
        }
        let root = self.marked[i].path.clone();
        let name = self.name(i);
        let aside = replica::damaged_dir(&root, self.run_ms);
        if let Found::Stuck(stuck, beside) = judged {
            let reach = match guard {
                Some(why) => Reach::Guarded(why.to_owned()),
                None if matches!(beside, Beside::Lost | Beside::Removed)
                    && form_present(&root, path) =>
                {
                    Reach::Unjudged
                }
                None => Reach::Every,
            };
            return self.move_stuck(i, path, found, stuck, &reach, &stuck_then(*beside, hot));
        }
        let moved = match replica::move_aside(&root, path, &aside) {
            Ok(moved) => moved,
            Err(error) => {
                self.problem(format!(
                    "{path} on {name}: no copy record, and {found}; moving it aside failed: \
                     {error}; nothing is replaced (ADR-062 D11)"
                ));
                return Ok(());
            }
        };
        if moved.is_empty() {
            return Ok(());
        }
        let names = moved.join(", ");
        if matches!(judged, Found::Unreadable(_)) {
            self.problem(format!(
                "{path} on {name}: no copy record, and {found}, so it is no verifiable copy: \
                 {names} moved aside into {}; the closing pass copies it from hot and records \
                 it. {} (ADR-062 D11)",
                aside.display(),
                restore::READ_ERROR
            ));
        } else {
            self.line(format!(
                "{path} on {name}: no copy record, and {found}, which the pass's D5 copy \
                 refuses: {names} moved aside into {}; the closing pass copies it from hot and \
                 records it (ADR-062 D5, D11)",
                aside.display()
            ));
        }
        reached(RepairStep::AfterMoveAside)
    }

    /// R2b's move of the temp names `stuck` of `path` on the marked target
    /// `i` that the pass's copy cannot remove, each by itself into
    /// `.mie/damaged/<at_ms>/` under its own relative name, as the hot
    /// restore's H6 k2 moves a temp name; `found` describes them, `reach`
    /// says what the pass does with them, and `then` ends the report
    /// ([`stuck_then`]). Nothing beside them is moved. A `FAIL:`, as for
    /// an unreadable copy.
    fn move_stuck(
        &mut self,
        i: usize,
        path: &str,
        found: &str,
        stuck: &[(PathBuf, String)],
        reach: &Reach,
        then: &str,
    ) -> Result<(), Stop> {
        let root = self.marked[i].path.clone();
        let name = self.name(i);
        let aside = replica::damaged_dir(&root, self.run_ms);
        let mut moved = Vec::new();
        for (file, _) in stuck {
            let Ok(relative) = file.strip_prefix(&root) else {
                continue;
            };
            match replica::move_file_aside(file, &aside.join(relative)) {
                Ok(true) => moved.push(relative.display().to_string()),
                Ok(false) => {}
                Err(error) => {
                    self.problem(format!(
                        "{path} on {name}: no copy record, and {found}; moving {} aside \
                         failed: {error}; nothing is replaced (ADR-062 D11)",
                        relative.display()
                    ));
                    return Ok(());
                }
            }
        }
        if moved.is_empty() {
            return Ok(());
        }
        self.problem(format!(
            "{path} on {name}: no copy record, and {found}, {}: {} moved aside into {}; {then} \
             (ADR-062 D11)",
            reach.says(),
            moved.join(", "),
            aside.display()
        ));
        reached(RepairStep::AfterMoveAside)
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
            replica::OtherText::Stop,
            replica::SourceForms::Sealed,
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
        let hot_len = fs::metadata(&journal).map_or(0, |metadata| metadata.len());
        let upto = match replica::line_start(TailSource::File(&journal), first) {
            Ok(upto) => upto,
            Err(error) => {
                self.problem(format!("hot's journal: {error}"));
                return Ok(());
            }
        };
        // D5 (ADR-066) over D8 (ADR-065): the rebuild writes hot's bytes
        // into this copy from `upto` on (the closing pass extends it to
        // L_h), so every byte from there that a journal contradicts must be
        // settled for hot, by the lines' checks or by a strict majority:
        // hot, this copy and every other target's copy, each as it was
        // before this run.
        let others: Vec<PathBuf> = targets
            .iter()
            .filter(|&&j| j != i)
            .filter_map(|&j| {
                self.journal_before
                    .get(&j)
                    .cloned()
                    .or_else(|| self.journal_copy(j))
            })
            .collect();
        let mut witnesses: Vec<&Path> = vec![copy.as_path()];
        witnesses.extend(others.iter().map(PathBuf::as_path));
        let contest = match journal_contest(&journal, &witnesses, upto, u64::MAX) {
            Ok(contest) => contest,
            Err(error) => {
                self.problem(format!(
                    "the journal on {name} ({}): refused: it differs from hot's journal at byte \
                     {first}, and a journal the rule counts cannot be read ({error}), so hot's \
                     majority cannot be established (ADR-065 D8, ADR-066 D5)",
                    copy.display()
                ));
                return Ok(());
            }
        };
        if let Some(contest) = contest {
            let repair_hot = "The repair never rewrites hot: with `mie ingest` stopped, `mie \
                              store repair-hot --scope journal` decides hot's journal";
            let rebuild = format!(
                "The rebuild would write hot's bytes into it from byte {upto}, the start of that \
                 line"
            );
            let tail = match contest {
                Contest::HotLineFails { at, line_start } => format!(
                    " {rebuild}, and hot's line from byte {line_start}, which a copy contradicts \
                     at byte {at}, fails its check: hot's bytes there are not the bytes written, \
                     so hot's journal is the suspect, whatever a majority holds. {repair_hot}, \
                     and `mie store restore` is for a lost hot only (ADR-066 D5)"
                ),
                Contest::Fork { at, witness, hot } => {
                    let holder = if witness == 0 {
                        "this copy".to_owned()
                    } else {
                        format!("the journal copy {}", witnesses[witness].display())
                    };
                    let why = if hot == LineCheck::Intact {
                        "hot's line there verifies too: both sides hold lines as written, a fork, \
                         which no byte count decides"
                            .to_owned()
                    } else {
                        "hot's line there carries no check, and a line that verifies is never \
                         overwritten by bytes that do not"
                            .to_owned()
                    };
                    format!(
                        " {rebuild}, and the line of {holder} that holds byte {at} verifies its \
                         check, while {why}. {repair_hot} (ADR-066 D5)"
                    )
                }
                Contest::Majority(contested) => {
                    let at = contested.at;
                    let mine = byte_at(&copy, first).ok().flatten();
                    let theirs = byte_at(&journal, first).ok().flatten();
                    let held: Vec<u8> = others
                        .iter()
                        .filter_map(|other| byte_at(other, first).ok().flatten())
                        .collect();
                    let suspect = at == first
                        && !held.is_empty()
                        && held.iter().all(|&byte| Some(byte) == mine)
                        && mine != theirs;
                    if suspect {
                        format!(
                            " Every other copy agrees with this one against hot, so hot's \
                             journal is the suspect. {repair_hot}, and `mie store restore` is \
                             for a lost hot only (ADR-065)"
                        )
                    } else {
                        let why = if at >= hot_len {
                            format!(
                                "at byte {at}, past the end of hot's journal ({hot_len} bytes), \
                                 {} copies hold a byte that hot does not, and the rebuild would \
                                 drop or overwrite it",
                                contested.covering
                            )
                        } else if contested.covering == 2 {
                            format!(
                                "at byte {at} only hot and one copy hold a byte, and they \
                                 differ: one against one, which bytes without a line check \
                                 cannot attribute"
                            )
                        } else {
                            format!(
                                "at byte {at} hot's byte is held by {} of the {} journals that \
                                 hold a byte there, no strict majority",
                                contested.with_hot, contested.covering
                            )
                        };
                        format!(
                            " {rebuild}, so hot must hold a strict majority at every byte from \
                             there that a journal contradicts where no line check decides; \
                             {why}. {repair_hot} (ADR-065 D8, ADR-066 D5)"
                        )
                    }
                }
            };
            self.problem(format!(
                "the journal on {name} ({}): refused: it differs from hot's journal at byte \
                 {first}.{tail} (ADR-062 D11)",
                copy.display()
            ));
            return Ok(());
        }
        let relative = PathBuf::from(META_DIR)
            .join(JOURNAL_DIR)
            .join(copy.file_name().unwrap_or_default());
        let aside = aside_of(&self.marked[i].path, self.run_ms, &relative);
        match replica::rebuild_prefix(TailSource::File(&journal), &copy, upto, &aside) {
            Ok(()) => {
                self.journal_before.insert(i, aside.clone());
                self.line(format!(
                    "the journal on {name}: it differed from hot's at byte {first}, and every \
                     contradicted byte from byte {upto}, the start of that line, is settled for \
                     hot by the lines' checks or a strict majority (ADR-065 D8, ADR-066 D5); \
                     moved aside to {} and rebuilt up to byte {upto}; the closing pass extends \
                     it",
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
        if let Some(record) = self.superseded.get(entry) {
            self.line(format!(
                "import entry {entry} on {name}: superseded by {record}; not repaired; the next \
                 verification closes the finding"
            ));
            return Ok(());
        }
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
        let wrong = entry_mismatches(&files, &self.state, self.hot);
        if !wrong.is_empty() {
            self.problem(format!(
                "import entry {entry} on {name}: refused: hot's entry lists {} with another \
                 SHA-256 than the copy ledger's manifest record (else hot's manifest), so hot's \
                 entry is no good source. When a copy target holds an entry of that name that \
                 passes this test, `mie store repair-hot --scope import-ledger`, with the \
                 importer stopped, restores hot's entry from it (ADR-065); otherwise it is an \
                 entry conflict, for the hold resolution (`mie store resolve`; ADR-062 D11)",
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

/// R8's test, shared with `mie store repair-hot` (ADR-065 D5), so the two
/// commands never disagree about which bytes are the entry: the files of
/// `files` (`(path, sha256)`, as an entry lists them) whose SHA-256 is not
/// the one of the copy ledger's `manifest` record (`state`), else of hot's
/// sealed manifest. An entry is good when this is empty.
pub(super) fn entry_mismatches(
    files: &[(String, String)],
    state: &CopyState,
    hot: &Path,
) -> Vec<String> {
    let mut wrong = Vec::new();
    for (file, sha) in files {
        let expected = match state.ledger_text(file) {
            Some(text) => replica::parse_manifest(text).ok().map(|f| f.sha256),
            None => replica::sealed_manifest(hot, file)
                .ok()
                .flatten()
                .map(|(_, f)| f.sha256),
        };
        if expected.as_deref() != Some(sha.as_str()) {
            wrong.push(file.clone());
        }
    }
    wrong
}

/// What [`journal_contest`] stops at (ADR-066 D5): the first contradicted
/// byte at or after `from` that no line check settles for hot and ADR-065
/// D8's majority does not either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Contest {
    /// D8's strict majority fails at a byte where no check decides, or
    /// past hot's end.
    Majority(Contested),
    /// Hot's line from `line_start` fails its check and a witness
    /// contradicts it at byte `at`: hot's bytes there are not the bytes
    /// written, whatever a majority holds.
    HotLineFails {
        /// The first contradicted byte of the line.
        at: u64,
        /// Where hot's line starts.
        line_start: u64,
    },
    /// The line of witness `witness` that holds its first byte `at`
    /// against hot verifies its check: with `hot` [`LineCheck::Intact`]
    /// both sides verify, a fork; otherwise a verifying line would be
    /// overwritten by bytes that do not verify.
    Fork {
        /// The witness's first byte against hot's line.
        at: u64,
        /// The witness's index.
        witness: usize,
        /// The verdict of hot's line there.
        hot: LineCheck,
    },
}

/// One line of a journal file as D5 and V4 judge it (ADR-066 D2): where it
/// starts, where it ends (after its `\n`, or where the read stopped), and
/// its verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HeldLine {
    /// Its first byte.
    pub start: u64,
    /// The byte after its `\n`; the read's end for a line without one.
    pub end: u64,
    /// Its verdict; [`LineCheck::Unchecked`] for a line without its `\n`
    /// (it may be in the middle of being written) and for one longer than
    /// [`MAX_CHECKED_LINE`].
    pub check: LineCheck,
}

/// The line of `path` that holds byte `offset` (`offset` < `len`), read no
/// further than `len`, in bounded chunks: at most [`MAX_CHECKED_LINE`]
/// bytes of it are held in memory, however long it is (ADR-066 D2).
///
/// # Errors
///
/// An I/O error, a file shorter than `len` included.
pub(super) fn held_line(path: &Path, offset: u64, len: u64) -> std::io::Result<HeldLine> {
    const STEP: u64 = 1 << 16;
    let mut file = fs::File::open(path)?;
    let mut read = |from: u64, n: u64| -> std::io::Result<Vec<u8>> {
        let mut chunk = vec![0; usize::try_from(n).unwrap_or(usize::MAX)];
        file.seek(SeekFrom::Start(from))?;
        file.read_exact(&mut chunk)?;
        Ok(chunk)
    };
    let mut start = 0;
    let mut back = offset;
    while back > 0 {
        let from = back.saturating_sub(STEP);
        if let Some(i) = read(from, back - from)?.iter().rposition(|&b| b == b'\n') {
            start = from + i as u64 + 1;
            break;
        }
        back = from;
    }
    let mut body = Vec::new();
    let mut kept = true;
    let mut terminated = false;
    let mut end = start;
    while end < len {
        let chunk = read(end, (len - end).min(STEP))?;
        let found = chunk.iter().position(|&b| b == b'\n');
        let part = &chunk[..found.unwrap_or(chunk.len())];
        if kept && body.len() + part.len() <= MAX_CHECKED_LINE {
            body.extend_from_slice(part);
        } else {
            kept = false;
            body = Vec::new();
        }
        end += part.len() as u64;
        if found.is_some() {
            end += 1;
            terminated = true;
            break;
        }
    }
    let check = if terminated && kept {
        check_line(&body)
    } else {
        LineCheck::Unchecked
    };
    Ok(HeldLine { start, end, check })
}

/// D5 (ADR-066), the rule by which `mie store repair` (R7) may write hot's
/// journal bytes into a journal copy, shared with `mie store repair-hot`'s
/// `r7_refuses` and its *behind* test so the two commands never disagree.
/// It walks the bytes p in `[from, until)` where some witness contradicts
/// hot ([`replica::first_contradiction_file_before`]); at each:
///
/// - p ≥ L_h: [`Contest::Majority`], as ADR-065 D8 (hot holds no byte);
/// - hot's line that holds p Failed: [`Contest::HotLineFails`], even when
///   hot holds a majority;
/// - a contradicting witness's line (the one that holds its first byte
///   against hot's line) Intact: [`Contest::Fork`];
/// - hot's line Intact and every contradicting witness's line Failed:
///   settled for hot, with no count;
/// - otherwise D8's majority over the rest of hot's line
///   ([`replica::first_contested_file_before`]), unchanged: every case
///   where a line carries no check, so a journal without checked lines
///   gets exactly D8.
///
/// Then the walk continues after hot's line. `None` when every
/// contradicted byte is settled. A witness that does not exist holds no
/// byte. The record rule of ADR-066 D8 (a failed hot line recorded lost)
/// is not part of it yet.
///
/// # Errors
///
/// A [`CopyError`] on [`Side::Source`] when hot's journal cannot be read,
/// on [`Side::Destination`] when a witness cannot: a witness that cannot
/// be read cannot be judged, so the rule cannot hold.
pub(super) fn journal_contest(
    hot: &Path,
    witnesses: &[&Path],
    from: u64,
    until: u64,
) -> Result<Option<Contest>, CopyError> {
    let io = |side: Side, path: &Path| {
        let path = path.display().to_string();
        move |error: std::io::Error| CopyError {
            side,
            error: RawStoreError::Io(format!("{path}: {error}")),
        }
    };
    let hot_len = fs::metadata(hot).map_err(io(Side::Source, hot))?.len();
    let mut lens = Vec::with_capacity(witnesses.len());
    for &path in witnesses {
        lens.push(match fs::metadata(path) {
            Ok(metadata) => Some(metadata.len()),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(io(Side::Destination, path)(error)),
        });
    }
    let mut next = from;
    while let Some(p) = replica::first_contradiction_file_before(hot, witnesses, next, until)? {
        if p >= hot_len {
            let covering = lens.iter().flatten().filter(|&&len| len > p).count();
            return Ok(Some(Contest::Majority(Contested {
                at: p,
                covering,
                with_hot: 0,
            })));
        }
        let line = held_line(hot, p, hot_len).map_err(io(Side::Source, hot))?;
        if line.check == LineCheck::Failed {
            return Ok(Some(Contest::HotLineFails {
                at: p,
                line_start: line.start,
            }));
        }
        let to = line.end.min(until);
        let mut theirs = Vec::new();
        for (w, (&path, len)) in witnesses.iter().zip(&lens).enumerate() {
            let Some(len) = *len else {
                continue;
            };
            let Some(q) =
                replica::first_difference_in(path, TailSource::File(hot), p, to.min(len))?
            else {
                continue;
            };
            let witness = held_line(path, q, len).map_err(io(Side::Destination, path))?;
            if witness.check == LineCheck::Intact {
                return Ok(Some(Contest::Fork {
                    at: q,
                    witness: w,
                    hot: line.check,
                }));
            }
            theirs.push(witness.check);
        }
        let settled =
            line.check == LineCheck::Intact && theirs.iter().all(|c| *c == LineCheck::Failed);
        if !settled
            && let Some(contested) = replica::first_contested_file_before(hot, witnesses, p, to)?
        {
            return Ok(Some(Contest::Majority(contested)));
        }
        next = line.end;
    }
    Ok(None)
}
