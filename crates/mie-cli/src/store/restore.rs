//! The hot restore after a hot-disk loss (ADR-062 D11, #195): `mie store
//! restore`.
//!
//! An explicit step that restores hot from the copy targets: the journal,
//! the import ledger, the copy ledger, every partition hot must hold, and
//! last hot's root marker. Until that marker exists, `mie ingest`, the
//! importer and every other `mie store` command refuse the hot
//! ([`check_hot`](super::check_hot)). It is built like the rebuild (#193):
//! refusals and whole-run locks first, then every read, then the pure
//! derivation ([`rebuild::derive`] in [`Mode::Restore`]), and only then the
//! writes, each idempotent, in a crash-safe order with the marker as the
//! commit point. Running the command again completes a restore that
//! crashed or stopped. The rules, as numbered in the plan of #195 (the
//! tests cite them):
//!
//! - **H0 Refusals**, nothing written: an empty `cold_roots`; a `raw_root`
//!   that does not exist (mie never creates it: a directory it created
//!   could put hot on the wrong filesystem); a `raw_root` with hot's marker
//!   (the restore is for a lost hot; damage on an intact hot is #217) or a
//!   cold root's; an archive configuration of another store; a configured
//!   root, copy target or read-only, without a readable marker (an
//!   unmounted root reads as empty, D7 step 2); a cold marker with hot's
//!   id, one id on two roots; hot and the copy targets not on pairwise
//!   distinct devices; a `paths.journal` without a file name; an
//!   import-ledger path that is a dangling link.
//! - **H1 Locks**, each without waiting, for the whole run: `.mie/` is
//!   created when absent (no marker), then the copy-ledger hold, the pass
//!   lock, the verify lock and the writer lock of every source in name
//!   order: `instrument.source`, the archive source with an archive
//!   configuration, and every `source=` at the top of any root's layout.
//!   The live source too: if hot's marker was deleted under a running `mie
//!   ingest`, the restore must not copy into partitions that writer
//!   appends to. Nothing but `.mie/` and the lock files is written before
//!   H7.
//! - **H2 Reads**, nothing written, in this order: every copy target's
//!   ledger copy and hot's surviving ledger, which choose the start (H5 a);
//!   the journal candidates; every import-ledger file on hot and on the
//!   targets; hot's layout (data not checked) and `.quarantine/`; each
//!   cold root's layout and `.quarantine/`. A copy's data is checked only
//!   on a copy target whose copy the start does not record (#195 STOP 1
//!   decision 4); everything copied back is verified by the copy. An I/O
//!   error on any read stops the run: a skipped directory would turn its
//!   files into lost candidates.
//! - **H3 The journal** ([`choose_journal`]): the longest candidate C;
//!   every other must be C's prefix of its own length, otherwise the run
//!   stops naming both and the first differing byte. A target
//!   `.mie/journal/` with files but none under the configured name stops
//!   the run (a renamed `paths.journal` would drop every pre-loss run).
//!   Hot's journal is created or extended to C, never rewritten; a C that
//!   ends inside a line gets one `\n`, so the next `run_start` parses.
//! - **H4 The import ledger** ([`plan_import_ledger`]): a malformed name is
//!   named and never written; byte-identical copies are written to hot
//!   when hot lacks the name; differing copies follow the entry resolution
//!   of [`archive_checks`](super::archive_checks), a surviving hot copy of
//!   another variant moved to `.mie/damaged/<at_ms>/import-ledger/` first;
//!   differing supersede records keep hot's when it parses, otherwise the
//!   first target copy that parses and binds. Hot's `PENDING` is left and
//!   named; the checks get none.
//! - **H5 The copy ledger**: [`rebuild::derive`] in [`Mode::Restore`]
//!   (its differences (a) to (h) are documented there), then the rebuild's
//!   install (B9). Lost candidates are recorded only with `--record-lost`;
//!   without it the run stops before any write (#195 STOP 1 decision 3).
//!   (h): a path cold roots hold only as debris (data without its
//!   manifest, a manifest that does not parse) that no start record names
//!   is a lost candidate, and an unrecorded sealed copy on any cold root
//!   gets its `manifest` record, so the writer's numbering counts every
//!   path a cold root holds (D5, D6). When hot's surviving ledger already
//!   equals the derived one (a re-run), only the replace is skipped: the
//!   markers are re-read and non-prefix target copies moved aside as in
//!   an uncrashed run.
//! - **H6 Copy-back** ([`plan_copy_back`]): every partition dated in the
//!   hot window or not recorded removed by the start; per partition, every
//!   path whose verdict in the installed ledger is ordinary, from the cold
//!   roots in configuration order with the installed text
//!   ([`replica::copy_back`], [`OtherText::PassOver`]). A damaged copy
//!   passed over, and a path with no verified copy left (the lost-file
//!   rule: it stays recorded, every read of it is `Integrity`), are `FAIL:`
//!   lines and do not stop the restore; an error on hot's side does. A
//!   window partition the start records removed gets `returned` once all
//!   its paths are back.
//! - **H7 Write order and commit**: H3, H4, the install, the copy-back and
//!   its `returned` records; every root's marker re-read; hot's marker
//!   last ([`root_marker::publish`]). Why last: a marker written first and
//!   a crash after it would let `mie ingest` start on a half-restored hot,
//!   numbering parts from hot alone and reusing paths cold holds (D5).
//! - **H8 Report**: the journal candidates and the choice, the
//!   import-ledger counts, the derivation's lines, the install and the
//!   copy-back per partition, then `restored: …` or `not restored: …`.
//!   `PASS` only with the marker written and no `FAIL:` line; a `FAIL:`
//!   line does not undo the commit.

use super::archive_checks::EntryCopy;
use super::rebuild::{
    self, HotRead, LedgerCopyRead, Marked, Mode, RebuildInputs, RebuildReport, RestoreMode,
    RootRead, Stop,
};
use super::replicate::{self, IMPORT_LEDGER_DIR, JOURNAL_DIR, META_DIR, sorted_names};
use super::{DeviceProbe, check_devices, check_hot};
use crate::config::IngestConfig;
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_binance::archive::ledger::{LedgerKind, LedgerName, PENDING_FILE, classify};
use mie_adapter_parquet::copy_ledger::{self, CopyLedger, Kind, Record};
use mie_adapter_parquet::removal;
use mie_adapter_parquet::replica::{self, CopyOutcome, OtherText, TailSource};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::verification::{CopyState, PathVerdict};
use mie_ports::raw::{RawStoreError, validate_segment};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod run_tests;

/// The write steps of H7, in order. Tests stop the restore after any of
/// them, as a crash would.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// Hot's journal is written (or was already C).
    Journal,
    /// The import ledger is written.
    ImportLedger,
    /// Hot's copy ledger is replaced (or was already the derived one); the
    /// targets' non-prefix copies are not moved aside yet.
    Replace,
    /// The copy ledger is installed.
    Install,
    /// The first file is copied back.
    FirstCopy,
    /// Every partition is copied back and every `returned` appended.
    Returned,
    /// Every marker is re-read; hot's is not written yet.
    BeforeMarker,
}

#[cfg(test)]
impl Step {
    pub(crate) const ALL: [Self; 7] = [
        Self::Journal,
        Self::ImportLedger,
        Self::Replace,
        Self::Install,
        Self::FirstCopy,
        Self::Returned,
        Self::BeforeMarker,
    ];
}

/// A test hook that runs once.
#[cfg(test)]
type Hook = std::cell::RefCell<Option<Box<dyn FnOnce()>>>;

#[cfg(test)]
thread_local! {
    /// Test hook: the restore stops after this step, once.
    pub(crate) static AFTER_STEP: std::cell::Cell<Option<Step>> =
        const { std::cell::Cell::new(None) };
    /// Test hook: runs once after the first file of a partition with more
    /// than one file is copied back (the AW3 race).
    pub(crate) static DURING_COPY_BACK: Hook = std::cell::RefCell::new(None);
}

/// Marks the end of a write step; under test, simulates a crash there once.
fn reached(step: Step) -> Result<(), Stop> {
    #[cfg(test)]
    if AFTER_STEP.with(std::cell::Cell::get) == Some(step) {
        AFTER_STEP.with(|crash| crash.set(None));
        return Err(Stop(format!("simulated crash after {step:?}")));
    }
    let _ = step;
    Ok(())
}

/// The flags of `mie store restore`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Flags {
    /// `--record-lost`: write the `lost` records (#195 STOP 1 decision 3).
    pub record_lost: bool,
}

/// What one restore did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    /// The report in H8 order, ending in `restored: …` or `not restored:
    /// …`.
    pub lines: Vec<String>,
    /// Every refusal, stop and finding; empty when the restore passed.
    pub problems: Vec<String>,
    /// Whether hot's marker was written: the restore committed.
    pub restored: bool,
}

// -------------------------------------------------------- pure functions

/// One journal candidate (H3): hot's surviving journal or a target's copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalCandidate {
    /// Its name in the report.
    pub label: String,
    /// Its length in bytes.
    pub len: u64,
}

/// H3: the index of C, the longest candidate (the first of the longest
/// ones, so hot's own when it is one), or `None` without a candidate.
/// `differs(i, j, len)` gives the first byte in `[0, len)` where candidate
/// `i` differs from candidate `j`, streamed by the caller.
///
/// # Errors
///
/// A [`Stop`] naming both candidates and the first differing byte when a
/// candidate is not C's prefix of its own length, equal lengths included;
/// or the read error.
pub fn choose_journal(
    candidates: &[JournalCandidate],
    differs: &mut dyn FnMut(usize, usize, u64) -> Result<Option<u64>, String>,
) -> Result<Option<usize>, Stop> {
    let Some(longest) = candidates.iter().map(|c| c.len).max() else {
        return Ok(None);
    };
    let chosen = candidates
        .iter()
        .position(|c| c.len == longest)
        .expect("the longest exists");
    for (i, other) in candidates.iter().enumerate() {
        if i == chosen {
            continue;
        }
        match differs(i, chosen, other.len).map_err(Stop)? {
            None => {}
            Some(byte) => {
                return Err(Stop(format!(
                    "the journal copies differ beyond their common prefix: {} ({} bytes) differs \
                     from {} ({} bytes) at byte {byte}. A copy is a byte prefix of hot's journal, \
                     so one of them is damaged, and nothing can tell which holds the live data; \
                     the restore stops with nothing written (ADR-062 D11 step 2, step 5)",
                    other.label, other.len, candidates[chosen].label, candidates[chosen].len
                )));
            }
        }
    }
    Ok(Some(chosen))
}

/// One copy of an import-ledger file as H2 read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerFileCopy {
    /// `hot`, or a copy target's id.
    pub root: String,
    /// Its path, the source of a write to hot.
    pub path: PathBuf,
    /// Its bytes.
    pub bytes: Vec<u8>,
}

/// What H4 writes to hot's import ledger and what it names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportPlan {
    /// The surviving hot copies of another variant, moved aside first:
    /// `<segment>/<name>`.
    pub set_aside: Vec<String>,
    /// The files to write to hot: `<segment>/<name>` and the copy whose
    /// bytes it gets.
    pub writes: Vec<(String, PathBuf)>,
    /// Report lines.
    pub lines: Vec<String>,
    /// `FAIL:` findings.
    pub problems: Vec<String>,
}

/// H4 over every well-formed import-ledger name (hot's copy first, then
/// the targets' in configuration order) and the variants the entry
/// resolution decided (`chosen`, [`rebuild::Derived::chosen_entries`]).
/// Pure.
pub fn plan_import_ledger(
    files: &BTreeMap<String, Vec<LedgerFileCopy>>,
    chosen: &BTreeMap<String, Vec<u8>>,
) -> ImportPlan {
    let mut plan = ImportPlan::default();
    for (name, copies) in files {
        let Some((segment, file_name)) = name.split_once('/') else {
            continue;
        };
        let Ok(parsed) = LedgerName::parse(file_name) else {
            continue;
        };
        let hot = copies.iter().find(|copy| copy.root == HOT_ID);
        let identical = copies.windows(2).all(|pair| pair[0].bytes == pair[1].bytes);
        let winner: Option<&LedgerFileCopy> = if identical {
            copies.first()
        } else {
            match parsed.kind {
                LedgerKind::Entry => match chosen.get(name) {
                    Some(bytes) => copies.iter().find(|copy| copy.bytes == *bytes),
                    None => {
                        // A current entry the resolution could not decide is
                        // held `entry-conflict` by the derivation; a
                        // superseded entry is no current entry at all.
                        plan.problems.push(format!(
                            "import-ledger file {name}: its copies differ and no variant is \
                             decided, so none is written to hot; `mie store verify`, then `mie \
                             store repair` or `mie store resolve` decides it (ADR-062 D11)"
                        ));
                        None
                    }
                },
                LedgerKind::Supersede => {
                    let binds = |copy: &&LedgerFileCopy| {
                        classify(segment, &[(parsed.clone(), copy.bytes.clone())]).is_ok()
                    };
                    let found = hot
                        .filter(binds)
                        .or_else(|| copies.iter().filter(|c| c.root != HOT_ID).find(binds));
                    if found.is_none() {
                        plan.problems.push(format!(
                            "supersede record {name}: no copy parses and names its own file, so \
                             none is written to hot (ADR-062 D11)"
                        ));
                    }
                    found
                }
            }
        };
        let Some(winner) = winner else {
            continue;
        };
        match hot {
            Some(hot) if hot.bytes == winner.bytes => {}
            Some(_) => {
                plan.set_aside.push(name.clone());
                plan.writes.push((name.clone(), winner.path.clone()));
                plan.lines.push(format!(
                    "import-ledger file {name}: hot's copy differs from the decided one on {}: \
                     moved aside, then replaced",
                    winner.root
                ));
            }
            None => plan.writes.push((name.clone(), winner.path.clone())),
        }
    }
    plan
}

/// The date of a partition directory.
fn date_of(partition: &str) -> &str {
    partition.rsplit_once("/date=").map_or("", |(_, date)| date)
}

/// The partition directory of a data path.
fn partition_of(path: &str) -> &str {
    path.rsplit_once('/')
        .map_or(path, |(partition, _)| partition)
}

/// H6's plan P over every partition named by the start or any root's
/// layout: those dated on or after `first_hot_date`, and every one the
/// start does not record as removed. Pure.
pub fn plan_copy_back<'a>(
    partitions: impl IntoIterator<Item = &'a str>,
    first_hot_date: &str,
    removed: &dyn Fn(&str) -> bool,
) -> BTreeSet<String> {
    partitions
        .into_iter()
        .filter(|partition| date_of(partition) >= first_hot_date || !removed(partition))
        .map(str::to_owned)
        .collect()
}

// ------------------------------------------------------------- the run

/// Runs one restore (module docs). `first_hot_date` and `now_ms` come from
/// one clock read in production; `now_ms` is read once more for the run's
/// `at_ms`, which every record and every `damaged/<at_ms>/` name carries.
pub fn run(
    config: &IngestConfig,
    archive: Option<&Path>,
    flags: Flags,
    first_hot_date: &str,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
) -> RestoreReport {
    let mut report = RestoreReport::default();
    if let Err(Stop(why)) = restore(
        config,
        archive,
        flags,
        first_hot_date,
        probe,
        now_ms,
        &mut report,
    ) {
        report.problems.push(why);
    }
    report.lines.push(if report.restored {
        "restored: hot marker written; mie ingest may start".to_owned()
    } else {
        "not restored: no hot marker written".to_owned()
    });
    report
}

/// The `restore` wording of a refusal on a root.
fn kind_of(copy_target: bool) -> &'static str {
    if copy_target {
        "copy target"
    } else {
        "read-only root"
    }
}

/// H0's marker checks over every configured cold root.
fn marked_roots(config: &IngestConfig) -> Result<Vec<Marked>, Stop> {
    let hot = config.paths.raw_root.as_path();
    let mut marked: Vec<Marked> = Vec::new();
    for cold in &config.paths.cold_roots {
        let kind = kind_of(cold.copy_target);
        match root_marker::read(&cold.path) {
            Ok(Some(id)) if id == HOT_ID => {
                return Err(Stop(format!(
                    "{kind} {} carries hot's marker (id {HOT_ID:?}), the id of raw_root {}: the \
                     same root under two names, or a copied marker (ADR-062 D7)",
                    cold.path.display(),
                    hot.display()
                )));
            }
            Ok(Some(id)) => {
                if let Some(other) = marked.iter().find(|root| root.id == id) {
                    return Err(Stop(format!(
                        "cold roots {} and {} both carry the id {id:?}: two spellings of one \
                         root, or a copied marker (ADR-062 D7)",
                        other.path.display(),
                        cold.path.display()
                    )));
                }
                marked.push(Marked {
                    path: cold.path.clone(),
                    id,
                    copy_target: cold.copy_target,
                });
            }
            Ok(None) => {
                return Err(Stop(format!(
                    "{kind} {} has no root marker: every configured root must present its marker, \
                     since the marker is what tells an unmounted root from an empty one, and the \
                     restore lists every root; it stops with nothing written (ADR-062 D7 step 2, \
                     D11 step 1)",
                    cold.path.display()
                )));
            }
            Err(error) => {
                return Err(Stop(format!(
                    "{kind} {}: its root marker cannot be read ({error}); the restore stops with \
                     nothing written (ADR-062 D7 step 2)",
                    cold.path.display()
                )));
            }
        }
    }
    Ok(marked)
}

/// The `source=` names at the top of the layout of `root`.
fn sources_of(root: &Path) -> Result<BTreeSet<String>, Stop> {
    let names = sorted_names(root).map_err(|error| {
        Stop(format!(
            "cannot list {} ({error}); the restore stops with nothing written (ADR-062 D11)",
            root.display()
        ))
    })?;
    Ok(names
        .iter()
        .filter_map(|name| name.strip_prefix("source="))
        .filter(|source| validate_segment(source).is_ok())
        .map(str::to_owned)
        .collect())
}

/// Fsyncs the directory `dir`.
fn sync_dir(dir: &Path) -> Result<(), Stop> {
    fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|error| Stop(format!("sync {}: {error}", dir.display())))
}

/// An import-ledger directory as H2 reads it: every well-formed file by
/// `<segment>/<name>`, and every malformed name.
#[derive(Default)]
struct LedgerListing {
    files: Vec<(String, PathBuf)>,
    malformed: Vec<String>,
}

/// Lists the import-ledger directory `dir` (hot's, or a target's
/// `.mie/import-ledger/`; a missing one is empty). `PENDING` and the
/// immutable copy's temp names (`.<name>.tmp`) are skipped; every other
/// name in a segment that is no import-ledger name (G1) is malformed,
/// a segment that is no valid segment too: named, never written (H4).
fn list_ledger(dir: &Path) -> std::io::Result<LedgerListing> {
    let mut listing = LedgerListing::default();
    for segment in sorted_names(dir)? {
        let path = dir.join(&segment);
        if segment == PENDING_FILE || !path.is_dir() {
            continue;
        }
        if validate_segment(&segment).is_err() {
            listing.malformed.push(format!("{segment}/"));
            continue;
        }
        for name in sorted_names(&path)? {
            if name.starts_with('.') && name.ends_with(".tmp") {
                continue;
            }
            let label = format!("{segment}/{name}");
            if LedgerName::parse(&name).is_ok() && path.join(&name).is_file() {
                listing.files.push((label, path.join(&name)));
            } else {
                listing.malformed.push(label);
            }
        }
    }
    Ok(listing)
}

/// The restore; a [`Stop`] means no marker was written.
#[allow(clippy::too_many_lines)]
fn restore(
    config: &IngestConfig,
    archive: Option<&Path>,
    flags: Flags,
    first_hot_date: &str,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    report: &mut RestoreReport,
) -> Result<(), Stop> {
    let hot = config.paths.raw_root.as_path();
    let cold_roots = &config.paths.cold_roots;

    // H0.
    if cold_roots.is_empty() {
        return Err(Stop(
            "paths.cold_roots is empty: a single-root store has no copy targets to restore hot \
             from (ADR-062 D11)"
                .to_owned(),
        ));
    }
    match fs::metadata(hot) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(Stop(format!(
                "raw_root {} is no directory (ADR-062 D11)",
                hot.display()
            )));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(Stop(format!(
                "raw_root {} does not exist: mount or create the replacement directory first. \
                 The restore never creates it, since a directory it created could put hot on the \
                 wrong filesystem (an unmounted mount point); nothing is written (ADR-062 D11)",
                hot.display()
            )));
        }
        Err(error) => {
            return Err(Stop(format!(
                "raw_root {}: cannot inspect it: {error}",
                hot.display()
            )));
        }
    }
    match root_marker::read(hot) {
        Ok(None) => {}
        Ok(Some(id)) if id == HOT_ID => {
            return Err(Stop(format!(
                "raw_root {} carries hot's marker: `mie store restore` is for a lost hot only and \
                 refuses an intact one. Damage to hot's own journal or import-ledger files on an \
                 intact hot is #217; a damaged copy on a copy target is `mie store verify`, then \
                 `mie store repair`; nothing is written (ADR-062 D11)",
                hot.display()
            )));
        }
        Ok(Some(_)) => {
            return Err(Stop(check_hot(hot, cold_roots).err().unwrap_or_default()));
        }
        Err(error) => {
            return Err(Stop(format!(
                "raw_root {}: cannot read hot's root marker: {error}",
                hot.display()
            )));
        }
    }
    let archive = replicate::archive_config(config, archive).map_err(Stop)?;
    let marked = marked_roots(config)?;
    let targets: Vec<&Marked> = marked.iter().filter(|root| root.copy_target).collect();
    let target_paths: Vec<&Path> = targets.iter().map(|root| root.path.as_path()).collect();
    check_devices(hot, &target_paths, probe).map_err(Stop)?;
    let journal_path = config.paths.journal.as_path();
    let Some(journal_name) = journal_path.file_name().map(ToOwned::to_owned) else {
        return Err(Stop(format!(
            "paths.journal {} has no file name, so its copies on the targets cannot be found \
             (ADR-062 D11)",
            journal_path.display()
        )));
    };
    if let Some(archive) = &archive {
        let dir = &archive.paths.import_ledger;
        let link = fs::symlink_metadata(dir).is_ok_and(|m| m.file_type().is_symlink());
        if link && matches!(fs::metadata(dir), Err(e) if e.kind() == ErrorKind::NotFound) {
            return Err(Stop(format!(
                "the import ledger {} (paths.import_ledger of the archive configuration) is a \
                 link to a missing directory: an unmounted disk. Mount it, then run the restore \
                 again; nothing is written (ADR-062 D11)",
                dir.display()
            )));
        }
    }

    // H1.
    let meta = root_marker::meta_dir(hot);
    match fs::create_dir(&meta) {
        Ok(()) => sync_dir(hot)?,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
        Err(error) => return Err(Stop(format!("create {}: {error}", meta.display()))),
    }
    let mut sources = sources_of(hot)?;
    for root in &marked {
        sources.extend(sources_of(&root.path)?);
    }
    sources.insert(config.instrument.source.clone());
    if archive.is_some() {
        sources.insert(ARCHIVE_SOURCE.to_owned());
    }
    let locked = |what: &str, error: RawStoreError| {
        Stop(format!(
            "{what}: {error}; the restore takes it without waiting and holds it for its whole run \
             (ADR-062 D11)"
        ))
    };
    let hold = CopyLedger::at(hot)
        .try_hold()
        .map_err(|e| locked("the copy-ledger lock", e))?;
    let _pass = replica::try_replication_lock(hot).map_err(|e| locked("the pass lock", e))?;
    let _verify = replica::try_verify_lock(hot).map_err(|e| locked("the verify lock", e))?;
    let mut source_locks = Vec::new();
    for source in &sources {
        source_locks.push(
            removal::take_source_lock(hot, source)
                .map_err(|e| locked(&format!("the writer lock of source {source}"), e))?,
        );
    }

    // H2.
    let at_ms = now_ms();
    let io = |what: String| {
        move |error: std::io::Error| {
            Stop(format!(
                "{what}: {error}; a skipped read would turn its files into lost candidates, so \
                 the restore stops with nothing written (ADR-062 D11)"
            ))
        }
    };
    // 1. The ledger copies, and hot's surviving ledger.
    let mut ledger_copies = Vec::new();
    for target in &targets {
        let path = copy_ledger::ledger_path(&target.path);
        let bytes = match fs::read(&path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(io(format!("read {}", path.display()))(error)),
        };
        ledger_copies.push(LedgerCopyRead {
            id: target.id.clone(),
            bytes,
        });
    }
    let hot_ledger_path = copy_ledger::ledger_path(hot);
    let hot_ledger = match fs::read(&hot_ledger_path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => return Err(io(format!("read {}", hot_ledger_path.display()))(error)),
    };
    let mut mode = RestoreMode {
        hot_ledger: hot_ledger.clone(),
        copy_back: BTreeSet::new(),
    };
    let (_, _, start_ledger, _) = rebuild::choose_start(&rebuild::start_candidates(
        &Mode::Restore(mode.clone()),
        &ledger_copies,
    ));
    let start = CopyState::fold(&start_ledger)
        .map_err(|error| Stop(format!("the start copy cannot be folded: {error}")))?;

    // 2. The journal candidates: hot's, then each target's copy.
    let mut candidates: Vec<(JournalCandidate, PathBuf)> = Vec::new();
    match fs::metadata(journal_path) {
        Ok(metadata) => candidates.push((
            JournalCandidate {
                label: format!("hot's journal {}", journal_path.display()),
                len: metadata.len(),
            },
            journal_path.to_owned(),
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(io(format!("inspect {}", journal_path.display()))(error)),
    }
    for target in &targets {
        let dir = target.path.join(META_DIR).join(JOURNAL_DIR);
        let names: Vec<String> = sorted_names(&dir)
            .map_err(io(format!("list {}", dir.display())))?
            .into_iter()
            .filter(|name| !name.starts_with('.'))
            .collect();
        let wanted = journal_name.to_string_lossy();
        if !names.iter().any(|name| *name == wanted) {
            if !names.is_empty() {
                return Err(Stop(format!(
                    "{} holds {} but no copy of {wanted} (the file name of paths.journal): a \
                     renamed paths.journal would drop every pre-loss run, so the restore stops \
                     with nothing written; restore the configured name (ADR-062 D11 step 2)",
                    dir.display(),
                    names.join(", ")
                )));
            }
            continue;
        }
        let path = dir.join(&journal_name);
        let len = fs::metadata(&path)
            .map_err(io(format!("inspect {}", path.display())))?
            .len();
        candidates.push((
            JournalCandidate {
                label: format!("the journal copy on {} ({})", target.id, path.display()),
                len,
            },
            path,
        ));
    }

    // 3. The import ledger: hot's (when it exists) and every target's.
    let mut files: BTreeMap<String, Vec<LedgerFileCopy>> = BTreeMap::new();
    let mut malformed = Vec::new();
    let mut hot_ledger_dir: Option<(PathBuf, bool, usize)> = None;
    if let Some(archive) = &archive {
        let dir = archive.paths.import_ledger.clone();
        let exists = match fs::metadata(&dir) {
            Ok(_) => true,
            Err(error) if error.kind() == ErrorKind::NotFound => false,
            Err(error) => return Err(io(format!("inspect {}", dir.display()))(error)),
        };
        let dirs =
            std::iter::once((HOT_ID.to_owned(), dir.clone())).chain(targets.iter().map(|target| {
                (
                    target.id.clone(),
                    target.path.join(META_DIR).join(IMPORT_LEDGER_DIR),
                )
            }));
        let mut on_hot = 0;
        for (root, ledger_dir) in dirs {
            let listing = list_ledger(&ledger_dir).map_err(|error| {
                Stop(format!(
                    "the import ledger on {root} ({}) cannot be listed ({error}); the restore \
                     stops with nothing written (ADR-062 D11)",
                    ledger_dir.display()
                ))
            })?;
            for name in listing.malformed {
                malformed.push(format!(
                    "{name:?} on {root}: no import-ledger name (F.import, F.<n>.import or \
                     F.<n>.supersede.import); named, never written to hot (ADR-062 D11)"
                ));
            }
            for (name, path) in listing.files {
                let bytes = fs::read(&path).map_err(io(format!("read {}", path.display())))?;
                if root == HOT_ID {
                    on_hot += 1;
                }
                files.entry(name).or_default().push(LedgerFileCopy {
                    root: root.clone(),
                    path,
                    bytes,
                });
            }
        }
        if fs::symlink_metadata(dir.join(PENDING_FILE)).is_ok() {
            report.lines.push(format!(
                "{}: an interrupted import's PENDING survives on hot; it is left in place for the \
                 importer, and the restore's checks get none (ADR-062 D11 step 5)",
                dir.join(PENDING_FILE).display()
            ));
        }
        hot_ledger_dir = Some((dir, exists, on_hot));
    }
    let entries: BTreeMap<String, Vec<EntryCopy>> = files
        .iter()
        .map(|(name, copies)| {
            let copies = copies
                .iter()
                .map(|copy| EntryCopy {
                    root: copy.root.clone(),
                    bytes: copy.bytes.clone(),
                })
                .collect();
            (name.clone(), copies)
        })
        .collect();

    // 4. Hot's layout.
    let listing = |root: &Path, error: RawStoreError| {
        Stop(format!(
            "cannot list the layout of {} ({error}); a skipped directory would turn its files into \
             lost candidates, so the restore stops with nothing written (ADR-062 D11)",
            root.display()
        ))
    };
    let quarantine = |root: &Path, error: RawStoreError| {
        Stop(format!(
            "cannot read the quarantine of {} ({error}); the restore stops with nothing written \
             (ADR-062 D7 step 5a)",
            root.display()
        ))
    };
    let mut hot_read = HotRead {
        quarantine: replica::list_quarantine(hot).map_err(|e| quarantine(hot, e))?,
        ..HotRead::default()
    };
    for path in replica::list_sealed(hot).map_err(|e| listing(hot, e))? {
        let copy = rebuild::read_copy(hot, &path, false)?;
        hot_read.files.insert(path, copy);
    }
    hot_read.orphan_data = rebuild::orphan_data(hot).map_err(|e| listing(hot, e))?;
    hot_read.partitions = removal::scan_hot(hot, "")
        .map_err(|e| listing(hot, e))?
        .partitions
        .into_keys()
        .collect();

    // 5. Each cold root's layout; data checked only for unrecorded copies
    // on a copy target (decision 4).
    let mut roots = Vec::new();
    for root in &marked {
        let mut root_files = BTreeMap::new();
        for path in replica::list_sealed(&root.path).map_err(|e| listing(&root.path, e))? {
            let recorded = start
                .copy(&path, &root.id)
                .is_some_and(|copy| copy.latest.is_some());
            let check = root.copy_target && !recorded;
            let copy = rebuild::read_copy(&root.path, &path, check)?;
            root_files.insert(path, copy);
        }
        roots.push(RootRead {
            id: root.id.clone(),
            copy_target: root.copy_target,
            files: root_files,
            orphan_data: rebuild::orphan_data(&root.path).map_err(|e| listing(&root.path, e))?,
            quarantine: replica::list_quarantine(&root.path)
                .map_err(|e| quarantine(&root.path, e))?,
        });
    }

    // H6's plan P, before the derivation, which counts it as on hot (H5 e).
    let named: BTreeSet<&str> = start
        .paths()
        .map(|(path, _)| path)
        .chain(hot_read.files.keys().map(String::as_str))
        .chain(hot_read.orphan_data.iter().map(String::as_str))
        .chain(
            roots
                .iter()
                .flat_map(|root| root.files.keys().chain(&root.orphan_data))
                .map(String::as_str),
        )
        .filter(|path| copy_ledger::is_data_path(path))
        .map(partition_of)
        .collect();
    mode.copy_back = plan_copy_back(named, first_hot_date, &|partition| {
        start.is_removed(partition)
    });

    // H3: the journal, decided before anything is written.
    let labels: Vec<JournalCandidate> = candidates.iter().map(|(c, _)| c.clone()).collect();
    let chosen = choose_journal(&labels, &mut |i, j, len| {
        replica::first_difference(&candidates[i].1, TailSource::File(&candidates[j].1), len)
            .map_err(|error| {
                format!(
                    "compare {} with {}: {error}; the restore stops with nothing written",
                    candidates[i].0.label, candidates[j].0.label
                )
            })
    })?;
    for (candidate, _) in &candidates {
        report.lines.push(format!(
            "journal candidate: {}, {} bytes",
            candidate.label, candidate.len
        ));
    }
    let mut journal_bytes = Vec::new();
    match chosen {
        None => report.lines.push(
            "journal: no candidate on hot or on any copy target; the journal stays absent"
                .to_owned(),
        ),
        Some(i) => {
            let (candidate, path) = &candidates[i];
            let mut file = fs::File::open(path).map_err(io(format!("open {}", path.display())))?;
            Read::take(&mut file, candidate.len)
                .read_to_end(&mut journal_bytes)
                .map_err(io(format!("read {}", path.display())))?;
            report
                .lines
                .push(format!("journal: chosen {}", candidate.label));
        }
    }
    let terminate = journal_bytes.last().is_some_and(|&byte| byte != b'\n');
    if terminate {
        journal_bytes.push(b'\n');
    }

    // H5: the derivation.
    let mut inputs = RebuildInputs {
        mode: Mode::Restore(mode.clone()),
        at_ms,
        first_hot_date: first_hot_date.to_owned(),
        record_lost: flags.record_lost,
        record_unlisted: false,
        import_ledger: hot_ledger_dir
            .as_ref()
            .map(|(dir, _, _)| dir.display().to_string()),
        archive_configured: archive.is_some(),
        missing_import_ledger: None,
        ledger_copies,
        journal: rebuild::parse_journal(&journal_bytes),
        entries,
        pending: None,
        hot: hot_read,
        roots,
    };
    drop(journal_bytes);
    // H4 takes the variants the entry resolution decides; the derivation
    // then runs again over the import ledger as H4 leaves it on hot, so its
    // findings describe the restored store (an entry H4 writes is no longer
    // "on the targets only").
    let decided = rebuild::derive(&inputs)?;
    let plan = plan_import_ledger(&files, &decided.chosen_entries);
    drop(decided);
    for (name, source) in &plan.writes {
        let Some(bytes) = files
            .get(name)
            .and_then(|copies| copies.iter().find(|copy| copy.path == *source))
            .map(|copy| copy.bytes.clone())
        else {
            continue;
        };
        let copies = inputs.entries.entry(name.clone()).or_default();
        copies.retain(|copy| copy.root != HOT_ID);
        copies.insert(
            0,
            EntryCopy {
                root: HOT_ID.to_owned(),
                bytes,
            },
        );
    }
    let derived = rebuild::derive(&inputs)?;
    if let Some((dir, exists, on_hot)) = &hot_ledger_dir {
        report.lines.push(format!(
            "import ledger {}: {}, {on_hot} file(s) on hot, {} to write, {} hot cop{} to set \
             aside",
            dir.display(),
            if *exists { "exists" } else { "absent, created" },
            plan.writes.len(),
            plan.set_aside.len(),
            if plan.set_aside.len() == 1 {
                "y"
            } else {
                "ies"
            }
        ));
    }
    report.lines.extend(plan.lines.iter().cloned());
    report.lines.extend(derived.lines.iter().cloned());
    report.problems.extend(malformed);
    report.problems.extend(plan.problems.iter().cloned());
    report.problems.extend(derived.problems.iter().cloned());
    let unrecorded: Vec<&str> = derived
        .lost
        .iter()
        .filter(|file| !file.recorded)
        .map(|file| file.path.as_str())
        .collect();
    if !unrecorded.is_empty() {
        return Err(Stop(format!(
            "{} lost candidate(s) ({}): the restore stops before any write. In the default layout \
             the restored journal names only files already on its copy, so a candidate means a \
             second fault; after checking for a shared cause (an unmounted root, roots restored \
             from an old backup), `mie store restore --record-lost` records them lost (ADR-062 \
             D7 step 4, D11)",
            unrecorded.len(),
            unrecorded.join(", ")
        )));
    }

    // H7: no evidence of this `at_ms` may exist before the first write.
    let hot_damaged = replica::damaged_dir(hot, at_ms);
    let target_asides: Vec<PathBuf> = targets
        .iter()
        .map(|target| replica::damaged_dir(&target.path, at_ms).join("copy-ledger"))
        .collect();
    for aside in std::iter::once(&hot_damaged).chain(&target_asides) {
        if fs::symlink_metadata(aside).is_ok() {
            return Err(Stop(format!(
                "{} exists: evidence is never overwritten; the restore stops with nothing written \
                 (ADR-062 D7)",
                aside.display()
            )));
        }
    }

    // H7 1: hot's journal (H3).
    if let Some(i) = chosen {
        let (candidate, path) = &candidates[i];
        let appended = if *path == journal_path {
            0
        } else {
            replica::extend_prefix(TailSource::File(path), journal_path, candidate.len).map_err(
                |error| {
                    Stop(format!(
                        "extend hot's journal {} from {}: {error}",
                        journal_path.display(),
                        candidate.label
                    ))
                },
            )?
        };
        report.lines.push(format!(
            "journal: hot's journal {} extended by {appended} byte(s)",
            journal_path.display()
        ));
        if terminate {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(journal_path)
                .map_err(|e| Stop(format!("open {}: {e}", journal_path.display())))?;
            file.write_all(b"\n")
                .and_then(|()| file.sync_all())
                .map_err(|e| Stop(format!("append to {}: {e}", journal_path.display())))?;
            report.lines.push(
                "journal: its last line was cut, so one `\\n` ends it: the next run_start parses \
                 as a line of its own (ADR-062 D11)"
                    .to_owned(),
            );
        }
    }
    reached(Step::Journal)?;

    // H7 2: hot's import ledger (H4).
    if let Some((dir, _, _)) = &hot_ledger_dir {
        fs::create_dir_all(dir).map_err(|e| Stop(format!("create {}: {e}", dir.display())))?;
        for name in &plan.set_aside {
            let aside = hot_damaged.join(IMPORT_LEDGER_DIR).join(name);
            replica::move_file_aside(&dir.join(name), &aside).map_err(|error| {
                Stop(format!(
                    "move hot's import-ledger file {name} aside to {}: {error}",
                    aside.display()
                ))
            })?;
        }
        for (name, source) in &plan.writes {
            replica::copy_immutable(source, &dir.join(name)).map_err(|error| {
                Stop(format!("write import-ledger file {name} to hot: {error}"))
            })?;
            report.lines.push(format!(
                "import ledger: wrote {name} from {}",
                source.display()
            ));
        }
    }
    reached(Step::ImportLedger)?;

    // H7 3: the install (H5, B9). A hot ledger that already equals the
    // derived one (a re-run after the replace) skips only the replace, so
    // no identical ledger is linked into a new `damaged/<at_ms>/`; the
    // markers are re-read and the targets' non-prefix copies moved aside
    // either way, so a resumed restore leaves the same files as an
    // uncrashed one (review of #218, finding 2).
    for root in &marked {
        if let Some(why) = rebuild::marker_gone(root) {
            return Err(Stop(format!(
                "{} ({}): {why} since the run started; a mount lost during the listing reads \
                 as absent files, so the restore stops before the install (ADR-062 D7 step 2)",
                root.path.display(),
                root.id
            )));
        }
    }
    let replace = hot_ledger.as_deref() != Some(derived.ledger.as_slice());
    if replace {
        let hot_aside = hot_damaged.join("copy-ledger");
        hold.replace(&derived.ledger, Some(&hot_aside))
            .map_err(|error| Stop(format!("the install failed: {error}")))?;
        if hot_ledger.is_some() {
            report.lines.push(format!(
                "hot's former copy ledger is linked to {}",
                hot_aside.display()
            ));
        }
        report.lines.push(format!(
            "gate: {} holds {}: the hot removal waits for a `mie store verify` of every copy \
             target that covers the restored ledger (ADR-062 D7 step 6)",
            mie_adapter_parquet::rebuilt::gate_path(hot).display(),
            derived.ledger.len()
        ));
    } else {
        report.lines.push(format!(
            "hot's copy ledger {} already holds the derived ledger; it is kept",
            hot_ledger_path.display()
        ));
    }
    reached(Step::Replace)?;
    let mut moved = RebuildReport::default();
    for (target, aside) in targets.iter().zip(&target_asides) {
        rebuild::move_copy_aside(hot, target, aside, &mut moved);
    }
    report.lines.extend(moved.lines);
    report.problems.extend(moved.problems);
    if replace {
        report.lines.push(format!(
            "wrote {} ({} bytes, {} records added)",
            hot_ledger_path.display(),
            derived.ledger.len(),
            derived.added.len()
        ));
    }
    let installed = &derived.ledger;
    reached(Step::Install)?;

    // H7 4: the copy-back (H6).
    let ledger = copy_ledger::parse(&hot_ledger_path, installed)
        .map_err(|error| Stop(format!("the installed ledger does not parse: {error}")))?;
    let state = CopyState::fold(&ledger)
        .map_err(|error| Stop(format!("the installed ledger cannot be folded: {error}")))?;
    let mut by_partition: BTreeMap<&str, Vec<(&str, &str)>> = BTreeMap::new();
    for (path, verdict) in state.paths() {
        if verdict != PathVerdict::Ordinary || !mode.copy_back.contains(partition_of(path)) {
            continue;
        }
        if let Some(text) = state.ledger_text(path) {
            by_partition
                .entry(partition_of(path))
                .or_default()
                .push((path, text));
        }
    }
    let cold_paths: Vec<PathBuf> = cold_roots.iter().map(|root| root.path.clone()).collect();
    let mut first = true;
    for (partition, paths) in &by_partition {
        let (mut copied, mut present, mut failed) = (0usize, 0usize, 0usize);
        for (i, (path, text)) in paths.iter().enumerate() {
            let mut damaged: Vec<replica::DamagedCopy> = Vec::new();
            let outcome = replica::copy_back(
                &cold_paths,
                hot,
                path,
                text,
                OtherText::PassOver,
                &mut |copy| damaged.push(copy.clone()),
            );
            for copy in &damaged {
                report.problems.push(format!(
                    "{path} on {}: {}: the copy does not verify and is passed over; run `mie \
                     store verify`, then `mie store repair` (ADR-062 D11)",
                    copy.root.display(),
                    copy.error
                ));
            }
            match outcome {
                Ok(back) if back.outcome == CopyOutcome::AlreadyPresent => present += 1,
                Ok(_) => copied += 1,
                Err(error) => {
                    let manifest =
                        format!("{}.manifest", path.strip_suffix(".parquet").unwrap_or(path));
                    let holders: Vec<&PathBuf> = cold_paths
                        .iter()
                        .filter(|root| fs::symlink_metadata(root.join(&manifest)).is_ok())
                        .collect();
                    let no_good_copy = holders
                        .iter()
                        .all(|root| damaged.iter().any(|copy| copy.root == **root));
                    if !no_good_copy {
                        return Err(Stop(format!(
                            "{path}: the copy back onto hot failed on hot's side ({error}); the \
                             restore stops before hot's marker, and running it again completes \
                             it (ADR-062 D11)"
                        )));
                    }
                    failed += 1;
                    report.problems.push(format!(
                        "{path}: no cold root holds a verified copy ({error}); it stays recorded, \
                         so every selection that covers it fails with Integrity: the lost-file \
                         rule (ADR-062 D11 step 5)"
                    ));
                }
            }
            if first {
                first = false;
                reached(Step::FirstCopy)?;
            }
            #[cfg(test)]
            if i == 0
                && paths.len() > 1
                && let Some(hook) = DURING_COPY_BACK.with(|hook| hook.borrow_mut().take())
            {
                hook();
            }
            let _ = i;
        }
        let mut line = format!(
            "copy-back {partition}: {copied} copied, {present} already on hot, {failed} with no \
             good copy"
        );
        if failed == 0 && state.is_removed(partition) {
            let record = Record {
                at_ms,
                kind: Kind::Returned {
                    partition: (*partition).to_owned(),
                },
            };
            let outcome = hold
                .append(&record)
                .map_err(|error| Stop(format!("append `returned {partition}`: {}", error.error)))?;
            if let Some(truncation) = outcome.truncation {
                report
                    .lines
                    .push(replicate::truncation_line(&hot_ledger_path, truncation));
            }
            line.push_str("; returned");
        }
        report.lines.push(line);
    }
    reached(Step::Returned)?;

    // H7 5: every marker re-read, then hot's marker, the commit.
    for root in &marked {
        if let Some(why) = rebuild::marker_gone(root) {
            return Err(Stop(format!(
                "{} ({}): {why} since the run started; the restore stops before hot's marker, \
                 and running it again completes it (ADR-062 D11)",
                root.path.display(),
                root.id
            )));
        }
    }
    reached(Step::BeforeMarker)?;
    root_marker::publish(hot, HOT_ID)
        .map_err(|error| Stop(format!("write hot's marker: {error}")))?;
    report.restored = true;
    report.lines.push(format!(
        "wrote {} (id {HOT_ID})",
        root_marker::marker_path(hot).display()
    ));
    Ok(())
}
