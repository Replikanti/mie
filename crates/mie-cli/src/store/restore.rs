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
//!   decision 4), and the data of every crash form on a root without the
//!   manifest (rule 1′); everything copied back is verified by the copy.
//!   A listing error stops the run: a skipped directory would turn its
//!   files into lost candidates. **U** (amendment 2): a read error on a
//!   name the listing found never stops it, and no byte is deleted: the
//!   name stays counted and its content is a damaged copy (a cold
//!   manifest, a pending manifest, a target's ledger copy, an import-ledger
//!   file, a quarantined manifest, a journal candidate no longer than C).
//!   Hot's layout and hot's copy ledger are hot's side and still stop it.
//! - **H3′ The journal** ([`choose_journal`]): C is the longest candidate
//!   that reads in full (hot's own on a tie); every other must be C's
//!   prefix of its own length, otherwise the run stops naming both and the
//!   first differing byte. A candidate that cannot be read is left out with
//!   a `FAIL:` when it is no longer than C; a longer one stops the run with
//!   nothing written, unless `--skip-unreadable-journal` leaves it out.
//!   Hot's own journal, when left out, is moved to
//!   `<paths.journal>.unreadable-<at_ms>`; a target's copy the flag leaves
//!   out to `<target>/.mie/damaged/<at_ms>/.mie/journal/`. A target
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
//!   (its differences (a) to (g) are documented there), then the rebuild's
//!   install (B9). Lost candidates are recorded only with `--record-lost`;
//!   without it the run stops before any write (#195 STOP 1 decision 3).
//!   The rebuild's rule B6a, shared by both modes, brings every path a cold
//!   root holds and no start record names into the restored ledger's
//!   numbering: a `manifest` record from a cold text (a manifest, else a
//!   pending manifest whose data verifies, rule 1′), else a lost
//!   candidate; a recorded path hot lacks is held only by a cold manifest,
//!   a data file or a verified crash copy (rule 4′). When hot's surviving
//!   ledger
//!   already equals the derived one (a re-run), only the replace is
//!   skipped: the markers are re-read and non-prefix target copies moved
//!   aside as in an uncrashed run.
//! - **H6 Copy-back** ([`plan_copy_back`]): every partition dated in the
//!   hot window or not recorded removed by the start; per partition, every
//!   path whose verdict in the installed ledger is ordinary, from the cold
//!   roots in configuration order with the installed text
//!   ([`replica::copy_back`], [`OtherText::PassOver`]), (i) reading a
//!   root's crash forms too ([`SourceForms::CrashForms`]: its manifest,
//!   else its pending manifest; its data, else its data temp file). A
//!   damaged copy passed over, and a path with no verified copy left (the
//!   lost-file rule: it stays recorded, every read of it is `Integrity`),
//!   are `FAIL:` lines and do not stop the restore; an error on hot's side
//!   does. A passed-over copy's `FAIL:` names verify, then repair, only for
//!   a recorded copy on a copy target (round 3). (J, amendment 2) Then,
//!   one by one in path order, every path of a removed partition outside
//!   the plan that the installed ledger records ordinary, with no `copy`
//!   record or no cold data file, and of which a cold root holds a text
//!   form or hot a verified copy ([`plan_unreplicated`], round 4: a crash
//!   after (k) moved the only cold text form keeps the path in J, so (k)
//!   still reaches every target); no `returned` is written, so a re-run
//!   lists the same paths. (k) After each path's copy-back ([`set_aside`]),
//!   on every copy target: k1 moves an unrecorded `M`/`D` copy the next
//!   pass's D5 copy would refuse into `.mie/damaged/<at_ms>/` (never a
//!   self-consistent other file, D5); otherwise k2 moves every temp name
//!   that is no verifiable copy of the installed text, since the next pass
//!   removes both temp names of a path it copies. An unreadable file there
//!   is moved too, with a `FAIL:`. A window partition the start records
//!   removed gets `returned` once all its paths are back.
//! - **H7 Write order and commit**: H3′'s moves of the candidates it
//!   leaves out, H3, H4, the install, the copy-back and its `returned`
//!   records; every root's marker re-read; hot's marker last
//!   ([`root_marker::publish`]). Why last: a marker written first and a
//!   crash after it would let `mie ingest` start on a half-restored hot,
//!   numbering parts from hot alone and reusing paths cold holds (D5). A
//!   stop names what completes it: a taken aside name a re-run (a new
//!   `at_ms`), a refused write the root accepting it, a gone marker the
//!   root presenting it.
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
use mie_adapter_parquet::replica::{
    self, CopyCheck, CopyOutcome, OtherText, SourceForms, TailSource, TempName,
};
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
    /// The unreadable journal candidates H3′ leaves out are moved aside
    /// (reached only when one was moved).
    JournalAside,
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
    /// The first temp name is set aside after its path's copy-back (H6 k).
    SetAside,
    /// Every partition is copied back and every `returned` appended.
    Returned,
    /// Every marker is re-read; hot's is not written yet.
    BeforeMarker,
}

#[cfg(test)]
impl Step {
    /// Every step a restore without an unreadable journal candidate
    /// reaches; [`Step::JournalAside`] needs one, so it is left out.
    pub(crate) const ALL: [Self; 8] = [
        Self::Journal,
        Self::ImportLedger,
        Self::Replace,
        Self::Install,
        Self::FirstCopy,
        Self::SetAside,
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
    /// `--skip-unreadable-journal`: leave out a journal candidate that
    /// cannot be read and is longer than every readable one, moved aside
    /// and never deleted (#195 amendment 2, H3′, decided variant C).
    pub skip_unreadable_journal: bool,
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

/// A journal candidate H3′ leaves out, since it cannot be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeftOut {
    /// Its index among the candidates.
    pub index: usize,
    /// Its read error.
    pub error: String,
    /// Left out by `--skip-unreadable-journal`, since it is longer than C;
    /// otherwise by U, since it is at most C's length.
    pub by_flag: bool,
}

/// What H3′ decided.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JournalChoice {
    /// The index of C, the longest candidate that reads in full; `None`
    /// when no candidate reads.
    pub chosen: Option<usize>,
    /// C's bytes, read in full; empty without C.
    pub bytes: Vec<u8>,
    /// The candidates left out, in candidate order.
    pub left_out: Vec<LeftOut>,
}

/// H3′'s comparison oracle: the first byte in `[0, len)` where candidate
/// `i` differs from C's bytes, or the candidate's read error.
pub type Differs<'a> = dyn FnMut(usize, &[u8], u64) -> Result<Option<u64>, String> + 'a;

/// H3′ (#195 amendment 2): C is the first candidate, longest first and
/// H2's order on equal lengths (so hot's own journal on a tie), whose
/// bytes `read(i)` gives in full; every other candidate must be C's prefix
/// of its own length (`differs(i, c, len)`, the first byte in `[0, len)`
/// where candidate `i` differs from C's bytes `c`). A candidate whose read
/// fails is unreadable: one of at most C's length (L, 0 without C) is left
/// out (U), a longer one stops the run unless `skip_unreadable`, which
/// leaves it out. `aside(i)` names where the caller moves candidate `i`.
/// Pure.
///
/// # Errors
///
/// One line naming both candidates and the first differing byte when a
/// candidate is not C's prefix of its own length, equal lengths included;
/// otherwise one line per unreadable candidate longer than C, without
/// `skip_unreadable`. Nothing may be written then.
pub fn choose_journal(
    candidates: &[JournalCandidate],
    skip_unreadable: bool,
    read: &mut dyn FnMut(usize) -> Result<Vec<u8>, String>,
    differs: &mut Differs<'_>,
    aside: &dyn Fn(usize) -> String,
) -> Result<JournalChoice, Vec<String>> {
    let mut order: Vec<usize> = (0..candidates.len()).collect();
    order.sort_by(|a, b| candidates[*b].len.cmp(&candidates[*a].len));
    let mut unreadable: BTreeMap<usize, String> = BTreeMap::new();
    let mut choice = JournalChoice::default();
    for &i in &order {
        match read(i) {
            Ok(bytes) => {
                choice.chosen = Some(i);
                choice.bytes = bytes;
                break;
            }
            Err(error) => {
                unreadable.insert(i, error);
            }
        }
    }
    if let Some(c) = choice.chosen {
        for &i in &order {
            if i == c || unreadable.contains_key(&i) {
                continue;
            }
            match differs(i, &choice.bytes, candidates[i].len) {
                Ok(None) => {}
                Ok(Some(byte)) => {
                    let (other, chosen) = (&candidates[i], &candidates[c]);
                    return Err(vec![format!(
                        "the journal copies differ beyond their common prefix: {} ({} bytes) \
                         differs from {} ({} bytes) at byte {byte}. A copy is a byte prefix of \
                         hot's journal, so one of them is damaged, and nothing can tell which \
                         holds the live data; the restore stops with nothing written (ADR-062 \
                         D11 step 2, step 5)",
                        other.label, other.len, chosen.label, chosen.len
                    )]);
                }
                Err(error) => {
                    unreadable.insert(i, error);
                }
            }
        }
    }
    let longest = choice.chosen.map_or(0, |c| candidates[c].len);
    let from = choice.chosen.map_or_else(
        || "none reads".to_owned(),
        |c| format!("{}, {longest} bytes", candidates[c].label),
    );
    let restored = choice.chosen.map_or_else(
        || "stays absent".to_owned(),
        |c| format!("is restored from {}", candidates[c].label),
    );
    let mut stops = Vec::new();
    for (&i, error) in &unreadable {
        let candidate = &candidates[i];
        let by_flag = candidate.len > longest;
        if by_flag && !skip_unreadable {
            stops.push(format!(
                "the journal candidate {} ({} bytes) cannot be read ({error}), and it is longer \
                 than every readable candidate ({from}). Nothing can tell what its last {} \
                 byte(s) hold, and the runs that only they name would be missing from hot's \
                 journal. The restore stops with nothing written. After a transient read error, \
                 a plain re-run completes it. For a permanent one (a failing disk), \
                 `--skip-unreadable-journal` leaves it out: hot's journal {restored}, and the \
                 candidate is moved to {}, never deleted (ADR-062 D11 step 2)",
                candidate.label,
                candidate.len,
                candidate.len - longest,
                aside(i)
            ));
            continue;
        }
        choice.left_out.push(LeftOut {
            index: i,
            error: error.clone(),
            by_flag,
        });
    }
    if stops.is_empty() {
        Ok(choice)
    } else {
        Err(stops)
    }
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

/// A form of a path that J asks about (H6 J): the first three are held by
/// a cold root, by name; the last by hot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Held {
    /// The manifest `M`.
    Manifest,
    /// The pending manifest `M~`.
    PendingManifest,
    /// The data file `D`.
    Data,
    /// Hot's sealed copy, verified against the installed text
    /// ([`replica::check_sealed_copy`] is `Good`; review round 4).
    VerifiedOnHot,
}

/// H6 J (#195 amendment 2): the paths the restore copies back one by one
/// after the planned partitions, in path order. From the installed ledger
/// `state`: a path whose partition is recorded removed and not in the
/// copy-back plan `plan`, whose verdict is ordinary, that has no `copy`
/// record with a copy on any root or whose data file no cold root holds,
/// and of which some cold root holds the manifest or the pending manifest
/// (`holds`, over every cold root), or hot a verified copy. Why: removal
/// rule (e) needs a `copy` record on all k targets, so such a path was
/// sealed after a recall whose `returned` sat in hot's lost tail, or
/// recorded from a cold copy by a rebuild or a restore; a path no root
/// holds as `D` cannot be read; without a text form there is nothing to
/// copy back from, unless hot already holds the path: then the copy-back's
/// branch for a path hot holds still runs (k). That keeps a path
/// whose only cold text form (k) moved before a crash in the re-run's J
/// (review round 4, rule 6). J reads only the installed ledger, names and
/// hot's copy, so a re-run lists the same paths. Pure.
pub fn plan_unreplicated(
    state: &CopyState,
    plan: &BTreeSet<String>,
    holds: &dyn Fn(&str, Held) -> bool,
) -> Vec<String> {
    let mut paths: Vec<String> = state
        .paths()
        .filter(|(path, verdict)| {
            let partition = partition_of(path);
            *verdict == PathVerdict::Ordinary
                && state.is_removed(partition)
                && !plan.contains(partition)
        })
        .filter(|(path, _)| {
            state.copies_of(path).all(|(_, copy)| copy.latest.is_none()) || !holds(path, Held::Data)
        })
        .filter(|(path, _)| {
            holds(path, Held::Manifest)
                || holds(path, Held::PendingManifest)
                || holds(path, Held::VerifiedOnHot)
        })
        .map(|(path, _)| path.to_owned())
        .collect();
    paths.sort();
    paths
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

/// The `FAIL:` sentence for a file that cannot be read (U, #195
/// amendment 2).
const READ_ERROR: &str = "A read error can mean a failing disk: run `mie store verify`, then `mie \
                          store repair`, for what verify finds on that target";

/// The stop of a move into `.mie/damaged/` that `error` refused (H6 k,
/// H3′): a taken aside name completes on a re-run with a new `at_ms`, any
/// other failure once the root accepts the write.
fn move_stop(what: &str, root: &Path, error: &RawStoreError) -> Stop {
    let then = match error {
        RawStoreError::Integrity(_) => {
            "evidence is never overwritten, so the restore stops before hot's marker, and a \
             re-run, with a new `at_ms`, completes it"
                .to_owned()
        }
        _ => format!(
            "the restore stops before hot's marker and completes once {} accepts the write",
            root.display()
        ),
    };
    Stop(format!("{what}: {error}; {then} (ADR-062 D11)"))
}

/// The stop for a root whose marker is gone or names another id.
fn marker_stop(root: &Marked, why: &str) -> Stop {
    Stop(format!(
        "{} ({}): {why} since the run started; the restore stops before hot's marker and \
         completes once the root presents its marker (ADR-062 D11)",
        root.path.display(),
        root.id
    ))
}

/// H6 k1 (#195 round 3) for `path` on the copy target `target`, right
/// after the path's copy-back: when the installed ledger (`state`) has no
/// `copy` record of the path on the target and the target holds its data
/// file `D` or its manifest `M`, that copy is judged by the test the next
/// pass's D5 copy from hot applies: with `M`, [`replica::check_sealed_copy`]
/// against the installed `text`; without it, [`replica::check_file`] on
/// `D`. `Good` stays (the pass records or publishes it), and so does a
/// self-consistent other file (D5, a `FAIL:`; #219 case 2). Everything
/// else, an unreadable copy included (U), is moved by R5's own
/// [`replica::move_aside`] into `.mie/damaged/<at_ms>/`, so the next pass
/// copies the path from hot. Returns whether anything moved.
fn sealed_aside(
    target: &Marked,
    state: &CopyState,
    path: &str,
    text: &str,
    at_ms: i64,
    report: &mut RestoreReport,
) -> Result<bool, Stop> {
    if state
        .copy(path, &target.id)
        .is_some_and(|copy| copy.latest.is_some())
    {
        return Ok(false);
    }
    let present = |file: &Path| !matches!(fs::symlink_metadata(file), Err(e) if e.kind() == ErrorKind::NotFound);
    let data = target.path.join(path);
    let manifest = target.path.join(manifest_name(path));
    let has_manifest = present(&manifest);
    if !has_manifest && !present(&data) {
        return Ok(false);
    }
    let check = if has_manifest {
        replica::check_sealed_copy(&target.path, path, text)
    } else {
        replica::check_file(&data, text)
    };
    let (why, error) = match check {
        Ok(CopyCheck::Good) => return Ok(false),
        Ok(CopyCheck::Manifest { other_file: true }) => {
            report.problems.push(format!(
                "{path} on {} ({}): another self-consistent file under the path (its own \
                 manifest with another text and data that matches it): not moved, since it may \
                 be a second sealed file (D5); the path stays one copy short on this target \
                 until #219 decides it (ADR-062 D5, D11)",
                target.path.display(),
                target.id
            ));
            return Ok(false);
        }
        Ok(CopyCheck::Missing) => ("missing: a manifest without its data".to_owned(), None),
        Ok(CopyCheck::Manifest { other_file: false }) => (
            "its manifest is not the installed text and its data does not match it".to_owned(),
            None,
        ),
        Ok(CopyCheck::Size) => ("size".to_owned(), None),
        Ok(CopyCheck::Sha256) => ("sha256".to_owned(), None),
        Err(error) => (String::new(), Some(error.to_string())),
    };
    if let Some(why) = rebuild::marker_gone(target) {
        return Err(marker_stop(target, &why));
    }
    let aside = replica::damaged_dir(&target.path, at_ms);
    let moved = replica::move_aside(&target.path, path, &aside).map_err(|error| {
        move_stop(
            &format!(
                "move the copy of {path} on {} aside to {}",
                target.path.display(),
                aside.display()
            ),
            &target.path,
            &error,
        )
    })?;
    if moved.is_empty() {
        return Ok(false);
    }
    let names = moved.join(", ");
    match error {
        Some(error) => report.problems.push(format!(
            "{path} on {} ({}): its copy cannot be read ({error}), so it is no verifiable copy: \
             {names} moved aside to {}; the next pass copies the path from hot. {READ_ERROR} \
             (ADR-062 D11)",
            target.path.display(),
            target.id,
            aside.display()
        )),
        None => report.lines.push(format!(
            "{path} on {} ({}): an unrecorded copy the next pass's D5 copy would refuse \
             ({why}): {names} moved aside to {}; the next pass copies the path from hot \
             (ADR-062 D5, D11)",
            target.path.display(),
            target.id,
            aside.display()
        )),
    }
    Ok(true)
}

/// H6 k2 for `path` on the copy target `target`, when k1 moved nothing: a
/// temp name of the path that is no verifiable copy of the installed
/// `text` (an `M~` with other bytes, a `D~` that does not verify, either
/// one unreadable) is moved to `<target>/.mie/damaged/<at_ms>/<partition>/
/// <name>`, since the next pass's D5 copy of the path to that target
/// removes both temp names before it copies. A moved `M~` that parses with
/// another text (row 12 of B6a) and an unreadable one (U) are `FAIL:`
/// lines; every other move is a report line. Returns whether anything
/// moved.
fn temps_aside(
    target: &Marked,
    path: &str,
    text: &str,
    at_ms: i64,
    report: &mut RestoreReport,
    first_move: &mut bool,
) -> Result<bool, Stop> {
    let mut any = false;
    for name in [TempName::Data, TempName::Manifest] {
        let file = replica::temp_path(&target.path, path, name)
            .map_err(|error| Stop(format!("{path}: {error}")))?;
        let (other_text, unreadable) = match name {
            TempName::Manifest => match fs::read(&file) {
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => (false, Some(format!("read {}: {error}", file.display()))),
                Ok(bytes) if bytes == text.as_bytes() => continue,
                Ok(bytes) => (
                    matches!(
                        rebuild::classify_text(path, &bytes),
                        rebuild::RootCopy::Text { .. }
                    ),
                    None,
                ),
            },
            TempName::Data => match replica::check_file(&file, text) {
                Ok(CopyCheck::Missing | CopyCheck::Good) => continue,
                Ok(_) => (false, None),
                Err(error) => (false, Some(error.to_string())),
            },
        };
        if let Some(why) = rebuild::marker_gone(target) {
            return Err(marker_stop(target, &why));
        }
        let rel = file
            .strip_prefix(&target.path)
            .map_err(|error| Stop(format!("{}: {error}", file.display())))?;
        let aside = replica::damaged_dir(&target.path, at_ms).join(rel);
        replica::move_file_aside(&file, &aside).map_err(|error| {
            move_stop(
                &format!("move {} aside to {}", file.display(), aside.display()),
                &target.path,
                &error,
            )
        })?;
        any = true;
        if let Some(error) = unreadable {
            report.problems.push(format!(
                "{} on {} ({}) cannot be read ({error}), so it is no verifiable copy of {path}: \
                 moved aside to {}; the next pass copies the path from hot. {READ_ERROR} \
                 (ADR-062 D11)",
                file.display(),
                target.path.display(),
                target.id,
                aside.display()
            ));
        } else if other_text {
            report.problems.push(format!(
                "{} on {} ({}): a pending manifest with another text than the installed one, \
                 a remnant of an earlier use of {path}: moved aside to {}; the next pass \
                 copies the path from hot (ADR-062 D5, D11)",
                file.display(),
                target.path.display(),
                target.id,
                aside.display()
            ));
        } else {
            report.lines.push(format!(
                "{} on {} ({}): no verifiable copy of {path}: moved aside to {}, since the \
                 next pass removes the temp names of a path it copies (ADR-062 D5)",
                file.display(),
                target.path.display(),
                target.id,
                aside.display()
            ));
        }
        if *first_move {
            *first_move = false;
            reached(Step::SetAside)?;
        }
    }
    Ok(any)
}

/// H6 k for `path`, right after its copy-back succeeded: on every copy
/// target in configuration order, k1 ([`sealed_aside`]), then k2
/// ([`temps_aside`]) when k1 moved nothing (k1's move takes the temp
/// names too). Read-only roots are never written, and a recorded copy is
/// never moved: its damage is R1's, once verify records the finding.
/// Invariant: afterwards the next pass's D5 copy of the path to every copy
/// target succeeds, unless a target holds a self-consistent other file
/// (D5).
fn set_aside(
    targets: &[&Marked],
    state: &CopyState,
    path: &str,
    text: &str,
    at_ms: i64,
    report: &mut RestoreReport,
    first_move: &mut bool,
) -> Result<(), Stop> {
    for target in targets {
        if sealed_aside(target, state, path, text, at_ms, report)? {
            if *first_move {
                *first_move = false;
                reached(Step::SetAside)?;
            }
            continue;
        }
        temps_aside(target, path, text, at_ms, report, first_move)?;
    }
    Ok(())
}

/// The manifest name of a data path.
fn manifest_name(path: &str) -> String {
    format!("{}.manifest", path.strip_suffix(".parquet").unwrap_or(path))
}

/// What one path's copy-back did (H6).
enum Back {
    Copied,
    Present,
    Failed,
}

/// The copy-back of one path (H6, J): [`replica::copy_back`] from the cold
/// roots in configuration order, passing over damaged copies (each a
/// `FAIL:` line with its remedy, round 3), then (k); a path with no
/// verified copy left is the lost-file rule's `FAIL:`; an error on hot's
/// side stops the run.
struct CopyBack<'a> {
    hot: &'a Path,
    cold_paths: &'a [PathBuf],
    marked: &'a [Marked],
    targets: &'a [&'a Marked],
    state: &'a CopyState,
    at_ms: i64,
}

impl CopyBack<'_> {
    /// The remedy of a damaged cold copy of `path` on `root` (round 3):
    /// verify, then repair (R1) only for a recorded copy on a copy target.
    fn remedy(&self, path: &str, root: &Path) -> String {
        match self.marked.iter().find(|marked| marked.path == root) {
            Some(marked) if !marked.copy_target => {
                "the read-only root is never written (ADR-062 D11), so no mie command changes \
                 this copy"
                    .to_owned()
            }
            Some(marked)
                if !self
                    .state
                    .copy(path, &marked.id)
                    .is_some_and(|copy| copy.latest.is_some()) =>
            {
                "`mie store verify` does not check this unrecorded copy; the restore moves it \
                 aside once it has copied the path back (H6 k), and the next pass copies the \
                 path from hot"
                    .to_owned()
            }
            _ => "run `mie store verify`, then `mie store repair`".to_owned(),
        }
    }

    fn path(
        &self,
        path: &str,
        text: &str,
        report: &mut RestoreReport,
        first_move: &mut bool,
    ) -> Result<Back, Stop> {
        let mut damaged: Vec<replica::DamagedCopy> = Vec::new();
        let outcome = replica::copy_back(
            self.cold_paths,
            self.hot,
            path,
            text,
            OtherText::PassOver,
            SourceForms::CrashForms,
            &mut |copy| damaged.push(copy.clone()),
        );
        for copy in &damaged {
            report.problems.push(format!(
                "{path} on {}: {}: the copy does not verify and is passed over; {} (ADR-062 \
                 D11)",
                copy.root.display(),
                copy.error,
                self.remedy(path, &copy.root)
            ));
        }
        match outcome {
            Ok(back) => {
                set_aside(
                    self.targets,
                    self.state,
                    path,
                    text,
                    self.at_ms,
                    report,
                    first_move,
                )?;
                Ok(if back.outcome == CopyOutcome::AlreadyPresent {
                    Back::Present
                } else {
                    Back::Copied
                })
            }
            Err(error) => {
                // A root holds the path when it has its manifest or its
                // pending manifest (H6 i).
                let manifest = manifest_name(path);
                let holders: Vec<&PathBuf> = self
                    .cold_paths
                    .iter()
                    .filter(|root| {
                        fs::symlink_metadata(root.join(&manifest)).is_ok()
                            || replica::temp_path(root, path, TempName::Manifest)
                                .is_ok_and(|pending| fs::symlink_metadata(pending).is_ok())
                    })
                    .collect();
                let no_good_copy = holders
                    .iter()
                    .all(|root| damaged.iter().any(|copy| copy.root == **root));
                if !no_good_copy {
                    return Err(Stop(format!(
                        "{path}: the copy back onto hot failed on hot's side ({error}); the \
                         restore stops before hot's marker and completes once hot accepts the \
                         copy (ADR-062 D11)"
                    )));
                }
                // A re-run after (k) moved the only cold source aside: hot
                // already holds the path, verified against the installed
                // text, as an uncrashed run left it.
                if matches!(
                    replica::check_sealed_copy(self.hot, path, text),
                    Ok(CopyCheck::Good)
                ) {
                    set_aside(
                        self.targets,
                        self.state,
                        path,
                        text,
                        self.at_ms,
                        report,
                        first_move,
                    )?;
                    return Ok(Back::Present);
                }
                report.problems.push(format!(
                    "{path}: no cold root holds a verified copy ({error}); it stays recorded, so \
                     every selection that covers it fails with Integrity: the lost-file rule \
                     (ADR-062 D11 step 5)"
                ));
                Ok(Back::Failed)
            }
        }
    }
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
    // A target's ledger copy that cannot be read is rejected like one that
    // does not parse (U, #195 amendment 2); hot's own is hot's side.
    let mut ledger_copies = Vec::new();
    for target in &targets {
        let path = copy_ledger::ledger_path(&target.path);
        let (bytes, error) = match fs::read(&path) {
            Ok(bytes) => (Some(bytes), None),
            Err(error) if error.kind() == ErrorKind::NotFound => (None, None),
            Err(error) => (None, Some(format!("read {}: {error}", path.display()))),
        };
        ledger_copies.push(LedgerCopyRead {
            id: target.id.clone(),
            bytes,
            error,
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

    // 2. The journal candidates: hot's, then each target's copy; their
    // lengths only (H3′ reads them).
    let mut candidates: Vec<(JournalCandidate, PathBuf, Option<&Marked>)> = Vec::new();
    match fs::metadata(journal_path) {
        Ok(metadata) => candidates.push((
            JournalCandidate {
                label: format!("hot's journal {}", journal_path.display()),
                len: metadata.len(),
            },
            journal_path.to_owned(),
            None,
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
            Some(*target),
        ));
    }

    // 3. The import ledger: hot's (when it exists) and every target's.
    let mut files: BTreeMap<String, Vec<LedgerFileCopy>> = BTreeMap::new();
    // The import-ledger files that cannot be read: `(name, root, path,
    // error)`, damaged copies, never a source (U, #195 amendment 2).
    let mut unreadable_files: Vec<(String, String, PathBuf, String)> = Vec::new();
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
                let bytes = match fs::read(&path) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        unreadable_files.push((name, root.clone(), path, error.to_string()));
                        continue;
                    }
                };
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
        let copy = rebuild::read_copy(hot, &path, false, false)?;
        hot_read.files.insert(path, copy);
    }
    hot_read.orphan_data = rebuild::orphan_data(hot).map_err(|e| listing(hot, e))?;
    hot_read.evidence = replica::list_evidence(hot).map_err(|e| listing(hot, e))?;
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
        let check = |path: &str| {
            root.copy_target
                && !start
                    .copy(path, &root.id)
                    .is_some_and(|copy| copy.latest.is_some())
        };
        // U: a manifest that cannot be read is a damaged copy, and the
        // data of every crash form on a root without the manifest is
        // checked (rule 1′), recorded or not.
        for path in replica::list_sealed(&root.path).map_err(|e| listing(&root.path, e))? {
            let copy = rebuild::read_copy(&root.path, &path, check(&path), true)?;
            root_files.insert(path, copy);
        }
        let temps = rebuild::read_temps(&root.path, &root_files, &|_| true)?;
        roots.push(RootRead {
            id: root.id.clone(),
            copy_target: root.copy_target,
            files: root_files,
            orphan_data: rebuild::orphan_data(&root.path).map_err(|e| listing(&root.path, e))?,
            temps,
            evidence: replica::list_evidence(&root.path).map_err(|e| listing(&root.path, e))?,
            quarantine: replica::list_quarantine(&root.path)
                .map_err(|e| quarantine(&root.path, e))?,
        });
    }

    // U: a quarantined manifest that cannot be read is counted without its
    // text, as one that is not UTF-8, with a `FAIL:`.
    let mut unreadable_quarantine = Vec::new();
    let listings = std::iter::once((HOT_ID, hot, &hot_read.quarantine)).chain(
        marked
            .iter()
            .zip(&roots)
            .map(|(root, read)| (root.id.as_str(), root.path.as_path(), &read.quarantine)),
    );
    for (id, root, listing) in listings {
        for (qid, parts) in &listing.ids {
            for (path, copy) in parts {
                if let Some(error) = &copy.unreadable {
                    unreadable_quarantine.push(format!(
                        "{path} in {} on {id}: its quarantined manifest cannot be read ({error}); \
                         the part is counted without its text. {READ_ERROR} (ADR-062 D7 step 5a, \
                         D11)",
                        replica::quarantine_dir(root, qid).display()
                    ));
                }
            }
        }
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
                .flat_map(|root| {
                    root.files
                        .keys()
                        .chain(&root.orphan_data)
                        .chain(root.temps.keys())
                })
                .map(String::as_str),
        )
        .filter(|path| copy_ledger::is_data_path(path))
        .map(partition_of)
        .collect();
    mode.copy_back = plan_copy_back(named, first_hot_date, &|partition| {
        start.is_removed(partition)
    });

    // H3′: the journal, decided before anything is written. A candidate
    // left out goes aside: hot's own journal beside itself (a rename
    // across filesystems would fail, and hot's journal survives a hot
    // loss mainly on another disk), a target's copy where R7 puts one.
    let journal_aside = |i: usize| -> PathBuf {
        match candidates[i].2 {
            None => {
                let mut name = journal_path.as_os_str().to_owned();
                name.push(format!(".unreadable-{at_ms}"));
                PathBuf::from(name)
            }
            Some(target) => replica::damaged_dir(&target.path, at_ms)
                .join(META_DIR)
                .join(JOURNAL_DIR)
                .join(&journal_name),
        }
    };
    let labels: Vec<JournalCandidate> = candidates.iter().map(|(c, _, _)| c.clone()).collect();
    let choice = choose_journal(
        &labels,
        flags.skip_unreadable_journal,
        &mut |i| {
            let (candidate, path, _) = &candidates[i];
            let mut bytes = Vec::new();
            fs::File::open(path)
                .and_then(|mut file| Read::take(&mut file, candidate.len).read_to_end(&mut bytes))
                .map_err(|error| error.to_string())?;
            if bytes.len() as u64 != candidate.len {
                return Err(format!(
                    "{} of its {} bytes read",
                    bytes.len(),
                    candidate.len
                ));
            }
            Ok(bytes)
        },
        &mut |i, c, len| {
            replica::first_difference(&candidates[i].1, TailSource::Bytes(c), len)
                .map_err(|error| error.error.to_string())
        },
        &|i| journal_aside(i).display().to_string(),
    );
    let choice = match choice {
        Ok(choice) => choice,
        Err(mut stops) => {
            let last = stops.pop().unwrap_or_default();
            report.problems.extend(stops);
            return Err(Stop(last));
        }
    };
    for (candidate, _, _) in &candidates {
        report.lines.push(format!(
            "journal candidate: {}, {} bytes",
            candidate.label, candidate.len
        ));
    }
    let longest = choice.chosen.map_or(0, |c| candidates[c].0.len);
    let restored = choice.chosen.map_or_else(
        || "stays absent".to_owned(),
        |c| {
            format!(
                "is restored from {} ({longest} bytes)",
                candidates[c].0.label
            )
        },
    );
    report.lines.push(match choice.chosen {
        None if candidates.is_empty() => {
            "journal: no candidate on hot or on any copy target; the journal stays absent"
                .to_owned()
        }
        None => "journal: no candidate reads; the journal stays absent".to_owned(),
        Some(i) => format!("journal: chosen {}", candidates[i].0.label),
    });
    let mut journal_fails = Vec::new();
    for out in &choice.left_out {
        let (candidate, path, target) = &candidates[out.index];
        let aside = journal_aside(out.index);
        let place = match target {
            None => format!(
                "beside paths.journal: moved to {}, never deleted",
                aside.display()
            ),
            Some(target) if out.by_flag => format!(
                "on {}: moved to {}, never deleted",
                target.id,
                aside.display()
            ),
            Some(target) => format!("on {}, in place ({})", target.id, path.display()),
        };
        let read_error = if target.is_some() {
            format!(" {READ_ERROR}.")
        } else {
            String::new()
        };
        journal_fails.push(if out.by_flag {
            format!(
                "the journal candidate {} ({} bytes) cannot be read ({}). \
                 `--skip-unreadable-journal` left it out, and hot's journal {restored}. The \
                 runs that only its last {} byte(s) name are missing from hot's journal, and a \
                 file of theirs that neither a root nor the copy ledger holds gets no `lost` \
                 record. Its bytes stay {place}.{read_error} (ADR-062 D11 step 2)",
                candidate.label,
                candidate.len,
                out.error,
                candidate.len - longest
            )
        } else {
            format!(
                "the journal candidate {} ({} bytes) cannot be read ({}); it is no longer than \
                 the chosen candidate, so only its cross-check is lost: it is left out, and \
                 hot's journal {restored}. Its bytes stay {place}.{read_error} (ADR-062 D11 \
                 step 2)",
                candidate.label, candidate.len, out.error
            )
        });
    }
    // The moves of H3′ step 5 and 6: hot's journal whenever it is left
    // out, a target's copy only when the flag left it out.
    let journal_moves: Vec<(PathBuf, PathBuf, Option<&Marked>)> = choice
        .left_out
        .iter()
        .filter(|out| out.by_flag || candidates[out.index].2.is_none())
        .map(|out| {
            let (_, path, target) = &candidates[out.index];
            (path.clone(), journal_aside(out.index), *target)
        })
        .collect();
    let mut journal_bytes = choice.bytes;
    let chosen = choice.chosen;
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
    let mut plan = plan_import_ledger(&files, &decided.chosen_entries);
    // U: an unreadable import-ledger file is a damaged copy, never a
    // source; the readable copies decided above. Hot's is moved aside, as
    // H4 moves a losing variant; a target's is named for verify, then
    // repair (R8).
    let mut unreadable_fails = Vec::new();
    for (name, root, path, error) in &unreadable_files {
        let left = if files.get(name).is_some_and(|copies| !copies.is_empty()) {
            String::new()
        } else {
            " No readable copy of it is left, so the archive parts it lists are held `unlisted` \
             until `mie store resolve` decides them."
                .to_owned()
        };
        if root == HOT_ID {
            plan.set_aside.push(name.clone());
            unreadable_fails.push(format!(
                "import-ledger file {name} on hot ({}) cannot be read ({error}): a damaged copy, \
                 never a source; it is moved aside to {}, never deleted.{left} (ADR-062 D11)",
                path.display(),
                replica::damaged_dir(hot, at_ms)
                    .join(IMPORT_LEDGER_DIR)
                    .join(name)
                    .display()
            ));
        } else {
            unreadable_fails.push(format!(
                "import-ledger file {name} on {root} ({}) cannot be read ({error}): a damaged \
                 copy, never a source; run `mie store verify`, then `mie store repair` for it \
                 (R8). A read error can mean a failing disk.{left} (ADR-062 D11)",
                path.display()
            ));
        }
    }
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
    report.problems.extend(journal_fails);
    report.problems.extend(unreadable_fails);
    report.problems.extend(unreadable_quarantine);
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

    // H7: no evidence of this `at_ms` may exist before the first write,
    // the asides of H3′'s moves included.
    let hot_damaged = replica::damaged_dir(hot, at_ms);
    let target_asides: Vec<PathBuf> = targets
        .iter()
        .map(|target| replica::damaged_dir(&target.path, at_ms).join("copy-ledger"))
        .collect();
    let moved_journals = journal_moves.iter().map(|(_, aside, _)| aside);
    for aside in std::iter::once(&hot_damaged)
        .chain(&target_asides)
        .chain(moved_journals)
    {
        if fs::symlink_metadata(aside).is_ok() {
            return Err(Stop(format!(
                "{} exists: evidence is never overwritten; the restore stops with nothing written, \
                 and a re-run, with a new `at_ms`, completes it (ADR-062 D7)",
                aside.display()
            )));
        }
    }

    // H7 0 (H3′): the journal candidates left out are moved first, so none
    // of them meets hot's written journal again.
    for (path, aside, target) in &journal_moves {
        let root = match target {
            Some(target) => {
                if let Some(why) = rebuild::marker_gone(target) {
                    return Err(marker_stop(target, &why));
                }
                target.path.as_path()
            }
            None => journal_path.parent().unwrap_or(journal_path),
        };
        replica::move_file_aside(path, aside).map_err(|error| {
            move_stop(
                &format!(
                    "move the journal candidate {} aside to {}",
                    path.display(),
                    aside.display()
                ),
                root,
                &error,
            )
        })?;
    }
    if !journal_moves.is_empty() {
        reached(Step::JournalAside)?;
    }

    // H7 1: hot's journal (H3).
    let hot_accepts = |what: String| {
        move |error: String| {
            Stop(format!(
                "{what}: {error}; the restore stops before hot's marker and completes once hot \
                 accepts the write (ADR-062 D11)"
            ))
        }
    };
    if let Some(i) = chosen {
        let (candidate, path, target) = &candidates[i];
        let appended = if target.is_none() {
            0
        } else {
            replica::extend_prefix(TailSource::File(path), journal_path, candidate.len)
                .map_err(|error| error.to_string())
                .map_err(hot_accepts(format!(
                    "extend hot's journal {} from {}",
                    journal_path.display(),
                    candidate.label
                )))?
        };
        report.lines.push(format!(
            "journal: hot's journal {} extended by {appended} byte(s)",
            journal_path.display()
        ));
        if terminate {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(journal_path)
                .map_err(|e| e.to_string())
                .map_err(hot_accepts(format!("open {}", journal_path.display())))?;
            file.write_all(b"\n")
                .and_then(|()| file.sync_all())
                .map_err(|e| e.to_string())
                .map_err(hot_accepts(format!("append to {}", journal_path.display())))?;
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
        fs::create_dir_all(dir)
            .map_err(|e| e.to_string())
            .map_err(hot_accepts(format!("create {}", dir.display())))?;
        for name in &plan.set_aside {
            let aside = hot_damaged.join(IMPORT_LEDGER_DIR).join(name);
            replica::move_file_aside(&dir.join(name), &aside).map_err(|error| {
                move_stop(
                    &format!(
                        "move hot's import-ledger file {name} aside to {}",
                        aside.display()
                    ),
                    dir,
                    &error,
                )
            })?;
        }
        for (name, source) in &plan.writes {
            replica::copy_immutable(source, &dir.join(name))
                .map_err(|error| error.error.to_string())
                .map_err(hot_accepts(format!(
                    "write import-ledger file {name} to hot"
                )))?;
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
                 as absent files, so the restore stops before the install and completes once the \
                 root presents its marker (ADR-062 D7 step 2)",
                root.path.display(),
                root.id
            )));
        }
    }
    let replace = hot_ledger.as_deref() != Some(derived.ledger.as_slice());
    if replace {
        let hot_aside = hot_damaged.join("copy-ledger");
        hold.replace(&derived.ledger, Some(&hot_aside))
            .map_err(|error| error.to_string())
            .map_err(hot_accepts("the install failed".to_owned()))?;
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
    let copy_back = CopyBack {
        hot,
        cold_paths: &cold_paths,
        marked: &marked,
        targets: &targets,
        state: &state,
        at_ms,
    };
    let mut first = true;
    let mut first_move = true;
    for (partition, paths) in &by_partition {
        let (mut copied, mut present, mut failed) = (0usize, 0usize, 0usize);
        for (i, (path, text)) in paths.iter().enumerate() {
            match copy_back.path(path, text, report, &mut first_move)? {
                Back::Copied => copied += 1,
                Back::Present => present += 1,
                Back::Failed => failed += 1,
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
                .map_err(|error| error.error.to_string())
                .map_err(hot_accepts(format!("append `returned {partition}`")))?;
            if let Some(truncation) = outcome.truncation {
                report
                    .lines
                    .push(replicate::truncation_line(&hot_ledger_path, truncation));
            }
            line.push_str("; returned");
        }
        report.lines.push(line);
    }
    // J (#195 amendment 2): the unreplicated paths of removed partitions
    // outside the plan, one by one from the installed ledger, with no
    // `returned`: the hot removal finishes the old removal once each path's
    // k copies are recorded and verified (removal rules (e), (f)). A name
    // check error other than not-found counts as present, so the copy-back
    // tries that root and reports what it finds. Hot's copy counts only
    // when it verifies: the copy-back then finds no cold source and takes
    // its branch for a path hot already holds, which runs (k) (round 4).
    let holds = |path: &str, form: Held| {
        let cold = |root: &PathBuf| {
            let file = match form {
                Held::Manifest => Ok(root.join(manifest_name(path))),
                Held::PendingManifest => replica::temp_path(root, path, TempName::Manifest),
                Held::Data | Held::VerifiedOnHot => Ok(root.join(path)),
            };
            file.is_ok_and(|file| {
                !matches!(fs::symlink_metadata(file), Err(e) if e.kind() == ErrorKind::NotFound)
            })
        };
        match form {
            Held::VerifiedOnHot => state.ledger_text(path).is_some_and(|text| {
                matches!(
                    replica::check_sealed_copy(hot, path, text),
                    Ok(CopyCheck::Good)
                )
            }),
            _ => cold_paths.iter().any(cold),
        }
    };
    for path in plan_unreplicated(&state, &mode.copy_back, &holds) {
        let Some(text) = state.ledger_text(&path) else {
            continue;
        };
        let back = copy_back.path(&path, text, report, &mut first_move)?;
        report.lines.push(format!(
            "copy-back {path}: {}",
            match back {
                Back::Copied => "copied, a path of a removed partition no copy target recorded",
                Back::Present => "already on hot",
                Back::Failed => "no good copy",
            }
        ));
        if first {
            first = false;
            reached(Step::FirstCopy)?;
        }
    }
    reached(Step::Returned)?;

    // H7 5: every marker re-read, then hot's marker, the commit.
    for root in &marked {
        if let Some(why) = rebuild::marker_gone(root) {
            return Err(marker_stop(root, &why));
        }
    }
    reached(Step::BeforeMarker)?;
    root_marker::publish(hot, HOT_ID)
        .map_err(|error| error.to_string())
        .map_err(hot_accepts("write hot's marker".to_owned()))?;
    report.restored = true;
    report.lines.push(format!(
        "wrote {} (id {HOT_ID})",
        root_marker::marker_path(hot).display()
    ));
    Ok(())
}
