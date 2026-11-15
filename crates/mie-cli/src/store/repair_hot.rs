//! The repair of hot's own files on an intact hot (ADR-065, #217): `mie
//! store repair-hot`.
//!
//! `mie store repair` never rewrites hot, and `mie store restore` is for a
//! lost hot only. This command repairs hot's journal (`--scope journal`)
//! or hot's import-ledger files (`--scope import-ledger`) from the copy
//! targets, one scope per run. It is built like the restore: refusals and
//! whole-run locks, then every read, then a pure decision, then the
//! writes. It is all or nothing: a difference it cannot attribute stops the
//! run before the first write, naming both sides. It never deletes: a file
//! of hot's it replaces is first hard-linked to an evidence name on its own
//! filesystem, then the new bytes are renamed over it, so hot never lacks
//! the file, even after a crash. The rules, as numbered in the plan of
//! #217 (the tests cite them):
//!
//! - **X0 Usage, refusals, locks.** Refused, nothing written: a missing or
//!   unknown `--scope`; `--keep-hot-journal` with `--scope import-ledger`;
//!   an empty `cold_roots`; a hot without its marker or with a cold root's
//!   ([`check_hot`], which names `mie store restore`); a cold root with
//!   hot's id, one id on two roots, hot and the copy targets not on
//!   pairwise distinct devices; journal scope: a `paths.journal` without a
//!   file name or that is a dangling link; import-ledger scope: no archive
//!   configuration or another store's, a missing hot copy ledger (names
//!   `mie store rebuild`), an import-ledger path that is a dangling link. A
//!   copy target without a readable marker is skipped and named, never read
//!   or written; read-only roots are never witnesses. Locks, each without
//!   waiting, for the whole run: the pass lock, the verify lock, then the
//!   writer lock of `instrument.source` and the journal lock (journal
//!   scope: they exclude `mie ingest`, the journal lock until its last
//!   journal line, since the source's lock goes when its store closes,
//!   before `run_end`; the pass lock excludes `mie store adopt`, the
//!   journal's only other writer) or the writer lock of the archive source
//!   (import-ledger scope: it excludes the importer, `mie store resolve`
//!   and the hot removal of archive partitions). The import-ledger scope leaves the capture
//!   running: no live writer touches the import ledger, and every minute of
//!   a stopped capture is irreplaceable live data.
//! - **X1 Journal reads.** The witnesses are the marked copy targets'
//!   `.mie/journal/<file name of paths.journal>`. C is the longest witness
//!   that reads in full, configuration order on a tie, and every other
//!   readable witness must be its byte prefix
//!   ([`restore::choose_journal_stops`]); an unreadable witness is no
//!   witness. Hot's journal is `paths.journal` canonicalized (a link is
//!   followed, its target replaced); d and e are the first and the last
//!   byte where it differs from C over min(L_h, L_C)
//!   ([`replica::difference_bounds`]); a read error on it counts as a
//!   difference at every byte. L_2 is the second-longest witness's length,
//!   lls(C) the start of C's last complete line.
//! - **X2 The journal decision** ([`decide_journal`]): *agree* (no
//!   difference, L_C ≤ L_h, and hot does not equal a C that ends inside a
//!   line: nothing written); *behind* (no difference, L_h < L_C, or L_h =
//!   L_C with C ending inside a line, the state an extension leaves when
//!   its `\n` did not land: hot is extended to C, one witness suffices, as
//!   in the restore's H3); *damaged* (a difference, L_C = L_h, e < L_2, no
//!   fork shape: hot is replaced by C, since every byte where hot differs
//!   has two independent witnesses against it). Every other state stops,
//!   naming both sides: *copy-damaged* (the copies differ, and R7 rebuilds
//!   every copy that is no byte prefix of hot under ADR-065 D8, the same
//!   rule over the same bytes: `mie store verify`, then `mie store
//!   repair`); *two-fault* (the copies differ otherwise); *hot ahead* (L_C
//!   < L_h, e < lls(C): stop `mie ingest`, run `mie store replicate`, run
//!   this again); *diverged* (L_h > L_C with e ≥ lls(C), L_C > L_h with a
//!   difference, or a fork shape); *one-witness* (L_C = L_h, e ≥ L_2). A
//!   written journal that ends inside a line gets one `\n`.
//! - **X2a The fork guard** ([`fork_shaped`]): before *damaged* writes, an
//!   equal-length fork (lines of a later run where the copies hold others)
//!   makes the state *diverged*. It errs toward stopping.
//! - **X3 `--keep-hot-journal`**: in *diverged*, *one-witness*,
//!   *copy-damaged* and *two-fault* only, with hot's journal readable in
//!   full, every witness that is no prefix of hot's journal is rebuilt from
//!   hot up to the start of the line that holds its first difference
//!   ([`replica::rebuild_prefix`]), its bytes kept in its
//!   `.mie/damaged/<at_ms>/`; the next pass extends it. Hot is not written.
//! - **X4 The import ledger** ([`plan_hot_import_ledger`]): per name, hot's
//!   copy that is *good* (R8's test, [`entry_mismatches`], for a current
//!   entry; the SHA-256 a supersede record names, for a superseded one; its
//!   own file named, for a supersede record) is kept; otherwise exactly one
//!   good byte variant among the target copies is written to hot; none, or
//!   two, stops naming every copy and, with no good copy anywhere, the
//!   closure by hand. A name the readers fail closed on (a malformed
//!   `.import` name) is moved to the evidence directory; every other
//!   malformed name and `PENDING` are named and left.
//! - **X5 Writes, evidence, crash states.** One clock read gives `at_ms`;
//!   every decision is made and every evidence name checked before the
//!   first write. Evidence: `<paths.journal>.damaged-<at_ms>`, and
//!   `<import-ledger dir>.damaged-<at_ms>/<segment>/<name>`, beside the
//!   files, since a hard link needs their own filesystem. The replace is
//!   [`replica::replace_with_evidence`].
//! - **X6 Report and exit**: the witnesses and the decision, each
//!   import-ledger name that is not good and identical everywhere, the
//!   evidence paths, then `repaired: <n> file(s) written` or `nothing
//!   written`. `PASS` needs no stop, no skipped target and no unreadable
//!   witness.

use super::repair::entry_mismatches;
use super::replicate::{self, IMPORT_LEDGER_DIR, JOURNAL_DIR, META_DIR, sorted_names};
use super::restore::{self, JournalCandidate, JournalStop, LedgerListing, READ_ERROR, list_ledger};
use super::{DeviceProbe, check_devices, check_hot};
use crate::config::IngestConfig;
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_binance::archive::ledger::{
    ImportLedger, LedgerKind, LedgerName, PENDING_FILE, Pending, SupersedeRecord, SupersededEntry,
    classify, current_generation,
};
use mie_adapter_parquet::copy_ledger;
use mie_adapter_parquet::removal;
use mie_adapter_parquet::replica::{self, ReplaceEvidenceStep, Side, TailSource};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::verification::CopyState;
use mie_ports::raw::RawStoreError;
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod run_tests;

/// The remedy the problems of hot's import ledger name (X7, ADR-065): the
/// readers fail an archive file closed on one of hot's own files, and only
/// this command restores it. Appended at the call sites of
/// [`replicate::hot_entries`] in the pass, the verification and the repair
/// (the adoption reads another store's ledger with the same function).
pub(super) fn with_remedy(problem: String) -> String {
    format!(
        "{problem}. When hot's own import-ledger file is damaged, missing or misnamed while a \
         copy target holds a good copy, `mie store repair-hot --scope import-ledger` restores \
         it, with the importer stopped (ADR-065)"
    )
}

/// What one run repairs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Hot's journal (`--scope journal`).
    Journal,
    /// Hot's import-ledger files (`--scope import-ledger`).
    ImportLedger,
}

impl Scope {
    /// Parses `--scope` (X0).
    ///
    /// # Errors
    ///
    /// A description naming both scopes for a missing or unknown value.
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        match value {
            Some("journal") => Ok(Self::Journal),
            Some("import-ledger") => Ok(Self::ImportLedger),
            Some(other) => Err(format!(
                "--scope {other:?} is neither journal nor import-ledger: one scope per run \
                 (ADR-065 D1)"
            )),
            None => Err(
                "--scope journal|import-ledger is required: one scope per run, since the journal \
                 scope needs `mie ingest` stopped and the import-ledger scope does not (ADR-065 \
                 D1)"
                .to_owned(),
            ),
        }
    }
}

/// The flags of `mie store repair-hot`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Flags {
    /// `--keep-hot-journal` (X3, ADR-065 D4).
    pub keep_hot_journal: bool,
}

/// What one run did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RepairHotReport {
    /// The report (X6), ending in `repaired: …` or `nothing written`.
    pub lines: Vec<String>,
    /// Every refusal, stop and finding; empty when the run passed.
    pub problems: Vec<String>,
    /// The files written: replaced, created, extended, moved aside or
    /// rebuilt.
    pub written: usize,
}

/// The write steps (X5). Tests stop the run after any of them, once, as a
/// crash would.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// A file of hot's is linked to its evidence name.
    Linked,
    /// Its new bytes are in the temp name.
    Temp,
    /// The new bytes are renamed over it.
    Renamed,
    /// The first malformed import-ledger name is moved aside.
    MalformedMoved,
    /// The first copy is rebuilt by `--keep-hot-journal`.
    CopyRebuilt,
    /// *behind* extended hot's journal to C, before its closing `\n`.
    Extended,
}

#[cfg(test)]
thread_local! {
    /// Test hook: the run stops after this step, once.
    pub(crate) static AFTER_STEP: std::cell::Cell<Option<Step>> =
        const { std::cell::Cell::new(None) };
    /// Test hook: runs once after every decision, before the first write
    /// of `--keep-hot-journal` (an unmount mid-run).
    pub(crate) static BEFORE_WRITES: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
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

/// The replace's step hook: the steps inside
/// [`replica::replace_with_evidence`] are crash points too.
fn replace_step(step: ReplaceEvidenceStep) -> Result<(), RawStoreError> {
    let step = match step {
        ReplaceEvidenceStep::Linked => Step::Linked,
        ReplaceEvidenceStep::Temp => Step::Temp,
    };
    reached(step).map_err(|Stop(why)| RawStoreError::Io(why))
}

/// A stop: the run ends with this problem.
struct Stop(String);

// -------------------------------------------------------- pure: journal

/// One readable journal witness (X1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitnessFacts {
    /// Its copy target's id.
    pub id: String,
    /// Its length.
    pub len: u64,
    /// The first byte in `[0, min(len, L_h))` where it differs from hot's
    /// journal; `None` when it is consistent with it.
    pub vs_hot: Option<u64>,
    /// Per witness, the first byte in `[0, min(len, theirs))` where the two
    /// differ; `None` when one is a byte prefix of the other (itself
    /// included).
    pub vs_others: Vec<Option<u64>>,
    /// Whether `mie store repair`'s R7 refuses to rebuild it from hot
    /// (ADR-065 D8): it is no byte prefix of hot's journal, and hot holds
    /// no strict majority at some byte the rebuild would write
    /// ([`replica::first_contested_file`] over every witness), or hot
    /// cannot be read. False for a byte prefix of hot's journal. Read only
    /// where the copies differ.
    pub r7_refuses: bool,
}

/// What X2 decides on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalFacts {
    /// L_h, 0 for an absent journal.
    pub hot_len: u64,
    /// The readable witnesses, in configuration order.
    pub witnesses: Vec<WitnessFacts>,
    /// C's index among them; `None` without a witness or when the copies
    /// differ.
    pub chosen: Option<usize>,
    /// d and e, the first and the last byte where hot differs from C over
    /// min(L_h, L_C).
    pub bounds: Option<(u64, u64)>,
    /// lls(C): where C's last complete line starts.
    pub last_line_start: u64,
    /// Whether C ends inside a line (it is not empty and its last byte is
    /// no `\n`).
    pub cut: bool,
    /// X2a's verdict, computed only where it decides (L_C = L_h, a
    /// difference, e < L_2, hot readable).
    pub fork: bool,
    /// k, the copy targets configured.
    pub copy_targets: usize,
}

impl JournalFacts {
    /// L_C, 0 without C.
    #[must_use]
    pub fn chosen_len(&self) -> u64 {
        self.chosen.map_or(0, |c| self.witnesses[c].len)
    }

    /// L_2, the second-longest witness's length (0 with one).
    #[must_use]
    pub fn second_len(&self) -> u64 {
        let mut lens: Vec<u64> = self.witnesses.iter().map(|w| w.len).collect();
        lens.sort_unstable_by(|a, b| b.cmp(a));
        lens.get(1).copied().unwrap_or(0)
    }

    /// Whether every pair of witnesses is consistent (one a byte prefix of
    /// the other).
    fn copies_agree(&self) -> bool {
        self.witnesses
            .iter()
            .all(|w| w.vs_others.iter().all(Option::is_none))
    }
}

/// X2's decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalDecision {
    /// No difference, L_C ≤ L_h: nothing is written.
    Agree,
    /// No difference, L_C > L_h, or L_C = L_h with C ending inside a line:
    /// hot is extended to C and ended with its `\n`.
    Behind,
    /// Two witnesses against hot at every differing byte: hot is replaced
    /// by C.
    Damaged,
    /// Stop: the copies differ, and R7 rebuilds every copy that is no byte
    /// prefix of hot (`mie store verify`, then `mie store repair`).
    CopyDamaged,
    /// Stop: the copies differ, and hot differs too, or past L_h. With
    /// k ≥ 3, `group` names copy targets whose journals are byte prefixes
    /// of one another, at least two.
    TwoFault {
        /// The witnesses of a configuration to run with.
        group: Option<Vec<usize>>,
    },
    /// Stop: hot is longer and differs before the copies' last line; the
    /// next pass can extend the copies.
    HotAhead,
    /// Stop: each side holds bytes the other lacks.
    Diverged,
    /// Stop: one witness against hot past L_2.
    OneWitness,
}

impl JournalDecision {
    /// Its name in the report.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Agree => "agree",
            Self::Behind => "behind",
            Self::Damaged => "damaged",
            Self::CopyDamaged => "copy-damaged",
            Self::TwoFault { .. } => "two-fault",
            Self::HotAhead => "hot-ahead",
            Self::Diverged => "diverged",
            Self::OneWitness => "one-witness",
        }
    }

    /// Whether `--keep-hot-journal` may act on it (X3).
    #[must_use]
    pub fn keeps_hot(&self) -> bool {
        matches!(
            self,
            Self::Diverged | Self::OneWitness | Self::CopyDamaged | Self::TwoFault { .. }
        )
    }
}

/// The copy-damaged test (X2): R7 rebuilds every witness that is no byte
/// prefix of hot's journal (ADR-065 D8, `r7_refuses`), so after `mie store
/// verify` and `mie store repair` every copy is a byte prefix of hot's
/// journal. The test is the rule R7 applies, over the same bytes: the
/// two commands never disagree about which copies a repair heals.
fn copy_damaged(facts: &JournalFacts) -> bool {
    facts.witnesses.iter().all(|w| !w.r7_refuses)
}

/// The largest set of witnesses whose journals are byte prefixes of one
/// another, at least two, when it leaves a witness out (configuration
/// order on a tie).
fn consistent_group(facts: &JournalFacts) -> Option<Vec<usize>> {
    let witnesses = &facts.witnesses;
    let mut best: Option<Vec<usize>> = None;
    for a in 0..witnesses.len() {
        let mut group = vec![a];
        for w in (0..witnesses.len()).filter(|&w| w != a) {
            if group.iter().all(|&g| witnesses[g].vs_others[w].is_none()) {
                group.push(w);
            }
        }
        group.sort_unstable();
        if group.len() >= 2
            && group.len() < witnesses.len()
            && best.as_ref().is_none_or(|b| group.len() > b.len())
        {
            best = Some(group);
        }
    }
    best
}

/// X2 (ADR-065 D2): the journal decision. Pure.
#[must_use]
pub fn decide_journal(facts: &JournalFacts) -> JournalDecision {
    if !facts.copies_agree() {
        if copy_damaged(facts) {
            return JournalDecision::CopyDamaged;
        }
        let group = (facts.copy_targets >= 3)
            .then(|| consistent_group(facts))
            .flatten();
        return JournalDecision::TwoFault { group };
    }
    let (hot, chosen) = (facts.hot_len, facts.chosen_len());
    let Some((_, e)) = facts.bounds else {
        // Hot equal to C that ends inside a line is the state *behind*
        // leaves when its last write did not land (review of #226, finding
        // 5): the re-run ends it with its `\n`.
        return if chosen < hot || (chosen == hot && !(facts.cut && facts.chosen.is_some())) {
            JournalDecision::Agree
        } else {
            JournalDecision::Behind
        };
    };
    if hot > chosen {
        return if e < facts.last_line_start {
            JournalDecision::HotAhead
        } else {
            JournalDecision::Diverged
        };
    }
    if chosen > hot {
        return JournalDecision::Diverged;
    }
    if e >= facts.second_len() {
        return JournalDecision::OneWitness;
    }
    if facts.fork {
        JournalDecision::Diverged
    } else {
        JournalDecision::Damaged
    }
}

/// Where the last complete (`\n`-terminated) line of `bytes` starts, as
/// [`replica::extend_prefix`] compares it: after the line end before the
/// one that closes it, else 0.
#[must_use]
pub fn last_line_start(bytes: &[u8]) -> u64 {
    let mut ends = bytes
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, b)| **b == b'\n')
        .map(|(i, _)| i);
    let _closing = ends.next();
    ends.next().map_or(0, |i| i as u64 + 1)
}

/// Where the line holding byte `offset` of `bytes` starts.
fn line_start_in(bytes: &[u8], offset: u64) -> u64 {
    let end = usize::try_from(offset)
        .unwrap_or(usize::MAX)
        .min(bytes.len());
    bytes[..end]
        .iter()
        .rposition(|b| *b == b'\n')
        .map_or(0, |i| i as u64 + 1)
}

// ----------------------------------------------------- pure: fork guard

/// What X2a reads besides hot's lines.
#[derive(Debug, Clone, Copy)]
pub struct ForkFacts<'a> {
    /// C's bytes; L = L_h = L_C.
    pub c: &'a [u8],
    /// d, the first byte where hot differs from C.
    pub d: u64,
    /// e, the last byte where hot differs from C.
    pub e: u64,
}

/// The `run_id` of `line` (without its `\n`) when it is a record: a JSON
/// object whose `type` and `run_id` are strings and whose `at_ms` is an
/// integer, the envelope `Journal::write` gives every line. Not the type
/// table: a line type added later must not turn a fork into damage.
fn record_run_id(line: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(line).ok()?;
    let object = value.as_object()?;
    object.get("type")?.as_str()?;
    let at_ms = object.get("at_ms")?;
    if !(at_ms.is_i64() || at_ms.is_u64()) {
        return None;
    }
    object.get("run_id")?.as_str().map(str::to_owned)
}

/// Whether `line` (with no `\n`, the journal's end) is a torn record: it
/// starts with `{` and its parse fails at the end of the input, the line a
/// crash leaves; or it is a whole record whose `\n` the crash cut. Zero
/// bytes fail with a syntax error instead.
fn torn_record(line: &[u8]) -> bool {
    if line.first() != Some(&b'{') {
        return false;
    }
    match serde_json::from_slice::<serde_json::Value>(line) {
        Ok(_) => record_run_id(line).is_some(),
        Err(error) => error.classify() == serde_json::error::Category::Eof,
    }
}

/// The `run_id` of C's line that starts at `start`, when a line of C
/// starts there and is a complete record.
fn c_run_id_at(c: &[u8], start: u64) -> Option<String> {
    let start = usize::try_from(start).ok()?;
    if start > 0 && c.get(start - 1) != Some(&b'\n') {
        return None;
    }
    let rest = c.get(start..)?;
    let end = rest.iter().position(|b| *b == b'\n')?;
    record_run_id(&rest[..end])
}

/// X2a (ADR-065 D3): whether hot's region that differs from C has the
/// shape of a fork. `next_line` gives hot's lines from s, the start of the
/// line that holds d (which hot and C share), one at a time with their
/// `\n` (only the journal's last line can lack it); a read error is a fork,
/// since the guard errs toward stopping.
///
/// Fork-shaped ⇔ (1) every line of hot's region through the line that holds
/// e is a record or glued, except that the last may be torn; and (2) the
/// `\n` positions of hot and C over `[d, e]` differ, or a line is glued
/// through a complete record whose `run_id` differs from that of C's line
/// at the same start, itself a complete record. A line is glued when it is
/// no record but its suffix from some p is one (a torn record, when it is
/// the journal's last line): the shape `Journal::open` leaves after a torn
/// line. For the first line s < p ≤ d (its bytes before p are C's); for
/// every later line (review of #226, finding 4) the line's bytes before p
/// must be a torn record themselves, since they need not be C's.
pub fn fork_shaped(
    facts: &ForkFacts<'_>,
    next_line: &mut dyn FnMut() -> Result<Option<Vec<u8>>, String>,
) -> bool {
    let (c, d, e) = (facts.c, facts.d, facts.e);
    let s = line_start_in(c, d);
    let mut start = s;
    let mut first = true;
    let mut newlines_differ = false;
    let mut glued_other_run = false;
    loop {
        let line = match next_line() {
            Ok(Some(line)) if !line.is_empty() => line,
            Ok(_) => break,
            Err(_) => return true,
        };
        let end = start + line.len() as u64;
        for i in start.max(d)..end.min(e + 1) {
            let hot_byte = line[(i - start) as usize];
            let c_byte = usize::try_from(i).ok().and_then(|i| c.get(i)).copied();
            if (hot_byte == b'\n') != (c_byte == Some(b'\n')) {
                newlines_differ = true;
            }
        }
        let complete = line.last() == Some(&b'\n');
        let body = if complete {
            &line[..line.len() - 1]
        } else {
            &line[..]
        };
        let fits = if complete {
            record_run_id(body).is_some()
        } else {
            torn_record(body)
        };
        if !fits {
            let c_run_id = c_run_id_at(c, start);
            let candidates: Box<dyn Iterator<Item = usize>> = if first {
                Box::new(((s + 1)..=d).map(|p| (p - s) as usize))
            } else {
                Box::new(1..body.len())
            };
            let mut glued = false;
            for at in candidates {
                if at >= body.len() || body[at] != b'{' || (!first && !torn_record(&body[..at])) {
                    continue;
                }
                let suffix = &body[at..];
                if complete {
                    if let Some(run_id) = record_run_id(suffix) {
                        glued = true;
                        if c_run_id.as_ref().is_some_and(|c| *c != run_id) {
                            glued_other_run = true;
                        }
                    }
                } else if torn_record(suffix) {
                    glued = true;
                }
            }
            if !glued {
                return false;
            }
        }
        first = false;
        start = end;
        if end > e || !complete {
            break;
        }
    }
    newlines_differ || glued_other_run
}

// ------------------------------------------------ pure: import ledger

/// What an import-ledger name is, by the union of the names of its
/// archive file on hot and on the targets (G4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameKind {
    /// The current entry.
    Current,
    /// A superseded entry.
    Superseded,
    /// A supersede record.
    Record,
}

/// One copy's state under X4's test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileState {
    /// It passes the test.
    Good,
    /// It does not parse or bind.
    DoesNotParse(String),
    /// A current entry listing files with another SHA-256 than the copy
    /// ledger's (R8).
    OtherSha(Vec<String>),
    /// A superseded entry whose SHA-256 no supersede record names for it.
    Unnamed,
    /// It cannot be read.
    Unreadable(String),
}

impl FileState {
    fn describe(&self) -> String {
        match self {
            Self::Good => "good".to_owned(),
            Self::DoesNotParse(why) => format!("does not parse ({why})"),
            Self::OtherSha(files) => {
                format!("lists {} with another SHA-256", files.join(", "))
            }
            Self::Unnamed => {
                "a superseded entry whose SHA-256 no supersede record names".to_owned()
            }
            Self::Unreadable(why) => format!("unreadable ({why})"),
        }
    }
}

/// One copy of an import-ledger file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyFacts {
    /// `hot`, or a copy target's id.
    pub root: String,
    /// Its path.
    pub path: PathBuf,
    /// Its state.
    pub state: FileState,
    /// Its bytes, when it was read.
    pub bytes: Option<Vec<u8>>,
}

/// One import-ledger name as X4 reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameFacts {
    /// What it is.
    pub kind: NameKind,
    /// Hot's copy; `None` when hot lacks the name.
    pub hot: Option<CopyFacts>,
    /// The copy targets' copies, in configuration order.
    pub targets: Vec<CopyFacts>,
    /// What closes the name by hand when no copy anywhere is good
    /// (ADR-065 D5); named in the stop.
    pub closure: String,
}

/// X4's decision for one name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameDecision {
    /// Hot's copy is good; the target copies that differ from it are R8's
    /// (named).
    Keep {
        /// The target copies that differ from hot's.
        differing: Vec<String>,
    },
    /// The good variant of target copy `from` is written to hot; `replace`
    /// when hot holds a copy (replaced, kept as evidence), else created.
    Write {
        /// The index of the target copy.
        from: usize,
        /// Whether hot holds a copy.
        replace: bool,
    },
    /// Nothing can be decided: the stop's text.
    Stop(String),
}

/// R8's test as an oracle: the listed files `(path, sha256)` whose SHA-256
/// the copy ledger does not record.
pub type Mismatches<'a> = dyn Fn(&[(String, String)]) -> Vec<String> + 'a;

/// X4's test (ADR-065 D5) for one copy of `name` (`<segment>/<file>`)
/// of kind `kind`, with the supersede records of its archive file that
/// parse and name their own file (`records`): a supersede record is good
/// when it parses and names its own file (G3); a current entry when it
/// parses, binds and lists every file with the SHA-256 the copy ledger
/// records (`mismatches`, R8's [`entry_mismatches`]); a superseded entry
/// when it parses, binds, and a record names its SHA-256 for its name.
/// Pure.
pub fn file_state(
    name: &str,
    kind: NameKind,
    bytes: &[u8],
    records: &[&SupersedeRecord],
    mismatches: &Mismatches<'_>,
) -> FileState {
    let Some((segment, file_name)) = name.split_once('/') else {
        return FileState::DoesNotParse(format!("{name:?} has no segment"));
    };
    let parsed = match LedgerName::parse(file_name) {
        Ok(parsed) => parsed,
        Err(error) => return FileState::DoesNotParse(error.0),
    };
    let pair = (parsed, bytes.to_vec());
    let classified = match classify(segment, std::slice::from_ref(&pair)) {
        Ok(classified) => classified,
        Err(error) => return FileState::DoesNotParse(error.0),
    };
    match kind {
        NameKind::Record => FileState::Good,
        NameKind::Current => {
            let files: Vec<(String, String)> = classified
                .current
                .map(|(_, ledger)| {
                    ledger
                        .files
                        .iter()
                        .map(|f| (f.relative_path.clone(), f.sha256.clone()))
                        .collect()
                })
                .unwrap_or_default();
            let wrong = mismatches(&files);
            if wrong.is_empty() {
                FileState::Good
            } else {
                FileState::OtherSha(wrong)
            }
        }
        NameKind::Superseded => {
            let sha = SupersededEntry::of(name.to_owned(), bytes).sha256;
            let named = records
                .iter()
                .any(|r| r.entries.iter().any(|e| e.name == name && e.sha256 == sha));
            if named {
                FileState::Good
            } else {
                FileState::Unnamed
            }
        }
    }
}

/// X4 (ADR-065 D5) over every name: hot's good copy is kept; otherwise
/// exactly one good byte variant among the target copies is written;
/// otherwise the name stops, naming hot's copy and every target copy with
/// its state, and with no good copy anywhere the closure by hand. Pure.
#[must_use]
pub fn plan_hot_import_ledger(
    names: &BTreeMap<String, NameFacts>,
) -> BTreeMap<String, NameDecision> {
    let mut plan = BTreeMap::new();
    for (name, facts) in names {
        if let Some(hot) = facts
            .hot
            .as_ref()
            .filter(|hot| hot.state == FileState::Good)
        {
            let differing = facts
                .targets
                .iter()
                .filter(|copy| copy.bytes != hot.bytes)
                .map(|copy| copy.root.clone())
                .collect();
            plan.insert(name.clone(), NameDecision::Keep { differing });
            continue;
        }
        let mut variants: Vec<(usize, &[u8])> = Vec::new();
        for (i, copy) in facts.targets.iter().enumerate() {
            let Some(bytes) = copy
                .bytes
                .as_deref()
                .filter(|_| copy.state == FileState::Good)
            else {
                continue;
            };
            if !variants.iter().any(|(_, known)| *known == bytes) {
                variants.push((i, bytes));
            }
        }
        if let [(from, _)] = variants.as_slice() {
            plan.insert(
                name.clone(),
                NameDecision::Write {
                    from: *from,
                    replace: facts.hot.is_some(),
                },
            );
            continue;
        }
        let mut sides = vec![match &facts.hot {
            Some(hot) => format!("hot ({}): {}", hot.path.display(), hot.state.describe()),
            None => "hot: absent".to_owned(),
        }];
        sides.extend(facts.targets.iter().map(|copy| {
            format!(
                "{} ({}): {}",
                copy.root,
                copy.path.display(),
                copy.state.describe()
            )
        }));
        let why = if variants.is_empty() {
            format!(
                "no copy is good, so no copy can be restored to hot. {}",
                facts.closure
            )
        } else {
            "two good byte variants on the copy targets, and nothing tells which is hot's; \
             nothing is written"
                .to_owned()
        };
        plan.insert(
            name.clone(),
            NameDecision::Stop(format!(
                "import-ledger file {name}: {}. {why} (ADR-065 D5)",
                sides.join("; ")
            )),
        );
    }
    plan
}

// ------------------------------------------------------------- the run

/// Runs one repair of hot (module docs). `now_ms` is read once, for the
/// run's `at_ms`, which every evidence name carries.
pub fn run(
    config: &IngestConfig,
    archive: Option<&Path>,
    scope: Scope,
    flags: Flags,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
) -> RepairHotReport {
    let mut report = RepairHotReport::default();
    if let Err(Stop(why)) = repair_hot(config, archive, scope, flags, probe, now_ms, &mut report) {
        report.problems.push(why);
    }
    if report.problems.is_empty() {
        report.lines.push(
            "next: `mie store verify`, which closes the copies' open findings (ADR-062 D11)"
                .to_owned(),
        );
    }
    report.lines.push(if report.written == 0 {
        "nothing written".to_owned()
    } else {
        format!("repaired: {} file(s) written", report.written)
    });
    report
}

/// A marked copy target.
struct Target {
    path: PathBuf,
    id: String,
}

/// The run's shared reads.
struct Run<'a> {
    config: &'a IngestConfig,
    hot: &'a Path,
    targets: Vec<Target>,
    at_ms: i64,
    flags: Flags,
}

/// The canonical path of `path` when it exists; a dangling link is
/// refused (an unmounted disk), an absent path is kept as given.
fn resolve_path(path: &Path, what: &str) -> Result<(PathBuf, bool), Stop> {
    let link = fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink());
    match fs::canonicalize(path) {
        Ok(canonical) => Ok((canonical, true)),
        Err(error) if error.kind() == ErrorKind::NotFound && link => Err(Stop(format!(
            "{what} {} is a link to a missing path: an unmounted disk. Mount it, then run this \
             again; nothing is written (ADR-065 D1)",
            path.display()
        ))),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok((path.to_owned(), false)),
        Err(error) => Err(Stop(format!(
            "{what} {}: cannot resolve it ({error}); nothing is written (ADR-065 D1)",
            path.display()
        ))),
    }
}

/// The evidence name beside `path`: `<path>.damaged-<at_ms>`.
fn beside(path: &Path, at_ms: i64) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".damaged-{at_ms}"));
    PathBuf::from(name)
}

/// The evidence names an earlier run left beside `path`
/// (`<name>.damaged-*`), sorted: a crash after the link leaves one, which
/// the report names (X5).
fn earlier_evidence(path: &Path) -> Vec<PathBuf> {
    let (Some(dir), Some(name)) = (path.parent(), path.file_name().and_then(|n| n.to_str())) else {
        return Vec::new();
    };
    let prefix = format!("{name}.damaged-");
    sorted_names(dir)
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| entry.starts_with(&prefix))
        .map(|entry| dir.join(entry))
        .collect()
}

/// A byte for a stop's text.
fn byte_text(path: &Path, at: u64) -> String {
    let read = fs::File::open(path).and_then(|mut file| {
        file.seek(SeekFrom::Start(at))?;
        let mut byte = [0u8; 1];
        let n = file.read(&mut byte)?;
        Ok((n == 1).then_some(byte[0]))
    });
    match read {
        Ok(Some(byte)) => format!("0x{byte:02x}"),
        Ok(None) => "past its end".to_owned(),
        Err(error) => format!("unreadable ({error})"),
    }
}

/// Whether the first `len` bytes of `path` read without error.
fn reads_in_full(path: &Path, len: u64) -> Result<(), String> {
    let mut file = fs::File::open(path).map_err(|e| e.to_string())?;
    let mut buffer = vec![0u8; 1 << 20];
    let mut left = len;
    while left > 0 {
        let n = (buffer.len() as u64).min(left) as usize;
        file.read_exact(&mut buffer[..n])
            .map_err(|e| e.to_string())?;
        left -= n as u64;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn repair_hot(
    config: &IngestConfig,
    archive: Option<&Path>,
    scope: Scope,
    flags: Flags,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    report: &mut RepairHotReport,
) -> Result<(), Stop> {
    let hot = config.paths.raw_root.as_path();
    let cold_roots = &config.paths.cold_roots;

    // X0: the refusals.
    if flags.keep_hot_journal && scope == Scope::ImportLedger {
        return Err(Stop(
            "--keep-hot-journal applies to --scope journal only: it keeps hot's journal against \
             its copies; nothing is written (ADR-065 D4)"
                .to_owned(),
        ));
    }
    if cold_roots.is_empty() {
        return Err(Stop(
            "paths.cold_roots is empty: a single-root store has no copy targets to repair hot \
             from (ADR-065 D1)"
                .to_owned(),
        ));
    }
    check_hot(hot, cold_roots).map_err(Stop)?;
    let mut targets: Vec<Target> = Vec::new();
    let mut ids: Vec<(String, PathBuf)> = Vec::new();
    let mut skipped = Vec::new();
    for cold in cold_roots {
        match root_marker::read(&cold.path) {
            Ok(Some(id)) if id == HOT_ID => {
                return Err(Stop(format!(
                    "cold root {} carries hot's marker (id {HOT_ID:?}), the id of raw_root {}: \
                     the same root under two names, or a copied marker; nothing is written \
                     (ADR-062 D7)",
                    cold.path.display(),
                    hot.display()
                )));
            }
            Ok(Some(id)) => {
                if let Some((_, other)) = ids.iter().find(|(known, _)| *known == id) {
                    return Err(Stop(format!(
                        "cold roots {} and {} both carry the id {id:?}: two spellings of one \
                         root, or a copied marker; nothing is written (ADR-062 D7)",
                        other.display(),
                        cold.path.display()
                    )));
                }
                ids.push((id.clone(), cold.path.clone()));
                if cold.copy_target {
                    targets.push(Target {
                        path: cold.path.clone(),
                        id,
                    });
                }
            }
            Ok(None) if cold.copy_target => skipped.push(format!(
                "copy target {} has no root marker: skipped, never read or written (an unmounted \
                 mount point is an empty directory), so it is no witness (ADR-062 D11)",
                cold.path.display()
            )),
            Err(error) if cold.copy_target => skipped.push(format!(
                "copy target {}: its root marker cannot be read ({error}): skipped, never read or \
                 written, so it is no witness (ADR-062 D11)",
                cold.path.display()
            )),
            // A read-only root is never a witness.
            Ok(None) | Err(_) => {}
        }
    }
    let target_paths: Vec<&Path> = targets.iter().map(|t| t.path.as_path()).collect();
    check_devices(hot, &target_paths, probe).map_err(Stop)?;
    let archive = match scope {
        Scope::Journal => {
            let journal = config.paths.journal.as_path();
            if journal.file_name().is_none() {
                return Err(Stop(format!(
                    "paths.journal {} has no file name, so its copies on the targets cannot be \
                     found; nothing is written (ADR-065 D1)",
                    journal.display()
                )));
            }
            resolve_path(journal, "paths.journal")?;
            None
        }
        Scope::ImportLedger => {
            let Some(archive) = replicate::archive_config(config, archive).map_err(Stop)? else {
                return Err(Stop(
                    "no archive configuration: --archive-config, else [history] archive_config, \
                     names the import ledger to repair; nothing is written (ADR-065 D1)"
                        .to_owned(),
                ));
            };
            resolve_path(&archive.paths.import_ledger, "the import ledger")?;
            Some(archive)
        }
    };
    report.problems.extend(skipped);
    let mut run = Run {
        config,
        hot,
        targets,
        at_ms: 0,
        flags,
    };
    let _locks = run.lock(scope)?;
    run.at_ms = now_ms();
    match archive {
        None => run.journal(report),
        Some(archive) => run.import_ledger(&archive.paths.import_ledger, report),
    }
}

impl Run<'_> {
    /// X0's locks, each without waiting; dropped at the end of the run.
    fn lock(&self, scope: Scope) -> Result<Vec<Box<dyn std::any::Any>>, Stop> {
        let locked = |what: &str, error: RawStoreError, then: &str| {
            Stop(format!(
                "{what}: {error}; {then}this command takes it without waiting and holds it for \
                 its whole run; nothing is written (ADR-065 D1)"
            ))
        };
        let mut held: Vec<Box<dyn std::any::Any>> = Vec::new();
        held.push(Box::new(replica::try_replication_lock(self.hot).map_err(
            |e| locked("the pass lock", e, "a replication pass runs; "),
        )?));
        held.push(Box::new(replica::try_verify_lock(self.hot).map_err(
            |e| {
                locked(
                    "the verify lock",
                    e,
                    "`mie store verify` or `mie store repair` runs; ",
                )
            },
        )?));
        let (source, then) = match scope {
            Scope::Journal => (
                self.config.instrument.source.clone(),
                "`mie ingest` holds it: stop `mie ingest`, then run this again; ",
            ),
            Scope::ImportLedger => (
                ARCHIVE_SOURCE.to_owned(),
                "the importer, `mie store resolve` or the hot removal holds it; ",
            ),
        };
        held.push(Box::new(
            removal::take_source_lock(self.hot, &source)
                .map_err(|e| locked(&format!("the writer lock of source {source}"), e, then))?,
        ));
        if scope == Scope::Journal {
            // `mie ingest` releases the source's lock when its store
            // closes, before `sealed`, `run_end` and the final pass's
            // `replication`; its journal holds this one until its last
            // line (review of #226, finding 3).
            held.push(Box::new(
                replica::try_journal_lock(&self.config.paths.journal).map_err(|e| {
                    locked(
                        "the journal lock",
                        e,
                        "`mie ingest` (until its last journal line) or `mie store restore` holds \
                         it: stop `mie ingest` and wait for it to exit, then run this again; ",
                    )
                })?,
            ));
        }
        Ok(held)
    }
}

/// One journal witness as the run reads it: its target and path.
struct WitnessAt {
    target: usize,
    path: PathBuf,
}

impl Run<'_> {
    /// The journal copy's evidence name on target `t` (X3, R7's).
    fn copy_aside(&self, t: usize, name: &std::ffi::OsStr) -> PathBuf {
        replica::damaged_dir(&self.targets[t].path, self.at_ms)
            .join(META_DIR)
            .join(JOURNAL_DIR)
            .join(name)
    }

    /// The journal scope: X1, X2, X2a, X3 and the journal's writes.
    #[allow(clippy::too_many_lines)]
    fn journal(&self, report: &mut RepairHotReport) -> Result<(), Stop> {
        let configured = self.config.paths.journal.as_path();
        let name = configured.file_name().unwrap_or_default().to_owned();
        let (hot_path, hot_exists) = resolve_path(configured, "paths.journal")?;
        let hot_len = if hot_exists {
            fs::metadata(&hot_path)
                .map_err(|e| Stop(format!("inspect {}: {e}", hot_path.display())))?
                .len()
        } else {
            0
        };
        report.lines.push(if hot_exists {
            format!("hot's journal {}: {hot_len} bytes", hot_path.display())
        } else {
            format!("hot's journal {}: absent", hot_path.display())
        });
        for earlier in earlier_evidence(&hot_path) {
            report.lines.push(format!(
                "evidence of an earlier run beside hot's journal, kept: {}",
                earlier.display()
            ));
        }

        // X1: the witnesses, in configuration order.
        let mut candidates: Vec<JournalCandidate> = Vec::new();
        let mut at: Vec<WitnessAt> = Vec::new();
        let mut elsewhere = Vec::new();
        // A copy that cannot be inspected is no witness here, but R7 counts
        // it and refuses (ADR-065 D8).
        let mut uninspected = false;
        for (t, target) in self.targets.iter().enumerate() {
            let dir = target.path.join(META_DIR).join(JOURNAL_DIR);
            let path = dir.join(&name);
            match fs::metadata(&path) {
                Ok(metadata) => {
                    candidates.push(JournalCandidate {
                        label: format!("the journal copy on {} ({})", target.id, path.display()),
                        len: metadata.len(),
                    });
                    at.push(WitnessAt { target: t, path });
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    let names: Vec<String> = sorted_names(&dir)
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|n| !n.starts_with('.'))
                        .collect();
                    if !names.is_empty() {
                        elsewhere.push(format!("{} holds {}", target.id, names.join(", ")));
                    }
                }
                Err(error) => {
                    uninspected = true;
                    report.problems.push(format!(
                        "the journal copy on {} ({}) cannot be inspected ({error}): no witness. \
                         {READ_ERROR} (ADR-065 D2)",
                        target.id,
                        path.display()
                    ));
                }
            }
        }
        if candidates.is_empty() {
            let name = name.to_string_lossy();
            report.lines.push(if elsewhere.is_empty() {
                format!("no copy target holds a journal copy named {name}")
            } else {
                format!(
                    "no copy target holds a journal copy named {name} (the file name of \
                     paths.journal); under other names: {}",
                    elsewhere.join("; ")
                )
            });
        }
        let read = |i: usize| -> Result<Vec<u8>, String> {
            let mut bytes = Vec::new();
            fs::File::open(&at[i].path)
                .and_then(|mut file| {
                    Read::take(&mut file, candidates[i].len).read_to_end(&mut bytes)
                })
                .map_err(|error| error.to_string())?;
            if bytes.len() as u64 != candidates[i].len {
                return Err(format!(
                    "{} of its {} bytes read",
                    bytes.len(),
                    candidates[i].len
                ));
            }
            Ok(bytes)
        };
        let choice =
            restore::choose_journal_stops(&candidates, true, &mut |i| read(i), &mut |i, c, len| {
                replica::first_difference(&at[i].path, TailSource::Bytes(c), len)
                    .map_err(|error| error.error.to_string())
            });
        let witness_line = |i: usize| {
            format!(
                "witness: {}, {} bytes",
                candidates[i].label, candidates[i].len
            )
        };
        let mut hot_readable = hot_exists;
        let mut c_bytes = Vec::new();
        let (facts, readable, chosen) = match choice {
            Ok(choice) => {
                let mut readable: Vec<usize> = Vec::new();
                for (i, candidate) in candidates.iter().enumerate() {
                    if let Some(out) = choice.left_out.iter().find(|out| out.index == i) {
                        report.problems.push(format!(
                            "{} ({} bytes) cannot be read ({}): no witness. {READ_ERROR} \
                             (ADR-065 D2)",
                            candidate.label, candidate.len, out.error
                        ));
                    } else {
                        report.lines.push(witness_line(i));
                        readable.push(i);
                    }
                }
                c_bytes = choice.bytes;
                let chosen_len = c_bytes.len() as u64;
                let common = hot_len.min(chosen_len);
                let bounds = if common == 0 {
                    None
                } else {
                    match replica::difference_bounds(&hot_path, TailSource::Bytes(&c_bytes), common)
                    {
                        Ok(bounds) => bounds,
                        Err(error) if error.side == Side::Destination => {
                            hot_readable = false;
                            report.lines.push(format!(
                                "hot's journal cannot be read ({}): a difference at every byte",
                                error.error
                            ));
                            Some((0, hot_len - 1))
                        }
                        Err(error) => return Err(Stop(error.to_string())),
                    }
                };
                let witnesses: Vec<WitnessFacts> = readable
                    .iter()
                    .map(|&i| WitnessFacts {
                        id: self.targets[at[i].target].id.clone(),
                        len: candidates[i].len,
                        vs_hot: bounds
                            .map(|(d, _)| d)
                            .filter(|d| *d < candidates[i].len.min(hot_len)),
                        vs_others: vec![None; readable.len()],
                        r7_refuses: false,
                    })
                    .collect();
                let chosen = choice
                    .chosen
                    .and_then(|c| readable.iter().position(|&i| i == c));
                let mut facts = JournalFacts {
                    hot_len,
                    witnesses,
                    chosen,
                    bounds,
                    last_line_start: last_line_start(&c_bytes),
                    cut: c_bytes.last().is_some_and(|&byte| byte != b'\n'),
                    fork: false,
                    copy_targets: self
                        .config
                        .paths
                        .cold_roots
                        .iter()
                        .filter(|r| r.copy_target)
                        .count(),
                };
                if let Some((d, e)) = bounds
                    && hot_readable
                    && chosen_len == hot_len
                    && e < facts.second_len()
                {
                    let s = line_start_in(&c_bytes, d);
                    let mut reader = fs::File::open(&hot_path).and_then(|mut file| {
                        file.seek(SeekFrom::Start(s))?;
                        Ok(BufReader::new(file))
                    });
                    facts.fork = fork_shaped(&ForkFacts { c: &c_bytes, d, e }, &mut || {
                        let reader = reader.as_mut().map_err(|e| e.to_string())?;
                        let mut line = Vec::new();
                        reader
                            .read_until(b'\n', &mut line)
                            .map_err(|e| e.to_string())?;
                        Ok((!line.is_empty()).then_some(line))
                    });
                }
                (facts, readable, chosen)
            }
            Err(stops) => {
                // The copies differ (H3′'s first divergence); every
                // comparison from here is file against file.
                let c = stops.iter().find_map(|stop| match stop {
                    JournalStop::Diverged { chosen, .. } => Some(*chosen),
                    JournalStop::Unreadable { .. } => None,
                });
                let mut readable: Vec<usize> = Vec::new();
                for i in 0..candidates.len() {
                    match reads_in_full(&at[i].path, candidates[i].len) {
                        Ok(()) => {
                            report.lines.push(witness_line(i));
                            readable.push(i);
                        }
                        Err(error) => report.problems.push(format!(
                            "{} ({} bytes) cannot be read ({error}): no witness. {READ_ERROR} \
                             (ADR-065 D2)",
                            candidates[i].label, candidates[i].len
                        )),
                    }
                }
                if hot_exists && reads_in_full(&hot_path, hot_len).is_err() {
                    hot_readable = false;
                }
                let compare = |a: &Path, a_len: u64, b: &Path, b_len: u64| {
                    let len = a_len.min(b_len);
                    replica::first_difference(a, TailSource::File(b), len)
                };
                // D8's witnesses: every journal copy, the unreadable ones
                // too, since R7 counts them and refuses when one cannot be
                // read.
                let all: Vec<&Path> = at.iter().map(|w| w.path.as_path()).collect();
                let mut witnesses = Vec::new();
                for &i in &readable {
                    let vs_hot = if !hot_readable {
                        (candidates[i].len.min(hot_len) > 0).then_some(0)
                    } else if hot_exists {
                        compare(&at[i].path, candidates[i].len, &hot_path, hot_len)
                            .map_err(|e| Stop(e.to_string()))?
                    } else {
                        None
                    };
                    let mut vs_others = Vec::new();
                    for &j in &readable {
                        vs_others.push(if i == j {
                            None
                        } else {
                            compare(
                                &at[i].path,
                                candidates[i].len,
                                &at[j].path,
                                candidates[j].len,
                            )
                            .map_err(|e| Stop(e.to_string()))?
                        });
                    }
                    let len = candidates[i].len;
                    let r7_refuses = if !hot_readable || !hot_exists {
                        len > 0
                    } else if vs_hot.is_none() && len <= hot_len {
                        false
                    } else {
                        let d = vs_hot.unwrap_or(hot_len);
                        let contested = replica::line_start(TailSource::File(&hot_path), d)
                            .and_then(|u| replica::first_contested_file(&hot_path, &all, u));
                        uninspected || !matches!(contested, Ok(None))
                    };
                    witnesses.push(WitnessFacts {
                        id: self.targets[at[i].target].id.clone(),
                        len,
                        vs_hot,
                        vs_others,
                        r7_refuses,
                    });
                }
                let chosen = c.and_then(|c| readable.iter().position(|&i| i == c));
                let facts = JournalFacts {
                    hot_len,
                    witnesses,
                    chosen: None,
                    bounds: None,
                    last_line_start: 0,
                    cut: false,
                    fork: false,
                    copy_targets: self
                        .config
                        .paths
                        .cold_roots
                        .iter()
                        .filter(|r| r.copy_target)
                        .count(),
                };
                (facts, readable, chosen)
            }
        };
        let decision = decide_journal(&facts);
        report
            .lines
            .push(format!("journal: decision {}", decision.name()));
        let paths: Vec<&Path> = readable.iter().map(|&i| at[i].path.as_path()).collect();
        let targets_of: Vec<usize> = readable.iter().map(|&i| at[i].target).collect();

        // X3.
        if self.flags.keep_hot_journal {
            if !decision.keeps_hot() {
                return Err(Stop(format!(
                    "--keep-hot-journal is refused in the state {}: there the evidence decides, \
                     or a pass will, so no copy is rebuilt and nothing is written (ADR-065 D4)",
                    decision.name()
                )));
            }
            let readable = if hot_exists {
                reads_in_full(&hot_path, hot_len)
            } else {
                Err("it is absent".to_owned())
            };
            if let Err(error) = readable {
                return Err(Stop(format!(
                    "--keep-hot-journal is refused: hot's journal {} does not read in full \
                     ({error}), so there is no readable hot journal to keep; nothing is written \
                     (ADR-065 D4)",
                    hot_path.display()
                )));
            }
            return self.keep_hot(&facts, &paths, &targets_of, &hot_path, &name, report);
        }

        let sides = || self.sides(&facts, &paths, chosen, &hot_path, &c_bytes);
        match decision {
            JournalDecision::Agree => {
                report.lines.push(
                    "journal: hot's journal holds every copy's bytes; nothing to write, the next \
                     pass extends the copies"
                        .to_owned(),
                );
                Ok(())
            }
            JournalDecision::Behind => self.extend_hot(&hot_path, &c_bytes, &facts, report),
            JournalDecision::Damaged => self.replace_hot(&hot_path, &c_bytes, &facts, report),
            JournalDecision::CopyDamaged => Err(Stop(format!(
                "the journal copies differ, and hot's journal holds a strict majority at every \
                 byte where a copy contradicts it: the faults are on the copies. {}. Run `mie \
                 store verify`, then `mie store repair` (R7 rebuilds each differing copy from \
                 hot, ADR-065 D8), then this command again; nothing is written (ADR-065 D2)",
                sides()
            ))),
            JournalDecision::TwoFault { group } => {
                let configuration = group.map_or_else(String::new, |group| {
                    let ids: Vec<&str> = group
                        .iter()
                        .map(|&w| facts.witnesses[w].id.as_str())
                        .collect();
                    format!(
                        " With {} copy targets, a run of this command whose --config lists only \
                         the copy targets {} (their journals are byte prefixes of one another, \
                         at least {} of them) decides hot by those; then, with the full \
                         configuration, `mie store verify` and `mie store repair` rebuild the \
                         left-out copy from the repaired hot.",
                        facts.copy_targets,
                        ids.join(", "),
                        crate::config::MIN_COPY_TARGETS
                    )
                });
                Err(Stop(format!(
                    "the journal copies differ, and at a byte where a copy contradicts hot's \
                     journal, hot holds no strict majority, so R7 refuses that copy (ADR-065 D8): \
                     two faults, which neither the copies nor hot can attribute byte for byte. \
                     {}. `mie store repair` compares against hot, so it is not the next step. \
                     When you hold hot's journal for right, `--keep-hot-journal` rebuilds every \
                     copy that is no prefix of it from hot, keeping their bytes in \
                     .mie/damaged/.{configuration} Nothing is written (ADR-065 D2, D4)",
                    sides()
                )))
            }
            JournalDecision::HotAhead => Err(Stop(format!(
                "hot's journal is longer than every copy and differs from them before the \
                 copies' last line (byte {}). {}. Stop `mie ingest`, run `mie store replicate` \
                 (the standalone pass writes no journal line, so it brings the copies to hot's \
                 length), then run this command again; nothing is written (ADR-065 D2)",
                facts.last_line_start,
                sides()
            ))),
            JournalDecision::Diverged => Err(Stop(format!(
                "hot's journal and the copies diverged: each side holds bytes the other lacks \
                 ({}). {}. Nothing tells which holds the live data, so nothing is written. When \
                 you hold hot's journal for right, `--keep-hot-journal` rebuilds every copy that \
                 is no prefix of it from hot, keeping their bytes in .mie/damaged/ (ADR-065 D2, \
                 D3, D4)",
                if facts.fork {
                    "a fork of equal length: the lines differ in shape, not by damage"
                } else if hot_len > facts.chosen_len() {
                    "hot is longer, and the difference reaches the copies' last line, so the \
                     pass refuses the tail"
                } else if facts.chosen_len() > hot_len {
                    "the copies are longer, and hot contradicts them"
                } else {
                    "equal lengths"
                },
                sides()
            ))),
            JournalDecision::OneWitness => Err(Stop(format!(
                "hot's journal differs from the copies up to byte {}, and only one copy reaches \
                 it (the second-longest ends at byte {}): one copy against hot cannot be \
                 attributed, since the journal has no hash chain (ADR-062 D11 R7). {}. Nothing \
                 is written. When you hold hot's journal for right, `--keep-hot-journal` \
                 rebuilds every copy that is no prefix of it from hot (ADR-065 D2, D4)",
                facts.bounds.map_or(0, |(_, e)| e),
                facts.second_len(),
                sides()
            ))),
        }
    }

    /// Both sides of a stop (X2): hot's path, length, d, the line start s
    /// and its byte at d, then each witness's root, path, length and byte at
    /// d; with copies that differ, each witness's first difference against
    /// C and against hot.
    fn sides(
        &self,
        facts: &JournalFacts,
        paths: &[&Path],
        chosen: Option<usize>,
        hot_path: &Path,
        c_bytes: &[u8],
    ) -> String {
        let mut parts = Vec::new();
        if let Some((d, e)) = facts.bounds {
            let s = line_start_in(c_bytes, d);
            parts.push(format!(
                "hot's journal {} ({} bytes) differs from the copies from byte {d} to byte {e}, \
                 in the line from byte {s}, and holds {} at byte {d}",
                hot_path.display(),
                facts.hot_len,
                byte_text(hot_path, d)
            ));
            for (w, witness) in facts.witnesses.iter().enumerate() {
                parts.push(format!(
                    "{} {} ({} bytes) holds {} at byte {d}",
                    witness.id,
                    paths[w].display(),
                    witness.len,
                    byte_text(paths[w], d)
                ));
            }
        } else {
            parts.push(format!(
                "hot's journal {} ({} bytes)",
                hot_path.display(),
                facts.hot_len
            ));
            for (w, witness) in facts.witnesses.iter().enumerate() {
                let against_c = match chosen {
                    Some(c) if c == w => "the longest copy (C)".to_owned(),
                    Some(c) => match witness.vs_others[c] {
                        Some(at) => format!(
                            "first differs from {}'s copy at byte {at}",
                            facts.witnesses[c].id
                        ),
                        None => format!("a byte prefix of {}'s copy", facts.witnesses[c].id),
                    },
                    None => "C undecided".to_owned(),
                };
                let against_hot = match witness.vs_hot {
                    Some(at) => format!("first differs from hot's journal at byte {at}"),
                    None => "consistent with hot's journal".to_owned(),
                };
                parts.push(format!(
                    "{} {} ({} bytes): {against_c}, {against_hot}",
                    witness.id,
                    paths[w].display(),
                    witness.len
                ));
            }
        }
        parts.join("; ")
    }

    /// *behind* (X2): hot's journal is extended to C, then ended with one
    /// `\n` when C ends inside a line. The extension is chunked and the
    /// `\n` comes after it, so a crash or a failed write can leave hot equal
    /// to C without its `\n`; the re-run decides *behind* there too, with
    /// nothing left to extend, and writes the `\n` (review of #226,
    /// finding 5).
    fn extend_hot(
        &self,
        hot_path: &Path,
        c_bytes: &[u8],
        facts: &JournalFacts,
        report: &mut RepairHotReport,
    ) -> Result<(), Stop> {
        let from = facts.chosen.map_or("", |c| facts.witnesses[c].id.as_str());
        let appended = replica::extend_prefix(
            TailSource::Bytes(c_bytes),
            hot_path,
            c_bytes.len() as u64,
        )
        .map_err(|error| {
            Stop(format!(
                "extend hot's journal {} from the copy on {from}: {error}; it completes once hot \
                 accepts the write (ADR-065 D2)",
                hot_path.display()
            ))
        })?;
        if appended > 0 {
            report.written += 1;
            report.lines.push(format!(
                "journal: hot's journal {} extended by {appended} byte(s) from the copy on {from}",
                hot_path.display()
            ));
            reached(Step::Extended)?;
        } else {
            report.lines.push(format!(
                "journal: hot's journal {} already holds every byte of the copy on {from}, but \
                 not the `\\n` that ends its last line: an earlier run's extension, or a cut \
                 line a pass copied",
                hot_path.display()
            ));
        }
        if facts.cut {
            fs::OpenOptions::new()
                .append(true)
                .open(hot_path)
                .and_then(|mut file| {
                    file.write_all(b"\n")?;
                    file.sync_all()
                })
                .map_err(|e| Stop(format!("append to {}: {e}", hot_path.display())))?;
            if appended == 0 {
                report.written += 1;
            }
            report.lines.push(
                "journal: its last line was cut, so one `\\n` ends it: the next run_start parses \
                 as a line of its own (ADR-065 D2)"
                    .to_owned(),
            );
        }
        Ok(())
    }

    /// *damaged* (X2, X5): hot's journal is replaced by C, its old bytes
    /// linked to `<journal>.damaged-<at_ms>`.
    fn replace_hot(
        &self,
        hot_path: &Path,
        c_bytes: &[u8],
        facts: &JournalFacts,
        report: &mut RepairHotReport,
    ) -> Result<(), Stop> {
        let aside = beside(hot_path, self.at_ms);
        if fs::symlink_metadata(&aside).is_ok() {
            return Err(Stop(format!(
                "{} exists: evidence is never overwritten; nothing is written, and a re-run, \
                 with a new `at_ms`, completes it (ADR-065 D6)",
                aside.display()
            )));
        }
        let from = facts.chosen.map_or("", |c| facts.witnesses[c].id.as_str());
        let newline = c_bytes.last().is_some_and(|&byte| byte != b'\n');
        let mut linked = false;
        let result = replica::replace_with_evidence(
            TailSource::Bytes(c_bytes),
            hot_path,
            c_bytes.len() as u64,
            newline,
            &aside,
            false,
            &mut |step| {
                if step == ReplaceEvidenceStep::Linked {
                    linked = true;
                }
                replace_step(step)
            },
        );
        if let Err(error) = result {
            if linked {
                report.written += 1;
                report.lines.push(format!(
                    "evidence: hot's journal is linked to {}; the journal is unchanged",
                    aside.display()
                ));
            }
            return Err(Stop(format!(
                "replace hot's journal {}: {error}; hot still holds its journal, and a re-run, \
                 with a new `at_ms`, completes it (ADR-065 D6)",
                hot_path.display()
            )));
        }
        report.written += 1;
        report.lines.push(format!(
            "journal: hot's journal {} replaced by the copy on {from} ({} bytes{}); every \
             differing byte had two copies against it. Its former bytes are linked to {}, never \
             deleted (ADR-065 D2, D6)",
            hot_path.display(),
            c_bytes.len(),
            if newline { ", plus one `\\n`" } else { "" },
            aside.display()
        ));
        reached(Step::Renamed)
    }

    /// X3: every witness that is no prefix of hot's journal is rebuilt
    /// from hot up to the start of the line that holds its first
    /// difference; the next pass extends it.
    fn keep_hot(
        &self,
        facts: &JournalFacts,
        paths: &[&Path],
        targets_of: &[usize],
        hot_path: &Path,
        name: &std::ffi::OsStr,
        report: &mut RepairHotReport,
    ) -> Result<(), Stop> {
        let rebuild: Vec<(usize, u64)> = facts
            .witnesses
            .iter()
            .enumerate()
            .filter_map(|(w, witness)| match witness.vs_hot {
                Some(at) => Some((w, at)),
                None if witness.len > facts.hot_len => Some((w, facts.hot_len)),
                None => None,
            })
            .collect();
        for &(w, _) in &rebuild {
            let aside = self.copy_aside(targets_of[w], name);
            if fs::symlink_metadata(&aside).is_ok() {
                return Err(Stop(format!(
                    "{} exists: evidence is never overwritten; nothing is written, and a re-run, \
                     with a new `at_ms`, completes it (ADR-065 D6)",
                    aside.display()
                )));
            }
        }
        #[cfg(test)]
        if let Some(hook) = BEFORE_WRITES.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
        let mut first = true;
        for (w, at) in rebuild {
            let target = &self.targets[targets_of[w]];
            match root_marker::read(&target.path) {
                Ok(Some(id)) if id == target.id => {}
                _ => {
                    return Err(Stop(format!(
                        "copy target {} ({}): its marker is gone or changed since the run \
                         started, so its journal copy is not rebuilt; the run stops and completes \
                         once the root presents its marker (ADR-065 D4)",
                        target.path.display(),
                        target.id
                    )));
                }
            }
            let upto = replica::line_start(TailSource::File(hot_path), at)
                .map_err(|error| Stop(format!("hot's journal: {error}")))?;
            let aside = self.copy_aside(targets_of[w], name);
            replica::rebuild_prefix(TailSource::File(hot_path), paths[w], upto, &aside).map_err(
                |error| {
                    Stop(format!(
                        "rebuild the journal copy on {} ({}): {error}; the run stops (ADR-065 \
                         D4)",
                        target.id,
                        paths[w].display()
                    ))
                },
            )?;
            report.written += 1;
            report.lines.push(format!(
                "journal: --keep-hot-journal: the copy on {} first differed from hot's journal at \
                 byte {at}; moved aside to {} and rebuilt from hot up to byte {upto}, the start \
                 of that line; the next pass extends it (ADR-065 D4)",
                target.id,
                aside.display()
            ));
            if first {
                first = false;
                reached(Step::CopyRebuilt)?;
            }
        }
        Ok(())
    }
}

/// One copy of an import-ledger file as the run read it.
struct ReadCopy {
    root: String,
    path: PathBuf,
    bytes: Result<Vec<u8>, String>,
}

/// The archive file of a name: `(segment, F)`.
fn group_of(name: &str) -> Option<((String, String), LedgerName)> {
    let (segment, file_name) = name.split_once('/')?;
    let parsed = LedgerName::parse(file_name).ok()?;
    Some(((segment.to_owned(), parsed.file.clone()), parsed))
}

/// Whether a malformed name on hot is one the readers fail closed on (X4):
/// no leading `.`, ending in `.import`, and no import-ledger name (G1).
fn fails_closed(label: &str) -> bool {
    label.split_once('/').is_some_and(|(_, name)| {
        !name.is_empty()
            && !name.starts_with('.')
            && name.ends_with(".import")
            && LedgerName::parse(name).is_err()
    })
}

impl Run<'_> {
    /// The import-ledger scope: X4 and its writes.
    #[allow(clippy::too_many_lines)]
    fn import_ledger(&self, configured: &Path, report: &mut RepairHotReport) -> Result<(), Stop> {
        let ledger_path = copy_ledger::ledger_path(self.hot);
        let snapshot = match copy_ledger::read(self.hot) {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                return Err(Stop(format!(
                    "hot's copy ledger {} does not exist: the entry test reads its manifest \
                     records, so `mie store rebuild` restores it first; nothing is written \
                     (ADR-065 D5)",
                    ledger_path.display()
                )));
            }
            Err(error) => {
                return Err(Stop(format!(
                    "hot's copy ledger {}: {error}; nothing is written (ADR-065 D5)",
                    ledger_path.display()
                )));
            }
        };
        let state = CopyState::fold(&snapshot).map_err(|error| {
            Stop(format!(
                "hot's copy ledger {} cannot be folded: {error}; nothing is written (ADR-065 D5)",
                ledger_path.display()
            ))
        })?;
        let (dir, _) = resolve_path(configured, "the import ledger")?;
        let evidence = beside(&dir, self.at_ms);
        for earlier in earlier_evidence(&dir) {
            report.lines.push(format!(
                "evidence of an earlier run beside hot's import ledger, kept: {}",
                earlier.display()
            ));
        }

        // The reads: hot's listing, then each target's.
        let listing = |root: &str, path: &Path| -> Result<LedgerListing, Stop> {
            list_ledger(path).map_err(|error| {
                Stop(format!(
                    "the import ledger on {root} ({}) cannot be listed ({error}); nothing is \
                     written (ADR-065 D5)",
                    path.display()
                ))
            })
        };
        let hot_listing = listing(HOT_ID, &dir)?;
        let mut copies: BTreeMap<String, Vec<ReadCopy>> = BTreeMap::new();
        let read = |path: &Path| fs::read(path).map_err(|error| error.to_string());
        for (name, path) in &hot_listing.files {
            copies.entry(name.clone()).or_default().push(ReadCopy {
                root: HOT_ID.to_owned(),
                path: path.clone(),
                bytes: read(path),
            });
        }
        let mut target_malformed = Vec::new();
        for target in &self.targets {
            let path = target.path.join(META_DIR).join(IMPORT_LEDGER_DIR);
            let found = listing(&target.id, &path)?;
            for name in found.malformed {
                target_malformed.push(format!(
                    "{name:?} on {}: no import-ledger name; left in place, since the readers \
                     skip a target's names (ADR-065 D5)",
                    target.id
                ));
            }
            for (name, path) in found.files {
                copies.entry(name).or_default().push(ReadCopy {
                    root: target.id.clone(),
                    bytes: read(&path),
                    path,
                });
            }
        }

        // Per archive file: the union of its names' generations (G4), and
        // its supersede records that parse and name their own file.
        let mut generations: BTreeMap<(String, String), Vec<(u32, LedgerKind)>> = BTreeMap::new();
        let mut records: BTreeMap<(String, String), Vec<(u32, SupersedeRecord)>> = BTreeMap::new();
        for (name, found) in &copies {
            let Some((group, parsed)) = group_of(name) else {
                continue;
            };
            generations
                .entry(group.clone())
                .or_default()
                .push((parsed.generation, parsed.kind));
            if parsed.kind != LedgerKind::Supersede {
                continue;
            }
            for copy in found {
                let Ok(bytes) = &copy.bytes else {
                    continue;
                };
                let pair = (parsed.clone(), bytes.clone());
                if classify(&group.0, std::slice::from_ref(&pair)).is_err() {
                    continue;
                }
                if let Ok(record) = std::str::from_utf8(bytes).map(SupersedeRecord::parse)
                    && let Ok(record) = record
                {
                    records
                        .entry(group.clone())
                        .or_default()
                        .push((parsed.generation, record));
                }
            }
        }
        let pending = fs::read_to_string(dir.join(PENDING_FILE))
            .ok()
            .and_then(|text| Pending::parse(&text).ok());
        if fs::symlink_metadata(dir.join(PENDING_FILE)).is_ok() {
            report.lines.push(format!(
                "{}: an interrupted import's PENDING; left in place for the importer's resume \
                 (ADR-065 D5)",
                dir.join(PENDING_FILE).display()
            ));
        }

        // The facts per name.
        let mut names: BTreeMap<String, NameFacts> = BTreeMap::new();
        for (name, found) in &copies {
            let Some((group, parsed)) = group_of(name) else {
                continue;
            };
            let current = current_generation(generations.get(&group).cloned().unwrap_or_default());
            let kind = match parsed.kind {
                LedgerKind::Supersede => NameKind::Record,
                LedgerKind::Entry if Some(parsed.generation) == current => NameKind::Current,
                LedgerKind::Entry => NameKind::Superseded,
            };
            let group_records: Vec<&SupersedeRecord> = records
                .get(&group)
                .into_iter()
                .flatten()
                .map(|(_, record)| record)
                .collect();
            let state_of = |bytes: &[u8]| {
                file_state(name, kind, bytes, &group_records, &|files| {
                    entry_mismatches(files, &state, self.hot)
                })
            };
            let facts_of = |copy: &ReadCopy| CopyFacts {
                root: copy.root.clone(),
                path: copy.path.clone(),
                state: match &copy.bytes {
                    Ok(bytes) => state_of(bytes),
                    Err(error) => FileState::Unreadable(error.clone()),
                },
                bytes: copy.bytes.clone().ok(),
            };
            let hot = found.iter().find(|c| c.root == HOT_ID).map(facts_of);
            let targets = found
                .iter()
                .filter(|c| c.root != HOT_ID)
                .map(facts_of)
                .collect();
            let archive = records
                .get(&group)
                .and_then(|list| list.first().map(|(_, r)| r.archive.clone()))
                .or_else(|| {
                    found.iter().find_map(|copy| {
                        let text = std::str::from_utf8(copy.bytes.as_ref().ok()?).ok()?;
                        ImportLedger::parse(text).ok().map(|ledger| ledger.archive)
                    })
                })
                .or_else(|| {
                    pending
                        .as_ref()
                        .filter(|p| p.archive.rsplit('/').next() == Some(group.1.as_str()))
                        .map(|p| p.archive.clone())
                });
            let latest_record = records
                .get(&group)
                .into_iter()
                .flatten()
                .filter(|(generation, _)| *generation <= parsed.generation)
                .max_by_key(|(generation, _)| *generation);
            let record_generation = generations
                .get(&group)
                .into_iter()
                .flatten()
                .filter(|(g, k)| *k == LedgerKind::Supersede && *g <= parsed.generation)
                .map(|(g, _)| *g)
                .max();
            let pending_names_it = pending.as_ref().is_some_and(|p| {
                p.stream.stream() == group.0 && p.archive.rsplit('/').next() == Some(&group.1)
            });
            let closure = closure(
                name,
                kind,
                &dir,
                archive.as_deref(),
                record_generation.map(|g| {
                    latest_record
                        .filter(|(lg, _)| *lg == g)
                        .map(|(_, r)| r.quarantine.is_some())
                }),
                pending_names_it,
            );
            names.insert(
                name.clone(),
                NameFacts {
                    kind,
                    hot,
                    targets,
                    closure,
                },
            );
        }

        // X4: the decisions, all before the first write.
        let plan = plan_hot_import_ledger(&names);
        let mut stops = Vec::new();
        let mut writes: Vec<(&String, usize, bool)> = Vec::new();
        let mut kept = 0usize;
        for (name, decision) in &plan {
            match decision {
                NameDecision::Keep { differing } => {
                    kept += 1;
                    if !differing.is_empty() {
                        report.lines.push(format!(
                            "import-ledger file {name}: hot's copy is good and kept; the copy \
                             on {} differs: `mie store verify`, then `mie store repair` (R8) \
                             replaces it from hot",
                            differing.join(", ")
                        ));
                    }
                }
                NameDecision::Write { from, replace } => writes.push((name, *from, *replace)),
                NameDecision::Stop(why) => stops.push(why.clone()),
            }
        }
        let mut moves: Vec<(String, PathBuf, PathBuf)> = Vec::new();
        for label in &hot_listing.malformed {
            if fails_closed(label) {
                moves.push((label.clone(), dir.join(label), evidence.join(label)));
            } else {
                report.lines.push(format!(
                    "{label:?} on hot: no import-ledger name, and the readers skip it; left in \
                     place (ADR-065 D5)"
                ));
            }
        }
        report.lines.extend(target_malformed);
        report.lines.push(format!(
            "import ledger {}: {} name(s), {kept} good on hot and kept, {} to write to hot, {} \
             malformed name(s) to move aside, {} stop(s)",
            dir.display(),
            plan.len(),
            writes.len(),
            moves.len(),
            stops.len()
        ));
        if let Some(last) = stops.pop() {
            report.problems.extend(stops);
            return Err(Stop(last));
        }
        for (name, _, replace) in &writes {
            if *replace && fs::symlink_metadata(evidence.join(name)).is_ok() {
                return Err(Stop(format!(
                    "{} exists: evidence is never overwritten; nothing is written, and a re-run, \
                     with a new `at_ms`, completes it (ADR-065 D6)",
                    evidence.join(name).display()
                )));
            }
        }
        for (_, _, aside) in &moves {
            if fs::symlink_metadata(aside).is_ok() {
                return Err(Stop(format!(
                    "{} exists: evidence is never overwritten; nothing is written, and a re-run, \
                     with a new `at_ms`, completes it (ADR-065 D6)",
                    aside.display()
                )));
            }
        }

        // X5: the malformed names first, then the names in name order.
        let mut first = true;
        for (label, path, aside) in &moves {
            replica::move_file_aside(path, aside).map_err(|error| {
                Stop(format!(
                    "move {label:?} on hot aside to {}: {error}; the run stops and completes \
                     once hot accepts the write (ADR-065 D6)",
                    aside.display()
                ))
            })?;
            report.written += 1;
            report.lines.push(format!(
                "{label:?} on hot: a malformed import-ledger name the readers fail its archive \
                 file closed on; moved aside to {}, never deleted (ADR-065 D5)",
                aside.display()
            ));
            if first {
                first = false;
                reached(Step::MalformedMoved)?;
            }
        }
        for (name, from, replace) in writes {
            let source = &names[name].targets[from];
            let dest = dir.join(name);
            let Some(bytes) = source.bytes.as_deref() else {
                continue;
            };
            if replace {
                let aside = evidence.join(name);
                let mut linked = false;
                let result = replica::replace_with_evidence(
                    TailSource::Bytes(bytes),
                    &dest,
                    bytes.len() as u64,
                    false,
                    &aside,
                    true,
                    &mut |step| {
                        if step == ReplaceEvidenceStep::Linked {
                            linked = true;
                        }
                        replace_step(step)
                    },
                );
                if let Err(error) = result {
                    if linked {
                        report.written += 1;
                        report.lines.push(format!(
                            "evidence: hot's {name} is linked to {}; the file is unchanged",
                            aside.display()
                        ));
                    }
                    return Err(Stop(format!(
                        "replace hot's import-ledger file {name}: {error}; hot still holds the \
                         file, and a re-run, with a new `at_ms`, completes it (ADR-065 D6)"
                    )));
                }
                report.written += 1;
                report.lines.push(format!(
                    "import-ledger file {name}: hot's copy is {}; replaced by the good copy on \
                     {}, read-only; its former bytes are linked to {}, never deleted (ADR-065 D5, \
                     D6)",
                    names[name]
                        .hot
                        .as_ref()
                        .map_or_else(String::new, |hot| hot.state.describe()),
                    source.root,
                    aside.display()
                ));
                reached(Step::Renamed)?;
            } else {
                replica::copy_immutable(&source.path, &dest).map_err(|error| {
                    Stop(format!(
                        "write import-ledger file {name} to hot: {error}; the run stops and \
                         completes once hot accepts the write (ADR-065 D6)"
                    ))
                })?;
                report.written += 1;
                report.lines.push(format!(
                    "import-ledger file {name}: absent on hot; written from the good copy on {} \
                     (ADR-065 D5)",
                    source.root
                ));
            }
        }
        Ok(())
    }
}

/// The closure by hand of a name with no good copy anywhere (ADR-065
/// D5, the plan's residual 3). `replace_record`: for an entry behind a
/// supersede record, whether that record is a replace's (`Some(None)`
/// when it cannot be read).
pub fn closure(
    name: &str,
    kind: NameKind,
    dir: &Path,
    archive: Option<&str>,
    replace_record: Option<Option<bool>>,
    pending: bool,
) -> String {
    let file = name.split_once('/').map_or(name, |(_, f)| f);
    let moves = format!(
        "With the importer stopped, move {name} aside by hand on hot (to {}.damaged-<at_ms>/\
         {name}, any unused time in ms) and on every copy target (to \
         <target>/.mie/damaged/<at_ms>/.mie/import-ledger/{name}, the path R8 uses)",
        dir.display()
    );
    let no_mie_command = |why: &str| {
        format!(
            "No mie command recreates it: {why}. Restore the file from a backup kept outside \
             mie, or write it again from its fields (the import-ledger format has a fixed key \
             order and no wall-clock field, so a correct re-derivation reproduces its bytes); \
             then run this command again, which checks it on hot, and `mie store verify`, \
             then `mie store repair` (R8), which bring the targets' copies in line. Until \
             then every reader fails {file}'s archive file closed"
        )
    };
    match kind {
        NameKind::Record => no_mie_command(
            "moving a supersede record aside can change its archive file's current entry \
             and drops the moved set its quarantine reads, and no write recreates a record's \
             generation",
        ),
        NameKind::Superseded => format!(
            "{moves}. Nothing needs to be recreated: no reader uses a superseded entry whose \
             bytes differ from what its record names, and one that does not parse only fails \
             its archive file closed"
        ),
        NameKind::Current => match replace_record {
            Some(Some(true)) => no_mie_command(
                "it follows a replace's supersede record, where the re-admit resumes the replace instead of writing the entry",
            ),
            Some(None) => no_mie_command(
                "the supersede record before it has no good copy either, so nothing tells \
                 whether a re-admit may write it",
            ),
            _ if pending => format!(
                "{moves}; then run the importer: its PENDING names this archive file, so its \
                 resume writes the entry (`mie store resolve` refuses while PENDING is set)"
            ),
            _ => format!(
                "{moves}; then `mie store rebuild --replace-hot-ledger --record-unlisted` \
                 (the parts it listed become `unlisted` holds; --replace-hot-ledger since \
                 hot's copy ledger is readable); then `mie store resolve --file {}`, which \
                 writes the entry from the held parts after its row check",
                archive.unwrap_or("<archive path, which `mie store resolve` proposes>")
            ),
        },
    }
}
