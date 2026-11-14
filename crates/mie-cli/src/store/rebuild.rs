//! The copy-ledger rebuild (ADR-062 D7, #193): `mie store rebuild`.
//!
//! An explicit step that restores hot's copy ledger from the ledger copies
//! on the copy targets, the manifests on every root, hot's journal and the
//! import ledger, while `mie ingest` keeps capturing. It is built like the
//! verification and the repair: refusals and whole-run locks first, then
//! reads in a fixed order, then a **pure** derivation of the records to add
//! ([`derive`], the seam the hot restore #195 reuses), then one atomic
//! install. The rules, as numbered in the plan of #193 (the tests cite
//! them):
//!
//! - **B0 Refusals and locks.** An empty `cold_roots`, a hot without its
//!   marker, an archive configuration that does not describe this store; a
//!   configured root, copy target or read-only, without its marker or with
//!   an unreadable one (the marker tells an unmounted root from an empty
//!   one), a cold marker with hot's id, one id on two roots, two roots on
//!   one device. Then, without waiting and held for the whole run: the
//!   copy-ledger hold, the pass lock (no pass extends a ledger copy or
//!   appends mid-run), the verify lock (no verification reads a snapshot
//!   the install replaces, B10) and the archive source's writer lock (no
//!   import mid-run). The live source's lock is not taken: `mie ingest`
//!   keeps capturing, and its degraded writer opens only hot-window dates.
//!   Hot's own ledger: absent or unreadable (`Integrity`, `Corrupt`)
//!   proceeds; readable is refused without `--replace-hot-ledger`; an `Io`
//!   error is refused. Archive paths without an archive configuration are
//!   refused, and so is a missing hot import-ledger directory while an
//!   archive part exists or a copy target holds entries: an unmounted or
//!   misconfigured directory reads as empty (review of #209, finding 2).
//! - **B1 Read order**, nothing written: every copy target's ledger copy;
//!   hot's journal up to the length J read now (a line cut at J is not
//!   parsed); every import entry on hot and on the targets, and hot's
//!   `PENDING`; hot's layout; each cold root's layout with every copy's
//!   data checked against that root's own manifest, and its temp names (a
//!   pending manifest's data checked against its text where the root
//!   lacks the manifest); the part names below each root's
//!   `.mie/damaged/` (B6a). The journal and the
//!   entries come first: a part is sealed before its `sealed` line or its
//!   entry, so a "no root holds it" verdict never comes from a listing
//!   that ran ahead of the record. An I/O error on a listing, a manifest,
//!   the journal, an entry, a ledger copy or a quarantined manifest stops
//!   the run; unreadable data is a damaged copy (B5), never an absence, and
//!   so is an unreadable pending manifest (row 10 of B6a; #195 amendment 2,
//!   U), which the rebuild did not read before #195.
//! - **B2 Start:** the longest verified prefix among the targets' ledger
//!   copies, configuration order on a tie, the header alone without one.
//! - **B3 Texts:** a root holds a path when it has its manifest. The
//!   distinct texts that parse and describe the path's own location, from
//!   every root and the start, are the path's texts; two of them are a D5
//!   conflict, a stop on a live path and a `manifest-conflict` hold on an
//!   archive path.
//! - **B4 Records for verified copies:** `manifest` when the start lacks
//!   one, `copy` per copy target with a verified copy the start does not
//!   record. A finding after a recorded copy stays open.
//! - **B5 Damaged copies** get no record and a `FAIL:` line.
//! - **B6 Lost:** a path named by the start, hot's journal or a current
//!   import entry that no root holds is a candidate, recorded `lost` only
//!   with `--record-lost`; a name outside the layout is reported, never
//!   recorded; a path the start records lost stays lost. Rule 4′ (#195
//!   amendment 2): a recorded path is held when hot holds it, a cold root
//!   holds its manifest (any) or its data file, or a root without the
//!   manifest holds a verified crash copy (a pending manifest byte-equal
//!   to the start's text whose data verifies); no other temp name or
//!   evidence holds it, and its candidate line names every form, all left
//!   in place. A path the start records lost is a `FAIL:` ("a root holds
//!   it again") only when hot holds it or a cold root holds a parseable
//!   manifest of it; any other form is a report line, left in place.
//! - **B6a Cold-only paths** (#195, shared by both modes). A path no start
//!   record other than `finding` and `lost` names, that hot lacks (neither
//!   its manifest nor its data) and that a cold root holds in any form or
//!   whose part name sits below a root's `.mie/damaged/<dir>/`: (1) its
//!   text comes from B3/B4 over the roots' manifests; without one (1′),
//!   from the parseable pending manifests (`M~`) whose data (`D`, else
//!   `D~`) verifies, of the roots without the manifest, one distinct text,
//!   or for an archive path the one a current entry lists; `M~` never
//!   enters B3's conflict check. (2) With a text it gets its `manifest`
//!   record, its data verified or not, on a copy target or a read-only
//!   root (`copy` only for a verified `D`+`M` on a copy target). A pending
//!   form with the same text and data that does not verify is a damaged
//!   copy. (3) Without a text it is a lost candidate, origin `verify`
//!   unless a higher source names it, named by every form (B6's flag).
//!   Evidence names a path, never holds it. (6) Outcomes re-derive (#195
//!   amendment 2): a later run that finds the path recorded derives the
//!   same lost candidates and the same `FAIL:` paths. So the rebuild names
//!   every ordinary path hot lacks that no cold root holds as a data file
//!   and that only a verified crash copy holds, and a pending form with
//!   another text than a path's decided text is a damaged copy for every
//!   path hot lacks. Every branch needs hot to lack the path, so an intact
//!   hot's paths keep the derivation they had.
//! - **B7 Archive holds**, re-derived ([`archive_checks`]). A new
//!   `unlisted` hold comes from an absence, so it is written only with
//!   `--record-unlisted`; without it the rebuild reports the candidates,
//!   the import ledger's entry count and the target entries hot lacks, and
//!   installs nothing. Conflict holds are always written. "New" means no
//!   open hold: a start hold already cleared that the sources derive again
//!   gets a new `hold`.
//! - **B7′ Leftover start holds** (#210). A start hold the sources no
//!   longer derive is cleared (`hold-cleared <path> readmit`) only on
//!   positive evidence: its archive file has a current entry listing it
//!   with the SHA-256 of its one manifest text, and the hold is `unlisted`,
//!   or `entry-conflict` with that entry of generation ≥ 1 (a resolve wrote
//!   it). Every other one, `manifest-conflict` always, stays held and is
//!   named. So a full, a stale and an empty start agree after a re-admit.
//! - **B8 Removed:** a partition with a verified cold copy and no
//!   directory on hot; inside the hot window it is also a `FAIL:` line.
//! - **Q1–Q6 Quarantine** (step 5a, #211). Q1: each root's `.quarantine/`
//!   is read with the layouts (an I/O error stops the run). Q2–Q5 are
//!   [`archive_checks::quarantine`]: the set Q of moved and quarantined
//!   paths and their records; a Q path is complete when no layout of hot
//!   or of a copy target holds it, and a read-only root that holds it gets
//!   an info line (#216); a complete path whose data file only read-only
//!   layouts hold, with no quarantine copy on hot or a copy target, is a
//!   `FAIL:` line naming the remedy (review of #221). Q3: every Q path skips B4 to B8. Q6: their
//!   records follow B7′'s, as `manifest`, `hold-cleared … replace`,
//!   `quarantined` and `replaced`, each in path order; `records derived`
//!   counts them, and the report has one line per quarantine id.
//! - **B9 Install:** records per path, then `lost`, `removed`, `hold` and
//!   `hold-cleared`, each in path order, then the quarantine's (Q6);
//!   markers re-read, the old ledger linked aside, the gate
//!   written, the new ledger renamed into place
//!   ([`LedgerHold::replace`](mie_adapter_parquet::LedgerHold::replace)),
//!   then every ledger copy that is no prefix of it moved aside.
//! - **B10** The hot removal waits for a verification covering the rebuilt
//!   ledger ([`removal::plan`](mie_adapter_parquet::removal::plan) rule (g)).
//! - **B11** The report; `PASS` only with the ledger written and no `FAIL:`
//!   line.

use super::archive_checks::{
    self, ArchiveInputs, EntryCopy, PartTexts, QuarantineInputs, RootQuarantine,
};
use super::replicate::{self, IMPORT_LEDGER_DIR, META_DIR, entry_files, is_archive_part};
use super::{DeviceProbe, check_devices, check_hot};
use crate::config::IngestConfig;
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_binance::archive::ledger::{LedgerName, PENDING_FILE, Pending};
use mie_adapter_parquet::copy_ledger::{
    self, CopyLedger, HoldReason, Kind, LedgerBuilder, LedgerCopyCheck, LostOrigin, Record,
    Resolution,
};
use mie_adapter_parquet::removal;
use mie_adapter_parquet::replica::{self, CopyCheck, QuarantineListing};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::verification::{CopyState, PathVerdict};
use mie_ports::raw::RawStoreError;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod run_tests;

/// A test hook that runs once.
#[cfg(test)]
type Hook = std::cell::RefCell<Option<Box<dyn FnOnce()>>>;

#[cfg(test)]
thread_local! {
    /// Test hook: runs once between the journal read and the listings.
    pub(crate) static AFTER_JOURNAL: Hook = std::cell::RefCell::new(None);
    /// Test hook: runs once after the derivation, before the install's
    /// marker re-read.
    pub(crate) static BEFORE_INSTALL: Hook = std::cell::RefCell::new(None);
}

/// Runs a test hook once.
#[cfg(test)]
fn hook(slot: &'static std::thread::LocalKey<Hook>) {
    if let Some(hook) = slot.with(|hook| hook.borrow_mut().take()) {
        hook();
    }
}

/// The flags of `mie store rebuild`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Flags {
    /// `--record-lost`: write the `lost` records (#193 STOP 1 decision 1).
    pub record_lost: bool,
    /// `--record-unlisted`: write the `unlisted` holds. Like `lost`, a
    /// hold derived from an absence (no current entry lists the part) needs
    /// an explicit resolution (`mie store resolve`), and an empty or
    /// unmounted import ledger reads as no entries (re-review of #209).
    pub record_unlisted: bool,
    /// `--replace-hot-ledger`: replace a readable hot ledger (decision 2).
    pub replace_hot_ledger: bool,
}

/// What one rebuild did: its report lines and every problem that makes it
/// fail.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RebuildReport {
    /// The report in B11 order, up to `wrote …` or `nothing written`.
    pub lines: Vec<String>,
    /// Every refusal, stop and finding; empty when the rebuild passed.
    pub problems: Vec<String>,
}

// ------------------------------------------------------------- the inputs

/// How the data of one copy compares with that root's own manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataCheck {
    /// Not read (hot: its copies never count, D11).
    NotChecked,
    /// Size and SHA-256 match.
    Good,
    /// The manifest is there, the data is not.
    Missing,
    /// The size differs.
    Size,
    /// The SHA-256 differs.
    Sha256,
    /// The data cannot be read: a damaged copy, never an absence.
    Unreadable(String),
}

/// One root's copy of a sealed part: it has the part's manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootCopy {
    /// The manifest parses and describes the part's own location.
    Text {
        /// The manifest text, verbatim.
        text: String,
        /// The data against it.
        data: DataCheck,
    },
    /// The manifest is not UTF-8, does not parse or names another
    /// location: a damaged copy, never a second text.
    BadText(String),
}

/// The seal temp names one cold root holds of a part (B6a): the states a
/// crashed D5 copy leaves (`D~` = `.part-N.parquet.tmp`, `M~` =
/// `.part-N.manifest.tmp`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TempForms {
    /// Whether its data temp file `D~` exists.
    pub data_tmp: bool,
    /// Its pending manifest `M~`, classified as [`classify_text`] does;
    /// `None` when there is none. On a root without the part's manifest,
    /// and where the root's data is checked, a text's data is its data
    /// file `D`, else `D~`, checked against that text; otherwise
    /// [`DataCheck::NotChecked`].
    pub pending: Option<RootCopy>,
}

/// One cold root's layout, in configuration order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootRead {
    /// Its marker's id.
    pub id: String,
    /// Whether it is a copy target.
    pub copy_target: bool,
    /// Every part it has a manifest of.
    pub files: BTreeMap<String, RootCopy>,
    /// The data files it holds without their manifest.
    pub orphan_data: BTreeSet<String>,
    /// Every part it holds a seal temp name of (B6a).
    pub temps: BTreeMap<String, TempForms>,
    /// Every part a name below its `.mie/damaged/<dir>/` names, with those
    /// directories (B6a row 16): evidence, never a source.
    pub evidence: BTreeMap<String, BTreeSet<String>>,
    /// Its `.quarantine/` directory (Q1).
    pub quarantine: QuarantineListing,
}

/// Hot's layout.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HotRead {
    /// Every part it has a manifest of, data not checked.
    pub files: BTreeMap<String, RootCopy>,
    /// The data files it holds without their manifest.
    pub orphan_data: BTreeSet<String>,
    /// Its `date=` partition directories (a removal's hidden leftover is
    /// none).
    pub partitions: BTreeSet<String>,
    /// Every part a name below its `.mie/damaged/<dir>/` names (B6a row
    /// 16).
    pub evidence: BTreeMap<String, BTreeSet<String>>,
    /// Its `.quarantine/` directory (Q1).
    pub quarantine: QuarantineListing,
}

/// One copy target's ledger copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerCopyRead {
    /// The target's id.
    pub id: String,
    /// Its bytes, `None` when it holds none or it cannot be read.
    pub bytes: Option<Vec<u8>>,
    /// Its read error: B2 rejects it as a copy that does not parse. Only
    /// the hot restore passes one (#195 amendment 2, U); the rebuild stops
    /// on it.
    pub error: Option<String>,
}

/// A data path named by hot's journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalName {
    /// The path, as written.
    pub path: String,
    /// The journal line, from 1.
    pub line: u64,
    /// The line's `run_id`.
    pub run_id: String,
    /// From a `recovery` line's `rolled_forward`, not a `sealed` line.
    pub recovery: bool,
}

/// Hot's journal up to the length read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JournalRead {
    /// The length read, J.
    pub bytes: u64,
    /// Complete lines that are no JSON: counted, not parsed.
    pub non_json: u64,
    /// The data paths of its `sealed` and `recovery` lines.
    pub names: Vec<JournalName>,
}

/// What runs the derivation: the rebuild, or the hot restore (#195 H5),
/// which reuses it with seven differences, (a) to (g) of its plan. B6a,
/// which brings every path a cold root holds into the ledger's numbering,
/// is no difference: both modes apply it. Only [`Mode::Restore`] changes
/// anything; [`run`] always passes [`Mode::Rebuild`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    /// `mie store rebuild`: B0 to B11 as above.
    Rebuild,
    /// `mie store restore` (the module [`restore`](super::restore)).
    Restore(RestoreMode),
}

/// What the hot restore adds to the derivation's inputs (#195 H5).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestoreMode {
    /// Hot's own copy ledger when it survived, the first start candidate
    /// (a): it holds every record the copies hold. On a tie it wins, then
    /// configuration order.
    pub hot_ledger: Option<Vec<u8>>,
    /// The partitions the copy-back plans to bring back (H6): B8 counts
    /// them as on hot (e), since they are on hot once the restore commits.
    pub copy_back: BTreeSet<String>,
}

impl Mode {
    /// The restore's additions, `None` for the rebuild.
    fn restore(&self) -> Option<&RestoreMode> {
        match self {
            Self::Rebuild => None,
            Self::Restore(restore) => Some(restore),
        }
    }
}

/// Everything [`derive`] reads (B1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildInputs {
    /// The rebuild or the hot restore.
    pub mode: Mode,
    /// The run's `at_ms`, read once: every record carries it.
    pub at_ms: i64,
    /// The first date of the hot window.
    pub first_hot_date: String,
    /// `--record-lost`.
    pub record_lost: bool,
    /// `--record-unlisted`.
    pub record_unlisted: bool,
    /// Hot's import-ledger directory as configured, named in the report.
    pub import_ledger: Option<String>,
    /// Whether an archive configuration was given.
    pub archive_configured: bool,
    /// Hot's import-ledger directory (`paths.import_ledger`), named, when
    /// it does not exist: an unmounted or misconfigured directory reads as
    /// empty, so the derivation stops when it could hide entries.
    pub missing_import_ledger: Option<String>,
    /// The copy targets' ledger copies, in configuration order.
    pub ledger_copies: Vec<LedgerCopyRead>,
    /// Hot's journal.
    pub journal: JournalRead,
    /// Every copy of every import entry by name: hot's first.
    pub entries: BTreeMap<String, Vec<EntryCopy>>,
    /// Hot's `PENDING`.
    pub pending: Option<Result<Pending, String>>,
    /// Hot's layout.
    pub hot: HotRead,
    /// The cold roots' layouts, in configuration order.
    pub roots: Vec<RootRead>,
}

// ------------------------------------------------------------ the result

/// The start the derivation chose (B2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Start {
    /// The copy target whose ledger copy it is; `None` for the header
    /// alone.
    pub root: Option<String>,
    /// Its verified length in bytes.
    pub len: u64,
    /// Its records.
    pub records: usize,
}

/// A lost candidate (B6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostFile {
    /// The data path.
    pub path: String,
    /// Its origin, by precedence.
    pub origin: LostOrigin,
    /// Its SHA-256 when the origin knows it.
    pub sha256: Option<String>,
    /// Every source that names it.
    pub sources: Vec<String>,
    /// Whether its `lost` record is in the ledger (`--record-lost`).
    pub recorded: bool,
}

/// What [`derive`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Derived {
    /// The start.
    pub start: Start,
    /// Every rejected ledger copy, with its reason.
    pub rejected: Vec<String>,
    /// The rebuilt ledger.
    pub ledger: Vec<u8>,
    /// The records added to the start, in order.
    pub added: Vec<Record>,
    /// Every lost candidate.
    pub lost: Vec<LostFile>,
    /// Every derived hold, in path order.
    pub holds: Vec<(String, HoldReason)>,
    /// The new `unlisted` holds left unrecorded without
    /// `--record-unlisted`: the rebuild then installs nothing.
    pub unrecorded_unlisted: Vec<String>,
    /// Report lines (B11): the start, the rejected copies, the journal,
    /// the records derived, the lost files, the holds. They state what the
    /// rebuilt ledger holds, never that it was written: only the install's
    /// `wrote …` line does.
    pub lines: Vec<String>,
    /// Every `FAIL:` finding.
    pub problems: Vec<String>,
    /// The bytes of each current import entry the archive checks decided
    /// on ([`archive_checks::ArchiveFindings::chosen`]): what the hot
    /// restore writes to hot (#195 H4).
    pub chosen_entries: BTreeMap<String, Vec<u8>>,
}

/// Why the rebuild stops with nothing written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stop(pub String);

/// What names a path for the lost check (B6).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Source {
    /// A record of the start, by kind.
    Ledger(&'static str),
    /// A current import entry, with the SHA-256 it lists.
    Import(String, String),
    /// A journal `sealed` line.
    Journal(u64, String),
    /// A journal `recovery` line.
    Recovery(u64, String),
    /// B6a: a cold root's data file without its manifest.
    Data(String),
    /// B6a: a cold root's manifest that does not parse or names another
    /// location, with why.
    BadManifest(String, String),
    /// B6a: a cold root's data temp file.
    DataTmp(String),
    /// B6a: a cold root's pending manifest that does not parse or names
    /// another location, with why.
    BadPending(String, String),
    /// B6a: a cold root's parseable pending manifest whose data verifies
    /// (row 11: with no decided text).
    Pending(String),
    /// B6a rule 1′: a cold root's parseable pending manifest whose data
    /// (`D`, else `D~`) does not verify against it, with why (row 7).
    PendingBadData(String, String),
    /// B6a row 16: a part name below `.mie/damaged/<dir>/` of a root
    /// (`hot` or a cold root's id), with `<dir>`.
    Evidence(String, String),
}

impl Source {
    fn describe(&self) -> String {
        match self {
            Self::Ledger(kind) => format!("the start copy's `{kind}` record"),
            Self::Import(entry, _) => format!("import entry {entry}"),
            Self::Journal(line, run) => format!("journal line {line} (run {run}, `sealed`)"),
            Self::Recovery(line, run) => {
                format!("journal line {line} (run {run}, `recovery` rolled_forward)")
            }
            Self::Data(root) => format!("the data file without its manifest on {root}"),
            Self::BadManifest(root, why) => format!(
                "a manifest on {root} that does not parse or names another location ({why})"
            ),
            Self::DataTmp(root) => format!("the data temp file on {root}"),
            Self::BadPending(root, why) => format!(
                "a pending manifest on {root} that does not parse or names another location \
                 ({why})"
            ),
            Self::Pending(root) => format!("a pending manifest on {root}"),
            Self::PendingBadData(root, why) => {
                format!("a pending manifest on {root} whose data does not verify ({why})")
            }
            Self::Evidence(root, dir) => format!(
                "a part name below .mie/damaged/{dir}/ on {root} (evidence, never a source)"
            ),
        }
    }

    /// Whether it is a form a root holds (B6a), not a record or a line.
    fn is_cold_form(&self) -> bool {
        matches!(
            self,
            Self::Data(_)
                | Self::BadManifest(..)
                | Self::DataTmp(_)
                | Self::BadPending(..)
                | Self::Pending(_)
                | Self::PendingBadData(..)
                | Self::Evidence(..)
        )
    }
}

// -------------------------------------------------------- pure functions

/// Parses hot's journal bytes read up to J (B1): every complete line, a
/// line cut at J excluded; a complete line that is no JSON is counted and
/// skipped, as `read_runs` skips it.
pub fn parse_journal(bytes: &[u8]) -> JournalRead {
    let mut read = JournalRead {
        bytes: bytes.len() as u64,
        ..JournalRead::default()
    };
    for (index, line) in bytes.split_inclusive(|&b| b == b'\n').enumerate() {
        let Some(line) = line.strip_suffix(b"\n") else {
            break;
        };
        let number = index as u64 + 1;
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            read.non_json += 1;
            continue;
        };
        let run_id = value["run_id"].as_str().unwrap_or("?").to_owned();
        let (paths, recovery): (Vec<&Value>, bool) = match value["type"].as_str() {
            Some("sealed") => (
                value["files"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|file| &file["path"])
                    .collect(),
                false,
            ),
            Some("recovery") => (
                value["rolled_forward"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .collect(),
                true,
            ),
            _ => continue,
        };
        for path in paths.into_iter().filter_map(Value::as_str) {
            read.names.push(JournalName {
                path: path.to_owned(),
                line: number,
                run_id: run_id.clone(),
                recovery,
            });
        }
    }
    read
}

/// What a manifest's bytes make of the copy of `path` (B3): its text when
/// it parses and describes `path`, otherwise a damaged copy. The data is
/// not checked here.
pub fn classify_text(path: &str, bytes: &[u8]) -> RootCopy {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return RootCopy::BadText("the manifest is not UTF-8".to_owned());
    };
    match replica::parse_manifest(text) {
        Err(error) => RootCopy::BadText(format!("the manifest does not parse: {error}")),
        Ok(file) if file.relative_path != path => RootCopy::BadText(format!(
            "the manifest describes {}, not its own location",
            file.relative_path
        )),
        Ok(_) => RootCopy::Text {
            text: text.to_owned(),
            data: DataCheck::NotChecked,
        },
    }
}

/// The partition directory of a data path.
fn partition_of(path: &str) -> &str {
    path.rsplit_once('/')
        .map_or(path, |(partition, _)| partition)
}

/// The date of a partition directory.
fn date_of(partition: &str) -> &str {
    partition.rsplit_once("/date=").map_or("", |(_, date)| date)
}

/// One start candidate of B2: a root's id, its ledger copy's bytes, and
/// the read error of a copy that cannot be read (the hot restore only).
pub(super) type Candidate<'a> = (&'a str, Option<&'a [u8]>, Option<&'a str>);

/// The start candidates of B2 in their order: the copy targets' ledger
/// copies, and in [`Mode::Restore`] hot's own ledger first (H5 a).
pub(super) fn start_candidates<'a>(
    mode: &'a Mode,
    copies: &'a [LedgerCopyRead],
) -> Vec<Candidate<'a>> {
    let hot = mode
        .restore()
        .and_then(|restore| restore.hot_ledger.as_deref())
        .map(|bytes| (HOT_ID, Some(bytes), None));
    hot.into_iter()
        .chain(copies.iter().map(|copy| {
            (
                copy.id.as_str(),
                copy.bytes.as_deref(),
                copy.error.as_deref(),
            )
        }))
        .collect()
}

/// B2: the start among the ledger copies, its builder and its ledger.
pub(super) fn choose_start(
    copies: &[Candidate<'_>],
) -> (Start, LedgerBuilder, copy_ledger::Ledger, Vec<String>) {
    let mut rejected = Vec::new();
    let mut best: Option<(usize, copy_ledger::Ledger)> = None;
    for (i, &(id, bytes, error)) in copies.iter().enumerate() {
        if let Some(error) = error {
            rejected.push(format!(
                "the ledger copy on {id}: rejected: cannot be read ({error})"
            ));
            continue;
        }
        let Some(bytes) = bytes else {
            rejected.push(format!("the ledger copy on {id}: rejected: absent"));
            continue;
        };
        let name = PathBuf::from(format!("{id}:.mie/copy-ledger"));
        match copy_ledger::parse(&name, bytes) {
            Ok(ledger) => {
                if best
                    .as_ref()
                    .is_none_or(|(_, known)| ledger.verified_len > known.verified_len)
                {
                    best = Some((i, ledger));
                }
            }
            Err(error) => rejected.push(format!("the ledger copy on {id}: rejected: {error}")),
        }
    }
    match best {
        Some((i, ledger)) => {
            let bytes = copies[i].1.unwrap_or_default();
            let builder = LedgerBuilder::from_prefix(Path::new("start"), bytes)
                .unwrap_or_else(|_| LedgerBuilder::empty());
            let start = Start {
                root: Some(copies[i].0.to_owned()),
                len: builder.len(),
                records: ledger.records.len(),
            };
            (start, builder, ledger, rejected)
        }
        None => {
            let builder = LedgerBuilder::empty();
            let ledger = copy_ledger::parse(Path::new("start"), copy_ledger::HEADER.as_bytes())
                .expect("the header alone is a ledger");
            let start = Start {
                root: None,
                len: builder.len(),
                records: 0,
            };
            (start, builder, ledger, rejected)
        }
    }
}

/// The data path a record names, with its kind (B6 sources).
fn named_path(kind: &Kind) -> Option<String> {
    match kind {
        Kind::Manifest { text } => replica::parse_manifest(text).ok().map(|f| f.relative_path),
        Kind::Copy { path, .. }
        | Kind::Lost { path, .. }
        | Kind::Hold { path, .. }
        | Kind::HoldCleared { path, .. }
        | Kind::Quarantined { path, .. }
        | Kind::Replaced { path, .. }
        | Kind::Finding {
            subject: copy_ledger::Subject::File(path),
            ..
        } => Some(path.clone()),
        _ => None,
    }
}

/// Every data path a root's layout or `.mie/damaged/` names: hot's and
/// the cold roots' manifests, data files, temp names and evidence (B0).
fn layout_paths(inputs: &RebuildInputs) -> impl Iterator<Item = &String> {
    inputs
        .hot
        .files
        .keys()
        .chain(&inputs.hot.orphan_data)
        .chain(inputs.hot.evidence.keys())
        .chain(inputs.roots.iter().flat_map(|root| {
            root.files
                .keys()
                .chain(&root.orphan_data)
                .chain(root.temps.keys())
                .chain(root.evidence.keys())
        }))
}

/// B6a rule 1′ for a path without a manifest text: the parseable pending
/// texts of the roots without the path's manifest whose data (`D`, else
/// `D~`) verifies against them, as `(root, text)` in configuration order.
/// Why only those: the D5 copy writes `M~` before its `Verify` step and
/// renames `M` only after it, so an `M` is a seal and an `M~` becomes one
/// only through verified data (#195 amendment 2).
fn pending_texts<'a>(inputs: &'a RebuildInputs, path: &str) -> Vec<(&'a str, &'a str)> {
    inputs
        .roots
        .iter()
        .filter(|root| !root.files.contains_key(path))
        .filter_map(|root| match root.temps.get(path)?.pending.as_ref()? {
            RootCopy::Text {
                text,
                data: DataCheck::Good,
            } => Some((root.id.as_str(), text.as_str())),
            RootCopy::Text { .. } | RootCopy::BadText(_) => None,
        })
        .collect()
}

/// Rule 4′: whether `root` holds a verified crash copy of `path` with one
/// of `texts`: no manifest there, and a pending manifest byte-equal to
/// one of them whose data verifies against it (#195 amendment 2).
fn verified_crash_copy(root: &RootRead, path: &str, texts: &[&str]) -> bool {
    !root.files.contains_key(path)
        && matches!(
            root.temps.get(path).and_then(|temps| temps.pending.as_ref()),
            Some(RootCopy::Text { text, data: DataCheck::Good }) if texts.contains(&text.as_str())
        )
}

/// Why a data check failed, for a report line.
fn data_why(data: &DataCheck) -> String {
    match data {
        DataCheck::NotChecked => "not checked".to_owned(),
        DataCheck::Good => "good".to_owned(),
        DataCheck::Missing => "missing".to_owned(),
        DataCheck::Size => "size".to_owned(),
        DataCheck::Sha256 => "sha256".to_owned(),
        DataCheck::Unreadable(error) => format!("its data cannot be read ({error})"),
    }
}

/// B6a: the forms the roots hold of `path` other than a parseable
/// manifest (rows 4, 8 to 11, 16), in root order: every cold root's layout
/// forms, then the evidence of hot and of each cold root.
fn cold_forms(inputs: &RebuildInputs, path: &str) -> Vec<Source> {
    let mut forms = Vec::new();
    for root in &inputs.roots {
        let id = &root.id;
        if let Some(RootCopy::BadText(why)) = root.files.get(path) {
            forms.push(Source::BadManifest(id.clone(), why.clone()));
        }
        if root.orphan_data.contains(path) {
            forms.push(Source::Data(id.clone()));
        }
        if let Some(temps) = root.temps.get(path) {
            if temps.data_tmp {
                forms.push(Source::DataTmp(id.clone()));
            }
            match &temps.pending {
                Some(RootCopy::BadText(why)) => {
                    forms.push(Source::BadPending(id.clone(), why.clone()));
                }
                Some(RootCopy::Text {
                    data: DataCheck::Good,
                    ..
                }) => forms.push(Source::Pending(id.clone())),
                // Rule 1′: a pending text without verified data gives no
                // text; on a root with the manifest it is not checked.
                Some(RootCopy::Text { data, .. }) if !root.files.contains_key(path) => {
                    forms.push(Source::PendingBadData(id.clone(), data_why(data)));
                }
                Some(RootCopy::Text { .. }) => forms.push(Source::Pending(id.clone())),
                None => {}
            }
        }
    }
    let evidence = std::iter::once(("hot", &inputs.hot.evidence)).chain(
        inputs
            .roots
            .iter()
            .map(|root| (root.id.as_str(), &root.evidence)),
    );
    for (id, evidence) in evidence {
        for dir in evidence.get(path).into_iter().flatten() {
            forms.push(Source::Evidence(id.to_owned(), dir.clone()));
        }
    }
    forms
}

/// The derivation of B2 to B8 over what B1 read: pure.
///
/// # Errors
///
/// A [`Stop`] for a live manifest conflict or archive parts without an
/// archive configuration; nothing may be written then.
pub fn derive(inputs: &RebuildInputs) -> Result<Derived, Stop> {
    let mut lines = Vec::new();
    let mut problems = Vec::new();
    let restore = inputs.mode.restore();

    // B2 (in the restore, hot's own ledger first: H5 a).
    let (start, mut builder, start_ledger, rejected) =
        choose_start(&start_candidates(&inputs.mode, &inputs.ledger_copies));
    let state = CopyState::fold(&start_ledger)
        .map_err(|error| Stop(format!("the start copy cannot be folded: {error}")))?;
    let mut start_texts: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut sources: BTreeMap<String, BTreeSet<Source>> = BTreeMap::new();
    // The paths a start record other than `finding` and `lost` names: the
    // installed ledger keeps them, so the writer's numbering counts them
    // (D6). The restore's rules for unrecorded cold copies (B4, B6) skip
    // them.
    let mut start_named: BTreeSet<String> = BTreeSet::new();
    for record in &start_ledger.records {
        if !matches!(record.kind, Kind::Finding { .. } | Kind::Lost { .. })
            && let Some(path) = named_path(&record.kind)
        {
            start_named.insert(path);
        }
        if let Kind::Manifest { text } = &record.kind
            && let Ok(file) = replica::parse_manifest(text)
        {
            let texts = start_texts.entry(file.relative_path).or_default();
            if !texts.contains(text) {
                texts.push(text.clone());
            }
        }
        if let Some(path) = named_path(&record.kind) {
            sources
                .entry(path)
                .or_default()
                .insert(Source::Ledger(record.kind.name()));
        }
    }
    match &start.root {
        Some(root) => lines.push(format!(
            "start: the ledger copy on {root}, {} bytes, {} records",
            start.len, start.records
        )),
        None => lines.push(
            "started empty: no ledger copy verifies, so only the root listing and the journal \
             and import-ledger cross-check protect against a missing file (ADR-062 D7 step 1)"
                .to_owned(),
        ),
    }
    lines.extend(rejected.iter().cloned());
    // U (#195 amendment 2): in the restore an unreadable ledger copy is a
    // damaged copy, never a stop.
    for copy in inputs.ledger_copies.iter().filter(|_| restore.is_some()) {
        if let Some(error) = &copy.error {
            problems.push(format!(
                "the ledger copy on {} cannot be read ({error}): it is no start candidate, and \
                 the install names it and leaves it in place. A read error can mean a failing \
                 disk: run `mie store verify`, then `mie store repair`, for what verify finds on \
                 that target (ADR-062 D11)",
                copy.id
            ));
        }
    }
    lines.push(format!(
        "journal: {} byte(s) read, {} complete non-JSON line(s) counted and not parsed",
        inputs.journal.bytes, inputs.journal.non_json
    ));

    // B0, last refusal: archive paths need the archive configuration.
    if !inputs.archive_configured {
        let held = layout_paths(inputs)
            .chain(sources.keys())
            .find(|path| is_archive_part(path));
        if let Some(path) = held {
            return Err(Stop(format!(
                "{path} is an archive part, and no archive configuration is given \
                 (--archive-config or [history] archive_config): without the import ledger the \
                 rebuild can neither cross-check archive files nor derive their holds (ADR-062 \
                 D7)"
            )));
        }
    }

    // B0 again: a missing hot import ledger reads as empty, and every
    // archive part only hot holds would become an `unlisted` hold that only
    // `mie store resolve` clears (review of #209, finding 2). It
    // stops the rebuild wherever it could hide an entry: an archive part
    // on a root or in the start, or a copy target holding entries. The
    // restore restores hot's import ledger itself (H5 f).
    if let Some(dir) = inputs
        .missing_import_ledger
        .as_ref()
        .filter(|_| restore.is_none())
    {
        let part = layout_paths(inputs)
            .chain(sources.keys())
            .find(|path| is_archive_part(path));
        let why = match (part, inputs.entries.keys().next()) {
            (Some(path), _) => Some(format!("archive part {path} exists")),
            (None, Some(entry)) => Some(format!("the copy targets hold import entry {entry}")),
            (None, None) => None,
        };
        if let Some(why) = why {
            return Err(Stop(format!(
                "hot's import ledger {dir} (paths.import_ledger of the archive configuration) \
                 does not exist, while {why}: an unmounted or misconfigured directory reads as \
                 empty and would turn every archive part only hot holds into a permanent \
                 `unlisted` hold. Mount it or fix the archive configuration, then run the \
                 rebuild again; nothing is written (ADR-062 D7)"
            )));
        }
    }

    // B6a: the paths no start record other than `finding` and `lost` names
    // and hot lacks (neither its manifest nor its data), that a cold root
    // holds in any form or a root's evidence names. Every B6a branch is
    // limited to them, so the paths an intact hot holds keep today's
    // derivation in both modes.
    let hot_holds =
        |path: &str| inputs.hot.files.contains_key(path) || inputs.hot.orphan_data.contains(path);
    let cold_only: BTreeSet<&str> = inputs
        .roots
        .iter()
        .flat_map(|root| {
            root.files
                .keys()
                .chain(&root.orphan_data)
                .chain(root.temps.keys())
                .chain(root.evidence.keys())
        })
        .chain(inputs.hot.evidence.keys())
        .map(String::as_str)
        .filter(|path| !start_named.contains(*path) && !hot_holds(path))
        .collect();
    let has_m_text = |path: &str| {
        inputs
            .roots
            .iter()
            .any(|root| matches!(root.files.get(path), Some(RootCopy::Text { .. })))
    };
    // B6a rule 1 for the archive checks: a path whose only text is one
    // distinct pending text counts that text as its roots' text.
    let pending_single: BTreeMap<&str, Vec<(&str, &str)>> = cold_only
        .iter()
        .filter(|path| !has_m_text(path) && state.verdict(path) != Some(PathVerdict::Lost))
        .filter_map(|path| {
            let found = pending_texts(inputs, path);
            let mut texts: Vec<&str> = found.iter().map(|(_, text)| *text).collect();
            texts.sort_unstable();
            texts.dedup();
            (texts.len() == 1).then_some((*path, found))
        })
        .collect();

    // B3: the texts of every path any root or the start holds.
    let mut holders: BTreeMap<&str, Vec<(&str, &str)>> = BTreeMap::new();
    for (path, copy) in &inputs.hot.files {
        let slot = holders.entry(path).or_default();
        if let RootCopy::Text { text, .. } = copy {
            slot.push(("hot", text));
        }
    }
    for root in &inputs.roots {
        for (path, copy) in &root.files {
            let slot = holders.entry(path).or_default();
            if let RootCopy::Text { text, .. } = copy {
                slot.push((&root.id, text));
            }
        }
    }
    for (path, texts) in &start_texts {
        let slot = holders.entry(path).or_default();
        for text in texts {
            slot.push(("the start copy", text));
        }
    }
    let distinct = |path: &str| -> Vec<&str> {
        let mut texts: Vec<&str> = holders
            .get(path)
            .into_iter()
            .flatten()
            .map(|(_, text)| *text)
            .collect();
        texts.sort_unstable();
        texts.dedup();
        texts
    };
    // In the restore, a live path's one `manifest` record in the start
    // decides its text, and a copy with another text is a damaged copy (H5
    // b, ADR-062 D11 step 5).
    let recorded_text = |path: &str| -> Option<&str> {
        restore?;
        match start_texts.get(path).map(Vec::as_slice) {
            Some([only]) => Some(only.as_str()),
            _ => None,
        }
    };
    let mut conflicts = Vec::new();
    for (path, held) in &holders {
        let texts = distinct(path);
        if texts.len() > 1 && !is_archive_part(path) && recorded_text(path).is_none() {
            let groups: Vec<String> = texts
                .iter()
                .map(|text| {
                    let on: Vec<&str> = held
                        .iter()
                        .filter(|(_, t)| t == text)
                        .map(|(root, _)| *root)
                        .collect();
                    on.join(", ")
                })
                .collect();
            conflicts.push(format!(
                "{path} carries different manifest texts on {}",
                groups.join(" and on ")
            ));
        }
    }
    if !conflicts.is_empty() {
        return Err(Stop(format!(
            "{}: one live path must carry byte-identical manifests everywhere, and nothing can \
             tell which is the sealed file; the rebuild stops with nothing written (ADR-062 D5, D7 \
             step 3)",
            conflicts.join("; ")
        )));
    }

    // Q2 to Q5: the quarantine, before B4 to B8, which skip its paths (Q3).
    let mut layout: BTreeMap<String, Vec<(String, Option<String>)>> = BTreeMap::new();
    let roots_layout = std::iter::once(("hot", &inputs.hot.files, &inputs.hot.orphan_data)).chain(
        inputs
            .roots
            .iter()
            .map(|root| (root.id.as_str(), &root.files, &root.orphan_data)),
    );
    for (root, files, orphans) in roots_layout {
        for (path, copy) in files {
            let text = match copy {
                RootCopy::Text { text, .. } => Some(text.clone()),
                RootCopy::BadText(_) => None,
            };
            layout
                .entry(path.clone())
                .or_default()
                .push((root.to_owned(), text));
        }
        for path in orphans {
            let slot = layout.entry(path.clone()).or_default();
            if !slot.iter().any(|(r, _)| r == root) {
                slot.push((root.to_owned(), None));
            }
        }
    }
    let quarantines: Vec<RootQuarantine> = std::iter::once(RootQuarantine {
        root: "hot".to_owned(),
        listing: inputs.hot.quarantine.clone(),
    })
    .chain(inputs.roots.iter().map(|root| RootQuarantine {
        root: root.id.clone(),
        listing: root.quarantine.clone(),
    }))
    .collect();
    // A read-only root's layout feeds the text rule, never the commit
    // point (#216).
    let read_only: BTreeSet<String> = inputs
        .roots
        .iter()
        .filter(|root| !root.copy_target)
        .map(|root| root.id.clone())
        .collect();
    // Q5's h = 0 rule counts data files: a cold copy whose data was
    // checked and is missing holds the manifest alone (review of #221).
    let mut without_data: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for root in &inputs.roots {
        for (path, copy) in &root.files {
            if matches!(
                copy,
                RootCopy::Text {
                    data: DataCheck::Missing,
                    ..
                }
            ) {
                without_data
                    .entry(path.clone())
                    .or_default()
                    .insert(root.id.clone());
            }
        }
    }
    let quarantine = archive_checks::quarantine(&QuarantineInputs {
        entries: &inputs.entries,
        roots: &quarantines,
        layout: &layout,
        read_only: &read_only,
        without_data: &without_data,
        start: &state,
    });
    let in_q = |path: &str| quarantine.paths.contains_key(path);

    // B7: the archive checks.
    let parts: BTreeMap<String, PartTexts> = holders
        .keys()
        .copied()
        .chain(sources.keys().map(String::as_str))
        .chain(pending_single.keys().copied())
        .filter(|path| is_archive_part(path) && !in_q(path))
        .map(|path| {
            let mut roots: Vec<(String, String)> = holders
                .get(path)
                .into_iter()
                .flatten()
                .filter(|(root, _)| *root != "the start copy")
                .map(|(root, text)| ((*root).to_owned(), (*text).to_owned()))
                .collect();
            if roots.is_empty()
                && let Some(found) = pending_single.get(path)
            {
                roots = found
                    .iter()
                    .map(|(root, text)| ((*root).to_owned(), (*text).to_owned()))
                    .collect();
            }
            let start = start_texts.get(path).cloned().unwrap_or_default();
            (path.to_owned(), PartTexts { roots, start })
        })
        .collect();
    let findings = archive_checks::check(&ArchiveInputs {
        entries: inputs.entries.clone(),
        parts,
        pending: inputs.pending.clone(),
    });
    for (name, files) in &findings.current {
        for (path, sha) in files {
            sources
                .entry(path.clone())
                .or_default()
                .insert(Source::Import(name.clone(), sha.clone()));
        }
    }
    for name in &inputs.journal.names {
        let source = if name.recovery {
            Source::Recovery(name.line, name.run_id.clone())
        } else {
            Source::Journal(name.line, name.run_id.clone())
        };
        sources.entry(name.path.clone()).or_default().insert(source);
    }

    // B4, B5: per path in path order, every path any root or the start
    // holds a manifest of and every B6a path.
    // B4's tie rule: the one text whose SHA-256 a current entry lists.
    let listed_one = |path: &str, texts: &[&'_ str]| -> Option<usize> {
        let listed: BTreeSet<&str> = findings
            .current
            .values()
            .flatten()
            .filter(|(listed, _)| listed == path)
            .map(|(_, sha)| sha.as_str())
            .collect();
        let matching: Vec<usize> = (0..texts.len())
            .filter(|&i| {
                replica::parse_manifest(texts[i])
                    .is_ok_and(|file| listed.contains(file.sha256.as_str()))
            })
            .collect();
        match matching[..] {
            [only] => Some(only),
            _ => None,
        }
    };
    // The remedy a `FAIL:` line for a damaged copy names (B5). The rebuild
    // keeps its text; in the restore it depends on the copy (#195 round 3):
    // verify, then repair (R1) only for a copy with a `copy` record on a
    // copy target, since verify checks only those (V4) and the repair
    // leaves a file on hot to its closing pass, whose D5 copy refuses the
    // damaged one. An unrecorded copy, or a temp name, on a copy target is
    // set aside by the restore after the path's copy-back (H6 k); a
    // read-only root is never written (D11).
    let remedy = |path: &str, id: &str, pending: bool| -> String {
        let repair = "run `mie store verify`, then `mie store repair`".to_owned();
        if restore.is_none() {
            return repair;
        }
        match inputs.roots.iter().find(|root| root.id == id) {
            None => repair,
            Some(root) if !root.copy_target => "the read-only root is never written (ADR-062 \
                 D11), so no mie command changes this copy"
                .to_owned(),
            Some(_) if !pending && state.copy(path, id).is_some_and(|c| c.latest.is_some()) => {
                repair
            }
            Some(_) => "`mie store verify` does not check this unrecorded copy; the restore \
                 moves it aside once it has copied the path back (H6 k), and the next pass \
                 copies the path from hot"
                .to_owned(),
        }
    };
    let mut records: Vec<Kind> = Vec::new();
    let mut cold_recorded: BTreeSet<String> = BTreeSet::new();
    let paths: BTreeSet<&str> = holders
        .keys()
        .copied()
        .chain(cold_only.iter().copied())
        .collect();
    for path in paths {
        if in_q(path) {
            continue;
        }
        let b6a = cold_only.contains(path) && state.verdict(path) != Some(PathVerdict::Lost);
        let texts = distinct(path);
        let text: Option<&str> = match texts[..] {
            [only] => Some(only),
            [] => None,
            _ if !is_archive_part(path) && recorded_text(path).is_some() => recorded_text(path),
            _ => listed_one(path, &texts).map(|i| texts[i]),
        };
        // B6a rules 1 and 1′: with no manifest text anywhere, the parseable
        // pending texts whose data verifies, of the roots without the
        // manifest, decide by B4's selection; they never enter B3's
        // conflict check.
        let text = if texts.is_empty() && b6a {
            let mut pending: Vec<&str> = pending_texts(inputs, path)
                .into_iter()
                .map(|(_, text)| text)
                .collect();
            pending.sort_unstable();
            pending.dedup();
            match pending[..] {
                [only] => Some(only),
                [] => None,
                _ if is_archive_part(path) => listed_one(path, &pending).map(|i| pending[i]),
                _ => None,
            }
        } else {
            text
        };
        // B5: copies that hold the path and do not verify.
        let mut damaged_cold = false;
        let mut damage = |root: &str, why: String, cold: bool, remedy: String| {
            damaged_cold |= cold;
            problems.push(format!(
                "{path} on {root}: {why}: the copy does not verify and gets no record; {remedy} \
                 (ADR-062 D11)"
            ));
        };
        let hot = inputs.hot.files.get(path).map(|copy| ("hot", copy, false));
        let cold = inputs.roots.iter().filter_map(|root| {
            root.files
                .get(path)
                .map(|copy| (root.id.as_str(), copy, true))
        });
        let mut verified: Vec<&str> = Vec::new();
        for (root, copy, is_cold) in hot.into_iter().chain(cold) {
            let fix = remedy(path, root, false);
            match copy {
                RootCopy::BadText(why) => damage(root, why.clone(), is_cold, fix),
                RootCopy::Text { text: own, data } if Some(own.as_str()) == text => match data {
                    DataCheck::Good => verified.push(root),
                    DataCheck::NotChecked => {}
                    DataCheck::Missing => {
                        damage(
                            root,
                            "missing: a manifest without its data".to_owned(),
                            true,
                            fix,
                        );
                    }
                    DataCheck::Size => damage(root, "size".to_owned(), true, fix),
                    DataCheck::Sha256 => damage(root, "sha256".to_owned(), true, fix),
                    DataCheck::Unreadable(error) => {
                        damage(
                            root,
                            format!("its data cannot be read ({error})"),
                            true,
                            fix,
                        );
                    }
                },
                // H5 b: another text than the start's on a live path. A
                // copy whose data matches its own text is another sealed
                // file under the path (D5), which no restore step moves
                // (#219 case 2).
                RootCopy::Text { data, .. }
                    if !is_archive_part(path)
                        && text.is_some()
                        && recorded_text(path).is_some() =>
                {
                    let fix = if *data == DataCheck::Good
                        && inputs.roots.iter().any(|r| r.id == root && r.copy_target)
                        && state.copy(path, root).is_none_or(|c| c.latest.is_none())
                    {
                        "it is another self-consistent file under the path (D5): the restore \
                         never moves it, and the path stays one copy short on this target (#219)"
                            .to_owned()
                    } else {
                        fix
                    };
                    damage(
                        root,
                        "its manifest differs from the copy ledger's manifest record".to_owned(),
                        is_cold,
                        fix,
                    );
                }
                RootCopy::Text { .. } => {}
            }
        }
        // B6a: a cold root's pending form of a path hot lacks with a
        // decided text: another text is a damaged copy (row 12; for every
        // such path, rule 6 of #195 amendment 2), and for a B6a path the
        // same text whose data does not verify on a root without the
        // manifest too (row 7).
        if !hot_holds(path)
            && let Some(text) = text
        {
            for root in &inputs.roots {
                let Some(RootCopy::Text { text: own, data }) = root
                    .temps
                    .get(path)
                    .and_then(|temps| temps.pending.as_ref())
                else {
                    continue;
                };
                let label = format!("{} (its pending manifest)", root.id);
                let why = if own != text {
                    "its text differs from the path's manifest text, a remnant of an earlier \
                     use of the path"
                        .to_owned()
                } else if !b6a || root.files.contains_key(path) {
                    continue;
                } else {
                    match data {
                        DataCheck::Good | DataCheck::NotChecked => continue,
                        DataCheck::Missing => {
                            "missing: neither its data file nor its data temp file".to_owned()
                        }
                        DataCheck::Size => "size".to_owned(),
                        DataCheck::Sha256 => "sha256".to_owned(),
                        DataCheck::Unreadable(error) => {
                            format!("its data cannot be read ({error})")
                        }
                    }
                };
                damage(&label, why, true, remedy(path, &root.id, true));
            }
        }
        let Some(text) = text else {
            continue;
        };
        if state.verdict(path) == Some(PathVerdict::Lost) {
            continue;
        }
        let targets: Vec<&str> = inputs
            .roots
            .iter()
            .filter(|root| root.copy_target && verified.contains(&root.id.as_str()))
            .map(|root| root.id.as_str())
            .collect();
        // B6a rule 2: a B6a path with a text gets its `manifest` record, on
        // a copy target or a read-only root, its data verified or not: hot
        // lacks it, so without the record the writer's numbering would not
        // count it (D5, D6) and the restore's copy-back would skip it.
        if !start_texts.contains_key(path) && (!targets.is_empty() || damaged_cold || b6a) {
            records.push(Kind::Manifest {
                text: text.to_owned(),
            });
            if b6a {
                cold_recorded.insert(path.to_owned());
            }
        }
        // Rule 6 (#195 amendment 2): every ordinary path hot lacks that no
        // cold root holds as a data file and that a verified crash copy
        // holds gets this line, whether this run or an earlier install
        // recorded it, so a later rebuild names what this one names.
        let ordinary = !findings.holds.contains_key(path)
            && state
                .verdict(path)
                .is_none_or(|verdict| verdict == PathVerdict::Ordinary);
        let cold_data = inputs.roots.iter().any(|root| {
            root.orphan_data.contains(path)
                || matches!(
                    root.files.get(path),
                    Some(RootCopy::Text { data, .. }) if *data != DataCheck::Missing
                )
        });
        let on: Vec<&str> = inputs
            .roots
            .iter()
            .filter(|root| verified_crash_copy(root, path, &[text]))
            .map(|root| root.id.as_str())
            .collect();
        if restore.is_none() && ordinary && !hot_holds(path) && !cold_data && !on.is_empty() {
            problems.push(format!(
                "{path}: its `manifest` record comes from the pending manifest on {} alone: no \
                 root holds it sealed and hot lacks it, so every selection that covers it fails \
                 until a sealed copy exists (ADR-062 D5, D7)",
                on.join(", ")
            ));
        }
        for id in targets {
            if state
                .copy(path, id)
                .is_none_or(|copy| copy.latest.is_none())
            {
                records.push(Kind::Copy {
                    path: path.to_owned(),
                    root: id.to_owned(),
                });
            }
        }
    }

    // B6a rule 3: a B6a path without a text is a lost candidate (H5 d in
    // the restore, `--record-lost` in the rebuild), named by every form
    // found. A path with a manifest text and no record (an archive
    // conflict without a listed text) stays held by its manifests, as
    // before B6a.
    for path in &cold_only {
        if in_q(path)
            || state.verdict(path) == Some(PathVerdict::Lost)
            || cold_recorded.contains(*path)
            || has_m_text(path)
        {
            continue;
        }
        sources
            .entry((*path).to_owned())
            .or_default()
            .extend(cold_forms(inputs, path));
    }

    // B6: lost. B6a rule 4′ (#195 amendment 2): a B6a path is held only
    // through its record (rule 2). Any other path is held when hot holds
    // it, a cold root holds its manifest (any) or its data file, or a root
    // holds a verified crash copy of the start's text. Nothing else holds
    // it: not a `D~` alone, an `M~` with other bytes, a torn or unreadable
    // `M~`, an `M~` whose data fails, or evidence. Why a lone `D~` never
    // holds: no mie path serves it (the copy-back needs a text form on the
    // root, and the tiered `read` reads only `D`).
    let held_anywhere = |path: &str| {
        if cold_only.contains(path) {
            return cold_recorded.contains(path) || has_m_text(path);
        }
        let texts: Vec<&str> = start_texts
            .get(path)
            .map(|texts| texts.iter().map(String::as_str).collect())
            .unwrap_or_default();
        hot_holds(path)
            || inputs.roots.iter().any(|root| {
                root.files.contains_key(path)
                    || root.orphan_data.contains(path)
                    || verified_crash_copy(root, path, &texts)
            })
    };
    let mut lost = Vec::new();
    for (path, named) in &sources {
        if in_q(path) {
            continue;
        }
        let mut described: Vec<String> = named.iter().map(Source::describe).collect();
        if !copy_ledger::is_data_path(path) {
            problems.push(format!(
                "{path:?}, named by {}, is no data path of the layout: reported, never recorded \
                 (ADR-062 D7)",
                described.join(", ")
            ));
            continue;
        }
        let held = held_anywhere(path);
        if state.verdict(path) == Some(PathVerdict::Lost) {
            // B6a rule 5: only hot or a parseable manifest on a cold root
            // holds it again; any other form is left in place and named.
            if hot_holds(path) || has_m_text(path) {
                problems.push(format!(
                    "{path}: the start copy records it lost, and a root holds it again; it stays \
                     lost, since recovering a lost file needs a new ADR (ADR-062 D11)"
                ));
            } else {
                let forms: Vec<String> = cold_forms(inputs, path)
                    .iter()
                    .filter(|form| !matches!(form, Source::Evidence(..)))
                    .map(Source::describe)
                    .collect();
                if !forms.is_empty() {
                    lines.push(format!(
                        "{path}: recorded lost; {} left in place (ADR-062 D11)",
                        forms.join(", ")
                    ));
                }
            }
            continue;
        }
        if held {
            continue;
        }
        // Rule 4′: a recorded path's candidate line names every form the
        // roots hold of it, all left in place.
        if !cold_only.contains(path.as_str()) && !hot_holds(path) {
            described.extend(cold_forms(inputs, path).iter().map(Source::describe));
        }
        let ledger_text = start_texts.get(path).and_then(|texts| texts.first());
        let import = named.iter().find_map(|source| match source {
            Source::Import(_, sha) => Some(sha.clone()),
            _ => None,
        });
        let has = |wanted: fn(&Source) -> bool| named.iter().any(wanted);
        let (origin, sha256) = if let Some(text) = ledger_text {
            (
                LostOrigin::Ledger,
                replica::parse_manifest(text).ok().map(|file| file.sha256),
            )
        } else if let Some(sha) = import {
            (LostOrigin::Import, Some(sha))
        } else if has(|s| matches!(s, Source::Journal(..))) {
            (LostOrigin::Journal, None)
        } else if has(|s| matches!(s, Source::Recovery(..))) {
            (LostOrigin::Recovery, None)
        } else if has(Source::is_cold_form) {
            // Only the roots' forms name it (B6a rule 3): a copy that does
            // not verify, as the repair's lost-file rule.
            (LostOrigin::Verify, None)
        } else {
            (LostOrigin::Ledger, None)
        };
        lines.push(format!(
            "lost candidate: {path}: origin {}, sha256 {}; named by {}",
            origin.as_str(),
            sha256.as_deref().unwrap_or("-"),
            described.join(", ")
        ));
        // The derivation states what the rebuilt ledger holds, never that
        // it was written: the install comes later and may not happen
        // (re-review 2 of #209, finding 2).
        let what = match (inputs.record_lost, restore.is_some()) {
            (true, false) => {
                "with --record-lost its `lost` record is derived into the rebuilt ledger, and it \
                 takes effect only if this run installs it (`wrote …` below): then every \
                 selection that covers it fails"
            }
            (false, false) => {
                "nothing is written; after checking for a shared cause (an unmounted root, \
                 roots restored from an old backup), `mie store rebuild --record-lost` records \
                 it lost"
            }
            (true, true) => {
                "with --record-lost its `lost` record is derived into the restored ledger, and \
                 it takes effect only once the restore installs it: then every selection that \
                 covers it fails"
            }
            (false, true) => {
                "nothing is written; after checking for a shared cause (an unmounted root, \
                 roots restored from an old backup), `mie store restore --record-lost` records \
                 it lost"
            }
        };
        let absent = if cold_only.contains(path.as_str()) {
            "no root holds a sealed copy or a pending manifest that decides its text, hot lacks \
             it, and the path must stay counted, or the next capture reuses it (ADR-062 D5, D6);"
        } else {
            "no root holds it (no data file and no manifest on hot or on any cold root), and"
        };
        problems.push(format!(
            "{path}: {absent} {} names it; {what} (ADR-062 D7 step 4, D11)",
            described.join(", ")
        ));
        lost.push(LostFile {
            path: path.clone(),
            origin,
            sha256,
            sources: described,
            recorded: inputs.record_lost,
        });
    }

    // B7: holds.
    // An `unlisted` hold comes from an absence (no current entry lists the
    // part), so a new one is written only with `--record-unlisted`, as a
    // `lost` record only with `--record-lost`: an empty or unmounted import
    // ledger reads as no entries (re-review of #209). The conflict reasons
    // come from positive evidence and are always written.
    let mut holds = Vec::new();
    let mut hold_records = Vec::new();
    let mut unrecorded_unlisted = Vec::new();
    let mut new_unlisted = 0usize;
    // The restore writes them without a flag (H5 c): it must complete
    // before `mie ingest` may start, and D11's replication order limits them
    // to the one archive file whose parts and entry a loss separated.
    let record_unlisted = inputs.record_unlisted || restore.is_some();
    for (path, reason) in &findings.holds {
        holds.push((path.clone(), *reason));
        let new = state.hold_reason(path).is_none();
        if new && *reason == HoldReason::Unlisted {
            new_unlisted += 1;
        }
        if new && *reason == HoldReason::Unlisted && !record_unlisted {
            lines.push(format!("unlisted: {path} (not recorded)"));
            problems.push(format!(
                "{path}: no current import entry lists it, so it would be held (unlisted); \
                 nothing is written without --record-unlisted (ADR-062 D7 step 5a, D11)"
            ));
            unrecorded_unlisted.push(path.clone());
            continue;
        }
        lines.push(format!("hold derived: {path} ({})", reason.as_str()));
        problems.push(format!(
            "{path}: held ({}) in the rebuilt ledger: once it is installed, the importer refuses \
             and every selection that covers it fails until the hold is resolved (`mie store \
             resolve`; ADR-062 D11)",
            reason.as_str()
        ));
        if new {
            hold_records.push(Kind::Hold {
                path: path.clone(),
                reason: *reason,
            });
        }
    }
    // The evidence line on every run that derives a new `unlisted` hold,
    // the flagged run included: an import-ledger disk that became an empty
    // mount point between two runs must show in the run that writes the
    // holds (re-review 2 of #209, finding 1).
    if new_unlisted > 0 && restore.is_none() {
        let what = if inputs.record_unlisted {
            "With --record-unlisted their holds are derived into the rebuilt ledger, and only \
             the hold resolution clears them once it is installed; if these counts show an empty \
             or unmounted import ledger, fix it before a run that installs"
        } else {
            "Check it; once it is the store's, `mie store rebuild --record-unlisted` records \
             these holds, which only the hold resolution clears"
        };
        let on_hot = inputs
            .entries
            .values()
            .filter(|copies| copies.iter().any(|copy| copy.root == "hot"))
            .count();
        let targets_only = inputs.entries.len() - on_hot;
        problems.push(format!(
            "{new_unlisted} archive part(s) are listed by no current import entry. Hot's import \
             ledger {} holds {on_hot} entries, and the copy targets hold {targets_only} entries \
             that hot lacks: an empty or unmounted import ledger reads as no entries. {what} \
             (ADR-062 D7 step 5a, D11)",
            inputs
                .import_ledger
                .as_deref()
                .unwrap_or("(none configured)")
        ));
    }
    // B7′ (#210): a start hold the sources no longer derive is cleared
    // only on positive evidence in the import ledger, which is immutable
    // and replicated: the path's archive file has a current entry listing
    // it with the SHA-256 of its one manifest text, and the hold is
    // `unlisted`, or `entry-conflict` with that entry written by a resolve
    // (generation ≥ 1). An absence (an emptied or unmounted import-ledger
    // directory) never clears one, and a `manifest-conflict` never is.
    let mut start_holds = BTreeSet::new();
    for record in &start_ledger.records {
        if let Kind::Hold { path, .. } = &record.kind {
            start_holds.insert(path.clone());
        }
    }
    let mut cleared_records = Vec::new();
    for path in start_holds {
        let Some(reason) = state.hold_reason(&path) else {
            continue;
        };
        if in_q(&path) {
            continue;
        }
        if findings.holds.contains_key(&path) {
            continue;
        }
        let evidence = match distinct(&path)[..] {
            [only] => replica::parse_manifest(only).ok().and_then(|file| {
                findings
                    .current
                    .iter()
                    .filter(|(_, files)| files.contains(&(path.clone(), file.sha256.clone())))
                    .filter_map(|(name, _)| {
                        let (_, file_name) = name.split_once('/')?;
                        LedgerName::parse(file_name).ok()
                    })
                    .map(|name| name.generation)
                    .max()
            }),
            _ => None,
        };
        let clears = match (reason, evidence) {
            (HoldReason::Unlisted, Some(_)) => true,
            (HoldReason::EntryConflict, Some(generation)) => generation >= 1,
            _ => false,
        };
        if clears {
            lines.push(format!("hold-cleared {path} readmit"));
            cleared_records.push(Kind::HoldCleared {
                path: path.clone(),
                resolution: Resolution::Readmit,
            });
            continue;
        }
        problems.push(format!(
            "{path}: the start copy holds it ({}) and the sources no longer derive that hold; it \
             stays held in the rebuilt ledger, since only positive evidence in the import ledger \
             clears a hold (a current entry listing it, written by `mie store resolve` for an \
             entry conflict); resolve it with `mie store resolve` (ADR-062 D7 step 5a, D11)",
            reason.as_str()
        ));
    }
    for path in &findings.exempt {
        lines.push(format!(
            "{path}: not held: the import PENDING names resumes it (ADR-034 D4)"
        ));
    }
    problems.extend(findings.problems.iter().cloned());
    lines.extend(quarantine.lines.iter().cloned());
    problems.extend(quarantine.problems.iter().cloned());

    // B8: removed. In the restore a copy the start records is not read
    // (H2, decision 4) and counts as verified, and every partition the
    // copy-back brings back counts as on hot (H5 e).
    let partitions: BTreeSet<&str> = inputs
        .roots
        .iter()
        .flat_map(|root| &root.files)
        .filter(|(path, copy)| {
            !in_q(path)
                && match copy {
                    RootCopy::Text {
                        data: DataCheck::Good,
                        ..
                    } => true,
                    RootCopy::Text {
                        data: DataCheck::NotChecked,
                        ..
                    } => restore.is_some(),
                    _ => false,
                }
        })
        .map(|(path, _)| partition_of(path))
        .collect();
    let mut removed = Vec::new();
    for partition in partitions {
        if inputs.hot.partitions.contains(partition)
            || state.is_removed(partition)
            || restore.is_some_and(|restore| restore.copy_back.contains(partition))
        {
            continue;
        }
        if date_of(partition) >= inputs.first_hot_date.as_str() {
            problems.push(format!(
                "{partition}: verified cold copies and no directory on hot, but it is dated inside \
                 the hot window, which starts on {}: the hot removal never removes such a \
                 partition, so this is hot damage. Its `removed` record is derived, so once the \
                 ledger is installed a writer recalls it before it appends; restart a running `mie ingest` so its writer numbers parts \
                 from the union (ADR-062 D6, D8)",
                inputs.first_hot_date
            ));
        }
        removed.push(Kind::Removed {
            partition: partition.to_owned(),
        });
    }

    // B9 step 1: the record order.
    if inputs.record_lost {
        records.extend(lost.iter().map(|file| Kind::Lost {
            path: file.path.clone(),
            origin: file.origin,
            sha256: file.sha256.clone(),
        }));
    }
    records.extend(removed);
    records.extend(hold_records);
    records.extend(cleared_records);
    records.extend(quarantine.records.iter().cloned());
    let mut added = Vec::new();
    for kind in records {
        let record = Record {
            at_ms: inputs.at_ms,
            kind,
        };
        builder
            .push(&record)
            .map_err(|error| Stop(format!("a derived record is invalid: {error}")))?;
        added.push(record);
    }
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for record in &added {
        *counts.entry(record.kind.name()).or_default() += 1;
    }
    let counted: Vec<String> = [
        "manifest",
        "copy",
        "lost",
        "removed",
        "hold",
        "hold-cleared",
        "quarantined",
        "replaced",
    ]
    .iter()
    .map(|kind| format!("{kind} {}", counts.get(kind).copied().unwrap_or(0)))
    .collect();
    // After the start, the rejected copies and the journal line.
    lines.insert(
        2 + rejected.len(),
        format!("records derived: {}", counted.join(", ")),
    );
    Ok(Derived {
        start,
        rejected,
        ledger: builder.finish(),
        added,
        lost,
        holds,
        unrecorded_unlisted,
        lines,
        problems,
        chosen_entries: findings.chosen,
    })
}

// ------------------------------------------------------------- the run

/// A cold root that presented its marker.
pub(super) struct Marked {
    pub(super) path: PathBuf,
    pub(super) id: String,
    pub(super) copy_target: bool,
}

/// Runs one rebuild (module docs). `now_ms` is read once: every record and
/// every `damaged/<at_ms>/` name carries it. Every outcome is in the
/// report.
pub fn run(
    config: &IngestConfig,
    archive: Option<&Path>,
    flags: Flags,
    first_hot_date: &str,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
) -> RebuildReport {
    let mut report = RebuildReport::default();
    match rebuild(
        config,
        archive,
        flags,
        first_hot_date,
        probe,
        now_ms,
        &mut report,
    ) {
        Ok(()) => {}
        Err(Stop(why)) => {
            report.problems.push(why);
            report.lines.push("nothing written".to_owned());
        }
    }
    report
}

/// The rebuild; a [`Stop`] means nothing was written.
fn rebuild(
    config: &IngestConfig,
    archive: Option<&Path>,
    flags: Flags,
    first_hot_date: &str,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    report: &mut RebuildReport,
) -> Result<(), Stop> {
    let hot = config.paths.raw_root.as_path();
    let cold_roots = &config.paths.cold_roots;
    // B0.
    if cold_roots.is_empty() {
        return Err(Stop(
            "paths.cold_roots is empty: a single-root store has no copy ledger to rebuild \
             (ADR-062 D7)"
                .to_owned(),
        ));
    }
    check_hot(hot, cold_roots).map_err(Stop)?;
    let archive = replicate::archive_config(config, archive).map_err(Stop)?;
    let mut marked: Vec<Marked> = Vec::new();
    for cold in cold_roots {
        let kind = if cold.copy_target {
            "copy target"
        } else {
            "read-only root"
        };
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
                     since the marker is what tells an unmounted root from an empty one; the \
                     rebuild stops (ADR-062 D7 step 2)",
                    cold.path.display()
                )));
            }
            Err(error) => {
                return Err(Stop(format!(
                    "{kind} {}: its root marker cannot be read ({error}); the rebuild stops \
                     (ADR-062 D7 step 2)",
                    cold.path.display()
                )));
            }
        }
    }
    let targets: Vec<&Marked> = marked.iter().filter(|root| root.copy_target).collect();
    let target_paths: Vec<&Path> = targets.iter().map(|root| root.path.as_path()).collect();
    check_devices(hot, &target_paths, probe).map_err(Stop)?;
    let locked = |what: &str, error: RawStoreError| {
        Stop(format!(
            "{what}: {error}; the rebuild takes it without waiting and holds it for its whole run \
             (ADR-062 D7)"
        ))
    };
    let hold = CopyLedger::at(hot)
        .try_hold()
        .map_err(|e| locked("the copy-ledger lock", e))?;
    let _pass = replica::try_replication_lock(hot).map_err(|e| locked("the pass lock", e))?;
    let _verify = replica::try_verify_lock(hot).map_err(|e| locked("the verify lock", e))?;
    let _archive_lock = removal::try_source_lock(hot, ARCHIVE_SOURCE)
        .map_err(|e| locked("the archive source's writer lock", e))?;
    let ledger_path = copy_ledger::ledger_path(hot);
    let hot_ledger = match copy_ledger::read(hot) {
        Ok(None) => false,
        Ok(Some(_)) if flags.replace_hot_ledger => true,
        Ok(Some(_)) => {
            return Err(Stop(format!(
                "hot's copy ledger {} is readable: the rebuild replaces it only with \
                 --replace-hot-ledger, which links the old one into .mie/damaged/ (ADR-062 D7)",
                ledger_path.display()
            )));
        }
        Err(RawStoreError::Integrity(error) | RawStoreError::Corrupt(error)) => {
            report.lines.push(format!(
                "hot's copy ledger is unreadable ({error}); the install links it aside"
            ));
            true
        }
        Err(error) => {
            return Err(Stop(format!(
                "hot's copy ledger {}: {error}",
                ledger_path.display()
            )));
        }
    };

    // B1.
    let at_ms = now_ms();
    let io = |what: String| {
        move |error: std::io::Error| {
            Stop(format!(
                "{what}: {error}; a skipped read would turn its files into lost candidates, so \
                 the rebuild stops with nothing written (ADR-062 D7)"
            ))
        }
    };
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
            error: None,
        });
    }
    let journal = read_journal(&config.paths.journal).map_err(io(format!(
        "read the journal {}",
        config.paths.journal.display()
    )))?;
    #[cfg(test)]
    hook(&AFTER_JOURNAL);
    let mut entries: BTreeMap<String, Vec<EntryCopy>> = BTreeMap::new();
    let mut pending = None;
    let mut missing_import_ledger = None;
    if let Some(archive) = &archive {
        let ledger_dir = &archive.paths.import_ledger;
        // Links are followed: a symlink to an unmounted disk is missing
        // (re-review of #209).
        match fs::metadata(ledger_dir) {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                missing_import_ledger = Some(ledger_dir.display().to_string());
            }
            Err(error) => {
                return Err(io(format!(
                    "inspect the import ledger {}",
                    ledger_dir.display()
                ))(error));
            }
        }
        let dirs = std::iter::once(("hot".to_owned(), ledger_dir.clone())).chain(
            targets.iter().map(|target| {
                (
                    target.id.clone(),
                    target.path.join(META_DIR).join(IMPORT_LEDGER_DIR),
                )
            }),
        );
        for (root, dir) in dirs {
            let (files, errors) = entry_files(&dir);
            if let Some(error) = errors.into_iter().next() {
                return Err(Stop(format!(
                    "the import ledger on {root} cannot be listed ({error}); the rebuild stops \
                     with nothing written (ADR-062 D7)"
                )));
            }
            for file in files {
                let bytes = fs::read(&file.path)
                    .map_err(io(format!("read import entry {}", file.path.display())))?;
                entries.entry(file.name).or_default().push(EntryCopy {
                    root: root.clone(),
                    bytes,
                });
            }
        }
        let path = ledger_dir.join(PENDING_FILE);
        pending = match fs::read(&path) {
            Ok(bytes) => Some(
                std::str::from_utf8(&bytes)
                    .map_err(|_| "not UTF-8".to_owned())
                    .and_then(|text| Pending::parse(text).map_err(|e| e.to_string())),
            ),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => Some(Err(format!("{}: {error}", path.display()))),
        };
    }
    let listing = |root: &Path, error: RawStoreError| {
        Stop(format!(
            "cannot list the layout of {} ({error}); a skipped directory would turn its files into \
             lost candidates, so the rebuild stops with nothing written (ADR-062 D7)",
            root.display()
        ))
    };
    let quarantine = |root: &Path, error: RawStoreError| {
        Stop(format!(
            "cannot read the quarantine of {} ({error}); a skipped quarantine would turn its \
             files into lost candidates, so the rebuild stops with nothing written (ADR-062 D7 \
             step 5a)",
            root.display()
        ))
    };
    // A quarantined manifest that cannot be read stops the rebuild, as
    // before #195 (amendment 2, U: only the restore counts it).
    let read_quarantine = |root: &Path| -> Result<QuarantineListing, Stop> {
        let listing = replica::list_quarantine(root).map_err(|e| quarantine(root, e))?;
        let unreadable = listing
            .ids
            .values()
            .flat_map(|parts| parts.values())
            .find_map(|copy| copy.unreadable.clone());
        match unreadable {
            Some(error) => Err(quarantine(root, RawStoreError::Io(error))),
            None => Ok(listing),
        }
    };
    let mut hot_read = HotRead {
        quarantine: read_quarantine(hot)?,
        ..HotRead::default()
    };
    for path in replica::list_sealed(hot).map_err(|e| listing(hot, e))? {
        let copy = read_copy(hot, &path, false, false)?;
        hot_read.files.insert(path, copy);
    }
    hot_read.orphan_data = orphan_data(hot).map_err(|e| listing(hot, e))?;
    hot_read.evidence = replica::list_evidence(hot).map_err(|e| listing(hot, e))?;
    hot_read.partitions = removal::scan_hot(hot, "")
        .map_err(|e| listing(hot, e))?
        .partitions
        .into_keys()
        .collect();
    let mut roots = Vec::new();
    for root in &marked {
        let mut files = BTreeMap::new();
        for path in replica::list_sealed(&root.path).map_err(|e| listing(&root.path, e))? {
            let copy = read_copy(&root.path, &path, true, false)?;
            files.insert(path, copy);
        }
        let temps = read_temps(&root.path, &files, &|_| true)?;
        roots.push(RootRead {
            id: root.id.clone(),
            copy_target: root.copy_target,
            files,
            orphan_data: orphan_data(&root.path).map_err(|e| listing(&root.path, e))?,
            temps,
            evidence: replica::list_evidence(&root.path).map_err(|e| listing(&root.path, e))?,
            quarantine: read_quarantine(&root.path)?,
        });
    }
    let inputs = RebuildInputs {
        mode: Mode::Rebuild,
        at_ms,
        first_hot_date: first_hot_date.to_owned(),
        record_lost: flags.record_lost,
        record_unlisted: flags.record_unlisted,
        import_ledger: archive
            .as_ref()
            .map(|archive| archive.paths.import_ledger.display().to_string()),
        archive_configured: archive.is_some(),
        missing_import_ledger,
        ledger_copies,
        journal,
        entries,
        pending,
        hot: hot_read,
        roots,
    };

    // B2 to B8.
    let derived = derive(&inputs)?;
    report.lines.extend(derived.lines.iter().cloned());
    report.problems.extend(derived.problems.iter().cloned());
    if derived.lost.iter().any(|file| !file.recorded) || !derived.unrecorded_unlisted.is_empty() {
        report.lines.push("nothing written".to_owned());
        return Ok(());
    }

    // B9.
    #[cfg(test)]
    hook(&BEFORE_INSTALL);
    let hot_aside = replica::damaged_dir(hot, at_ms).join("copy-ledger");
    let asides: Vec<PathBuf> = targets
        .iter()
        .map(|target| replica::damaged_dir(&target.path, at_ms).join("copy-ledger"))
        .collect();
    for aside in std::iter::once(&hot_aside)
        .filter(|_| hot_ledger)
        .chain(&asides)
    {
        if fs::symlink_metadata(aside).is_ok() {
            return Err(Stop(format!(
                "{} exists: evidence is never overwritten; the rebuild stops with nothing written \
                 (ADR-062 D7)",
                aside.display()
            )));
        }
    }
    for root in &marked {
        if let Some(why) = marker_gone(root) {
            return Err(Stop(format!(
                "{} ({}): {why} since the run started; a mount lost during the listing reads as \
                 absent files, so the rebuild stops with nothing written (ADR-062 D7 step 2)",
                root.path.display(),
                root.id
            )));
        }
    }
    hold.replace(&derived.ledger, Some(&hot_aside))
        .map_err(|error| Stop(format!("the install failed: {error}")))?;
    let len = derived.ledger.len() as u64;
    if hot_ledger {
        report.lines.push(format!(
            "hot's former copy ledger is linked to {}",
            hot_aside.display()
        ));
    }
    report.lines.push(format!(
        "gate: {} holds {len}: the hot removal waits for a `mie store verify` of every copy \
         target that covers the rebuilt ledger (ADR-062 D7 step 6)",
        mie_adapter_parquet::rebuilt::gate_path(hot).display()
    ));
    for (target, aside) in targets.iter().zip(&asides) {
        move_copy_aside(hot, target, aside, report);
    }
    report.lines.push(format!(
        "wrote {} ({len} bytes, {} records added)",
        ledger_path.display(),
        derived.added.len()
    ));
    Ok(())
}

/// Why the marker of `root` no longer names it, if it does not.
pub(super) fn marker_gone(root: &Marked) -> Option<String> {
    match root_marker::read(&root.path) {
        Ok(Some(id)) if id == root.id => None,
        Ok(Some(other)) => Some(format!("its marker now carries the id {other:?}")),
        Ok(None) => Some("its marker is gone (an unmounted mount point?)".to_owned()),
        Err(error) => Some(format!("its marker cannot be read ({error})")),
    }
}

/// B9 step 6 for one copy target: a ledger copy that is no prefix of the
/// installed ledger is moved aside; the next pass copies the rebuilt one
/// from byte 0.
pub(super) fn move_copy_aside(
    hot: &Path,
    target: &Marked,
    aside: &Path,
    report: &mut RebuildReport,
) {
    let name = format!("{} ({})", target.path.display(), target.id);
    let check = match copy_ledger::check_copy(hot, &target.path) {
        Ok(LedgerCopyCheck::Good | LedgerCopyCheck::Absent) => return,
        Ok(check) => check,
        Err(error) => {
            report.problems.push(format!(
                "the ledger copy on {name}: cannot be checked: {error}"
            ));
            return;
        }
    };
    if let Some(why) = marker_gone(target) {
        report.problems.push(format!(
            "the ledger copy on {name} ({check:?}) is not moved aside: {why}"
        ));
        return;
    }
    match replica::move_file_aside(&copy_ledger::ledger_path(&target.path), aside) {
        Ok(_) => report.lines.push(format!(
            "the ledger copy on {name} ({check:?}) is no prefix of the rebuilt ledger: moved \
             aside to {}; the next pass copies the rebuilt ledger from byte 0",
            aside.display()
        )),
        Err(error) => report.problems.push(format!(
            "the ledger copy on {name} ({check:?}): moving it aside failed: {error}"
        )),
    }
}

/// Reads hot's journal up to the length read now (B1); no journal is an
/// empty one.
pub(super) fn read_journal(path: &Path) -> std::io::Result<JournalRead> {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(JournalRead::default()),
        Err(error) => return Err(error),
    };
    let len = file.metadata()?.len();
    let mut bytes = Vec::with_capacity(len as usize);
    file.by_ref().take(len).read_to_end(&mut bytes)?;
    Ok(parse_journal(&bytes))
}

/// The copy of the sealed part `path` on `root` (B1): its manifest
/// classified and, with `check`, its data against that root's own text.
/// A manifest that cannot be read stops the run, or with
/// `unreadable_is_damage` (the hot restore's cold roots, #195 amendment
/// 2, U) is a damaged copy: the name stays counted.
pub(super) fn read_copy(
    root: &Path,
    path: &str,
    check: bool,
    unreadable_is_damage: bool,
) -> Result<RootCopy, Stop> {
    let manifest = format!("{}.manifest", path.strip_suffix(".parquet").unwrap_or(path));
    let file = root.join(&manifest);
    let bytes = match fs::read(&file) {
        Ok(bytes) => bytes,
        Err(error) if unreadable_is_damage => {
            return Ok(RootCopy::BadText(format!(
                "the manifest cannot be read: {error}"
            )));
        }
        Err(error) => {
            return Err(Stop(format!(
                "read {}: {error}; the rebuild stops with nothing written (ADR-062 D7)",
                file.display()
            )));
        }
    };
    let copy = classify_text(path, &bytes);
    let RootCopy::Text { text, .. } = &copy else {
        return Ok(copy);
    };
    if !check {
        return Ok(copy);
    }
    let data = match replica::check_sealed_copy(root, path, text) {
        Ok(CopyCheck::Good) => DataCheck::Good,
        Ok(CopyCheck::Missing) => DataCheck::Missing,
        Ok(CopyCheck::Size) => DataCheck::Size,
        Ok(CopyCheck::Sha256) => DataCheck::Sha256,
        Ok(CopyCheck::Manifest { .. }) => {
            DataCheck::Unreadable("its manifest changed while it was read".to_owned())
        }
        Err(error) => DataCheck::Unreadable(error.to_string()),
    };
    Ok(RootCopy::Text {
        text: text.clone(),
        data,
    })
}

/// The seal temp names of the layout of `root` (B6a), each pending
/// manifest classified. Where `root` lacks the part's manifest and
/// `check(path)` holds, a pending text's data (`D`, else `D~`) is checked
/// against it.
pub(super) fn read_temps(
    root: &Path,
    files: &BTreeMap<String, RootCopy>,
    check: &dyn Fn(&str) -> bool,
) -> Result<BTreeMap<String, TempForms>, Stop> {
    let listed = replica::list_temps(root).map_err(|error| {
        Stop(format!(
            "cannot list the seal temp names of {} ({error}); a skipped directory would turn \
             its files into lost candidates, so nothing is written (ADR-062 D7)",
            root.display()
        ))
    })?;
    let mut temps = BTreeMap::new();
    for (path, found) in listed {
        // U (#195 amendment 2): a pending manifest that cannot be read
        // counts as one that does not parse (row 10), never a stop.
        let unreadable = found.unreadable.map(|error| {
            RootCopy::BadText(format!("the pending manifest cannot be read: {error}"))
        });
        let pending = found.manifest.map(|text| {
            let copy = match text {
                None => RootCopy::BadText("the pending manifest is not UTF-8".to_owned()),
                Some(text) => classify_text(&path, text.as_bytes()),
            };
            match copy {
                RootCopy::Text { text, .. } if !files.contains_key(&path) && check(&path) => {
                    let data = pending_data(root, &path, &text);
                    RootCopy::Text { text, data }
                }
                other => other,
            }
        });
        temps.insert(
            path,
            TempForms {
                data_tmp: found.data,
                pending: pending.or(unreadable),
            },
        );
    }
    Ok(temps)
}

/// The data of a pending manifest's text on `root`: the data file, else
/// the data temp file, against the text (B6a).
fn pending_data(root: &Path, path: &str, text: &str) -> DataCheck {
    let checked = replica::check_file(&root.join(path), text).and_then(|check| {
        if check == CopyCheck::Missing {
            replica::temp_path(root, path, replica::TempName::Data)
                .and_then(|tmp| replica::check_file(&tmp, text))
        } else {
            Ok(check)
        }
    });
    match checked {
        Ok(CopyCheck::Good) => DataCheck::Good,
        Ok(CopyCheck::Missing) => DataCheck::Missing,
        Ok(CopyCheck::Size) => DataCheck::Size,
        Ok(CopyCheck::Sha256) => DataCheck::Sha256,
        Ok(CopyCheck::Manifest { .. }) => DataCheck::Unreadable("its manifest changed".to_owned()),
        Err(error) => DataCheck::Unreadable(error.to_string()),
    }
}

/// The data files without their manifest in the layout of `root`.
pub(super) fn orphan_data(root: &Path) -> Result<BTreeSet<String>, RawStoreError> {
    Ok(replica::list_unsealed(root)?
        .into_iter()
        .filter(|path| {
            path.rsplit_once('/')
                .is_some_and(|(_, name)| !name.starts_with('.') && name.ends_with(".parquet"))
        })
        .collect())
}
