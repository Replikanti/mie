//! The hold resolution (ADR-062 D11, proposed; #210 re-admit, #211
//! replace): `mie store resolve`.
//!
//! A held archive part fails every selection that covers it and halts the
//! importer. This command re-admits the held parts of one archive file
//! when they equal its published rows: it writes the missing entry, or a
//! supersede record and a superseding entry when a conflicting one exists,
//! and clears the holds. Bytes, paths and dataset versions stay as they
//! were. With `--replace` it instead moves the archive file's superseded
//! generation into quarantine ([`replace`], RQ0 to RQ8 below). Built like the
//! other `mie store` verbs: refusals and one lock, then reads, then a pure
//! [`plan`], then the row check, then writes in a fixed order; a re-run
//! completes an interrupted run. The rules, as numbered in the plan of
//! #210 (the tests cite them):
//!
//! - **RA0 Usage.** `--config <ingest config> [--archive-config <path>]
//!   [--file <archive path>]`; the archive configuration is
//!   `--archive-config`, else `[history] archive_config`, else refused.
//!   Without `--file` the command only proposes (RA3) and writes nothing.
//! - **RA1 Refusals**, each with nothing written: an empty `cold_roots`, a
//!   hot without its marker, an archive configuration of another store or
//!   none, a missing or unreadable copy ledger (the rebuild first), a copy
//!   target without its marker or with an unreadable one, one id on two
//!   roots, two roots on one device; then **one lock**, the archive
//!   source's writer lock, taken without waiting (created when absent, as
//!   the writer creates it) and held for the whole run. It excludes the
//!   importer, the rebuild and a second resolve. The copy-ledger lock is
//!   taken per append only: `mie ingest`'s remover appends from the seal
//!   path, and a whole-run hold would stall the capture for a download.
//! - **RA2 Reads**: hot's copy ledger folded (the open holds and their
//!   ledger texts), hot's manifest of each held path, and with `--file` the
//!   archive file's import-ledger files on hot and on every copy target
//!   (one listing per directory; an I/O error other than "not found"
//!   stops), and hot's `PENDING`.
//! - **RA3 Proposals.** `--file` must be exactly one catalog archive path
//!   of the configured symbol. A held part is proposed the file of its
//!   stream and its ordering times by the catalog's period rules
//!   ([`ArchiveStream::period_of`]); a part whose first and last times map
//!   to two files gets no proposal. Without `--file`: one line per open
//!   hold, then `PASS` with none open, or `FAIL` naming the count.
//! - **RA4 Attribution.** parts(F): the open holds proposing F, in path
//!   order. Refused: none; a `manifest-conflict` hold (re-admit only; RQ2); a part with
//!   no manifest; hot's text differing from the ledger's (D5); a `PENDING`
//!   naming F (its resume owns those parts).
//! - **RA5 Plan** ([`plan`], pure), over F's files on hot and the targets:
//!   (a) none: write `F.import`; (b) a supersede generation above every
//!   entry: an interrupted re-admit, write that generation's entry (a
//!   pending replace, `quarantine <id>`, resumes, RQ7); (c) the latest entry,
//!   one variant, lists every held part with its manifest's rows and
//!   SHA-256 and every other path it lists is ordinary: nothing to write
//!   but the clearing; (d) otherwise supersede: a record of generation n
//!   (1 + the highest generation; never wrapping past [`u32::MAX`]) naming
//!   every variant of the latest entry, then the entry of generation n.
//!   A part of parts(F) held as `entry-conflict` takes (d) instead of (a)
//!   and instead of (c) at generation 0 (a record naming no entry when F
//!   has none): the rebuild clears such a hold only on an entry of
//!   generation ≥ 1 (B7′), so a stale start agrees with a full and an
//!   empty one (review of #212, finding 1). No orphan is left behind: a
//!   path a superseded variant lists that is not held for F must be on no
//!   root and unrecorded. Read and bound as G1–G3 must be every supersede
//!   record, every file of F on hot (the importer and the replication
//!   read them all and fail F closed; re-review of #212, finding 3) and
//!   every copy of the entry the case decides on: the latest entry in (c)
//!   and (d), and in (b) the copies below m whose bytes the record names.
//!   Any other entry copy on a target is superseded, decides nothing, and
//!   no verification or repair reaches it (C4, C5); one that does not read
//!   is named, as is a malformed name on a copy target only, which no
//!   reader counts (review of #212, finding 2). A malformed name on hot,
//!   a name on the targets only and differing record copies are refused.
//! - **RA6 Row check** ([`readmit::check`]).
//! - **RA7 Writes**, in this order: the record, the entry, then one
//!   `hold-cleared <path> readmit` per part with one `at_ms`. The fold takes
//!   `hold-cleared readmit` as proof and lists the part at once, so the
//!   entry that lists it must exist first. After the record only, a re-run
//!   takes case (b); after the entry, case (c).
//! - **RA8 Output**: F's files per root, the file and its parts, the
//!   published SHA-256 and row count, `rows match`, `wrote …`, one
//!   `hold-cleared` line per record and every truncation; then `PASS`, or
//!   `FAIL:` with nothing written for a difference, which also prints the
//!   cost of a replace (RQ5) and names `--replace`.
//!
//! The replace (#211), numbered as in its plan:
//!
//! - **RQ0 Usage.** `--replace` needs `--file` (a usage error before
//!   anything is read) and makes no network request. A replace happens
//!   only with the flag (operator, #211 STOP 1 decision 1).
//! - **RQ1 Refusals**, nothing written: everything RA1 refuses; a
//!   configured cold root, copy target or read-only, without a readable
//!   marker; a `.quarantine` that is no directory (a symlink included) on
//!   any root; the pass lock held (taken after the writer lock, without
//!   waiting, for the whole run: a pass that planned a copy of a moved part
//!   could put it back into a layout after its rename).
//! - **RQ2 Moved set M**: parts(F) (RA4, a `manifest-conflict` part and
//!   hot's text differing from the ledger's allowed) plus every path a
//!   variant of F's latest entry lists that some root's layout holds, the
//!   whole superseded generation (operator, #211 STOP 1 decision 2: a
//!   sibling left in the layout would be an orphan that stops the
//!   re-import). Refused: a part or a recorded sibling no root holds, a
//!   sibling that is not ordinary or unrecorded, two ledger texts for a
//!   moved path, a read-only root holding one, a `PENDING` naming F. A
//!   listed path no root holds and the ledger does not record is named.
//! - **RQ3 Quarantine id** `q-<at_ms>`, the run's one clock read; refused
//!   when any supersede record on hot or a target names it, or any root
//!   has `.quarantine/<id>`.
//! - **RQ4 S_n**: n = 1 + F's highest generation, never wrapping; one
//!   `entry` line per variant of the latest entry; `quarantine <id>` and M
//!   as `moved` lines.
//! - **RQ5 Cost**, printed before the first write: one line per moved
//!   path and distinct manifest (stream, date, event range, rows,
//!   SHA-256), one per journaled warm run whose prefix `[origin, cutover)`
//!   overlaps a moved part (run id, `prefix_dataset`), a count line, `no
//!   journal` without one; an unreadable journal is refused (operator,
//!   #211 STOP 1 decision 3).
//! - **RQ6 Writes**: W1 S_n; W2 per moved path `manifest` when the ledger
//!   has none (the text rule) then `quarantined`; W3 the renames, cold
//!   roots in configuration order, hot last, each root's marker re-read
//!   first; W4 `hold-cleared <path> replace` per open hold. Every
//!   intermediate state fails a covering selection instead of shortening
//!   it, and the importer stays halted until every root's renames (I1).
//! - **RQ7 Resume.** With `--file F` whose highest supersede generation is
//!   above every entry and carries a quarantine, or with a supersede
//!   record of F whose moved paths some root's layout still holds (review
//!   of #215, finding 1): no new record, M from
//!   S_m, W2 for what the ledger lacks (a path pending under another id is
//!   refused), W3 idempotent (a name in both places is `Integrity`, never
//!   overwritten), W4; `--replace` is not needed.
//! - **RQ8 Proposals**: without `--file`, one line per supersede record
//!   on hot with a quarantine; an incomplete replace fails, naming `--file
//!   F` ([`quarantine::replace_lines`]).

use super::replicate::{self, IMPORT_LEDGER_DIR, META_DIR};
use super::{
    DeviceProbe, check_copy_ledger, check_devices, check_hot, quarantine, truncation_line,
};
use crate::archive::{ArchiveTransports, fetch_policy};
use crate::config::IngestConfig;
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_binance::archive::catalog::{ArchiveStream, Period, parse_archive_path};
use mie_adapter_binance::archive::fetch::Fetcher;
use mie_adapter_binance::archive::ledger::{
    ImportLedger, LedgerDir, LedgerKind, LedgerName, SupersedeRecord, SupersededEntry,
};
use mie_adapter_binance::archive::readmit::{self, ReadmitRequest};
use mie_adapter_parquet::copy_ledger::{self, CopyLedger, HoldReason, Kind, Record, Resolution};
use mie_adapter_parquet::removal;
use mie_adapter_parquet::replica;
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::verification::{CopyState, PathVerdict};
use mie_ports::raw::{RawStreamKey, SealedFile};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

pub mod replace;

#[cfg(test)]
mod tests;

/// What one run did: its report lines and every problem that makes it
/// fail.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResolveReport {
    /// The report in RA8 order.
    pub lines: Vec<String>,
    /// Every refusal and difference; empty when the run passed.
    pub problems: Vec<String>,
}

// ------------------------------------------------------------ pure parts

/// One open hold as resolve reads it (RA2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldPart {
    /// The data path.
    pub path: String,
    /// Its hold reason.
    pub reason: HoldReason,
    /// Hot's manifest text, if hot lists it.
    pub hot_text: Option<String>,
    /// The copy ledger's manifest text, if it records one.
    pub ledger_text: Option<String>,
}

impl HeldPart {
    /// The part's manifest: hot's text, else the ledger's.
    fn manifest(&self) -> Option<SealedFile> {
        self.hot_text
            .as_deref()
            .or(self.ledger_text.as_deref())
            .and_then(|text| replica::parse_manifest(text).ok())
    }
}

/// RA3: the archive path proposed for the held part `path` of `symbol`
/// with the manifest `manifest`, or why there is none.
///
/// # Errors
///
/// The reason there is no proposal.
pub fn propose(symbol: &str, path: &str, manifest: Option<&SealedFile>) -> Result<String, String> {
    let mut segments = path.split('/');
    let mut value = |prefix: &str| segments.next().and_then(|s| s.strip_prefix(prefix));
    let (source, instrument, stream) = (value("source="), value("instrument="), value("stream="));
    if source != Some(ARCHIVE_SOURCE) {
        return Err("not an archive part".to_owned());
    }
    if instrument != Some(symbol) {
        return Err(format!(
            "instrument {} is not the configured symbol {symbol}",
            instrument.unwrap_or("?")
        ));
    }
    let stream = stream
        .and_then(ArchiveStream::from_raw_name)
        .ok_or_else(|| format!("stream {} is no catalog stream", stream.unwrap_or("?")))?;
    let manifest = manifest.ok_or_else(|| "no manifest on hot or in the copy ledger".to_owned())?;
    let first = stream.period_of(manifest.min_event_time.as_millis());
    let last = stream.period_of(manifest.max_event_time.as_millis());
    if first != last {
        return Err(format!(
            "its ordering times map to two files, {} and {}",
            stream.archive_path(symbol, first),
            stream.archive_path(symbol, last)
        ));
    }
    Ok(stream.archive_path(symbol, first))
}

/// RA3 without `--file`: one line per open hold and whether none is open.
pub fn proposals(symbol: &str, holds: &[HeldPart]) -> (Vec<String>, bool) {
    let lines = holds
        .iter()
        .map(|held| {
            let what = match propose(symbol, &held.path, held.manifest().as_ref()) {
                Ok(file) => format!("--file {file}"),
                Err(why) => format!("no proposal ({why})"),
            };
            format!("held {} ({}): {what}", held.path, held.reason.as_str())
        })
        .collect();
    (lines, holds.is_empty())
}

/// RA4: parts(F), the open holds proposing `file`, as their manifests in
/// path order; `pending` is the archive path `PENDING` names, if any.
///
/// # Errors
///
/// The refusal.
pub fn attribute(
    symbol: &str,
    file: &str,
    holds: &[HeldPart],
    pending: Option<&str>,
) -> Result<Vec<SealedFile>, String> {
    attribute_with(symbol, file, holds, pending, false)
}

/// RA4 for a re-admit, or with `replace` for a replace (RQ2): a replace
/// accepts a `manifest-conflict` part and hot's text differing from the
/// ledger's, since it moves every copy and chooses none.
///
/// # Errors
///
/// The refusal.
pub fn attribute_with(
    symbol: &str,
    file: &str,
    holds: &[HeldPart],
    pending: Option<&str>,
    replace: bool,
) -> Result<Vec<SealedFile>, String> {
    let mut parts = Vec::new();
    let mut proposed: BTreeSet<String> = BTreeSet::new();
    let mut unproposed = Vec::new();
    for held in holds {
        match propose(symbol, &held.path, held.manifest().as_ref()) {
            Ok(proposal) if proposal == file => parts.push(held),
            Ok(proposal) => {
                proposed.insert(proposal);
            }
            Err(why) => unproposed.push(format!("{} ({why})", held.path)),
        }
    }
    if parts.is_empty() {
        let list: Vec<&str> = proposed.iter().map(String::as_str).collect();
        let mut why = format!(
            "no open hold proposes {file}; the open holds propose {}",
            if list.is_empty() {
                "no file".to_owned()
            } else {
                list.join(", ")
            }
        );
        if !unproposed.is_empty() {
            why.push_str(&format!(
                "; held without a proposal: {}",
                unproposed.join(", ")
            ));
        }
        return Err(format!("{why} (run without --file for every hold)"));
    }
    let mut manifests = Vec::new();
    for held in parts {
        if held.reason == HoldReason::ManifestConflict && !replace {
            return Err(format!(
                "{} is held as manifest-conflict: two roots hold different sealed files under its \
                 path, and re-admit never chooses between them; its resolution is the replace, \
                 `--replace` (ADR-062 D11)",
                held.path
            ));
        }
        if let (Some(hot), Some(ledger)) = (&held.hot_text, &held.ledger_text)
            && hot != ledger
            && !replace
        {
            return Err(format!(
                "{}: hot's manifest differs from the copy ledger's manifest record (ADR-062 D5); \
                 run `mie store verify`, then `mie store repair`",
                held.path
            ));
        }
        let manifest = held.manifest().ok_or_else(|| {
            format!(
                "{}: no manifest on hot or in the copy ledger, so its rows cannot be read",
                held.path
            )
        })?;
        manifests.push(manifest);
    }
    if pending == Some(file) {
        return Err(format!(
            "the import PENDING names {file}: its resume owns those parts (ADR-034 D4); run `mie \
             archive-import` once no hold is open, or resolve the holds of other files first"
        ));
    }
    Ok(manifests)
}

/// One import-ledger file of the archive file, on one root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileOf {
    /// `hot`, or a copy target's id.
    pub root: String,
    /// Its name.
    pub name: LedgerName,
    /// Its bytes.
    pub bytes: Vec<u8>,
}

/// What [`plan`] reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanInputs {
    /// The raw stream directory, the stream's raw name.
    pub segment: String,
    /// The archive path.
    pub archive: String,
    /// The raw stream.
    pub stream: RawStreamKey,
    /// The archive file's period.
    pub period: Period,
    /// F's files on hot and on every copy target.
    pub files: Vec<FileOf>,
    /// Malformed names in those directories that belong to F, as (root,
    /// name).
    pub malformed: Vec<Malformed>,
    /// parts(F), in path order.
    pub parts: Vec<SealedFile>,
    /// Whether a part of parts(F) is held as `entry-conflict`.
    pub entry_conflict: bool,
    /// The copy ledger's verdict of every recorded path.
    pub verdicts: BTreeMap<String, PathVerdict>,
    /// Where each path an entry of F lists is held: hot's or a cold root's
    /// manifest, a copy-ledger record. A path absent here is held by
    /// nothing.
    pub located: BTreeMap<String, Vec<String>>,
}

/// The RA5 case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Case {
    /// (a) No file of F: the missing entry.
    Missing,
    /// (b) A supersede record without its entry: an interrupted re-admit.
    Interrupted,
    /// (c) The latest entry already lists the held parts.
    Listed,
    /// (d) A superseding entry.
    Supersede,
}

/// What [`plan`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    /// The case.
    pub case: Case,
    /// The supersede record to write first, with its generation (d).
    pub record: Option<(u32, SupersedeRecord)>,
    /// The generation of the entry to write; `None` in case (c).
    pub entry: Option<u32>,
    /// The paths the row check compares, in path order.
    pub check_set: Vec<String>,
    /// Report lines: paths only a superseded entry lists and no root holds.
    pub lines: Vec<String>,
}

/// One byte variant of an entry: its bytes, the roots holding it and its
/// parse.
type Variant<'a> = (&'a [u8], Vec<&'a str>, Option<ImportLedger>);

/// The files of one name: its distinct byte variants with their roots.
fn variants<'a>(files: &[&'a FileOf]) -> Vec<(&'a [u8], Vec<&'a str>)> {
    let mut variants: Vec<(&[u8], Vec<&str>)> = Vec::new();
    for file in files {
        match variants.iter_mut().find(|(bytes, _)| *bytes == file.bytes) {
            Some((_, roots)) => roots.push(&file.root),
            None => variants.push((&file.bytes, vec![&file.root])),
        }
    }
    variants
}

/// What [`survey`] read of F's files (RA5's read and binding rules).
struct Survey<'a> {
    /// Every name with its copies.
    by_name: BTreeMap<&'a LedgerName, Vec<&'a FileOf>>,
    /// Every entry name with its byte variants and their parses.
    parsed: BTreeMap<&'a LedgerName, Vec<Variant<'a>>>,
    /// Every supersede record by generation.
    records: BTreeMap<u32, SupersedeRecord>,
    /// Entry variants on the targets only that cannot be read or bound:
    /// (generation, bytes, the refusal or note naming it).
    unreadable: Vec<(u32, &'a [u8], String)>,
    /// Notes on malformed names on a copy target only.
    notes: Vec<String>,
}

/// The remedy RA5 names for a file that does not read.
const REPAIR: &str = "run `mie store verify`, then `mie store repair` (or the hot restore, #195)";

impl Survey<'_> {
    /// The entry copies the case decides on (`decides`) must read; any
    /// other is superseded and only named.
    fn decided(&self, decides: &dyn Fn(u32, &[u8]) -> bool) -> Result<Vec<String>, String> {
        let mut lines = self.notes.clone();
        for (generation, bytes, why) in &self.unreadable {
            if decides(*generation, bytes) {
                return Err(format!("{why}: {REPAIR}"));
            }
            lines.push(format!(
                "{why}: a superseded entry copy, which decides nothing here and which no \
                 verification or repair reaches (ADR-062 D11)"
            ));
        }
        Ok(lines)
    }

    /// The highest entry generation.
    fn highest_entry(&self) -> Option<u32> {
        self.parsed.keys().map(|name| name.generation).max()
    }

    /// The highest generation of any file of F.
    fn highest(&self) -> Option<u32> {
        self.by_name.keys().map(|name| name.generation).max()
    }

    /// The variants of the entry of generation `g`.
    fn variants_of(&self, g: u32) -> Option<(&LedgerName, &Vec<Variant<'_>>)> {
        self.parsed
            .iter()
            .find(|(name, _)| name.generation == g)
            .map(|(name, list)| (*name, list))
    }
}

/// RA5's read and binding rules over F's files (module docs): a malformed
/// name on hot, a name on the targets only, differing record copies and a
/// file on hot that does not read are refused; a malformed name or an
/// unreadable entry copy on a target only is kept for [`Survey::decided`].
fn survey(inputs: &PlanInputs) -> Result<Survey<'_>, String> {
    // A malformed name on hot fails the importer and the replication of F
    // closed; on a copy target only it counts for nothing anywhere.
    let mut notes = Vec::new();
    for (root, name) in &inputs.malformed {
        if root == HOT_ID {
            return Err(format!(
                "{name} on {root} is a malformed import-ledger name of this archive file: {REPAIR}"
            ));
        }
        notes.push(format!(
            "{}/{name} on {root}: a malformed import-ledger name on a copy target only; no \
             reader counts it, so it decides nothing here (ADR-062 D11)",
            inputs.segment
        ));
    }
    // By name: its copies.
    let mut by_name: BTreeMap<&LedgerName, Vec<&FileOf>> = BTreeMap::new();
    for file in &inputs.files {
        by_name.entry(&file.name).or_default().push(file);
    }
    let mut parsed: BTreeMap<&LedgerName, Vec<Variant<'_>>> = BTreeMap::new();
    let mut records: BTreeMap<u32, SupersedeRecord> = BTreeMap::new();
    let mut unreadable: Vec<(u32, &[u8], String)> = Vec::new();
    for (name, copies) in &by_name {
        let label = format!("{}/{}", inputs.segment, name.render());
        if !copies.iter().any(|copy| copy.root == HOT_ID) {
            return Err(format!(
                "{label} is on {} only, not on hot: {REPAIR}",
                copies
                    .iter()
                    .map(|c| c.root.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let found = variants(copies);
        if name.kind == LedgerKind::Supersede && found.len() > 1 {
            return Err(format!(
                "the copies of supersede record {label} differ: {REPAIR}"
            ));
        }
        for (bytes, roots) in &found {
            let pair = ((*name).clone(), bytes.to_vec());
            if let Err(e) = mie_adapter_binance::archive::ledger::classify(
                &inputs.segment,
                std::slice::from_ref(&pair),
            ) {
                let why = format!(
                    "{label} on {}: it cannot be read as such ({e})",
                    roots.join(", ")
                );
                // Hot's copy of any generation is read by the importer and
                // the replication, which fail F closed on it (re-review of
                // #212, finding 3): only a target's copy may be superseded
                // and merely named.
                match name.kind {
                    LedgerKind::Entry if !roots.contains(&HOT_ID) => {
                        unreadable.push((name.generation, bytes, why));
                    }
                    _ => return Err(format!("{why}: {REPAIR}")),
                }
            }
        }
        match name.kind {
            LedgerKind::Supersede => {
                let text = std::str::from_utf8(found[0].0).map_err(|e| e.to_string())?;
                let record = SupersedeRecord::parse(text).map_err(|e| e.to_string())?;
                records.insert(name.generation, record);
            }
            LedgerKind::Entry => {
                let list = found
                    .into_iter()
                    .map(|(bytes, roots)| {
                        let ledger = std::str::from_utf8(bytes)
                            .ok()
                            .and_then(|text| ImportLedger::parse(text).ok());
                        (bytes, roots, ledger)
                    })
                    .collect();
                parsed.insert(*name, list);
            }
        }
    }
    Ok(Survey {
        by_name,
        parsed,
        records,
        unreadable,
        notes,
    })
}

/// RA5 (module docs), pure.
///
/// # Errors
///
/// The refusal; nothing may be written.
pub fn plan(inputs: &PlanInputs) -> Result<Planned, String> {
    let survey = survey(inputs)?;
    let Survey {
        by_name,
        parsed,
        records,
        notes,
        ..
    } = &survey;
    let highest_entry = survey.highest_entry();
    let m = records.keys().copied().max();
    let highest = survey.highest();
    let held: BTreeSet<&str> = inputs
        .parts
        .iter()
        .map(|p| p.relative_path.as_str())
        .collect();
    let parts_set: Vec<String> = inputs
        .parts
        .iter()
        .map(|p| p.relative_path.clone())
        .collect();

    // No orphan left behind (b, d).
    let no_orphan = |superseded: &[Variant<'_>]| {
        let mut lines = Vec::new();
        let listed: BTreeSet<&str> = superseded
            .iter()
            .filter_map(|(_, _, ledger)| ledger.as_ref())
            .flat_map(|ledger| ledger.files.iter().map(|f| f.relative_path.as_str()))
            .collect();
        for path in listed {
            if held.contains(path) {
                continue;
            }
            if let Some(places) = inputs.located.get(path)
                && !places.is_empty()
            {
                return Err(format!(
                    "{path} is listed by an entry this run would supersede and is not held for \
                     this file, but {} holds or records it: superseding would leave it listed by \
                     no current entry; nothing is written (ADR-062 D11)",
                    places.join(", ")
                ));
            }
            lines.push(format!(
                "{path}: listed only by the superseded entry; no root holds it"
            ));
        }
        Ok(lines)
    };
    let decided = |decides: &dyn Fn(u32, &[u8]) -> bool| survey.decided(decides);
    let notes = notes.clone();
    let variants_of = |g: u32| survey.variants_of(g);

    // (a)
    if by_name.is_empty() && !inputs.entry_conflict {
        return Ok(Planned {
            case: Case::Missing,
            record: None,
            entry: Some(0),
            check_set: parts_set,
            lines: notes,
        });
    }
    // (b)
    if let Some(m) = m
        && highest_entry.is_none_or(|g| m > g)
    {
        if let Some(quarantine) = &records[&m].quarantine {
            return Err(format!(
                "supersede record {}/{} names quarantine {}: a replace is pending; `mie store \
                 resolve --file` resumes it (RQ7)",
                inputs.segment,
                LedgerName::supersede(&archive_file(&inputs.archive), m).render(),
                quarantine.id
            ));
        }
        let below = parsed
            .iter()
            .filter(|(name, _)| name.generation < m)
            .max_by_key(|(name, _)| name.generation);
        // The record names the bytes it supersedes; a copy of other bytes
        // is one it never saw, superseded by name all the same (G4).
        let below_generation = below.map(|(name, _)| name.generation);
        let below_label = below.map(|(name, _)| format!("{}/{}", inputs.segment, name.render()));
        let named = |bytes: &[u8]| {
            below_label.as_ref().is_some_and(|label| {
                records[&m]
                    .entries
                    .contains(&SupersededEntry::of(label.clone(), bytes))
            })
        };
        let mut lines =
            decided(&|generation, bytes| Some(generation) == below_generation && named(bytes))?;
        let below = below.map(|(_, list)| list.as_slice()).unwrap_or_default();
        lines.extend(no_orphan(below)?);
        return Ok(Planned {
            case: Case::Interrupted,
            record: None,
            entry: Some(m),
            check_set: parts_set,
            lines,
        });
    }
    // The latest entry: none only in (a) with an `entry-conflict` part.
    let latest = match highest_entry {
        Some(g) => Some(variants_of(g).ok_or_else(|| "no latest entry".to_owned())?),
        None if by_name.is_empty() => None,
        None => return Err("no entry and no supersede record".to_owned()),
    };
    let mut lines = decided(&|generation, _| Some(generation) == highest_entry)?;
    // (c), never at generation 0 for an `entry-conflict` part: B7′ clears
    // such a hold only on an entry of generation ≥ 1.
    if let Some((_, list)) = latest
        && let [(_, _, Some(ledger))] = list.as_slice()
        && !(inputs.entry_conflict && highest_entry == Some(0))
    {
        let by_path: BTreeMap<&str, (u64, &str)> = ledger
            .files
            .iter()
            .map(|f| (f.relative_path.as_str(), (f.rows, f.sha256.as_str())))
            .collect();
        let lists_parts = inputs.parts.iter().all(|part| {
            by_path.get(part.relative_path.as_str()) == Some(&(part.rows, part.sha256.as_str()))
        });
        let others_ordinary = by_path.keys().all(|path| {
            held.contains(path)
                || matches!(
                    inputs.verdicts.get(*path),
                    None | Some(PathVerdict::Ordinary)
                )
        });
        if lists_parts && others_ordinary {
            return Ok(Planned {
                case: Case::Listed,
                record: None,
                entry: None,
                check_set: ledger
                    .files
                    .iter()
                    .map(|f| f.relative_path.clone())
                    .collect(),
                lines,
            });
        }
    }
    // (d)
    let n = highest.unwrap_or(0).checked_add(1).ok_or_else(|| {
        format!(
            "the next generation of {} would exceed {}: a generation never wraps to 0, which \
                 would name the first entry",
            archive_file(&inputs.archive),
            u32::MAX
        )
    })?;
    let mut entries: Vec<SupersededEntry> = Vec::new();
    if let Some((latest_name, latest)) = latest {
        lines.extend(no_orphan(latest)?);
        entries = latest
            .iter()
            .map(|(bytes, _, _)| {
                SupersededEntry::of(
                    format!("{}/{}", inputs.segment, latest_name.render()),
                    bytes,
                )
            })
            .collect();
    }
    entries.sort();
    entries.dedup();
    Ok(Planned {
        case: Case::Supersede,
        record: Some((
            n,
            SupersedeRecord {
                archive: inputs.archive.clone(),
                stream: inputs.stream.clone(),
                period: inputs.period,
                entries,
                quarantine: None,
            },
        )),
        entry: Some(n),
        check_set: parts_set,
        lines,
    })
}

/// The file name of an archive path.
fn archive_file(archive: &str) -> String {
    archive.rsplit('/').next().unwrap_or(archive).to_owned()
}

// ------------------------------------------------------------- the run

/// Why the run stops: a refusal, nothing written by it.
struct Stop(String);

/// Runs one resolve (module docs). `file` is `--file`, `replace` is
/// `--replace`; `now_ms` stamps the records, read once.
pub fn run(
    config: &IngestConfig,
    archive: Option<&Path>,
    file: Option<&str>,
    replace: bool,
    transports: &ArchiveTransports,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
) -> ResolveReport {
    let mut report = ResolveReport::default();
    if replace && file.is_none() {
        report.problems.push(
            "--replace needs --file: a replace moves the superseded generation of one archive \
             file (ADR-062 D11)"
                .to_owned(),
        );
        return report;
    }
    if let Err(Stop(why)) = resolve(
        config,
        archive,
        file,
        replace,
        transports,
        probe,
        now_ms,
        &mut report,
    ) {
        report.problems.push(why);
    }
    report
}

/// A copy target that presented its marker.
struct Target {
    path: PathBuf,
    id: String,
}

/// A malformed import-ledger name of F: (root, name).
pub type Malformed = (String, String);

/// Lists F's import-ledger files in `dir` of `root`: its parsed names with
/// their bytes, and the malformed names that belong to F.
fn files_in(
    root: &str,
    dir: &Path,
    archive_file: &str,
) -> Result<(Vec<FileOf>, Vec<Malformed>), Stop> {
    let io = |error: std::io::Error| {
        Stop(format!(
            "the import ledger {} on {root} cannot be read ({error}); nothing is written",
            dir.display()
        ))
    };
    let mut names = match fs::read_dir(dir) {
        Ok(entries) => entries
            .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(io)?,
        Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(io(error)),
    };
    names.sort();
    let (mut files, mut malformed) = (Vec::new(), Vec::new());
    for name in names {
        if name.starts_with('.') || !name.ends_with(".import") {
            continue;
        }
        match LedgerName::parse(&name) {
            Ok(parsed) if parsed.file == archive_file => {
                let bytes = fs::read(dir.join(&name)).map_err(io)?;
                files.push(FileOf {
                    root: root.to_owned(),
                    name: parsed,
                    bytes,
                });
            }
            Ok(_) => {}
            Err(_) => {
                if name.starts_with(&format!("{archive_file}.")) {
                    malformed.push((root.to_owned(), name));
                }
            }
        }
    }
    Ok((files, malformed))
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
fn resolve(
    config: &IngestConfig,
    archive: Option<&Path>,
    file: Option<&str>,
    replace: bool,
    transports: &ArchiveTransports,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    report: &mut ResolveReport,
) -> Result<(), Stop> {
    let hot = config.paths.raw_root.as_path();
    let cold_roots = &config.paths.cold_roots;
    // RA1.
    if cold_roots.is_empty() {
        return Err(Stop(
            "paths.cold_roots is empty: a single-root store has no holds to resolve (ADR-062 D11)"
                .to_owned(),
        ));
    }
    check_hot(hot, cold_roots).map_err(Stop)?;
    let archive = replicate::archive_config(config, archive)
        .map_err(Stop)?
        .ok_or_else(|| {
            Stop(
                "no archive configuration is given (--archive-config or [history] \
                 archive_config): holds are archive parts, resolved against the import ledger \
                 (ADR-062 D11)"
                    .to_owned(),
            )
        })?;
    check_copy_ledger(hot, cold_roots).map_err(|e| {
        Stop(e.replace(
            "the importer refuses, since its days can lie outside the hot window",
            "the hold resolution refuses",
        ))
    })?;
    let mut targets: Vec<Target> = Vec::new();
    let mut ids: BTreeMap<String, PathBuf> = BTreeMap::new();
    for cold in cold_roots {
        match root_marker::read(&cold.path) {
            Ok(Some(id)) if id == HOT_ID => {
                return Err(Stop(format!(
                    "cold root {} carries hot's marker (id {HOT_ID:?}) (ADR-062 D7)",
                    cold.path.display()
                )));
            }
            Ok(Some(id)) => {
                if let Some(other) = ids.get(&id) {
                    return Err(Stop(format!(
                        "cold roots {} and {} both carry the id {id:?} (ADR-062 D7)",
                        other.display(),
                        cold.path.display()
                    )));
                }
                ids.insert(id.clone(), cold.path.clone());
                if cold.copy_target {
                    targets.push(Target {
                        path: cold.path.clone(),
                        id,
                    });
                }
            }
            Ok(None) if cold.copy_target => {
                return Err(Stop(format!(
                    "copy target {} has no root marker: resolve reads every target's import \
                     ledger, and an unmounted target reads as empty (ADR-062 D7)",
                    cold.path.display()
                )));
            }
            Ok(None) => {}
            Err(error) if cold.copy_target => {
                return Err(Stop(format!(
                    "copy target {}: its root marker cannot be read ({error})",
                    cold.path.display()
                )));
            }
            Err(_) => {}
        }
    }
    let target_paths: Vec<&Path> = targets.iter().map(|t| t.path.as_path()).collect();
    check_devices(hot, &target_paths, probe).map_err(Stop)?;
    let _lock = removal::take_source_lock(hot, ARCHIVE_SOURCE).map_err(|error| {
        Stop(format!(
            "the archive source's writer lock: {error}; resolve takes it without waiting and holds \
             it for its whole run, excluding the importer, the rebuild and a second resolve \
             (ADR-062 D11)"
        ))
    })?;

    // RA2.
    let ledger = match copy_ledger::read(hot) {
        Ok(Some(ledger)) => ledger,
        Ok(None) => return Err(Stop("hot's copy ledger is gone".to_owned())),
        Err(error) => return Err(Stop(format!("hot's copy ledger: {error}"))),
    };
    let state = CopyState::fold(&ledger)
        .map_err(|error| Stop(format!("the copy ledger cannot be folded: {error}")))?;
    let mut holds = Vec::new();
    for (path, reason) in state.held() {
        let hot_text = replica::sealed_manifest(hot, &path)
            .map_err(|error| Stop(format!("{path}: hot's manifest: {error}")))?
            .map(|(text, _)| text);
        holds.push(HeldPart {
            ledger_text: state.ledger_text(&path).map(str::to_owned),
            path,
            reason,
            hot_text,
        });
    }
    let symbol = archive.instrument.symbol.clone();
    let ledgers = LedgerDir::new(&archive.paths.import_ledger);

    // RA3, RQ8.
    let Some(file) = file else {
        let (lines, none) = proposals(&symbol, &holds);
        report.lines.extend(lines);
        if !none {
            report.problems.push(format!(
                "{} hold(s) open; resolve each proposed file with --file",
                holds.len()
            ));
        }
        let roots = quarantine::ReplaceRoots::read(hot, cold_roots);
        let (lines, problems) =
            quarantine::replace_lines(&roots, &ledgers, &state).map_err(Stop)?;
        report.lines.extend(lines);
        report.problems.extend(problems);
        return Ok(());
    };
    let (stream, period) = parse_archive_path(&symbol, file).ok_or_else(|| {
        Stop(format!(
            "--file {file:?} is not an archive path of {symbol}: it must be exactly a catalog \
             path below the base URL, such as \
             data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-YYYY-MM-DD.zip, as the \
             proposals without --file print it"
        ))
    })?;
    let pending = match ledgers.pending() {
        Ok(pending) => pending.map(|p| p.archive),
        Err(error) => {
            return Err(Stop(format!(
                "the import PENDING cannot be read ({error}): it may name this file"
            )));
        }
    };
    let segment = stream.raw_name().to_owned();
    let file_name = stream.file_name(&symbol, period);
    let mut files = Vec::new();
    let mut malformed = Vec::new();
    let dirs = std::iter::once((
        HOT_ID.to_owned(),
        archive.paths.import_ledger.join(&segment),
    ))
    .chain(targets.iter().map(|t| {
        (
            t.id.clone(),
            t.path.join(META_DIR).join(IMPORT_LEDGER_DIR).join(&segment),
        )
    }));
    for (root, dir) in dirs {
        let (found, bad) = files_in(&root, &dir, &file_name)?;
        let names: Vec<String> = found.iter().map(|f| f.name.render()).collect();
        report.lines.push(format!(
            "import ledger on {root} ({}): {}",
            dir.display(),
            if names.is_empty() {
                format!("no file of {file_name}")
            } else {
                names.join(", ")
            }
        ));
        files.extend(found);
        malformed.extend(bad);
    }

    // RQ7 (a pending replace resumes), RQ0 (`--replace`), else re-admit.
    let layouts = quarantine::ReplaceRoots::read(hot, cold_roots);
    let resume = replace::pending_replace(&files, &|record| {
        !matches!(layouts.unfinished(record, false), Ok(None))
    });
    let replacing = resume.is_some() || replace;
    let mut roots = replace::marked_roots(cold_roots);
    let _pass = if replacing {
        roots = replace::check_roots(hot, cold_roots).map_err(Stop)?;
        Some(replica::try_replication_lock(hot).map_err(|error| {
            Stop(format!(
                "the pass lock: {error}; the replace takes it without waiting and holds it for \
                 its whole run, so no pass copies a moved part back into a layout (ADR-062 D11)"
            ))
        })?)
    } else {
        None
    };
    let ctx = replace::Context {
        hot,
        roots: &roots,
        state: &state,
        ledgers: &ledgers,
        stream,
        file_name: &file_name,
        archive: file,
        journal: &config.paths.journal,
        now_ms,
    };
    if let Some((generation, record)) = &resume {
        if pending.as_deref() == Some(file) {
            return Err(Stop(format!(
                "the import PENDING names {file}: its resume owns those parts (ADR-034 D4)"
            )));
        }
        return replace::run_resume(&ctx, *generation, record, &files, report);
    }
    let parts = attribute_with(&symbol, file, &holds, pending.as_deref(), replace).map_err(Stop)?;
    let entry_conflict = holds.iter().any(|held| {
        held.reason == HoldReason::EntryConflict
            && parts.iter().any(|part| part.relative_path == held.path)
    });
    report
        .lines
        .push(format!("file {file}: {} held part(s)", parts.len()));
    for part in &parts {
        let reason = holds
            .iter()
            .find(|h| h.path == part.relative_path)
            .map_or("?", |h| h.reason.as_str());
        report
            .lines
            .push(format!("part {} ({reason})", part.relative_path));
    }

    // RA5.
    let mut listed: BTreeSet<String> = BTreeSet::new();
    for f in &files {
        if f.name.kind == LedgerKind::Entry
            && let Ok(text) = std::str::from_utf8(&f.bytes)
            && let Ok(entry) = ImportLedger::parse(text)
        {
            listed.extend(entry.files.into_iter().map(|f| f.relative_path));
        }
    }
    let mut located: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut verdicts = BTreeMap::new();
    for path in &listed {
        let mut places = Vec::new();
        if matches!(replica::sealed_manifest(hot, path), Ok(Some(_)) | Err(_)) {
            places.push("hot".to_owned());
        }
        for cold in cold_roots {
            let manifest = format!("{}.manifest", path.strip_suffix(".parquet").unwrap_or(path));
            if fs::symlink_metadata(cold.path.join(&manifest)).is_ok()
                || fs::symlink_metadata(cold.path.join(path)).is_ok()
            {
                places.push(cold.path.display().to_string());
            }
        }
        if let Some(verdict) = state.verdict(path) {
            places.push(format!("the copy ledger ({verdict:?})"));
            verdicts.insert(path.clone(), verdict);
        }
        located.insert(path.clone(), places);
    }
    let key = RawStreamKey::new(ARCHIVE_SOURCE, &symbol, &segment)
        .map_err(|error| Stop(error.to_string()))?;
    for part in &parts {
        if let Some(verdict) = state.verdict(&part.relative_path) {
            verdicts.insert(part.relative_path.clone(), verdict);
        }
    }
    let inputs = PlanInputs {
        segment: segment.clone(),
        archive: file.to_owned(),
        stream: key,
        period,
        files,
        malformed,
        parts: parts.clone(),
        entry_conflict,
        verdicts,
        located,
    };
    if replace {
        return replace::run_replace(&ctx, &inputs, report);
    }
    let planned = plan(&inputs).map_err(Stop)?;
    report.lines.extend(planned.lines.iter().cloned());

    // RA6.
    let mut check_set = Vec::new();
    for path in &planned.check_set {
        if let Some(part) = parts.iter().find(|p| p.relative_path == *path) {
            check_set.push(part.clone());
            continue;
        }
        let manifest = match replica::sealed_manifest(hot, path) {
            Ok(Some((_, manifest))) => Some(manifest),
            _ => state
                .ledger_text(path)
                .and_then(|text| replica::parse_manifest(text).ok()),
        };
        check_set.push(manifest.ok_or_else(|| {
            Stop(format!(
                "{path}: no manifest on hot or in the copy ledger, so its rows cannot be read"
            ))
        })?);
    }
    let store = super::raw_store(hot, cold_roots).map_err(Stop)?;
    let mut fetcher = Fetcher::new(
        transports.http.as_ref(),
        transports.download.as_ref(),
        transports.clock.as_ref(),
        fetch_policy(&archive),
    );
    let checked = readmit::check(
        &mut fetcher,
        &store,
        &ReadmitRequest {
            symbol: symbol.clone(),
            base_url: archive.archive.base_url.clone(),
            staging_dir: archive.paths.staging.clone(),
            stream,
            period,
            parts: check_set.clone(),
        },
    )
    .map_err(|error| {
        Stop(format!(
            "the re-admit check of {file} could not decide: {error}; nothing written; the hold \
             stays"
        ))
    })?;
    report.lines.push(format!(
        "published sha256 {}, {} rows",
        checked.published, checked.rows
    ));
    let entry = match checked.outcome {
        Ok(entry) => entry,
        Err(difference) => {
            // RQ5: the difference prints what a replace would cost; it
            // still writes nothing.
            let preview = replace::holders_of(&ctx, &inputs)
                .and_then(|holders| {
                    replace::plan_replace(&inputs, &holders, "q-preview").map_err(Stop)
                })
                .and_then(|plan| ctx.cost_lines(plan.moved(), &|_| Vec::new()));
            match preview {
                Ok(lines) => report.lines.extend(lines),
                Err(Stop(why)) => report
                    .lines
                    .push(format!("cost: a replace would be refused: {why}")),
            }
            return Err(Stop(format!(
                "{difference}; nothing written; the hold stays; `mie store resolve --config \
                 <ingest config> --file {file} --replace` moves the superseded generation into \
                 quarantine at the cost above (ADR-062 D11)"
            )));
        }
    };
    report.lines.push(format!(
        "rows match ({} parts, {} rows)",
        check_set.len(),
        checked.rows
    ));

    // RA7.
    if let Some((generation, record)) = &planned.record {
        ledgers
            .write_supersede(stream, &file_name, *generation, record)
            .map_err(|error| Stop(format!("writing the supersede record failed: {error}")))?;
        report.lines.push(format!(
            "wrote {segment}/{}",
            LedgerName::supersede(&file_name, *generation).render()
        ));
    }
    if let Some(generation) = planned.entry {
        ledgers
            .write(stream, &file_name, &entry)
            .map_err(|error| Stop(format!("writing the entry failed: {error}")))?;
        report.lines.push(format!(
            "wrote {segment}/{}",
            LedgerName::entry(&file_name, generation).render()
        ));
    }
    let at_ms = now_ms();
    let appender = CopyLedger::at(hot);
    let ledger_path = copy_ledger::ledger_path(hot);
    for part in &parts {
        let record = Record {
            at_ms,
            kind: Kind::HoldCleared {
                path: part.relative_path.clone(),
                resolution: Resolution::Readmit,
            },
        };
        match appender.append(&record) {
            Ok(outcome) => {
                report
                    .lines
                    .push(format!("hold-cleared {} readmit", part.relative_path));
                if let Some(truncation) = outcome.truncation {
                    report.lines.push(truncation_line(&ledger_path, truncation));
                }
            }
            Err(error) => {
                if let Some(truncation) = error.truncation {
                    report.lines.push(truncation_line(&ledger_path, truncation));
                }
                return Err(Stop(format!(
                    "{}: the hold-cleared record could not be appended ({}); a re-run clears the \
                     rest",
                    part.relative_path, error.error
                )));
            }
        }
    }
    Ok(())
}
