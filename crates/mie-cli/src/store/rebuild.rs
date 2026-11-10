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
//!   data checked against that root's own manifest. The journal and the
//!   entries come first: a part is sealed before its `sealed` line or its
//!   entry, so a "no root holds it" verdict never comes from a listing
//!   that ran ahead of the record. An I/O error on a listing, a manifest,
//!   the journal, an entry or a ledger copy stops the run; unreadable data
//!   is a damaged copy (B5), never an absence.
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
//!   recorded; a path the start records lost stays lost.
//! - **B7 Archive holds**, re-derived ([`archive_checks`]).
//! - **B8 Removed:** a partition with a verified cold copy and no
//!   directory on hot; inside the hot window it is also a `FAIL:` line.
//! - **B9 Install:** markers re-read, the old ledger linked aside, the gate
//!   written, the new ledger renamed into place
//!   ([`LedgerHold::replace`](mie_adapter_parquet::LedgerHold::replace)),
//!   then every ledger copy that is no prefix of it moved aside.
//! - **B10** The hot removal waits for a verification covering the rebuilt
//!   ledger ([`removal::plan`](mie_adapter_parquet::removal::plan) rule (g)).
//! - **B11** The report; `PASS` only with the ledger written and no `FAIL:`
//!   line.

use super::archive_checks::{self, ArchiveInputs, EntryCopy, PartTexts};
use super::replicate::{self, IMPORT_LEDGER_DIR, META_DIR, entry_files, is_archive_part};
use super::{DeviceProbe, check_devices, check_hot};
use crate::config::IngestConfig;
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_binance::archive::ledger::{PENDING_FILE, Pending};
use mie_adapter_parquet::copy_ledger::{
    self, CopyLedger, HoldReason, Kind, LedgerBuilder, LedgerCopyCheck, LostOrigin, Record,
};
use mie_adapter_parquet::removal;
use mie_adapter_parquet::replica::{self, CopyCheck};
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
}

/// One copy target's ledger copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerCopyRead {
    /// The target's id.
    pub id: String,
    /// Its bytes, `None` when it holds none.
    pub bytes: Option<Vec<u8>>,
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

/// Everything [`derive`] reads (B1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildInputs {
    /// The run's `at_ms`, read once: every record carries it.
    pub at_ms: i64,
    /// The first date of the hot window.
    pub first_hot_date: String,
    /// `--record-lost`.
    pub record_lost: bool,
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
    /// Report lines (B11): the start, the rejected copies, the journal,
    /// the records added, the lost files, the holds.
    pub lines: Vec<String>,
    /// Every `FAIL:` finding.
    pub problems: Vec<String>,
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
        }
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

/// B2: the start among the ledger copies, its builder and its ledger.
fn choose_start(
    copies: &[LedgerCopyRead],
) -> (Start, LedgerBuilder, copy_ledger::Ledger, Vec<String>) {
    let mut rejected = Vec::new();
    let mut best: Option<(usize, copy_ledger::Ledger)> = None;
    for (i, copy) in copies.iter().enumerate() {
        let Some(bytes) = &copy.bytes else {
            rejected.push(format!("the ledger copy on {}: rejected: absent", copy.id));
            continue;
        };
        let name = PathBuf::from(format!("{}:.mie/copy-ledger", copy.id));
        match copy_ledger::parse(&name, bytes) {
            Ok(ledger) => {
                if best
                    .as_ref()
                    .is_none_or(|(_, known)| ledger.verified_len > known.verified_len)
                {
                    best = Some((i, ledger));
                }
            }
            Err(error) => {
                rejected.push(format!("the ledger copy on {}: rejected: {error}", copy.id))
            }
        }
    }
    match best {
        Some((i, ledger)) => {
            let bytes = copies[i].bytes.as_deref().unwrap_or_default();
            let builder = LedgerBuilder::from_prefix(Path::new("start"), bytes)
                .unwrap_or_else(|_| LedgerBuilder::empty());
            let start = Start {
                root: Some(copies[i].id.clone()),
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

/// The derivation of B2 to B8 over what B1 read: pure.
///
/// # Errors
///
/// A [`Stop`] for a live manifest conflict or archive parts without an
/// archive configuration; nothing may be written then.
pub fn derive(inputs: &RebuildInputs) -> Result<Derived, Stop> {
    let mut lines = Vec::new();
    let mut problems = Vec::new();

    // B2.
    let (start, mut builder, start_ledger, rejected) = choose_start(&inputs.ledger_copies);
    let state = CopyState::fold(&start_ledger)
        .map_err(|error| Stop(format!("the start copy cannot be folded: {error}")))?;
    let mut start_texts: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut sources: BTreeMap<String, BTreeSet<Source>> = BTreeMap::new();
    for record in &start_ledger.records {
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
    lines.push(format!(
        "journal: {} byte(s) read, {} complete non-JSON line(s) counted and not parsed",
        inputs.journal.bytes, inputs.journal.non_json
    ));

    // B0, last refusal: archive paths need the archive configuration.
    if !inputs.archive_configured {
        let held = inputs
            .hot
            .files
            .keys()
            .chain(&inputs.hot.orphan_data)
            .chain(
                inputs
                    .roots
                    .iter()
                    .flat_map(|root| root.files.keys().chain(&root.orphan_data)),
            )
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
    // archive part only hot holds would become a permanent `unlisted` hold
    // (nothing clears a hold before #194; review of #209, finding 2). It
    // stops the rebuild wherever it could hide an entry: an archive part
    // on a root or in the start, or a copy target holding entries.
    if let Some(dir) = &inputs.missing_import_ledger {
        let part = inputs
            .hot
            .files
            .keys()
            .chain(&inputs.hot.orphan_data)
            .chain(
                inputs
                    .roots
                    .iter()
                    .flat_map(|root| root.files.keys().chain(&root.orphan_data)),
            )
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
    let mut conflicts = Vec::new();
    for (path, held) in &holders {
        let texts = distinct(path);
        if texts.len() > 1 && !is_archive_part(path) {
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

    // B7: the archive checks.
    let parts: BTreeMap<String, PartTexts> = holders
        .keys()
        .copied()
        .chain(sources.keys().map(String::as_str))
        .filter(|path| is_archive_part(path))
        .map(|path| {
            let roots = holders
                .get(path)
                .into_iter()
                .flatten()
                .filter(|(root, _)| *root != "the start copy")
                .map(|(root, text)| ((*root).to_owned(), (*text).to_owned()))
                .collect();
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

    // B4, B5: per path in path order.
    let mut records: Vec<Kind> = Vec::new();
    for path in holders.keys().copied() {
        let texts = distinct(path);
        let text: Option<&str> = match texts[..] {
            [only] => Some(only),
            [] => None,
            _ => {
                let listed: BTreeSet<&str> = findings
                    .current
                    .values()
                    .flatten()
                    .filter(|(listed, _)| listed == path)
                    .map(|(_, sha)| sha.as_str())
                    .collect();
                let matching: Vec<&str> = texts
                    .iter()
                    .copied()
                    .filter(|text| {
                        replica::parse_manifest(text)
                            .is_ok_and(|file| listed.contains(file.sha256.as_str()))
                    })
                    .collect();
                match matching[..] {
                    [only] => Some(only),
                    _ => None,
                }
            }
        };
        // B5: copies that hold the path and do not verify.
        let mut damaged_cold = false;
        let mut damage = |root: &str, why: String, cold: bool| {
            damaged_cold |= cold;
            problems.push(format!(
                "{path} on {root}: {why}: the copy does not verify and gets no record; run `mie \
                 store verify`, then `mie store repair` (ADR-062 D11)"
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
            match copy {
                RootCopy::BadText(why) => damage(root, why.clone(), is_cold),
                RootCopy::Text { text: own, data } if Some(own.as_str()) == text => match data {
                    DataCheck::Good => verified.push(root),
                    DataCheck::NotChecked => {}
                    DataCheck::Missing => {
                        damage(
                            root,
                            "missing: a manifest without its data".to_owned(),
                            true,
                        );
                    }
                    DataCheck::Size => damage(root, "size".to_owned(), true),
                    DataCheck::Sha256 => damage(root, "sha256".to_owned(), true),
                    DataCheck::Unreadable(error) => {
                        damage(root, format!("its data cannot be read ({error})"), true);
                    }
                },
                RootCopy::Text { .. } => {}
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
        if !start_texts.contains_key(path) && (!targets.is_empty() || damaged_cold) {
            records.push(Kind::Manifest {
                text: text.to_owned(),
            });
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

    // B6: lost.
    let held_anywhere = |path: &str| {
        inputs.hot.files.contains_key(path)
            || inputs.hot.orphan_data.contains(path)
            || inputs
                .roots
                .iter()
                .any(|root| root.files.contains_key(path) || root.orphan_data.contains(path))
    };
    let mut lost = Vec::new();
    for (path, named) in &sources {
        let described: Vec<String> = named.iter().map(Source::describe).collect();
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
            if held {
                problems.push(format!(
                    "{path}: the start copy records it lost, and a root holds it again; it stays \
                     lost, since recovering a lost file needs a new ADR (ADR-062 D11)"
                ));
            }
            continue;
        }
        if held {
            continue;
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
        } else {
            (LostOrigin::Ledger, None)
        };
        lines.push(format!(
            "lost: {path}: origin {}, sha256 {}; named by {}",
            origin.as_str(),
            sha256.as_deref().unwrap_or("-"),
            described.join(", ")
        ));
        let what = if inputs.record_lost {
            "recorded lost (--record-lost): every selection that covers it fails from now on"
        } else {
            "nothing is written; after checking for a shared cause (an unmounted root, roots \
             restored from an old backup), `mie store rebuild --record-lost` records it lost"
        };
        problems.push(format!(
            "{path}: no root holds it (no data file and no manifest on hot or on any cold root), \
             and {} names it; {what} (ADR-062 D7 step 4, D11)",
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
    let mut holds = Vec::new();
    let mut hold_records = Vec::new();
    for (path, reason) in &findings.holds {
        holds.push((path.clone(), *reason));
        lines.push(format!("hold: {path} ({})", reason.as_str()));
        problems.push(format!(
            "{path}: held ({}): the importer refuses and every selection that covers it fails \
             until the hold is resolved (`mie store resolve`, #194; ADR-062 D11)",
            reason.as_str()
        ));
        if state.hold_reason(path).is_none() {
            hold_records.push(Kind::Hold {
                path: path.clone(),
                reason: *reason,
            });
        }
    }
    let mut start_holds = BTreeSet::new();
    for record in &start_ledger.records {
        if let Kind::Hold { path, .. } = &record.kind {
            start_holds.insert(path.clone());
        }
    }
    for path in start_holds {
        if let Some(reason) = state.hold_reason(&path)
            && !findings.holds.contains_key(&path)
        {
            problems.push(format!(
                "{path}: the start copy holds it ({}) and the sources no longer derive that \
                 hold; it stays held, since no record clears a hold before #194 (AW12) (ADR-062 \
                 D11)",
                reason.as_str()
            ));
        }
    }
    for path in &findings.exempt {
        lines.push(format!(
            "{path}: not held: the import PENDING names resumes it (ADR-034 D4)"
        ));
    }
    problems.extend(findings.problems.iter().cloned());

    // B8: removed.
    let partitions: BTreeSet<&str> = inputs
        .roots
        .iter()
        .flat_map(|root| &root.files)
        .filter(|(_, copy)| {
            matches!(
                copy,
                RootCopy::Text {
                    data: DataCheck::Good,
                    ..
                }
            )
        })
        .map(|(path, _)| partition_of(path))
        .collect();
    let mut removed = Vec::new();
    for partition in partitions {
        if inputs.hot.partitions.contains(partition) || state.is_removed(partition) {
            continue;
        }
        if date_of(partition) >= inputs.first_hot_date.as_str() {
            problems.push(format!(
                "{partition}: verified cold copies and no directory on hot, but it is dated inside \
                 the hot window, which starts on {}: the hot removal never removes such a \
                 partition, so this is hot damage. It is recorded removed, so a writer recalls it \
                 before it appends; restart a running `mie ingest` so its writer numbers parts \
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
    let counted: Vec<String> = ["manifest", "copy", "lost", "removed", "hold"]
        .iter()
        .map(|kind| format!("{kind} {}", counts.get(kind).copied().unwrap_or(0)))
        .collect();
    // After the start, the rejected copies and the journal line.
    lines.insert(
        2 + rejected.len(),
        format!("records added: {}", counted.join(", ")),
    );
    Ok(Derived {
        start,
        rejected,
        ledger: builder.finish(),
        added,
        lost,
        holds,
        lines,
        problems,
    })
}

// ------------------------------------------------------------- the run

/// A cold root that presented its marker.
struct Marked {
    path: PathBuf,
    id: String,
    copy_target: bool,
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
                "hot's copy ledger is unreadable ({error}); it is linked aside"
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
        match fs::symlink_metadata(ledger_dir) {
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
    let mut hot_read = HotRead::default();
    for path in replica::list_sealed(hot).map_err(|e| listing(hot, e))? {
        let copy = read_copy(hot, &path, false)?;
        hot_read.files.insert(path, copy);
    }
    hot_read.orphan_data = orphan_data(hot).map_err(|e| listing(hot, e))?;
    hot_read.partitions = removal::scan_hot(hot, "")
        .map_err(|e| listing(hot, e))?
        .partitions
        .into_keys()
        .collect();
    let mut roots = Vec::new();
    for root in &marked {
        let mut files = BTreeMap::new();
        for path in replica::list_sealed(&root.path).map_err(|e| listing(&root.path, e))? {
            let copy = read_copy(&root.path, &path, true)?;
            files.insert(path, copy);
        }
        roots.push(RootRead {
            id: root.id.clone(),
            copy_target: root.copy_target,
            files,
            orphan_data: orphan_data(&root.path).map_err(|e| listing(&root.path, e))?,
        });
    }
    let inputs = RebuildInputs {
        at_ms,
        first_hot_date: first_hot_date.to_owned(),
        record_lost: flags.record_lost,
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
    if derived.lost.iter().any(|file| !file.recorded) {
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
fn marker_gone(root: &Marked) -> Option<String> {
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
fn move_copy_aside(hot: &Path, target: &Marked, aside: &Path, report: &mut RebuildReport) {
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
fn read_journal(path: &Path) -> std::io::Result<JournalRead> {
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
fn read_copy(root: &Path, path: &str, check: bool) -> Result<RootCopy, Stop> {
    let manifest = format!("{}.manifest", path.strip_suffix(".parquet").unwrap_or(path));
    let file = root.join(&manifest);
    let bytes = fs::read(&file).map_err(|error| {
        Stop(format!(
            "read {}: {error}; the rebuild stops with nothing written (ADR-062 D7)",
            file.display()
        ))
    })?;
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

/// The data files without their manifest in the layout of `root`.
fn orphan_data(root: &Path) -> Result<BTreeSet<String>, RawStoreError> {
    Ok(replica::list_unsealed(root)?
        .into_iter()
        .filter(|path| {
            path.rsplit_once('/')
                .is_some_and(|(_, name)| !name.starts_with('.') && name.ends_with(".parquet"))
        })
        .collect())
}
