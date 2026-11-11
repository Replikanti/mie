//! The import ledger: one immutable file per imported archive file, plus
//! the `PENDING` marker of the file being imported (ADR-034 D4).
//!
//! A ledger records the archive file's published SHA-256 and the sealed
//! raw-store files its rows went into. A re-run skips a file whose ledger
//! matches the freshly published checksum and whose listed files are all in
//! the store with the same hash; a different published hash is a conflict
//! that is reported, never re-imported. `PENDING` names the one file whose
//! rows are being appended, so a crash is resumed without duplicating
//! records in the immutable store.
//!
//! Format: one `key value` per line in a fixed key order, no wall-clock
//! fields, like the raw-store manifest (ADR-030):
//!
//! ```text
//! mie-archive-import 1
//! archive data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-09-30.zip
//! sha256 <64 hex>
//! stream binance-archive/BTCUSDT/aggTrades
//! period 2026-09-30
//! header agg_trade_id,price,…      (or `header -` for a headerless file)
//! rows 1434005
//! file <relative path> <rows> <sha256>   (zero or more, sorted by path)
//! ```
//!
//! A ledger lives at `<ledger_dir>/<raw stream>/<archive file name>.import`
//! and is written through a temp file, fsync and rename, then marked
//! read-only.
//!
//! **Generations** (ADR-062 D11, proposed; #210 G1–G5). An entry is never
//! edited; a later one can supersede it. With F the archive file name, the
//! files of F in its raw stream's directory are:
//!
//! - `F.import`, the entry of generation 0 (every entry written before
//!   #210 keeps this name and its bytes);
//! - `F.<n>.import`, the entry of generation n;
//! - `F.<n>.supersede.import`, the supersede record of generation n.
//!
//! n is a canonical decimal from 1 to [`u32::MAX`]. Any other name that
//! ends in `.import` and does not start with `.` is malformed, an error
//! that names it, never skipped. Every name keeps the `.import` suffix, so
//! the copy ledger's `entry:<segment>/<name>.import` subject and its
//! `replaced` record accept it unchanged.
//!
//! A supersede record (format below) names the archive file and every
//! (entry, SHA-256) it supersedes, plus the quarantine id and the moved
//! paths of a replace (`quarantine -` for a re-admit). From generation 1
//! on, an entry and every record must name their own archive file and
//! directory (G3). **The current entry** of F is the entry of the highest
//! generation g ≥ m, with m the highest supersede generation (0 without
//! one); without such an entry F reads as not imported, and every other
//! entry is superseded (G4, [`current_generation`]). Supersession goes by
//! generation, never by the record's `entry` lines, which are the evidence
//! of what was superseded.
//!
//! ```text
//! mie-archive-supersede 1
//! archive data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-10-03.zip
//! stream binance-archive/BTCUSDT/metrics
//! period 2026-10-03
//! entry metrics/BTCUSDT-metrics-2026-10-03.zip.import <64 hex>   (zero or more, sorted)
//! quarantine -              (or `quarantine <id>` and one or more sorted `moved <path>`)
//! ```

use super::catalog::{ArchiveStream, Period};
use mie_ports::raw::{RawStreamKey, is_sha256_hex, validate_segment};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const LEDGER_MAGIC: &str = "mie-archive-import 1";
const PENDING_MAGIC: &str = "mie-archive-pending 1";
const SUPERSEDE_MAGIC: &str = "mie-archive-supersede 1";
/// Every import-ledger file name ends with it (G1).
const SUFFIX: &str = ".import";

/// The name of the pending marker in the ledger directory.
pub const PENDING_FILE: &str = "PENDING";

/// Why a ledger or the pending marker could not be read or written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerError(pub String);

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "import ledger: {}", self.0)
    }
}

impl std::error::Error for LedgerError {}

/// One sealed raw-store file a ledger lists.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LedgerFile {
    /// Path relative to the store root, as in the manifest.
    pub relative_path: String,
    /// Records in the file.
    pub rows: u64,
    /// The file's SHA-256.
    pub sha256: String,
}

/// The record of one imported archive file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportLedger {
    /// The archive path below the base URL.
    pub archive: String,
    /// The published SHA-256 of the zip.
    pub sha256: String,
    /// The raw stream the rows went into.
    pub stream: RawStreamKey,
    /// The archive file's period.
    pub period: Period,
    /// The verbatim header line; `None` for a headerless file.
    pub header: Option<String>,
    /// Data rows in the file, every one stored.
    pub rows: u64,
    /// The sealed files holding the rows, sorted by path.
    pub files: Vec<LedgerFile>,
}

/// Splits `line` into its key and value, checking the key.
fn value<'a>(line: Option<&'a str>, key: &str) -> Result<&'a str, LedgerError> {
    let line = line.ok_or_else(|| LedgerError(format!("missing key {key:?}")))?;
    match line.split_once(' ') {
        Some((found, rest)) if found == key => Ok(rest),
        _ => Err(LedgerError(format!("expected key {key:?}, found {line:?}"))),
    }
}

fn digest(text: &str) -> Result<String, LedgerError> {
    if is_sha256_hex(text) {
        Ok(text.to_owned())
    } else {
        Err(LedgerError(format!("{text:?} is not a SHA-256")))
    }
}

fn stream_key(text: &str) -> Result<RawStreamKey, LedgerError> {
    let parts: Vec<&str> = text.split('/').collect();
    match parts.as_slice() {
        [source, instrument, stream] => RawStreamKey::new(source, instrument, stream)
            .map_err(|e| LedgerError(format!("stream {text:?}: {e}"))),
        _ => Err(LedgerError(format!(
            "stream {text:?} is not source/instrument/stream"
        ))),
    }
}

fn period(text: &str) -> Result<Period, LedgerError> {
    Period::parse(text).ok_or_else(|| LedgerError(format!("period {text:?}")))
}

fn count(text: &str) -> Result<u64, LedgerError> {
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        text.parse()
            .map_err(|_| LedgerError(format!("count {text:?}")))
    } else {
        Err(LedgerError(format!("count {text:?}")))
    }
}

impl ImportLedger {
    /// The ledger's text.
    pub fn render(&self) -> String {
        let mut out = format!(
            "{LEDGER_MAGIC}\narchive {}\nsha256 {}\nstream {}\nperiod {}\nheader {}\nrows {}\n",
            self.archive,
            self.sha256,
            self.stream,
            self.period,
            self.header.as_deref().unwrap_or("-"),
            self.rows
        );
        for file in &self.files {
            out.push_str(&format!(
                "file {} {} {}\n",
                file.relative_path, file.rows, file.sha256
            ));
        }
        out
    }

    /// Parses a ledger's text, accepting only what [`render`](Self::render)
    /// writes: every key once, in order, files sorted by path.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] for unknown, missing or reordered keys and malformed
    /// values.
    pub fn parse(text: &str) -> Result<Self, LedgerError> {
        let body = text
            .strip_suffix('\n')
            .ok_or_else(|| LedgerError("missing final newline".to_owned()))?;
        let mut lines = body.split('\n');
        if lines.next() != Some(LEDGER_MAGIC) {
            return Err(LedgerError(format!("first line is not {LEDGER_MAGIC:?}")));
        }
        let archive = value(lines.next(), "archive")?.to_owned();
        let sha256 = digest(value(lines.next(), "sha256")?)?;
        let stream = stream_key(value(lines.next(), "stream")?)?;
        let period = period(value(lines.next(), "period")?)?;
        let header = match value(lines.next(), "header")? {
            "-" => None,
            line => Some(line.to_owned()),
        };
        let rows = count(value(lines.next(), "rows")?)?;
        let mut files: Vec<LedgerFile> = Vec::new();
        for line in lines {
            let fields: Vec<&str> = value(Some(line), "file")?.split(' ').collect();
            let [path, rows, sha] = fields.as_slice() else {
                return Err(LedgerError(format!("file line {line:?}")));
            };
            let file = LedgerFile {
                relative_path: (*path).to_owned(),
                rows: count(rows)?,
                sha256: digest(sha)?,
            };
            if files
                .last()
                .is_some_and(|last| last.relative_path >= file.relative_path)
            {
                return Err(LedgerError(format!(
                    "file {} is out of order or repeated",
                    file.relative_path
                )));
            }
            files.push(file);
        }
        if archive.is_empty() || archive.contains(' ') {
            return Err(LedgerError(format!("archive {archive:?}")));
        }
        Ok(Self {
            archive,
            sha256,
            stream,
            period,
            header,
            rows,
            files,
        })
    }
}

/// The archive file whose rows are being appended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pending {
    /// The archive path below the base URL.
    pub archive: String,
    /// The published SHA-256 the import started from.
    pub sha256: String,
    /// The raw stream the rows go into.
    pub stream: RawStreamKey,
    /// The archive file's period.
    pub period: Period,
}

impl Pending {
    /// The marker's text.
    pub fn render(&self) -> String {
        format!(
            "{PENDING_MAGIC}\narchive {}\nsha256 {}\nstream {}\nperiod {}\n",
            self.archive, self.sha256, self.stream, self.period
        )
    }

    /// Parses a marker written by [`render`](Self::render).
    ///
    /// # Errors
    ///
    /// [`LedgerError`] for any other text.
    pub fn parse(text: &str) -> Result<Self, LedgerError> {
        let body = text
            .strip_suffix('\n')
            .ok_or_else(|| LedgerError("PENDING: missing final newline".to_owned()))?;
        let mut lines = body.split('\n');
        if lines.next() != Some(PENDING_MAGIC) {
            return Err(LedgerError(format!(
                "PENDING: first line is not {PENDING_MAGIC:?}"
            )));
        }
        let pending = Self {
            archive: value(lines.next(), "archive")?.to_owned(),
            sha256: digest(value(lines.next(), "sha256")?)?,
            stream: stream_key(value(lines.next(), "stream")?)?,
            period: period(value(lines.next(), "period")?)?,
        };
        match lines.next() {
            None => Ok(pending),
            Some(extra) => Err(LedgerError(format!("PENDING: unexpected line {extra:?}"))),
        }
    }
}

/// What an import-ledger file is (G1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LedgerKind {
    /// An import entry ([`ImportLedger`]).
    Entry,
    /// A supersede record ([`SupersedeRecord`]).
    Supersede,
}

/// The name of one import-ledger file (G1): its archive file, generation
/// and kind.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LedgerName {
    /// The archive file name F, ending in `.zip`.
    pub file: String,
    /// The generation: 0 only for an entry named `F.import`.
    pub generation: u32,
    /// Entry or supersede record.
    pub kind: LedgerKind,
}

/// Whether `file` can be an archive file name: `[A-Za-z0-9._-]`, ending in
/// `.zip` after at least one byte, not starting with `.`.
fn archive_file_name(file: &str) -> bool {
    file.strip_suffix(".zip")
        .is_some_and(|stem| !stem.is_empty())
        && !file.starts_with('.')
        && file
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// A canonical decimal generation from 1 to [`u32::MAX`].
fn generation(text: &str) -> Option<u32> {
    let canonical =
        !text.is_empty() && !text.starts_with('0') && text.bytes().all(|b| b.is_ascii_digit());
    canonical.then(|| text.parse().ok()).flatten()
}

impl LedgerName {
    /// The entry of `file` at `generation`.
    pub fn entry(file: &str, generation: u32) -> Self {
        Self {
            file: file.to_owned(),
            generation,
            kind: LedgerKind::Entry,
        }
    }

    /// The supersede record of `file` at `generation` (at least 1).
    pub fn supersede(file: &str, generation: u32) -> Self {
        Self {
            file: file.to_owned(),
            generation,
            kind: LedgerKind::Supersede,
        }
    }

    /// Parses a file name of an import-ledger directory (G1).
    ///
    /// # Errors
    ///
    /// [`LedgerError`] naming any other name.
    pub fn parse(name: &str) -> Result<Self, LedgerError> {
        let malformed = || {
            LedgerError(format!(
                "{name:?} is no import-ledger name: F.import, F.<n>.import or \
                 F.<n>.supersede.import, F an archive file name and n from 1 to {}",
                u32::MAX
            ))
        };
        let rest = name.strip_suffix(SUFFIX).ok_or_else(malformed)?;
        if archive_file_name(rest) {
            return Ok(Self::entry(rest, 0));
        }
        let (inner, kind) = match rest.strip_suffix(".supersede") {
            Some(inner) => (inner, LedgerKind::Supersede),
            None => (rest, LedgerKind::Entry),
        };
        let (file, number) = inner.rsplit_once('.').ok_or_else(malformed)?;
        let generation = generation(number).ok_or_else(malformed)?;
        if !archive_file_name(file) {
            return Err(malformed());
        }
        Ok(Self {
            file: file.to_owned(),
            generation,
            kind,
        })
    }

    /// The file name: `F.import`, `F.<n>.import` or `F.<n>.supersede.import`.
    pub fn render(&self) -> String {
        match (self.kind, self.generation) {
            (LedgerKind::Entry, 0) => format!("{}{SUFFIX}", self.file),
            (LedgerKind::Entry, n) => format!("{}.{n}{SUFFIX}", self.file),
            (LedgerKind::Supersede, n) => format!("{}.{n}.supersede{SUFFIX}", self.file),
        }
    }
}

/// The current entry's generation among the files of one archive file
/// (G4): with m the highest supersede generation (0 without one), the
/// highest entry generation g ≥ m; `None` when no entry reaches m, and F
/// reads as not imported.
pub fn current_generation(files: impl IntoIterator<Item = (u32, LedgerKind)>) -> Option<u32> {
    let files: Vec<(u32, LedgerKind)> = files.into_iter().collect();
    let m = files
        .iter()
        .filter(|(_, kind)| *kind == LedgerKind::Supersede)
        .map(|(generation, _)| *generation)
        .max()
        .unwrap_or(0);
    files
        .iter()
        .filter(|(generation, kind)| *kind == LedgerKind::Entry && *generation >= m)
        .map(|(generation, _)| *generation)
        .max()
}

/// The generation of the entry that replaces the quarantine of the
/// supersede record of generation `record` (ADR-062 D11, #211 I3): the
/// lowest entry generation g ≥ `record` among `files`. Not the current
/// generation: a later record supersedes that entry in turn, and the
/// paths `record` moved were replaced by the first entry written after it.
/// `None` while no such entry exists.
pub fn replacement_generation(
    record: u32,
    files: impl IntoIterator<Item = (u32, LedgerKind)>,
) -> Option<u32> {
    files
        .into_iter()
        .filter(|(generation, kind)| *kind == LedgerKind::Entry && *generation >= record)
        .map(|(generation, _)| generation)
        .min()
}

/// One superseded entry a supersede record names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SupersededEntry {
    /// `<raw stream>/<entry file name>`.
    pub name: String,
    /// The SHA-256 of that file's bytes.
    pub sha256: String,
}

impl SupersededEntry {
    /// The superseded entry `name` (`<raw stream>/<entry file name>`) with
    /// the SHA-256 of its `bytes`.
    pub fn of(name: String, bytes: &[u8]) -> Self {
        use sha2::{Digest, Sha256};
        Self {
            name,
            sha256: super::fetch::hex(&Sha256::digest(bytes)),
        }
    }
}

/// The quarantine of a replace (T8b, #211).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quarantine {
    /// The quarantine id, a valid path segment.
    pub id: String,
    /// The moved relative paths, sorted, at least one.
    pub moved: Vec<String>,
}

/// A supersede record (G2): immutable, written once, never replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupersedeRecord {
    /// The archive path below the base URL.
    pub archive: String,
    /// The raw stream.
    pub stream: RawStreamKey,
    /// The archive file's period.
    pub period: Period,
    /// Every distinct (entry, SHA-256) superseded, sorted; empty when the
    /// file had no entry.
    pub entries: Vec<SupersededEntry>,
    /// `None` for `quarantine -` (a re-admit).
    pub quarantine: Option<Quarantine>,
}

/// The last `/` segment of `text`.
fn last_segment(text: &str) -> &str {
    text.rsplit('/').next().unwrap_or(text)
}

impl SupersedeRecord {
    /// The record's text.
    pub fn render(&self) -> String {
        let mut out = format!(
            "{SUPERSEDE_MAGIC}\narchive {}\nstream {}\nperiod {}\n",
            self.archive, self.stream, self.period
        );
        for entry in &self.entries {
            out.push_str(&format!("entry {} {}\n", entry.name, entry.sha256));
        }
        match &self.quarantine {
            None => out.push_str("quarantine -\n"),
            Some(quarantine) => {
                out.push_str(&format!("quarantine {}\n", quarantine.id));
                for path in &quarantine.moved {
                    out.push_str(&format!("moved {path}\n"));
                }
            }
        }
        out
    }

    /// Parses a record, accepting only what [`render`](Self::render)
    /// writes: printable ASCII, every key in order, `entry` lines sorted,
    /// unique and naming entries of this record's archive file and stream,
    /// `moved` lines exactly with a quarantine id.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] for any other text.
    pub fn parse(text: &str) -> Result<Self, LedgerError> {
        let body = text
            .strip_suffix('\n')
            .ok_or_else(|| LedgerError("supersede record: missing final newline".to_owned()))?;
        if let Some(byte) = body
            .bytes()
            .find(|&b| b != b'\n' && !(b' '..=b'~').contains(&b))
        {
            return Err(LedgerError(format!(
                "supersede record: byte 0x{byte:02x} is not printable ASCII"
            )));
        }
        let mut lines = body.split('\n').peekable();
        if lines.next() != Some(SUPERSEDE_MAGIC) {
            return Err(LedgerError(format!(
                "supersede record: first line is not {SUPERSEDE_MAGIC:?}"
            )));
        }
        let archive = value(lines.next(), "archive")?.to_owned();
        if archive.is_empty() || archive.contains(' ') {
            return Err(LedgerError(format!(
                "supersede record: archive {archive:?}"
            )));
        }
        let stream = stream_key(value(lines.next(), "stream")?)?;
        let period = period(value(lines.next(), "period")?)?;
        let file = last_segment(&archive).to_owned();
        let mut entries: Vec<SupersededEntry> = Vec::new();
        while let Some(line) = lines.next_if(|line| line.starts_with("entry ")) {
            let fields: Vec<&str> = value(Some(line), "entry")?.split(' ').collect();
            let [name, sha] = fields.as_slice() else {
                return Err(LedgerError(format!(
                    "supersede record: entry line {line:?}"
                )));
            };
            let names_own_entry = name.split_once('/').is_some_and(|(segment, entry)| {
                segment == stream.stream()
                    && LedgerName::parse(entry)
                        .is_ok_and(|n| n.kind == LedgerKind::Entry && n.file == file)
            });
            if !names_own_entry {
                return Err(LedgerError(format!(
                    "supersede record: {name:?} is no entry of {file} in {}",
                    stream.stream()
                )));
            }
            let entry = SupersededEntry {
                name: (*name).to_owned(),
                sha256: digest(sha)?,
            };
            if entries.last().is_some_and(|last| *last >= entry) {
                return Err(LedgerError(format!(
                    "supersede record: entry {} {} is out of order or repeated",
                    entry.name, entry.sha256
                )));
            }
            entries.push(entry);
        }
        let quarantine = match value(lines.next(), "quarantine")? {
            "-" => None,
            id => {
                validate_segment(id)
                    .map_err(|e| LedgerError(format!("supersede record: quarantine: {e}")))?;
                let mut moved: Vec<String> = Vec::new();
                while let Some(line) = lines.next_if(|line| line.starts_with("moved ")) {
                    let path = value(Some(line), "moved")?;
                    let clean = !path.is_empty()
                        && !path.contains(' ')
                        && path
                            .split('/')
                            .all(|s| !s.is_empty() && s != "." && s != "..");
                    if !clean {
                        return Err(LedgerError(format!(
                            "supersede record: moved path {path:?}"
                        )));
                    }
                    if moved.last().is_some_and(|last| last.as_str() >= path) {
                        return Err(LedgerError(format!(
                            "supersede record: moved {path} is out of order or repeated"
                        )));
                    }
                    moved.push(path.to_owned());
                }
                if moved.is_empty() {
                    return Err(LedgerError(format!(
                        "supersede record: quarantine {id} moved nothing"
                    )));
                }
                Some(Quarantine {
                    id: id.to_owned(),
                    moved,
                })
            }
        };
        if let Some(extra) = lines.next() {
            return Err(LedgerError(format!(
                "supersede record: unexpected line {extra:?}"
            )));
        }
        Ok(Self {
            archive,
            stream,
            period,
            entries,
            quarantine,
        })
    }
}

/// G3: a file of generation ≥ 1, and every record, names its own archive
/// file and directory.
fn check_binding(
    segment: &str,
    name: &LedgerName,
    archive: &str,
    stream: &RawStreamKey,
) -> Result<(), LedgerError> {
    if name.generation == 0 && name.kind == LedgerKind::Entry {
        return Ok(());
    }
    if last_segment(archive) != name.file || stream.stream() != segment {
        return Err(LedgerError(format!(
            "{segment}/{}: it names archive {archive} of {stream}, not {} of {segment}",
            name.render(),
            name.file
        )));
    }
    Ok(())
}

/// One import-ledger file of an archive file, read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerFileOf {
    /// Its name.
    pub name: LedgerName,
    /// Its path.
    pub path: PathBuf,
    /// Its bytes.
    pub bytes: Vec<u8>,
}

/// Every file of one archive file F, parsed and classified (G3, G4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classified {
    /// The current entry: its name and text.
    pub current: Option<(LedgerName, ImportLedger)>,
    /// Every supersede record, in generation order.
    pub records: Vec<(LedgerName, SupersedeRecord)>,
    /// Every superseded entry, in generation order.
    pub superseded: Vec<LedgerName>,
}

/// Parses and classifies the files of one archive file in the directory
/// `segment` (G2–G4): every file must parse, and from generation 1 on name
/// its own archive file and directory. Fail closed: any failure is an
/// error naming the file, and nothing of F counts.
///
/// # Errors
///
/// [`LedgerError`] naming the first file that fails.
pub fn classify(segment: &str, files: &[(LedgerName, Vec<u8>)]) -> Result<Classified, LedgerError> {
    let mut entries: BTreeMap<u32, (LedgerName, ImportLedger)> = BTreeMap::new();
    let mut records: Vec<(LedgerName, SupersedeRecord)> = Vec::new();
    for (name, bytes) in files {
        let named = |e: LedgerError| LedgerError(format!("{segment}/{}: {}", name.render(), e.0));
        let text =
            std::str::from_utf8(bytes).map_err(|_| named(LedgerError("not UTF-8".to_owned())))?;
        match name.kind {
            LedgerKind::Entry => {
                let ledger = ImportLedger::parse(text).map_err(named)?;
                check_binding(segment, name, &ledger.archive, &ledger.stream)?;
                entries.insert(name.generation, (name.clone(), ledger));
            }
            LedgerKind::Supersede => {
                let record = SupersedeRecord::parse(text).map_err(named)?;
                check_binding(segment, name, &record.archive, &record.stream)?;
                records.push((name.clone(), record));
            }
        }
    }
    records.sort_by_key(|(name, _)| name.generation);
    let current = current_generation(files.iter().map(|(name, _)| (name.generation, name.kind)));
    let mut superseded = Vec::new();
    let mut found = None;
    for (generation, entry) in entries {
        if Some(generation) == current {
            found = Some(entry);
        } else {
            superseded.push(entry.0);
        }
    }
    Ok(Classified {
        current: found,
        records,
        superseded,
    })
}

/// One supersede record found by [`LedgerDir::supersede_records`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupersedeOf {
    /// Its raw stream's directory.
    pub stream: ArchiveStream,
    /// Its name.
    pub name: LedgerName,
    /// The record, or why it cannot be read, parsed or bound (G2, G3).
    pub record: Result<SupersedeRecord, LedgerError>,
}

/// The ledger directory: ledgers per raw stream and the pending marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerDir {
    root: PathBuf,
}

fn io(action: &str, path: &Path) -> impl FnOnce(std::io::Error) -> LedgerError {
    let context = format!("{action} {}", path.display());
    move |e| LedgerError(format!("{context}: {e}"))
}

/// Writes `text` to `path` durably: temp file, fsync, rename, directory
/// fsync, read-only.
fn write_durably(path: &Path, text: &str) -> Result<(), LedgerError> {
    let dir = path
        .parent()
        .ok_or_else(|| LedgerError(format!("{} has no directory", path.display())))?;
    fs::create_dir_all(dir).map_err(io("create", dir))?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| LedgerError(format!("{} has no file name", path.display())))?;
    let tmp = dir.join(format!(".{name}.tmp"));
    let _ = fs::remove_file(&tmp);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .map_err(io("create", &tmp))?;
    file.write_all(text.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(io("write", &tmp))?;
    let mut permissions = file.metadata().map_err(io("stat", &tmp))?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&tmp, permissions).map_err(io("protect", &tmp))?;
    fs::rename(&tmp, path).map_err(io("rename", &tmp))?;
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(io("sync", dir))
}

impl LedgerDir {
    /// The ledger directory at `root` (created on first write).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Where the generation-0 entry of `file_name` of `stream` lives.
    pub fn ledger_path(&self, stream: ArchiveStream, file_name: &str) -> PathBuf {
        self.root
            .join(stream.raw_name())
            .join(format!("{file_name}{SUFFIX}"))
    }

    /// Every import-ledger name in `stream`'s directory, sorted; hidden
    /// temp files and other names are skipped, as `PENDING` is.
    fn names(&self, stream: ArchiveStream) -> Result<Vec<LedgerName>, LedgerError> {
        let dir = self.root.join(stream.raw_name());
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io("list", &dir)(e)),
        };
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(io("list", &dir))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') || !name.ends_with(SUFFIX) {
                continue;
            }
            let parsed = LedgerName::parse(&name).map_err(|e| {
                LedgerError(format!("{}: {}", dir.join(name.as_ref()).display(), e.0))
            })?;
            names.push(parsed);
        }
        names.sort();
        Ok(names)
    }

    /// Reads the files `names` of `stream`.
    fn read_files(
        &self,
        stream: ArchiveStream,
        names: Vec<LedgerName>,
    ) -> Result<Vec<LedgerFileOf>, LedgerError> {
        let dir = self.root.join(stream.raw_name());
        let mut files = Vec::new();
        for name in names {
            let path = dir.join(name.render());
            let bytes = fs::read(&path).map_err(io("read", &path))?;
            files.push(LedgerFileOf { name, path, bytes });
        }
        Ok(files)
    }

    /// Every file of the archive file `file_name` of `stream`, with its
    /// generation, kind and bytes, in generation order (an entry before the
    /// record of its generation).
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when the directory cannot be listed, a name in it is
    /// malformed or a file cannot be read.
    pub fn files(
        &self,
        stream: ArchiveStream,
        file_name: &str,
    ) -> Result<Vec<LedgerFileOf>, LedgerError> {
        let names = self
            .names(stream)?
            .into_iter()
            .filter(|name| name.file == file_name)
            .collect();
        self.read_files(stream, names)
    }

    /// Classifies `files` of `stream`.
    fn classify(
        &self,
        stream: ArchiveStream,
        files: Vec<LedgerFileOf>,
    ) -> Result<Classified, LedgerError> {
        let pairs: Vec<(LedgerName, Vec<u8>)> =
            files.into_iter().map(|f| (f.name, f.bytes)).collect();
        classify(stream.raw_name(), &pairs).map_err(|e| {
            LedgerError(format!(
                "{}: {}",
                self.root.join(stream.raw_name()).display(),
                e.0
            ))
        })
    }

    /// Reads the current entry of `file_name` (G4), if there is one. It
    /// lists the stream's directory once.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when a file of `file_name` cannot be read or parsed,
    /// a name in the directory is malformed, or a file of generation ≥ 1
    /// names another archive file or directory (G1, G3).
    pub fn read(
        &self,
        stream: ArchiveStream,
        file_name: &str,
    ) -> Result<Option<ImportLedger>, LedgerError> {
        let files = self.files(stream, file_name)?;
        Ok(self
            .classify(stream, files)?
            .current
            .map(|(_, ledger)| ledger))
    }

    /// Writes the entry of `file_name` at the supersede generation m (G5):
    /// `F.import` without a record, `F.<m>.import` after one. An entry is
    /// written once: one of generation ≥ m is never replaced.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when such an entry exists already, the entry names
    /// another archive file (generation ≥ 1), or the write fails.
    pub fn write(
        &self,
        stream: ArchiveStream,
        file_name: &str,
        ledger: &ImportLedger,
    ) -> Result<(), LedgerError> {
        let names: Vec<LedgerName> = self
            .names(stream)?
            .into_iter()
            .filter(|name| name.file == file_name)
            .collect();
        let m = names
            .iter()
            .filter(|name| name.kind == LedgerKind::Supersede)
            .map(|name| name.generation)
            .max()
            .unwrap_or(0);
        let name = LedgerName::entry(file_name, m);
        let path = self.root.join(stream.raw_name()).join(name.render());
        if let Some(existing) = names
            .iter()
            .find(|n| n.kind == LedgerKind::Entry && n.generation >= m)
        {
            return Err(LedgerError(format!(
                "{} exists; a ledger is never replaced",
                self.root
                    .join(stream.raw_name())
                    .join(existing.render())
                    .display()
            )));
        }
        check_binding(stream.raw_name(), &name, &ledger.archive, &ledger.stream)?;
        if path.exists() {
            return Err(LedgerError(format!(
                "{} exists; a ledger is never replaced",
                path.display()
            )));
        }
        write_durably(&path, &ledger.render())
    }

    /// Writes the supersede record of generation `generation` of
    /// `file_name` (G5). Like an entry it is written once.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when `generation` is 0 or not above every generation
    /// of `file_name` on this directory, the name exists, the record names
    /// another archive file or directory (G3), or the write fails.
    pub fn write_supersede(
        &self,
        stream: ArchiveStream,
        file_name: &str,
        generation: u32,
        record: &SupersedeRecord,
    ) -> Result<(), LedgerError> {
        let name = LedgerName::supersede(file_name, generation);
        let path = self.root.join(stream.raw_name()).join(name.render());
        let highest = self
            .names(stream)?
            .into_iter()
            .filter(|n| n.file == file_name)
            .map(|n| n.generation)
            .max();
        if generation == 0 || highest.is_some_and(|highest| generation <= highest) {
            return Err(LedgerError(format!(
                "{}: generation {generation} is not above every generation of {file_name} \
                 ({highest:?}); a supersede record is never replaced",
                path.display()
            )));
        }
        check_binding(stream.raw_name(), &name, &record.archive, &record.stream)?;
        if path.exists() {
            return Err(LedgerError(format!(
                "{} exists; a supersede record is never replaced",
                path.display()
            )));
        }
        write_durably(&path, &record.render())
    }

    /// The current entry of every archive file of `stream` (G4), sorted by
    /// file name.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when the directory cannot be listed, a name is
    /// malformed or a file cannot be read, parsed or bound (G1–G3).
    pub fn list(&self, stream: ArchiveStream) -> Result<Vec<ImportLedger>, LedgerError> {
        let mut by_file: BTreeMap<String, Vec<LedgerName>> = BTreeMap::new();
        for name in self.names(stream)? {
            by_file.entry(name.file.clone()).or_default().push(name);
        }
        let mut ledgers = Vec::new();
        for names in by_file.into_values() {
            let files = self.read_files(stream, names)?;
            if let Some((_, ledger)) = self.classify(stream, files)?.current {
                ledgers.push(ledger);
            }
        }
        Ok(ledgers)
    }

    /// Every supersede record of every archive stream, each stream's
    /// directory listed once, in stream and name order (#211). A record
    /// that cannot be read, parsed or bound is returned as its error, so
    /// each caller decides whether it fails closed or names it. Names that
    /// do not parse are skipped: they are no record, and the readers of
    /// their archive file refuse them (G1).
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when a directory cannot be listed.
    pub fn supersede_records(&self) -> Result<Vec<SupersedeOf>, LedgerError> {
        let mut found = Vec::new();
        for stream in ArchiveStream::ALL {
            let dir = self.root.join(stream.raw_name());
            let entries = match fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io("list", &dir)(e)),
            };
            let mut names = Vec::new();
            for entry in entries {
                let entry = entry.map_err(io("list", &dir))?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with('.') || !name.ends_with(SUFFIX) {
                    continue;
                }
                if let Ok(parsed) = LedgerName::parse(&name)
                    && parsed.kind == LedgerKind::Supersede
                {
                    names.push(parsed);
                }
            }
            names.sort();
            for name in names {
                let path = dir.join(name.render());
                let record = fs::read(&path)
                    .map_err(io("read", &path))
                    .and_then(|bytes| {
                        String::from_utf8(bytes)
                            .map_err(|_| LedgerError(format!("{}: not UTF-8", path.display())))
                    })
                    .and_then(|text| {
                        SupersedeRecord::parse(&text)
                            .map_err(|e| LedgerError(format!("{}: {}", path.display(), e.0)))
                    })
                    .and_then(|record| {
                        check_binding(stream.raw_name(), &name, &record.archive, &record.stream)
                            .map(|()| record)
                    });
                found.push(SupersedeOf {
                    stream,
                    name,
                    record,
                });
            }
        }
        Ok(found)
    }

    fn pending_path(&self) -> PathBuf {
        self.root.join(PENDING_FILE)
    }

    /// The pending marker, if one exists.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when it exists but cannot be read or parsed.
    pub fn pending(&self) -> Result<Option<Pending>, LedgerError> {
        let path = self.pending_path();
        match fs::read_to_string(&path) {
            Ok(text) => Pending::parse(&text).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io("read", &path)(e)),
        }
    }

    /// Writes the pending marker durably.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when one exists already or the write fails.
    pub fn set_pending(&self, pending: &Pending) -> Result<(), LedgerError> {
        let path = self.pending_path();
        if path.exists() {
            return Err(LedgerError(format!(
                "{} exists; resolve it first",
                path.display()
            )));
        }
        write_durably(&path, &pending.render())
    }

    /// Removes the pending marker durably.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when the removal fails.
    pub fn clear_pending(&self) -> Result<(), LedgerError> {
        let path = self.pending_path();
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(io("remove", &path)(e)),
        }
        File::open(&self.root)
            .and_then(|d| d.sync_all())
            .map_err(io("sync", &self.root))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::catalog::parse_day;

    fn sample() -> ImportLedger {
        ImportLedger {
            archive: "data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-09-30.zip"
                .to_owned(),
            sha256: "62cd39b1b90be376c56c7e24759a8ee3e2ea5d818d098a762cfa7e4fe0c56e1c".to_owned(),
            stream: RawStreamKey::new("binance-archive", "BTCUSDT", "aggTrades").unwrap(),
            period: Period::Day(parse_day("2026-09-30").unwrap()),
            header: Some(ArchiveStream::AggTrades.expected_header().to_owned()),
            rows: 1_434_005,
            files: vec![
                LedgerFile {
                    relative_path: "source=binance-archive/instrument=BTCUSDT/stream=aggTrades/date=2026-09-30/part-00000.parquet".to_owned(),
                    rows: 1_000_000,
                    sha256: "a".repeat(64),
                },
                LedgerFile {
                    relative_path: "source=binance-archive/instrument=BTCUSDT/stream=aggTrades/date=2026-09-30/part-00001.parquet".to_owned(),
                    rows: 434_005,
                    sha256: "b".repeat(64),
                },
            ],
        }
    }

    #[test]
    fn ledgers_round_trip() {
        let ledger = sample();
        let text = ledger.render();
        assert!(text.starts_with("mie-archive-import 1\narchive data/futures/um/"));
        assert_eq!(ImportLedger::parse(&text), Ok(ledger.clone()));
        let headerless = ImportLedger {
            header: None,
            files: Vec::new(),
            period: Period::Month {
                year: 2026,
                month: 8,
            },
            ..ledger
        };
        let text = headerless.render();
        assert!(text.contains("\nheader -\n"));
        assert!(text.contains("\nperiod 2026-08\n"));
        assert_eq!(ImportLedger::parse(&text), Ok(headerless));
    }

    #[test]
    fn unknown_reordered_or_unsorted_lines_are_rejected() {
        let text = sample().render();
        let swapped = text.replacen("archive ", "ARCHIVE ", 1);
        let lines: Vec<&str> = text.lines().collect();
        let mut reordered = lines.clone();
        reordered.swap(2, 3);
        let mut unsorted = lines.clone();
        unsorted.swap(7, 8);
        let extra = format!("{text}note hello\n");
        for bad in [
            swapped,
            format!("{}\n", reordered.join("\n")),
            format!("{}\n", unsorted.join("\n")),
            extra,
            text.trim_end().to_owned(),
            text.replace("rows 1434005", "rows -1"),
            text.replace(&"a".repeat(64), &"A".repeat(64)),
        ] {
            assert!(ImportLedger::parse(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn pending_markers_round_trip() {
        let ledger = sample();
        let pending = Pending {
            archive: ledger.archive,
            sha256: ledger.sha256,
            stream: ledger.stream,
            period: ledger.period,
        };
        assert_eq!(Pending::parse(&pending.render()), Ok(pending.clone()));
        assert!(Pending::parse(&format!("{}rows 1\n", pending.render())).is_err());
        assert!(Pending::parse("mie-archive-import 1\n").is_err());
    }
    // ------------------------------------------------- generations (#210)

    const F: &str = "BTCUSDT-metrics-2026-10-03.zip";
    const METRICS_ARCHIVE: &str =
        "data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-10-03.zip";

    /// G1: the three name forms round-trip; every look-alike is malformed.
    #[test]
    fn ledger_names_round_trip_and_reject_lookalikes() {
        for (name, generation, kind) in [
            (format!("{F}.import"), 0, LedgerKind::Entry),
            (format!("{F}.1.import"), 1, LedgerKind::Entry),
            (format!("{F}.1.supersede.import"), 1, LedgerKind::Supersede),
            (
                format!("{F}.4294967295.import"),
                u32::MAX,
                LedgerKind::Entry,
            ),
            ("x.zip.import".to_owned(), 0, LedgerKind::Entry),
        ] {
            let parsed = LedgerName::parse(&name).unwrap();
            assert_eq!(
                (parsed.generation, parsed.kind),
                (generation, kind),
                "{name}"
            );
            assert_eq!(parsed.render(), name);
        }
        assert_eq!(LedgerName::parse(&format!("{F}.7.import")).unwrap().file, F);
        for bad in [
            format!("{F}.0.import"),
            format!("{F}.01.import"),
            format!("{F}.+1.import"),
            format!("{F}.\u{FF11}.import"),
            format!("{F}.1\u{200B}.import"),
            format!("{F}.1 .import"),
            format!("{F}.0.supersede.import"),
            format!("{F}.supersede.import"),
            format!("{F}.1.supersede.2.import"),
            format!("{F}.4294967296.import"),
            format!("{F}..import"),
            "x.ZIP.import".to_owned(),
            format!("{F}.1.Supersede.import"),
            format!(".{F}.import"),
            ".zip.import".to_owned(),
            format!("{F}.1.supersede"),
            format!("{F}.1.IMPORT"),
        ] {
            assert!(LedgerName::parse(&bad).is_err(), "{bad:?}");
        }
    }

    /// G4, every row of the table.
    #[test]
    fn current_entry_follows_the_highest_supersede_generation() {
        use LedgerKind::{Entry as E, Supersede as S};
        for (files, current) in [
            (vec![(0, E)], Some(0)),
            (vec![(0, E), (1, S)], None),
            (vec![(0, E), (1, S), (1, E)], Some(1)),
            (vec![(0, E), (1, E)], Some(1)),
            (vec![(0, E), (1, S), (1, E), (2, S)], None),
            (vec![(0, E), (1, S), (1, E), (2, S), (2, E)], Some(2)),
            (vec![(1, S)], None),
            (vec![(2, E), (1, S)], Some(2)),
            (vec![], None),
        ] {
            assert_eq!(current_generation(files.clone()), current, "{files:?}");
        }
    }

    /// #211 I3: the entry that replaces a quarantine is the lowest entry
    /// generation at or above the record's, never wrapping.
    #[test]
    fn replacement_generation_table() {
        use LedgerKind::{Entry as E, Supersede as S};
        for (files, record, replacement) in [
            (vec![(0, E), (1, S)], 1, None),
            (vec![(0, E), (1, S), (1, E)], 1, Some(1)),
            (vec![(0, E), (1, S), (2, S), (2, E)], 1, Some(2)),
            (vec![(0, E), (1, S), (2, S), (2, E)], 2, Some(2)),
            (vec![(0, E), (1, S), (1, E), (2, S), (2, E)], 1, Some(1)),
            (vec![(0, E), (1, S), (1, E), (2, S), (2, E)], 2, Some(2)),
            (
                vec![(0, E), (u32::MAX, S), (u32::MAX, E)],
                u32::MAX,
                Some(u32::MAX),
            ),
            (vec![], 1, None),
        ] {
            assert_eq!(
                replacement_generation(record, files.clone()),
                replacement,
                "{files:?} S{record}"
            );
        }
    }

    /// #211 RQ4: a record moving paths on two dates renders and parses
    /// back equal, and lists them sorted.
    #[test]
    fn supersede_record_with_moved_paths_round_trips() {
        let moved = vec![
            "source=binance-archive/instrument=BTCUSDT/stream=metrics/date=2026-10-03/part-00001.parquet".to_owned(),
            "source=binance-archive/instrument=BTCUSDT/stream=metrics/date=2026-10-04/part-00000.parquet".to_owned(),
        ];
        let replace = SupersedeRecord {
            entries: Vec::new(),
            quarantine: Some(Quarantine {
                id: "q-1791259200000".to_owned(),
                moved: moved.clone(),
            }),
            ..record()
        };
        let text = replace.render();
        assert_eq!(SupersedeRecord::parse(&text), Ok(replace));
        let swapped = text
            .replace(&moved[0], "X")
            .replace(&moved[1], &moved[0])
            .replace('X', &moved[1]);
        assert!(SupersedeRecord::parse(&swapped).is_err(), "{swapped}");
    }

    /// #211: every record of every stream, a broken one as its error; a
    /// malformed name and an entry are skipped.
    #[test]
    fn supersede_records_lists_every_stream_once() {
        let dir = Dir::new("supersede-records");
        let ledgers = LedgerDir::new(&dir.0);
        let metrics = dir.0.join("metrics");
        fs::create_dir_all(&metrics).unwrap();
        fs::write(
            metrics.join(format!("{F}.1.supersede.import")),
            record().render(),
        )
        .unwrap();
        fs::write(metrics.join(format!("{F}.2.supersede.import")), b"rotten\n").unwrap();
        fs::write(metrics.join(format!("{F}.import")), b"entry\n").unwrap();
        fs::write(metrics.join(format!("{F}.01.import")), b"malformed\n").unwrap();
        let found = ledgers.supersede_records().unwrap();
        assert_eq!(found.len(), 2, "{found:?}");
        assert_eq!(found[0].name, LedgerName::supersede(F, 1));
        assert_eq!(found[0].record, Ok(record()));
        assert_eq!(found[1].name, LedgerName::supersede(F, 2));
        assert!(found[1].record.is_err());
        assert!(found.iter().all(|f| f.stream == ArchiveStream::Metrics));
        assert!(
            LedgerDir::new(dir.0.join("none"))
                .supersede_records()
                .unwrap()
                .is_empty()
        );
    }

    fn metrics_key() -> RawStreamKey {
        RawStreamKey::new("binance-archive", "BTCUSDT", "metrics").unwrap()
    }

    fn record() -> SupersedeRecord {
        SupersedeRecord {
            archive: METRICS_ARCHIVE.to_owned(),
            stream: metrics_key(),
            period: Period::Day(parse_day("2026-10-03").unwrap()),
            entries: vec![
                SupersededEntry {
                    name: format!("metrics/{F}.import"),
                    sha256: "1".repeat(64),
                },
                SupersededEntry {
                    name: format!("metrics/{F}.import"),
                    sha256: "2".repeat(64),
                },
            ],
            quarantine: None,
        }
    }

    /// G2: the golden bytes of T8a's form and of T8b's form.
    #[test]
    fn supersede_record_golden_text() {
        let golden = format!(
            "mie-archive-supersede 1\n\
             archive {METRICS_ARCHIVE}\n\
             stream binance-archive/BTCUSDT/metrics\n\
             period 2026-10-03\n\
             entry metrics/{F}.import {}\n\
             entry metrics/{F}.import {}\n\
             quarantine -\n",
            "1".repeat(64),
            "2".repeat(64)
        );
        assert_eq!(record().render(), golden);
        assert_eq!(SupersedeRecord::parse(&golden), Ok(record()));

        let replace = SupersedeRecord {
            quarantine: Some(Quarantine {
                id: "q-1791200000000".to_owned(),
                moved: vec![
                    "source=binance-archive/instrument=BTCUSDT/stream=metrics/date=2026-10-03/part-00001.parquet".to_owned(),
                    "source=binance-archive/instrument=BTCUSDT/stream=metrics/date=2026-10-04/part-00000.parquet".to_owned(),
                ],
            }),
            ..record()
        };
        let golden = format!(
            "mie-archive-supersede 1\n\
             archive {METRICS_ARCHIVE}\n\
             stream binance-archive/BTCUSDT/metrics\n\
             period 2026-10-03\n\
             entry metrics/{F}.import {}\n\
             entry metrics/{F}.import {}\n\
             quarantine q-1791200000000\n\
             moved source=binance-archive/instrument=BTCUSDT/stream=metrics/date=2026-10-03/part-00001.parquet\n\
             moved source=binance-archive/instrument=BTCUSDT/stream=metrics/date=2026-10-04/part-00000.parquet\n",
            "1".repeat(64),
            "2".repeat(64)
        );
        assert_eq!(replace.render(), golden);
        assert_eq!(SupersedeRecord::parse(&golden), Ok(replace));

        // No entry line at all (T8b's replace of an `unlisted` hold).
        let none = SupersedeRecord {
            entries: Vec::new(),
            ..record()
        };
        assert_eq!(SupersedeRecord::parse(&none.render()), Ok(none));
    }

    #[test]
    fn supersede_record_parser_is_strict() {
        let text = record().render();
        let lines: Vec<&str> = text.lines().collect();
        let mut unsorted = lines.clone();
        unsorted.swap(4, 5);
        let mut duplicate = lines.clone();
        duplicate[5] = lines[4];
        let mut reordered = lines.clone();
        reordered.swap(1, 2);
        let moved = "moved source=binance-archive/instrument=BTCUSDT/stream=metrics/date=2026-10-03/part-00001.parquet";
        for bad in [
            text.replace('\n', "\r\n"),
            text.replace("period ", "period\t"),
            text.replace("quarantine -\n", "quarantine - \n"),
            text.replace(&"1".repeat(64), &"A".repeat(64)),
            format!("{}\n", unsorted.join("\n")),
            format!("{}\n", duplicate.join("\n")),
            format!("{}\n", reordered.join("\n")),
            format!("{text}{moved}\n"),
            text.replace("quarantine -", "quarantine q-1"),
            text.replace(
                &format!("metrics/{F}.import {}", "1".repeat(64)),
                &format!(
                    "metrics/BTCUSDT-metrics-2026-10-02.zip.import {}",
                    "1".repeat(64)
                ),
            ),
            text.replace(
                &format!("metrics/{F}.import {}", "1".repeat(64)),
                &format!("aggTrades/{F}.import {}", "1".repeat(64)),
            ),
            text.replace(
                &format!("metrics/{F}.import {}", "1".repeat(64)),
                &format!("metrics/{F}.1.supersede.import {}", "1".repeat(64)),
            ),
            text.replace("quarantine -\n", ""),
            text.replace("mie-archive-supersede 1", "mie-archive-supersede 2"),
            text.trim_end().to_owned(),
            format!("{text}note x\n"),
        ] {
            assert!(SupersedeRecord::parse(&bad).is_err(), "{bad:?}");
        }
    }

    /// The entry format is unchanged byte for byte (ADR-034 D4).
    #[test]
    fn entry_golden_text_is_unchanged() {
        let golden = format!(
            "mie-archive-import 1\n\
             archive data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-09-30.zip\n\
             sha256 62cd39b1b90be376c56c7e24759a8ee3e2ea5d818d098a762cfa7e4fe0c56e1c\n\
             stream binance-archive/BTCUSDT/aggTrades\n\
             period 2026-09-30\n\
             header {}\n\
             rows 1434005\n\
             file source=binance-archive/instrument=BTCUSDT/stream=aggTrades/date=2026-09-30/part-00000.parquet 1000000 {}\n\
             file source=binance-archive/instrument=BTCUSDT/stream=aggTrades/date=2026-09-30/part-00001.parquet 434005 {}\n",
            ArchiveStream::AggTrades.expected_header(),
            "a".repeat(64),
            "b".repeat(64)
        );
        assert_eq!(sample().render(), golden);
    }

    /// A unique, empty directory for one test.
    struct Dir(PathBuf);

    impl Dir {
        fn new(tag: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "mie-ledger-{tag}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn metrics_entry(sha: &str) -> ImportLedger {
        ImportLedger {
            archive: METRICS_ARCHIVE.to_owned(),
            sha256: sha.to_owned(),
            stream: metrics_key(),
            period: Period::Day(parse_day("2026-10-03").unwrap()),
            header: None,
            rows: 0,
            files: Vec::new(),
        }
    }

    fn put(dir: &Path, name: &str, text: &str) {
        let segment = dir.join("metrics");
        fs::create_dir_all(&segment).unwrap();
        fs::write(segment.join(name), text).unwrap();
    }

    /// G3: from generation 1 an entry and every record must name their own
    /// archive file and directory; a generation-0 entry keeps today's rules.
    #[test]
    fn binding_applies_from_generation_one() {
        let dir = Dir::new("binding");
        let ledgers = LedgerDir::new(&dir.0);
        let other = ImportLedger {
            archive: METRICS_ARCHIVE.replace("2026-10-03", "2026-10-02"),
            ..metrics_entry(&"c".repeat(64))
        };
        // Generation 0 holding another file's text: today's behaviour.
        put(&dir.0, &format!("{F}.import"), &other.render());
        assert_eq!(
            ledgers.read(ArchiveStream::Metrics, F).unwrap(),
            Some(other.clone())
        );
        // The same text at generation 1, after a record: malformed.
        put(
            &dir.0,
            &format!("{F}.1.supersede.import"),
            &record().render(),
        );
        put(&dir.0, &format!("{F}.1.import"), &other.render());
        let error = ledgers.read(ArchiveStream::Metrics, F).unwrap_err();
        assert!(error.0.contains(&format!("{F}.1.import")), "{error}");
        // A record naming another archive file: malformed.
        let dir = Dir::new("binding-record");
        let ledgers = LedgerDir::new(&dir.0);
        put(
            &dir.0,
            &format!("{F}.import"),
            &metrics_entry(&"c".repeat(64)).render(),
        );
        let foreign = SupersedeRecord {
            archive: other.archive.clone(),
            entries: Vec::new(),
            ..record()
        };
        put(
            &dir.0,
            &format!("{F}.1.supersede.import"),
            &foreign.render(),
        );
        let error = ledgers.read(ArchiveStream::Metrics, F).unwrap_err();
        assert!(error.0.contains("supersede"), "{error}");
        // A record in the wrong directory: malformed.
        let dir = Dir::new("binding-segment");
        let ledgers = LedgerDir::new(&dir.0);
        let segment = dir.0.join("aggTrades");
        fs::create_dir_all(&segment).unwrap();
        fs::write(
            segment.join(format!("{F}.1.supersede.import")),
            SupersedeRecord {
                entries: Vec::new(),
                ..record()
            }
            .render(),
        )
        .unwrap();
        assert!(ledgers.read(ArchiveStream::AggTrades, F).is_err());
        // A malformed name in the directory is an error naming it.
        let dir = Dir::new("binding-name");
        let ledgers = LedgerDir::new(&dir.0);
        put(&dir.0, &format!("{F}.01.import"), "x");
        let error = ledgers.list(ArchiveStream::Metrics).unwrap_err();
        assert!(error.0.contains(&format!("{F}.01.import")), "{error}");
    }

    /// G5: `write` lands at the supersede generation and never replaces;
    /// `write_supersede` must be above every generation.
    #[test]
    fn writes_land_at_the_supersede_generation_and_never_replace() {
        let dir = Dir::new("writes");
        let ledgers = LedgerDir::new(&dir.0);
        let e0 = metrics_entry(&"c".repeat(64));
        ledgers.write(ArchiveStream::Metrics, F, &e0).unwrap();
        assert!(dir.0.join(format!("metrics/{F}.import")).exists());
        assert!(ledgers.write(ArchiveStream::Metrics, F, &e0).is_err());
        assert!(
            ledgers
                .write_supersede(ArchiveStream::Metrics, F, 0, &record())
                .is_err()
        );
        ledgers
            .write_supersede(ArchiveStream::Metrics, F, 1, &record())
            .unwrap();
        // E0 is superseded: F reads as not imported.
        assert_eq!(ledgers.read(ArchiveStream::Metrics, F).unwrap(), None);
        assert_eq!(ledgers.list(ArchiveStream::Metrics).unwrap(), Vec::new());
        // A record of an existing name or at or below a generation: refused.
        assert!(
            ledgers
                .write_supersede(ArchiveStream::Metrics, F, 1, &record())
                .is_err()
        );
        let e1 = metrics_entry(&"d".repeat(64));
        ledgers.write(ArchiveStream::Metrics, F, &e1).unwrap();
        assert!(dir.0.join(format!("metrics/{F}.1.import")).exists());
        assert!(ledgers.write(ArchiveStream::Metrics, F, &e1).is_err());
        assert_eq!(
            ledgers.read(ArchiveStream::Metrics, F).unwrap(),
            Some(e1.clone())
        );
        assert_eq!(ledgers.list(ArchiveStream::Metrics).unwrap(), vec![e1]);
        // E0's bytes never change.
        assert_eq!(
            fs::read_to_string(dir.0.join(format!("metrics/{F}.import"))).unwrap(),
            e0.render()
        );
        let files = ledgers.files(ArchiveStream::Metrics, F).unwrap();
        let names: Vec<String> = files.iter().map(|f| f.name.render()).collect();
        assert_eq!(
            names,
            [
                format!("{F}.import"),
                format!("{F}.1.import"),
                format!("{F}.1.supersede.import"),
            ]
        );
        assert!(
            ledgers
                .write_supersede(ArchiveStream::Metrics, F, 1, &record())
                .is_err()
        );
        ledgers
            .write_supersede(ArchiveStream::Metrics, F, 2, &record())
            .unwrap();
        assert_eq!(ledgers.read(ArchiveStream::Metrics, F).unwrap(), None);
    }
}
