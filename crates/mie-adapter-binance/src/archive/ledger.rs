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

use super::catalog::{ArchiveStream, Period};
use mie_ports::raw::{RawStreamKey, is_sha256_hex};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const LEDGER_MAGIC: &str = "mie-archive-import 1";
const PENDING_MAGIC: &str = "mie-archive-pending 1";

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

    /// Where the ledger of `file_name` of `stream` lives.
    pub fn ledger_path(&self, stream: ArchiveStream, file_name: &str) -> PathBuf {
        self.root
            .join(stream.raw_name())
            .join(format!("{file_name}.import"))
    }

    /// Reads the ledger of `file_name`, if one exists.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when it exists but cannot be read or parsed.
    pub fn read(
        &self,
        stream: ArchiveStream,
        file_name: &str,
    ) -> Result<Option<ImportLedger>, LedgerError> {
        let path = self.ledger_path(stream, file_name);
        match fs::read_to_string(&path) {
            Ok(text) => ImportLedger::parse(&text)
                .map(Some)
                .map_err(|e| LedgerError(format!("{}: {}", path.display(), e.0))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io("read", &path)(e)),
        }
    }

    /// Writes the ledger of `file_name`. A ledger is written once: an
    /// existing one is never replaced.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when one exists already or the write fails.
    pub fn write(
        &self,
        stream: ArchiveStream,
        file_name: &str,
        ledger: &ImportLedger,
    ) -> Result<(), LedgerError> {
        let path = self.ledger_path(stream, file_name);
        if path.exists() {
            return Err(LedgerError(format!(
                "{} exists; a ledger is never replaced",
                path.display()
            )));
        }
        write_durably(&path, &ledger.render())
    }

    /// Every ledger of `stream`, sorted by file name.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when the directory cannot be listed or a ledger is
    /// malformed.
    pub fn list(&self, stream: ArchiveStream) -> Result<Vec<ImportLedger>, LedgerError> {
        let dir = self.root.join(stream.raw_name());
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io("list", &dir)(e)),
        };
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(io("list", &dir))?;
            if let Ok(name) = entry.file_name().into_string()
                && let Some(file_name) = name.strip_suffix(".import")
                && !name.starts_with('.')
            {
                names.push(file_name.to_owned());
            }
        }
        names.sort();
        let mut ledgers = Vec::new();
        for name in names {
            if let Some(ledger) = self.read(stream, &name)? {
                ledgers.push(ledger);
            }
        }
        Ok(ledgers)
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
}
