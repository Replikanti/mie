//! The copy ledger of a tiered raw store (ADR-062 D7).
//!
//! An append-only text file at `<raw_root>/.mie/copy-ledger`, on hot's
//! local filesystem (ADR-062 D4) and outside the layout. It records which
//! copies of which sealed files exist on the copy targets, which partitions
//! left hot and came back, and the results of every verification pass.
//!
//! # Grammar v1
//!
//! ```text
//! mie-copy-ledger 1
//! <kind> <at_ms> <fields…> <chain>
//! …
//! ```
//!
//! - Every line ends with `\n`. Tokens are separated by single spaces; no
//!   token is empty.
//! - `at_ms` is a canonical decimal UTC epoch millisecond in
//!   `0 ..= MAX_EVENT_TIME_MS`. The caller passes it in: this module reads
//!   no clock.
//! - The *body* of a record is its line without ` <chain>\n`. `chain` is 64
//!   lowercase hex characters: the SHA-256 of the previous record's chain
//!   (its 64 ASCII hex characters) followed directly by this record's body.
//!   The chain before record 1 is the SHA-256 of the header line, its `\n`
//!   included. Each hash covers its predecessor, so it commits to the whole
//!   prefix, while an append reads only the last records, and a flipped
//!   byte in record n fails record n's own chain.
//! - The kinds and their fields are those of [`Kind`]; the value sets of
//!   [`FindingCode`], [`Subject`], [`LostOrigin`], [`HoldReason`] and
//!   [`Resolution`] are closed.
//! - The manifest text of a [`Kind::Manifest`] record is escaped: `%`, the
//!   bytes 0x00–0x20 and 0x7F are written as `%XX` with uppercase hex, every
//!   other byte is literal, and any other spelling is refused. It is stored
//!   verbatim, never re-rendered.
//!
//! The parser is strict: any line [`Record`] rendering cannot produce is
//! [`RawStoreError::Corrupt`]. A later step adds a kind only together with
//! an ADR-062 edit and a golden test.
//!
//! # Readers and appenders
//!
//! Readers ([`read`]) take no lock and ignore *the* incomplete trailing
//! record: the bytes after the last `\n`, if there are any, or otherwise the
//! last line when its chain fails while the line before it verifies. Only a
//! crashed or running append leaves one. Any other chain failure is
//! [`RawStoreError::Integrity`].
//!
//! Appenders ([`CopyLedger::append`], [`LedgerHold::append`]) hold an
//! exclusive lock on `<raw_root>/.mie/copy-ledger.lock`, a separate file
//! because the rebuild replaces the ledger by rename and a lock on the
//! ledger's inode would stay on the replaced file. Under the lock an append
//! reads the header and the last records back, then decides before it
//! writes anything: it truncates at most the one incomplete trailing record
//! and appends, or it refuses with [`RawStoreError::Integrity`] and leaves
//! the file byte-identical. It never truncates a complete record before the
//! last one. The new line is written with one `write_all` and fsynced
//! before the lock is released. A truncation is reported to the caller as
//! a structured [`Truncation`], in the [`AppendOutcome`] or, when a step
//! after it fails, in the [`AppendError`], so the caller can journal it
//! even when the append failed. The ledger is never created by an append;
//! only the init step creates it (ADR-062 D7,
//! [`create_meta`](crate::root_marker::create_meta)).

use crate::io_error;
use crate::layout::{self, COPY_LEDGER, COPY_LEDGER_LOCK, META_DIR, PartFile};
use crate::manifest::{self, hex, sha256_hex};
use crate::replica::{self, TailSource};
use mie_ports::raw::{RawStoreError, is_sha256_hex, validate_segment};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// The first line of every copy ledger: the format name and version.
pub const HEADER: &str = "mie-copy-ledger 1\n";

/// Length of a chain hash in hex characters.
const CHAIN_LEN: usize = 64;

/// The path of the copy ledger of the store rooted at `raw_root`.
pub fn ledger_path(raw_root: &Path) -> PathBuf {
    raw_root.join(META_DIR).join(COPY_LEDGER)
}

/// The path of the copy ledger's lock file of the store rooted at
/// `raw_root`.
pub fn lock_path(raw_root: &Path) -> PathBuf {
    raw_root.join(META_DIR).join(COPY_LEDGER_LOCK)
}

/// A closed set of tokens: an enum with its token per variant.
macro_rules! vocabulary {
    (
        $(#[$meta:meta])*
        $name:ident { $($(#[$vmeta:meta])* $variant:ident = $text:literal,)+ }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        pub enum $name {
            $($(#[$vmeta])* $variant,)+
        }

        impl $name {
            /// Every value, in grammar order.
            pub const ALL: &[Self] = &[$(Self::$variant,)+];

            /// The value's token in a ledger record.
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                }
            }

            fn parse(token: &str) -> Option<Self> {
                Self::ALL.iter().copied().find(|value| value.as_str() == token)
            }
        }
    };
}

vocabulary! {
    /// What a verification pass found (ADR-062 D11, *On a finding*).
    FindingCode {
        /// A copy that should exist does not.
        Missing = "missing",
        /// A copy's size differs from its manifest.
        Size = "size",
        /// A copy's SHA-256 differs from its manifest.
        Sha256 = "sha256",
        /// A root's manifest text differs from the ledger's.
        Manifest = "manifest",
        /// Fewer than k good copies on distinct copy targets.
        Copies = "copies",
        /// A journal or ledger copy is not a byte prefix of hot's.
        Prefix = "prefix",
        /// A copy-ledger hash chain fails.
        Chain = "chain",
        /// A copy's last good verification is older than P.
        Stale = "stale",
        /// Two copies of one import-ledger entry differ.
        Entry = "entry",
    }
}

vocabulary! {
    /// Which record named a lost file (ADR-062 D7 rebuild step 4, D11
    /// lost-file rule).
    LostOrigin {
        /// The copy ledger.
        Ledger = "ledger",
        /// A capture journal's `sealed` record.
        Journal = "journal",
        /// A capture journal's `recovery` `rolled_forward` path.
        Recovery = "recovery",
        /// An import-ledger entry.
        Import = "import",
        /// A verification pass.
        Verify = "verify",
    }
}

vocabulary! {
    /// Why an archive part is held (ADR-062 D11, hot restore step 5).
    HoldReason {
        /// No current import entry lists the part.
        Unlisted = "unlisted",
        /// Two copies of its import entry differ.
        EntryConflict = "entry-conflict",
        /// Its path carries different manifest texts on two roots.
        ManifestConflict = "manifest-conflict",
    }
}

vocabulary! {
    /// How a hold was resolved (ADR-062 D11, *Held archive parts and their
    /// resolution*).
    Resolution {
        /// The held parts matched the file and were re-admitted.
        Readmit = "readmit",
        /// The held parts were quarantined and the file is re-imported.
        Replace = "replace",
    }
}

/// What a [`Kind::Finding`] is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subject {
    /// A sealed data file, by its relative path.
    File(String),
    /// The capture journal.
    Journal,
    /// The copy ledger.
    CopyLedger,
    /// An import-ledger entry, `<segment>/<name>.import`.
    Entry(String),
}

impl Subject {
    fn render(&self) -> Result<String, String> {
        Ok(match self {
            Self::File(path) => {
                check_data_path(path)?;
                path.clone()
            }
            Self::Journal => "journal".to_owned(),
            Self::CopyLedger => "copy-ledger".to_owned(),
            Self::Entry(entry) => {
                check_entry(entry)?;
                format!("entry:{entry}")
            }
        })
    }

    fn parse(token: &str) -> Result<Self, String> {
        match token {
            "journal" => Ok(Self::Journal),
            "copy-ledger" => Ok(Self::CopyLedger),
            _ => match token.strip_prefix("entry:") {
                Some(entry) => check_entry(entry).map(|()| Self::Entry(entry.to_owned())),
                None => check_data_path(token)
                    .map(|()| Self::File(token.to_owned()))
                    .map_err(|_| format!("unknown subject {token:?}")),
            },
        }
    }
}

/// The content of a record, by kind (ADR-062 D6, D7, D11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// `manifest`: the manifest text of a sealed file, verbatim, written
    /// with its first verified copy.
    Manifest {
        /// The manifest text; it must parse as a manifest.
        text: String,
    },
    /// `copy`: a verified copy of a sealed file on a root.
    Copy {
        /// The data file's relative path.
        path: String,
        /// The root id.
        root: String,
    },
    /// `removed`: a partition removed from hot (D6).
    Removed {
        /// The partition directory, `source=…/instrument=…/stream=…/date=…`.
        partition: String,
    },
    /// `returned`: a partition returned to hot (recall, hot restore).
    Returned {
        /// The partition directory.
        partition: String,
    },
    /// `ledger-copy`: a verified copy of a copy-ledger prefix on a root.
    LedgerCopy {
        /// The root id.
        root: String,
        /// The prefix length in bytes.
        prefix_bytes: u64,
        /// The prefix's SHA-256, lowercase hex.
        prefix_sha256: String,
    },
    /// `finding`: one finding of a verification pass. A pass's findings
    /// precede its [`Kind::Verify`] record and carry the same root and
    /// `at_ms`.
    Finding {
        /// The root id.
        root: String,
        /// What was found.
        code: FindingCode,
        /// What it is about.
        subject: Subject,
    },
    /// `verify`: a verification pass over a root.
    Verify {
        /// The root id.
        root: String,
        /// The ledger prefix the pass covered, in bytes.
        prefix_bytes: u64,
        /// The pass's findings.
        findings: u64,
    },
    /// `lost`: a file with no good copy left (D11 lost-file rule).
    Lost {
        /// The data file's relative path.
        path: String,
        /// The record that named it.
        origin: LostOrigin,
        /// Its SHA-256, when that record knows it (written `-` otherwise).
        sha256: Option<String>,
    },
    /// `hold`: an archive part held (D11).
    Hold {
        /// The data file's relative path.
        path: String,
        /// Why it is held.
        reason: HoldReason,
    },
    /// `hold-cleared`: a hold resolved.
    HoldCleared {
        /// The data file's relative path.
        path: String,
        /// How.
        resolution: Resolution,
    },
    /// `quarantined`: a held part moved to quarantine.
    Quarantined {
        /// The data file's relative path.
        path: String,
        /// The quarantine id.
        quarantine: String,
    },
    /// `replaced`: a quarantined part replaced by a re-import's entry.
    Replaced {
        /// The data file's relative path.
        path: String,
        /// The import entry, `<segment>/<name>.import`.
        entry: String,
    },
}

impl Kind {
    /// The kind's token.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Manifest { .. } => "manifest",
            Self::Copy { .. } => "copy",
            Self::Removed { .. } => "removed",
            Self::Returned { .. } => "returned",
            Self::LedgerCopy { .. } => "ledger-copy",
            Self::Finding { .. } => "finding",
            Self::Verify { .. } => "verify",
            Self::Lost { .. } => "lost",
            Self::Hold { .. } => "hold",
            Self::HoldCleared { .. } => "hold-cleared",
            Self::Quarantined { .. } => "quarantined",
            Self::Replaced { .. } => "replaced",
        }
    }
}

/// One copy-ledger record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// When it was written, UTC epoch milliseconds, supplied by the caller.
    pub at_ms: i64,
    /// What it records.
    pub kind: Kind,
}

impl Record {
    /// The record's body: its line without ` <chain>\n`.
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Invalid`] when a field is outside the grammar.
    pub fn body(&self) -> Result<String, RawStoreError> {
        render_body(self).map_err(|detail| {
            RawStoreError::Invalid(format!("copy-ledger {} record: {detail}", self.kind.name()))
        })
    }
}

fn render_body(record: &Record) -> Result<String, String> {
    check_at_ms(record.at_ms)?;
    let fields: Vec<String> = match &record.kind {
        Kind::Manifest { text } => {
            manifest::parse(text).map_err(|e| e.to_string())?;
            vec![escape(text)]
        }
        Kind::Copy { path, root } => {
            check_data_path(path)?;
            check_id(root)?;
            vec![path.clone(), root.clone()]
        }
        Kind::Removed { partition } | Kind::Returned { partition } => {
            check_partition(partition)?;
            vec![partition.clone()]
        }
        Kind::LedgerCopy {
            root,
            prefix_bytes,
            prefix_sha256,
        } => {
            check_id(root)?;
            check_sha256(prefix_sha256)?;
            vec![
                root.clone(),
                prefix_bytes.to_string(),
                prefix_sha256.clone(),
            ]
        }
        Kind::Finding {
            root,
            code,
            subject,
        } => {
            check_id(root)?;
            vec![root.clone(), code.as_str().to_owned(), subject.render()?]
        }
        Kind::Verify {
            root,
            prefix_bytes,
            findings,
        } => {
            check_id(root)?;
            vec![root.clone(), prefix_bytes.to_string(), findings.to_string()]
        }
        Kind::Lost {
            path,
            origin,
            sha256,
        } => {
            check_data_path(path)?;
            let sha = match sha256 {
                Some(sha) => {
                    check_sha256(sha)?;
                    sha.clone()
                }
                None => "-".to_owned(),
            };
            vec![path.clone(), origin.as_str().to_owned(), sha]
        }
        Kind::Hold { path, reason } => {
            check_data_path(path)?;
            vec![path.clone(), reason.as_str().to_owned()]
        }
        Kind::HoldCleared { path, resolution } => {
            check_data_path(path)?;
            vec![path.clone(), resolution.as_str().to_owned()]
        }
        Kind::Quarantined { path, quarantine } => {
            check_data_path(path)?;
            check_id(quarantine)?;
            vec![path.clone(), quarantine.clone()]
        }
        Kind::Replaced { path, entry } => {
            check_data_path(path)?;
            check_entry(entry)?;
            vec![path.clone(), entry.clone()]
        }
    };
    let mut body = format!("{} {}", record.kind.name(), record.at_ms);
    for field in fields {
        body.push(' ');
        body.push_str(&field);
    }
    Ok(body)
}

/// Parses a record body strictly: exactly what [`render_body`] writes.
fn parse_body(body: &str) -> Result<Record, String> {
    let tokens: Vec<&str> = body.split(' ').collect();
    if tokens.iter().any(|t| t.is_empty()) {
        return Err("empty token (a double, leading or trailing space)".to_owned());
    }
    let (kind, at, fields) = match &tokens[..] {
        [kind, at, fields @ ..] => (*kind, *at, fields),
        _ => return Err("missing kind or time".to_owned()),
    };
    let at_ms = i64::try_from(decimal(at)?).map_err(|_| format!("time {at:?} out of range"))?;
    check_at_ms(at_ms)?;
    let arity = |n: usize| -> Result<(), String> {
        if fields.len() == n {
            Ok(())
        } else {
            Err(format!(
                "{kind} takes {n} fields after the time, found {}",
                fields.len()
            ))
        }
    };
    let kind = match kind {
        "manifest" => {
            arity(1)?;
            let text = unescape(fields[0])?;
            manifest::parse(&text).map_err(|e| e.to_string())?;
            Kind::Manifest { text }
        }
        "copy" => {
            arity(2)?;
            check_data_path(fields[0])?;
            check_id(fields[1])?;
            Kind::Copy {
                path: fields[0].to_owned(),
                root: fields[1].to_owned(),
            }
        }
        "removed" | "returned" => {
            arity(1)?;
            check_partition(fields[0])?;
            let partition = fields[0].to_owned();
            if kind == "removed" {
                Kind::Removed { partition }
            } else {
                Kind::Returned { partition }
            }
        }
        "ledger-copy" => {
            arity(3)?;
            check_id(fields[0])?;
            check_sha256(fields[2])?;
            Kind::LedgerCopy {
                root: fields[0].to_owned(),
                prefix_bytes: decimal(fields[1])?,
                prefix_sha256: fields[2].to_owned(),
            }
        }
        "finding" => {
            arity(3)?;
            check_id(fields[0])?;
            Kind::Finding {
                root: fields[0].to_owned(),
                code: FindingCode::parse(fields[1])
                    .ok_or_else(|| format!("unknown finding code {:?}", fields[1]))?,
                subject: Subject::parse(fields[2])?,
            }
        }
        "verify" => {
            arity(3)?;
            check_id(fields[0])?;
            Kind::Verify {
                root: fields[0].to_owned(),
                prefix_bytes: decimal(fields[1])?,
                findings: decimal(fields[2])?,
            }
        }
        "lost" => {
            arity(3)?;
            check_data_path(fields[0])?;
            let sha256 = match fields[2] {
                "-" => None,
                sha => {
                    check_sha256(sha)?;
                    Some(sha.to_owned())
                }
            };
            Kind::Lost {
                path: fields[0].to_owned(),
                origin: LostOrigin::parse(fields[1])
                    .ok_or_else(|| format!("unknown origin {:?}", fields[1]))?,
                sha256,
            }
        }
        "hold" => {
            arity(2)?;
            check_data_path(fields[0])?;
            Kind::Hold {
                path: fields[0].to_owned(),
                reason: HoldReason::parse(fields[1])
                    .ok_or_else(|| format!("unknown hold reason {:?}", fields[1]))?,
            }
        }
        "hold-cleared" => {
            arity(2)?;
            check_data_path(fields[0])?;
            Kind::HoldCleared {
                path: fields[0].to_owned(),
                resolution: Resolution::parse(fields[1])
                    .ok_or_else(|| format!("unknown resolution {:?}", fields[1]))?,
            }
        }
        "quarantined" => {
            arity(2)?;
            check_data_path(fields[0])?;
            check_id(fields[1])?;
            Kind::Quarantined {
                path: fields[0].to_owned(),
                quarantine: fields[1].to_owned(),
            }
        }
        "replaced" => {
            arity(2)?;
            check_data_path(fields[0])?;
            check_entry(fields[1])?;
            Kind::Replaced {
                path: fields[0].to_owned(),
                entry: fields[1].to_owned(),
            }
        }
        other => return Err(format!("unknown kind {other:?}")),
    };
    Ok(Record { at_ms, kind })
}

/// A canonical decimal `u64`: digits only, no leading zero.
fn decimal(text: &str) -> Result<u64, String> {
    let canonical =
        text.bytes().all(|b| b.is_ascii_digit()) && (text == "0" || !text.starts_with('0'));
    canonical
        .then(|| text.parse().ok())
        .flatten()
        .ok_or_else(|| format!("{text:?} is not a canonical decimal"))
}

fn check_at_ms(at_ms: i64) -> Result<(), String> {
    if (0..=layout::MAX_EVENT_TIME_MS).contains(&at_ms) {
        Ok(())
    } else {
        Err(format!(
            "time {at_ms} is outside 0 ..= {}",
            layout::MAX_EVENT_TIME_MS
        ))
    }
}

/// A sealed data path that renders back to itself.
fn check_data_path(path: &str) -> Result<(), String> {
    let (key, date, part) = layout::parse_data_path(path).map_err(|e| e.to_string())?;
    if layout::file_path(&key, &date, PartFile::Data, part) == path {
        Ok(())
    } else {
        Err(format!("{path:?} is not a canonical data path"))
    }
}

fn check_partition(dir: &str) -> Result<(), String> {
    layout::parse_partition_dir(dir)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn check_id(id: &str) -> Result<(), String> {
    validate_segment(id).map_err(|e| format!("id: {e}"))
}

fn check_sha256(sha: &str) -> Result<(), String> {
    if is_sha256_hex(sha) {
        Ok(())
    } else {
        Err(format!("{sha:?} is not 64 lowercase hex characters"))
    }
}

/// An import-ledger entry: `<segment>/<name>.import`, the name from
/// `[A-Za-z0-9._-]`, not starting with `.`.
fn check_entry(entry: &str) -> Result<(), String> {
    let bad = || format!("{entry:?} is not an import entry <segment>/<name>.import");
    let (segment, file) = entry.split_once('/').ok_or_else(bad)?;
    validate_segment(segment).map_err(|_| bad())?;
    let stem = file.strip_suffix(".import").ok_or_else(bad)?;
    let allowed = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-');
    if stem.is_empty() || file.starts_with('.') || !file.bytes().all(allowed) {
        return Err(bad());
    }
    Ok(())
}

/// Whether a byte is written as `%XX` in a manifest field.
fn needs_escape(byte: u8) -> bool {
    byte <= 0x20 || byte == 0x7F || byte == b'%'
}

fn escape(text: &str) -> String {
    let mut bytes = Vec::with_capacity(text.len() + text.len() / 4);
    for &byte in text.as_bytes() {
        if needs_escape(byte) {
            bytes.extend_from_slice(format!("%{byte:02X}").as_bytes());
        } else {
            bytes.push(byte);
        }
    }
    // Only ASCII bytes are replaced, so multi-byte sequences stay intact.
    String::from_utf8(bytes).expect("escaping keeps UTF-8 intact")
}

fn unescape(token: &str) -> Result<String, String> {
    let bytes = token.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte == b'%' {
            let digit = |b: Option<&u8>| match b {
                Some(&b @ b'0'..=b'9') => Some(b - b'0'),
                Some(&b @ b'A'..=b'F') => Some(b - b'A' + 10),
                _ => None,
            };
            let (Some(high), Some(low)) = (digit(bytes.get(i + 1)), digit(bytes.get(i + 2))) else {
                return Err(format!("bad escape at byte {i} of the manifest field"));
            };
            let value = high * 16 + low;
            if !needs_escape(value) {
                return Err(format!(
                    "needless escape %{value:02X} in the manifest field"
                ));
            }
            out.push(value);
            i += 3;
        } else if needs_escape(byte) {
            return Err(format!("unescaped byte 0x{byte:02X} in the manifest field"));
        } else {
            out.push(byte);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "the manifest field is not UTF-8".to_owned())
}

/// The chain before record 1: the SHA-256 of the header line.
fn genesis() -> String {
    sha256_hex(HEADER.as_bytes())
}

/// The chain of a record whose body is `body`, after the chain `previous`.
fn chain_of(previous: &[u8], body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(previous);
    hasher.update(body);
    hex(&hasher.finalize())
}

/// Splits a line (without its `\n`) into its body and its chain field, the
/// last [`CHAIN_LEN`] bytes after a space. `None` when the line is too short
/// for both or the separator is missing.
fn split_line(line: &[u8]) -> Option<(&[u8], &[u8])> {
    let split = line.len().checked_sub(CHAIN_LEN + 1).filter(|&n| n > 0)?;
    let (body, rest) = line.split_at(split);
    (rest[0] == b' ').then(|| (body, &rest[1..]))
}

/// The chain field of a line whose chain verifies after `previous`. The
/// field is compared byte for byte, so a flipped letter case fails it.
fn verified_chain<'a>(previous: &[u8], line: &'a [u8]) -> Option<&'a [u8]> {
    let (body, chain) = split_line(line)?;
    (chain_of(previous, body).as_bytes() == chain).then_some(chain)
}

/// Where a hash chain first fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainFailure {
    /// The record number, from 1; 0 is the header.
    pub record: u64,
    /// The byte offset of the record's line in the file.
    pub offset: u64,
}

/// The result of [`check_chain`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainCheck {
    /// Complete records: lines after the header that end with `\n`.
    pub records: u64,
    /// The first record whose chain fails, if any.
    pub failure: Option<ChainFailure>,
    /// Bytes after the last `\n`: a partial record.
    pub tail_bytes: u64,
}

/// Checks the header and the hash chain of a whole ledger (hot's or a
/// copy's). It applies no trailing-record policy: a failing last record is
/// reported like any other, and a partial tail only counted.
pub fn check_chain(bytes: &[u8]) -> ChainCheck {
    let header = HEADER.as_bytes();
    if !bytes.starts_with(header) {
        return ChainCheck {
            records: 0,
            failure: Some(ChainFailure {
                record: 0,
                offset: 0,
            }),
            tail_bytes: 0,
        };
    }
    let mut previous = genesis().into_bytes();
    let mut offset = header.len();
    let mut records = 0;
    let mut failure = None;
    while let Some(len) = bytes[offset..].iter().position(|&b| b == b'\n') {
        records += 1;
        let line = &bytes[offset..offset + len];
        if failure.is_none() {
            match verified_chain(&previous, line) {
                Some(chain) => previous = chain.to_vec(),
                None => {
                    failure = Some(ChainFailure {
                        record: records,
                        offset: offset as u64,
                    });
                }
            }
        }
        offset += len + 1;
    }
    ChainCheck {
        records,
        failure,
        tail_bytes: (bytes.len() - offset) as u64,
    }
}

/// A copy ledger as a reader sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ledger {
    /// Every complete, verified record, in file order.
    pub records: Vec<Record>,
    /// The bytes of the ignored incomplete trailing record, 0 if none.
    pub ignored_bytes: u64,
}

/// Reads the copy ledger of the store rooted at `raw_root`, without a lock.
/// `None` when it does not exist. The incomplete trailing record, if any, is
/// ignored (see the module docs).
///
/// # Errors
///
/// [`RawStoreError::Corrupt`] for a bad header or a verified line outside
/// the grammar; [`RawStoreError::Integrity`] for a chain failure before the
/// last record, naming the record and its byte offset;
/// [`RawStoreError::Io`] when it cannot be read.
pub fn read(raw_root: &Path) -> Result<Option<Ledger>, RawStoreError> {
    let path = ledger_path(raw_root);
    match fs::read(&path) {
        Ok(bytes) => parse(&path, &bytes).map(Some),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("read", &path)(error)),
    }
}

/// Parses the bytes of a whole ledger file as [`read`] does; `path` names
/// it in errors. The replication pass parses exactly the bytes it copies:
/// its verified prefix is `bytes.len() - ignored_bytes`, so the incomplete
/// trailing record is never inside it.
///
/// # Errors
///
/// As [`read`].
pub fn parse(path: &Path, bytes: &[u8]) -> Result<Ledger, RawStoreError> {
    if !bytes.starts_with(HEADER.as_bytes()) {
        return Err(RawStoreError::Corrupt(format!(
            "copy ledger {}: missing header or unknown format version",
            path.display()
        )));
    }
    let check = check_chain(bytes);
    let (kept, ignored_bytes) = match check.failure {
        None => (check.records, check.tail_bytes),
        // A complete last line whose chain fails, after a verified line, is
        // the incomplete trailing record.
        Some(failure) if failure.record == check.records && check.tail_bytes == 0 => {
            (check.records - 1, bytes.len() as u64 - failure.offset)
        }
        Some(failure) => {
            return Err(RawStoreError::Integrity(format!(
                "copy ledger {}: the hash chain fails at record {} (byte {})",
                path.display(),
                failure.record,
                failure.offset
            )));
        }
    };
    let mut records = Vec::new();
    let mut offset = HEADER.len();
    for number in 1..=kept {
        let len = bytes[offset..]
            .iter()
            .position(|&b| b == b'\n')
            .expect("a counted record ends with a line end");
        let line = &bytes[offset..offset + len];
        let record = parse_line(line).map_err(|detail| {
            RawStoreError::Corrupt(format!(
                "copy ledger {}: record {number} (byte {offset}): {detail}",
                path.display()
            ))
        })?;
        records.push(record);
        offset += len + 1;
    }
    Ok(Ledger {
        records,
        ignored_bytes,
    })
}

/// Parses a line whose chain verified.
fn parse_line(line: &[u8]) -> Result<Record, String> {
    let (body, _) = split_line(line).ok_or("no chain field")?;
    let body = std::str::from_utf8(body).map_err(|_| "not UTF-8".to_owned())?;
    parse_body(body)
}

/// An incomplete trailing record that an append truncated (ADR-062 D7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Truncation {
    /// The truncated bytes.
    pub bytes: u64,
    /// The byte offset the ledger was truncated to, where the truncated
    /// record started.
    pub offset: u64,
}

/// What [`extend_copy`] did to one copy target's ledger copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerCopy {
    /// The copy was up to date (the up-to-date rule of [`extend_copy`]):
    /// nothing was appended, and the pass appends no `ledger-copy` record.
    pub current: bool,
    /// The bytes appended to the copy.
    pub appended_bytes: u64,
    /// B: the length of hot's verified prefix, from the same read the copy
    /// was extended from.
    pub prefix_bytes: u64,
    /// The SHA-256 of hot's `[0, B)`, lowercase hex.
    pub prefix_sha256: String,
}

/// Extends the ledger copy `<target_root>/.mie/copy-ledger` of the copy
/// target `root_id` to hot's verified prefix (ADR-062 D11, *The journal
/// and the copy ledger*).
///
/// One read of hot's ledger gives its bytes, its records and B, the end of
/// its last complete, verified record: the single-trailing-record rule, so
/// a torn trailing record (partial bytes, or a complete last line whose
/// chain fails) is never copied. A copy is **up to date** when every hot
/// record from its length up to B is a `ledger-copy` record and the latest
/// `ledger-copy` record for `root_id` has prefix length = the copy's
/// length. Why: each pass ends with `ledger-copy` records that the copies
/// cannot hold yet; copying them would add new ones on every pass, and a
/// pass over unchanged state would never append nothing. A crash between a
/// tail append and its record leaves no matching record, so the next pass
/// writes it. An up-to-date copy's last record is still compared with
/// hot's; otherwise the copy is extended to B
/// ([`replica::extend_prefix`]) from the bytes of that same read. Nothing
/// is appended to hot's ledger here: the pass appends the `ledger-copy`
/// record.
///
/// # Errors
///
/// [`RawStoreError::Integrity`] for a missing hot ledger, a chain failure
/// before its last record, and a copy that is not a byte prefix of hot's at
/// its tail or is longer than B; [`RawStoreError::Corrupt`] for a bad
/// header or grammar; [`RawStoreError::Io`] on I/O failure.
pub fn extend_copy(
    hot_root: &Path,
    target_root: &Path,
    root_id: &str,
) -> Result<LedgerCopy, RawStoreError> {
    let path = ledger_path(hot_root);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Err(missing_ledger(&path)),
        Err(error) => return Err(io_error("read", &path)(error)),
    };
    let ledger = parse(&path, &bytes)?;
    let prefix = &bytes[..bytes.len() - ledger.ignored_bytes as usize];
    let copy = ledger_path(target_root);
    let copy_len = match fs::metadata(&copy) {
        Ok(metadata) => Some(metadata.len()),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => return Err(io_error("inspect", &copy)(error)),
    };
    let current = copy_len.filter(|&len| is_current(prefix, &ledger.records, root_id, len));
    let end = current.unwrap_or(prefix.len() as u64);
    let appended_bytes = replica::extend_prefix(TailSource::Bytes(prefix), &copy, end)?;
    Ok(LedgerCopy {
        current: current.is_some(),
        appended_bytes,
        prefix_bytes: prefix.len() as u64,
        prefix_sha256: sha256_hex(prefix),
    })
}

/// The up-to-date rule of [`extend_copy`] for a copy of `len` bytes over
/// hot's verified `prefix` and its `records`.
fn is_current(prefix: &[u8], records: &[Record], root_id: &str, len: u64) -> bool {
    if len > prefix.len() as u64 {
        return false;
    }
    let mut offset = HEADER.len() as u64;
    let mut latest = None;
    for (record, line) in records
        .iter()
        .zip(prefix[HEADER.len()..].split_inclusive(|&b| b == b'\n'))
    {
        let is_ledger_copy = match &record.kind {
            Kind::LedgerCopy {
                root, prefix_bytes, ..
            } => {
                if root == root_id {
                    latest = Some(*prefix_bytes);
                }
                true
            }
            _ => false,
        };
        if offset >= len && !is_ledger_copy {
            return false;
        }
        offset += line.len() as u64;
    }
    latest == Some(len)
}

/// What an append did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendOutcome {
    /// The incomplete trailing record truncated before the append, if any.
    /// A caller journals every truncation.
    pub truncation: Option<Truncation>,
    /// The byte offset of the appended record.
    pub offset: u64,
}

/// A failed append. A truncation that happened before the failure is
/// carried as a structured field, so the caller journals it even though
/// the append failed (review of #197, finding 5).
///
/// There is deliberately no `From<AppendError> for RawStoreError`: a `?`
/// could then drop the truncation without the compiler noticing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendError {
    /// Why the append failed.
    pub error: RawStoreError,
    /// The incomplete trailing record truncated before the failure. `Some`
    /// whenever the truncation itself succeeded, whatever failed after it
    /// (the fsync of the truncation or the write of the record).
    pub truncation: Option<Truncation>,
}

impl From<RawStoreError> for AppendError {
    /// A failure before anything was truncated.
    fn from(error: RawStoreError) -> Self {
        Self {
            error,
            truncation: None,
        }
    }
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)?;
        if let Some(Truncation { bytes, offset }) = self.truncation {
            write!(
                f,
                "; the incomplete trailing record ({bytes} bytes at byte {offset}) was truncated \
                 before the failure"
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for AppendError {}

/// The appender of the copy ledger of one store.
///
/// Every [`append`](Self::append) takes the ledger lock for its duration.
/// Inside a [`LedgerHold`], append through the hold: a second `CopyLedger`
/// in the same process opens its own descriptor and would block on the
/// hold's lock, since the lock belongs to the open file, not the process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyLedger {
    ledger: PathBuf,
    lock: PathBuf,
}

impl CopyLedger {
    /// The appender of the copy ledger of the store rooted at `raw_root`.
    /// Touches nothing.
    pub fn at(raw_root: &Path) -> Self {
        Self {
            ledger: ledger_path(raw_root),
            lock: lock_path(raw_root),
        }
    }

    /// The ledger's path.
    pub fn path(&self) -> &Path {
        &self.ledger
    }

    /// Appends `record` under a blocking lock (see the module docs).
    ///
    /// # Errors
    ///
    /// An [`AppendError`] whose `error` is [`RawStoreError::Invalid`] for a
    /// record outside the grammar (nothing is touched),
    /// [`RawStoreError::Integrity`] when the ledger does not exist or its
    /// last records are damaged beyond the one trailing record (the file is
    /// left byte-identical), [`RawStoreError::Corrupt`] for a bad header,
    /// and [`RawStoreError::Io`] on I/O failure. Its `truncation` names a
    /// truncation that happened before the failure.
    pub fn append(&self, record: &Record) -> Result<AppendOutcome, AppendError> {
        let body = record.body()?;
        // A missing ledger is refused before the lock file is created, so
        // an append to a store without a ledger creates no file.
        match fs::symlink_metadata(&self.ledger) {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(missing_ledger(&self.ledger).into());
            }
            Err(error) => return Err(io_error("inspect", &self.ledger)(error).into()),
            Ok(_) => {}
        }
        let lock = open_lock(&self.lock)?;
        lock.lock().map_err(io_error("lock", &self.lock))?;
        // The ledger is opened only under the lock: a holder (the rebuild)
        // may have replaced it by rename while this append waited, and a
        // descriptor opened before would write into the replaced file.
        let outcome = open_ledger(&self.ledger)
            .map_err(AppendError::from)
            .and_then(|mut file| append_locked(&self.ledger, &mut file, &body));
        drop(lock);
        outcome
    }

    /// Takes the ledger lock for a whole run (the rebuild and the hot
    /// restore), without waiting. The ledger itself need not exist.
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Locked`] when another holder has it;
    /// [`RawStoreError::Io`] when the lock file cannot be opened.
    pub fn try_hold(&self) -> Result<LedgerHold, RawStoreError> {
        let lock = open_lock(&self.lock)?;
        match lock.try_lock() {
            Ok(()) => Ok(LedgerHold {
                ledger: self.ledger.clone(),
                _lock: lock,
            }),
            Err(TryLockError::WouldBlock) => Err(RawStoreError::Locked(format!(
                "another process holds the copy-ledger lock {}",
                self.lock.display()
            ))),
            Err(TryLockError::Error(error)) => Err(io_error("lock", &self.lock)(error)),
        }
    }
}

/// The copy-ledger lock held for a whole run; dropping it releases the
/// lock.
#[derive(Debug)]
pub struct LedgerHold {
    ledger: PathBuf,
    _lock: File,
}

impl LedgerHold {
    /// Appends `record` under the held lock, as [`CopyLedger::append`] does.
    ///
    /// # Errors
    ///
    /// As [`CopyLedger::append`].
    pub fn append(&self, record: &Record) -> Result<AppendOutcome, AppendError> {
        let body = record.body()?;
        let mut file = open_ledger(&self.ledger)?;
        append_locked(&self.ledger, &mut file, &body)
    }
}

/// The error for a ledger that does not exist.
fn missing_ledger(path: &Path) -> RawStoreError {
    RawStoreError::Integrity(format!(
        "copy ledger {} does not exist; only the init step creates it, never an append \
         (ADR-062 D7)",
        path.display()
    ))
}

/// Opens the ledger for reading and appending; never creates it. Called
/// only under the lock.
fn open_ledger(path: &Path) -> Result<File, RawStoreError> {
    OpenOptions::new()
        .read(true)
        .append(true)
        .open(path)
        .map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                missing_ledger(path)
            } else {
                io_error("open", path)(error)
            }
        })
}

fn open_lock(path: &Path) -> Result<File, RawStoreError> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .map_err(io_error("open", path))
}

/// Reads `len` bytes at `offset`.
fn read_at(
    file: &mut File,
    path: &Path,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>, RawStoreError> {
    let mut buffer = vec![0; len];
    file.seek(SeekFrom::Start(offset))
        .and_then(|_| file.read_exact(&mut buffer))
        .map_err(io_error("read", path))?;
    Ok(buffer)
}

/// How far back an append reads per step. Unit tests use a step shorter
/// than a record, so every append reads back through several steps.
const TAIL_STEP: u64 = if cfg!(test) { 61 } else { 4_096 };

/// The append itself, under the lock: decide, then truncate at most the
/// trailing record and append, or refuse with the file untouched.
fn append_locked(path: &Path, file: &mut File, body: &str) -> Result<AppendOutcome, AppendError> {
    let header = HEADER.as_bytes();
    let header_len = header.len() as u64;
    let len = file.metadata().map_err(io_error("stat", path))?.len();
    let corrupt = || {
        RawStoreError::Corrupt(format!(
            "copy ledger {}: missing header or unknown format version",
            path.display()
        ))
    };
    if len < header_len || read_at(file, path, 0, header.len())? != header {
        return Err(corrupt().into());
    }

    // Read back until the buffer holds four line ends, or reaches the
    // header: then it holds the last three complete lines whole (the last
    // two are checked, the one before supplies their predecessor's chain).
    let mut start = len;
    let mut tail = Vec::new();
    while start > header_len && tail.iter().filter(|&&b| b == b'\n').count() < 4 {
        let step = TAIL_STEP.min(start - header_len);
        start -= step;
        let mut chunk = read_at(file, path, start, step as usize)?;
        chunk.extend_from_slice(&tail);
        tail = chunk;
    }
    // Complete lines as (offset, bytes), the first partial one skipped
    // unless the buffer starts at the first record.
    let mut lines: Vec<(u64, &[u8])> = Vec::new();
    let mut cursor = if start == header_len {
        0
    } else {
        tail.iter()
            .position(|&b| b == b'\n')
            .map_or(tail.len(), |p| p + 1)
    };
    while let Some(n) = tail[cursor..].iter().position(|&b| b == b'\n') {
        lines.push((start + cursor as u64, &tail[cursor..cursor + n]));
        cursor += n + 1;
    }
    let partial = (tail.len() - cursor) as u64;

    let genesis = genesis().into_bytes();
    // The chain before line `i`, when it is known.
    let predecessor = |i: usize| -> Option<Vec<u8>> {
        if lines[i].0 == header_len {
            Some(genesis.clone())
        } else if i > 0 {
            split_line(lines[i - 1].1).map(|(_, chain)| chain.to_vec())
        } else {
            None
        }
    };
    let verifies = |i: usize| -> Option<Vec<u8>> {
        let previous = predecessor(i)?;
        verified_chain(&previous, lines[i].1).map(<[u8]>::to_vec)
    };
    let refuse = |offset: u64| {
        RawStoreError::Integrity(format!(
            "copy ledger {}: the hash chain fails at the record at byte {offset}, before the \
             last record; an append truncates only an incomplete trailing record (ADR-062 D7)",
            path.display()
        ))
    };

    let (keep, previous) = match lines.len() {
        0 => (header_len, genesis.clone()),
        count => {
            let last = count - 1;
            let last_chain = verifies(last);
            // The line before the last: verified unless it is the header.
            let before_ok = last == 0 || verifies(last - 1).is_some();
            if !before_ok {
                return Err(refuse(lines[last - 1].0).into());
            }
            match last_chain {
                Some(chain) => (len - partial, chain),
                // A complete last line that fails is the trailing record,
                // unless a partial tail follows it.
                None if partial == 0 => (
                    lines[last].0,
                    predecessor(last).expect("the line before the last verified"),
                ),
                None => return Err(refuse(lines[last].0).into()),
            }
        }
    };

    let chain = chain_of(&previous, body.as_bytes());
    let line = format!("{body} {chain}\n");
    let truncation = (len > keep).then_some(Truncation {
        bytes: len - keep,
        offset: keep,
    });
    if truncation.is_some() {
        file.set_len(keep).map_err(io_error("truncate", path))?;
        // From here on the truncation happened: every error carries it, so
        // the caller journals it even though the append failed.
        sync_truncation(file).map_err(|error| AppendError {
            error: io_error("sync the truncation of", path)(error),
            truncation,
        })?;
    }
    write_line(file, line.as_bytes()).map_err(|error| AppendError {
        error: RawStoreError::Io(format!("append to {}: {error}", path.display())),
        truncation,
    })?;
    Ok(AppendOutcome {
        truncation,
        offset: keep,
    })
}

#[cfg(test)]
thread_local! {
    /// Test hook: the next [`write_line`] on this thread fails.
    pub(crate) static FAIL_NEXT_WRITE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Test hook: the next [`sync_truncation`] on this thread fails.
    pub(crate) static FAIL_NEXT_TRUNCATE_SYNC: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// Fsyncs the ledger after a truncation.
fn sync_truncation(file: &File) -> std::io::Result<()> {
    #[cfg(test)]
    if FAIL_NEXT_TRUNCATE_SYNC.with(|fail| fail.replace(false)) {
        return Err(std::io::Error::other("injected fsync failure"));
    }
    file.sync_all()
}

/// Writes one line with one `write_all` and fsyncs it.
fn write_line(file: &mut File, line: &[u8]) -> std::io::Result<()> {
    #[cfg(test)]
    if FAIL_NEXT_WRITE.with(|fail| fail.replace(false)) {
        return Err(std::io::Error::other("injected write failure"));
    }
    file.write_all(line).and_then(|()| file.sync_all())
}

#[cfg(test)]
mod tests;
