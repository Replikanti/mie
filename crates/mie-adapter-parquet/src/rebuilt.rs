//! The post-rebuild removal gate of a tiered raw store (ADR-062 D7 step 6,
//! #193 B10).
//!
//! `mie store rebuild` installs a copy ledger that no copy target has
//! verified yet, so the hot removal must wait for the next full
//! verification. The gate is a small file on hot, `<raw_root>/.mie/rebuilt`:
//!
//! ```text
//! mie-rebuilt 1
//! prefix <L>
//! ```
//!
//! L is the installed ledger's length in bytes, a canonical decimal. The
//! hot removal's plan ([`removal::plan`](crate::removal::plan)) plans no
//! candidate until every copy target has a `verify` record whose prefix is
//! at least L. Why a file and not a ledger record kind: a new kind is a
//! format change (header v2 under every existing ledger), and the gate
//! needs no history (#193 STOP 1, decision 3).
//!
//! The rebuild writes the gate before it renames the new ledger into place
//! (temp name, fsync, rename, directory fsync), and the planner reads the
//! ledger first and the gate second, so a planner that sees the rebuilt
//! ledger always finds its gate. A gate next to an older ledger only makes
//! removal wait. The parser is strict: anything [`render`] cannot produce
//! is [`RawStoreError::Corrupt`].

use crate::layout::{META_DIR, REBUILT};
use crate::{io_error, sync_dir};
use mie_ports::raw::RawStoreError;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// The gate's first line: the format name and version.
const HEADER: &str = "mie-rebuilt 1\n";

/// The gate's path on the store rooted at `raw_root`.
pub fn gate_path(raw_root: &Path) -> PathBuf {
    raw_root.join(META_DIR).join(REBUILT)
}

/// The gate's text for a rebuilt ledger of `len` bytes.
pub fn render(len: u64) -> String {
    format!("{HEADER}prefix {len}\n")
}

/// Parses a gate strictly: exactly what [`render`] writes.
///
/// # Errors
///
/// [`RawStoreError::Corrupt`] for any other bytes.
pub fn parse(bytes: &[u8]) -> Result<u64, RawStoreError> {
    let corrupt = || {
        RawStoreError::Corrupt(format!(
            "the rebuild gate {:?} is not `mie-rebuilt 1` with one canonical `prefix` line \
             (ADR-062 D7)",
            String::from_utf8_lossy(bytes)
        ))
    };
    let number = bytes
        .strip_prefix(HEADER.as_bytes())
        .and_then(|rest| rest.strip_prefix(b"prefix "))
        .and_then(|rest| rest.strip_suffix(b"\n"))
        .ok_or_else(corrupt)?;
    let canonical = !number.is_empty()
        && number.iter().all(u8::is_ascii_digit)
        && (number == b"0" || number[0] != b'0');
    if !canonical {
        return Err(corrupt());
    }
    std::str::from_utf8(number)
        .ok()
        .and_then(|digits| digits.parse().ok())
        .ok_or_else(corrupt)
}

/// The gate of the store rooted at `raw_root`, `None` when there is none.
///
/// # Errors
///
/// [`RawStoreError::Corrupt`] for a gate [`parse`] refuses;
/// [`RawStoreError::Io`] when it cannot be read.
pub fn read(raw_root: &Path) -> Result<Option<u64>, RawStoreError> {
    let path = gate_path(raw_root);
    match fs::read(&path) {
        Ok(bytes) => parse(&bytes)
            .map(Some)
            .map_err(|error| RawStoreError::Corrupt(format!("{}: {error}", path.display()))),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("read", &path)(error)),
    }
}

/// Writes the gate for a rebuilt ledger of `len` bytes: a temp name in
/// `.mie/`, fsync, rename over the gate, directory fsync.
///
/// # Errors
///
/// [`RawStoreError::Io`] on any I/O failure; an existing gate is then
/// unchanged.
pub fn write(raw_root: &Path, len: u64) -> Result<(), RawStoreError> {
    let meta = raw_root.join(META_DIR);
    let tmp = meta.join(format!(".{REBUILT}.tmp"));
    let gate = gate_path(raw_root);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .map_err(io_error("create", &tmp))?;
    file.write_all(render(len).as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(io_error("write", &tmp))?;
    drop(file);
    fs::rename(&tmp, &gate).map_err(io_error("rename", &tmp))?;
    sync_dir(&meta)
}
