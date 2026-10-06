//! Polite, verified downloads from the archive (ADR-034 D6).
//!
//! - One request at a time, at least [`FetchPolicy::request_interval`]
//!   between request starts.
//! - 403, 418, 429, 5xx and transport errors are retried with an
//!   exponential backoff ([`FetchPolicy::backoff_initial`] doubling up to
//!   [`FetchPolicy::backoff_max`]), at most [`FetchPolicy::max_attempts`]
//!   attempts per request. Any other status fails at once.
//! - A zip is streamed into a staging file while its SHA-256 is computed,
//!   and accepted only when that equals the published `.CHECKSUM`; a
//!   mismatch deletes the file.
//! - The single CSV entry is read with its CRC checked at the end of the
//!   entry, line by line, with the line terminator stripped and the bytes
//!   otherwise verbatim.
//!
//! All waiting goes through the [`Clock`] seam, so tests run on a fake
//! clock.

use crate::transport::{Clock, HttpDownload, HttpGet};
use mie_ports::raw::is_sha256_hex;
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Duration;

/// Request pacing and retries (ADR-034 D6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchPolicy {
    /// Least time between the starts of two requests.
    pub request_interval: Duration,
    /// Most attempts per request, the first included. At least 1.
    pub max_attempts: u32,
    /// Wait after the first failed attempt.
    pub backoff_initial: Duration,
    /// Longest wait between attempts.
    pub backoff_max: Duration,
}

impl Default for FetchPolicy {
    fn default() -> Self {
        Self {
            request_interval: Duration::from_millis(100),
            max_attempts: 5,
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
        }
    }
}

/// Why a fetch failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    /// The server answered with a status that is not retried, or kept
    /// answering with a retryable one until the attempts ran out.
    Status {
        /// The requested URL.
        url: String,
        /// The last status.
        status: u16,
    },
    /// The transport failed on every attempt.
    Transport {
        /// The requested URL.
        url: String,
        /// The last failure.
        detail: String,
    },
    /// A `.CHECKSUM` body is malformed or names another file.
    Checksum(String),
    /// A download's SHA-256 differs from the published one.
    Mismatch {
        /// The published digest.
        expected: String,
        /// The digest of the downloaded bytes.
        actual: String,
    },
    /// The zip is malformed, holds anything but the one expected entry, or
    /// its CRC does not match.
    Zip(String),
    /// A staging file could not be written or read.
    Io(String),
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status { url, status } => write!(f, "GET {url}: HTTP {status}"),
            Self::Transport { url, detail } => write!(f, "GET {url}: {detail}"),
            Self::Checksum(detail) => write!(f, "checksum file: {detail}"),
            Self::Mismatch { expected, actual } => {
                write!(
                    f,
                    "SHA-256 {actual} does not match the published {expected}"
                )
            }
            Self::Zip(detail) => write!(f, "zip: {detail}"),
            Self::Io(detail) => write!(f, "staging: {detail}"),
        }
    }
}

impl std::error::Error for FetchError {}

/// Whether a status is worth another attempt: access throttling (403,
/// 418, 429) and server errors.
fn retryable(status: u16) -> bool {
    matches!(status, 403 | 418 | 429) || (500..600).contains(&status)
}

/// Parses a `.CHECKSUM` body, `<sha256>  <file name>` with an optional
/// trailing newline, and returns the digest.
///
/// # Errors
///
/// [`FetchError::Checksum`] when the body has another shape, the digest is
/// not 64 lowercase hex characters, or it names a file other than
/// `file_name`.
pub fn parse_checksum(body: &[u8], file_name: &str) -> Result<String, FetchError> {
    let text =
        std::str::from_utf8(body).map_err(|_| FetchError::Checksum("not UTF-8".to_owned()))?;
    let line = text.strip_suffix('\n').unwrap_or(text);
    let (digest, name) = line
        .split_once("  ")
        .ok_or_else(|| FetchError::Checksum(format!("{line:?} is not `<sha256>  <name>`")))?;
    if !is_sha256_hex(digest) {
        return Err(FetchError::Checksum(format!(
            "{digest:?} is not 64 lowercase hex characters"
        )));
    }
    if name != file_name {
        return Err(FetchError::Checksum(format!(
            "names {name:?}, expected {file_name:?}"
        )));
    }
    Ok(digest.to_owned())
}

/// Hashes and counts every byte on its way to the staging file.
struct HashingFile {
    file: File,
    hasher: Sha256,
    bytes: u64,
}

impl Write for HashingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Lowercase hex of a digest.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// Paced, retried requests against the archive.
pub struct Fetcher<'a> {
    http: &'a dyn HttpGet,
    download: &'a dyn HttpDownload,
    clock: &'a dyn Clock,
    policy: FetchPolicy,
    last_start_ns: Option<u64>,
    requests: u64,
    downloads: u64,
}

impl fmt::Debug for Fetcher<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fetcher")
            .field("policy", &self.policy)
            .field("requests", &self.requests)
            .field("downloads", &self.downloads)
            .finish_non_exhaustive()
    }
}

/// One attempt's outcome before retry classification.
enum Attempt<T> {
    Done(T),
    Status(u16),
    Transport(String),
}

impl<'a> Fetcher<'a> {
    /// A fetcher over the given seams. A `max_attempts` of 0 counts as 1.
    pub fn new(
        http: &'a dyn HttpGet,
        download: &'a dyn HttpDownload,
        clock: &'a dyn Clock,
        policy: FetchPolicy,
    ) -> Self {
        Self {
            http,
            download,
            clock,
            policy,
            last_start_ns: None,
            requests: 0,
            downloads: 0,
        }
    }

    /// Requests started so far, retries included.
    pub fn requests(&self) -> u64 {
        self.requests
    }

    /// Zip downloads started so far, retries included.
    pub fn downloads(&self) -> u64 {
        self.downloads
    }

    /// Waits until the request interval since the last request start has
    /// passed, then marks a new start.
    fn pace(&mut self) {
        let interval = u64::try_from(self.policy.request_interval.as_nanos()).unwrap_or(u64::MAX);
        if let Some(last) = self.last_start_ns {
            let next = last.saturating_add(interval);
            let now = self.clock.monotonic_ns();
            if now < next {
                self.clock.sleep(Duration::from_nanos(next - now));
            }
        }
        self.last_start_ns = Some(self.clock.monotonic_ns());
        self.requests += 1;
    }

    /// Runs `attempt` until it succeeds, fails for good, or the attempts
    /// run out.
    fn retry<T>(
        &mut self,
        url: &str,
        mut attempt: impl FnMut(&mut Self) -> Attempt<T>,
    ) -> Result<T, FetchError> {
        let attempts = self.policy.max_attempts.max(1);
        let mut backoff = self.policy.backoff_initial;
        for number in 1..=attempts {
            self.pace();
            let error = match attempt(self) {
                Attempt::Done(value) => return Ok(value),
                Attempt::Status(status) if !retryable(status) => {
                    return Err(FetchError::Status {
                        url: url.to_owned(),
                        status,
                    });
                }
                Attempt::Status(status) => FetchError::Status {
                    url: url.to_owned(),
                    status,
                },
                Attempt::Transport(detail) => FetchError::Transport {
                    url: url.to_owned(),
                    detail,
                },
            };
            if number == attempts {
                return Err(error);
            }
            self.clock.sleep(backoff);
            backoff = (backoff * 2).min(self.policy.backoff_max);
        }
        unreachable!("the last attempt returns")
    }

    /// Fetches the published SHA-256 of `zip_url` from `<zip_url>.CHECKSUM`;
    /// `None` when the file is not published (404).
    ///
    /// # Errors
    ///
    /// [`FetchError`] when the request fails or the body is malformed.
    pub fn checksum(
        &mut self,
        zip_url: &str,
        file_name: &str,
    ) -> Result<Option<String>, FetchError> {
        let url = format!("{zip_url}.CHECKSUM");
        let http = self.http;
        let body = self.retry(&url, |_| match http.get(&url) {
            Ok((404, _)) => Attempt::Done(None),
            Ok((status, body)) if (200..300).contains(&status) => Attempt::Done(Some(body)),
            Ok((status, _)) => Attempt::Status(status),
            Err(detail) => Attempt::Transport(detail),
        })?;
        body.map(|body| parse_checksum(&body, file_name))
            .transpose()
    }

    /// Downloads `url` to `dest` and keeps it only when its SHA-256 equals
    /// `expected`. Returns the size in bytes.
    ///
    /// # Errors
    ///
    /// [`FetchError`] when the download fails or the digest differs; `dest`
    /// is removed either way.
    pub fn download_verified(
        &mut self,
        url: &str,
        dest: &Path,
        expected: &str,
    ) -> Result<u64, FetchError> {
        let download = self.download;
        let result = self.retry(url, |fetcher| {
            fetcher.downloads += 1;
            let file = match File::create(dest) {
                Ok(file) => file,
                Err(e) => {
                    return Attempt::Done(Err(FetchError::Io(format!(
                        "create {}: {e}",
                        dest.display()
                    ))));
                }
            };
            let mut sink = HashingFile {
                file,
                hasher: Sha256::new(),
                bytes: 0,
            };
            match download.download(url, &mut sink) {
                Ok(status) if (200..300).contains(&status) => {
                    let synced = sink
                        .file
                        .sync_all()
                        .map_err(|e| FetchError::Io(format!("sync {}: {e}", dest.display())));
                    Attempt::Done(synced.map(|()| (hex(&sink.hasher.finalize()), sink.bytes)))
                }
                Ok(status) => Attempt::Status(status),
                Err(detail) => Attempt::Transport(detail),
            }
        });
        let verified = result.and_then(|inner| inner).and_then(|(actual, bytes)| {
            if actual == expected {
                Ok(bytes)
            } else {
                Err(FetchError::Mismatch {
                    expected: expected.to_owned(),
                    actual,
                })
            }
        });
        if verified.is_err() {
            let _ = fs::remove_file(dest);
        }
        verified
    }
}

/// Calls `line` with every line of the zip's single entry `entry_name`, in
/// order, with the `\n` or `\r\n` terminator stripped. The entry's CRC is
/// checked when its end is reached, so a corrupt entry fails before
/// `for_each_line` returns `Ok`.
///
/// # Errors
///
/// [`FetchError::Zip`] when the zip is malformed, has another entry layout
/// or fails its CRC; the first error `line` returns, converted.
pub fn for_each_line<E>(
    zip_path: &Path,
    entry_name: &str,
    mut line: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E>
where
    E: From<FetchError>,
{
    let file = File::open(zip_path)
        .map_err(|e| FetchError::Io(format!("open {}: {e}", zip_path.display())))?;
    let zip_error = |e: &dyn fmt::Display| FetchError::Zip(format!("{}: {e}", zip_path.display()));
    let mut archive = zip::ZipArchive::new(file).map_err(|e| zip_error(&e))?;
    if archive.len() != 1 {
        return Err(FetchError::Zip(format!(
            "{} holds {} entries, expected exactly {entry_name}",
            zip_path.display(),
            archive.len()
        ))
        .into());
    }
    let entry = archive.by_index(0).map_err(|e| zip_error(&e))?;
    if entry.name() != entry_name {
        return Err(FetchError::Zip(format!(
            "{} holds {:?}, expected {entry_name:?}",
            zip_path.display(),
            entry.name()
        ))
        .into());
    }
    let mut reader = BufReader::with_capacity(1 << 20, entry);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        let read = reader
            .read_until(b'\n', &mut buffer)
            .map_err(|e| zip_error(&e))?;
        if read == 0 {
            return Ok(());
        }
        let mut row = buffer.as_slice();
        if let Some(rest) = row.strip_suffix(b"\n") {
            row = rest.strip_suffix(b"\r").unwrap_or(rest);
        }
        line(row)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NAME: &str = "BTCUSDT-1d-2026-09-30.zip";
    const DIGEST: &str = "00977e0f5d445a8f5f8e2eb6ac5667d3e3a47a5ec13f90e1e16a35a6e0ca52ed";

    #[test]
    fn checksum_bodies_parse_with_or_without_a_newline() {
        let body = format!("{DIGEST}  {NAME}\n");
        assert_eq!(parse_checksum(body.as_bytes(), NAME), Ok(DIGEST.to_owned()));
        let body = format!("{DIGEST}  {NAME}");
        assert_eq!(parse_checksum(body.as_bytes(), NAME), Ok(DIGEST.to_owned()));
    }

    #[test]
    fn checksum_bodies_naming_another_file_are_rejected() {
        let body = format!("{DIGEST}  BTCUSDT-1d-2026-09-29.zip\n");
        assert!(matches!(
            parse_checksum(body.as_bytes(), NAME),
            Err(FetchError::Checksum(_))
        ));
    }

    #[test]
    fn checksum_bodies_with_bad_hex_or_shape_are_rejected() {
        for body in [
            format!("{}  {NAME}\n", DIGEST.to_ascii_uppercase()),
            format!("{}  {NAME}\n", &DIGEST[..63]),
            format!("{}g  {NAME}\n", &DIGEST[..63]),
            format!("{DIGEST} {NAME}\n"),
            format!("{DIGEST}\n"),
            String::new(),
        ] {
            assert!(
                matches!(
                    parse_checksum(body.as_bytes(), NAME),
                    Err(FetchError::Checksum(_))
                ),
                "{body:?}"
            );
        }
    }

    #[test]
    fn only_throttling_and_server_errors_are_retried() {
        for status in [403, 418, 429, 500, 502, 503, 599] {
            assert!(retryable(status), "{status}");
        }
        for status in [200, 301, 400, 401, 404, 410] {
            assert!(!retryable(status), "{status}");
        }
    }

    #[test]
    fn hex_is_lowercase_and_padded() {
        assert_eq!(hex(&[0x00, 0x0f, 0xab, 0xff]), "000fabff");
    }
}
