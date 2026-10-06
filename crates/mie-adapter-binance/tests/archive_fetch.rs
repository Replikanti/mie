//! Archive downloads against scripted HTTP fakes and a fake clock: no
//! network, no real waiting.

use mie_adapter_binance::archive::fetch::{FetchError, FetchPolicy, Fetcher, for_each_line};
use mie_adapter_binance::transport::{Clock, HttpDownload, HttpGet};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const ZIP_URL: &str =
    "https://archive.invalid/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2026-09-30.zip";
const NAME: &str = "BTCUSDT-1d-2026-09-30.zip";
const CSV: &str = "BTCUSDT-1d-2026-09-30.csv";

/// A clock whose monotonic time only moves when someone sleeps.
#[derive(Default)]
struct FakeClock {
    now_ns: Mutex<u64>,
    sleeps: Mutex<Vec<Duration>>,
}

impl Clock for FakeClock {
    fn now_utc_ns(&self) -> i64 {
        *self.now_ns.lock().unwrap() as i64
    }

    fn monotonic_ns(&self) -> u64 {
        *self.now_ns.lock().unwrap()
    }

    fn sleep(&self, duration: Duration) {
        *self.now_ns.lock().unwrap() += duration.as_nanos() as u64;
        self.sleeps.lock().unwrap().push(duration);
    }
}

/// One scripted answer: a status with a body, or a transport failure.
type Answer = Result<(u16, Vec<u8>), String>;

/// Plays scripted answers in order and records when each request started.
struct Scripted<'a> {
    clock: &'a FakeClock,
    answers: Mutex<VecDeque<Answer>>,
    starts: Mutex<Vec<(u64, String)>>,
}

impl<'a> Scripted<'a> {
    fn new(clock: &'a FakeClock, answers: Vec<Answer>) -> Self {
        Self {
            clock,
            answers: Mutex::new(answers.into()),
            starts: Mutex::new(Vec::new()),
        }
    }

    fn next(&self, url: &str) -> Answer {
        self.starts
            .lock()
            .unwrap()
            .push((self.clock.monotonic_ns(), url.to_owned()));
        self.answers
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| panic!("unexpected request {url}"))
    }

    fn starts(&self) -> Vec<(u64, String)> {
        self.starts.lock().unwrap().clone()
    }
}

impl HttpGet for Scripted<'_> {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        self.next(url)
    }
}

impl HttpDownload for Scripted<'_> {
    fn download(&self, url: &str, sink: &mut dyn Write) -> Result<u16, String> {
        let (status, body) = self.next(url)?;
        if (200..300).contains(&status) {
            sink.write_all(&body).map_err(|e| e.to_string())?;
        }
        Ok(status)
    }
}

/// A directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mie-binance-archive-fetch-{tag}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn zip_of(entries: &[(&str, &[u8])], method: zip::CompressionMethod) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default().compression_method(method);
    for (name, data) in entries {
        writer.start_file(*name, options).unwrap();
        writer.write_all(data).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn checksum_body(zip: &[u8]) -> Vec<u8> {
    format!("{}  {NAME}\n", sha256(zip)).into_bytes()
}

fn lines(path: &Path) -> Result<Vec<String>, FetchError> {
    let mut out = Vec::new();
    for_each_line(path, CSV, |line| {
        out.push(String::from_utf8(line.to_vec()).unwrap());
        Ok::<(), FetchError>(())
    })?;
    Ok(out)
}

#[test]
fn an_unpublished_file_is_missing_not_an_error() {
    let clock = FakeClock::default();
    let http = Scripted::new(&clock, vec![Ok((404, b"Not Found".to_vec()))]);
    let mut fetcher = Fetcher::new(&http, &http, &clock, FetchPolicy::default());
    assert_eq!(fetcher.checksum(ZIP_URL, NAME), Ok(None));
    assert_eq!(http.starts()[0].1, format!("{ZIP_URL}.CHECKSUM"));
    assert_eq!(fetcher.requests(), 1);
}

#[test]
fn a_verified_download_streams_through_and_its_lines_come_back_verbatim() {
    let dir = TempDir::new("ok");
    let csv = b"open_time,open\r\n1,2\n3,4.50\n5,6";
    let zip = zip_of(&[(CSV, csv)], zip::CompressionMethod::Deflated);
    let clock = FakeClock::default();
    let http = Scripted::new(
        &clock,
        vec![Ok((200, checksum_body(&zip))), Ok((200, zip.clone()))],
    );
    let mut fetcher = Fetcher::new(&http, &http, &clock, FetchPolicy::default());
    let digest = fetcher.checksum(ZIP_URL, NAME).unwrap().unwrap();
    let dest = dir.path().join(NAME);
    assert_eq!(
        fetcher.download_verified(ZIP_URL, &dest, &digest),
        Ok(zip.len() as u64)
    );
    assert_eq!(fetcher.downloads(), 1);
    assert_eq!(
        lines(&dest).unwrap(),
        ["open_time,open", "1,2", "3,4.50", "5,6"]
    );
}

#[test]
fn a_hash_mismatch_rejects_and_removes_the_download() {
    let dir = TempDir::new("mismatch");
    let zip = zip_of(&[(CSV, b"a,b\n")], zip::CompressionMethod::Deflated);
    let other = zip_of(&[(CSV, b"a,c\n")], zip::CompressionMethod::Deflated);
    let clock = FakeClock::default();
    let http = Scripted::new(&clock, vec![Ok((200, other))]);
    let mut fetcher = Fetcher::new(&http, &http, &clock, FetchPolicy::default());
    let dest = dir.path().join(NAME);
    let error = fetcher
        .download_verified(ZIP_URL, &dest, &sha256(&zip))
        .expect_err("mismatch");
    assert!(matches!(error, FetchError::Mismatch { .. }), "{error}");
    assert!(!dest.exists());
}

#[test]
fn throttling_and_server_errors_back_off_exponentially() {
    let dir = TempDir::new("backoff");
    let zip = zip_of(&[(CSV, b"a\n")], zip::CompressionMethod::Deflated);
    let clock = FakeClock::default();
    let http = Scripted::new(
        &clock,
        vec![
            Ok((429, Vec::new())),
            Ok((503, Vec::new())),
            Ok((200, zip.clone())),
        ],
    );
    let mut fetcher = Fetcher::new(&http, &http, &clock, FetchPolicy::default());
    let dest = dir.path().join(NAME);
    fetcher
        .download_verified(ZIP_URL, &dest, &sha256(&zip))
        .unwrap();
    assert_eq!(
        *clock.sleeps.lock().unwrap(),
        [Duration::from_secs(1), Duration::from_secs(2)]
    );
    assert_eq!(fetcher.downloads(), 3);
}

#[test]
fn retries_stop_after_max_attempts_and_the_backoff_is_capped() {
    let clock = FakeClock::default();
    let http = Scripted::new(
        &clock,
        vec![
            Err("connection reset".to_owned()),
            Ok((418, Vec::new())),
            Ok((500, Vec::new())),
            Ok((403, Vec::new())),
        ],
    );
    let policy = FetchPolicy {
        max_attempts: 4,
        backoff_initial: Duration::from_secs(1),
        backoff_max: Duration::from_secs(3),
        ..FetchPolicy::default()
    };
    let mut fetcher = Fetcher::new(&http, &http, &clock, policy);
    let error = fetcher.checksum(ZIP_URL, NAME).expect_err("gives up");
    assert!(
        matches!(error, FetchError::Status { status: 403, .. }),
        "{error}"
    );
    assert_eq!(fetcher.requests(), 4);
    assert_eq!(
        *clock.sleeps.lock().unwrap(),
        [
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(3)
        ]
    );
}

#[test]
fn other_statuses_fail_at_once() {
    let clock = FakeClock::default();
    let http = Scripted::new(&clock, vec![Ok((400, Vec::new()))]);
    let mut fetcher = Fetcher::new(&http, &http, &clock, FetchPolicy::default());
    assert!(matches!(
        fetcher.checksum(ZIP_URL, NAME),
        Err(FetchError::Status { status: 400, .. })
    ));
    assert_eq!(fetcher.requests(), 1);
}

#[test]
fn requests_are_paced_by_their_start_times() {
    let clock = FakeClock::default();
    let body = format!("{}  {NAME}\n", "ab".repeat(32)).into_bytes();
    let http = Scripted::new(
        &clock,
        vec![
            Ok((200, body.clone())),
            Ok((200, body.clone())),
            Ok((404, Vec::new())),
        ],
    );
    let policy = FetchPolicy {
        request_interval: Duration::from_millis(250),
        ..FetchPolicy::default()
    };
    let mut fetcher = Fetcher::new(&http, &http, &clock, policy);
    for _ in 0..3 {
        fetcher.checksum(ZIP_URL, NAME).unwrap();
    }
    let starts: Vec<u64> = http.starts().into_iter().map(|(t, _)| t).collect();
    assert_eq!(starts.len(), 3);
    for pair in starts.windows(2) {
        assert!(pair[1] - pair[0] >= 250_000_000, "{starts:?}");
    }
}

#[test]
fn a_corrupt_entry_fails_its_crc_check() {
    let dir = TempDir::new("crc");
    let csv = b"open_time,open\n1,2\n3,4\n";
    let mut zip = zip_of(&[(CSV, csv)], zip::CompressionMethod::Stored);
    // Flip one stored data byte: "3,4" becomes "3,5".
    let at = zip
        .windows(3)
        .position(|w| w == b"3,4")
        .expect("stored data is literal");
    zip[at + 2] = b'5';
    let path = dir.path().join(NAME);
    std::fs::write(&path, &zip).unwrap();
    let error = lines(&path).expect_err("bad CRC");
    assert!(matches!(error, FetchError::Zip(_)), "{error}");
}

#[test]
fn a_zip_with_another_entry_layout_is_rejected() {
    let dir = TempDir::new("layout");
    let path = dir.path().join(NAME);
    for entries in [
        vec![("other.csv", b"a\n".as_slice())],
        vec![(CSV, b"a\n".as_slice()), ("extra.csv", b"b\n".as_slice())],
    ] {
        std::fs::write(&path, zip_of(&entries, zip::CompressionMethod::Deflated)).unwrap();
        assert!(matches!(lines(&path), Err(FetchError::Zip(_))));
    }
    std::fs::write(&path, b"not a zip").unwrap();
    assert!(matches!(lines(&path), Err(FetchError::Zip(_))));
}
