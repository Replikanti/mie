//! The re-admit row check (#210 RA6) over an in-memory store and a fake
//! archive: positional per-day comparison, both-way counts, and refusals
//! that leave nothing in staging.

use super::*;
use crate::archive::fetch::FetchPolicy;
use crate::archive::normalize::record_time;
use crate::testing::MemorySource;
use crate::transport::{Clock, HttpDownload, HttpGet};
use sha2::{Digest, Sha256};
use std::io::{Cursor, Write};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

const BASE: &str = "https://archive.invalid";
const SYMBOL: &str = "BTCUSDT";

/// Serves zips and their `.CHECKSUM`s; a `.CHECKSUM` override serves
/// another digest (a corrupt download).
#[derive(Default)]
struct Fake {
    zips: Mutex<BTreeMap<String, Vec<u8>>>,
    checksum: Mutex<Option<String>>,
}

impl Fake {
    fn publish(&self, stream: ArchiveStream, period: Period, csv: &str) {
        let path = stream.archive_path(SYMBOL, period);
        let entry = stream.file_name(SYMBOL, period).replace(".zip", ".csv");
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        writer.start_file(entry, options).unwrap();
        writer.write_all(csv.as_bytes()).unwrap();
        let zip = writer.finish().unwrap().into_inner();
        self.zips
            .lock()
            .unwrap()
            .insert(format!("{BASE}/{path}"), zip);
    }
}

impl HttpGet for Fake {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        let zip_url = url.strip_suffix(".CHECKSUM").unwrap();
        let name = zip_url.rsplit('/').next().unwrap();
        Ok(match self.zips.lock().unwrap().get(zip_url) {
            Some(zip) => {
                let sha = self.checksum.lock().unwrap().clone().unwrap_or_else(|| {
                    Sha256::digest(zip)
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect()
                });
                (200, format!("{sha}  {name}\n").into_bytes())
            }
            None => (404, b"Not Found".to_vec()),
        })
    }
}

impl HttpDownload for Fake {
    fn download(&self, url: &str, sink: &mut dyn Write) -> Result<u16, String> {
        match self.zips.lock().unwrap().get(url) {
            Some(zip) => {
                sink.write_all(zip).map_err(|e| e.to_string())?;
                Ok(200)
            }
            None => Ok(404),
        }
    }
}

struct NoClock;

impl Clock for NoClock {
    fn now_utc_ns(&self) -> i64 {
        0
    }
    fn monotonic_ns(&self) -> u64 {
        0
    }
    fn sleep(&self, _: Duration) {}
}

/// A unique staging directory, removed on drop.
struct Staging(PathBuf);

impl Staging {
    fn new(tag: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "mie-readmit-{tag}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }

    fn is_empty(&self) -> bool {
        match fs::read_dir(&self.0) {
            Ok(mut entries) => entries.next().is_none(),
            Err(_) => true,
        }
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn day(date: &str) -> Period {
    Period::Day(parse_day(date).unwrap())
}

/// Hourly metrics of 2026-10-03 plus the 23:55 sample, ordered at the next
/// midnight (ADR-034 D3): 24 rows on 2026-10-03, 1 on 2026-10-04.
fn metrics_csv() -> String {
    let mut csv = format!("{}\n", ArchiveStream::Metrics.expected_header());
    for hour in 0..24 {
        csv.push_str(&format!(
            "2026-10-03 {hour:02}:00:00,{SYMBOL},{}.1230000000000000,1.0,1.0,1.0,1.0,1.0\n",
            90_000 + hour
        ));
    }
    csv.push_str(&format!(
        "2026-10-03 23:55:00,{SYMBOL},90100.0000000000000000,1.0,1.0,1.0,1.0,1.0\n"
    ));
    csv
}

/// The records of every data line of `csv`, as the importer stores them.
fn records(stream: ArchiveStream, csv: &str) -> Vec<RawRecord> {
    csv.lines()
        .filter(|line| *line != stream.expected_header())
        .map(|line| RawRecord {
            event_time: record_time(stream, line.as_bytes()).unwrap(),
            capture: None,
            payload: line.as_bytes().to_vec(),
        })
        .collect()
}

fn sealed(source: &mut MemorySource, stream: ArchiveStream, records: Vec<RawRecord>) -> SealedFile {
    source.seal(ARCHIVE_SOURCE, stream.raw_name(), records);
    let path = source.path_at(source.file_count() - 1);
    source.file(&path)
}

fn run(
    fake: &Fake,
    source: &MemorySource,
    staging: &Path,
    stream: ArchiveStream,
    period: Period,
    parts: Vec<SealedFile>,
) -> Result<Checked, ReadmitError> {
    let mut fetcher = Fetcher::new(fake, fake, &NoClock, FetchPolicy::default());
    let mut parts = parts;
    parts.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    check(
        &mut fetcher,
        source,
        &ReadmitRequest {
            symbol: SYMBOL.to_owned(),
            base_url: BASE.to_owned(),
            staging_dir: staging.to_owned(),
            stream,
            period,
            parts,
        },
    )
}

const METRICS: &str = "data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-10-03.zip";

/// The metrics fixture: day 2026-10-03 in two parts (12 + 12 rows, so the
/// path order of one day's parts counts), 2026-10-04 in one.
fn metrics_store(
    edit: impl FnOnce(&mut Vec<RawRecord>, &mut Vec<RawRecord>, &mut Vec<RawRecord>),
) -> (MemorySource, Vec<SealedFile>) {
    let mut all = records(ArchiveStream::Metrics, &metrics_csv());
    let next_day = all.split_off(24);
    let second = all.split_off(12);
    let (mut a, mut b, mut c) = (all, second, next_day);
    edit(&mut a, &mut b, &mut c);
    let mut source = MemorySource::default();
    let mut parts = Vec::new();
    for records in [a, b, c] {
        if !records.is_empty() {
            parts.push(sealed(&mut source, ArchiveStream::Metrics, records));
        }
    }
    (source, parts)
}

#[track_caller]
fn difference(result: Result<Checked, ReadmitError>) -> String {
    match result.unwrap().outcome {
        Err(difference) => difference,
        Ok(entry) => panic!("matched: {entry:?}"),
    }
}

#[test]
fn rows_equal_all_rows_per_day_in_part_order() {
    let fake = Fake::default();
    fake.publish(ArchiveStream::Metrics, day("2026-10-03"), &metrics_csv());
    let (source, parts) = metrics_store(|_, _, _| {});
    let staging = Staging::new("equal");
    let checked = run(
        &fake,
        &source,
        &staging.0,
        ArchiveStream::Metrics,
        day("2026-10-03"),
        parts.clone(),
    )
    .unwrap();
    assert_eq!(checked.rows, 25);
    let mut files: Vec<LedgerFile> = parts
        .iter()
        .map(|part| LedgerFile {
            relative_path: part.relative_path.clone(),
            rows: part.rows,
            sha256: part.sha256.clone(),
        })
        .collect();
    files.sort();
    let expected = ImportLedger {
        archive: METRICS.to_owned(),
        sha256: checked.published.clone(),
        stream: RawStreamKey::new(ARCHIVE_SOURCE, SYMBOL, "metrics").unwrap(),
        period: day("2026-10-03"),
        header: Some(ArchiveStream::Metrics.expected_header().to_owned()),
        rows: 25,
        files,
    };
    assert_eq!(checked.outcome, Ok(expected));
    assert!(staging.is_empty());

    // The two parts of one day swapped in the check set's records: the
    // concatenation in path order no longer equals the file.
    let (source, parts) = metrics_store(|a, b, _| std::mem::swap(a, b));
    let found = difference(run(
        &fake,
        &source,
        &staging.0,
        ArchiveStream::Metrics,
        day("2026-10-03"),
        parts,
    ));
    assert!(found.starts_with("line 2 of "), "{found}");
}

#[test]
fn the_first_and_the_last_row_are_checked() {
    let fake = Fake::default();
    fake.publish(ArchiveStream::Metrics, day("2026-10-03"), &metrics_csv());
    let staging = Staging::new("ends");
    let (source, parts) = metrics_store(|a, _, _| a[0].payload.push(b'0'));
    let first = parts[0].relative_path.clone();
    assert_eq!(
        difference(run(
            &fake,
            &source,
            &staging.0,
            ArchiveStream::Metrics,
            day("2026-10-03"),
            parts
        )),
        format!("line 2 of {METRICS} (2026-10-03, row 0 of the day) differs from {first} record 0")
    );
    let (source, parts) = metrics_store(|_, _, c| {
        c[0].event_time = EventTime::from_millis(c[0].event_time.as_millis() + 1)
    });
    let last = parts[2].relative_path.clone();
    assert_eq!(
        difference(run(
            &fake,
            &source,
            &staging.0,
            ArchiveStream::Metrics,
            day("2026-10-03"),
            parts
        )),
        format!("line 26 of {METRICS} (2026-10-04, row 0 of the day) differs from {last} record 0")
    );
    assert!(staging.is_empty());
}

#[test]
fn missing_or_extra_records_are_a_difference() {
    let fake = Fake::default();
    fake.publish(ArchiveStream::Metrics, day("2026-10-03"), &metrics_csv());
    let staging = Staging::new("counts");
    // 23 stored against 24.
    let (source, parts) = metrics_store(|_, b, _| {
        b.pop();
    });
    assert_eq!(
        difference(run(
            &fake,
            &source,
            &staging.0,
            ArchiveStream::Metrics,
            day("2026-10-03"),
            parts
        )),
        format!(
            "line 25 of {METRICS} (2026-10-03, row 23 of the day) has no stored record (the held \
             parts of 2026-10-03 hold 23)"
        )
    );
    // 25 stored against 24.
    let (source, parts) = metrics_store(|_, b, _| {
        let mut extra = b[11].clone();
        extra.payload.push(b'9');
        b.push(extra);
    });
    let second = parts[1].relative_path.clone();
    assert_eq!(
        difference(run(
            &fake,
            &source,
            &staging.0,
            ArchiveStream::Metrics,
            day("2026-10-03"),
            parts
        )),
        format!("{second} record 12 (2026-10-03) has no row in {METRICS} (24 rows that day)")
    );
}

fn agg_csv(rows: &[(u64, i64)]) -> String {
    let mut csv = format!("{}\n", ArchiveStream::AggTrades.expected_header());
    for (id, time) in rows {
        csv.push_str(&format!("{id},85000.10,0.010,{id},{id},{time},false\n"));
    }
    csv
}

fn at(date: &str, ms: i64) -> i64 {
    parse_day(date).unwrap() * DAY_MS + ms
}

const AGG: &str = "data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-03.zip";

#[test]
fn equal_times_compare_in_file_order() {
    let t = at("2026-10-03", 3_600_000);
    let csv = agg_csv(&[(1, t), (2, t), (3, t + 1)]);
    let fake = Fake::default();
    fake.publish(ArchiveStream::AggTrades, day("2026-10-03"), &csv);
    let staging = Staging::new("equal-times");
    let mut stored = records(ArchiveStream::AggTrades, &csv);
    stored.swap(0, 1);
    let mut source = MemorySource::default();
    let part = sealed(&mut source, ArchiveStream::AggTrades, stored);
    assert_eq!(
        difference(run(
            &fake,
            &source,
            &staging.0,
            ArchiveStream::AggTrades,
            day("2026-10-03"),
            vec![part.clone()]
        )),
        format!(
            "line 2 of {AGG} (2026-10-03, row 0 of the day) differs from {} record 0",
            part.relative_path
        )
    );
}

#[test]
fn days_must_match_both_ways() {
    let csv = agg_csv(&[(1, at("2026-10-03", 5)), (2, at("2026-10-03", 6))]);
    let fake = Fake::default();
    fake.publish(ArchiveStream::AggTrades, day("2026-10-03"), &csv);
    let staging = Staging::new("days");
    // A part on a day without rows.
    let mut source = MemorySource::default();
    let good = sealed(
        &mut source,
        ArchiveStream::AggTrades,
        records(ArchiveStream::AggTrades, &csv),
    );
    let stray = sealed(
        &mut source,
        ArchiveStream::AggTrades,
        records(
            ArchiveStream::AggTrades,
            &agg_csv(&[(9, at("2026-10-02", 5))]),
        ),
    );
    assert_eq!(
        difference(run(
            &fake,
            &source,
            &staging.0,
            ArchiveStream::AggTrades,
            day("2026-10-03"),
            vec![good, stray.clone()]
        )),
        format!(
            "{} record 0 (2026-10-02) has no row in {AGG} (0 rows that day)",
            stray.relative_path
        )
    );
    // Rows on a day without a part: the metrics 23:55 row without its part.
    fake.publish(ArchiveStream::Metrics, day("2026-10-03"), &metrics_csv());
    let (source, mut parts) = metrics_store(|_, _, _| {});
    parts.pop();
    assert_eq!(
        difference(run(
            &fake,
            &source,
            &staging.0,
            ArchiveStream::Metrics,
            day("2026-10-03"),
            parts
        )),
        format!(
            "line 26 of {METRICS} (2026-10-04, row 0 of the day) has no stored record (the held \
             parts of 2026-10-04 hold 0)"
        )
    );
}

#[test]
fn invalid_or_unpublished_files_write_nothing() {
    let staging = Staging::new("invalid");
    let (source, parts) = metrics_store(|_, _, _| {});
    let refused = |fake: &Fake, needle: &str| {
        let error = run(
            fake,
            &source,
            &staging.0,
            ArchiveStream::Metrics,
            day("2026-10-03"),
            parts.clone(),
        )
        .unwrap_err();
        assert!(error.0.contains(needle), "{error}");
        assert!(staging.is_empty(), "{needle}");
    };
    // Not published.
    refused(&Fake::default(), "not published");
    // A bad header.
    let fake = Fake::default();
    fake.publish(
        ArchiveStream::Metrics,
        day("2026-10-03"),
        &metrics_csv().replacen("create_time", "createTime", 1),
    );
    refused(&fake, "line 1:");
    // A time outside the period ± 1 day.
    let fake = Fake::default();
    fake.publish(
        ArchiveStream::Metrics,
        day("2026-10-03"),
        &metrics_csv().replacen("2026-10-03 05:00:00", "2026-10-07 05:00:00", 1),
    );
    refused(&fake, "outside 2026-10-03 ± 1 day");
    // A download whose SHA-256 differs from the published one.
    let fake = Fake::default();
    fake.publish(ArchiveStream::Metrics, day("2026-10-03"), &metrics_csv());
    *fake.checksum.lock().unwrap() = Some("0".repeat(64));
    refused(&fake, &"0".repeat(64));
}

/// An aggTrades row at 2026-10-04T00:00:00.000 in the 2026-10-03 file is
/// filed under the next day, outside the catalog rule that proposes the
/// file: its part is proposed for the next day's file, so this file's
/// check set lacks it and the check fails, safely.
#[test]
fn a_row_outside_the_catalog_rule_fails_the_check() {
    let rows = [(1, at("2026-10-03", 5)), (2, at("2026-10-04", 0))];
    let csv = agg_csv(&rows);
    let fake = Fake::default();
    fake.publish(ArchiveStream::AggTrades, day("2026-10-03"), &csv);
    let staging = Staging::new("catalog-rule");
    let mut stored = records(ArchiveStream::AggTrades, &csv);
    let next = stored.split_off(1);
    let mut source = MemorySource::default();
    let part = sealed(&mut source, ArchiveStream::AggTrades, stored);
    let next_part = sealed(&mut source, ArchiveStream::AggTrades, next);
    assert_eq!(
        ArchiveStream::AggTrades.period_of(next_part.min_event_time.as_millis()),
        day("2026-10-04")
    );
    assert_eq!(
        difference(run(
            &fake,
            &source,
            &staging.0,
            ArchiveStream::AggTrades,
            day("2026-10-03"),
            vec![part]
        )),
        format!(
            "line 3 of {AGG} (2026-10-04, row 0 of the day) has no stored record (the held parts \
             of 2026-10-04 hold 0)"
        )
    );
}
