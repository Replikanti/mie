//! Directory layout and file names of the raw store (ADR-030).
//!
//! ```text
//! <root>/source=<s>/instrument=<i>/stream=<st>/date=YYYY-MM-DD/part-NNNNN.parquet
//! <root>/source=<s>/instrument=<i>/stream=<st>/date=YYYY-MM-DD/part-NNNNN.manifest
//! ```
//!
//! Relative paths always use `/`, whatever the platform, because they are
//! part of the manifest and of the dataset version.

use mie_ports::raw::{RawStoreError, RawStreamKey};
use std::path::{Path, PathBuf};

/// Milliseconds per UTC day.
pub(crate) const DAY_MS: i64 = 86_400_000;

/// Latest supported event time: 9999-12-31T23:59:59.999Z.
pub(crate) const MAX_EVENT_TIME_MS: i64 = 253_402_300_799_999;

/// Highest part number (`part-99999`).
pub(crate) const MAX_PART: u32 = 99_999;

/// Days between 0000-03-01 and 1970-01-01 in the proleptic Gregorian
/// calendar (the epoch shift of the civil-date algorithm).
const EPOCH_SHIFT_DAYS: i64 = 719_468;
const DAYS_PER_ERA: i64 = 146_097;

/// The UTC date `YYYY-MM-DD` of an event time in milliseconds.
///
/// # Errors
///
/// [`RawStoreError::Invalid`] outside `0 ..= MAX_EVENT_TIME_MS`.
pub(crate) fn civil_date(event_time_ms: i64) -> Result<String, RawStoreError> {
    if !(0..=MAX_EVENT_TIME_MS).contains(&event_time_ms) {
        return Err(RawStoreError::Invalid(format!(
            "event time {event_time_ms}ms is outside the supported range 0 ..= {MAX_EVENT_TIME_MS}ms"
        )));
    }
    let (year, month, day) = civil_from_days(event_time_ms / DAY_MS);
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

/// Parses `YYYY-MM-DD` to days since 1970-01-01, accepting only real dates
/// in the supported range written exactly as [`civil_date`] writes them.
pub(crate) fn parse_date(date: &str) -> Option<i64> {
    let bytes = date.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let number = |range: std::ops::Range<usize>| -> Option<i64> {
        let digits = &date[range];
        if digits.bytes().all(|b| b.is_ascii_digit()) {
            digits.parse().ok()
        } else {
            None
        }
    };
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    // Reject dates that do not exist (2026-02-30) and dates before the epoch.
    (days >= 0 && civil_from_days(days) == (year, month, day)).then_some(days)
}

/// First millisecond of the UTC day after the one containing `event_time_ms`.
pub(crate) fn next_day_start_ms(event_time_ms: i64) -> i64 {
    (event_time_ms.div_euclid(DAY_MS) + 1) * DAY_MS
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 to
/// (year, month, day) in the proleptic Gregorian calendar.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + EPOCH_SHIFT_DAYS;
    let era = z.div_euclid(DAYS_PER_ERA);
    let doe = z - era * DAYS_PER_ERA;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Howard Hinnant's `days_from_civil`, the inverse of [`civil_from_days`].
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * DAYS_PER_ERA + doe - EPOCH_SHIFT_DAYS
}

/// `part-NNNNN`.
pub(crate) fn part_name(part: u32) -> String {
    format!("part-{part:05}")
}

/// Parses `part-NNNNN` (exactly five digits).
pub(crate) fn parse_part_name(name: &str) -> Option<u32> {
    let digits = name.strip_prefix("part-")?;
    if digits.len() == 5 && digits.bytes().all(|b| b.is_ascii_digit()) {
        digits.parse().ok()
    } else {
        None
    }
}

/// The kinds of file a partition directory may hold for one part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PartFile {
    /// `part-NNNNN.parquet`: sealed data (or rolled-forward data whose
    /// manifest is still pending).
    Data,
    /// `part-NNNNN.manifest`: the seal.
    Manifest,
    /// `.part-NNNNN.parquet.tmp`: data being written.
    DataTmp,
    /// `.part-NNNNN.manifest.tmp`: a pending manifest.
    ManifestTmp,
}

impl PartFile {
    /// The file name of `part` of this kind.
    pub(crate) fn name(self, part: u32) -> String {
        let base = part_name(part);
        match self {
            Self::Data => format!("{base}.parquet"),
            Self::Manifest => format!("{base}.manifest"),
            Self::DataTmp => format!(".{base}.parquet.tmp"),
            Self::ManifestTmp => format!(".{base}.manifest.tmp"),
        }
    }

    /// Classifies a file name; `None` for files the store does not own.
    pub(crate) fn classify(name: &str) -> Option<(Self, u32)> {
        let (kind, base) = if let Some(rest) = name.strip_prefix('.') {
            if let Some(base) = rest.strip_suffix(".parquet.tmp") {
                (Self::DataTmp, base)
            } else {
                (Self::ManifestTmp, rest.strip_suffix(".manifest.tmp")?)
            }
        } else if let Some(base) = name.strip_suffix(".parquet") {
            (Self::Data, base)
        } else {
            (Self::Manifest, name.strip_suffix(".manifest")?)
        };
        Some((kind, parse_part_name(base)?))
    }
}

/// `source=<s>`.
pub(crate) fn source_dir(source: &str) -> String {
    format!("source={source}")
}

/// `source=<s>/instrument=<i>/stream=<st>`.
pub(crate) fn stream_dir(key: &RawStreamKey) -> String {
    format!(
        "source={}/instrument={}/stream={}",
        key.source(),
        key.instrument(),
        key.stream()
    )
}

/// `source=<s>/instrument=<i>/stream=<st>/date=YYYY-MM-DD`.
pub(crate) fn partition_dir(key: &RawStreamKey, date: &str) -> String {
    format!("{}/date={date}", stream_dir(key))
}

/// Relative path of a file of `part` in a partition.
pub(crate) fn file_path(key: &RawStreamKey, date: &str, kind: PartFile, part: u32) -> String {
    format!("{}/{}", partition_dir(key, date), kind.name(part))
}

/// Parses the relative path of a sealed data file back into its location.
///
/// # Errors
///
/// [`RawStoreError::Invalid`] for anything [`file_path`] with
/// [`PartFile::Data`] cannot produce.
pub(crate) fn parse_data_path(path: &str) -> Result<(RawStreamKey, String, u32), RawStoreError> {
    let invalid = || RawStoreError::Invalid(format!("{path:?} is not a sealed raw data path"));
    let segments: Vec<&str> = path.split('/').collect();
    let [source, instrument, stream, date, file] = segments[..] else {
        return Err(invalid());
    };
    let value = |segment: &'static str, text: &str| -> Result<String, RawStoreError> {
        text.strip_prefix(segment)
            .map(str::to_owned)
            .ok_or_else(invalid)
    };
    let key = RawStreamKey::new(
        &value("source=", source)?,
        &value("instrument=", instrument)?,
        &value("stream=", stream)?,
    )
    .map_err(|_| invalid())?;
    let date = value("date=", date)?;
    parse_date(&date).ok_or_else(invalid)?;
    match PartFile::classify(file) {
        Some((PartFile::Data, part)) => Ok((key, date, part)),
        _ => Err(invalid()),
    }
}

/// Joins a `/`-separated relative path onto `root`.
pub(crate) fn resolve(root: &Path, relative: &str) -> PathBuf {
    relative
        .split('/')
        .fold(root.to_path_buf(), |path, segment| path.join(segment))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_dates_cover_the_supported_range() {
        assert_eq!(civil_date(0).unwrap(), "1970-01-01");
        assert_eq!(civil_date(DAY_MS - 1).unwrap(), "1970-01-01");
        assert_eq!(civil_date(DAY_MS).unwrap(), "1970-01-02");
        assert_eq!(civil_date(951_782_400_000).unwrap(), "2000-02-29");
        assert_eq!(civil_date(1_791_244_800_000).unwrap(), "2026-10-06");
        assert_eq!(civil_date(MAX_EVENT_TIME_MS).unwrap(), "9999-12-31");
        for bad in [-1, MAX_EVENT_TIME_MS + 1, i64::MIN, i64::MAX] {
            assert!(
                matches!(civil_date(bad), Err(RawStoreError::Invalid(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn dates_parse_back_to_their_day() {
        for ms in [0, 951_782_400_000, 1_791_244_800_000, MAX_EVENT_TIME_MS] {
            let date = civil_date(ms).unwrap();
            assert_eq!(parse_date(&date), Some(ms / DAY_MS), "{date}");
        }
        // Every day of a leap-year span round-trips.
        for day in 10_957..(10_957 + 3 * 366) {
            assert_eq!(parse_date(&civil_date(day * DAY_MS).unwrap()), Some(day));
        }
        for bad in [
            "",
            "1970-1-01",
            "1970/01/01",
            "2026-02-30",
            "2100-02-29",
            "2026-13-01",
            "2026-00-10",
            "+970-01-01",
            "1969-12-31",
            "2026-10-06 ",
            "２026-10-06",
        ] {
            assert_eq!(parse_date(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn next_day_starts_at_midnight() {
        assert_eq!(next_day_start_ms(0), DAY_MS);
        assert_eq!(next_day_start_ms(DAY_MS - 1), DAY_MS);
        assert_eq!(next_day_start_ms(DAY_MS), 2 * DAY_MS);
    }

    #[test]
    fn part_names_round_trip() {
        for part in [0, 1, 42, MAX_PART] {
            assert_eq!(parse_part_name(&part_name(part)), Some(part));
        }
        assert_eq!(part_name(7), "part-00007");
        for bad in [
            "part-1",
            "part-100000",
            "part--0001",
            "part-0000a",
            "Part-00001",
            "part-",
        ] {
            assert_eq!(parse_part_name(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn part_files_classify_by_name() {
        for kind in [
            PartFile::Data,
            PartFile::Manifest,
            PartFile::DataTmp,
            PartFile::ManifestTmp,
        ] {
            assert_eq!(PartFile::classify(&kind.name(12)), Some((kind, 12)));
        }
        assert_eq!(PartFile::Data.name(3), "part-00003.parquet");
        assert_eq!(PartFile::ManifestTmp.name(3), ".part-00003.manifest.tmp");
        for other in [
            ".lock",
            "part-00001.csv",
            "notes.parquet",
            ".part-1.parquet.tmp",
            "x",
        ] {
            assert_eq!(PartFile::classify(other), None, "{other:?}");
        }
    }

    #[test]
    fn data_paths_round_trip_and_reject_escapes() {
        let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
        let path = file_path(&key, "2026-10-06", PartFile::Data, 4);
        assert_eq!(
            path,
            "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00004.parquet"
        );
        assert_eq!(
            parse_data_path(&path).unwrap(),
            (key.clone(), "2026-10-06".to_owned(), 4)
        );
        for bad in [
            "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00004.manifest",
            "source=../instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00004.parquet",
            "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-32/part-00004.parquet",
            "/source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00004.parquet",
            "source=binance-um/instrument=BTCUSDT/date=2026-10-06/part-00004.parquet",
            "instrument=binance-um/source=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00004.parquet",
        ] {
            assert!(
                matches!(parse_data_path(bad), Err(RawStoreError::Invalid(_))),
                "{bad:?}"
            );
        }
    }
}
