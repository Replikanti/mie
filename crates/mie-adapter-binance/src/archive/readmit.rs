//! The re-admit check of `mie store resolve` (ADR-062 D11, proposed; #210
//! RA6): do held archive parts equal the rows of one archive file?
//!
//! The check downloads the file with the importer's fetch policy into its
//! staging directory (the zip is removed in every outcome), refuses a file
//! that is not published (a `.CHECKSUM` 404) or whose download has another
//! SHA-256, and makes one streaming pass over the CSV:
//!
//! - **Validation** through the importer's own per-row function
//!   ([`RowValidator`]): the header is the dataset's or absent, every
//!   ordering time lies within the period ± 1 day. Re-admit and import
//!   therefore never disagree on what a valid file is.
//! - **Comparison**, positional and per UTC day: the records of the check
//!   set dated d, in path order and record order, must equal the file's
//!   rows of day d in file order (event time and payload, no capture
//!   metadata). Every row counts, so the counts must be equal both ways;
//!   rows with equal times are compared in file order too. The first
//!   difference in file order is reported, as ADR-034 D4's prefix rule
//!   with the prefix being every row.
//!
//! The parts are read through [`RawRecordSource::read`], which verifies
//! each copy against its manifest; a read error refuses the check. One part
//! per day is held in memory at a time: a part holds at most the writer's
//! row and byte limits (`RotationPolicy` defaults, 1 000 000 rows and 128
//! MiB of payload), and a daily file touches at most three days (the
//! importer's orphan window), so at most three parts are held at once.
//!
//! On a match the check returns the entry the importer would have written
//! for the same parts, byte for byte: the archive, the published SHA-256,
//! the stream, the period, the header, the row count and one `file` line
//! per part of the check set with its manifest's rows and SHA-256. The
//! check writes nothing else; the caller writes the entry.

use super::ARCHIVE_SOURCE;
use super::catalog::{ArchiveStream, DAY_MS, Period, day_label, parse_day};
use super::fetch::{FetchError, Fetcher, for_each_line};
use super::import::RowValidator;
use super::ledger::{ImportLedger, LedgerFile};
use mie_domain::time::EventTime;
use mie_ports::raw::{RawRecord, RawRecordSource, RawStreamKey, SealedFile};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::fs;
use std::path::PathBuf;

/// One re-admit check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadmitRequest {
    /// Exchange symbol, also the raw-store instrument.
    pub symbol: String,
    /// Archive base URL.
    pub base_url: String,
    /// Where the zip is downloaded to; removed in every outcome.
    pub staging_dir: PathBuf,
    /// The archive file's dataset.
    pub stream: ArchiveStream,
    /// The archive file's period.
    pub period: Period,
    /// The check set: the held parts' manifests, in path order.
    pub parts: Vec<SealedFile>,
}

/// What the check found in a published, valid file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checked {
    /// The published SHA-256 of the zip.
    pub published: String,
    /// The file's data rows.
    pub rows: u64,
    /// The entry to write on a match; on a difference, the first one.
    pub outcome: Result<ImportLedger, String>,
}

/// Why the check could not decide: nothing may be written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadmitError(pub String);

impl fmt::Display for ReadmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ReadmitError {}

impl From<FetchError> for ReadmitError {
    fn from(error: FetchError) -> Self {
        Self(error.to_string())
    }
}

/// The parts of one day of the check set and the cursor over their
/// records: one part loaded at a time.
struct DayCursor<'a> {
    parts: VecDeque<&'a SealedFile>,
    /// The part loaded, its records and the next record's index.
    loaded: Option<(&'a SealedFile, Vec<RawRecord>, usize)>,
    /// The day's records in the check set, from the manifests.
    total: u64,
    /// Rows of the file compared on this day so far.
    rows: u64,
}

impl<'a> DayCursor<'a> {
    /// The next stored record, loading the next part when needed.
    fn next(
        &mut self,
        source: &dyn RawRecordSource,
    ) -> Result<Option<(&'a SealedFile, usize, RawRecord)>, ReadmitError> {
        loop {
            if let Some((part, records, index)) = &mut self.loaded {
                if let Some(record) = records.get(*index) {
                    let found = (*part, *index, record.clone());
                    *index += 1;
                    return Ok(Some(found));
                }
                self.loaded = None;
            }
            let Some(part) = self.parts.pop_front() else {
                return Ok(None);
            };
            let records = source.read(part).map_err(|error| {
                ReadmitError(format!(
                    "{}: it cannot be read ({error}); run `mie store verify`, then `mie store \
                     repair`",
                    part.relative_path
                ))
            })?;
            self.loaded = Some((part, records, 0));
        }
    }

    /// The first stored record left after the file ended, as its part and
    /// index.
    fn leftover(
        &mut self,
        source: &dyn RawRecordSource,
    ) -> Result<Option<(&'a SealedFile, usize)>, ReadmitError> {
        Ok(self.next(source)?.map(|(part, index, _)| (part, index)))
    }
}

/// Runs the check (module docs).
///
/// # Errors
///
/// [`ReadmitError`] when the file is not published, its download fails or
/// has another SHA-256, it fails the importer's validation, or a part
/// cannot be read. The zip is removed in every outcome.
pub fn check(
    fetcher: &mut Fetcher<'_>,
    source: &dyn RawRecordSource,
    request: &ReadmitRequest,
) -> Result<Checked, ReadmitError> {
    let archive = request.stream.archive_path(&request.symbol, request.period);
    let file_name = request.stream.file_name(&request.symbol, request.period);
    let url = format!("{}/{archive}", request.base_url.trim_end_matches('/'));
    let Some(published) = fetcher.checksum(&url, &file_name)? else {
        return Err(ReadmitError(format!(
            "{archive} is not published (its .CHECKSUM answers 404): there is nothing to compare \
             the held parts with"
        )));
    };
    fs::create_dir_all(&request.staging_dir).map_err(|e| {
        ReadmitError(format!(
            "create staging {}: {e}",
            request.staging_dir.display()
        ))
    })?;
    let zip = request.staging_dir.join(&file_name);
    let result = fetcher
        .download_verified(&url, &zip, &published)
        .map_err(ReadmitError::from)
        .and_then(|_| compare(&zip, &archive, &file_name, &published, source, request));
    let _ = fs::remove_file(&zip);
    result
}

/// The streaming pass over a verified zip.
fn compare(
    zip: &std::path::Path,
    archive: &str,
    file_name: &str,
    published: &str,
    source: &dyn RawRecordSource,
    request: &ReadmitRequest,
) -> Result<Checked, ReadmitError> {
    let mut days: BTreeMap<i64, DayCursor<'_>> = BTreeMap::new();
    for part in &request.parts {
        let day = parse_day(&part.date).ok_or_else(|| {
            ReadmitError(format!("{} has date {:?}", part.relative_path, part.date))
        })?;
        let cursor = days.entry(day).or_insert_with(|| DayCursor {
            parts: VecDeque::new(),
            loaded: None,
            total: 0,
            rows: 0,
        });
        cursor.parts.push_back(part);
        cursor.total += part.rows;
    }
    let entry = file_name.replace(".zip", ".csv");
    let mut validator = RowValidator::new(request.stream, request.period);
    let mut difference: Option<String> = None;
    let mut rows = 0u64;
    for_each_line(zip, &entry, |row| -> Result<(), ReadmitError> {
        let Some((line, time)) = validator.line(row).map_err(ReadmitError)? else {
            return Ok(());
        };
        rows += 1;
        if difference.is_some() {
            return Ok(());
        }
        let day = time.div_euclid(DAY_MS);
        let label = day_label(day);
        let Some(cursor) = days.get_mut(&day) else {
            difference = Some(format!(
                "line {line} of {archive} ({label}, row 0 of the day) has no stored record (the \
                 held parts of {label} hold 0)"
            ));
            return Ok(());
        };
        let position = cursor.rows;
        cursor.rows += 1;
        let expected = RawRecord {
            event_time: EventTime::from_millis(time),
            capture: None,
            payload: row.to_vec(),
        };
        match cursor.next(source)? {
            Some((_, _, stored)) if stored == expected => {}
            Some((part, index, _)) => {
                difference = Some(format!(
                    "line {line} of {archive} ({label}, row {position} of the day) differs from \
                     {} record {index}",
                    part.relative_path
                ));
            }
            None => {
                difference = Some(format!(
                    "line {line} of {archive} ({label}, row {position} of the day) has no stored \
                     record (the held parts of {label} hold {})",
                    cursor.total
                ));
            }
        }
        Ok(())
    })?;
    if difference.is_none() {
        for (day, cursor) in &mut days {
            if let Some((part, index)) = cursor.leftover(source)? {
                difference = Some(format!(
                    "{} record {index} ({}) has no row in {archive} ({} rows that day)",
                    part.relative_path,
                    day_label(*day),
                    cursor.rows
                ));
                break;
            }
        }
    }
    let outcome = match difference {
        Some(difference) => Err(difference),
        None => {
            let stream =
                RawStreamKey::new(ARCHIVE_SOURCE, &request.symbol, request.stream.raw_name())
                    .map_err(|e| ReadmitError(e.to_string()))?;
            let mut files: Vec<LedgerFile> = request
                .parts
                .iter()
                .map(|part| LedgerFile {
                    relative_path: part.relative_path.clone(),
                    rows: part.rows,
                    sha256: part.sha256.clone(),
                })
                .collect();
            files.sort();
            Ok(ImportLedger {
                archive: archive.to_owned(),
                sha256: published.to_owned(),
                stream,
                period: request.period,
                header: validator.header(),
                rows,
                files,
            })
        }
    };
    Ok(Checked {
        published: published.to_owned(),
        rows,
        outcome,
    })
}

#[cfg(test)]
mod tests;
