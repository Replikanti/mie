//! The archive importer: published files → verbatim raw records, exactly
//! once (ADR-034 D4).
//!
//! Archive files are processed sequentially in time order (a monthly file
//! at its first day), streams in [`ArchiveStream::ALL`] order within a
//! period. Per file:
//!
//! 1. **Probe** the `.CHECKSUM`; 404 marks the file missing.
//! 2. **Skip or conflict.** A ledger whose SHA-256 equals the published one
//!    and whose listed files are all in the store with the same hash is
//!    skipped. A ledger with another SHA-256 is a `changed` conflict and is
//!    never re-imported automatically.
//! 3. **Download** into staging and verify the SHA-256.
//! 4. **Validation pass**: the header equals the dataset's (or is absent
//!    and the first line is a data row), and every row's ordering time
//!    parses and lies within one day of the file's period, which catches a
//!    millisecond → microsecond switch. A failure appends nothing.
//! 5. Write **`PENDING`**, 6. **append** every row in file order,
//!    7. **seal**, 8. write the **ledger**, 9. remove `PENDING`.
//!
//! Each file's rows are sealed before the next file starts, so every sealed
//! part belongs to exactly one archive file. A sealed part of the file's
//! partitions that no ledger lists is an orphan: left by a crash during
//! steps 6–8. On start-up `PENDING` names the interrupted file; its orphans
//! must equal, per partition, a prefix of that file's rows (payload and
//! event time). Then only the rest is appended and the ledger lists the
//! orphans with the new parts. Anything else is an integrity conflict that
//! stops the run before anything is appended, and so is an orphan when no
//! file is pending.
//!
//! The importer sees the store only through the raw-store ports (ADR-025).

use super::ARCHIVE_SOURCE;
use super::catalog::{ArchiveStream, DAY_MS, Period};
use super::fetch::{FetchError, Fetcher, for_each_line};
use super::ledger::{ImportLedger, LedgerDir, LedgerError, LedgerFile, Pending};
use super::normalize::record_time;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{
    RawRecord, RawRecordSink, RawRecordSource, RawSelection, RawStoreError, RawStreamKey,
    SealedFile,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

/// What to import and where the bookkeeping lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportOptions {
    /// Exchange symbol, also the raw-store instrument.
    pub symbol: String,
    /// Archive base URL, e.g. `https://data.binance.vision`.
    pub base_url: String,
    /// The import ledger directory.
    pub ledger_dir: PathBuf,
    /// Where zips are downloaded to; each is removed once imported.
    pub staging_dir: PathBuf,
    /// The streams to import.
    pub streams: Vec<ArchiveStream>,
    /// First UTC day, as days since 1970-01-01.
    pub from_day: i64,
    /// Last UTC day, inclusive.
    pub to_day: i64,
    /// Probe the checksums only: no download, no write.
    pub dry_run: bool,
}

/// What happened to one archive file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileOutcome {
    /// Every row was appended and sealed.
    Imported {
        /// Data rows stored.
        rows: u64,
        /// Sealed files written.
        files: usize,
    },
    /// An interrupted import was completed.
    Resumed {
        /// Rows already sealed before the interruption.
        sealed: u64,
        /// Rows appended now.
        appended: u64,
    },
    /// Already imported with the published checksum.
    Skipped,
    /// Not published (404 on the checksum).
    Missing,
    /// Published but not imported yet (dry run only).
    Published,
    /// The published checksum differs from the ledger's: a conflict, never
    /// re-imported automatically.
    Changed {
        /// The ledger's SHA-256.
        ledger: String,
        /// The published SHA-256.
        published: String,
    },
    /// The file could not be imported; nothing of it was appended.
    Failed(String),
}

/// One archive file and its outcome, as reported to the observer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileReport {
    /// The dataset.
    pub stream: ArchiveStream,
    /// The file's period.
    pub period: Period,
    /// The archive path.
    pub archive: String,
    /// What happened.
    pub outcome: FileOutcome,
}

impl fmt::Display for FileReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = self.archive.rsplit('/').next().unwrap_or(&self.archive);
        match &self.outcome {
            FileOutcome::Imported { rows, files } => {
                write!(f, "imported {name}: {rows} rows, {files} files")
            }
            FileOutcome::Resumed { sealed, appended } => {
                write!(
                    f,
                    "resumed {name}: {sealed} rows sealed, {appended} appended"
                )
            }
            FileOutcome::Skipped => write!(f, "skipped {name}"),
            FileOutcome::Missing => write!(f, "missing {name}"),
            FileOutcome::Published => write!(f, "published {name}"),
            FileOutcome::Changed { ledger, published } => write!(
                f,
                "changed {name}: ledger {ledger}, published {published} (not re-imported)"
            ),
            FileOutcome::Failed(reason) => write!(f, "failed {name}: {reason}"),
        }
    }
}

/// Totals of a run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportSummary {
    /// Files imported in full.
    pub imported: u64,
    /// Interrupted files completed.
    pub resumed: u64,
    /// Files already imported.
    pub skipped: u64,
    /// Files not published.
    pub missing: u64,
    /// Files published but not imported (dry run).
    pub published: u64,
    /// Files whose published checksum changed.
    pub changed: u64,
    /// Files that failed.
    pub failed: u64,
    /// Rows stored by this run per raw stream name.
    pub rows: BTreeMap<String, u64>,
    /// Zip downloads, retries included.
    pub downloads: u64,
    /// Requests, retries included.
    pub requests: u64,
    /// The run stopped early on SIGINT/SIGTERM.
    pub stopped: bool,
}

impl ImportSummary {
    /// 0 when no file failed or changed, 1 otherwise.
    pub fn exit_code(&self) -> i32 {
        i32::from(self.failed > 0 || self.changed > 0)
    }

    fn count(&mut self, report: &FileReport) {
        match &report.outcome {
            FileOutcome::Imported { rows, .. } => {
                self.imported += 1;
                *self
                    .rows
                    .entry(report.stream.raw_name().to_owned())
                    .or_default() += rows;
            }
            FileOutcome::Resumed { appended, .. } => {
                self.resumed += 1;
                *self
                    .rows
                    .entry(report.stream.raw_name().to_owned())
                    .or_default() += appended;
            }
            FileOutcome::Skipped => self.skipped += 1,
            FileOutcome::Missing => self.missing += 1,
            FileOutcome::Published => self.published += 1,
            FileOutcome::Changed { .. } => self.changed += 1,
            FileOutcome::Failed(_) => self.failed += 1,
        }
    }
}

impl fmt::Display for ImportSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "imported {}, resumed {}, skipped {}, missing {}, published {}, changed {}, failed {}; \
             downloads {}, requests {}",
            self.imported,
            self.resumed,
            self.skipped,
            self.missing,
            self.published,
            self.changed,
            self.failed,
            self.downloads,
            self.requests
        )?;
        if self.stopped {
            write!(f, "; stopped by signal")?;
        }
        for (stream, rows) in &self.rows {
            write!(f, "\nrows {stream} {rows}")?;
        }
        Ok(())
    }
}

/// Why a run stopped. Unlike a [`FileOutcome::Failed`] file, these leave no
/// safe way to continue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportError {
    /// The raw store failed (append, seal, select or read).
    Store(RawStoreError),
    /// The ledger directory failed.
    Ledger(LedgerError),
    /// The store and the ledgers disagree in a way no rule resolves:
    /// orphans that are not a prefix of the pending file, orphans without a
    /// pending file, or a pending file whose published checksum changed.
    Integrity(String),
    /// The pending file could not be completed (for example its download
    /// failed). Nothing else is imported until it is.
    Pending(String),
}

impl fmt::Display for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(e) => write!(f, "{e}"),
            Self::Ledger(e) => write!(f, "{e}"),
            Self::Integrity(detail) => write!(f, "integrity conflict: {detail}"),
            Self::Pending(detail) => write!(f, "pending import not completed: {detail}"),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<RawStoreError> for ImportError {
    fn from(e: RawStoreError) -> Self {
        Self::Store(e)
    }
}

impl From<LedgerError> for ImportError {
    fn from(e: LedgerError) -> Self {
        Self::Ledger(e)
    }
}

/// A file-level failure: reported, then the run moves on.
#[derive(Debug)]
enum FileError {
    Fetch(FetchError),
    Invalid(String),
    Fatal(ImportError),
}

impl From<FetchError> for FileError {
    fn from(e: FetchError) -> Self {
        Self::Fetch(e)
    }
}

impl From<ImportError> for FileError {
    fn from(e: ImportError) -> Self {
        Self::Fatal(e)
    }
}

impl From<RawStoreError> for FileError {
    fn from(e: RawStoreError) -> Self {
        Self::Fatal(ImportError::Store(e))
    }
}

impl From<LedgerError> for FileError {
    fn from(e: LedgerError) -> Self {
        Self::Fatal(ImportError::Ledger(e))
    }
}

/// One archive file to process.
#[derive(Debug, Clone)]
struct Job {
    stream: ArchiveStream,
    period: Period,
    key: RawStreamKey,
    archive: String,
    file_name: String,
}

/// The result of the validation pass.
struct Validated {
    header: Option<String>,
    /// Ordering time of every data row, in file order.
    times: Vec<i64>,
}

/// The UTC day of an event time.
fn day_of(millis: i64) -> i64 {
    millis.div_euclid(DAY_MS)
}

/// Imports archive files into the raw store.
pub struct Importer<'a> {
    sink: &'a mut dyn RawRecordSink,
    source: &'a dyn RawRecordSource,
    fetcher: Fetcher<'a>,
    options: ImportOptions,
    ledgers: LedgerDir,
    shutdown: &'a AtomicBool,
    /// Every sealed file some ledger lists, per raw stream, loaded lazily.
    ledgered: BTreeMap<ArchiveStream, BTreeSet<String>>,
}

impl fmt::Debug for Importer<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Importer")
            .field("options", &self.options)
            .field("fetcher", &self.fetcher)
            .finish_non_exhaustive()
    }
}

impl<'a> Importer<'a> {
    /// An importer writing through `sink` (the writer of
    /// [`ARCHIVE_SOURCE`]) and reading the same store through `source`.
    pub fn new(
        sink: &'a mut dyn RawRecordSink,
        source: &'a dyn RawRecordSource,
        fetcher: Fetcher<'a>,
        options: ImportOptions,
        shutdown: &'a AtomicBool,
    ) -> Self {
        let ledgers = LedgerDir::new(&options.ledger_dir);
        Self {
            sink,
            source,
            fetcher,
            options,
            ledgers,
            shutdown,
            ledgered: BTreeMap::new(),
        }
    }

    fn job(&self, stream: ArchiveStream, period: Period) -> Result<Job, ImportError> {
        let key = RawStreamKey::new(ARCHIVE_SOURCE, &self.options.symbol, stream.raw_name())?;
        Ok(Job {
            stream,
            period,
            key,
            archive: stream.archive_path(&self.options.symbol, period),
            file_name: stream.file_name(&self.options.symbol, period),
        })
    }

    fn url(&self, job: &Job) -> String {
        format!(
            "{}/{}",
            self.options.base_url.trim_end_matches('/'),
            job.archive
        )
    }

    /// Runs the import: the pending file first, then every requested file.
    /// `observer` sees each file's report as it completes.
    ///
    /// # Errors
    ///
    /// [`ImportError`] when the run must stop; files reported before stay
    /// imported.
    pub fn run(
        &mut self,
        observer: &mut dyn FnMut(&FileReport),
    ) -> Result<ImportSummary, ImportError> {
        let mut summary = ImportSummary::default();
        let result = self.run_inner(observer, &mut summary);
        summary.downloads = self.fetcher.downloads();
        summary.requests = self.fetcher.requests();
        result.map(|()| summary)
    }

    fn run_inner(
        &mut self,
        observer: &mut dyn FnMut(&FileReport),
        summary: &mut ImportSummary,
    ) -> Result<(), ImportError> {
        if !self.options.dry_run
            && let Some(pending) = self.ledgers.pending()?
        {
            let report = self.resume(&pending)?;
            summary.count(&report);
            observer(&report);
        }
        let mut jobs = Vec::new();
        for (index, stream) in ArchiveStream::ALL.iter().enumerate() {
            if !self.options.streams.contains(stream) {
                continue;
            }
            for period in stream
                .period_kind()
                .periods(self.options.from_day, self.options.to_day)
            {
                jobs.push((period.first_day(), index, *stream, period));
            }
        }
        jobs.sort();
        for (_, _, stream, period) in jobs {
            if self.shutdown.load(Ordering::Relaxed) {
                summary.stopped = true;
                break;
            }
            let job = self.job(stream, period)?;
            let outcome = match self.process(&job, None) {
                Ok(outcome) => outcome,
                Err(FileError::Fatal(e)) => return Err(e),
                Err(FileError::Fetch(e)) => FileOutcome::Failed(e.to_string()),
                Err(FileError::Invalid(reason)) => FileOutcome::Failed(reason),
            };
            let report = FileReport {
                stream,
                period,
                archive: job.archive,
                outcome,
            };
            summary.count(&report);
            observer(&report);
        }
        Ok(())
    }

    /// Completes the file `PENDING` names.
    fn resume(&mut self, pending: &Pending) -> Result<FileReport, ImportError> {
        let stream = ArchiveStream::from_raw_name(pending.stream.stream()).ok_or_else(|| {
            ImportError::Integrity(format!("PENDING names unknown stream {}", pending.stream))
        })?;
        let job = self.job(stream, pending.period)?;
        if job.key != pending.stream || job.archive != pending.archive {
            return Err(ImportError::Integrity(format!(
                "PENDING names {} of {}, but this run imports symbol {}",
                pending.archive, pending.stream, self.options.symbol
            )));
        }
        if self.ledgers.read(stream, &job.file_name)?.is_some() {
            // The crash came after the ledger was written.
            self.ledgers.clear_pending()?;
            return Ok(FileReport {
                stream,
                period: job.period,
                archive: job.archive,
                outcome: FileOutcome::Skipped,
            });
        }
        let outcome = match self.process(&job, Some(pending)) {
            Ok(outcome) => outcome,
            Err(FileError::Fatal(e)) => return Err(e),
            Err(FileError::Fetch(e)) => return Err(ImportError::Pending(e.to_string())),
            Err(FileError::Invalid(reason)) => return Err(ImportError::Pending(reason)),
        };
        Ok(FileReport {
            stream,
            period: job.period,
            archive: job.archive,
            outcome,
        })
    }

    /// Steps 1–9 for one file; `pending` when resuming it.
    fn process(&mut self, job: &Job, pending: Option<&Pending>) -> Result<FileOutcome, FileError> {
        let url = self.url(job);
        let Some(published) = self.fetcher.checksum(&url, &job.file_name)? else {
            if pending.is_some() {
                return Err(FileError::Invalid(format!(
                    "{} is no longer published",
                    job.archive
                )));
            }
            return Ok(FileOutcome::Missing);
        };
        if let Some(pending) = pending
            && pending.sha256 != published
        {
            return Err(ImportError::Integrity(format!(
                "{} was republished with SHA-256 {published} while its import from {} was \
                 interrupted",
                job.archive, pending.sha256
            ))
            .into());
        }
        if pending.is_none()
            && let Some(ledger) = self.ledgers.read(job.stream, &job.file_name)?
        {
            if ledger.sha256 != published {
                return Ok(FileOutcome::Changed {
                    ledger: ledger.sha256,
                    published,
                });
            }
            return match self.missing_from_store(&ledger)? {
                None => Ok(FileOutcome::Skipped),
                Some(detail) => Ok(FileOutcome::Failed(format!(
                    "the ledger lists {detail}; not re-imported"
                ))),
            };
        }
        if self.options.dry_run {
            return Ok(FileOutcome::Published);
        }

        fs::create_dir_all(&self.options.staging_dir).map_err(|e| {
            FileError::Invalid(format!(
                "create staging {}: {e}",
                self.options.staging_dir.display()
            ))
        })?;
        let zip = self.options.staging_dir.join(&job.file_name);
        self.fetcher.download_verified(&url, &zip, &published)?;
        let result = self.import_zip(job, &zip, &published, pending);
        let _ = fs::remove_file(&zip);
        result
    }

    /// Steps 4–9 on a verified zip.
    fn import_zip(
        &mut self,
        job: &Job,
        zip: &std::path::Path,
        published: &str,
        pending: Option<&Pending>,
    ) -> Result<FileOutcome, FileError> {
        let entry = job.file_name.replace(".zip", ".csv");
        let (first_day, end_day) = (job.period.first_day(), job.period.end_day());

        // Orphans: sealed parts in the partitions this file may touch that
        // no ledger lists.
        let orphans = self.orphans(job, first_day - 1, end_day)?;
        if pending.is_none() && !orphans.is_empty() {
            return Err(ImportError::Integrity(format!(
                "{} sealed files of {} near {} are in no ledger and no import is pending: {}",
                orphans.len(),
                job.key,
                job.period,
                orphans
                    .iter()
                    .map(|f| f.relative_path.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
            .into());
        }
        let mut orphan_records: BTreeMap<i64, Vec<RawRecord>> = BTreeMap::new();
        for file in &orphans {
            let day = crate::archive::catalog::parse_day(&file.date).ok_or_else(|| {
                ImportError::Integrity(format!(
                    "orphan {} has date {}",
                    file.relative_path, file.date
                ))
            })?;
            orphan_records
                .entry(day)
                .or_default()
                .extend(self.source.read(file)?);
        }

        // Validation pass, with the prefix check of a resumed file.
        let validated = self.validate(job, zip, &entry, &orphan_records)?;
        let mut per_day: BTreeMap<i64, u64> = BTreeMap::new();
        for &time in &validated.times {
            *per_day.entry(day_of(time)).or_default() += 1;
        }
        for (day, records) in &orphan_records {
            let rows = per_day.get(day).copied().unwrap_or(0);
            if records.len() as u64 > rows {
                return Err(ImportError::Integrity(format!(
                    "{} orphan records in {} {} exceed the file's {rows} rows there",
                    records.len(),
                    job.key,
                    crate::archive::catalog::day_label(*day)
                ))
                .into());
            }
        }

        if pending.is_none() {
            self.ledgers.set_pending(&Pending {
                archive: job.archive.clone(),
                sha256: published.to_owned(),
                stream: job.key.clone(),
                period: job.period,
            })?;
        }

        // Append pass: the rows not sealed yet, in file order.
        let mut skip: BTreeMap<i64, u64> = orphan_records
            .iter()
            .map(|(day, records)| (*day, records.len() as u64))
            .collect();
        let mut line_no = 0_usize;
        let mut appended = 0_u64;
        let header_lines = usize::from(validated.header.is_some());
        let key = job.key.clone();
        let sink = &mut *self.sink;
        for_each_line(zip, &entry, |row| -> Result<(), FileError> {
            line_no += 1;
            if line_no <= header_lines {
                return Ok(());
            }
            let row_index = line_no - header_lines - 1;
            let time = validated.times[row_index];
            if let Some(left) = skip.get_mut(&day_of(time))
                && *left > 0
            {
                *left -= 1;
                return Ok(());
            }
            sink.append(
                &key,
                RawRecord {
                    event_time: EventTime::from_millis(time),
                    capture: None,
                    payload: row.to_vec(),
                },
            )?;
            appended += 1;
            Ok(())
        })?;
        let sealed = self.sink.seal_all()?;

        let mut files: Vec<LedgerFile> = orphans
            .iter()
            .chain(&sealed)
            .map(|f| LedgerFile {
                relative_path: f.relative_path.clone(),
                rows: f.rows,
                sha256: f.sha256.clone(),
            })
            .collect();
        files.sort();
        let rows = validated.times.len() as u64;
        let stored: u64 = files.iter().map(|f| f.rows).sum();
        if stored != rows {
            return Err(ImportError::Integrity(format!(
                "{}: {stored} rows sealed for {rows} rows in the file",
                job.archive
            ))
            .into());
        }
        let ledger = ImportLedger {
            archive: job.archive.clone(),
            sha256: published.to_owned(),
            stream: job.key.clone(),
            period: job.period,
            header: validated.header,
            rows,
            files,
        };
        self.ledgers.write(job.stream, &job.file_name, &ledger)?;
        self.ledgered
            .entry(job.stream)
            .or_default()
            .extend(ledger.files.iter().map(|f| f.relative_path.clone()));
        self.ledgers.clear_pending()?;
        Ok(match pending {
            None => FileOutcome::Imported {
                rows,
                files: sealed.len(),
            },
            Some(_) => FileOutcome::Resumed {
                sealed: rows - appended,
                appended,
            },
        })
    }

    /// Step 4: header, row times within one day of the period, and for a
    /// resumed file the orphan prefix per partition.
    fn validate(
        &self,
        job: &Job,
        zip: &std::path::Path,
        entry: &str,
        orphans: &BTreeMap<i64, Vec<RawRecord>>,
    ) -> Result<Validated, FileError> {
        let lower = job.period.start_ms() - DAY_MS;
        let upper = job.period.end_ms() + DAY_MS;
        let expected = job.stream.expected_header();
        let mut header = None;
        let mut first = true;
        let mut times = Vec::new();
        let mut seen: BTreeMap<i64, usize> = BTreeMap::new();
        for_each_line(zip, entry, |row| -> Result<(), FileError> {
            if std::mem::take(&mut first) && row == expected.as_bytes() {
                header = Some(expected.to_owned());
                return Ok(());
            }
            let line = times.len() + 1 + usize::from(header.is_some());
            let time = record_time(job.stream, row)
                .map_err(|e| {
                    FileError::Invalid(format!(
                        "line {line}: {e} (a header must be exactly {expected:?})"
                    ))
                })?
                .as_millis();
            if !(lower..upper).contains(&time) {
                return Err(FileError::Invalid(format!(
                    "line {line}: time {time} ms is outside {} ± 1 day",
                    job.period
                )));
            }
            let day = day_of(time);
            let position = seen.entry(day).or_default();
            if let Some(stored) = orphans.get(&day).and_then(|records| records.get(*position)) {
                let expected = RawRecord {
                    event_time: EventTime::from_millis(time),
                    capture: None,
                    payload: row.to_vec(),
                };
                if *stored != expected {
                    return Err(ImportError::Integrity(format!(
                        "sealed record {position} of {} {} is not line {line} of {}",
                        job.key,
                        crate::archive::catalog::day_label(day),
                        job.archive
                    ))
                    .into());
                }
            }
            *position += 1;
            times.push(time);
            Ok(())
        })?;
        Ok(Validated { header, times })
    }

    /// Sealed files of `job`'s stream in the partitions of days
    /// `[first_day, last_day]` that no ledger lists.
    fn orphans(
        &mut self,
        job: &Job,
        first_day: i64,
        last_day: i64,
    ) -> Result<Vec<SealedFile>, ImportError> {
        let files = self.sealed(&job.key, first_day, last_day)?;
        let ledgered = self.ledgered(job.stream)?;
        Ok(files
            .into_iter()
            .filter(|f| !ledgered.contains(&f.relative_path))
            .collect())
    }

    /// Every sealed file of `key` in the partitions of days
    /// `[first_day, last_day]`.
    fn sealed(
        &self,
        key: &RawStreamKey,
        first_day: i64,
        last_day: i64,
    ) -> Result<Vec<SealedFile>, ImportError> {
        let window = ReplayWindow {
            start: EventTime::from_millis(first_day.max(0) * DAY_MS),
            end: EventTime::from_millis((last_day + 1).max(1) * DAY_MS),
        };
        let selection = RawSelection::new(BTreeSet::from([key.clone()]), window)?;
        Ok(self.source.select(&selection)?.files)
    }

    fn ledgered(&mut self, stream: ArchiveStream) -> Result<&BTreeSet<String>, ImportError> {
        if !self.ledgered.contains_key(&stream) {
            let paths = self
                .ledgers
                .list(stream)?
                .into_iter()
                .flat_map(|ledger| ledger.files.into_iter().map(|f| f.relative_path))
                .collect();
            self.ledgered.insert(stream, paths);
        }
        Ok(&self.ledgered[&stream])
    }

    /// `None` when every file `ledger` lists is in the store with the same
    /// hash; otherwise the first one that is not.
    fn missing_from_store(&self, ledger: &ImportLedger) -> Result<Option<String>, ImportError> {
        let mut days = BTreeSet::new();
        for file in &ledger.files {
            let day = file
                .relative_path
                .split('/')
                .find_map(|segment| segment.strip_prefix("date="))
                .and_then(crate::archive::catalog::parse_day)
                .ok_or_else(|| {
                    ImportError::Integrity(format!("ledger path {}", file.relative_path))
                })?;
            days.insert(day);
        }
        let mut stored = BTreeMap::new();
        for day in days {
            for file in self.sealed(&ledger.stream, day, day)? {
                stored.insert(file.relative_path, file.sha256);
            }
        }
        Ok(ledger
            .files
            .iter()
            .find_map(|file| match stored.get(&file.relative_path) {
                Some(sha) if *sha == file.sha256 => None,
                Some(_) => Some(format!("{} with another hash", file.relative_path)),
                None => Some(format!("{}, which the store lacks", file.relative_path)),
            }))
    }
}
