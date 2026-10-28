//! The outcome dataset writer and its write protocol (ADR-057 D5).

use super::manifest::{OutcomeFile, OutcomeManifest};
use super::schema::{self, ContextRow, OutcomeRow, PassageRow, Row};
use super::{MANIFEST_FILE, OutcomeError, TABLES, io_error};
use crate::manifest::hex;
use crate::recovery::{mark_read_only, rename_no_replace};
use crate::sync_dir;
use crate::writer::HashingWriter;
use mie_domain::conditional::ContextRecord;
use mie_domain::conditional::label::{ForwardLabel, LabelProvenance};
use mie_domain::num::Price;
use mie_domain::order::CanonicalKey;
use mie_ports::outbound::ReplayWindow;
use parquet::arrow::ArrowWriter;
use sha2::Digest;
use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Rows buffered per table before they are handed to the Parquet encoder
/// as one batch.
const BATCH_ROWS: usize = 8_192;

/// What a dataset is built from, known before its first row (ADR-057 D3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeHeader {
    /// The manifest's `source` value: `<archive|live> <source>/<instrument>
    /// streams <list> correction <fold|none>`. One line.
    pub source: String,
    /// The replay window: every event of it fed the engine and the labels.
    pub replay: ReplayWindow,
    /// The records window: the records taken and labeled.
    pub records: ReplayWindow,
    /// The run's provenance, from the labeling run before its first event.
    pub provenance: LabelProvenance,
    /// The code version (ADR-057 D4), outside the content hash.
    pub code: String,
}

/// One table being written under its hidden temp name.
struct Table<R: Row> {
    name: &'static str,
    temp: PathBuf,
    writer: ArrowWriter<HashingWriter>,
    buffer: Vec<R>,
    rows: u64,
}

impl<R: Row> Table<R> {
    fn create(dir: &Path, name: &'static str) -> Result<Self, OutcomeError> {
        let temp = dir.join(format!(".{name}.tmp"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(io_error("create", &temp))?;
        let writer = ArrowWriter::try_new(
            HashingWriter::new(file),
            R::schema(),
            Some(schema::writer_properties()),
        )
        .map_err(|e| OutcomeError::Io(format!("start {}: {e}", temp.display())))?;
        Ok(Self {
            name,
            temp,
            writer,
            buffer: Vec::new(),
            rows: 0,
        })
    }

    fn push(&mut self, row: R, provenance: &[String; 6]) -> Result<(), OutcomeError> {
        self.buffer.push(row);
        self.rows += 1;
        if self.buffer.len() >= BATCH_ROWS {
            self.flush(provenance)?;
        }
        Ok(())
    }

    fn flush(&mut self, provenance: &[String; 6]) -> Result<(), OutcomeError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.writer
            .write(&schema::to_batch(&self.buffer, provenance))
            .map_err(|e| OutcomeError::Io(format!("encode {}: {e}", self.name)))?;
        self.buffer.clear();
        Ok(())
    }

    /// Writes the footer and seals the file: fsync, read-only, final name.
    fn seal(mut self, dir: &Path, provenance: &[String; 6]) -> Result<OutcomeFile, OutcomeError> {
        self.flush(provenance)?;
        let sink = self
            .writer
            .into_inner()
            .map_err(|e| OutcomeError::Io(format!("finish {}: {e}", self.temp.display())))?;
        sink.file.sync_all().map_err(io_error("sync", &self.temp))?;
        mark_read_only(&self.temp)?;
        rename_no_replace(&self.temp, &dir.join(self.name))?;
        Ok(OutcomeFile {
            name: self.name.to_owned(),
            rows: self.rows,
            sha256: hex(&sink.hasher.finalize()),
        })
    }
}

/// A record whose label has not arrived yet: the label's join key and the
/// values the label repeats from it.
struct Pending {
    number: u64,
    event: CanonicalKey,
    ordinal: u32,
    reference: Option<Price>,
    atr: Option<Price>,
}

/// Writes one outcome dataset (ADR-057).
///
/// Takes the records and labels of one labeling run in the order the run
/// emits them: every record as it is taken, every label once it is
/// complete. Labels come in record order (ADR-055 D1), so each must match
/// the oldest unlabeled record; anything else is
/// [`OutcomeError::Contract`]. [`finish`](Self::finish) seals the tables
/// and writes the manifest last. Dropping the writer, or any error, leaves
/// a directory without `dataset.manifest`, which is no dataset. After an
/// error the writer accepts no more calls.
pub struct OutcomeWriter {
    dir: PathBuf,
    header: OutcomeHeader,
    provenance: [String; 6],
    contexts: Table<ContextRow>,
    outcomes: Table<OutcomeRow>,
    passages: Table<PassageRow>,
    pending: VecDeque<Pending>,
    next_record: u64,
    /// The event of the last record and its ordinal (ADR-055 D1).
    last: Option<(CanonicalKey, u32)>,
    poisoned: bool,
}

impl std::fmt::Debug for OutcomeWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutcomeWriter")
            .field("dir", &self.dir)
            .field("records", &self.next_record)
            .field("pending", &self.pending.len())
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl OutcomeWriter {
    /// Creates `dir` (its parent must exist) and the tables' temp files.
    ///
    /// # Errors
    ///
    /// [`OutcomeError::Exists`] when `dir` exists: a dataset is never
    /// overwritten. [`OutcomeError::Contract`] when the header cannot be
    /// written as manifest lines. [`OutcomeError::Io`] when the directory
    /// or a file cannot be created.
    pub fn create(dir: &Path, header: OutcomeHeader) -> Result<Self, OutcomeError> {
        if header.source.is_empty() || header.source.contains(['\n', '\r']) {
            return Err(OutcomeError::Contract(format!(
                "source {:?} is not one manifest line",
                header.source
            )));
        }
        if header.code.is_empty() || header.code.contains(char::is_whitespace) {
            return Err(OutcomeError::Contract(format!(
                "code version {:?} is not one word",
                header.code
            )));
        }
        let (replay, records) = (header.replay, header.records);
        if records.start >= records.end || replay.start > records.start || replay.end < records.end
        {
            return Err(OutcomeError::Contract(
                "the records window must be non-empty and inside the replay window".to_owned(),
            ));
        }
        match fs::create_dir(dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(OutcomeError::Exists(format!(
                    "{} exists; a dataset is never overwritten",
                    dir.display()
                )));
            }
            Err(error) => return Err(io_error("create", dir)(error)),
        }
        if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_dir(parent)?;
        }
        let provenance = schema::provenance_values(&header.provenance);
        Ok(Self {
            dir: dir.to_path_buf(),
            header,
            provenance,
            contexts: Table::create(dir, TABLES[0])?,
            outcomes: Table::create(dir, TABLES[1])?,
            passages: Table::create(dir, TABLES[2])?,
            pending: VecDeque::new(),
            next_record: 0,
            last: None,
            poisoned: false,
        })
    }

    /// Records written so far.
    pub fn records(&self) -> u64 {
        self.next_record
    }

    /// Labels written so far.
    pub fn labels(&self) -> u64 {
        self.next_record - self.pending.len() as u64
    }

    fn check_poisoned(&self) -> Result<(), OutcomeError> {
        if self.poisoned {
            Err(OutcomeError::Io(
                "the writer failed earlier and accepts no more calls".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn poison_on_error<T>(&mut self, result: Result<T, OutcomeError>) -> Result<T, OutcomeError> {
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Writes the `contexts` row of the next record and waits for its
    /// label.
    ///
    /// # Errors
    ///
    /// [`OutcomeError::Range`] for a price beyond the column type,
    /// [`OutcomeError::Io`] when encoding fails.
    pub fn record(&mut self, record: &ContextRecord) -> Result<(), OutcomeError> {
        self.check_poisoned()?;
        let result = self.write_record(record);
        self.poison_on_error(result)
    }

    fn write_record(&mut self, record: &ContextRecord) -> Result<(), OutcomeError> {
        // ADR-055 D1: the position among the records of the same event.
        let ordinal = match self.last {
            Some((key, ordinal)) if key == record.event => ordinal + 1,
            _ => 0,
        };
        let number = self.next_record;
        let row = ContextRow::new(number, ordinal, record)?;
        self.contexts.push(row, &self.provenance)?;
        self.last = Some((record.event, ordinal));
        self.pending.push_back(Pending {
            number,
            event: record.event,
            ordinal,
            reference: record.state.last_price.ready().copied(),
            atr: record.state.atr.ready().copied(),
        });
        self.next_record += 1;
        Ok(())
    }

    /// Writes the `outcomes` and `passages` rows of the oldest unlabeled
    /// record.
    ///
    /// # Errors
    ///
    /// [`OutcomeError::Contract`] when no record waits for a label or the
    /// label's key, reference or ATR is not that record's;
    /// [`OutcomeError::Range`] and [`OutcomeError::Io`] as for
    /// [`record`](Self::record).
    pub fn label(&mut self, label: &ForwardLabel) -> Result<(), OutcomeError> {
        self.check_poisoned()?;
        let result = self.write_label(label);
        self.poison_on_error(result)
    }

    fn write_label(&mut self, label: &ForwardLabel) -> Result<(), OutcomeError> {
        let Some(pending) = self.pending.pop_front() else {
            return Err(OutcomeError::Contract(format!(
                "label {} #{} without a record",
                label.event, label.ordinal
            )));
        };
        if (pending.event, pending.ordinal) != (label.event, label.ordinal) {
            return Err(OutcomeError::Contract(format!(
                "label {} #{} arrived while record {} ({} #{}) waits for its label",
                label.event, label.ordinal, pending.number, pending.event, pending.ordinal
            )));
        }
        // The label repeats the record's reference and ATR; the dataset
        // stores them once, on `contexts`, so they must agree.
        if (pending.reference, pending.atr) != (label.reference, label.atr) {
            return Err(OutcomeError::Contract(format!(
                "label {} #{}: reference and ATR differ from record {}",
                label.event, label.ordinal, pending.number
            )));
        }
        for horizon in &label.horizons {
            let row = OutcomeRow::new(pending.number, horizon)?;
            self.outcomes.push(row, &self.provenance)?;
        }
        for row in PassageRow::of(pending.number, label)? {
            self.passages.push(row, &self.provenance)?;
        }
        Ok(())
    }

    /// Seals the tables, then writes the manifest, the last step: from here
    /// on the directory is a dataset. `events` and `rejections` are the
    /// labeling run's.
    ///
    /// # Errors
    ///
    /// [`OutcomeError::Contract`] when a record has no label;
    /// [`OutcomeError::Io`] when a step of the protocol fails. Either way
    /// no manifest is written.
    pub fn finish(self, events: u64, rejections: u64) -> Result<OutcomeManifest, OutcomeError> {
        self.check_poisoned()?;
        if let Some(pending) = self.pending.front() {
            return Err(OutcomeError::Contract(format!(
                "{} record(s) without a label, the first record {} ({} #{})",
                self.pending.len(),
                pending.number,
                pending.event,
                pending.ordinal
            )));
        }
        let Self {
            dir,
            header,
            provenance,
            contexts,
            outcomes,
            passages,
            ..
        } = self;
        let files = vec![
            contexts.seal(&dir, &provenance)?,
            outcomes.seal(&dir, &provenance)?,
            passages.seal(&dir, &provenance)?,
        ];
        sync_dir(&dir)?;

        let context = &header.provenance.context;
        let mut manifest = OutcomeManifest {
            source: header.source,
            replay: (
                header.replay.start.as_millis(),
                header.replay.end.as_millis(),
            ),
            records: (
                header.records.start.as_millis(),
                header.records.end.as_millis(),
            ),
            data: context.data.to_string(),
            engine: context.engine.to_string(),
            sampling: context.policy.to_string(),
            sampling_fingerprint: context.policy_fingerprint.to_string(),
            labeling: header.provenance.labeling.to_string(),
            labeling_fingerprint: header.provenance.labeling_fingerprint.to_string(),
            events,
            rejections,
            files,
            content: String::new(),
            code: header.code,
        };
        manifest.content = manifest.compute_content();

        let temp = dir.join(format!(".{MANIFEST_FILE}.tmp"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(io_error("create", &temp))?;
        file.write_all(manifest.render().as_bytes())
            .and_then(|()| file.sync_all())
            .map_err(io_error("write", &temp))?;
        mark_read_only(&temp)?;
        rename_no_replace(&temp, &dir.join(MANIFEST_FILE))?;
        sync_dir(&dir)?;
        Ok(manifest)
    }
}
