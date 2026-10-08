//! An in-memory [`RawRecordSource`] for the replay unit tests.

use crate::archive::catalog::{DAY_MS, day_label};
use mie_domain::fingerprint::Fingerprinter;
use mie_ports::raw::{
    DatasetVersion, RawDataset, RawRecord, RawRecordSource, RawSelection, RawStoreError,
    RawStreamKey, SealedFile,
};
use std::cell::Cell;

/// Sealed files held in memory. `select` behaves like the Parquet store:
/// the files of the selected streams in the date partitions the window
/// touches whose event-time range overlaps it, sorted by path.
#[derive(Debug, Default)]
pub(crate) struct MemorySource {
    files: Vec<(SealedFile, Vec<RawRecord>)>,
    /// Number of `read` calls.
    pub(crate) reads: Cell<u64>,
}

impl MemorySource {
    /// Seals `records` (in stored order) as one file of `source` /
    /// `BTCUSDT` / `stream` in the date partition of their first record.
    pub(crate) fn seal(&mut self, source: &str, stream: &str, records: Vec<RawRecord>) {
        let key = RawStreamKey::new(source, "BTCUSDT", stream).unwrap();
        let min = records.iter().map(|r| r.event_time).min().unwrap();
        let max = records.iter().map(|r| r.event_time).max().unwrap();
        let date = day_label(records[0].event_time.as_millis().div_euclid(DAY_MS));
        let part = self
            .files
            .iter()
            .filter(|(f, _)| f.stream == key && f.date == date)
            .count() as u32;
        let file = SealedFile {
            relative_path: format!("{key}/date={date}/part-{part:05}.parquet"),
            stream: key,
            date,
            part,
            rows: records.len() as u64,
            bytes: 0,
            min_event_time: min,
            max_event_time: max,
            sha256: "0".repeat(64),
        };
        self.files.push((file, records));
    }
}

impl RawRecordSource for MemorySource {
    fn select(&self, selection: &RawSelection) -> Result<RawDataset, RawStoreError> {
        let window = selection.window();
        let first = day_label(window.start.as_millis().max(0).div_euclid(DAY_MS));
        let last = day_label((window.end.as_millis() - 1).div_euclid(DAY_MS));
        let mut files: Vec<SealedFile> = self
            .files
            .iter()
            .map(|(f, _)| f)
            .filter(|f| selection.streams().contains(&f.stream))
            .filter(|f| f.date >= first && f.date <= last)
            .filter(|f| f.min_event_time < window.end && f.max_event_time >= window.start)
            .cloned()
            .collect();
        files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        let mut hasher = Fingerprinter::new();
        hasher.write_i64(window.start.as_millis());
        hasher.write_i64(window.end.as_millis());
        for key in selection.streams() {
            hasher.write_str(&key.to_string());
        }
        for file in &files {
            hasher.write_str(&file.relative_path);
        }
        let hex = hasher.finish().to_string().repeat(4);
        Ok(RawDataset {
            version: DatasetVersion::from_hex(&hex)?,
            files,
        })
    }

    fn read(&self, file: &SealedFile) -> Result<Vec<RawRecord>, RawStoreError> {
        self.reads.set(self.reads.get() + 1);
        self.files
            .iter()
            .find(|(f, _)| f == file)
            .map(|(_, records)| records.clone())
            .ok_or_else(|| RawStoreError::Integrity(format!("{} is missing", file.relative_path)))
    }
}
