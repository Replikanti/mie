//! An in-memory [`RawRecordSource`] and live capture-run fixtures for the
//! replay unit tests.

use crate::archive::catalog::{DAY_MS, day_label};
use crate::normalize::record_time;
use crate::pipeline::Pipeline;
use crate::replay::LiveRun;
use crate::stream::BinanceStream;
use mie_domain::event::MarketEvent;
use mie_domain::fingerprint::Fingerprinter;
use mie_ports::raw::{
    Capture, DatasetVersion, RawDataset, RawRecord, RawRecordSource, RawSelection, RawStoreError,
    RawStreamKey, SealedFile,
};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

/// The raw source of the live fixtures.
pub(crate) const SOURCE: &str = "binance-um";

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

    /// The number of files sealed.
    pub(crate) fn file_count(&self) -> usize {
        self.files.len()
    }

    /// The path of the `index`-th file sealed.
    pub(crate) fn path_at(&self, index: usize) -> String {
        self.files[index].0.relative_path.clone()
    }

    /// The manifest of the file at `path`.
    pub(crate) fn file(&self, path: &str) -> SealedFile {
        self.files
            .iter()
            .find(|(f, _)| f.relative_path == path)
            .map(|(f, _)| f.clone())
            .unwrap()
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

/// A live `aggTrade` frame.
pub(crate) fn agg(id: u64, time: i64) -> String {
    format!(
        r#"{{"e":"aggTrade","E":{},"a":{id},"s":"BTCUSDT","p":"85000.10","q":"0.010","f":1,"l":1,"T":{time},"m":false}}"#,
        time + 50
    )
}

/// A live `markPriceUpdate` frame.
pub(crate) fn mark(time: i64) -> String {
    format!(
        r#"{{"e":"markPriceUpdate","E":{time},"s":"BTCUSDT","p":"85001.00000000","i":"85002.50000000","r":"0.00010000","T":1791273600000}}"#
    )
}

/// A live open-interest poll answer.
pub(crate) fn oi(time: i64) -> String {
    format!(r#"{{"symbol":"BTCUSDT","openInterest":"95253.475","time":{time}}}"#)
}

/// The records of one capture run, in processing order.
pub(crate) struct RunBuilder {
    pub(crate) run_id: String,
    pub(crate) records: Vec<(BinanceStream, RawRecord)>,
}

impl RunBuilder {
    pub(crate) fn new(run_id: &str) -> Self {
        Self {
            run_id: run_id.to_owned(),
            records: Vec::new(),
        }
    }

    pub(crate) fn push(
        &mut self,
        stream: BinanceStream,
        session: u32,
        payload: String,
    ) -> &mut Self {
        let event_time = record_time(stream, payload.as_bytes()).unwrap();
        let record = RawRecord {
            event_time,
            capture: Some(Capture {
                receive_time_ns: event_time.as_millis() * 1_000_000,
                receive_seq: self.records.len() as u64,
                session_id: format!("{}/{}/{session}", self.run_id, stream.raw_name()),
            }),
            payload: payload.into_bytes(),
        };
        self.records.push((stream, record));
        self
    }

    /// The records of `stream`, without the `skip`ped `receive_seq`s.
    pub(crate) fn of(&self, stream: BinanceStream, skip: &[u64]) -> Vec<RawRecord> {
        self.records
            .iter()
            .filter(|(s, r)| {
                *s == stream && !skip.contains(&r.capture.as_ref().unwrap().receive_seq)
            })
            .map(|(_, r)| r.clone())
            .collect()
    }

    /// Seals one file per stream.
    pub(crate) fn seal(&self, source: &mut MemorySource, skip: &[u64]) {
        for stream in BinanceStream::ALL {
            let records = self.of(stream, skip);
            if !records.is_empty() {
                source.seal(SOURCE, stream.raw_name(), records);
            }
        }
    }

    /// The files the writer seals when it seals every open part each
    /// `epoch` consecutive `receive_seq`s (`RawWriter::seal_all` every seal
    /// interval): per epoch, one file per stream and UTC date, in stored
    /// (append) order, without the `skip`ped seqs.
    pub(crate) fn epoch_files(
        &self,
        epoch: usize,
        skip: &[u64],
    ) -> Vec<(BinanceStream, Vec<RawRecord>)> {
        let mut files = Vec::new();
        for chunk in self.records.chunks(epoch) {
            let mut parts: BTreeMap<(BinanceStream, i64), Vec<RawRecord>> = BTreeMap::new();
            for (stream, record) in chunk {
                if skip.contains(&record.capture.as_ref().unwrap().receive_seq) {
                    continue;
                }
                let day = record.event_time.as_millis().div_euclid(DAY_MS);
                parts
                    .entry((*stream, day))
                    .or_default()
                    .push(record.clone());
            }
            files.extend(
                parts
                    .into_iter()
                    .map(|((stream, _), records)| (stream, records)),
            );
        }
        files
    }

    /// Seals [`epoch_files`](Self::epoch_files), as `seal` seals one file
    /// per stream.
    pub(crate) fn seal_epochs(&self, source: &mut MemorySource, epoch: usize, skip: &[u64]) {
        for (stream, records) in self.epoch_files(epoch, skip) {
            source.seal(SOURCE, stream.raw_name(), records);
        }
    }

    pub(crate) fn run(&self, started: i64, ended: Option<i64>, clean: Option<u64>) -> LiveRun {
        LiveRun {
            run_id: self.run_id.clone(),
            symbol: "BTCUSDT".to_owned(),
            hold_back_ms: 750,
            oi_retime_ms: 10_000,
            seeds: BTreeMap::new(),
            streams: vec![
                BinanceStream::AggTrade,
                BinanceStream::MarkPrice,
                BinanceStream::OpenInterest,
            ],
            started_at_ms: started,
            ended_at_ms: ended,
            clean_records: clean,
        }
    }

    /// What live delivered: the run's pipeline over its records.
    pub(crate) fn live(&self, run: &LiveRun) -> Vec<MarketEvent> {
        let mut pipeline =
            Pipeline::new(&run.symbol, run.hold_back_ms, run.oi_retime_ms, &run.seeds);
        let mut out = Vec::new();
        for (stream, record) in &self.records {
            out.extend(pipeline.push(*stream, record).events);
        }
        out.extend(pipeline.finish());
        out
    }
}

/// A [`MemorySource`] that injects faults, for the tests of the read paths.
/// Reads are numbered from 1 across every pass.
#[derive(Debug)]
pub(crate) struct FaultySource {
    pub(crate) inner: MemorySource,
    reads: Cell<u64>,
    /// The read that fails with an I/O error.
    fail: Option<u64>,
    /// The read that returns its records without the last one.
    alter: Option<u64>,
    /// Files `select` returns in addition to the inner source's.
    extra: Vec<SealedFile>,
    /// The paths read, in read order.
    pub(crate) log: RefCell<Vec<String>>,
}

impl FaultySource {
    pub(crate) fn new(inner: MemorySource) -> Self {
        Self {
            inner,
            reads: Cell::new(0),
            fail: None,
            alter: None,
            extra: Vec::new(),
            log: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn failing_read(mut self, read: u64) -> Self {
        self.fail = Some(read);
        self
    }

    pub(crate) fn altering_read(mut self, read: u64) -> Self {
        self.alter = Some(read);
        self
    }

    /// `select` also returns `file`, whatever the selection.
    pub(crate) fn selecting(mut self, file: SealedFile) -> Self {
        self.extra.push(file);
        self
    }

    /// How often `path` was read.
    pub(crate) fn reads_of(&self, path: &str) -> usize {
        self.log.borrow().iter().filter(|p| *p == path).count()
    }
}

impl RawRecordSource for FaultySource {
    fn select(&self, selection: &RawSelection) -> Result<RawDataset, RawStoreError> {
        let mut dataset = self.inner.select(selection)?;
        dataset.files.extend(self.extra.iter().cloned());
        dataset
            .files
            .sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
        Ok(dataset)
    }

    fn read(&self, file: &SealedFile) -> Result<Vec<RawRecord>, RawStoreError> {
        let read = self.reads.get() + 1;
        self.reads.set(read);
        self.log.borrow_mut().push(file.relative_path.clone());
        if self.fail == Some(read) {
            return Err(RawStoreError::Io(format!(
                "{}: simulated failure",
                file.relative_path
            )));
        }
        let mut records = self.inner.read(file)?;
        if self.alter == Some(read) {
            records.pop();
        }
        Ok(records)
    }
}
