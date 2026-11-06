//! The streaming read of one cleanly ended live run for
//! [`LiveReplay::run_replay`](crate::replay::LiveReplay::run_replay)
//! (ADR-063).
//!
//! The run's files are read twice, one file at a time:
//!
//! - **Pass 1, integrity** ([`plan_clean_run`]). Every selected file is read
//!   once, in path order. Only a bitset of the run's `receive_seq`s and, per
//!   file, the run's record count and smallest and largest `receive_seq`
//!   are kept. Every integrity error of the run is raised here, with the
//!   texts and precedence of the buffered read it replaces, so a run is
//!   still replayed in full or not at all (ADR-063 D2).
//! - **Pass 2, recompute** ([`RunMerge`]). A k-way merge by `receive_seq`
//!   over per-file cursors, each opened only when its first record can be
//!   next and dropped when used up. Pass 1 proved that the run's seqs are
//!   exactly `0..N`, once each, so the merge yields the order of a global
//!   sort by `receive_seq` (ADR-063 D3).
//!
//! Memory: the records of the open cursors, which is at most the run's
//! records of one seal interval (ADR-063 D4), plus `N / 8` bytes of bitset
//! and a few words per file.

use crate::replay::{LiveRun, clean_run_count, clean_run_hole, not_clean, stored_twice};
use crate::stream::BinanceStream;
use mie_ports::outbound::ProviderError;
use mie_ports::raw::{RawRecord, RawRecordSource, SealedFile};
use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, VecDeque};
use std::fmt;

/// One file holding records of the run, as pass 1 found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedFile {
    pub(crate) file: SealedFile,
    pub(crate) stream: BinanceStream,
    /// The run's records in the file.
    pub(crate) count: u64,
    /// The smallest `receive_seq` of the run in the file.
    pub(crate) first_seq: u64,
    /// The largest `receive_seq` of the run in the file.
    pub(crate) last_seq: u64,
}

/// What pass 1 found: the files with records of the run, sorted by
/// `first_seq` (unique, since no seq repeats), and the run's record count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunPlan {
    pub(crate) files: Vec<PlannedFile>,
    pub(crate) found: u64,
}

/// Pass 1 over `files`, the selection of the cleanly ended `run`: reads
/// every file once, in the given (path) order, and checks the run's
/// records without holding them.
///
/// # Errors
///
/// The errors of the buffered read, in its order: per file, an unknown
/// stream ([`ProviderError::Contract`], before the read), a store failure
/// ([`ProviderError::Source`]) and a record without capture metadata
/// (`Contract`); then a `receive_seq` stored twice (`Contract`, naming the
/// smallest), a hole, and a count other than the run's `run_end` count
/// (both `Source`). A run that did not end cleanly is a `Contract` error.
pub(crate) fn plan_clean_run(
    source: &dyn RawRecordSource,
    files: &[SealedFile],
    run: &LiveRun,
) -> Result<RunPlan, ProviderError> {
    let Some(expected) = run.clean_records else {
        return Err(not_clean(&run.run_id));
    };
    // Never sized from the journal count alone: a corrupt `run_end` must
    // not allocate gigabytes. A valid run's seqs lie below both bounds.
    let rows = files
        .iter()
        .fold(0_u64, |rows, file| rows.saturating_add(file.rows));
    let mut seen = SeqSet::new(expected.min(rows));
    let prefix = format!("{}/", run.run_id);
    let mut planned = Vec::new();
    let mut found = 0_u64;
    let mut twice: Option<u64> = None;
    for file in files {
        let Some(stream) = BinanceStream::from_raw_name(file.stream.stream()) else {
            return Err(ProviderError::Contract(format!(
                "{}: unknown live stream {}",
                file.relative_path, file.stream
            )));
        };
        let mut span: Option<(u64, u64, u64)> = None;
        for record in source.read(file).map_err(source_error)? {
            let Some(capture) = &record.capture else {
                return Err(ProviderError::Contract(format!(
                    "{}: a live record at {} has no capture metadata",
                    file.relative_path, record.event_time
                )));
            };
            if !capture.session_id.starts_with(&prefix) {
                continue;
            }
            let seq = capture.receive_seq;
            found += 1;
            if !seen.insert(seq) {
                twice = Some(twice.map_or(seq, |t| t.min(seq)));
            }
            span = Some(match span {
                None => (1, seq, seq),
                Some((count, first, last)) => (count + 1, first.min(seq), last.max(seq)),
            });
        }
        if let Some((count, first_seq, last_seq)) = span {
            planned.push(PlannedFile {
                file: file.clone(),
                stream,
                count,
                first_seq,
                last_seq,
            });
        }
    }
    if let Some(seq) = twice {
        return Err(stored_twice(&run.run_id, seq));
    }
    let missing = seen.first_missing();
    if let Some(next) = seen.next_after(missing) {
        return Err(clean_run_hole(&run.run_id, missing, next));
    }
    if found != expected {
        return Err(clean_run_count(&run.run_id, found, expected));
    }
    planned.sort_by_key(|file| file.first_seq);
    Ok(RunPlan {
        files: planned,
        found,
    })
}

/// Pass 2: the run's records in ascending `receive_seq`, merged from
/// per-file cursors (module docs).
///
/// **Opening rule:** before each record, every pending file whose
/// `first_seq` is at or below the smallest open head is opened, or the next
/// pending file when none is open. Pending files are sorted by `first_seq`,
/// so no record smaller than the next one yielded sits in an unopened file.
pub(crate) struct RunMerge<'a> {
    source: &'a dyn RawRecordSource,
    prefix: String,
    pending: VecDeque<PlannedFile>,
    cursors: Vec<Option<Cursor>>,
    free: Vec<usize>,
    /// Open cursors keyed by their head `receive_seq`.
    heads: BinaryHeap<Reverse<(u64, usize)>>,
    /// The seq the next record must carry: pass 1 proved `0..N`.
    next_seq: u64,
    /// Records in open cursors not yet yielded.
    held: u64,
    peak: u64,
}

/// One open file: the run's records of it, sorted by `receive_seq`.
struct Cursor {
    path: String,
    stream: BinanceStream,
    records: VecDeque<(u64, RawRecord)>,
}

impl fmt::Debug for RunMerge<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunMerge")
            .field("pending", &self.pending.len())
            .field("open", &self.heads.len())
            .field("next_seq", &self.next_seq)
            .field("held", &self.held)
            .field("peak", &self.peak)
            .finish_non_exhaustive()
    }
}

impl<'a> RunMerge<'a> {
    /// The merge of the files `plan` found for the run `run_id`.
    pub(crate) fn new(source: &'a dyn RawRecordSource, run_id: &str, plan: RunPlan) -> Self {
        Self {
            source,
            prefix: format!("{run_id}/"),
            pending: plan.files.into(),
            cursors: Vec::new(),
            free: Vec::new(),
            heads: BinaryHeap::new(),
            next_seq: 0,
            held: 0,
            peak: 0,
        }
    }

    /// The most records the open cursors held at once so far.
    pub(crate) fn peak_held(&self) -> u64 {
        self.peak
    }

    /// The run's next record and its stream; `None` once every record was
    /// yielded.
    ///
    /// # Errors
    ///
    /// [`ProviderError::Source`] when a file fails to read, or holds other
    /// records of the run than pass 1 found (a storage fault between the
    /// passes, ADR-063 D6).
    pub(crate) fn next(&mut self) -> Result<Option<(BinanceStream, RawRecord)>, ProviderError> {
        loop {
            let open = match (self.pending.front(), self.heads.peek()) {
                (Some(_), None) => true,
                (Some(file), Some(Reverse((head, _)))) => file.first_seq <= *head,
                (None, _) => false,
            };
            if !open {
                break;
            }
            let Some(file) = self.pending.pop_front() else {
                break;
            };
            self.open(file)?;
        }
        let Some(Reverse((seq, index))) = self.heads.pop() else {
            return Ok(None);
        };
        let Some(cursor) = self.cursors[index].as_mut() else {
            unreachable!("a head points at an open cursor");
        };
        let Some((_, record)) = cursor.records.pop_front() else {
            unreachable!("an open cursor holds its head");
        };
        if seq != self.next_seq {
            return Err(changed(&cursor.path));
        }
        let stream = cursor.stream;
        self.next_seq += 1;
        self.held -= 1;
        match cursor.records.front() {
            Some((head, _)) => self.heads.push(Reverse((*head, index))),
            None => {
                self.cursors[index] = None;
                self.free.push(index);
            }
        }
        Ok(Some((stream, record)))
    }

    /// Reads `file`, keeps the run's records sorted by `receive_seq` (a
    /// no-op on files `mie ingest` wrote, which hold append order) and
    /// checks them against pass 1.
    fn open(&mut self, file: PlannedFile) -> Result<(), ProviderError> {
        let path = file.file.relative_path.clone();
        let mut records = Vec::new();
        for record in self.source.read(&file.file).map_err(source_error)? {
            let Some(capture) = &record.capture else {
                return Err(changed(&path));
            };
            if capture.session_id.starts_with(&self.prefix) {
                records.push((capture.receive_seq, record));
            }
        }
        records.sort_by_key(|(seq, _)| *seq);
        let span = (
            records.len() as u64,
            records.first().map(|(seq, _)| *seq),
            records.last().map(|(seq, _)| *seq),
        );
        if span != (file.count, Some(file.first_seq), Some(file.last_seq)) {
            return Err(changed(&path));
        }
        self.held += file.count;
        self.peak = self.peak.max(self.held);
        let cursor = Cursor {
            path,
            stream: file.stream,
            records: records.into(),
        };
        let index = match self.free.pop() {
            Some(index) => {
                self.cursors[index] = Some(cursor);
                index
            }
            None => {
                self.cursors.push(Some(cursor));
                self.cursors.len() - 1
            }
        };
        self.heads.push(Reverse((file.first_seq, index)));
        Ok(())
    }
}

fn changed(path: &str) -> ProviderError {
    ProviderError::Source(format!(
        "{path}: the run's records changed between the integrity pass and the recompute"
    ))
}

fn source_error(detail: impl fmt::Display) -> ProviderError {
    ProviderError::Source(detail.to_string())
}

/// The `receive_seq`s seen: a bitset over `[0, bound)`, and an ordered set
/// above it, which stays empty for a valid run.
struct SeqSet {
    words: Vec<u64>,
    bound: u64,
    above: BTreeSet<u64>,
}

impl SeqSet {
    fn new(bound: u64) -> Self {
        Self {
            words: vec![0; bound.div_ceil(64) as usize],
            bound,
            above: BTreeSet::new(),
        }
    }

    /// Marks `seq`; `false` when it was marked already.
    fn insert(&mut self, seq: u64) -> bool {
        if seq >= self.bound {
            return self.above.insert(seq);
        }
        let (word, bit) = ((seq / 64) as usize, 1_u64 << (seq % 64));
        let fresh = self.words[word] & bit == 0;
        self.words[word] |= bit;
        fresh
    }

    /// The smallest seq not marked.
    fn first_missing(&self) -> u64 {
        let below = self
            .words
            .iter()
            .position(|word| *word != u64::MAX)
            .map(|word| word as u64 * 64 + u64::from(self.words[word].trailing_ones()));
        if let Some(seq) = below.filter(|seq| *seq < self.bound) {
            return seq;
        }
        let mut seq = self.bound;
        while self.above.contains(&seq) {
            seq += 1;
        }
        seq
    }

    /// The smallest marked seq above `seq`.
    fn next_after(&self, seq: u64) -> Option<u64> {
        let from = seq.checked_add(1)?;
        if from < self.bound {
            let mut word = (from / 64) as usize;
            let mut bits = self.words[word] & (u64::MAX << (from % 64));
            loop {
                if bits != 0 {
                    return Some(word as u64 * 64 + u64::from(bits.trailing_zeros()));
                }
                word += 1;
                if word == self.words.len() {
                    break;
                }
                bits = self.words[word];
            }
        }
        self.above.range(from..).next().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{FaultySource, MemorySource, RunBuilder, SOURCE, agg, mark, oi};
    use mie_domain::time::EventTime;
    use mie_ports::outbound::ReplayWindow;
    use mie_ports::raw::RawSelection;

    /// 2026-10-06T01:00:00Z.
    const T0: i64 = 1_791_248_400_000;

    #[test]
    fn a_seq_set_finds_the_first_hole_and_the_next_seq_after_it() {
        let mut set = SeqSet::new(130);
        for seq in (0..130).filter(|seq| *seq != 64 && *seq != 65 && *seq != 129) {
            assert!(set.insert(seq));
        }
        assert!(!set.insert(3));
        assert_eq!((set.first_missing(), set.next_after(64)), (64, Some(66)));
        assert_eq!(set.next_after(128), None);
        assert!(set.insert(64) && set.insert(65));
        assert_eq!((set.first_missing(), set.next_after(129)), (129, None));
        // Above the bound: the ordered set.
        assert!(set.insert(129) && set.insert(131) && !set.insert(131));
        assert_eq!((set.first_missing(), set.next_after(130)), (130, Some(131)));
        let empty = SeqSet::new(0);
        assert_eq!((empty.first_missing(), empty.next_after(0)), (0, None));
    }

    /// A run of three streams over `n` records.
    fn three_streams(n: u64) -> RunBuilder {
        let mut b = RunBuilder::new("20261006T010000Z");
        for i in 0..n {
            let t = T0 + 100 * i as i64;
            match i % 3 {
                0 => b.push(BinanceStream::AggTrade, 1, agg(1 + i, t)),
                1 => b.push(BinanceStream::MarkPrice, 1, mark(t)),
                _ => b.push(BinanceStream::OpenInterest, 1, oi(t)),
            };
        }
        b
    }

    fn files(source: &dyn RawRecordSource, run: &LiveRun) -> Vec<SealedFile> {
        let keys = run
            .streams
            .iter()
            .map(|s| mie_ports::raw::RawStreamKey::new(SOURCE, "BTCUSDT", s.raw_name()).unwrap())
            .collect();
        let selection = RawSelection::new(
            keys,
            ReplayWindow {
                start: EventTime::from_millis(T0 - 3_600_000),
                end: EventTime::from_millis(T0 + 3_600_000),
            },
        )
        .unwrap();
        source.select(&selection).unwrap().files
    }

    fn merged(merge: &mut RunMerge<'_>) -> Vec<u64> {
        let mut seqs = Vec::new();
        while let Some((_, record)) = merge.next().unwrap() {
            seqs.push(record.capture.unwrap().receive_seq);
        }
        seqs
    }

    #[test]
    fn the_merge_yields_ascending_seqs_and_holds_one_epoch_at_most() {
        let b = three_streams(60);
        let run = b.run(T0, Some(T0 + 10_000), Some(60));
        for epoch in [1, 5, 7, 60] {
            let mut source = MemorySource::default();
            b.seal_epochs(&mut source, epoch, &[]);
            let files = files(&source, &run);
            let plan = plan_clean_run(&source, &files, &run).unwrap();
            assert_eq!(plan.found, 60);
            assert!(
                plan.files
                    .windows(2)
                    .all(|w| w[0].first_seq < w[1].first_seq)
            );
            let mut merge = RunMerge::new(&source, &run.run_id, plan);
            assert_eq!(merged(&mut merge), (0..60).collect::<Vec<_>>(), "{epoch}");
            assert!(
                merge.peak_held() <= epoch as u64,
                "epoch {epoch}: {merge:?}"
            );
            assert_eq!(merge.next().unwrap(), None);
        }
    }

    #[test]
    fn the_merge_opens_a_file_only_when_its_first_record_can_be_next() {
        let b = three_streams(30);
        let run = b.run(T0, Some(T0 + 10_000), Some(30));
        let mut source = MemorySource::default();
        b.seal_epochs(&mut source, 10, &[]);
        let files = files(&source, &run);
        let plan = plan_clean_run(&source, &files, &run).unwrap();
        assert_eq!(plan.files.len(), 9);
        let reads = source.reads.get();
        let mut merge = RunMerge::new(&source, &run.run_id, plan);
        // Seqs 0, 1 and 2 open the epoch's three files one by one: each
        // file's first record is the next one only then.
        let mut opened = Vec::new();
        for _ in 0..11 {
            merge.next().unwrap().unwrap();
            opened.push(source.reads.get() - reads);
        }
        assert_eq!(opened, [1, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4]);
        // The second epoch's file opened after the first epoch's were used
        // up: never more than one epoch held.
        assert!(merge.peak_held() <= 10, "{merge:?}");
        assert_eq!(merged(&mut merge), (11..30).collect::<Vec<_>>());
        assert_eq!(source.reads.get() - reads, 9);
    }

    #[test]
    fn the_merge_fails_on_a_file_that_changed_between_the_passes() {
        let b = three_streams(30);
        let run = b.run(T0, Some(T0 + 10_000), Some(30));
        let mut inner = MemorySource::default();
        b.seal_epochs(&mut inner, 10, &[]);
        // Pass 1 reads the 9 files, pass 2 the first epoch's 3; the 13th
        // read is the file of seq 10.
        let source = FaultySource::new(inner).altering_read(13);
        let files = files(&source, &run);
        let plan = plan_clean_run(&source, &files, &run).unwrap();
        let mut merge = RunMerge::new(&source, &run.run_id, plan);
        for seq in 0..10 {
            let (_, record) = merge.next().unwrap().unwrap();
            assert_eq!(record.capture.unwrap().receive_seq, seq);
        }
        let err = merge.next().unwrap_err();
        assert!(
            matches!(&err, ProviderError::Source(d) if d.contains("changed between")),
            "{err}"
        );
    }
}
