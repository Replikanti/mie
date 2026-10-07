//! Order-book synchronization: Binance's diff/snapshot procedure as a pure
//! state machine (ADR-038, proposed).
//!
//! [`BookSequencer`] sees the `depth` diffs and the `depthSnapshot` REST
//! bodies in processing order, already normalized, and decides which of them
//! reach the domain. It has no clock and does no I/O: its output is a
//! function of the records it is given, in order, plus its seed and the
//! hold-back's last released event (`floor`), which is itself a function of
//! the same records. So replay recomputes every book event, gap, reset and
//! rejection that live capture delivered (ADR-019, ADR-032 D4).
//!
//! **Synced**: a diff whose `pu` equals the last emitted `u` is emitted as a
//! `BookUpdate`. Anything else ends the synced period.
//!
//! **Unsynced** (rules 1–9 of ADR-038):
//!
//! 1. A diff from a new capture session makes the book unsynced with cause
//!    `Disconnected`; the buffer becomes `[diff]`.
//! 2. A synced diff that does not chain makes it unsynced with cause
//!    `SequenceBreak`; the buffer becomes `[diff]`.
//! 3. While unsynced, diffs are buffered. A diff that does not chain to the
//!    buffer's tail restarts the buffer. The buffer keeps the newest
//!    [`MAX_UNSYNCED_DIFFS`]; the dropped ones are counted.
//! 4. A snapshot arriving while unsynced is rejected as
//!    [`SnapshotRejection::Stale`] when its `L` (`lastUpdateId`) is at or
//!    below the last emitted snapshot id or below the last emitted update id.
//!    Otherwise it becomes the pending snapshot; the one with the higher `L`
//!    wins.
//! 5. Sync attempt, after every diff or snapshot while unsynced. Buffered
//!    diffs with `u < L` (or not above the last emitted `u`) are dropped and
//!    counted as stale; an empty buffer waits. With `d` the first remaining
//!    diff and `S` the pending snapshot:
//!    - `d.U > L`: rejected as [`SnapshotRejection::TooOld`];
//!    - `d.T < S.T`: rejected as [`SnapshotRejection::TimeOrder`];
//!    - the gap, or the snapshot when there is no gap, does not sort above
//!      `floor` and above every book event emitted so far: rejected as
//!      [`SnapshotRejection::Late`];
//!    - otherwise the sequencer emits `FeedGap { OrderBook, start, end: S.T,
//!      cause }`, the `BookSnapshot` and every buffered diff from `d` on, and
//!      is synced. The gap is left out at the run's first sync when the run
//!      has no seed. `start` is the time of the last emitted book event, else
//!      the seed, clamped to `end`.
//!
//!    Every rejection drops the pending snapshot and asks for a new one.
//! 6. A snapshot arriving while synced is a **checkpoint**. With `d*` the
//!    first diff with `u >= L` and `d_prev` the emitted diff before it, the
//!    checkpoint is re-anchored (emitted as a `BookSnapshot`, which sorts
//!    right before `d*`) only when `L` is above the last emitted snapshot id,
//!    `d_prev.T < S.T <= d*.T`, `d*.U <= L` and `S` sorts above `floor`. A
//!    `d*` already emitted must be among the last [`RECENT_DIFFS`]; a `d*`
//!    still to come is waited for, and any desync drops the wait. Every other
//!    checkpoint stays raw-only and is counted as skipped.
//! 7. [`BookSequencer::desync`] makes the book unsynced and clears the buffer
//!    and the pending snapshot.
//! 8. A diff that failed normalization makes the book unsynced with cause
//!    `MissingData` and clears the buffer. A snapshot that failed is counted
//!    and, while unsynced, asks for a new one.
//! 9. The first cause wins until the next sync: each unsynced period yields
//!    exactly one `OrderBook` gap, delivered right before the snapshot that
//!    ends it (ADR-028 D4: announced on resumption).
//!
//! The constants are code, not run parameters: changing them changes what a
//! recompute delivers, so it is a versioned change of this module.

use crate::normalize::NormalizeError;
use mie_domain::event::{BookSnapshot, BookUpdate, FeedGap, GapReason, MarketEvent, Stream};
use mie_domain::order::CanonicalKey;
use mie_domain::time::EventTime;
use std::collections::{BTreeMap, VecDeque};

/// Most diffs buffered while unsynced: two minutes at the 100 ms cadence.
pub const MAX_UNSYNCED_DIFFS: usize = 1_200;

/// Emitted diffs remembered for re-anchoring a checkpoint whose straddling
/// diff was already emitted: 6.4 s at the 100 ms cadence.
pub const RECENT_DIFFS: usize = 64;

/// Why a snapshot did not resync the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SnapshotRejection {
    /// Not newer than what the book already delivered (rule 4).
    Stale,
    /// The buffered diffs start after it: updates in between are missing
    /// (rule 5a).
    TooOld,
    /// The first diff after it is older than it (rule 5b).
    TimeOrder,
    /// It would sort at or below an event already released or emitted
    /// (rule 5c).
    Late,
}

/// Why a checkpoint snapshot stayed raw-only (rule 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CheckpointSkip {
    /// Its `L` is not above the last emitted snapshot id.
    NotNewer,
    /// The first diff with `u >= L` starts after `L`.
    NoStraddle,
    /// Its time is not strictly after the diff before it, or after the diff
    /// that straddles it.
    TimeOrder,
    /// The straddling diff was emitted too long ago, or first after the
    /// anchor, so its predecessor is unknown.
    OutOfRecent,
    /// It would sort at or below the last released event.
    Late,
    /// A desync or a newer checkpoint replaced it while it waited.
    Dropped,
}

/// A change of the sync state, for the capture journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookTransition {
    /// An unsynced period began.
    Desynced(GapReason),
    /// A snapshot resynced the book.
    Synced {
        /// The snapshot's last update id.
        last_update_id: u64,
        /// The snapshot's time.
        time: EventTime,
    },
    /// A snapshot was rejected while unsynced.
    SnapshotRejected(SnapshotRejection),
    /// A checkpoint snapshot re-anchored the book.
    CheckpointEmitted {
        /// The snapshot's last update id.
        last_update_id: u64,
        /// The snapshot's time.
        time: EventTime,
    },
    /// A checkpoint snapshot stayed raw-only.
    CheckpointSkipped(CheckpointSkip),
}

/// Counters of the book sequencer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BookStats {
    /// Resyncs.
    pub syncs: u64,
    /// Unsynced periods, by cause.
    pub desyncs: BTreeMap<GapReason, u64>,
    /// Buffered diffs dropped as older than the snapshot.
    pub stale_diffs: u64,
    /// Diffs dropped because the unsynced buffer was full.
    pub buffer_drops: u64,
    /// Snapshots rejected while unsynced, by reason.
    pub snapshots_rejected: BTreeMap<SnapshotRejection, u64>,
    /// Snapshots that failed normalization.
    pub snapshot_errors: u64,
    /// Checkpoints re-anchored.
    pub checkpoints_emitted: u64,
    /// Checkpoints that stayed raw-only, by reason.
    pub checkpoints_skipped: BTreeMap<CheckpointSkip, u64>,
}

/// What one push produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BookOutput {
    /// Events to deliver, in emission order.
    pub events: Vec<MarketEvent>,
    /// Sync-state changes, in order.
    pub transitions: Vec<BookTransition>,
}

/// The ids and time of an emitted diff.
#[derive(Debug, Clone, Copy)]
struct Emitted {
    time: EventTime,
    first: u64,
    last: u64,
}

impl Emitted {
    fn of(update: &BookUpdate) -> Self {
        Self {
            time: update.time,
            first: update.first_update_id,
            last: update.last_update_id,
        }
    }
}

/// Binance's order-book sync procedure (ADR-038).
#[derive(Debug, Clone)]
pub struct BookSequencer {
    /// The previous run's last persisted book event time.
    seed: Option<EventTime>,
    /// Capture session of the last diff.
    session: Option<String>,
    synced: bool,
    /// The cause of the open unsynced period; `None` while synced and before
    /// the first diff.
    cause: Option<GapReason>,
    /// Diffs waiting for a snapshot, chained.
    buffer: VecDeque<BookUpdate>,
    /// The snapshot waiting for a straddling diff while unsynced.
    pending: Option<BookSnapshot>,
    /// A checkpoint waiting for its straddling diff while synced.
    checkpoint: Option<BookSnapshot>,
    /// The diffs emitted in the current synced period, newest last.
    recent: VecDeque<Emitted>,
    /// The greatest canonical key emitted so far.
    max_key: Option<CanonicalKey>,
    /// Time of the last emitted snapshot or update.
    last_time: Option<EventTime>,
    last_snapshot_id: Option<u64>,
    last_update_id: Option<u64>,
    want: u64,
    stats: BookStats,
}

impl BookSequencer {
    /// A sequencer seeded with the previous run's last persisted book event
    /// time.
    pub fn new(seed: Option<EventTime>) -> Self {
        Self {
            seed,
            session: None,
            synced: false,
            cause: None,
            buffer: VecDeque::new(),
            pending: None,
            checkpoint: None,
            recent: VecDeque::new(),
            max_key: None,
            last_time: None,
            last_snapshot_id: None,
            last_update_id: None,
            want: 0,
            stats: BookStats::default(),
        }
    }

    /// Takes the next diff, received in `session`; `floor` is the hold-back's
    /// last released event.
    pub fn push_diff(
        &mut self,
        session: &str,
        diff: Result<BookUpdate, NormalizeError>,
        floor: Option<&MarketEvent>,
    ) -> BookOutput {
        let mut out = BookOutput::default();
        let Ok(diff) = diff else {
            // Rule 8.
            self.unsync(GapReason::MissingData, &mut out);
            self.buffer.clear();
            return out;
        };
        if self.session.as_deref() != Some(session) {
            // Rule 1.
            self.session = Some(session.to_owned());
            self.unsync(GapReason::Disconnected, &mut out);
            self.buffer.clear();
            self.buffer.push_back(diff);
            return out;
        }
        if self.synced {
            if Some(diff.prev_update_id) == self.last_update_id {
                self.emit_chained(diff, floor, &mut out);
            } else {
                // Rule 2.
                self.unsync(GapReason::SequenceBreak, &mut out);
                self.buffer.clear();
                self.buffer.push_back(diff);
            }
            return out;
        }
        // Rule 3.
        let chains = self
            .buffer
            .back()
            .is_some_and(|tail| tail.last_update_id == diff.prev_update_id);
        if !chains {
            self.buffer.clear();
        }
        self.buffer.push_back(diff);
        while self.buffer.len() > MAX_UNSYNCED_DIFFS {
            self.buffer.pop_front();
            self.stats.buffer_drops += 1;
        }
        self.try_sync(floor, &mut out);
        out
    }

    /// Takes the next snapshot; `floor` is the hold-back's last released
    /// event.
    pub fn push_snapshot(
        &mut self,
        snapshot: Result<BookSnapshot, NormalizeError>,
        floor: Option<&MarketEvent>,
    ) -> BookOutput {
        let mut out = BookOutput::default();
        let Ok(snapshot) = snapshot else {
            // Rule 8.
            self.stats.snapshot_errors += 1;
            if !self.synced {
                self.want += 1;
            }
            return out;
        };
        if self.synced {
            self.checkpoint(snapshot, floor, &mut out);
            return out;
        }
        // Rule 4.
        let id = snapshot.last_update_id;
        let stale = self.last_snapshot_id.is_some_and(|last| id <= last)
            || self.last_update_id.is_some_and(|last| id < last)
            || self
                .pending
                .as_ref()
                .is_some_and(|pending| id <= pending.last_update_id);
        if stale {
            self.reject(SnapshotRejection::Stale, &mut out);
            return out;
        }
        self.pending = Some(snapshot);
        self.try_sync(floor, &mut out);
        out
    }

    /// Rule 7: makes the book unsynced with `cause` (the first cause of the
    /// period wins) and clears the buffer and the pending snapshot.
    pub fn desync(&mut self, cause: GapReason) -> Vec<BookTransition> {
        let mut out = BookOutput::default();
        self.unsync(cause, &mut out);
        self.buffer.clear();
        out.transitions
    }

    /// `Some(want id)` while the book needs a snapshot: unsynced, diffs
    /// buffered and none pending. The id changes whenever a new snapshot is
    /// needed, so a caller requests each id once.
    pub fn snapshot_wanted(&self) -> Option<u64> {
        (!self.synced && !self.buffer.is_empty() && self.pending.is_none()).then_some(self.want)
    }

    /// Whether the book is synced.
    pub fn is_synced(&self) -> bool {
        self.synced
    }

    /// The counters so far.
    pub fn stats(&self) -> &BookStats {
        &self.stats
    }

    /// Enters (or stays in) an unsynced period.
    fn unsync(&mut self, cause: GapReason, out: &mut BookOutput) {
        if self.synced || self.cause.is_none() {
            self.cause = Some(cause);
            *self.stats.desyncs.entry(cause).or_default() += 1;
            out.transitions.push(BookTransition::Desynced(cause));
        }
        self.synced = false;
        self.pending = None;
        self.recent.clear();
        if self.checkpoint.take().is_some() {
            self.skip(CheckpointSkip::Dropped, out);
        }
        self.want += 1;
    }

    fn reject(&mut self, reason: SnapshotRejection, out: &mut BookOutput) {
        self.pending = None;
        *self.stats.snapshots_rejected.entry(reason).or_default() += 1;
        out.transitions
            .push(BookTransition::SnapshotRejected(reason));
        self.want += 1;
    }

    fn skip(&mut self, reason: CheckpointSkip, out: &mut BookOutput) {
        *self.stats.checkpoints_skipped.entry(reason).or_default() += 1;
        out.transitions
            .push(BookTransition::CheckpointSkipped(reason));
    }

    /// Rule 5.
    fn try_sync(&mut self, floor: Option<&MarketEvent>, out: &mut BookOutput) {
        let Some(snapshot) = &self.pending else {
            return;
        };
        let id = snapshot.last_update_id;
        let last_update_id = self.last_update_id;
        while let Some(front) = self.buffer.front() {
            let u = front.last_update_id;
            if u < id || last_update_id.is_some_and(|last| u <= last) {
                self.buffer.pop_front();
                self.stats.stale_diffs += 1;
            } else {
                break;
            }
        }
        let Some(first) = self.buffer.front() else {
            return;
        };
        let reason = if first.first_update_id > id {
            Some(SnapshotRejection::TooOld)
        } else if first.time < snapshot.time {
            Some(SnapshotRejection::TimeOrder)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.reject(reason, out);
            return;
        }
        let gap =
            (self.stats.syncs > 0 || self.seed.is_some() || self.last_time.is_some()).then(|| {
                let end = snapshot.time;
                let start = self.last_time.or(self.seed).unwrap_or(end).min(end);
                MarketEvent::FeedGap(FeedGap {
                    stream: Stream::OrderBook,
                    start,
                    end,
                    reason: self.cause.unwrap_or(GapReason::Disconnected),
                })
            });
        let Some(snapshot) = self.pending.take() else {
            return;
        };
        let snapshot = MarketEvent::BookSnapshot(snapshot);
        let lead = gap.as_ref().unwrap_or(&snapshot);
        let above_floor = floor.is_none_or(|floor| lead > floor);
        let above_emitted = self.max_key.is_none_or(|key| lead.canonical_key() > key);
        if !(above_floor && above_emitted) {
            self.reject(SnapshotRejection::Late, out);
            return;
        }
        let MarketEvent::BookSnapshot(anchor) = &snapshot else {
            unreachable!("built as a snapshot above");
        };
        let (anchor_id, anchor_time) = (anchor.last_update_id, anchor.time);
        out.events.extend(gap);
        self.note(&snapshot);
        self.last_snapshot_id = Some(anchor_id);
        out.events.push(snapshot);
        self.synced = true;
        self.cause = None;
        self.recent.clear();
        self.stats.syncs += 1;
        out.transitions.push(BookTransition::Synced {
            last_update_id: anchor_id,
            time: anchor_time,
        });
        for diff in std::mem::take(&mut self.buffer) {
            self.emit_update(diff, out);
        }
    }

    /// A chained diff while synced; decides a waiting checkpoint first.
    fn emit_chained(
        &mut self,
        diff: BookUpdate,
        floor: Option<&MarketEvent>,
        out: &mut BookOutput,
    ) {
        if let Some(checkpoint) = self.checkpoint.take() {
            if diff.last_update_id >= checkpoint.last_update_id {
                let prev = self.recent.back().copied();
                match prev {
                    Some(prev) => {
                        self.decide(checkpoint, prev, Emitted::of(&diff), floor, out);
                    }
                    None => self.skip(CheckpointSkip::OutOfRecent, out),
                }
            } else {
                self.checkpoint = Some(checkpoint);
            }
        }
        self.emit_update(diff, out);
    }

    /// Rule 6.
    fn checkpoint(
        &mut self,
        snapshot: BookSnapshot,
        floor: Option<&MarketEvent>,
        out: &mut BookOutput,
    ) {
        let id = snapshot.last_update_id;
        if self.last_snapshot_id.is_some_and(|last| id <= last) {
            self.skip(CheckpointSkip::NotNewer, out);
            return;
        }
        match self.recent.iter().position(|d| d.last >= id) {
            Some(0) => self.skip(CheckpointSkip::OutOfRecent, out),
            Some(index) => {
                let (prev, star) = (self.recent[index - 1], self.recent[index]);
                self.decide(snapshot, prev, star, floor, out);
            }
            None if self.recent.is_empty() => self.skip(CheckpointSkip::OutOfRecent, out),
            None => {
                if self.checkpoint.replace(snapshot).is_some() {
                    self.skip(CheckpointSkip::Dropped, out);
                }
            }
        }
    }

    /// Emits `snapshot` as a re-anchor between `prev` and `star`, or skips
    /// it.
    fn decide(
        &mut self,
        snapshot: BookSnapshot,
        prev: Emitted,
        star: Emitted,
        floor: Option<&MarketEvent>,
        out: &mut BookOutput,
    ) {
        let id = snapshot.last_update_id;
        let reason = if self.last_snapshot_id.is_some_and(|last| id <= last) {
            Some(CheckpointSkip::NotNewer)
        } else if !(prev.time < snapshot.time && snapshot.time <= star.time) {
            Some(CheckpointSkip::TimeOrder)
        } else if star.first > id {
            Some(CheckpointSkip::NoStraddle)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.skip(reason, out);
            return;
        }
        let time = snapshot.time;
        let event = MarketEvent::BookSnapshot(snapshot);
        if floor.is_some_and(|floor| event <= *floor) {
            self.skip(CheckpointSkip::Late, out);
            return;
        }
        self.note(&event);
        self.last_snapshot_id = Some(id);
        self.stats.checkpoints_emitted += 1;
        out.transitions.push(BookTransition::CheckpointEmitted {
            last_update_id: id,
            time,
        });
        out.events.push(event);
    }

    fn emit_update(&mut self, diff: BookUpdate, out: &mut BookOutput) {
        self.recent.push_back(Emitted::of(&diff));
        if self.recent.len() > RECENT_DIFFS {
            self.recent.pop_front();
        }
        self.last_update_id = Some(diff.last_update_id);
        let event = MarketEvent::BookUpdate(diff);
        self.note(&event);
        out.events.push(event);
    }

    /// Tracks the time and canonical key of an emitted snapshot or update.
    fn note(&mut self, event: &MarketEvent) {
        let time = event.time();
        self.last_time = Some(self.last_time.map_or(time, |last| last.max(time)));
        let key = event.canonical_key();
        self.max_key = Some(self.max_key.map_or(key, |max| max.max(key)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::book::{BookStep, OrderBook};
    use mie_domain::event::Level;
    use mie_domain::num::{Price, Qty};
    use mie_domain::state::MarketStateEngine;

    fn t(millis: i64) -> EventTime {
        EventTime::from_millis(millis)
    }

    /// A diff covering ids `first..=last` after `prev`, at `time`, that sets
    /// one bid level per id range.
    fn diff(first: u64, last: u64, prev: u64, time: i64) -> BookUpdate {
        BookUpdate {
            time: t(time),
            first_update_id: first,
            last_update_id: last,
            prev_update_id: prev,
            bids: vec![Level {
                price: Price::from_units(i64::try_from(first).unwrap() * 100),
                qty: Qty::from_units(i64::try_from(last).unwrap()),
            }],
            asks: Vec::new(),
        }
    }

    fn snap(last: u64, time: i64) -> BookSnapshot {
        BookSnapshot {
            time: t(time),
            last_update_id: last,
            bids: vec![Level {
                price: Price::from_units(1),
                qty: Qty::from_units(1),
            }],
            asks: Vec::new(),
        }
    }

    /// Drives a sequencer and collects everything it emitted.
    struct Run {
        seq: BookSequencer,
        events: Vec<MarketEvent>,
        transitions: Vec<BookTransition>,
        floor: Option<MarketEvent>,
    }

    impl Run {
        fn new(seed: Option<i64>) -> Self {
            Self {
                seq: BookSequencer::new(seed.map(t)),
                events: Vec::new(),
                transitions: Vec::new(),
                floor: None,
            }
        }

        fn take(&mut self, out: BookOutput) -> BookOutput {
            self.events.extend(out.events.iter().cloned());
            self.transitions.extend(out.transitions.iter().copied());
            out
        }

        fn diff(&mut self, session: &str, diff: BookUpdate) -> BookOutput {
            let out = self.seq.push_diff(session, Ok(diff), self.floor.as_ref());
            self.take(out)
        }

        fn snap(&mut self, snapshot: BookSnapshot) -> BookOutput {
            let out = self.seq.push_snapshot(Ok(snapshot), self.floor.as_ref());
            self.take(out)
        }

        /// The emitted events in canonical order, checked by the engine and
        /// a domain book: only an `OrderBook` gap may invalidate it.
        fn verified(&self) -> Vec<MarketEvent> {
            let mut events = self.events.clone();
            events.sort();
            let mut engine = MarketStateEngine::new();
            let mut book = OrderBook::new();
            for (i, event) in events.iter().enumerate() {
                engine
                    .apply(event)
                    .unwrap_or_else(|e| panic!("event {i} rejected: {e}"));
                match (book.apply(event), event) {
                    (BookStep::Invalidated(_), MarketEvent::FeedGap(_)) => {}
                    (BookStep::Invalidated(why), _) => panic!("event {i} invalidated: {why:?}"),
                    _ => {}
                }
            }
            events
        }
    }

    fn kinds(events: &[MarketEvent]) -> Vec<String> {
        events
            .iter()
            .map(|e| match e {
                MarketEvent::FeedGap(g) => format!(
                    "gap {:?} {}..{}",
                    g.reason,
                    g.start.as_millis(),
                    g.end.as_millis()
                ),
                MarketEvent::BookSnapshot(s) => format!("snap {}", s.last_update_id),
                MarketEvent::BookUpdate(u) => format!("upd {}", u.last_update_id),
                other => format!("{other:?}"),
            })
            .collect()
    }

    #[test]
    fn a_valid_sequence_buffers_drops_stale_diffs_straddles_and_chains() {
        let mut run = Run::new(None);
        let out = run.diff("s", diff(1, 10, 0, 100));
        assert_eq!(
            out.transitions,
            [BookTransition::Desynced(GapReason::Disconnected)]
        );
        assert!(out.events.is_empty());
        let want = run.seq.snapshot_wanted().expect("wants a snapshot");
        run.diff("s", diff(11, 20, 10, 200));
        run.diff("s", diff(21, 30, 20, 300));
        // Still the same request: nothing changed.
        assert_eq!(run.seq.snapshot_wanted(), Some(want));
        let out = run.snap(snap(15, 150));
        // The first sync of an unseeded run has no gap.
        assert_eq!(kinds(&out.events), ["snap 15", "upd 20", "upd 30"]);
        assert_eq!(
            out.transitions,
            [BookTransition::Synced {
                last_update_id: 15,
                time: t(150)
            }]
        );
        assert_eq!(run.seq.snapshot_wanted(), None);
        assert!(run.seq.is_synced());
        let out = run.diff("s", diff(31, 40, 30, 400));
        assert_eq!(kinds(&out.events), ["upd 40"]);
        assert_eq!(run.seq.stats().stale_diffs, 1);
        assert_eq!(run.seq.stats().syncs, 1);
        run.verified();
    }

    #[test]
    fn a_pu_break_resyncs_behind_one_gap() {
        let mut run = Run::new(None);
        run.diff("s", diff(1, 10, 0, 100));
        run.snap(snap(5, 90));
        run.diff("s", diff(11, 20, 10, 200));
        let want = run.seq.snapshot_wanted();
        assert_eq!(want, None);
        // 21..=30 is missing.
        let out = run.diff("s", diff(31, 40, 30, 400));
        assert_eq!(
            out.transitions,
            [BookTransition::Desynced(GapReason::SequenceBreak)]
        );
        assert!(out.events.is_empty());
        let want = run.seq.snapshot_wanted().expect("wants a snapshot");
        // No update while unsynced; a second break keeps the first cause.
        assert!(run.diff("s", diff(41, 50, 40, 500)).events.is_empty());
        let out = run.diff("s", diff(61, 70, 60, 700));
        assert!(out.events.is_empty() && out.transitions.is_empty());
        assert_eq!(run.seq.snapshot_wanted(), Some(want));
        let out = run.snap(snap(65, 650));
        assert_eq!(
            kinds(&out.events),
            ["gap SequenceBreak 200..650", "snap 65", "upd 70"]
        );
        assert_eq!(run.seq.stats().desyncs[&GapReason::SequenceBreak], 1);
        assert_eq!(run.seq.stats().syncs, 2);
        run.verified();
    }

    #[test]
    fn rejections_ask_for_another_snapshot() {
        let mut run = Run::new(None);
        run.diff("s", diff(50, 60, 40, 100));
        let first = run.seq.snapshot_wanted().unwrap();
        // The buffer starts after 45: the ids 41..=49 are unknown.
        let out = run.snap(snap(45, 90));
        assert_eq!(
            out.transitions,
            [BookTransition::SnapshotRejected(SnapshotRejection::TooOld)]
        );
        let second = run.seq.snapshot_wanted().unwrap();
        assert_ne!(first, second);
        // The straddling diff is older than the snapshot.
        let out = run.snap(snap(55, 101));
        assert_eq!(
            out.transitions,
            [BookTransition::SnapshotRejected(
                SnapshotRejection::TimeOrder
            )]
        );
        assert_ne!(run.seq.snapshot_wanted(), Some(second));
        // Below the floor.
        run.floor = Some(MarketEvent::BookUpdate(diff(1, 1, 0, 95)));
        let out = run.snap(snap(56, 95));
        assert_eq!(
            out.transitions,
            [BookTransition::SnapshotRejected(SnapshotRejection::Late)]
        );
        run.floor = None;
        let out = run.snap(snap(57, 100));
        assert_eq!(kinds(&out.events), ["snap 57", "upd 60"]);
        // Stale: not above the snapshot already delivered.
        run.diff("s", diff(70, 80, 65, 300));
        let out = run.snap(snap(57, 310));
        assert_eq!(
            out.transitions,
            [BookTransition::SnapshotRejected(SnapshotRejection::Stale)]
        );
        // Stale: below the last update delivered.
        let out = run.snap(snap(59, 310));
        assert_eq!(
            out.transitions,
            [BookTransition::SnapshotRejected(SnapshotRejection::Stale)]
        );
        let stats = run.seq.stats();
        for (reason, n) in [
            (SnapshotRejection::TooOld, 1),
            (SnapshotRejection::TimeOrder, 1),
            (SnapshotRejection::Late, 1),
            (SnapshotRejection::Stale, 2),
        ] {
            assert_eq!(stats.snapshots_rejected[&reason], n, "{reason:?}");
        }
        run.verified();
    }

    #[test]
    fn a_pending_snapshot_waits_for_its_straddling_diff() {
        let mut run = Run::new(None);
        run.diff("s", diff(1, 10, 0, 100));
        assert!(run.snap(snap(25, 250)).events.is_empty());
        // Pending: nothing more is wanted.
        assert_eq!(run.seq.snapshot_wanted(), None);
        assert!(run.diff("s", diff(11, 20, 10, 200)).events.is_empty());
        let out = run.diff("s", diff(21, 30, 20, 300));
        assert_eq!(kinds(&out.events), ["snap 25", "upd 30"]);
        assert_eq!(run.seq.stats().stale_diffs, 2);
        // A newer snapshot than the pending one replaces it; an older one is
        // stale.
        let mut run = Run::new(None);
        run.diff("s", diff(1, 10, 0, 100));
        run.snap(snap(25, 250));
        assert_eq!(
            run.snap(snap(24, 240)).transitions,
            [BookTransition::SnapshotRejected(SnapshotRejection::Stale)]
        );
        run.snap(snap(35, 350));
        run.diff("s", diff(11, 20, 10, 200));
        run.diff("s", diff(21, 30, 20, 300));
        let out = run.diff("s", diff(31, 40, 30, 400));
        assert_eq!(kinds(&out.events), ["snap 35", "upd 40"]);
        run.verified();
    }

    #[test]
    fn a_session_change_is_a_disconnect() {
        let mut run = Run::new(None);
        run.diff("a", diff(1, 10, 0, 100));
        run.snap(snap(5, 90));
        let out = run.diff("b", diff(11, 20, 10, 200));
        // Even a chaining diff: the new connection may have missed nothing,
        // but the reconnect is announced.
        assert_eq!(
            out.transitions,
            [BookTransition::Desynced(GapReason::Disconnected)]
        );
        assert!(out.events.is_empty());
        let out = run.snap(snap(15, 150));
        assert_eq!(
            kinds(&out.events),
            ["gap Disconnected 100..150", "snap 15", "upd 20"]
        );
        run.verified();
    }

    #[test]
    fn a_seeded_run_opens_with_a_gap_from_the_seed() {
        let mut run = Run::new(Some(40));
        run.diff("s", diff(1, 10, 0, 100));
        let out = run.snap(snap(5, 90));
        assert_eq!(
            kinds(&out.events),
            ["gap Disconnected 40..90", "snap 5", "upd 10"]
        );
        // A seed after the snapshot is clamped to the gap's end.
        let mut run = Run::new(Some(500));
        run.diff("s", diff(1, 10, 0, 100));
        let out = run.snap(snap(5, 90));
        assert_eq!(kinds(&out.events)[0], "gap Disconnected 90..90");
        run.verified();
    }

    #[test]
    fn normalize_errors_desync_or_ask_again() {
        let mut run = Run::new(None);
        run.diff("s", diff(1, 10, 0, 100));
        run.snap(snap(5, 90));
        let bad = NormalizeError::Missing("pu");
        let out = run.seq.push_diff("s", Err(bad), None);
        assert_eq!(
            out.transitions,
            [BookTransition::Desynced(GapReason::MissingData)]
        );
        // The buffer was cleared: nothing to sync until the next diff.
        assert_eq!(run.seq.snapshot_wanted(), None);
        run.diff("s", diff(11, 20, 10, 200));
        let want = run.seq.snapshot_wanted().unwrap();
        let out = run
            .seq
            .push_snapshot(Err(NormalizeError::Missing("bids")), None);
        assert_eq!(out, BookOutput::default());
        assert_ne!(run.seq.snapshot_wanted(), Some(want));
        assert_eq!(run.seq.stats().snapshot_errors, 1);
        let out = run.snap(snap(15, 150));
        assert_eq!(
            kinds(&out.events),
            ["gap MissingData 100..150", "snap 15", "upd 20"]
        );
        run.verified();
    }

    #[test]
    fn the_unsynced_buffer_is_bounded_and_restarts_on_breaks() {
        let mut run = Run::new(None);
        let n = u64::try_from(MAX_UNSYNCED_DIFFS).unwrap() + 5;
        for i in 0..n {
            run.diff("s", diff(i * 10 + 1, i * 10 + 10, i * 10, 100 + i as i64));
        }
        assert_eq!(run.seq.stats().buffer_drops, 5);
        // A break restarts the buffer: the snapshot can only straddle the
        // new chain.
        let base = n * 10;
        run.diff("s", diff(base + 101, base + 110, base + 100, 5_000));
        let out = run.snap(snap(base + 50, 4_000));
        assert_eq!(
            out.transitions,
            [BookTransition::SnapshotRejected(SnapshotRejection::TooOld)]
        );
        let out = run.snap(snap(base + 105, 4_900));
        assert_eq!(
            kinds(&out.events),
            [
                format!("snap {}", base + 105),
                format!("upd {}", base + 110)
            ]
        );
        run.verified();
    }

    /// Synced at snapshot 15 with diffs up to 60, 100 ms apart.
    fn synced() -> Run {
        let mut run = Run::new(None);
        run.diff("s", diff(11, 20, 10, 200));
        run.snap(snap(15, 150));
        for i in 2..6 {
            run.diff(
                "s",
                diff(i * 10 + 1, i * 10 + 10, i * 10, i as i64 * 100 + 100),
            );
        }
        assert!(run.seq.is_synced());
        run
    }

    #[test]
    fn a_checkpoint_after_its_straddling_diff_re_anchors() {
        let mut run = synced();
        // Diffs 41..=50 at 500 and 51..=60 at 600: 45 lies in the first.
        let out = run.snap(snap(45, 450));
        assert_eq!(kinds(&out.events), ["snap 45"]);
        assert_eq!(
            out.transitions,
            [BookTransition::CheckpointEmitted {
                last_update_id: 45,
                time: t(450)
            }]
        );
        // Not newer than the checkpoint already delivered.
        let out = run.snap(snap(45, 455));
        assert_eq!(
            out.transitions,
            [BookTransition::CheckpointSkipped(CheckpointSkip::NotNewer)]
        );
        assert_eq!(run.seq.stats().checkpoints_emitted, 1);
        let events = run.verified();
        let at = events
            .iter()
            .position(|e| matches!(e, MarketEvent::BookSnapshot(s) if s.last_update_id == 45))
            .unwrap();
        assert!(matches!(&events[at + 1], MarketEvent::BookUpdate(u) if u.last_update_id == 50));
    }

    #[test]
    fn a_checkpoint_before_its_straddling_diff_waits_for_it() {
        let mut run = synced();
        let out = run.snap(snap(65, 650));
        assert_eq!(out, BookOutput::default());
        let out = run.diff("s", diff(61, 70, 60, 700));
        assert_eq!(kinds(&out.events), ["snap 65", "upd 70"]);
        // A diff below the checkpoint's id keeps it waiting.
        run.snap(snap(85, 790));
        let out = run.diff("s", diff(71, 80, 70, 780));
        assert_eq!(kinds(&out.events), ["upd 80"]);
        let out = run.diff("s", diff(81, 90, 80, 800));
        assert_eq!(kinds(&out.events), ["snap 85", "upd 90"]);
        assert_eq!(run.seq.stats().checkpoints_emitted, 2);
        run.verified();
    }

    #[test]
    fn inconsistent_checkpoints_stay_raw() {
        let skipped = |run: &mut Run, snapshot: BookSnapshot| {
            let out = run.snap(snapshot);
            assert!(out.events.is_empty(), "{:?}", out.events);
            out.transitions
        };
        // The diff before the straddling one has the snapshot's time.
        let mut run = synced();
        assert_eq!(
            skipped(&mut run, snap(45, 400)),
            [BookTransition::CheckpointSkipped(CheckpointSkip::TimeOrder)]
        );
        // The straddling diff is the anchor's first: no predecessor.
        assert_eq!(
            skipped(&mut run, snap(18, 190)),
            [BookTransition::CheckpointSkipped(
                CheckpointSkip::OutOfRecent
            )]
        );
        // Below the floor.
        run.floor = Some(MarketEvent::BookUpdate(diff(51, 60, 50, 600)));
        assert_eq!(
            skipped(&mut run, snap(45, 450)),
            [BookTransition::CheckpointSkipped(CheckpointSkip::Late)]
        );
        run.floor = None;
        // Further back than the remembered diffs.
        let mut run = synced();
        for i in 6..(6 + RECENT_DIFFS as u64) {
            run.diff(
                "s",
                diff(i * 10 + 1, i * 10 + 10, i * 10, i as i64 * 100 + 100),
            );
        }
        assert_eq!(
            skipped(&mut run, snap(45, 450)),
            [BookTransition::CheckpointSkipped(
                CheckpointSkip::OutOfRecent
            )]
        );
        // A desync drops a waiting checkpoint.
        let mut run = synced();
        assert!(skipped(&mut run, snap(75, 750)).is_empty());
        let transitions = run.seq.desync(GapReason::SequenceBreak);
        assert_eq!(
            transitions,
            [
                BookTransition::Desynced(GapReason::SequenceBreak),
                BookTransition::CheckpointSkipped(CheckpointSkip::Dropped)
            ]
        );
        assert_eq!(run.seq.snapshot_wanted(), None, "the buffer was cleared");
        run.verified();
    }

    #[test]
    fn a_resync_snapshot_must_sort_after_every_emitted_book_event() {
        let mut run = synced();
        // A break, then a snapshot whose gap would sort before the last
        // emitted update (same millisecond, a gap ranks first).
        run.diff("s", diff(71, 80, 70, 800));
        let out = run.snap(snap(75, 600));
        assert_eq!(
            out.transitions,
            [BookTransition::SnapshotRejected(SnapshotRejection::Late)]
        );
        let out = run.snap(snap(76, 750));
        assert_eq!(
            kinds(&out.events),
            ["gap SequenceBreak 600..750", "snap 76", "upd 80"]
        );
        run.verified();
    }
}
