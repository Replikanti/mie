//! The capture journal: one JSON object per line, appended.
//!
//! Every line carries `type`, `run_id` and `at_ms` (wall clock, for humans
//! and the soak report; it orders nothing) and, as its last member,
//! `crc32c`, the line's own check (ADR-066, below). Line types:
//!
//! | `type` | Written by | Content |
//! |---|---|---|
//! | `run_start` | ingest | symbol, streams, hold-back, open-interest re-time allowance, seeds per stream: the run's pipeline parameters ([`RunParameters`]); depth snapshot limit and checkpoint interval (informational); the state checkpoint interval (`state_checkpoint_interval_ms`), the state and event-stream hash encodings and the engine's feature set (16 hex digits): the comparability keys of the equivalence harness ([`Checkpointing`], ADR-041); `history_origin_ms` and `history_cutover_ms`, the history chain's bounds (null without `[history]`, [`RunHistory`], ADR-052 D6); in a tiered store only, `seed_listing`, `"union"` or `"hot"`, and with `"hot"` `seed_listing_error`, the union `select`'s error that made the seed scan fall back to hot's listing (ADR-062 D7 *Seeds*) |
//! | `warm_start` | ingest | how the core's engine started, one per run ([`WarmStart`], ADR-052 D6): `warm` true with `origin_ms`, `effective_origin_ms` (ms or null), `cutover_ms`, the prefix's `prefix_dataset` (64 hex), `prefix_last_event_ms` (ms or null), `prefix_events`, `prefix_event_hash` (16 hex) and `prefix_rejections`, the `handoff_state_hash` (16 hex), the `seeds` the core's pipeline uses (raw stream → ms), the queue's `queue_high_water` (records) and `queue_high_water_bytes` (payload bytes), and `warming_up`, the long-warm-up features of ADR-041 still warming up at the handoff; or `warm` false with `cold` (`no-history`, `before-cutover`, `warm-up-failed`), `error` (text or null) and, with `[history]`, `origin_ms` and `cutover_ms` |
//! | `warm_ready` | ingest | `ready_after_ms`: wall time from `run_start` until the warm engine has applied the queue (ADR-052 D6, D12) |
//! | `chain_restart` | ingest | `time` (ms): an uncrossable hole restarted the warm engine in the run's live part (ADR-052 D4); written and synced when the restart happens |
//! | `recovery` | ingest | parts rolled forward and discarded by the store |
//! | `connected`, `connect_failed`, `disconnected`, `planned_rotation`, `backoff` | capture | connection lifecycle per stream |
//! | `oi_poll` | capture | request/response time (ns), status, whether persisted |
//! | `depth_snapshot_fetch` | capture | `trigger` (sync, checkpoint), request/response time (ns), status, whether persisted |
//! | `book` | capture | an order-book sync change: `event` = desync, sync, snapshot_rejected, checkpoint_emitted, checkpoint_skipped; its cause, reason or id; the `receive_seq` of the record that caused it |
//! | `book_checkpoint` | capture | the audit of a checkpoint: `result` = matched, inconclusive, mismatched, unverifiable, invalidated; matched and inconclusive carry `levels` (total), `levels_bid`, `levels_ask` and `window_bid_cbps`/`window_ask_cbps` (window width in hundredths of a bp, ADR-049), inconclusive also `min_side_levels`; mismatched carries levels compared and mismatch examples |
//! | `gap` | capture | `stream`, `start`, `end` (ms), `reason`, as delivered to the core |
//! | `sealed` | capture | the files sealed |
//! | `normalize_error` | capture | stream, `receive_seq`, error |
//! | `stats` | capture | counters (the order-book sync counters under `streams.depth.book`), channel high-water marks and blocked time |
//! | `domain_rejection` | ingest | an event the engine rejected |
//! | `state_checkpoint` | ingest | `ordinal` (events delivered so far, rejections included), `as_of` (ms or null), `event_hash` and `state_hash` (16 hex digits), `last`: a checkpoint of the core (ADR-041) |
//! | `run_end` | ingest | totals (`events`, `domain_rejections`, `records`) and the exit code |
//! | `replication` | ingest | one per replication pass of a tiered store (ADR-062 D11, after every seal pass, on the `mie-replicate` thread): `passed`, `files_copied`, `entries_copied`, `ledger_bytes`, `journal_bytes`, `problems` (a count), `first_problem` (text or null; `mie store replicate` prints the full list), `candidates` (the partitions planned for removal from hot, or null without a plan) and `removal_waiting` (text or null: after a copy-ledger rebuild, why the plan waits for a verification covering the rebuilt ledger, ADR-062 D7 step 6, #193 B10). The final pass's line lands after `run_end` |
//! | `copy_ledger_truncated` | ingest | an append truncated the copy ledger's incomplete trailing record (ADR-062 D7): `ledger` (path), `bytes`, `offset` |
//! | `removal` | ingest | a hot-removal pass in the seal path that did something (ADR-062 D6): `source`, `removed` and `completed` (partitions), `skipped` (`[{partition, reason}]`), `locked` (the source's lock was held by an import) and `error` (text or null) |
//! | `recall` | ingest | the live writer recalled a partition removed from hot (ADR-062 D6): `partition`, `files`, `from` (`{cold root: files}`) and `damaged` (`[{path, root, error}]`, the damaged copies passed over) |
//!
//! The journal is flushed on every `stats` line and at the end of a run, and
//! synced to stable storage right after `run_start` and at the end of a run:
//! replay needs a run's `run_start` for every record the run seals
//! (ADR-039 D2). A warm `warm_start` is synced before the core uses its
//! seeds, and a cold one after a failed warm-up before the core drives.
//!
//! **The line check** (ADR-066 D1, D2). [`Journal::write`] serializes a
//! line as before, computes the CRC-32C of those bytes, and ends the line
//! with one more member, `,"crc32c":"<8 lowercase hex>"`, appended as bytes
//! in place of the closing `}` ([`with_check`]). So a damaged line
//! identifies itself with no other copy: [`check_line`] removes the member
//! and recomputes, with no JSON parse, and calls a line
//! [`LineCheck::Intact`], [`LineCheck::Failed`] or [`LineCheck::Unchecked`]
//! (no member of that shape: a line from before ADR-066, a torn line, a
//! line longer than [`MAX_CHECKED_LINE`]). The name `crc32c` is reserved:
//! the writer drops a caller field of that name at every depth. The check
//! guards against accidental damage only: it is no MAC.
//! Lines written before ADR-066 never gain it. Readers ignore the member:
//! they index named keys, so a line reads the same with it or without it.
//!
//! **The gate** (ADR-068). [`read_runs`] and [`warm_handoffs`] recompute
//! from the journal, so they do not read a line whose check fails when its
//! kind may be one they consume: they stop and name the line and both
//! repairs, unless a lost-line record (ADR-066 D8) accepts its loss. A
//! recorded line is read without its bytes, by a rule that marks or
//! excludes runs and is printed as a note. `capture-report` and the
//! rebuild's B1 are not gated (ADR-068, the reader verdicts).
//!
//! The raw store alone does not determine what live delivered: the
//! pipeline's `hold_back_ms`, `oi_retime_ms` and per-stream seeds are run
//! parameters, kept only in `run_start`. A recompute (replay #11, equivalence harness #13)
//! reads them with [`RunParameters::from_run_start`], never from
//! configuration defaults (ADR-032). [`read_runs`] joins every run's
//! `run_start` with its `run_end` into the [`LiveRun`]s `mie replay`
//! recomputes (ADR-039 D1), and collects the run's `state_checkpoint`
//! lines for `mie equivalence` (ADR-041) and its `warm_start`, `warm_ready`
//! and `chain_restart` lines (ADR-052).
//!
//! **Warm runs** (ADR-052 D3). The core of a warm-started run recomputes the
//! run's records with the history chain's seeds, journaled in `warm_start`,
//! while `run_start` keeps the seeds of the capture's own pipeline (the
//! 7-day lookback). That pipeline still writes the `gap` lines, so a warm
//! run's `gap` lines describe the capture's pipeline, not what reached the
//! engine. [`JournaledRun::live_run`] takes a warm run's seeds from
//! `warm_start`: they are the seeds of the pipeline whose output reached the
//! engine, and the history chain keeps them, so it recomputes the run
//! exactly as it was consumed.

use crate::config::Paths;
use crate::ingest::tiers::Recalled;
use crate::store::PassReport;
use mie_adapter_binance::live::ChannelStats;
use mie_adapter_binance::{
    BinanceStream, BookStats, BookTransition, CaptureEvent, CaptureObserver, CheckpointResult,
    LiveRun, MIN_SIDE_LEVELS, PipelineStats, SideCoverage, SnapshotTrigger,
};
use mie_adapter_parquet::copy_ledger::{self, Truncation};
use mie_adapter_parquet::removal::{RemovalPlan, RemovalReport};
use mie_adapter_parquet::replica::{self, JournalLock};
use mie_adapter_parquet::verification::{CopyState, LostLine};
use mie_app::equivalence::StateCheckpoint;
use mie_domain::book::{Invalidation, Side};
use mie_domain::event_hash::EventStreamHash;
use mie_domain::feature::FeatureSetVersion;
use mie_domain::fingerprint::Fingerprint;
use mie_domain::state_hash::StateHash;
use mie_domain::time::EventTime;
use mie_ports::raw::{DatasetVersion, SealedFile};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// The inputs of a run's [`Pipeline`](mie_adapter_binance::Pipeline), as
/// journaled in its `run_start` line. With the run's raw records in
/// `receive_seq` order they determine everything live delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunParameters {
    /// The run.
    pub run_id: String,
    /// The exchange symbol.
    pub symbol: String,
    /// The hold-back in exchange milliseconds.
    pub hold_back_ms: i64,
    /// The open-interest re-time allowance in milliseconds (ADR-032 D12).
    pub oi_retime_ms: i64,
    /// The restart seeds per stream.
    pub seeds: BTreeMap<BinanceStream, EventTime>,
}

impl RunParameters {
    /// Reads the parameters from a `run_start` journal line.
    ///
    /// # Errors
    ///
    /// A description of a missing or malformed field.
    pub fn from_run_start(line: &Value) -> Result<Self, String> {
        if line["type"] != "run_start" {
            return Err(format!("not a run_start line: {line}"));
        }
        let text = |name: &str| {
            line[name]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("run_start without {name}"))
        };
        let hold_back_ms = line["hold_back_ms"]
            .as_i64()
            .ok_or("run_start without hold_back_ms")?;
        let oi_retime_ms = line["oi_retime_ms"]
            .as_i64()
            .ok_or("run_start without oi_retime_ms")?;
        let seeds = seeds_from_json(&line["seeds"], "run_start")?;
        Ok(Self {
            run_id: text("run_id")?,
            symbol: text("symbol")?,
            hold_back_ms,
            oi_retime_ms,
            seeds,
        })
    }
}

/// One run of the journal: its `run_start` joined with its `run_end` and
/// its state checkpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournaledRun {
    /// The pipeline parameters of `run_start`.
    pub parameters: RunParameters,
    /// The raw-store source the run wrote to.
    pub source: String,
    /// The captured streams.
    pub streams: Vec<BinanceStream>,
    /// Wall clock of `run_start`, UTC ms.
    pub started_at_ms: i64,
    /// The run's `run_end`, if it has one (a crashed run has none).
    pub end: Option<RunEnd>,
    /// How the run recorded state checkpoints; `None` for a run journaled
    /// before checkpoints existed (#13).
    pub checkpointing: Option<Checkpointing>,
    /// The run's `state_checkpoint` lines, in journal order.
    pub checkpoints: Vec<StateCheckpoint>,
    /// The history chain's bounds from `run_start`; `None` without
    /// `[history]` or for a run journaled before ADR-052.
    pub history: Option<RunHistory>,
    /// How the core's engine started; `None` for a run journaled before
    /// ADR-052, or one that crashed before its `warm_start`.
    pub warm_start: Option<WarmStart>,
    /// `warm_ready`'s `ready_after_ms`, for a warm run that drained its
    /// queue.
    pub ready_after_ms: Option<i64>,
    /// The times of the run's `chain_restart` lines, in journal order.
    pub chain_restarts: Vec<EventTime>,
    /// The run's lines read without their bytes, which mark it (ADR-068
    /// D4): a lost `run_end`, `state_checkpoint` or `chain_restart`.
    pub lost: Vec<LostRef>,
}

/// The history chain's bounds a run journaled in `run_start` (ADR-052 D6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunHistory {
    /// The chain's origin, epoch ms.
    pub origin_ms: i64,
    /// The cutover between the archive and the live segment, epoch ms.
    pub cutover_ms: i64,
}

impl RunHistory {
    /// Reads `history_origin_ms` and `history_cutover_ms` from a `run_start`
    /// line: `None` when both are null or absent.
    ///
    /// # Errors
    ///
    /// A description when only one is set or one is not an integer.
    pub fn from_run_start(line: &Value) -> Result<Option<Self>, String> {
        let field = |name: &str| match &line[name] {
            Value::Null => Ok(None),
            value => value
                .as_i64()
                .map(Some)
                .ok_or_else(|| format!("run_start with a malformed {name}")),
        };
        match (field("history_origin_ms")?, field("history_cutover_ms")?) {
            (None, None) => Ok(None),
            (Some(origin_ms), Some(cutover_ms)) => Ok(Some(Self {
                origin_ms,
                cutover_ms,
            })),
            _ => Err(
                "run_start with only one of history_origin_ms and history_cutover_ms".to_owned(),
            ),
        }
    }
}

/// Why a run's core started with a cold engine (ADR-052 D8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdReason {
    /// The configuration has no `[history]`.
    NoHistory,
    /// The run started before the history cutover: it is not a chain run.
    BeforeCutover,
    /// A chain run whose warm-up failed, panicked, was refused by the memory
    /// guard (D13) or had not finished when the run ended.
    WarmUpFailed,
}

impl ColdReason {
    /// The journal's name of the reason.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoHistory => "no-history",
            Self::BeforeCutover => "before-cutover",
            Self::WarmUpFailed => "warm-up-failed",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        [Self::NoHistory, Self::BeforeCutover, Self::WarmUpFailed]
            .into_iter()
            .find(|reason| reason.as_str() == name)
    }
}

/// How a run's core started its engine: a run's `warm_start` line
/// (ADR-052 D6, module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarmStart {
    /// From the history chain through the run's predecessor.
    Warm(WarmHandoff),
    /// With a fresh engine.
    Cold(ColdStart),
}

/// A warm start's handoff: the prefix the warm engine was built from and
/// what the run's own records were recomputed with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmHandoff {
    /// The chain's origin, epoch ms.
    pub origin_ms: i64,
    /// The prefix's last restart after an uncrossable hole (ADR-052 D4);
    /// `None`: the origin itself.
    pub effective_origin_ms: Option<i64>,
    /// The cutover, epoch ms.
    pub cutover_ms: i64,
    /// The prefix's dataset version.
    pub prefix_dataset: DatasetVersion,
    /// The warm engine's `as_of` at the handoff; `None` for an empty prefix.
    pub prefix_last_event_ms: Option<i64>,
    /// The event-stream hash of the prefix (ADR-039 D7).
    pub prefix_stream: EventStreamHash,
    /// The prefix's events the domain rejected.
    pub prefix_rejections: u64,
    /// The Market State hash at the handoff (ADR-041 D1).
    pub handoff_state: StateHash,
    /// The seeds of the core's pipeline: the chain seeds (ADR-052 D3).
    pub seeds: BTreeMap<BinanceStream, EventTime>,
    /// Records queued during the warm-up, at its largest.
    pub queue_high_water: u64,
    /// Payload bytes queued during the warm-up, at its largest.
    pub queue_high_water_bytes: u64,
    /// ADR-041's long-warm-up features still warming up at the handoff.
    pub warming_up: Vec<String>,
}

/// A cold start's reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdStart {
    /// Why the engine started cold.
    pub reason: ColdReason,
    /// What failed, for [`ColdReason::WarmUpFailed`].
    pub error: Option<String>,
    /// The chain's origin, epoch ms, with `[history]`.
    pub origin_ms: Option<i64>,
    /// The cutover, epoch ms, with `[history]`.
    pub cutover_ms: Option<i64>,
}

/// The comparability keys of a run's state checkpoints, from `run_start`
/// (ADR-041).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpointing {
    /// The event-time cadence.
    pub interval_ms: i64,
    /// The Market State hash encoding.
    pub state_encoding: u32,
    /// The event-stream hash encoding.
    pub event_encoding: u32,
    /// The feature set of the run's engine.
    pub feature_set: FeatureSetVersion,
}

impl Checkpointing {
    /// Reads the keys from a `run_start` line: `None` when it has none.
    ///
    /// # Errors
    ///
    /// A description of a missing or malformed key.
    pub fn from_run_start(line: &Value) -> Result<Option<Self>, String> {
        if line.get("state_checkpoint_interval_ms").is_none() {
            return Ok(None);
        }
        let interval_ms = line["state_checkpoint_interval_ms"]
            .as_i64()
            .filter(|ms| *ms > 0)
            .ok_or("run_start with a malformed state_checkpoint_interval_ms")?;
        let encoding = |name: &str| {
            line[name]
                .as_u64()
                .and_then(|v| u32::try_from(v).ok())
                .ok_or_else(|| format!("run_start without {name}"))
        };
        let state_encoding = encoding("state_hash_encoding")?;
        let event_encoding = encoding("event_hash_encoding")?;
        let feature_set = line["feature_set"]
            .as_str()
            .and_then(parse_hex)
            .ok_or("run_start without a feature_set of 16 hex digits")?;
        Ok(Some(Self {
            interval_ms,
            state_encoding,
            event_encoding,
            feature_set: FeatureSetVersion::from_fingerprint(feature_set),
        }))
    }
}

/// What `run_end` says about a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunEnd {
    /// Wall clock of `run_end`, UTC ms.
    pub at_ms: i64,
    /// The run's exit code; 0 for a clean run.
    pub exit_code: i64,
    /// Records the run appended to the raw store.
    pub records: u64,
    /// Events the core consumed, rejections included; `None` when the line
    /// lacks it.
    pub events: Option<u64>,
    /// Events the engine rejected; `None` when the line lacks it.
    pub domain_rejections: Option<u64>,
}

/// A `state_checkpoint` journal line as JSON fields (module docs).
pub fn state_checkpoint_json(checkpoint: &StateCheckpoint) -> Value {
    json!({
        "ordinal": checkpoint.ordinal,
        "as_of": checkpoint.as_of.map(EventTime::as_millis),
        "event_hash": checkpoint.events.fingerprint.to_string(),
        "state_hash": checkpoint.state.to_string(),
        "last": checkpoint.last,
    })
}

/// A `warm_start` journal line as JSON fields (module docs).
pub fn warm_start_json(start: &WarmStart) -> Value {
    match start {
        WarmStart::Warm(warm) => json!({
            "warm": true,
            "origin_ms": warm.origin_ms,
            "effective_origin_ms": warm.effective_origin_ms,
            "cutover_ms": warm.cutover_ms,
            "prefix_dataset": warm.prefix_dataset.as_str(),
            "prefix_last_event_ms": warm.prefix_last_event_ms,
            "prefix_events": warm.prefix_stream.events,
            "prefix_event_hash": warm.prefix_stream.fingerprint.to_string(),
            "prefix_rejections": warm.prefix_rejections,
            "handoff_state_hash": warm.handoff_state.to_string(),
            "seeds": seeds_json(&warm.seeds),
            "queue_high_water": warm.queue_high_water,
            "queue_high_water_bytes": warm.queue_high_water_bytes,
            "warming_up": warm.warming_up,
        }),
        WarmStart::Cold(cold) => {
            let mut value = json!({
                "warm": false,
                "cold": cold.reason.as_str(),
                "error": cold.error,
            });
            if let Some(origin_ms) = cold.origin_ms {
                value["origin_ms"] = json!(origin_ms);
            }
            if let Some(cutover_ms) = cold.cutover_ms {
                value["cutover_ms"] = json!(cutover_ms);
            }
            value
        }
    }
}

/// A `warm_ready` journal line as JSON fields (module docs).
pub fn warm_ready_json(ready_after_ms: i64) -> Value {
    json!({ "ready_after_ms": ready_after_ms })
}

/// A `chain_restart` journal line as JSON fields (module docs).
pub fn chain_restart_json(time: EventTime) -> Value {
    json!({ "time": time.as_millis() })
}

/// Seeds per stream as journal JSON: raw stream name → ms.
pub fn seeds_json(seeds: &BTreeMap<BinanceStream, EventTime>) -> Value {
    Value::Object(
        seeds
            .iter()
            .map(|(s, t)| (s.raw_name().to_owned(), json!(t.as_millis())))
            .collect(),
    )
}

/// Reads seeds per stream from journal JSON (raw stream name → ms); `what`
/// names the line in errors.
fn seeds_from_json(
    value: &Value,
    what: &str,
) -> Result<BTreeMap<BinanceStream, EventTime>, String> {
    let journaled = value
        .as_object()
        .ok_or_else(|| format!("{what} without seeds"))?;
    let mut seeds = BTreeMap::new();
    for (name, millis) in journaled {
        let stream = BinanceStream::from_raw_name(name)
            .ok_or_else(|| format!("{what} seed of unknown stream {name:?}"))?;
        let millis = millis
            .as_i64()
            .ok_or_else(|| format!("{what} seed of {name} is not an integer"))?;
        seeds.insert(stream, EventTime::from_millis(millis));
    }
    Ok(seeds)
}

/// Reads a `warm_start` journal line.
///
/// # Errors
///
/// A description of a missing or malformed field.
pub fn warm_start_from_json(line: &Value) -> Result<WarmStart, String> {
    let optional_ms = |name: &str| match &line[name] {
        Value::Null => Ok(None),
        value => value
            .as_i64()
            .map(Some)
            .ok_or_else(|| format!("warm_start with a malformed {name}")),
    };
    let warm = line["warm"].as_bool().ok_or("warm_start without warm")?;
    if !warm {
        let reason = line["cold"]
            .as_str()
            .and_then(ColdReason::parse)
            .ok_or("warm_start without a cold reason")?;
        let error = match &line["error"] {
            Value::Null => None,
            Value::String(error) => Some(error.clone()),
            _ => return Err("warm_start with a malformed error".to_owned()),
        };
        return Ok(WarmStart::Cold(ColdStart {
            reason,
            error,
            origin_ms: optional_ms("origin_ms")?,
            cutover_ms: optional_ms("cutover_ms")?,
        }));
    }
    let ms = |name: &str| {
        line[name]
            .as_i64()
            .ok_or_else(|| format!("warm_start without {name}"))
    };
    let count = |name: &str| {
        line[name]
            .as_u64()
            .ok_or_else(|| format!("warm_start without {name}"))
    };
    let hash = |name: &str| {
        line[name]
            .as_str()
            .and_then(parse_hex)
            .ok_or_else(|| format!("warm_start without a {name} of 16 hex digits"))
    };
    let prefix_dataset = line["prefix_dataset"]
        .as_str()
        .and_then(|hex| DatasetVersion::from_hex(hex).ok())
        .ok_or("warm_start without a prefix_dataset of 64 hex digits")?;
    let warming_up = line["warming_up"]
        .as_array()
        .and_then(|ids| {
            ids.iter()
                .map(|id| id.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
        })
        .ok_or("warm_start without warming_up")?;
    Ok(WarmStart::Warm(WarmHandoff {
        origin_ms: ms("origin_ms")?,
        effective_origin_ms: optional_ms("effective_origin_ms")?,
        cutover_ms: ms("cutover_ms")?,
        prefix_dataset,
        prefix_last_event_ms: optional_ms("prefix_last_event_ms")?,
        prefix_stream: EventStreamHash {
            events: count("prefix_events")?,
            fingerprint: hash("prefix_event_hash")?,
        },
        prefix_rejections: count("prefix_rejections")?,
        handoff_state: StateHash::from_fingerprint(hash("handoff_state_hash")?),
        seeds: seeds_from_json(&line["seeds"], "warm_start")?,
        queue_high_water: count("queue_high_water")?,
        queue_high_water_bytes: count("queue_high_water_bytes")?,
        warming_up,
    }))
}

/// Every warm run's handoff in the journal at `journal`, as (run id,
/// handoff), in journal order (ADR-062 D11, the replace's cost, #211
/// RQ5), with the notes of the lines read without their bytes. The journal
/// is streamed line by line with bounded memory; only `warm_start` lines
/// with `warm: true` are taken, and a line that is no JSON or no complete
/// warm handoff is skipped, as [`read_runs`] skips it.
///
/// The gate of ADR-068 D1 applies with `warm_start` as the consumed kind
/// (D8): a complete line that fails its check and is no JSON object, has no
/// string `type`, is a `warm_start` or names no kind the writer writes,
/// stops the read unless a lost-line record in hot's copy ledger matches
/// it. A recorded line, and a line that is no JSON object between two
/// Intact lines of one run (D6), is a note: if it was a warm run's
/// `warm_start`, that run is not named. `None` when the journal does not
/// exist.
///
/// # Errors
///
/// A description of the I/O error when the journal exists and cannot be
/// read, or the gate's stop.
pub fn warm_handoffs(journal: &Path, hot: &Path) -> Result<Option<WarmHandoffs>, String> {
    use std::io::BufRead;
    let unreadable = |error: std::io::Error| {
        format!("the journal {} cannot be read ({error})", journal.display())
    };
    let file = match File::open(journal) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(unreadable(error)),
    };
    let mut fetch = || tiered_records(hot, journal);
    let mut records = Records {
        fetch: &mut fetch,
        fetched: None,
    };
    let mut reader = std::io::BufReader::new(file);
    let mut handoffs = WarmHandoffs::default();
    let mut line = Vec::new();
    // The previous complete line's body, and a D6 candidate: the previous
    // line's number and offset, and the run of the Intact line before it.
    let mut previous: Option<Vec<u8>> = None;
    let mut candidate: Option<(u64, u64, String)> = None;
    let (mut number, mut offset) = (0_u64, 0_u64);
    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line).map_err(unreadable)?;
        if n == 0 {
            break;
        }
        number += 1;
        let at = offset;
        offset += n as u64;
        let complete = line.last() == Some(&b'\n');
        let body = if complete {
            &line[..line.len() - 1]
        } else {
            &line[..]
        };
        if let Some((line, offset, run)) = candidate.take()
            && complete
            && check_line(body) == LineCheck::Intact
            && run_id_of(body).as_ref() == Some(&run)
        {
            handoffs
                .notes
                .push(handoff_note(line, offset, &LostWhy::MemberDamaged));
        }
        let value = serde_json::from_slice::<Value>(body).ok();
        let mut skip = false;
        if complete && gate_candidate(value.as_ref(), &HANDOFF_KINDS) {
            match check_line(body) {
                LineCheck::Failed => {
                    let why = records.judge(journal, number, at, body)?;
                    handoffs.notes.push(handoff_note(number, at, &why));
                    skip = true;
                }
                LineCheck::Unchecked if value.as_ref().is_none_or(|v| !v.is_object()) => {
                    candidate = previous
                        .as_deref()
                        .filter(|before| check_line(before) == LineCheck::Intact)
                        .and_then(run_id_of)
                        .map(|run| (number, at, run));
                }
                _ => {}
            }
        }
        previous = complete.then(|| body.to_vec());
        let Some(value) = value.filter(|_| !skip) else {
            continue;
        };
        if value["type"].as_str() != Some("warm_start") {
            continue;
        }
        if let Ok(WarmStart::Warm(handoff)) = warm_start_from_json(&value) {
            let run_id = value["run_id"].as_str().unwrap_or("?").to_owned();
            handoffs.runs.push((run_id, handoff));
        }
    }
    Ok(Some(handoffs))
}

/// What [`warm_handoffs`] read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WarmHandoffs {
    /// Every warm handoff, as (run id, handoff), in journal order.
    pub runs: Vec<(String, WarmHandoff)>,
    /// One cost line per line read without its bytes (ADR-068 D8).
    pub notes: Vec<String>,
}

/// The cost line of a line [`warm_handoffs`] reads without its bytes.
fn handoff_note(line: u64, offset: u64, why: &LostWhy) -> String {
    format!(
        "cost: journal line {line} at byte {offset} {}: if it was a warm run's warm_start, that \
         run is not named above (ADR-068 D8)",
        lost_why(why, line)
    )
}

/// Reads a `state_checkpoint` journal line.
///
/// # Errors
///
/// A description of a missing or malformed field.
pub fn state_checkpoint_from_json(line: &Value) -> Result<StateCheckpoint, String> {
    let ordinal = line["ordinal"]
        .as_u64()
        .ok_or("state_checkpoint without ordinal")?;
    let as_of = match &line["as_of"] {
        Value::Null => None,
        value => Some(EventTime::from_millis(
            value
                .as_i64()
                .ok_or("state_checkpoint with a malformed as_of")?,
        )),
    };
    let hash = |name: &str| {
        line[name]
            .as_str()
            .and_then(parse_hex)
            .ok_or_else(|| format!("state_checkpoint without a {name} of 16 hex digits"))
    };
    Ok(StateCheckpoint {
        ordinal,
        as_of,
        events: EventStreamHash {
            events: ordinal,
            fingerprint: hash("event_hash")?,
        },
        state: StateHash::from_fingerprint(hash("state_hash")?),
        last: line["last"]
            .as_bool()
            .ok_or("state_checkpoint without last")?,
    })
}

/// Exactly 16 lowercase hex digits, as a [`Fingerprint`] displays.
fn parse_hex(text: &str) -> Option<Fingerprint> {
    let lower_hex = text
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if text.len() != 16 || !lower_hex {
        return None;
    }
    u64::from_str_radix(text, 16)
        .ok()
        .map(Fingerprint::from_raw)
}

impl JournaledRun {
    /// The run as the live replay takes it: a clean end (exit code 0)
    /// carries the record count the replay checks. The seeds are those of
    /// the pipeline whose output reached the engine: `warm_start`'s for a
    /// warm run, `run_start`'s otherwise (module docs).
    pub fn live_run(&self) -> LiveRun {
        let seeds = match &self.warm_start {
            Some(WarmStart::Warm(warm)) => warm.seeds.clone(),
            _ => self.parameters.seeds.clone(),
        };
        LiveRun {
            run_id: self.parameters.run_id.clone(),
            symbol: self.parameters.symbol.clone(),
            hold_back_ms: self.parameters.hold_back_ms,
            oi_retime_ms: self.parameters.oi_retime_ms,
            seeds,
            streams: self.streams.clone(),
            started_at_ms: self.started_at_ms,
            ended_at_ms: self.end.map(|end| end.at_ms),
            clean_records: self
                .end
                .filter(|end| end.exit_code == 0)
                .map(|end| end.records),
        }
    }
}

/// Reads every run of `source` from the journal of `paths`, in journal
/// order, behind the line gate of ADR-068.
///
/// **Structure.** A line that is not JSON is skipped: a crash can leave a
/// partial line. The next [`Journal::open`] ends it with a `\n` (#225), so
/// the next run's first line follows it on a line of its own; journals
/// written before that may hold the next run's first line glued onto it
/// (#217). A line that is not valid UTF-8 (a crash can tear a multibyte
/// character) is skipped the same way: the journal is read as bytes, and
/// only the lines are decoded. Only named keys are read, so a line reads
/// the same with its check (`crc32c`, ADR-066) or without it (written
/// before ADR-066). A `run_start`, `run_end`, `state_checkpoint`,
/// `warm_start`, `warm_ready` or `chain_restart` ([`GATED_KINDS`]) that
/// does not carry its fields is an error, and so are two `run_start`s of
/// one run id — a restart within the same second, whose records the session
/// prefix could not tell apart (ADR-032) — a second `run_end`, `warm_start`
/// or `warm_ready`, and any of these lines without a `run_start`.
///
/// **The gate** (ADR-068 D1, D2). A complete line whose check fails
/// ([`LineCheck::Failed`]) is *gated* when the reader cannot tell that it
/// ignores the line's kind: the line is no JSON object, has no string
/// `type`, or its `type` is one of [`GATED_KINDS`] or names no kind the
/// writer writes ([`KNOWN_KINDS`]). Lines are parsed first, so only such
/// lines pay a CRC. The first gated line in journal order stops the read
/// with an error naming the line, its byte offset and both repairs
/// ([`FAILED_LINE_REMEDY`]), unless a lost-line record matches it (ADR-066
/// D8). Hot's copy ledger is read for that at most once, and only when a
/// gated line exists (D3); when it gives no records, the read stops and
/// says why. A single-root store has neither copies nor a copy ledger: its
/// gated lines are read as recorded ones, with a note (D5). The last line
/// without its `\n` is never classified (ADR-066 D2). Every other line
/// reads as before: a Failed line of a kind the reader ignores, and every
/// Intact or Unchecked line.
///
/// **A lost line** (D4) is read without its bytes. Its neighbours are the
/// nearest lines on each side that parse with a string `run_id`, do not
/// fail their check and are not lost themselves. When the line parses, its
/// kind is one of the six and its `run_id` is every neighbour's, it acts by
/// its kind: a lost `run_end`, `state_checkpoint` or `chain_restart` marks
/// its run ([`JournaledRun::lost`]), which reads without the line; a lost
/// `run_start`, `warm_start` or `warm_ready` excludes the run. Otherwise
/// the runs of both neighbours are excluded. An excluded run is read as one
/// whose `run_start` was lost (ADR-039 D2): its structural lines are
/// skipped, and it is listed in [`RunsRead::excluded`].
///
/// **Member damage** (D6). A complete line that is no JSON object, carries
/// no member of the check's shape and lies between two Intact lines of one
/// run excludes that run: no crash leaves such a line.
///
/// Every lost line is a [`LostNote`]; [`RunsRead::notes`] prints them.
///
/// # Errors
///
/// A description naming the line: the gate's stop, or a structural error.
pub fn read_runs(paths: &Paths, source: &str) -> Result<RunsRead, String> {
    let bytes = std::fs::read(&paths.journal)
        .map_err(|e| format!("read journal {}: {e}", paths.journal.display()))?;
    runs_from_bytes(&bytes, &paths.journal, source, &mut || {
        lost_line_records_of(paths)
    })
}

/// The kinds [`read_runs`] consumes, its six arms (ADR-068 D1): a failed
/// line of one of them is gated.
pub const GATED_KINDS: [&str; 6] = [
    "run_start",
    "run_end",
    "state_checkpoint",
    "warm_start",
    "warm_ready",
    "chain_restart",
];

/// The kind [`warm_handoffs`] consumes (ADR-068 D1, D8).
const HANDOFF_KINDS: [&str; 1] = ["warm_start"];

/// Every kind [`Journal::write`] writes: the module table's (ADR-068 D1).
/// A failed line whose `type` names none of them is gated, since damage can
/// have renamed a consumed kind.
pub const KNOWN_KINDS: [&str; 25] = [
    "run_start",
    "warm_start",
    "warm_ready",
    "chain_restart",
    "recovery",
    "connected",
    "connect_failed",
    "disconnected",
    "planned_rotation",
    "backoff",
    "oi_poll",
    "depth_snapshot_fetch",
    "book",
    "book_checkpoint",
    "gap",
    "sealed",
    "normalize_error",
    "stats",
    "domain_rejection",
    "state_checkpoint",
    "run_end",
    "replication",
    "copy_ledger_truncated",
    "removal",
    "recall",
];

/// What to do about a hot journal line that fails its check and is not
/// recorded lost: `mie store verify` (V3, ADR-066 D6) and the gate of
/// ADR-068 D2 print the same text.
pub const FAILED_LINE_REMEDY: &str = "when a copy holds it intact, `mie store repair-hot \
     --scope journal` (with `mie ingest` stopped) restores it; otherwise, after checking for a \
     shared cause, `mie store repair --record-lost-lines` records its loss";

/// The lost-line records a gated line is matched against (ADR-068 D3, D5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LostLineRecords {
    /// A single-root store: no copy to restore a line from and no copy
    /// ledger to record it in (D5).
    SingleRoot,
    /// The records of the journal in hot's copy ledger, in ledger order.
    Read(Vec<LostLine>),
    /// Why the copy ledger gives none: the stricter outcome, every gated
    /// line stops the read.
    Unavailable(String),
}

/// Why a line is read without its bytes (ADR-068 D4–D6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LostWhy {
    /// A lost-line record matches it (ADR-066 D8): the first in ledger
    /// order.
    Recorded {
        /// When it was recorded.
        at_ms: i64,
        /// The line's number when it was recorded.
        line: u64,
    },
    /// It fails its check in a single-root store, which cannot record it
    /// (D5).
    NotRecordable,
    /// It is no JSON object between two Intact lines of its run (D6).
    MemberDamaged,
}

/// A line read without its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostRef {
    /// Its 1-based number among the `\n`-split lines.
    pub line: u64,
    /// Its first byte.
    pub offset: u64,
    /// The `type` its bytes name, when they parse with one; trusted only
    /// for a line that marks a run.
    pub kind: Option<String>,
    /// Why.
    pub why: LostWhy,
}

/// What a lost line does to the runs (ADR-068 D4, D6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LostEffect {
    /// A trusted `run_end`, `state_checkpoint` or `chain_restart`: the run
    /// reads without it and is marked.
    Marked(String),
    /// A trusted `run_start`, `warm_start` or `warm_ready`, or a D6 line:
    /// its run is excluded.
    Excluded(String),
    /// An untrusted line: the runs of its neighbours are excluded, none when
    /// it has none.
    Neighbours(Vec<String>),
}

/// A lost line and its effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostNote {
    /// The line.
    pub lost: LostRef,
    /// Its effect.
    pub effect: LostEffect,
}

impl LostNote {
    /// The note's text, without the `journal: ` prefix (ADR-068 D7).
    pub fn text(&self) -> String {
        let LostRef {
            line, offset, why, ..
        } = &self.lost;
        let why = lost_why(why, *line);
        let kind = self.lost.kind.as_deref().unwrap_or("?");
        let effect = match &self.effect {
            LostEffect::Marked(run) if kind == "run_end" => {
                format!("run {run} reads without its run_end, as a crashed run, and is marked")
            }
            LostEffect::Marked(run) => {
                format!("run {run} reads without this {kind} and is marked")
            }
            LostEffect::Excluded(run) if self.lost.why == LostWhy::MemberDamaged => {
                format!("run {run} is excluded, as a run whose run_start is lost (ADR-039 D2)")
            }
            LostEffect::Excluded(run) => format!(
                "its {kind} is lost, so run {run} is excluded, as a run whose run_start is lost \
                 (ADR-039 D2)"
            ),
            LostEffect::Neighbours(runs) if runs.is_empty() => {
                "its run cannot be told and no line beside it names one: no run is excluded"
                    .to_owned()
            }
            LostEffect::Neighbours(runs) => format!(
                "its run cannot be told, so run(s) {} beside it are excluded, as runs whose \
                 run_start is lost (ADR-039 D2)",
                runs.join(", ")
            ),
        };
        let decision = if self.lost.why == LostWhy::MemberDamaged {
            "D6"
        } else {
            "D4"
        };
        format!("line {line} at byte {offset} {why}: {effect} (ADR-068 {decision})")
    }
}

/// Why a line numbered `line` is read without its bytes, as a note says it.
fn lost_why(why: &LostWhy, line: u64) -> String {
    match why {
        LostWhy::Recorded { at_ms, line: then } => format!(
            "fails its check (ADR-066 D2), recorded lost at {at_ms}{} (ADR-066 D8)",
            renumbered(*then, line)
        ),
        LostWhy::NotRecordable => "fails its check (ADR-066 D2) and is not recordable: a \
             single-root store keeps no copy to restore it from and no copy ledger to record \
             it (ADR-068 D5)"
            .to_owned(),
        LostWhy::MemberDamaged => "is no JSON object between two intact lines of its run, \
             damage no crash leaves (ADR-068 D6)"
            .to_owned(),
    }
}

/// A run a lost line excludes (ADR-068 D4, D6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcludedRun {
    /// The run.
    pub run_id: String,
    /// The first lost line that excludes it.
    pub by: LostRef,
    /// Its `run_start`'s `at_ms`, symbol and source, when that line was
    /// read.
    pub start: Option<(i64, String, String)>,
    /// The earliest and the latest `at_ms` of the lines naming the run
    /// that were read: the span of its readable lines.
    pub lines_at_ms: Option<(i64, i64)>,
    /// Without its `run_start`: the `at_ms` of the first line after the
    /// excluding line that belongs to another run, a bound above on the
    /// run's start; `None` when no such line exists.
    pub started_by_ms: Option<i64>,
}

impl ExcludedRun {
    /// Whether the run may have started in `[from, to)` (`to` `None`: no
    /// end), failing closed (ADR-068 D4). With its `run_start` read, its
    /// start decides. Without it, the start lies at or after the earliest
    /// readable line of the run (its `recovery` is written before its
    /// `run_start`) and at or before the first line of another run after
    /// the excluding line ([`ExcludedRun::started_by_ms`], no bound when
    /// there is none), or the run's latest line when that is later; the
    /// run counts when that interval meets the window. A run with no
    /// readable time counts.
    pub fn may_have_started_in(&self, from: i64, to: Option<i64>) -> bool {
        let before_end = |at: i64| to.is_none_or(|to| at < to);
        match (&self.start, self.lines_at_ms) {
            (Some((at, _, _)), _) => *at >= from && before_end(*at),
            (None, Some((first, last))) => {
                let latest = self.started_by_ms.map(|by| by.max(last));
                latest.is_none_or(|latest| latest >= from) && before_end(first)
            }
            (None, None) => true,
        }
    }

    /// Whether the run may be of `symbol`: its `run_start` names it, or
    /// that line was not read.
    pub fn may_be_of(&self, symbol: &str) -> bool {
        self.start.as_ref().is_none_or(|(_, s, _)| s == symbol)
    }

    /// Where its exclusion comes from, for a report line.
    pub fn cause(&self) -> String {
        format!(
            "excluded: journal line {} at byte {} (ADR-068 D4)",
            self.by.line, self.by.offset
        )
    }
}

/// What [`read_runs`] read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunsRead {
    /// The runs of the source, in journal order, the excluded ones left
    /// out.
    pub runs: Vec<JournaledRun>,
    /// The excluded runs not known to be another source's, in the order
    /// their lost lines are met.
    pub excluded: Vec<ExcludedRun>,
    /// Every line read without its bytes, in journal order.
    pub lost: Vec<LostNote>,
}

impl RunsRead {
    /// One `journal: <note>` line per lost line, in journal order (ADR-068
    /// D7). Empty for a journal without a failed or member-damaged line.
    pub fn notes(&self) -> Vec<String> {
        self.lost
            .iter()
            .map(|note| format!("journal: {}", note.text()))
            .collect()
    }

    /// The excluded run `run_id`, if any.
    pub fn excluded_run(&self, run_id: &str) -> Option<&ExcludedRun> {
        self.excluded.iter().find(|run| run.run_id == run_id)
    }
}

/// The records of `paths`' journal (ADR-068 D3, D5): none to read in a
/// single-root store, otherwise hot's copy ledger, read once.
pub fn lost_line_records_of(paths: &Paths) -> LostLineRecords {
    if paths.cold_roots.is_empty() {
        return LostLineRecords::SingleRoot;
    }
    tiered_records(&paths.raw_root, &paths.journal)
}

/// The records of `journal` in the copy ledger of the tiered store whose
/// hot is `hot`.
fn tiered_records(hot: &Path, journal: &Path) -> LostLineRecords {
    if journal.file_name().and_then(|n| n.to_str()).is_none() {
        return LostLineRecords::Unavailable(
            "the journal's file name is not UTF-8, so no lost-line record can name it".to_owned(),
        );
    }
    match lost_line_records(hot, journal) {
        Ok(records) => LostLineRecords::Read(records),
        Err(why) => LostLineRecords::Unavailable(format!(
            "hot's copy ledger {} gives none ({why})",
            copy_ledger::ledger_path(hot).display()
        )),
    }
}

/// The `lost-line` records of the journal `journal` (by its file name) in
/// hot's copy ledger (ADR-066 D8), in ledger order, as `mie store repair`
/// reads them for R7. A name that is not UTF-8 has none.
///
/// # Errors
///
/// Why the ledger gives none: it is absent, cannot be read or folded.
pub fn lost_line_records(hot: &Path, journal: &Path) -> Result<Vec<LostLine>, String> {
    let Some(name) = journal.file_name().and_then(|name| name.to_str()) else {
        return Ok(Vec::new());
    };
    match copy_ledger::read(hot) {
        Ok(Some(ledger)) => CopyState::fold(&ledger)
            .map(|state| state.lost_lines(name).to_vec())
            .map_err(|error| error.to_string()),
        Ok(None) => Err("it does not exist".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}

/// A lost-line record's line number beside the line's number now, when
/// they differ: an earlier `\n` flip shifts the numbers, not the bytes
/// (ADR-066 D8).
pub fn renumbered(recorded: u64, now: u64) -> String {
    if recorded == now {
        String::new()
    } else {
        format!(" (recorded as line {recorded})")
    }
}

/// The `type` of a parsed line, when it is an object with a string `type`.
fn kind_of(value: Option<&Value>) -> Option<&str> {
    value?.as_object()?.get("type")?.as_str()
}

/// Whether a line parsed as `value` is a candidate of the gate for a reader
/// consuming `consumed` (ADR-068 D1): no object, no string `type`, a
/// consumed kind, or a kind the writer does not write.
fn gate_candidate(value: Option<&Value>, consumed: &[&str]) -> bool {
    match kind_of(value) {
        None => true,
        Some(kind) => consumed.contains(&kind) || !KNOWN_KINDS.contains(&kind),
    }
}

/// The `run_id` of a line's bytes, when they parse as an object with a
/// string one.
fn run_id_of(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    value.get("run_id")?.as_str().map(str::to_owned)
}

/// The stop of a gated line no record matches (ADR-068 D2, D3).
fn gate_stop(journal: &Path, line: u64, offset: u64, unavailable: Option<&str>) -> String {
    let why = match unavailable {
        None => "and is not recorded lost, so the journal is not read (ADR-068 D2)".to_owned(),
        Some(why) => format!(
            "and no lost-line record is used, the stricter outcome, since {why}; so the journal \
             is not read (ADR-068 D3)"
        ),
    };
    format!(
        "journal {} line {line} at byte {offset} fails its check (ADR-066 D2) {why}: \
         {FAILED_LINE_REMEDY}; `mie store verify` names which applies",
        journal.display()
    )
}

/// The records of a read, fetched at most once (ADR-068 D3).
struct Records<'a> {
    fetch: &'a mut dyn FnMut() -> LostLineRecords,
    fetched: Option<LostLineRecords>,
}

impl Records<'_> {
    /// Why a gated line at `offset` with `body` is lost, or the read's stop.
    fn judge(
        &mut self,
        journal: &Path,
        line: u64,
        offset: u64,
        body: &[u8],
    ) -> Result<LostWhy, String> {
        let records = self.fetched.get_or_insert_with(|| (self.fetch)());
        match records {
            LostLineRecords::SingleRoot => Ok(LostWhy::NotRecordable),
            LostLineRecords::Unavailable(why) => Err(gate_stop(journal, line, offset, Some(why))),
            LostLineRecords::Read(records) => {
                let sha = copy_ledger::line_sha256(body);
                records
                    .iter()
                    .find(|record| record.matches(offset, &sha))
                    .map(|record| LostWhy::Recorded {
                        at_ms: record.at_ms,
                        line: record.line,
                    })
                    .ok_or_else(|| gate_stop(journal, line, offset, None))
            }
        }
    }
}

/// The span `[start, end)` of the line of `bytes` that starts at `start`,
/// `end` at its `\n` or at the end of `bytes`.
fn span_from(bytes: &[u8], start: usize) -> (usize, usize) {
    let end = bytes[start..]
        .iter()
        .position(|b| *b == b'\n')
        .map_or(bytes.len(), |i| start + i);
    (start, end)
}

/// The line before the one that starts at `start`, if any.
fn span_before(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    let end = start.checked_sub(1)?;
    let begin = bytes[..end]
        .iter()
        .rposition(|b| *b == b'\n')
        .map_or(0, |i| i + 1);
    Some((begin, end))
}

/// The line after the one that ends at `end`, if any: none past the last
/// `\n`, whose empty piece is no line.
fn span_after(bytes: &[u8], end: usize) -> Option<(usize, usize)> {
    (end + 1 < bytes.len()).then(|| span_from(bytes, end + 1))
}

/// The nearest line beside the line `span` on one side that `accepts`
/// (ADR-068 D4): of the lines that parse with a string `run_id`, do not
/// fail their check (complete lines only; the unterminated tail is not
/// classified) and are not lost (`lost` holds the lost lines' first
/// bytes). Lines that do not qualify are walked past.
fn walk(
    bytes: &[u8],
    span: (usize, usize),
    forward: bool,
    lost: &BTreeSet<usize>,
    accepts: impl Fn(&str) -> bool,
) -> Option<Value> {
    let mut at = span;
    loop {
        at = if forward {
            span_after(bytes, at.1)?
        } else {
            span_before(bytes, at.0)?
        };
        if lost.contains(&at.0) {
            continue;
        }
        let body = &bytes[at.0..at.1];
        let complete = at.1 < bytes.len();
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            continue;
        };
        if value
            .get("run_id")
            .and_then(Value::as_str)
            .is_some_and(&accepts)
            && (!complete || check_line(body) != LineCheck::Failed)
        {
            return Some(value);
        }
    }
}

/// The nearest neighbour of the line `span` on one side (ADR-068 D4): the
/// nearest line [`walk`] reaches. Its run id, and whether it opens its run.
fn neighbour(
    bytes: &[u8],
    span: (usize, usize),
    forward: bool,
    lost: &BTreeSet<usize>,
) -> Option<Neighbour> {
    let value = walk(bytes, span, forward, lost, |_| true)?;
    Some(Neighbour {
        run_id: value["run_id"].as_str()?.to_owned(),
        opens: matches!(kind_of(Some(&value)), Some(kind) if RUN_OPENERS.contains(&kind)),
    })
}

/// The `at_ms` of the first line after the line `span` that belongs to a
/// run other than `run` (ADR-068 D4): a bound above on `run`'s lost
/// `run_start`, since lines are written in order under the journal lock.
/// `None` when no such line exists (no bound), or it has no `at_ms`.
fn next_run_at_ms(
    bytes: &[u8],
    span: (usize, usize),
    run: &str,
    lost: &BTreeSet<usize>,
) -> Option<i64> {
    walk(bytes, span, true, lost, |other| other != run)?["at_ms"].as_i64()
}

/// The kinds that open a run: `mie ingest` writes `recovery` and then
/// `run_start` before any other line of the run (ADR-068 D4, a run
/// boundary).
const RUN_OPENERS: [&str; 2] = ["recovery", "run_start"];

/// A lost line's neighbour (ADR-068 D4).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Neighbour {
    /// Its run id.
    run_id: String,
    /// Whether its kind opens a run ([`RUN_OPENERS`]).
    opens: bool,
}

/// Whether a lost line of `run` is trusted (ADR-068 D4): at least one
/// neighbour, and every neighbour is of `run`, except that for a kind that
/// marks its run (`marks`), an after-neighbour that opens another run is a
/// run boundary and counts as agreement when the before-neighbour is of
/// `run`.
fn trusted(run: &str, marks: bool, before: Option<&Neighbour>, after: Option<&Neighbour>) -> bool {
    let agrees = |side: &Neighbour| side.run_id == run;
    match (before, after) {
        (None, None) => false,
        (Some(before), Some(after)) if marks && agrees(before) && after.opens => true,
        _ => before.into_iter().chain(after).all(agrees),
    }
}

/// Whether the complete line `span` is Intact, and its run id when it
/// parses with one (ADR-068 D6).
fn intact_run(bytes: &[u8], span: (usize, usize)) -> Option<String> {
    let body = &bytes[span.0..span.1];
    if span.1 >= bytes.len() || check_line(body) != LineCheck::Intact {
        return None;
    }
    run_id_of(body)
}

/// The run of a D6 line `span`: both immediate neighbours Intact lines of
/// that run.
fn member_damaged_run(bytes: &[u8], span: (usize, usize)) -> Option<String> {
    let before = intact_run(bytes, span_before(bytes, span.0)?)?;
    let after = intact_run(bytes, span_after(bytes, span.1)?)?;
    (before == after).then_some(before)
}

/// A lost line as the gate pass found it.
struct Lost {
    span: (usize, usize),
    lost: LostRef,
    run_id: Option<String>,
}

/// The core of [`read_runs`] over the journal's `bytes`; `journal` names it
/// in errors, `records` fetches the lost-line records, called at most once
/// and only when a gated line exists (ADR-068 D3).
///
/// One pass parses every line once: it gates, and reads the structure as
/// long as no line is lost. A lost line can exclude a run whose lines came
/// before it, so the structure is then read again with the exclusions:
/// only a journal holding a lost line is parsed twice.
///
/// # Errors
///
/// The gate's stop, or a structural error naming the line.
pub fn runs_from_bytes(
    bytes: &[u8],
    journal: &Path,
    source: &str,
    records: &mut dyn FnMut() -> LostLineRecords,
) -> Result<RunsRead, String> {
    let mut records = Records {
        fetch: records,
        fetched: None,
    };
    let mut lost: Vec<Lost> = Vec::new();
    let mut structure = Structure::default();
    let mut deferred: Option<String> = None;
    // The previous line when it is a D6 candidate.
    let mut candidate: Option<(usize, usize, u64)> = None;
    let mut start = 0;
    // `serde_json` rejects invalid UTF-8 and takes a `\r` before the `\n`
    // as whitespace, so this splits as `str::lines` did.
    for (index, body) in bytes.split(|b| *b == b'\n').enumerate() {
        let span = (start, start + body.len());
        start = span.1 + 1;
        let number = index as u64 + 1;
        let complete = span.1 < bytes.len();
        if let Some((s, e, line)) = candidate.take()
            && let Some(run_id) = member_damaged_run(bytes, (s, e))
        {
            lost.push(Lost {
                span: (s, e),
                lost: LostRef {
                    line,
                    offset: s as u64,
                    kind: None,
                    why: LostWhy::MemberDamaged,
                },
                run_id: Some(run_id),
            });
        }
        let value = serde_json::from_slice::<Value>(body).ok();
        let mut skip = false;
        if complete && gate_candidate(value.as_ref(), &GATED_KINDS) {
            match check_line(body) {
                LineCheck::Failed => {
                    let why = records.judge(journal, number, span.0 as u64, body)?;
                    lost.push(Lost {
                        span,
                        lost: LostRef {
                            line: number,
                            offset: span.0 as u64,
                            kind: kind_of(value.as_ref()).map(str::to_owned),
                            why,
                        },
                        run_id: value
                            .as_ref()
                            .and_then(|v| v.get("run_id"))
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                    });
                    skip = true;
                }
                LineCheck::Unchecked if value.as_ref().is_none_or(|v| !v.is_object()) => {
                    candidate = Some((span.0, span.1, number));
                }
                _ => {}
            }
        }
        if !skip
            && lost.is_empty()
            && deferred.is_none()
            && let Some(value) = &value
            && let Err(error) = structure.line(number, value, None)
        {
            deferred = Some(error);
        }
    }
    let mut excluded: Vec<ExcludedRun> = Vec::new();
    let mut notes = Vec::new();
    let mut marks: Vec<(String, LostRef)> = Vec::new();
    let lost_at: BTreeSet<usize> = lost.iter().map(|l| l.span.0).collect();
    lost.sort_by_key(|l| l.span.0);
    for line in &lost {
        let effect = if line.lost.why == LostWhy::MemberDamaged {
            LostEffect::Excluded(line.run_id.clone().unwrap_or_default())
        } else {
            let before = neighbour(bytes, line.span, false, &lost_at);
            let after = neighbour(bytes, line.span, true, &lost_at);
            let kind = line.lost.kind.as_deref();
            let marks = matches!(kind, Some("run_end" | "state_checkpoint" | "chain_restart"));
            let trusted = kind.is_some_and(|kind| GATED_KINDS.contains(&kind))
                && line
                    .run_id
                    .as_ref()
                    .is_some_and(|run| trusted(run, marks, before.as_ref(), after.as_ref()));
            let sides: Vec<String> = before.into_iter().chain(after).map(|n| n.run_id).collect();
            match (trusted, kind, &line.run_id) {
                (true, Some("run_end" | "state_checkpoint" | "chain_restart"), Some(run)) => {
                    LostEffect::Marked(run.clone())
                }
                (true, _, Some(run)) => LostEffect::Excluded(run.clone()),
                _ => {
                    let mut runs = sides;
                    runs.dedup();
                    LostEffect::Neighbours(runs)
                }
            }
        };
        let excludes: Vec<&String> = match &effect {
            LostEffect::Marked(run) => {
                marks.push((run.clone(), line.lost.clone()));
                Vec::new()
            }
            LostEffect::Excluded(run) => vec![run],
            LostEffect::Neighbours(runs) => runs.iter().collect(),
        };
        for run in excludes {
            if !excluded.iter().any(|e| e.run_id == *run) {
                excluded.push(ExcludedRun {
                    run_id: run.clone(),
                    by: line.lost.clone(),
                    start: None,
                    lines_at_ms: None,
                    started_by_ms: None,
                });
            }
        }
        notes.push(LostNote {
            lost: line.lost.clone(),
            effect,
        });
    }
    let mut runs = if lost.is_empty() {
        if let Some(error) = deferred {
            return Err(error);
        }
        structure.runs
    } else {
        // The second pass, with the exclusions.
        let mut structure = Structure::default();
        let mut start = 0;
        for (index, body) in bytes.split(|b| *b == b'\n').enumerate() {
            let at = start;
            start += body.len() + 1;
            if lost_at.contains(&at) {
                continue;
            }
            let Ok(value) = serde_json::from_slice::<Value>(body) else {
                continue;
            };
            structure.line(index as u64 + 1, &value, Some(&mut excluded))?;
        }
        structure.runs
    };
    // Without its run_start, an excluded run's start is bounded above by
    // the next line of another run (ADR-068 D4).
    for run in excluded.iter_mut().filter(|run| run.start.is_none()) {
        let span = span_from(bytes, run.by.offset as usize);
        run.started_by_ms = next_run_at_ms(bytes, span, &run.run_id, &lost_at);
    }
    for (run_id, lost) in marks {
        if let Some(run) = runs.iter_mut().find(|r| r.parameters.run_id == run_id) {
            run.lost.push(lost);
        }
    }
    runs.retain(|run| run.source == source);
    excluded.retain(|run| run.start.as_ref().is_none_or(|(_, _, s)| s == source));
    Ok(RunsRead {
        runs,
        excluded,
        lost: notes,
    })
}

/// The structural read of [`read_runs`], line by line.
#[derive(Default)]
struct Structure {
    runs: Vec<JournaledRun>,
}

impl Structure {
    /// Reads one parsed line, number `number`. With `excluded`, a line
    /// naming an excluded run is skipped, after it fills in the run's
    /// `start` and `lines_at_ms`.
    fn line(
        &mut self,
        number: u64,
        value: &Value,
        excluded: Option<&mut Vec<ExcludedRun>>,
    ) -> Result<(), String> {
        let at = |detail: String| format!("journal line {number}: {detail}");
        if let Some(excluded) = excluded
            && let Some(run_id) = value["run_id"].as_str()
            && let Some(run) = excluded.iter_mut().find(|r| r.run_id == run_id)
        {
            if let Some(at) = value["at_ms"].as_i64() {
                run.lines_at_ms = Some(match run.lines_at_ms {
                    None => (at, at),
                    Some((first, last)) => (first.min(at), last.max(at)),
                });
            }
            if value["type"] == "run_start"
                && run.start.is_none()
                && let (Some(at_ms), Some(symbol), Some(source)) = (
                    value["at_ms"].as_i64(),
                    value["symbol"].as_str(),
                    value["source"].as_str(),
                )
            {
                run.start = Some((at_ms, symbol.to_owned(), source.to_owned()));
            }
            return Ok(());
        }
        let runs = &mut self.runs;
        match value["type"].as_str() {
            Some("run_start") => {
                let parameters = RunParameters::from_run_start(value).map_err(at)?;
                if runs
                    .iter()
                    .any(|r| r.parameters.run_id == parameters.run_id)
                {
                    return Err(at(format!(
                        "run {} started twice (a restart within one second)",
                        parameters.run_id
                    )));
                }
                let run_source = value["source"]
                    .as_str()
                    .ok_or_else(|| at("run_start without source".to_owned()))?;
                let streams = value["streams"]
                    .as_array()
                    .ok_or_else(|| at("run_start without streams".to_owned()))?
                    .iter()
                    .map(|name| {
                        name.as_str()
                            .and_then(BinanceStream::from_raw_name)
                            .ok_or_else(|| at(format!("run_start with unknown stream {name}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let started_at_ms = value["at_ms"]
                    .as_i64()
                    .ok_or_else(|| at("run_start without at_ms".to_owned()))?;
                let checkpointing = Checkpointing::from_run_start(value).map_err(at)?;
                let history = RunHistory::from_run_start(value).map_err(at)?;
                runs.push(JournaledRun {
                    parameters,
                    source: run_source.to_owned(),
                    streams,
                    started_at_ms,
                    end: None,
                    checkpointing,
                    checkpoints: Vec::new(),
                    history,
                    warm_start: None,
                    ready_after_ms: None,
                    chain_restarts: Vec::new(),
                    lost: Vec::new(),
                });
            }
            Some(kind @ ("state_checkpoint" | "warm_start" | "warm_ready" | "chain_restart")) => {
                let run_id = value["run_id"].as_str().unwrap_or_default();
                let run = runs
                    .iter_mut()
                    .find(|r| r.parameters.run_id == run_id)
                    .ok_or_else(|| at(format!("{kind} of run {run_id:?} without run_start")))?;
                match kind {
                    "state_checkpoint" => {
                        run.checkpoints
                            .push(state_checkpoint_from_json(value).map_err(at)?);
                    }
                    "warm_start" => {
                        let start = warm_start_from_json(value).map_err(at)?;
                        if run.warm_start.is_some() {
                            return Err(at(format!("run {run_id} has a second warm_start")));
                        }
                        run.warm_start = Some(start);
                    }
                    "warm_ready" => {
                        let ready = value["ready_after_ms"]
                            .as_i64()
                            .ok_or_else(|| at("warm_ready without ready_after_ms".to_owned()))?;
                        if run.ready_after_ms.is_some() {
                            return Err(at(format!("run {run_id} has a second warm_ready")));
                        }
                        run.ready_after_ms = Some(ready);
                    }
                    _ => {
                        let time = value["time"]
                            .as_i64()
                            .ok_or_else(|| at("chain_restart without time".to_owned()))?;
                        run.chain_restarts.push(EventTime::from_millis(time));
                    }
                }
            }
            Some("run_end") => {
                let field = |name: &str| {
                    value[name]
                        .as_i64()
                        .ok_or_else(|| at(format!("run_end without {name}")))
                };
                let end = RunEnd {
                    at_ms: field("at_ms")?,
                    exit_code: field("exit_code")?,
                    records: value["records"]
                        .as_u64()
                        .ok_or_else(|| at("run_end without records".to_owned()))?,
                    events: value["events"].as_u64(),
                    domain_rejections: value["domain_rejections"].as_u64(),
                };
                let run_id = value["run_id"].as_str().unwrap_or_default();
                let run = runs
                    .iter_mut()
                    .find(|r| r.parameters.run_id == run_id)
                    .ok_or_else(|| at(format!("run_end of run {run_id:?} without run_start")))?;
                if run.end.is_some() {
                    return Err(at(format!("run {run_id} ended twice")));
                }
                run.end = Some(end);
            }
            _ => {}
        }
        Ok(())
    }
}

/// The name of the line check's member, always a line's last (ADR-066 D1).
pub const CHECK_FIELD: &str = "crc32c";

/// Drops every member named [`CHECK_FIELD`] from `value`, at every depth
/// and inside arrays (ADR-066 D1). The name is the check's: a nested object
/// keyed so would end in the member's shape, and a line torn right after it
/// would read as [`LineCheck::Failed`] instead of [`LineCheck::Unchecked`].
fn drop_check_fields(value: &mut Value) {
    match value {
        Value::Object(members) => {
            members.remove(CHECK_FIELD);
            members.values_mut().for_each(drop_check_fields);
        }
        Value::Array(items) => items.iter_mut().for_each(drop_check_fields),
        _ => {}
    }
}

/// The longest line body, in bytes without its `\n`, that [`check_line`]
/// classifies (ADR-066 D2). A longer line is [`LineCheck::Unchecked`]: the
/// cap bounds the memory per line of the scans that stream a journal beside
/// a live capture, which a zero-filled region with no `\n` would otherwise
/// fill. 1 MiB is about 450 times the longest line measured (2 318 B).
pub const MAX_CHECKED_LINE: usize = 1_048_576;

/// What precedes the hex digits in the member's 21-byte shape:
/// `,"crc32c":"`. The shape is this, 8 lowercase hex digits, and `"}`.
const CHECK_PREFIX: &[u8] = b",\"crc32c\":\"";

/// The bytes the shape takes at a line's end, the closing `}` included.
const CHECK_SHAPE: usize = CHECK_PREFIX.len() + 8 + 2;

/// The byte-wise lookup table of CRC-32C (Castagnoli, the iSCSI CRC):
/// the reflected polynomial 0x82F63B78.
const CRC32C_TABLE: [u32; 256] = {
    let mut table = [0; 256];
    let mut byte = 0;
    while byte < 256 {
        let mut crc = byte as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[byte] = crc;
        byte += 1;
    }
    table
};

/// Runs the CRC register `crc` over `bytes`, with no initial or final
/// inversion, so a CRC can span pieces.
fn crc32c_update(mut crc: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        crc = CRC32C_TABLE[usize::from((crc as u8) ^ byte)] ^ (crc >> 8);
    }
    crc
}

/// The CRC-32C of `bytes`: init and xorout 0xFFFFFFFF (ADR-066 D1).
fn crc32c(bytes: &[u8]) -> u32 {
    !crc32c_update(!0, bytes)
}

/// A checked body split into the bytes before its member and the CRC its
/// hex states, or `None` when the body does not end with the member's
/// shape or is longer than [`MAX_CHECKED_LINE`].
fn split_check(body: &[u8]) -> Option<(&[u8], u32)> {
    if body.len() <= CHECK_SHAPE || body.len() > MAX_CHECKED_LINE {
        return None;
    }
    let (head, shape) = body.split_at(body.len() - CHECK_SHAPE);
    let hex = shape.strip_prefix(CHECK_PREFIX)?.strip_suffix(b"\"}")?;
    hex.iter()
        .try_fold(0_u32, |crc, digit| {
            let value = match digit {
                b'0'..=b'9' => digit - b'0',
                b'a'..=b'f' => digit - b'a' + 10,
                _ => return None,
            };
            Some(crc << 4 | u32::from(value))
        })
        .map(|crc| (head, crc))
}

/// A line's verdict under its check (ADR-066 D2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineCheck {
    /// The line ends with the member's shape and its CRC matches.
    Intact,
    /// The line ends with the member's shape and its CRC differs: the line
    /// was damaged after it was written.
    Failed,
    /// No member of that shape, so no claim: a line from before ADR-066, a
    /// torn line, a trailing `\r`, damage to the member's fixed bytes, or a
    /// line longer than [`MAX_CHECKED_LINE`].
    Unchecked,
}

/// `object` with its check (ADR-066 D1): its bytes without the closing
/// `}`, then `,"crc32c":"<CRC-32C of object, 8 lowercase hex>"}`. No `\n`.
///
/// `object` is a compact JSON object with at least one member, as
/// `serde_json::to_vec` writes it. The check itself is over bytes: an
/// `object` of `{}` gets a body that verifies but is no JSON.
pub fn with_check(object: &[u8]) -> Vec<u8> {
    debug_assert_eq!(object.last(), Some(&b'}'), "a JSON object");
    let head = object.strip_suffix(b"}").unwrap_or(object);
    let mut body = Vec::with_capacity(head.len() + CHECK_SHAPE + 1);
    body.extend_from_slice(head);
    body.extend_from_slice(CHECK_PREFIX);
    body.extend_from_slice(format!("{:08x}\"}}", crc32c(object)).as_bytes());
    body
}

/// The inverse of [`with_check`]: the object a checked body was made from,
/// or `None` when the body carries no member of the check's shape
/// ([`LineCheck::Unchecked`]). A [`LineCheck::Failed`] body is stripped
/// too. For fixtures that make journals from before ADR-066.
pub fn strip_check(body: &[u8]) -> Option<Vec<u8>> {
    let (head, _) = split_check(body)?;
    let mut object = Vec::with_capacity(head.len() + 1);
    object.extend_from_slice(head);
    object.push(b'}');
    Some(object)
}

/// The verdict of one line `body`: the bytes between two `\n`, without
/// them (ADR-066 D2). A last line without its `\n` may be in the middle of
/// being written, and callers do not classify it.
pub fn check_line(body: &[u8]) -> LineCheck {
    match split_check(body) {
        None => LineCheck::Unchecked,
        // The CRC of the head with its `}` restored, without a copy.
        Some((head, crc)) if !crc32c_update(crc32c_update(!0, head), b"}") == crc => {
            LineCheck::Intact
        }
        Some(_) => LineCheck::Failed,
    }
}

/// An open journal for one run. It holds the journal lock
/// ([`replica::try_journal_lock`]) for as long as it exists, so every line
/// of the run, `run_end` and the final pass's `replication` included, is
/// written under it, and `mie store repair-hot --scope journal` and `mie
/// store restore` refuse while it is open (review of #226, finding 3).
#[derive(Debug)]
pub struct Journal {
    out: BufWriter<File>,
    run_id: String,
    write_error: Option<String>,
    _lock: JournalLock,
}

impl Journal {
    /// Opens `path` for appending, creating it and its directory, takes the
    /// journal lock without waiting, and then ends a torn last line with a
    /// `\n` ([`terminate_torn_tail`]), so the run's first line starts a line
    /// of its own. The check runs only under the lock: a writer that holds
    /// it is never raced.
    ///
    /// # Errors
    ///
    /// A description of the I/O failure, or of the held lock.
    pub fn open(path: &Path, run_id: &str) -> Result<Self, String> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("open journal {}: {e}", path.display()))?;
        let lock = replica::try_journal_lock(path)
            .map_err(|e| format!("the journal lock of {}: {e}", path.display()))?;
        terminate_torn_tail(&mut file, path)?;
        Ok(Self {
            out: BufWriter::new(file),
            run_id: run_id.to_owned(),
            write_error: None,
            _lock: lock,
        })
    }

    /// Appends one line of `kind` with `fields`, stamped at `at_utc_ns`, and
    /// ends it with its check ([`with_check`], ADR-066 D1). A field named
    /// [`CHECK_FIELD`] is dropped at every depth: the name is the check's.
    pub fn write(&mut self, at_utc_ns: i64, kind: &str, fields: Value) {
        debug_assert!(
            KNOWN_KINDS.contains(&kind),
            "{kind} is not in KNOWN_KINDS (ADR-068 D1)"
        );
        let mut line = Map::new();
        line.insert("type".to_owned(), json!(kind));
        line.insert("run_id".to_owned(), json!(self.run_id));
        line.insert("at_ms".to_owned(), json!(at_utc_ns.div_euclid(1_000_000)));
        if let Value::Object(fields) = fields {
            line.extend(fields);
        }
        let mut line = Value::Object(line);
        drop_check_fields(&mut line);
        let result = serde_json::to_vec(&line)
            .map_err(|e| e.to_string())
            .and_then(|object| {
                let mut body = with_check(&object);
                body.push(b'\n');
                self.out.write_all(&body).map_err(|e| e.to_string())
            });
        if let Err(error) = result {
            self.write_error.get_or_insert(error);
        }
    }

    /// Flushes buffered lines.
    pub fn flush(&mut self) {
        if let Err(error) = self.out.flush() {
            self.write_error.get_or_insert(error.to_string());
        }
    }

    /// Flushes buffered lines and forces them to stable storage. `run_start`
    /// is synced: without it a run's sealed records cannot be replayed as
    /// live delivered them (ADR-039 D2).
    pub fn sync(&mut self) {
        self.flush();
        if let Err(error) = self.out.get_ref().sync_data() {
            self.write_error.get_or_insert(error.to_string());
        }
    }

    /// The first write failure, if any.
    pub fn write_error(&self) -> Option<&str> {
        self.write_error.as_deref()
    }
}

/// Appends one `\n` to the journal when it is not empty and its last byte
/// is not one (#225). [`Journal::write`] hands its lines to a `BufWriter`
/// that flushes when its buffer is full, not at line ends, so a crash after
/// a partial flush leaves a last line without its `\n`. The next run's
/// `run_start` would be glued onto it: [`read_runs`] skips the glued line as
/// no JSON and then fails the whole journal on the run's next line, "...
/// without run_start".
///
/// The torn bytes are kept, never truncated: they are the crash's evidence,
/// and readers skip them as no JSON. Nothing is reported: the crashed run
/// already shows as a run without `run_end`. The check reads one byte, never
/// text, so a tail torn inside a multibyte character is ended too. No sync
/// here: the run's `run_start`, written next, is synced, and that covers this
/// byte of the same file.
///
/// # Errors
///
/// A description of the I/O failure: a run never appends onto a tail that
/// could not be checked.
fn terminate_torn_tail(file: &mut File, path: &Path) -> Result<(), String> {
    use std::io::{Read, Seek, SeekFrom};
    let failed =
        |e: std::io::Error| format!("end the torn last line of journal {}: {e}", path.display());
    if file.metadata().map_err(failed)?.len() == 0 {
        return Ok(());
    }
    let mut last = [0; 1];
    file.seek(SeekFrom::End(-1)).map_err(failed)?;
    file.read_exact(&mut last).map_err(failed)?;
    if last != *b"\n" {
        // Append mode writes at the end, wherever the cursor is.
        file.write_all(b"\n").map_err(failed)?;
    }
    Ok(())
}

/// A shared journal as the capture's observer.
#[derive(Debug, Clone)]
pub struct JournalObserver(pub Arc<Mutex<Journal>>);

impl CaptureObserver for JournalObserver {
    fn on_event(&mut self, at_utc_ns: i64, event: &CaptureEvent) {
        let (kind, fields) = describe(event);
        let mut journal = self.0.lock().unwrap_or_else(|p| p.into_inner());
        journal.write(at_utc_ns, kind, fields);
        if matches!(event, CaptureEvent::Stats(_)) {
            journal.flush();
        }
    }
}

/// The journal line type and fields of a capture event.
fn describe(event: &CaptureEvent) -> (&'static str, Value) {
    match event {
        CaptureEvent::Connected { stream, session_id } => (
            "connected",
            json!({"stream": stream.raw_name(), "session_id": session_id}),
        ),
        CaptureEvent::ConnectFailed { stream, error } => (
            "connect_failed",
            json!({"stream": stream.raw_name(), "error": error}),
        ),
        CaptureEvent::Disconnected {
            stream,
            session_id,
            reason,
        } => (
            "disconnected",
            json!({"stream": stream.raw_name(), "session_id": session_id, "reason": reason}),
        ),
        CaptureEvent::PlannedRotation { stream, session_id } => (
            "planned_rotation",
            json!({"stream": stream.raw_name(), "session_id": session_id}),
        ),
        CaptureEvent::Backoff { stream, delay_ms } => (
            "backoff",
            json!({"stream": stream.raw_name(), "delay_ms": delay_ms}),
        ),
        CaptureEvent::OiPoll {
            request_time_ns,
            response_time_ns,
            status,
            error,
            persisted,
        } => (
            "oi_poll",
            json!({
                "request_time_ns": request_time_ns,
                "response_time_ns": response_time_ns,
                "status": status,
                "error": error,
                "persisted": persisted,
            }),
        ),
        CaptureEvent::DepthSnapshotFetch {
            trigger,
            request_ns,
            response_ns,
            status,
            error,
            persisted,
        } => (
            "depth_snapshot_fetch",
            json!({
                "trigger": match trigger {
                    SnapshotTrigger::Sync => "sync",
                    SnapshotTrigger::Checkpoint => "checkpoint",
                },
                "request_time_ns": request_ns,
                "response_time_ns": response_ns,
                "status": status,
                "error": error,
                "persisted": persisted,
            }),
        ),
        CaptureEvent::Book {
            transition,
            receive_seq,
        } => ("book", book_json(transition, *receive_seq)),
        CaptureEvent::BookCheckpoint(result) => ("book_checkpoint", checkpoint_json(result)),
        CaptureEvent::Gap(gap) => (
            "gap",
            json!({
                "stream": format!("{:?}", gap.stream),
                "start": gap.start.as_millis(),
                "end": gap.end.as_millis(),
                "reason": format!("{:?}", gap.reason),
            }),
        ),
        CaptureEvent::Sealed(files) => ("sealed", json!({"files": files_json(files)})),
        CaptureEvent::NormalizeError {
            stream,
            receive_seq,
            error,
        } => (
            "normalize_error",
            json!({"stream": stream.raw_name(), "receive_seq": receive_seq, "error": error}),
        ),
        CaptureEvent::Stats(stats) => (
            "stats",
            json!({
                "records": stats.records,
                "time_fallbacks": stats.time_fallbacks,
                "sealed_files": stats.sealed_files,
                "buffered": stats.buffered,
                "inbound": channel_json(&stats.inbound),
                "core": channel_json(&stats.core),
                "streams": pipeline_json(&stats.pipeline),
            }),
        ),
    }
}

/// An order-book sync change as journal JSON.
fn book_json(transition: &BookTransition, receive_seq: u64) -> Value {
    let mut value = match transition {
        BookTransition::Desynced(cause) => {
            json!({"event": "desync", "cause": format!("{cause:?}")})
        }
        BookTransition::Synced {
            last_update_id,
            time,
        } => json!({"event": "sync", "last_update_id": last_update_id, "time": time.as_millis()}),
        BookTransition::SnapshotRejected(reason) => {
            json!({"event": "snapshot_rejected", "reason": format!("{reason:?}")})
        }
        BookTransition::CheckpointEmitted {
            last_update_id,
            time,
        } => json!({
            "event": "checkpoint_emitted",
            "last_update_id": last_update_id,
            "time": time.as_millis(),
        }),
        BookTransition::CheckpointSkipped(reason) => {
            json!({"event": "checkpoint_skipped", "reason": format!("{reason:?}")})
        }
    };
    value["receive_seq"] = json!(receive_seq);
    value
}

/// A checkpoint verdict as journal JSON.
fn checkpoint_json(result: &CheckpointResult) -> Value {
    match result {
        CheckpointResult::Matched { bid, ask } => coverage_json("matched", bid, ask),
        CheckpointResult::Inconclusive { bid, ask } => {
            let mut value = coverage_json("inconclusive", bid, ask);
            value["min_side_levels"] = json!(MIN_SIDE_LEVELS);
            value
        }
        CheckpointResult::Mismatched {
            levels,
            mismatches,
            examples,
        } => json!({
            "result": "mismatched",
            "levels": levels,
            "mismatches": mismatches,
            "examples": examples
                .iter()
                .map(|m| json!({
                    "side": side(m.side),
                    "price": m.price.to_string(),
                    "ours": m.ours.map(|q| q.to_string()),
                    "theirs": m.theirs.map(|q| q.to_string()),
                }))
                .collect::<Vec<_>>(),
        }),
        CheckpointResult::Unverifiable(reason) => {
            json!({"result": "unverifiable", "reason": format!("{reason:?}")})
        }
        CheckpointResult::Invalidated(why) => json!({
            "result": "invalidated",
            "reason": match why {
                Invalidation::Gap(reason) => format!("Gap({reason:?})"),
                Invalidation::MissedStraddle {
                    snapshot_id,
                    first_update_id,
                    last_update_id,
                } => format!(
                    "MissedStraddle(snapshot {snapshot_id}, update {first_update_id}..={last_update_id})"
                ),
                Invalidation::ChainBreak { expected, found } => {
                    format!("ChainBreak(expected {expected}, found {found})")
                }
                Invalidation::NegativeQty { side: s, price } => {
                    format!("NegativeQty({} {price})", side(*s))
                }
            },
        }),
    }
}

/// A verdict that carries per-side coverage: the total level count (kept for
/// continuity with pre-ADR-049 lines), the per-side counts and the window
/// widths in hundredths of a basis point (ADR-049 D5).
fn coverage_json(result: &str, bid: &SideCoverage, ask: &SideCoverage) -> Value {
    json!({
        "result": result,
        "levels": bid.levels + ask.levels,
        "levels_bid": bid.levels,
        "levels_ask": ask.levels,
        "window_bid_cbps": bid.width_cbps,
        "window_ask_cbps": ask.width_cbps,
    })
}

fn side(side: Side) -> &'static str {
    match side {
        Side::Bid => "bid",
        Side::Ask => "ask",
    }
}

/// A replication pass as its `replication` line (ADR-062 D11): counts and
/// the first problem only, since a pass with a new target can name hundreds
/// of thousands of files, 288 times a day.
pub fn replication_json(report: &PassReport, plan: Option<&RemovalPlan>) -> Value {
    json!({
        "passed": report.problems.is_empty(),
        "files_copied": report.files_copied,
        "entries_copied": report.entries_copied,
        "ledger_bytes": report.ledger_bytes,
        "journal_bytes": report.journal_bytes,
        "problems": report.problems.len(),
        "first_problem": report.problems.first(),
        "candidates": plan.map(|plan| plan.candidates.len()),
        "removal_waiting": plan.and_then(|plan| plan.waiting.as_deref()),
    })
}

/// A truncation of the copy ledger's trailing record (ADR-062 D7).
pub fn copy_ledger_truncated_json(ledger: &Path, truncation: Truncation) -> Value {
    json!({
        "ledger": ledger.display().to_string(),
        "bytes": truncation.bytes,
        "offset": truncation.offset,
    })
}

/// A hot-removal pass of one source (ADR-062 D6).
pub fn removal_json(report: &RemovalReport) -> Value {
    json!({
        "source": report.source,
        "removed": report.removed,
        "completed": report.completed,
        "skipped": report
            .skipped
            .iter()
            .map(|(partition, reason)| json!({"partition": partition, "reason": reason}))
            .collect::<Vec<_>>(),
        "locked": report.locked,
        "error": report.error,
    })
}

/// A recalled partition (ADR-062 D6).
pub(crate) fn recall_json(recalled: &Recalled) -> Value {
    json!({
        "partition": recalled.partition,
        "files": recalled.files,
        "from": recalled.from,
        "damaged": recalled
            .damaged
            .iter()
            .map(|(path, root, error, _)| json!({"path": path, "root": root, "error": error}))
            .collect::<Vec<_>>(),
    })
}

/// Sealed files as journal JSON.
pub fn files_json(files: &[SealedFile]) -> Value {
    files
        .iter()
        .map(|f| {
            json!({
                "path": f.relative_path,
                "rows": f.rows,
                "bytes": f.bytes,
                "min_event_time": f.min_event_time.as_millis(),
                "max_event_time": f.max_event_time.as_millis(),
            })
        })
        .collect()
}

fn channel_json(channel: &ChannelStats) -> Value {
    json!({
        "capacity": channel.capacity,
        "sent": channel.sent,
        "high_water": channel.high_water,
        "blocked_ms": channel.blocked_ns / 1_000_000,
    })
}

/// Per-stream pipeline counters as journal JSON. The order-book sync
/// counters ride on the `depth` stream, under `book`.
pub fn pipeline_json(stats: &PipelineStats) -> Value {
    let mut streams: Map<String, Value> = stats
        .streams
        .iter()
        .map(|(stream, s)| {
            let gaps: Map<String, Value> = s
                .gaps
                .iter()
                .map(|(reason, n)| (format!("{reason:?}"), json!(n)))
                .collect();
            let value = json!({
                "records": s.records,
                "events": s.events,
                "normalize_errors": s.normalize_errors,
                "duplicates": s.duplicates,
                "retimed": s.retimed,
                "regressions": s.regressions,
                "gaps": gaps,
                "max_lateness_ms": s.max_lateness_ms,
            });
            (stream.raw_name().to_owned(), value)
        })
        .collect();
    if stats.book != BookStats::default() {
        let depth = streams
            .entry(BinanceStream::Depth.raw_name())
            .or_insert_with(|| json!({}));
        depth["book"] = book_stats_json(&stats.book);
    }
    Value::Object(streams)
}

/// The order-book sync counters as journal JSON.
fn book_stats_json(book: &BookStats) -> Value {
    let by = |map: Vec<(String, u64)>| -> Map<String, Value> {
        map.into_iter().map(|(k, n)| (k, json!(n))).collect()
    };
    json!({
        "syncs": book.syncs,
        "desyncs": by(book.desyncs.iter().map(|(k, n)| (format!("{k:?}"), *n)).collect()),
        "stale_diffs": book.stale_diffs,
        "buffer_drops": book.buffer_drops,
        "snapshots_rejected": by(
            book.snapshots_rejected.iter().map(|(k, n)| (format!("{k:?}"), *n)).collect()
        ),
        "snapshot_errors": book.snapshot_errors,
        "checkpoints_emitted": book.checkpoints_emitted,
        "checkpoints_skipped": by(
            book.checkpoints_skipped.iter().map(|(k, n)| (format!("{k:?}"), *n)).collect()
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(run_id: &str, at_ms: i64, source: &str) -> String {
        json!({
            "type": "run_start", "run_id": run_id, "at_ms": at_ms, "symbol": "BTCUSDT",
            "source": source, "streams": ["aggTrade", "openInterest"], "hold_back_ms": 750,
            "oi_retime_ms": 10_000, "seal_interval_secs": 300, "seeds": {"aggTrade": 5},
        })
        .to_string()
    }

    fn end(run_id: &str, at_ms: i64, exit_code: i64, records: u64) -> String {
        json!({
            "type": "run_end", "run_id": run_id, "at_ms": at_ms, "exit_code": exit_code,
            "records": records,
        })
        .to_string()
    }

    fn read(lines: &[String]) -> Result<Vec<JournaledRun>, String> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mie-journal-read-runs-{}-{n}.jsonl",
            std::process::id()
        ));
        std::fs::write(&path, lines.join("\n")).unwrap();
        let runs = runs_at(&path);
        std::fs::remove_file(&path).unwrap();
        runs
    }

    /// A single-root store's paths around `journal`: [`read_runs`] reads no
    /// copy ledger there (ADR-068 D5).
    fn single_root(journal: &Path) -> Paths {
        Paths {
            raw_root: std::path::PathBuf::new(),
            journal: journal.to_owned(),
            cold_roots: Vec::new(),
        }
    }

    /// The runs [`read_runs`] reads from the journal at `path` of a
    /// single-root store.
    fn runs_at(path: &Path) -> Result<Vec<JournaledRun>, String> {
        read_runs(&single_root(path), "binance-um").map(|read| read.runs)
    }

    #[test]
    fn runs_join_their_start_and_end_and_crashes_have_no_end() {
        let runs = read(&[
            start("A", 1_000, "binance-um"),
            r#"{"type":"gap","run_id":"A"}"#.to_owned(),
            end("A", 2_000, 0, 42),
            start("X", 2_500, "other-source"),
            end("X", 2_600, 0, 1),
            // A crash leaves a partial line; the next run follows it.
            start("B", 3_000, "binance-um"),
            r#"{"type":"stats","run_id":"B","reco"#.to_owned(),
            start("C", 4_000, "binance-um"),
            end("C", 5_000, 1, 7),
        ])
        .unwrap();
        let ids: Vec<_> = runs.iter().map(|r| r.parameters.run_id.as_str()).collect();
        assert_eq!(ids, ["A", "B", "C"]);
        let a = runs[0].live_run();
        assert_eq!(
            (a.started_at_ms, a.ended_at_ms, a.clean_records),
            (1_000, Some(2_000), Some(42))
        );
        assert_eq!(
            a.streams,
            [BinanceStream::AggTrade, BinanceStream::OpenInterest]
        );
        assert_eq!(a.seeds[&BinanceStream::AggTrade], EventTime::from_millis(5));
        assert_eq!((a.hold_back_ms, a.oi_retime_ms), (750, 10_000));
        let b = runs[1].live_run();
        assert_eq!((b.ended_at_ms, b.clean_records), (None, None));
        // A non-zero exit is not clean: the replay takes its prefix.
        let c = runs[2].live_run();
        assert_eq!((c.ended_at_ms, c.clean_records), (Some(5_000), None));
    }

    fn checkpoint(ordinal: u64, last: bool) -> StateCheckpoint {
        StateCheckpoint {
            ordinal,
            as_of: Some(EventTime::from_millis(1_000 + ordinal as i64)),
            events: EventStreamHash {
                events: ordinal,
                fingerprint: Fingerprint::from_raw(0xabc0 + ordinal),
            },
            state: StateHash::from_fingerprint(Fingerprint::from_raw(u64::MAX - ordinal)),
            last,
        }
    }

    fn checkpoint_line(run_id: &str, c: &StateCheckpoint) -> String {
        let mut value = state_checkpoint_json(c);
        value["type"] = json!("state_checkpoint");
        value["run_id"] = json!(run_id);
        value["at_ms"] = json!(1_500);
        value.to_string()
    }

    #[test]
    fn state_checkpoints_and_their_keys_join_their_run() {
        let mut checkpointed: Value =
            serde_json::from_str(&start("B", 3_000, "binance-um")).unwrap();
        checkpointed["state_checkpoint_interval_ms"] = json!(60_000);
        checkpointed["state_hash_encoding"] = json!(1);
        checkpointed["event_hash_encoding"] = json!(1);
        checkpointed["feature_set"] = json!("00000000000000ff");
        let mut first = checkpoint(10, false);
        first.as_of = None;
        let runs = read(&[
            start("A", 1_000, "binance-um"),
            end("A", 2_000, 0, 42),
            checkpointed.to_string(),
            checkpoint_line("B", &first),
            checkpoint_line("B", &checkpoint(25, true)),
            json!({
                "type": "run_end", "run_id": "B", "at_ms": 4_000, "exit_code": 0,
                "records": 30, "events": 25, "domain_rejections": 2,
            })
            .to_string(),
        ])
        .unwrap();
        // A run journaled before #13 has neither.
        assert_eq!(runs[0].checkpointing, None);
        assert!(runs[0].checkpoints.is_empty());
        assert_eq!(
            (
                runs[0].end.unwrap().events,
                runs[0].end.unwrap().domain_rejections
            ),
            (None, None)
        );
        let b = &runs[1];
        assert_eq!(
            b.checkpointing,
            Some(Checkpointing {
                interval_ms: 60_000,
                state_encoding: 1,
                event_encoding: 1,
                feature_set: FeatureSetVersion::from_fingerprint(Fingerprint::from_raw(0xff)),
            })
        );
        assert_eq!(b.checkpoints, [first, checkpoint(25, true)]);
        assert_eq!(
            (b.end.unwrap().events, b.end.unwrap().domain_rejections),
            (Some(25), Some(2))
        );
        let shown = state_checkpoint_json(&checkpoint(25, true));
        assert_eq!(shown["event_hash"], "000000000000abd9");
        assert_eq!(shown["state_hash"], "ffffffffffffffe6");
        assert_eq!(shown["as_of"], 1_025);
    }

    #[test]
    fn malformed_state_checkpoints_are_errors() {
        let good: Value =
            serde_json::from_str(&checkpoint_line("A", &checkpoint(3, false))).unwrap();
        for (field, value) in [
            ("ordinal", json!(-1)),
            ("as_of", json!("soon")),
            ("event_hash", json!("abc")),
            ("state_hash", json!("FFFFFFFFFFFFFFFF")),
            ("last", json!(1)),
        ] {
            let mut bad = good.clone();
            bad[field] = value;
            let error = read(&[start("A", 1_000, "binance-um"), bad.to_string()]).unwrap_err();
            assert!(
                error.starts_with("journal line 2: state_checkpoint "),
                "{field}: {error}"
            );
        }
        let orphan = read(&[good.to_string()]).unwrap_err();
        assert!(orphan.contains("without run_start"), "{orphan}");
        let mut keys: Value = serde_json::from_str(&start("A", 1_000, "binance-um")).unwrap();
        keys["state_checkpoint_interval_ms"] = json!(0);
        let error = read(&[keys.to_string()]).unwrap_err();
        assert!(error.contains("state_checkpoint_interval_ms"), "{error}");
        keys["state_checkpoint_interval_ms"] = json!(10_000);
        let error = read(&[keys.to_string()]).unwrap_err();
        assert!(error.contains("state_hash_encoding"), "{error}");
    }

    fn line(kind: &str, run_id: &str, fields: Value) -> String {
        let mut value = fields;
        value["type"] = json!(kind);
        value["run_id"] = json!(run_id);
        value["at_ms"] = json!(1_500);
        value.to_string()
    }

    fn handoff() -> WarmHandoff {
        WarmHandoff {
            origin_ms: 100,
            effective_origin_ms: Some(400),
            cutover_ms: 1_000,
            prefix_dataset: DatasetVersion::from_hex(&"ab".repeat(32)).unwrap(),
            prefix_last_event_ms: Some(990),
            prefix_stream: EventStreamHash {
                events: 77,
                fingerprint: Fingerprint::from_raw(0x1234),
            },
            prefix_rejections: 2,
            handoff_state: StateHash::from_fingerprint(Fingerprint::from_raw(0xfeed)),
            seeds: BTreeMap::from([
                (BinanceStream::AggTrade, EventTime::from_millis(980)),
                (BinanceStream::OpenInterest, EventTime::from_millis(960)),
            ]),
            queue_high_water: 12,
            queue_high_water_bytes: 3_456,
            warming_up: vec!["volatility.regime.1h@1".to_owned()],
        }
    }

    #[test]
    fn warm_and_cold_starts_round_trip_and_set_the_seeds_replay_takes() {
        let mut history: Value = serde_json::from_str(&start("W", 2_000, "binance-um")).unwrap();
        history["history_origin_ms"] = json!(100);
        history["history_cutover_ms"] = json!(1_000);
        let warm = WarmStart::Warm(handoff());
        let cold = WarmStart::Cold(ColdStart {
            reason: ColdReason::WarmUpFailed,
            error: Some("memory guard".to_owned()),
            origin_ms: Some(100),
            cutover_ms: Some(1_000),
        });
        let no_history = WarmStart::Cold(ColdStart {
            reason: ColdReason::NoHistory,
            error: None,
            origin_ms: None,
            cutover_ms: None,
        });
        let runs = read(&[
            start("A", 1_000, "binance-um"),
            history.to_string(),
            line("warm_start", "W", warm_start_json(&warm)),
            line("warm_ready", "W", warm_ready_json(1_234)),
            line(
                "chain_restart",
                "W",
                chain_restart_json(EventTime::from_millis(5_000)),
            ),
            line(
                "chain_restart",
                "W",
                chain_restart_json(EventTime::from_millis(6_000)),
            ),
            start("C", 3_000, "binance-um"),
            line("warm_start", "C", warm_start_json(&cold)),
            start("N", 4_000, "binance-um"),
            line("warm_start", "N", warm_start_json(&no_history)),
        ])
        .unwrap();
        // A run journaled before ADR-052 has none of it.
        let a = &runs[0];
        assert_eq!(
            (&a.history, &a.warm_start, a.ready_after_ms),
            (&None, &None, None)
        );
        assert!(a.chain_restarts.is_empty());
        let w = &runs[1];
        assert_eq!(
            w.history,
            Some(RunHistory {
                origin_ms: 100,
                cutover_ms: 1_000
            })
        );
        assert_eq!(w.warm_start, Some(warm));
        assert_eq!(w.ready_after_ms, Some(1_234));
        assert_eq!(
            w.chain_restarts,
            [EventTime::from_millis(5_000), EventTime::from_millis(6_000)]
        );
        assert_eq!(runs[2].warm_start, Some(cold));
        assert_eq!(runs[3].warm_start, Some(no_history.clone()));
        let shown = warm_start_json(&no_history);
        assert!(shown.get("origin_ms").is_none(), "{shown}");
        assert_eq!(shown["cold"], "no-history");

        // The seeds of the pipeline whose output reached the engine: the
        // warm start's for a warm run, run_start's otherwise.
        assert_eq!(
            w.parameters.seeds[&BinanceStream::AggTrade],
            EventTime::from_millis(5)
        );
        assert_eq!(w.live_run().seeds, handoff().seeds);
        assert_eq!(runs[2].live_run().seeds, runs[2].parameters.seeds);
    }

    #[test]
    fn malformed_or_repeated_warm_starts_are_errors() {
        let good = warm_start_json(&WarmStart::Warm(handoff()));
        for (field, value) in [
            ("warm", json!("yes")),
            ("cutover_ms", json!(null)),
            ("prefix_dataset", json!("abc")),
            ("prefix_event_hash", json!("XYZ")),
            ("handoff_state_hash", json!(7)),
            ("seeds", json!({"bookTicker": 1})),
            ("queue_high_water", json!(-1)),
            ("warming_up", json!([1])),
            ("effective_origin_ms", json!("later")),
        ] {
            let mut bad = good.clone();
            bad[field] = value;
            let error = read(&[
                start("A", 1_000, "binance-um"),
                line("warm_start", "A", bad),
            ])
            .unwrap_err();
            assert!(
                error.starts_with("journal line 2: warm_start "),
                "{field}: {error}"
            );
        }
        let mut cold = warm_start_json(&WarmStart::Cold(ColdStart {
            reason: ColdReason::BeforeCutover,
            error: None,
            origin_ms: None,
            cutover_ms: None,
        }));
        cold["cold"] = json!("lukewarm");
        let error = read(&[
            start("A", 1_000, "binance-um"),
            line("warm_start", "A", cold),
        ])
        .unwrap_err();
        assert!(error.contains("cold reason"), "{error}");

        let twice = read(&[
            start("A", 1_000, "binance-um"),
            line("warm_start", "A", good.clone()),
            line("warm_start", "A", good.clone()),
        ])
        .unwrap_err();
        assert!(twice.contains("second warm_start"), "{twice}");
        let orphan = read(&[line("warm_start", "A", good)]).unwrap_err();
        assert!(
            orphan.contains("warm_start of run \"A\" without run_start"),
            "{orphan}"
        );
        let ready = read(&[
            start("A", 1_000, "binance-um"),
            line("warm_ready", "A", json!({})),
        ])
        .unwrap_err();
        assert!(
            ready.contains("warm_ready without ready_after_ms"),
            "{ready}"
        );
        let restart = read(&[
            start("A", 1_000, "binance-um"),
            line("chain_restart", "A", json!({"time": "now"})),
        ])
        .unwrap_err();
        assert!(restart.contains("chain_restart without time"), "{restart}");
        let mut half: Value = serde_json::from_str(&start("A", 1_000, "binance-um")).unwrap();
        half["history_origin_ms"] = json!(100);
        let error = read(&[half.to_string()]).unwrap_err();
        assert!(error.contains("only one of history_origin_ms"), "{error}");
    }

    #[test]
    fn ambiguous_or_malformed_runs_are_errors() {
        let twice = read(&[
            start("A", 1_000, "binance-um"),
            start("A", 1_500, "binance-um"),
        ]);
        assert!(twice.unwrap_err().contains("started twice"));
        let orphan = read(&[end("A", 2_000, 0, 1)]);
        assert!(orphan.unwrap_err().contains("without run_start"));
        let ended_twice = read(&[
            start("A", 1_000, "binance-um"),
            end("A", 2_000, 0, 1),
            end("A", 2_001, 0, 1),
        ]);
        assert!(ended_twice.unwrap_err().contains("ended twice"));
        let mut broken: Value = serde_json::from_str(&start("A", 1_000, "binance-um")).unwrap();
        broken["streams"] = json!(["bookTicker"]);
        let unknown = read(&[broken.to_string()]);
        assert!(
            unknown
                .unwrap_err()
                .starts_with("journal line 1: run_start with unknown stream")
        );
        let no_records = read(&[
            start("A", 1_000, "binance-um"),
            json!({"type": "run_end", "run_id": "A", "at_ms": 2, "exit_code": 0}).to_string(),
        ]);
        assert!(no_records.unwrap_err().contains("run_end without records"));
    }

    /// Like [`read`], over raw bytes: a journal need not be valid UTF-8.
    fn read_bytes(bytes: &[u8]) -> Result<Vec<JournaledRun>, String> {
        let dir = Scratch::new();
        let path = dir.journal();
        std::fs::write(&path, bytes).unwrap();
        runs_at(&path)
    }

    /// A directory of one test's journal (and its lock), named like
    /// [`read`]'s files and removed on drop.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("mie-journal-open-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn journal(&self) -> std::path::PathBuf {
            self.0.join("journal.jsonl")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Opens the journal at `path` for run `run_id` and writes its
    /// `run_start` and `warm_ready`, as `mie ingest` does.
    fn append_run(path: &Path, run_id: &str, at_ms: i64) {
        let mut journal = Journal::open(path, run_id).unwrap();
        let fields: Value = serde_json::from_str(&start(run_id, at_ms, "binance-um")).unwrap();
        journal.write(at_ms * 1_000_000, "run_start", fields);
        journal.write(
            (at_ms + 10) * 1_000_000,
            "warm_ready",
            json!({"ready_after_ms": 10}),
        );
        journal.sync();
        assert_eq!(journal.write_error(), None);
    }

    fn ids(runs: &[JournaledRun]) -> Vec<&str> {
        runs.iter().map(|r| r.parameters.run_id.as_str()).collect()
    }

    /// `original`'s bytes, one `\n`, then the next run's `run_start`.
    fn assert_terminated_once(after: &[u8], original: &[u8]) {
        assert!(after.starts_with(original), "the torn bytes are kept");
        assert_eq!(after[original.len()], b'\n');
        let next = after[original.len() + 1..].split(|b| *b == b'\n').next();
        let next: Value = serde_json::from_slice(next.unwrap()).unwrap();
        assert_eq!(next["type"], "run_start");
    }

    #[test]
    fn a_torn_last_line_does_not_swallow_the_next_runs_start() {
        let dir = Scratch::new();
        let path = dir.journal();
        // Run A crashed after a partial flush: its last line has no `\n`.
        let original = format!(
            "{}\n{}\n{}",
            start("A", 1_000, "binance-um"),
            r#"{"type":"gap","run_id":"A"}"#,
            r#"{"type":"stats","run_id":"A","reco"#,
        );
        std::fs::write(&path, &original).unwrap();
        append_run(&path, "B", 3_000);
        let runs = runs_at(&path).unwrap();
        assert_eq!(ids(&runs), ["A", "B"]);
        assert_terminated_once(&std::fs::read(&path).unwrap(), original.as_bytes());
        // The crash stays visible: A has no run_end.
        assert_eq!(runs[0].live_run().ended_at_ms, None);
        assert_eq!(runs[1].ready_after_ms, Some(10));
    }

    #[test]
    fn a_tail_torn_inside_a_multibyte_character_does_not_hide_the_next_run() {
        let dir = Scratch::new();
        let path = dir.journal();
        let mut original = format!(
            "{}\n{}\n{}\n",
            start("A", 1_000, "binance-um"),
            end("A", 2_000, 0, 42),
            start("C", 2_500, "binance-um"),
        )
        .into_bytes();
        // The first two bytes of the three of `€`.
        original.extend_from_slice(br#"{"type":"stats","run_id":"C","note":""#);
        original.extend_from_slice(&[0xE2, 0x82]);
        std::fs::write(&path, &original).unwrap();
        append_run(&path, "B", 3_000);
        let runs = runs_at(&path).unwrap();
        assert_eq!(ids(&runs), ["A", "C", "B"]);
        assert_terminated_once(&std::fs::read(&path).unwrap(), &original);
        assert_eq!(runs[0].live_run().clean_records, Some(42));
    }

    #[test]
    fn a_line_that_is_not_utf8_is_skipped_like_one_that_is_not_json() {
        let around = |bad: &[u8]| {
            let mut bytes = format!(
                "{}\n{}\n",
                start("A", 1_000, "binance-um"),
                end("A", 2_000, 0, 42)
            )
            .into_bytes();
            bytes.extend_from_slice(bad);
            bytes.extend_from_slice(
                format!(
                    "\n{}\n{}\n",
                    start("B", 3_000, "binance-um"),
                    end("B", 4_000, 0, 7)
                )
                .as_bytes(),
            );
            read_bytes(&bytes)
        };
        // A bare invalid byte, outside any JSON string.
        let runs = around(&[0xFF]).unwrap();
        assert_eq!(ids(&runs), ["A", "B"]);
        // An object of the right shape with an invalid byte in a string.
        let runs = around(b"{\"type\":\"stats\",\"run_id\":\"A\",\"note\":\"\xFF\"}").unwrap();
        assert_eq!(ids(&runs), ["A", "B"]);
        assert_eq!(runs[1].live_run().clean_records, Some(7));
    }

    #[test]
    fn error_line_numbers_count_the_skipped_non_utf8_line() {
        let mut bytes = format!("{}\n", start("A", 1_000, "binance-um")).into_bytes();
        bytes.extend_from_slice(b"\xFF\n");
        bytes.extend_from_slice(
            json!({"type": "run_end", "run_id": "A", "at_ms": 2, "exit_code": 0})
                .to_string()
                .as_bytes(),
        );
        let error = read_bytes(&bytes).unwrap_err();
        assert!(
            error.starts_with("journal line 3: run_end without records"),
            "{error}"
        );
    }

    #[test]
    fn crlf_and_an_unterminated_last_record_still_parse() {
        let text = format!(
            "{}\r\n{}\r\n{}\r\n{}",
            start("A", 1_000, "binance-um"),
            end("A", 2_000, 0, 42),
            start("B", 3_000, "binance-um"),
            end("B", 4_000, 0, 7),
        );
        let runs = read_bytes(text.as_bytes()).unwrap();
        assert_eq!(ids(&runs), ["A", "B"]);
        assert_eq!(runs[1].live_run().clean_records, Some(7));
    }

    #[test]
    fn a_terminated_journal_gains_no_byte_on_open() {
        let dir = Scratch::new();
        let path = dir.journal();
        let original = format!(
            "{}\n{}\n",
            start("A", 1_000, "binance-um"),
            end("A", 2_000, 0, 42)
        );
        std::fs::write(&path, &original).unwrap();
        drop(Journal::open(&path, "B").unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), original.as_bytes());
    }

    #[test]
    fn a_new_or_empty_journal_gains_no_byte_on_open() {
        let dir = Scratch::new();
        let missing = dir.0.join("new").join("journal.jsonl");
        drop(Journal::open(&missing, "A").unwrap());
        assert_eq!(std::fs::read(&missing).unwrap(), b"");
        let empty = dir.journal();
        std::fs::write(&empty, b"").unwrap();
        drop(Journal::open(&empty, "A").unwrap());
        assert_eq!(std::fs::read(&empty).unwrap(), b"");
    }

    #[test]
    fn a_journal_of_one_torn_fragment_is_terminated() {
        let dir = Scratch::new();
        let path = dir.journal();
        let original = br#"{"type":"run_st"#;
        std::fs::write(&path, original).unwrap();
        append_run(&path, "B", 3_000);
        assert_eq!(ids(&runs_at(&path).unwrap()), ["B"]);
        assert_terminated_once(&std::fs::read(&path).unwrap(), original);
    }

    #[test]
    fn a_complete_record_missing_its_newline_still_counts() {
        let dir = Scratch::new();
        let path = dir.journal();
        let original = format!(
            "{}\n{}",
            start("A", 1_000, "binance-um"),
            end("A", 2_000, 0, 42)
        );
        std::fs::write(&path, &original).unwrap();
        append_run(&path, "B", 3_000);
        let runs = runs_at(&path).unwrap();
        assert_eq!(ids(&runs), ["A", "B"]);
        assert_terminated_once(&std::fs::read(&path).unwrap(), original.as_bytes());
        let a = runs[0].live_run();
        assert_eq!((a.ended_at_ms, a.clean_records), (Some(2_000), Some(42)));
    }

    #[test]
    fn reopening_adds_the_newline_only_once() {
        let dir = Scratch::new();
        let path = dir.journal();
        let original = format!(
            "{}\n{}",
            start("A", 1_000, "binance-um"),
            r#"{"type":"stats","run_id":"A","reco"#
        );
        std::fs::write(&path, &original).unwrap();
        drop(Journal::open(&path, "B").unwrap());
        drop(Journal::open(&path, "C").unwrap());
        assert_eq!(
            std::fs::read(&path).unwrap(),
            format!("{original}\n").as_bytes()
        );
    }

    #[test]
    fn a_held_journal_lock_refuses_before_any_byte_is_written() {
        let dir = Scratch::new();
        let path = dir.journal();
        let original = format!(
            "{}\n{}",
            start("A", 1_000, "binance-um"),
            r#"{"type":"stats","run_id":"A","reco"#
        );
        std::fs::write(&path, &original).unwrap();
        let held = replica::try_journal_lock(&path).unwrap();
        let error = Journal::open(&path, "B").unwrap_err();
        assert!(error.contains("journal lock"), "{error}");
        assert_eq!(std::fs::read(&path).unwrap(), original.as_bytes());
        drop(held);
    }

    #[test]
    fn the_guard_works_on_bytes_not_text() {
        let dir = Scratch::new();
        let path = dir.journal();
        let mut original = format!(
            "{}\n{}\n",
            start("A", 1_000, "binance-um"),
            end("A", 2_000, 0, 42)
        )
        .into_bytes();
        original.extend_from_slice(br#"{"type":"stats","run_id":"A","note":""#);
        original.extend_from_slice(&[0xE2, 0x82]);
        std::fs::write(&path, &original).unwrap();
        drop(Journal::open(&path, "B").unwrap());
        let mut expected = original.clone();
        expected.push(b'\n');
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert_eq!(ids(&runs_at(&path).unwrap()), ["A"]);
    }

    // -----------------------------------------------------------------
    // The line check (ADR-066).

    /// A compact object of exactly `len` bytes: `{"a":"xx…x"}`.
    fn object_of(len: usize) -> Vec<u8> {
        assert!(len >= 8);
        let mut object = br#"{"a":""#.to_vec();
        object.resize(len - 2, b'x');
        object.extend_from_slice(br#""}"#);
        object
    }

    /// `object` with a member of the check's name but `separator` between
    /// `:` and the hex, and a correct CRC: a look-alike of the shape.
    fn member_with(object: &[u8], name: &str, separator: &str) -> Vec<u8> {
        let mut body = object[..object.len() - 1].to_vec();
        body.extend_from_slice(
            format!(",\"{name}\":{separator}\"{:08x}\"}}", crc32c(object)).as_bytes(),
        );
        body
    }

    /// The bodies of a journal's lines; the piece after the last `\n` is
    /// returned too (empty when the journal ends with one).
    fn bodies(bytes: &[u8]) -> Vec<&[u8]> {
        bytes.split(|b| *b == b'\n').collect()
    }

    /// `bytes` with every checked line stripped back to its object: the
    /// same journal as a writer from before ADR-066 would have left it.
    fn stripped(bytes: &[u8]) -> Vec<u8> {
        let lines: Vec<Vec<u8>> = bodies(bytes)
            .into_iter()
            .map(|body| strip_check(body).unwrap_or_else(|| body.to_vec()))
            .collect();
        lines.join(&b'\n')
    }

    /// A small deterministic generator (Knuth's MMIX LCG).
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 33
        }
    }

    #[test]
    fn crc32c_matches_the_standard_check_values() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        // RFC 3720 B.4.
        assert_eq!(crc32c(&[0x00; 32]), 0x8a91_36aa);
        assert_eq!(crc32c(&[0xff; 32]), 0x62a8_ab43);
        let ascending: Vec<u8> = (0..32).collect();
        assert_eq!(crc32c(&ascending), 0x46dd_794e);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn a_written_line_has_its_golden_bytes() {
        let dir = Scratch::new();
        let path = dir.journal();
        let mut journal = Journal::open(&path, "R1").unwrap();
        journal.write(
            1_700_000_000_123_456_789,
            "gap",
            json!({"stream": "aggTrade", "start": 1, "end": 2, "reason": "Disconnected"}),
        );
        journal.sync();
        assert_eq!(journal.write_error(), None);
        // The CRC-32C of the line up to `"type":"gap"}`, computed
        // independently of this code.
        let golden = concat!(
            r#"{"at_ms":1700000000123,"end":2,"reason":"Disconnected","run_id":"R1","#,
            r#""start":1,"stream":"aggTrade","type":"gap","crc32c":"6cb9bc4c"}"#,
            "\n"
        );
        assert_eq!(
            String::from_utf8(std::fs::read(&path).unwrap()).unwrap(),
            golden
        );
    }

    #[test]
    fn every_written_line_carries_the_check_and_it_verifies() {
        let dir = Scratch::new();
        let path = dir.journal();
        append_run(&path, "A", 1_000);
        let mut journal = Journal::open(&path, "A").unwrap();
        journal.write(
            1_500_000_000,
            "stats",
            json!({"streams": {"depth": {"book": {"syncs": 1}}}, "note": "a \"quoted\" }"}),
        );
        journal.write(1_600_000_000, "sealed", json!({"files": []}));
        let fields: Value = serde_json::from_str(&end("A", 2_000, 0, 42)).unwrap();
        journal.write(2_000_000_000, "run_end", fields);
        journal.sync();
        let bytes = std::fs::read(&path).unwrap();
        let lines = bodies(&bytes);
        assert_eq!(lines.last(), Some(&&b""[..]), "the journal ends with a \\n");
        let lines = &lines[..lines.len() - 1];
        assert_eq!(lines.len(), 5);
        for body in lines {
            assert_eq!(check_line(body), LineCheck::Intact, "{body:?}");
            let value: Value = serde_json::from_slice(body).unwrap();
            assert_eq!(value[CHECK_FIELD].as_str().unwrap().len(), 8);
            assert!(body.ends_with(b"\"}"));
            assert_eq!(
                &body[body.len() - CHECK_SHAPE..][..CHECK_PREFIX.len()],
                CHECK_PREFIX
            );
        }
        let runs = runs_at(&path).unwrap();
        assert_eq!(ids(&runs), ["A"]);
        assert_eq!(runs[0].live_run().clean_records, Some(42));
    }

    #[test]
    fn the_check_is_the_crc_of_the_line_without_its_member() {
        let dir = Scratch::new();
        let path = dir.journal();
        append_run(&path, "A", 1_000);
        let bytes = std::fs::read(&path).unwrap();
        for body in bodies(&bytes).into_iter().filter(|b| !b.is_empty()) {
            let object = strip_check(body).unwrap();
            let mut value: Value = serde_json::from_slice(body).unwrap();
            let hex = value.as_object_mut().unwrap().remove(CHECK_FIELD).unwrap();
            // The member is the last; the rest is serialized as before.
            assert_eq!(object, serde_json::to_vec(&value).unwrap());
            assert_eq!(hex, format!("{:08x}", crc32c(&object)));
            assert_eq!(with_check(&object), body);
        }
    }

    #[test]
    fn a_caller_field_named_crc32c_is_dropped() {
        let dir = Scratch::new();
        let path = dir.journal();
        let mut journal = Journal::open(&path, "A").unwrap();
        journal.write(
            1_000_000_000,
            "stats",
            json!({"crc32c": "00000000", "records": 3}),
        );
        journal.sync();
        let bytes = std::fs::read(&path).unwrap();
        let body = bodies(&bytes)[0];
        assert_eq!(check_line(body), LineCheck::Intact);
        let object: Value = serde_json::from_slice(&strip_check(body).unwrap()).unwrap();
        assert_eq!(object.get(CHECK_FIELD), None);
        assert_eq!(object["records"], 3);
        let text = std::str::from_utf8(body).unwrap();
        assert_eq!(text.matches("crc32c").count(), 1, "{text}");
    }

    #[test]
    fn a_nested_field_named_crc32c_is_dropped() {
        let dir = Scratch::new();
        let path = dir.journal();
        let mut journal = Journal::open(&path, "A").unwrap();
        journal.write(
            1_000_000_000,
            "stats",
            json!({
                "inner": {"a": 1, "crc32c": "00000000"},
                "list": [{"b": 2, "crc32c": "00000000"}, 3],
            }),
        );
        journal.sync();
        let bytes = std::fs::read(&path).unwrap();
        let body = bodies(&bytes)[0];
        assert_eq!(check_line(body), LineCheck::Intact);
        let text = std::str::from_utf8(body).unwrap();
        assert_eq!(text.matches("crc32c").count(), 1, "{text}");
        let object: Value = serde_json::from_slice(&strip_check(body).unwrap()).unwrap();
        assert_eq!(object["inner"], json!({"a": 1}));
        assert_eq!(object["list"], json!([{"b": 2}, 3]));
        // Torn right after the nested object: without the drop it would end
        // in the member's shape and read as Failed.
        let torn = &body[..text.find("},\"list\"").unwrap() + 1];
        assert_eq!(check_line(torn), LineCheck::Unchecked);
    }

    #[test]
    fn the_hex_keeps_its_leading_zeros() {
        let object = (0_u64..)
            .map(|n| format!(r#"{{"n":{n}}}"#).into_bytes())
            .find(|object| crc32c(object) < 0x0100_0000)
            .unwrap();
        let body = with_check(&object);
        let hex = &body[body.len() - 10..body.len() - 2];
        assert!(hex.starts_with(b"00"), "{:?}", std::str::from_utf8(hex));
        assert_eq!(body.len(), object.len() + 20);
        assert_eq!(check_line(&body), LineCheck::Intact);
    }

    #[test]
    fn check_line_minimum_shape() {
        assert_eq!(check_line(b""), LineCheck::Unchecked);
        // The member alone, 21 bytes, even with the CRC of `}`.
        let alone = format!(",\"crc32c\":\"{:08x}\"}}", crc32c(b"}"));
        assert_eq!(alone.len(), CHECK_SHAPE);
        assert_eq!(check_line(alone.as_bytes()), LineCheck::Unchecked);
        // `{` and the member, 22 bytes: the check is over bytes, not JSON.
        let least = format!("{{,\"crc32c\":\"{:08x}\"}}", crc32c(b"{}"));
        assert_eq!(least.len(), 22);
        assert_eq!(check_line(least.as_bytes()), LineCheck::Intact);
        assert_eq!(with_check(b"{}"), least.as_bytes());
        assert_eq!(strip_check(least.as_bytes()).unwrap(), b"{}");
        let wrong = format!("{{,\"crc32c\":\"{:08x}\"}}", crc32c(b"{}") ^ 1);
        assert_eq!(check_line(wrong.as_bytes()), LineCheck::Failed);
    }

    #[test]
    fn lines_over_the_cap_are_unchecked() {
        let at_cap = with_check(&object_of(MAX_CHECKED_LINE - 20));
        assert_eq!(at_cap.len(), MAX_CHECKED_LINE);
        assert_eq!(check_line(&at_cap), LineCheck::Intact);
        let over = with_check(&object_of(MAX_CHECKED_LINE - 19));
        assert_eq!(over.len(), MAX_CHECKED_LINE + 1);
        assert_eq!(check_line(&over), LineCheck::Unchecked);
        assert_eq!(strip_check(&over), None);
    }

    #[test]
    fn a_torn_line_is_unchecked_at_every_cut() {
        let object = start("A", 1_000, "binance-um").into_bytes();
        let body = with_check(&object);
        for cut in 0..body.len() {
            let torn = &body[..cut];
            assert_eq!(check_line(torn), LineCheck::Unchecked, "cut at {cut}");
            // The same once the next `Journal::open` ends it (#225).
            let mut ended = torn.to_vec();
            ended.push(b'\n');
            ended.extend_from_slice(&with_check(&object));
            assert_eq!(check_line(bodies(&ended)[0]), LineCheck::Unchecked);
            assert_eq!(check_line(bodies(&ended)[1]), LineCheck::Intact);
        }
        assert_eq!(check_line(&body), LineCheck::Intact);
    }

    #[test]
    fn every_single_bit_flip_fails() {
        let body = with_check(&object_of(280));
        assert_eq!(body.len(), 300);
        let mut cases = 0;
        // Every byte before the member's `,`.
        for at in 0..body.len() - CHECK_SHAPE {
            for bit in 0..8 {
                let mut damaged = body.clone();
                damaged[at] ^= 1 << bit;
                assert_eq!(
                    check_line(&damaged),
                    LineCheck::Failed,
                    "bit {bit} of byte {at}"
                );
                cases += 1;
            }
        }
        assert_eq!(cases, 279 * 8);
    }

    #[test]
    fn damage_in_the_member() {
        let body = with_check(start("A", 1_000, "binance-um").as_bytes());
        let hex = body.len() - 10..body.len() - 2;
        let with = |at: usize, bytes: &[u8]| {
            let mut damaged = body[..at].to_vec();
            damaged.extend_from_slice(bytes);
            damaged.extend_from_slice(&body[at + 1..]);
            damaged
        };
        for at in hex.clone() {
            for digit in b"0123456789abcdef" {
                let expected = if *digit == body[at] {
                    LineCheck::Intact
                } else {
                    LineCheck::Failed
                };
                assert_eq!(check_line(&with(at, &[*digit])), expected, "{at} {digit}");
            }
            for other in [&b"A"[..], b"F", b"g", "０".as_bytes()] {
                assert_eq!(check_line(&with(at, other)), LineCheck::Unchecked);
            }
            if body[at].is_ascii_lowercase() {
                let upper = body[at].to_ascii_uppercase();
                assert_eq!(check_line(&with(at, &[upper])), LineCheck::Unchecked);
            }
        }
        // `"}` to `" }`.
        let end = body.len() - 1;
        assert_eq!(check_line(&with(end, b" }")), LineCheck::Unchecked);
        // No `,`.
        let comma = body.len() - CHECK_SHAPE;
        assert_eq!(body[comma], b',');
        assert_eq!(check_line(&with(comma, b"")), LineCheck::Unchecked);
        // 7 and 9 digits.
        assert_eq!(check_line(&with(hex.start, b"")), LineCheck::Unchecked);
        assert_eq!(check_line(&with(hex.start, b"00")), LineCheck::Unchecked);
        let nine = with(hex.start, &[b'0', body[hex.start]]);
        assert_eq!(check_line(&nine), LineCheck::Unchecked);
    }

    #[test]
    fn lookalike_members_are_unchecked() {
        let object = start("A", 1_000, "binance-um").into_bytes();
        assert_eq!(
            member_with(&object, "crc32c", ""),
            with_check(&object),
            "the helper builds the real member"
        );
        for (name, separator) in [
            ("crc32с", ""),         // Cyrillic `с` (U+0441) last
            ("сrc32c", ""),         // and first
            ("crc\u{200B}32c", ""), // a zero-width space
            ("crc32c", " "),
            ("crc32c", "\t"),
            ("crc32c", "\u{00A0}"),
        ] {
            let body = member_with(&object, name, separator);
            assert_eq!(
                check_line(&body),
                LineCheck::Unchecked,
                "{name:?} {separator:?}"
            );
            assert_eq!(strip_check(&body), None);
        }
        let mut crlf = with_check(&object);
        crlf.push(b'\r');
        assert_eq!(check_line(&crlf), LineCheck::Unchecked);
    }

    #[test]
    fn check_line_never_panics() {
        let body = with_check(start("A", 1_000, "binance-um").as_bytes());
        let shape = body.len() - CHECK_SHAPE;
        let hex = shape + CHECK_PREFIX.len()..body.len() - 2;
        for at in shape..body.len() {
            for value in 0..=u8::MAX {
                let mut damaged = body.clone();
                damaged[at] = value;
                let expected = if value == body[at] {
                    LineCheck::Intact
                } else if hex.contains(&at) && matches!(value, b'0'..=b'9' | b'a'..=b'f') {
                    LineCheck::Failed
                } else {
                    LineCheck::Unchecked
                };
                assert_eq!(check_line(&damaged), expected, "{value} at {at}");
            }
        }
        let mut random = Lcg(224);
        for case in 0..1_000 {
            // Bodies of 0 to 300 bytes; a checked one has a head of at least
            // one byte, or it is the member alone (`check_line_minimum_shape`).
            let len = usize::try_from(random.next() % 301).unwrap();
            let len = if case % 3 == 0 { len } else { len.max(1) };
            let mut head: Vec<u8> = (0..len).map(|_| random.next() as u8).collect();
            match case % 3 {
                0 => {
                    let verdict = check_line(&head);
                    assert_eq!(
                        strip_check(&head).is_some(),
                        verdict != LineCheck::Unchecked
                    );
                }
                1 => {
                    head.push(b'}');
                    let checked = with_check(&head);
                    assert_eq!(check_line(&checked), LineCheck::Intact, "case {case}");
                    assert_eq!(strip_check(&checked).unwrap(), head);
                }
                _ => {
                    head.push(b'}');
                    let mut checked = with_check(&head);
                    let before = checked.len() - CHECK_SHAPE;
                    let at = usize::try_from(random.next()).unwrap() % before;
                    checked[at] ^= 1 << (random.next() % 8);
                    assert_eq!(check_line(&checked), LineCheck::Failed, "case {case}");
                }
            }
        }
    }

    #[test]
    fn unicode_text_is_checked_by_bytes() {
        let object = serde_json::to_vec(&json!({"error": "chyba é 🚀 ok", "type": "x"})).unwrap();
        let rocket = "🚀".as_bytes();
        let at = object
            .windows(rocket.len())
            .position(|w| w == rocket)
            .expect("serde_json writes non-ASCII text unescaped");
        let body = with_check(&object);
        assert_eq!(check_line(&body), LineCheck::Intact);
        assert_eq!(strip_check(&body).unwrap(), object);
        let mut damaged = body.clone();
        damaged[at + 1] ^= 0x01;
        assert_eq!(check_line(&damaged), LineCheck::Failed);
        assert!(
            std::str::from_utf8(&damaged).is_ok(),
            "still text, still caught"
        );
        let mut damaged = body;
        damaged[at + 2] ^= 0x40;
        assert!(std::str::from_utf8(&damaged).is_err());
        assert_eq!(check_line(&damaged), LineCheck::Failed);
    }

    #[test]
    fn mixed_journal_reads_as_without_the_check() {
        let dir = Scratch::new();
        let path = dir.journal();
        // Run A from before ADR-066, then run B with checked lines.
        let old = format!(
            "{}\n{}\n",
            start("A", 1_000, "binance-um"),
            end("A", 2_000, 0, 42)
        );
        std::fs::write(&path, &old).unwrap();
        append_run(&path, "B", 3_000);
        let mut journal = Journal::open(&path, "B").unwrap();
        let fields: Value = serde_json::from_str(&end("B", 4_000, 0, 7)).unwrap();
        journal.write(4_000_000_000, "run_end", fields);
        journal.sync();
        drop(journal);
        let mixed = std::fs::read(&path).unwrap();
        let verdicts: Vec<LineCheck> = bodies(&mixed)
            .into_iter()
            .filter(|b| !b.is_empty())
            .map(check_line)
            .collect();
        use LineCheck::{Intact, Unchecked};
        assert_eq!(verdicts, [Unchecked, Unchecked, Intact, Intact, Intact]);
        let with_checks = runs_at(&path).unwrap();
        assert_eq!(ids(&with_checks), ["A", "B"]);
        let plain = read_bytes(&stripped(&mixed)).unwrap();
        assert!(!stripped(&mixed).windows(6).any(|w| w == b"crc32c"));
        assert_eq!(with_checks, plain);
        assert_eq!(with_checks[1].live_run().clean_records, Some(7));
    }

    // -----------------------------------------------------------------
    // The gate (ADR-068).

    /// The lines of one run as `mie ingest` writes them, the ones
    /// [`read_runs`] ignores at both ends: `recovery`, `run_start`,
    /// `warm_start`, `warm_ready`, `stats`, `state_checkpoint`,
    /// `chain_restart`, `run_end`, `replication`.
    fn run_lines(run_id: &str, at_ms: i64) -> Vec<String> {
        let mut start: Value = serde_json::from_str(&start(run_id, at_ms, "binance-um")).unwrap();
        start["state_checkpoint_interval_ms"] = json!(60_000);
        start["state_hash_encoding"] = json!(1);
        start["event_hash_encoding"] = json!(1);
        start["feature_set"] = json!("00000000000000ff");
        vec![
            line(
                "recovery",
                run_id,
                json!({"rolled_forward": [], "discarded": []}),
            ),
            start.to_string(),
            line(
                "warm_start",
                run_id,
                warm_start_json(&WarmStart::Warm(handoff())),
            ),
            line("warm_ready", run_id, warm_ready_json(25)),
            line("stats", run_id, json!({"records": 3, "note": "é"})),
            checkpoint_line(run_id, &checkpoint(3, true)),
            line(
                "chain_restart",
                run_id,
                chain_restart_json(EventTime::from_millis(at_ms + 7)),
            ),
            end(run_id, at_ms + 900, 0, 3),
            line(
                "replication",
                run_id,
                json!({"passed": true, "problems": 0}),
            ),
        ]
    }

    /// Runs A and B, nine lines each (A: lines 1–9, B: 10–18).
    fn two_runs() -> Vec<String> {
        let mut lines = run_lines("A", 1_000);
        lines.extend(run_lines("B", 5_000));
        lines
    }

    /// `lines` with their checks, each ended by `\n`, as the writer leaves
    /// them.
    fn checked(lines: &[String]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for line in lines {
            bytes.extend_from_slice(&with_check(line.as_bytes()));
            bytes.push(b'\n');
        }
        bytes
    }

    /// The span `[start, end)` of every line of `bytes` (the empty piece
    /// after a last `\n` left out).
    fn spans(bytes: &[u8]) -> Vec<(usize, usize)> {
        let mut spans = Vec::new();
        let mut start = 0;
        for body in bytes.split(|b| *b == b'\n') {
            if start < bytes.len() {
                spans.push((start, start + body.len()));
            }
            start += body.len() + 1;
        }
        spans
    }

    /// `bytes` with the last digit of line `index`'s (0-based) `at_ms`
    /// value flipped to another digit (the first could make a leading
    /// zero, which is no JSON): the line still parses, as its kind, and
    /// fails its check.
    fn flip_digit(bytes: &[u8], index: usize) -> Vec<u8> {
        let (start, end) = spans(bytes)[index];
        let value = bytes[start..end]
            .windows(8)
            .position(|w| w == b"\"at_ms\":")
            .expect("an at_ms")
            + start
            + 8;
        let at = value
            + bytes[value..]
                .iter()
                .take_while(|b| b.is_ascii_digit())
                .count()
            - 1;
        let mut damaged = bytes.to_vec();
        damaged[at] ^= 0x01;
        assert!(damaged[at].is_ascii_digit());
        damaged
    }

    /// `bytes` with line `index`'s `{` turned into `[` (one bit): no JSON.
    fn break_json(bytes: &[u8], index: usize) -> Vec<u8> {
        let (start, _) = spans(bytes)[index];
        let mut damaged = bytes.to_vec();
        assert_eq!(damaged[start], b'{');
        damaged[start] ^= 0x20;
        damaged
    }

    /// The record of line `index` of `bytes` as it is now.
    fn record_of(bytes: &[u8], index: usize, at_ms: i64) -> LostLine {
        let (start, end) = spans(bytes)[index];
        LostLine {
            at_ms,
            line: index as u64 + 1,
            offset: start as u64,
            sha256: copy_ledger::line_sha256(&bytes[start..end]),
        }
    }

    /// Reads `bytes` with `records`; the result and how often the records
    /// were fetched.
    fn gated(bytes: &[u8], records: LostLineRecords) -> (Result<RunsRead, String>, usize) {
        let mut calls = 0;
        let read = runs_from_bytes(bytes, Path::new("j.jsonl"), "binance-um", &mut || {
            calls += 1;
            records.clone()
        });
        (read, calls)
    }

    fn none() -> LostLineRecords {
        LostLineRecords::Read(Vec::new())
    }

    /// The fixture's verdicts hold: every line of `bytes` Intact except
    /// `damaged`, which is `verdict` (no test can pass vacuously).
    fn assert_verdicts(bytes: &[u8], damaged: &[usize], verdict: LineCheck) {
        for (index, (start, end)) in spans(bytes).into_iter().enumerate() {
            let expected = if damaged.contains(&index) {
                verdict
            } else {
                LineCheck::Intact
            };
            assert_eq!(check_line(&bytes[start..end]), expected, "line {index}");
        }
    }

    /// The undamaged read of [`two_runs`].
    fn undamaged() -> RunsRead {
        let (read, calls) = gated(&checked(&two_runs()), none());
        assert_eq!(calls, 0);
        read.unwrap()
    }

    fn assert_stops_at(read: Result<RunsRead, String>, bytes: &[u8], index: usize) {
        let error = read.unwrap_err();
        let (start, _) = spans(bytes)[index];
        assert!(
            error.contains(&format!(
                "line {} at byte {start} fails its check",
                index + 1
            )),
            "{error}"
        );
        assert!(
            error.contains("`mie store repair-hot --scope journal`"),
            "{error}"
        );
        assert!(
            error.contains("`mie store repair --record-lost-lines`"),
            "{error}"
        );
        assert!(error.contains(FAILED_LINE_REMEDY), "{error}");
    }

    #[test]
    fn gate_refuses_a_failed_structural_line_that_still_parses() {
        let good = checked(&two_runs());
        for (index, kind) in [
            (1, "run_start"),
            (2, "warm_start"),
            (3, "warm_ready"),
            (5, "state_checkpoint"),
            (6, "chain_restart"),
            (7, "run_end"),
        ] {
            let bytes = flip_digit(&good, index);
            assert_verdicts(&bytes, &[index], LineCheck::Failed);
            let (start, end) = spans(&bytes)[index];
            let value: Value = serde_json::from_slice(&bytes[start..end]).unwrap();
            assert_eq!(value["type"], kind);
            let (read, calls) = gated(&bytes, none());
            assert_stops_at(read, &bytes, index);
            assert_eq!(calls, 1, "{kind}");
        }
    }

    #[test]
    fn gate_refuses_a_failed_line_that_no_longer_parses() {
        // A `stats` line, a kind the reader ignores, that no longer parses:
        // its kind may have been one it consumes.
        let bytes = break_json(&checked(&two_runs()), 4);
        assert_verdicts(&bytes, &[4], LineCheck::Failed);
        let (read, _) = gated(&bytes, none());
        assert_stops_at(read, &bytes, 4);
    }

    #[test]
    fn gate_refuses_a_failed_line_that_is_not_utf8() {
        let good = checked(&two_runs());
        let (start, end) = spans(&good)[4];
        let at = good[start..end]
            .windows(2)
            .position(|w| w == "é".as_bytes())
            .unwrap()
            + start;
        let mut bytes = good.clone();
        bytes[at + 1] ^= 0x40;
        assert!(std::str::from_utf8(&bytes[start..end]).is_err());
        assert_verdicts(&bytes, &[4], LineCheck::Failed);
        let (read, _) = gated(&bytes, none());
        assert_stops_at(read, &bytes, 4);
    }

    #[test]
    fn failed_lines_of_ignored_kinds_read_as_today() {
        for kind in KNOWN_KINDS.iter().filter(|k| !GATED_KINDS.contains(k)) {
            let mut lines = two_runs();
            lines.insert(4, line(kind, "A", json!({"n": 1})));
            let good = checked(&lines);
            let bytes = flip_digit(&good, 4);
            assert_verdicts(&bytes, &[4], LineCheck::Failed);
            let (read, calls) = gated(&bytes, none());
            assert_eq!(read.unwrap(), undamaged(), "{kind}");
            assert_eq!(calls, 0, "{kind}: no ledger read");
        }
    }

    #[test]
    fn a_failed_line_whose_kind_was_renamed_is_gated() {
        let good = checked(&two_runs());
        let (start, end) = spans(&good)[7];
        let body = good[start..end].to_vec();
        let text = String::from_utf8(body.clone()).unwrap();
        for renamed in [
            text.replace("\"run_end\"", "\"run_enx\""),
            text.replace("\"run_end\"", "\"run_\u{0435}nd\""),
            text.replace("\"run_end\"", "\"run\u{200B}_end\""),
            text.replace("\"run_end\"", "\"run_end \""),
            text.replace("\"run_end\"", "\"Stats\""),
            text.replace("\"type\":\"run_end\",", ""),
            text.replace("\"type\":\"run_end\"", "\"type\":5"),
        ] {
            assert_ne!(renamed, text);
            let mut bytes = good[..start].to_vec();
            bytes.extend_from_slice(renamed.as_bytes());
            bytes.extend_from_slice(&good[end..]);
            assert_verdicts(&bytes, &[7], LineCheck::Failed);
            let (read, _) = gated(&bytes, none());
            assert_stops_at(read, &bytes, 7);
        }
    }

    #[test]
    fn an_intact_line_of_an_unknown_kind_is_ignored() {
        let mut lines = two_runs();
        lines.insert(4, line("a_future_kind", "A", json!({"n": 1})));
        let bytes = checked(&lines);
        assert_verdicts(&bytes, &[], LineCheck::Intact);
        let (read, calls) = gated(&bytes, none());
        assert_eq!(read.unwrap(), undamaged());
        assert_eq!(calls, 0);
    }

    #[test]
    fn the_gated_kinds_are_the_arms_read_runs_matches() {
        // A lone line of a kind the reader consumes is an error (no
        // run_start, or no fields); of a kind it ignores, nothing.
        let mut consumed: Vec<&str> = KNOWN_KINDS
            .iter()
            .copied()
            .chain(["a_future_kind", "Stats"])
            .filter(|kind| read(&[line(kind, "Z", json!({}))]).is_err())
            .collect();
        consumed.sort_unstable();
        let mut gated = GATED_KINDS.to_vec();
        gated.sort_unstable();
        assert_eq!(consumed, gated);
        for kind in GATED_KINDS {
            assert!(KNOWN_KINDS.contains(&kind), "{kind}");
        }
        assert_eq!(HANDOFF_KINDS, ["warm_start"]);
    }

    #[test]
    fn known_kinds_match_the_module_table() {
        let source = include_str!("journal.rs");
        let mut table: Vec<&str> = source
            .lines()
            .filter_map(|line| line.strip_prefix("//! | `"))
            .filter_map(|row| row.split(" |").next())
            .flat_map(|cell| cell.split(", "))
            .map(|name| name.trim_matches('`'))
            .filter(|name| *name != "type")
            .collect();
        table.sort_unstable();
        let mut known = KNOWN_KINDS.to_vec();
        known.sort_unstable();
        assert_eq!(table, known);
        known.dedup();
        assert_eq!(known.len(), KNOWN_KINDS.len());
    }

    #[test]
    #[should_panic(expected = "not in KNOWN_KINDS")]
    #[cfg(debug_assertions)]
    fn writing_an_unknown_kind_panics_in_debug() {
        let dir = Scratch::new();
        let mut journal = Journal::open(&dir.journal(), "A").unwrap();
        journal.write(1, "a_future_kind", json!({}));
    }

    #[test]
    fn an_unterminated_failed_last_line_is_not_classified() {
        let good = checked(&two_runs());
        let bytes = flip_digit(&good, 16);
        assert_verdicts(&bytes, &[16], LineCheck::Failed);
        // B's run_end is the last line, without its `\n`.
        let (start, _) = spans(&bytes)[17];
        let tail = &bytes[..start - 1];
        let (read, calls) = gated(tail, none());
        let read = read.unwrap();
        assert_eq!(calls, 0);
        assert_eq!(ids(&read.runs), ["A", "B"]);
        assert!(read.runs[1].end.is_some(), "read as today: from its bytes");
        assert!(read.lost.is_empty());
        // With its `\n`, it is classified and gated.
        let (read, _) = gated(&bytes[..start], none());
        assert_stops_at(read, &bytes, 16);
    }

    #[test]
    fn gate_bounds_at_max_checked_line() {
        let body = |len: usize| {
            let mut object = end("A", 1_900, 0, 3).into_bytes();
            object.pop();
            object.extend_from_slice(b",\"pad\":\"");
            object.resize(len - 20 - 2, b'x');
            object.extend_from_slice(b"\"}");
            let body = with_check(&object);
            assert_eq!(body.len(), len);
            body
        };
        let mut lines = run_lines("A", 1_000);
        lines.remove(7);
        for (len, stops) in [(MAX_CHECKED_LINE, true), (MAX_CHECKED_LINE + 1, false)] {
            let mut bytes = checked(&lines[..7]);
            let start = bytes.len();
            let mut long = body(len);
            // A pad byte: the line still parses as a run_end.
            long[len - 100] ^= 0x01;
            bytes.extend_from_slice(&long);
            bytes.push(b'\n');
            bytes.extend_from_slice(&checked(&lines[7..]));
            let verdict = check_line(&bytes[start..start + len]);
            let (read, _) = gated(&bytes, none());
            if stops {
                assert_eq!(verdict, LineCheck::Failed);
                assert_stops_at(read, &bytes, 7);
            } else {
                assert_eq!(verdict, LineCheck::Unchecked);
                assert!(read.unwrap().runs[0].end.is_some());
            }
        }
    }

    #[test]
    fn no_failed_line_reads_no_ledger() {
        let (read, calls) = gated(&checked(&two_runs()), none());
        assert_eq!(calls, 0);
        let read = read.unwrap();
        assert!(read.lost.is_empty() && read.excluded.is_empty());
        assert!(read.notes().is_empty());
    }

    #[test]
    fn the_ledger_is_read_once() {
        let good = checked(&two_runs());
        let bytes = flip_digit(&flip_digit(&good, 5), 14);
        assert_verdicts(&bytes, &[5, 14], LineCheck::Failed);
        let records = vec![record_of(&bytes, 5, 10), record_of(&bytes, 14, 11)];
        let (read, calls) = gated(&bytes, LostLineRecords::Read(records));
        assert_eq!(read.unwrap().lost.len(), 2);
        assert_eq!(calls, 1);
    }

    #[test]
    fn a_recorded_line_mid_run_reads_by_its_kind() {
        let good = checked(&two_runs());
        let whole = undamaged();
        for (index, kind) in [
            (1, "run_start"),
            (2, "warm_start"),
            (3, "warm_ready"),
            (5, "state_checkpoint"),
            (6, "chain_restart"),
            (7, "run_end"),
        ] {
            let bytes = flip_digit(&good, index);
            let record = record_of(&bytes, index, 77);
            let (read, _) = gated(&bytes, LostLineRecords::Read(vec![record]));
            let read = read.unwrap();
            assert_eq!(read.lost.len(), 1, "{kind}");
            let note = &read.notes()[0];
            assert!(note.contains("recorded lost at 77"), "{note}");
            assert!(
                note.starts_with(&format!(
                    "journal: line {} at byte {}",
                    index + 1,
                    spans(&bytes)[index].0
                )),
                "{note}"
            );
            let b = read.runs.iter().find(|r| r.parameters.run_id == "B");
            assert_eq!(b, Some(&whole.runs[1]), "B is untouched");
            match kind {
                "run_start" | "warm_start" | "warm_ready" => {
                    assert_eq!(ids(&read.runs), ["B"], "{kind}");
                    assert_eq!(read.excluded.len(), 1);
                    assert_eq!(read.excluded[0].run_id, "A");
                    assert_eq!(read.lost[0].effect, LostEffect::Excluded("A".to_owned()));
                    assert!(note.contains("so run A is excluded"), "{note}");
                }
                _ => {
                    assert_eq!(ids(&read.runs), ["A", "B"], "{kind}");
                    assert!(read.excluded.is_empty());
                    let a = &read.runs[0];
                    assert_eq!(a.lost.len(), 1);
                    assert_eq!(a.lost[0].kind.as_deref(), Some(kind));
                    let mut expected = whole.runs[0].clone();
                    match kind {
                        "state_checkpoint" => expected.checkpoints.clear(),
                        "chain_restart" => expected.chain_restarts.clear(),
                        _ => expected.end = None,
                    }
                    expected.lost = a.lost.clone();
                    assert_eq!(*a, expected, "{kind}");
                    assert!(note.contains("and is marked"), "{note}");
                }
            }
        }
    }

    #[test]
    fn a_stale_record_gates_again() {
        let good = checked(&two_runs());
        let once = flip_digit(&good, 5);
        let (start, end) = spans(&once)[5];
        let mut twice = once.clone();
        twice[start + 30] ^= 0x01;
        assert_verdicts(&twice, &[5], LineCheck::Failed);
        let intact = record_of(&good, 5, 1);
        let mut elsewhere = record_of(&twice, 5, 1);
        elsewhere.offset = spans(&twice)[6].0 as u64;
        let mut inside = record_of(&twice, 5, 1);
        inside.offset += 1;
        for stale in [record_of(&once, 5, 1), intact, elsewhere, inside] {
            let (read, _) = gated(&twice, LostLineRecords::Read(vec![stale]));
            assert_stops_at(read, &twice, 5);
        }
        assert_ne!(&twice[start..end], &once[start..end]);
        let (read, _) = gated(&twice, LostLineRecords::Read(vec![record_of(&twice, 5, 1)]));
        assert!(read.is_ok());
    }

    #[test]
    fn two_records_of_one_line_name_the_first() {
        let bytes = flip_digit(&checked(&two_runs()), 5);
        for (first, second) in [(7, 5), (5, 7), (6, 6)] {
            let mut a = record_of(&bytes, 5, first);
            let mut b = record_of(&bytes, 5, second);
            a.line = 40;
            b.line = 41;
            let (read, _) = gated(&bytes, LostLineRecords::Read(vec![a, b]));
            let note = &read.unwrap().notes()[0];
            assert!(
                note.contains(&format!("recorded lost at {first} (recorded as line 40)")),
                "{note}"
            );
        }
    }

    #[test]
    fn records_unavailable_gate_and_say_why() {
        let bytes = flip_digit(&checked(&two_runs()), 5);
        let (read, _) = gated(
            &bytes,
            LostLineRecords::Unavailable("the ledger is gone".to_owned()),
        );
        let error = read.unwrap_err();
        assert!(error.contains("the ledger is gone"), "{error}");
        assert!(error.contains("the stricter outcome"), "{error}");
        assert_stops_at(Err(error), &bytes, 5);
        // From a tiered store's paths: an absent ledger, a name that is not
        // UTF-8.
        let dir = Scratch::new();
        let tiered = |journal: std::path::PathBuf| Paths {
            raw_root: dir.0.join("hot"),
            journal,
            cold_roots: vec![crate::config::ColdRoot {
                path: dir.0.join("cold"),
                copy_target: true,
            }],
        };
        let absent = lost_line_records_of(&tiered(dir.journal()));
        let LostLineRecords::Unavailable(why) = absent else {
            panic!("{absent:?}");
        };
        assert!(why.contains("does not exist"), "{why}");
        use std::os::unix::ffi::OsStrExt;
        let odd = dir.0.join(std::ffi::OsStr::from_bytes(b"j\xFF.jsonl"));
        let odd = lost_line_records_of(&tiered(odd));
        let LostLineRecords::Unavailable(why) = odd else {
            panic!("{odd:?}");
        };
        assert!(why.contains("not UTF-8"), "{why}");
        assert_eq!(
            lost_line_records_of(&single_root(&dir.journal())),
            LostLineRecords::SingleRoot
        );
        std::fs::write(dir.journal(), &bytes).unwrap();
        let error = read_runs(&tiered(dir.journal()), "binance-um").unwrap_err();
        assert!(error.contains("does not exist"), "{error}");
        assert!(error.contains("the stricter outcome"), "{error}");
    }

    /// The effect of the one recorded line `index` of `bytes`.
    fn recorded(bytes: &[u8], index: usize) -> RunsRead {
        let (read, _) = gated(
            bytes,
            LostLineRecords::Read(vec![record_of(bytes, index, 9)]),
        );
        read.unwrap()
    }

    fn neighbours(runs: &[&str]) -> LostEffect {
        LostEffect::Neighbours(runs.iter().map(|r| (*r).to_owned()).collect())
    }

    #[test]
    fn an_untrusted_recorded_line_excludes_its_neighbours_runs() {
        let good = checked(&two_runs());
        // A's replication, between A's run_end and B's recovery.
        let read = recorded(&break_json(&good, 8), 8);
        assert_eq!(read.lost[0].effect, neighbours(&["A", "B"]));
        assert!(read.runs.is_empty());
        let excluded: Vec<&str> = read.excluded.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(excluded, ["A", "B"]);
        assert_eq!(read.excluded[1].start.as_ref().unwrap().0, 5_000);
        assert!(read.notes()[0].contains("so run(s) A, B beside it are excluded"));
        // A's stats, between two A lines.
        let read = recorded(&break_json(&good, 4), 4);
        assert_eq!(read.lost[0].effect, neighbours(&["A"]));
        assert_eq!(ids(&read.runs), ["B"]);
        // At the head, and at the tail.
        let read = recorded(&break_json(&good, 0), 0);
        assert_eq!(read.lost[0].effect, neighbours(&["A"]));
        let read = recorded(&break_json(&good, 17), 17);
        assert_eq!(read.lost[0].effect, neighbours(&["B"]));
        assert_eq!(ids(&read.runs), ["A"]);
        // The only line in the journal.
        let alone = break_json(&checked(&[end("A", 2, 0, 1)]), 0);
        let read = recorded(&alone, 0);
        assert_eq!(read.lost[0].effect, neighbours(&[]));
        assert!(read.runs.is_empty() && read.excluded.is_empty());
        assert!(
            read.notes()[0].contains("no run is excluded"),
            "{:?}",
            read.notes()
        );
        // A trusted kind with no neighbour is untrusted too.
        let alone = flip_digit(&checked(&[end("A", 2, 0, 1)]), 0);
        assert_eq!(recorded(&alone, 0).lost[0].effect, neighbours(&[]));
    }

    /// `bytes` with line `index`'s `"run_id":"A"` replaced by
    /// `"run_id":"<run>"` after its check was computed: Failed, parses.
    fn claim(bytes: &[u8], index: usize, run: &str) -> Vec<u8> {
        let (start, end) = spans(bytes)[index];
        let text = std::str::from_utf8(&bytes[start..end]).unwrap();
        let claimed = text.replacen("\"run_id\":\"A\"", &format!("\"run_id\":\"{run}\""), 1);
        assert_ne!(claimed, text);
        let mut out = bytes[..start].to_vec();
        out.extend_from_slice(claimed.as_bytes());
        out.extend_from_slice(&bytes[end..]);
        out
    }

    #[test]
    fn a_recorded_line_whose_run_id_disagrees_is_untrusted() {
        let good = checked(&two_runs());
        // A's run_end claiming run B, between two A lines.
        let bytes = claim(&good, 7, "B");
        assert_verdicts(&bytes, &[7], LineCheck::Failed);
        let read = recorded(&bytes, 7);
        assert_eq!(read.lost[0].effect, neighbours(&["A"]));
        assert_eq!(ids(&read.runs), ["B"]);
        // At the A|B boundary: A's replication turned into B's run_end.
        let mut lines = two_runs();
        lines[8] = end("A", 1_950, 0, 3);
        let bytes = claim(&checked(&lines), 8, "B");
        let read = recorded(&bytes, 8);
        assert_eq!(read.lost[0].effect, neighbours(&["A", "B"]));
        assert!(read.runs.is_empty());
        // One agreeing neighbour of two is not enough.
        let mut lines = two_runs();
        lines.insert(9, end("B", 4_000, 0, 1));
        lines.remove(8);
        let bytes = flip_digit(&checked(&lines), 8);
        let read = recorded(&bytes, 8);
        assert_eq!(read.lost[0].effect, neighbours(&["A", "B"]));
    }

    /// Runs A and B without their `replication` lines, as a single-root
    /// store writes them (the replicator writes it): A's run_end is
    /// followed by B's recovery.
    fn two_runs_single_root() -> Vec<String> {
        let mut lines = run_lines("A", 1_000);
        lines.pop();
        let mut b = run_lines("B", 5_000);
        b.pop();
        lines.extend(b);
        lines
    }

    /// Review round 1 of #235 (ADR-068 D4, a run boundary): a lost line of
    /// a kind that marks its run, whose before-neighbour is of its run and
    /// whose after-neighbour opens another run (`recovery` or `run_start`),
    /// is trusted. A's run_end then marks A and B reads untouched.
    #[test]
    fn a_lost_mark_line_before_a_run_boundary_marks_its_run() {
        let good = checked(&two_runs_single_root());
        let bytes = flip_digit(&good, 7);
        assert_verdicts(&bytes, &[7], LineCheck::Failed);
        let whole = gated(&good, none()).0.unwrap();
        let read = gated(&bytes, LostLineRecords::SingleRoot).0.unwrap();
        assert_eq!(read.lost[0].effect, LostEffect::Marked("A".to_owned()));
        assert_eq!(ids(&read.runs), ["A", "B"]);
        assert!(read.excluded.is_empty());
        assert_eq!(read.runs[0].end, None);
        assert_eq!(read.runs[1], whole.runs[1]);
        // The same with B's run_start right after A's run_end.
        let mut lines = two_runs_single_root();
        lines.remove(8);
        let bytes = flip_digit(&checked(&lines), 7);
        let read = recorded(&bytes, 7);
        assert_eq!(read.lost[0].effect, LostEffect::Marked("A".to_owned()));
        assert_eq!(ids(&read.runs), ["A", "B"]);
    }

    /// The boundary rule needs a line that opens the other run: an
    /// after-neighbour of another run that is no `recovery` or `run_start`
    /// leaves the line untrusted, and so does a before-neighbour of another
    /// run, or a kind that excludes its run.
    #[test]
    fn a_lost_line_before_another_runs_ordinary_line_stays_untrusted() {
        // A's run_end, then B's stats (B's recovery and run_start gone).
        let mut lines = two_runs_single_root();
        lines.drain(8..10);
        let bytes = flip_digit(&checked(&lines), 7);
        let read = recorded(&bytes, 7);
        assert_eq!(read.lost[0].effect, neighbours(&["A", "B"]));
        // A's run_end claiming B, before B's recovery: the before-neighbour
        // is A's.
        let bytes = claim(&checked(&two_runs_single_root()), 7, "B");
        assert_eq!(recorded(&bytes, 7).lost[0].effect, neighbours(&["A", "B"]));
        // A's warm_ready is no mark kind: before B's opener it needs both
        // sides to agree, so the runs on either side are excluded.
        let mut lines = two_runs_single_root();
        lines.drain(4..8);
        let bytes = flip_digit(&checked(&lines), 3);
        assert_eq!(recorded(&bytes, 3).lost[0].effect, neighbours(&["A", "B"]));
    }

    #[test]
    fn neighbour_walk_skips_failed_and_recorded_lines() {
        let good = checked(&two_runs());
        // A's stats, failing and claiming run Z, before A's lost checkpoint:
        // the walk skips it and reaches A's warm_ready.
        let bytes = flip_digit(&claim(&good, 4, "Z"), 5);
        assert_verdicts(&bytes, &[4, 5], LineCheck::Failed);
        let read = recorded(&bytes, 5);
        assert_eq!(read.lost[0].effect, LostEffect::Marked("A".to_owned()));
        assert_eq!(ids(&read.runs), ["A", "B"]);
        // Two recorded lines in a row share their neighbours: A's stats (no
        // JSON) and A's checkpoint.
        let bytes = flip_digit(&break_json(&good, 4), 5);
        let records = vec![record_of(&bytes, 4, 1), record_of(&bytes, 5, 2)];
        let (read, _) = gated(&bytes, LostLineRecords::Read(records));
        let read = read.unwrap();
        assert_eq!(read.lost[0].effect, neighbours(&["A"]));
        assert_eq!(read.lost[1].effect, LostEffect::Marked("A".to_owned()));
        // An unparseable line is no neighbour: A's stats broken, but
        // unchecked (legacy, so no D6 line either), before the lost
        // checkpoint.
        let mut bytes = good.clone();
        let (start, end) = spans(&good)[4];
        let stripped = strip_check(&good[start..end]).unwrap();
        let mut broken = stripped.clone();
        broken[0] = b'[';
        bytes.splice(start..end, broken);
        let bytes = flip_digit(&bytes, 5);
        let read = recorded(&bytes, 5);
        assert_eq!(read.lost[0].effect, LostEffect::Marked("A".to_owned()));
    }

    #[test]
    fn excluded_runs_raise_no_orphan_errors() {
        // A's run_start lost: A's warm_start, warm_ready, checkpoint,
        // chain_restart and run_end would each be an orphan.
        let bytes = flip_digit(&checked(&two_runs()), 1);
        let read = recorded(&bytes, 1);
        assert_eq!(ids(&read.runs), ["B"]);
        assert_eq!(read.excluded[0].start, None);
        assert_eq!(read.excluded[0].lines_at_ms, Some((1_500, 1_900)));
    }

    #[test]
    fn an_unrecorded_line_in_an_excluded_run_still_stops() {
        let bytes = flip_digit(&flip_digit(&checked(&two_runs()), 1), 7);
        let (read, _) = gated(&bytes, LostLineRecords::Read(vec![record_of(&bytes, 1, 1)]));
        assert_stops_at(read, &bytes, 7);
    }

    #[test]
    fn renumbered_records_name_both_numbers() {
        let bytes = flip_digit(&checked(&two_runs()), 6);
        let mut record = record_of(&bytes, 6, 3);
        record.line = 6;
        let (read, _) = gated(&bytes, LostLineRecords::Read(vec![record]));
        let note = &read.unwrap().notes()[0];
        assert!(
            note.contains("line 7 at byte") && note.contains("(recorded as line 6)"),
            "{note}"
        );
        let record = record_of(&bytes, 6, 3);
        let (read, _) = gated(&bytes, LostLineRecords::Read(vec![record]));
        assert!(!read.unwrap().notes()[0].contains("recorded as line"));
    }

    #[test]
    fn records_at_head_and_tail_match() {
        let bytes = break_json(&break_json(&checked(&two_runs()), 0), 17);
        assert_eq!(spans(&bytes)[0].0, 0);
        let records = vec![record_of(&bytes, 0, 1), record_of(&bytes, 17, 2)];
        assert_eq!(records[0].offset, 0);
        let (read, _) = gated(&bytes, LostLineRecords::Read(records));
        let read = read.unwrap();
        assert_eq!(read.lost.len(), 2);
        assert_eq!(read.lost[0].lost.line, 1);
        assert_eq!(read.lost[1].lost.line, 18);
    }

    #[test]
    fn single_root_reads_a_gated_line_without_a_record() {
        let good = checked(&two_runs());
        let bytes = flip_digit(&good, 5);
        let (read, calls) = gated(&bytes, LostLineRecords::SingleRoot);
        let read = read.unwrap();
        assert_eq!(calls, 1);
        assert_eq!(read.lost[0].effect, LostEffect::Marked("A".to_owned()));
        assert_eq!(read.lost[0].lost.why, LostWhy::NotRecordable);
        let note = &read.notes()[0];
        assert!(
            note.contains(
                "is not recordable: a single-root store keeps no copy to restore it from and no \
                 copy ledger to record it"
            ),
            "{note}"
        );
        // Never a stop, for any gated line.
        for index in 0..18 {
            let (read, _) = gated(&break_json(&good, index), LostLineRecords::SingleRoot);
            assert!(read.is_ok(), "{index}");
        }
    }

    /// `bytes` with the member's `,` of line `index` damaged: Unchecked,
    /// no JSON (ADR-068 D6).
    fn damage_member(bytes: &[u8], index: usize) -> Vec<u8> {
        let (_, end) = spans(bytes)[index];
        let mut damaged = bytes.to_vec();
        assert_eq!(damaged[end - CHECK_SHAPE], b',');
        damaged[end - CHECK_SHAPE] ^= 0x01;
        damaged
    }

    #[test]
    fn a_member_damaged_line_mid_run_excludes_its_run() {
        let good = checked(&two_runs());
        // A's stats: between A's warm_ready and A's checkpoint.
        let bytes = damage_member(&good, 4);
        assert_verdicts(&bytes, &[4], LineCheck::Unchecked);
        let (read, calls) = gated(&bytes, none());
        let read = read.unwrap();
        assert_eq!(calls, 0);
        assert_eq!(ids(&read.runs), ["B"]);
        assert_eq!(read.lost[0].lost.why, LostWhy::MemberDamaged);
        assert_eq!(read.lost[0].effect, LostEffect::Excluded("A".to_owned()));
        let note = &read.notes()[0];
        assert!(
            note.contains("damage no crash leaves (ADR-068 D6)"),
            "{note}"
        );
        assert!(note.ends_with("(ADR-068 D6)"), "{note}");
        // The same on a run_end mid-run (A's replication follows it).
        let read = gated(&damage_member(&good, 7), none()).0.unwrap();
        assert_eq!(ids(&read.runs), ["B"]);
    }

    #[test]
    fn a_torn_line_at_a_boundary_reads_as_today() {
        let good = checked(&two_runs());
        // A's replication torn by a crash and ended by the next open (#225).
        let (start, end) = spans(&good)[8];
        let mut bytes = good[..start + (end - start) / 2].to_vec();
        bytes.extend_from_slice(&good[end..]);
        assert_eq!(
            check_line(&bytes[start..spans(&bytes)[8].1]),
            LineCheck::Unchecked
        );
        let (read, calls) = gated(&bytes, none());
        assert_eq!(read.unwrap(), undamaged());
        assert_eq!(calls, 0);
        // Legacy lines never qualify: their neighbours carry no check.
        let legacy = stripped(&good);
        let (start, _) = spans(&legacy)[4];
        let mut bytes = legacy.clone();
        bytes[start] = b'[';
        let read = gated(&bytes, none()).0.unwrap();
        assert!(read.lost.is_empty());
        assert_eq!(ids(&read.runs), ["A", "B"]);
    }

    #[test]
    fn a_damaged_member_at_a_run_boundary_reads_as_today() {
        // The residual (ADR-068 D6): A's run_end is A's last line, and its
        // damaged member cannot be told apart from a torn line: A reads as
        // crashed, with no note.
        let mut lines = two_runs();
        lines.remove(8);
        let good = checked(&lines);
        let bytes = damage_member(&good, 7);
        assert_verdicts(&bytes, &[7], LineCheck::Unchecked);
        let read = gated(&bytes, none()).0.unwrap();
        assert!(read.lost.is_empty() && read.excluded.is_empty());
        assert_eq!(ids(&read.runs), ["A", "B"]);
        assert_eq!(read.runs[0].end, None);
    }

    #[test]
    fn legacy_journal_reads_byte_identically() {
        // Two runs from before ADR-066, with an unparseable line, a line
        // that is not UTF-8 and CRLF line ends.
        let lines = two_runs();
        let mut bytes = Vec::new();
        for (i, line) in lines.iter().enumerate() {
            bytes.extend_from_slice(line.as_bytes());
            bytes.extend_from_slice(if i % 2 == 0 { b"\r\n" } else { b"\n" });
            if i == 4 {
                bytes.extend_from_slice(b"{\"type\":\"run_end\",\"run_id\":\"A\",\"at\n");
                bytes.extend_from_slice(b"{\"type\":\"run_start\",\"x\":\"\xFF\"}\n");
            }
        }
        let (read, calls) = gated(&bytes, none());
        let read = read.unwrap();
        assert_eq!(calls, 0);
        assert!(read.lost.is_empty() && read.excluded.is_empty());
        // The pinned read: what the checked journal reads, line for line.
        assert_eq!(read, undamaged());
        let a = &read.runs[0];
        assert_eq!(a.live_run().clean_records, Some(3));
        assert_eq!(a.checkpoints, [checkpoint(3, true)]);
        assert_eq!(a.warm_start, Some(WarmStart::Warm(handoff())));
        assert_eq!(a.ready_after_ms, Some(25));
        assert_eq!(a.chain_restarts, [EventTime::from_millis(1_007)]);
        assert!(a.lost.is_empty());
    }

    /// The single-bit flips of every byte of [`two_runs`]'s journal: each
    /// either stops at the flipped line (or the line a flipped `\n` merged
    /// it into), or reads every run as written or excluded and named by a
    /// D6 line, and never panics. Returns the flipped journals that stop,
    /// with the flipped byte and the index of the line they name, and the
    /// count of D6 exclusions.
    fn all_flips() -> (Vec<(Vec<u8>, usize, usize)>, usize) {
        let good = checked(&two_runs());
        let whole = undamaged();
        let mut stops = Vec::new();
        let mut member = 0;
        for at in 0..good.len() {
            for bit in 0..8 {
                let mut bytes = good.clone();
                bytes[at] ^= 1 << bit;
                let line = spans(&good)
                    .iter()
                    .position(|(s, e)| (*s..=*e).contains(&at))
                    .unwrap();
                match gated(&bytes, none()).0 {
                    Err(error) => {
                        let (start, _) = spans(&bytes)[line];
                        assert!(
                            error.contains(&format!("line {} at byte {start} fails", line + 1)),
                            "{at}/{bit}: {error}"
                        );
                        stops.push((bytes, at, line));
                    }
                    Ok(read) => {
                        for run in &whole.runs {
                            let id = &run.parameters.run_id;
                            if let Some(excluded) = read.excluded_run(id) {
                                assert_eq!(excluded.by.why, LostWhy::MemberDamaged);
                                assert!(
                                    read.notes()
                                        .iter()
                                        .any(|n| n.contains(&format!("run {id}"))),
                                    "{at}/{bit}"
                                );
                                member += 1;
                            } else {
                                let read = read.runs.iter().find(|r| r.parameters.run_id == *id);
                                assert_eq!(read, Some(run), "{at}/{bit}");
                            }
                        }
                    }
                }
            }
        }
        (stops, member)
    }

    #[test]
    fn every_single_bit_flip_stops_or_reads_as_written() {
        let good = checked(&two_runs());
        let (stops, member) = all_flips();
        // Every flip before the member of a line of a consumed kind fails
        // that line's check (ADR-066) and stops the read, whatever it does
        // to the line's kind.
        for (index, (start, end)) in spans(&good).into_iter().enumerate() {
            let value: Value = serde_json::from_slice(&good[start..end]).unwrap();
            if !GATED_KINDS.contains(&value["type"].as_str().unwrap()) {
                continue;
            }
            for at in start..end - CHECK_SHAPE {
                let stopped = stops
                    .iter()
                    .filter(|(_, a, l)| *a == at && *l == index)
                    .count();
                assert_eq!(stopped, 8, "line {index} byte {at}");
            }
        }
        assert_eq!(member, MEMBER_DAMAGE_EXCLUSIONS, "{member}");
    }

    /// The D6 exclusions among the single-bit flips of [`two_runs`]'s
    /// journal: flips in the member of one of the 14 lines between two
    /// Intact lines of its run (lines 2–8 of each run) that leave the line
    /// Unchecked and no JSON, 79.3 a line (ADR-068 D6, its count; the
    /// per-line terms are [`member_damage_bit_positions`]').
    const MEMBER_DAMAGE_EXCLUSIONS: usize = 1_110;

    /// The single-bit flips of a line's member that leave the line
    /// Unchecked and no JSON (ADR-068 D6): 65 of the 104 bits of its 13
    /// fixed bytes (`,"crc32c":"` and `"}`), and per hex digit 2 for `0`–`9`
    /// and `b`, 3 for `2`, 1 for `a` and `c`–`f`. So 73–89 a line, 79 for
    /// uniform digits (65 + 8 × 28 / 16).
    #[test]
    fn member_damage_bit_positions() {
        let per_digit = |digit: u8| match digit {
            b'2' => 3,
            b'0'..=b'9' | b'b' => 2,
            _ => 1,
        };
        let damaged = |body: &[u8], at: usize, bit: u8| {
            let mut bytes = body.to_vec();
            bytes[at] ^= 1 << bit;
            check_line(&bytes) == LineCheck::Unchecked
                && !serde_json::from_slice::<Value>(&bytes).is_ok_and(|v| v.is_object())
        };
        let mut seen = BTreeSet::new();
        let mut total = 0;
        for line in two_runs()
            .iter()
            .map(String::as_bytes)
            .chain([&b"{\"n\":222}"[..], b"{\"n\":5}"])
        {
            let body = with_check(line);
            let member = body.len() - CHECK_SHAPE;
            let hex = member + CHECK_PREFIX.len()..body.len() - 2;
            let mut fixed = 0;
            let mut digits = 0;
            for at in member..body.len() {
                let count = (0..8).filter(|bit| damaged(&body, at, *bit)).count();
                if hex.contains(&at) {
                    assert_eq!(count, per_digit(body[at]), "{}", body[at] as char);
                    seen.insert(body[at]);
                    digits += count;
                } else {
                    fixed += count;
                }
            }
            assert_eq!(fixed, 65);
            assert!((73..=89).contains(&(fixed + digits)));
            total += fixed + digits;
        }
        assert!(seen.contains(&b'2') && seen.len() >= 10, "{seen:?}");
        assert!(total > 0);
    }

    /// ADR-068 D1: why the parsed `type` of a failed line can be trusted.
    /// Between a kind [`read_runs`] consumes and one it ignores, names of
    /// equal length differ by at least 13 bits, in 6 bytes (`run_end` and
    /// `removal`); for [`warm_handoffs`], by 9 bits in 4 bytes
    /// (`warm_start` and `warm_ready`). Damage in place inserts no byte, so
    /// names of other lengths are further apart.
    #[test]
    fn kind_names_are_far_apart_in_bits() {
        fn nearest(consumed: &[&'static str]) -> (u32, usize, &'static str, &'static str) {
            let mut pairs = Vec::new();
            for a in consumed {
                for b in KNOWN_KINDS.iter().filter(|b| !consumed.contains(b)) {
                    if a.len() != b.len() {
                        continue;
                    }
                    let (bits, bytes) = a.bytes().zip(b.bytes()).fold((0, 0), |(n, m), (x, y)| {
                        (n + (x ^ y).count_ones(), m + usize::from(x != y))
                    });
                    pairs.push((bits, bytes, *a, *b));
                }
            }
            pairs.sort_unstable();
            pairs[0]
        }
        assert_eq!(nearest(&GATED_KINDS), (13, 6, "run_end", "removal"));
        assert_eq!(nearest(&HANDOFF_KINDS), (9, 4, "warm_start", "warm_ready"));
        // A run's openers (the run boundary of D4): `run_start` is 23 bits
        // from its nearest name of equal length, and no other kind has
        // `recovery`'s length.
        assert_eq!(nearest(&["run_start"]).0, 23);
        assert!(KNOWN_KINDS.iter().all(|k| *k == "recovery" || k.len() != 8));
    }

    #[test]
    fn every_recorded_flip_reads_by_the_rule() {
        let whole = undamaged();
        let (stops, _) = all_flips();
        for (bytes, _, line) in stops {
            let record = record_of(&bytes, line, 1);
            let read = gated(&bytes, LostLineRecords::Read(vec![record]))
                .0
                .unwrap_or_else(|e| panic!("line {line}: {e}"));
            assert_eq!(read.lost.len(), 1, "line {line}");
            for run in &whole.runs {
                let id = &run.parameters.run_id;
                match read.runs.iter().find(|r| r.parameters.run_id == *id) {
                    None => assert!(read.excluded_run(id).is_some(), "line {line}"),
                    Some(got) if got.lost.is_empty() => assert_eq!(got, run, "line {line}"),
                    Some(got) => {
                        assert_eq!(read.lost[0].effect, LostEffect::Marked(id.clone()));
                        assert_eq!(got.parameters, run.parameters, "line {line}");
                    }
                }
            }
        }
    }
}
