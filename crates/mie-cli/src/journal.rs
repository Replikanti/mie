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
//! the writer drops a caller field of that name, and no line type nests
//! one. The check guards against accidental damage only: it is no MAC.
//! Lines written before ADR-066 never gain it. Readers ignore the member:
//! [`read_runs`] and every other reader index named keys.
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

use crate::ingest::tiers::Recalled;
use crate::store::PassReport;
use mie_adapter_binance::live::ChannelStats;
use mie_adapter_binance::{
    BinanceStream, BookStats, BookTransition, CaptureEvent, CaptureObserver, CheckpointResult,
    LiveRun, MIN_SIDE_LEVELS, PipelineStats, SideCoverage, SnapshotTrigger,
};
use mie_adapter_parquet::copy_ledger::Truncation;
use mie_adapter_parquet::removal::{RemovalPlan, RemovalReport};
use mie_adapter_parquet::replica::{self, JournalLock};
use mie_app::equivalence::StateCheckpoint;
use mie_domain::book::{Invalidation, Side};
use mie_domain::event_hash::EventStreamHash;
use mie_domain::feature::FeatureSetVersion;
use mie_domain::fingerprint::Fingerprint;
use mie_domain::state_hash::StateHash;
use mie_domain::time::EventTime;
use mie_ports::raw::{DatasetVersion, SealedFile};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
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

/// Every warm run's handoff in the journal at `path`, as (run id,
/// handoff), in journal order (ADR-062 D11, the replace's cost, #211
/// RQ5). The journal is streamed line by line with bounded memory; only
/// `warm_start` lines with `warm: true` are parsed, and a line that is no
/// JSON or no complete warm handoff is skipped, as `read_runs` skips it.
/// `None` when the journal does not exist.
///
/// # Errors
///
/// The I/O error when the journal exists and cannot be read.
pub fn warm_handoffs(path: &Path) -> std::io::Result<Option<Vec<(String, WarmHandoff)>>> {
    use std::io::BufRead;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut reader = std::io::BufReader::new(file);
    let mut runs = Vec::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let Ok(value) = serde_json::from_slice::<Value>(&line) else {
            continue;
        };
        if value["type"].as_str() != Some("warm_start") {
            continue;
        }
        if let Ok(WarmStart::Warm(handoff)) = warm_start_from_json(&value) {
            let run_id = value["run_id"].as_str().unwrap_or("?").to_owned();
            runs.push((run_id, handoff));
        }
    }
    Ok(Some(runs))
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

/// Reads every run of `source` from the journal at `path`, in journal
/// order.
///
/// A line that is not JSON is skipped: a crash can leave a partial line.
/// The next [`Journal::open`] ends it with a `\n` (#225), so the next run's
/// first line follows it on a line of its own; journals written before
/// that may hold the next run's first line glued onto it (#217). A line
/// that is not valid UTF-8 (a crash can tear a multibyte character) is
/// skipped the same way: the journal is read as bytes, and only the lines
/// are decoded. A line's check (`crc32c`, ADR-066) is ignored: only named
/// keys are read, so a line reads the same with the member or without it
/// (written before ADR-066), and a line whose check fails reads like any
/// other line (no gate here, ADR-066 D7). A `run_start`, `run_end`,
/// `state_checkpoint`, `warm_start`, `warm_ready` or `chain_restart` that
/// does not carry its fields is an error, and so are two `run_start`s of
/// one run id — a restart within the same second, whose records the session
/// prefix could not tell apart (ADR-032) — a second `run_end`, `warm_start`
/// or `warm_ready`, and any of these lines without a `run_start`.
///
/// # Errors
///
/// A description naming the line.
pub fn read_runs(path: &Path, source: &str) -> Result<Vec<JournaledRun>, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read journal {}: {e}", path.display()))?;
    let mut runs: Vec<JournaledRun> = Vec::new();
    // `serde_json` rejects invalid UTF-8 and takes a `\r` before the `\n`
    // as whitespace, so this splits as `str::lines` did.
    for (index, line) in bytes.split(|b| *b == b'\n').enumerate() {
        let at = |detail: String| format!("journal line {}: {detail}", index + 1);
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        match value["type"].as_str() {
            Some("run_start") => {
                let parameters = RunParameters::from_run_start(&value).map_err(at)?;
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
                let checkpointing = Checkpointing::from_run_start(&value).map_err(at)?;
                let history = RunHistory::from_run_start(&value).map_err(at)?;
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
                            .push(state_checkpoint_from_json(&value).map_err(at)?);
                    }
                    "warm_start" => {
                        let start = warm_start_from_json(&value).map_err(at)?;
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
    }
    runs.retain(|run| run.source == source);
    Ok(runs)
}

/// The name of the line check's member, always a line's last (ADR-066 D1).
pub const CHECK_FIELD: &str = "crc32c";

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
    /// [`CHECK_FIELD`] is dropped: the name is the check's.
    pub fn write(&mut self, at_utc_ns: i64, kind: &str, fields: Value) {
        let mut line = Map::new();
        line.insert("type".to_owned(), json!(kind));
        line.insert("run_id".to_owned(), json!(self.run_id));
        line.insert("at_ms".to_owned(), json!(at_utc_ns.div_euclid(1_000_000)));
        if let Value::Object(fields) = fields {
            line.extend(fields);
        }
        line.remove(CHECK_FIELD);
        let result = serde_json::to_vec(&Value::Object(line))
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
        let runs = read_runs(&path, "binance-um");
        std::fs::remove_file(&path).unwrap();
        runs
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
        read_runs(&path, "binance-um")
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
        let runs = read_runs(&path, "binance-um").unwrap();
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
        let runs = read_runs(&path, "binance-um").unwrap();
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
        assert_eq!(ids(&read_runs(&path, "binance-um").unwrap()), ["B"]);
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
        let runs = read_runs(&path, "binance-um").unwrap();
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
        assert_eq!(ids(&read_runs(&path, "binance-um").unwrap()), ["A"]);
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
        let runs = read_runs(&path, "binance-um").unwrap();
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
        let with_checks = read_runs(&path, "binance-um").unwrap();
        assert_eq!(ids(&with_checks), ["A", "B"]);
        let plain = read_bytes(&stripped(&mixed)).unwrap();
        assert!(!stripped(&mixed).windows(6).any(|w| w == b"crc32c"));
        assert_eq!(with_checks, plain);
        assert_eq!(with_checks[1].live_run().clean_records, Some(7));
    }
}
