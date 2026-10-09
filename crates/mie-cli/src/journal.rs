//! The capture journal: one JSON object per line, appended.
//!
//! Every line carries `type`, `run_id` and `at_ms` (wall clock, for humans
//! and the soak report; it orders nothing). Line types:
//!
//! | `type` | Written by | Content |
//! |---|---|---|
//! | `run_start` | ingest | symbol, streams, hold-back, open-interest re-time allowance, seeds per stream: the run's pipeline parameters ([`RunParameters`]); depth snapshot limit and checkpoint interval (informational); the state checkpoint interval (`state_checkpoint_interval_ms`), the state and event-stream hash encodings and the engine's feature set (16 hex digits): the comparability keys of the equivalence harness ([`Checkpointing`], ADR-041) |
//! | `recovery` | ingest | parts rolled forward and discarded by the store |
//! | `connected`, `connect_failed`, `disconnected`, `planned_rotation`, `backoff` | capture | connection lifecycle per stream |
//! | `oi_poll` | capture | request/response time (ns), status, whether persisted |
//! | `depth_snapshot_fetch` | capture | `trigger` (sync, checkpoint), request/response time (ns), status, whether persisted |
//! | `book` | capture | an order-book sync change: `event` = desync, sync, snapshot_rejected, checkpoint_emitted, checkpoint_skipped; its cause, reason or id; the `receive_seq` of the record that caused it |
//! | `book_checkpoint` | capture | the audit of a checkpoint: `result` = matched, mismatched, unverifiable, invalidated, with levels compared, window in bps or mismatch examples |
//! | `gap` | capture | `stream`, `start`, `end` (ms), `reason`, as delivered to the core |
//! | `sealed` | capture | the files sealed |
//! | `normalize_error` | capture | stream, `receive_seq`, error |
//! | `stats` | capture | counters (the order-book sync counters under `streams.depth.book`), channel high-water marks and blocked time |
//! | `domain_rejection` | ingest | an event the engine rejected |
//! | `state_checkpoint` | ingest | `ordinal` (events delivered so far, rejections included), `as_of` (ms or null), `event_hash` and `state_hash` (16 hex digits), `last`: a checkpoint of the core (ADR-041) |
//! | `run_end` | ingest | totals (`events`, `domain_rejections`, `records`) and the exit code |
//!
//! The journal is flushed on every `stats` line and at the end of a run, and
//! synced to stable storage right after `run_start` and at the end of a run:
//! replay needs a run's `run_start` for every record the run seals
//! (ADR-039 D2).
//!
//! The raw store alone does not determine what live delivered: the
//! pipeline's `hold_back_ms`, `oi_retime_ms` and per-stream seeds are run
//! parameters, kept only in `run_start`. A recompute (replay #11, equivalence harness #13)
//! reads them with [`RunParameters::from_run_start`], never from
//! configuration defaults (ADR-032). [`read_runs`] joins every run's
//! `run_start` with its `run_end` into the [`LiveRun`]s `mie replay`
//! recomputes (ADR-039 D1), and collects the run's `state_checkpoint`
//! lines for `mie equivalence` (ADR-041).

use mie_adapter_binance::live::ChannelStats;
use mie_adapter_binance::{
    BinanceStream, BookStats, BookTransition, CaptureEvent, CaptureObserver, CheckpointResult,
    LiveRun, PipelineStats, SnapshotTrigger,
};
use mie_app::equivalence::StateCheckpoint;
use mie_domain::book::{Invalidation, Side};
use mie_domain::event_hash::EventStreamHash;
use mie_domain::feature::FeatureSetVersion;
use mie_domain::fingerprint::Fingerprint;
use mie_domain::state_hash::StateHash;
use mie_domain::time::EventTime;
use mie_ports::raw::SealedFile;
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
        let mut seeds = BTreeMap::new();
        let journaled = line["seeds"].as_object().ok_or("run_start without seeds")?;
        for (name, millis) in journaled {
            let stream = BinanceStream::from_raw_name(name)
                .ok_or_else(|| format!("run_start seed of unknown stream {name:?}"))?;
            let millis = millis
                .as_i64()
                .ok_or_else(|| format!("run_start seed of {name} is not an integer"))?;
            seeds.insert(stream, EventTime::from_millis(millis));
        }
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
    /// carries the record count the replay checks.
    pub fn live_run(&self) -> LiveRun {
        LiveRun {
            run_id: self.parameters.run_id.clone(),
            symbol: self.parameters.symbol.clone(),
            hold_back_ms: self.parameters.hold_back_ms,
            oi_retime_ms: self.parameters.oi_retime_ms,
            seeds: self.parameters.seeds.clone(),
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
/// A line that is not JSON is skipped: a crash can leave a partial line,
/// which the next run's first line then follows. A `run_start`, `run_end`
/// or `state_checkpoint` that does not carry its fields is an error, and so
/// is one without its run's `run_start`, and so are two `run_start`s
/// of one run id — a restart within the same second, whose records the
/// session prefix could not tell apart (ADR-032) — a second `run_end`, and a
/// `run_end` without a `run_start`.
///
/// # Errors
///
/// A description naming the line.
pub fn read_runs(path: &Path, source: &str) -> Result<Vec<JournaledRun>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read journal {}: {e}", path.display()))?;
    let mut runs: Vec<JournaledRun> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let at = |detail: String| format!("journal line {}: {detail}", index + 1);
        let Ok(value) = serde_json::from_str::<Value>(line) else {
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
                runs.push(JournaledRun {
                    parameters,
                    source: run_source.to_owned(),
                    streams,
                    started_at_ms,
                    end: None,
                    checkpointing,
                    checkpoints: Vec::new(),
                });
            }
            Some("state_checkpoint") => {
                let checkpoint = state_checkpoint_from_json(&value).map_err(at)?;
                let run_id = value["run_id"].as_str().unwrap_or_default();
                let run = runs
                    .iter_mut()
                    .find(|r| r.parameters.run_id == run_id)
                    .ok_or_else(|| {
                        at(format!(
                            "state_checkpoint of run {run_id:?} without run_start"
                        ))
                    })?;
                run.checkpoints.push(checkpoint);
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

/// An open journal for one run.
#[derive(Debug)]
pub struct Journal {
    out: BufWriter<File>,
    run_id: String,
    write_error: Option<String>,
}

impl Journal {
    /// Opens `path` for appending, creating it and its directory.
    ///
    /// # Errors
    ///
    /// A description of the I/O failure.
    pub fn open(path: &Path, run_id: &str) -> Result<Self, String> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("open journal {}: {e}", path.display()))?;
        Ok(Self {
            out: BufWriter::new(file),
            run_id: run_id.to_owned(),
            write_error: None,
        })
    }

    /// Appends one line of `kind` with `fields`, stamped at `at_utc_ns`.
    pub fn write(&mut self, at_utc_ns: i64, kind: &str, fields: Value) {
        let mut line = Map::new();
        line.insert("type".to_owned(), json!(kind));
        line.insert("run_id".to_owned(), json!(self.run_id));
        line.insert("at_ms".to_owned(), json!(at_utc_ns.div_euclid(1_000_000)));
        if let Value::Object(fields) = fields {
            line.extend(fields);
        }
        let result = serde_json::to_writer(&mut self.out, &Value::Object(line))
            .map_err(|e| e.to_string())
            .and_then(|()| self.out.write_all(b"\n").map_err(|e| e.to_string()));
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
        CheckpointResult::Matched {
            levels,
            window_bid_bps,
            window_ask_bps,
        } => json!({
            "result": "matched",
            "levels": levels,
            "window_bid_bps": window_bid_bps,
            "window_ask_bps": window_ask_bps,
        }),
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

fn side(side: Side) -> &'static str {
    match side {
        Side::Bid => "bid",
        Side::Ask => "ask",
    }
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
}
