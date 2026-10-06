//! The capture journal: one JSON object per line, appended.
//!
//! Every line carries `type`, `run_id` and `at_ms` (wall clock, for humans
//! and the soak report; it orders nothing). Line types:
//!
//! | `type` | Written by | Content |
//! |---|---|---|
//! | `run_start` | ingest | symbol, streams, hold-back, open-interest re-time allowance, seeds per stream: the run's pipeline parameters ([`RunParameters`]) |
//! | `recovery` | ingest | parts rolled forward and discarded by the store |
//! | `connected`, `connect_failed`, `disconnected`, `planned_rotation`, `backoff` | capture | connection lifecycle per stream |
//! | `oi_poll` | capture | request/response time (ns), status, whether persisted |
//! | `gap` | capture | `stream`, `start`, `end` (ms), `reason`, as delivered to the core |
//! | `sealed` | capture | the files sealed |
//! | `normalize_error` | capture | stream, `receive_seq`, error |
//! | `stats` | capture | counters, channel high-water marks and blocked time |
//! | `domain_rejection` | ingest | an event the engine rejected |
//! | `run_end` | ingest | totals and the exit code |
//!
//! The journal is flushed on every `stats` line and at the end of a run.
//!
//! The raw store alone does not determine what live delivered: the
//! pipeline's `hold_back_ms`, `oi_retime_ms` and per-stream seeds are run
//! parameters, kept only in `run_start`. A recompute (replay #11, equivalence harness #13)
//! reads them with [`RunParameters::from_run_start`], never from
//! configuration defaults (ADR-032).

use mie_adapter_binance::live::ChannelStats;
use mie_adapter_binance::{BinanceStream, CaptureEvent, CaptureObserver, PipelineStats};
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

/// Per-stream pipeline counters as journal JSON.
pub fn pipeline_json(stats: &PipelineStats) -> Value {
    let streams: Map<String, Value> = stats
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
    Value::Object(streams)
}
