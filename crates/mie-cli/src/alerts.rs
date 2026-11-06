//! `mie alerts replay` and `mie alerts report`: the replay dry-run of the
//! informational alerts and the market report (#38, ADR-059, proposed).
//!
//! `replay` drives a replay through
//! [`alert_window`](mie_app::alert::alert_window) — or
//! [`alert_chain_window`](mie_app::alert::alert_chain_window) for the
//! history chain — into an [`AlertLog`] gateway: the decision path live
//! ingestion uses, with a log instead of a chat (ADR-019, ADR-059 D1). The
//! log is JSON Lines, written to a new file once the replay has finished:
//!
//! - a `header`: the log format, the source, the replay window and
//!   `emit_from`, the data version, the engine key, the alert policy and
//!   its fingerprint;
//! - one `message` line per message: its event time, event key, the
//!   alerts' canonical lines and the rendered text;
//! - a `summary`: events, rejections, messages, delivery errors, alerts per
//!   kind, messages per UTC day (p50, p95, max over every UTC day the emit
//!   window touches, partial ones included), and the history chain's
//!   restarts.
//!
//! Keys are sorted and nothing reads the wall clock, so the same inputs
//! give the same bytes. The file must not exist; a failed replay or an
//! empty window writes nothing and exits 1, as `mie replay` does.
//!
//! `report` replays a window through [`ReportService`] and prints the
//! report as of its end, in the text a report message carries.

use crate::journal::read_runs;
use crate::replay::{ReplaySource, archive_replay, history_chain};
use mie_adapter_binance::LiveReplay;
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_app::alert::{AlertReport, ReportService, alert_chain_window, alert_window, render};
use mie_domain::alert::{Alert, AlertKind, AlertProvenance};
use mie_domain::time::EventTime;
use mie_ports::inbound::GenerateMarketReport;
use mie_ports::outbound::{
    AlertError, AlertGateway, AlertMessage, HistoricalDataProvider, ProviderError, ReplayWindow,
};
use serde_json::{Map, Value, json};
use std::io::Write;
use std::path::PathBuf;

/// The first line's format tag.
pub const LOG_FORMAT: &str = "mie-alert-log 1";

const DAY_MS: i64 = 86_400_000;

/// One `mie alerts replay` invocation.
#[derive(Debug, Clone)]
pub struct ReplayRequest {
    /// The source.
    pub source: ReplaySource,
    /// The replay window: `[--replay-from or --from, --to)`.
    pub replay: ReplayWindow,
    /// The first event time whose alerts are emitted: `--from`.
    pub emit_from: EventTime,
    /// The log file to create.
    pub out: PathBuf,
}

/// One `mie alerts report` invocation.
#[derive(Debug, Clone)]
pub struct ReportRequest {
    /// The source.
    pub source: ReplaySource,
    /// The window; the report is the market as of its end.
    pub window: ReplayWindow,
}

/// The dry-run gateway (ADR-059 D1): keeps every message as its log line.
/// It never refuses one.
#[derive(Debug, Default)]
pub struct AlertLog {
    lines: Vec<String>,
    times: Vec<EventTime>,
}

impl AlertLog {
    /// The `message` line of `message`.
    pub fn line(message: &AlertMessage) -> String {
        let alerts: Vec<String> = message.alerts.iter().map(Alert::to_string).collect();
        json!({
            "type": "message",
            "known_at_ms": message.known_at.as_millis(),
            "event": message.event.to_string(),
            "alerts": alerts,
            "text": message.text,
        })
        .to_string()
    }
}

impl AlertGateway for AlertLog {
    fn deliver(&mut self, message: &AlertMessage) -> Result<(), AlertError> {
        self.lines.push(Self::line(message));
        self.times.push(message.known_at);
        Ok(())
    }
}

/// p50, p95 and the maximum of `counts` by nearest rank; zeros for none.
pub fn quantiles(counts: &[u64]) -> (u64, u64, u64) {
    if counts.is_empty() {
        return (0, 0, 0);
    }
    let mut sorted = counts.to_vec();
    sorted.sort_unstable();
    let rank = |q: u64| {
        let n = sorted.len() as u64;
        let index = (q * n).div_ceil(100).max(1) - 1;
        sorted[usize::try_from(index).expect("an index")]
    };
    (rank(50), rank(95), sorted[sorted.len() - 1])
}

/// Messages per UTC day over every day `[from, end)` touches.
fn per_day(times: &[EventTime], from: EventTime, end: EventTime) -> Vec<u64> {
    let first = from.as_millis().div_euclid(DAY_MS);
    let last = (end.as_millis() - 1).div_euclid(DAY_MS);
    let mut counts = vec![0_u64; usize::try_from(last - first + 1).unwrap_or(0)];
    for time in times {
        let day = time.as_millis().div_euclid(DAY_MS) - first;
        if let Some(count) = usize::try_from(day)
            .ok()
            .and_then(|day| counts.get_mut(day))
        {
            *count += 1;
        }
    }
    counts
}

fn header(source: &str, request: &ReplayRequest, provenance: &AlertProvenance) -> String {
    json!({
        "type": "header",
        "format": LOG_FORMAT,
        "source": source,
        "replay": {
            "start_ms": request.replay.start.as_millis(),
            "end_ms": request.replay.end.as_millis(),
        },
        "emit_from_ms": request.emit_from.as_millis(),
        "data": provenance.data.to_string(),
        "engine": provenance.engine.to_string(),
        "policy": provenance.policy.to_string(),
        "policy_fingerprint": provenance.policy_fingerprint.to_string(),
    })
    .to_string()
}

fn summary(request: &ReplayRequest, report: &AlertReport, log: &AlertLog) -> Value {
    let mut kinds = Map::new();
    for (kind, count) in AlertKind::ALL.iter().zip(report.alerts) {
        kinds.insert(kind.as_str().to_owned(), json!(count));
    }
    let days = per_day(&log.times, request.emit_from, request.replay.end);
    let (p50, p95, max) = quantiles(&days);
    let restarts: Vec<i64> = report.restarts.iter().map(|t| t.as_millis()).collect();
    json!({
        "type": "summary",
        "events": report.events,
        "rejections": report.rejections,
        "messages": report.messages,
        "delivery_errors": report.delivery_errors,
        "alerts": kinds,
        "messages_per_day": { "days": days.len(), "p50": p50, "p95": p95, "max": max },
        "restarts_ms": restarts,
    })
}

/// Runs the dry-run, writes the log, and reports to `out`. Returns whether
/// it passed.
///
/// # Errors
///
/// A description when the report cannot be written. Every replay failure
/// is reported in the output.
pub fn replay(request: &ReplayRequest, out: &mut dyn Write) -> Result<bool, String> {
    let (pass, text) = match run_replay(request) {
        Ok(text) => (true, format!("{text}PASS\n")),
        Err(error) => (false, format!("FAIL: {error}\n")),
    };
    out.write_all(text.as_bytes())
        .and_then(|()| out.flush())
        .map_err(|e| format!("write the report: {e}"))?;
    Ok(pass)
}

/// The dry-run: the report's lines before the verdict.
fn run_replay(request: &ReplayRequest) -> Result<String, String> {
    if request.out.exists() {
        return Err(format!(
            "{} exists; an alert log is never overwritten",
            request.out.display()
        ));
    }
    let window = request.replay;
    if window.start >= window.end || !window.contains(request.emit_from) {
        return Err("the emit start must lie inside a non-empty replay window".to_owned());
    }
    let mut log = AlertLog::default();
    let (source, (provenance, report)) = match &request.source {
        ReplaySource::Live(config) => {
            let source = &config.instrument.source;
            let symbol = &config.instrument.symbol;
            let runs = read_runs(&config.paths.journal, source)?;
            let store = crate::store::raw_store(&config.paths.raw_root, &config.paths.cold_roots)?;
            let runs = runs.iter().map(|run| run.live_run()).collect();
            let live = LiveReplay::new(&store, source, symbol, runs);
            let text = format!("live {source}/{symbol}");
            (text, drive(&live, request, false, &mut log)?)
        }
        ReplaySource::Archive {
            config,
            streams,
            correction,
        } => {
            let store = crate::store::raw_store(&config.paths.raw_root, &config.paths.cold_roots)?;
            let archive = archive_replay(&store, config, streams.as_deref(), *correction)?;
            let mut names: Vec<&str> = archive.streams().iter().map(|s| s.raw_name()).collect();
            names.sort_unstable();
            let text = format!(
                "archive {ARCHIVE_SOURCE}/{} streams {} correction {correction}",
                config.instrument.symbol,
                names.join(",")
            );
            (text, drive(&archive, request, false, &mut log)?)
        }
        ReplaySource::History(config) => {
            let store = crate::store::raw_store(&config.paths.raw_root, &config.paths.cold_roots)?;
            let chain = history_chain(config, &store)?;
            (
                "history".to_owned(),
                drive(&chain, request, true, &mut log)?,
            )
        }
    };
    if report.events == 0 {
        return Err("the window holds no event".to_owned());
    }
    let mut bytes = Vec::new();
    for line in std::iter::once(header(&source, request, &provenance))
        .chain(log.lines.iter().cloned())
        .chain(std::iter::once(summary(request, &report, &log).to_string()))
    {
        bytes.extend(line.into_bytes());
        bytes.push(b'\n');
    }
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&request.out)
        .and_then(|mut file| file.write_all(&bytes).and_then(|()| file.sync_all()))
        .map_err(|e| format!("write {}: {e}", request.out.display()))?;
    let (p50, p95, max) = quantiles(&per_day(&log.times, request.emit_from, request.replay.end));
    Ok(format!(
        "alerts {source} {provenance}\n\
         events {} rejections {} messages {} alerts {}\n\
         messages per UTC day p50 {p50} p95 {p95} max {max}\n\
         log {}\n",
        report.events,
        report.rejections,
        report.messages,
        report.alerts.iter().sum::<u64>(),
        request.out.display()
    ))
}

fn drive<H: HistoricalDataProvider>(
    history: &H,
    request: &ReplayRequest,
    chain: bool,
    log: &mut AlertLog,
) -> Result<(AlertProvenance, AlertReport), String> {
    let ran: Result<_, ProviderError> = if chain {
        alert_chain_window(history, request.replay, Some(request.emit_from), log)
    } else {
        alert_window(history, request.replay, Some(request.emit_from), log)
    };
    ran.map_err(|e| e.to_string())
}

/// Prints the market report as of the window's end. Returns whether it
/// passed: the window held an event.
///
/// # Errors
///
/// A description when the report cannot be written.
pub fn report(request: &ReportRequest, out: &mut dyn Write) -> Result<bool, String> {
    let (pass, text) = match run_report(request) {
        Ok(text) => (true, format!("{text}PASS\n")),
        Err(error) => (false, format!("FAIL: {error}\n")),
    };
    out.write_all(text.as_bytes())
        .and_then(|()| out.flush())
        .map_err(|e| format!("write the report: {e}"))?;
    Ok(pass)
}

fn run_report(request: &ReportRequest) -> Result<String, String> {
    let window = request.window;
    let report = match &request.source {
        ReplaySource::Live(config) => {
            let source = &config.instrument.source;
            let runs = read_runs(&config.paths.journal, source)?;
            let store = crate::store::raw_store(&config.paths.raw_root, &config.paths.cold_roots)?;
            let runs = runs.iter().map(|run| run.live_run()).collect();
            let live = LiveReplay::new(&store, source, &config.instrument.symbol, runs);
            ReportService::new(live).report(window)
        }
        ReplaySource::Archive {
            config,
            streams,
            correction,
        } => {
            let store = crate::store::raw_store(&config.paths.raw_root, &config.paths.cold_roots)?;
            let archive = archive_replay(&store, config, streams.as_deref(), *correction)?;
            ReportService::new(archive).report(window)
        }
        ReplaySource::History(config) => {
            let store = crate::store::raw_store(&config.paths.raw_root, &config.paths.cold_roots)?;
            let chain = history_chain(config, &store)?;
            ReportService::chain(chain).report(window)
        }
    }
    .map_err(|e| e.to_string())?;
    if report.as_of.is_none() {
        return Err("the window holds no event".to_owned());
    }
    Ok(render(&[Alert::Report(report)]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_are_nearest_rank() {
        assert_eq!(quantiles(&[]), (0, 0, 0));
        assert_eq!(quantiles(&[7]), (7, 7, 7));
        let counts: Vec<u64> = (1..=20).collect();
        assert_eq!(quantiles(&counts), (10, 19, 20));
        assert_eq!(quantiles(&[3, 1, 2]), (2, 3, 3));
    }

    #[test]
    fn days_cover_every_day_the_window_touches() {
        let t = EventTime::from_millis;
        let times = [t(DAY_MS + 5), t(DAY_MS + 6), t(3 * DAY_MS - 1)];
        assert_eq!(per_day(&times, t(DAY_MS / 2), t(3 * DAY_MS)), vec![0, 2, 1]);
        assert_eq!(per_day(&times, t(DAY_MS), t(DAY_MS + 1)), vec![2]);
    }
}
