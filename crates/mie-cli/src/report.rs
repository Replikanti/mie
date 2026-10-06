//! `mie capture-report`: verifies a capture window against its journal.
//!
//! The runs of the window are the journal's runs whose `run_start` falls in
//! `[from, to)`. Their sealed raw files are read through the store, so every
//! read is verified against its manifest, and records are grouped by run
//! (the `session_id` prefix) in `receive_seq` order. Checks:
//!
//! 1. **Trade-id continuity**: within each run, every aggregate-trade id
//!    discontinuity lies inside a journaled `SequenceBreak` or
//!    `Disconnected` gap on `Trades` (`start <= previous T`,
//!    `end >= next T`).
//! 2. **Disconnects are gaps**: every session change of a stream within a
//!    run — a disconnect or rotation followed by a resume — is covered by a
//!    journaled `Disconnected` gap on its series, unless no event of that
//!    stream followed in the run (a reconnect right before shutdown).
//! 3. **Clean shutdown**: every run has a `run_end` with exit code 0, and no
//!    `.tmp` file is left in the source's directory.
//! 4. **Journal totals**: no normalize errors, no domain rejections, no
//!    time fallbacks, and every journal line parses.
//!
//! It prints per-stream rows, then one line per failed check and a final
//! `PASS` or `FAIL`.

use crate::config::IngestConfig;
use mie_adapter_binance::BinanceStream;
use mie_adapter_binance::normalize::parse;
use mie_adapter_parquet::ParquetRawStore;
use mie_domain::event::MarketEvent;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawRecordSource, RawSelection, RawStreamKey};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;

/// One raw record, reduced to what the checks need.
struct Row {
    seq: u64,
    session: String,
    time: i64,
    /// The record normalizes to a domain event.
    has_event: bool,
    trade_id: Option<u64>,
}

/// A journaled gap.
struct Gap {
    stream: String,
    start: i64,
    end: i64,
    reason: String,
}

/// What the journal says about one run.
#[derive(Default)]
struct RunJournal {
    started: bool,
    end: Option<Value>,
    gaps: Vec<Gap>,
    normalize_errors: u64,
    domain_rejections: u64,
    disconnects: u64,
}

/// Runs the report and writes it to `out`; returns whether it passed.
///
/// # Errors
///
/// A description of a failure to read the journal or the store, or to
/// write the report.
pub fn run(
    config: &IngestConfig,
    from_ms: i64,
    to_ms: i64,
    out: &mut dyn Write,
) -> Result<bool, String> {
    let io = |e: std::io::Error| e.to_string();
    let mut problems = Vec::new();

    let (runs, unparsable) = read_journal(&config.paths.journal, from_ms, to_ms)?;
    if unparsable > 0 {
        problems.push(format!("journal: {unparsable} unparsable lines"));
    }
    if runs.is_empty() {
        problems.push(format!("journal: no run started in [{from_ms}, {to_ms})"));
    }

    let streams = config.streams().map_err(|e| e.to_string())?;
    let store = ParquetRawStore::new(&config.paths.raw_root);
    writeln!(
        out,
        "capture report [{from_ms}, {to_ms}) — {} run(s)",
        runs.len()
    )
    .map_err(io)?;
    writeln!(
        out,
        "{:<14} {:>6} {:>10} {:>15} {:>15} {:>8} {:>8}",
        "stream", "files", "records", "first_ms", "last_ms", "sessions", "changes"
    )
    .map_err(io)?;

    for &stream in &streams {
        let (files, rows) = read_stream(&store, config, stream, from_ms, to_ms)?;
        let mut by_run: BTreeMap<String, Vec<Row>> = BTreeMap::new();
        for row in rows {
            let run = row.session.split('/').next().unwrap_or_default().to_owned();
            by_run.entry(run).or_default().push(row);
        }
        let mut records = 0;
        let mut sessions = BTreeSet::new();
        let mut changes = 0;
        let (mut first, mut last) = (i64::MAX, i64::MIN);
        for (run, rows) in &mut by_run {
            rows.sort_by_key(|r| r.seq);
            records += rows.len();
            for row in rows.iter() {
                sessions.insert(row.session.clone());
                first = first.min(row.time);
                last = last.max(row.time);
            }
            let Some(journal) = runs.get(run) else {
                problems.push(format!(
                    "{}: {} records of run {run}, which did not start in the window",
                    stream.raw_name(),
                    rows.len()
                ));
                continue;
            };
            changes += check_sessions(stream, run, rows, journal, &mut problems);
            if stream == BinanceStream::AggTrade {
                check_trade_ids(run, rows, journal, &mut problems);
            }
        }
        let range = |t: i64, empty: bool| {
            if empty { "-".to_owned() } else { t.to_string() }
        };
        writeln!(
            out,
            "{:<14} {:>6} {:>10} {:>15} {:>15} {:>8} {:>8}",
            stream.raw_name(),
            files,
            records,
            range(first, records == 0),
            range(last, records == 0),
            sessions.len(),
            changes
        )
        .map_err(io)?;
    }

    for (run, journal) in &runs {
        writeln!(
            out,
            "run {run}: {} gap(s), {} disconnect(s) or rotation(s)",
            journal.gaps.len(),
            journal.disconnects
        )
        .map_err(io)?;
        check_run_end(run, journal, &mut problems);
    }
    let source_dir = config
        .paths
        .raw_root
        .join(format!("source={}", config.instrument.source));
    let mut temps = Vec::new();
    find_temp_files(&source_dir, &mut temps)?;
    if !temps.is_empty() {
        problems.push(format!(
            "{} temp file(s) left after shutdown, e.g. {}",
            temps.len(),
            temps[0]
        ));
    }

    for problem in &problems {
        writeln!(out, "FAIL: {problem}").map_err(io)?;
    }
    let pass = problems.is_empty();
    writeln!(
        out,
        "{}",
        if pass {
            "PASS".to_owned()
        } else {
            format!("FAIL ({} problem(s))", problems.len())
        }
    )
    .map_err(io)?;
    Ok(pass)
}

/// The runs that started in the window, by run id, and the number of
/// unparsable lines.
fn read_journal(
    path: &Path,
    from_ms: i64,
    to_ms: i64,
) -> Result<(BTreeMap<String, RunJournal>, u64), String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read journal {}: {e}", path.display()))?;
    let mut runs: BTreeMap<String, RunJournal> = BTreeMap::new();
    let mut unparsable = 0;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            unparsable += 1;
            continue;
        };
        let (Some(kind), Some(run)) = (
            value["type"].as_str(),
            value["run_id"].as_str().map(str::to_owned),
        ) else {
            unparsable += 1;
            continue;
        };
        let at = value["at_ms"].as_i64().unwrap_or(i64::MIN);
        if kind == "run_start" && (from_ms..to_ms).contains(&at) {
            runs.entry(run.clone()).or_default().started = true;
        }
        let entry = runs.entry(run).or_default();
        match kind {
            "gap" => entry.gaps.push(Gap {
                stream: value["stream"].as_str().unwrap_or_default().to_owned(),
                start: value["start"].as_i64().unwrap_or(i64::MAX),
                end: value["end"].as_i64().unwrap_or(i64::MIN),
                reason: value["reason"].as_str().unwrap_or_default().to_owned(),
            }),
            "normalize_error" => entry.normalize_errors += 1,
            "domain_rejection" => entry.domain_rejections += 1,
            "disconnected" | "planned_rotation" => entry.disconnects += 1,
            "run_end" => entry.end = Some(value),
            _ => {}
        }
    }
    runs.retain(|_, r| r.started);
    Ok((runs, unparsable))
}

/// The number of sealed files and the rows of `stream` in the window.
fn read_stream(
    store: &ParquetRawStore,
    config: &IngestConfig,
    stream: BinanceStream,
    from_ms: i64,
    to_ms: i64,
) -> Result<(usize, Vec<Row>), String> {
    let key = RawStreamKey::new(
        &config.instrument.source,
        &config.instrument.symbol,
        stream.raw_name(),
    )
    .map_err(|e| e.to_string())?;
    let window = ReplayWindow {
        start: EventTime::from_millis(from_ms),
        end: EventTime::from_millis(to_ms),
    };
    let selection = RawSelection::new(BTreeSet::from([key]), window).map_err(|e| e.to_string())?;
    let dataset = store.select(&selection).map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    for file in &dataset.files {
        for record in store.read(file).map_err(|e| e.to_string())? {
            if !window.contains(record.event_time) {
                continue;
            }
            let Some(capture) = record.capture else {
                continue;
            };
            let event = parse(stream, &config.instrument.symbol, &record.payload)
                .ok()
                .flatten();
            let trade_id = match &event {
                Some(MarketEvent::Trade(trade)) => Some(trade.trade_id),
                _ => None,
            };
            rows.push(Row {
                seq: capture.receive_seq,
                session: capture.session_id,
                time: record.event_time.as_millis(),
                has_event: event.is_some(),
                trade_id,
            });
        }
    }
    Ok((dataset.files.len(), rows))
}

/// Check 2; returns the number of session changes.
fn check_sessions(
    stream: BinanceStream,
    run: &str,
    rows: &[Row],
    journal: &RunJournal,
    problems: &mut Vec<String>,
) -> usize {
    let series = format!("{:?}", stream.domain_stream());
    let mut changes = 0;
    for (i, pair) in rows.windows(2).enumerate() {
        if pair[0].session == pair[1].session {
            continue;
        }
        changes += 1;
        // The first event of the stream from the resumed session on.
        let Some(resumed) = rows[i + 1..].iter().find(|r| r.has_event) else {
            continue;
        };
        let covered = journal.gaps.iter().any(|g| {
            g.stream == series
                && g.reason == "Disconnected"
                && g.start <= resumed.time
                && g.end >= resumed.time
        });
        if !covered {
            problems.push(format!(
                "{} run {run}: session change {} -> {} (resumed at {} ms) has no Disconnected gap",
                stream.raw_name(),
                pair[0].session,
                pair[1].session,
                resumed.time
            ));
        }
    }
    changes
}

/// Check 1.
fn check_trade_ids(run: &str, rows: &[Row], journal: &RunJournal, problems: &mut Vec<String>) {
    let mut last: Option<(u64, i64)> = None;
    for row in rows {
        let Some(id) = row.trade_id else {
            continue;
        };
        match last {
            Some((last_id, _)) if id <= last_id => continue,
            Some((last_id, last_time)) if Some(id) != last_id.checked_add(1) => {
                let covered = journal.gaps.iter().any(|g| {
                    g.stream == "Trades"
                        && (g.reason == "SequenceBreak" || g.reason == "Disconnected")
                        && g.start <= last_time
                        && g.end >= row.time
                });
                if !covered {
                    problems.push(format!(
                        "aggTrade run {run}: ids {last_id} -> {id} ({last_time} -> {} ms) without a journaled gap",
                        row.time
                    ));
                }
            }
            _ => {}
        }
        last = Some((id, row.time));
    }
}

/// Checks 3 and 4 for one run.
fn check_run_end(run: &str, journal: &RunJournal, problems: &mut Vec<String>) {
    let Some(end) = &journal.end else {
        problems.push(format!("run {run}: no run_end (not a clean shutdown)"));
        return;
    };
    let field = |name: &str| end[name].as_u64().unwrap_or(0);
    if end["exit_code"].as_i64() != Some(0) {
        problems.push(format!(
            "run {run}: exit code {} ({})",
            end["exit_code"],
            end["error"].as_str().unwrap_or("no error recorded")
        ));
    }
    let normalize_errors = journal.normalize_errors.max(field("normalize_errors"));
    if normalize_errors > 0 {
        problems.push(format!("run {run}: {normalize_errors} normalize error(s)"));
    }
    let rejections = journal.domain_rejections.max(field("domain_rejections"));
    if rejections > 0 {
        problems.push(format!("run {run}: {rejections} domain rejection(s)"));
    }
    if field("time_fallbacks") > 0 {
        problems.push(format!(
            "run {run}: {} time fallback(s)",
            field("time_fallbacks")
        ));
    }
}

/// Collects files ending in `.tmp` below `dir`.
fn find_temp_files(dir: &Path, out: &mut Vec<String>) -> Result<(), String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("list {}: {e}", dir.display())),
    };
    for entry in entries {
        let entry = entry.map_err(|e| format!("list {}: {e}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            find_temp_files(&path, out)?;
        } else if entry.file_name().to_string_lossy().ends_with(".tmp") {
            out.push(path.display().to_string());
        }
    }
    Ok(())
}
