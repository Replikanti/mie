//! `mie equivalence` offline (#13, ADR-041): live sessions replayed from
//! the raw store must reach the states live journaled, and a mutation must
//! be caught at the first checkpoint it affects.
//!
//! - (A) A recorded live session (`tests/fixtures/equivalence`, a real
//!   `mie ingest` run) written into a temporary raw store is EQUIVALENT.
//!   Once a later change moves the feature set, its state hashes are no
//!   longer comparable and the test expects EVENTS EQUIVALENT instead; (B)
//!   keeps the state check.
//! - (B) The fixture's payloads played through the offline `mie ingest`
//!   composition in two runs against one store (a restart with seeds) are
//!   EQUIVALENT with full state comparison. Thread interleaving varies
//!   between runs of the test; equivalence holds for every interleaving,
//!   so nothing that depends on it is pinned.
//! - (C) A mutated trade, a flipped journaled state hash and a dropped
//!   record fail.
//! - (D) A run without checkpoints, a run that delivered no event, or a
//!   window without runs, fails.
//! - (E) The same request prints the same bytes.
//! - (F) A run recorded with another feature set compares its events only:
//!   what the engine decides may differ, a mutated trade still diverges as
//!   an event stream.
//! - (G) The order book in the state (ADR-043): the recorded live depth
//!   window (`mie-adapter-binance/tests/fixtures/depth*.jsonl`) through the
//!   offline `mie ingest` composition, checkpoints every second, is
//!   EQUIVALENT with the state compared; a mutated level quantity in one
//!   recorded diff diverges at the first checkpoint after it, and a flipped
//!   state hash as state.
//!
//! Warm-started runs (ADR-052 D9), through the offline `mie ingest`
//! composition with `[history]` over an imported archive (the scripted
//! capture of `common::warm`):
//!
//! - (H) Three warm runs, the second a fast restart 2 s after the first,
//!   whose overlapping trade becomes a `LateEvent` gap inside its compared
//!   part, the third an hour later: all `EQUIVALENT` with state compared on
//!   one pass; each prefix contains the predecessor; a second invocation
//!   prints the same bytes; the third run alone is `EQUIVALENT` too.
//! - (I) Two warm runs an hour apart: a changed archive trade makes both
//!   `DIVERGED (warm prefix)`; a changed trade of the first run diverges it
//!   at a checkpoint and the second at its prefix; a flipped journaled
//!   handoff state hash diverges the second as state, with the dataset
//!   version unchanged.
//! - (J) A cold chain run (failed warm-up) before a warm one: the first is
//!   compared in the cold unit, the second's prefix crosses the first's
//!   chain recompute; both `EQUIVALENT`.
//! - (K) The runs of (I) without `[history]` or under another cutover are
//!   `NOT COMPARABLE`, naming both chains; under another feature set they
//!   compare the events only.

mod common;

use common::warm::{
    Step, import_archive, import_archive_with, oi, one, run_steps, warm_config, warm_ingest,
};
use common::{
    CUTOVER, Connections, DepthConnection, DrivenClock, HOUR, ORIGIN, TempDir, ingest_depth,
    journal,
};
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::DAY_MS;
use mie_adapter_binance::transport::{Clock, HttpGet};
use mie_adapter_binance::{BinanceStream, LiveReplay, RUN_MARGIN_MS};
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_app::equivalence::{DivergenceKind, StateCheckpoint, drive_checkpointed};
use mie_cli::config::IngestConfig;
use mie_cli::equivalence::{
    self, EquivalenceOutcome, EquivalenceRequest, Mismatch, NOTHING_COMPARED, Verdict,
    WarmPrefixDivergence,
};
use mie_cli::ingest::Transports;
use mie_cli::journal::{JournaledRun, read_runs};
use mie_cli::replay::{self, ReplayRequest, ReplaySource};
use mie_domain::event::MarketEvent;
use mie_domain::feature::{FeatureValue, catalog};
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::outbound::{MarketDataProvider, ReplayWindow};
use mie_ports::raw::{
    Capture, RawRecord, RawRecordSink, RawRecordSource, RawSelection, RawStreamKey,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The recorded session.
const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/equivalence");

/// Checkpoints of the recorded session's recompute, its delivered events
/// and the event-stream hash of all of them: they depend on the raw records
/// and the pipeline, never on the feature set.
const FIXTURE_CHECKPOINTS: usize = 18;
const FIXTURE_EVENTS: u64 = 1_369;
const FIXTURE_LAST_EVENT_HASH: &str = "f580ebf8d7010210";

/// The header line of `records.tsv`.
const TSV_HEADER: &str = "stream\treceive_seq\treceive_time_ns\tsession_id\tevent_time_ms\tpayload";

/// The journal line types the fixture keeps: `sealed` lines carry local
/// paths, the rest is not needed.
const JOURNAL_TYPES: [&str; 5] = [
    "run_start",
    "gap",
    "domain_rejection",
    "state_checkpoint",
    "run_end",
];

/// One raw record of the fixture.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    stream: BinanceStream,
    receive_seq: u64,
    receive_time_ns: i64,
    session_id: String,
    event_time_ms: i64,
    payload: String,
}

/// The fixture: its records in `receive_seq` order and its journal lines.
#[derive(Debug, Clone)]
struct Fixture {
    records: Vec<Record>,
    journal: Vec<Value>,
}

impl Fixture {
    fn load() -> Self {
        let text = std::fs::read_to_string(Path::new(FIXTURE).join("records.tsv"))
            .expect("read records.tsv");
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some(TSV_HEADER));
        let records = lines
            .map(|line| {
                let fields: Vec<&str> = line.splitn(6, '\t').collect();
                assert_eq!(fields.len(), 6, "{line}");
                Record {
                    stream: BinanceStream::from_raw_name(fields[0]).expect("a live stream"),
                    receive_seq: fields[1].parse().unwrap(),
                    receive_time_ns: fields[2].parse().unwrap(),
                    session_id: fields[3].to_owned(),
                    event_time_ms: fields[4].parse().unwrap(),
                    payload: fields[5].to_owned(),
                }
            })
            .collect();
        let journal = std::fs::read_to_string(Path::new(FIXTURE).join("journal.jsonl"))
            .expect("read journal.jsonl")
            .lines()
            .map(|l| serde_json::from_str(l).expect("journal line is JSON"))
            .collect();
        Self { records, journal }
    }

    fn line(&self, kind: &str) -> &Value {
        self.journal
            .iter()
            .find(|l| l["type"] == kind)
            .unwrap_or_else(|| panic!("no {kind} line"))
    }

    fn run_id(&self) -> String {
        self.line("run_start")["run_id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    fn started_at_ms(&self) -> i64 {
        self.line("run_start")["at_ms"].as_i64().unwrap()
    }

    /// The fixture's feature set equals this binary's: its state hashes are
    /// comparable (ADR-041 D2).
    fn same_feature_set(&self) -> bool {
        self.line("run_start")["feature_set"] == catalog::current_set().version().to_string()
    }

    /// Writes the records into a fresh raw store and the journal next to
    /// it, under `dir`; returns the configuration that reads them.
    fn materialize(&self, dir: &Path) -> IngestConfig {
        let config = config(dir);
        write_store(&config.paths.raw_root, &self.records);
        write_journal(&config.paths.journal, &self.journal);
        config
    }
}

fn write_store(root: &Path, records: &[Record]) {
    let mut writer = ParquetRawStore::new(root)
        .writer("binance-um", RotationPolicy::default())
        .unwrap();
    for r in records {
        let key = RawStreamKey::new("binance-um", "BTCUSDT", r.stream.raw_name()).unwrap();
        let record = RawRecord {
            event_time: EventTime::from_millis(r.event_time_ms),
            capture: Some(Capture {
                receive_time_ns: r.receive_time_ns,
                receive_seq: r.receive_seq,
                session_id: r.session_id.clone(),
            }),
            payload: r.payload.clone().into_bytes(),
        };
        writer.append(&key, record).unwrap();
    }
    writer.close().unwrap();
}

fn write_journal(path: &Path, lines: &[Value]) {
    let text: String = lines.iter().map(|l| format!("{l}\n")).collect();
    std::fs::write(path, text).unwrap();
}

/// The configuration of the recorded session and of (B): its five
/// streams, checkpoints every 10 s, fake endpoints.
fn config(dir: &Path) -> IngestConfig {
    IngestConfig::parse(&format!(
        r#"
[instrument]
symbol = "BTCUSDT"
source = "binance-um"

[paths]
raw_root = "{}"
journal = "{}"

[binance]
ws_base_url = "wss://fake.invalid/market/ws"
rest_base_url = "https://fake.invalid"
streams = ["aggTrade", "markPrice", "forceOrder", "kline_1m", "openInterest"]

[capture]
state_checkpoint_interval_secs = 10
"#,
        dir.join("raw").display(),
        dir.join("journal.jsonl").display()
    ))
    .expect("valid test config")
}

fn check(config: &IngestConfig, from_ms: i64, to_ms: i64) -> (EquivalenceOutcome, String) {
    let request = EquivalenceRequest {
        config: config.clone(),
        from_ms,
        to_ms,
    };
    let mut out = Vec::new();
    let outcome = equivalence::run(&request, &mut out).expect("the report is written");
    (outcome, String::from_utf8(out).unwrap())
}

/// The recompute of `run_id` from the store of `config`: its delivered
/// events and checkpoints at `interval_ms`.
fn recompute(
    config: &IngestConfig,
    run_id: &str,
    interval_ms: i64,
) -> (Vec<MarketEvent>, Vec<StateCheckpoint>) {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let runs: Vec<_> = read_runs(&config.paths.journal, "binance-um")
        .unwrap()
        .iter()
        .map(JournaledRun::live_run)
        .collect();
    let live = LiveReplay::new(&store, "binance-um", "BTCUSDT", runs);
    let mut replay = live.run_replay(run_id).unwrap();
    let mut events = Vec::new();
    let mut tee = Tee {
        inner: &mut replay,
        events: &mut events,
    };
    let mut checkpoints = Vec::new();
    drive_checkpointed(
        &mut tee,
        &mut MarketStateEngine::new(),
        interval_ms,
        |_| {},
        |c| checkpoints.push(*c),
    )
    .unwrap();
    (events, checkpoints)
}

/// Copies what a provider delivers.
struct Tee<'a, P> {
    inner: &'a mut P,
    events: &'a mut Vec<MarketEvent>,
}

impl<P: MarketDataProvider> MarketDataProvider for Tee<'_, P> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, mie_ports::outbound::ProviderError> {
        let event = self.inner.next_event()?;
        if let Some(event) = &event {
            self.events.push(event.clone());
        }
        Ok(event)
    }
}

// ---------------------------------------------------------------------------
// (A) The recorded live session.

#[test]
fn the_recorded_live_session_replays_equivalently() {
    let fixture = Fixture::load();
    let dir = TempDir::new("equivalence-recorded");
    let config = fixture.materialize(dir.path());
    let start = fixture.started_at_ms();
    let (outcome, text) = check(&config, start, start + 1);
    assert_eq!(outcome.runs.len(), 1, "{text}");
    assert_eq!(outcome.runs[0].run_id, fixture.run_id());
    if fixture.same_feature_set() {
        assert_eq!(outcome.runs[0].verdict, Verdict::Equivalent, "{text}");
        assert!(outcome.pass);
        assert_eq!(outcome.exit_code(), 0);
        assert!(text.ends_with("PASS\n"), "{text}");
        assert!(text.contains(": EQUIVALENT\n"), "{text}");
    } else {
        // A later feature set: the events must still match (ADR-041 D5).
        assert!(
            matches!(outcome.runs[0].verdict, Verdict::EventsOnly { .. }),
            "{text}"
        );
        assert_eq!(outcome.exit_code(), 1);
        assert!(
            text.contains("EVENTS EQUIVALENT, STATE NOT COMPARABLE (feature set "),
            "{text}"
        );
    }

    // Pinned: independent of the feature set.
    let (events, checkpoints) = recompute(&config, &fixture.run_id(), 10_000);
    assert_eq!(checkpoints.len(), FIXTURE_CHECKPOINTS);
    let last = checkpoints.last().unwrap();
    assert!(last.last);
    assert_eq!(last.events.events, FIXTURE_EVENTS);
    assert_eq!(events.len() as u64, FIXTURE_EVENTS);
    assert_eq!(last.events.fingerprint.to_string(), FIXTURE_LAST_EVENT_HASH);
    let journaled = fixture
        .journal
        .iter()
        .filter(|l| l["type"] == "state_checkpoint")
        .count();
    assert_eq!(journaled, FIXTURE_CHECKPOINTS);
    assert_eq!(fixture.line("run_end")["events"], FIXTURE_EVENTS);
}

// ---------------------------------------------------------------------------
// (E) Determinism.

#[test]
fn the_same_request_prints_the_same_bytes() {
    let fixture = Fixture::load();
    let dir = TempDir::new("equivalence-determinism");
    let config = fixture.materialize(dir.path());
    let start = fixture.started_at_ms();
    let (first, text) = check(&config, start - 1_000, start + 1_000);
    let (second, again) = check(&config, start - 1_000, start + 1_000);
    assert_eq!(text, again);
    assert_eq!(first, second);
}

// ---------------------------------------------------------------------------
// (C) Mutations.

/// The fixture with the quantity of one aggregate trade from its middle
/// changed, and the index of the first journaled checkpoint that trade
/// reaches.
fn mutated_trade(fixture: &Fixture) -> (Fixture, usize) {
    let original = TempDir::new("equivalence-mutation-original");
    let config = fixture.materialize(original.path());
    let (delivered, _) = recompute(&config, &fixture.run_id(), 10_000);
    let journaled: Vec<u64> = fixture
        .journal
        .iter()
        .filter(|l| l["type"] == "state_checkpoint")
        .map(|l| l["ordinal"].as_u64().unwrap())
        .collect();

    // The first aggregate trade from the middle of the fixture on that the
    // core received as a trade (not as a late-event gap).
    let trades: Vec<usize> = (0..fixture.records.len())
        .filter(|&i| fixture.records[i].stream == BinanceStream::AggTrade)
        .collect();
    let (index, position) = trades[trades.len() / 2..]
        .iter()
        .find_map(|&i| {
            let payload: Value = serde_json::from_str(&fixture.records[i].payload).unwrap();
            let id = payload["a"].as_u64().unwrap();
            delivered
                .iter()
                .position(|e| matches!(e, MarketEvent::Trade(t) if t.trade_id == id))
                .map(|p| (i, p as u64 + 1))
        })
        .expect("a delivered trade");
    let mut mutated = fixture.clone();
    let mut payload: Value = serde_json::from_str(&mutated.records[index].payload).unwrap();
    let qty = payload["q"].as_str().unwrap().to_owned();
    payload["q"] = json!(if qty == "9.999" { "8.888" } else { "9.999" });
    mutated.records[index].payload = payload.to_string();
    let expected = journaled
        .iter()
        .position(|&ordinal| ordinal >= position)
        .expect("a checkpoint at or after the trade");
    assert!(expected > 0, "the mutation is past the first checkpoint");
    (mutated, expected)
}

#[test]
fn a_mutated_trade_diverges_at_the_first_checkpoint_it_reaches() {
    let fixture = Fixture::load();
    let start = fixture.started_at_ms();
    let (mutated, expected) = mutated_trade(&fixture);
    let journaled: Vec<u64> = fixture
        .journal
        .iter()
        .filter(|l| l["type"] == "state_checkpoint")
        .map(|l| l["ordinal"].as_u64().unwrap())
        .collect();

    let dir = TempDir::new("equivalence-c1");
    let config = mutated.materialize(dir.path());
    let (outcome, text) = check(&config, start, start + 1);
    if fixture.same_feature_set() {
        let Verdict::Diverged(Mismatch::Checkpoint(d)) = &outcome.runs[0].verdict else {
            panic!("{text}")
        };
        assert_eq!((d.index, d.kind), (expected, DivergenceKind::EventStream));
        assert_eq!(d.live.unwrap().ordinal, journaled[expected]);
        assert_ne!(d.live.unwrap().events, d.replay.unwrap().events);
    } else {
        // After a feature-set change the run compares events only (ADR-041
        // D4); the first divergent checkpoint is the same.
        let Verdict::Diverged(Mismatch::Events(d)) = &outcome.runs[0].verdict else {
            panic!("{text}")
        };
        assert_eq!(d.index, expected);
        assert_eq!(d.live.ordinal, journaled[expected]);
        assert_ne!(Some(d.live.events), d.replay);
    }
    assert_eq!(outcome.exit_code(), 1);
    assert!(
        text.contains(&format!(
            ": DIVERGED at checkpoint {expected} (event stream)\n"
        )),
        "{text}"
    );
    assert!(text.contains("  first divergence: checkpoint "), "{text}");
    assert!(
        text.ends_with("FAIL: 1 of 1 run(s) not equivalent\n"),
        "{text}"
    );
}

#[test]
fn a_flipped_state_hash_diverges_as_state_and_a_dropped_record_fails() {
    let fixture = Fixture::load();
    let start = fixture.started_at_ms();

    // C2: one recorded state hash flipped.
    let mut flipped = fixture.clone();
    let positions: Vec<usize> = (0..flipped.journal.len())
        .filter(|&i| flipped.journal[i]["type"] == "state_checkpoint")
        .collect();
    let target = positions.len() / 2;
    let line = &mut flipped.journal[positions[target]];
    let hash = line["state_hash"].as_str().unwrap().to_owned();
    line["state_hash"] = json!(if hash == "0123456789abcdef" {
        "fedcba9876543210"
    } else {
        "0123456789abcdef"
    });
    let dir = TempDir::new("equivalence-c2");
    let config = flipped.materialize(dir.path());
    let (outcome, text) = check(&config, start, start + 1);
    if fixture.same_feature_set() {
        let Verdict::Diverged(Mismatch::Checkpoint(d)) = &outcome.runs[0].verdict else {
            panic!("{text}")
        };
        assert_eq!((d.index, d.kind), (target, DivergenceKind::State));
    } else {
        // The state is not compared; (B) covers this case.
        assert!(
            matches!(outcome.runs[0].verdict, Verdict::EventsOnly { .. }),
            "{text}"
        );
    }
    assert_eq!(outcome.exit_code(), 1);

    // C3: one record dropped; the clean run's integrity check fails.
    let mut dropped = fixture.clone();
    let middle = dropped.records.len() / 2;
    let seq = dropped.records.remove(middle).receive_seq;
    let dir = TempDir::new("equivalence-c3");
    let config = dropped.materialize(dir.path());
    let (outcome, text) = check(&config, start, start + 1);
    let Verdict::NotComparable(reason) = &outcome.runs[0].verdict else {
        panic!("{text}")
    };
    assert!(reason.starts_with("replay failed: "), "{reason}");
    assert!(
        reason.contains(&format!("receive_seq [{seq}, {}) is missing", seq + 1)),
        "{reason}"
    );
    assert_eq!(outcome.exit_code(), 1);
    assert!(text.contains(": NOT COMPARABLE: replay failed: "), "{text}");
}

// ---------------------------------------------------------------------------
// (D) Runs that cannot be compared.

#[test]
fn a_run_without_checkpoints_or_a_window_without_runs_fails() {
    let fixture = Fixture::load();
    let start = fixture.started_at_ms();
    let mut before = fixture.clone();
    before.journal.retain(|l| l["type"] != "state_checkpoint");
    for line in &mut before.journal {
        if line["type"] == "run_start" {
            let fields = line.as_object_mut().unwrap();
            for key in [
                "state_checkpoint_interval_ms",
                "state_hash_encoding",
                "event_hash_encoding",
                "feature_set",
            ] {
                fields.remove(key);
            }
        }
    }
    let dir = TempDir::new("equivalence-d");
    let config = before.materialize(dir.path());
    let (outcome, text) = check(&config, start, start + 1);
    assert_eq!(
        outcome.runs[0].verdict,
        Verdict::NotComparable(
            "the run journaled no state checkpoints (recorded before ADR-041)".to_owned()
        )
    );
    assert_eq!(outcome.exit_code(), 1);
    assert!(
        text.ends_with("FAIL: 1 of 1 run(s) not equivalent\n"),
        "{text}"
    );

    let (outcome, text) = check(&config, start + 1, start + 2);
    assert!(outcome.runs.is_empty());
    assert_eq!(outcome.exit_code(), 1);
    assert!(
        text.ends_with("FAIL: no run started in the window\n"),
        "{text}"
    );
}

#[test]
fn a_run_that_delivered_no_event_is_not_compared() {
    // A clean run stopped before any frame arrived: its `run_start` carries
    // the ADR-041 keys, its `run_end` counts nothing, no checkpoint.
    let fixture = Fixture::load();
    let mut journal = fixture.journal.clone();
    let mut start = fixture.line("run_start").clone();
    let at = fixture.line("run_end")["at_ms"].as_i64().unwrap() + 60_000;
    let run_id = "20991231T235959Z";
    start["run_id"] = json!(run_id);
    start["at_ms"] = json!(at);
    journal.push(start);
    journal.push(json!({
        "type": "run_end", "run_id": run_id, "at_ms": at + 5_000, "exit_code": 0,
        "error": null, "events": 0, "domain_rejections": 0, "records": 0,
    }));
    let dir = TempDir::new("equivalence-d-empty");
    let config = Fixture {
        records: fixture.records.clone(),
        journal,
    }
    .materialize(dir.path());

    // Alone in the window: nothing was compared, so no PASS.
    let (outcome, text) = check(&config, at, at + 1);
    assert_eq!(outcome.runs.len(), 1, "{text}");
    assert_eq!(
        outcome.runs[0].verdict,
        Verdict::NotComparable(NOTHING_COMPARED.to_owned())
    );
    assert!(!outcome.pass);
    assert_eq!(outcome.exit_code(), 1);
    assert!(
        text.contains(&format!(
            "run {run_id}: records 0, events 0, rejections 0, checkpoints 0, dataset "
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!(": NOT COMPARABLE: {NOTHING_COMPARED}\n")),
        "{text}"
    );
    assert!(
        text.ends_with("FAIL: 1 of 1 run(s) not equivalent\n"),
        "{text}"
    );

    // Next to a compared run it still fails the window.
    let (outcome, text) = check(&config, fixture.started_at_ms(), at + 1);
    assert_eq!(outcome.runs.len(), 2, "{text}");
    assert_eq!(
        outcome.runs[1].verdict,
        Verdict::NotComparable(NOTHING_COMPARED.to_owned())
    );
    assert_eq!(outcome.exit_code(), 1);
    let failed = if fixture.same_feature_set() { 1 } else { 2 };
    assert!(
        text.ends_with(&format!("FAIL: {failed} of 2 run(s) not equivalent\n")),
        "{text}"
    );
}

// ---------------------------------------------------------------------------
// (F) Another feature set: events only.

/// `fixture` as if recorded with another feature set than this binary's.
fn another_feature_set(fixture: &Fixture) -> Fixture {
    let mut other = fixture.clone();
    for line in &mut other.journal {
        if line["type"] == "run_start" {
            line["feature_set"] = json!("00000000000000ff");
        }
    }
    assert!(!other.same_feature_set());
    other
}

#[test]
fn another_feature_set_compares_the_events_only() {
    let fixture = Fixture::load();
    let start = fixture.started_at_ms();

    // What the recording engine decided — `as_of`, the end marker, the
    // state, where checkpoints fall, the domain rejections — differs from
    // this binary's; the events are the same.
    let mut engine_moved = another_feature_set(&fixture);
    let mut seen = 0;
    engine_moved.journal.retain(|line| {
        if line["type"] != "state_checkpoint" {
            return true;
        }
        seen += 1;
        seen != 3
    });
    for line in &mut engine_moved.journal {
        match line["type"].as_str() {
            Some("state_checkpoint") => {
                line["as_of"] = json!(line["as_of"].as_i64().unwrap() + 1);
                line["last"] = json!(!line["last"].as_bool().unwrap());
                line["state_hash"] = json!("0123456789abcdef");
            }
            Some("run_end") => line["domain_rejections"] = json!(7),
            _ => {}
        }
    }
    let dir = TempDir::new("equivalence-f-engine");
    let config = engine_moved.materialize(dir.path());
    let (outcome, text) = check(&config, start, start + 1);
    assert!(
        matches!(outcome.runs[0].verdict, Verdict::EventsOnly { .. }),
        "{text}"
    );
    assert_eq!(outcome.exit_code(), 1);
    assert!(
        text.contains("EVENTS EQUIVALENT, STATE NOT COMPARABLE (feature set 00000000000000ff ≠ "),
        "{text}"
    );

    // A mutated trade still diverges at the first checkpoint it reaches.
    let (mutated, expected) = mutated_trade(&fixture);
    let dir = TempDir::new("equivalence-f-mutated");
    let config = another_feature_set(&mutated).materialize(dir.path());
    let (outcome, text) = check(&config, start, start + 1);
    let Verdict::Diverged(Mismatch::Events(d)) = &outcome.runs[0].verdict else {
        panic!("{text}")
    };
    assert_eq!(d.index, expected);
    assert_eq!(d.replay.unwrap().events, d.live.ordinal);
    assert_ne!(d.replay.unwrap(), d.live.events);
    assert_eq!(outcome.exit_code(), 1);
    assert!(
        text.contains(&format!(
            ": DIVERGED at checkpoint {expected} (event stream)\n"
        )),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "  first divergence: checkpoint {expected}, the delivered events differ (events only)\n"
        )),
        "{text}"
    );
}

// ---------------------------------------------------------------------------
// (B) The offline live path: the fixture's payloads through `mie ingest`.

/// Serves the recorded open-interest bodies in order, each once every
/// WebSocket frame was handed to the capture; shuts the capture down when
/// they are spent.
struct RecordedOi {
    bodies: Mutex<VecDeque<String>>,
    pending: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
}

impl HttpGet for RecordedOi {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        assert!(
            url.contains("/fapi/v1/openInterest?symbol=BTCUSDT"),
            "{url}"
        );
        let Some(body) = self.bodies.lock().unwrap().pop_front() else {
            self.shutdown.store(true, Ordering::Relaxed);
            return Err("script exhausted".to_owned());
        };
        while self.pending.load(Ordering::SeqCst) > 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok((200, body.into_bytes()))
    }
}

/// One offline `mie ingest` run at `start_ms` playing `records`: the
/// WebSocket payloads per stream on one connection each, the open-interest
/// bodies through the poller.
fn play(config: &IngestConfig, start_ms: i64, records: &[&Record]) {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = Arc::new(DrivenClock {
        utc_ns: Mutex::new(start_ms * 1_000_000),
        drivers: ["mie-openInterest", "mie-aggTrade"],
    });
    let mut scripts: BTreeMap<String, VecDeque<Vec<String>>> = BTreeMap::new();
    let mut bodies = VecDeque::new();
    for stream in config.streams().unwrap() {
        let payloads: Vec<String> = records
            .iter()
            .filter(|r| r.stream == stream)
            .map(|r| r.payload.clone())
            .collect();
        match stream.ws_path("BTCUSDT") {
            Some(path) => {
                scripts.insert(path, VecDeque::from([payloads]));
            }
            None => bodies.extend(payloads),
        }
    }
    let pending = Arc::new(AtomicUsize::new(scripts.len()));
    let transports = Transports {
        connector: Arc::new(Connections {
            scripts: Mutex::new(scripts),
            pending: Arc::clone(&pending),
            last_path: String::new(),
        }),
        http: Arc::new(RecordedOi {
            bodies: Mutex::new(bodies),
            pending,
            shutdown: Arc::clone(&shutdown),
        }),
        clock: clock as Arc<dyn Clock>,
    };
    let outcome = mie_cli::ingest::run(config, transports, shutdown).expect("ingest starts");
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
}

#[test]
fn the_offline_live_path_in_two_runs_is_equivalent_with_state() {
    let fixture = Fixture::load();
    let dir = TempDir::new("equivalence-live-path");
    let config = config(dir.path());
    let first_ms = fixture.records.first().unwrap().event_time_ms;
    let last_ms = fixture.records.last().unwrap().event_time_ms;
    let middle_ms = first_ms + (last_ms - first_ms) / 2;
    let (early, late): (Vec<&Record>, Vec<&Record>) = fixture
        .records
        .iter()
        .partition(|r| r.event_time_ms < middle_ms);
    let start1 = fixture.started_at_ms();
    play(&config, start1, &early);
    let runs = read_runs(&config.paths.journal, "binance-um").unwrap();
    let start2 = runs[0].end.unwrap().at_ms + 1_000;
    play(&config, start2, &late);

    let runs = read_runs(&config.paths.journal, "binance-um").unwrap();
    assert_eq!(runs.len(), 2);
    // Run 2 restarted with seeds from run 1's records.
    assert!(!runs[1].parameters.seeds.is_empty());
    for run in &runs {
        assert!(run.checkpoints.len() >= 2, "{run:?}");
        assert!(run.checkpoints.last().unwrap().last);
        assert_eq!(
            run.checkpointing.unwrap().feature_set,
            catalog::current_set().version()
        );
    }
    let (outcome, text) = check(&config, start1, start2 + 1);
    assert_eq!(outcome.runs.len(), 2, "{text}");
    for run in &outcome.runs {
        assert_eq!(run.verdict, Verdict::Equivalent, "{text}");
    }
    assert!(outcome.pass, "{text}");
    assert_eq!(outcome.exit_code(), 0);

    // A flipped state hash of run 2 is caught with full state comparison.
    let mut lines: Vec<Value> = std::fs::read_to_string(&config.paths.journal)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let run2 = runs[1].parameters.run_id.clone();
    let line = lines
        .iter_mut()
        .filter(|l| l["type"] == "state_checkpoint" && l["run_id"] == run2.as_str())
        .nth(1)
        .unwrap();
    let hash = line["state_hash"].as_str().unwrap().to_owned();
    line["state_hash"] = json!(if hash == "0123456789abcdef" {
        "fedcba9876543210"
    } else {
        "0123456789abcdef"
    });
    write_journal(&config.paths.journal, &lines);
    let (outcome, text) = check(&config, start1, start2 + 1);
    assert_eq!(outcome.runs[0].verdict, Verdict::Equivalent, "{text}");
    let Verdict::Diverged(Mismatch::Checkpoint(d)) = &outcome.runs[1].verdict else {
        panic!("{text}")
    };
    assert_eq!((d.index, d.kind), (1, DivergenceKind::State));
    assert_eq!(outcome.exit_code(), 1);
}

// ---------------------------------------------------------------------------
// (G) The order book in the state: a depth-only offline ingest.

/// The adapter's recorded live depth window (its fixture README): 29 diffs
/// and two `limit=100` snapshots.
const DEPTH_FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../mie-adapter-binance/tests/fixtures"
);

fn depth_lines(name: &str) -> Vec<String> {
    std::fs::read_to_string(Path::new(DEPTH_FIXTURES).join(format!("{name}.jsonl")))
        .expect("read the depth fixture")
        .lines()
        .map(str::to_owned)
        .collect()
}

/// A depth-only capture into `dir`, state checkpoints every second.
fn depth_config(dir: &Path) -> IngestConfig {
    IngestConfig::parse(&format!(
        r#"
[instrument]
symbol = "BTCUSDT"
source = "binance-um"

[paths]
raw_root = "{}"
journal = "{}"

[binance]
ws_base_url = "wss://fake.invalid/market/ws"
ws_public_base_url = "wss://fake.invalid/public/ws"
rest_base_url = "https://fake.invalid"
streams = ["depth", "depthSnapshot"]

[capture]
hold_back_ms = 750
depth_snapshot_min_spacing_ms = 1
state_checkpoint_interval_secs = 1
"#,
        dir.join("raw").display(),
        dir.join("journal.jsonl").display()
    ))
    .expect("valid test config")
}

/// Every record of `config`'s store, in `receive_seq` order.
fn stored_records(config: &IngestConfig) -> Vec<Record> {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let keys: BTreeSet<RawStreamKey> = ["depth", "depthSnapshot"]
        .iter()
        .map(|s| RawStreamKey::new("binance-um", "BTCUSDT", s).unwrap())
        .collect();
    let day = 86_400_000;
    let first = depth_time(&depth_lines("depth")[0]);
    let window = ReplayWindow {
        start: EventTime::from_millis(first - day),
        end: EventTime::from_millis(first + day),
    };
    let mut records = Vec::new();
    for file in store
        .select(&RawSelection::new(keys, window).unwrap())
        .unwrap()
        .files
    {
        for record in store.read(&file).unwrap() {
            let capture = record.capture.clone().unwrap();
            records.push(Record {
                stream: BinanceStream::from_raw_name(file.stream.stream()).unwrap(),
                receive_seq: capture.receive_seq,
                receive_time_ns: capture.receive_time_ns,
                session_id: capture.session_id,
                event_time_ms: record.event_time.as_millis(),
                payload: String::from_utf8(record.payload).unwrap(),
            });
        }
    }
    records.sort_by_key(|r| r.receive_seq);
    records
}

/// The `T` of a depth diff.
fn depth_time(diff: &str) -> i64 {
    let payload: Value = serde_json::from_str(diff).unwrap();
    payload["T"].as_i64().unwrap()
}

#[test]
fn the_order_book_state_replays_equivalently_and_a_mutated_level_diverges() {
    let diffs = depth_lines("depth");
    let snapshots = depth_lines("depthSnapshot");
    let dir = TempDir::new("equivalence-depth");
    let config = depth_config(dir.path());
    let start = depth_time(&diffs[0]);
    let outcome = ingest_depth(
        &config,
        start,
        vec![DepthConnection {
            frames: diffs.clone(),
            await_served: 1,
        }],
        snapshots,
        vec![],
    );
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    assert_eq!(outcome.domain_rejections, 0);
    let runs = read_runs(&config.paths.journal, "binance-um").unwrap();
    assert_eq!(runs.len(), 1);
    let run = &runs[0];
    assert!(run.checkpoints.len() >= 3, "{run:?}");
    let run_id = run.parameters.run_id.clone();

    let (outcome, text) = check(&config, start, start + 1);
    assert_eq!(outcome.runs.len(), 1, "{text}");
    assert_eq!(outcome.runs[0].verdict, Verdict::Equivalent, "{text}");
    assert_eq!(outcome.exit_code(), 0);

    // The compared states carry the book: the recompute ends with it ready.
    let (delivered, checkpoints) = recompute(&config, &run_id, 1_000);
    assert_eq!(checkpoints.len(), run.checkpoints.len());
    let mut engine = MarketStateEngine::new();
    for event in &delivered {
        engine.apply(event).unwrap();
    }
    assert!(
        matches!(engine.state().book.l2, FeatureValue::Ready(_)),
        "{:?}",
        engine.state().book.l2
    );

    // A mutated level quantity in a diff from the middle of the window.
    let records = stored_records(&config);
    let journal: Vec<Value> = std::fs::read_to_string(&config.paths.journal)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let journaled: Vec<u64> = journal
        .iter()
        .filter(|l| l["type"] == "state_checkpoint")
        .map(|l| l["ordinal"].as_u64().unwrap())
        .collect();
    let target = records
        .iter()
        .position(|r| r.stream == BinanceStream::Depth && r.payload == diffs[14])
        .expect("the 15th diff is stored");
    let u = serde_json::from_str::<Value>(&diffs[14]).unwrap()["u"]
        .as_u64()
        .unwrap();
    let position = delivered
        .iter()
        .position(|e| matches!(e, MarketEvent::BookUpdate(b) if b.last_update_id == u))
        .expect("the 15th diff is delivered after the sync") as u64
        + 1;
    let expected = journaled
        .iter()
        .position(|&ordinal| ordinal >= position)
        .expect("a checkpoint at or after the diff");
    let mut mutated = records.clone();
    let mut payload: Value = serde_json::from_str(&mutated[target].payload).unwrap();
    let qty = payload["b"][0][1].as_str().unwrap().to_owned();
    payload["b"][0][1] = json!(if qty == "9.999" { "8.888" } else { "9.999" });
    mutated[target].payload = payload.to_string();
    let dir = TempDir::new("equivalence-depth-mutated");
    let mutated_config = depth_config(dir.path());
    write_store(&mutated_config.paths.raw_root, &mutated);
    write_journal(&mutated_config.paths.journal, &journal);
    let (outcome, text) = check(&mutated_config, start, start + 1);
    let Verdict::Diverged(Mismatch::Checkpoint(d)) = &outcome.runs[0].verdict else {
        panic!("{text}")
    };
    assert_eq!(
        (d.index, d.kind),
        (expected, DivergenceKind::EventStream),
        "{text}"
    );
    assert_eq!(outcome.exit_code(), 1);

    // A flipped state hash diverges as state.
    let mut flipped = journal.clone();
    let positions: Vec<usize> = (0..flipped.len())
        .filter(|&i| flipped[i]["type"] == "state_checkpoint")
        .collect();
    let target = positions.len() / 2;
    let line = &mut flipped[positions[target]];
    let hash = line["state_hash"].as_str().unwrap().to_owned();
    line["state_hash"] = json!(if hash == "0123456789abcdef" {
        "fedcba9876543210"
    } else {
        "0123456789abcdef"
    });
    let dir = TempDir::new("equivalence-depth-flipped");
    let flipped_config = depth_config(dir.path());
    write_store(&flipped_config.paths.raw_root, &records);
    write_journal(&flipped_config.paths.journal, &flipped);
    let (outcome, text) = check(&flipped_config, start, start + 1);
    let Verdict::Diverged(Mismatch::Checkpoint(d)) = &outcome.runs[0].verdict else {
        panic!("{text}")
    };
    assert_eq!((d.index, d.kind), (target, DivergenceKind::State), "{text}");
}

// ---------------------------------------------------------------------------
// (H)–(K) Warm-started runs (ADR-052 D9): one pass over the history chain.

/// The streams of the scripted warm captures.
const WARM_STREAMS: [&str; 3] = ["aggTrade", "markPrice", "openInterest"];

/// A warm-started chain run of `run_steps` at `t`, trade ids from
/// `first_id`, its open-interest sample at `oi_ms`: its run id.
fn warm_run(config: &IngestConfig, t: i64, first_id: u64, oi_ms: i64) -> String {
    let outcome = warm_ingest(config, t, run_steps(t, first_id), vec![oi(oi_ms)], None);
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    let lines = journal(config);
    assert_eq!(one(&lines, &outcome.run_id, "warm_start")["warm"], true);
    outcome.run_id
}

/// Two warm runs an hour apart under `dir`: more than `RUN_MARGIN_MS`
/// after the first ended, so the second's files lie outside its prefix's
/// selection and the journaled prefix dataset version stays current. The
/// configuration, the run ids and the start times.
fn two_warm_runs(dir: &Path) -> (IngestConfig, [String; 2], [i64; 2]) {
    import_archive(dir);
    let config = warm_config(dir, Some(1_000));
    let (t1, t2) = (CUTOVER + HOUR, CUTOVER + 2 * HOUR);
    let r1 = warm_run(&config, t1, 10_000, t1 - 4_000);
    let r2 = warm_run(&config, t2, 20_000, t2 - 3_000);
    (config, [r1, r2], [t1, t2])
}

/// The warm prefix divergence of `verdict`.
fn warm_prefix<'a>(verdict: &'a Verdict, text: &str) -> &'a WarmPrefixDivergence {
    let Verdict::Diverged(Mismatch::WarmPrefix(d)) = verdict else {
        panic!("{text}")
    };
    d
}

/// Every live record of `config`'s store, by run and `receive_seq`.
fn live_records(config: &IngestConfig) -> Vec<Record> {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let keys: BTreeSet<RawStreamKey> = WARM_STREAMS
        .iter()
        .map(|s| RawStreamKey::new("binance-um", "BTCUSDT", s).unwrap())
        .collect();
    let window = ReplayWindow {
        start: EventTime::from_millis(CUTOVER - DAY_MS),
        end: EventTime::from_millis(CUTOVER + 2 * DAY_MS),
    };
    let mut records = Vec::new();
    for file in store
        .select(&RawSelection::new(keys, window).unwrap())
        .unwrap()
        .files
    {
        for record in store.read(&file).unwrap() {
            let capture = record.capture.clone().unwrap();
            records.push(Record {
                stream: BinanceStream::from_raw_name(file.stream.stream()).unwrap(),
                receive_seq: capture.receive_seq,
                receive_time_ns: capture.receive_time_ns,
                session_id: capture.session_id,
                event_time_ms: record.event_time.as_millis(),
                payload: String::from_utf8(record.payload).unwrap(),
            });
        }
    }
    let run_of = |r: &Record| {
        r.session_id
            .split('/')
            .next()
            .unwrap_or_default()
            .to_owned()
    };
    records.sort_by_key(|r| (run_of(r), r.receive_seq));
    records
}

/// `config`'s store rebuilt under `dir`: the archive of
/// [`import_archive_with`] `archive`, the live records read back and
/// rewritten after `live`, and a copy of the journal of the unchanged runs.
fn rebuilt(
    config: &IngestConfig,
    dir: &Path,
    archive: impl FnOnce(&mut Vec<(ArchiveStream, String)>),
    live: impl FnOnce(&mut [Record]),
) -> IngestConfig {
    import_archive_with(dir, archive);
    let rebuilt = warm_config(dir, Some(1_000));
    let mut records = live_records(config);
    live(&mut records);
    write_store(&rebuilt.paths.raw_root, &records);
    std::fs::copy(&config.paths.journal, &rebuilt.paths.journal).unwrap();
    rebuilt
}

/// `config` over the same store, reading the journal `lines` written under
/// `dir`.
fn with_journal(config: &IngestConfig, dir: &Path, lines: &[Value]) -> IngestConfig {
    let mut copy = config.clone();
    copy.paths.journal = dir.join("journal.jsonl");
    write_journal(&copy.paths.journal, lines);
    copy
}

/// The journaled `field` of run `run_id`'s `kind` line, as a count.
fn count(lines: &[Value], run_id: &str, kind: &str, field: &str) -> u64 {
    one(lines, run_id, kind)[field].as_u64().unwrap()
}

#[test]
fn warm_started_runs_with_a_restart_between_them_are_equivalent() {
    let dir = TempDir::new("equivalence-warm");
    import_archive(dir.path());
    let config = warm_config(dir.path(), Some(1_000));
    let t1 = CUTOVER + HOUR;
    let r1 = warm_run(&config, t1, 10_000, t1 - 4_000);
    // A fast restart 2 s after the first run ended (the schedule of
    // `a_fast_restart_overlap_is_late_or_retimed_never_rejected`): its first
    // trade and its open interest are older than the first run's last
    // event, so the join turns them into a `LateEvent` gap and re-timed
    // open interest inside the compared part.
    let t2 = t1 + 3_500;
    let steps = vec![
        Step::Advance(6_500),
        Step::Polled(1),
        Step::Frame(common::warm::AGG, common::agg(10_100, t1 + 64_000)),
        Step::Frame(common::warm::AGG, common::agg(10_101, t1 + 70_000)),
        Step::Gate,
        Step::Frame(common::warm::AGG, common::agg(10_102, t1 + 71_000)),
        Step::Frame(common::warm::AGG, common::agg(10_103, t1 + 72_000)),
    ];
    let second = warm_ingest(&config, t2, steps, vec![oi(t1 + 60_000)], None);
    assert_eq!(second.exit_code(), 0, "{:?}", second.error);
    let r2 = second.run_id;
    // About an hour later, on a 10 s poll slot as `run_steps` needs.
    let t3 = t1 + 2 * HOUR;
    let r3 = warm_run(&config, t3, 30_000, t3 - 3_000);
    let runs = [r1, r2, r3];
    let lines = journal(&config);
    assert_eq!(one(&lines, &runs[1], "warm_start")["warm"], true);

    // Each prefix contains the predecessor's prefix and its events.
    for pair in runs.windows(2) {
        assert_eq!(
            count(&lines, &pair[1], "warm_start", "prefix_events"),
            count(&lines, &pair[0], "warm_start", "prefix_events")
                + count(&lines, &pair[0], "run_end", "events"),
            "{pair:?}"
        );
    }
    // The fast restart's join made a late gap, which its core consumed.
    let end2 = one(&lines, &runs[1], "run_end")["at_ms"].as_i64().unwrap();
    let request = ReplayRequest {
        source: ReplaySource::History(config.clone()),
        window: ReplayWindow {
            start: EventTime::from_millis(ORIGIN),
            end: EventTime::from_millis(end2 + RUN_MARGIN_MS),
        },
    };
    let mut out = Vec::new();
    replay::run(&request, &mut out).expect("the report is written");
    let replayed = String::from_utf8(out).unwrap();
    assert!(
        replayed
            .lines()
            .any(|l| l.starts_with("delivered Trades:") && l.contains("LateEvent")),
        "{replayed}"
    );

    let (outcome, text) = check(&config, t1, t3 + 1);
    assert_eq!(outcome.runs.len(), 3, "{text}");
    for run in &outcome.runs {
        assert_eq!(run.verdict, Verdict::Equivalent, "{text}");
    }
    assert!(outcome.pass, "{text}");
    assert_eq!(outcome.exit_code(), 0);
    assert!(
        text.ends_with(
            "PASS
"
        ),
        "{text}"
    );
    assert_eq!(text.matches(": one pass through run ").count(), 1, "{text}");
    assert!(
        text.contains(&format!(
            "history origin {ORIGIN} cutover {CUTOVER}: one pass through run {}
",
            runs[2]
        )),
        "{text}"
    );
    for run in &runs {
        let prefix = count(&lines, run, "warm_start", "prefix_events");
        assert!(
            text.contains(&format!("run {run}: warm prefix events {prefix}, records ")),
            "{text}"
        );
    }

    // A second invocation prints the same bytes.
    let (again, repeated) = check(&config, t1, t3 + 1);
    assert_eq!(repeated, text);
    assert_eq!(again, outcome);

    // The last run alone: the pass crosses the runs not selected.
    let (alone, text) = check(&config, t3, t3 + 1);
    assert_eq!(alone.runs.len(), 1, "{text}");
    assert_eq!(alone.runs[0].verdict, Verdict::Equivalent, "{text}");
    assert!(alone.pass, "{text}");
}

#[test]
fn a_changed_warm_prefix_or_handoff_diverges_as_the_warm_prefix() {
    let dir = TempDir::new("equivalence-warm-prefix");
    let (config, [r1, r2], [t1, t2]) = two_warm_runs(dir.path());
    let lines = journal(&config);
    let (outcome, text) = check(&config, t1, t2 + 1);
    assert!(outcome.pass, "{text}");

    // (c) The second run's journaled handoff state hash flipped, in a
    // journal copy over the same store.
    let mut flipped = lines.clone();
    let warm = flipped
        .iter_mut()
        .find(|l| l["type"] == "warm_start" && l["run_id"] == r2.as_str())
        .unwrap();
    let hash = warm["handoff_state_hash"].as_str().unwrap().to_owned();
    warm["handoff_state_hash"] = json!(if hash == "0123456789abcdef" {
        "fedcba9876543210"
    } else {
        "0123456789abcdef"
    });
    let copy = TempDir::new("equivalence-warm-handoff");
    let (outcome, text) = check(&with_journal(&config, copy.path(), &flipped), t1, t2 + 1);
    assert_eq!(outcome.runs[0].verdict, Verdict::Equivalent, "{text}");
    let d = warm_prefix(&outcome.runs[1].verdict, &text);
    assert_eq!(d.kind, DivergenceKind::State, "{text}");
    assert_eq!(d.live_stream, d.replay_stream);
    assert_ne!(d.live_state, d.replay_state);
    assert_eq!(d.live_dataset, d.replay_dataset, "{text}");
    assert_eq!(outcome.exit_code(), 1);
    assert!(
        text.contains(&format!("run {r2}: warm prefix events ")),
        "{text}"
    );
    assert!(
        text.contains(
            ": DIVERGED (warm prefix)
"
        ),
        "{text}"
    );
    assert!(
        text.contains(
            "  first divergence: the warm prefix, the events match, the handoff state differs
"
        ),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "  dataset    live {} replay {}
",
            d.live_dataset, d.replay_dataset
        )),
        "{text}"
    );
    assert!(
        text.ends_with(
            "FAIL: 1 of 2 run(s) not equivalent
"
        ),
        "{text}"
    );

    // (a) One archive trade's quantity changed.
    let store = TempDir::new("equivalence-warm-archive");
    let changed = rebuilt(
        &config,
        store.path(),
        |rows| {
            let row = &mut rows[30].1;
            assert!(row.contains(",0.010,"), "{row}");
            *row = row.replacen(",0.010,", ",0.020,", 1);
        },
        |_| {},
    );
    let (outcome, text) = check(&changed, t1, t2 + 1);
    assert_eq!(outcome.runs.len(), 2, "{text}");
    for run in &outcome.runs {
        let d = warm_prefix(&run.verdict, &text);
        assert_eq!(d.kind, DivergenceKind::EventStream, "{text}");
        assert_ne!(d.live_stream, d.replay_stream);
        assert_ne!(d.live_dataset, d.replay_dataset, "{text}");
    }
    assert!(
        text.contains(
            "  first divergence: the warm prefix, the delivered events differ
"
        ),
        "{text}"
    );
    assert_eq!(outcome.exit_code(), 1);

    // (b) One trade of the first run changed.
    let store = TempDir::new("equivalence-warm-run");
    let prefix = format!("{r1}/");
    let changed = rebuilt(
        &config,
        store.path(),
        |_| {},
        |records| {
            let record = records
                .iter_mut()
                .find(|r| {
                    r.session_id.starts_with(&prefix)
                        && r.stream == BinanceStream::AggTrade
                        && serde_json::from_str::<Value>(&r.payload).unwrap()["a"] == 10_003
                })
                .expect("the first run's fourth trade");
            let mut payload: Value = serde_json::from_str(&record.payload).unwrap();
            payload["q"] = json!("9.999");
            record.payload = payload.to_string();
        },
    );
    let (outcome, text) = check(&changed, t1, t2 + 1);
    let Verdict::Diverged(Mismatch::Checkpoint(d)) = &outcome.runs[0].verdict else {
        panic!("{text}")
    };
    assert_eq!(d.kind, DivergenceKind::EventStream, "{text}");
    let d = warm_prefix(&outcome.runs[1].verdict, &text);
    assert_eq!(d.kind, DivergenceKind::EventStream, "{text}");
    assert_eq!(outcome.exit_code(), 1);
}

#[test]
fn a_warm_run_after_a_cold_chain_run_crosses_its_chain_recompute() {
    let dir = TempDir::new("equivalence-warm-after-cold");
    import_archive(dir.path());
    // Without max_run_records no warm-up starts (`warm-up-failed`), so the
    // script has no gate to wait for.
    let cold = warm_config(dir.path(), None);
    let t1 = CUTOVER + HOUR;
    let steps = run_steps(t1, 10_000)
        .into_iter()
        .filter(|step| !matches!(step, Step::Gate))
        .collect();
    let first = warm_ingest(&cold, t1, steps, vec![oi(t1 - 4_000)], None);
    assert_eq!(first.exit_code(), 0, "{:?}", first.error);
    let r1 = first.run_id;
    let config = warm_config(dir.path(), Some(1_000));
    let t2 = t1 + HOUR;
    let r2 = warm_run(&config, t2, 20_000, t2 - 3_000);
    let lines = journal(&config);
    assert_eq!(one(&lines, &r1, "warm_start")["cold"], "warm-up-failed");

    let (outcome, text) = check(&config, t1, t2 + 1);
    assert_eq!(outcome.runs.len(), 2, "{text}");
    for run in &outcome.runs {
        assert_eq!(run.verdict, Verdict::Equivalent, "{text}");
    }
    assert!(outcome.pass, "{text}");
    // The first run in the cold unit, the second in the warm one.
    assert!(text.contains(&format!("run {r1}: records ")), "{text}");
    assert!(
        text.contains(&format!("run {r2}: warm prefix events ")),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            ": one pass through run {r2}
"
        )),
        "{text}"
    );
}

#[test]
fn a_warm_run_under_another_history_or_feature_set_is_not_compared_in_full() {
    let dir = TempDir::new("equivalence-warm-other");
    let (config, runs, [t1, t2]) = two_warm_runs(dir.path());
    let journaled = format!(
        "the run warm-started from the history chain with origin {ORIGIN} ms and cutover \
         {CUTOVER} ms"
    );

    // Without [history].
    let mut none = config.clone();
    none.history = None;
    let (outcome, text) = check(&none, t1, t2 + 1);
    for run in &outcome.runs {
        assert_eq!(
            run.verdict,
            Verdict::NotComparable(format!("{journaled}, the configuration has no [history]")),
            "{text}"
        );
    }
    assert!(!text.contains("one pass"), "{text}");
    assert_eq!(outcome.exit_code(), 1);

    // Under another cutover.
    let mut other = config.clone();
    other.history.as_mut().unwrap().cutover = "2026-10-05".to_owned();
    let (outcome, text) = check(&other, t1, t2 + 1);
    for run in &outcome.runs {
        assert_eq!(
            run.verdict,
            Verdict::NotComparable(format!(
                "{journaled}, the configured [history] has origin {ORIGIN} ms and cutover {} ms",
                CUTOVER - DAY_MS
            )),
            "{text}"
        );
    }
    assert_eq!(outcome.exit_code(), 1);

    // Recorded with another feature set: the events only, the prefix's
    // too.
    let mut lines = journal(&config);
    for line in &mut lines {
        if line["type"] == "run_start" {
            line["feature_set"] = json!("00000000000000ff");
        }
    }
    let copy = TempDir::new("equivalence-warm-feature-set");
    let (outcome, text) = check(&with_journal(&config, copy.path(), &lines), t1, t2 + 1);
    assert_eq!(outcome.runs.len(), runs.len(), "{text}");
    for run in &outcome.runs {
        assert!(matches!(run.verdict, Verdict::EventsOnly { .. }), "{text}");
    }
    assert!(
        text.contains("EVENTS EQUIVALENT, STATE NOT COMPARABLE (feature set 00000000000000ff ≠ "),
        "{text}"
    );
    assert_eq!(outcome.exit_code(), 1);
}

// ---------------------------------------------------------------------------
// Re-recording the fixture.

fn env_path(name: &str) -> PathBuf {
    PathBuf::from(std::env::var(name).unwrap_or_else(|_| panic!("set {name}")))
}

/// Exports one cleanly ended run of a real `mie ingest` capture as the
/// fixture (recipe in `tests/fixtures/equivalence/README.md`):
/// `MIE_FIXTURE_RAW_ROOT` and `MIE_FIXTURE_JOURNAL` are the capture's
/// raw root and journal, `MIE_FIXTURE_RUN_ID` the run, `MIE_FIXTURE_OUT`
/// the fixture directory.
#[test]
#[ignore = "re-records the fixture from a real capture; see the fixture README"]
fn export_equivalence_fixture() {
    let raw_root = env_path("MIE_FIXTURE_RAW_ROOT");
    let journal_path = env_path("MIE_FIXTURE_JOURNAL");
    let run_id = std::env::var("MIE_FIXTURE_RUN_ID").expect("set MIE_FIXTURE_RUN_ID");
    let out = env_path("MIE_FIXTURE_OUT");

    let journal: Vec<Value> = std::fs::read_to_string(&journal_path)
        .unwrap()
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|l| l["run_id"] == run_id.as_str())
        .filter(|l| JOURNAL_TYPES.contains(&l["type"].as_str().unwrap_or_default()))
        .collect();
    let start = journal.iter().find(|l| l["type"] == "run_start").unwrap();
    let end = journal.iter().find(|l| l["type"] == "run_end").unwrap();
    assert_eq!(end["exit_code"], 0, "the run must have ended cleanly");
    let source = start["source"].as_str().unwrap();
    let symbol = start["symbol"].as_str().unwrap();
    let keys: BTreeSet<RawStreamKey> = start["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| RawStreamKey::new(source, symbol, s.as_str().unwrap()).unwrap())
        .collect();
    let day = 86_400_000;
    let window = ReplayWindow {
        start: EventTime::from_millis(start["at_ms"].as_i64().unwrap() - day),
        end: EventTime::from_millis(end["at_ms"].as_i64().unwrap() + day),
    };
    let store = ParquetRawStore::new(&raw_root);
    let prefix = format!("{run_id}/");
    let mut records = Vec::new();
    for file in store
        .select(&RawSelection::new(keys, window).unwrap())
        .unwrap()
        .files
    {
        for record in store.read(&file).unwrap() {
            let capture = record.capture.clone().unwrap();
            if capture.session_id.starts_with(&prefix) {
                records.push((file.stream.stream().to_owned(), capture, record));
            }
        }
    }
    records.sort_by_key(|(_, capture, _)| capture.receive_seq);
    assert_eq!(records.len() as u64, end["records"].as_u64().unwrap());
    let mut tsv = format!("{TSV_HEADER}\n");
    for (i, (stream, capture, record)) in records.iter().enumerate() {
        assert_eq!(capture.receive_seq, i as u64, "a hole in the run");
        let payload = String::from_utf8(record.payload.clone()).expect("UTF-8 payload");
        assert!(
            !payload.contains(['\t', '\n', '\r']),
            "receive_seq {}: a tab or newline in the payload",
            capture.receive_seq
        );
        tsv.push_str(&format!(
            "{stream}\t{}\t{}\t{}\t{}\t{payload}\n",
            capture.receive_seq,
            capture.receive_time_ns,
            capture.session_id,
            record.event_time.as_millis()
        ));
    }
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("records.tsv"), tsv).unwrap();
    write_journal(&out.join("journal.jsonl"), &journal);
}
