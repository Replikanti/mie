//! The #146 tests: the model against the sampler on the golden and the
//! generated tapes, the episode rule on synthetic streams, edge-class
//! coverage, determinism and the independence guard.

use super::harness::{self, Seen, Start, Tape};
use super::streams;
use super::tapes;
use crate::event::MarketEvent;
use crate::time::EventTime;
use std::sync::OnceLock;

const DAY: i64 = 86_400_000;

/// The index and time of the first event at or after `time`.
fn split(events: &[MarketEvent], time: i64) -> (usize, EventTime) {
    let index = events
        .iter()
        .position(|event| event.time().as_millis() >= time)
        .expect("an event after the split");
    (index, events[index].time())
}

/// The golden tapes with their starts: the candidate golden tape full, with
/// `emit_from` at day 10 and observed late from that event (the M1 tests'
/// split, after the day-9 trades gap); the location walk tape; the book tape.
fn golden_tapes() -> Vec<Tape> {
    let candidate = crate::candidate::tests::golden_tape();
    let (index, time) = split(&candidate, 10 * DAY);
    vec![
        Tape {
            name: "candidate golden".into(),
            seed: 0x6d69_6500_0000_0022,
            events: candidate,
            starts: vec![Start::Full, Start::EmitFrom(time), Start::LateAt(index)],
        },
        Tape {
            name: "location walk".into(),
            seed: 0x6d69_6500_0000_0044,
            events: crate::location::tests::walk_tape(0x6d69_6500_0000_0044, 7),
            starts: vec![Start::Full],
        },
        Tape {
            name: "book".into(),
            seed: 0x6d69_6500_0022_0003,
            events: crate::candidate::tests::book_tape(),
            starts: vec![Start::Full],
        },
    ]
}

/// The golden comparison, replayed once (a test-only cache).
fn golden_seen() -> &'static Seen {
    static SEEN: OnceLock<Seen> = OnceLock::new();
    SEEN.get_or_init(|| {
        let mut seen = Seen::default();
        for tape in golden_tapes() {
            let one = harness::run(&tape);
            println!("{}: {one:?}", tape.name);
            seen.add(&one);
        }
        seen
    })
}

#[test]
fn model_matches_the_sampler_on_the_golden_tapes() {
    let seen = golden_seen();
    println!("golden: {seen:?}");
    assert_eq!(seen.rejected, 0);
    assert!(seen.baseline > 0, "baseline records compared");
    assert!(seen.trigger > 0, "trigger records compared");
    assert!(seen.non_opener > 0, "non-opener samples compared");
    assert!(seen.truncated > 0, "truncated samples compared");
    assert!(seen.held_back > 0, "records held back before emit_from");
}

/// The stream templates, from the candidate golden tape (a test-only cache).
fn templates() -> &'static streams::Templates {
    static TEMPLATES: OnceLock<streams::Templates> = OnceLock::new();
    TEMPLATES.get_or_init(|| streams::Templates::from_tape(&crate::candidate::tests::golden_tape()))
}

/// The stream seeds: a fixed base and its successors.
const STREAM_SEEDS: std::ops::Range<u64> = 0x6d69_6501_4600_0000..0x6d69_6501_4600_0010;

#[test]
fn model_matches_the_episode_tracker_on_synthetic_trigger_streams() {
    let mut cases = streams::Cases::default();
    for seed in STREAM_SEEDS {
        cases.add(&streams::run_stream(templates(), seed, 400));
    }
    println!("streams: {cases:?}");
    let streams::Cases {
        assignments,
        overlap,
        touch,
        containment,
        wholly_before,
        out_of_order,
        disjoint_in_order,
        disjoint_reversed,
        first_at_observed,
        bridges,
        truncated,
        bridge_reading,
    } = cases;
    for (name, count) in [
        ("assignments", assignments),
        ("overlap", overlap),
        ("touch", touch),
        ("containment", containment),
        ("wholly before", wholly_before),
        ("out of order", out_of_order),
        ("disjoint pair in order", disjoint_in_order),
        ("disjoint pair reversed", disjoint_reversed),
        ("first == observed_from", first_at_observed),
        ("bridge", bridges),
        ("truncated", truncated),
        ("bridge reading decided", bridge_reading),
    ] {
        assert!(count > 0, "stream case {name} never occurred");
    }
}

/// The generated corpus comparison, run once (a test-only cache).
fn corpus_seen() -> &'static Seen {
    static SEEN: OnceLock<Seen> = OnceLock::new();
    SEEN.get_or_init(|| {
        let mut seen = Seen::default();
        for tape in tapes::corpus() {
            let one = harness::run(&tape);
            println!(
                "{} {:#x} ({} events, starts {:?}): {one:?}",
                tape.name,
                tape.seed,
                tape.events.len(),
                tape.starts
            );
            seen.add(&one);
        }
        seen
    })
}

#[test]
fn model_matches_the_sampler_on_generated_tapes() {
    let seen = corpus_seen();
    println!("generated: {seen:?}");
    assert_eq!(seen.rejected, 0);
    assert!(seen.baseline > 0 && seen.trigger > 0 && seen.non_opener > 0);
    assert!(seen.truncated > 0 && seen.held_back > 0);
}

#[test]
fn generator_covers_every_edge_class() {
    let seen = corpus_seen();
    let classes = [
        ("1 bridge sample", seen.bridge),
        ("2 baseline anchor on a padded edge", seen.padded_edge),
        (
            "3 span earlier than its id's previous span",
            seen.out_of_order,
        ),
        ("4 event closing only empty minutes", seen.empty_only),
        (
            "5 divergence confirmed across a trades gap",
            seen.gap_confirmation,
        ),
        (
            "6 opener with first == observed_from",
            seen.first_at_observed,
        ),
    ];
    for (class, count) in classes {
        println!("class {class}: {count}");
    }
    println!(
        "reported: spans wholly before the open episode {}, SFP spans out of order {}",
        seen.wholly_before, seen.sfp_out_of_order
    );
    for (class, count) in classes {
        assert!(count >= 1, "edge class {class} never occurred");
    }
}

#[test]
fn generated_tapes_are_deterministic() {
    let [one, two] = tapes::BRIDGE_SEEDS;
    assert_eq!(tapes::bridge_tape(one), tapes::bridge_tape(one));
    assert_ne!(tapes::bridge_tape(one), tapes::bridge_tape(two));
    let [edge] = tapes::EDGE_SEEDS;
    assert_eq!(tapes::edge_tape(edge), tapes::edge_tape(edge));
    let fuzz = tapes::fuzz_tape(tapes::FUZZ_BASE, 2);
    assert_eq!(fuzz, tapes::fuzz_tape(tapes::FUZZ_BASE, 2));
    assert_ne!(fuzz, tapes::fuzz_tape(tapes::FUZZ_BASE + 1, 2));
    // The same tape gives the same records on a second run.
    let records = |events: &[MarketEvent]| {
        let mut engine = crate::state::MarketStateEngine::new();
        let mut sampler = crate::conditional::ContextSampler::new(None);
        let mut records = Vec::new();
        for event in events {
            engine.apply(event).expect("a generated event is accepted");
            sampler.observe(event, &engine, |record| records.push(record));
        }
        records
    };
    let first = records(&fuzz);
    assert!(!first.is_empty());
    assert_eq!(first, records(&fuzz));
}

/// The names `model.rs` must not use (#146 protocol item 5): the sampler's
/// span and episode code, its snapshot reader, policy text and the sampler
/// itself, and the location reader the baseline context re-derives.
const BANNED: [&str; 13] = [
    "evidence_span",
    "EvidenceSpan",
    "EpisodeTracker",
    "SpanRule",
    "Assignment",
    "trigger_position",
    "TRIGGER_IDS",
    "SNAPSHOT_FEATURES",
    "StateSnapshot::read",
    "policy_text",
    "ContextSampler",
    "LocationContext::read",
    "location::within",
];

/// What `model.rs` may import from `crate::conditional`.
const ALLOWED: [&str; 5] = [
    "ContextRecord",
    "Sample",
    "Episode",
    "EpisodeId",
    "StateSnapshot",
];

#[test]
fn model_reads_no_sampler_internals() {
    let source = include_str!("model.rs");
    for name in BANNED {
        assert!(!source.contains(name), "model.rs names {name}");
    }
    // `conditional` appears only in one `use crate::conditional::{…};`
    // statement, whose names are on the allow-list.
    let statement = "use crate::conditional::{";
    assert_eq!(
        source.matches("conditional").count(),
        1,
        "one conditional path"
    );
    let start = source.find(statement).expect("the conditional import") + statement.len();
    let end = start + source[start..].find("};").expect("the import's end");
    for name in source[start..end]
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        assert!(
            ALLOWED.contains(&name),
            "model.rs imports {name} from conditional"
        );
    }
}

#[test]
fn model_constants_are_the_adrs() {
    assert_eq!(
        super::model::PADDING_BPS,
        i128::from(crate::location::TOLERANCE_BPS)
    );
    let ids: Vec<_> = crate::conditional::TRIGGER_IDS
        .iter()
        .map(|(key, _)| *key)
        .collect();
    assert_eq!(super::model::trigger_ids(), ids);
}
