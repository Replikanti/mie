//! Order-flow candidates (brief §8; Market State & Regime brief; ADR-051,
//! proposed).
//!
//! A candidate is a Market State fact composed from features that already
//! exist: a hypothesis for Conditional Behavior and research, never a
//! signal (ADR-012, ADR-013). Candidates carry no bias, direction, location
//! gate or execution meaning; the trigger rule of each
//! [`TriggerFamily`](crate::trigger::TriggerFamily) that builds on one is
//! owned by the bias/trigger issue. The only sided fields are facts: which
//! extreme ([`Side`](crate::structure::Side)), which taker side carried the
//! net aggression ([`Aggressor`](crate::event::Aggressor), ADR-023) and
//! which passive side.
//!
//! - `candidate.divergence.cvd.<tf>@1` and `candidate.divergence.oi.<tf>@1`
//!   ([`divergence`], decision 4): CVD and open-interest divergences on
//!   confirmed swings of every structure timeframe.
//!
//! Candidates are the last stage of an event (decision 2): they run after
//! the location stage, on the committed state, and cannot fail. A change
//! that does not fit its type makes that value `Unavailable(OutOfRange)`
//! and never rejects the event. Each emitted [`CandidateEvent`] carries its
//! inputs with their feature keys and a [`LocationContext`] (decision 3);
//! events are not hashed, the same as structure and location facts.

pub mod divergence;

mod context;

pub use context::{ContextLevel, ContextLevels, LocationContext};

use crate::bars::{Bar, Timeframe};
use crate::derivatives::Derivatives;
use crate::event::MarketEvent;
use crate::fingerprint::Fingerprinter;
use crate::flow::OrderFlow;
use crate::location::LocationState;
use crate::state_hash::StateEncode;
use crate::structure::{STRUCTURE_TIMEFRAMES, StructureEvent};
use crate::time::EventTime;
use divergence::{
    CvdComparison, DivergenceEvent, DivergenceTracker, Emitted, OiComparison, TimeframeDivergences,
};
use std::fmt;

/// The candidate features of the Market State (ADR-051 D1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidates {
    /// The divergence features of every structure timeframe, shortest
    /// first.
    divergences: [TimeframeDivergences; 4],
}

impl StateEncode for Candidates {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { divergences } = self;
        divergences.encode(f);
    }
}

impl Default for Candidates {
    fn default() -> Self {
        Self::new()
    }
}

impl Candidates {
    /// Every feature warming up.
    pub fn new() -> Self {
        Self {
            divergences: STRUCTURE_TIMEFRAMES.map(|_| TimeframeDivergences::new()),
        }
    }

    /// The divergence features of `timeframe`, if it is a structure
    /// timeframe.
    pub fn divergences(&self, timeframe: Timeframe) -> Option<&TimeframeDivergences> {
        STRUCTURE_TIMEFRAMES
            .iter()
            .position(|candidate| *candidate == timeframe)
            .map(|index| &self.divergences[index])
    }

    /// The divergence features of `timeframe`, for tests that build a
    /// state by hand.
    #[cfg(test)]
    pub(crate) fn divergences_mut(
        &mut self,
        timeframe: Timeframe,
    ) -> Option<&mut TimeframeDivergences> {
        STRUCTURE_TIMEFRAMES
            .iter()
            .position(|candidate| *candidate == timeframe)
            .map(|index| &mut self.divergences[index])
    }

    /// The divergence features of every structure timeframe, shortest
    /// first.
    pub fn iter_divergences(&self) -> impl Iterator<Item = (Timeframe, &TimeframeDivergences)> {
        STRUCTURE_TIMEFRAMES.into_iter().zip(&self.divergences)
    }
}

/// A candidate fact, in emission order (ADR-051 D2): per closed bar in
/// close order, per swing it confirmed, the CVD comparison before the OI
/// one.
///
/// Not hashed. `Display` prints the canonical line the golden tests pin:
/// the kind, then the fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateEvent {
    /// A CVD divergence: `candidate.divergence.cvd.<tf>@1`.
    CvdDivergence(DivergenceEvent<CvdComparison>),
    /// An open-interest divergence: `candidate.divergence.oi.<tf>@1`.
    OiDivergence(DivergenceEvent<OiComparison>),
}

impl CandidateEvent {
    /// When the fact became visible: the time of the event that emitted it.
    pub fn time(&self) -> EventTime {
        match self {
            Self::CvdDivergence(event) => event.known_at,
            Self::OiDivergence(event) => event.known_at,
        }
    }

    /// The feature that produced the fact.
    pub fn feature(&self) -> crate::feature::FeatureKey {
        match self {
            Self::CvdDivergence(event) => event.feature,
            Self::OiDivergence(event) => event.feature,
        }
    }

    /// Where the fact happened.
    pub fn context(&self) -> &LocationContext {
        match self {
            Self::CvdDivergence(event) => &event.context,
            Self::OiDivergence(event) => &event.context,
        }
    }
}

impl fmt::Display for CandidateEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CvdDivergence(event) => write!(f, "cvd_divergence {event}"),
            Self::OiDivergence(event) => write!(f, "oi_divergence {event}"),
        }
    }
}

/// The committed state the candidate stage reads.
pub(crate) struct Inputs<'a> {
    /// The order flow: `flow.cvd.continuous@1`.
    pub(crate) flow: &'a OrderFlow,
    /// The derivatives context: `derivatives.oi.5m@1`.
    pub(crate) derivatives: &'a Derivatives,
    /// Location, for the context of each event (decision 3).
    pub(crate) location: &'a LocationState,
}

/// The candidate engine state (ADR-051 D2). Engine state, updated in place;
/// the Market State exposes only [`Candidates`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateTracker {
    /// One per structure timeframe, shortest first.
    divergences: [DivergenceTracker; 4],
    /// Scratch buffer for one swing's comparisons.
    emitted: Vec<Emitted>,
}

impl Default for CandidateTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CandidateTracker {
    /// An empty tracker.
    pub(crate) fn new() -> Self {
        Self {
            divergences: [0, 1, 2, 3].map(DivergenceTracker::new),
            emitted: Vec::new(),
        }
    }

    /// The infallible stage (decision 2): after every family and location
    /// have committed `event`, walks the bars it closed in close order,
    /// handling after each one the swings it confirmed, then records the
    /// event's trade at the developing bars' extremes. Updates `candidates`
    /// and appends the facts to `events`.
    pub(crate) fn detect(
        &mut self,
        event: &MarketEvent,
        closed: &[Bar],
        structure_events: &[StructureEvent],
        inputs: Inputs<'_>,
        candidates: &mut Candidates,
        events: &mut Vec<CandidateEvent>,
    ) {
        for bar in closed {
            let Some(position) = self
                .divergences
                .iter()
                .position(|tracker| tracker.timeframe() == bar.timeframe)
            else {
                continue;
            };
            let tracker = &mut self.divergences[position];
            tracker.close(bar);
            for fact in structure_events {
                let StructureEvent::Swing(swing) = fact else {
                    continue;
                };
                if swing.timeframe != bar.timeframe || swing.confirmed_bar_end != bar.end() {
                    continue;
                }
                tracker.confirm(
                    swing,
                    &inputs,
                    &mut candidates.divergences[position],
                    &mut self.emitted,
                );
                events.extend(self.emitted.drain(..).map(|emitted| match emitted {
                    Emitted::Cvd(event) => CandidateEvent::CvdDivergence(event),
                    Emitted::Oi(event) => CandidateEvent::OiDivergence(event),
                }));
            }
        }
        if let MarketEvent::Trade(trade) = event
            && let Some(cvd) = inputs.flow.cvd.ready()
        {
            let oi = inputs.derivatives.oi_5m.ready();
            for tracker in &mut self.divergences {
                tracker.trade(trade, cvd, oi);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::tests::Lcg;
    use crate::event::samples::t;
    use crate::event::{Aggressor, FeedGap, GapReason, OpenInterest, Stream, Trade};
    use crate::feature::{FeatureDefinition, FeatureKey, FeatureValue, catalog};
    use crate::fingerprint::Fingerprinter;
    use crate::num::{Price, Qty, SCALE};
    use crate::state::MarketStateEngine;

    const DAY: i64 = 86_400_000;

    /// The candidate golden tape: 13:00 UTC on day 0 to 00:00 UTC on day
    /// 45. A random walk in cents with drifting trends and trades 20 s to
    /// 160 s apart, as the structure golden tape; open interest on every
    /// 5-minute boundary, a random walk of its own; a trades gap on day 9
    /// and no open interest for two hours of day 20.
    fn golden_tape() -> Vec<MarketEvent> {
        let mut lcg = Lcg(0x6d69_6500_0000_0022);
        let mut events = Vec::new();
        let mut time = 13 * 3_600_000;
        let mut cents: i64 = 62_000 * 100;
        let mut trend = 0;
        let mut trade_id = 0;
        let mut gapped = false;
        let mut boundary = (time / 300_000 + 1) * 300_000;
        let mut oi: i64 = 80_000 * SCALE;
        let gap_start = 9 * DAY + 10 * 3_600_000;
        let gap_end = gap_start + 45 * 60_000;
        let oi_hole = (20 * DAY + 3_600_000)..(20 * DAY + 3 * 3_600_000);
        while time < 45 * DAY {
            time += 20_000 + lcg.below(140_000);
            while boundary < time {
                oi += (lcg.below(2_001) - 1_000) * 10_000_000;
                if !oi_hole.contains(&boundary) {
                    events.push(MarketEvent::OpenInterest(OpenInterest {
                        time: t(boundary),
                        open_interest: Qty::from_units(oi),
                        resolution_ms: 300_000,
                    }));
                }
                boundary += 300_000;
            }
            if !gapped && time >= gap_start {
                events.push(MarketEvent::FeedGap(FeedGap {
                    stream: Stream::Trades,
                    start: t(gap_start),
                    end: t(gap_end),
                    reason: GapReason::Disconnected,
                }));
                time = gap_end + 1 + lcg.below(30_000);
                boundary = (time / 300_000 + 1) * 300_000;
                gapped = true;
            }
            if lcg.below(400) == 0 {
                trend = lcg.below(7) - 3;
            }
            cents += trend + lcg.below(1_001) - 500;
            trade_id += 1;
            events.push(MarketEvent::Trade(Trade {
                time: t(time),
                trade_id,
                price: Price::from_units(cents * (SCALE / 100)),
                qty: Qty::from_units(1 + lcg.below(200_000_000)),
                aggressor: if lcg.below(2) == 0 {
                    Aggressor::Buy
                } else {
                    Aggressor::Sell
                },
            }));
        }
        events
    }

    /// The golden lines of every candidate feature on the golden tape, in
    /// tape order: `<event time> value <value>` whenever the feature's value
    /// changes, and `<event time> <fact>` for each fact it emits. Replayed
    /// once and shared by the golden tests of all eight ids (a test-only
    /// cache: the tape takes seconds to replay in a debug build).
    fn golden_lines() -> &'static [(FeatureKey, String)] {
        static LINES: std::sync::OnceLock<Vec<(FeatureKey, String)>> = std::sync::OnceLock::new();
        LINES.get_or_init(replay_golden)
    }

    /// Replays the golden tape into its golden lines ([`golden_lines`]).
    fn replay_golden() -> Vec<(FeatureKey, String)> {
        let mut engine = MarketStateEngine::new();
        let mut lines = Vec::new();
        let mut before = engine.state().candidates.clone();
        for event in golden_tape() {
            engine.apply(&event).unwrap();
            let time = event.time();
            let after = &engine.state().candidates;
            for ((timeframe, old), (_, new)) in
                before.iter_divergences().zip(after.iter_divergences())
            {
                let (_, cvd) = catalog::CANDIDATE_DIVERGENCE_CVD
                    .into_iter()
                    .find(|(candidate, _)| *candidate == timeframe)
                    .unwrap();
                let (_, oi) = catalog::CANDIDATE_DIVERGENCE_OI
                    .into_iter()
                    .find(|(candidate, _)| *candidate == timeframe)
                    .unwrap();
                if old.cvd != new.cvd {
                    lines.push((cvd.key, format!("{time} value {}", shown(&new.cvd))));
                }
                if old.oi != new.oi {
                    lines.push((oi.key, format!("{time} value {}", shown(&new.oi))));
                }
            }
            for fact in engine.candidate_events() {
                lines.push((fact.feature(), format!("{time} {fact}")));
            }
            before = after.clone();
        }
        lines
    }

    /// A value as a golden line shows it.
    fn shown<T: fmt::Display>(value: &FeatureValue<T>) -> String {
        match value {
            FeatureValue::Ready(value) => value.to_string(),
            FeatureValue::WarmingUp { observed, required } => {
                format!("warming {observed}/{required}")
            }
            FeatureValue::Unavailable { reason } => format!("unavailable {reason:?}"),
        }
    }

    /// The digest of `lines`.
    fn digest(lines: &[&str]) -> String {
        let mut hasher = Fingerprinter::new();
        hasher.write_len(lines.len());
        for line in lines {
            hasher.write_str(line);
        }
        hasher.finish().to_string()
    }

    /// Checks the golden of `feature`: the count of its lines, its first
    /// fact line verbatim (or its first value line, without facts), and
    /// the digest of all of them.
    fn assert_golden(feature: &FeatureDefinition, count: usize, first: &str, all: &str) {
        let own: Vec<&str> = golden_lines()
            .iter()
            .filter(|(key, _)| *key == feature.key)
            .map(|(_, line)| line.as_str())
            .collect();
        let shown = own
            .iter()
            .find(|line| !line.contains(" value "))
            .or(own.first())
            .copied()
            .unwrap_or_default();
        assert_eq!(shown, first, "{}", feature.key);
        assert_eq!(
            (own.len(), digest(&own).as_str()),
            (count, all),
            "{}",
            feature.key
        );
    }

    #[test]
    fn golden_candidate_divergence_cvd_15m_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_CVD_15M_V1,
            961,
            GOLDEN_CVD_15M,
            "4f4d7236e5ef3d6e",
        );
    }

    #[test]
    fn golden_candidate_divergence_cvd_1h_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_CVD_1H_V1,
            233,
            GOLDEN_CVD_1H,
            "8f8c74676e526e86",
        );
    }

    #[test]
    fn golden_candidate_divergence_cvd_4h_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_CVD_4H_V1,
            53,
            GOLDEN_CVD_4H,
            "4fc7cb36282c4d69",
        );
    }

    #[test]
    fn golden_candidate_divergence_cvd_1d_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_CVD_1D_V1,
            4,
            GOLDEN_CVD_1D,
            "9284f60919fa13b1",
        );
    }

    #[test]
    fn golden_candidate_divergence_oi_15m_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_OI_15M_V1,
            984,
            GOLDEN_OI_15M,
            "cbf50ccc3590c65c",
        );
    }

    #[test]
    fn golden_candidate_divergence_oi_1h_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_OI_1H_V1,
            234,
            GOLDEN_OI_1H,
            "206e41e0e7be6838",
        );
    }

    #[test]
    fn golden_candidate_divergence_oi_4h_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_OI_4H_V1,
            53,
            GOLDEN_OI_4H,
            "8c76cfff3a887542",
        );
    }

    #[test]
    fn golden_candidate_divergence_oi_1d_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_OI_1D_V1,
            4,
            GOLDEN_OI_1D,
            "520811c85c537e3e",
        );
    }

    #[test]
    fn the_same_tape_gives_the_same_facts_and_hashes() {
        let run = |events: &[MarketEvent]| {
            let mut engine = MarketStateEngine::new();
            let mut facts = Vec::new();
            let mut hashes = Vec::new();
            for event in events {
                engine.apply(event).unwrap();
                facts.extend(engine.candidate_events().iter().map(ToString::to_string));
                hashes.push(engine.state().state_hash());
            }
            (facts, hashes)
        };
        let tape = crate::bars::tests::random_tape(0x6d69_6500_0000_0022, 20_000);
        let (facts, hashes) = run(&tape);
        assert!(!facts.is_empty());
        assert_eq!(run(&tape), (facts, hashes));
    }

    #[test]
    fn a_rejected_event_leaves_the_candidates_and_their_facts() {
        use crate::candidate::divergence::tests::Scenario;
        use crate::state::StateError;
        let events = Scenario::divergent(crate::structure::Side::High).tape();
        let mut engine = MarketStateEngine::new();
        for event in &events {
            engine.apply(event).unwrap();
        }
        // The confirming trade emitted the divergence.
        let facts = engine.candidate_events().to_vec();
        assert!(!facts.is_empty());
        let candidates = engine.state().candidates.clone();
        // A trade whose volume overflows the developing bars.
        let last = events.last().unwrap().time().as_millis();
        let overflow = MarketEvent::Trade(Trade {
            time: t(last + 1),
            trade_id: 1_000,
            price: Price::from_units(60_000 * SCALE),
            qty: Qty::from_units(i64::MAX),
            aggressor: Aggressor::Buy,
        });
        assert_eq!(
            engine.apply(&overflow),
            Err(StateError::Overflow {
                event: overflow.canonical_key()
            })
        );
        assert_eq!(engine.candidate_events(), facts.as_slice());
        assert_eq!(engine.state().candidates, candidates);
    }

    #[test]
    fn no_line_names_a_direction_or_an_execution_meaning() {
        // Candidates are facts (ADR-012, ADR-023, ADR-051): no fact or
        // value line may read as a direction or a trade instruction.
        let forbidden = [
            "bull",
            "bear",
            "long",
            "short",
            "signal",
            "reversal",
            "continuation",
        ];
        let mut lines: Vec<&str> = golden_lines()
            .iter()
            .map(|(_, line)| line.as_str())
            .collect();
        let samples: Vec<String> = [crate::structure::Side::High, crate::structure::Side::Low]
            .into_iter()
            .flat_map(|side| {
                let events = crate::candidate::divergence::tests::Scenario::divergent(side).tape();
                let mut engine = MarketStateEngine::new();
                crate::candidate::divergence::tests::run(&mut engine, &events)
                    .into_iter()
                    .map(|(_, fact)| fact.to_string())
            })
            .collect();
        assert!(!samples.is_empty());
        lines.extend(samples.iter().map(String::as_str));
        let kinds = lines
            .iter()
            .filter(|line| line.contains("cvd_divergence") || line.contains("oi_divergence"))
            .count();
        assert!(kinds > 0);
        for line in lines {
            let lower = line.to_lowercase();
            for word in forbidden {
                assert!(!lower.contains(word), "{word}: {line}");
            }
        }
    }

    #[test]
    fn the_location_levels_v1_engine_still_builds() {
        // ADR-051 D3: no candidate declares a location input, so the
        // `location.levels@1` selection keeps its closure (ADR-044 D10).
        for definition in catalog::CANDIDATE_DIVERGENCE_CVD
            .into_iter()
            .chain(catalog::CANDIDATE_DIVERGENCE_OI)
            .map(|(_, definition)| definition)
        {
            assert!(
                definition
                    .inputs
                    .iter()
                    .all(|input| !matches!(input, crate::feature::Input::Feature(key) if key.id.as_str().starts_with("location."))),
                "{}",
                definition.key
            );
        }
        let features = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key]).unwrap();
        assert!(
            features
                .definitions()
                .any(|definition| definition.key == catalog::CANDIDATE_DIVERGENCE_CVD_15M_V1.key)
        );
        let engine = MarketStateEngine::with_features(features.clone()).unwrap();
        assert_eq!(engine.state().feature_set, features.version());
    }

    #[test]
    #[ignore = "prints the golden lines: cargo test -p mie-domain print_candidate_golden -- --ignored --nocapture"]
    fn print_candidate_golden() {
        for definition in catalog::CANDIDATE_DIVERGENCE_CVD
            .into_iter()
            .chain(catalog::CANDIDATE_DIVERGENCE_OI)
            .map(|(_, definition)| definition)
        {
            let own: Vec<&str> = golden_lines()
                .iter()
                .filter(|(key, _)| *key == definition.key)
                .map(|(_, line)| line.as_str())
                .collect();
            let first = own
                .iter()
                .find(|line| !line.contains(" value "))
                .or(own.first())
                .copied()
                .unwrap_or_default();
            println!("{} {} {}\n{first}", definition.key, own.len(), digest(&own));
        }
    }

    const GOLDEN_CVD_15M: &str = "64809276ms cvd_divergence 15m low prev=61969.20000000@56332199ms cur=61966.54000000@61514129ms dprice=-2.66000000 cvd=-11.99608482->-9.75701857 dcvd=2.23906625 gaps=0 span=5181930ms complete divergent=true candidate.divergence.cvd.15m@1 ctx=61966.54000000 levels=sfp_zone:low@61969.20000000[61966.54000000,61969.20000000],prior_sweep:low@61969.20000000,vwap@61983.67574530 location.levels@2 auction=warming";

    const GOLDEN_CVD_1H: &str = "108035804ms cvd_divergence 1h low prev=61900.09000000@79003423ms cur=61882.07000000@95647782ms dprice=-18.02000000 cvd=30.34139291->45.29007073 dcvd=14.94867782 gaps=0 span=16644359ms complete divergent=true candidate.divergence.cvd.1h@1 ctx=61882.07000000 levels=structural_low:low@61882.07000000,sfp_zone:low@61900.09000000[61882.07000000,61900.09000000],prior_sweep:low@61900.09000000,prior_sweep:low@61900.09000000 location.levels@2 auction=acceptance";

    const GOLDEN_CVD_4H: &str = "374447759ms cvd_divergence 4h high prev=62057.32000000@185552262ms cur=62140.03000000@321483259ms dprice=82.71000000 cvd=60.27519949->38.05121265 dcvd=-22.22398684 gaps=0 span=135930997ms complete divergent=true candidate.divergence.cvd.4h@1 ctx=62140.03000000 levels=sfp_zone:low@62108.54000000[62095.20000000,62108.54000000],vah@62110.00000000[62100.00000000,62110.00000000],sfp_zone:low@62110.52000000[62100.74000000,62110.52000000],prior_sweep:high@62103.29000000,prior_sweep:low@62108.54000000,prior_sweep:low@62110.52000000,vwap@62112.68118136,prior_sweep:high@62113.10000000,sfp_zone:high@62113.10000000[62113.10000000,62117.71000000],prior_sweep:high@62117.71000000,sfp_zone:high@62117.71000000[62117.71000000,62117.72000000],structural_high:high@62130.28000000,prior_sweep:high@62134.29000000,sfp_zone:high@62134.29000000[62134.29000000,62136.28000000],structural_high:high@62136.28000000,structural_high:high@62136.28000000,structural_high:high@62140.03000000,structural_high:high@62140.03000000 location.levels@2 auction=unavailable";

    const GOLDEN_CVD_1D: &str = "1209708595ms value high=- low=- candidate.divergence.cvd.1d@1";

    const GOLDEN_OI_15M: &str = "64809276ms oi_divergence 15m low prev=61969.20000000@56332199ms cur=61966.54000000@61514129ms dprice=-2.66000000 cvd=-11.99608482->-9.75701857 dcvd=2.23906625 gaps=0 span=5181930ms complete oi=80319.70000000->80067.90000000 doi=-251.80000000 divergent=true candidate.divergence.oi.15m@1 ctx=61966.54000000 levels=sfp_zone:low@61969.20000000[61966.54000000,61969.20000000],prior_sweep:low@61969.20000000,vwap@61983.67574530 location.levels@2 auction=warming";

    const GOLDEN_OI_1H: &str = "198042133ms oi_divergence 1h high prev=61947.27000000@107168767ms cur=62057.32000000@185552262ms dprice=110.05000000 cvd=63.52029271->60.27519949 dcvd=-3.24509322 gaps=0 span=78383495ms complete oi=80141.20000000->79555.50000000 doi=-585.70000000 divergent=true candidate.divergence.oi.1h@1 ctx=62057.32000000 levels=prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62030.03772028,prior_sweep:high@62033.71000000,structural_high:high@62049.17000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000],structural_high:high@62057.32000000 location.levels@2 auction=outside_value";

    const GOLDEN_OI_4H: &str = "374447759ms oi_divergence 4h high prev=62057.32000000@185552262ms cur=62140.03000000@321483259ms dprice=82.71000000 cvd=60.27519949->38.05121265 dcvd=-22.22398684 gaps=0 span=135930997ms complete oi=79555.50000000->79417.30000000 doi=-138.20000000 divergent=true candidate.divergence.oi.4h@1 ctx=62140.03000000 levels=sfp_zone:low@62108.54000000[62095.20000000,62108.54000000],vah@62110.00000000[62100.00000000,62110.00000000],sfp_zone:low@62110.52000000[62100.74000000,62110.52000000],prior_sweep:high@62103.29000000,prior_sweep:low@62108.54000000,prior_sweep:low@62110.52000000,vwap@62112.68118136,prior_sweep:high@62113.10000000,sfp_zone:high@62113.10000000[62113.10000000,62117.71000000],prior_sweep:high@62117.71000000,sfp_zone:high@62117.71000000[62117.71000000,62117.72000000],structural_high:high@62130.28000000,prior_sweep:high@62134.29000000,sfp_zone:high@62134.29000000[62134.29000000,62136.28000000],structural_high:high@62136.28000000,structural_high:high@62136.28000000,structural_high:high@62140.03000000,structural_high:high@62140.03000000 location.levels@2 auction=unavailable";

    const GOLDEN_OI_1D: &str = "1209708595ms value high=- low=- candidate.divergence.oi.1d@1";
}
