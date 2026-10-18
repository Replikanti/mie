//! The ADR-044 measurements over the archive backfill (not run in CI): an
//! `ArchiveReplay` of aggTrades over a window read from the environment
//! drives two `MarketStateEngine`s over the same events — the default one,
//! with `location.levels@2` over the 7-day composite, and one built with
//! `MarketStateEngine::with_features` for `location.levels@1` over the
//! 5-day composite (ADR-044 D10, #96) — and the exact location logic
//! prints:
//!
//! - **M1**, the 1m close-to-close move `|Δclose|` in bps of the previous
//!   close, from the engine's closed 1m bars with trades: p50, p75, p90,
//!   p95, p99 per half of the window and overall; the share of closes in
//!   the edge bands; the prior-day value-area width in bps (p10, p50, p90,
//!   minimum); the days whose first close lies beyond an edge band.
//! - **M2**, the breakout table from the exact `location.auction.prior_day@1`
//!   classifier ([`AuctionClassifier`]) with acceptance switched off, for
//!   `w` ∈ {3, 5, 6, 8} bps, per half: breakouts (probes out of value) and how
//!   they ended — failed (a close back in value, the same UTC day: the
//!   classifier resets at each prior day), survived to the day's end, or
//!   retargeted; the failure hazard per counted close over intervals from 1
//!   to 360 closes; and per `N` the share of failures already failed before
//!   `N` counted closes ("caught": labelled `FailedBreakout` at
//!   `N_acc = N`) and the share of breakouts that held `N` closes and still
//!   failed ("held, then returned").
//! - **M3**, per registry version (`@1`, `@2`): the registry size per kind
//!   (p50, p99 over the closed minutes where it was rebuilt) and the zone
//!   events per day by cause.
//! - **M4**, the share of closes in each `AuctionState` per half at the
//!   engine's fixed `w` and `N_acc` (cross-checked against the engine's own
//!   state), and the state transitions per day.
//! - The #96 checks of `@2` against `@1`, each a count asserted zero:
//!   **isolation** (rule I), events after which the two states differ
//!   anywhere but in the feature-set stamp, the registry and the candidate
//!   trigger values its location gate reads it for (ADR-053 D4), or the two
//!   engines' auction facts differ, or that only one engine rejected;
//!   **sourcing** (rule S), rebuilds where a registry holds a level of the
//!   other version's composite, or fewer than 3 levels of its own composite
//!   while that is ready, or any while it is not.
//! - The replay's wall time, events and domain rejections of each engine.
//!
//! M1, M2 and M4 come from the `@2` engine; they read only the prior-day
//! profile, which both engines share.
//!
//! Run it with
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_FROM=<YYYY-MM-DD>
//! MIE_MEASURE_TO=<YYYY-MM-DD> cargo test --release -p mie-cli --test
//! location_measure -- --ignored --nocapture`; `MIE_MEASURE_TO` is
//! included, as in `mie replay`.

use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::replay::parse_bound;
use mie_domain::bars::Timeframe;
use mie_domain::feature::{FeatureKey, FeatureValue, catalog};
use mie_domain::location::{
    ACCEPTANCE_CLOSES, AuctionClassifier, AuctionState, AuctionStatus, EnterCause, LeaveCause,
    LevelKind, LocationEvent, LocationState, Position, Region, TOLERANCE_BPS,
};
use mie_domain::profile::{VolumeProfile, VolumeProfiles};
use mie_domain::state::{MarketState, MarketStateEngine};
use mie_domain::time::EventTime;
use mie_ports::outbound::{HistoricalDataProvider, MarketDataProvider, ReplayWindow};
use std::time::Instant;

const DAY_MS: i64 = 86_400_000;

/// The tolerances M2 compares, in bps.
const TOLERANCES: [i64; 4] = [3, 5, 6, 8];

/// The `N` rows of the M2 table.
const ROWS: [u32; 14] = [1, 2, 3, 5, 10, 15, 30, 45, 60, 90, 120, 180, 240, 360];

/// The hazard intervals of M2, `[from, to)` in counted closes.
const INTERVALS: [(u32, u32); 15] = [
    (1, 2),
    (2, 3),
    (3, 4),
    (4, 5),
    (5, 6),
    (6, 10),
    (10, 15),
    (15, 30),
    (30, 45),
    (45, 60),
    (60, 90),
    (90, 120),
    (120, 180),
    (180, 360),
    (360, u32::MAX),
];

const KINDS: [LevelKind; 12] = [
    LevelKind::Poc,
    LevelKind::Vah,
    LevelKind::Val,
    LevelKind::Hvn,
    LevelKind::Lvn,
    LevelKind::StructuralHigh,
    LevelKind::StructuralLow,
    LevelKind::LiquidityCluster,
    LevelKind::PriorSweep,
    LevelKind::SfpRejectionZone,
    LevelKind::Vwap,
    LevelKind::ValidatedReference,
];

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

fn quantile(values: &mut [f64], q: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    values.sort_by(f64::total_cmp);
    let rank = ((values.len() - 1) as f64 * q).round() as usize;
    values[rank]
}

fn quantiles(values: &mut [f64]) -> String {
    let qs: Vec<String> = [0.5, 0.75, 0.9, 0.95, 0.99]
        .iter()
        .map(|q| format!("{:.2}", quantile(values, *q)))
        .collect();
    format!("p50/p75/p90/p95/p99 {} (n {})", qs.join("/"), values.len())
}

fn pct(part: u64, whole: u64) -> String {
    if whole == 0 {
        return "-".to_owned();
    }
    format!("{:.2} %", 100.0 * part as f64 / whole as f64)
}

/// How a breakout ended.
#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    /// A close back in the accepted region.
    Failed,
    /// The day ended (the classifier reset) with the probe active.
    Survived,
    /// A close in the other outside region retargeted the probe.
    Retargeted,
}

/// One breakout: its half, its counted closes when it ended, how it ended.
struct Episode {
    half: usize,
    closes: u32,
    end: End,
}

/// One M2 classifier with acceptance switched off.
struct Breakouts {
    tolerance_bps: i64,
    classifier: AuctionClassifier,
    last: Option<AuctionStatus>,
    episodes: Vec<Episode>,
}

impl Breakouts {
    fn new(tolerance_bps: i64) -> Self {
        Self {
            tolerance_bps,
            classifier: AuctionClassifier::new(tolerance_bps, u32::MAX),
            last: None,
            episodes: Vec::new(),
        }
    }

    /// Records how the previous close's breakout, if any, ended at `now`.
    fn close(&mut self, now: &AuctionStatus, mid: i64) {
        let Some(previous) = self.last.and_then(|last| last.probe) else {
            return;
        };
        if previous.toward == Region::In {
            return;
        }
        let half = usize::from(previous.since.as_millis() >= mid);
        let end = match (now.probe, now.failure) {
            (Some(probe), _) if probe.since == previous.since => return,
            (_, Some(failure)) if failure.at == now.known_at => End::Failed,
            _ => End::Retargeted,
        };
        self.episodes.push(Episode {
            half,
            closes: previous.closes,
            end,
        });
    }

    /// Records a breakout still active when the reference changed.
    fn reset(&mut self, mid: i64) {
        if let Some(probe) = self.last.and_then(|last| last.probe)
            && probe.toward != Region::In
        {
            self.episodes.push(Episode {
                half: usize::from(probe.since.as_millis() >= mid),
                closes: probe.closes,
                end: End::Survived,
            });
        }
        self.last = None;
    }

    fn print(&self) {
        for half in [Some(0), Some(1), None] {
            let episodes: Vec<&Episode> = self
                .episodes
                .iter()
                .filter(|episode| half.is_none_or(|half| episode.half == half))
                .collect();
            let count = |end: End| episodes.iter().filter(|e| e.end == end).count() as u64;
            let total = episodes.len() as u64;
            let failed = count(End::Failed);
            println!(
                "  w={} bps, {}: breakouts {total}, failed same day {} ({}), survived the day {}, \
                 retargeted {}",
                self.tolerance_bps,
                half_label(half),
                failed,
                pct(failed, total),
                count(End::Survived),
                count(End::Retargeted)
            );
            // Episodes still at risk at counted close n: closes ≥ n.
            let at_risk = |n: u32| episodes.iter().filter(|e| e.closes >= n).count() as u64;
            let failed_at = |n: u32| {
                episodes
                    .iter()
                    .filter(|e| e.end == End::Failed && e.closes == n)
                    .count() as u64
            };
            let mut hazards = Vec::new();
            for (from, to) in INTERVALS {
                let last = to.min(from.saturating_add(4_000));
                let (mut fails, mut exposure) = (0, 0);
                for n in from..last {
                    fails += failed_at(n);
                    exposure += at_risk(n);
                }
                let label = if to == u32::MAX {
                    format!("{from}+")
                } else if to == from + 1 {
                    format!("{from}")
                } else {
                    format!("{from}-{}", to - 1)
                };
                hazards.push(format!("{label}: {}", pct(fails, exposure)));
            }
            println!("    hazard per close: {}", hazards.join(", "));
            for n in ROWS {
                let caught = episodes
                    .iter()
                    .filter(|e| e.end == End::Failed && e.closes < n)
                    .count() as u64;
                let held = episodes.iter().filter(|e| e.closes >= n).count() as u64;
                let returned = episodes
                    .iter()
                    .filter(|e| e.end == End::Failed && e.closes >= n)
                    .count() as u64;
                println!(
                    "    N={n:>3}: failures caught before N {} ; held N, then returned {} ({returned}/{held})",
                    pct(caught, failed),
                    pct(returned, held)
                );
            }
        }
    }
}

fn half_label(half: Option<usize>) -> &'static str {
    match half {
        Some(0) => "first half",
        Some(_) => "second half",
        None => "whole window",
    }
}

/// Whether `a` and `b` are equal in every field but the feature-set stamp
/// and the level registry (#96 rule I). Exhaustive, so a new field does not
/// compile until it is compared. Of the triggers it compares the bias and
/// the auction and SFP values only: the candidate trigger values depend on
/// the selected registry by design, since their location gate reads it
/// (ADR-053 D4).
fn equal_but_registry(a: &MarketState, b: &MarketState) -> bool {
    let MarketState {
        feature_set: _,
        as_of,
        last_trade_price,
        bars,
        motion,
        atr,
        regime,
        flow,
        profile,
        structure,
        derivatives,
        book,
        location,
        trade_count,
        candidates,
        triggers,
    } = a;
    let LocationState {
        vwap,
        levels: _,
        auction,
    } = location;
    *as_of == b.as_of
        && *last_trade_price == b.last_trade_price
        && *bars == b.bars
        && *motion == b.motion
        && *atr == b.atr
        && *regime == b.regime
        && *flow == b.flow
        && *profile == b.profile
        && *structure == b.structure
        && *derivatives == b.derivatives
        && *book == b.book
        && *vwap == b.location.vwap
        && *auction == b.location.auction
        && *trade_count == b.trade_count
        && *candidates == b.candidates
        && triggers.bias() == b.triggers.bias()
        && triggers.iter_auction().eq(b.triggers.iter_auction())
        && triggers.iter_sfp().eq(b.triggers.iter_sfp())
}

/// The auction facts of the last event.
fn auction_facts(engine: &MarketStateEngine) -> impl Iterator<Item = &LocationEvent> {
    engine
        .location_events()
        .iter()
        .filter(|fact| matches!(fact, LocationEvent::Auction { .. }))
}

/// One registry version: its engine and its M3 and rule-S counts.
struct Registry {
    label: &'static str,
    engine: MarketStateEngine,
    /// The composite this version reads, and the one it must not.
    own: FeatureKey,
    other: FeatureKey,
    composite: fn(&VolumeProfiles) -> &FeatureValue<VolumeProfile>,
    rejections: u64,
    /// Per kind, then all kinds: the size at each closed minute where it
    /// was rebuilt (the unit ADR-044 M3 recorded `@1` in).
    sizes: Vec<Vec<f64>>,
    minutes: u64,
    /// Entered: arrived, created. Left: departed, retired.
    entered: [u64; 2],
    left: [u64; 2],
    rebuilds: u64,
    /// Rebuilds that break rule S.
    sourcing: u64,
}

impl Registry {
    fn new(
        label: &'static str,
        engine: MarketStateEngine,
        own: FeatureKey,
        other: FeatureKey,
        composite: fn(&VolumeProfiles) -> &FeatureValue<VolumeProfile>,
    ) -> Self {
        Self {
            label,
            engine,
            own,
            other,
            composite,
            rejections: 0,
            sizes: vec![Vec::new(); KINDS.len() + 1],
            entered: [0; 2],
            left: [0; 2],
            minutes: 0,
            rebuilds: 0,
            sourcing: 0,
        }
    }

    /// M3 and rule S after an accepted event at `known_at`.
    fn observe(&mut self, known_at: EventTime) {
        let state = self.engine.state();
        if let FeatureValue::Ready(set) = &state.location.levels
            && set.known_at == known_at
        {
            // M3 samples once per closed minute, as ADR-044 M3 did; rule S
            // checks every rebuild.
            if !self.engine.closed_bars().is_empty() {
                let mut counts = [0_u32; KINDS.len()];
                for level in set.levels() {
                    counts[KINDS.iter().position(|kind| *kind == level.kind).unwrap()] += 1;
                }
                for (slot, count) in counts.iter().enumerate() {
                    self.sizes[slot].push(f64::from(*count));
                }
                self.sizes[KINDS.len()].push(set.levels().len() as f64);
                self.minutes += 1;
            }
            self.rebuilds += 1;
            let from = |source: FeatureKey| {
                set.levels()
                    .iter()
                    .filter(|level| level.source == source)
                    .count()
            };
            let own = from(self.own);
            let sourced = if (self.composite)(&state.profile).ready().is_some() {
                own >= 3
            } else {
                own == 0
            };
            if !sourced || from(self.other) != 0 {
                self.sourcing += 1;
            }
        }
        for fact in self.engine.location_events() {
            match fact {
                LocationEvent::Entered { cause, .. } => {
                    self.entered[usize::from(*cause == EnterCause::Created)] += 1;
                }
                LocationEvent::Left { cause, .. } => {
                    self.left[usize::from(*cause == LeaveCause::Retired)] += 1;
                }
                LocationEvent::Auction { .. } => {}
            }
        }
    }

    /// The M3 lines.
    fn print(&mut self, days: f64) {
        println!(
            "M3: {} registry size per kind over {} closed minutes (p50 / p99)",
            self.label, self.minutes
        );
        let mut columns = Vec::new();
        for (slot, values) in self.sizes.iter_mut().enumerate() {
            let name = KINDS
                .get(slot)
                .map_or("all".to_owned(), ToString::to_string);
            columns.push(format!(
                "{name} {:.0}/{:.0}",
                quantile(values, 0.5),
                quantile(values, 0.99)
            ));
        }
        println!("  {}", columns.join(", "));
        println!(
            "  zone events per day: entered arrived {:.1}, created {:.1}; left departed {:.1}, retired {:.1}",
            self.entered[0] as f64 / days,
            self.entered[1] as f64 / days,
            self.left[0] as f64 / days,
            self.left[1] as f64 / days
        );
    }
}

#[test]
#[ignore = "measures the archive for ADR-044; see the module docs"]
fn measure_location() {
    let raw_root = env("MIE_MEASURE_RAW_ROOT");
    let from = parse_bound("MIE_MEASURE_FROM", &env("MIE_MEASURE_FROM"), false).unwrap();
    let to = parse_bound("MIE_MEASURE_TO", &env("MIE_MEASURE_TO"), true).unwrap();
    let mid = from + (to - from) / 2;
    let window = ReplayWindow {
        start: EventTime::from_millis(from),
        end: EventTime::from_millis(to),
    };
    let store = ParquetRawStore::new(&raw_root);
    // As measured: aggTrades as published, without the ADR-050 fold.
    let replay = ArchiveReplay::new(
        &store,
        "BTCUSDT",
        &[ArchiveStream::AggTrades],
        TradeCorrection::None,
    );
    let mut stream = replay.replay(window).unwrap().stream;
    let started = Instant::now();

    let levels_v1 = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key]).unwrap();
    let mut registries = [
        Registry::new(
            "location.levels@1",
            MarketStateEngine::with_features(levels_v1).unwrap(),
            catalog::PROFILE_VOLUME_COMPOSITE_5D_V1.key,
            catalog::PROFILE_VOLUME_COMPOSITE_7D_V1.key,
            |profiles| &profiles.composite_5d,
        ),
        Registry::new(
            "location.levels@2",
            MarketStateEngine::new(),
            catalog::PROFILE_VOLUME_COMPOSITE_7D_V1.key,
            catalog::PROFILE_VOLUME_COMPOSITE_5D_V1.key,
            |profiles| &profiles.composite_7d,
        ),
    ];
    let mut breakouts: Vec<Breakouts> = TOLERANCES.into_iter().map(Breakouts::new).collect();
    let mut labels = AuctionClassifier::new(TOLERANCE_BPS, ACCEPTANCE_CLOSES);
    let mut events = 0_u64;
    // Rule I: events after which the states or auction facts differ,
    // including events only one engine rejected (`one_sided`).
    let mut isolation = 0_u64;
    let mut one_sided = 0_u64;
    // M1.
    let mut moves: [Vec<f64>; 2] = [Vec::new(), Vec::new()];
    let mut previous_close: Option<f64> = None;
    let mut widths = Vec::new();
    let mut edge_closes = [0_u64; 2];
    let mut outside_at_open = 0_u64;
    let mut opens = 0_u64;
    // M4.
    let mut states = [[0_u64; 7]; 2];
    let mut transitions = [0_u64; 2];
    let mut closes = [0_u64; 2];
    let mut mismatches = 0_u64;

    while let Some(event) = stream.next_event().unwrap() {
        events += 1;
        let mut accepted = [false; 2];
        for (registry, accepted) in registries.iter_mut().zip(&mut accepted) {
            *accepted = registry.engine.apply(&event).is_ok();
            if !*accepted {
                registry.rejections += 1;
            }
        }
        // Rule I: an event only one engine rejected is a divergence where it
        // happens, even if nothing later shows it.
        if accepted[0] != accepted[1] {
            one_sided += 1;
            isolation += 1;
        }
        if accepted != [true, true] {
            continue;
        }
        let known_at = event.time();
        for registry in &mut registries {
            registry.observe(known_at);
        }
        let [v1, v2] = &registries;
        let engine = &v2.engine;
        let closed = engine.closed_bars();
        if closed.is_empty() {
            continue;
        }
        // Rule I, after every event that closed a bar.
        if v1.engine.closed_bars() != closed
            || !equal_but_registry(v1.engine.state(), engine.state())
            || !auction_facts(&v1.engine).eq(auction_facts(engine))
        {
            isolation += 1;
        }
        let state = engine.state();
        for bar in closed {
            if bar.timeframe == Timeframe::M1
                && let Some(ohlc) = bar.ohlc
            {
                let half = usize::from(bar.open_time.as_millis() >= mid);
                let close = ohlc.close.units() as f64;
                if let Some(previous) = previous_close {
                    moves[half].push((close - previous).abs() * 10_000.0 / previous);
                }
                previous_close = Some(close);
            }
            if bar.timeframe == Timeframe::D1
                && let FeatureValue::Ready(prior) = &state.profile.prior_day
            {
                let (val, vah) = (prior.val.units() as f64, prior.vah.units() as f64);
                widths.push((vah - val) * 10_000.0 / val);
            }
        }
        // M2: the classifiers without acceptance, driven as the engine
        // drives its own.
        for tracker in &mut breakouts {
            let mut seen = Vec::new();
            tracker
                .classifier
                .step(closed, &state.profile.prior_day, known_at, |classified| {
                    seen.push(classified.status);
                });
            for status in seen {
                tracker.close(&status, mid);
                tracker.last = Some(status);
            }
            // A new prior day: the status warms up (or is unavailable)
            // until its first close.
            if tracker.classifier.status().ready().is_none() {
                tracker.reset(mid);
            }
        }
        // M4: the engine's constants.
        let mut first_close = labels.status().ready().is_none();
        labels.step(closed, &state.profile.prior_day, known_at, |classified| {
            let status = classified.status;
            let half = usize::from(status.bar_end.as_millis() - 60_000 >= mid);
            let index = AuctionState::ALL
                .iter()
                .position(|s| *s == status.state)
                .unwrap();
            states[half][index] += 1;
            closes[half] += 1;
            if classified.from.is_some_and(|from| from != status.state) {
                transitions[half] += 1;
            }
            if matches!(status.position, Position::LowEdge | Position::HighEdge) {
                edge_closes[half] += 1;
            }
            if first_close {
                opens += 1;
                if status.accepted != Region::In {
                    outside_at_open += 1;
                }
                first_close = false;
            }
        });
        if let FeatureValue::Ready(status) = &state.location.auction
            && status.known_at == known_at
            && labels.status().ready() != Some(status)
        {
            mismatches += 1;
        }
    }
    let elapsed = started.elapsed();
    let days = (to - from) as f64 / DAY_MS as f64;

    println!(
        "window {} .. {} ({days:.0} days), halves split at {}",
        EventTime::from_millis(from),
        EventTime::from_millis(to),
        EventTime::from_millis(mid)
    );
    println!("M1: 1m |Δclose| in bps of the previous close");
    let mut all: Vec<f64> = moves.iter().flatten().copied().collect();
    for (half, values) in moves.iter_mut().enumerate() {
        println!("  {}: {}", half_label(Some(half)), quantiles(values));
    }
    println!("  {}: {}", half_label(None), quantiles(&mut all));
    for half in [0, 1] {
        println!(
            "  closes in the w={TOLERANCE_BPS} edge bands, {}: {}",
            half_label(Some(half)),
            pct(edge_closes[half], closes[half])
        );
    }
    let narrowest = widths.iter().copied().fold(f64::INFINITY, f64::min);
    println!(
        "  prior-day value-area width in bps: p10 {:.1}, p50 {:.1}, p90 {:.1}, min {narrowest:.1} ({} days)",
        quantile(&mut widths, 0.1),
        quantile(&mut widths, 0.5),
        quantile(&mut widths, 0.9),
        widths.len()
    );
    println!("  days whose first close lies beyond an edge band: {outside_at_open} of {opens}");
    println!("M2: breakouts (probes out of prior-day value), acceptance off");
    for tracker in &breakouts {
        tracker.print();
    }
    for registry in &mut registries {
        registry.print(days);
    }
    println!(
        "M4: share of closes per AuctionState (w={TOLERANCE_BPS} bps, N_acc={ACCEPTANCE_CLOSES})"
    );
    for half in [0, 1] {
        let shares: Vec<String> = AuctionState::ALL
            .iter()
            .zip(states[half])
            .map(|(state, count)| format!("{state} {} ({count})", pct(count, closes[half])))
            .collect();
        println!("  {}: {}", half_label(Some(half)), shares.join(", "));
        println!(
            "  {}: transitions per day {:.1}",
            half_label(Some(half)),
            transitions[half] as f64 / (days / 2.0)
        );
    }
    let missing: Vec<String> = [0, 1]
        .iter()
        .flat_map(|half| {
            AuctionState::ALL
                .iter()
                .zip(states[*half])
                .filter(|(_, count)| *count == 0)
                .map(move |(state, _)| format!("{state} in the {}", half_label(Some(*half))))
        })
        .collect();
    if missing.is_empty() {
        println!("  every AuctionState occurs in each half");
    } else {
        println!("  MISSING: {}", missing.join(", "));
    }
    println!("  engine/tool label mismatches: {mismatches}");
    println!("#96: @2 against @1");
    println!(
        "  isolation (rule I): {isolation} events where the states or auction facts differ \
         ({one_sided} of them rejected by one engine only)"
    );
    for registry in &registries {
        println!(
            "  sourcing (rule S), {}: {} of {} rebuilds break it",
            registry.label, registry.sourcing, registry.rebuilds
        );
    }
    let [v1, v2] = &registries;
    println!(
        "wall time {:.1} s, {events} events, domain rejections {} {}, {} {}",
        elapsed.as_secs_f64(),
        v1.label,
        v1.rejections,
        v2.label,
        v2.rejections
    );
    assert_eq!(
        mismatches, 0,
        "the tool's classifier disagrees with the engine"
    );
    assert_eq!(isolation, 0, "@2 differs from @1 beyond the registry");
    assert!(
        registries.iter().all(|registry| registry.sourcing == 0),
        "a registry reads the wrong composite"
    );
}
