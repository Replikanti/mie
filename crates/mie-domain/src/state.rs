//! Market State: the canonical deterministic representation of current
//! conditions (brief §8, Market State & Regime brief).
//!
//! The engine enforces the canonical event order (ADR-028), tracks the
//! latest trade, builds event-time bars on every timeframe (ADR-031,
//! [`bars`]), computes bar motion, ATR(14) and the volatility regime from
//! the bars each event closes (ADR-033, [`volatility`]), the order flow:
//! CVD and rolling aggression windows (ADR-035, [`flow`]), and the volume
//! profiles: developing UTC day, prior day, 5-day and 7-day composite
//! (ADR-036, [`profile`]), the market structure: swings, structural levels,
//! sweeps and SFPs on 15m, 1h, 4h and 1d (ADR-037, [`structure`]), and the
//! derivatives context: open interest, mark price and funding, and
//! liquidation windows (ADR-042, [`derivatives`]), and the order book: the
//! L2 book itself, banded depth and imbalance, liquidity flow windows and
//! concentration (ADR-043, [`liquidity`]), and location: the UTC-day VWAP
//! the level registry with its zone events, and the auction state against
//! the prior day's value (ADR-044, [`location`]), and the order-flow
//! candidates: CVD and open-interest divergences on confirmed swings,
//! effort vs result, exhaustion and absorption (ADR-051, [`candidate`]),
//! and the bias context and trigger rules (ADR-053, [`trigger`]); every
//! other kind passes through.
//! Further feature families are added by the Market State issues, each as a
//! registered, versioned definition (ADR-029, [`feature`]).
//! The engine computes one [`FeatureSet`] and stamps its
//! [`FeatureSetVersion`] on every state; each feature value carries its
//! validity ([`FeatureValue`]). The set is [`catalog::CURRENT`], or
//! [`catalog::CURRENT`] with an older version from [`catalog::SELECTABLE`]
//! ([`MarketStateEngine::with_features`], ADR-044 D10).
//!
//! [`bars`]: crate::bars
//! [`candidate`]: crate::candidate
//! [`derivatives`]: crate::derivatives
//! [`feature`]: crate::feature
//! [`flow`]: crate::flow
//! [`liquidity`]: crate::liquidity
//! [`location`]: crate::location
//! [`profile`]: crate::profile
//! [`structure`]: crate::structure
//! [`trigger`]: crate::trigger
//! [`volatility`]: crate::volatility

use crate::bars::{Bar, BarError, BarSet, MAX_BARS_PER_EVENT, Timeframe};
use crate::candidate::{self, CandidateEvent, CandidateTracker, Candidates};
use crate::derivatives::{Derivatives, DerivativesError, DerivativesTracker};
use crate::event::{MarketEvent, Stream};
use crate::feature::{FeatureKey, FeatureSet, FeatureSetVersion, FeatureValue, catalog};
use crate::flow::{FlowError, FlowTracker, OrderFlow};
use crate::liquidity::{BookState, LiquidityError, LiquidityTracker};
use crate::location::{
    self, LevelsVersion, LocationError, LocationEvent, LocationState, LocationTracker,
};
use crate::num::Price;
use crate::order::CanonicalKey;
use crate::profile::{ProfileError, ProfileTracker, VolumeProfiles};
use crate::regime::Regime;
use crate::structure::{
    LevelsRule, StructureError, StructureEvent, StructureSet, StructureTracker,
};
use crate::time::EventTime;
use crate::trigger::{self, TriggerEvent, TriggerTracker, Triggers};
use crate::volatility::{AtrRegimeSeries, MotionAnchors, MotionSet, VolatilityError};
use std::cmp::Ordering;
use std::fmt;

/// The market as known after the last consumed event.
///
/// [`MarketState::state_hash`] identifies it across processes, so live and
/// replay states can be compared (ADR-041, [`state_hash`]).
///
/// [`state_hash`]: crate::state_hash
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketState {
    /// Version of the feature set this state was computed with (ADR-029);
    /// experiments record it.
    pub feature_set: FeatureSetVersion,
    /// Ordering time (ADR-028) of the last consumed event; `None` before the
    /// first one.
    pub as_of: Option<EventTime>,
    /// Price of the last trade: `trade.last_price@1`
    /// ([`catalog::TRADE_LAST_PRICE_V1`]), warming up until the first trade.
    pub last_trade_price: FeatureValue<Price>,
    /// Event-time bars: `bars.time.<tf>@1` ([`catalog::BARS_TIME`]), the
    /// last closed and the developing bar of every timeframe. History is
    /// kept by consumers, from [`MarketStateEngine::closed_bars`].
    pub bars: BarSet,
    /// Motion of the last closed bar of every timeframe: `bars.motion.<tf>@1`
    /// ([`catalog::BARS_MOTION`]).
    pub motion: MotionSet,
    /// ATR(14) of 1h bars after the last closed one: `volatility.atr.1h@1`
    /// ([`catalog::VOLATILITY_ATR_1H_V1`]).
    pub atr: FeatureValue<Price>,
    /// The ATR-percentile regime after the last closed 1h bar:
    /// `volatility.regime.1h@1` ([`catalog::VOLATILITY_REGIME_1H_V1`]),
    /// label plus raw percentile. Context, never an entry signal (brief §9,
    /// ADR-012).
    pub regime: FeatureValue<Regime>,
    /// Order flow (ADR-035): `flow.cvd.continuous@1`, `flow.cvd.utc_day@1`
    /// and `flow.window.<5m|15m|1h>@1` ([`catalog::FLOW_WINDOWS`]) — CVD and
    /// rolling aggression over closed minutes. Aggression, not direction
    /// (ADR-023).
    pub flow: OrderFlow,
    /// Volume profiles (ADR-036): `profile.volume.utc_day@1`,
    /// `profile.volume.prior_day@1`, `profile.volume.composite_5d@1` and
    /// `profile.volume.composite_7d@1` — exact volume per 10 USDT bin with
    /// POC, value area, HVNs and LVNs.
    pub profile: VolumeProfiles,
    /// Market structure (ADR-037): `structure.swing.<tf>@1` and
    /// `structure.levels.<tf>@1` ([`catalog::STRUCTURE_SWING`],
    /// [`catalog::STRUCTURE_LEVELS`]) for 15m, 1h, 4h and 1d — the last
    /// swings and the structural level registry with touches, sweeps and
    /// SFPs. Structure facts, never signals (ADR-012).
    pub structure: StructureSet,
    /// Derivatives context (ADR-042): `derivatives.oi.sample@1`,
    /// `derivatives.oi.5m@1`, `derivatives.mark@1`,
    /// `derivatives.funding.settled@1` and
    /// `derivatives.liq.window.<5m|15m|1h>@1` ([`catalog::LIQ_WINDOWS`]) —
    /// open interest at its source resolution and on the 5-minute grid,
    /// mark price with indicative funding, settled funding, and liquidations
    /// by side over closed minutes. The liquidation values are a lower
    /// bound: the exchange stream is throttled.
    pub derivatives: Derivatives,
    /// The order book (ADR-043): `book.l2@1`, the L2 book itself, hashed
    /// whole; `book.depth@1`, best levels and depth within 1, 2 and 5 bps
    /// of mid; `book.clusters@1`, the largest levels per side within 5 bps;
    /// and `book.liquidity.window.<5m|15m|1h>@1`
    /// ([`catalog::BOOK_LIQUIDITY_WINDOWS`]), liquidity added, cancelled and
    /// filled per side and band over closed minutes. Live only: archive
    /// replays have no book and keep every value warming up.
    pub book: BookState,
    /// Location (ADR-044): `location.vwap.utc_day@1`, the volume-weighted
    /// average price of the current UTC day over its closed minutes, and
    /// `location.levels@2` (or `@1` when the engine selected it,
    /// [`MarketStateEngine::with_features`]), the level registry with score
    /// components and in-zone flags, rebuilt at each closed 1m bar with
    /// trades; and
    /// `location.auction.prior_day@1`, the auction state against the prior
    /// day's value area. Location facts, never signals (ADR-012).
    pub location: LocationState,
    /// Number of trades consumed. A diagnostic counter, not a feature: it
    /// depends on where consumption started, so it is not reproducible
    /// across replay windows.
    pub trade_count: u64,
    /// Order-flow candidates (ADR-051): `candidate.divergence.cvd.<tf>@1`
    /// and `candidate.divergence.oi.<tf>@1`
    /// ([`catalog::CANDIDATE_DIVERGENCE_CVD`],
    /// [`catalog::CANDIDATE_DIVERGENCE_OI`]) for 15m, 1h, 4h and 1d — the
    /// last comparison of consecutive swing highs and of swing lows at
    /// their anchors; `candidate.effort_result.<5m|15m|1h>@1`
    /// ([`catalog::CANDIDATE_EFFORT_RESULT`]), the last judged
    /// effort-vs-result evaluation of each aggression window;
    /// `candidate.exhaustion@1`; and `candidate.absorption.<5m|15m|1h>@1`
    /// ([`catalog::CANDIDATE_ABSORPTION`], live only). Candidates, never
    /// signals (ADR-012). Appended after `trade_count` (ADR-051 D1).
    pub candidates: Candidates,
    /// Bias and triggers (ADR-053): `bias.context@1`
    /// ([`catalog::BIAS_CONTEXT_V1`]), the regime and the swing steps
    /// every trigger records and no rule reads; and the last trigger of
    /// `trigger.failed_auction@1`, `trigger.failed_reclaim@1`,
    /// `trigger.breakout_acceptance@1`, `trigger.sfp.<tf>@1`
    /// ([`catalog::TRIGGER_SFP`]), and the location-gated candidate
    /// triggers `trigger.cvd_divergence.<tf>@1`
    /// ([`catalog::TRIGGER_CVD_DIVERGENCE`]),
    /// `trigger.effort_result.<w>@1`, `trigger.exhaustion@1` and
    /// `trigger.absorption.<w>@1` (live only), and the OI-expansion
    /// triggers `trigger.oi_expansion_rejection.<tf>@1`
    /// ([`catalog::TRIGGER_OI_EXPANSION_REJECTION`]) and
    /// `trigger.oi_expansion_rejection.prior_day@1` on the SFPs and the
    /// failed auction. Each trigger value follows its fact input's
    /// validity. Never an execution instruction (ADR-010, ADR-012). The last
    /// field, appended after `candidates` (ADR-053 D1).
    pub triggers: Triggers,
}

/// Builds [`MarketState`] incrementally from an event stream in canonical
/// order (ADR-028).
///
/// Live ingestion and historical replay drive the same engine (ADR-019); the
/// engine never knows which of them is feeding it.
#[derive(Debug)]
pub struct MarketStateEngine {
    /// The features this engine computes.
    features: FeatureSet,
    state: MarketState,
    /// The last consumed event: the lower bound for the next one.
    last: Option<MarketEvent>,
    /// Last accepted exchange id of each id-carrying kind.
    ids: LastIds,
    /// The bars closed by the last accepted event.
    closed: Vec<Bar>,
    /// Scratch buffer for the bars an event closes, swapped with `closed`
    /// once the event is accepted.
    pending: Vec<Bar>,
    /// The previous close of every motion series.
    anchors: MotionAnchors,
    /// ATR and regime of the regime timeframe.
    volatility: AtrRegimeSeries,
    /// CVD, the developing minute's large prints and the recent minutes.
    flow: FlowTracker,
    /// The developing day's volume per bin and the recent completed days.
    profile: ProfileTracker,
    /// The bar windows, swings and level registries of the structure.
    structure: StructureTracker,
    /// The structure facts of the last accepted event.
    structure_events: Vec<StructureEvent>,
    /// Scratch buffer for an event's structure facts, swapped with
    /// `structure_events` once the event is accepted.
    structure_pending: Vec<StructureEvent>,
    /// The last open-interest sample and grid boundary, and the liquidation
    /// minutes.
    derivatives: DerivativesTracker,
    /// The order-book flow minutes and pending fills.
    liquidity: LiquidityTracker,
    /// The VWAP sums.
    location: LocationTracker,
    /// The location facts of the last accepted event.
    location_events: Vec<LocationEvent>,
    /// Scratch buffer for an event's location facts, swapped with
    /// `location_events` once the event is accepted.
    location_pending: Vec<LocationEvent>,
    /// The swing anchors and last comparisons of the candidates.
    candidates: CandidateTracker,
    /// The candidate facts of the last accepted event.
    candidate_events: Vec<CandidateEvent>,
    /// Scratch buffer for an event's candidate facts, swapped with
    /// `candidate_events` once the event is accepted.
    candidate_pending: Vec<CandidateEvent>,
    /// The last trigger of every trigger id.
    triggers: TriggerTracker,
    /// The trigger facts of the last accepted event.
    trigger_events: Vec<TriggerEvent>,
    /// Scratch buffer for an event's trigger facts, swapped with
    /// `trigger_events` once the event is accepted.
    trigger_pending: Vec<TriggerEvent>,
}

/// Volatility state after the bars an event closed, committed with them.
struct Stepped {
    motion: MotionSet,
    anchors: MotionAnchors,
    /// `None` when no bar of the regime timeframe closed.
    volatility: Option<AtrRegimeSeries>,
}

/// Last accepted exchange ids (ADR-028): the canonical order alone lets the
/// same trade or book id through twice when the payloads differ, or an id
/// fall at a later time.
#[derive(Debug, Default, Clone, Copy)]
struct LastIds {
    trade: Option<u64>,
    snapshot: Option<u64>,
    update: Option<u64>,
}

impl LastIds {
    /// Checks the exchange id of `event` and returns the bounds after it.
    fn check(self, event: &MarketEvent) -> Result<Self, StateError> {
        let mut next = self;
        match event {
            MarketEvent::Trade(trade) => {
                check_id(event, trade.trade_id, self.trade, None, Stream::Trades)?;
                next.trade = Some(trade.trade_id);
            }
            MarketEvent::BookSnapshot(snapshot) => {
                let id = snapshot.last_update_id;
                check_id(event, id, self.snapshot, self.update, Stream::OrderBook)?;
                next.snapshot = Some(id);
            }
            MarketEvent::BookUpdate(update) => {
                let id = update.last_update_id;
                check_id(event, id, self.update, self.snapshot, Stream::OrderBook)?;
                next.update = Some(id);
            }
            // Kinds without an exchange id.
            MarketEvent::FeedGap(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_) => {}
        }
        Ok(next)
    }
}

/// `id` must exceed the last id of the same kind (`same_kind`) and must not
/// fall below the last id of the other kind on the same stream
/// (`other_kind`): an order-book update may carry the snapshot's last update
/// id, and a snapshot may restate the book at the last applied update.
fn check_id(
    event: &MarketEvent,
    id: u64,
    same_kind: Option<u64>,
    other_kind: Option<u64>,
    stream: Stream,
) -> Result<(), StateError> {
    if same_kind == Some(id) {
        return Err(StateError::Duplicate {
            key: event.canonical_key(),
        });
    }
    for last_id in [same_kind, other_kind].into_iter().flatten() {
        if id < last_id {
            return Err(StateError::IdRegression {
                stream,
                last_id,
                event: event.canonical_key(),
            });
        }
    }
    Ok(())
}

impl Default for MarketStateEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketStateEngine {
    /// Creates an engine with an empty state that computes the current
    /// feature set ([`catalog::CURRENT`]).
    pub fn new() -> Self {
        Self::build(
            catalog::current_set(),
            LevelsVersion::V2,
            [LevelsRule::V1; 4],
        )
    }

    /// Creates an engine with an empty state that computes `features`:
    /// [`catalog::CURRENT`] with any id at a [`catalog::SELECTABLE`]
    /// version instead, such as `location.levels@1` (ADR-044 D10). The
    /// state is stamped with the version of `features`, so experiments
    /// recorded on an older version reproduce.
    ///
    /// # Errors
    ///
    /// - [`UnsupportedFeatureSet::NotComputed`] — a member at a version that
    ///   is neither in [`catalog::CURRENT`] nor in [`catalog::SELECTABLE`]:
    ///   this binary has no code path for it;
    /// - [`UnsupportedFeatureSet::Redefined`] — a member whose definition
    ///   differs from the catalog's under the same key: the engine computes
    ///   the catalog definition, so the stamp would name definitions it
    ///   never computed;
    /// - [`UnsupportedFeatureSet::Missing`] — an id of
    ///   [`catalog::CURRENT`] the set omits: the engine computes every
    ///   family on every event, so the stamp would not name every value the
    ///   state holds.
    pub fn with_features(features: FeatureSet) -> Result<Self, UnsupportedFeatureSet> {
        if let Some(definition) = features.definitions().find(|definition| {
            !catalog::CURRENT.contains(&definition.key)
                && !catalog::SELECTABLE.contains(&definition.key)
        }) {
            return Err(UnsupportedFeatureSet::NotComputed {
                feature: definition.key,
            });
        }
        let registry = catalog::registry();
        if let Some(definition) = features.definitions().find(|definition| {
            registry
                .get(definition.key)
                .is_none_or(|computed| computed.fingerprint() != definition.fingerprint())
        }) {
            return Err(UnsupportedFeatureSet::Redefined {
                feature: definition.key,
            });
        }
        if let Some(missing) = catalog::CURRENT.iter().find(|key| {
            !features
                .definitions()
                .any(|definition| definition.key.id == key.id)
        }) {
            return Err(UnsupportedFeatureSet::Missing { feature: *missing });
        }
        // One version per id: the set holds `location.levels` at `@1` or at
        // `@2`.
        let levels = if features
            .definitions()
            .any(|definition| definition.key == catalog::LOCATION_LEVELS_V1.key)
        {
            LevelsVersion::V1
        } else {
            LevelsVersion::V2
        };
        // Each structure timeframe's registry rule, from the set's
        // `structure.levels.<tf>` version: engine state, which the stamp
        // names through that version.
        let mut rules = [LevelsRule::V1; 4];
        for (rule, (timeframe, current)) in rules.iter_mut().zip(catalog::STRUCTURE_LEVELS) {
            let member = features
                .definitions()
                .find(|definition| definition.key.id == current.key.id)
                .ok_or(UnsupportedFeatureSet::Missing {
                    feature: current.key,
                })?
                .key;
            *rule = LevelsRule::ALL
                .into_iter()
                .find(|candidate| candidate.key(timeframe) == Some(member))
                .ok_or(UnsupportedFeatureSet::NotComputed { feature: member })?;
        }
        Ok(Self::build(features, levels, rules))
    }

    /// An engine with an empty state that computes `features`, its registry
    /// at `levels` and each structure timeframe's level registry under its
    /// rule in `rules`, in [`crate::structure::STRUCTURE_TIMEFRAMES`] order.
    fn build(features: FeatureSet, levels: LevelsVersion, rules: [LevelsRule; 4]) -> Self {
        let volatility = AtrRegimeSeries::new();
        let flow = FlowTracker::new();
        let derivatives = DerivativesTracker::new();
        let state = MarketState {
            feature_set: features.version(),
            as_of: None,
            // `trade.last_price@1` warms up on one trade.
            last_trade_price: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            bars: BarSet::new(),
            motion: MotionSet::new(),
            atr: volatility.atr(),
            regime: volatility.regime(),
            flow: flow.flow(),
            profile: VolumeProfiles::new(),
            structure: StructureSet::new(),
            derivatives: derivatives.derivatives(),
            book: BookState::new(),
            location: LocationState::new(),
            trade_count: 0,
            candidates: Candidates::new(),
            triggers: Triggers::new(),
        };
        Self {
            features,
            state,
            last: None,
            ids: LastIds::default(),
            closed: Vec::new(),
            pending: Vec::new(),
            anchors: MotionAnchors::default(),
            volatility,
            flow,
            profile: ProfileTracker::new(),
            structure: StructureTracker::new(rules),
            structure_events: Vec::new(),
            structure_pending: Vec::new(),
            derivatives,
            liquidity: LiquidityTracker::new(),
            location: LocationTracker::new(levels),
            location_events: Vec::new(),
            location_pending: Vec::new(),
            candidates: CandidateTracker::new(),
            candidate_events: Vec::new(),
            candidate_pending: Vec::new(),
            triggers: TriggerTracker::new(),
            trigger_events: Vec::new(),
            trigger_pending: Vec::new(),
        }
    }

    /// The features this engine computes; experiments record its canonical
    /// list (`Display`) next to [`FeatureSet::version`].
    pub fn feature_set(&self) -> &FeatureSet {
        &self.features
    }

    /// Consumes the next event.
    ///
    /// # Errors
    ///
    /// The state is left unchanged on every error:
    /// - [`StateError::InvalidGap`] for a feed gap with `start > end`;
    /// - [`StateError::OutOfOrder`] if the event sorts before the last
    ///   consumed one;
    /// - [`StateError::Duplicate`] if it equals the last consumed one, or
    ///   repeats the exchange id of the last trade, snapshot or update;
    /// - [`StateError::IdRegression`] if its exchange id falls below the last
    ///   accepted one of its stream;
    /// - [`StateError::Overflow`] if a bar's time or quantity arithmetic, a
    ///   closed bar's true range, change or range, an order-flow sum (CVD,
    ///   window), a volume-profile sum (bin, total, composite), a
    ///   structure count (touches, window bars), a derivatives value
    ///   (liquidation sum or count, ΔOI, time difference), an order-book
    ///   liquidity value (flow sum, pending fill) or a VWAP sum leaves its
    ///   integer range;
    /// - [`StateError::TimeJump`] if it would close more than
    ///   [`MAX_BARS_PER_EVENT`] bars of one timeframe. Recovery: the replay
    ///   or session stops; restart it from a fresh engine after the jump.
    ///
    /// Ordering is the market-data provider's job — the domain never
    /// reorders.
    pub fn apply(&mut self, event: &MarketEvent) -> Result<(), StateError> {
        if let MarketEvent::FeedGap(gap) = event
            && gap.start > gap.end
        {
            return Err(StateError::InvalidGap {
                start: gap.start,
                end: gap.end,
            });
        }
        if let Some(last) = &self.last {
            match event.cmp(last) {
                Ordering::Less => {
                    return Err(StateError::OutOfOrder {
                        last: last.canonical_key(),
                        event: event.canonical_key(),
                    });
                }
                Ordering::Equal => {
                    return Err(StateError::Duplicate {
                        key: event.canonical_key(),
                    });
                }
                Ordering::Greater => {}
            }
        }
        let ids = self.ids.check(event)?;
        // Bars are updated on a copy, committed only once nothing can fail.
        let mut bars = self.state.bars;
        self.pending.clear();
        if let Err(error) = bars.apply(event, &mut self.pending) {
            // Drop any partial result; `closed` is untouched.
            self.pending.clear();
            let event = event.canonical_key();
            return Err(match error {
                BarError::Overflow => StateError::Overflow { event },
                BarError::TooManyBars {
                    timeframe,
                    from,
                    bars,
                } => StateError::TimeJump {
                    event,
                    timeframe,
                    from,
                    bars,
                },
            });
        }
        let stepped = match self.step_volatility() {
            Ok(stepped) => stepped,
            Err(VolatilityError::Overflow) => {
                self.pending.clear();
                return Err(StateError::Overflow {
                    event: event.canonical_key(),
                });
            }
        };
        let flow = match self.flow.step(event, &self.pending, &bars) {
            Ok(flow) => flow,
            Err(FlowError::Overflow) => {
                self.pending.clear();
                return Err(StateError::Overflow {
                    event: event.canonical_key(),
                });
            }
        };
        let profile = match self.profile.step(event, &self.pending) {
            Ok(profile) => profile,
            Err(ProfileError::Overflow) => {
                self.pending.clear();
                return Err(StateError::Overflow {
                    event: event.canonical_key(),
                });
            }
        };
        let structure = match self.structure.step(event, &self.pending) {
            Ok(structure) => structure,
            Err(StructureError::Overflow) => {
                self.pending.clear();
                return Err(StateError::Overflow {
                    event: event.canonical_key(),
                });
            }
        };
        let derivatives = match self.derivatives.step(event, &self.pending) {
            Ok(derivatives) => derivatives,
            Err(DerivativesError::Overflow) => {
                self.pending.clear();
                return Err(StateError::Overflow {
                    event: event.canonical_key(),
                });
            }
        };
        let liquidity = match self.liquidity.step(event, &self.pending, &self.state.book) {
            Ok(liquidity) => liquidity,
            Err(LiquidityError::Overflow) => {
                self.pending.clear();
                return Err(StateError::Overflow {
                    event: event.canonical_key(),
                });
            }
        };
        let location = match self.location.step(event, &self.pending) {
            Ok(location) => location,
            Err(LocationError::Overflow) => {
                self.pending.clear();
                return Err(StateError::Overflow {
                    event: event.canonical_key(),
                });
            }
        };

        match event {
            MarketEvent::Trade(trade) => {
                self.state.last_trade_price = FeatureValue::Ready(trade.price);
                self.state.trade_count += 1;
            }
            // Consumed by the trackers above, not by trade fields.
            MarketEvent::FeedGap(_)
            | MarketEvent::BookSnapshot(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::BookUpdate(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_) => {}
        }
        self.state.bars = bars;
        if let Some(stepped) = stepped {
            self.state.motion = stepped.motion;
            self.anchors = stepped.anchors;
            if let Some(volatility) = stepped.volatility {
                self.state.atr = volatility.atr();
                self.state.regime = volatility.regime();
                self.volatility = volatility;
            }
        }
        self.flow.commit(flow);
        self.state.flow = self.flow.flow();
        self.profile.commit(profile, &mut self.state.profile);
        self.structure_pending.clear();
        if let Some(structure) = structure {
            self.structure.commit(
                structure,
                &mut self.state.structure,
                &mut self.structure_pending,
            );
        }
        std::mem::swap(&mut self.structure_events, &mut self.structure_pending);
        self.derivatives.commit(derivatives);
        self.state.derivatives = self.derivatives.derivatives();
        self.liquidity
            .commit(liquidity, event, &mut self.state.book);
        self.location.commit(location, &mut self.state.location);
        // The infallible location stage, on the committed state (ADR-044
        // decision 2).
        self.location_pending.clear();
        let MarketState {
            profile,
            structure,
            book,
            location: located,
            ..
        } = &mut self.state;
        self.location.locate(
            event,
            &self.pending,
            location::Inputs {
                profile,
                structure,
                book,
            },
            located,
            &mut self.location_pending,
        );
        std::mem::swap(&mut self.location_events, &mut self.location_pending);
        // The infallible candidate stage, after location, on the committed
        // state (ADR-051 decision 2).
        self.candidate_pending.clear();
        let MarketState {
            flow,
            derivatives,
            book,
            location,
            candidates,
            ..
        } = &mut self.state;
        self.candidates.detect(
            event,
            &self.pending,
            &self.structure_events,
            candidate::Inputs {
                flow,
                derivatives,
                location,
                book,
            },
            candidates,
            &mut self.candidate_pending,
        );
        std::mem::swap(&mut self.candidate_events, &mut self.candidate_pending);
        // The infallible trigger stage, last, on the committed state
        // (ADR-053 decision 2).
        self.trigger_pending.clear();
        let MarketState {
            regime,
            structure,
            derivatives,
            location,
            candidates,
            triggers,
            ..
        } = &mut self.state;
        self.triggers.detect(
            &self.structure_events,
            &self.location_events,
            &self.candidate_events,
            trigger::Inputs {
                regime,
                structure,
                location,
                candidates,
                oi_5m: &derivatives.oi_5m,
            },
            triggers,
            &mut self.trigger_pending,
        );
        std::mem::swap(&mut self.trigger_events, &mut self.trigger_pending);
        self.state.as_of = Some(event.time());
        self.last = Some(event.clone());
        self.ids = ids;
        std::mem::swap(&mut self.closed, &mut self.pending);
        Ok(())
    }

    /// Steps motion, ATR and regime through the bars in `pending`, in close
    /// order, on copies (ADR-033); `None` when no bar closed. The ATR series
    /// is copied only when a bar of its timeframe closed.
    fn step_volatility(&self) -> Result<Option<Stepped>, VolatilityError> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let mut motion = self.state.motion;
        let mut anchors = self.anchors;
        let mut volatility = self
            .pending
            .iter()
            .any(|bar| bar.timeframe == catalog::REGIME_TIMEFRAME)
            .then(|| self.volatility.clone());
        for bar in &self.pending {
            anchors.apply(bar, &mut motion)?;
            if let Some(volatility) = &mut volatility {
                volatility.push(bar)?;
            }
        }
        Ok(Some(Stepped {
            motion,
            anchors,
            volatility,
        }))
    }

    /// The current state.
    pub fn state(&self) -> &MarketState {
        &self.state
    }

    /// The bars the last accepted event closed, in `(end, timeframe)` order
    /// (ADR-031); empty when it closed none, as after every event that is
    /// not on the trades stream. A rejected event leaves it unchanged.
    pub fn closed_bars(&self) -> &[Bar] {
        &self.closed
    }

    /// The structure facts of the last accepted event, in processing order
    /// (ADR-037, decision 8); empty when it produced none. A rejected event
    /// leaves them unchanged.
    pub fn structure_events(&self) -> &[StructureEvent] {
        &self.structure_events
    }

    /// The location facts of the last accepted event, in emission order
    /// (ADR-044, decision 7); empty when it produced none. A rejected event
    /// leaves them unchanged.
    pub fn location_events(&self) -> &[LocationEvent] {
        &self.location_events
    }

    /// The candidate facts of the last accepted event, in emission order
    /// (ADR-051, decision 2); empty when it produced none. A rejected event
    /// leaves them unchanged.
    pub fn candidate_events(&self) -> &[CandidateEvent] {
        &self.candidate_events
    }

    /// The trigger facts of the last accepted event, in emission order
    /// (ADR-053, decision 2); empty when it produced none. A rejected event
    /// leaves them unchanged.
    pub fn trigger_events(&self) -> &[TriggerEvent] {
        &self.trigger_events
    }

    /// The trigger engine state, for tests of its records.
    #[cfg(test)]
    pub(crate) fn trigger_tracker(&self) -> &TriggerTracker {
        &self.triggers
    }
}

/// Why the engine rejected an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateError {
    /// The event sorts before the last consumed event in the canonical order
    /// (ADR-028).
    OutOfOrder {
        /// Key of the last consumed event.
        last: CanonicalKey,
        /// Key of the rejected event.
        event: CanonicalKey,
    },
    /// The event equals the last consumed event, or repeats the exchange id
    /// of the last accepted event of its kind (ADR-028).
    Duplicate {
        /// Key of the repeated event.
        key: CanonicalKey,
    },
    /// The event's exchange id falls below the last accepted id of its
    /// stream, although it sorts after the last consumed event (ADR-028).
    IdRegression {
        /// The stream whose ids regressed.
        stream: Stream,
        /// The last accepted id the event falls below.
        last_id: u64,
        /// Key of the rejected event.
        event: CanonicalKey,
    },
    /// A feed gap whose start is after its end.
    InvalidGap {
        /// Start of the rejected gap.
        start: EventTime,
        /// End of the rejected gap.
        end: EventTime,
    },
    /// The event would push a bar's time or quantity arithmetic, a
    /// volatility value, an order-flow sum, a volume-profile sum, a
    /// structure count, a derivatives value, an order-book liquidity value
    /// or a VWAP sum out of its integer range (ADR-027, ADR-031, ADR-033,
    /// ADR-035, ADR-036, ADR-037, ADR-042, ADR-043, ADR-044).
    Overflow {
        /// Key of the rejected event.
        event: CanonicalKey,
    },
    /// The event jumps so far ahead in event time that it would close more
    /// than [`MAX_BARS_PER_EVENT`] bars of one timeframe (ADR-031).
    TimeJump {
        /// Key of the rejected event; its time is where the jump lands.
        event: CanonicalKey,
        /// The timeframe that exceeds the bound (the shortest one).
        timeframe: Timeframe,
        /// Open time of that timeframe's developing bar: where the jump
        /// starts.
        from: EventTime,
        /// How many bars of `timeframe` the event would close.
        bars: u64,
    },
}

impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfOrder { last, event } => write!(
                f,
                "event ({event}) sorts before the last consumed event ({last})"
            ),
            Self::Duplicate { key } => {
                write!(f, "event ({key}) repeats the last consumed event")
            }
            Self::IdRegression {
                stream,
                last_id,
                event,
            } => write!(
                f,
                "event ({event}) has an exchange id below {last_id}, the last accepted on {stream:?}"
            ),
            Self::InvalidGap { start, end } => {
                write!(f, "feed gap starts at {start}, after its end at {end}")
            }
            Self::Overflow { event } => {
                write!(
                    f,
                    "event ({event}) overflows the bar, volatility, order-flow, \
                     volume-profile, structure, derivatives, order-book liquidity \
                     or VWAP arithmetic"
                )
            }
            Self::TimeJump {
                event,
                timeframe,
                from,
                bars,
            } => write!(
                f,
                "event ({event}) would close {bars} {timeframe} bars from {from}, \
                 more than the {MAX_BARS_PER_EVENT} one event may close"
            ),
        }
    }
}

impl std::error::Error for StateError {}

/// Why [`MarketStateEngine::with_features`] rejected a feature set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedFeatureSet {
    /// The set holds a version this binary cannot compute: neither in
    /// [`catalog::CURRENT`] nor in [`catalog::SELECTABLE`].
    NotComputed {
        /// The member.
        feature: FeatureKey,
    },
    /// The set holds a key the engine computes, but under a definition
    /// (fingerprint) other than the catalog's.
    Redefined {
        /// The member.
        feature: FeatureKey,
    },
    /// The set omits an id the engine always computes.
    Missing {
        /// The [`catalog::CURRENT`] key of the omitted id.
        feature: FeatureKey,
    },
}

impl fmt::Display for UnsupportedFeatureSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotComputed { feature } => {
                write!(f, "the engine cannot compute {feature}")
            }
            Self::Redefined { feature } => write!(
                f,
                "the set defines {feature} other than the catalog the engine computes"
            ),
            Self::Missing { feature } => write!(
                f,
                "the set omits {}, which the engine always computes ({feature})",
                feature.id
            ),
        }
    }
}

impl std::error::Error for UnsupportedFeatureSet {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::Coverage;
    use crate::event::samples::{gap, kline, mark, one_of_each, snapshot, t, trade, update};
    use crate::event::{GapReason, Stream};
    use crate::flow::{AggressionWindows, Cvd, DayCvd};
    use crate::num::Qty;
    use crate::order::EventKind;

    fn engine_after(events: &[MarketEvent]) -> MarketStateEngine {
        let mut engine = MarketStateEngine::new();
        for event in events {
            engine.apply(event).unwrap();
        }
        engine
    }

    /// Applies `rejected` after `accepted`, expects `err`, and checks that
    /// neither the state nor the ordering bound moved.
    fn assert_rejected(accepted: Option<&MarketEvent>, rejected: &MarketEvent, err: StateError) {
        let mut engine = engine_after(accepted.map(std::slice::from_ref).unwrap_or_default());
        let before = engine.state().clone();
        assert_eq!(engine.apply(rejected), Err(err));
        assert_eq!(engine.state(), &before);
        if let Some(last) = accepted {
            // The bound is still the last accepted event, not the rejected one.
            assert_eq!(
                engine.apply(last),
                Err(StateError::Duplicate {
                    key: last.canonical_key()
                })
            );
            assert_eq!(engine.state(), &before);
        }
    }

    #[test]
    fn tracks_latest_trade() {
        let events = [trade(1_000, 1), trade(1_000, 2), trade(1_500, 3)];
        let engine = engine_after(&events);
        let mut bars = BarSet::new();
        for event in &events {
            bars.apply(event, &mut Vec::new()).unwrap();
        }
        assert_eq!(
            engine.state(),
            &MarketState {
                feature_set: catalog::current_set().version(),
                as_of: Some(t(1_500)),
                last_trade_price: FeatureValue::Ready(Price::from_units(6_354_210_000_000)),
                bars,
                motion: MotionSet::new(),
                atr: FeatureValue::WarmingUp {
                    observed: 0,
                    required: 14,
                },
                regime: FeatureValue::WarmingUp {
                    observed: 0,
                    required: 214,
                },
                flow: OrderFlow {
                    // Three buys of 0.015 BTC.
                    cvd: FeatureValue::Ready(Cvd {
                        feature: catalog::FLOW_CVD_CONTINUOUS_V1.key,
                        cvd: Qty::from_units(4_500_000),
                        anchor: t(1_000),
                        gaps: 0,
                    }),
                    cvd_utc_day: FeatureValue::Ready(DayCvd {
                        feature: catalog::FLOW_CVD_UTC_DAY_V1.key,
                        day_open: t(0),
                        cvd: Qty::from_units(4_500_000),
                        coverage: Coverage {
                            partial_start: true,
                            feed_gap: false,
                        },
                    }),
                    windows: AggressionWindows::new(),
                },
                profile: VolumeProfiles::new(),
                structure: StructureSet::new(),
                derivatives: Derivatives::new(),
                book: BookState::new(),
                location: LocationState::new(),
                trade_count: 3,
                candidates: Candidates::new(),
                triggers: Triggers::new(),
            }
        );
        assert!(engine.state().bars.iter().all(|series| {
            series
                .developing()
                .ready()
                .is_some_and(|bar| bar.trade_count == 3)
        }));
    }

    #[test]
    fn closed_bars_hold_what_the_last_event_closed() {
        let mut engine = MarketStateEngine::new();
        assert!(engine.closed_bars().is_empty());
        engine.apply(&trade(30_000, 1)).unwrap();
        assert!(engine.closed_bars().is_empty());
        engine.apply(&trade(300_000, 2)).unwrap();
        let closed: Vec<_> = engine
            .closed_bars()
            .iter()
            .map(|bar| (bar.end().as_millis(), bar.timeframe))
            .collect();
        assert_eq!(
            closed,
            [
                (60_000, Timeframe::M1),
                (120_000, Timeframe::M1),
                (180_000, Timeframe::M1),
                (240_000, Timeframe::M1),
                (300_000, Timeframe::M1),
                (300_000, Timeframe::M5),
            ]
        );
        // A rejected event leaves them; an event on another stream clears
        // them.
        engine.apply(&trade(299_999, 3)).unwrap_err();
        assert_eq!(engine.closed_bars().len(), 6);
        engine.apply(&mark(400_000, 1)).unwrap();
        assert!(engine.closed_bars().is_empty());
        // The mark price did not close the 1m bar of 300_000; the next trade
        // does.
        engine.apply(&trade(400_001, 4)).unwrap();
        assert_eq!(engine.closed_bars().len(), 1);
        assert_eq!(
            engine
                .state()
                .bars
                .get(Timeframe::M1)
                .unwrap()
                .last_closed(),
            &FeatureValue::Ready(engine.closed_bars()[0])
        );
    }

    #[test]
    fn rejects_a_jump_beyond_the_bar_bound() {
        // The reviewer's repro: a trade at the epoch, then one at a
        // present-day millisecond timestamp (~28M 1m bars).
        let first = trade(0, 1);
        let jump = trade(1_700_000_000_000, 2);
        let err = StateError::TimeJump {
            event: jump.canonical_key(),
            timeframe: Timeframe::M1,
            from: t(0),
            bars: 28_333_333,
        };
        assert_rejected(Some(&first), &jump, err);

        // Exactly the bound closes; one more bar is rejected, for trades and
        // for trades gaps alike, with the state and closed bars unchanged.
        let bound = i64::try_from(MAX_BARS_PER_EVENT).unwrap();
        let mut engine = engine_after(&[first.clone(), trade(bound * 60_000, 2)]);
        let minutes = engine
            .closed_bars()
            .iter()
            .filter(|bar| bar.timeframe == Timeframe::M1)
            .count();
        assert_eq!(minutes, 44_640);
        let before = engine.state().clone();
        let closed_before = engine.closed_bars().to_vec();
        let from = t(bound * 60_000);
        let far = (2 * bound + 1) * 60_000;
        for event in [
            trade(far, 3),
            gap(Stream::Trades, far - 1, far, GapReason::Disconnected),
        ] {
            assert_eq!(
                engine.apply(&event),
                Err(StateError::TimeJump {
                    event: event.canonical_key(),
                    timeframe: Timeframe::M1,
                    from,
                    bars: 44_641,
                })
            );
            assert_eq!(engine.state(), &before);
            assert_eq!(engine.closed_bars(), closed_before);
        }
        // A microsecond timestamp read as milliseconds.
        let mut engine = engine_after(&[trade(1_700_000_000_000, 1)]);
        assert!(matches!(
            engine.apply(&trade(1_700_000_000_000_000, 2)),
            Err(StateError::TimeJump { bars, .. }) if bars > MAX_BARS_PER_EVENT
        ));
        // Other streams never close bars, so they never jump.
        engine.apply(&mark(1_700_000_000_000_000, 1)).unwrap();
    }

    #[test]
    fn rejects_an_event_that_overflows_a_bar() {
        // A trade whose bar would end past the last representable instant.
        let far = trade(i64::MAX, 1);
        assert_rejected(
            None,
            &far,
            StateError::Overflow {
                event: far.canonical_key(),
            },
        );
        // Volume overflow: the state, the bars and the closed bars stay put.
        // The 5m bar reaches exactly `i64::MAX` units; the 1m bar holds
        // `huge` alone, closing the first minute.
        let huge = sized_trade(60_000, 2, i64::MAX - 1_500_000);
        let mut engine = engine_after(&[trade(500, 1), huge]);
        let before = engine.state().clone();
        let closed_before = engine.closed_bars().to_vec();
        assert_eq!(closed_before.len(), 1);
        let overflow = sized_trade(60_001, 3, 1);
        assert_eq!(
            engine.apply(&overflow),
            Err(StateError::Overflow {
                event: overflow.canonical_key()
            })
        );
        assert_eq!(engine.state(), &before);
        assert_eq!(engine.closed_bars(), closed_before);
        engine.apply(&trade(60_001, 3)).unwrap_err();
        engine.apply(&mark(60_001, 1)).unwrap();
    }

    #[test]
    fn accepts_every_kind_and_only_trades_change_trade_fields() {
        let mut events = one_of_each(1_000);
        // The second round carries fresh exchange ids.
        events.extend(one_of_each(2_000).into_iter().map(|event| match event {
            MarketEvent::Trade(trade) => MarketEvent::Trade(crate::event::Trade {
                trade_id: trade.trade_id + 1,
                ..trade
            }),
            MarketEvent::BookSnapshot(snapshot) => {
                MarketEvent::BookSnapshot(crate::event::BookSnapshot {
                    last_update_id: 200,
                    ..snapshot
                })
            }
            MarketEvent::BookUpdate(update) => MarketEvent::BookUpdate(crate::event::BookUpdate {
                first_update_id: 201,
                last_update_id: 205,
                prev_update_id: 200,
                ..update
            }),
            other @ (MarketEvent::FeedGap(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_)) => other,
        }));
        let mut engine = MarketStateEngine::new();
        for event in &events {
            let before = engine.state().clone();
            engine.apply(event).unwrap();
            let after = engine.state();
            assert_eq!(after.as_of, Some(event.time()), "{event:?}");
            if event.kind() == EventKind::Trade {
                assert_eq!(after.trade_count, before.trade_count + 1);
                assert!(after.last_trade_price.is_ready());
            } else {
                assert_eq!(after.trade_count, before.trade_count, "{event:?}");
                assert_eq!(after.last_trade_price, before.last_trade_price, "{event:?}");
            }
        }
        assert_eq!(engine.state().trade_count, 2);
        // The kline closing at 2_000 is the last event.
        assert_eq!(engine.state().as_of, Some(t(2_000)));
    }

    #[test]
    fn stamps_the_current_feature_set() {
        let engine = MarketStateEngine::new();
        let current = catalog::current_set();
        assert_eq!(engine.state().feature_set, current.version());
        assert_eq!(engine.feature_set(), &current);
        assert_eq!(
            engine.feature_set().to_string(),
            "bars.motion.15m@1,bars.motion.1d@1,bars.motion.1h@1,bars.motion.1m@1,\
             bars.motion.4h@1,bars.motion.5m@1,bars.time.15m@1,bars.time.1d@1,\
             bars.time.1h@1,bars.time.1m@1,bars.time.4h@1,bars.time.5m@1,\
             bias.context@1,book.clusters@1,book.depth@1,book.l2@1,book.liquidity.window.15m@1,\
             book.liquidity.window.1h@1,book.liquidity.window.5m@1,\
             candidate.absorption.15m@1,candidate.absorption.1h@1,\
             candidate.absorption.5m@1,\
             candidate.divergence.cvd.15m@1,candidate.divergence.cvd.1d@1,\
             candidate.divergence.cvd.1h@1,candidate.divergence.cvd.4h@1,\
             candidate.divergence.oi.15m@1,candidate.divergence.oi.1d@1,\
             candidate.divergence.oi.1h@1,candidate.divergence.oi.4h@1,\
             candidate.effort_result.15m@1,candidate.effort_result.1h@1,\
             candidate.effort_result.5m@1,candidate.exhaustion@1,\
             derivatives.funding.settled@1,derivatives.liq.window.15m@1,\
             derivatives.liq.window.1h@1,derivatives.liq.window.5m@1,\
             derivatives.mark@1,derivatives.oi.5m@1,derivatives.oi.sample@1,\
             flow.cvd.continuous@1,flow.cvd.utc_day@1,flow.window.15m@1,\
             flow.window.1h@1,flow.window.5m@1,location.auction.prior_day@1,\
             location.levels@2,location.vwap.utc_day@1,profile.volume.composite_5d@1,\
             profile.volume.composite_7d@1,profile.volume.prior_day@1,\
             profile.volume.utc_day@1,\
             structure.levels.15m@1,structure.levels.1d@1,structure.levels.1h@1,\
             structure.levels.4h@1,structure.swing.15m@1,structure.swing.1d@1,\
             structure.swing.1h@1,structure.swing.4h@1,trade.last_price@1,\
             trigger.absorption.15m@1,trigger.absorption.1h@1,\
             trigger.absorption.5m@1,trigger.breakout_acceptance@1,\
             trigger.cvd_divergence.15m@1,trigger.cvd_divergence.1d@1,\
             trigger.cvd_divergence.1h@1,trigger.cvd_divergence.4h@1,\
             trigger.effort_result.15m@1,trigger.effort_result.1h@1,\
             trigger.effort_result.5m@1,trigger.exhaustion@1,trigger.failed_auction@1,\
             trigger.failed_reclaim@1,trigger.oi_expansion_rejection.15m@1,\
             trigger.oi_expansion_rejection.1d@1,trigger.oi_expansion_rejection.1h@1,\
             trigger.oi_expansion_rejection.4h@1,trigger.oi_expansion_rejection.prior_day@1,\
             trigger.sfp.15m@1,trigger.sfp.1d@1,\
             trigger.sfp.1h@1,trigger.sfp.4h@1,\
             volatility.atr.1h@1,volatility.regime.1h@1"
        );
        // Consuming events never changes it.
        let engine = engine_after(&one_of_each(1_000));
        assert_eq!(engine.state().feature_set, current.version());
        assert_eq!(
            MarketStateEngine::default().state(),
            MarketStateEngine::new().state()
        );
    }

    #[test]
    fn last_trade_price_warms_up_on_the_first_trade() {
        let warming = FeatureValue::WarmingUp {
            observed: 0,
            required: 1,
        };
        let mut engine = MarketStateEngine::new();
        assert_eq!(engine.state().last_trade_price, warming);
        assert_eq!(engine.state().last_trade_price.ready(), None);
        // Non-trade events leave it warming.
        for event in [
            gap(Stream::Trades, 0, 900, GapReason::MissingData),
            snapshot(950, 10),
            mark(960, 1),
        ] {
            engine.apply(&event).unwrap();
            assert_eq!(engine.state().last_trade_price, warming, "{event:?}");
        }
        engine.apply(&priced_trade(1_000, 1, 42)).unwrap();
        assert_eq!(
            engine.state().last_trade_price,
            FeatureValue::Ready(Price::from_units(42))
        );
        // Gap policy of `trade.last_price@1`: none — a gap keeps the price.
        engine
            .apply(&gap(Stream::Trades, 1_001, 2_000, GapReason::Disconnected))
            .unwrap();
        assert_eq!(
            engine.state().last_trade_price.ready(),
            Some(&Price::from_units(42))
        );
        engine.apply(&priced_trade(2_000, 2, 43)).unwrap();
        assert_eq!(
            engine.state().last_trade_price.ready(),
            Some(&Price::from_units(43))
        );
    }

    #[test]
    fn exposes_motion_atr_and_regime_of_the_last_closed_bars() {
        const HOUR: i64 = 3_600_000;
        let mut engine = MarketStateEngine::new();
        fn warming<T>(required: u64) -> FeatureValue<T> {
            FeatureValue::WarmingUp {
                observed: 0,
                required,
            }
        }
        assert_eq!(engine.state().motion, MotionSet::new());
        assert_eq!(engine.state().atr, warming(14));
        assert_eq!(engine.state().regime, warming(214));
        // Hour 0 is the partial start: it only anchors (ADR-033, decision 6).
        engine.apply(&priced_trade(1_800_000, 1, 100)).unwrap();
        engine.apply(&priced_trade(HOUR + 600_000, 2, 130)).unwrap();
        let motion = engine.state().motion;
        assert_eq!(motion.get(Timeframe::H1), Some(&warming(1)));
        assert_eq!(engine.state().atr, warming(14));
        // Hour 1 is a sample against hour 0's close.
        engine
            .apply(&priced_trade(2 * HOUR + 600_000, 3, 120))
            .unwrap();
        let state = engine.state();
        let hour = *state.motion.get(Timeframe::H1).unwrap().ready().unwrap();
        assert_eq!(
            (hour.previous_close, hour.close, hour.change),
            (
                Price::from_units(100),
                Price::from_units(130),
                Price::from_units(30)
            )
        );
        assert_eq!(
            state.atr,
            FeatureValue::WarmingUp {
                observed: 1,
                required: 14
            }
        );
        assert_eq!(
            state.regime,
            FeatureValue::WarmingUp {
                observed: 1,
                required: 214
            }
        );
        // The 1m motion is that of the last closed minute, an empty one.
        let minute = *state.motion.get(Timeframe::M1).unwrap().ready().unwrap();
        assert_eq!(minute.open_time, t(2 * HOUR + 540_000));
        assert_eq!(minute.change, Price::from_units(0));
        // Other streams change none of them.
        let before = engine.state().clone();
        engine.apply(&mark(2 * HOUR + 700_000, 1)).unwrap();
        assert_eq!(engine.state().motion, before.motion);
        assert_eq!(engine.state().atr, before.atr);
    }

    #[test]
    fn rejects_an_event_that_overflows_a_volatility_value() {
        // Minute 0 anchors at -10 units; minute 1 closes at `i64::MAX`, so
        // its change leaves the price range when minute 2 closes it.
        let mut engine =
            engine_after(&[priced_trade(0, 1, -10), priced_trade(60_000, 2, i64::MAX)]);
        let before = engine.state().clone();
        let closed_before = engine.closed_bars().to_vec();
        let overflow = priced_trade(120_000, 3, 1);
        assert_eq!(
            engine.apply(&overflow),
            Err(StateError::Overflow {
                event: overflow.canonical_key()
            })
        );
        assert_eq!(engine.state(), &before);
        assert_eq!(engine.closed_bars(), closed_before);
        // Bars, motion, ATR and regime all stayed where they were.
        assert_eq!(engine.state().bars, before.bars);
        assert_eq!(engine.state().motion, before.motion);
        assert_eq!(engine.state().atr, before.atr);
        assert_eq!(engine.state().regime, before.regime);
        // Events that close no bar still pass.
        engine.apply(&mark(120_000, 1)).unwrap();
    }

    #[test]
    fn rejects_an_event_that_overflows_the_cvd() {
        // The first day's bars hold `i64::MAX` units; the next day's bars
        // and every window fit, but the continuous CVD does not.
        let mut engine = engine_after(&[sized_trade(500, 1, i64::MAX)]);
        let before = engine.state().clone();
        let closed_before = engine.closed_bars().to_vec();
        let overflow = sized_trade(86_400_000, 2, 1);
        assert_eq!(
            engine.apply(&overflow),
            Err(StateError::Overflow {
                event: overflow.canonical_key()
            })
        );
        assert_eq!(engine.state(), &before);
        assert_eq!(engine.closed_bars(), closed_before);
        assert_eq!(engine.state().flow, before.flow);
        // The ordering bound did not move: the accepted trade is still the
        // last one, and an event on another stream passes.
        assert_eq!(
            engine.apply(&sized_trade(500, 1, i64::MAX)),
            Err(StateError::Duplicate {
                key: sized_trade(500, 1, i64::MAX).canonical_key()
            })
        );
        engine.apply(&mark(86_400_000, 1)).unwrap();
        // The same trade with the other aggressor fits: only the CVD
        // overflowed.
        let MarketEvent::Trade(base) = overflow else {
            unreachable!("sized_trade builds a trade")
        };
        engine
            .apply(&MarketEvent::Trade(crate::event::Trade {
                time: t(86_400_001),
                aggressor: crate::event::Aggressor::Sell,
                ..base
            }))
            .unwrap();
        let cvd = engine.state().flow.cvd.ready().unwrap().cvd;
        assert_eq!(cvd, Qty::from_units(i64::MAX - 1));
    }

    #[test]
    fn rejects_an_event_that_overflows_a_volume_profile() {
        // One trade a day at one price, sides alternating so the CVD stays
        // small: every bar, window and day fits, but five days in one bin
        // leave the composite's `i64` range when day 4 closes.
        const DAY: i64 = 86_400_000;
        let big = i64::MAX / 5 + 1;
        let day_trade = |day: i64, aggressor| {
            let MarketEvent::Trade(base) = sized_trade(day * DAY + 1_000, 0, big) else {
                unreachable!("sized_trade builds a trade")
            };
            MarketEvent::Trade(crate::event::Trade {
                trade_id: u64::try_from(day).unwrap() + 1,
                aggressor,
                ..base
            })
        };
        let events: Vec<MarketEvent> = (0..5)
            .map(|day| {
                let aggressor = if day % 2 == 0 {
                    crate::event::Aggressor::Buy
                } else {
                    crate::event::Aggressor::Sell
                };
                day_trade(day, aggressor)
            })
            .collect();
        let mut engine = engine_after(&events);
        assert_eq!(
            engine.state().profile.composite_5d,
            FeatureValue::WarmingUp {
                observed: 4,
                required: 5
            }
        );
        let before = engine.state().clone();
        let tracker = engine.profile.clone();
        let closed_before = engine.closed_bars().to_vec();
        let overflow = sized_trade(5 * DAY + 1_000, 6, 1);
        assert_eq!(
            engine.apply(&overflow),
            Err(StateError::Overflow {
                event: overflow.canonical_key()
            })
        );
        assert_eq!(engine.state(), &before);
        assert_eq!(engine.state().profile, before.profile);
        assert_eq!(engine.profile, tracker);
        assert_eq!(engine.closed_bars(), closed_before);
        // Events that close no bar still pass.
        engine.apply(&mark(5 * DAY + 1_000, 1)).unwrap();
    }

    #[test]
    fn exposes_structure_and_its_events() {
        const M15: i64 = 900_000;
        let usdt = |whole: i64| whole * crate::num::SCALE;
        // Four rising 15m bars, then a swing high at 60 015 on bar 7.
        let mut events = Vec::new();
        for (index, high) in (0..).zip([
            59_990, 59_992, 59_994, 59_996, 60_010, 60_011, 60_012, 60_015, 60_012, 60_011, 60_010,
        ]) {
            let id = u64::try_from(index).unwrap() * 2;
            events.push(priced_trade(index * M15 + 1_000, id + 1, usdt(high)));
            events.push(priced_trade(index * M15 + 2_000, id + 2, usdt(high - 5)));
        }
        let mut engine = engine_after(&events);
        assert!(engine.structure_events().is_empty());
        let warm = engine.state().structure.get(Timeframe::M15).unwrap();
        assert!(warm.levels.ready().unwrap().highs().is_empty());
        // The trade that closes bar 10 confirms it.
        engine
            .apply(&priced_trade(11 * M15 + 1_000, 100, usdt(60_008)))
            .unwrap();
        let facts = engine.structure_events().to_vec();
        assert!(
            matches!(
                facts.as_slice(),
                [StructureEvent::Swing(swing)] if swing.price == Price::from_units(usdt(60_015))
            ),
            "{facts:?}"
        );
        let structure = engine.state().structure.get(Timeframe::M15).unwrap();
        assert_eq!(structure.levels.ready().unwrap().highs().len(), 1);
        assert!(structure.swings.ready().unwrap().high.is_some());
        // A rejected event changes neither the structure nor its facts.
        let before = engine.state().clone();
        engine
            .apply(&priced_trade(11 * M15, 101, usdt(70_000)))
            .unwrap_err();
        assert_eq!(engine.structure_events(), facts);
        assert_eq!(engine.state(), &before);
        // An event that emits nothing clears the facts, not the structure.
        engine.apply(&mark(11 * M15 + 2_000, 1)).unwrap();
        assert!(engine.structure_events().is_empty());
        assert_eq!(engine.state().structure, before.structure);
        // A trade one unit beyond the level sweeps it.
        engine
            .apply(&priced_trade(11 * M15 + 3_000, 102, usdt(60_015) + 1))
            .unwrap();
        assert!(
            matches!(engine.structure_events(), [StructureEvent::Sweep(_)]),
            "{:?}",
            engine.structure_events()
        );
        let swept = engine.state().structure.get(Timeframe::M15).unwrap();
        assert!(swept.levels.ready().unwrap().highs().is_empty());
        assert_eq!(swept.levels.ready().unwrap().pending().len(), 1);
    }

    #[test]
    fn other_streams_leave_the_order_flow_unchanged() {
        let mut engine = engine_after(&[trade(1_000, 1), trade(400_000, 2)]);
        let flow = engine.state().flow;
        assert!(flow.windows.get(Timeframe::M5).unwrap().is_ready());
        for event in [
            mark(400_001, 1),
            snapshot(400_002, 10),
            gap(Stream::OrderBook, 400_000, 400_003, GapReason::Disconnected),
            update(400_004, 11, 12, 10),
            kline(340_005, 400_004),
        ] {
            engine.apply(&event).unwrap();
            assert_eq!(engine.state().flow, flow, "{event:?}");
        }
    }

    #[test]
    fn rejects_an_event_that_overflows_a_derivatives_sum() {
        let huge = |millis: i64| {
            MarketEvent::Liquidation(crate::event::Liquidation {
                time: t(millis),
                aggressor: crate::event::Aggressor::Sell,
                price: Price::from_units(1),
                avg_price: Price::from_units(1),
                filled_qty: Qty::from_units(i64::MAX),
            })
        };
        let mut engine = engine_after(&[trade(500, 1), huge(1_000)]);
        let before = engine.state().clone();
        let closed_before = engine.closed_bars().to_vec();
        let overflow = huge(2_000);
        assert_eq!(
            engine.apply(&overflow),
            Err(StateError::Overflow {
                event: overflow.canonical_key()
            })
        );
        assert_eq!(engine.state(), &before);
        assert_eq!(engine.closed_bars(), closed_before);
        // The ordering bound did not move.
        assert_eq!(
            engine.apply(&huge(1_000)),
            Err(StateError::Duplicate {
                key: huge(1_000).canonical_key()
            })
        );
        // The minute's short side still has room.
        engine
            .apply(&MarketEvent::Liquidation(crate::event::Liquidation {
                time: t(2_000),
                aggressor: crate::event::Aggressor::Buy,
                price: Price::from_units(1),
                avg_price: Price::from_units(1),
                filled_qty: Qty::from_units(i64::MAX),
            }))
            .unwrap();
    }

    #[test]
    fn rejects_an_event_that_overflows_a_vwap_sum() {
        // Unreachable from trades alone: a day's volume fits the 1d bar's
        // `i64`, so |Σ price · qty| < 2^126. The sum is set directly.
        let mut engine = engine_after(&[trade(1_000, 1)]);
        engine.location.vwap.notional = i128::MAX - 1;
        let before = engine.state().clone();
        let tracker = engine.location.clone();
        let closed_before = engine.closed_bars().to_vec();
        let overflow = trade(2_000, 2);
        assert_eq!(
            engine.apply(&overflow),
            Err(StateError::Overflow {
                event: overflow.canonical_key()
            })
        );
        assert_eq!(engine.state(), &before);
        assert_eq!(engine.location, tracker);
        assert_eq!(engine.closed_bars(), closed_before);
        // The ordering bound did not move; other streams still pass.
        assert_eq!(
            engine.apply(&trade(1_000, 1)),
            Err(StateError::Duplicate {
                key: trade(1_000, 1).canonical_key()
            })
        );
        engine.apply(&mark(2_000, 1)).unwrap();
    }

    #[test]
    fn other_streams_leave_the_derivatives_unchanged() {
        let mut engine = engine_after(&[
            trade(1_000, 1),
            crate::event::samples::liquidation(2_000, 10_000_000),
            crate::event::samples::open_interest(3_000, 10_000),
            mark(4_000, 6_354_150_000_000),
            crate::event::samples::settlement(5_000, 10_000),
        ]);
        let derivatives = engine.state().derivatives;
        assert!(derivatives.oi.is_ready());
        assert!(derivatives.mark.is_ready());
        assert!(derivatives.funding_settled.is_ready());
        for event in [
            trade(6_000, 2),
            snapshot(6_001, 10),
            gap(Stream::OrderBook, 6_000, 6_002, GapReason::Disconnected),
            gap(Stream::Trades, 6_000, 6_003, GapReason::Disconnected),
            update(6_004, 11, 12, 10),
            kline(-53_995, 6_004),
        ] {
            engine.apply(&event).unwrap();
            assert_eq!(engine.state().derivatives, derivatives, "{event:?}");
        }
    }

    /// A synced book: bids 63542.00/63541.90 and asks 63542.10/63542.20,
    /// last update id 100, at 1 000 ms.
    fn book_snapshot(millis: i64) -> MarketEvent {
        let level = |price: i64, qty: i64| crate::event::Level {
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
        };
        MarketEvent::BookSnapshot(crate::event::BookSnapshot {
            time: t(millis),
            last_update_id: 100,
            bids: vec![
                level(6_354_200_000_000, 300_000_000),
                level(6_354_190_000_000, 100_000_000),
            ],
            asks: vec![
                level(6_354_210_000_000, 120_000_000),
                level(6_354_220_000_000, 80_000_000),
            ],
        })
    }

    /// An update setting the best bid to `qty_units`.
    fn book_update(millis: i64, first: u64, last: u64, prev: u64, qty_units: i64) -> MarketEvent {
        MarketEvent::BookUpdate(crate::event::BookUpdate {
            time: t(millis),
            first_update_id: first,
            last_update_id: last,
            prev_update_id: prev,
            bids: vec![crate::event::Level {
                price: Price::from_units(6_354_200_000_000),
                qty: Qty::from_units(qty_units),
            }],
            asks: Vec::new(),
        })
    }

    #[test]
    fn rejects_an_event_that_overflows_a_liquidity_sum() {
        // Two updates in one minute raise a bid each to i64::MAX units: the
        // minute's added bid liquidity overflows on the second one.
        let raise = |millis: i64, first: u64, last: u64, prev: u64, price: i64| {
            MarketEvent::BookUpdate(crate::event::BookUpdate {
                time: t(millis),
                first_update_id: first,
                last_update_id: last,
                prev_update_id: prev,
                bids: vec![crate::event::Level {
                    price: Price::from_units(price),
                    qty: Qty::from_units(i64::MAX),
                }],
                asks: Vec::new(),
            })
        };
        let mut engine = engine_after(&[
            book_snapshot(1_000),
            raise(1_100, 95, 105, 0, 6_354_200_000_000),
        ]);
        assert!(engine.state().book.l2.is_ready());
        let before = engine.state().clone();
        let closed_before = engine.closed_bars().to_vec();
        let overflow = raise(1_200, 106, 110, 105, 6_354_190_000_000);
        assert_eq!(
            engine.apply(&overflow),
            Err(StateError::Overflow {
                event: overflow.canonical_key()
            })
        );
        // The state, the book with it, and the closed bars are unchanged.
        assert_eq!(engine.state(), &before);
        assert_eq!(engine.state().book, before.book);
        assert_eq!(engine.closed_bars(), closed_before);
        // The ordering bound did not move: the book takes the next update
        // in the chain.
        engine
            .apply(&book_update(1_200, 106, 110, 105, 200_000_000))
            .unwrap();
        assert_ne!(engine.state().book, before.book);
    }

    #[test]
    fn other_streams_leave_the_book_unchanged() {
        let mut engine = engine_after(&[
            book_snapshot(1_000),
            book_update(1_100, 95, 105, 0, 250_000_000),
        ]);
        let book = engine.state().book.clone();
        assert!(book.l2.is_ready());
        assert!(book.depth.is_ready());
        assert!(book.clusters.is_ready());
        for event in [
            mark(2_000, 1),
            crate::event::samples::liquidation(2_001, 10_000_000),
            crate::event::samples::open_interest(2_002, 10_000),
            crate::event::samples::settlement(2_003, 10_000),
            kline(-57_997, 2_003),
            gap(Stream::MarkPrice, 2_000, 2_004, GapReason::Disconnected),
        ] {
            engine.apply(&event).unwrap();
            assert_eq!(engine.state().book, book, "{event:?}");
        }
    }

    #[test]
    fn rejects_an_earlier_time() {
        let last = trade(2_000, 1);
        let late = trade(1_999, 2);
        assert_rejected(
            Some(&last),
            &late,
            StateError::OutOfOrder {
                last: last.canonical_key(),
                event: late.canonical_key(),
            },
        );
    }

    #[test]
    fn rejects_a_lower_rank_in_the_same_millisecond() {
        let last = mark(1_000, 1);
        let lower = trade(1_000, u64::MAX);
        assert_rejected(
            Some(&last),
            &lower,
            StateError::OutOfOrder {
                last: last.canonical_key(),
                event: lower.canonical_key(),
            },
        );
        // A gap at the same millisecond after the resumed event is late too.
        let resumed = snapshot(1_000, 10);
        let gap_after = gap(Stream::OrderBook, 900, 1_000, GapReason::Disconnected);
        assert_rejected(
            Some(&resumed),
            &gap_after,
            StateError::OutOfOrder {
                last: resumed.canonical_key(),
                event: gap_after.canonical_key(),
            },
        );
    }

    #[test]
    fn rejects_a_lower_seq_of_the_same_kind() {
        let last = trade(1_000, 5);
        let lower = trade(1_000, 4);
        assert_rejected(
            Some(&last),
            &lower,
            StateError::OutOfOrder {
                last: last.canonical_key(),
                event: lower.canonical_key(),
            },
        );
    }

    #[test]
    fn rejects_same_key_events_in_the_wrong_structural_order() {
        let higher = mark(1_000, 200);
        let lower = mark(1_000, 100);
        assert_eq!(higher.canonical_key(), lower.canonical_key());
        assert_rejected(
            Some(&higher),
            &lower,
            StateError::OutOfOrder {
                last: higher.canonical_key(),
                event: lower.canonical_key(),
            },
        );
        // The right order is accepted.
        engine_after(&[lower, higher]);
    }

    #[test]
    fn rejects_a_duplicate() {
        for event in [
            trade(1_000, 1),
            mark(1_000, 1),
            gap(Stream::Trades, 900, 1_000, GapReason::SequenceBreak),
        ] {
            assert_rejected(
                Some(&event),
                &event,
                StateError::Duplicate {
                    key: event.canonical_key(),
                },
            );
        }
    }

    #[test]
    fn rejects_a_gap_that_ends_before_it_starts() {
        let inverted = gap(Stream::Klines, 2_001, 2_000, GapReason::MissingData);
        let err = StateError::InvalidGap {
            start: t(2_001),
            end: t(2_000),
        };
        assert_rejected(None, &inverted, err);
        assert_rejected(Some(&trade(1_000, 1)), &inverted, err);
        // Validity is checked before order: an inverted gap that is also late
        // is reported as invalid.
        assert_rejected(Some(&trade(3_000, 1)), &inverted, err);

        // The rejected gap did not move the bound to its end.
        let mut engine = engine_after(&[trade(1_000, 1)]);
        engine
            .apply(&gap(
                Stream::Trades,
                1_000_000,
                9_000,
                GapReason::Disconnected,
            ))
            .unwrap_err();
        engine.apply(&trade(1_500, 2)).unwrap();
    }

    #[test]
    fn accepts_a_single_instant_gap() {
        let engine = engine_after(&[
            trade(1_000, 1),
            gap(Stream::Trades, 2_000, 2_000, GapReason::LateEvent),
            trade(2_000, 2),
        ]);
        assert_eq!(engine.state().as_of, Some(t(2_000)));
        assert_eq!(engine.state().trade_count, 2);
    }

    /// A trade with `trade_id` at `millis` and a quantity of `qty_units`.
    fn sized_trade(millis: i64, trade_id: u64, qty_units: i64) -> MarketEvent {
        let MarketEvent::Trade(base) = trade(millis, trade_id) else {
            unreachable!("samples::trade builds a trade")
        };
        MarketEvent::Trade(crate::event::Trade {
            qty: crate::num::Qty::from_units(qty_units),
            ..base
        })
    }

    /// A trade with `trade_id` at `millis` and a price of `price_units`.
    fn priced_trade(millis: i64, trade_id: u64, price_units: i64) -> MarketEvent {
        let MarketEvent::Trade(base) = trade(millis, trade_id) else {
            unreachable!("samples::trade builds a trade")
        };
        MarketEvent::Trade(crate::event::Trade {
            price: Price::from_units(price_units),
            ..base
        })
    }

    #[test]
    fn late_event_gap_must_end_after_the_last_released_millisecond() {
        // A mark price at 2_000 was released; a trade for 1_500 arrives late.
        let released = mark(2_000, 1);
        let same_ms = gap(Stream::Trades, 1_500, 2_000, GapReason::LateEvent);
        assert_rejected(
            Some(&released),
            &same_ms,
            StateError::OutOfOrder {
                last: released.canonical_key(),
                event: same_ms.canonical_key(),
            },
        );
        // The same holds after a released trade at that millisecond.
        let released_trade = trade(2_000, 9);
        assert_rejected(
            Some(&released_trade),
            &same_ms,
            StateError::OutOfOrder {
                last: released_trade.canonical_key(),
                event: same_ms.canonical_key(),
            },
        );
        // One millisecond later sorts strictly after it and is accepted.
        let engine = engine_after(&[
            released,
            gap(Stream::Trades, 1_500, 2_001, GapReason::LateEvent),
            trade(2_001, 10),
        ]);
        assert_eq!(engine.state().as_of, Some(t(2_001)));
    }

    #[test]
    fn rejects_a_repeated_trade_id_with_a_different_payload() {
        let first = priced_trade(1_000, 7, 100);
        let same_ms = priced_trade(1_000, 7, 101);
        assert!(
            first < same_ms,
            "sorts after, so only the id check catches it"
        );
        assert_rejected(
            Some(&first),
            &same_ms,
            StateError::Duplicate {
                key: same_ms.canonical_key(),
            },
        );
        let later = priced_trade(1_500, 7, 100);
        assert_rejected(
            Some(&first),
            &later,
            StateError::Duplicate {
                key: later.canonical_key(),
            },
        );
        let mut engine = engine_after(&[first]);
        engine.apply(&same_ms).unwrap_err();
        assert_eq!(engine.state().trade_count, 1);
    }

    #[test]
    fn rejects_a_trade_id_regression_at_a_later_time() {
        let first = trade(1_000, 7);
        let regressed = trade(2_000, 5);
        let err = StateError::IdRegression {
            stream: Stream::Trades,
            last_id: 7,
            event: regressed.canonical_key(),
        };
        assert_rejected(Some(&first), &regressed, err);

        // The rejection did not lower the bound; the next id is accepted.
        let mut engine = engine_after(&[first]);
        assert_eq!(engine.apply(&regressed), Err(err));
        assert_eq!(
            engine.apply(&trade(2_000, 6)),
            Err(StateError::IdRegression {
                stream: Stream::Trades,
                last_id: 7,
                event: trade(2_000, 6).canonical_key(),
            })
        );
        engine.apply(&trade(2_000, 8)).unwrap();
        // Gaps carry no exchange id and do not reset the bound.
        engine
            .apply(&gap(Stream::Trades, 2_001, 3_000, GapReason::SequenceBreak))
            .unwrap();
        assert!(matches!(
            engine.apply(&trade(3_000, 8)),
            Err(StateError::Duplicate { .. })
        ));
        engine.apply(&trade(3_000, 9)).unwrap();
    }

    #[test]
    fn rejects_repeated_or_regressing_book_ids() {
        let snap = snapshot(1_000, 100);
        let repeated_snap = snapshot(2_000, 100);
        assert_rejected(
            Some(&snap),
            &repeated_snap,
            StateError::Duplicate {
                key: repeated_snap.canonical_key(),
            },
        );
        let older_snap = snapshot(2_000, 99);
        assert_rejected(
            Some(&snap),
            &older_snap,
            StateError::IdRegression {
                stream: Stream::OrderBook,
                last_id: 100,
                event: older_snap.canonical_key(),
            },
        );

        let diff = update(1_000, 101, 105, 100);
        let repeated_diff = update(2_000, 101, 105, 100);
        assert_rejected(
            Some(&diff),
            &repeated_diff,
            StateError::Duplicate {
                key: repeated_diff.canonical_key(),
            },
        );
        let older_diff = update(2_000, 99, 104, 98);
        assert_rejected(
            Some(&diff),
            &older_diff,
            StateError::IdRegression {
                stream: Stream::OrderBook,
                last_id: 105,
                event: older_diff.canonical_key(),
            },
        );

        // Across kinds: an update below the snapshot (same millisecond, so
        // it sorts after it by rank) regresses; equal ids are allowed both
        // ways.
        let stale = update(1_000, 90, 99, 89);
        assert_rejected(
            Some(&snap),
            &stale,
            StateError::IdRegression {
                stream: Stream::OrderBook,
                last_id: 100,
                event: stale.canonical_key(),
            },
        );
        let stale_snap = snapshot(2_000, 104);
        assert_rejected(
            Some(&diff),
            &stale_snap,
            StateError::IdRegression {
                stream: Stream::OrderBook,
                last_id: 105,
                event: stale_snap.canonical_key(),
            },
        );
        engine_after(&[
            snapshot(1_000, 100),
            update(1_000, 95, 100, 94),
            update(1_001, 101, 105, 100),
            snapshot(2_000, 105),
            update(2_000, 106, 106, 105),
        ]);
    }

    /// The engine of `location.levels@1` (ADR-044 D10).
    fn levels_v1_engine() -> MarketStateEngine {
        let features = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key]).unwrap();
        MarketStateEngine::with_features(features).unwrap()
    }

    #[test]
    fn with_features_accepts_the_current_set_as_new_does() {
        let engine = MarketStateEngine::with_features(catalog::current_set()).unwrap();
        assert_eq!(engine.feature_set(), &catalog::current_set());
        assert_eq!(engine.state(), MarketStateEngine::new().state());
        let mut built = engine;
        let mut new = MarketStateEngine::new();
        for event in one_of_each(1_000) {
            built.apply(&event).unwrap();
            new.apply(&event).unwrap();
            assert_eq!(built.state(), new.state());
        }
    }

    #[test]
    fn with_features_derives_the_structure_rules() {
        let engines = [
            MarketStateEngine::new(),
            MarketStateEngine::with_features(catalog::current_set()).unwrap(),
            levels_v1_engine(),
        ];
        for engine in &engines {
            assert_eq!(engine.structure.rules(), [LevelsRule::V1; 4]);
        }
    }

    #[test]
    fn with_features_selects_location_levels_v1_and_stamps_its_set() {
        let features = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key]).unwrap();
        let engine = MarketStateEngine::with_features(features.clone()).unwrap();
        assert_eq!(engine.feature_set(), &features);
        assert_eq!(engine.state().feature_set, features.version());
        assert_ne!(features.version(), catalog::current_set().version());
        assert!(
            features
                .definitions()
                .any(|definition| definition.key == catalog::LOCATION_LEVELS_V1.key)
        );
        // The registry carries the selected version.
        let mut engine = engine;
        engine.apply(&trade(1_000, 1)).unwrap();
        engine.apply(&trade(61_000, 2)).unwrap();
        let set = engine.state().location.levels.ready().unwrap();
        assert_eq!(set.feature, catalog::LOCATION_LEVELS_V1.key);
        let mut current = MarketStateEngine::new();
        current.apply(&trade(1_000, 1)).unwrap();
        current.apply(&trade(61_000, 2)).unwrap();
        let set = current.state().location.levels.ready().unwrap();
        assert_eq!(set.feature, catalog::LOCATION_LEVELS_V2.key);
    }

    #[test]
    fn with_features_refuses_a_set_without_an_always_computed_id() {
        // The default set before #96: closed, but without
        // `profile.volume.composite_7d@1`, which the engine computes anyway.
        let keys: Vec<FeatureKey> = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key])
            .unwrap()
            .definitions()
            .map(|definition| definition.key)
            .filter(|key| *key != catalog::PROFILE_VOLUME_COMPOSITE_7D_V1.key)
            .collect();
        let set = FeatureSet::new(&catalog::registry(), &keys).unwrap();
        let error = MarketStateEngine::with_features(set).unwrap_err();
        assert_eq!(
            error,
            UnsupportedFeatureSet::Missing {
                feature: catalog::PROFILE_VOLUME_COMPOSITE_7D_V1.key
            }
        );
        assert_eq!(
            error.to_string(),
            "the set omits profile.volume.composite_7d, which the engine always computes \
             (profile.volume.composite_7d@1)"
        );
    }

    /// `trade.last_price@2`: registered, but not computed by the engine.
    static TRADE_LAST_PRICE_V2: crate::feature::FeatureDefinition =
        crate::feature::FeatureDefinition {
            key: FeatureKey::new("trade.last_price", 2),
            params: &[],
            inputs: &[crate::feature::Input::Stream(Stream::Trades)],
            warm_up: crate::feature::WarmUp::Samples(2),
        };

    #[test]
    fn with_features_refuses_a_version_it_cannot_compute() {
        let mut definitions = catalog::DEFINITIONS.to_vec();
        definitions.push(&TRADE_LAST_PRICE_V2);
        let registry = crate::feature::FeatureRegistry::new(&definitions).unwrap();
        let keys: Vec<FeatureKey> = catalog::CURRENT
            .iter()
            .map(|key| {
                if *key == catalog::TRADE_LAST_PRICE_V1.key {
                    TRADE_LAST_PRICE_V2.key
                } else {
                    *key
                }
            })
            .collect();
        let set = FeatureSet::new(&registry, &keys).unwrap();
        let error = MarketStateEngine::with_features(set).unwrap_err();
        assert_eq!(
            error,
            UnsupportedFeatureSet::NotComputed {
                feature: TRADE_LAST_PRICE_V2.key
            }
        );
        assert_eq!(
            error.to_string(),
            "the engine cannot compute trade.last_price@2"
        );
    }

    /// `trade.last_price@1` under a warm-up the engine does not compute.
    static TRADE_LAST_PRICE_V1_REDEFINED: crate::feature::FeatureDefinition =
        crate::feature::FeatureDefinition {
            key: FeatureKey::new("trade.last_price", 1),
            params: &[],
            inputs: &[crate::feature::Input::Stream(Stream::Trades)],
            warm_up: crate::feature::WarmUp::Samples(999),
        };

    #[test]
    fn with_features_refuses_a_member_defined_other_than_the_catalog() {
        // Same keys as `CURRENT`, but one definition differs: the stamp
        // would name a warm-up of 999 trades while the engine computes 1.
        let definitions: Vec<&'static crate::feature::FeatureDefinition> = catalog::DEFINITIONS
            .iter()
            .map(|definition| {
                if definition.key == catalog::TRADE_LAST_PRICE_V1.key {
                    &TRADE_LAST_PRICE_V1_REDEFINED
                } else {
                    *definition
                }
            })
            .collect();
        let registry = crate::feature::FeatureRegistry::new(&definitions).unwrap();
        let set = FeatureSet::new(&registry, catalog::CURRENT).unwrap();
        assert_ne!(set.version(), catalog::current_set().version());
        let error = MarketStateEngine::with_features(set).unwrap_err();
        assert_eq!(
            error,
            UnsupportedFeatureSet::Redefined {
                feature: catalog::TRADE_LAST_PRICE_V1.key
            }
        );
        assert_eq!(
            error.to_string(),
            "the set defines trade.last_price@1 other than the catalog the engine computes"
        );
    }

    #[test]
    fn v1_and_v2_differ_only_in_the_registry() {
        // ADR-044 D10 and the #96 rule I: `@2` changes only the composite
        // that feeds the registry, so with the stamp, the registry and the
        // candidate trigger values it gates (ADR-053 D4) set equal the two
        // states are equal after every event, and the auction facts are
        // the same.
        let mut v1 = levels_v1_engine();
        let mut v2 = MarketStateEngine::new();
        let mut differed = 0;
        let mut gated = 0;
        let auction = |engine: &MarketStateEngine| -> Vec<LocationEvent> {
            engine
                .location_events()
                .iter()
                .filter(|fact| matches!(fact, LocationEvent::Auction { .. }))
                .copied()
                .collect()
        };
        for event in crate::location::tests::walk_tape(0x6d69_6500_0000_0096, 10) {
            v1.apply(&event).unwrap();
            v2.apply(&event).unwrap();
            let mut aligned = v1.state().clone();
            assert_ne!(aligned.feature_set, v2.state().feature_set);
            aligned.feature_set = v2.state().feature_set;
            if aligned.location.levels != v2.state().location.levels {
                differed += 1;
            }
            aligned.location.levels = v2.state().location.levels.clone();
            // The candidate trigger values depend on the registry by design
            // (ADR-053 D4): the gate reads the selected registry. Every other
            // trigger value, and the bias, must agree.
            let triggers = &v2.state().triggers;
            if aligned.triggers != *triggers {
                gated += 1;
            }
            assert!(
                aligned.triggers.equal_but_registry(triggers),
                "{}",
                event.time()
            );
            aligned.triggers = triggers.clone();
            assert_eq!(&aligned, v2.state(), "{}", event.time());
            assert_eq!(auction(&v1), auction(&v2), "{}", event.time());
        }
        // The registries do differ once a composite is ready.
        assert!(v2.state().profile.composite_7d.is_ready());
        assert!(differed > 1_000, "{differed}");
        // …and so do the candidate trigger values the gate reads them for.
        assert!(gated > 0, "{gated}");
    }

    #[test]
    fn errors_describe_themselves() {
        let last = trade(2_000, 1).canonical_key();
        let event = trade(1_999, 2).canonical_key();
        assert_eq!(
            StateError::OutOfOrder { last, event }.to_string(),
            "event (1999ms Trade seq 2) sorts before the last consumed event (2000ms Trade seq 1)"
        );
        assert_eq!(
            StateError::Duplicate { key: last }.to_string(),
            "event (2000ms Trade seq 1) repeats the last consumed event"
        );
        assert_eq!(
            StateError::IdRegression {
                stream: Stream::Trades,
                last_id: 7,
                event,
            }
            .to_string(),
            "event (1999ms Trade seq 2) has an exchange id below 7, the last accepted on Trades"
        );
        assert_eq!(
            StateError::InvalidGap {
                start: t(5),
                end: t(4)
            }
            .to_string(),
            "feed gap starts at 5ms, after its end at 4ms"
        );
        assert_eq!(
            StateError::Overflow { event }.to_string(),
            "event (1999ms Trade seq 2) overflows the bar, volatility, order-flow, \
             volume-profile, structure, derivatives, order-book liquidity or VWAP \
             arithmetic"
        );
        assert_eq!(
            StateError::TimeJump {
                event,
                timeframe: Timeframe::M1,
                from: t(0),
                bars: 44_641,
            }
            .to_string(),
            "event (1999ms Trade seq 2) would close 44641 1m bars from 0ms, \
             more than the 44640 one event may close"
        );
    }
}
