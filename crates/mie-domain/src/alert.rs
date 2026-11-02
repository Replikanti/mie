//! Informational alerts (brief §17, ADR-010; ADR-059, proposed).
//!
//! Alerts are **decided** here and only rendered and delivered outside the
//! domain, so live ingestion and a replay dry-run share one decision path
//! (ADR-019, ADR-059 D1). [`AlertMonitor`] turns the engine's committed
//! facts after each accepted event into [`Alert`]s under the policy
//! `alert.policy@1` ([`ALERT_POLICY_V1`], D2):
//!
//! - **Zone alerts** (D2a, D2b): [`Alert::ZoneEntered`] on a
//!   [`LocationEvent::Entered`] of a monitored level — the POC, VAH and VAL
//!   of `profile.volume.prior_day@1` ([`is_monitored`]) —, and
//!   [`Alert::ZoneLeft`] on its `Left { cause: Departed }`, only for a visit
//!   whose enter was emitted. A visit whose level leaves the registry
//!   (`Retired` at the rollover, or a fresh engine after a chain restart,
//!   ADR-052 D4) closes without an alert. The debounce is ADR-044 D7's own.
//! - **Guide** (D2c): a VAH or VAL enter carries a [`Guide`] while
//!   `location.auction.prior_day@1` is ready: from the accepted region, the
//!   close that starts a probe across the edge, the closes counted toward
//!   acceptance, and both outcomes with explicit prices — each the stated
//!   invalidation of the other. Side-neutral by construction.
//! - **Confirmations** (D2d): [`Alert::TriggerFired`] for the three
//!   auction triggers the guide names ([`ALERT_TRIGGERS`]) only.
//! - **Regime** (D2e): [`Alert::RegimeChanged`] when the label of
//!   `volatility.regime.1h@1` differs between two consecutive ready values
//!   with no not-ready stretch between them: a stretch where the regime is
//!   not ready (warming up again after a feed hole, or a fresh engine after
//!   a chain restart) forgets the label.
//! - **Market report** (D2f): [`Alert::Report`] at every event that closes a
//!   1d bar, a [`MarketReport`] read from the committed state.
//!
//! Within one event the alerts come in a fixed order (D1): every
//! `ZoneLeft`, every `ZoneEntered` (both in the order of the location facts,
//! ADR-044 D7), the triggers in emission order (ADR-053 D2), the regime
//! change, the report. The renderer bundles them into one message.
//!
//! The monitor is pure: three fixed visit slots, no clock, no hash-ordered
//! collection. It is seeded from the state the engine holds before its
//! first observed event (D2g), so decisions at or after `emit_from` do not
//! depend on where observation started. It must be called after accepted
//! events only (the ADR-054 D6 reason: after a rejected event the engine's
//! facts still show the previous event).
//!
//! No type, field or `Display` text says bullish, bearish, long, short,
//! signal, reversal, continuation or entry, and nothing carries an
//! execution meaning (ADR-010, ADR-053 naming rule). `Display` for
//! [`Alert`] is a canonical line for goldens and logs; the text a person
//! reads is rendered outside the domain.

use crate::bars::Timeframe;
use crate::event::MarketEvent;
use crate::feature::{FeatureKey, FeatureValue, catalog};
use crate::fingerprint::{Fingerprint, Fingerprinter};
use crate::location::{
    ACCEPTANCE_CLOSES, AuctionStatus, EnterCause, LeaveCause, LevelKind, LocationEvent,
    LocationLevel, Region, TOLERANCE_BPS, Vwap,
};
use crate::num::Price;
use crate::regime::{Regime, RegimeLabel};
use crate::research::{DataVersion, EngineKey, PipelineKey};
use crate::state::{MarketState, MarketStateEngine};
use crate::state_hash::STATE_HASH_ENCODING;
use crate::time::EventTime;
use crate::trigger::TriggerEvent;
use std::fmt;

/// The alert policy (ADR-059 D2): the monitored levels, the zone, guide,
/// confirmation, regime and report rules, the seeding and the bundling
/// order. Any change to them is a new version.
pub const ALERT_POLICY_V1: PipelineKey = PipelineKey::new("alert.policy", 1);

/// The level source whose levels are monitored (D2a): the prior UTC day's
/// volume profile, the auction classifier's reference (ADR-044 D8).
pub const MONITORED_SOURCE: FeatureKey = catalog::PROFILE_VOLUME_PRIOR_DAY_V1.key;

/// The monitored level kinds of [`MONITORED_SOURCE`] (D2a), in slot order.
pub const MONITORED_KINDS: [LevelKind; 3] = [LevelKind::Poc, LevelKind::Vah, LevelKind::Val];

/// The trigger ids that are alerted (D2d): the outcomes a [`Guide`] names.
pub const ALERT_TRIGGERS: [FeatureKey; 3] = [
    catalog::TRIGGER_FAILED_AUCTION_V1.key,
    catalog::TRIGGER_FAILED_RECLAIM_V1.key,
    catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key,
];

/// The canonical parameter text of [`ALERT_POLICY_V1`], which
/// [`policy_fingerprint`] hashes:
///
/// ```text
/// mie-alert-policy 1
/// policy alert.policy@1
/// levels source=profile.volume.prior_day@1 kinds=poc,vah,val
/// zone_entered on=entered causes=arrived,created
/// zone_left on=left causes=departed requires=emitted_zone_entered silent=retired,vanished
/// guide on=zone_entered kinds=vah,val requires=location.auction.prior_day@1 tolerance_bps=6 acceptance_closes=60
/// trigger ids=trigger.failed_auction@1,trigger.failed_reclaim@1,trigger.breakout_acceptance@1
/// regime feature=volatility.regime.1h@1 on=label_change_between_consecutive_ready_values not_ready=forget hysteresis=none
/// report on=1d_close
/// seed regime=ready_label visits=in_zone_not_alerted
/// bundle per=event order=zone_left,zone_entered,trigger,regime,report
/// ```
///
/// Every line ends in `\n`.
pub fn policy_text() -> String {
    let kinds: Vec<String> = MONITORED_KINDS.iter().map(ToString::to_string).collect();
    let triggers: Vec<String> = ALERT_TRIGGERS.iter().map(ToString::to_string).collect();
    let mut text = String::from("mie-alert-policy 1\n");
    text.push_str(&format!("policy {ALERT_POLICY_V1}\n"));
    text.push_str(&format!(
        "levels source={MONITORED_SOURCE} kinds={}\n",
        kinds.join(",")
    ));
    text.push_str("zone_entered on=entered causes=arrived,created\n");
    text.push_str(
        "zone_left on=left causes=departed requires=emitted_zone_entered \
         silent=retired,vanished\n",
    );
    text.push_str(&format!(
        "guide on=zone_entered kinds=vah,val requires={} tolerance_bps={TOLERANCE_BPS} \
         acceptance_closes={ACCEPTANCE_CLOSES}\n",
        catalog::LOCATION_AUCTION_PRIOR_DAY_V1.key
    ));
    text.push_str(&format!("trigger ids={}\n", triggers.join(",")));
    text.push_str(&format!(
        "regime feature={} on=label_change_between_consecutive_ready_values \
         not_ready=forget hysteresis=none\n",
        catalog::VOLATILITY_REGIME_1H_V1.key
    ));
    text.push_str("report on=1d_close\n");
    text.push_str("seed regime=ready_label visits=in_zone_not_alerted\n");
    text.push_str("bundle per=event order=zone_left,zone_entered,trigger,regime,report\n");
    text
}

/// FNV-1a 64 ([`Fingerprinter::write_str`]) of [`policy_text`].
pub fn policy_fingerprint() -> Fingerprint {
    let mut hasher = Fingerprinter::new();
    hasher.write_str(&policy_text());
    hasher.finish()
}

/// Where a run of alerts comes from, delivered once per run (the
/// provenance of an alert log, ADR-059 D1).
///
/// `Display` prints `data=<64 hex> engine=<engine key> policy=<id@N>
/// policy_fingerprint=<16 hex>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertProvenance {
    /// The version of the raw data replayed (ADR-030).
    pub data: DataVersion,
    /// The engine: its feature-set version and the state-hash encoding.
    pub engine: EngineKey,
    /// The alert policy: [`ALERT_POLICY_V1`].
    pub policy: PipelineKey,
    /// [`policy_fingerprint`].
    pub policy_fingerprint: Fingerprint,
}

impl AlertProvenance {
    /// The provenance of alerts decided on `engine` over the raw data
    /// `data` under [`ALERT_POLICY_V1`].
    pub fn new(data: DataVersion, engine: &MarketStateEngine) -> Self {
        Self {
            data,
            engine: EngineKey {
                features: engine.state().feature_set,
                state_encoding: STATE_HASH_ENCODING,
            },
            policy: ALERT_POLICY_V1,
            policy_fingerprint: policy_fingerprint(),
        }
    }
}

impl fmt::Display for AlertProvenance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "data={} engine={} policy={} policy_fingerprint={}",
            self.data, self.engine, self.policy, self.policy_fingerprint
        )
    }
}

/// Whether zone alerts are decided for `level` (D2a): a POC, VAH or VAL of
/// [`MONITORED_SOURCE`].
pub fn is_monitored(level: &LocationLevel) -> bool {
    level.source == MONITORED_SOURCE && slot(level.kind).is_some()
}

/// The visit slot of a monitored kind.
fn slot(kind: LevelKind) -> Option<usize> {
    MONITORED_KINDS
        .iter()
        .position(|monitored| *monitored == kind)
}

/// The tolerance `w` of ADR-044 D3 at `price`, on the fixed-point grid:
/// `⌊|price| · TOLERANCE_BPS / 10 000⌋`. For an integer gap `g`,
/// `g · 10 000 > |price| · TOLERANCE_BPS` exactly when `g > w`, so the
/// thresholds below reproduce the classifier's exact tests.
pub fn tolerance(price: Price) -> Price {
    let units = i128::from(price.units()).abs() * i128::from(TOLERANCE_BPS) / 10_000;
    Price::from_units(i64::try_from(units).expect("a fraction of a price fits its range"))
}

/// `price` shifted by `units`, saturating at the `Price` range.
fn shift(price: Price, units: i64) -> Price {
    Price::from_units(price.units().saturating_add(units))
}

/// A test on the close of a 1m bar with trades, exact on the fixed-point
/// grid (D2c).
///
/// `Display` prints `>p`, `>=p`, `<p`, `(a,b)` or `[a,b)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseTest {
    /// `close > p`.
    Above(Price),
    /// `close ≥ p`.
    AtOrAbove(Price),
    /// `close < p`.
    Below(Price),
    /// `low < close < high`: region In of ADR-044 D8.
    Between {
        /// The exclusive lower bound.
        low: Price,
        /// The exclusive upper bound.
        high: Price,
    },
    /// `low ≤ close < high`: the value area itself.
    Within {
        /// The inclusive lower bound.
        low: Price,
        /// The exclusive upper bound.
        high: Price,
    },
}

impl CloseTest {
    /// Whether `close` passes the test.
    pub fn holds(&self, close: Price) -> bool {
        match *self {
            Self::Above(price) => close > price,
            Self::AtOrAbove(price) => close >= price,
            Self::Below(price) => close < price,
            Self::Between { low, high } => low < close && close < high,
            Self::Within { low, high } => low <= close && close < high,
        }
    }
}

impl fmt::Display for CloseTest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Above(price) => write!(f, ">{price}"),
            Self::AtOrAbove(price) => write!(f, ">={price}"),
            Self::Below(price) => write!(f, "<{price}"),
            Self::Between { low, high } => write!(f, "({low},{high})"),
            Self::Within { low, high } => write!(f, "[{low},{high})"),
        }
    }
}

/// What would decide a probe across a value edge (D2c): the rules of
/// ADR-044 D8/D9 and ADR-053 D5 at the edge just entered, from the accepted
/// region, with every price explicit. The failure and the acceptance are
/// each the stated invalidation of the other. Side-neutral: it names
/// regions and rules, never a direction to act on.
///
/// `Display` prints `edge=<kind> accepted=<region> toward=<region>
/// starts=<test> counts=<test> fails=<test> failure=<id> acceptance=<id|value>
/// closes=<n>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Guide {
    /// The edge entered: [`LevelKind::Vah`] or [`LevelKind::Val`].
    pub edge: LevelKind,
    /// The reference's VAL (inclusive).
    pub val: Price,
    /// The reference's VAH (exclusive).
    pub vah: Price,
    /// The accepted region when the zone was entered.
    pub accepted: Region,
    /// The region a probe across this edge goes toward.
    pub toward: Region,
    /// A close that starts the probe: a close in `toward`.
    pub starts: CloseTest,
    /// A close counted toward acceptance: beyond the edge itself.
    pub counts: CloseTest,
    /// A close that fails the probe: back in the accepted region.
    pub fails: CloseTest,
    /// The trigger a failure fires: `trigger.failed_auction@1` for a probe
    /// out of value, `trigger.failed_reclaim@1` for one back into it.
    pub failure: FeatureKey,
    /// The trigger the acceptance fires: `trigger.breakout_acceptance@1`
    /// for a probe out of value; `None` for one back into value, whose
    /// acceptance no `@1` trigger names.
    pub acceptance: Option<FeatureKey>,
    /// Counted closes that accept the probe: [`ACCEPTANCE_CLOSES`].
    pub closes: u32,
}

impl Guide {
    /// The guide at the VAH or VAL `level` just entered, from the auction
    /// `status` after the same event; `None` for any other kind or when
    /// `status` holds another reference.
    pub fn at(level: &LocationLevel, status: &AuctionStatus) -> Option<Self> {
        let edge_price = match level.kind {
            LevelKind::Vah => status.vah,
            LevelKind::Val => status.val,
            _ => return None,
        };
        if edge_price != level.price {
            return None;
        }
        let accepted = status.accepted;
        let toward = match (level.kind, accepted) {
            (LevelKind::Vah, Region::Above) | (LevelKind::Val, Region::Below) => Region::In,
            (LevelKind::Vah, _) => Region::Above,
            _ => Region::Below,
        };
        let (val, vah) = (status.val, status.vah);
        let region = |region: Region| match region {
            Region::Above => CloseTest::Above(shift(vah, tolerance(vah).units())),
            Region::Below => CloseTest::Below(shift(val, -tolerance(val).units())),
            Region::In => CloseTest::Between {
                low: shift(val, tolerance(val).units()),
                high: shift(vah, -tolerance(vah).units()),
            },
        };
        let counts = match toward {
            Region::Above => CloseTest::AtOrAbove(vah),
            Region::Below => CloseTest::Below(val),
            Region::In => CloseTest::Within {
                low: val,
                high: vah,
            },
        };
        let out_of_value = toward != Region::In;
        Some(Self {
            edge: level.kind,
            val,
            vah,
            accepted,
            toward,
            starts: region(toward),
            counts,
            fails: region(accepted),
            failure: if out_of_value {
                catalog::TRIGGER_FAILED_AUCTION_V1.key
            } else {
                catalog::TRIGGER_FAILED_RECLAIM_V1.key
            },
            acceptance: out_of_value.then_some(catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key),
            closes: ACCEPTANCE_CLOSES,
        })
    }
}

impl fmt::Display for Guide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "edge={} accepted={} toward={} starts={} counts={} fails={} failure={} acceptance=",
            self.edge,
            self.accepted,
            self.toward,
            self.starts,
            self.counts,
            self.fails,
            self.failure
        )?;
        match self.acceptance {
            Some(key) => write!(f, "{key}")?,
            None => f.write_str("value")?,
        }
        write!(f, " closes={}", self.closes)
    }
}

/// The market as of an event, for a report (D2f): read from the committed
/// state, nothing derived.
///
/// `Display` prints `as_of=<time|-> price=<value> regime=<value>
/// vwap=<value> auction=<value> levels=<kind@price[low,high][*],…|value>`,
/// where `*` marks a level in zone and a value that is not ready prints as
/// `warming` or `unavailable`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketReport {
    /// The ordering time of the last consumed event; `None` before any.
    pub as_of: Option<EventTime>,
    /// `trade.last_price@1`.
    pub last_price: FeatureValue<Price>,
    /// `volatility.regime.1h@1`: label and raw percentile.
    pub regime: FeatureValue<Regime>,
    /// The monitored levels ([`is_monitored`]) in registry order, with their
    /// zones and in-zone flags; those in zone are the active monitors.
    pub levels: FeatureValue<Vec<LocationLevel>>,
    /// `location.vwap.utc_day@1`.
    pub vwap: FeatureValue<Vwap>,
    /// `location.auction.prior_day@1`.
    pub auction: FeatureValue<AuctionStatus>,
}

impl MarketReport {
    /// The report of `state`.
    pub fn read(state: &MarketState) -> Self {
        let levels = match &state.location.levels {
            FeatureValue::Ready(set) => FeatureValue::Ready(
                set.levels()
                    .iter()
                    .filter(|level| is_monitored(level))
                    .copied()
                    .collect(),
            ),
            FeatureValue::WarmingUp { observed, required } => FeatureValue::WarmingUp {
                observed: *observed,
                required: *required,
            },
            FeatureValue::Unavailable { reason } => FeatureValue::Unavailable { reason: *reason },
        };
        Self {
            as_of: state.as_of,
            last_price: state.last_trade_price,
            regime: state.regime,
            levels,
            vwap: state.location.vwap,
            auction: state.location.auction,
        }
    }

    /// The monitored levels in zone: the active monitors.
    pub fn active(&self) -> impl Iterator<Item = &LocationLevel> {
        self.levels
            .ready()
            .into_iter()
            .flatten()
            .filter(|level| level.in_zone)
    }
}

/// Writes a value that is not ready as `warming` or `unavailable`.
fn write_value<T>(
    f: &mut fmt::Formatter<'_>,
    value: &FeatureValue<T>,
    ready: impl FnOnce(&mut fmt::Formatter<'_>, &T) -> fmt::Result,
) -> fmt::Result {
    match value {
        FeatureValue::Ready(value) => ready(f, value),
        FeatureValue::WarmingUp { .. } => f.write_str("warming"),
        FeatureValue::Unavailable { .. } => f.write_str("unavailable"),
    }
}

impl fmt::Display for MarketReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_of {
            Some(as_of) => write!(f, "as_of={as_of}")?,
            None => f.write_str("as_of=-")?,
        }
        f.write_str(" price=")?;
        write_value(f, &self.last_price, |f, price| write!(f, "{price}"))?;
        f.write_str(" regime=")?;
        write_value(f, &self.regime, |f, regime| {
            write!(f, "{}:{}", regime.label, regime.atr_percentile.value())
        })?;
        f.write_str(" vwap=")?;
        write_value(f, &self.vwap, |f, vwap| write!(f, "{}", vwap.price))?;
        f.write_str(" auction=")?;
        write_value(f, &self.auction, |f, status| {
            write!(f, "{}", status.state)?;
            if let Some(side) = status.side {
                write!(f, ":{side}")?;
            }
            write!(f, "/a={}", status.accepted)
        })?;
        f.write_str(" levels=")?;
        write_value(f, &self.levels, |f, levels| {
            if levels.is_empty() {
                return f.write_str("-");
            }
            for (index, level) in levels.iter().enumerate() {
                if index > 0 {
                    f.write_str(",")?;
                }
                write!(
                    f,
                    "{}@{}[{},{}]",
                    level.kind, level.price, level.low, level.high
                )?;
                if level.in_zone {
                    f.write_str("*")?;
                }
            }
            Ok(())
        })
    }
}

/// The kind of an [`Alert`], in bundling order (D1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertKind {
    /// [`Alert::ZoneLeft`].
    ZoneLeft,
    /// [`Alert::ZoneEntered`].
    ZoneEntered,
    /// [`Alert::TriggerFired`].
    TriggerFired,
    /// [`Alert::RegimeChanged`].
    RegimeChanged,
    /// [`Alert::Report`].
    Report,
}

impl AlertKind {
    /// Every kind, in bundling order.
    pub const ALL: [Self; 5] = [
        Self::ZoneLeft,
        Self::ZoneEntered,
        Self::TriggerFired,
        Self::RegimeChanged,
        Self::Report,
    ];

    /// The canonical name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ZoneLeft => "zone_left",
            Self::ZoneEntered => "zone_entered",
            Self::TriggerFired => "trigger",
            Self::RegimeChanged => "regime",
            Self::Report => "report",
        }
    }
}

impl fmt::Display for AlertKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One alert decision (ADR-059 D2). Every alert of an event carries that
/// event's time, the decision time.
///
/// `Display` prints the canonical line the golden tests pin: the kind, the
/// time, then the level and guide, the trigger event, the two labels with
/// the new regime, or the report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Alert {
    /// A monitored level's zone was entered (D2b), with the guide at a VAH
    /// or VAL while the auction state is ready (D2c).
    ZoneEntered {
        /// The level after the bar.
        level: LocationLevel,
        /// Why: arrived or created (ADR-044 D7).
        cause: EnterCause,
        /// What would decide a probe across the edge.
        guide: Option<Guide>,
        /// Time of the event that closed the bar.
        known_at: EventTime,
    },
    /// The zone of a level whose enter was alerted was left: a whole closed
    /// bar stayed away from it (D2b).
    ZoneLeft {
        /// The level after the bar.
        level: LocationLevel,
        /// Time of the event that closed the bar.
        known_at: EventTime,
    },
    /// An auction trigger the guide names fired (D2d), as emitted. Boxed:
    /// several times the size of the other alerts.
    TriggerFired(Box<TriggerEvent>),
    /// The regime label changed between two consecutive ready values (D2e).
    RegimeChanged {
        /// The previous ready label.
        from: RegimeLabel,
        /// The new value.
        to: Regime,
        /// Time of the event that closed the 1h bar.
        known_at: EventTime,
    },
    /// The market report of an event that closed a 1d bar (D2f).
    Report(MarketReport),
}

impl Alert {
    /// The kind.
    pub fn kind(&self) -> AlertKind {
        match self {
            Self::ZoneEntered { .. } => AlertKind::ZoneEntered,
            Self::ZoneLeft { .. } => AlertKind::ZoneLeft,
            Self::TriggerFired(_) => AlertKind::TriggerFired,
            Self::RegimeChanged { .. } => AlertKind::RegimeChanged,
            Self::Report(_) => AlertKind::Report,
        }
    }

    /// When it became visible: the time of the event that decided it.
    pub fn known_at(&self) -> EventTime {
        match self {
            Self::ZoneEntered { known_at, .. }
            | Self::ZoneLeft { known_at, .. }
            | Self::RegimeChanged { known_at, .. } => *known_at,
            Self::TriggerFired(trigger) => trigger.known_at,
            Self::Report(report) => report.as_of.unwrap_or(EventTime::from_millis(0)),
        }
    }
}

impl fmt::Display for Alert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.kind(), self.known_at())?;
        match self {
            Self::ZoneEntered {
                level,
                cause,
                guide,
                ..
            } => {
                write!(f, " {cause:?} {level}")?;
                match guide {
                    Some(guide) => write!(f, " guide=[{guide}]"),
                    None => f.write_str(" guide=-"),
                }
            }
            Self::ZoneLeft { level, .. } => write!(f, " {level}"),
            Self::TriggerFired(trigger) => write!(f, " {} {trigger}", trigger.feature),
            Self::RegimeChanged { from, to, .. } => write!(f, " {from}->{to}"),
            Self::Report(report) => write!(f, " {report}"),
        }
    }
}

/// An open visit of a monitored level: its price, and whether its enter
/// was emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Visit {
    price: Price,
    alerted: bool,
}

/// Turns the engine's committed facts after each accepted event into
/// alerts (ADR-059 D1, D2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertMonitor {
    /// Alerts of events before this time are decided, not emitted.
    emit_from: Option<EventTime>,
    /// The open visit of each monitored kind, in [`MONITORED_KINDS`] order:
    /// one prior day holds one level of each.
    visits: [Option<Visit>; 3],
    /// The label of the regime value after the last observed event, `None`
    /// while it is not ready (D2e).
    regime: Option<RegimeLabel>,
}

impl AlertMonitor {
    /// A monitor that emits the alerts of events at or after `emit_from`
    /// (all with `None`), seeded from `seed`, the state the engine holds
    /// before the first event the monitor observes (D2g): the regime label
    /// if the regime is ready, and every monitored level in zone as an open visit
    /// whose enter was not emitted. Pass a fresh engine's state to start
    /// from nothing.
    pub fn new(emit_from: Option<EventTime>, seed: &MarketState) -> Self {
        let mut visits = [None; 3];
        if let FeatureValue::Ready(set) = &seed.location.levels {
            for level in set.levels() {
                if level.in_zone
                    && is_monitored(level)
                    && let Some(slot) = slot(level.kind)
                {
                    visits[slot] = Some(Visit {
                        price: level.price,
                        alerted: false,
                    });
                }
            }
        }
        Self {
            emit_from,
            visits,
            regime: seed.regime.ready().map(|regime| regime.label),
        }
    }

    /// Observes `event`, which `engine` has just accepted, and passes its
    /// alerts to `emit` in bundling order (module docs). Call it after
    /// every accepted event and never after a rejected one.
    pub fn observe(
        &mut self,
        event: &MarketEvent,
        engine: &MarketStateEngine,
        mut emit: impl FnMut(Alert),
    ) {
        let known_at = event.time();
        let emitting = self.emit_from.is_none_or(|from| known_at >= from);
        let state = engine.state();
        let facts = engine.location_events();
        // Every Left precedes every Entered within a bar (ADR-044 D7), and
        // an event closes at most one 1m bar with trades.
        for fact in facts {
            if let LocationEvent::Left { level, cause, .. } = fact
                && let Some(slot) = slot(level.kind).filter(|_| is_monitored(level))
                && let Some(visit) = self.visits[slot].filter(|visit| visit.price == level.price)
            {
                self.visits[slot] = None;
                // A retired level vanished; price did not move (D2b).
                if *cause == LeaveCause::Departed && visit.alerted {
                    emit(Alert::ZoneLeft {
                        level: *level,
                        known_at,
                    });
                }
            }
        }
        for fact in facts {
            if let LocationEvent::Entered { level, cause, .. } = fact
                && let Some(slot) = slot(level.kind).filter(|_| is_monitored(level))
            {
                self.visits[slot] = Some(Visit {
                    price: level.price,
                    alerted: emitting,
                });
                if emitting {
                    let guide = state
                        .location
                        .auction
                        .ready()
                        .and_then(|status| Guide::at(level, status));
                    emit(Alert::ZoneEntered {
                        level: *level,
                        cause: *cause,
                        guide,
                        known_at,
                    });
                }
            }
        }
        self.close_vanished(engine);
        if emitting {
            for trigger in engine.trigger_events() {
                if ALERT_TRIGGERS.contains(&trigger.feature) {
                    emit(Alert::TriggerFired(Box::new(trigger.clone())));
                }
            }
        }
        if let FeatureValue::Ready(regime) = &state.regime {
            if let Some(from) = self.regime
                && from != regime.label
                && emitting
            {
                emit(Alert::RegimeChanged {
                    from,
                    to: *regime,
                    known_at,
                });
            }
            self.regime = Some(regime.label);
        } else {
            // A not-ready stretch forgets the label (D2e): a monitor seeded
            // during it cannot see the label before it, and after a chain
            // restart that label belongs to the replaced engine.
            self.regime = None;
        }
        if emitting
            && engine
                .closed_bars()
                .iter()
                .any(|bar| bar.timeframe == Timeframe::D1)
        {
            emit(Alert::Report(MarketReport::read(state)));
        }
    }

    /// Closes, without an alert, every open visit whose level is no longer
    /// in the registry or no longer in zone (D2b): a fresh engine after a
    /// chain restart (ADR-052 D4) holds a warming registry. The registry
    /// changes only when a 1m bar with trades closes (ADR-044 D2), so a
    /// ready registry is read at those events only.
    fn close_vanished(&mut self, engine: &MarketStateEngine) {
        if self.visits.iter().all(Option::is_none) {
            return;
        }
        let FeatureValue::Ready(set) = &engine.state().location.levels else {
            self.visits = [None; 3];
            return;
        };
        let rebuilt = engine
            .closed_bars()
            .iter()
            .any(|bar| bar.timeframe == Timeframe::M1 && bar.ohlc.is_some());
        if !rebuilt {
            return;
        }
        for (kind, visit) in MONITORED_KINDS.iter().zip(&mut self.visits) {
            let Some(open) = *visit else {
                continue;
            };
            let held = set.levels().iter().any(|level| {
                level.kind == *kind
                    && level.price == open.price
                    && level.in_zone
                    && is_monitored(level)
            });
            if !held {
                *visit = None;
            }
        }
    }
}

#[cfg(test)]
mod tests;
