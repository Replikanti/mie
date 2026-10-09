//! The level registry `location.levels@1`: every level location monitors,
//! with its score components and the monitored-zone events (ADR-044,
//! decisions 5–7).
//!
//! The registry is rebuilt from its sources at every closed 1m bar with
//! trades. A level keeps its registration time, touches and in-zone flag
//! across rebuilds through its identity; nothing is hash-ordered.

use super::{LevelKind, TOLERANCE_BPS, Vwap, within};
use crate::bars::{Bar, Ohlc};
use crate::feature::{FeatureKey, FeatureValue, catalog};
use crate::fingerprint::Fingerprinter;
use crate::liquidity::{BookClusters, BookState};
use crate::num::{Price, Qty};
use crate::profile::VolumeProfiles;
use crate::state_hash::StateEncode;
use crate::structure::{Side, StructureSet};
use crate::time::EventTime;
use std::fmt;

/// Which side of its source a level sits on: part of its identity
/// (decision 5).
///
/// The declaration order is frozen: it gives the state-hash codes
/// (ADR-041).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LevelSide {
    /// A structural swing high, or a level swept from below.
    High,
    /// A structural swing low, or a level swept from above.
    Low,
    /// A resting bid.
    Bid,
    /// A resting ask.
    Ask,
}

impl LevelSide {
    /// The state-hash code, in declaration order.
    fn code(self) -> u8 {
        match self {
            Self::High => 0,
            Self::Low => 1,
            Self::Bid => 2,
            Self::Ask => 3,
        }
    }
}

impl StateEncode for LevelSide {
    /// `write_u8` in declaration order: High 0, Low 1, Bid 2, Ask 3
    /// (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(self.code());
    }
}

impl fmt::Display for LevelSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::High => "high",
            Self::Low => "low",
            Self::Bid => "bid",
            Self::Ask => "ask",
        })
    }
}

/// The resting quantity behind a liquidity-cluster level (decision 6): the
/// level's quantity and its side's median level quantity, exact. The
/// multiple is derived on demand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterStrength {
    /// The level's resting quantity.
    pub qty: Qty,
    /// The lower-median level quantity of its side within 5 bps (ADR-043,
    /// decision 8).
    pub median_qty: Qty,
}

impl StateEncode for ClusterStrength {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { qty, median_qty } = self;
        qty.encode(f);
        median_qty.encode(f);
    }
}

impl ClusterStrength {
    /// The quantity as a multiple of the median; `None` with a zero median.
    pub fn multiple(&self) -> Option<f64> {
        (self.median_qty.units() != 0)
            .then(|| self.qty.units() as f64 / self.median_qty.units() as f64)
    }
}

/// One level of the registry with its score components (decisions 5 and
/// 6). There is no scalar score: the weights are #31's measured output
/// (ADR-013).
///
/// `Display` prints `kind@price`, the zone when it is wider than the price,
/// then `t=touches c=confluence d=distance`, and `in` while price is in its
/// zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocationLevel {
    /// Which level.
    pub kind: LevelKind,
    /// The feature that produced it.
    pub source: FeatureKey,
    /// The side, for structure and cluster levels.
    pub side: Option<LevelSide>,
    /// Open time of the swing bar for a structure level; the open of the
    /// UTC day for the VWAP. `None` otherwise.
    pub anchor: Option<EventTime>,
    /// The level's price.
    pub price: Price,
    /// Lower edge of the zone (inclusive).
    pub low: Price,
    /// Upper edge of the zone (inclusive).
    pub high: Price,
    /// When the registry first held the level: the time of the event that
    /// closed that bar (ADR-037 decision 8 semantics). Never a bar end.
    pub known_at: EventTime,
    /// `Entered(Arrived)` events since registration.
    pub touches: u32,
    /// Whether the last closed bar's range overlaps the zone padded by the
    /// tolerance. Always `false` for a liquidity cluster, which is not
    /// monitored (decision 7).
    pub in_zone: bool,
    /// Signed distance from the last close to the zone: positive when the
    /// zone lies above, negative below, 0 inside.
    pub distance: Price,
    /// Levels from other sources whose zones lie within the tolerance of
    /// this one's.
    pub confluence: u32,
    /// The resting quantity, for a liquidity cluster; `None` otherwise.
    pub strength: Option<ClusterStrength>,
}

impl StateEncode for LocationLevel {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            kind,
            source,
            side,
            anchor,
            price,
            low,
            high,
            known_at,
            touches,
            in_zone,
            distance,
            confluence,
            strength,
        } = self;
        kind.encode(f);
        source.encode(f);
        side.encode(f);
        anchor.encode(f);
        price.encode(f);
        low.encode(f);
        high.encode(f);
        known_at.encode(f);
        touches.encode(f);
        in_zone.encode(f);
        distance.encode(f);
        confluence.encode(f);
        strength.encode(f);
    }
}

impl fmt::Display for LocationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        if let Some(side) = self.side {
            write!(f, ":{side}")?;
        }
        write!(f, "@{}", self.price)?;
        if (self.low, self.high) != (self.price, self.price) {
            write!(f, "[{},{}]", self.low, self.high)?;
        }
        write!(
            f,
            " t={} c={} d={}",
            self.touches, self.confluence, self.distance
        )?;
        if self.in_zone {
            f.write_str(" in")?;
        }
        Ok(())
    }
}

impl LocationLevel {
    /// Milliseconds from `known_at` to `now`, derived on demand; negative if
    /// `now` comes first, saturating at the `i64` range.
    pub fn age_ms(&self, now: EventTime) -> i64 {
        now.as_millis().saturating_sub(self.known_at.as_millis())
    }

    /// The distance in basis points of `close`, derived on demand; `None`
    /// for a zero close.
    pub fn distance_bps(&self, close: Price) -> Option<f64> {
        (close.units() != 0).then(|| self.distance.units() as f64 * 10_000.0 / close.units() as f64)
    }

    /// Whether zone events are emitted for the level: every kind but
    /// [`LevelKind::LiquidityCluster`] (decision 7).
    pub fn is_monitored(&self) -> bool {
        self.kind != LevelKind::LiquidityCluster
    }

    /// The identity that carries `known_at`, touches and the in-zone flag
    /// across rebuilds (decision 5): kind, source, side, anchor, and the
    /// price for every kind but the VWAP, whose price moves. Compared with
    /// the source last: structure ids share a long prefix, and the other
    /// fields almost always differ first.
    fn identity(&self) -> Identity {
        let price = (self.kind != LevelKind::Vwap).then_some(self.price.units());
        (
            self.kind.code(),
            price,
            self.anchor.map(EventTime::as_millis),
            self.side.map(LevelSide::code),
            self.source,
        )
    }

    /// The registry order (decision 5): zone low, zone high, kind code,
    /// source, identity.
    fn order(&self) -> Order {
        (
            self.low,
            self.high,
            self.kind.code(),
            self.source,
            self.identity(),
        )
    }

    /// A newly sourced level, not yet registered.
    fn sourced(
        kind: LevelKind,
        source: FeatureKey,
        side: Option<LevelSide>,
        anchor: Option<EventTime>,
        (price, low, high): (Price, Price, Price),
    ) -> Self {
        Self {
            kind,
            source,
            side,
            anchor,
            price,
            low,
            high,
            known_at: EventTime::from_millis(0),
            touches: 0,
            in_zone: false,
            distance: Price::from_units(0),
            confluence: 0,
            strength: None,
        }
    }
}

/// Kind code, price units, anchor ms, side code and source.
type Identity = (u8, Option<i64>, Option<i64>, Option<u8>, FeatureKey);

/// Zone low, zone high, kind code, source, identity.
type Order = (Price, Price, u8, FeatureKey, Identity);

/// Compares two levels in registry order ([`LocationLevel::order`]),
/// building the full key only on a tie of the zone and kind.
fn cmp_order(a: &LocationLevel, b: &LocationLevel) -> std::cmp::Ordering {
    (a.low, a.high, a.kind.code())
        .cmp(&(b.low, b.high, b.kind.code()))
        .then_with(|| a.order().cmp(&b.order()))
}

/// Whether two keys are equal, trying the id's address first.
fn same(a: FeatureKey, b: FeatureKey) -> bool {
    a.version == b.version && (std::ptr::eq(a.id.as_str(), b.id.as_str()) || a.id == b.id)
}

/// A small index per distinct source of `levels`, for the same-source part
/// of the confluence. Levels arrive in runs of one source, so most take the
/// previous index without a comparison.
fn source_groups(levels: &[LocationLevel]) -> (Vec<usize>, usize) {
    let mut keys: Vec<FeatureKey> = Vec::new();
    let mut last: Option<(FeatureKey, usize)> = None;
    let groups = levels
        .iter()
        .map(|level| match last {
            Some((key, group)) if same(key, level.source) => group,
            _ => {
                let group = keys
                    .iter()
                    .position(|key| same(*key, level.source))
                    .unwrap_or_else(|| {
                        keys.push(level.source);
                        keys.len() - 1
                    });
                last = Some((level.source, group));
                group
            }
        })
        .collect();
    (groups, keys.len())
}

/// The level registry after a closed 1m bar: one `location.levels@1` value
/// (decisions 5–7).
///
/// `Display` prints the canonical line the golden tests pin: the close, the
/// level count, the levels in zone and the three nearest, then the feature
/// key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelSet {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// Close of the last closed 1m bar with trades.
    pub close: Price,
    /// End of that bar. A bar time, not a visibility time.
    pub bar_end: EventTime,
    /// Time of the event that closed it: the value's visibility time.
    pub known_at: EventTime,
    /// Every level, in registry order.
    levels: Vec<LocationLevel>,
}

impl StateEncode for LevelSet {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            close,
            bar_end,
            known_at,
            levels,
        } = self;
        feature.encode(f);
        close.encode(f);
        bar_end.encode(f);
        known_at.encode(f);
        levels.encode(f);
    }
}

impl LevelSet {
    /// Every level in registry order (decision 5): zone low, zone high, kind,
    /// source, identity.
    pub fn levels(&self) -> &[LocationLevel] {
        &self.levels
    }

    /// The `n` levels nearest the close by absolute distance, sorted on
    /// demand; ties keep the registry order.
    pub fn nearest(&self, n: usize) -> Vec<&LocationLevel> {
        let mut nearest: Vec<&LocationLevel> = self.levels.iter().collect();
        nearest.sort_by_key(|level| level.distance.units().unsigned_abs());
        nearest.truncate(n);
        nearest
    }
}

impl fmt::Display for LevelSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "close={} levels={} in=", self.close, self.levels.len())?;
        let mut any = false;
        for level in self.levels.iter().filter(|level| level.in_zone) {
            if any {
                f.write_str(",")?;
            }
            any = true;
            write!(f, "{}@{}", level.kind, level.price)?;
        }
        if !any {
            f.write_str("-")?;
        }
        f.write_str(" nearest=")?;
        for (index, level) in self.nearest(3).into_iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{}@{}:{}", level.kind, level.price, level.distance)?;
        }
        write!(f, " {}", self.feature)
    }
}

/// Why a level entered its zone (decision 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnterCause {
    /// A closed bar reached the zone of a registered level.
    Arrived,
    /// The level appeared while price was in its zone.
    Created,
}

/// Why a level left its zone (decision 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaveCause {
    /// A whole closed bar stayed away from the zone.
    Departed,
    /// The level vanished from its source while price was in its zone.
    Retired,
}

/// The zone events of one rebuild, in emission order: every `Left`, then
/// every `Entered`, each in registry order (decision 7).
pub(super) struct ZoneEvents {
    pub(super) left: Vec<(LocationLevel, LeaveCause)>,
    pub(super) entered: Vec<(LocationLevel, EnterCause)>,
}

/// The levels of every source after the last commit (decision 5), not yet
/// registered: the prior-day and 5-day composite profiles, the structure
/// registries, the book's cluster candidates and the VWAP.
pub(super) fn sourced(
    profile: &VolumeProfiles,
    structure: &StructureSet,
    book: &BookState,
    vwap: &FeatureValue<Vwap>,
) -> Vec<LocationLevel> {
    let mut levels = Vec::new();
    for value in [&profile.prior_day, &profile.composite_5d] {
        if let FeatureValue::Ready(profile) = value {
            levels.extend(profile.levels().map(|level| {
                LocationLevel::sourced(
                    level.kind,
                    level.source,
                    None,
                    None,
                    (level.price, level.low, level.high),
                )
            }));
        }
    }
    for (_, timeframe) in structure.iter() {
        if let FeatureValue::Ready(registry) = &timeframe.levels {
            levels.extend(registry.levels().map(|level| {
                let side = match level.side {
                    Side::High => LevelSide::High,
                    Side::Low => LevelSide::Low,
                };
                LocationLevel::sourced(
                    level.kind,
                    level.source,
                    Some(side),
                    Some(level.swing_time),
                    (level.price, level.low, level.high),
                )
            }));
        }
    }
    if let FeatureValue::Ready(clusters) = &book.clusters {
        clusters_of(clusters, &mut levels);
    }
    if let FeatureValue::Ready(vwap) = vwap {
        levels.push(LocationLevel::sourced(
            LevelKind::Vwap,
            vwap.feature,
            None,
            Some(vwap.start),
            (vwap.price, vwap.price, vwap.price),
        ));
    }
    levels
}

/// Every top-5 cluster candidate of both sides (decision 5): no threshold.
fn clusters_of(clusters: &BookClusters, levels: &mut Vec<LocationLevel>) {
    for (side, value) in [
        (LevelSide::Bid, &clusters.bid),
        (LevelSide::Ask, &clusters.ask),
    ] {
        let FeatureValue::Ready(candidates) = value else {
            continue;
        };
        for level in candidates.top.iter().flatten() {
            levels.push(LocationLevel {
                strength: Some(ClusterStrength {
                    qty: level.qty,
                    median_qty: candidates.median_qty,
                }),
                ..LocationLevel::sourced(
                    LevelKind::LiquidityCluster,
                    clusters.feature,
                    Some(side),
                    None,
                    (level.price, level.price, level.price),
                )
            });
        }
    }
}

/// Whether the bar's range overlaps `level`'s zone padded by the tolerance
/// of the level's price, exact in `i128` (decision 7).
fn overlaps(ohlc: &Ohlc, level: &LocationLevel, tolerance_bps: i64) -> bool {
    let below = i128::from(level.low.units()) - i128::from(ohlc.high.units());
    let above = i128::from(ohlc.low.units()) - i128::from(level.high.units());
    within(below, level.price, tolerance_bps) && within(above, level.price, tolerance_bps)
}

/// The signed distance from `close` to the zone of `level`, saturating at
/// the `Price` range.
fn distance(close: Price, level: &LocationLevel) -> Price {
    let close = i128::from(close.units());
    let units = if close < i128::from(level.low.units()) {
        i128::from(level.low.units()) - close
    } else if close > i128::from(level.high.units()) {
        i128::from(level.high.units()) - close
    } else {
        0
    };
    let clamped = units.clamp(i128::from(i64::MIN), i128::from(i64::MAX));
    Price::from_units(i64::try_from(clamped).unwrap_or(0))
}

/// Fills the confluence of every level of `levels`: the levels from other
/// sources whose zone lies within the tolerance of the level's price from
/// its zone (decision 6).
///
/// With `W = ⌊|price| · tolerance_bps / 10 000⌋` — exact for the integer
/// gaps of decision 3 — another zone `[l, h]` is within reach iff
/// `l ≤ high + W` and `h ≥ low − W`. As `h ≥ l`, the count of such zones is
/// the zones with `l ≤ high + W` minus those with `h < low − W`: two binary
/// searches over the sorted lows and highs, overall and of the level's own
/// source, so a rebuild stays `O(n log n)` however wide a zone is.
fn fill_confluence(levels: &mut [LocationLevel], tolerance_bps: i64) {
    let (groups, count) = source_groups(levels);
    // The levels are in registry order, so every list of lows is sorted
    // already; the highs are sorted here. Sized up front: a rebuild runs at
    // every closed minute.
    let mut sizes = vec![0; count + 1];
    for group in &groups {
        sizes[*group] += 1;
    }
    sizes[count] = levels.len();
    let mut lows: Vec<Vec<i64>> = sizes.iter().map(|size| Vec::with_capacity(*size)).collect();
    let mut highs: Vec<Vec<i64>> = sizes.iter().map(|size| Vec::with_capacity(*size)).collect();
    for (level, group) in levels.iter().zip(&groups) {
        for at in [*group, count] {
            lows[at].push(level.low.units());
            highs[at].push(level.high.units());
        }
    }
    for list in &mut highs {
        list.sort_unstable();
    }
    // Bounds beyond the `i64` range reach every edge or none.
    let clamp = |bound: i128| {
        i64::try_from(bound.clamp(i128::from(i64::MIN), i128::from(i64::MAX))).unwrap_or(0)
    };
    for (level, group) in levels.iter_mut().zip(&groups) {
        let pad =
            (i128::from(level.price.units()).abs() * i128::from(tolerance_bps)).div_euclid(10_000);
        let from = i128::from(level.low.units()) - pad;
        let to = i128::from(level.high.units()) + pad;
        // An edge below `i64::MIN` cannot exist, so a clamped `from` counts
        // the same highs; likewise `to` above `i64::MAX`.
        let (from, to) = (clamp(from), clamp(to));
        let reach = |at: usize| {
            lows[at].partition_point(|low| *low <= to)
                - highs[at].partition_point(|high| *high < from)
        };
        let others = reach(count) - reach(*group);
        level.confluence = u32::try_from(others).unwrap_or(u32::MAX);
    }
}

/// Rebuilds the registry from `sourced` at the closed 1m bar `bar` (with
/// trades), carried by the event at `known_at`, against the previous
/// registry `previous` (decisions 5–7). Integer arithmetic and moves only:
/// it cannot fail.
pub(super) fn rebuild(
    previous: Option<&LevelSet>,
    sourced: Vec<LocationLevel>,
    bar: &Bar,
    ohlc: &Ohlc,
    known_at: EventTime,
) -> (LevelSet, ZoneEvents) {
    // Sorted indices, vectors and binary searches instead of maps: this
    // runs at every closed minute over a few hundred levels.
    let mut sorted: Vec<usize> = (0..sourced.len()).collect();
    sorted.sort_unstable_by(|a, b| cmp_order(&sourced[*a], &sourced[*b]));
    // Equal keys are equal levels. Every source lists an identity once with
    // one zone, so no other duplicate can occur.
    sorted.dedup_by(|later, first| cmp_order(&sourced[*later], &sourced[*first]).is_eq());
    let mut levels: Vec<LocationLevel> = sorted.into_iter().map(|index| sourced[index]).collect();
    // The previous levels by identity, each marked once matched.
    let held = previous.map_or(&[][..], |set| set.levels.as_slice());
    let mut carried: Vec<(Identity, &LocationLevel, bool)> = held
        .iter()
        .map(|level| (level.identity(), level, false))
        .collect();
    carried.sort_unstable_by_key(|(identity, _, _)| *identity);
    let mut departed = Vec::new();
    let mut entered = Vec::new();
    for (index, level) in levels.iter_mut().enumerate() {
        let in_zone = level.is_monitored() && overlaps(ohlc, level, TOLERANCE_BPS);
        level.distance = distance(ohlc.close, level);
        level.in_zone = in_zone;
        let identity = level.identity();
        match carried.binary_search_by(|(other, _, _)| other.cmp(&identity)) {
            Ok(at) => {
                let old = carried[at].1;
                carried[at].2 = true;
                level.known_at = old.known_at;
                level.touches = old.touches;
                if in_zone && !old.in_zone {
                    level.touches = level.touches.saturating_add(1);
                    entered.push((index, EnterCause::Arrived));
                } else if old.in_zone && !in_zone {
                    departed.push(index);
                }
            }
            Err(_) => {
                level.known_at = known_at;
                if in_zone {
                    entered.push((index, EnterCause::Created));
                }
            }
        }
    }
    fill_confluence(&mut levels, TOLERANCE_BPS);
    // Score components are final now: the events carry them.
    let mut left: Vec<(LocationLevel, LeaveCause)> = departed
        .into_iter()
        .map(|index| (levels[index], LeaveCause::Departed))
        .collect();
    let retired = carried
        .iter()
        .filter(|(_, old, matched)| old.in_zone && !matched);
    left.extend(retired.map(|(_, old, _)| (**old, LeaveCause::Retired)));
    left.sort_by(|a, b| cmp_order(&a.0, &b.0));
    let events = ZoneEvents {
        left,
        entered: entered
            .into_iter()
            .map(|(index, cause)| (levels[index], cause))
            .collect(),
    };
    let set = LevelSet {
        feature: catalog::LOCATION_LEVELS_V1.key,
        close: ohlc.close,
        bar_end: bar.end(),
        known_at,
        levels,
    };
    (set, events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::tests::random_tape;
    use crate::bars::{Coverage, Timeframe};
    use crate::event::MarketEvent;
    use crate::event::samples::{t, trade as sample_trade};
    use crate::location::LocationEvent;
    use crate::location::tests::walk_tape;
    use crate::state::MarketStateEngine;
    use std::collections::BTreeSet;

    fn price(text: &str) -> Price {
        text.parse().unwrap()
    }

    fn source(id: &'static str) -> FeatureKey {
        FeatureKey::new(id, 1)
    }

    /// A point level of `kind` from `id` at `at`.
    fn point(kind: LevelKind, id: &'static str, at: &str) -> LocationLevel {
        let at = price(at);
        LocationLevel::sourced(kind, source(id), None, None, (at, at, at))
    }

    /// A closed 1m bar opening at `minute` with range `low`–`high` and close
    /// `close`.
    fn bar(minute: i64, low: &str, high: &str, close: &str) -> (Bar, Ohlc) {
        let ohlc = Ohlc {
            open: price(close),
            high: price(high),
            low: price(low),
            close: price(close),
        };
        let bar = Bar {
            ohlc: Some(ohlc),
            coverage: Coverage::default(),
            ..Bar::empty(Timeframe::M1, t(minute * 60_000))
        };
        (bar, ohlc)
    }

    /// Rebuilds at minute `minute`, known one second after its end.
    fn step(
        previous: Option<&LevelSet>,
        levels: &[LocationLevel],
        minute: i64,
        range: (&str, &str, &str),
    ) -> (LevelSet, ZoneEvents) {
        let (bar, ohlc) = bar(minute, range.0, range.1, range.2);
        rebuild(
            previous,
            levels.to_vec(),
            &bar,
            &ohlc,
            t((minute + 1) * 60_000 + 1_000),
        )
    }

    /// The prices and causes of the `Left` and the `Entered` events.
    type Causes = (Vec<(Price, LeaveCause)>, Vec<(Price, EnterCause)>);

    fn causes(events: &ZoneEvents) -> Causes {
        (
            events
                .left
                .iter()
                .map(|(level, cause)| (level.price, *cause))
                .collect(),
            events
                .entered
                .iter()
                .map(|(level, cause)| (level.price, *cause))
                .collect(),
        )
    }

    #[test]
    fn the_zone_is_the_bar_range_against_the_padded_zone() {
        // 6 bps of 60 000 is 36 USDT: a bar whose high is 36 below the level
        // reaches it, 36.00000001 below does not.
        let level = point(LevelKind::Poc, "profile.volume.prior_day", "60000");
        let reaches = |high: &str| {
            let (_, ohlc) = bar(0, "59900", high, "59950");
            overlaps(&ohlc, &level, TOLERANCE_BPS)
        };
        assert!(reaches("59964"));
        assert!(!reaches("59963.99999999"));
        assert!(reaches("60100"));
        let from_above = |low: &str| {
            let (_, ohlc) = bar(0, low, "60100", "60050");
            overlaps(&ohlc, &level, TOLERANCE_BPS)
        };
        assert!(from_above("60036"));
        assert!(!from_above("60036.00000001"));
    }

    #[test]
    fn the_distance_is_signed_and_zero_inside() {
        let zone = LocationLevel::sourced(
            LevelKind::SfpRejectionZone,
            source("structure.levels.15m"),
            Some(LevelSide::High),
            Some(t(0)),
            (price("60000"), price("60000"), price("60050")),
        );
        assert_eq!(distance(price("59990"), &zone), price("10"));
        assert_eq!(distance(price("60000"), &zone), price("0"));
        assert_eq!(distance(price("60050"), &zone), price("0"));
        assert_eq!(distance(price("60060"), &zone), price("-10"));
        let extreme = LocationLevel::sourced(
            LevelKind::Poc,
            source("profile.volume.prior_day"),
            None,
            None,
            (
                Price::from_units(i64::MAX),
                Price::from_units(i64::MAX),
                Price::from_units(i64::MAX),
            ),
        );
        assert_eq!(
            distance(Price::from_units(i64::MIN), &extreme),
            Price::from_units(i64::MAX)
        );
    }

    #[test]
    fn touches_and_registration_survive_a_rebuild() {
        let levels = [
            point(LevelKind::Poc, "profile.volume.prior_day", "60000"),
            point(LevelKind::Val, "profile.volume.prior_day", "59000"),
        ];
        // Minute 0 is away from both: registered, not in zone.
        let (first, events) = step(None, &levels, 0, ("59500", "59600", "59550"));
        assert!(events.entered.is_empty() && events.left.is_empty());
        assert!(
            first
                .levels()
                .iter()
                .all(|level| level.known_at == t(61_000))
        );
        // Minute 1 reaches the POC: Arrived, one touch.
        let (second, events) = step(Some(&first), &levels, 1, ("59900", "60010", "60000"));
        assert_eq!(
            causes(&events),
            (vec![], vec![(price("60000"), EnterCause::Arrived)])
        );
        let poc = second.levels()[1];
        assert_eq!(
            (poc.kind, poc.touches, poc.in_zone),
            (LevelKind::Poc, 1, true)
        );
        assert_eq!(poc.known_at, t(61_000));
        // Staying in zone neither touches nor emits.
        let (third, events) = step(Some(&second), &levels, 2, ("59990", "60005", "60000"));
        assert!(events.entered.is_empty() && events.left.is_empty());
        assert_eq!(third.levels()[1].touches, 1);
        // A whole minute away departs; coming back is the second touch.
        let (fourth, events) = step(Some(&third), &levels, 3, ("59800", "59900", "59850"));
        assert_eq!(
            causes(&events),
            (vec![(price("60000"), LeaveCause::Departed)], vec![])
        );
        let (fifth, _) = step(Some(&fourth), &levels, 4, ("59950", "60000", "59990"));
        let poc = fifth.levels()[1];
        assert_eq!((poc.touches, poc.known_at), (2, t(61_000)));
        assert_eq!(fifth.known_at, t(5 * 60_000 + 1_000));
        assert_eq!(fifth.bar_end, t(5 * 60_000));
    }

    #[test]
    fn created_and_retired_mark_levels_that_appear_or_vanish_in_zone() {
        let poc = point(LevelKind::Poc, "profile.volume.prior_day", "60000");
        let hvn = point(LevelKind::Hvn, "profile.volume.prior_day", "60010");
        let (first, events) = step(None, &[poc], 0, ("59990", "60020", "60000"));
        assert_eq!(
            causes(&events),
            (vec![], vec![(price("60000"), EnterCause::Created)])
        );
        assert_eq!(first.levels()[0].touches, 0);
        // The POC vanishes and an HVN appears, both in zone.
        let (second, events) = step(Some(&first), &[hvn], 1, ("59990", "60020", "60000"));
        assert_eq!(
            causes(&events),
            (
                vec![(price("60000"), LeaveCause::Retired)],
                vec![(price("60010"), EnterCause::Created)]
            )
        );
        // The HVN vanishes after a bar away from it: its flag still held the
        // previous bar, so it retires; a level never in zone leaves silently.
        let (_, events) = step(Some(&second), &[], 2, ("59000", "59100", "59050"));
        assert_eq!(
            causes(&events),
            (vec![(price("60010"), LeaveCause::Retired)], vec![])
        );
        let (away, _) = step(None, &[hvn], 0, ("59000", "59100", "59050"));
        let (_, events) = step(Some(&away), &[], 1, ("59000", "59100", "59050"));
        assert!(events.left.is_empty());
    }

    #[test]
    fn the_vwap_keeps_its_identity_while_it_moves_within_the_day() {
        let vwap = |at: &str, day: i64| {
            let at = price(at);
            LocationLevel::sourced(
                LevelKind::Vwap,
                catalog::LOCATION_VWAP_UTC_DAY_V1.key,
                None,
                Some(t(day * 86_400_000)),
                (at, at, at),
            )
        };
        let (first, _) = step(None, &[vwap("60000", 0)], 0, ("59990", "60010", "60000"));
        let (second, events) = step(
            Some(&first),
            &[vwap("60004", 0)],
            1,
            ("59990", "60010", "60000"),
        );
        assert!(events.entered.is_empty() && events.left.is_empty());
        assert_eq!(second.levels()[0].known_at, first.levels()[0].known_at);
        // A new day is a new VWAP.
        let (_, events) = step(
            Some(&second),
            &[vwap("60004", 1)],
            2,
            ("59990", "60010", "60000"),
        );
        assert_eq!(
            causes(&events),
            (
                vec![(price("60004"), LeaveCause::Retired)],
                vec![(price("60004"), EnterCause::Created)]
            )
        );
    }

    #[test]
    fn clusters_are_scored_but_never_monitored() {
        let cluster = LocationLevel {
            strength: Some(ClusterStrength {
                qty: Qty::from_units(800),
                median_qty: Qty::from_units(10),
            }),
            ..LocationLevel::sourced(
                LevelKind::LiquidityCluster,
                catalog::BOOK_CLUSTERS_V1.key,
                Some(LevelSide::Bid),
                None,
                (price("60000"), price("60000"), price("60000")),
            )
        };
        let (set, events) = step(None, &[cluster], 0, ("59990", "60010", "60000"));
        assert!(events.entered.is_empty() && events.left.is_empty());
        let level = set.levels()[0];
        assert!(!level.in_zone && !level.is_monitored());
        assert_eq!(level.strength.unwrap().multiple(), Some(80.0));
    }

    #[test]
    fn confluence_counts_other_sources_only() {
        let levels = [
            // A POC and an HVN of one profile at one price: same source.
            point(LevelKind::Poc, "profile.volume.prior_day", "60000"),
            point(LevelKind::Hvn, "profile.volume.prior_day", "60000"),
            // 36 USDT above (6 bps of 60 000): within.
            point(LevelKind::Hvn, "profile.volume.composite_5d", "60036"),
            // 36.00000001 below the POC: beyond.
            point(
                LevelKind::Lvn,
                "profile.volume.composite_5d",
                "59963.99999999",
            ),
            // A wide zone below whose top reaches the POC.
            LocationLevel::sourced(
                LevelKind::SfpRejectionZone,
                source("structure.levels.1h"),
                Some(LevelSide::Low),
                Some(t(0)),
                (price("59964"), price("59000"), price("59990")),
            ),
        ];
        let (set, _) = step(None, &levels, 0, ("59000", "59100", "59050"));
        let confluence: Vec<(LevelKind, Price, u32)> = set
            .levels()
            .iter()
            .map(|level| (level.kind, level.price, level.confluence))
            .collect();
        assert_eq!(
            confluence,
            [
                (LevelKind::SfpRejectionZone, price("59964"), 3),
                (LevelKind::Lvn, price("59963.99999999"), 1),
                (LevelKind::Poc, price("60000"), 2),
                (LevelKind::Hvn, price("60000"), 2),
                (LevelKind::Hvn, price("60036"), 2),
            ]
        );
    }

    /// A level's identity with its source key (decision 5).
    type Key = (
        u8,
        FeatureKey,
        Option<LevelSide>,
        Option<EventTime>,
        Option<Price>,
    );

    fn key(level: &LocationLevel) -> Key {
        let price = (level.kind != LevelKind::Vwap).then_some(level.price);
        (
            level.kind.code(),
            level.source,
            level.side,
            level.anchor,
            price,
        )
    }

    /// Applies `tape` and checks every location fact against the state:
    /// each `Entered` is closed by exactly one `Left`, clusters emit
    /// nothing, the open zones are the in-zone levels, and every fact is
    /// visible at the time of the event that emitted it. Returns the facts.
    fn checked_facts(tape: &[MarketEvent]) -> Vec<(EventTime, LocationEvent)> {
        let mut engine = MarketStateEngine::new();
        let mut open = BTreeSet::new();
        let mut facts = Vec::new();
        for event in tape {
            engine.apply(event).unwrap();
            for fact in engine.location_events() {
                assert_eq!(fact.time(), event.time(), "{fact}");
                let (level, entered, bar_end) = match fact {
                    LocationEvent::Entered { level, bar_end, .. } => (level, true, bar_end),
                    LocationEvent::Left { level, bar_end, .. } => (level, false, bar_end),
                    LocationEvent::Auction { bar_end, .. } => {
                        assert!(*bar_end <= event.time());
                        continue;
                    }
                };
                assert!(*bar_end <= event.time());
                assert!(level.is_monitored(), "{fact}");
                if entered {
                    assert!(open.insert(key(level)), "entered twice: {fact}");
                } else {
                    assert!(open.remove(&key(level)), "left unentered: {fact}");
                }
                facts.push((event.time(), *fact));
            }
            if let FeatureValue::Ready(set) = &engine.state().location.levels {
                let flagged: BTreeSet<Key> = set
                    .levels()
                    .iter()
                    .filter(|level| level.in_zone)
                    .map(key)
                    .collect();
                assert_eq!(flagged, open);
            }
        }
        facts
    }

    #[test]
    fn every_entered_is_closed_by_exactly_one_left() {
        let facts = checked_facts(&walk_tape(0x6d69_6500_0000_0023, 4));
        let count = |entered: Option<EnterCause>, left: Option<LeaveCause>| {
            facts
                .iter()
                .filter(|(_, fact)| match fact {
                    LocationEvent::Entered { cause, .. } => Some(*cause) == entered,
                    LocationEvent::Left { cause, .. } => Some(*cause) == left,
                    LocationEvent::Auction { .. } => false,
                })
                .count()
        };
        // Every cause occurs.
        assert!(count(Some(EnterCause::Arrived), None) > 100);
        assert!(count(Some(EnterCause::Created), None) > 10);
        assert!(count(None, Some(LeaveCause::Departed)) > 100);
        assert!(count(None, Some(LeaveCause::Retired)) > 10);
        // Random tapes with jumps of hours and gaps.
        for seed in [41, 42] {
            checked_facts(&random_tape(seed, 6_000));
        }
    }

    #[test]
    fn clusters_never_emit_zone_events() {
        let level = |price: i64, qty: i64| crate::event::Level {
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
        };
        // A book around 63 542 whose trusted window reaches beyond 5 bps on
        // both sides, synced by its first update; trades at the best ask.
        let snapshot = MarketEvent::BookSnapshot(crate::event::BookSnapshot {
            time: t(500),
            last_update_id: 100,
            bids: vec![
                level(6_354_200_000_000, 300_000_000),
                level(6_354_190_000_000, 100_000_000),
                level(6_350_000_000_000, 100_000_000),
            ],
            asks: vec![
                level(6_354_210_000_000, 120_000_000),
                level(6_354_220_000_000, 80_000_000),
                level(6_359_000_000_000, 100_000_000),
            ],
        });
        let update = MarketEvent::BookUpdate(crate::event::BookUpdate {
            time: t(600),
            first_update_id: 95,
            last_update_id: 105,
            prev_update_id: 0,
            bids: vec![level(6_354_200_000_000, 250_000_000)],
            asks: Vec::new(),
        });
        let mut engine = MarketStateEngine::new();
        engine.apply(&snapshot).unwrap();
        engine.apply(&update).unwrap();
        engine.apply(&sample_trade(1_000, 1)).unwrap();
        engine.apply(&sample_trade(61_000, 2)).unwrap();
        let set = engine.state().location.levels.ready().unwrap();
        let clusters: Vec<&LocationLevel> = set
            .levels()
            .iter()
            .filter(|level| level.kind == LevelKind::LiquidityCluster)
            .collect();
        assert_eq!(clusters.len(), 4);
        assert!(
            clusters
                .iter()
                .all(|level| !level.in_zone && level.strength.is_some())
        );
        // The VWAP is entered on creation; no cluster emits anything.
        let facts = engine.location_events();
        assert!(matches!(
            facts,
            [LocationEvent::Entered {
                cause: EnterCause::Created,
                level: LocationLevel {
                    kind: LevelKind::Vwap,
                    ..
                },
                ..
            }]
        ));
        assert_eq!(facts[0].time(), t(61_000));
        // The clusters confluence with the VWAP, from another source.
        assert!(clusters.iter().all(|level| level.confluence == 1));
    }

    #[test]
    fn golden_location_levels_v1() {
        let mut engine = MarketStateEngine::new();
        let mut lines = Vec::new();
        let mut facts = Fingerprinter::new();
        let mut fact_count = 0;
        for event in walk_tape(0x6d69_6500_0000_0044, 7) {
            engine.apply(&event).unwrap();
            for fact in engine.location_events() {
                if matches!(fact, LocationEvent::Auction { .. }) {
                    continue;
                }
                facts.write_str(&format!("{} {fact}", fact.time()));
                fact_count += 1;
            }
            let closed = engine
                .closed_bars()
                .iter()
                .any(|bar| bar.timeframe == Timeframe::H4);
            if let (true, FeatureValue::Ready(set)) = (closed, &engine.state().location.levels) {
                let mut digest = Fingerprinter::new();
                set.encode(&mut digest);
                lines.push(format!("{} {set} {}", event.time(), digest.finish()));
            }
        }
        lines.push(format!("facts={fact_count} {}", facts.finish()));
        assert_eq!(lines, GOLDEN_LEVELS);
    }

    const GOLDEN_LEVELS: [&str; 40] = [
        "57643383ms close=61952.09000000 levels=2 in=- nearest=vwap@61863.66256600:-88.42743400,structural_low@61676.70000000:-275.39000000 location.levels@1 48c5aa34dbbefdeb",
        "72037693ms close=61233.69000000 levels=3 in=- nearest=prior_sweep@61676.70000000:443.01000000,vwap@61863.49739924:629.80739924,structural_high@62106.60000000:872.91000000 location.levels@1 bd6cedd46790e885",
        "86420258ms close=61406.66000000 levels=20 in=- nearest=hvn@61475.00000000:63.34000000,lvn@61505.00000000:93.34000000,hvn@61535.00000000:123.34000000 location.levels@1 5e5cc20e761be5b5",
        "100836539ms close=61691.19000000 levels=23 in=prior_sweep@61676.70000000,hvn@61725.00000000 nearest=prior_sweep@61676.70000000:-14.49000000,hvn@61725.00000000:28.81000000,lvn@61765.00000000:68.81000000 location.levels@1 2f0ffc066bc2ab59",
        "115203856ms close=61421.65000000 levels=28 in=prior_sweep@61406.66000000 nearest=prior_sweep@61406.66000000:-14.99000000,hvn@61475.00000000:48.35000000,lvn@61505.00000000:78.35000000 location.levels@1 ce8578536de93784",
        "129605687ms close=61805.33000000 levels=33 in=lvn@61765.00000000,prior_sweep@61812.25000000,sfp_zone@61812.25000000,poc@61825.00000000,hvn@61825.00000000 nearest=prior_sweep@61812.25000000:6.92000000,sfp_zone@61812.25000000:6.92000000,poc@61825.00000000:14.67000000 location.levels@1 5107558ea5dd5bc0",
        "144000697ms close=61915.17000000 levels=36 in=lvn@61875.00000000,hvn@61935.00000000 nearest=hvn@61935.00000000:14.83000000,lvn@61875.00000000:-35.17000000,lvn@61965.00000000:44.83000000 location.levels@1 85109b534acc2bd5",
        "158415979ms close=62326.31000000 levels=40 in=- nearest=sfp_zone@62106.60000000:-204.84000000,prior_sweep@62106.60000000:-219.71000000,prior_sweep@62106.60000000:-219.71000000 location.levels@1 623d4d408c1df253",
        "172814127ms close=62008.11000000 levels=43 in=lvn@61975.00000000,hvn@62005.00000000,prior_sweep@62029.35000000,prior_sweep@62029.35000000,vah@62040.00000000 nearest=hvn@62005.00000000:0.00000000,prior_sweep@62029.35000000:21.24000000,prior_sweep@62029.35000000:21.24000000 location.levels@1 2401ed9b37b105b4",
        "187223532ms close=61992.57000000 levels=47 in=lvn@61975.00000000,hvn@62005.00000000,prior_sweep@62029.35000000,prior_sweep@62029.35000000,vah@62040.00000000 nearest=hvn@62005.00000000:7.43000000,lvn@61975.00000000:-12.57000000,prior_sweep@62029.35000000:36.78000000 location.levels@1 efa1379ab5d209ad",
        "201601106ms close=61709.80000000 levels=50 in=prior_sweep@61676.70000000 nearest=prior_sweep@61676.70000000:-33.10000000,sfp_zone@61760.67000000:49.35000000,prior_sweep@61760.67000000:50.87000000 location.levels@1 bd7501143cd59b1e",
        "216021184ms close=61646.12000000 levels=54 in=prior_sweep@61676.70000000 nearest=prior_sweep@61676.70000000:30.58000000,lvn@61595.00000000:-46.12000000,prior_sweep@61705.73000000:59.61000000 location.levels@1 f8d7cc1aa5a9f205",
        "230456871ms close=62143.09000000 levels=62 in=hvn@62105.00000000,prior_sweep@62106.60000000,prior_sweep@62106.60000000,sfp_zone@62106.60000000,prior_sweep@62125.82000000,prior_sweep@62165.30000000 nearest=prior_sweep@62125.82000000:-17.27000000,sfp_zone@62106.60000000:-21.62000000,prior_sweep@62165.30000000:22.21000000 location.levels@1 950b2c8310f90efd",
        "244853059ms close=61896.28000000 levels=66 in=prior_sweep@61925.66000000 nearest=prior_sweep@61925.66000000:29.38000000,prior_sweep@61859.13000000:-37.15000000,sfp_zone@61812.25000000:-70.20000000 location.levels@1 0c5acc51bf3d7b14",
        "259217962ms close=62116.57000000 levels=68 in=poc@62105.00000000,hvn@62105.00000000,prior_sweep@62106.60000000,prior_sweep@62106.60000000,sfp_zone@62106.60000000,prior_sweep@62125.82000000,prior_sweep@62144.24000000,sfp_zone@62144.24000000,structural_high@62146.34000000 nearest=sfp_zone@62106.60000000:0.00000000,poc@62105.00000000:-6.57000000,hvn@62105.00000000:-6.57000000 location.levels@1 c290663bf840494e",
        "273630147ms close=61914.02000000 levels=73 in=hvn@61895.00000000,lvn@61925.00000000,prior_sweep@61925.66000000 nearest=lvn@61925.00000000:5.98000000,prior_sweep@61925.66000000:11.64000000,hvn@61895.00000000:-14.02000000 location.levels@1 cac9bc7dde023d58",
        "288002592ms close=62161.83000000 levels=74 in=prior_sweep@62125.82000000,prior_sweep@62144.24000000,sfp_zone@62144.24000000,prior_sweep@62146.34000000,prior_sweep@62146.34000000,prior_sweep@62165.30000000 nearest=prior_sweep@62165.30000000:3.47000000,sfp_zone@62144.24000000:-15.49000000,prior_sweep@62146.34000000:-15.49000000 location.levels@1 fc8c280764046fdd",
        "302429455ms close=61641.91000000 levels=78 in=sfp_zone@61644.12000000,sfp_zone@61750.57000000,lvn@61635.00000000,prior_sweep@61644.12000000,prior_sweep@61644.12000000,sfp_zone@61705.73000000,sfp_zone@61705.73000000 nearest=sfp_zone@61644.12000000:0.00000000,sfp_zone@61750.57000000:0.00000000,lvn@61635.00000000:-1.91000000 location.levels@1 c5f4dc3dca40ef5d",
        "316812845ms close=62038.16000000 levels=80 in=prior_sweep@62029.35000000,prior_sweep@62029.35000000 nearest=prior_sweep@62029.35000000:-8.81000000,prior_sweep@62029.35000000:-8.81000000,poc@62105.00000000:61.84000000 location.levels@1 8c43c90624157af2",
        "331208612ms close=61609.72000000 levels=81 in=sfp_zone@61578.84000000,sfp_zone@61644.12000000,sfp_zone@61750.57000000,prior_sweep@61578.84000000,prior_sweep@61578.84000000,prior_sweep@61584.30000000,prior_sweep@61614.20000000,prior_sweep@61614.20000000,lvn@61635.00000000,prior_sweep@61644.12000000,prior_sweep@61644.12000000,sfp_zone@61705.73000000,sfp_zone@61705.73000000 nearest=sfp_zone@61644.12000000:0.00000000,sfp_zone@61750.57000000:0.00000000,prior_sweep@61614.20000000:4.48000000 location.levels@1 b8a2fb8bdba5cbe3",
        "345626979ms close=61616.41000000 levels=87 in=sfp_zone@61644.12000000,sfp_zone@61750.57000000,prior_sweep@61584.30000000,structural_low@61594.26000000,structural_low@61594.26000000,sfp_zone@61614.20000000,sfp_zone@61614.20000000,prior_sweep@61614.20000000,prior_sweep@61614.20000000,prior_sweep@61644.12000000,prior_sweep@61644.12000000,sfp_zone@61705.73000000,sfp_zone@61705.73000000 nearest=sfp_zone@61644.12000000:0.00000000,sfp_zone@61750.57000000:0.00000000,sfp_zone@61614.20000000:-2.21000000 location.levels@1 ec7f5c4cc30ff911",
        "360012485ms close=62372.59000000 levels=87 in=sfp_zone@62312.18000000,prior_sweep@62343.34000000,prior_sweep@62343.34000000,prior_sweep@62343.35000000,prior_sweep@62343.35000000,prior_sweep@62343.35000000,sfp_zone@62343.35000000,sfp_zone@62343.35000000,prior_sweep@62402.12000000,prior_sweep@62402.12000000,prior_sweep@62402.12000000,sfp_zone@62402.12000000 nearest=sfp_zone@62312.18000000:0.00000000,sfp_zone@62343.35000000:0.00000000,sfp_zone@62343.35000000:0.00000000 location.levels@1 eb4e37e60b049bf1",
        "374407905ms close=62483.55000000 levels=91 in=sfp_zone@62402.12000000,structural_high@62530.70000000 nearest=sfp_zone@62402.12000000:0.00000000,structural_high@62530.70000000:47.15000000,sfp_zone@62312.18000000:-81.43000000 location.levels@1 f06f281a5979f6be",
        "388829346ms close=62816.00000000 levels=91 in=- nearest=structural_high@62933.74000000:117.74000000,sfp_zone@62402.12000000:-220.82000000,prior_sweep@62595.18000000:-220.82000000 location.levels@1 10dcf2afe0aab09e",
        "403205890ms close=62478.79000000 levels=91 in=sfp_zone@62402.12000000 nearest=sfp_zone@62402.12000000:0.00000000,vwap@62430.06359953:-48.72640047,prior_sweep@62530.70000000:51.91000000 location.levels@1 a5e80bc8124f009a",
        "417615844ms close=62383.48000000 levels=92 in=sfp_zone@62312.18000000,sfp_zone@62343.35000000,sfp_zone@62343.35000000,vwap@62386.66299949,prior_sweep@62402.12000000,prior_sweep@62402.12000000,prior_sweep@62402.12000000,sfp_zone@62402.12000000 nearest=sfp_zone@62312.18000000:0.00000000,sfp_zone@62343.35000000:0.00000000,sfp_zone@62343.35000000:0.00000000 location.levels@1 7222c21a97deafc7",
        "432006040ms close=61903.49000000 levels=108 in=sfp_zone@61867.24000000,prior_sweep@61867.24000000,prior_sweep@61867.24000000,sfp_zone@62018.22000000,lvn@61875.00000000,hvn@61885.00000000,hvn@61915.00000000,prior_sweep@61931.21000000 nearest=sfp_zone@62018.22000000:0.00000000,hvn@61915.00000000:6.51000000,hvn@61885.00000000:-13.49000000 location.levels@1 a7dfad908de4a86e",
        "446416920ms close=61935.23000000 levels=106 in=sfp_zone@62018.22000000,prior_sweep@61906.93000000,hvn@61915.00000000,prior_sweep@61931.21000000,prior_sweep@61943.13000000,prior_sweep@61958.18000000,lvn@61965.00000000,lvn@61965.00000000 nearest=sfp_zone@62018.22000000:0.00000000,prior_sweep@61931.21000000:-4.02000000,prior_sweep@61943.13000000:7.90000000 location.levels@1 5802f2323205e46f",
        "460829548ms close=61459.71000000 levels=106 in=structural_low@61435.04000000,structural_low@61435.04000000,structural_low@61435.04000000,sfp_zone@61471.38000000,sfp_zone@61530.85000000,prior_sweep@61471.38000000,prior_sweep@61471.39000000,prior_sweep@61471.39000000,sfp_zone@61578.84000000,prior_sweep@61488.78000000,prior_sweep@61488.78000000,sfp_zone@61594.26000000,sfp_zone@61609.65000000 nearest=sfp_zone@61471.38000000:0.00000000,sfp_zone@61530.85000000:0.00000000,prior_sweep@61471.38000000:11.67000000 location.levels@1 6ff43df8f6e64984",
        "475232978ms close=61579.16000000 levels=109 in=sfp_zone@61578.84000000,sfp_zone@61594.26000000,sfp_zone@61609.65000000,sfp_zone@61644.12000000,sfp_zone@61750.57000000,hvn@61555.00000000,prior_sweep@61578.84000000,prior_sweep@61578.84000000,lvn@61585.00000000,prior_sweep@61594.26000000,prior_sweep@61594.26000000,sfp_zone@61614.20000000,sfp_zone@61614.20000000,prior_sweep@61609.65000000,prior_sweep@61609.65000000,prior_sweep@61614.20000000,prior_sweep@61614.20000000 nearest=sfp_zone@61594.26000000:0.00000000,sfp_zone@61609.65000000:0.00000000,sfp_zone@61644.12000000:0.00000000 location.levels@1 89a57d6d8cf292df",
        "489609614ms close=61937.89000000 levels=109 in=sfp_zone@62018.22000000,prior_sweep@61906.93000000,hvn@61915.00000000,prior_sweep@61943.13000000,lvn@61965.00000000,lvn@61965.00000000 nearest=sfp_zone@62018.22000000:0.00000000,prior_sweep@61943.13000000:5.24000000,hvn@61915.00000000:-17.89000000 location.levels@1 a50813cabb9db19c",
        "504002804ms close=61757.72000000 levels=113 in=sfp_zone@61750.57000000,sfp_zone@61867.24000000,hvn@61755.00000000,prior_sweep@61750.57000000,lvn@61795.00000000 nearest=sfp_zone@61867.24000000:0.00000000,hvn@61755.00000000:0.00000000,sfp_zone@61750.57000000:-7.15000000 location.levels@1 f32271e93f62f2c2",
        "518408813ms close=62230.26000000 levels=109 in=sfp_zone@62159.22000000,vah@62220.00000000,prior_sweep@62210.85000000,prior_sweep@62210.85000000,lvn@62235.00000000,lvn@62235.00000000,prior_sweep@62230.87000000,hvn@62265.00000000 nearest=lvn@62235.00000000:0.00000000,lvn@62235.00000000:0.00000000,prior_sweep@62230.87000000:0.61000000 location.levels@1 705f038629f36bb5",
        "532802072ms close=62012.60000000 levels=112 in=sfp_zone@62018.22000000,lvn@61975.00000000,lvn@61985.00000000,prior_sweep@61991.26000000,hvn@62015.00000000,prior_sweep@62018.22000000,prior_sweep@62018.22000000,hvn@62025.00000000,vwap@62024.35628948,prior_sweep@62032.86000000,sfp_zone@62032.86000000 nearest=sfp_zone@62018.22000000:0.00000000,hvn@62015.00000000:0.00000000,prior_sweep@62018.22000000:5.62000000 location.levels@1 2a69e5745d936e9a",
        "547205592ms close=62238.17000000 levels=112 in=sfp_zone@62159.22000000,vah@62220.00000000,prior_sweep@62210.85000000,prior_sweep@62210.85000000,lvn@62235.00000000,lvn@62235.00000000,prior_sweep@62230.87000000,hvn@62265.00000000,hvn@62275.00000000,structural_high@62282.58000000,structural_high@62282.58000000 nearest=lvn@62235.00000000:0.00000000,lvn@62235.00000000:0.00000000,prior_sweep@62230.87000000:-7.30000000 location.levels@1 df50b83c053e492e",
        "561611926ms close=61608.87000000 levels=107 in=sfp_zone@61578.84000000,sfp_zone@61594.26000000,sfp_zone@61609.65000000,prior_sweep@61578.84000000,prior_sweep@61579.16000000,val@61580.00000000,prior_sweep@61594.26000000,prior_sweep@61594.26000000,sfp_zone@61614.20000000,prior_sweep@61609.65000000,prior_sweep@61609.65000000,prior_sweep@61614.20000000,prior_sweep@61623.42000000,prior_sweep@61623.42000000,hvn@61645.00000000 nearest=sfp_zone@61609.65000000:0.00000000,sfp_zone@61614.20000000:0.00000000,prior_sweep@61609.65000000:0.78000000 location.levels@1 65f7b5eeefbada59",
        "576009400ms close=61926.26000000 levels=109 in=sfp_zone@62018.22000000,hvn@61885.00000000,lvn@61925.00000000,hvn@61955.00000000 nearest=sfp_zone@62018.22000000:0.00000000,lvn@61925.00000000:0.00000000,hvn@61955.00000000:23.74000000 location.levels@1 7477fad6d4fb6e8c",
        "590417633ms close=61384.82000000 levels=110 in=structural_low@61356.52000000,structural_low@61356.52000000,sfp_zone@61419.92000000,sfp_zone@61435.04000000,sfp_zone@61435.04000000,sfp_zone@61456.17000000,sfp_zone@61456.17000000,sfp_zone@61467.24000000,prior_sweep@61419.92000000,prior_sweep@61419.92000000 nearest=sfp_zone@61419.92000000:0.00000000,sfp_zone@61435.04000000:0.00000000,sfp_zone@61435.04000000:0.00000000 location.levels@1 e51905465b646379",
        "604819944ms close=61623.97000000 levels=110 in=sfp_zone@61594.26000000,sfp_zone@61609.65000000,val@61590.00000000,prior_sweep@61594.26000000,prior_sweep@61594.26000000,sfp_zone@61614.20000000,prior_sweep@61609.65000000,prior_sweep@61614.20000000,prior_sweep@61623.42000000,prior_sweep@61623.42000000,val@61640.00000000 nearest=prior_sweep@61623.42000000:-0.55000000,prior_sweep@61623.42000000:-0.55000000,sfp_zone@61614.20000000:-9.77000000 location.levels@1 1586a4d1a4e56a30",
        "facts=8303 f7d11d5b0ee97231",
    ];

    #[test]
    fn duplicates_collapse_and_the_order_is_by_zone() {
        let poc = point(LevelKind::Poc, "profile.volume.prior_day", "60000");
        let (set, _) = step(None, &[poc, poc], 0, ("59000", "59100", "59050"));
        assert_eq!(set.levels().len(), 1);
        assert_eq!(
            set.to_string(),
            "close=59050.00000000 levels=1 in=- nearest=poc@60000.00000000:950.00000000 \
             location.levels@1"
        );
        assert_eq!(
            set.levels()[0].to_string(),
            "poc@60000.00000000 t=0 c=0 d=950.00000000"
        );
        assert_eq!(set.nearest(5).len(), 1);
        assert_eq!(
            set.levels()[0].distance_bps(set.close).map(f64::round),
            Some(161.0)
        );
        assert_eq!(set.levels()[0].age_ms(t(61_000 + 500)), 500);
    }
}
