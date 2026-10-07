//! Volume profile: POC, value area, HVN and LVN (brief §8 and §10; Market
//! State & Regime brief; ADR-036, proposed).
//!
//! Every trade adds its exact quantity to a fixed 10 USDT price bin
//! (decision 1); nothing is spread or estimated from bars. Three profiles
//! (decision 2):
//!
//! - `profile.volume.utc_day@1`: the developing profile of the current UTC
//!   day, recomputed once per closed 1m bar from the trades of closed
//!   minutes only — the developing minute is never included.
//! - `profile.volume.prior_day@1`: the completed profile of the last closed
//!   UTC day.
//! - `profile.volume.composite_5d@1`: the bin-wise sum of the last five
//!   completed UTC days.
//!
//! Each [`VolumeProfile`] carries its POC (decision 4), value area
//! (decision 5) and high- and low-volume nodes (decision 6), and hands them
//! to location as [`ProfileLevel`]s ([`VolumeProfile::levels`]). Everything
//! is integer arithmetic on fixed point (ADR-027, decision 7): no float is
//! computed or stored, so every tie and threshold is exact and the state
//! stays `Eq`.
//!
//! A volume profile describes where volume traded, never which way price
//! will go (ADR-023, ADR-024).

use crate::bars::{Bar, Coverage, Timeframe};
use crate::event::MarketEvent;
use crate::feature::{FeatureKey, FeatureValue, Unavailability, catalog};
use crate::location::LevelKind;
use crate::num::{Price, Qty, SCALE};
use crate::time::EventTime;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;

/// The bin size: parameter `bin_size` (ADR-036, decision 1).
pub const BIN_SIZE: Price = Price::from_units(10 * SCALE);

/// [`BIN_SIZE`] in `1 / SCALE` units.
const BIN_UNITS: i64 = BIN_SIZE.units();

/// Half a bin: the offset of a bin's midpoint from its lower edge.
const HALF_BIN_UNITS: i64 = BIN_UNITS / 2;

// A positive, even bin size keeps every midpoint exact.
const _: () = assert!(BIN_UNITS > 0 && BIN_UNITS % 2 == 0);

/// The widest profile in bins: parameter `max_bins` (decision 1). A wider
/// range is `Unavailable(OutOfRange)`.
pub const MAX_BINS: usize = 10_000;

/// The value area's share of the volume in percent: parameter
/// `value_area_pct` (decision 5).
pub const VALUE_AREA_PCT: i64 = 70;

/// The least prominence of a node, in percent of the highest smoothed bin:
/// parameter `node_prominence_pct` (decision 6).
pub const NODE_PROMINENCE_PCT: i64 = 10;

/// Completed UTC days in the composite: parameter `sessions` of
/// `profile.volume.composite_5d@1` (decision 2).
pub const COMPOSITE_SESSIONS: usize = 5;

/// The integer triangular kernel `triangular_5` that smooths the histogram
/// before node detection (decision 6), centred on the middle weight.
pub const NODE_KERNEL: [i64; 5] = [1, 2, 3, 2, 1];

/// A high- or low-volume node: one bin of the profile (decision 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileNode {
    /// The bin's midpoint.
    pub price: Price,
    /// The bin's lower edge (inclusive).
    pub low: Price,
    /// The bin's upper edge (exclusive).
    pub high: Price,
    /// The raw volume in the bin.
    pub volume: Qty,
    /// `floor(1000 · prominence / max(s))`: the prominence on the smoothed
    /// series, in permille of its highest bin.
    pub prominence_permille: u32,
}

/// A volume profile with its levels: one `profile.volume.*@1` value
/// (ADR-036).
///
/// Prices are bin midpoints except VAL and VAH, which are bin edges: value
/// is `[val, vah)`. `Display` prints the canonical line the golden tests
/// pin, nodes as `price:prominence_permille`, ending with the feature key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeProfile {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// Open of the first UTC day included.
    pub start: EventTime,
    /// Exclusive end of what is included: the last folded minute for the
    /// developing profile, the last included day for completed ones.
    pub end: EventTime,
    /// UTC days included: 1, or [`COMPOSITE_SESSIONS`] for the composite.
    pub sessions: u32,
    /// Volume in the profile.
    pub total_volume: Qty,
    /// Lower edge of the lowest bin with volume.
    pub low: Price,
    /// Upper (exclusive) edge of the highest bin with volume.
    pub high: Price,
    /// Point of control: the midpoint of the bin with the most volume
    /// (decision 4).
    pub poc: Price,
    /// Volume in the POC bin.
    pub poc_volume: Qty,
    /// Value-area low: the lower edge of the lowest value-area bin
    /// (decision 5).
    pub val: Price,
    /// Value-area high: the upper (exclusive) edge of the highest value-area
    /// bin.
    pub vah: Price,
    /// Volume in the value area: at least [`VALUE_AREA_PCT`] % of the total.
    pub value_area_volume: Qty,
    /// High-volume nodes, by price ascending (decision 6).
    pub hvn: Vec<ProfileNode>,
    /// Low-volume nodes, by price ascending (decision 6).
    pub lvn: Vec<ProfileNode>,
    /// The OR of the inputs' coverage (decision 3).
    pub coverage: Coverage,
}

/// A level for location (brief §10): the hand-off to the level registry
/// (#23). Its zone `[low, high)` is the level's bin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileLevel {
    /// Which level.
    pub kind: LevelKind,
    /// The level's price: a bin midpoint, or a value-area edge.
    pub price: Price,
    /// Lower edge of the level's bin (inclusive).
    pub low: Price,
    /// Upper edge of the level's bin (exclusive).
    pub high: Price,
    /// The profile that produced the level.
    pub source: FeatureKey,
}

impl VolumeProfile {
    /// The profile's levels: POC, VAL and VAH, then every HVN, then every
    /// LVN, each node group by price ascending. VAL's zone is the lowest
    /// value-area bin, VAH's the highest.
    pub fn levels(&self) -> impl Iterator<Item = ProfileLevel> + '_ {
        let level = move |kind, price, low, high| ProfileLevel {
            kind,
            price,
            low,
            high,
            source: self.feature,
        };
        // Every bin edge of the profile is representable (decision 1), so
        // these offsets stay inside its range.
        let shift = |price: Price, units: i64| Price::from_units(price.units() + units);
        [
            level(
                LevelKind::Poc,
                self.poc,
                shift(self.poc, -HALF_BIN_UNITS),
                shift(self.poc, HALF_BIN_UNITS),
            ),
            level(
                LevelKind::Val,
                self.val,
                self.val,
                shift(self.val, BIN_UNITS),
            ),
            level(
                LevelKind::Vah,
                self.vah,
                shift(self.vah, -BIN_UNITS),
                self.vah,
            ),
        ]
        .into_iter()
        .chain(
            self.hvn
                .iter()
                .map(move |node| level(LevelKind::Hvn, node.price, node.low, node.high)),
        )
        .chain(
            self.lvn
                .iter()
                .map(move |node| level(LevelKind::Lvn, node.price, node.low, node.high)),
        )
    }
}

/// Writes nodes as `price:permille` separated by commas, or `-` for none.
fn write_nodes(f: &mut fmt::Formatter<'_>, nodes: &[ProfileNode]) -> fmt::Result {
    if nodes.is_empty() {
        return f.write_str("-");
    }
    for (index, node) in nodes.iter().enumerate() {
        if index > 0 {
            f.write_str(",")?;
        }
        write!(f, "{}:{}", node.price, node.prominence_permille)?;
    }
    Ok(())
}

impl fmt::Display for VolumeProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "start={} end={} sessions={} vol={} low={} high={} poc={} poc_vol={} \
             val={} vah={} va_vol={} hvn=",
            self.start,
            self.end,
            self.sessions,
            self.total_volume,
            self.low,
            self.high,
            self.poc,
            self.poc_volume,
            self.val,
            self.vah,
            self.value_area_volume
        )?;
        write_nodes(f, &self.hvn)?;
        f.write_str(" lvn=")?;
        write_nodes(f, &self.lvn)?;
        write!(f, " {} {}", self.coverage, self.feature)
    }
}

/// The volume profiles of the Market State (ADR-036).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeProfiles {
    /// `profile.volume.utc_day@1`, warming up until a minute of the current
    /// UTC day with volume closes.
    pub utc_day: FeatureValue<VolumeProfile>,
    /// `profile.volume.prior_day@1`, warming up until the first UTC day
    /// closes.
    pub prior_day: FeatureValue<VolumeProfile>,
    /// `profile.volume.composite_5d@1`, warming up until five UTC days have
    /// closed.
    pub composite_5d: FeatureValue<VolumeProfile>,
}

impl Default for VolumeProfiles {
    fn default() -> Self {
        Self::new()
    }
}

impl VolumeProfiles {
    /// Every profile warming up.
    pub fn new() -> Self {
        Self {
            utc_day: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            prior_day: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            composite_5d: FeatureValue::WarmingUp {
                observed: 0,
                required: COMPOSITE_SESSIONS as u64,
            },
        }
    }
}

/// The bin of `price` (decision 1).
fn bin_of(price: Price) -> i64 {
    price.units().div_euclid(BIN_UNITS)
}

/// The lower edge, midpoint and upper edge of bin `bin`, if representable.
fn bin_prices(bin: i64) -> Option<(Price, Price, Price)> {
    let low = bin.checked_mul(BIN_UNITS)?;
    let high = low.checked_add(BIN_UNITS)?;
    Some((
        Price::from_units(low),
        Price::from_units(low + HALF_BIN_UNITS),
        Price::from_units(high),
    ))
}

/// The levels of a dense histogram: a [`VolumeProfile`] without its window.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Levels {
    total_volume: Qty,
    low: Price,
    high: Price,
    poc: Price,
    poc_volume: Qty,
    val: Price,
    vah: Price,
    value_area_volume: Qty,
    hvn: Vec<ProfileNode>,
    lvn: Vec<ProfileNode>,
}

/// The levels of the dense histogram `bins`, whose first bin is `low_bin`
/// (decisions 4–6).
///
/// `bins` is non-empty, holds no negative volume, and its first and last
/// bins hold volume. `Ok(None)` when a bin edge is not representable as a
/// [`Price`] (the caller reports `OutOfRange`).
///
/// # Errors
///
/// [`ProfileError::Overflow`] if the total volume leaves the `i64` range.
fn levels(low_bin: i64, bins: &[Qty]) -> Result<Option<Levels>, ProfileError> {
    let total: i128 = bins.iter().map(|volume| i128::from(volume.units())).sum();
    let total_volume = Qty::from_units(i64::try_from(total).map_err(|_| ProfileError::Overflow)?);
    let Some(last_bin) = i64::try_from(bins.len())
        .ok()
        .and_then(|len| low_bin.checked_add(len - 1))
    else {
        return Ok(None);
    };
    // Both outer edges representable: so is every edge between them.
    let (Some((low, _, _)), Some((_, _, high))) = (bin_prices(low_bin), bin_prices(last_bin))
    else {
        return Ok(None);
    };
    let prices = |index: usize| {
        // `index < bins.len()`, inside the representable range above.
        let offset = i64::try_from(index).unwrap_or(i64::MAX);
        let low = low.units() + offset * BIN_UNITS;
        (
            Price::from_units(low),
            Price::from_units(low + HALF_BIN_UNITS),
            Price::from_units(low + BIN_UNITS),
        )
    };
    let poc = poc(bins);
    let (lo, hi, value_area) = value_area(bins, poc, total);
    let smoothed = smooth(bins);
    let (peaks, valleys) = nodes(&smoothed);
    let to_node = |candidate: &Candidate| {
        let (low, price, high) = prices(candidate.index);
        ProfileNode {
            price,
            low,
            high,
            volume: bins[candidate.index],
            prominence_permille: candidate.permille,
        }
    };
    Ok(Some(Levels {
        total_volume,
        low,
        high,
        poc: prices(poc).1,
        poc_volume: bins[poc],
        val: prices(lo).0,
        vah: prices(hi).2,
        // At most the total, which fits.
        value_area_volume: Qty::from_units(i64::try_from(value_area).unwrap_or(i64::MAX)),
        hvn: peaks.iter().map(to_node).collect(),
        lvn: valleys.iter().map(to_node).collect(),
    }))
}

/// The distance of bin `index` from the centre of `len` bins, doubled so it
/// is exact: `|2 · index − (len − 1)|`.
fn centre_distance(index: usize, len: usize) -> usize {
    (2 * index).abs_diff(len - 1)
}

/// The POC bin (decision 4): the most volume; a tie goes to the bin nearest
/// the range centre, then to the lower bin. `bins` is non-empty.
fn poc(bins: &[Qty]) -> usize {
    let len = bins.len();
    let mut best = 0;
    for (index, volume) in bins.iter().enumerate().skip(1) {
        let nearer = centre_distance(index, len) < centre_distance(best, len);
        if *volume > bins[best] || (*volume == bins[best] && nearer) {
            best = index;
        }
    }
    best
}

/// The value area (decision 5): single-bin expansion from `poc` until it
/// holds [`VALUE_AREA_PCT`] % of `total`, adding the larger neighbour, or
/// both when they are equal. Returns the lowest and highest bin and the
/// volume.
fn value_area(bins: &[Qty], poc: usize, total: i128) -> (usize, usize, i128) {
    let (mut lo, mut hi) = (poc, poc);
    let mut volume = i128::from(bins[poc].units());
    let target = total * i128::from(VALUE_AREA_PCT);
    while volume * 100 < target {
        let below = lo.checked_sub(1).map(|index| bins[index]);
        let above = bins.get(hi + 1).copied();
        match (below, above) {
            (Some(below), Some(above)) if above > below => {
                hi += 1;
                volume += i128::from(above.units());
            }
            (Some(below), Some(above)) if below > above => {
                lo -= 1;
                volume += i128::from(below.units());
            }
            (Some(below), Some(above)) => {
                lo -= 1;
                hi += 1;
                volume += i128::from(below.units()) + i128::from(above.units());
            }
            (Some(below), None) => {
                lo -= 1;
                volume += i128::from(below.units());
            }
            (None, Some(above)) => {
                hi += 1;
                volume += i128::from(above.units());
            }
            // The whole range holds the total: unreachable below the target.
            (None, None) => break,
        }
    }
    (lo, hi, volume)
}

/// The smoothed series `s[k] = Σ w_j · v[k+j]` with [`NODE_KERNEL`]; bins
/// outside the range count as 0 (decision 6).
fn smooth(bins: &[Qty]) -> Vec<i128> {
    let reach = NODE_KERNEL.len() / 2;
    (0..bins.len())
        .map(|center| {
            NODE_KERNEL
                .iter()
                .enumerate()
                .filter_map(|(offset, weight)| {
                    let index = (center + offset).checked_sub(reach)?;
                    let volume = bins.get(index)?;
                    Some(i128::from(*weight) * i128::from(volume.units()))
                })
                .sum()
        })
        .collect()
}

/// A detected node on the smoothed series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Candidate {
    /// The lower-middle bin of the run.
    index: usize,
    /// The topographic prominence.
    prominence: i128,
    /// `floor(1000 · prominence / max(s))`.
    permille: u32,
}

/// The peaks and valleys of `s` that meet [`NODE_PROMINENCE_PCT`]
/// (decision 6), each by index ascending. `s` is non-empty and
/// non-negative.
fn nodes(s: &[i128]) -> (Vec<Candidate>, Vec<Candidate>) {
    let max = s.iter().copied().max().unwrap_or(0);
    let keep = |index: usize, prominence: i128| {
        (max > 0 && prominence * 100 >= max * i128::from(NODE_PROMINENCE_PCT)).then(|| Candidate {
            index,
            prominence,
            // At most 1000: the prominence never exceeds the maximum.
            permille: u32::try_from(prominence * 1000 / max).unwrap_or(u32::MAX),
        })
    };
    let (mut peaks, mut valleys) = (Vec::new(), Vec::new());
    let mut start = 0;
    while start < s.len() {
        let level = s[start];
        let mut end = start;
        while end + 1 < s.len() && s[end + 1] == level {
            end += 1;
        }
        let left = start.checked_sub(1).map(|index| s[index]);
        let right = s.get(end + 1).copied();
        let position = start + (end - start) / 2;
        // Beyond the range nothing traded: a virtual 0 for peaks.
        if left.unwrap_or(0) < level && right.unwrap_or(0) < level {
            let base = peak_base(s[..start].iter().rev(), level)
                .max(peak_base(s[end + 1..].iter(), level));
            peaks.extend(keep(position, level - base));
        } else if left.is_some_and(|left| left > level) && right.is_some_and(|right| right > level)
        {
            let top = valley_top(s[..start].iter().rev(), level)
                .min(valley_top(s[end + 1..].iter(), level));
            valleys.extend(keep(position, top - level));
        }
        start = end + 1;
    }
    (peaks, valleys)
}

/// The base of a peak of height `level` on one side: the minimum of `side`
/// (walking outward) before the first strictly higher bin, or 0 when the
/// walk leaves the range (the virtual 0 beyond it).
fn peak_base<'a>(side: impl Iterator<Item = &'a i128>, level: i128) -> i128 {
    let mut base = level;
    for &value in side {
        if value > level {
            return base;
        }
        base = base.min(value);
    }
    0
}

/// The top of a valley of depth `level` on one side: the maximum of `side`
/// (walking outward) before the first strictly lower bin, or up to the
/// range edge.
fn valley_top<'a>(side: impl Iterator<Item = &'a i128>, level: i128) -> i128 {
    let mut top = level;
    for &value in side {
        if value < level {
            break;
        }
        top = top.max(value);
    }
    top
}

/// What a profile value covers besides its histogram.
struct Window {
    feature: FeatureKey,
    start: EventTime,
    end: EventTime,
    sessions: u32,
    coverage: Coverage,
}

/// The profile of the bin-wise sum of `histograms` over `window`
/// (decisions 1 and 4–6): `Unavailable(InputInvalid)` without volume,
/// `Unavailable(OutOfRange)` beyond [`MAX_BINS`] or the `Price` range.
///
/// # Errors
///
/// [`ProfileError::Overflow`] if a bin or the total leaves the `i64` range.
fn profile(
    histograms: &[&BTreeMap<i64, Qty>],
    window: Window,
) -> Result<FeatureValue<VolumeProfile>, ProfileError> {
    let unavailable = |reason| Ok(FeatureValue::Unavailable { reason });
    let first = histograms
        .iter()
        .filter_map(|bins| bins.keys().next())
        .min();
    let last = histograms
        .iter()
        .filter_map(|bins| bins.keys().next_back())
        .max();
    let (Some(&first), Some(&last)) = (first, last) else {
        return unavailable(Unavailability::InputInvalid);
    };
    let span = i128::from(last) - i128::from(first) + 1;
    let Some(len) = usize::try_from(span).ok().filter(|len| *len <= MAX_BINS) else {
        return unavailable(Unavailability::OutOfRange);
    };
    let mut dense = vec![Qty::from_units(0); len];
    for bins in histograms {
        for (bin, volume) in *bins {
            // `first ≤ bin ≤ last`, so the offset is below `len`.
            let slot = &mut dense[usize::try_from(bin - first).unwrap_or(0)];
            *slot = slot.checked_add(*volume).ok_or(ProfileError::Overflow)?;
        }
    }
    let Some(levels) = levels(first, &dense)? else {
        return unavailable(Unavailability::OutOfRange);
    };
    Ok(FeatureValue::Ready(VolumeProfile {
        feature: window.feature,
        start: window.start,
        end: window.end,
        sessions: window.sessions,
        total_volume: levels.total_volume,
        low: levels.low,
        high: levels.high,
        poc: levels.poc,
        poc_volume: levels.poc_volume,
        val: levels.val,
        vah: levels.vah,
        value_area_volume: levels.value_area_volume,
        hvn: levels.hvn,
        lvn: levels.lvn,
        coverage: window.coverage,
    }))
}

/// One completed UTC day.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Session {
    /// Open of the day.
    open: EventTime,
    /// Volume per bin.
    bins: BTreeMap<i64, Qty>,
    /// The closed daily bar's coverage.
    coverage: Coverage,
}

/// The volume-profile engine state (ADR-036): the developing day's volume
/// per bin and the last [`COMPOSITE_SESSIONS`] completed days. Engine state;
/// the Market State exposes only [`VolumeProfiles`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProfileTracker {
    /// Volume per bin of the developing UTC day, the developing minute's
    /// trades included.
    developing: BTreeMap<i64, Qty>,
    /// The OR of the coverage of the day's closed minutes.
    coverage: Coverage,
    /// The day's closed minutes with volume.
    minutes_with_volume: u64,
    /// The latest completed days, oldest first.
    sessions: VecDeque<Session>,
    /// UTC days closed since the start.
    closed_days: u64,
}

/// The tracker after one event, committed with the bars it was computed
/// from.
pub(crate) struct ProfileStep {
    coverage: Coverage,
    minutes_with_volume: u64,
    /// Open and coverage of every UTC day the event closed, in close order:
    /// the first takes the developing histogram, later ones are empty.
    closed_days: Vec<(EventTime, Coverage)>,
    /// New values; `None` leaves a value as it is.
    utc_day: Option<FeatureValue<VolumeProfile>>,
    prior_day: Option<FeatureValue<VolumeProfile>>,
    composite_5d: Option<FeatureValue<VolumeProfile>>,
    /// The trade's bin and its new volume, after any closed day.
    insert: Option<(i64, Qty)>,
}

impl Default for ProfileTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ProfileTracker {
    /// An empty tracker.
    pub(crate) fn new() -> Self {
        Self {
            developing: BTreeMap::new(),
            coverage: Coverage::default(),
            minutes_with_volume: 0,
            sessions: VecDeque::new(),
            closed_days: 0,
        }
    }

    /// Steps the profiles with `event`, given the bars it closed (`closed`,
    /// in close order), without changing the tracker (ADR-036).
    ///
    /// Order: closed bars are walked in close order — a 1m bar folds into
    /// the developing day, a 1d bar completes it (any further day the same
    /// jump closes is empty); then the affected values are recomputed; then
    /// a trade's new bin volume is computed against the developing
    /// histogram, empty if a day closed. Nothing is cloned per trade.
    ///
    /// # Errors
    ///
    /// [`ProfileError::Overflow`] if a bin, a total or a count leaves its
    /// integer range; nothing is committed then.
    pub(crate) fn step(
        &self,
        event: &MarketEvent,
        closed: &[Bar],
    ) -> Result<ProfileStep, ProfileError> {
        let mut step = ProfileStep {
            coverage: self.coverage,
            minutes_with_volume: self.minutes_with_volume,
            closed_days: Vec::new(),
            utc_day: None,
            prior_day: None,
            composite_5d: None,
            insert: None,
        };
        let mut last_minute = None;
        for bar in closed {
            match bar.timeframe {
                Timeframe::M1 => {
                    if bar.volume.units() > 0 {
                        step.minutes_with_volume = step
                            .minutes_with_volume
                            .checked_add(1)
                            .ok_or(ProfileError::Overflow)?;
                    }
                    step.coverage.partial_start |= bar.coverage.partial_start;
                    step.coverage.feed_gap |= bar.coverage.feed_gap;
                    last_minute = Some(bar);
                }
                Timeframe::D1 => {
                    step.closed_days.push((bar.open_time, bar.coverage));
                    step.coverage = Coverage::default();
                    step.minutes_with_volume = 0;
                }
                Timeframe::M5 | Timeframe::M15 | Timeframe::H1 | Timeframe::H4 => {}
            }
        }
        let empty = BTreeMap::new();
        let developing = if step.closed_days.is_empty() {
            &self.developing
        } else {
            &empty
        };
        if let Some(minute) = last_minute {
            step.utc_day = Some(if step.minutes_with_volume == 0 {
                FeatureValue::WarmingUp {
                    observed: 0,
                    required: 1,
                }
            } else {
                let start = Timeframe::D1
                    .open_of(minute.open_time)
                    .ok_or(ProfileError::Overflow)?;
                profile(
                    &[developing],
                    Window {
                        feature: catalog::PROFILE_VOLUME_UTC_DAY_V1.key,
                        start,
                        end: minute.end(),
                        sessions: 1,
                        coverage: step.coverage,
                    },
                )?
            });
        }
        if !step.closed_days.is_empty() {
            self.complete(&mut step)?;
        }
        if let MarketEvent::Trade(trade) = event
            && trade.qty.units() > 0
        {
            let bin = bin_of(trade.price);
            let volume = developing
                .get(&bin)
                .copied()
                .unwrap_or(Qty::from_units(0))
                .checked_add(trade.qty)
                .ok_or(ProfileError::Overflow)?;
            step.insert = Some((bin, volume));
        }
        Ok(step)
    }

    /// Recomputes the completed profiles after the days in
    /// `step.closed_days`: the developing histogram completes the first,
    /// later ones are empty.
    fn complete(&self, step: &mut ProfileStep) -> Result<(), ProfileError> {
        let empty = BTreeMap::new();
        let closed_days = u64::try_from(step.closed_days.len())
            .ok()
            .and_then(|days| self.closed_days.checked_add(days))
            .ok_or(ProfileError::Overflow)?;
        // The completed days after the step, oldest first: the kept ones,
        // then the new ones.
        let new = step
            .closed_days
            .iter()
            .enumerate()
            .map(|(index, (open, coverage))| {
                let bins = if index == 0 { &self.developing } else { &empty };
                (*open, bins, *coverage)
            });
        let all: Vec<(EventTime, &BTreeMap<i64, Qty>, Coverage)> = self
            .sessions
            .iter()
            .map(|session| (session.open, &session.bins, session.coverage))
            .chain(new)
            .collect();
        let recent = &all[all.len().saturating_sub(COMPOSITE_SESSIONS)..];
        let window = |feature: FeatureKey,
                      days: &[(EventTime, &BTreeMap<i64, Qty>, Coverage)]|
         -> Result<Window, ProfileError> {
            let (first, last) = (days[0], days[days.len() - 1]);
            let mut coverage = Coverage::default();
            for (_, _, day) in days {
                coverage.partial_start |= day.partial_start;
                coverage.feed_gap |= day.feed_gap;
            }
            let end = last
                .0
                .as_millis()
                .checked_add(Timeframe::D1.millis())
                .ok_or(ProfileError::Overflow)?;
            Ok(Window {
                feature,
                start: first.0,
                end: EventTime::from_millis(end),
                sessions: u32::try_from(days.len()).map_err(|_| ProfileError::Overflow)?,
                coverage,
            })
        };
        let prior = &recent[recent.len() - 1..];
        step.prior_day = Some(profile(
            &[prior[0].1],
            window(catalog::PROFILE_VOLUME_PRIOR_DAY_V1.key, prior)?,
        )?);
        step.composite_5d = Some(if recent.len() < COMPOSITE_SESSIONS {
            FeatureValue::WarmingUp {
                observed: closed_days,
                required: COMPOSITE_SESSIONS as u64,
            }
        } else {
            let histograms: Vec<&BTreeMap<i64, Qty>> =
                recent.iter().map(|(_, bins, _)| *bins).collect();
            profile(
                &histograms,
                window(catalog::PROFILE_VOLUME_COMPOSITE_5D_V1.key, recent)?,
            )?
        });
        Ok(())
    }

    /// Commits a step computed by [`Self::step`] and writes its new values
    /// to `profiles`. Moves and one insert: it cannot fail.
    pub(crate) fn commit(&mut self, step: ProfileStep, profiles: &mut VolumeProfiles) {
        for (index, (open, coverage)) in step.closed_days.iter().enumerate() {
            let bins = if index == 0 {
                std::mem::take(&mut self.developing)
            } else {
                BTreeMap::new()
            };
            if self.sessions.len() == COMPOSITE_SESSIONS {
                self.sessions.pop_front();
            }
            self.sessions.push_back(Session {
                open: *open,
                bins,
                coverage: *coverage,
            });
        }
        // `step` checked the sum.
        self.closed_days = self
            .closed_days
            .saturating_add(u64::try_from(step.closed_days.len()).unwrap_or(u64::MAX));
        self.coverage = step.coverage;
        self.minutes_with_volume = step.minutes_with_volume;
        if let Some((bin, volume)) = step.insert {
            self.developing.insert(bin, volume);
        }
        if let Some(value) = step.utc_day {
            profiles.utc_day = value;
        }
        if let Some(value) = step.prior_day {
            profiles.prior_day = value;
        }
        if let Some(value) = step.composite_5d {
            profiles.composite_5d = value;
        }
    }
}

/// Why the volume profile could not take an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileError {
    /// A bin volume, a profile total or a count left its integer range
    /// (ADR-027).
    Overflow,
}

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => f.write_str("a volume-profile value leaves its integer range"),
        }
    }
}

impl std::error::Error for ProfileError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::tests::{Lcg, random_tape};
    use crate::event::samples::{gap, mark, t};
    use crate::event::{Aggressor, GapReason, Stream, Trade};
    use crate::feature::{FeatureDefinition, ParamValue, WarmUp};
    use crate::state::MarketStateEngine;

    const DAY: i64 = 86_400_000;
    const MINUTE: i64 = 60_000;
    const COMPLETE: Coverage = Coverage {
        partial_start: false,
        feed_gap: false,
    };
    const PARTIAL: Coverage = Coverage {
        partial_start: true,
        feed_gap: false,
    };
    const GAP: Coverage = Coverage {
        partial_start: false,
        feed_gap: true,
    };

    fn price(text: &str) -> Price {
        text.parse().unwrap()
    }

    fn qty(text: &str) -> Qty {
        text.parse().unwrap()
    }

    /// A histogram of raw unit volumes.
    fn hist(volumes: &[i64]) -> Vec<Qty> {
        volumes.iter().copied().map(Qty::from_units).collect()
    }

    fn trade(
        millis: i64,
        trade_id: u64,
        at: &str,
        size: &str,
        aggressor: Aggressor,
    ) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id,
            price: price(at),
            qty: qty(size),
            aggressor,
        })
    }

    fn buy(millis: i64, trade_id: u64, at: &str, size: &str) -> MarketEvent {
        trade(millis, trade_id, at, size, Aggressor::Buy)
    }

    fn warming<T>(observed: u64, required: u64) -> FeatureValue<T> {
        FeatureValue::WarmingUp { observed, required }
    }

    fn unavailable<T>(reason: Unavailability) -> FeatureValue<T> {
        FeatureValue::Unavailable { reason }
    }

    /// The engine's volume profiles after each event.
    fn profiles(events: &[MarketEvent]) -> Vec<VolumeProfiles> {
        let mut engine = MarketStateEngine::new();
        events
            .iter()
            .map(|event| {
                engine.apply(event).unwrap();
                engine.state().profile.clone()
            })
            .collect()
    }

    fn ready(value: &FeatureValue<VolumeProfile>) -> &VolumeProfile {
        value.ready().unwrap()
    }

    /// The levels of `volumes` from bin 6 200 (62 000 USDT).
    fn levels_of(volumes: &[i64]) -> Levels {
        levels(6_200, &hist(volumes)).unwrap().unwrap()
    }

    /// `(index, prominence, permille)` of every candidate.
    fn summary(candidates: &[Candidate]) -> Vec<(usize, i128, u32)> {
        candidates
            .iter()
            .map(|candidate| (candidate.index, candidate.prominence, candidate.permille))
            .collect()
    }

    fn node_indices(volumes: &[i64]) -> (Vec<usize>, Vec<usize>) {
        let (peaks, valleys) = nodes(&smooth(&hist(volumes)));
        let indices = |candidates: Vec<Candidate>| {
            candidates
                .iter()
                .map(|candidate| candidate.index)
                .collect::<Vec<_>>()
        };
        (indices(peaks), indices(valleys))
    }

    #[test]
    fn poc_prefers_volume_then_the_centre_then_the_lower_bin() {
        assert_eq!(poc(&hist(&[7])), 0);
        assert_eq!(poc(&hist(&[1, 5, 2])), 1);
        // Two equal maxima: bin 3 is nearer the centre (bin 2) than bin 0.
        assert_eq!(poc(&hist(&[5, 1, 1, 5, 1])), 3);
        assert_eq!(poc(&hist(&[5, 1, 1, 1, 1, 1, 5, 1])), 6);
        // Equidistant from the centre: the lower bin wins.
        assert_eq!(poc(&hist(&[5, 1, 5])), 0);
        assert_eq!(poc(&hist(&[1, 5, 1, 5, 1])), 1);
        // An even count has its centre between two bins.
        assert_eq!(poc(&hist(&[1, 5, 5, 1])), 1);
        assert_eq!(centre_distance(1, 4), 1);
        assert_eq!(centre_distance(2, 4), 1);
    }

    #[test]
    fn value_area_expands_one_bin_at_a_time() {
        let area = |volumes: &[i64]| {
            let bins = hist(volumes);
            let total = volumes.iter().map(|volume| i128::from(*volume)).sum();
            value_area(&bins, poc(&bins), total)
        };
        // Symmetric and unimodal; the tie at 4 / 4 adds both bins at once.
        assert_eq!(area(&[1, 2, 4, 8, 4, 2, 1]), (2, 4, 16));
        // The larger neighbour first.
        assert_eq!(area(&[1, 3, 8, 4, 1]), (2, 3, 12));
        // Nothing below the POC: the other side alone.
        assert_eq!(area(&[10, 5, 3, 1]), (0, 1, 15));
        assert_eq!(area(&[1, 3, 5, 10]), (2, 3, 15));
        // Exactly 70 % stops; one unit less takes another step.
        assert_eq!(area(&[5, 25, 40, 30]), (2, 3, 70));
        assert_eq!(area(&[6, 25, 40, 29]), (1, 3, 94));
        // Zero-volume interior bins belong to the area like any other.
        assert_eq!(area(&[5, 0, 10, 0, 5]), (0, 4, 20));
        // A single bin is its own value area.
        assert_eq!(area(&[7]), (0, 0, 7));
    }

    #[test]
    fn a_single_bin_profile_spans_its_bin() {
        let levels = levels_of(&[700_000_000]);
        assert_eq!(levels.total_volume, qty("7"));
        assert_eq!((levels.low, levels.high), (price("62000"), price("62010")));
        assert_eq!((levels.poc, levels.poc_volume), (price("62005"), qty("7")));
        assert_eq!((levels.val, levels.vah), (price("62000"), price("62010")));
        assert_eq!(levels.value_area_volume, qty("7"));
        assert_eq!(
            levels.hvn,
            [ProfileNode {
                price: price("62005"),
                low: price("62000"),
                high: price("62010"),
                volume: qty("7"),
                prominence_permille: 1000,
            }]
        );
        assert!(levels.lvn.is_empty());
    }

    #[test]
    fn levels_sit_on_bin_midpoints_and_value_on_bin_edges() {
        // Bins 62 000 … 62 060; the POC is bin 3, value bins 2–4.
        let ladder = levels_of(&[1, 2, 4, 8, 4, 2, 1]);
        assert_eq!((ladder.low, ladder.high), (price("62000"), price("62070")));
        assert_eq!(ladder.poc, price("62035"));
        assert_eq!((ladder.val, ladder.vah), (price("62020"), price("62050")));
        assert_eq!(ladder.value_area_volume, Qty::from_units(16));
        // Negative prices bin by floor division.
        let below_zero = levels(-1, &hist(&[3])).unwrap().unwrap();
        assert_eq!(
            (below_zero.low, below_zero.poc, below_zero.high),
            (price("-10"), price("-5"), price("0"))
        );
        // A bin whose upper edge leaves the `Price` range has no levels.
        assert_eq!(levels(i64::MAX / BIN_UNITS, &hist(&[1])), Ok(None));
        assert!(
            levels(i64::MAX / BIN_UNITS - 1, &hist(&[1]))
                .unwrap()
                .is_some()
        );
        // A total beyond `i64` overflows.
        assert_eq!(
            levels(0, &hist(&[i64::MAX, 1])),
            Err(ProfileError::Overflow)
        );
    }

    #[test]
    fn smoothing_uses_the_triangular_kernel() {
        assert_eq!(smooth(&hist(&[7])), [21]);
        assert_eq!(smooth(&hist(&[1, 0, 0, 0, 0])), [3, 2, 1, 0, 0]);
        assert_eq!(smooth(&hist(&[0, 0, 4, 0, 0])), [4, 8, 12, 8, 4]);
        assert_eq!(
            smooth(&hist(&[5, 5, 5, 5, 5, 5, 5])),
            [30, 40, 45, 45, 45, 40, 30]
        );
    }

    #[test]
    fn nodes_meet_the_prominence_threshold_exactly() {
        // Peak 3 has prominence 10 = 10 % of 100 (its base is the valley at
        // 2), and so has the valley: both are in.
        let (peaks, valleys) = nodes(&[10, 100, 10, 20, 10]);
        assert_eq!(summary(&peaks), [(1, 100, 1000), (3, 10, 100)]);
        assert_eq!(summary(&valleys), [(2, 10, 100)]);
        // One unit less: both are out.
        let (peaks, valleys) = nodes(&[10, 100, 10, 19, 10]);
        assert_eq!(summary(&peaks), [(1, 100, 1000)]);
        assert!(valleys.is_empty());
        // A valley's prominence is the lower of its two tops above it.
        let (_, valleys) = nodes(&[100, 90, 100, 50, 100]);
        assert_eq!(summary(&valleys), [(1, 10, 100), (3, 50, 500)]);
        let (_, valleys) = nodes(&[100, 91, 100, 50, 100]);
        assert_eq!(summary(&valleys), [(3, 50, 500)]);
        // The permille rounds down.
        let (peaks, _) = nodes(&[0, 300, 0, 0, 0, 101, 0]);
        assert_eq!(summary(&peaks), [(1, 300, 1000), (5, 101, 336)]);
    }

    #[test]
    fn plateaus_sit_on_their_lower_middle_bin() {
        // Smoothed: 30 40 45 45 45 45 45 40 30 — a plateau of five.
        assert_eq!(node_indices(&[5; 9]), (vec![4], vec![]));
        // A plateau of six, bins 2–7: the lower of the two middle bins.
        assert_eq!(node_indices(&[5; 10]), (vec![4], vec![]));
        // A valley of five zero bins (7–11) between two clusters…
        let mut odd = vec![9; 5];
        odd.extend([0; 9]);
        odd.extend([9; 5]);
        assert_eq!(node_indices(&odd), (vec![2, 16], vec![9]));
        // …and of six (7–12).
        let mut even = vec![9; 5];
        even.extend([0; 10]);
        even.extend([9; 5]);
        assert_eq!(node_indices(&even), (vec![2, 17], vec![9]));
    }

    #[test]
    fn edge_peaks_are_allowed_edge_valleys_never() {
        // Smoothed: 30 24 17 9 9 8 6 — falling from the lower edge.
        assert_eq!(
            smooth(&hist(&[9, 1, 1, 1, 1, 1, 1])),
            [30, 24, 17, 9, 9, 8, 6]
        );
        assert_eq!(node_indices(&[9, 1, 1, 1, 1, 1, 1]), (vec![0], vec![]));
        assert_eq!(node_indices(&[1, 1, 1, 1, 1, 1, 9]), (vec![6], vec![]));
        // The thin tails are never valleys, however low.
        assert_eq!(node_indices(&[1, 9, 9, 9, 9, 9, 1]).1, Vec::<usize>::new());
    }

    #[test]
    fn multimodal_profiles_find_every_node() {
        // Bimodal with a zero-volume gap: the gap is the LVN.
        let bimodal = [1, 4, 9, 4, 1, 0, 0, 0, 1, 4, 9, 4, 1];
        assert_eq!(node_indices(&bimodal), (vec![2, 10], vec![6]));
        let levels = levels_of(&bimodal);
        assert_eq!(
            levels.hvn.iter().map(|node| node.price).collect::<Vec<_>>(),
            [price("62025"), price("62105")]
        );
        assert_eq!(
            levels.lvn,
            [ProfileNode {
                price: price("62065"),
                low: price("62060"),
                high: price("62070"),
                volume: Qty::from_units(0),
                prominence_permille: 955,
            }]
        );
        // Trimodal: three HVNs, two LVNs on the lower bin of each two-bin
        // valley plateau.
        let trimodal = [1, 4, 9, 4, 1, 0, 0, 1, 4, 9, 4, 1, 0, 0, 1, 4, 9, 4, 1];
        assert_eq!(node_indices(&trimodal), (vec![2, 9, 16], vec![5, 12]));
        // A minor bump below 10 % prominence is not a node.
        let bump = [100, 400, 900, 400, 100, 0, 3, 0, 100, 400, 900, 400, 100];
        assert_eq!(node_indices(&bump), (vec![2, 10], vec![6]));
    }

    #[test]
    fn the_global_maximum_is_always_an_hvn() {
        for volumes in [
            vec![1, 1, 1, 50, 1, 1, 1],
            vec![50, 1, 1, 1],
            vec![1, 2, 3, 4, 5, 6],
            vec![3, 3, 3],
        ] {
            let bins = hist(&volumes);
            let smoothed = smooth(&bins);
            let max = smoothed.iter().copied().max().unwrap();
            let (peaks, _) = nodes(&smoothed);
            assert!(
                peaks
                    .iter()
                    .any(|peak| smoothed[peak.index] == max && peak.permille == 1000),
                "{volumes:?}"
            );
        }
    }

    /// `(index, prominence)` of each node found.
    type NodeList = Vec<(usize, i128)>;

    /// Every peak and valley of `s` with its prominence, from the written
    /// definition (decision 6): the oracle for [`nodes`].
    fn reference_nodes(s: &[i128]) -> (NodeList, NodeList) {
        let n = s.len();
        let (mut peaks, mut valleys) = (Vec::new(), Vec::new());
        for a in 0..n {
            if a > 0 && s[a - 1] == s[a] {
                continue;
            }
            let level = s[a];
            let b = a + s[a..].iter().take_while(|&&value| value == level).count() - 1;
            let position = a + (b - a) / 2;
            let left = (a > 0).then(|| s[a - 1]);
            let right = (b + 1 < n).then(|| s[b + 1]);
            if left.is_none_or(|value| value < level) && right.is_none_or(|value| value < level) {
                let left_base = match s[..a].iter().rposition(|&value| value > level) {
                    Some(higher) => *s[higher + 1..a].iter().min().unwrap(),
                    None => 0,
                };
                let right_base = match s[b + 1..].iter().position(|&value| value > level) {
                    Some(higher) => *s[b + 1..b + 1 + higher].iter().min().unwrap(),
                    None => 0,
                };
                peaks.push((position, level - left_base.max(right_base)));
            }
            if left.is_some_and(|value| value > level) && right.is_some_and(|value| value > level) {
                let from = s[..a]
                    .iter()
                    .rposition(|&value| value < level)
                    .map_or(0, |lower| lower + 1);
                let to = s[b + 1..]
                    .iter()
                    .position(|&value| value < level)
                    .map_or(n, |lower| b + 1 + lower);
                let left_top = *s[from..a].iter().max().unwrap();
                let right_top = *s[b + 1..to].iter().max().unwrap();
                valleys.push((position, left_top.min(right_top) - level));
            }
        }
        (peaks, valleys)
    }

    /// The value area from the written rule (decision 5), as the volume
    /// after each expansion step: the oracle for [`value_area`].
    fn reference_value_area(bins: &[i64], poc: usize) -> (usize, usize, Vec<i128>) {
        let mut below: Vec<i64> = bins[..poc].to_vec();
        let mut above: Vec<i64> = bins[poc + 1..].iter().rev().copied().collect();
        let total: i128 = bins.iter().map(|&volume| i128::from(volume)).sum();
        let mut steps = vec![i128::from(bins[poc])];
        let (mut lo, mut hi) = (poc, poc);
        while *steps.last().unwrap() * 100 < total * 70 {
            let mut added = 0;
            match (below.last().copied(), above.last().copied()) {
                (Some(b), Some(a)) if a == b => {
                    added += i128::from(below.pop().unwrap()) + i128::from(above.pop().unwrap());
                    lo -= 1;
                    hi += 1;
                }
                (Some(b), Some(a)) if b > a => {
                    added += i128::from(below.pop().unwrap());
                    lo -= 1;
                }
                (Some(_), None) => {
                    added += i128::from(below.pop().unwrap());
                    lo -= 1;
                }
                (_, Some(_)) => {
                    added += i128::from(above.pop().unwrap());
                    hi += 1;
                }
                (None, None) => unreachable!("the whole range holds the total"),
            }
            steps.push(steps.last().unwrap() + added);
        }
        (lo, hi, steps)
    }

    /// A random dense histogram with volume in its first and last bins:
    /// small values half the time, so ties and plateaus are common.
    fn random_histogram(lcg: &mut Lcg) -> Vec<i64> {
        let len = 1 + lcg.below(48) as usize;
        let small = lcg.below(2) == 0;
        let mut volumes: Vec<i64> = (0..len)
            .map(|_| match (small, lcg.below(5)) {
                (_, 0) => 0,
                (true, _) => lcg.below(4),
                (false, _) => 1 + lcg.below(1_000_000_000),
            })
            .collect();
        for edge in [0, len - 1] {
            if volumes[edge] == 0 {
                volumes[edge] = 1 + lcg.below(3);
            }
        }
        volumes
    }

    #[test]
    fn random_histograms_match_the_written_definitions() {
        let mut lcg = Lcg(0x6d69_6500_0000_0036);
        let (mut peak_count, mut valley_count, mut ties) = (0, 0, 0);
        for _ in 0..2_000 {
            let volumes = random_histogram(&mut lcg);
            let bins = hist(&volumes);
            let total: i128 = volumes.iter().map(|&volume| i128::from(volume)).sum();
            let low_bin = 6_000 + lcg.below(1_000);

            // Nodes against the oracle, threshold applied to both.
            let smoothed = smooth(&bins);
            let max = *smoothed.iter().max().unwrap();
            let (peaks, valleys) = nodes(&smoothed);
            let (reference_peaks, reference_valleys) = reference_nodes(&smoothed);
            let kept = |candidates: Vec<(usize, i128)>| {
                candidates
                    .into_iter()
                    .filter(|(_, prominence)| prominence * 100 >= max * 10)
                    .map(|(index, prominence)| {
                        (
                            index,
                            prominence,
                            u32::try_from(prominence * 1000 / max).unwrap(),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(summary(&peaks), kept(reference_peaks), "{volumes:?}");
            assert_eq!(summary(&valleys), kept(reference_valleys), "{volumes:?}");
            assert!(
                peaks
                    .iter()
                    .any(|peak| smoothed[peak.index] == max && peak.permille == 1000),
                "the global maximum is an HVN: {volumes:?}"
            );
            peak_count += peaks.len();
            valley_count += valleys.len();

            // Value area against the oracle and its invariants.
            let poc_index = poc(&bins);
            let (lo, hi, volume) = value_area(&bins, poc_index, total);
            let (reference_lo, reference_hi, steps) = reference_value_area(&volumes, poc_index);
            assert_eq!((lo, hi), (reference_lo, reference_hi), "{volumes:?}");
            assert_eq!(volume, *steps.last().unwrap());
            assert!(lo <= poc_index && poc_index <= hi);
            assert_eq!(
                volume,
                volumes[lo..=hi]
                    .iter()
                    .map(|&v| i128::from(v))
                    .sum::<i128>()
            );
            assert!(volume * 100 >= total * 70, "{volumes:?}");
            if let [.., before, _] = steps.as_slice() {
                assert!(before * 100 < total * 70, "{volumes:?}");
            }
            // The POC holds the most volume.
            assert!(volumes.iter().all(|&v| v <= volumes[poc_index]));
            if volumes.iter().filter(|&&v| v == volumes[poc_index]).count() > 1 {
                ties += 1;
            }

            // Prices: VAL ≤ POC < VAH, and every node inside the range.
            let ladder = levels(low_bin, &bins).unwrap().unwrap();
            assert!(ladder.low <= ladder.val && ladder.val <= ladder.poc);
            assert!(ladder.poc < ladder.vah && ladder.vah <= ladder.high);
            assert_eq!(ladder.total_volume.units(), i64::try_from(total).unwrap());
            for node in ladder.hvn.iter().chain(&ladder.lvn) {
                assert!(ladder.low <= node.low && node.high <= ladder.high);
                assert!(node.low < node.price && node.price < node.high);
                assert_eq!(node.high.units() - node.low.units(), BIN_UNITS);
            }
            assert!(
                ladder
                    .hvn
                    .windows(2)
                    .all(|pair| pair[0].price < pair[1].price)
            );
            assert!(
                ladder
                    .lvn
                    .windows(2)
                    .all(|pair| pair[0].price < pair[1].price)
            );
        }
        assert!(
            peak_count > 2_000 && valley_count > 500,
            "{peak_count} {valley_count}"
        );
        assert!(ties > 100, "{ties}");
    }

    #[test]
    fn levels_hand_over_poc_value_edges_and_nodes() {
        let levels = levels_of(&[1, 4, 9, 4, 1, 0, 0, 0, 1, 4, 9, 4, 1]);
        let profile = VolumeProfile {
            feature: catalog::PROFILE_VOLUME_PRIOR_DAY_V1.key,
            start: t(0),
            end: t(DAY),
            sessions: 1,
            total_volume: levels.total_volume,
            low: levels.low,
            high: levels.high,
            poc: levels.poc,
            poc_volume: levels.poc_volume,
            val: levels.val,
            vah: levels.vah,
            value_area_volume: levels.value_area_volume,
            hvn: levels.hvn,
            lvn: levels.lvn,
            coverage: COMPLETE,
        };
        let handed: Vec<(LevelKind, Price, Price, Price)> = profile
            .levels()
            .map(|level| {
                assert_eq!(level.source, catalog::PROFILE_VOLUME_PRIOR_DAY_V1.key);
                (level.kind, level.price, level.low, level.high)
            })
            .collect();
        // Two equal maxima (bins 2 and 10) are equidistant from the centre:
        // the lower one is the POC. Value: bins 0–10.
        assert_eq!(
            handed,
            [
                (
                    LevelKind::Poc,
                    price("62025"),
                    price("62020"),
                    price("62030")
                ),
                (
                    LevelKind::Val,
                    price("62000"),
                    price("62000"),
                    price("62010")
                ),
                (
                    LevelKind::Vah,
                    price("62110"),
                    price("62100"),
                    price("62110")
                ),
                (
                    LevelKind::Hvn,
                    price("62025"),
                    price("62020"),
                    price("62030")
                ),
                (
                    LevelKind::Hvn,
                    price("62105"),
                    price("62100"),
                    price("62110")
                ),
                (
                    LevelKind::Lvn,
                    price("62065"),
                    price("62060"),
                    price("62070")
                ),
            ]
        );
        assert_eq!(
            profile.to_string(),
            "start=0ms end=86400000ms sessions=1 vol=0.00000038 low=62000.00000000 \
             high=62130.00000000 poc=62025.00000000 poc_vol=0.00000009 \
             val=62000.00000000 vah=62110.00000000 va_vol=0.00000033 \
             hvn=62025.00000000:1000,62105.00000000:1000 lvn=62065.00000000:955 \
             complete profile.volume.prior_day@1"
        );
        let empty_nodes = VolumeProfile {
            hvn: Vec::new(),
            lvn: Vec::new(),
            ..profile
        };
        assert!(empty_nodes.to_string().contains(" hvn=- lvn=- complete "));
    }

    #[test]
    fn constants_match_the_catalog() {
        let param = |definition: &FeatureDefinition, name: &str| {
            definition
                .params
                .iter()
                .find(|param| param.name == name)
                .map(|param| param.value)
        };
        let all = [
            &catalog::PROFILE_VOLUME_UTC_DAY_V1,
            &catalog::PROFILE_VOLUME_PRIOR_DAY_V1,
            &catalog::PROFILE_VOLUME_COMPOSITE_5D_V1,
        ];
        for definition in all {
            assert_eq!(
                param(definition, "bin_size"),
                Some(ParamValue::Price(BIN_SIZE))
            );
            assert_eq!(
                param(definition, "max_bins"),
                Some(ParamValue::Int(i64::try_from(MAX_BINS).unwrap()))
            );
            assert_eq!(
                param(definition, "value_area_pct"),
                Some(ParamValue::Int(VALUE_AREA_PCT))
            );
            assert_eq!(
                param(definition, "node_prominence_pct"),
                Some(ParamValue::Int(NODE_PROMINENCE_PCT))
            );
            assert_eq!(
                param(definition, "node_smoothing"),
                Some(ParamValue::Text("triangular_5"))
            );
            assert_eq!(
                param(definition, "session_ms"),
                Some(ParamValue::Int(Timeframe::D1.millis()))
            );
            assert!(catalog::CURRENT.contains(&definition.key));
        }
        assert_eq!(NODE_KERNEL, [1, 2, 3, 2, 1]);
        assert_eq!(BIN_SIZE, price("10"));
        assert_eq!(param(all[0], "sessions"), None);
        assert_eq!(param(all[1], "sessions"), Some(ParamValue::Int(1)));
        assert_eq!(
            param(all[2], "sessions"),
            Some(ParamValue::Int(i64::try_from(COMPOSITE_SESSIONS).unwrap()))
        );
        assert_eq!(
            all.map(|definition| definition.warm_up),
            [
                WarmUp::Samples(1),
                WarmUp::Samples(1),
                WarmUp::Samples(u32::try_from(COMPOSITE_SESSIONS).unwrap()),
            ]
        );
        assert_eq!(
            VolumeProfiles::new().composite_5d,
            warming(0, COMPOSITE_SESSIONS as u64)
        );
    }

    #[test]
    fn the_developing_profile_steps_on_closed_minutes_only() {
        let events = [
            buy(1_000, 1, "62000", "1"),
            buy(2_000, 2, "62015", "0.5"),
            // Closes minute 0; this trade is in the developing minute.
            buy(61_000, 3, "62100", "2"),
            buy(62_000, 4, "62100", "1"),
            mark(63_000, 1),
            // Closes minute 1.
            buy(121_000, 5, "62000", "1"),
        ];
        let profiles = profiles(&events);
        assert_eq!(profiles[0].utc_day, warming(0, 1));
        assert_eq!(profiles[1].utc_day, warming(0, 1));
        let first = ready(&profiles[2].utc_day);
        assert_eq!(
            first.to_string(),
            "start=0ms end=60000ms sessions=1 vol=1.50000000 low=62000.00000000 \
             high=62020.00000000 poc=62005.00000000 poc_vol=1.00000000 \
             val=62000.00000000 vah=62020.00000000 va_vol=1.50000000 \
             hvn=62005.00000000:1000 lvn=- partial_start profile.volume.utc_day@1"
        );
        // Trades and other streams inside a minute change nothing.
        assert_eq!(profiles[3], profiles[2]);
        assert_eq!(profiles[4], profiles[2]);
        let second = ready(&profiles[5].utc_day);
        assert_eq!(
            (
                second.end,
                second.total_volume,
                second.poc,
                second.poc_volume
            ),
            (t(120_000), qty("4.5"), price("62105"), qty("3"))
        );
        assert_eq!((second.low, second.high), (price("62000"), price("62110")));
        // Minute 0 was partial; the OR keeps the flag.
        assert_eq!(second.coverage, PARTIAL);
        // Nothing completed yet.
        for profile in &profiles {
            assert_eq!(profile.prior_day, warming(0, 1));
            assert_eq!(profile.composite_5d, warming(0, 5));
        }
    }

    #[test]
    fn the_developing_profile_restarts_at_utc_midnight() {
        let events = [
            buy(DAY - 3 * MINUTE, 1, "62000", "1"),
            buy(DAY - MINUTE, 2, "62050", "2"),
            // Closes the day: the new day has no closed minute yet.
            buy(DAY + 1_000, 3, "63000", "0.5"),
            buy(DAY + MINUTE + 1_000, 4, "63010", "0.25"),
        ];
        let profiles = profiles(&events);
        assert!(profiles[1].utc_day.is_ready());
        assert_eq!(profiles[2].utc_day, warming(0, 1));
        let prior = ready(&profiles[2].prior_day);
        assert_eq!((prior.start, prior.end, prior.sessions), (t(0), t(DAY), 1));
        assert_eq!((prior.total_volume, prior.coverage), (qty("3"), PARTIAL));
        assert_eq!(prior.poc, price("62055"));
        assert_eq!(profiles[2].composite_5d, warming(1, 5));
        // The first closed minute of the new day, complete.
        let day = ready(&profiles[3].utc_day);
        assert_eq!(
            (day.start, day.end, day.total_volume, day.coverage),
            (t(DAY), t(DAY + MINUTE), qty("0.5"), COMPLETE)
        );
        assert_eq!(profiles[3].prior_day, profiles[2].prior_day);
    }

    #[test]
    fn the_composite_warms_up_over_five_days_then_slides() {
        let events: Vec<MarketEvent> = (0..=6)
            .map(|day| {
                let at = format!("{}", 62_000 + 100 * day);
                let trade_id = u64::try_from(day).unwrap() + 1;
                buy(day * DAY + 1_000, trade_id, &at, "1")
            })
            .collect();
        let profiles = profiles(&events);
        assert_eq!(profiles[0].composite_5d, warming(0, 5));
        for (day, profile) in (1..).zip(&profiles[1..5]) {
            assert_eq!(profile.composite_5d, warming(day, 5));
            let prior = ready(&profile.prior_day);
            assert_eq!(prior.total_volume, qty("1"));
        }
        let first = ready(&profiles[5].composite_5d);
        assert_eq!(
            (first.start, first.end, first.sessions, first.total_volume),
            (t(0), t(5 * DAY), 5, qty("5"))
        );
        assert_eq!((first.low, first.high), (price("62000"), price("62410")));
        // Day 0 was partial.
        assert_eq!(first.coverage, PARTIAL);
        // Five equal bins: the POC is the centre one.
        assert_eq!(first.poc, price("62205"));
        let second = ready(&profiles[6].composite_5d);
        assert_eq!(
            (
                second.start,
                second.end,
                second.total_volume,
                second.coverage
            ),
            (t(DAY), t(6 * DAY), qty("5"), COMPLETE)
        );
        assert_eq!((second.low, second.high), (price("62100"), price("62510")));
    }

    #[test]
    fn a_trades_gap_flags_the_coverage_without_a_reset() {
        let events = [
            buy(DAY + 1_000, 1, "62000", "1"),
            buy(DAY + DAY - 1, 2, "62000", "1"),
            // Day 1 opens complete.
            buy(2 * DAY + 1_000, 3, "62000", "1"),
            buy(2 * DAY + MINUTE + 1_000, 4, "62010", "1"),
            gap(
                Stream::Trades,
                2 * DAY + MINUTE + 2_000,
                2 * DAY + MINUTE + 3_000,
                GapReason::Disconnected,
            ),
            buy(2 * DAY + 2 * MINUTE + 1_000, 5, "62020", "1"),
            buy(2 * DAY + 3 * MINUTE + 1_000, 6, "62030", "1"),
        ];
        let profiles = profiles(&events);
        let before = ready(&profiles[3].utc_day);
        assert_eq!((before.total_volume, before.coverage), (qty("1"), COMPLETE));
        // The gap closes no minute.
        assert_eq!(profiles[4], profiles[3]);
        let gapped = ready(&profiles[5].utc_day);
        assert_eq!((gapped.total_volume, gapped.coverage), (qty("2"), GAP));
        // Never reset: the volume keeps growing and the flag stays.
        let after = ready(&profiles[6].utc_day);
        assert_eq!((after.total_volume, after.coverage), (qty("3"), GAP));
        assert_eq!(after.start, t(2 * DAY));
    }

    #[test]
    fn one_jump_closes_several_days() {
        // Days 0–5 trade; day 6 is empty, closed by the trade on day 7
        // together with day 5.
        let mut events: Vec<MarketEvent> = (0..=5)
            .map(|day| {
                buy(
                    day * DAY + 1_000,
                    u64::try_from(day).unwrap() + 1,
                    "62000",
                    "1",
                )
            })
            .collect();
        events.push(buy(7 * DAY + 1_000, 7, "62000", "1"));
        let profiles = profiles(&events);
        let last = &profiles[6];
        assert_eq!(last.utc_day, warming(0, 1));
        // The prior day is the empty day 6.
        assert_eq!(last.prior_day, unavailable(Unavailability::InputInvalid));
        // The composite holds days 2–6.
        let composite = ready(&last.composite_5d);
        assert_eq!(
            (composite.start, composite.end, composite.total_volume),
            (t(2 * DAY), t(7 * DAY), qty("4"))
        );
        assert_eq!(composite.coverage, COMPLETE);

        // A single jump over three days: two of them empty.
        let profiles = profiles_of_jump();
        assert_eq!(
            profiles.prior_day,
            unavailable(Unavailability::InputInvalid)
        );
        assert_eq!(profiles.composite_5d, warming(3, 5));
        assert_eq!(profiles.utc_day, warming(0, 1));
    }

    fn profiles_of_jump() -> VolumeProfiles {
        profiles(&[
            buy(1_000, 1, "62000", "1"),
            buy(3 * DAY + 1_000, 2, "62000", "1"),
        ])
        .pop()
        .unwrap()
    }

    #[test]
    fn five_empty_days_leave_the_composite_unavailable() {
        // A gap opens the run on day 0; its days never trade until day 5.
        let profiles = profiles(&[
            gap(Stream::Trades, 0, 1_000, GapReason::Disconnected),
            buy(5 * DAY + 1_000, 1, "62000", "1"),
        ]);
        let last = &profiles[1];
        assert_eq!(last.prior_day, unavailable(Unavailability::InputInvalid));
        assert_eq!(last.composite_5d, unavailable(Unavailability::InputInvalid));
    }

    #[test]
    fn a_range_beyond_max_bins_is_out_of_range() {
        let developing = |high: &str| {
            profiles(&[
                buy(1_000, 1, "60000", "1"),
                buy(2_000, 2, high, "1"),
                buy(61_000, 3, "60000", "1"),
            ])
            .pop()
            .unwrap()
            .utc_day
        };
        // Bins 6 000 … 15 999: exactly the bound.
        let widest = developing("159999.99999999");
        assert_eq!(ready(&widest).high, price("160000"));
        // Bins 6 000 … 16 000: one bin too many. The event is accepted.
        assert_eq!(
            developing("160000"),
            unavailable(Unavailability::OutOfRange)
        );
        // A bin whose upper edge leaves the `Price` range.
        let edge = Price::from_units(i64::MAX).to_string();
        let mut engine = MarketStateEngine::new();
        engine.apply(&buy(1_000, 1, &edge, "1")).unwrap();
        engine.apply(&buy(61_000, 2, &edge, "1")).unwrap();
        assert_eq!(
            engine.state().profile.utc_day,
            unavailable(Unavailability::OutOfRange)
        );
    }

    #[test]
    fn a_failed_step_leaves_the_tracker_alone() {
        let tracker = ProfileTracker {
            developing: BTreeMap::from([(6_200, Qty::from_units(i64::MAX))]),
            ..ProfileTracker::new()
        };
        let before = tracker.clone();
        let overflow = buy(1_000, 1, "62000", "0.00000001");
        assert!(matches!(
            tracker.step(&overflow, &[]),
            Err(ProfileError::Overflow)
        ));
        assert_eq!(tracker, before);
        // The same trade in another bin fits.
        let elsewhere = buy(1_000, 1, "62010", "0.00000001");
        let mut committed = tracker.clone();
        let mut values = VolumeProfiles::new();
        committed.commit(tracker.step(&elsewhere, &[]).unwrap(), &mut values);
        assert_eq!(committed.developing.len(), 2);
        assert_eq!(values, VolumeProfiles::new());
        // Zero quantities add nothing.
        let zero = buy(1_000, 1, "62010", "0");
        assert!(tracker.step(&zero, &[]).unwrap().insert.is_none());
    }

    #[test]
    fn profiles_fold_the_closed_bars_on_random_tapes() {
        for seed in [21, 22, 23] {
            let tape = random_tape(seed, 6_000);
            let mut engine = MarketStateEngine::new();
            let (mut day_volume, mut day_coverage) = (0_i128, COMPLETE);
            let mut last_minute: Option<Bar> = None;
            let (mut checked, mut days) = (0, 0);
            for event in &tape {
                engine.apply(event).unwrap();
                let state = engine.state();
                let closed = engine.closed_bars();
                for bar in closed {
                    match bar.timeframe {
                        Timeframe::M1 => {
                            day_volume += i128::from(bar.volume.units());
                            day_coverage.partial_start |= bar.coverage.partial_start;
                            day_coverage.feed_gap |= bar.coverage.feed_gap;
                            last_minute = Some(*bar);
                        }
                        Timeframe::D1 => {
                            day_volume = 0;
                            day_coverage = COMPLETE;
                        }
                        _ => {}
                    }
                }
                if !closed.iter().any(|bar| bar.timeframe == Timeframe::M1) {
                    continue;
                }
                match &state.profile.utc_day {
                    FeatureValue::Ready(profile) => {
                        let minute = last_minute.unwrap();
                        assert_eq!(i128::from(profile.total_volume.units()), day_volume);
                        assert_eq!(profile.end, minute.end(), "seed {seed}");
                        assert_eq!(
                            profile.start,
                            Timeframe::D1.open_of(minute.open_time).unwrap()
                        );
                        assert_eq!(profile.coverage, day_coverage, "seed {seed}");
                        checked += 1;
                    }
                    other => {
                        assert_eq!(*other, warming(0, 1), "seed {seed}");
                        assert_eq!(day_volume, 0, "seed {seed}");
                    }
                }
                let Some(day) = closed.iter().rfind(|bar| bar.timeframe == Timeframe::D1) else {
                    continue;
                };
                days += 1;
                match &state.profile.prior_day {
                    FeatureValue::Ready(prior) => {
                        assert_eq!(prior.total_volume, day.volume, "seed {seed}");
                        assert_eq!(
                            (prior.start, prior.end, prior.coverage),
                            (day.open_time, day.end(), day.coverage)
                        );
                    }
                    other => {
                        assert_eq!(*other, unavailable(Unavailability::InputInvalid));
                        assert_eq!(day.volume, Qty::from_units(0), "seed {seed}");
                    }
                }
            }
            assert!(checked > 1_000, "seed {seed}: {checked}");
            assert!(days >= 5, "seed {seed}: {days}");
            assert!(engine.state().profile.composite_5d.is_ready() || days < 5);
        }
    }

    /// The trades gap of the golden tape: 10:00 to 10:45 UTC on day 3.
    const GAP_START: i64 = 3 * DAY + 10 * 3_600_000;
    const GAP_END: i64 = GAP_START + 45 * MINUTE;

    /// The volume-profile golden tape: 13:00 UTC on day 0 (a partial day)
    /// to just after 00:00 UTC on day 7. Trades from an LCG about three
    /// price clusters that drift from day to day, with a quiet day-5
    /// afternoon, and a trades gap on day 3.
    fn golden_tape() -> Vec<MarketEvent> {
        let mut lcg = Lcg(0x6d69_6500_0000_0036);
        let mut events = Vec::new();
        let mut time = 13 * 3_600_000;
        let mut trade_id = 0;
        let mut gapped = false;
        while time < 7 * DAY {
            time += 20_000 + lcg.below(140_000);
            if !gapped && time >= GAP_START {
                events.push(gap(
                    Stream::Trades,
                    GAP_START,
                    GAP_END,
                    GapReason::Disconnected,
                ));
                time = GAP_END + 1 + lcg.below(30_000);
                gapped = true;
            }
            let day = time / DAY;
            if day == 5 && time % DAY > 12 * 3_600_000 && lcg.below(4) != 0 {
                continue;
            }
            // Cluster centres in whole USDT, drifting by day.
            let base = 62_000 + 150 * day - 40 * (day % 3);
            let centre = match lcg.below(10) {
                0..=4 => base,
                5..=7 => base + 420,
                _ => base + 1_130 - 60 * (day % 2),
            };
            // About ±90 USDT around the centre, to the cent.
            let spread = lcg.below(6_001) + lcg.below(6_001) + lcg.below(6_001) - 9_000;
            let price = centre * SCALE + spread * (SCALE / 100);
            let aggressor = if lcg.below(2) == 0 {
                Aggressor::Buy
            } else {
                Aggressor::Sell
            };
            trade_id += 1;
            events.push(MarketEvent::Trade(Trade {
                time: t(time),
                trade_id,
                price: Price::from_units(price),
                qty: Qty::from_units(1 + lcg.below(200_000_000)),
                aggressor,
            }));
        }
        events
    }

    /// One line per event of the golden tape that closed a bar of
    /// `timeframe`, from the ready values of `pick`: the event time, then
    /// the value.
    fn golden_lines(
        timeframe: Timeframe,
        pick: impl Fn(&VolumeProfiles) -> &FeatureValue<VolumeProfile>,
    ) -> Vec<String> {
        let mut engine = MarketStateEngine::new();
        let mut lines = Vec::new();
        for event in golden_tape() {
            engine.apply(&event).unwrap();
            let closed = engine
                .closed_bars()
                .iter()
                .any(|bar| bar.timeframe == timeframe);
            if let (true, FeatureValue::Ready(value)) = (closed, pick(&engine.state().profile)) {
                lines.push(format!("{} {value}", event.time()));
            }
        }
        lines
    }

    #[test]
    fn golden_profile_volume_utc_day_v1() {
        let lines = golden_lines(Timeframe::H4, |profiles| &profiles.utc_day);
        assert_eq!(lines, GOLDEN_UTC_DAY);
    }

    #[test]
    fn golden_profile_volume_prior_day_v1() {
        let lines = golden_lines(Timeframe::D1, |profiles| &profiles.prior_day);
        assert_eq!(lines, GOLDEN_PRIOR_DAY);
    }

    #[test]
    fn golden_profile_volume_composite_5d_v1() {
        let lines = golden_lines(Timeframe::D1, |profiles| &profiles.composite_5d);
        assert_eq!(lines, GOLDEN_COMPOSITE_5D);
    }

    const GOLDEN_UTC_DAY: [&str; 32] = [
        "57638820ms start=0ms end=57600000ms sessions=1 vol=113.07016974 low=61930.00000000 high=63170.00000000 poc=62435.00000000 poc_vol=11.92107502 val=61970.00000000 vah=62780.00000000 va_vol=79.83911955 hvn=62015.00000000:1000,62435.00000000:906,63115.00000000:542 lvn=62215.00000000:906,62775.00000000:542 partial_start profile.volume.utc_day@1",
        "72074452ms start=0ms end=72060000ms sessions=1 vol=266.43924466 low=61930.00000000 high=63190.00000000 poc=62015.00000000 poc_vol=20.26812056 val=61930.00000000 vah=62440.00000000 va_vol=194.18862541 hvn=62015.00000000:1000,62425.00000000:893,63115.00000000:426 lvn=62215.00000000:893,62775.00000000:426 partial_start profile.volume.utc_day@1",
        "100882089ms start=86400000ms end=100860000ms sessions=1 vol=144.51284566 low=62040.00000000 high=63270.00000000 poc=62125.00000000 poc_vol=11.43701494 val=62040.00000000 vah=62540.00000000 va_vol=102.42732083 hvn=62115.00000000:1000,62535.00000000:571,63175.00000000:355 lvn=62315.00000000:571,62845.00000000:355 complete profile.volume.utc_day@1",
        "115256841ms start=86400000ms end=115200000ms sessions=1 vol=293.74461615 low=62040.00000000 high=63270.00000000 poc=62115.00000000 poc_vol=27.21792370 val=62040.00000000 vah=62540.00000000 va_vol=215.90090831 hvn=62115.00000000:1000,62535.00000000:498,63185.00000000:337 lvn=62315.00000000:498,62855.00000000:337 complete profile.volume.utc_day@1",
        "129614162ms start=86400000ms end=129600000ms sessions=1 vol=442.94329974 low=62040.00000000 high=63270.00000000 poc=62115.00000000 poc_vol=37.24628678 val=62040.00000000 vah=62540.00000000 va_vol=317.81582666 hvn=62115.00000000:1000,62535.00000000:504,63185.00000000:333 lvn=62315.00000000:504,62855.00000000:333 complete profile.volume.utc_day@1",
        "144106472ms start=86400000ms end=144060000ms sessions=1 vol=598.04536959 low=62040.00000000 high=63270.00000000 poc=62115.00000000 poc_vol=47.93464463 val=62040.00000000 vah=62550.00000000 va_vol=436.47697089 hvn=62115.00000000:1000,62535.00000000:536,63185.00000000:329 lvn=62315.00000000:536,62855.00000000:329 complete profile.volume.utc_day@1",
        "158419122ms start=86400000ms end=158400000ms sessions=1 vol=744.47766199 low=62040.00000000 high=63270.00000000 poc=62115.00000000 poc_vol=59.30391344 val=62040.00000000 vah=62550.00000000 va_vol=535.88368580 hvn=62115.00000000:1000,62535.00000000:534,63175.00000000:354 lvn=62315.00000000:534,62855.00000000:354 complete profile.volume.utc_day@1",
        "187329597ms start=172800000ms end=187320000ms sessions=1 vol=143.29521658 low=62150.00000000 high=63410.00000000 poc=62195.00000000 poc_vol=12.60671621 val=62150.00000000 vah=62670.00000000 va_vol=103.30271637 hvn=62225.00000000:1000,62645.00000000:664,63355.00000000:470 lvn=62425.00000000:664,62995.00000000:470 complete profile.volume.utc_day@1",
        "201638647ms start=172800000ms end=201600000ms sessions=1 vol=286.65964155 low=62140.00000000 high=63410.00000000 poc=62235.00000000 poc_vol=18.21126901 val=62140.00000000 vah=62680.00000000 va_vol=210.20756178 hvn=62225.00000000:1000,62645.00000000:690,63345.00000000:514 lvn=62425.00000000:690,62995.00000000:514 complete profile.volume.utc_day@1",
        "216054533ms start=172800000ms end=216000000ms sessions=1 vol=439.32145882 low=62140.00000000 high=63420.00000000 poc=62225.00000000 poc_vol=30.43433911 val=62140.00000000 vah=62670.00000000 va_vol=308.84433890 hvn=62225.00000000:1000,62645.00000000:614,63345.00000000:470 lvn=62425.00000000:614,62995.00000000:470 complete profile.volume.utc_day@1",
        "230485739ms start=172800000ms end=230460000ms sessions=1 vol=594.83422797 low=62140.00000000 high=63420.00000000 poc=62225.00000000 poc_vol=36.88529900 val=62140.00000000 vah=62670.00000000 va_vol=428.54923494 hvn=62225.00000000:1000,62645.00000000:604,63345.00000000:426 lvn=62425.00000000:604,62995.00000000:426 complete profile.volume.utc_day@1",
        "244809038ms start=172800000ms end=244800000ms sessions=1 vol=734.58682780 low=62140.00000000 high=63420.00000000 poc=62225.00000000 poc_vol=52.31817216 val=62140.00000000 vah=62660.00000000 va_vol=521.09726080 hvn=62225.00000000:1000,62645.00000000:588,63345.00000000:378 lvn=62425.00000000:588,62995.00000000:378 complete profile.volume.utc_day@1",
        "273671762ms start=259200000ms end=273660000ms sessions=1 vol=151.23928161 low=62390.00000000 high=63580.00000000 poc=62465.00000000 poc_vol=11.49519281 val=62390.00000000 vah=62880.00000000 va_vol=106.19838600 hvn=62465.00000000:1000,62855.00000000:686,63525.00000000:501 lvn=62655.00000000:686,63195.00000000:501 complete profile.volume.utc_day@1",
        "288106295ms start=259200000ms end=288060000ms sessions=1 vol=302.05528586 low=62380.00000000 high=63580.00000000 poc=62475.00000000 poc_vol=17.36465053 val=62380.00000000 vah=62900.00000000 va_vol=217.40691639 hvn=62465.00000000:1000,62865.00000000:818,63525.00000000:620 lvn=62645.00000000:818,63195.00000000:620 complete profile.volume.utc_day@1",
        "302435923ms start=259200000ms end=302400000ms sessions=1 vol=425.44578671 low=62370.00000000 high=63610.00000000 poc=62475.00000000 poc_vol=22.59986849 val=62370.00000000 vah=62900.00000000 va_vol=306.65156422 hvn=62465.00000000:1000,62875.00000000:747,63525.00000000:586 lvn=62645.00000000:747,63195.00000000:586 feed_gap profile.volume.utc_day@1",
        "316820812ms start=259200000ms end=316800000ms sessions=1 vol=558.35230789 low=62370.00000000 high=63610.00000000 poc=62465.00000000 poc_vol=31.23012376 val=62370.00000000 vah=62900.00000000 va_vol=395.84146362 hvn=62465.00000000:1000,62875.00000000:637,63525.00000000:542 lvn=62655.00000000:637,63195.00000000:542 feed_gap profile.volume.utc_day@1",
        "331265233ms start=259200000ms end=331260000ms sessions=1 vol=712.20612844 low=62370.00000000 high=63610.00000000 poc=62465.00000000 poc_vol=42.27247150 val=62370.00000000 vah=62900.00000000 va_vol=502.15235778 hvn=62465.00000000:1000,62875.00000000:573,63525.00000000:586 lvn=62655.00000000:586,63195.00000000:586 feed_gap profile.volume.utc_day@1",
        "360018971ms start=345600000ms end=360000000ms sessions=1 vol=165.40876521 low=62500.00000000 high=63750.00000000 poc=62555.00000000 poc_vol=13.57091054 val=62500.00000000 vah=63000.00000000 va_vol=117.23528830 hvn=62575.00000000:1000,62995.00000000:550,63685.00000000:478 lvn=62775.00000000:550,63325.00000000:478 complete profile.volume.utc_day@1",
        "374411778ms start=345600000ms end=374400000ms sessions=1 vol=331.38399305 low=62480.00000000 high=63750.00000000 poc=62555.00000000 poc_vol=20.22390184 val=62480.00000000 vah=63010.00000000 va_vol=242.51387001 hvn=62565.00000000:1000,62995.00000000:564,63685.00000000:623 lvn=62775.00000000:623,63325.00000000:623 complete profile.volume.utc_day@1",
        "388849840ms start=345600000ms end=388800000ms sessions=1 vol=482.79939511 low=62480.00000000 high=63750.00000000 poc=62555.00000000 poc_vol=30.61932405 val=62480.00000000 vah=63010.00000000 va_vol=356.22747098 hvn=62555.00000000:1000,62995.00000000:508,63695.00000000:579 lvn=62775.00000000:579,63335.00000000:579 complete profile.volume.utc_day@1",
        "403358138ms start=345600000ms end=403320000ms sessions=1 vol=637.50804688 low=62480.00000000 high=63780.00000000 poc=62565.00000000 poc_vol=39.19405489 val=62480.00000000 vah=63000.00000000 va_vol=448.20774070 hvn=62565.00000000:1000,62995.00000000:498,63685.00000000:538 lvn=62765.00000000:538,63335.00000000:538 complete profile.volume.utc_day@1",
        "417736301ms start=345600000ms end=417720000ms sessions=1 vol=789.51031540 low=62480.00000000 high=63780.00000000 poc=62545.00000000 poc_vol=48.30972951 val=62480.00000000 vah=63000.00000000 va_vol=562.47648345 hvn=62555.00000000:1000,62985.00000000:498,63685.00000000:503 lvn=62765.00000000:503,63335.00000000:503 complete profile.volume.utc_day@1",
        "446476931ms start=432000000ms end=446460000ms sessions=1 vol=142.68654098 low=62610.00000000 high=63800.00000000 poc=62625.00000000 poc_vol=10.53869682 val=62610.00000000 vah=63130.00000000 va_vol=101.01572937 hvn=62675.00000000:1000,63115.00000000:575,63725.00000000:542 lvn=62875.00000000:575,63415.00000000:542 complete profile.volume.utc_day@1",
        "460812749ms start=432000000ms end=460800000ms sessions=1 vol=297.65319781 low=62610.00000000 high=63810.00000000 poc=62685.00000000 poc_vol=17.86487533 val=62610.00000000 vah=63130.00000000 va_vol=210.36659113 hvn=62675.00000000:1000,63085.00000000:702,63745.00000000:759 lvn=62875.00000000:759,63415.00000000:759 complete profile.volume.utc_day@1",
        "475287418ms start=432000000ms end=475260000ms sessions=1 vol=463.61033899 low=62610.00000000 high=63820.00000000 poc=62655.00000000 poc_vol=24.59577735 val=62610.00000000 vah=63120.00000000 va_vol=329.19018295 hvn=62675.00000000:1000,63085.00000000:756,63745.00000000:645 lvn=62885.00000000:756,63425.00000000:645 complete profile.volume.utc_day@1",
        "489753964ms start=432000000ms end=489720000ms sessions=1 vol=496.24817686 low=62610.00000000 high=63820.00000000 poc=62655.00000000 poc_vol=31.06905746 val=62610.00000000 vah=63120.00000000 va_vol=357.44532236 hvn=62655.00000000:1000,63085.00000000:696,63745.00000000:576 lvn=62885.00000000:696,63425.00000000:576 complete profile.volume.utc_day@1",
        "504054771ms start=432000000ms end=504000000ms sessions=1 vol=537.32293670 low=62590.00000000 high=63820.00000000 poc=62655.00000000 poc_vol=32.77293910 val=62590.00000000 vah=63120.00000000 va_vol=390.93726230 hvn=62675.00000000:1000,63085.00000000:652,63745.00000000:563 lvn=62885.00000000:652,63425.00000000:563 complete profile.volume.utc_day@1",
        "532879107ms start=518400000ms end=532860000ms sessions=1 vol=173.07671040 low=62830.00000000 high=64090.00000000 poc=62905.00000000 poc_vol=11.74569499 val=62830.00000000 vah=63330.00000000 va_vol=126.39811397 hvn=62905.00000000:1000,63305.00000000:741,64025.00000000:401 lvn=63105.00000000:741,63675.00000000:401 complete profile.volume.utc_day@1",
        "547220615ms start=518400000ms end=547200000ms sessions=1 vol=337.04642464 low=62830.00000000 high=64090.00000000 poc=62865.00000000 poc_vol=22.55157764 val=62830.00000000 vah=63320.00000000 va_vol=243.66183470 hvn=62905.00000000:1000,63305.00000000:608,64025.00000000:390 lvn=63105.00000000:608,63665.00000000:390 complete profile.volume.utc_day@1",
        "561676485ms start=518400000ms end=561660000ms sessions=1 vol=508.44994725 low=62830.00000000 high=64090.00000000 poc=62875.00000000 poc_vol=33.20996854 val=62830.00000000 vah=63320.00000000 va_vol=364.55964105 hvn=62885.00000000:1000,63315.00000000:627,64035.00000000:414 lvn=63115.00000000:627,63665.00000000:414 complete profile.volume.utc_day@1",
        "576034425ms start=518400000ms end=576000000ms sessions=1 vol=663.32511665 low=62820.00000000 high=64110.00000000 poc=62875.00000000 poc_vol=49.53074997 val=62820.00000000 vah=63320.00000000 va_vol=471.73787601 hvn=62895.00000000:1000,63315.00000000:591,64035.00000000:403 lvn=63115.00000000:591,63665.00000000:403 complete profile.volume.utc_day@1",
        "590496065ms start=518400000ms end=590460000ms sessions=1 vol=837.57877332 low=62820.00000000 high=64110.00000000 poc=62905.00000000 poc_vol=59.86981519 val=62820.00000000 vah=63320.00000000 va_vol=601.67029730 hvn=62905.00000000:1000,63315.00000000:521,64035.00000000:375 lvn=63115.00000000:521,63665.00000000:375 complete profile.volume.utc_day@1",
    ];
    const GOLDEN_PRIOR_DAY: [&str; 7] = [
        "86480524ms start=0ms end=86400000ms sessions=1 vol=424.68311440 low=61920.00000000 high=63190.00000000 poc=62015.00000000 poc_vol=29.38714669 val=61920.00000000 vah=62440.00000000 va_vol=300.33135470 hvn=62005.00000000:985,62415.00000000:1000,63115.00000000:499 lvn=62215.00000000:985,62775.00000000:499 partial_start profile.volume.prior_day@1",
        "172819489ms start=86400000ms end=172800000ms sessions=1 vol=884.96941412 low=62020.00000000 high=63270.00000000 poc=62115.00000000 poc_vol=66.45003259 val=62020.00000000 vah=62550.00000000 va_vol=641.63888282 hvn=62115.00000000:1000,62535.00000000:599,63175.00000000:366 lvn=62315.00000000:599,62855.00000000:366 complete profile.volume.prior_day@1",
        "259208734ms start=172800000ms end=259200000ms sessions=1 vol=898.04449025 low=62140.00000000 high=63420.00000000 poc=62225.00000000 poc_vol=64.46088065 val=62140.00000000 vah=62670.00000000 va_vol=653.13725806 hvn=62225.00000000:1000,62655.00000000:615,63355.00000000:415 lvn=62425.00000000:615,62995.00000000:415 complete profile.volume.prior_day@1",
        "345658352ms start=259200000ms end=345600000ms sessions=1 vol=871.73927290 low=62370.00000000 high=63610.00000000 poc=62475.00000000 poc_vol=47.72249756 val=62370.00000000 vah=62910.00000000 va_vol=631.10026291 hvn=62465.00000000:1000,62865.00000000:646,63515.00000000:595 lvn=62655.00000000:646,63195.00000000:595 feed_gap profile.volume.prior_day@1",
        "432017592ms start=345600000ms end=432000000ms sessions=1 vol=947.18611227 low=62480.00000000 high=63780.00000000 poc=62545.00000000 poc_vol=60.48307683 val=62480.00000000 vah=63000.00000000 va_vol=674.81465017 hvn=62555.00000000:1000,62985.00000000:504,63685.00000000:516 lvn=62765.00000000:516,63335.00000000:516 complete profile.volume.prior_day@1",
        "518417461ms start=432000000ms end=518400000ms sessions=1 vol=576.56747755 low=62590.00000000 high=63820.00000000 poc=62655.00000000 poc_vol=36.30677867 val=62590.00000000 vah=63120.00000000 va_vol=419.00495519 hvn=62675.00000000:1000,63085.00000000:639,63745.00000000:533 lvn=62885.00000000:639,63415.00000000:533 complete profile.volume.prior_day@1",
        "604815642ms start=518400000ms end=604800000ms sessions=1 vol=1000.69239106 low=62820.00000000 high=64110.00000000 poc=62905.00000000 poc_vol=71.01475728 val=62820.00000000 vah=63320.00000000 va_vol=712.01836557 hvn=62905.00000000:1000,63315.00000000:492,64035.00000000:349 lvn=63115.00000000:492,63675.00000000:349 complete profile.volume.prior_day@1",
    ];
    const GOLDEN_COMPOSITE_5D: [&str; 3] = [
        "432017592ms start=0ms end=432000000ms sessions=5 vol=4026.62240394 low=61920.00000000 high=63780.00000000 poc=62535.00000000 poc_vol=99.12699621 val=61920.00000000 vah=62900.00000000 va_vol=2835.47645956 hvn=62115.00000000:681,62225.00000000:348,62545.00000000:1000,62985.00000000:328,63165.00000000:314,63355.00000000:257,63515.00000000:291,63685.00000000:346 lvn=62155.00000000:348,62325.00000000:681,62745.00000000:346,63065.00000000:314,63265.00000000:328,63435.00000000:257,63605.00000000:291 partial_start+feed_gap profile.volume.composite_5d@1",
        "518417461ms start=86400000ms end=518400000ms sessions=5 vol=4178.50676709 low=62020.00000000 high=63820.00000000 poc=62535.00000000 poc_vol=99.12699621 val=62310.00000000 vah=63550.00000000 va_vol=2933.75900539 hvn=62115.00000000:681,62225.00000000:348,62545.00000000:1000,62655.00000000:239,62985.00000000:328,63175.00000000:132,63355.00000000:257,63515.00000000:291,63695.00000000:376 lvn=62155.00000000:348,62335.00000000:681,62605.00000000:239,62765.00000000:376,63045.00000000:132,63265.00000000:328,63435.00000000:257,63605.00000000:291 feed_gap profile.volume.composite_5d@1",
        "604815642ms start=172800000ms end=604800000ms sessions=5 vol=4294.22974403 low=62140.00000000 high=64110.00000000 poc=62905.00000000 poc_vol=94.48696331 val=62330.00000000 vah=63360.00000000 va_vol=3042.20555029 hvn=62225.00000000:632,62465.00000000:158,62555.00000000:166,62655.00000000:732,62905.00000000:1000,63085.00000000:103,63325.00000000:561,63515.00000000:277,63695.00000000:343,64035.00000000:253 lvn=62335.00000000:632,62505.00000000:158,62605.00000000:166,62765.00000000:732,63045.00000000:103,63205.00000000:561,63435.00000000:343,63605.00000000:277,63885.00000000:253 feed_gap profile.volume.composite_5d@1",
    ];
}
