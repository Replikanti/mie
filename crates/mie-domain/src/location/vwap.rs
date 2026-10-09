//! The session-anchored VWAP `location.vwap.utc_day@1` (ADR-044, decision
//! 4).
//!
//! Every trade of the current UTC day adds its exact `price · qty` and `qty`
//! to two `i128` sums; the value is recomputed once per closed 1m bar from
//! the trades of closed minutes only, as the developing volume profile is
//! (ADR-036), and floored to [`Price`]. The sums restart when the daily bar
//! closes.

use crate::bars::{Bar, Coverage, Timeframe};
use crate::event::MarketEvent;
use crate::feature::{FeatureKey, FeatureValue, catalog};
use crate::fingerprint::Fingerprinter;
use crate::num::{Price, Qty};
use crate::state_hash::StateEncode;
use crate::time::EventTime;
use std::fmt;

use super::LocationError;

/// The volume-weighted average price of the current UTC day: one
/// `location.vwap.utc_day@1` value (ADR-044, decision 4).
///
/// `Display` prints the canonical line the golden tests pin, ending with the
/// feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Vwap {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// Open of the UTC day.
    pub start: EventTime,
    /// Exclusive end of the last folded minute.
    pub end: EventTime,
    /// `floor(Σ(price · qty) / Σqty)` over the trades of the day's closed
    /// minutes. The floor loses less than 1e-8 USDT.
    pub price: Price,
    /// `Σqty`: the day's volume over its closed minutes.
    pub volume: Qty,
    /// The OR of the coverage of the day's closed minutes.
    pub coverage: Coverage,
}

impl StateEncode for Vwap {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            start,
            end,
            price,
            volume,
            coverage,
        } = self;
        feature.encode(f);
        start.encode(f);
        end.encode(f);
        price.encode(f);
        volume.encode(f);
        coverage.encode(f);
    }
}

impl fmt::Display for Vwap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "start={} end={} vwap={} vol={} {} {}",
            self.start, self.end, self.price, self.volume, self.coverage, self.feature
        )
    }
}

/// The VWAP of a value warming up: no closed minute of the day with volume
/// yet.
pub(super) fn warming() -> FeatureValue<Vwap> {
    FeatureValue::WarmingUp {
        observed: 0,
        required: 1,
    }
}

/// The VWAP engine state: the day's sums, the developing minute's trades
/// included, and the coverage of its closed minutes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct VwapTracker {
    /// `Σ(price · qty)` in `1e-16` USDT.
    pub(crate) notional: i128,
    /// `Σqty` in `1e-8` BTC.
    pub(crate) volume: i128,
    /// The OR of the coverage of the day's closed minutes.
    pub(crate) coverage: Coverage,
}

/// The tracker after one event, committed with the bars it was computed
/// from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VwapStep {
    /// The tracker after the event.
    tracker: VwapTracker,
    /// The new value; `None` leaves the value as it is.
    value: Option<FeatureValue<Vwap>>,
}

impl VwapTracker {
    /// Steps the VWAP with `event`, given the bars it closed (`closed`, in
    /// close order), without changing the tracker.
    ///
    /// Order, as the developing volume profile's (ADR-036): closed bars are
    /// walked in close order — a 1m bar folds its coverage into the day, a
    /// 1d bar restarts the day; then the value is recomputed if a 1m bar
    /// closed, from the sums before the event (the trades of closed minutes);
    /// then a trade with a positive quantity is added.
    ///
    /// # Errors
    ///
    /// [`LocationError::Overflow`] if a sum leaves its range — `Σ(price ·
    /// qty)` the `i128` range, `Σqty` the `Qty` range; nothing is committed
    /// then.
    pub(crate) fn step(
        &self,
        event: &MarketEvent,
        closed: &[Bar],
    ) -> Result<VwapStep, LocationError> {
        let mut next = *self;
        let mut last_minute = None;
        for bar in closed {
            match bar.timeframe {
                Timeframe::M1 => {
                    next.coverage.partial_start |= bar.coverage.partial_start;
                    next.coverage.feed_gap |= bar.coverage.feed_gap;
                    last_minute = Some(bar);
                }
                Timeframe::D1 => next = Self::default(),
                Timeframe::M5 | Timeframe::M15 | Timeframe::H1 | Timeframe::H4 => {}
            }
        }
        let value = match last_minute {
            None => None,
            Some(_) if next.volume == 0 => Some(warming()),
            Some(minute) => Some(FeatureValue::Ready(next.value(minute)?)),
        };
        if let MarketEvent::Trade(trade) = event
            && trade.qty.units() > 0
        {
            let qty = i128::from(trade.qty.units());
            // |price · qty| < 2^126: the product itself never overflows.
            next.notional = next
                .notional
                .checked_add(i128::from(trade.price.units()) * qty)
                .ok_or(LocationError::Overflow)?;
            next.volume = next
                .volume
                .checked_add(qty)
                .filter(|volume| *volume <= i128::from(i64::MAX))
                .ok_or(LocationError::Overflow)?;
        }
        Ok(VwapStep {
            tracker: next,
            value,
        })
    }

    /// The value after the closed minute `minute`, from sums with volume.
    fn value(&self, minute: &Bar) -> Result<Vwap, LocationError> {
        let start = Timeframe::D1
            .open_of(minute.open_time)
            .ok_or(LocationError::Overflow)?;
        // A weighted mean of trade prices lies between two of them, so it
        // fits a `Price`; the volume was bounded when it was added.
        let price = i64::try_from(self.notional.div_euclid(self.volume))
            .map_err(|_| LocationError::Overflow)?;
        let volume = i64::try_from(self.volume).map_err(|_| LocationError::Overflow)?;
        Ok(Vwap {
            feature: catalog::LOCATION_VWAP_UTC_DAY_V1.key,
            start,
            end: minute.end(),
            price: Price::from_units(price),
            volume: Qty::from_units(volume),
            coverage: self.coverage,
        })
    }

    /// Commits a step computed by [`Self::step`] and writes its new value to
    /// `vwap`. Copies only: it cannot fail.
    pub(crate) fn commit(&mut self, step: VwapStep, vwap: &mut FeatureValue<Vwap>) {
        *self = step.tracker;
        if let Some(value) = step.value {
            *vwap = value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::tests::random_tape;
    use crate::event::samples::{gap, mark, t};
    use crate::event::{Aggressor, GapReason, Stream, Trade};
    use crate::feature::{ParamValue, WarmUp};
    use crate::state::MarketStateEngine;

    const DAY: i64 = 86_400_000;
    const MINUTE: i64 = 60_000;

    fn buy(millis: i64, trade_id: u64, at: &str, size: &str) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id,
            price: at.parse().unwrap(),
            qty: size.parse().unwrap(),
            aggressor: Aggressor::Buy,
        })
    }

    /// The engine's VWAP after each event.
    fn values(events: &[MarketEvent]) -> Vec<FeatureValue<Vwap>> {
        let mut engine = MarketStateEngine::new();
        events
            .iter()
            .map(|event| {
                engine.apply(event).unwrap();
                engine.state().location.vwap
            })
            .collect()
    }

    #[test]
    fn sums_are_exact_and_the_mean_is_floored() {
        let values = values(&[
            buy(1_000, 1, "62000", "1"),
            buy(2_000, 2, "62000.00000001", "2"),
            // In the developing minute: not folded yet.
            buy(61_000, 3, "70000", "5"),
        ]);
        assert_eq!(values[0], warming());
        assert_eq!(values[1], warming());
        let vwap = values[2].ready().unwrap();
        // (62000 · 1 + 62000.00000001 · 2) / 3 = 62000.000000006…, floored.
        assert_eq!(vwap.price, "62000.00000000".parse().unwrap());
        assert_eq!(vwap.volume, "3".parse().unwrap());
        assert_eq!((vwap.start, vwap.end), (t(0), t(MINUTE)));
        assert_eq!(
            vwap.to_string(),
            "start=0ms end=60000ms vwap=62000.00000000 vol=3.00000000 partial_start \
             location.vwap.utc_day@1"
        );
        // One more unit of the higher price reaches the next unit.
        let values = super::tests::values(&[
            buy(1_000, 1, "62000", "1"),
            buy(2_000, 2, "62000.00000003", "2"),
            buy(61_000, 3, "70000", "5"),
        ]);
        assert_eq!(
            values[2].ready().unwrap().price,
            "62000.00000002".parse().unwrap()
        );
    }

    #[test]
    fn the_day_restarts_and_warms_up_on_a_minute_with_volume() {
        let values = values(&[
            buy(DAY - 2 * MINUTE, 1, "62000", "1"),
            buy(DAY - MINUTE + 1, 2, "62100", "1"),
            // Closes the day: the new day has no closed minute yet.
            buy(DAY + 1_000, 3, "63000", "1"),
            mark(DAY + 2_000, 1),
            buy(DAY + MINUTE + 1_000, 4, "64000", "1"),
            // An empty minute in between: still the new day's first trade.
            buy(DAY + 3 * MINUTE + 1_000, 5, "65000", "1"),
        ]);
        let before = values[1].ready().unwrap();
        assert_eq!(
            (before.price, before.end),
            ("62000".parse().unwrap(), t(DAY - MINUTE))
        );
        assert_eq!(values[2], warming());
        // Other streams change nothing.
        assert_eq!(values[3], values[2]);
        let first = values[4].ready().unwrap();
        assert_eq!(
            (first.start, first.end, first.price, first.coverage),
            (
                t(DAY),
                t(DAY + MINUTE),
                "63000".parse().unwrap(),
                Coverage::default()
            )
        );
        let later = values[5].ready().unwrap();
        assert_eq!(later.price, "63500".parse().unwrap());
        assert_eq!(later.end, t(DAY + 3 * MINUTE));
    }

    #[test]
    fn a_trades_gap_flags_the_coverage_without_a_reset() {
        let values = values(&[
            buy(DAY + 1_000, 1, "62000", "1"),
            gap(
                Stream::Trades,
                DAY + 2_000,
                DAY + 3_000,
                GapReason::Disconnected,
            ),
            buy(DAY + MINUTE + 1_000, 2, "62010", "1"),
            buy(DAY + 2 * MINUTE + 1_000, 3, "62020", "1"),
        ]);
        let gapped = values[2].ready().unwrap();
        assert!(gapped.coverage.feed_gap && gapped.coverage.partial_start);
        let later = values[3].ready().unwrap();
        assert_eq!(later.price, "62005".parse().unwrap());
        assert_eq!(later.coverage, gapped.coverage);
    }

    #[test]
    fn a_failed_step_leaves_the_tracker_alone() {
        let tracker = VwapTracker {
            notional: i128::MAX - 1,
            volume: 1,
            coverage: Coverage::default(),
        };
        let before = tracker;
        let overflow = buy(1_000, 1, "1", "0.00000001");
        assert_eq!(tracker.step(&overflow, &[]), Err(LocationError::Overflow));
        assert_eq!(tracker, before);
        // A volume beyond the `Qty` range overflows too.
        let full = VwapTracker {
            volume: i128::from(i64::MAX),
            ..VwapTracker::default()
        };
        assert_eq!(
            full.step(&buy(1_000, 1, "1", "0.00000001"), &[]),
            Err(LocationError::Overflow)
        );
        // Zero quantities add nothing.
        let zero = buy(1_000, 1, "62000", "0");
        assert_eq!(full.step(&zero, &[]).unwrap().tracker, full);
    }

    #[test]
    fn the_vwap_folds_the_closed_minutes_on_random_tapes() {
        for seed in [31, 32] {
            let tape = random_tape(seed, 6_000);
            let mut engine = MarketStateEngine::new();
            let (mut notional, mut volume) = (0_i128, 0_i128);
            let (mut closed_notional, mut closed_volume) = (0_i128, 0_i128);
            let mut checked = 0;
            for event in &tape {
                engine.apply(event).unwrap();
                let closed = engine.closed_bars();
                if closed.iter().any(|bar| bar.timeframe == Timeframe::M1) {
                    (closed_notional, closed_volume) = (notional, volume);
                }
                if closed.iter().any(|bar| bar.timeframe == Timeframe::D1) {
                    (notional, volume) = (0, 0);
                    (closed_notional, closed_volume) = (0, 0);
                }
                if closed.iter().any(|bar| bar.timeframe == Timeframe::M1) {
                    match engine.state().location.vwap {
                        FeatureValue::Ready(vwap) => {
                            assert_eq!(i128::from(vwap.volume.units()), closed_volume);
                            assert_eq!(
                                i128::from(vwap.price.units()),
                                closed_notional.div_euclid(closed_volume),
                                "seed {seed}"
                            );
                            checked += 1;
                        }
                        other => {
                            assert_eq!(other, warming(), "seed {seed}");
                            assert_eq!(closed_volume, 0, "seed {seed}");
                        }
                    }
                }
                if let MarketEvent::Trade(trade) = event {
                    notional += i128::from(trade.price.units()) * i128::from(trade.qty.units());
                    volume += i128::from(trade.qty.units());
                }
            }
            assert!(checked > 1_000, "seed {seed}: {checked}");
        }
    }

    #[test]
    fn constants_match_the_catalog() {
        let definition = &catalog::LOCATION_VWAP_UTC_DAY_V1;
        assert_eq!(definition.warm_up, WarmUp::Samples(1));
        assert!(
            definition
                .params
                .iter()
                .any(|param| param.name == "session_ms"
                    && param.value == ParamValue::Int(Timeframe::D1.millis()))
        );
        assert!(catalog::CURRENT.contains(&definition.key));
    }

    /// The VWAP golden tape: three days of trades about a drifting price,
    /// from 13:00 UTC on day 0, with a trades gap on day 1.
    fn golden_tape() -> Vec<MarketEvent> {
        let mut lcg = crate::bars::tests::Lcg(0x6d69_6500_0000_0044);
        let mut events = Vec::new();
        let mut time = 13 * 3_600_000;
        let mut trade_id = 0;
        let mut gapped = false;
        while time < 3 * DAY + 1_000 {
            time += 5_000 + lcg.below(55_000);
            if !gapped && time >= DAY + 6 * 3_600_000 {
                let end = time + 20 * MINUTE;
                events.push(gap(Stream::Trades, time, end, GapReason::Disconnected));
                time = end + 1;
                gapped = true;
            }
            let drift = (time / 3_600_000) * 7 * crate::num::SCALE;
            let price =
                62_000 * crate::num::SCALE + drift + lcg.below(40_000_000_000) - 20_000_000_000;
            trade_id += 1;
            events.push(MarketEvent::Trade(Trade {
                time: t(time),
                trade_id,
                price: Price::from_units(price),
                qty: Qty::from_units(1 + lcg.below(300_000_000)),
                aggressor: if lcg.below(2) == 0 {
                    Aggressor::Buy
                } else {
                    Aggressor::Sell
                },
            }));
        }
        events
    }

    #[test]
    fn golden_location_vwap_utc_day_v1() {
        let mut engine = MarketStateEngine::new();
        let mut lines = Vec::new();
        for event in golden_tape() {
            engine.apply(&event).unwrap();
            let closed = engine
                .closed_bars()
                .iter()
                .any(|bar| bar.timeframe == Timeframe::H4);
            if let (true, FeatureValue::Ready(vwap)) = (closed, engine.state().location.vwap) {
                lines.push(format!("{} {vwap}", event.time()));
            }
        }
        assert_eq!(lines, GOLDEN_VWAP);
    }

    const GOLDEN_VWAP: [&str; 12] = [
        "57615362ms start=0ms end=57600000ms vwap=62088.52480783 vol=484.55191955 partial_start location.vwap.utc_day@1",
        "72033494ms start=0ms end=72000000ms vwap=62102.79216634 vol=1171.66373825 partial_start location.vwap.utc_day@1",
        "100819498ms start=86400000ms end=100800000ms vwap=62183.43060048 vol=679.26330361 complete location.vwap.utc_day@1",
        "115237302ms start=86400000ms end=115200000ms vwap=62192.28820590 vol=1267.50381934 feed_gap location.vwap.utc_day@1",
        "129611908ms start=86400000ms end=129600000ms vwap=62206.52944994 vol=1937.66222216 feed_gap location.vwap.utc_day@1",
        "144019062ms start=86400000ms end=144000000ms vwap=62225.30091632 vol=2612.84595467 feed_gap location.vwap.utc_day@1",
        "158441592ms start=86400000ms end=158400000ms vwap=62237.68951015 vol=3270.73457713 feed_gap location.vwap.utc_day@1",
        "187212212ms start=172800000ms end=187200000ms vwap=62339.80013256 vol=704.97706633 complete location.vwap.utc_day@1",
        "201626909ms start=172800000ms end=201600000ms vwap=62362.26145868 vol=1366.86107426 complete location.vwap.utc_day@1",
        "216028345ms start=172800000ms end=216000000ms vwap=62376.37531050 vol=2030.13007040 complete location.vwap.utc_day@1",
        "230400181ms start=172800000ms end=230400000ms vwap=62389.48040755 vol=2689.65167178 complete location.vwap.utc_day@1",
        "244824388ms start=172800000ms end=244800000ms vwap=62403.35605815 vol=3351.77338099 complete location.vwap.utc_day@1",
    ];
}
