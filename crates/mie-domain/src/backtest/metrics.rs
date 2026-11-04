//! Metrics and the regime split over trade records (ADR-060 D11).
//!
//! Every value is an exact integer sum with at most one half-to-even
//! rounding at its final division (`div_half_even`), so anyone can
//! re-derive it from the stored trade lines with integer arithmetic. The
//! functions are total: a record without the shape of a trade joins no
//! mean, and a value outside the `i64` unit range is `None` (D10).

use super::{Exit, TradeRecord};
use crate::num::{Rate, SCALE, div_half_even};
use crate::regime::RegimeLabel;

#[cfg(test)]
mod tests;

/// The buckets of one regime feature, in D11's order: `LOW`, `MEDIUM`,
/// `HIGH`, `EXTREME`, then unready (`-`).
pub const REGIME_BUCKETS: [Option<RegimeLabel>; 5] = [
    Some(RegimeLabel::Low),
    Some(RegimeLabel::Medium),
    Some(RegimeLabel::High),
    Some(RegimeLabel::Extreme),
    None,
];

/// The metrics of one set of records: the `<fields>` of a `metrics` line,
/// in ADR-060 D12's order. `None` is the line's `-`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metrics {
    /// Target, stop and time exits.
    pub trades: u64,
    /// Censored lines.
    pub censored: u64,
    /// Unfilled lines.
    pub unfilled: u64,
    /// Mean net R; `None` without a trade.
    pub expectancy: Option<Rate>,
    /// Mean gross R; `None` without a trade.
    pub mean_gross: Option<Rate>,
    /// Σ positive net R ÷ |Σ negative net R|; `None` without a losing
    /// trade, or outside the `i64` unit range.
    pub profit_factor: Option<Rate>,
    /// The share of trades with net R > 0, from 0 to 1; `None` without a
    /// trade.
    pub win_rate: Option<Rate>,
    /// The largest fall of cumulative net R from its running peak, which
    /// starts at 0, in record order: peak − trough, ≥ 0. `Some(0)` without
    /// a trade; `None` outside the `i64` unit range.
    pub max_drawdown: Option<Rate>,
    /// Mean (touch − decision time) over target exits, in ms; `None`
    /// without a target exit.
    pub time_to_target_ms: Option<i64>,
    /// Mean MFE in R; `None` without a trade.
    pub mean_mfe: Option<Rate>,
    /// Mean MAE in R; `None` without a trade.
    pub mean_mae: Option<Rate>,
    /// Mean displacement in R; `None` without a trade.
    pub mean_displacement: Option<Rate>,
}

/// An exact sum that stays exact or becomes `None` on overflow, never
/// wrapping (ADR-060 D10).
#[derive(Debug, Clone, Copy)]
struct Sum(Option<i128>);

impl Sum {
    const ZERO: Self = Self(Some(0));

    fn add(&mut self, value: i128) {
        self.0 = self.0.and_then(|sum| sum.checked_add(value));
    }

    /// ⟨sum / count⟩, or `None` with no count or outside the `i64` range.
    fn mean(self, count: u64) -> Option<i64> {
        let sum = self.0?;
        (count > 0)
            .then(|| div_half_even(sum, i128::from(count)))
            .and_then(|mean| i64::try_from(mean).ok())
    }
}

impl Metrics {
    /// The metrics over `records`, in their order (ADR-060 D11).
    ///
    /// A record counts as a trade only when its exit is target, stop or
    /// time **and** it carries net, gross, MFE, MAE and displacement: the
    /// shape the parser enforces and the simulator produces. Counts and
    /// denominators come from that one filter.
    pub fn over<'a>(records: impl IntoIterator<Item = &'a TradeRecord>) -> Self {
        let mut trades = 0_u64;
        let mut censored = 0_u64;
        let mut unfilled = 0_u64;
        let mut wins = 0_u64;
        let mut targets = 0_u64;
        let [mut net, mut gross, mut mfe, mut mae, mut displacement] = [Sum::ZERO; 5];
        let [mut positive, mut negative, mut to_target] = [Sum::ZERO; 3];
        // (cumulative net, its running peak, the largest fall so far).
        let mut drawdown = Some((0_i128, 0_i128, 0_i128));
        for record in records {
            match record.exit {
                Exit::Censored(_) => {
                    censored += 1;
                    continue;
                }
                Exit::Unfilled => {
                    unfilled += 1;
                    continue;
                }
                Exit::Target | Exit::Stop | Exit::Time => {}
            }
            let (Some(n), Some(g), Some(f), Some(a), Some(d)) = (
                record.net,
                record.gross,
                record.mfe,
                record.mae,
                record.displacement,
            ) else {
                continue;
            };
            let n = i128::from(n.units());
            trades += 1;
            net.add(n);
            gross.add(i128::from(g.units()));
            mfe.add(i128::from(f.units()));
            mae.add(i128::from(a.units()));
            displacement.add(i128::from(d.units()));
            if n > 0 {
                wins += 1;
                positive.add(n);
            } else if n < 0 {
                negative.add(-n);
            }
            drawdown = drawdown.and_then(|(cum, peak, fall)| {
                let cum = cum.checked_add(n)?;
                let peak = peak.max(cum);
                Some((cum, peak, fall.max(peak.checked_sub(cum)?)))
            });
            if record.exit == Exit::Target {
                targets += 1;
                to_target.add(
                    i128::from(record.exit_at.as_millis())
                        - i128::from(record.decision.time.as_millis()),
                );
            }
        }
        let rate = |units: Option<i64>| units.map(Rate::from_units);
        let scale = i128::from(SCALE);
        let profit_factor = match (positive.0, negative.0) {
            (Some(p), Some(n)) if n > 0 => p
                .checked_mul(scale)
                .and_then(|p| i64::try_from(div_half_even(p, n)).ok()),
            _ => None,
        };
        let win_rate = (trades > 0)
            .then(|| div_half_even(i128::from(wins) * scale, i128::from(trades)))
            .and_then(|share| i64::try_from(share).ok());
        Self {
            trades,
            censored,
            unfilled,
            expectancy: rate(net.mean(trades)),
            mean_gross: rate(gross.mean(trades)),
            profit_factor: rate(profit_factor),
            win_rate: rate(win_rate),
            max_drawdown: rate(drawdown.and_then(|(_, _, fall)| i64::try_from(fall).ok())),
            time_to_target_ms: to_target.mean(targets),
            mean_mfe: rate(mfe.mean(trades)),
            mean_mae: rate(mae.mean(trades)),
            mean_displacement: rate(displacement.mean(trades)),
        }
    }
}

/// The metrics of a run: over all records, and per regime feature of the
/// spec's set and per [`REGIME_BUCKETS`] label (ADR-060 D11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestMetrics {
    /// Over every record.
    pub all: Metrics,
    /// `regimes[i][b]`: over the records whose label of regime feature `i`
    /// is bucket `b` of [`REGIME_BUCKETS`]. Every bucket is present, empty
    /// or not, and its drawdown runs over its own trades only.
    pub regimes: Vec<[Metrics; 5]>,
}

impl BacktestMetrics {
    /// The metrics of `records` for a spec with `regime_features` regime
    /// features. A record without a label at index `i` joins no bucket of
    /// feature `i`; parsed text never has one.
    pub fn compute(records: &[TradeRecord], regime_features: usize) -> Self {
        let regimes = (0..regime_features)
            .map(|feature| {
                REGIME_BUCKETS.map(|bucket| {
                    Metrics::over(
                        records
                            .iter()
                            .filter(|record| record.regime.get(feature) == Some(&bucket)),
                    )
                })
            })
            .collect();
        Self {
            all: Metrics::over(records),
            regimes,
        }
    }
}
