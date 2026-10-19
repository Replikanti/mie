//! The declared Market State snapshot of a context record (ADR-054 D2).
//!
//! A feature is in the snapshot of `conditional.sampling@1` if and only if a
//! filed consumer issue names it (#26 thresholds, #28 order-flow studies,
//! #31 suitability). Each value is copied from the committed state with its
//! [`FeatureValue`] validity: no arithmetic and no new float. Adding a
//! feature is a new policy version (ADR-054 D5).

use crate::candidate::effort::EffortResult;
use crate::derivatives::OiGridPoint;
use crate::feature::{FeatureKey, FeatureValue, catalog};
use crate::flow::{AggressionWindow, DayCvd};
use crate::liquidity::LiquidityWindow;
use crate::num::Price;
use crate::state::MarketState;
use std::fmt;

/// The features of [`StateSnapshot`], in field order (ADR-054 D2).
pub const SNAPSHOT_FEATURES: [FeatureKey; 13] = [
    catalog::TRADE_LAST_PRICE_V1.key,
    catalog::VOLATILITY_ATR_1H_V1.key,
    catalog::FLOW_WINDOW_5M_V1.key,
    catalog::FLOW_WINDOW_15M_V1.key,
    catalog::FLOW_WINDOW_1H_V1.key,
    catalog::FLOW_CVD_UTC_DAY_V1.key,
    catalog::CANDIDATE_EFFORT_RESULT_5M_V1.key,
    catalog::CANDIDATE_EFFORT_RESULT_15M_V1.key,
    catalog::CANDIDATE_EFFORT_RESULT_1H_V1.key,
    catalog::BOOK_LIQUIDITY_WINDOW_5M_V1.key,
    catalog::BOOK_LIQUIDITY_WINDOW_15M_V1.key,
    catalog::BOOK_LIQUIDITY_WINDOW_1H_V1.key,
    catalog::DERIVATIVES_OI_5M_V1.key,
];

/// The Market State subset of a context record (ADR-054 D2), each value a
/// copy of its committed feature.
///
/// `Display` prints `state` and, per feature in [`SNAPSHOT_FEATURES`] order,
/// `<key>=[<value>]`, `<key>=warming` or `<key>=unavailable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateSnapshot {
    /// `trade.last_price@1`: the threshold reference at decision time (#26).
    pub last_price: FeatureValue<Price>,
    /// `volatility.atr.1h@1`: thresholds in ATR multiples (#26).
    pub atr: FeatureValue<Price>,
    /// `flow.window.<5m|15m|1h>@1`, shortest first: aggressive buy and sell
    /// volume, delta and price displacement (#28).
    pub aggression: [FeatureValue<AggressionWindow>; 3],
    /// `flow.cvd.utc_day@1`: delta/CVD (#28).
    pub cvd_utc_day: FeatureValue<DayCvd>,
    /// `candidate.effort_result.<5m|15m|1h>@1`, shortest first: the judged
    /// evaluations, so "high aggression + strong price response" can be
    /// read on any sample (#28).
    pub effort: [FeatureValue<EffortResult>; 3],
    /// `book.liquidity.window.<5m|15m|1h>@1`, shortest first: passive
    /// liquidity (#28, #31). Live only: warming up in archive replays.
    pub liquidity: [FeatureValue<LiquidityWindow>; 3],
    /// `derivatives.oi.5m@1`: the OI condition (#31).
    pub oi_5m: FeatureValue<OiGridPoint>,
}

impl StateSnapshot {
    /// The snapshot of `state`.
    ///
    /// # Panics
    ///
    /// Never: every window of [`catalog::FLOW_WINDOWS`],
    /// [`catalog::CANDIDATE_EFFORT_RESULT`] and
    /// [`catalog::BOOK_LIQUIDITY_WINDOWS`] is computed.
    pub fn read(state: &MarketState) -> Self {
        Self {
            last_price: state.last_trade_price,
            atr: state.atr,
            aggression: catalog::FLOW_WINDOWS.map(|(timeframe, _)| {
                *state
                    .flow
                    .windows
                    .get(timeframe)
                    .expect("every flow window is computed")
            }),
            cvd_utc_day: state.flow.cvd_utc_day,
            effort: catalog::CANDIDATE_EFFORT_RESULT.map(|(timeframe, _)| {
                *state
                    .candidates
                    .effort_result(timeframe)
                    .expect("every effort window is evaluated")
            }),
            liquidity: catalog::BOOK_LIQUIDITY_WINDOWS.map(|(timeframe, _)| {
                *state
                    .book
                    .windows
                    .get(timeframe)
                    .expect("every liquidity window is computed")
            }),
            oi_5m: state.derivatives.oi_5m,
        }
    }
}

/// Writes ` <key>=[<value>]`, ` <key>=warming` or ` <key>=unavailable`.
fn write_value<T: fmt::Display>(
    f: &mut fmt::Formatter<'_>,
    key: FeatureKey,
    value: &FeatureValue<T>,
) -> fmt::Result {
    match value {
        FeatureValue::Ready(value) => write!(f, " {key}=[{value}]"),
        FeatureValue::WarmingUp { .. } => write!(f, " {key}=warming"),
        FeatureValue::Unavailable { .. } => write!(f, " {key}=unavailable"),
    }
}

impl fmt::Display for StateSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut keys = SNAPSHOT_FEATURES.into_iter();
        let mut key = || keys.next().expect("one key per snapshot value");
        f.write_str("state")?;
        write_value(f, key(), &self.last_price)?;
        write_value(f, key(), &self.atr)?;
        for value in &self.aggression {
            write_value(f, key(), value)?;
        }
        write_value(f, key(), &self.cvd_utc_day)?;
        for value in &self.effort {
            write_value(f, key(), value)?;
        }
        for value in &self.liquidity {
            write_value(f, key(), value)?;
        }
        write_value(f, key(), &self.oi_5m)
    }
}
