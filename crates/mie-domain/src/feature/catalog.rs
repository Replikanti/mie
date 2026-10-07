//! Every feature definition ever shipped, and its lock (ADR-029).
//!
//! The catalog only grows: a definition is never edited or deleted once its
//! [`LOCK`] line is merged. A change ships as a new version next to the old
//! one, which stays computable so past experiments reproduce. The steps are
//! in the [`feature`](super) module docs ("Adding a feature").

use super::{
    FeatureDefinition, FeatureKey, FeatureRegistry, FeatureSet, Input, LockEntry, Param,
    ParamValue, WarmUp,
};
use crate::bars::Timeframe;
use crate::event::Stream;
use crate::num::{Price, SCALE};

/// `trade.last_price@1`: the price of the last trade.
///
/// - Inputs: trades.
/// - Warm-up: one sample, where a sample is a consumed trade.
/// - Gap policy: none — a feed gap leaves the last price in place.
pub const TRADE_LAST_PRICE_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("trade.last_price", 1),
    params: &[],
    inputs: &[Input::Stream(Stream::Trades)],
    warm_up: WarmUp::Samples(1),
};

/// A `bars.time.<label>@1` definition (ADR-031): the event-time bars of one
/// timeframe, built from trades.
const fn bars_time_v1(id: &'static str, params: &'static [Param]) -> FeatureDefinition {
    FeatureDefinition {
        key: FeatureKey::new(id, 1),
        params,
        inputs: &[Input::Stream(Stream::Trades)],
        warm_up: WarmUp::Samples(1),
    }
}

/// `bars.time.1m@1`: UTC-aligned one-minute bars from trades (ADR-031).
///
/// - Parameters: `timeframe_ms` = 60 000.
/// - Inputs: trades (trades and trades-stream feed gaps).
/// - Warm-up: one sample, where a sample is a closed bar of this timeframe.
///   The developing bar is ready from the first trades-stream event.
/// - Gap policy: a trades gap marks every bar it overlaps with
///   `coverage.feed_gap`; the series never goes back to warming up.
pub const BARS_TIME_1M_V1: FeatureDefinition = bars_time_v1(
    "bars.time.1m",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(60_000),
    }],
);

/// `bars.time.5m@1`: UTC-aligned five-minute bars from trades (ADR-031).
///
/// Parameters: `timeframe_ms` = 300 000. Inputs, warm-up and gap policy as
/// [`BARS_TIME_1M_V1`].
pub const BARS_TIME_5M_V1: FeatureDefinition = bars_time_v1(
    "bars.time.5m",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(300_000),
    }],
);

/// `bars.time.15m@1`: UTC-aligned fifteen-minute bars from trades
/// (ADR-031).
///
/// Parameters: `timeframe_ms` = 900 000. Inputs, warm-up and gap policy as
/// [`BARS_TIME_1M_V1`].
pub const BARS_TIME_15M_V1: FeatureDefinition = bars_time_v1(
    "bars.time.15m",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(900_000),
    }],
);

/// `bars.time.1h@1`: UTC-aligned one-hour bars from trades (ADR-031).
///
/// Parameters: `timeframe_ms` = 3 600 000. Inputs, warm-up and gap policy as
/// [`BARS_TIME_1M_V1`].
pub const BARS_TIME_1H_V1: FeatureDefinition = bars_time_v1(
    "bars.time.1h",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(3_600_000),
    }],
);

/// `bars.time.4h@1`: UTC-aligned four-hour bars from trades (ADR-031).
///
/// Parameters: `timeframe_ms` = 14 400 000. Inputs, warm-up and gap policy
/// as [`BARS_TIME_1M_V1`].
pub const BARS_TIME_4H_V1: FeatureDefinition = bars_time_v1(
    "bars.time.4h",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(14_400_000),
    }],
);

/// `bars.time.1d@1`: UTC-aligned daily bars from trades, opening at
/// 00:00 UTC (ADR-031).
///
/// Parameters: `timeframe_ms` = 86 400 000. Inputs, warm-up and gap policy
/// as [`BARS_TIME_1M_V1`].
pub const BARS_TIME_1D_V1: FeatureDefinition = bars_time_v1(
    "bars.time.1d",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(86_400_000),
    }],
);

/// The bar feature of each timeframe, in [`Timeframe::ALL`] order; the
/// Market State builds its [`BarSet`](crate::bars::BarSet) from it.
pub const BARS_TIME: [(Timeframe, &FeatureDefinition); 6] = [
    (Timeframe::M1, &BARS_TIME_1M_V1),
    (Timeframe::M5, &BARS_TIME_5M_V1),
    (Timeframe::M15, &BARS_TIME_15M_V1),
    (Timeframe::H1, &BARS_TIME_1H_V1),
    (Timeframe::H4, &BARS_TIME_4H_V1),
    (Timeframe::D1, &BARS_TIME_1D_V1),
];

/// A `bars.motion.<label>@1` definition (ADR-033): change, range, return
/// and velocity of each closed bar of one timeframe.
const fn bars_motion_v1(
    id: &'static str,
    params: &'static [Param],
    inputs: &'static [Input],
) -> FeatureDefinition {
    FeatureDefinition {
        key: FeatureKey::new(id, 1),
        params,
        inputs,
        warm_up: WarmUp::Samples(1),
    }
}

/// `bars.motion.1m@1`: the motion of each closed 1m bar against the previous
/// close (ADR-033, decision 7): `change = close − previous close` and
/// `range = high − low`, exact; the simple return and the velocity (return
/// per minute) are derived from them.
///
/// - Parameters: `timeframe_ms` = 60 000.
/// - Inputs: `bars.time.1m@1`.
/// - Warm-up: one sample, where a sample is a closed bar with a previous
///   close (the anchor).
/// - Gap policy (ADR-033, decision 6): a bar with trades sets the anchor;
///   one without an anchor only anchors. An empty complete bar is a sample
///   with change and range 0. An empty incomplete bar (`partial_start` or
///   `feed_gap`) breaks the series: back to warming up, anchor cleared. A
///   `feed_gap` bar with trades is a sample and carries its coverage.
pub const BARS_MOTION_1M_V1: FeatureDefinition = bars_motion_v1(
    "bars.motion.1m",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(60_000),
    }],
    &[Input::Feature(BARS_TIME_1M_V1.key)],
);

/// `bars.motion.5m@1`: the motion of each closed 5m bar (ADR-033).
///
/// Parameters: `timeframe_ms` = 300 000. Inputs: `bars.time.5m@1`. Warm-up
/// and gap policy as [`BARS_MOTION_1M_V1`].
pub const BARS_MOTION_5M_V1: FeatureDefinition = bars_motion_v1(
    "bars.motion.5m",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(300_000),
    }],
    &[Input::Feature(BARS_TIME_5M_V1.key)],
);

/// `bars.motion.15m@1`: the motion of each closed 15m bar (ADR-033).
///
/// Parameters: `timeframe_ms` = 900 000. Inputs: `bars.time.15m@1`. Warm-up
/// and gap policy as [`BARS_MOTION_1M_V1`].
pub const BARS_MOTION_15M_V1: FeatureDefinition = bars_motion_v1(
    "bars.motion.15m",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(900_000),
    }],
    &[Input::Feature(BARS_TIME_15M_V1.key)],
);

/// `bars.motion.1h@1`: the motion of each closed 1h bar (ADR-033).
///
/// Parameters: `timeframe_ms` = 3 600 000. Inputs: `bars.time.1h@1`.
/// Warm-up and gap policy as [`BARS_MOTION_1M_V1`].
pub const BARS_MOTION_1H_V1: FeatureDefinition = bars_motion_v1(
    "bars.motion.1h",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(3_600_000),
    }],
    &[Input::Feature(BARS_TIME_1H_V1.key)],
);

/// `bars.motion.4h@1`: the motion of each closed 4h bar (ADR-033).
///
/// Parameters: `timeframe_ms` = 14 400 000. Inputs: `bars.time.4h@1`.
/// Warm-up and gap policy as [`BARS_MOTION_1M_V1`].
pub const BARS_MOTION_4H_V1: FeatureDefinition = bars_motion_v1(
    "bars.motion.4h",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(14_400_000),
    }],
    &[Input::Feature(BARS_TIME_4H_V1.key)],
);

/// `bars.motion.1d@1`: the motion of each closed daily bar (ADR-033).
///
/// Parameters: `timeframe_ms` = 86 400 000. Inputs: `bars.time.1d@1`.
/// Warm-up and gap policy as [`BARS_MOTION_1M_V1`].
pub const BARS_MOTION_1D_V1: FeatureDefinition = bars_motion_v1(
    "bars.motion.1d",
    &[Param {
        name: "timeframe_ms",
        value: ParamValue::Int(86_400_000),
    }],
    &[Input::Feature(BARS_TIME_1D_V1.key)],
);

/// The motion feature of each timeframe, in [`Timeframe::ALL`] order; the
/// Market State builds its [`MotionSet`](crate::volatility::MotionSet) from
/// it.
pub const BARS_MOTION: [(Timeframe, &FeatureDefinition); 6] = [
    (Timeframe::M1, &BARS_MOTION_1M_V1),
    (Timeframe::M5, &BARS_MOTION_5M_V1),
    (Timeframe::M15, &BARS_MOTION_15M_V1),
    (Timeframe::H1, &BARS_MOTION_1H_V1),
    (Timeframe::H4, &BARS_MOTION_4H_V1),
    (Timeframe::D1, &BARS_MOTION_1D_V1),
];

/// The timeframe whose bars feed ATR(14) and the regime (ADR-033,
/// decision 1).
pub const REGIME_TIMEFRAME: Timeframe = Timeframe::H1;

/// `volatility.atr.1h@1`: ATR(14) of closed 1h bars, as Pine Script v5
/// `ta.atr(14)` (ADR-033, decision 2).
///
/// True range is `high − low` without a previous close, else
/// `max(high − low, |high − previous close|, |low − previous close|)`. The
/// first ATR is the mean of the first 14 true ranges, then
/// `ATR = (13 · ATR[1] + TR) / 14`, each rounded half to even to 1e-8.
///
/// - Parameters: `length` = 14, `smoothing` = `wilder_sma_seed`,
///   `timeframe_ms` = 3 600 000.
/// - Inputs: `bars.time.1h@1`.
/// - Warm-up: 14 samples, where a sample is a closed 1h bar that the gap
///   policy admits.
/// - Gap policy (ADR-033, decision 6): a bar with trades sets the anchor
///   (the previous close). A complete bar with trades is a sample, against
///   the anchor if there is one. An incomplete bar with trades is a sample
///   on its observed prices if there is an anchor, else it only anchors. An
///   empty complete bar is a sample with true range 0 if there is an
///   anchor, else skipped. An empty incomplete bar breaks the series: back
///   to warming up from 0, anchor and window cleared.
pub const VOLATILITY_ATR_1H_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("volatility.atr.1h", 1),
    params: &[
        Param {
            name: "length",
            value: ParamValue::Int(14),
        },
        Param {
            name: "smoothing",
            value: ParamValue::Text("wilder_sma_seed"),
        },
        Param {
            name: "timeframe_ms",
            value: ParamValue::Int(3_600_000),
        },
    ],
    inputs: &[Input::Feature(BARS_TIME_1H_V1.key)],
    warm_up: WarmUp::Samples(14),
};

/// `volatility.regime.1h@1`: the ATR-percentile regime (ADR-017, ADR-033,
/// decisions 3 and 4). The percentile is Pine Script v5
/// `ta.percentrank(atr, 200)`: the share of the previous 200 ATR values at
/// or below the current one, in half-steps from 0 to 100; the label uses
/// the upper-closed ADR-017 bands. Context, never an entry signal.
///
/// - Parameters: `bands` = `adr017_upper_closed`, `lookback` = 200,
///   `rank` = `previous_at_or_below`, `timeframe_ms` = 3 600 000.
/// - Inputs: `volatility.atr.1h@1`.
/// - Warm-up: 214 samples (14 for the ATR, then 200 previous ATR values),
///   with samples as in [`VOLATILITY_ATR_1H_V1`].
/// - Gap policy: that of [`VOLATILITY_ATR_1H_V1`]; a break clears the
///   window.
pub const VOLATILITY_REGIME_1H_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("volatility.regime.1h", 1),
    params: &[
        Param {
            name: "bands",
            value: ParamValue::Text("adr017_upper_closed"),
        },
        Param {
            name: "lookback",
            value: ParamValue::Int(200),
        },
        Param {
            name: "rank",
            value: ParamValue::Text("previous_at_or_below"),
        },
        Param {
            name: "timeframe_ms",
            value: ParamValue::Int(3_600_000),
        },
    ],
    inputs: &[Input::Feature(VOLATILITY_ATR_1H_V1.key)],
    warm_up: WarmUp::Samples(214),
};

/// `flow.cvd.continuous@1`: cumulative volume delta — the sum of signed
/// aggressor quantity (buy `+qty`, sell `−qty`) since the first consumed
/// trade, the run anchor (ADR-035, decision 2). Aggression, not direction
/// (ADR-023).
///
/// - Parameters: `gap_policy` = `continue_counted`.
/// - Inputs: trades (trades and trades-stream feed gaps).
/// - Warm-up: one sample, where a sample is a consumed trade.
/// - Gap policy: a trades gap never resets the sum; once the anchor is set,
///   each trades gap increments the value's `gaps` count. The level is
///   relative to the run: two values differ by the exact market delta only
///   when they carry the same `anchor` and `gaps`.
pub const FLOW_CVD_CONTINUOUS_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("flow.cvd.continuous", 1),
    params: &[Param {
        name: "gap_policy",
        value: ParamValue::Text("continue_counted"),
    }],
    inputs: &[Input::Stream(Stream::Trades)],
    warm_up: WarmUp::Samples(1),
};

/// `flow.cvd.utc_day@1`: cumulative volume delta since 00:00 UTC — the delta
/// of the developing `bars.time.1d@1` bar (ADR-035, decision 2).
///
/// - Parameters: `session_ms` = 86 400 000.
/// - Inputs: `bars.time.1d@1`.
/// - Warm-up: one sample, where a sample is a trades-stream event (the
///   developing daily bar exists from the first one).
/// - Gap policy: that of `bars.time.1d@1` — the value carries the daily
///   bar's coverage (`partial_start`, `feed_gap`) and never goes back to
///   warming up.
pub const FLOW_CVD_UTC_DAY_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("flow.cvd.utc_day", 1),
    params: &[Param {
        name: "session_ms",
        value: ParamValue::Int(86_400_000),
    }],
    inputs: &[Input::Feature(BARS_TIME_1D_V1.key)],
    warm_up: WarmUp::Samples(1),
};

/// A `flow.window.<label>@1` definition (ADR-035): rolling aggression over
/// the last `minutes` closed 1m bars, with the large-print stats from
/// trades.
const fn flow_window_v1(
    id: &'static str,
    params: &'static [Param],
    minutes: u32,
) -> FeatureDefinition {
    FeatureDefinition {
        key: FeatureKey::new(id, 1),
        params,
        inputs: &[
            Input::Stream(Stream::Trades),
            Input::Feature(BARS_TIME_1M_V1.key),
        ],
        warm_up: WarmUp::Samples(minutes),
    }
}

/// `flow.window.5m@1`: aggression over the last 5 closed 1m bars (ADR-035,
/// decisions 1 and 3–6): buy and sell volume, delta, volume and trade count;
/// large-print count and volumes; trade intensity; and the price response to
/// the net aggression. Aggression, not direction (ADR-023).
///
/// - Parameters: `large_notional_usdt` = 100 000 (a print is large when
///   `|price × qty|` is at or above it), `window_ms` = 300 000.
/// - Inputs: trades (for the large-print classification) and
///   `bars.time.1m@1` (for everything else).
/// - Warm-up: 5 samples, where a sample is a closed 1m bar. The value steps
///   once per closed 1m bar; the developing minute is never included.
/// - Gap policy: the window never goes back to warming up. It carries the
///   OR of its minutes' coverage; an empty incomplete minute adds zeros and
///   its `feed_gap` flag.
pub const FLOW_WINDOW_5M_V1: FeatureDefinition = flow_window_v1(
    "flow.window.5m",
    &[
        Param {
            name: "large_notional_usdt",
            value: ParamValue::Int(100_000),
        },
        Param {
            name: "window_ms",
            value: ParamValue::Int(300_000),
        },
    ],
    5,
);

/// `flow.window.15m@1`: aggression over the last 15 closed 1m bars
/// (ADR-035).
///
/// Parameters: `large_notional_usdt` = 100 000, `window_ms` = 900 000.
/// Warm-up: 15 samples (closed 1m bars). Inputs and gap policy as
/// [`FLOW_WINDOW_5M_V1`].
pub const FLOW_WINDOW_15M_V1: FeatureDefinition = flow_window_v1(
    "flow.window.15m",
    &[
        Param {
            name: "large_notional_usdt",
            value: ParamValue::Int(100_000),
        },
        Param {
            name: "window_ms",
            value: ParamValue::Int(900_000),
        },
    ],
    15,
);

/// `flow.window.1h@1`: aggression over the last 60 closed 1m bars
/// (ADR-035).
///
/// Parameters: `large_notional_usdt` = 100 000, `window_ms` = 3 600 000.
/// Warm-up: 60 samples (closed 1m bars). Inputs and gap policy as
/// [`FLOW_WINDOW_5M_V1`].
pub const FLOW_WINDOW_1H_V1: FeatureDefinition = flow_window_v1(
    "flow.window.1h",
    &[
        Param {
            name: "large_notional_usdt",
            value: ParamValue::Int(100_000),
        },
        Param {
            name: "window_ms",
            value: ParamValue::Int(3_600_000),
        },
    ],
    60,
);

/// The aggression window of each length, shortest first; the Market State
/// builds its [`AggressionWindows`](crate::flow::AggressionWindows) from it.
/// The timeframe is the window length, not a bar series.
pub const FLOW_WINDOWS: [(Timeframe, &FeatureDefinition); 3] = [
    (Timeframe::M5, &FLOW_WINDOW_5M_V1),
    (Timeframe::M15, &FLOW_WINDOW_15M_V1),
    (Timeframe::H1, &FLOW_WINDOW_1H_V1),
];

/// `profile.volume.utc_day@1`: the developing volume profile of the current
/// UTC day (ADR-036): exact volume per 10 USDT price bin from trades, with
/// POC, value area (VAL, VAH), HVNs and LVNs.
///
/// - Parameters: `bin_size` = 10 USDT, `max_bins` = 10 000 (a wider range
///   is `Unavailable(OutOfRange)`), `node_prominence_pct` = 10,
///   `node_smoothing` = `triangular_5`, `poc_rule` =
///   `max_volume_center_lower`, `session_ms` = 86 400 000 (the day opens at
///   00:00 UTC), `value_area_pct` = 70, `value_area_rule` =
///   `single_bin_ties_both`.
/// - Inputs: trades (the volume per bin) and `bars.time.1m@1` (when the
///   levels step, and the coverage).
/// - Warm-up: one sample, where a sample is a closed minute of the current
///   UTC day with volume. The levels step once per closed 1m bar, from the
///   trades of closed minutes only; the developing minute is never
///   included. Every UTC day open restarts the warm-up.
/// - Gap policy: a trades gap never resets the profile. The value carries
///   the OR of the day's closed minutes' coverage.
pub const PROFILE_VOLUME_UTC_DAY_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("profile.volume.utc_day", 1),
    params: &[
        Param {
            name: "bin_size",
            value: ParamValue::Price(Price::from_units(10 * SCALE)),
        },
        Param {
            name: "max_bins",
            value: ParamValue::Int(10_000),
        },
        Param {
            name: "node_prominence_pct",
            value: ParamValue::Int(10),
        },
        Param {
            name: "node_smoothing",
            value: ParamValue::Text("triangular_5"),
        },
        Param {
            name: "poc_rule",
            value: ParamValue::Text("max_volume_center_lower"),
        },
        Param {
            name: "session_ms",
            value: ParamValue::Int(86_400_000),
        },
        Param {
            name: "value_area_pct",
            value: ParamValue::Int(70),
        },
        Param {
            name: "value_area_rule",
            value: ParamValue::Text("single_bin_ties_both"),
        },
    ],
    inputs: &[
        Input::Stream(Stream::Trades),
        Input::Feature(BARS_TIME_1M_V1.key),
    ],
    warm_up: WarmUp::Samples(1),
};

/// `profile.volume.prior_day@1`: the completed volume profile of the last
/// closed UTC day (ADR-036), fixed when its daily bar closes.
///
/// - Parameters: those of [`PROFILE_VOLUME_UTC_DAY_V1`] and `sessions` = 1.
/// - Inputs: trades (the volume per bin) and `bars.time.1d@1` (when the day
///   closes, and the coverage).
/// - Warm-up: one sample, where a sample is a closed UTC day. A day without
///   volume is `Unavailable(InputInvalid)`.
/// - Gap policy: a trades gap never resets the profile. The value carries
///   the closed daily bar's coverage.
pub const PROFILE_VOLUME_PRIOR_DAY_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("profile.volume.prior_day", 1),
    params: &[
        Param {
            name: "bin_size",
            value: ParamValue::Price(Price::from_units(10 * SCALE)),
        },
        Param {
            name: "max_bins",
            value: ParamValue::Int(10_000),
        },
        Param {
            name: "node_prominence_pct",
            value: ParamValue::Int(10),
        },
        Param {
            name: "node_smoothing",
            value: ParamValue::Text("triangular_5"),
        },
        Param {
            name: "poc_rule",
            value: ParamValue::Text("max_volume_center_lower"),
        },
        Param {
            name: "session_ms",
            value: ParamValue::Int(86_400_000),
        },
        Param {
            name: "sessions",
            value: ParamValue::Int(1),
        },
        Param {
            name: "value_area_pct",
            value: ParamValue::Int(70),
        },
        Param {
            name: "value_area_rule",
            value: ParamValue::Text("single_bin_ties_both"),
        },
    ],
    inputs: &[
        Input::Stream(Stream::Trades),
        Input::Feature(BARS_TIME_1D_V1.key),
    ],
    warm_up: WarmUp::Samples(1),
};

/// `profile.volume.composite_5d@1`: the bin-wise sum of the last 5
/// completed UTC days (ADR-036), recomputed once per day close.
///
/// - Parameters: those of [`PROFILE_VOLUME_UTC_DAY_V1`] and `sessions` = 5.
/// - Inputs: as [`PROFILE_VOLUME_PRIOR_DAY_V1`].
/// - Warm-up: 5 samples, where a sample is a closed UTC day. Empty days
///   count; five days without volume are `Unavailable(InputInvalid)`.
/// - Gap policy: a trades gap never resets the profile. The value carries
///   the OR of the five closed daily bars' coverage.
pub const PROFILE_VOLUME_COMPOSITE_5D_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("profile.volume.composite_5d", 1),
    params: &[
        Param {
            name: "bin_size",
            value: ParamValue::Price(Price::from_units(10 * SCALE)),
        },
        Param {
            name: "max_bins",
            value: ParamValue::Int(10_000),
        },
        Param {
            name: "node_prominence_pct",
            value: ParamValue::Int(10),
        },
        Param {
            name: "node_smoothing",
            value: ParamValue::Text("triangular_5"),
        },
        Param {
            name: "poc_rule",
            value: ParamValue::Text("max_volume_center_lower"),
        },
        Param {
            name: "session_ms",
            value: ParamValue::Int(86_400_000),
        },
        Param {
            name: "sessions",
            value: ParamValue::Int(5),
        },
        Param {
            name: "value_area_pct",
            value: ParamValue::Int(70),
        },
        Param {
            name: "value_area_rule",
            value: ParamValue::Text("single_bin_ties_both"),
        },
    ],
    inputs: &[
        Input::Stream(Stream::Trades),
        Input::Feature(BARS_TIME_1D_V1.key),
    ],
    warm_up: WarmUp::Samples(5),
};

/// Every version of every feature. Never shrinks.
pub const DEFINITIONS: &[&FeatureDefinition] = &[
    &TRADE_LAST_PRICE_V1,
    &BARS_TIME_1M_V1,
    &BARS_TIME_5M_V1,
    &BARS_TIME_15M_V1,
    &BARS_TIME_1H_V1,
    &BARS_TIME_4H_V1,
    &BARS_TIME_1D_V1,
    &BARS_MOTION_1M_V1,
    &BARS_MOTION_5M_V1,
    &BARS_MOTION_15M_V1,
    &BARS_MOTION_1H_V1,
    &BARS_MOTION_4H_V1,
    &BARS_MOTION_1D_V1,
    &VOLATILITY_ATR_1H_V1,
    &VOLATILITY_REGIME_1H_V1,
    &FLOW_CVD_CONTINUOUS_V1,
    &FLOW_CVD_UTC_DAY_V1,
    &FLOW_WINDOW_5M_V1,
    &FLOW_WINDOW_15M_V1,
    &FLOW_WINDOW_1H_V1,
    &PROFILE_VOLUME_UTC_DAY_V1,
    &PROFILE_VOLUME_PRIOR_DAY_V1,
    &PROFILE_VOLUME_COMPOSITE_5D_V1,
];

/// The fingerprint each published `id@version` must keep. Append-only: one
/// line per version; never edit or delete a line.
pub const LOCK: &[LockEntry] = &[
    LockEntry::new("trade.last_price", 1, 0x2eae_f1e1_bac4_d513),
    LockEntry::new("bars.time.1m", 1, 0x718b_6ca8_9bdf_18f1),
    LockEntry::new("bars.time.5m", 1, 0x674e_b284_f01d_20d4),
    LockEntry::new("bars.time.15m", 1, 0xcaa1_f8c6_0efd_c385),
    LockEntry::new("bars.time.1h", 1, 0xefab_77d5_2069_7498),
    LockEntry::new("bars.time.4h", 1, 0x9af3_15e8_7a1e_781e),
    LockEntry::new("bars.time.1d", 1, 0x0ec3_e6ec_029e_9943),
    LockEntry::new("bars.motion.1m", 1, 0x9573_ef81_3cc8_82db),
    LockEntry::new("bars.motion.5m", 1, 0x36ef_9df0_dd4e_5b88),
    LockEntry::new("bars.motion.15m", 1, 0x5c6e_04d5_9e34_0441),
    LockEntry::new("bars.motion.1h", 1, 0x688c_da22_4281_8ae9),
    LockEntry::new("bars.motion.4h", 1, 0x6976_2fda_d9bf_46e4),
    LockEntry::new("bars.motion.1d", 1, 0x4629_af65_63d4_ab38),
    LockEntry::new("volatility.atr.1h", 1, 0xc6cd_151d_59ca_3a88),
    LockEntry::new("volatility.regime.1h", 1, 0x33ef_daf4_43c8_143a),
    LockEntry::new("flow.cvd.continuous", 1, 0x64b2_96cc_a73a_3c27),
    LockEntry::new("flow.cvd.utc_day", 1, 0xdafb_4466_3bdf_c2e9),
    LockEntry::new("flow.window.5m", 1, 0xd5b8_a750_f763_4076),
    LockEntry::new("flow.window.15m", 1, 0xd57b_f62e_9582_61a1),
    LockEntry::new("flow.window.1h", 1, 0xdd02_bda3_c32f_83b9),
    LockEntry::new("profile.volume.utc_day", 1, 0x664c_8237_3b35_4b5a),
    LockEntry::new("profile.volume.prior_day", 1, 0x8cec_f1e9_1a0a_2d6f),
    LockEntry::new("profile.volume.composite_5d", 1, 0x9c8a_e4da_061a_e2b4),
];

/// The default feature set: the latest version of each computed feature.
pub const CURRENT: &[FeatureKey] = &[
    TRADE_LAST_PRICE_V1.key,
    BARS_TIME_1M_V1.key,
    BARS_TIME_5M_V1.key,
    BARS_TIME_15M_V1.key,
    BARS_TIME_1H_V1.key,
    BARS_TIME_4H_V1.key,
    BARS_TIME_1D_V1.key,
    BARS_MOTION_1M_V1.key,
    BARS_MOTION_5M_V1.key,
    BARS_MOTION_15M_V1.key,
    BARS_MOTION_1H_V1.key,
    BARS_MOTION_4H_V1.key,
    BARS_MOTION_1D_V1.key,
    VOLATILITY_ATR_1H_V1.key,
    VOLATILITY_REGIME_1H_V1.key,
    FLOW_CVD_CONTINUOUS_V1.key,
    FLOW_CVD_UTC_DAY_V1.key,
    FLOW_WINDOW_5M_V1.key,
    FLOW_WINDOW_15M_V1.key,
    FLOW_WINDOW_1H_V1.key,
    PROFILE_VOLUME_UTC_DAY_V1.key,
    PROFILE_VOLUME_PRIOR_DAY_V1.key,
    PROFILE_VOLUME_COMPOSITE_5D_V1.key,
];

/// The registry of [`DEFINITIONS`].
///
/// # Panics
///
/// If the catalog is invalid, which the catalog tests rule out before
/// anything ships.
pub fn registry() -> FeatureRegistry {
    FeatureRegistry::new(DEFINITIONS).expect("the catalog is valid (see the catalog tests)")
}

/// The default feature set, built from [`CURRENT`].
///
/// # Panics
///
/// If the catalog is invalid, which the catalog tests rule out before
/// anything ships.
pub fn current_set() -> FeatureSet {
    FeatureSet::new(&registry(), CURRENT).expect("the current set is valid (see the catalog tests)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fingerprint::Fingerprinter;

    #[test]
    fn catalog_is_valid() {
        FeatureRegistry::new(DEFINITIONS).unwrap();
        FeatureSet::new(&registry(), CURRENT).unwrap();
        assert_eq!(registry().definitions().count(), DEFINITIONS.len());
    }

    #[test]
    fn catalog_matches_lock() {
        if let Err(errors) = registry().verify_lock(LOCK) {
            let report: Vec<String> = errors.iter().map(ToString::to_string).collect();
            panic!(
                "the feature catalog disagrees with its lock:\n{}",
                report.join("\n")
            );
        }
        assert_eq!(LOCK.len(), DEFINITIONS.len());
    }

    #[test]
    fn every_definition_stays_computable() {
        // ADR-029: an old version must still fit a feature set — its own
        // dependency closure.
        let registry = registry();
        for definition in DEFINITIONS {
            let closure = registry.closure(definition.key).unwrap();
            let set = FeatureSet::new(&registry, &closure)
                .unwrap_or_else(|error| panic!("{} is not computable: {error}", definition.key));
            assert!(set.definitions().any(|member| member == *definition));
        }
    }

    #[test]
    fn lock_table_is_pinned() {
        // LOCK is append-only. Deleting a version together with its lock
        // line passes catalog_matches_lock, so the whole table is pinned
        // here: appending a line updates both pins below in the same PR;
        // any other change to them is a deleted or edited published version
        // and must not merge.
        let mut hasher = Fingerprinter::new();
        hasher.write_len(LOCK.len());
        for entry in LOCK {
            hasher.write_str(entry.key.id.as_str());
            hasher.write_u32(entry.key.version.get());
            hasher.write_u64(entry.fingerprint.value());
        }
        assert_eq!(LOCK.len(), 23, "lock lines");
        assert_eq!(
            hasher.finish().to_string(),
            "7115052d525501db",
            "lock digest"
        );
    }

    #[test]
    fn current_uses_the_latest_version_of_each_feature() {
        let registry = registry();
        for key in CURRENT {
            let latest = registry
                .definitions()
                .filter(|definition| definition.key.id == key.id)
                .map(|definition| definition.key.version)
                .max();
            assert_eq!(latest, Some(key.version), "{key}");
        }
    }

    #[test]
    fn bar_features_match_their_timeframes() {
        assert_eq!(BARS_TIME.map(|(timeframe, _)| timeframe), Timeframe::ALL);
        for (timeframe, definition) in BARS_TIME {
            assert_eq!(
                definition.key.id.as_str(),
                format!("bars.time.{}", timeframe.label()),
                "{timeframe:?}"
            );
            assert_eq!(
                definition.params,
                &[Param {
                    name: "timeframe_ms",
                    value: ParamValue::Int(timeframe.millis()),
                }],
                "{timeframe:?}"
            );
            assert!(CURRENT.contains(&definition.key), "{}", definition.key);
        }
    }

    #[test]
    fn motion_features_match_their_timeframes() {
        assert_eq!(BARS_MOTION.map(|(timeframe, _)| timeframe), Timeframe::ALL);
        for ((timeframe, definition), (_, bars)) in BARS_MOTION.into_iter().zip(BARS_TIME) {
            assert_eq!(
                definition.key.id.as_str(),
                format!("bars.motion.{}", timeframe.label()),
                "{timeframe:?}"
            );
            assert_eq!(
                definition.params,
                &[Param {
                    name: "timeframe_ms",
                    value: ParamValue::Int(timeframe.millis()),
                }],
                "{timeframe:?}"
            );
            assert_eq!(definition.inputs, &[Input::Feature(bars.key)]);
            assert!(CURRENT.contains(&definition.key), "{}", definition.key);
        }
    }

    #[test]
    fn volatility_features_use_the_regime_timeframe() {
        for definition in [&VOLATILITY_ATR_1H_V1, &VOLATILITY_REGIME_1H_V1] {
            assert!(
                definition.params.contains(&Param {
                    name: "timeframe_ms",
                    value: ParamValue::Int(REGIME_TIMEFRAME.millis()),
                }),
                "{}",
                definition.key
            );
            assert!(CURRENT.contains(&definition.key), "{}", definition.key);
        }
        assert_eq!(
            VOLATILITY_ATR_1H_V1.inputs,
            &[Input::Feature(BARS_TIME_1H_V1.key)]
        );
        assert_eq!(
            VOLATILITY_REGIME_1H_V1.inputs,
            &[Input::Feature(VOLATILITY_ATR_1H_V1.key)]
        );
    }

    #[test]
    fn flow_features_match_their_windows() {
        assert_eq!(
            FLOW_WINDOWS.map(|(timeframe, _)| timeframe),
            [Timeframe::M5, Timeframe::M15, Timeframe::H1]
        );
        for (timeframe, definition) in FLOW_WINDOWS {
            assert_eq!(
                definition.key.id.as_str(),
                format!("flow.window.{}", timeframe.label()),
                "{timeframe:?}"
            );
            assert_eq!(
                definition.params,
                &[
                    Param {
                        name: "large_notional_usdt",
                        value: ParamValue::Int(100_000),
                    },
                    Param {
                        name: "window_ms",
                        value: ParamValue::Int(timeframe.millis()),
                    },
                ],
                "{timeframe:?}"
            );
            assert_eq!(
                definition.inputs,
                &[
                    Input::Stream(Stream::Trades),
                    Input::Feature(BARS_TIME_1M_V1.key)
                ]
            );
            let minutes = u32::try_from(timeframe.millis() / Timeframe::M1.millis()).unwrap();
            assert_eq!(
                definition.warm_up,
                WarmUp::Samples(minutes),
                "{timeframe:?}"
            );
        }
        assert_eq!(
            FLOW_CVD_CONTINUOUS_V1.inputs,
            &[Input::Stream(Stream::Trades)]
        );
        assert_eq!(
            FLOW_CVD_UTC_DAY_V1.inputs,
            &[Input::Feature(BARS_TIME_1D_V1.key)]
        );
        assert_eq!(
            FLOW_CVD_UTC_DAY_V1.params,
            &[Param {
                name: "session_ms",
                value: ParamValue::Int(Timeframe::D1.millis()),
            }]
        );
        let flow = [
            &FLOW_CVD_CONTINUOUS_V1,
            &FLOW_CVD_UTC_DAY_V1,
            &FLOW_WINDOW_5M_V1,
            &FLOW_WINDOW_15M_V1,
            &FLOW_WINDOW_1H_V1,
        ];
        for definition in flow {
            assert!(definition.key.id.as_str().starts_with("flow."));
            assert!(CURRENT.contains(&definition.key), "{}", definition.key);
        }
    }

    #[test]
    fn current_set_version_is_pinned() {
        // The feature-set version every experiment records (ADR-029). It may
        // change only when CURRENT does — any other change here means a
        // published definition or the set encoding moved.
        let set = current_set();
        assert_eq!(
            set.to_string(),
            "bars.motion.15m@1,bars.motion.1d@1,bars.motion.1h@1,bars.motion.1m@1,\
             bars.motion.4h@1,bars.motion.5m@1,bars.time.15m@1,bars.time.1d@1,\
             bars.time.1h@1,bars.time.1m@1,bars.time.4h@1,bars.time.5m@1,\
             flow.cvd.continuous@1,flow.cvd.utc_day@1,flow.window.15m@1,\
             flow.window.1h@1,flow.window.5m@1,profile.volume.composite_5d@1,\
             profile.volume.prior_day@1,profile.volume.utc_day@1,trade.last_price@1,\
             volatility.atr.1h@1,volatility.regime.1h@1"
        );
        assert_eq!(set.version().to_string(), "cdad39bf5282bd8a");
    }
}
