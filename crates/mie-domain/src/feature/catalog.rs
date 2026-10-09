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
use crate::num::{Price, Rate, SCALE};

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

/// A `structure.swing.<label>@1` or `structure.levels.<label>@1`
/// definition (ADR-037): both warm up over seven closed bars of their
/// timeframe, the window of one swing.
const fn structure_v1(
    id: &'static str,
    params: &'static [Param],
    inputs: &'static [Input],
) -> FeatureDefinition {
    FeatureDefinition {
        key: FeatureKey::new(id, 1),
        params,
        inputs,
        warm_up: WarmUp::Samples(7),
    }
}

/// `structure.swing.15m@1`: the last confirmed swing high and swing low of
/// the closed 15m bars (ADR-037, decisions 2–4).
///
/// - Parameters: `swing_bars` = 3 (a swing is a fractal with 3 bars on each
///   side), `tie_rule` = `strict_left_weak_right` (a plateau of equal
///   extremes yields one swing, at its first bar), `timeframe_ms` =
///   900 000.
/// - Inputs: `bars.time.15m@1`.
/// - Warm-up: 7 samples, where a sample is a closed 15m bar. A swing is
///   confirmed, and only then visible, when the third bar after it closes.
/// - Gap policy: nothing resets on a gap. A swing carries the OR of the
///   coverage of its seven window bars; empty bars never qualify or
///   disqualify a swing.
pub const STRUCTURE_SWING_15M_V1: FeatureDefinition = structure_v1(
    "structure.swing.15m",
    &[
        Param {
            name: "swing_bars",
            value: ParamValue::Int(3),
        },
        Param {
            name: "tie_rule",
            value: ParamValue::Text("strict_left_weak_right"),
        },
        Param {
            name: "timeframe_ms",
            value: ParamValue::Int(900_000),
        },
    ],
    &[Input::Feature(BARS_TIME_15M_V1.key)],
);

/// `structure.swing.1h@1`: the last confirmed swing high and swing low of
/// the closed 1h bars (ADR-037).
///
/// Parameters: `swing_bars` = 3, `tie_rule` = `strict_left_weak_right`,
/// `timeframe_ms` = 3 600 000. Input `bars.time.1h@1`. Warm-up (7 closed 1h
/// bars) and gap policy as [`STRUCTURE_SWING_15M_V1`].
pub const STRUCTURE_SWING_1H_V1: FeatureDefinition = structure_v1(
    "structure.swing.1h",
    &[
        Param {
            name: "swing_bars",
            value: ParamValue::Int(3),
        },
        Param {
            name: "tie_rule",
            value: ParamValue::Text("strict_left_weak_right"),
        },
        Param {
            name: "timeframe_ms",
            value: ParamValue::Int(3_600_000),
        },
    ],
    &[Input::Feature(BARS_TIME_1H_V1.key)],
);

/// `structure.swing.4h@1`: the last confirmed swing high and swing low of
/// the closed 4h bars (ADR-037).
///
/// Parameters: `swing_bars` = 3, `tie_rule` = `strict_left_weak_right`,
/// `timeframe_ms` = 14 400 000. Input `bars.time.4h@1`. Warm-up (7 closed 4h
/// bars) and gap policy as [`STRUCTURE_SWING_15M_V1`].
pub const STRUCTURE_SWING_4H_V1: FeatureDefinition = structure_v1(
    "structure.swing.4h",
    &[
        Param {
            name: "swing_bars",
            value: ParamValue::Int(3),
        },
        Param {
            name: "tie_rule",
            value: ParamValue::Text("strict_left_weak_right"),
        },
        Param {
            name: "timeframe_ms",
            value: ParamValue::Int(14_400_000),
        },
    ],
    &[Input::Feature(BARS_TIME_4H_V1.key)],
);

/// `structure.swing.1d@1`: the last confirmed swing high and swing low of
/// the closed 1d bars (ADR-037).
///
/// Parameters: `swing_bars` = 3, `tie_rule` = `strict_left_weak_right`,
/// `timeframe_ms` = 86 400 000. Input `bars.time.1d@1`. Warm-up (7 closed 1d
/// bars) and gap policy as [`STRUCTURE_SWING_15M_V1`].
pub const STRUCTURE_SWING_1D_V1: FeatureDefinition = structure_v1(
    "structure.swing.1d",
    &[
        Param {
            name: "swing_bars",
            value: ParamValue::Int(3),
        },
        Param {
            name: "tie_rule",
            value: ParamValue::Text("strict_left_weak_right"),
        },
        Param {
            name: "timeframe_ms",
            value: ParamValue::Int(86_400_000),
        },
    ],
    &[Input::Feature(BARS_TIME_1D_V1.key)],
);

/// `structure.levels.15m@1`: the structural level registry of the 15m
/// bars (ADR-037, decisions 5–8): the active swing highs and lows with
/// their touches, the levels swept by a trade, and whether each sweep
/// turned into a swing-failure pattern (SFP) or a clean break. An SFP is a
/// structure fact, never a signal (ADR-012, ADR-024).
///
/// - Parameters: `max_levels` = 20 (per list: active highs, active lows,
///   resolved sweeps; the oldest is evicted), `sfp_rule` =
///   `close_at_or_inside`, `sfp_window_bars` = 2 (bars of this timeframe
///   from the one containing the sweep trade), `sweep_rule` =
///   `trade_through_strict` (a trade strictly beyond the level),
///   `timeframe_ms` = 900 000, `touch_tolerance_bps` = 5.
/// - Inputs: trades (the sweeps), `bars.time.15m@1` (touches and SFP
///   windows) and `structure.swing.15m@1` (the levels).
/// - Warm-up: 7 samples, where a sample is a closed 15m bar, as the swings.
/// - Gap policy: nothing resets on a gap. A trade-through inside a trades
///   gap is unobservable; sweeps and resolutions carry the OR of the
///   coverage of the bars they read.
pub const STRUCTURE_LEVELS_15M_V1: FeatureDefinition = structure_v1(
    "structure.levels.15m",
    &[
        Param {
            name: "max_levels",
            value: ParamValue::Int(20),
        },
        Param {
            name: "sfp_rule",
            value: ParamValue::Text("close_at_or_inside"),
        },
        Param {
            name: "sfp_window_bars",
            value: ParamValue::Int(2),
        },
        Param {
            name: "sweep_rule",
            value: ParamValue::Text("trade_through_strict"),
        },
        Param {
            name: "timeframe_ms",
            value: ParamValue::Int(900_000),
        },
        Param {
            name: "touch_tolerance_bps",
            value: ParamValue::Int(5),
        },
    ],
    &[
        Input::Stream(Stream::Trades),
        Input::Feature(BARS_TIME_15M_V1.key),
        Input::Feature(STRUCTURE_SWING_15M_V1.key),
    ],
);

/// `structure.levels.1h@1`: the structural level registry of the 1h
/// bars, with touches, sweeps and SFPs (ADR-037).
///
/// Parameters: as [`STRUCTURE_LEVELS_15M_V1`] with `timeframe_ms` =
/// 3 600 000. Inputs trades, `bars.time.1h@1` and `structure.swing.1h@1`.
/// Warm-up (7 closed 1h bars) and gap policy as
/// [`STRUCTURE_LEVELS_15M_V1`].
pub const STRUCTURE_LEVELS_1H_V1: FeatureDefinition = structure_v1(
    "structure.levels.1h",
    &[
        Param {
            name: "max_levels",
            value: ParamValue::Int(20),
        },
        Param {
            name: "sfp_rule",
            value: ParamValue::Text("close_at_or_inside"),
        },
        Param {
            name: "sfp_window_bars",
            value: ParamValue::Int(2),
        },
        Param {
            name: "sweep_rule",
            value: ParamValue::Text("trade_through_strict"),
        },
        Param {
            name: "timeframe_ms",
            value: ParamValue::Int(3_600_000),
        },
        Param {
            name: "touch_tolerance_bps",
            value: ParamValue::Int(5),
        },
    ],
    &[
        Input::Stream(Stream::Trades),
        Input::Feature(BARS_TIME_1H_V1.key),
        Input::Feature(STRUCTURE_SWING_1H_V1.key),
    ],
);

/// `structure.levels.4h@1`: the structural level registry of the 4h
/// bars, with touches, sweeps and SFPs (ADR-037).
///
/// Parameters: as [`STRUCTURE_LEVELS_15M_V1`] with `timeframe_ms` =
/// 14 400 000. Inputs trades, `bars.time.4h@1` and `structure.swing.4h@1`.
/// Warm-up (7 closed 4h bars) and gap policy as
/// [`STRUCTURE_LEVELS_15M_V1`].
pub const STRUCTURE_LEVELS_4H_V1: FeatureDefinition = structure_v1(
    "structure.levels.4h",
    &[
        Param {
            name: "max_levels",
            value: ParamValue::Int(20),
        },
        Param {
            name: "sfp_rule",
            value: ParamValue::Text("close_at_or_inside"),
        },
        Param {
            name: "sfp_window_bars",
            value: ParamValue::Int(2),
        },
        Param {
            name: "sweep_rule",
            value: ParamValue::Text("trade_through_strict"),
        },
        Param {
            name: "timeframe_ms",
            value: ParamValue::Int(14_400_000),
        },
        Param {
            name: "touch_tolerance_bps",
            value: ParamValue::Int(5),
        },
    ],
    &[
        Input::Stream(Stream::Trades),
        Input::Feature(BARS_TIME_4H_V1.key),
        Input::Feature(STRUCTURE_SWING_4H_V1.key),
    ],
);

/// `structure.levels.1d@1`: the structural level registry of the 1d
/// bars, with touches, sweeps and SFPs (ADR-037).
///
/// Parameters: as [`STRUCTURE_LEVELS_15M_V1`] with `timeframe_ms` =
/// 86 400 000. Inputs trades, `bars.time.1d@1` and `structure.swing.1d@1`.
/// Warm-up (7 closed 1d bars) and gap policy as
/// [`STRUCTURE_LEVELS_15M_V1`].
pub const STRUCTURE_LEVELS_1D_V1: FeatureDefinition = structure_v1(
    "structure.levels.1d",
    &[
        Param {
            name: "max_levels",
            value: ParamValue::Int(20),
        },
        Param {
            name: "sfp_rule",
            value: ParamValue::Text("close_at_or_inside"),
        },
        Param {
            name: "sfp_window_bars",
            value: ParamValue::Int(2),
        },
        Param {
            name: "sweep_rule",
            value: ParamValue::Text("trade_through_strict"),
        },
        Param {
            name: "timeframe_ms",
            value: ParamValue::Int(86_400_000),
        },
        Param {
            name: "touch_tolerance_bps",
            value: ParamValue::Int(5),
        },
    ],
    &[
        Input::Stream(Stream::Trades),
        Input::Feature(BARS_TIME_1D_V1.key),
        Input::Feature(STRUCTURE_SWING_1D_V1.key),
    ],
);

/// The swing feature of each structure timeframe, shortest first; the
/// Market State builds its [`StructureSet`](crate::structure::StructureSet)
/// from it with [`STRUCTURE_LEVELS`].
pub const STRUCTURE_SWING: [(Timeframe, &FeatureDefinition); 4] = [
    (Timeframe::M15, &STRUCTURE_SWING_15M_V1),
    (Timeframe::H1, &STRUCTURE_SWING_1H_V1),
    (Timeframe::H4, &STRUCTURE_SWING_4H_V1),
    (Timeframe::D1, &STRUCTURE_SWING_1D_V1),
];

/// The level-registry feature of each structure timeframe, shortest first.
pub const STRUCTURE_LEVELS: [(Timeframe, &FeatureDefinition); 4] = [
    (Timeframe::M15, &STRUCTURE_LEVELS_15M_V1),
    (Timeframe::H1, &STRUCTURE_LEVELS_1H_V1),
    (Timeframe::H4, &STRUCTURE_LEVELS_4H_V1),
    (Timeframe::D1, &STRUCTURE_LEVELS_1D_V1),
];

/// `derivatives.oi.sample@1`: the last open-interest sample at its source
/// resolution, with the exact step (ΔOI, elapsed time) against the previous
/// sample (ADR-042, decision 1). OI velocity is derived from the step.
///
/// - Parameters: `gap_policy` = `break_chain`, `step_tolerance_ms` =
///   15 000.
/// - Inputs: open interest (samples and open-interest feed gaps).
/// - Warm-up: one sample, where a sample is an open-interest event.
/// - Gap policy: the level stays ready. An open-interest gap breaks the
///   chain: the next sample has no step. So does a change of
///   `resolution_ms` or an elapsed time outside `(0, resolution_ms +
///   step_tolerance_ms]`.
pub const DERIVATIVES_OI_SAMPLE_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("derivatives.oi.sample", 1),
    params: &[
        Param {
            name: "gap_policy",
            value: ParamValue::Text("break_chain"),
        },
        Param {
            name: "step_tolerance_ms",
            value: ParamValue::Int(15_000),
        },
    ],
    inputs: &[Input::Stream(Stream::OpenInterest)],
    warm_up: WarmUp::Samples(1),
};

/// `derivatives.oi.5m@1`: open interest on the UTC 5-minute grid — the
/// latest sample at or before each boundary, with ΔOI against the previous
/// boundary (ADR-042, decision 2). Live (10 s) and archive (5 min) samples
/// both produce it; the value keeps the source `resolution_ms`.
///
/// - Parameters: `grid_ms` = 300 000, `max_age_ms` = 60 000.
/// - Inputs: open interest.
/// - Warm-up: one sample, where a sample is a closed boundary. Boundaries
///   close on open-interest events only.
/// - Gap policy: no separate rule. A boundary whose sample is older than
///   `max_age_ms` is `Unavailable(InputInvalid)`, which is what a boundary
///   after a feed hole becomes; the delta needs the previous boundary,
///   ready and at the same resolution.
pub const DERIVATIVES_OI_5M_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("derivatives.oi.5m", 1),
    params: &[
        Param {
            name: "grid_ms",
            value: ParamValue::Int(300_000),
        },
        Param {
            name: "max_age_ms",
            value: ParamValue::Int(60_000),
        },
    ],
    inputs: &[Input::Stream(Stream::OpenInterest)],
    warm_up: WarmUp::Samples(1),
};

/// `derivatives.mark@1`: the last mark and index price with the indicative
/// funding rate and the next funding time (ADR-042, decision 3). Basis and
/// the time to the next funding are derived exactly.
///
/// - Inputs: mark price (live only).
/// - Warm-up: one sample, where a sample is a mark-price event.
/// - Gap policy: none — a feed gap leaves the value, which keeps its own
///   time.
pub const DERIVATIVES_MARK_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("derivatives.mark", 1),
    params: &[],
    inputs: &[Input::Stream(Stream::MarkPrice)],
    warm_up: WarmUp::Samples(1),
};

/// `derivatives.funding.settled@1`: the last settled funding rate (ADR-042,
/// decision 4).
///
/// - Inputs: funding settlements (archive only today).
/// - Warm-up: one sample, where a sample is a funding settlement.
/// - Gap policy: none — a feed gap leaves the value, which keeps its own
///   time.
pub const DERIVATIVES_FUNDING_SETTLED_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("derivatives.funding.settled", 1),
    params: &[],
    inputs: &[Input::Stream(Stream::Funding)],
    warm_up: WarmUp::Samples(1),
};

/// A `derivatives.liq.window.<label>@1` definition (ADR-042, decision 5):
/// liquidations by side over the last `minutes` closed 1m bars.
const fn liq_window_v1(
    id: &'static str,
    params: &'static [Param],
    minutes: u32,
) -> FeatureDefinition {
    FeatureDefinition {
        key: FeatureKey::new(id, 1),
        params,
        inputs: &[
            Input::Stream(Stream::Liquidations),
            Input::Feature(BARS_TIME_1M_V1.key),
        ],
        warm_up: WarmUp::Samples(minutes),
    }
}

/// `derivatives.liq.window.5m@1`: liquidation count and filled quantity by
/// side over the last 5 closed 1m bars (ADR-042, decision 5). A `Sell`
/// liquidation order closes a long, a `Buy` order a short. Every value is a
/// lower bound: the exchange stream is throttled.
///
/// - Parameters: `window_ms` = 300 000.
/// - Inputs: liquidations and `bars.time.1m@1` (the clock).
/// - Warm-up: 5 samples, where a sample is a closed 1m bar from the minute
///   of the first liquidations-stream event (a liquidation or a gap) on.
///   Until that event the window warms up with 0 samples: the archive has
///   no liquidations, and its zeros are not a quiet market.
/// - Gap policy: the window never goes back to warming up. A liquidations
///   gap flags every minute it overlaps with `feed_gap`, also minutes that
///   already closed; the window carries the OR of its minutes' flags, and
///   the first minute is `partial_start`.
pub const DERIVATIVES_LIQ_WINDOW_5M_V1: FeatureDefinition = liq_window_v1(
    "derivatives.liq.window.5m",
    &[Param {
        name: "window_ms",
        value: ParamValue::Int(300_000),
    }],
    5,
);

/// `derivatives.liq.window.15m@1`: liquidations by side over the last 15
/// closed 1m bars (ADR-042, decision 5). A lower bound.
///
/// Parameters: `window_ms` = 900 000. Warm-up: 15 samples (closed 1m bars
/// from the first liquidations-stream event on). Inputs and gap policy as
/// [`DERIVATIVES_LIQ_WINDOW_5M_V1`].
pub const DERIVATIVES_LIQ_WINDOW_15M_V1: FeatureDefinition = liq_window_v1(
    "derivatives.liq.window.15m",
    &[Param {
        name: "window_ms",
        value: ParamValue::Int(900_000),
    }],
    15,
);

/// `derivatives.liq.window.1h@1`: liquidations by side over the last 60
/// closed 1m bars (ADR-042, decision 5). A lower bound.
///
/// Parameters: `window_ms` = 3 600 000. Warm-up: 60 samples (closed 1m bars
/// from the first liquidations-stream event on). Inputs and gap policy as
/// [`DERIVATIVES_LIQ_WINDOW_5M_V1`].
pub const DERIVATIVES_LIQ_WINDOW_1H_V1: FeatureDefinition = liq_window_v1(
    "derivatives.liq.window.1h",
    &[Param {
        name: "window_ms",
        value: ParamValue::Int(3_600_000),
    }],
    60,
);

/// The liquidation window of each length, shortest first; the Market State
/// builds its [`LiquidationWindows`](crate::derivatives::LiquidationWindows)
/// from it. The timeframe is the window length, not a bar series.
pub const LIQ_WINDOWS: [(Timeframe, &FeatureDefinition); 3] = [
    (Timeframe::M5, &DERIVATIVES_LIQ_WINDOW_5M_V1),
    (Timeframe::M15, &DERIVATIVES_LIQ_WINDOW_15M_V1),
    (Timeframe::H1, &DERIVATIVES_LIQ_WINDOW_1H_V1),
];

/// `book.l2@1`: the L2 order book rebuilt from snapshots and updates
/// (ADR-038 D6, ADR-043 D1): every level, the update-id chain and the
/// trusted window.
///
/// - Inputs: order book (snapshots, updates and order-book feed gaps).
/// - Warm-up: one sample, where a sample is a snapshot. Live only: archive
///   replays have no order book and keep it warming up (ADR-043 D11).
/// - Gap policy: an order-book gap, or an update that breaks the chain or
///   carries a negative quantity, makes it `Unavailable(InputInvalid)` until
///   the next snapshot builds a fresh book. A snapshot is a reset, never
///   flow (ADR-038 D6).
pub const BOOK_L2_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("book.l2", 1),
    params: &[],
    inputs: &[Input::Stream(Stream::OrderBook)],
    warm_up: WarmUp::Samples(1),
};

/// The depth bands of the order-book features, innermost first: 1, 2 and 5
/// bps of mid as [`Rate`] units (1 bps = 10 000 units), cumulative and
/// inclusive (ADR-043 D2).
pub const BOOK_BANDS: [Rate; 3] = [
    Rate::from_units(10_000),
    Rate::from_units(20_000),
    Rate::from_units(50_000),
];

/// The band parameters every banded book feature carries.
const BOOK_BAND_PARAMS: [Param; 3] = [
    Param {
        name: "band_1",
        value: ParamValue::Rate(BOOK_BANDS[0]),
    },
    Param {
        name: "band_2",
        value: ParamValue::Rate(BOOK_BANDS[1]),
    },
    Param {
        name: "band_3",
        value: ParamValue::Rate(BOOK_BANDS[2]),
    },
];

/// `book.depth@1`: best bid and ask, and per band the resting bid and ask
/// quantity and level count within 1, 2 and 5 bps of mid (ADR-043 D2–D4).
/// Imbalance `(bid − ask) / (bid + ask)` is derived on demand.
///
/// - Parameters: `band_1` = 0.0001, `band_2` = 0.0002, `band_3` = 0.0005
///   (`Rate`).
/// - Inputs: `book.l2@1`.
/// - Warm-up: one sample, where a sample is a snapshot (through
///   `book.l2@1`). Recomputed after every order-book event.
/// - Gap policy: `Unavailable(InputInvalid)` while the book is not ready,
///   one-sided or crossed. A band whose far edge lies beyond the trusted
///   window on either side, or whose sum leaves the `Qty` range, is
///   `Unavailable(OutOfRange)`.
pub const BOOK_DEPTH_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("book.depth", 1),
    params: &BOOK_BAND_PARAMS,
    inputs: &[Input::Feature(BOOK_L2_V1.key)],
    warm_up: WarmUp::Samples(1),
};

/// `book.clusters@1`: per side within 5 bps of mid, the 5 largest levels
/// (ties to the level nearer mid), the lower-median level quantity, the
/// total quantity and the level count (ADR-043 D8). Multiples of the median
/// are derived on demand; there is no cluster threshold (#23 owns it).
///
/// - Parameters: `band` = 0.0005 (`Rate`), `top_k` = 5.
/// - Inputs: `book.l2@1`.
/// - Warm-up: one sample, where a sample is a snapshot (through
///   `book.l2@1`).
/// - Gap policy: `Unavailable(InputInvalid)` while the book is not ready,
///   one-sided or crossed; a side whose band edge lies beyond the trusted
///   window is `Unavailable(OutOfRange)`.
pub const BOOK_CLUSTERS_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("book.clusters", 1),
    params: &[
        Param {
            name: "band",
            value: ParamValue::Rate(BOOK_BANDS[2]),
        },
        Param {
            name: "top_k",
            value: ParamValue::Int(5),
        },
    ],
    inputs: &[Input::Feature(BOOK_L2_V1.key)],
    warm_up: WarmUp::Samples(1),
};

/// A `book.liquidity.window.<label>@1` definition (ADR-043 D5, D6): passive
/// liquidity added, cancelled and filled per side and band over the last
/// `minutes` closed 1m bars.
const fn book_liquidity_window_v1(
    id: &'static str,
    params: &'static [Param],
    minutes: u32,
) -> FeatureDefinition {
    FeatureDefinition {
        key: FeatureKey::new(id, 1),
        params,
        inputs: &[
            Input::Feature(BOOK_L2_V1.key),
            Input::Stream(Stream::Trades),
            Input::Feature(BARS_TIME_1M_V1.key),
        ],
        warm_up: WarmUp::Samples(minutes),
    }
}

/// `book.liquidity.window.5m@1`: liquidity added, cancelled and filled per
/// side and band (1, 2, 5 bps of mid) over the last 5 closed 1m bars
/// (ADR-043 D5, D6). A decrease at a level is matched with the taker trades
/// at its price, carried for one update; a fill the book never showed as a
/// decrease is inferred as added and filled and counted in `inferred`.
/// `added` and `cancelled` are lower bounds.
///
/// - Parameters: `band_1` = 0.0001, `band_2` = 0.0002, `band_3` = 0.0005
///   (`Rate`), `carry_updates` = 1, `window_ms` = 300 000.
/// - Inputs: `book.l2@1`, trades (the fills) and `bars.time.1m@1` (the
///   clock, ADR-035 D1).
/// - Warm-up: 5 samples, where a sample is a closed 1m bar from the minute
///   of the first valid book on. Archive replays have no book and stay
///   warming up.
/// - Gap policy: the window never goes back to warming up. An order-book or
///   trades gap flags every minute it overlaps with `feed_gap`, also minutes
///   that already closed; so does a book event that finds or leaves the book
///   unavailable. A snapshot books no flow and drops the pending fills. The
///   window carries the OR of its minutes' flags (`feed_gap`, per band
///   `beyond_window`), and its first minute is `partial_start`.
pub const BOOK_LIQUIDITY_WINDOW_5M_V1: FeatureDefinition = book_liquidity_window_v1(
    "book.liquidity.window.5m",
    &[
        BOOK_BAND_PARAMS[0],
        BOOK_BAND_PARAMS[1],
        BOOK_BAND_PARAMS[2],
        Param {
            name: "carry_updates",
            value: ParamValue::Int(1),
        },
        Param {
            name: "window_ms",
            value: ParamValue::Int(300_000),
        },
    ],
    5,
);

/// `book.liquidity.window.15m@1`: order-book liquidity flow over the last
/// 15 closed 1m bars (ADR-043 D5, D6).
///
/// Parameters: bands and `carry_updates` as [`BOOK_LIQUIDITY_WINDOW_5M_V1`],
/// `window_ms` = 900 000. Warm-up: 15 samples (closed 1m bars from the first
/// valid book on). Inputs and gap policy as
/// [`BOOK_LIQUIDITY_WINDOW_5M_V1`].
pub const BOOK_LIQUIDITY_WINDOW_15M_V1: FeatureDefinition = book_liquidity_window_v1(
    "book.liquidity.window.15m",
    &[
        BOOK_BAND_PARAMS[0],
        BOOK_BAND_PARAMS[1],
        BOOK_BAND_PARAMS[2],
        Param {
            name: "carry_updates",
            value: ParamValue::Int(1),
        },
        Param {
            name: "window_ms",
            value: ParamValue::Int(900_000),
        },
    ],
    15,
);

/// `book.liquidity.window.1h@1`: order-book liquidity flow over the last 60
/// closed 1m bars (ADR-043 D5, D6).
///
/// Parameters: bands and `carry_updates` as [`BOOK_LIQUIDITY_WINDOW_5M_V1`],
/// `window_ms` = 3 600 000. Warm-up: 60 samples (closed 1m bars from the
/// first valid book on). Inputs and gap policy as
/// [`BOOK_LIQUIDITY_WINDOW_5M_V1`].
pub const BOOK_LIQUIDITY_WINDOW_1H_V1: FeatureDefinition = book_liquidity_window_v1(
    "book.liquidity.window.1h",
    &[
        BOOK_BAND_PARAMS[0],
        BOOK_BAND_PARAMS[1],
        BOOK_BAND_PARAMS[2],
        Param {
            name: "carry_updates",
            value: ParamValue::Int(1),
        },
        Param {
            name: "window_ms",
            value: ParamValue::Int(3_600_000),
        },
    ],
    60,
);

/// The order-book liquidity window of each length, shortest first; the
/// Market State builds its
/// [`LiquidityWindows`](crate::liquidity::LiquidityWindows) from it. The
/// timeframe is the window length, not a bar series.
pub const BOOK_LIQUIDITY_WINDOWS: [(Timeframe, &FeatureDefinition); 3] = [
    (Timeframe::M5, &BOOK_LIQUIDITY_WINDOW_5M_V1),
    (Timeframe::M15, &BOOK_LIQUIDITY_WINDOW_15M_V1),
    (Timeframe::H1, &BOOK_LIQUIDITY_WINDOW_1H_V1),
];

/// `location.vwap.utc_day@1`: the volume-weighted average price of the
/// current UTC day, `Σ(price · qty) / Σqty` over the trades of its closed
/// minutes (ADR-044 D4). The sums are exact `i128`; the mean is floored to
/// 1e-8 USDT. No bands, no prior-day VWAP.
///
/// - Parameters: `rounding` = `floor`, `session_ms` = 86 400 000.
/// - Inputs: trades (the sums) and `bars.time.1m@1` (the clock and the
///   coverage).
/// - Warm-up: one sample, where a sample is a closed 1m bar of the current
///   UTC day with volume. It restarts at 00:00 UTC; the developing minute is
///   never included.
/// - Gap policy: a trades gap never resets the sums. The value carries the
///   OR of the day's closed minutes' coverage.
pub const LOCATION_VWAP_UTC_DAY_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("location.vwap.utc_day", 1),
    params: &[
        Param {
            name: "rounding",
            value: ParamValue::Text("floor"),
        },
        Param {
            name: "session_ms",
            value: ParamValue::Int(86_400_000),
        },
    ],
    inputs: &[
        Input::Stream(Stream::Trades),
        Input::Feature(BARS_TIME_1M_V1.key),
    ],
    warm_up: WarmUp::Samples(1),
};

/// The location tolerance `w` = 5 bps as a `Rate` (ADR-044 D3): parameter
/// `tolerance` of `location.levels@1` and `location.auction.prior_day@1`.
pub const LOCATION_TOLERANCE: Rate = Rate::from_units(50_000);

/// `location.levels@1`: the level registry (ADR-044 D5–D7), rebuilt at each
/// closed 1m bar with trades from the POC, VAL, VAH, HVNs and LVNs of the
/// prior-day and 5-day composite profiles, the structural highs and lows,
/// prior sweeps and SFP rejection zones of every structure timeframe, the
/// top-5 cluster candidates per book side and the UTC-day VWAP. Each level
/// carries its score components: signed distance to the close, touches
/// (entries since registration), registration time, source, confluence
/// (levels from other sources within the tolerance) and, for a cluster, its
/// quantity and side median. A level is in zone while the closed bar's range
/// overlaps its zone padded by the tolerance; entering and leaving are
/// location events. Clusters are not monitored.
///
/// - Parameters: `confluence_rule` = `other_source_within_tolerance`,
///   `tolerance` = 0.0005 (`Rate`), `zone_rule` = `bar_range_overlap`.
/// - Inputs: `profile.volume.prior_day@1`, `profile.volume.composite_5d@1`,
///   `structure.levels.<15m|1h|4h|1d>@1`, `book.clusters@1`,
///   `location.vwap.utc_day@1` and `bars.time.1m@1` (the clock).
/// - Warm-up: one sample, where a sample is a closed 1m bar with trades.
/// - Gap policy: an empty bar neither counts nor changes anything; a source
///   that is not ready contributes no level, and its levels retire.
pub const LOCATION_LEVELS_V1: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("location.levels", 1),
    params: &[
        Param {
            name: "confluence_rule",
            value: ParamValue::Text("other_source_within_tolerance"),
        },
        Param {
            name: "tolerance",
            value: ParamValue::Rate(LOCATION_TOLERANCE),
        },
        Param {
            name: "zone_rule",
            value: ParamValue::Text("bar_range_overlap"),
        },
    ],
    inputs: &[
        Input::Feature(PROFILE_VOLUME_PRIOR_DAY_V1.key),
        Input::Feature(PROFILE_VOLUME_COMPOSITE_5D_V1.key),
        Input::Feature(STRUCTURE_LEVELS_15M_V1.key),
        Input::Feature(STRUCTURE_LEVELS_1H_V1.key),
        Input::Feature(STRUCTURE_LEVELS_4H_V1.key),
        Input::Feature(STRUCTURE_LEVELS_1D_V1.key),
        Input::Feature(BOOK_CLUSTERS_V1.key),
        Input::Feature(LOCATION_VWAP_UTC_DAY_V1.key),
        Input::Feature(BARS_TIME_1M_V1.key),
    ],
    warm_up: WarmUp::Samples(1),
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
    &STRUCTURE_SWING_15M_V1,
    &STRUCTURE_SWING_1H_V1,
    &STRUCTURE_SWING_4H_V1,
    &STRUCTURE_SWING_1D_V1,
    &STRUCTURE_LEVELS_15M_V1,
    &STRUCTURE_LEVELS_1H_V1,
    &STRUCTURE_LEVELS_4H_V1,
    &STRUCTURE_LEVELS_1D_V1,
    &DERIVATIVES_OI_SAMPLE_V1,
    &DERIVATIVES_OI_5M_V1,
    &DERIVATIVES_MARK_V1,
    &DERIVATIVES_FUNDING_SETTLED_V1,
    &DERIVATIVES_LIQ_WINDOW_5M_V1,
    &DERIVATIVES_LIQ_WINDOW_15M_V1,
    &DERIVATIVES_LIQ_WINDOW_1H_V1,
    &BOOK_L2_V1,
    &BOOK_DEPTH_V1,
    &BOOK_CLUSTERS_V1,
    &BOOK_LIQUIDITY_WINDOW_5M_V1,
    &BOOK_LIQUIDITY_WINDOW_15M_V1,
    &BOOK_LIQUIDITY_WINDOW_1H_V1,
    &LOCATION_VWAP_UTC_DAY_V1,
    &LOCATION_LEVELS_V1,
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
    LockEntry::new("structure.swing.15m", 1, 0x0c13_e804_592f_2c14),
    LockEntry::new("structure.swing.1h", 1, 0x8ede_32da_52d7_b4f0),
    LockEntry::new("structure.swing.4h", 1, 0x3d95_0ad1_de6d_6fdd),
    LockEntry::new("structure.swing.1d", 1, 0xfdb7_ceac_b72a_1f5d),
    LockEntry::new("structure.levels.15m", 1, 0xdbcd_67b3_c804_62e6),
    LockEntry::new("structure.levels.1h", 1, 0x8dca_ac85_b01e_2e17),
    LockEntry::new("structure.levels.4h", 1, 0x9e80_e15b_4382_0c99),
    LockEntry::new("structure.levels.1d", 1, 0xb81f_aa9e_a38e_5d88),
    LockEntry::new("derivatives.oi.sample", 1, 0x0306_c571_c734_0c06),
    LockEntry::new("derivatives.oi.5m", 1, 0x0323_23ab_6e3e_fd77),
    LockEntry::new("derivatives.mark", 1, 0xe582_9004_7f9c_145d),
    LockEntry::new("derivatives.funding.settled", 1, 0x593b_4c94_d6cc_22d0),
    LockEntry::new("derivatives.liq.window.5m", 1, 0xe58f_6c17_aae9_6808),
    LockEntry::new("derivatives.liq.window.15m", 1, 0x08b6_f494_da9d_36ed),
    LockEntry::new("derivatives.liq.window.1h", 1, 0xf8bf_c670_4109_615b),
    LockEntry::new("book.l2", 1, 0x1540_06c4_5c86_8fe2),
    LockEntry::new("book.depth", 1, 0xd411_5d7e_28bc_ebc6),
    LockEntry::new("book.clusters", 1, 0x0fef_026f_910e_b2aa),
    LockEntry::new("book.liquidity.window.5m", 1, 0x393d_380d_74a3_3d99),
    LockEntry::new("book.liquidity.window.15m", 1, 0xe2f9_5033_82fc_e9c0),
    LockEntry::new("book.liquidity.window.1h", 1, 0xd4a5_f617_f61e_8388),
    LockEntry::new("location.vwap.utc_day", 1, 0x03e9_3688_148e_d1ac),
    LockEntry::new("location.levels", 1, 0x0fb0_3473_902a_b08b),
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
    STRUCTURE_SWING_15M_V1.key,
    STRUCTURE_SWING_1H_V1.key,
    STRUCTURE_SWING_4H_V1.key,
    STRUCTURE_SWING_1D_V1.key,
    STRUCTURE_LEVELS_15M_V1.key,
    STRUCTURE_LEVELS_1H_V1.key,
    STRUCTURE_LEVELS_4H_V1.key,
    STRUCTURE_LEVELS_1D_V1.key,
    DERIVATIVES_OI_SAMPLE_V1.key,
    DERIVATIVES_OI_5M_V1.key,
    DERIVATIVES_MARK_V1.key,
    DERIVATIVES_FUNDING_SETTLED_V1.key,
    DERIVATIVES_LIQ_WINDOW_5M_V1.key,
    DERIVATIVES_LIQ_WINDOW_15M_V1.key,
    DERIVATIVES_LIQ_WINDOW_1H_V1.key,
    BOOK_L2_V1.key,
    BOOK_DEPTH_V1.key,
    BOOK_CLUSTERS_V1.key,
    BOOK_LIQUIDITY_WINDOW_5M_V1.key,
    BOOK_LIQUIDITY_WINDOW_15M_V1.key,
    BOOK_LIQUIDITY_WINDOW_1H_V1.key,
    LOCATION_VWAP_UTC_DAY_V1.key,
    LOCATION_LEVELS_V1.key,
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
        assert_eq!(LOCK.len(), 46, "lock lines");
        assert_eq!(
            hasher.finish().to_string(),
            "f95a4ff310b2a6cc",
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
    fn structure_features_match_their_timeframes() {
        use crate::structure::{
            MAX_LEVELS, SFP_WINDOW_BARS, STRUCTURE_TIMEFRAMES, SWING_BARS, SWING_WINDOW,
            TOUCH_TOLERANCE_BPS,
        };
        let int = |value: usize| ParamValue::Int(i64::try_from(value).unwrap());
        assert_eq!(
            STRUCTURE_SWING.map(|(timeframe, _)| timeframe),
            STRUCTURE_TIMEFRAMES
        );
        assert_eq!(
            STRUCTURE_LEVELS.map(|(timeframe, _)| timeframe),
            STRUCTURE_TIMEFRAMES
        );
        let warm_up = WarmUp::Samples(u32::try_from(SWING_WINDOW).unwrap());
        for ((timeframe, swing), (_, levels)) in STRUCTURE_SWING.into_iter().zip(STRUCTURE_LEVELS) {
            let bars = BARS_TIME
                .iter()
                .find(|(bars, _)| *bars == timeframe)
                .unwrap()
                .1;
            let timeframe_ms = Param {
                name: "timeframe_ms",
                value: ParamValue::Int(timeframe.millis()),
            };
            assert_eq!(
                swing.key.id.as_str(),
                format!("structure.swing.{}", timeframe.label())
            );
            assert_eq!(
                swing.params,
                &[
                    Param {
                        name: "swing_bars",
                        value: int(SWING_BARS),
                    },
                    Param {
                        name: "tie_rule",
                        value: ParamValue::Text("strict_left_weak_right"),
                    },
                    timeframe_ms,
                ],
                "{timeframe:?}"
            );
            assert_eq!(swing.inputs, &[Input::Feature(bars.key)]);
            assert_eq!(swing.warm_up, warm_up);
            assert_eq!(
                levels.key.id.as_str(),
                format!("structure.levels.{}", timeframe.label())
            );
            assert_eq!(
                levels.params,
                &[
                    Param {
                        name: "max_levels",
                        value: int(MAX_LEVELS),
                    },
                    Param {
                        name: "sfp_rule",
                        value: ParamValue::Text("close_at_or_inside"),
                    },
                    Param {
                        name: "sfp_window_bars",
                        value: ParamValue::Int(i64::from(SFP_WINDOW_BARS)),
                    },
                    Param {
                        name: "sweep_rule",
                        value: ParamValue::Text("trade_through_strict"),
                    },
                    timeframe_ms,
                    Param {
                        name: "touch_tolerance_bps",
                        value: ParamValue::Int(TOUCH_TOLERANCE_BPS),
                    },
                ],
                "{timeframe:?}"
            );
            assert_eq!(
                levels.inputs,
                &[
                    Input::Stream(Stream::Trades),
                    Input::Feature(bars.key),
                    Input::Feature(swing.key),
                ]
            );
            assert_eq!(levels.warm_up, warm_up);
            assert!(CURRENT.contains(&swing.key), "{}", swing.key);
            assert!(CURRENT.contains(&levels.key), "{}", levels.key);
        }
    }

    #[test]
    fn derivatives_features_match_their_parameters() {
        assert_eq!(
            LIQ_WINDOWS.map(|(timeframe, _)| timeframe),
            [Timeframe::M5, Timeframe::M15, Timeframe::H1]
        );
        for (timeframe, definition) in LIQ_WINDOWS {
            assert_eq!(
                definition.key.id.as_str(),
                format!("derivatives.liq.window.{}", timeframe.label()),
                "{timeframe:?}"
            );
            assert_eq!(
                definition.params,
                &[Param {
                    name: "window_ms",
                    value: ParamValue::Int(timeframe.millis()),
                }],
                "{timeframe:?}"
            );
            assert_eq!(
                definition.inputs,
                &[
                    Input::Stream(Stream::Liquidations),
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
            DERIVATIVES_OI_SAMPLE_V1.params,
            &[
                Param {
                    name: "gap_policy",
                    value: ParamValue::Text("break_chain"),
                },
                Param {
                    name: "step_tolerance_ms",
                    value: ParamValue::Int(15_000),
                },
            ]
        );
        assert_eq!(
            DERIVATIVES_OI_5M_V1.params,
            &[
                Param {
                    name: "grid_ms",
                    value: ParamValue::Int(Timeframe::M5.millis()),
                },
                Param {
                    name: "max_age_ms",
                    value: ParamValue::Int(60_000),
                },
            ]
        );
        assert!(DERIVATIVES_MARK_V1.params.is_empty());
        assert!(DERIVATIVES_FUNDING_SETTLED_V1.params.is_empty());
        let streams = [
            (&DERIVATIVES_OI_SAMPLE_V1, Stream::OpenInterest),
            (&DERIVATIVES_OI_5M_V1, Stream::OpenInterest),
            (&DERIVATIVES_MARK_V1, Stream::MarkPrice),
            (&DERIVATIVES_FUNDING_SETTLED_V1, Stream::Funding),
        ];
        for (definition, stream) in streams {
            assert_eq!(definition.inputs, &[Input::Stream(stream)]);
            assert_eq!(definition.warm_up, WarmUp::Samples(1));
        }
        let derivatives = [
            &DERIVATIVES_OI_SAMPLE_V1,
            &DERIVATIVES_OI_5M_V1,
            &DERIVATIVES_MARK_V1,
            &DERIVATIVES_FUNDING_SETTLED_V1,
            &DERIVATIVES_LIQ_WINDOW_5M_V1,
            &DERIVATIVES_LIQ_WINDOW_15M_V1,
            &DERIVATIVES_LIQ_WINDOW_1H_V1,
        ];
        for definition in derivatives {
            assert!(definition.key.id.as_str().starts_with("derivatives."));
            assert_eq!(definition.key.version.get(), 1);
            assert!(CURRENT.contains(&definition.key), "{}", definition.key);
        }
    }

    #[test]
    fn book_features_match_their_parameters() {
        assert_eq!(
            BOOK_BANDS.map(Rate::units),
            [10_000, 20_000, 50_000],
            "1, 2 and 5 bps"
        );
        let bands = [
            Param {
                name: "band_1",
                value: ParamValue::Rate(Rate::from_units(10_000)),
            },
            Param {
                name: "band_2",
                value: ParamValue::Rate(Rate::from_units(20_000)),
            },
            Param {
                name: "band_3",
                value: ParamValue::Rate(Rate::from_units(50_000)),
            },
        ];
        assert!(BOOK_L2_V1.params.is_empty());
        assert_eq!(BOOK_L2_V1.inputs, &[Input::Stream(Stream::OrderBook)]);
        assert_eq!(BOOK_DEPTH_V1.params, &bands);
        assert_eq!(
            BOOK_CLUSTERS_V1.params,
            &[
                Param {
                    name: "band",
                    value: ParamValue::Rate(Rate::from_units(50_000)),
                },
                Param {
                    name: "top_k",
                    value: ParamValue::Int(5),
                },
            ]
        );
        for definition in [&BOOK_DEPTH_V1, &BOOK_CLUSTERS_V1] {
            assert_eq!(definition.inputs, &[Input::Feature(BOOK_L2_V1.key)]);
        }
        for definition in [&BOOK_L2_V1, &BOOK_DEPTH_V1, &BOOK_CLUSTERS_V1] {
            assert_eq!(definition.warm_up, WarmUp::Samples(1));
        }
        assert_eq!(
            BOOK_LIQUIDITY_WINDOWS.map(|(timeframe, _)| timeframe),
            [Timeframe::M5, Timeframe::M15, Timeframe::H1]
        );
        for (timeframe, definition) in BOOK_LIQUIDITY_WINDOWS {
            assert_eq!(
                definition.key.id.as_str(),
                format!("book.liquidity.window.{}", timeframe.label()),
                "{timeframe:?}"
            );
            let mut params = bands.to_vec();
            params.push(Param {
                name: "carry_updates",
                value: ParamValue::Int(1),
            });
            params.push(Param {
                name: "window_ms",
                value: ParamValue::Int(timeframe.millis()),
            });
            assert_eq!(definition.params, params.as_slice(), "{timeframe:?}");
            assert_eq!(
                definition.inputs,
                &[
                    Input::Feature(BOOK_L2_V1.key),
                    Input::Stream(Stream::Trades),
                    Input::Feature(BARS_TIME_1M_V1.key),
                ]
            );
            let minutes = u32::try_from(timeframe.millis() / Timeframe::M1.millis()).unwrap();
            assert_eq!(
                definition.warm_up,
                WarmUp::Samples(minutes),
                "{timeframe:?}"
            );
        }
        let book = [
            &BOOK_L2_V1,
            &BOOK_DEPTH_V1,
            &BOOK_CLUSTERS_V1,
            &BOOK_LIQUIDITY_WINDOW_5M_V1,
            &BOOK_LIQUIDITY_WINDOW_15M_V1,
            &BOOK_LIQUIDITY_WINDOW_1H_V1,
        ];
        for definition in book {
            assert!(definition.key.id.as_str().starts_with("book."));
            assert_eq!(definition.key.version.get(), 1);
            assert!(CURRENT.contains(&definition.key), "{}", definition.key);
        }
    }

    #[test]
    fn location_features_match_adr_044() {
        assert_eq!(
            LOCATION_VWAP_UTC_DAY_V1.params,
            &[
                Param {
                    name: "rounding",
                    value: ParamValue::Text("floor"),
                },
                Param {
                    name: "session_ms",
                    value: ParamValue::Int(Timeframe::D1.millis()),
                },
            ]
        );
        assert_eq!(
            LOCATION_VWAP_UTC_DAY_V1.inputs,
            &[
                Input::Stream(Stream::Trades),
                Input::Feature(BARS_TIME_1M_V1.key)
            ]
        );
        assert_eq!(
            LOCATION_LEVELS_V1.params,
            &[
                Param {
                    name: "confluence_rule",
                    value: ParamValue::Text("other_source_within_tolerance"),
                },
                Param {
                    name: "tolerance",
                    value: ParamValue::Rate(Rate::from_units(50_000)),
                },
                Param {
                    name: "zone_rule",
                    value: ParamValue::Text("bar_range_overlap"),
                },
            ]
        );
        assert_eq!(
            crate::location::TOLERANCE_BPS * 10_000,
            LOCATION_TOLERANCE.units(),
            "the tolerance constant and parameter agree"
        );
        assert_eq!(LOCATION_LEVELS_V1.inputs.len(), 9);
        assert_eq!(
            LOCATION_LEVELS_V1.inputs.last(),
            Some(&Input::Feature(BARS_TIME_1M_V1.key))
        );
        for (_, levels) in STRUCTURE_LEVELS {
            assert!(
                LOCATION_LEVELS_V1
                    .inputs
                    .contains(&Input::Feature(levels.key))
            );
        }
        let location = [&LOCATION_VWAP_UTC_DAY_V1, &LOCATION_LEVELS_V1];
        for definition in location {
            assert!(definition.key.id.as_str().starts_with("location."));
            assert_eq!(definition.key.version.get(), 1);
            assert_eq!(definition.warm_up, WarmUp::Samples(1));
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
             book.clusters@1,book.depth@1,book.l2@1,book.liquidity.window.15m@1,\
             book.liquidity.window.1h@1,book.liquidity.window.5m@1,\
             derivatives.funding.settled@1,derivatives.liq.window.15m@1,\
             derivatives.liq.window.1h@1,derivatives.liq.window.5m@1,\
             derivatives.mark@1,derivatives.oi.5m@1,derivatives.oi.sample@1,\
             flow.cvd.continuous@1,flow.cvd.utc_day@1,flow.window.15m@1,\
             flow.window.1h@1,flow.window.5m@1,location.levels@1,\
             location.vwap.utc_day@1,profile.volume.composite_5d@1,\
             profile.volume.prior_day@1,profile.volume.utc_day@1,\
             structure.levels.15m@1,structure.levels.1d@1,structure.levels.1h@1,\
             structure.levels.4h@1,structure.swing.15m@1,structure.swing.1d@1,\
             structure.swing.1h@1,structure.swing.4h@1,trade.last_price@1,\
             volatility.atr.1h@1,volatility.regime.1h@1"
        );
        assert_eq!(set.version().to_string(), "03d7fd13415653e3");
    }
}
