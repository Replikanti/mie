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

/// Every version of every feature. Never shrinks.
pub const DEFINITIONS: &[&FeatureDefinition] = &[
    &TRADE_LAST_PRICE_V1,
    &BARS_TIME_1M_V1,
    &BARS_TIME_5M_V1,
    &BARS_TIME_15M_V1,
    &BARS_TIME_1H_V1,
    &BARS_TIME_4H_V1,
    &BARS_TIME_1D_V1,
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
        assert_eq!(LOCK.len(), 7, "lock lines");
        assert_eq!(
            hasher.finish().to_string(),
            "e402c98d7d8a932c",
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
    fn current_set_version_is_pinned() {
        // The feature-set version every experiment records (ADR-029). It may
        // change only when CURRENT does — any other change here means a
        // published definition or the set encoding moved.
        let set = current_set();
        assert_eq!(
            set.to_string(),
            "bars.time.15m@1,bars.time.1d@1,bars.time.1h@1,bars.time.1m@1,\
             bars.time.4h@1,bars.time.5m@1,trade.last_price@1"
        );
        assert_eq!(set.version().to_string(), "828e8bb02f5d6813");
    }
}
