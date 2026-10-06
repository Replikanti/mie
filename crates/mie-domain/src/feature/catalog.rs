//! Every feature definition ever shipped, and its lock (ADR-029).
//!
//! The catalog only grows: a definition is never edited or deleted once its
//! [`LOCK`] line is merged. A change ships as a new version next to the old
//! one, which stays computable so past experiments reproduce. The steps are
//! in the [`feature`](super) module docs ("Adding a feature").

use super::{FeatureDefinition, FeatureKey, FeatureRegistry, FeatureSet, Input, LockEntry, WarmUp};
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

/// Every version of every feature. Never shrinks.
pub const DEFINITIONS: &[&FeatureDefinition] = &[&TRADE_LAST_PRICE_V1];

/// The fingerprint each published `id@version` must keep. Append-only: one
/// line per version; never edit or delete a line.
pub const LOCK: &[LockEntry] = &[LockEntry::new("trade.last_price", 1, 0x2eae_f1e1_bac4_d513)];

/// The default feature set: the latest version of each computed feature.
pub const CURRENT: &[FeatureKey] = &[TRADE_LAST_PRICE_V1.key];

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
        assert_eq!(LOCK.len(), 1, "lock lines");
        assert_eq!(
            hasher.finish().to_string(),
            "008a70255d2e2bff",
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
    fn current_set_version_is_pinned() {
        // The feature-set version every experiment records (ADR-029). It may
        // change only when CURRENT does — any other change here means a
        // published definition or the set encoding moved.
        let set = current_set();
        assert_eq!(set.to_string(), "trade.last_price@1");
        assert_eq!(set.version().to_string(), "9b8a3d042c350a62");
    }
}
