//! The #12 development year as ADR-054 *Acceptance*, ADR-056 *Measurement*
//! and ADR-057 *Acceptance* published it: the provenance and counts the
//! measurement tools over its outcome dataset assert before they print
//! anything (ADR-057 D9, ADR-058 D1).

/// The records window, 2025-10-01 … 2026-09-30, `[start, end)` in ms.
pub const YEAR_RECORDS: (i64, i64) = (1_759_276_800_000, 1_790_812_800_000);

/// The provenance: data version, engine key, sampling and labeling with
/// their fingerprints.
pub const YEAR_PROVENANCE: [&str; 6] = [
    "5d9e27f862ac54dd93cff904458e9ee92767d5105a9d13a144351cba069f224d",
    "4c844593779da2e3-1",
    "conditional.sampling@1",
    "2c71fff23e8b0415",
    "conditional.labels@2",
    "6f90bff079e746f2",
];

/// Contexts, baseline, trigger.
pub const YEAR_CONTEXTS: (u64, u64, u64) = (561_695, 525_599, 36_096);

/// Per horizon: `neither`, `unavailable`, windows censored `end_of_data`,
/// clear baseline, clear trigger (ADR-056 *Measurement*, the year table).
pub const YEAR_HORIZONS: [(&str, [u64; 5]); 5] = [
    ("5m", [43_925, 74, 5, 94_333, 25_519]),
    ("15m", [44_593, 224, 17, 33_758, 22_828]),
    ("1h", [56_372, 899, 62, 8_678, 16_654]),
    ("4h", [68_977, 3_670, 245, 2_185, 9_815]),
    ("1d", [56_383, 22_724, 1_548, 365, 3_604]),
];
