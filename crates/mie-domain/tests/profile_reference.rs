//! Reference cross-check of the `profile.volume.*@1` levels (ADR-036, rule
//! A5 of *Accept when*).
//!
//! The 10 USDT histograms of twelve real UTC days (the 15th of each month,
//! 2025-10 to 2026-09, from the Binance aggTrades archive) are fed to
//! [`shape_levels`] at [`ProfileShape::V1`], and every day's levels are
//! compared exactly with the values an independent transcription of the
//! ADR text computed from the same trades
//! (`tools/reference/volume_profile_reference.py`; provenance in
//! `tests/fixtures/README.md`). Both sides are exact (decision 7), so there
//! is no tolerance: any difference is a defect.

use mie_domain::bars::Coverage;
use mie_domain::feature::catalog;
use mie_domain::num::{Price, Qty};
use mie_domain::profile::{BIN_SIZE, ProfileShape, VolumeProfile, shape_levels};
use mie_domain::time::EventTime;
use std::collections::BTreeMap;

const HISTOGRAMS: &str = include_str!("fixtures/btcusdt-profile-histograms.tsv");
const REFERENCE: &str = include_str!("fixtures/profile-reference.tsv");

const DAY_MS: i64 = 86_400_000;

/// The A5 days: the 15th of each month of the #12 window, chosen by
/// calendar, not by outcome.
const DAYS: [&str; 12] = [
    "2025-10-15",
    "2025-11-15",
    "2025-12-15",
    "2026-01-15",
    "2026-02-15",
    "2026-03-15",
    "2026-04-15",
    "2026-05-15",
    "2026-06-15",
    "2026-07-15",
    "2026-08-15",
    "2026-09-15",
];

/// The UTC open of `YYYY-MM-DD` in milliseconds (proleptic Gregorian,
/// days-from-civil).
fn day_open_ms(day: &str) -> i64 {
    let fields: Vec<i64> = day.split('-').map(|part| part.parse().unwrap()).collect();
    let [year, month, dom] = fields[..] else {
        panic!("malformed day: {day}")
    };
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + dom - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    (era * 146_097 + day_of_era - 719_468) * DAY_MS
}

/// The histogram of each day, keyed by its `@1` bin; the rows of a day are
/// the dense range, every bin with volume.
fn histograms() -> BTreeMap<String, BTreeMap<i64, Qty>> {
    let mut days: BTreeMap<String, BTreeMap<i64, Qty>> = BTreeMap::new();
    for line in HISTOGRAMS.lines() {
        let fields: Vec<&str> = line.split('\t').collect();
        let [day, edge, volume] = fields[..] else {
            panic!("malformed histogram row: {line}")
        };
        let edge: Price = edge.parse().unwrap();
        let volume: Qty = volume.parse().unwrap();
        assert!(volume.units() > 0, "{line}");
        let bin = ProfileShape::V1.bin_of(edge);
        assert_eq!(
            bin * BIN_SIZE.units(),
            edge.units(),
            "not a bin edge: {line}"
        );
        let bins = days.entry(day.to_owned()).or_default();
        if let Some((last, _)) = bins.last_key_value() {
            assert_eq!(bin, last + 1, "rows not dense and ascending: {line}");
        }
        assert!(bins.insert(bin, volume).is_none(), "{line}");
    }
    days
}

/// The reference level line of each day: the `VolumeProfile` `Display`
/// fields from `start=` to `lvn=`.
fn reference() -> BTreeMap<String, String> {
    REFERENCE
        .lines()
        .map(|line| {
            let (day, levels) = line
                .split_once('\t')
                .unwrap_or_else(|| panic!("malformed reference row: {line}"));
            (day.to_owned(), levels.to_owned())
        })
        .collect()
}

#[test]
fn profile_levels_match_the_independent_reference_on_real_days() {
    let histograms = histograms();
    let reference = reference();
    let days: Vec<&str> = DAYS.to_vec();
    assert_eq!(
        histograms.keys().map(String::as_str).collect::<Vec<_>>(),
        days
    );
    assert_eq!(
        reference.keys().map(String::as_str).collect::<Vec<_>>(),
        days
    );
    assert_eq!(histograms.values().map(BTreeMap::len).sum::<usize>(), 2_980);

    let mut lvn_free = 0;
    for day in DAYS {
        let levels = shape_levels(&ProfileShape::V1, &[&histograms[day]])
            .unwrap()
            .ready()
            .cloned()
            .unwrap_or_else(|| panic!("{day}: profile not ready"));
        if levels.lvn.is_empty() {
            lvn_free += 1;
        }
        // The line carries each node's midpoint and permille only; its edges
        // and raw bin volume (hashed with the state) are checked against the
        // histogram the node's midpoint falls in.
        let bin_units = BIN_SIZE.units();
        for node in levels.hvn.iter().chain(&levels.lvn) {
            let bin = ProfileShape::V1.bin_of(node.price);
            assert_eq!(node.low.units(), bin * bin_units, "{day}: {node:?}");
            assert_eq!(node.high.units(), (bin + 1) * bin_units, "{day}: {node:?}");
            assert_eq!(
                node.price.units(),
                bin * bin_units + bin_units / 2,
                "{day}: {node:?}"
            );
            assert_eq!(
                Some(&node.volume),
                histograms[day].get(&bin),
                "{day}: {node:?}"
            );
        }
        let start = day_open_ms(day);
        let profile = VolumeProfile {
            feature: catalog::PROFILE_VOLUME_PRIOR_DAY_V1.key,
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(start + DAY_MS),
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
            coverage: Coverage::default(),
        }
        .to_string();
        // The reference covers the 13 fields from `start=` to `lvn=`; the
        // engine line ends with the coverage and the feature key.
        let fields: Vec<&str> = profile.split(' ').collect();
        assert_eq!(fields.len(), 15, "{day}: {profile}");
        assert_eq!(fields[..13].join(" "), reference[day], "{day}");
    }
    // 2026-08-15, a quiet Saturday of 29 bins, has no LVN; the other
    // eleven days do, so both node kinds are exercised.
    assert_eq!(lvn_free, 1);
}

#[test]
fn day_open_is_the_utc_midnight() {
    assert_eq!(day_open_ms("1970-01-01"), 0);
    assert_eq!(day_open_ms("2025-10-15"), 1_760_486_400_000);
    assert_eq!(day_open_ms("2026-09-15"), 1_789_430_400_000);
}
