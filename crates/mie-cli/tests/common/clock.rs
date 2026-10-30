//! The New York clock of the measurement tools: moved out of
//! `season_measure.rs` (#155) unchanged, so the order-flow study (#28,
//! ADR-058 D4) stratifies by the same hour of week.

use mie_adapter_binance::archive::catalog::{civil_from_days, days_from_civil};

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;

/// `ms` on the UTC clock, or on the US Eastern clock when `new_york`.
pub fn on_clock(ms: i64, new_york: bool) -> i64 {
    if !new_york {
        return ms;
    }
    let offset = if us_dst(ms.div_euclid(DAY_MS)) { 4 } else { 5 };
    ms - offset * HOUR_MS
}

/// The weekday of a day since 1970-01-01, Monday = 0.
pub fn weekday(day: i64) -> usize {
    (day + 3).rem_euclid(7) as usize
}

/// The first Sunday on or after `day`.
pub fn sunday_from(day: i64) -> i64 {
    day + (6 - weekday(day) as i64)
}

/// US daylight-saving time by UTC date: from the second Sunday of March to
/// the day before the first Sunday of November.
pub fn us_dst(day: i64) -> bool {
    let (year, _, _) = civil_from_days(day);
    let start = sunday_from(days_from_civil(year, 3, 1)) + 7;
    let end = sunday_from(days_from_civil(year, 11, 1));
    (start..end).contains(&day)
}
