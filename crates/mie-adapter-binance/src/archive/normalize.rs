//! Archive CSV row → domain events: the archive counterpart of
//! [`crate::normalize`], shared by import validation, `archive-verify` and
//! replay (#11).
//!
//! Everything here is a pure function of the stored row bytes (ADR-019,
//! ADR-030). The rows are plain comma-separated ASCII without quoting; a row
//! must carry exactly its dataset's column count. Decimals go only through
//! the domain's exact `FromStr` (ADR-027): the archive's trailing zeros
//! beyond eight places are accepted, a ninth significant decimal is not.
//! Unlike live payloads, an archive decimal may carry an exponent: the
//! funding files write rates below 10⁻⁶ as `-1.8E-7` or `9.0E-7` (ADR-034).
//! Such a value is first rewritten exactly into the plain grammar
//! (`expand_exponent`), so both spellings of a value yield the same units.
//! Booleans are `true` / `false` exactly, as published; integers are plain
//! ASCII digits. A trade quantity that is not positive is an error, as on
//! the live path.
//!
//! | Stream | Event | Ordering time (ADR-028) |
//! |---|---|---|
//! | `aggTrades` | `Trade` (`is_buyer_maker` ⇒ aggressive sell, as live `m`) | `transact_time` |
//! | `klines_<interval>` | `Kline` (interval checked) | `close_time` |
//! | `fundingRate` | `FundingSettlement` | `calc_time` |
//! | `metrics` | `OpenInterest`, resolution [`METRICS_RESOLUTION_MS`] | `create_time` + one resolution step |
//! | `bookDepth` | none (raw only) | `timestamp` |
//! | `trades` | `Trade` with the raw trade id (kline cross-check only) | `time` |
//!
//! The metrics rule (ADR-034): the archive carries no publication time, and
//! a five-minute open-interest value is ordered at the end of its interval,
//! never at its start, so no consumer sees it before it could have been
//! known.

use super::catalog::{ArchiveStream, DAY_MS, digits};
use crate::normalize::{NormalizeError, decimal, positive_trade_qty};
use mie_domain::event::{Aggressor, FundingSettlement, Kline, MarketEvent, OpenInterest, Trade};
use mie_domain::num::{DECIMALS, ParseDecimalError, Price, Qty, Rate};
use mie_domain::time::EventTime;

/// Sampling resolution of the archive's open interest (`metrics`), in
/// milliseconds.
pub const METRICS_RESOLUTION_MS: u32 = 300_000;

/// The columns of a row, checked against the dataset's column names.
struct Row<'a> {
    fields: Vec<&'a str>,
    names: &'static [&'static str],
}

impl<'a> Row<'a> {
    fn split(row: &'a [u8], names: &'static [&'static str]) -> Result<Self, NormalizeError> {
        let text = std::str::from_utf8(row)
            .map_err(|_| NormalizeError::Unexpected("row is not UTF-8".to_owned()))?;
        let fields: Vec<&str> = text.split(',').collect();
        if fields.len() < names.len() {
            return Err(NormalizeError::Missing(names[fields.len()]));
        }
        if fields.len() > names.len() {
            return Err(NormalizeError::Unexpected(format!(
                "{} columns, expected {}",
                fields.len(),
                names.len()
            )));
        }
        Ok(Self { fields, names })
    }

    fn get(&self, index: usize) -> (&'a str, &'static str) {
        (self.fields[index], self.names[index])
    }

    fn decimal<T>(&self, index: usize) -> Result<T, NormalizeError>
    where
        T: std::str::FromStr<Err = mie_domain::num::ParseDecimalError>,
    {
        let (value, name) = self.get(index);
        if value.bytes().any(|b| matches!(b, b'e' | b'E')) {
            decimal(Some(&expand_exponent(value, name)?), name)
        } else {
            decimal(Some(value), name)
        }
    }

    fn integer(&self, index: usize) -> Result<u64, NormalizeError> {
        let (value, name) = self.get(index);
        if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
            value
                .parse()
                .map_err(|_| unexpected(name, value, "a 64-bit unsigned integer"))
        } else {
            Err(unexpected(name, value, "ASCII digits"))
        }
    }

    fn millis(&self, index: usize) -> Result<EventTime, NormalizeError> {
        let (value, name) = self.get(index);
        let millis = self.integer(index)?;
        i64::try_from(millis)
            .map(EventTime::from_millis)
            .map_err(|_| unexpected(name, value, "epoch milliseconds"))
    }

    fn civil_time(&self, index: usize) -> Result<EventTime, NormalizeError> {
        let (value, name) = self.get(index);
        parse_civil_time(value)
            .map(EventTime::from_millis)
            .ok_or_else(|| unexpected(name, value, "YYYY-MM-DD HH:MM:SS"))
    }

    fn boolean(&self, index: usize) -> Result<bool, NormalizeError> {
        match self.get(index) {
            ("true", _) => Ok(true),
            ("false", _) => Ok(false),
            (value, name) => Err(unexpected(name, value, "true or false")),
        }
    }
}

fn unexpected(name: &str, value: &str, expected: &str) -> NormalizeError {
    NormalizeError::Unexpected(format!("{name} {value:?}, expected {expected}"))
}

/// Rewrites an archive decimal with an exponent, `m[eE][+-]?x` with `m` in
/// the domain grammar `-?[0-9]+(.[0-9]+)?`, into that plain grammar by
/// moving the decimal point. The rewrite is exact: digit strings and integer
/// exponent arithmetic only, never a float, and the result then goes through
/// the same domain `FromStr` as every plain value, so `-1.8E-7` and
/// `-0.00000018` yield the same units.
///
/// # Errors
///
/// [`NormalizeError::Unexpected`] when the mantissa or the exponent is
/// malformed; [`NormalizeError::Decimal`] with
/// [`ParseDecimalError::TooManyDecimals`] when a non-zero digit lands beyond
/// the eighth decimal place, or [`ParseDecimalError::Overflow`] when the
/// value is too large for the fixed scale (the domain parser reports
/// overflows the bound here lets through).
fn expand_exponent(value: &str, name: &'static str) -> Result<String, NormalizeError> {
    let malformed = || {
        unexpected(
            name,
            value,
            "a decimal -?[0-9]+(.[0-9]+)?([eE][+-]?[0-9]+)?",
        )
    };
    let is_digits = |part: &[u8]| !part.is_empty() && part.iter().all(u8::is_ascii_digit);
    let bytes = value.as_bytes();
    let split = bytes
        .iter()
        .position(|&b| matches!(b, b'e' | b'E'))
        .ok_or_else(malformed)?;
    let (mantissa, exponent) = (&bytes[..split], &bytes[split + 1..]);

    let (negative, mantissa) = match mantissa.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, mantissa),
    };
    let (int, frac) = match mantissa.iter().position(|&b| b == b'.') {
        Some(point) => (&mantissa[..point], Some(&mantissa[point + 1..])),
        None => (mantissa, None),
    };
    if !is_digits(int) || frac.is_some_and(|frac| !is_digits(frac)) {
        return Err(malformed());
    }
    let frac = frac.unwrap_or_default();
    let (exponent_negative, exponent) = match exponent.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, exponent),
    };
    if !is_digits(exponent) {
        return Err(malformed());
    }

    // An all-zero mantissa (`0E-8`) is zero at any exponent.
    let digits: Vec<u8> = int.iter().chain(frac).copied().collect();
    let (Some(first), Some(last)) = (
        digits.iter().position(|&b| b != b'0'),
        digits.iter().rposition(|&b| b != b'0'),
    ) else {
        return Ok("0".to_owned());
    };
    let significant = &digits[first..=last];

    // value = significant × 10^shift. Exponents beyond the i64 range
    // saturate: such a value is too small or too large either way.
    let exponent = exponent
        .iter()
        .try_fold(0_i64, |acc, &d| {
            acc.checked_mul(10)?.checked_add(i64::from(d - b'0'))
        })
        .unwrap_or(i64::MAX);
    let exponent = if exponent_negative {
        -exponent
    } else {
        exponent
    };
    let below = i64::try_from(digits.len() - 1 - last).unwrap_or(i64::MAX);
    let above = i64::try_from(frac.len()).unwrap_or(i64::MAX);
    let shift = exponent.saturating_add(below).saturating_sub(above);
    let len = i64::try_from(significant.len()).unwrap_or(i64::MAX);

    // The last significant digit is non-zero and sits `-shift` places after
    // the point.
    let decimal_error = |error| NormalizeError::Decimal { field: name, error };
    if shift < -i64::from(DECIMALS) {
        return Err(decimal_error(ParseDecimalError::TooManyDecimals));
    }
    // A whole part of more than 20 digits exceeds every 64-bit fixed-point
    // value. The bound only keeps the rewrite small; the domain parser
    // decides the exact range.
    if len.saturating_add(shift) > 20 {
        return Err(decimal_error(ParseDecimalError::Overflow));
    }

    // From here -8 <= shift <= 19 and len + shift <= 20.
    let significant = std::str::from_utf8(significant).map_err(|_| malformed())?;
    let zeros = |count: i64| "0".repeat(usize::try_from(count).unwrap_or_default());
    let sign = if negative { "-" } else { "" };
    let point = len + shift;
    Ok(if shift >= 0 {
        format!("{sign}{significant}{}", zeros(shift))
    } else if point > 0 {
        let (whole, fraction) = significant.split_at(usize::try_from(point).unwrap_or_default());
        format!("{sign}{whole}.{fraction}")
    } else {
        format!("{sign}0.{}{significant}", zeros(-point))
    })
}

const AGG_TRADES: &[&str] = &[
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
];
const KLINES: &[&str] = &[
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
];
const FUNDING_RATE: &[&str] = &["calc_time", "funding_interval_hours", "last_funding_rate"];
const METRICS: &[&str] = &[
    "create_time",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
];
const BOOK_DEPTH: &[&str] = &["timestamp", "percentage", "depth", "notional"];
const TRADES: &[&str] = &["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"];

fn columns(stream: ArchiveStream) -> &'static [&'static str] {
    match stream {
        ArchiveStream::AggTrades => AGG_TRADES,
        ArchiveStream::Klines(_) => KLINES,
        ArchiveStream::FundingRate => FUNDING_RATE,
        ArchiveStream::Metrics => METRICS,
        ArchiveStream::BookDepth => BOOK_DEPTH,
        ArchiveStream::Trades => TRADES,
    }
}

/// Parses the archive's UTC civil time `YYYY-MM-DD HH:MM:SS` to epoch
/// milliseconds. Only real dates and times written with exactly these
/// widths are accepted.
pub fn parse_civil_time(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() != 19 || bytes[10] != b' ' || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let days = super::catalog::parse_day(&text[0..10])?;
    let (hour, minute, second) = (
        digits(&text[11..13])?,
        digits(&text[14..16])?,
        digits(&text[17..19])?,
    );
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(days * DAY_MS + ((hour * 60 + minute) * 60 + second) * 1_000)
}

/// The ADR-028 ordering time of a row, used as the raw record's
/// `event_time` (ADR-030 D4); see the module table.
///
/// # Errors
///
/// [`NormalizeError`] when the row has the wrong column count or the time
/// column does not parse.
pub fn record_time(stream: ArchiveStream, row: &[u8]) -> Result<EventTime, NormalizeError> {
    let row = Row::split(row, columns(stream))?;
    match stream {
        ArchiveStream::AggTrades => row.millis(5),
        ArchiveStream::Klines(_) => row.millis(6),
        ArchiveStream::FundingRate => row.millis(0),
        ArchiveStream::Metrics => {
            let created = row.civil_time(0)?;
            Ok(EventTime::from_millis(
                created.as_millis() + i64::from(METRICS_RESOLUTION_MS),
            ))
        }
        ArchiveStream::BookDepth => row.civil_time(0),
        ArchiveStream::Trades => row.millis(4),
    }
}

/// Normalizes one data row of `stream` for `symbol`.
///
/// `Ok(None)` is a valid row without a domain event (`bookDepth`).
///
/// # Errors
///
/// [`NormalizeError`] when the row has the wrong column count, a field does
/// not parse, a decimal is inexact, a kline's span is not its interval, or a
/// metrics row names another symbol.
pub fn parse(
    stream: ArchiveStream,
    symbol: &str,
    row: &[u8],
) -> Result<Option<MarketEvent>, NormalizeError> {
    let fields = Row::split(row, columns(stream))?;
    let event = match stream {
        ArchiveStream::AggTrades => trade(&fields, [0, 1, 2, 5, 6])?,
        ArchiveStream::Trades => trade(&fields, [0, 1, 2, 4, 5])?,
        ArchiveStream::Klines(timeframe) => {
            let open_time = fields.millis(0)?;
            let close_time = fields.millis(6)?;
            let span = close_time.as_millis() - open_time.as_millis() + 1;
            if span != timeframe.millis() {
                return Err(NormalizeError::Unexpected(format!(
                    "kline spans {span} ms, expected {} ms for {timeframe}",
                    timeframe.millis()
                )));
            }
            MarketEvent::Kline(Kline {
                open_time,
                close_time,
                open: fields.decimal::<Price>(1)?,
                high: fields.decimal::<Price>(2)?,
                low: fields.decimal::<Price>(3)?,
                close: fields.decimal::<Price>(4)?,
                volume: fields.decimal::<Qty>(5)?,
                taker_buy_volume: fields.decimal::<Qty>(9)?,
                trade_count: fields.integer(8)?,
            })
        }
        ArchiveStream::FundingRate => MarketEvent::FundingSettlement(FundingSettlement {
            time: fields.millis(0)?,
            rate: fields.decimal::<Rate>(2)?,
        }),
        ArchiveStream::Metrics => {
            let (found, name) = fields.get(1);
            if found != symbol {
                return Err(NormalizeError::Unexpected(format!(
                    "symbol {found:?} in {name:?}, expected {symbol:?}"
                )));
            }
            MarketEvent::OpenInterest(OpenInterest {
                time: record_time(stream, row)?,
                open_interest: fields.decimal::<Qty>(2)?,
                resolution_ms: METRICS_RESOLUTION_MS,
            })
        }
        ArchiveStream::BookDepth => {
            fields.civil_time(0)?;
            return Ok(None);
        }
    };
    Ok(Some(event))
}

/// A trade from the id, price, quantity, time and buyer-is-maker columns.
fn trade(
    row: &Row<'_>,
    [id, price, qty, time, maker]: [usize; 5],
) -> Result<MarketEvent, NormalizeError> {
    // The buyer is the maker, so the seller crossed the spread (live `m`).
    let aggressor = if row.boolean(maker)? {
        Aggressor::Sell
    } else {
        Aggressor::Buy
    };
    Ok(MarketEvent::Trade(Trade {
        time: row.millis(time)?,
        trade_id: row.integer(id)?,
        price: row.decimal::<Price>(price)?,
        qty: positive_trade_qty(row.decimal::<Qty>(qty)?, row.get(qty).1)?,
        aggressor,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::catalog::days_from_civil;
    use mie_domain::bars::Timeframe;

    #[test]
    fn civil_times_parse_strictly() {
        assert_eq!(parse_civil_time("1970-01-01 00:00:00"), Some(0));
        assert_eq!(
            parse_civil_time("2026-09-30 23:55:00"),
            Some(days_from_civil(2026, 9, 30) * DAY_MS + 86_100_000)
        );
        assert_eq!(
            parse_civil_time("2024-02-29 12:34:56"),
            Some(days_from_civil(2024, 2, 29) * DAY_MS + 45_296_000)
        );
        for bad in [
            "2026-09-30T23:55:00",
            "2026-09-30 24:00:00",
            "2026-09-30 23:60:00",
            "2026-09-30 23:59:60",
            "2025-02-29 00:00:00",
            "2026-09-30 1:00:00",
            "2026-09-30 01:00:00.000",
            "",
        ] {
            assert_eq!(parse_civil_time(bad), None, "{bad}");
        }
    }

    #[test]
    fn column_counts_are_exact() {
        let short = b"1,2,3";
        assert_eq!(
            record_time(ArchiveStream::AggTrades, short),
            Err(NormalizeError::Missing("first_trade_id"))
        );
        let long = b"1,2,3,4,5,6,true,extra";
        assert!(matches!(
            parse(ArchiveStream::AggTrades, "BTCUSDT", long),
            Err(NormalizeError::Unexpected(_))
        ));
        // An empty row is one empty column.
        assert_eq!(
            record_time(ArchiveStream::Klines(Timeframe::M1), b""),
            Err(NormalizeError::Missing("open"))
        );
    }

    #[test]
    fn integers_and_booleans_are_exact() {
        let row = |time: &str, maker: &str| format!("1,100.0,0.5,1,1,{time},{maker}");
        for (time, maker) in [
            ("+1790726400261", "true"),
            ("1790726400261.0", "true"),
            (" 1790726400261", "true"),
            ("1790726400261", "True"),
            ("1790726400261", "1"),
            ("99999999999999999999", "true"),
        ] {
            assert!(
                matches!(
                    parse(
                        ArchiveStream::AggTrades,
                        "BTCUSDT",
                        row(time, maker).as_bytes()
                    ),
                    Err(NormalizeError::Unexpected(_))
                ),
                "{time} {maker}"
            );
        }
    }

    /// The funding rate of a `fundingRate` row carrying `rate`.
    fn funding_rate(rate: &str) -> Result<Rate, NormalizeError> {
        let row = format!("1760976000000,8,{rate}");
        match parse(ArchiveStream::FundingRate, "BTCUSDT", row.as_bytes())? {
            Some(MarketEvent::FundingSettlement(settlement)) => Ok(settlement.rate),
            other => panic!("{rate}: {other:?}"),
        }
    }

    #[test]
    fn exponents_expand_exactly_to_the_plain_grammar() {
        for (exponent, plain, units) in [
            ("-1.8E-7", "-0.00000018", -18),
            ("-6E-8", "-0.00000006", -6),
            ("9.0E-7", "0.0000009", 90),
            ("6.8E-7", "0.00000068", 68),
            ("6.8e-7", "0.00000068", 68),
            ("1E-8", "0.00000001", 1),
            ("1.0E-8", "0.00000001", 1),
            ("123.456E-2", "1.23456", 123_456_000),
            ("1.25E+2", "125", 12_500_000_000),
            ("1.25E2", "125", 12_500_000_000),
            ("1e+0", "1", 100_000_000),
            ("-0.0005E+3", "-0.5", -50_000_000),
            ("00012E-1", "1.2", 120_000_000),
            ("1E10", "10000000000", 1_000_000_000_000_000_000),
            ("92233720368.54775807E0", "92233720368.54775807", i64::MAX),
            (
                "-92233720368.54775808e+0",
                "-92233720368.54775808",
                i64::MIN,
            ),
            ("0E-8", "0", 0),
            ("-0E-8", "0", 0),
            ("0.000E+99999999999999999999", "0", 0),
        ] {
            assert_eq!(
                expand_exponent(exponent, "last_funding_rate").as_deref(),
                Ok(plain),
                "{exponent}"
            );
            assert_eq!(
                funding_rate(exponent),
                Ok(Rate::from_units(units)),
                "{exponent}"
            );
        }
    }

    #[test]
    fn exponents_out_of_the_fixed_scale_are_rejected() {
        for (value, error) in [
            ("1E-9", ParseDecimalError::TooManyDecimals),
            ("-1.5E-8", ParseDecimalError::TooManyDecimals),
            ("1.000000001E0", ParseDecimalError::TooManyDecimals),
            (
                "1E-99999999999999999999",
                ParseDecimalError::TooManyDecimals,
            ),
            ("1E11", ParseDecimalError::Overflow),
            ("-1E11", ParseDecimalError::Overflow),
            ("92233720368.54775808E0", ParseDecimalError::Overflow),
            ("1E21", ParseDecimalError::Overflow),
            ("1E+99999999999999999999", ParseDecimalError::Overflow),
        ] {
            assert_eq!(
                funding_rate(value),
                Err(NormalizeError::Decimal {
                    field: "last_funding_rate",
                    error,
                }),
                "{value}"
            );
        }
    }

    #[test]
    fn malformed_exponents_are_rejected() {
        for bad in [
            "1E",
            "1E+",
            "1E-",
            "1E+-7",
            "1E--7",
            "1EE7",
            "1E7.5",
            "1E 7",
            "1E7 ",
            "E-7",
            "-E-7",
            ".5E-7",
            "1.E-7",
            "+1E-7",
            "--1E-7",
            "1_5E-7",
            "1E0x7",
            "1E\u{2212}7",
            "1e-7e",
        ] {
            let error = funding_rate(bad).expect_err(bad);
            assert!(
                matches!(&error, NormalizeError::Unexpected(detail)
                    if detail.contains("[eE][+-]?[0-9]+")),
                "{bad}: {error}"
            );
        }
        // Without an exponent the domain's own error stays.
        assert_eq!(
            funding_rate("1.5x"),
            Err(NormalizeError::Decimal {
                field: "last_funding_rate",
                error: ParseDecimalError::Malformed,
            })
        );
    }
}
