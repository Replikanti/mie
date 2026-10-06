//! Archive CSV row → domain events: the archive counterpart of
//! [`crate::normalize`], shared by import validation, `archive-verify` and
//! replay (#11).
//!
//! Everything here is a pure function of the stored row bytes (ADR-019,
//! ADR-030). The rows are plain comma-separated ASCII without quoting; a row
//! must carry exactly its dataset's column count. Decimals go only through
//! the domain's exact `FromStr` (ADR-027): the archive's trailing zeros
//! beyond eight places are accepted, a ninth significant decimal is not.
//! Booleans are `true` / `false` exactly, as published; integers are plain
//! ASCII digits.
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
use crate::normalize::{NormalizeError, decimal};
use mie_domain::event::{Aggressor, FundingSettlement, Kline, MarketEvent, OpenInterest, Trade};
use mie_domain::num::{Price, Qty, Rate};
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
        decimal(Some(value), name)
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
        qty: row.decimal::<Qty>(qty)?,
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
}
