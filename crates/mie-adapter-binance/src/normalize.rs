//! Binance wire format → domain events: the only place that knows it.
//!
//! Live capture and replay (#11) normalize the same persisted bytes with
//! this code (ADR-019, ADR-030). Archive rows have their own normalizer in
//! [`crate::archive::normalize`], built on the same decimal parsing.
//! Everything here is a pure function of the payload: no clock, no state.
//!
//! - Decimals are borrowed as strings and parsed only through the exact
//!   `FromStr` of [`Price`], [`Qty`] and [`Rate`] (ADR-027). A decimal sent
//!   as a JSON number is rejected, never coerced through `f64`.
//! - A trade quantity that is not positive is an error
//!   ([`NormalizeError::Unexpected`]): it is not an execution, and every
//!   downstream sum (bars, flow, profile) assumes positive volume.
//! - Unknown fields are ignored, so additive exchange changes do not break
//!   capture. A missing field, a wrong event type or a symbol mismatch is an
//!   error; the raw message is kept either way.
//!
//! | Stream | Event | Ordering field (ADR-028) |
//! |---|---|---|
//! | `aggTrade` | `Trade` (`m` = buyer is maker ⇒ aggressive sell) | `T` |
//! | `markPrice` | `MarkPrice` | `E` |
//! | `forceOrder` | `Liquidation` | `o.T` |
//! | `kline_1m` | `Kline`, closed bars (`x = true`) only | `k.T` |
//! | `openInterest` | `OpenInterest`, resolution [`OI_POLL_INTERVAL_MS`] | `time` |

use crate::stream::{BinanceStream, OI_POLL_INTERVAL_MS};
use mie_domain::event::{
    Aggressor, Kline, Liquidation, MarkPrice, MarketEvent, OpenInterest, Trade,
};
use mie_domain::num::{ParseDecimalError, Price, Qty, Rate};
use mie_domain::time::EventTime;
use serde::Deserialize;
use std::fmt;
use std::str::FromStr;

/// Why a payload could not be normalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeError {
    /// The payload is not JSON of the expected shape (including a decimal
    /// sent as a number or a field of the wrong type).
    Json(String),
    /// A required field is absent.
    Missing(&'static str),
    /// A decimal field is not an exact ADR-027 decimal.
    Decimal {
        /// The wire field name.
        field: &'static str,
        /// What the parser rejected.
        error: ParseDecimalError,
    },
    /// A field has a value this adapter does not accept (event type, symbol,
    /// side, interval).
    Unexpected(String),
}

impl fmt::Display for NormalizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(detail) => write!(f, "malformed payload: {detail}"),
            Self::Missing(field) => write!(f, "missing field {field:?}"),
            Self::Decimal { field, error } => write!(f, "field {field:?}: {error}"),
            Self::Unexpected(detail) => write!(f, "unexpected value: {detail}"),
        }
    }
}

impl std::error::Error for NormalizeError {}

/// The fields that can order a record, for every stream.
#[derive(Deserialize)]
struct TimeFields {
    #[serde(rename = "E")]
    event: Option<i64>,
    #[serde(rename = "T")]
    trade: Option<i64>,
    time: Option<i64>,
    o: Option<NestedTime>,
    k: Option<NestedTime>,
}

#[derive(Deserialize)]
struct NestedTime {
    #[serde(rename = "T")]
    time: Option<i64>,
}

/// The ADR-028 ordering time of a payload, used as the raw record's
/// `event_time` (ADR-030): aggTrade `T`, markPrice `E`, forceOrder `o.T`,
/// kline `k.T` (the bar's close time, also for open bars), open interest
/// `time`. When that field is missing it falls back to the push time `E`;
/// `None` when neither is present or the payload is not JSON.
pub fn record_time(stream: BinanceStream, payload: &[u8]) -> Option<EventTime> {
    let fields: TimeFields = serde_json::from_slice(payload).ok()?;
    let primary = match stream {
        BinanceStream::AggTrade => fields.trade,
        BinanceStream::MarkPrice => fields.event,
        BinanceStream::ForceOrder => fields.o.and_then(|o| o.time),
        BinanceStream::Kline1m => fields.k.and_then(|k| k.time),
        BinanceStream::OpenInterest => fields.time,
    };
    primary.or(fields.event).map(EventTime::from_millis)
}

/// Normalizes one payload of `stream` for `symbol`.
///
/// `Ok(None)` is a valid message without a domain event: a kline whose bar
/// is still open (`x = false`).
///
/// # Errors
///
/// [`NormalizeError`] when the payload is malformed, misses a field, carries
/// an inexact decimal, or belongs to another event type or symbol.
pub fn parse(
    stream: BinanceStream,
    symbol: &str,
    payload: &[u8],
) -> Result<Option<MarketEvent>, NormalizeError> {
    match stream {
        BinanceStream::AggTrade => parse_agg_trade(symbol, payload).map(Some),
        BinanceStream::MarkPrice => parse_mark_price(symbol, payload).map(Some),
        BinanceStream::ForceOrder => parse_force_order(symbol, payload).map(Some),
        BinanceStream::Kline1m => parse_kline(symbol, payload),
        BinanceStream::OpenInterest => parse_open_interest(symbol, payload).map(Some),
    }
}

fn from_json<'a, T: Deserialize<'a>>(payload: &'a [u8]) -> Result<T, NormalizeError> {
    serde_json::from_slice(payload).map_err(|e| NormalizeError::Json(e.to_string()))
}

fn req<T>(value: Option<T>, field: &'static str) -> Result<T, NormalizeError> {
    value.ok_or(NormalizeError::Missing(field))
}

/// Parses a required decimal field through the domain's exact `FromStr`
/// (ADR-027). Shared with the archive normalizer.
pub(crate) fn decimal<T>(value: Option<&str>, field: &'static str) -> Result<T, NormalizeError>
where
    T: FromStr<Err = ParseDecimalError>,
{
    req(value, field)?
        .parse()
        .map_err(|error| NormalizeError::Decimal { field, error })
}

/// Accepts a trade quantity only when it is positive: a zero or negative
/// quantity is not an execution (issue #54). Shared with the archive
/// normalizer, so both trade paths enforce the same rule.
pub(crate) fn positive_trade_qty(qty: Qty, field: &'static str) -> Result<Qty, NormalizeError> {
    if qty.units() > 0 {
        Ok(qty)
    } else {
        Err(NormalizeError::Unexpected(format!(
            "{field}: trade quantity must be positive, got {qty}"
        )))
    }
}

fn expect_event_type(value: Option<&str>, expected: &str) -> Result<(), NormalizeError> {
    match req(value, "e")? {
        e if e == expected => Ok(()),
        e => Err(NormalizeError::Unexpected(format!(
            "event type {e:?}, expected {expected:?}"
        ))),
    }
}

fn expect_symbol(
    value: Option<&str>,
    field: &'static str,
    symbol: &str,
) -> Result<(), NormalizeError> {
    match req(value, field)? {
        s if s == symbol => Ok(()),
        s => Err(NormalizeError::Unexpected(format!(
            "symbol {s:?} in {field:?}, expected {symbol:?}"
        ))),
    }
}

fn time(value: Option<i64>, field: &'static str) -> Result<EventTime, NormalizeError> {
    req(value, field).map(EventTime::from_millis)
}

#[derive(Deserialize)]
struct AggTradeMsg<'a> {
    #[serde(borrow)]
    e: Option<&'a str>,
    #[serde(borrow)]
    s: Option<&'a str>,
    a: Option<u64>,
    #[serde(borrow)]
    p: Option<&'a str>,
    #[serde(borrow)]
    q: Option<&'a str>,
    #[serde(rename = "T")]
    trade_time: Option<i64>,
    m: Option<bool>,
}

fn parse_agg_trade(symbol: &str, payload: &[u8]) -> Result<MarketEvent, NormalizeError> {
    let msg: AggTradeMsg<'_> = from_json(payload)?;
    expect_event_type(msg.e, "aggTrade")?;
    expect_symbol(msg.s, "s", symbol)?;
    // `m`: the buyer is the maker, so the seller crossed the spread.
    let aggressor = if req(msg.m, "m")? {
        Aggressor::Sell
    } else {
        Aggressor::Buy
    };
    Ok(MarketEvent::Trade(Trade {
        time: time(msg.trade_time, "T")?,
        trade_id: req(msg.a, "a")?,
        price: decimal::<Price>(msg.p, "p")?,
        qty: positive_trade_qty(decimal::<Qty>(msg.q, "q")?, "q")?,
        aggressor,
    }))
}

#[derive(Deserialize)]
struct MarkPriceMsg<'a> {
    #[serde(borrow)]
    e: Option<&'a str>,
    #[serde(rename = "E")]
    event_time: Option<i64>,
    #[serde(borrow)]
    s: Option<&'a str>,
    #[serde(borrow)]
    p: Option<&'a str>,
    #[serde(borrow)]
    i: Option<&'a str>,
    #[serde(borrow)]
    r: Option<&'a str>,
    #[serde(rename = "T")]
    next_funding_time: Option<i64>,
}

fn parse_mark_price(symbol: &str, payload: &[u8]) -> Result<MarketEvent, NormalizeError> {
    let msg: MarkPriceMsg<'_> = from_json(payload)?;
    expect_event_type(msg.e, "markPriceUpdate")?;
    expect_symbol(msg.s, "s", symbol)?;
    Ok(MarketEvent::MarkPrice(MarkPrice {
        time: time(msg.event_time, "E")?,
        mark_price: decimal::<Price>(msg.p, "p")?,
        index_price: decimal::<Price>(msg.i, "i")?,
        funding_rate: decimal::<Rate>(msg.r, "r")?,
        next_funding_time: time(msg.next_funding_time, "T")?,
    }))
}

#[derive(Deserialize)]
struct ForceOrderMsg<'a> {
    #[serde(borrow)]
    e: Option<&'a str>,
    #[serde(borrow)]
    o: Option<ForceOrderBody<'a>>,
}

#[derive(Deserialize)]
struct ForceOrderBody<'a> {
    #[serde(borrow)]
    s: Option<&'a str>,
    #[serde(rename = "S", borrow)]
    side: Option<&'a str>,
    #[serde(borrow)]
    p: Option<&'a str>,
    #[serde(borrow)]
    ap: Option<&'a str>,
    #[serde(borrow)]
    z: Option<&'a str>,
    #[serde(rename = "T")]
    trade_time: Option<i64>,
}

fn parse_force_order(symbol: &str, payload: &[u8]) -> Result<MarketEvent, NormalizeError> {
    let msg: ForceOrderMsg<'_> = from_json(payload)?;
    expect_event_type(msg.e, "forceOrder")?;
    let order = req(msg.o, "o")?;
    expect_symbol(order.s, "o.s", symbol)?;
    // The liquidation order's own side: SELL closes a long.
    let aggressor = match req(order.side, "o.S")? {
        "SELL" => Aggressor::Sell,
        "BUY" => Aggressor::Buy,
        other => {
            return Err(NormalizeError::Unexpected(format!(
                "liquidation side {other:?}"
            )));
        }
    };
    Ok(MarketEvent::Liquidation(Liquidation {
        time: time(order.trade_time, "o.T")?,
        aggressor,
        price: decimal::<Price>(order.p, "o.p")?,
        avg_price: decimal::<Price>(order.ap, "o.ap")?,
        filled_qty: decimal::<Qty>(order.z, "o.z")?,
    }))
}

#[derive(Deserialize)]
struct KlineMsg<'a> {
    #[serde(borrow)]
    e: Option<&'a str>,
    #[serde(borrow)]
    s: Option<&'a str>,
    #[serde(borrow)]
    k: Option<KlineBody<'a>>,
}

#[derive(Deserialize)]
struct KlineBody<'a> {
    t: Option<i64>,
    #[serde(rename = "T")]
    close_time: Option<i64>,
    #[serde(borrow)]
    i: Option<&'a str>,
    #[serde(borrow)]
    o: Option<&'a str>,
    #[serde(borrow)]
    h: Option<&'a str>,
    #[serde(borrow)]
    l: Option<&'a str>,
    #[serde(borrow)]
    c: Option<&'a str>,
    #[serde(borrow)]
    v: Option<&'a str>,
    #[serde(rename = "V", borrow)]
    taker_buy_volume: Option<&'a str>,
    n: Option<u64>,
    x: Option<bool>,
}

fn parse_kline(symbol: &str, payload: &[u8]) -> Result<Option<MarketEvent>, NormalizeError> {
    let msg: KlineMsg<'_> = from_json(payload)?;
    expect_event_type(msg.e, "kline")?;
    expect_symbol(msg.s, "s", symbol)?;
    let bar = req(msg.k, "k")?;
    match req(bar.i, "k.i")? {
        "1m" => {}
        other => {
            return Err(NormalizeError::Unexpected(format!(
                "kline interval {other:?}"
            )));
        }
    }
    // Closed bars only (ADR-028 supporting rules).
    if !req(bar.x, "k.x")? {
        return Ok(None);
    }
    Ok(Some(MarketEvent::Kline(Kline {
        open_time: time(bar.t, "k.t")?,
        close_time: time(bar.close_time, "k.T")?,
        open: decimal::<Price>(bar.o, "k.o")?,
        high: decimal::<Price>(bar.h, "k.h")?,
        low: decimal::<Price>(bar.l, "k.l")?,
        close: decimal::<Price>(bar.c, "k.c")?,
        volume: decimal::<Qty>(bar.v, "k.v")?,
        taker_buy_volume: decimal::<Qty>(bar.taker_buy_volume, "k.V")?,
        trade_count: req(bar.n, "k.n")?,
    })))
}

#[derive(Deserialize)]
struct OpenInterestMsg<'a> {
    #[serde(borrow)]
    symbol: Option<&'a str>,
    #[serde(rename = "openInterest", borrow)]
    open_interest: Option<&'a str>,
    time: Option<i64>,
}

fn parse_open_interest(symbol: &str, payload: &[u8]) -> Result<MarketEvent, NormalizeError> {
    let msg: OpenInterestMsg<'_> = from_json(payload)?;
    expect_symbol(msg.symbol, "symbol", symbol)?;
    Ok(MarketEvent::OpenInterest(OpenInterest {
        time: time(msg.time, "time")?,
        open_interest: decimal::<Qty>(msg.open_interest, "openInterest")?,
        resolution_ms: OI_POLL_INTERVAL_MS,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::event::Stream;

    const SYMBOL: &str = "BTCUSDT";

    const AGG: &str = r#"{"e":"aggTrade","E":1791269849801,"a":3476312671,"s":"BTCUSDT","p":"85299.90","q":"0.004","nq":"0.004","f":8149080393,"l":8149080393,"T":1791269849651,"m":true,"st":1}"#;
    const MARK: &str = r#"{"e":"markPriceUpdate","E":1791269856000,"s":"BTCUSDT","p":"85296.60260870","ap":"85296.60260870","P":"85321.91711993","i":"85345.93826087","r":"0.00000523","T":1791273600000,"st":1}"#;
    const FORCE: &str = r#"{"e":"forceOrder","E":1791269900123,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.014","p":"85100.0","ap":"85150.5","X":"FILLED","l":"0.014","z":"0.014","T":1791269900120}}"#;
    const KLINE_OPEN: &str = r#"{"e":"kline","E":1791269859051,"s":"BTCUSDT","k":{"t":1791269820000, "T":1791269879999, "s":"BTCUSDT", "i":"1m", "f":8149080204, "L":8149080532, "o":"85300.00", "c":"85296.00", "h":"85300.00", "l":"85296.00", "v":"8.410", "n":321, "x":false, "q":"717368.32060", "V":"4.732", "Q":"403639.25130", "B":"0"}}"#;
    const OI: &str = r#"{"symbol":"BTCUSDT","openInterest":"95253.475","time":1791269771665}"#;

    fn closed_kline() -> String {
        KLINE_OPEN.replace(r#""x":false"#, r#""x":true"#)
    }

    fn parse_ok(stream: BinanceStream, payload: &str) -> MarketEvent {
        parse(stream, SYMBOL, payload.as_bytes())
            .expect("parses")
            .expect("has an event")
    }

    fn parse_err(stream: BinanceStream, payload: &str) -> NormalizeError {
        parse(stream, SYMBOL, payload.as_bytes()).expect_err("rejected")
    }

    #[test]
    fn agg_trade_maps_exact_units_and_maker_flag() {
        let event = parse_ok(BinanceStream::AggTrade, AGG);
        assert_eq!(
            event,
            MarketEvent::Trade(Trade {
                time: EventTime::from_millis(1_791_269_849_651),
                trade_id: 3_476_312_671,
                price: Price::from_units(8_529_990_000_000),
                qty: Qty::from_units(400_000),
                aggressor: Aggressor::Sell,
            })
        );
        let taker_buy = AGG.replace(r#""m":true"#, r#""m":false"#);
        let MarketEvent::Trade(trade) = parse_ok(BinanceStream::AggTrade, &taker_buy) else {
            panic!("not a trade");
        };
        assert_eq!(trade.aggressor, Aggressor::Buy);
    }

    #[test]
    fn mark_price_maps_every_field() {
        assert_eq!(
            parse_ok(BinanceStream::MarkPrice, MARK),
            MarketEvent::MarkPrice(MarkPrice {
                time: EventTime::from_millis(1_791_269_856_000),
                mark_price: Price::from_units(8_529_660_260_870),
                index_price: Price::from_units(8_534_593_826_087),
                funding_rate: Rate::from_units(523),
                next_funding_time: EventTime::from_millis(1_791_273_600_000),
            })
        );
    }

    #[test]
    fn force_order_maps_side_and_fill() {
        assert_eq!(
            parse_ok(BinanceStream::ForceOrder, FORCE),
            MarketEvent::Liquidation(Liquidation {
                time: EventTime::from_millis(1_791_269_900_120),
                aggressor: Aggressor::Sell,
                price: Price::from_units(8_510_000_000_000),
                avg_price: Price::from_units(8_515_050_000_000),
                filled_qty: Qty::from_units(1_400_000),
            })
        );
        let buy = FORCE.replace(r#""S":"SELL""#, r#""S":"BUY""#);
        let MarketEvent::Liquidation(liq) = parse_ok(BinanceStream::ForceOrder, &buy) else {
            panic!("not a liquidation");
        };
        assert_eq!(liq.aggressor, Aggressor::Buy);
        let odd = FORCE.replace(r#""S":"SELL""#, r#""S":"SIDEWAYS""#);
        assert!(matches!(
            parse_err(BinanceStream::ForceOrder, &odd),
            NormalizeError::Unexpected(_)
        ));
    }

    #[test]
    fn klines_emit_closed_bars_only() {
        assert_eq!(
            parse(BinanceStream::Kline1m, SYMBOL, KLINE_OPEN.as_bytes()),
            Ok(None)
        );
        assert_eq!(
            parse_ok(BinanceStream::Kline1m, &closed_kline()),
            MarketEvent::Kline(Kline {
                open_time: EventTime::from_millis(1_791_269_820_000),
                close_time: EventTime::from_millis(1_791_269_879_999),
                open: Price::from_units(8_530_000_000_000),
                high: Price::from_units(8_530_000_000_000),
                low: Price::from_units(8_529_600_000_000),
                close: Price::from_units(8_529_600_000_000),
                volume: Qty::from_units(841_000_000),
                taker_buy_volume: Qty::from_units(473_200_000),
                trade_count: 321,
            })
        );
        let five = closed_kline().replace(r#""i":"1m""#, r#""i":"5m""#);
        assert!(matches!(
            parse_err(BinanceStream::Kline1m, &five),
            NormalizeError::Unexpected(_)
        ));
    }

    #[test]
    fn open_interest_carries_the_poll_resolution() {
        let event = parse_ok(BinanceStream::OpenInterest, OI);
        assert_eq!(
            event,
            MarketEvent::OpenInterest(OpenInterest {
                time: EventTime::from_millis(1_791_269_771_665),
                open_interest: Qty::from_units(9_525_347_500_000),
                resolution_ms: 10_000,
            })
        );
        assert_eq!(event.stream(), Stream::OpenInterest);
    }

    #[test]
    fn record_time_uses_the_ordering_field_per_stream() {
        let cases = [
            (BinanceStream::AggTrade, AGG.to_owned(), 1_791_269_849_651),
            (BinanceStream::MarkPrice, MARK.to_owned(), 1_791_269_856_000),
            (
                BinanceStream::ForceOrder,
                FORCE.to_owned(),
                1_791_269_900_120,
            ),
            // An open bar is ordered at its future close time too.
            (
                BinanceStream::Kline1m,
                KLINE_OPEN.to_owned(),
                1_791_269_879_999,
            ),
            (
                BinanceStream::OpenInterest,
                OI.to_owned(),
                1_791_269_771_665,
            ),
        ];
        for (stream, payload, expected) in cases {
            assert_eq!(
                record_time(stream, payload.as_bytes()),
                Some(EventTime::from_millis(expected)),
                "{stream:?}"
            );
        }
    }

    #[test]
    fn record_time_falls_back_to_push_time() {
        let no_trade_time = AGG.replace(r#","T":1791269849651"#, "");
        assert_eq!(
            record_time(BinanceStream::AggTrade, no_trade_time.as_bytes()),
            Some(EventTime::from_millis(1_791_269_849_801))
        );
        let no_order = r#"{"e":"forceOrder","E":1791269900123}"#;
        assert_eq!(
            record_time(BinanceStream::ForceOrder, no_order.as_bytes()),
            Some(EventTime::from_millis(1_791_269_900_123))
        );
        assert_eq!(
            record_time(BinanceStream::OpenInterest, br#"{"symbol":"BTCUSDT"}"#),
            None
        );
        assert_eq!(record_time(BinanceStream::AggTrade, b"not json"), None);
    }

    #[test]
    fn missing_fields_are_named() {
        let no_qty = AGG.replace(r#""q":"0.004","#, "");
        assert_eq!(
            parse_err(BinanceStream::AggTrade, &no_qty),
            NormalizeError::Missing("q")
        );
        let no_order = r#"{"e":"forceOrder","E":1}"#;
        assert_eq!(
            parse_err(BinanceStream::ForceOrder, no_order),
            NormalizeError::Missing("o")
        );
        let no_close = KLINE_OPEN.replace(r#""x":false, "#, "");
        assert_eq!(
            parse_err(BinanceStream::Kline1m, &no_close),
            NormalizeError::Missing("k.x")
        );
    }

    #[test]
    fn non_positive_trade_quantities_are_rejected() {
        for qty in ["0", "0.00000000", "-0", "-0.004"] {
            let payload = AGG.replace(r#""q":"0.004""#, &format!(r#""q":"{qty}""#));
            match parse_err(BinanceStream::AggTrade, &payload) {
                NormalizeError::Unexpected(detail) => {
                    assert!(detail.starts_with("q: "), "{qty}: {detail}");
                }
                other => panic!("{qty}: {other:?}"),
            }
        }
        // The smallest positive unit still parses.
        let smallest = AGG.replace(r#""q":"0.004""#, r#""q":"0.00000001""#);
        let MarketEvent::Trade(trade) = parse_ok(BinanceStream::AggTrade, &smallest) else {
            panic!("not a trade");
        };
        assert_eq!(trade.qty, Qty::from_units(1));
    }

    #[test]
    fn number_typed_decimals_are_rejected_not_coerced() {
        let number = AGG.replace(r#""p":"85299.90""#, r#""p":63542.1"#);
        assert!(matches!(
            parse_err(BinanceStream::AggTrade, &number),
            NormalizeError::Json(_)
        ));
    }

    #[test]
    fn inexact_decimals_are_rejected() {
        let long = MARK.replace(r#""r":"0.00000523""#, r#""r":"0.000005231""#);
        assert_eq!(
            parse_err(BinanceStream::MarkPrice, &long),
            NormalizeError::Decimal {
                field: "r",
                error: ParseDecimalError::TooManyDecimals
            }
        );
    }

    #[test]
    fn exponent_decimals_stay_rejected_on_the_live_path() {
        // The live streams send plain decimals (`"r":"0.00000016"`); only
        // the archive normalizer accepts an exponent (ADR-034).
        let exponent = MARK.replace(r#""r":"0.00000523""#, r#""r":"5.23E-6""#);
        assert_eq!(
            parse_err(BinanceStream::MarkPrice, &exponent),
            NormalizeError::Decimal {
                field: "r",
                error: ParseDecimalError::Malformed
            }
        );
    }

    #[test]
    fn wrong_symbol_or_event_type_is_rejected() {
        let eth = AGG.replace(r#""s":"BTCUSDT""#, r#""s":"ETHUSDT""#);
        assert!(matches!(
            parse_err(BinanceStream::AggTrade, &eth),
            NormalizeError::Unexpected(_)
        ));
        let trade = AGG.replace(r#""e":"aggTrade""#, r#""e":"trade""#);
        assert!(matches!(
            parse_err(BinanceStream::AggTrade, &trade),
            NormalizeError::Unexpected(_)
        ));
        // A mark-price payload on the trade stream.
        assert!(matches!(
            parse_err(BinanceStream::AggTrade, MARK),
            NormalizeError::Unexpected(_)
        ));
        let oi_eth = OI.replace("BTCUSDT", "ETHUSDT");
        assert!(matches!(
            parse_err(BinanceStream::OpenInterest, &oi_eth),
            NormalizeError::Unexpected(_)
        ));
        assert!(matches!(
            parse_err(BinanceStream::AggTrade, "[1,2]"),
            NormalizeError::Json(_)
        ));
    }
}
