//! Cross-check of event-time bars against exchange klines (ADR-031).
//!
//! Bars are built from trades only (ADR-028); exchange klines enter the
//! domain for this check alone. [`cross_check_klines`] drives any
//! [`MarketDataProvider`] — a synthetic tape, a live stream or an archive
//! replay — through the normal domain path and compares every complete bar
//! with the kline of its interval as the bar closes.

use crate::drive_observed;
use mie_domain::bars::{Bar, KlineField, Timeframe, kline_mismatches};
use mie_domain::event::{Kline, MarketEvent};
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::inbound::UseCaseError;
use mie_ports::outbound::MarketDataProvider;
use std::collections::BTreeMap;
use std::fmt;

/// A complete bar that disagrees with its kline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KlineMismatch {
    /// The bar's timeframe.
    pub timeframe: Timeframe,
    /// The bar's open time.
    pub open_time: EventTime,
    /// The differing fields, in [`KlineField`] order.
    pub fields: Vec<KlineField>,
}

/// The outcome of [`cross_check_klines`]. `Display` prints the summary
/// posted on an issue after a real-data run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KlineCheckReport {
    /// Events consumed.
    pub events: u64,
    /// Complete bars compared with a kline.
    pub compared: u64,
    /// Compared bars that matched their kline.
    pub matched: u64,
    /// Compared bars that did not.
    pub mismatches: Vec<KlineMismatch>,
    /// Closed bars skipped because they are incomplete (partial start or
    /// feed gap).
    pub incomplete_skipped: u64,
    /// Complete bars that closed without a kline for their interval.
    pub bars_without_kline: u64,
    /// Klines of a configured timeframe whose bar never closed complete or
    /// incomplete: before the first trade, or still open when the stream
    /// ended.
    pub klines_without_bar: u64,
    /// Klines whose interval is not one of [`Timeframe::ALL`].
    pub unconfigured_klines: u64,
    /// Compared bars whose trade count differs from the kline's. Not a
    /// mismatch: bars count aggregate trades, klines raw trades.
    pub trade_count_differences: u64,
}

impl KlineCheckReport {
    /// Whether every compared bar matched its kline.
    pub fn all_matched(&self) -> bool {
        self.mismatches.is_empty()
    }

    fn compare(&mut self, bar: &Bar, kline: Option<Kline>) {
        if !bar.is_complete() {
            self.incomplete_skipped += 1;
            return;
        }
        let Some(kline) = kline else {
            self.bars_without_kline += 1;
            return;
        };
        self.compared += 1;
        if bar.trade_count != kline.trade_count {
            self.trade_count_differences += 1;
        }
        let fields = kline_mismatches(bar, &kline);
        if fields.is_empty() {
            self.matched += 1;
        } else {
            self.mismatches.push(KlineMismatch {
                timeframe: bar.timeframe,
                open_time: bar.open_time,
                fields,
            });
        }
    }
}

impl fmt::Display for KlineCheckReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "kline cross-check over {} events", self.events)?;
        writeln!(
            f,
            "complete bars compared: {}, matched: {}, mismatched: {}",
            self.compared,
            self.matched,
            self.mismatches.len()
        )?;
        writeln!(f, "incomplete bars skipped: {}", self.incomplete_skipped)?;
        writeln!(
            f,
            "complete bars without a kline: {}",
            self.bars_without_kline
        )?;
        writeln!(
            f,
            "klines without a closed bar: {}",
            self.klines_without_bar
        )?;
        writeln!(
            f,
            "klines of an unconfigured interval: {}",
            self.unconfigured_klines
        )?;
        write!(
            f,
            "trade-count differences (not a mismatch): {}",
            self.trade_count_differences
        )?;
        for mismatch in &self.mismatches {
            let fields: Vec<String> = mismatch.fields.iter().map(ToString::to_string).collect();
            write!(
                f,
                "\nmismatch: {} {} {}",
                mismatch.timeframe,
                mismatch.open_time,
                fields.join(",")
            )?;
        }
        Ok(())
    }
}

/// Drives `provider` into `engine` and compares every closed complete bar
/// with the exchange kline of its interval.
///
/// A kline is delivered at its close time, the last millisecond of its
/// interval, so it is held until its bar closes at the next trades-stream
/// event. Incomplete bars are skipped (their kline is dropped with them).
///
/// # Errors
///
/// As [`drive_observed`]: the first provider failure or domain rejection.
pub fn cross_check_klines<P>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
) -> Result<KlineCheckReport, UseCaseError>
where
    P: MarketDataProvider + ?Sized,
{
    let mut report = KlineCheckReport::default();
    let mut pending: BTreeMap<(Timeframe, EventTime), Kline> = BTreeMap::new();
    report.events = drive_observed(provider, engine, |event, engine| {
        for bar in engine.closed_bars() {
            let kline = pending.remove(&(bar.timeframe, bar.open_time));
            report.compare(bar, kline);
        }
        if let MarketEvent::Kline(kline) = event {
            match Timeframe::of_kline(kline) {
                Some(timeframe) => {
                    pending.insert((timeframe, kline.open_time), *kline);
                }
                None => report.unconfigured_klines += 1,
            }
        }
    })?;
    report.klines_without_bar = u64::try_from(pending.len()).unwrap_or(u64::MAX);
    Ok(report)
}
