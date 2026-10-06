//! Reference cross-check of `volatility.atr.1h@1` and
//! `volatility.regime.1h@1` (ADR-033, decision 9).
//!
//! Real BTCUSDT perpetual 1h klines (Binance public data archive, 2024-07
//! and 2024-08) are pushed as complete 1h bars into [`AtrRegimeSeries`], and
//! every row is compared with the values an exact-arithmetic transcription
//! of the Pine Script v5 definitions computed on the same data
//! (`tools/reference/atr_regime_reference.py`; provenance in
//! `tests/fixtures/README.md`).

use mie_domain::bars::{Bar, Ohlc, Timeframe};
use mie_domain::feature::FeatureValue;
use mie_domain::num::Price;
use mie_domain::time::EventTime;
use mie_domain::volatility::AtrRegimeSeries;
use std::collections::BTreeSet;

const KLINES: &str = include_str!("fixtures/btcusdt-1h-2024-07-08.csv");
const REFERENCE: &str = include_str!("fixtures/atr-regime-reference.tsv");

/// The fixed-point ATR stays within 7 units of 1e-8 of the exact value
/// (ADR-033, decision 2): 7·10^4 units of 1e-12, plus the reference's own
/// rounding to 12 decimals.
const ATR_TOLERANCE_PICO: i128 = 70_001;

/// One reference row: open time, ATR in units of 1e-12, percentile text and
/// label; `None` while warming up.
struct Expected {
    open_time: i64,
    atr_pico: Option<i128>,
    percentile: Option<String>,
    label: Option<String>,
}

fn klines() -> Vec<Bar> {
    let mut lines = KLINES.lines();
    assert_eq!(lines.next(), Some("open_time,open,high,low,close"));
    lines
        .map(|line| {
            let fields: Vec<&str> = line.split(',').collect();
            let [open_time, open, high, low, close] = fields[..] else {
                panic!("malformed kline row: {line}")
            };
            let price = |text: &str| text.parse::<Price>().unwrap();
            let open_time = EventTime::from_millis(open_time.parse().unwrap());
            Bar {
                ohlc: Some(Ohlc {
                    open: price(open),
                    high: price(high),
                    low: price(low),
                    close: price(close),
                }),
                trade_count: 1,
                ..Bar::empty(Timeframe::H1, open_time)
            }
        })
        .collect()
}

/// A decimal with exactly 12 places as units of 1e-12.
fn pico(text: &str) -> i128 {
    let (whole, fraction) = text.split_once('.').unwrap();
    assert_eq!(fraction.len(), 12, "{text}");
    whole.parse::<i128>().unwrap() * 1_000_000_000_000 + fraction.parse::<i128>().unwrap()
}

fn reference() -> Vec<Expected> {
    REFERENCE
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let fields: Vec<&str> = line.split('\t').collect();
            let [open_time, atr, percentile, label] = fields[..] else {
                panic!("malformed reference row: {line}")
            };
            let present = |text: &str| (text != "-").then(|| text.to_owned());
            Expected {
                open_time: open_time.parse().unwrap(),
                atr_pico: (atr != "-").then(|| pico(atr)),
                percentile: present(percentile),
                label: present(label),
            }
        })
        .collect()
}

#[test]
fn atr_and_regime_match_the_pine_reference_on_real_klines() {
    let bars = klines();
    let expected = reference();
    assert_eq!(bars.len(), 1_488);
    assert_eq!(expected.len(), bars.len());

    let mut series = AtrRegimeSeries::new();
    let mut labels = BTreeSet::new();
    let mut worst = 0;
    for (row, (bar, expected)) in bars.iter().zip(&expected).enumerate() {
        assert_eq!(bar.open_time.as_millis(), expected.open_time, "row {row}");
        series.push(bar).unwrap();

        match (series.atr(), expected.atr_pico) {
            (FeatureValue::Ready(atr), Some(reference)) => {
                let ours = i128::from(atr.units()) * 10_000;
                let deviation = (ours - reference).abs();
                assert!(
                    deviation <= ATR_TOLERANCE_PICO,
                    "row {row}: ATR {atr} deviates {deviation}e-12 from the reference"
                );
                worst = worst.max(deviation);
            }
            (FeatureValue::WarmingUp { .. }, None) => {}
            (ours, reference) => {
                panic!("row {row}: ATR readiness differs: {ours:?} vs {reference:?}")
            }
        }

        match (series.regime(), &expected.percentile, &expected.label) {
            (FeatureValue::Ready(regime), Some(percentile), Some(label)) => {
                assert_eq!(
                    &format!("{:.1}", regime.atr_percentile.value()),
                    percentile,
                    "row {row}"
                );
                assert_eq!(&regime.label.to_string(), label, "row {row}");
                labels.insert(regime.label);
            }
            (FeatureValue::WarmingUp { .. }, None, None) => {}
            (ours, percentile, _) => {
                panic!("row {row}: regime readiness differs: {ours:?} vs {percentile:?}")
            }
        }
    }
    // Ready from row 213, the 214th sample (ADR-033, decision 5).
    assert_eq!(
        expected.iter().filter(|row| row.label.is_none()).count(),
        213
    );
    assert_eq!(labels.len(), 4, "every label occurs: {labels:?}");
    assert!(worst <= ATR_TOLERANCE_PICO);
}
