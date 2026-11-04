//! The lines of the `backtest` outcome kind of result text v1 (ADR-060
//! D12): the counts, one `trade` line per record and the `metrics` lines,
//! with their name tables. `research::result` frames them with the
//! provenance lines and the spec, and names the line of every error these
//! parsers return.
//!
//! The parsers check the grammar and the line shape of each exit kind.
//! They accept non-canonical spellings of a value (`01`, `+1`,
//! `-0.00000000`), which the caller's canonical comparison refuses, as for
//! the other outcome kinds.

use super::{
    BacktestMetrics, CensorReason, Exit, Fill, Metrics, PositionSide, REGIME_BUCKETS,
    SimulationStats, SkipCounts, TradeRecord,
};
use crate::feature::{FeatureKey, parse_versioned_id};
use crate::num::{Price, Rate};
use crate::order::{CanonicalKey, EventKind};
use crate::regime::RegimeLabel;
use crate::time::EventTime;

#[cfg(test)]
mod tests;

/// Every event kind, in rank order (ADR-028).
const EVENT_KINDS: [EventKind; 9] = [
    EventKind::FeedGap,
    EventKind::BookSnapshot,
    EventKind::Trade,
    EventKind::Liquidation,
    EventKind::BookUpdate,
    EventKind::MarkPrice,
    EventKind::FundingSettlement,
    EventKind::OpenInterest,
    EventKind::Kline,
];

/// The event-kind name of the trade line's decision key: ADR-057 D2's
/// `event_kind_name` strings, which the parquet adapter stores and a test
/// there pins against this table (ADR-060 D12).
pub const fn event_kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::FeedGap => "feed_gap",
        EventKind::BookSnapshot => "book_snapshot",
        EventKind::Trade => "trade",
        EventKind::Liquidation => "liquidation",
        EventKind::BookUpdate => "book_update",
        EventKind::MarkPrice => "mark_price",
        EventKind::FundingSettlement => "funding_settlement",
        EventKind::OpenInterest => "open_interest",
        EventKind::Kline => "kline",
    }
}

/// The event kind named `name` by [`event_kind_name`]; `None` for any
/// other text.
pub fn event_kind_from_name(name: &str) -> Option<EventKind> {
    EVENT_KINDS
        .into_iter()
        .find(|&kind| event_kind_name(kind) == name)
}

/// Every censor reason, in the order of ADR-060 D12's token list.
const CENSOR_REASONS: [CensorReason; 6] = [
    CensorReason::Gap,
    CensorReason::Rejected,
    CensorReason::EndOfData,
    CensorReason::ChainRestart,
    CensorReason::FundingGap,
    CensorReason::Overflow,
];

/// The `skipped` line's pair names, in their order.
const SKIP_FIELDS: [&str; 5] = [
    "overlap",
    "stale_reference",
    "no_side",
    "no_risk",
    "stop_side",
];

/// The trade line's pair names, in their order (ADR-060 D12).
const TRADE_FIELDS: [&str; 17] = [
    "side",
    "regime",
    "reference",
    "stop",
    "target",
    "risk",
    "entry",
    "exit",
    "exit_at",
    "fill",
    "funding",
    "gross",
    "net",
    "displacement",
    "mfe",
    "mae",
    "censor",
];

/// A metrics line's pair names, in their order (ADR-060 D11, D12).
const METRIC_FIELDS: [&str; 12] = [
    "trades",
    "censored",
    "unfilled",
    "expectancy",
    "mean_gross",
    "profit_factor",
    "win_rate",
    "max_drawdown",
    "time_to_target",
    "mean_mfe",
    "mean_mae",
    "mean_displacement",
];

/// The `skipped` line's form, for error messages.
pub(crate) const SKIPPED_FORM: &str =
    "skipped overlap=<n> stale_reference=<n> no_side=<n> no_risk=<n> stop_side=<n>";

/// `value`, or `-` for `None`.
fn or_dash(value: Option<impl ToString>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

/// `-` as `None`, anything else through `parse`; `None` when `parse`
/// refuses it.
fn dash_or<T>(text: &str, parse: impl FnOnce(&str) -> Option<T>) -> Option<Option<T>> {
    if text == "-" {
        Some(None)
    } else {
        parse(text).map(Some)
    }
}

fn price(text: &str) -> Option<Price> {
    text.parse().ok()
}

fn rate(text: &str) -> Option<Rate> {
    text.parse().ok()
}

fn millis(text: &str) -> Option<EventTime> {
    text.parse().ok().map(EventTime::from_millis)
}

/// `<ms>:<price>`.
fn fill(text: &str) -> Option<Fill> {
    let (at, value) = text.split_once(':')?;
    Some(Fill {
        at: millis(at)?,
        price: price(value)?,
    })
}

fn render_fill(value: Option<Fill>) -> String {
    value.map_or_else(
        || "-".to_owned(),
        |fill| format!("{}:{}", fill.at.as_millis(), fill.price),
    )
}

/// The values of `name=value` pairs with exactly `names`, in that order.
fn pairs<'a, const N: usize>(tokens: &[&'a str], names: [&str; N]) -> Result<[&'a str; N], String> {
    if tokens.len() != N {
        return Err(format!(
            "expected {N} `name=value` fields ({}), found {}",
            names.join(", "),
            tokens.len()
        ));
    }
    let mut values = [""; N];
    for ((value, token), name) in values.iter_mut().zip(tokens).zip(names) {
        *value = token
            .strip_prefix(name)
            .and_then(|rest| rest.strip_prefix('='))
            .ok_or_else(|| format!("expected `{name}=` where {token:?} stands"))?;
    }
    Ok(values)
}

/// A field value parsed by `parse`, or the error naming the field and its
/// form.
fn value<T>(
    name: &str,
    form: &str,
    text: &str,
    parse: impl FnOnce(&str) -> Option<T>,
) -> Result<T, String> {
    parse(text).ok_or_else(|| format!("`{name}=` expects {form}, found {text:?}"))
}

/// The four count lines (ADR-060 D12): `triggers`, `filtered`, `skipped`
/// and `reach-backs`.
pub(crate) fn render_counts(stats: &SimulationStats, out: &mut String) {
    let SkipCounts {
        overlap,
        stale_reference,
        no_side,
        no_risk,
        stop_side,
    } = stats.skipped;
    out.push_str(&format!(
        "triggers {}\n\
         filtered {}\n\
         skipped overlap={overlap} stale_reference={stale_reference} no_side={no_side} \
         no_risk={no_risk} stop_side={stop_side}\n\
         reach-backs {}\n",
        stats.triggers, stats.filtered, stats.reach_backs
    ));
}

/// `<key> <n>`.
pub(crate) fn parse_count(text: &str, key: &str) -> Result<u64, String> {
    text.strip_prefix(key)
        .and_then(|rest| rest.strip_prefix(' '))
        .and_then(|n| n.parse().ok())
        .ok_or_else(|| format!("expected `{key} <n>`"))
}

/// The `skipped` line.
pub(crate) fn parse_skipped(text: &str) -> Result<SkipCounts, String> {
    let tokens: Vec<&str> = text.split(' ').collect();
    let Some((&"skipped", fields)) = tokens.split_first() else {
        return Err(format!("expected `{SKIPPED_FORM}`"));
    };
    let values = pairs(fields, SKIP_FIELDS)?;
    let mut counts = [0_u64; 5];
    for ((count, text), name) in counts.iter_mut().zip(values).zip(SKIP_FIELDS) {
        *count = value(name, "<n>", text, |n| n.parse().ok())?;
    }
    let [overlap, stale_reference, no_side, no_risk, stop_side] = counts;
    Ok(SkipCounts {
        overlap,
        stale_reference,
        no_side,
        no_risk,
        stop_side,
    })
}

/// D4's conservation, `triggers` = `filtered` + Σ`skipped` + `trades`,
/// with checked sums: a sum that overflows `u64` breaks it.
pub(crate) fn conserves(stats: &SimulationStats, trades: usize) -> bool {
    let s = stats.skipped;
    [
        s.overlap,
        s.stale_reference,
        s.no_side,
        s.no_risk,
        s.stop_side,
    ]
    .into_iter()
    .try_fold(stats.filtered, u64::checked_add)
    .zip(u64::try_from(trades).ok())
    .and_then(|(counted, trades)| counted.checked_add(trades))
        == Some(stats.triggers)
}

/// The `regime=` value: `none` for an empty list, else the labels or `-`,
/// comma-separated.
fn render_regime(labels: &[Option<RegimeLabel>]) -> String {
    if labels.is_empty() {
        return "none".to_owned();
    }
    labels
        .iter()
        .map(|label| label.map_or("-", RegimeLabel::as_str))
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_regime(text: &str) -> Result<Vec<Option<RegimeLabel>>, String> {
    if text == "none" {
        return Ok(Vec::new());
    }
    text.split(',')
        .map(|name| {
            REGIME_BUCKETS
                .into_iter()
                .find(|bucket| bucket.map_or("-", RegimeLabel::as_str) == name)
                .ok_or_else(|| {
                    format!(
                        "`regime=` label {name:?} is not LOW, MEDIUM, HIGH, EXTREME or -, \
                         and the list is not `none`"
                    )
                })
        })
        .collect()
}

/// One `trade` line (ADR-060 D12).
pub(crate) fn render_trade(record: &TradeRecord, out: &mut String) {
    let censor = match record.exit {
        Exit::Censored(reason) => reason.as_str(),
        _ => "-",
    };
    out.push_str(&format!(
        "trade {} {} {} side={} regime={} reference={} stop={} target={} risk={} entry={} \
         exit={} exit_at={} fill={} funding={} gross={} net={} displacement={} mfe={} mae={} \
         censor={censor}\n",
        record.decision.time.as_millis(),
        event_kind_name(record.decision.kind),
        record.decision.seq,
        record.side.as_str(),
        render_regime(&record.regime),
        record.reference,
        record.stop,
        record.target,
        record.risk,
        render_fill(record.entry),
        record.exit.as_str(),
        record.exit_at.as_millis(),
        render_fill(record.fill),
        or_dash(record.funding),
        or_dash(record.gross),
        or_dash(record.net),
        or_dash(record.displacement),
        or_dash(record.mfe),
        or_dash(record.mae),
    ));
}

/// One `trade` line, grammar only: the caller checks it against the shape
/// of its exit kind ([`check_shape`]).
pub(crate) fn parse_trade(text: &str) -> Result<TradeRecord, String> {
    let tokens: Vec<&str> = text.split(' ').collect();
    let [keyword, time, kind, seq, fields @ ..] = tokens.as_slice() else {
        return Err("expected `trade <decision ms> <event_kind> <seq> <fields>`".to_owned());
    };
    if *keyword != "trade" {
        return Err("expected `trade <decision ms> <event_kind> <seq> <fields>`".to_owned());
    }
    let decision = CanonicalKey {
        time: millis(time).ok_or_else(|| format!("the decision time {time:?} is not <ms>"))?,
        kind: event_kind_from_name(kind).ok_or_else(|| format!("{kind:?} is not an event kind"))?,
        seq: seq
            .parse()
            .map_err(|_| format!("the decision seq {seq:?} is not <n>"))?,
    };
    let [
        side,
        regime,
        reference,
        stop,
        target,
        risk,
        entry,
        exit,
        exit_at,
        exit_fill,
        funding,
        gross,
        net,
        displacement,
        mfe,
        mae,
        censor,
    ] = pairs(fields, TRADE_FIELDS)?;
    let r_value = |name: &str, text: &str| value(name, "<R> or `-`", text, |t| dash_or(t, rate));
    let price_value = |name: &str, text: &str| value(name, "<price>", text, price);
    let fill_value =
        |name: &str, text: &str| value(name, "<ms>:<price> or `-`", text, |t| dash_or(t, fill));
    let side = match side {
        "long" => PositionSide::Long,
        "short" => PositionSide::Short,
        other => return Err(format!("`side=` expects long or short, found {other:?}")),
    };
    let exit = match (exit, censor) {
        ("target", "-") => Exit::Target,
        ("stop", "-") => Exit::Stop,
        ("time", "-") => Exit::Time,
        ("unfilled", "-") => Exit::Unfilled,
        ("censored", reason) => Exit::Censored(
            CENSOR_REASONS
                .into_iter()
                .find(|known| known.as_str() == reason)
                .ok_or_else(|| {
                    format!(
                        "`censor=` on a censored line expects gap, rejected, end_of_data, \
                         chain_restart, funding_gap or overflow, found {reason:?}"
                    )
                })?,
        ),
        ("target" | "stop" | "time" | "unfilled", reason) => {
            return Err(format!(
                "`censor=` is `-` exactly when the exit is not censored, found {reason:?} \
                 on exit={exit}"
            ));
        }
        (other, _) => {
            return Err(format!(
                "`exit=` expects target, stop, time, unfilled or censored, found {other:?}"
            ));
        }
    };
    Ok(TradeRecord {
        decision,
        side,
        regime: parse_regime(regime)?,
        reference: price_value("reference", reference)?,
        stop: price_value("stop", stop)?,
        target: price_value("target", target)?,
        risk: price_value("risk", risk)?,
        entry: fill_value("entry", entry)?,
        exit,
        exit_at: value("exit_at", "<ms>", exit_at, millis)?,
        fill: fill_value("fill", exit_fill)?,
        funding: value("funding", "<price> or `-`", funding, |t| dash_or(t, price))?,
        gross: r_value("gross", gross)?,
        net: r_value("net", net)?,
        displacement: r_value("displacement", displacement)?,
        mfe: r_value("mfe", mfe)?,
        mae: r_value("mae", mae)?,
    })
}

/// ADR-060 D12's `-` cases, one shape per exit kind:
///
/// - target, stop and time: every value is set;
/// - unfilled: `entry`, `fill`, `funding`, `gross`, `net`,
///   `displacement`, `mfe` and `mae` are `-`;
/// - censored: `fill`, `funding`, `gross` and `net` are `-`, and `entry`,
///   `displacement`, `mfe` and `mae` are either all `-` or all set with
///   the entry fill strictly before the censor time.
pub(crate) fn check_shape(record: &TradeRecord) -> Result<(), String> {
    let exit_values = [
        record.fill.is_some(),
        record.funding.is_some(),
        record.gross.is_some(),
        record.net.is_some(),
    ];
    let entry_values = [
        record.entry.is_some(),
        record.displacement.is_some(),
        record.mfe.is_some(),
        record.mae.is_some(),
    ];
    let all = |values: [bool; 4]| values.iter().all(|&set| set);
    let none = |values: [bool; 4]| values.iter().all(|&set| !set);
    let exit = record.exit.as_str();
    match record.exit {
        Exit::Target | Exit::Stop | Exit::Time => {
            if all(exit_values) && all(entry_values) {
                Ok(())
            } else {
                Err(format!("an exit={exit} line sets every value"))
            }
        }
        Exit::Unfilled => {
            if none(exit_values) && none(entry_values) {
                Ok(())
            } else {
                Err(
                    "an exit=unfilled line has `-` for entry, fill, funding, gross, net, \
                     displacement, mfe and mae"
                        .to_owned(),
                )
            }
        }
        Exit::Censored(_) => {
            if !none(exit_values) {
                return Err(
                    "an exit=censored line has `-` for fill, funding, gross and net".to_owned(),
                );
            }
            if none(entry_values) {
                return Ok(());
            }
            if !all(entry_values) {
                return Err(
                    "an exit=censored line sets entry, displacement, mfe and mae together or \
                     none of them"
                        .to_owned(),
                );
            }
            match record.entry {
                Some(entry) if entry.at < record.exit_at => Ok(()),
                _ => Err(
                    "an exit=censored line keeps its entry only when the entry fill is \
                     strictly before exit_at"
                        .to_owned(),
                ),
            }
        }
    }
}

/// A metrics line's `<fields>`.
fn render_fields(metrics: &Metrics) -> String {
    format!(
        "trades={} censored={} unfilled={} expectancy={} mean_gross={} profit_factor={} \
         win_rate={} max_drawdown={} time_to_target={} mean_mfe={} mean_mae={} \
         mean_displacement={}",
        metrics.trades,
        metrics.censored,
        metrics.unfilled,
        or_dash(metrics.expectancy),
        or_dash(metrics.mean_gross),
        or_dash(metrics.profit_factor),
        or_dash(metrics.win_rate),
        or_dash(metrics.max_drawdown),
        or_dash(metrics.time_to_target_ms),
        or_dash(metrics.mean_mfe),
        or_dash(metrics.mean_mae),
        or_dash(metrics.mean_displacement),
    )
}

/// The `metrics` lines (ADR-060 D11, D12): `metrics all`, then for each of
/// `regime_features`, in the given order, one line per [`REGIME_BUCKETS`]
/// label.
pub(crate) fn render_metrics(
    metrics: &BacktestMetrics,
    regime_features: &[FeatureKey],
    out: &mut String,
) {
    out.push_str(&format!("metrics all {}\n", render_fields(&metrics.all)));
    for (feature, buckets) in regime_features.iter().zip(&metrics.regimes) {
        for (bucket, metrics) in REGIME_BUCKETS.iter().zip(buckets) {
            out.push_str(&format!(
                "metrics {feature} {} {}\n",
                bucket.map_or("-", RegimeLabel::as_str),
                render_fields(metrics)
            ));
        }
    }
}

/// Which records a metrics line covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricsScope {
    /// `metrics all`.
    All,
    /// `metrics <id@N> <LABEL|->`.
    Bucket,
}

/// The form of a metrics line, for error messages.
pub(crate) const METRICS_FORM: &str = "metrics <all|id@N LABEL|id@N -> <fields>";

/// One metrics line, grammar only: whether its values follow from the
/// trade lines is the caller's canonical comparison.
pub(crate) fn parse_metrics(text: &str) -> Result<(MetricsScope, Metrics), String> {
    let tokens: Vec<&str> = text.split(' ').collect();
    let (scope, fields) = match tokens.as_slice() {
        ["metrics", "all", fields @ ..] => (MetricsScope::All, fields),
        ["metrics", feature, label, fields @ ..] => {
            if parse_versioned_id(feature).is_none() {
                return Err(format!("{feature:?} is not a feature key `id@N`"));
            }
            if !REGIME_BUCKETS
                .into_iter()
                .any(|bucket| bucket.map_or("-", RegimeLabel::as_str) == *label)
            {
                return Err(format!(
                    "the bucket {label:?} is not LOW, MEDIUM, HIGH, EXTREME or -"
                ));
            }
            (MetricsScope::Bucket, fields)
        }
        _ => return Err(format!("expected `{METRICS_FORM}`")),
    };
    let [
        trades,
        censored,
        unfilled,
        expectancy,
        mean_gross,
        profit_factor,
        win_rate,
        max_drawdown,
        time_to_target,
        mean_mfe,
        mean_mae,
        mean_displacement,
    ] = pairs(fields, METRIC_FIELDS)?;
    let count = |name: &str, text: &str| value(name, "<n>", text, |n| n.parse().ok());
    let r_value = |name: &str, text: &str| value(name, "<R> or `-`", text, |t| dash_or(t, rate));
    let metrics = Metrics {
        trades: count("trades", trades)?,
        censored: count("censored", censored)?,
        unfilled: count("unfilled", unfilled)?,
        expectancy: r_value("expectancy", expectancy)?,
        mean_gross: r_value("mean_gross", mean_gross)?,
        profit_factor: r_value("profit_factor", profit_factor)?,
        win_rate: r_value("win_rate", win_rate)?,
        max_drawdown: r_value("max_drawdown", max_drawdown)?,
        time_to_target_ms: value("time_to_target", "<ms> or `-`", time_to_target, |t| {
            dash_or(t, |ms| ms.parse().ok())
        })?,
        mean_mfe: r_value("mean_mfe", mean_mfe)?,
        mean_mae: r_value("mean_mae", mean_mae)?,
        mean_displacement: r_value("mean_displacement", mean_displacement)?,
    };
    Ok((scope, metrics))
}
