//! The four tables of the outcome dataset (ADR-057 D1, D2; ADR-058 D2):
//! their Arrow schemas, their rows and the enum names they store.
//!
//! Every enum is stored as its snake_case name, produced by an exhaustive
//! `match` (or the domain's own `as_str`/`label`, which are), never by
//! `Debug`: a renamed Rust variant must not change a stored dataset.

use super::OutcomeError;
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use mie_domain::candidate::effort::EffortResult;
use mie_domain::conditional::label::{
    Censor, ForwardLabel, HorizonLabel, LabelProvenance, Passage, PassageLabel, Race,
};
use mie_domain::conditional::{ContextRecord, Sample};
use mie_domain::event::Aggressor;
use mie_domain::feature::{FeatureValue, catalog};
use mie_domain::location::{AuctionState, LevelKind, Region};
use mie_domain::num::{Price, Qty};
use mie_domain::order::EventKind;
use mie_domain::regime::RegimeLabel;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

/// Key-value metadata key holding the dataset format version.
pub(crate) const KEY_FORMAT: &str = "mie.outcome.format";
/// Dataset format version written by this crate: 2 adds `effort.parquet`
/// and the order-flow columns of `contexts` (ADR-058 D2).
pub(crate) const FORMAT_VERSION: &str = "2";

/// Most rows per row group (ADR-057 D2): DuckDB's `DEFAULT_ROW_GROUP_SIZE`,
/// its own Parquet write default and the unit its Parquet scan hands to one
/// thread.
pub(crate) const ROW_GROUP_ROWS: usize = 122_880;

/// Precision of every price and quantity column: the most decimal digits
/// Parquet stores in an INT64 (ADR-057 D2).
pub(crate) const PRICE_PRECISION: u8 = 18;
/// Scale of every price and quantity column: the ADR-027 unit, 1e-8 (USDT
/// for prices, BTC for quantities).
pub(crate) const PRICE_SCALE: i8 = 8;
/// The largest magnitude, in units, that `Decimal128(18, 8)` holds.
const DECIMAL_MAX_UNITS: i64 = 999_999_999_999_999_999;

/// The names of the six run-provenance columns every table ends with
/// (ADR-054 D5, ADR-055 D10), in order.
pub(crate) const PROVENANCE_COLUMNS: [&str; 6] = [
    "data_version",
    "engine",
    "sampling",
    "sampling_fingerprint",
    "labeling",
    "labeling_fingerprint",
];

fn utc_ms() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

fn price() -> DataType {
    DataType::Decimal128(PRICE_PRECISION, PRICE_SCALE)
}

/// A quantity column: the same exact `Decimal128(18, 8)`, in the ADR-027
/// quantity unit (ADR-058 D2).
fn quantity() -> DataType {
    DataType::Decimal128(PRICE_PRECISION, PRICE_SCALE)
}

/// `fields` followed by the provenance columns.
fn with_provenance(mut fields: Vec<Field>) -> SchemaRef {
    fields.extend(
        PROVENANCE_COLUMNS
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false)),
    );
    Arc::new(Schema::new(fields))
}

/// The schema of `contexts.parquet`.
pub(crate) fn contexts_schema() -> SchemaRef {
    with_provenance(vec![
        Field::new("record", DataType::UInt64, false),
        Field::new("decision_time", utc_ms(), false),
        Field::new("event_kind", DataType::Utf8, false),
        Field::new("event_seq", DataType::UInt64, false),
        Field::new("ordinal", DataType::UInt32, false),
        Field::new("sample", DataType::Utf8, false),
        Field::new("minute_end", utc_ms(), true),
        Field::new("trigger", DataType::Utf8, true),
        Field::new("trigger_family", DataType::Utf8, true),
        Field::new("episode", DataType::Utf8, true),
        Field::new("opener", DataType::Boolean, true),
        Field::new("truncated", DataType::Boolean, true),
        Field::new("bridge", DataType::Boolean, true),
        Field::new("regime", DataType::Utf8, true),
        Field::new("regime_percentile", DataType::Float64, true),
        Field::new("levels_ready", DataType::Boolean, false),
        Field::new("at_value_edge", DataType::Boolean, true),
        Field::new("at_structural_extreme", DataType::Boolean, true),
        Field::new("auction_state", DataType::Utf8, true),
        Field::new("anchor", price(), false),
        Field::new("reference", price(), true),
        Field::new("atr_1h", price(), true),
        // Format 2 (ADR-058 D2): the order-flow context the studies read.
        Field::new("auction_side", DataType::Utf8, true),
        Field::new("cvd_utc_day", quantity(), true),
        Field::new("cvd_partial_start", DataType::Boolean, true),
        Field::new("cvd_feed_gap", DataType::Boolean, true),
    ])
}

/// The schema of `outcomes.parquet`.
pub(crate) fn outcomes_schema() -> SchemaRef {
    with_provenance(vec![
        Field::new("record", DataType::UInt64, false),
        Field::new("horizon", DataType::Utf8, false),
        Field::new("horizon_ms", DataType::Int64, false),
        Field::new("clear", DataType::Boolean, false),
        Field::new("lower", price(), true),
        Field::new("upper", price(), true),
        Field::new("race", DataType::Utf8, false),
        Field::new("race_at", utc_ms(), true),
        Field::new("same_ms", DataType::Boolean, true),
        Field::new("race_censor_at", utc_ms(), true),
        Field::new("race_censor_reason", DataType::Utf8, true),
        Field::new("up", price(), false),
        Field::new("up_at", utc_ms(), true),
        Field::new("down", price(), false),
        Field::new("down_at", utc_ms(), true),
        Field::new("last", price(), true),
        Field::new("censor_at", utc_ms(), true),
        Field::new("censor_reason", DataType::Utf8, true),
    ])
}

/// The schema of `passages.parquet`.
pub(crate) fn passages_schema() -> SchemaRef {
    with_provenance(vec![
        Field::new("record", DataType::UInt64, false),
        Field::new("target", DataType::Utf8, false),
        Field::new("price", price(), false),
        Field::new("passage", DataType::Utf8, false),
        Field::new("at", utc_ms(), true),
        Field::new("censor_at", utc_ms(), true),
        Field::new("censor_reason", DataType::Utf8, true),
    ])
}

/// The schema of `effort.parquet` (ADR-058 D2): one row per record and
/// effort window, in `catalog::CANDIDATE_EFFORT_RESULT` order. Every column
/// after `state` is NULL unless the value is ready.
pub(crate) fn effort_schema() -> SchemaRef {
    with_provenance(vec![
        Field::new("record", DataType::UInt64, false),
        Field::new("window", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("aggressor", DataType::Utf8, true),
        Field::new("effort", quantity(), true),
        Field::new("effort_high", quantity(), true),
        Field::new("progress", price(), true),
        Field::new("move_median", price(), true),
        Field::new("move_high", price(), true),
        Field::new("partial_start", DataType::Boolean, true),
        Field::new("feed_gap", DataType::Boolean, true),
        Field::new("active", DataType::Boolean, true),
    ])
}

/// The pinned writer properties of every table (ADR-057 D2).
pub(crate) fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_key_value_metadata(Some(vec![KeyValue::new(
            KEY_FORMAT.to_owned(),
            FORMAT_VERSION.to_owned(),
        )]))
        .build()
}

// ---------------------------------------------------------------------------
// Enum names. Each table is pinned by a test.

/// The snake_case name of an event kind (ADR-028).
pub(crate) const fn event_kind_name(kind: EventKind) -> &'static str {
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

/// The snake_case name of a regime label (ADR-017).
pub(crate) const fn regime_name(label: RegimeLabel) -> &'static str {
    match label {
        RegimeLabel::Low => "low",
        RegimeLabel::Medium => "medium",
        RegimeLabel::High => "high",
        RegimeLabel::Extreme => "extreme",
    }
}

/// The snake_case name of an auction state (ADR-044).
pub(crate) const fn auction_state_name(state: AuctionState) -> &'static str {
    match state {
        AuctionState::InsideValue => "inside_value",
        AuctionState::AtValueEdge => "at_value_edge",
        AuctionState::OutsideValue => "outside_value",
        AuctionState::Breakout => "breakout",
        AuctionState::FailedBreakout => "failed_breakout",
        AuctionState::FailedReclaim => "failed_reclaim",
        AuctionState::Acceptance => "acceptance",
    }
}

/// The snake_case name of a value region (ADR-044 D8): the side of value
/// an auction label refers to.
pub(crate) const fn region_name(region: Region) -> &'static str {
    match region {
        Region::Below => "below",
        Region::In => "in",
        Region::Above => "above",
    }
}

/// The snake_case name of a taker side (ADR-023).
pub(crate) const fn aggressor_name(aggressor: Aggressor) -> &'static str {
    match aggressor {
        Aggressor::Buy => "buy",
        Aggressor::Sell => "sell",
    }
}

/// The name of a feature value's validity state (ADR-029).
pub(crate) const fn value_state_name<T>(value: &FeatureValue<T>) -> &'static str {
    match value {
        FeatureValue::WarmingUp { .. } => "warming",
        FeatureValue::Ready(_) => "ready",
        FeatureValue::Unavailable { .. } => "unavailable",
    }
}

/// The name of a race outcome (ADR-055 D4).
pub(crate) const fn race_name(race: &Race) -> &'static str {
    match race {
        Race::Upper { .. } => "upper",
        Race::Lower { .. } => "lower",
        Race::Neither => "neither",
        Race::Censored(_) => "censored",
        Race::Unavailable => "unavailable",
    }
}

/// The name of a passage outcome (ADR-055 D8).
pub(crate) const fn passage_name(passage: &Passage) -> &'static str {
    match passage {
        Passage::AtDecision => "at_decision",
        Passage::Touched { .. } => "touched",
        Passage::NotWithin => "not_within",
        Passage::Censored(_) => "censored",
    }
}

/// ADR-051 R1's location bins of a context, read as the context
/// measurement reads them: whether a VAH or VAL, and whether a structural
/// high or low, is among the levels at the anchor. `None` while the levels
/// are not ready.
fn location_bins(record: &ContextRecord) -> Option<(bool, bool)> {
    let levels = record.context.levels.ready()?;
    let mut bins = (false, false);
    for level in &levels.levels {
        match level.kind {
            LevelKind::Vah | LevelKind::Val => bins.0 = true,
            LevelKind::StructuralHigh | LevelKind::StructuralLow => bins.1 = true,
            LevelKind::Poc
            | LevelKind::Hvn
            | LevelKind::Lvn
            | LevelKind::LiquidityCluster
            | LevelKind::PriorSweep
            | LevelKind::SfpRejectionZone
            | LevelKind::Vwap
            | LevelKind::ValidatedReference => {}
        }
    }
    Some(bins)
}

// ---------------------------------------------------------------------------
// Rows.

/// A price in units, checked against the `Decimal128(18, 8)` range.
fn units(column: &str, price: Price) -> Result<i64, OutcomeError> {
    checked(column, price.units(), &price)
}

/// A quantity in units, checked against the `Decimal128(18, 8)` range
/// (ADR-058 D2). Never rounded.
fn qty_units(column: &str, quantity: Qty) -> Result<i64, OutcomeError> {
    checked(column, quantity.units(), &quantity)
}

/// `units` if `Decimal128(18, 8)` holds it, else [`OutcomeError::Range`]
/// naming `column` and the value as `shown`.
fn checked(column: &str, units: i64, shown: &dyn std::fmt::Display) -> Result<i64, OutcomeError> {
    if units.unsigned_abs() > DECIMAL_MAX_UNITS.unsigned_abs() {
        return Err(OutcomeError::Range(format!(
            "{column} {shown} exceeds Decimal128({PRICE_PRECISION}, {PRICE_SCALE})"
        )));
    }
    Ok(units)
}

fn opt_units(column: &str, price: Option<Price>) -> Result<Option<i64>, OutcomeError> {
    price.map(|price| units(column, price)).transpose()
}

/// One row of `contexts.parquet`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ContextRow {
    pub(crate) record: u64,
    pub(crate) decision_time: i64,
    pub(crate) event_kind: &'static str,
    pub(crate) event_seq: u64,
    pub(crate) ordinal: u32,
    pub(crate) sample: &'static str,
    pub(crate) minute_end: Option<i64>,
    pub(crate) trigger: Option<String>,
    pub(crate) trigger_family: Option<&'static str>,
    pub(crate) episode: Option<String>,
    pub(crate) opener: Option<bool>,
    pub(crate) truncated: Option<bool>,
    pub(crate) bridge: Option<bool>,
    pub(crate) regime: Option<&'static str>,
    pub(crate) regime_percentile: Option<f64>,
    pub(crate) levels_ready: bool,
    pub(crate) at_value_edge: Option<bool>,
    pub(crate) at_structural_extreme: Option<bool>,
    pub(crate) auction_state: Option<&'static str>,
    pub(crate) anchor: i64,
    pub(crate) reference: Option<i64>,
    pub(crate) atr_1h: Option<i64>,
    pub(crate) auction_side: Option<&'static str>,
    pub(crate) cvd_utc_day: Option<i64>,
    pub(crate) cvd_partial_start: Option<bool>,
    pub(crate) cvd_feed_gap: Option<bool>,
}

impl ContextRow {
    /// The row of `record`, the `number`-th record, at `ordinal` among its
    /// event's records.
    pub(crate) fn new(
        number: u64,
        ordinal: u32,
        record: &ContextRecord,
    ) -> Result<Self, OutcomeError> {
        let (minute_end, trigger, trigger_family, episode) = match &record.sample {
            Sample::Baseline { minute_end } => (Some(minute_end.as_millis()), None, None, None),
            Sample::Trigger { trigger, episode } => (
                None,
                Some(trigger.feature.to_string()),
                Some(trigger.family.as_str()),
                Some(*episode),
            ),
        };
        let regime = record.bias.regime.ready();
        let bins = location_bins(record);
        let auction = record.context.auction.ready();
        let cvd = record.state.cvd_utc_day.ready();
        Ok(Self {
            record: number,
            decision_time: record.event.time.as_millis(),
            event_kind: event_kind_name(record.event.kind),
            event_seq: record.event.seq,
            ordinal,
            sample: match record.sample {
                Sample::Baseline { .. } => "baseline",
                Sample::Trigger { .. } => "trigger",
            },
            minute_end,
            trigger,
            trigger_family,
            episode: episode.map(|episode| episode.id.to_string()),
            opener: episode.map(|episode| episode.opener),
            truncated: episode.map(|episode| episode.truncated),
            bridge: episode.map(|episode| episode.bridge),
            regime: regime.map(|regime| regime_name(regime.label)),
            regime_percentile: regime.map(|regime| regime.atr_percentile.value()),
            levels_ready: bins.is_some(),
            at_value_edge: bins.map(|bins| bins.0),
            at_structural_extreme: bins.map(|bins| bins.1),
            auction_state: auction.map(|status| auction_state_name(status.state)),
            anchor: units("anchor", record.context.anchor)?,
            reference: opt_units("reference", record.state.last_price.ready().copied())?,
            atr_1h: opt_units("atr_1h", record.state.atr.ready().copied())?,
            auction_side: auction.and_then(|status| status.side).map(region_name),
            cvd_utc_day: cvd
                .map(|cvd| qty_units("cvd_utc_day", cvd.cvd))
                .transpose()?,
            cvd_partial_start: cvd.map(|cvd| cvd.coverage.partial_start),
            cvd_feed_gap: cvd.map(|cvd| cvd.coverage.feed_gap),
        })
    }
}

/// One row of `outcomes.parquet`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutcomeRow {
    pub(crate) record: u64,
    pub(crate) horizon: &'static str,
    pub(crate) horizon_ms: i64,
    pub(crate) clear: bool,
    pub(crate) lower: Option<i64>,
    pub(crate) upper: Option<i64>,
    pub(crate) race: &'static str,
    pub(crate) race_at: Option<i64>,
    pub(crate) same_ms: Option<bool>,
    pub(crate) race_censor_at: Option<i64>,
    pub(crate) race_censor_reason: Option<&'static str>,
    pub(crate) up: i64,
    pub(crate) up_at: Option<i64>,
    pub(crate) down: i64,
    pub(crate) down_at: Option<i64>,
    pub(crate) last: Option<i64>,
    pub(crate) censor_at: Option<i64>,
    pub(crate) censor_reason: Option<&'static str>,
}

fn censor_at(censor: Option<Censor>) -> Option<i64> {
    censor.map(|censor| censor.at.as_millis())
}

fn censor_reason(censor: Option<Censor>) -> Option<&'static str> {
    censor.map(|censor| censor.reason.as_str())
}

impl OutcomeRow {
    /// The row of `label` for the `number`-th record.
    pub(crate) fn new(number: u64, label: &HorizonLabel) -> Result<Self, OutcomeError> {
        let (race_at, same_ms, race_censor) = match label.race {
            Race::Upper { at, same_ms } | Race::Lower { at, same_ms } => {
                (Some(at.as_millis()), Some(same_ms), None)
            }
            Race::Censored(censor) => (None, None, Some(censor)),
            Race::Neither | Race::Unavailable => (None, None, None),
        };
        Ok(Self {
            record: number,
            horizon: label.horizon.label(),
            horizon_ms: label.horizon.millis(),
            clear: label.clear,
            lower: opt_units("lower", label.barriers.map(|b| b.lower))?,
            upper: opt_units("upper", label.barriers.map(|b| b.upper))?,
            race: race_name(&label.race),
            race_at,
            same_ms,
            race_censor_at: censor_at(race_censor),
            race_censor_reason: censor_reason(race_censor),
            up: units("up", label.up.size)?,
            up_at: label.up.at.map(|at| at.as_millis()),
            down: units("down", label.down.size)?,
            down_at: label.down.at.map(|at| at.as_millis()),
            last: opt_units("last", label.last)?,
            censor_at: censor_at(label.censored),
            censor_reason: censor_reason(label.censored),
        })
    }
}

/// One row of `passages.parquet`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PassageRow {
    pub(crate) record: u64,
    pub(crate) target: &'static str,
    pub(crate) price: i64,
    pub(crate) passage: &'static str,
    pub(crate) at: Option<i64>,
    pub(crate) censor_at: Option<i64>,
    pub(crate) censor_reason: Option<&'static str>,
}

impl PassageRow {
    /// The rows of the present passages of `label`, the `number`-th
    /// record's: `level` first, then `extreme`.
    pub(crate) fn of(number: u64, label: &ForwardLabel) -> Result<Vec<Self>, OutcomeError> {
        [("level", &label.level), ("extreme", &label.extreme)]
            .into_iter()
            .filter_map(|(target, passage)| passage.as_ref().map(|p| (target, p)))
            .map(|(target, passage)| Self::new(number, target, passage))
            .collect()
    }

    fn new(number: u64, target: &'static str, label: &PassageLabel) -> Result<Self, OutcomeError> {
        let (at, censor) = match label.passage {
            Passage::Touched { at } => (Some(at.as_millis()), None),
            Passage::Censored(censor) => (None, Some(censor)),
            Passage::AtDecision | Passage::NotWithin => (None, None),
        };
        Ok(Self {
            record: number,
            target,
            price: units("passage price", label.price)?,
            passage: passage_name(&label.passage),
            at,
            censor_at: censor_at(censor),
            censor_reason: censor_reason(censor),
        })
    }
}

/// One row of `effort.parquet` (ADR-058 D2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffortRow {
    pub(crate) record: u64,
    pub(crate) window: &'static str,
    pub(crate) state: &'static str,
    pub(crate) aggressor: Option<&'static str>,
    pub(crate) effort: Option<i64>,
    pub(crate) effort_high: Option<i64>,
    pub(crate) progress: Option<i64>,
    pub(crate) move_median: Option<i64>,
    pub(crate) move_high: Option<i64>,
    pub(crate) partial_start: Option<bool>,
    pub(crate) feed_gap: Option<bool>,
    pub(crate) active: Option<bool>,
}

impl EffortRow {
    /// The rows of `record`, the `number`-th record: one per effort window,
    /// in `catalog::CANDIDATE_EFFORT_RESULT` order, the order of
    /// `StateSnapshot::effort`.
    pub(crate) fn of(number: u64, record: &ContextRecord) -> Result<Vec<Self>, OutcomeError> {
        catalog::CANDIDATE_EFFORT_RESULT
            .iter()
            .zip(&record.state.effort)
            .map(|((timeframe, _), value)| Self::new(number, timeframe.label(), value))
            .collect()
    }

    fn new(
        number: u64,
        window: &'static str,
        value: &FeatureValue<EffortResult>,
    ) -> Result<Self, OutcomeError> {
        let ready = value.ready();
        let qty = |column: &str, pick: fn(&EffortResult) -> Qty| {
            ready.map(|r| qty_units(column, pick(r))).transpose()
        };
        let price = |column: &str, pick: fn(&EffortResult) -> Price| {
            ready.map(|r| units(column, pick(r))).transpose()
        };
        Ok(Self {
            record: number,
            window,
            state: value_state_name(value),
            aggressor: ready.map(|r| aggressor_name(r.aggressor)),
            effort: qty("effort", |r| r.effort)?,
            effort_high: qty("effort_high", |r| r.effort_high)?,
            progress: price("progress", |r| r.progress)?,
            move_median: price("move_median", |r| r.move_median)?,
            move_high: price("move_high", |r| r.move_high)?,
            partial_start: ready.map(|r| r.coverage.partial_start),
            feed_gap: ready.map(|r| r.coverage.feed_gap),
            active: ready.map(|r| r.active),
        })
    }
}

// ---------------------------------------------------------------------------
// Batches.

/// The six provenance values of a run, in [`PROVENANCE_COLUMNS`] order.
pub(crate) fn provenance_values(provenance: &LabelProvenance) -> [String; 6] {
    [
        provenance.context.data.to_string(),
        provenance.context.engine.to_string(),
        provenance.context.policy.to_string(),
        provenance.context.policy_fingerprint.to_string(),
        provenance.labeling.to_string(),
        provenance.labeling_fingerprint.to_string(),
    ]
}

/// A table's rows, convertible to a batch of its schema.
pub(crate) trait Row: Sized {
    /// The table's schema.
    fn schema() -> SchemaRef;
    /// The data columns of `rows`, without the provenance columns.
    fn columns(rows: &[Self]) -> Vec<ArrayRef>;
}

fn timestamps(values: impl Iterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(
        values
            .collect::<TimestampMillisecondArray>()
            .with_timezone("UTC"),
    )
}

/// Exact `Decimal128(18, 8)` values in units: prices and quantities alike.
fn prices(values: impl Iterator<Item = Option<i64>>) -> ArrayRef {
    let array = values
        .map(|units| units.map(i128::from))
        .collect::<Decimal128Array>()
        .with_precision_and_scale(PRICE_PRECISION, PRICE_SCALE)
        .expect("18, 8 is a valid decimal precision and scale");
    Arc::new(array)
}

fn texts<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(values.collect::<StringArray>())
}

fn flags(values: impl Iterator<Item = Option<bool>>) -> ArrayRef {
    Arc::new(values.collect::<BooleanArray>())
}

fn records(values: impl Iterator<Item = u64>) -> ArrayRef {
    Arc::new(values.map(Some).collect::<UInt64Array>())
}

impl Row for ContextRow {
    fn schema() -> SchemaRef {
        contexts_schema()
    }

    fn columns(rows: &[Self]) -> Vec<ArrayRef> {
        vec![
            records(rows.iter().map(|r| r.record)),
            timestamps(rows.iter().map(|r| Some(r.decision_time))),
            texts(rows.iter().map(|r| Some(r.event_kind))),
            Arc::new(
                rows.iter()
                    .map(|r| Some(r.event_seq))
                    .collect::<UInt64Array>(),
            ),
            Arc::new(
                rows.iter()
                    .map(|r| Some(r.ordinal))
                    .collect::<UInt32Array>(),
            ),
            texts(rows.iter().map(|r| Some(r.sample))),
            timestamps(rows.iter().map(|r| r.minute_end)),
            texts(rows.iter().map(|r| r.trigger.as_deref())),
            texts(rows.iter().map(|r| r.trigger_family)),
            texts(rows.iter().map(|r| r.episode.as_deref())),
            flags(rows.iter().map(|r| r.opener)),
            flags(rows.iter().map(|r| r.truncated)),
            flags(rows.iter().map(|r| r.bridge)),
            texts(rows.iter().map(|r| r.regime)),
            Arc::new(
                rows.iter()
                    .map(|r| r.regime_percentile)
                    .collect::<Float64Array>(),
            ),
            flags(rows.iter().map(|r| Some(r.levels_ready))),
            flags(rows.iter().map(|r| r.at_value_edge)),
            flags(rows.iter().map(|r| r.at_structural_extreme)),
            texts(rows.iter().map(|r| r.auction_state)),
            prices(rows.iter().map(|r| Some(r.anchor))),
            prices(rows.iter().map(|r| r.reference)),
            prices(rows.iter().map(|r| r.atr_1h)),
            texts(rows.iter().map(|r| r.auction_side)),
            prices(rows.iter().map(|r| r.cvd_utc_day)),
            flags(rows.iter().map(|r| r.cvd_partial_start)),
            flags(rows.iter().map(|r| r.cvd_feed_gap)),
        ]
    }
}

impl Row for OutcomeRow {
    fn schema() -> SchemaRef {
        outcomes_schema()
    }

    fn columns(rows: &[Self]) -> Vec<ArrayRef> {
        vec![
            records(rows.iter().map(|r| r.record)),
            texts(rows.iter().map(|r| Some(r.horizon))),
            Arc::new(
                rows.iter()
                    .map(|r| Some(r.horizon_ms))
                    .collect::<Int64Array>(),
            ),
            flags(rows.iter().map(|r| Some(r.clear))),
            prices(rows.iter().map(|r| r.lower)),
            prices(rows.iter().map(|r| r.upper)),
            texts(rows.iter().map(|r| Some(r.race))),
            timestamps(rows.iter().map(|r| r.race_at)),
            flags(rows.iter().map(|r| r.same_ms)),
            timestamps(rows.iter().map(|r| r.race_censor_at)),
            texts(rows.iter().map(|r| r.race_censor_reason)),
            prices(rows.iter().map(|r| Some(r.up))),
            timestamps(rows.iter().map(|r| r.up_at)),
            prices(rows.iter().map(|r| Some(r.down))),
            timestamps(rows.iter().map(|r| r.down_at)),
            prices(rows.iter().map(|r| r.last)),
            timestamps(rows.iter().map(|r| r.censor_at)),
            texts(rows.iter().map(|r| r.censor_reason)),
        ]
    }
}

impl Row for PassageRow {
    fn schema() -> SchemaRef {
        passages_schema()
    }

    fn columns(rows: &[Self]) -> Vec<ArrayRef> {
        vec![
            records(rows.iter().map(|r| r.record)),
            texts(rows.iter().map(|r| Some(r.target))),
            prices(rows.iter().map(|r| Some(r.price))),
            texts(rows.iter().map(|r| Some(r.passage))),
            timestamps(rows.iter().map(|r| r.at)),
            timestamps(rows.iter().map(|r| r.censor_at)),
            texts(rows.iter().map(|r| r.censor_reason)),
        ]
    }
}

impl Row for EffortRow {
    fn schema() -> SchemaRef {
        effort_schema()
    }

    fn columns(rows: &[Self]) -> Vec<ArrayRef> {
        vec![
            records(rows.iter().map(|r| r.record)),
            texts(rows.iter().map(|r| Some(r.window))),
            texts(rows.iter().map(|r| Some(r.state))),
            texts(rows.iter().map(|r| r.aggressor)),
            prices(rows.iter().map(|r| r.effort)),
            prices(rows.iter().map(|r| r.effort_high)),
            prices(rows.iter().map(|r| r.progress)),
            prices(rows.iter().map(|r| r.move_median)),
            prices(rows.iter().map(|r| r.move_high)),
            flags(rows.iter().map(|r| r.partial_start)),
            flags(rows.iter().map(|r| r.feed_gap)),
            flags(rows.iter().map(|r| r.active)),
        ]
    }
}

/// `rows` as one batch of their table, the provenance columns set to
/// `provenance` on every row.
pub(crate) fn to_batch<R: Row>(rows: &[R], provenance: &[String; 6]) -> RecordBatch {
    let mut columns = R::columns(rows);
    for value in provenance {
        columns.push(Arc::new(StringArray::from(vec![
            value.as_str();
            rows.len()
        ])));
    }
    RecordBatch::try_new(R::schema(), columns)
        .expect("columns are built from the table's schema with equal lengths")
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::conditional::label::CensorReason;

    #[test]
    fn enum_names_are_pinned() {
        // Stored names: a change here changes every dataset (ADR-057 D2).
        let kinds = [
            (EventKind::FeedGap, "feed_gap"),
            (EventKind::BookSnapshot, "book_snapshot"),
            (EventKind::Trade, "trade"),
            (EventKind::Liquidation, "liquidation"),
            (EventKind::BookUpdate, "book_update"),
            (EventKind::MarkPrice, "mark_price"),
            (EventKind::FundingSettlement, "funding_settlement"),
            (EventKind::OpenInterest, "open_interest"),
            (EventKind::Kline, "kline"),
        ];
        for (kind, name) in kinds {
            assert_eq!(event_kind_name(kind), name);
        }
        let regimes = [
            (RegimeLabel::Low, "low"),
            (RegimeLabel::Medium, "medium"),
            (RegimeLabel::High, "high"),
            (RegimeLabel::Extreme, "extreme"),
        ];
        for (label, name) in regimes {
            assert_eq!(regime_name(label), name);
        }
        let states: Vec<&str> = AuctionState::ALL
            .into_iter()
            .map(auction_state_name)
            .collect();
        assert_eq!(
            states,
            [
                "inside_value",
                "at_value_edge",
                "outside_value",
                "breakout",
                "failed_breakout",
                "failed_reclaim",
                "acceptance"
            ]
        );
        let reasons: Vec<&str> = [
            CensorReason::Gap,
            CensorReason::StaleReference,
            CensorReason::Rejected,
            CensorReason::EndOfData,
        ]
        .into_iter()
        .map(CensorReason::as_str)
        .collect();
        assert_eq!(
            reasons,
            ["gap", "stale_reference", "rejected", "end_of_data"]
        );
        let censor = Censor {
            at: mie_domain::time::EventTime::from_millis(1),
            reason: CensorReason::Gap,
        };
        let at = mie_domain::time::EventTime::from_millis(1);
        let races: Vec<&str> = [
            Race::Upper { at, same_ms: false },
            Race::Lower { at, same_ms: true },
            Race::Neither,
            Race::Censored(censor),
            Race::Unavailable,
        ]
        .iter()
        .map(race_name)
        .collect();
        assert_eq!(
            races,
            ["upper", "lower", "neither", "censored", "unavailable"]
        );
        let passages: Vec<&str> = [
            Passage::AtDecision,
            Passage::Touched { at },
            Passage::NotWithin,
            Passage::Censored(censor),
        ]
        .iter()
        .map(passage_name)
        .collect();
        assert_eq!(
            passages,
            ["at_decision", "touched", "not_within", "censored"]
        );
    }

    #[test]
    fn format_2_names_are_pinned() {
        // Stored names of ADR-058 D2: a change here changes every dataset.
        let regions: Vec<&str> = [Region::Below, Region::In, Region::Above]
            .into_iter()
            .map(region_name)
            .collect();
        assert_eq!(regions, ["below", "in", "above"]);
        assert_eq!(aggressor_name(Aggressor::Buy), "buy");
        assert_eq!(aggressor_name(Aggressor::Sell), "sell");
        let warming: FeatureValue<()> = FeatureValue::WarmingUp {
            observed: 0,
            required: 1,
        };
        let unavailable: FeatureValue<()> = FeatureValue::Unavailable {
            reason: mie_domain::feature::Unavailability::InputInvalid,
        };
        assert_eq!(value_state_name(&warming), "warming");
        assert_eq!(value_state_name(&FeatureValue::Ready(())), "ready");
        assert_eq!(value_state_name(&unavailable), "unavailable");
        // The effort windows, in the order of `StateSnapshot::effort`.
        let windows: Vec<&str> = catalog::CANDIDATE_EFFORT_RESULT
            .iter()
            .map(|(timeframe, _)| timeframe.label())
            .collect();
        assert_eq!(windows, ["5m", "15m", "1h"]);
    }

    #[test]
    fn prices_beyond_decimal_18_8_are_refused_never_rounded() {
        let max = Price::from_units(DECIMAL_MAX_UNITS);
        assert_eq!(units("x", max), Ok(DECIMAL_MAX_UNITS));
        assert_eq!(
            units("x", Price::from_units(-DECIMAL_MAX_UNITS)),
            Ok(-DECIMAL_MAX_UNITS)
        );
        for over in [
            DECIMAL_MAX_UNITS + 1,
            -DECIMAL_MAX_UNITS - 1,
            i64::MIN,
            i64::MAX,
        ] {
            assert!(matches!(
                units("x", Price::from_units(over)),
                Err(OutcomeError::Range(_))
            ));
        }
    }

    #[test]
    fn quantities_beyond_decimal_18_8_are_refused_never_rounded() {
        assert_eq!(
            qty_units("x", Qty::from_units(DECIMAL_MAX_UNITS)),
            Ok(DECIMAL_MAX_UNITS)
        );
        assert_eq!(
            qty_units("x", Qty::from_units(-DECIMAL_MAX_UNITS)),
            Ok(-DECIMAL_MAX_UNITS)
        );
        for over in [DECIMAL_MAX_UNITS + 1, -DECIMAL_MAX_UNITS - 1, i64::MIN] {
            let error = qty_units("cvd_utc_day", Qty::from_units(over)).unwrap_err();
            assert!(
                matches!(&error, OutcomeError::Range(detail) if detail.starts_with("cvd_utc_day ")),
                "{error}"
            );
        }
    }

    #[test]
    fn writer_properties_pin_adr_057_row_groups() {
        let properties = writer_properties();
        assert_eq!(properties.max_row_group_row_count(), Some(122_880));
        assert_eq!(
            properties.compression(&parquet::schema::types::ColumnPath::from("record")),
            Compression::LZ4_RAW
        );
    }
}
