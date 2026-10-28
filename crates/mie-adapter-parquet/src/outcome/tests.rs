//! Round trip, determinism, integrity and contract tests of the outcome
//! dataset (ADR-057 D1–D5).

use super::schema::{self, ContextRow, event_kind_name, to_batch};
use super::*;
use crate::testutil::TempDir;
use arrow_array::{
    Array, BooleanArray, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use bytes::Bytes;
use mie_domain::bars::{Coverage, Timeframe};
use mie_domain::candidate::{ContextLevel, ContextLevels, LocationContext};
use mie_domain::conditional::label::{
    Barriers, Censor, CensorReason, Excursion, ForwardLabel, HORIZONS, HorizonLabel,
    LabelProvenance, Labeling, Passage, PassageLabel, Race,
};
use mie_domain::conditional::{
    ContextProvenance, ContextRecord, Episode, EpisodeId, Sample, StateSnapshot,
};
use mie_domain::feature::{FeatureValue, catalog};
use mie_domain::location::LevelKind;
use mie_domain::num::Price;
use mie_domain::order::{CanonicalKey, EventKind};
use mie_domain::regime::{AtrPercentile, Regime};
use mie_domain::research::DataVersion;
use mie_domain::state::MarketStateEngine;
use mie_domain::structure::{Level, Side, Sweep, SweepOutcome};
use mie_domain::time::EventTime;
use mie_domain::trigger::{Bias, TriggerEvent, TriggerEvidence, TriggerFamily};
use mie_ports::outbound::ReplayWindow;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::Path;

const T0: i64 = 1_791_244_800_000;

fn t(millis: i64) -> EventTime {
    EventTime::from_millis(millis)
}

fn p(units: i64) -> Price {
    Price::from_units(units)
}

fn warming<T>() -> FeatureValue<T> {
    FeatureValue::WarmingUp {
        observed: 0,
        required: 1,
    }
}

fn ready_or_warming<T>(value: Option<T>) -> FeatureValue<T> {
    value.map_or_else(warming, FeatureValue::Ready)
}

fn snapshot(last: Option<i64>, atr: Option<i64>) -> StateSnapshot {
    StateSnapshot {
        last_price: ready_or_warming(last.map(p)),
        atr: ready_or_warming(atr.map(p)),
        aggression: [warming(), warming(), warming()],
        cvd_utc_day: warming(),
        effort: [warming(), warming(), warming()],
        liquidity: [warming(), warming(), warming()],
        oi_5m: warming(),
    }
}

fn level(kind: LevelKind, price: i64) -> ContextLevel {
    ContextLevel {
        kind,
        source: catalog::LOCATION_LEVELS_V2.key,
        side: None,
        anchor: None,
        price: p(price),
        low: p(price - 10),
        high: p(price + 10),
    }
}

fn context(anchor: i64, levels: Option<Vec<ContextLevel>>) -> LocationContext {
    LocationContext {
        anchor: p(anchor),
        levels: ready_or_warming(levels.map(|levels| ContextLevels {
            feature: catalog::LOCATION_LEVELS_V2.key,
            levels,
        })),
        auction: warming(),
    }
}

fn bias(regime: Option<f64>) -> Bias {
    let mut bias = Bias::read(MarketStateEngine::new().state());
    bias.regime = ready_or_warming(regime.map(|percentile| {
        Regime::new(
            AtrPercentile::new(percentile).unwrap(),
            catalog::VOLATILITY_REGIME_1H_V1.key,
        )
    }));
    bias
}

fn key(time: i64, seq: u64) -> CanonicalKey {
    CanonicalKey {
        time: t(time),
        kind: EventKind::Trade,
        seq,
    }
}

fn baseline(event: CanonicalKey, context: LocationContext, last: Option<i64>) -> ContextRecord {
    ContextRecord {
        event,
        sample: Sample::Baseline {
            minute_end: t(event.time.as_millis() - 7),
        },
        context,
        bias: bias(Some(57.5)),
        state: snapshot(last, last.map(|_| 4_000_000_000)),
    }
}

fn trigger(event: CanonicalKey, context: LocationContext, opener: bool) -> ContextRecord {
    let coverage = Coverage {
        partial_start: false,
        feed_gap: false,
    };
    let sweep = Sweep {
        feature: catalog::STRUCTURE_LEVELS_15M_V1.key,
        timeframe: Timeframe::M15,
        level: Level {
            side: Side::High,
            price: p(8_500_000_000_000),
            swing_time: t(T0 - 3_600_000),
            confirmed_bar_end: t(T0 - 2_700_000),
            known_at: t(T0 - 2_700_000),
            touches: 1,
            coverage,
        },
        time: event.time,
        price: p(8_501_000_000_000),
        extreme: p(8_502_000_000_000),
        window_bars: 1,
        outcome: SweepOutcome::Sfp,
        resolved_bar_end: Some(event.time),
        known_at: event.time,
        coverage,
    };
    let trigger = TriggerEvent {
        feature: catalog::TRIGGER_SFP_15M_V1.key,
        family: TriggerFamily::Sfp,
        known_at: event.time,
        inputs: catalog::TRIGGER_SFP_15M_V1.inputs,
        evidence: TriggerEvidence::Sfp(sweep),
        context: context.clone(),
        bias: bias(None),
    };
    ContextRecord {
        event,
        sample: Sample::Trigger {
            trigger: Box::new(trigger),
            episode: Episode {
                id: EpisodeId {
                    trigger: catalog::TRIGGER_SFP_15M_V1.key,
                    opened_at: event,
                    ordinal: 0,
                },
                opener,
                truncated: !opener,
                bridge: opener,
            },
        },
        context,
        bias: bias(None),
        state: snapshot(Some(8_500_500_000_000), None),
    }
}

/// Four records: three of one event (a baseline whose levels hold both a
/// value edge and a structural extreme, then two triggers), then a
/// baseline before the first trade.
fn records() -> Vec<ContextRecord> {
    let first = key(T0 + 1_000, 5);
    vec![
        baseline(
            first,
            context(
                8_500_000_000_000,
                Some(vec![
                    level(LevelKind::Vah, 8_500_000_000_000),
                    level(LevelKind::Poc, 8_499_000_000_000),
                    level(LevelKind::StructuralHigh, 8_500_000_000_000),
                ]),
            ),
            Some(8_500_500_000_000),
        ),
        trigger(first, context(8_500_000_000_000, None), true),
        trigger(first, context(8_500_000_000_000, Some(Vec::new())), false),
        baseline(
            key(T0 + 61_000, 9),
            context(
                8_400_000_000_000,
                Some(vec![level(LevelKind::Val, 8_400_000_000_000)]),
            ),
            None,
        ),
    ]
}

fn censor(at: i64, reason: CensorReason) -> Censor {
    Censor { at: t(at), reason }
}

fn horizon(position: usize, race: Race, censored: Option<Censor>) -> HorizonLabel {
    let barriers = (!matches!(race, Race::Unavailable)).then(|| Barriers {
        lower: p(8_400_000_000_000 - position as i64),
        upper: p(8_600_000_000_000 + position as i64),
    });
    HorizonLabel {
        horizon: HORIZONS[position],
        clear: position.is_multiple_of(2),
        barriers,
        race,
        up: if position == 1 {
            Excursion::NONE
        } else {
            Excursion {
                size: p(1_000 + position as i64),
                at: Some(t(T0 + 5_000 + position as i64)),
            }
        },
        down: Excursion {
            size: p(2_000),
            at: Some(t(T0 + 6_000)),
        },
        last: (position != 3).then(|| p(8_500_000_000_001)),
        censored,
    }
}

/// One label per record, together covering every race, passage and
/// censor reason, and both `None` and `Some` of every optional field.
fn labels(records: &[ContextRecord]) -> Vec<ForwardLabel> {
    let horizons = [
        [
            horizon(
                0,
                Race::Upper {
                    at: t(T0 + 2_000),
                    same_ms: true,
                },
                None,
            ),
            horizon(
                1,
                Race::Lower {
                    at: t(T0 + 3_000),
                    same_ms: false,
                },
                None,
            ),
            horizon(2, Race::Neither, None),
            horizon(
                3,
                Race::Censored(censor(T0 + 9_000, CensorReason::Gap)),
                Some(censor(T0 + 9_000, CensorReason::Gap)),
            ),
            horizon(4, Race::Unavailable, None),
        ],
        [
            horizon(0, Race::Neither, None),
            horizon(
                1,
                Race::Censored(censor(T0 + 9_001, CensorReason::StaleReference)),
                Some(censor(T0 + 9_001, CensorReason::StaleReference)),
            ),
            horizon(
                2,
                Race::Upper {
                    at: t(T0 + 4_000),
                    same_ms: false,
                },
                Some(censor(T0 + 9_002, CensorReason::Rejected)),
            ),
            horizon(3, Race::Unavailable, None),
            horizon(
                4,
                Race::Censored(censor(T0 + 9_003, CensorReason::EndOfData)),
                Some(censor(T0 + 9_003, CensorReason::EndOfData)),
            ),
        ],
    ];
    let passages = [
        (None, None),
        (
            Some(PassageLabel {
                price: p(8_500_000_000_000),
                passage: Passage::Touched { at: t(T0 + 7_000) },
            }),
            Some(PassageLabel {
                price: p(8_502_000_000_000),
                passage: Passage::Censored(censor(T0 + 9_004, CensorReason::Gap)),
            }),
        ),
        (
            Some(PassageLabel {
                price: p(8_500_500_000_000),
                passage: Passage::AtDecision,
            }),
            Some(PassageLabel {
                price: p(8_502_000_000_000),
                passage: Passage::NotWithin,
            }),
        ),
        (None, None),
    ];
    let mut ordinal = 0;
    records
        .iter()
        .enumerate()
        .map(|(i, record)| {
            if i > 0 && records[i - 1].event == record.event {
                ordinal += 1;
            } else {
                ordinal = 0;
            }
            ForwardLabel {
                event: record.event,
                ordinal,
                reference: record.state.last_price.ready().copied(),
                atr: record.state.atr.ready().copied(),
                horizons: horizons[i % 2],
                level: passages[i].0,
                extreme: passages[i].1,
            }
        })
        .collect()
}

fn provenance() -> LabelProvenance {
    LabelProvenance::new(
        ContextProvenance::new(
            DataVersion::from_hex(&"5d".repeat(32)).unwrap(),
            &MarketStateEngine::new(),
        ),
        Labeling::CURRENT,
    )
}

fn header(code: &str) -> OutcomeHeader {
    OutcomeHeader {
        source: "archive binance-archive/BTCUSDT streams aggTrades correction fold".to_owned(),
        replay: ReplayWindow {
            start: t(T0 - 86_400_000),
            end: t(T0 + 86_400_000),
        },
        records: ReplayWindow {
            start: t(T0),
            end: t(T0 + 3_600_000),
        },
        provenance: provenance(),
        code: code.to_owned(),
    }
}

/// Writes `records` and `labels` as the labeling run emits them: each
/// label after its record, the last two labels late.
fn build(
    dir: &Path,
    header: OutcomeHeader,
    records: &[ContextRecord],
    labels: &[ForwardLabel],
) -> OutcomeManifest {
    let mut writer = OutcomeWriter::create(dir, header).unwrap();
    for (i, record) in records.iter().enumerate() {
        writer.record(record).unwrap();
        if i < 2 {
            writer.label(&labels[i]).unwrap();
        }
    }
    for label in &labels[2..] {
        writer.label(label).unwrap();
    }
    assert_eq!((writer.records(), writer.labels()), (4, 4));
    writer.finish(100, 3).unwrap()
}

fn read(path: &Path) -> RecordBatch {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(fs::read(path).unwrap())).unwrap();
    let schema = builder.schema().clone();
    let batches: Vec<RecordBatch> = builder.build().unwrap().map(Result::unwrap).collect();
    arrow_select_concat(&schema, &batches)
}

/// The batches as one (the tables here hold a few rows: one batch).
fn arrow_select_concat(schema: &arrow_schema::SchemaRef, batches: &[RecordBatch]) -> RecordBatch {
    match batches {
        [] => RecordBatch::new_empty(schema.clone()),
        [one] => one.clone(),
        _ => panic!("expected one batch, got {}", batches.len()),
    }
}

fn col<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column {name} has another type"))
}

fn opt_time(array: &TimestampMillisecondArray, row: usize) -> Option<EventTime> {
    array.is_valid(row).then(|| t(array.value(row)))
}

fn opt_price(array: &Decimal128Array, row: usize) -> Option<Price> {
    array
        .is_valid(row)
        .then(|| p(i64::try_from(array.value(row)).unwrap()))
}

fn opt_reason(array: &StringArray, row: usize) -> Option<CensorReason> {
    array.is_valid(row).then(|| {
        [
            CensorReason::Gap,
            CensorReason::StaleReference,
            CensorReason::Rejected,
            CensorReason::EndOfData,
        ]
        .into_iter()
        .find(|reason| reason.as_str() == array.value(row))
        .unwrap()
    })
}

fn opt_censor(at: &TimestampMillisecondArray, reason: &StringArray, row: usize) -> Option<Censor> {
    match (opt_time(at, row), opt_reason(reason, row)) {
        (Some(at), Some(reason)) => Some(Censor { at, reason }),
        (None, None) => None,
        _ => panic!("censor time and reason are only partly null at row {row}"),
    }
}

/// The labels rebuilt from the three tables alone.
fn rebuild(dir: &Path) -> Vec<ForwardLabel> {
    let contexts = read(&dir.join("contexts.parquet"));
    let outcomes = read(&dir.join("outcomes.parquet"));
    let passages = read(&dir.join("passages.parquet"));
    let kinds = [
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
    let time = col::<TimestampMillisecondArray>(&contexts, "decision_time");
    let kind = col::<StringArray>(&contexts, "event_kind");
    let seq = col::<UInt64Array>(&contexts, "event_seq");
    let ordinal = col::<UInt32Array>(&contexts, "ordinal");
    let reference = col::<Decimal128Array>(&contexts, "reference");
    let atr = col::<Decimal128Array>(&contexts, "atr_1h");
    let record = col::<UInt64Array>(&contexts, "record");

    let o_record = col::<UInt64Array>(&outcomes, "record");
    let o_horizon = col::<StringArray>(&outcomes, "horizon");
    let o_ms = col::<Int64Array>(&outcomes, "horizon_ms");
    let o_clear = col::<BooleanArray>(&outcomes, "clear");
    let o_lower = col::<Decimal128Array>(&outcomes, "lower");
    let o_upper = col::<Decimal128Array>(&outcomes, "upper");
    let o_race = col::<StringArray>(&outcomes, "race");
    let o_race_at = col::<TimestampMillisecondArray>(&outcomes, "race_at");
    let o_same = col::<BooleanArray>(&outcomes, "same_ms");
    let o_rc_at = col::<TimestampMillisecondArray>(&outcomes, "race_censor_at");
    let o_rc_reason = col::<StringArray>(&outcomes, "race_censor_reason");
    let o_up = col::<Decimal128Array>(&outcomes, "up");
    let o_up_at = col::<TimestampMillisecondArray>(&outcomes, "up_at");
    let o_down = col::<Decimal128Array>(&outcomes, "down");
    let o_down_at = col::<TimestampMillisecondArray>(&outcomes, "down_at");
    let o_last = col::<Decimal128Array>(&outcomes, "last");
    let o_c_at = col::<TimestampMillisecondArray>(&outcomes, "censor_at");
    let o_c_reason = col::<StringArray>(&outcomes, "censor_reason");

    let s_record = col::<UInt64Array>(&passages, "record");
    let s_target = col::<StringArray>(&passages, "target");
    let s_price = col::<Decimal128Array>(&passages, "price");
    let s_passage = col::<StringArray>(&passages, "passage");
    let s_at = col::<TimestampMillisecondArray>(&passages, "at");
    let s_c_at = col::<TimestampMillisecondArray>(&passages, "censor_at");
    let s_c_reason = col::<StringArray>(&passages, "censor_reason");

    (0..contexts.num_rows())
        .map(|row| {
            assert_eq!(record.value(row), row as u64);
            let horizons = std::array::from_fn(|position| {
                let o = 5 * row + position;
                assert_eq!(o_record.value(o), row as u64);
                let horizon = HORIZONS[position];
                assert_eq!(o_horizon.value(o), horizon.label());
                assert_eq!(o_ms.value(o), horizon.millis());
                let race_at = opt_time(o_race_at, o);
                let race = match o_race.value(o) {
                    "upper" => Race::Upper {
                        at: race_at.unwrap(),
                        same_ms: o_same.value(o),
                    },
                    "lower" => Race::Lower {
                        at: race_at.unwrap(),
                        same_ms: o_same.value(o),
                    },
                    "neither" => Race::Neither,
                    "censored" => Race::Censored(opt_censor(o_rc_at, o_rc_reason, o).unwrap()),
                    "unavailable" => Race::Unavailable,
                    other => panic!("race {other}"),
                };
                HorizonLabel {
                    horizon,
                    clear: o_clear.value(o),
                    barriers: match (opt_price(o_lower, o), opt_price(o_upper, o)) {
                        (Some(lower), Some(upper)) => Some(Barriers { lower, upper }),
                        (None, None) => None,
                        _ => panic!("half a barrier pair"),
                    },
                    race,
                    up: Excursion {
                        size: opt_price(o_up, o).unwrap(),
                        at: opt_time(o_up_at, o),
                    },
                    down: Excursion {
                        size: opt_price(o_down, o).unwrap(),
                        at: opt_time(o_down_at, o),
                    },
                    last: opt_price(o_last, o),
                    censored: opt_censor(o_c_at, o_c_reason, o),
                }
            });
            let passage = |target: &str| {
                (0..passages.num_rows())
                    .find(|&s| s_record.value(s) == row as u64 && s_target.value(s) == target)
                    .map(|s| PassageLabel {
                        price: opt_price(s_price, s).unwrap(),
                        passage: match s_passage.value(s) {
                            "at_decision" => Passage::AtDecision,
                            "touched" => Passage::Touched {
                                at: opt_time(s_at, s).unwrap(),
                            },
                            "not_within" => Passage::NotWithin,
                            "censored" => {
                                Passage::Censored(opt_censor(s_c_at, s_c_reason, s).unwrap())
                            }
                            other => panic!("passage {other}"),
                        },
                    })
            };
            ForwardLabel {
                event: CanonicalKey {
                    time: t(time.value(row)),
                    kind: kinds
                        .into_iter()
                        .find(|kind_| event_kind_name(*kind_) == kind.value(row))
                        .unwrap(),
                    seq: seq.value(row),
                },
                ordinal: ordinal.value(row),
                reference: opt_price(reference, row),
                atr: opt_price(atr, row),
                horizons,
                level: passage("level"),
                extreme: passage("extreme"),
            }
        })
        .collect()
}

#[test]
fn round_trip_rebuilds_every_label_and_the_record_projection() {
    let dir = TempDir::new("outcome-round-trip");
    let out = dir.path().join("dataset");
    let records = records();
    let labels = labels(&records);
    let manifest = build(&out, header("unknown"), &records, &labels);
    assert_eq!(manifest.rows("contexts.parquet"), Some(4));
    assert_eq!(manifest.rows("outcomes.parquet"), Some(20));
    assert_eq!(manifest.rows("passages.parquet"), Some(4));
    assert_eq!((manifest.events, manifest.rejections), (100, 3));
    assert_eq!(OutcomeDataset::open(&out).unwrap().manifest(), &manifest);

    assert_eq!(rebuild(&out), labels);

    // The record projection: what was written is read back unchanged.
    let contexts = read(&out.join("contexts.parquet"));
    let ordinals = [0, 1, 2, 0];
    let rows: Vec<ContextRow> = records
        .iter()
        .zip(ordinals)
        .enumerate()
        .map(|(i, (record, ordinal))| ContextRow::new(i as u64, ordinal, record).unwrap())
        .collect();
    let expected = to_batch(&rows, &schema::provenance_values(&provenance()));
    assert_eq!(contexts.columns(), expected.columns());

    // And by hand, the fields the projection derives.
    let sample = col::<StringArray>(&contexts, "sample");
    let edge = col::<BooleanArray>(&contexts, "at_value_edge");
    let extreme = col::<BooleanArray>(&contexts, "at_structural_extreme");
    let ready = col::<BooleanArray>(&contexts, "levels_ready");
    let regime = col::<StringArray>(&contexts, "regime");
    let percentile = col::<Float64Array>(&contexts, "regime_percentile");
    let family = col::<StringArray>(&contexts, "trigger_family");
    let episode = col::<StringArray>(&contexts, "episode");
    let opener = col::<BooleanArray>(&contexts, "opener");
    let minute_end = col::<TimestampMillisecondArray>(&contexts, "minute_end");
    assert_eq!(
        (0..4).map(|r| sample.value(r)).collect::<Vec<_>>(),
        ["baseline", "trigger", "trigger", "baseline"]
    );
    // Record 0: both bins (ADR-051 R1); 1: levels not ready; 2: neither;
    // 3: value edge only.
    assert!(ready.value(0) && edge.value(0) && extreme.value(0));
    assert!(!ready.value(1) && edge.is_null(1) && extreme.is_null(1));
    assert!(ready.value(2) && !edge.value(2) && !extreme.value(2));
    assert!(edge.value(3) && !extreme.value(3));
    assert_eq!(regime.value(0), "high");
    assert_eq!(percentile.value(0), 57.5);
    assert!(regime.is_null(1) && percentile.is_null(1));
    assert!(family.is_null(0) && episode.is_null(0) && opener.is_null(0));
    assert_eq!(family.value(1), "sfp");
    assert_eq!(
        episode.value(1),
        format!("trigger.sfp.15m@1/{}ms/5/0", T0 + 1_000)
    );
    assert!(opener.value(1) && !opener.value(2));
    assert_eq!(minute_end.value(0), T0 + 1_000 - 7);
    assert!(minute_end.is_null(1));
    let data = col::<StringArray>(&contexts, "data_version");
    assert_eq!(data.value(3), "5d".repeat(32));
}

#[test]
fn prices_are_stored_as_int64_decimals() {
    let dir = TempDir::new("outcome-physical");
    let out = dir.path().join("dataset");
    let records = records();
    build(&out, header("unknown"), &records, &labels(&records));
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(
        fs::read(out.join("contexts.parquet")).unwrap(),
    ))
    .unwrap();
    let parquet_schema = builder.metadata().file_metadata().schema_descr();
    let anchor = (0..parquet_schema.num_columns())
        .map(|i| parquet_schema.column(i))
        .find(|column| column.name() == "anchor")
        .unwrap();
    assert_eq!(anchor.physical_type(), parquet::basic::Type::INT64);
    let decision = (0..parquet_schema.num_columns())
        .map(|i| parquet_schema.column(i))
        .find(|column| column.name() == "decision_time")
        .unwrap();
    assert_eq!(decision.physical_type(), parquet::basic::Type::INT64);
}

fn snapshot_dir(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
        .into_iter()
        .map(|name| {
            let bytes = fs::read(dir.join(&name)).unwrap();
            (name, bytes)
        })
        .collect()
}

#[test]
fn the_same_inputs_write_the_same_bytes_and_content_hash() {
    let dir = TempDir::new("outcome-determinism");
    let records = records();
    let labels = labels(&records);
    let a = build(&dir.path().join("a"), header("unknown"), &records, &labels);
    let b = build(&dir.path().join("b"), header("unknown"), &records, &labels);
    assert_eq!(a, b);
    assert_eq!(
        snapshot_dir(&dir.path().join("a")),
        snapshot_dir(&dir.path().join("b"))
    );
    let names: Vec<String> = snapshot_dir(&dir.path().join("a"))
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        names,
        [
            "contexts.parquet",
            "dataset.manifest",
            "outcomes.parquet",
            "passages.parquet"
        ]
    );

    // Another code version: the same content hash and the same tables.
    let code = "git:0123456789abcdef0123456789abcdef01234567-dirty";
    let c = build(&dir.path().join("c"), header(code), &records, &labels);
    assert_eq!(c.content, a.content);
    assert_eq!(c.files, a.files);
    assert_eq!(c.code, code);

    // Another records window: another content hash.
    let mut moved = header("unknown");
    moved.records.end = t(T0 + 7_200_000);
    let d = build(&dir.path().join("d"), moved, &records, &labels);
    assert_eq!(d.files, a.files);
    assert_ne!(d.content, a.content);
}

fn make_writable(path: &Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions).unwrap();
}

#[test]
fn tampering_is_an_integrity_error_and_no_manifest_is_incomplete() {
    let dir = TempDir::new("outcome-tamper");
    let records = records();
    let labels = labels(&records);
    for (name, flip) in [
        ("outcomes.parquet", 200),
        ("passages.parquet", 0),
        ("dataset.manifest", 30),
    ] {
        let out = dir.path().join(name);
        build(&out, header("unknown"), &records, &labels);
        let path = out.join(name);
        make_writable(&path);
        let mut bytes = fs::read(&path).unwrap();
        bytes[flip] ^= 0x01;
        fs::write(&path, bytes).unwrap();
        assert!(
            matches!(OutcomeDataset::open(&out), Err(OutcomeError::Integrity(_))),
            "{name}"
        );
    }

    // The code line is outside the hash but still checked by the parser.
    let out = dir.path().join("code");
    build(&out, header("unknown"), &records, &labels);
    let path = out.join(MANIFEST_FILE);
    make_writable(&path);
    let text = fs::read_to_string(&path).unwrap();
    fs::write(&path, text.replace("code unknown", "code other")).unwrap();
    assert!(OutcomeDataset::open(&out).is_ok());

    let out = dir.path().join("incomplete");
    build(&out, header("unknown"), &records, &labels);
    fs::remove_file(out.join(MANIFEST_FILE)).unwrap();
    assert!(matches!(
        OutcomeDataset::open(&out),
        Err(OutcomeError::Incomplete(_))
    ));

    // Every written file is read-only.
    let out = dir.path().join("read-only");
    build(&out, header("unknown"), &records, &labels);
    for (name, _) in snapshot_dir(&out) {
        assert!(
            fs::metadata(out.join(&name))
                .unwrap()
                .permissions()
                .readonly()
        );
    }
}

#[test]
fn an_existing_directory_is_refused() {
    let dir = TempDir::new("outcome-exists");
    let out = dir.path().join("dataset");
    fs::create_dir(&out).unwrap();
    assert!(matches!(
        OutcomeWriter::create(&out, header("unknown")),
        Err(OutcomeError::Exists(_))
    ));
    assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
}

#[test]
fn a_header_that_breaks_the_manifest_is_refused() {
    let dir = TempDir::new("outcome-header");
    let mut bad = header("unknown");
    bad.source.push('\n');
    let mut code = header("git:abc dirty");
    code.source = "live x/y streams journaled correction none".to_owned();
    let mut window = header("unknown");
    window.records.start = t(T0 - 2 * 86_400_000);
    for (i, header) in [bad, code, window].into_iter().enumerate() {
        assert!(matches!(
            OutcomeWriter::create(&dir.path().join(i.to_string()), header),
            Err(OutcomeError::Contract(_))
        ));
    }
}

#[test]
fn misaligned_labels_break_the_contract_and_leave_no_dataset() {
    let dir = TempDir::new("outcome-contract");
    let records = records();
    let labels = labels(&records);

    // A label without a record.
    let mut writer = OutcomeWriter::create(&dir.path().join("a"), header("unknown")).unwrap();
    assert!(matches!(
        writer.label(&labels[0]),
        Err(OutcomeError::Contract(_))
    ));
    // Poisoned from here on.
    assert!(writer.record(&records[0]).is_err());

    // A label out of order.
    let mut writer = OutcomeWriter::create(&dir.path().join("b"), header("unknown")).unwrap();
    writer.record(&records[0]).unwrap();
    writer.record(&records[1]).unwrap();
    assert!(matches!(
        writer.label(&labels[1]),
        Err(OutcomeError::Contract(_))
    ));

    // A label whose reference is not its record's.
    let mut writer = OutcomeWriter::create(&dir.path().join("c"), header("unknown")).unwrap();
    writer.record(&records[0]).unwrap();
    let mut other = labels[0].clone();
    other.reference = None;
    assert!(matches!(
        writer.label(&other),
        Err(OutcomeError::Contract(_))
    ));

    // A record left without a label.
    let out = dir.path().join("d");
    let mut writer = OutcomeWriter::create(&out, header("unknown")).unwrap();
    writer.record(&records[0]).unwrap();
    writer.label(&labels[0]).unwrap();
    writer.record(&records[1]).unwrap();
    assert!(matches!(
        writer.finish(1, 0),
        Err(OutcomeError::Contract(_))
    ));
    assert!(!out.join(MANIFEST_FILE).exists());
    assert!(matches!(
        OutcomeDataset::open(&out),
        Err(OutcomeError::Incomplete(_))
    ));
}

#[test]
fn an_empty_run_writes_empty_tables() {
    let dir = TempDir::new("outcome-empty");
    let out = dir.path().join("dataset");
    let manifest = OutcomeWriter::create(&out, header("unknown"))
        .unwrap()
        .finish(0, 0)
        .unwrap();
    assert!(manifest.files.iter().all(|file| file.rows == 0));
    assert_eq!(OutcomeDataset::open(&out).unwrap().manifest(), &manifest);
    assert_eq!(read(&out.join("outcomes.parquet")).num_rows(), 0);
}
