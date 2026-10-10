//! Envelope schema v1: one exchange-agnostic row per raw message (ADR-030).
//!
//! | Column | Type | Null |
//! |---|---|---|
//! | `event_time` | Timestamp(ms, "UTC") — exchange ordering time | no |
//! | `receive_time` | Timestamp(ns, "UTC") | yes |
//! | `receive_seq` | UInt64 | yes |
//! | `session_id` | Utf8 | yes |
//! | `payload` | Binary | no |
//!
//! The capture columns are all null (archive) or all set (live).

use arrow_array::{
    Array, ArrayRef, BinaryArray, RecordBatch, StringArray, TimestampMillisecondArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use mie_domain::time::EventTime;
use mie_ports::raw::{Capture, RawRecord, RawStoreError, RawStreamKey};
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

/// Key-value metadata key holding the envelope format version.
pub(crate) const KEY_FORMAT: &str = "mie.raw.format";
/// Envelope format version written by this crate.
pub(crate) const FORMAT_VERSION: &str = "1";
/// Key-value metadata key holding the source segment.
pub(crate) const KEY_SOURCE: &str = "mie.raw.source";
/// Key-value metadata key holding the instrument segment.
pub(crate) const KEY_INSTRUMENT: &str = "mie.raw.instrument";
/// Key-value metadata key holding the stream segment.
pub(crate) const KEY_STREAM: &str = "mie.raw.stream";

/// Most rows per row group.
const ROW_GROUP_ROWS: usize = 65_536;
/// Most estimated encoded bytes per row group.
const ROW_GROUP_BYTES: usize = 32 * 1024 * 1024;

fn utc(unit: TimeUnit) -> DataType {
    DataType::Timestamp(unit, Some("UTC".into()))
}

/// The envelope schema v1.
pub(crate) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_time", utc(TimeUnit::Millisecond), false),
        Field::new("receive_time", utc(TimeUnit::Nanosecond), true),
        Field::new("receive_seq", DataType::UInt64, true),
        Field::new("session_id", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, false),
    ]))
}

/// The key-value metadata every file of `stream` carries.
pub(crate) fn key_value_metadata(stream: &RawStreamKey) -> [(&'static str, &str); 4] {
    [
        (KEY_FORMAT, FORMAT_VERSION),
        (KEY_SOURCE, stream.source()),
        (KEY_INSTRUMENT, stream.instrument()),
        (KEY_STREAM, stream.stream()),
    ]
}

/// The pinned writer properties of a file of `stream`.
pub(crate) fn writer_properties(stream: &RawStreamKey) -> WriterProperties {
    let metadata = key_value_metadata(stream)
        .into_iter()
        .map(|(key, value)| KeyValue::new(key.to_owned(), value.to_owned()))
        .collect();
    WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_max_row_group_bytes(Some(ROW_GROUP_BYTES))
        .set_key_value_metadata(Some(metadata))
        .build()
}

/// Converts records to one batch of the envelope schema.
pub(crate) fn to_batch(records: &[RawRecord]) -> RecordBatch {
    let event_time: TimestampMillisecondArray = records
        .iter()
        .map(|r| Some(r.event_time.as_millis()))
        .collect();
    let receive_time: TimestampNanosecondArray = records
        .iter()
        .map(|r| r.capture.as_ref().map(|c| c.receive_time_ns))
        .collect();
    let receive_seq: UInt64Array = records
        .iter()
        .map(|r| r.capture.as_ref().map(|c| c.receive_seq))
        .collect();
    let session_id: StringArray = records
        .iter()
        .map(|r| r.capture.as_ref().map(|c| c.session_id.as_str()))
        .collect();
    let payload: BinaryArray = records.iter().map(|r| Some(r.payload.as_slice())).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(event_time.with_timezone("UTC")),
        Arc::new(receive_time.with_timezone("UTC")),
        Arc::new(receive_seq),
        Arc::new(session_id),
        Arc::new(payload),
    ];
    RecordBatch::try_new(schema(), columns)
        .expect("columns are built from the envelope schema with equal lengths")
}

/// Converts a batch of the envelope schema back to records.
///
/// # Errors
///
/// [`RawStoreError::Corrupt`] when the batch's fields differ from schema v1,
/// a required value is null, or a row's capture columns are only partly
/// null.
pub(crate) fn from_batch(batch: &RecordBatch) -> Result<Vec<RawRecord>, RawStoreError> {
    let expected = schema();
    if batch.schema().fields() != expected.fields() {
        return Err(RawStoreError::Corrupt(format!(
            "schema is not the raw envelope v1: {:?}",
            batch.schema().fields()
        )));
    }
    fn column<T: 'static>(batch: &RecordBatch, index: usize) -> &T {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<T>()
            .expect("column type is fixed by the checked schema")
    }
    let event_time = column::<TimestampMillisecondArray>(batch, 0);
    let receive_time = column::<TimestampNanosecondArray>(batch, 1);
    let receive_seq = column::<UInt64Array>(batch, 2);
    let session_id = column::<StringArray>(batch, 3);
    let payload = column::<BinaryArray>(batch, 4);

    (0..batch.num_rows())
        .map(|row| {
            if event_time.is_null(row) || payload.is_null(row) {
                return Err(RawStoreError::Corrupt(format!(
                    "row {row}: null event_time or payload"
                )));
            }
            let set = [
                receive_time.is_valid(row),
                receive_seq.is_valid(row),
                session_id.is_valid(row),
            ];
            let capture = match set {
                [true, true, true] => Some(Capture {
                    receive_time_ns: receive_time.value(row),
                    receive_seq: receive_seq.value(row),
                    session_id: session_id.value(row).to_owned(),
                }),
                [false, false, false] => None,
                _ => {
                    return Err(RawStoreError::Corrupt(format!(
                        "row {row}: capture columns are only partly null"
                    )));
                }
            };
            Ok(RawRecord {
                event_time: EventTime::from_millis(event_time.value(row)),
                capture,
                payload: payload.value(row).to_vec(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(time: i64, capture: Option<Capture>, payload: &[u8]) -> RawRecord {
        RawRecord {
            event_time: EventTime::from_millis(time),
            capture,
            payload: payload.to_vec(),
        }
    }

    fn capture(seq: u64) -> Capture {
        Capture {
            receive_time_ns: 1_791_244_800_123_456_789,
            receive_seq: seq,
            session_id: format!("session-{seq}"),
        }
    }

    #[test]
    fn writer_properties_pin_adr_030_row_groups() {
        // ADR-030: row groups of at most 65 536 rows or 32 MiB. They are part
        // of the bytes a re-import writes, so of every dataset version.
        let stream = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
        let properties = writer_properties(&stream);
        assert_eq!(properties.max_row_group_row_count(), Some(65_536));
        assert_eq!(properties.max_row_group_bytes(), Some(33_554_432));
    }

    #[test]
    fn batches_round_trip_every_shape() {
        let records = vec![
            record(1, Some(capture(0)), br#"{"e":"aggTrade","p":"60000.10"}"#),
            record(2, None, b"1,60000.10,0.001,1,1,1791244800000,true"),
            record(3, Some(capture(u64::MAX)), b""),
            record(4, None, &[0xff, 0x00, 0xfe, 0xc3]),
            record(5, Some(capture(7)), &vec![b'x'; 1 << 20]),
        ];
        let batch = to_batch(&records);
        assert_eq!(batch.schema(), schema());
        assert_eq!(from_batch(&batch).unwrap(), records);
        assert_eq!(from_batch(&to_batch(&[])).unwrap(), vec![]);
    }

    #[test]
    fn partly_null_capture_is_corrupt() {
        let batch = to_batch(&[record(1, Some(capture(1)), b"x")]);
        let mut columns = batch.columns().to_vec();
        columns[2] = Arc::new(UInt64Array::from(vec![None::<u64>]));
        let broken = RecordBatch::try_new(schema(), columns).unwrap();
        assert!(matches!(
            from_batch(&broken),
            Err(RawStoreError::Corrupt(_))
        ));
    }

    #[test]
    fn a_foreign_schema_is_corrupt() {
        let foreign = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from(vec![b"x".as_slice()]))],
        )
        .unwrap();
        assert!(matches!(
            from_batch(&foreign),
            Err(RawStoreError::Corrupt(_))
        ));
    }
}
