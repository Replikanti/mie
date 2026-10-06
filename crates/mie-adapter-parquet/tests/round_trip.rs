//! Acceptance (#8): records written to the raw store read back identical.

mod common;

use common::{D0, DAY, TempDir, record, selection, stream};
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_ports::raw::{RawRecord, RawRecordSink, RawRecordSource, RawStreamKey};

#[test]
fn records_round_trip_across_streams_dates_and_parts() {
    let dir = TempDir::new("round-trip");
    let store = ParquetRawStore::new(dir.path());
    let streams = [stream("aggTrade"), stream("depth"), stream("markPrice")];
    // Interleaved streams, crossing midnight UTC, mixed capture metadata.
    let input: Vec<(RawStreamKey, RawRecord)> = (0..3_000u64)
        .map(|i| {
            let key = streams[(i % 3) as usize].clone();
            let time = D0 + DAY - 1_500 + i as i64;
            let mut r = record(time, i);
            if key.stream() == "depth" {
                r.payload = format!(r#"{{"e":"depthUpdate","u":{i},"b":[["60000.10","0.5"]]}}"#)
                    .into_bytes();
            }
            (key, r)
        })
        .collect();

    let policy = RotationPolicy {
        max_rows: 400,
        ..RotationPolicy::default()
    };
    let mut writer = store.writer("binance-um", policy).unwrap();
    for (key, r) in &input {
        writer.append(key, r.clone()).unwrap();
    }
    let sealed = writer.close().unwrap();

    let all = [&streams[0], &streams[1], &streams[2]];
    let dataset = store.select(&selection(&all, D0, D0 + 2 * DAY)).unwrap();
    assert_eq!(dataset.files.len(), sealed.len());
    let dates: std::collections::BTreeSet<_> =
        dataset.files.iter().map(|f| f.date.as_str()).collect();
    assert_eq!(
        dates.into_iter().collect::<Vec<_>>(),
        ["2026-10-06", "2026-10-07"]
    );

    // Files in path order are grouped by stream, then date, then part; each
    // group holds its records in append order.
    let mut expected = input.clone();
    expected.sort_by_key(|(key, r)| (key.clone(), r.event_time.as_millis() >= D0 + DAY));
    let mut read = Vec::new();
    for file in &dataset.files {
        let records = store.read(file).unwrap();
        assert_eq!(records.len() as u64, file.rows);
        read.extend(records.into_iter().map(|r| (file.stream.clone(), r)));
    }
    assert_eq!(read, expected);
}

#[test]
fn a_window_selects_only_overlapping_files() {
    let dir = TempDir::new("round-trip-window");
    let store = ParquetRawStore::new(dir.path());
    let key = stream("aggTrade");
    let policy = RotationPolicy {
        max_rows: 10,
        ..RotationPolicy::default()
    };
    let mut writer = store.writer("binance-um", policy).unwrap();
    for i in 0..30u64 {
        writer.append(&key, record(D0 + i as i64, i)).unwrap();
    }
    writer.close().unwrap();
    // Parts hold [0, 9], [10, 19] and [20, 29].
    let parts = |start, end| -> Vec<u32> {
        store
            .select(&selection(&[&key], D0 + start, D0 + end))
            .unwrap()
            .files
            .iter()
            .map(|f| f.part)
            .collect()
    };
    assert_eq!(parts(0, 30), vec![0, 1, 2]);
    assert_eq!(parts(9, 10), vec![0]);
    assert_eq!(parts(10, 11), vec![1]);
    assert_eq!(parts(9, 11), vec![0, 1]);
    assert_eq!(parts(30, 40), Vec::<u32>::new());
    assert_eq!(parts(-DAY, 0), Vec::<u32>::new());
}
