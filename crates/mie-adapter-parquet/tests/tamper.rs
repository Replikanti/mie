//! Acceptance (#8): manifests are verified on read, a tampered file is
//! detected, and the dataset version pins exactly the covered files.

mod common;

use common::{D0, DAY, TempDir, record, selection, stream};
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_ports::raw::{RawRecordSink, RawRecordSource, RawStoreError, RawStreamKey, SealedFile};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

const PARTITION: &str = "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06";

/// Two dates of one stream, three parts of ten records on the first date.
fn sample(tag: &str) -> (TempDir, ParquetRawStore, RawStreamKey) {
    let dir = TempDir::new(tag);
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
    writer.append(&key, record(D0 + DAY + 5, 30)).unwrap();
    writer.close().unwrap();
    (dir, store, key)
}

fn files(store: &ParquetRawStore, key: &RawStreamKey) -> Vec<SealedFile> {
    store
        .select(&selection(&[key], D0, D0 + 2 * DAY))
        .unwrap()
        .files
}

fn part_path(root: &Path, kind: &str, part: u32) -> PathBuf {
    root.join(PARTITION).join(format!("part-{part:05}.{kind}"))
}

/// Replaces a (read-only) sealed file with `edit` applied to its bytes.
fn rewrite(path: &Path, edit: impl FnOnce(&mut Vec<u8>)) {
    let mut data = fs::read(path).unwrap();
    edit(&mut data);
    fs::remove_file(path).unwrap();
    fs::write(path, data).unwrap();
}

fn assert_integrity(result: Result<impl std::fmt::Debug, RawStoreError>, case: &str) {
    assert!(
        matches!(result, Err(RawStoreError::Integrity(_))),
        "{case}: {result:?}"
    );
}

#[test]
fn every_untampered_file_reads() {
    let (_dir, store, key) = sample("tamper-baseline");
    let files = files(&store, &key);
    assert_eq!(files.len(), 4);
    for file in &files {
        store.read(file).unwrap();
    }
}

#[test]
fn tampered_data_is_detected() {
    type Edit = fn(&mut Vec<u8>);
    let edits: [(&str, Edit); 3] = [
        ("flipped byte", |d| {
            let mid = d.len() / 2;
            d[mid] ^= 0x01;
        }),
        ("truncated", |d| {
            d.pop();
        }),
        ("appended", |d| d.push(0)),
    ];
    for (case, edit) in edits {
        let (dir, store, key) = sample("tamper-data");
        let file = files(&store, &key).remove(0);
        rewrite(&part_path(dir.path(), "parquet", 0), edit);
        assert_integrity(store.read(&file), case);
    }
}

#[test]
fn a_swapped_part_is_detected() {
    let (dir, store, key) = sample("tamper-swap");
    let file = files(&store, &key).remove(0);
    let other = fs::read(part_path(dir.path(), "parquet", 1)).unwrap();
    rewrite(&part_path(dir.path(), "parquet", 0), |d| *d = other);
    assert_integrity(store.read(&file), "swapped part");
}

#[test]
fn an_edited_manifest_is_detected() {
    let (dir, store, key) = sample("tamper-manifest");
    rewrite(&part_path(dir.path(), "manifest", 0), |d| {
        let text = String::from_utf8(d.clone()).unwrap();
        *d = text.replace("\nrows 10\n", "\nrows 9\n").into_bytes();
    });
    let file = files(&store, &key).remove(0);
    assert_eq!(file.rows, 9);
    assert_integrity(store.read(&file), "edited rows");
}

#[test]
fn a_misplaced_manifest_is_corrupt() {
    let (dir, store, key) = sample("tamper-move");
    let target = dir
        .path()
        .join("source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-07")
        .join("part-00009.manifest");
    fs::rename(part_path(dir.path(), "manifest", 0), target).unwrap();
    let result = store.select(&selection(&[&key], D0, D0 + 2 * DAY));
    assert!(
        matches!(result, Err(RawStoreError::Corrupt(_))),
        "{result:?}"
    );
}

/// The documented canonical text (ADR-030), built independently.
fn documented_version(start: i64, end: i64, streams: &[&str], files: &[SealedFile]) -> String {
    let mut text = format!("mie-dataset 1\nwindow {start} {end}\n");
    for s in streams {
        text.push_str(&format!("stream {s}\n"));
    }
    for f in files {
        text.push_str(&format!(
            "file {} {} {}\n",
            f.relative_path, f.rows, f.sha256
        ));
    }
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[test]
fn the_dataset_version_pins_the_covered_files() {
    let (_dir, store, key) = sample("tamper-version");
    let (start, end) = (D0, D0 + DAY);
    let first = store.select(&selection(&[&key], start, end)).unwrap();
    let again = store.select(&selection(&[&key], start, end)).unwrap();
    assert_eq!(first, again);
    assert_eq!(first.files.len(), 3);
    assert_eq!(
        first.version.as_str(),
        documented_version(start, end, &["binance-um/BTCUSDT/aggTrade"], &first.files)
    );

    // A sealed file outside the window leaves the version unchanged.
    let mut writer = store
        .writer("binance-um", RotationPolicy::default())
        .unwrap();
    writer.append(&key, record(D0 + 3 * DAY, 99)).unwrap();
    writer.close().unwrap();
    assert_eq!(
        store.select(&selection(&[&key], start, end)).unwrap(),
        first
    );

    // One inside the window changes it.
    let mut writer = store
        .writer("binance-um", RotationPolicy::default())
        .unwrap();
    writer.append(&key, record(D0 + 5, 100)).unwrap();
    writer.close().unwrap();
    let after = store.select(&selection(&[&key], start, end)).unwrap();
    assert_eq!(after.files.len(), 4);
    assert_ne!(after.version, first.version);
    assert_eq!(
        after.version.as_str(),
        documented_version(start, end, &["binance-um/BTCUSDT/aggTrade"], &after.files)
    );

    // Selecting another stream changes the version even without its files.
    let both = store
        .select(&selection(&[&key, &stream("depth")], start, end))
        .unwrap();
    assert_eq!(both.files, after.files);
    assert_ne!(both.version, after.version);
}

#[test]
fn stream_lines_sort_bytewise_across_sources() {
    let dir = TempDir::new("tamper-source-order");
    let store = ParquetRawStore::new(dir.path());
    let um = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
    let plain = RawStreamKey::new("binance", "BTCUSDT", "aggTrade").unwrap();
    // Field-wise the keys order `binance` first; bytewise the rendered lines
    // order `binance-um/` first, because '-' (0x2D) < '/' (0x2F).
    assert!(plain < um);
    for (source, key) in [("binance", &plain), ("binance-um", &um)] {
        let mut writer = store.writer(source, RotationPolicy::default()).unwrap();
        writer.append(key, record(D0 + 1, 1)).unwrap();
        writer.close().unwrap();
    }
    let (start, end) = (D0, D0 + DAY);
    let dataset = store
        .select(&selection(&[&plain, &um], start, end))
        .unwrap();
    assert_eq!(dataset.files.len(), 2);

    // Expected text built by hand: stream lines as literals in bytewise
    // order, file lines sorted here rather than taken in the store's order.
    let mut file_lines: Vec<String> = dataset
        .files
        .iter()
        .map(|f| format!("file {} {} {}\n", f.relative_path, f.rows, f.sha256))
        .collect();
    file_lines.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    assert!(file_lines[0].starts_with("file source=binance-um/"));
    let text = format!(
        "mie-dataset 1\nwindow {start} {end}\n\
         stream binance-um/BTCUSDT/aggTrade\n\
         stream binance/BTCUSDT/aggTrade\n{}",
        file_lines.concat()
    );
    let expected: String = Sha256::digest(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(dataset.version.as_str(), expected);
}
