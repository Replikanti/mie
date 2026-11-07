//! `mie store replicate` offline (ADR-062 D5, D11, #189): the device and
//! marker refusals (AW6), archive parts that wait for their import entry,
//! damaged copies, a second pass over unchanged state, and `select` before
//! and after a pass.

mod common;

use common::{MapProbe, RawLine, TempDir, init_tiers};
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_parquet::replica;
use mie_adapter_parquet::root_marker;
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_cli::config::{ColdRoot, IngestConfig};
use mie_cli::store::{self, DeviceProbe, StDevProbe};
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawRecord, RawRecordSink, RawRecordSource, RawSelection, RawStreamKey};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const D0: i64 = common::D0;

/// The archive file whose two parts the fixtures seal.
const ENTRY: &str = "aggTrades/BTCUSDT-aggTrades-2026-10-05.zip.import";

/// The ingest configuration of a tiered store under `dir`: hot `dir/raw`,
/// copy targets `dir/cold-a` and `dir/cold-b`, written to
/// `dir/ingest.toml` too, for the binary.
fn tiered(dir: &Path) -> IngestConfig {
    let text = format!(
        "[instrument]\nsymbol = \"BTCUSDT\"\nsource = \"binance-um\"\n[paths]\n\
         raw_root = \"{}\"\njournal = \"{}\"\ncold_roots = {}\n[binance]\n\
         ws_base_url = \"wss://fake.invalid/ws\"\nrest_base_url = \"https://fake.invalid\"\n\
         streams = [\"aggTrade\"]\n",
        dir.join("raw").display(),
        dir.join("journal.jsonl").display(),
        common::cold_roots_toml(&common::cold_roots(dir)),
    );
    fs::write(dir.join("ingest.toml"), &text).unwrap();
    IngestConfig::parse(&text).unwrap()
}

/// The archive configuration of the store under `dir` at
/// `dir/archive.toml`, with `raw_root` and `cold_roots` as given.
fn archive_config(dir: &Path, raw_root: &Path, cold_roots: &[ColdRoot]) -> PathBuf {
    let path = dir.join("archive.toml");
    fs::write(
        &path,
        format!(
            "[instrument]\nsymbol = \"BTCUSDT\"\n[paths]\nraw_root = \"{}\"\n\
             import_ledger = \"{}\"\nstaging = \"{}\"\ncold_roots = {}\n",
            raw_root.display(),
            dir.join("ledger").display(),
            dir.join("staging").display(),
            common::cold_roots_toml(cold_roots),
        ),
    )
    .unwrap();
    path
}

/// A tiered store under `dir` with three live `aggTrade` parts, two
/// archive parts of one archive file (no entry yet) and a journal, hot and
/// both copy targets initialized. Returns the configuration and the
/// archive parts.
fn store(dir: &Path) -> (IngestConfig, Vec<String>) {
    let config = tiered(dir);
    let records: Vec<RawLine> = (0..3)
        .map(|i| RawLine {
            stream: "aggTrade",
            receive_seq: i,
            session_id: "r1/aggTrade/1".to_owned(),
            event_time_ms: D0 + i as i64 * 7_200_000,
            payload: common::agg(i + 1, D0 + i as i64 * 7_200_000),
        })
        .collect();
    let journal = [
        serde_json::json!({"type": "run_start", "run_id": "r1"}),
        serde_json::json!({"type": "sealed", "run_id": "r1"}),
    ];
    common::write_records(&config, &records, &journal);
    // Two hours apart, past the one-hour span of a part (ADR-030 D4): three
    // live parts.
    let raw = &config.paths.raw_root;
    for path in replica::list_sealed(raw).unwrap() {
        assert!(path.starts_with("source=binance-um/"), "{path}");
    }
    let archive = seal_archive(raw);
    init_tiers(raw, &config.paths.cold_roots);
    (config, archive)
}

/// Seals four archive records two per part: the two parts of one archive
/// file. Returns their data paths.
fn seal_archive(raw: &Path) -> Vec<String> {
    let before = replica::list_sealed(raw).unwrap();
    let key = RawStreamKey::new(ARCHIVE_SOURCE, "BTCUSDT", "aggTrades").unwrap();
    let policy = RotationPolicy {
        max_rows: 2,
        ..RotationPolicy::default()
    };
    let mut writer = ParquetRawStore::tiered(raw, Vec::new())
        .writer(ARCHIVE_SOURCE, policy)
        .unwrap();
    for i in 0..4 {
        let time = D0 - 86_400_000 + i * 60_000;
        writer
            .append(
                &key,
                RawRecord {
                    event_time: EventTime::from_millis(time),
                    capture: None,
                    payload: format!("{i},85000.10,0.010,{i},{i},{time},false").into_bytes(),
                },
            )
            .unwrap();
    }
    writer.close().unwrap();
    replica::list_sealed(raw)
        .unwrap()
        .into_iter()
        .filter(|path| !before.contains(path))
        .collect()
}

/// Writes the importer's `PENDING` marker for the archive file.
fn write_pending(dir: &Path) {
    fs::create_dir_all(dir.join("ledger")).unwrap();
    fs::write(
        dir.join("ledger/PENDING"),
        format!(
            "mie-archive-pending 1\n\
             archive data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-05.zip\n\
             sha256 {}\nstream binance-archive/BTCUSDT/aggTrades\nperiod 2026-10-05\n",
            "ab".repeat(32)
        ),
    )
    .unwrap();
}

/// Writes the import entry listing `parts` with their manifests' SHA-256,
/// and removes `PENDING`, as the importer ends a file.
fn write_entry(dir: &Path, raw: &Path, parts: &[String]) {
    let mut text = format!(
        "mie-archive-import 1\n\
         archive data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-05.zip\n\
         sha256 {}\nstream binance-archive/BTCUSDT/aggTrades\nperiod 2026-10-05\nheader -\n\
         rows 4\n",
        "ab".repeat(32)
    );
    for part in parts {
        let (_, file) = replica::sealed_manifest(raw, part).unwrap().unwrap();
        text.push_str(&format!("file {part} {} {}\n", file.rows, file.sha256));
    }
    let path = dir.join("ledger").join(ENTRY);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, text).unwrap();
    let _ = fs::remove_file(dir.join("ledger/PENDING"));
}

/// hot on device 1, the copy targets on 2 and 3.
fn distinct(dir: &Path) -> MapProbe {
    MapProbe(vec![
        (dir.join("raw"), 1),
        (dir.join("cold-a"), 2),
        (dir.join("cold-b"), 3),
    ])
}

/// One pass: whether it passed, and its report.
fn replicate(
    config: &IngestConfig,
    archive: Option<&Path>,
    probe: &dyn DeviceProbe,
) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::replicate(config, archive, probe, &|| D0 + 1, &mut out).unwrap();
    (pass, String::from_utf8(out).unwrap())
}

/// Every file and directory below `root`, with file contents, sorted.
fn snapshot(root: &Path) -> Vec<(String, Option<Vec<u8>>)> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, Option<Vec<u8>>)>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        let mut names: Vec<String> = entries
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        for name in names {
            let path = dir.join(&name);
            let rel = format!("{prefix}{name}");
            if path.is_dir() {
                out.push((rel.clone(), None));
                walk(&path, &format!("{rel}/"), out);
            } else {
                out.push((rel, Some(fs::read(&path).unwrap())));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, "", &mut out);
    out
}

/// Whether `root` holds the sealed part `path` (data and manifest)
/// byte-identical to hot's.
fn holds(raw: &Path, root: &Path, path: &str) -> bool {
    let manifest = format!("{}.manifest", path.strip_suffix(".parquet").unwrap());
    [path, manifest.as_str()].iter().all(|file| {
        fs::read(root.join(file)).is_ok_and(|bytes| bytes == fs::read(raw.join(file)).unwrap())
    })
}

#[track_caller]
fn assert_refused(result: (bool, String), expected: &[&str]) {
    let (pass, out) = result;
    assert!(!pass, "{out}");
    for text in expected {
        assert!(out.contains(text), "expected {text:?} in {out}");
    }
}

/// AW6: two copy targets on one device, and a copy target on hot's, are
/// refused, naming both roots, before anything is written.
#[test]
fn roots_on_one_device_are_refused_naming_both() {
    let dir = TempDir::new("replicate-devices");
    let dir = dir.path();
    let (config, _) = store(dir);
    let (cold_a, cold_b) = (dir.join("cold-a"), dir.join("cold-b"));
    let before = [snapshot(&cold_a), snapshot(&cold_b)];
    let shared = MapProbe(vec![
        (dir.join("raw"), 1),
        (cold_a.clone(), 2),
        (cold_b.clone(), 2),
    ]);
    let (a, b) = (cold_a.display().to_string(), cold_b.display().to_string());
    assert_refused(
        replicate(&config, None, &shared),
        &["FAIL: copy targets", &a, &b, "both on device 2"],
    );
    let on_hot = MapProbe(vec![
        (dir.join("raw"), 1),
        (cold_a.clone(), 1),
        (cold_b.clone(), 3),
    ]);
    let raw = dir.join("raw").display().to_string();
    assert_refused(
        replicate(&config, None, &on_hot),
        &[&a, &raw, "the device of raw_root"],
    );
    assert_eq!([snapshot(&cold_a), snapshot(&cold_b)], before);

    // The real probe: two directories on one filesystem.
    let (pass, out) = replicate(&config, None, &StDevProbe);
    assert!(!pass, "{out}");
    assert!(
        out.contains(&format!("copy targets {a} and {b} are both on device")),
        "{out}"
    );
    assert_eq!([snapshot(&cold_a), snapshot(&cold_b)], before);

    // And the binary, without any override.
    let output = Command::new(env!("CARGO_BIN_EXE_mie"))
        .args(["store", "replicate", "--config"])
        .arg(dir.join("ingest.toml"))
        .env_remove("MIE_DEV_DEVICE_IDS")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(output.status.code(), Some(1), "{stdout}");
    assert!(
        stdout.contains(&format!(
            "FAIL: copy targets {a} and {b} are both on device"
        )),
        "{stdout}"
    );
    assert_eq!([snapshot(&cold_a), snapshot(&cold_b)], before);
}

/// AW6: a copy target without its marker is skipped and named, nothing is
/// written into it, and the pass fails; with both markers it passes.
#[test]
fn a_target_without_its_marker_is_skipped_and_untouched() {
    let dir = TempDir::new("replicate-marker");
    let dir = dir.path();
    let (config, parts) = store(dir);
    let (cold_a, cold_b) = (dir.join("cold-a"), dir.join("cold-b"));
    fs::remove_dir_all(cold_b.join(".mie")).unwrap();
    let probe = distinct(dir);
    let (pass, out) = replicate(&config, None, &probe);
    assert!(!pass, "{out}");
    assert!(
        out.contains(&format!(
            "FAIL: copy target {} has no root marker: skipped",
            cold_b.display()
        )),
        "{out}"
    );
    assert_eq!(snapshot(&cold_b), Vec::new(), "nothing, not even .mie/");
    let raw = &config.paths.raw_root;
    let live: Vec<String> = replica::list_sealed(raw)
        .unwrap()
        .into_iter()
        .filter(|path| path.starts_with("source=binance-um/"))
        .collect();
    assert_eq!(live.len(), 3);
    for path in &live {
        assert!(holds(raw, &cold_a, path), "{path}");
        assert!(
            out.contains(&format!("FAIL: {path}: 1 of 2 copies")),
            "{out}"
        );
    }

    // With the marker back the pass completes.
    root_marker::create_meta(&cold_b, "cold-b", false).unwrap();
    let archive = archive_config(dir, raw, &config.paths.cold_roots);
    write_entry(dir, raw, &parts);
    let (pass, out) = replicate(&config, Some(&archive), &probe);
    assert!(pass, "{out}");
    assert!(out.ends_with("PASS\n"), "{out}");
    for path in replica::list_sealed(raw).unwrap() {
        assert!(holds(raw, &cold_b, &path), "{path}");
    }
}

/// An archive file's parts wait for its entry, then the parts and then the
/// entry reach both targets; `PENDING` never does. Without an archive
/// configuration the live files and the journal are copied and the archive
/// parts are named.
#[test]
fn archive_parts_wait_for_their_import_entry() {
    let dir = TempDir::new("replicate-archive");
    let dir = dir.path();
    let (config, parts) = store(dir);
    assert_eq!(parts.len(), 2);
    let raw = config.paths.raw_root.clone();
    let probe = distinct(dir);

    // Without an archive configuration.
    let (pass, out) = replicate(&config, None, &probe);
    assert!(!pass, "{out}");
    for part in &parts {
        assert!(
            out.contains(&format!(
                "FAIL: {part}: 0 of 2 copies: an archive part, and no archive"
            )),
            "{out}"
        );
    }
    for root in common::cold_roots(dir) {
        for part in &parts {
            assert!(!root.path.join(part).exists(), "{part}");
        }
        assert_eq!(
            fs::read(root.path.join(".mie/journal/journal.jsonl")).unwrap(),
            fs::read(&config.paths.journal).unwrap()
        );
    }

    // The import runs: PENDING, no entry yet.
    let archive = archive_config(dir, &raw, &config.paths.cold_roots);
    write_pending(dir);
    let (pass, out) = replicate(&config, Some(&archive), &probe);
    assert!(!pass, "{out}");
    for part in &parts {
        assert!(
            out.contains(&format!(
                "FAIL: {part}: 0 of 2 copies: waiting for its import entry"
            )),
            "{out}"
        );
    }
    for root in common::cold_roots(dir) {
        for part in &parts {
            assert!(!root.path.join(part).exists(), "{part}");
        }
        assert!(!root.path.join(".mie/import-ledger").exists());
    }

    // The entry exists: the parts, then the entry.
    write_entry(dir, &raw, &parts);
    let (pass, out) = replicate(&config, Some(&archive), &probe);
    assert!(pass, "{out}");
    let entry_line = out
        .lines()
        .position(|line| line == format!("copied import entry {ENTRY} to cold-a"))
        .expect("the entry was copied");
    for part in &parts {
        let part_line = out
            .lines()
            .position(|line| line == format!("copied {part} to cold-a"))
            .expect("the part was copied");
        assert!(part_line < entry_line, "{out}");
    }
    for root in common::cold_roots(dir) {
        for part in &parts {
            assert!(holds(&raw, &root.path, part), "{part}");
        }
        assert_eq!(
            fs::read(root.path.join(".mie/import-ledger").join(ENTRY)).unwrap(),
            fs::read(dir.join("ledger").join(ENTRY)).unwrap()
        );
        let names: Vec<_> = snapshot(&root.path)
            .into_iter()
            .map(|(name, _)| name)
            .filter(|name| name.contains("PENDING"))
            .collect();
        assert!(names.is_empty(), "{names:?}");
    }
}

/// An entry that lists a part with another SHA-256 than its manifest's is
/// an integrity problem: the part is not copied.
#[test]
fn an_entry_with_another_sha256_keeps_its_part_off_the_targets() {
    let dir = TempDir::new("replicate-entry-sha");
    let dir = dir.path();
    let (config, parts) = store(dir);
    let raw = config.paths.raw_root.clone();
    let archive = archive_config(dir, &raw, &config.paths.cold_roots);
    write_entry(dir, &raw, &parts);
    let entry = dir.join("ledger").join(ENTRY);
    let (_, file) = replica::sealed_manifest(&raw, &parts[0]).unwrap().unwrap();
    let text = fs::read_to_string(&entry)
        .unwrap()
        .replace(&file.sha256, &"0".repeat(64));
    fs::write(&entry, text).unwrap();
    let (pass, out) = replicate(&config, Some(&archive), &distinct(dir));
    assert!(!pass, "{out}");
    assert!(
        out.contains(&format!("FAIL: {}: 0 of 2 copies: integrity", parts[0])),
        "{out}"
    );
    assert!(!dir.join("cold-a").join(&parts[0]).exists());
    assert!(holds(&raw, &dir.join("cold-a"), &parts[1]));
}

/// A flipped byte in the last record of a target's journal copy, or of its
/// ledger copy, refuses that target's step and names it; the other target
/// completes.
#[test]
fn a_damaged_tail_copy_is_refused_for_its_target_only() {
    let dir = TempDir::new("replicate-damage");
    let dir = dir.path();
    let (config, _) = store(dir);
    let probe = distinct(dir);
    let (pass, out) = replicate(&config, None, &probe);
    assert!(!pass, "the archive parts wait: {out}");
    let flip_last = |path: &Path| {
        let mut bytes = fs::read(path).unwrap();
        let at = bytes.len() - 3;
        bytes[at] ^= 0x01;
        fs::write(path, bytes).unwrap();
    };
    let journal_a = dir.join("cold-a/.mie/journal/journal.jsonl");
    let ledger_b = dir.join("cold-b/.mie/copy-ledger");
    flip_last(&journal_a);
    flip_last(&ledger_b);
    let (damaged_journal, damaged_ledger) =
        (fs::read(&journal_a).unwrap(), fs::read(&ledger_b).unwrap());
    // A new journal line to copy.
    let mut journal = fs::read(&config.paths.journal).unwrap();
    journal.extend_from_slice(b"{\"type\":\"run_end\",\"run_id\":\"r1\"}\n");
    fs::write(&config.paths.journal, &journal).unwrap();

    let (pass, out) = replicate(&config, None, &probe);
    assert!(!pass, "{out}");
    assert!(
        out.contains(&format!(
            "FAIL: the journal copy on cold-a ({}) is refused",
            journal_a.display()
        )),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "FAIL: the copy-ledger copy on cold-b ({}) is refused",
            ledger_b.display()
        )),
        "{out}"
    );
    // The damaged copies are untouched; the other target's copies are
    // current.
    assert_eq!(fs::read(&journal_a).unwrap(), damaged_journal);
    assert_eq!(fs::read(&ledger_b).unwrap(), damaged_ledger);
    assert_eq!(
        fs::read(dir.join("cold-b/.mie/journal/journal.jsonl")).unwrap(),
        journal
    );
    let hot_ledger = fs::read(dir.join("raw/.mie/copy-ledger")).unwrap();
    let ledger_a = fs::read(dir.join("cold-a/.mie/copy-ledger")).unwrap();
    assert_eq!(&hot_ledger[..ledger_a.len()], &ledger_a[..]);
}

/// A second pass over unchanged state copies nothing and appends nothing:
/// every root, hot's ledger and the journal are byte-identical.
#[test]
fn a_second_pass_over_unchanged_state_writes_nothing() {
    let dir = TempDir::new("replicate-idle");
    let dir = dir.path();
    let (config, parts) = store(dir);
    let raw = config.paths.raw_root.clone();
    let archive = archive_config(dir, &raw, &config.paths.cold_roots);
    write_entry(dir, &raw, &parts);
    let probe = distinct(dir);
    let (pass, out) = replicate(&config, Some(&archive), &probe);
    assert!(pass, "{out}");
    let before: Vec<_> = ["raw", "cold-a", "cold-b", "ledger"]
        .map(|name| snapshot(&dir.join(name)))
        .to_vec();
    let journal = fs::read(&config.paths.journal).unwrap();
    let (pass, out) = replicate(&config, Some(&archive), &probe);
    assert!(pass, "{out}");
    assert_eq!(
        out,
        "copied 0 file(s) and 0 import entries; appended 0 copy-ledger byte(s) and 0 journal \
         byte(s)\nPASS\n"
    );
    let after: Vec<_> = ["raw", "cold-a", "cold-b", "ledger"]
        .map(|name| snapshot(&dir.join(name)))
        .to_vec();
    assert_eq!(after, before);
    assert_eq!(fs::read(&config.paths.journal).unwrap(), journal);
}

/// `select` returns the same files and dataset version before and after a
/// pass: the new `manifest` records merge with hot's (D5, D7).
#[test]
fn select_is_unchanged_by_a_pass() {
    let dir = TempDir::new("replicate-select");
    let dir = dir.path();
    let (config, parts) = store(dir);
    let raw = config.paths.raw_root.clone();
    let archive = archive_config(dir, &raw, &config.paths.cold_roots);
    write_entry(dir, &raw, &parts);
    let tiered = store::raw_store(&raw, &config.paths.cold_roots).unwrap();
    let selection = RawSelection::new(
        [
            RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap(),
            RawStreamKey::new(ARCHIVE_SOURCE, "BTCUSDT", "aggTrades").unwrap(),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>(),
        ReplayWindow {
            start: EventTime::from_millis(0),
            end: EventTime::from_millis(D0 + 2 * 86_400_000),
        },
    )
    .unwrap();
    let before = tiered.select(&selection).unwrap();
    assert_eq!(before.files.len(), 5);
    let (pass, out) = replicate(&config, Some(&archive), &distinct(dir));
    assert!(pass, "{out}");
    assert_eq!(tiered.select(&selection).unwrap(), before);
}

/// An archive configuration that describes another store is refused
/// before anything is written.
#[test]
fn an_archive_configuration_of_another_store_is_refused() {
    let dir = TempDir::new("replicate-archive-config");
    let dir = dir.path();
    let (config, _) = store(dir);
    let raw = config.paths.raw_root.clone();
    let probe = distinct(dir);
    let before = [snapshot(&dir.join("cold-a")), snapshot(&dir.join("cold-b"))];

    let elsewhere = dir.join("elsewhere");
    fs::create_dir_all(&elsewhere).unwrap();
    let archive = archive_config(dir, &elsewhere, &config.paths.cold_roots);
    assert_refused(
        replicate(&config, Some(&archive), &probe),
        &[
            "FAIL: invalid configuration: --archive-config",
            "its raw_root",
        ],
    );
    let mut swapped = config.paths.cold_roots.clone();
    swapped.reverse();
    let archive = archive_config(dir, &raw, &swapped);
    assert_refused(
        replicate(&config, Some(&archive), &probe),
        &[
            "FAIL: invalid configuration: --archive-config",
            "paths.cold_roots",
        ],
    );
    assert_eq!(
        [snapshot(&dir.join("cold-a")), snapshot(&dir.join("cold-b"))],
        before
    );
}

/// Without a copy ledger on hot the pass does nothing and says so; with
/// no cold roots it is refused.
#[test]
fn a_missing_ledger_or_no_cold_roots_refuses_the_pass() {
    let dir = TempDir::new("replicate-degraded");
    let dir = dir.path();
    let (config, _) = store(dir);
    let probe = distinct(dir);
    let ledger = dir.join("raw/.mie/copy-ledger");
    fs::remove_file(&ledger).unwrap();
    let before = [snapshot(&dir.join("cold-a")), snapshot(&dir.join("cold-b"))];
    assert_refused(
        replicate(&config, None, &probe),
        &[
            &format!("FAIL: copy ledger {} does not exist", ledger.display()),
            "degraded start",
        ],
    );
    assert_eq!(
        [snapshot(&dir.join("cold-a")), snapshot(&dir.join("cold-b"))],
        before
    );
    assert_refused(
        replicate(&common::config(dir), None, &probe),
        &["FAIL: paths.cold_roots is empty"],
    );
}

#[test]
fn usage_errors_exit_2() {
    for args in [
        vec!["store", "replicate"],
        vec!["store", "replicate", "--config"],
        vec![
            "store",
            "replicate",
            "--config",
            "x.toml",
            "--archive-config",
        ],
        vec!["store", "replicate", "--config", "x.toml", "--root", "y"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_mie"))
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}");
    }
}
