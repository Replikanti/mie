//! `mie store restore` offline (ADR-062 D11, #195): AW9 end to end, the
//! held variant and its re-admit, a replace that keeps failing until the
//! re-import, a damaged copy passed over, and the two stops. The fixture is
//! the adopted store of `mie store adopt` offline.

mod common;

use common::resolve::*;
use common::tiers::*;
use common::{D0, HOUR, NoHttp, ScriptedConnector, agg, ingest, journal};
use mie_adapter_parquet::copy_ledger;
use mie_adapter_parquet::{replica, root_marker};
use mie_cli::config::IngestConfig;
use mie_cli::ingest::Transports;
use mie_cli::store::{self, restore::Flags};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// When the restore runs.
const RESTORED: i64 = D0 + 5 * HOUR;
/// The capture before the loss, and the one after the restore.
const BEFORE: i64 = D0 + 2 * HOUR;
const AFTER: i64 = D0 + 6 * HOUR;

fn restore(fixture: &Fixture, flags: Flags, now: i64) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::restore(
        &fixture.config,
        Some(&fixture.dst_archive),
        flags,
        FIRST_HOT,
        &fixture.probe(),
        &|| now,
        &mut out,
    )
    .unwrap();
    (pass, String::from_utf8(out).unwrap())
}

fn repair(fixture: &Fixture, now: i64) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::repair(
        &fixture.config,
        Some(&fixture.dst_archive),
        false,
        &fixture.probe(),
        &|| now,
        &mut out,
    )
    .unwrap();
    (pass, String::from_utf8(out).unwrap())
}

/// `mie ingest` up to its start refusal, if any.
fn start_ingest(config: &IngestConfig, start: i64) -> Result<i32, String> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let transports = Transports {
        connector: ScriptedConnector::new(
            "btcusdt@aggTrade",
            vec![vec![agg(900, start + 10)]],
            &shutdown,
        ),
        http: Arc::new(NoHttp),
        clock: common::FakeClock::new(start, "mie-aggTrade"),
        device_probe: common::probe_for(config),
    };
    mie_cli::ingest::run(config, transports, shutdown).map(|outcome| outcome.exit_code())
}

/// The run ids of the journal's `run_start` lines.
fn run_ids(config: &IngestConfig) -> Vec<String> {
    journal(config)
        .into_iter()
        .filter(|line| line["type"] == "run_start")
        .map(|line| line["run_id"].as_str().unwrap().to_owned())
        .collect()
}

/// The `run_start` line of the last run.
fn last_run_start(config: &IngestConfig) -> Value {
    journal(config)
        .into_iter()
        .rfind(|line| line["type"] == "run_start")
        .unwrap()
}

/// Copies the layout below `from` into `to` (no `.mie/`, no quarantine, no
/// lock files), keeping what `to` already holds.
fn copy_layout(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name == ".mie" || name == ".quarantine" || name.ends_with(".lock") {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            copy_layout(&path, &to.join(&name));
        } else if !to.join(&name).exists() {
            fs::copy(&path, to.join(&name)).unwrap();
        }
    }
}

fn part_number(path: &str) -> u32 {
    path.rsplit_once("part-")
        .and_then(|(_, n)| n.strip_suffix(".parquet"))
        .unwrap()
        .parse()
        .unwrap()
}

/// AW9: a capture with its replication, the loss of hot, both writers
/// refusing with nothing created, then the restore. After it the run ids
/// and every pre-loss report are unchanged, a new capture continues each
/// partition's part numbers without reusing a cold path, and its seeds
/// equal a single-root start over the pre-loss store.
#[test]
fn aw9_base_restore_then_capture() {
    let (_dir, fixture) = adopted("restore-aw9");
    let outcome = ingest(
        &fixture.config,
        BEFORE,
        vec![vec![agg(400, BEFORE + 10), agg(401, BEFORE + 20)]],
    );
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    assert_pass(&replicate(&fixture, BEFORE + HOUR));
    let baseline = reports(&fixture.dir, &fixture.dst_ingest, &fixture.dst_archive);
    let runs = run_ids(&fixture.config);
    assert!(runs.len() >= 3, "{runs:?}");

    // The pre-loss store as a single root, for the seeds.
    let single = fixture.dir.join("single");
    for root in [cold(&fixture, "cold-a"), fixture.hot()] {
        copy_layout(&root, &single.join("raw"));
    }
    fs::copy(&fixture.config.paths.journal, single.join("journal.jsonl")).unwrap();
    let single_config = ingest_toml(
        &fixture.dir.join("single.toml"),
        &single.join("raw"),
        &single.join("journal.jsonl"),
        &[],
    );
    let cold_paths: BTreeSet<String> = replica::list_sealed(&cold(&fixture, "cold-a"))
        .unwrap()
        .into_iter()
        .collect();

    lose_hot(&fixture);
    let error = start_ingest(&fixture.config, AFTER).unwrap_err();
    assert!(error.contains("mie store restore"), "{error}");
    for dry_run in [false, true] {
        let error =
            run_import(&fixture, &metrics_day(day_index("2026-10-03"), dry_run)).unwrap_err();
        assert!(error.contains("mie store restore"), "{error}");
    }
    assert_eq!(fs::read_dir(fixture.hot()).unwrap().count(), 0);
    assert!(!fixture.config.paths.journal.exists());
    assert!(!fixture.archive_config.paths.import_ledger.exists());

    let (pass, out) = restore(&fixture, Flags::default(), RESTORED);
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    assert_eq!(run_ids(&fixture.config), runs);
    assert_eq!(
        reports(&fixture.dir, &fixture.dst_ingest, &fixture.dst_archive),
        baseline
    );

    // A new capture continues each partition's numbering.
    let hot_before: BTreeSet<String> = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .collect();
    let frames = vec![vec![agg(500, AFTER + 10), agg(501, AFTER + 20)]];
    let outcome = ingest(&fixture.config, AFTER, frames.clone());
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    let fresh: Vec<String> = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .filter(|path| !hot_before.contains(path))
        .collect();
    assert!(!fresh.is_empty());
    for path in &fresh {
        assert!(!cold_paths.contains(path), "{path} reuses a cold path");
        let partition = partition_of(path);
        let highest = cold_paths
            .iter()
            .filter(|p| partition_of(p) == partition)
            .map(|p| part_number(p))
            .max();
        if let Some(highest) = highest {
            assert_eq!(part_number(path), highest + 1, "{path}");
        }
    }
    let start = last_run_start(&fixture.config);
    let reference = ingest(&single_config, AFTER, frames);
    assert_eq!(reference.exit_code(), 0, "{:?}", reference.error);
    let reference = last_run_start(&single_config);
    assert_ne!(reference["seeds"], serde_json::json!({}));
    assert_eq!(start["seeds"], reference["seeds"]);
    assert_eq!(start["seed_listing"], "union");
}

/// The held variant: an import and its pass, then the entry copies gone
/// from the targets (the state a pass that stopped between parts and entry
/// leaves; the targets' ledger copies already record the parts), then the
/// loss. The restore holds both parts; an archive replay covering them
/// fails naming them; the importer refuses; the re-admit clears them, and
/// every report equals the pre-loss one.
fn held_after_restore(tag: &str) -> (common::TempDir, Fixture, Vec<String>, String) {
    let (dir, fixture) = adopted(tag);
    let day = day_index("2026-10-03");
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    run_import(&fixture, &metrics_day(day, false)).unwrap();
    let parts: Vec<String> = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .filter(|path| !before.contains(path))
        .filter(|path| !cold(&fixture, "cold-a").join(path).exists())
        .collect();
    assert_eq!(parts.len(), 2, "{parts:?}");
    assert_pass(&replicate(&fixture, D0 + 2 * HOUR));
    let baseline = reports(&fixture.dir, &fixture.dst_ingest, &fixture.dst_archive);
    let e0 = format!("{}.import", metrics_name());
    for root in ["cold-a", "cold-b"] {
        fs::remove_file(target_file(&fixture, root, &e0)).unwrap();
    }
    lose_hot(&fixture);
    let (pass, out) = restore(&fixture, Flags::default(), RESTORED);
    assert!(!pass, "{out}");
    assert!(out.contains("restored: hot marker written"), "{out}");
    for part in &parts {
        assert_fail_line(&out, &[part, "held (unlisted)"]);
        assert!(!fixture.hot().join(part).exists(), "{part}");
    }
    assert_eq!(held(&fixture), parts.iter().cloned().collect());
    (dir, fixture, parts, baseline)
}

#[test]
fn aw9_held_then_readmit() {
    let (_dir, fixture, parts, baseline) = held_after_restore("restore-aw9-held");
    let failing = selected(&fixture);
    assert!(
        failing
            .iter()
            .any(|s| s.starts_with("error") && s.contains(&parts[0])),
        "{failing:?}"
    );
    let error = run_import(&fixture, &metrics_day(day_index("2026-10-03"), false)).unwrap_err();
    assert!(error.contains("held"), "{error}");
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESTORED + HOUR);
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    assert!(held(&fixture).is_empty());
    assert_eq!(
        reports(&fixture.dir, &fixture.dst_ingest, &fixture.dst_archive),
        baseline
    );
}

/// The fake archive now serves changed rows: the re-admit names the
/// difference; `--replace` quarantines and supersedes; every covering
/// replay fails until `mie archive-import` writes the new entry and its
/// `replaced` records; the quarantined bytes stay on every root.
#[test]
fn aw9_replace_keeps_failing_until_reimport() {
    let (_dir, fixture, parts, _) = held_after_restore("restore-aw9-replace");
    let day = day_index("2026-10-03");
    let changed = metrics_csv(day).replace(
        "2026-10-03 07:00:00,BTCUSDT,90007.1230000000000000",
        "2026-10-03 07:00:00,BTCUSDT,90007.9990000000000000",
    );
    fixture.archive.publish(
        mie_adapter_binance::archive::ArchiveStream::Metrics,
        day,
        &changed,
    );
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESTORED + HOUR);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["nothing written", "--replace"]);
    let (pass, out) = resolve_full(
        &fixture,
        Some(&metrics_file()),
        true,
        &fixture.probe(),
        Some(&fixture.dst_archive),
        RESTORED + 2 * HOUR,
    );
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    let id = format!("q-{}", RESTORED + 2 * HOUR);
    let failing = selected(&fixture);
    assert!(
        failing
            .iter()
            .any(|s| s.contains(&format!("quarantined under {id}"))),
        "{failing:?}"
    );
    let out = run_import(&fixture, &metrics_day(day, false)).unwrap();
    let e1 = format!("{}.1.import", metrics_name());
    assert!(ledger_file(&fixture, &e1).exists(), "{out}");
    let after = selected(&fixture);
    assert!(after.iter().all(|s| !s.starts_with("error")), "{after:?}");
    for part in &parts {
        let held_somewhere = ["cold-a", "cold-b"].iter().any(|root| {
            replica::quarantine_dir(&cold(&fixture, root), &id)
                .join(part)
                .exists()
        });
        assert!(held_somewhere, "{part}");
        for root in ["cold-a", "cold-b"] {
            assert!(
                replica::quarantine_dir(&cold(&fixture, root), &id)
                    .join(part)
                    .exists(),
                "{part} on {root}"
            );
        }
    }
}

/// One byte flipped in cold-a's copy of an archive part in the hot window:
/// the restore copies it from cold-b, a `FAIL:` line names cold-a's copy
/// for verify, then repair, and nothing is held; verify, then repair fix
/// it.
#[test]
fn aw9_damaged_copy_is_passed_over() {
    let (_dir, fixture) = adopted("restore-aw9-damaged");
    let part = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .find(|path| path.contains("stream=aggTrades") && date_of(path) >= FIRST_HOT)
        .unwrap();
    lose_hot(&fixture);
    let file = cold(&fixture, "cold-a").join(&part);
    let mut permissions = fs::metadata(&file).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&file, permissions).unwrap();
    let mut bytes = fs::read(&file).unwrap();
    bytes[0] ^= 0x01;
    fs::write(&file, bytes).unwrap();
    let (pass, out) = restore(&fixture, Flags::default(), RESTORED);
    assert!(!pass, "{out}");
    assert!(out.contains("restored: hot marker written"), "{out}");
    assert_fail_line(
        &out,
        &[&part, "cold-a", "mie store verify", "mie store repair"],
    );
    assert_eq!(
        out.lines().filter(|l| l.starts_with("FAIL: ")).count(),
        1,
        "{out}"
    );
    assert!(held(&fixture).is_empty());
    assert_eq!(
        fs::read(fixture.hot().join(&part)).unwrap(),
        fs::read(cold(&fixture, "cold-b").join(&part)).unwrap()
    );
    let (pass, out) = verify(&fixture, RESTORED + HOUR);
    assert!(!pass && out.contains(&part), "{out}");
    let (pass, out) = repair(&fixture, RESTORED + 2 * HOUR);
    assert!(pass, "{out}");
    let (pass, out) = verify(&fixture, RESTORED + 3 * HOUR);
    assert!(pass, "{out}");
}

/// Journal copies that differ beyond their common prefix stop the
/// restore, nothing written.
#[test]
fn stop_journal_divergence() {
    let (_dir, fixture) = adopted("restore-stop-journal");
    lose_hot(&fixture);
    let copy = cold(&fixture, "cold-a").join(".mie/journal/journal.jsonl");
    let mut bytes = fs::read(&copy).unwrap();
    bytes[10] ^= 0x01;
    let mut permissions = fs::metadata(&copy).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&copy, permissions).unwrap();
    fs::write(&copy, bytes).unwrap();
    let (pass, out) = restore(&fixture, Flags::default(), RESTORED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["differ", "byte 10", "cold-a"]);
    assert!(out.contains("not restored: no hot marker written"), "{out}");
    assert!(!fixture.config.paths.journal.exists());
    assert_eq!(root_marker::read(&fixture.hot()).unwrap(), None);
    assert!(replica::list_sealed(&fixture.hot()).unwrap().is_empty());
}

/// A live path with different manifest texts on two roots and no
/// copy-ledger record to decide stops the restore, nothing written.
#[test]
fn stop_live_two_texts_without_record() {
    let (_dir, fixture) = adopted("restore-stop-texts");
    let path = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .find(|path| path.starts_with("source=binance-um"))
        .unwrap();
    lose_hot(&fixture);
    damage_both_copies(&fixture);
    let manifest = cold(&fixture, "cold-a").join(format!(
        "{}.manifest",
        path.strip_suffix(".parquet").unwrap()
    ));
    let text = fs::read_to_string(&manifest).unwrap();
    let rows = text
        .lines()
        .find(|l| l.starts_with("rows "))
        .unwrap()
        .to_owned();
    let other = text.replacen(&rows, &format!("{rows}0"), 1);
    let mut permissions = fs::metadata(&manifest).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&manifest, permissions).unwrap();
    fs::write(&manifest, other).unwrap();
    let (pass, out) = restore(&fixture, Flags::default(), RESTORED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&path, "different manifest texts"]);
    assert!(!fixture.config.paths.journal.exists());
    assert!(!copy_ledger::ledger_path(&fixture.hot()).exists());
}
