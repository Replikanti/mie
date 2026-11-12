//! `mie store resolve --replace` offline (ADR-062 D5, D7 step 5a, D11,
//! proposed; #211): the replace into quarantine on every root, the
//! re-import with new part numbers, its `replaced` records, the stops
//! between steps, the rebuild's quarantine part and AW12. The fixtures are
//! those of `mie store resolve` offline (`common::resolve`).

mod common;

use common::resolve::*;
use common::tiers::*;
use common::{HOUR, MapProbe};
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::Period;
use mie_adapter_binance::archive::ledger::{LedgerDir, Pending, Quarantine, SupersedeRecord};
use mie_adapter_parquet::copy_ledger::{self, Kind};
use mie_adapter_parquet::{replica, root_marker};
use mie_cli::store::{self, rebuild::Flags};
use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The quarantine id of a replace at [`RESOLVED`].
fn id() -> String {
    format!("q-{RESOLVED}")
}

/// `mie store resolve --file F --replace` against an archive that serves
/// nothing: a request would fail the run.
fn replace_with(
    fixture: &Fixture,
    file: Option<&str>,
    probe: &MapProbe,
    now: i64,
) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::resolve(
        &fixture.config,
        Some(&fixture.dst_archive),
        file,
        true,
        &transports(&Arc::new(FakeArchive::default())),
        probe,
        &|| now,
        &mut out,
    )
    .unwrap();
    (pass, String::from_utf8(out).unwrap())
}

fn replace(fixture: &Fixture, now: i64) -> (bool, String) {
    replace_with(fixture, Some(&metrics_file()), &fixture.probe(), now)
}

/// The roots: hot, cold-a and cold-b, by id.
fn roots(fixture: &Fixture) -> [(&'static str, PathBuf); 3] {
    [
        ("hot", fixture.hot()),
        ("cold-a", cold(fixture, "cold-a")),
        ("cold-b", cold(fixture, "cold-b")),
    ]
}

fn manifest_of(path: &str) -> String {
    format!("{}.manifest", path.strip_suffix(".parquet").unwrap())
}

/// The data and manifest bytes of `parts` on every root, below `base`
/// (the layout, or a quarantine directory), sorted.
fn bytes_of(fixture: &Fixture, parts: &[String], base: &dyn Fn(&Path) -> PathBuf) -> Vec<Vec<u8>> {
    let mut found = Vec::new();
    for (_, root) in roots(fixture) {
        for part in parts {
            for file in [part.clone(), manifest_of(part)] {
                if let Ok(bytes) = fs::read(base(&root).join(file)) {
                    found.push(bytes);
                }
            }
        }
    }
    found.sort();
    found
}

/// The copy-ledger records of hot.
fn records(fixture: &Fixture) -> Vec<Kind> {
    copy_ledger::read(&fixture.hot())
        .unwrap()
        .unwrap()
        .records
        .into_iter()
        .map(|r| r.kind)
        .collect()
}

fn quarantined(fixture: &Fixture) -> BTreeSet<String> {
    records(fixture)
        .into_iter()
        .filter_map(|kind| match kind {
            Kind::Quarantined { path, .. } => Some(path),
            _ => None,
        })
        .collect()
}

fn replaced(fixture: &Fixture) -> Vec<(String, String)> {
    records(fixture)
        .into_iter()
        .filter_map(|kind| match kind {
            Kind::Replaced { path, entry } => Some((path, entry)),
            _ => None,
        })
        .collect()
}

/// The archive parts of 2026-10-03's metrics file on hot that are not in
/// `before`.
fn new_parts(fixture: &Fixture, before: &[String]) -> Vec<String> {
    replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .filter(|path| path.contains("stream=metrics") && !before.contains(path))
        .collect()
}

fn part_number(path: &str) -> u32 {
    path.rsplit_once("part-")
        .and_then(|(_, n)| n.strip_suffix(".parquet"))
        .unwrap()
        .parse()
        .unwrap()
}

fn chmod(path: &Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

/// Whether permissions bind this process (they do not for root).
fn permissions_bind(dir: &Path) -> bool {
    let probe = dir.join("perm-probe");
    fs::create_dir(&probe).unwrap();
    chmod(&probe, 0o000);
    let binds = fs::read_dir(&probe).is_err();
    chmod(&probe, 0o755);
    fs::remove_dir(&probe).unwrap();
    binds
}

/// Cuts both ledger copies right after their last `hold` record.
fn cut_after_holds(fixture: &Fixture) {
    for name in ["cold-a", "cold-b"] {
        let path = copy_ledger::ledger_path(&cold(fixture, name));
        let bytes = fs::read(&path).unwrap();
        let ledger = copy_ledger::parse(&path, &bytes).unwrap();
        let last = ledger
            .records
            .iter()
            .rposition(|r| matches!(r.kind, Kind::Hold { .. }))
            .unwrap();
        fs::write(&path, &bytes[..ledger.ends[last] as usize]).unwrap();
    }
}

/// Removes hot's ledger, prepares the start and rebuilds.
fn rebuild_from(fixture: &Fixture, start: &str, flags: Flags, now: i64) -> (bool, String) {
    fs::remove_file(hot_ledger(fixture)).unwrap();
    match start {
        "stale" => cut_after_holds(fixture),
        "empty" => damage_both_copies(fixture),
        _ => {}
    }
    let (pass, out) = rebuild(fixture, flags, now);
    assert!(out.contains("wrote "), "{start}: {out}");
    (pass, out)
}

/// Publishes 2026-10-03's metrics again with hour 7 changed.
fn republish(fixture: &Fixture) {
    let day = day_index("2026-10-03");
    let changed = metrics_csv(day).replace(
        "2026-10-03 07:00:00,BTCUSDT,90007.1230000000000000",
        "2026-10-03 07:00:00,BTCUSDT,90007.9990000000000000",
    );
    fixture
        .archive
        .publish(ArchiveStream::Metrics, day, &changed);
}

fn day() -> i64 {
    day_index("2026-10-03")
}

/// Overwrites a read-only file.
fn overwrite(path: &Path, bytes: &[u8]) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions).unwrap();
    fs::write(path, bytes).unwrap();
}

/// A replace of the conflicting entry (2026-10-03 republished with hour 7
/// changed): the re-admit refuses with the cost and the command; the
/// replace moves every copy into quarantine on every root, deleting
/// nothing; `select` fails until the re-import, which writes `F.1.import`
/// with new part numbers and one `replaced` record per moved path.
#[test]
fn replace_quarantines_on_every_root_and_reimports_with_new_part_numbers() {
    let (_dir, fixture) = adopted("replace-every-root");
    let (_, parts) = conflict(&fixture);
    republish(&fixture);
    let layout_bytes = bytes_of(&fixture, &parts, &|root| root.to_owned());
    assert_eq!(
        layout_bytes.len(),
        12,
        "two parts, data and manifest, three roots"
    );

    // The re-admit: a difference, the cost and the command, nothing written.
    let before = files(&fixture.dir);
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
    assert!(!pass, "{out}");
    for part in &parts {
        assert!(
            out.lines()
                .any(|l| l.starts_with(&format!("cost: {part}: stream ")) && l.contains("changes")),
            "{out}"
        );
    }
    assert!(out.contains("cost: 2 moved path(s)"), "{out}");
    assert_fail_line(&out, &["nothing written", "--replace", &metrics_file()]);
    assert_eq!(files(&fixture.dir), before);

    // The replace.
    let (pass, out) = replace(&fixture, RESOLVED);
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    let s1 = format!("{}.1.supersede.import", metrics_name());
    let record =
        SupersedeRecord::parse(&fs::read_to_string(ledger_file(&fixture, &s1)).unwrap()).unwrap();
    assert_eq!(
        record.quarantine,
        Some(Quarantine {
            id: id(),
            moved: parts.clone()
        })
    );
    for (_, root) in roots(&fixture) {
        for part in &parts {
            assert!(!root.join(part).exists() && !root.join(manifest_of(part)).exists());
        }
    }
    let quarantine_bytes = bytes_of(&fixture, &parts, &|root| {
        replica::quarantine_dir(root, &id())
    });
    assert_eq!(
        quarantine_bytes, layout_bytes,
        "nothing deleted, nothing changed"
    );
    for part in &parts {
        assert!(
            out.contains(&format!("quarantined {part} {}", id())),
            "{out}"
        );
        assert!(
            out.contains(&format!("hold-cleared {part} replace")),
            "{out}"
        );
    }
    assert!(held(&fixture).is_empty());
    let failing = selected(&fixture);
    assert!(
        failing
            .iter()
            .any(|s| s.contains(&format!("quarantined under {}", id()))),
        "{failing:?}"
    );

    // The re-import: F.1.import, new part numbers, the `replaced` records.
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    let out = run_import(&fixture, &metrics_day(day(), false)).unwrap();
    let e1 = format!("{}.1.import", metrics_name());
    assert!(ledger_file(&fixture, &e1).exists(), "{out}");
    let fresh = new_parts(&fixture, &before);
    assert_eq!(fresh.len(), 2, "{fresh:?}");
    for (old, new) in parts.iter().zip(&fresh) {
        assert_eq!(partition_of(old), partition_of(new));
        assert_eq!(part_number(new), part_number(old) + 1, "{old} {new}");
    }
    assert_eq!(
        replaced(&fixture),
        parts
            .iter()
            .map(|p| (p.clone(), format!("metrics/{e1}")))
            .collect::<Vec<_>>()
    );
    let after = selected(&fixture);
    assert!(after.iter().all(|s| !s.starts_with("error")), "{after:?}");
    let all = after.join("\n");
    for part in &parts {
        assert!(!all.contains(part.as_str()), "{all}");
    }
    for part in &fresh {
        assert!(all.contains(part.as_str()), "{all}");
    }
}

/// S_n names every variant of the latest entry: a conflicting E0 gives
/// two `entry` lines.
#[test]
fn replace_supersedes_both_variants_of_a_conflicting_entry() {
    let (_dir, fixture) = adopted("replace-both-variants");
    conflict(&fixture);
    assert_pass(&replace(&fixture, RESOLVED));
    let s1 = format!("{}.1.supersede.import", metrics_name());
    let record =
        SupersedeRecord::parse(&fs::read_to_string(ledger_file(&fixture, &s1)).unwrap()).unwrap();
    assert_eq!(record.entries.len(), 2);
    assert!(
        record
            .entries
            .iter()
            .all(|e| e.name == format!("metrics/{}.import", metrics_name()))
    );
}

/// A `manifest-conflict` part (cold-a holds another self-consistent file
/// under P1) and an ordinary sibling P2: re-admit refuses; the replace
/// moves each root's own copy of P1 and P2, writes no second `manifest`
/// text, and the re-import replaces both.
#[test]
fn a_manifest_conflict_replace_moves_the_whole_generation() {
    let (_dir, fixture) = adopted("replace-manifest-conflict");
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    run_import(&fixture, &metrics_day(day(), false)).unwrap();
    let parts: Vec<String> = new_parts(&fixture, &before)
        .into_iter()
        .filter(|path| !cold(&fixture, "cold-a").join(path).exists())
        .collect();
    assert_eq!(parts.len(), 2, "{parts:?}");
    replicate(&fixture, D0_PLUS_2H);
    // cold-a's P1: another sealed file under the same path.
    let a = cold(&fixture, "cold-a");
    let other = b"another sealed file".to_vec();
    let text = fs::read_to_string(a.join(manifest_of(&parts[0]))).unwrap();
    let sha = text
        .lines()
        .find_map(|l| l.strip_prefix("sha256 "))
        .unwrap();
    let bytes_line = text.lines().find(|l| l.starts_with("bytes ")).unwrap();
    let other_sha = sha2_hex(&other);
    let other_text = text
        .replace(sha, &other_sha)
        .replace(bytes_line, &format!("bytes {}", other.len()));
    overwrite(&a.join(&parts[0]), &other);
    overwrite(&a.join(manifest_of(&parts[0])), other_text.as_bytes());
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    rebuild(&fixture, Flags::default(), REBUILT);
    assert_eq!(held(&fixture), BTreeSet::from([parts[0].clone()]));
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["manifest-conflict", "--replace"]);

    let (pass, out) = replace(&fixture, RESOLVED);
    assert!(pass, "{out}");
    let q = |root: &Path, file: &str| replica::quarantine_dir(root, &id()).join(file);
    assert_eq!(fs::read(q(&a, &parts[0])).unwrap(), other);
    assert_eq!(
        fs::read_to_string(q(&a, &manifest_of(&parts[0]))).unwrap(),
        other_text
    );
    for (_, root) in roots(&fixture) {
        for part in &parts {
            assert!(q(&root, part).exists(), "{part} on {}", root.display());
        }
    }
    let manifests = records(&fixture)
        .into_iter()
        .filter(|kind| matches!(kind, Kind::Manifest { text } if text.contains(&parts[0])))
        .count();
    assert_eq!(manifests, 1, "no second manifest text");
    run_import(&fixture, &metrics_day(day(), false)).unwrap();
    let replaced: BTreeSet<String> = replaced(&fixture).into_iter().map(|(p, _)| p).collect();
    assert_eq!(replaced, parts.iter().cloned().collect());
    assert!(selected(&fixture).iter().all(|s| !s.starts_with("error")));
}

const D0_PLUS_2H: i64 = common::D0 + 2 * HOUR;

fn sha2_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A `warm_start` journal line whose prefix is `[origin, cutover)`.
fn warm_line(run: &str, origin: i64, cutover: i64, dataset: char) -> String {
    format!(
        "{{\"type\":\"warm_start\",\"run_id\":\"{run}\",\"at_ms\":1,\"warm\":true,\
         \"origin_ms\":{origin},\"effective_origin_ms\":null,\"cutover_ms\":{cutover},\
         \"prefix_dataset\":\"{}\",\"prefix_last_event_ms\":null,\"prefix_events\":0,\
         \"prefix_event_hash\":\"0000000000000000\",\"prefix_rejections\":0,\
         \"handoff_state_hash\":\"0000000000000000\",\"seeds\":{{}},\"queue_high_water\":0,\
         \"queue_high_water_bytes\":0,\"warming_up\":[]}}\n",
        dataset.to_string().repeat(64)
    )
}

/// RQ5: of two journaled warm runs, only the one whose prefix covers a
/// moved part is named, with its `prefix_dataset`; a cold run and a
/// non-JSON line name nothing.
#[test]
fn the_replace_cost_names_every_covering_warm_run() {
    let (_dir, fixture) = adopted("replace-cost-runs");
    missing_entry(&fixture);
    let d3 = day() * 86_400_000;
    let journal = &fixture.config.paths.journal;
    let mut text = fs::read_to_string(journal).unwrap_or_default();
    text.push_str(&warm_line(
        "run-covers",
        d3 - 86_400_000,
        d3 + 3 * HOUR,
        'a',
    ));
    text.push_str(&warm_line(
        "run-before",
        d3 - 2 * 86_400_000,
        d3 - 86_400_000,
        'b',
    ));
    text.push_str("not json\n");
    text.push_str(
        "{\"type\":\"warm_start\",\"run_id\":\"run-cold\",\"warm\":false,\"cold\":\"no-history\",\"error\":null}\n",
    );
    fs::write(journal, text).unwrap();
    let (pass, out) = replace(&fixture, RESOLVED);
    assert!(pass, "{out}");
    let runs: Vec<&str> = out
        .lines()
        .filter(|l| l.starts_with("cost: warm run "))
        .collect();
    assert_eq!(runs.len(), 1, "{out}");
    assert!(
        runs[0].contains("run-covers") && runs[0].contains(&"a".repeat(64)),
        "{out}"
    );
    assert!(
        out.contains("cost: 2 moved path(s), 1 journaled warm run(s)"),
        "{out}"
    );
}

/// RQ6: cold-a's 2026-10-03 partition is read-only, so the first rename
/// fails: S1 and the `quarantined` records exist, nothing has moved,
/// `select` fails naming P1 and the importer refuses.
#[test]
fn the_supersede_record_and_records_precede_every_rename() {
    let (_dir, fixture) = adopted("replace-first-rename");
    if !permissions_bind(&fixture.dir) {
        return;
    }
    let (_, parts) = conflict(&fixture);
    let partition = cold(&fixture, "cold-a").join(partition_of(&parts[0]));
    let layout_before = bytes_of(&fixture, &parts, &|root| root.to_owned());
    chmod(&partition, 0o555);
    let (pass, out) = replace(&fixture, RESOLVED);
    chmod(&partition, 0o755);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&parts[0], "cold-a", "completes the replace"]);
    assert!(ledger_file(&fixture, &format!("{}.1.supersede.import", metrics_name())).exists());
    assert_eq!(quarantined(&fixture), parts.iter().cloned().collect());
    assert_eq!(
        bytes_of(&fixture, &parts, &|root| root.to_owned()),
        layout_before
    );
    assert!(
        selected(&fixture)
            .iter()
            .any(|s| s.starts_with("error") && s.contains(&parts[0]))
    );
    assert!(run_import(&fixture, &metrics_day(day(), false)).is_err());
}

/// RQ6, RQ7, Q4, Q5: cold-b's partition is read-only, so cold-a is
/// renamed and hot is untouched. Each rebuild start derives `quarantined`,
/// no lost candidate and no `unlisted` line, and names the stop; the
/// importer refuses after each. With the permission restored, `resolve
/// --file F` without `--replace` completes it and the re-import runs.
#[test]
fn a_replace_stopped_between_renames_reads_as_quarantined() {
    for start in ["full", "stale", "empty"] {
        let (_dir, fixture) = adopted(&format!("replace-stopped-{start}"));
        if !permissions_bind(&fixture.dir) {
            return;
        }
        let (_, parts) = conflict(&fixture);
        let partition = cold(&fixture, "cold-b").join(partition_of(&parts[0]));
        chmod(&partition, 0o555);
        let (pass, out) = replace(&fixture, RESOLVED);
        assert!(!pass, "{out}");
        let a = cold(&fixture, "cold-a");
        for part in &parts {
            assert!(
                replica::quarantine_dir(&a, &id()).join(part).exists(),
                "{out}"
            );
            assert!(fixture.hot().join(part).exists(), "{out}");
        }
        // W4 comes last: the holds are still open.
        assert_eq!(held(&fixture), parts.iter().cloned().collect(), "{out}");
        replicate(&fixture, RESOLVED + 1);
        let (_, out) = rebuild_from(&fixture, start, Flags::default(), RESOLVED + HOUR);
        assert_eq!(
            quarantined(&fixture),
            parts.iter().cloned().collect(),
            "{start}: {out}"
        );
        assert!(!out.contains("lost candidate"), "{start}: {out}");
        assert!(!out.contains("unlisted"), "{start}: {out}");
        assert_fail_line(&out, &["stopped between renames", &metrics_file()]);
        let error = run_import(&fixture, &metrics_day(day(), false)).unwrap_err();
        assert!(
            error.contains("held") || error.contains("mie store resolve"),
            "{start}: {error}"
        );
        if start == "empty" {
            assert!(error.contains("incomplete"), "{error}");
        }
        chmod(&partition, 0o755);
        let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED + 2 * HOUR);
        assert!(pass, "{start}: {out}");
        assert!(out.contains("resume:"), "{out}");
        for (_, root) in roots(&fixture) {
            for part in &parts {
                assert!(!root.join(part).exists(), "{start}: {part}");
            }
        }
        // A second resume finds nothing to do.
        let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED + 3 * HOUR);
        assert!(
            pass && out.contains("is complete; the re-import is pending"),
            "{out}"
        );
        run_import(&fixture, &metrics_day(day(), false)).unwrap();
        assert_eq!(replaced(&fixture).len(), 2, "{start}");
    }
}

/// RQ0: `--replace` needs `--file`.
#[test]
fn replace_needs_a_file() {
    let (_dir, fixture) = adopted("replace-needs-file");
    missing_entry(&fixture);
    let before = files(&fixture.dir);
    let (pass, out) = replace_with(&fixture, None, &fixture.probe(), RESOLVED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["--replace needs --file"]);
    assert_eq!(files(&fixture.dir), before);
}

/// RQ1, RQ2: every refusal writes nothing.
#[test]
fn replace_refusals_write_nothing() {
    let (_dir, fixture) = adopted("replace-refusals");
    missing_entry(&fixture);
    let refused = |needles: &[&str], prepare: &dyn Fn(), undo: &dyn Fn()| {
        prepare();
        let before = files(&fixture.dir);
        let (pass, out) = replace(&fixture, RESOLVED);
        assert!(!pass, "{out}");
        assert_fail_line(&out, needles);
        assert_eq!(files(&fixture.dir), before, "{out}");
        undo();
    };
    // A cold root without its marker.
    let marker = root_marker::marker_path(&cold(&fixture, "cold-b"));
    let aside = fixture.dir.join("marker-b");
    refused(
        &["cold-b", "no root marker"],
        &|| fs::rename(&marker, &aside).unwrap(),
        &|| fs::rename(&aside, &marker).unwrap(),
    );
    // `.quarantine` as a symlink on hot, as a file on cold-a.
    let link = fixture.hot().join(replica::QUARANTINE_DIR);
    refused(
        &["no directory"],
        &|| {
            fs::create_dir_all(fixture.dir.join("elsewhere")).unwrap();
            std::os::unix::fs::symlink(fixture.dir.join("elsewhere"), &link).unwrap();
        },
        &|| fs::remove_file(&link).unwrap(),
    );
    let file = cold(&fixture, "cold-a").join(replica::QUARANTINE_DIR);
    refused(
        &["no directory"],
        &|| fs::write(&file, b"").unwrap(),
        &|| fs::remove_file(&file).unwrap(),
    );
    // The pass lock held.
    let lock = std::cell::RefCell::new(None);
    refused(
        &["pass lock"],
        &|| *lock.borrow_mut() = Some(replica::try_replication_lock(&fixture.hot()).unwrap()),
        &|| *lock.borrow_mut() = None,
    );
    // The import PENDING names F.
    let ledgers = LedgerDir::new(&fixture.archive_config.paths.import_ledger);
    refused(
        &["PENDING"],
        &|| {
            ledgers
                .set_pending(&Pending {
                    archive: metrics_file(),
                    sha256: "a".repeat(64),
                    stream: mie_ports::raw::RawStreamKey::new("binance-archive", SYMBOL, "metrics")
                        .unwrap(),
                    period: Period::Day(day()),
                })
                .unwrap();
        },
        &|| ledgers.clear_pending().unwrap(),
    );
    // Nothing changed: the replace then passes.
    assert_pass(&replace(&fixture, RESOLVED));
}

/// RQ2: a held part that no root holds (its copies gone everywhere, its
/// manifest only in the copy ledger) refuses the replace: there is nothing
/// to quarantine.
#[test]
fn a_held_part_no_root_holds_is_refused() {
    let (_dir, fixture) = adopted("replace-part-gone");
    let (_, parts) = conflict(&fixture);
    for (_, root) in roots(&fixture) {
        for file in [parts[0].clone(), manifest_of(&parts[0])] {
            fs::remove_file(root.join(file)).unwrap();
        }
    }
    let before = files(&fixture.dir);
    let (pass, out) = replace(&fixture, RESOLVED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&parts[0], "no root holds it"]);
    assert_eq!(files(&fixture.dir), before);
}

/// RQ1: a read-only root that holds a moved path refuses the replace: D4
/// never writes a read-only root.
#[test]
fn a_read_only_root_holding_a_moved_path_is_refused() {
    let (_dir, fixture) = adopted("replace-read-only");
    let (_, parts, _) = missing_entry(&fixture);
    let c = fixture.dir.join("cold-c");
    fs::create_dir_all(&c).unwrap();
    root_marker::create_meta(&c, "cold-c", false).unwrap();
    for file in [parts[0].clone(), manifest_of(&parts[0])] {
        let to = c.join(&file);
        fs::create_dir_all(to.parent().unwrap()).unwrap();
        fs::copy(fixture.hot().join(&file), to).unwrap();
    }
    let mut cold_roots = cold_roots(&fixture.dir);
    cold_roots.push(mie_cli::config::ColdRoot {
        path: c.clone(),
        copy_target: false,
    });
    let with_c = fixture_with(&fixture, &cold_roots);
    let before = files(&fixture.dir);
    let (pass, out) = replace(&with_c, RESOLVED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&parts[0], "read-only root cold-c"]);
    assert_eq!(files(&fixture.dir), before);
}

/// The fixture with another `cold_roots` list in both configurations.
fn fixture_with(fixture: &Fixture, cold_roots: &[mie_cli::config::ColdRoot]) -> Fixture {
    let dst = fixture.dir.join("dst");
    let config = ingest_toml(
        &dst.join("ingest-c.toml"),
        &fixture.hot(),
        &fixture.config.paths.journal,
        cold_roots,
    );
    let archive_config = archive_toml(
        &dst.join("archive-c.toml"),
        &dst,
        &fixture.hot(),
        cold_roots,
    );
    Fixture {
        dir: fixture.dir.clone(),
        archive: fixture.archive.clone(),
        src_ingest: fixture.src_ingest.clone(),
        src_archive: fixture.src_archive.clone(),
        dst_ingest: dst.join("ingest-c.toml"),
        dst_archive: dst.join("archive-c.toml"),
        config,
        archive_config,
    }
}

/// RQ3: an id another record names, or a directory that exists, is
/// refused with nothing written.
#[test]
fn a_quarantine_id_in_use_is_refused() {
    let (_dir, fixture) = adopted("replace-id-in-use");
    missing_entry(&fixture);
    let dir = replica::quarantine_dir(&cold(&fixture, "cold-b"), &id());
    fs::create_dir_all(&dir).unwrap();
    let before = files(&fixture.dir);
    let (pass, out) = replace(&fixture, RESOLVED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&id(), "in use"]);
    assert_eq!(files(&fixture.dir), before);
    fs::remove_dir_all(cold(&fixture, "cold-b").join(replica::QUARANTINE_DIR)).unwrap();

    // A record of another archive file on cold-a names the id.
    let other_day = day_index("2026-10-02");
    let other = SupersedeRecord {
        archive: ArchiveStream::Metrics.archive_path(SYMBOL, Period::Day(other_day)),
        stream: mie_ports::raw::RawStreamKey::new("binance-archive", SYMBOL, "metrics").unwrap(),
        period: Period::Day(other_day),
        entries: Vec::new(),
        quarantine: Some(Quarantine {
            id: id(),
            moved: vec![
                "source=binance-archive/instrument=BTCUSDT/stream=metrics/date=2026-10-02/part-00099.parquet"
                    .to_owned(),
            ],
        }),
    };
    let name = format!(
        "{}.1.supersede.import",
        ArchiveStream::Metrics.file_name(SYMBOL, Period::Day(other_day))
    );
    let path = target_file(&fixture, "cold-a", &name);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, other.render()).unwrap();
    let before = files(&fixture.dir);
    let (pass, out) = replace(&fixture, RESOLVED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&id(), "in use", &name]);
    assert_eq!(files(&fixture.dir), before);
    // Another millisecond is another id.
    assert_pass(&replace(&fixture, RESOLVED + 1));
}

/// I1: a moved path back in hot's layout halts the importer, its dry run
/// too, naming the file and `mie store resolve`; RQ8 names it as well.
#[test]
fn the_importer_refuses_while_a_replace_is_incomplete() {
    let (_dir, fixture) = adopted("replace-importer-refuses");
    let (_, parts, _) = missing_entry(&fixture);
    assert_pass(&replace(&fixture, RESOLVED));
    let q = replica::quarantine_dir(&fixture.hot(), &id());
    for file in [parts[0].clone(), manifest_of(&parts[0])] {
        fs::copy(q.join(&file), fixture.hot().join(&file)).unwrap();
    }
    for dry_run in [true, false] {
        let error = run_import(&fixture, &metrics_day(day(), dry_run)).unwrap_err();
        assert!(
            error.contains("incomplete")
                && error.contains(&metrics_file())
                && error.contains("mie store resolve"),
            "{error}"
        );
    }
    let (pass, out) = resolve(&fixture, None, RESOLVED + 1);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["incomplete", &format!("--file {}", metrics_file())]);
}

/// I3: an import that wrote its entry and stopped before the `replaced`
/// records (hot's ledger cut before them) is completed by the next import
/// run; a part the entry lists that is not sealed on hot holds them back.
#[test]
fn the_next_import_completes_the_replaced_records() {
    let (_dir, fixture) = adopted("replace-next-import");
    let (_, parts, _) = missing_entry(&fixture);
    assert_pass(&replace(&fixture, RESOLVED));
    // Proposals name the complete replace awaiting its import.
    let (pass, out) = resolve(&fixture, None, RESOLVED + 1);
    assert!(pass && out.contains("the re-import is pending"), "{out}");
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    run_import(&fixture, &metrics_day(day(), false)).unwrap();
    assert_eq!(replaced(&fixture).len(), 2);
    let fresh = new_parts(&fixture, &before);
    // The crash: hot's ledger without the `replaced` records.
    let path = hot_ledger(&fixture);
    let bytes = fs::read(&path).unwrap();
    let ledger = copy_ledger::parse(&path, &bytes).unwrap();
    let first = ledger
        .records
        .iter()
        .position(|r| matches!(r.kind, Kind::Replaced { .. }))
        .unwrap();
    let cut = if first == 0 {
        copy_ledger::HEADER.len()
    } else {
        ledger.ends[first - 1] as usize
    };
    fs::write(&path, &bytes[..cut]).unwrap();
    assert!(replaced(&fixture).is_empty());
    let ledgers = LedgerDir::new(&fixture.archive_config.paths.import_ledger);

    // A listed part missing on hot: nothing appended, the part named.
    let manifest = fixture.hot().join(manifest_of(&fresh[0]));
    let aside = fixture.dir.join("aside.manifest");
    fs::rename(&manifest, &aside).unwrap();
    let roots =
        store::quarantine::ReplaceRoots::read(&fixture.hot(), &fixture.config.paths.cold_roots);
    let lines =
        store::quarantine::record_replacements(&fixture.hot(), &roots, &ledgers, &|| RESOLVED + 2)
            .unwrap();
    assert!(
        lines
            .iter()
            .any(|l| l.contains("replacement waits") && l.contains(&fresh[0])),
        "{lines:?}"
    );
    assert!(replaced(&fixture).is_empty());
    fs::rename(&aside, &manifest).unwrap();

    // The next import run appends them.
    let out = run_import(&fixture, &metrics_day(day(), false)).unwrap();
    for part in &parts {
        assert!(out.contains(&format!("replaced {part} ")), "{out}");
    }
    assert_eq!(replaced(&fixture).len(), 2);
    // A dry run appends nothing.
    fs::write(&path, &bytes[..cut]).unwrap();
    run_import(&fixture, &metrics_day(day(), true)).unwrap();
    assert!(replaced(&fixture).is_empty());
}

/// AW12: after a replace, the copy ledger is rebuilt from a full, a stale
/// and an empty start: each `select` equals the post-replace result
/// (`Integrity` naming the quarantined paths); after the re-import and its
/// pass, the three rebuilds give the new parts with the post-import dataset
/// versions. No rebuild names a lost file or lists a quarantined part, and
/// the three starts agree.
#[test]
fn aw12_rebuilds_after_a_replace_agree() {
    let mut expected: Option<(Vec<String>, Vec<String>)> = None;
    for start in ["full", "stale", "empty"] {
        let (_dir, fixture) = adopted(&format!("replace-aw12-{start}"));
        let (_, parts, _) = missing_entry(&fixture);
        // Phase A.
        assert_pass(&replace(&fixture, RESOLVED));
        replicate(&fixture, RESOLVED + 1);
        let after_replace = selected(&fixture);
        for (part, date) in parts.iter().zip(["2026-10-03", "2026-10-04"]) {
            assert!(date_of(part) == date);
            assert!(
                after_replace
                    .iter()
                    .any(|s| s.contains(&format!("{part}: it is quarantined under {}", id()))),
                "{after_replace:?}"
            );
        }
        let (_, out) = rebuild_from(&fixture, start, Flags::default(), RESOLVED + HOUR);
        assert!(
            !out.contains("lost candidate") && out.contains(", lost 0,"),
            "{start}: {out}"
        );
        assert_eq!(selected(&fixture), after_replace, "{start} A: {out}");
        assert!(held(&fixture).is_empty(), "{start}: {out}");

        // Phase B.
        let before = replica::list_sealed(&fixture.hot()).unwrap();
        run_import(&fixture, &metrics_day(day(), false)).unwrap();
        let fresh = new_parts(&fixture, &before);
        assert_eq!(fresh.len(), 2, "{start}");
        replicate(&fixture, RESOLVED + 2 * HOUR);
        let after_import = selected(&fixture);
        let (_, out) = rebuild_from(&fixture, start, Flags::default(), RESOLVED + 3 * HOUR);
        assert!(
            !out.contains("lost candidate") && out.contains(", lost 0,"),
            "{start}: {out}"
        );
        let rebuilt = selected(&fixture);
        assert_eq!(rebuilt, after_import, "{start} B: {out}");
        for dataset in rebuilt.iter().filter(|s| !s.starts_with("error")) {
            for part in &parts {
                assert!(!dataset.contains(part.as_str()), "{start}: {dataset}");
            }
        }
        assert!(
            rebuilt.iter().all(|s| !s.starts_with("error")),
            "{rebuilt:?}"
        );
        let outcome = (after_replace, rebuilt);
        match &expected {
            None => expected = Some(outcome),
            Some(expected) => assert_eq!(&outcome, expected, "{start}"),
        }
    }
}

/// AW12: unresolved holds (`unlisted`, `entry-conflict`) stay held in a
/// rebuild from a full, a stale and an empty start, never ordinary files,
/// and every selection covering them fails.
#[test]
fn aw12_an_unresolved_hold_stays_held_in_every_rebuild() {
    for case in ["unlisted", "entry-conflict"] {
        for start in ["full", "stale", "empty"] {
            let (_dir, fixture) = adopted(&format!("replace-aw12-held-{case}-{start}"));
            let parts = match case {
                "unlisted" => missing_entry(&fixture).1,
                _ => conflict(&fixture).1,
            };
            replicate(&fixture, RESOLVED);
            let flags = Flags {
                record_unlisted: case == "unlisted",
                ..Flags::default()
            };
            fs::remove_file(hot_ledger(&fixture)).unwrap();
            match start {
                "stale" => {
                    // Cut before the first hold.
                    for name in ["cold-a", "cold-b"] {
                        let path = copy_ledger::ledger_path(&cold(&fixture, name));
                        let bytes = fs::read(&path).unwrap();
                        let ledger = copy_ledger::parse(&path, &bytes).unwrap();
                        let first = ledger
                            .records
                            .iter()
                            .position(|r| matches!(r.kind, Kind::Hold { .. }))
                            .unwrap();
                        fs::write(&path, &bytes[..ledger.ends[first - 1] as usize]).unwrap();
                    }
                }
                "empty" => damage_both_copies(&fixture),
                _ => {}
            }
            let (_, out) = rebuild(&fixture, flags, RESOLVED + HOUR);
            assert_eq!(
                held(&fixture),
                parts.iter().cloned().collect(),
                "{case} {start}: {out}"
            );
            assert!(quarantined(&fixture).is_empty());
            let all = selected(&fixture);
            for part in &parts {
                assert!(
                    all.iter()
                        .filter(|s| !s.starts_with("error"))
                        .all(|s| !s.contains(part.as_str())),
                    "{case} {start}"
                );
            }
        }
    }
}

/// AW12: a re-import after an empty-start rebuild numbers its parts above
/// the quarantined ones, which only the rebuilt `quarantined` records name.
#[test]
fn aw12_reimport_after_an_empty_start_rebuild_numbers_above_the_quarantine() {
    let (_dir, fixture) = adopted("replace-aw12-numbering");
    let (_, parts, _) = missing_entry(&fixture);
    assert_pass(&replace(&fixture, RESOLVED));
    replicate(&fixture, RESOLVED + 1);
    rebuild_from(&fixture, "empty", Flags::default(), RESOLVED + HOUR);
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    run_import(&fixture, &metrics_day(day(), false)).unwrap();
    let fresh = new_parts(&fixture, &before);
    assert_eq!(fresh.len(), 2);
    for (old, new) in parts.iter().zip(&fresh) {
        assert_eq!(partition_of(old), partition_of(new));
        assert!(part_number(new) > part_number(old), "{old} {new}");
    }
}

/// Review of #215, finding 1: the moved parts live on the cold roots only
/// (hot's partitions removed, D6); the replace stops between cold-a and
/// cold-b; a rebuild from an empty start has no hold. The importer still
/// refuses, its dry run too, because cold-b's layout holds the moved
/// parts (I1 checks every root); proposals name the stop; `resolve --file
/// F` completes it; the import then runs, and a full and an empty rebuild
/// start agree (AW12). A part put back into a layout after its
/// replacement halts the importer again and the resume moves it.
#[test]
fn a_replace_stopped_between_cold_roots_keeps_the_importer_halted() {
    let (_dir, fixture) = adopted("replace-cold-only");
    if !permissions_bind(&fixture.dir) {
        return;
    }
    let (_, parts) = conflict(&fixture);
    // The D6 hot removal of both partitions, simulated.
    let aside = fixture.dir.join("hot-aside");
    for part in &parts {
        let from = fixture.hot().join(partition_of(part));
        let to = aside.join(partition_of(part));
        fs::create_dir_all(to.parent().unwrap()).unwrap();
        fs::rename(&from, &to).unwrap();
    }
    replicate(&fixture, RESOLVED - 10);
    rebuild_from(&fixture, "full", Flags::default(), RESOLVED - 5);
    assert_eq!(held(&fixture), parts.iter().cloned().collect());

    // The replace stops between cold-a and cold-b.
    let b = cold(&fixture, "cold-b");
    let partition = b.join(partition_of(&parts[0]));
    chmod(&partition, 0o555);
    let (pass, out) = replace(&fixture, RESOLVED);
    chmod(&partition, 0o755);
    assert!(!pass, "{out}");
    assert!(b.join(&parts[0]).exists());

    // An empty start: no hold, and still no import.
    replicate(&fixture, RESOLVED + 1);
    let (_, out) = rebuild_from(&fixture, "empty", Flags::default(), RESOLVED + HOUR);
    assert!(held(&fixture).is_empty(), "{out}");
    for dry_run in [true, false] {
        let error = run_import(&fixture, &metrics_day(day(), dry_run)).unwrap_err();
        assert!(
            error.contains("incomplete") && error.contains("cold-b's layout"),
            "{error}"
        );
    }
    assert!(replaced(&fixture).is_empty());
    let (pass, out) = resolve(&fixture, None, RESOLVED + HOUR + 1);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["incomplete", "cold-b's layout", &metrics_file()]);

    // The resume completes it; the import runs.
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED + 2 * HOUR);
    assert!(pass && out.contains("resume:"), "{out}");
    for part in &parts {
        assert!(!b.join(part).exists(), "{out}");
        assert!(
            replica::quarantine_dir(&b, &id()).join(part).exists(),
            "{out}"
        );
    }
    run_import(&fixture, &metrics_day(day(), false)).unwrap();
    assert_eq!(replaced(&fixture).len(), 2);

    // AW12: a full and an empty start agree.
    replicate(&fixture, RESOLVED + 3 * HOUR);
    let copies: Vec<(PathBuf, Vec<u8>)> = ["cold-a", "cold-b"]
        .iter()
        .map(|name| {
            let path = copy_ledger::ledger_path(&cold(&fixture, name));
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    let hot_bytes = fs::read(hot_ledger(&fixture)).unwrap();
    let live = selected(&fixture);
    rebuild_from(&fixture, "full", Flags::default(), RESOLVED + 4 * HOUR);
    let after_full = selected(&fixture);
    fs::write(hot_ledger(&fixture), &hot_bytes).unwrap();
    for (path, bytes) in &copies {
        fs::write(path, bytes).unwrap();
    }
    let (_, out) = rebuild_from(&fixture, "empty", Flags::default(), RESOLVED + 4 * HOUR);
    let after_empty = selected(&fixture);
    assert_eq!(after_full, live);
    assert_eq!(after_empty, after_full, "{out}");
    assert!(
        after_empty.iter().all(|s| !s.starts_with("error")),
        "{after_empty:?}"
    );

    // A moved part back in cold-b's layout after its replacement.
    let q = replica::quarantine_dir(&b, &id());
    for file in [parts[0].clone(), manifest_of(&parts[0])] {
        fs::rename(q.join(&file), b.join(&file)).unwrap();
    }
    let error = run_import(&fixture, &metrics_day(day(), true)).unwrap_err();
    assert!(error.contains("cold-b's layout"), "{error}");
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED + 5 * HOUR);
    assert!(pass && out.contains("resume:"), "{out}");
    assert!(
        !b.join(&parts[0]).exists() && q.join(&parts[0]).exists(),
        "{out}"
    );
    run_import(&fixture, &metrics_day(day(), true)).unwrap();
}

/// AW12 (review of #215, test gap): another file's hold left unresolved
/// next to a replace. After the replace of 2026-10-03's metrics, the
/// rebuilds from a full, a stale and an empty start keep 2026-10-02's
/// parts held and the replaced parts quarantined, never ordinary files,
/// and the three starts give the same selections.
#[test]
fn aw12_an_unresolved_hold_beside_a_replace_stays_held_in_every_rebuild() {
    let mut expected: Option<Vec<String>> = None;
    for start in ["full", "stale", "empty"] {
        let (_dir, fixture) = adopted(&format!("replace-aw12-beside-{start}"));
        let (_, parts, _) = missing_entry(&fixture);
        // 2026-10-02's metrics entry gone everywhere: its parts are held
        // `unlisted` and stay so, since nothing resolves them.
        let name = ArchiveStream::Metrics
            .file_name(SYMBOL, Period::Day(day_index("2026-10-02")))
            .replace(".zip", ".zip.import");
        let mut gone = 0;
        for path in [
            ledger_file(&fixture, &name),
            target_file(&fixture, "cold-a", &name),
            target_file(&fixture, "cold-b", &name),
        ] {
            if path.exists() {
                fs::remove_file(path).unwrap();
                gone += 1;
            }
        }
        assert!(gone >= 1);
        let flags = Flags {
            record_unlisted: true,
            ..Flags::default()
        };
        fs::remove_file(hot_ledger(&fixture)).unwrap();
        rebuild(&fixture, flags, REBUILT + 1);
        let other: BTreeSet<String> = held(&fixture)
            .into_iter()
            .filter(|path| !parts.contains(path))
            .collect();
        assert!(!other.is_empty());

        assert_pass(&replace(&fixture, RESOLVED));
        assert_eq!(held(&fixture), other);
        replicate(&fixture, RESOLVED + 1);
        let after_replace = selected(&fixture);
        let (_, out) = rebuild_from(&fixture, start, flags, RESOLVED + HOUR);
        assert_eq!(held(&fixture), other, "{start}: {out}");
        assert_eq!(
            quarantined(&fixture),
            parts.iter().cloned().collect(),
            "{start}: {out}"
        );
        let rebuilt = selected(&fixture);
        assert_eq!(rebuilt, after_replace, "{start}: {out}");
        for dataset in rebuilt.iter().filter(|s| !s.starts_with("error")) {
            for path in parts.iter().chain(&other) {
                assert!(!dataset.contains(path.as_str()), "{start}: {dataset}");
            }
        }
        assert!(run_import(&fixture, &metrics_day(day(), false)).is_err());
        match &expected {
            None => expected = Some(rebuilt),
            Some(expected) => assert_eq!(&rebuilt, expected, "{start}"),
        }
    }
}
