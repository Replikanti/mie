//! `mie store resolve` offline (ADR-062 D11, proposed; #210): the
//! re-admit of a missing and of a conflicting import entry, a difference
//! that writes nothing, rebuilds after a superseding re-admit, an
//! interrupted re-admit, proposals and the refusals. The fixture is the
//! adopted store of `mie store adopt` offline; the archive is the
//! in-process fake every archive offline test uses (the "loopback fetch"
//! of the acceptance).

mod common;

use common::resolve::*;
use common::tiers::*;
use common::{D0, HOUR, MapProbe};
use mie_adapter_binance::archive::ledger::{LedgerDir, SupersedeRecord};
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_parquet::copy_ledger::{self, CopyLedger, HoldReason, Kind, Record, Resolution};
use mie_adapter_parquet::{removal, replica, root_marker};
use mie_cli::store::{self, rebuild::Flags};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

/// Re-admit of a missing entry: proposals, then the entry byte-identical to
/// the importer's, no supersede record, no hold, the same selections and
/// dataset versions; the importer refuses before and runs after.
#[test]
fn readmit_writes_the_missing_entry_and_keeps_the_dataset_version() {
    let (_dir, fixture) = adopted("resolve-missing");
    let (baseline, parts, entry) = missing_entry(&fixture);
    // P1 on 2026-10-03 with 24 rows, P2 on 2026-10-04 with the 23:55 row.
    let rows: Vec<(String, u64)> = parts
        .iter()
        .map(|path| {
            let (_, file) = replica::sealed_manifest(&fixture.hot(), path)
                .unwrap()
                .unwrap();
            (date_of(path).to_owned(), file.rows)
        })
        .collect();
    assert_eq!(
        rows,
        vec![("2026-10-03".to_owned(), 24), ("2026-10-04".to_owned(), 1)]
    );
    let day = day_index("2026-10-03");
    let error = run_import(&fixture, &metrics_day(day, false)).unwrap_err();
    assert!(error.contains("held"), "{error}");

    // Proposals only: both parts propose the 2026-10-03 file, nothing
    // written.
    let before = files(&fixture.dir);
    let (pass, out) = resolve(&fixture, None, RESOLVED);
    assert!(!pass, "{out}");
    for part in &parts {
        assert!(
            out.contains(&format!(
                "held {part} (unlisted): --file {}",
                metrics_file()
            )),
            "{out}"
        );
    }
    assert_fail_line(&out, &["2 hold(s) open"]);
    assert_eq!(files(&fixture.dir), before);

    // Re-admit.
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    assert!(out.contains("rows match (2 parts, 25 rows)"), "{out}");
    assert!(
        out.contains(&format!("wrote metrics/{}.import", metrics_name())),
        "{out}"
    );
    assert_eq!(
        fs::read(ledger_file(&fixture, &format!("{}.import", metrics_name()))).unwrap(),
        entry
    );
    let supersede = ledger_file(&fixture, &format!("{}.1.supersede.import", metrics_name()));
    assert!(!supersede.exists());
    assert!(held(&fixture).is_empty());
    assert_eq!(selected(&fixture), baseline);

    // The importer runs again: the file is skipped, another day dry-runs.
    let out = run_import(&fixture, &metrics_day(day, false)).unwrap();
    assert!(out.contains("skipped"), "{out}");
    run_import(&fixture, &metrics_day(day + 1, true)).unwrap();
}

/// Re-admit over a conflicting entry: a supersede record naming both
/// variants, a superseding entry, the clearing; E0 unchanged everywhere;
/// the same selections; the next pass copies both files, a verification
/// passes and a re-import skips.
#[test]
fn readmit_supersedes_a_conflicting_entry() {
    let (_dir, fixture) = adopted("resolve-conflict");
    let (baseline, parts) = conflict(&fixture);
    let e0 = format!("{}.import", metrics_name());
    let e0_bytes: Vec<Vec<u8>> = [
        ledger_file(&fixture, &e0),
        target_file(&fixture, "cold-a", &e0),
        target_file(&fixture, "cold-b", &e0),
    ]
    .iter()
    .map(|path| fs::read(path).unwrap())
    .collect();

    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    let s1 = format!("{}.1.supersede.import", metrics_name());
    let e1 = format!("{}.1.import", metrics_name());
    let wrote: Vec<&str> = out.lines().filter(|l| l.starts_with("wrote ")).collect();
    assert_eq!(
        wrote,
        vec![format!("wrote metrics/{s1}"), format!("wrote metrics/{e1}")]
    );
    // RA7: the entry exists before the first `hold-cleared` record.
    let position = |prefix: &str| out.lines().position(|l| l.starts_with(prefix)).unwrap();
    assert!(
        position(&format!("wrote metrics/{e1}")) < position("hold-cleared "),
        "{out}"
    );
    let record =
        SupersedeRecord::parse(&fs::read_to_string(ledger_file(&fixture, &s1)).unwrap()).unwrap();
    assert_eq!(record.entries.len(), 2);
    assert!(
        record
            .entries
            .iter()
            .all(|e| e.name == format!("metrics/{e0}"))
    );
    assert!(record.entries[0] < record.entries[1]);
    assert_eq!(record.quarantine, None);
    assert!(ledger_file(&fixture, &e1).exists());
    let cleared: Vec<String> = copy_ledger::read(&fixture.hot())
        .unwrap()
        .unwrap()
        .records
        .into_iter()
        .filter_map(|r| match r.kind {
            Kind::HoldCleared {
                path,
                resolution: Resolution::Readmit,
            } => Some(path),
            _ => None,
        })
        .collect();
    assert_eq!(cleared, parts);
    let after: Vec<Vec<u8>> = [
        ledger_file(&fixture, &e0),
        target_file(&fixture, "cold-a", &e0),
        target_file(&fixture, "cold-b", &e0),
    ]
    .iter()
    .map(|path| fs::read(path).unwrap())
    .collect();
    assert_eq!(after, e0_bytes);
    assert!(held(&fixture).is_empty());
    assert_eq!(selected(&fixture), baseline);

    // The next pass copies S1 and E1; a verification at now + G passes.
    let (_, out) = replicate(&fixture, RESOLVED + 1);
    for root in ["cold-a", "cold-b"] {
        for name in [&s1, &e1] {
            assert_eq!(
                fs::read(target_file(&fixture, root, name)).unwrap(),
                fs::read(ledger_file(&fixture, name)).unwrap(),
                "{root} {name}: {out}"
            );
        }
    }
    let (pass, out) = verify(&fixture, RESOLVED + 1 + G);
    assert!(pass, "{out}");
    let out = run_import(&fixture, &metrics_day(day_index("2026-10-03"), false)).unwrap();
    assert!(out.contains("skipped"), "{out}");
}

/// A republished file whose hour 7 differs: the first difference is named,
/// nothing is written anywhere, the hold stays and the importer refuses.
#[test]
fn rows_differ_writes_nothing_and_keeps_the_hold() {
    let (_dir, fixture) = adopted("resolve-differ");
    let (_, parts, _) = missing_entry(&fixture);
    let day = day_index("2026-10-03");
    let changed = metrics_csv(day).replace(
        "2026-10-03 07:00:00,BTCUSDT,90007.1230000000000000",
        "2026-10-03 07:00:00,BTCUSDT,90007.9990000000000000",
    );
    assert_ne!(changed, metrics_csv(day));
    fixture
        .archive
        .publish(ArchiveStream::Metrics, day, &changed);
    let before = files(&fixture.dir);
    let held_before = held(&fixture);
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[
            &format!(
                "line 9 of {} (2026-10-03, row 7 of the day) differs from {} record 7",
                metrics_file(),
                parts[0]
            ),
            "nothing written",
            "the hold stays",
            "--replace",
        ],
    );
    assert_eq!(files(&fixture.dir), before);
    let staging = &fixture.archive_config.paths.staging;
    assert!(files(staging).is_empty());
    assert_eq!(held(&fixture), held_before);
    let error = run_import(&fixture, &metrics_day(day, false)).unwrap_err();
    assert!(error.contains("held"), "{error}");
}

/// After a superseding re-admit and its pass, the copy ledger is rebuilt
/// from a full, a stale and an empty start: each gives the post-re-admit
/// selections with the same dataset versions and no hold; the stale start
/// names both B7′ clears.
#[test]
fn rebuilds_after_a_superseding_readmit_agree() {
    let mut expected: Option<Vec<String>> = None;
    for start in ["full", "stale", "empty"] {
        let (_dir, fixture) = adopted(&format!("resolve-rebuild-{start}"));
        let (_, parts) = conflict(&fixture);
        let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
        assert!(pass, "{out}");
        replicate(&fixture, RESOLVED + 1);
        let after = selected(&fixture);
        fs::remove_file(hot_ledger(&fixture)).unwrap();
        match start {
            "stale" => cut_after_last_hold(&fixture),
            "empty" => damage_both_copies(&fixture),
            _ => {}
        }
        let (_, out) = rebuild(&fixture, Flags::default(), RESOLVED + HOUR);
        assert!(out.contains("wrote "), "{start}: {out}");
        assert!(
            !out.lines()
                .any(|l| l.starts_with("FAIL: ") && l.contains("held")),
            "{start}: {out}"
        );
        if start == "stale" {
            for part in &parts {
                assert!(
                    out.contains(&format!("hold-cleared {part} readmit")),
                    "{out}"
                );
            }
        }
        assert!(held(&fixture).is_empty(), "{start}: {out}");
        assert_eq!(selected(&fixture), after, "{start}: {out}");
        match &expected {
            None => expected = Some(after),
            Some(expected) => assert_eq!(&after, expected, "{start}"),
        }
    }
}

/// Review of #212, finding 1: a generation-0 `entry-conflict` hold whose
/// E0 copies agree again (as `mie store repair` R8 leaves them) is
/// re-admitted with S1 and E1, not by the clearing alone (RA5 (c)), so a
/// full, a stale and an empty rebuild start all clear it (B7′ takes only
/// an entry of generation ≥ 1 as evidence for `entry-conflict`).
#[test]
fn a_gen0_entry_conflict_readmit_survives_a_stale_rebuild() {
    for start in ["full", "stale", "empty"] {
        let (_dir, fixture) = adopted(&format!("resolve-gen0-{start}"));
        conflict(&fixture);
        let e0 = format!("{}.import", metrics_name());
        let copy = target_file(&fixture, "cold-a", &e0); // what R8 does
        fs::remove_file(&copy).unwrap();
        fs::copy(ledger_file(&fixture, &e0), &copy).unwrap();
        let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
        assert!(pass, "{out}");
        let wrote: Vec<&str> = out.lines().filter(|l| l.starts_with("wrote ")).collect();
        assert_eq!(
            wrote,
            vec![
                format!("wrote metrics/{}.1.supersede.import", metrics_name()),
                format!("wrote metrics/{}.1.import", metrics_name()),
            ],
            "case (d), not (c): {out}"
        );
        replicate(&fixture, RESOLVED + 1);
        fs::remove_file(hot_ledger(&fixture)).unwrap();
        match start {
            "stale" => cut_after_last_hold(&fixture),
            "empty" => damage_both_copies(&fixture),
            _ => {}
        }
        let (_, out) = rebuild(&fixture, Flags::default(), RESOLVED + HOUR);
        assert!(held(&fixture).is_empty(), "{start}: {out}");
    }
}

/// The same for case (a): every copy of E0 gone while its parts are held
/// `entry-conflict`, so the entry resolve writes is the only evidence a
/// stale rebuild start can clear those holds on: S1 naming no entry, then
/// E1.
#[test]
fn an_entry_conflict_readmit_of_a_missing_entry_survives_a_stale_rebuild() {
    for start in ["full", "stale", "empty"] {
        let (_dir, fixture) = adopted(&format!("resolve-gone-{start}"));
        conflict(&fixture);
        let e0 = format!("{}.import", metrics_name());
        for path in [
            ledger_file(&fixture, &e0),
            target_file(&fixture, "cold-a", &e0),
            target_file(&fixture, "cold-b", &e0),
        ] {
            fs::remove_file(path).unwrap();
        }
        let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
        assert!(pass, "{out}");
        let s1 = format!("{}.1.supersede.import", metrics_name());
        let record =
            SupersedeRecord::parse(&fs::read_to_string(ledger_file(&fixture, &s1)).unwrap())
                .unwrap();
        assert!(record.entries.is_empty(), "{out}");
        assert!(
            ledger_file(&fixture, &format!("{}.1.import", metrics_name())).exists(),
            "{out}"
        );
        replicate(&fixture, RESOLVED + 1);
        fs::remove_file(hot_ledger(&fixture)).unwrap();
        match start {
            "stale" => cut_after_last_hold(&fixture),
            "empty" => damage_both_copies(&fixture),
            _ => {}
        }
        let (_, out) = rebuild(&fixture, Flags::default(), RESOLVED + HOUR);
        assert!(held(&fixture).is_empty(), "{start}: {out}");
    }
}

/// Rewrites the `sha256` line of the import-entry copy at `copy` to
/// another published SHA-256: a second variant listing the same files.
fn rewrite_published_sha(copy: &Path) {
    let text = fs::read_to_string(copy).unwrap();
    let sha = text
        .lines()
        .find_map(|line| line.strip_prefix("sha256 "))
        .unwrap()
        .to_owned();
    let other = format!(
        "{}{}",
        &sha[..63],
        if sha.ends_with('0') { '1' } else { '0' }
    );
    overwrite(copy, text.replace(&sha, &other).as_bytes());
}

/// Overwrites the read-only import-ledger copy at `path`.
fn overwrite(path: &Path, bytes: &[u8]) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions).unwrap();
    fs::write(path, bytes).unwrap();
}

/// Review of #212, finding 2: after a superseding re-admit and its pass,
/// cold-a's copy of the superseded E0 rots, which no verification checks
/// and no repair touches; a later conflict on E1 is still re-admitted (S2,
/// E2), the rotten copy named.
#[test]
fn a_rotten_superseded_copy_blocks_no_later_readmit() {
    let (_dir, fixture) = adopted("resolve-rotten-superseded");
    conflict(&fixture);
    assert_pass(&resolve(&fixture, Some(&metrics_file()), RESOLVED));
    replicate(&fixture, RESOLVED + 1);
    let e0 = format!("{}.import", metrics_name());
    let e1 = format!("{}.1.import", metrics_name());
    overwrite(&target_file(&fixture, "cold-a", &e0), b"rotten\n");
    rewrite_published_sha(&target_file(&fixture, "cold-b", &e1));
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    let (pass, out) = rebuild(&fixture, Flags::default(), RESOLVED + HOUR);
    assert!(!pass && held(&fixture).len() == 2, "{out}");

    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED + 2 * HOUR);
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    let wrote: Vec<&str> = out.lines().filter(|l| l.starts_with("wrote ")).collect();
    assert_eq!(
        wrote,
        vec![
            format!("wrote metrics/{}.2.supersede.import", metrics_name()),
            format!("wrote metrics/{}.2.import", metrics_name()),
        ],
        "{out}"
    );
    assert!(
        out.lines()
            .any(|l| l.contains(&format!("metrics/{e0}")) && l.contains("superseded")),
        "{out}"
    );
    assert!(held(&fixture).is_empty(), "{out}");
}

/// Re-review of #212, finding 3: the same rot on hot's copy of the
/// superseded E0 still refuses. On hot it stops the importer and the
/// replication of F (both read every file of F), so a re-admit that cleared
/// the holds would leave F unimportable and S2/E2 unreplicated.
#[test]
fn a_rotten_superseded_copy_on_hot_still_refuses() {
    let (_dir, fixture) = adopted("resolve-rotten-superseded-hot");
    conflict(&fixture);
    assert_pass(&resolve(&fixture, Some(&metrics_file()), RESOLVED));
    replicate(&fixture, RESOLVED + 1);
    let e0 = format!("{}.import", metrics_name());
    let e1 = format!("{}.1.import", metrics_name());
    overwrite(&ledger_file(&fixture, &e0), b"rotten\n");
    rewrite_published_sha(&target_file(&fixture, "cold-b", &e1));
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    let (pass, out) = rebuild(&fixture, Flags::default(), RESOLVED + HOUR);
    assert!(!pass && !held(&fixture).is_empty(), "{out}");

    let before = files(&fixture.dir);
    let held_before = held(&fixture);
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED + 2 * HOUR);
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[&format!("metrics/{e0}"), "on hot", "cannot be read", "#217"],
    );
    assert_eq!(files(&fixture.dir), before, "{out}");
    assert_eq!(held(&fixture), held_before, "{out}");
}

/// Review of #212, finding 2: a malformed name of F on a copy target only
/// counts for nothing anywhere, so it is named and blocks nothing; on hot
/// it still refuses.
#[test]
fn a_malformed_name_on_a_target_only_blocks_no_readmit() {
    let (_dir, fixture) = adopted("resolve-malformed-target");
    missing_entry(&fixture);
    let bad = format!("{}.01.import", metrics_name());
    let on_target = target_file(&fixture, "cold-a", &bad);
    fs::create_dir_all(on_target.parent().unwrap()).unwrap();
    fs::write(&on_target, b"anything\n").unwrap();
    let on_hot = ledger_file(&fixture, &bad);
    fs::write(&on_hot, b"anything\n").unwrap();
    let before = files(&fixture.dir);
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&bad, "on hot", "malformed"]);
    assert_eq!(files(&fixture.dir), before);

    fs::remove_file(&on_hot).unwrap();
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    assert!(
        out.lines()
            .any(|l| l.contains(&bad) && l.contains("cold-a") && l.contains("malformed")),
        "{out}"
    );
    assert!(held(&fixture).is_empty(), "{out}");
}

/// Hot's import-ledger files, the held paths and the selections.
type Outcome = (Vec<(String, Vec<u8>)>, BTreeSet<String>, Vec<String>);

/// Every import-ledger file on hot by name, plus the held paths and the
/// selections: what a re-run must reach.
fn outcome(fixture: &Fixture) -> Outcome {
    (
        files(&fixture.archive_config.paths.import_ledger),
        held(fixture),
        selected(fixture),
    )
}

/// RA7: a re-run from each interrupted state (S1 only; S1 + E1; S1 + E1 +
/// one `hold-cleared`) ends equal to the uninterrupted run and never
/// writes S2.
#[test]
fn a_rerun_completes_an_interrupted_readmit() {
    let (_ref_dir, reference) = adopted("resolve-rerun-reference");
    conflict(&reference);
    assert_pass(&resolve(&reference, Some(&metrics_file()), RESOLVED));
    let reference_outcome = outcome(&reference);
    let s1 = format!("{}.1.supersede.import", metrics_name());
    let e1 = format!("{}.1.import", metrics_name());
    let s1_bytes = fs::read(ledger_file(&reference, &s1)).unwrap();
    let e1_bytes = fs::read(ledger_file(&reference, &e1)).unwrap();
    for state in ["S1", "S1+E1", "S1+E1+cleared"] {
        let (_dir, fixture) = adopted(&format!("resolve-rerun-{}", state.replace('+', "-")));
        let (_, parts) = conflict(&fixture);
        fs::write(ledger_file(&fixture, &s1), &s1_bytes).unwrap();
        if state != "S1" {
            fs::write(ledger_file(&fixture, &e1), &e1_bytes).unwrap();
        }
        if state == "S1+E1+cleared" {
            CopyLedger::at(&fixture.hot())
                .append(&Record {
                    at_ms: RESOLVED - 1,
                    kind: Kind::HoldCleared {
                        path: parts[0].clone(),
                        resolution: Resolution::Readmit,
                    },
                })
                .unwrap();
        }
        let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
        assert!(pass, "{state}: {out}");
        assert!(!out.contains(".2."), "{out}");
        let wrote_e1 = out.contains(&format!("wrote metrics/{e1}"));
        assert_eq!(wrote_e1, state == "S1", "{state}: {out}");
        assert!(!ledger_file(&fixture, &format!("{}.2.supersede.import", metrics_name())).exists());
        assert_eq!(outcome(&fixture), reference_outcome, "{state}: {out}");
    }
}

/// A held part that left hot is read from a cold copy; one missing on
/// every root refuses the check, naming the repair.
#[test]
fn held_parts_are_read_through_the_union() {
    let manifest_of = |path: &str| format!("{}.manifest", path.strip_suffix(".parquet").unwrap());
    let (_dir, fixture) = adopted("resolve-union");
    let (_, parts) = conflict(&fixture);
    for file in [parts[0].clone(), manifest_of(&parts[0])] {
        fs::remove_file(fixture.hot().join(file)).unwrap();
    }
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
    assert!(pass, "{out}");

    let (_dir, fixture) = adopted("resolve-union-gone");
    let (_, parts) = conflict(&fixture);
    for root in [
        fixture.hot(),
        cold(&fixture, "cold-a"),
        cold(&fixture, "cold-b"),
    ] {
        for file in [parts[0].clone(), manifest_of(&parts[0])] {
            fs::remove_file(root.join(file)).unwrap();
        }
    }
    let before = files(&fixture.dir);
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&parts[0], "mie store verify", "mie store repair"]);
    assert_eq!(files(&fixture.dir), before);
}

/// Proposals: no open hold passes; a held live path and another symbol's
/// part get no proposal, named; nothing is written.
#[test]
fn proposals_list_every_open_hold() {
    let (_dir, fixture) = adopted("resolve-proposals");
    let (pass, out) = resolve(&fixture, None, RESOLVED);
    assert!(pass && out == "PASS\n", "{out}");
    let live =
        "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00009.parquet";
    let eth = "source=binance-archive/instrument=ETHUSDT/stream=metrics/date=2026-10-03/part-00000.parquet";
    for path in [live, eth] {
        CopyLedger::at(&fixture.hot())
            .append(&Record {
                at_ms: D0 + 2,
                kind: Kind::Hold {
                    path: path.to_owned(),
                    reason: HoldReason::Unlisted,
                },
            })
            .unwrap();
    }
    let before = files(&fixture.dir);
    let (pass, out) = resolve(&fixture, None, RESOLVED);
    assert!(!pass, "{out}");
    assert!(
        out.contains(&format!(
            "held {live} (unlisted): no proposal (not an archive part)"
        )),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "held {eth} (unlisted): no proposal (instrument ETHUSDT"
        )),
        "{out}"
    );
    assert_fail_line(&out, &["2 hold(s) open"]);
    assert_eq!(files(&fixture.dir), before);
}

/// RA1: every refusal leaves the tree unchanged.
#[test]
fn resolve_refusals_write_nothing() {
    let (_dir, fixture) = adopted("resolve-refusals");
    missing_entry(&fixture);
    let file = metrics_file();
    let snapshot = || files(&fixture.dir);
    let refused = |result: (bool, String), needles: &[&str], before: &Vec<(String, Vec<u8>)>| {
        let (pass, out) = result;
        assert!(!pass, "{out}");
        assert_fail_line(&out, needles);
        assert_eq!(&files(&fixture.dir), before, "{out}");
    };

    // An empty cold_roots.
    let single = ingest_toml(
        &fixture.dir.join("single.toml"),
        &fixture.hot(),
        &fixture.config.paths.journal,
        &[],
    );
    let before = snapshot();
    let mut out = Vec::new();
    let pass = store::resolve(
        &single,
        Some(&fixture.dst_archive),
        Some(&file),
        false,
        &transports(&fixture.archive),
        &fixture.probe(),
        &|| RESOLVED,
        &mut out,
    )
    .unwrap();
    refused(
        (pass, String::from_utf8(out).unwrap()),
        &["cold_roots is empty"],
        &before,
    );

    // No archive configuration ([history] is absent from the fixture).
    refused(
        resolve_with(&fixture, Some(&file), &fixture.probe(), None, RESOLVED),
        &["no archive configuration"],
        &before,
    );
    // Another store's archive configuration.
    archive_toml(
        &fixture.dir.join("other-archive.toml"),
        &fixture.dir.join("other"),
        &fixture.dir.join("other/raw"),
        &cold_roots(&fixture.dir),
    );
    let before = snapshot();
    refused(
        resolve_with(
            &fixture,
            Some(&file),
            &fixture.probe(),
            Some(&fixture.dir.join("other-archive.toml")),
            RESOLVED,
        ),
        &[],
        &before,
    );
    // One device for hot and a target.
    let shared = MapProbe(vec![
        (fixture.hot(), 1),
        (fixture.dir.join("cold-a"), 1),
        (fixture.dir.join("cold-b"), 3),
    ]);
    refused(
        resolve_with(
            &fixture,
            Some(&file),
            &shared,
            Some(&fixture.dst_archive),
            RESOLVED,
        ),
        &["device"],
        &before,
    );
    // A target without its marker.
    let marker = root_marker::marker_path(&cold(&fixture, "cold-b"));
    let aside = fixture.dir.join("marker-b");
    fs::rename(&marker, &aside).unwrap();
    let result = resolve(&fixture, Some(&file), RESOLVED);
    fs::rename(&aside, &marker).unwrap();
    refused(result, &["cold-b", "no root marker"], &before);
    // No hot marker.
    let marker = root_marker::marker_path(&fixture.hot());
    let aside = fixture.dir.join("marker-hot");
    fs::rename(&marker, &aside).unwrap();
    let result = resolve(&fixture, Some(&file), RESOLVED);
    fs::rename(&aside, &marker).unwrap();
    refused(result, &["no root marker"], &before);
    // The archive writer lock held.
    let lock = removal::take_source_lock(&fixture.hot(), ARCHIVE_SOURCE).unwrap();
    let result = resolve(&fixture, Some(&file), RESOLVED);
    drop(lock);
    refused(result, &["writer lock"], &before);
    // A missing copy ledger.
    let ledger = hot_ledger(&fixture);
    let aside = fixture.dir.join("ledger-aside");
    fs::rename(&ledger, &aside).unwrap();
    let before_missing = snapshot();
    let result = resolve(&fixture, Some(&file), RESOLVED);
    refused(result, &["copy ledger", "does not exist"], &before_missing);
    fs::rename(&aside, &ledger).unwrap();
    // Nothing changed the holds; a resolve then passes.
    assert_pass(&resolve(&fixture, Some(&file), RESOLVED));
}

/// RA1: a second resolve is refused on the lock, also when no `.lock`
/// file existed before the first took it.
#[test]
fn a_second_resolve_is_refused() {
    let (_dir, fixture) = adopted("resolve-second");
    missing_entry(&fixture);
    let lock_file = fixture.hot().join("source=binance-archive/.lock");
    fs::remove_file(&lock_file).unwrap();
    // The first resolve's lock, created where none existed.
    let first = removal::take_source_lock(&fixture.hot(), ARCHIVE_SOURCE).unwrap();
    assert!(lock_file.exists());
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["writer lock", "another writer holds"]);
    drop(first);
    assert_pass(&resolve(&fixture, Some(&metrics_file()), RESOLVED));
    let ledgers = LedgerDir::new(&fixture.archive_config.paths.import_ledger);
    assert!(
        ledgers
            .read(ArchiveStream::Metrics, &metrics_name())
            .unwrap()
            .is_some()
    );
}
