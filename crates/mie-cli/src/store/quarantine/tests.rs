//! The replace's commit point over hot, the copy targets and the read-only
//! roots (ADR-062 D11 *As amended (#216)*): which roots count, the marker
//! rule, and the info lines that name a read-only root still holding a
//! moved path.

use super::*;
use crate::config::ColdRoot;
use crate::store::archive_checks::tests::part;
use crate::store::replicate::tests::Dir;
use mie_adapter_binance::archive::catalog::{Period, parse_day};
use mie_adapter_binance::archive::ledger::Quarantine;
use mie_adapter_parquet::root_marker;
use mie_ports::raw::RawStreamKey;

const ID: &str = "q-1791259200000";

/// A supersede record moving `moved` (sorted) under [`ID`].
fn record(moved: &[&str]) -> SupersedeRecord {
    let mut moved: Vec<String> = moved.iter().map(|p| (*p).to_owned()).collect();
    moved.sort();
    SupersedeRecord {
        archive: "data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-10-01.zip"
            .to_owned(),
        stream: RawStreamKey::new("binance-archive", "BTCUSDT", "aggTrades").unwrap(),
        period: Period::Day(parse_day("2026-10-01").unwrap()),
        entries: Vec::new(),
        quarantine: Some(Quarantine {
            id: ID.to_owned(),
            moved,
        }),
    }
}

/// `dir/<name>`, created, marked `<name>` when `marked`.
fn root(dir: &Dir, name: &str, marked: bool) -> PathBuf {
    let path = dir.0.join(name);
    fs::create_dir_all(&path).unwrap();
    if marked {
        root_marker::create_meta(&path, name, false).unwrap();
    }
    path
}

fn cold(path: &Path, copy_target: bool) -> ColdRoot {
    ColdRoot {
        path: path.to_owned(),
        copy_target,
    }
}

/// How a layout holds a path: its data file, its manifest, or both.
#[derive(Debug, Clone, Copy)]
enum Form {
    Data,
    Manifest,
    Both,
}

const FORMS: [Form; 3] = [Form::Data, Form::Manifest, Form::Both];

/// Puts `path` into the layout of `root` in `form`.
fn put(root: &Path, path: &str, form: Form) {
    let manifest = format!("{}.manifest", path.strip_suffix(".parquet").unwrap());
    let names: &[&str] = match form {
        Form::Data => &[path],
        Form::Manifest => &[&manifest],
        Form::Both => &[path, &manifest],
    };
    for name in names {
        let file = root.join(name);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, b"bytes").unwrap();
    }
}

/// A read-only root holding a moved path, in any form, never makes the
/// replace unfinished; with or without the marker rule.
#[test]
fn unfinished_skips_read_only_roots() {
    let p = part("aggTrades", "2026-10-01", 0);
    for form in FORMS {
        let dir = Dir::new("quarantine-ro-skip");
        let hot = root(&dir, "hot", false);
        let a = root(&dir, "cold-a", true);
        let r = root(&dir, "cold-r", true);
        put(&r, &p, form);
        let roots = ReplaceRoots::read(&hot, &[cold(&a, true), cold(&r, false)]);
        for require_markers in [true, false] {
            assert_eq!(
                roots.unfinished(&record(&[&p]), require_markers),
                Ok(None),
                "{form:?}"
            );
        }
    }
}

/// A read-only holder of the first moved path, between two copy targets,
/// does not end the loop: the copy target after it that holds the last
/// path, in any form, makes the replace unfinished and is named.
#[test]
fn unfinished_finds_a_copy_target_after_a_read_only_holder() {
    let (first, last) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-01", 1),
    );
    for form in FORMS {
        let dir = Dir::new("quarantine-ro-then-target");
        let hot = root(&dir, "hot", false);
        let a = root(&dir, "cold-a", true);
        let r = root(&dir, "cold-r", true);
        let b = root(&dir, "cold-b", true);
        put(&r, &first, Form::Both);
        put(&b, &last, form);
        let roots = ReplaceRoots::read(&hot, &[cold(&a, true), cold(&r, false), cold(&b, true)]);
        assert_eq!(
            roots.unfinished(&record(&[&first, &last]), true),
            Ok(Some(format!("{last} is still in cold-b's layout"))),
            "{form:?}"
        );
        // Hot counts as well.
        fs::remove_dir_all(&b).unwrap();
        fs::create_dir_all(&b).unwrap();
        root_marker::create_meta(&b, "cold-b", false).unwrap();
        put(&hot, &last, form);
        assert_eq!(
            roots.unfinished(&record(&[&first, &last]), true),
            Ok(Some(format!("{last} is still in hot's layout"))),
            "{form:?}"
        );
    }
}

/// The marker rule binds the copy targets only: an unmarked read-only
/// root, empty or holding a moved path, is neither inspected nor named;
/// an unmarked copy target is refused under the rule, as before.
#[test]
fn unfinished_markers_count_on_copy_targets_only() {
    let p = part("aggTrades", "2026-10-01", 0);
    let dir = Dir::new("quarantine-ro-markers");
    let hot = root(&dir, "hot", false);
    let a = root(&dir, "cold-a", true);
    let u = root(&dir, "cold-u", false);
    let roots = ReplaceRoots::read(&hot, &[cold(&a, true), cold(&u, false)]);
    assert_eq!(roots.unfinished(&record(&[&p]), true), Ok(None));
    put(&u, &p, Form::Both);
    assert_eq!(roots.unfinished(&record(&[&p]), true), Ok(None));
    assert!(roots.read_only_holders(&record(&[&p])).is_empty());

    let t = root(&dir, "cold-t", false);
    let roots = ReplaceRoots::read(&hot, &[cold(&a, true), cold(&t, true)]);
    let why = roots.unfinished(&record(&[&p]), true).unwrap().unwrap();
    assert!(why.contains("no readable marker"), "{why}");
    assert_eq!(roots.unfinished(&record(&[&p]), false), Ok(None));
}

/// One directory configured twice, once read-only and once as a copy
/// target (through a symlink): the copy-target entry counts, in either
/// order, so the replace is unfinished (fail closed).
#[test]
fn one_directory_as_copy_target_and_read_only_counts() {
    let p = part("aggTrades", "2026-10-01", 0);
    let dir = Dir::new("quarantine-ro-twice");
    let hot = root(&dir, "hot", false);
    let c = root(&dir, "cold-c", true);
    let link = dir.0.join("cold-c-link");
    std::os::unix::fs::symlink(&c, &link).unwrap();
    put(&c, &p, Form::Both);
    for roots in [
        [cold(&c, false), cold(&link, true)],
        [cold(&link, true), cold(&c, false)],
        [cold(&link, false), cold(&c, true)],
    ] {
        let roots = ReplaceRoots::read(&hot, &roots);
        assert_eq!(
            roots.unfinished(&record(&[&p]), true),
            Ok(Some(format!("{p} is still in cold-c's layout")))
        );
    }
}

/// One info line per (moved path, marked read-only root holding it), in
/// path order, then configuration order; none for a copy target, an
/// unmarked root or a record without a quarantine.
#[test]
fn read_only_holders_lines() {
    let (p0, p1) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-01", 1),
    );
    let dir = Dir::new("quarantine-ro-lines");
    let hot = root(&dir, "hot", false);
    let a = root(&dir, "cold-a", true);
    let r = root(&dir, "cold-r", true);
    let s = root(&dir, "cold-s", true);
    let u = root(&dir, "cold-u", false);
    let roots = ReplaceRoots::read(
        &hot,
        &[
            cold(&a, true),
            cold(&r, false),
            cold(&s, false),
            cold(&u, false),
        ],
    );
    let moved = record(&[&p0, &p1]);

    // Holding nothing: no line.
    assert!(roots.read_only_holders(&moved).is_empty());
    // A record without a quarantine: no line, never unfinished.
    let mut plain = moved.clone();
    plain.quarantine = None;
    put(&a, &p0, Form::Both);
    put(&r, &p0, Form::Data);
    assert!(roots.read_only_holders(&plain).is_empty());
    assert_eq!(roots.unfinished(&plain, true), Ok(None));
    // cold-r holds 1 of 2: exactly 1 line; cold-a, a copy target, none.
    assert_eq!(
        roots.read_only_holders(&moved),
        vec![read_only_line(&p0, "cold-r", ID)]
    );
    // M = 2 paths and 2 read-only roots holding both: 2 × 2 lines, paths
    // first; the unmarked root holding both is not named.
    for root in [&r, &s, &u] {
        put(root, &p0, Form::Manifest);
        put(root, &p1, Form::Both);
    }
    assert_eq!(
        roots.read_only_holders(&moved),
        vec![
            read_only_line(&p0, "cold-r", ID),
            read_only_line(&p0, "cold-s", ID),
            read_only_line(&p1, "cold-r", ID),
            read_only_line(&p1, "cold-s", ID),
        ]
    );
    let line = read_only_line(&p0, "cold-r", ID);
    assert!(
        line.starts_with(&format!(
            "{p0} (quarantine {ID}): still in the layout of read-only root cold-r;"
        )) && line.contains("ADR-062 D4, D11"),
        "{line}"
    );
}

/// An I/O error inspecting a read-only root (a layout directory that is a
/// file) is a line, never an error and never a panic; on a copy target the
/// same error stops `unfinished`, as before.
#[test]
fn read_only_holders_report_an_inspection_error_as_a_line() {
    let p = part("aggTrades", "2026-10-01", 0);
    let dir = Dir::new("quarantine-ro-error");
    let hot = root(&dir, "hot", false);
    let r = root(&dir, "cold-r", true);
    fs::write(r.join("source=binance-archive"), b"no directory").unwrap();
    let lines = read_only_lines(&[("cold-r", r.as_path())], std::slice::from_ref(&p), ID);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with(&format!(
            "{p} on read-only root cold-r cannot be inspected ("
        )),
        "{lines:?}"
    );
    let roots = ReplaceRoots::read(&hot, &[cold(&r, false)]);
    assert_eq!(roots.read_only_holders(&record(&[&p])), lines);
    assert_eq!(roots.unfinished(&record(&[&p]), true), Ok(None));

    let roots = ReplaceRoots::read(&hot, &[cold(&r, true)]);
    let error = roots.unfinished(&record(&[&p]), true).unwrap_err();
    assert!(error.contains(&format!("inspect {p} on cold-r")), "{error}");
}
