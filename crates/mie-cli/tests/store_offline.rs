//! `mie store init` and `mie store init-root` offline (ADR-062 D4, D7):
//! every refusal, in order, and what a success writes.

mod common;

use common::TempDir;
use mie_adapter_parquet::copy_ledger;
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_cli::config::IngestConfig;
use mie_cli::store::{self, DeviceProbe, StDevProbe};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

/// A tiered ingest configuration under `dir`: hot `dir/raw`, copy targets
/// `dir/cold-a` and `dir/cold-b`, plus `extra` lines at the end. Written to
/// `dir/ingest.toml` too, for the binary.
fn tiered(dir: &Path, extra: &str) -> IngestConfig {
    let text = format!(
        "[instrument]\nsymbol = \"BTCUSDT\"\nsource = \"binance-um\"\n[paths]\n\
         raw_root = \"{}\"\njournal = \"{}\"\ncold_roots = [\n\
         {{ path = \"{}\", copy_target = true }},\n\
         {{ path = \"{}\", copy_target = true }},\n]\n[binance]\n\
         ws_base_url = \"wss://fake.invalid/ws\"\nrest_base_url = \"https://fake.invalid\"\n\
         streams = [\"aggTrade\"]\n{extra}",
        dir.join("raw").display(),
        dir.join("journal.jsonl").display(),
        dir.join("cold-a").display(),
        dir.join("cold-b").display(),
    );
    fs::write(dir.join("ingest.toml"), &text).unwrap();
    IngestConfig::parse(&text).unwrap()
}

fn init(config: &IngestConfig) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::init(config, &mut out).unwrap();
    (pass, String::from_utf8(out).unwrap())
}

fn init_root(
    config: &IngestConfig,
    root: &Path,
    id: &str,
    probe: &dyn DeviceProbe,
) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::init_root(config, root, id, probe, &mut out).unwrap();
    (pass, String::from_utf8(out).unwrap())
}

#[track_caller]
fn assert_fail(result: (bool, String), expected: &str) {
    let (pass, out) = result;
    assert!(!pass, "{out}");
    assert!(out.starts_with("FAIL: "), "{out}");
    assert!(out.contains(expected), "expected {expected:?} in {out}");
}

fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

/// Device ids by path, compared after resolving both sides (the mount
/// check queries `<root>/..`): distinct devices without distinct
/// filesystems.
struct MapProbe(Vec<(PathBuf, u64)>);

impl DeviceProbe for MapProbe {
    fn device_id(&self, root: &Path) -> io::Result<u64> {
        let resolve = |path: &Path| fs::canonicalize(path).unwrap_or_else(|_| path.to_owned());
        let wanted = resolve(root);
        self.0
            .iter()
            .find(|(path, _)| resolve(path) == wanted)
            .map(|(_, id)| *id)
            .ok_or_else(|| io::Error::other(format!("{} unknown", root.display())))
    }
}

/// hot on device 1, `cold-a` on 2, `cold-b` on 3, and their parent (the
/// test directory) on 9: both cold roots are mount points.
fn distinct(dir: &Path) -> MapProbe {
    MapProbe(vec![
        (dir.join("raw"), 1),
        (dir.join("cold-a"), 2),
        (dir.join("cold-b"), 3),
        (dir.to_path_buf(), 9),
    ])
}

#[test]
fn init_writes_the_hot_marker_and_a_header_only_ledger() {
    let dir = TempDir::new("store-init");
    let dir = dir.path();
    let config = tiered(dir, "");
    fs::create_dir_all(dir.join("cold-a")).unwrap();
    fs::create_dir_all(dir.join("cold-b")).unwrap();
    // raw_root does not exist yet: init creates it, as the writer does.
    let (pass, out) = init(&config);
    assert!(pass, "{out}");
    let raw = dir.join("raw");
    assert_eq!(root_marker::read(&raw).unwrap().as_deref(), Some(HOT_ID));
    assert_eq!(
        fs::read_to_string(copy_ledger::ledger_path(&raw)).unwrap(),
        copy_ledger::HEADER
    );
    assert_eq!(names(&raw.join(".mie")), ["copy-ledger", "root"]);
    assert_eq!(names(&raw), [".mie"]);
    for cold in ["cold-a", "cold-b"] {
        assert!(
            out.contains(&format!(
                "cold root {}: no marker, no ledger copy",
                dir.join(cold).display()
            )),
            "{out}"
        );
        assert_eq!(names(&dir.join(cold)), Vec::<String>::new());
    }
    assert!(out.contains("wrote"), "{out}");
    // Once only.
    assert_fail(init(&config), "already exists");
}

#[test]
fn init_refuses_in_order() {
    let dir = TempDir::new("store-init-refusals");
    let dir = dir.path();
    let raw = dir.join("raw");

    // 1. No cold roots: a single-root store needs no `.mie/`.
    assert_fail(init(&common::config(dir)), "paths.cold_roots is empty");
    assert!(!raw.join(".mie").exists());

    // 2. A [history] whose archive configuration does not load (on an
    //    existing raw_root; a missing one is named first, see
    //    `init_with_history_names_a_missing_raw_root`).
    fs::create_dir_all(&raw).unwrap();
    let history = format!(
        "[history]\norigin = \"2025-10-01\"\ncutover = \"2026-10-20\"\n\
         archive_config = \"{}\"\n",
        dir.join("missing-archive.toml").display()
    );
    assert_fail(init(&tiered(dir, &history)), "history.archive_config");
    assert!(!raw.join(".mie").exists());

    let config = tiered(dir, "");
    for cold in ["cold-a", "cold-b"] {
        fs::create_dir_all(dir.join(cold)).unwrap();
    }

    // 4a. A cold root with a marker: a lost hot, never re-initialized.
    root_marker::create_meta(&dir.join("cold-b"), "cold-b", false).unwrap();
    assert_fail(init(&config), "hot restore");
    assert!(!raw.join(".mie").exists());
    fs::remove_dir_all(dir.join("cold-b").join(".mie")).unwrap();

    // 4b. A cold root with a ledger copy and no marker.
    fs::create_dir_all(dir.join("cold-a").join(".mie")).unwrap();
    fs::write(
        copy_ledger::ledger_path(&dir.join("cold-a")),
        copy_ledger::HEADER,
    )
    .unwrap();
    assert_fail(init(&config), "copy-ledger copy");
    assert!(!raw.join(".mie").exists());
    fs::remove_dir_all(dir.join("cold-a").join(".mie")).unwrap();

    // 4c. An unreadable cold root fails closed.
    {
        use std::os::unix::fs::PermissionsExt;
        let cold = dir.join("cold-a");
        fs::set_permissions(&cold, fs::Permissions::from_mode(0o000)).unwrap();
        // Root reads through any permission; the check is moot there.
        if fs::read_dir(&cold).is_err() {
            assert_fail(init(&config), "cannot read its marker");
            assert!(!raw.join(".mie").exists());
        }
        fs::set_permissions(&cold, fs::Permissions::from_mode(0o755)).unwrap();
    }

    // 3. `.mie` exists (checked before the cold roots).
    fs::create_dir_all(raw.join(".mie")).unwrap();
    root_marker::create_meta(&dir.join("cold-b"), "cold-b", false).unwrap();
    assert_fail(init(&config), "already exists");
    fs::remove_dir_all(raw.join(".mie")).unwrap();
    fs::remove_dir_all(dir.join("cold-b").join(".mie")).unwrap();

    let (pass, out) = init(&config);
    assert!(pass, "{out}");
}

#[test]
fn init_root_refuses_in_order() {
    let dir = TempDir::new("store-init-root");
    let dir = dir.path();
    let config = tiered(dir, "");
    let (cold_a, cold_b) = (dir.join("cold-a"), dir.join("cold-b"));
    let probe = distinct(dir);

    // 1. Not a configured cold root (lexically).
    let spelled = dir.join("cold-b").join("..").join("cold-a");
    assert_fail(
        init_root(&config, &spelled, "cold-a", &probe),
        "not one of the configured paths.cold_roots",
    );
    assert_fail(
        init_root(&config, &dir.join("raw"), "cold-a", &probe),
        "not one of the configured",
    );
    // 2. A bad or reserved id.
    for (id, expected) in [("cold/a", "--id"), ("", "--id"), ("hot", "reserved")] {
        assert_fail(init_root(&config, &cold_a, id, &probe), expected);
    }
    // 3. Hot has no marker: `mie store init` comes first.
    assert_fail(
        init_root(&config, &cold_a, "cold-a", &probe),
        "mie store init",
    );
    let (pass, out) = init(&config);
    assert!(pass, "{out}");

    // 4. The root is missing (and is not created), not a directory, or not
    //    empty.
    assert_fail(
        init_root(&config, &cold_a, "cold-a", &probe),
        "never created",
    );
    assert!(!cold_a.exists());
    fs::write(&cold_a, b"").unwrap();
    assert_fail(
        init_root(&config, &cold_a, "cold-a", &probe),
        "not a directory",
    );
    fs::remove_file(&cold_a).unwrap();
    fs::create_dir_all(cold_a.join("lost+found")).unwrap();
    assert_fail(init_root(&config, &cold_a, "cold-a", &probe), "not empty");
    fs::remove_dir(cold_a.join("lost+found")).unwrap();

    // 5. Hot's device.
    let same = MapProbe(vec![(dir.join("raw"), 1), (cold_a.clone(), 1)]);
    let (pass, out) = init_root(&config, &cold_a, "cold-a", &same);
    assert!(!pass);
    assert!(
        out.contains("device 1") && out.contains("(1)") && out.contains("unmounted mount point"),
        "{out}"
    );
    assert!(!cold_a.join(".mie").exists());

    // 5b. Review of #197, finding 4: the parent's device. A directory that
    //     is not a mount point is refused even when its filesystem is not
    //     hot's.
    let not_mounted = MapProbe(vec![
        (dir.join("raw"), 1),
        (cold_a.clone(), 2),
        (dir.to_path_buf(), 2),
    ]);
    assert_fail(
        init_root(&config, &cold_a, "cold-a", &not_mounted),
        "not a mount point",
    );
    assert!(!cold_a.join(".mie").exists());

    // Distinct devices pass.
    let (pass, out) = init_root(&config, &cold_a, "cold-a", &probe);
    assert!(pass, "{out}");
    assert_eq!(
        root_marker::read(&cold_a).unwrap().as_deref(),
        Some("cold-a")
    );
    assert_eq!(names(&cold_a.join(".mie")), ["root"]);

    // 6. Another configured root already carries the id.
    fs::create_dir_all(&cold_b).unwrap();
    assert_fail(
        init_root(&config, &cold_b, "cold-a", &probe),
        "already carries the id",
    );
    let (pass, out) = init_root(&config, &cold_b, "cold-b", &probe);
    assert!(pass, "{out}");
    // Once only: the root is no longer empty.
    assert_fail(init_root(&config, &cold_b, "cold-b", &probe), "not empty");
}

/// Review of #197, finding 3: a retry after a crashed or failed attempt
/// is not refused as "not empty" because of that attempt's staging
/// directory, which the successful retry removes. Any other entry still
/// makes the root non-empty.
#[test]
fn init_root_retries_over_a_crashed_attempts_staging_directory() {
    let dir = TempDir::new("store-init-root-retry");
    let dir = dir.path();
    let config = tiered(dir, "");
    assert!(init(&config).0);
    let cold_a = dir.join("cold-a");
    fs::create_dir_all(cold_a.join(".mie-init-4242-0.tmp")).unwrap();
    fs::write(
        cold_a.join(".mie-init-4242-0.tmp").join("root"),
        b"mie-root 1\n",
    )
    .unwrap();
    fs::create_dir_all(cold_a.join(".mie-init.tmp")).unwrap();
    let probe = distinct(dir);
    // A file under a staging name is not ours: still not empty.
    fs::write(cold_a.join(".mie-init-1-1.tmp"), b"").unwrap();
    assert_fail(init_root(&config, &cold_a, "cold-a", &probe), "not empty");
    fs::remove_file(cold_a.join(".mie-init-1-1.tmp")).unwrap();

    let (pass, out) = init_root(&config, &cold_a, "cold-a", &probe);
    assert!(pass, "{out}");
    assert_eq!(names(&cold_a), [".mie"]);
    assert_eq!(
        root_marker::read(&cold_a).unwrap().as_deref(),
        Some("cold-a")
    );
}

/// QA of #197, Known limit: with [history], a missing raw_root is named
/// with what to do, and nothing is created.
#[test]
fn init_with_history_names_a_missing_raw_root() {
    let dir = TempDir::new("store-init-history");
    let dir = dir.path();
    let raw = dir.join("raw");
    fs::write(
        dir.join("archive.toml"),
        format!(
            "[instrument]\nsymbol = \"BTCUSDT\"\n[paths]\nraw_root = \"{}\"\n\
             import_ledger = \"{}\"\nstaging = \"{}\"\ncold_roots = [\n\
             {{ path = \"{}\", copy_target = true }},\n\
             {{ path = \"{}\", copy_target = true }},\n]\n",
            raw.display(),
            dir.join("ledger").display(),
            dir.join("staging").display(),
            dir.join("cold-a").display(),
            dir.join("cold-b").display(),
        ),
    )
    .unwrap();
    let history = format!(
        "[history]\norigin = \"2025-10-01\"\ncutover = \"2026-10-20\"\n\
         archive_config = \"{}\"\n",
        dir.join("archive.toml").display()
    );
    let config = tiered(dir, &history);
    let before = names(dir);
    let (pass, out) = init(&config);
    assert!(!pass, "{out}");
    assert!(
        out.contains(&format!("raw_root {} does not exist", raw.display()))
            && out.contains("create it"),
        "{out}"
    );
    assert_eq!(names(dir), before, "nothing is created");

    fs::create_dir_all(&raw).unwrap();
    let (pass, out) = init(&config);
    assert!(pass, "{out}");
}

#[test]
fn the_real_probe_refuses_a_directory_beside_hot() {
    let dir = TempDir::new("store-st-dev");
    let dir = dir.path();
    let config = tiered(dir, "");
    assert!(init(&config).0);
    fs::create_dir_all(dir.join("cold-a")).unwrap();
    assert_fail(
        init_root(&config, &dir.join("cold-a"), "cold-a", &StDevProbe),
        "unmounted mount point",
    );
    assert!(!dir.join("cold-a").join(".mie").exists());
}

/// `mie store init-root` through the binary, with the override variable
/// giving hot and the target distinct ids.
fn binary_init_root(dir: &Path) -> std::process::Output {
    let config = tiered(dir, "");
    assert!(init(&config).0);
    let cold = dir.join("cold-a");
    fs::create_dir_all(&cold).unwrap();
    Command::new(env!("CARGO_BIN_EXE_mie"))
        .args(["store", "init-root", "--config"])
        .arg(dir.join("ingest.toml"))
        .arg("--root")
        .arg(&cold)
        .args(["--id", "cold-a"])
        .env(
            "MIE_DEV_DEVICE_IDS",
            format!(
                "{}=1;{}=2;{}=3",
                dir.join("raw").display(),
                cold.display(),
                dir.display()
            ),
        )
        .output()
        .unwrap()
}

/// A build without the feature never reads the variable: the real probe
/// refuses the directory beside hot.
#[cfg(not(feature = "dev-device-override"))]
#[test]
fn a_default_build_ignores_the_device_override() {
    let dir = TempDir::new("store-binary-default");
    let output = binary_init_root(dir.path());
    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert_eq!(output.status.code(), Some(1), "{stdout}{stderr}");
    assert!(
        stdout.contains("FAIL: ") && stdout.contains("unmounted mount point"),
        "{stdout}"
    );
    assert!(!stderr.contains("warning"), "{stderr}");
}

/// A build with the feature takes the override and warns.
#[cfg(feature = "dev-device-override")]
#[test]
fn a_dev_build_takes_the_device_override_and_warns() {
    let dir = TempDir::new("store-binary-dev");
    let output = binary_init_root(dir.path());
    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert_eq!(output.status.code(), Some(0), "{stdout}{stderr}");
    assert!(stdout.contains("wrote"), "{stdout}");
    assert!(
        stderr.contains("warning") && stderr.contains("MIE_DEV_DEVICE_IDS"),
        "{stderr}"
    );
    assert_eq!(
        root_marker::read(&dir.path().join("cold-a"))
            .unwrap()
            .as_deref(),
        Some("cold-a")
    );
}

#[test]
fn usage_errors_exit_2() {
    for args in [
        vec!["store"],
        vec!["store", "init"],
        vec!["store", "remove"],
        vec!["store", "init-root", "--config", "x.toml"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_mie"))
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}");
    }
}
