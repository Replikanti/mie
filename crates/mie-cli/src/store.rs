//! `mie store`: the setup of a tiered raw store (ADR-062 D4, D7).
//!
//! - [`init`] (`mie store init`) writes hot's root marker and an empty copy
//!   ledger, once, for a new tiered store.
//! - [`init_root`] (`mie store init-root`) writes the marker of a freshly
//!   provisioned cold root, once.
//! - [`check_hot`] is the start refusal of `mie ingest` and `mie
//!   archive-import`: with `cold_roots` configured, hot must carry its
//!   marker.
//! - [`check_copy_ledger`] is the importer's second start refusal: with
//!   `cold_roots` configured, hot's copy ledger must be readable; and
//!   [`check_holds`], folded from the same read, its third: no open hold
//!   (#193 H1, ADR-062 D11).
//! - [`rebuild`] (`mie store rebuild`) rebuilds a lost or damaged copy
//!   ledger from the ledger copies, the manifests on every root, the
//!   journal and the import ledger, records what it finds lost only with
//!   `--record-lost`, and gates the hot removal on the next verification
//!   ([`rebuild`](mod@rebuild) holds the rules, #193, ADR-062 D7); the
//!   archive holds it re-derives are [`archive_checks`].
//! - [`replicate`] (`mie store replicate`) runs one replication pass: it
//!   copies every sealed file, import-ledger entry and the tails of the
//!   copy ledger and the journal to the copy targets that present their
//!   markers, and never removes anything ([`replicate`](mod@replicate) holds
//!   the pass, #189, ADR-062 D5, D9, D11). `mie ingest` runs the same pass
//!   after every seal pass, plans the hot removal from it and removes in
//!   its seal path (#191, D6).
//! - [`adopt`] (`mie store adopt`) adopts an existing single-root store:
//!   every sealed file goes straight to every copy target, partitions older
//!   than the hot window are recorded as removed from hot, the hot-window
//!   partitions, the journal and the import entries go to hot, and one
//!   replication pass follows. The source is only read
//!   ([`adopt`](mod@adopt) holds the run, #190, ADR-062 D10, D8, D6).
//! - [`verify`] (`mie store verify`) checks every copy on every root and
//!   appends its findings to the copy ledger; it reads the roots only
//!   ([`verify`](mod@verify) holds the rules, #192, ADR-062 D11).
//! - [`repair`] (`mie store repair`) replaces the damaged or missing
//!   copies the ledger records from a good one, moves each damaged copy
//!   aside into `.mie/damaged/`, fills a target that lacks the files that
//!   left hot, and ends with one replication pass
//!   ([`repair`](mod@repair) holds the rules, #192, ADR-062 D11).
//! - [`resolve`] (`mie store resolve`) proposes the archive file of every
//!   open hold, and with `--file` re-admits the held parts of that file
//!   when they equal its published rows: it writes the missing or a
//!   superseding import entry and clears the holds
//!   ([`resolve`](mod@resolve) holds the rules, #210, ADR-062 D11).
//! - [`check_devices`] refuses two roots that report one device id, before
//!   any `mie store` command writes a byte to a copy target.
//! - [`raw_store`] builds the raw store of every command: the tiered store
//!   of `raw_root` and `cold_roots`, which with no cold root is the
//!   single-root store. No command builds a single-root store over a tiered
//!   configuration, and a configuration without `cold_roots` over a hot
//!   that holds `.mie/` is refused.
//!
//! Every device check goes through one [`DeviceProbe`]. [`StDevProbe`]
//! (`st_dev`) is its only production implementation. A Cargo feature
//! `dev-device-override`, not in `default`, compiles in a second probe for
//! host runs that keep hot and the copy targets as plain directories on one
//! filesystem (#184 STOP 1 decision 1): it reads `MIE_DEV_DEVICE_IDS`,
//! `<root path>=<id>` pairs separated by `;`, paths canonicalized. It must
//! list hot, every cold root and each cold root's parent directory (the
//! mount-point check of `init-root`). A root the variable does not list is
//! an error, never a fallback to `st_dev`,
//! and every probe selection with the variable set prints a warning on
//! stderr naming the override. A build without the feature has no override
//! and never reads the variable. This comment is the only operator-facing
//! place that names them: they are a test device, not a configuration key.

use crate::config::{ColdRoot, IngestConfig};
use mie_adapter_parquet::ParquetRawStore;
use mie_adapter_parquet::copy_ledger;
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_ports::raw::validate_segment;
use std::fs;
use std::io::{self, ErrorKind, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

pub mod adopt;
pub mod archive_checks;
pub mod quarantine;
pub mod rebuild;
pub mod repair;
pub mod replicate;
pub mod resolve;
pub mod verify;

pub use adopt::{AdoptReport, AdoptRequest};
pub use repair::RepairReport;
pub use replicate::{LedgerAppend, PassObserver, PassReport, truncation_line};
pub use verify::VerifyReport;

/// The device a root lives on.
///
/// `Send + Sync`, so the replicator's thread (#189) can share the probe.
pub trait DeviceProbe: Send + Sync {
    /// The device id of `root`.
    ///
    /// # Errors
    ///
    /// The I/O error of the probe.
    fn device_id(&self, root: &Path) -> io::Result<u64>;
}

/// The production probe: `st_dev` of the root's metadata.
#[derive(Debug, Clone, Copy, Default)]
pub struct StDevProbe;

impl DeviceProbe for StDevProbe {
    fn device_id(&self, root: &Path) -> io::Result<u64> {
        Ok(fs::metadata(root)?.dev())
    }
}

/// The test-only device override (see the module docs).
#[cfg(any(test, feature = "dev-device-override"))]
mod dev_override {
    use super::{DeviceProbe, StDevProbe};
    use std::io;
    use std::path::{Path, PathBuf};

    /// Device ids by canonical root path.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct OverrideProbe {
        ids: Vec<(PathBuf, u64)>,
    }

    impl DeviceProbe for OverrideProbe {
        fn device_id(&self, root: &Path) -> io::Result<u64> {
            let canonical = std::fs::canonicalize(root)?;
            self.ids
                .iter()
                .find(|(path, _)| *path == canonical)
                .map(|(_, id)| *id)
                .ok_or_else(|| {
                    io::Error::other(format!(
                        "{} is not listed in the device override",
                        root.display()
                    ))
                })
        }
    }

    /// Parses `<path>=<id>` pairs separated by `;`. Each pair splits on its
    /// last `=`, the id is a canonical decimal, the path must exist and is
    /// canonicalized, and no canonical path may repeat.
    pub(crate) fn parse(value: &str) -> Result<OverrideProbe, String> {
        let mut ids: Vec<(PathBuf, u64)> = Vec::new();
        for pair in value.split(';') {
            if pair.is_empty() {
                return Err(format!("device override {value:?}: an empty entry"));
            }
            let (path, id) = pair
                .rsplit_once('=')
                .filter(|(path, _)| !path.is_empty())
                .ok_or_else(|| format!("device override: {pair:?} is not <path>=<id>"))?;
            let canonical =
                id.bytes().all(|b| b.is_ascii_digit()) && (id == "0" || !id.starts_with('0'));
            let id: u64 = canonical
                .then(|| id.parse().ok())
                .flatten()
                .ok_or_else(|| format!("device override: {id:?} is not a decimal id"))?;
            let path =
                std::fs::canonicalize(path).map_err(|e| format!("device override: {path}: {e}"))?;
            if ids.iter().any(|(listed, _)| *listed == path) {
                return Err(format!(
                    "device override: {} is listed twice",
                    path.display()
                ));
            }
            ids.push((path, id));
        }
        Ok(OverrideProbe { ids })
    }

    /// The probe for the variable's value, plus the warning to print when
    /// it is set.
    pub(crate) fn select(
        value: Option<&str>,
    ) -> Result<(Box<dyn DeviceProbe>, Option<String>), String> {
        match value {
            None => Ok((Box::new(StDevProbe), None)),
            Some(value) => Ok((
                Box::new(parse(value)?),
                Some(
                    "warning: MIE_DEV_DEVICE_IDS is set: device ids come from the test-only \
                     override of the dev-device-override build, not from st_dev; such a build \
                     must never run in production"
                        .to_owned(),
                ),
            )),
        }
    }
}

/// The device probe of this build: [`StDevProbe`], or with the
/// `dev-device-override` feature and the variable set, the override (with
/// its warning on stderr).
///
/// # Errors
///
/// With the feature: a malformed or non-Unicode override.
pub fn device_probe() -> Result<Box<dyn DeviceProbe>, String> {
    #[cfg(feature = "dev-device-override")]
    {
        let value = match std::env::var("MIE_DEV_DEVICE_IDS") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err("MIE_DEV_DEVICE_IDS is not Unicode".to_owned());
            }
        };
        let (probe, warning) = dev_override::select(value.as_deref())?;
        if let Some(warning) = warning {
            eprintln!("mie: {warning}");
        }
        Ok(probe)
    }
    #[cfg(not(feature = "dev-device-override"))]
    {
        Ok(Box::new(StDevProbe))
    }
}

/// The start refusal on a hot without its marker (ADR-062 D7, *Hot
/// lost*). With `cold_roots` empty it returns at once, without a filesystem
/// call. It reads hot's marker only, never a cold root.
///
/// # Errors
///
/// A description naming `mie store init` and the hot restore when hot has
/// no marker, or naming the cold root whose marker `raw_root` carries, or
/// the read failure.
pub fn check_hot(raw_root: &Path, cold_roots: &[ColdRoot]) -> Result<(), String> {
    if cold_roots.is_empty() {
        return Ok(());
    }
    match root_marker::read(raw_root) {
        Ok(Some(id)) if id == HOT_ID => Ok(()),
        Ok(Some(id)) => Err(format!(
            "raw_root {} carries the marker of cold root {id:?}, not hot's: raw_root must name \
             the hot root (ADR-062 D4)",
            raw_root.display()
        )),
        Ok(None) => Err(format!(
            "raw_root {} has no root marker although paths.cold_roots is configured. A new \
             tiered store starts with `mie store init --config <ingest config>`; a lost hot is \
             restored from the copy targets by the hot restore (ADR-062 D11), never \
             re-initialized",
            raw_root.display()
        )),
        Err(error) => Err(format!(
            "raw_root {}: cannot read hot's root marker: {error}",
            raw_root.display()
        )),
    }
}

/// The raw store of `raw_root` and `cold_roots` (ADR-062 D7): hot, then the
/// cold roots in configuration order. It never touches a cold mount; with
/// `cold_roots` empty it is the single-root store.
///
/// With `cold_roots` empty it refuses a `raw_root` that holds a `.mie/`
/// directory: only a tiered store has one (D7, `mie store init`), and a
/// configuration that omits its `cold_roots` (an archive configuration
/// written before the store was tiered, where the key defaults to empty)
/// would read hot alone, a shorter selection without an error once
/// partitions leave hot. That check stats hot only.
///
/// # Errors
///
/// A description naming `<raw_root>/.mie` and the missing `cold_roots`, or
/// the failure to inspect it.
pub fn raw_store(raw_root: &Path, cold_roots: &[ColdRoot]) -> Result<ParquetRawStore, String> {
    if cold_roots.is_empty() {
        let meta = root_marker::meta_dir(raw_root);
        if exists(&meta)? {
            return Err(format!(
                "raw_root {} is a tiered store ({} exists) but this configuration has no \
                 paths.cold_roots: it would read hot alone and miss every partition that left \
                 hot. Add the store's cold_roots, the same entries in the same order as the \
                 ingest configuration, to this configuration (ADR-062 D7)",
                raw_root.display(),
                meta.display()
            ));
        }
    }
    Ok(ParquetRawStore::tiered(
        raw_root,
        cold_roots.iter().map(|root| root.path.clone()).collect(),
    ))
}

/// The importer's refusal on a missing or unreadable copy ledger (ADR-062
/// D6, D7 *Hot intact, ledger lost*). With `cold_roots` empty it returns
/// `None` at once, without a filesystem call; otherwise the ledger read,
/// which [`check_holds`] folds.
///
/// # Errors
///
/// A description naming the ledger, and the read failure if there is one:
/// the importer refuses, since its days can lie outside the hot window, and
/// the copy-ledger rebuild restores the ledger.
pub fn check_copy_ledger(
    raw_root: &Path,
    cold_roots: &[ColdRoot],
) -> Result<Option<copy_ledger::Ledger>, String> {
    if cold_roots.is_empty() {
        return Ok(None);
    }
    let path = copy_ledger::ledger_path(raw_root);
    let refusal = "the importer refuses, since its days can lie outside the hot window; the \
                   copy-ledger rebuild restores the ledger (ADR-062 D7)";
    match copy_ledger::read(raw_root) {
        Ok(Some(ledger)) => Ok(Some(ledger)),
        Ok(None) => Err(format!(
            "copy ledger {} does not exist although paths.cold_roots is configured: {refusal}",
            path.display()
        )),
        Err(error) => Err(format!(
            "copy ledger {} cannot be read ({error}): {refusal}",
            path.display()
        )),
    }
}

/// The importer's refusal while a hold is open (ADR-062 D11, #193 H1),
/// folded from the read of [`check_copy_ledger`], so it costs no extra I/O:
/// every held path fails the selections that cover it, and an import could
/// write parts next to it before the hold is resolved.
///
/// # Errors
///
/// A description naming the count, the first held path in path order, its
/// reason and the resolution step.
pub fn check_holds(ledger: &copy_ledger::Ledger) -> Result<(), String> {
    let state = mie_adapter_parquet::verification::CopyState::fold(ledger)
        .map_err(|error| format!("the copy ledger cannot be folded: {error}"))?;
    let held = state.held();
    match held.first() {
        None => Ok(()),
        Some((path, reason)) => Err(format!(
            "{} archive part(s) are held, the first {path} ({}): the importer refuses until \
             every hold is resolved by `mie store resolve` (ADR-062 D11)",
            held.len(),
            reason.as_str()
        )),
    }
}

/// Refuses two roots that report one device id (ADR-062 D11, *What mie
/// can check*): hot and every copy target that presented its marker must
/// be on pairwise distinct devices. Every colliding pair is named with both
/// roots, in a fixed order: the target pairs in configuration order, then
/// each target against hot.
///
/// # Errors
///
/// A description of every collision, or the probe's failure.
pub fn check_devices(hot: &Path, targets: &[&Path], probe: &dyn DeviceProbe) -> Result<(), String> {
    let device = |path: &Path| {
        probe
            .device_id(path)
            .map_err(|e| format!("device of {}: {e}", path.display()))
    };
    let hot_device = device(hot)?;
    let devices = targets
        .iter()
        .map(|target| device(target))
        .collect::<Result<Vec<_>, _>>()?;
    let mut collisions = Vec::new();
    for (i, (a, a_device)) in targets.iter().zip(&devices).enumerate() {
        for (b, b_device) in targets.iter().zip(&devices).skip(i + 1) {
            if a_device == b_device {
                collisions.push(format!(
                    "copy targets {} and {} are both on device {a_device}",
                    a.display(),
                    b.display()
                ));
            }
        }
    }
    for (target, target_device) in targets.iter().zip(&devices) {
        if *target_device == hot_device {
            collisions.push(format!(
                "copy target {} is on device {target_device}, the device of raw_root {}",
                target.display(),
                hot.display()
            ));
        }
    }
    if collisions.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{}: copies on one device fail together, so k copies need k independent devices; an \
         unmounted mount point is an empty directory on its parent's filesystem (ADR-062 D11)",
        collisions.join("; ")
    ))
}

/// `mie store replicate`: one replication pass (ADR-062 D11, the module
/// [`replicate`](mod@replicate)) over the store of `config`, with the
/// archive configuration `archive` (`--archive-config`), else `[history]
/// archive_config`, else none. `now_ms` stamps the ledger records.
///
/// It prints what it copied and appended, then every truncation of the
/// copy ledger's trailing record on its own line, then `PASS` (returns
/// `true`) or one `FAIL: <problem>` line per problem (returns `false`).
///
/// # Errors
///
/// A description when the report cannot be written.
pub fn replicate(
    config: &IngestConfig,
    archive: Option<&Path>,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    out: &mut dyn Write,
) -> Result<bool, String> {
    /// Collects every truncation as a report line.
    struct Lines(Vec<String>);
    impl PassObserver for Lines {
        fn ledger_truncated(&mut self, ledger: &Path, truncation: copy_ledger::Truncation) {
            self.0.push(truncation_line(ledger, truncation));
        }
    }
    let ledger = copy_ledger::CopyLedger::at(&config.paths.raw_root);
    let mut truncations = Lines(Vec::new());
    let report = replicate::run(config, archive, probe, now_ms, &ledger, &mut truncations);
    let pass = report.problems.is_empty();
    let mut lines = report.lines;
    lines.extend(truncations.0);
    if pass {
        lines.push("PASS".to_owned());
    }
    lines.extend(
        report
            .problems
            .iter()
            .map(|problem| format!("FAIL: {problem}")),
    );
    for line in lines {
        writeln!(out, "{line}").map_err(|e| format!("write the report: {e}"))?;
    }
    out.flush().map_err(|e| format!("write the report: {e}"))?;
    Ok(pass)
}

/// `mie store adopt`: adopts the store `request.from` into the tiered store
/// of `config` (ADR-062 D10, the module [`adopt`](mod@adopt)).
/// `first_hot_date` is the first UTC date of the hot window,
/// [`crate::ingest::first_hot_date`] in production; a test passes another
/// date for a shorter window. `now_ms` stamps the ledger records.
///
/// It prints what it adopted, the replication pass's lines and every
/// copy-ledger truncation, then `PASS` (returns `true`) or one `FAIL:
/// <problem>` line per problem (returns `false`).
///
/// # Errors
///
/// A description when the report cannot be written.
pub fn adopt(
    config: &IngestConfig,
    request: &AdoptRequest,
    first_hot_date: &str,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    out: &mut dyn Write,
) -> Result<bool, String> {
    let report = adopt::run(config, request, first_hot_date, probe, now_ms);
    let pass = report.problems.is_empty();
    let mut lines = report.lines;
    if pass {
        lines.push("PASS".to_owned());
    }
    lines.extend(
        report
            .problems
            .iter()
            .map(|problem| format!("FAIL: {problem}")),
    );
    for line in lines {
        writeln!(out, "{line}").map_err(|e| format!("write the report: {e}"))?;
    }
    out.flush().map_err(|e| format!("write the report: {e}"))?;
    Ok(pass)
}

/// `mie store verify`: one verification of the store of `config` (ADR-062
/// D11, the module [`verify`](mod@verify)), with the archive configuration
/// `archive`, else `[history] archive_config`, else none, and P from
/// `period_hours` (`--period-hours`, refused unless a whole number of at
/// least 1). `now_ms` stamps the records and is "now" for P and G.
///
/// It prints one summary line per root and every copy-ledger truncation,
/// then `PASS` (returns `true`) or one `FAIL: <finding>` line per finding
/// or refusal (returns `false`).
///
/// # Errors
///
/// A description when the report cannot be written.
pub fn verify(
    config: &IngestConfig,
    archive: Option<&Path>,
    period_hours: &str,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    out: &mut dyn Write,
) -> Result<bool, String> {
    let ledger = copy_ledger::CopyLedger::at(&config.paths.raw_root);
    let report = verify::run(config, archive, period_hours, probe, now_ms, &ledger);
    print_report(report.lines, &report.problems, out)
}

/// `mie store repair`: one repair of the store of `config` (ADR-062 D11,
/// the module [`repair`](mod@repair)), with the archive configuration
/// `archive`, else `[history] archive_config`, else none. With
/// `record_lost` (`--record-lost`) a lost file gets its `lost` record.
/// `now_ms` stamps the records and names the run's `.mie/damaged/`
/// directories.
///
/// It prints what it repaired, the closing pass's lines and every
/// copy-ledger truncation, then `PASS` (returns `true`) or one `FAIL:
/// <problem>` line per problem (returns `false`).
///
/// # Errors
///
/// A description when the report cannot be written.
pub fn repair(
    config: &IngestConfig,
    archive: Option<&Path>,
    record_lost: bool,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    out: &mut dyn Write,
) -> Result<bool, String> {
    let ledger = copy_ledger::CopyLedger::at(&config.paths.raw_root);
    let report = repair::run(config, archive, record_lost, probe, now_ms, &ledger);
    print_report(report.lines, &report.problems, out)
}

/// `mie store rebuild`: one copy-ledger rebuild of the store of `config`
/// (ADR-062 D7, the module [`rebuild`](mod@rebuild)), with the archive
/// configuration `archive`, else `[history] archive_config`, else none,
/// and the hot window starting on `first_hot_date`
/// ([`crate::ingest::first_hot_date`] in production). `now_ms` is read once:
/// it stamps every record and names the run's `.mie/damaged/` directories.
///
/// It prints the start, the records derived, every lost file and hold, the
/// gate and every ledger copy moved aside, then `wrote …` or `nothing
/// written`, then `PASS` (returns `true`) or one `FAIL: <problem>` line per
/// problem (returns `false`).
///
/// # Errors
///
/// A description when the report cannot be written.
pub fn rebuild(
    config: &IngestConfig,
    archive: Option<&Path>,
    flags: rebuild::Flags,
    first_hot_date: &str,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    out: &mut dyn Write,
) -> Result<bool, String> {
    let report = rebuild::run(config, archive, flags, first_hot_date, probe, now_ms);
    print_report(report.lines, &report.problems, out)
}

/// `mie store resolve`: proposes the archive file of every open hold, or
/// with `file` (`--file`) re-admits that file's held parts, or with
/// `replace` (`--replace`) moves its superseded generation into quarantine
/// (ADR-062 D11, the module [`resolve`](mod@resolve)), with the archive
/// configuration `archive`, else `[history] archive_config`. `transports`
/// fetch the file (never for a replace); `now_ms` stamps the records.
///
/// It prints what it read, checked and wrote, then `PASS` (returns
/// `true`) or one `FAIL: <problem>` line per problem (returns `false`).
///
/// # Errors
///
/// A description when the report cannot be written.
#[allow(clippy::too_many_arguments)]
pub fn resolve(
    config: &IngestConfig,
    archive: Option<&Path>,
    file: Option<&str>,
    replace: bool,
    transports: &crate::archive::ArchiveTransports,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    out: &mut dyn Write,
) -> Result<bool, String> {
    let report = resolve::run(config, archive, file, replace, transports, probe, now_ms);
    print_report(report.lines, &report.problems, out)
}

/// Prints `lines`, then `PASS` without problems or one `FAIL:` line per
/// problem; returns whether it passed.
fn print_report(
    mut lines: Vec<String>,
    problems: &[String],
    out: &mut dyn Write,
) -> Result<bool, String> {
    let pass = problems.is_empty();
    if pass {
        lines.push("PASS".to_owned());
    }
    lines.extend(problems.iter().map(|problem| format!("FAIL: {problem}")));
    for line in lines {
        writeln!(out, "{line}").map_err(|e| format!("write the report: {e}"))?;
    }
    out.flush().map_err(|e| format!("write the report: {e}"))?;
    Ok(pass)
}

/// Writes the outcome of a `mie store` verb: its lines, or `FAIL: <reason>`.
fn report(result: Result<Vec<String>, String>, out: &mut dyn Write) -> Result<bool, String> {
    let (lines, pass) = match result {
        Ok(lines) => (lines, true),
        Err(reason) => (vec![format!("FAIL: {reason}")], false),
    };
    for line in lines {
        writeln!(out, "{line}").map_err(|e| format!("write the report: {e}"))?;
    }
    out.flush().map_err(|e| format!("write the report: {e}"))?;
    Ok(pass)
}

/// Whether `path` exists. Any error other than "not found" fails closed.
fn exists(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("cannot inspect {}: {error}", path.display())),
    }
}

/// `mie store init`: writes hot's marker and an empty copy ledger, printing
/// what it wrote and what it found on each cold root. Returns whether it
/// succeeded.
///
/// # Errors
///
/// A description when the report cannot be written. Every refusal is
/// reported in the output as `FAIL: <reason>`.
pub fn init(config: &IngestConfig, out: &mut dyn Write) -> Result<bool, String> {
    report(run_init(config), out)
}

fn run_init(config: &IngestConfig) -> Result<Vec<String>, String> {
    let raw_root = &config.paths.raw_root;
    let cold_roots = &config.paths.cold_roots;
    // A single-root store needs no `.mie/`, so a `.mie/` always means a
    // tiered store.
    if cold_roots.is_empty() {
        return Err(
            "paths.cold_roots is empty: a single-root store has no root marker and no copy \
             ledger (ADR-062 D4)"
                .to_owned(),
        );
    }
    if config.history.is_some() {
        // The [history] check resolves both raw roots on disk, so a missing
        // raw_root would fail it with a bare "No such file or directory".
        if !exists(raw_root)? {
            return Err(format!(
                "raw_root {} does not exist: create it (for example `mkdir -p {}`) and run \
                 `mie store init` again. With [history], init first checks that the archive \
                 configuration names the same raw root, which needs the directory to exist",
                raw_root.display(),
                raw_root.display()
            ));
        }
        config.history_archive().map_err(|e| e.to_string())?;
    }
    let meta = root_marker::meta_dir(raw_root);
    if exists(&meta)? {
        return Err(format!(
            "{} already exists: the store is initialized (ADR-062 D7)",
            meta.display()
        ));
    }
    let mut lines = Vec::new();
    for cold in cold_roots {
        // A cold marker next to a hot without one always means a hot that
        // was initialized and then lost: `init-root` needs hot's marker.
        if let Some(id) = root_marker::read(&cold.path).map_err(|e| {
            format!(
                "cold root {}: cannot read its marker, so a lost hot cannot be ruled out: {e}",
                cold.path.display()
            )
        })? {
            return Err(format!(
                "cold root {} carries the marker of root {id:?}, so this hot was initialized \
                 before and is lost: restore it by the hot restore (ADR-062 D11), never \
                 re-initialize it",
                cold.path.display()
            ));
        }
        let ledger = copy_ledger::ledger_path(&cold.path);
        if exists(&ledger).map_err(|e| format!("cold root {}: {e}", cold.path.display()))? {
            return Err(format!(
                "cold root {} holds a copy-ledger copy {}, so this hot is lost: restore it by \
                 the hot restore (ADR-062 D11), never re-initialize it",
                cold.path.display(),
                ledger.display()
            ));
        }
        lines.push(format!(
            "cold root {}: no marker, no ledger copy",
            cold.path.display()
        ));
    }
    fs::create_dir_all(raw_root).map_err(|e| format!("create {}: {e}", raw_root.display()))?;
    root_marker::create_meta(raw_root, HOT_ID, true).map_err(|e| e.to_string())?;
    lines.push(format!(
        "wrote {} (id {HOT_ID})",
        root_marker::marker_path(raw_root).display()
    ));
    lines.push(format!(
        "wrote {} (header only)",
        copy_ledger::ledger_path(raw_root).display()
    ));
    Ok(lines)
}

/// `mie store init-root`: writes the marker of the cold root `root` with
/// `id`, printing what it wrote. Returns whether it succeeded.
///
/// # Errors
///
/// A description when the report cannot be written. Every refusal is
/// reported in the output as `FAIL: <reason>`.
pub fn init_root(
    config: &IngestConfig,
    root: &Path,
    id: &str,
    probe: &dyn DeviceProbe,
    out: &mut dyn Write,
) -> Result<bool, String> {
    report(run_init_root(config, root, id, probe), out)
}

fn run_init_root(
    config: &IngestConfig,
    root: &Path,
    id: &str,
    probe: &dyn DeviceProbe,
) -> Result<Vec<String>, String> {
    let raw_root = &config.paths.raw_root;
    let cold_roots = &config.paths.cold_roots;
    if !cold_roots.iter().any(|cold| cold.path == root) {
        return Err(format!(
            "--root {} is not one of the configured paths.cold_roots",
            root.display()
        ));
    }
    validate_segment(id).map_err(|e| format!("--id: {e}"))?;
    if id == HOT_ID {
        return Err(format!(
            "--id {HOT_ID:?} is reserved for the hot root's marker"
        ));
    }
    // The device check compares against hot, and `init`'s cold-marker rule
    // relies on hot being initialized first.
    match root_marker::read(raw_root) {
        Ok(Some(hot)) if hot == HOT_ID => {}
        Ok(Some(other)) => {
            return Err(format!(
                "raw_root {} carries the marker of root {other:?}, not hot's",
                raw_root.display()
            ));
        }
        Ok(None) => {
            return Err(format!(
                "raw_root {} has no root marker: run `mie store init --config <ingest config>` \
                 first",
                raw_root.display()
            ));
        }
        Err(error) => return Err(format!("raw_root {}: {error}", raw_root.display())),
    }
    // Never created: a created directory lands on the parent's filesystem,
    // which is the unmounted-mount-point trap.
    let metadata = fs::metadata(root).map_err(|e| {
        format!(
            "--root {}: {e}; a cold root is never created, it must be a mounted, empty \
             directory",
            root.display()
        )
    })?;
    if !metadata.is_dir() {
        return Err(format!("--root {} is not a directory", root.display()));
    }
    // Empty but for staging directories that a crashed or failed earlier
    // attempt left behind; `create_meta` removes them once it succeeds.
    let entries = fs::read_dir(root).map_err(|e| format!("--root {}: {e}", root.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("--root {}: {e}", root.display()))?;
        let staging = entry
            .file_name()
            .to_str()
            .is_some_and(root_marker::is_staging_name)
            && entry.file_type().is_ok_and(|kind| kind.is_dir());
        if !staging {
            return Err(format!(
                "--root {} is not empty ({:?}): a marker is written only on a freshly \
                 provisioned root",
                root.display(),
                entry.file_name()
            ));
        }
    }
    let device = |path: &Path| {
        probe
            .device_id(path)
            .map_err(|e| format!("device of {}: {e}", path.display()))
    };
    let (root_device, hot_device) = (device(root)?, device(raw_root)?);
    if root_device == hot_device {
        return Err(format!(
            "--root {} is on device {root_device}, the device of raw_root {} \
             ({hot_device}): an unmounted mount point is an empty directory on hot's \
             filesystem, and a marker written there would make it look mounted (ADR-062 D4, \
             D11)",
            root.display(),
            raw_root.display()
        ));
    }
    // A cold root is a mount point itself: an unmounted mount point lies on
    // its parent's filesystem, which need not be hot's.
    let parent = root.join("..");
    let parent_device = device(&parent)?;
    if root_device == parent_device {
        return Err(format!(
            "--root {} is on device {root_device}, the device of its parent directory: it is \
             not a mount point. An unmounted mount point is an empty directory on its \
             parent's filesystem, and a marker written there would make it look mounted; \
             mount the cold volume at this path (ADR-062 D4)",
            root.display()
        ));
    }
    for cold in cold_roots.iter().filter(|cold| cold.path != root) {
        let other = root_marker::read(&cold.path)
            .map_err(|e| format!("cold root {}: {e}", cold.path.display()))?;
        if other.as_deref() == Some(id) {
            return Err(format!(
                "cold root {} already carries the id {id:?}",
                cold.path.display()
            ));
        }
    }
    root_marker::create_meta(root, id, false).map_err(|e| e.to_string())?;
    Ok(vec![format!(
        "wrote {} (id {id}, device {root_device})",
        root_marker::marker_path(root).display()
    )])
}

#[cfg(test)]
mod tests {
    use super::dev_override::{parse, select};
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Dir(PathBuf);

    impl Dir {
        fn new(tag: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let n = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "mie-cli-store-unit-{tag}-{}-{n}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn the_override_parses_pairs_of_canonical_paths() {
        let dir = Dir::new("override");
        let (a, b) = (dir.0.join("a"), dir.0.join("x=y"));
        fs::create_dir_all(&a).unwrap();
        fs::create_dir_all(&b).unwrap();
        // A path containing `=` splits on the last one.
        let probe = parse(&format!("{}=1;{}=2", a.display(), b.display())).unwrap();
        assert_eq!(probe.device_id(&a).unwrap(), 1);
        assert_eq!(probe.device_id(&b).unwrap(), 2);
        // Another spelling of a listed root.
        assert_eq!(probe.device_id(&a.join("..").join("a")).unwrap(), 1);
        // An unlisted root is an error, never st_dev.
        let unlisted = probe.device_id(&dir.0).unwrap_err();
        assert!(unlisted.to_string().contains("not listed"), "{unlisted}");

        let spelled = a.join("..").join("a");
        for bad in [
            format!("{}", a.display()),
            format!("{}=", a.display()),
            format!("={}", 1),
            format!("{}=1;", a.display()),
            format!("{}=1;;{}=2", a.display(), b.display()),
            String::new(),
            format!("{}=01", a.display()),
            format!("{}=-1", a.display()),
            format!("{}=one", a.display()),
            format!("{}=18446744073709551616", a.display()),
            format!("{}=1", dir.0.join("missing").display()),
            format!("{}=1;{}=2", a.display(), spelled.display()),
        ] {
            assert!(parse(&bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn selection_warns_only_with_the_override() {
        let dir = Dir::new("select");
        let (probe, warning) = select(None).unwrap();
        assert_eq!(warning, None);
        assert_eq!(
            probe.device_id(&dir.0).unwrap(),
            StDevProbe.device_id(&dir.0).unwrap()
        );
        let (probe, warning) = select(Some(&format!("{}=7", dir.0.display()))).unwrap();
        assert_eq!(probe.device_id(&dir.0).unwrap(), 7);
        let warning = warning.expect("a warning");
        assert!(
            warning.contains("MIE_DEV_DEVICE_IDS") && warning.contains("override"),
            "{warning}"
        );
        assert!(select(Some("nonsense")).is_err());
    }

    #[test]
    fn check_hot_touches_nothing_without_cold_roots() {
        // A raw root that does not exist: no filesystem call is made.
        assert_eq!(check_hot(Path::new("/nonexistent/raw"), &[]), Ok(()));
    }
}
