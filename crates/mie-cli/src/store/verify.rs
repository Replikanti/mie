//! The verification pass (ADR-062 D11 *Periodic verification*, #192):
//! `mie store verify`.
//!
//! It reads every root and appends its findings to hot's copy ledger; it
//! never writes a root and never repairs (D11: several bad copies at once
//! point at a shared cause, so the repair is an explicit step). It takes
//! the verification lock, never the pass lock, so it runs beside the
//! replicator of `mie ingest` (AW11). The rules, as numbered in the plan of
//! #192 (the tests cite them):
//!
//! - **V1 Refusals, before any cold read.** P (`--period-hours`) not a
//!   canonical decimal of at least 1 hour, or overflowing as milliseconds
//!   (0 would make every copy stale at once); an empty `cold_roots`; a hot
//!   without its marker; an archive configuration of another store; the
//!   verification lock held by another process; hot's copy ledger missing
//!   or failing its chain before its last record (the error names the
//!   record and its byte offset: the hot half of "the chain verifies on hot
//!   and on every copy"); a copy target with hot's id or an id two targets
//!   share; two roots on one device.
//! - **V2 Read order.** Hot's sealed listing, then hot's ledger, read once
//!   (the snapshot S, verified prefix Vₛ), then hot's import entries. For
//!   every tail check the copy is read **before** hot: a copy is always a
//!   prefix of a hot file that only grows, so this order can never produce
//!   a false `prefix`.
//! - **V3 Hot, report only.** Every sealed file on hot is checked against
//!   the ledger's `manifest` record (hot's own text when there is none),
//!   and every ordinary recorded path that hot lacks although its partition
//!   is not recorded removed is named. Nothing is recorded: ledger records
//!   name copy targets only, and the hot copy never counts (D11).
//! - **V4 Per copy target R** that presents its marker, in configuration
//!   order, one walk with `at_ms` = now at its start; the marker is re-read
//!   before every record about R.
//!   - *Data:* every path with an ordinary or held verdict and a `copy`
//!     record (P, R) in S: one finding per copy, `missing`, then
//!     `manifest`, then `size`, then `sha256`, the data always checked
//!     against the ledger's text. A self-consistent other file is a
//!     `manifest` finding with D5's wording, two sealed files under one
//!     path.
//!   - *k:* an ordinary path (the hot listing plus the ledger's manifests,
//!     minus lost, replaced and pending paths) without a `copy` record (P,
//!     R) is `copies`, except an archive part that no current hot entry
//!     lists (it waits for its entry) and a file on hot whose data mtime lies within
//!     G before now.
//!   - *Entries:* each current hot entry whose listed parts all have
//!     `copy` records for R older than G, and each supersede record whose
//!     mtime on hot lies outside G before now (a future mtime is never
//!     exempt): absent is `missing entry:<e>`, other bytes are `entry`. A
//!     superseded entry is not checked, so an open finding about one closes
//!     at the next pass over R (#210 C4). Without an archive configuration entries are not checked
//!     and archive parts get no k check; one `FAIL` line says so when the
//!     store holds archive parts.
//!   - *Copy ledger:* absent while S holds a `ledger-copy R` record is
//!     `missing copy-ledger`; otherwise a chain failure is `chain`, naming
//!     the record and its byte; otherwise a copy longer than hot's verified
//!     prefix, or one differing byte, is `prefix`.
//!   - *Journal:* absent while R has a `copy` record is `missing journal`;
//!     otherwise the copy's bytes are compared with hot's in bounded
//!     chunks, and any difference is `prefix`, naming the first differing
//!     byte (equivalent to comparing the SHA-256 of the copy with that of
//!     hot's first `len(copy)` bytes, and it locates the damage, which the
//!     repair needs).
//!   - *Records:* each recorded finding is appended as soon as it is found
//!     (the remover counts a damaged copy until its finding exists, and a
//!     walk takes long), with R's root and `at_ms`; after the walk comes
//!     `verify <at_ms> R <Vₛ> <recorded findings>`. An I/O error other than
//!     an absent file stops R: its findings so far stay, it gets no
//!     `verify` record, and a `FAIL` line names it.
//! - **V5 Unverified roots.** A copy target without its marker, or one
//!   stopped by V4, gets `FAIL: … not verified in this run`, and its
//!   recorded copies whose last good verification is older than now − P are
//!   `stale`: the count, the oldest time and the first path. The last good
//!   verification of (P, R) is the later of its latest `copy` record and
//!   the latest pass over R that covered that record and recorded no
//!   finding about P. A target without its marker has no id to look its
//!   copies up by, so the root ids that S records copies on and that no
//!   marked target carries stand in for it.
//! - **V6 Read-only cold roots** that present a marker: the copies they
//!   hold of recorded paths are checked as data, reported only.
//! - **V7 Not recorded:** `copies`, `stale`, and findings on hot or on
//!   read-only roots. They are re-derived from S on every run, so a record
//!   adds nothing, and their count scales with the store: a copy target
//!   added after one year lacks about 670 000 `copy` records, about 124 MB
//!   of `finding` records per run that every later ledger fold would read.
//! - **V8 G** = 2 × `capture.seal_interval_secs`: a pass runs after every
//!   seal pass and a pass running while a part seals leaves one wake-up
//!   behind, so a healthy replicator copies a part within two intervals. A
//!   future mtime is never exempt.
//! - **V9 Output.** One summary line per root, then the `FAIL:` lines:
//!   hot; each root in configuration order (data paths in path order, then
//!   entries, the copy ledger and the journal); then `stale`; then one line
//!   per file with fewer than k good copies. That last check needs every
//!   copy target verified in this run; otherwise the `not verified` and
//!   `stale` lines stand in for it, instead of one line per file of the
//!   whole store. `PASS` only without any `FAIL` line.
//! - **V10 Concurrency.** The verification lock excludes the repair, the
//!   only writer of a second `copy` record for a copy S records, so a
//!   finding never voids a copy made after S.

use super::replicate::{
    self, Entry, IMPORT_LEDGER_DIR, JOURNAL_DIR, LedgerAppend, META_DIR, hot_entries,
    is_archive_part,
};
use super::{DeviceProbe, check_devices, check_hot, truncation_line};
use crate::config::{ArchiveConfig, IngestConfig};
use mie_adapter_binance::archive::ledger::LedgerKind;
use mie_adapter_parquet::copy_ledger::{
    self, AppendError, FindingCode, Kind, LedgerCopyCheck, Record, Subject,
};
use mie_adapter_parquet::replica::{self, CopyCheck, CopyError, Side, TailSource};
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::verification::{CopyState, PathVerdict};
use mie_ports::raw::RawStoreError;
use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

#[cfg(test)]
pub(crate) mod tests;

/// Milliseconds per hour.
const HOUR_MS: i64 = 3_600_000;

#[cfg(test)]
thread_local! {
    /// Test hook: runs once right after the ledger snapshot S is read,
    /// before anything is appended (AW11's interleaving).
    pub(crate) static AFTER_SNAPSHOT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}

/// What one verification did: the summary and truncation lines, and every
/// `FAIL` line in the order of V9.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct VerifyReport {
    /// One summary line per root, then every copy-ledger truncation.
    pub lines: Vec<String>,
    /// Every finding and refusal; empty when the verification passed.
    pub problems: Vec<String>,
}

/// P from `--period-hours` (V1): a canonical decimal of at least 1, in
/// milliseconds.
///
/// # Errors
///
/// A description of the refusal.
pub fn parse_period_hours(text: &str) -> Result<i64, String> {
    let canonical =
        !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) && !text.starts_with('0');
    let hours: Option<i64> = canonical.then(|| text.parse().ok()).flatten();
    let refusal = || {
        format!(
            "--period-hours {text:?} is not a whole number of hours of at least 1: P bounds how \
             old a copy's last good verification may be, and 0 would make every copy stale at \
             once (ADR-062 D11)"
        )
    };
    hours
        .and_then(|hours| hours.checked_mul(HOUR_MS))
        .ok_or_else(refusal)
}

/// G (V8): twice the seal interval, in milliseconds.
pub(super) fn grace_ms(config: &IngestConfig) -> i64 {
    i64::try_from(config.capture.seal_interval_secs)
        .unwrap_or(i64::MAX / 2_000)
        .saturating_mul(2_000)
}

/// A root of the run.
struct Root {
    path: PathBuf,
    /// The marker's id; `None` without a marker.
    id: Option<String>,
    copy_target: bool,
    /// Why it was not verified, if it was not.
    unverified: Option<String>,
    /// Its `FAIL` lines, in V9 order.
    problems: Vec<String>,
    /// The data paths found bad on it in this run.
    bad: BTreeSet<String>,
    /// Its summary line.
    summary: Option<String>,
}

/// Why a root's walk ended early.
enum Stop {
    /// The root stopped (V4 I/O, a marker gone); the run goes on.
    Root(String),
    /// A ledger append failed: the run stops.
    Run(String),
}

/// The verification in progress.
struct Verify<'a> {
    config: &'a IngestConfig,
    hot: &'a Path,
    k: usize,
    archive: Option<ArchiveConfig>,
    period_ms: i64,
    grace_ms: i64,
    now: i64,
    now_ms: &'a dyn Fn() -> i64,
    ledger: &'a dyn LedgerAppend,
    state: CopyState,
    verified_len: u64,
    listing: Vec<String>,
    entries: Vec<Entry>,
    roots: Vec<Root>,
    hot_problems: Vec<String>,
    /// Hot's summary line.
    hot_line: String,
    /// Every copy-ledger truncation, as a line.
    truncations: Vec<String>,
}

/// Runs one verification (module docs). Every outcome is in the report.
pub fn run(
    config: &IngestConfig,
    archive: Option<&Path>,
    period_hours: &str,
    probe: &dyn DeviceProbe,
    now_ms: &dyn Fn() -> i64,
    ledger: &dyn LedgerAppend,
) -> VerifyReport {
    let mut report = VerifyReport::default();
    let hot = config.paths.raw_root.as_path();
    let cold_roots = &config.paths.cold_roots;
    let mut refuse = |problem: String| {
        report.problems.push(problem);
    };
    if cold_roots.is_empty() {
        refuse(
            "paths.cold_roots is empty: a single-root store has no copies to verify (ADR-062 D11)"
                .to_owned(),
        );
        return report;
    }
    if let Err(error) = check_hot(hot, cold_roots) {
        refuse(error);
        return report;
    }
    let archive = match replicate::archive_config(config, archive) {
        Ok(archive) => archive,
        Err(error) => {
            refuse(error);
            return report;
        }
    };
    let period_ms = match parse_period_hours(period_hours) {
        Ok(period) => period,
        Err(error) => {
            refuse(error);
            return report;
        }
    };
    let _lock = match replica::try_verify_lock(hot) {
        Ok(lock) => lock,
        Err(error) => {
            refuse(error.to_string());
            return report;
        }
    };
    // V2: the listing, then S, then the entries.
    let listing = match replica::list_sealed(hot) {
        Ok(listing) => listing,
        Err(error) => {
            refuse(format!("cannot list hot's sealed files: {error}"));
            return report;
        }
    };
    let ledger_path = copy_ledger::ledger_path(hot);
    let snapshot = match copy_ledger::read(hot) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            refuse(format!(
                "copy ledger {} does not exist: the copy-ledger rebuild restores it (ADR-062 D7)",
                ledger_path.display()
            ));
            return report;
        }
        Err(error) => {
            refuse(format!("hot's copy ledger: {error}"));
            return report;
        }
    };
    #[cfg(test)]
    if let Some(hook) = AFTER_SNAPSHOT.with(|hook| hook.borrow_mut().take()) {
        hook();
    }
    let state = match CopyState::fold(&snapshot) {
        Ok(state) => state,
        Err(error) => {
            refuse(format!("copy ledger {}: {error}", ledger_path.display()));
            return report;
        }
    };
    let mut hot_problems = Vec::new();
    let entries = match &archive {
        Some(archive) => {
            let found = hot_entries(&archive.paths.import_ledger);
            hot_problems.extend(found.problems);
            found.entries
        }
        None => Vec::new(),
    };

    // The roots and their markers.
    let mut roots: Vec<Root> = Vec::new();
    for cold in cold_roots {
        let (id, unverified) = match root_marker::read(&cold.path) {
            Ok(Some(id)) if id == HOT_ID => {
                refuse(format!(
                    "cold root {} carries hot's marker (id {HOT_ID:?}), the id of raw_root {}: \
                     the same root under two names, or a copied marker (ADR-062 D7)",
                    cold.path.display(),
                    hot.display()
                ));
                return report;
            }
            Ok(Some(id)) => {
                if let Some(other) = roots.iter().find(|r| r.id.as_deref() == Some(&id)) {
                    refuse(format!(
                        "cold roots {} and {} both carry the id {id:?}: two spellings of one \
                         root, or a copied marker (ADR-062 D7)",
                        other.path.display(),
                        cold.path.display()
                    ));
                    return report;
                }
                (Some(id), None)
            }
            Ok(None) => (
                None,
                Some(
                    "it has no root marker (an unmounted mount point is an empty directory; \
                     ADR-062 D11)"
                        .to_owned(),
                ),
            ),
            Err(error) => (
                None,
                Some(format!("its root marker cannot be read ({error})")),
            ),
        };
        roots.push(Root {
            path: cold.path.clone(),
            id,
            copy_target: cold.copy_target,
            unverified,
            problems: Vec::new(),
            bad: BTreeSet::new(),
            summary: None,
        });
    }
    let marked: Vec<&Path> = roots
        .iter()
        .filter(|root| root.copy_target && root.id.is_some())
        .map(|root| root.path.as_path())
        .collect();
    if let Err(error) = check_devices(hot, &marked, probe) {
        refuse(error);
        return report;
    }

    let mut verify = Verify {
        config,
        hot,
        k: cold_roots.iter().filter(|root| root.copy_target).count(),
        archive,
        period_ms,
        grace_ms: grace_ms(config),
        now: now_ms(),
        now_ms,
        ledger,
        verified_len: snapshot.verified_len,
        state,
        listing,
        entries,
        roots,
        hot_problems,
        hot_line: String::new(),
        truncations: Vec::new(),
    };
    let stopped = verify.verify_all();
    let mut report = verify.report();
    if let Some(error) = stopped {
        report.problems.push(error);
    }
    report
}

impl Verify<'_> {
    /// V3 to V6, in order. `Some` with the reason when a failed append
    /// stopped the run.
    fn verify_all(&mut self) -> Option<String> {
        self.verify_hot();
        for i in 0..self.roots.len() {
            if self.roots[i].unverified.is_some() || self.roots[i].id.is_none() {
                continue;
            }
            let result = if self.roots[i].copy_target {
                self.verify_target(i)
            } else {
                self.verify_read_only(i)
            };
            match result {
                Ok(()) => {}
                Err(Stop::Root(why)) => self.roots[i].unverified = Some(why),
                Err(Stop::Run(why)) => {
                    self.roots[i].unverified = Some(why.clone());
                    return Some(why);
                }
            }
        }
        None
    }

    /// The ordinary paths of the k check: the hot listing plus the ledger's
    /// manifests, minus lost, replaced and pending paths.
    fn ordinary_paths(&self) -> BTreeSet<String> {
        let excluded = |verdict: Option<PathVerdict>| {
            matches!(
                verdict,
                Some(PathVerdict::Lost | PathVerdict::Replaced | PathVerdict::Pending)
            )
        };
        self.listing
            .iter()
            .map(String::as_str)
            .chain(
                self.state
                    .paths()
                    .filter(|(path, _)| self.state.ledger_text(path).is_some())
                    .map(|(path, _)| path),
            )
            .filter(|path| !excluded(self.state.verdict(path)))
            .map(str::to_owned)
            .collect()
    }

    /// Whether `path` is exempt from the k check: an archive part without
    /// an entry listing it, any archive part without an archive
    /// configuration, or a file on hot whose data mtime lies within G
    /// before now.
    fn k_exempt(&self, path: &str) -> bool {
        if is_archive_part(path) {
            if self.archive.is_none() {
                return true;
            }
            let listed = self
                .entries
                .iter()
                .any(|entry| entry.files.iter().any(|(file, _)| file == path));
            if !listed {
                return true;
            }
        }
        self.within_grace(&self.hot.join(path))
    }

    /// Whether `file`'s mtime lies within G before now (V8): a future mtime
    /// is never within it.
    fn within_grace(&self, file: &Path) -> bool {
        let modified = fs::metadata(file)
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .and_then(|age| i64::try_from(age.as_millis()).ok());
        modified.is_some_and(|mtime| mtime <= self.now && self.now - mtime < self.grace_ms)
    }

    /// V3: hot, report only.
    fn verify_hot(&mut self) {
        let mut problems = Vec::new();
        let listed: BTreeSet<&str> = self.listing.iter().map(String::as_str).collect();
        for path in &self.listing {
            let removed = self.state.is_removed(partition_of(path));
            let text = match self.state.ledger_text(path) {
                Some(text) => text.to_owned(),
                None => match replica::sealed_manifest(self.hot, path) {
                    Ok(Some((text, _))) => text,
                    // Removed from hot since the listing.
                    Ok(None) => continue,
                    Err(error) => {
                        problems.push(format!("{path} on hot: {error}"));
                        continue;
                    }
                },
            };
            match replica::check_sealed_copy(self.hot, path, &text) {
                Ok(CopyCheck::Good) => {}
                Ok(CopyCheck::Missing) if removed => {}
                Ok(check) => problems.push(format!(
                    "{path} on hot ({}): {}",
                    self.hot.display(),
                    describe(check, path)
                )),
                Err(error) => problems.push(format!("{path} on hot: {error}")),
            }
        }
        let recorded: Vec<&str> = self
            .state
            .paths()
            .filter(|(path, verdict)| {
                *verdict == PathVerdict::Ordinary
                    && !listed.contains(path)
                    && !self.state.is_removed(partition_of(path))
            })
            .map(|(path, _)| path)
            .collect();
        for path in recorded {
            // Sealed since the listing, or recalled: only a file still
            // absent is named.
            if let Ok(Some(_)) = replica::sealed_manifest(self.hot, path) {
                continue;
            }
            problems.push(format!(
                "{path}: hot ({}) lacks it although its partition is not recorded removed \
                 (ADR-062 D6)",
                self.hot.display()
            ));
        }
        let archive_parts = self.listing.iter().any(|path| is_archive_part(path))
            || self.state.paths().any(|(path, _)| is_archive_part(path));
        if self.archive.is_none() && archive_parts {
            problems.push(
                "the store holds archive parts, and no archive configuration is given \
                 (--archive-config, else [history] archive_config): import-ledger entries are \
                 not checked and archive parts get no check of their k copies"
                    .to_owned(),
            );
        }
        self.hot_problems.extend(problems);
        self.hot_line = format!(
            "hot {}: {} sealed file(s) checked",
            self.hot.display(),
            self.listing.len()
        );
    }

    /// Whether root `i` still presents its marker.
    fn marker_present(&self, i: usize) -> Result<(), Stop> {
        let root = &self.roots[i];
        match root_marker::read(&root.path) {
            Ok(Some(id)) if Some(&id) == root.id.as_ref() => Ok(()),
            Ok(Some(other)) => Err(Stop::Root(format!(
                "its marker now carries the id {other:?}"
            ))),
            Ok(None) => Err(Stop::Root(
                "its marker is gone (an unmounted mount point?); nothing more is recorded for it \
                 (ADR-062 D11)"
                    .to_owned(),
            )),
            Err(error) => Err(Stop::Root(format!("its marker cannot be read: {error}"))),
        }
    }

    /// Appends one record about root `i`, after re-reading its marker.
    fn record(&mut self, i: usize, at_ms: i64, kind: Kind) -> Result<(), Stop> {
        self.marker_present(i)?;
        let record = Record { at_ms, kind };
        let (truncation, result) = match self.ledger.append(&record) {
            Ok(outcome) => (outcome.truncation, Ok(())),
            Err(AppendError { error, truncation }) => (truncation, Err(error)),
        };
        if let Some(truncation) = truncation {
            self.truncations
                .push(truncation_line(self.ledger.path(), truncation));
        }
        result.map_err(|error| {
            Stop::Run(format!(
                "the {} append to the copy ledger {} failed, so the verification stops: {error}",
                record.kind.name(),
                self.ledger.path().display()
            ))
        })
    }

    /// Records one finding about root `i` and names it.
    fn finding(
        &mut self,
        i: usize,
        at_ms: i64,
        code: FindingCode,
        subject: Subject,
        line: String,
        recorded: &mut u64,
    ) -> Result<(), Stop> {
        let id = self.roots[i].id.clone().unwrap_or_default();
        self.record(
            i,
            at_ms,
            Kind::Finding {
                root: id,
                code,
                subject,
            },
        )?;
        *recorded += 1;
        self.roots[i].problems.push(line);
        Ok(())
    }

    /// The name of root `i` in a line.
    fn name(&self, i: usize) -> String {
        let root = &self.roots[i];
        let kind = if root.copy_target {
            "copy target"
        } else {
            "read-only root"
        };
        match &root.id {
            Some(id) => format!("{kind} {} ({id})", root.path.display()),
            None => format!("{kind} {}", root.path.display()),
        }
    }

    /// V4 over the copy target `i`.
    fn verify_target(&mut self, i: usize) -> Result<(), Stop> {
        let at_ms = (self.now_ms)();
        let id = self.roots[i].id.clone().unwrap_or_default();
        let root = self.roots[i].path.clone();
        let name = self.name(i);
        let mut recorded = 0u64;

        // Data and k, in path order.
        let data: BTreeSet<String> = self
            .state
            .copied_paths()
            .filter(|path| {
                self.state
                    .copy(path, &id)
                    .is_some_and(|copy| copy.latest.is_some())
                    && matches!(
                        self.state.verdict(path),
                        Some(PathVerdict::Ordinary | PathVerdict::Held)
                    )
                    && self.state.ledger_text(path).is_some()
            })
            .map(str::to_owned)
            .collect();
        let ordinary = self.ordinary_paths();
        let mut checked = 0usize;
        for path in data.union(&ordinary) {
            if data.contains(path) {
                checked += 1;
                let text = self
                    .state
                    .ledger_text(path)
                    .expect("a data path has a ledger text")
                    .to_owned();
                let check = match replica::check_sealed_copy(&root, path, &text) {
                    Ok(check) => check,
                    Err(RawStoreError::Io(error)) => {
                        return Err(Stop::Root(format!("reading {path}: {error}")));
                    }
                    Err(error) => {
                        self.roots[i]
                            .problems
                            .push(format!("{path} on {name}: {error}"));
                        continue;
                    }
                };
                let code = match check {
                    CopyCheck::Good => continue,
                    CopyCheck::Missing => FindingCode::Missing,
                    CopyCheck::Manifest { .. } => FindingCode::Manifest,
                    CopyCheck::Size => FindingCode::Size,
                    CopyCheck::Sha256 => FindingCode::Sha256,
                };
                let line = format!("{path} on {name}: {}", describe(check, path));
                self.finding(
                    i,
                    at_ms,
                    code,
                    Subject::File(path.clone()),
                    line,
                    &mut recorded,
                )?;
                self.roots[i].bad.insert(path.clone());
            } else if !self
                .state
                .copy(path, &id)
                .is_some_and(|copy| copy.latest.is_some())
                && !self.k_exempt(path)
            {
                self.roots[i].problems.push(format!(
                    "{path} on {name}: copies: the copy ledger records no copy of it on {id}, so it \
                     has fewer than k good copies (ADR-062 D11)"
                ));
            }
        }

        // Entries.
        let mut entries_checked = 0usize;
        if self.archive.is_some() {
            let entries: Vec<(String, String, String, PathBuf)> = self
                .entries
                .iter()
                .filter(|entry| match entry.kind {
                    // A supersede record has no part: its own mtime on hot
                    // gives the replicator its G (#210 C4).
                    LedgerKind::Supersede => !self.within_grace(&entry.path),
                    LedgerKind::Entry => entry.files.iter().all(|(file, _)| {
                        self.state
                            .copy(file, &id)
                            .and_then(|copy| copy.latest)
                            .is_some_and(|(_, at)| at <= self.now - self.grace_ms)
                    }),
                })
                .map(|entry| {
                    (
                        entry.name.clone(),
                        entry.segment.clone(),
                        entry.file_name.clone(),
                        entry.path.clone(),
                    )
                })
                .collect();
            for (entry, segment, file_name, hot_path) in entries {
                entries_checked += 1;
                let copy_path = root
                    .join(META_DIR)
                    .join(IMPORT_LEDGER_DIR)
                    .join(&segment)
                    .join(&file_name);
                let copy = match fs::read(&copy_path) {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == ErrorKind::NotFound => None,
                    Err(error) => {
                        return Err(Stop::Root(format!(
                            "reading {}: {error}",
                            copy_path.display()
                        )));
                    }
                };
                let hot_bytes = match fs::read(&hot_path) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        self.hot_problems.push(format!(
                            "import entry {entry} on hot ({}): {error}",
                            hot_path.display()
                        ));
                        continue;
                    }
                };
                let (code, what) = match copy {
                    None => (FindingCode::Missing, "missing: the copy does not exist"),
                    Some(bytes) if bytes != hot_bytes => (
                        FindingCode::Entry,
                        "entry: the copy differs from hot's entry byte for byte",
                    ),
                    Some(_) => continue,
                };
                let line = format!(
                    "import entry {entry} on {name} ({}): {what} (ADR-062 D11)",
                    copy_path.display()
                );
                self.finding(
                    i,
                    at_ms,
                    code,
                    Subject::Entry(entry.clone()),
                    line,
                    &mut recorded,
                )?;
            }
        }

        // The copy ledger.
        let ledger_copy = copy_ledger::ledger_path(&root);
        let check = match copy_ledger::check_copy(self.hot, &root) {
            Ok(check) => check,
            Err(RawStoreError::Io(error)) => {
                return Err(Stop::Root(format!("reading its copy ledger: {error}")));
            }
            Err(error) => {
                self.hot_problems.push(format!(
                    "hot's copy ledger, read for the copy on {id}: {error}"
                ));
                LedgerCopyCheck::Good
            }
        };
        let ledger_finding = match check {
            LedgerCopyCheck::Good => None,
            LedgerCopyCheck::Absent if !self.state.has_ledger_copy_record(&id) => None,
            LedgerCopyCheck::Absent => Some((
                FindingCode::Missing,
                "missing: the copy does not exist although the copy ledger records a ledger-copy \
                 for it"
                    .to_owned(),
            )),
            LedgerCopyCheck::Chain { record, offset } => Some((
                FindingCode::Chain,
                format!(
                    "chain: the hash chain fails at record {record} (byte {offset}); record 0 is \
                     the header"
                ),
            )),
            LedgerCopyCheck::Diverged { offset } => Some((
                FindingCode::Prefix,
                format!(
                    "prefix: byte {offset} differs from hot's copy ledger, so the copy is not a \
                     byte prefix of it"
                ),
            )),
            LedgerCopyCheck::Tail { offset } => Some((
                FindingCode::Prefix,
                format!(
                    "prefix: its unterminated tail from byte {offset} differs from hot's copy \
                     ledger (a crashed extension); every complete record equals hot's"
                ),
            )),
            LedgerCopyCheck::Longer { len, verified } => Some((
                FindingCode::Prefix,
                format!(
                    "prefix: {len} bytes, longer than the {verified} bytes of hot's verified \
                     prefix"
                ),
            )),
        };
        if let Some((code, what)) = ledger_finding {
            let line = format!(
                "copy ledger on {name} ({}): {what} (ADR-062 D11)",
                ledger_copy.display()
            );
            self.finding(i, at_ms, code, Subject::CopyLedger, line, &mut recorded)?;
        }

        // The journal.
        let journal = self.config.paths.journal.clone();
        if let Some(file_name) = journal.file_name() {
            let copy = root.join(META_DIR).join(JOURNAL_DIR).join(file_name);
            let journal_finding = self.journal_check(&copy, &journal, &id)?;
            if let Some((code, what)) = journal_finding {
                let line = format!(
                    "journal on {name} ({}): {what} (ADR-062 D11)",
                    copy.display()
                );
                self.finding(i, at_ms, code, Subject::Journal, line, &mut recorded)?;
            }
        }

        self.record(
            i,
            at_ms,
            Kind::Verify {
                root: id.clone(),
                prefix_bytes: self.verified_len,
                findings: recorded,
            },
        )?;
        self.roots[i].summary = Some(format!(
            "{name}: {checked} data cop(ies), {entries_checked} import entr(ies), the copy ledger \
             and the journal checked; {recorded} finding(s) recorded with a verify record over {} \
             byte(s) of the copy ledger",
            self.verified_len
        ));
        Ok(())
    }

    /// The journal check of V4: the copy read before hot.
    fn journal_check(
        &self,
        copy: &Path,
        journal: &Path,
        id: &str,
    ) -> Result<Option<(FindingCode, String)>, Stop> {
        let copy_len = match fs::metadata(copy) {
            Ok(metadata) => Some(metadata.len()),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => {
                return Err(Stop::Root(format!(
                    "inspecting {}: {error}",
                    copy.display()
                )));
            }
        };
        let hot_len = match fs::metadata(journal) {
            Ok(metadata) => Some(metadata.len()),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => {
                return Err(Stop::Root(format!(
                    "inspecting hot's journal {}: {error}",
                    journal.display()
                )));
            }
        };
        let Some(copy_len) = copy_len else {
            return Ok(
                (hot_len.is_some() && self.state.has_copies_on(id)).then(|| {
                    (
                    FindingCode::Missing,
                    "missing: the copy does not exist although the copy ledger records copies on \
                     this target"
                        .to_owned(),
                )
                }),
            );
        };
        let hot_len = hot_len.unwrap_or(0);
        if copy_len > hot_len {
            return Ok(Some((
                FindingCode::Prefix,
                format!(
                    "prefix: {copy_len} bytes, longer than hot's journal ({hot_len} bytes), so \
                     byte {hot_len} is the first that differs"
                ),
            )));
        }
        match replica::first_difference(copy, TailSource::File(journal), copy_len) {
            Ok(None) => Ok(None),
            Ok(Some(offset)) => Ok(Some((
                FindingCode::Prefix,
                format!(
                    "prefix: byte {offset} differs from hot's journal, so the copy is not a byte \
                     prefix of it"
                ),
            ))),
            Err(CopyError {
                side: Side::Destination,
                error,
            }) => Err(Stop::Root(format!("reading {}: {error}", copy.display()))),
            Err(CopyError { error, .. }) => Err(Stop::Root(format!(
                "reading hot's journal {}: {error}",
                journal.display()
            ))),
        }
    }

    /// V6 over the read-only root `i`: report only.
    fn verify_read_only(&mut self, i: usize) -> Result<(), Stop> {
        let root = self.roots[i].path.clone();
        let name = self.name(i);
        let paths: Vec<(String, String)> = self
            .state
            .paths()
            .filter(|(_, verdict)| matches!(verdict, PathVerdict::Ordinary | PathVerdict::Held))
            .filter_map(|(path, _)| {
                self.state
                    .ledger_text(path)
                    .map(|text| (path.to_owned(), text.to_owned()))
            })
            .collect();
        let mut checked = 0usize;
        for (path, text) in paths {
            match replica::check_sealed_copy(&root, &path, &text) {
                Ok(CopyCheck::Good) => checked += 1,
                Ok(CopyCheck::Missing) => {}
                Ok(check) => {
                    checked += 1;
                    self.roots[i]
                        .problems
                        .push(format!("{path} on {name}: {}", describe(check, &path)));
                }
                Err(RawStoreError::Io(error)) => {
                    return Err(Stop::Root(format!("reading {path}: {error}")));
                }
                Err(error) => self.roots[i]
                    .problems
                    .push(format!("{path} on {name}: {error}")),
            }
        }
        self.roots[i].summary = Some(format!("{name}: {checked} cop(ies) checked, reported only"));
        Ok(())
    }

    /// V5: the stale copies of an unverified copy target, by the root
    /// ids that stand in for it.
    fn stale_lines(&self, i: usize, presented: &BTreeSet<String>) -> Vec<String> {
        let root = &self.roots[i];
        let ids: Vec<String> = match &root.id {
            Some(id) => vec![id.clone()],
            None => {
                let mut ids = BTreeSet::new();
                for path in self.state.copied_paths() {
                    for (id, copy) in self.state.copies_of(path) {
                        if copy.latest.is_some() && id != HOT_ID && !presented.contains(id) {
                            ids.insert(id.to_owned());
                        }
                    }
                }
                ids.into_iter().collect()
            }
        };
        let limit = self.now.saturating_sub(self.period_ms);
        let mut lines = Vec::new();
        for id in ids {
            let (mut count, mut oldest, mut first): (u64, i64, Option<&str>) = (0, i64::MAX, None);
            for path in self.state.copied_paths() {
                let counts = self
                    .state
                    .copy(path, &id)
                    .is_some_and(|copy| copy.valid_end.is_some() && copy.latest.is_some())
                    && matches!(
                        self.state.verdict(path),
                        Some(PathVerdict::Ordinary | PathVerdict::Held)
                    );
                if !counts {
                    continue;
                }
                let Some(last) = self.state.last_good_verification(path, &id) else {
                    continue;
                };
                if last < limit {
                    count += 1;
                    oldest = oldest.min(last);
                    first.get_or_insert(path);
                }
            }
            if let Some(first) = first {
                lines.push(format!(
                    "{}: stale: {count} cop(ies) on {id} whose last good verification is older \
                     than P ({} h); the oldest at {oldest} ms, the first {first} (ADR-062 D11)",
                    self.name(i),
                    self.period_ms / HOUR_MS
                ));
            }
        }
        lines
    }

    /// The report in the order of V9.
    fn report(mut self) -> VerifyReport {
        let mut problems = std::mem::take(&mut self.hot_problems);
        let presented: BTreeSet<String> = self
            .roots
            .iter()
            .filter(|root| root.copy_target)
            .filter_map(|root| root.id.clone())
            .collect();
        let mut lines = vec![std::mem::take(&mut self.hot_line)];
        let mut stale = Vec::new();
        let mut all_verified = true;
        for i in 0..self.roots.len() {
            problems.extend(std::mem::take(&mut self.roots[i].problems));
            let root = &self.roots[i];
            match &root.unverified {
                Some(why) if root.copy_target => {
                    all_verified = false;
                    let line = format!("{}: not verified in this run: {why}", self.name(i));
                    lines.push(line.clone());
                    problems.push(line);
                    stale.extend(self.stale_lines(i, &presented));
                }
                Some(why) => lines.push(format!("{}: not checked: {why}", self.name(i))),
                None => lines.extend(root.summary.clone()),
            }
        }
        problems.extend(stale);
        if all_verified {
            problems.extend(self.k_lines());
        }
        lines.extend(self.truncations);
        VerifyReport { lines, problems }
    }

    /// V9's last block: every ordinary file with fewer than k good copies
    /// on the copy targets verified in this run.
    fn k_lines(&self) -> Vec<String> {
        let targets: Vec<(&str, &Root)> = self
            .roots
            .iter()
            .filter(|root| root.copy_target)
            .filter_map(|root| root.id.as_deref().map(|id| (id, root)))
            .collect();
        let mut lines = Vec::new();
        for path in self.ordinary_paths() {
            let mut short = Vec::new();
            for (id, root) in &targets {
                let why = match self.state.copy(&path, id) {
                    Some(copy) if copy.latest.is_none() => "no copy record",
                    None => "no copy record",
                    Some(copy) if copy.valid_end.is_none() => {
                        "an open finding; run `mie store repair`"
                    }
                    Some(_) if root.bad.contains(&path) => "found bad in this run",
                    Some(_) => continue,
                };
                short.push(format!("{id} ({why})"));
            }
            if short.is_empty() || self.k_exempt(&path) {
                continue;
            }
            lines.push(format!(
                "{path}: {} of {} good copies on distinct copy targets; not good on {} (ADR-062 \
                 D11)",
                self.k - short.len().min(self.k),
                self.k,
                short.join(", ")
            ));
        }
        lines
    }
}

/// The partition directory of a data path.
fn partition_of(path: &str) -> &str {
    path.rsplit_once('/')
        .map_or(path, |(partition, _)| partition)
}

/// The text of a copy check that found something.
fn describe(check: CopyCheck, path: &str) -> String {
    match check {
        CopyCheck::Good => "good".to_owned(),
        CopyCheck::Missing => {
            "missing: no data file (no file at all, or a manifest without its data)".to_owned()
        }
        CopyCheck::Manifest { other_file: true } => format!(
            "manifest: its manifest differs from the copy ledger's manifest record and its data \
             matches that other manifest: two sealed files under one path {path}, which must be \
             byte-identical on every root (ADR-062 D5)"
        ),
        CopyCheck::Manifest { other_file: false } => {
            "manifest: its manifest is absent or differs from the copy ledger's manifest record \
             (ADR-062 D5)"
                .to_owned()
        }
        CopyCheck::Size => "size: its size differs from the copy ledger's manifest".to_owned(),
        CopyCheck::Sha256 => {
            "sha256: its SHA-256 differs from the copy ledger's manifest".to_owned()
        }
    }
}
