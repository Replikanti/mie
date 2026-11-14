//! The importer's side of the replace resolution (ADR-062 D11, proposed;
//! #211), shared with `mie store resolve` and the rebuild.
//!
//! - **I1 Gate** ([`check_replaces`]). `mie archive-import`, its dry runs
//!   too, refuses while a supersede record on hot with a quarantine has a
//!   moved path whose data or manifest is in the layout of hot or of a
//!   configured copy target, or whose copy-ledger verdict is neither
//!   pending nor replaced; and, while a moved path is not yet replaced,
//!   while a copy target presents no readable marker (an unmounted root
//!   reads as empty, so its layout cannot be confirmed free). The layouts
//!   of hot and every copy target, not hot's alone, are the commit point:
//!   the parts of a day already taken off hot live on cold roots only, and
//!   a replace that stopped between two of them must keep the importer
//!   halted after a rebuild from an empty start too, which has no hold
//!   record (review of #215, finding 1). A read-only root is skipped, its
//!   marker and its layout: mie never writes it (D4), so a copy there can
//!   never move, and the copy ledger keeps the path pending or replaced,
//!   so no `select` lists it (#216). A record that does not parse is
//!   skipped here: the import of its own archive file fails closed on it
//!   (#210 C1).
//! - **I3 Replacement records** ([`record_replacements`]). For each such
//!   record S_n, E_g is its archive file's entry of the lowest generation
//!   g ≥ n on hot ([`replacement_generation`]). Once E_g parses and every
//!   file it lists is sealed on hot with the listed SHA-256 (hot's
//!   manifest, else an ordinary copy-ledger text), one `replaced <path>
//!   <segment>/<E_g>` record is appended per moved path that is still
//!   pending, all with one `at_ms`; otherwise the missing file is named and
//!   nothing is appended (D7 *Write order*: `replaced` only after the
//!   entry and its parts are sealed). It also waits while the replace is
//!   unfinished on hot or a copy target (I1's rule), so a `replaced`
//!   record never names a path such a layout still holds, which a rebuild
//!   from an empty start could not re-derive (Q4). The importer runs it at
//!   its start and after its run, never in a dry run, so a crash between
//!   the entry and the records is completed by the next import (or by a
//!   rebuild, Q4).
//! - **The text rule** ([`text_rule`]), shared by the replace (W2) and the
//!   rebuild (Q4): the one distinct manifest text over the roots' layouts,
//!   read-only roots included, and quarantine copies; with several, the
//!   one whose SHA-256 the superseded entry lists; otherwise none, and the
//!   path then covers its whole stream-day in `select`, failing more
//!   selections, never fewer.
//! - **RQ8 proposals** ([`replace_lines`]): one line per supersede record
//!   on hot that carries a quarantine; an incomplete replace (unfinished
//!   on hot or a copy target, by I1's rule) is a problem naming `mie store
//!   resolve --file F`. A marked read-only root whose layout still holds a
//!   moved path gets an info line ([`read_only_line`]), never a problem of
//!   its own. A moved path whose data file no root holds in its
//!   `.quarantine/<id>/`, a read-only root included, and no layout of hot
//!   or of a copy target holds, while a read-only layout does or a
//!   read-only root presents no readable marker, is a problem with the
//!   remedy ([`ReplaceRoots::read_only_alone`], the h = 0 rule of review
//!   of #221 and its re-review, findings 3 and 4, decided by [`h_zero`]),
//!   and its replace is never reported complete. I1 does not apply that
//!   rule: no byte is lost in that state and the remedy works after
//!   `replaced` (ADR-062 D11 *As amended (#216)*).

use mie_adapter_binance::archive::ledger::{
    ImportLedger, LedgerDir, LedgerKind, LedgerName, SupersedeRecord, replacement_generation,
};
use mie_adapter_parquet::copy_ledger::{self, CopyLedger, Kind, Record};
use mie_adapter_parquet::replica;
use mie_adapter_parquet::verification::{CopyState, PathVerdict};
use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// Every root a replace may find a moved path on: hot, then each
/// configured cold root with its marker's id as read now (`None` without a
/// readable marker) and whether it counts toward the replace's commit
/// point (hot and the copy targets; a read-only root never does, #216).
#[derive(Debug, Clone)]
pub struct ReplaceRoots {
    roots: Vec<ReplaceRoot>,
}

/// One root of [`ReplaceRoots`].
#[derive(Debug, Clone)]
struct ReplaceRoot {
    /// Its marker's id, else its path.
    label: String,
    path: PathBuf,
    /// Its marker's id as read now.
    id: Option<String>,
    /// Hot or a copy target: the commit point.
    counts: bool,
}

impl ReplaceRoots {
    /// Hot at `hot` and the configured `cold` roots, markers read now.
    pub fn read(hot: &Path, cold: &[crate::config::ColdRoot]) -> Self {
        let mut roots = vec![ReplaceRoot {
            label: "hot".to_owned(),
            path: hot.to_owned(),
            id: Some("hot".to_owned()),
            counts: true,
        }];
        for root in cold {
            let id = mie_adapter_parquet::root_marker::read(&root.path)
                .ok()
                .flatten();
            let label = id
                .clone()
                .unwrap_or_else(|| root.path.display().to_string());
            roots.push(ReplaceRoot {
                label,
                path: root.path.clone(),
                id,
                counts: root.copy_target,
            });
        }
        Self { roots }
    }

    /// Why the replace of `record` is unfinished on hot and the copy
    /// targets, if it is: one whose layout still holds a moved path, or,
    /// with `require_markers`, a copy target without a readable marker,
    /// whose layout cannot be confirmed free. A read-only root is skipped,
    /// marker and layout: mie never writes it (ADR-062 D4), so its copy can
    /// never move and must not hold the replace back (#216).
    ///
    /// # Errors
    ///
    /// An I/O error inspecting hot or a copy target.
    pub fn unfinished(
        &self,
        record: &SupersedeRecord,
        require_markers: bool,
    ) -> Result<Option<String>, String> {
        let Some(quarantine) = &record.quarantine else {
            return Ok(None);
        };
        for root in self.roots.iter().filter(|root| root.counts) {
            let label = &root.label;
            if root.id.is_none() {
                if require_markers {
                    return Ok(Some(format!(
                        "copy target {label} presents no readable marker, so its layout cannot \
                         be confirmed free of the moved paths (an unmounted root reads as empty)"
                    )));
                }
                continue;
            }
            for path in &quarantine.moved {
                if in_layout(&root.path, path)
                    .map_err(|e| format!("inspect {path} on {label}: {e}"))?
                {
                    return Ok(Some(format!("{path} is still in {label}'s layout")));
                }
            }
        }
        Ok(None)
    }

    /// The info lines of `record` ([`read_only_lines`]) over the read-only
    /// roots that present a marker; an unmarked one is neither inspected
    /// nor named.
    pub fn read_only_holders(&self, record: &SupersedeRecord) -> Vec<String> {
        let Some(quarantine) = &record.quarantine else {
            return Vec::new();
        };
        let read_only: Vec<(&str, &Path)> = self
            .roots
            .iter()
            .filter(|root| !root.counts)
            .filter_map(|root| root.id.as_deref().map(|id| (id, root.path.as_path())))
            .collect();
        read_only_lines(&read_only, &quarantine.moved, &quarantine.id)
    }

    /// The h = 0 rule over `record` ([`read_only_alone`]): one problem
    /// ([`read_only_alone_line`]) per moved path whose data file no root
    /// holds in its `.quarantine/<id>/`, and no layout of hot or of a marked
    /// copy target, while the layout of a read-only root does or a
    /// read-only root presents no readable marker. An unmarked read-only
    /// root counts as a possible holder only then (fail closed: an
    /// unmounted root reads as empty; re-review of #221, finding 4); an
    /// unmarked copy target is not inspected, since it already makes the
    /// replace incomplete.
    ///
    /// # Errors
    ///
    /// An I/O error inspecting hot or a copy target.
    pub fn read_only_alone(&self, record: &SupersedeRecord) -> Result<Vec<String>, String> {
        let Some(quarantine) = &record.quarantine else {
            return Ok(Vec::new());
        };
        let writable: Vec<(&str, &Path)> = self
            .roots
            .iter()
            .filter(|root| root.counts)
            .filter_map(|root| root.id.as_deref().map(|id| (id, root.path.as_path())))
            .collect();
        let read_only: Vec<(&str, Option<&Path>)> = self
            .roots
            .iter()
            .filter(|root| !root.counts)
            .map(|root| {
                let marked = root.id.is_some().then_some(root.path.as_path());
                (root.label.as_str(), marked)
            })
            .collect();
        let mut problems = Vec::new();
        for path in &quarantine.moved {
            let alone = read_only_alone(&writable, &read_only, path, &quarantine.id)?;
            if !alone.is_empty() {
                problems.push(read_only_alone_line(
                    path,
                    &alone,
                    &quarantine.id,
                    &record.archive,
                ));
            }
        }
        Ok(problems)
    }
}

/// The h = 0 rule (ADR-062 D11 *As amended (#216)*), the one predicate
/// RQ2, the resume (RQ7), the proposals (RQ8) and the rebuild and the
/// restore (Q5) decide it with, so they cannot drift. For one moved path,
/// h counts the roots holding its data file in their `.quarantine/<id>/`,
/// any root, a read-only one included (D5 never deletes a quarantine
/// copy), and hot and the copy targets whose layout holds it; r names the
/// read-only roots whose layout holds it or that cannot be confirmed not
/// to. The path fails, and r is returned, when h = 0 and r ≥ 1; otherwise
/// nothing is.
///
/// Each caller supplies the three lookups; they run in this order and stop
/// at the first that makes h ≥ 1, so a read-only root is looked at only
/// when no writable root counts: `writable`, whether hot or a copy target
/// holds the data file in its quarantine or its layout; then
/// `read_only_quarantined`, whether a read-only root holds it in its
/// quarantine; then `read_only_layout`, r.
///
/// # Errors
///
/// The first error of a lookup.
pub fn h_zero<E>(
    writable: impl FnOnce() -> Result<bool, E>,
    read_only_quarantined: impl FnOnce() -> Result<bool, E>,
    read_only_layout: impl FnOnce() -> Result<Vec<String>, E>,
) -> Result<Vec<String>, E> {
    if writable()? || read_only_quarantined()? {
        return Ok(Vec::new());
    }
    read_only_layout()
}

/// The h = 0 rule ([`h_zero`]) of the resume (RQ7) and the proposals (RQ8)
/// over the roots themselves: `writable` (hot and the copy targets) and
/// `read_only`, each read-only root with its path when it presents a
/// readable marker. A manifest alone is no holder: the superseded bytes
/// are the data file's.
///
/// Cost per call: at most 1 `lstat` per writable root in its quarantine,
/// then, only when none holds it there, at most 1 per writable root in its
/// layout, then, only when none holds it there either, at most 1 per
/// marked read-only root in its quarantine, then, only with h = 0, 1 per
/// marked read-only root in its layout; it stops at the first hit that
/// makes h ≥ 1. An unmarked read-only root costs nothing.
///
/// With h = 0 the rule fails closed: a read-only root without a readable
/// marker (an unmounted root reads as empty) and one whose layout cannot
/// be inspected count in r, named with the reason. An error inspecting a
/// read-only root's quarantine counts as no copy there, which can only
/// lower h.
///
/// # Errors
///
/// The I/O error of an inspection of a writable root other than "not
/// found", naming the root.
pub fn read_only_alone(
    writable: &[(&str, &Path)],
    read_only: &[(&str, Option<&Path>)],
    path: &str,
    quarantine_id: &str,
) -> Result<Vec<String>, String> {
    let held = |label: &str, file: PathBuf| {
        exists(&file).map_err(|error| format!("inspect {path} on {label}: {error}"))
    };
    let marked = || {
        read_only
            .iter()
            .filter_map(|(label, root)| root.map(|root| (*label, root)))
    };
    h_zero(
        || {
            for (label, root) in writable {
                if held(
                    label,
                    replica::quarantine_dir(root, quarantine_id).join(path),
                )? {
                    return Ok(true);
                }
            }
            for (label, root) in writable {
                if held(label, root.join(path))? {
                    return Ok(true);
                }
            }
            Ok(false)
        },
        || {
            Ok(marked().any(|(_, root)| {
                exists(&replica::quarantine_dir(root, quarantine_id).join(path)).unwrap_or(false)
            }))
        },
        || {
            let mut alone = Vec::new();
            for (label, root) in read_only {
                let Some(root) = root else {
                    alone.push(format!(
                        "{label} (no readable marker: an unmounted root reads as empty, so it \
                         cannot be confirmed free of the data file; mount it)"
                    ));
                    continue;
                };
                match exists(&root.join(path)) {
                    Ok(false) => {}
                    Ok(true) => alone.push((*label).to_owned()),
                    Err(error) => alone.push(format!("{label} (cannot be inspected: {error})")),
                }
            }
            Ok(alone)
        },
    )
}

/// The problem of a moved path whose data only read-only roots hold
/// ([`read_only_alone`]), naming them and the remedy, shared by the resume,
/// the proposals (RQ8) and the rebuild (Q5).
pub fn read_only_alone_line(
    path: &str,
    roots: &[String],
    quarantine_id: &str,
    archive: &str,
) -> String {
    format!(
        "{path} (quarantine {quarantine_id}): its data file is held only by the read-only \
         root(s) {}; neither hot nor a copy target holds it in its layout or in its \
         `.quarantine/{quarantine_id}/`, so its superseded bytes would stay only on roots mie \
         never writes (ADR-062 D4, D11). Set `copy_target = true` for one of them in \
         `paths.cold_roots`, then run `mie store resolve --file {archive}`, which moves its \
         copy into quarantine",
        roots.join(", ")
    )
}

/// Whether a name exists at `file` (`lstat`, a symlink is not followed).
fn exists(file: &Path) -> std::io::Result<bool> {
    match fs::symlink_metadata(file) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// The info line of a read-only root whose layout still holds a moved
/// path (ADR-062 D11 *As amended (#216)*), shared by the replace, its
/// resume, the proposals (RQ8) and the rebuild (Q5).
pub fn read_only_line(path: &str, root: &str, quarantine_id: &str) -> String {
    format!(
        "{path} (quarantine {quarantine_id}): still in the layout of read-only root {root}; mie \
         never writes a read-only root and the copy ledger keeps the path pending or replaced, \
         so no `select` lists it; the replace does not wait for it (ADR-062 D4, D11)"
    )
}

/// One [`read_only_line`] per (moved path, read-only root whose layout
/// holds it): `moved` in its order, then `read_only` (id, path) in its
/// order. An inspection error is a line too, never an error: the root
/// counts for nothing, so it stops nothing.
pub fn read_only_lines(
    read_only: &[(&str, &Path)],
    moved: &[String],
    quarantine_id: &str,
) -> Vec<String> {
    let mut lines = Vec::new();
    for path in moved {
        for (root, dir) in read_only {
            match in_layout(dir, path) {
                Ok(true) => lines.push(read_only_line(path, root, quarantine_id)),
                Ok(false) => {}
                Err(error) => lines.push(format!(
                    "{path} on read-only root {root} cannot be inspected ({error})"
                )),
            }
        }
    }
    lines
}

/// Whether the layout of `root` holds `path`: its data file or its
/// manifest.
///
/// # Errors
///
/// The I/O error of an inspection other than "not found".
pub fn in_layout(root: &Path, path: &str) -> std::io::Result<bool> {
    Ok(layout_holds(root, path)?.is_some())
}

/// What the layout of `root` holds of `path`: `Some(true)` its data file,
/// `Some(false)` its manifest alone, `None` neither. At most 2 `lstat`, the
/// data file first, as [`in_layout`].
///
/// # Errors
///
/// The I/O error of an inspection other than "not found".
pub fn layout_holds(root: &Path, path: &str) -> std::io::Result<Option<bool>> {
    if exists(&root.join(path))? {
        return Ok(Some(true));
    }
    let manifest = format!("{}.manifest", path.strip_suffix(".parquet").unwrap_or(path));
    Ok(exists(&root.join(manifest))?.then_some(false))
}

/// The text rule (module docs): the one distinct text of `texts`; with
/// several, the one whose SHA-256 is in `listed`; otherwise none.
pub fn text_rule<'a>(
    texts: impl IntoIterator<Item = &'a str>,
    listed: &BTreeSet<String>,
) -> Option<String> {
    let mut distinct: Vec<&str> = texts.into_iter().collect();
    distinct.sort_unstable();
    distinct.dedup();
    match distinct[..] {
        [only] => Some(only.to_owned()),
        [] => None,
        _ => {
            let matching: Vec<&str> = distinct
                .iter()
                .copied()
                .filter(|text| {
                    replica::parse_manifest(text).is_ok_and(|file| listed.contains(&file.sha256))
                })
                .collect();
            match matching[..] {
                [only] => Some(only.to_owned()),
                _ => None,
            }
        }
    }
}

/// The SHA-256 the superseded entries of `record` list for `path`: every
/// file of `files` (F's import-ledger files as (name, bytes)) whose
/// `<segment>/<name>` and SHA-256 an `entry` line of `record` names.
pub fn superseded_shas(
    record: &SupersedeRecord,
    segment: &str,
    files: &[(LedgerName, Vec<u8>)],
    path: &str,
) -> BTreeSet<String> {
    use mie_adapter_binance::archive::ledger::SupersededEntry;
    let mut listed = BTreeSet::new();
    for (name, bytes) in files {
        if name.kind != LedgerKind::Entry {
            continue;
        }
        let entry = SupersededEntry::of(format!("{segment}/{}", name.render()), bytes);
        if !record.entries.contains(&entry) {
            continue;
        }
        if let Some(ledger) = std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| ImportLedger::parse(text).ok())
        {
            listed.extend(
                ledger
                    .files
                    .into_iter()
                    .filter(|file| file.relative_path == path)
                    .map(|file| file.sha256),
            );
        }
    }
    listed
}

/// Why the replace of `record` is not complete (RQ8, I1), if it is not:
/// unfinished on hot or a copy target ([`ReplaceRoots::unfinished`]), a
/// moved path not
/// recorded `quarantined` under its id, or still held.
///
/// # Errors
///
/// An I/O error inspecting hot or a copy target.
fn incomplete(
    roots: &ReplaceRoots,
    record: &SupersedeRecord,
    state: &CopyState,
) -> Result<Option<String>, String> {
    let Some(quarantine) = &record.quarantine else {
        return Ok(None);
    };
    if let Some(why) = roots.unfinished(record, true)? {
        return Ok(Some(why));
    }
    for path in &quarantine.moved {
        if state.quarantine(path) != Some(quarantine.id.as_str()) {
            return Ok(Some(format!(
                "{path} is not recorded quarantined under {}",
                quarantine.id
            )));
        }
        if state.hold_reason(path).is_some() {
            return Ok(Some(format!("{path} is still held")));
        }
    }
    Ok(None)
}

/// I1 (module docs).
///
/// # Errors
///
/// The refusal, naming the archive file and `mie store resolve --file F`.
pub fn check_replaces(
    roots: &ReplaceRoots,
    ledgers: &LedgerDir,
    state: &CopyState,
) -> Result<(), String> {
    let records = ledgers
        .supersede_records()
        .map_err(|error| format!("the import ledger's supersede records: {error}"))?;
    for found in records {
        let Ok(record) = &found.record else {
            continue;
        };
        let Some(quarantine) = &record.quarantine else {
            continue;
        };
        let refuse = |why: String| {
            Err(format!(
                "the replace of {} under quarantine {} is incomplete: {why}; the importer refuses \
                 until `mie store resolve --config <ingest config> --file {}` completes it (ADR-062 \
                 D11)",
                record.archive, quarantine.id, record.archive
            ))
        };
        let pending = quarantine
            .moved
            .iter()
            .any(|path| state.verdict(path) != Some(PathVerdict::Replaced));
        if let Some(why) = roots.unfinished(record, pending)? {
            return refuse(why);
        }
        for path in &quarantine.moved {
            let verdict = state.verdict(path);
            if !matches!(verdict, Some(PathVerdict::Pending | PathVerdict::Replaced)) {
                return refuse(format!("{path} has the copy-ledger verdict {verdict:?}"));
            }
        }
    }
    Ok(())
}

/// I3 (module docs): appends the `replaced` records that are due and
/// returns one line per record appended and per replacement still waiting.
///
/// # Errors
///
/// The supersede records cannot be listed, the copy ledger cannot be read
/// or folded, or an append fails.
pub fn record_replacements(
    hot: &Path,
    roots: &ReplaceRoots,
    ledgers: &LedgerDir,
    now_ms: &dyn Fn() -> i64,
) -> Result<Vec<String>, String> {
    let records = ledgers
        .supersede_records()
        .map_err(|error| format!("the import ledger's supersede records: {error}"))?;
    let mut lines = Vec::new();
    let mut due: Vec<Kind> = Vec::new();
    let mut state: Option<CopyState> = None;
    for found in records {
        let Ok(record) = &found.record else {
            continue;
        };
        let Some(quarantine) = &record.quarantine else {
            continue;
        };
        if state.is_none() {
            let ledger = copy_ledger::read(hot)
                .map_err(|error| format!("hot's copy ledger: {error}"))?
                .ok_or_else(|| "hot's copy ledger does not exist".to_owned())?;
            state = Some(
                CopyState::fold(&ledger)
                    .map_err(|error| format!("the copy ledger cannot be folded: {error}"))?,
            );
        }
        let state = state.as_ref().expect("folded above");
        let pending: Vec<&String> = quarantine
            .moved
            .iter()
            .filter(|path| state.verdict(path) == Some(PathVerdict::Pending))
            .collect();
        if pending.is_empty() {
            continue;
        }
        if let Some(why) = roots.unfinished(record, true)? {
            lines.push(format!(
                "replacement waits: the replace of {} under {} is unfinished ({why}); no \
                 `replaced` record is appended until `mie store resolve --file {}` completes it",
                record.archive, quarantine.id, record.archive
            ));
            continue;
        }
        let files = ledgers
            .files(found.stream, &found.name.file)
            .map_err(|error| format!("the import ledger of {}: {error}", found.name.file))?;
        let Some(g) = replacement_generation(
            found.name.generation,
            files.iter().map(|f| (f.name.generation, f.name.kind)),
        ) else {
            continue;
        };
        let segment = found.stream.raw_name();
        let name = LedgerName::entry(&found.name.file, g);
        let label = format!("{segment}/{}", name.render());
        let Some(file) = files.iter().find(|f| f.name == name) else {
            continue;
        };
        let entry = match std::str::from_utf8(&file.bytes)
            .map_err(|e| e.to_string())
            .and_then(|text| ImportLedger::parse(text).map_err(|e| e.to_string()))
        {
            Ok(entry) => entry,
            Err(error) => {
                lines.push(format!(
                    "replacement waits: {label} cannot be read ({error}); no `replaced` record is \
                     appended (ADR-062 D7)"
                ));
                continue;
            }
        };
        let mut missing = None;
        for listed in &entry.files {
            let sealed = match replica::sealed_manifest(hot, &listed.relative_path) {
                Ok(Some((_, file))) => file.sha256 == listed.sha256,
                Ok(None) => {
                    state.verdict(&listed.relative_path) == Some(PathVerdict::Ordinary)
                        && state
                            .ledger_text(&listed.relative_path)
                            .and_then(|text| replica::parse_manifest(text).ok())
                            .is_some_and(|file| file.sha256 == listed.sha256)
                }
                Err(_) => false,
            };
            if !sealed {
                missing = Some(listed.relative_path.clone());
                break;
            }
        }
        if let Some(path) = missing {
            lines.push(format!(
                "replacement waits: {path}, listed by {label}, is not sealed on hot with its listed \
                 SHA-256; no `replaced` record is appended (ADR-062 D7 write order)"
            ));
            continue;
        }
        for path in pending {
            due.push(Kind::Replaced {
                path: path.clone(),
                entry: label.clone(),
            });
        }
    }
    if due.is_empty() {
        return Ok(lines);
    }
    let at_ms = now_ms();
    let appender = CopyLedger::at(hot);
    let ledger_path = copy_ledger::ledger_path(hot);
    for kind in due {
        let line = match &kind {
            Kind::Replaced { path, entry } => format!("replaced {path} {entry}"),
            _ => unreachable!("only replaced records are due"),
        };
        match appender.append(&Record { at_ms, kind }) {
            Ok(outcome) => {
                lines.push(line);
                if let Some(truncation) = outcome.truncation {
                    lines.push(super::truncation_line(&ledger_path, truncation));
                }
            }
            Err(error) => {
                return Err(format!(
                    "{line}: the record could not be appended ({}); the next import appends it",
                    error.error
                ));
            }
        }
    }
    Ok(lines)
}

/// RQ8 (module docs): one line per supersede record on hot with a
/// quarantine, a problem per incomplete replace, and the info lines of the
/// read-only roots that still hold a moved path.
///
/// # Errors
///
/// The supersede records cannot be listed or hot cannot be inspected.
pub fn replace_lines(
    roots: &ReplaceRoots,
    ledgers: &LedgerDir,
    state: &CopyState,
) -> Result<(Vec<String>, Vec<String>), String> {
    let (mut lines, mut problems) = (Vec::new(), Vec::new());
    let records = ledgers
        .supersede_records()
        .map_err(|error| format!("the import ledger's supersede records: {error}"))?;
    for found in records {
        let label = format!("{}/{}", found.stream.raw_name(), found.name.render());
        let record = match &found.record {
            Ok(record) => record,
            Err(error) => {
                lines.push(format!(
                    "supersede record {label}: it cannot be read ({error})"
                ));
                continue;
            }
        };
        let Some(quarantine) = &record.quarantine else {
            continue;
        };
        let read_only = roots.read_only_holders(record);
        let alone = roots.read_only_alone(record)?;
        if let Some(why) = incomplete(roots, record, state)? {
            problems.push(format!(
                "the replace of {} under quarantine {} ({label}) is incomplete: {why}; `mie store \
                 resolve --file {}` completes it",
                record.archive, quarantine.id, record.archive
            ));
            problems.extend(alone);
            lines.extend(read_only);
            continue;
        }
        if !alone.is_empty() {
            problems.extend(alone);
            lines.extend(read_only);
            continue;
        }
        let files = ledgers
            .files(found.stream, &found.name.file)
            .map_err(|error| format!("the import ledger of {}: {error}", found.name.file))?;
        let replaced = replacement_generation(
            found.name.generation,
            files.iter().map(|f| (f.name.generation, f.name.kind)),
        );
        match replaced {
            Some(g) => lines.push(format!(
                "replace of {} under quarantine {}: complete; replaced by {}/{}",
                record.archive,
                quarantine.id,
                found.stream.raw_name(),
                LedgerName::entry(&found.name.file, g).render()
            )),
            None => lines.push(format!(
                "replace of {} under quarantine {}: complete; the re-import is pending: run `mie \
                 archive-import` for its period",
                record.archive, quarantine.id
            )),
        }
        lines.extend(read_only);
    }
    Ok((lines, problems))
}
