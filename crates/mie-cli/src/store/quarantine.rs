//! The importer's side of the replace resolution (ADR-062 D11, proposed;
//! #211), shared with `mie store resolve` and the rebuild.
//!
//! - **I1 Gate** ([`check_replaces`]). `mie archive-import`, its dry runs
//!   too, refuses while a supersede record on hot with a quarantine has a
//!   moved path whose data or manifest is in hot's layout, or whose
//!   copy-ledger verdict is neither pending nor replaced. The replace
//!   renames hot last, so hot's layout is the commit point, which keeps
//!   the gate exact after a rebuild from an empty start (no hold record).
//!   A record that does not parse is skipped here: the import of its own
//!   archive file fails closed on it (#210 C1).
//! - **I3 Replacement records** ([`record_replacements`]). For each such
//!   record S_n, E_g is its archive file's entry of the lowest generation
//!   g ≥ n on hot ([`replacement_generation`]). Once E_g parses and every
//!   file it lists is sealed on hot with the listed SHA-256 (hot's
//!   manifest, else an ordinary copy-ledger text), one `replaced <path>
//!   <segment>/<E_g>` record is appended per moved path that is still
//!   pending, all with one `at_ms`; otherwise the missing file is named and
//!   nothing is appended (D7 *Write order*: `replaced` only after the
//!   entry and its parts are sealed). The importer runs it at its start and
//!   after its run, never in a dry run, so a crash between the entry and
//!   the records is completed by the next import (or by a rebuild, Q4).
//! - **The text rule** ([`text_rule`]), shared by the replace (W2) and the
//!   rebuild (Q4): the one distinct manifest text over the roots' layouts
//!   and quarantine copies; with several, the one whose SHA-256 the
//!   superseded entry lists; otherwise none, and the path then covers its
//!   whole stream-day in `select`, failing more selections, never fewer.
//! - **RQ8 proposals** ([`replace_lines`]): one line per supersede record
//!   on hot that carries a quarantine; an incomplete replace is a problem
//!   naming `mie store resolve --file F`.

use mie_adapter_binance::archive::ledger::{
    ImportLedger, LedgerDir, LedgerKind, LedgerName, SupersedeRecord, replacement_generation,
};
use mie_adapter_parquet::copy_ledger::{self, CopyLedger, Kind, Record};
use mie_adapter_parquet::replica;
use mie_adapter_parquet::verification::{CopyState, PathVerdict};
use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

/// Whether the layout of `root` holds `path`: its data file or its
/// manifest.
///
/// # Errors
///
/// The I/O error of an inspection other than "not found".
pub fn in_layout(root: &Path, path: &str) -> std::io::Result<bool> {
    let manifest = format!("{}.manifest", path.strip_suffix(".parquet").unwrap_or(path));
    for name in [path, manifest.as_str()] {
        match fs::symlink_metadata(root.join(name)) {
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(false)
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
/// a moved path in hot's layout, not recorded `quarantined` under its id,
/// or still held.
///
/// # Errors
///
/// An I/O error inspecting hot.
fn incomplete(
    hot: &Path,
    record: &SupersedeRecord,
    state: &CopyState,
) -> Result<Option<String>, String> {
    let Some(quarantine) = &record.quarantine else {
        return Ok(None);
    };
    for path in &quarantine.moved {
        if in_layout(hot, path).map_err(|e| format!("inspect {path} on hot: {e}"))? {
            return Ok(Some(format!("{path} is still in hot's layout")));
        }
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
pub fn check_replaces(hot: &Path, ledgers: &LedgerDir, state: &CopyState) -> Result<(), String> {
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
        for path in &quarantine.moved {
            let on_hot = in_layout(hot, path).map_err(|e| format!("inspect {path} on hot: {e}"))?;
            let verdict = state.verdict(path);
            if on_hot || !matches!(verdict, Some(PathVerdict::Pending | PathVerdict::Replaced)) {
                let why = if on_hot {
                    "is still in hot's layout".to_owned()
                } else {
                    format!("has the copy-ledger verdict {verdict:?}")
                };
                return Err(format!(
                    "the replace of {} under quarantine {} is incomplete: {path} {why}; the importer \
                     refuses until `mie store resolve --config <ingest config> --file {}` completes \
                     it (ADR-062 D11)",
                    record.archive, quarantine.id, record.archive
                ));
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
/// quarantine, and a problem per incomplete replace.
///
/// # Errors
///
/// The supersede records cannot be listed or hot cannot be inspected.
pub fn replace_lines(
    hot: &Path,
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
        if let Some(why) = incomplete(hot, record, state)? {
            problems.push(format!(
                "the replace of {} under quarantine {} ({label}) is incomplete: {why}; `mie store \
                 resolve --file {}` completes it",
                record.archive, quarantine.id, record.archive
            ));
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
    }
    Ok((lines, problems))
}
