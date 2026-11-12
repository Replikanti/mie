//! The replace resolution of `mie store resolve --replace` (ADR-062 D5,
//! D11, proposed; #211): the superseded generation of one archive file
//! moves into `.quarantine/<id>/` on every root, and a resume completes a
//! replace that stopped. Rules RQ1 to RQ7 of the module docs of
//! [`super`]; the pure parts are [`plan_replace`] and [`cost`].

use super::{FileOf, PlanInputs, ResolveReport, Stop, Survey, Variant, survey};
use crate::journal::WarmHandoff;
use crate::store::quarantine::{in_layout, superseded_shas, text_rule};
use crate::store::truncation_line;
use mie_adapter_binance::archive::catalog::ArchiveStream;
use mie_adapter_binance::archive::ledger::{
    LedgerDir, LedgerName, Quarantine, SupersedeRecord, SupersededEntry,
};
use mie_adapter_parquet::copy_ledger::{self, CopyLedger, Kind, Record, Resolution};
use mie_adapter_parquet::replica;
use mie_adapter_parquet::root_marker::{self, HOT_ID};
use mie_adapter_parquet::verification::{CopyState, PathVerdict};
use mie_ports::raw::SealedFile;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// A test hook that sees a root's id.
#[cfg(test)]
type RootHook = std::cell::RefCell<Option<Box<dyn FnMut(&str)>>>;

#[cfg(test)]
thread_local! {
    /// Test hook: runs after each root's renames (W3) with its id, `hot`
    /// last.
    pub(crate) static AFTER_ROOT: RootHook = std::cell::RefCell::new(None);
}

// ------------------------------------------------------------ pure parts

/// One root whose layout holds a path (its data file or its manifest).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    /// `hot`, or a cold root's id.
    pub root: String,
    /// A read-only cold root (no copy target): D4 never writes it.
    pub read_only: bool,
    /// Its manifest text, when it parses and describes the path.
    pub text: Option<String>,
}

/// What [`plan_replace`] decided (RQ2 to RQ4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacePlan {
    /// S_n's generation.
    pub generation: u32,
    /// S_n.
    pub record: SupersedeRecord,
    /// Per moved path, the SHA-256 the superseded entry lists for it (the
    /// text rule's tie-break).
    pub listed: BTreeMap<String, BTreeSet<String>>,
    /// Report lines: notes, and the listed paths no root holds and the
    /// ledger does not record (named, not moved).
    pub lines: Vec<String>,
}

impl ReplacePlan {
    /// M, sorted.
    pub fn moved(&self) -> &[String] {
        self.record
            .quarantine
            .as_ref()
            .map_or(&[], |quarantine| quarantine.moved.as_slice())
    }
}

/// RQ2 to RQ4, pure: the moved set M of a new replace under `id` and its
/// supersede record. `holders` maps every path of parts(F) and every path
/// an entry of F lists to the roots whose layout holds it.
///
/// # Errors
///
/// The refusal; nothing may be written.
pub fn plan_replace(
    inputs: &PlanInputs,
    holders: &BTreeMap<String, Vec<Holder>>,
    id: &str,
) -> Result<ReplacePlan, String> {
    let survey: Survey<'_> = survey(inputs)?;
    let highest_entry = survey.highest_entry();
    let mut lines = survey.decided(&|generation, _| Some(generation) == highest_entry)?;
    let file = super::archive_file(&inputs.archive);
    // RQ4: n = 1 + the highest generation, never wrapping.
    let generation = survey
        .highest()
        .map_or(Some(1), |highest| highest.checked_add(1))
        .ok_or_else(|| {
            format!(
                "the next generation of {file} would exceed {}: a generation never wraps to 0, \
                 which would name the first entry",
                u32::MAX
            )
        })?;
    let latest: &[Variant<'_>] = match highest_entry {
        Some(g) => survey
            .variants_of(g)
            .map(|(_, list)| list.as_slice())
            .unwrap_or_default(),
        None => &[],
    };
    let mut entries: Vec<SupersededEntry> = match highest_entry {
        Some(g) => latest
            .iter()
            .map(|(bytes, _, _)| {
                SupersededEntry::of(
                    format!(
                        "{}/{}",
                        inputs.segment,
                        LedgerName::entry(&file, g).render()
                    ),
                    bytes,
                )
            })
            .collect(),
        None => Vec::new(),
    };
    entries.sort();
    entries.dedup();

    // RQ2: parts(F), then the siblings the latest entry lists.
    let parts: BTreeSet<&str> = inputs
        .parts
        .iter()
        .map(|part| part.relative_path.as_str())
        .collect();
    let mut listed: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for ledger in latest.iter().filter_map(|(_, _, ledger)| ledger.as_ref()) {
        for listed_file in &ledger.files {
            listed
                .entry(listed_file.relative_path.clone())
                .or_default()
                .insert(listed_file.sha256.clone());
        }
    }
    let holds = |path: &str| holders.get(path).is_some_and(|list| !list.is_empty());
    let mut moved: BTreeSet<String> = BTreeSet::new();
    for part in &parts {
        if !holds(part) {
            return Err(format!(
                "{part} is held for this file and no root holds it (no data file and no manifest \
                 in any root's layout): there is nothing to quarantine; run `mie store verify`, \
                 then `mie store repair --record-lost` (ADR-062 D11)"
            ));
        }
        moved.insert((*part).to_owned());
    }
    for path in listed.keys() {
        if parts.contains(path.as_str()) {
            continue;
        }
        let verdict = inputs.verdicts.get(path);
        if !holds(path) {
            if let Some(verdict) = verdict {
                return Err(format!(
                    "{path} is listed by the entry this replace supersedes, no root holds it and \
                     the copy ledger records it ({verdict:?}): moving the rest would leave it \
                     behind; nothing is written (ADR-062 D11)"
                ));
            }
            lines.push(format!(
                "{path}: listed by the superseded entry; no root holds it and the copy ledger \
                 does not record it, so it is named and not moved"
            ));
            continue;
        }
        if !matches!(verdict, None | Some(PathVerdict::Ordinary)) {
            return Err(format!(
                "{path} is listed by the entry this replace supersedes and its copy-ledger verdict \
                 is {:?}, not ordinary: a lost, conflicting, held, pending or replaced sibling \
                 cannot move with this file; nothing is written (ADR-062 D11)",
                verdict.expect("not None")
            ));
        }
        moved.insert(path.clone());
    }
    for path in &moved {
        if inputs.verdicts.get(path) == Some(&PathVerdict::Conflict) {
            return Err(format!(
                "{path}: the copy ledger holds two manifest texts for it, a D5 conflict that \
                 outranks `replaced` in the fold and would fail every selection of it for good; \
                 nothing is written (ADR-062 D5, D11)"
            ));
        }
        if let Some(holder) = holders
            .get(path)
            .into_iter()
            .flatten()
            .find(|holder| holder.read_only)
        {
            return Err(format!(
                "{path} is held by the read-only root {}: only the replicator writes cold roots, \
                 and never a read-only one (ADR-062 D4), so its copy cannot move to quarantine; \
                 nothing is written",
                holder.root
            ));
        }
    }
    let moved: Vec<String> = moved.into_iter().collect();
    let listed = moved
        .iter()
        .map(|path| (path.clone(), listed.get(path).cloned().unwrap_or_default()))
        .collect();
    Ok(ReplacePlan {
        generation,
        record: SupersedeRecord {
            archive: inputs.archive.clone(),
            stream: inputs.stream.clone(),
            period: inputs.period,
            entries,
            quarantine: Some(Quarantine {
                id: id.to_owned(),
                moved,
            }),
        },
        listed,
        lines,
    })
}

/// One moved path with every distinct manifest the roots or the copy
/// ledger hold for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MovedFile {
    /// The data path.
    pub path: String,
    /// Its distinct manifests; empty when none parses.
    pub manifests: Vec<SealedFile>,
}

/// The journal's warm runs, as the cost reads them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Journal {
    /// No journal file.
    Missing,
    /// Every warm handoff, as (run id, handoff).
    Runs(Vec<(String, WarmHandoff)>),
}

/// RQ5, pure: the cost lines of moving `moved`.
pub fn cost(moved: &[MovedFile], journal: &Journal) -> Vec<String> {
    let mut lines = Vec::new();
    for file in moved {
        if file.manifests.is_empty() {
            lines.push(format!(
                "cost: {}: no manifest; it covers its whole stream-day, and every dataset version \
                 of a selection of that day changes",
                file.path
            ));
        }
        for manifest in &file.manifests {
            lines.push(format!(
                "cost: {}: stream {}, date {}, events [{}, {}] ms, {} rows, sha256 {}: every \
                 dataset version of a selection overlapping it changes",
                file.path,
                manifest.stream,
                manifest.date,
                manifest.min_event_time.as_millis(),
                manifest.max_event_time.as_millis(),
                manifest.rows,
                manifest.sha256
            ));
        }
    }
    let mut runs = 0usize;
    match journal {
        Journal::Missing => lines.push("cost: no journal: no warm run recorded".to_owned()),
        Journal::Runs(handoffs) => {
            for (run_id, handoff) in handoffs {
                let overlaps =
                    |(min, max): (i64, i64)| min < handoff.cutover_ms && max >= handoff.origin_ms;
                let covered: Vec<&str> = moved
                    .iter()
                    .filter(|file| {
                        // Without a manifest the path covers its whole
                        // stream-day, as `select` fails it (review of
                        // #215, finding 2): the superset rule.
                        if file.manifests.is_empty() {
                            return whole_day(&file.path).is_none_or(overlaps);
                        }
                        file.manifests.iter().any(|manifest| {
                            overlaps((
                                manifest.min_event_time.as_millis(),
                                manifest.max_event_time.as_millis(),
                            ))
                        })
                    })
                    .map(|file| file.path.as_str())
                    .collect();
                if covered.is_empty() {
                    continue;
                }
                runs += 1;
                lines.push(format!(
                    "cost: warm run {run_id}: its prefix [{}, {}) covers {}; its prefix_dataset {} \
                     reads `DIVERGED (warm prefix)` for good",
                    handoff.origin_ms,
                    handoff.cutover_ms,
                    covered.join(", "),
                    handoff.prefix_dataset.as_str()
                ));
            }
        }
    }
    lines.push(format!(
        "cost: {} moved path(s), {runs} journaled warm run(s) whose prefix_dataset changes",
        moved.len()
    ));
    lines
}

/// The event-time range `[first, last]` ms of the day a data path's
/// `date=` segment names; `None` when it names none (then every run is
/// taken to cover it, never fewer).
fn whole_day(path: &str) -> Option<(i64, i64)> {
    use mie_adapter_binance::archive::catalog::{DAY_MS, parse_day};
    let day = path
        .split('/')
        .find_map(|segment| segment.strip_prefix("date="))
        .and_then(parse_day)?;
    Some((day * DAY_MS, (day + 1) * DAY_MS - 1))
}

// ------------------------------------------------------------- the run

/// A configured cold root that presented its marker (RQ1).
#[derive(Debug, Clone)]
pub struct Root {
    /// Its path.
    pub path: PathBuf,
    /// Its marker's id.
    pub id: String,
    /// Whether it is a copy target.
    pub copy_target: bool,
}

/// Every configured cold root that presents a readable marker, for the
/// cost a re-admit's difference prints: it writes nothing, so a root
/// without its marker is left out rather than refused.
pub fn marked_roots(cold_roots: &[crate::config::ColdRoot]) -> Vec<Root> {
    cold_roots
        .iter()
        .filter_map(|cold| match root_marker::read(&cold.path) {
            Ok(Some(id)) => Some(Root {
                path: cold.path.clone(),
                id,
                copy_target: cold.copy_target,
            }),
            _ => None,
        })
        .collect()
}

/// RQ1's root checks: every configured cold root, copy target or
/// read-only, carries a readable marker (the renames must reach every root
/// that holds a moved path, and an unmounted root reads as empty), and no
/// root has a `.quarantine` that is no directory.
///
/// # Errors
///
/// The refusal.
pub fn check_roots(
    hot: &Path,
    cold_roots: &[crate::config::ColdRoot],
) -> Result<Vec<Root>, String> {
    let mut roots = Vec::new();
    for cold in cold_roots {
        let kind = if cold.copy_target {
            "copy target"
        } else {
            "read-only root"
        };
        match root_marker::read(&cold.path) {
            Ok(Some(id)) => roots.push(Root {
                path: cold.path.clone(),
                id,
                copy_target: cold.copy_target,
            }),
            Ok(None) => {
                return Err(format!(
                    "{kind} {} has no root marker: the replace renames on every root that holds a \
                     moved path, and an unmounted root reads as empty; nothing is written \
                     (ADR-062 D11)",
                    cold.path.display()
                ));
            }
            Err(error) => {
                return Err(format!(
                    "{kind} {}: its root marker cannot be read ({error}); nothing is written",
                    cold.path.display()
                ));
            }
        }
    }
    for root in std::iter::once(hot).chain(roots.iter().map(|r| r.path.as_path())) {
        let dir = root.join(replica::QUARANTINE_DIR);
        match fs::symlink_metadata(&dir) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(format!(
                    "{} exists and is no directory (a symlink or a file): the quarantine must be a \
                     directory within the root, so its renames stay on one filesystem; nothing is \
                     written (ADR-062 D11)",
                    dir.display()
                ));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(format!("inspect {}: {error}", dir.display())),
        }
    }
    Ok(roots)
}

/// A path's candidate manifest texts and the SHA-256 the superseded entry
/// lists for it (the text rule's inputs).
type TextsOf<'a> = dyn Fn(&str) -> (Vec<String>, BTreeSet<String>) + 'a;

/// What the replace and the resume run against.
pub struct Context<'a> {
    /// Hot.
    pub hot: &'a Path,
    /// Every configured cold root, in configuration order (RQ1).
    pub roots: &'a [Root],
    /// The copy ledger as resolve folded it.
    pub state: &'a CopyState,
    /// Hot's import ledger.
    pub ledgers: &'a LedgerDir,
    /// F's archive stream.
    pub stream: ArchiveStream,
    /// F's file name.
    pub file_name: &'a str,
    /// The archive path F.
    pub archive: &'a str,
    /// The ingest configuration's journal.
    pub journal: &'a Path,
    /// The run's clock, read once.
    pub now_ms: &'a dyn Fn() -> i64,
}

impl Context<'_> {
    /// Every root: the cold roots in configuration order, then hot, with
    /// id, path and whether it is read-only.
    fn rename_order(&self) -> Vec<(&str, &Path, bool)> {
        self.roots
            .iter()
            .map(|root| (root.id.as_str(), root.path.as_path(), !root.copy_target))
            .chain(std::iter::once((HOT_ID, self.hot, false)))
            .collect()
    }

    /// The roots whose layout holds `path`, hot first.
    fn holders(&self, path: &str) -> Result<Vec<Holder>, Stop> {
        let mut holders = Vec::new();
        for (id, root, read_only) in std::iter::once((HOT_ID, self.hot, false)).chain(
            self.roots
                .iter()
                .map(|root| (root.id.as_str(), root.path.as_path(), !root.copy_target)),
        ) {
            let held = in_layout(root, path)
                .map_err(|e| Stop(format!("inspect {path} on {id}: {e}; nothing is written")))?;
            if held {
                holders.push(Holder {
                    root: id.to_owned(),
                    read_only,
                    text: replica::sealed_manifest(root, path)
                        .ok()
                        .flatten()
                        .map(|(text, _)| text),
                });
            }
        }
        Ok(holders)
    }

    /// The manifest texts of `path` in the quarantine `id` of every root.
    fn quarantine_texts(&self, path: &str, id: &str) -> Vec<String> {
        self.rename_order()
            .into_iter()
            .filter_map(|(_, root, _)| {
                replica::sealed_manifest(&replica::quarantine_dir(root, id), path)
                    .ok()
                    .flatten()
                    .map(|(text, _)| text)
            })
            .collect()
    }

    /// RQ3: refused when a supersede record on hot or a copy target names
    /// `id`, or a root already has `.quarantine/<id>`.
    fn check_id(&self, id: &str) -> Result<(), Stop> {
        for (root_id, root, _) in self.rename_order() {
            let dir = replica::quarantine_dir(root, id);
            if fs::symlink_metadata(&dir).is_ok() {
                return Err(Stop(format!(
                    "the quarantine id {id} is in use: {} exists on {root_id}; a second replace in \
                     the same millisecond is refused, run it again (ADR-062 D11)",
                    dir.display()
                )));
            }
        }
        let dirs = std::iter::once((HOT_ID.to_owned(), self.ledgers.clone())).chain(
            self.roots
                .iter()
                .filter(|root| root.copy_target)
                .map(|root| {
                    (
                        root.id.clone(),
                        LedgerDir::new(
                            root.path
                                .join(super::META_DIR)
                                .join(super::IMPORT_LEDGER_DIR),
                        ),
                    )
                }),
        );
        for (root_id, ledgers) in dirs {
            let records = ledgers.supersede_records().map_err(|error| {
                Stop(format!(
                    "the supersede records on {root_id} cannot be listed ({error}); nothing is \
                     written"
                ))
            })?;
            for found in records {
                if let Ok(record) = &found.record
                    && record.quarantine.as_ref().is_some_and(|q| q.id == id)
                {
                    return Err(Stop(format!(
                        "the quarantine id {id} is in use: supersede record {}/{} on {root_id} \
                         names it; run the replace again (ADR-062 D11)",
                        found.stream.raw_name(),
                        found.name.render()
                    )));
                }
            }
        }
        Ok(())
    }

    /// The journal's warm runs (RQ5).
    fn journal(&self) -> Result<Journal, Stop> {
        match crate::journal::warm_handoffs(self.journal) {
            Ok(None) => Ok(Journal::Missing),
            Ok(Some(runs)) => Ok(Journal::Runs(runs)),
            Err(error) => Err(Stop(format!(
                "the journal {} cannot be read ({error}): the replace's cost names every warm run \
                 that cited a moved part, so nothing is written",
                self.journal.display()
            ))),
        }
    }

    /// Appends one record, reporting it and any truncation.
    fn append(
        &self,
        kind: Kind,
        at_ms: i64,
        line: String,
        report: &mut ResolveReport,
    ) -> Result<(), Stop> {
        let ledger_path = copy_ledger::ledger_path(self.hot);
        match CopyLedger::at(self.hot).append(&Record { at_ms, kind }) {
            Ok(outcome) => {
                report.lines.push(line);
                if let Some(truncation) = outcome.truncation {
                    report.lines.push(truncation_line(&ledger_path, truncation));
                }
                Ok(())
            }
            Err(error) => {
                if let Some(truncation) = error.truncation {
                    report.lines.push(truncation_line(&ledger_path, truncation));
                }
                Err(Stop(format!(
                    "{line}: the record could not be appended ({}); `mie store resolve --file {}` \
                     completes the replace",
                    error.error, self.archive
                )))
            }
        }
    }

    /// W2: for each moved path, `manifest` when the ledger has no text
    /// (the text rule over `texts`), then `quarantined` unless the ledger
    /// already has it pending under `id`. Returns how many it appended.
    fn write_records(
        &self,
        moved: &[String],
        id: &str,
        texts: &TextsOf<'_>,
        at_ms: i64,
        report: &mut ResolveReport,
    ) -> Result<usize, Stop> {
        let mut written = 0;
        for path in moved {
            if self.state.ledger_text(path).is_none() {
                let (candidates, listed) = texts(path);
                if let Some(text) = text_rule(candidates.iter().map(String::as_str), &listed) {
                    self.append(
                        Kind::Manifest { text },
                        at_ms,
                        format!("manifest {path}"),
                        report,
                    )?;
                    written += 1;
                } else {
                    report.lines.push(format!(
                        "{path}: no single manifest text; it gets none, so it covers its whole \
                         stream-day in `select` and fails more selections, never fewer"
                    ));
                }
            }
            if self.state.quarantine(path) != Some(id) {
                self.append(
                    Kind::Quarantined {
                        path: path.clone(),
                        quarantine: id.to_owned(),
                    },
                    at_ms,
                    format!("quarantined {path} {id}"),
                    report,
                )?;
                written += 1;
            }
        }
        Ok(written)
    }

    /// W3: the renames, root by root, cold roots in configuration order,
    /// hot last; each root's marker re-read first. Returns how many names
    /// moved.
    fn rename(
        &self,
        moved: &[String],
        id: &str,
        report: &mut ResolveReport,
    ) -> Result<usize, Stop> {
        let mut count = 0;
        for (root_id, root, read_only) in self.rename_order() {
            if read_only {
                continue;
            }
            let marker = root_marker::read(root);
            if !matches!(&marker, Ok(Some(found)) if found == root_id) {
                return Err(Stop(format!(
                    "{} ({root_id}): its marker is gone or changed ({marker:?}) since the run \
                     started; the replace stops before this root's renames, and `mie store \
                     resolve --file {}` completes it",
                    root.display(),
                    self.archive
                )));
            }
            let dir = replica::quarantine_dir(root, id);
            for path in moved {
                let names = replica::move_aside(root, path, &dir).map_err(|error| {
                    Stop(format!(
                        "{path} on {root_id}: the rename into {} failed ({error}); the records \
                         already make every selection covering it fail, and `mie store resolve \
                         --file {}` completes the replace",
                        dir.display(),
                        self.archive
                    ))
                })?;
                for name in &names {
                    report.lines.push(format!(
                        "moved {name} on {root_id} to {}/{id}/",
                        replica::QUARANTINE_DIR
                    ));
                }
                count += names.len();
            }
            #[cfg(test)]
            AFTER_ROOT.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook(root_id);
                }
            });
        }
        Ok(count)
    }

    /// W4: `hold-cleared <path> replace` for each moved path with an open
    /// hold. Returns how many it appended.
    fn clear_holds(
        &self,
        moved: &[String],
        at_ms: i64,
        report: &mut ResolveReport,
    ) -> Result<usize, Stop> {
        let mut count = 0;
        for path in moved {
            if self.state.hold_reason(path).is_none() {
                continue;
            }
            self.append(
                Kind::HoldCleared {
                    path: path.clone(),
                    resolution: Resolution::Replace,
                },
                at_ms,
                format!("hold-cleared {path} replace"),
                report,
            )?;
            count += 1;
        }
        Ok(count)
    }

    /// The cost of moving `moved` (RQ5): every manifest the holders or
    /// the ledger hold, and the journal's warm runs.
    pub(super) fn cost_lines(
        &self,
        moved: &[String],
        extra: &dyn Fn(&str) -> Vec<String>,
    ) -> Result<Vec<String>, Stop> {
        let mut files = Vec::new();
        for path in moved {
            let mut texts: Vec<String> = self
                .holders(path)?
                .into_iter()
                .filter_map(|holder| holder.text)
                .chain(self.state.ledger_text(path).map(str::to_owned))
                .chain(extra(path))
                .collect();
            texts.sort();
            texts.dedup();
            files.push(MovedFile {
                path: path.clone(),
                manifests: texts
                    .iter()
                    .filter_map(|text| replica::parse_manifest(text).ok())
                    .collect(),
            });
        }
        Ok(cost(&files, &self.journal()?))
    }

    /// The `next` line after a complete run.
    fn next_step(&self, id: &str) -> String {
        format!(
            "next: `mie archive-import` for the period of {}; until its entry exists every \
             selection covering the paths quarantined under {id} fails with `Integrity`",
            self.archive
        )
    }
}

/// The holders of every path of parts(F) and of every path an entry of F
/// lists.
pub(super) fn holders_of(
    ctx: &Context<'_>,
    inputs: &PlanInputs,
) -> Result<BTreeMap<String, Vec<Holder>>, Stop> {
    let mut paths: BTreeSet<String> = inputs
        .parts
        .iter()
        .map(|part| part.relative_path.clone())
        .collect();
    for file in &inputs.files {
        if let Ok(text) = std::str::from_utf8(&file.bytes)
            && let Ok(entry) = mie_adapter_binance::archive::ledger::ImportLedger::parse(text)
        {
            paths.extend(entry.files.into_iter().map(|f| f.relative_path));
        }
    }
    let mut holders = BTreeMap::new();
    for path in paths {
        let found = ctx.holders(&path)?;
        holders.insert(path, found);
    }
    Ok(holders)
}

/// A new replace (RQ2 to RQ6).
///
/// # Errors
///
/// A [`Stop`]: before W1 nothing was written; after it, a re-run resumes.
pub(super) fn run_replace(
    ctx: &Context<'_>,
    inputs: &PlanInputs,
    report: &mut ResolveReport,
) -> Result<(), Stop> {
    let at_ms = (ctx.now_ms)();
    let id = format!("q-{at_ms}");
    ctx.check_id(&id)?;
    let holders = holders_of(ctx, inputs)?;
    let plan = plan_replace(inputs, &holders, &id).map_err(Stop)?;
    report.lines.extend(plan.lines.iter().cloned());
    let moved = plan.moved().to_vec();
    report.lines.push(format!(
        "replace under quarantine {id}: {} path(s) move",
        moved.len()
    ));
    report
        .lines
        .extend(ctx.cost_lines(&moved, &|_| Vec::new())?);

    // W1.
    ctx.ledgers
        .write_supersede(ctx.stream, ctx.file_name, plan.generation, &plan.record)
        .map_err(|error| Stop(format!("writing the supersede record failed: {error}")))?;
    report.lines.push(format!(
        "wrote {}/{}",
        ctx.stream.raw_name(),
        LedgerName::supersede(ctx.file_name, plan.generation).render()
    ));
    // W2.
    let texts = |path: &str| {
        let texts = holders
            .get(path)
            .into_iter()
            .flatten()
            .filter_map(|holder| holder.text.clone())
            .collect();
        (texts, plan.listed.get(path).cloned().unwrap_or_default())
    };
    ctx.write_records(&moved, &id, &texts, at_ms, report)?;
    // W3.
    ctx.rename(&moved, &id, report)?;
    // W4.
    ctx.clear_holds(&moved, at_ms, report)?;
    report.lines.push(ctx.next_step(&id));
    Ok(())
}

/// RQ7: the resume of the replace S_m records (`generation` m).
///
/// # Errors
///
/// A [`Stop`]; a re-run resumes again.
pub(super) fn run_resume(
    ctx: &Context<'_>,
    generation: u32,
    record: &SupersedeRecord,
    files: &[FileOf],
    report: &mut ResolveReport,
) -> Result<(), Stop> {
    let Some(quarantine) = &record.quarantine else {
        return Err(Stop("the supersede record names no quarantine".to_owned()));
    };
    let (id, moved) = (quarantine.id.as_str(), quarantine.moved.as_slice());
    report.lines.push(format!(
        "resume: {}/{} names quarantine {id} with {} moved path(s)",
        ctx.stream.raw_name(),
        LedgerName::supersede(ctx.file_name, generation).render(),
        moved.len()
    ));
    for path in moved {
        if let Some(other) = ctx.state.quarantine(path)
            && other != id
        {
            return Err(Stop(format!(
                "{path} is pending under quarantine {other}, not {id}: two replaces claim it; \
                 nothing is written (ADR-062 D11)"
            )));
        }
        if let Some(holder) = ctx.holders(path)?.into_iter().find(|h| h.read_only) {
            return Err(Stop(format!(
                "{path} is held by the read-only root {}: only the replicator writes cold roots, \
                 and never a read-only one (ADR-062 D4); nothing is written",
                holder.root
            )));
        }
    }
    let at_ms = (ctx.now_ms)();
    let pairs: Vec<(LedgerName, Vec<u8>)> = files
        .iter()
        .filter(|file| file.root == HOT_ID)
        .map(|file| (file.name.clone(), file.bytes.clone()))
        .collect();
    let segment = ctx.stream.raw_name();
    let texts = |path: &str| {
        let mut texts: Vec<String> = ctx
            .holders(path)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|holder| holder.text)
            .collect();
        texts.extend(ctx.quarantine_texts(path, id));
        (texts, superseded_shas(record, segment, &pairs, path))
    };
    let written = ctx.write_records(moved, id, &texts, at_ms, report)?;
    let renamed = ctx.rename(moved, id, report)?;
    let cleared = ctx.clear_holds(moved, at_ms, report)?;
    if written + renamed + cleared == 0 {
        report.lines.push(format!(
            "the replace of {} under {id} is complete; the re-import is pending: run `mie \
             archive-import`",
            ctx.archive
        ));
    } else {
        report.lines.push(ctx.next_step(id));
    }
    Ok(())
}

/// The replace the resume completes (RQ7), as (generation, record): F's
/// latest supersede record on hot when its generation is above every entry
/// and it carries a quarantine; otherwise the latest record on hot with a
/// quarantine that `unfinished` says some root's layout still holds (a
/// replace stopped between cold roots whose entry exists, review of #215,
/// finding 1). I1 keeps the importer from writing that entry now, but a
/// store from before the fix, or a root restored from an old backup, can
/// still be in that state, and only the resume moves the copies left.
pub fn pending_replace(
    files: &[FileOf],
    unfinished: &dyn Fn(&SupersedeRecord) -> bool,
) -> Option<(u32, SupersedeRecord)> {
    use mie_adapter_binance::archive::ledger::LedgerKind;
    let highest_entry = files
        .iter()
        .filter(|f| f.name.kind == LedgerKind::Entry)
        .map(|f| f.name.generation)
        .max();
    let mut records: Vec<(u32, SupersedeRecord)> = files
        .iter()
        .filter(|f| f.root == HOT_ID && f.name.kind == LedgerKind::Supersede)
        .filter_map(|f| {
            std::str::from_utf8(&f.bytes)
                .ok()
                .and_then(|text| SupersedeRecord::parse(text).ok())
                .map(|record| (f.name.generation, record))
        })
        .collect();
    records.sort_by_key(|(generation, _)| std::cmp::Reverse(*generation));
    if let Some((m, latest)) = records.first()
        && highest_entry.is_none_or(|g| g < *m)
    {
        return latest.quarantine.is_some().then(|| (*m, latest.clone()));
    }
    records
        .into_iter()
        .find(|(_, record)| record.quarantine.is_some() && unfinished(record))
}
