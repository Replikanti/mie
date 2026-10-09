//! The experiment specification: parsing, validation and the canonical
//! spec text v1 (ADR-040; grammar in the [`research`](super) module docs).

use super::{DataVersion, ExperimentId, HypothesisId, RuleParam, RuleRef, SpecValue, parse_hex16};
use crate::feature::{
    FeatureKey, FeatureRegistry, FeatureSet, FeatureSetVersion, parse_versioned_id,
};
use crate::fingerprint::{Fingerprint, Fingerprinter};
use crate::num::{Rate, SCALE};
use crate::regime::RegimeLabel;
use crate::time::EventTime;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// The first line of spec text v1.
pub const HEADER: &str = "mie-experiment 1";

/// Largest latency assumption: one hour.
pub const MAX_LATENCY_MS: u32 = 3_600_000;

/// Longest `text:` parameter value in bytes.
pub const MAX_TEXT_LEN: usize = 64;

/// The fields of an experiment: the fourteen of the Strategy Research Lab
/// brief plus the hypothesis link, in canonical order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpecField {
    /// `hypothesis`: the hypothesis the experiment tests.
    Hypothesis,
    /// `sample`: the sample period.
    Sample,
    /// `data`: the data version.
    Data,
    /// `features`: the feature version.
    Features,
    /// `state-filter`: Market State filters.
    StateFilter,
    /// `regime-filter`: regime filters.
    RegimeFilter,
    /// `location`: the location rule.
    Location,
    /// `trigger`: the trigger rule.
    Trigger,
    /// `entry`: the entry rule.
    Entry,
    /// `invalidation`: the invalidation rule.
    Invalidation,
    /// `target`: the target and management rule.
    Target,
    /// `fees`: the fee assumption.
    Fees,
    /// `slippage`: the slippage assumption.
    Slippage,
    /// `funding`: the funding assumption.
    Funding,
    /// `latency`: the latency assumption.
    Latency,
}

impl SpecField {
    /// Every field, in canonical order.
    pub const ALL: [Self; 15] = [
        Self::Hypothesis,
        Self::Sample,
        Self::Data,
        Self::Features,
        Self::StateFilter,
        Self::RegimeFilter,
        Self::Location,
        Self::Trigger,
        Self::Entry,
        Self::Invalidation,
        Self::Target,
        Self::Fees,
        Self::Slippage,
        Self::Funding,
        Self::Latency,
    ];

    /// The line key.
    pub const fn key(self) -> &'static str {
        match self {
            Self::Hypothesis => "hypothesis",
            Self::Sample => "sample",
            Self::Data => "data",
            Self::Features => "features",
            Self::StateFilter => "state-filter",
            Self::RegimeFilter => "regime-filter",
            Self::Location => "location",
            Self::Trigger => "trigger",
            Self::Entry => "entry",
            Self::Invalidation => "invalidation",
            Self::Target => "target",
            Self::Fees => "fees",
            Self::Slippage => "slippage",
            Self::Funding => "funding",
            Self::Latency => "latency",
        }
    }

    /// What the field is, as error messages name it.
    pub const fn category(self) -> &'static str {
        match self {
            Self::Hypothesis => "hypothesis link",
            Self::Sample => "sample period",
            Self::Data => "data version",
            Self::Features => "feature version",
            Self::StateFilter => "Market State filter",
            Self::RegimeFilter => "regime filter",
            Self::Location => "location rule",
            Self::Trigger => "trigger rule",
            Self::Entry => "entry rule",
            Self::Invalidation => "invalidation rule",
            Self::Target => "target and management rule",
            Self::Fees | Self::Slippage | Self::Funding => "cost assumption",
            Self::Latency => "latency assumption",
        }
    }

    /// The line to add, as error messages suggest it.
    pub const fn syntax(self) -> &'static str {
        match self {
            Self::Hypothesis => "hypothesis <id>",
            Self::Sample => "sample <start_ms> <end_ms>",
            Self::Data => "data <64 hex dataset version>",
            Self::Features => "features <16 hex feature-set version> <id@N,...|none>",
            Self::StateFilter => "state-filter <rule id@N> [<name>=<type>:<value> ...] (or none)",
            Self::RegimeFilter => "regime-filter <volatility.regime.* id@N> <LABEL,...> (or none)",
            Self::Location
            | Self::Trigger
            | Self::Entry
            | Self::Invalidation
            | Self::Target
            | Self::Slippage
            | Self::Funding => "<key> <rule id@N> [<name>=<type>:<value> ...]",
            Self::Fees => "fees maker=<decimal> taker=<decimal>",
            Self::Latency => "latency <ms>",
        }
    }

    fn from_key(key: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|field| field.key() == key)
    }

    /// Filter fields take one or more lines; every other field exactly one.
    const fn repeatable(self) -> bool {
        matches!(self, Self::StateFilter | Self::RegimeFilter)
    }
}

impl fmt::Display for SpecField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.key())
    }
}

/// Why spec text was rejected. Line numbers count from 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecError {
    /// The first line is not [`HEADER`].
    Header {
        /// The first line as found.
        found: String,
    },
    /// A required field has no line.
    Missing {
        /// The field.
        field: SpecField,
    },
    /// A single-line field appears again, or a filter line repeats.
    Duplicate {
        /// The repeated line.
        line: usize,
        /// The field.
        field: SpecField,
    },
    /// The line's key is not a spec field.
    UnknownKey {
        /// The line.
        line: usize,
        /// The key as found.
        key: String,
    },
    /// The line's arguments break the field's grammar.
    Malformed {
        /// The line.
        line: usize,
        /// The field.
        field: SpecField,
        /// What is wrong.
        reason: String,
    },
    /// The sample period is empty: its start is not below its end.
    EmptySample {
        /// The `sample` line.
        line: usize,
    },
    /// The declared feature-set version is not the version of the listed
    /// features.
    FeatureSetVersionMismatch {
        /// The `features` line.
        line: usize,
        /// The version on the line.
        declared: FeatureSetVersion,
        /// The version the list computes.
        computed: FeatureSetVersion,
    },
    /// A rule parameter or regime filter names a feature outside the spec's
    /// feature set.
    FeatureNotInSet {
        /// The line.
        line: usize,
        /// The feature.
        feature: FeatureKey,
    },
    /// A regime filter names a feature outside the `volatility.regime.`
    /// family.
    NotARegimeFeature {
        /// The line.
        line: usize,
        /// The feature.
        feature: FeatureKey,
    },
    /// A second `regime-filter` line names a feature that another line
    /// already filters: every admitted label of a feature goes on one line.
    RegimeFeatureRepeated {
        /// The later line.
        line: usize,
        /// The feature.
        feature: FeatureKey,
    },
    /// `none` appears next to filter lines of the same field.
    NoneWithFilters {
        /// The `none` line.
        line: usize,
        /// The field.
        field: SpecField,
    },
}

impl SpecError {
    /// The line the error points at; `None` for a missing field.
    pub fn line(&self) -> Option<usize> {
        match self {
            Self::Header { .. } => Some(1),
            Self::Missing { .. } => None,
            Self::Duplicate { line, .. }
            | Self::UnknownKey { line, .. }
            | Self::Malformed { line, .. }
            | Self::EmptySample { line }
            | Self::FeatureSetVersionMismatch { line, .. }
            | Self::FeatureNotInSet { line, .. }
            | Self::NotARegimeFeature { line, .. }
            | Self::RegimeFeatureRepeated { line, .. }
            | Self::NoneWithFilters { line, .. } => Some(*line),
        }
    }
}

impl fmt::Display for SpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Header { found } => {
                write!(f, "line 1: expected the header {HEADER:?}, found {found:?}")
            }
            Self::Missing { field } => write!(
                f,
                "missing {} {:?}: add a line `{}`",
                field.category(),
                field.key(),
                field.syntax().replace("<key>", field.key())
            ),
            Self::Duplicate { line, field } if field.repeatable() => {
                write!(f, "line {line}: this {field} line is given twice")
            }
            Self::Duplicate { line, field } => write!(
                f,
                "line {line}: {field} is given twice; a spec has one {}",
                field.category()
            ),
            Self::UnknownKey { line, key } => write!(
                f,
                "line {line}: unknown key {key:?} (keys: {})",
                SpecField::ALL.map(SpecField::key).join(", ")
            ),
            Self::Malformed {
                line,
                field,
                reason,
            } => write!(f, "line {line}: {field}: {reason}"),
            Self::EmptySample { line } => write!(
                f,
                "line {line}: sample: the period is empty; its start must be below its end"
            ),
            Self::FeatureSetVersionMismatch {
                line,
                declared,
                computed,
            } => write!(
                f,
                "line {line}: features: declared version {declared}, but the listed features \
                 have version {computed}"
            ),
            Self::FeatureNotInSet { line, feature } => write!(
                f,
                "line {line}: {feature} is not in the experiment's feature set; add it to the \
                 features line"
            ),
            Self::NotARegimeFeature { line, feature } => write!(
                f,
                "line {line}: regime-filter: {feature} is not a volatility.regime.* feature"
            ),
            Self::RegimeFeatureRepeated { line, feature } => write!(
                f,
                "line {line}: regime-filter: {feature} is already filtered; list every admitted \
                 label on one line (`regime-filter {feature} LABEL,...`)"
            ),
            Self::NoneWithFilters { line, field } => write!(
                f,
                "line {line}: {field} none next to {field} lines; use either none or filters"
            ),
        }
    }
}

impl std::error::Error for SpecError {}

/// The sample period: the half-open event-time window `[start, end)`,
/// never empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SamplePeriod {
    start: EventTime,
    end: EventTime,
}

impl SamplePeriod {
    /// The first included event time.
    pub const fn start(self) -> EventTime {
        self.start
    }

    /// The first excluded event time.
    pub const fn end(self) -> EventTime {
        self.end
    }
}

/// Exchange fees as exact fractions of notional, `0 <= fee < 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeSchedule {
    /// The maker fee.
    pub maker: Rate,
    /// The taker fee.
    pub taker: Rate,
}

/// A regime filter: the experiment admits only the listed labels of one
/// `volatility.regime.*` feature (ADR-017, ADR-033). A spec has at most one
/// filter per feature, and all of its filters must admit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegimeFilter {
    /// The regime feature.
    pub feature: FeatureKey,
    /// The admitted labels; non-empty.
    pub labels: BTreeSet<RegimeLabel>,
}

impl fmt::Display for RegimeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.feature)?;
        for (i, label) in self.labels.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{label}")?;
        }
        Ok(())
    }
}

/// A validated experiment specification (ADR-040). Constructed only by
/// [`ExperimentSpec::parse`], so every instance satisfies the grammar and
/// the cross-field rules; equal canonical text means an equal spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperimentSpec {
    hypothesis: HypothesisId,
    sample: SamplePeriod,
    data: DataVersion,
    features: FeatureSet,
    state_filters: Vec<RuleRef>,
    regime_filters: Vec<RegimeFilter>,
    location: RuleRef,
    trigger: RuleRef,
    entry: RuleRef,
    invalidation: RuleRef,
    target: RuleRef,
    fees: FeeSchedule,
    slippage: RuleRef,
    funding: RuleRef,
    latency_ms: u32,
    canonical: String,
    id: ExperimentId,
}

impl ExperimentSpec {
    /// Parses and validates spec text v1, resolving features through
    /// `registry`.
    ///
    /// # Errors
    ///
    /// Every [`SpecError`] found, in line order, then the missing fields in
    /// canonical order. A wrong header is reported alone.
    pub fn parse(text: &str, registry: &FeatureRegistry) -> Result<Self, Vec<SpecError>> {
        let mut lines = text.split('\n');
        let header = lines.next().unwrap_or_default();
        if header != HEADER {
            return Err(vec![SpecError::Header {
                found: header.to_owned(),
            }]);
        }
        let mut draft = Draft::default();
        for (index, raw) in lines.enumerate() {
            draft.line(index + 2, raw, registry);
        }
        draft.finish()
    }

    /// The canonical spec text (module docs of [`research`](super)).
    pub fn canonical_text(&self) -> &str {
        &self.canonical
    }

    /// The experiment id: the fingerprint of [`Self::canonical_text`].
    pub fn id(&self) -> ExperimentId {
        self.id
    }

    /// The hypothesis the experiment tests.
    pub fn hypothesis(&self) -> &HypothesisId {
        &self.hypothesis
    }

    /// The sample period.
    pub fn sample(&self) -> SamplePeriod {
        self.sample
    }

    /// The version of the raw data the experiment reads.
    pub fn data_version(&self) -> &DataVersion {
        &self.data
    }

    /// The experiment's own feature set (ADR-029 D7).
    pub fn features(&self) -> &FeatureSet {
        &self.features
    }

    /// The Market State filters, sorted by canonical line; empty means
    /// explicitly unfiltered.
    pub fn state_filters(&self) -> &[RuleRef] {
        &self.state_filters
    }

    /// The regime filters, sorted by canonical line, at most one per
    /// feature; an event passes when every filter admits its label. Empty
    /// means explicitly unfiltered.
    pub fn regime_filters(&self) -> &[RegimeFilter] {
        &self.regime_filters
    }

    /// The location rule.
    pub fn location(&self) -> &RuleRef {
        &self.location
    }

    /// The trigger rule.
    pub fn trigger(&self) -> &RuleRef {
        &self.trigger
    }

    /// The entry rule.
    pub fn entry(&self) -> &RuleRef {
        &self.entry
    }

    /// The invalidation rule.
    pub fn invalidation(&self) -> &RuleRef {
        &self.invalidation
    }

    /// The target and management rule.
    pub fn target(&self) -> &RuleRef {
        &self.target
    }

    /// The fee assumption.
    pub fn fees(&self) -> FeeSchedule {
        self.fees
    }

    /// The slippage assumption.
    pub fn slippage(&self) -> &RuleRef {
        &self.slippage
    }

    /// The funding assumption.
    pub fn funding(&self) -> &RuleRef {
        &self.funding
    }

    /// The latency assumption in milliseconds.
    pub fn latency_ms(&self) -> u32 {
        self.latency_ms
    }

    fn render(&self) -> String {
        let mut out = String::new();
        let mut line = |key: &str, args: &str| {
            out.push_str(key);
            out.push(' ');
            out.push_str(args);
            out.push('\n');
        };
        line("mie-experiment", "1");
        line("hypothesis", self.hypothesis.as_str());
        line(
            "sample",
            &format!(
                "{} {}",
                self.sample.start.as_millis(),
                self.sample.end.as_millis()
            ),
        );
        line("data", self.data.as_str());
        let members = self.features.to_string();
        line(
            "features",
            &format!(
                "{} {}",
                self.features.version(),
                if members.is_empty() { "none" } else { &members }
            ),
        );
        if self.state_filters.is_empty() {
            line("state-filter", "none");
        }
        for filter in &self.state_filters {
            line("state-filter", &filter.to_string());
        }
        if self.regime_filters.is_empty() {
            line("regime-filter", "none");
        }
        for filter in &self.regime_filters {
            line("regime-filter", &filter.to_string());
        }
        line("location", &self.location.to_string());
        line("trigger", &self.trigger.to_string());
        line("entry", &self.entry.to_string());
        line("invalidation", &self.invalidation.to_string());
        line("target", &self.target.to_string());
        line(
            "fees",
            &format!("maker={} taker={}", self.fees.maker, self.fees.taker),
        );
        line("slippage", &self.slippage.to_string());
        line("funding", &self.funding.to_string());
        line("latency", &self.latency_ms.to_string());
        out
    }
}

/// The fingerprint of canonical spec text: `write_str` of the text.
fn identify(canonical: &str) -> ExperimentId {
    let mut hasher = Fingerprinter::new();
    hasher.write_str(canonical);
    ExperimentId::from_fingerprint(hasher.finish())
}

/// The fields of a spec as its lines are read, with the line each came
/// from.
#[derive(Default)]
struct Draft {
    /// The first line of every field seen, malformed or not.
    seen: BTreeMap<SpecField, usize>,
    errors: Vec<SpecError>,
    hypothesis: Option<HypothesisId>,
    sample: Option<SamplePeriod>,
    data: Option<DataVersion>,
    features: Option<FeatureSet>,
    /// The single-line rule slots.
    rules: BTreeMap<SpecField, (usize, RuleRef)>,
    state_filters: Vec<(usize, RuleRef)>,
    regime_filters: Vec<(usize, RegimeFilter)>,
    /// The `none` line per filter field.
    nones: BTreeMap<SpecField, usize>,
    fees: Option<FeeSchedule>,
    latency_ms: Option<u32>,
}

impl Draft {
    fn line(&mut self, line: usize, raw: &str, registry: &FeatureRegistry) {
        if raw.is_empty() || raw.starts_with('#') {
            return;
        }
        let tokens: Vec<&str> = raw.split(' ').collect();
        let Some(field) = SpecField::from_key(tokens[0]) else {
            self.errors.push(SpecError::UnknownKey {
                line,
                key: tokens[0].to_owned(),
            });
            return;
        };
        if self.seen.contains_key(&field) && !field.repeatable() {
            self.errors.push(SpecError::Duplicate { line, field });
            return;
        }
        self.seen.entry(field).or_insert(line);
        let args = &tokens[1..];
        let parsed = if !raw.bytes().all(|byte| (0x20..=0x7e).contains(&byte)) {
            Err("only printable ASCII is allowed".to_owned())
        } else if args.is_empty() {
            Err(format!(
                "expected `{}`",
                field.syntax().replace("<key>", field.key())
            ))
        } else if args.iter().any(|arg| arg.is_empty()) {
            Err("arguments are separated by single spaces".to_owned())
        } else {
            self.field(line, field, args, registry)
        };
        if let Err(reason) = parsed {
            self.errors.push(SpecError::Malformed {
                line,
                field,
                reason,
            });
        }
    }

    /// Parses one field's arguments. A grammar error is returned as the
    /// reason; a semantic error is pushed directly.
    fn field(
        &mut self,
        line: usize,
        field: SpecField,
        args: &[&str],
        registry: &FeatureRegistry,
    ) -> Result<(), String> {
        match field {
            SpecField::Hypothesis => {
                let [id] = args else {
                    return Err("expected one hypothesis id".to_owned());
                };
                let id = HypothesisId::new(id).ok_or_else(|| {
                    format!("{id:?} is not an id ([a-z][a-z0-9_]* segments joined by dots)")
                })?;
                self.hypothesis = Some(id);
            }
            SpecField::Sample => {
                let [start, end] = args else {
                    return Err("expected `sample <start_ms> <end_ms>`".to_owned());
                };
                let start = parse_int(start, "start")?;
                let end = parse_int(end, "end")?;
                if start >= end {
                    self.errors.push(SpecError::EmptySample { line });
                } else {
                    self.sample = Some(SamplePeriod {
                        start: EventTime::from_millis(start),
                        end: EventTime::from_millis(end),
                    });
                }
            }
            SpecField::Data => {
                let [hex] = args else {
                    return Err("expected one dataset version".to_owned());
                };
                let data = DataVersion::from_hex(hex).ok_or_else(|| {
                    format!("{hex:?} is not a dataset version (64 lowercase hex)")
                })?;
                self.data = Some(data);
            }
            SpecField::Features => {
                let [hex, list] = args else {
                    return Err("expected `features <16 hex> <id@N,...|none>`".to_owned());
                };
                let declared = parse_hex16(hex)
                    .map(|value| FeatureSetVersion::from_fingerprint(Fingerprint::from_raw(value)))
                    .ok_or_else(|| {
                        format!("{hex:?} is not a feature-set version (16 lowercase hex)")
                    })?;
                let mut keys = Vec::new();
                if *list != "none" {
                    for item in list.split(',') {
                        let definition = registry.resolve(item).map_err(|e| e.to_string())?;
                        keys.push(definition.key);
                    }
                }
                let set = FeatureSet::new(registry, &keys).map_err(|e| e.to_string())?;
                if set.version() != declared {
                    self.errors.push(SpecError::FeatureSetVersionMismatch {
                        line,
                        declared,
                        computed: set.version(),
                    });
                } else {
                    self.features = Some(set);
                }
            }
            SpecField::StateFilter => {
                if args == ["none"] {
                    self.none(line, field);
                } else {
                    let rule = parse_rule(args, registry)?;
                    self.state_filters.push((line, rule));
                }
            }
            SpecField::RegimeFilter => {
                if args == ["none"] {
                    self.none(line, field);
                } else {
                    let [feature, labels] = args else {
                        return Err("expected `regime-filter <id@N> <LABEL,...>`".to_owned());
                    };
                    let feature = registry.resolve(feature).map_err(|e| e.to_string())?.key;
                    let mut set = BTreeSet::new();
                    for name in labels.split(',') {
                        let label = parse_label(name)?;
                        if !set.insert(label) {
                            return Err(format!("label {name} is listed twice"));
                        }
                    }
                    if feature.id.as_str().starts_with("volatility.regime.") {
                        self.regime_filters.push((
                            line,
                            RegimeFilter {
                                feature,
                                labels: set,
                            },
                        ));
                    } else {
                        self.errors
                            .push(SpecError::NotARegimeFeature { line, feature });
                    }
                }
            }
            SpecField::Location
            | SpecField::Trigger
            | SpecField::Entry
            | SpecField::Invalidation
            | SpecField::Target
            | SpecField::Slippage
            | SpecField::Funding => {
                let rule = parse_rule(args, registry)?;
                self.rules.insert(field, (line, rule));
            }
            SpecField::Fees => {
                let mut maker = None;
                let mut taker = None;
                for arg in args {
                    let (name, value) = arg
                        .split_once('=')
                        .ok_or_else(|| format!("{arg:?} is not <name>=<decimal>"))?;
                    let slot = match name {
                        "maker" => &mut maker,
                        "taker" => &mut taker,
                        _ => return Err(format!("unknown fee {name:?} (maker, taker)")),
                    };
                    if slot.is_some() {
                        return Err(format!("{name} is given twice"));
                    }
                    *slot = Some(parse_fee(name, value)?);
                }
                match (maker, taker) {
                    (Some(maker), Some(taker)) => self.fees = Some(FeeSchedule { maker, taker }),
                    (None, _) => return Err("the maker fee is missing".to_owned()),
                    (_, None) => return Err("the taker fee is missing".to_owned()),
                }
            }
            SpecField::Latency => {
                let [millis] = args else {
                    return Err("expected `latency <ms>`".to_owned());
                };
                let latency = canonical_digits(millis)
                    .then(|| millis.parse::<u32>().ok())
                    .flatten()
                    .filter(|&ms| ms <= MAX_LATENCY_MS)
                    .ok_or_else(|| {
                        format!("{millis:?} is not a latency from 0 to {MAX_LATENCY_MS} ms")
                    })?;
                self.latency_ms = Some(latency);
            }
        }
        Ok(())
    }

    fn none(&mut self, line: usize, field: SpecField) {
        match self.nones.entry(field) {
            Entry::Occupied(_) => self.errors.push(SpecError::Duplicate { line, field }),
            Entry::Vacant(slot) => {
                slot.insert(line);
            }
        }
    }

    /// The cross-line checks, then the spec or every error.
    fn finish(mut self) -> Result<ExperimentSpec, Vec<SpecError>> {
        self.state_filters = dedupe(SpecField::StateFilter, self.state_filters, &mut self.errors);
        self.regime_filters = dedupe(
            SpecField::RegimeFilter,
            self.regime_filters,
            &mut self.errors,
        );
        // One line per regime feature, so filters on one feature never need
        // an AND/OR reading: the later lines of a feature are errors.
        let mut by_line: Vec<_> = self
            .regime_filters
            .iter()
            .map(|(line, filter)| (*line, filter.feature))
            .collect();
        by_line.sort();
        let mut filtered = BTreeSet::new();
        for (line, feature) in by_line {
            if !filtered.insert(feature) {
                self.errors
                    .push(SpecError::RegimeFeatureRepeated { line, feature });
            }
        }
        for (&field, &line) in &self.nones {
            let has_filters = match field {
                SpecField::StateFilter => !self.state_filters.is_empty(),
                _ => !self.regime_filters.is_empty(),
            };
            if has_filters {
                self.errors.push(SpecError::NoneWithFilters { line, field });
            }
        }
        if let Some(set) = &self.features {
            let in_set = |key: FeatureKey| set.definitions().any(|d| d.key == key);
            let rules = self
                .rules
                .values()
                .chain(&self.state_filters)
                .flat_map(|(line, rule)| rule.features().map(move |key| (*line, key)));
            let regimes = self
                .regime_filters
                .iter()
                .map(|(line, filter)| (*line, filter.feature));
            for (line, feature) in rules.chain(regimes) {
                if !in_set(feature) {
                    self.errors
                        .push(SpecError::FeatureNotInSet { line, feature });
                }
            }
        }
        // Stable: errors of one line keep the order they were found in.
        self.errors.sort_by_key(SpecError::line);
        for field in SpecField::ALL {
            if !self.seen.contains_key(&field) {
                self.errors.push(SpecError::Missing { field });
            }
        }
        if !self.errors.is_empty() {
            return Err(self.errors);
        }
        let mut rule = |field| {
            self.rules
                .remove(&field)
                .map(|(_, rule)| rule)
                .expect("a seen field without errors is parsed")
        };
        let (location, trigger, entry, invalidation, target, slippage, funding) = (
            rule(SpecField::Location),
            rule(SpecField::Trigger),
            rule(SpecField::Entry),
            rule(SpecField::Invalidation),
            rule(SpecField::Target),
            rule(SpecField::Slippage),
            rule(SpecField::Funding),
        );
        let parsed = "a seen field without errors is parsed";
        let mut spec = ExperimentSpec {
            hypothesis: self.hypothesis.expect(parsed),
            sample: self.sample.expect(parsed),
            data: self.data.expect(parsed),
            features: self.features.expect(parsed),
            state_filters: self.state_filters.into_iter().map(|(_, r)| r).collect(),
            regime_filters: self.regime_filters.into_iter().map(|(_, r)| r).collect(),
            location,
            trigger,
            entry,
            invalidation,
            target,
            fees: self.fees.expect(parsed),
            slippage,
            funding,
            latency_ms: self.latency_ms.expect(parsed),
            canonical: String::new(),
            id: ExperimentId::from_fingerprint(Fingerprint::from_raw(0)),
        };
        spec.canonical = spec.render();
        spec.id = identify(&spec.canonical);
        Ok(spec)
    }
}

/// Sorts filter lines bytewise by their canonical text and reports a
/// repeated line as a duplicate.
fn dedupe<T: fmt::Display>(
    field: SpecField,
    filters: Vec<(usize, T)>,
    errors: &mut Vec<SpecError>,
) -> Vec<(usize, T)> {
    let mut by_text: BTreeMap<String, (usize, T)> = BTreeMap::new();
    for (line, filter) in filters {
        let text = filter.to_string();
        match by_text.entry(text) {
            Entry::Occupied(_) => errors.push(SpecError::Duplicate { line, field }),
            Entry::Vacant(slot) => {
                slot.insert((line, filter));
            }
        }
    }
    by_text.into_values().collect()
}

/// `-?[0-9]+` without a leading `+`; the value is rendered back in plain
/// decimal.
fn parse_int(text: &str, what: &str) -> Result<i64, String> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if !digits.is_empty()
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && let Ok(value) = text.parse()
    {
        return Ok(value);
    }
    Err(format!("{what} {text:?} is not a 64-bit integer"))
}

/// Only ASCII digits, so `+1` and `-1` are rejected for unsigned values.
fn canonical_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

fn parse_fee(name: &str, text: &str) -> Result<Rate, String> {
    let rate: Rate = text
        .parse()
        .map_err(|e| format!("{name} fee {text:?}: {e}"))?;
    if (0..SCALE).contains(&rate.units()) {
        Ok(rate)
    } else {
        Err(format!(
            "{name} fee {text} is a fraction of notional: at least 0 and below 1"
        ))
    }
}

fn parse_label(name: &str) -> Result<RegimeLabel, String> {
    [
        RegimeLabel::Low,
        RegimeLabel::Medium,
        RegimeLabel::High,
        RegimeLabel::Extreme,
    ]
    .into_iter()
    .find(|label| label.as_str() == name)
    .ok_or_else(|| format!("{name:?} is not a regime label (LOW, MEDIUM, HIGH, EXTREME)"))
}

/// Parses a rule reference: `id@N` and `name=type:value` parameters.
fn parse_rule(args: &[&str], registry: &FeatureRegistry) -> Result<RuleRef, String> {
    let (reference, params) = args.split_first().ok_or("expected a rule reference")?;
    let (id, version) = parse_versioned_id(reference)
        .ok_or_else(|| format!("{reference:?} is not a rule reference id@N"))?;
    let mut parsed: Vec<RuleParam> = Vec::with_capacity(params.len());
    for param in params {
        let (name, typed) = param
            .split_once('=')
            .ok_or_else(|| format!("{param:?} is not <name>=<type>:<value>"))?;
        if !is_param_name(name) {
            return Err(format!(
                "parameter name {name:?} does not match [a-z][a-z0-9_]*"
            ));
        }
        if parsed.iter().any(|p| p.name == name) {
            return Err(format!("parameter {name} is given twice"));
        }
        let (kind, value) = typed
            .split_once(':')
            .ok_or_else(|| format!("parameter {name}: {typed:?} is not <type>:<value>"))?;
        let value = parse_value(kind, value, registry)
            .map_err(|reason| format!("parameter {name}: {reason}"))?;
        parsed.push(RuleParam {
            name: name.to_owned(),
            value,
        });
    }
    parsed.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(RuleRef {
        id: id.to_owned(),
        version,
        params: parsed,
    })
}

fn is_param_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn parse_value(kind: &str, value: &str, registry: &FeatureRegistry) -> Result<SpecValue, String> {
    let decimal = |e: crate::num::ParseDecimalError| format!("{kind} {value:?}: {e}");
    Ok(match kind {
        "int" => SpecValue::Int(parse_int(value, "int")?),
        "bool" => match value {
            "true" => SpecValue::Bool(true),
            "false" => SpecValue::Bool(false),
            _ => return Err(format!("bool {value:?} is neither true nor false")),
        },
        "text" => {
            let valid = !value.is_empty()
                && value.len() <= MAX_TEXT_LEN
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'));
            if !valid {
                return Err(format!(
                    "text {value:?} does not match [A-Za-z0-9_.-]{{1,{MAX_TEXT_LEN}}}"
                ));
            }
            SpecValue::Text(value.to_owned())
        }
        "price" => SpecValue::Price(value.parse().map_err(decimal)?),
        "qty" => SpecValue::Qty(value.parse().map_err(decimal)?),
        "rate" => SpecValue::Rate(value.parse().map_err(decimal)?),
        "feature" => SpecValue::Feature(registry.resolve(value).map_err(|e| e.to_string())?.key),
        _ => {
            return Err(format!(
                "unknown type {kind:?} (int, bool, text, price, qty, rate, feature)"
            ));
        }
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::feature::catalog;

    const FEATURES: &str = "bars.time.1h@1,bars.time.1m@1,profile.volume.utc_day@1,\
                            volatility.atr.1h@1,volatility.regime.1h@1";

    /// The version of [`FEATURES`], pinned by `the_golden_spec_is_pinned`.
    const FEATURES_HEX: &str = "4a14739bcf46afb3";

    /// The golden spec, in canonical form.
    pub(crate) fn golden() -> String {
        format!(
            "mie-experiment 1\n\
             hypothesis vah.failed_auction.short\n\
             sample 1759276800000 1761955200000\n\
             data {data}\n\
             features {FEATURES_HEX} {FEATURES}\n\
             state-filter none\n\
             regime-filter volatility.regime.1h@1 HIGH,EXTREME\n\
             location location.at_level@1 level=text:vah tolerance=rate:0.00100000\n\
             trigger trigger.failed_auction@1\n\
             entry entry.next_trade@1\n\
             invalidation invalidation.beyond_extreme@1\n\
             target target.level@1 level=text:poc\n\
             fees maker=0.00020000 taker=0.00050000\n\
             slippage slippage.fixed@1 rate=rate:0.00010000\n\
             funding funding.recorded@1\n\
             latency 250\n",
            data = "0f".repeat(32)
        )
    }

    fn parse(text: &str) -> Result<ExperimentSpec, Vec<SpecError>> {
        ExperimentSpec::parse(text, &catalog::registry())
    }

    /// `golden()` with the line of `key` replaced by `line` (or removed when
    /// `line` is empty).
    fn with_line(key: &str, line: &str) -> String {
        golden()
            .lines()
            .filter_map(|l| {
                if l.split(' ').next() == Some(key) {
                    (!line.is_empty()).then(|| line.to_owned())
                } else {
                    Some(l.to_owned())
                }
            })
            .map(|l| l + "\n")
            .collect()
    }

    fn errors(text: &str) -> Vec<SpecError> {
        parse(text).expect_err("the spec is invalid")
    }

    fn one_error(text: &str) -> SpecError {
        let errors = errors(text);
        assert_eq!(errors.len(), 1, "{errors:?}");
        errors.into_iter().next().unwrap()
    }

    fn malformed(key: &str, line: &str) -> String {
        match one_error(&with_line(key, line)) {
            SpecError::Malformed { reason, .. } => reason,
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn the_golden_spec_is_pinned() {
        let registry = catalog::registry();
        let keys: Vec<_> = FEATURES
            .split(',')
            .map(|text| registry.resolve(text).unwrap().key)
            .collect();
        let version = FeatureSet::new(&registry, &keys).unwrap().version();
        assert_eq!(version.to_string(), FEATURES_HEX);

        let spec = parse(&golden()).unwrap();
        assert_eq!(spec.canonical_text(), golden());
        assert_eq!(spec.id().to_string(), "3f902847001e1998");
        assert_eq!(spec.hypothesis().as_str(), "vah.failed_auction.short");
        assert_eq!(
            spec.sample().start(),
            EventTime::from_millis(1_759_276_800_000)
        );
        assert_eq!(
            spec.sample().end(),
            EventTime::from_millis(1_761_955_200_000)
        );
        assert_eq!(spec.data_version().as_str(), "0f".repeat(32));
        assert_eq!(spec.features().to_string(), FEATURES);
        assert!(spec.state_filters().is_empty());
        assert_eq!(spec.regime_filters().len(), 1);
        assert_eq!(
            spec.location().param("tolerance"),
            Some(&SpecValue::Rate(Rate::from_units(100_000)))
        );
        assert_eq!(spec.trigger().id(), "trigger.failed_auction");
        assert_eq!(spec.entry().version(), 1);
        assert_eq!(spec.invalidation().params(), []);
        assert_eq!(
            spec.target().param("level"),
            Some(&SpecValue::Text("poc".into()))
        );
        assert_eq!(spec.fees().maker, Rate::from_units(20_000));
        assert_eq!(spec.slippage().id(), "slippage.fixed");
        assert_eq!(spec.funding().id(), "funding.recorded");
        assert_eq!(spec.latency_ms(), 250);

        // The id is FNV-1a 64 of `write_str(canonical text)`.
        let mut hasher = Fingerprinter::new();
        hasher.write_str(&golden());
        assert_eq!(spec.id().fingerprint(), hasher.finish());
    }

    #[test]
    fn equivalent_input_renders_the_same_canonical_text() {
        let data = "0f".repeat(32);
        let messy = format!(
            "mie-experiment 1\n\
             # A comment, then a blank line.\n\
             \n\
             latency 250\n\
             funding funding.recorded@1\n\
             slippage slippage.fixed@1 rate=rate:0.0001\n\
             fees taker=0.0005 maker=0.00020000000\n\
             target target.level@1 level=text:poc\n\
             invalidation invalidation.beyond_extreme@1\n\
             entry entry.next_trade@1\n\
             trigger trigger.failed_auction@1\n\
             location location.at_level@1 tolerance=rate:0.001 level=text:vah\n\
             regime-filter volatility.regime.1h@1 EXTREME,HIGH\n\
             state-filter none\n\
             features {FEATURES_HEX} volatility.regime.1h@1,volatility.atr.1h@1,\
             profile.volume.utc_day@1,bars.time.1m@1,bars.time.1h@1\n\
             data {data}\n\
             sample 1759276800000 1761955200000\n\
             hypothesis vah.failed_auction.short"
        );
        let spec = parse(&messy).unwrap();
        assert_eq!(spec.canonical_text(), golden());
        assert_eq!(spec.id(), parse(&golden()).unwrap().id());
    }

    #[test]
    fn filter_lines_sort_bytewise() {
        let text = with_line(
            "state-filter",
            "state-filter z.rule@1\nstate-filter a.rule@2 x=int:-1 b=bool:false",
        );
        let spec = parse(&text).unwrap();
        assert!(
            spec.canonical_text()
                .contains("state-filter a.rule@2 b=bool:false x=int:-1\nstate-filter z.rule@1\n")
        );
        let swapped = with_line(
            "state-filter",
            "state-filter a.rule@2 x=int:-1 b=bool:false\nstate-filter z.rule@1",
        );
        assert_eq!(parse(&swapped).unwrap(), spec);
    }

    #[test]
    fn every_field_changes_the_id() {
        let base = parse(&golden()).unwrap().id();
        let other_features = "bars.time.1h@1,volatility.atr.1h@1,volatility.regime.1h@1";
        let registry = catalog::registry();
        let keys: Vec<_> = other_features
            .split(',')
            .map(|text| registry.resolve(text).unwrap().key)
            .collect();
        let other_hex = FeatureSet::new(&registry, &keys).unwrap().version();
        let changes = [
            (
                "hypothesis",
                "hypothesis vah.failed_auction.long".to_owned(),
            ),
            ("sample", "sample 1759276800000 1761955200001".to_owned()),
            ("data", format!("data {}", "0e".repeat(32))),
            ("features", format!("features {other_hex} {other_features}")),
            ("state-filter", "state-filter a.rule@1".to_owned()),
            (
                "regime-filter",
                "regime-filter volatility.regime.1h@1 HIGH".to_owned(),
            ),
            (
                "location",
                "location location.at_level@2 level=text:vah tolerance=rate:0.001".to_owned(),
            ),
            (
                "trigger",
                "trigger trigger.failed_auction@1 strict=bool:true".to_owned(),
            ),
            ("entry", "entry entry.limit@1".to_owned()),
            (
                "invalidation",
                "invalidation invalidation.beyond_extreme@2".to_owned(),
            ),
            ("target", "target target.level@1 level=text:vwap".to_owned()),
            ("fees", "fees maker=0.0002 taker=0.0004".to_owned()),
            (
                "slippage",
                "slippage slippage.fixed@1 rate=rate:0.0002".to_owned(),
            ),
            ("funding", "funding funding.none@1".to_owned()),
            ("latency", "latency 251".to_owned()),
        ];
        assert_eq!(changes.len(), SpecField::ALL.len());
        let mut ids = BTreeSet::from([base]);
        for (key, line) in changes {
            let spec = parse(&with_line(key, &line)).unwrap_or_else(|e| panic!("{key}: {e:?}"));
            assert!(ids.insert(spec.id()), "{key} did not change the id");
        }
    }

    #[test]
    fn every_missing_field_is_reported_once() {
        for field in SpecField::ALL {
            let text = with_line(field.key(), "");
            let errors = errors(&text);
            assert_eq!(errors, [SpecError::Missing { field }], "{field}");
        }
        let message = |key: &str| one_error(&with_line(key, "")).to_string();
        assert_eq!(
            message("fees"),
            "missing cost assumption \"fees\": add a line `fees maker=<decimal> taker=<decimal>`"
        );
        assert_eq!(
            message("slippage"),
            "missing cost assumption \"slippage\": add a line \
             `slippage <rule id@N> [<name>=<type>:<value> ...]`"
        );
        assert_eq!(
            message("funding"),
            "missing cost assumption \"funding\": add a line \
             `funding <rule id@N> [<name>=<type>:<value> ...]`"
        );
        assert_eq!(
            message("latency"),
            "missing latency assumption \"latency\": add a line `latency <ms>`"
        );
    }

    #[test]
    fn every_error_is_reported_in_line_order() {
        let text = golden()
            .replace("latency 250\n", "")
            .replace("fees maker=0.00020000 taker=0.00050000\n", "")
            .replace("sample 1759276800000", "sample 1761955200000")
            .replace("hypothesis vah.failed_auction.short", "hypothesis Vah")
            + "colour blue\nhypothesis again\n";
        let errors = errors(&text);
        assert_eq!(
            errors.iter().map(SpecError::line).collect::<Vec<_>>(),
            [Some(2), Some(3), Some(15), Some(16), None, None]
        );
        assert!(matches!(
            errors[0],
            SpecError::Malformed {
                field: SpecField::Hypothesis,
                ..
            }
        ));
        assert_eq!(errors[1], SpecError::EmptySample { line: 3 });
        assert_eq!(
            errors[2],
            SpecError::UnknownKey {
                line: 15,
                key: "colour".to_owned()
            }
        );
        assert_eq!(
            errors[3],
            SpecError::Duplicate {
                line: 16,
                field: SpecField::Hypothesis
            }
        );
        assert_eq!(
            errors[4..],
            [
                SpecError::Missing {
                    field: SpecField::Fees
                },
                SpecError::Missing {
                    field: SpecField::Latency
                }
            ]
        );
        assert_eq!(
            errors[3].to_string(),
            "line 16: hypothesis is given twice; a spec has one hypothesis link"
        );
    }

    #[test]
    fn the_header_comes_first() {
        for text in [
            String::new(),
            golden().replacen("mie-experiment 1", "mie-experiment 2", 1),
        ] {
            assert!(
                matches!(one_error(&text), SpecError::Header { .. }),
                "{text:?}"
            );
        }
        let commented = format!("# comment\n{}", golden());
        assert_eq!(
            one_error(&commented).to_string(),
            "line 1: expected the header \"mie-experiment 1\", found \"# comment\""
        );
    }

    #[test]
    fn grammar_errors_name_the_line_and_field() {
        let data = |hex: String| malformed("data", &format!("data {hex}"));
        assert!(data("0F".repeat(32)).contains("64 lowercase hex"));
        assert!(data("0f".repeat(31)).contains("64 lowercase hex"));
        assert!(
            malformed("features", &format!("features ABCDEF0123456789 {FEATURES}"))
                .contains("16 lowercase hex")
        );
        assert!(
            malformed(
                "features",
                &format!("features {FEATURES_HEX} bars.time.1h@01")
            )
            .contains("not of the form id@N")
        );
        assert!(
            malformed("trigger", "trigger trigger.failed_auction@01")
                .contains("is not a rule reference id@N")
        );
        assert!(
            malformed("trigger", "trigger trigger.failed_auction@1 n=float:1.5")
                .contains("unknown type \"float\"")
        );
        assert!(
            malformed(
                "trigger",
                "trigger trigger.failed_auction@1 n=int:1 n=int:2"
            )
            .contains("parameter n is given twice")
        );
        assert!(
            malformed("trigger", "trigger trigger.failed_auction@1 N=int:1")
                .contains("does not match [a-z][a-z0-9_]*")
        );
        assert!(
            malformed("trigger", "trigger trigger.failed_auction@1 n=int:+1")
                .contains("not a 64-bit integer")
        );
        assert!(
            malformed("trigger", "trigger trigger.failed_auction@1 n=text:a/b")
                .contains("[A-Za-z0-9_.-]{1,64}")
        );
        assert!(
            malformed(
                "trigger",
                "trigger trigger.failed_auction@1 n=price:1.000000001"
            )
            .contains("beyond 8 decimal places")
        );
        assert!(
            malformed("trigger", "trigger trigger.failed_auction@1 n=bool:yes")
                .contains("neither true nor false")
        );
        assert!(malformed("fees", "fees maker=-0.0001 taker=0.0005").contains("at least 0"));
        assert!(malformed("fees", "fees maker=0.0002 taker=1").contains("below 1"));
        assert!(malformed("fees", "fees maker=0.0002").contains("taker fee is missing"));
        assert!(malformed("fees", "fees maker=0.0002 maker=0.0002").contains("given twice"));
        assert!(
            malformed("fees", "fees maker=0.0002 taker=0.0005 rebate=0").contains("unknown fee")
        );
        for latency in ["latency 3600001", "latency -1", "latency +1", "latency 1.5"] {
            assert!(
                malformed("latency", latency).contains("not a latency"),
                "{latency}"
            );
        }
        assert_eq!(
            malformed("latency", "latency 3600000 x"),
            "expected `latency <ms>`"
        );
        assert!(
            malformed("latency", "latency  250").contains("single spaces"),
            "a double space"
        );
        assert!(malformed("latency", "latency 250\r").contains("printable ASCII"));
        assert!(
            malformed(
                "regime-filter",
                "regime-filter volatility.regime.1h@1 HIGH,CALM"
            )
            .contains("\"CALM\" is not a regime label")
        );
        assert!(
            malformed(
                "regime-filter",
                "regime-filter volatility.regime.1h@1 HIGH,HIGH"
            )
            .contains("listed twice")
        );
        assert!(
            malformed("regime-filter", "regime-filter volatility.regime.1h@1")
                .contains("regime-filter <id@N> <LABEL,...>")
        );
        assert!(malformed("sample", "sample 1 x").contains("end \"x\""));
        let error = one_error(&with_line("latency", "latency 3600001"));
        assert_eq!(
            error.to_string(),
            "line 16: latency: \"3600001\" is not a latency from 0 to 3600000 ms"
        );
    }

    #[test]
    fn the_sample_must_not_be_empty() {
        for sample in ["sample 5 5", "sample 6 5"] {
            assert_eq!(
                one_error(&with_line("sample", sample)),
                SpecError::EmptySample { line: 3 }
            );
        }
        assert!(parse(&with_line("sample", "sample -5 -4")).is_ok());
    }

    #[test]
    fn the_feature_list_must_be_closed_and_match_its_version() {
        // profile.volume.utc_day@1 builds on bars.time.1m@1.
        let unclosed = "bars.time.1h@1,profile.volume.utc_day@1,volatility.atr.1h@1,\
                        volatility.regime.1h@1";
        assert!(
            malformed("features", &format!("features {FEATURES_HEX} {unclosed}"))
                .contains("requires bars.time.1m@1, which is not in the set")
        );
        let error = one_error(&with_line(
            "features",
            &format!("features 0123456789abcdef {FEATURES}"),
        ));
        let SpecError::FeatureSetVersionMismatch {
            line: 5, declared, ..
        } = error
        else {
            panic!("{error:?}");
        };
        assert_eq!(declared.to_string(), "0123456789abcdef");
    }

    #[test]
    fn referenced_features_must_be_in_the_set() {
        let atr = FeatureKey::new("volatility.atr.1h", 1);
        let trades = FeatureKey::new("trade.last_price", 1);
        let text = with_line(
            "trigger",
            "trigger trigger.atr_break@1 atr=feature:volatility.atr.1h@1 \
             last=feature:trade.last_price@1",
        );
        assert_eq!(
            one_error(&text),
            SpecError::FeatureNotInSet {
                line: 9,
                feature: trades
            }
        );
        let fine = with_line(
            "trigger",
            "trigger trigger.atr_break@1 atr=feature:volatility.atr.1h@1",
        );
        assert_eq!(
            parse(&fine).unwrap().trigger().param("atr"),
            Some(&SpecValue::Feature(atr))
        );

        // A regime filter needs its feature in the set too.
        let none_hex = FeatureSet::new(&catalog::registry(), &[])
            .unwrap()
            .version();
        let text = with_line("features", &format!("features {none_hex} none"));
        assert_eq!(
            one_error(&text),
            SpecError::FeatureNotInSet {
                line: 7,
                feature: FeatureKey::new("volatility.regime.1h", 1)
            }
        );
        let unfiltered = with_line("regime-filter", "regime-filter none").replace(
            &format!("{FEATURES_HEX} {FEATURES}"),
            &format!("{none_hex} none"),
        );
        let spec = parse(&unfiltered).unwrap();
        assert!(
            spec.canonical_text()
                .contains(&format!("features {none_hex} none\n"))
        );
        assert!(spec.regime_filters().is_empty());
    }

    #[test]
    fn regime_filters_take_regime_features_only() {
        let error = one_error(&with_line(
            "regime-filter",
            "regime-filter volatility.atr.1h@1 HIGH",
        ));
        assert_eq!(
            error,
            SpecError::NotARegimeFeature {
                line: 7,
                feature: FeatureKey::new("volatility.atr.1h", 1)
            }
        );
        assert_eq!(
            error.to_string(),
            "line 7: regime-filter: volatility.atr.1h@1 is not a volatility.regime.* feature"
        );
    }

    #[test]
    fn none_stands_alone() {
        let text = with_line("state-filter", "state-filter a.rule@1\nstate-filter none");
        let error = one_error(&text);
        assert_eq!(
            error,
            SpecError::NoneWithFilters {
                line: 7,
                field: SpecField::StateFilter
            }
        );
        assert_eq!(
            error.to_string(),
            "line 7: state-filter none next to state-filter lines; use either none or filters"
        );
        let twice = with_line("state-filter", "state-filter none\nstate-filter none");
        assert_eq!(
            one_error(&twice),
            SpecError::Duplicate {
                line: 7,
                field: SpecField::StateFilter
            }
        );
        let repeated = with_line(
            "regime-filter",
            "regime-filter volatility.regime.1h@1 HIGH\nregime-filter volatility.regime.1h@1 HIGH",
        );
        assert_eq!(
            one_error(&repeated),
            SpecError::Duplicate {
                line: 8,
                field: SpecField::RegimeFilter
            }
        );
    }

    #[test]
    fn a_regime_feature_is_filtered_on_one_line_only() {
        // Two lines on one feature would need an AND/OR reading that ids
        // would freeze: `HIGH` + `EXTREME` must be written `HIGH,EXTREME`,
        // and a contradictory `LOW` + `EXTREME` never validates.
        for (first, second) in [("HIGH", "EXTREME"), ("EXTREME", "HIGH"), ("LOW", "EXTREME")] {
            let text = with_line(
                "regime-filter",
                &format!(
                    "regime-filter volatility.regime.1h@1 {first}\n\
                     regime-filter volatility.regime.1h@1 {second}"
                ),
            );
            let error = one_error(&text);
            assert_eq!(
                error,
                SpecError::RegimeFeatureRepeated {
                    line: 8,
                    feature: FeatureKey::new("volatility.regime.1h", 1)
                },
                "{first} + {second}"
            );
            assert_eq!(
                error.to_string(),
                "line 8: regime-filter: volatility.regime.1h@1 is already filtered; list every \
                 admitted label on one line (`regime-filter volatility.regime.1h@1 LABEL,...`)"
            );
        }
        let merged = with_line(
            "regime-filter",
            "regime-filter volatility.regime.1h@1 EXTREME,HIGH",
        );
        assert_eq!(parse(&merged).unwrap().id().to_string(), "3f902847001e1998");
    }

    #[test]
    fn unknown_keys_are_listed() {
        let error = one_error(&format!("{}slipage x@1\n", golden()));
        assert_eq!(
            error.to_string(),
            "line 17: unknown key \"slipage\" (keys: hypothesis, sample, data, features, \
             state-filter, regime-filter, location, trigger, entry, invalidation, target, fees, \
             slippage, funding, latency)"
        );
    }
}
