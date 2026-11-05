//! Experiment results and result text v1 (ADR-040; grammar in the
//! [`research`](super) module docs).

use super::{DataVersion, ExperimentId, ExperimentSpec, SpecError, parse_hex16};
use crate::backtest::text::{self, MetricsScope};
use crate::backtest::{BacktestMetrics, SimulationStats, TradeRecord, is_book_slippage};
use crate::event_hash::EventStreamHash;
use crate::feature::{FeatureRegistry, FeatureSetVersion, is_valid_id, parse_versioned_id};
use crate::fingerprint::Fingerprint;
use crate::num::{Qty, Rate};
use crate::slippage::{BookModel, CALIBRATION_GRID, CalibrationRow, SlippageCalibration};
use crate::state_hash::StateHash;
use crate::time::EventTime;
use std::borrow::Cow;
use std::fmt;

/// The first line of result text v1.
pub const HEADER: &str = "mie-result 1";

/// The deterministic pipeline that produced a result, `id@N`, in the
/// feature key grammar. A new pipeline version records its results next to
/// the old ones.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PipelineKey {
    id: Cow<'static, str>,
    version: u32,
}

impl PipelineKey {
    /// `id@version` for a pipeline defined in code.
    ///
    /// # Panics
    ///
    /// If `id` breaks the grammar of [`is_valid_id`] or `version` is 0 — a
    /// compile error when evaluated in a `const` item.
    pub const fn new(id: &'static str, version: u32) -> Self {
        assert!(is_valid_id(id), "invalid pipeline id");
        assert!(version > 0, "pipeline versions start at 1");
        Self {
            id: Cow::Borrowed(id),
            version,
        }
    }

    /// Parses the canonical text `id@N`; `None` for any other text.
    pub fn parse(text: &str) -> Option<Self> {
        parse_versioned_id(text).map(|(id, version)| Self {
            id: Cow::Owned(id.to_owned()),
            version,
        })
    }

    /// The pipeline id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The pipeline version, from 1.
    pub fn version(&self) -> u32 {
        self.version
    }
}

impl fmt::Display for PipelineKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.version)
    }
}

/// The engine that computed a result's Market State: the feature-set
/// version and the state-hash encoding, the pair that makes two state
/// hashes comparable (ADR-041; `state_hash` module docs, *Versioning*).
/// Displays as `<feature-set 16 hex>-<state encoding>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EngineKey {
    /// The version of the feature set the engine computes.
    pub features: FeatureSetVersion,
    /// The state-hash encoding, from 1.
    pub state_encoding: u32,
}

impl fmt::Display for EngineKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.features, self.state_encoding)
    }
}

/// Where a result is stored: one per experiment, pipeline version and, for
/// a pipeline that reads Market State, engine (ADR-040 D6, ADR-052 D10). A
/// build with another engine records its result next to the old one.
/// Displays as `<experiment hex>/<name>` ([`ResultKey::name`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResultKey {
    /// The experiment.
    pub experiment: ExperimentId,
    /// The pipeline.
    pub pipeline: PipelineKey,
    /// The engine, for an outcome that reads Market State; `None` for one
    /// that does not.
    pub engine: Option<EngineKey>,
}

impl ResultKey {
    /// The key's name within its experiment: `<pipeline id>@<N>` without an
    /// engine, `<pipeline id>@<N>-<feature-set 16 hex>-<state encoding>`
    /// with one.
    pub fn name(&self) -> String {
        match self.engine {
            None => self.pipeline.to_string(),
            Some(engine) => format!("{}-{engine}", self.pipeline),
        }
    }

    /// Parses the canonical [`name`](Self::name) of a key of `experiment`;
    /// `None` for any other text. A pipeline key holds no `-`, so the name
    /// splits at its first `-`.
    pub fn from_name(experiment: ExperimentId, name: &str) -> Option<Self> {
        let (pipeline, engine) = match name.split_once('-') {
            None => (name, None),
            Some((pipeline, engine)) => {
                let (features, encoding) = engine.split_once('-')?;
                let engine = EngineKey {
                    features: FeatureSetVersion::from_fingerprint(Fingerprint::from_raw(
                        parse_hex16(features)?,
                    )),
                    state_encoding: parse_encoding(encoding)?,
                };
                (pipeline, Some(engine))
            }
        };
        let key = Self {
            experiment,
            pipeline: PipelineKey::parse(pipeline)?,
            engine,
        };
        (key.name() == name).then_some(key)
    }
}

impl fmt::Display for ResultKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.experiment, self.name())
    }
}

/// What a pipeline produced, tagged by kind in result text v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// `replay-summary`: the provenance of a replay of the sample — what
    /// was delivered and what the domain rejected (ADR-039 D7, D9).
    ReplaySummary {
        /// The hash of the delivered event sequence.
        stream: EventStreamHash,
        /// Delivered events the domain rejected.
        domain_rejections: u64,
    },
    /// `history-replay-summary`: the sample replayed on the history chain,
    /// after the chain's prefix drove the same engine (ADR-052 D10).
    HistoryReplaySummary(HistoryReplaySummary),
    /// `backtest`: the trades of the spec's hypothesis, simulated on the
    /// history chain (ADR-060 D12, D13). The metric lines are not stored:
    /// they are rendered from `records` and the spec's regime features, so
    /// a text whose metric lines differ from its trade lines is not
    /// canonical.
    Backtest {
        /// The chain provenance, with the lines of
        /// `history-replay-summary`.
        summary: HistoryReplaySummary,
        /// The counts (ADR-060 D4, D9).
        stats: SimulationStats,
        /// One record per taken decision, in decision order.
        records: Vec<TradeRecord>,
    },
    /// `slippage-calibration`: what the book models cost at every trade of
    /// the sample, against its print (ADR-061 D7, D8), with the provenance
    /// lines of `replay-summary`. The mean on each line is not stored: it
    /// is rendered from the line's sum and fills.
    SlippageCalibration {
        /// The hash of the delivered event sequence.
        stream: EventStreamHash,
        /// Delivered events the domain rejected.
        domain_rejections: u64,
        /// The trades observed and one line per model, boxed: 18 lines
        /// outweigh every other kind.
        calibration: Box<SlippageCalibration>,
    },
}

impl Outcome {
    /// The tag on the `outcome` line.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ReplaySummary { .. } => "replay-summary",
            Self::HistoryReplaySummary(_) => "history-replay-summary",
            Self::Backtest { .. } => "backtest",
            Self::SlippageCalibration { .. } => "slippage-calibration",
        }
    }

    /// The engine whose Market State the outcome reads; `None` for an
    /// outcome that reads none.
    pub const fn engine(&self) -> Option<EngineKey> {
        match self {
            Self::ReplaySummary { .. } | Self::SlippageCalibration { .. } => None,
            Self::HistoryReplaySummary(summary) | Self::Backtest { summary, .. } => {
                Some(summary.engine)
            }
        }
    }
}

/// The outcome of `research.replay_summary@2` (ADR-052 D10): the prefix
/// `[origin, sample start)` of the history chain drives the engine without
/// being evaluated, then the sample is replayed as `@1` replays it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryReplaySummary {
    /// The chain's origin.
    pub origin: EventTime,
    /// The time of the last restart after an uncrossable hole, in the
    /// prefix or the sample (ADR-052 D4): the origin of the state at the
    /// sample's end. `None`: no restart, the origin itself.
    pub effective_origin: Option<EventTime>,
    /// The chain's cutover between the archive and the live segment.
    pub cutover: EventTime,
    /// The dataset version of the prefix's replay; `None` when the sample
    /// starts at the origin and the prefix is empty.
    pub prefix_data: Option<DataVersion>,
    /// The hash of the prefix's delivered events.
    pub prefix_stream: EventStreamHash,
    /// Delivered prefix events the domain rejected.
    pub prefix_rejections: u64,
    /// The engine that computed both state hashes.
    pub engine: EngineKey,
    /// The state hash at the sample's start, after the prefix.
    pub state_start: StateHash,
    /// The hash of the sample's delivered events.
    pub stream: EventStreamHash,
    /// Delivered sample events the domain rejected.
    pub rejections: u64,
    /// The state hash at the sample's end.
    pub state_end: StateHash,
}

/// An immutable experiment result: the spec it answers, the pipeline that
/// produced it and the outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperimentResult {
    /// The experiment.
    pub spec: ExperimentSpec,
    /// The pipeline.
    pub pipeline: PipelineKey,
    /// What the pipeline produced.
    pub outcome: Outcome,
}

impl ExperimentResult {
    /// The storage key: the spec's id, the pipeline and the outcome's
    /// engine ([`Outcome::engine`]).
    pub fn key(&self) -> ResultKey {
        ResultKey {
            experiment: self.spec.id(),
            pipeline: self.pipeline.clone(),
            engine: self.outcome.engine(),
        }
    }

    /// Result text v1, ending with the canonical spec text.
    pub fn canonical_text(&self) -> String {
        let mut out = format!(
            "{HEADER}\nexperiment {}\npipeline {}\noutcome {}\n",
            self.spec.id(),
            self.pipeline,
            self.outcome.kind()
        );
        match &self.outcome {
            Outcome::ReplaySummary {
                stream,
                domain_rejections,
            } => out.push_str(&format!(
                "stream {stream}\nrejections {domain_rejections}\n"
            )),
            Outcome::HistoryReplaySummary(summary) => summary.render(&mut out),
            Outcome::Backtest {
                summary,
                stats,
                records,
            } => {
                summary.render(&mut out);
                text::render_counts(stats, &mut out);
                for record in records {
                    text::render_trade(record, &mut out);
                }
                let features = self.spec.regime_features();
                let metrics = BacktestMetrics::compute(records, features.len());
                text::render_metrics(&metrics, &features, &mut out);
            }
            Outcome::SlippageCalibration {
                stream,
                domain_rejections,
                calibration,
            } => {
                out.push_str(&format!(
                    "stream {stream}
rejections {domain_rejections}
"
                ));
                render_calibration(calibration, &mut out);
            }
        }
        out.push_str("spec\n");
        out.push_str(self.spec.canonical_text());
        out
    }

    /// Parses result text v1, resolving the spec's features through
    /// `registry`.
    ///
    /// # Errors
    ///
    /// [`ResultParseError`]: a malformed line (an `outcome` line of another
    /// kind than its pipeline's is one, [`PIPELINE_KINDS`]), an invalid
    /// embedded spec, an `experiment` line that differs from the embedded
    /// spec's id, or text that is valid but not canonical.
    ///
    /// A `backtest` outcome is malformed, too, with a `trade` line whose
    /// shape is not its exit kind's, trade lines out of non-decreasing
    /// decision-key order, counts that break ADR-060 D4's conservation (on
    /// the `triggers` line), a `regime=` list whose length is not the
    /// spec's number of regime features, or a `trade` line whose slipped
    /// fills do not match the spec's slippage rule (ADR-061 D5): present on
    /// every line under a book rule, on none under any other. Its metric
    /// lines are checked for
    /// grammar only here; values that differ from what the trade lines
    /// give make the text not canonical.
    ///
    /// A `slippage-calibration` outcome is malformed, too, with a line
    /// whose fills, book and window do not add up to 2 × `trades`
    /// (ADR-061 D7), a negative sum, or a sum other than 0 without a
    /// priced fill. Its means are checked for grammar only; a mean other
    /// than the line's ⟨sum / fills⟩ makes the text not canonical.
    pub fn parse(text: &str, registry: &FeatureRegistry) -> Result<Self, ResultParseError> {
        let mut lines = Lines {
            rest: text,
            line: 0,
        };
        let malformed = |line: usize, reason: String| ResultParseError::Malformed { line, reason };

        let header = lines.next(HEADER)?;
        if header != HEADER {
            return Err(malformed(1, format!("expected the header {HEADER:?}")));
        }
        let declared = lines
            .next("experiment <16 hex>")?
            .strip_prefix("experiment ")
            .and_then(ExperimentId::from_hex)
            .ok_or_else(|| malformed(2, "expected `experiment <16 hex>`".to_owned()))?;
        let pipeline = lines
            .next("pipeline <id@N>")?
            .strip_prefix("pipeline ")
            .and_then(PipelineKey::parse)
            .ok_or_else(|| malformed(3, "expected `pipeline <id@N>`".to_owned()))?;
        let outcome = match lines.next("outcome <kind>")? {
            "outcome replay-summary" => {
                let stream = lines
                    .next("stream <events>:<16 hex>")?
                    .strip_prefix("stream ")
                    .and_then(parse_stream_hash)
                    .ok_or_else(|| {
                        malformed(5, "expected `stream <events>:<16 hex>`".to_owned())
                    })?;
                let domain_rejections = lines
                    .next("rejections <n>")?
                    .strip_prefix("rejections ")
                    .and_then(|n| n.parse().ok())
                    .ok_or_else(|| malformed(6, "expected `rejections <n>`".to_owned()))?;
                Outcome::ReplaySummary {
                    stream,
                    domain_rejections,
                }
            }
            "outcome history-replay-summary" => {
                Outcome::HistoryReplaySummary(HistoryReplaySummary::parse(&mut lines)?)
            }
            "outcome backtest" => parse_backtest(&mut lines)?,
            "outcome slippage-calibration" => parse_calibration(&mut lines)?,
            other => return Err(malformed(4, format!("unknown outcome line {other:?}"))),
        };
        check_kind(&pipeline, &outcome)?;
        if lines.next("spec")? != "spec" {
            return Err(malformed(lines.line, "expected `spec`".to_owned()));
        }
        let spec = ExperimentSpec::parse(lines.rest, registry).map_err(ResultParseError::Spec)?;
        if spec.id() != declared {
            return Err(ResultParseError::ExperimentMismatch {
                declared,
                computed: spec.id(),
            });
        }
        let result = Self {
            spec,
            pipeline,
            outcome,
        };
        // Every check but the regime arity has run on the way here; the
        // arity needs the spec.
        result.validate()?;
        if result.canonical_text() != text {
            return Err(ResultParseError::NotCanonical);
        }
        Ok(result)
    }

    /// Checks what result text v1 refuses beyond its grammar, with the
    /// error [`parse`](Self::parse) gives for this value's
    /// [`canonical_text`](Self::canonical_text), naming the same line:
    ///
    /// - an `outcome` kind other than its pipeline's ([`PIPELINE_KINDS`]);
    /// - a state-hash encoding of 0;
    /// - for a `backtest` outcome (ADR-060 D12): a record whose values are
    ///   not its exit kind's shape, records whose decision keys decrease,
    ///   counts that break D4's conservation, a regime list whose length is
    ///   not the spec's number of regime features, and slipped fills on a
    ///   record that its spec's slippage rule does not give (ADR-061 D5);
    /// - for a `slippage-calibration` outcome (ADR-061 D7): a line that
    ///   breaks the conservation, a negative sum, or a sum other than 0
    ///   without a priced fill.
    ///
    /// Every other value renders text that parses back to it, so `Ok`
    /// means [`parse`](Self::parse) returns this value for its canonical
    /// text. A store calls this before it writes: the fields are public,
    /// so a value no pipeline produces can be built, and its stored text
    /// would never read back.
    ///
    /// # Errors
    ///
    /// The first [`ResultParseError::Malformed`] in line order, as `parse`
    /// reports it.
    pub fn validate(&self) -> Result<(), ResultParseError> {
        let summary = match &self.outcome {
            Outcome::ReplaySummary { .. } | Outcome::SlippageCalibration { .. } => None,
            Outcome::HistoryReplaySummary(summary) | Outcome::Backtest { summary, .. } => {
                Some(summary)
            }
        };
        if summary.is_some_and(|summary| summary.engine.state_encoding == 0) {
            return Err(ResultParseError::Malformed {
                line: ENCODING_LINE,
                reason: format!("expected `{ENCODING_FORM}`"),
            });
        }
        if let Outcome::Backtest { stats, records, .. } = &self.outcome {
            let mut previous = None;
            for (index, record) in records.iter().enumerate() {
                check_trade(FIRST_TRADE_LINE + index, record, previous)?;
                previous = Some(record);
            }
            check_conservation(TRIGGERS_LINE, stats, records.len())?;
        }
        if let Outcome::SlippageCalibration { calibration, .. } = &self.outcome {
            check_calibration(calibration)?;
        }
        check_kind(&self.pipeline, &self.outcome)?;
        if let Outcome::Backtest { records, .. } = &self.outcome {
            let features = self.spec.regime_features().len();
            if let Some(index) = records
                .iter()
                .position(|record| record.regime.len() != features)
            {
                return Err(ResultParseError::Malformed {
                    line: FIRST_TRADE_LINE + index,
                    reason: format!(
                        "expected {features} `regime=` labels, one per regime feature of the \
                         spec (`none` for zero)"
                    ),
                });
            }
            let book = is_book_slippage(self.spec.slippage());
            if let Some(index) = records
                .iter()
                .position(|record| record.slipped.is_some() != book)
            {
                let rule = self.spec.slippage();
                let reason = if book {
                    format!(
                        "under the book slippage rule {}@{} every trade line ends with \
                         `entry_slipped=` and `exit_slipped=` (ADR-061 D5)",
                        rule.id(),
                        rule.version()
                    )
                } else {
                    format!(
                        "`entry_slipped=` and `exit_slipped=` belong only on the trade lines of \
                         a book slippage rule, not {}@{} (ADR-061 D5)",
                        rule.id(),
                        rule.version()
                    )
                };
                return Err(ResultParseError::Malformed {
                    line: FIRST_TRADE_LINE + index,
                    reason,
                });
            }
        }
        Ok(())
    }
}

/// The line of `state-encoding` in a kind with the chain provenance: the
/// four opening lines, then the eighth provenance line.
const ENCODING_LINE: usize = 12;

/// The form of the `state-encoding` line.
const ENCODING_FORM: &str = "state-encoding <n from 1>";

/// The line of `triggers` in a `backtest` outcome: after the four opening
/// lines and the twelve provenance lines.
const TRIGGERS_LINE: usize = 17;

/// The line of the first `trade` line, after the four count lines.
const FIRST_TRADE_LINE: usize = 21;

/// The line of the first calibration line, `spread`, in a
/// `slippage-calibration` outcome: after the four opening lines, `stream`,
/// `rejections` and `trades`. The grid's lines follow it.
const FIRST_CALIBRATION_LINE: usize = 8;

/// The fields of a calibration line, after its model.
const CALIBRATION_FIELDS: &str = "fills=<n> book=<n> window=<n> sum=<rate> mean=<rate|->";

/// An `outcome` kind other than the one `pipeline` produces is malformed,
/// at the `outcome` line.
fn check_kind(pipeline: &PipelineKey, outcome: &Outcome) -> Result<(), ResultParseError> {
    match kind_of(pipeline).filter(|kind| *kind != outcome.kind()) {
        Some(kind) => Err(ResultParseError::Malformed {
            line: 4,
            reason: format!("expected `outcome {kind}`: the outcome of {pipeline}"),
        }),
        None => Ok(()),
    }
}

/// The checks of one `trade` line at `line` beyond its grammar (ADR-060
/// D12): the shape of its exit kind, and a decision key not below the
/// `previous` line's.
fn check_trade(
    line: usize,
    record: &TradeRecord,
    previous: Option<&TradeRecord>,
) -> Result<(), ResultParseError> {
    let malformed = |reason: String| ResultParseError::Malformed { line, reason };
    text::check_shape(record).map_err(malformed)?;
    if previous.is_some_and(|previous| record.decision < previous.decision) {
        return Err(malformed(
            "the decision key is below the previous trade line's: trade lines come in \
             decision order"
                .to_owned(),
        ));
    }
    Ok(())
}

/// ADR-060 D4's conservation, reported on the `triggers` line.
fn check_conservation(
    line: usize,
    stats: &SimulationStats,
    trades: usize,
) -> Result<(), ResultParseError> {
    if text::conserves(stats, trades) {
        Ok(())
    } else {
        Err(ResultParseError::Malformed {
            line,
            reason: format!(
                "triggers is not filtered + skipped + the {trades} trade lines (ADR-060 D4)"
            ),
        })
    }
}

/// The outcome kind of each pipeline version that has one (ADR-040,
/// ADR-052 D10, ADR-060 D12, ADR-061 D7). A result pairing one of them
/// with another kind is no pipeline's output, and its key would be
/// impossible: `research.replay_summary@2` or `research.backtest@1`
/// without an engine, `research.replay_summary@1` or
/// `research.slippage_calibration@1` with one.
const PIPELINE_KINDS: [(&str, u32, &str); 4] = [
    ("research.replay_summary", 1, "replay-summary"),
    ("research.replay_summary", 2, "history-replay-summary"),
    ("research.backtest", 1, "backtest"),
    ("research.slippage_calibration", 1, "slippage-calibration"),
];

/// The outcome kind `pipeline` produces; `None` for a pipeline version not
/// in [`PIPELINE_KINDS`].
fn kind_of(pipeline: &PipelineKey) -> Option<&'static str> {
    PIPELINE_KINDS
        .iter()
        .find(|(id, version, _)| pipeline.id() == *id && pipeline.version() == *version)
        .map(|(_, _, kind)| *kind)
}

/// The text before the embedded spec, line by line.
struct Lines<'a> {
    rest: &'a str,
    /// The number of the line returned last, from 1.
    line: usize,
}

impl<'a> Lines<'a> {
    fn next(&mut self, expected: &str) -> Result<&'a str, ResultParseError> {
        self.line += 1;
        let (head, tail) =
            self.rest
                .split_once('\n')
                .ok_or_else(|| ResultParseError::Malformed {
                    line: self.line,
                    reason: format!("expected `{expected}`, found the end of the text"),
                })?;
        self.rest = tail;
        Ok(head)
    }

    /// The next line without consuming it; `None` at the end of the text.
    fn peek(&self) -> Option<&'a str> {
        self.rest.split_once('\n').map(|(head, _)| head)
    }

    /// The next line, parsed by `parse`; its error is the reason of a
    /// malformed line that names this one.
    fn with<T>(
        &mut self,
        expected: &str,
        parse: impl FnOnce(&'a str) -> Result<T, String>,
    ) -> Result<T, ResultParseError> {
        let text = self.next(expected)?;
        parse(text).map_err(|reason| ResultParseError::Malformed {
            line: self.line,
            reason,
        })
    }

    /// The next line, `<key> <value>` with `value` in the form `form`,
    /// parsed by `parse`; a line that does not match is malformed and named.
    fn field<T>(
        &mut self,
        key: &str,
        form: &str,
        parse: impl FnOnce(&str) -> Option<T>,
    ) -> Result<T, ResultParseError> {
        let expected = format!("{key} {form}");
        let text = self.next(&expected)?;
        text.strip_prefix(key)
            .and_then(|rest| rest.strip_prefix(' '))
            .and_then(parse)
            .ok_or_else(|| ResultParseError::Malformed {
                line: self.line,
                reason: format!("expected `{expected}`"),
            })
    }
}

impl HistoryReplaySummary {
    /// The kind's twelve lines, which `backtest` repeats (ADR-060 D12).
    fn render(&self, out: &mut String) {
        let Self {
            origin,
            effective_origin,
            cutover,
            prefix_data,
            prefix_stream,
            prefix_rejections,
            engine,
            state_start,
            stream,
            rejections,
            state_end,
        } = self;
        let effective_origin =
            effective_origin.map_or_else(|| "-".to_owned(), |t| t.as_millis().to_string());
        let prefix_data = prefix_data.as_ref().map_or("-", |version| version.as_str());
        out.push_str(&format!(
            "origin {}\n\
             effective-origin {effective_origin}\n\
             cutover {}\n\
             prefix-data {prefix_data}\n\
             prefix-stream {prefix_stream}\n\
             prefix-rejections {prefix_rejections}\n\
             engine-features {}\n\
             state-encoding {}\n\
             state-start {state_start}\n\
             stream {stream}\n\
             rejections {rejections}\n\
             state-end {state_end}\n",
            origin.as_millis(),
            cutover.as_millis(),
            engine.features,
            engine.state_encoding,
        ));
    }

    /// The kind's lines, in their order (the [`research`](super) module
    /// docs).
    fn parse(lines: &mut Lines<'_>) -> Result<Self, ResultParseError> {
        let millis = |text: &str| text.parse().ok().map(EventTime::from_millis);
        let state_hash = |text: &str| {
            parse_hex16(text).map(|v| StateHash::from_fingerprint(Fingerprint::from_raw(v)))
        };
        let origin = lines.field("origin", "<ms>", millis)?;
        let effective_origin = lines.field("effective-origin", "<ms|->", |text| {
            if text == "-" {
                Some(None)
            } else {
                millis(text).map(Some)
            }
        })?;
        let cutover = lines.field("cutover", "<ms>", millis)?;
        let prefix_data = lines.field("prefix-data", "<64 hex|->", |text| {
            if text == "-" {
                Some(None)
            } else {
                DataVersion::from_hex(text).map(Some)
            }
        })?;
        let prefix_stream = lines.field("prefix-stream", "<events>:<16 hex>", parse_stream_hash)?;
        let prefix_rejections = lines.field("prefix-rejections", "<n>", |n| n.parse().ok())?;
        let features = lines.field("engine-features", "<16 hex>", |text| {
            parse_hex16(text).map(|v| FeatureSetVersion::from_fingerprint(Fingerprint::from_raw(v)))
        })?;
        let state_encoding = lines.field("state-encoding", "<n from 1>", parse_encoding)?;
        debug_assert_eq!(lines.line, ENCODING_LINE);
        let state_start = lines.field("state-start", "<16 hex>", state_hash)?;
        let stream = lines.field("stream", "<events>:<16 hex>", parse_stream_hash)?;
        let rejections = lines.field("rejections", "<n>", |n| n.parse().ok())?;
        let state_end = lines.field("state-end", "<16 hex>", state_hash)?;
        Ok(Self {
            origin,
            effective_origin,
            cutover,
            prefix_data,
            prefix_stream,
            prefix_rejections,
            engine: EngineKey {
                features,
                state_encoding,
            },
            state_start,
            stream,
            rejections,
            state_end,
        })
    }
}

/// The lines of a `backtest` outcome after its `outcome` line, up to
/// `spec` (ADR-060 D12): the provenance, the counts, the `trade` lines and
/// the `metrics` lines. The regime arity is checked once the spec has
/// parsed ([`ExperimentResult::validate`]).
fn parse_backtest(lines: &mut Lines<'_>) -> Result<Outcome, ResultParseError> {
    let malformed = |line: usize, reason: String| ResultParseError::Malformed { line, reason };
    let summary = HistoryReplaySummary::parse(lines)?;
    let triggers = lines.with("triggers <n>", |t| text::parse_count(t, "triggers"))?;
    let triggers_line = lines.line;
    let filtered = lines.with("filtered <n>", |t| text::parse_count(t, "filtered"))?;
    let skipped = lines.with(text::SKIPPED_FORM, text::parse_skipped)?;
    let reach_backs = lines.with("reach-backs <n>", |t| text::parse_count(t, "reach-backs"))?;
    let stats = SimulationStats {
        triggers,
        filtered,
        skipped,
        reach_backs,
    };
    let mut records: Vec<TradeRecord> = Vec::new();
    while lines.peek().is_some_and(|line| line.starts_with("trade ")) {
        let record = lines.with("trade <fields>", text::parse_trade)?;
        check_trade(lines.line, &record, records.last())?;
        records.push(record);
    }
    check_conservation(triggers_line, &stats, records.len())?;
    let (scope, _) = lines.with("metrics all <fields>", text::parse_metrics)?;
    if scope != MetricsScope::All {
        return Err(malformed(
            lines.line,
            "expected `metrics all <fields>` before the bucket lines".to_owned(),
        ));
    }
    while lines.peek().is_some_and(|line| line != "spec") {
        lines.with(text::METRICS_FORM, text::parse_metrics)?;
    }
    Ok(Outcome::Backtest {
        summary,
        stats,
        records,
    })
}

/// The `trades` line and one line per model of a `slippage-calibration`
/// outcome (ADR-061 D7): `spread <fields>`, then `depth_walk <q> <fields>`
/// for each size of the grid, in grid order, q in its 8-place form.
fn render_calibration(calibration: &SlippageCalibration, out: &mut String) {
    out.push_str(&format!("trades {}\n", calibration.trades));
    for (model, row) in calibration.rows() {
        let CalibrationRow {
            fills,
            book,
            window,
            sum,
        } = row;
        let mean = row
            .mean()
            .map_or_else(|| "-".to_owned(), |mean| mean.to_string());
        let fields = format!("fills={fills} book={book} window={window} sum={sum} mean={mean}");
        match model {
            BookModel::Spread => out.push_str(&format!("spread {fields}\n")),
            BookModel::DepthWalk(qty) => out.push_str(&format!("depth_walk {qty} {fields}\n")),
        }
    }
}

/// The lines of a `slippage-calibration` outcome after its `outcome` line,
/// up to `spec` (ADR-061 D7): the provenance of `replay-summary`, the
/// `trades` line and the model lines, each checked as
/// [`check_calibration`] checks it.
fn parse_calibration(lines: &mut Lines<'_>) -> Result<Outcome, ResultParseError> {
    let stream = lines.field("stream", "<events>:<16 hex>", parse_stream_hash)?;
    let domain_rejections = lines.field("rejections", "<n>", |n| n.parse().ok())?;
    let trades = lines.field("trades", "<n>", |n| n.parse().ok())?;
    let mut calibration = SlippageCalibration {
        trades,
        ..SlippageCalibration::new()
    };
    let expected = format!("spread {CALIBRATION_FIELDS}");
    calibration.spread = lines.with(&expected, |text| {
        text.strip_prefix("spread ")
            .and_then(parse_calibration_row)
            .ok_or_else(|| format!("expected `{expected}`"))
    })?;
    for (row, qty) in calibration.depth_walk.iter_mut().zip(CALIBRATION_GRID) {
        let expected = format!("depth_walk {qty} {CALIBRATION_FIELDS}");
        *row = lines.with(&expected, |text| {
            text.strip_prefix("depth_walk ")
                .and_then(|rest| rest.split_once(' '))
                .filter(|(size, _)| size.parse::<Qty>() == Ok(qty))
                .and_then(|(_, fields)| parse_calibration_row(fields))
                .ok_or_else(|| {
                    format!("expected `{expected}`: the grid's sizes in order (ADR-061 D8)")
                })
        })?;
    }
    check_calibration(&calibration)?;
    Ok(Outcome::SlippageCalibration {
        stream,
        domain_rejections,
        calibration: Box::new(calibration),
    })
}

/// Parses the fields of a calibration line, [`CALIBRATION_FIELDS`]. The
/// mean is checked for grammar only: it is rendered from the sum and the
/// fills, so another value makes the text not canonical.
fn parse_calibration_row(text: &str) -> Option<CalibrationRow> {
    let mut fields = text.split(' ');
    let mut next = |name: &str| fields.next()?.strip_prefix(name)?.strip_prefix('=');
    let fills = next("fills")?.parse().ok()?;
    let book = next("book")?.parse().ok()?;
    let window = next("window")?.parse().ok()?;
    let sum = next("sum")?.parse::<Rate>().ok()?;
    let mean = next("mean")?;
    if mean != "-" {
        mean.parse::<Rate>().ok()?;
    }
    if fields.next().is_some() {
        return None;
    }
    Some(CalibrationRow {
        fills,
        book,
        window,
        sum,
    })
}

/// The checks of a calibration beyond its grammar, in line order (ADR-061
/// D2, D7): every line accounts for 2 × `trades` fills, and its sum, a sum
/// of rates that are each ≥ 0, is ≥ 0, and 0 without a priced fill.
fn check_calibration(calibration: &SlippageCalibration) -> Result<(), ResultParseError> {
    let fills = 2 * u128::from(calibration.trades);
    for (index, (_, row)) in calibration.rows().enumerate() {
        let reason = if !row.accounts_for(fills) {
            format!(
                "fills + book + window is not 2 × trades = {fills}: every trade gives a buy \
                 and a sell on every line (ADR-061 D7)"
            )
        } else if row.sum.units() < 0 {
            "the sum is negative: every priced fill costs at least its print (ADR-061 D2)"
                .to_owned()
        } else if row.fills == 0 && row.sum.units() != 0 {
            "the sum is not 0 without a priced fill".to_owned()
        } else {
            continue;
        };
        return Err(ResultParseError::Malformed {
            line: FIRST_CALIBRATION_LINE + index,
            reason,
        });
    }
    Ok(())
}

/// Parses the `Display` form of an [`EventStreamHash`],
/// `<events>:<16 hex>`.
fn parse_stream_hash(text: &str) -> Option<EventStreamHash> {
    let (events, hex) = text.split_once(':')?;
    Some(EventStreamHash {
        events: events.parse().ok()?,
        fingerprint: Fingerprint::from_raw(parse_hex16(hex)?),
    })
}

/// Parses a state-hash encoding, a number from 1. Callers that need the
/// canonical form compare it afterwards.
fn parse_encoding(text: &str) -> Option<u32> {
    text.parse().ok().filter(|&n| n > 0)
}

/// Why result text was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultParseError {
    /// A line before the embedded spec breaks the grammar. Lines count from
    /// 1.
    Malformed {
        /// The line.
        line: usize,
        /// What is wrong.
        reason: String,
    },
    /// The embedded spec is invalid; its line numbers count from the line
    /// after `spec`.
    Spec(Vec<SpecError>),
    /// The `experiment` line is not the embedded spec's id.
    ExperimentMismatch {
        /// The id on the `experiment` line.
        declared: ExperimentId,
        /// The id of the embedded spec.
        computed: ExperimentId,
    },
    /// The text parses but is not in canonical form, such as an embedded
    /// spec that is not canonical.
    NotCanonical,
}

impl fmt::Display for ResultParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed { line, reason } => write!(f, "result line {line}: {reason}"),
            Self::Spec(errors) => {
                f.write_str("the embedded spec is invalid: ")?;
                for (i, error) in errors.iter().enumerate() {
                    if i > 0 {
                        f.write_str("; ")?;
                    }
                    write!(f, "{error}")?;
                }
                Ok(())
            }
            Self::ExperimentMismatch { declared, computed } => write!(
                f,
                "the result names experiment {declared}, but its spec is experiment {computed}"
            ),
            Self::NotCanonical => f.write_str("the result text is not in canonical form"),
        }
    }
}

impl std::error::Error for ResultParseError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feature::catalog;
    use crate::research::spec::tests::golden;

    const PIPELINE: PipelineKey = PipelineKey::new("research.replay_summary", 1);

    fn result() -> ExperimentResult {
        ExperimentResult {
            spec: ExperimentSpec::parse(&golden(), &catalog::registry()).unwrap(),
            pipeline: PIPELINE,
            outcome: Outcome::ReplaySummary {
                stream: EventStreamHash {
                    events: 48,
                    fingerprint: Fingerprint::from_raw(0x0123_4567_89ab_cdef),
                },
                domain_rejections: 2,
            },
        }
    }

    fn parse(text: &str) -> Result<ExperimentResult, ResultParseError> {
        ExperimentResult::parse(text, &catalog::registry())
    }

    #[test]
    fn result_text_is_pinned() {
        let result = result();
        assert_eq!(
            result.canonical_text(),
            format!(
                "mie-result 1\n\
                 experiment 3f902847001e1998\n\
                 pipeline research.replay_summary@1\n\
                 outcome replay-summary\n\
                 stream 48:0123456789abcdef\n\
                 rejections 2\n\
                 spec\n\
                 {}",
                golden()
            )
        );
        assert_eq!(
            result.key().to_string(),
            "3f902847001e1998/research.replay_summary@1"
        );
    }

    #[test]
    fn result_text_round_trips() {
        let result = result();
        assert_eq!(parse(&result.canonical_text()), Ok(result));
    }

    #[test]
    fn the_experiment_line_must_match_the_spec() {
        let text = result()
            .canonical_text()
            .replace("experiment 3f902847001e1998", "experiment 3f902847001e1999");
        assert_eq!(
            parse(&text),
            Err(ResultParseError::ExperimentMismatch {
                declared: ExperimentId::from_hex("3f902847001e1999").unwrap(),
                computed: ExperimentId::from_hex("3f902847001e1998").unwrap(),
            })
        );
        assert_eq!(
            parse(&text).unwrap_err().to_string(),
            "the result names experiment 3f902847001e1999, but its spec is experiment \
             3f902847001e1998"
        );
    }

    #[test]
    fn only_canonical_text_parses() {
        let text = result().canonical_text();
        let non_canonical = [
            text.replace("latency 250\n", "latency 250\n# comment\n"),
            text.replace("rejections 2", "rejections 02"),
            text.replace("stream 48:", "stream 048:"),
        ];
        for text in non_canonical {
            assert_eq!(parse(&text), Err(ResultParseError::NotCanonical), "{text}");
        }
    }

    #[test]
    fn malformed_result_text_names_the_line() {
        let text = result().canonical_text();
        let cases = [
            (text.replace("mie-result 1", "mie-result 2"), 1),
            (text.replace("experiment 3f9", "experiment 3F9"), 2),
            (
                text.replace("pipeline research.replay_summary@1", "pipeline x@0"),
                3,
            ),
            (
                text.replace("outcome replay-summary", "outcome walk-forward"),
                4,
            ),
            (text.replace("stream 48:0123456789abcdef", "stream 48"), 5),
            (text.replace("rejections 2", "rejections -2"), 6),
            (text.replace("\nspec\n", "\nspecification\n"), 7),
            ("mie-result 1\nexperiment 3f902847001e1998".to_owned(), 2),
        ];
        for (text, line) in cases {
            match parse(&text) {
                Err(ResultParseError::Malformed { line: found, .. }) => {
                    assert_eq!(found, line, "{text}");
                }
                other => panic!("{text}: {other:?}"),
            }
        }
        let broken_spec = text.replace("latency 250\n", "");
        assert_eq!(
            parse(&broken_spec),
            Err(ResultParseError::Spec(vec![SpecError::Missing {
                field: crate::research::SpecField::Latency
            }]))
        );
    }

    const PIPELINE_V2: PipelineKey = PipelineKey::new("research.replay_summary", 2);

    const ENGINE: EngineKey = EngineKey {
        features: FeatureSetVersion::from_fingerprint(Fingerprint::from_raw(0x0fed_cba9_8765_4321)),
        state_encoding: 1,
    };

    fn stream_hash(events: u64, raw: u64) -> EventStreamHash {
        EventStreamHash {
            events,
            fingerprint: Fingerprint::from_raw(raw),
        }
    }

    fn state_hash(raw: u64) -> StateHash {
        StateHash::from_fingerprint(Fingerprint::from_raw(raw))
    }

    /// A `@2` result with a restart and a prefix.
    fn history_result() -> ExperimentResult {
        ExperimentResult {
            spec: ExperimentSpec::parse(&golden(), &catalog::registry()).unwrap(),
            pipeline: PIPELINE_V2,
            outcome: Outcome::HistoryReplaySummary(HistoryReplaySummary {
                origin: EventTime::from_millis(1_759_276_800_000),
                effective_origin: Some(EventTime::from_millis(1_759_536_000_007)),
                cutover: EventTime::from_millis(1_791_244_800_000),
                prefix_data: DataVersion::from_hex(&"ab".repeat(32)),
                prefix_stream: stream_hash(1_200, 0x1111_2222_3333_4444),
                prefix_rejections: 3,
                engine: ENGINE,
                state_start: state_hash(0x5555_6666_7777_8888),
                stream: stream_hash(48, 0x0123_4567_89ab_cdef),
                rejections: 2,
                state_end: state_hash(0x9999_aaaa_bbbb_cccc),
            }),
        }
    }

    /// A `@2` result whose sample starts at the origin, without a restart.
    fn history_result_at_origin() -> ExperimentResult {
        let mut result = history_result();
        let Outcome::HistoryReplaySummary(summary) = &mut result.outcome else {
            unreachable!();
        };
        summary.effective_origin = None;
        summary.prefix_data = None;
        summary.prefix_stream = stream_hash(0, 0xcbf2_9ce4_8422_2325);
        summary.prefix_rejections = 0;
        result
    }

    #[test]
    fn history_result_text_is_pinned() {
        let result = history_result();
        assert_eq!(
            result.canonical_text(),
            format!(
                "mie-result 1\n\
                 experiment 3f902847001e1998\n\
                 pipeline research.replay_summary@2\n\
                 outcome history-replay-summary\n\
                 origin 1759276800000\n\
                 effective-origin 1759536000007\n\
                 cutover 1791244800000\n\
                 prefix-data {}\n\
                 prefix-stream 1200:1111222233334444\n\
                 prefix-rejections 3\n\
                 engine-features 0fedcba987654321\n\
                 state-encoding 1\n\
                 state-start 5555666677778888\n\
                 stream 48:0123456789abcdef\n\
                 rejections 2\n\
                 state-end 9999aaaabbbbcccc\n\
                 spec\n\
                 {}",
                "ab".repeat(32),
                golden()
            )
        );
        assert_eq!(
            result.key().to_string(),
            "3f902847001e1998/research.replay_summary@2-0fedcba987654321-1"
        );
        assert_eq!(result.key().engine, Some(ENGINE));

        let at_origin = history_result_at_origin();
        let text = at_origin.canonical_text();
        assert!(
            text.contains(
                "\norigin 1759276800000\n\
                 effective-origin -\n\
                 cutover 1791244800000\n\
                 prefix-data -\n\
                 prefix-stream 0:cbf29ce484222325\n\
                 prefix-rejections 0\n\
                 engine-features 0fedcba987654321\n"
            ),
            "{text}"
        );
        assert_eq!(at_origin.key(), result.key());
    }

    #[test]
    fn history_result_text_round_trips() {
        for result in [history_result(), history_result_at_origin()] {
            assert_eq!(parse(&result.canonical_text()), Ok(result));
        }
    }

    #[test]
    fn only_canonical_history_result_text_parses() {
        let text = history_result().canonical_text();
        let non_canonical = [
            text.replace("state-encoding 1\n", "state-encoding 01\n"),
            text.replace("state-encoding 1\n", "state-encoding +1\n"),
            text.replace("origin 1759276800000", "origin 01759276800000"),
            text.replace("prefix-rejections 3", "prefix-rejections +3"),
            text.replace("prefix-stream 1200:", "prefix-stream 01200:"),
        ];
        for text in non_canonical {
            assert_eq!(parse(&text), Err(ResultParseError::NotCanonical), "{text}");
        }
    }

    #[test]
    fn malformed_history_result_text_names_the_line() {
        let text = history_result().canonical_text();
        let cases = [
            (text.replace("origin 1759276800000\n", "origin x\n"), 5),
            (
                text.replace("effective-origin 1759536000007", "effective-origin"),
                6,
            ),
            (
                text.replace("cutover 1791244800000", "cut-over 1791244800000"),
                7,
            ),
            (
                text.replace(
                    &format!("prefix-data {}", "ab".repeat(32)),
                    "prefix-data AB",
                ),
                8,
            ),
            (
                text.replace("prefix-stream 1200:1111222233334444", "prefix-stream 1200"),
                9,
            ),
            (
                text.replace("prefix-rejections 3", "prefix-rejections -3"),
                10,
            ),
            (
                text.replace(
                    "engine-features 0fedcba987654321",
                    "engine-features 0FEDCBA987654321",
                ),
                11,
            ),
            (text.replace("state-encoding 1", "state-encoding 0"), 12),
            (
                text.replace("state-start 5555666677778888", "state-start 5555"),
                13,
            ),
            (text.replace("stream 48:0123456789abcdef", "stream 48"), 14),
            (text.replace("\nrejections 2", "\nrejections two"), 15),
            (
                text.replace("state-end 9999aaaabbbbcccc", "state-end -"),
                16,
            ),
            (text.replace("\nspec\n", "\nspecification\n"), 17),
        ];
        for (text, line) in cases {
            match parse(&text) {
                Err(ResultParseError::Malformed { line: found, .. }) => {
                    assert_eq!(found, line, "{text}");
                }
                other => panic!("{text}: {other:?}"),
            }
        }
        let truncated = text.split("state-start").next().unwrap();
        assert_eq!(
            parse(truncated),
            Err(ResultParseError::Malformed {
                line: 13,
                reason: "expected `state-start <16 hex>`, found the end of the text".to_owned()
            })
        );
    }

    #[test]
    fn the_pipeline_version_fixes_the_outcome_kind() {
        // No pipeline writes either text, and their keys would be
        // impossible: `@2` without an engine, `@1` with one.
        let v2_replay_summary = result().canonical_text().replace(
            "pipeline research.replay_summary@1",
            "pipeline research.replay_summary@2",
        );
        assert_eq!(
            parse(&v2_replay_summary),
            Err(ResultParseError::Malformed {
                line: 4,
                reason: "expected `outcome history-replay-summary`: the outcome of \
                         research.replay_summary@2"
                    .to_owned()
            })
        );
        let v1_history = history_result().canonical_text().replace(
            "pipeline research.replay_summary@2",
            "pipeline research.replay_summary@1",
        );
        assert_eq!(
            parse(&v1_history),
            Err(ResultParseError::Malformed {
                line: 4,
                reason: "expected `outcome replay-summary`: the outcome of \
                         research.replay_summary@1"
                    .to_owned()
            })
        );
        let v1_backtest = result().canonical_text().replace(
            "pipeline research.replay_summary@1",
            "pipeline research.backtest@1",
        );
        assert_eq!(
            parse(&v1_backtest),
            Err(ResultParseError::Malformed {
                line: 4,
                reason: "expected `outcome backtest`: the outcome of research.backtest@1"
                    .to_owned()
            })
        );
        // A pipeline version without a kind of its own is not checked.
        let other = result().canonical_text().replace(
            "pipeline research.replay_summary@1",
            "pipeline research.example@1",
        );
        assert_eq!(
            parse(&other).map(|result| result.pipeline),
            Ok(PipelineKey::new("research.example", 1))
        );
    }

    const CALIBRATION_V1: PipelineKey = PipelineKey::new("research.slippage_calibration", 1);

    fn calibration_row(fills: u64, book: u64, window: u64, sum: i64) -> CalibrationRow {
        CalibrationRow {
            fills,
            book,
            window,
            sum: Rate::from_units(sum),
        }
    }

    /// A calibration of 3 trades (6 fills a line): one book-less trade, the
    /// spread priced at every other fill, the depth walk running out of the
    /// window from 10 BTC on and pricing nothing from 100 BTC on.
    fn calibration_result() -> ExperimentResult {
        let mut depth_walk = [calibration_row(4, 2, 0, 1_000); 17];
        depth_walk[9] = calibration_row(4, 2, 0, 3_201);
        depth_walk[12] = calibration_row(3, 2, 1, 9_999);
        depth_walk[15] = calibration_row(0, 2, 4, 0);
        depth_walk[16] = calibration_row(0, 2, 4, 0);
        ExperimentResult {
            spec: ExperimentSpec::parse(&golden(), &catalog::registry()).unwrap(),
            pipeline: CALIBRATION_V1,
            outcome: Outcome::SlippageCalibration {
                stream: stream_hash(48, 0x0123_4567_89ab_cdef),
                domain_rejections: 2,
                calibration: Box::new(SlippageCalibration {
                    trades: 3,
                    spread: calibration_row(4, 2, 0, 1_000),
                    depth_walk,
                }),
            },
        }
    }

    #[test]
    fn calibration_result_text_is_pinned() {
        let result = calibration_result();
        let small = "fills=4 book=2 window=0 sum=0.00001000 mean=0.00000250";
        let mut expected = format!(
            "mie-result 1\n\
             experiment 3f902847001e1998\n\
             pipeline research.slippage_calibration@1\n\
             outcome slippage-calibration\n\
             stream 48:0123456789abcdef\n\
             rejections 2\n\
             trades 3\n\
             spread {small}\n"
        );
        let sizes = [
            "0.00100000",
            "0.00200000",
            "0.00500000",
            "0.01000000",
            "0.02000000",
            "0.05000000",
            "0.10000000",
            "0.20000000",
            "0.50000000",
            "1.00000000",
            "2.00000000",
            "5.00000000",
            "10.00000000",
            "20.00000000",
            "50.00000000",
            "100.00000000",
            "120.00000000",
        ];
        for (index, size) in sizes.iter().enumerate() {
            let fields = match index {
                // 3 201 / 4 = 800.25 → 800.
                9 => "fills=4 book=2 window=0 sum=0.00003201 mean=0.00000800",
                // 9 999 / 3 = 3 333.
                12 => "fills=3 book=2 window=1 sum=0.00009999 mean=0.00003333",
                15 | 16 => "fills=0 book=2 window=4 sum=0.00000000 mean=-",
                _ => small,
            };
            expected.push_str(&format!("depth_walk {size} {fields}\n"));
        }
        expected.push_str("spec\n");
        expected.push_str(&golden());
        assert_eq!(result.canonical_text(), expected);
        assert_eq!(
            result.key().to_string(),
            "3f902847001e1998/research.slippage_calibration@1"
        );
        assert_eq!(result.key().engine, None);
    }

    #[test]
    fn calibration_result_text_round_trips() {
        let result = calibration_result();
        assert_eq!(result.validate(), Ok(()));
        assert_eq!(parse(&result.canonical_text()), Ok(result));
    }

    #[test]
    fn only_canonical_calibration_result_text_parses() {
        let text = calibration_result().canonical_text();
        let non_canonical = [
            // A mean that is not ⟨sum / fills⟩.
            text.replace("mean=0.00000800", "mean=0.00000801"),
            text.replace("mean=0.00003333", "mean=-"),
            text.replace("sum=0.00000000 mean=-", "sum=0.00000000 mean=0.00000000"),
            text.replace("trades 3", "trades 03"),
            text.replace("spread fills=4", "spread fills=+4"),
            text.replace("sum=0.00003201", "sum=0.000032010"),
            // The grid's size, not in its 8-place form.
            text.replace("depth_walk 1.00000000", "depth_walk 1"),
        ];
        for text in non_canonical {
            assert_eq!(parse(&text), Err(ResultParseError::NotCanonical), "{text}");
        }
    }

    #[test]
    fn malformed_calibration_result_text_names_the_line() {
        let text = calibration_result().canonical_text();
        let cases = [
            (text.replace("stream 48:0123456789abcdef", "stream 48"), 5),
            (text.replace("rejections 2", "rejections -2"), 6),
            (text.replace("trades 3", "trades three"), 7),
            (text.replace("spread fills", "spread  fills"), 8),
            (text.replace("spread fills=4", "spread fill=4"), 8),
            (
                text.replace("mean=0.00000250\ndepth_walk 0.001", "\ndepth_walk 0.001"),
                8,
            ),
            (
                text.replace("depth_walk 0.00100000", "depth_walk 0.00100001"),
                9,
            ),
            // The grid's order is fixed.
            (
                text.replace("depth_walk 0.00200000", "depth_walk 0.00500000"),
                10,
            ),
            (
                text.replace("depth_walk 1.00000000", "depth_walk 2.00000000"),
                18,
            ),
            (
                text.replace("120.00000000 fills=0", "120.00000000 fills=0 extra=1"),
                25,
            ),
            (text.replace("\nspec\n", "\nspecification\n"), 26),
        ];
        for (text, line) in cases {
            match parse(&text) {
                Err(ResultParseError::Malformed { line: found, .. }) => {
                    assert_eq!(found, line, "{text}");
                }
                other => panic!("{text}: {other:?}"),
            }
        }
    }

    /// A change to a calibration value.
    type Mutation = fn(&mut SlippageCalibration);

    /// ADR-061 D7's conservation and the sums of rates that are each ≥ 0,
    /// refused by `parse` and `validate` alike, on the line that breaks
    /// them.
    #[test]
    fn a_calibration_that_breaks_the_conservation_or_a_sum_is_malformed() {
        let conservation = "fills + book + window is not 2 × trades = 6: every trade gives a \
                            buy and a sell on every line (ADR-061 D7)";
        let cases: [(Mutation, usize, &str); 5] = [
            (
                |c| c.trades = 4,
                8,
                "fills + book + window is not 2 × trades = 8: every trade gives a buy and a \
                 sell on every line (ADR-061 D7)",
            ),
            (|c| c.depth_walk[3].book = 3, 12, conservation),
            (
                |c| c.depth_walk[16] = calibration_row(1, 2, 4, 0),
                25,
                conservation,
            ),
            (
                |c| c.spread.sum = Rate::from_units(-1),
                8,
                "the sum is negative: every priced fill costs at least its print (ADR-061 D2)",
            ),
            (
                |c| c.depth_walk[15].sum = Rate::from_units(1),
                24,
                "the sum is not 0 without a priced fill",
            ),
        ];
        for (mutate, line, reason) in cases {
            let mut result = calibration_result();
            let Outcome::SlippageCalibration { calibration, .. } = &mut result.outcome else {
                unreachable!();
            };
            mutate(calibration);
            let validated = result.validate();
            let parsed = parse(&result.canonical_text());
            assert_eq!(validated, parsed.map(|_| ()), "line {line}");
            match validated {
                Err(ResultParseError::Malformed {
                    line: found,
                    reason: text,
                }) => {
                    assert_eq!((found, text.as_str()), (line, reason));
                }
                other => panic!("line {line}: {other:?}"),
            }
        }
    }

    #[test]
    fn the_calibration_pipeline_fixes_its_outcome_kind() {
        let summary_text = result().canonical_text().replace(
            "pipeline research.replay_summary@1",
            "pipeline research.slippage_calibration@1",
        );
        assert_eq!(
            parse(&summary_text),
            Err(ResultParseError::Malformed {
                line: 4,
                reason: "expected `outcome slippage-calibration`: the outcome of \
                         research.slippage_calibration@1"
                    .to_owned()
            })
        );
        let mut as_summary = calibration_result();
        as_summary.pipeline = PIPELINE;
        assert_eq!(
            as_summary.validate(),
            Err(ResultParseError::Malformed {
                line: 4,
                reason: "expected `outcome replay-summary`: the outcome of \
                         research.replay_summary@1"
                    .to_owned()
            })
        );
        assert_eq!(
            parse(&as_summary.canonical_text()),
            Err(as_summary.validate().unwrap_err())
        );
    }

    #[test]
    fn result_keys_round_trip_through_their_names() {
        let experiment = ExperimentId::from_hex("3f902847001e1998").unwrap();
        let v1 = ResultKey {
            experiment,
            pipeline: PIPELINE,
            engine: None,
        };
        let v2 = ResultKey {
            experiment,
            pipeline: PIPELINE_V2,
            engine: Some(ENGINE),
        };
        assert_eq!(v1.name(), "research.replay_summary@1");
        assert_eq!(v2.name(), "research.replay_summary@2-0fedcba987654321-1");
        for key in [&v1, &v2] {
            assert_eq!(
                ResultKey::from_name(experiment, &key.name()).as_ref(),
                Some(key)
            );
        }
        let encoded = ResultKey {
            engine: Some(EngineKey {
                state_encoding: 12,
                ..ENGINE
            }),
            ..v2.clone()
        };
        assert_eq!(
            ResultKey::from_name(experiment, "research.replay_summary@2-0fedcba987654321-12"),
            Some(encoded)
        );
        for bad in [
            "research.replay_summary@2-0fedcba987654321",
            "research.replay_summary@2-0fedcba98765432-1",
            "research.replay_summary@2-0FEDCBA987654321-1",
            "research.replay_summary@2-0fedcba987654321-0",
            "research.replay_summary@2-0fedcba987654321-01",
            "research.replay_summary@2-0fedcba987654321-+1",
            "research.replay_summary@2-0fedcba987654321-",
            "research.replay_summary@2-0fedcba987654321-1-",
            "research.replay_summary@2-0fedcba987654321-1-2",
            "research.replay_summary@2-",
            "research.replay_summary@02",
            "",
        ] {
            assert_eq!(ResultKey::from_name(experiment, bad), None, "{bad}");
        }
        assert!(
            v1 < ResultKey {
                engine: Some(ENGINE),
                ..v1.clone()
            }
        );
    }

    #[test]
    fn pipeline_keys_follow_the_id_grammar() {
        assert_eq!(
            PipelineKey::parse("research.replay_summary@1"),
            Some(PIPELINE)
        );
        assert_eq!(PIPELINE.id(), "research.replay_summary");
        assert_eq!(PIPELINE.version(), 1);
        for bad in [
            "research",
            "research@0",
            "research@01",
            "Research@1",
            "a@1@2",
        ] {
            assert_eq!(PipelineKey::parse(bad), None, "{bad}");
        }
        assert!(PipelineKey::new("a", 1) < PipelineKey::new("a", 2));
    }
}
