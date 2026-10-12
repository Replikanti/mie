//! Experiment results and result text v1 (ADR-040; grammar in the
//! [`research`](super) module docs).

use super::{DataVersion, ExperimentId, ExperimentSpec, SpecError, parse_hex16};
use crate::event_hash::EventStreamHash;
use crate::feature::{FeatureRegistry, FeatureSetVersion, is_valid_id, parse_versioned_id};
use crate::fingerprint::Fingerprint;
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
}

impl Outcome {
    /// The tag on the `outcome` line.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ReplaySummary { .. } => "replay-summary",
            Self::HistoryReplaySummary(_) => "history-replay-summary",
        }
    }

    /// The engine whose Market State the outcome reads; `None` for an
    /// outcome that reads none.
    pub const fn engine(&self) -> Option<EngineKey> {
        match self {
            Self::ReplaySummary { .. } => None,
            Self::HistoryReplaySummary(summary) => Some(summary.engine),
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
            Outcome::HistoryReplaySummary(summary) => {
                let HistoryReplaySummary {
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
                } = summary;
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
            other => return Err(malformed(4, format!("unknown outcome line {other:?}"))),
        };
        if let Some(kind) = kind_of(&pipeline).filter(|kind| *kind != outcome.kind()) {
            return Err(malformed(
                4,
                format!("expected `outcome {kind}`: the outcome of {pipeline}"),
            ));
        }
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
        if result.canonical_text() != text {
            return Err(ResultParseError::NotCanonical);
        }
        Ok(result)
    }
}

/// The outcome kind of each pipeline version that has one (ADR-040,
/// ADR-052 D10). A result pairing one of them with another kind is no
/// pipeline's output, and its key would be impossible: `@2` without an
/// engine, `@1` with one.
const PIPELINE_KINDS: [(&str, u32, &str); 2] = [
    ("research.replay_summary", 1, "replay-summary"),
    ("research.replay_summary", 2, "history-replay-summary"),
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
                text.replace("outcome replay-summary", "outcome backtest"),
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
        // A pipeline version without a kind of its own is not checked.
        let other = result().canonical_text().replace(
            "pipeline research.replay_summary@1",
            "pipeline research.backtest@1",
        );
        assert_eq!(
            parse(&other).map(|result| result.pipeline),
            Ok(PipelineKey::new("research.backtest", 1))
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
