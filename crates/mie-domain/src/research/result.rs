//! Experiment results and result text v1 (ADR-040; grammar in the
//! [`research`](super) module docs).

use super::{ExperimentId, ExperimentSpec, SpecError, parse_hex16};
use crate::event_hash::EventStreamHash;
use crate::feature::{FeatureRegistry, is_valid_id, parse_versioned_id};
use crate::fingerprint::Fingerprint;
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

/// Where a result is stored: one per experiment and pipeline version.
/// Displays as `<experiment hex>/<pipeline id>@<N>`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResultKey {
    /// The experiment.
    pub experiment: ExperimentId,
    /// The pipeline.
    pub pipeline: PipelineKey,
}

impl fmt::Display for ResultKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.experiment, self.pipeline)
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
}

impl Outcome {
    /// The tag on the `outcome` line.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ReplaySummary { .. } => "replay-summary",
        }
    }
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
    /// The storage key: the spec's id and the pipeline.
    pub fn key(&self) -> ResultKey {
        ResultKey {
            experiment: self.spec.id(),
            pipeline: self.pipeline.clone(),
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
    /// [`ResultParseError`]: a malformed line, an invalid embedded spec, an
    /// `experiment` line that differs from the embedded spec's id, or text
    /// that is valid but not canonical.
    pub fn parse(text: &str, registry: &FeatureRegistry) -> Result<Self, ResultParseError> {
        let mut lines = Lines {
            rest: text,
            line: 0,
        };
        let mut next = |expected: &str| lines.next(expected);
        let malformed = |line: usize, reason: String| ResultParseError::Malformed { line, reason };

        let header = next(HEADER)?;
        if header != HEADER {
            return Err(malformed(1, format!("expected the header {HEADER:?}")));
        }
        let declared = next("experiment <16 hex>")?
            .strip_prefix("experiment ")
            .and_then(ExperimentId::from_hex)
            .ok_or_else(|| malformed(2, "expected `experiment <16 hex>`".to_owned()))?;
        let pipeline = next("pipeline <id@N>")?
            .strip_prefix("pipeline ")
            .and_then(PipelineKey::parse)
            .ok_or_else(|| malformed(3, "expected `pipeline <id@N>`".to_owned()))?;
        let outcome = match next("outcome <kind>")? {
            "outcome replay-summary" => {
                let stream = next("stream <events>:<16 hex>")?
                    .strip_prefix("stream ")
                    .and_then(parse_stream_hash)
                    .ok_or_else(|| {
                        malformed(5, "expected `stream <events>:<16 hex>`".to_owned())
                    })?;
                let domain_rejections = next("rejections <n>")?
                    .strip_prefix("rejections ")
                    .and_then(|n| n.parse().ok())
                    .ok_or_else(|| malformed(6, "expected `rejections <n>`".to_owned()))?;
                Outcome::ReplaySummary {
                    stream,
                    domain_rejections,
                }
            }
            other => return Err(malformed(4, format!("unknown outcome line {other:?}"))),
        };
        if next("spec")? != "spec" {
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
