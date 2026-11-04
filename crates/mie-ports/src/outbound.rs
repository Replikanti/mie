//! Driven (outbound) ports.
//!
//! Present: live and historical market data, the research result store
//! ([`ResearchResultReader`], [`ResearchResultStore`], ADR-040), the
//! [`BacktestEngine`] that simulates a resolved plan (ADR-060 D14), and the
//! informational egress [`AlertGateway`] (ADR-059, proposed). Planned, each
//! landing with the issue that first needs it: `FeatureStore`,
//! `StrategyRepository`. There is deliberately no `ExecutionGateway`: the
//! MVP is informational (ADR-010), and the backtest is a simulation.

use crate::inbound::ResearchError;
use crate::raw::DatasetVersion;
use mie_domain::alert::Alert;
use mie_domain::backtest::{Plan, SimulationStats, TradeRecord};
use mie_domain::event::MarketEvent;
use mie_domain::order::CanonicalKey;
use mie_domain::research::{
    ExperimentId, ExperimentResult, ExperimentSpec, HistoryReplaySummary, HypothesisId, ResultKey,
    ResultParseError,
};
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use std::fmt;

/// A source of domain-level market observations (brief §5, Data Plane brief).
///
/// Contract:
/// - events arrive strictly increasing in the canonical order (ADR-028,
///   `Ord for MarketEvent`), which is built from exchange fields only.
///   Providers sort or merge with the full comparator, not with
///   [`MarketEvent::canonical_key`] alone;
/// - providers dedupe by exchange id: a `Trade`, `BookSnapshot` or
///   `BookUpdate` whose id was already delivered is dropped even when its
///   payload differs, an id-less event only when it is an exact repeat. Ids
///   never fall: trade ids strictly increase, and so do snapshot and update
///   ids each among their own kind (the engine rejects violations, ADR-028);
/// - continuity loss is delivered as a [`MarketEvent::FeedGap`], never as
///   silence. A live event that arrives after its slot was released becomes
///   a `LateEvent` gap and is never delivered into the past. The gap's `end`
///   is a later millisecond than the last released event's ordering time, so
///   the gap sorts strictly after it (ADR-028);
/// - only domain types cross the port — never exchange SDK or wire types;
/// - `Ok(None)` means the stream has ended (replay exhausted, feed closed).
pub trait MarketDataProvider {
    /// Pulls the next event, blocking until one is available.
    ///
    /// # Errors
    ///
    /// [`ProviderError`] when the source fails or breaks this contract.
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError>;
}

/// Replays recorded market data through the same [`MarketDataProvider`]
/// contract as live data (ADR-019).
///
/// A replay exposes only the information that was available at each decision
/// moment and delivers it in the canonical order (ADR-028), so it matches
/// live processing of the same events. Recorded arrival order is capture
/// metadata, not the replay order.
///
/// Contract (ADR-039):
/// - every replay names the exact raw data it reads: [`Replay::dataset`] is
///   the [`DatasetVersion`] of the selection behind the stream (Data Plane
///   rule 6), so no replay can omit its provenance;
/// - a recompute of live capture runs each capture run's pipeline from the
///   run's first record, with the run's journaled parameters, and restricts
///   the output to the window afterwards. Starting mid-run would change what
///   the hold-back released, so a window inside a run delivers exactly the
///   full run's events that fall into it;
/// - an event is delivered when its ordering time (`MarketEvent::time`, a
///   gap's `end`) lies in the window. A gap that is still open at the
///   window's end (its `end` at or after `window.end`) is not delivered:
///   live announced it only when the stream resumed, which is after the
///   window. Providers report such trailing gaps out of band.
pub trait HistoricalDataProvider {
    /// The event stream a replay yields.
    type Stream: MarketDataProvider;

    /// Opens a replay of `window`.
    ///
    /// # Errors
    ///
    /// [`ProviderError`] when the requested data cannot be opened.
    fn replay(&self, window: ReplayWindow) -> Result<Replay<Self::Stream>, ProviderError>;
}

/// An opened replay: the event stream and the version of the raw data
/// behind it (ADR-039 D8).
#[derive(Debug)]
pub struct Replay<S> {
    /// The events, in the canonical order.
    pub stream: S,
    /// Identifies the raw data the stream reads (ADR-030).
    pub dataset: DatasetVersion,
}

/// A half-open event-time window `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayWindow {
    /// First included event time.
    pub start: EventTime,
    /// First excluded event time.
    pub end: EventTime,
}

impl ReplayWindow {
    /// Whether `time` falls inside the window.
    pub fn contains(&self, time: EventTime) -> bool {
        self.start <= time && time < self.end
    }
}

/// Why a market-data provider could not deliver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderError {
    /// The underlying source failed (connection, file, decoding).
    Source(String),
    /// The source produced data that breaks the port contract.
    Contract(String),
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(detail) => write!(f, "market data source failed: {detail}"),
            Self::Contract(detail) => {
                write!(f, "market data source broke the port contract: {detail}")
            }
        }
    }
}

impl std::error::Error for ProviderError {}

/// Reads stored experiment results (ADR-040). Agents and reports get this
/// trait only; writing is the pipeline's (ADR-020).
pub trait ResearchResultReader {
    /// The result stored under `key`, verified, if any.
    ///
    /// # Errors
    ///
    /// [`ResultStoreError`] when the stored result fails verification or the
    /// store cannot be read.
    fn get(&self, key: &ResultKey) -> Result<Option<ExperimentResult>, ResultStoreError>;

    /// The keys of every stored result whose spec tests `hypothesis`,
    /// sorted.
    ///
    /// # Errors
    ///
    /// As [`Self::get`].
    fn by_hypothesis(&self, hypothesis: &HypothesisId) -> Result<Vec<ResultKey>, ResultStoreError>;
}

/// The append-only store of experiment results (ADR-040), written only by
/// the deterministic research pipeline (ADR-020).
///
/// Contract: a stored result is never replaced and never deleted. A second
/// append under a stored key fails with [`ResultStoreError::AlreadyStored`]
/// and leaves the stored result untouched; so does an append whose spec
/// differs from a stored spec with the same experiment id
/// ([`ResultStoreError::Collision`]). A result that fails
/// [`ExperimentResult::validate`] is refused before anything is written
/// ([`ResultStoreError::Invalid`]): its text would never read back, and
/// every later read of its experiment would fail. `Ok` means the result is
/// stored and durable: it survives a crash or power loss.
pub trait ResearchResultStore: ResearchResultReader {
    /// Stores `result` under [`ExperimentResult::key`] and returns the key.
    ///
    /// # Errors
    ///
    /// [`ResultStoreError`]. [`ResultStoreError::NotDurable`] means the
    /// result is stored and readable but may not survive a crash; on every
    /// other error nothing stored is changed.
    fn append(&mut self, result: &ExperimentResult) -> Result<ResultKey, ResultStoreError>;
}

/// Why the result store refused or failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultStoreError {
    /// A result is already stored under the key; it stays as it is.
    AlreadyStored {
        /// The key.
        key: ResultKey,
        /// Whether the stored result equals the one offered.
        identical: bool,
    },
    /// A different spec with the same experiment id is stored: a 64-bit
    /// fingerprint collision. Nothing is written.
    Collision {
        /// The experiment id both specs share.
        experiment: ExperimentId,
    },
    /// A stored result fails its integrity check.
    Integrity(String),
    /// A stored result is intact but not where its key says it belongs, or
    /// does not parse.
    Corrupt(String),
    /// The result was stored and is readable, but the store could not make
    /// it durable (for example, a directory sync failed), so it may not
    /// survive a crash. It is not rewritten; a later run of the key finds it
    /// stored or, after a crash, appends it again.
    NotDurable {
        /// The key.
        key: ResultKey,
        /// What failed.
        detail: String,
    },
    /// The offered result fails [`ExperimentResult::validate`]: no
    /// pipeline produces it, and its text would not parse back. Nothing is
    /// written.
    Invalid(ResultParseError),
    /// The store cannot be read or written.
    Io(String),
}

impl fmt::Display for ResultStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyStored {
                key,
                identical: true,
            } => write!(f, "result {key} is already stored, identical"),
            Self::AlreadyStored {
                key,
                identical: false,
            } => write!(
                f,
                "result {key} is already stored with a different outcome; stored results are \
                 never overwritten"
            ),
            Self::Collision { experiment } => write!(
                f,
                "experiment id {experiment} is already taken by a different spec (fingerprint \
                 collision)"
            ),
            Self::Integrity(detail) => write!(f, "stored result failed verification: {detail}"),
            Self::Corrupt(detail) => write!(f, "stored result is corrupt: {detail}"),
            Self::NotDurable { key, detail } => write!(
                f,
                "result {key} is stored, but it may not survive a crash: {detail}"
            ),
            Self::Invalid(error) => write!(f, "result refused before storing: {error}"),
            Self::Io(detail) => write!(f, "result store I/O failed: {detail}"),
        }
    }
}

impl std::error::Error for ResultStoreError {}

/// Simulates a resolved plan over a spec's sample (ADR-060 D14): the
/// domain's run output before metrics, which the research pipeline renders,
/// judges by the same metric code and stores. Every engine takes a resolved
/// plan, so no engine reinterprets spec text.
///
/// Contract:
/// - `plan` is [`resolve`](mie_domain::backtest::resolve)'s for `spec`, and
///   `engine` is built and checked for the spec's feature set (ADR-040 D4)
///   by the caller, which also checked the funding coverage (ADR-060 D7);
/// - a sample that starts before the source's origin is refused, and the
///   sample is opened and its data version checked against the spec's
///   (ADR-040 D5) before anything is driven;
/// - the prefix `[origin, sample start)` drives `engine` without being
///   evaluated, then the sample does (ADR-052 D10); the simulator is fed
///   per its driver contract (`Simulator`), and the run ends with the
///   trailing gaps and `finish` (ADR-060 D9);
/// - errors are only [`ResearchError::Provider`],
///   [`ResearchError::DataVersionMismatch`],
///   [`ResearchError::FeatureUnavailable`], [`ResearchError::EmptySample`]
///   and [`ResearchError::SampleBeforeOrigin`]. Nothing is stored: the
///   store is the pipeline's (ADR-020).
///
/// The output is a simulation only: no order, fill report or execution type
/// crosses the port (ADR-010).
pub trait BacktestEngine {
    /// Runs `plan` over `spec`'s sample, driving `engine`.
    ///
    /// # Errors
    ///
    /// The [`ResearchError`] variants of the contract.
    fn run(
        &mut self,
        spec: &ExperimentSpec,
        plan: &Plan,
        engine: MarketStateEngine,
    ) -> Result<BacktestRun, ResearchError>;
}

/// What a [`BacktestEngine`] run produced (ADR-060 D12, D14): the lines of
/// the `backtest` outcome before its metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestRun {
    /// The chain provenance, the lines of `research.replay_summary@2`
    /// (ADR-052 D10); its engine is the result key's (ADR-060 D13).
    pub summary: HistoryReplaySummary,
    /// The counts (ADR-060 D4, D9).
    pub stats: SimulationStats,
    /// One record per taken decision, in decision order.
    pub records: Vec<TradeRecord>,
}

/// The informational egress (brief §17, ADR-010, ADR-059 D3): delivers one
/// rendered message per engine event that produced at least one alert.
///
/// Contract:
/// - best effort, and `deliver` never blocks the caller on I/O: the core
///   thread calls it, and a stalled core thread blocks the capture through
///   the bounded core channel (ADR-026). An adapter queues and sends on
///   its own thread;
/// - a delivery error is counted by the caller and never stops the drive;
/// - the decision log is the record of what was decided; delivery is not.
///
/// There is no path from a message to an order (ADR-010).
pub trait AlertGateway {
    /// Hands `message` over for delivery.
    ///
    /// # Errors
    ///
    /// [`AlertError`] when the message is not taken; the caller counts it
    /// and goes on.
    fn deliver(&mut self, message: &AlertMessage) -> Result<(), AlertError>;
}

/// One message: the alerts of one engine event, bundled, and their
/// rendered text (ADR-059 D1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertMessage {
    /// The event's time, the decision time of every alert in it.
    pub known_at: EventTime,
    /// The event's canonical key (ADR-028).
    pub event: CanonicalKey,
    /// The alerts, in bundling order.
    pub alerts: Vec<Alert>,
    /// The plain text a person reads (ADR-059 D4).
    pub text: String,
}

/// Why a gateway did not take a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertError {
    /// The gateway cannot deliver now (closed, disabled, full).
    Unavailable(String),
    /// The message can never be delivered (for example, too long).
    Rejected(String),
}

impl fmt::Display for AlertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(detail) => write!(f, "alert gateway unavailable: {detail}"),
            Self::Rejected(detail) => write!(f, "alert message rejected: {detail}"),
        }
    }
}

impl std::error::Error for AlertError {}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::fingerprint::Fingerprint;
    use mie_domain::research::PipelineKey;

    #[test]
    fn result_store_errors_describe_themselves() {
        let experiment = ExperimentId::from_fingerprint(Fingerprint::from_raw(0xab));
        let key = ResultKey {
            experiment,
            pipeline: PipelineKey::new("research.replay_summary", 1),
            engine: None,
        };
        let cases = [
            (
                ResultStoreError::AlreadyStored {
                    key: key.clone(),
                    identical: true,
                },
                "result 00000000000000ab/research.replay_summary@1 is already stored, identical",
            ),
            (
                ResultStoreError::AlreadyStored {
                    key: key.clone(),
                    identical: false,
                },
                "result 00000000000000ab/research.replay_summary@1 is already stored with a \
                 different outcome; stored results are never overwritten",
            ),
            (
                ResultStoreError::Collision { experiment },
                "experiment id 00000000000000ab is already taken by a different spec \
                 (fingerprint collision)",
            ),
            (
                ResultStoreError::Integrity("sha256 mismatch".to_owned()),
                "stored result failed verification: sha256 mismatch",
            ),
            (
                ResultStoreError::Corrupt("misplaced".to_owned()),
                "stored result is corrupt: misplaced",
            ),
            (
                ResultStoreError::NotDurable {
                    key,
                    detail: "fsync failed".to_owned(),
                },
                "result 00000000000000ab/research.replay_summary@1 is stored, but it may not \
                 survive a crash: fsync failed",
            ),
            (
                ResultStoreError::Io("disk full".to_owned()),
                "result store I/O failed: disk full",
            ),
        ];
        for (error, text) in cases {
            assert_eq!(error.to_string(), text);
        }
    }

    #[test]
    fn alert_errors_describe_themselves() {
        assert_eq!(
            AlertError::Unavailable("closed".to_owned()).to_string(),
            "alert gateway unavailable: closed"
        );
        assert_eq!(
            AlertError::Rejected("too long".to_owned()).to_string(),
            "alert message rejected: too long"
        );
    }

    #[test]
    fn replay_window_is_half_open() {
        let window = ReplayWindow {
            start: EventTime::from_millis(1_000),
            end: EventTime::from_millis(2_000),
        };
        assert!(!window.contains(EventTime::from_millis(999)));
        assert!(window.contains(EventTime::from_millis(1_000)));
        assert!(window.contains(EventTime::from_millis(1_999)));
        assert!(!window.contains(EventTime::from_millis(2_000)));
    }
}
