//! The slippage calibration pipeline, `research.slippage_calibration@1`
//! (ADR-061 D7, D8, proposed): what the book models cost at every trade of
//! a live sample, against its print.
//!
//! [`CalibrationService`] replays the spec's sample, as
//! `research.replay_summary@1` does, through the live rejection policy
//! ([`drive_tolerant_observed`]), and after every accepted trade observes
//! two hypothetical fills, a buy and a sell at the print, on the order book
//! the engine's state holds at that trade (D1): a trade never changes the
//! book, and it ranks before the book updates of its millisecond. Each fill
//! is priced by the spread model and the depth walk at every grid size,
//! and counts as priced only once the next book event proves that book
//! current ([`Calibrator`]): fills between a depth reconnect or a run
//! restart and the resync count unpriced `book`.
//!
//! The result keeps the provenance of `@1` (the stream hash and the
//! rejections) and, like `@1`, has no engine in its key: the values depend
//! only on the data and on `book.l2@1`, which the spec's feature set must
//! contain ([`ResearchError::BookNotInSet`]). The book exists only in live
//! captures (ADR-043 D11), and each value depends only on the book since
//! the run's last snapshot and on the print, so the composition root runs
//! this pipeline over a live replay only. The pipeline reads no rule line
//! of the spec.

use crate::research::{checked_engine, record};
use crate::{HashingProvider, drive_tolerant_observed};
use mie_domain::feature::catalog;
use mie_domain::research::{DataVersion, ExperimentResult, ExperimentSpec, Outcome, PipelineKey};
use mie_domain::slippage::Calibrator;
use mie_domain::time::EventTime;
use mie_ports::inbound::{ExperimentRun, ResearchError, RunResearchExperiment};
use mie_ports::outbound::{HistoricalDataProvider, Replay, ReplayWindow, ResearchResultStore};
use std::cell::Cell;

/// `research.slippage_calibration@1`: the book models' cost at every trade
/// of the sample (ADR-061 D7, module docs).
pub const SLIPPAGE_CALIBRATION_V1: PipelineKey =
    PipelineKey::new("research.slippage_calibration", 1);

/// [`RunResearchExperiment`] for [`SLIPPAGE_CALIBRATION_V1`] over a
/// historical data source, a live replay, and a result store.
#[derive(Debug)]
pub struct CalibrationService<H, S> {
    history: H,
    store: S,
}

impl<H: HistoricalDataProvider, S: ResearchResultStore> CalibrationService<H, S> {
    /// Creates the service over `history`, a replay of live captures. It is
    /// the only holder of the writable store.
    pub fn new(history: H, store: S) -> Self {
        Self { history, store }
    }

    /// The result store, read-only.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Unwraps the data source and the store.
    pub fn into_parts(self) -> (H, S) {
        (self.history, self.store)
    }
}

impl<H: HistoricalDataProvider, S: ResearchResultStore> RunResearchExperiment
    for CalibrationService<H, S>
{
    /// Runs [`SLIPPAGE_CALIBRATION_V1`] (ADR-061 D7). The first two checks
    /// are pure, so a spec that fails one opens nothing:
    ///
    /// 1. the feature set, as the replay summary checks it (ADR-040 D4);
    /// 2. the set contains `book.l2@1`, else
    ///    [`ResearchError::BookNotInSet`];
    /// 3. the replay of the sample must open exactly the spec's data
    ///    version (ADR-040 D5);
    /// 4. the sample is driven with the live rejection policy, every
    ///    accepted event observed by a [`Calibrator`] on the book of the
    ///    engine's state, and must deliver at least one event; a
    ///    calibration that leaves its integer range fails
    ///    [`ResearchError::CalibrationOverflow`];
    /// 5. the result is recorded as the replay summary records it:
    ///    `Recorded`, `Reproduced` or `Diverged` (ADR-040 D7).
    fn run(&mut self, spec: &ExperimentSpec) -> Result<ExperimentRun, ResearchError> {
        let mut engine = checked_engine(spec)?;
        let book = catalog::BOOK_L2_V1.key;
        if !spec.features().definitions().any(|d| d.key == book) {
            return Err(ResearchError::BookNotInSet);
        }

        let window = ReplayWindow {
            start: spec.sample().start(),
            end: spec.sample().end(),
        };
        let Replay { stream, dataset } = self.history.replay(window)?;
        let opened = DataVersion::from(&dataset);
        if &opened != spec.data_version() {
            return Err(ResearchError::DataVersionMismatch {
                declared: spec.data_version().clone(),
                opened,
            });
        }

        let mut stream = HashingProvider::new(stream);
        let mut domain_rejections = 0;
        // The rejection callback runs before the observer of the same
        // event; it only sets the flag, so the observer skips that event.
        let rejected = Cell::new(false);
        let mut calibrator = Calibrator::new();
        let mut overflow: Option<EventTime> = None;
        drive_tolerant_observed(
            &mut stream,
            &mut engine,
            |_| {
                domain_rejections += 1;
                rejected.set(true);
            },
            |event, engine| {
                if rejected.replace(false) || overflow.is_some() {
                    return;
                }
                if calibrator.observe(event, &engine.state().book.l2).is_err() {
                    overflow = Some(event.time());
                }
            },
        )?;
        if let Some(at) = overflow {
            return Err(ResearchError::CalibrationOverflow { at });
        }
        let hash = stream.hash();
        let calibration = calibrator
            .finish()
            .map_err(|_| ResearchError::CalibrationOverflow {
                at: spec.sample().end(),
            })?;
        if hash.events == 0 {
            return Err(ResearchError::EmptySample);
        }

        let result = ExperimentResult {
            spec: spec.clone(),
            pipeline: SLIPPAGE_CALIBRATION_V1,
            outcome: Outcome::SlippageCalibration {
                stream: hash,
                domain_rejections,
                calibration: Box::new(calibration),
            },
        };
        record(&mut self.store, result)
    }
}
