//! The backtest engine on the history chain (ADR-060 D13, D14, #170).
//!
//! [`ChainBacktest`] implements the `BacktestEngine` port: it drives the
//! spec's sample on the history chain through the chain drive of
//! `research.replay_summary@2`, so its provenance lines are `@2`'s, and
//! feeds the domain [`Simulator`] per its driver contract:
//!
//! - every accepted event is observed, the prefix included, so the
//!   staleness of the reference follows the engine (ADR-052 D10), with the
//!   plan's order book in the engine's state after the event, the book a
//!   fill trade met (ADR-061 D1);
//! - in the sample only, each emission of the plan's trigger id goes
//!   through [`Plan::intents`] to [`Simulator::decide`]; prefix events are
//!   never evaluated (ADR-060 D4);
//! - a rejected event is reported instead of observed, and a chain restart
//!   before the restart event's own step (ADR-060 D9, ADR-052 D4);
//! - after the drive, the provider's trailing gaps of the trades stream,
//!   and under `funding.recorded@1` those of the funding stream, are
//!   checked, observed in canonical order with an unready book, since no
//!   trade is priced there, and the simulator finishes (ADR-060 D9).
//!
//! Simulation only (ADR-010): the records are the prices a resting order
//! would have met on the recorded trades, never an order. This file and
//! `research.rs` are the only files of this crate on the leakage guard's
//! allow-list (ADR-060 D15).

use crate::check_trailing_trades_gap;
use crate::research::{ChainBounds, ChainStep, drive_history};
use mie_domain::backtest::{FundingRule, Plan, Simulator};
use mie_domain::event::{FeedGap, MarketEvent, Stream};
use mie_domain::feature::FeatureValue;
use mie_domain::research::ExperimentSpec;
use mie_domain::state::{MarketStateEngine, StateError};
use mie_domain::time::EventTime;
use mie_ports::inbound::ResearchError;
use mie_ports::outbound::{BacktestEngine, BacktestRun, HistoricalDataProvider, ProviderError};

/// The `BacktestEngine` on the history chain (module docs): `history` is
/// the chain within `bounds`, and `trailing` reads a replay stream's
/// trailing gaps once it has ended. Trailing gaps are reported out of band,
/// so reading them is provider-specific (the `HistoricalDataProvider`
/// contract), as for the label driver.
#[derive(Debug)]
pub struct ChainBacktest<H, T> {
    history: H,
    bounds: ChainBounds,
    trailing: T,
}

impl<H, T> ChainBacktest<H, T>
where
    H: HistoricalDataProvider,
    T: Fn(&H::Stream) -> Vec<FeedGap>,
{
    /// The engine over `history`, the history chain within `bounds`, with
    /// `trailing` reading a stream's trailing gaps.
    pub fn new(history: H, bounds: ChainBounds, trailing: T) -> Self {
        Self {
            history,
            bounds,
            trailing,
        }
    }

    /// Unwraps the data source.
    pub fn into_inner(self) -> H {
        self.history
    }
}

impl<H, T> BacktestEngine for ChainBacktest<H, T>
where
    H: HistoricalDataProvider,
    T: Fn(&H::Stream) -> Vec<FeedGap>,
{
    fn run(
        &mut self,
        spec: &ExperimentSpec,
        plan: &Plan,
        engine: MarketStateEngine,
    ) -> Result<BacktestRun, ResearchError> {
        let mut simulator = Simulator::new(*plan.costs());
        // The latest accepted trades-stream time: the trailing trades
        // gaps are checked against it.
        let mut known: Option<EventTime> = None;
        let trigger = plan.trigger();
        let (summary, stream) =
            drive_history(
                &self.history,
                spec,
                engine,
                self.bounds,
                |step| match step {
                    ChainStep::Restart => simulator.restart(),
                    ChainStep::Rejected { event } => simulator.reject(event),
                    ChainStep::Accepted {
                        event,
                        engine,
                        sample,
                    } => {
                        simulator.observe(event, plan.book(engine.state()));
                        if event.stream() == Stream::Trades {
                            known = known.max(Some(event.time()));
                        }
                        // `intents` filters by the same predicate; checking it
                        // first spares the regime vector it builds per call on
                        // the events without an emission, nearly all of them.
                        let fired = engine
                            .trigger_events()
                            .iter()
                            .any(|fact| fact.feature == trigger);
                        if sample && fired {
                            let decision = event.canonical_key();
                            for intent in
                                plan.intents(decision, engine.state(), engine.trigger_events())
                            {
                                simulator.decide(intent);
                            }
                        }
                    }
                },
            )?;
        let gaps = trailing_gaps(
            &(self.trailing)(&stream),
            plan.costs().funding() == FundingRule::Recorded,
            known,
        )?;
        // A gap is no trade, so no fill reads this book.
        let no_book = FeatureValue::WarmingUp {
            observed: 0,
            required: 1,
        };
        for gap in &gaps {
            simulator.observe(gap, &no_book);
        }
        let (records, stats) = simulator.finish();
        Ok(BacktestRun {
            summary,
            stats,
            records,
        })
    }
}

/// The trailing gaps the simulator observes before it finishes (ADR-060
/// D9), in canonical order: the [`Stream::Trades`] entries of `trailing`,
/// plus the [`Stream::Funding`] entries when `funding` (`funding.recorded@1`).
/// Every entry is checked before any is returned: a trades entry against
/// the trades `known` up to then, by the label driver's rule, and a funding
/// entry for a start at or before its end.
///
/// # Errors
///
/// [`ProviderError::Contract`] for the first entry, in canonical order,
/// that breaks its rule.
fn trailing_gaps(
    trailing: &[FeedGap],
    funding: bool,
    mut known: Option<EventTime>,
) -> Result<Vec<MarketEvent>, ProviderError> {
    let mut gaps: Vec<MarketEvent> = trailing
        .iter()
        .filter(|gap| gap.stream == Stream::Trades || (funding && gap.stream == Stream::Funding))
        .map(|gap| MarketEvent::FeedGap(*gap))
        .collect();
    gaps.sort();
    for gap in &gaps {
        let MarketEvent::FeedGap(gap) = gap else {
            continue;
        };
        if gap.stream == Stream::Trades {
            known = Some(check_trailing_trades_gap(gap, known)?);
        } else if gap.start > gap.end {
            return Err(ProviderError::Contract(format!(
                "trailing funding gap {} to {}: {}",
                gap.start,
                gap.end,
                StateError::InvalidGap {
                    start: gap.start,
                    end: gap.end,
                }
            )));
        }
    }
    Ok(gaps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::event::GapReason;

    fn gap(stream: Stream, start: i64, end: i64) -> FeedGap {
        FeedGap {
            stream,
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(end),
            reason: GapReason::MissingData,
        }
    }

    fn streams(gaps: &[MarketEvent]) -> Vec<(Stream, i64)> {
        gaps.iter()
            .map(|event| (event.stream(), event.time().as_millis()))
            .collect()
    }

    #[test]
    fn trailing_gaps_take_the_funding_stream_under_recorded_funding_only() {
        let trailing = [
            gap(Stream::Funding, 500, 3_000),
            gap(Stream::OpenInterest, 100, 4_000),
            gap(Stream::Trades, 1_000, 2_000),
        ];
        let known = Some(EventTime::from_millis(1_000));
        let recorded = trailing_gaps(&trailing, true, known).unwrap();
        assert_eq!(
            streams(&recorded),
            [(Stream::Trades, 2_000), (Stream::Funding, 3_000)]
        );
        let none = trailing_gaps(&trailing, false, known).unwrap();
        assert_eq!(streams(&none), [(Stream::Trades, 2_000)]);
    }

    #[test]
    fn a_broken_trailing_gap_is_a_contract_error() {
        let known = Some(EventTime::from_millis(1_000));
        for (trailing, funding) in [
            // A trades gap that starts after the known trades.
            (gap(Stream::Trades, 1_500, 2_000), false),
            // A trades gap that ends before them.
            (gap(Stream::Trades, 500, 900), false),
            // An inverted funding gap, under recorded funding only.
            (gap(Stream::Funding, 3_000, 2_000), true),
        ] {
            let error = trailing_gaps(&[trailing], funding, known).unwrap_err();
            assert!(matches!(error, ProviderError::Contract(_)), "{error}");
        }
        assert!(trailing_gaps(&[gap(Stream::Funding, 3_000, 2_000)], false, known).is_ok());
    }
}
