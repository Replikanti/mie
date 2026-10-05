//! MIE application layer: use-case services that orchestrate the domain
//! through ports without owning any infrastructure (brief §3).
//!
//! [`drive`] is the single place where market data meets the domain. Live
//! analysis and historical replay both go through it — replay is an input
//! difference, not a second implementation (ADR-019).

use mie_domain::state::MarketStateEngine;
use mie_ports::inbound::{ReplayMarket, ReplayReport, UseCaseError};
use mie_ports::outbound::{HistoricalDataProvider, MarketDataProvider, ReplayWindow};

/// Feeds every event from `provider` into `engine` until the stream ends and
/// returns the number of events consumed.
///
/// # Errors
///
/// The first provider failure or domain rejection. Events consumed before it
/// stay applied to `engine`.
pub fn drive<P>(provider: &mut P, engine: &mut MarketStateEngine) -> Result<u64, UseCaseError>
where
    P: MarketDataProvider + ?Sized,
{
    let mut events = 0;
    while let Some(event) = provider.next_event()? {
        engine.apply(&event)?;
        events += 1;
    }
    Ok(events)
}

/// [`ReplayMarket`] over any [`HistoricalDataProvider`].
#[derive(Debug)]
pub struct ReplayService<H> {
    history: H,
}

impl<H: HistoricalDataProvider> ReplayService<H> {
    /// Creates the service over a historical data source.
    pub fn new(history: H) -> Self {
        Self { history }
    }
}

impl<H: HistoricalDataProvider> ReplayMarket for ReplayService<H> {
    fn replay(&self, window: ReplayWindow) -> Result<ReplayReport, UseCaseError> {
        let mut stream = self.history.replay(window)?;
        let mut engine = MarketStateEngine::new();
        let events = drive(&mut stream, &mut engine)?;
        Ok(ReplayReport {
            events,
            state: engine.state().clone(),
        })
    }
}
