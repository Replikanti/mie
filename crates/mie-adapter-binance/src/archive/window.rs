//! A temporary market-data provider over imported archive days, for the
//! kline cross-check (ADR-034 D7).
//!
//! **Superseded by #11**: the general `HistoricalDataProvider` replaces it.
//! Until then it lets `mie archive-kline-check` drive real archive days
//! through `MarketStateEngine`.
//!
//! It selects one trade stream (`aggTrades` or `trades`) over
//! `[start − 60 s, end + 60 s)` and the six kline streams over
//! `[start, end)`, so every in-window bar of every timeframe can close
//! complete. Every record is normalized, and the events are delivered in the
//! full canonical order (`Ord for MarketEvent`, ADR-028 D5) — the same
//! sequence a k-way merge of the per-stream sequences yields — with exact
//! repeats and repeated trade ids dropped. Feed gaps are not synthesized:
//! archive holes are reported by `archive-verify`.

use super::ARCHIVE_SOURCE;
use super::catalog::ArchiveStream;
use super::normalize::parse;
use mie_domain::bars::Timeframe;
use mie_domain::event::MarketEvent;
use mie_domain::time::EventTime;
use mie_ports::outbound::{MarketDataProvider, ProviderError, ReplayWindow};
use mie_ports::raw::{DatasetVersion, RawRecordSource, RawSelection, RawStreamKey};
use std::collections::BTreeSet;

/// How far the trade window reaches beyond the requested window.
pub const TRADE_MARGIN_MS: i64 = 60_000;

/// The archive's trades and klines over a window, in canonical order.
#[derive(Debug)]
pub struct ArchiveWindowProvider {
    events: std::vec::IntoIter<MarketEvent>,
    versions: Vec<(String, DatasetVersion)>,
}

fn source_error(detail: impl std::fmt::Display) -> ProviderError {
    ProviderError::Source(detail.to_string())
}

/// Reads and normalizes every record of `streams` over `window`.
fn load(
    source: &dyn RawRecordSource,
    symbol: &str,
    streams: &[ArchiveStream],
    window: ReplayWindow,
    events: &mut Vec<MarketEvent>,
) -> Result<DatasetVersion, ProviderError> {
    let mut keys = BTreeSet::new();
    for stream in streams {
        keys.insert(
            RawStreamKey::new(ARCHIVE_SOURCE, symbol, stream.raw_name()).map_err(source_error)?,
        );
    }
    let selection = RawSelection::new(keys, window).map_err(source_error)?;
    let dataset = source.select(&selection).map_err(source_error)?;
    for file in &dataset.files {
        let stream = ArchiveStream::from_raw_name(file.stream.stream())
            .ok_or_else(|| source_error(format!("unknown archive stream {}", file.stream)))?;
        for record in source.read(file).map_err(source_error)? {
            if !window.contains(record.event_time) {
                continue;
            }
            let event = parse(stream, symbol, &record.payload).map_err(|e| {
                source_error(format!(
                    "{} at {}: {e}",
                    file.relative_path, record.event_time
                ))
            })?;
            events.extend(event);
        }
    }
    Ok(dataset.version)
}

impl ArchiveWindowProvider {
    /// Loads `window` from the archive source of `source`, with trades from
    /// `trade_stream` (`AggTrades` or `Trades`).
    ///
    /// # Errors
    ///
    /// [`ProviderError::Source`] when the store fails or a record does not
    /// normalize; [`ProviderError::Contract`] for another trade stream.
    pub fn open(
        source: &dyn RawRecordSource,
        symbol: &str,
        trade_stream: ArchiveStream,
        window: ReplayWindow,
    ) -> Result<Self, ProviderError> {
        if !matches!(
            trade_stream,
            ArchiveStream::AggTrades | ArchiveStream::Trades
        ) {
            return Err(ProviderError::Contract(format!(
                "{trade_stream} is not a trade stream"
            )));
        }
        let mut events = Vec::new();
        let trades_window = ReplayWindow {
            start: EventTime::from_millis(window.start.as_millis() - TRADE_MARGIN_MS),
            end: EventTime::from_millis(window.end.as_millis() + TRADE_MARGIN_MS),
        };
        let trades = load(source, symbol, &[trade_stream], trades_window, &mut events)?;
        let kline_streams: Vec<_> = Timeframe::ALL
            .into_iter()
            .map(ArchiveStream::Klines)
            .collect();
        let klines = load(source, symbol, &kline_streams, window, &mut events)?;

        events.sort_unstable();
        events.dedup();
        let mut seen_trades = BTreeSet::new();
        events.retain(|event| match event {
            MarketEvent::Trade(trade) => seen_trades.insert(trade.trade_id),
            _ => true,
        });
        Ok(Self {
            events: events.into_iter(),
            versions: vec![
                (trade_stream.raw_name().to_owned(), trades),
                ("klines".to_owned(), klines),
            ],
        })
    }

    /// The dataset versions read: the trade stream's, then the klines'.
    pub fn dataset_versions(&self) -> &[(String, DatasetVersion)] {
        &self.versions
    }

    /// Events not delivered yet.
    pub fn remaining(&self) -> usize {
        self.events.len()
    }
}

impl MarketDataProvider for ArchiveWindowProvider {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        Ok(self.events.next())
    }
}
