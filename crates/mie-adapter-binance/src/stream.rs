//! The Binance USDⓈ-M feeds this adapter captures, and how each maps to a
//! raw stream name (ADR-030) and a domain series (ADR-028).

use mie_domain::event::Stream;

/// Fixed open-interest poll cadence in milliseconds.
///
/// It is also the `resolution_ms` of every live
/// [`OpenInterest`](mie_domain::event::OpenInterest), which keeps
/// normalization a pure function of the payload.
pub const OI_POLL_INTERVAL_MS: u32 = 10_000;

/// One captured Binance feed.
///
/// Ordered by declaration, which is also the index used to stagger planned
/// reconnects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BinanceStream {
    /// Aggregate trades (`<symbol>@aggTrade`).
    AggTrade,
    /// Mark price, index price and indicative funding (`<symbol>@markPrice@1s`).
    MarkPrice,
    /// Liquidation order snapshots (`<symbol>@forceOrder`).
    ForceOrder,
    /// One-minute klines (`<symbol>@kline_1m`).
    Kline1m,
    /// Open interest, polled over REST (`/fapi/v1/openInterest`).
    OpenInterest,
}

impl BinanceStream {
    /// Every stream, in declaration order.
    pub const ALL: [Self; 5] = [
        Self::AggTrade,
        Self::MarkPrice,
        Self::ForceOrder,
        Self::Kline1m,
        Self::OpenInterest,
    ];

    /// The raw stream name, a valid ADR-030 path segment.
    pub const fn raw_name(self) -> &'static str {
        match self {
            Self::AggTrade => "aggTrade",
            Self::MarkPrice => "markPrice",
            Self::ForceOrder => "forceOrder",
            Self::Kline1m => "kline_1m",
            Self::OpenInterest => "openInterest",
        }
    }

    /// The stream whose [`raw_name`](Self::raw_name) is `name`.
    pub fn from_raw_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.raw_name() == name)
    }

    /// The domain series the stream's events belong to.
    pub const fn domain_stream(self) -> Stream {
        match self {
            Self::AggTrade => Stream::Trades,
            Self::MarkPrice => Stream::MarkPrice,
            Self::ForceOrder => Stream::Liquidations,
            Self::Kline1m => Stream::Klines,
            Self::OpenInterest => Stream::OpenInterest,
        }
    }

    /// The captured stream whose events belong to the domain series
    /// `stream`; `None` for series this adapter does not capture.
    pub fn of_domain(stream: Stream) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.domain_stream() == stream)
    }

    /// The WebSocket stream path for `symbol`, appended to the configured
    /// base URL; `None` for the REST-polled open interest.
    pub fn ws_path(self, symbol: &str) -> Option<String> {
        let symbol = symbol.to_ascii_lowercase();
        let suffix = match self {
            Self::AggTrade => "aggTrade",
            Self::MarkPrice => "markPrice@1s",
            Self::ForceOrder => "forceOrder",
            Self::Kline1m => "kline_1m",
            Self::OpenInterest => return None,
        };
        Some(format!("{symbol}@{suffix}"))
    }

    /// Position in [`ALL`](Self::ALL).
    pub const fn index(self) -> usize {
        self as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_ports::raw::validate_segment;

    #[test]
    fn raw_names_are_valid_segments_and_round_trip() {
        for stream in BinanceStream::ALL {
            assert_eq!(validate_segment(stream.raw_name()), Ok(()));
            assert_eq!(
                BinanceStream::from_raw_name(stream.raw_name()),
                Some(stream)
            );
            assert_eq!(BinanceStream::ALL[stream.index()], stream);
        }
        assert_eq!(BinanceStream::from_raw_name("depth"), None);
    }

    #[test]
    fn ws_paths_use_the_lowercase_symbol() {
        let paths: Vec<_> = BinanceStream::ALL
            .iter()
            .map(|s| s.ws_path("BTCUSDT"))
            .collect();
        assert_eq!(
            paths,
            [
                Some("btcusdt@aggTrade".to_owned()),
                Some("btcusdt@markPrice@1s".to_owned()),
                Some("btcusdt@forceOrder".to_owned()),
                Some("btcusdt@kline_1m".to_owned()),
                None,
            ]
        );
    }

    #[test]
    fn streams_map_to_distinct_domain_series() {
        let series: Vec<_> = BinanceStream::ALL
            .iter()
            .map(|s| s.domain_stream())
            .collect();
        assert_eq!(
            series,
            [
                Stream::Trades,
                Stream::MarkPrice,
                Stream::Liquidations,
                Stream::Klines,
                Stream::OpenInterest,
            ]
        );
        for stream in BinanceStream::ALL {
            assert_eq!(
                BinanceStream::of_domain(stream.domain_stream()),
                Some(stream)
            );
        }
        assert_eq!(BinanceStream::of_domain(Stream::OrderBook), None);
    }
}
