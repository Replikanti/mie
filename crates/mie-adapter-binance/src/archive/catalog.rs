//! The archive datasets this importer reads, their file paths and their
//! UTC periods (days and months).
//!
//! Every path is under the USDⓈ-M futures prefix `data/futures/um/` of the
//! public archive (ADR-002: the BTCUSDT perpetual only). Availability is
//! never taken from the archive's bucket listing, which lags the CDN by
//! months: each file is probed through its `.CHECKSUM` sidecar instead.

use mie_domain::bars::Timeframe;
use mie_domain::event::Stream;
use std::fmt;

/// Milliseconds per UTC day.
pub const DAY_MS: i64 = 86_400_000;

/// Days between 0000-03-01 and 1970-01-01 in the proleptic Gregorian
/// calendar (the epoch shift of the civil-date algorithm).
const EPOCH_SHIFT_DAYS: i64 = 719_468;
const DAYS_PER_ERA: i64 = 146_097;

/// Howard Hinnant's `days_from_civil`: (year, month, day) to days since
/// 1970-01-01 in the proleptic Gregorian calendar.
///
/// A private copy of the raw-store adapter's helper: adapters share no code
/// (ADR-025).
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * DAYS_PER_ERA + doe - EPOCH_SHIFT_DAYS
}

/// Howard Hinnant's `civil_from_days`, the inverse of [`days_from_civil`].
pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + EPOCH_SHIFT_DAYS;
    let era = z.div_euclid(DAYS_PER_ERA);
    let doe = z - era * DAYS_PER_ERA;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Fixed-width ASCII digits as a number; `None` for anything else.
pub(crate) fn digits(text: &str) -> Option<i64> {
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        text.parse().ok()
    } else {
        None
    }
}

/// Parses `YYYY-MM-DD` to days since 1970-01-01. Only real dates written
/// with exactly these widths are accepted.
pub fn parse_day(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let (year, month, day) = (
        digits(&text[0..4])?,
        digits(&text[5..7])?,
        digits(&text[8..10])?,
    );
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    (civil_from_days(days) == (year, month, day)).then_some(days)
}

/// Days since 1970-01-01 as `YYYY-MM-DD`.
pub fn day_label(days: i64) -> String {
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

/// One archive file's period: a UTC day or a UTC calendar month.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Period {
    /// A UTC day, as days since 1970-01-01.
    Day(i64),
    /// A UTC calendar month.
    Month {
        /// The year.
        year: i64,
        /// The month, 1 to 12.
        month: i64,
    },
}

impl Period {
    /// The month containing day `days`.
    pub fn month_of(days: i64) -> Self {
        let (year, month, _) = civil_from_days(days);
        Self::Month { year, month }
    }

    /// The first day of the period, as days since 1970-01-01.
    pub fn first_day(self) -> i64 {
        match self {
            Self::Day(days) => days,
            Self::Month { year, month } => days_from_civil(year, month, 1),
        }
    }

    /// The day after the period's last day.
    pub fn end_day(self) -> i64 {
        match self {
            Self::Day(days) => days + 1,
            Self::Month { year, month } => {
                if month == 12 {
                    days_from_civil(year + 1, 1, 1)
                } else {
                    days_from_civil(year, month + 1, 1)
                }
            }
        }
    }

    /// First millisecond of the period.
    pub fn start_ms(self) -> i64 {
        self.first_day() * DAY_MS
    }

    /// First millisecond after the period.
    pub fn end_ms(self) -> i64 {
        self.end_day() * DAY_MS
    }

    /// `YYYY-MM-DD` or `YYYY-MM`, as in the archive's file names.
    pub fn label(self) -> String {
        match self {
            Self::Day(days) => day_label(days),
            Self::Month { year, month } => format!("{year:04}-{month:02}"),
        }
    }

    /// Parses a [`label`](Self::label).
    pub fn parse(text: &str) -> Option<Self> {
        if text.len() == 10 {
            return parse_day(text).map(Self::Day);
        }
        let bytes = text.as_bytes();
        if bytes.len() != 7 || bytes[4] != b'-' {
            return None;
        }
        let (year, month) = (digits(&text[0..4])?, digits(&text[5..7])?);
        (1..=12)
            .contains(&month)
            .then_some(Self::Month { year, month })
    }
}

impl fmt::Display for Period {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.label())
    }
}

/// Whether a dataset is published per day or per month.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodKind {
    /// One file per UTC day (`daily/`).
    Daily,
    /// One file per UTC month (`monthly/`).
    Monthly,
}

impl PeriodKind {
    /// The periods of this kind that overlap the inclusive day range
    /// `[from_day, to_day]`, in time order.
    pub fn periods(self, from_day: i64, to_day: i64) -> Vec<Period> {
        let mut periods = Vec::new();
        if from_day > to_day {
            return periods;
        }
        match self {
            Self::Daily => periods.extend((from_day..=to_day).map(Period::Day)),
            Self::Monthly => {
                let mut month = Period::month_of(from_day);
                while month.first_day() <= to_day {
                    periods.push(month);
                    month = Period::month_of(month.end_day());
                }
            }
        }
        periods
    }
}

/// One imported archive dataset of the USDⓈ-M futures archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ArchiveStream {
    /// Aggregate trades (`daily/aggTrades`), normalized to `Trade`.
    AggTrades,
    /// Klines of one ADR-031 interval (`daily/klines/<interval>`),
    /// normalized to `Kline`.
    Klines(Timeframe),
    /// Funding settlements (`monthly/fundingRate`), normalized to
    /// `FundingSettlement`.
    FundingRate,
    /// Five-minute open interest and ratios (`daily/metrics`), normalized to
    /// `OpenInterest`.
    Metrics,
    /// Aggregated depth in percentage bands (`daily/bookDepth`): raw only,
    /// no domain event fits it.
    BookDepth,
    /// Individual trades (`daily/trades`): raw only for replay; normalized
    /// for the kline cross-check alone.
    Trades,
}

impl ArchiveStream {
    /// Every stream, in import order.
    pub const ALL: [Self; 11] = [
        Self::AggTrades,
        Self::Klines(Timeframe::M1),
        Self::Klines(Timeframe::M5),
        Self::Klines(Timeframe::M15),
        Self::Klines(Timeframe::H1),
        Self::Klines(Timeframe::H4),
        Self::Klines(Timeframe::D1),
        Self::FundingRate,
        Self::Metrics,
        Self::BookDepth,
        Self::Trades,
    ];

    /// The raw stream name, a valid ADR-030 path segment: `aggTrades`,
    /// `klines_1m` … `klines_1d`, `fundingRate`, `metrics`, `bookDepth`,
    /// `trades`.
    pub fn raw_name(self) -> &'static str {
        match self {
            Self::AggTrades => "aggTrades",
            Self::Klines(Timeframe::M1) => "klines_1m",
            Self::Klines(Timeframe::M5) => "klines_5m",
            Self::Klines(Timeframe::M15) => "klines_15m",
            Self::Klines(Timeframe::H1) => "klines_1h",
            Self::Klines(Timeframe::H4) => "klines_4h",
            Self::Klines(Timeframe::D1) => "klines_1d",
            Self::FundingRate => "fundingRate",
            Self::Metrics => "metrics",
            Self::BookDepth => "bookDepth",
            Self::Trades => "trades",
        }
    }

    /// The stream whose [`raw_name`](Self::raw_name) is `name`.
    pub fn from_raw_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.raw_name() == name)
    }

    /// Daily or monthly files.
    pub fn period_kind(self) -> PeriodKind {
        match self {
            Self::FundingRate => PeriodKind::Monthly,
            _ => PeriodKind::Daily,
        }
    }

    /// The archive's dataset directory name.
    fn dataset(self) -> &'static str {
        match self {
            Self::AggTrades => "aggTrades",
            Self::Klines(_) => "klines",
            Self::FundingRate => "fundingRate",
            Self::Metrics => "metrics",
            Self::BookDepth => "bookDepth",
            Self::Trades => "trades",
        }
    }

    /// The zip's file name, e.g. `BTCUSDT-aggTrades-2026-09-30.zip` or
    /// `BTCUSDT-1m-2026-09-30.zip`.
    pub fn file_name(self, symbol: &str, period: Period) -> String {
        let kind = match self {
            Self::Klines(timeframe) => timeframe.label(),
            other => other.dataset(),
        };
        format!("{symbol}-{kind}-{}.zip", period.label())
    }

    /// The zip's path below the archive's base URL, e.g.
    /// `data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-09-30.zip`.
    pub fn archive_path(self, symbol: &str, period: Period) -> String {
        let cadence = match self.period_kind() {
            PeriodKind::Daily => "daily",
            PeriodKind::Monthly => "monthly",
        };
        let interval = match self {
            Self::Klines(timeframe) => format!("{}/", timeframe.label()),
            _ => String::new(),
        };
        format!(
            "data/futures/um/{cadence}/{}/{symbol}/{interval}{}",
            self.dataset(),
            self.file_name(symbol, period)
        )
    }

    /// The exact header line of the CSV inside the zip (checked 2025-10 and
    /// 2026-09).
    pub fn expected_header(self) -> &'static str {
        match self {
            Self::AggTrades => {
                "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker"
            }
            Self::Klines(_) => {
                "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore"
            }
            Self::FundingRate => "calc_time,funding_interval_hours,last_funding_rate",
            Self::Metrics => {
                "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio"
            }
            Self::BookDepth => "timestamp,percentage,depth,notional",
            Self::Trades => "id,price,qty,quote_qty,time,is_buyer_maker",
        }
    }

    /// The domain series the stream normalizes to; `None` for raw-only
    /// streams.
    pub fn domain_stream(self) -> Option<Stream> {
        match self {
            Self::AggTrades | Self::Trades => Some(Stream::Trades),
            Self::Klines(_) => Some(Stream::Klines),
            Self::FundingRate => Some(Stream::Funding),
            Self::Metrics => Some(Stream::OpenInterest),
            Self::BookDepth => None,
        }
    }

    /// Whether a replay of the archive includes the stream by default.
    /// `trades` duplicates `aggTrades` at finer grain and `bookDepth` has no
    /// domain event, so both are opt-in.
    pub fn replay_default(self) -> bool {
        !matches!(self, Self::Trades | Self::BookDepth)
    }
}

impl fmt::Display for ArchiveStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.raw_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_ports::raw::validate_segment;

    #[test]
    fn raw_names_are_valid_segments_and_round_trip() {
        for stream in ArchiveStream::ALL {
            assert_eq!(validate_segment(stream.raw_name()), Ok(()));
            assert_eq!(
                ArchiveStream::from_raw_name(stream.raw_name()),
                Some(stream)
            );
        }
        assert_eq!(ArchiveStream::from_raw_name("klines"), None);
        assert_eq!(ArchiveStream::from_raw_name("bookTicker"), None);
    }

    #[test]
    fn archive_paths_follow_the_usd_m_layout() {
        let day = Period::Day(parse_day("2026-09-30").unwrap());
        assert_eq!(
            ArchiveStream::AggTrades.archive_path("BTCUSDT", day),
            "data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-09-30.zip"
        );
        assert_eq!(
            ArchiveStream::Klines(Timeframe::M1).archive_path("BTCUSDT", day),
            "data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-09-30.zip"
        );
        assert_eq!(
            ArchiveStream::Klines(Timeframe::D1).file_name("BTCUSDT", day),
            "BTCUSDT-1d-2026-09-30.zip"
        );
        assert_eq!(
            ArchiveStream::Metrics.archive_path("BTCUSDT", day),
            "data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-09-30.zip"
        );
        assert_eq!(
            ArchiveStream::BookDepth.archive_path("BTCUSDT", day),
            "data/futures/um/daily/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2026-09-30.zip"
        );
        assert_eq!(
            ArchiveStream::Trades.archive_path("BTCUSDT", day),
            "data/futures/um/daily/trades/BTCUSDT/BTCUSDT-trades-2026-09-30.zip"
        );
        let month = Period::Month {
            year: 2026,
            month: 8,
        };
        assert_eq!(
            ArchiveStream::FundingRate.archive_path("BTCUSDT", month),
            "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-08.zip"
        );
    }

    #[test]
    fn only_normalized_default_streams_replay() {
        let replayed: Vec<_> = ArchiveStream::ALL
            .into_iter()
            .filter(|s| s.replay_default())
            .collect();
        assert_eq!(replayed.len(), 9);
        assert_eq!(ArchiveStream::BookDepth.domain_stream(), None);
        assert_eq!(ArchiveStream::Trades.domain_stream(), Some(Stream::Trades));
        assert_eq!(
            ArchiveStream::FundingRate.period_kind(),
            PeriodKind::Monthly
        );
    }

    #[test]
    fn days_parse_strictly_and_round_trip() {
        assert_eq!(parse_day("1970-01-01"), Some(0));
        assert_eq!(parse_day("2026-10-06"), Some(1_791_244_800_000 / DAY_MS));
        assert_eq!(
            parse_day("2024-02-29").map(day_label).as_deref(),
            Some("2024-02-29")
        );
        for bad in [
            "2025-02-29",
            "2026-13-01",
            "2026-04-31",
            "2026-1-01",
            "2026/10/06",
            "26-10-06",
            "",
        ] {
            assert_eq!(parse_day(bad), None, "{bad}");
        }
        for days in [-1, 0, 19_000, 20_732, 100_000] {
            assert_eq!(parse_day(&day_label(days)), Some(days));
        }
    }

    #[test]
    fn daily_periods_cover_the_inclusive_range_with_leap_days() {
        let from = parse_day("2024-02-27").unwrap();
        let to = parse_day("2024-03-01").unwrap();
        let labels: Vec<_> = PeriodKind::Daily
            .periods(from, to)
            .into_iter()
            .map(Period::label)
            .collect();
        assert_eq!(
            labels,
            ["2024-02-27", "2024-02-28", "2024-02-29", "2024-03-01"]
        );
        assert!(PeriodKind::Daily.periods(to, from).is_empty());
    }

    #[test]
    fn monthly_periods_overlap_the_range_across_years() {
        let from = parse_day("2025-10-15").unwrap();
        let to = parse_day("2026-02-01").unwrap();
        let months = PeriodKind::Monthly.periods(from, to);
        let labels: Vec<_> = months.iter().map(|p| p.label()).collect();
        assert_eq!(
            labels,
            ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02"]
        );
        let feb = Period::Month {
            year: 2024,
            month: 2,
        };
        assert_eq!(feb.end_day() - feb.first_day(), 29);
        assert_eq!(feb.start_ms(), parse_day("2024-02-01").unwrap() * DAY_MS);
        let dec = Period::Month {
            year: 2025,
            month: 12,
        };
        assert_eq!(dec.end_day(), parse_day("2026-01-01").unwrap());
        assert_eq!(Period::parse("2025-12"), Some(dec));
        assert_eq!(Period::parse("2025-13"), None);
        assert_eq!(
            Period::parse("2026-09-30"),
            Some(Period::Day(parse_day("2026-09-30").unwrap()))
        );
    }
}
