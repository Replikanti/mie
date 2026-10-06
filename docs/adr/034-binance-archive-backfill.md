# ADR-034: Binance archive backfill — verbatim rows, per-file ledger, end-of-interval open interest

- Status: proposed
- Date: 2026-10-06

## Context

#12 imports history from the Binance public data archive
(`data.binance.vision`), so research has the months to years of data that
live capture alone cannot provide (ATR percentile, conditional behavior,
walk-forward validation). The instrument is the BTCUSDT USDⓈ-M perpetual
only (ADR-002, TradingView `BTCUSDT.P`): archive prefix `data/futures/um/`,
symbol `BTCUSDT`, the same instrument the live adapter captures (ADR-032).
Spot, COIN-M and delivery contracts are out of scope.

The earlier decisions frame the importer:

- Raw data is stored verbatim, under a source of its own, and parsing
  happens in normalization (ADR-003, ADR-022, ADR-030). Archive records
  carry no capture metadata.
- Live and replay share one normalization (ADR-019); every event has one
  position in the canonical order built from exchange fields (ADR-028).
  ADR-028 left the ordering time of archive open interest to this issue.
- Adapters may not depend on each other (ADR-025).
- Bars are built from trades and checked against exchange klines (ADR-031);
  its acceptance runs on a backfilled day.

The archive as checked on 2026-10-06 (first and last month of the window,
2025-10 and 2026-09):

- **Availability.** The S3 bucket listing behind the archive's web pages is
  stale (aggTrades stops at 2025-02-18, metrics at 2024-03-03), while the
  CDN serves files up to the previous day. Availability is therefore probed
  per file through its `.CHECKSUM` sidecar (200 or 404), never listed.
- **Format.** One deflated CSV entry per zip, a header line, `\n` line
  endings. `.CHECKSUM` holds `<sha256>  <file name>\n`. The headers are
  identical at both ends of the window. Prices drop trailing zeros
  (`83624.5`); metrics decimals carry 16 places with trailing zeros.
- **aggTrades** (daily): `agg_trade_id,price,quantity,first_trade_id,
  last_trade_id,transact_time,is_buyer_maker`, ms times, `true`/`false`.
  One day is ~1.43 M rows: a 95 MB CSV in a 17 MB zip.
- **klines/<interval>** (daily): 12 columns with `close_time`, `count`,
  `taker_buy_volume`.
- **fundingRate**: monthly files only; `calc_time` in ms with jitter
  (`…001`).
- **metrics** (daily, 288 rows): `create_time` is the text
  `YYYY-MM-DD HH:MM:SS`, rows are not in time order, no publication time.
- **bookDepth** (daily): `timestamp,percentage,depth,notional`, text times
  at second resolution; percentage bands, not a book (`-5` in 2025-10,
  `-5.00` in 2026-09).
- **trades** (daily, individual trades): `id,price,qty,quote_qty,time,
  is_buyer_maker`, ~28 MB zipped per day.
- Not usable for the window: `bookTicker` ended 2024-03-30; USD-M has no
  liquidation dataset.
- History starts (bucket listing): aggTrades and klines 2019-12-31,
  fundingRate 2020-01, metrics 2021-12-01, bookDepth 2023-01-01.
- Preview of the kline cross-check on 2026-09-30: 1m bars from
  **aggTrades** differ from the archive 1m klines in 119 of 1440 bars on
  volume (60 taker-buy volume, 42 open, 1 high, 1 low); bars from the
  individual **trades** file match 1440 of 1440. 65 aggregates have
  constituent trades in another minute than the aggregate's
  `transact_time`, and 3 trades at the day's first milliseconds are in no
  aggregate of that day's file.

## Decision

1. **Verbatim rows under a fixed source.** Each CSV data row of a
   checksum-verified file is one raw record, payload = the row bytes without
   its line terminator, `capture` null, under the source `binance-archive`.
   The source is a constant in code, not configuration, so archive
   provenance can never land in the live source. Header lines are not
   records; the ledger keeps them. The importer lives in
   `mie-adapter-binance` (`archive` module), reusing its HTTP/TLS stack and
   decimal parsing; it writes only through the raw-store ports.
2. **Streams (D2).** Default: `aggTrades` (→ `Trade`), `klines` on the six
   ADR-031 intervals (→ `Kline`, raw names `klines_1m` … `klines_1d`),
   `fundingRate` (→ `FundingSettlement`), `metrics` (→ `OpenInterest`,
   `resolution_ms` 300 000), and `bookDepth` stored raw only: no domain
   event fits percentage bands, DuckDB research reads them. `trades` is
   opt-in and raw only for replay; its normalizer serves the kline
   cross-check. Not imported: bookTicker, mark/index/premium-index price
   klines.
3. **Ordering times (ADR-028, ADR-030 D4).** aggTrades `transact_time`,
   klines `close_time`, fundingRate `calc_time` as published, bookDepth
   `timestamp`, trades `time`. **Archive open interest is ordered at
   `create_time + 300 000 ms`** (D3): the end of its five-minute interval.
   The archive carries no publication time, and look-ahead is the costlier
   error for forward labels (#26), so a value is never visible before its
   interval ended. The raw `event_time` is this ordering time, so store
   windows and delivered events agree; the day's 23:55 sample is filed
   under the next day's partition.
4. **Exactly once (D4).** One immutable ledger per archive file
   (`<ledger>/<raw stream>/<file>.import`: archive path, published SHA-256,
   stream, period, header, rows, sealed files with rows and hash; no
   wall-clock fields), written via temp file, fsync and rename, read-only.
   A file is **skipped** when its ledger's SHA-256 equals the freshly
   fetched `.CHECKSUM` and every listed file is in the store with the same
   hash. A different published hash is a **`changed` conflict**: reported,
   exit status 1, never re-imported automatically. Each file is validated
   before any append (exact header, every row's ordering time within one
   day of the file's period — a ms → µs switch fails the file) and its rows
   are sealed before the next file starts, so every sealed part belongs to
   one archive file. A `PENDING` marker names the file being appended.
   After a crash its sealed parts that no ledger lists (orphans) must equal,
   per date partition, a prefix of the file's rows (payload and event
   time); only the rest is appended and the ledger lists orphans and new
   parts. Any other orphan stops the run before anything is appended.
5. **Day-span parts (D4).** The archive writer uses `max_event_span_ms` =
   one day with the store's default row and byte limits: the live one-hour
   span would split each metrics or klines day into ~24 tiny files, and
   unsorted metrics rows would fragment it further.
6. **Storage (D5).** The same `raw_root` as live capture: separate source,
   separate lock (ADR-030). Ledger and staging directories next to it; zips
   are deleted once imported. A 12-month backfill needs a disk with
   ≥ 50 GB free (measured: one aggTrades day is 36.8 MB of Parquet).
7. **Network etiquette (D6).** One request at a time, ≥ 100 ms between
   request starts, `User-Agent: mie-archive-import
   (+https://github.com/Replikanti/mie)`. 403, 418, 429, 5xx and transport
   errors back off exponentially from 1 s to 60 s, at most 5 attempts; then
   the file is failed, the run continues and exits 1. Zips stream to disk
   while hashed (connect 10 s, body 15 min, no size cap); `.CHECKSUM`
   availability probing is the only discovery.
8. **Replayable (D7).** Until #11's `HistoricalDataProvider` exists,
   "replayable via #11" means: `mie archive-verify` re-reads every imported
   file (hash-verified), checks every record is filed at its ordering time
   and normalizes it with zero errors; and `mie archive-kline-check` drives
   real archive days through `MarketStateEngine` with a temporary window
   provider (`archive::window`, superseded by #11). The full `mie replay` of
   the backfill is #11's acceptance.
9. **Kline cross-check (D8).** `mie archive-kline-check --trade-source
   aggTrades|trades` runs the ADR-031 harness on archive days with either
   trade source. A run on individual trades with zero mismatches explains
   every aggTrades mismatch as an aggregation-boundary effect with the same
   bar code. Whether ADR-031 is then accepted with that mismatch class
   documented, or amended (for example bars from individual trades), is
   decided after the acceptance run, outside this ADR.

## Consequences

- Archive and live data sit side by side under different sources; merging
  or deduplicating them is #11/#13 work.
- Re-runs cost one `.CHECKSUM` request per file (~3 300 for 12 months) and
  no download; extending the window later is cheap.
- A republished archive file is never silently replaced; resolving a
  `changed` conflict is a manual, recorded decision.
- Metrics values reach consumers up to five minutes after the archive's
  `create_time`, so archive OI features lag the true publication time by at
  most one interval; live OI keeps its own 10 s resolution (ADR-032).
- Depth-dependent order-flow features have no archive history: bookDepth
  bands are coarse and raw only, and there is no diff-depth archive. They
  exist from the start of live depth capture (#10).
- aggTrades-built bars do not reproduce exchange klines exactly at
  aggregation boundaries; the kline check needs the individual-trades file
  to separate that effect from real disagreement.
- The temporary window provider holds a whole window in memory: the kline
  check is meant for a few days, not months.

## Alternatives considered

- **A separate `mie-adapter-binance-archive` crate.** ADR-025 forbids
  adapter → adapter edges, so it would duplicate the TLS/HTTP stack and the
  decimal helpers.
- **Discovery through the bucket listing.** Stale by months for the
  window; per-file `.CHECKSUM` probes are authoritative and cheap.
- **Open interest at `create_time`.** Matches the archive's label, but
  delivers a five-minute aggregate at the start of its interval — a
  look-ahead for forward labels.
- **Deduplicating by payload on re-runs.** Needs a scan of everything
  stored and cannot tell a crash leftover from a republished row; the
  per-file ledger with prefix resume is exact and cheap.
- **Re-importing a republished file.** Would leave two versions of the same
  rows in the immutable store; a conflict that a human resolves keeps
  provenance clean.
- **Live rotation policy (one-hour parts).** Dozens of tiny files per
  metrics or klines day.
- **Deferring both acceptance items to #11.** Leaves the backfill unverified
  until replay exists; `archive-verify` and the kline check run now.

## Accept when

The acceptance run of #12 passes: a 12-month dry run, import, a deliberate
`kill -9` mid-file followed by a resume without duplicates, an idempotent
re-run (no download, unchanged manifest list and dataset versions),
`archive-verify` PASS, and the kline cross-check on three consecutive days
with `--trade-source trades` matching every complete bar.
