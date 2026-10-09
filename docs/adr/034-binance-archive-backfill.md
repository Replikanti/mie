# ADR-034: Binance archive backfill — verbatim rows, per-file ledger, end-of-interval open interest

- Status: accepted
- Date: 2026-10-06
- Amended: 2026-10-10 (#76)

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
  (`…001`). Rates are plain eight-place decimals (`0.00003355`), except
  those below 10⁻⁶ in magnitude, which carry an exponent (`-1.8E-7`,
  `-6E-8`, `9.0E-7`): 12 of 1095 rows in 2025-10 … 2026-09. No decimal
  column the other streams normalize carried one in that window (0
  normalize errors in `archive-verify`), and the live streams send plain
  decimals (`"r":"0.00000016"`).
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
   provenance can never land in the live source; the ingest configuration
   rejects it as a live source, so live capture can never write to or lock
   it either. Header lines are not
   records; the ledger keeps them. The importer lives in
   `mie-adapter-binance` (`archive` module), reusing its HTTP/TLS stack and
   decimal parsing; it writes only through the raw-store ports. An archive
   decimal may carry an exponent (`[eE][+-]?[0-9]+`, any decimal column):
   the archive normalizer rewrites it exactly into the ADR-027 plain grammar
   by moving the point (digit strings, never a float) before the shared
   domain parser, so `-1.8E-7` and `-0.00000018` give the same units and the
   eight-place and range limits stay the domain's. The live normalizer keeps
   rejecting exponents.
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
   and normalizes it with zero errors — and fails when any configured
   stream has no imported file for a period of the range, so a partial or
   empty backfill never passes; and `mie archive-kline-check` drives
   real archive days through `MarketStateEngine` with a temporary window
   provider (`archive::window`, superseded by #11). The full `mie replay` of
   the backfill is #11's acceptance.
9. **Kline cross-check (D8).** `mie archive-kline-check --trade-source
   aggTrades|trades` runs the ADR-031 harness on archive days with either
   trade source. A check that compares no complete bar (for example
   `trades` never imported) fails: it is no evidence. A run on individual
   trades with zero mismatches explains
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

## Acceptance

Accepted 2026-10-07 after the acceptance run of #12 (window 2025-10-01 …
2026-09-30): dry run 3297 published, 0 missing; import with a deliberate
`kill -9` mid-file resumed without duplicates; idempotent re-run with 0
downloads and unchanged manifest list and dataset versions; `archive-verify`
PASS (after the exact fix for exponent-notation fundingRate rows, #55); the
kline cross-check with `--trade-source trades` matched 5565/5565 bars on
2026-02-05 … 02-07. One upstream defect is documented:
`BTCUSDT-trades-2025-10-10.zip` omits trades from 22:03 UTC to the end of the
file. Full results:
<https://github.com/Replikanti/mie/issues/12#issuecomment-6032650398>.

## Justification addendum (2026-10-10)

Added for #76 (audit #70). It explains where the numbers above come from
and how they behaved in the #12 acceptance run. It changes no value,
criterion or decision. Run figures are from the #12 results
(<https://github.com/Replikanti/mie/issues/12#issuecomment-6032650398>)
unless stated otherwise.

### Decision 7: network etiquette

History: the #12 plan set every value in D6 (approved 2026-10-06) without
a derivation. The code carries the same defaults (`FetchPolicy` in
`crates/mie-adapter-binance/src/archive/fetch.rs`, `DownloadTimeouts` in
`crates/mie-adapter-binance/src/transport.rs`). The
`binance/binance-public-data` README publishes no rate limit for
`data.binance.vision` (checked 2026-10-10), so no exchange number exists
to derive them from. They are a conservative ceiling, checked here against
the measured run.

- **One request at a time.** No parallel load on a CDN whose limits are
  unknown, and it matches exactly-once: one `PENDING` file at a time
  (decision 4). Parallel downloads would need several pending markers and
  a resume rule for each.
- **≥ 100 ms between request starts**: at most 10 requests per second. The
  dry run made 3297 requests in 41 min, 0.75 s per request on average, so
  the spacing seldom binds: 3297 × 100 ms = 5.5 min, about 13 % of the
  run. At 1 s the spacing alone would take 55 min, longer than the whole
  latency-bound run. At 0 nothing would bound a burst when the CDN answers
  fast.
- **Backoff, the real schedule.** The wait doubles from 1 s after every
  failed attempt except the last (`FetchPolicy::retry`). At 5 attempts the
  waits are 1 + 2 + 4 + 8 = 15 s in total. The 60 s cap first binds at
  8 attempts (the 7th wait, 64 s → 60 s), so at the defaults it is never
  reached. Why that is enough: a file that fails every attempt fails alone,
  the run continues and exits 1, and a re-run retries only that file at
  the cost of one `.CHECKSUM` request per file already imported
  (decision 4). The #12 import had 0 failed files in 3278 downloads.
- **Retried statuses.** 429 (rate limited), 418 (the IP is banned after
  ignoring 429s) and 403 (a web-application-firewall limit was hit) are
  the statuses the Binance API documentation lists under its limits; 5xx
  and transport errors are transient. 404 means "not published" and is
  never retried: availability is probed through `.CHECKSUM` 404s, so a
  retried 404 would add 15 s to every unpublished file of a dry run.
- **Connect 10 s**: TCP connect and TLS handshake, also used as the wait
  for the response headers (`timeout_recv_response`). An average request
  completed in 0.75 s, so 10 s is more than 13× that: only a stalled
  connection hits it, and its retry then starts after 1 s instead of
  hanging.
- **Body 15 min.** The largest files of the window belong to its busiest
  day: `BTCUSDT-trades-2026-02-05.zip` is 155 350 401 bytes and
  `BTCUSDT-aggTrades-2026-02-05.zip` 111 860 342 bytes (`Content-Length`,
  checked 2026-10-10). Carrying them within 15 min needs a sustained
  173 kB/s (about 1.4 Mbit/s) and 124 kB/s. A shorter limit fails big files
  on slow links (at 2 min the trades file needs 1.3 MB/s); a longer one
  holds the only request slot on a stalled body for longer before the
  retry.
- **No size cap.** The body is hashed while it streams, and the SHA-256
  check against `.CHECKSUM` rejects a wrong body of any size. A runaway
  body is bounded by the 15 min body limit. A cap would have to follow
  market activity: the busiest day had 9.54 M aggTrades rows against a
  median of 1.49 M (import ledger rows), so a cap set from a typical day
  would fail the days research wants most.

### Decision 6: ≥ 50 GB free

History: the #12 plan (D5) estimated about 7 GB downloaded and 12–25 GB
stored, and asked for 50 GB, about 2× the upper estimate. The 36.8 MB in
decision 6 is one aggTrades day measured while the PR was written; the
year averages 40.6 MiB per day.

Measured on the store after #12 (`du` per stream): aggTrades 14 815 MiB,
bookDepth 344 MiB, the six klines 73 MiB, metrics 14 MiB, fundingRate
3 MiB, so about 15 250 MiB (16.0 GB) for the default streams; trades
1 532 MiB for the 10 cross-check days; the ledger 13 MiB. Total 17.6 GB,
as reported on #12. About 7 GB was downloaded, as estimated.

What 50 GB covers:

- the default backfill about 3 times over;
- or the default backfill plus an estimated year of individual trades:
  the trades/aggTrades byte ratio of 1.49–1.80 on the 10 imported days
  (ADR-031 justification addendum) applied to 14 815 MiB gives 22–26 GiB
  (23–28 GB), which leaves 6–11 GB. This is an extrapolation from 10
  high-activity days, not a measurement;
- with live capture writing to the same `raw_root` at about 720 MB per day
  (ADR-038 Acceptance), the about 32 GB left after the default backfill
  and the 10 trades days last about 45 days.

What another value breaks: at 25 GB (the upper estimate alone) the default
backfill fits, but the 9 GB left last about 12 days of live capture and
leave no room for a year of trades. A larger
figure breaks nothing; it would only turn away a disk that holds the
backfill. The figure is advice in the example configuration, not a check
in code.

### Window: 12 months (2025-10 … 2026-09)

History: #12 asked for "≥ 12 months". Its plan (D1) took the 12 full
months that ended with the last complete month before 2026-10-06, noting
that re-runs are idempotent, so extending later is cheap, and that
24 months doubles the volume.

What the window covers: the ADR-033 regime is ready after 214 hourly
samples, and 12 months hold 8 760 hourly bars, about 40× the warm-up,
which leaves 8 546 ready samples. The window holds the extreme days the
cross-check picked: 2025-10-10 (largest absolute 1d range, 20 980 USDT)
and 2026-02-06 (largest relative 1d range, 18.95 %).

What it is not: no sample-size derivation backs 12 months. How much
history walk-forward validation needs is the question of #33, and the
window can grow then.

Cost of each further year, from this year's figures and scaled by that
year's activity: about 16 GB stored, about 2 h of import (2 h 08 min,
3278 downloads) and about 20 min of replay (ADR-039 Acceptance).
Extending is idempotent (decision 4).

### Decision 9: the open choice

Settled in the ADR-031 justification addendum: ADR-031 was accepted with
the aggregation-boundary mismatch class documented, and bars come from
aggregate trades.

### Accept when: three consecutive days

History: the #12 plan (step 5) asked for three consecutive days that
include the window's day with the largest 1d range, without a reason for
three. The run centred each window on its extreme day.

What three days bought over one:

- two interior UTC midnights where bars on both sides are compared: the
  joins of the daily files, where trades at a day's first milliseconds can
  sit in no aggregate of that day's file (Context). With one day, neither
  join has bars compared on both sides, because the bar before the window
  is `partial_start`;
- 3 complete 1d bars and 18 4h bars instead of 1 and 6;
- the extreme day with a whole neighbour day on each side.

What more days cost: one more trades zip per day (3.5–19.6 M rows on the
10 imported days), each a repeat of the same per-day comparison. The
aggregation-boundary class already shows in 1039 and 1658 bars per window.
