# ADR-031: Event-time bars on a fixed timeframe set

- Status: accepted
- Date: 2026-10-06
- Amended: 2026-10-10 (#76)

## Context

Volatility (#16), volume profile (#20), structure (#21) and
multi-timeframe context (Market State & Regime brief, brief §8) all start
from bars. Several earlier decisions constrain how bars are built:

- Live processing and replay share one domain path and must agree exactly
  (ADR-019). The domain has no wall clock: "now" is the ordering time of the
  last consumed event (ADR-028).
- Raw trades are the source of truth (ADR-022). Klines enter the domain as
  closed bars for cross-checks only, and no feature derives OHLCV from them
  (ADR-028).
- Prices and quantities are exact fixed point, and arithmetic is explicit
  about overflow (ADR-027).
- Continuity loss is an event, `FeedGap { stream, start, end }`, announced
  when the stream resumes. An ongoing outage is silent for its stream
  (ADR-028).
- Every feature is a registered `id@version` with an append-only lock and a
  golden-output test per version (ADR-029).

## Decision

1. **Timeframes**: 1m, 5m, 15m, 1h, 4h and 1d (`Timeframe::ALL`). Each one
   divides 86 400 000 ms, so Unix-epoch alignment is UTC-midnight alignment,
   and each is a Binance kline interval, so every bar can be cross-checked.
   The set is fixed in code; each timeframe is its own registered feature.
2. **Intervals**: a bar covers the half-open interval `[open, open + tf)`,
   with `open` a multiple of `tf` since the Unix epoch. A trade belongs to
   the bar containing its trade time `T`, the ADR-028 ordering time.
3. **Built from trades**: every timeframe is built directly from the trade
   sequence, never from klines and never by rolling up a shorter timeframe.
   A bar carries OHLC, volume, aggressive buy and sell volume (buy volume is
   Binance taker-buy volume), `delta = buy - sell` (aggression, not
   direction: ADR-023), the trade count and its coverage.
4. **Closing**: only trades-stream events close bars — a `Trade`, or a
   `FeedGap` on `Stream::Trades` — when their ordering time is at or after a
   bar's end. Events of other streams never close or mark a bar.
5. **Empty intervals**: every elapsed interval yields a bar, so a series has
   no holes. An interval without trades is a bar with no OHLC and zero
   volumes; no price is invented.
6. **Completeness** (`Coverage`): the bar containing the first consumed
   trades-stream event is `partial_start`. A trades gap `[start, end]` marks
   `feed_gap` on every open bar it overlaps (`open <= end` and
   `start < open + tf`): the developing bar, the bars it opens while
   advancing to `end`, and the bar containing `end`. Closed bars are
   immutable.
7. **Exposure**: per timeframe, `MarketState.bars` holds the last closed bar
   and the developing bar as `FeatureValue<Bar>` — warming up until the
   first close and the first trades-stream event respectively.
   `MarketStateEngine::closed_bars()` returns the bars the last accepted
   event closed, sorted by `(end, timeframe)`. `MarketState` keeps no N-bar
   history: consumers (#16, #21) keep their own windows.
8. **No look-ahead**: a closed bar is never revised, a bar closes only once
   an event at or after its end was consumed, and the developing bar holds
   exactly the trades consumed in its interval so far. Higher timeframes
   reach lower ones through closed bars plus the explicit developing bar.
9. **Features**: `bars.time.1m@1`, `bars.time.5m@1`, `bars.time.15m@1`,
   `bars.time.1h@1`, `bars.time.4h@1` and `bars.time.1d@1`, each with the
   parameter `timeframe_ms`, input `Stream(Trades)` and warm-up `Samples(1)`
   (a sample is a closed bar). Gap policy: mark `coverage.feed_gap`; a series
   never goes back to warming up.
10. **Arithmetic**: volume, delta, trade count and bar times use checked
    operations. An event that would overflow any of them is rejected
    (`StateError::Overflow`) and leaves the state unchanged.
11. **Bounded jumps**: one event may close at most `MAX_BARS_PER_EVENT` =
    44 640 bars of one timeframe — 31 days of 1m bars. Before any bar is
    built, the number of bars a trade or trades gap would close is computed
    in constant time from the developing bar's end and the event's ordering
    time (`gap.end` for a gap); above the bound the event is rejected with
    `StateError::TimeJump { event, timeframe, from, bars }` and the state is
    unchanged. The 1m series closes the most bars, so it is the one
    reported. Rationale: a week-long outage (10 080 1m bars) stays well
    inside, while a corrupt timestamp — a microsecond value read as
    milliseconds, or a first trade far from the next — would otherwise
    materialize millions to billions of empty bars and exhaust memory
    instead of failing as a typed error. Recovery: the error halts the drive
    like any other rejection; a dataset with a genuine longer hole is
    replayed as two runs, each from a fresh engine.
12. **Kline cross-check**: a complete bar matches the exchange kline of its
    interval when the interval (open time, and close time = end − 1 ms),
    open, high, low, close, volume and taker-buy volume are equal. An empty
    bar is compared on the two volumes only. The trade count is not
    compared — bars count aggregate trades, klines raw trades — and is
    reported separately. Incomplete bars are skipped. The harness
    (`mie_app::kline_check`) is generic over `MarketDataProvider`, so it runs
    unchanged on a live stream or an archive replay.

## Consequences

- Bars are identical whichever other streams a dataset carries (archive vs
  live), because only trades-stream events touch them.
- A closed bar becomes visible at the next trade or trades gap rather than at
  the first event of any kind; on BTCUSDT that is milliseconds.
- A silent trades outage cannot produce *complete* empty bars: no
  trades-stream event arrives during it, so nothing closes until the gap
  that marks those bars.
- A `LateEvent` gap (ADR-028 §6) whose `start` reaches back before
  already-closed bars marks only bars that are still open. A closed bar can
  then miss the late trade without being marked; live hold-back misses are
  the only source of this.
- A long gap materializes one empty bar per elapsed interval: a 7-day gap
  closes 10 080 1m bars in one event. The `MarketDataProvider` contract
  sets no bound on timestamps, so the domain bounds the jump itself
  (decision 11): a hole longer than 31 days cannot be replayed in one run
  and must be split at the hole.
- Higher-timeframe bars equal the fold of their 1m bars (OHLC, volumes,
  delta, count, OR of coverage); a property test pins it although nothing is
  rolled up.
- Features that need a continuous price series (ATR, #16) choose their own
  rule for empty bars.
- Adding a timeframe means a new registered feature id; weekly and monthly
  bars need a non-epoch alignment and are out of this decision.

## Alternatives considered

- **Closing on events of any stream** (the issue's first wording). A closed
  bar would appear sooner, but during a silent trades outage mark-price or
  depth events would close complete, empty bars that the later gap could no
  longer amend, and bars would depend on which streams a dataset carries.
  Rejected.
- **Rolling higher timeframes up from 1m bars.** Same result for complete
  data, but couples every timeframe to the 1m series; building each from
  trades is as simple, and the fold is still pinned by a test. Rejected.
- **A flat bar at the previous close for empty intervals** (the Binance
  kline convention). Invents a price no trade printed. Rejected; the
  cross-check compares empty bars on volume only.
- **A runtime-configurable timeframe set.** Each timeframe is a registered,
  locked feature id; a configurable set would compute unregistered
  features. Rejected.

## Accept when

The kline cross-check on a backfilled day (run with the archive backfill,
#12) matches every complete bar, or explains each mismatch.

## Acceptance

Accepted 2026-10-07 on the archive backfill of #12 (ADR-034 D8), which is
the backfilled-day run this criterion names; no live-capture comparison is
part of it. Bars built from individual trades match the archive klines bar
for bar on a clean 3-day window (2026-02-05 … 02-07: 5565/5565). Bars built
from aggTrades differ at aggregation boundaries on the same bars (1658 of
5565 there), as expected: an aggregate's constituent trades can fall in a
minute other than its `transact_time`; the same bar code on individual
trades explains every one of them. The other window (2025-10-09 … 10-11)
matches except 153 bars on 2025-10-10 22:03 – 24:00 UTC, an upstream defect
of `BTCUSDT-trades-2025-10-10.zip`: from 22:03 the individual-trades dump
omits trades that both the aggTrades file and the klines contain. Single
skipped trade ids are skipped by the klines too and cause no mismatch.
Results: <https://github.com/Replikanti/mie/issues/12#issuecomment-6032650398>.

References: ADR-019, ADR-022, ADR-023, ADR-027, ADR-028, ADR-029. ADR-029
stays proposed: its *Accept when* also needs a volatility feature (#16).

## Justification addendum (2026-10-10)

Added for #76 (audit #70). It explains where the numbers and the
acceptance above come from. It changes no value, criterion or decision.

### Decision 11: `MAX_BARS_PER_EVENT` = 44 640

History: the bound came in the #46 review round (commit 4e1ad5d), after
the review showed that one event could build an unbounded number of bars.
The one-week-outage sentence in decision 11 was its only stated reason.

The bound has two jobs, and neither forces 31 days:

- **Turn a corrupt timestamp into a typed error before it allocates.** The
  #46 review measured two classes. A defaulted `t = 0` followed by a real
  trade (about 1.7 × 10¹² ms later) is 28 M 1m bars, about 36 M over the
  six timeframes: 3.5 GB at 96 B per `Bar`, a process abort under a 2 GB
  limit. A microsecond value read as milliseconds is about 2.8 × 10¹⁰ 1m
  bars. Even a 365-day bound (525 600 1m bars) stays more than 50× below
  the smaller of the two, so any value from 8 to 365 days detects both.
- **Let a genuine hole through in one run.** The holes known inside one
  source are short. `archive-verify` reports 0 holes over 60 s on every
  stream of the 12-month backfill (#12). The #9 soak ran about 30 h inside
  a 30.2 h span, so its restart and its network outage lasted minutes
  together (ADR-032 Acceptance). The longest hole in the data, about
  6.7 days between the archive's end (2026-09-30) and the start of live
  capture (2026-10-07 15:50 UTC), spans two sources, and a replay reads
  only one (ADR-039 D6).

The cost per event is small at either end. At the bound one event builds
57 505 bars over the six timeframes (44 640 + 8 928 + 2 976 + 744 + 186 +
31), about 5.5 MB. A 365-day bound would allow 677 075 bars in one event,
about 65 MB and 0.28 s (measured in the #46 review).

So 31 days is one calendar month: a round margin over a one-week hole,
not a derived limit. What another value breaks:

- **8 days**: a host outage longer than 8 days inside one source no longer
  replays in one run and must be split at the hole (decision 11,
  Recovery). For the ATR regime the split costs nothing extra: a hole that
  long already yields empty incomplete 1h bars, which break the series
  back to warm-up (ADR-033 decision 6). Other consumers lose the windows
  they held across the hole, as with any fresh engine.
- **365 days**: corruption is still caught, but one event may allocate
  65 MB and stall for 0.28 s before the domain decides.

### Accept when: "matches every complete bar, or explains each mismatch"

History: the criterion did not say what counts as an explanation. The
approved #12 plan (step 5, 2026-10-06) fixed it before the run: an
aggTrades mismatch is explained (aggregation boundary) when the same bar
matches under `--trade-source trades`; any mismatch under `trades` is
unexplained and blocks the acceptance of this ADR.

Applied to the two windows of the run
(<https://github.com/Replikanti/mie/issues/12#issuecomment-6032650398>):

- **2026-02-05 … 02-07**: 1658 aggTrades mismatches, 0 under `trades`
  (5565/5565), so 0 unexplained. This window alone meets the
  pre-registered rule, and with it "a backfilled day".
- **2025-10-09 … 10-11**: 153 mismatches under `trades`, every one a bar
  that overlaps 2025-10-10 22:03 – 24:00 UTC. Under the pre-registered
  rule they are unexplained. The acceptance counted them as an upstream
  defect of `BTCUSDT-trades-2025-10-10.zip` on different evidence:
  trade ids that lie inside aggTrades `first_trade_id..last_trade_id` and
  that the klines count are missing from the trades file (minute 22:03:
  id range 20 276, kline count 20 269, trades file 20 266). That was a
  departure from the pre-registered rule, disclosed here.

The run used the temporary window provider (ADR-034 decision 8). ADR-039
replaced it, and `archive-kline-check` now replays through the shared
sequencer, which turns every skipped trade id into a `SequenceBreak` gap.
A re-run on 2026-10-10 over the February window: `--trade-source
aggTrades` reproduces the #12 report exactly (5565 bars compared, the same
1658 mismatches); `--trade-source trades` compares 5 complete bars, skips
5566 as incomplete and still prints `PASS`. The trades-source evidence of
this acceptance therefore cannot be regenerated with the command as it
stands; the #12 output is the record.

### Bars from aggregate trades (the choice ADR-034 decision 9 left open)

ADR-034 decision 9 left open whether this ADR is accepted with the
aggregation-boundary mismatch class documented, or amended (for example
to bars from individual trades). The acceptance took the first branch
without naming it. The bar code is source-agnostic (decision 3: bars are
built from the trade sequence). The bars consumers see come from
aggregate trades: live capture reads `aggTrade` (ADR-032), and an archive
replay leaves out `trades` by default and refuses it next to `aggTrades`
(ADR-039 D5).

Why aggregate trades:

- **Live source.** ADR-032 keeps `aggTrade` as the live trade source
  because its `a` is the ADR-028 sequence id. ADR-019 allows one domain
  path for live and replay, so replay feeds the same kind of trade.
- **Continuity.** The archive aggTrades have 0 id breaks over the
  12 months (603 218 226 rows, `archive-verify`, #12). The individual-trades
  files have 155 464 skipped ids over the 10 imported days, plus the
  2025-10-10 defect. Through the shared sequencer every skipped id is a
  `SequenceBreak` gap (in the re-run above, 5566 of 5571 bars came out
  incomplete). Bars from individual trades would need their own continuity
  rule for that id space, which is an ADR-028/ADR-032 change, not a
  switch.
- **Cost.** On the same 10 days, trades hold 2.15× the rows of aggTrades
  (85.2 M against 39.7 M, import ledger rows) and 1.63× the stored bytes
  (1 532 MiB against 940 MiB, per-day ratio 1.49–1.80). Over a year that
  is an estimated 22–26 GiB more storage, and the year's replay (604 M
  events in about 20 min, ADR-039 Acceptance, nearly all of them
  aggTrades) would roughly double.

The measured price of this choice, from the aggTrades runs of #12 (bars
compared per window: 5565):

| Bars that differ from the kline | 2025-10-09 … 10-11 | 2026-02-05 … 02-07 |
|---|---|---|
| any field | 1039 (18.7 %) | 1658 (29.8 %) |
| close | 0 | 0 |
| high or low | 25 (0.45 %), all 1m or 5m | 47 (0.84 %), all 1m or 5m |
| open | 368 | 713 |
| volume | 1038 | 1651 |
| taker-buy volume | 536 | 884 |
| 1d bars | 3 of 3 (volume) | 3 of 3 (volume) |

The cause is the one the acceptance names: an aggregate's volume is booked
to the minute of its `transact_time`, while some of its trades fall in
another minute. Any consumer that checks its output against exchange
klines, or against a chart or profile built from them, inherits this
class. Highs and lows of 15m and longer bars matched in both windows, so
the ADR-037 chart spot check on 1h and 4h is unaffected; volumes are not,
so the ADR-036 comparison with an external session volume profile is.

Rejected: bars from individual trades, for the three reasons above.
Changing this takes a superseding ADR.

### Three days instead of one

The criterion names one backfilled day. The run compared three consecutive
days because ADR-034's acceptance names three; why three is explained in
the ADR-034 justification addendum. Each day contributes all 1855 of its
bars (1440 + 288 + 96 + 24 + 6 + 1), 5565 per window. The 6 bars skipped
per run are the `partial_start` bars that open at the start of the 60 s
trade margin before the window (`TRADE_MARGIN_MS`), outside the window.
