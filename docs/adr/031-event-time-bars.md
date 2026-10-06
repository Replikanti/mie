# ADR-031: Event-time bars on a fixed timeframe set

- Status: proposed
- Date: 2026-10-06

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

References: ADR-019, ADR-022, ADR-023, ADR-027, ADR-028, ADR-029. ADR-029
stays proposed: its *Accept when* also needs a volatility feature (#16).
