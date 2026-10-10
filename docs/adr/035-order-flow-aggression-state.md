# ADR-035: Order-flow and aggression state

- Status: accepted
- Date: 2026-10-07

## Context

The Market State & Regime brief ("Order Flow / Aggression State") and the
research brief (§8) ask for a deterministic order-flow state that keeps
aggressor flow apart from passive liquidity and from the price response it
gets: aggressive buy and sell volume and delta, CVD, trade intensity,
large-trade activity, and price response to aggression. Issue #17 leaves
several definitions open:

- how CVD is anchored and what a feed gap does to it;
- which rolling windows exist and how they step;
- what a "large" trade is;
- how intensity and price response are measured.

Earlier decisions constrain the answer:

- Trades carry the aggressor side (ADR-021); raw data is the source of truth
  and every feature is reproducible from it (ADR-022). Aggression is not
  direction (ADR-023), and no feature here may become a signal (ADR-024).
- Quantities are exact fixed point at 1e-8; floats only for derived
  statistics in a defined order; arithmetic is explicit about overflow
  (ADR-027).
- One domain path for live and replay (ADR-019), in the canonical event
  order (ADR-028).
- Every feature is a registered `id@version` with an append-only lock and a
  golden test per version (ADR-029).
- Bars are built from trades on six UTC-aligned timeframes, and every bar
  already carries aggressive buy volume, sell volume and delta (ADR-031,
  decision 3). Incomplete bars carry `partial_start` or `feed_gap`.
- Live capture plans a sub-second reconnect gap on every stream once a day
  (ADR-032, decision 7). ADR-033 keeps incomplete bars with data in its
  series for the same reason.
- A feature earns its place by measured contribution (ADR-013).

Per-bar delta therefore ships already; this decision adds what spans bars.

## Decision

1. **Rolling windows: 5m, 15m and 1h over closed 1m bars.** A window is the
   last N closed 1m bars (N = 5, 15, 60) and steps once per closed 1m bar.
   There is no intrabar value: the developing minute is excluded. Because
   the windows sit on the 1m grid, a window that ends on a 5m, 15m or 1h
   boundary has exactly the volumes, delta and trade count of that
   timeframe's closed bar.

   Why these horizons (a judgment; no measurement supports it). 5m, 15m and
   1h are the timeframes of the ADR-031 set between the 1m grid and 4h. On
   those the closed-bar agreement above is a free check of the window
   against bars that are themselves cross-checked against klines
   (ADR-031). The horizons above are already served: 4h and 1d by the
   per-bar delta (ADR-031, decision 3), the day by the UTC-day CVD
   (decision 2). Below 5m the 1m bar's own delta ships. What breaks at
   other values: a length that is no bar timeframe, such as 7m, has no
   closed bar to agree with, so the window loses its only check; a 4h
   window would need a ring of 241 minutes (decision 9). Whether these
   horizons carry information is not established here; ADR-013 assigns
   that judgment to the order-flow candidates (#22), which can add another
   window as its own id.
2. **CVD under two explicit anchors**, as two features:
   - **Continuous** (`flow.cvd.continuous@1`): the sum of signed aggressor
     quantity (buy `+qty`, sell `−qty`) since the first consumed trade, the
     run anchor. It continues through trades gaps and counts them. The
     value carries `anchor` and `gaps`. Its level is relative to the run,
     like the ATR seed (ADR-033), so it is not comparable across replay
     windows. The difference between two values is the exact market delta
     when both carry the same `anchor` and `gaps`.
   - **UTC day** (`flow.cvd.utc_day@1`): the CVD since 00:00 UTC. It equals
     the delta of the developing `bars.time.1d@1` bar by construction and
     carries that bar's coverage. Its value is reproducible from any replay
     window that starts at or before the day's open; its coverage follows
     ADR-031, so a window starting exactly at 00:00 reports `partial_start`,
     and only an earlier start yields `complete`.
3. **Gap policy.** Windows never go back to warming up. A window carries
   the OR of its minutes' coverage; an empty incomplete minute contributes
   zeros and its `feed_gap` flag. This is the ADR-031 bar policy, and
   ADR-033's reasoning applies: with a reset on every gap, a 60-minute
   window would be unavailable for an hour after each daily reconnect.
4. **Large print.** A print is large when `|price × qty| ≥ 100 000 USDT`,
   computed exactly in `i128`. The threshold is per print (`aggTrade`),
   absolute, inclusive, and the parameter `large_notional_usdt`. One
   `aggTrade` holds one taker order's fills at one price, so a taker order
   that sweeps k levels is k prints, each judged on its own size: sweeps are
   understated.

   Where 100 000 comes from. No exchange limit or brief defines a large
   print, so the only possible source is measurement. The value is a round
   number set when this ADR was drafted, before any measurement; the
   measurement below shows that it passes the band, not that it is the best
   value inside the band. It is absolute USDT, not a fixed BTC quantity,
   because a taker's notional is the capital it commits, while a fixed
   quantity changes that size with the price level (1.6 BTC is 100 000 USDT
   at a price of 62 500 and 160 000 at 100 000). The USDT share still
   drifts with price level and activity, which is why the check below is per
   day over twelve months and not pooled.

   The band (a judgment, not a measurement or an exchange number). The
   archive averages about 1.65 M prints a day (603 218 226 rows over 365
   days), about 5 700 per 5-minute window. At 0.1 % that is about 6 large
   prints per 5m window: below it, quiet windows show 0 or 1 and
   `large_count`, `large_buy` and `large_sell` degenerate into an on/off
   flag. At 2 % (1 print in 50) it is about 115 per 5m window: above it the
   large class stops being a tail and `large_volume_share` tracks total
   volume. The band is checked per day, not pooled, because #22 consumes
   windows on every day.

   Measured (*Accept when*, 12 UTC days, the 15th of each month): print
   share 1.03 % ... 2.00 % per day (pooled 255 626 of 17 488 959 prints,
   1.46 %), volume share 39 % ... 58 %, all twelve days inside the band.
   Of 3 456 five-minute buckets with prints, 3 (0.09 %) had no large print.

   | Day | Prints | Large | Print share | Volume share |
   |---|---:|---:|---:|---:|
   | 2025-10-15 | 1 978 366 | 27 444 | 1.387 % | 49.63 % |
   | 2025-11-15 | 1 305 142 | 14 501 | 1.111 % | 39.25 % |
   | 2025-12-15 | 2 183 186 | 33 275 | 1.524 % | 51.42 % |
   | 2026-01-15 | 1 713 783 | 27 447 | 1.602 % | 51.63 % |
   | 2026-02-15 | 1 693 202 | 17 375 | 1.026 % | 42.75 % |
   | 2026-03-15 | 1 269 283 | 15 559 | 1.226 % | 46.58 % |
   | 2026-04-15 | 1 439 403 | 18 061 | 1.255 % | 47.55 % |
   | 2026-05-15 | 1 341 111 | 23 960 | 1.787 % | 54.28 % |
   | 2026-06-15 | 1 354 068 | 20 387 | 1.506 % | 48.57 % |
   | 2026-07-15 | 1 107 089 | 16 378 | 1.479 % | 48.87 % |
   | 2026-08-15 | 204 161 | 3 246 | 1.590 % | 49.80 % |
   | 2026-09-15 | 1 900 165 | 37 993 | 1.999 % | 57.63 % |

   2026-08-15 is a quiet Saturday: its row count equals the import ledger
   and #12 found no holes.

   The upper edge was nearly reached. On 2026-09-15, the day with the
   highest share in the sample (its 18:00 UTC hour is also the sample's
   busiest), the print share is 37 993 / 1 900 165 = 1.99946 %: inside by
   about 11 prints (2 % is 38 003.3). The share rises with activity and with
   the BTC price level, and the sample has no day above it. This is a fact
   for any future re-check of the threshold.

   What breaks at a different value (print share per day over the same 12
   days):

   | Threshold (USDT) | Min | Max | Days inside 0.1 % ... 2 % |
   |---:|---:|---:|---:|
   | 25 000 | 4.569 % | 6.913 % | 0 of 12 (above the band) |
   | 50 000 | 2.546 % | 4.159 % | 0 of 12 (above the band) |
   | **100 000** | 1.026 % | 1.999 % | 12 of 12 |
   | 250 000 | 0.249 % | 0.617 % | 12 of 12 |
   | 500 000 | 0.065 % | 0.182 % | 9 of 12 (2026-02-15, 2025-11-15 and 2026-04-15 fall below) |
   | 1 000 000 | 0.017 % | 0.048 % | 0 of 12 (below the band) |

   At 25 000 and 50 000 USDT the class is no tail (one print in 15 to 40).
   At 1 000 000 it is nearly empty. At 250 000 the band would also hold,
   with more margin on the upper edge and less on the lower; the band does
   not choose between 100 000 and 250 000, and this ADR does not claim it
   does. A percentile of recent trade sizes is rejected below.

   Caveat from ADR-047. Trades of the `trades` file that no aggregate
   covers were measured up to 2026-06-09, at most 478 a day (2026-05-15),
   and on none of the 22 sampled days from 2026-06-10. Against about 1.65 M
   prints a day that is at most 0.03 %, below the band's resolution; even if
   all 478 were large, 2026-05-15 would move from 1.787 % to 1.823 %. The
   day nearest the upper edge, 2026-09-15, lies after the last uncovered
   trade.
5. **Intensity**: trades per minute and volume per minute over the
   window's nominal span (N minutes), whatever its coverage. An incomplete
   window understates intensity, and its coverage flag says so.
6. **Price response.**
   - The reference price is the last trade price at the window's start:
     the close of the latest minute with trades before the window. `None`
     until the run has a trade before the window starts.
   - `displacement = last trade price at the window's end − reference`, as
     an exact `Price`.
   - `response = displacement / delta`, in USDT per BTC of net aggression.
     `None` when the delta is 0 or there is no displacement.
   - A negative response means price moved against the net aggression.
     That is a measurement; it is never read as a direction (ADR-023).
     Baselines and effort-versus-result judgments belong to the order-flow
     candidates (#22).
7. **Exactness.** Sums use `Qty` and `u64` with checked arithmetic; the
   notional is exact in `i128`. Floats are derived on demand in this order
   and never stored, so `MarketState` stays `Eq`:
   - `imbalance = delta / volume` (`None` when the volume is 0);
   - `trades_per_minute = trade_count / N`;
   - `volume_per_minute = (volume / 1e8) / N`;
   - `large_volume_share = (large buy + large sell volume) / volume`
     (`None` when the volume is 0);
   - `price_response = displacement / delta` (decision 6).

   Each ratio divides the `f64` conversions of the exact unit counts.
   Overflow rejects the event as `StateError::Overflow` and leaves the state
   unchanged.
8. **Features** (the ids are permanent):
   - `flow.cvd.continuous@1`: parameter `gap_policy` `continue_counted`;
     input trades; warm-up `Samples(1)`, where a sample is a trade.
   - `flow.cvd.utc_day@1`: parameter `session_ms` 86 400 000; input
     `bars.time.1d@1`; warm-up `Samples(1)`, where a sample is a
     trades-stream event.
   - `flow.window.5m@1`, `flow.window.15m@1`, `flow.window.1h@1`:
     parameters `large_notional_usdt` 100 000 and `window_ms` 300 000 /
     900 000 / 3 600 000; inputs trades (the large-print classification) and
     `bars.time.1m@1` (everything else); warm-up `Samples(N)`, where a
     sample is a closed 1m bar.

   `MarketState.flow` carries the five values. Each names the feature that
   produced it. They are stepped on the same atomic path as bars and
   volatility: a rejected event changes none of them.
9. **Ring of 61 closed minutes.** The engine keeps the last 61 closed 1m
   bars in a fixed ring: 60 for the longest window (1h) plus the minute
   before it, whose close is the window's reference price (decision 6; the
   `RING` constant in `crates/mie-domain/src/flow.rs`). One extra minute is
   enough because every minute carries the last close forward over empty
   minutes (`close_after`), so the latest close before the window is always
   the close of that one minute. At 60 the 1h window has no reference
   minute and never a displacement or a price response. Above 61 every
   closed minute copies state that no input reads. Decision 1 gives the
   cost of a 4h window (241). The ring is copied only when a minute closes.

## Consequences

- The windows lag by up to one minute plus the time to the next
  trades-stream event, which closes the minute. Nothing intrabar is
  visible.
- The continuous CVD level is not reproducible across replay windows; only
  differences under the same `anchor` and `gaps` are. Consumers that need a
  reproducible level use the UTC-day CVD.
- Taker orders that sweep several price levels are understated as large
  prints (decision 4).
- An incomplete window understates intensity; the coverage flag is the only
  warning (decision 5).
- The price response is undefined at zero net aggression, and a value near
  zero delta can be very large; it is a raw measurement for #22 to baseline.
- Nothing here emits a direction, a side bias or a signal (ADR-023,
  ADR-024). The order-flow candidates (#22) judge the measurements.
- Per trade the work is one checked add and one `i128` multiply; the ring
  (decision 9) is copied only when a minute closes.

## Alternatives considered

- **Sliding windows over individual trades in event time.** Memory grows
  with the trade rate (the busiest hour of the 12 sampled days held
  438 754 prints, 2026-09-15 18:00 UTC; a sample, so a lower bound on
  peaks), values change on every print, and the windows could no longer be
  checked against the bars they must agree with. Rejected.
- **Resetting the CVD at every gap.** ADR-032 plans a sub-second reconnect
  gap every day, so the CVD would reset at a random time each day.
  Rejected.
- **A UTC-day CVD alone.** Leaves divergences across days (#22) to every
  consumer. Rejected; both anchors ship.
- **A percentile of recent trade sizes as the large-print threshold.**
  Needs a warm-up, depends on the window, costs more per trade, and can
  ship later as its own id if research justifies it (ADR-013). Rejected for
  `@1`.
- **A fixed BTC quantity as the large-print threshold.** Its economic size
  moves with the price level (decision 4); the USDT share moves with it too,
  but the threshold keeps meaning the same capital. Rejected for `@1`.
- **A ring of 60 closed minutes.** The 1h window would have no reference
  price (decision 9). Rejected.
- **Windows of other lengths** (for example 7m, or 4h). No closed bar to
  check against, or a ring of 241 minutes (decision 1). Rejected for `@1`;
  a further window can ship as its own id if #22 shows a need.
- **Merging same-millisecond, same-side prints** to recover sweeps. Merges
  distinct taker orders and delays the classification by one print.
  Rejected.
- **Resetting windows after a gap.** A 60-minute window would be
  unavailable for an hour after each daily reconnect (decision 3).
  Rejected.

## Accept when

Both rules hold on the sample below; there is no clause that lets a mismatch
be explained away.

Sample: the 15th of each month from 2025-10 to 2026-09, 12 UTC days of the
#12 backfill. One day per month covers every month's price level and
activity once, chosen by calendar and not by outcome (the calendar rule of
the ADR-047 sample). It replaces "30 consecutive days": the measurement is
a property of each print, nothing carries across days, and 30 adjacent days
sample one price regime while the threshold is an absolute USDT figure.
12 days are about 17.5 M prints, so count noise at a share near 0.1 % is
negligible; day-to-day regime variation is what the twelve months are for.

- **R0, the run is valid.** For every day the measuring script's row count
  equals `rows` in the import ledger and the zip's sha256 equals the
  published `.CHECKSUM` and the ledger `sha256`; the script's classifier
  reproduces the boundary cases of the `flow.rs` test
  (`large_prints_meet_the_threshold_exactly`). Any mismatch voids the run;
  it is never read as a result.
- **R1, the threshold holds.** On each of the 12 days the print share at
  100 000 USDT lies within [0.1 %, 2 %], both ends inclusive. One day
  outside is a FAIL. Widening the sample may characterise a failure, never
  rescue it. On a FAIL, a `@2` of the three window ids with a recalibrated
  `large_notional_usdt` supersedes this decision (ADR-029 lock).

Recorded, not criteria: volume share per day, maximum prints in one UTC
hour, the share of 5m buckets without a large print, the sensitivity table
(decision 4).

Met 2026-10-11: R0 valid on all 12 days (sha256, `.CHECKSUM` and ledger
agree; script rows equal ledger rows; boundary cases reproduced). R1 PASS,
12 of 12 days inside the band, from 1.026 % (2026-02-15) to 1.99946 %
(2026-09-15; see decision 4 for the margin). The measuring script, the
sha256 of the 12 archive zips and the full results are posted in
<https://github.com/Replikanti/mie/issues/78#issuecomment-6102779743>;
the script is a one-off over archive files and is not committed (the
ADR-047 precedent).

References: ADR-013, ADR-019, ADR-021, ADR-022, ADR-023, ADR-024, ADR-027,
ADR-028, ADR-029, ADR-031, ADR-032, ADR-033, ADR-034, ADR-047.
