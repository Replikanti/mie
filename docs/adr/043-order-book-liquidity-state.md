# ADR-043: Order-book liquidity state — the book in Market State, banded depth, liquidity flow and concentration

- Status: proposed
- Date: 2026-10-09

## Context

#18 asks for passive-liquidity features from the reconstructed L2 book:
imbalance, liquidity added and removed, and concentration. The issue leaves
three things to decide: the depth bands, how removed size is split between
cancellations and fills, and what counts as concentration. Its acceptance
criteria are tests (mixed cancels and fills, reset on gap, band edges) and
the book joining the live/replay equivalence hashes of #13.

What exists:

- The domain `OrderBook` (ADR-038 D6, accepted): a `BTreeMap` per side,
  reset by a snapshot, chained by update ids, invalidated by an order-book
  gap. Its **trusted window** is the snapshot's price range; beyond it only
  the levels changed since the snapshot are known, never their absence. "A
  snapshot is a reset, never liquidity flow." The engine is not wired to it
  yet.
- Live capture delivers `@depth@100ms` diffs, whose levels carry absolute
  quantities at the end of each batch with `T` the transaction time of the
  batch's last event, plus REST `limit=1000` snapshots: one on every resync
  and a checkpoint 60 s after the last fetch that re-anchors the book when
  provably consistent (ADR-038 D3, D7).
- The archive has no L2 diffs; its `bookDepth` dataset stays raw only
  (ADR-034).
- Taker trades arrive as `aggTrade` with the aggressor (ADR-021, ADR-023).
  In the canonical order a trade ranks before a book update of the same
  millisecond (ADR-028: rank 2 < 4).
- The Market State hash (ADR-041) covers every public field of
  `MarketState`; trackers are excluded on the grounds that a hidden
  divergence surfaces in the public state later.
- Earlier decisions constrain the answer: one domain path for live and
  replay (ADR-019), exact fixed point with explicit overflow (ADR-027),
  registered `id@version` features with a lock and goldens (ADR-029),
  rolling windows over closed 1m bars on the order-flow clock (ADR-031,
  ADR-035), raw data as the source of truth (ADR-022), measured
  contribution before trust (ADR-013), no direction or signal (ADR-012,
  ADR-023, ADR-024).

### Measurements behind the numbers

Every number below cites one of these. The probe used public data only.

- **M1, trusted window.** The #10 soak (ADR-038 Acceptance): 812 matched
  checkpoints, the narrower side of the window at p10 10 bps and median
  13 bps from mid; 14 of 812 at 5 bps or less. A REST probe of 12
  `limit=1000` snapshots on 2026-10-09 12:47–12:48 UTC: 15–16 bps per side.
- **M2, drift between re-anchors.** The checkpoint cadence is 60 s
  (ADR-038 D3), so the window can lose at most about one minute of price
  movement before it re-anchors. 1500 `1m` klines from 2026-10-08 11:48 to
  2026-10-09 12:47 UTC: the maximum excursion from the minute's open is p50
  3.9, p90 10.9, p95 14.6 and p99 29 bps.
- **M3, level density** (the 12 probe snapshots, per side): 42–68 levels
  within 1 bps, 105–138 within 2 bps, 316–361 within 5 bps. Level quantity:
  median 0.02–0.05 BTC, p90 at 29–50× the median, p99 at 93–200×. The 5th
  largest level within 5 bps was at least 80× the side's median in all 24
  side-snapshots.
- **M4, liquidations in the trade stream.** The Binance documentation site
  answers scripted requests with an empty `202`, so it could not be read
  when this was written. The check was made on data instead: over the #10
  soak's raw store, 387 of 393 `forceOrder` events have an `aggTrade` on
  the liquidation's taker side at exactly its average price, with at least
  its last filled quantity, within 100 ms (at or before the liquidation's
  `T`, never after). The same query shifted by ±10 s matches 17 and 7. A
  liquidation's execution is in the trade stream.

## Decision

1. **`book.l2@1`: the engine owns the book.**
   - `MarketState.book.l2: FeatureValue<OrderBook>`:
     - `WarmingUp{0, 1}` until the first snapshot;
     - `Ready(book)` while the book is valid;
     - `Unavailable{InputInvalid}` after an `OrderBook` gap or an update the
       book rejects (chain break, missed straddle, negative quantity), until
       the next snapshot builds a fresh `OrderBook`.
   - ADR-038 D6 semantics are unchanged. `OrderBook` gains read-only
     helpers only: `peek` (what `apply` would return, so a feature can
     decide before the book changes), `qty_at`, its state-hash encoder and a
     summary `Display`.
   - An invalidation is not a `StateError`. ADR-038 D8 already makes it a
     provider fault the live audit reports; replay must go on past it.
   - The state hash covers the whole book: the chain state and id, the
     trusted window and every level of both sides (encoding in ADR-041 D1).
     *Why:* a level that diverged outside the bands is invisible in derived
     features until price reaches it, possibly never within a run. ADR-041
     accepts that for trackers, but here the book is the state itself. Hashing
     it costs one pass over its levels per state checkpoint (60 s): the 2 000
     of a `limit=1000` snapshot plus those changed beyond the window since.
2. **Mid and bands.**
   - Mid = (best bid + best ask) / 2. Every comparison uses doubled units in
     `i128`, so it is exact for any mid, also a half-unit one.
   - Bands are cumulative and inclusive: **1, 2 and 5 bps** of mid. Price
     `p` on either side is in band `e` iff `|a + b − 2p| · SCALE ≤ e · (a + b)`,
     with `e` in `Rate` units (1 bps = 10 000).
   - *Why the absolute distance:* for the resting levels of an uncrossed
     book it equals the signed distance away from the spread. Decision 5
     measures an update's levels against the mid before the update, and a
     batch can place a level beyond that mid (a bid above it, an ask below
     it). The signed form gives such a level a negative distance, so a bid
     50 bps above mid would count in every band; the absolute form books it
     by how far it really is. Rejected: clamping the signed distance at 0,
     which still puts any far-side level in the 1 bps band.
   - *Why bps:* they scale with price; an absolute USDT band changes meaning
     as price moves. Rejected: ATR-scaled bands, which widen in high
     volatility, exactly when drift eats the window, so they would be out of
     range most when they matter; level-count bands, which depend on tick
     occupancy rather than distance.
   - *Why 5 bps outer:* the window shrinks on the side price moves toward,
     by up to the drift since the last re-anchor. Window minus outer edge
     leaves a margin of 8 bps at the soak median (13 bps, M1) and 11 bps in
     the probe (16 bps, M1). The minute's maximum excursion exceeds 8 bps in
     18 % and 11 bps in 10 % of the 1500 minutes (M2). That is an upper
     bound on the minutes in which the band is lost at some point: a band
     is lost only for the part of the minute beyond the margin. At 10 bps
     the margin is 3–6 bps, exceeded in 38–65 % of minutes: unusable.
   - *Why 1 and 2 bps inner:* the 1–2–5 series gives roughly even log steps
     over one decade below the outer band. The 2 bps band's margin of
     11–14 bps is exceeded in 5–10 % of minutes (M2). The 1 bps band still
     aggregates 42–68 levels per side (M3), so it is not top-of-book noise.
3. **Validity.**
   - A book that is not ready, one-sided or crossed (best bid ≥ best ask)
     makes depth and clusters `Unavailable{InputInvalid}`; before the first
     snapshot they warm up with it.
   - A band whose far edge lies beyond the trusted window on a side, or
     with no window on that side, is `Unavailable{OutOfRange}`. The edge
     exactly at the window bound is still in range: every level up to it is
     known. Depth bands need both sides; clusters are per side.
4. **`book.depth@1`.**
   - Contents: best bid and best ask; per band, the bid and ask quantity and
     level count.
   - Imbalance `(bid − ask) / (bid + ask)` is derived on demand as an `f64`
     from the exact `i128` difference and sum; `None` when both are 0.
   - Recomputed in commit after every book event, in one pass over the
     levels within 5 bps. Sums are `i128`; a sum outside the `Qty` range
     makes that band `OutOfRange`, so commit stays infallible.
5. **Attribution rule** (`book.liquidity.window.*`). For each `BookUpdate`
   applied to a ready book, with `m` the mid before the update:
   1. Within the update, the last value per (side, price) wins: one final
      quantity per level.
   2. Accounted prices lie inside the trusted window on their side and
      within 5 bps of `m` by absolute distance (decision 2). Each counts in
      every band that contains it.
   3. `Δ = final − old`, with `old` = 0 when the level is absent inside the
      window.
   4. Pending fills per (side, price): a `Trade` with aggressor `Buy` adds
      its quantity at (Ask, price), `Sell` at (Bid, price). Trades are
      recorded only while the book is ready. Each pending quantity has age 0
      (traded since the last update) or 1 (carried over one update).
   5. `Δ < 0`: `removed = −Δ`, `filled = min(removed, pending)` taking age 1
      before age 0, `cancelled = removed − filled`. `Δ > 0`: `added = Δ`.
   6. After the update: the age-1 remainder at accounted prices is booked as
      `added` **and** `filled` and also counted in `inferred`; elsewhere it
      is dropped. The age-0 remainder becomes age 1.
   7. A `BookSnapshot` (resync or checkpoint) books no flow and drops all
      pending fills (decision 7). An `OrderBook` gap or an invalidation also
      drops them.

   An update to a ready book without a mid (one-sided or crossed) accounts
   nothing and flags every band `beyond_window` for its minute: no band can
   be located.

   *Why:*
   - Depth diffs carry end-of-batch absolute quantities, so a decrease alone
     cannot tell fills from cancels; taker trades are the only evidence of a
     fill (ADR-021).
   - Carry of one update: the canonical order puts a trade before the
     update of its millisecond (ADR-028), but an exchange batch can end
     inside a millisecond, so a trade at the previous update's `T` may
     belong to the next batch. Every update has its own `T` (the batches
     are 100 ms apart), so one update of carry covers that boundary. More
     carry would let old fills explain unrelated later cancels at the same
     price.
   - Inference: a fill on a level that was replenished or created within
     one batch shows no decrease. The trade proves the volume rested there;
     `inferred` makes the guessed share visible.
   - Liquidations are not fills: their executions are in the trade stream
     (M4), so counting them would count them twice.

   *Rejected:*
   - **Gross rule** (fills = trades, the residual is adds or cancels, no
     carry): every trade/diff clock skew becomes an invisible spurious
     add-and-cancel pair.
   - **Net-only rule** (no inference): fills on replenished levels are lost,
     and `added` loses the replenishment with them.

   *Stated limits:*
   - An add and a cancel that offset each other at one price within a batch
     are invisible, so `added` and `cancelled` are lower bounds.
   - `filled` counts every taker trade at an accounted price exactly once
     while no reset intervenes, but a carried or inferred part is booked in
     the minute and with the mid of the update that matches or infers it,
     one update after its own. Over a run, the outer band's `filled` equals
     the traded volume; an inner band can differ by at most the inferred
     volume (the simulation test measures both).
6. **Windows.**
   - 5m, 15m and 1h over closed 1m bars, on the order-flow clock (ADR-035
     D1), so #22 can compare passive with aggressor flow over identical
     minutes. Flow counts in the minute of the update's time.
   - The minutes sit in a ring of 61 slots keyed by minute (60 for the
     longest window plus one, as ADR-042 D5 reasons); a step returns the one
     slot an event changes, with no per-event ring copy.
   - Warm-up `Samples(N)`: closed 1m bars from the minute of the first valid
     book. Before it the windows stay `WarmingUp{0, N}`: archive replays
     have no book, and zero flow there is not a quiet book.
   - `partial_start` as ADR-042 D5: the window contains the first valid
     book's minute. `feed_gap` marks the minutes that an `OrderBook` or
     `Trades` gap overlaps (a trades gap means missing fills), also minutes
     that already closed, with the ready windows that reach them; and the
     minute of a book event while `book.l2` is unavailable, or of the book
     event that invalidates it.
   - Per band, `beyond_window` flags a minute in which an accounted update
     saw that band's far edge beyond the window on a side: part of the
     band's flow was not observed.
   - Windows never go back to warming up (ADR-035 D3 reasoning: a daily
     reconnect would otherwise blank every window for an hour).
7. **Snapshots honour ADR-038 D6.**
   - The changes between the last applied update and a checkpoint's `L` are
     absorbed by the reset. At most one 100 ms batch is unobserved per
     checkpoint, and a checkpoint comes every 60 s (ADR-038 D3): at most
     0.1 s / 60 s ≈ 0.17 % of the time.
   - Pending fills are dropped because their effect is inside the snapshot.
   - *Rejected:* booking the old-book-to-snapshot difference as flow. It
     would cover that 0.17 %, but it contradicts accepted ADR-038 D6 and
     needs a superseding ADR, and the difference also contains whatever the
     book lost before the reset, which is not market activity.
8. **`book.clusters@1`.** Per side, within the 5 bps band:
   - the top **5** levels by quantity; on equal quantity the level nearer
     mid first;
   - the side's lower-median level quantity (the `⌊(n − 1) / 2⌋`-th
     smallest of `n`), its total quantity and its level count;
   - multiples of the median, derived on demand.

   There is no cluster threshold: #23 owns that through its scoring
   (ADR-013), and a threshold fixed here would be a parameter nobody
   measured.

   *Why K = 5:* about 1 % of the 316–361 levels per side (3–4) exceed the
   side's p99 (M3). The 5th candidate was at least 80× the median, well
   above the p90 of 29–50× (M3), so K = 5 holds the whole tail plus one
   spare. A smaller K drops ≥ 100× levels on typical books; a larger K adds
   levels that approach the bulk. *Why 5 bps:* the outer band of decision 2,
   with the same availability.
9. **Exactness and atomicity.**
   - A minute's flow and the pending sums use checked `Qty` and fail the
     step with `StateError::Overflow`; nothing is committed then, the book
     included.
   - A window sum outside the `Qty` range makes that window
     `Unavailable{OutOfRange}`, as a depth sum does its band (decision 4).
     *Why:* the windows are recomputed on the event that closes a minute,
     usually a trade. Failing the step there would reject an ordinary
     trade for every bar, flow and profile family for as long as the
     out-of-range minutes stay in the window, up to an hour; the window is
     the only value that cannot be computed.
   - The book is mutated only in commit, after every step succeeded.
   - Floats are derived on demand only, so `MarketState` stays `Eq`.
10. **Features** (the ids are permanent). Parameters are sorted by name; a
    band is a `Rate`.
    - `book.l2@1`: no parameters; input `OrderBook`; warm-up `Samples(1)`,
      where a sample is a snapshot.
    - `book.depth@1`: `band_1` 0.0001, `band_2` 0.0002, `band_3` 0.0005;
      input `book.l2@1`; warm-up `Samples(1)`.
    - `book.clusters@1`: `band` 0.0005, `top_k` 5; input `book.l2@1`;
      warm-up `Samples(1)`.
    - `book.liquidity.window.<5m|15m|1h>@1`: `band_1..3` as above,
      `carry_updates` 1, `window_ms` 300 000 / 900 000 / 3 600 000; inputs
      `book.l2@1`, the `Trades` stream and `bars.time.1m@1` (the clock);
      warm-up `Samples(5 | 15 | 60)`.

    `MarketState.book` carries the six values, declared after
    `derivatives`. They are stepped on the same atomic path as the other
    families: a rejected event changes none of them.
11. **Live only.** The archive has no L2 diffs (`bookDepth` stays raw only,
    ADR-034), so archive replays keep every `book.*` value warming up.

## Consequences

- The Market State carries the full book; checkpoint hashes compare every
  level of it between live and replay (ADR-041). The feature-set change
  moves all state-hash goldens and the feature-set version, and runs
  recorded before it compare events only (ADR-041 D2, D4).
- Every depth diff costs the engine one pass over the diff's levels and one
  pass over the levels within 5 bps (about 700 at the M3 density) for depth
  and clusters: on the order of 1–2 k operations per diff at 10 diffs/s,
  plus a full-book hash once per state checkpoint. #13's acceptance run
  measures it: its core channel blocked time must stay 0 ms, and its
  high-water is reported against the #9 and #10 baselines (ADR-041
  *Accept when*).
- With `limit=100` snapshots (the test fixture's window, about 1.5 bps) only
  the 1 bps band is in range; the default `limit=1000` is what makes 2 and
  5 bps available (M1).
- After a resync the windows carry `feed_gap` for the minutes of the
  outage; the flow of the unsynced period is lost, not estimated.
- `added` and `cancelled` are lower bounds, and the `inferred` share says
  how much of `filled` rests on the clock-alignment premise of decision 5.
- Nothing here emits a direction, a side bias or a signal (ADR-012,
  ADR-023, ADR-024). Absorption and exhaustion belong to #22, the cluster
  threshold and its scoring to #23.

## Alternatives considered

- **Hashing only the derived features** (decision 1). Cheaper, but a
  diverged level outside the bands stays invisible until price reaches it.
  Rejected.
- **Counting the old-book → checkpoint difference as flow** (decision 7).
  Contradicts accepted ADR-038 D6 for at most about 0.17 % of the time.
  Rejected.
- **ATR-scaled or level-count bands** (decision 2). Rejected for the
  reasons given there.
- **The gross and the net-only attribution rules** (decision 5). Rejected
  for the reasons given there.
- **A cluster threshold as a multiple of the median** (decision 8). It
  would pre-empt #23's measured scoring. Rejected for `@1`.

## Accept when

Both are reachable on this host.

1. The measurement tool (`crates/mie-cli/tests/book_liquidity_measure.rs`,
   ignored in CI) runs over the #10 soak's raw store and journal (13.7 h,
   two runs) and its table is recorded here:
   - the 5 bps band is ready after at least 90 % of book events. M2 bounds
     the minutes that lose it at 10–18 %, and a time-weighted loss is lower.
     Below 90 % the outer band is lost too often to research, and a `@2`
     narrows the bands;
   - `inferred` stays below 50 % of `filled` per side. Above that, more
     fills are guessed than matched, the clock-alignment premise of
     decision 5 fails, and a `@2` revises the rule;
   - the 5th candidate's median multiple stays above the side's median p90
     level multiple; otherwise a `@2` lowers K.
2. #13's acceptance run (ADR-041 *Accept when*: its coverage list includes
   a book desync and resync), on a binary with this feature set, reports
   every run `EQUIVALENT` with the state, and with it the book, compared.

References: ADR-012, ADR-013, ADR-019, ADR-021, ADR-022, ADR-023, ADR-024,
ADR-027, ADR-028, ADR-029, ADR-031, ADR-034, ADR-035, ADR-038, ADR-041,
ADR-042.
