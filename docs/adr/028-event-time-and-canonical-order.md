# ADR-028: Exchange event time and the canonical multi-stream order

- Status: accepted; the live OpenInterest ordering time (table row
  OpenInterest, "live: the response timestamp") is superseded by ADR-032 D12
- Date: 2026-10-06

## Context

The core consumes one sequence of `MarketEvent`s merged from several Binance
USDⓈ-M streams (trades, depth, liquidations, mark price, funding, open
interest, klines). ADR-019 requires live processing and replay to produce
identical results, so the merged order must be the same however the events
were obtained.

- Binance messages carry an event (push) time `E` and, for executions and
  book diffs, a transaction or trade time `T`. Trades carry aggregate trade
  ids. Depth diffs carry `U` / `u` / `pu`: first, last and previous final
  update id, which chain the diffs of one book.
- There is no sequence across streams. Each stream is ordered within itself
  only.
- Timestamps have millisecond resolution. At BTCUSDT rates, events of
  several streams regularly share a millisecond.
- The public archive (historical import, #12) carries exchange fields only —
  no receive timestamps — and archive trades carry `T`, not `E`.
- Live arrival order depends on the network and the connection layout. It
  differs between two captures of the same market.

## Decision

1. **Ordering time is exchange time only**, chosen per kind (table below).
   Local receive time never orders domain events. It is capture metadata for
   latency research (#8).
2. **Canonical order**: the total order `(ordering time, kind rank,
   per-stream sequence id)`, then the payload's structural order as the final
   tie-break. It is implemented once in `mie-domain` as `Ord for MarketEvent`
   (`mie_domain::order`), with `CanonicalKey` as the exchange-derived prefix.
3. **Same-millisecond rank**: FeedGap < BookSnapshot < Trade < Liquidation <
   BookUpdate < MarkPrice < FundingSettlement < OpenInterest < Kline. The
   rule: continuity loss first, then resets, then executions, then the book
   changes they cause, then periodic observations, then aggregates. A
   snapshot ranks before an update, so an update straddling the snapshot's
   last update id is applied after the reset.
4. **Feed gaps are events**. `FeedGap { stream, start, end, reason }` means
   the stream may be incomplete over the closed interval `[start, end]`, with
   `start <= end`. It is ordered at `end` with rank 0, so it precedes the
   stream's first resumed event at that millisecond.
5. **Enforcement**: every `MarketDataProvider` delivers a strictly
   increasing sequence in this order. Providers sort or merge with the full
   comparator, not with the key alone, and dedupe by exchange id: an
   id-carrying kind (Trade, BookSnapshot, BookUpdate) is a duplicate when its
   id was already delivered, whatever the payload; an id-less kind only when
   it is an exact repeat. `MarketStateEngine` checks both, as defense in
   depth:
   - an event below the last one is `OutOfOrder`, an event equal to it is
     `Duplicate`;
   - trade ids strictly increase. A repeated id is `Duplicate`, a lower one
     `IdRegression`;
   - on the order book, snapshot ids strictly increase among snapshots and
     update ids (`u`) among updates, with the same two errors. Across the two
     kinds the id may stay equal but never fall (`IdRegression`): an update
     whose `u` equals the snapshot's last update id is the first diff the
     exchange allows after it, and a snapshot may restate the book exactly
     at the last applied update.
6. **Late live events are never delivered into the past.** The live adapter
   reorders within a bounded hold-back (its size is chosen with the live
   adapters, #9). An event that arrives after its slot was released becomes
   `FeedGap(LateEvent)` on its stream. The gap must sort strictly after the
   last released event, so its `end` is a later millisecond than that
   event's ordering time: a gap ranks first in its millisecond, so a gap
   ending in the same millisecond would sort before the released event and
   be rejected as `OutOfOrder`.

| Kind (rank) | Ordering time | Sequence id | Serves |
|---|---|---|---|
| FeedGap (0) | `end` | ordinal of the affected `Stream` | ADR-026 consequences (feed gaps as data) |
| BookSnapshot (1) | snapshot transaction time | last update id | Data Plane order-flow contract (depth events); ADR-021, ADR-022 |
| Trade (2) | trade time `T` | (aggregate) trade id | order-flow contract (trades, aggressor side); ADR-021, ADR-022 |
| Liquidation (3) | trade time of the liquidation order | 0 | brief §7 raw liquidations |
| BookUpdate (4) | diff transaction time | last update id `u` | order-flow contract (bid/ask liquidity changes); ADR-021, ADR-022 |
| MarkPrice (5) | exchange event time | 0 | brief §7 funding, §8 Market State |
| FundingSettlement (6) | funding time | 0 | brief §7 funding |
| OpenInterest (7) | exchange publication time — live: the response timestamp; archive: a rule fixed with the import (#12), never a validity time earlier than publication | 0 | brief §8 OI, ΔOI |
| Kline (8) | close time | 0 | brief §7 klines (cross-check only) |

`Stream` ordinals: Trades 0, OrderBook 1, Liquidations 2, MarkPrice 3,
Funding 4, OpenInterest 5, Klines 6.

Supporting rules fixed with this order: klines enter the domain as closed
bars only, for cross-checks, and no feature derives OHLCV from them; the
indicative funding rate rides on `MarkPrice`, settled rates are
`FundingSettlement` events.

## Consequences

- The delivered sequence is a pure function of the event set. Live capture,
  archive import and every correct merge (stable sort, unstable sort, k-way
  heap merge) produce the same sequence, and the equivalence harness (#13)
  can compare runs event by event.
- The rank and the stream ordinals are pinned by tests. Changing either, or
  any other part of this decision, needs a superseding ADR and invalidates
  recorded equivalence hashes.
- Intra-millisecond causality across streams is a convention, not a fact:
  a trade and the book update it caused may be reported in the same
  millisecond by different streams, and the rank — not the exchange — puts
  the trade first.
- Live processing pays the hold-back latency, and a late event is lost to a
  gap instead of corrupting the past.
- A gap is announced when its stream resumes, so an ongoing outage is silent
  for that stream. Features must check staleness in event time rather than
  wait for a gap.
- The field declaration order of the payload structs is part of the final
  tie-break. Reordering fields changes the canonical order and is a change to
  this decision.
- The strict engine rejects duplicates loudly — exact repeats, and repeated
  or regressing exchange ids even with a different payload; providers dedupe
  by id before that.
  Distinct id-less events in one millisecond (two liquidations, say) remain
  valid and are ordered by payload.
- A gap that is still open at the end of a replay window (a trailing gap) is
  handled by the replay merge (#11).

## Alternatives considered

- **Recorded arrival order** — reproduces one live session exactly, but the
  archive has no receive times, and two captures of the same market differ.
  Kept as capture metadata only.
- **Exchange push time `E` for every stream** — uniform, but archive trades
  carry only `T`, so live and archive would order trades differently.
- **Per-stream concatenation** (all trades, then all depth, …) — trivially
  deterministic, but breaks event time: the state would see the future of
  one stream before the past of another.
- **A gap ordered at `start`** — announces the outage at its beginning, but
  the gap is known only at resumption, so the live hold-back would have to
  cover the whole gap.
