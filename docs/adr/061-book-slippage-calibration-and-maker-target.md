# ADR-061: Book slippage, live-book calibration and the maker target

- Status: proposed
- Date: 2026-11-05

## Context

#172 asks for three things that ADR-060 left out of scope (ADR-060 *Out of
scope*: "Spread and depth-walk slippage, maker legs and slippage
calibration"):

- **Book slippage.** `slippage.spread@1` and `slippage.depth_walk@1` on
  `book.l2@1`. The book exists only live: the archive has no L2 diffs
  (ADR-043 D11), `bookTicker` is not imported and the raw `bookDepth`
  bands are too coarse (`docs/data-availability.md`).
- **Calibration.** `docs/direction.md` (*Net outcome*) measures slippage
  "from the reconstructed order book where live depth exists" and, for the
  archive period, "from a slippage model calibrated on the live book and
  recorded with the experiment". ADR-060 runs every archive backtest on
  `slippage.fixed@1` with a rate nobody has measured (#30 decision 6).
- **The maker leg.** #30 decision 5 put taker fees on both legs and asked
  for the maker effect as a sensitivity.

The plan and its STOP 1 resolution are in
https://github.com/Replikanti/mie/issues/172#issuecomment-6645059441. Its
decisions 1–5 are inputs here: the calibration averages over every
recorded trade, both sides, on the soak captures (1); the position size is
a public grid read as an upper bound (2); the maker leg is a bracket of two
rules with no live calibration (3); book slippage ships in the backtest
now, on the history chain only (4); the calibrated statistic is the mean
(5).

What exists:

- The domain `OrderBook` (ADR-038 D6) with its trusted window: the book is
  fully known only between the snapshot's deepest bid and deepest ask;
  beyond it only changed levels are known, never their absence.
- `book.l2@1` in the Market State (ADR-043 D1): `Ready` while the book is
  valid, warming up before the first snapshot and unavailable after an
  invalidation. A trade never changes it (ADR-043 D1, D5).
- The canonical order ranks a trade before the book updates of its
  millisecond (ADR-028, cited in ADR-043 *Context*).
- The backtest of ADR-060 on the history chain only (D13), with a live
  segment after the cutover (ADR-052) that does not exist yet: #106's
  production cutover has not happened.

**Live data available** (from the capture journals, checked 2026-11-05):
14 runs with depth in 8 journals, 110.05 h between 2026-10-08 22:11 and
2026-10-20 20:15 UTC, 4 903 679 `aggTrade` events and about 9.1 M events in
total, every run ended with exit code 0. No capture runs now. The #99 and
#22 runs resynced often (up to 203 syncs in one 8.25 h run), the #99
sessions cover US hours only (30.5 of the 110 h), and the stores are
separate, with multi-day holes between them.

Delivery: this ADR stays proposed. #172 PR 1 implements D1–D6 and the
ADR-060 cross-references, PR 2 the calibration pipeline (D7–D9), and PR 3
records the *Acceptance* section and flips the status.

## Decision

### D1. The fill book

The fill book is `book.l2@1` in the engine state when the fill trade is
observed.

- *Why:* a trade ranks before the book updates of its millisecond
  (ADR-028), and a trade never changes `OrderBook` (ADR-043 D1, D5). So
  this is the book a marketable order meets when that print happens.
- *Rejected:* the next book update, up to one 100 ms batch later
  (ADR-038 D3), which already holds unrelated changes.

### D2. Reachable levels and the price of a fill

- A buy (long entry, short exit) takes asks and a sell (short entry, long
  exit) takes bids. It walks only the levels at or beyond the print price
  e: asks ≥ e, bids ≤ e. The walk stays inside the trusted window
  (ADR-038 D6: beyond the window, absence is unknown) and goes best level
  first.
- `spread`: E′ = the first reachable level's price.
- `depth_walk` at size q: E′ = ⟨Σ pᵢ·tᵢ / q⟩, with tᵢ = min(remaining,
  qtyᵢ), computed exactly in i128 and rounded half to even once to 1e-8
  USDT (ADR-027). The sum cannot overflow: |Σ pᵢ·tᵢ| ≤ max |pᵢ| · q < 2¹²⁶.
- **Unpriced** has two causes:
  - `book`: the book is not `Ready`, is one-sided or is crossed (best bid
    ≥ best ask), the invalid books of ADR-043 D3;
  - `window`: no level is reachable inside the window, or the window holds
    less than q.
- *Why at or beyond e:* liquidity better than e was taken by the order
  that printed e (in a same-millisecond sweep the pre-trade book still
  shows it), so a later order cannot get it. This also keeps every
  slippage ≥ 0.
- *Rejected:*
  - the whole pre-trade book, which prices below the print in sweeps
    (negative slippage);
  - mid ± half-spread, which ignores which side the print hit.
- *Weak spot:* the level at e still holds the prints since the last book
  update. That is at most one 100 ms batch (ADR-038 D3, ADR-043 D5), and
  it is optimistic only when q is near that level's size.

Code: `OrderBook::walk` (`crates/mie-domain/src/book.rs`, a read-only
helper as ADR-043 D1 allows) and `slippage::price_fill`
(`crates/mie-domain/src/slippage.rs`), shared by the backtest and the
calibration.

### D3. New rows in the rule registry

Three rows join `REGISTRY_V1`, each after the rows of its slot. No
existing row changes, and a spec that resolved before resolves to the same
plan.

| Slot | Rule | Parameters | Valid range | Effect |
|---|---|---|---|---|
| slippage | `slippage.spread@1` | `book=feature:<book.l2@N>` | a `book.l2` member of the spec's set with a reader | D2's spread price at each taker fill |
| slippage | `slippage.depth_walk@1` | `book=feature:<book.l2@N>`, `qty=qty:q` | as above; q > 0 | D2's walk of size q at each taker fill |
| target | `target.r_multiple_maker@1` | `fill=text:through\|touch`, `max_hold_ms=int:n`, `r=rate:k` | these two; n and k as `target.r_multiple@1` | D6 |

- *Why an explicit `book=` parameter:* the text then pins the book's
  version, ADR-060 D2's reason for `atr=`. A feature of another family is
  its own resolution error (`WrongFamily`), and a book feature registered
  later is refused until it gets a reader.
- *Why no cap on q:* the range is the domain of definition, as in ADR-060
  D2. An exchange limit would be a plausibility cap tied to one venue and
  one date.
- Every ADR-060 D2 resolution error applies to the new rows unchanged:
  unknown, missing, wrong type, out of range, wrong family, and `latency`
  ≥ `max_hold_ms` on the maker target's line. The hint of an unknown rule
  lists every rule of its slot, so it now names the new rows too.

### D4. The backtest under a book rule

- E′ (entry) and X′ (stop, time and taker-target exits) are D2's price at
  the fill trade, in place of ⟨e(1 + σs)⟩. Fees stay ⟨taker · fill⟩ on
  every taker leg.
- A fill that D2 cannot price censors the trade outright with the new
  token `no_book`, at c = the fill trade's time. This is the ADR-060 D9
  class of `funding_gap` and `overflow`: a value on the line is unknown,
  the path is not. At the entry the line keeps no entry (its fill trade is
  not strictly before c); at the exit it keeps the entry and E′.
- *Weak spot:* unlike `funding_gap` and `overflow`, `no_book` is tied to
  the cost being measured. Under `slippage.depth_walk@1` the fills it
  drops are the thin or fast moments, where the window holds less than q
  or the print has drifted past the window bound since the last re-anchor
  (ADR-043 M2). These are the expensive fills, so a depth-walk backtest's
  net R leans optimistic as q grows, as D8 says for the calibration.
- Before any drive, the run is refused with
  `ResearchError::BookNotDelivered` when the sample ends at or before the
  chain's cutover. The archive has no book (ADR-043 D11), so every fill
  would be censored. ADR-060 D13's order becomes: the feature set,
  resolution, funding coverage, **book coverage**, a sample before the
  origin, then the drive.
- ADR-060 D7 is unchanged, so on the live segment a book-rule spec can
  only use `funding.none@1`, with the bias of ADR-060 D16 stated.
- The driver hands the simulator the plan's book (`Plan::book`, the
  reader of the `book=` feature) after each accepted event, and an unready
  book with the trailing gaps, which price no trade.

### D5. The trade line

- Under a book rule only, every `trade` line gets two fields after
  `censor=`: `entry_slipped=<price|->` and `exit_slipped=<price|->`. Their
  `-` cases are those of `entry` and `fill`.
- *Why:* E′ and X′ then depend on the book and can no longer be derived
  from e, x and the spec (ADR-060 D12's re-derivation reason).
- Under `slippage.fixed@1` every line stays byte-identical, so no existing
  result changes. This follows, by analogy, the spec-text extension rule
  that a new form is rendered only where it differs from the default.
- The censor-token list gains `no_book` (seven tokens).
- Strict parsing and `ExperimentResult::validate` tie the fields to the
  embedded spec: a `backtest` outcome whose spec's `slippage` rule is a
  book row carries the fields on every line, any other carries none. Each
  mismatch is a malformed line, reported on the first such trade line.
- *Rejected:*
  - always rendering the fields, which changes the lines of an existing
    kind (ADR-040 D6);
  - a new outcome kind, which duplicates `backtest` for two fields.

### D6. The maker target

- The target is a resting limit order placed with the entry. On trades
  strictly after the entry fill trade and before the deadline:
  - `through` fills it at the first trade σ-strictly beyond the target;
  - `touch` fills it at the first trade at or beyond the target.
- It fills at the target price T, with no slippage and a fee of ⟨maker ·
  T⟩. The line shows `exit=target`, `exit_at` = that trade's time, and
  `fill` = that trade's time and print price. Gross R = σ(T − e) / R.
  Under a book rule `exit_slipped` is T.
- `exit_at` is the fill trade itself, so ADR-060 D9's stand and finality
  rules read it as they read a touch: the exit stands when `exit_at` is
  before c and is final once a trades-stream event in a later
  millisecond is observed. A maker fill at the end of the data is
  therefore censored `end_of_data`.
- A target already at or behind the entry fill price is marketable when
  placed, so it exits as ADR-060 D6's taker touch.
- Stop and time exits stay ADR-060 D6 (taker), and a stop touch ends the
  monitoring as before.
- *Why `through` needs no number:* under price-time priority, no trade can
  print beyond a resting limit while it rests. A print one tick (0.10
  USDT) beyond the target proves the fill, whatever the queue position.
- `touch` is the optimistic bound. `through` fills are a subset of `touch`
  fills, so the pair brackets the maker effect on archive data without a
  book.
- *Why no queue rule:* a queue rule needs the queue at placement. The 1h
  median barrier half-width is 19 bps (`docs/direction.md`), but the
  trusted window reaches a median of 13 bps (p10 10 bps) on the narrower
  side (ADR-043 M1). So for 1h and longer horizons, the target level is
  usually outside the known book when the order is placed.
- *Rejected:* a calibrated trade-through distance, which would be a fitted
  number that price priority makes unnecessary.

### D7. Pipeline `research.slippage_calibration@1`, outcome kind `slippage-calibration`

A new `PIPELINE_KINDS` entry (#172 PR 2).

- **Source:** `--source live` only.
  - *Why:* the book exists only live, and each priced value depends on the
    book since the run's last snapshot (a reset, ADR-038 D6) and on the
    print. So ADR-060 D13's warm-state skew does not arise.
  - `archive` (no book) and `history` (no live segment today) are refused
    before the store opens.
- **The spec:** the pipeline reads `sample`, `data` and `features`, and
  `features` must contain `book.l2@1` (`ResearchError::BookNotInSet`
  before any drive). It reads no rule line.
- **Observation:** every accepted trade gives two hypothetical fills, a
  buy and a sell, each priced by D2.
  - Per fill: s = ⟨σ_b(E′ − e) · 10⁸ / e⟩ in `Rate` units, with σ_b = +1
    for a buy and −1 for a sell; s ≥ 0 by D2 (`slippage::slippage_rate`).
  - Lines: `trades <n>`, then one line for `spread` and one for each grid
    size (D8), each
    `fills=<n> book=<n> window=<n> sum=<rate> mean=<rate|->`, with mean =
    ⟨sum / fills⟩.
  - Conservation: fills + book + window = 2 × trades.
  - *Settlement:* a fill counts as priced only once the next book event
    proves the book it met. A `BookUpdate` after which the engine's book
    is still `Ready` does: the sequencer emits one only while the diffs
    chain, and the domain book stays ready only while they chain to its
    own (when the domain rejects a book event, the next diff breaks that
    chain and invalidates the book). Any other update, a `BookSnapshot`,
    an `OrderBook` gap or the sample's end counts the fills held since
    the last book event `book`. *Why:* a book that stops receiving diffs (a depth reconnect,
    a run restart) stays `Ready` with its old levels, and its gap is
    announced only at the resync (ADR-038 rule 9), so a fill before the
    resync would be priced on a stale book (PR #181 review: s ≈ 1 % on a
    1 % move). The cost: the fills of at most one diff interval before
    each checkpoint snapshot also count `book`.
- **Provenance:** the `stream` and `rejections` lines of
  `replay-summary`. The key has no engine part (like
  `research.replay_summary@1`): the values depend only on the data and on
  `book.l2@1`, which the spec's set pins.
- *Weak spot:* two calibration specs that differ only in rule lines the
  pipeline does not read get different ids with identical outcome lines.
  Mitigation: *Accept when* pins the spec template.

### D8. The size grid

A 1-2-5 series 0.001, 0.002, 0.005 … 50, 100, plus 120 BTC (17 sizes).

- *Reading rule:* per fill, the cost does not decrease as q grows. So the
  row at the smallest grid size ≥ the position size is an upper bound on
  the cost at that size. The position size itself never appears in public
  content (`docs/direction.md` keeps capital and risk per trade out of the
  repository).
- A row with `window` > 0 is also a lower bound for its own size, because
  the walks that left the window are the expensive ones. The *Acceptance*
  table marks such rows.

### D9. The calibrated rate

s*(q) = ⟨Σ sum / Σ fills⟩ over the results of every store, pooled exactly.

- Archive backtests then use `slippage.fixed@1 rate=rate:s*(q′)` and cite
  this ADR's *Acceptance*. This is the "calibrated on the live book and
  recorded with the experiment" of `docs/direction.md` *Net outcome*.
- *Why the mean:* net expectancy (T2) is a mean, and with E′ = e(1 + σs)
  slippage enters net R linearly per fill.
- *Rejected:*
  - the median, which understates a right-skewed cost;
  - a high quantile, a safety margin with no source (T2's lower bound
    already carries the uncertainty).
- *Weak spot:* the population is every trade, not the hypothesis's fill
  moments (STOP 1 decision 1). The mean is also not robust to rare deep
  walks.

### Where each number comes from

| Number | Source | What breaks at another value |
|---|---|---|
| Grid start 0.001 BTC | Binance BTCUSDT `LOT_SIZE` and `MARKET_LOT_SIZE` `minQty` (`GET /fapi/v1/exchangeInfo`, read 2026-11-05) | smaller: no such order exists; larger: no row bounds the smallest orders |
| Grid end 120 BTC | `MARKET_LOT_SIZE` `maxQty`, same read (`LOT_SIZE` max 1000 bounds limit orders; a market order is also bound by `MARKET_LOT_SIZE`) | larger: no market order of that size exists; smaller: sizes up to 120 have no bounding row |
| 1-2-5 spacing, 17 sizes | D8's bound: the next row up overstates the size by at most 2.5×; decades would by 10× | coarser: a looser bound; finer: more lines from the same pass, no other change |
| 8 decimals for E′, s, sum, mean | ADR-027 units, the result text v1 form | fewer: a rounding a hand re-derivation must also reproduce |
| 2 hypothetical fills per trade | the fixed rate is used for both legs of both sides (ADR-060 D6) | one side only: that side's asymmetry would leak into every leg |
| q > 0 | the domain of definition (ADR-060 D2's rule) | q ≤ 0: no walk |
| 13 bps median / 10 bps p10 trusted window; 4.7 / 8.9 / 19 / 39 / 109 bps barrier half-widths | ADR-043 M1; `docs/direction.md` (the #12 year) | cited facts behind D6, not parameters |
| 0.10 USDT tick | `PRICE_FILTER` `tickSize`, same read | cited only: "strictly beyond" means at least one tick on this venue; the rule has no number |
| ≤ one 100 ms batch of prints in the level at e | ADR-038 D3, ADR-043 D5 | the bound of D2's weak spot |
| 14 runs, 110.05 h, 4 903 679 `aggTrade` events, 2026-10-08 22:11 … 10-20 20:15 UTC | the capture journals, checked 2026-11-05 | cited inventory |
| 8 GB memory cap per run | ADR-054's live measurement (#147): 6 248 084 kB peak over the #22 rerun's 2 042 225 events (≈ 3.06 kB/event); the #22 run's 2 212 028 events give ≈ 6.8 GB | lower: the largest store aborts; `LiveReplay` memory grows with run length (#140) |
| ≈ 6 min per pass over all stores | the same run's 1:13.90 for 2.04 M events, scaled to ≈ 9.1 M events | a forecast, not a criterion |

Every other figure in the text is an identifier (an ADR, issue or id), a
constant that follows from a row above (2¹²⁶ from the i64 ranges of price
and quantity, 10⁸ from the 8 decimals), or a 0 or 1 that a rule defines.

### Out of scope

- A book-based queue rule for the maker target (`fill=book`) and any
  trade-through distance other than "strictly beyond" (D6).
- Calibration on `--source history`, and a calibration conditional on
  backtest fill moments. Both need the chain's live segment (#106) and are
  a follow-up.
- Running any backtest with the new rules on real data, and reading any
  exit kind or metric. A maker-bracket or book-slippage backtest is a Lab
  result, so `docs/direction.md` rule 1 sends it to #33.
- Maker entries or stops, scale-outs.
- Funding on live data (ADR-060 D16).
- Any change to `book.*` features, the catalog or lock, state hashes or
  live/replay equivalence. No feature changes, so no golden hash moves.
- Edits to `docs/direction.md` inputs.

## Consequences

- **ADR-060 is extended, not changed:** its D2 table points to D3's rows,
  its D6 `maker` is used by `target.r_multiple_maker@1` only, and its D12
  token list and `trade` line gain `no_book` and the two fields that
  appear only under a book rule. No existing value, line or *Accept when*
  item changes, and #171 is unaffected because it uses fixed slippage. If
  ADR-060 is accepted before these cross-references land, ADR-060 gets
  only a status-line note ("accepted; D2, D6 and D12 extended by
  ADR-061"), the ADR-038 header precedent, and this ADR states the
  extensions itself.
- **Result-text determinism:** every existing `backtest` result renders
  byte-identically (the ADR-060 D12 golden is pinned unchanged); a new
  golden pins the book fields and `no_book`.
- **Book slippage has no real-data run until #106:** the backtest takes
  the book rules now, fixture-tested, but the chain has no live segment,
  so every book-rule run today is refused `BookNotDelivered`. Every such
  run later omits funding (ADR-060 D7, D16).
- **The calibration has a population bias** (D9): every trade, not the
  hypothesis's fill moments; busy periods and US hours are over-weighted.
- **The maker bracket may be wide** (D6), and where the truth lies inside
  it stays unmeasured here.
- **The leakage guard** (ADR-060 D15) is unchanged: `slippage.rs` is a new
  core file outside the allow-list that names no guarded identifier.

## Alternatives considered

- **Calibrating with backtest runs on the history chain.** ADR-060 D13
  limits the backtest to the chain, and the chain has no live segment yet,
  so that calibration has no data today (*Context*).
- **The next book update as the fill book.** It already holds unrelated
  changes (D1).
- **The whole pre-trade book, or mid ± half-spread.** The first prices
  below the print in sweeps; the second ignores which side the print hit
  (D2).
- **Rendering the book fields on every line, or a new outcome kind.** The
  first changes the lines of an existing kind; the second duplicates
  `backtest` for two fields (D5).
- **A calibrated trade-through distance or a queue rule for the maker
  target.** The first is a fitted number price priority makes unnecessary;
  the second needs a queue the window mostly does not hold (D6).
- **A single private size in the spec.** It would put the position size
  into public content (D8, STOP 1 decision 2).
- **The median or a high quantile as the calibrated rate.** The first
  understates a right-skewed cost; the second is a margin without a source
  (D9).

## Accept when

#172 PR 3, run by the main session.

1. **Runs:** one calibration result per depth journal (the 8 of
   *Context*). Each uses this spec template: ADR-060 *Accept when* item 1
   with `sample` and `data` from `mie replay --source live` for that
   store's window, `features` = that closure plus `book.l2@1`,
   `slippage slippage.spread@1 book=feature:book.l2@1` and
   `funding funding.none@1`. A journal that `--source live` refuses (for
   example a #106 rehearsal run with a history configuration) is excluded
   and the reason recorded; no store is selected by its values.
2. **Reproduction:** each run prints `recorded`, then `reproduced` on a
   second run, and the result file's SHA-256 is unchanged.
3. **Conservation:** holds on every line of every result.
4. **Pooled table:** s*(q) for `spread` and all 17 sizes, with Σfills,
   Σbook and Σwindow, and the lower-bound mark per D8, recorded in
   *Acceptance*.
5. **Caps:** 8 GB memory and 400 % CPU, `setsid`, blocking foreground
   polling.
6. **Partitioning:** splitting a store into windows is allowed only after a
   memory abort. Sums pool exactly, apart from the trades before the first
   snapshot of each split, which become `book`.

Anything else fails: a `FAIL: …` line, a `Diverged` result, a broken
conservation line or a changed file.
