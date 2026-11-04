# ADR-060: Backtest semantics on spec text v1

- Status: proposed
- Date: 2026-11-03

## Context

#30 asks for the backtest: an experiment spec goes in, and a result with
net outcomes in R comes out through the ADR-040 store, reproducible and
with its provenance. The Strategy Research Lab brief names the inputs
(trigger, location, entry, invalidation, target, fees, slippage, funding,
latency, sample period, data and feature versions), and its acceptance
asks for reproduction and for a check against an independent value.

ADR-040 left two things open for this issue:

- **Rule ids.** ADR-040 D3 validates the rule references of the
  `location`, `trigger`, `entry`, `invalidation`, `target`, `slippage` and
  `funding` slots for syntax and feature membership only. No rule id is
  resolved, so no spec has meaning yet.
- **Spec text v1 or v2.** ADR-040 *Consequences*: #30 either records its
  results through the store with spec text v1, or lands the spec-text-v2
  ADR first.

#30 was decomposed into S1 (this ADR, #166), S2–S5 (#167–#170, the
simulator, the outcome text and metrics, the rule registry, the pipeline)
and S6 (#171, the acceptance run). The decomposition and its STOP 1
resolution are in
https://github.com/Replikanti/mie/issues/30#issuecomment-6592036297
(§5–§6). Its decisions 1–7 are inputs here: the decision is taken on spec
text v1 (1), R is planned risk (2), one open position per spec (3), the
S6 trigger family (4), taker fees on both legs (5), the S6 values with
nothing calibrated (6), and the history chain as the only source (7).

Delivery: this ADR stays proposed. S2–S5 may refine it in place (README
lifecycle), and each refinement is named in the PR that makes it. S6
records the *Acceptance* section and flips the status.

## Decision

### Approach

One ADR settles every semantic question and number that S2–S5 implement
and S6 accepts, on spec text v1. The decisions below are D1–D17.

*Rejected: a spec-text-v2 ADR first.* The canonical text is the identity
(ADR-040 D2), so v2 would orphan every stored result, and it would fill
no slot the backtest lacks (D1). *Rejected: one ADR per sub-issue.* S2–S5
share one decision model, and S6's independent check has to be
pre-registered in a single text before any backtest result exists.

### D1. Spec text v1 suffices (the answer to ADR-040 *Consequences*)

- #30 records through the ADR-040 store with spec text v1. The backtest
  uses only the extension paths ADR-040 allows: registries for its opaque
  rule ids (ADR-040 D3) and a new outcome kind (ADR-040 D6). Every #30
  input already has a v1 slot (#30 plan §1 table).
- Three weak spots of v1 are recorded here:
  1. *Exactly-once slots.* `entry`, `invalidation`, `target` and `trigger`
     may appear once each (`crates/mie-domain/src/research/spec.rs`).
     Scale-outs, trailing stages and "trigger + N confirmations" (#31) fit
     only as composite rule ids or as a new repeatable line kind. Changing
     the cardinality of an existing key is v2, and the first consumer that
     needs it writes the v2 ADR.
  2. *Closed `fees` line.* It accepts exactly two names, `maker` and
     `taker`, so a third fee concept needs a new line kind or v2.
  3. *Resolution after parsing.* A spec can pass `validate` and still not
     run, and a misspelled parameter would give a new id with the same
     behaviour. Mitigation: the strict resolver (D2) and a second
     `validate` stage (D13).
- The ADR-040 golden spec (the `crates/mie-domain/src/research.rs` module
  docs, id `3f902847001e1998`) stays a parse and identity golden. It does
  **not** resolve under registry v1: it uses `location.at_level@1`, uses
  `trigger.failed_auction@1` as a rule id, uses `target.level@1` (#31),
  and lacks the `side`, `atr` and `buffer` parameters. S4 pins that as a
  resolution-error test.

### D2. Rule registry v1

Every parameter is required and none has a default, so every value a run
uses is in the canonical text and the id names it.

| Slot | Rule | Parameters | Valid range | Effect |
|---|---|---|---|---|
| trigger | `trigger.fires@1` | `feature=feature:<trigger.*@N>` | a `trigger.` member of the spec's set | a decision at each emission of that id |
| location | `location.any@1` | none | — | admits every decision |
| entry | `entry.next_trade@1` | `side=text:with\|against` | these two | D3, D6 |
| invalidation | `invalidation.atr@1` | `atr=feature:<volatility.atr.*@N>`, `multiple=rate:m` | m > 0 | stop = reference ∓ ⟨m·ATR⟩ |
| invalidation | `invalidation.beyond_extreme@1` | `atr=feature:…`, `buffer=rate:b` | b ≥ 0; only ids with an extreme (below) | stop = extreme ∓ ⟨b·ATR⟩ |
| target | `target.r_multiple@1` | `r=rate:k`, `max_hold_ms=int:n` | k > 0; 1 ≤ n ≤ sample end − start; `latency` < n | target = reference ± ⟨k·R⟩; time stop at decision + n |
| slippage | `slippage.fixed@1` | `rate=rate:s` | 0 ≤ s < 1 | D6 |
| funding | `funding.recorded@1`, `funding.none@1` | none | — | D7 |
| state-filter | `none` only | — | — | state-filter rules belong to #31 |
| regime-filter | unchanged (ADR-040 D1) | — | — | an unready label fails every line |

⟨x⟩ is x rounded half to even, once, to 1e-8 USDT. ∓ and ± are taken with
the side's sign σ (long +1, short −1).

- *Why `trigger.fires@1 feature=…`:* one registry entry covers every
  trigger id, current and future, and the spec's `features` set pins the
  trigger's version (ADR-053 *Approach*). *Rejected:* the trigger id as
  the rule id, the golden spec's form, which needs one registry entry per
  trigger id and version.
- *Why `location.any@1` only:* every trigger is already location-gated
  (ADR-053 D4). A finer location filter refines a hypothesis, which is
  #31's job.
- *Why an explicit `atr` feature parameter:* the text then pins the ATR
  version, as it pins the trigger's. *Why ATR multiples, not bps:*
  ADR-055 D4's argument; a bps distance would restate the regime.
- *The ranges are the domain of definition, not plausibility caps.*
  - m = 0 gives R = 0.
  - b < 0 puts the stop inside the extreme, which is a different rule.
  - k ≤ 0 puts the target on the stop's side.
  - s ≥ 1 gives a fill price ≤ 0.
  - A hold longer than the sample changes no result: such a deadline lies
    past the sample end, and the trade censors at end of data either way
    (D9). The bound also rules out deadline overflow.
  - `latency` ≥ hold can never fill (D6).
  - There is no upper cap on m, b or k: a cap would be a number without a
    source.
- *Which ids have an extreme.* `beyond_extreme` is allowed for
  `trigger.sfp.*`, `trigger.cvd_divergence.*`, `trigger.exhaustion@1` and
  `trigger.oi_expansion_rejection.<15m|1h|4h|1d>@1`, the ids with an
  ADR-055 D8 extreme. It is refused for the three auction ids
  (`failed_auction`, `failed_reclaim`, `breakout_acceptance`),
  `effort_result.*`, `absorption.*` and
  `oi_expansion_rejection.prior_day@1` (a failed-auction rejection, which
  has no extreme). v1 adds no extreme for those families: a new evidence
  price needs its own source, and `invalidation.atr@1` covers every
  family. The ATR must be ready even when b = 0, so a buffer sweep
  compares the same decisions.
- *Strict resolution.* Each of these is its own error, and all are listed
  in line order (the ADR-040 D1 style): an unknown id, version or
  parameter; a missing parameter; a wrong type; an out-of-range value; a
  feature parameter of the wrong family; `beyond_extreme` on an id
  without an extreme; `latency` ≥ `max_hold_ms`; any `state-filter` rule.

### D3. Side mapping per trigger family

Each family reads its evidence side, *up* or *down*, from the evidence's
own sided fact. `with` maps up to long and down to short; `against` does
the reverse.

| Trigger ids | Evidence (`TriggerEvent.evidence`) | Field | up | down |
|---|---|---|---|---|
| `failed_auction`, `failed_reclaim`, `breakout_acceptance` | `Auction(status)` | `status.side` | `Some(Above)` | `Some(Below)` (`Some(In)` or `None`: skipped `no_side`) |
| `sfp.*` | `Sfp(sweep)` | `sweep.level.side` | `High` | `Low` |
| `cvd_divergence.*` | `Candidate(CvdDivergence(e))` | `e.comparison.pair.side` | `High` | `Low` |
| `effort_result.*` | `Candidate(EffortVsResult(e))` | `e.value.aggressor` | `Buy` | `Sell` |
| `exhaustion` | `Candidate(Exhaustion(e))` | `e.value.expansion.aggressor` | `Buy` | `Sell` |
| `absorption.*` | `Candidate(Absorption(e))` | `e.value.effort.aggressor` | `Buy` | `Sell` |
| `oi_expansion_rejection.*` | `OiExpansion(x)` | as for `x.rejection` (the `Sfp` or `Auction` row) | | |

- *Why relative to the evidence:* trigger facts carry no side (ADR-053
  D1), but every piece of evidence carries a sided fact (ADR-053 D1's
  list: `structure::Side`, `location::Region`, `event::Aggressor`). An
  absolute long/short would put mirror-image setups, a swept high and a
  swept low, into one spec.
- *Why the expansion's aggressor for exhaustion:* the exhaustion extreme
  is the highest high after net buying or the lowest low after net selling
  (`Exhaustion::extreme`, `crates/mie-domain/src/candidate/effort.rs`).
- Direction belongs to the hypothesis (ADR-055 D7). It lives in the spec
  and in `mie_domain::backtest` only. No trigger, candidate, record or
  label type gains a side, so the ADR-053 D1 naming rule stands.
- *Rejected:* absolute `long|short` (above); a hand mapping per family
  ("an SFP of a high is short"), which is an interpretation no accepted
  ADR makes.

### D4. Decision event, reference, filters

- A **decision** is each emission of the rule's trigger id in
  `MarketStateEngine::trigger_events()` after an accepted sample event
  (ADR-053 D2). It reads the state after that event. Prefix events
  (ADR-052 D10) are driven but never evaluated.
- *Order within an event:* the simulator first applies the event to the
  open position (fill, touch, funding, gap), and only then takes that
  event's decisions, in emission order. Why: a decision then can never
  fill on its own event, ADR-055 D2's strictly-after rule, and a position
  that this event's trade closes frees the slot (D8) for this event's
  decisions. *Rejected:* decisions first. A decision would then see the
  position as it was before the event, and a decision on the trade that
  fills the exit would be skipped as `overlap` although no position is
  open after that trade.
- *Reference:* `trade.last_price@1` at the decision, ADR-055 D2's
  reference. A decision is skipped as `stale_reference` when no trade came
  before it, or when a trades gap was delivered after the last trade and
  at or before the decision (ADR-055 D6's condition). *Rejected:* a
  reference from another stream, since the archive imports no mark price
  and no book price as a domain event (ADR-034 D2); and a stale last
  price used anyway, whose path to the decision is unknown (ADR-055 D6).
- *Filters:* `location.any@1` admits every decision. Regime filters apply
  ADR-040 D1 to the label at the decision; an unready label fails because
  it is no listed label. Decisions removed here count as `filtered`, not
  as skipped. *Why:* a filtered decision lies outside the hypothesis,
  while a skipped one lies inside it and could not trade. *Rejected:* one
  count for both, which would hide how much of the hypothesis could not
  trade.
- **Order of the checks.** Each decision goes through these checks in
  this order, and the first one that removes it names its count:
  1. `filtered` (the filters above);
  2. `no_side` (D3);
  3. `stale_reference` (above);
  4. `no_risk` (D5);
  5. `stop_side` (D5);
  6. `overlap` (D8).

  A decision that passes all six opens the position. *Why this order:*
  checks 1–5 read only the spec and the state and prices at the decision,
  and `overlap` alone depends on earlier trades. With `overlap` last,
  every count except `overlap` is a function of the spec and the data,
  whatever the exit path, so a cost or latency sweep moves decisions only
  between `overlap` and `trade` lines. `overlap` then counts exactly the
  decisions that would have traded. Checks 2–5 follow their dependencies:
  the levels need σ and the reference (D5), and `no_risk` comes before
  `stop_side` (D5). σ and the reference do not depend on each other, so
  their order changes no trade, only the split of the counts, and this
  ADR fixes it in the order D3 and D4 define them. *Rejected:* an
  unordered set of reasons, under which two implementations that both
  follow the text render different `skipped` lines; and `overlap` first,
  under which every other count would depend on the exit path.
- *Conservation:* `triggers` = `filtered` + Σ`skipped` + the number of
  `trade` lines (D12).

### D5. Levels and the R unit

- *Risk:* R = |reference − stop| in price units. This is planned risk
  (STOP 1 decision 2), so cost assumptions never change the unit.
- *Validity:* the stop needs σ · (reference − stop) > 0.
  - With `invalidation.atr@1`, an unready ATR or an offset that rounds to
    0 skips the decision as `no_risk` (ADR-055 D4's `Unavailable` causes).
  - Under either invalidation rule, a stop or a target outside the price
    range skips it as `no_risk` (ADR-055 D4: either barrier outside the
    range makes the race `Unavailable`). The levels are fixed at the
    decision, so no overflow comes before the entry fill (D10).
  - With `invalidation.beyond_extreme@1`, an unready ATR skips the
    decision as `no_risk`, checked before `stop_side`, because the stop
    needs ⟨b·ATR⟩ (D2 requires a ready ATR even at b = 0). *Why:* ADR-055
    D4 names an ATR that is not `Ready` as an `Unavailable` cause, and the
    `invalidation.atr@1` case above maps that cause to `no_risk`. One cause
    keeps one token whatever the rule, and D12's `skipped` line keeps its
    five reasons. *Rejected:* a new `atr_unready` token, which would split
    one cause by rule; a resolution error, since readiness depends on the
    data and not on the text.
  - With `invalidation.beyond_extreme@1`, a stop with
    σ · (reference − stop) ≤ 0 skips the decision as `stop_side`,
    equality included, since the stop needs > 0.
- *Offset rule:* the offset is ATR × m, rounded half to even once, the
  rule of `barrier_offset` (`crates/mie-domain/src/conditional/label.rs`).
  At m = `0.31333000` it equals the labels' 1h offset exactly, since
  31 333 000 / 10⁸ = 31 333 / 10⁵ (ADR-056 D4′).
- *All levels are fixed at the decision, from the reference, not from the
  fill.* Latency and slippage then move fills but never levels, so the
  direction of every cost assumption can be proved (S2), and the levels
  equal ADR-056's barriers (the S6 check). *Rejected:* levels from the
  entry fill, which would let R move with every cost assumption.

### D6. Fills, slippage, fees

- **Entry:** the first trade whose canonical key (ADR-028) is after the
  decision event and whose time is ≥ decision time + `latency`. It must
  come before the **deadline**, decision time + `max_hold_ms`; otherwise
  the decision ends `unfilled` and holds the position slot until the
  deadline (D8).
- **Exit monitoring** covers the trades from the entry fill trade
  (inclusive) with a time before the deadline.
  - A **touch** is a trade at or beyond a level (ADR-055 D5): for a long,
    a price ≤ stop or ≥ target; for a short, mirrored. The first touch in
    canonical order decides between stop and target. One trade cannot
    touch both levels, since the stop and the target lie on opposite sides
    of the reference.
  - The exit fills at the **first trade after the touching trade**, with
    no latency: stop and target are resting exchange-side orders placed
    with the entry, and a triggered order fills on the next print.
  - **Time exit:** with no touch before the deadline, the exit fills at
    the first trade with a time ≥ the deadline.
- *Why the fill trade is monitored:* a resting order placed at the fill is
  immediately marketable when the fill is already beyond it. With latency
  0 this also makes the monitored trades exactly ADR-055 D2's window.
- *Why the deadline counts from the decision:* it is the hypothesis's
  horizon, like the label window. Counted from the fill, latency would
  move the horizon too.
- *Latency applies to the entry only:* it is the alert-to-order delay of
  an informational system (ADR-010). Weak spot: the time exit is treated
  as pre-scheduled.
- **Slippage** is adverse: fill = ⟨trade price × (1 + σs)⟩ at entry and
  ⟨trade price × (1 − σs)⟩ at exit.
- **Fees:** taker on both legs (STOP 1 decision 5; the `docs/direction.md`
  fee tier), fee = ⟨taker × slipped fill⟩ per leg. `maker` is parsed and
  unused by registry v1.
- *Rejected:*
  - filling at the touching trade's own price: that print happened before
    any order could react to it;
  - filling at the level price: it ignores gaps through a level and is
    optimistic for stops;
  - latency on every leg: a resting order has no human delay.

### D7. Funding

- `funding.recorded@1` books every `FundingSettlement` ordered after the
  entry fill trade and before the exit fill trade. The cost per unit of
  base is ⟨σ × rate × p⟩, where p is the last trade price before the
  settlement (a settlement ranks after the trades of its millisecond,
  `crates/mie-domain/src/order.rs`). Positive means paid; the sign
  convention is the venue's (a positive rate makes longs pay).
- *Why the last trade price, not the mark price:* the archive imports no
  mark price (ADR-034 D2; `docs/data-availability.md`). The amount then
  errs by the basis as a fraction of itself: by 1 % of the funding amount
  at a 1 % basis.
- **Funding is never zero by omission:**
  - `funding.recorded@1` requires the sample to end at or before the
    chain's cutover, because live delivers no settlement event
    (`docs/data-availability.md`). This is checked before any drive
    (`FundingNotDelivered`, D13).
  - Under `funding.recorded@1`, a `FeedGap` on `Stream::Funding` that
    meets the booking span censors the trade (`funding_gap`; the span,
    c and the trailing funding gaps are in D9).
- `funding.none@1` books no funding, and says so in the text. A funding
  gap then changes nothing, because nothing is booked.

### D8. Overlap

One position per spec (STOP 1 decision 3). It is busy from the decision
until the exit fill, the `unfilled` deadline or the censor. Every decision
in that time that passes checks 1–5 of D4 is skipped as `overlap`. When
one event emits the id more than once, its emissions go through D4's
checks in emission order. The first one that passes all of them opens the
position, and every later one is then an `overlap`. A skipped emission
does not take the slot.

*Why one position:* overlapping trades share one price path, so their
outcomes are not independent. A drawdown over them would count one move
several times, and the trade sequence would not be one equity path per
unit of base (D10). *Rejected:* independent overlapping trades, every
decision its own trade, for that reason. Their outcomes per decision are
what the labels already measure (ADR-055).

### D9. Censoring: ADR-055 D6's rules applied to the position

- **Censor time c**, as for a label opened at the decision, with its
  reason token (D12):
  - a trades `FeedGap` (`gap`): max(gap start, decision time);
  - a rejected trades-stream event (`rejected`): ADR-055 D6's min rule;
  - end of data (`end_of_data`): max(decision time, the last accepted
    trades-stream event's time), the form of ADR-055 D6's end-of-data
    rule;
  - a chain restart (ADR-052 D4; `chain_restart`): the same max, taken
    over the trades-stream events accepted before the restart;
  - a funding gap (`funding_gap`, D7), under `funding.recorded@1` only:
    a `FeedGap` on `Stream::Funding` hits a trade when its closed interval
    `[start, end]` meets D7's booking span, that is, when `start` is
    before the exit fill trade's time (or the position is still open) and
    `end` is at or after the entry fill trade's time. c = max(gap start,
    entry fill time). *Why:* ADR-055 D6's uniform rule puts c where the
    unknown begins, bounded below by the start of the span the value
    reads. Funding is booked from the entry fill (D7), and a pending entry
    books nothing, so the bound is the fill and not the decision;
  - a rejected `Stream::Funding` event (`funding_gap`), under
    `funding.recorded@1` only: it hits a trade when D7's booking span holds
    the event's time, that is, when the time is at or after the entry fill
    trade's time and before the exit fill trade's time (or the position
    has no exit fill). c = max(event time, entry fill time). Under
    `funding.none@1` it is ignored, like a funding gap. *Why:* the engine
    can refuse a `FundingSettlement` (`OutOfOrder`, `Duplicate` or
    `Overflow`, `StateError` in `crates/mie-domain/src/state.rs`), and
    skipping it would book zero funding by omission, which D7 forbids. Its
    cause is a funding gap's, a settlement value missing from the span, so
    it takes that token and that c rule. *Rejected:* ignoring the event,
    which is zero by omission; the `rejected` token, which names a
    trades-stream cause, the class *Accept when* 4(b) compares with the
    labels; and a seventh token for one cause. Weak spot: a `Duplicate`
    rejection loses no value and still censors the trade. S6 asserts
    `rejections 0`, so it is unaffected. (Refined by S2, #167, STOP 1
    decision 1.)
  - an overflow (`overflow`, D10): the time of the event whose value
    overflowed, that is the entry fill trade (E′, the entry fee,
    displacement), a monitored trade (MFE, MAE), a settlement (funding)
    or the exit fill trade (X′, the exit fee, gross, net).
- `funding_gap` and `overflow` censor the trade outright, even when its
  exit would stand by the rule below: they make a value on the line
  unknown, not the path.
- **Precedence:** when one trade gets several censors, the earliest c
  wins, and at equal c the first one applied stays (ADR-055 D6;
  `crates/mie-domain/src/conditional/label.rs` replaces a censor only
  when the new c is strictly earlier).
- **When an outcome stands:** it is fixed by the trades **strictly before
  c**. A stop or target exit stands when its touch is before c. A time
  exit stands when its deadline is ≤ c, because the window
  `[entry, deadline)` is then complete. An `unfilled` decision stands
  under the same rule, deadline ≤ c: the window `[decision, deadline)`
  then holds every trade that could have filled the entry, so a pending
  entry whose deadline is ≤ c ends `unfilled`, not censored. Everything
  else is censored at c: open positions, pending entries, and exits
  decided at or after c. Every exit that stands has an observed fill: the
  provider opens a gap at the last delivered trade (ADR-055 D6,
  *Emission*). (The `unfilled` case refined by S2, #167: D6 and this list
  named the outcome and its slot, not the rule.)
- **Finality:** a trade is final once a trades-stream event is observed
  in a later millisecond than its touch, or at or after its deadline (a
  time exit or an `unfilled` decision). A censored trade is final once a
  trades-stream event at or after its c is observed. This is ADR-055 D6's
  emission rule: a label is emitted once the trades are known up to its
  censor time or window end, and a censored label once they are known up
  to c. A later gap reaching back into a final trade does not reopen it;
  it is counted in `reach-backs`. (The censored case refined by S2, #167:
  without it a censored trade would stay replaceable by every later
  reach-back, which the labels never are.)
- **A funding gap is never a reach-back.** It censors whenever it is
  delivered, including after the trade has closed, for two reasons:
  finality is trades-stream finality and does not cover funding, and the
  result is written only after `finish`, so nothing has been emitted that
  would need reopening. The slot history (D8) does not change.
- At the sample end the driver passes the trades-stream trailing gaps,
  then finishes (the ADR-055 D6 contract). Under `funding.recorded@1` it
  also passes the `Stream::Funding` entries of `trailing_gaps()`,
  together with the trades entries in canonical order, before `finish`.
  *Why:* a trailing gap is listed, not delivered (ADR-055 D6, *Trailing
  gaps*), and without it a missing last month would book zero funding,
  which D7 forbids. A funding gap moves no trades-stream time and closes
  nothing. Nothing runs past the sample, so a result depends only on
  `[origin, sample end)`.
- Censored trades keep their fields up to c and are counted, but they are
  excluded from every metric, because their outcome is unknown.
- *Why these rules:* they are what the trades stream can prove about
  coverage, and copying them makes the S6 check exact.
- *Rejected:* dropping censored trades from the result, which would hide
  the censor rate and leave S6 nothing to compare with the labels'
  censors; and closing a censored position as a time exit at c, which
  would book an outcome from trades the data did not show (ADR-055 D6
  keeps only what is resolved strictly before c).

### D10. Per-trade values

Values are per unit of base, so no position size enters a result (the
capital and the risk per trade stay out of this repository,
`docs/direction.md`). Symbols: e and x are the prices of the entry and
exit fill trades, E′ and X′ the slipped fills (D6), fees the sum of both
legs' fees, funding the D7 sum.

- gross R = σ(x − e) / R, on trade prices;
- net R = (σ(X′ − E′) − fees − funding) / R;
- displacement = σ(e − reference) / R; positive means adverse;
- MFE and MAE, in R: MFE = max(0, max σ(p − e)) / R and
  MAE = max(0, max σ(e − p)) / R, with p over the trades from the entry
  fill trade through the exit fill trade. Both are ≥ 0. *Why R:*
  `docs/direction.md` (*Net outcome*): "Every outcome is measured in R".
  A mean of price excursions over trades would mix volatility regimes
  and mostly restate the regime, ADR-055 D4's argument, and R is an ATR
  multiple. Price units can still be derived as value × `risk`.
  *Rejected:* price units.
- Each R value is (price units × 10⁸) / R units, rounded half to even
  once, to 8 decimals: the ADR-027 decimal form that result text v1
  already uses.
- Arithmetic is i128 integer, with no float. Overflow is checked and
  censors the trade as `overflow` (c in D9); it never wraps. Overflow
  means an i128 step overflowing, or a value leaving the i64 range of
  10⁻⁸ units, which is the range of `Price` and `Rate` (ADR-027,
  `crates/mie-domain/src/num.rs`). That keeps every rendered value
  readable by the existing decimal parser.
- *Rejected: floats*, which ADR-027 allows for derived statistics. The
  result text is the identity used for reproduction, and fixed point
  renders in the existing form without a float-formatting rule.

### D11. Metrics and the regime split

- **Counts:** `trades` (target, stop and time exits), `censored`,
  `unfilled`.
- **Over `trades`:**
  - expectancy (mean net R) and mean gross R;
  - profit factor = Σ positive net R ÷ |Σ negative net R|, `-` with no
    losing trade or no trade;
  - win rate = the share of trades with net R > 0;
  - max drawdown = the largest fall of cumulative net R from its running
    peak, which starts at 0, in exit order (equal to decision order under
    D8);
  - time to target = the mean of (touch − decision) over target exits,
    in integer ms: Σ(touch − decision) ÷ the number of target exits,
    rounded half to even once;
  - mean MFE, MAE and displacement.
- Sums are exact over the per-trade values, with one half-to-even rounding
  at each final division (`div_half_even`, which S2 moves to `num.rs`).
  Every mean and the win rate are one rounding of an exact sum divided by
  a count, and profit factor is one rounding of Σ positive net R divided
  by |Σ negative net R|, each scaled by 10⁸ for the 8-place values. Max
  drawdown takes no rounding: it is a difference of two exact cumulative
  sums of the trades' net R values. Why: anyone can re-derive every metric
  from the stored trade lines with integer arithmetic.
- *Why integer ms for time to target:* the ms is the resolution of event
  time (ADR-028) and the unit of every time in result text v1 (`origin`,
  `sample`, `latency`) and of ADR-057's `time_to_event_ms`. *Rejected:*
  8 decimals, which would put a second time form into one text.
- *Means only, no quantiles:* the trade lines hold every value, so any
  other statistic can be derived from the stored result, and a quantile
  choice would be a number without a source.
- **Regime split:** one metrics line per regime feature of the spec's set
  and per label, `LOW` … `EXTREME` and then `-` (unready). Every line is
  rendered even when its bucket is empty, so the set of lines does not
  depend on the data. A bucket's drawdown runs over that bucket's trades
  only.

### D12. Outcome kind `backtest`

A result text v1 outcome kind (ADR-040 D6), with a new `PIPELINE_KINDS`
entry `("research.backtest", 1, "backtest")`
(`crates/mie-domain/src/research/result.rs`). After `outcome backtest`, in
order:

1. the twelve provenance lines of `history-replay-summary`, with
   unchanged grammar (ADR-052 D10); `engine-features` and `state-encoding`
   give the result key's engine;
2. `triggers <n>`, `filtered <n>`,
   `skipped overlap=<n> stale_reference=<n> no_side=<n> no_risk=<n> stop_side=<n>`,
   `reach-backs <n>`;
3. one `trade` line per taken decision, in decision order;
4. the `metrics` lines (D11): `all` first, then the buckets.

`-` marks an undefined value. Parsing is strict and canonical-only
(ADR-040 D6).

**The `trade` line:**

```
trade <decision ms> <event_kind> <seq> side=<long|short> regime=<labels> reference=<price> stop=<price> target=<price> risk=<price> entry=<ms>:<price>|- exit=<target|stop|time|unfilled|censored> exit_at=<ms> fill=<ms>:<price>|- funding=<price>|- gross=<R|-> net=<R|-> displacement=<R|-> mfe=<R|-> mae=<R|-> censor=<reason|->
```

- The decision key is positional: `CanonicalKey`'s (time, kind, seq)
  (`crates/mie-domain/src/order.rs`) in ADR-057's column order
  (`decision_time`, `event_kind`, `event_seq`). Time and seq are decimal
  integers. The kind uses the strings of ADR-057 D2's `event_kind_name`.
  That table lives in the parquet adapter, which the domain cannot
  import, so S3 adds a domain table with the same strings, pinned by a
  test. Every other field is `name=value`, in the fixed order shown.
  *Why the event key:* it is ADR-055 D1's join key (`decision_time`,
  `event_kind`, `event_seq` on ADR-057's `contexts`), so S6 joins without
  any mapping.
- `side` is σ as D2 names it (long +1, short −1).
- `regime` holds the label at the decision of each regime feature in the
  spec's set, comma-separated in feature-id order. Labels are `LOW` …
  `EXTREME` (spec text v1's upper-case form) or `-` when unready. The
  value is `none` when the set has no regime feature, which is spec text
  v1's `none` for an empty list.
- `reference`, `stop`, `target`, `risk` (R, in price units) and `funding`
  use the 8-place price form. `funding` is per unit of base, with
  positive meaning paid (D7). Under `funding.none@1` it is `0.00000000`
  on target, stop and time lines, and `-` on `unfilled` and `censored`
  lines as under `funding.recorded@1` (the `-` cases below). *Why:* the
  set of `-` fields then depends on the exit kind only, never on the
  funding rule, so a parser checks one shape per exit kind. *Rejected:*
  `0.00000000` on every line under `funding.none@1`, which would make the
  line shape depend on the rule.
- `entry` and `fill` hold the time and price of the fill **trades** (e,
  x), not the slipped fills. E′, X′ and both fees follow from e and x,
  the spec's `slippage` and `taker`, by D6, so every value on the line
  can be re-derived with integer arithmetic (D11's reason). *Rejected:*
  storing E′, which would lose e to rounding.
- `exit_at` is always set: the touch, the deadline or c.
- The `-` cases:
  - `entry`, `displacement`, `mfe` and `mae` are `-` when there is no
    entry fill, or on a censored line whose entry fill trade is not
    strictly before c;
  - `fill`, `funding`, `gross` and `net` are `-` on `unfilled` and
    `censored` lines;
  - any value whose computation overflowed is `-`;
  - `censor` is `-` exactly when `exit` is not `censored`.
- On a censored line, `mfe` and `mae` cover the trades strictly before c
  and are lower bounds (ADR-055 D6).

**The censor reason tokens:** `censor=` takes exactly `gap`, `rejected`,
`end_of_data`, `chain_restart`, `funding_gap` and `overflow` (D9).

- The first three are the strings of `CensorReason::as_str`
  (`crates/mie-domain/src/conditional/label.rs`) and therefore ADR-057's
  `race_censor_reason`, so S6 compares the text directly. The backtest
  defines its own enum with these strings, because D15 keeps it off
  `label::`. S3 pins the name table with a test, as ADR-057 D2 pins its
  tables.
- `stale_reference` is a skip in the backtest (D4) and never a censor
  token.
- `chain_restart` is the name ADR-052 D6 already uses for the journal
  record of the same event.

**The `metrics` lines:**

```
metrics all <fields>
metrics <regime feature id@N> <LOW|MEDIUM|HIGH|EXTREME|-> <fields>
<fields> = trades=<n> censored=<n> unfilled=<n> expectancy=<R|-> mean_gross=<R|-> profit_factor=<x|-> win_rate=<x|-> max_drawdown=<R|-> time_to_target=<ms|-> mean_mfe=<R|-> mean_mae=<R|-> mean_displacement=<R|->
```

- The bucket form copies spec text v1's `regime-filter <id@N> <LABEL,…>`:
  the feature key, then one label. Buckets come in feature-id order (the
  canonical spec sorts features by id), with labels in the order `LOW`,
  `MEDIUM`, `HIGH`, `EXTREME`, then `-`. A spec set without a regime
  feature gives only the `all` line.
- Field names are the Research Lab brief's metric list (expectancy,
  profit factor, win rate, MFE / MAE, drawdown, entry displacement,
  time-to-target) in snake_case. That is the pair-name grammar of spec
  text v1 rule references (`[a-z][a-z0-9_]*`), which the `skipped` line
  and the `trade` line (`exit_at`) use too, so every pair name follows
  one grammar. Line keys stay kebab-case, as in result text v1
  (`effective-origin`) and in this kind's `reach-backs`. Fields come in
  D11's order.
- Value forms: `<n>` is a decimal integer. R values, `profit_factor` and
  `win_rate` use the signed 8-place form of `num.rs` (`-0.12345678`, with
  zero as `0.00000000`). `win_rate` is a fraction from 0 to 1, not a
  percent; that is `Rate`'s dimensionless form (ADR-027).
  `time_to_target` is in ms (D11).
- `-` appears in these cases: every mean and `win_rate` when `trades=0`;
  `time_to_target` with no target exit; `profit_factor` with no losing
  trade or no trade (D11); and any metric whose value leaves the i64 unit
  range (D10). Two metrics can leave it: `profit_factor`, an unbounded
  ratio, and `max_drawdown`, a sum over trades whose values are each in
  range. A mean of in-range values lies between their extremes, and
  `win_rate` lies in [0, 1], so neither can. `max_drawdown` is
  `0.00000000` with no trade, because the running peak starts at 0 (D11).

### D13. Pipeline `research.backtest@1`

- **Source:** the history chain only (STOP 1 decision 7; ADR-052 D10:
  every pipeline that reads Market State runs on the chain). *Rejected:*
  `--source archive` or `live`. Their engine starts at the window's first
  event, so the state at a decision would depend on where the sample
  begins, and it would differ from the state live holds at the same
  instant: the research/live skew that ADR-052 removes by driving both
  from one fixed origin (ADR-052 *Context*). Live also delivers no
  settlement event (D7).
- **Key:** (experiment id, `research.backtest@1`, engine key), for
  ADR-052 D10's reason: keyed without the engine, the store would block
  the experiment for good at the next catalog change or encoding bump.
- **CLI:** `mie experiment run <spec> --config <ingest config with
  [history]> --results <dir> --source history --pipeline backtest`.
  `--pipeline backtest` with any other source fails. `mie experiment
  validate` gains a second stage that prints resolution errors, without
  changing which texts parse.
- **Run order:**
  1. Before any drive, with nothing stored on failure: open the sample,
     check `data` (ADR-040 D5) and the feature set (ADR-040 D4), resolve
     the rules (D2), and check funding coverage (D7).
  2. Then ADR-052 D10's order: the prefix drive (not evaluated), then the
     sample drive through `drive_chain_with`, so restarts reach the
     simulator; then the trailing trades gaps (and, under
     `funding.recorded@1`, the trailing funding gaps, D9) and finish.
  3. Compute the metrics and store the result as `Recorded`, `Reproduced`
     or `Diverged` (ADR-040 D7).
- **New `ResearchError` variants:** `Unresolved` (every resolution error)
  and `FundingNotDelivered` (the sample end and the cutover).

### D14. `BacktestEngine` port

- `mie_ports::outbound::BacktestEngine` has one method. It runs a
  resolved plan over the spec's sample and returns the domain's run
  output, before metrics: the chain provenance values, the trade records
  and the counts. Its errors are the provider, data, feature and sample
  variants of `ResearchError`.
- `mie_app::backtest` implements it on the chain, driving the domain
  `Simulator` and rule evaluator through `drive_chain_with`.
  `ExperimentService` holds it and keeps resolution, the funding check,
  metrics, rendering and the store. `mie-cli` wires it.
- *Why:* the Research Lab brief ("Concrete engines can be replaced
  without changing the research-domain rules"), and ADR-018 lists the
  port. Every engine is judged by the same metric code and store rules,
  and each takes a resolved plan, so no engine reinterprets spec text.
- *Rejected:*
  - no port, with the service calling the simulator directly: this fixes
    the service to one engine and leaves ADR-018's port unrealized;
  - an observer port (`observe`/`reject`/`restart`/`finish`): this makes
    the drive loop part of the port surface and ties every engine to it.

### D15. Leakage guard

- The simulator reads trades after the decision, the same path ADR-055
  D11 closes for labels. So no file under `crates/mie-domain/src`,
  `crates/mie-ports/src` or `crates/mie-app/src` may name `backtest::` on
  any line, or a `backtest` path segment in a `use` item (joined across
  lines), with the label guard's scan and self-probes in
  `tools/check-architecture.sh`.
- Exceptions:
  - `crates/mie-domain/src/backtest.rs` and `crates/mie-domain/src/backtest/`;
  - `crates/mie-domain/src/research/result.rs` (the outcome kind);
  - `crates/mie-ports/src/outbound.rs` (the port) and
    `crates/mie-ports/src/inbound.rs` (`Unresolved`);
  - `crates/mie-app/src/backtest.rs` and `crates/mie-app/src/research.rs`.
- `evidence_prices` moves from `conditional/label.rs` to `trigger.rs`
  (S4), so the backtest never names `label::`.

### D16. Source, and what #33 inherits

- The chain's origin is 2025-10-01 (ADR-052 D1), so a chain backtest
  cannot reach the calibration slice (2024-10-01 … 2024-12-31) or the
  hold-out (2025-01-01 … 2025-09-30, `docs/direction.md`).
- Reaching them needs an ADR that moves the origin (ADR-052 D1), and
  either a `fundingRate` import for that year (today `aggTrades` and
  `metrics` only, `docs/data-availability.md`) or `funding.none@1` with
  the bias stated.
- The live segment has no settlement events, so a forward test on live
  data needs a settlement source in the chain before `funding.recorded@1`
  can run there.

### D17. Where each number comes from

| Number | Source | What breaks at another value |
|---|---|---|
| 8 decimals for R and metrics | ADR-027's unit, the 8-place form of result text v1 (ADR-040 D2) | fewer: a rounding that a hand re-derivation must also reproduce; more: no text form |
| One position per spec | STOP 1 decision 3 | independent trades: overlapping trades share one path, so the drawdown counts one move several times |
| Range bounds in D2 | the domain of definition (D2) | the boundary value is undefined (D2) |
| Taker on both legs | STOP 1 decision 5, the `docs/direction.md` fee tier | — |
| Fee and latency ranges | unchanged, ADR-040 D1 (grammar in the `mie_domain::research` module docs) | — (no new number) |
| S6 sample `1759276800000 1790812800000` | the #12 year: the only window with an accepted `conditional.labels@2` dataset (ADR-057 *Acceptance*) | any other window has no independent labels |
| S6 trigger `trigger.effort_result.5m@1` | the most fires of any id, 12 952 (ADR-053 *Acceptance*; next: `effort_result.15m` with 7 496), counted from trigger events only | fewer comparisons; a hand pick would be a choice made after #28 |
| S6 `side=text:with` | the identity mapping. The check is side-symmetric, and the evidence has both sides, so both mappings run | — |
| S6 `multiple=rate:0.31333000` | ADR-056 D4′: b = 31 333 / 10⁵ | the levels are no longer the barriers, and the check breaks |
| S6 `r=rate:1.00000000` | the symmetric barrier pair (ADR-055 D4, ADR-056 D4′) | the target is not the opposite barrier |
| S6 `max_hold_ms=int:3600000` | the 1h horizon, ADR-056's 1h identity (v1 = v2 = the engine's ATR(1h)) | other horizons depend on the per-horizon ATR |
| S6 `taker=0.00050000` | `docs/direction.md`: VIP 0, 10 bps round trip | — |
| S6 `maker=0.00020000` | `docs/direction.md`: one maker leg moves the round trip by about 3 bps (5 bps taker − 2 bps maker) | unused by registry v1, but part of the id |
| S6 slippage 0, latency 0 | #30 decision 6: nothing is calibrated or measured, so explicit zeros | latency > 0 breaks the window alignment (D6) |
| 604 001 515 events, 0 rejections | ADR-040 *Acceptance* (nine streams) = ADR-055's 603 323 345 (`aggTrades` + `metrics`) + 677 075 klines (365 days × 1 855 bars of the six intervals, 0 missing) + 1 095 settlements (365 days × 3); the fold changes quantities, not the count (ADR-050 *Acceptance*: 0 unattributed) | any other count: a day, a stream or a fold differs |
| Integer ms for time to target | ADR-028's event-time resolution; the time unit of result text v1 | 8 decimals: a second time form in one text |
| i64 range of 10⁻⁸ units for every rendered value | ADR-027 `Price`/`Rate` (`num.rs`): the range the existing decimal parser reads | wider: the text no longer parses with the existing parser |
| Year price range 57 758.6 … 126 208.5 USDT (the overflow precondition, *Accept when* 4) | ADR-036 *Measurements behind the numbers*, every trade of the S6 window; ADR-050 D3 keeps prices under the fold | a recorded range narrower than the true one would make the bound unsound |
| 1 097 rounding units in the precondition's B | 2 fee roundings + 1 095 settlement roundings (this table's 1 095) | fewer: B misses a rounding unit |
| 0 expected mismatches | the derivation in *Accept when* | — (a single mismatch is a bug) |
| ≈ 25 min, < 0.4 GB per run | the nearest measured analog, ADR-040 *Acceptance* (26.1 and 24.9 min, 361 MB, nine streams) | — (a forecast, not a criterion) |
| 8 GB memory and 400 % CPU cap for the S6 run | the caps of the label runs it is checked against: ADR-055 *Measurement* (8 GB, 400 %) and ADR-057 *Acceptance* (8 GB) | none on the result, which depends only on the events (ADR-040 D7); a memory cap below the run's peak (the row above) aborts the run |
| About 70 min per labels rebuild | ADR-057 *Acceptance*: 1:08:26 and 1:10:11 | — (a cost, not a criterion) |
| 560 796 records with 0 scale mismatches | ADR-056 *Measurement* | any mismatch: ATR_1h is not `state.atr` on that record, and the levels are not the barriers there |
| 12 monthly `fundingRate` files, 0 missing | `docs/data-availability.md`, the #12 year | fewer: the year's funding is incomplete, and the expected 0 `funding_gap` censors in 4(b) no longer follow |
| 1 % in D7's funding error example | an illustration of the stated relation (error = basis as a fraction of the amount) | — (an example, not a parameter) |
| 2025-10-01 origin; 2024-10-01 … 2024-12-31 and 2025-01-01 … 2025-09-30 (D16) | ADR-052 D1; `docs/direction.md` | — (cited facts, not choices of this ADR) |

Every other figure in the text is an identifier (an ADR, issue, id or
hash), a constant that follows from a row above (2⁶³ − 1 from the i64
range, 10⁸ from the 8 decimals, 1 095 settlements from the event count),
or a 0 or 1 that a rule defines.

### Out of scope

- Any code (S2–S5).
- Edits to existing ADRs; ADR-052 stays as it is.
- State-filter rules and `target.level@1` (#31).
- Spread and depth-walk slippage, maker legs and slippage calibration
  (#172).
- Walk-forward, the hold-out and an origin move (#33).
- Limit or maker entries, scale-outs, trailing stops and absolute sides.
- Reading any metric.
- The S6 check tool (#171).

## Consequences

- **What S2–S5 must pin**, as test obligations:
  - S2: D6, D7 and D9, including a touch by the fill trade, `unfilled`,
    finality and every censor reason; a funding gap inside a position's
    span, one that reaches back into a closed trade, a trailing funding
    gap and a funding gap under `funding.none@1` (no effect); an overflow
    at each event class; a chain restart; precedence at equal c;
  - S3: the D12 golden and the D10/D11 arithmetic; the golden covers
    every censor token, every `-` case, `regime=none` and a metric
    outside the range; tests pin the censor-token and event-kind name
    tables;
  - S4: the D2 errors, the D3 table and the golden spec not resolving;
    the `no_risk` skip of an unready ATR under
    `invalidation.beyond_extreme@1` and its order before `stop_side`
    (D5);
  - S5: the D13 order and errors;
  - #171: the check computes and asserts the overflow precondition
    (*Accept when* 4) before it compares anything.
- **The weak spots of spec text v1** (D1): exactly-once slots, the closed
  `fees` line and resolution after parsing. The first consumer that needs
  a repeated slot writes the v2 ADR.
- **The latency and time-exit weak spots** (D6): latency delays the entry
  only, and the time exit is treated as pre-scheduled, so a latency
  sensitivity understates the cost of a manual time exit.
- **The funding error bound** (D7): without a mark price, the funding
  amount errs by the basis as a fraction of itself.
- **S6 proves the engine, not a strategy.** Its check compares exits,
  times, levels and censors with independent labels; it reads no R value
  and no metric (#30 decision 6).
- **#33 inherits D16:** an origin-moving ADR and a `fundingRate` import
  (or `funding.none@1` with its bias) before any backtest reaches the
  calibration slice or the hold-out, and a settlement source before
  `funding.recorded@1` runs on live data.
- **The chain cutover dependency:** S6 needs #106's production cutover.
  ADR-052 is proposed, and the chain's dataset version includes the
  cutover (ADR-052 D5), so a run on a provisional cutover records a result
  tied to that chain.
- **Stream assumption of the S6 check:** the chain replays nine streams,
  the dataset was built from `aggTrades` + `metrics`. They are expected to
  yield the same trigger events: klines are a cross-check only and never a
  bar source (ADR-031, `docs/data-availability.md`), and no trigger rule
  reads a funding input (ADR-053 D2, D5–D7). The asserted 12 952 and the
  join catch a difference, and a fallback is a decision for the issue, not
  a tolerance.

## Alternatives considered

- **Spec text v2 first.** Orphans every stored result and fills no slot
  the backtest lacks (Approach, D1).
- **Levels from the entry fill.** R would move with every cost
  assumption, and the levels would no longer equal the barriers (D5).
- **Absolute sides** (`long|short`). Mirror-image setups would share one
  spec (D3).
- **A hand side mapping per family.** An interpretation no accepted ADR
  makes (D3).
- **Fill at the touching trade or at the level price.** The touching
  print happened before any order could react; the level price ignores
  gaps through a level and is optimistic for stops (D6).
- **Latency on every leg.** A resting order has no human delay (D6).
- **Unordered skip reasons, or `overlap` first.** The first lets two
  implementations of the text render different counts; the second makes
  every count depend on the exit path (D4).
- **Independent overlapping trades.** One price path counted several
  times in the drawdown (D8).
- **Dropping censored trades, or closing them as time exits at c.** The
  first hides the censor rate; the second books outcomes the data did not
  show (D9).
- **Archive or live as the source.** The research/live skew of ADR-052,
  and no settlements on live (D13).
- **No port, or an observer port.** The first fixes the service to one
  engine and leaves ADR-018's port unrealized; the second makes the drive
  loop part of the port surface (D14).
- **Floats.** The result text is the reproduction identity, and fixed
  point renders without a float-formatting rule (D10).
- **Quantile metrics.** Derivable from the stored trade lines, and a
  quantile choice would be a number without a source (D11).
- **A fresh labels build for the check.** A rebuild either reproduces the
  accepted content hash, and is then the same dataset at about 70 min
  more (ADR-057 *Acceptance*: 1:08:26 and 1:10:11), or does not, and then
  the check would compare against labels no accepted ADR measured. The
  accepted dataset is used, and a rebuild only stands in for it when it
  reproduces the hash (*Accept when*, item 4).

## Accept when

S6 (#171). It runs under an 8 GB memory cap and a 400 % CPU cap, the caps
of the label runs it is checked against (D17).

1. **The spec:**
   - `sample 1759276800000 1790812800000`, with `data` as
     `mie replay --source history` prints it for that window;
   - `features`: the closure of `trigger.effort_result.5m@1`,
     `trade.last_price@1`, `volatility.atr.1h@1` and
     `volatility.regime.1h@1`;
   - `state-filter none`, `regime-filter none`,
     `location location.any@1`;
   - `trigger trigger.fires@1 feature=feature:trigger.effort_result.5m@1`;
   - `entry entry.next_trade@1 side=text:with`;
   - `invalidation invalidation.atr@1 atr=feature:volatility.atr.1h@1 multiple=rate:0.31333000`;
   - `target target.r_multiple@1 max_hold_ms=int:3600000 r=rate:1.00000000`;
   - `fees maker=0.00020000 taker=0.00050000`,
     `slippage slippage.fixed@1 rate=rate:0.00000000`,
     `funding funding.recorded@1`, `latency 0`.
2. **Reproduction:** the first run prints `recorded <key>` and the second
   `reproduced <key>`. The result file's SHA-256 is the same before and
   after the second run.
3. **Provenance (asserted):** `stream 604001515:<hex>`, `rejections 0`,
   `prefix-data -`, `effective-origin -`, `triggers 12952`.
4. **Independent check** against the year dataset of ADR-057's
   *Acceptance*, content hash
   `47d9cb621aff5d1c361308c9935d183a66ea91594102d269b437c8fce549a683`.
   `mie conditional verify` must PASS on it first, or a rebuild with
   ADR-057's recorded command must reproduce that hash.
   - **Overflow precondition, asserted before any comparison.** No record
     bounds R from below, so the text alone cannot derive the expected 0
     for `overflow` in (b). The check computes a sufficient condition
     instead. A FAIL is a decision for the issue, not a tolerance.
     - *Inputs:*
       - R_min is the smallest `upper` − `contexts.reference` over the
         compared decisions' 1h `outcomes` rows that are not
         `unavailable`, read from the ADR-057 dataset. By (c) and D5 this
         is exactly the smallest `risk` the backtest uses.
       - The year's trade-price range [P_lo, P_hi] =
         [57 758.6, 126 208.5] USDT was measured over every trade of the
         same window (ADR-036, *Measurements behind the numbers*). The
         later fold changes quantities only: the aggregate "keeps its id,
         price, `transact_time` and side" (ADR-050 D3), so the range
         still holds.
       - S = Σ|rate| over the sample's 1 095 settlements (D17), read from
         the year's 12 monthly `fundingRate` files.
     - *Bound:* the numerator of every R value (D10) is at most
       B = (P_hi − P_lo) + (2 · taker + S) · P_hi + 1 097 units:
       - every price the backtest reads is a trade price (reference, e,
         x, the monitored trades, the funding price p), so the price
         differences behind gross, displacement, MFE and MAE are at most
         P_hi − P_lo;
       - with slippage 0, E′ = e and X′ = x;
       - the two fees add at most 2 · taker · P_hi;
       - funding adds at most S · P_hi, because a trade books a subset of
         the sample's settlements;
       - the 1 097 units are one rounding unit per fee (2) and per
         settlement term (1 095).
     - *Assert:* B ≤ 2⁶³ − 1 units, so fees and funding stay in the i64
       range (D10); and B · 10⁸ < (2⁶³ − 1) · R_min, so every R value
       stays in range after its single rounding (D10). Together these rule
       out every `overflow` censor on the year, which derives (b)'s
       expected 0 from recorded and computed inputs. The check prints PASS
       or FAIL together with R_min and S; neither is an outcome value, so
       the blindness of item 5 holds.
     - *Rejected:* a lower bound on ATR_1h or on R that no record gives (a
       number without a source); excluding overflow-censored trades from
       the comparison, which would hide a wrongly flagged overflow.
   - (a) **Join:** every decision joins the dataset's trigger records of
     the id at the same `decision_time`, `event_kind` and `event_seq`.
     Their count is 12 952.
   - (b) **Outcomes:** each decision except an `overlap` skip is compared
     with its 1h `outcomes` row:
     - target ↔ `upper` for a long and `lower` for a short, stop the
       reverse, with `exit_at` = `race_at`;
     - time ↔ `neither`;
     - `unfilled` ↔ `neither` with `last` NULL;
     - censored ↔ `censored` with the same reason
       (`race_censor_reason`) and `race_censor_at` = c;
     - `stale_reference` ↔ a row whose window censor (`censor_reason`,
       `censor_at`) is `stale_reference` at the decision time, with race
       `censored` or `unavailable`. The labeler censors a label with a
       stale reference as a whole at the decision. The race is `censored`
       with `race_censor_reason` `stale_reference` when the label has
       barriers, and it stays `unavailable` when it has none (no
       reference, an unready ATR, an offset that rounds to 0 or a barrier
       outside the range), because an unavailable race stays unavailable
       under every censor (ADR-055 D6;
       `crates/mie-domain/src/conditional/label.rs`, pinned by the test
       `a_gap_since_the_last_trade_makes_the_reference_stale`). D4's order
       puts `stale_reference` before `no_risk`, so a stale decision
       without barriers is a `stale_reference` skip, never a `no_risk`
       one;
     - `no_risk` ↔ race `unavailable` with no `stale_reference` window
       censor at the decision;
     - `no_side` must be 0, because the aggressor is always set;
     - `chain_restart`, `funding_gap` and `overflow` have no label
       counterpart, so each counts as a mismatch. Expected count: 0.
       `effective-origin -` is asserted (item 3), so there is no restart,
       and the year has 0 missing `fundingRate` months
       (`docs/data-availability.md`), so there is no funding gap. The
       expected 0 for `overflow` comes from the overflow precondition
       below.
   - (c) **Levels:** (stop, target) = (`lower`, `upper`) for a long or
     (`upper`, `lower`) for a short, and `reference` =
     `contexts.reference`, exactly to the price unit.
   - **Expected mismatches: 0, derived and not tolerated:**
     - latency 0 makes the entry fill trade the first trade of ADR-055
       D2's window, and the monitored trades the window's (D6);
     - the offset rule is identical (D5), and ATR_1h = `state.atr`
       (ADR-056 *Measurement*: 0 scale mismatches at 560 796 records), so
       with k = 1 the levels are the barriers;
     - touch and tie follow ADR-055 D5;
     - the stale condition is ADR-055 D6's (D4), and D4's order splits
       stale decisions from decisions without barriers exactly as the
       labeler's window censor and race do (above);
     - the censor times and the strictly-before rule follow ADR-055 D6
       (D9).
   - The difference between entry and reference moves only fill prices,
     which the label does not hold, so it cannot cause a mismatch.
     Fill-to-fill gross R is not in the label. It is pinned by S2's
     hand-computed fixture and S5's end-to-end test.
5. **Blindness:** the check prints only the join count, the mismatches
   per class, the non-outcome counts (filtered, skipped by reason,
   censored, unfilled) and the overflow precondition (PASS or FAIL, R_min
   and S). It prints no exit-kind counts, no R values and no
   `metrics` line (#30 decision 6).
6. **Reachability, flagged now:**
   - The chain needs #106's production cutover (*Consequences*).
   - The dataset must exist, or must be rebuilt (about 70 min, ADR-057
     *Acceptance*).
   - The check is a tool written from this text that shares no backtest
     code. It belongs to #171.

Anything else fails: a `FAIL: …` line, a `Diverged` result, another count
or provenance value, a single mismatch, or a changed file.
