# ADR-055: Forward outcome labels — ATR-scaled barrier races, excursions and passages from the trades after the decision

- Status: proposed
- Date: 2026-10-20

## Context

#26 asks to measure what happened after each context, strictly from data
after the decision time (Conditional Behavior brief, outputs; brief §12; the
Strategy Research Lab brief, leakage controls). Its scope:

- outcomes: which of an upside and a downside threshold is reached first
  (thresholds in ATR multiples and/or bps, to be decided), the maximum
  favorable and adverse excursion within horizons, time to event, level
  revisit, invalidation reached first, excursion distributions;
- labels computed in replay only and stored separately from features, so
  feature computation can never read them;
- censoring: samples whose horizon hits the end of data or a feed gap are
  marked censored, never silently dropped;
- a deterministic tie rule when both thresholds are touched at the same
  event time.

Its acceptance criteria: a leakage test (changing any event after the
decision time changes labels only, never the context record), and tests for
threshold races, censoring and ties.

What exists:

- The context records of ADR-054: one per trigger event and one baseline
  sample per event that closes a 1m bar, each keyed by the `CanonicalKey`
  (ADR-028) of its event, whose time is the decision time. ADR-054 D2
  snapshots `trade.last_price@1` as "the threshold reference at decision
  time" and `volatility.atr.1h@1` for "thresholds in ATR multiples", both
  named for #26.
- ADR-054 *Out of scope* hands this issue "forward outcome labels, horizons,
  censoring, and horizon-overlap thinning of samples"; its Consequences ask
  #26/#27 to thin the about 525 000 baseline samples a year by horizon.
- ADR-034 decision 3 orders archive open interest at the end of its
  interval because "look-ahead is the costlier error for forward labels
  (#26)".
- `docs/direction.md` T3 consumes the labels: "the probability of each
  threshold being reached first", against the baseline in the same location
  context.

This ADR decides the label, its window, the barriers, the tie rule,
censoring, excursions, passages, thinning, provenance and placement. No
accepted ADR is edited.

Delivery is in three PRs under #26: M1 ships this ADR and the domain
labeler `mie_domain::conditional::label`; M2 the replay driver and the
measurement tool `label_measure`; M3 records the measurement of *Accept
when* here and flips the status.

## Decision

### Approach

A pure domain labeler takes in the ADR-054 records as they are emitted,
plus the accepted events after them, and emits one **forward label** per
record, in record order, to a sink separate from the records. A label
depends on two things only: the **trade path strictly after the record's
event** in canonical order, and two values the record already holds at
decision time, `state.last_price` and `state.atr`. So the labeler never
sees the engine, and no engine, catalog or sampler code sees a label. The
outcomes are trade-level first passages and window extremes over five
horizons, with one symmetric ATR-scaled barrier pair per horizon. Trades
gaps, rejected trades and the end of data censor labels explicitly.

*Rejected: labels built from 1m bars.* The 5m barrier band (D4) is
2 · 0.09045 = 0.181 ATR wide, 1.4 times the null expected 1m range of
0.129 ATR (the hourly range, which the ATR measures, times √(1/60) under
the Brownian null of D4). One bar would often touch both barriers, and
which came first would be lost. A bar's high and low also include the
trades of its minute from before the decision. *Rejected: labels as engine
features.* That is the leakage path this issue exists to close.

### D1. Unit and join

- **One label per context record**, keyed `(event, ordinal)`. `ordinal` is
  the record's position among its event's records in the ADR-054 D1 order:
  the baseline sample first, then the triggers in emission order.
- All records of one event share their path fields (the same reference,
  ATR and trades); trigger records also carry D8 passages.
- *Why per record:* #27 gets a 1:1 join for about 7 % duplicated path data:
  36 096 trigger records against 525 599 baseline records in the #12 year
  (ADR-054 *Acceptance*).
- *Rejected:* one row per event plus a separate passage table. Two joins
  for #27, to save 7 %.

### D2. Reference and window

- **Reference** = the record's `state.last_price` (`trade.last_price@1`,
  `Ready`): the last trade at decision.
- **Path** = `Trade` prices only.
- **Window** of horizon H = the trades with a canonical key greater than
  the record's `event` and a time before `event.time + H`: half-open, like
  the ADR-031 bars and the ADR-035 windows. Trades in the decision's
  millisecond that rank after the decision event belong to the label, never
  to the record; the decision trade itself and every trade before it do
  not.
- A window **closes** when an event (of any stream) at or after its end is
  observed: events are delivered in canonical order, so every trade before
  the end has been seen. Whether a trades outage took some of them is known
  only from the trades stream, so a closed window stays censorable until
  its label is emitted (D6).
- *Rejected:*
  - The context anchor as reference. A CVD divergence's anchor is the swing
    price, which can be days old, so the label would include moves that had
    already happened.
  - Mark price as the path. The archive imports no mark-price stream
    (ADR-034 D2), so live and archive labels would differ.
  - Kline high and low. They include the bar's trades from before the
    decision.

### D3. Horizons: H ∈ {5m, 15m, 1h, 4h, 1d}

- These are the bar timeframes trigger evidence is built on (ADR-053):
  effort at 5m / 15m / 1h; SFP, CVD divergence and OI expansion at
  15m / 1h / 4h / 1d; exhaustion on its 1h expansion; the auction ids
  against prior-day value. So every trigger id has a horizon at its own
  scale, and baseline records carry the same set, which keeps every
  conditional-vs-baseline contrast like for like.
- *Rejected 1m:* it is the sampling grid step (ADR-054 D3), and no evidence
  is built at that scale.
- *Rejected anything longer than 1d:* no evidence spans more than 1d, and
  the longest horizon sets both the end-of-data censoring (the last H of
  every replay) and the memory held for open labels (D11).

### D4. Barriers

- **Definition:** upper / lower = reference ± b_H · ATR, with ATR =
  `volatility.atr.1h@1` at decision: the only ATR there is (ADR-033), and
  the one ADR-054 D2 snapshots for #26.
- **Values:** b_H = √(π · H / 32 h), giving 0.09045 / 0.15666 / 0.31333 /
  0.62666 / 1.53499 for the five horizons, stored as
  `BARRIER_MULTIPLE_E5 = [9_045, 15_666, 31_333, 62_666, 153_499]` per 10⁵.
  The offset `b_H · ATR` is rounded half to even to the price unit (1e-8
  USDT, ADR-027). Storing b_H to 10⁻⁵ errs by at most 0.5 · 10⁻⁵ / 0.09045
  < 6 · 10⁻⁵ relative.
- **Derivation:**
  - Under a driftless Brownian null with σ per √hour, the mean range of an
    hour is √(8/π) · σ (Feller), so ATR ≈ 1.596 σ.
  - The mean exit time from ± a is a² / σ². With a = b_H · ATR that is
    b_H² · 8/π hours, so b_H² = π H / 32 h gives a mean exit time of H / 4.
  - The null share of races still open at H is then, to the leading term of
    the exit-time series, (4/π) · e^(−π²/2) ≈ **0.92 %**.
- **What breaks at other values:**
  - Mean exit time H/2 leaves 10.8 % of null races open at H, and H leaves
    37 %: "neither" becomes a large third outcome and the race no longer
    answers "which threshold first".
  - H/6 leaves 0.08 % open, but the race then measures a scale six times
    shorter than its horizon.
- **Why ATR rather than bps:**
  - The regime is the ATR percentile (ADR-017, ADR-033), and the exit time
    of a fixed-bps band scales with 1/σ². In bps, the race outcome and its
    timeout share would mostly restate the regime label.
  - ATR scaling makes the null P(upper first) = ½ and the null timeout
    share the same in every regime.
  - bps outcomes stay derivable: the excursions (D7) are stored in price
    units next to the reference and the ATR.
- *Rejected:*
  - One multiple for every horizon. At 1 ATR the null mean exit is
    8/π h ≈ 2.55 h, so the 5m race would almost never resolve and the 1d
    race would measure about 2.5 h.
  - bps barriers (above).
  - Both units: twice the races, and the bps race only restates the
    regime.
- **Unavailable:** an ATR that is not `Ready`, an offset that rounds to 0,
  or a barrier outside the price range makes the race `Unavailable`. The
  excursions are still computed.

### D5. Tie rule

- A **touch** is a trade at or beyond a barrier: price ≥ upper, or price ≤
  lower. One trade cannot touch both, since upper > reference > lower.
- When the two barriers are first touched in the same millisecond, **the
  earlier canonical key wins**. For trades that key is the exchange trade id
  (ADR-028), the matching sequence.
- The race then sets `same_ms: true` if the other barrier is also touched
  later in that millisecond.
- *Why:* the canonical order is the order everything in the engine is
  defined on, and the trade id is the exchange's own sequence.
- *Rejected:* a `Tie` outcome, which throws away the exchange sequence; a
  fixed "pessimistic" side, which is a direction (ADR-010, the naming rule
  of ADR-054 D1). The flag lets a consumer drop these races, and M3 reports
  how many there are.

### D6. Censoring

A censored label is never dropped: every record gets exactly one label. A
**censor** has a time c and a reason. The uniform rule: a component
resolved **strictly before c** keeps its result; everything else from c on
is unknown.

- **Trades gap.** A `FeedGap` on `Stream::Trades` delivered before the
  label is emitted, with c = max(`gap.start`, decision time). Every horizon
  whose window ends after c is censored (`Censored { at: c, reason: Gap }`),
  open or already closed:
  - a race touched strictly before c keeps its result;
  - the excursions and `last` keep what the trades strictly before c showed
    and the horizon is flagged `censored`, which makes the excursions lower
    bounds;
  - a horizon whose window ends at or before c is complete and not touched
    by the gap (the gap's interval `[start, end]` is closed, the window
    half-open);
  - the gap is applied before the windows that end at or before its own
    ordering time close, because a gap ordered after a window's end
    (ADR-028 orders a gap at its end) may still have started inside it;
  - a window that an event of another stream has already closed is
    censored too, as long as its label is not emitted. A trades outage is
    silent until trades resume (ADR-028), while book, mark-price, OI and
    kline events go on and close windows (D2). Closing proves the window
    complete only provisionally; the label's emission (below) is the point
    after which it is final, and emission waits for the trades stream, so
    this holds for the 1d window and the passages too. Without this,
    whether a window ending inside an outage is marked would depend on
    whether some other stream ticked between its end and the gap, and it
    would come out as fully covered (`Neither`, or a race winner that
    missing trades may have preceded). The trades it needs are still held:
    they reach back to the oldest unemitted label (D11);
  - a horizon already censored keeps the earliest censor: a later censor
    with an earlier c replaces it, one with a later c changes nothing;
  - a gap that started before the decision but is delivered after it (a
    late detection, ADR-048) gives c = the decision time: nothing after the
    decision is known;
  - gaps on other streams do not censor: labels read trades only.
- **Stale reference.** If a trades gap was delivered after the last trade,
  at or before the decision, or no trade preceded the decision at all, the
  whole label is censored at the decision (`StaleReference`): the path from
  the reference to the decision is unknown.
- **Rejected trades-stream event.** The driver reports a rejected event to
  the labeler. A rejected event carries no information, its time
  included. One on the trades stream censors the labels not yet emitted
  at that moment at c = max(the last accepted trades-stream event's time,
  decision time) (`Rejected`; with no accepted trades-stream event yet, c
  is the decision time), closed windows included, like a trades gap: the
  engine refused data the path needs, so nothing after the last data it
  accepted is known. It closes no window, does not move the trades-stream
  time that emission and the end of data use (below), and counts for no
  emission. A label it completes is emitted at the next `observe` or
  `finish`. Rejections of other streams are ignored.
  - *Why not its own time:* a rejected trades-stream event can carry any
    time: before the last accepted event (`OutOfOrder`), a repeated or
    regressing exchange id at any later time, an overflow, an inverted gap
    ordered at its end, or a time jump more than `MAX_BARS_PER_EVENT` =
    44 640 one-minute bars (31 days) ahead (ADR-031). The engine refused
    the event, so its time is not evidence that the trades are complete up
    to it, and only the accepted trades-stream events bound what is known
    (the end-of-data rule below rests on the same fact).
  - *Rejected alternative: censor at the rejected event's time*, max(its
    time, decision time), which #26's plan first worded as "censored at its
    time". A time jump refutes it: a decision trade at T0, one accepted
    trade at T0 + 1 s, then a trade rejected as a time jump at T0 + 32 d.
    The censor at T0 + 32 d reaches no window, and treating that time as
    known coverage closes all five windows as fully covered: every horizon
    comes out `Neither`, uncensored, from one second of trades, while the
    same data without the rejected trade gives `EndOfData` at T0 + 1 s. A
    time jump stops the replay or session (the engine's recovery rule), so
    it is the last trades-stream event in practice and would turn up to a
    day of unobserved path of every open label into "neither barrier was
    touched". At its own time a rejection either says nothing (dated
    earlier than the last accepted event) or claims coverage nobody
    observed (dated later); at the last accepted event it says exactly
    what is known, for every kind.
- **End of data.** `finish()` censors every label not yet emitted at
  c = max(the last accepted trades-stream event's time, decision time)
  (`EndOfData`), closed windows included, like a trades gap. The last
  accepted trades-stream event is the latest observed trade or trades gap
  (its end); a rejected one does not count (above); with none, c is the
  decision time. *Why the trades stream:* trades that stop before the
  data does are indistinguishable from an outage no gap announced: an
  outage is silent until the stream resumes (ADR-028), and at the end of
  the data it never resumes. The trades are therefore known to be
  complete only up to the last accepted trades-stream event, and another
  stream's later events prove nothing about them. Censored at the last
  event of any stream instead, a label waiting for the trades stream
  (Emission, below) would come out fully covered whenever another stream
  ticked after its 1d end, and censored when none did. By the uniform
  rule a trade at c's very millisecond does not count: the last
  millisecond may be incomplete. On healthy data, where trades run up to
  the end, this moves c from the last event to the last trade,
  milliseconds earlier.
  Every window that ends at or before c was closed when the event at c
  was observed (an event closes every window ending at or before its
  time), and the censor reaches every window ending after c, so every
  opened label is emitted exactly once, in record order, however the data
  ends, a rejection included. *Rejected:* `finish()` also closing the
  windows ending at or before the last rejected trades-stream event, so
  that a rejection later than every observed event loses no label. That
  closing took the rejected time as coverage; with a time jump it reported
  windows nobody observed as fully covered (the counterexample above).
  With c at the last accepted event there is nothing left for it to close.
- **Trailing gaps: the replay driver's contract (M2).** A provider does
  not deliver a gap still open at its window's end; it lists it in
  `trailing_gaps()` (archive, live and chained replay alike). After the
  provider's last event the driver passes exactly the `Stream::Trades`
  entries of `trailing_gaps()` to `observe`, in canonical order (each
  ends at or before the window's end, after the stream's last delivered
  event: an archive trailing gap ends at the earlier of its last missing
  day's end (plus the stream's source offset) and the window's end; the
  labeler's result does not depend on where it sorts among the other
  streams' last events), passes no other stream's trailing gap to the
  labeler, and then calls `finish()`.
  - With the end-of-data rule above, the trades entries are redundant for
    coverage, not required: a trailing trades gap starts at the stream's
    last delivered event, at or before the end-of-data c, so it cuts the
    same windows at the same time or earlier. Passing it names the cut
    `Gap`, the outage the provider recorded, instead of `EndOfData`.
  - Another stream's trailing gap ends past the data's end and so closes
    windows the data never covered. Under the rule above it changes no
    label, but under a censor at the last observed event it moved that
    time past the data's end and dropped the `EndOfData` censor; labels
    read trades only, so the labeler never takes it.
- **Unavailable stays unavailable**, under every censor: no race existed.
  A passage price equal to the reference (`AtDecision`) counts as reached
  at the decision, so it survives a censor after the decision and not one
  at it.
- **Emission.** A label's **until** is its 1d censor time, or its 1d
  window end when the 1d window is not censored; no other window of the
  label ends, or is censored, later. A label is emitted at the end of the
  first `observe` after which all its windows are closed **and** a
  trades-stream event at or after its until has been observed: a trade or
  a trades gap passed to `observe` (the gap's ordering time, its end); a
  rejected trades-stream event does not count. Labels go out in record
  order, so a label also waits for every earlier one. At the end of the
  data `finish()` emits every remaining label after the end-of-data
  censor (above), which reaches each of their windows that ends after the
  last accepted trades-stream event, closed or not: a complete label still
  waiting for the trades stream comes out censored there, never as fully
  covered. The windows ending at or before that event closed when it was
  observed, so every opened label is emitted exactly once.
  - *Why the trades stream:* ADR-028 orders a gap at its `end` with rank 0,
    so it precedes the stream's first resumed event in that millisecond,
    and the providers open a trades gap at the last trade they delivered:
    the live sequencer and the archive replay emit `FeedGap { start: the
    last delivered trade's time (or the seed or window start), end: the
    resumed event's time }` right before the resumed event, in replay and
    live alike. Gap starts therefore do not fall back behind a delivered
    trade, and once a trades-stream event at or after until has arrived,
    every trades gap that starts before until has been delivered and
    applied. Another stream's event carries no such guarantee: it can
    close the 1d window in the middle of an outage of any length. ADR-031
    decision 4 closes bars on trades-stream events only, for the same
    reason.
  - *Where the guarantee does not hold, the cause reaches back* (below):
    a live hold-back `LateEvent` gap that stands in for a late trade
    (ADR-028 decision 6) starts at that trade's own time, which can lie
    before trades already released; and any provider that breaks the
    ADR-028 contract. A rejected trades-stream event never reaches back:
    it censors at the last accepted trades-stream event, which no emitted
    label's until is later than.
  - *Cost:* a complete label is held until the trades stream resumes; a
    record's own 1d censor makes its until earlier, so a gap that censors
    the 1d window emits the label at once (D11 states the memory bound).
- **Late gaps after emission.** From its emission on a label is
  immutable, like a closed bar (ADR-031); before that every censor above
  still reaches all its windows. A trades gap whose start falls before the
  latest until of the emitted labels reaches back into at least one of
  them. It does not reopen
  any label; it is counted once in `LabelStats.reach_backs`. Unemitted
  labels it reaches are censored as above.
- **`Neither` is an outcome, not censoring:** the window closed with full
  coverage, no censor reached it before its label was emitted, and neither
  barrier was touched.

### D7. Excursions and time to event, per horizon

- `up` = max(0, highest trade − reference), with the time of the earliest
  trade at that highest price; `down` the same on the low side. An
  excursion of 0 (no trade beyond the reference) has no time.
- `last` = the price of the last trade in the window; none for an empty
  window.
- The race's `at` = the time of the touching trade: the time to event.
- The fields are named up/down, not MFE/MAE: favorable and adverse need a
  direction, and direction belongs to the hypothesis (#28) under the
  ADR-054 naming rule.

### D8. Trigger passages (level revisit, invalidation)

Each trigger record gets up to two prices, read from its own `TriggerEvent`
(`evidence_prices`), so they are known at decision. Each row names the
exact field of the evidence:

| Evidence (`TriggerEvent.evidence`) | `level` | `extreme` |
|---|---|---|
| `Sfp(sweep)` | `sweep.level.price` | `sweep.extreme` |
| `Auction(status)` | `status.side == Some(Region::Above)` → `status.vah`; `Some(Region::Below)` → `status.val`; `Some(Region::In)` or `None` → none | none |
| `Candidate(CvdDivergence(event))` | `event.comparison.pair.current.price` | `event.comparison.pair.current.price` |
| `Candidate(OiDivergence(event))` | `event.comparison.pair.current.price` | `event.comparison.pair.current.price` |
| `Candidate(Exhaustion(event))` | none | `event.value.extreme` |
| `Candidate(EffortVsResult(_))`, `Candidate(Absorption(_))` | none | none |
| `OiExpansion(expansion)` | as for `expansion.rejection`: `OiRejection::Sfp(sweep)` as `Sfp`, `OiRejection::FailedAuction(status)` as `Auction` | as for `expansion.rejection` |

- *Why these fields:* the swept level is what an SFP is about and the
  sweep's extreme is how far it went (ADR-037 decisions 6 and 7); the value edge on the
  status's side is the level an auction label refers to (ADR-044 decision
  8); the
  later swing's anchor price is the extreme a divergence compares (ADR-051
  D4); the exhaustion extreme is the price aggression decayed at (ADR-051
  D6). Effort vs result and absorption judge windows, not prices.
- **Passage:** each price gets a first passage within the 1d window. Its
  side is fixed at decision: a price above the reference is reached by the
  first trade at or above it, a price below by the first trade at or below
  it. A price equal to the reference is `AtDecision`. The other results are
  `Touched { at }`, `NotWithin` (the 1d window closed with full coverage)
  and `Censored { at, reason }` (D6).
- **Composition:** "revisited within H" and "extreme reached before the
  opposite barrier" are comparisons of these times with the D4 and D7
  fields, done in #27's views. Which side counts as invalidation is the
  hypothesis's call (ADR-014).
- **Baselines** carry no passages: whether a price p was reached within H
  is read from their excursions.
- `OiDivergence` has no trigger (ADR-053 D7); its row only keeps the
  function total.

### D9. Horizon-overlap thinning (handed over by ADR-054 *Out of scope*)

Each horizon gets a `clear` flag.

- **Baseline:** the first baseline record is clear, then each next one
  whose decision time is at least the last clear one's time + H.
- **Triggers:** the same greedy rule, run over the episode openers
  (`Episode.opener`, ADR-054 D4) of one trigger id. Non-openers are never
  clear.
- **Result:** the clear records of one stream have disjoint outcome
  windows, and for triggers also disjoint inputs (ADR-054 D4).
- The flag reads only decision times and episode flags, so it is not a
  label, cannot leak, and needs no constant beyond H.

### D10. Provenance

- `LABELING_V1 = PipelineKey::new("conditional.labels", 1)`.
- `labeling_text()` names D2–D9: the reference and window rule, the
  horizons, the multiples and their rounding, the touch and tie rule, the
  excursion rule, the censor rules, the passage table and the thinning rule.
  Its FNV-1a 64 fingerprint (`Fingerprinter::write_str`, ADR-029) is pinned
  by a golden test, so a change that forgets the version bump fails a test.
- `LabelProvenance { context: ContextProvenance, labeling,
  labeling_fingerprint }` is delivered once per run. #27 adds the code
  version.
- Any change to D2–D9 is a new labeling version.

### D11. Placement and isolation

- **Pure domain code** (`ForwardLabeler`): no `HashMap`, no clock.
- **API:** `open(&ContextRecord)`, `observe(&MarketEvent, emit)`,
  `reject(&MarketEvent)`, `finish(emit)`, `stats()`. The driver calls
  `observe` with each accepted event first, then `open` with each record the
  sampler emitted at it. The labeler never takes the engine.
- **Window extremes:** one monotonic deque each for the maximum and the
  minimum per horizon over the window of the oldest open label of that
  horizon. Windows close in record order, since decision times do not
  decrease, so a trade costs amortized O(1) per horizon. A censor reads the
  held trades once, newest first, for every open label at once.
- **Pending touches:** two ordered sets keyed by `(price, label, target)`,
  one per side; a touch costs O(log n).
- **Memory:** about one day of open labels and at most one day of trades
  (the trades after the oldest unemitted record's event), plus, during a
  trades outage that runs over a label's until, the labels of the outage's
  first day and the records opened during it: a complete label waits for a
  trades-stream event (D6). No trades arrive meanwhile, so the trades held
  stay within one day, and few records open, since a baseline needs a 1m
  bar close and only trades-stream events close bars (ADR-031 decision 4).
  The held labels go out when the stream resumes (its gap is the first
  trades-stream event) or at `finish()`.
- **Replay only:** the labeler is built by the mie-app replay driver (M2)
  and by tests, never wired into `mie ingest`. The driver ends a replay
  as D6 states: the trades-stream trailing gaps to `observe`, then
  `finish()`.
- **Guard in `tools/check-architecture.sh`:** no file under
  `crates/mie-domain/src`, `crates/mie-ports/src` or `crates/mie-app/src`
  may name `label::` or `ForwardLabel` on any line, or a `label` path
  segment in any `use` item (joined across lines, so renamed, grouped and
  `super`/`self` imports count), except
  `crates/mie-domain/src/conditional/label*` and
  `crates/mie-app/src/conditional.rs`. `mie_domain::conditional` declares
  the module without a re-export, so every use names its path. The script
  runs probes of these import shapes against its own scan first. Features are
  computed only in mie-domain (ADR-019, ADR-025), so this closes the only
  path from a label to a feature. It is a text guard, not a proof; review
  still applies.
- **Walk-forward:** purging and embargo belong to the lab's split
  (#29/#30). Every label carries its decision time and horizons, which is
  what that split needs.

### D12. Where each number comes from

| Number | Source | What breaks at another value |
|---|---|---|
| Horizons 5m, 15m, 1h, 4h, 1d | D3: the bar timeframes trigger evidence is built on (ADR-053) | 1m: the grid step, no evidence at that scale. Longer than 1d: no evidence spans it, and it lengthens the end-of-data censoring and the memory held |
| b_H = √(π · H / 32 h) | D4: null mean exit time H/4, from ATR ≈ √(8/π) σ (Feller) and the exit time a²/σ² | H/2: 10.8 % null timeouts; H: 37 %; H/6: the race measures a scale six times shorter than H |
| 0.92 % null timeouts | (4/π) · e^(−π²/2), the leading term of the exit-time series at H = 4 · mean exit time | — (a prediction M3 reports next to the measured share) |
| `BARRIER_MULTIPLE_E5`, 10⁵ | b_H stored to 5 decimals: relative error < 6 · 10⁻⁵ | 4 decimals: the 5m multiple errs by up to 0.055 %, 3 decimals by up to 0.55 % |
| Offset rounded half to even to 1e-8 USDT | The price unit (ADR-027); half to even has no bias | — |
| ATR = `volatility.atr.1h@1` | The only ATR (ADR-033), snapshotted for #26 (ADR-054 D2) | Another timeframe needs a new engine feature: out of scope |
| 0.181 ATR vs 0.129 ATR (bar labels rejected) | 2 · 0.09045, and ATR · √(1/60) for the null 1m range | — (the reason, not a parameter) |
| 1d passage window | The longest horizon (D3) | Shorter: levels of 1d evidence could not be revisited at their own scale |
| Thinning step = H | D9: disjoint windows of one stream | Shorter: clear windows overlap; longer: fewer clear records than disjointness needs |
| Censor "strictly before c" | Event time has millisecond resolution and the order within the censor's millisecond is unknown (ADR-028) | "At or before c": a trade in a millisecond the gap covers would count |
| ~45 000 model-checked year labels | 36 096 trigger records + 8 760 whole UTC hours of baselines (Accept when) | Fewer: weaker T0; every label: a model as costly as the replay times the window |
| Development year 2025-10-01 … 2026-09-30 | The #12 year every earlier measurement used; nothing is fitted, so no hold-out is needed | — |
| 0 mismatches (T0) | The labeler and the model are deterministic code written from one text, so any difference is a bug | — |

There is no other number.

### Out of scope

- The Parquet writer, DuckDB views, the `mie` subcommand and the code
  version (#27).
- Studies, intervals, the favorable/adverse mapping and counter-hypothesis
  pairs (#28).
- Purging and embargo of walk-forward splits (#29/#30).
- Fees, slippage, funding and R units (`docs/direction.md`).
- Live labeling and egress (#38).
- New ATR timeframes, and any engine, catalog, state-hash or ADR-054 change:
  there is no new sampling version.
- A history-chain driver.
- Reading conditional outcome rates in M3.

## Consequences

- Labels are not Market State: no feature, catalog, lock, state-hash,
  record or engine golden changes, and the ADR-041 equivalence still covers
  everything a label is computed from.
- About 561 000 labels a year (one per record), each with five horizons;
  the effective count is far lower, since baseline windows of consecutive
  minutes overlap at every horizon. Studies use the `clear` records or a
  block bootstrap (#27/#28).
- The last day of any replay is censored at 1d (`EndOfData`), the last 4h at
  4h, and so on: M2's `label_window` lets a caller feed events beyond the
  record window so those labels complete.
- Late gaps after emission leave emitted labels unchanged; the count is
  reported, not repaired. Labels are correct for the data the replay
  delivered in order.
- The Brownian null may not fit BTC: fat tails and the Wilder lag of the ATR
  can push the measured `Neither` share well above 0.92 %. M3 reports it
  next to the prediction; a large miss means a new labeling version from a
  stated reason, not a refit (no hold-out is left, ADR-053 D7).
- In archive replays every label reads the same trades as live (the path is
  trades only), so archive and live labels differ only where the records
  differ (ADR-054 Consequences).
- A label's `Display` line is canonical and pinned on the candidate golden
  tape.

## Alternatives considered

- **Labels from 1m bars.** Bands narrower than a bar's range and bars that
  straddle the decision (Approach). Rejected.
- **Labels as engine features.** The leakage path (Approach). Rejected.
- **bps barriers, or both units.** The race would restate the regime (D4).
  Rejected; bps stays derivable.
- **One ATR multiple for all horizons.** Short races never resolve, long
  ones measure the wrong scale (D4). Rejected.
- **A `Tie` outcome or a fixed side on ties.** Loses the exchange sequence,
  or is a direction (D5). Rejected.
- **The context anchor, mark price or kline extremes as reference or path.**
  Old moves, no archive stream, or pre-decision trades (D2). Rejected.
- **Reopening emitted labels on late gaps.** Labels would no longer be a
  stream; counted instead (D6). Rejected.
- **A window final when it closes.** An event of another stream would then
  decide whether a window ending inside a silent trades outage is censored
  or reported as fully covered (D6). Rejected: a window is final when its
  label is emitted.
- **A label emitted when its last window closes.** For the 1d window and
  the passages, closing and emission would be the same event, so an
  event of another stream at the 1d end would again decide whether a trades
  outage over it is censored, even a ten-minute one. Rejected: emission
  waits for a trades-stream event at or after the label's until (D6).
- **A rejected trades-stream event censoring at its own time.** Its time
  is the one value the engine refused; a time jump at the end of the data
  then reported up to a day of unobserved path of every open label as
  fully covered (D6). Rejected: a rejection censors at the last accepted
  trades-stream event.
- **One label per event.** Two joins for #27 (D1). Rejected.
- **Passages in a follow-up issue.** The issue names level revisit and
  invalidation; as neutral passages they need no hypothesis (D8). Rejected.

## Accept when

M3 runs `label_measure` (M2) on the #12 year, 2025-10-01 … 2026-09-30, and
on the ADR-051 R0 live store:

- **T0 (asserted):** 0 mismatches against an independent brute-force model
  written from this text:
  - on the live store, every label;
  - on the year, every trigger record's label plus the baseline labels
    whose `minute_end` falls on a whole UTC hour: about 45 000 labels,
    chosen by decision time only, never by outcome, so the model costs
    about as much as the replay.
- **Completeness (asserted):** exactly one label per record, in record
  order.
- **Reported, no threshold:**
  - per horizon, the `Neither` share (next to the 0.92 % null prediction),
    the censored shares by reason, `Unavailable` and `same_ms`;
  - `reach_backs` and the clear counts;
  - wall time and peak RSS.
- **Deliberately not reported:** upper-vs-lower shares, excursion or time
  quantiles, and any split by trigger, regime or location. No outcome is
  read before #27/#28 and the calibration slice of `docs/direction.md` are
  fixed.
- **Reachability:** every criterion runs on stores that exist today: the
  #12 year is imported, and the R0 live store holds a cleanly ended run.
