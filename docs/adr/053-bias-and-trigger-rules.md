# ADR-053: Bias and trigger rules — a non-authorizing bias context and one versioned rule per trigger family

- Status: accepted
- Date: 2026-10-18

## Context

#24 asks to separate what the evidence suggests (bias) from where price is
(location) and from the observable event that may authorize action
(trigger), as brief §11 and ADR-012 require. Its scope:

- a bias representation that is structured context and explicitly
  non-authorizing;
- trigger events for each `TriggerFamily` of
  `crates/mie-domain/src/trigger.rs` — failed auction, SFP, absorption,
  exhaustion, effort vs result, CVD divergence, OI expansion followed by
  rejection, failed reclaim, breakout + acceptance — as deterministic,
  versioned rules on top of the candidates and location;
- the same bias and location tested against several trigger families (the
  brief §11 example);
- no execution semantics: triggers feed Conditional Behavior and Research
  only.

Its acceptance criteria: each family has a documented rule and synthetic
tests, and trigger events carry location, family, parameter version and the
evidence they used.

What exists, and what each family is built from:

- The auction classifier `location.auction.prior_day@1` with its
  `LocationEvent::Auction` facts (ADR-044 D8, D9): the facts of failed
  auction, failed reclaim and breakout + acceptance.
- The structure facts `StructureEvent::Sweep`, `Sfp` and `Break` of
  `structure.levels.<15m|1h|4h|1d>@1` (ADR-037 D6, D7): the fact of SFP.
- The order-flow candidates `candidate.*@1` and their `CandidateEvent`s
  (ADR-051): the facts of CVD divergence, effort vs result, exhaustion and
  absorption. ADR-051 left "the trigger rule of each `TriggerFamily`" and
  "location gating" to this issue, and kept `OiExpansionRejection` a #24
  rule: the OI divergence feeds no family directly.
- The 5-minute open-interest grid `derivatives.oi.5m@1` (ADR-042 D2),
  produced by both the archive and live capture. Nothing on main judges
  "expansion".
- The location context of ADR-051 D3 (`LocationContext`), the volatility
  regime `volatility.regime.1h@1` (ADR-033), the Market State hash
  (ADR-041), the feature registry with its lock and goldens (ADR-029), and
  exact fixed point (ADR-027).

This ADR decides the bias representation, nine trigger rules, a location
gate for the candidate families, a definition of OI expansion and an
extension of ADR-041's field list. The accepted ADRs 037, 041, 042, 044 and
051 are not edited; this ADR records the extensions, as ADR-051 D1 did for
ADR-041.

Delivery is in four PRs under #24: M1 ships this ADR in full, the framework,
the bias and the families of D5; M2 the candidate families of D6; M3 the OI
family of D7 and the measurement tool; M4 records the measurement of
*Accept when* here and flips the status.

## Decision

### Approach

Two feature families join the catalog: `bias` and `trigger`. They are
computed by one infallible trigger stage that runs last in
`MarketStateEngine::apply`, after the candidate stage, on the committed
state. Each family gets versioned trigger features
`trigger.<family>[.<qualifier>]@1`. A rule reads only the facts the same
event already produced (structure, location and candidate events) plus the
committed state, and emits `TriggerEvent`s that carry the rule's feature key
(the parameter version), the family, a `LocationContext`, a `Bias` snapshot
and the evidence fact. The bias (`bias.context@1`) is recorded and read by
no rule, so one bias and location can be tested against every family.

*Rejected:* a separate trigger-rule registry outside the feature catalog,
with events only. It would duplicate ADR-029's lock, fingerprint and
input-closure machinery; triggers would sit outside the state hash, so the
ADR-041 live/replay equivalence check, which compares state hashes and not
events, would not cover them; and an experiment spec's `features` set could
not pin a trigger version.

### D1. Facts and exposure

- **State field.** `MarketState.triggers` is appended as the **last**
  field, after `candidates`. *Why last:* ADR-041 D2 then holds unchanged.
  Every field encoded before it is written as before, with no encoding
  bump, and the new feature-set version makes old and new hashes
  incomparable. A test pins that the engine still reproduces the two
  encoding-v1 goldens pinned before #24 once the field is left out and the
  pre-#24 feature-set stamp is put back (the ADR-051 D1 pattern). Later
  milestones append their values at the end of `Triggers`.
- **`TriggerFamily`** gets state-hash codes in declaration order —
  FailedAuction 0, Sfp 1, Absorption 2, Exhaustion 3, EffortVsResult 4,
  CvdDivergence 5, OiExpansionRejection 6, FailedReclaim 7,
  BreakoutAcceptance 8, frozen — a snake_case `Display` and `ALL`.
- **Events.** `MarketStateEngine::trigger_events()` exposes the trigger
  events of the last accepted event. They are not hashed, the same as
  structure, location and candidate facts: the values they follow from are
  state.
- **Values.** The value of each trigger id is its last trigger: the
  `known_at`, the anchor price and the evidence, in the hashed form its
  input feature already defines (`AuctionStatus`, `Sweep`, and in M2
  `CvdComparison`, `EffortResult`, `Exhaustion`, `Absorption`). The OI
  family's evidence is `OiExpansion` (M3): its rejection in that form
  (`Sweep` or `AuctionStatus`), both `OiGridPoint`s and the change (D7).
  It is `None` until the first trigger. The
  `LocationContext` and the `Bias` are carried by events only: they are
  reads of other hashed features (the ADR-051 D3 reason), and a value that
  repeated them would hash the same fact twice.
- **Warm-up.** `WarmUp::None`: a trigger has no lookback of its own. The
  value is `WarmingUp` or `Unavailable`, with the same counts or reason,
  exactly while its fact input is (for example the auction ids every UTC
  day until the first close under the new prior day); otherwise it is
  ready and holds the last trigger, which survives the input's warming
  phases. One event can emit an id more than once (a trade can sweep
  several levels of one timeframe, and one close can resolve them all);
  the value then holds the last in emission order (D2).
- **Naming.** The ADR-051 rule extends here: no type, field or `Display`
  text says bullish, bearish, long, short, signal, reversal, continuation
  or entry. No execution semantics (ADR-010): no price targets, stops or
  sizes. The only sided fields are the facts' own (`structure::Side`,
  `location::Region`, `event::Aggressor`, `book::Side`).

### D2. The stage

- **Placement.** The trigger stage runs after the candidate stage in
  `MarketStateEngine::apply`, on the committed state of the same event, and
  cannot fail. *Why last:* every input it reads — the structure, location
  and candidate facts of the event, the registry for the context, the
  regime and the divergences for the bias — is final only once the
  candidate stage has run.
- **Reads only** the event's `structure_events`, `location_events` and
  `candidate_events` plus the committed `MarketState`. It never reads
  another trigger, so the order of the families cannot change a value.
- **Tracker.** The last trigger of each id is engine state, mutated in
  place; the events go to a scratch buffer swapped in once the event is
  accepted, as in the other stages. A rejected event never reaches the
  stage, so it leaves the tracker, the values and `trigger_events()` as
  they were.
- **Emission order** (total, pinned by a test):
  1. structure facts in their order: each `Sfp` trigger, followed by its
     OI-expansion trigger if any (M3);
  2. location facts in their order: each auction trigger, a failed auction
     followed by its OI-expansion trigger if any (M3);
  3. candidate facts in their order (M2).
- **Visibility time** is the `known_at` of the input fact: the time of the
  current event, never a bar end. There is no look-ahead beyond the input's
  own confirmation delay.

### D3. Bias `bias.context@1`

- **Components:**
  - `regime`: a copy of `volatility.regime.1h@1`, the label and the raw
    percentile (brief §11's "HIGH volatility"). A copy, so no arithmetic
    and no new float: the percentile is the half-step value ADR-033
    already stores.
  - `swings`, per structure timeframe (15m, 1h, 4h, 1d): for each side,
    the step of the last swing pair — `Higher`, `Equal` or `Lower` from the
    sign of its `price_change` — with the later swing's bar time. It is
    read from `candidate.divergence.cvd.<tf>@1`, the only state that holds
    consecutive swings, and uses its price facts only (never the CVD).
- **The location half** of brief §11 is the event's `LocationContext` and
  is not duplicated in the bias. Brief §11's three parts map one to one:
  bias = `Bias`, location = `LocationContext`, trigger = family + rule key
  + evidence.
- **Explicitly non-authorizing:** no aggregate direction or score, no
  method that returns an action, and no trigger rule reads it (a test pins
  this: two states that differ only in the bias inputs give the same
  triggers, apart from the snapshot).
  - *Why no aggregate:* its weights would be unmeasured numbers (ADR-013).
  - *Why no order flow:* aggression is not direction (ADR-023).
  - *Why no trend/range label* although brief §11's example names "Range":
    that dimension is deferred until validated (brief §9); the swing steps
    are the facts such a label would be built from.
  - *Rejected for `@1`:* price against the VWAP, funding and basis. None has
    a measured contribution yet (ADR-013); a `bias.context@2` can add one
    once #28/#31 show one.
- **Recomputation.** `Bias::read(&MarketState)` is pure and public, so #25
  can snapshot unconditional baseline samples with the same definition.
  The stage recomputes it after every event. *Why every event rather than
  at the closes that move its inputs:* it is a copy of about 100 bytes,
  identical in result to a close-driven schedule (its inputs change only
  at those closes), and simpler to verify.
- **Inputs:** `volatility.regime.1h@1` and
  `candidate.divergence.cvd.<15m|1h|4h|1d>@1`. **Parameters:** `step_rule`
  = `sign_of_price_change`.
- **Warm-up:** `WarmUp::None`; each component carries its own
  `FeatureValue`, copied from its input.

### D4. Location (ADR-012: a trigger occurs at a defined location)

- **Every trigger event carries a `LocationContext`**, the ADR-051 D3 type,
  built with the same crate-private `LocationContext::read`: the anchor
  price, the registry levels whose padded zone contains it, and the auction
  state.
- **Auction and structure facts are at their location by definition.** The
  anchor is the level the fact is about:
  - Failed auction and breakout + acceptance anchor at the crossed edge:
    VAH for a probe toward Above, VAL for one toward Below.
  - Failed reclaim anchors at the edge between the accepted region and
    value: VAH when Above is accepted, VAL when Below is.
  - An SFP anchors at the swept level's price. Its context leaves out every
    registry level derived from the swept swing, on every structure
    timeframe: each level of any `structure.levels.<tf>` source with the
    swing's side and price whose swing bar nests with the swept swing's bar
    (one bar lies within the other). That is its structural level, and the
    prior sweep and SFP zone it became, in the registry that swept it and in
    every other registry that holds the same extreme. *Why:* the ADR-051 D3
    reason. The swept swing is the fact, not a place it happened at, and
    the event that resolves the SFP also enters the swing's SFP zone into
    the registry, so every context would list it. One extreme can be a
    swing on several timeframes: a 1h swing low at P whose 15m bar inside
    that hour also has its low at P is one price seen on two grids.
    Excluding only the sweeping registry's levels left the swing in the
    context through the other timeframes: on the trigger golden tape, 202
    of 515 SFPs, 104 of them with a level of a kind the candidate gate
    below admits.
    - *Why this key:* it names the extreme without a timeframe. A registry
      level carries no time of the extreme trade, only the open of its own
      timeframe's swing bar, so side, price and nested bar is the finest
      identity it holds. It is exact: the structure timeframes share the
      UTC grid, so two of their bars either nest or do not overlap, and two
      nested bars with the same extreme on the same side have that price
      inside the shorter bar, which the longer one contains. Two shorter
      swings at the same price inside one longer swept bar are both left
      out: inside the swept bar they are the same price on the same side,
      not another place.
    - *Rejected:* the same source, side and swing bar only (the
      ADR-051 D3 key as written; it keeps the swing through the other
      timeframes, as above); side and price alone (it would also drop an
      older swing at the same price outside the swept bar, which is a place
      price came back to).
- **Candidate families (M2): gate.** A candidate is a trigger only if its
  own context lists at least one level of kind `Poc`, `Vah`, `Val`, `Hvn`,
  `Lvn`, `StructuralHigh`, `StructuralLow`, `SfpRejectionZone` or `Vwap`.
  If the context levels are not `Ready`, there is no trigger.
  - *A divergence's own swings do not pass the gate.* A CVD divergence is
    a pair, the swing just confirmed and the previous swing of its side,
    and the comparison of the two is the candidate. Its recorded context
    keeps ADR-051 D3's exclusion (its own registry, its current swing; a
    merged definition). The gate reads the registry at the current swing's
    price again and skips every level that the SFP key above names for
    either swing of the pair: any `structure.levels.<tf>` source, that
    swing's side and price, a swing bar that nests with that swing's bar.
    The trigger event carries the context the gate read.
    - *Why the current swing:* else a 1h CVD divergence would pass the gate
      on the 15m structural high or SFP zone of its own swing, the
      self-reference the SFP rule removes.
    - *Why the previous swing:* it is the other half of the comparison, so
      it is the candidate too, not a place (the ADR-051 D3 reason). The
      current swing is a strictly further extreme, so the first trade
      beyond the previous swing swept that swing's level in every registry
      before the divergence is confirmed (ADR-037 D6). What remains of it
      is its prior sweep, which the gate does not admit, and, if the sweep
      closed back inside, its SFP zone: from the previous swing's price to
      the sweep's extreme (ADR-037 D7), padded by the registry tolerance of
      6 bps of that price (ADR-044 D3). That zone contains the current
      swing's price in two ways, and neither is a place. When the move
      that made the current swing is the sweep, the zone reaches the
      current price because that price is the sweep's extreme: the
      containment holds by construction. Otherwise it holds when the
      current swing lies within 6 bps beyond the zone: a bound on the
      candidate's own price change, set by a padding nobody measured for
      that purpose (ADR-013).
    - *Measured* on the candidate golden tape, a synthetic 45-day random
      walk (a test pins the counts): 257 CVD divergences; 120 list an
      admitted level of their previous swing at the anchor, each an SFP
      zone and none a structural level; 248 pass the gate with the current
      swing skipped, 243 with both. Of the 5 that the previous swing alone
      decides, the zone reaches the current price in 1 and only its
      padding does in 4. The counts show the rule is not vacuous on that
      tape, not how often it binds on BTC; *Accept when* reports that.
    - *A swing outside the pair stays a place,* even when the candidate's
      own move made its SFP zone: it existed before that move and is not
      part of the comparison, and its zone records that the move went
      beyond it and was rejected. On the same tape 3 of the 257 pass the
      gate only on such a zone (one that resolved at or after the current
      swing's anchor trade).
    - *Rejected:* admitting the previous swing (the gate would pass on the
      candidate's own comparison base, by construction or through a 6 bps
      bound on its own price change); skipping every SFP zone that
      resolved at or after the current swing's anchor trade (it also drops
      swings outside the pair, which are places; it still admits the
      previous swing's older zones that reach the current price only
      through the padding; and a context level carries no sweep or
      resolution time, so ADR-051's `ContextLevel` would need a new field);
      skipping the previous swing's zone only when it reaches the current
      price unpadded (it keeps the padding cases, the 6 bps bound).
  - For effort vs result, exhaustion and absorption the gate reads the
    candidate's own context unchanged, and the trigger event carries it:
    their anchor is the last 1m close, not a swing, so no registry level is
    derived from the fact (ADR-051 D3).
  - *Why a gate at all:* ADR-012 requires a trigger at a defined location;
    a candidate is defined anywhere (ADR-051), so its trigger rule is where
    the location condition goes.
  - *`LiquidityCluster` excluded:* clusters are live only, and lie within
    5 bps of mid by construction (ADR-043 D8). They would pass nearly every
    live candidate and no archive candidate, so the gate would mean
    different things live and in archive replays (ADR-019).
  - *`PriorSweep` excluded:* these are levels price has already traded
    through, and the most numerous kind (p50 80 per closed minute, ADR-044
    M3). A gate on them measures proximity to old prices.
  - *`ValidatedReference` excluded:* it has no source until #33; admitting
    it then is a new version, decided with its source.
  - Every level stays in the event's context, so research can still split
    by kind. The gate's pass rate was unknown until M4 measured it: 88.9–94.8 %
    of the candidates of the #12 year pass (see Consequences and
    *Acceptance*).
  - *Rejected:* any registry level (clusters split live from archive, and
    prior sweeps make the gate nearly vacuous); only value edges and
    structural extremes (ADR-051 R1's `neither` bin "holds most episodes",
    and R1's floor already binds at the bottom of the `q_hi` grid, so it
    would drop most candidates and leave #28 cells unpowered).
- **No `location.levels@N` input.** No trigger definition declares one: with
  that input, `with_features(location.levels@1)` would leave the trigger
  without its upstream (the ADR-051 D3 reason, ADR-044 D10). The context
  names the registry it read, and the feature-set version names the
  selection. `location.auction.prior_day@1` is declared: the context copies
  it, and it is not selectable. The candidate trigger values therefore
  depend on the registry an engine selected: a `location.levels@1` engine
  can gate a candidate differently from the default one. The feature-set
  version tells the two apart (ADR-041 D2); the #96 isolation check (rule
  I of `location_measure`) compares every trigger value except the
  candidate families'.

### D5. Rules on location and structure facts (no new numbers)

- **`trigger.failed_auction@1`:** a `LocationEvent::Auction` with `to ==
  FailedBreakout` and `status.failure.at == known_at` (the failing close).
  Evidence: the event's `AuctionStatus`.
- **`trigger.failed_reclaim@1`:** a `LocationEvent::Auction` with `to ==
  FailedReclaim` and `status.failure.at == known_at`.
- **`trigger.breakout_acceptance@1`:** a `LocationEvent::Auction` with
  `from == Some(Breakout)` and `to == Acceptance`: the `N_acc`-th counted
  close of the probe (ADR-044 D9).
- The auction rules read the **event's** `status`, never
  `MarketState.location.auction`. *Why:* at the UTC-day rollover the state
  already shows the new reference, and the old day's last close is visible
  only in its event (ADR-044 D2).
- **`trigger.sfp.<15m|1h|4h|1d>@1`:** a `StructureEvent::Sfp` of that
  timeframe. Evidence: the resolved `Sweep`. A `Break` is no trigger.
- **Why no extra confirmation delay or later-reclaim horizon** (ADR-037 D7
  left the horizon to #24): the confirmation depth is a research variable
  (ADR-015, #31), and a fixed horizon would be a number nobody measured.
  The facts already carry the confirmation their own definitions fix
  (ADR-044's `N_acc` and failure rule, ADR-037's SFP window).
- **Parameters:** `anchor_rule` = `crossed_edge` (failed auction, breakout
  + acceptance), `accepted_edge` (failed reclaim), `swept_level` (SFP); the
  SFP ids also `timeframe_ms`. **Inputs:** the fact feature
  (`location.auction.prior_day@1` or `structure.levels.<tf>@1`),
  `location.auction.prior_day@1` for the context, and `bias.context@1` for
  the snapshot.

### D6. Rules on candidates (M2)

`trigger.cvd_divergence.<15m|1h|4h|1d>@1`,
`trigger.effort_result.<5m|15m|1h>@1`, `trigger.exhaustion@1` and
`trigger.absorption.<5m|15m|1h>@1`.

- Each is the candidate event — the divergent comparison, or the rising
  edge of an evaluated candidate (ADR-051) — plus the D4 gate (for a
  divergence on the context without both swings of its pair, for the
  others on the candidate's own context). No new number: the candidate's
  own definition carries every threshold.
- The event carries the full `CandidateEvent` as evidence.
- The absorption ids are live only, as their input: they stay warming up in
  archive replays.
- *Why the timeframes and windows:* they are the candidates' own (the
  structure timeframes of ADR-037 D1 and the aggression windows of ADR-035);
  a trigger on another horizon would need a candidate on it first.
- **Value:** the last trigger, with the candidate's hashed value as
  evidence (`CvdComparison`, `EffortResult`, `Exhaustion`, `Absorption`)
  and its anchor: the current swing's price for a divergence, the last
  close otherwise. A candidate the gate stops leaves the value as it was.
- **Parameters:** `gate_kinds` =
  `poc,vah,val,hvn,lvn,structural_high,structural_low,sfp_zone,vwap` (the
  admitted kinds of D4, in `LevelKind` declaration order); `gate_skip` =
  `swing_pair` for the CVD ids and `none` for the others (D4); the CVD ids
  also `timeframe_ms`, the effort-vs-result and absorption ids
  `window_ms`. *Why the gate is a parameter:* any change to it (admitting
  `ValidatedReference` once #33 gives it a source, say) must be a new
  version, and only a parameter puts it in the fingerprint (ADR-029).
  **Inputs:** the candidate feature (`candidate.divergence.cvd.<tf>@1`,
  `candidate.effort_result.<w>@1`, `candidate.exhaustion@1` or
  `candidate.absorption.<w>@1`), `location.auction.prior_day@1` for the
  context, and `bias.context@1` for the snapshot.

### D7. OI expansion followed by rejection (M3)

`trigger.oi_expansion_rejection.<15m|1h|4h|1d>@1` (the rejection is that
timeframe's `Sfp`) and `trigger.oi_expansion_rejection.prior_day@1` (the
rejection is the D5 failed auction).

- **Start point:** the point at the event (below) that starts the
  excursion beyond the level.
  - For an SFP: the `StructureEvent::Sweep` (the sweeping trade). Recorded
    under the key (timeframe, side, `swing_time`) and dropped at the `Sfp`
    or `Break` resolution. A sweep without a point leaves no record, so its
    SFP has no comparison.
  - For a failed auction: the 1m close at which a probe toward Above or
    Below starts or retargets. **The hook:** after the location facts, the
    stage reads the committed `location.auction.prior_day@1` status and
    records its probe the first time it sees the probe's identity
    (`reference_start`, `toward` ∈ {Above, Below}, `since`), with the point
    at `since`.
    - *Why that is the probe's close:* the status changes only at a
      classified 1m close, and an event classifies at most one close with
      trades (ADR-031), so a new identity is seen at its own close. A later
      event at the same millisecond (an open-interest sample) sees the same
      identity and records nothing. A time test (`status.known_at` equal to
      the event time) would record such a follower again, after its sample
      moved the grid. A retarget between Above and Below keeps the
      `Breakout` label and so emits no `Auction` event; that is why the
      hook reads the status, not the facts.
    - *One record at a time.* A failed auction consults it — the same
      reference and `failure.toward` — before the hook updates it in the
      same event. The hook keeps it while the committed probe keeps its
      identity, replaces it on a new identity (a new probe or a retarget),
      and drops it when the committed status holds no probe toward Above or
      Below (failure, acceptance, a probe toward value, warming up,
      unavailable, a new reference).
    - *Rollover.* The old day's last close is classified before the switch
      (ADR-044 D2). A failure there is an auction fact, and it finds the
      record before the hook drops it. A probe started there is never
      recorded, since the committed status already shows the new
      reference, and it cannot fail.
    - *Live and archive alike:* the hook reads committed state only, on one
      code path, rebuilt by replay (ADR-019, ADR-052). It adds no number
      and no branch.
    - *Rejected:* closed bars as an input of the stage (D2 reads the
      event's facts and the committed state only); a location fact for
      retargets (it would edit the accepted ADR-044).
- **End point:** the point at the rejecting event.
- **The point at an event** at time `t` is the held ready
  `derivatives.oi.5m@1` point of the boundary
  `⌊(t − 25 000 ms) / 300 000 ms⌋ · 300 000 ms`, else none. That boundary
  lies in `[t − 325 000, t − 25 000]` ms. The stage holds the last two
  ready points it saw: the boundary read is at most one grid interval
  before the newest one closed by `t`, so two suffice. A boundary that
  closed unavailable, or not at all, has no point.
  - *Why:* every structure bar end is a 5-minute boundary and SFPs resolve
    at bar closes; every fifth 1m close, where failures happen, is one too.
    The archive orders a boundary's point at the boundary itself (`metrics`
    rows stand at `create_time + 5 min` and measure that instant, ADR-042
    *Acceptance* 1), so the trade that closes the bar sees it. A live feed
    closes the boundary at its first 10 s poll after it (spacing p50
    9 996 ms, so about 5 s later on average), while the closing trade comes
    about 50 ms after the bar end (603 323 345 trades in the #12 year,
    about 19 per second). Reading the newest point, the archive would
    compare up to the bar-end boundary at nearly every SFP and live up to
    the one before: one id, two meanings (ADR-019). Reading the boundary
    25 s before the event, both sources select the same boundary at every
    start and end while the live feed is continuous; `resolution_ms` still
    tells their values apart.
  - *Derivation of 25 000 ms:* the longest live spacing ADR-042 D1 still
    calls continuous, the 10 000 ms resolution plus the 15 000 ms
    `step_tolerance_ms`; the longest spacing measured on the live runs of
    ADR-042 *Acceptance* is 15 049 ms. *Smaller:* a slow but continuous
    live feed may not have closed the boundary yet, so live would lose
    points the archive has. *Larger:* the points drift away from their
    events and nothing is gained in agreement.
  - *Cost:* live loses about nothing, since it already reads the boundary
    before the bar end. The archive loses the comparison of an SFP that
    resolves in its first window bar when the sweep fell in the bar's last
    275 s (300 000 − 25 000 ms): the start then reads the same boundary as
    the end. By ADR-037 S3, the first window bar resolves 49.43 / 64.62
    = 76 % of 15m SFPs, 50.15 / 63.69 = 79 % on 1h and 52.75 / 64.25 =
    82 % on 4h; with the sweep time spread evenly over the bar (S3 puts its
    p50 at 0.51–0.61 of the bar), that is 76 % × 275 / 900 ≈ 23 % of 15m
    SFPs, ≈ 6 % on 1h and ≈ 2 % on 4h. *Measured* (M4, #12 year, shares of
    all SFPs of the timeframe, whose outcomes sum to the SFP count): 765 of
    4 267 15m SFPs = 17.9 %, 34 / 1 063 = 3.2 % on 1h, 3 / 257 = 1.2 % on
    4h and 0 / 44 on 1d, below the estimates (23 / 6 / 2 %) at every
    timeframe. The newest-point rule instead loses
    none in the archive and, live, every first-bar SFP swept in the bar's
    last 300 s, where the live end still reads the boundary before the bar
    end: 76 % × 300 / 900 ≈ 25 % of 15m SFPs.
  - *Measured* on the candidate golden tape, a synthetic 45-day walk with
    archive open interest (a test pins the counts): 398 15m SFPs, of
    which the newest-point rule fires 197, none on a shared boundary, and
    this rule 195, with 12 shared boundaries; on 1h 96 SFPs, 48 against 46
    (1 shared); on 4h 21, 9 and 9. Only 122 of the 398 15m SFPs resolve
    within 25 s of their bar end, where the two rules read different end
    boundaries: the tape's trades are sparse, so it understates the shift
    on BTC, where nearly every SFP does.
  - *Weak spots:* the BTC figures above are rate estimates (the 34 h live
    store holds too few SFPs to measure parity); an expansion only in the
    last interval before the rejection is invisible in both sources; and
    it is one new number.
  - *Rejected:* the newest ready point within an age bound of 360 000 ms
    (one grid interval plus `max_age_ms`), the rule this ADR first stated:
    archive and live read different boundaries at nearly every SFP, as
    above. Reading the end after the rejection: visible only after the
    fact (D2). Rebuilding the previous point from `delta`: a delta carries
    no `sample_time` and exists only between ready neighbours.
- **Expansion condition:** both points are present, have the same
  `resolution_ms` (no comparison across an archive/live seam), `end.boundary
  > start.boundary`, and `end.open_interest > start.open_interest`
  (strict, compared exactly in `i128`).
  - *Stated limit:* an excursion whose start and end read the same
    boundary, a shared boundary, has no comparison. M4 reports how often
    that happens, per OI id.
  - *Overflow:* a change that does not fit a `Qty` (possible only with
    negative open interest) is no trigger and leaves the value as it was.
    *Rejected:* `Unavailable(OutOfRange)`, which would give the value a
    validity of its own, while D1 has it follow its fact input.
- **Unsigned:** open interest counts both sides' positions, so the rule is
  the same for a probe above and below (the ADR-051 D4 reason).
- **No magnitude threshold in `@1`** (ADR-013). The event carries both
  points and the change, the span between them is their boundaries, so
  #28/#31 can threshold on measured contribution.
  - *Rejected:* a tail quantile against a rolling baseline ("expansion"
    meaning unusual). Fixing the two effort numbers took a pre-registered
    tool, two rules and a hold-out year (ADR-051 D9); that hold-out year has
    now been read, so no clean data is left to fix a third.
  - *Rejected:* the OI at the swing anchor. On 4h/1d it can be days old, so
    it measures the move before the excursion, not the excursion.
- **Value:** the last trigger: its `known_at`, the rejection's anchor (the
  swept level's price, or the crossed edge) and the evidence `OiExpansion`
  — the rejection (`Sweep` or `AuctionStatus`; one event can resolve
  several SFPs, so the value names which), both `OiGridPoint`s and
  `change: Qty`. The value follows the rejection's fact input
  (`structure.levels.<tf>@1` or `location.auction.prior_day@1`), not the
  grid (D1): a point that is missing means no trigger, not an invalid
  value. The event directly follows its rejection's trigger, with the same
  anchor, context and bias (D2).
- **The #96 isolation check compares the OI values.** They read no
  registry: their facts and points do not, and only their events' contexts
  do, which are not values. `Triggers::equal_but_registry` therefore
  places them on the compared side. *Rejected:* leaving them out, which
  would hide a future registry read in this rule.
- **Why these two rejections:** brief §11 lists this family next to failed
  auction and SFP as triggers at a value edge, and those two are the facts
  on main that mean "beyond a level, then back". A failed reclaim is a
  different auction context (returning into value); it gets its own id if
  #28 asks for one.
- **Parameters:** `compare_rule` = `strict`, `oi_lag_ms` = 25 000,
  `rejection` = `sfp` or `failed_auction` (text); the SFP ids also
  `timeframe_ms`. **Inputs:** the fact feature (`structure.levels.<tf>@1`,
  or `location.auction.prior_day@1` for the failed auction),
  `derivatives.oi.5m@1` for the points, `location.auction.prior_day@1` for
  the context, and `bias.context@1` for the snapshot.
- **State:** engine state, rebuilt by replay; ADR-052's warm start replays
  from the origin, so a warm-started engine holds the same records. One
  start record per pending sweep that had a point (so at most as many as
  the structure registries' pending sweeps), one probe record, and the
  last two ready points.

### D8. Where each number comes from

| Number | Reason | Kind |
|---|---|---|
| SFP timeframes 15m, 1h, 4h, 1d | The structure timeframes: swings, sweeps and SFPs exist only there (ADR-037 D1) | inherited ids, own reason |
| Candidate timeframes 15m, 1h, 4h, 1d and windows 5m, 15m, 1h | The candidates' own ids (ADR-051 D4–D7); a trigger on another horizon needs a candidate on it first | inherited ids, own reason |
| `N_acc` of breakout + acceptance | None of its own: the rule reads ADR-044's acceptance label, whose count is that ADR's | no new number |
| No confirmation delay or reclaim horizon | Confirmation depth is a research variable (ADR-015, #31) | no new number |
| Excluded gate kinds `LiquidityCluster`, `PriorSweep`, `ValidatedReference` | Live-only within 5 bps of mid (ADR-043 D8); traded-through and the most numerous kind, p50 80 of 256 per closed minute (ADR-044 M3, `@2`); no source until #33 | derivation + measurement |
| Admitted kinds' density (p50 per closed minute: SFP zones 51, structural highs 48 / lows 29, HVN 22, LVN 20, POC/VAH/VAL 2 each, VWAP 1) | ADR-044 M3 (`@2`); recorded to show the gate may be wide, measured by M4 | measurement, recorded |
| Previous-swing skip of the CVD gate | No number of its own: the D4 key. The 6 bps it cites is ADR-044 D3's registry padding, quoted to show what admitting the swing would filter | no new number |
| Golden-tape counts 257 / 120 / 248 / 243 / 5 (1 + 4) / 3; 6 892 of 6 962 effort-vs-result and exhaustion edges pass the gate | Measured on the synthetic candidate golden tape by an M2 test that pins them; they show the pair skip is not vacuous and the gate is wide for the other families there, not how BTC behaves | measurement, recorded |
| `oi_lag_ms` 25 000 | The longest live spacing ADR-042 D1 calls continuous (10 000 ms resolution + 15 000 ms `step_tolerance_ms`; measured maximum 15 049 ms): a continuous live feed has closed the boundary the lag before every event, as the archive has. Less: a slow continuous feed may not have closed it; more: the points drift from their events for no gain | derivation |
| Two held OI points | The boundary read lies in `[t − 325 000, t − 25 000]` ms, at most one grid interval before the newest boundary closed by `t` | derivation |
| Archive cost ≈ 23 % / 6 % / 2 % of 15m / 1h / 4h SFPs | ADR-037 S3's first-window-bar share of SFPs (76 / 79 / 82 %) times the 275 s of a bar where a sweep and its first-bar SFP read one boundary, over the bar length; a rate estimate. Measured by M4: 765 / 4 267 = 17.9 %, 34 / 1 063 = 3.2 %, 3 / 257 = 1.2 % | derivation, estimate; measurement, recorded |
| Golden-tape counts 398 / 197 / 195 / 12 / 122 (15m), 96 / 48 / 46 / 1 (1h), 21 / 9 / 9 (4h) | Measured on the synthetic candidate golden tape by an M3 test that pins them; they show the rule is not vacuous there and where it parts from the newest-point rule, not how BTC behaves | measurement, recorded |
| OI comparison strict, unsigned | Equal open interest is no expansion; open interest counts both sides | derivation |
| Development year 2025-10-01 … 2026-09-30 for *Accept when* | The #12 year every earlier measurement used; nothing is fitted, so no hold-out is needed | derivation |

## Consequences

- `MarketState` grows by one field (`triggers`); every state hash and the
  feature-set version change, and old and new hashes are incomparable
  (ADR-041 D2). The pre-#24 encoding is pinned by a test. M2 and M3 append
  values to `Triggers` and move the goldens again.
- A trigger is visible exactly when its fact is: an SFP at the close that
  resolves the sweep, an auction trigger at the 1m close that changes the
  label. No trigger is later than its fact and none is earlier.
- The auction trigger values are `WarmingUp` from every UTC-day rollover
  until the first close under the new prior day, and `Unavailable` while
  the prior day is (D1). The last trigger is kept and shows again once the
  input is ready.
- On the rollover event, a failed auction of the old day's last close
  carries the old reference in its evidence, while its context reads the
  registry already rebuilt on the new prior day (ADR-044 D2, step 4) and
  the auction state after the switch. The evidence's `reference_start`
  tells such events apart.
- In archive replays the location context lists no cluster levels
  (ADR-043 is live only), so contexts from archive and live differ by those
  levels; the gate (D4) does not depend on them.
- The bias is recomputed per event at the cost of a copy; it adds no
  arithmetic and no float of its own.
- The candidate gate may be wide: the registry holds p50 256 levels per
  closed minute, and the admitted kinds include the dense SFP zones (p50
  51) and structural highs (p50 48). M4 reports the pass rate per family
  and kind before anyone relies on it. On the synthetic candidate golden
  tape it passes 243 of 257 CVD divergences and 6 892 of 6 962
  effort-vs-result and exhaustion edges (D8). M4 measured it on the #12
  year: CVD divergence 91.3–94.8 % of the candidates on 15m / 1h / 4h
  (711 / 752, 182 / 192, 42 / 46; 1d 3 / 4), effort vs result 88.9–90.7 %
  (5m 12 952 / 14 572, 15m 7 496 / 8 420, 1h 3 738 / 4 120), exhaustion
  90.8 % (1 462 / 1 610). The gate is wide, as this risk expected: it drops
  5.2–11.1 % of the candidates (1d CVD aside). Narrowing it is a new
  version, decided on this data.
- An excursion whose start and end read the same OI boundary has no
  comparison: one inside a grid interval, and one swept in the last 275 s
  of the bar that resolves it (D7), about 23 % of 15m SFPs in the archive
  by the ADR-037 S3 rates. So `trigger.oi_expansion_rejection.15m@1` may
  be rare. M4 reports the shared boundaries per OI id, and its
  reachability rule stops the run if an id never fires. Measured on the
  #12 year: 765 of 4 267 15m SFPs share a boundary (17.9 %, against the
  estimate's 23 %), and the id fires 1 469 times.
- The archive and a continuous live feed select the same OI boundaries at
  every start and end (D7); their points still differ in `resolution_ms`
  and sample time, so the OI values of one stretch differ between the two
  sources, and none compares across a seam.
- Without magnitude thresholds and without cooldowns, the trigger rate is
  the fact rate (times the gate's pass rate for candidates). Episodes,
  cooldowns and de-duplication belong to #25.

## Alternatives considered

- **A separate trigger-rule registry with events only.** Duplicates
  ADR-029, leaves triggers outside the state hash and the ADR-041 check,
  and an experiment spec could not pin a trigger version (Approach).
  Rejected.
- **An aggregate bias score or direction.** Unmeasured weights (ADR-013);
  aggression is not direction (ADR-023). Rejected.
- **A trend/range label in the bias.** Deferred until validated (brief §9).
  Rejected for `@1`.
- **VWAP, funding and basis in the bias.** No measured contribution yet.
  Rejected for `@1`; a later version may add one.
- **Bias recomputed only at the closes that move its inputs.** The same
  values with a schedule to verify. Rejected.
- **Location as a feature input (`location.levels@N`).** Breaks the
  `location.levels@1` selection (D4). Rejected; the context records it.
- **Any registry level as the gate; only value edges and structural
  extremes as the gate.** D4. Rejected.
- **Admitting a divergence's previous swing as its location; skipping
  every SFP zone the candidate's move made; skipping the previous swing's
  zone only when it reaches the current price.** D4. Rejected.
- **An SFP context without the swept swing's levels in its own registry
  only.** Keeps the swing through the other structure timeframes (D4).
  Rejected.
- **A confirmation delay or later-reclaim horizon on the auction and SFP
  rules.** An unmeasured number (D5). Rejected.
- **A tail-quantile OI expansion; the OI at the swing anchor.** D7.
  Rejected.
- **The newest ready OI point within a 360 000 ms age bound; reading the
  end after the rejection; rebuilding a point from its `delta`.** D7.
  Rejected.
- **A time test or closed bars for the probe's start; a location fact for
  retargets.** D7. Rejected.
- **`Unavailable(OutOfRange)` for an OI change outside `Qty`; leaving the
  OI values out of the #96 isolation check.** D7. Rejected.
- **Reading `MarketState.location.auction` in the auction rules.** Wrong
  reference at the rollover (D5). Rejected.

## Accept when

M4 runs the ignored measurement tool `trigger_measure` (M3) on the #12
development year, 2025-10-01 … 2026-09-30:

- **T0 (asserted):** 0 mismatches between the engine's trigger events and
  an independent model written from this text.
- **Reachability:** every archive-reachable id fires at least once in the
  year. An id with 0 stops the run and is reported on #24: a trigger that
  never fires cannot be researched, so its absence is a definition bug or a
  fact that only a human decision records.
- **Reported, no threshold:** counts per id, per half and per regime label;
  the gate pass rate per candidate family; the CVD divergences per
  timeframe that the previous-swing skip alone stops (D4); the in-zone
  share per level kind; per OI id, the rejections with no start point, no
  end point, a resolution mismatch, a shared boundary (D7), open interest
  not higher, a change out of range, and those that fired.
- **Absorption:** reachability is checked on the live store that ADR-051's
  live R0 run used, since the absorption ids are live only.
- **No hold-out needed:** no constant is fitted to data, so the
  development year serves.

Every criterion is reachable on the stores that exist today: the #12 year
is imported, and the live store of ADR-051's R0 run holds absorption facts.

## Acceptance

Accepted 2026-10-19. Both runs used the `trigger_measure` ignored tests
(`measure_triggers`, `measure_triggers_live`) of main `040dfae` (M3), release
build. The year run covered 2025-10-01 … 2026-09-30 with
`MIE_MEASURE_EVENTS=603323345` under a 4 GB memory cap; its halves split at
the window's midpoint, 2026-04-01 12:00 UTC. The live run replayed ADR-051's
R0 live store, run `20261016T225606Z` and the other cleanly ended runs of
that capture.

- **T0 (asserted):** the year replayed 603 323 345 events, the expected
  count, with 0 rejections and 0 mismatches; values were compared at
  30 275 events. The live run: 2 042 225 events in run `20261016T225606Z`,
  0 rejections, 0 mismatches, values compared at 65 events.
- **Reachability:** every archive-reachable id fires in the year (ids
  without a trigger: none). The three absorption ids are live only and fire
  on the live store, 5m / 15m / 1h 16 / 15 / 6.
- **Cost:** the year run took a wall time of 4 078.1 s at a peak RSS of
  423 MB (the live run 75.4 s, 125 s for the test); ADR-051 R0's
  `candidate_measure` of the same replay took 3 005.3 s. The live run peaks
  at 6.1 GB of anonymous memory (a re-run under an 8 GB cap; under the
  4 GB cap it reached the limit, 28 616 `memory.max` events, and still
  passed without an OOM kill).
  `measure_candidates_live` shows the same 6.1 GB on the same store. The
  memory is `LiveReplay::run_replay` collecting and sorting a whole run's
  records before recomputing, so it grows with the run length (34 h here);
  ADR-052's 550 MB came from a 1 h 40 min sample. Follow-up: #140.

**Reported (no threshold).** *Counts per id;* the cells are the regime
labels low / medium / high / extreme / unlabelled, and the live column is
the total (every live trigger is unlabelled):

| Trigger id | Year total | Half 1 | Half 2 | Live |
|---|---|---|---|---|
| `trigger.failed_auction@1` | 697 | 91 / 94 / 57 / 126 / 8 | 76 / 88 / 96 / 61 / 0 | 2 |
| `trigger.failed_reclaim@1` | 430 | 47 / 63 / 59 / 64 / 4 | 59 / 51 / 54 / 29 / 0 | 0 |
| `trigger.breakout_acceptance@1` | 367 | 46 / 50 / 38 / 42 / 6 | 49 / 61 / 40 / 35 / 0 | 2 |
| `trigger.sfp.15m@1` | 4 267 | 628 / 591 / 392 / 447 / 85 | 691 / 674 / 470 / 289 / 0 | 15 |
| `trigger.sfp.1h@1` | 1 063 | 129 / 137 / 91 / 149 / 17 | 142 / 167 / 125 / 106 / 0 | 3 |
| `trigger.sfp.4h@1` | 257 | 15 / 37 / 17 / 48 / 4 | 22 / 31 / 27 / 56 / 0 | 0 |
| `trigger.sfp.1d@1` | 44 | 0 / 6 / 3 / 11 / 0 | 1 / 5 / 4 / 14 / 0 | 0 |
| `trigger.cvd_divergence.15m@1` | 711 | 104 / 95 / 81 / 67 / 9 | 108 / 103 / 78 / 66 / 0 | 3 |
| `trigger.cvd_divergence.1h@1` | 182 | 25 / 34 / 14 / 24 / 2 | 15 / 30 / 18 / 20 / 0 | 0 |
| `trigger.cvd_divergence.4h@1` | 42 | 5 / 5 / 8 / 1 / 1 | 5 / 8 / 8 / 1 / 0 | 0 |
| `trigger.cvd_divergence.1d@1` | 3 | 0 / 0 / 0 / 1 / 0 | 0 / 2 / 0 / 0 / 0 | 0 |
| `trigger.effort_result.5m@1` | 12 952 | 1 743 / 1 779 / 1 160 / 1 262 / 212 | 1 969 / 2 245 / 1 440 / 1 142 / 0 | 17 |
| `trigger.effort_result.15m@1` | 7 496 | 1 040 / 983 / 673 / 795 / 126 | 1 134 / 1 266 / 843 / 636 / 0 | 16 |
| `trigger.effort_result.1h@1` | 3 738 | 509 / 506 / 336 / 451 / 57 | 548 / 528 / 445 / 358 / 0 | 10 |
| `trigger.exhaustion@1` | 1 462 | 184 / 147 / 82 / 85 / 30 | 359 / 311 / 148 / 116 / 0 | 4 |
| `trigger.absorption.5m@1` | 0 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 | 16 |
| `trigger.absorption.15m@1` | 0 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 | 15 |
| `trigger.absorption.1h@1` | 0 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 | 6 |
| `trigger.oi_expansion_rejection.15m@1` | 1 469 | 202 / 221 / 118 / 158 / 22 | 227 / 244 / 164 / 113 / 0 | 6 |
| `trigger.oi_expansion_rejection.1h@1` | 453 | 62 / 61 / 40 / 74 / 9 | 47 / 70 / 46 / 44 / 0 | 2 |
| `trigger.oi_expansion_rejection.4h@1` | 125 | 9 / 13 / 10 / 27 / 2 | 13 / 15 / 12 / 24 / 0 | 0 |
| `trigger.oi_expansion_rejection.1d@1` | 19 | 0 / 6 / 0 / 5 / 0 | 0 / 4 / 1 / 3 / 0 | 0 |
| `trigger.oi_expansion_rejection.prior_day@1` | 319 | 42 / 44 / 23 / 50 / 4 | 34 / 45 / 48 / 29 / 0 | 1 |

*Gate pass rate per candidate id (D4):*

| Candidate id | Year passed / candidates | Rate | Live passed / candidates |
|---|---|---|---|
| `trigger.cvd_divergence.15m@1` | 711 / 752 | 94.5 % | 3 / 3 |
| `trigger.cvd_divergence.1h@1` | 182 / 192 | 94.8 % | 0 / 0 |
| `trigger.cvd_divergence.4h@1` | 42 / 46 | 91.3 % | 0 / 0 |
| `trigger.cvd_divergence.1d@1` | 3 / 4 | 75.0 % | 0 / 0 |
| `trigger.effort_result.5m@1` | 12 952 / 14 572 | 88.9 % | 17 / 17 |
| `trigger.effort_result.15m@1` | 7 496 / 8 420 | 89.0 % | 16 / 16 |
| `trigger.effort_result.1h@1` | 3 738 / 4 120 | 90.7 % | 10 / 10 |
| `trigger.exhaustion@1` | 1 462 / 1 610 | 90.8 % | 4 / 4 |
| `trigger.absorption.5m@1` | 0 / 0 | - | 16 / 16 |
| `trigger.absorption.15m@1` | 0 / 0 | - | 15 / 15 |
| `trigger.absorption.1h@1` | 0 / 0 | - | 6 / 6 |

The CVD divergences that only the previous-swing skip stops (D4), on
15m / 1h / 4h / 1d: 18 / 5 / 1 / 1.

*Context share, the percentage of trigger contexts that list a level kind,
per family* (year; `cluster` and `validated_reference` are 0 in every
family, since clusters are live only and `validated_reference` has no
source, so they are left out):

| Family (triggers) | poc | vah | val | hvn | lvn | structural_high | structural_low | prior_sweep | sfp_zone | vwap |
|---|---|---|---|---|---|---|---|---|---|---|
| failed_auction (697) | 2.4 | 47.1 | 53.2 | 37.0 | 38.5 | 0.0 | 0.0 | 82.1 | 92.8 | 11.1 |
| sfp (5 631) | 5.0 | 5.5 | 5.1 | 34.2 | 34.2 | 2.0 | 1.9 | 63.8 | 90.0 | 4.7 |
| absorption (0) | - | - | - | - | - | - | - | - | - | - |
| exhaustion (1 462) | 4.7 | 8.0 | 6.3 | 42.1 | 36.2 | 3.4 | 3.7 | 68.6 | 91.9 | 8.1 |
| effort_vs_result (24 186) | 5.4 | 5.5 | 5.0 | 38.8 | 36.0 | 0.8 | 0.9 | 70.8 | 92.4 | 8.9 |
| cvd_divergence (938) | 6.6 | 3.4 | 5.9 | 39.5 | 35.3 | 10.8 | 8.2 | 63.6 | 91.5 | 3.8 |
| oi_expansion_rejection (2 385) | 3.9 | 10.9 | 10.5 | 33.1 | 33.2 | 2.2 | 1.8 | 64.7 | 89.9 | 5.2 |
| failed_reclaim (430) | 3.0 | 54.2 | 46.3 | 34.4 | 37.0 | 0.0 | 0.0 | 84.2 | 84.9 | 12.8 |
| breakout_acceptance (367) | 5.2 | 53.1 | 47.7 | 41.1 | 34.6 | 0.0 | 0.3 | 85.6 | 91.8 | 21.5 |

On the live store the `cluster` share is 100 % for failed auction (2
triggers), absorption (37), exhaustion (4) and effort vs result (43), 78 %
for SFP (18), 67 % for CVD divergence (3), 89 % for OI (9) and 0 % for
breakout acceptance (2); the other kinds rest on too few triggers to read.

*OI outcomes per id (D7),* the year:

| OI id | no start | no end | resolution mismatch | shared boundary | not higher | out of range | fired |
|---|---|---|---|---|---|---|---|
| `trigger.oi_expansion_rejection.15m@1` | 0 | 0 | 0 | 765 | 2 033 | 0 | 1 469 |
| `trigger.oi_expansion_rejection.1h@1` | 0 | 0 | 0 | 34 | 576 | 0 | 453 |
| `trigger.oi_expansion_rejection.4h@1` | 0 | 0 | 0 | 3 | 129 | 0 | 125 |
| `trigger.oi_expansion_rejection.1d@1` | 0 | 0 | 0 | 0 | 25 | 0 | 19 |
| `trigger.oi_expansion_rejection.prior_day@1` | 0 | 0 | 0 | 65 | 313 | 0 | 319 |

The outcomes of each id sum to its SFP count (4 267 / 1 063 / 257 / 44) or,
for `prior_day`, to the 697 failed auctions. No rejection lacked a start
or end point or had a resolution mismatch. The live store: 15m 0 / 0 / 0 /
1 / 8 / 0 / 6, 1h 0 / 0 / 0 / 0 / 1 / 0 / 2, `prior_day` 0 / 0 / 0 / 0 / 1 /
0 / 1, 4h and 1d none, in the same column order.

Full results: https://github.com/Replikanti/mie/issues/24#issuecomment-6269005686
