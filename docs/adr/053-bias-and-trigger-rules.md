# ADR-053: Bias and trigger rules — a non-authorizing bias context and one versioned rule per trigger family

- Status: proposed
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
  input feature already defines (`AuctionStatus`, `Sweep`, and in M2/M3
  `CvdComparison`, `EffortResult`, `Exhaustion`, `Absorption`,
  `OiGridPoint`). It is `None` until the first trigger. The
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
    by kind. The gate's pass rate is unknown until M4 measures it (see
    Consequences).
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

- **Start point:** the ready `derivatives.oi.5m@1` point at the event that
  starts the excursion beyond the level.
  - For an SFP: the `StructureEvent::Sweep` (the sweeping trade). Recorded
    under the key (timeframe, side, `swing_time`) and dropped at the `Sfp`
    or `Break` resolution.
  - For a failed auction: the 1m close at which a probe toward Above or
    Below starts or retargets (`probe.since == known_at`). A retarget
    between Above and Below keeps the `Breakout` label and so emits no
    `Auction` event; the rule therefore reads the committed status at every
    1m close. One probe record at a time, keyed by the reference; it is
    consulted before it is updated in the same event, and dropped on
    failure, acceptance or a new reference.
- **End point:** the ready point at the rejecting event.
- **Age bound.** A point counts only if its boundary is at or after the
  event time − 360 000 ms. *Derivation:* one grid interval (300 000 ms:
  until the next boundary is due) plus the grid's `max_age_ms` of
  60 000 ms, the lag `derivatives.oi.5m@1` accepts between a boundary and
  its sample (ADR-042 D2). A live boundary closes only on the first poll
  after it, so for a few seconds of every interval the newest ready point
  is just over one interval old although the feed is healthy. *Smaller:*
  those healthy live points are lost while archive points are not, so the
  id means different things live and in the archive. *Larger:* during an
  open-interest outage the newest ready point stays ready, so an excursion
  would read an outage's stale point as its start.
- **Expansion condition:** both points are present, have the same
  `resolution_ms` (no comparison across an archive/live seam), `end.boundary
  > start.boundary`, and `end.open_interest > start.open_interest`
  (strict).
  - *Stated limit:* an excursion that starts and is rejected inside one grid
    interval has no comparison. M4 measures how often that happens.
- **Unsigned:** open interest counts both sides' positions, so the rule is
  the same for a probe above and below (the ADR-051 D4 reason).
- **No magnitude threshold in `@1`** (ADR-013). The event carries both
  points, the change and the span, so #28/#31 can threshold on measured
  contribution.
  - *Rejected:* a tail quantile against a rolling baseline ("expansion"
    meaning unusual). Fixing the two effort numbers took a pre-registered
    tool, two rules and a hold-out year (ADR-051 D9); that hold-out year has
    now been read, so no clean data is left to fix a third.
  - *Rejected:* the OI at the swing anchor. On 4h/1d it can be days old, so
    it measures the move before the excursion, not the excursion.
- **Why these two rejections:** brief §11 lists this family next to failed
  auction and SFP as triggers at a value edge, and those two are the facts
  on main that mean "beyond a level, then back". A failed reclaim is a
  different auction context (returning into value); it gets its own id if
  #28 asks for one.
- **Parameters:** `compare_rule` = `strict`, `oi_max_age_ms` = 360 000,
  `rejection` = `sfp` or `failed_auction` (text); the SFP ids also
  `timeframe_ms`.
- **State:** engine state, rebuilt by replay; ADR-052's warm start replays
  from the origin, so a warm-started engine holds the same records.

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
| OI point age ≤ 360 000 ms | One grid interval plus the 60 s lag `derivatives.oi.5m@1` accepts (ADR-042 D2); less loses healthy live points, more reads an outage's stale point | derivation |
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
  effort-vs-result and exhaustion edges (D8).
- SFPs whose excursion resolves inside one 5-minute grid interval can never
  qualify for the OI family, so `trigger.oi_expansion_rejection.15m@1` may
  be rare. M4 measures that share, and its reachability rule stops the run
  if the id never fires.
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
- **Reading `MarketState.location.auction` in the auction rules.** Wrong
  reference at the rollover (D5). Rejected.

## Accept when

M4 runs the ignored measurement tool (M3) on the #12 development year,
2025-10-01 … 2026-09-30:

- **T0 (asserted):** 0 mismatches between the engine's trigger events and
  an independent model written from this text.
- **Reachability:** every archive-reachable id fires at least once in the
  year. An id with 0 stops the run and is reported on #24: a trigger that
  never fires cannot be researched, so its absence is a definition bug or a
  fact that only a human decision records.
- **Reported, no threshold:** counts per id, per half and per regime label;
  the gate pass rate per candidate family; the CVD divergences per
  timeframe that the previous-swing skip alone stops (D4); the in-zone
  share per level kind; the share of SFPs whose excursion stays inside one
  grid interval.
- **Absorption:** reachability is checked on the live store that ADR-051's
  live R0 run used, since the absorption ids are live only.
- **No hold-out needed:** no constant is fitted to data, so the
  development year serves.

Every criterion is reachable on the stores that exist today: the #12 year
is imported, and the live store of ADR-051's R0 run holds absorption facts.
