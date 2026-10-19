# ADR-054: Context records and sampling — trigger and baseline samples, episodes from evidence overlap

- Status: proposed
- Date: 2026-10-19

## Context

#25 asks to define the unit Conditional Behavior measures outcomes for (the
Conditional Behavior brief, brief §12, ADR-006): a context record and the
rule for when one is taken. Its scope:

- the context: a Market State subset, the regime (label and raw
  percentile), the location (level, auction state) and the trigger (family,
  version) at decision time, with the feature-set and dataset versions;
- sampling: event-driven, on trigger and location events, and/or at a
  fixed cadence as a baseline, decided and documented;
- overlap control: one market episode must not yield dozens of correlated
  samples, so sample counts stay honest;
- unconditional baseline samples to compare against.

Its acceptance criteria: context records produced in replay with full
provenance, and tests for episode de-duplication and baseline sampling.

What exists:

- The trigger events of ADR-053: one versioned rule per trigger family,
  each event carrying its feature key, family, evidence fact, a
  `LocationContext` and a `Bias` snapshot. ADR-053 has no cooldowns and no
  magnitude thresholds, so the trigger rate is the fact rate.
- The order-flow candidates of ADR-051 with their location context
  (ADR-051 D3), the location and auction state of ADR-044, and
  `Bias::read`, made public by ADR-053 D3 for #25's baseline.
- One engine for live and replay (ADR-019), the replay over the raw store
  (ADR-039) with its dataset version (ADR-030), and the engine key
  (feature-set version plus state-hash encoding, ADR-041, ADR-052 D10).

Three accepted ADRs hand this issue its open questions: ADR-044
(Consequences) leaves "episodes, cooldowns" to #25, ADR-051 (D5) leaves it
"cooldowns and episode de-duplication", and ADR-053 (Consequences) leaves it
"episodes, cooldowns and de-duplication". ADR-044 also expected #25 to
consume zone events (D3 below decides not to), and ADR-040 D3 lists #25
among the owners of rule-id semantics (*Out of scope* below).

This ADR decides the record, the snapshot, what is sampled, the episode
rule, provenance and placement. No accepted ADR is edited.

Delivery is in three PRs under #25: M1 ships this ADR and the domain module
`mie_domain::conditional`; M2 the app driver and the measurement tool
`context_measure`; M3 records the measurement of *Accept when* here and
flips the status.

## Decision

### Approach

A pure domain module turns the engine's output after each accepted event
into **context records** of two kinds: a **trigger sample** for every
`TriggerEvent`, carried unchanged, and an **unconditional baseline sample**
at every event that closes a 1m bar, the instants at which every trigger
family is decided. Each record carries a declared Market State snapshot,
the bias, the location context and the event key; the run's provenance
comes once per run. Overlap control has **no time constant**: within one
trigger id, a trigger joins the open episode when the input data its
evidence was built from overlaps that episode's data, and otherwise opens a
new one. Every sample is kept; counts are taken over episodes.

*Rejected:* a fixed cooldown per family ("one sample per id per N
minutes"). N would be a number nobody measured (ADR-013). Fitting it needs a
clean hold-out, and none is left: ADR-053 D7 records that the ADR-051
hold-out year has been read. A cooldown also mixes two different
correlations: samples that share **inputs**, which this issue can define
exactly from the evidence, and samples that share an **outcome horizon**,
which only exists once #26 fixes horizons.

### D1. The context record

`ContextRecord`:

- `event`: the `CanonicalKey` (ADR-028) of the emitting event. Its time is
  the **decision time** and the leakage boundary for #26: a record reads
  nothing after this event.
- `sample`, one of:
  - `Trigger { trigger, episode }`: the ADR-053 event **as emitted**, with
    its key (family and parameter version), evidence, `LocationContext` and
    `Bias`, and its `Episode` (D4).
  - `Baseline { minute_end }`: the end of the 1m bar whose close set the
    anchor. It is the grid point, so a consumer can subsample.
- `context: LocationContext` and `bias: Bias`:
  - A trigger record uses the trigger event's own values, never a re-read.
    *Why:* the SFP and CVD-divergence contexts leave out the swing's own
    levels (ADR-053 D4); a re-read at the anchor would put them back.
  - A baseline record reads the context with
    `LocationContext::read(anchor, None, &state.location)` and the bias
    with `Bias::read(&state)`, the definitions the trigger events use.
- **The baseline anchor** is the close of the last closed 1m bar with
  trades, the anchor of the effort-vs-result, exhaustion and absorption
  contexts (ADR-051 D3). *Why:* a baseline sample and a candidate trigger
  at the same event then read **the same context**, so a
  conditional-vs-baseline contrast within one location bin compares like
  with like. *Rejected:* the last trade price. That is the closing trade of
  the next minute, and its context would differ from the candidates' at the
  same instant.
- `state: StateSnapshot` (D2).
- Nothing is duplicated: the regime's "label and raw percentile" is
  `bias.regime`, the location's "level, auction state" is `context.levels`
  and `context.auction`, the trigger's "family, version" is
  `trigger.family` and `trigger.feature`.
- **Order within one event:** the baseline sample first, then one trigger
  sample per trigger event in emission order (ADR-053 D2).
- **Naming** (the ADR-051/053 rule extends here): no type, field or
  `Display` text says bullish, bearish, long, short, signal, reversal,
  continuation or entry. No execution semantics (ADR-010).

### D2. The snapshot of `conditional.sampling@1`

Each value is copied from the committed state with its `FeatureValue`
validity: no arithmetic and no new float.

- **Rule:** a feature is in the snapshot if and only if a filed consumer
  issue names it. *Why:* every other feature would be stored about 525 000
  times a year (D3) on a guess; adding one later is a new policy version
  (D5) at the cost of a re-run, not a silent schema change.
- **Included** (`SNAPSHOT_FEATURES`, in field order):

  | Field | Feature(s) | Consumer that names it |
  |---|---|---|
  | `last_price` | `trade.last_price@1` | #26: the threshold reference at decision time |
  | `atr` | `volatility.atr.1h@1` | #26: "thresholds in ATR multiples" |
  | `aggression` | `flow.window.<5m\|15m\|1h>@1` | #28: aggressive buy/sell volume, delta, price displacement (`displacement` is a field of the window) |
  | `cvd_utc_day` | `flow.cvd.utc_day@1` | #28: "delta/CVD" |
  | `effort` | `candidate.effort_result.<5m\|15m\|1h>@1` | #28: "high aggression + strong price response" is not a trigger; on any sample it is read from the judged evaluation (`effort_high`, `move_high`) |
  | `liquidity` | `book.liquidity.window.<5m\|15m\|1h>@1` | #28 and #31: passive liquidity. Live only: warming up in archive replays, and the record says so |
  | `oi_5m` | `derivatives.oi.5m@1` | #31: "OI condition" |

- **Excluded, each with its reason:**
  - `flow.cvd.continuous@1`: relative to the run's anchor, so not
    reproducible across replay windows (ADR-035 D2).
  - Funding, mark price and basis, liquidations: no consumer names them.
  - `bars.motion.*`: the displacement is already in the windows, and no
    consumer names another horizon.
  - The trigger values and the other candidates: co-occurring triggers are
    **rows of the same dataset**, joinable on `event`. Copying them would
    store each trigger many times.
  - A per-record state hash: hashing the whole state, the live L2 book
    included, about 525 000 times a year costs a lot for no new
    information. The data version, the engine key and the event key
    reproduce any record by replay.
- A test pins that every entry of `SNAPSHOT_FEATURES` is in
  `catalog::CURRENT`, appears once, names its field's feature key, and
  that every value equals its state field.

### D3. What is sampled

- **Every trigger event** is a trigger sample. The location-fact families
  (failed auction, failed reclaim, breakout + acceptance) are triggers
  already.
- **Baseline: every accepted event that closes at least one 1m bar**
  (`closed_bars()` holds a `Timeframe::M1` bar), once per event even when a
  trades gap closes many minutes at once. Sampling starts with the first
  closed 1m bar with trades, so that an anchor exists. An event that closes
  only empty minutes keeps the anchor and `minute_end` of the last minute
  with trades; M3 counts such instants.
- *Why this grid:* every trigger family is decided at such an event:
  - candidates at 1m closes (ADR-051 D2);
  - auction labels at 1m closes (ADR-044);
  - SFPs and swing confirmations at closes of 15m and longer bars, which
    are 1m closes too (ADR-037);
  - OI triggers right after their rejection, at the same event (ADR-053
    D2).

  So the baseline is drawn from exactly the instants a trigger sample can
  occur at, and a conditional-vs-baseline difference reflects context, not
  timing. A finer grid adds instants at which no trigger can occur.
- *Rejected: a coarser grid (5m, 1h).* A 5m grid holds every 15m, 1h, 4h
  and 1d close, so those instants weigh 5× their share of the 1m grid (1 in
  3 grid points instead of 1 in 15 for the 15m closes); a 1h grid weighs
  the 1h, 4h and 1d closes 60×. That is a timing bias the 1m grid does not
  have, and a consumer can still thin the 1m grid without bias on
  `minute_end`.
- M3 **asserts** "every trigger instant is a baseline instant" on BTC data.
  If it fails, the derivation above is false and the run stops.
- **Location zone events (`Entered`/`Left`) are not sampled.** *Why:*
  - About 1 380 arrivals and 1 400 departures a day (ADR-044
    Consequences): about 1 million correlated samples a year that would
    duplicate the baseline's location split.
  - A zone entry is location without a trigger (ADR-012).
  - Location-only conditioning is the baseline split by its context.
  - "Price just arrived at a level" as a decision event would be a new
    trigger family under an ADR-053-style rule, not a sampling choice.

### D4. Episodes (overlap control)

One episode stream per **trigger id** (feature key).

- **Evidence span** `[first, last]` of a trigger, both inclusive: the data
  time of the first and the last input its evidence was built from. It is
  read from the evidence's own fields, with no constant in code but the
  deciding minute:

  | Ids | `first` | `last` |
  |---|---|---|
  | `trigger.effort_result.<w>@1`, `trigger.absorption.<w>@1` (via `Absorption.effort`) | `end − w` (`w` = the window's `timeframe`) | `end − 1 ms` |
  | `trigger.exhaustion@1` | `expansion.end − expansion.timeframe` (= `t − 65 min`) | `end − 1 ms` |
  | `trigger.cvd_divergence.<tf>@1` | `pair.previous.time` | `pair.current.time` |
  | `trigger.sfp.<tf>@1` | `sweep.time` | `resolved_bar_end − 1 ms` |
  | `trigger.oi_expansion_rejection.<tf>@1` and `.prior_day@1` | the earlier of the rejection's `first` and `start.boundary` | the later of the rejection's `last` and `end.boundary` |
  | auction ids (failed auction, failed reclaim, breakout + acceptance) | the deciding 1m bar: `bar_end − 60 000 ms` | `bar_end − 1 ms` |

  - *Why the auction ids get only their deciding bar:* a failure or an
    acceptance ends its probe (ADR-044 D8), and the next trigger of the id
    needs a new probe, which starts at a later close. Two triggers of one
    auction id never share a probe's closes, so the deciding bar is enough
    to keep them apart.
  - *What the span leaves out:* the yardsticks and places a fact is judged
    against, not built from — the 1 440 earlier evaluations an effort edge
    is judged against (ADR-051 D5), a window's reference price (the last
    trade before it, ADR-035), the swing that made a swept level. Counting
    them would chain every effort edge within a day into one episode: a
    24-hour cooldown by another name.
- **Join rule:** a trigger joins its id's open episode when
  `first ≤ open.last`; the episode's `last` becomes the later of the two.
  Otherwise it opens a new episode, and the open one closes. So
  consecutive divergences that share a swing chain, rising edges of one
  window whose minutes overlap chain, and SFPs whose excursions overlap
  chain.
- **Invariant**, which the tests pin: two episodes of one id share no
  input data, except across a reported bridge.
- **Bridges:** a trigger that joins the open episode while its `first` is
  at or before the previous closed episode's `last` reaches back into that
  episode's data. Streaming cannot merge two closed episodes, so bridges
  are counted (`SamplingStats.bridges`) and M3 reports them. They occur
  only where `first` is not monotone per id: the high and low sides of a
  divergence id, and SFPs of different levels.
- **Identity:** `EpisodeId { trigger, opened_at, ordinal }`: the trigger
  id, the `CanonicalKey` of the event that opened it, and how many episodes
  the id had opened at that event before it. *Why the ordinal:* one close
  can confirm a high and a low swing with disjoint spans. Each record
  carries `Episode { id, opener, truncated }`; consumers count
  `DISTINCT id`.
- **Truncated:** the opener's `first` is earlier than the first event the
  sampler observed, so its earlier members may be missing.
- **Scope is per id, not per family.**
  - *Why:* the id is what a spec pins (ADR-053 Approach) and the cell unit
    ADR-051's R1 rule counts in.
  - *Rejected:* overlap across the ids of one family. A 1d SFP's
    sweep-to-resolution span covers dozens of unrelated 15m SFPs and would
    swallow them.
  - Cross-id co-emissions (the 5m, 15m and 1h windows at one minute, the
    SFPs of one extreme on two timeframes) share the record's `event` key,
    so a family-pooled study can de-duplicate on it. M3 reports how often
    they occur.
- **Rejected:**
  - a fixed cooldown (Approach);
  - a cooldown equal to the outcome horizon: #26 owns horizons, and it
    would couple the context to the labels;
  - a cooldown fitted to the autocorrelation of trigger times on the #12
    year: a fitted number with no hold-out left;
  - **zone-stay episodes**, one episode per stay of price inside a level
    zone, which ADR-044 hinted at. A trigger context lists several levels:
    SFP zones are in 85–93 % of trigger contexts and prior sweeps in
    64–86 % (ADR-053 *Acceptance*), so the stay is ambiguous without a
    precedence nobody measured; the SFP context also excludes the swept
    level (ADR-053 D4); and a stay measures where price is, not which data
    a trigger read;
  - dropping the non-opening samples: it loses data, while a flag keeps
    them and the counts stay honest.
- **Baseline samples carry no episode.** Their serial correlation is a
  horizon question. Their nominal count overstates their effective count
  (Consequences); #26/#27 thin them by horizon or use a block bootstrap.

### D5. Provenance and versioning

- The policy key `conditional.sampling@1` (a `PipelineKey`, `SAMPLING_V1`)
  covers the baseline grid, the episode rule, the span table and the
  snapshot list.
- Its fingerprint is FNV-1a 64 (`Fingerprinter::write_str`, ADR-029) of a
  canonical parameter text that names each of them (`policy_text`), pinned
  by a golden test, so a change to any of them that forgets the version
  bump fails a test.
- `ContextProvenance { data, engine, policy, policy_fingerprint }`:
  - `data`: the `DataVersion` of the replayed raw data (ADR-030), from
    `Replay::dataset` through the existing
    `From<&DatasetVersion> for DataVersion` (M2);
  - `engine`: the `EngineKey`, the state's `feature_set` and
    `STATE_HASH_ENCODING` (ADR-041).
- It is delivered once per run, not per record. #27 copies it into its
  dataset rows and adds the code version and the labeling version (#26).
- Any change to D1–D4 is a new policy version.

### D6. Placement

- The sampler (`ContextSampler`) is domain code and pure: no `HashMap`, no
  clock, fixed per-id episode slots in catalog order (`TRIGGER_IDS`). That
  gives one code path for live and replay (ADR-019).
- It reads only the engine's committed state, `closed_bars()` and
  `trigger_events()` after an **accepted** event. It holds the last 1m close
  with trades itself, from `closed_bars()`, by the rule `CandidateTracker`
  uses.
- The app driver (M2) calls it for accepted events only. *Why:*
  `drive_tolerant_observed` also calls its observer after a rejected event,
  while `closed_bars()` and `trigger_events()` still show the previous
  event. Without the guard every rejection would duplicate the previous
  event's records.
- `ContextSampler::new(emit_from)` lets a caller drive a warm prefix
  through the engine and the episode tracker while emitting records only
  from the window start.
- Records stream to a sink; nothing is collected (the #140 lesson: a
  replay that keeps its output grows with the run).

### D7. Where each number comes from

| Number | Source | What breaks at another value |
|---|---|---|
| 1m baseline grid | D3: every trigger family is decided at an event that closes a 1m bar (ADR-051 D2, ADR-044, ADR-037, ADR-053 D2) | Coarser: the closes of longer bars weigh 5× (5m grid) or 60× (1h grid) their share. Finer: instants at which no trigger can occur |
| Effort and absorption span `[end − w, end − 1 ms]` | The window's own `timeframe` and `end` (ADR-035 windows of closed minutes, half-open) | Shorter than the span: two episodes share input data, so one stretch of evidence is counted twice. Longer: episodes that share no input are merged, and how much longer is #26's horizon |
| Exhaustion `first` = `t − 65 min` | The judged 1h expansion ending at `t − 5 min` and the extreme over `(t − 65 min, t]` (ADR-051 D6), read from the evidence | As above |
| CVD divergence `[previous.time, current.time]` | The anchor trades of the swing pair (ADR-051 D4) | As above |
| SFP `[sweep.time, resolved_bar_end − 1 ms]` | The sweeping trade and the resolving bar (ADR-037) | As above |
| OI union with `start.boundary`, `end.boundary` | The two `derivatives.oi.5m@1` points the expansion compares (ADR-053 D7) | As above |
| Auction deciding bar, 60 000 ms | The 1m bar whose close changed the label (ADR-044 classifies at 1m closes); a probe ends with its failure or acceptance (ADR-044 D8) | Longer: triggers of one id on separate probes could chain. Shorter: part of the deciding bar is left out |
| 1 ms in every `last` | Event time has millisecond resolution and bar and window ends are exclusive (ADR-028, ADR-031) | Without it, windows that only touch would chain |
| The snapshot list | D2: the consumer issues that name each feature | A longer list stores unclaimed features about 525 000 times a year; a shorter one forces a re-run for a consumer that needs one |
| Development year 2025-10-01 … 2026-09-30 | The #12 year every earlier measurement used; nothing is fitted, so no hold-out is needed | — |
| 0 mismatches (T0) | The sampler and the model are deterministic code written from one text, so any difference is a bug | — |

There is no other number.

### Out of scope

- Forward outcome labels, horizons, censoring, and horizon-overlap
  thinning of samples: #26.
- The Parquet dataset writer, DuckDB views, the `mie` subcommand, and the
  code and labeling versions in provenance: #27.
- The studies, bootstrap intervals and minimum-sample flags: #28.
- **Resolving the `state-filter` rule ids** of spec text v1. ADR-040 D3
  lists #25 among the owners of rule-id semantics; #24 likewise left the
  `trigger` slot unresolved and pinned triggers through `features`. The
  first pipeline that applies filters (#30, the backtest) resolves them
  against the D2 snapshot. This ADR records the hand-over.
- Live-ingestion wiring of the sampler, and egress (#38).
- A history-chain driver: a restart in `drive_chain` (ADR-052 D4) would
  also need a sampler reset. That is #27's choice of source.
- New trigger families (for example, arrival at a level), cross-family
  de-duplication, and any change to the engine, the catalog or the state
  hash.

## Consequences

- Records are not Market State: no feature, catalog, lock, state-hash or
  golden of the engine changes, and the ADR-041 equivalence still covers
  everything a record is computed from.
- About 525 000 baseline samples a year (one per minute), with an
  effective count far lower: consecutive minutes share most of their
  context and their outcomes overlap at every horizon longer than a minute.
  Their nominal count must not be used as a sample size; #26/#27 thin them
  by horizon (`minute_end` makes that trivial) or use a block bootstrap,
  and the interval method belongs to #27/#28.
- Episode counts are per id. A family-pooled count over-counts cross-id
  co-emissions unless it de-duplicates on `event`; M3 reports how often
  they occur.
- Without a time constant, sustained activity (1h windows, flickering
  rising edges) can chain into long episodes, so episode counts may be
  low. That is not bounded by a number, which would be the rejected
  cooldown: M3 reports samples and duration per episode, and a human reads
  that table before #26 fixes horizons.
- Bridges leave two closed episodes that share data. They are counted, not
  repaired.
- Episode ids depend on where a replay starts: an episode open at the
  first observed event is marked `truncated`, and `emit_from` lets a warm
  prefix rebuild it. Replays from the same start reproduce byte for byte.
- In archive replays the liquidity windows are warming up in every record
  (ADR-043 is live only), and the contexts list no cluster levels; records
  from archive and live differ by those fields.
- A record's `Display` line includes the floats that the snapshot's
  features derive on demand (ADR-035); the record stores none.

## Alternatives considered

- **A fixed cooldown per family or id.** An unmeasured number, with no
  hold-out left to fit it, mixing shared inputs with shared horizons
  (Approach). Rejected.
- **A cooldown equal to the outcome horizon; one fitted to the
  autocorrelation of trigger times.** D4. Rejected.
- **Zone-stay episodes.** Ambiguous for contexts with several levels, and
  a measure of where price is, not what a trigger read (D4). Rejected.
- **Episodes across the ids of a family.** Long spans swallow unrelated
  short ones (D4). Rejected; co-emissions are linked by `event`.
- **Dropping non-opening samples.** Loses data (D4). Rejected; `opener`
  flags them.
- **Sampling location zone events.** About 1 million correlated samples a
  year that duplicate the baseline split; location without a trigger
  (D3). Rejected.
- **A 5m or 1h baseline grid.** A timing bias toward the closes of longer
  bars (D3). Rejected.
- **The last trade price as the baseline anchor.** Its context differs
  from the candidates' at the same instant (D1). Rejected.
- **Re-reading the context of trigger samples.** Undoes the ADR-053 D4
  exclusions (D1). Rejected.
- **The whole Market State, or a state hash, per record.** Hundreds of
  thousands of copies a year for no information the provenance does not
  already give (D2). Rejected.
- **Collecting the records in memory.** Grows with the run (D6, #140).
  Rejected.

## Accept when

M3 runs the ignored tool `context_measure` (M2) on the #12 development
year, 2025-10-01 … 2026-09-30, and on the ADR-051 R0 live store:

- **T0 (asserted):** 0 mismatches between the sampler's records and an
  independent model written from this text. The model covers the baseline
  instants and anchors, episode ids, the `opener` and `truncated` flags,
  and the snapshot values; for trigger records, the context and the bias
  must equal the event's.
- **Grid invariant (asserted):** every trigger record's event is a
  baseline instant (D3).
- **Reachability (asserted):** every archive-reachable trigger id has at
  least one episode in the year, and the absorption ids have one on the
  live store.
- **Reported, no threshold:**
  - per id: samples, episodes, samples per episode (p50/p90/p99/max),
    episode duration (p50/p99/max), bridges and truncated episodes;
  - cross-id co-emissions per family at one event;
  - baseline samples: in total, per regime label and per ADR-051 location
    bin (`value_edge` / `structural_extreme` / `neither`), and the instants
    that close only empty minutes;
  - wall time and peak RSS.
- **No hold-out needed:** no constant is fitted, so the development year
  serves.

Every criterion is reachable on the stores that exist today: the #12 year
is imported, and the R0 live store holds absorption facts.
