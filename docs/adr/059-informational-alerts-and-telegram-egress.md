# ADR-059: Informational alerts — decided in the domain, rendered in the app, delivered by a gateway; policy `alert.policy@1`

- Status: proposed
- Date: 2026-10-30

## Context

#38 asks for the first practical egress (brief §17): informational
Telegram alerts and market reports, with autonomous execution outside the
MVP. Its scope: an `AlertGateway` outbound port with a Telegram adapter and
a `GenerateMarketReport` inbound port; alerts for entering and leaving a
monitored location (a VAH zone in the brief's example), a trigger guide
that states what would confirm and the explicit invalidation, regime
changes and, later, suitability results; a periodic market report (state,
regime, location, active monitors); de-duplication and rate limiting;
credentials only from the environment; and the same domain path in a
replay dry-run, so alerting can be audited historically. Its acceptance
criteria: rendering tests for every alert kind without a network, a replay
dry-run that produces the alert log of a historical window, and no message
that implies an order or an instruction to trade.

ADR-010 fixes only the boundary — "first egress is informational;
autonomous execution remains outside the MVP". The port contract, which
alerts exist, their wording, the delivery limits and the client are new
decisions; this ADR makes them and implements ADR-010 without superseding
it.

What exists:

- The location facts of ADR-044: `LocationEvent::Entered{Arrived|Created}`,
  `Left{Departed|Retired}` and `Auction{from, to, status}` per closed 1m bar
  with trades, the level registry with in-zone flags, and the auction
  classifier `location.auction.prior_day@1` against the prior day's value
  area, with its tolerance `w` (`TOLERANCE_BPS` = 6, D3) and acceptance
  time (`ACCEPTANCE_CLOSES` = 60, D9).
- The trigger rules of ADR-053 D5 on auction facts: `trigger.failed_auction@1`,
  `trigger.failed_reclaim@1`, `trigger.breakout_acceptance@1`, with the
  naming rule (no bullish, bearish, long, short, signal, reversal,
  continuation, entry; no targets, stops or sizes).
- The regime `volatility.regime.1h@1` (ADR-033), which can only change at a
  1h close.
- One engine for live and replay (ADR-019), the replay over the raw store
  (ADR-039), the history chain and warm start (ADR-052), and the bounded
  core channel of live capture (ADR-026), which blocks the capture when the
  core thread stalls.

The volume problem: the registry produces about 1 380 arrivals a day over
about 254 levels (ADR-044 M3), and ADR-044's consequences already route
alert rate limits to this issue. No figure exists for the levels a person
would actually watch, so no budget or rate limit can be derived yet.

Delivery is in four PRs under #38: **M1** (this ADR, proposed) ships the
decision path, the dry-run and the measurement tool; the main session then
measures the #12 development year; a **gate PR** records the measurement
and the gate decisions (D6); **M2** adds the Telegram adapter and live
wiring (D5); **M3** records the live smoke and accepts this ADR.

## Decision

### Approach

Alerts are **decided** in the core and only **delivered** by adapters. A
pure `AlertMonitor` in `mie-domain` turns the engine's committed facts
after each accepted event into `Alert`s; `mie-app` renders them to plain
text and hands one message per engine event to the outbound port
`AlertGateway`. Live wiring uses a Telegram adapter (M2); the replay
dry-run wires a log gateway in `mie-cli`. De-duplication comes from
definitions that already exist — ADR-044's zone debounce and the
classifier's own failure and acceptance rules — and the volume of the
resulting policy is measured on a year of data before any budget or extra
rule is chosen (D6).

*Rejected:* deciding or rendering inside the Telegram adapter. An adapter
may not depend on another adapter (ADR-025), so the replay's audit log
would become a second implementation of the alert logic, which ADR-019
forbids. *Rejected:* a fixed event-time cooldown or a global daily cap
picked up front. It would be a number nobody measured (ADR-013); ADR-054
rejected the same for episodes. The cap that matters is how many messages a
person reads a day, which D6 sets from measured data.

### D1. Placement and bundling

- **Decision** in the domain (`mie_domain::alert`): pure, event time only,
  called after accepted events only — after a rejected event the engine's
  facts still show the previous event (the ADR-054 D6 reason).
- **Rendering** in the app (`mie_app::alert::render`): one text per bundle.
- **Delivery** in adapters, behind `AlertGateway` (D3).
- **One message per engine event** that produced at least one alert. Within
  the event the alerts come in a fixed order: every zone leave, every zone
  enter (both in the order of the location facts, ADR-044 D7), the
  triggers in emission order (ADR-053 D2), the regime change, the report.
  *Why bundle:* a UTC rollover publishes the new prior day, so its report,
  the new levels' zone changes and a trigger arrive at one instant; five
  messages for one instant cost reading time and delivery budget without
  adding information. *Why this order:* what changed before the summary of
  the state it changed.
- The alert log of a replay is the record; it carries the run's
  provenance: data version, engine key, policy and policy fingerprint, as
  ADR-054 D5 does for context records.

### D2. The policy `alert.policy@1`

A `PipelineKey` with a canonical `policy_text()` and an FNV-1a fingerprint
pinned by a golden test, as `conditional.sampling@1` (ADR-054 D5). Any
change below is `alert.policy@2`; the M1 logs stay attributable through
the `@1` text and fingerprint recorded here.

- **a. Monitored levels.** The registry's POC, VAH and VAL whose source is
  `profile.volume.prior_day@1`. *Why these:* the prior day's value area is
  the auction classifier's reference (ADR-044 D8), so every guide (c) and
  every confirmation (d) is defined at exactly these edges, and the brief
  §17 example is a VAH zone. *Why nothing else:* the whole registry yields
  about 1 380 arrivals a day (ADR-044 M3), and no level kind has a measured
  contribution yet (ADR-013). Gate G (D6) may narrow this set from the M1
  measurement; it never widens it, since widening has no measurement behind
  it.
- **b. Zone alerts.** `ZoneEntered` on `LocationEvent::Entered` (`Arrived`
  or `Created`) of a monitored level; `ZoneLeft` on `Left { cause:
  Departed }`, only for a visit whose `ZoneEntered` was emitted. A visit
  whose level is no longer in the registry closes **without** an alert:
  `Retired` at the rollover, or a fresh engine after a chain restart
  (ADR-052 D4), whose registry is warming. *Why silent:* the level
  vanished, price did not move; a "left" there would report a fact that
  did not happen. *Debounce:* ADR-044 D7's own — a level is left only when a
  whole closed 1m bar stayed away from its padded zone. No new constant in
  `@1`; a measured visit-merge gap is a gate G option (G2).
- **c. Guide.** On `ZoneEntered` of a VAH or VAL while
  `location.auction.prior_day@1` is ready, from the accepted region after
  the event: the region a probe across that edge goes toward (from inside
  value at the VAH: above; from above at the VAH: back into value), the
  close that starts it (a close in that region), the closes counted toward
  acceptance (beyond the edge itself), and both outcomes — the failure, a
  close back in the accepted region before `ACCEPTANCE_CLOSES` counted
  closes (`trigger.failed_auction@1` for a probe out of value,
  `trigger.failed_reclaim@1` for one back into it), and the acceptance at
  the `ACCEPTANCE_CLOSES`-th counted close (`trigger.breakout_acceptance@1`
  out of value; back into value, ADR-044 D8 accepts the region and no `@1`
  trigger names it). Each outcome is the stated invalidation of the other.
  Every price is explicit and computed from `TOLERANCE_BPS` and
  `ACCEPTANCE_CLOSES`, never restated: `w = ⌊|p| · TOLERANCE_BPS / 10 000⌋`
  on the 1e-8 grid, which reproduces the classifier's exact tests (for an
  integer gap `g`, `g · 10 000 > |p| · 6` exactly when `g > w`); a test
  checks every bound one unit below, on and above against the classifier.
  Example, VAH approached from inside value: a probe starts with a close
  above VAH + w; failed auction if a close returns below VAH − w (inside
  value) before 60 closes count at or above VAH; breakout acceptance at the
  60th counted close. The guide is side-neutral and deliberately does
  **not** copy the brief's "failed-auction short" wording: it would break
  the ADR-053 naming rule and #38's third criterion (owner decision at STOP
  1). No guide at a POC (no auction rule resolves there) or while the
  auction state is warming up or unavailable — the text says so.
- **d. Confirmations.** `TriggerFired` for `trigger.failed_auction@1`,
  `trigger.failed_reclaim@1` and `trigger.breakout_acceptance@1` only: the
  outcomes a guide names, at the monitored edges by construction. Other
  trigger ids (SFP, CVD divergence, effort vs result, exhaustion,
  absorption, OI expansion) are outside `@1`: they fire away from the
  monitored levels and no guide announces them.
- **e. Regime.** `RegimeChanged` when the label of `volatility.regime.1h@1`
  differs between two consecutive ready values. No hysteresis constant in
  `@1`: the flip rate is unmeasured; the M1 tool measures it and gate G
  decides whether a rule is needed (G4).
- **f. Market report.** At every event that closes a 1d bar — the UTC
  rollover that publishes the new prior day, so the report shows the day's
  new monitored levels: the event time, the last price, the regime (label
  and percentile, or warming up), `location.vwap.utc_day@1`, the auction
  status, the monitored levels with their zones and in-zone flags, and the
  levels in zone ("active monitors"). It is read from the committed state
  only (`MarketReport::read`), so `GenerateMarketReport` gives the same
  report for any window end. *Why daily:* the prior day changes once a day;
  an intraday report is a gate G option (G3), since its cost in messages is
  unmeasured.
- **g. Seeding.** The monitor is built from the state the engine holds
  before its first observed event: the last ready regime label, and every
  monitored level in zone as an open visit whose enter was not emitted.
  Events before `emit_from` are decided but not emitted. *Why:* decisions at
  or after `emit_from` then do not depend on where observation started, so
  a warm live run and a replay of the same events agree (ADR-019). A test
  seeds at three points and compares; A2 checks it on the year.

### D3. The port contract

`AlertGateway::deliver(&AlertMessage) -> Result<(), AlertError>`; a message
carries the event time, the event key, the alerts and the rendered text.

- **Best effort, never blocking the caller on I/O.** The core thread calls
  it; a stalled core thread blocks the capture through the bounded core
  channel (ADR-026), so a slow chat would lose market data. An adapter
  queues and sends on its own thread.
- **Errors are counted and never stop the drive.** Alerts are a by-product
  of the market state; losing one must not lose the state.
- **The decision log is the record; delivery is not.** What was decided is
  reproducible by replay; whether a chat received it is operational.
- `GenerateMarketReport::report(window)` returns the market as of
  `window.end`, with the rejection policy of `ReplayMarket` (ADR-039 D9).

### D4. The wording rule

- No message contains a word of the ADR-053 naming list (bullish, bearish,
  long, short, signal, reversal, continuation, entry) or a trade-instruction
  word (buy, sell, order, position, stop, target, take profit, leverage,
  size), as whole words with a plural `s`. *Why whole words:* "longest" or
  "entered" carry no instruction; a substring rule would forbid plain
  English and push the text toward contortions.
- No price other than levels and rule thresholds (and, in the report, the
  last price and the VWAP, which are the state being reported).
- Every message states its event time in UTC, so a late delivery stays
  visible as late.
- Plain ASCII text without markup, at most 4 096 characters by
  construction: blocks that would pass the bound are replaced by one line
  counting them. *Why 4 096:* Telegram's limit on a message text; the bound
  is the delivery client's, so no message can be rejected for length.
  *Why no markup:* text built from data cannot then break an escape rule.
- Rendering tests cover every alert kind and a bundle; A1 checks every
  message of the measured year.

### D5. Delivery

Filled in by M2 (the Telegram adapter: client, pacing, retries, outbox,
timeouts, secrets), after gate G.

### D6. Gate

Filled in by the gate PR from the M1 measurement, before M2: G1 the daily
message budget B (a personal input, `docs/direction.md`), G2 the monitored
set and any de-duplication rule, G3 the report cadence, G4 the regime
hysteresis. Each decision names the figure that decided it and the options
rejected. Every choice is made on the first half of the year and checked on
the second (ADR-051's halves rule); a choice that fails the second half is
not taken.

## Consequences

- Live and replay share one decision path; the dry-run (`mie alerts
  replay`) writes the log of what live would have sent, reproducible to the
  byte, and `mie alerts report` prints the report as of a window's end.
- The decision costs little next to the engine: per event a scan of its
  location and trigger facts and two lookups; the registry is scanned for
  the three open visits only at closed 1m bars with trades.
- A report at the rollover shows the new day's VWAP and auction state as
  warming up: both start with the day, and the report reads the committed
  state as of that event rather than inventing a value. The last price,
  the regime and the new monitored levels are ready there.
- The guide reads `TOLERANCE_BPS` and `ACCEPTANCE_CLOSES` from the domain;
  a new classifier version moves its prices with it, and the policy text
  names both values, so the fingerprint moves too.
- `@1` alerts nothing for the other ~250 registry levels or the other
  trigger families. That is deliberate (D2a, D2d) and the cost is
  information a person does not get until a measured reason widens the
  policy.
- No message carries a direction; a person reads regions, rules and
  thresholds. Nothing in the path can place an order (ADR-010: there is no
  `ExecutionGateway`).
- Negative: until gate G, the volume is unbounded by design; M1 has no live
  wiring, so nothing is sent.

## Alternatives considered

- **Alerting every registry level** (all of ADR-044 D5): rejected; about
  1 380 arrivals a day before leaves, triggers and reports (ADR-044 M3),
  with no level kind's contribution measured (ADR-013).
- **A cooldown per level or a global cap** in `@1`: rejected (Approach);
  gate G chooses rules from measured rows, each with its stated cost.
- **One message per alert**: rejected (D1).
- **Rendering in the domain**: rejected; wording is presentation, and the
  domain's canonical lines must stay stable for goldens while the text may
  change without a policy version.
- **The brief's "failed-auction short" guide**: rejected (D2c, owner
  decision).
- **A guide for every trigger family**: rejected; only the auction rules
  have outcomes defined at the monitored levels.

## Accept when

- **A1** (M1 year run): 0 wording-rule hits over every message of the #12
  development year (2025-10-01 … 2026-09-30).
- **A2** (M1 year run): a monitor seeded at the window start and one seeded
  at the second half's first accepted event, both emitting from the split,
  emit identical alerts at every event of the second half.
- **A3** (gate G): p95 messages per UTC day ≤ B on each half, for the
  policy the gate adopts.
- **A4** (M3, live): `mie alerts send-test` is delivered, and a cold
  `mie ingest` with alerts across one 00:00 UTC rollover delivers its
  report and journals the same messages a `mie alerts replay --source live`
  of its window logs.

## Measurement

Filled in by the gate PR: provenance, every figure of the M1 tool
(`crates/mie-cli/tests/alert_measure.rs`), the option rows, and cost.

The tool's own constants, each with its reason:

- *FIFO pacing 3 s:* the Telegram Bot API FAQ allows about one message a
  second per chat and 20 a minute per group; the adapter cannot know the
  chat type, and 3 s meets both. The tool reports the queue and delay this
  pacing would reach; D5 decides the pacing.
- *Burst window 60 minutes:* a probe at the monitored edges resolves within
  `ACCEPTANCE_CLOSES` = 60 closes (ADR-044 D9), so an hour is the span in
  which a backlog still describes an unsettled situation.
- *Deciles 10 … 90:* the option grid needs candidate values for the merge
  gap and the dwell taken from the data, not picked; nine deciles span the
  distribution, and a value off them would be a number without a
  measurement.
- *Warm-up days excluded:* days before the regime (214 hourly samples,
  about 8.9 days, ADR-052) and the prior-day profile are both ready count
  low by construction and would bias every per-day figure.
- *Replay warm-up for sample days, 10 days:* the smallest whole number of
  days that covers the regime's 214 hourly samples.
