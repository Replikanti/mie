# ADR-052: History chain and warm-start from a fixed origin

- Status: proposed
- Date: 2026-10-12

Relation to earlier ADRs: this ADR supersedes ADR-041 decision 4 (the cold
engine as the comparison unit) for warm-started runs, ADR-039 decision 6
(one source per replay) for the history chain, and ADR-032 decision 5's
7-day seed lookback for runs with a history. Each keeps its status line
until the PR that changes the behaviour: ADR-039 in W2 (#116), ADR-032 in
W3 (#117) and ADR-041 in W4 (#118). W5 (#119) edits ADR-040 (`proposed`)
decisions 7 and 10 in place. ADR-041's *What the run cannot cover* says
"105 minutes at 15m"; that is the upper bound, true only for a start on a
15m boundary (decision 12). It is accepted text, so it stays as it is and
the correction lives here.

The decisions record the #106 decomposition plan
(<https://github.com/Replikanti/mie/issues/106#issuecomment-6123180013>),
whose decisions D1–D8 were accepted at its STOP 1. Each decision below names
the plan decision it records, as "(#106 plan Dn)", so that the sub-issues'
"plan Dn" references stay resolvable.

## Context

**The research/live skew** (#95, section *Research/live skew*). `mie ingest`
starts a fresh `MarketStateEngine` for every run (ADR-041 Context), while
research replays a window from its first event. Research therefore sees
memory that production never holds:

- Levels that stay unswept far into the past. At the end of the 12-month
  window, 1.80 % of the 15m levels were still unswept, 4.03 % of 1h, 8.43 %
  of 4h and 14.47 % of 1d (#80). The `@1` cap barely binds on 4h and never
  on 1d (21 and 0 active evictions in the year, #95 S5), so the skew
  already exists under `@1`.
- Features that stay cold for days after every restart. ADR-041's *What
  the run cannot cover* lists them: `volatility.atr.1h@1` (14 hourly bars),
  `volatility.regime.1h@1` (214 hourly samples, about 8.9 days), the 5-day
  and 7-day volume composites, structure on 1h, 4h and 1d (7 h, 28 h and
  7 days), `derivatives.funding.settled@1` and
  `location.auction.prior_day@1`.
- `structure.levels.<tf>@2` (`until_swept`, #95) extends the unbounded
  memory to 15m and 1h, where most levels are.

ADR-041 named the gap in its Consequences: "A replay window spanning
restarts still cannot be compared with live state; that would need
warm-starting the live engine from history."

**Option A, decided at #95 STOP 1** (#95 plan D7): live and research both
drive their engine from one fixed history origin, so at any instant t both
hold the state of one chain over the same records. The skew is gone for
every feature, not only structure, nothing is approximated, and there is
still one domain path (ADR-019). The rejected options B, C and E and the
deferred option D are under *Alternatives considered*.

## Decision

1. **The origin is 2025-10-01 00:00 UTC.**
   - Why this day: it is the first day of the #12 archive backfill
     (ADR-034 Acceptance, `docs/data-availability.md`), so the earliest data
     the store holds. Every full-year measurement on record (#80, #94, #96)
     starts there, so those runs already computed the canonical state.
   - Why 00:00 UTC: every bar timeframe divides a day and is epoch-aligned
     (ADR-031 decision 1), archive files are daily (ADR-034) and raw-store
     partitions are UTC dates (ADR-030 decision 4). An origin at midnight
     therefore never splits a bar, an archive file or a partition.
   - Rejected, an earlier origin: there is no data before it.
   - Rejected, a later origin: it discards stored history and reopens the
     skew for the levels confirmed before it.
   - Moving the origin needs a new ADR, because it changes the canonical
     state of every result recorded against it.

2. **The history chain** (#106 plan D1, D2). One provider yields the
   canonical event sequence, and live ingestion, research and equivalence
   all drive their engine through it.
   - **The archive segment:** `ArchiveReplay` (ADR-039 decision 5) over
     `[origin, cutover)`, with the default streams and the `aggTrades` fold
     (ADR-050).
   - **The live segment:** `LiveReplay` over the journaled runs that
     started at or after the cutover, chained by ADR-039 decision 3. Runs
     that started before the cutover are not part of the chain, and a run
     that started before the cutover and ended after it is refused: the
     cutover must not fall inside a run.
   - **The seam:** the first live run continues after the archive's last
     event through the same zero hold-back buffer that chains runs,
     `HoldBack::after`. Live and archive events never meet in one stream,
     so no stitching or deduplication is needed; this is the only part of
     ADR-039 decision 6 this ADR supersedes.
   - **The cutover** (#106 plan D2, accepted at STOP 1 as a rule): 00:00
     UTC of the first day of continuous production capture, with the
     archive imported through the day before. The concrete date is
     recorded by #106's acceptance, after #99's remaining capture sessions
     (the last one before 2026-10-28). Why a rule and not a date now: the
     day continuous capture starts is not known yet, and the earlier runs
     (the soaks, #99's sessions) have multi-day holes between them that
     must stay out of the canonical state. 00:00 UTC for the same reasons
     as the origin.
   - **Holes stay holes** (#106 plan D1): the chain is exactly what live
     delivered. The archive never fills a live hole.
   - Rejected, re-basing the chain on the newest archived day at every
     start: each run's chain would differ, and the canonical past would
     change with every import.
   - Rejected, filling live holes from the archive: live never held that
     state, so the skew would return after every hole.
   - Rejected, a 2026-10-01 cutover (the day after the #12 backfill ends):
     it would pull the October soak runs, their multi-day holes and the
     6.7-day archive-to-live hole (2026-09-30 to 2026-10-07 15:50 UTC,
     ADR-031's justification addendum) into the canonical state.
   - **Every request replays from the origin and is restricted
     afterwards** by ADR-039 decision 4: an event is delivered when its
     time lies in the window, a gap counts at its end, and a gap still
     open at the window's end is withheld and listed as trailing. The
     archive segment spans `[origin, cutover)` for every request, even for
     a window that ends before the cutover.
     - Why: only this makes adjacent windows concatenate exactly, events
       and trailing gaps alike, and makes the "through run k" prefix below
       a prefix of the windowed chain. W3 to W5 rely on both.
     - Rejected, replaying a window from its own start: cheaper, but
       missing-day and fold gaps are then placed "from the window start"
       and the sequencers start without a predecessor, so the gaps at a
       split differ from the union's and W5's two-window drive would
       diverge.
     - Rejected, an archive segment that ends at the window's end when
       that is before the cutover: cheaper for early windows, but its
       missing-day and fold gaps then end at the window's end, while the
       next window delivers them ending at the resumed event, so the gaps
       one window withholds are no longer those the next delivers.
     - The cost: every request replays the whole archive segment (about
       19.2 min per year at r = 1, decision 11), even a short window.
       `--source live|archive` stay for cheap ad-hoc windows.
   - **The live segment starts at the first chain run's start minus
     `RUN_MARGIN_MS`** (300 000 ms) and ends at the window's end. ADR-039
     decision 2 bounds a record's distance from its run's span by 300 s
     (open klines filed at their close, open interest trailing its poll).
     A later start would cut the first run's early records, for example
     open interest trailing its poll, out of the chain. An earlier one
     only adds files of runs outside the chain to the version. A window
     that ends at or before this start has no live segment. Such a window
     then does not list as trailing a gap of the first chain run that
     starts before its end (a seed gap, decision 3); nothing delivered
     changes.
   - **The cutover rule reads the journal only.** A run without `run_end`
     ends, for the rule, at its journaled successor's start (ADR-039
     decision 2), and an open run (no successor) ends after any cutover.
     Why: the rule then never depends on the window. Rejected, refusing
     only when the store holds a record of the run at or after the
     cutover: more permissive, but window-dependent, since a window that
     ends before those records would pass while a longer one fails. A
     crashed last run before the cutover therefore blocks that cutover;
     #106's runbook ends that run with `run_end` or picks a later cutover.
   - **Records of runs outside the chain are skipped** wherever the live
     segment reads them. Extents are still computed over every journaled
     run, so a crashed chain run ends at its successor's start even when
     that successor is outside the chain. Why skipping is exact: each run
     had its own pipeline (ADR-039 decision 1), so a run outside the chain
     never affected a chain run. Records of chain runs and of runs the
     journal does not know keep every ADR-039 decision 2 check.
   - **The "through run k" prefix**, for the warm start and its
     equivalence check: the archive segment and every chain run that
     started before k, over `[origin, end + RUN_MARGIN_MS)`, where `end`
     is the ADR-039 decision 2 end of k's predecessor; by that decision
     every record of the predecessor lies inside it, so none is cut. Run
     k and every later run are skipped, never recomputed. For the first
     chain run the prefix is the archive segment alone. A run that started
     before the cutover has no prefix.
   - **The archive's last metrics row** (23:55 of the day before the
     cutover) is ordered at 00:00 of the cutover day (ADR-034 decision 3),
     so the half-open archive segment excludes it. Open interest after the
     archive's last included row reaches the chain only through the live
     poller.
   - **The archive segment's trailing gaps** (missing days at its end, the
     fold's unchecked tail at the cutover) are reported, not delivered:
     the live segment takes over at the seam, and the seam's seed gaps
     (decision 3) mark that hole.

3. **Holes and chain seeds** (#106 plan D1). For runs with a history, each
   stream's seed is the newest event of the same domain stream in the
   chain, archive included, instead of the newest sealed event of the last
   7 days (ADR-032 decision 5). So every hole between runs carries a
   `Disconnected` gap, and the seam carries one per seeded stream. Seeds
   are journaled in `run_start` (ADR-032 decision 4), so replay
   reproduces them.
   - Why: under the 7-day lookback, a pause of more than 7 days gets no
     gap, and the engine would treat the bars across it as complete. A
     chain seed has no window, so every hole is marked.
   - **At the seam, only streams the archive carries are seeded:**
     `Trades` (aggTrades), `Klines` and `OpenInterest` (metrics)
     (`docs/data-availability.md`). `MarkPrice`, `Liquidations` and
     `OrderBook` have no archive dataset, so the chain holds no earlier
     event of theirs: the first live run's first event of each is
     unseeded and yields no gap, as in a first capture on an empty store
     today. That is correct, because their chain history has no hole,
     only a start, and their features stay `WarmingUp` until it (the
     liquidation windows by design, ADR-042). Inventing a seed, for
     example the origin, would put a year-long `Disconnected` gap into the
     canonical state of the mark, liquidation and book features. From the
     first live run on, these streams have chain events and are seeded
     like every other, so every later hole is marked. `Funding` has no
     live stream (ADR-042 decision 4) and needs no seed.
   - Runs without a history keep the lookback (decision 8).

4. **Uncrossable holes and the effective origin** (#106 plan D8). A hole
   is uncrossable exactly when the domain rejects the first trades-stream
   event after it for closing more than `MAX_BARS_PER_EVENT` one-minute
   bars (`BarError::TooManyBars`, reported by the engine as
   `StateError::TimeJump`; ADR-031 decision 11). The chain's *effective
   origin* then moves to the end of the latest such hole: the chain
   restarts a fresh engine at the first event after it, as ADR-031
   decision 11's recovery prescribes ("replayed as two runs, each from a
   fresh engine"). The effective origin is journaled (decision 6).
   - Why the domain's bound: the chain and the engine then share one
     bound instead of a duration derived a second time. A warm engine
     cannot cross such a hole anyway: a rejected event leaves the
     developing bar, and so the jump, unchanged, so every later
     trades-stream event would be rejected too.
   - Rejected, a cold start after an uncrossable hole: a single
     month-long outage would disable the warm-start at every later start.
   - **Where the rule lives:** `mie_app::drive_chain`, the drive of every
     chain consumer. When the engine returns `StateError::TimeJump`, it is
     replaced by a fresh engine with the same feature set, which then
     applies the event; any other rejection is counted as before. The
     effective origin is the time of that event, the first after the hole
     (for an archive hole, the end of the resuming `MissingData` gap). The
     provider's sequence is unchanged.
   - Rejected, detecting holes in the provider: it would need a second
     derivation of the bar arithmetic; it cannot restart an engine it does
     not own; and moving its start needs the latest hole before the first
     event, so a second pass or a pre-scan of the sealed-file manifests,
     which is exact for archive files but not for holes between live runs,
     whose edges are known only after the recompute.
   - Consequence: the effective origin a report prints is that of its
     drive. A window that starts after a hole drives a fresh engine from
     its first event and does not see that hole; only a drive from the
     origin (the warm-up, the experiment prefix) sees every restart.

5. **The chain's dataset version** covers both segments' selections: the
   archive files `ArchiveReplay` selects and the live files of the chained
   runs. An archive correction (#102, ADR-050) therefore changes it
   visibly, and so does a newly sealed run.
   - **Encoding 1:** the lowercase hex SHA-256 of the text

     ```text
     mie-history-chain-dataset 1
     origin <ms>
     cutover <ms>
     archive <archive segment version>
     live <live segment version, or - without a live segment>
     ```

     with every line ending in `\n`. A change to the text is a new
     encoding.
   - Why a hash over the segments' versions: each already identifies its
     selection (ADR-039 decisions 2 and 5), so the chain's version is a
     function of the two selections alone, and it needs no selection rule
     of its own. A closed window therefore keeps its version while a later
     run goes on capturing, once that run has sealed past the window's end
     plus `RUN_MARGIN_MS` (ADR-039 decision 2). The origin and the cutover
     are in the text because the same files make another chain under
     another cutover.

6. **Configuration and journal.**
   - `[history]` in the ingest configuration: `origin` and `cutover` as
     UTC dates (00:00 UTC is implied, decision 1), plus the path of the
     archive configuration. Unknown keys are rejected, as elsewhere.
     - The keys are `origin`, `cutover` and `archive_config`. Both dates
       accept `YYYY-MM-DD` only, so a time of day is rejected, and the
       origin must be before the cutover.
     - The archive configuration must name the same raw root (compared
       after resolving both paths) and the same symbol: both segments are
       read from one store, for one instrument, and the chain's version
       covers one selection of it. It is loaded when the chain is used, so
       parsing the ingest configuration stays text-only.
     - The origin is configurable, not pinned to 2025-10-01 in code:
       fixtures need other dates, and decision 1 binds production. W3
       journals it with every run. Rejected, refusing any other origin at
       validation: no fixture could build a chain.
   - `run_start` gains `history_origin_ms` and `history_cutover_ms`. They
     are comparability keys, like those ADR-041 decision 3 added, not
     pipeline parameters: ADR-032 decision 4 is unchanged.
   - A new `warm_start` journal record holds:
     - the origin, the effective origin and the cutover;
     - the prefix's dataset version, last event time, event-stream hash
       (ADR-039 decision 7) and domain-rejection count;
     - the state hash at the handoff (ADR-041 decision 1);
     - `ready_after_ms`: the wall time from `run_start` until the queue is
       drained, the moment the live engine is current;
     - the queue's high-water (decision 7);
     - which of ADR-041's long-warm-up features are still `WarmingUp` at
       the handoff;
     - or, instead of the above, an `error` (decision 8).

7. **The warm-up runs concurrently with the capture** (#106 plan D3). The
   capture starts at once. The warm-up replays the chain through the
   current run's predecessor on its own thread, while the core drains the
   live provider into an in-memory queue. At the handoff the queue drains
   into the warm engine through the chain's `HoldBack`, in arrival order;
   a fast-restart overlap becomes a `LateEvent` gap, as in the chained
   replay (ADR-039 decision 3), never an engine `OutOfOrder`.
   - Rejected, warming up before the capture: each restart would lose the
     warm-up's duration of raw data (about 19.2 min per year of history,
     decision 11), and after the cutover no archive fills that hole.
   - Rejected, buffering in the core channel: its 65 536 slots last about
     27 s at the year's peak arrival rate (ADR-026, table below). After
     that the capture thread blocks, which ADR-041's 0 ms blocked-time gate
     forbids.
   - The queue's memory grows with the warm-up time times the arrival
     rate; #106 records its high-water (*Consequences*).

8. **Absence and failure give a cold run** (#106 plan D4). `[history]` is
   optional. A configuration without it, a run started before the
   cutover, or a failing warm-up all capture as today, with a cold engine.
   The `warm_start` record says which, and equivalence compares such a run
   under ADR-041 decision 4. The capture never waits for the warm-up and
   never stops because of it.
   - Why: #99's measurement configuration and every existing fixture keep
     working unchanged, and a broken prefix never costs raw data.
   - Rejected, a mandatory `[history]` that refuses to start: raw data is
     lost whenever the prefix cannot be replayed.

9. **The equivalence unit for warm-started runs** (supersedes ADR-041
   decision 4 for them).
   - `mie equivalence` replays the chain through the run's predecessor
     and checks the prefix's event-stream hash and the handoff state hash
     against `warm_start`.
   - It then drives the run's own events through the decision 3
     `HoldBack` of ADR-039 into the same engine, with a fresh checkpoint
     recorder, and compares the checkpoints as ADR-041 decision 4 does.
     Ordinals and event hashes stay per run (ADR-041 decision 3).
   - A prefix mismatch is `DIVERGED (warm prefix)`. The report prints both
     the journaled and the current dataset version, so a data change
     (decision 5) can be told from a determinism failure.
   - One chain pass serves every run of an invocation. ADR-041 decisions
     1–3 and 5 are unchanged, and runs without a warm start keep the cold
     unit.
   - Why: a warm run's state includes its prefix, so a cold recompute of
     the run alone diverges from it at the first checkpoint by design.

10. **Research: `research.replay_summary@2` behind `--source history`**
    (#106 plan D5).
    - `@2` drives `[origin, sample start)` without evaluating it, then the
      sample as `@1` does.
    - Its new outcome kind (result text v1 allows a new tag, ADR-040
      decision 6) adds the origin, the effective origin, the cutover, the
      prefix's dataset version, event-stream hash and rejections, and the
      state hashes at the sample's start and end.
    - Spec text v1 and the meaning of `data` (the sample's dataset
      version, ADR-040 decision 5) are unchanged.
    - `@1` and its `--source archive|live` path stay unchanged, so ADR-040's
      pending acceptance run stays valid. `@1` reads no Market State, so a
      prefix changes nothing it records. Every later pipeline that reads
      state (#30 onwards) runs on the chain.
    - Rejected, changing `@1` in place: it would void ADR-040's pending
      acceptance run, for the same reason W waited for #13 before changing
      ADR-041's comparison unit.

11. **The startup cost per segment.**
    - **Archive.** Two full-year runs of one tool
      (`crates/mie-cli/tests/location_measure.rs`) over the same 365-day
      window on the same host give the cost of one engine: #96 drove two
      engines in 1 863.0 s, and ADR-044's run drove one in 1 152.2 s. With
      R the replay and E one engine, R + E = 1 152.2 s and R + 2E =
      1 863.0 s, so E = 710.8 s and R = 441.4 s. A year of archive history
      therefore costs about

      `T_archive ≈ Y · (441.4 s + r · 710.8 s)`

      where Y is the years of history and r the engine-time ratio #95 S3
      measures (the engine with `structure.levels.<tf>@2` against today's;
      r = 1 is today's engine). At r = 1 that is 1 152.2 s, 19.2 min per
      year.
    - **Live.** Its rate is *measured by #106*, per day of live history,
      and is never taken from the archive's. A live replay recomputes every
      run's pipeline (ADR-039 decision 1) and replays depth diffs into the
      order book (ADR-038, ADR-043); the archive replay does neither.
    - The formula only dates the forecast. The trigger (decision 12) fires
      on the measured `ready_after_ms`, so a wrong estimate never makes it
      fire wrongly.

12. **Debt 1: the engine snapshot (option D), with a trigger** (#106 plan
    D6). The snapshot becomes due when a live start's journaled
    `ready_after_ms` exceeds **90 min**. No issue is filed before that.
    - **Derivation.** Cold 15m structure needs `Samples(7)` closed 15m bars
      (ADR-037 decision 10; `SWING_WINDOW` = 2·3 + 1). A cold engine's
      first bar is partial (`partial_start`, ADR-031 decision 6), and
      `TimeframeTracker::confirm` counts it like any closed bar. The 7th
      close therefore falls 105 min after the first bar's open, so a cold
      engine has 15m structure `Ready` 105 min − offset after its start,
      with offset ∈ [0, 15 min) the start's position in its bar: more than
      90 and at most 105 min. The test
      `a_cold_start_mid_bar_is_ready_at_the_seventh_15m_close`
      (`crates/mie-domain/src/structure.rs`) pins this at offsets 0, 7.5
      min and 15 min − 1 ms: `Ready` at the 7th close, the partial bar
      included, 105 min, 97.5 min and 90 min + 1 ms after the start.
    - **Why 90.** 15m is the fastest structure timeframe and the busiest
      one (#95). While the warm-up stays at or under 90 min, a warm engine
      is current no later than a cold engine would have had even 15m
      structure `Ready`, wherever in the bar the run started; the regime
      (8.9 days cold) and the composites (5 and 7 days) are far behind
      that. So the warm-start costs no structure availability against a
      cold start and only adds memory.
    - Rejected, keeping #95's ratified 105 min: it keeps the claim "no
      later than a cold start" only for a start on a 15m boundary. For a
      start late in a 15m bar, a warm-up between 90 and 105 min makes 15m
      structure current later than a cold engine would have, the
      regression #95 defined.
    - Rejected, lower than 90 min: the snapshot needs a versioned encoding
      of every internal tracker, which ADR-041 decision 1 kept out of every
      contract. Building it while the replay still beats every cold
      structure warm-up buys no structure availability (#95, "Why not
      lower").
    - `ready_after_ms` is wall time, the cold warm-up event time. Both
      delay a cold engine's 7th close in wall time: a bar closes only at
      the next trades-stream event (ADR-031 decision 4), and the live
      pipeline holds events back by 2 000 ms (ADR-032 decision 6). So the
      comparison errs on the safe side. A live run's first trades-stream
      event is its seed gap, which opens the partial bar the same way
      (`partial_start_marks_the_first_bar_of_every_timeframe`,
      `crates/mie-domain/src/bars.rs`).
    - **Fire estimate**, at archive rates and r = 1: 90 / 19.2 ≈ 4.7 years
      of history (105 min would be about 5.5 years). The live segment is
      not measured yet (decision 11), so the estimate can be far off; #106
      updates it.
    - **The trigger does not see research.** Every state-reading research
      request (`mie replay --source history`, W5's experiments) replays the
      same prefix from the origin (decision 2), at the same cost per year
      of history, while the trigger watches only live starts
      (`ready_after_ms`). Research iteration can therefore become the
      binding cost before the 90 min trigger fires. The trigger and its
      number stay unchanged: no research-cost threshold has been measured,
      so this paragraph records the debt, not a second number.

13. **Debt 2: one live run in memory** (#106 plan D7). `LiveReplay` holds
    one run's records and output at a time (ADR-039 Consequences). With
    the warm-start every live start replays every run since the cutover,
    so the longest of them must fit in memory at the next start.
    - **What breaks if it does not fit.** The warm-up runs inside the
      `mie ingest` process (decision 7). An allocation failure aborts the
      whole process (Rust aborts on a failed allocation, and the OOM killer
      kills the process), so the capture dies with the warm-up, and every
      restart replays the same run and aborts again: a crash loop that
      loses raw data. That would break decision 8, so it must never
      happen.
    - **The guard (W3, #117).** Before it replays the live segment, the
      warm-up predicts the largest run's memory from that run's journaled
      record count times the bytes per record below. When the prediction
      exceeds the memory the host leaves to the warm-up, it journals an
      `error` in `warm_start` and the run starts cold (decision 8),
      without allocating the prefix. The capture never dies from the
      run's size.
    - **The trigger, in records.** `N_max` = the memory the host leaves to
      the warm-up ÷ the bytes per record. The bytes per record are
      measured as the peak memory of an offline `LiveReplay` of the
      longest journaled run on record divided by its record count; the
      host memory is recorded at #106 with its source. Why an offline
      replay: no journaled run is known to reach 24 h (#9's about 30 h
      include a restart, #10 ran 13.7 h in two runs, #13's runs were under
      3 h), so a per-day memory measurement cannot be reached, while bytes
      per record can. The trigger fires when a run since the cutover
      reaches `N_max` records (`run_end.records`, or the sealed count of an
      open run), which is before the guard has to refuse a warm start.
    - **The run length it allows**, a forecast only: `N_max` ÷ 2.40 M
      records per 24 h. 2.40 M is the largest per-day rate of any journaled
      run (table below: #9 run 2, against 1.96 M for #10's seven-stream
      run). The rate follows the day's trading more than the stream set
      (the aggTrades yearly mean alone is 1.65 M a day), so the largest
      measured rate is the conservative basis: a lower one would overstate
      the safe run length. A busier day exceeds 2.40 M; that brings the
      date forward but never past the limit, because the trigger and the
      guard count records, not days.
    - When the trigger fires, a streaming per-run recompute in
      `LiveReplay` becomes due; no issue is filed before that.
    - Rejected, a streaming-recompute sub-issue now: it builds a
      replacement for a limit no run has reached, before the limit is
      known.

## Numbers and where they come from

| Number | Source | Derivation | What breaks at another value |
|---|---|---|---|
| Origin 2025-10-01 00:00 UTC | The first day of the #12 backfill (ADR-034 Acceptance; `docs/data-availability.md`) and the start of the #80, #94 and #96 full-year runs | — | Earlier: there is no data. Later: stored history is discarded, the skew reopens for the levels before it, and the full-year runs no longer computed the canonical state |
| 00:00 UTC for the origin and the cutover | ADR-031 decision 1: every timeframe divides a day and is epoch-aligned. ADR-034: daily archive files. ADR-030 decision 4: UTC-date partitions | — | Any other time of day splits a bar, an archive file or a date partition at the seam |
| The seam hole is under 1 day | The cutover rule (decision 2) | The archive ends the day before the cutover, and the first chain run starts on the cutover day | The seam can never move the effective origin, because it is far below the 31-day bound |
| The live segment starts at the first chain run's start − 300 000 ms (`RUN_MARGIN_MS`); the "through run k" prefix ends 300 000 ms after the predecessor's end | ADR-039 decision 2: a record lies at most `RUN_MARGIN_MS` outside its run's span (open klines filed at their close, up to a minute ahead; open interest trailing its poll by seconds) | — | A later start, or an earlier prefix end, cuts the run's edge records, for example open interest trailing its poll, out of the chain. An earlier start, or a later end, only adds files of runs outside the chain to the version: their records are skipped |
| 31 days = 44 640 1m bars | `MAX_BARS_PER_EVENT` (ADR-031 decision 11, `bars.rs`) | 31 × 1 440 = 44 640. Not chosen here: ADR-031's justification addendum calls 31 days "a round margin over a one-week hole" | Decision 4 follows the domain's bound, not a number of its own. A smaller chain threshold discards history the engine could cross. A larger one leaves a warm engine that rejects every later trades-stream event, because a rejected event leaves the developing bar, and so the jump, unchanged |
| The 7-day seed lookback (superseded for runs with a history) | ADR-032 decision 5, `SEED_LOOKBACK_MS`; ADR-032's justification table marks it "engineering default, not measured" | — | Under the lookback, a pause of more than 7 days gets no `Disconnected` gap. Chain seeds have no window, so every hole is marked (decision 3) |
| 65 536 slots, about 27 s | ADR-026 (`core_channel_capacity`, *Why 65 536*) | Peak rate 60 015 / 25 s + 19/s = 2 419.6/s; 65 536 / 2 419.6 ≈ 27.1 s | Any fixed capacity blocks once the warm-up outlasts capacity / arrival rate. Holding one year's warm-up (1 152.2 s at r = 1) at the peak rate takes 1 152.2 × 2 419.6 ≈ 2.79 M slots, about 245 MB pre-allocated at 88 B per core slot (ADR-026), and that need grows with the history. The quiet side: at the archive's mean aggTrades rate (603 218 226 / 365 days ≈ 19.1/s) plus the other six streams at their p99 rate (19/s, ADR-026), 65 536 slots last about 29 min. So the rejection rests on a burst during the warm-up, since one blocked millisecond fails ADR-041's 0 ms gate, and on the growth with the history, not on the 27 s alone |
| 1 863.0 s and 1 152.2 s, giving 710.8 s and 441.4 s | #96 (two engines) and ADR-044 (one engine): the same tool (`location_measure.rs`), the same 365-day window, the same host | E = 1 863.0 − 1 152.2 = 710.8 s per engine; R = 1 152.2 − 710.8 = 441.4 s for the rest | Caveats: (1) both runs read aggTrades only (ADR-044), while the chain reads the default streams plus the ADR-050 fold, which also reads `trades` files over its 252-day defect window. (2) ADR-044 also records the #11 replay of the same window over the default streams: 20.5 min. (3) "Per year" means per the 603.2 M aggTrades of that year, so a busier year costs more. The formula only dates the forecast; the trigger fires on the measured `ready_after_ms`, so a wrong estimate never makes it fire wrongly |
| 19.2 min per year at r = 1 | The row above | 1 152.2 s / 60 ≈ 19.2 min | r is #95 S3's measured engine-time ratio; at r > 1 a year costs more and the trigger fires sooner |
| The live segment's rate | Not measured | #106 measures the per-day rate | Borrowing the archive's rate would misdate the trigger: the live replay recomputes every run's pipeline and replays depth diffs, which the archive replay does not |
| The 90 min trigger (105 min rejected) | `Samples(7)` (ADR-037 decision 10; `SWING_WINDOW` = 2·3 + 1). The first bar is `partial_start` (ADR-031 decision 6), and `TimeframeTracker::confirm` counts it. Pinned by `a_cold_start_mid_bar_is_ready_at_the_seventh_15m_close` | The 7th close falls 105 min after the first bar's open, so a cold engine is `Ready` 105 min − offset after the start, with offset ∈ [0, 15 min): (90, 105] min | Above 90: for a start late in a 15m bar, the warm engine becomes current later than a cold engine would have had 15m structure `Ready`, the regression #95 defined; 105 holds only for a start on a 15m boundary. Below 90: the snapshot gets built while the replay still beats every cold structure warm-up (#95, "Why not lower") |
| `ready_after_ms` is wall time; the cold warm-up is event time | ADR-031 decision 4 (a bar closes at the next trades-stream event); ADR-032 decision 6 (2 000 ms hold-back) | Both delay a cold engine's 7th close in wall time | So the 90 min comparison errs on the safe side. A live run's first trades-stream event is its seed gap, which opens the partial bar the same way (`partial_start_marks_the_first_bar_of_every_timeframe`, `bars.rs`) |
| Fire estimates: about 4.7 years at 90 min, about 5.5 years at 105 min | The two rows above | 90 / 19.2 ≈ 4.7; 105 / 19.2 ≈ 5.5, at archive rates and r = 1 | The live segment is unmeasured: it replays depth diffs and every run's pipeline. The estimate can be far off, and #106 updates it |
| 2.40 M records per 24 h (debt 2's day forecast) | Capture journals, `run_end.records` over the run's duration (measured in the PR #121 review): #9 run `20261008T035138Z` 1 811 583 records in 18.14 h (five streams, no depth); #10 run `20261008T231852Z` 1 055 736 in 12.93 h (all seven streams); the aggTrades yearly mean 603 218 226 / 365 days | 1 811 583 / 18.14 × 24 ≈ 2.40 M; 1 055 736 / 12.93 × 24 ≈ 1.96 M; 603 218 226 / 365 ≈ 1.65 M a day. The maximum is taken (decision 13). ADR-039's "about 1.5 M for a 24 h run" is below both measured runs and is not used | Lower: the forecast overstates the safe run length (1.5 M would by 2.40 / 1.5 = 1.6 ×). Higher: the forecast comes too early. Neither moves the limit: the trigger and the guard count records (`N_max`), so the rate only dates the forecast |
| 6.7 days (the rejected 2026-10-01 cutover) | ADR-031's justification addendum, decision 11: archive end 2026-09-30, live start 2026-10-07 15:50 UTC | — | It would pull the seam and the October soak holes into the canonical state |

## Consequences

- Every experiment on the chain replays its prefix: about 19.2 min per
  year of history at r = 1, plus the live segment's measured rate.
- Every equivalence check of warm-started runs replays the chain once per
  invocation.
- A correction of archive days before the cutover (#102, ADR-050) changes
  the canonical state. Older warm runs then report `DIVERGED (warm
  prefix)`, with the changed dataset version printed next to the journaled
  one.
- For warm runs, live now covers what ADR-041's *What the run cannot
  cover* lists, except `derivatives.funding.settled@1`, which stays
  archive-only (no live settlement event, ADR-042 decision 4): the
  long-warm-up features are `Ready` at the handoff, and #106's acceptance
  shows it.
- During the warm-up the live engine is current for no feature, including
  those a cold engine has `Ready` within minutes: bars, flow and the
  UTC-day VWAP. #95's *Limits* left this point to W. No consumer of live
  state exists yet (#24, #38), and `ready_after_ms` measures the delay.
- The queue's memory grows with the warm-up time times the arrival rate,
  about 2.79 M events for a one-year warm-up at the year's peak rate (table
  above). #106 records its high-water.

## Alternatives considered

- **(B) Restart-aware research replays**, which reset the engine where
  live restarted or on a fixed cadence (#95). Rejected: restarts are
  operational events with no schedule; the archive part of the history has
  no runs to copy; a cadence would be a number with no source; and it
  closes the gap by making research as poor as a restarted engine instead
  of making production complete.
- **(C) Bound the memory with a TTL or a horizon H** (#95). Rejected: it
  reverses #95's D1, since its R2 result would depend on next year's path,
  the argument that rejected a TTL; and a restarted live engine would
  still need H of uptime to converge.
- **(D) Warm-start from a persisted engine snapshot** (#95). Deferred, not
  rejected: its startup time is constant, but it needs a versioned
  encoding of every internal tracker, which ADR-041 decision 1 kept out of
  every contract, and any tracker change invalidates every snapshot, which
  then falls back to (A). It is the optimisation of (A) and becomes due
  when decision 12's trigger fires.
- **(E) Record the window with each experiment (#29).** Rejected: it
  documents the skew and does not remove it.
- **Re-basing the chain on the newest archived day** — decision 2: each
  run's chain would differ, and the canonical past would change with every
  import.
- **Filling live holes from the archive** — decision 2: live never held
  that state.
- **A 2026-10-01 cutover** — decision 2: the October soak holes and the
  6.7-day seam would enter the canonical state.
- **Warming up before the capture** — decision 7: the restart gap grows by
  the warm-up time, unrecoverable after the cutover.
- **Buffering in the core channel** — decision 7: about 27 s at the peak
  rate, then a blocked capture thread.
- **A mandatory `[history]`** — decision 8: raw data lost whenever the
  prefix cannot be replayed.
- **Changing `@1` in place** — decision 10: voids ADR-040's pending
  acceptance run.
- **Keeping the 105 min trigger** — decision 12: a regression for a start
  late in a 15m bar.
- **A streaming per-run recompute now** — decision 13: before its limit is
  known.
- **A cold start after an uncrossable hole** — decision 4: one month-long
  outage would disable the warm-start for good.

## Accept when

#106's acceptance run, by the main session, passes this coverage list (a
coverage list, as in ADR-041, not a duration):

1. The archive is imported through the day before the cutover, and the
   production configuration carries `[history]`.
2. Two warm-started runs: the first, whose prefix ends at the seam, then a
   second after SIGTERM, whose prefix contains the first. `mie
   equivalence` reports both `EQUIVALENT` with state compared, and a
   second invocation prints identical bytes.
3. At each handoff, `warm_start` shows ADR-041's long-warm-up features
   `Ready`: `volatility.atr.1h@1`, `volatility.regime.1h@1`, both volume
   composites, 4h and 1d structure, and `location.auction.prior_day@1`. A
   cold run cannot show this, so it is the evidence that the warm-start
   carries memory into live. Reachable: the archive segment alone spans
   about a year, far beyond the longest warm-up (8.9 days).
4. 0 ms blocked in every `stats` record of both runs, the warm-up
   included. ADR-041's hard gate applies because the warm-up adds a replay
   thread beside the capture.
5. `ready_after_ms`, peak memory, queue high-water and the per-segment
   rates go into the Acceptance section, with decision 12's fire estimate
   updated. If `ready_after_ms` already exceeds 90 min, the snapshot issue
   is filed then.
6. A `--source history` experiment on a sample after the cutover prints
   `recorded`, then `reproduced`, and the result file's SHA-256 is
   unchanged.
7. The results are posted on #106, and this ADR is flipped to `accepted`.

Two values are written into the Acceptance section with it:

- the concrete cutover date (decision 2);
- debt 2's trigger value (decision 13): `N_max`, the host memory left to
  the warm-up ÷ the bytes per record from an offline `LiveReplay` of the
  longest journaled run on record, each with its source, and the run
  length it allows at 2.40 M records per 24 h (or the largest per-day rate
  of #106's runs, if higher). Reachable without a 24 h run.
