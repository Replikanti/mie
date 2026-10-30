# ADR-058: Order-flow conditional studies — dataset format 2 and a pre-registered, blind study of four hypothesis pairs

- Status: proposed
- Date: 2026-10-30

## Context

#28 asks for the first conditional studies of order flow: absorption,
continuation, no acceptance and failed breakout, each tested against its
counter-hypothesis (brief §11, "Counter-hypothesis"; `docs/direction.md`
T6), on the conditional outcome dataset of #27 (ADR-057). Four things stand
in the way, and each is a decision this ADR takes:

- **The dataset cannot express the studies.** ADR-057 D1 left the
  order-flow snapshot out on purpose: "#28, its consumer, adds a table and
  bumps the format version". Format 1 holds no aggressor, no effort, no
  price response, no CVD and no auction side, so no study of these pairs
  can be written over it.
- **The study must be fixed before an outcome is read.** ADR-055 *Accept
  when* and ADR-057 D9 held every outcome share back until #28 and the
  calibration slice of `docs/direction.md` were fixed. A study whose
  populations, statistic, sample threshold and reading are chosen after
  looking can find whatever it looks for. So this ADR pre-registers the
  whole study, and the tool that runs it prints only the pre-registered
  tables.
- **Samples are not independent.** Baseline records come every minute and
  their forward windows overlap (ADR-054 *Consequences*); a held order-flow
  condition yields a sample every minute for as long as it holds. `docs/
  direction.md` T1 says "n counts episodes, not samples". ADR-057's
  `outcome_summary` counts ADR-055 D9 `clear` records, which is sound for
  its own groups (one trigger id, or the whole baseline) but not for a
  study cell, which is a sub-population of a stream.
- **Time of day confounds order flow.** #124 found effort episodes
  concentrated in 13–16 UTC; #155 measured an hour-of-week volatility cycle
  and found that the New York clock aligns the daylight-saving and
  standard-time profiles (correlation 0.77 → 0.88); labeling v2's barriers
  are scaled by trailing ATR, not deseasonalized (ADR-056). A population
  concentrated in US hours, compared with a time-uniform baseline, would
  read the clock as an effect. #147 assigns this stratification to #28.

The issue ships in three milestones, as #27 did: **M1** dataset format 2
and this ADR (proposed); **M2** the committed study queries under
`docs/research/` and the blind tool, tested on fixtures; **M3** the
calibration slice, the format-2 year built once, the study run once and
its report, which flip this ADR. M1 and M2 land together.

## Decision

### Approach

- **Format 2** is written by the same writer (`mie-adapter-parquet`'s
  `outcome` module) from the same `ContextRecord`, which already carries
  `state.effort`, `state.cvd_utc_day` and `context.auction`. No engine,
  catalog, sampling, labeling or state-hash change; `mie-domain` and
  `mie-app` are not touched.
- **The study queries** are a committed SQL file,
  `docs/research/028-order-flow-interactions.sql`, run over the DuckDB
  views of ADR-057 D7. They define views only: the population members with
  their mapped outcomes, the baseline stream, the exclusion counts, the
  median barrier widths and P4's SFP compositions. `mie-adapter-duckdb`
  gains `create_views` (runs such a file on the in-memory connection,
  which links no extension but Parquet) and `select_tsv` (a query as TSV in
  the form of ADR-057 D7).
- **The tool** is the ignored test `measure_order_flow_study` in
  `crates/mie-cli/tests/order_flow_study.rs`, like the measurement tools of
  ADR-054 … ADR-057. It loads the SQL with `include_str!` and does in Rust
  what SQL here should not: clock arithmetic (ICU is not linked, ADR-057
  D8), the episodes and units (D6), the bootstrap (D5) and the reading
  (D7). The bootstrap reuses the tests' `SplitMix64` and `resample`
  (`tests/common/bootstrap.rs`); the New York clock moves unchanged from
  `season_measure.rs` to `tests/common/clock.rs`. No new crate.
- **A twin** written from this text rebuilds the study views and every
  cell's unit count from the base views; the tool compares it before it
  prints anything.

### D1. Data read

- **Read:** the #12 development year, records = replay = 2025-10-01 …
  2026-09-30 (`1759276800000..1790812800000` ms), built in format 2 with
  the ADR-057 D9 flags, minus any range passed in `MIE_STUDY_EXCLUDE`
  (`from..to` ms, comma-separated). The excluded ranges are dropped in the
  first study view, before any other view reads a record. Under the
  owner's inputs the calibration slice lies in the hold-out year, so
  nothing is excluded; the mechanism exists for the case the slice moves
  into this year.
- **Not read:** the 2024-10-01 … 2025-09-30 hold-out store, beyond building
  the calibration slice in M3 (which queries nothing), and every live
  store. Both stay unread for the walk-forward and forward tests (#30,
  #33).
- **Before it prints anything** the tool asserts:
  - `verify` passes, and the manifest's records window is the year;
  - provenance `data 5d9e27f862ac54dd…224d`, `engine
    4c844593779da2e3-1`, `conditional.sampling@1 2c71fff23e8b0415`,
    `conditional.labels@2 6f90bff079e746f2`;
  - the reconciliation counts of ADR-057 *Acceptance* on the whole
    dataset (561 695 contexts, 525 599 baseline, 36 096 trigger; per
    horizon the `neither`, `unavailable`, `end_of_data`, clear baseline and
    clear trigger counts) and 3 × 561 695 effort rows;
  - 0 mismatches between the study views and the twin, and between the
    tool's and the twin's unit counts.
  If any fails, it stops before printing.

### D2. Format 2

- **New table `effort.parquet`.**
  - Rows: one per record × effort window (`5m`, `15m`, `1h`, in
    `catalog::CANDIDATE_EFFORT_RESULT` order), so 3 per record, in record
    order.
  - Always set: `record`, `window`, `state` (`ready`|`warming`|
    `unavailable`).
  - NULL unless ready: `aggressor` (`buy`|`sell`); `effort`, `effort_high`
    (`Decimal128(18, 8)`, the ADR-027 quantity unit); `progress`,
    `move_median`, `move_high` (`Decimal128(18, 8)`, price); `partial_start`,
    `feed_gap`, `active` (Boolean).
  - Then the six provenance columns.
  - Only fields the study reads are stored (ADR-054 D2's rule), plus
    `active` (ADR-051 D5's own flag, so a reader can check P1's definition
    against it). Left out: `effort_median` and `end`; the raw
    `flow.window.*` values (`effort`, `aggressor` and `progress` already are
    |delta|, the sign of delta and displacement × that sign); liquidity
    (*Out of scope*).
  - `window` is a keyword in SQL; the views and the study queries quote it.
- **`contexts` gains four columns, after `atr_1h` and before provenance:**
  `auction_side` (`above`|`in`|`below`; NULL when the auction status is not
  ready or has no side), `cvd_utc_day` (`Decimal128(18, 8)`, quantity),
  `cvd_partial_start`, `cvd_feed_gap` (all three NULL unless ready). *Why
  on `contexts`:* one table per grain (ADR-057 D10), and the snapshot's
  `reference` and `atr_1h` live there already.
- **Encoding** as ADR-057 D2: exact decimals, a quantity beyond
  ±999 999 999 999 999 999 units is `OutcomeError::Range`, never rounded;
  names from exhaustive `match`es (`Region`, `Aggressor`, the feature-value
  state), pinned by tests.
- **Version and verification.** The key-value metadata reads `2`, the
  manifest header `mie-outcome-dataset 2`, and a line `file effort.parquet
  <rows> <sha256>` follows the passages line. `verify` also checks the
  effort table's SHA-256, rows, format and schema, and 3 effort rows per
  `contexts` row. A format-1 manifest is refused with an error that names
  format 1 and asks for a rebuild (the strict parser of ADR-057 D3).
- **Views.** `views.sql` gains an `effort` base view, ordered by `record`
  and window order. `labeled` picks up the four `contexts` columns through
  `c.*`. `outcome_summary` is unchanged. Without the new columns,
  `contexts` and `labeled` are exactly their format-1 views: M3 asserts the
  view digests ADR-057 *Acceptance* published on that projection, which
  shows the rebuilt year holds the accepted content unchanged.

### D3. Hypothesis pairs, identical samples

Every pair names one barrier as the hypothesis and the other as the
counter-hypothesis, on the same samples. `ready` below means `state =
ready` with neither `partial_start` nor `feed_gap` ("covered").

| Pair | Population | Hypothesis (barrier first) | Counter-hypothesis |
|---|---|---|---|
| P1-buy / P1-sell (brief: absorption) | Baseline records whose effort window `w` is covered, `effort ≥ effort_high` and `progress ≤ move_median` (ADR-051 D5's `active`), aggressor buy / sell | away from the aggressor | the aggressor's side |
| P2 (brief: continuation) | As P1, but `progress ≥ move_high`; both aggressors, pooled through the mapping | the aggressor's side | the opposite side (exhaustion) |
| P3 (brief: no acceptance) | Baseline records with `effort ≥ effort_high` on a covered window `w`, `auction_state ∈ {failed_breakout, failed_reclaim}`, and the aggressor in the probe's direction: `auction_side` for `failed_breakout`, the opposite side for `failed_reclaim` (ADR-044 decision 8: a failed breakout's side is the region the probe went to; a failed reclaim's is the accepted region the probe left toward value) | A: against the probe (the failure follows through) | B: in the probe's direction (the probe resumes) |
| P4 (brief: failed breakout) | Trigger records of `trigger.failed_auction@1` (failed side = `auction_side`) and of each `trigger.sfp.<15m\|1h\|4h\|1d>@1` (failed side = the side of the extreme passage's price from the level passage's), one cell per trigger id; no effort condition | away from the failed side (reversal) | the failed side (delayed continuation); for SFP ids also the `extreme_vs_opposite` counts of ADR-057 D7 over the cell's own members |
| P1-trig (secondary) | Trigger records of `trigger.effort_result.<w>@1` whose window `w` is covered, aggressor from that window | as P1 | as P1 |

- Every pair is measured per effort window (where it uses one) and per
  horizon 5m … 1d: 4 × 3 × 5 = 60 cells for P1-buy, P1-sell, P2 and P3,
  5 × 5 = 25 for P4 and 3 × 5 = 15 for P1-trig, **100 primary cells**.
- Excluded from the shares and counted in the output: races `censored` or
  `unavailable` at the horizon; effort values not ready or with
  incomplete coverage (per universe: the baseline per window, each
  `trigger.effort_result.<w>@1`); P4 triggers without a failed side (no
  auction side, a missing passage or an extreme at the level).
- Naming: dataset and view columns stay neutral (ADR-054 D1). "Absorption",
  "continuation", "reversal" and "exhaustion" are hypothesis names here and
  in the report only.

### D4. Statistic

- For one cell, among its members whose race ran (`upper`|`lower`|
  `neither`): the hypothesis share P_h, the counter share P_c and the
  neither share P_n.
- The tilt **T = P_h − P_c**. The barriers are symmetric (ADR-056 D4′), so
  this is the mean gross outcome in R per race (+1, −1 or 0).
- The baseline tilt **T_b** is directly standardized: for each member,
  take the tilt of the same physical barriers among the baseline records
  of its stratum whose race ran ((n_upper − n_lower) / n for an upper
  hypothesis, its negative for a lower one), then average over the
  members.
  - **Stratum** = New York hour of week (168) × the record's location-bin
    set (`not_ready`, `neither`, `value_edge`, `structural_extreme`, both
    bins): 840 strata.
  - The New York clock is #155's: UTC − 4 h on US daylight-saving days and
    UTC − 5 h otherwise, by UTC date.
  - In a regime-split cell (D8) the baseline is restricted to the same
    regime. In a location-split cell the stratum already holds the bin set,
    and every bin set of a member in bin `v` lies in `v`, so the restriction
    is implied. Side and CVD splits are properties of the aggression, which
    a baseline record without a condition does not have: no restriction.
- **The effect E = T − T_b.**
- A member whose stratum holds no baseline race is dropped from T and T_b
  and counted (`dropped`).
- *Why standardize by time and location:* the clock confound of the
  *Context*; location because ADR-051 R1 bins carry their own outcome
  profile and order-flow conditions are not uniform across them. Regime
  confounding remains; the regime splits show it rather than remove it.

### D5. Uncertainty

- **Method:** a circular block bootstrap (Politis and Romano 1992) over
  the UTC days of the decision times that hold a study record, with the
  tests' `SplitMix64` and `resample`. Members and baseline are drawn with
  the same days (paired): one resample is one multiset of days, and both T
  and T_b are recomputed from it.
- **Estimation uses every member of the cell,** not only the units D6
  counts; the block bootstrap carries the overlap dependence.
- **Settings:**

  | Setting | Value | Reason |
  |---|---|---|
  | Primary block | 56 days | Whole weeks keep the weekday cycle inside each block. A race on ATR-scaled barriers depends on volatility only through the ratio of forward realized volatility to the trailing ATR, so its persistence is at most that of ln σ: on the #12 year that dependence, without the weekday cycle, is about 0.12 at 28 days and −0.06 at 56 (measured in ADR-056 *The noise, measured on v1*). Here it bounds how long a block must be to carry the dependence of a race outcome, which is why the same length serves. |
  | Sensitivity block | 28 days | The shortest whole-week block that still carries most of that dependence. A reading that changes between 28 and 56 depends on how much residual dependence the block carries, and is flagged `block-sensitive`. |
  | Resamples | 10 000 | The interval's ends are the 2.5 % and 97.5 % quantiles at c = 0.95. The Monte Carlo error of an empirical tail quantile's level is √(p(1 − p)/B): at B = 10 000 the lower end sits at 2.5 ± 0.16 % (a 6 % relative error of the tail probability); at 1 000 it would be ± 0.49 % (20 %). Each resample is one pass over per-day counts (implementation, below), so the larger count costs no replay. |
  | Seed | 28 | The issue number, named before the run; both block lengths start from it. |
  | Interval | Nearest-rank percentiles (1 − c)/2 and (1 + c)/2 of the defined resamples | The repository's quantile convention: an observed value, no interpolation. c is the owner's confidence level. |

- A resample in which the cell keeps no race that ran is undefined. If
  more than (1 − c)/2 of the resamples are undefined, the cell reads
  `insufficient`: the interval's own tail would rest on missing values.
- Implementation: per day × stratum counts, so a resample costs one pass
  over the days' counts, not over 525 599 baseline rows. The resamples are
  split over threads; every resample's result is stored by its index, so
  the output does not depend on the thread count (tested).

### D6. Minimum sample

- **Threshold.** n_min = ⌈(z/μ)²⌉: `docs/direction.md` T1 applied to the
  gross race outcome X ∈ {−1, 0, +1}, whose σ ≤ 1 for any distribution.
  - z = Φ⁻¹((1 + c)/2), from Acklam's rational approximation (relative
    error below 1.2e-9, pinned in the tool; no statistics crate); μ = the
    owner's smallest net expectancy worth trading, in R per trade.
  - The bound σ ≤ 1 needs no calibration data. A measured σ (σ² = 1 − P_n
    − T²) can only lower n_min, so the bound errs toward `insufficient`.
  - Under the owner's inputs (c = 0.95, μ = 0.10 R): z = 1.959964,
    (z/μ)² = 384.15, **n_min = 385**.
- **What n_min is compared with: `units`,** the independent units of one
  cell at one horizon, counted in two stages.
  1. **Episodes.** An episode is one occurrence of the event that defines
     the population.
     - P4 and P1-trig (trigger records): the record's `episode` id
       (ADR-054 D4). Each such cell holds one trigger id, so the count is
       defined; a family-pooled count is not (ADR-057 D7).
     - P1 and P2 (baseline records, which carry no episode, ADR-054 D4):
       ADR-054 D4's join rule over each member's effort span `[decision −
       w, decision − 1 ms]`, the span ADR-054 gives
       `trigger.effort_result.<w>@1`. Every span has length w and ends at
       its decision, so in decision order a member joins the open episode
       when it lies less than w after the previous member, and otherwise
       opens a new one.
     - P3: as P1, and members inside one held failure also chain. A
       failure run is a maximal stretch of consecutive baseline records
       with the same `auction_state` ∈ {`failed_breakout`,
       `failed_reclaim`} and the same `auction_side`. ADR-044 decision 8
       holds a failure until 60 closes count back or a new probe starts,
       so one failure can hold for hours. *Why:* the failure is the event
       P3 is about, and every burst inside it shares it. Location and
       regime are not chained so: ADR-054 D4 leaves the places a fact is
       judged against out of the span.
     - Episodes are formed over the whole population (pair × window or
       trigger id), before any split, so a split cell keeps an episode
       whose opener lies in the other split value.
  2. **Horizon thinning.** Each episode is represented by its earliest
     member in the cell whose race at H ran; an episode without one adds
     nothing. Over the representatives in decision order the first counts,
     then each whose decision is at least the last counted one's + H:
     ADR-055 D9's greedy rule, run per cell instead of per stream. Taking
     the earliest each time gives the largest set of representatives with
     pairwise disjoint windows (tested against a brute-force maximum).
  The count reads decision times, episode ids, failure runs and whether a
  race ran, never which barrier won, so the gate cannot select on the
  outcome (tested by swapping the barriers).
- *Why both stages, and not either alone:*
  - Episodes alone leave forward windows overlapping: two failed-auction
    episodes 2 h apart run their 1d races over the same 22 h.
    `trigger.failed_auction@1` has 697 episodes in the year (ADR-054
    *Acceptance*) and would pass n_min = 385 at 1d, although the year holds
    at most 365 disjoint 1d windows.
  - Horizon thinning alone counts one held condition many times: a 1h
    absorption condition held for 3 h is one episode, but 36 units at 5m
    and 3 at 1h.
  - ADR-055 D9 `clear` is thinned by horizon per stream, not per cell. A
    cell is a sub-population of the baseline stream, so its `clear` count
    is the stream's grid of clear minutes that happens to fall inside it:
    too sparse for short-lived conditions (one minute a day at 1d), too
    dense for one long-held condition, which spans several grid points with
    one effort window or one failure.
  - Openers as the only representatives (ADR-055 D9's choice) would make
    an episode vanish from a cell its opener is not in, or whose opener's
    race was censored.
- **Estimation is unchanged** (D5): E and its interval use every member.
  Only the gate uses `units`.
- **`outcome_summary(n_min)` is not used and not changed.** Its
  `meets_min_sample` counts `clear`, sound for its own groups: for an
  unsplit trigger id `clear` is this rule with openers as representatives
  (equal to `units` when every opener's race ran, tested), for the baseline
  the thinning of the whole stream. The study's cells are not its groups.
- **Consequence (intended).** Disjoint windows cap `units` in the 365-day
  year at 105 120 / 35 040 / 8 760 / 2 190 / 365 for 5m / 15m / 1h / 4h /
  1d.
  - Every 1d cell reads `insufficient` by construction (365 < 385).
  - At 4h a cell needs units in 385 of the 2 190 4h windows (17.6 %), so
    most split cells read `insufficient`.
  - Episode counts bound P4 and P3 further: `trigger.sfp.4h@1` (203
    episodes) and `trigger.sfp.1d@1` (35) cannot pass at any horizon; P3
    draws on about 1 127 held failures (697 failed-auction and 430
    failed-reclaim episodes), and only those with aggression into the probe
    count.
  - A cell below n_min cannot resolve an effect of size μ at confidence c,
    so reading it would read noise. Its shares, E and interval are still
    printed; only the reading is `insufficient`.

### D7. Reading

- Each cell reads one of:
  - `hypothesis`: the whole interval of E is above 0;
  - `counter`: the whole interval is below 0;
  - `inconclusive`: the interval contains 0;
  - `insufficient`: `units` < n_min (D6), or too many undefined resamples
    (D5).
- A cell is flagged `block-sensitive` when the 28-day block gives another
  reading than the 56-day block.
- Each family (pair) gets two multiplicity lines, primary cells and split
  cells: N cells read (`insufficient` cells are not read), K intervals that
  exclude 0, and (1 − c)·N expected by chance if no effect existed.
- No permutation null and no false-discovery-rate correction here:
  `docs/direction.md` T3 belongs to the Research Lab (#32). Nothing is
  promoted.

### D8. Secondary splits

The same statistic, and each split cell counts its own `units` (D6 runs
over the split cell's members, with the population's episodes):

- regime label (`low`, `medium`, `high`, `extreme`; a member whose regime
  is not ready is in no regime cell): every pair;
- location bin (`value_edge`, `structural_extreme`, `neither`,
  `not_ready`; a record in both bins is in both cells, ADR-051 R1): every
  pair;
- aggressor side (`buy`, `sell`): P2 and P3;
- for P1 and P2, whether the aggressor agrees with the sign of
  `cvd_utc_day`: `with`, `against`, `flat` (zero). A member whose CVD is
  not ready or has incomplete coverage is in no CVD cell.

Per horizon that is 11 split values for P1-buy and P1-sell, 13 for P2, 10
for P3 and 8 for P4 and P1-trig; over the windows, trigger ids and five
horizons, **995 split cells**.

### D9. Output

The tool prints, as markdown, only:

- the dataset manifest's identity (content, data, engine, sampling,
  labeling, code), the queries file's SHA-256, the inputs (c, μ, z, n_min,
  the fee) and the excluded ranges;
- per horizon: the median barrier half-width of the baseline in bps (from
  `upper − reference`, never from a race), the round-trip fee in R at that
  width (fee bps ÷ median half-width bps; the owner's fee tier), and the
  count of disjoint windows in the days read;
- the exclusion counts (D3);
- per population, the primary table and one table per split dimension,
  each row: split value, horizon, n, censored, unavailable, dropped,
  episodes, units, P_h, P_c, P_n, T, T_b, E, the 56-day interval, the
  reading and its flags;
- P4's SFP `extreme_vs_opposite` counts;
- the multiplicity lines.

No per-record output: no record number, decision time or episode id.

### D10. Where each number comes from

| Number | Source | What breaks at another value |
|---|---|---|
| Format version 2; 3 effort rows per record | D2: the next version after ADR-057's 1; `catalog::CANDIDATE_EFFORT_RESULT` has three windows | — (a label, bumped on any column or line change; another window count is another catalog, not this format) |
| `Decimal128(18, 8)` for quantities | ADR-057 D2's reasoning applies unchanged: scale 8 is the ADR-027 quantity unit, precision 18 the largest plain INT64 | Below 8: quantities rounded; above 18 digits: FIXED_LEN_BYTE_ARRAY for range no BTC quantity needs |
| 100 primary cells | D3: 4 pairs × 3 windows × 5 horizons + 5 P4 ids × 5 + 3 P1-trig ids × 5 | — (a count of what D3 names) |
| 995 split cells | D8: (2 × 11 + 13 + 10) split values × 3 windows × 5 horizons + 8 × (5 + 3) ids × 5 | — (a count of what D8 names) |
| 840 strata (168 × 5) | D4: hours in a week; the five location-bin sets | Fewer hours (hour of day): weekends pooled with weekdays although #155 measured a weekly cycle; finer (minutes): strata too thin to hold baseline races, more dropped members |
| New York clock, −4 / −5 h by UTC date | #155: the profile clock that aligned DST and standard time (0.77 → 0.88) | UTC: the US hours that hold the effort episodes (#124) sit one UTC hour later in standard time than in daylight-saving time, so every UTC stratum mixes two New York hours across the year |
| Block 56 days | D5: whole weeks; ln σ's dependence without the weekday cycle ≈ 0.12 at 28 d, none at 56 d (measured in ADR-056 on the same year) | Shorter: the intervals miss residual dependence and read too narrow; not whole weeks: blocks hold some weekdays more than others; longer: fewer than 6.5 blocks a year, and the interval rests on fewer units |
| Block 28 days (sensitivity) | D5: the shortest whole-week block that keeps most of the dependence | — (a flag, not a reading) |
| 10 000 resamples | D5: Monte Carlo error of the 2.5 % tail quantile, ± 0.16 % at 10 000 | 1 000: ± 0.49 %, a 20 % relative error at the interval's end |
| Seed 28 | D5: the issue number, fixed before the run | — (any fixed seed; a seed chosen after seeing results would be a choice) |
| Interval at (1 − c)/2 and (1 + c)/2 | D5: two-sided at the owner's confidence level | — (follows from c) |
| Undefined limit (1 − c)/2 | D5: more undefined resamples than the tail would put the interval's end on missing values | Higher: an end read from a resample set that misses its tail; lower: cells refused whose ends are well defined |
| n_min = ⌈(z/μ)²⌉ | D6: T1 with σ ≤ 1 and the owner's c and μ | Larger: fewer cells read; smaller: cells read that cannot resolve μ |
| Thinning step H | D6: the race's longest window | Shorter: counted races overlap; longer: fewer units than disjointness needs |
| Chaining span w | D6: ADR-054 D4's evidence span for `trigger.effort_result.<w>@1`: two members less than w apart share window data | Shorter: members sharing window data counted twice; longer: members sharing no datum chained |
| P3 failure runs | D6: ADR-044 decision 8 holds a failure for up to 60 counted closes or until a new probe | Without: one held failure counted once per disjoint burst |
| 105 120 / 35 040 / 8 760 / 2 190 / 365 | D6: 365 days × 1 440 min ÷ H | — (arithmetic) |
| Data `5d9e27f8…`, engine `4c844593779da2e3-1`, sampling and labeling keys; the reconciliation counts | D1: the provenance and counts of ADR-054 *Acceptance*, ADR-056 *Measurement* and ADR-057 *Acceptance* | — (a difference is a bug or a provenance change, checked first) |
| Acklam's approximation | D6: relative error below 1.2e-9, so n_min is exact unless (z/μ)² lies within about 1e-6 of an integer | — (pinned; z(0.95) = 1.959964 within 1e-6, tested) |

There is no other number.

### Out of scope

- **Passive liquidity:** no `book.*` column and no absorption cells by
  resting liquidity. Archive replays have no book (ADR-043), and the
  accepted R0 live run holds 2 185 records; reading it would spend
  forward-test data on cells that cannot reach n_min. A follow-up issue
  takes it up when live capture is large enough.
- Net outcomes in R (fees beyond the per-horizon print, slippage,
  funding), permutation nulls, false discovery rate and the T2–T6 verdicts:
  the Research Lab (#30, #32, #33).
- Changing `outcome_summary` or its `clear`-based flag (ADR-057,
  accepted).
- Experiment specs and result-store entries (ADR-040).
- Any engine, catalog, sampling, labeling or state-hash change; labeling
  v3; history-chain sources; views across several datasets.
- Promoting any finding to a strategy.

## Consequences

- **Format 2 invalidates the accepted format-1 datasets.** They must be
  rebuilt (about 70 minutes for the year); M3 shows on the format-1
  projection that the rebuilt content equals ADR-057's.
- 100 primary and 995 split cells give spurious readings at
  rate 1 − c each if no effect exists. The multiplicity lines show the
  expected number; primary and split cells are kept apart, `insufficient`
  cells stay out of N, and nothing is promoted.
- Most 4h cells and every 1d cell read `insufficient` on one year (D6
  *Consequence*). The report prints `episodes`, `units` and the ceiling,
  so the gap is visible; the long horizons go to the Research Lab with
  more data.
- The unit count is conservative: chaining merges bursts whose windows
  overlap by a minute, and P3 merges bursts inside one held failure. The
  cost is more `insufficient` cells, never a spurious reading.
- Effort warm-up: the first day of the year has no ready effort values;
  those samples are excluded and counted.
- The dataset grows by one table of 3 × 561 695 rows on the year; the
  writer holds one row group per table as before.
- `mie-adapter-duckdb` can run statements a caller passes
  (`create_views`), on its in-memory connection without network or other
  extensions.

## Alternatives considered

- **A format-1 projection: re-deriving order flow in SQL from other
  columns.** Format 1 holds none of the inputs. Rejected.
- **A separate per-record order-flow table** for the four `contexts`
  columns. One table per grain (ADR-057 D10); they are per record.
  Rejected.
- **Re-reading the snapshot by replay inside a study tool** (the
  `context_measure` pattern). Every iteration would cost the 70-minute
  year replay, and the inputs would not be pinned by a content hash.
  Rejected.
- **Bootstrapping in SQL.** DuckDB's `random()` depends on thread
  scheduling, and ADR-057 D7 bans aggregates whose result depends on
  order. Rejected.
- **Storing liquidity now.** No cell can use it (*Out of scope*); ADR-054
  D2's rule. Deferred.
- **`clear`, episodes alone, or horizon thinning alone as the D6 count.**
  D6. Rejected.
- **A measured σ in n_min** (the calibration slice's). It can only lower
  n_min, it needs outcome data from the slice, and the bound is valid for
  any distribution. Rejected for this study.
- **A permutation null and FDR now.** T3 is the Research Lab's (#32); this
  study prints the expected count instead and promotes nothing. Deferred.

## Accept when

M3, in the main session, from the merge commit of this ADR's code:

- the calibration slice (2024-10-01 … 2024-12-31, hold-out archive) is
  built in format 2 and verified, and its content hash is reported; it is
  not queried;
- the owner's `docs/direction.md` *Personal inputs* are on main;
- the #12 year is built once in format 2 from a clean commit, two builds of
  the R0 live store are byte-identical, `verify` passes on all three, and
  `measure_outcome_views` passes on the year, including the ADR-057 digests
  on the format-1 projection;
- `measure_order_flow_study` runs once with the inputs of
  `docs/direction.md` and passes its D1 checks;
- the report `docs/research/028-order-flow-interactions.md` records the
  dataset, the queries hash, the commands, the inputs and every table, null
  results and `insufficient` cells included.

Null results and `insufficient` cells do not block acceptance. The
acceptance PR adds an *Acceptance* section and sets this ADR to accepted.
