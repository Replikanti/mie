-- The research views over one conditional outcome dataset (#27, ADR-057 D7;
-- format 2, ADR-058 D2).
--
-- Usable from any DuckDB client of the version Cargo.lock pins (ADR-057 D8):
-- set the dataset directory first, then run this file, e.g.
--
--   SET VARIABLE mie_dataset = '/path/to/dataset';
--   .read views.sql
--   SELECT * FROM outcome_summary(30);
--
-- `mie conditional query` verifies the dataset (manifest, content hash,
-- every file's SHA-256) before it creates these views; another client reads
-- whatever files sit in the directory, so run `mie conditional verify` first.
--
-- Every aggregate is order-independent (counts, distinct counts, min/max,
-- nearest-rank quantiles over a sorted list): no floating-point sum or mean,
-- so a result does not depend on how DuckDB schedules its threads. Every view
-- ends with an ORDER BY over its key columns.

-- The base tables, one view per Parquet file (ADR-057 D1, ADR-058 D2).
CREATE OR REPLACE VIEW contexts AS
SELECT * FROM read_parquet(getvariable('mie_dataset') || '/contexts.parquet')
ORDER BY record;

CREATE OR REPLACE VIEW outcomes AS
SELECT * FROM read_parquet(getvariable('mie_dataset') || '/outcomes.parquet')
ORDER BY record, horizon_ms;

CREATE OR REPLACE VIEW passages AS
SELECT * FROM read_parquet(getvariable('mie_dataset') || '/passages.parquet')
ORDER BY record, target DESC;

-- Three rows per record, one per effort window, in the writer's order
-- (5m, 15m, 1h). `window` is a keyword in SQL, so the column is quoted.
CREATE OR REPLACE VIEW effort AS
SELECT * FROM read_parquet(getvariable('mie_dataset') || '/effort.parquet')
ORDER BY record, list_position(['5m', '15m', '1h'], "window");

-- A price column in its integer unit, 1e-8 USDT (ADR-027): exact, so the
-- excursion ratios below are integer divisions in IEEE double. Widened
-- first: DECIMAL(18, 8) times 1e8 does not fit 18 digits.
CREATE OR REPLACE MACRO mie_units(price) AS
    CAST(CAST(price AS DECIMAL(38, 8)) * 100000000 AS BIGINT);

-- The nearest-rank quantile q of a sorted list: its ceil(q * n)-th value,
-- at least the first; NULL for an empty list. An observed value, never an
-- interpolation (the repository's convention).
CREATE OR REPLACE MACRO mie_rank(sorted, q) AS
    sorted[greatest(1, CAST(ceil(q * len(sorted)) AS BIGINT))];

-- One row per record and horizon: the record's context, its outcome, and
-- the compositions ADR-055 D8 left to these views (ADR-057 D7). The context
-- columns are every `contexts` column (format 2 adds four, ADR-058 D2).
--
-- - group_trigger: the trigger id, or 'baseline'.
-- - location_bins: ADR-051 R1's bins, a record in both value_edge and
--   structural_extreme lists both; 'neither' when ready and in none;
--   'not_ready' while the levels are not ready.
-- - offset = upper - reference; up_x, down_x = excursion / offset, NULL when
--   the race is unavailable or the window censored (the excursions are then
--   lower bounds, ADR-055 D6/D7).
-- - time_to_event_ms = race_at - decision_time for an upper or lower race.
-- - level_within, extreme_vs_opposite: the case tables of ADR-057 D7, read
--   against the window end decision_time + H.
CREATE OR REPLACE VIEW labeled AS
WITH joined AS (
    SELECT
        c.* EXCLUDE (data_version, engine, sampling, sampling_fingerprint,
                     labeling, labeling_fingerprint),
        o.* EXCLUDE (record, data_version, engine, sampling,
                     sampling_fingerprint, labeling, labeling_fingerprint),
        pl.price AS level_price,
        pl.passage AS level_passage,
        epoch_ms(pl.at) AS level_at_ms,
        epoch_ms(pl.censor_at) AS level_censor_ms,
        pe.price AS extreme_price,
        pe.passage AS extreme_passage,
        epoch_ms(pe.at) AS extreme_at_ms,
        epoch_ms(pe.censor_at) AS extreme_censor_ms,
        epoch_ms(c.decision_time) AS decision_ms,
        epoch_ms(c.decision_time) + o.horizon_ms AS end_ms,
        epoch_ms(o.race_at) AS race_ms,
        epoch_ms(o.race_censor_at) AS race_censor_ms
    FROM contexts c
    JOIN outcomes o ON o.record = c.record
    LEFT JOIN passages pl ON pl.record = c.record AND pl.target = 'level'
    LEFT JOIN passages pe ON pe.record = c.record AND pe.target = 'extreme'
)
SELECT
    * EXCLUDE (level_at_ms, level_censor_ms, extreme_at_ms, extreme_censor_ms,
               decision_ms, end_ms, race_ms, race_censor_ms),
    coalesce(trigger, 'baseline') AS group_trigger,
    coalesce(trigger_family, 'baseline') AS group_family,
    coalesce(regime, 'not_ready') AS group_regime,
    CASE
        WHEN NOT levels_ready THEN ['not_ready']
        WHEN at_value_edge AND at_structural_extreme
            THEN ['value_edge', 'structural_extreme']
        WHEN at_value_edge THEN ['value_edge']
        WHEN at_structural_extreme THEN ['structural_extreme']
        ELSE ['neither']
    END AS location_bins,
    upper - reference AS "offset",
    CASE WHEN race <> 'unavailable' AND censor_reason IS NULL
        THEN CAST(mie_units(up) AS DOUBLE)
            / CAST(mie_units(upper) - mie_units(reference) AS DOUBLE)
    END AS up_x,
    CASE WHEN race <> 'unavailable' AND censor_reason IS NULL
        THEN CAST(mie_units(down) AS DOUBLE)
            / CAST(mie_units(upper) - mie_units(reference) AS DOUBLE)
    END AS down_x,
    CASE WHEN race IN ('upper', 'lower') THEN race_ms - decision_ms END
        AS time_to_event_ms,
    CASE
        WHEN level_passage IS NULL THEN NULL
        WHEN level_passage = 'at_decision' THEN 'at_decision'
        WHEN level_passage = 'touched' AND level_at_ms < end_ms THEN 'touched'
        WHEN level_passage = 'touched' THEN 'not_within'
        WHEN level_passage = 'not_within' THEN 'not_within'
        WHEN level_censor_ms >= end_ms THEN 'not_within'
        ELSE 'unknown'
    END AS level_within,
    CASE
        WHEN extreme_passage IS NULL OR race = 'unavailable' THEN NULL
        WHEN extreme_passage = 'at_decision' THEN 'extreme_first'
        WHEN race = 'neither' THEN CASE
            WHEN extreme_passage = 'touched' AND extreme_at_ms < end_ms
                THEN 'extreme_first'
            WHEN extreme_passage = 'censored' AND extreme_censor_ms < end_ms
                THEN 'unknown'
            ELSE 'neither'
        END
        WHEN race = 'censored' THEN CASE
            WHEN extreme_passage = 'touched' AND extreme_at_ms < race_censor_ms
                THEN 'extreme_first'
            ELSE 'unknown'
        END
        -- The race was won at race_ms on the extreme's side of the reference.
        WHEN (race = 'upper') = (extreme_price > reference) THEN CASE
            WHEN extreme_passage = 'touched'
                AND (extreme_at_ms < race_ms
                     OR (extreme_at_ms = race_ms AND NOT same_ms))
                THEN 'extreme_first'
            ELSE 'undetermined'
        END
        -- The race was won at race_ms by the opposite barrier.
        ELSE CASE
            WHEN extreme_passage = 'touched' AND extreme_at_ms < race_ms
                THEN 'extreme_first'
            WHEN extreme_passage = 'touched' AND extreme_at_ms = race_ms
                THEN 'undetermined'
            WHEN extreme_passage = 'censored' AND extreme_censor_ms <= race_ms
                THEN 'unknown'
            ELSE 'opposite_first'
        END
    END AS extreme_vs_opposite
FROM joined
ORDER BY record, horizon_ms;

-- Outcome distributions per horizon and per every combination of trigger
-- id, regime and location: 8 grouping sets (ADR-057 D7). A dimension a row
-- does not split by reads 'all'; `split` names the dimensions it does.
--
-- - The sets without location count every labeled row once. The sets with
--   location count one row per bin of the record, so a record in both bins
--   counts in both location groups (ADR-051 R1). Both come out of one
--   GROUP BY over the union of the plain rows (location 'all') and the
--   unnested ones.
-- - The family branch (split 'family') pools the trigger ids of one family
--   and reports only `samples` and `events` (distinct decisions): a
--   family-pooled independent count is undefined (ADR-054 Consequences), so
--   every other column is NULL there.
-- - meets_min_sample: clear >= n_min, the per-horizon clear records of
--   ADR-055 D9. n_min has no default: the threshold is the study's (#28).
-- - The shares divide by the races that ran: upper + lower + neither.
-- - Five-number summaries are nearest-rank over the non-NULL values, with
--   the count used (*_n).
CREATE OR REPLACE MACRO outcome_summary(n_min) AS TABLE
WITH expanded AS (
    SELECT *, 'all' AS location FROM labeled
    UNION ALL BY NAME
    SELECT * EXCLUDE (bin), bin AS location
    FROM (SELECT *, unnest(location_bins) AS bin FROM labeled)
),
grouped AS (
    SELECT
        horizon,
        horizon_ms,
        concat_ws(',',
            CASE WHEN grouping(group_trigger) = 0 THEN 'trigger' END,
            CASE WHEN grouping(group_regime) = 0 THEN 'regime' END,
            CASE WHEN location <> 'all' THEN 'location' END) AS split,
        CASE WHEN grouping(group_trigger) = 0 THEN group_trigger ELSE 'all' END
            AS trigger,
        CASE WHEN grouping(group_trigger) = 0 THEN group_family ELSE 'all' END
            AS family,
        CASE WHEN grouping(group_regime) = 0 THEN group_regime ELSE 'all' END
            AS regime,
        location,
        count(*) AS samples,
        count(DISTINCT {'t': decision_time, 'k': event_kind, 's': event_seq})
            AS events,
        count(DISTINCT episode) AS episodes,
        count(*) FILTER (WHERE clear) AS clear,
        count(*) FILTER (WHERE race = 'upper') AS n_upper,
        count(*) FILTER (WHERE race = 'lower') AS n_lower,
        count(*) FILTER (WHERE race = 'neither') AS n_neither,
        count(*) FILTER (WHERE race = 'censored') AS n_censored,
        count(*) FILTER (WHERE race = 'unavailable') AS n_unavailable,
        count(*) FILTER (WHERE same_ms) AS n_same_ms,
        list_sort(list(time_to_event_ms) FILTER (WHERE time_to_event_ms IS NOT NULL))
            AS tte,
        list_sort(list(up_x) FILTER (WHERE up_x IS NOT NULL)) AS upx,
        list_sort(list(down_x) FILTER (WHERE down_x IS NOT NULL)) AS downx,
        count(*) FILTER (WHERE level_within = 'at_decision') AS level_at_decision,
        count(*) FILTER (WHERE level_within = 'touched') AS level_touched,
        count(*) FILTER (WHERE level_within = 'not_within') AS level_not_within,
        count(*) FILTER (WHERE level_within = 'unknown') AS level_unknown,
        count(*) FILTER (WHERE extreme_vs_opposite = 'extreme_first')
            AS extreme_first,
        count(*) FILTER (WHERE extreme_vs_opposite = 'opposite_first')
            AS opposite_first,
        count(*) FILTER (WHERE extreme_vs_opposite = 'neither') AS extreme_neither,
        count(*) FILTER (WHERE extreme_vs_opposite = 'undetermined')
            AS extreme_undetermined,
        count(*) FILTER (WHERE extreme_vs_opposite = 'unknown') AS extreme_unknown
    FROM expanded
    GROUP BY GROUPING SETS (
        (horizon, horizon_ms, group_trigger, group_family, group_regime, location),
        (horizon, horizon_ms, group_trigger, group_family, location),
        (horizon, horizon_ms, group_regime, location),
        (horizon, horizon_ms, location)
    )
),
summary AS (
    SELECT
        horizon, horizon_ms,
        CASE WHEN split = '' THEN 'all' ELSE split END AS split,
        trigger, family, regime, location,
        samples, events, episodes, clear,
        clear >= n_min AS meets_min_sample,
        n_upper, n_lower, n_neither, n_censored, n_unavailable, n_same_ms,
        CAST(n_upper AS DOUBLE) / nullif(n_upper + n_lower + n_neither, 0)
            AS share_upper,
        CAST(n_lower AS DOUBLE) / nullif(n_upper + n_lower + n_neither, 0)
            AS share_lower,
        CAST(n_neither AS DOUBLE) / nullif(n_upper + n_lower + n_neither, 0)
            AS share_neither,
        coalesce(len(tte), 0) AS tte_n, tte[1] AS tte_min, mie_rank(tte, 0.25) AS tte_p25,
        mie_rank(tte, 0.5) AS tte_p50, mie_rank(tte, 0.75) AS tte_p75,
        tte[len(tte)] AS tte_max,
        coalesce(len(upx), 0) AS up_x_n, upx[1] AS up_x_min, mie_rank(upx, 0.25) AS up_x_p25,
        mie_rank(upx, 0.5) AS up_x_p50, mie_rank(upx, 0.75) AS up_x_p75,
        upx[len(upx)] AS up_x_max,
        coalesce(len(downx), 0) AS down_x_n, downx[1] AS down_x_min,
        mie_rank(downx, 0.25) AS down_x_p25, mie_rank(downx, 0.5) AS down_x_p50,
        mie_rank(downx, 0.75) AS down_x_p75, downx[len(downx)] AS down_x_max,
        level_at_decision, level_touched, level_not_within, level_unknown,
        extreme_first, opposite_first, extreme_neither, extreme_undetermined,
        extreme_unknown
    FROM grouped
    UNION ALL BY NAME
    SELECT
        horizon, horizon_ms, 'family' AS split, 'all' AS trigger,
        group_family AS family, 'all' AS regime, 'all' AS location,
        count(*) AS samples,
        count(DISTINCT {'t': decision_time, 'k': event_kind, 's': event_seq})
            AS events
    FROM labeled
    WHERE sample = 'trigger'
    GROUP BY horizon, horizon_ms, group_family
)
SELECT * FROM summary
ORDER BY horizon_ms, split, trigger, family, regime, location;
