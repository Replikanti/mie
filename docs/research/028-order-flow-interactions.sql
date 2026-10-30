-- The order-flow interaction study of #28, pre-registered by ADR-058.
--
-- Run it after views.sql over one verified format-2 outcome dataset
-- (ADR-057 D7, ADR-058 D2). It only defines views. The measurement tool
-- `crates/mie-cli/tests/order_flow_study.rs` reads them, and it does in Rust
-- what this file does not: the New York hour of week, the strata, the
-- baseline standardization, the episodes and independent units (ADR-058
-- D4–D6), the bootstrap and the reading. There is no clock arithmetic here:
-- DuckDB's time-zone support (ICU) is not linked (ADR-057 D8).
--
-- Exclusions (ADR-058 D1): every decision time in a range [from_ms, to_ms)
-- of `study_exclude` is dropped in `study_records`, before any other study
-- view reads a record. The tool fills the table from MIE_STUDY_EXCLUDE.
--
-- Every view ends with ORDER BY its key columns, and every aggregate is
-- order-independent (counts, max over one value, a nearest-rank median of a
-- sorted list), so the rows do not depend on how DuckDB schedules threads.
-- The hypothesis names ("absorption", "continuation", ...) live in ADR-058;
-- the columns here stay neutral (ADR-054 D1).

CREATE TABLE IF NOT EXISTS study_exclude (from_ms BIGINT NOT NULL, to_ms BIGINT NOT NULL);

-- The records the study reads. `location` is the record's ADR-051 R1 bin
-- set: 'both' when it lies in a value edge and at a structural extreme.
CREATE OR REPLACE VIEW study_records AS
SELECT
    c.record,
    c.sample,
    c.trigger,
    c.episode,
    epoch_ms(c.decision_time) AS decision_ms,
    coalesce(c.regime, 'not_ready') AS regime,
    CASE
        WHEN NOT c.levels_ready THEN 'not_ready'
        WHEN c.at_value_edge AND c.at_structural_extreme THEN 'both'
        WHEN c.at_value_edge THEN 'value_edge'
        WHEN c.at_structural_extreme THEN 'structural_extreme'
        ELSE 'neither'
    END AS location,
    c.auction_state,
    c.auction_side,
    c.cvd_utc_day,
    c.cvd_partial_start,
    c.cvd_feed_gap
FROM contexts c
WHERE NOT EXISTS (
    SELECT 1 FROM study_exclude x
    WHERE epoch_ms(c.decision_time) >= x.from_ms
      AND epoch_ms(c.decision_time) < x.to_ms
)
ORDER BY c.record;

-- The race of every record per horizon (ADR-055 D4), one column each.
CREATE OR REPLACE VIEW study_races AS
SELECT
    record,
    max(race) FILTER (WHERE horizon = '5m') AS race_5m,
    max(race) FILTER (WHERE horizon = '15m') AS race_15m,
    max(race) FILTER (WHERE horizon = '1h') AS race_1h,
    max(race) FILTER (WHERE horizon = '4h') AS race_4h,
    max(race) FILTER (WHERE horizon = '1d') AS race_1d
FROM outcomes
GROUP BY record
ORDER BY record;

-- The baseline stream: the standardization's reference (ADR-058 D4) and,
-- through auction_state and auction_side, the failure runs of P3's
-- episodes (D6).
CREATE OR REPLACE VIEW study_baseline AS
SELECT
    r.record, r.decision_ms, r.regime, r.location, r.auction_state,
    r.auction_side, o.race_5m, o.race_15m, o.race_1h, o.race_4h, o.race_1d
FROM study_records r
JOIN study_races o USING (record)
WHERE r.sample = 'baseline'
ORDER BY r.record;

-- One effort row per record and window with the comparisons the pairs
-- read. `covered`: ready, with neither partial_start nor feed_gap.
CREATE OR REPLACE VIEW study_effort AS
SELECT
    e.record,
    e."window" AS effort_window,
    list_position(['5m', '15m', '1h'], e."window") AS window_order,
    e.state = 'ready' AS ready,
    e.state = 'ready' AND NOT e.partial_start AND NOT e.feed_gap AS covered,
    e.aggressor,
    e.effort >= e.effort_high AS high_effort,
    e.progress <= e.move_median AS low_progress,
    e.progress >= e.move_high AS high_progress
FROM effort e
ORDER BY e.record, window_order;

-- The race at one horizon mapped to the pair's hypothesis: 'hypothesis' when
-- the hypothesis barrier was reached first, 'counter' for the other one,
-- otherwise the race itself (neither, censored, unavailable).
CREATE OR REPLACE MACRO study_map(race, hypothesis) AS
    CASE
        WHEN race = hypothesis THEN 'hypothesis'
        WHEN race IN ('upper', 'lower') THEN 'counter'
        ELSE race
    END;

-- The members of every population of ADR-058 D3, one row per population
-- and record. `side` is the aggressor (P1, P2, P3, P1-trig) or the failed
-- side (P4); `hypothesis` is the barrier the hypothesis expects first.
CREATE OR REPLACE VIEW study_members AS
WITH flow AS (
    SELECT r.record, r.auction_state, r.auction_side, e.effort_window,
           e.window_order, e.aggressor, e.high_effort, e.low_progress,
           e.high_progress
    FROM study_records r
    JOIN study_effort e USING (record)
    WHERE r.sample = 'baseline' AND e.covered
),
sfp_side AS (
    SELECT r.record, r.trigger,
           CASE
               WHEN pe.price > pl.price THEN 'above'
               WHEN pe.price < pl.price THEN 'below'
           END AS side
    FROM study_records r
    LEFT JOIN passages pl ON pl.record = r.record AND pl.target = 'level'
    LEFT JOIN passages pe ON pe.record = r.record AND pe.target = 'extreme'
    WHERE r.sample = 'trigger'
      AND r.trigger IN ('trigger.sfp.15m@1', 'trigger.sfp.1h@1',
                        'trigger.sfp.4h@1', 'trigger.sfp.1d@1')
),
members AS (
    -- P1-buy / P1-sell: high effort, progress at most the median move; the
    -- hypothesis is the barrier away from the aggressor.
    SELECT 'P1-' || aggressor AS pair, effort_window, window_order,
           NULL::VARCHAR AS trigger, record, aggressor AS side,
           CASE aggressor WHEN 'buy' THEN 'lower' ELSE 'upper' END AS hypothesis
    FROM flow
    WHERE high_effort AND low_progress
    UNION ALL
    -- P2: high effort, progress at least the high move; the hypothesis is
    -- the aggressor's barrier.
    SELECT 'P2', effort_window, window_order, NULL, record, aggressor,
           CASE aggressor WHEN 'buy' THEN 'upper' ELSE 'lower' END
    FROM flow
    WHERE high_effort AND high_progress
    UNION ALL
    -- P3: high effort in a held failure, the aggressor in the probe's
    -- direction: the failure's side for failed_breakout, the opposite side
    -- for failed_reclaim (ADR-044 decision 8). The hypothesis is the barrier
    -- against the probe.
    SELECT 'P3', effort_window, window_order, NULL, record, aggressor,
           CASE aggressor WHEN 'buy' THEN 'lower' ELSE 'upper' END
    FROM flow
    WHERE high_effort
      AND auction_state IN ('failed_breakout', 'failed_reclaim')
      AND auction_side IN ('above', 'below')
      AND aggressor = CASE
              WHEN (auction_state = 'failed_breakout') = (auction_side = 'above')
                  THEN 'buy'
              ELSE 'sell'
          END
    UNION ALL
    -- P4, failed auction: the failed side is the auction side; the
    -- hypothesis is the barrier away from it.
    SELECT 'P4', NULL, 0, trigger, record, auction_side,
           CASE auction_side WHEN 'above' THEN 'lower' ELSE 'upper' END
    FROM study_records
    WHERE sample = 'trigger' AND trigger = 'trigger.failed_auction@1'
      AND auction_side IN ('above', 'below')
    UNION ALL
    -- P4, SFP: the failed side is the side of the extreme from the swept
    -- level.
    SELECT 'P4', NULL, 0, trigger, record, side,
           CASE side WHEN 'above' THEN 'lower' ELSE 'upper' END
    FROM sfp_side
    WHERE side IS NOT NULL
    UNION ALL
    -- P1-trig: the effort trigger of window w, aggressor from the same
    -- window; hypothesis as P1.
    SELECT 'P1-trig', e.effort_window, e.window_order, r.trigger, r.record,
           e.aggressor,
           CASE e.aggressor WHEN 'buy' THEN 'lower' ELSE 'upper' END
    FROM study_records r
    JOIN study_effort e USING (record)
    WHERE r.sample = 'trigger'
      AND r.trigger = 'trigger.effort_result.' || e.effort_window || '@1'
      AND e.covered
)
SELECT
    m.pair, m.effort_window, m.trigger, m.record, r.decision_ms, m.side,
    m.hypothesis, r.regime, r.location, r.episode,
    CASE WHEN m.pair IN ('P1-buy', 'P1-sell', 'P2') THEN
        CASE
            WHEN r.cvd_utc_day IS NULL OR r.cvd_partial_start OR r.cvd_feed_gap
                THEN 'unknown'
            WHEN r.cvd_utc_day = 0 THEN 'flat'
            WHEN (r.cvd_utc_day > 0) = (m.side = 'buy') THEN 'with'
            ELSE 'against'
        END
    END AS cvd,
    study_map(o.race_5m, m.hypothesis) AS out_5m,
    study_map(o.race_15m, m.hypothesis) AS out_15m,
    study_map(o.race_1h, m.hypothesis) AS out_1h,
    study_map(o.race_4h, m.hypothesis) AS out_4h,
    study_map(o.race_1d, m.hypothesis) AS out_1d
FROM members m
JOIN study_records r USING (record)
JOIN study_races o USING (record)
ORDER BY m.pair, m.window_order, m.trigger NULLS FIRST, m.record;

-- What each population's universe loses before any race is read: records
-- whose effort value is not ready, or ready with incomplete coverage, and
-- P4 triggers without a failed side. `universe` is 'baseline' (P1, P2, P3,
-- per window), 'P1-trig' (per window) or 'P4' (per trigger id).
CREATE OR REPLACE VIEW study_exclusions AS
SELECT 'baseline' AS universe, e.effort_window, e.window_order,
       NULL::VARCHAR AS trigger, count(*) AS records,
       count(*) FILTER (WHERE NOT e.ready) AS not_ready,
       count(*) FILTER (WHERE e.ready AND NOT e.covered) AS incomplete,
       0::BIGINT AS no_side
FROM study_records r
JOIN study_effort e USING (record)
WHERE r.sample = 'baseline'
GROUP BY e.effort_window, e.window_order
UNION ALL
SELECT 'P1-trig', e.effort_window, e.window_order, r.trigger, count(*),
       count(*) FILTER (WHERE NOT e.ready),
       count(*) FILTER (WHERE e.ready AND NOT e.covered),
       0::BIGINT
FROM study_records r
JOIN study_effort e USING (record)
WHERE r.sample = 'trigger'
  AND r.trigger = 'trigger.effort_result.' || e.effort_window || '@1'
GROUP BY e.effort_window, e.window_order, r.trigger
UNION ALL
SELECT 'P4', NULL, 0, r.trigger, count(*), 0, 0,
       count(*) FILTER (WHERE m.record IS NULL)
FROM study_records r
LEFT JOIN (SELECT record FROM study_members WHERE pair = 'P4') m USING (record)
WHERE r.sample = 'trigger'
  AND r.trigger IN ('trigger.failed_auction@1', 'trigger.sfp.15m@1',
                    'trigger.sfp.1h@1', 'trigger.sfp.4h@1', 'trigger.sfp.1d@1')
GROUP BY r.trigger
ORDER BY universe, window_order, trigger NULLS FIRST;

-- The median barrier half-width per horizon over the baseline records whose
-- barriers exist, in basis points of the reference: (upper - reference) /
-- reference. Read from the barriers only, never from a race (ADR-058 D9:
-- the fee in R per horizon).
CREATE OR REPLACE VIEW study_barriers AS
WITH widths AS (
    SELECT
        o.horizon, o.horizon_ms,
        CAST(mie_units(o.upper) - mie_units(c.reference) AS DOUBLE) * 10000
            / CAST(mie_units(c.reference) AS DOUBLE) AS half_bps
    FROM study_records r
    JOIN contexts c USING (record)
    JOIN outcomes o USING (record)
    WHERE r.sample = 'baseline' AND o.upper IS NOT NULL
),
sorted AS (
    SELECT horizon, horizon_ms, list_sort(list(half_bps)) AS w
    FROM widths
    GROUP BY horizon, horizon_ms
)
SELECT horizon, horizon_ms, len(w) AS barriers,
       mie_rank(w, 0.5) AS median_half_width_bps
FROM sorted
ORDER BY horizon_ms;

-- P4's SFP cells: `extreme_vs_opposite` (ADR-057 D7) counted over each
-- cell's own members, per trigger id and horizon (ADR-058 D3).
CREATE OR REPLACE VIEW study_sfp_extreme AS
SELECT l.group_trigger AS trigger, l.horizon, l.horizon_ms,
       coalesce(l.extreme_vs_opposite, 'none') AS extreme_vs_opposite,
       count(*) AS records
FROM labeled l
JOIN (SELECT record FROM study_members
      WHERE pair = 'P4' AND trigger LIKE 'trigger.sfp.%') m USING (record)
GROUP BY l.group_trigger, l.horizon, l.horizon_ms, l.extreme_vs_opposite
ORDER BY trigger, horizon_ms, 4;
