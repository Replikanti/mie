-- The P1 replication of #205 on 2020-01-01 … 2024-09-30, pre-registered by
-- ADR-064.
--
-- Run it after views.sql and `028-order-flow-interactions.sql` over one
-- verified format-2 outcome dataset (ADR-057 D7). It only defines views.
-- The tool `crates/mie-cli/tests/order_flow_study/replication.rs` reads
-- them and does in Rust what this file does not: the strata, the
-- episodes and units (ADR-058 D4–D6), the per-label fee, T2, the T3
-- rotation null, Benjamini–Hochberg and the reading (ADR-064 D2–D4).
--
-- Two chains, kept apart on purpose (ADR-064 D5):
--
-- - **Coverage** (`rep_ran`, `rep_coverage_*`) restates the P1 condition of
--   ADR-058 D3 from `study_records` and `study_effort` and never selects
--   from `study_members` or `study_baseline`, which carry outcomes.
--   `rep_ran` is the only place it touches `race`, and only as whether the
--   race ran: no direction, and no `neither` against a barrier.
-- - **Measurement** (`rep_members`, `rep_baseline`) adds the mapped outcomes
--   and the races at 4h and 1d to the coverage rows. Only the measurement
--   entry, after its guard, reads it.
--
-- Every view ends with ORDER BY its key columns, and every aggregate is
-- order-independent (bool_or and max over one value per record), so the
-- rows do not depend on how DuckDB schedules threads.

-- Per record and replication horizon: the barrier and reference the
-- per-label fee reads (ADR-064 D3), and whether the race ran.
CREATE OR REPLACE VIEW rep_ran AS
SELECT
    o.record,
    o.horizon,
    o.upper,
    c.reference,
    o.race IN ('upper', 'lower', 'neither') AS ran
FROM outcomes o
JOIN contexts c USING (record)
WHERE o.horizon IN ('4h', '1d')
ORDER BY o.record, o.horizon_ms;

-- Per record: ran and the barrier half-width in bps of the reference,
-- (upper - reference) / reference, at 4h and 1d. NULL without barriers.
CREATE OR REPLACE VIEW rep_coverage_widths AS
SELECT
    record,
    bool_or(ran) FILTER (WHERE horizon = '4h') AS ran_4h,
    bool_or(ran) FILTER (WHERE horizon = '1d') AS ran_1d,
    max(CAST(mie_units(upper) - mie_units(reference) AS DOUBLE) * 10000
        / CAST(mie_units(reference) AS DOUBLE)) FILTER (WHERE horizon = '4h')
        AS half_bps_4h,
    max(CAST(mie_units(upper) - mie_units(reference) AS DOUBLE) * 10000
        / CAST(mie_units(reference) AS DOUBLE)) FILTER (WHERE horizon = '1d')
        AS half_bps_1d
FROM rep_ran
GROUP BY record
ORDER BY record;

-- The P1 rows (ADR-058 D3, ADR-051 D5 `active` on a covered window),
-- restated from study_records and study_effort: the same rows as
-- `study_members` WHERE pair IN ('P1-buy', 'P1-sell'), which the
-- measurement guard asserts.
CREATE OR REPLACE VIEW rep_coverage_members AS
SELECT
    'P1-' || e.aggressor AS pair,
    e.effort_window,
    r.record,
    r.decision_ms,
    r.minute_end_ms,
    r.regime,
    r.location,
    w.ran_4h,
    w.ran_1d,
    w.half_bps_4h,
    w.half_bps_1d
FROM study_records r
JOIN study_effort e USING (record)
JOIN rep_coverage_widths w USING (record)
WHERE r.sample = 'baseline' AND e.covered AND e.high_effort AND e.low_progress
ORDER BY pair, e.window_order, r.record;

-- The baseline stream as coverage reads it: the strata, the regime, the
-- minute key of the T3 rotation, and whether each race ran.
CREATE OR REPLACE VIEW rep_coverage_baseline AS
SELECT
    r.record,
    r.decision_ms,
    r.minute_end_ms,
    r.regime,
    r.location,
    w.ran_4h,
    w.ran_1d,
    w.half_bps_4h,
    w.half_bps_1d
FROM study_records r
JOIN rep_coverage_widths w USING (record)
WHERE r.sample = 'baseline'
ORDER BY r.record;

-- What the baseline universe loses per effort window before any race is
-- read: `study_exclusions`' baseline rows, restated so the coverage chain
-- does not read that view (its P4 rows join `study_members`).
CREATE OR REPLACE VIEW rep_coverage_exclusions AS
SELECT
    e.effort_window,
    e.window_order,
    count(*) AS records,
    count(*) FILTER (WHERE NOT e.ready) AS not_ready,
    count(*) FILTER (WHERE e.ready AND NOT e.covered) AS incomplete
FROM study_records r
JOIN study_effort e USING (record)
WHERE r.sample = 'baseline'
GROUP BY e.effort_window, e.window_order
ORDER BY e.window_order;

-- Measurement only: the coverage rows with the aggressor, the hypothesis
-- barrier and the mapped outcomes of `study_members` at 4h and 1d.
CREATE OR REPLACE VIEW rep_members AS
SELECT
    c.*,
    m.side,
    m.hypothesis,
    m.out_4h,
    m.out_1d
FROM rep_coverage_members c
JOIN study_members m
    ON m.pair = c.pair AND m.effort_window = c.effort_window
   AND m.record = c.record
ORDER BY c.pair, list_position(['5m', '15m', '1h'], c.effort_window), c.record;

-- Measurement only: the baseline rows with their races at 4h and 1d.
CREATE OR REPLACE VIEW rep_baseline AS
SELECT
    c.*,
    b.race_4h,
    b.race_1d
FROM rep_coverage_baseline c
JOIN study_baseline b USING (record)
ORDER BY c.record;
