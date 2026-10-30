//! Adapter tests: the dataset path, missing tables, the request checks,
//! the TSV form and the extension refusal. The view semantics are tested
//! against an independent model over real datasets in
//! `crates/mie-cli/tests/outcome_views.rs`.

use super::*;
use std::fs;
use std::path::PathBuf;

/// A fresh directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("mie-adapter-duckdb-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// One record with every column of the ADR-057 D1 and ADR-058 D2 schemas,
/// typed as the writer types them: a baseline at a value edge whose 5m race
/// went up, with its three effort rows written out of window order.
const FIXTURE: &str = "
CREATE TEMP TABLE prov AS SELECT
    'd' AS data_version, 'e' AS engine, 's' AS sampling, 'sf' AS sampling_fingerprint,
    'l' AS labeling, 'lf' AS labeling_fingerprint;
COPY (SELECT
    0::UBIGINT AS record, epoch_ms(60000)::TIMESTAMPTZ AS decision_time,
    'trade' AS event_kind, 7::UBIGINT AS event_seq, 0::UINTEGER AS ordinal,
    'baseline' AS sample, epoch_ms(59999)::TIMESTAMPTZ AS minute_end,
    NULL::VARCHAR AS trigger, NULL::VARCHAR AS trigger_family,
    NULL::VARCHAR AS episode, NULL::BOOLEAN AS opener, NULL::BOOLEAN AS truncated,
    NULL::BOOLEAN AS bridge, 'low' AS regime, 12.5::DOUBLE AS regime_percentile,
    true AS levels_ready, true AS at_value_edge, false AS at_structural_extreme,
    NULL::VARCHAR AS auction_state, 100::DECIMAL(18, 8) AS anchor,
    100::DECIMAL(18, 8) AS reference, 4::DECIMAL(18, 8) AS atr_1h,
    'above' AS auction_side, -2.5::DECIMAL(18, 8) AS cvd_utc_day,
    false AS cvd_partial_start, false AS cvd_feed_gap, prov.*
    FROM prov) TO '{dir}/contexts.parquet' (FORMAT parquet);
COPY (SELECT
    0::UBIGINT AS record, w.name AS \"window\", w.state,
    CASE WHEN w.state = 'ready' THEN 'buy' END AS aggressor,
    CASE WHEN w.state = 'ready' THEN 3::DECIMAL(18, 8) END AS effort,
    CASE WHEN w.state = 'ready' THEN 2::DECIMAL(18, 8) END AS effort_high,
    CASE WHEN w.state = 'ready' THEN -1::DECIMAL(18, 8) END AS progress,
    CASE WHEN w.state = 'ready' THEN 0.5::DECIMAL(18, 8) END AS move_median,
    CASE WHEN w.state = 'ready' THEN 4::DECIMAL(18, 8) END AS move_high,
    CASE WHEN w.state = 'ready' THEN false END AS partial_start,
    CASE WHEN w.state = 'ready' THEN false END AS feed_gap,
    CASE WHEN w.state = 'ready' THEN true END AS active, prov.*
    FROM (VALUES ('1h', 'warming'), ('5m', 'ready'), ('15m', 'unavailable'))
        w(name, state), prov)
    TO '{dir}/effort.parquet' (FORMAT parquet);
COPY (SELECT
    0::UBIGINT AS record, h.horizon, h.horizon_ms, true AS clear,
    98::DECIMAL(18, 8) AS lower, 102::DECIMAL(18, 8) AS upper,
    'upper' AS race, epoch_ms(60000 + 1000)::TIMESTAMPTZ AS race_at,
    false AS same_ms, NULL::TIMESTAMPTZ AS race_censor_at,
    NULL::VARCHAR AS race_censor_reason, 3::DECIMAL(18, 8) AS up,
    epoch_ms(60000 + 2000)::TIMESTAMPTZ AS up_at, 1::DECIMAL(18, 8) AS down,
    epoch_ms(60000 + 500)::TIMESTAMPTZ AS down_at, 101::DECIMAL(18, 8) AS last,
    NULL::TIMESTAMPTZ AS censor_at, NULL::VARCHAR AS censor_reason, prov.*
    FROM (VALUES ('5m', 300000::BIGINT), ('15m', 900000), ('1h', 3600000),
                 ('4h', 14400000), ('1d', 86400000)) h(horizon, horizon_ms), prov)
    TO '{dir}/outcomes.parquet' (FORMAT parquet);
COPY (SELECT
    0::UBIGINT AS record, 'level' AS target, 1::DECIMAL(18, 8) AS price,
    'touched' AS passage, NULL::TIMESTAMPTZ AS at, NULL::TIMESTAMPTZ AS censor_at,
    NULL::VARCHAR AS censor_reason, prov.*
    FROM prov WHERE false) TO '{dir}/passages.parquet' (FORMAT parquet);
";

/// Writes the fixture tables into `dir` with a separate DuckDB.
fn write_fixture(dir: &Path) {
    let connection = Connection::open_in_memory().unwrap();
    let dir = dir.to_str().unwrap().replace('\'', "''");
    connection
        .execute_batch(&FIXTURE.replace("{dir}", &dir))
        .unwrap();
}

fn tsv(views: &OutcomeViews, view: &str, n_min: Option<u64>) -> String {
    let mut out = Vec::new();
    views.query_tsv(view, n_min, &mut out).unwrap();
    String::from_utf8(out).unwrap()
}

#[test]
fn literal_doubles_quotes_and_refuses_globs() {
    assert_eq!(sql_literal(Path::new("/a/b")).unwrap(), "'/a/b'");
    // DuckDB rewrites these two prefixes; only absolute paths pass.
    for rewritten in ["~/ds", "~ds", "file:/tmp/ds", "file:///tmp/ds", "ds"] {
        assert!(
            matches!(sql_literal(Path::new(rewritten)), Err(ViewsError::Path(_))),
            "{rewritten}"
        );
    }
    assert_eq!(sql_literal(Path::new("/a/it's")).unwrap(), "'/a/it''s'");
    for glob in ["/a/*", "/a/b?", "/a/[x]"] {
        assert!(matches!(
            sql_literal(Path::new(glob)),
            Err(ViewsError::Path(_))
        ));
    }
}

#[test]
fn a_quote_in_the_path_reads_the_dataset() {
    let temp = TempDir::new("quote");
    let dir = temp.0.join("it's a 'dataset'");
    fs::create_dir(&dir).unwrap();
    write_fixture(&dir);
    let views = OutcomeViews::open(&dir).unwrap();
    let contexts = tsv(&views, "contexts", None);
    assert_eq!(contexts.lines().count(), 2, "{contexts}");
    assert!(contexts.starts_with("record\tdecision_time\tevent_kind\t"));
}

#[test]
fn duckdb_gets_the_canonical_path() {
    let temp = TempDir::new("canonical");
    let dir = temp.0.join("ds");
    fs::create_dir(&dir).unwrap();
    fs::create_dir(temp.0.join("other")).unwrap();
    write_fixture(&dir);
    // A `..` component resolves to the verified directory.
    let views = OutcomeViews::open(&temp.0.join("other").join("..").join("ds")).unwrap();
    let mut out = Vec::new();
    views
        .write_tsv("SELECT getvariable('mie_dataset') AS dataset", &mut out)
        .unwrap();
    let canonical = dir.canonicalize().unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        format!("dataset\n{}\n", canonical.display())
    );
}

#[test]
fn a_missing_dataset_or_table_is_refused() {
    let temp = TempDir::new("missing");
    assert!(matches!(
        OutcomeViews::open(&temp.0.join("absent")),
        Err(ViewsError::Missing(_))
    ));
    write_fixture(&temp.0);
    fs::remove_file(temp.0.join("passages.parquet")).unwrap();
    let error = OutcomeViews::open(&temp.0).unwrap_err();
    assert!(
        matches!(&error, ViewsError::Missing(detail) if detail.contains("passages.parquet")),
        "{error}"
    );
}

#[test]
fn requests_are_checked() {
    let temp = TempDir::new("requests");
    write_fixture(&temp.0);
    let views = OutcomeViews::open(&temp.0).unwrap();
    let mut out = Vec::new();
    for (view, n_min) in [
        ("nope", None),
        ("contexts; DROP VIEW labeled", None),
        ("outcome_summary", None),
        ("labeled", Some(3)),
    ] {
        assert!(
            matches!(
                views.query_tsv(view, n_min, &mut out),
                Err(ViewsError::Request(_))
            ),
            "{view}"
        );
    }
    assert!(out.is_empty());
}

#[test]
fn values_print_in_the_documented_form() {
    let temp = TempDir::new("values");
    write_fixture(&temp.0);
    let views = OutcomeViews::open(&temp.0).unwrap();
    let mut out = Vec::new();
    views
        .write_tsv(
            "SELECT NULL::INTEGER AS n, true AS b, -3::BIGINT AS i, 0.1::DOUBLE AS d, \
             12.5::DECIMAL(18, 8) AS p, epoch_ms(1234)::TIMESTAMPTZ AS t, \
             ['a', 'b'] AS l, E'x\\ty' AS s",
            &mut out,
        )
        .unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "n\tb\ti\td\tp\tt\tl\ts\nNULL\ttrue\t-3\t0.1\t12.50000000\t1234\t[a,b]\tx\\ty\n"
    );
    let labeled = tsv(&views, "labeled", None);
    let header: Vec<&str> = labeled.lines().next().unwrap().split('\t').collect();
    let first: Vec<&str> = labeled.lines().nth(1).unwrap().split('\t').collect();
    let cell = |name: &str| first[header.iter().position(|h| *h == name).unwrap()];
    assert_eq!(cell("location_bins"), "[value_edge]");
    assert_eq!(cell("offset"), "2.00000000");
    assert_eq!(cell("up_x"), "1.5");
    assert_eq!(cell("down_x"), "0.5");
    assert_eq!(cell("time_to_event_ms"), "1000");
    assert_eq!(cell("level_within"), "NULL");
}

#[test]
fn a_query_needing_an_unlinked_extension_fails_instead_of_downloading() {
    let temp = TempDir::new("extension");
    write_fixture(&temp.0);
    let views = OutcomeViews::open(&temp.0).unwrap();
    // `json` is a known extension the bundled build does not link: with
    // autoinstall and autoload on, DuckDB would download and load it here.
    let error = views.create_views("SELECT '{\"a\": 1}'::JSON").unwrap_err();
    // The catalog error names the extension; an attempted autoload would
    // fail differently (an install or HTTP error), or succeed online.
    assert!(
        matches!(&error, ViewsError::Engine(detail)
            if detail.contains("not in the catalog") && detail.contains("json extension")),
        "{error}"
    );
}

#[test]
fn the_summary_takes_its_threshold() {
    let temp = TempDir::new("summary");
    write_fixture(&temp.0);
    let views = OutcomeViews::open(&temp.0).unwrap();
    let summary = tsv(&views, "outcome_summary", Some(1));
    // Per horizon: 4 plain sets and 4 located sets over one baseline
    // record in one bin, no family row (no trigger).
    assert_eq!(summary.lines().count(), 1 + 5 * 8, "{summary}");
    assert!(
        summary
            .lines()
            .nth(1)
            .unwrap()
            .starts_with("5m\t300000\tall\tall\t")
    );
}

#[test]
fn the_effort_view_orders_by_record_and_window() {
    let temp = TempDir::new("effort");
    write_fixture(&temp.0);
    let views = OutcomeViews::open(&temp.0).unwrap();
    let effort = tsv(&views, "effort", None);
    let lines: Vec<&str> = effort.lines().collect();
    assert_eq!(
        lines[0],
        "record\twindow\tstate\taggressor\teffort\teffort_high\tprogress\t\
         move_median\tmove_high\tpartial_start\tfeed_gap\tactive\tdata_version\t\
         engine\tsampling\tsampling_fingerprint\tlabeling\tlabeling_fingerprint"
    );
    let windows: Vec<&str> = lines[1..]
        .iter()
        .map(|line| line.split('\t').nth(1).unwrap())
        .collect();
    assert_eq!(windows, ["5m", "15m", "1h"]);
    assert_eq!(
        lines[1],
        "0\t5m\tready\tbuy\t3.00000000\t2.00000000\t-1.00000000\t0.50000000\t\
         4.00000000\tfalse\tfalse\ttrue\td\te\ts\tsf\tl\tlf"
    );
    assert_eq!(
        lines[2],
        "0\t15m\tunavailable\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\t\
         d\te\ts\tsf\tl\tlf"
    );
    // `labeled` carries the format-2 context columns through `c.*`.
    let labeled = tsv(&views, "labeled", None);
    let header: Vec<&str> = labeled.lines().next().unwrap().split('\t').collect();
    let at = |name: &str| header.iter().position(|h| *h == name).unwrap();
    assert_eq!(at("auction_side"), at("atr_1h") + 1);
    assert_eq!(at("cvd_feed_gap"), at("atr_1h") + 4);
    assert_eq!(at("horizon"), at("cvd_feed_gap") + 1);
}

#[test]
fn a_study_defines_views_and_selects_them() {
    let temp = TempDir::new("study");
    write_fixture(&temp.0);
    let views = OutcomeViews::open(&temp.0).unwrap();
    views
        .create_views(
            "CREATE OR REPLACE VIEW ready_windows AS \
             SELECT record, \"window\" FROM effort WHERE state = 'ready' \
             ORDER BY record, \"window\";",
        )
        .unwrap();
    let mut out = Vec::new();
    let rows = views
        .select_tsv("SELECT * FROM ready_windows ORDER BY record", &mut out)
        .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(String::from_utf8(out).unwrap(), "record\twindow\n0\t5m\n");
    assert!(matches!(
        views.create_views("CREATE VIEW broken AS SELECT nope FROM effort"),
        Err(ViewsError::Engine(_))
    ));
}
