//! The research views over one conditional outcome dataset (#27, ADR-004,
//! ADR-057 D7/D8, proposed).
//!
//! [`OutcomeViews::open`] opens an in-memory DuckDB, the bundled engine
//! `Cargo.lock` pins (ADR-057 D8), and creates the views of
//! [`VIEWS_SQL`] over the dataset directory's three Parquet files:
//!
//! - `contexts`, `outcomes`, `passages`: the tables as written (ADR-057 D1);
//! - `labeled`: one row per record and horizon, with the trigger group, the
//!   location bins, the excursions in barrier offsets, the time to the race
//!   and the two passage compositions of ADR-055 D8;
//! - `outcome_summary(n_min)`: the distributions per horizon and per every
//!   combination of trigger id, regime and location, plus a family branch.
//!
//! The adapter reads files by path and knows no internal crate: the caller
//! verifies the dataset first (`mie_adapter_parquet::outcome::
//! OutcomeDataset::open` in `mie-cli`), so the views never read a file the
//! manifest does not vouch for.
//!
//! **No network.** The bundled build compiles extension autoinstall and
//! autoload on, which would download an extension a query names; every
//! connection turns both off before its first statement, so such a query
//! fails instead. Only the Parquet reader is linked.
//!
//! **Deterministic output.** Every view aggregates order-independently and
//! [`OutcomeViews::query_tsv`] orders by the view's key columns, so the
//! TSV is the same at any thread count. Values print as: `NULL`; `true` /
//! `false`; integers; doubles in Rust's shortest round-trip form; decimals
//! exactly (scale 8); timestamps as epoch milliseconds; lists as
//! `[a,b]`; text with `\`, tab and newline escaped.

use duckdb::types::{TimeUnit, Value};
use duckdb::{Config, Connection};
use std::fmt;
use std::fmt::Write as _;
use std::io::Write;
use std::path::Path;

/// The view definitions, for this adapter and for any other DuckDB client
/// (`mie conditional views`). They read the dataset directory from the
/// DuckDB variable `mie_dataset`.
pub const VIEWS_SQL: &str = include_str!("views.sql");

/// The views [`OutcomeViews::query_tsv`] serves, each with the columns its
/// output is ordered by (the view's own `ORDER BY`).
pub const VIEWS: [(&str, &str); 5] = [
    ("contexts", "record"),
    ("outcomes", "record, horizon_ms"),
    ("passages", "record, target DESC"),
    ("labeled", "record, horizon_ms"),
    (
        "outcome_summary",
        "horizon_ms, split, trigger, family, regime, location",
    ),
];

/// The view that takes the minimum-sample threshold.
const SUMMARY: &str = "outcome_summary";

/// The dataset tables the views read.
const TABLES: [&str; 3] = ["contexts.parquet", "outcomes.parquet", "passages.parquet"];

/// Why the views could not be opened or queried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewsError {
    /// The dataset directory or one of its tables is missing.
    Missing(String),
    /// The dataset path cannot be handed to DuckDB: not UTF-8, or a glob
    /// character DuckDB's `read_parquet` would expand.
    Path(String),
    /// An unknown view, or a parameter the view does not take or lacks.
    Request(String),
    /// DuckDB failed.
    Engine(String),
    /// A value of a type the TSV output does not print.
    Unsupported(String),
    /// The output could not be written.
    Io(String),
}

impl fmt::Display for ViewsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing(detail) => write!(f, "outcome dataset missing: {detail}"),
            Self::Path(detail) => write!(f, "unusable dataset path: {detail}"),
            Self::Request(detail) => write!(f, "bad view request: {detail}"),
            Self::Engine(detail) => write!(f, "DuckDB failed: {detail}"),
            Self::Unsupported(detail) => write!(f, "unsupported value: {detail}"),
            Self::Io(detail) => write!(f, "write the view output: {detail}"),
        }
    }
}

impl std::error::Error for ViewsError {}

impl From<duckdb::Error> for ViewsError {
    fn from(error: duckdb::Error) -> Self {
        Self::Engine(error.to_string())
    }
}

/// The views over one dataset directory (module docs).
pub struct OutcomeViews {
    connection: Connection,
}

impl fmt::Debug for OutcomeViews {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutcomeViews").finish_non_exhaustive()
    }
}

impl OutcomeViews {
    /// Opens an in-memory DuckDB without extension autoinstall or autoload
    /// and creates the views over the tables in `dir`. The caller verifies
    /// the dataset first; this only checks that the three tables exist.
    ///
    /// # Errors
    ///
    /// [`ViewsError::Missing`] without the directory or a table,
    /// [`ViewsError::Path`] for a path DuckDB cannot take literally,
    /// [`ViewsError::Engine`] when DuckDB fails.
    pub fn open(dir: &Path) -> Result<Self, ViewsError> {
        if !dir.is_dir() {
            return Err(ViewsError::Missing(format!(
                "{} is not a directory",
                dir.display()
            )));
        }
        for table in TABLES {
            if !dir.join(table).is_file() {
                return Err(ViewsError::Missing(format!(
                    "{} has no {table}",
                    dir.display()
                )));
            }
        }
        let literal = sql_literal(dir)?;
        let config = Config::default()
            .enable_autoload_extension(false)?
            .with("autoinstall_known_extensions", "false")?;
        let connection = Connection::open_in_memory_with_flags(config)?;
        connection.execute_batch(&format!("SET VARIABLE mie_dataset = {literal};"))?;
        connection.execute_batch(VIEWS_SQL)?;
        Ok(Self { connection })
    }

    /// Sets the number of DuckDB worker threads.
    ///
    /// # Errors
    ///
    /// [`ViewsError::Engine`] when DuckDB refuses the setting.
    pub fn set_threads(&self, threads: u32) -> Result<(), ViewsError> {
        self.connection
            .execute_batch(&format!("SET threads = {threads};"))?;
        Ok(())
    }

    /// The SQL that created the views ([`VIEWS_SQL`]).
    #[must_use]
    pub fn view_sql(&self) -> &'static str {
        VIEWS_SQL
    }

    /// Writes `view` as TSV to `out`: a header line with the column names,
    /// then one line per row, ordered by the view's key columns. `n_min`
    /// is the minimum clear-sample threshold of `outcome_summary`, which
    /// requires it; every other view refuses it. Returns the row count.
    ///
    /// # Errors
    ///
    /// [`ViewsError::Request`] for an unknown view or a misplaced or
    /// missing `n_min`, [`ViewsError::Engine`] when DuckDB fails,
    /// [`ViewsError::Unsupported`] for a value TSV cannot print,
    /// [`ViewsError::Io`] when `out` fails.
    pub fn query_tsv(
        &self,
        view: &str,
        n_min: Option<u64>,
        out: &mut dyn Write,
    ) -> Result<u64, ViewsError> {
        let Some((_, order)) = VIEWS.iter().find(|(name, _)| *name == view) else {
            let names: Vec<&str> = VIEWS.iter().map(|(name, _)| *name).collect();
            return Err(ViewsError::Request(format!(
                "unknown view {view:?}; the views are {}",
                names.join(", ")
            )));
        };
        let source = match (view == SUMMARY, n_min) {
            (true, Some(n_min)) => format!("{SUMMARY}({n_min})"),
            (true, None) => {
                return Err(ViewsError::Request(format!(
                    "{SUMMARY} needs a minimum clear-sample count (no default, ADR-057 D7)"
                )));
            }
            (false, None) => view.to_owned(),
            (false, Some(_)) => {
                return Err(ViewsError::Request(format!(
                    "{view} takes no minimum sample count"
                )));
            }
        };
        self.write_tsv(&format!("SELECT * FROM {source} ORDER BY {order}"), out)
    }

    /// Runs `sql` and writes its result as TSV. Returns the row count.
    fn write_tsv(&self, sql: &str, out: &mut dyn Write) -> Result<u64, ViewsError> {
        let mut statement = self.connection.prepare(sql)?;
        let mut rows = statement.query([])?;
        let columns = rows
            .as_ref()
            .map(duckdb::Statement::column_names)
            .unwrap_or_default();
        let io = |error: std::io::Error| ViewsError::Io(error.to_string());
        let mut line = columns.join("\t");
        line.push('\n');
        out.write_all(line.as_bytes()).map_err(io)?;
        let mut count = 0u64;
        while let Some(row) = rows.next()? {
            line.clear();
            for index in 0..columns.len() {
                if index > 0 {
                    line.push('\t');
                }
                let value: Value = row.get(index)?;
                format_value(&value, &mut line)?;
            }
            line.push('\n');
            out.write_all(line.as_bytes()).map_err(io)?;
            count += 1;
        }
        out.flush().map_err(io)?;
        Ok(count)
    }

    /// Runs `sql` for its effect. Tests only: the views' own statements
    /// and the extension refusal.
    #[cfg(test)]
    fn execute(&self, sql: &str) -> Result<(), ViewsError> {
        Ok(self.connection.execute_batch(sql)?)
    }
}

/// `dir` as a DuckDB string literal: single quotes doubled, refused when
/// not UTF-8 or when it holds a character `read_parquet` would expand as a
/// glob (`*`, `?`, `[`), since then another file set could be read.
fn sql_literal(dir: &Path) -> Result<String, ViewsError> {
    let Some(text) = dir.to_str() else {
        return Err(ViewsError::Path(format!("{} is not UTF-8", dir.display())));
    };
    if let Some(glob) = text.chars().find(|c| matches!(c, '*' | '?' | '[')) {
        return Err(ViewsError::Path(format!(
            "{text} holds the glob character {glob:?}"
        )));
    }
    Ok(format!("'{}'", text.replace('\'', "''")))
}

/// Appends the TSV form of `value` (module docs) to `line`.
fn format_value(value: &Value, line: &mut String) -> Result<(), ViewsError> {
    // Writing to a String cannot fail.
    let _ = match value {
        Value::Null => write!(line, "NULL"),
        Value::Boolean(v) => write!(line, "{v}"),
        Value::TinyInt(v) => write!(line, "{v}"),
        Value::SmallInt(v) => write!(line, "{v}"),
        Value::Int(v) => write!(line, "{v}"),
        Value::BigInt(v) => write!(line, "{v}"),
        Value::HugeInt(v) => write!(line, "{v}"),
        Value::UTinyInt(v) => write!(line, "{v}"),
        Value::USmallInt(v) => write!(line, "{v}"),
        Value::UInt(v) => write!(line, "{v}"),
        Value::UBigInt(v) => write!(line, "{v}"),
        Value::UHugeInt(v) => write!(line, "{v}"),
        Value::Float(v) => write!(line, "{v}"),
        Value::Double(v) => write!(line, "{v}"),
        Value::Decimal(v) => write!(line, "{v}"),
        Value::Timestamp(unit, v) => write!(line, "{}", epoch_millis(*unit, *v)),
        Value::Text(text) | Value::Enum(text) => {
            for c in text.chars() {
                match c {
                    '\\' => line.push_str("\\\\"),
                    '\t' => line.push_str("\\t"),
                    '\n' => line.push_str("\\n"),
                    '\r' => line.push_str("\\r"),
                    c => line.push(c),
                }
            }
            Ok(())
        }
        Value::List(items) | Value::Array(items) => {
            line.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    line.push(',');
                }
                format_value(item, line)?;
            }
            line.push(']');
            Ok(())
        }
        other => {
            return Err(ViewsError::Unsupported(format!("{other:?}")));
        }
    };
    Ok(())
}

/// Epoch milliseconds of a timestamp in `unit`, rounded toward negative
/// infinity (the dataset's times are whole milliseconds).
fn epoch_millis(unit: TimeUnit, value: i64) -> i64 {
    match unit {
        TimeUnit::Second => value.saturating_mul(1_000),
        TimeUnit::Millisecond => value,
        TimeUnit::Microsecond => value.div_euclid(1_000),
        TimeUnit::Nanosecond => value.div_euclid(1_000_000),
    }
}

#[cfg(test)]
mod tests;
