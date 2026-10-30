# Research studies

Conditional studies over the conditional outcome dataset (ADR-057): the
committed queries of each study, and its report once it has run. A study is
pre-registered in an ADR before any outcome is read, and its report records
every result, null results included.

## Files

- `NNN-slug.sql`: the study's queries, named after its issue. They define
  views over the dataset's DuckDB views (`views.sql`, ADR-057 D7) and are
  run by the study's tool, which loads the file with `include_str!`, so the
  file in the repository is the file that ran.
- `NNN-slug.md`: the study's report, one per study, same name.

No data files: a report names its dataset by content hash, and the dataset
is rebuilt from versioned raw data (ADR-057 D3).

## What a report records

- **The dataset:** its manifest's `content`, `data`, `engine`, `sampling`
  and `labeling` lines, and its `code` line (the build that wrote it).
- **The queries:** the file's path and its SHA-256, as the tool printed it.
- **The run:** the exact tool command with every input, and the commit it
  ran from.
- **The pre-registration:** the ADR that fixed the study before the run;
  anything the run did differently is listed as a deviation.
- **Every result** the pre-registered output holds, null results and cells
  too small to read included. A result is never left out because of its
  value.

## Studies

| Study | Issue | Pre-registered by | Queries | Report |
|---|---|---|---|---|
| Order-flow interactions and counter-hypotheses | #28 | ADR-058 | [`028-order-flow-interactions.sql`](028-order-flow-interactions.sql) | [`028-order-flow-interactions.md`](028-order-flow-interactions.md) |
