#!/usr/bin/env bash
# Enforces the hexagonal boundary (ADR-018) as crate-graph rules (ADR-025):
#
#   mie-domain     -> nothing
#   mie-ports      -> mie-domain
#   mie-app        -> mie-domain, mie-ports
#   mie-adapter-*  -> mie-domain, mie-ports, external crates
#   mie-cli        -> anything (composition root); nothing depends on it
#
# plus core purity: the core crates (domain, ports, app) take no external
# dependencies and never touch the wall clock, the filesystem, the network,
# the environment, threads, processes, async or hash-ordered collections.
# The purity scan is a pattern guard over source text, not a proof — review
# still applies. Dev-dependencies are exempt from the graph rules. A third
# text guard keeps forward outcome labels out of every core file but the
# labeler and its replay driver (ADR-055 D11), a fourth keeps the backtest
# out of every core file but its allow-list (ADR-060 D15), and a fifth bans
# `macro_rules!` in the core, so that neither can be named without the
# text showing.
set -euo pipefail
cd "$(dirname "$0")/.."

status=0
fail() {
  echo "FAIL: $*"
  status=1
}

meta=$(cargo metadata --format-version 1 --no-deps)
members=$(jq -r '.packages[].name' <<<"$meta")

for core in mie-domain mie-ports mie-app; do
  grep -qx "$core" <<<"$members" || fail "core crate $core is missing from the workspace"
done

# Normal and build dependencies of a workspace member, by package name.
deps_of() {
  jq -r --arg name "$1" \
    '.packages[] | select(.name == $name) | .dependencies[] | select(.kind != "dev") | .name' \
    <<<"$meta"
}

for name in $members; do
  case "$name" in
    mie-domain) allowed="" external=no ;;
    mie-ports) allowed="mie-domain" external=no ;;
    mie-app) allowed="mie-domain mie-ports" external=no ;;
    mie-adapter-*) allowed="mie-domain mie-ports" external=yes ;;
    mie-cli) allowed="*" external=yes ;;
    *)
      fail "$name: unknown crate role — name it mie-domain|mie-ports|mie-app|mie-adapter-<tech>|mie-cli or amend ADR-025"
      continue
      ;;
  esac
  for dep in $(deps_of "$name"); do
    if [[ "$dep" == mie-cli ]]; then
      fail "$name -> mie-cli: the composition root must stay a leaf"
    elif [[ "$dep" == mie-* ]]; then
      [[ "$allowed" == "*" || " $allowed " == *" $dep "* ]] \
        || fail "$name -> $dep breaks the crate graph (allowed: ${allowed:-none})"
    elif [[ "$external" == no ]]; then
      fail "$name -> $dep: core crates take no external dependencies"
    fi
  done
done

# Module paths are matched both as `std::fs` and as a bare `fs::` usage, so
# grouped imports (`use std::{fs, env};`, also across lines) are caught where
# the module is used.
io='(fs|net|env|thread|process)'
forbidden="SystemTime::now|Instant::now|UNIX_EPOCH|\.elapsed\(\)|std::${io}\b|std::\{[^}]*\b${io}\b|\b${io}::|\b(HashMap|HashSet|RandomState)\b|\basync\b|\.await\b"
hits=$(grep -rnE "$forbidden" crates/mie-domain/src crates/mie-ports/src crates/mie-app/src \
  | grep -vE '^[^:]+:[0-9]+:[[:space:]]*//' || true)
if [[ -n "$hits" ]]; then
  fail "core purity violations (ADR-025):"
  echo "$hits"
fi

# Label isolation (ADR-055 D11): forward outcome labels read the trades
# after the decision time, so no core file may name them except the labeler
# itself and its replay driver. Features are computed only in mie-domain
# (ADR-019, ADR-025), so this closes the path from a label into a feature.
# `mie_domain::conditional` declares `pub mod label;` without a re-export,
# so every use names `label::` or imports the module itself, possibly
# renamed (`label as x`), grouped across lines or through `super`/`self`.
# Two scans: every line for `label::` and `ForwardLabel` (comments count
# too: the guard is about naming, not only about code), and every `use`
# item, joined onto one line, for a `label` path segment.
label_lines='label[[:space:]]*::|ForwardLabel'
label_use='(^|[^[:alnum:]_])label($|[^[:alnum:]_])'

# Every item of the given files that starts on a line matching the ERE
# `$1`, joined onto one line up to the `;` that ends it, the first one
# outside brackets (`[u8; 4]` holds one): `<file>:<line>:<item>`, with
# line comments dropped.
joined_items() {
  local start=$1
  shift
  awk -v start="$start" '
    FNR == 1 { item = "" }
    item == "" && $0 ~ start { first = FNR; item = " "; depth = 0 }
    item != "" {
      line = $0
      sub(/\/\/.*/, "", line)
      item = item " " line
      for (i = 1; i <= length(line); i++) {
        c = substr(line, i, 1)
        if (c ~ /[([{]/) {
          depth++
        } else if (c ~ /[])}]/) {
          depth--
        } else if (c == ";" && depth <= 0) {
          print FILENAME ":" first ":" item
          item = ""
          break
        }
      }
    }
  ' "$@"
}

# Every `use` item of the given files, joined by `joined_items`.
use_items() {
  joined_items '^[[:space:]]*(pub([(][^)]*[)])?[[:space:]]+)?use[[:space:]]' "$@"
}

# The hits of an isolation scan in the given directories or files: every
# line matching the ERE `$1`, and every `use` item with a path segment
# matching the ERE `$2`.
isolation_scan() {
  local lines=$1 use=$2 files
  shift 2
  mapfile -d '' files < <(find "$@" -name '*.rs' -print0 | sort -z)
  {
    grep -rnHE "$lines" "$@" || true
    if [[ ${#files[@]} -gt 0 ]]; then
      use_items "${files[@]}" | grep -E "^[^:]+:[0-9]+:.*$use" || true
    fi
  } | sort -u
}

# The label-isolation hits in the given directories or files.
label_scan() {
  isolation_scan "$label_lines" "$label_use" "$@"
}

# Self-test of an isolation scan: each probe of the array named `$3` must
# hit, each clean snippet of the array named `$4` must not. A guard that
# silently stops matching would pass every tree, so its blind spots are
# pinned here (PR #148 review).
probe_dir=$(mktemp -d)
trap 'rm -rf "$probe_dir"' EXIT
self_test() {
  local guard=$1 scan=$2 index
  local -n hit_probes=$3 clean_snippets=$4
  for index in "${!hit_probes[@]}"; do
    printf '%s\n' "${hit_probes[$index]}" >"$probe_dir/${guard}_probe_$index.rs"
    [[ -n "$("$scan" "$probe_dir/${guard}_probe_$index.rs")" ]] \
      || fail "$guard guard self-test: probe $index is not caught: ${hit_probes[$index]}"
  done
  for index in "${!clean_snippets[@]}"; do
    printf '%s\n' "${clean_snippets[$index]}" >"$probe_dir/${guard}_clean_$index.rs"
    [[ -z "$("$scan" "$probe_dir/${guard}_clean_$index.rs")" ]] \
      || fail "$guard guard self-test: clean snippet $index is flagged: ${clean_snippets[$index]}"
  done
}

# shellcheck disable=SC2034 # read by self_test through a nameref
probes=(
  'use crate::conditional::label::ForwardLabel;'
  'use crate::conditional::label as outcome;'
  'use crate::conditional::{label, ContextRecord};'
  $'use crate::conditional::{\n    ContextRecord as _A, ContextSampler as _B, label as outcome,\n};'
  $'use mie_domain::conditional::{\n    ContextRecord,\n    label,\n};'
  $'use crate::{\n    conditional::{label as x},\n};'
  'use super::label as x;'
  'use self::label as x;'
  $'pub(crate) use super::{\n    label as x, // the labeler\n};'
  'fn f(race: outcome::Race) -> bool { matches!(race, label::Race::Neither) }'
  'fn f(m: u64) -> bool { matches!(Some(m), Some(m) if m >= crate::conditional::label ::SCALE_MULTIPLE_E5) }'
)
# shellcheck disable=SC2034 # read by self_test through a nameref
clean=(
  'use crate::conditional::{ContextRecord, ContextSampler};'
  $'use crate::regime::{\n    RegimeLabel,\n};'
  'let label = regime.label(); // a regime label'
  'pub fn label(self) -> RegimeLabel {'
)
self_test label label_scan probes clean

# Macro ban (ADR-055 D11, ADR-060 D15): a `macro_rules!` can paste its
# arguments into a path, `crate::$x::Race` with `m!(label)` or the
# repetition `$($seg)::*` with `m!(crate, conditional, label, Race)`, and
# name a module while no line shows `label::` and no `use` item has the
# segment (PR #176 review and re-review). A ban on splice shapes loses to
# the next shape, so no core file defines a macro at all, the allow-list
# and the labeler included: a macro exported from an allowed file would
# reopen the hole for its callers. The core has none, and core crates take
# no dependencies, so the only macros left are the standard library's,
# which build no path from their arguments. A module is then named only by
# a literal token, which the isolation scans see. The scan flags the
# identifier on any line, comments included like the other scans, so
# spacing (`macro_rules !`) and a raw identifier do not hide it. File
# inclusion is banned with it for the same reason (PR #176 third review):
# `include!` and a `#[path = ...]` module splice in a file the scans,
# which read the `.rs` files under `src`, never see.
macro_lines='(^|[^[:alnum:]_])macro_rules($|[^[:alnum:]_])|(^|[^[:alnum:]_])include[[:space:]]*!|#[[:space:]]*!?\[[[:space:]]*path[[:space:]]*='

# The macro definitions in the given directories or files.
macro_scan() {
  grep -rnHE "$macro_lines" "$@" || true
}

# shellcheck disable=SC2034,SC2016 # read by self_test through a nameref; literal Rust `$`
macro_probes=(
  $'macro_rules! m { ($x:ident) => { crate::$x::Simulator }; }\nfn f(_: Option<m!(backtest)>) {}'
  $'macro_rules! review_path {\n    ($($seg:ident),*) => { $($seg)::* };\n}'
  'macro_rules ! m { () => {}; }'
  $'#[macro_export]\nmacro_rules! m { ($p:path) => { use $p as outcome; }; }'
  'include!("regime/review_window.in");'
  $'#[path = "../tests/support/x.rs"]\nmod x;'
)
# shellcheck disable=SC2034 # read by self_test through a nameref
macro_clean=(
  'fn macro_rules_seen() -> bool { matches!(KIND, Kind::Macro) }'
  'let rules = macro_count; // a macro argument'
  'assert_eq!(rules, 0);'
  'let include_str_seen = included; // include it'
)
self_test macro macro_scan macro_probes macro_clean

macro_hits=$(macro_scan crates/mie-domain/src crates/mie-ports/src crates/mie-app/src)
if [[ -n "$macro_hits" ]]; then
  fail "macro definitions in a core crate (ADR-055 D11, ADR-060 D15):"
  echo "$macro_hits"
fi

label_hits=$(label_scan crates/mie-domain/src crates/mie-ports/src crates/mie-app/src \
  | grep -vE '^crates/mie-domain/src/conditional/label[^:]*:|^crates/mie-app/src/conditional\.rs:' || true)
if [[ -n "$label_hits" ]]; then
  fail "forward labels named outside the labeler and its replay driver (ADR-055 D11):"
  echo "$label_hits"
fi

# Backtest isolation (ADR-060 D15): the simulator reads the trades after
# the decision, the path the label guard closes, so no core file may name
# the backtest except the module itself, the outcome kind, the port, the
# `Unresolved` error and the app's engine and research service. The same
# two scans as for labels: every line for `backtest::`, and every `use`
# item for a `backtest` path segment. Unlike `label`, `backtest` is no
# common word in the code, so a third scan flags the bare identifier on
# any code line, with comments, string and char literals dropped and the
# `mod backtest;` declaration exempt. It came for macro arguments
# (`m!(backtest)`, PR #176 review), which the macro ban above now rules
# out, and stays as a backstop. A space before `::` (`crate::backtest
# ::X`), which rustfmt removes except in a macro call it cannot parse, is
# matched by the line scan itself, for labels too (PR #176 third review).
# The identifier scan's literal stripping is line-based and knows no raw
# strings: `r"\"` reads as an escaped quote, and a string that spans lines
# pairs its quotes wrong, so a token later on such a line can be hidden
# from it (PR #176 re-review). The other two scans strip no literals and
# do not share that limit. The identifier scan also flags the items that
# carry the simulator's records without the module's name: the port types
# `BacktestRun` and `BacktestEngine` and the outcome variant `Backtest`
# (`Outcome::Backtest`, or the bare variant after a glob import), PR #179
# review. Case-sensitive and whole-word, so the pipeline's service and key
# (`BacktestService`, `BACKTEST_V1`), which hand out no record, stay free
# for `mie-app`'s re-exports. The allow-list names
# exact files, so a neighbour (`backtester.rs`, `research.rs` in the
# domain) stays scanned.
backtest_lines='backtest[[:space:]]*::'
backtest_use='(^|[^[:alnum:]_])backtest($|[^[:alnum:]_])'
backtest_identifiers='(backtest|Backtest|BacktestRun|BacktestEngine)'
backtest_items='(^|[^[:alnum:]_])(backtest|Backtest|BacktestRun|BacktestEngine)($|[^[:alnum:]_])'
backtest_allow='^crates/mie-domain/src/backtest\.rs:|^crates/mie-domain/src/backtest/|^crates/mie-domain/src/research/result\.rs:|^crates/mie-ports/src/(outbound|inbound)\.rs:|^crates/mie-app/src/(backtest|research)\.rs:'

# The code lines of the given files that hold the identifier `$1`:
# `<file>:<line>:<text>`.
identifier_scan() {
  local word=$1 files
  shift
  mapfile -d '' files < <(find "$@" -name '*.rs' -print0 | sort -z)
  [[ ${#files[@]} -gt 0 ]] || return 0
  awk -v word="$word" -v quote="'" '
    {
      line = $0
      gsub(quote "([\\\\].|[^\\\\" quote "])" quote, "", line)
      gsub(/"([\\].|[^"\\])*"/, "\"\"", line)
      sub(/\/\/.*/, "", line)
    }
    line ~ ("^[[:space:]]*(pub(\\([^)]*\\))?[[:space:]]+)?mod[[:space:]]+" word "[[:space:]]*;") { next }
    line ~ ("(^|[^[:alnum:]_])" word "($|[^[:alnum:]_])") { print FILENAME ":" FNR ":" $0 }
  ' "${files[@]}"
}

# The backtest-isolation hits in the given directories or files.
backtest_scan() {
  {
    isolation_scan "$backtest_lines" "$backtest_use" "$@"
    identifier_scan "$backtest_identifiers" "$@"
  } | sort -u
}

# The hits on stdin, `<path>:<line>:<text>`, outside the allow-list.
backtest_disallowed() {
  grep -vE "$backtest_allow" || true
}

# shellcheck disable=SC2034 # read by self_test through a nameref
backtest_probes=(
  'use crate::backtest::simulator::Simulator;'
  'use crate::backtest as bt;'
  'use crate::{backtest, research::ExperimentSpec};'
  $'use crate::research::{\n    ExperimentSpec as _A, ResultKey as _B, backtest as bt,\n};'
  $'use mie_domain::{\n    research,\n    backtest,\n};'
  $'use crate::{\n    backtest::{Simulator as x},\n};'
  'use super::backtest as x;'
  'use self::backtest as x;'
  $'pub(crate) use super::{\n    backtest as x, // the simulator\n};'
  'fn f(record: bt::TradeRecord) -> bool { matches!(record.exit, backtest::Exit::Stop) }'
  'pub use backtest::Simulator;'
  'fn f() -> bool { matches!(X, x if x == crate::backtest ::Exit::Stop) }'
  '/// Renders a crate::backtest::TradeRecord as a trade line.'
  'fn f(_: Option<m!(backtest)>) {}'
  $'fn f() -> Option<\n    m![crate::backtest]\n> { None }'
  $'let quote = \'"\'; m!(backtest); let s = "x";'
  'm!(backtest) // a "string" after the call'
  'pub fn p(run: &mie_ports::outbound::BacktestRun) -> usize { run.records.len() }'
  'fn f<B: mie_ports::outbound::BacktestEngine>(_: B) {}'
  'fn f(o: &Outcome) -> bool { matches!(o, Outcome::Backtest { .. }) }'
  $'use mie_domain::research::Outcome::{\n    self, Backtest,\n};'
  'fn f(o: &Outcome) -> bool { matches!(o, Backtest { .. }) }'
)
# shellcheck disable=SC2034 # read by self_test through a nameref
backtest_clean=(
  'pub mod backtest;'
  'const PIPELINE: &str = "pipeline research.backtest@1";'
  '// The backtest (#30) adds its own pipeline.'
  'use crate::research::{backtest_kind};'
  'pub(crate) mod backtest;'
  $'let text = "a \\"backtest\\" line"; // the backtest'
  'let kind = Kind::Backtester; let run = backtests[0];'
  'pub use research::{BACKTEST_V1, BacktestService, ChainBounds};'
  '// A BacktestRun is the port output; see Outcome::Backtest.'
  'let run = BacktestRunner::new(); let kind = Kind::BacktestEngines;'
)
self_test backtest backtest_scan backtest_probes backtest_clean

# Self-test of the allow-list: every allowed path is dropped, and every
# lookalike or neighbour is kept.
backtest_allowed_paths=(
  crates/mie-domain/src/backtest.rs
  crates/mie-domain/src/backtest/simulator.rs
  crates/mie-domain/src/backtest/simulator/tests.rs
  crates/mie-domain/src/research/result.rs
  crates/mie-ports/src/outbound.rs
  crates/mie-ports/src/inbound.rs
  crates/mie-app/src/backtest.rs
  crates/mie-app/src/research.rs
)
backtest_kept_paths=(
  crates/mie-domain/src/backtester.rs
  crates/mie-domain/src/research.rs
  crates/mie-domain/src/research/spec.rs
  crates/mie-domain/src/lib.rs
  crates/mie-app/src/backtest/extra.rs
  crates/mie-app/src/backtester.rs
  crates/mie-ports/src/lib.rs
)
for path in "${backtest_allowed_paths[@]}"; do
  [[ -z "$(backtest_disallowed <<<"$path:1:use crate::backtest::Simulator;")" ]] \
    || fail "backtest guard self-test: allowed path is flagged: $path"
done
for path in "${backtest_kept_paths[@]}"; do
  [[ -n "$(backtest_disallowed <<<"$path:1:use crate::backtest::Simulator;")" ]] \
    || fail "backtest guard self-test: path outside the allow-list is dropped: $path"
done

# Re-exports from the allow-list (PR #176 review): an allowed file outside
# the backtest module that re-exports a backtest item, `pub use` or
# `pub type`, any visibility, would let every importer name it without a
# `backtest` token. Its importers must name the module themselves. Each
# item is joined up to its `;` by `joined_items`, since rustfmt wraps a
# long alias (PR #176 re-review). Takes files; an allowed file that does
# not exist yet is skipped.
backtest_reexport_scan() {
  local file files=()
  for file in "$@"; do
    [[ -f "$file" ]] && files+=("$file")
  done
  [[ ${#files[@]} -gt 0 ]] || return 0
  joined_items '^[[:space:]]*pub([[:space:]]*[(][^)]*[)][[:space:]]*|[[:space:]]+)(use|type)([[:space:]]|$)' "${files[@]}" \
    | grep -E "^[^:]+:[0-9]+:.*$backtest_items" || true
}

# shellcheck disable=SC2034 # read by self_test through a nameref
backtest_reexport_probes=(
  'pub use crate::backtest::TradeRecord;'
  $'pub(crate) use crate::backtest::{\n    Simulator,\n};'
  'pub(super) use crate::backtest as bt;'
  'pub type Sim = crate::backtest::Simulator;'
  'pub type Run = crate::outbound::BacktestRun;'
  'pub use crate::outbound::BacktestEngine as Engine;'
  $'/// Review probe.\npub type ReviewPair = (\n    crate::backtest::Simulator,\n    crate::backtest::TradeRecord,\n    crate::backtest::SimulationStats,\n);'
  $'pub type Window = (\n    [u8; 4],\n    crate::backtest::Simulator,\n);'
  $'pub(crate) type Sim =\n    crate::backtest::Simulator;'
)
# shellcheck disable=SC2034 # read by self_test through a nameref
backtest_reexport_clean=(
  'use crate::backtest::TradeRecord;'
  'pub use crate::research::ResultKey;'
  'pub type Records = Vec<Record>;'
  $'pub type Window = [u8; 4];\nfn f(_: crate::backtest::Simulator) {}'
)
self_test backtest-reexport backtest_reexport_scan backtest_reexport_probes backtest_reexport_clean

backtest_reexports=$(backtest_reexport_scan \
  crates/mie-domain/src/research/result.rs crates/mie-ports/src/outbound.rs \
  crates/mie-ports/src/inbound.rs crates/mie-app/src/backtest.rs crates/mie-app/src/research.rs)
if [[ -n "$backtest_reexports" ]]; then
  fail "backtest items re-exported from the allow-list (ADR-060 D15):"
  echo "$backtest_reexports"
fi

backtest_hits=$(backtest_scan crates/mie-domain/src crates/mie-ports/src crates/mie-app/src \
  | backtest_disallowed)
if [[ -n "$backtest_hits" ]]; then
  fail "the backtest named outside its allow-list (ADR-060 D15):"
  echo "$backtest_hits"
fi

if [[ $status -eq 0 ]]; then
  echo "OK: $(wc -w <<<"$members") crates — crate graph, core purity (ADR-025), label isolation (ADR-055), backtest isolation (ADR-060) and the macro ban hold"
fi
exit $status
