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
# labeler and its replay driver (ADR-055 D11).
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
label_lines='label::|ForwardLabel'
label_use='(^|[^[:alnum:]_])label($|[^[:alnum:]_])'

# Every `use` item of the given files on one line: `<file>:<line>:<item>`,
# with line comments dropped.
use_items() {
  awk '
    FNR == 1 { item = "" }
    item == "" && /^[[:space:]]*(pub(\([^)]*\))?[[:space:]]+)?use[[:space:]]/ { start = FNR; item = " " }
    item != "" {
      line = $0
      sub(/\/\/.*/, "", line)
      item = item " " line
      if (line ~ /;/) {
        print FILENAME ":" start ":" item
        item = ""
      }
    }
  ' "$@"
}

# The label-isolation hits in the given directories or files.
label_scan() {
  local files
  mapfile -d '' files < <(find "$@" -name '*.rs' -print0 | sort -z)
  {
    grep -rnHE "$label_lines" "$@" || true
    if [[ ${#files[@]} -gt 0 ]]; then
      use_items "${files[@]}" | grep -E "^[^:]+:[0-9]+:.*$label_use" || true
    fi
  } | sort -u
}

# Self-test of the label scan: each probe below must hit, each clean
# snippet must not. A guard that silently stops matching would pass every
# tree, so its blind spots are pinned here (PR #148 review).
probe_dir=$(mktemp -d)
trap 'rm -rf "$probe_dir"' EXIT
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
)
clean=(
  'use crate::conditional::{ContextRecord, ContextSampler};'
  $'use crate::regime::{\n    RegimeLabel,\n};'
  'let label = regime.label(); // a regime label'
  'pub fn label(self) -> RegimeLabel {'
)
for index in "${!probes[@]}"; do
  printf '%s\n' "${probes[$index]}" >"$probe_dir/probe_$index.rs"
  [[ -n "$(label_scan "$probe_dir/probe_$index.rs")" ]] \
    || fail "label guard self-test: probe $index is not caught: ${probes[$index]}"
done
for index in "${!clean[@]}"; do
  printf '%s\n' "${clean[$index]}" >"$probe_dir/clean_$index.rs"
  [[ -z "$(label_scan "$probe_dir/clean_$index.rs")" ]] \
    || fail "label guard self-test: clean snippet $index is flagged: ${clean[$index]}"
done

label_hits=$(label_scan crates/mie-domain/src crates/mie-ports/src crates/mie-app/src \
  | grep -vE '^crates/mie-domain/src/conditional/label[^:]*:|^crates/mie-app/src/conditional\.rs:' || true)
if [[ -n "$label_hits" ]]; then
  fail "forward labels named outside the labeler and its replay driver (ADR-055 D11):"
  echo "$label_hits"
fi

if [[ $status -eq 0 ]]; then
  echo "OK: $(wc -w <<<"$members") crates — crate graph, core purity (ADR-025) and label isolation (ADR-055) hold"
fi
exit $status
