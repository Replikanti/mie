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
# still applies. Dev-dependencies are exempt from the graph rules.
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

if [[ $status -eq 0 ]]; then
  echo "OK: $(wc -w <<<"$members") crates — crate graph and core purity hold (ADR-025)"
fi
exit $status
