#!/usr/bin/env bash
# Prints the AW1 reports of ADR-062 (#190): for each window, in argument
# order, `mie replay --source live`, `mie replay --source archive` and
# `mie equivalence`, so a host run diffs the reports of one store before
# and after a tier change (adoption, removal, a copy target swapped)
# against the same baseline.
#
#   tools/tier-reports.sh [--strip <dir>] <ingest config> <archive config> \
#       <from> <to> [<from> <to>]...
#
# The binary is $MIE (default `mie`). Each report is a header line
# `== <verb and flags, without --config> ==`, its stdout, a line
# `-- stderr --`, its stderr and `exit <code>`. A failing report is part of
# the output, not an error of this script. With --strip, every occurrence of
# <dir>, as given and canonicalized, becomes `<work>`, so the output holds
# no local path. <from> and <to> are passed as given (`--from`/`--to` of
# `mie replay`: epoch ms or YYYY-MM-DD).
#
# Exits 0 once every report is printed, 2 on a usage error or when $MIE
# does not run.
set -euo pipefail

usage() {
  echo "usage: tools/tier-reports.sh [--strip <dir>] <ingest config> <archive config> <from> <to> [<from> <to>]..." >&2
  exit 2
}

mie=${MIE:-mie}
strip=()
if [[ ${1:-} == --strip ]]; then
  [[ $# -ge 2 && -n $2 ]] || usage
  strip+=("$2")
  if canonical=$(realpath -- "$2" 2>/dev/null) && [[ $canonical != "$2" ]]; then
    strip+=("$canonical")
  fi
  shift 2
fi
[[ $# -ge 4 && $((($# - 2) % 2)) -eq 0 ]] || usage
ingest=$1
archive=$2
shift 2

if ! "$mie" --help >/dev/null 2>&1; then
  echo "tier-reports: cannot run $mie (set MIE to the binary)" >&2
  exit 2
fi

# The longer spelling first, so a canonical path that extends the given one
# is replaced whole.
if [[ ${#strip[@]} -eq 2 && ${#strip[1]} -gt ${#strip[0]} ]]; then
  strip=("${strip[1]}" "${strip[0]}")
fi

work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT

# Prints the file $1 with every stripped directory replaced by <work>,
# trailing newlines kept.
scrubbed() {
  local text
  text=$(
    cat -- "$1"
    printf x
  )
  text=${text%x}
  local dir
  for dir in ${strip[@]+"${strip[@]}"}; do
    text=${text//"$dir"/<work>}
  done
  printf '%s' "$text"
}

# One report: the header $1, then `mie` with the remaining arguments.
report() {
  local header=$1
  shift
  local code=0
  "$mie" "$@" >"$work/out" 2>"$work/err" || code=$?
  printf '== %s ==\n' "$header"
  scrubbed "$work/out"
  printf -- '-- stderr --\n'
  scrubbed "$work/err"
  printf 'exit %s\n' "$code"
}

while [[ $# -gt 0 ]]; do
  from=$1
  to=$2
  shift 2
  report "replay --source live --from $from --to $to" \
    replay --config "$ingest" --source live --from "$from" --to "$to"
  report "replay --source archive --from $from --to $to" \
    replay --config "$archive" --source archive --from "$from" --to "$to"
  report "equivalence --from $from --to $to" \
    equivalence --config "$ingest" --from "$from" --to "$to"
done
