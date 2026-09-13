#!/usr/bin/env bash
#
# Fuzz smoke test shared by `make fuzz-smoke` and hosted CI (roadmap #27).
#
# Runs every registered fuzz target for a short, bounded time so pull
# requests get signal without a long fuzzing budget. Nightly/merge runs
# can raise FUZZTIME (e.g. FUZZTIME=5m).
#
# Usage: ./scripts/fuzz-smoke.sh [fuzztime]
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

FUZZTIME="${1:-${FUZZTIME:-10s}}"
log() { printf '\033[1;34m[fuzz-smoke]\033[0m %s\n' "$*"; }

# package-path target pairs; keep in sync with the Fuzz* functions.
TARGETS="
./test/workflow FuzzParseYAML
./test/workflow FuzzFieldPaths
./test/workflow FuzzFilterComparisons
./test/workflow FuzzNumericConversions
./workflow/record FuzzRecordFieldPaths
./checkpoint FuzzExtractCheckpoint
"

CONTROL_TARGETS="
./api FuzzAPIRequestDecoding
./api FuzzAPIRequestBodyLimits
"

fail=0
run_fuzz() {
    local dir="$1" target="$2"
    log "fuzzing $target in $dir for $FUZZTIME"
    if (cd "$dir" && go test -run="^$" -fuzz="^${target}$" -fuzztime="$FUZZTIME" .); then
        log "ok: $target"
    else
        printf '\033[1;31m[fuzz-smoke]\033[0m FAIL: %s in %s\n' "$target" "$dir" >&2
        fail=1
    fi
}

while read -r dir target; do
    [ -z "$dir" ] && continue
    run_fuzz "$PROJECT_ROOT/$dir" "$target"
done <<< "$TARGETS"

while read -r dir target; do
    [ -z "$dir" ] && continue
    run_fuzz "$PROJECT_ROOT/control/$dir" "$target"
done <<< "$CONTROL_TARGETS"

if [ "$fail" -ne 0 ]; then
    echo "[fuzz-smoke] FAILED" >&2
    exit 1
fi
log "all fuzz smoke targets passed"
