#!/usr/bin/env bash
#
# Coverage quality gate shared by `make coverage-gate` and hosted CI.
#
# Roadmap #27: publish root/control coverage separately and gate
# changed-package regressions before adopting a global threshold.
#
# What it does:
#   1. Expects coverage profiles (default: ./coverage.out and
#      ./control/control-coverage.out — produced by `make test-coverage`).
#   2. Prints a per-package coverage table for the root and control
#      modules separately.
#   3. When --gate is passed, fails if any package touched by the diff
#      against --base (default: origin/main) is missing from the profile
#      or sits below --min percent (default: 50). Untouched packages are
#      never gated, so legacy low-coverage code cannot block unrelated PRs.
#
# Gate rule: a package counts as changed only when the diff touches its
# non-test sources. Test-only changes can only raise coverage, so they
# skip the gate. A gated package must sit at or above --min percent
# (default 50); anything lower fails with the package and its current
# number, so the author adds tests in the same PR or justifies `--min`.
#
# Usage:
#   ./scripts/check-coverage.sh [--root-prof FILE] [--control-prof FILE]
#                               [--base REF] [--min PCT] [--gate]
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

ROOT_PROF="coverage.out"
CONTROL_PROF="control/control-coverage.out"
BASE="origin/main"
MIN="50"
GATE=0

while [ $# -gt 0 ]; do
    case "$1" in
        --root-prof) ROOT_PROF="$2"; shift 2 ;;
        --control-prof) CONTROL_PROF="$2"; shift 2 ;;
        --base) BASE="$2"; shift 2 ;;
        --min) MIN="$2"; shift 2 ;;
        --gate) GATE=1; shift ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

log() { printf '\033[1;34m[check-coverage]\033[0m %s\n' "$*"; }
bad() { printf '\033[1;31m[check-coverage]\033[0m %s\n' "$*" >&2; }

# Per-package averages parsed from the coverage profile itself
# (mode + file:start,end stmts count per line).
#
# NOTE: profiles produced by `go test -coverpkg=./... ./...` concatenate
# one section per test binary, so every block appears many times (count=0
# in binaries that never ran it). Deduplicate by block: statements count
# once, and a block is covered when ANY copy has count > 0.
package_table() {
    local prof="$1"
    awk '
        NR > 1 && $1 != "mode:" {
            if (!($1 in stmts)) { stmts[$1]=$2; order[++n]=$1 }
            if ($3 > 0) hit[$1]=1
        }
        END {
            for (i=1; i<=n; i++) {
                file=order[i]; sub(/:[0-9]+\.[0-9]+,[0-9]+\.[0-9]+$/, "", file);
                dir=file; sub(/\/[^\/]+$/, "", dir);
                total[dir]+=stmts[order[i]]; if (hit[order[i]]) covered[dir]+=stmts[order[i]];
            }
            for (d in total) {
                pct=(total[d]>0)?(covered[d]/total[d]*100):100;
                printf "%-70s %5d stmts %6.2f%%\n", d, total[d], pct;
            }
        }' "$prof" | sort
}

# Coverage percent for one package dir given as a full import path prefix
# (e.g. github.com/ASHUTOSH-SWAIN-GIT/weibo/sink). Empty when absent.
package_pct() {
    local prof="$1" pkg="$2"
    awk -v pkg="$pkg" '
        NR > 1 && $1 != "mode:" {
            if (!($1 in stmts)) { stmts[$1]=$2; order[++n]=$1 }
            if ($3 > 0) hit[$1]=1
        }
        END {
            for (i=1; i<=n; i++) {
                file=order[i]; sub(/:[0-9]+\.[0-9]+,[0-9]+\.[0-9]+$/, "", file);
                dir=file; sub(/\/[^\/]+$/, "", dir);
                if (dir == pkg) { t+=stmts[order[i]]; if (hit[order[i]]) s+=stmts[order[i]]; m++ }
            }
            if (m>0 && t>0) printf "%.2f", s/t*100
        }' "$prof"
}

fail=0
for mod in "root:$ROOT_PROF" "control:$CONTROL_PROF"; do
    name="${mod%%:*}"; prof="${mod##*:}"
    if [ ! -f "$prof" ]; then
        bad "missing profile $prof (run make test-coverage first)"
        fail=1
        continue
    fi
    log "== $name module ($prof) =="
    package_table "$prof"
    go tool cover -func="$prof" | tail -1
done
[ "$fail" -ne 0 ] && exit 1

if [ "$GATE" -eq 0 ]; then
    log "report only (pass --gate to enforce changed-package minimums)"
    exit 0
fi

# --- changed-package gate ------------------------------------------------------
if ! git rev-parse --verify "$BASE" >/dev/null 2>&1; then
    log "base $BASE not found; gating against working-tree diff only"
    CHANGED="$(git diff --name-only HEAD -- '*.go' || true)"
else
    # Committed range plus uncommitted working-tree changes (covers both
    # CI merge-commits and local `make coverage-gate` runs).
    CHANGED="$(printf '%s\n%s' \
        "$(git diff --name-only "$BASE"...HEAD -- '*.go' || true)" \
        "$(git diff --name-only HEAD -- '*.go' || true)")"
fi
# New files are not in any diff: include untracked Go files too.
UNTRACKED="$(git ls-files --others --exclude-standard -- '*.go' || true)"
CHANGED="$(printf '%s\n%s' "$CHANGED" "$UNTRACKED" | sed '/^[[:space:]]*$/d' | sort -u)"
if [ -z "$CHANGED" ]; then
    log "no Go files changed; gate passes"
    exit 0
fi

log "changed Go files:"; echo "$CHANGED" | sed 's/^/  /'
PKGS="$(echo "$CHANGED" | xargs -n1 dirname | sort -u)"

for pkg in $PKGS; do
    # Test-only changes skip the gate: they can only raise coverage.
    if [ "$pkg" = "." ]; then
        src_changed="$(echo "$CHANGED" | grep -v '/' || true)"
    else
        src_changed="$(echo "$CHANGED" | grep "^$pkg/" | grep -v '_test\.go$' || true)"
    fi
    if [ -z "$src_changed" ]; then
        log "gate skip: $pkg (test-only change)"
        continue
    fi
    case "$pkg" in
        control/*) prof="$CONTROL_PROF"; cover_pkg="github.com/ASHUTOSH-SWAIN-GIT/weibo/$pkg" ;;
        telemetry/*) log "skip $pkg (telemetry module has its own profile)"; continue ;;
        .) prof="$ROOT_PROF"; cover_pkg="github.com/ASHUTOSH-SWAIN-GIT/weibo" ;;
        *) prof="$ROOT_PROF"; cover_pkg="github.com/ASHUTOSH-SWAIN-GIT/weibo/$pkg" ;;
    esac
    pct="$(package_pct "$prof" "$cover_pkg")"
    if [ -z "$pct" ]; then
        # No coverable statements in the profile (doc/test-only package,
        # or files excluded by build tags from the default coverage run).
        log "gate skip: $pkg has no coverable statements in $prof"
        continue
    fi
    if awk "BEGIN {exit !($pct < $MIN)}"; then
        bad "gate: $pkg coverage ${pct}% < ${MIN}% minimum"
        fail=1
    else
        log "gate ok: $pkg ${pct}% (>= ${MIN}%)"
    fi
done

if [ "$fail" -ne 0 ]; then
    bad "coverage gate FAILED"
    exit 1
fi
log "coverage gate passed"
