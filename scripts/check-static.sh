#!/usr/bin/env bash
#
# Static-repo checks shared by `make check-static` and hosted CI.
#
# Covers roadmap #25: workflows, Dockerfiles, shell, YAML, and docs links —
# the non-Go checks that `gofmt`/`go vet` do not cover.
#
# YAML validation uses PyYAML when available; CI installs it first.
# Shell scripts get `bash -n`; shellcheck/shfmt run only when installed.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

fail=0
warn() { printf '\033[1;33m[check-static]\033[0m %s\n' "$*" >&2; }
bad()  { printf '\033[1;31m[check-static]\033[0m %s\n' "$*" >&2; fail=1; }
ok()   { printf '\033[1;34m[check-static]\033[0m %s\n' "$*"; }

# --- YAML -------------------------------------------------------------------
# Every workflow, example workflow, and manifest must parse.

# Tracked files plus new untracked ones; never the module cache or .git.
list_files() {
    git ls-files --cached --others --exclude-standard 2>/dev/null || \
        find . -path ./.git -prune -o -path ./.cache -prune -o -path ./vendor -prune -o -type f -print | sed 's|^\./||'
}
YAML_FILES="$(list_files | grep -E '\.ya?ml$' || true)"
if [ -z "$YAML_FILES" ]; then
    warn "no YAML files found"
else
    if python3 -c "import yaml" 2>/dev/null; then
        for f in $YAML_FILES; do
            if ! python3 -c "import sys,yaml; list(yaml.safe_load_all(open(sys.argv[1])))" "$f"; then
                bad "invalid YAML: $f"
            fi
        done
        ok "yaml: all files parse"
    else
        warn "PyYAML not installed; skipping YAML parse (CI installs it). Falling back to tab check."
        for f in $YAML_FILES; do
            if grep -P '\t' "$f" >/dev/null 2>&1; then
                bad "tab indentation in YAML (use spaces): $f"
            fi
        done
    fi
fi

# Workflows must pin third-party actions (tag or SHA), never a bare branch.
for wf in .github/workflows/*.yml; do
    while IFS= read -r line; do
        case "$line" in
            *uses:*@*) ;;
            *uses:./*|*uses:docker://*) ;;
            *uses:*) bad "unpinned action in $wf: $line" ;;
        esac
    done < <(grep -n 'uses:' "$wf" || true)
done
ok "workflows: actions pinned"

# --- shell -------------------------------------------------------------------
SH_FILES="$(list_files | grep -E '\.sh$' || true)"
for f in $SH_FILES; do
    if ! bash -n "$f"; then
        bad "bash syntax error: $f"
    fi
    if command -v shellcheck >/dev/null 2>&1; then
        if ! shellcheck -S warning "$f"; then
            bad "shellcheck findings: $f"
        fi
    fi
done
ok "shell: syntax ok (${SH_FILES:-none})"
if ! command -v shellcheck >/dev/null 2>&1; then
    warn "shellcheck not installed; ran bash -n only"
fi

# --- Dockerfiles --------------------------------------------------------------
DOCKERFILES="$(list_files | grep -E 'Dockerfile' || true)"
if [ -z "$DOCKERFILES" ]; then
    warn "no Dockerfiles found"
else
    for f in $DOCKERFILES; do
        if ! grep -q '^FROM ' "$f"; then
            bad "Dockerfile without FROM: $f"
        fi
    done
    ok "dockerfiles: FROM present in all"
fi
if command -v hadolint >/dev/null 2>&1; then
    for f in $DOCKERFILES; do
        if ! hadolint "$f"; then
            bad "hadolint findings: $f"
        fi
    done
    ok "dockerfiles: hadolint clean"
else
    warn "hadolint not installed; ran FROM check only"
fi

# --- docs links ---------------------------------------------------------------
# Relative markdown links must resolve to a file in the repo.
LINK_FAIL=0
for md in $(list_files | grep -E '\.md$' || true); do
    dir="$(dirname "$md")"
    while IFS= read -r link; do
        # strip query/fragment, skip absolute URLs and anchors
        clean="$(printf '%s' "$link" | sed 's/[?#].*$//')"
        case "$clean" in
            ""|http://*|https://*|mailto:*|"#") continue ;;
        esac
        if [ ! -e "$dir/$clean" ] && [ ! -e "$clean" ]; then
            bad "broken relative link in $md: $link"
            LINK_FAIL=1
        fi
    done < <(grep -oE '\]\([^)]+\)' "$md" | sed 's/^](//;s/)$//' || true)
done
if [ "$LINK_FAIL" -eq 0 ]; then
    ok "docs: relative links resolve"
fi

if [ "$fail" -ne 0 ]; then
    bad "check-static: FAILED"
    exit 1
fi
ok "check-static: all checks passed"
