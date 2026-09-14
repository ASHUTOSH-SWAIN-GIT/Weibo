#!/usr/bin/env bash
set -euo pipefail

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

if command -v govulncheck >/dev/null 2>&1; then
  govulncheck_cmd=(govulncheck)
else
  govulncheck_cmd=(go run golang.org/x/vuln/cmd/govulncheck@latest)
fi

set +e
"${govulncheck_cmd[@]}" ./... 2>&1 | tee "$tmp"
status=${PIPESTATUS[0]}
set -e

if [ "$status" -eq 0 ]; then
  exit 0
fi

ids="$(grep -Eo 'GO-[0-9]{4}-[0-9]+' "$tmp" | sort -u || true)"
unexpected="$(
  printf '%s\n' "$ids" |
    grep -Ev '^(GO-2026-4883|GO-2026-4887)$' || true
)"

if [ -n "$unexpected" ]; then
  echo "::error::govulncheck found non-allowlisted vulnerabilities:"
  printf '%s\n' "$unexpected"
  exit "$status"
fi

if ! grep -q 'Module: github.com/docker/docker' "$tmp"; then
  echo "::error::allowlisted vulnerabilities were not reported against github.com/docker/docker"
  exit "$status"
fi

echo "::warning::Temporarily allowing Docker/Moby govulncheck findings GO-2026-4883 and GO-2026-4887 because the vulnerability database reports Fixed in: N/A."
