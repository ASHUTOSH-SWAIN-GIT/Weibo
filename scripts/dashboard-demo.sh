#!/usr/bin/env bash
#
# Boots the real dashboard and submits the stream-demo job so you can watch a
# live pipeline in the UI — no assertions, no teardown, just a job to look at.
#
# This is the script used to take the screenshots in docs/dashboard.md. For a
# scripted pass/fail check of the same read path, use `make dashboard-e2e`
# (control/scripts/dashboard-stream-e2e.sh) instead.
#
# Usage:
#   scripts/dashboard-demo.sh              # build the demo image, then run
#   SKIP_BUILD=1 scripts/dashboard-demo.sh # reuse an existing weibo-stream-demo:dev image
#
# Env overrides: PORT, RATE, CHECKPOINT_INTERVAL, WEIBO_STREAM_IMAGE
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTROL="$ROOT/control"

PORT="${PORT:-9000}"
BASE="http://127.0.0.1:${PORT}"
IMAGE="${WEIBO_STREAM_IMAGE:-weibo-stream-demo:dev}"
RATE="${RATE:-20}"
CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL:-5s}"
MANIFEST="$ROOT/examples/stream-demo/weibo.yaml"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/weibo-demo.XXXXXX")"
DB="$WORK/control.db"
DASH_PID=""
JOB_ID=""

log()  { printf '\033[1;34m[demo]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[demo] READY\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[demo] FAIL\033[0m %s\n' "$*" >&2; exit 1; }

cleanup() {
  if [[ -n "$JOB_ID" ]]; then
    curl -sS -X DELETE "$BASE/jobs/$JOB_ID?deleteData=true" >/dev/null 2>&1 || true
  fi
  if [[ -n "$DASH_PID" ]] && kill -0 "$DASH_PID" 2>/dev/null; then
    kill "$DASH_PID" 2>/dev/null || true
    wait "$DASH_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

command -v docker >/dev/null || fail "docker is required"
command -v jq >/dev/null || fail "jq is required"
docker info >/dev/null 2>&1 || fail "docker daemon is not reachable"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  log "building demo image $IMAGE …"
  docker build -f "$ROOT/Dockerfile.stream-demo" -t "$IMAGE" "$ROOT" >"$WORK/build.log" 2>&1 \
    || { cat "$WORK/build.log" >&2; fail "image build failed"; }
fi

log "building weibo CLI …"
(cd "$CONTROL" && go build -o "$WORK/weibo" ./cmd/weibo)

log "starting dashboard on $BASE …"
"$WORK/weibo" dashboard -no-open -addr "127.0.0.1:$PORT" -db "$DB" \
  -image "$IMAGE" -reconcile 1s -history-interval 2s \
  >"$WORK/dashboard.log" 2>&1 &
DASH_PID=$!

for _ in $(seq 1 60); do
  [[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/readyz" || true)" == "200" ]] && break
  kill -0 "$DASH_PID" 2>/dev/null || { cat "$WORK/dashboard.log" >&2; fail "dashboard exited early"; }
  sleep 0.5
done

log "submitting stream-demo (rate=${RATE}/s, checkpoint=${CHECKPOINT_INTERVAL}) …"
BODY="$(jq -n --rawfile wf "$MANIFEST" \
  --arg rate "$RATE" --arg ci "$CHECKPOINT_INTERVAL" \
  '{workflow:$wf, env:{RECORDS_PER_SECOND:$rate, CHECKPOINT_INTERVAL:$ci}}')"
SUBMIT="$(curl -sS -X POST "$BASE/jobs" -H 'content-type: application/json' -d "$BODY")"
JOB_ID="$(jq -r '.job.id // .id // empty' <<<"$SUBMIT")"
[[ -n "$JOB_ID" ]] || fail "submit failed: $SUBMIT"

ok "job $JOB_ID submitted — open $BASE and watch Sources / Pipeline / Reliability fill in"
log "press Ctrl-C to stop the job and the dashboard"
while kill -0 "$DASH_PID" 2>/dev/null; do sleep 1; done
