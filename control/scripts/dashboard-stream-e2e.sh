#!/usr/bin/env bash
#
# End-to-end dashboard test for the Weibo control plane.
#
# It boots the real `weibo dashboard` server, submits a continuously-running
# stream-processing job (source → filter → keyBy → window → reduce → sink),
# and then asserts the dashboard read-path actually reports live processing:
#
#   * /jobs                         job appears and reaches a running phase
#   * /jobs/{id}/describe           source, operators and sink are exposed
#   * /jobs/{id}/state              recordsIn/recordsOut climb over time
#   * /jobs/{id}/metrics            Prometheus counters + stage workers flow
#   * /jobs/history                 rolling sparkline samples accumulate
#
# By default it leaves the dashboard + job running so you can open the UI and
# watch it. Pass --ci to tear everything down when the assertions finish.
#
# Usage:
#   control/scripts/dashboard-stream-e2e.sh            # verify, then leave running
#   control/scripts/dashboard-stream-e2e.sh --ci       # verify, then clean up
#
# Env overrides: PORT, RATE, CHECKPOINT_INTERVAL, WEIBO_STREAM_IMAGE,
#                SKIP_BUILD=1 (reuse an existing image).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONTROL="$ROOT/control"

PORT="${PORT:-9000}"
BASE="http://127.0.0.1:${PORT}"
IMAGE="${WEIBO_STREAM_IMAGE:-weibo-stream-demo:dev}"
RATE="${RATE:-20}"
CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL:-5s}"
MANIFEST="$ROOT/examples/stream-demo/weibo.yaml"

CI=0
[[ "${1:-}" == "--ci" ]] && CI=1

WORK="$(mktemp -d "${TMPDIR:-/tmp}/weibo-e2e.XXXXXX")"
DB="$WORK/control.db"
DASH_PID=""
JOB_ID=""

log()  { printf '\033[1;34m[e2e]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[e2e] PASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[e2e] FAIL\033[0m %s\n' "$*" >&2; exit 1; }

cleanup() {
  local rc=$?
  if [[ -n "$JOB_ID" ]]; then
    # DELETE stops the container and removes store rows; -deleteData drops the
    # durable volume too so repeated runs start clean.
    curl -sS -X DELETE "$BASE/jobs/$JOB_ID?deleteData=true" >/dev/null 2>&1 || true
  fi
  if [[ -n "$DASH_PID" ]] && kill -0 "$DASH_PID" 2>/dev/null; then
    kill "$DASH_PID" 2>/dev/null || true
    wait "$DASH_PID" 2>/dev/null || true
  fi
  if [[ $CI -eq 1 || $rc -ne 0 ]]; then
    rm -rf "$WORK"
  else
    log "dashboard logs: $WORK/dashboard.log"
  fi
}
trap cleanup EXIT

# --- prerequisites ------------------------------------------------------------
command -v docker >/dev/null || fail "docker is required"
command -v jq >/dev/null || fail "jq is required"
docker info >/dev/null 2>&1 || fail "docker daemon is not reachable"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  if docker image inspect "$IMAGE" >/dev/null 2>&1; then
    log "image $IMAGE already exists (SKIP_BUILD=0 to force a rebuild)"
  else
    log "building demo image $IMAGE …"
    docker build -f "$ROOT/Dockerfile.stream-demo" -t "$IMAGE" "$ROOT" >"$WORK/build.log" 2>&1 \
      || { cat "$WORK/build.log" >&2; fail "image build failed"; }
  fi
fi

log "building weibo CLI …"
(cd "$CONTROL" && go build -o "$WORK/weibo" ./cmd/weibo)

# --- boot the dashboard -------------------------------------------------------
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
[[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/readyz")" == "200" ]] || fail "dashboard never became ready"
ok "dashboard up at $BASE (pid $DASH_PID)"

# --- submit the streaming job -------------------------------------------------
log "submitting stream-demo (rate=${RATE}/s, checkpoint=${CHECKPOINT_INTERVAL}) …"
BODY="$(jq -n --rawfile wf "$MANIFEST" \
  --arg rate "$RATE" --arg ci "$CHECKPOINT_INTERVAL" \
  '{workflow:$wf, env:{RECORDS_PER_SECOND:$rate, CHECKPOINT_INTERVAL:$ci}}')"
SUBMIT="$(curl -sS -X POST "$BASE/jobs" -H 'content-type: application/json' -d "$BODY")"
JOB_ID="$(jq -r '.job.id // .id // empty' <<<"$SUBMIT")"
[[ -n "$JOB_ID" ]] || fail "submit failed: $SUBMIT"
ok "submitted job $JOB_ID"

# --- 1. job reaches running ---------------------------------------------------
log "waiting for the job to report running …"
RUNNING=0
for _ in $(seq 1 60); do
  PHASE="$(curl -s "$BASE/jobs/$JOB_ID/state" | jq -r '.phase // empty' 2>/dev/null || true)"
  if [[ "$PHASE" == "running" ]]; then RUNNING=1; break; fi
  sleep 1
done
[[ "$RUNNING" == "1" ]] || fail "job never reached running (last phase: ${PHASE:-none})"
ok "job phase = running"

# --- 2. describe exposes the operator graph ----------------------------------
DESCRIBE="$(curl -s "$BASE/jobs/$JOB_ID/describe")"
jq -e '.source.type' <<<"$DESCRIBE" >/dev/null || fail "describe missing source: $DESCRIBE"
jq -e '.sink.type'   <<<"$DESCRIBE" >/dev/null || fail "describe missing sink: $DESCRIBE"
NOPS="$(jq '.operators | length' <<<"$DESCRIBE")"
[[ "$NOPS" -ge 4 ]] || fail "expected >=4 operators, got $NOPS: $DESCRIBE"
ok "describe: source=$(jq -r '.source.type' <<<"$DESCRIBE") sink=$(jq -r '.sink.type' <<<"$DESCRIBE") operators=$NOPS"

# --- 3. records actually flow ------------------------------------------------
log "waiting for the first records …"
IN1=0
for _ in $(seq 1 30); do
  IN1="$(curl -s "$BASE/jobs/$JOB_ID/state" | jq -r '.recordsIn // 0' 2>/dev/null || echo 0)"
  [[ "$IN1" -gt 0 ]] && break
  sleep 1
done
[[ "$IN1" -gt 0 ]] || fail "no records read after start (recordsIn=$IN1)"
sleep 4
IN2="$(curl -s "$BASE/jobs/$JOB_ID/state" | jq -r '.recordsIn // 0')"
[[ "$IN2" -gt "$IN1" ]] || fail "recordsIn did not advance ($IN1 → $IN2)"
ok "records flowing: recordsIn $IN1 → $IN2"

# recordsOut only appears once the first 10s tumbling window closes.
log "waiting for windowed results (recordsOut) …"
OUT=0
for _ in $(seq 1 40); do
  OUT="$(curl -s "$BASE/jobs/$JOB_ID/state" | jq -r '.recordsOut // 0')"
  [[ "$OUT" -gt 0 ]] && break
  sleep 1
done
[[ "$OUT" -gt 0 ]] || fail "no windowed output produced (recordsOut still 0)"
ok "windowed output produced: recordsOut=$OUT"

# --- 4. Prometheus metrics flow ----------------------------------------------
METRICS="$(curl -s "$BASE/jobs/$JOB_ID/metrics")"
grep -q 'weibo_records_read_total' <<<"$METRICS" || fail "metrics missing weibo_records_read_total"
grep -q 'weibo_stage_workers'      <<<"$METRICS" || fail "metrics missing weibo_stage_workers"
grep -q 'weibo_records_written_total' <<<"$METRICS" || fail "metrics missing weibo_records_written_total"
ok "Prometheus metrics exposed (records + stage workers)"

# --- 5. rolling history accumulates ------------------------------------------
log "waiting for rolling history samples …"
POINTS=0
for _ in $(seq 1 20); do
  POINTS="$(curl -s "$BASE/jobs/$JOB_ID/history?points=60" | jq -r '.points | length' 2>/dev/null || echo 0)"
  [[ "$POINTS" -ge 2 ]] && break
  sleep 2
done
[[ "$POINTS" -ge 2 ]] || fail "history did not accumulate samples (points=$POINTS)"
ok "history: $POINTS samples"

# --- done ---------------------------------------------------------------------
echo
ok "dashboard end-to-end verified for job $JOB_ID"
echo "    UI:    $BASE"
echo "    state: $BASE/jobs/$JOB_ID/state"

if [[ $CI -eq 1 ]]; then
  log "--ci: tearing down"
  exit 0
fi

log "leaving dashboard + job running — press Ctrl-C to stop and clean up"
while kill -0 "$DASH_PID" 2>/dev/null; do sleep 1; done