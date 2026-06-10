#!/usr/bin/env bash
#
# End-to-end test for the mailer Kafka pipeline.
#
# What this does:
#   1. Builds the kafka-orders example
#   2. Creates input and output Kafka topics
#   3. Produces a set of JSON-encoded Order records (including one malformed)
#   4. Starts the pipeline in the background
#   5. Waits for windowed aggregate results on the output topic
#   6. Validates per-customer totals and that the malformed record was dropped
#   7. Cleans up: kills the pipeline, deletes topics
#
# Requirements:
#   - Kafka broker on localhost:9092 (or set KAFKA_BROKERS)
#   - kafka-topics / kafka-console-producer / kafka-console-consumer in PATH
#     (brew install kafka, or set KAFKA_BIN)
#   - Go toolchain
#
# Usage:
#   ./scripts/test-kafka.sh
#
# Environment overrides:
#   KAFKA_BROKERS       broker list          (default: localhost:9092)
#   KAFKA_BIN           dir with kafka CLIs  (default: auto-detect)
#   KAFKA_INPUT_TOPIC   input topic          (default: mailer-orders-test)
#   KAFKA_OUTPUT_TOPIC  output topic         (default: mailer-order-summary-test)
#   KAFKA_WINDOW_SIZE   window size          (default: 5s)
#   PIPELINE_TIMEOUT    max wait for results (default: 30s)

set -euo pipefail

# --- config --------------------------------------------------------------------

BROKERS="${KAFKA_BROKERS:-localhost:9092}"
INPUT_TOPIC="${KAFKA_INPUT_TOPIC:-mailer-orders-test}"
OUTPUT_TOPIC="${KAFKA_OUTPUT_TOPIC:-mailer-order-summary-test}"
WINDOW_SIZE="${KAFKA_WINDOW_SIZE:-5s}"
TIMEOUT="${PIPELINE_TIMEOUT:-30}"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# --- locate kafka CLI tools ---------------------------------------------------

if [[ -n "${KAFKA_BIN:-}" ]]; then
    BIN="$KAFKA_BIN"
elif [[ -x /opt/homebrew/bin/kafka-topics ]]; then
    BIN=/opt/homebrew/bin
elif command -v kafka-topics >/dev/null 2>&1; then
    BIN="$(dirname "$(command -v kafka-topics)")"
else
    echo "error: kafka CLI tools not found. Set KAFKA_BIN or install kafka." >&2
    exit 1
fi

KAFKA_TOPICS="$BIN/kafka-topics"
KAFKA_CONSOLE_PRODUCER="$BIN/kafka-console-producer"
KAFKA_CONSOLE_CONSUMER="$BIN/kafka-console-consumer"

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
warn() { printf '\033[1;33m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# --- cleanup machinery --------------------------------------------------------

PIPELINE_PID=""
PIPELINE_LOG=$(mktemp -t mailer-pipeline.XXXXXX.log)

cleanup_and_exit() {
    local code=$1
    if [[ -n "$PIPELINE_PID" ]] && kill -0 "$PIPELINE_PID" 2>/dev/null; then
        log "stopping pipeline (pid $PIPELINE_PID)"
        kill "$PIPELINE_PID" 2>/dev/null || true
        for _ in 1 2 3 4; do
            kill -0 "$PIPELINE_PID" 2>/dev/null || break
            sleep 0.5
        done
        kill -9 "$PIPELINE_PID" 2>/dev/null || true
        wait "$PIPELINE_PID" 2>/dev/null || true
    fi
    if [[ -n "${KAFKA_TOPICS:-}" ]]; then
        "$KAFKA_TOPICS" --bootstrap-server "$BROKERS" --delete --topic "$INPUT_TOPIC"  2>/dev/null || true
        "$KAFKA_TOPICS" --bootstrap-server "$BROKERS" --delete --topic "$OUTPUT_TOPIC" 2>/dev/null || true
    fi
    rm -f /tmp/mailer-kafka-orders "$PIPELINE_LOG"
    exit "$code"
}
trap 'cleanup_and_exit $?' EXIT INT TERM

fail() { printf '\033[1;31m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; cleanup_and_exit 1; }

# --- preflight ----------------------------------------------------------------

# Split host:port from the first broker in the list
HOST="${BROKERS%%:*}"
PORT="${BROKERS##*:}"

log "checking kafka broker at $HOST:$PORT"
if ! nc -z "$HOST" "$PORT"; then
    echo "error: kafka broker not reachable at $HOST:$PORT" >&2
    cleanup_and_exit 1
fi
command -v go >/dev/null || { echo "error: go toolchain not in PATH" >&2; cleanup_and_exit 1; }

# --- build --------------------------------------------------------------------

log "building kafka-orders example"
(cd "$PROJECT_ROOT" && go build -o /tmp/mailer-kafka-orders ./examples/kafka-orders/)
[[ -x /tmp/mailer-kafka-orders ]] || fail "build failed"

# --- topics -------------------------------------------------------------------

log "creating topics"
"$KAFKA_TOPICS" --bootstrap-server "$BROKERS" --create --topic "$INPUT_TOPIC"  --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
"$KAFKA_TOPICS" --bootstrap-server "$BROKERS" --create --topic "$OUTPUT_TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true

# --- start pipeline -----------------------------------------------------------

log "starting pipeline (window=$WINDOW_SIZE)"

# 8 valid + 1 malformed. All valid records fall into the same window since they're
# produced within milliseconds of each other. The 9th is dropped by Filter.
ORDERS='{"order_id":"o1","customer":"alice","amount":100}
{"order_id":"o2","customer":"bob","amount":200}
{"order_id":"o3","customer":"alice","amount":150}
{"order_id":"o4","customer":"charlie","amount":300}
{"order_id":"o5","customer":"alice","amount":50}
{"order_id":"o6","customer":"bob","amount":100}
{"order_id":"o7","customer":"alice","amount":200}
{"order_id":"o8","customer":"bob","amount":50}
not-a-json-payload'
KAFKA_BROKERS="$BROKERS" \
KAFKA_INPUT_TOPIC="$INPUT_TOPIC" \
KAFKA_OUTPUT_TOPIC="$OUTPUT_TOPIC" \
KAFKA_GROUP_ID="mailer-test-$(date +%s)" \
KAFKA_WINDOW_SIZE="$WINDOW_SIZE" \
    /tmp/mailer-kafka-orders > "$PIPELINE_LOG" 2>&1 &
PIPELINE_PID=$!
log "pipeline started (pid $PIPELINE_PID)"

# Give the pipeline a moment to subscribe to the input topic before producing
sleep 2

# --- produce orders (now that the consumer is ready) -------------------------
log "producing 9 records (8 valid + 1 malformed) to $INPUT_TOPIC"
printf '%s\n' "$ORDERS" | "$KAFKA_CONSOLE_PRODUCER" \
    --bootstrap-server "$BROKERS" \
    --topic "$INPUT_TOPIC" \
    >/dev/null

# --- collect results ----------------------------------------------------------

log "collecting results from $OUTPUT_TOPIC (timeout ${TIMEOUT}s)..."
RESULTS_FILE=$(mktemp -t mailer-results.XXXXXX.jsonl)

# Expected: Reduce emits per record (running aggregate). For 8 valid orders:
#   alice  = 4 results (100, 250, 300, 500)
#   bob    = 3 results (200, 300, 350)
#   charlie = 1 result  (300)
#   total  = 8 results
EXPECTED_RESULTS=8

# Locate kafka-get-offsets
if [[ -x /opt/homebrew/bin/kafka-get-offsets ]]; then
    KAFKA_GET_OFFSETS=/opt/homebrew/bin/kafka-get-offsets
elif command -v kafka-get-offsets >/dev/null 2>&1; then
    KAFKA_GET_OFFSETS="$(command -v kafka-get-offsets)"
else
    KAFKA_GET_OFFSETS="$BIN/kafka-get-offsets"
fi

# Parse from kafka-get-offsets which gives "topic:partition:high_watermark"
get_offset_count() {
    "$KAFKA_GET_OFFSETS" --bootstrap-server "$BROKERS" --topic "$OUTPUT_TOPIC" 2>/dev/null \
        | awk -F: 'NR==1 {print $3; exit}' 2>/dev/null
}

DEADLINE=$(( $(date +%s) + TIMEOUT ))

# Poll the topic's high watermark until we have at least EXPECTED_RESULTS messages
# OR the timeout expires.
while [[ $(date +%s) -lt $DEADLINE ]]; do
    COUNT=$(get_offset_count)
    if [[ -n "$COUNT" ]] && [[ "$COUNT" -ge "$EXPECTED_RESULTS" ]]; then
        break
    fi
    sleep 1
done

# Signal the pipeline to shut down gracefully so it flushes any pending writes
log "sending SIGINT to pipeline for graceful shutdown"
kill -INT "$PIPELINE_PID" 2>/dev/null || true
# Wait for the pipeline to exit (with a 10s grace)
for _ in 1 2 3 4 5 6 7 8 9 10; do
    kill -0 "$PIPELINE_PID" 2>/dev/null || break
    sleep 1
done
PIPELINE_PID=""   # tell cleanup not to kill it again

# Now read all messages from the output topic
"$KAFKA_CONSOLE_CONSUMER" \
    --bootstrap-server "$BROKERS" \
    --topic "$OUTPUT_TOPIC" \
    --from-beginning \
    --timeout-ms 5000 2>/dev/null > "$RESULTS_FILE" || true

COUNT=$(wc -l < "$RESULTS_FILE" | tr -d ' ')
if [[ "$COUNT" -lt "$EXPECTED_RESULTS" ]]; then
    log "pipeline log:"
    sed 's/^/    /' "$PIPELINE_LOG"
    fail "expected $EXPECTED_RESULTS results on $OUTPUT_TOPIC, got $COUNT after ${TIMEOUT}s"
fi

log "received $COUNT result(s):"
sed 's/^/    /' "$RESULTS_FILE"

# --- verify -------------------------------------------------------------------

# Extract total for each customer from JSON: {"customer":"X","total":N,...}
# Use python or awk for robust JSON parsing.
extract_total() {
    local customer="$1"
    python3 -c "
import json, sys
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        d = json.loads(line)
        if d.get('customer') == '$customer':
            print(d.get('total'))
            sys.exit(0)
    except json.JSONDecodeError:
        pass
" 2>/dev/null
}

ALICE_TOTAL=$(extract_total "alice"   < "$RESULTS_FILE")
BOB_TOTAL=$(extract_total "bob"      < "$RESULTS_FILE")
CHARLIE_TOTAL=$(extract_total "charlie" < "$RESULTS_FILE")

EXPECTED_ALICE=500    # 100 + 150 + 50 + 200
EXPECTED_BOB=350      # 200 + 100 + 50
EXPECTED_CHARLIE=300  # 300

log "verifying aggregates:"
FAIL=0
check() {
    local name="$1" got="$2" want="$3"
    if [[ -z "$got" ]]; then
        warn "  ✗ $name: no result found"
        FAIL=1
    elif [[ "$got" == "$want" ]]; then
        log "  ✓ $name total = \$$got"
    else
        warn "  ✗ $name total = \$$got (expected \$$want)"
        FAIL=1
    fi
}

check alice   "$ALICE_TOTAL"   "$EXPECTED_ALICE"
check bob     "$BOB_TOTAL"     "$EXPECTED_BOB"
check charlie "$CHARLIE_TOTAL" "$EXPECTED_CHARLIE"

# Verify the malformed record was dropped
if grep -q '99999\|o-bad' "$RESULTS_FILE" 2>/dev/null; then
    warn "  ✗ malformed record leaked into output"
    FAIL=1
else
    log "  ✓ malformed record was dropped by Filter"
fi

rm -f "$RESULTS_FILE"

if [[ $FAIL -ne 0 ]]; then
    log "pipeline log:"
    sed 's/^/    /' "$PIPELINE_LOG"
    fail "verification failed"
fi

log "all checks passed"
