#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export GOPATH="${GOPATH:-$ROOT/.cache/go}"
export GOCACHE="${GOCACHE:-$ROOT/.cache/go-build}"
export GOMODCACHE="${GOMODCACHE:-$GOPATH/pkg/mod}"
mkdir -p "$GOCACHE" "$GOMODCACHE"

run() {
  echo
  echo "==> $*"
  "$@"
}

check_fmt() {
  local dir="$1"
  local out
  out="$(cd "$dir" && find . \
    \( -path './.git' -o -path './.cache' -o -path './node_modules' \) -prune -o \
    -name '*.go' -type f -print | xargs gofmt -l)"
  if [ -n "$out" ]; then
    echo "unformatted Go files in $dir:"
    echo "$out"
    echo
    echo "Run: gofmt -w <files>"
    exit 1
  fi
}

kafka_ready() {
  if command -v kafka-topics.sh >/dev/null 2>&1; then
    kafka-topics.sh --bootstrap-server "${KAFKA_BROKERS:-localhost:9092}" --list >/dev/null 2>&1
    return $?
  fi
  if [ -x /opt/kafka/bin/kafka-topics.sh ]; then
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${KAFKA_BROKERS:-localhost:9092}" --list >/dev/null 2>&1
    return $?
  fi
  return 1
}

echo "Running local pre-commit CI mirror..."
echo "Go path: $GOPATH"
echo "Go cache: $GOCACHE"
echo "Go module cache: $GOMODCACHE"

run go mod download

check_fmt "."
check_fmt "control"

run go vet ./...
run go vet -tags kubernetes ./control/...

run go build ./...
run go build ./examples/...
run go build ./control/...

run go test -race ./...
run go test -short -race ./control/...
run go test -short -race -tags kubernetes ./control/...

run go test -coverpkg=./... -coverprofile=coverage.out -covermode=atomic ./...
run go test -short -coverprofile=control/control-coverage.out -covermode=atomic ./control/...

if kafka_ready; then
  echo
  echo "==> Kafka broker detected; running kafka-e2e workflow checks"
  KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}" ./scripts/test-kafka.sh
  KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}" go test -count=1 -run TestKafkaMultiTopicCheckpointRestore ./source
  KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}" go test -count=1 -run TestTxnKafkaMarkerProbeIntegration ./sink
else
  echo
  echo "==> Kafka broker/CLI not detected; skipping kafka-e2e workflow checks"
  echo "    To include them locally, start Kafka on localhost:9092 and install kafka-topics.sh."
fi

echo
echo "pre-commit CI mirror passed"
