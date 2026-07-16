package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIRequiresFile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runCLI(nil, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code: got %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "missing required --file") {
		t.Fatalf("stderr missing usage error: %s", stderr.String())
	}
}

func TestCLIDryRun(t *testing.T) {
	path := writeCLIWorkflow(t, `name: cli-dry-run
source:
  type: generator
  records: [{key: k, value: '{"amount":1}'}]
pipeline:
  - id: by-key
    type: keyBy
    keyBy: {field: amount, partitions: 1}
  - id: count
    type: reduce
    reduce: {function: count}
sink: {type: blackhole}
`)
	var stdout, stderr bytes.Buffer
	code := runCLI([]string{"--file", path, "--dry-run", "--data-dir", t.TempDir()}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("exit code: got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "workflow: cli-dry-run") {
		t.Fatalf("stdout missing summary: %s", stdout.String())
	}
}

func TestCLIInvalidWorkflow(t *testing.T) {
	path := writeCLIWorkflow(t, `name: bad
source: {type: generator, records: [{value: '{"amount":1}'}]}
pipeline:
  - id: count
    type: reduce
    reduce: {function: count}
sink: {type: blackhole}
`)
	var stdout, stderr bytes.Buffer
	code := runCLI([]string{"--file", path, "--dry-run", "--data-dir", t.TempDir()}, &stdout, &stderr)
	if code != exitError {
		t.Fatalf("exit code: got %d, want %d", code, exitError)
	}
	if !strings.Contains(stderr.String(), "reduce requires a keyBy before it") {
		t.Fatalf("stderr missing validation error: %s", stderr.String())
	}
}

func TestCLIDescribeDoesNotLeakSecrets(t *testing.T) {
	t.Setenv("KAFKA_USERNAME", "cli-user")
	t.Setenv("KAFKA_PASSWORD", "cli-super-secret")
	path := writeCLIWorkflow(t, `name: cli-secret
source:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: in
    groupID: g
    sasl: {mechanism: plain, username: "${KAFKA_USERNAME}", password: "${KAFKA_PASSWORD}"}
pipeline:
  - id: by-key
    type: keyBy
    keyBy: {field: amount, partitions: 1}
  - id: count
    type: reduce
    reduce: {function: count}
sink:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: out
    sasl: {mechanism: plain, username: "${KAFKA_USERNAME}", password: "${KAFKA_PASSWORD}"}
`)
	var stdout, stderr bytes.Buffer
	code := runCLI([]string{"--file", path, "--dry-run", "--describe", "--data-dir", t.TempDir()}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("exit code: got %d stderr=%s", code, stderr.String())
	}
	for _, secret := range []string{"cli-user", "cli-super-secret"} {
		if strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
			t.Fatalf("CLI leaked secret %q\nstdout=%s\nstderr=%s", secret, stdout.String(), stderr.String())
		}
	}
	start := strings.Index(stdout.String(), "{")
	if start == -1 {
		t.Fatalf("describe JSON not found in stdout: %s", stdout.String())
	}
	var desc map[string]any
	if err := json.Unmarshal([]byte(stdout.String()[start:]), &desc); err != nil {
		t.Fatalf("invalid describe JSON: %v\n%s", err, stdout.String()[start:])
	}
}

func writeCLIWorkflow(t *testing.T, doc string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workflow.yaml")
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
