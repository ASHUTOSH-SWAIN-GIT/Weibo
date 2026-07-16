package runner_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/runner"
)

func TestCompileFileDryRunDoesNotExecute(t *testing.T) {
	path := writeWorkflow(t, `name: dry-run
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
sink: {type: stdout}
`)

	cw, err := runner.CompileFile(path, runner.Options{BaseDataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("CompileFile: %v", err)
	}
	if cw.Name != "dry-run" || cw.Graph.Source != "generator" || cw.Graph.Sink != "stdout" {
		t.Fatalf("unexpected compiled workflow: %+v", cw)
	}
}

func TestRunFileExecutesWorkflow(t *testing.T) {
	path := writeWorkflow(t, `name: run-file
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

	result, err := runner.RunFile(context.Background(), path, runner.Options{BaseDataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("RunFile: %v", err)
	}
	if result.Name != "run-file" || len(result.Graph.Operators) != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestCompileFileInvalidWorkflow(t *testing.T) {
	path := writeWorkflow(t, `name: invalid
source: {type: generator, records: [{value: '{"amount":1}'}]}
pipeline:
  - id: count
    type: reduce
    reduce: {function: count}
sink: {type: blackhole}
`)

	_, err := runner.CompileFile(path, runner.Options{BaseDataDir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "reduce requires a keyBy before it") {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestCompileFileMissingSecretErrorIsSanitized(t *testing.T) {
	t.Setenv("KAFKA_PASSWORD", "super-secret-value")
	path := writeWorkflow(t, `name: missing-secret
source:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: in
    groupID: g
    sasl: {mechanism: plain, username: "${KAFKA_USERNAME_MISSING}", password: "${KAFKA_PASSWORD}"}
sink: {type: blackhole}
`)

	_, err := runner.CompileFile(path, runner.Options{BaseDataDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected missing secret error")
	}
	if strings.Contains(err.Error(), "super-secret-value") {
		t.Fatalf("error leaked secret: %v", err)
	}
}

func writeWorkflow(t *testing.T, doc string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workflow.yaml")
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
