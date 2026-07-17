package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestRun_MissingWorkflow(t *testing.T) {
	var out, errb bytes.Buffer
	code := run(context.Background(), envFrom(nil), &out, &errb)
	if code != exitUsage {
		t.Fatalf("exit: got %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errb.String(), "WORKFLOW is required") {
		t.Errorf("stderr missing usage hint: %q", errb.String())
	}
}

func TestRun_CompileError(t *testing.T) {
	var out, errb bytes.Buffer
	code := run(context.Background(), envFrom(map[string]string{
		"WORKFLOW": filepath.Join(t.TempDir(), "does-not-exist.yaml"),
		"DATA_DIR": t.TempDir(),
	}), &out, &errb)
	if code != exitError {
		t.Fatalf("exit: got %d, want %d", code, exitError)
	}
	if !strings.Contains(errb.String(), "compile") {
		t.Errorf("stderr missing compile error: %q", errb.String())
	}
}

// End-to-end: the runner compiles and runs a real bounded YAML job to
// completion under the agent, deriving state/checkpoint dirs under a
// mounted DATA_DIR — the same path a container takes, minus Docker.
func TestRun_YAMLJobToCompletion(t *testing.T) {
	wf, err := filepath.Abs(filepath.Join("..", "..", "examples", "workflows", "wordcount.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	code := run(context.Background(), envFrom(map[string]string{
		"WORKFLOW": wf,
		"DATA_DIR": t.TempDir(),
		"PORT":     "0", // ephemeral port; avoids collisions in CI
	}), &out, &errb)

	if code != exitOK {
		t.Fatalf("exit: got %d, want 0 (stderr: %s)", code, errb.String())
	}
	if !strings.Contains(out.String(), "job=wordcount") {
		t.Errorf("stdout missing job banner: %q", out.String())
	}
	if !strings.Contains(out.String(), "finished") {
		t.Errorf("stdout missing terminal phase: %q", out.String())
	}
}
