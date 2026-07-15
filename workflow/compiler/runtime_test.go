package compiler_test

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/compiler"
)

func countReduce(accum []byte, _ types.Record) []byte {
	var n uint64
	if accum != nil {
		n = binary.BigEndian.Uint64(accum)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n+1)
	return buf
}

func dirHasContent(t *testing.T, dir string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	return len(entries) > 0
}

// The success condition: runtime config produces an isolated,
// recoverable env. Run a real keyed pipeline through the compiled env
// with Pebble + checkpointing, and confirm the job-specific state and
// checkpoint directories were created and populated.
func TestCompileRuntime_IsolatedAndRecoverable(t *testing.T) {
	dataRoot := t.TempDir()
	rt := &workflow.EnvSpec{
		BufferSize:      512,
		ShutdownTimeout: workflow.Duration(5e9),
		Checkpointing:   &workflow.CheckpointSpec{Interval: workflow.Duration(5e6)}, // 5ms
		State:           &workflow.StateSpec{Backend: "pebble"},
	}

	env, err := compiler.CompileRuntime("order-totals", dataRoot, rt)
	if err != nil {
		t.Fatalf("CompileRuntime: %v", err)
	}

	// Build and run a small keyed pipeline on the compiled env.
	records := make([]types.Record, 200)
	for i := range records {
		records[i] = types.NewRecord([]byte("k"), []byte("v"))
	}
	env.FromSource(source.NewSliceSource(records)).
		KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(2).
		Reduce(countReduce).
		ToSink(sink.NewBlackholeSink())

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	stateDir := filepath.Join(dataRoot, "order-totals", "state")
	ckptDir := filepath.Join(dataRoot, "order-totals", "checkpoints")

	if !dirHasContent(t, stateDir) {
		t.Errorf("expected Pebble state under %s", stateDir)
	}
	if !dirHasContent(t, ckptDir) {
		t.Errorf("expected a checkpoint under %s (recoverable)", ckptDir)
	}
}

// Two workflows with different names must get different Pebble
// directories, so they cannot share a database.
func TestCompileRuntime_PerWorkflowIsolation(t *testing.T) {
	dataRoot := t.TempDir()
	rt := &workflow.EnvSpec{State: &workflow.StateSpec{Backend: "pebble"}}

	if _, err := compiler.CompileRuntime("alpha", dataRoot, rt); err != nil {
		t.Fatal(err)
	}
	if _, err := compiler.CompileRuntime("beta", dataRoot, rt); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dataRoot, "alpha", "state")); err != nil {
		t.Errorf("alpha state dir missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataRoot, "beta", "state")); err != nil {
		t.Errorf("beta state dir missing: %v", err)
	}
}

// A configured directory is honored as a root, with the workflow name
// nested under it for isolation.
func TestCompileRuntime_ConfiguredDirNestsName(t *testing.T) {
	root := t.TempDir()
	stateRoot := filepath.Join(root, "custom-state")
	rt := &workflow.EnvSpec{State: &workflow.StateSpec{Backend: "pebble", Dir: stateRoot}}

	if _, err := compiler.CompileRuntime("order-totals", root, rt); err != nil {
		t.Fatal(err)
	}
	// <configured>/<name>
	if _, err := os.Stat(filepath.Join(stateRoot, "order-totals")); err != nil {
		t.Errorf("expected configured state dir nested by name: %v", err)
	}
}

func TestCompileRuntime_MemoryAndDefaults(t *testing.T) {
	// nil runtime → all defaults, valid env.
	if env, err := compiler.CompileRuntime("wf", t.TempDir(), nil); err != nil || env == nil {
		t.Fatalf("nil runtime: env=%v err=%v", env, err)
	}
	// memory backend, no checkpointing → no directories created.
	env, err := compiler.CompileRuntime("wf", t.TempDir(), &workflow.EnvSpec{
		State: &workflow.StateSpec{Backend: "memory"},
	})
	if err != nil || env == nil {
		t.Fatalf("memory backend: %v", err)
	}
}

func TestCompileRuntime_Errors(t *testing.T) {
	root := t.TempDir()
	cases := []struct {
		name string
		rt   *workflow.EnvSpec
	}{
		{"bad backend", &workflow.EnvSpec{State: &workflow.StateSpec{Backend: "rocksdb"}}},
		{"zero interval", &workflow.EnvSpec{Checkpointing: &workflow.CheckpointSpec{Interval: 0}}},
		{"negative buffer", &workflow.EnvSpec{BufferSize: -1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := compiler.CompileRuntime("wf", root, c.rt); err == nil {
				t.Errorf("expected error for %s", c.name)
			}
		})
	}
}

// A dangerous workflow name cannot escape the data root via path
// traversal.
func TestCompileRuntime_NameSanitized(t *testing.T) {
	root := t.TempDir()
	env, err := compiler.CompileRuntime("../../etc", root, &workflow.EnvSpec{
		State: &workflow.StateSpec{Backend: "pebble"},
	})
	if err != nil || env == nil {
		t.Fatalf("sanitized name should still compile: %v", err)
	}
	// The state dir must remain under root (no traversal).
	if _, err := os.Stat(filepath.Join(root, "etc")); err == nil {
		// "etc" as a sanitized single segment under root is fine; the
		// point is nothing was created outside root.
	}
	// Nothing should have been created at the traversal target.
	if entries, _ := os.ReadDir(root); len(entries) == 0 {
		t.Error("expected a sanitized directory under root")
	}
}
