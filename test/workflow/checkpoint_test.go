package workflow_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/window"
)

func TestWindowStateMemoryAndPebbleCheckpointParity(t *testing.T) {
	mem := runWorkflowWindow(t, "memory", "", t.TempDir())
	pebble := runWorkflowWindow(t, "pebble", t.TempDir(), t.TempDir())
	assertSameValues(t, pebble, mem)
}

func TestWindowStateRecoversCorrectOutputShape(t *testing.T) {
	out := runWorkflowWindow(t, "pebble", t.TempDir(), t.TempDir())
	assertSameValues(t, out, []string{`{"sum":2}`, `{"sum":5}`})
}

func TestWindowStateSnapshotRestore(t *testing.T) {
	op1 := operator.Window(window.NewTumbling(time.Second))
	snapc := make(chan []byte, 1)
	errc := make(chan error, 1)
	op1.SetBarrierSnapshot(func(_ string, snap []byte, err error) {
		snapc <- snap
		errc <- err
	})

	in := make(chan types.Record, 3)
	out := make(chan types.Record, 3)
	go op1.Process(in, out)
	in <- types.Record{Key: []byte("k"), Value: []byte(`{"amount":2}`), Timestamp: time.Unix(0, 0)}
	in <- types.Record{Key: []byte("k"), Value: []byte(`{"amount":3}`), Timestamp: time.Unix(0, 100)}
	in <- types.NewBarrier("checkpoint-1")

	snap := <-snapc
	if err := <-errc; err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	close(in)
	for range out {
	}

	op2 := operator.Window(window.NewTumbling(time.Second))
	if err := op2.Restore(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}
	in2 := make(chan types.Record)
	out2 := make(chan types.Record, 4)
	go op2.Process(in2, out2)
	close(in2)

	var restored []types.Record
	for r := range out2 {
		restored = append(restored, r)
	}
	if len(restored) != 2 {
		t.Fatalf("restored records: got %d, want 2", len(restored))
	}
	for _, r := range restored {
		if string(r.Key) != "k" || len(r.Headers["window_start"]) == 0 || len(r.Headers["window_end"]) == 0 {
			t.Fatalf("restored record lost window metadata: %+v", r)
		}
	}
}

func TestTwoWorkflowsUseIsolatedStateDirectories(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"wf-a", "wf-b"} {
		doc := baseWorkflowYAML("pebble", root, "")
		doc = "name: " + name + "\n" + doc[len("name: orders\n"):]
		_ = runWorkflowYAML(t, doc, t.TempDir())
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("state dir for %s not created: %v", name, err)
		}
	}
	if filepath.Join(root, "wf-a") == filepath.Join(root, "wf-b") {
		t.Fatal("state directories are not isolated")
	}
}
