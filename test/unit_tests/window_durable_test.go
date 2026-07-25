package weibo_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/state"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/window"
)

// windowRun drives a WindowOperator (with an injected backend) over a
// script of inputs and returns the non-marker output records. If a
// barrier appears in inputs, snapCapture receives the operator's
// snapshot bytes taken at that barrier.
func windowRun(t *testing.T, op *operator.WindowOperator, inputs []types.Record) []types.Record {
	t.Helper()
	in := make(chan types.Record, len(inputs)+1)
	out := make(chan types.Record, 256)
	for _, r := range inputs {
		in <- r
	}
	close(in)

	done := make(chan struct{})
	var got []types.Record
	go func() {
		defer close(done)
		for r := range out {
			if !r.IsWatermark && !r.IsBarrier {
				got = append(got, r)
			}
		}
	}()
	op.Process(in, out)
	<-done
	return got
}

func winHeader(r types.Record, name string) string { return string(r.Headers[name]) }

// TestWindow_RecordsLiveInBackend proves buffered window records are
// stored in the injected backend (not a private map): after buffering
// two records into an open window, the backend's window_records list
// namespace holds them, keyed by the window key.
func TestWindow_RecordsLiveInBackend(t *testing.T) {
	backend := state.NewMemoryBackend()
	op := operator.Window(window.NewTumbling(5 * time.Second))
	op.SetStateBackend(backend)

	// Inspect the backend from inside the barrier hook — it runs
	// synchronously on the Process goroutine, so there is no race with
	// record buffering.
	type snap struct {
		windows int
		records int
	}
	result := make(chan snap, 1)
	op.SetBarrierSnapshot(func(string, []byte, error) {
		ls := backend.ListState("window_records")
		keys := ls.Keys()
		recs := 0
		for _, k := range keys {
			ls.SetKey(k)
			recs += len(ls.GetAll())
		}
		result <- snap{windows: len(keys), records: recs}
	})

	windowRun(t, op, []types.Record{
		{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(2, 0)},
		{Key: []byte("k1"), Value: []byte("v2"), Timestamp: time.Unix(3, 0)},
		types.NewBarrier("cp-1"),
	})

	s := <-result
	if s.windows != 1 {
		t.Fatalf("expected 1 open window buffered in the backend, got %d", s.windows)
	}
	if s.records != 2 {
		t.Errorf("expected 2 buffered records in the backend, got %d", s.records)
	}
}

// TestWindow_SnapshotRestore_JSONPath verifies the serialized snapshot
// path (memory backend and pebble-compat) roundtrips window contents:
// records buffered before a barrier are restored into a fresh operator
// and fire correctly afterwards.
func TestWindow_SnapshotRestore_JSONPath(t *testing.T) {
	for _, bk := range []struct {
		name string
		open func(t *testing.T) state.StateBackend
	}{
		{"Memory", func(*testing.T) state.StateBackend { return state.NewMemoryBackend() }},
		{"Pebble", func(t *testing.T) state.StateBackend {
			p, err := state.OpenPebble(filepath.Join(t.TempDir(), "db"))
			if err != nil {
				t.Fatal(err)
			}
			return p
		}},
	} {
		t.Run(bk.name, func(t *testing.T) {
			b1 := bk.open(t)

			// Buffer 2 records into window [0,5), snapshot at a barrier,
			// do NOT fire (no watermark reaches the window end).
			var snap []byte
			op1 := operator.Window(window.NewTumbling(5 * time.Second))
			op1.SetStateBackend(b1)
			op1.SetBarrierSnapshot(func(_ string, s []byte, err error) {
				if err != nil {
					t.Fatalf("snapshot: %v", err)
				}
				snap = s
			})
			windowRun(t, op1, []types.Record{
				{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(2, 0)},
				{Key: []byte("k1"), Value: []byte("v2"), Timestamp: time.Unix(3, 0)},
				types.NewBarrier("cp-1"),
				// input closes → flushRemaining fires, but op1's output is
				// discarded; we only care about the captured snapshot.
			})
			if len(snap) == 0 {
				t.Fatal("no snapshot captured at barrier")
			}

			// Restore into a fresh operator + backend, then fire.
			b2 := bk.open(t)
			op2 := operator.Window(window.NewTumbling(5 * time.Second))
			op2.SetStateBackend(b2)
			if err := op2.Restore(snap); err != nil {
				t.Fatalf("restore: %v", err)
			}
			got := windowRun(t, op2, []types.Record{
				types.NewWatermark(time.Unix(6, 0)), // fires window [0,5)
			})
			if len(got) != 2 {
				t.Fatalf("expected 2 records from restored window, got %d", len(got))
			}
			for _, r := range got {
				if winHeader(r, "window_start") != "1970-01-01T00:00:00Z" ||
					winHeader(r, "window_end") != "1970-01-01T00:00:05Z" {
					t.Errorf("restored window bounds wrong: [%s,%s)",
						winHeader(r, "window_start"), winHeader(r, "window_end"))
				}
			}
		})
	}
}

// TestWindow_NativeCheckpointRestore proves window records survive a
// native (hard-link) Pebble checkpoint: the operator checkpoints its
// backend at a barrier, a fresh backend is rebuilt from that checkpoint
// dir, and the restored window fires with all its records — no JSON
// serialization of window contents involved.
func TestWindow_NativeCheckpointRestore(t *testing.T) {
	dir := t.TempDir()
	b1, err := state.OpenPebble(filepath.Join(dir, "live"))
	if err != nil {
		t.Fatal(err)
	}
	ckptDir := filepath.Join(dir, "ckpt")

	op1 := operator.Window(window.NewTumbling(5 * time.Second))
	op1.SetStateBackend(b1)
	// Native snapshot: hard-link the backend at the barrier.
	op1.SetNativeSnapshot(func(string) ([]byte, error) {
		return nil, b1.CheckpointTo(ckptDir)
	})
	captured := make(chan error, 1)
	op1.SetBarrierSnapshot(func(_ string, _ []byte, err error) { captured <- err })

	// Buffer 3 records into window [0,5) and checkpoint at the barrier.
	windowRun(t, op1, []types.Record{
		{Key: []byte("k1"), Value: []byte("a"), Timestamp: time.Unix(1, 0)},
		{Key: []byte("k1"), Value: []byte("b"), Timestamp: time.Unix(2, 0)},
		{Key: []byte("k1"), Value: []byte("c"), Timestamp: time.Unix(3, 0)},
		types.NewBarrier("cp-1"),
	})
	if err := <-captured; err != nil {
		t.Fatalf("native checkpoint: %v", err)
	}
	b1.Close()

	// Rebuild a fresh backend from the checkpoint dir (as recovery does).
	b2, err := state.OpenPebble(filepath.Join(dir, "live2"))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.RestoreFrom(ckptDir); err != nil {
		t.Fatalf("restore from native checkpoint: %v", err)
	}

	op2 := operator.Window(window.NewTumbling(5 * time.Second))
	op2.SetStateBackend(b2)
	got := windowRun(t, op2, []types.Record{
		types.NewWatermark(time.Unix(6, 0)), // fires window [0,5)
	})
	if len(got) != 3 {
		t.Fatalf("expected 3 records from natively-restored window, got %d", len(got))
	}
	vals := map[string]bool{}
	for _, r := range got {
		vals[string(r.Value)] = true
	}
	for _, v := range []string{"a", "b", "c"} {
		if !vals[v] {
			t.Errorf("record %q lost across native checkpoint", v)
		}
	}
}

// TestWindow_NativeWatermarkRestore checks the watermark itself survives
// a native checkpoint (it lives in the backend's value state): after
// restore, late records below the restored watermark are dropped.
func TestWindow_NativeWatermarkRestore(t *testing.T) {
	dir := t.TempDir()
	b1, err := state.OpenPebble(filepath.Join(dir, "live"))
	if err != nil {
		t.Fatal(err)
	}
	ckptDir := filepath.Join(dir, "ckpt")

	op1 := operator.Window(window.NewTumbling(5 * time.Second))
	op1.SetStateBackend(b1)
	op1.SetNativeSnapshot(func(string) ([]byte, error) { return nil, b1.CheckpointTo(ckptDir) })
	captured := make(chan error, 1)
	op1.SetBarrierSnapshot(func(_ string, _ []byte, err error) { captured <- err })

	// Advance the watermark to t=15, then checkpoint.
	windowRun(t, op1, []types.Record{
		{Key: []byte("k1"), Value: []byte("x"), Timestamp: time.Unix(10, 0)},
		types.NewWatermark(time.Unix(15, 0)), // fires [10,15) and advances wm
		types.NewBarrier("cp-1"),
	})
	if err := <-captured; err != nil {
		t.Fatalf("native checkpoint: %v", err)
	}
	b1.Close()

	b2, err := state.OpenPebble(filepath.Join(dir, "live2"))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if err := b2.RestoreFrom(ckptDir); err != nil {
		t.Fatal(err)
	}
	op2 := operator.Window(window.NewTumbling(5 * time.Second))
	op2.SetStateBackend(b2)

	if got := op2.CurrentWatermark(); !got.Equal(time.Unix(15, 0)) {
		// CurrentWatermark is loaded lazily in Process; drive one record
		// through to trigger the load, then assert via behavior below.
		_ = got
	}

	got := windowRun(t, op2, []types.Record{
		{Key: []byte("k1"), Value: []byte("late"), Timestamp: time.Unix(12, 0)}, // < wm 15 → dropped
		{Key: []byte("k1"), Value: []byte("ok"), Timestamp: time.Unix(20, 0)},   // >= wm → kept
		types.NewWatermark(time.Unix(26, 0)),                                    // fires [20,25)
	})
	for _, r := range got {
		if string(r.Value) == "late" {
			t.Error("late record below restored watermark was not dropped — watermark did not survive native checkpoint")
		}
	}
	if len(got) != 1 || string(got[0].Value) != "ok" {
		t.Fatalf("expected only the on-time record, got %d records", len(got))
	}
}
