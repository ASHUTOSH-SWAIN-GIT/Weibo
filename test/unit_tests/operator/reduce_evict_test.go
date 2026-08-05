package operator_test

import (
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// sumFn folds 8-byte big-endian counters.
func sumFn(accum []byte, curr types.Record) []byte {
	var total uint64
	if len(accum) == 8 {
		total = binary.BigEndian.Uint64(accum)
	}
	if len(curr.Value) == 8 {
		total += binary.BigEndian.Uint64(curr.Value)
	}
	return u64(total)
}

// windowed returns a record tagged with window bounds exactly as the
// WindowOperator tags records it fires.
func windowed(key string, val uint64, start, end time.Time) types.Record {
	return types.Record{
		Key:       []byte(key),
		Value:     u64(val),
		Timestamp: start,
		Headers: map[string][]byte{
			"window_start": []byte(start.Format(time.RFC3339Nano)),
			"window_end":   []byte(end.Format(time.RFC3339Nano)),
		},
	}
}

// runReduce feeds records through a ReduceOperator and returns the emitted
// records once the operator has drained.
func runReduce(op *operator.ReduceOperator, records []types.Record) []types.Record {
	in := make(chan types.Record, len(records))
	out := make(chan types.Record, len(records))
	for _, r := range records {
		in <- r
	}
	close(in)

	done := make(chan []types.Record, 1)
	go func() {
		var got []types.Record
		for r := range out {
			got = append(got, r)
		}
		done <- got
	}()
	op.Process(in, out)
	return <-done
}

func stateKeys(op *operator.ReduceOperator) []string {
	keys := op.Backend().ValueState("reduce").Keys()
	sort.Strings(keys)
	return keys
}

// TestReduce_EvictsClosedWindowState verifies that per-(key, window) reduce
// state is dropped once the window-close frontier moves past that window.
// Without eviction every distinct window leaves a dead accumulator behind
// forever, growing memory and every checkpoint without bound. Covers #7.
func TestReduce_EvictsClosedWindowState(t *testing.T) {
	base := time.Unix(0, 0).UTC()
	w1s, w1e := base, base.Add(10*time.Second)
	w2s, w2e := w1e, w1e.Add(10*time.Second)
	w3s, w3e := w2e, w2e.Add(10*time.Second)

	op := operator.Reduce(sumFn)
	got := runReduce(op, []types.Record{
		windowed("k1", 1, w1s, w1e),
		windowed("k2", 2, w1s, w1e),
		windowed("k1", 3, w2s, w2e),
		windowed("k2", 4, w2s, w2e),
		windowed("k1", 5, w3s, w3e),
	})

	if len(got) != 5 {
		t.Fatalf("expected 5 emitted records, got %d", len(got))
	}
	// Per-window isolation must survive eviction: k1's window-2 accumulator
	// starts fresh at 3 rather than continuing window-1's 1.
	if v := binary.BigEndian.Uint64(got[2].Value); v != 3 {
		t.Errorf("k1 window-2 accumulator = %d, want 3 (window state leaked across windows)", v)
	}

	// Frontier is w3e. Windows 1 and 2 closed below it and must be gone;
	// only the open (frontier) window's entry remains.
	keys := stateKeys(op)
	if len(keys) != 1 {
		t.Fatalf("expected 1 surviving state entry, got %d: %v", len(keys), keys)
	}
	wantKey := "k1/" + w3s.Format(time.RFC3339Nano) + "/" + w3e.Format(time.RFC3339Nano)
	if keys[0] != wantKey {
		t.Errorf("surviving key = %q, want %q", keys[0], wantKey)
	}
}

// TestReduce_NonWindowedStateNeverEvicted guards the streaming (non-windowed)
// Reduce contract: its per-key accumulator is the running total for the life
// of the job and must never be swept.
func TestReduce_NonWindowedStateNeverEvicted(t *testing.T) {
	op := operator.Reduce(sumFn)
	got := runReduce(op, []types.Record{
		{Key: []byte("k1"), Value: u64(1)},
		{Key: []byte("k2"), Value: u64(2)},
		{Key: []byte("k1"), Value: u64(3)},
	})

	if v := binary.BigEndian.Uint64(got[2].Value); v != 4 {
		t.Errorf("k1 running total = %d, want 4", v)
	}
	if keys := stateKeys(op); len(keys) != 2 {
		t.Errorf("expected 2 state entries, got %d: %v", len(keys), keys)
	}
}

// TestReduce_SlashyKeyNotMistakenForWindow guards the eviction key parser: a
// plain record key containing slashes must not be read as window bounds and
// swept away. Only keys whose last two segments are both RFC3339Nano
// timestamps are windows.
func TestReduce_SlashyKeyNotMistakenForWindow(t *testing.T) {
	base := time.Unix(0, 0).UTC()
	w1s, w1e := base, base.Add(10*time.Second)
	w2s, w2e := w1e, w1e.Add(10*time.Second)

	op := operator.Reduce(sumFn)
	runReduce(op, []types.Record{
		// A non-windowed record whose key looks structurally similar.
		{Key: []byte("tenant/a/b"), Value: u64(7)},
		windowed("k1", 1, w1s, w1e),
		windowed("k1", 2, w2s, w2e), // advances the frontier, triggers a sweep
	})

	keys := stateKeys(op)
	found := false
	for _, k := range keys {
		if k == "tenant/a/b" {
			found = true
		}
	}
	if !found {
		t.Errorf("non-windowed slashy key was evicted; keys = %v", keys)
	}
}
