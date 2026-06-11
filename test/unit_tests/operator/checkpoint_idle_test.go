package operator_test

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"mailer/operator"
	"mailer/types"
	"mailer/window"
)

// --- Barrier propagation tests ---

func TestMapOperator_PassesThroughBarriers(t *testing.T) {
	op := operator.Map(func(r types.Record) types.Record { return r })

	in := make(chan types.Record, 5)
	out := make(chan types.Record, 5)

	go op.Process(in, out)

	b := types.NewBarrier("cp-1")
	in <- b
	close(in)

	r := <-out
	if !r.IsBarrier {
		t.Error("barrier should pass through Map")
	}
	if r.CheckpointID != "cp-1" {
		t.Errorf("CheckpointID: got %q, want %q", r.CheckpointID, "cp-1")
	}
}

func TestFilterOperator_PassesThroughBarriers(t *testing.T) {
	op := operator.Filter(func(r types.Record) bool { return false })

	in := make(chan types.Record, 5)
	out := make(chan types.Record, 5)

	go op.Process(in, out)

	b := types.NewBarrier("cp-1")
	in <- b
	close(in)

	r := <-out
	if !r.IsBarrier {
		t.Error("barrier should pass through Filter even if predicate is false")
	}
}

func TestReduceOperator_BarrierPassesThrough(t *testing.T) {
	op := operator.Reduce(func(accum []byte, curr types.Record) []byte { return curr.Value })

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1")}
	in <- types.NewBarrier("cp-1")
	in <- types.Record{Key: []byte("k1"), Value: []byte("v2")}
	close(in)

	var gotBarrier bool
	var dataCount int
	for r := range out {
		if r.IsBarrier {
			gotBarrier = true
			if r.CheckpointID != "cp-1" {
				t.Errorf("CheckpointID: got %q, want %q", r.CheckpointID, "cp-1")
			}
		} else {
			dataCount++
		}
	}
	if !gotBarrier {
		t.Error("expected barrier to pass through Reduce")
	}
	if dataCount != 2 {
		t.Errorf("expected 2 data records, got %d", dataCount)
	}
}

func TestWindowOperator_BarrierPassesThrough(t *testing.T) {
	op := operator.Window(window.NewTumbling(5 * time.Second))

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(2, 0)}
	in <- types.NewBarrier("cp-1")
	in <- types.Record{Key: []byte("k1"), Value: []byte("v2"), Timestamp: time.Unix(3, 0)}
	in <- types.NewWatermark(time.Unix(6, 0))
	close(in)

	var gotBarrier bool
	var dataCount int
	for r := range out {
		if r.IsBarrier {
			gotBarrier = true
		} else if !r.IsWatermark {
			dataCount++
		}
	}
	if !gotBarrier {
		t.Error("expected barrier to pass through Window")
	}
	if dataCount != 2 {
		t.Errorf("expected 2 data records, got %d", dataCount)
	}
}

// --- Snapshot/Restore tests ---

func TestReduceOperator_SnapshotRestore(t *testing.T) {
	countFn := func(accum []byte, curr types.Record) []byte {
		prev := 0
		if accum != nil {
			prev = int(binary.BigEndian.Uint64(accum))
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(prev+1))
		return buf
	}

	op1 := operator.Reduce(countFn)

	// Process some records to build state.
	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)
	go op1.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("a")}
	in <- types.Record{Key: []byte("k2"), Value: []byte("b")}
	in <- types.Record{Key: []byte("k1"), Value: []byte("c")}
	// Drain output.
	<-out
	<-out
	<-out
	close(in)
	for range out {
	}

	// Snapshot state.
	snap, err := op1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Create a new operator and restore state.
	op2 := operator.Reduce(countFn)
	if err := op2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Process another record on the restored operator.
	in2 := make(chan types.Record, 10)
	out2 := make(chan types.Record, 10)
	go op2.Process(in2, out2)

	in2 <- types.Record{Key: []byte("k1"), Value: []byte("d")}
	close(in2)

	r := <-out2
	if got := binary.BigEndian.Uint64(r.Value); got != 3 {
		t.Errorf("restored reduce: k1 should be count 3, got %d", got)
	}
}

func TestWindowOperator_SnapshotRestore(t *testing.T) {
	assigner := window.NewTumbling(5 * time.Second)

	// Construct a snapshot JSON representing:
	// - current watermark at ts=3
	// - one window [0, 5s) for key "k1" with 2 buffered records
	// We build it with json.Marshal since the operator types are unexported.
	winStart := time.Unix(0, 0).UTC()
	winEnd := time.Unix(5, 0).UTC()
	keyStr := "k1/" + winStart.Format(time.RFC3339Nano) + "/" + winEnd.Format(time.RFC3339Nano)

	snap := map[string]interface{}{
		"current_watermark": time.Unix(3, 0).UnixNano(),
		"windows": map[string]interface{}{
			keyStr: map[string]interface{}{
				"win": map[string]interface{}{
					"Start": winStart.Format(time.RFC3339Nano),
					"End":   winEnd.Format(time.RFC3339Nano),
				},
				"records": []interface{}{
					map[string]interface{}{
						"key":       base64.StdEncoding.EncodeToString([]byte("k1")),
						"value":     base64.StdEncoding.EncodeToString([]byte("v1")),
						"timestamp": time.Unix(2, 0).UnixNano(),
						"offset":    0,
					},
					map[string]interface{}{
						"key":       base64.StdEncoding.EncodeToString([]byte("k1")),
						"value":     base64.StdEncoding.EncodeToString([]byte("v2")),
						"timestamp": time.Unix(3, 0).UnixNano(),
						"offset":    1,
					},
				},
			},
		},
	}
	snapData, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	op := operator.Window(assigner)
	if err := op.Restore(snapData); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Verify restored watermark.
	if !op.CurrentWatermark().Equal(time.Unix(3, 0)) {
		t.Errorf("restored watermark: got %v, want %v", op.CurrentWatermark(), time.Unix(3, 0))
	}

	// Process on the restored operator: send watermark past window end to fire it.
	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)
	go op.Process(in, out)

	in <- types.NewWatermark(time.Unix(6, 0))
	close(in)

	dataCount := 0
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier {
			dataCount++
		}
	}
	if dataCount != 2 {
		t.Errorf("restored window should emit 2 records, got %d", dataCount)
	}
}

// --- IdleTimeout test ---

func TestWindowOperator_IdleTimeout(t *testing.T) {
	op := operator.Window(window.NewTumbling(5 * time.Second)).WithIdleTimeout(1 * time.Second)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(2, 0)}
	close(in)

	// With idle timeout, remaining windows should fire after timeout.
	// But since we closed the input, flushRemaining is called immediately.
	var dataCount int
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier {
			dataCount++
		}
	}
	if dataCount != 1 {
		t.Errorf("expected 1 record from remaining window, got %d", dataCount)
	}
}