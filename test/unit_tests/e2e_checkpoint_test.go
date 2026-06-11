package mailer_test

import (
	"encoding/binary"
	"testing"
	"time"

	"mailer/checkpoint"
	"mailer/operator"
	"mailer/types"
	"mailer/window"
)

// countReduceFn is a simple reduce function that counts records per key.
var countReduceFn = func(accum []byte, curr types.Record) []byte {
	prev := 0
	if accum != nil {
		prev = int(binary.BigEndian.Uint64(accum))
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(prev+1))
	return buf
}

// TestE2E_PipelineWithCheckpoint_Recovery tests that a pipeline can be
// checkpointed and restored, and the restored state produces correct results.
func TestE2E_PipelineWithCheckpoint_Recovery(t *testing.T) {
	// Phase 1: Create a Reduce operator, process some data, and manually
	// save its state via the checkpoint system.
	//
	// Phase 2: Create a fresh Reduce operator, restore its state from the
	// checkpoint, and verify it continues computing correctly.

	dir := t.TempDir()
	storage := checkpoint.NewFileStorage(dir)

	// Phase 1: Build and exercise a Reduce operator.
	reduceOp := operator.Reduce(countReduceFn)

	in1 := make(chan types.Record, 10)
	out1 := make(chan types.Record, 10)
	go reduceOp.Process(in1, out1)

	// Process 3 records for key "k1".
	in1 <- types.Record{Key: []byte("k1"), Value: []byte("a")}
	<-out1
	in1 <- types.Record{Key: []byte("k2"), Value: []byte("b")}
	<-out1
	in1 <- types.Record{Key: []byte("k1"), Value: []byte("c")}
	<-out1

	// Close and drain.
	close(in1)
	for range out1 {
	}

	// Snapshot the operator's state.
	snapData, err := reduceOp.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Save to checkpoint storage.
	cp := &checkpoint.CheckpointData{
		ID:        "cp-1",
		Timestamp: time.Now().UTC(),
		Operators: map[string][]byte{
			"op-0": snapData,
		},
	}
	if err := storage.Save(cp); err != nil {
		t.Fatalf("Save checkpoint: %v", err)
	}

	// Phase 2: Create a fresh Reduce operator, restore from checkpoint,
	// and verify state was recovered.
	reduceOp2 := operator.Reduce(countReduceFn)

	loaded, err := storage.Load()
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint data, got nil")
	}

	if err := reduceOp2.Restore(loaded.Operators["op-0"]); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Process another record for key "k1" — count should continue from 2 (not start at 1).
	in2 := make(chan types.Record, 10)
	out2 := make(chan types.Record, 10)
	go reduceOp2.Process(in2, out2)

	in2 <- types.Record{Key: []byte("k1"), Value: []byte("d")}
	close(in2)

	var results []types.Record
	for r := range out2 {
		if !r.IsWatermark && !r.IsBarrier {
			results = append(results, r)
		}
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// The count for "k1" should be 3 (was 2 before checkpoint, now 2+1=3).
	got := binary.BigEndian.Uint64(results[0].Value)
	if got != 3 {
		t.Errorf("restored reduce: k1 count should be 3 (2 restored + 1 new), got %d", got)
	}
}

// TestE2E_PipelineWithCheckpoint_FullRun tests that Reduce state can be
// checkpointed, saved to FileStorage, loaded, restored, and processing continues.
func TestE2E_PipelineWithCheckpoint_FullRun(t *testing.T) {
	dir := t.TempDir()
	storage := checkpoint.NewFileStorage(dir)

	records := []types.Record{
		{Key: []byte("k1"), Value: []byte("a")},
		{Key: []byte("k1"), Value: []byte("b")},
		{Key: []byte("k2"), Value: []byte("c")},
	}

	reduceOp := operator.Reduce(countReduceFn)

	in1 := make(chan types.Record, 10)
	out1 := make(chan types.Record, 10)
	go reduceOp.Process(in1, out1)

	for _, r := range records {
		in1 <- r
	}
	close(in1)
	for range out1 {
	}

	snap, err := reduceOp.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	cp := &checkpoint.CheckpointData{
		ID:        "cp-1",
		Timestamp: time.Now().UTC(),
		Operators: map[string][]byte{"op-0": snap},
	}
	if err := storage.Save(cp); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reduceOp2 := operator.Reduce(countReduceFn)
	loaded, err := storage.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := reduceOp2.Restore(loaded.Operators["op-0"]); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Send one more record for k1 — count should be 3 (2 restored + 1 new).
	in2 := make(chan types.Record, 10)
	out2 := make(chan types.Record, 10)
	go reduceOp2.Process(in2, out2)

	in2 <- types.Record{Key: []byte("k1"), Value: []byte("d")}
	close(in2)

	for r := range out2 {
		if string(r.Key) == "k1" {
			got := binary.BigEndian.Uint64(r.Value)
			if got != 3 {
				t.Errorf("k1 count: got %d, want 3", got)
			}
		}
	}
}

// TestE2E_PipelineWithCheckpoint_WindowRestore tests that window state can be
// checkpointed and restored, and the restored operator continues processing correctly.
func TestE2E_PipelineWithCheckpoint_WindowRestore(t *testing.T) {
	winOp := operator.Window(window.NewTumbling(5 * time.Second))

	in1 := make(chan types.Record, 20)
	out1 := make(chan types.Record, 20)
	go winOp.Process(in1, out1)

	// Send 2 records in window [0, 5) for key "k1".
	in1 <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(2, 0)}
	in1 <- types.Record{Key: []byte("k1"), Value: []byte("v2"), Timestamp: time.Unix(3, 0)}
	// Send a watermark past the window to fire it.
	in1 <- types.NewWatermark(time.Unix(6, 0))
	// Drain the fired records.
	var phase1Count int
	for r := range out1 {
		if !r.IsWatermark && !r.IsBarrier {
			phase1Count++
		}
		// Stop after we've read 2 data records (the windowed output).
		if phase1Count >= 2 {
			break
		}
	}
	close(in1)
	for range out1 {
	}

	if phase1Count != 2 {
		t.Fatalf("phase 1: expected 2 windowed records, got %d", phase1Count)
	}

	// The window state is now empty (fired). Snapshot should still capture watermark.
	snap, err := winOp.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Phase 2: Restore and send new records into a new window.
	winOp2 := operator.Window(window.NewTumbling(5 * time.Second))
	if err := winOp2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Verify watermark was restored.
	if !winOp2.CurrentWatermark().Equal(time.Unix(6, 0)) {
		t.Errorf("restored watermark: got %v, want %v", winOp2.CurrentWatermark(), time.Unix(6, 0))
	}

	// The restored watermark (ts=6) should cause late records (ts < 6) to be dropped,
	// and records at ts >= 6 to be accepted.
	in2 := make(chan types.Record, 20)
	out2 := make(chan types.Record, 20)
	go winOp2.Process(in2, out2)

	// Send a late record (ts=4 < wm=6) — should be dropped.
	// Send a valid record (ts=8 > wm=6) — should be accepted.
	in2 <- types.Record{Key: []byte("k1"), Value: []byte("late"), Timestamp: time.Unix(4, 0)}
	in2 <- types.Record{Key: []byte("k1"), Value: []byte("valid"), Timestamp: time.Unix(8, 0)}
	// Fire the window [5, 10).
	in2 <- types.NewWatermark(time.Unix(11, 0))
	close(in2)

	var validCount int
	var lateCount int
	for r := range out2 {
		if r.IsWatermark || r.IsBarrier {
			continue
		}
		if string(r.Value) == "late" {
			lateCount++
		} else {
			validCount++
		}
	}

	if lateCount != 0 {
		t.Errorf("late record should have been dropped, got %d late records", lateCount)
	}
	if validCount != 1 {
		t.Errorf("expected 1 valid record in window, got %d", validCount)
	}
}
