package operator_test

import (
	"testing"
	"time"

	"mailer/operator"
	"mailer/types"
	"mailer/window"
)

func TestKeyBy_SetsKeyAndRoutes(t *testing.T) {
	op := operator.KeyBy(func(r types.Record) []byte {
		return r.Key
	}).WithPartitions(4)

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	records := []types.Record{
		{Key: []byte("alice"), Value: []byte("v1")},
		{Key: []byte("bob"), Value: []byte("v2")},
		{Key: []byte("alice"), Value: []byte("v3")},
	}
	for _, r := range records {
		in <- r
	}
	close(in)

	var results []types.Record
	for r := range out {
		results = append(results, r)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	aliceKeys := 0
	bobKeys := 0
	for _, r := range results {
		if string(r.Key) == "alice" {
			aliceKeys++
		} else if string(r.Key) == "bob" {
			bobKeys++
		}
	}
	if aliceKeys != 2 {
		t.Errorf("alice keys: got %d, want 2", aliceKeys)
	}
	if bobKeys != 1 {
		t.Errorf("bob keys: got %d, want 1", bobKeys)
	}
}

func TestKeyBy_ForwardsWatermark_AfterDataDrains(t *testing.T) {
	op := operator.KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(2)

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("a"), Value: []byte("data")}
	in <- types.NewWatermark(time.Unix(100, 0))
	in <- types.Record{Key: []byte("b"), Value: []byte("data2")}
	in <- types.NewWatermark(time.Unix(200, 0))
	close(in)

	var gotWatermark time.Time
	var dataCount int
	for r := range out {
		if r.IsWatermark {
			if r.Timestamp.After(gotWatermark) {
				gotWatermark = r.Timestamp
			}
		} else {
			dataCount++
		}
	}

	if dataCount != 2 {
		t.Errorf("data records: got %d, want 2", dataCount)
	}
	if !gotWatermark.Equal(time.Unix(200, 0)) {
		t.Errorf("watermark: got %v, want %v", gotWatermark, time.Unix(200, 0))
	}
}

func TestKeyBy_OnlyWatermark(t *testing.T) {
	op := operator.KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(2)

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	wm := time.Unix(50, 0)
	in <- types.NewWatermark(wm)
	close(in)

	var gotWatermark time.Time
	for r := range out {
		if r.IsWatermark {
			gotWatermark = r.Timestamp
		}
	}
	if !gotWatermark.Equal(wm) {
		t.Errorf("watermark: got %v, want %v", gotWatermark, wm)
	}
}

func TestPartition_EmptyKey(t *testing.T) {
	idx := operator.Partition([]byte{}, 16)
	if idx != 0 {
		t.Errorf("empty key should go to partition 0, got %d", idx)
	}
}

func TestPartition_Deterministic(t *testing.T) {
	key := []byte("test-key")
	p1 := operator.Partition(key, 16)
	p2 := operator.Partition(key, 16)
	if p1 != p2 {
		t.Errorf("same key should always map to same partition: %d != %d", p1, p2)
	}
}

func TestPartition_DifferentKeysDifferentPartitions(t *testing.T) {
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	partitions := make(map[int]bool)
	for _, k := range keys {
		partitions[operator.Partition(k, 16)] = true
	}
	if len(partitions) < 2 {
		t.Errorf("expected keys to spread across at least 2 partitions, got %d", len(partitions))
	}
}

func TestPartition_SinglePartition(t *testing.T) {
	idx := operator.Partition([]byte("any-key"), 1)
	if idx != 0 {
		t.Errorf("single partition should always return 0, got %d", idx)
	}
}

func TestWindowOperator_TumblingWindow_FiresOnWatermark(t *testing.T) {
	op := operator.Window(window.NewTumbling(5 * time.Second))

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	ts1 := time.Unix(2, 0)
	ts2 := time.Unix(3, 0)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: ts1}
	in <- types.Record{Key: []byte("k1"), Value: []byte("v2"), Timestamp: ts2}

	wm := time.Unix(6, 0)
	in <- types.NewWatermark(wm)
	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark {
			results = append(results, r)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 windowed records, got %d", len(results))
	}

	for _, r := range results {
		ws, hasWS := r.Headers["window_start"]
		we, hasWE := r.Headers["window_end"]
		if !hasWS || !hasWE {
			t.Errorf("missing window headers on record")
			continue
		}
		winStart, _ := time.Parse(time.RFC3339Nano, string(ws))
		winEnd, _ := time.Parse(time.RFC3339Nano, string(we))
		wantStart := time.Unix(0, 0).UTC()
		wantEnd := time.Unix(5, 0).UTC()
		if !winStart.Equal(wantStart) {
			t.Errorf("window_start: got %v, want %v", winStart, wantStart)
		}
		if !winEnd.Equal(wantEnd) {
			t.Errorf("window_end: got %v, want %v", winEnd, wantEnd)
		}
	}
}

func TestWindowOperator_DropsLateRecords(t *testing.T) {
	op := operator.Window(window.NewTumbling(5 * time.Second))

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(10, 0)}
	in <- types.NewWatermark(time.Unix(15, 0))

	in <- types.Record{Key: []byte("k1"), Value: []byte("late"), Timestamp: time.Unix(8, 0)}

	close(in)

	var lateCount int
	for r := range out {
		if !r.IsWatermark && string(r.Value) == "late" {
			lateCount++
		}
	}
	if lateCount != 0 {
		t.Errorf("late record should have been dropped, but got %d", lateCount)
	}
}

func TestWindowOperator_FiresRemainingWindowsOnClose(t *testing.T) {
	op := operator.Window(window.NewTumbling(10 * time.Second))

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(12, 0)}
	in <- types.Record{Key: []byte("k1"), Value: []byte("v2"), Timestamp: time.Unix(14, 0)}

	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark {
			results = append(results, r)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 records from remaining windows, got %d", len(results))
	}
	for _, r := range results {
		if _, ok := r.Headers["window_start"]; !ok {
			t.Error("expected window_start header")
		}
	}
}

func TestWindowOperator_SeparateKeysGetSeparateWindows(t *testing.T) {
	op := operator.Window(window.NewTumbling(5 * time.Second))

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("alice"), Value: []byte("v1"), Timestamp: time.Unix(1, 0)}
	in <- types.Record{Key: []byte("bob"), Value: []byte("v2"), Timestamp: time.Unix(2, 0)}
	in <- types.NewWatermark(time.Unix(6, 0))

	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark {
			results = append(results, r)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 records, got %d", len(results))
	}

	keys := map[string]bool{}
	for _, r := range results {
		keys[string(r.Key)] = true
	}
	if !keys["alice"] || !keys["bob"] {
		t.Errorf("expected both alice and bob in output, got keys: %v", keys)
	}
}

func TestWindowOperator_WeakWatermarkDoesNotFireWindow(t *testing.T) {
	op := operator.Window(window.NewTumbling(5 * time.Second))

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(2, 0)}
	in <- types.NewWatermark(time.Unix(3, 0))
	in <- types.Record{Key: []byte("k1"), Value: []byte("v2"), Timestamp: time.Unix(4, 0)}
	in <- types.NewWatermark(time.Unix(6, 0))
	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark {
			results = append(results, r)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 records, got %d", len(results))
	}
	for _, r := range results {
		ws := string(r.Headers["window_start"])
		we := string(r.Headers["window_end"])
		if ws != "1970-01-01T00:00:00Z" || we != "1970-01-01T00:00:05Z" {
			t.Errorf("expected window [0,5), got [%s, %s)", ws, we)
		}
	}
}
