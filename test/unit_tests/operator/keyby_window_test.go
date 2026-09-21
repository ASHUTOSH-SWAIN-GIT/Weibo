package operator_test

import (
	"context"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/window"
)

func TestKeyBy_Router_Deterministic(t *testing.T) {
	kb := operator.KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(16)
	r := types.Record{Key: []byte("test-key")}
	p1 := kb.Route(r)
	p2 := kb.Route(r)
	if p1 != p2 {
		t.Errorf("same key/record should always map to same worker: %d != %d", p1, p2)
	}
}

func TestKeyBy_Router_DifferentKeysDifferentWorkers(t *testing.T) {
	kb := operator.KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(16)
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	workers := make(map[int]bool)
	for _, k := range keys {
		workers[kb.Route(types.Record{Key: k})] = true
	}
	if len(workers) < 2 {
		t.Errorf("expected keys to spread across at least 2 workers, got %d", len(workers))
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

type captureLateSink struct {
	records []types.Record
}

func (s *captureLateSink) Write(_ context.Context, r types.Record) error {
	s.records = append(s.records, r)
	return nil
}

func TestWindowOperator_AllowedLatenessAcceptsAndSideOutputsTooLateRecords(t *testing.T) {
	late := &captureLateSink{}
	op := operator.Window(window.NewTumbling(5 * time.Second)).
		WithAllowedLateness(5 * time.Second).
		WithLateSink(late)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)
	go op.Process(in, out)

	in <- types.NewWatermark(time.Unix(15, 0))
	// Within allowed lateness: 12 >= 15-5, accepted into [10,15).
	in <- types.Record{Key: []byte("k1"), Value: []byte("accepted-late"), Timestamp: time.Unix(12, 0)}
	// Beyond allowed lateness: 8 < 15-5, side-output/drop.
	in <- types.Record{Key: []byte("k1"), Value: []byte("too-late"), Timestamp: time.Unix(8, 0)}
	in <- types.NewWatermark(time.Unix(20, 0))
	close(in)

	var accepted int
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier && string(r.Value) == "accepted-late" {
			accepted++
			if string(r.Headers["window_allowed_lateness_nanos"]) == "" {
				t.Fatal("window output missing allowed-lateness metadata")
			}
		}
	}
	if accepted != 1 {
		t.Fatalf("expected accepted late record to fire once, got %d", accepted)
	}
	if len(late.records) != 1 || string(late.records[0].Value) != "too-late" {
		t.Fatalf("expected too-late record in side output, got %+v", late.records)
	}
	if string(late.records[0].Headers["_late_reason"]) == "" {
		t.Fatal("late side-output record missing reason header")
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
