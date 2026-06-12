package operator_test

import (
	"testing"
	"time"

	"mailer/operator"
	"mailer/types"
	"mailer/window"
)

func TestWindowOperator_SlidingWindow_RecordInMultipleWindows(t *testing.T) {
	// Size=10s, slide=5s. A record at ts=7s should be in:
	//   [0,10), [5,15)
	assigner := window.NewSliding(10*time.Second, 5*time.Second)
	op := operator.Window(assigner)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	// Send a single record at ts=7s.
	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(7, 0)}
	// Advance watermark past window ends to fire them.
	in <- types.NewWatermark(time.Unix(16, 0))
	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier {
			results = append(results, r)
		}
	}

	// The record should appear in 2 windows: [0,10) and [5,15).
	if len(results) != 2 {
		t.Fatalf("expected 2 windowed records (one per overlapping window), got %d", len(results))
	}

	windows := map[string]bool{}
	for _, r := range results {
		ws := string(r.Headers["window_start"])
		we := string(r.Headers["window_end"])
		windows[ws+"-"+we] = true
	}

	want0 := "1970-01-01T00:00:00Z-1970-01-01T00:00:10Z"
	want5 := "1970-01-01T00:00:05Z-1970-01-01T00:00:15Z"
	if !windows[want0] {
		t.Errorf("missing window [0,10), got windows: %v", windows)
	}
	if !windows[want5] {
		t.Errorf("missing window [5,15), got windows: %v", windows)
	}
}

func TestWindowOperator_SlidingWindow_SlideEqualsSize(t *testing.T) {
	// When slide == size, sliding behaves like tumbling: each record in exactly 1 window.
	assigner := window.NewSliding(5*time.Second, 5*time.Second)
	op := operator.Window(assigner)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(3, 0)}
	in <- types.NewWatermark(time.Unix(6, 0))
	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier {
			results = append(results, r)
		}
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 record (slide==size), got %d", len(results))
	}

	ws := string(results[0].Headers["window_start"])
	we := string(results[0].Headers["window_end"])
	if ws != "1970-01-01T00:00:00Z" || we != "1970-01-01T00:00:05Z" {
		t.Errorf("expected window [0,5), got [%s, %s)", ws, we)
	}
}

func TestWindowOperator_SlidingWindow_MultipleRecordsSameWindow(t *testing.T) {
	// Two records in the same overlapping windows.
	assigner := window.NewSliding(10*time.Second, 5*time.Second)
	op := operator.Window(assigner)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	// Both records at ts=3s fall in windows [0,10) and [5,15).
	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(3, 0)}
	in <- types.Record{Key: []byte("k1"), Value: []byte("v2"), Timestamp: time.Unix(3, 0)}
	// Fire both windows.
	in <- types.NewWatermark(time.Unix(16, 0))
	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier {
			results = append(results, r)
		}
	}

	// 2 records × 2 windows = 4 output records.
	if len(results) != 4 {
		t.Fatalf("expected 4 windowed records (2 records × 2 windows), got %d", len(results))
	}
}

func TestWindowOperator_SlidingWindow_DropsLateRecords(t *testing.T) {
	assigner := window.NewSliding(10*time.Second, 5*time.Second)
	op := operator.Window(assigner)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	// Advance watermark to ts=15.
	in <- types.NewWatermark(time.Unix(15, 0))
	// This record at ts=3 is late (3 < 15), should be dropped.
	in <- types.Record{Key: []byte("k1"), Value: []byte("late"), Timestamp: time.Unix(3, 0)}
	close(in)

	var lateCount int
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier && string(r.Value) == "late" {
			lateCount++
		}
	}
	if lateCount != 0 {
		t.Errorf("late record should have been dropped, got %d", lateCount)
	}
}

func TestWindowOperator_SessionWindow_BasicSession(t *testing.T) {
	// Session window with 30s gap.
	// Records at ts=0 and ts=10 should be in the same session (gap < 30s).
	assigner := window.NewSession(30 * time.Second)
	op := operator.Window(assigner)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(0, 0).UTC()}
	in <- types.Record{Key: []byte("k1"), Value: []byte("v2"), Timestamp: time.Unix(10, 0).UTC()}
	in <- types.NewWatermark(time.Unix(45, 0).UTC())
	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier {
			results = append(results, r)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 records in session, got %d", len(results))
	}

	// Both records should have the same session window.
	ws1 := string(results[0].Headers["window_start"])
	we1 := string(results[0].Headers["window_end"])
	ws2 := string(results[1].Headers["window_start"])
	we2 := string(results[1].Headers["window_end"])

	if ws1 != ws2 || we1 != we2 {
		t.Errorf("both records should be in the same session: [%s,%s) vs [%s,%s)", ws1, we1, ws2, we2)
	}
}

func TestWindowOperator_SessionWindow_TwoSeparateSessions(t *testing.T) {
	// Two records far apart should create separate sessions.
	assigner := window.NewSession(5 * time.Second)
	op := operator.Window(assigner)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	// First session: ts=0 → session [0, 5)
	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(0, 0)}
	// Second session: ts=20 → session [20, 25), far apart (gap=5s)
	in <- types.Record{Key: []byte("k1"), Value: []byte("v2"), Timestamp: time.Unix(20, 0)}
	// Advance watermark past both sessions.
	in <- types.NewWatermark(time.Unix(30, 0))
	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier {
			results = append(results, r)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 records (one per session), got %d", len(results))
	}

	// The two records should have DIFFERENT session windows.
	ws1 := string(results[0].Headers["window_start"])
	we1 := string(results[0].Headers["window_end"])
	ws2 := string(results[1].Headers["window_start"])
	we2 := string(results[1].Headers["window_end"])

	if ws1 == ws2 && we1 == we2 {
		t.Errorf("records should be in separate sessions, but both in [%s, %s)", ws1, we1)
	}
}

func TestWindowOperator_SessionWindow_MergesExpandingGap(t *testing.T) {
	// Record at ts=0 creates session [0, 30).
	// Record at ts=25 should expand the session to [0, 55) (25 + 30 = 55).
	assigner := window.NewSession(30 * time.Second)
	op := operator.Window(assigner)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(0, 0).UTC()}
	in <- types.Record{Key: []byte("k1"), Value: []byte("v2"), Timestamp: time.Unix(25, 0).UTC()}
	in <- types.NewWatermark(time.Unix(60, 0).UTC())
	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier {
			results = append(results, r)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 records in merged session, got %d", len(results))
	}

	// Both should be in the same (expanded) session.
	ws1 := string(results[0].Headers["window_start"])
	ws2 := string(results[1].Headers["window_start"])

	if ws1 != ws2 {
		t.Errorf("both records should be in the same session, got [%s] and [%s]", ws1, ws2)
	}

	// The merged session should start at ts=0.
	if ws1 != "1970-01-01T00:00:00Z" {
		t.Errorf("session start: got %s, want 1970-01-01T00:00:00Z", ws1)
	}
}

func TestWindowOperator_SessionWindow_DropsLateRecords(t *testing.T) {
	assigner := window.NewSession(10 * time.Second)
	op := operator.Window(assigner)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	// Advance watermark first.
	in <- types.NewWatermark(time.Unix(100, 0))
	// This record at ts=5 is late (5 < 100).
	in <- types.Record{Key: []byte("k1"), Value: []byte("late"), Timestamp: time.Unix(5, 0)}
	close(in)

	var lateCount int
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier && string(r.Value) == "late" {
			lateCount++
		}
	}
	if lateCount != 0 {
		t.Errorf("late record should be dropped, got %d", lateCount)
	}
}

func TestWindowOperator_SlidingWindow_FlushOnClose(t *testing.T) {
	assigner := window.NewSliding(10*time.Second, 5*time.Second)
	op := operator.Window(assigner)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	// Send records but don't send a watermark to fire windows.
	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(3, 0)}
	close(in)

	// flushRemaining should emit all buffered records.
	var results []types.Record
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier {
			results = append(results, r)
		}
	}

	if len(results) < 1 {
		t.Errorf("expected at least 1 record from flushRemaining, got %d", len(results))
	}
}

func TestWindowOperator_SessionWindow_FlushOnClose(t *testing.T) {
	assigner := window.NewSession(10 * time.Second)
	op := operator.Window(assigner)

	in := make(chan types.Record, 20)
	out := make(chan types.Record, 20)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Unix(5, 0)}
	close(in)

	var results []types.Record
	for r := range out {
		if !r.IsWatermark && !r.IsBarrier {
			results = append(results, r)
		}
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 record from flushRemaining, got %d", len(results))
	}

	// Check the session got window headers.
	if _, ok := results[0].Headers["window_start"]; !ok {
		t.Error("expected window_start header on flushed record")
	}
}
