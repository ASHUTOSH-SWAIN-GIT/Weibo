package window_test

import (
	"testing"
	"time"

	"mailer/window"
)

func TestTumbling_AssignWindows_Basic(t *testing.T) {
	w := window.NewTumbling(5 * time.Minute)
	ts := time.Date(2026, 1, 1, 0, 7, 30, 0, time.UTC)
	wins := w.AssignWindows(ts)
	if len(wins) != 1 {
		t.Fatalf("expected 1 window, got %d", len(wins))
	}
	wantStart := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	if !wins[0].Start.Equal(wantStart) {
		t.Errorf("Start: got %v, want %v", wins[0].Start, wantStart)
	}
	if !wins[0].End.Equal(wantEnd) {
		t.Errorf("End: got %v, want %v", wins[0].End, wantEnd)
	}
}

func TestTumbling_AssignWindows_ExactlyOnBoundary(t *testing.T) {
	w := window.NewTumbling(5 * time.Minute)
	ts := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	wins := w.AssignWindows(ts)
	if len(wins) != 1 {
		t.Fatalf("expected 1 window, got %d", len(wins))
	}
	wantStart := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	if !wins[0].Start.Equal(wantStart) {
		t.Errorf("Start: got %v, want %v", wins[0].Start, wantStart)
	}
}

func TestTumbling_WithOffset(t *testing.T) {
	w := window.NewTumbling(5 * time.Minute).WithOffset(1 * time.Minute)
	ts := time.Date(2026, 1, 1, 0, 7, 0, 0, time.UTC)
	wins := w.AssignWindows(ts)
	if len(wins) != 1 {
		t.Fatalf("expected 1 window, got %d", len(wins))
	}
	wantStart := time.Date(2026, 1, 1, 0, 6, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 1, 1, 0, 11, 0, 0, time.UTC)
	if !wins[0].Start.Equal(wantStart) {
		t.Errorf("Start: got %v, want %v", wins[0].Start, wantStart)
	}
	if !wins[0].End.Equal(wantEnd) {
		t.Errorf("End: got %v, want %v", wins[0].End, wantEnd)
	}
}

func TestTumbling_WindowSize(t *testing.T) {
	w := window.NewTumbling(10 * time.Second)
	if w.WindowSize() != 10*time.Second {
		t.Errorf("WindowSize: got %v, want %v", w.WindowSize(), 10*time.Second)
	}
}

func TestTumbling_EpochStart(t *testing.T) {
	w := window.NewTumbling(1 * time.Hour)
	ts := time.Unix(0, 0).UTC()
	wins := w.AssignWindows(ts)
	if len(wins) != 1 {
		t.Fatalf("expected 1 window, got %d", len(wins))
	}
	if !wins[0].Start.Equal(ts) {
		t.Errorf("epoch start: got %v, want %v", wins[0].Start, ts)
	}
}

func TestSliding_AssignWindows_Basic(t *testing.T) {
	s := window.NewSliding(5*time.Minute, 1*time.Minute)
	ts := time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC)
	wins := s.AssignWindows(ts)
	if len(wins) < 3 {
		t.Fatalf("expected at least 3 windows for sliding, got %d", len(wins))
	}
	for _, win := range wins {
		if !win.Start.Before(ts) || !win.End.After(ts) {
			if !(ts.Equal(win.Start) || ts.After(win.Start)) || !ts.Before(win.End) {
				t.Errorf("window %v-%v does not contain ts %v", win.Start, win.End, ts)
			}
		}
	}
}

func TestSliding_WindowSize(t *testing.T) {
	s := window.NewSliding(5*time.Minute, 1*time.Minute)
	if s.WindowSize() != 5*time.Minute {
		t.Errorf("WindowSize: got %v, want %v", s.WindowSize(), 5*time.Minute)
	}
}

func TestSliding_SlideEqualsSize(t *testing.T) {
	s := window.NewSliding(5*time.Minute, 5*time.Minute)
	ts := time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC)
	wins := s.AssignWindows(ts)
	if len(wins) != 1 {
		t.Fatalf("expected 1 window when slide==size, got %d", len(wins))
	}
	wantStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !wins[0].Start.Equal(wantStart) {
		t.Errorf("Start: got %v, want %v", wins[0].Start, wantStart)
	}
}

func TestSession_AssignWindows_Basic(t *testing.T) {
	s := window.NewSession(30 * time.Second)
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	wins := s.AssignWindows(ts)
	if len(wins) != 1 {
		t.Fatalf("expected 1 window, got %d", len(wins))
	}
	wantEnd := ts.Add(30 * time.Second)
	if !wins[0].Start.Equal(ts) {
		t.Errorf("Start: got %v, want %v", wins[0].Start, ts)
	}
	if !wins[0].End.Equal(wantEnd) {
		t.Errorf("End: got %v, want %v", wins[0].End, wantEnd)
	}
}

func TestSession_WindowSize(t *testing.T) {
	s := window.NewSession(1 * time.Minute)
	if s.WindowSize() != 1*time.Minute {
		t.Errorf("WindowSize: got %v, want %v", s.WindowSize(), 1*time.Minute)
	}
}

func TestMergeSessions_Overlapping(t *testing.T) {
	a := window.Window{Start: time.Unix(0, 0), End: time.Unix(30, 0)}
	b := window.Window{Start: time.Unix(20, 0), End: time.Unix(50, 0)}
	merged := window.MergeSessions(a, b)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged window, got %d", len(merged))
	}
	wantStart := time.Unix(0, 0)
	wantEnd := time.Unix(50, 0)
	if !merged[0].Start.Equal(wantStart) {
		t.Errorf("Start: got %v, want %v", merged[0].Start, wantStart)
	}
	if !merged[0].End.Equal(wantEnd) {
		t.Errorf("End: got %v, want %v", merged[0].End, wantEnd)
	}
}

func TestMergeSessions_Adjacent(t *testing.T) {
	a := window.Window{Start: time.Unix(0, 0), End: time.Unix(30, 0)}
	b := window.Window{Start: time.Unix(30, 0), End: time.Unix(60, 0)}
	merged := window.MergeSessions(a, b)
	if len(merged) != 2 {
		t.Fatalf("expected 2 separate windows (adjacent but not overlapping), got %d", len(merged))
	}
}

func TestMergeSessions_NonOverlapping(t *testing.T) {
	a := window.Window{Start: time.Unix(0, 0), End: time.Unix(10, 0)}
	b := window.Window{Start: time.Unix(50, 0), End: time.Unix(60, 0)}
	merged := window.MergeSessions(a, b)
	if len(merged) != 2 {
		t.Fatalf("expected 2 separate windows, got %d", len(merged))
	}
}

func TestMergeSessions_Contained(t *testing.T) {
	a := window.Window{Start: time.Unix(0, 0), End: time.Unix(60, 0)}
	b := window.Window{Start: time.Unix(10, 0), End: time.Unix(30, 0)}
	merged := window.MergeSessions(a, b)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged window, got %d", len(merged))
	}
	if !merged[0].Start.Equal(time.Unix(0, 0)) {
		t.Errorf("Start: got %v, want %v", merged[0].Start, time.Unix(0, 0))
	}
	if !merged[0].End.Equal(time.Unix(60, 0)) {
		t.Errorf("End: got %v, want %v", merged[0].End, time.Unix(60, 0))
	}
}

func TestMergeSessions_SameWindow(t *testing.T) {
	a := window.Window{Start: time.Unix(0, 0), End: time.Unix(10, 0)}
	merged := window.MergeSessions(a, a)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged window, got %d", len(merged))
	}
}

func TestWindowStart(t *testing.T) {
	size := 5 * time.Minute
	offset := time.Duration(0)
	ts := time.Date(2026, 1, 1, 0, 7, 30, 0, time.UTC)
	start := window.WindowStart(ts, size, offset)
	want := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	if !start.Equal(want) {
		t.Errorf("windowStart: got %v, want %v", start, want)
	}
}

func TestWindowStart_WithOffset(t *testing.T) {
	size := 5 * time.Minute
	offset := 2 * time.Minute
	ts := time.Date(2026, 1, 1, 0, 7, 0, 0, time.UTC)
	start := window.WindowStart(ts, size, offset)
	want := time.Date(2026, 1, 1, 0, 7, 0, 0, time.UTC)
	if !start.Equal(want) {
		t.Errorf("windowStart with offset: got %v, want %v", start, want)
	}
}
