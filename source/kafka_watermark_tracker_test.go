package source

import (
	"testing"
	"time"
)

func TestWatermarkTracker_Enabled(t *testing.T) {
	if (watermarkTracker{}).enabled() {
		t.Error("zero tracker: got enabled, want disabled")
	}
	if !(watermarkTracker{outOfOrderness: time.Second}).enabled() {
		t.Error("configured tracker: got disabled, want enabled")
	}
}

func TestWatermarkTracker_WrapPreservesInterval(t *testing.T) {
	wt := watermarkTracker{outOfOrderness: 2 * time.Second, interval: 250 * time.Millisecond}
	ws := wt.wrap(nil)
	if ws.Interval != 250*time.Millisecond {
		t.Errorf("interval: got %v, want 250ms", ws.Interval)
	}
	if ws.Generator == nil {
		t.Error("generator: got nil, want bounded-out-of-orderness generator")
	}
}
