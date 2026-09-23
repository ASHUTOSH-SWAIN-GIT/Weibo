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

// The Kafka wrapper must opt in to per-partition watermarks and pass the idle
// timeout through — otherwise records from slower partitions are dropped as late.
func TestWatermarkTracker_WrapEnablesPerPartitionWatermarks(t *testing.T) {
	wt := watermarkTracker{outOfOrderness: 3 * time.Second, interval: time.Second, idleTimeout: 45 * time.Second}
	ws := wt.wrap(nil)
	if ws.NewGenerator == nil {
		t.Fatal("NewGenerator: got nil, want per-partition generators")
	}
	if ws.PartitionIdleTimeout != 45*time.Second {
		t.Errorf("idle timeout: got %v, want 45s", ws.PartitionIdleTimeout)
	}
	// Each call must return an independent generator honoring the out-of-orderness.
	a, b := ws.NewGenerator(), ws.NewGenerator()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a.OnRecord(base.Add(time.Minute))
	if !b.GetWatermark().IsZero() {
		t.Error("generators share state: b saw a's record")
	}
	if got, want := a.GetWatermark(), base.Add(time.Minute-3*time.Second); !got.Equal(want) {
		t.Errorf("watermark: got %v, want %v", got, want)
	}
}

func TestKafkaPartitionIdleTimeoutOption(t *testing.T) {
	var cfg kafkaSourceConfig
	KafkaPartitionIdleTimeout(90 * time.Second)(&cfg)
	if cfg.watermarkIdleTimeout != 90*time.Second {
		t.Errorf("got %v, want 90s", cfg.watermarkIdleTimeout)
	}
}
