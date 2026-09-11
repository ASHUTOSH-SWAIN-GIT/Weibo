package source

import (
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go"
)

// TestOffsetTracker_TrackKeepsLatestNextOffset verifies track stores the next
// offset to read (lastOffset+1) per partition and that a later offset wins.
func TestOffsetTracker_TrackKeepsLatestNextOffset(t *testing.T) {
	tr := newOffsetTracker()
	tr.track(kafka.Message{Topic: "orders", Partition: 0, Offset: 10})
	tr.track(kafka.Message{Topic: "orders", Partition: 0, Offset: 11})
	tr.track(kafka.Message{Topic: "orders", Partition: 1, Offset: 4})

	got := decodeOffsets(t, tr)
	if got["orders/0"] != 12 {
		t.Errorf("partition 0: got %d, want 12", got["orders/0"])
	}
	if got["orders/1"] != 5 {
		t.Errorf("partition 1: got %d, want 5", got["orders/1"])
	}
}

// TestOffsetTracker_RestoreSeedsConsumed verifies restore populates both the
// seek target and consumed, so a quiet partition survives into the next
// snapshot at its restored position.
func TestOffsetTracker_RestoreSeedsConsumed(t *testing.T) {
	tr := newOffsetTracker()
	if err := tr.restore([]byte(`{"3":100,"5":50}`), "orders"); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if !tr.hasRestored() {
		t.Fatal("hasRestored: got false, want true")
	}
	if off, ok := tr.restoredOffset("orders", 3); !ok || off != 100 {
		t.Errorf("restoredOffset(3): got %d (ok=%v), want 100", off, ok)
	}

	// Only partition 5 advances; partition 3 stays quiet but must persist.
	tr.track(kafka.Message{Topic: "orders", Partition: 5, Offset: 60})

	got := decodeOffsets(t, tr)
	if got["orders/3"] != 100 {
		t.Errorf("quiet restored partition 3 dropped: %v", got)
	}
	if got["orders/5"] != 61 {
		t.Errorf("partition 5: got %d, want 61", got["orders/5"])
	}
}

func TestOffsetTracker_RestoreRejectsBadJSON(t *testing.T) {
	tr := newOffsetTracker()
	if err := tr.restore([]byte(`not-json`), "orders"); err == nil {
		t.Fatal("restore(bad json): got nil error, want error")
	}
}

func TestOffsetTrackerOperationalStateIncludesTopicPartitionAndLag(t *testing.T) {
	tr := newOffsetTracker()
	if err := tr.restore([]byte(`{"1":7}`), "orders"); err != nil {
		t.Fatal(err)
	}
	tr.track(kafka.Message{Topic: "orders", Partition: 1, Offset: 9, HighWaterMark: 15})
	got := tr.operationalState()
	if len(got) != 1 || got[0].Topic != "orders" || got[0].Partition != 1 ||
		got[0].CurrentOffset != 10 || got[0].CheckpointOffset != 7 || got[0].HighWatermark != 15 || got[0].Lag != 5 {
		t.Fatalf("operational state = %+v", got)
	}
}

func decodeOffsets(t *testing.T, tr *offsetTracker) map[string]int64 {
	t.Helper()
	data, err := tr.snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return decodePositionMap(t, data, "")
}

func decodePositionMap(t *testing.T, data []byte, legacySource string) map[string]int64 {
	t.Helper()
	positions, err := DecodePositions(data, legacySource)
	if err != nil {
		t.Fatalf("decode positions: %v", err)
	}
	got := make(map[string]int64, len(positions))
	for _, position := range positions {
		got[fmt.Sprintf("%s/%d", position.Source, position.Partition)] = position.Offset
	}
	return got
}
