package source

import (
	"encoding/json"
	"testing"

	"github.com/segmentio/kafka-go"
)

// TestOffsetTracker_TrackKeepsLatestNextOffset verifies track stores the next
// offset to read (lastOffset+1) per partition and that a later offset wins.
func TestOffsetTracker_TrackKeepsLatestNextOffset(t *testing.T) {
	tr := newOffsetTracker()
	tr.track(kafka.Message{Partition: 0, Offset: 10})
	tr.track(kafka.Message{Partition: 0, Offset: 11})
	tr.track(kafka.Message{Partition: 1, Offset: 4})

	got := decodeOffsets(t, tr)
	if got["0"] != 12 {
		t.Errorf("partition 0: got %d, want 12", got["0"])
	}
	if got["1"] != 5 {
		t.Errorf("partition 1: got %d, want 5", got["1"])
	}
}

// TestOffsetTracker_RestoreSeedsConsumed verifies restore populates both the
// seek target and consumed, so a quiet partition survives into the next
// snapshot at its restored position.
func TestOffsetTracker_RestoreSeedsConsumed(t *testing.T) {
	tr := newOffsetTracker()
	if err := tr.restore([]byte(`{"3":100,"5":50}`)); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if !tr.hasRestored() {
		t.Fatal("hasRestored: got false, want true")
	}
	if off, ok := tr.restoredOffset(3); !ok || off != 100 {
		t.Errorf("restoredOffset(3): got %d (ok=%v), want 100", off, ok)
	}

	// Only partition 5 advances; partition 3 stays quiet but must persist.
	tr.track(kafka.Message{Partition: 5, Offset: 60})

	got := decodeOffsets(t, tr)
	if got["3"] != 100 {
		t.Errorf("quiet restored partition 3 dropped: %v", got)
	}
	if got["5"] != 61 {
		t.Errorf("partition 5: got %d, want 61", got["5"])
	}
}

func TestOffsetTracker_RestoreRejectsBadJSON(t *testing.T) {
	tr := newOffsetTracker()
	if err := tr.restore([]byte(`not-json`)); err == nil {
		t.Fatal("restore(bad json): got nil error, want error")
	}
}

func decodeOffsets(t *testing.T, tr *offsetTracker) map[string]int64 {
	t.Helper()
	data, err := tr.snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var got map[string]int64
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return got
}
