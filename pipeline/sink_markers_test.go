package pipeline_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/pipeline"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// recordingSink is a plain sink.Sink: it captures everything it is
// handed, so a leaked marker shows up as a captured record.
type recordingSink struct {
	mu   sync.Mutex
	seen []types.Record
}

func (s *recordingSink) Write(ctx context.Context, in <-chan types.Record) error {
	for r := range in {
		s.mu.Lock()
		s.seen = append(s.seen, r)
		s.mu.Unlock()
	}
	return nil
}

func (s *recordingSink) snapshot() []types.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]types.Record(nil), s.seen...)
}

// checkpointedRecordingSink additionally implements sink.CheckpointedSink,
// so the stage must keep delivering barriers to it in-band.
type checkpointedRecordingSink struct{ recordingSink }

func (s *checkpointedRecordingSink) SetOnPrepared(func(id string, err error)) {}
func (s *checkpointedRecordingSink) Commit(context.Context, string) error     { return nil }
func (s *checkpointedRecordingSink) Abort(context.Context, string) error      { return nil }
func (s *checkpointedRecordingSink) WasCommitted(context.Context, string) (bool, error) {
	return true, nil
}

// runSinkStage pumps records through a SinkStage and returns once it
// completes.
func runSinkStage(t *testing.T, st *pipeline.SinkStage, records []types.Record) {
	t.Helper()
	in := make(chan types.Record, len(records))
	for _, r := range records {
		in <- r
	}
	close(in)

	done := make(chan error, 1)
	go func() { done <- st.Run(context.Background(), context.Background(), in, nil) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sink stage: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sink stage did not complete")
	}
}

func markerTestRecords() []types.Record {
	return []types.Record{
		{Key: []byte("a")},
		types.NewBarrier("ckpt-1"),
		{Key: []byte("b")},
		{IsWatermark: true, Timestamp: time.Unix(100, 0)},
		{Key: []byte("c")},
	}
}

// TestSinkStage_OrdinarySinkNeverSeesMarkers pins the fix: barriers and
// watermarks are engine-internal control records. An ordinary sink writes
// whatever it is handed, so a leaked marker becomes a published Kafka
// message, a printed line, or an INSERT of an empty row.
func TestSinkStage_OrdinarySinkNeverSeesMarkers(t *testing.T) {
	sk := &recordingSink{}
	runSinkStage(t, &pipeline.SinkStage{Sink: sk}, markerTestRecords())

	got := sk.snapshot()
	if len(got) != 3 {
		t.Fatalf("expected only the 3 data records, got %d: %+v", len(got), got)
	}
	for _, r := range got {
		if r.IsBarrier || r.IsWatermark {
			t.Errorf("marker leaked to sink: %+v", r)
		}
	}
	for i, want := range []string{"a", "b", "c"} {
		if string(got[i].Key) != want {
			t.Errorf("record %d = %q, want %q", i, got[i].Key, want)
		}
	}
}

// TestSinkStage_CheckpointedSinkStillGetsBarriers guards the other side:
// a CheckpointedSink needs barriers in-band to know which output belongs
// to which transaction. Filtering them would silently break exactly-once.
// Watermarks are still dropped.
func TestSinkStage_CheckpointedSinkStillGetsBarriers(t *testing.T) {
	sk := &checkpointedRecordingSink{}
	runSinkStage(t, &pipeline.SinkStage{Sink: sk}, markerTestRecords())

	got := sk.snapshot()
	barriers, watermarks := 0, 0
	for _, r := range got {
		if r.IsBarrier {
			barriers++
		}
		if r.IsWatermark {
			watermarks++
		}
	}
	if barriers != 1 {
		t.Errorf("coordinated sink got %d barriers, want 1 (exactly-once needs them in-band)", barriers)
	}
	if watermarks != 0 {
		t.Errorf("coordinated sink got %d watermarks, want 0", watermarks)
	}
	if len(got) != 4 {
		t.Errorf("expected 3 data records + 1 barrier, got %d: %+v", len(got), got)
	}
}

// TestSinkStage_OnBarrierStillFires verifies the uncoordinated
// checkpoint hook is unaffected: the barrier must still be reported for
// the checkpoint to be saved, even though it is no longer forwarded.
func TestSinkStage_OnBarrierStillFires(t *testing.T) {
	sk := &recordingSink{}
	var mu sync.Mutex
	var ids []string

	st := &pipeline.SinkStage{
		Sink: sk,
		OnBarrier: func(id string) {
			mu.Lock()
			ids = append(ids, id)
			mu.Unlock()
		},
	}
	runSinkStage(t, st, markerTestRecords())

	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 1 || ids[0] != "ckpt-1" {
		t.Errorf("OnBarrier ids = %v, want [ckpt-1]", ids)
	}
	if len(sk.snapshot()) != 3 {
		t.Errorf("expected 3 data records at the sink, got %+v", sk.snapshot())
	}
}
