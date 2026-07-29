package source

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestReaderSupervisor_BuildsSerialReader(t *testing.T) {
	s := newSerialReaderSupervisor(kafkaSourceConfig{
		brokers: []string{"localhost:9092"},
		topic:   "t",
		groupID: "g",
	})
	defer s.closeAll()

	if len(s.readers) != 1 {
		t.Fatalf("readers: got %d, want 1", len(s.readers))
	}
	if len(s.partitionIDs) != 1 || s.partitionIDs[0] != -1 {
		t.Errorf("partitionIDs: got %v, want [-1]", s.partitionIDs)
	}
	if s.parallelMode() {
		t.Error("parallelMode: got true, want false for a single reader")
	}
}

func TestReaderSupervisor_BuildsOneReaderPerPartition(t *testing.T) {
	s := newParallelReaderSupervisor(kafkaSourceConfig{
		brokers: []string{"localhost:9092"},
		topic:   "t",
	}, []int{0, 1, 2})
	defer s.closeAll()

	if len(s.readers) != 3 {
		t.Fatalf("readers: got %d, want 3", len(s.readers))
	}
	if len(s.partitionIDs) != 3 || s.partitionIDs[2] != 2 {
		t.Errorf("partitionIDs: got %v, want [0 1 2]", s.partitionIDs)
	}
	if !s.parallelMode() {
		t.Error("parallelMode: got false, want true for multiple readers")
	}
}

// TestReaderSupervisor_RunParallelReturnsFirstError verifies a fatal loop
// error while the context is live is returned and every loop is run.
func TestReaderSupervisor_RunParallelReturnsFirstError(t *testing.T) {
	s := &readerSupervisor{
		readers:      make([]*kafka.Reader, 3),
		partitionIDs: []int{0, 1, 2},
	}

	var started int32
	wantErr := errors.New("reader 1 fatal")
	err := s.runParallel(context.Background(), func(_ context.Context, idx int, _ *kafka.Reader) error {
		atomic.AddInt32(&started, 1)
		if idx == 1 {
			return wantErr
		}
		return nil
	})

	if !errors.Is(err, wantErr) {
		t.Errorf("runParallel: got %v, want %v", err, wantErr)
	}
	if got := atomic.LoadInt32(&started); got != 3 {
		t.Errorf("loops started: got %d, want 3", got)
	}
}

// TestReaderSupervisor_RunParallelIgnoresPostCancelErrors verifies errors from
// loops that stop because the context was cancelled are not surfaced, matching
// the previous behaviour (only errors with ctx.Err()==nil propagate).
func TestReaderSupervisor_RunParallelIgnoresPostCancelErrors(t *testing.T) {
	s := &readerSupervisor{
		readers:      make([]*kafka.Reader, 2),
		partitionIDs: []int{0, 1},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := s.runParallel(ctx, func(ctx context.Context, _ int, _ *kafka.Reader) error {
		return ctx.Err() // returns context.Canceled, must be ignored
	})
	if err != nil {
		t.Errorf("runParallel: got %v, want nil (post-cancel errors ignored)", err)
	}
}
