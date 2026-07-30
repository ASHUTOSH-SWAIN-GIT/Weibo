package source

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func testManagerConfig() kafkaSourceConfig {
	return kafkaSourceConfig{brokers: []string{"localhost:9092"}, topic: "t", parallel: true}
}

// blockUntilCancel is a partition loop that idles until its reader context is
// cancelled — it never touches the network, so tests need no broker.
func blockUntilCancel(ctx context.Context, _ *kafka.Reader) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestPartitionManager_ReconcileAddsAndRemoves verifies reconcile drives the
// running reader set toward the desired set in both directions.
func TestPartitionManager_ReconcileAddsAndRemoves(t *testing.T) {
	m := newPartitionManager(testManagerConfig(), newOffsetTracker(), nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		m.stopAll()
	}()
	fatal := make(chan error, 1)

	m.reconcile(ctx, []int{0, 1}, blockUntilCancel, fatal)
	if got := m.partitionCount(); got != 2 {
		t.Fatalf("after add: got %d readers, want 2", got)
	}

	// Drop 0, keep 1, add 2.
	m.reconcile(ctx, []int{1, 2}, blockUntilCancel, fatal)
	if got := m.partitionCount(); got != 2 {
		t.Fatalf("after reconcile: got %d readers, want 2", got)
	}
	m.mu.Lock()
	_, has0 := m.running[0]
	_, has2 := m.running[2]
	m.mu.Unlock()
	if has0 {
		t.Error("partition 0 should have been removed")
	}
	if !has2 {
		t.Error("partition 2 should have been added")
	}
}

// TestPartitionManager_FatalErrorCancelsAll verifies that a reader failing
// fatally (context still live) aborts the run and returns the original error.
func TestPartitionManager_FatalErrorCancelsAll(t *testing.T) {
	boom := errors.New("partition 1 fatal")
	handle := func(ctx context.Context, r *kafka.Reader) error {
		if r.Config().Partition == 1 {
			return boom
		}
		return blockUntilCancel(ctx, r)
	}

	m := newPartitionManager(testManagerConfig(), newOffsetTracker(),
		func(context.Context) ([]int, error) { return []int{0, 1}, nil }, 0)

	err := m.run(context.Background(), []int{0, 1}, handle)
	if !errors.Is(err, boom) {
		t.Errorf("run: got %v, want %v", err, boom)
	}
	if got := m.partitionCount(); got != 0 {
		t.Errorf("after fatal: got %d readers still running, want 0", got)
	}
}

// TestPartitionManager_GracefulCancelReturnsNil verifies shutdown via context
// cancel returns nil (matching the previous parallel path).
func TestPartitionManager_GracefulCancelReturnsNil(t *testing.T) {
	m := newPartitionManager(testManagerConfig(), newOffsetTracker(),
		func(context.Context) ([]int, error) { return []int{0}, nil }, 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.run(ctx, []int{0}, blockUntilCancel) }()

	waitFor(t, func() bool { return m.partitionCount() == 1 })
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run on cancel: got %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

// TestPartitionManager_WatchAddsNewPartition verifies the watch loop starts a
// reader when a partition appears on a later discovery.
func TestPartitionManager_WatchAddsNewPartition(t *testing.T) {
	var grown int32
	desired := func(context.Context) ([]int, error) {
		if atomic.LoadInt32(&grown) == 0 {
			return []int{0}, nil
		}
		return []int{0, 1}, nil
	}

	m := newPartitionManager(testManagerConfig(), newOffsetTracker(), desired, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.run(ctx, []int{0}, blockUntilCancel) }()

	waitFor(t, func() bool { return m.partitionCount() == 1 })
	atomic.StoreInt32(&grown, 1) // topic gains partition 1
	waitFor(t, func() bool { return m.partitionCount() == 2 })

	cancel()
	<-done
}

// waitFor polls cond until true or fails the test after a short deadline.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}
