package source

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// desiredPartitionsFunc returns the set of partition IDs that should currently
// be running. It is called on each watch tick to detect partitions that have
// been added to the topic. Kafka only ever adds partitions, but the manager
// handles removals generically so the same reconcile logic can later be driven
// by an assignment source.
type desiredPartitionsFunc func(ctx context.Context) ([]int, error)

// partitionLoop is the per-reader work body: fetch, track, deserialize, emit.
// It runs until the reader's context is cancelled or a fatal error occurs.
type partitionLoop func(ctx context.Context, r *kafka.Reader) error

// partitionReader is one running partition reader and its lifecycle handle.
type partitionReader struct {
	reader *kafka.Reader
	cancel context.CancelFunc
}

// partitionManager keeps the running partition readers aligned with a desired
// set of partitions (parallel mode only).
//
// It owns reader creation, per-partition cancellation and fatal-error
// supervision. On each reconcile it starts readers for partitions that have
// appeared and stops readers for partitions that have disappeared. A reader
// that fails fatally (its loop returns an error while its context is still
// live) cancels every other reader and fails the run — unlike the previous
// fixed-set fan-out, which left siblings running.
type partitionManager struct {
	cfg      kafkaSourceConfig
	offsets  *offsetTracker
	desired  desiredPartitionsFunc
	interval time.Duration // 0 = reconcile once at startup, no watching

	wg sync.WaitGroup

	mu      sync.Mutex
	running map[int]*partitionReader
}

// newPartitionManager builds a manager for parallel mode. desired supplies the
// live partition set; interval is the re-discovery period (0 disables
// watching, so the manager reconciles once against the startup seed).
func newPartitionManager(cfg kafkaSourceConfig, offsets *offsetTracker, desired desiredPartitionsFunc, interval time.Duration) *partitionManager {
	return &partitionManager{
		cfg:      cfg,
		offsets:  offsets,
		desired:  desired,
		interval: interval,
		running:  make(map[int]*partitionReader),
	}
}

// run reconciles to the startup seed, then (if watching is enabled) keeps
// reconciling on each tick until the context is cancelled or a reader fails
// fatally. All readers are cancelled and awaited before run returns.
func (m *partitionManager) run(ctx context.Context, seed []int, handle partitionLoop) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer m.stopAll()

	// Buffered so a dying reader never blocks on send if the manager has
	// already moved on; only the first fatal error is acted upon.
	fatal := make(chan error, 1)

	m.reconcile(ctx, seed, handle, fatal)

	var tick <-chan time.Time
	if m.interval > 0 {
		t := time.NewTicker(m.interval)
		defer t.Stop()
		tick = t.C
	}

	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown: match the previous parallel path, which
			// returned nil (not ctx.Err()) so shutdown logs stay quiet.
			return nil
		case err := <-fatal:
			// Cancel-on-error: the deferred cancel/stopAll tears down every
			// sibling reader; return the original error.
			return err
		case <-tick:
			ids, err := m.desired(ctx)
			if err != nil {
				// A transient metadata blip must not kill the pipeline; keep
				// the current readers and try again next tick.
				fmt.Printf("weibo/source: partition discovery failed: %v\n", err)
				continue
			}
			m.reconcile(ctx, ids, handle, fatal)
		}
	}
}

// reconcile drives the running readers toward desired: it stops readers for
// partitions no longer wanted and starts readers for newly wanted partitions.
func (m *partitionManager) reconcile(ctx context.Context, desired []int, handle partitionLoop, fatal chan error) {
	want := make(map[int]bool, len(desired))
	for _, id := range desired {
		want[id] = true
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop readers for partitions that are gone. The goroutine observes its
	// cancelled context, returns, and closes its own reader.
	for id, pr := range m.running {
		if !want[id] {
			pr.cancel()
			delete(m.running, id)
		}
	}

	// Start readers for partitions that appeared.
	for _, id := range desired {
		if _, ok := m.running[id]; ok {
			continue
		}
		m.startReader(ctx, id, handle, fatal)
	}
}

// startReader builds and launches one partition reader. Caller holds m.mu.
func (m *partitionManager) startReader(ctx context.Context, id int, handle partitionLoop, fatal chan error) {
	reader := buildPartitionReader(m.cfg, id)
	// Seek only partitions present in the restored checkpoint. A partition
	// that appeared after the checkpoint has no restored offset and starts
	// from the configured StartOffset instead.
	if off, ok := m.offsets.restoredOffset(id); ok {
		if err := reader.SetOffset(off); err != nil {
			fmt.Printf("weibo/source: restore offset partition %d: %v\n", id, err)
		}
	}

	rctx, rcancel := context.WithCancel(ctx)
	pr := &partitionReader{reader: reader, cancel: rcancel}
	m.running[id] = pr

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		err := handle(rctx, reader)
		// A non-nil error while the reader's context is still live is a
		// genuine failure (fetch retries exhausted). If rctx was cancelled —
		// by removal or by shutdown — the error is just the wind-down and is
		// ignored, matching the previous ctx.Err()==nil guard.
		if err != nil && rctx.Err() == nil {
			select {
			case fatal <- err:
			default:
			}
		}
		reader.Close()
	}()
}

// partitionCount reports how many partition readers are currently running.
func (m *partitionManager) partitionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.running)
}

// stopAll cancels every running reader and waits for all reader goroutines
// (including any already-removed ones) to exit and close their readers.
func (m *partitionManager) stopAll() {
	m.mu.Lock()
	for id, pr := range m.running {
		pr.cancel()
		delete(m.running, id)
	}
	m.mu.Unlock()
	m.wg.Wait()
}
