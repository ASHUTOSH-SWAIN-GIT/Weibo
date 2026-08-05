package sink

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// collector records the batches a batchWriter hands to flush.
type collector struct {
	mu      sync.Mutex
	batches [][]string
	err     error // returned by every flush when set
	block   chan struct{}
}

func (c *collector) flush(_ context.Context, batch []string) error {
	if c.block != nil {
		<-c.block
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := append([]string(nil), batch...)
	c.batches = append(c.batches, cp)
	return c.err
}

func (c *collector) snapshot() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]string(nil), c.batches...)
}

func (c *collector) flat() []string {
	var out []string
	for _, b := range c.snapshot() {
		out = append(out, b...)
	}
	return out
}

func keyConvert(r types.Record) (string, bool) { return string(r.Key), true }

func feed(keys ...string) chan types.Record {
	in := make(chan types.Record, len(keys))
	for _, k := range keys {
		in <- types.Record{Key: []byte(k)}
	}
	return in
}

// TestBatchWriter_FlushesOnBatchSize verifies the size trigger: full
// batches go out as they fill, and the remainder flushes when the input
// closes so no record is stranded.
func TestBatchWriter_FlushesOnBatchSize(t *testing.T) {
	c := &collector{}
	bw := &batchWriter[string]{batchSize: 2, convert: keyConvert, flush: c.flush}

	in := feed("a", "b", "c", "d", "e")
	close(in)
	if err := bw.run(context.Background(), in); err != nil {
		t.Fatalf("run: %v", err)
	}

	got := c.snapshot()
	if len(got) != 3 {
		t.Fatalf("expected 3 batches (2+2+1), got %d: %v", len(got), got)
	}
	if len(got[0]) != 2 || len(got[2]) != 1 {
		t.Errorf("batch shape = %v, want [2 2 1]", got)
	}
	if len(c.flat()) != 5 {
		t.Errorf("expected 5 records total, got %v", c.flat())
	}
}

// TestBatchWriter_FlushesOnInterval verifies the time trigger: a partial
// batch must not sit in the buffer indefinitely when a flush interval is
// configured.
func TestBatchWriter_FlushesOnInterval(t *testing.T) {
	c := &collector{}
	bw := &batchWriter[string]{
		batchSize:     1000, // never reached
		flushInterval: 10 * time.Millisecond,
		convert:       keyConvert,
		flush:         c.flush,
	}

	in := make(chan types.Record, 4)
	in <- types.Record{Key: []byte("a")}

	done := make(chan error, 1)
	go func() { done <- bw.run(context.Background(), in) }()

	deadline := time.After(2 * time.Second)
	for len(c.flat()) == 0 {
		select {
		case <-deadline:
			t.Fatal("partial batch was never flushed on the interval")
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(in)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

// TestBatchWriter_ConvertSkipsRecords verifies that a convert returning
// false drops the record entirely — the Postgres mapper's "no row for
// this record" case.
func TestBatchWriter_ConvertSkipsRecords(t *testing.T) {
	c := &collector{}
	bw := &batchWriter[string]{
		batchSize: 10,
		convert: func(r types.Record) (string, bool) {
			if string(r.Key) == "skip" {
				return "", false
			}
			return string(r.Key), true
		},
		flush: c.flush,
	}

	in := feed("a", "skip", "b", "skip")
	close(in)
	if err := bw.run(context.Background(), in); err != nil {
		t.Fatalf("run: %v", err)
	}

	got := c.flat()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("got %v, want [a b]", got)
	}
}

// TestBatchWriter_SyncFlushErrorPropagates verifies that a synchronous
// flush failure stops the loop and surfaces to the caller.
func TestBatchWriter_SyncFlushErrorPropagates(t *testing.T) {
	sentinel := errors.New("insert failed")
	c := &collector{err: sentinel}
	bw := &batchWriter[string]{batchSize: 1, convert: keyConvert, flush: c.flush}

	in := feed("a", "b")
	close(in)
	if err := bw.run(context.Background(), in); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want %v", err, sentinel)
	}
}

// TestBatchWriter_AsyncFlushErrorPropagates verifies that a background
// flush failure is not swallowed: it must still reach the caller, even
// when it lands after the input has closed.
func TestBatchWriter_AsyncFlushErrorPropagates(t *testing.T) {
	sentinel := errors.New("produce failed")
	c := &collector{err: sentinel}
	bw := &batchWriter[string]{batchSize: 1, async: true, convert: keyConvert, flush: c.flush}

	in := feed("a")
	close(in)
	if err := bw.run(context.Background(), in); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want %v", err, sentinel)
	}
}

// TestBatchWriter_DrainsAfterCancel verifies the shutdown contract:
// records already handed to the sink are flushed even though the context
// that delivered them is already cancelled.
func TestBatchWriter_DrainsAfterCancel(t *testing.T) {
	c := &collector{}
	bw := &batchWriter[string]{
		batchSize:       1000, // only the shutdown path can flush these
		shutdownTimeout: 500 * time.Millisecond,
		convert:         keyConvert,
		flush:           c.flush,
	}

	in := make(chan types.Record, 4)
	in <- types.Record{Key: []byte("a")}
	in <- types.Record{Key: []byte("b")}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before run even starts

	done := make(chan error, 1)
	go func() { done <- bw.run(ctx, in) }()

	// Deliver one more record during the drain window, then close so the
	// drain finishes without waiting out the full timeout.
	in <- types.Record{Key: []byte("c")}
	close(in)

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if got := c.flat(); len(got) != 3 {
		t.Errorf("expected all 3 records flushed on shutdown, got %v", got)
	}
}

// TestBatchWriter_AsyncOverlapsFlushes verifies that async mode keeps
// accepting records while a flush is in flight — the property that makes
// it worth having, and the reason the Kafka sink opts in.
func TestBatchWriter_AsyncOverlapsFlushes(t *testing.T) {
	c := &collector{block: make(chan struct{})}
	bw := &batchWriter[string]{batchSize: 1, async: true, convert: keyConvert, flush: c.flush}

	in := make(chan types.Record, 4)
	in <- types.Record{Key: []byte("a")}

	done := make(chan error, 1)
	go func() { done <- bw.run(context.Background(), in) }()

	// The first flush is parked inside c.block. A synchronous writer
	// would be stuck there; this send must still be accepted.
	select {
	case in <- types.Record{Key: []byte("b")}:
	case <-time.After(2 * time.Second):
		t.Fatal("async writer stalled while a flush was in flight")
	}

	close(c.block)
	close(in)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := c.flat(); len(got) != 2 {
		t.Errorf("expected 2 records, got %v", got)
	}
}
