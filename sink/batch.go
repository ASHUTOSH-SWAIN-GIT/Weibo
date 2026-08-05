package sink

import (
	"context"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

const (
	// defaultBatchSize is the flush threshold when a sink does not
	// configure one.
	defaultBatchSize = 100

	// defaultShutdownTimeout bounds how long a sink keeps draining its
	// input after the context is cancelled, before the final flush.
	defaultShutdownTimeout = 5 * time.Second
)

// batchWriter drives the accumulate-then-flush loop that every
// record-batching sink needs: convert each record into the sink's own
// entry type, buffer until the batch is full or the flush interval
// elapses, and — on shutdown — keep draining the input for a bounded
// period so in-flight records still land.
//
// It deliberately owns only the loop. Retries, failure policy and DLQ
// routing stay inside each sink's flush function, because those depend
// on what a partial failure means for that destination (a Kafka write
// fails per batch; a Postgres insert fails per table group).
//
// T is the sink's entry type — whatever it needs to remember per record
// to write it later, typically the converted payload plus the original
// types.Record so the failure policy can still see it.
type batchWriter[T any] struct {
	// batchSize is the flush threshold. <= 0 uses defaultBatchSize.
	batchSize int

	// flushInterval flushes a partial batch periodically. Zero disables
	// periodic flushing — the batch then moves only when it fills up or
	// the stream ends.
	flushInterval time.Duration

	// shutdownTimeout bounds the post-cancellation drain. <= 0 uses
	// defaultShutdownTimeout.
	shutdownTimeout time.Duration

	// async runs each flush in its own goroutine, letting the next batch
	// accumulate while the current one is in flight. Errors surface on a
	// later loop turn rather than at the call that triggered the flush.
	// Sinks whose client is safe for concurrent writes (Kafka) opt in;
	// sinks that must apply backpressure per batch (Postgres) do not.
	async bool

	// convert maps a record to an entry. Returning false skips the
	// record entirely — e.g. a Postgres mapper that declines a row.
	convert func(types.Record) (T, bool)

	// flush writes one batch. It is called with a non-empty slice, and
	// never concurrently with itself unless async is set.
	flush func(ctx context.Context, batch []T) error
}

// run consumes in until it closes or ctx is cancelled, batching records
// and flushing them through b.flush.
//
// Shutdown: on cancellation it drains in for shutdownTimeout and flushes
// what it collected using a fresh background context, so records already
// accepted by the pipeline are not lost to the cancelled one. It then
// reports ctx.Err().
func (b *batchWriter[T]) run(ctx context.Context, in <-chan types.Record) error {
	batchSize := b.batchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	shutdownTimeout := b.shutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultShutdownTimeout
	}

	var (
		mu    sync.Mutex
		batch []T
		wg    sync.WaitGroup
		errCh = make(chan error, 1)
	)

	// add buffers a record and reports whether the batch is now full.
	add := func(r types.Record) bool {
		item, ok := b.convert(r)
		if !ok {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		batch = append(batch, item)
		return len(batch) >= batchSize
	}

	// doFlush hands the buffered entries to b.flush. In async mode it
	// returns nil immediately and the error lands on errCh.
	doFlush := func(flushCtx context.Context) error {
		mu.Lock()
		if len(batch) == 0 {
			mu.Unlock()
			return nil
		}
		items := batch
		batch = nil
		mu.Unlock()

		if !b.async {
			return b.flush(flushCtx, items)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.flush(flushCtx, items); err != nil {
				select {
				case errCh <- err:
				default: // an earlier error is already reported
				}
			}
		}()
		return nil
	}

	// asyncErr reports a background flush failure, if one is waiting.
	asyncErr := func() error {
		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	}

	// drain keeps reading records after cancellation so the final flush
	// covers everything the pipeline already handed over.
	drain := func() {
		deadline := time.NewTimer(shutdownTimeout)
		defer deadline.Stop()
		for {
			select {
			case r, ok := <-in:
				if !ok {
					return
				}
				add(r)
			case <-deadline.C:
				return
			}
		}
	}

	var tick <-chan time.Time
	if b.flushInterval > 0 {
		ticker := time.NewTicker(b.flushInterval)
		defer ticker.Stop()
		tick = ticker.C
	}

	for {
		select {
		case err := <-errCh:
			wg.Wait()
			return err

		case <-ctx.Done():
			// The cancelled ctx can't carry the final write, so the
			// drain-and-flush runs on a fresh bounded context.
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			drain()
			err := doFlush(shutdownCtx)
			wg.Wait()
			cancel()
			if err != nil {
				return err
			}
			if err := asyncErr(); err != nil {
				return err
			}
			return ctx.Err()

		case r, ok := <-in:
			if !ok {
				err := doFlush(ctx)
				wg.Wait()
				if err != nil {
					return err
				}
				return asyncErr()
			}
			if add(r) {
				if err := doFlush(ctx); err != nil {
					return err
				}
			}

		case <-tick:
			if err := doFlush(ctx); err != nil {
				return err
			}
		}
	}
}
