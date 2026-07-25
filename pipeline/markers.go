package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// alignedMerge fans the worker output channels into out, aligning
// broadcast markers (barriers and watermarks): each marker was sent
// to every worker, so it arrives len(workerOuts) times. The merge
// emits it downstream exactly once — only after ALL workers have
// delivered their copy.
//
// Alignment is strict: a worker that delivers a marker is PARKED (its
// output is no longer read) until every worker has delivered the same
// marker. This guarantees that no post-marker record can overtake the
// marker downstream — required for transactional sinks, where a
// post-barrier record leaking ahead of the barrier would be committed
// with the wrong checkpoint and duplicated on replay. Backpressure
// holds the parked workers' output naturally.
//
// Per-worker order is preserved by routing markers through the same
// fan-in channel as data: a marker can never be counted before the
// records that preceded it on its own worker.
//
// Returns when all worker outputs are closed, or with an error if
// hardCtx fires. When sm is non-nil, downstream sends are counted and
// block-timed as the stage's output.
func alignedMerge(hardCtx context.Context, workerOuts []chan types.Record, out chan<- types.Record, sm *stageMetrics) error {
	n := len(workerOuts)
	send := sendRecord
	if sm != nil {
		send = sm.send
	}

	fanIn := make(chan types.Record, internalBuf)
	resume := make([]chan struct{}, n)
	for i := range resume {
		resume[i] = make(chan struct{}, 1)
	}

	go func() {
		defer close(fanIn)
		var wg sync.WaitGroup
		for w, ch := range workerOuts {
			wg.Add(1)
			go func(w int, c <-chan types.Record) {
				defer wg.Done()
				for r := range c {
					isMarker := r.IsBarrier || r.IsWatermark
					select {
					case fanIn <- r:
					case <-hardCtx.Done():
						for range c {
						}
						return
					}
					if isMarker {
						// Park until every worker reached this marker.
						select {
						case <-resume[w]:
						case <-hardCtx.Done():
							for range c {
							}
							return
						}
					}
				}
			}(w, ch)
		}
		wg.Wait()
	}()

	// Workers park after each marker, so at most one marker round is
	// in flight — a plain counter suffices.
	arrived := 0
	for {
		r, ok, err := recvRecord(hardCtx, fanIn)
		if err != nil || !ok {
			return err
		}
		if r.IsBarrier || r.IsWatermark {
			arrived++
			if arrived < n {
				continue // hold until the laggards deliver their copy
			}
			arrived = 0
			if err := send(hardCtx, out, r); err != nil {
				return err
			}
			for _, ch := range resume {
				select {
				case ch <- struct{}{}:
				case <-hardCtx.Done():
					return hardCtx.Err()
				}
			}
			continue
		}
		if err := send(hardCtx, out, r); err != nil {
			return err
		}
	}
}

// latencyBatcher reproduces the old timedRead behavior: batch 100
// records and observe the average time per record.
type latencyBatcher struct {
	observe func(avg float64)
	n       int
	start   time.Time
}

func newLatencyBatcher(observe func(float64)) *latencyBatcher {
	return &latencyBatcher{observe: observe, start: time.Now()}
}

func (b *latencyBatcher) tick() {
	b.n++
	if b.n >= 100 {
		b.observe(time.Since(b.start).Seconds() / float64(b.n))
		b.n = 0
		b.start = time.Now()
	}
}

func (b *latencyBatcher) flush() {
	if b.n > 0 {
		b.observe(time.Since(b.start).Seconds() / float64(b.n))
		b.n = 0
	}
}
