package pipeline

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// markerKey identifies a broadcast marker for alignment counting.
// Barriers align on CheckpointID, watermarks on their timestamp.
func markerKey(r types.Record) string {
	if r.IsBarrier {
		return "b:" + r.CheckpointID
	}
	return "w:" + strconv.FormatInt(r.Timestamp.UnixNano(), 10)
}

// alignedMerge fans the worker output channels into out, aligning
// broadcast markers (barriers and watermarks): each marker was sent
// to every worker, so it arrives len(workerOuts) times. The merge
// emits it downstream exactly once — only after ALL workers have
// delivered their copy. At that point every worker has processed all
// of its pre-marker records, so a checkpoint triggered downstream
// captures consistent state (Chandy-Lamport alignment).
//
// Data records are forwarded as they arrive. Post-marker records from
// already-aligned workers may pass the held marker; that is safe
// because workers own disjoint partitions — their state was already
// captured at their own marker.
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
	go func() {
		defer close(fanIn)
		var wg sync.WaitGroup
		for _, ch := range workerOuts {
			wg.Add(1)
			go func(c <-chan types.Record) {
				defer wg.Done()
				for r := range c {
					select {
					case fanIn <- r:
					case <-hardCtx.Done():
						for range c {
						}
						return
					}
				}
			}(ch)
		}
		wg.Wait()
	}()

	seen := make(map[string]int)
	for {
		r, ok, err := recvRecord(hardCtx, fanIn)
		if err != nil || !ok {
			return err
		}
		if r.IsBarrier || r.IsWatermark {
			k := markerKey(r)
			seen[k]++
			if seen[k] == n {
				delete(seen, k)
				if err := send(hardCtx, out, r); err != nil {
					return err
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
