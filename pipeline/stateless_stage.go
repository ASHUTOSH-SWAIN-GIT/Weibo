package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// StatelessStage executes a chain of stateless operators (Map, Filter,
// FlatMap, Process) as direct function calls — no channels between
// operators. With Parallelism > 1, N workers share the input via a
// round-robin dispatcher; barriers and watermarks are broadcast to all
// workers and re-aligned at the merge (C1). Record order across
// workers is not preserved when Parallelism > 1.
type StatelessStage struct {
	StageName   string
	Ops         []operator.Operator // every op must implement operator.SingleProcessor
	Labels      []string            // metric label per op, aligned with Ops
	Parallelism int
}

func (s *StatelessStage) Name() string { return s.StageName }

func (s *StatelessStage) Run(runCtx, hardCtx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	defer close(out)
	if s.Parallelism <= 1 {
		return s.runWorker(hardCtx, in, out)
	}
	return s.runParallel(hardCtx, in, out)
}

// runWorker is the execution loop shared by the serial path (reading
// the stage input directly) and each parallel worker (reading its own
// dispatched channel). Markers are forwarded as-is: in the serial
// path they stay in order; in the parallel path they land in the
// worker's output for the aligned merge to dedup.
func (s *StatelessStage) runWorker(hardCtx context.Context, in <-chan types.Record, out chan<- types.Record) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stage %s: operator panicked: %v", s.StageName, r)
		}
	}()

	timers := make([]*latencyBatcher, len(s.Ops))
	for i, label := range s.Labels {
		l := label
		timers[i] = newLatencyBatcher(func(avg float64) {
			metrics.OperatorLatencySeconds.WithLabelValues(l).Observe(avg)
		})
	}
	defer func() {
		for _, t := range timers {
			t.flush()
		}
	}()

	for {
		r, ok, rerr := recvRecord(hardCtx, in)
		if rerr != nil {
			return rerr
		}
		if !ok {
			return nil
		}
		if r.IsBarrier || r.IsWatermark {
			if serr := sendRecord(hardCtx, out, r); serr != nil {
				return serr
			}
			continue
		}
		outs := []types.Record{r}
		for i, op := range s.Ops {
			sp := op.(operator.SingleProcessor)
			var next []types.Record
			for _, rec := range outs {
				next = append(next, sp.ProcessOne(rec)...)
			}
			timers[i].tick()
			metrics.RecordsProcessedTotal.WithLabelValues(s.Labels[i]).Add(float64(len(next)))
			outs = next
			if len(outs) == 0 {
				break // dropped by Filter (or errored in Process) — emit nothing
			}
		}
		for _, rec := range outs {
			if serr := sendRecord(hardCtx, out, rec); serr != nil {
				return serr
			}
		}
	}
}

// runParallel fans the input across Parallelism workers:
//
//	in → dispatcher ──┬── worker 0 ──┐
//	   (round-robin,  ├── worker 1 ──┼── alignedMerge → out
//	broadcast markers)└── worker N ──┘
func (s *StatelessStage) runParallel(hardCtx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	// stageCtx lets a failed worker unwind the dispatcher and merge
	// immediately instead of deadlocking on its abandoned channels.
	stageCtx, stageCancel := context.WithCancel(hardCtx)
	defer stageCancel()

	n := s.Parallelism
	workerIns := make([]chan types.Record, n)
	workerOuts := make([]chan types.Record, n)
	for i := range workerIns {
		workerIns[i] = make(chan types.Record, internalBuf)
		workerOuts[i] = make(chan types.Record, internalBuf)
	}

	// Dispatcher: round-robins data records, broadcasts markers to
	// every worker (C1 — each worker must see every barrier/watermark
	// so the merge can align them).
	go func() {
		defer func() {
			for _, ch := range workerIns {
				close(ch)
			}
		}()
		next := 0
		for {
			r, ok, err := recvRecord(stageCtx, in)
			if err != nil || !ok {
				return
			}
			if r.IsBarrier || r.IsWatermark {
				for _, ch := range workerIns {
					if sendRecord(stageCtx, ch, r) != nil {
						return
					}
				}
				continue
			}
			if sendRecord(stageCtx, workerIns[next], r) != nil {
				return
			}
			next = (next + 1) % n
		}
	}()

	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(workerIn <-chan types.Record, workerOut chan<- types.Record) {
			defer wg.Done()
			defer close(workerOut)
			if err := s.runWorker(stageCtx, workerIn, workerOut); err != nil {
				select {
				case errCh <- err:
				default:
				}
				stageCancel()
			}
		}(workerIns[w], workerOuts[w])
	}

	mergeErr := alignedMerge(stageCtx, workerOuts, out)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	return mergeErr
}
