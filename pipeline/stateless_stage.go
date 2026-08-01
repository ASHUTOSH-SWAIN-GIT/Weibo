package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
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
	sm := newStageMetrics(s.StageName, "stateless")
	par := s.Parallelism
	if par < 1 {
		par = 1
	}
	defer sm.setWorkers(par)()
	if par == 1 {
		return s.runWorker(hardCtx, in, out, sm)
	}
	return s.runParallel(hardCtx, in, out, sm)
}

// runWorker is the execution loop shared by the serial path (reading
// the stage input directly) and each parallel worker (reading its own
// dispatched channel). Markers are forwarded as-is: in the serial
// path they stay in order; in the parallel path they land in the
// worker's output for the aligned merge to dedup.
//
// sm is non-nil only in the serial path, where this loop IS the stage
// boundary; parallel workers write to internal channels and the
// dispatcher/merge do the stage-level accounting instead.
func (s *StatelessStage) runWorker(hardCtx context.Context, in <-chan types.Record, out chan<- types.Record, sm *stageMetrics) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stage %s: operator panicked: %v", s.StageName, r)
		}
	}()

	// Resolve per-op handles once, not per record: the SingleProcessor type
	// assertion, the latency batcher, and the label→counter lookup (kept as a
	// bound Add method value) are all hot-loop-invariant.
	sps := make([]operator.SingleProcessor, len(s.Ops))
	timers := make([]*latencyBatcher, len(s.Ops))
	procAdd := make([]func(float64), len(s.Ops))
	for i := range s.Ops {
		sps[i] = s.Ops[i].(operator.SingleProcessor)
		l := s.Labels[i]
		timers[i] = newLatencyBatcher(func(avg float64) {
			metrics.OperatorLatencySeconds.WithLabelValues(l).Observe(avg)
		})
		procAdd[i] = metrics.RecordsProcessedTotal.WithLabelValues(l).Add
	}
	defer func() {
		for _, t := range timers {
			t.flush()
		}
	}()

	send := sendRecord
	if sm != nil {
		send = sm.send
	}

	// bufA/bufB are reused across records as a double buffer so chaining
	// operators no longer allocates a fresh output slice per record per op.
	var bufA, bufB []types.Record

	for {
		r, ok, rerr := recvRecord(hardCtx, in)
		if rerr != nil {
			return rerr
		}
		if !ok {
			return nil
		}
		if sm != nil {
			sm.countIn(r)
		}
		if r.IsBarrier || r.IsWatermark {
			if serr := send(hardCtx, out, r); serr != nil {
				return serr
			}
			continue
		}
		cur := append(bufA[:0], r)
		alt := bufB
		for i, sp := range sps {
			alt = alt[:0]
			for _, rec := range cur {
				alt = append(alt, sp.ProcessOne(rec)...)
			}
			timers[i].tick()
			procAdd[i](float64(len(alt)))
			cur, alt = alt, cur
			if len(cur) == 0 {
				break // dropped by Filter (or errored in Process) — emit nothing
			}
		}
		for _, rec := range cur {
			if serr := send(hardCtx, out, rec); serr != nil {
				return serr
			}
		}
		// Persist both buffers (with their grown capacity) for the next record.
		bufA, bufB = cur, alt
	}
}

// runParallel fans the input across Parallelism workers:
//
//	in → dispatcher ──┬── worker 0 ──┐
//	   (round-robin,  ├── worker 1 ──┼── alignedMerge → out
//	broadcast markers)└── worker N ──┘
func (s *StatelessStage) runParallel(hardCtx context.Context, in <-chan types.Record, out chan<- types.Record, sm *stageMetrics) error {
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
			sm.countIn(r)
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
			if err := s.runWorker(stageCtx, workerIn, workerOut, nil); err != nil {
				select {
				case errCh <- err:
				default:
				}
				stageCancel()
			}
		}(workerIns[w], workerOuts[w])
	}

	mergeErr := alignedMerge(stageCtx, workerOuts, out, sm)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	return mergeErr
}
