package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// KeyedStage executes stateful operators (Window, Reduce) with keyed
// parallelism:
//
//	          ┌── worker 0: Op₀_clone → Op₁_clone ──┐
//	in ── Router ── worker 1: Op₀_clone → Op₁_clone ──┼── alignedMerge → out
//	          └── worker N: Op₀_clone → Op₁_clone ──┘
//
// Each worker owns a clone of every stateful operator with an isolated
// state backend. The router hash-dispatches data records (same key →
// same worker) and broadcasts barriers/watermarks to all workers; the
// merge emits each marker exactly once after all workers deliver it (C1).
type KeyedStage struct {
	KeyBy *operator.KeyByOperator

	// StageName is the unique name assigned by the planner (metrics label).
	StageName string

	// workers[w] is worker w's cloned operator chain. Clones are
	// created eagerly at plan time so checkpoint state can be
	// restored into them before the stage starts running.
	workers [][]operator.Operator
}

// NewKeyedStage clones the stateful operator chain once per partition.
// hooks.OnClone registers every clone for checkpoint restore and
// returns its global index; hooks.OnSnapshot receives each clone's
// state captured synchronously when a barrier passes through it — the
// race-free snapshot point (snapshotting later, at the end of the
// pipeline, races with the clone processing post-barrier records);
// hooks.StateBackendFor injects each clone's state backend, keyed by
// the same "worker-<idx>" owner ID the checkpoint format uses.
func NewKeyedStage(kb *operator.KeyByOperator, ops []operator.Operator, hooks StageHooks) (*KeyedStage, error) {
	workers := make([][]operator.Operator, kb.Partitions)
	for w := range workers {
		chain := make([]operator.Operator, 0, len(ops))
		for _, op := range ops {
			clone := op.(operator.Cloneable).Clone()
			idx := -1
			if hooks.OnClone != nil {
				idx = hooks.OnClone(clone)
			}
			if idx >= 0 {
				key := fmt.Sprintf("worker-%d", idx)
				if err := hooks.assignBackend(clone, key); err != nil {
					return nil, err
				}
				if bs, ok := clone.(operator.BarrierSnapshotter); ok && hooks.OnSnapshot != nil {
					onSnapshot := hooks.OnSnapshot
					bs.SetBarrierSnapshot(func(id string, snap []byte, err error) {
						if err != nil {
							fmt.Printf("mailer: barrier snapshot failed for %s: %v\n", key, err)
							return
						}
						onSnapshot(id, key, snap)
					})
				}
			}
			chain = append(chain, clone)
		}
		workers[w] = chain
	}
	return &KeyedStage{KeyBy: kb, workers: workers}, nil
}

func (s *KeyedStage) Name() string {
	if s.StageName != "" {
		return s.StageName
	}
	return "keyed"
}

func (s *KeyedStage) Run(runCtx, hardCtx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	defer close(out)

	n := len(s.workers)
	sm := newStageMetrics(s.Name(), "keyed")
	defer sm.setWorkers(n)()

	// stageCtx: a worker panic cancels the whole stage so the router,
	// remaining workers, and merge unwind instead of deadlocking.
	stageCtx, stageCancel := context.WithCancel(hardCtx)
	defer stageCancel()

	workerIns := make([]chan types.Record, n)
	workerOuts := make([]chan types.Record, n)
	for i := 0; i < n; i++ {
		workerIns[i] = make(chan types.Record, internalBuf)
		workerOuts[i] = make(chan types.Record, internalBuf)
	}
	errCh := make(chan error, n)

	// Router: hash-dispatches data, broadcasts markers (C1).
	go func() {
		defer func() {
			for _, ch := range workerIns {
				close(ch)
			}
		}()
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
			if sendRecord(stageCtx, workerIns[s.KeyBy.Route(r)], r) != nil {
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(workerID int, inCh <-chan types.Record, outCh chan<- types.Record) {
			defer wg.Done()
			defer close(outCh)
			defer func() {
				if r := recover(); r != nil {
					select {
					case errCh <- fmt.Errorf("worker %d panicked: %v", workerID, r):
					default:
					}
					stageCancel()
				}
			}()

			wLabel := strconv.Itoa(workerID)
			prev := inCh

			chain := s.workers[workerID]
			if len(chain) > 0 {
				opName := chain[0].Name()
				prev = workerCountedRead(prev, func() {
					metrics.OperatorWorkerRecordsIn.WithLabelValues(opName, wLabel).Inc()
				})
			}

			for _, clone := range chain {
				opName := clone.Name()
				next := make(chan types.Record, internalBuf)
				go clone.Process(prev, next)

				prev = workerTimedRead(
					workerCountedRead(next, func() {
						metrics.OperatorWorkerRecordsOut.WithLabelValues(opName, wLabel).Inc()
					}),
					opName, wLabel,
				)
			}
			for r := range prev {
				if sendRecord(stageCtx, outCh, r) != nil {
					// Forced shutdown: the cloned operator chain above
					// is blocked writing into its internal channels.
					// Drain prev so the chain unwinds once the router
					// closes the worker input.
					go func() {
						for range prev {
						}
					}()
					return
				}
			}
		}(w, workerIns[w], workerOuts[w])
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

// workerCountedRead wraps a read channel so every record triggers incr.
// Used for per-worker-operator counters inside keyed workers, whose
// operators are channel-based and therefore still need wrappers.
func workerCountedRead(in <-chan types.Record, incr func()) <-chan types.Record {
	out := make(chan types.Record, internalBuf)
	go func() {
		defer close(out)
		for r := range in {
			incr()
			out <- r
		}
	}()
	return out
}

// workerTimedRead measures per-worker per-operator latency.
func workerTimedRead(in <-chan types.Record, opName, workerID string) <-chan types.Record {
	out := make(chan types.Record, internalBuf)
	go func() {
		defer close(out)
		lat := newLatencyBatcher(func(avg float64) {
			metrics.OperatorWorkerLatencySeconds.WithLabelValues(opName, workerID).Observe(avg)
		})
		for r := range in {
			lat.tick()
			out <- r
		}
		lat.flush()
	}()
	return out
}
