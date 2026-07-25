package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// internalBuf is the capacity of channels internal to a stage
// (worker inputs/outputs, fan-in). Edges between stages use the
// configurable env buffer size instead.
const internalBuf = 256

// Stage is one execution unit of a pipeline. Stages run concurrently,
// connected by bounded edges.
//
// Contract:
//   - Run consumes in until it is closed and closes out before
//     returning (C2: the writer owns the close). The wiring never
//     closes edges.
//   - runCtx is the caller's context: only the source stage reacts to
//     it, by stopping production. Downstream stages drain via the
//     cascading channel closes that follow (C3).
//   - hardCtx aborts blocked sends/reads; it fires only on
//     shutdown-timeout expiry or a fatal error in another stage.
type Stage interface {
	Name() string
	Run(runCtx, hardCtx context.Context, in <-chan types.Record, out chan<- types.Record) error
}

// SourceStage wraps a source.Source as the first stage of a plan.
type SourceStage struct {
	Source source.Source

	// DrainTimeout bounds the source's offset flush on shutdown.
	DrainTimeout time.Duration
}

func (s *SourceStage) Name() string { return "source" }

// Run starts the source and forwards its records to the output edge.
// Source errors are reported via metrics/log but do not fail the
// pipeline (parity with previous behavior — the stream simply ends).
func (s *SourceStage) Run(runCtx, hardCtx context.Context, _ <-chan types.Record, out chan<- types.Record) error {
	defer close(out)
	sm := newStageMetrics(s.Name(), "source")
	defer sm.setWorkers(1)()

	raw := make(chan types.Record, internalBuf)
	go func() {
		defer close(raw)
		if err := s.Source.Run(runCtx, raw); err != nil {
			if runCtx.Err() == nil {
				metrics.SourceErrorsTotal.Inc()
			}
			fmt.Printf("weibo: source error: %v\n", err)
		}
		// Flush pending offset commits before downstream drains.
		if d, ok := s.Source.(source.Drainable); ok {
			flushCtx, cancel := context.WithTimeout(context.Background(), s.DrainTimeout)
			defer cancel()
			if err := d.Drain(flushCtx); err != nil {
				fmt.Printf("weibo: source drain error: %v\n", err)
			}
		}
	}()

	for {
		r, ok, err := recvRecord(hardCtx, raw)
		if err != nil {
			go func() {
				for range raw {
				}
			}()
			return err
		}
		if !ok {
			return nil
		}
		metrics.RecordsReadTotal.Inc()
		sm.countIn(r)
		if err := sm.send(hardCtx, out, r); err != nil {
			go func() {
				for range raw {
				}
			}()
			return err
		}
	}
}

// SinkStage wraps a sink.Sink as the last stage of a plan.
type SinkStage struct {
	Sink sink.Sink

	// OnBarrier, when set (uncoordinated checkpointing), is invoked
	// for each barrier only after the sink has drained every record
	// ahead of it — so a checkpoint is never durable for records the
	// sink never received. Coordinated (exactly-once) pipelines leave
	// this nil; the sink itself acknowledges barriers there.
	OnBarrier func(checkpointID string)
}

func (s *SinkStage) Name() string { return "sink" }

// Run pumps records from the input edge into the sink. The sink gets
// hardCtx so it keeps draining through graceful shutdown and is only
// interrupted by the shutdown timeout (C3).
func (s *SinkStage) Run(runCtx, hardCtx context.Context, in <-chan types.Record, _ chan<- types.Record) error {
	sm := newStageMetrics(s.Name(), "sink")
	defer sm.setWorkers(1)()

	pumped := make(chan types.Record, internalBuf)
	done := make(chan error, 1)
	go func() {
		start := time.Now()
		err := s.Sink.Write(hardCtx, pumped)
		metrics.SinkWriteLatencySeconds.Observe(time.Since(start).Seconds())
		done <- err
	}()

	var sinkErr error
	received := false
	pumping := true
	for r := range in {
		sm.countIn(r)
		if !pumping {
			continue // forced shutdown: discard while upstream unwinds
		}

		// Uncoordinated checkpoint alignment: before reporting a
		// barrier, wait until the sink's Write loop has dequeued
		// every record ahead of it. Saving earlier would make source
		// offsets durable for records still sitting in this buffer —
		// lost on crash, violating at-least-once.
		if r.IsBarrier && s.OnBarrier != nil {
			for pumping && len(pumped) > 0 {
				select {
				case sinkErr = <-done:
					metrics.SinkErrorsTotal.Inc()
					return sinkErr
				case <-hardCtx.Done():
					pumping = false
				case <-time.After(200 * time.Microsecond):
				}
			}
			if pumping {
				s.OnBarrier(r.CheckpointID)
			} else {
				continue
			}
		}

		select {
		case pumped <- r:
			metrics.RecordsWrittenTotal.Inc()
			sm.countOut(r)
		case sinkErr = <-done:
			// Sink died mid-stream. Return immediately so Execute can
			// cancel the pipeline — upstream is blocked on full edges
			// and only hardCtx will release it.
			metrics.SinkErrorsTotal.Inc()
			return sinkErr
		case <-hardCtx.Done():
			pumping = false // upstream is aborting; drain in until it closes
		}
	}
	close(pumped)
	if !received {
		sinkErr = <-done
	}
	if sinkErr != nil {
		metrics.SinkErrorsTotal.Inc()
	}
	return sinkErr
}

// ChannelStage runs a single channel-based operator that is neither a
// SingleProcessor nor part of a keyed stage (e.g. Window or Reduce
// used without KeyBy). It preserves the old per-operator execution
// model for that operator alone.
type ChannelStage struct {
	Op        operator.Operator
	Label     string
	StageName string // unique name assigned by the planner
}

func (s *ChannelStage) Name() string {
	if s.StageName != "" {
		return s.StageName
	}
	return "op-" + s.Op.Name()
}

func (s *ChannelStage) Run(runCtx, hardCtx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	defer close(out)
	sm := newStageMetrics(s.Name(), "channel")
	defer sm.setWorkers(1)()

	countedIn := make(chan types.Record, internalBuf)
	go func() {
		defer close(countedIn)
		for r := range in {
			sm.countIn(r)
			countedIn <- r
		}
	}()

	mid := make(chan types.Record, internalBuf)
	go s.Op.Process(countedIn, mid) // Process closes mid when countedIn closes

	lat := newLatencyBatcher(func(avg float64) {
		metrics.OperatorLatencySeconds.WithLabelValues(s.Label).Observe(avg)
	})
	for {
		r, ok, err := recvRecord(hardCtx, mid)
		if err != nil || !ok {
			lat.flush()
			if err != nil {
				// Forced shutdown: unblock the abandoned operator so
				// its goroutine can exit once its input closes.
				go func() {
					for range mid {
					}
				}()
			}
			return err
		}
		metrics.RecordsProcessedTotal.WithLabelValues(s.Label).Inc()
		lat.tick()
		if err := sm.send(hardCtx, out, r); err != nil {
			lat.flush()
			go func() {
				for range mid {
				}
			}()
			return err
		}
	}
}
