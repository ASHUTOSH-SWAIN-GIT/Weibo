package mailer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/pipeline"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// DefaultEdgeCapacity is the default buffer size of the bounded edges
// between execution stages. Override with WithBufferSize.
const DefaultEdgeCapacity = 1024

// StreamExecutionEnv is the entry point for building and running stream pipelines.
// Create one with NewEnv(), define your pipeline using FromSource/ToSink,
// then call Execute() to run it.
//
//	env := mailer.NewEnv()
//	env.FromSource(src).Map(fn).Filter(fn).ToSink(stdout)
//	env.Execute(ctx)
type StreamExecutionEnv struct {
	source    source.Source
	sink      sink.Sink
	operators []operator.Operator

	checkpointInterval time.Duration
	checkpointStorage  checkpoint.Storage

	shutdownTimeout time.Duration
	edgeCapacity    int

	workerOps []operator.Operator
	workerMu  sync.Mutex
}

// NewEnv creates a new StreamExecutionEnv.
func NewEnv() *StreamExecutionEnv {
	return &StreamExecutionEnv{
		shutdownTimeout: 30 * time.Second,
		edgeCapacity:    DefaultEdgeCapacity,
	}
}

// WithShutdownTimeout sets how long the pipeline waits for in-flight
// records to drain before forcing shutdown (default 30s).
func (env *StreamExecutionEnv) WithShutdownTimeout(d time.Duration) *StreamExecutionEnv {
	env.shutdownTimeout = d
	return env
}

// WithBufferSize sets the capacity of the bounded edges between
// execution stages (default 1024). Larger buffers absorb bursts;
// smaller buffers propagate backpressure to the source sooner.
// Values < 1 are ignored.
func (env *StreamExecutionEnv) WithBufferSize(n int) *StreamExecutionEnv {
	if n > 0 {
		env.edgeCapacity = n
	}
	return env
}

// WithCheckpointing enables periodic checkpointing with the given interval
// and storage backend. Barriers are injected into the stream at the specified
// interval; when a barrier passes through all operators and reaches the sink,
// the checkpoint is complete.
//
// On recovery, Execute() will load the latest checkpoint, restore all stateful
// operators, and resume from the saved source offset.
//
// Example:
//
//	env := mailer.NewEnv()
//	env.WithCheckpointing(30*time.Second, checkpoint.NewFileStorage("/tmp/checkpoints"))
func (env *StreamExecutionEnv) WithCheckpointing(interval time.Duration, storage checkpoint.Storage) *StreamExecutionEnv {
	env.checkpointInterval = interval
	env.checkpointStorage = storage
	return env
}

// FromSource sets the data source for the pipeline and returns a Stream
// that you can chain operators on.
func (env *StreamExecutionEnv) FromSource(src source.Source) *Stream {
	env.source = src
	return &Stream{env: env}
}

// Execute runs the pipeline. Operators are grouped into execution
// stages (see the pipeline package); stages run concurrently,
// connected by bounded edges. A full edge blocks the upstream stage —
// backpressure propagates stage by stage back to the source, so a slow
// sink throttles ingestion instead of growing memory.
//
// Graceful shutdown (on context cancellation, C3 two-phase):
//  1. The source stops producing and flushes pending offset commits.
//  2. A final checkpoint barrier is injected (if checkpointing is on).
//  3. Channel closes cascade downstream; every stage drains in-flight
//     records — nothing is dropped.
//  4. The sink drains and the final checkpoint is saved.
//  5. Only if draining exceeds shutdownTimeout are blocked stages
//     forcibly aborted.
//
// Prometheus metrics are collected automatically during execution.
func (env *StreamExecutionEnv) Execute(ctx context.Context) error {
	if env.source == nil {
		return fmt.Errorf("mailer: no source configured, use FromSource()")
	}
	if env.sink == nil {
		return fmt.Errorf("mailer: no sink configured, use ToSink()")
	}

	metrics.PipelineRunning.Set(1)
	defer metrics.PipelineRunning.Set(0)

	// Phase A: restore the source offset before wiring, so the reader
	// knows where to resume.
	var savedCheckpoint *checkpoint.CheckpointData
	if env.checkpointStorage != nil {
		data, err := env.checkpointStorage.Load()
		if err != nil {
			fmt.Printf("mailer: checkpoint load failed (starting fresh): %v\n", err)
		} else if data != nil {
			env.restoreSourceOffset(data)
			savedCheckpoint = data
		}
	}

	plan, err := pipeline.BuildPlan(pipeline.PlanConfig{
		Source:       env.source,
		Operators:    env.operators,
		Labels:       env.operatorLabels(),
		Sink:         env.sink,
		DrainTimeout: env.shutdownTimeout,
		OnClone: func(op operator.Operator) {
			env.workerMu.Lock()
			env.workerOps = append(env.workerOps, op)
			env.workerMu.Unlock()
		},
	})
	if err != nil {
		return err
	}

	// Phase B: restore per-worker operator state. Keyed stages clone
	// their operators at plan time, so every clone exists before any
	// stage starts processing.
	if savedCheckpoint != nil {
		env.restoreWorkersFromCheckpoint(savedCheckpoint)
	}

	// Two-phase shutdown (C3). Cancelling ctx only stops the source;
	// the pipeline drains through cascading channel closes. hardCtx is
	// what unblocks stuck sends — it fires shutdownTimeout after ctx
	// is cancelled, or immediately on a fatal stage error.
	hardCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()
	pipelineDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			t := time.NewTimer(env.shutdownTimeout)
			defer t.Stop()
			select {
			case <-t.C:
				hardCancel()
			case <-pipelineDone:
			}
		case <-pipelineDone:
		}
	}()

	nStages := len(plan)
	edges := make([]*pipeline.Edge, nStages-1)
	for i := range edges {
		edges[i] = pipeline.NewEdge(fmt.Sprintf("edge-%d", i), env.edgeCapacity)
	}
	// Publish per-edge queue gauges: an edge pinned at capacity means
	// the stage downstream of it is the bottleneck.
	pipeline.SampleEdges(pipelineDone, edges)

	var wg sync.WaitGroup
	errs := make([]error, nStages)
	for i, stage := range plan {
		var in <-chan types.Record
		if i > 0 {
			in = edges[i-1].Ch
		}
		// Checkpoint barriers enter the stream right after the source
		// stage and are detected right before the sink stage, when
		// every operator upstream has forwarded them.
		if i == 1 && env.checkpointInterval > 0 {
			in = env.injectBarriers(ctx, in)
		}
		if i == nStages-1 {
			in = barrierDetect(in, env.saveCheckpoint)
		}
		var out chan<- types.Record
		if i < len(edges) {
			out = edges[i].Ch
		}

		wg.Add(1)
		go func(i int, st pipeline.Stage, in <-chan types.Record, out chan<- types.Record) {
			defer wg.Done()
			if err := st.Run(ctx, hardCtx, in, out); err != nil {
				errs[i] = err
				metrics.StageErrorsTotal.WithLabelValues(st.Name()).Inc()
				hardCancel() // fatal stage error: unwind the whole pipeline
			}
		}(i, stage, in, out)
	}
	wg.Wait()
	close(pipelineDone)

	for i, err := range errs {
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		if i == nStages-1 && ctx.Err() != nil {
			continue // sink error during graceful shutdown
		}
		return err
	}
	return nil
}

// operatorLabels returns a label string for each operator in the chain.
// Uses the user-provided label if set, otherwise the operator type name.
func (env *StreamExecutionEnv) operatorLabels() []string {
	labels := make([]string, len(env.operators))
	for i, op := range env.operators {
		if lab, ok := op.(operator.Labeled); ok && lab.GetLabel() != "" {
			labels[i] = lab.GetLabel()
		} else {
			labels[i] = op.Name()
		}
	}
	return labels
}

// barrierDetect wraps a read channel and calls saveCheckpoint
// whenever a barrier record passes through.  Barriers reach this
// wrapper only after every operator upstream has forwarded them,
// so operator state snapshots capture the correct point-in-time
// state (post-barrier Chandy-Lamport alignment).
func barrierDetect(in <-chan types.Record, save func(id string)) <-chan types.Record {
	out := make(chan types.Record, 256)
	go func() {
		defer close(out)
		for r := range in {
			out <- r
			if r.IsBarrier {
				save(r.CheckpointID)
			}
		}
	}()
	return out
}

// injectBarriers wraps a source channel and periodically injects checkpoint
// barriers into the stream. When a barrier reaches the end of the pipeline,
// all stateful operators snapshot their state and the checkpoint is saved.
func (env *StreamExecutionEnv) injectBarriers(ctx context.Context, sourceCh <-chan types.Record) <-chan types.Record {
	out := make(chan types.Record, 256)
	go func() {
		defer close(out)

		ticker := time.NewTicker(env.checkpointInterval)
		defer ticker.Stop()

		checkpointID := 0

		for {
			select {
			case <-ctx.Done():
				// Inject a final checkpoint barrier before draining so
				// state is saved on graceful shutdown.
				checkpointID++
				out <- types.NewBarrier(fmt.Sprintf("cp-%d-shutdown", checkpointID))

				for record := range sourceCh {
					out <- record
				}
				return

			case record, ok := <-sourceCh:
				if !ok {
					return
				}
				out <- record

			case <-ticker.C:
				checkpointID++
				id := fmt.Sprintf("cp-%d", checkpointID)

				// Inject barrier into the stream. The barrier flows
				// through all operators. When it reaches the end of the
				// operator chain, saveCheckpoint is triggered (see
				// barrierDetect). This ensures operator state is
				// captured AFTER all pre-barrier records are processed.
				out <- types.NewBarrier(id)
			}
		}
	}()

	return out
}

// saveCheckpoint captures a snapshot from all stateful operators
// and writes it to the checkpoint storage.
func (env *StreamExecutionEnv) saveCheckpoint(id string) {
	data := &checkpoint.CheckpointData{
		ID:        id,
		Timestamp: time.Now().UTC(),
		Operators: make(map[string][]byte),
		Source:    make(map[string][]byte),
	}

	for i, op := range env.operators {
		if snap, ok := op.(operator.Snapshotable); ok {
			snapshot, err := snap.Snapshot()
			if err != nil {
				fmt.Printf("mailer: checkpoint snapshot failed for operator %d: %v\n", i, err)
				continue
			}
			data.Operators[fmt.Sprintf("op-%d", i)] = snapshot
		}
	}

	// Snapshot per-worker operator instances created by keyed stages.
	env.workerMu.Lock()
	for i, op := range env.workerOps {
		if snap, ok := op.(operator.Snapshotable); ok {
			snapshot, err := snap.Snapshot()
			if err != nil {
				fmt.Printf("mailer: checkpoint worker-%d snapshot failed: %v\n", i, err)
				continue
			}
			data.Operators[fmt.Sprintf("worker-%d", i)] = snapshot
		}
	}
	env.workerMu.Unlock()

	if cps, ok := env.source.(source.CheckpointSource); ok {
		offset, err := cps.CheckpointOffset()
		if err != nil {
			fmt.Printf("mailer: checkpoint source offset failed: %v\n", err)
		} else {
			data.Source["offset"] = offset
		}
	}

	if err := env.checkpointStorage.Save(data); err != nil {
		fmt.Printf("mailer: checkpoint save failed: %v\n", err)
	}
}

// restoreSourceOffset restores the source offset from a checkpoint.
// Called before wiring so the Kafka reader knows where to resume.
func (env *StreamExecutionEnv) restoreSourceOffset(data *checkpoint.CheckpointData) {
	if data == nil {
		return
	}
	if cps, ok := env.source.(source.CheckpointSource); ok {
		if offsetData, exists := data.Source["offset"]; exists {
			if err := cps.RestoreOffset(offsetData); err != nil {
				fmt.Printf("mailer: restore source offset failed: %v\n", err)
			}
		}
	}
	fmt.Printf("mailer: restored from checkpoint %s\n", data.ID)
}

// restoreWorkersFromCheckpoint restores per-worker operator state for
// operator instances created by keyed stages. Called after the plan is
// built (which creates the worker clones) and before stages start.
func (env *StreamExecutionEnv) restoreWorkersFromCheckpoint(data *checkpoint.CheckpointData) {
	if data == nil {
		return
	}
	env.workerMu.Lock()
	defer env.workerMu.Unlock()
	for i, op := range env.workerOps {
		if snap, ok := op.(operator.Snapshotable); ok {
			key := fmt.Sprintf("worker-%d", i)
			if stateData, exists := data.Operators[key]; exists {
				if err := snap.Restore(stateData); err != nil {
					fmt.Printf("mailer: restore worker-%d failed: %v\n", i, err)
				}
			}
		}
	}
}
