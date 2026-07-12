package mailer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

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

	workerOps []operator.Operator
	workerMu  sync.Mutex
}

// NewEnv creates a new StreamExecutionEnv.
func NewEnv() *StreamExecutionEnv {
	return &StreamExecutionEnv{shutdownTimeout: 30 * time.Second}
}

// WithShutdownTimeout sets how long the pipeline waits for in-flight
// records to drain before forcing shutdown (default 30s).
func (env *StreamExecutionEnv) WithShutdownTimeout(d time.Duration) *StreamExecutionEnv {
	env.shutdownTimeout = d
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

// Execute runs the pipeline. It starts the source, wires up all operators
// as goroutines connected by channels, and connects the final output to the sink.
// Blocks until the source is exhausted or the context is cancelled.
//
// Graceful shutdown (on context cancellation):
//  1. Source stops accepting new records
//  2. Source flushes pending offset commits
//  3. Operators drain buffered records
//  4. Sink drains and flushes remaining records (up to shutdownTimeout)
//  5. Checkpoint is saved (if enabled)
//  6. Returns
//
// Prometheus metrics are collected automatically during execution.
//
// If checkpointing is enabled, the pipeline will attempt to restore from
// the latest checkpoint before starting. A goroutine injects checkpoint
// barriers at the configured interval.
func (env *StreamExecutionEnv) Execute(ctx context.Context) error {
	if env.source == nil {
		return fmt.Errorf("mailer: no source configured, use FromSource()")
	}
	if env.sink == nil {
		return fmt.Errorf("mailer: no sink configured, use ToSink()")
	}

	metrics.PipelineRunning.Set(1)
	defer metrics.PipelineRunning.Set(0)

	// Phase A: restore source offset before wiring (so the
	// reader knows where to resume). Worker state is restored
	// later because the worker instances don't exist yet.
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

	labels := env.operatorLabels()

	sourceCh := make(chan types.Record, 256)
	go func() {
		defer close(sourceCh)
		if err := env.source.Run(ctx, sourceCh); err != nil {
			if ctx.Err() == nil {
				metrics.SourceErrorsTotal.Inc()
			}
			fmt.Printf("mailer: source error: %v\n", err)
		}
		// Flush pending offset commits before operators drain.
		if d, ok := env.source.(source.Drainable); ok {
			flushCtx, cancel := context.WithTimeout(context.Background(), env.shutdownTimeout)
			defer cancel()
			if err := d.Drain(flushCtx); err != nil {
				fmt.Printf("mailer: source drain error: %v\n", err)
			}
		}
	}()

	var recordCh <-chan types.Record
	if env.checkpointInterval > 0 {
		recordCh = env.injectBarriers(ctx, sourceCh)
	} else {
		recordCh = sourceCh
	}

	current := countedRead(recordCh, func() { metrics.RecordsReadTotal.Inc() })

	for i := 0; i < len(env.operators); i++ {
		op := env.operators[i]
		label := labels[i]

		if kb, ok := op.(*operator.KeyByOperator); ok && kb.IsRouter() {
			stageOps, skip := takeStateful(env.operators[i+1:])
			i += skip
			current = env.wireKeyedStage(ctx, kb, stageOps, current)
			continue
		}

		next := make(chan types.Record, 256)
		go func(op operator.Operator, in <-chan types.Record, out chan<- types.Record) {
			op.Process(in, out)
		}(op, current, next)

		current = timedRead(
			countedRead(next, func() {
				metrics.RecordsProcessedTotal.WithLabelValues(label).Inc()
			}),
			label,
		)
	}

	// Phase B: restore worker instances now that the keyed stage
	// has created them via Clone().
	if savedCheckpoint != nil {
		env.restoreWorkersFromCheckpoint(savedCheckpoint)
	}

	// Wrap the final channel to detect checkpoint barriers.
	// When a barrier reaches the end of the operator chain, all
	// in-flight records before the barrier have been processed,
	// so this is the correct time to snapshot operator state.
	current = barrierDetect(current, env.saveCheckpoint)

	// Give sink a drain-only context so it doesn't bail early when
	// the caller's context is cancelled.
	start := time.Now()
	err := env.sink.Write(ctx, countedRead(current, func() { metrics.RecordsWrittenTotal.Inc() }))
	metrics.SinkWriteLatencySeconds.Observe(time.Since(start).Seconds())

	if err != nil && ctx.Err() != nil {
		return nil // graceful shutdown
	}
	if err != nil {
		metrics.SinkErrorsTotal.Inc()
	}
	return err
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

// countedRead wraps a read channel so every record triggers incr.
func countedRead(in <-chan types.Record, incr func()) <-chan types.Record {
	out := make(chan types.Record, 256)
	go func() {
		defer close(out)
		for r := range in {
			incr()
			out <- r
		}
	}()
	return out
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

// takeStateful collects consecutive Cloneable (stateful) operators
// from the given slice. Returns the collected operators and the
// number of operators consumed.
func takeStateful(ops []operator.Operator) ([]operator.Operator, int) {
	var stateful []operator.Operator
	for _, op := range ops {
		if _, ok := op.(operator.Cloneable); ok {
			stateful = append(stateful, op)
		} else {
			return stateful, len(stateful)
		}
	}
	return stateful, len(stateful)
}

// wireKeyedStage builds the router → workers → merger topology.
//
//	                        ┌── Worker 0: Op₀_clone → Op₁_clone → ... ──┐
//	current ── Router ──────┼── Worker 1: Op₀_clone → Op₁_clone → ... ──┼── merged → downstream
//	                        └── Worker 2: Op₀_clone → Op₁_clone → ... ──┘
//
// Each worker gets its own clone of every stateful operator in the
// stage, with an isolated state backend.  The router hash-dispatches
// records so the same key always reaches the same worker.
func (env *StreamExecutionEnv) wireKeyedStage(
	ctx context.Context,
	kb *operator.KeyByOperator,
	stageOps []operator.Operator,
	in <-chan types.Record,
) <-chan types.Record {

	n := kb.Partitions
	workerIns := make([]chan types.Record, n)
	workerOuts := make([]chan types.Record, n)
	for i := 0; i < n; i++ {
		workerIns[i] = make(chan types.Record, 256)
		workerOuts[i] = make(chan types.Record, 256)
	}

	// Router goroutine: hash-dispatches records to workers.
	go func() {
		defer func() {
			for _, ch := range workerIns {
				close(ch)
			}
		}()
		for r := range in {
			w := kb.Route(r)
			workerIns[w] <- r
		}
	}()

	var wg sync.WaitGroup

	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(workerID int, inCh <-chan types.Record, outCh chan<- types.Record) {
			defer wg.Done()
			defer close(outCh)

			prev := inCh
			for _, op := range stageOps {
				clone := op.(operator.Cloneable).Clone()
				env.workerMu.Lock()
				env.workerOps = append(env.workerOps, clone)
				env.workerMu.Unlock()
				next := make(chan types.Record, 256)
				go clone.Process(prev, next)
				prev = next
			}
			for r := range prev {
				outCh <- r
			}
		}(w, workerIns[w], workerOuts[w])
	}

	// Merger: reads all worker outputs concurrently.
	merged := make(chan types.Record, 256)
	go func() {
		defer close(merged)
		var wgMerge sync.WaitGroup
		for _, ch := range workerOuts {
			wgMerge.Add(1)
			go func(c <-chan types.Record) {
				defer wgMerge.Done()
				for r := range c {
					merged <- r
				}
			}(ch)
		}
		wgMerge.Wait()
	}()

	return merged
}

// timedRead measures per-record latency through an operator by batching
// 100 records and recording the average time per record.
func timedRead(in <-chan types.Record, label string) <-chan types.Record {
	out := make(chan types.Record, 256)
	go func() {
		defer close(out)
		const batchSize = 100
		var n int
		start := time.Now()
		for r := range in {
			n++
			out <- r
			if n >= batchSize {
				avg := time.Since(start).Seconds() / float64(n)
				metrics.OperatorLatencySeconds.WithLabelValues(label).Observe(avg)
				n = 0
				start = time.Now()
			}
		}
		if n > 0 {
			avg := time.Since(start).Seconds() / float64(n)
			metrics.OperatorLatencySeconds.WithLabelValues(label).Observe(avg)
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
				// barrierDetect below). This ensures operator state is
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
// operator instances created by keyed stages. Called after workers
// are created via wireKeyedStage.
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
