package mailer

import (
	"context"
	"fmt"
	"time"

	"mailer/checkpoint"
	"mailer/operator"
	"mailer/sink"
	"mailer/source"
	"mailer/types"
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

	// Checkpointing configuration.
	checkpointInterval time.Duration
	checkpointStorage  checkpoint.Storage
}

// NewEnv creates a new StreamExecutionEnv.
func NewEnv() *StreamExecutionEnv {
	return &StreamExecutionEnv{}
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
// If checkpointing is enabled, the pipeline will attempt to restore from
// the latest checkpoint before starting. A goroutine injects checkpoint
// barriers at the configured interval. When a barrier completes the full
// pipeline round-trip, operator state is saved to the checkpoint storage.
func (env *StreamExecutionEnv) Execute(ctx context.Context) error {
	if env.source == nil {
		return fmt.Errorf("mailer: no source configured, use FromSource()")
	}
	if env.sink == nil {
		return fmt.Errorf("mailer: no sink configured, use ToSink()")
	}

	// Attempt recovery from checkpoint.
	if env.checkpointStorage != nil {
		if err := env.restoreFromCheckpoint(); err != nil {
			fmt.Printf("mailer: checkpoint restore failed (starting fresh): %v\n", err)
		}
	}

	// Source writes into the first channel.
	sourceCh := make(chan types.Record, 256)
	go func() {
		defer close(sourceCh)
		if err := env.source.Run(ctx, sourceCh); err != nil {
			fmt.Printf("mailer: source error: %v\n", err)
		}
	}()

	// If checkpointing is enabled, wrap the source channel to inject barriers.
	var recordCh <-chan types.Record
	if env.checkpointInterval > 0 {
		recordCh = env.injectBarriers(ctx, sourceCh)
	} else {
		recordCh = sourceCh
	}

	// Wire operators: each reads from current, writes to next.
	current := recordCh

	for _, op := range env.operators {
		next := make(chan types.Record, 256)
		go func(op operator.Operator, in <-chan types.Record, out chan<- types.Record) {
			op.Process(in, out)
		}(op, current, next)
		current = next
	}

	// Final stage: sink reads from the last channel.
	return env.sink.Write(ctx, current)
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
				// Drain remaining records on cancellation.
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

				// Inject barrier into the stream.
				out <- types.NewBarrier(id)

				// Collect snapshots from all stateful operators.
				env.saveCheckpoint(id)
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

	// Snapshot each stateful operator.
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

	// Snapshot source offset if it supports checkpointing.
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

// restoreFromCheckpoint loads the latest checkpoint and restores
// all stateful operators from their saved state.
func (env *StreamExecutionEnv) restoreFromCheckpoint() error {
	data, err := env.checkpointStorage.Load()
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}
	if data == nil {
		// No checkpoint found, start fresh.
		return nil
	}

	// Restore each stateful operator.
	for i, op := range env.operators {
		if snap, ok := op.(operator.Snapshotable); ok {
			key := fmt.Sprintf("op-%d", i)
			if stateData, exists := data.Operators[key]; exists {
				if err := snap.Restore(stateData); err != nil {
					return fmt.Errorf("restore operator %d: %w", i, err)
				}
			}
		}
	}

	// Restore source offset if supported.
	if cps, ok := env.source.(source.CheckpointSource); ok {
		if offsetData, exists := data.Source["offset"]; exists {
			if err := cps.RestoreOffset(offsetData); err != nil {
				return fmt.Errorf("restore source offset: %w", err)
			}
		}
	}

	fmt.Printf("mailer: restored from checkpoint %s\n", data.ID)
	return nil
}
