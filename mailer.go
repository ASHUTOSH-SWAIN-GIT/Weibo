package mailer

import (
	"context"
	"fmt"

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
}

// NewEnv creates a new StreamExecutionEnv.
func NewEnv() *StreamExecutionEnv {
	return &StreamExecutionEnv{}
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
func (env *StreamExecutionEnv) Execute(ctx context.Context) error {
	if env.source == nil {
		return fmt.Errorf("mailer: no source configured, use FromSource()")
	}
	if env.sink == nil {
		return fmt.Errorf("mailer: no sink configured, use ToSink()")
	}

	// Source writes into the first channel.
	sourceCh := make(chan types.Record, 256)
	go func() {
		defer close(sourceCh)
		if err := env.source.Run(ctx, sourceCh); err != nil {
			fmt.Printf("mailer: source error: %v\n", err)
		}
	}()

	// Wire operators: each reads from current, writes to next.
	current := (<-chan types.Record)(sourceCh)

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
