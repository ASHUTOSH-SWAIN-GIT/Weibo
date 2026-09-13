package operator

import (
	"context"
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// ProcFailurePolicy determines what happens when a Process function
// returns an error. Follows the same pattern as sink FailurePolicy.
type ProcFailurePolicy int

const (
	ProcFailureDrop ProcFailurePolicy = iota
	ProcFailureDLQ
	ProcFailureFail
)

// RecordSink is a minimal sink interface used to forward failed
// records to a dead-letter queue. Implementations include
// a KafkaSink, PostgresSink, or any custom handler.
type RecordSink interface {
	Write(ctx context.Context, r types.Record) error
}

// ProcessOperator wraps a user function that may return an error.
// On error, the failure policy is applied: drop the record, forward
// it to a DLQ, or fail the pipeline.
//
// Watermarks and barriers pass through unchanged.
type ProcessOperator struct {
	Parallelizable
	Fn            func(types.Record) (types.Record, error)
	Label         string
	FailurePolicy ProcFailurePolicy
	DLQ           RecordSink
}

// NewProcess creates a ProcessOperator. Use ProcessOption functions
// to configure the failure policy and DLQ.
func NewProcess(fn func(types.Record) (types.Record, error), opts ...ProcessOption) *ProcessOperator {
	op := &ProcessOperator{Fn: fn, FailurePolicy: ProcFailureDrop}
	for _, opt := range opts {
		opt(op)
	}
	return op
}

// ProcessOption configures a ProcessOperator.
type ProcessOption func(*ProcessOperator)

// WithProcessLabel sets a human-readable label for the dashboard.
func WithProcessLabel(l string) ProcessOption {
	return func(op *ProcessOperator) { op.Label = l }
}

// WithProcessFailurePolicy sets what happens when the function
// returns an error. Default is ProcFailureDrop.
func WithProcessFailurePolicy(p ProcFailurePolicy) ProcessOption {
	return func(op *ProcessOperator) { op.FailurePolicy = p }
}

// WithProcessDLQ sets the dead-letter queue for failed records.
// Only used when failure policy is ProcFailureDLQ.
func WithProcessDLQ(dlq RecordSink) ProcessOption {
	return func(op *ProcessOperator) { op.DLQ = dlq }
}

func (op *ProcessOperator) Name() string     { return "Process" }
func (op *ProcessOperator) GetLabel() string { return op.Label }
func (op *ProcessOperator) DescribeOp() OperatorMeta {
	return OperatorMeta{Type: "Process", Label: op.Label}
}

// Process reads each record, calls the user function, and handles
// errors according to the configured failure policy.
func (op *ProcessOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	if err := op.ProcessE(context.Background(), in, out); err != nil {
		panic(err)
	}
}

func (op *ProcessOperator) ProcessE(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	defer close(out)
	for {
		var r types.Record
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok = <-in:
			if !ok {
				return nil
			}
		}
		if r.IsWatermark || r.IsBarrier {
			select {
			case out <- r:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		result, err := op.Fn(r)
		if err != nil {
			if err := op.handleFailure(ctx, r, err); err != nil {
				return err
			}
			continue
		}
		select {
		case out <- result:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ProcessOne applies the user function to a single record, handling
// errors via the configured failure policy.
func (op *ProcessOperator) ProcessOne(r types.Record) []types.Record {
	out, err := op.ProcessOneE(context.Background(), r)
	if err != nil {
		panic(err)
	}
	return out
}

func (op *ProcessOperator) ProcessOneE(ctx context.Context, r types.Record) ([]types.Record, error) {
	result, err := op.Fn(r)
	if err != nil {
		if err := op.handleFailure(ctx, r, err); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return []types.Record{result}, nil
}

func (op *ProcessOperator) handleFailure(ctx context.Context, r types.Record, err error) error {
	switch op.FailurePolicy {
	case ProcFailureDrop:
		return nil

	case ProcFailureDLQ:
		if op.DLQ == nil {
			return &OperatorError{Operator: op.Name(), Label: op.Label, Key: r.Key, Err: fmt.Errorf("DLQ is nil: %w", err)}
		}
		// Attach error info to headers for the DLQ consumer.
		r = r.WithHeader("_error", []byte(err.Error()))
		if werr := op.DLQ.Write(ctx, r); werr != nil {
			return &OperatorError{Operator: op.Name(), Label: op.Label, Key: r.Key, Err: fmt.Errorf("DLQ write failed: %w", werr)}
		}
		return nil

	case ProcFailureFail:
		return &OperatorError{Operator: op.Name(), Label: op.Label, Key: r.Key, Err: err}
	}
	return nil
}
