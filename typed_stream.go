package weibo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// TypedStream is a type-safe SDK layer over the raw Record runtime. The engine
// still moves types.Record internally; typed operators decode from Record.Parsed
// or Record.Value at the boundary and then write both Parsed and JSON Value back
// to the record so untyped operators and existing sinks keep working.
type TypedStream[T any] struct {
	stream *Stream
}

// TypedRecord is a typed payload plus the Record metadata needed for migration
// and interoperability. It is useful at typed source/sink boundaries where user
// code needs keys, timestamps, headers, offsets, or source identity.
type TypedRecord[T any] struct {
	Key       []byte
	Value     T
	Timestamp time.Time
	Offset    int64
	Partition int
	Source    string
	Headers   map[string][]byte
}

// FromTypedSource starts a typed stream from any existing source. Sources that
// already populate Record.Parsed avoid JSON decoding; otherwise Value is decoded
// as JSON into T when typed operators run.
func FromTypedSource[T any](env *StreamExecutionEnv, src source.Source) *TypedStream[T] {
	return &TypedStream[T]{stream: env.FromSource(src)}
}

// FromTypedRecords starts a typed stream from in-memory typed records.
func FromTypedRecords[T any](env *StreamExecutionEnv, records []TypedRecord[T]) *TypedStream[T] {
	return FromTypedSource[T](env, typedSliceSource[T]{records: records})
}

// FromTypedValues starts a typed stream from in-memory values. Record metadata
// is filled with SDK defaults (current timestamp and incrementing offset).
func FromTypedValues[T any](env *StreamExecutionEnv, values []T) *TypedStream[T] {
	records := make([]TypedRecord[T], len(values))
	for i, v := range values {
		records[i] = TypedRecord[T]{Value: v, Offset: int64(i), Timestamp: time.Now().UTC()}
	}
	return FromTypedRecords(env, records)
}

// AsTyped wraps an existing untyped stream with typed operators.
func AsTyped[T any](stream *Stream) *TypedStream[T] {
	return &TypedStream[T]{stream: stream}
}

// Untyped returns the underlying raw stream for migration and interoperability.
func (s *TypedStream[T]) Untyped() *Stream { return s.stream }

// Filter keeps records whose typed value satisfies fn. Decode failures are
// treated as operator errors and fail the pipeline with record context.
func (s *TypedStream[T]) Filter(fn func(T) bool, label ...string) *TypedStream[T] {
	op := &typedFilterOperator[T]{fn: fn}
	if len(label) > 0 {
		op.label = label[0]
	}
	s.stream.env.operators = append(s.stream.env.operators, op)
	return s
}

// Map transforms each typed value while preserving record metadata.
func (s *TypedStream[T]) Map(fn func(T) T, label ...string) *TypedStream[T] {
	op := typedMapOperator[T, T](fn)
	if len(label) > 0 {
		op.Label = label[0]
	}
	s.stream.env.operators = append(s.stream.env.operators, op)
	return s
}

// FlatMap transforms one typed value into zero or more typed values. Each
// output record inherits the input record metadata and receives its own typed
// payload.
func (s *TypedStream[T]) FlatMap(fn func(T) []T, label ...string) *TypedStream[T] {
	op := &typedFlatMapOperator[T]{fn: fn}
	if len(label) > 0 {
		op.label = label[0]
	}
	s.stream.env.operators = append(s.stream.env.operators, op)
	return s
}

// Process applies a typed function that can fail.
func (s *TypedStream[T]) Process(fn func(context.Context, T) (T, error), label ...string) *TypedStream[T] {
	op := &typedProcessOperator[T]{fn: fn}
	if len(label) > 0 {
		op.label = label[0]
	}
	s.stream.env.operators = append(s.stream.env.operators, op)
	return s
}

// KeyBy partitions a typed stream by a key derived from the typed payload.
func (s *TypedStream[T]) KeyBy(fn func(T) []byte, label ...string) *TypedStream[T] {
	s.stream.KeyBy(func(r types.Record) []byte {
		v, err := typedValue[T](r)
		if err != nil {
			return nil
		}
		return fn(v)
	}, label...)
	return s
}

// ProcessKeyed applies a typed stateful function per key. Use after KeyBy to
// get partitioned keyed execution. The context exposes raw keyed state and
// event-time timers; typed outputs preserve input metadata.
func (s *TypedStream[T]) ProcessKeyed(fn func(*operator.KeyedContext, T) ([]T, error), onTimer func(*operator.KeyedContext, time.Time) ([]T, error), label ...string) *TypedStream[T] {
	rawFn := func(ctx *operator.KeyedContext, r types.Record) ([]types.Record, error) {
		v, err := typedValue[T](r)
		if err != nil {
			return nil, err
		}
		items, err := fn(ctx, v)
		if err != nil {
			return nil, err
		}
		out := make([]types.Record, 0, len(items))
		for _, item := range items {
			next, err := recordWithTypedValue(r, item)
			if err != nil {
				return nil, err
			}
			out = append(out, next)
		}
		return out, nil
	}
	var rawTimer operator.TimerFn
	if onTimer != nil {
		rawTimer = func(ctx *operator.KeyedContext, ts time.Time) ([]types.Record, error) {
			items, err := onTimer(ctx, ts)
			if err != nil {
				return nil, err
			}
			out := make([]types.Record, 0, len(items))
			for _, item := range items {
				next, err := recordWithTypedValue(types.Record{
					Key:       ctx.Key(),
					Timestamp: ts,
				}, item)
				if err != nil {
					return nil, err
				}
				out = append(out, next)
			}
			return out, nil
		}
	}
	s.stream.ProcessKeyed(rawFn, rawTimer, label...)
	return s
}

// ToSink connects the typed stream to an existing record sink.
func (s *TypedStream[T]) ToSink(sk sink.Sink) *StreamExecutionEnv {
	return s.stream.ToSink(sk)
}

// ToTypedSink connects the typed stream to a typed sink adapter.
func (s *TypedStream[T]) ToTypedSink(sk TypedSink[T]) *StreamExecutionEnv {
	return s.stream.ToSink(typedSinkAdapter[T]{sink: sk})
}

// MapTyped changes the typed payload from T to U. It is a function, not a
// method, because Go methods cannot introduce a new type parameter.
func MapTyped[T, U any](s *TypedStream[T], fn func(T) U, label ...string) *TypedStream[U] {
	op := typedMapOperator[T, U](fn)
	if len(label) > 0 {
		op.Label = label[0]
	}
	s.stream.env.operators = append(s.stream.env.operators, op)
	return &TypedStream[U]{stream: s.stream}
}

// FlatMapTyped changes the typed payload from T to zero or more U values.
func FlatMapTyped[T, U any](s *TypedStream[T], fn func(T) []U, label ...string) *TypedStream[U] {
	op := &typedFlatMapOperator2[T, U]{fn: fn}
	if len(label) > 0 {
		op.label = label[0]
	}
	s.stream.env.operators = append(s.stream.env.operators, op)
	return &TypedStream[U]{stream: s.stream}
}

// TypedSink consumes typed records at the pipeline boundary.
type TypedSink[T any] interface {
	WriteTyped(ctx context.Context, in <-chan TypedRecord[T]) error
}

// TypedSinkFunc adapts a per-record function to a typed sink.
type TypedSinkFunc[T any] func(ctx context.Context, r TypedRecord[T]) error

func (f TypedSinkFunc[T]) WriteTyped(ctx context.Context, in <-chan TypedRecord[T]) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-in:
			if !ok {
				return nil
			}
			if err := f(ctx, r); err != nil {
				return err
			}
		}
	}
}

type typedSliceSource[T any] struct {
	records []TypedRecord[T]
}

func (s typedSliceSource[T]) Run(ctx context.Context, out chan<- types.Record) error {
	for _, tr := range s.records {
		r, err := recordFromTypedRecord(tr)
		if err != nil {
			return err
		}
		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type typedSinkAdapter[T any] struct {
	sink TypedSink[T]
}

func (s typedSinkAdapter[T]) Write(ctx context.Context, in <-chan types.Record) error {
	out := make(chan TypedRecord[T], 256)
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.sink.WriteTyped(ctx, out)
	}()
	for {
		select {
		case <-ctx.Done():
			close(out)
			<-errCh
			return ctx.Err()
		case err := <-errCh:
			close(out)
			return err
		case r, ok := <-in:
			if !ok {
				close(out)
				return <-errCh
			}
			if r.IsWatermark || r.IsBarrier {
				continue
			}
			tr, err := typedRecordFromRecord[T](r)
			if err != nil {
				close(out)
				<-errCh
				return err
			}
			select {
			case out <- tr:
			case <-ctx.Done():
				close(out)
				<-errCh
				return ctx.Err()
			case err := <-errCh:
				close(out)
				return err
			}
		}
	}
}

func typedMapOperator[T, U any](fn func(T) U) *operator.ProcessOperator {
	return operator.NewProcess(func(r types.Record) (types.Record, error) {
		v, err := typedValue[T](r)
		if err != nil {
			return r, err
		}
		return recordWithTypedValue(r, fn(v))
	}, operator.WithProcessFailurePolicy(operator.ProcFailureFail))
}

type typedFilterOperator[T any] struct {
	fn    func(T) bool
	label string
}

func (op *typedFilterOperator[T]) Name() string     { return "TypedFilter" }
func (op *typedFilterOperator[T]) GetLabel() string { return op.label }

func (op *typedFilterOperator[T]) Process(in <-chan types.Record, out chan<- types.Record) {
	if err := op.ProcessE(context.Background(), in, out); err != nil {
		panic(err)
	}
}

func (op *typedFilterOperator[T]) ProcessE(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-in:
			if !ok {
				return nil
			}
			if r.IsWatermark || r.IsBarrier {
				if err := sendTypedRecord(ctx, out, r); err != nil {
					return err
				}
				continue
			}
			v, err := typedValue[T](r)
			if err != nil {
				return &operator.OperatorError{Operator: op.Name(), Label: op.label, Key: r.Key, Err: err}
			}
			if !op.fn(v) {
				continue
			}
			next, err := recordWithTypedValue(r, v)
			if err != nil {
				return &operator.OperatorError{Operator: op.Name(), Label: op.label, Key: r.Key, Err: err}
			}
			if err := sendTypedRecord(ctx, out, next); err != nil {
				return err
			}
		}
	}
}

type typedFlatMapOperator[T any] struct {
	fn    func(T) []T
	label string
}

type typedFlatMapOperator2[T, U any] struct {
	fn    func(T) []U
	label string
}

func (op *typedFlatMapOperator2[T, U]) Name() string     { return "TypedFlatMap" }
func (op *typedFlatMapOperator2[T, U]) GetLabel() string { return op.label }

func (op *typedFlatMapOperator2[T, U]) Process(in <-chan types.Record, out chan<- types.Record) {
	if err := op.ProcessE(context.Background(), in, out); err != nil {
		panic(err)
	}
}

func (op *typedFlatMapOperator2[T, U]) ProcessE(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-in:
			if !ok {
				return nil
			}
			if r.IsWatermark || r.IsBarrier {
				if err := sendTypedRecord(ctx, out, r); err != nil {
					return err
				}
				continue
			}
			v, err := typedValue[T](r)
			if err != nil {
				return &operator.OperatorError{Operator: op.Name(), Label: op.label, Key: r.Key, Err: err}
			}
			for _, item := range op.fn(v) {
				next, err := recordWithTypedValue(r, item)
				if err != nil {
					return &operator.OperatorError{Operator: op.Name(), Label: op.label, Key: r.Key, Err: err}
				}
				if err := sendTypedRecord(ctx, out, next); err != nil {
					return err
				}
			}
		}
	}
}

type typedProcessOperator[T any] struct {
	fn    func(context.Context, T) (T, error)
	label string
}

func (op *typedProcessOperator[T]) Name() string     { return "TypedProcess" }
func (op *typedProcessOperator[T]) GetLabel() string { return op.label }

func (op *typedProcessOperator[T]) Process(in <-chan types.Record, out chan<- types.Record) {
	if err := op.ProcessE(context.Background(), in, out); err != nil {
		panic(err)
	}
}

func (op *typedProcessOperator[T]) ProcessE(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-in:
			if !ok {
				return nil
			}
			if r.IsWatermark || r.IsBarrier {
				if err := sendTypedRecord(ctx, out, r); err != nil {
					return err
				}
				continue
			}
			v, err := typedValue[T](r)
			if err != nil {
				return &operator.OperatorError{Operator: op.Name(), Label: op.label, Key: r.Key, Err: err}
			}
			next, err := op.fn(ctx, v)
			if err != nil {
				return &operator.OperatorError{Operator: op.Name(), Label: op.label, Key: r.Key, Err: err}
			}
			outRec, err := recordWithTypedValue(r, next)
			if err != nil {
				return &operator.OperatorError{Operator: op.Name(), Label: op.label, Key: r.Key, Err: err}
			}
			if err := sendTypedRecord(ctx, out, outRec); err != nil {
				return err
			}
		}
	}
}

func (op *typedFlatMapOperator[T]) Name() string     { return "TypedFlatMap" }
func (op *typedFlatMapOperator[T]) GetLabel() string { return op.label }

func (op *typedFlatMapOperator[T]) Process(in <-chan types.Record, out chan<- types.Record) {
	if err := op.ProcessE(context.Background(), in, out); err != nil {
		panic(err)
	}
}

func (op *typedFlatMapOperator[T]) ProcessE(ctx context.Context, in <-chan types.Record, out chan<- types.Record) error {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-in:
			if !ok {
				return nil
			}
			if r.IsWatermark || r.IsBarrier {
				if err := sendTypedRecord(ctx, out, r); err != nil {
					return err
				}
				continue
			}
			v, err := typedValue[T](r)
			if err != nil {
				return &operator.OperatorError{Operator: op.Name(), Label: op.label, Key: r.Key, Err: err}
			}
			for _, item := range op.fn(v) {
				next, err := recordWithTypedValue(r, item)
				if err != nil {
					return &operator.OperatorError{Operator: op.Name(), Label: op.label, Key: r.Key, Err: err}
				}
				if err := sendTypedRecord(ctx, out, next); err != nil {
					return err
				}
			}
		}
	}
}

func sendTypedRecord(ctx context.Context, out chan<- types.Record, r types.Record) error {
	select {
	case out <- r:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func typedValue[T any](r types.Record) (T, error) {
	var zero T
	if r.Parsed != nil {
		switch v := r.Parsed.(type) {
		case T:
			return v, nil
		case *T:
			if v == nil {
				return zero, fmt.Errorf("typed stream: parsed value is nil *T")
			}
			return *v, nil
		}
	}
	if len(r.Value) == 0 {
		return zero, fmt.Errorf("typed stream: record has no parsed value or JSON payload")
	}
	var v T
	if err := json.Unmarshal(r.Value, &v); err != nil {
		return zero, fmt.Errorf("typed stream: decode JSON: %w", err)
	}
	return v, nil
}

func recordWithTypedValue[T any](r types.Record, v T) (types.Record, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return r, fmt.Errorf("typed stream: encode JSON: %w", err)
	}
	r.Value = b
	r.Parsed = v
	return r, nil
}

func typedRecordFromRecord[T any](r types.Record) (TypedRecord[T], error) {
	v, err := typedValue[T](r)
	if err != nil {
		return TypedRecord[T]{}, err
	}
	return TypedRecord[T]{
		Key:       append([]byte(nil), r.Key...),
		Value:     v,
		Timestamp: r.Timestamp,
		Offset:    r.Offset,
		Partition: r.Partition,
		Source:    r.Source,
		Headers:   cloneHeaders(r.Headers),
	}, nil
}

func recordFromTypedRecord[T any](tr TypedRecord[T]) (types.Record, error) {
	r := types.Record{
		Key:       append([]byte(nil), tr.Key...),
		Timestamp: tr.Timestamp,
		Offset:    tr.Offset,
		Partition: tr.Partition,
		Source:    tr.Source,
		Headers:   cloneHeaders(tr.Headers),
	}
	if r.Timestamp.IsZero() {
		r.Timestamp = time.Now().UTC()
	}
	return recordWithTypedValue(r, tr.Value)
}

func cloneHeaders(in map[string][]byte) map[string][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}
