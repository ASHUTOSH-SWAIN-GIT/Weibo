package mailer

import (
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/window"
)

// Stream represents a pipeline stage. Method calls on Stream
// append operators to the chain and return the updated Stream.
//
// Streams are built using a fluent API:
//
//	env.FromSource(src).Map(fn).Filter(fn).ToSink(s)
//
// The pipeline is lazy — nothing runs until env.Execute() is called.
type Stream struct {
	env *StreamExecutionEnv
}

// Map applies a 1:1 transformation to each record in the stream.
// If the function returns the zero value of Record, the record is dropped.
// The optional label is shown in the dashboard.
func (s *Stream) Map(fn func(types.Record) types.Record, label ...string) *Stream {
	op := operator.Map(fn)
	if len(label) > 0 {
		op.Label = label[0]
	}
	s.env.operators = append(s.env.operators, op)
	return s
}

// FlatMap applies a 1:many transformation to each record.
// The function returns a slice; if empty, the record is dropped.
// The optional label is shown in the dashboard.
func (s *Stream) FlatMap(fn func(types.Record) []types.Record, label ...string) *Stream {
	op := operator.FlatMap(fn)
	if len(label) > 0 {
		op.Label = label[0]
	}
	s.env.operators = append(s.env.operators, op)
	return s
}

// Process applies a user function that may return an error.  On error
// the failure policy (Drop / DLQ / Fail) is applied.  Use the
// operator.WithProcessFailurePolicy and operator.WithProcessDLQ options
// to configure failure handling.
//
//	stream.Process(func(r types.Record) (types.Record, error) {
//	    if !isValid(r) { return r, fmt.Errorf("invalid") }
//	    return enrich(r), nil
//	}, operator.WithProcessFailurePolicy(operator.ProcFailureDLQ)).
//	    ToSink(s)
func (s *Stream) Process(fn func(types.Record) (types.Record, error), opts ...operator.ProcessOption) *Stream {
	op := operator.NewProcess(fn, opts...)
	s.env.operators = append(s.env.operators, op)
	return s
}

// Filter keeps only records where fn returns true.
// The optional label is shown in the dashboard.
func (s *Stream) Filter(fn func(types.Record) bool, label ...string) *Stream {
	op := operator.Filter(fn)
	if len(label) > 0 {
		op.Label = label[0]
	}
	s.env.operators = append(s.env.operators, op)
	return s
}

// KeyBy partitions the stream by the given key selector function.
// All records with the same key are routed together. Required before
// stateful operations like Reduce.
// The optional label is shown in the dashboard.
func (s *Stream) KeyBy(fn func(types.Record) []byte, label ...string) *Stream {
	op := operator.KeyBy(fn)
	if len(label) > 0 {
		op.Label = label[0]
	}
	s.env.operators = append(s.env.operators, op)
	return s
}

// Reduce applies a stateful aggregation per key. Must be used after KeyBy.
// The reduce function is called with the current accumulator (nil on first call)
// and the incoming record, and returns the new accumulator.
// The updated accumulator is emitted downstream after every record.
//
// Example (count per key):
//
//	stream.KeyBy(func(r types.Record) []byte { return r.Key }).
//	    Reduce(func(accum []byte, curr types.Record) []byte {
//	        count := 0
//	        if accum != nil {
//	            count = int(binary.BigEndian.Uint64(accum))
//	        }
//	        count++
//	        buf := make([]byte, 8)
//	        binary.BigEndian.PutUint64(buf, uint64(count))
//	        return buf
//	    })
func (s *Stream) Reduce(fn operator.ReduceFn, label ...string) *Stream {
	op := operator.Reduce(fn)
	if len(label) > 0 {
		op.Label = label[0]
	}
	s.env.operators = append(s.env.operators, op)
	return s
}

// Window groups records into time-based windows. Must be used after KeyBy.
// Records are buffered into windows, and when a watermark passes a window's
// end time, the window fires — all its records are emitted as a single result.
//
// Supported window types:
//   - window.Tumbling(size):   fixed-size, non-overlapping (e.g. 5-minute buckets)
//   - window.Sliding(size, slide): overlapping windows (e.g. 5-min every 1-min)
//   - window.Session(gap):     variable-size, closes after inactivity gap
//
// Example (5-minute tumbling window):
//
//	stream.KeyBy(func(r types.Record) []byte { return r.Key }).
//	    Window(window.Tumbling(5 * time.Minute)).
//	    Reduce(aggregateFn)
func (s *Stream) Window(assigner window.WindowAssigner, label ...string) *Stream {
	op := operator.Window(assigner)
	if len(label) > 0 {
		op.Label = label[0]
	}
	s.env.operators = append(s.env.operators, op)
	return s
}

// WindowWithIdleTimeout creates a window with an idle timeout.
// If no records arrive within the timeout duration, all remaining
// windows are fired and the pipeline stage completes. Useful for
// infinite streams that don't receive shutdown signals.
func (s *Stream) WindowWithIdleTimeout(assigner window.WindowAssigner, idleTimeout time.Duration, label ...string) *Stream {
	op := operator.Window(assigner).WithIdleTimeout(idleTimeout)
	if len(label) > 0 {
		op.Label = label[0]
	}
	s.env.operators = append(s.env.operators, op)
	return s
}

// ToSink connects the stream to a sink and returns the execution environment.
// The pipeline is still lazy — call env.Execute() to start processing.
func (st *Stream) ToSink(sk sink.Sink) *StreamExecutionEnv {
	st.env.sink = sk
	return st.env
}
