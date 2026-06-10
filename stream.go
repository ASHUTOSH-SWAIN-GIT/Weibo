package mailer

import (
	"mailer/operator"
	"mailer/sink"
	"mailer/types"
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
func (s *Stream) Map(fn func(types.Record) types.Record) *Stream {
	s.env.operators = append(s.env.operators, operator.Map(fn))
	return s
}

// FlatMap applies a 1:many transformation to each record.
// The function returns a slice; if empty, the record is dropped.
func (s *Stream) FlatMap(fn func(types.Record) []types.Record) *Stream {
	s.env.operators = append(s.env.operators, operator.FlatMap(fn))
	return s
}

// Filter keeps only records where fn returns true.
func (s *Stream) Filter(fn func(types.Record) bool) *Stream {
	s.env.operators = append(s.env.operators, operator.Filter(fn))
	return s
}

// KeyBy partitions the stream by the given key selector function.
// All records with the same key are routed together. Required before
// stateful operations like Reduce.
func (s *Stream) KeyBy(fn func(types.Record) []byte) *Stream {
	s.env.operators = append(s.env.operators, operator.KeyBy(fn))
	return s
}

// ToSink connects the stream to a sink and returns the execution environment.
// The pipeline is still lazy — call env.Execute() to start processing.
func (st *Stream) ToSink(sk sink.Sink) *StreamExecutionEnv {
	st.env.sink = sk
	return st.env
}