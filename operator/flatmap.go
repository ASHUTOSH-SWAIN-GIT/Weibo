package operator

import "mailer/types"

// FlatMapOperator applies a 1:many transformation to each record.
// The function returns a slice of records. If the slice is empty,
// the input record is effectively filtered out.
type FlatMapOperator struct {
	Fn func(types.Record) []types.Record
}

// FlatMap creates a FlatMapOperator with the given transformation function.
func FlatMap(fn func(types.Record) []types.Record) *FlatMapOperator {
	return &FlatMapOperator{Fn: fn}
}

// Process reads each record from in, applies the flat map function,
// and writes each result record to out. Watermarks are passed through unchanged.
func (op *FlatMapOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)
	for record := range in {
		if record.IsWatermark {
			out <- record
			continue
		}
		results := op.Fn(record)
		for _, result := range results {
			out <- result
		}
	}
}
