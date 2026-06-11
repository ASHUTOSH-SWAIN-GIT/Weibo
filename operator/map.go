package operator

import "mailer/types"

// MapOperator applies a 1:1 transformation to each record.
// Every input record produces exactly one output record.
// To conditionally drop records, use Filter before or after Map.
type MapOperator struct {
	Fn func(types.Record) types.Record
}

// Map creates a MapOperator with the given transformation function.
func Map(fn func(types.Record) types.Record) *MapOperator {
	return &MapOperator{Fn: fn}
}

// Process reads each record from in, applies the map function, and
// writes the result to out. Watermarks and barriers are passed through unchanged.
func (op *MapOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)
	for record := range in {
		if record.IsWatermark || record.IsBarrier {
			out <- record
			continue
		}
		out <- op.Fn(record)
	}
}
