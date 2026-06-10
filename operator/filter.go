package operator

import "mailer/types"

// FilterOperator keeps records that match the predicate and drops the rest.
type FilterOperator struct {
	Fn func(types.Record) bool
}

// Filter creates a FilterOperator with the given predicate.
// Only records where fn(record) == true will pass through.
func Filter(fn func(types.Record) bool) *FilterOperator {
	return &FilterOperator{Fn: fn}
}

// Process reads each record from in and writes it to out only if the
// predicate returns true. Watermarks are always passed through.
func (op *FilterOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)
	for record := range in {
		if record.IsWatermark {
			out <- record
			continue
		}
		if op.Fn(record) {
			out <- record
		}
	}
}