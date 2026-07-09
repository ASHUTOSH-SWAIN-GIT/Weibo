package operator

import "github.com/ASHUTOSH-SWAIN-GIT/mailer/types"

// FilterOperator keeps records that match the predicate and drops the rest.
type FilterOperator struct {
	Fn    func(types.Record) bool
	Label string
}

// Filter creates a FilterOperator with the given predicate.
// Only records where fn(record) == true will pass through.
func Filter(fn func(types.Record) bool) *FilterOperator {
	return &FilterOperator{Fn: fn}
}

func (op *FilterOperator) Name() string     { return "Filter" }
func (op *FilterOperator) GetLabel() string { return op.Label }
func (op *FilterOperator) DescribeOp() OperatorMeta {
	return OperatorMeta{Type: "Filter", Label: op.Label}
}

// Process reads each record from in and writes it to out only if the
// predicate returns true. Watermarks and barriers are always passed through.
func (op *FilterOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)
	for record := range in {
		if record.IsWatermark || record.IsBarrier {
			out <- record
			continue
		}
		if op.Fn(record) {
			out <- record
		}
	}
}
