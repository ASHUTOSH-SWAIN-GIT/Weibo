package source

import (
	"context"

	"mailer/types"
)

// SliceSource reads records from an in-memory slice. Unlike GeneratorSource,
// SliceSource accepts a raw []Record slice directly — no need to call
// NewGeneratorSource or FromSlices first.
//
// It emits each record in order and then closes. Useful for tests and
// one-shot pipelines where you already have the data in memory.
type SliceSource struct {
	records []types.Record
}

// NewSliceSource creates a source that emits the given records in order.
func NewSliceSource(records []types.Record) *SliceSource {
	return &SliceSource{records: records}
}

// Run emits all records into the output channel and then returns.
// The channel owner (StreamExecutionEnv) is responsible for closing it.
func (s *SliceSource) Run(ctx context.Context, out chan<- types.Record) error {
	for _, record := range s.records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- record:
		}
	}
	return nil
}
