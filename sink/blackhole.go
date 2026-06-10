package sink

import (
	"context"

	"mailer"
)

// BlackholeSink discards all records. Used for benchmarking pipeline throughput
// without the overhead of writing to an external system.
type BlackholeSink struct {
	count int64
}

// NewBlackholeSink creates a sink that silently discards all records.
func NewBlackholeSink() *BlackholeSink {
	return &BlackholeSink{}
}

// Write drains the input channel, counting records but discarding them.
func (s *BlackholeSink) Write(ctx context.Context, in <-chan mailer.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-in:
			if !ok {
				return nil
			}
			s.count++
		}
	}
}

// Count returns the total number of records consumed.
func (s *BlackholeSink) Count() int64 {
	return s.count
}