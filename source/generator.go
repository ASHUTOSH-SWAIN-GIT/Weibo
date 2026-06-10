package source

import (
	"context"
	"time"

	"mailer"
)

// GeneratorSource produces a fixed slice of records and then closes.
// Useful for testing and examples.
type GeneratorSource struct {
	records []mailer.Record
}

// NewGeneratorSource creates a source that emits the given records in order.
func NewGeneratorSource(records []mailer.Record) *GeneratorSource {
	return &GeneratorSource{records: records}
}

// FromSlices is a convenience function that creates records from string key-value pairs.
// Each record gets an incrementing offset and the current timestamp.
func FromSlices(keys []string, values []string) *GeneratorSource {
	records := make([]mailer.Record, len(keys))
	for i := range keys {
		records[i] = mailer.Record{
			Key:       []byte(keys[i]),
			Value:     []byte(values[i]),
			Offset:    int64(i),
			Timestamp: time.Now().UTC(),
		}
	}
	return NewGeneratorSource(records)
}

// Run emits all records into the output channel and then closes it.
func (s *GeneratorSource) Run(ctx context.Context, out chan<- mailer.Record) error {
	defer close(out)
	for _, record := range s.records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- record:
		}
	}
	return nil
}