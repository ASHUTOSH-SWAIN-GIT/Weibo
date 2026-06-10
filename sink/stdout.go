package sink

import (
	"context"
	"fmt"
	"time"

	"mailer/types"
)

// StdoutSink prints each record to stdout in a human-readable format.
// Useful for debugging and examples.
type StdoutSink struct{}

// NewStdoutSink creates a sink that prints records to stdout.
func NewStdoutSink() *StdoutSink {
	return &StdoutSink{}
}

// Write reads records from the input channel and prints each one to stdout.
func (s *StdoutSink) Write(ctx context.Context, in <-chan types.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-in:
			if !ok {
				return nil
			}
			fmt.Printf("key=%s value=%s timestamp=%s offset=%d\n",
				string(record.Key),
				string(record.Value),
				record.Timestamp.Format(time.RFC3339),
				record.Offset,
			)
		}
	}
}
