package sink

import (
	"context"

	"mailer"
)

// Sink is where processed data leaves the pipeline.
// A Sink reads records from the input channel until it's closed
// (meaning the source and all upstream operators are done).
type Sink interface {
	// Write consumes records from the in channel.
	// It should block until the in channel is closed or the context is cancelled.
	Write(ctx context.Context, in <-chan mailer.Record) error
}