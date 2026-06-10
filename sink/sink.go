package sink

import (
	"context"

	"mailer/types"
)

// Sink is where processed data leaves the pipeline.
// A Sink reads records from the input channel until it's closed
// or the context is cancelled.
type Sink interface {
	Write(ctx context.Context, in <-chan types.Record) error
}