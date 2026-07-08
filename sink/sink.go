package sink

import (
	"context"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// Sink is where processed data leaves the pipeline.
// A Sink reads records from the input channel until it's closed
// or the context is cancelled.
type Sink interface {
	Write(ctx context.Context, in <-chan types.Record) error
}

// Describable is an optional interface that Sinks can implement
// to expose metadata for the dashboard.
type Describable interface {
	Describe() SinkInfo
}

// SinkInfo holds display metadata about a sink.
type SinkInfo struct {
	Type  string            `json:"type"`
	Props map[string]string `json:"props"`
}
