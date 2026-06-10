package source

import (
	"context"

	"mailer"
)

// Source is where data enters the pipeline.
// A Source continuously emits Records into the output channel until
// the context is cancelled or the source is exhausted.
// When the source is done, it must close the output channel.
type Source interface {
	// Run starts producing records into the out channel.
	// It should block until the context is cancelled or the source is exhausted.
	// When finished, it must close the out channel.
	Run(ctx context.Context, out chan<- mailer.Record) error
}