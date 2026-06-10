package source

import (
	"context"

	"mailer/types"
)

// Source is where data enters the pipeline.
// A Source continuously emits Records into the output channel until
// the context is cancelled or the source is exhausted.
// When finished, it must close the output channel.
type Source interface {
	Run(ctx context.Context, out chan<- types.Record) error
}