package sink

import (
	"context"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// Sink is where processed data leaves the pipeline.
// A Sink reads records from the input channel until it's closed
// or the context is cancelled.
type Sink interface {
	Write(ctx context.Context, in <-chan types.Record) error
}

// CheckpointedSink is a Sink that participates in coordinated
// exactly-once checkpoints. Output between two checkpoint barriers is
// staged (e.g. in a Kafka transaction) and becomes visible only when
// the coordinator calls Commit.
//
// Contract:
//   - Write sees checkpoint barriers in-band, after every pre-barrier
//     record. On a barrier the sink must flush all staged output for
//     the ending interval into the open transaction (plus a
//     transaction marker identifying the checkpoint), invoke the
//     OnPrepared callback, and BLOCK further writes until the
//     coordinator calls Commit or Abort for that checkpoint.
//   - Commit makes the interval's output visible and opens the next
//     transaction. Abort discards it.
//   - WasCommitted reports, after a restart, whether the transaction
//     for a checkpoint ID actually committed (e.g. by probing the
//     transaction marker under read-committed isolation). It resolves
//     the prepared-but-unconfirmed recovery case.
type CheckpointedSink interface {
	Sink

	// SetOnPrepared registers the coordinator callback invoked when a
	// barrier's output is fully staged (or staging failed).
	SetOnPrepared(fn func(id string, err error))

	// Commit finalizes the transaction for checkpoint id and unblocks Write.
	Commit(ctx context.Context, id string) error

	// Abort discards the transaction for checkpoint id and unblocks Write.
	Abort(ctx context.Context, id string) error

	// WasCommitted reports whether checkpoint id's transaction
	// committed before a crash.
	WasCommitted(ctx context.Context, id string) (bool, error)
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
