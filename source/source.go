package source

import (
	"context"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// Source is where data enters the pipeline.
// A Source continuously emits Records into the output channel until
// the context is cancelled or the source is exhausted.
// The channel owner (e.g. StreamExecutionEnv) is responsible for closing the output channel.
type Source interface {
	Run(ctx context.Context, out chan<- types.Record) error
}

// Describable is an optional interface that Sources can implement
// to expose metadata for the dashboard.
type Describable interface {
	Describe() SourceInfo
}

// SourceInfo holds display metadata about a source.
type SourceInfo struct {
	Type  string            `json:"type"`
	Props map[string]string `json:"props"`
}

// CheckpointSource is an optional interface that Sources can implement
// to support checkpointing. When the CheckpointCoordinator needs to
// create a checkpoint, it asks the source to save its current offset
// so it can resume from that point on recovery.
type CheckpointSource interface {
	// CheckpointOffset returns the source's current position as opaque bytes.
	CheckpointOffset() ([]byte, error)

	// RestoreOffset seeks the source to the position saved by CheckpointOffset.
	RestoreOffset(data []byte) error
}

// Drainable is an optional interface that sources implement to flush
// pending state (e.g. uncommitted Kafka offsets) during graceful shutdown.
type Drainable interface {
	Drain(ctx context.Context) error
}

// OffsetCommitter is an optional interface for sources whose offsets
// should be committed externally (e.g. to the Kafka broker) after a
// coordinated checkpoint completes. The commit is ADVISORY — consumer
// lag visibility only. The checkpoint file remains the source of
// truth for recovery. Offsets use the same JSON shape as
// CheckpointSource.CheckpointOffset ({"partition": nextOffset}).
type OffsetCommitter interface {
	CommitOffsets(ctx context.Context, offsets []byte) error
}
