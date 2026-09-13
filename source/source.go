package source

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// Position is the next source offset to read for one source stream/partition.
// Source is connector-defined; Kafka uses the topic name.
type Position struct {
	Source    string `json:"source"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// PositionKey is the comparable identity used while tracking positions.
type PositionKey struct {
	Source    string
	Partition int
}

type positionEnvelope struct {
	Version   int        `json:"version"`
	Positions []Position `json:"positions"`
}

// EncodePositions writes the versioned topic-aware checkpoint format.
func EncodePositions(positions []Position) ([]byte, error) {
	positions = append([]Position(nil), positions...)
	sort.Slice(positions, func(i, j int) bool {
		if positions[i].Source != positions[j].Source {
			return positions[i].Source < positions[j].Source
		}
		return positions[i].Partition < positions[j].Partition
	})
	return json.Marshal(positionEnvelope{Version: 2, Positions: positions})
}

// DecodePositions reads the current format and legacy {"partition": offset}
// checkpoints. Legacy data requires a single unambiguous default source.
func DecodePositions(data []byte, legacySource string) ([]Position, error) {
	var envelope positionEnvelope
	if err := json.Unmarshal(data, &envelope); err == nil && envelope.Version != 0 {
		if envelope.Version != 2 {
			return nil, fmt.Errorf("source positions: unsupported version %d", envelope.Version)
		}
		seen := make(map[PositionKey]struct{}, len(envelope.Positions))
		for _, p := range envelope.Positions {
			if p.Source == "" || p.Partition < 0 || p.Offset < 0 {
				return nil, fmt.Errorf("source positions: invalid position %+v", p)
			}
			key := PositionKey{Source: p.Source, Partition: p.Partition}
			if _, ok := seen[key]; ok {
				return nil, fmt.Errorf("source positions: duplicate %s/%d", p.Source, p.Partition)
			}
			seen[key] = struct{}{}
		}
		return envelope.Positions, nil
	}

	if legacySource == "" {
		return nil, fmt.Errorf("source positions: legacy checkpoint has no unambiguous source")
	}
	var legacy map[string]int64
	if err := json.Unmarshal(data, &legacy); err != nil {
		return nil, fmt.Errorf("source positions: decode: %w", err)
	}
	positions := make([]Position, 0, len(legacy))
	for partText, offset := range legacy {
		part, err := strconv.Atoi(partText)
		if err != nil || part < 0 || offset < 0 {
			return nil, fmt.Errorf("source positions: invalid legacy position %q=%d", partText, offset)
		}
		positions = append(positions, Position{Source: legacySource, Partition: part, Offset: offset})
	}
	return positions, nil
}

// Source is where data enters the pipeline.
// A Source continuously emits Records into the output channel until
// the context is cancelled or the source is exhausted.
// The channel owner (e.g. StreamExecutionEnv) is responsible for closing the output channel.
type Source interface {
	Run(ctx context.Context, out chan<- types.Record) error
}

// Capabilities describes optional source behaviours that the runtime can
// validate before a pipeline starts. Implementing this is preferred for new
// connectors because it makes delivery guarantees explicit, but existing
// connectors remain compatible: CapabilitiesOf derives these values from the
// legacy optional interfaces below.
type Capabilities struct {
	// CheckpointOffsets means the source can persist and restore its read
	// position via CheckpointSource.
	CheckpointOffsets bool

	// PositionedCheckpoints means checkpoint positions carry source identity
	// in addition to partition identity via PositionedCheckpointSource.
	PositionedCheckpoints bool

	// Drain means the source can flush pending local work before shutdown.
	Drain bool

	// CommitOffsets means the source can externally commit checkpointed
	// offsets after a coordinated checkpoint completes.
	CommitOffsets bool

	// OperationalState means the source exposes read-only live status.
	OperationalState bool

	// Describe means the source exposes dashboard metadata.
	Describe bool
}

// CapabilityProvider is implemented by sources that explicitly declare their
// optional behaviours.
type CapabilityProvider interface {
	SourceCapabilities() Capabilities
}

// CapabilitiesOf returns a source's declared capabilities plus any capabilities
// implied by the legacy optional interfaces it implements.
func CapabilitiesOf(src Source) Capabilities {
	var caps Capabilities
	if src == nil {
		return caps
	}
	if p, ok := src.(CapabilityProvider); ok {
		caps = p.SourceCapabilities()
	}
	if _, ok := src.(CheckpointSource); ok {
		caps.CheckpointOffsets = true
	}
	if _, ok := src.(PositionedCheckpointSource); ok {
		caps.PositionedCheckpoints = true
	}
	if _, ok := src.(Drainable); ok {
		caps.Drain = true
	}
	if _, ok := src.(OffsetCommitter); ok {
		caps.CommitOffsets = true
	}
	if _, ok := src.(OperationalStateProvider); ok {
		caps.OperationalState = true
	}
	if _, ok := src.(Describable); ok {
		caps.Describe = true
	}
	return caps
}

// OperationalStateProvider exposes a concurrent-safe, read-only snapshot for
// the job agent's /state endpoint.
type OperationalStateProvider interface {
	OperationalState() any
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

// PositionedCheckpointSource lets a source preserve identity beyond a numeric
// partition when the engine captures barrier-aligned positions. Sources that
// do not implement it retain the legacy partition-only checkpoint contract.
type PositionedCheckpointSource interface {
	CheckpointPosition(record types.Record) PositionKey
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
