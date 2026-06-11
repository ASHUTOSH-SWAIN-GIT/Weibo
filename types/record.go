package types

import "time"

// Record is the fundamental data unit flowing through a stream pipeline.
// Every source emits Records, every operator transforms them, every sink receives them.
//
// A Record can be one of three types:
//   - Data record: IsWatermark == false, IsBarrier == false — carries key/value/timestamp
//   - Watermark:   IsWatermark == true — carries a Timestamp that says
//     "no records with event time < this timestamp will arrive after this point"
//   - Barrier:     IsBarrier == true — carries a CheckpointID that says
//     "snapshot state for this checkpoint when the barrier passes"
//
// Watermarks drive window firing. Barriers drive checkpointing.
type Record struct {
	// Key is used for partitioning after KeyBy. Nil means unkeyed.
	Key []byte

	// Value is the raw data payload.
	Value []byte

	// Timestamp is the event time — when the event actually happened.
	// Used for windowing and watermark generation.
	Timestamp time.Time

	// Offset is the source offset (e.g. Kafka partition offset).
	// Used for checkpointing — when we restore, we rewind to this offset.
	Offset int64

	// Headers carry optional metadata (e.g. Kafka headers, trace IDs).
	Headers map[string][]byte

	// IsWatermark marks this record as a watermark marker instead of data.
	// Watermarks have no Key or Value — they only carry a Timestamp.
	// Operators use watermarks to know when windows can close.
	IsWatermark bool

	// IsBarrier marks this record as a checkpoint barrier.
	// Barriers flow in-band through the pipeline (like watermarks).
	// When a stateful operator sees a barrier, it snapshots its state,
	// then forwards the barrier downstream. When the barrier reaches
	// the sink, the checkpoint is complete.
	IsBarrier bool

	// CheckpointID identifies which checkpoint this barrier belongs to.
	// Only valid when IsBarrier == true.
	CheckpointID string
}

// NewRecord creates a data Record with the given key, value, and current timestamp.
func NewRecord(key, value []byte) Record {
	return Record{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UTC(),
	}
}

// NewWatermark creates a watermark Record with the given timestamp.
// Watermarks signal that no more records with event time < timestamp will arrive.
func NewWatermark(ts time.Time) Record {
	return Record{
		Timestamp:   ts,
		IsWatermark: true,
	}
}

// NewBarrier creates a checkpoint barrier Record with the given checkpoint ID.
// Barriers flow through the pipeline and trigger stateful operators to snapshot
// their state. When the barrier reaches the sink, the checkpoint is complete.
func NewBarrier(checkpointID string) Record {
	return Record{
		CheckpointID: checkpointID,
		IsBarrier:    true,
	}
}

// WithTimestamp returns a copy of the record with the given timestamp.
func (r Record) WithTimestamp(t time.Time) Record {
	r.Timestamp = t
	return r
}

// WithOffset returns a copy of the record with the given source offset.
func (r Record) WithOffset(offset int64) Record {
	r.Offset = offset
	return r
}

// WithHeader returns a copy of the record with the given header added.
func (r Record) WithHeader(key string, value []byte) Record {
	if r.Headers == nil {
		r.Headers = make(map[string][]byte)
	}
	r.Headers[key] = value
	return r
}
