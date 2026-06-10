package types

import "time"

// Record is the fundamental data unit flowing through a stream pipeline.
// Every source emits Records, every operator transforms them, every sink receives them.
//
// A Record can be either a data record or a watermark marker:
//   - Data record: IsWatermark == false, carries key/value/timestamp
//   - Watermark:   IsWatermark == true, carries only a Timestamp that says
//     "no records with event time < this timestamp will arrive after this point"
//
// Watermarks drive window firing. When a watermark passes a window's end time,
// that window is considered complete and its results are emitted downstream.
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