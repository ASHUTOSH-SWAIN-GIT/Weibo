// Package state provides keyed state storage for stateful stream processing.
//
// After a stream is partitioned by KeyBy, each key gets its own isolated state.
// State is accessed within a Reduce or Process function — you never touch state
// directly from outside the pipeline.
//
// The two state types are:
//   - ValueState: a single value per key (like a per-key variable)
//   - ListState: an ordered list per key (like a per-key append-only list)
//
// StateBackend is the interface for storing state. Two backends exist today —
// an in-memory backend and a durable Pebble (LSM) backend — and the interface
// allows for other backends (RocksDB, file-based, etc.).
package state

// StateBackend persists keyed state for stateful operators.
// Each Reduce/Process operator gets its own StateBackend instance,
// so keys don't collide across different operators.
type StateBackend interface {
	ValueState(name string) ValueState
	ListState(name string) ListState
}

// ValueState holds a single value per key.
// Think of it as a map[string][]byte — each key gets one value.
//
// Before calling Get/Set/Clear, you must call SetKey to scope
// the operation to the current record's key.
type ValueState interface {
	// SetKey scopes all subsequent Get/Set/Clear calls to this key.
	SetKey(key string)

	// Get returns the stored value for the current key, or nil if none exists.
	Get() []byte

	// Set stores a value for the current key, overwriting any previous value.
	Set(value []byte)

	// Clear removes the value for the current key.
	Clear()

	// Keys returns every key in this namespace that currently holds a
	// value. Order is unspecified. It is independent of the key set via
	// SetKey. Used by operators that must iterate their whole keyspace
	// without materializing the values — e.g. Reduce evicting the state
	// of windows the watermark has closed. Prefer this over SnapshotAll
	// when only the keys are needed: SnapshotAll copies every value.
	Keys() []string

	// SnapshotAll returns a copy of all key-value pairs in this state namespace.
	// Used for checkpointing.
	SnapshotAll() map[string][]byte

	// RestoreAll replaces all key-value pairs in this state namespace.
	// Used for recovery from a checkpoint.
	RestoreAll(entries map[string][]byte) error
}

// ListState holds an ordered list of values per key.
// Think of it as a map[string][][]byte — each key gets an append-only list.
//
// Before calling Append/GetAll/Clear, you must call SetKey to scope
// the operation to the current record's key.
type ListState interface {
	// SetKey scopes all subsequent calls to this key.
	SetKey(key string)

	// Append adds a value to the list for the current key.
	Append(value []byte)

	// GetAll returns all values for the current key, or nil if none exist.
	GetAll() [][]byte

	// Clear removes all values for the current key.
	Clear()

	// Keys returns every key in this namespace that currently holds at
	// least one entry. Order is unspecified. It is independent of the
	// key set via SetKey. Used by operators that must iterate all their
	// keyed lists — e.g. Window firing every window whose end has
	// passed the watermark. Returns the keys only (small); records
	// within a key are loaded lazily via SetKey + GetAll.
	Keys() []string
}

// Checkpointable is an optional interface for backends that can
// checkpoint without serializing all state.  The engine calls
// CheckpointTo during a barrier-triggered snapshot to produce a
// portable copy of the current state directory (hard-linked),
// and RestoreFrom on recovery to rebuild the live DB from that
// copy.  This keeps checkpoint metadata small regardless of
// state size.
type Checkpointable interface {
	CheckpointTo(dir string) error
	RestoreFrom(dir string) error
}
