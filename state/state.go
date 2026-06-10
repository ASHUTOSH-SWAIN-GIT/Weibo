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
// StateBackend is the interface for storing state. Currently only MemoryBackend
// exists, but the interface allows for RocksDB, file-based, or other backends.
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
}