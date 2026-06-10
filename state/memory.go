package state

import (
	"sync"
)

// MemoryBackend is an in-memory StateBackend using maps protected by a mutex.
// Each key gets its own entry. State is lost when the process restarts.
type MemoryBackend struct {
	mu    sync.Mutex
	value map[string]map[string][]byte   // name -> key -> value
	list  map[string]map[string][][]byte // name -> key -> list of values
}

// NewMemoryBackend creates a fresh in-memory state backend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		value: make(map[string]map[string][]byte),
		list:  make(map[string]map[string][][]byte),
	}
}

func (m *MemoryBackend) ValueState(name string) ValueState {
	m.mu.Lock()
	if m.value[name] == nil {
		m.value[name] = make(map[string][]byte)
	}
	m.mu.Unlock()
	return &memoryValueState{
		backend: m,
		name:    name,
		data:    m.value[name],
	}
}

func (m *MemoryBackend) ListState(name string) ListState {
	m.mu.Lock()
	if m.list[name] == nil {
		m.list[name] = make(map[string][][]byte)
	}
	m.mu.Unlock()
	return &memoryListState{
		backend: m,
		name:    name,
		data:    m.list[name],
	}
}

// memoryValueState implements ValueState backed by a map.
// The current key is set via SetKey before Get/Set/Clear calls.
type memoryValueState struct {
	mu      sync.Mutex
	backend *MemoryBackend
	name    string
	key     string
	data    map[string][]byte
}

func (vs *memoryValueState) SetKey(key string) {
	vs.mu.Lock()
	vs.key = key
	vs.mu.Unlock()
}

func (vs *memoryValueState) Get() []byte {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Refresh the data map reference in case it was recreated.
	vs.data = vs.backend.value[vs.name]
	if vs.data == nil {
		return nil
	}
	val, ok := vs.data[vs.key]
	if !ok {
		return nil
	}
	return val
}

func (vs *memoryValueState) Set(value []byte) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.data = vs.backend.value[vs.name]
	if vs.data == nil {
		vs.data = make(map[string][]byte)
		vs.backend.value[vs.name] = vs.data
	}
	vs.data[vs.key] = value
}

func (vs *memoryValueState) Clear() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.data = vs.backend.value[vs.name]
	if vs.data != nil {
		delete(vs.data, vs.key)
	}
}

// memoryListState implements ListState backed by a map of slices.
type memoryListState struct {
	mu      sync.Mutex
	backend *MemoryBackend
	name    string
	key     string
	data    map[string][][]byte
}

func (ls *memoryListState) SetKey(key string) {
	ls.mu.Lock()
	ls.key = key
	ls.mu.Unlock()
}

func (ls *memoryListState) Append(value []byte) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.data = ls.backend.list[ls.name]
	if ls.data == nil {
		ls.data = make(map[string][][]byte)
		ls.backend.list[ls.name] = ls.data
	}
	ls.data[ls.key] = append(ls.data[ls.key], value)
}

func (ls *memoryListState) GetAll() [][]byte {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.data = ls.backend.list[ls.name]
	if ls.data == nil {
		return nil
	}
	val, ok := ls.data[ls.key]
	if !ok {
		return nil
	}
	return val
}

func (ls *memoryListState) Clear() {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.data = ls.backend.list[ls.name]
	if ls.data != nil {
		delete(ls.data, ls.key)
	}
}
