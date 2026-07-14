package state

// BackendFactory creates a StateBackend for one state owner. An owner
// is a stateful operator instance, identified by the same stable IDs
// the checkpoint format uses: "op-<i>" for a top-level operator,
// "worker-<idx>" for a keyed-worker clone. The engine calls the
// factory once per owner while building the execution plan, so each
// owner gets isolated state (per-worker isolation is what keeps keyed
// state and barrier-time snapshots consistent).
//
// Backends that also implement io.Closer are closed by the engine
// when the pipeline finishes.
type BackendFactory func(ownerID string) (StateBackend, error)

// InMemory returns the default factory: a fresh MemoryBackend per
// owner. State lives in RAM and is captured/restored only through
// checkpoints.
func InMemory() BackendFactory {
	return func(string) (StateBackend, error) {
		return NewMemoryBackend(), nil
	}
}
