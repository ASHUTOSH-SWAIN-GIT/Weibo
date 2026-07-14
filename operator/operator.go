package operator

import (
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/state"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// Operator transforms an input stream into an output stream.
// Each operator reads from an input channel, applies a transformation,
// and writes to an output channel. The output channel must be closed
// when the operator is done processing.
type Operator interface {
	// Name returns the operator type name (e.g. "Map", "Filter", "Window").
	// Used by the dashboard to display the pipeline graph.
	Name() string

	Process(in <-chan types.Record, out chan<- types.Record)
}

// Labeled operators carry a user-provided label for display in the dashboard.
// If no label is set, the dashboard falls back to Name().
type Labeled interface {
	GetLabel() string
}

// Snapshotable operators can snapshot and restore their internal state.
// This is used for checkpointing — when a barrier passes through a
// stateful operator, its state is captured and can be restored on recovery.
type Snapshotable interface {
	// Snapshot returns the operator's current state as opaque bytes.
	// The returned bytes must be sufficient to fully reconstruct the
	// operator's state via Restore.
	Snapshot() ([]byte, error)

	// Restore replaces the operator's internal state from the given bytes.
	// The bytes must have been produced by a prior call to Snapshot on the
	// same operator type. Restore is called before the pipeline starts.
	Restore(data []byte) error
}

// OperatorMeta describes the configuration of an operator for the dashboard.
type OperatorMeta struct {
	Type   string            `json:"type"`
	Label  string            `json:"label,omitempty"`
	Config map[string]string `json:"config,omitempty"`
}

// DescribableOperator is an optional interface that operators implement
// to expose their configuration for the dashboard.
type DescribableOperator interface {
	DescribeOp() OperatorMeta
}

// KeySelector extracts a routing key from a record. Used by KeyBy to
// deterministically route records to keyed workers.
type KeySelector func(types.Record) []byte

// Cloneable is an optional interface for operators that can be
// duplicated with independent state. Stateful operators (Window,
// Reduce) implement this so each keyed worker gets its own instance
// with an isolated state backend.
type Cloneable interface {
	Clone() Operator
}

// StateConfigurable is implemented by stateful operators whose state
// backend can be injected by the engine. The planner assigns each
// operator instance a backend created from the environment's
// BackendFactory (WithStateBackend), keyed by the owner ID used in
// checkpoint data. Operators keep a self-created in-memory backend as
// the default when no factory is configured.
//
// SetStateBackend is called during plan construction, strictly before
// the operator processes any record or has state restored into it.
type StateConfigurable interface {
	SetStateBackend(b state.StateBackend)
}

// BarrierSnapshotter is implemented by stateful operators that can
// snapshot their state synchronously when a checkpoint barrier passes
// through their Process loop. Snapshotting there — between two
// records — is the only race-free point: the operator is quiescent
// and its state reflects exactly the pre-barrier stream. Snapshots
// taken from outside (e.g. when the barrier reaches the end of the
// pipeline) race with the operator processing post-barrier records.
type BarrierSnapshotter interface {
	// SetBarrierSnapshot registers a callback invoked with the
	// operator's state each time a barrier passes through it.
	SetBarrierSnapshot(fn func(checkpointID string, snapshot []byte, err error))
}

// SingleProcessor is implemented by stateless operators that can be
// invoked directly, one record at a time, without channels. The
// execution engine chains SingleProcessors inside a stage as plain
// function calls.
//
// The returned slice length encodes the operator semantics:
// 0 = record dropped (Filter), 1 = transformed (Map), N = fan-out (FlatMap).
//
// Barriers and watermarks are never passed to ProcessOne — the stage
// machinery forwards and aligns them itself.
type SingleProcessor interface {
	ProcessOne(r types.Record) []types.Record
}

// Parallel is implemented by operators that can run with multiple
// workers. The planner groups consecutive operators with the same
// parallelism into one execution stage.
type Parallel interface {
	Parallelism() int
	SetParallelism(n int)
}

// Parallelizable provides the Parallel implementation for stateless
// operators via embedding. Zero value means parallelism 1.
type Parallelizable struct {
	Par int
}

// Parallelism returns the configured worker count (minimum 1).
func (p *Parallelizable) Parallelism() int {
	if p.Par < 1 {
		return 1
	}
	return p.Par
}

// SetParallelism sets the worker count. Values < 1 are treated as 1.
// Note: with parallelism > 1, record order is not preserved across workers.
func (p *Parallelizable) SetParallelism(n int) { p.Par = n }
