package operator

import "mailer/types"

// Operator transforms an input stream into an output stream.
// Each operator reads from an input channel, applies a transformation,
// and writes to an output channel. The output channel must be closed
// when the operator is done processing.
type Operator interface {
	Process(in <-chan types.Record, out chan<- types.Record)
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
