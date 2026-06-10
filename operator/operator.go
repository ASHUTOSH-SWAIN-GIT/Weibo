package operator

import "mailer/types"

// Operator transforms an input stream into an output stream.
// Each operator reads from an input channel, applies a transformation,
// and writes to an output channel. The output channel must be closed
// when the operator is done processing.
type Operator interface {
	Process(in <-chan types.Record, out chan<- types.Record)
}
