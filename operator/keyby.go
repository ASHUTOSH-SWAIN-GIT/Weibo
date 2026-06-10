package operator

import "mailer/types"

// KeyByOperator partitions the stream by key. All records with the same key
// are routed to the same downstream partition. This is required before
// stateful operations like Reduce, since each key gets its own state.
//
// After KeyBy, the record's Key field is set to the result of the key function.
type KeyByOperator struct {
	Fn func(types.Record) []byte
}

// KeyBy creates a KeyByOperator with the given key selector function.
func KeyBy(fn func(types.Record) []byte) *KeyByOperator {
	return &KeyByOperator{Fn: fn}
}

// Process reads each record from in, applies the key selector function
// to set the record's Key field, and writes it to out.
func (op *KeyByOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)
	for record := range in {
		record.Key = op.Fn(record)
		out <- record
	}
}