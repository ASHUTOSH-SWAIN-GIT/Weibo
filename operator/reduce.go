package operator

import (
	"mailer/state"
	"mailer/types"
)

// ReduceFn takes the current accumulator and the incoming record,
// and returns the new accumulator bytes. The accumulator is persisted per key.
//
// On the first record for a given key, accum will be nil (no previous state).
// The function should return the initial accumulator based on the first record.
//
// Example (count per key):
//
//	reduce := Reduce(func(accum []byte, curr types.Record) []byte {
//	    count := 0
//	    if accum != nil {
//	        count = int(binary.BigEndian.Uint64(accum))
//	    }
//	    count++
//	    buf := make([]byte, 8)
//	    binary.BigEndian.PutUint64(buf, uint64(count))
//	    return buf
//	})
type ReduceFn func(accum []byte, curr types.Record) []byte

// ReduceOperator maintains per-key state and applies a reduce function
// to merge each incoming record into the accumulator.
//
// It must be used after KeyBy — the key determines which accumulator to use.
// On every record:
//  1. Look up the ValueState for the record's key
//  2. If state exists, use it as accumulator; otherwise accum is nil
//  3. Call reduceFn(accum, record) to get the new accumulator
//  4. Save the new accumulator to state
//  5. Emit the updated accumulator downstream as a new Record
type ReduceOperator struct {
	Fn      ReduceFn
	backend state.StateBackend
}

// Reduce creates a ReduceOperator with the given reduce function.
// A fresh MemoryBackend is created for this operator's state.
func Reduce(fn ReduceFn) *ReduceOperator {
	return &ReduceOperator{
		Fn:      fn,
		backend: state.NewMemoryBackend(),
	}
}

// Process reads each record, applies the reduce function with per-key state,
// and emits the new accumulator value downstream as a Record.
func (op *ReduceOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)

	vs := op.backend.ValueState("reduce")

	for record := range in {
		key := string(record.Key)
		vs.SetKey(key)

		accum := vs.Get()
		newAccum := op.Fn(accum, record)
		vs.Set(newAccum)

		out <- types.Record{
			Key:       record.Key,
			Value:     newAccum,
			Timestamp: record.Timestamp,
			Offset:    record.Offset,
			Headers:   record.Headers,
		}
	}
}