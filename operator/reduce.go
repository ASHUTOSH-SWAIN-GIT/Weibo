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
// and emits the new accumulator value downstream. Watermarks are passed through.
// When records have window_start/window_end headers (from Window), state is
// scoped per-(key, window) so reduce is per-window.
func (op *ReduceOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)

	vs := op.backend.ValueState("reduce")

	for record := range in {
		if record.IsWatermark {
			out <- record
			continue
		}

		stateKey := stateKey(record)
		vs.SetKey(stateKey)

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

// stateKey returns the key used for Reduce state lookup.
// If the record has window metadata, the key includes window bounds
// so reduce is scoped per-(key, window).
func stateKey(r types.Record) string {
	if ws, ok := r.Headers["window_start"]; ok {
		we := r.Headers["window_end"]
		return string(r.Key) + "/" + string(ws) + "/" + string(we)
	}
	return string(r.Key)
}
