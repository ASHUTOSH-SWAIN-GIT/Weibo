package operator

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/state"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
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

// reduceStateJSON is the JSON representation of ReduceOperator's state,
// used for checkpointing snapshot/restore.
type reduceStateJSON struct {
	Entries map[string][]byte `json:"entries"`
}

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
	Label   string

	// barrierSnapshot, when set, is invoked synchronously from the
	// Process loop each time a barrier passes — the race-free
	// snapshot point (see operator.BarrierSnapshotter).
	barrierSnapshot func(checkpointID string, snapshot []byte, err error)

	// nativeSnapshot, when set, replaces Snapshot() at the barrier:
	// it checkpoints the backend natively (e.g. Pebble hard-links)
	// and returns a state-ref marker instead of serialized state.
	nativeSnapshot func(checkpointID string) ([]byte, error)

	// windowFrontier is the highest window_end observed on an incoming
	// record — the watermark-derived boundary below which every window
	// has closed. It drives state eviction; see evictClosedWindows.
	// RAM-only: after a restore it rebuilds from the first windowed
	// record, which also sweeps any stale entries the checkpoint carried.
	windowFrontier time.Time
}

// SetNativeSnapshot implements NativeSnapshotter.
func (op *ReduceOperator) SetNativeSnapshot(fn func(checkpointID string) ([]byte, error)) {
	op.nativeSnapshot = fn
}

// SetBarrierSnapshot implements BarrierSnapshotter.
func (op *ReduceOperator) SetBarrierSnapshot(fn func(checkpointID string, snapshot []byte, err error)) {
	op.barrierSnapshot = fn
}

// SetStateBackend implements StateConfigurable: the engine injects
// the backend created for this operator's owner ID. Called during
// plan construction, before any record is processed or state restored.
func (op *ReduceOperator) SetStateBackend(b state.StateBackend) {
	op.backend = b
}

// Reduce creates a ReduceOperator with the given reduce function.
// A fresh MemoryBackend is created for this operator's state.
func Reduce(fn ReduceFn) *ReduceOperator {
	return &ReduceOperator{
		Fn:      fn,
		backend: state.NewMemoryBackend(),
	}
}

func (op *ReduceOperator) Name() string     { return "Reduce" }
func (op *ReduceOperator) GetLabel() string { return op.Label }
func (op *ReduceOperator) DescribeOp() OperatorMeta {
	return OperatorMeta{Type: "Reduce", Label: op.Label}
}

// Process reads each record, applies the reduce function with per-key state,
// and emits the new accumulator value downstream. Watermarks and barriers are
// passed through. When records have window_start/window_end headers (from
// Window), state is scoped per-(key, window) so reduce is per-window.
//
// When a barrier arrives, the operator snapshots its state and forwards the
// barrier downstream. This enables checkpointing.
func (op *ReduceOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)

	vs := op.backend.ValueState("reduce")

	for record := range in {
		if record.IsWatermark {
			out <- record
			continue
		}

		if record.IsBarrier {
			// Snapshot NOW, between records: state reflects exactly the
			// pre-barrier stream and nothing is mutating it. Native
			// backends checkpoint via hard-links (cost ∝ changed data);
			// others serialize everything.
			if op.barrierSnapshot != nil {
				var snap []byte
				var err error
				if op.nativeSnapshot != nil {
					snap, err = op.nativeSnapshot(record.CheckpointID)
				} else {
					snap, err = op.Snapshot()
				}
				op.barrierSnapshot(record.CheckpointID, snap, err)
			}
			out <- record
			continue
		}

		sk := StateKey(record)
		vs.SetKey(sk)

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

		// Emit first, then evict: the record's own window is at the
		// frontier, never below it, so its accumulator survives.
		op.advanceWindowFrontier(vs, record)
	}
}

// advanceWindowFrontier tracks the highest window_end seen and, when it
// moves, evicts the state of every window that closed below it.
//
// Why this is a safe completion signal: the WindowOperator only fires a
// window once the watermark has passed its end, and a single watermark
// pass fires EVERY window whose end it covers. So observing a record
// tagged window_end=E proves the watermark reached E, which means every
// window ending before E has already fired and can receive no further
// records (later ones are dropped as late upstream). Its accumulator is
// final and dead.
//
// Watermark records themselves can't be used here: the WindowOperator
// consumes them and does not forward them, so a Reduce placed after a
// Window never sees one.
func (op *ReduceOperator) advanceWindowFrontier(vs state.ValueState, r types.Record) {
	raw, ok := r.Headers["window_end"]
	if !ok {
		return // non-windowed reduce: state is per-key and lives forever
	}
	end, err := time.Parse(time.RFC3339Nano, string(raw))
	if err != nil || !end.After(op.windowFrontier) {
		return
	}
	op.windowFrontier = end
	op.evictClosedWindows(vs)
	vs.SetKey(StateKey(r)) // eviction re-scoped vs; restore the caller's key
}

// evictClosedWindows drops every per-(key, window) entry whose window
// ended before the frontier. Without it a windowed Reduce accumulates one
// dead entry per (key, window) forever — unbounded memory on a long-running
// pipeline, and an ever-growing checkpoint, since Snapshot serializes the
// whole namespace.
//
// The scan runs only when the frontier advances (once per window, not per
// record). It leaves vs scoped to the last evicted key — the caller
// re-scopes.
func (op *ReduceOperator) evictClosedWindows(vs state.ValueState) {
	var stale []string
	for _, k := range vs.Keys() {
		end, ok := stateKeyWindowEnd(k)
		if ok && end.Before(op.windowFrontier) {
			stale = append(stale, k)
		}
	}
	for _, k := range stale {
		vs.SetKey(k)
		vs.Clear()
	}
}

// stateKeyWindowEnd extracts the window end from a StateKey of the form
// "<key>/<window_start>/<window_end>". It reports ok only when BOTH bounds
// parse as RFC3339Nano — a plain (non-windowed) key that happens to contain
// slashes must never be mistaken for a window and evicted.
func stateKeyWindowEnd(sk string) (time.Time, bool) {
	lastSlash := strings.LastIndex(sk, "/")
	if lastSlash < 0 {
		return time.Time{}, false
	}
	prevSlash := strings.LastIndex(sk[:lastSlash], "/")
	if prevSlash < 0 {
		return time.Time{}, false
	}
	if _, err := time.Parse(time.RFC3339Nano, sk[prevSlash+1:lastSlash]); err != nil {
		return time.Time{}, false
	}
	end, err := time.Parse(time.RFC3339Nano, sk[lastSlash+1:])
	if err != nil {
		return time.Time{}, false
	}
	return end, true
}

// Snapshot returns the operator's current per-key state as JSON bytes.
func (op *ReduceOperator) Snapshot() ([]byte, error) {
	vs := op.backend.ValueState("reduce")
	entries := vs.SnapshotAll()

	data := reduceStateJSON{Entries: entries}
	return json.Marshal(data)
}

// Restore replaces the operator's internal state from JSON bytes produced by Snapshot.
func (op *ReduceOperator) Restore(data []byte) error {
	var stateData reduceStateJSON
	if err := json.Unmarshal(data, &stateData); err != nil {
		return err
	}

	vs := op.backend.ValueState("reduce")
	return vs.RestoreAll(stateData.Entries)
}

// Clone returns a copy with the same reduce function and label but
// a fresh in-memory state backend. Used for per-worker isolation
// in keyed parallel execution.
func (op *ReduceOperator) Clone() Operator {
	return &ReduceOperator{
		Fn:      op.Fn,
		backend: state.NewMemoryBackend(),
		Label:   op.Label,
	}
}

// Backend returns the underlying state backend. Used by the
// checkpoint coordinator to check for native checkpointing support.
func (op *ReduceOperator) Backend() state.StateBackend { return op.backend }

// StateKey returns the key used for Reduce state lookup.
// If the record has window metadata, the key includes window bounds
// so reduce is scoped per-(key, window).
func StateKey(r types.Record) string {
	if ws, ok := r.Headers["window_start"]; ok {
		we := r.Headers["window_end"]
		return string(r.Key) + "/" + string(ws) + "/" + string(we)
	}
	return string(r.Key)
}
