package operator

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/state"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/window"
)

// State namespaces used inside the operator's injected backend.
const (
	windowRecordsNS   = "window_records" // ListState: windowKey -> buffered records
	windowWatermarkNS = "window_wm"      // ValueState: "wm" -> UnixNano watermark
	windowWatermarkK  = "wm"
)

// WindowOperator buffers records into time-based windows and fires them
// when the watermark passes the window's end time.
//
// How it works:
//  1. Data records are assigned to windows by the WindowAssigner and
//     buffered in the injected state backend (Pebble when configured),
//     so window contents live on disk instead of the Go heap.
//  2. Watermark records advance the current watermark.
//  3. When the watermark passes a window's end time, that window is
//     "closed" — all its buffered records are emitted as a single result.
//  4. Late records (timestamp < current watermark) are dropped.
//  5. Checkpoint barriers snapshot state (natively via the backend when
//     it supports hard-link checkpoints, else serialized to JSON).
//
// Must be used after KeyBy in a keyed stream so each key gets its
// own set of windows.
//
// State layout in the backend:
//   - records:   ListState(windowRecordsNS), keyed by windowKey.String()
//                ("<recordKey>/<start>/<end>"); each entry is one
//                serialized record. Window bounds are recovered from the
//                key, so the set of open windows is exactly Keys().
//   - watermark: ValueState(windowWatermarkNS)["wm"] — 8-byte UnixNano.
//                A RAM working copy (currentWatermark) serves the
//                per-record late-drop check without a backend read.
type WindowOperator struct {
	Assigner    window.WindowAssigner
	IdleTimeout time.Duration
	Label       string

	backend          state.StateBackend
	currentWatermark time.Time // RAM working copy of the durable watermark

	lastRecordTime time.Time
	timer          *time.Timer

	// barrierSnapshot / nativeSnapshot are invoked synchronously from
	// the Process loop when a barrier passes — the race-free snapshot
	// point (see operator.BarrierSnapshotter / NativeSnapshotter).
	barrierSnapshot func(checkpointID string, snapshot []byte, err error)
	nativeSnapshot  func(checkpointID string) ([]byte, error)
}

// Window creates a WindowOperator with the given window assigner.
// Supported assigners: window.Tumbling, window.Sliding, window.Session.
// A default in-memory backend is used until one is injected.
func Window(assigner window.WindowAssigner) *WindowOperator {
	return &WindowOperator{
		Assigner: assigner,
		backend:  state.NewMemoryBackend(),
	}
}

// SetStateBackend implements StateConfigurable: the engine injects the
// backend created for this operator's owner ID. Called during plan
// construction, before any record is processed or state restored.
func (op *WindowOperator) SetStateBackend(b state.StateBackend) { op.backend = b }

// SetBarrierSnapshot implements BarrierSnapshotter.
func (op *WindowOperator) SetBarrierSnapshot(fn func(checkpointID string, snapshot []byte, err error)) {
	op.barrierSnapshot = fn
}

// SetNativeSnapshot implements NativeSnapshotter.
func (op *WindowOperator) SetNativeSnapshot(fn func(checkpointID string) ([]byte, error)) {
	op.nativeSnapshot = fn
}

// Backend implements NativeSnapshotter: exposes the backend so the
// engine can detect state.Checkpointable support.
func (op *WindowOperator) Backend() state.StateBackend { return op.backend }

func (op *WindowOperator) Name() string     { return "Window" }
func (op *WindowOperator) GetLabel() string { return op.Label }
func (op *WindowOperator) DescribeOp() OperatorMeta {
	cfg := map[string]string{"type": op.Assigner.Name()}
	if op.IdleTimeout > 0 {
		cfg["idle_timeout"] = op.IdleTimeout.String()
	}
	return OperatorMeta{Type: "Window", Label: op.Label, Config: cfg}
}

// Clone returns a copy with the same configuration but a fresh default
// backend (the engine injects the real one afterwards). Used for
// per-worker isolation in keyed parallel execution.
func (op *WindowOperator) Clone() Operator {
	return &WindowOperator{
		Assigner:    op.Assigner,
		IdleTimeout: op.IdleTimeout,
		Label:       op.Label,
		backend:     state.NewMemoryBackend(),
	}
}

// WithIdleTimeout sets the idle timeout for the window operator.
// If no records arrive for this duration after windowing, the operator
// fires all pending windows and stops. Useful for infinite streams
// that don't receive shutdown signals.
func (op *WindowOperator) WithIdleTimeout(d time.Duration) *WindowOperator {
	op.IdleTimeout = d
	return op
}

// CurrentWatermark returns the operator's current watermark timestamp.
// Used for testing and checkpointing.
func (op *WindowOperator) CurrentWatermark() time.Time { return op.currentWatermark }

// Process reads records and watermarks, buffers data records into
// windows (in the backend), and emits results when watermarks indicate
// windows are complete. If IdleTimeout is set, the operator fires
// remaining windows and exits when no records arrive within the timeout.
func (op *WindowOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)

	recState := op.backend.ListState(windowRecordsNS)
	op.currentWatermark = op.loadWatermark() // recover watermark (e.g. after native restore)

	if op.IdleTimeout > 0 {
		op.timer = time.NewTimer(op.IdleTimeout)
		defer op.timer.Stop()
	}

	for {
		select {
		case record, ok := <-in:
			if !ok {
				op.flushRemaining(recState, out)
				return
			}
			if op.timer != nil {
				op.timer.Reset(op.IdleTimeout)
			}
			if record.IsWatermark {
				op.handleWatermark(recState, record, out)
				continue
			}
			if record.IsBarrier {
				// Snapshot NOW, between records: buffered windows and
				// the watermark reflect exactly the pre-barrier stream.
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

			// Drop late records (timestamp below current watermark).
			if !op.currentWatermark.IsZero() && record.Timestamp.Before(op.currentWatermark) {
				continue
			}

			op.handleDataRecord(recState, record)

		case <-op.timerFire():
			op.flushRemaining(recState, out)
			return
		}
	}
}

// timerFire returns a channel that fires when the idle timer expires,
// or nil if no idle timeout is configured.
func (op *WindowOperator) timerFire() <-chan time.Time {
	if op.timer != nil {
		return op.timer.C
	}
	return nil
}

// handleDataRecord assigns the record to one or more windows and buffers
// it in the backend.
func (op *WindowOperator) handleDataRecord(recState state.ListState, record types.Record) {
	recBytes := encodeRecord(record)
	wins := op.Assigner.AssignWindows(record.Timestamp)
	for _, win := range wins {
		op.assignToWindow(recState, string(record.Key), win, recBytes)
	}
}

// assignToWindow places a record into the appropriate window list,
// merging overlapping session windows for the same key when bounds
// change. Tumbling/sliding windows are pre-aligned, so records append
// straight to their window key. Session windows may overlap an existing
// session, in which case the two are merged by expanding the bounds —
// which changes the window key, so the existing records are moved to the
// new key (a get+clear+re-append). Sessions that buffer many records and
// merge repeatedly pay for those moves; bounded-size sessions do not.
func (op *WindowOperator) assignToWindow(recState state.ListState, key string, win window.Window, recBytes []byte) {
	wk := toWindowKey(key, win).String()

	if op.Assigner.IsSession() {
		for _, existingStr := range recState.Keys() {
			ek := parseWindowKey(existingStr)
			if ek.Key != key {
				continue
			}
			eStart := time.Unix(0, ek.Start).UTC()
			eEnd := time.Unix(0, ek.End).UTC()
			if !win.Start.Before(eEnd) || !win.End.After(eStart) {
				continue // no overlap
			}

			newStart, newEnd := eStart, eEnd
			if win.Start.Before(newStart) {
				newStart = win.Start
			}
			if win.End.After(newEnd) {
				newEnd = win.End
			}
			mergedStr := toWindowKey(key, window.Window{Start: newStart, End: newEnd}).String()

			if mergedStr == existingStr {
				// New record fits within the existing session bounds.
				recState.SetKey(existingStr)
				recState.Append(recBytes)
				return
			}
			// Bounds expanded: move existing records under the new key.
			recState.SetKey(existingStr)
			moved := recState.GetAll()
			recState.Clear()
			recState.SetKey(mergedStr)
			for _, r := range moved {
				recState.Append(r)
			}
			recState.Append(recBytes)
			return
		}
	}

	recState.SetKey(wk)
	recState.Append(recBytes)
}

// handleWatermark advances the watermark and fires all windows whose
// end time is <= the new watermark.
func (op *WindowOperator) handleWatermark(recState state.ListState, watermark types.Record, out chan<- types.Record) {
	if watermark.Timestamp.After(op.currentWatermark) {
		op.currentWatermark = watermark.Timestamp
		op.storeWatermark()
	}
	for _, keyStr := range recState.Keys() {
		wk := parseWindowKey(keyStr)
		if time.Unix(0, wk.End).UTC().After(op.currentWatermark) {
			continue // window still open
		}
		op.fireWindow(recState, keyStr, out)
	}
}

// flushRemaining fires all buffered windows and clears state.
func (op *WindowOperator) flushRemaining(recState state.ListState, out chan<- types.Record) {
	for _, keyStr := range recState.Keys() {
		op.fireWindow(recState, keyStr, out)
	}
}

// fireWindow emits every record in the window under keyStr (tagged with
// window bounds) and clears it.
func (op *WindowOperator) fireWindow(recState state.ListState, keyStr string, out chan<- types.Record) {
	wk := parseWindowKey(keyStr)
	win := window.Window{
		Start: time.Unix(0, wk.Start).UTC(),
		End:   time.Unix(0, wk.End).UTC(),
	}
	recState.SetKey(keyStr)
	for _, rb := range recState.GetAll() {
		out <- tagWithWindow(decodeRecord(rb), win)
	}
	recState.Clear()
}

// loadWatermark reads the durable watermark from the backend, or the
// zero time if none has been stored.
func (op *WindowOperator) loadWatermark() time.Time {
	vs := op.backend.ValueState(windowWatermarkNS)
	vs.SetKey(windowWatermarkK)
	b := vs.Get()
	if len(b) != 8 {
		return time.Time{}
	}
	return time.Unix(0, int64(binary.BigEndian.Uint64(b))).UTC()
}

// storeWatermark write-throughs the RAM watermark to the backend so it
// survives a native (backend-level) checkpoint restore.
func (op *WindowOperator) storeWatermark() {
	vs := op.backend.ValueState(windowWatermarkNS)
	vs.SetKey(windowWatermarkK)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(op.currentWatermark.UnixNano()))
	vs.Set(buf[:])
}

// windowKey uniquely identifies a window by key + start + end times as
// Unix nanoseconds. Including the record key ensures records from
// different keys aren't merged into the same window.
type windowKey struct {
	Key   string
	Start int64
	End   int64
}

// windowState / recordJSON / windowOperatorSnapshotJSON are the
// JSON-serializable forms used for compatible-mode checkpointing
// (memory backend) and as the per-record list entry encoding.
type windowState struct {
	Win     window.Window `json:"win"`
	Records []recordJSON  `json:"records"`
}

type recordJSON struct {
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"value,omitempty"`
	Timestamp int64             `json:"timestamp"` // UnixNano
	Offset    int64             `json:"offset"`
	Headers   map[string][]byte `json:"headers,omitempty"`
}

type windowOperatorSnapshotJSON struct {
	CurrentWatermark int64                  `json:"current_watermark"` // UnixNano
	Windows          map[string]windowState `json:"windows"`
}

// Snapshot serializes the backend's window contents to JSON. Used for
// the memory backend and any backend without native checkpoint support.
func (op *WindowOperator) Snapshot() ([]byte, error) {
	recState := op.backend.ListState(windowRecordsNS)
	snapshot := windowOperatorSnapshotJSON{
		CurrentWatermark: op.currentWatermark.UnixNano(),
		Windows:          make(map[string]windowState),
	}
	for _, keyStr := range recState.Keys() {
		wk := parseWindowKey(keyStr)
		win := window.Window{
			Start: time.Unix(0, wk.Start).UTC(),
			End:   time.Unix(0, wk.End).UTC(),
		}
		recState.SetKey(keyStr)
		entries := recState.GetAll()
		recs := make([]recordJSON, 0, len(entries))
		for _, rb := range entries {
			recs = append(recs, decodeRecordJSON(rb))
		}
		snapshot.Windows[keyStr] = windowState{Win: win, Records: recs}
	}
	return json.Marshal(snapshot)
}

// Restore replaces the backend's window contents from JSON produced by
// Snapshot. The backend is injected before Restore runs.
func (op *WindowOperator) Restore(data []byte) error {
	var snapshot windowOperatorSnapshotJSON
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}
	op.currentWatermark = time.Unix(0, snapshot.CurrentWatermark).UTC()
	op.storeWatermark()

	recState := op.backend.ListState(windowRecordsNS)
	for _, k := range recState.Keys() { // clear any pre-existing state
		recState.SetKey(k)
		recState.Clear()
	}
	for keyStr, ws := range snapshot.Windows {
		recState.SetKey(keyStr)
		for _, r := range ws.Records {
			recState.Append(encodeRecordJSON(r))
		}
	}
	return nil
}

// ---- record <-> bytes ------------------------------------------------------

func encodeRecord(r types.Record) []byte {
	b, _ := json.Marshal(recordToJSON(r))
	return b
}

func encodeRecordJSON(r recordJSON) []byte {
	b, _ := json.Marshal(r)
	return b
}

func decodeRecord(b []byte) types.Record {
	return recordFromJSON(decodeRecordJSON(b))
}

func decodeRecordJSON(b []byte) recordJSON {
	var r recordJSON
	_ = json.Unmarshal(b, &r)
	return r
}

func recordToJSON(r types.Record) recordJSON {
	return recordJSON{
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: r.Timestamp.UnixNano(),
		Offset:    r.Offset,
		Headers:   r.Headers,
	}
}

func recordFromJSON(r recordJSON) types.Record {
	return types.Record{
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: time.Unix(0, r.Timestamp).UTC(),
		Offset:    r.Offset,
		Headers:   r.Headers,
	}
}

// tagWithWindow returns a copy of the record with window metadata in Headers.
func tagWithWindow(r types.Record, win window.Window) types.Record {
	headers := make(map[string][]byte, len(r.Headers)+2)
	for k, v := range r.Headers {
		headers[k] = v
	}
	headers["window_start"] = []byte(win.Start.Format(time.RFC3339Nano))
	headers["window_end"] = []byte(win.End.Format(time.RFC3339Nano))
	return types.Record{
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: r.Timestamp,
		Offset:    r.Offset,
		Headers:   headers,
	}
}

// toWindowKey converts a key and Window to a comparable window key.
func toWindowKey(key string, win window.Window) windowKey {
	return windowKey{
		Key:   key,
		Start: win.Start.UnixNano(),
		End:   win.End.UnixNano(),
	}
}

func (k windowKey) String() string {
	return k.Key + "/" +
		time.Unix(0, k.Start).UTC().Format(time.RFC3339Nano) + "/" +
		time.Unix(0, k.End).UTC().Format(time.RFC3339Nano)
}

// parseWindowKey reverses windowKey.String(). The record key may itself
// contain "/", so the two window-bound timestamps are taken from the
// last two "/"-separated fields.
func parseWindowKey(s string) windowKey {
	lastSlash, secondLastSlash := -1, -1
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			if lastSlash == -1 {
				lastSlash = i
			} else {
				secondLastSlash = i
				break
			}
		}
	}
	if secondLastSlash == -1 || lastSlash == -1 {
		return windowKey{}
	}
	start, _ := time.Parse(time.RFC3339Nano, s[secondLastSlash+1:lastSlash])
	end, _ := time.Parse(time.RFC3339Nano, s[lastSlash+1:])
	return windowKey{
		Key:   s[:secondLastSlash],
		Start: start.UnixNano(),
		End:   end.UnixNano(),
	}
}
