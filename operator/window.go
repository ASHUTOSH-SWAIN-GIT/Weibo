package operator

import (
	"encoding/json"
	"time"

	"mailer/types"
	"mailer/window"
)

// WindowOperator buffers records into time-based windows and fires them
// when the watermark passes the window's end time.
//
// How it works:
//  1. Data records are assigned to windows by the WindowAssigner.
//  2. Watermark records advance the current watermark.
//  3. When the watermark passes a window's end time, that window is
//     "closed" — all its buffered records are emitted as a single result.
//  4. Late records (timestamp < current watermark) are dropped.
//  5. Checkpoint barriers are forwarded downstream (window buffers
//     are captured as part of the pipeline checkpoint).
//
// Must be used after KeyBy in a keyed stream so each key gets its
// own set of windows.
type WindowOperator struct {
	Assigner         window.WindowAssigner
	currentWatermark time.Time
	windows          map[windowKey]*windowState
	IdleTimeout      time.Duration
	lastRecordTime   time.Time
	timer            *time.Timer
}

// windowKey uniquely identifies a window by key + start + end times
// as Unix nanoseconds (so it's comparable and hashable as a map key).
// Including the record key ensures records from different keys don't
// get merged into the same window.
type windowKey struct {
	Key   string
	Start int64
	End   int64
}

// windowState holds the records buffered in a single window.
type windowState struct {
	Win     window.Window `json:"win"`
	Records []recordJSON  `json:"records"`
}

// recordJSON is the JSON-serializable representation of a types.Record,
// used for checkpointing window state.
type recordJSON struct {
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"value,omitempty"`
	Timestamp int64             `json:"timestamp"` // UnixNano
	Offset    int64             `json:"offset"`
	Headers   map[string][]byte `json:"headers,omitempty"`
}

// windowOperatorSnapshotJSON is the JSON representation of WindowOperator's
// state for checkpointing.
type windowOperatorSnapshotJSON struct {
	CurrentWatermark int64                  `json:"current_watermark"` // UnixNano
	Windows          map[string]windowState `json:"windows"`
}

// WithIdleTimeout sets the idle timeout for the window operator.
// If no records arrive for this duration after windowing, the operator
// fires all pending windows and stops. Useful for infinite streams
// that don't receive shutdown signals.
func (op *WindowOperator) WithIdleTimeout(d time.Duration) *WindowOperator {
	op.IdleTimeout = d
	return op
}

// Window creates a WindowOperator with the given window assigner.
// Supported assigners: window.Tumbling, window.Sliding, window.Session.
func Window(assigner window.WindowAssigner) *WindowOperator {
	return &WindowOperator{
		Assigner: assigner,
		windows:  make(map[windowKey]*windowState),
	}
}

// CurrentWatermark returns the operator's current watermark timestamp.
// Used for testing and checkpointing.
func (op *WindowOperator) CurrentWatermark() time.Time {
	return op.currentWatermark
}

// Process reads records and watermarks, buffers data records into windows,
// and emits results when watermarks indicate windows are complete.
// If IdleTimeout is set, the operator fires remaining windows and exits
// when no records arrive within the timeout.
func (op *WindowOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)

	if op.IdleTimeout > 0 {
		op.timer = time.NewTimer(op.IdleTimeout)
		defer op.timer.Stop()
	}

	for {
		select {
		case record, ok := <-in:
			if !ok {
				op.flushRemaining(out)
				return
			}
			if op.timer != nil {
				op.timer.Reset(op.IdleTimeout)
			}
			if record.IsWatermark {
				op.handleWatermark(record, out)
				continue
			}
			if record.IsBarrier {
				out <- record
				continue
			}

			// Drop late records (timestamp below current watermark).
			if !op.currentWatermark.IsZero() && record.Timestamp.Before(op.currentWatermark) {
				continue
			}

			op.handleDataRecord(record)

		case <-op.timerFire():
			op.flushRemaining(out)
			return
		}
	}
}

// timerFire returns a channel that fires when the idle timer expires,
// or nil if no idle timeout is configured (in which case this case is
// never selected).
func (op *WindowOperator) timerFire() <-chan time.Time {
	if op.timer != nil {
		return op.timer.C
	}
	return nil
}

// flushRemaining fires all buffered windows and clears state.
func (op *WindowOperator) flushRemaining(out chan<- types.Record) {
	for key, ws := range op.windows {
		for _, r := range ws.Records {
			out <- tagWithWindow(recordFromJSON(r), ws.Win)
		}
		delete(op.windows, key)
	}
}

// handleDataRecord assigns the record to one or more windows and buffers it.
// For session windows, overlapping sessions for the same key are merged
// (the window bounds expand and records are collected into one entry).
func (op *WindowOperator) handleDataRecord(record types.Record) {
	wins := op.Assigner.AssignWindows(record.Timestamp)
	for _, win := range wins {
		op.assignToWindow(string(record.Key), win, recordToJSON(record))
	}
}

// assignToWindow places a record into the appropriate window, merging
// overlapping session windows for the same key when bounds change.
// For tumbling and sliding windows, records are assigned to pre-aligned
// windows that always have the same key, so no merging is needed.
// Session windows create a unique window per record that may overlap
// with an existing session, in which case we merge by expanding the bounds.
func (op *WindowOperator) assignToWindow(key string, win window.Window, rec recordJSON) {
	wk := toWindowKey(key, win)

	// Fast path: the exact window key exists, just append the record.
	if ws, ok := op.windows[wk]; ok {
		ws.Records = append(ws.Records, rec)
		return
	}

	// For session windows, check if the new window overlaps with an existing
	// session for the same key and merge them.
	if op.Assigner.IsSession() {
		for existingKey, existingWs := range op.windows {
			if existingKey.Key != key {
				continue
			}
			if win.Start.Before(existingWs.Win.End) && win.End.After(existingWs.Win.Start) {
				newStart := existingWs.Win.Start
				newEnd := existingWs.Win.End
				if win.Start.Before(newStart) {
					newStart = win.Start
				}
				if win.End.After(newEnd) {
					newEnd = win.End
				}

				existingWs.Win = window.Window{Start: newStart, End: newEnd}
				existingWs.Records = append(existingWs.Records, rec)

				// Re-key since bounds changed.
				newWk := toWindowKey(key, existingWs.Win)
				if newWk != existingKey {
					op.windows[newWk] = existingWs
					delete(op.windows, existingKey)
				}
				return
			}
		}
	}

	// No existing window: create a new one.
	ws := &windowState{
		Win:     win,
		Records: []recordJSON{rec},
	}
	op.windows[wk] = ws
}

// handleWatermark advances the watermark and fires all windows whose
// end time is <= the new watermark.
func (op *WindowOperator) handleWatermark(watermark types.Record, out chan<- types.Record) {
	if watermark.Timestamp.After(op.currentWatermark) {
		op.currentWatermark = watermark.Timestamp
	}

	for key, ws := range op.windows {
		if !ws.Win.End.After(op.currentWatermark) {
			for _, r := range ws.Records {
				out <- tagWithWindow(recordFromJSON(r), ws.Win)
			}
			delete(op.windows, key)
		}
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

// toWindowKey converts a key and Window to a comparable map key.
func toWindowKey(key string, win window.Window) windowKey {
	return windowKey{
		Key:   key,
		Start: win.Start.UnixNano(),
		End:   win.End.UnixNano(),
	}
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
	ts := time.Unix(0, r.Timestamp).UTC()
	return types.Record{
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: ts,
		Offset:    r.Offset,
		Headers:   r.Headers,
	}
}

// Snapshot returns the operator's current window state as JSON bytes.
func (op *WindowOperator) Snapshot() ([]byte, error) {
	snapshot := windowOperatorSnapshotJSON{
		CurrentWatermark: op.currentWatermark.UnixNano(),
		Windows:          make(map[string]windowState),
	}
	for key, ws := range op.windows {
		snapshot.Windows[key.String()] = *ws
	}
	return json.Marshal(snapshot)
}

// Restore replaces the operator's internal state from JSON bytes produced by Snapshot.
func (op *WindowOperator) Restore(data []byte) error {
	var snapshot windowOperatorSnapshotJSON
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}
	op.currentWatermark = time.Unix(0, snapshot.CurrentWatermark).UTC()
	op.windows = make(map[windowKey]*windowState)
	for keyStr, ws := range snapshot.Windows {
		wk := parseWindowKey(keyStr)
		wsCopy := ws
		op.windows[wk] = &wsCopy
	}
	return nil
}

func (k windowKey) String() string {
	return k.Key + "/" + time.Unix(0, k.Start).UTC().Format(time.RFC3339Nano) + "/" + time.Unix(0, k.End).UTC().Format(time.RFC3339Nano)
}

func parseWindowKey(s string) windowKey {
	// Format: "key/startRFC3339Nano/endRFC3339Nano"
	// Find the last two "/" delimiters
	lastSlash := -1
	secondLastSlash := -1
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
	key := s[:secondLastSlash]
	startStr := s[secondLastSlash+1 : lastSlash]
	endStr := s[lastSlash+1:]
	start, _ := time.Parse(time.RFC3339Nano, startStr)
	end, _ := time.Parse(time.RFC3339Nano, endStr)
	return windowKey{
		Key:   key,
		Start: start.UnixNano(),
		End:   end.UnixNano(),
	}
}
