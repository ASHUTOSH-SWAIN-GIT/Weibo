package operator

import (
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
//
// Must be used after KeyBy in a keyed stream so each key gets its
// own set of windows.
type WindowOperator struct {
	Assigner         window.WindowAssigner
	currentWatermark time.Time
	windows          map[windowKey]*windowState
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
	win     window.Window
	records []types.Record
}

// Window creates a WindowOperator with the given window assigner.
// Supported assigners: window.Tumbling, window.Sliding, window.Session.
func Window(assigner window.WindowAssigner) *WindowOperator {
	return &WindowOperator{
		Assigner: assigner,
		windows: make(map[windowKey]*windowState),
	}
}

// Process reads records and watermarks, buffers data records into windows,
// and emits results when watermarks indicate windows are complete.
func (op *WindowOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)

	for record := range in {
		if record.IsWatermark {
			op.handleWatermark(record, out)
			continue
		}

		// Drop late records (timestamp below current watermark).
		if !op.currentWatermark.IsZero() && record.Timestamp.Before(op.currentWatermark) {
			continue
		}

		op.handleDataRecord(record)
	}

	// When input closes, fire any remaining windows.
	for key, ws := range op.windows {
		for _, r := range ws.records {
			out <- tagWithWindow(r, ws.win)
		}
		delete(op.windows, key)
	}
}

// handleDataRecord assigns the record to one or more windows and buffers it.
func (op *WindowOperator) handleDataRecord(record types.Record) {
	wins := op.Assigner.AssignWindows(record.Timestamp)
	for _, win := range wins {
		key := toWindowKey(string(record.Key), win)
		ws, ok := op.windows[key]
		if !ok {
			ws = &windowState{
				win:     win,
				records: make([]types.Record, 0),
			}
			op.windows[key] = ws
		}
		ws.records = append(ws.records, record)
	}
}

// handleWatermark advances the watermark and fires all windows whose
// end time is <= the new watermark.
func (op *WindowOperator) handleWatermark(watermark types.Record, out chan<- types.Record) {
	if watermark.Timestamp.After(op.currentWatermark) {
		op.currentWatermark = watermark.Timestamp
	}

	for key, ws := range op.windows {
		if !ws.win.End.After(op.currentWatermark) {
			for _, r := range ws.records {
				out <- tagWithWindow(r, ws.win)
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