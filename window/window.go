// Package window provides window assigners for grouping unbounded streams
// into finite chunks based on time.
//
// A WindowAssigner takes a Record's timestamp and determines which window(s)
// it belongs to. When a watermark passes a window's end time, that window
// closes and its accumulated records are emitted downstream.
package window

import "time"

// Window represents a time range [Start, End) that records are grouped into.
type Window struct {
	Start time.Time
	End   time.Time
}

// WindowAssigner determines which window(s) a record belongs to
// based on its timestamp. Different window types implement this interface.
type WindowAssigner interface {
	// AssignWindows returns one or more windows that the given timestamp falls into.
	// A timestamp can belong to multiple windows in the case of sliding windows.
	AssignWindows(timestamp time.Time) []Window

	// WindowSize returns the duration of a single window.
	WindowSize() time.Duration
}