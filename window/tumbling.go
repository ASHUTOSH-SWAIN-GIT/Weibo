package window

import "time"

// Tumbling assigns records to fixed-size, non-overlapping windows.
// Each record belongs to exactly one window.
//
// Example with size=5min, aligned to :00:
//
//	[00:00, 00:05), [00:05, 00:10), [00:10, 00:15), ...
//
// A record with timestamp 00:07 goes into window [00:05, 00:10).
type Tumbling struct {
	size   time.Duration
	offset time.Duration
}

// NewTumbling creates a tumbling window assigner with the given window size.
// Windows are aligned to Unix epoch (offset=0) by default.
func NewTumbling(size time.Duration) *Tumbling {
	return &Tumbling{size: size}
}

// WithOffset shifts the window alignment. For example, WithOffset(1*time.Minute)
// on a 5-minute tumbling window starts windows at :01, :06, :11, etc.
func (t *Tumbling) WithOffset(offset time.Duration) *Tumbling {
	t.offset = offset
	return t
}

// AssignWindows returns exactly one window for the given timestamp.
func (t *Tumbling) AssignWindows(timestamp time.Time) []Window {
	start := windowStart(timestamp, t.size, t.offset)
	return []Window{{Start: start, End: start.Add(t.size)}}
}

// WindowSize returns the duration of each tumbling window.
func (t *Tumbling) WindowSize() time.Duration {
	return t.size
}

// windowStart calculates the start of the window that contains the given
// timestamp, accounting for offset alignment.
func windowStart(ts time.Time, size, offset time.Duration) time.Time {
	// Convert to Unix nanoseconds for arithmetic.
	tsUnix := ts.UnixNano()
	offsetUnix := offset.Nanoseconds()
	sizeUnix := size.Nanoseconds()

	// Align to offset, then floor to window boundary.
	aligned := tsUnix - offsetUnix
	start := (aligned / sizeUnix) * sizeUnix
	start += offsetUnix

	return time.Unix(0, start).UTC()
}
