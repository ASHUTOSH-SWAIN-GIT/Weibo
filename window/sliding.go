package window

import "time"

// Sliding assigns records to overlapping fixed-size windows that slide
// by a fixed interval. A single record can belong to multiple windows.
//
// Example with size=5min, slide=1min, aligned to :00:
//   Window [00:00, 00:05) contains records from 00:00 to 00:04
//   Window [00:01, 00:06) contains records from 00:01 to 00:05
//   Window [00:02, 00:07) contains records from 00:02 to 00:06
//   ...
//
// A record at 00:03 belongs to windows: [00:00-00:05], [00:01-00:06],
// [00:02-00:07], [00:03-00:08], [00:04-00:09].
type Sliding struct {
	size   time.Duration
	slide  time.Duration
	offset time.Duration
}

// NewSliding creates a sliding window assigner with the given window size
// and slide interval. Slide must be <= size.
func NewSliding(size, slide time.Duration) *Sliding {
	return &Sliding{size: size, slide: slide}
}

// WithOffset shifts the window alignment.
func (s *Sliding) WithOffset(offset time.Duration) *Sliding {
	s.offset = offset
	return s
}

// AssignWindows returns all windows that the given timestamp falls into.
// For sliding windows, a record typically belongs to ceil(size/slide) windows.
func (s *Sliding) AssignWindows(timestamp time.Time) []Window {
	// Find the last window that could contain this timestamp,
	// then walk backwards by slide intervals to find all overlapping windows.
	lastStart := windowStart(timestamp, s.slide, s.offset)

	var windows []Window

	// Walk backwards by slide intervals until we go past the record.
	for start := lastStart; start.Add(s.size).After(timestamp); start = start.Add(-s.slide) {
		if !start.After(timestamp) {
			windows = append(windows, Window{Start: start, End: start.Add(s.size)})
		}
	}

	// If the last start equals the first window, we still need windows ahead.
	// Walk forwards to catch windows that start before the timestamp but were
	// not caught because we walked backwards from the aligned window start.
	for start := lastStart.Add(s.slide); start.Before(timestamp.Add(s.size)); start = start.Add(s.slide) {
		if !start.After(timestamp) && start.Add(s.size).After(timestamp) {
			windows = append(windows, Window{Start: start, End: start.Add(s.size)})
		}
	}

	if len(windows) == 0 {
		// Fallback: at minimum the timestamp is in its own aligned slot.
		windows = append(windows, Window{Start: lastStart, End: lastStart.Add(s.size)})
	}

	return windows
}

// WindowSize returns the window duration.
func (s *Sliding) WindowSize() time.Duration {
	return s.size
}