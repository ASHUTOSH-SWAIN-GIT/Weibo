package window

import "time"

// Session assigns records to variable-size windows that close after a gap
// of inactivity (the gap duration). Records that arrive within the gap
// of the previous record belong to the same session.
//
// Session windows are dynamic — they expand when new records arrive within
// the gap, and merge when two sessions grow close enough to overlap.
//
// Example with gap=30s:
//   Record at 00:00 → session window [00:00, 00:30)
//   Record at 00:15 → session window expands to [00:00, 00:45)
//   Record at 01:30 → new session window [01:30, 02:00)
type Session struct {
	gap time.Duration
}

// NewSession creates a session window assigner with the given inactivity gap.
// A session window stays open as long as records arrive within the gap.
// When no record arrives for gap duration, the session closes.
func NewSession(gap time.Duration) *Session {
	return &Session{gap: gap}
}

// AssignWindows returns a single session window starting at the record's
// timestamp and ending at timestamp + gap. The WindowOperator merges
// overlapping sessions as new records arrive.
func (s *Session) AssignWindows(timestamp time.Time) []Window {
	return []Window{
		{Start: timestamp, End: timestamp.Add(s.gap)},
	}
}

// WindowSize returns the session gap duration.
// Note: actual window size varies per session; this returns the minimum (the gap).
func (s *Session) WindowSize() time.Duration {
	return s.gap
}

// MergeSessions merges overlapping or adjacent session windows.
// Two sessions overlap if sessionA.End > sessionB.Start (or vice versa).
// Returns the merged window if they overlap, or both windows if they don't.
func MergeSessions(a, b Window) []Window {
	if a.End.After(b.Start) && a.Start.Before(b.End) {
		start := a.Start
		if b.Start.Before(start) {
			start = b.Start
		}
		end := a.End
		if b.End.After(end) {
			end = b.End
		}
		return []Window{{Start: start, End: end}}
	}
	return []Window{a, b}
}