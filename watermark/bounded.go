package watermark

import "time"

// BoundedOutOfOrderness is the most common watermark generator strategy.
// It tracks the maximum timestamp seen and subtracts a fixed lateness bound.
// The resulting watermark is: max_timestamp - allowed_lateness.
//
// This handles out-of-order events: if allowed_lateness is 5 seconds,
// any record arriving up to 5 seconds late (relative to the max timestamp)
// will still be placed in its correct window.
//
// Example:
//
//	Records arrive: ts=10, ts=8, ts=15, ts=12
//	Max timestamp seen: 15
//	Allowed lateness: 5s
//	Current watermark: 15 - 5 = 10
//	→ Windows with end time <= 10 can now close
type BoundedOutOfOrderness struct {
	maxTimestamp    time.Time
	allowedLateness time.Duration
}

// NewBoundedOutOfOrderness creates a watermark generator with the given
// allowed lateness. Events up to this duration late will still be
// placed in their correct window.
func NewBoundedOutOfOrderness(allowedLateness time.Duration) *BoundedOutOfOrderness {
	return &BoundedOutOfOrderness{
		allowedLateness: allowedLateness,
	}
}

// OnRecord updates the maximum timestamp seen.
func (g *BoundedOutOfOrderness) OnRecord(timestamp time.Time) {
	if timestamp.After(g.maxTimestamp) {
		g.maxTimestamp = timestamp
	}
}

// GetWatermark returns the current watermark: max_timestamp - allowed_lateness.
// If no records have been seen, returns the zero time.
func (g *BoundedOutOfOrderness) GetWatermark() time.Time {
	if g.maxTimestamp.IsZero() {
		return time.Time{}
	}
	return g.maxTimestamp.Add(-g.allowedLateness)
}

// CurrentWatermark returns the current watermark timestamp.
// This is the same as GetWatermark — it's here to satisfy the interface.
func (g *BoundedOutOfOrderness) CurrentWatermark() time.Time {
	return g.GetWatermark()
}