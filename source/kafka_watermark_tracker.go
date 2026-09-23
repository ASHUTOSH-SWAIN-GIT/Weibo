package source

import (
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/watermark"
)

// watermarkTracker holds the KafkaSource's watermark configuration and knows
// how to wrap the raw fetch loop with watermark generation.
//
// Semantics are unchanged: a bounded-out-of-orderness generator observes
// record event timestamps and emits watermarks every interval. This type only
// centralises the configuration and wrapping — window/watermark behaviour is
// still owned by WatermarkSource and the watermark package.
type watermarkTracker struct {
	outOfOrderness time.Duration
	interval       time.Duration
	// idleTimeout stops a quiet partition from holding the watermark back:
	// zero is the default (30s), negative disables idleness.
	idleTimeout time.Duration
}

// enabled reports whether watermark injection is configured.
func (w watermarkTracker) enabled() bool {
	return w.outOfOrderness > 0
}

// wrap returns a WatermarkSource that layers bounded-out-of-orderness
// watermarks over inner. Only call when enabled() is true.
//
// The watermark is tracked per (topic, partition) and is the minimum across
// them. One generator over all partitions would follow the fastest partition
// and make the window operator drop records from slower ones as late.
func (w watermarkTracker) wrap(inner Source) *WatermarkSource {
	outOfOrderness := w.outOfOrderness
	return &WatermarkSource{
		Source:               inner,
		Generator:            watermark.NewBoundedOutOfOrderness(outOfOrderness),
		NewGenerator:         func() watermark.WatermarkGenerator { return watermark.NewBoundedOutOfOrderness(outOfOrderness) },
		PartitionIdleTimeout: w.idleTimeout,
		Interval:             w.interval,
	}
}
