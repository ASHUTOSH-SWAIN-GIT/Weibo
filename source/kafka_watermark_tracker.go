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
}

// enabled reports whether watermark injection is configured.
func (w watermarkTracker) enabled() bool {
	return w.outOfOrderness > 0
}

// wrap returns a WatermarkSource that layers bounded-out-of-orderness
// watermarks over inner. Only call when enabled() is true.
func (w watermarkTracker) wrap(inner Source) *WatermarkSource {
	return &WatermarkSource{
		Source:    inner,
		Generator: watermark.NewBoundedOutOfOrderness(w.outOfOrderness),
		Interval:  w.interval,
	}
}
