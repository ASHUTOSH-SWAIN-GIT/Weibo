package source

import (
	"context"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/watermark"
)

// WatermarkSource wraps a Source and injects watermark records into the stream
// based on a WatermarkGenerator. The source tracks the maximum event timestamp
// seen and periodically emits watermarks to drive window completion.
type WatermarkSource struct {
	Source    Source
	Generator watermark.WatermarkGenerator
	Interval  time.Duration

	// NewGenerator, when set, switches to per-partition watermarks: a separate
	// generator is created for every (Record.Source, Record.Partition) seen,
	// and the emitted watermark is the MINIMUM across partitions.
	//
	// With the single Generator above, records from every partition feed one
	// clock, so a partition that runs ahead (fast reader, catch-up after a
	// restart) advances the watermark past records of slower partitions. The
	// window operator then drops those records as late — silently, since a
	// late record with no LateSink produces no error, log or metric. Taking
	// the minimum makes the watermark wait for the slowest partition, which is
	// the only bound that is safe when partitions progress at different rates.
	NewGenerator func() watermark.WatermarkGenerator

	// PartitionIdleTimeout stops a partition that has delivered no record for
	// this long (wall clock) from holding the watermark back, so an empty or
	// dead partition cannot stall every window. Only used with NewGenerator.
	// Zero means the default (30s); negative disables idleness.
	PartitionIdleTimeout time.Duration

	// now is the clock for idleness; nil means time.Now. Tests override it.
	now func() time.Time
}

// defaultPartitionIdleTimeout is used when PartitionIdleTimeout is zero.
const defaultPartitionIdleTimeout = 30 * time.Second

type partitionKey struct {
	source    string
	partition int
}

type partitionClock struct {
	gen      watermark.WatermarkGenerator
	lastSeen time.Time
}

func (ws *WatermarkSource) clock() time.Time {
	if ws.now != nil {
		return ws.now()
	}
	return time.Now()
}

// partitionWatermark returns the minimum watermark over partitions that are not
// idle, or the zero time when there is none (nothing seen yet, or all idle).
func (ws *WatermarkSource) partitionWatermark(parts map[partitionKey]*partitionClock) time.Time {
	idle := ws.PartitionIdleTimeout
	if idle == 0 {
		idle = defaultPartitionIdleTimeout
	}
	now := ws.clock()
	var min time.Time
	for _, p := range parts {
		wm := p.gen.GetWatermark()
		if wm.IsZero() {
			continue
		}
		if idle > 0 && now.Sub(p.lastSeen) >= idle {
			continue
		}
		if min.IsZero() || wm.Before(min) {
			min = wm
		}
	}
	return min
}

// NewWatermarkSource wraps a Source with watermark generation.
// Every interval, it checks if the generator has a new watermark and injects it.
func NewWatermarkSource(src Source, gen watermark.WatermarkGenerator, interval time.Duration) *WatermarkSource {
	return &WatermarkSource{
		Source:    src,
		Generator: gen,
		Interval:  interval,
	}
}

// Unwrap exposes the wrapped source so optional-capability lookups (As, and
// anything built on it — dashboard identity/position, checkpointing, drain,
// offset commit) see through this wrapper instead of silently losing
// whatever the wrapped source implements. Without this, every watermarked
// pipeline (the standard way to drive event-time windows) would show
// "Unknown" in the dashboard and — far more seriously — silently lose a
// wrapped Kafka source's exactly-once checkpointing.
func (ws *WatermarkSource) Unwrap() Source { return ws.Source }

// Run starts the underlying source, intercepts every record to update
// the watermark generator, and periodically injects watermark records.
// The channel owner is responsible for closing the output channel.
//
// On context cancellation or source completion, a max watermark is emitted
// to flush all remaining windows. This guarantees correct end-of-stream
// behavior for both batch and streaming sources.
func (ws *WatermarkSource) Run(ctx context.Context, out chan<- types.Record) error {
	records := make(chan types.Record, 256)
	sourceErr := make(chan error, 1)

	// Start the underlying source in a goroutine.
	// When it finishes, close the records channel so we know to drain and exit.
	go func() {
		err := ws.Source.Run(ctx, records)
		close(records)
		sourceErr <- err
	}()

	// Periodic watermark injection.
	ticker := time.NewTicker(ws.Interval)
	defer ticker.Stop()

	// maxWatermark signals "no more data" so windows can fire.
	maxWatermark := types.NewWatermark(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))

	// emitMaxWatermark sends the max watermark using a fresh context with a
	// short timeout, so it succeeds even when the original ctx is cancelled.
	emitMaxWatermark := func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		select {
		case out <- maxWatermark:
		case <-flushCtx.Done():
		}
	}

	// Per-partition state (only used when NewGenerator is set). lastWM keeps
	// the emitted watermark monotonic: a partition that appears late with old
	// timestamps must not pull it backwards.
	parts := make(map[partitionKey]*partitionClock)
	var lastWM time.Time

	for {
		select {
		case <-ctx.Done():
			emitMaxWatermark()
			return ctx.Err()

		case record, ok := <-records:
			if !ok {
				// Source finished naturally.
				emitMaxWatermark()
				return <-sourceErr
			}

			// Update the watermark generator with the record's timestamp.
			if ws.NewGenerator != nil {
				k := partitionKey{source: record.Source, partition: record.Partition}
				p := parts[k]
				if p == nil {
					p = &partitionClock{gen: ws.NewGenerator()}
					parts[k] = p
				}
				p.gen.OnRecord(record.Timestamp)
				p.lastSeen = ws.clock()
			} else {
				ws.Generator.OnRecord(record.Timestamp)
			}
			// Forward with a ctx guard so a cancelled pipeline whose
			// downstream is blocked doesn't leak this goroutine.
			select {
			case out <- record:
			case <-ctx.Done():
				emitMaxWatermark()
				return ctx.Err()
			}

		case <-ticker.C:
			var wm time.Time
			if ws.NewGenerator != nil {
				if pm := ws.partitionWatermark(parts); pm.After(lastWM) {
					lastWM = pm
				}
				wm = lastWM
			} else {
				wm = ws.Generator.GetWatermark()
			}
			if !wm.IsZero() {
				select {
				case out <- types.NewWatermark(wm):
				case <-ctx.Done():
				}
			}
		}
	}
}
