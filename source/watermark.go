package source

import (
	"context"
	"time"

	"mailer/types"
	"mailer/watermark"
)

// WatermarkSource wraps a Source and injects watermark records into the stream
// based on a WatermarkGenerator. The source tracks the maximum event timestamp
// seen and periodically emits watermarks to drive window completion.
type WatermarkSource struct {
	Source    Source
	Generator watermark.WatermarkGenerator
	Interval  time.Duration
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

// Run starts the underlying source, intercepts every record to update
// the watermark generator, and periodically injects watermark records.
// The channel owner is responsible for closing the output channel.
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case record, ok := <-records:
			if !ok {
				// Source finished. Emit a max watermark to flush all remaining windows.
				// This signals that no more records will arrive, so every window can close.
				maxWatermark := types.NewWatermark(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
				select {
				case out <- maxWatermark:
				default:
				}
				return <-sourceErr
			}

			// Update watermark generator with the record's timestamp.
			ws.Generator.OnRecord(record.Timestamp)
			out <- record

		case <-ticker.C:
			wm := ws.Generator.GetWatermark()
			if !wm.IsZero() {
				select {
				case out <- types.NewWatermark(wm):
				default:
				}
			}
		}
	}
}