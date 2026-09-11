package source

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// deliveryCoordinator converts Kafka messages into weibo records, applies the
// configured deserializer and its failure policy, and emits valid records to
// the source output channel.
//
// Per-partition ordering is preserved because each partition's reader loop
// calls toRecord/emit sequentially; emit blocks on a full output channel,
// preserving backpressure rather than buffering.
type deliveryCoordinator struct {
	deserializer    Deserializer
	deserFailPolicy DeserFailurePolicy
	deserDLQ        RecordSink
	metrics         sourceMetrics
}

// toRecord converts a Kafka message into a *types.Record, running the
// deserializer when configured. A nil record with a nil error means the
// configured policy intentionally consumed the failure (drop or successful
// DLQ). A non-nil error is pipeline-fatal.
func (d *deliveryCoordinator) toRecord(ctx context.Context, msg kafka.Message) (*types.Record, error) {
	record := KafkaToRecord(msg)
	if d.deserializer == nil {
		return &record, nil
	}

	parsed, err := d.deserializer.Deserialize(record.Value, record.Headers)
	if err != nil {
		d.metrics.recordDeserFailure()
		switch d.deserFailPolicy {
		case DeserFailureDLQ:
			if d.deserDLQ == nil {
				return nil, fmt.Errorf("weibo/source: deserialization DLQ is nil: %w", err)
			}
			failRecord := record.WithHeader("_deser_error", []byte(err.Error()))
			if werr := d.deserDLQ.Write(ctx, failRecord); werr != nil {
				return nil, fmt.Errorf("weibo/source: deserialization DLQ write failed: %w", werr)
			}
		case DeserFailureFail:
			return nil, fmt.Errorf("weibo/source: deserialize record topic=%q partition=%d offset=%d: %w", msg.Topic, msg.Partition, msg.Offset, err)
		default:
			// DeserFailureDrop
		}
		return nil, nil
	}

	record.Parsed = parsed
	return &record, nil
}

// emit sends a record to the output channel, blocking until there is room
// (backpressure) or the context is cancelled.
func (d *deliveryCoordinator) emit(ctx context.Context, out chan<- types.Record, record types.Record) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- record:
		return nil
	}
}
