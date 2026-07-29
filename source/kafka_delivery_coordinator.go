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
// deserializer when configured. It returns nil when the record should be
// dropped (deserialization failed under any policy).
func (d *deliveryCoordinator) toRecord(msg kafka.Message) *types.Record {
	record := KafkaToRecord(msg)
	if d.deserializer == nil {
		return &record
	}

	parsed, err := d.deserializer.Deserialize(record.Value, record.Headers)
	if err != nil {
		d.metrics.recordDeserFailure()
		switch d.deserFailPolicy {
		case DeserFailureDLQ:
			if d.deserDLQ != nil {
				failRecord := record.WithHeader("_deser_error", []byte(err.Error()))
				if werr := d.deserDLQ.Write(context.Background(), failRecord); werr != nil {
					fmt.Printf("weibo/source: deser DLQ write error: %v\n", werr)
				}
			}
		case DeserFailureFail:
			return nil // handled by caller via error
		default:
			// DeserFailureDrop
		}
		return nil
	}

	record.Parsed = parsed
	return &record
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
