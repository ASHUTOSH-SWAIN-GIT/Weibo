package source

import "github.com/segmentio/kafka-go"

// Offset is a mailer-agnostic Kafka offset position.
// It replaces direct use of kafka-go's offset constants so users
// never need to import segmentio/kafka-go.
type Offset int64

const (
	// OffsetEarliest starts reading from the earliest available message
	// in each partition (maps to kafka.FirstOffset).
	OffsetEarliest Offset = -2

	// OffsetLatest starts reading from the most recently produced message
	// in each partition (maps to kafka.LastOffset).
	OffsetLatest Offset = -1
)

// OffsetSpec describes where a Kafka source should start consuming.
// It is set via the KafkaStartFrom option.
type OffsetSpec struct {
	Mode     Offset
	Explicit map[int]int64 // partition -> offset (only used when Mode == offsetExplicit)
}

const offsetExplicit Offset = -3

// toKafka maps a mailer OffsetSpec to the kafka-go StartOffset value,
// or returns a concrete offset for explicit seeks.
func (s OffsetSpec) toKafka() int64 {
	switch s.Mode {
	case OffsetEarliest:
		return kafka.FirstOffset
	case OffsetLatest:
		return kafka.LastOffset
	default:
		return int64(s.Mode)
	}
}
