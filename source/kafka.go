package source

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"

	"mailer/types"
)

// KafkaSource reads records from one or more Kafka topics using a consumer group.
// It implements the Source interface for use in mailer pipelines,
// and the CheckpointSource interface for checkpoint/recovery support.
//
// Records are read continuously until the context is cancelled.
// The channel owner (StreamExecutionEnv) is responsible for closing the output channel.
//
// Kafka message fields are mapped to mailer.Record as follows:
//   - Key     → Record.Key
//   - Value   → Record.Value
//   - Time    → Record.Timestamp
//   - Offset  → Record.Offset
//   - Headers → Record.Headers
type KafkaSource struct {
	Reader *kafka.Reader
}

// KafkaConfig configures a Kafka consumer.
type KafkaConfig struct {
	Brokers     []string // e.g. []string{"localhost:9092"}
	Topic       string   // single-topic mode
	Topics      []string // multi-topic mode (takes precedence over Topic)
	GroupID     string   // consumer group; empty for unconsumed (no offset commits)
	MinBytes    int      // minimum bytes to fetch (default 1)
	MaxBytes    int      // maximum bytes to fetch (default 10MB)
	StartOffset int64    // kafka.FirstOffset or kafka.LastOffset (default FirstOffset)
}

// NewKafkaSource creates a Source that reads from Kafka using a consumer group.
func NewKafkaSource(cfg KafkaConfig) *KafkaSource {
	if cfg.MinBytes == 0 {
		cfg.MinBytes = 1
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 10 * 1024 * 1024
	}
	if cfg.StartOffset == 0 {
		cfg.StartOffset = kafka.FirstOffset
	}

	if len(cfg.Topics) == 0 && cfg.Topic == "" {
		panic("mailer/source: KafkaConfig requires Topic or Topics")
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     cfg.GroupID,
		MinBytes:    cfg.MinBytes,
		MaxBytes:    cfg.MaxBytes,
		StartOffset: cfg.StartOffset,
		Topic:       cfg.Topic,
		GroupTopics: cfg.Topics,
	})

	return &KafkaSource{Reader: r}
}

// Run fetches messages from Kafka and writes them to the output channel
// until the context is cancelled or the reader returns an error.
func (k *KafkaSource) Run(ctx context.Context, out chan<- types.Record) error {
	defer k.Reader.Close()

	for {
		msg, err := k.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("kafka fetch: %w", err)
		}

		record := KafkaToRecord(msg)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- record:
		}

		if err := k.Reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("kafka commit: %w", err)
		}
	}
}

// CheckpointOffset returns the source's current position as JSON bytes.
// For Kafka, this is the last committed offset per partition.
func (k *KafkaSource) CheckpointOffset() ([]byte, error) {
	stats := k.Reader.Stats()
	data := kafkaOffsetData{
		Topic:     stats.Topic,
		Partition: stats.Partition,
		Offset:    stats.Offset,
		Lag:       stats.Lag,
	}
	return json.Marshal(data)
}

// RestoreOffset is a no-op for Kafka since consumer group offsets are
// managed by Kafka itself. The GroupID handles offset tracking.
func (k *KafkaSource) RestoreOffset(data []byte) error {
	// Kafka consumer groups manage offsets via the broker.
	// On recovery, the consumer group resumes from the last committed offset.
	// We store the offset data in the checkpoint for informational purposes,
	// but don't need to explicitly seek.
	return nil
}

// kafkaOffsetData holds Kafka source position for checkpointing.
type kafkaOffsetData struct {
	Topic     string `json:"topic"`
	Partition string `json:"partition"`
	Offset    int64  `json:"offset"`
	Lag       int64  `json:"lag"`
}

// KafkaToRecord converts a kafka.Message to a mailer.Record.
func KafkaToRecord(msg kafka.Message) types.Record {
	headers := make(map[string][]byte, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = h.Value
	}

	return types.Record{
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: msg.Time,
		Offset:    msg.Offset,
		Headers:   headers,
	}
}

// Compile-time check: KafkaSource implements CheckpointSource.
var _ CheckpointSource = (*KafkaSource)(nil)
