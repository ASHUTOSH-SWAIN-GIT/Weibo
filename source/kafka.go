package source

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"

	"mailer/types"
	"mailer/watermark"
)

// KafkaSource reads records from one or more Kafka topics using a consumer group.
// It implements the Source interface for use in mailer pipelines,
// and the CheckpointSource interface for checkpoint/recovery support.
//
// Configure a KafkaSource with functional options via NewKafkaSource:
//
//	src := source.NewKafkaSource(
//	    source.KafkaBrokers("localhost:9092"),
//	    source.KafkaTopic("orders"),
//	    source.KafkaGroupID("order-processor"),
//	    source.KafkaStartFrom(source.OffsetEarliest),
//	    source.KafkaWithWatermarks(1*time.Second),
//	)
//
// Kafka message fields are mapped to mailer.Record as follows:
//   - Key     -> Record.Key
//   - Value   -> Record.Value
//   - Time    -> Record.Timestamp
//   - Offset  -> Record.Offset
//   - Headers -> Record.Headers
//
// If a Deserializer is configured (KafkaDeserialize), Record.Parsed is also set.
type KafkaSource struct {
	cfg    kafkaSourceConfig
	reader *kafka.Reader

	// pending holds messages whose offsets have not yet been committed
	// when batch committing is enabled.
	pending []kafka.Message

	// restoredOffsets holds per-partition offsets to seek to on startup,
	// populated by RestoreOffset from a checkpoint.
	restoredOffsets map[int]int64
}

// NewKafkaSource creates a Source that reads from Kafka.
// Brokers and at least one topic (via KafkaTopic or KafkaTopics) are required;
// if missing, NewKafkaSource panics to fail fast at construction time.
func NewKafkaSource(opts ...KafkaSourceOption) *KafkaSource {
	cfg := kafkaSourceConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if len(cfg.brokers) == 0 {
		panic("mailer/source: KafkaSource requires KafkaBrokers(...)")
	}
	if len(cfg.topics) == 0 && cfg.topic == "" {
		panic("mailer/source: KafkaSource requires KafkaTopic or KafkaTopics")
	}

	rc := kafka.ReaderConfig{
		Brokers:     cfg.brokers,
		GroupID:     cfg.groupID,
		MinBytes:    cfg.minBytes,
		MaxBytes:    cfg.maxBytes,
		StartOffset: cfg.offsetSpec.toKafka(),
		Topic:       cfg.topic,
		GroupTopics: cfg.topics,
	}

	// Phase 2: wire SASL/TLS into the dialer if configured.
	if cfg.sasl != nil || cfg.tls != nil {
		rc.Dialer = buildDialer(cfg.sasl, cfg.tls)
	}

	return &KafkaSource{
		cfg:    cfg,
		reader: kafka.NewReader(rc),
	}
}

// Run fetches messages from Kafka and writes them to the output channel
// until the context is cancelled or the reader returns an error.
//
// If KafkaWithWatermarks was set, Run applies watermarks by delegating
// to a WatermarkSource wrapping this source — callers always get a
// watermark-aware stream when the option is present.
func (k *KafkaSource) Run(ctx context.Context, out chan<- types.Record) error {
	// If watermarking is enabled, delegate to a WatermarkSource that wraps
	// the raw fetch loop (via kafkaSourceRunner) to avoid recursion.
	if k.cfg.watermarkOutOfOrderness > 0 {
		ws := &WatermarkSource{
			Source:    &kafkaSourceRunner{k},
			Generator: watermark.NewBoundedOutOfOrderness(k.cfg.watermarkOutOfOrderness),
			Interval:  k.cfg.watermarkInterval,
		}
		return ws.Run(ctx, out)
	}
	return k.runOnce(ctx, out)
}

// runOnce is the core fetch loop without watermark injection.
// It is also used by WatermarkSource via kafkaSourceRunner.
func (k *KafkaSource) runOnce(ctx context.Context, out chan<- types.Record) error {
	defer k.reader.Close()

	// Apply restored offsets from a checkpoint, if any.
	if len(k.restoredOffsets) > 0 {
		for partition, offset := range k.restoredOffsets {
			if err := k.reader.SetOffset(offset); err != nil {
				return fmt.Errorf("kafka restore offset (partition %d): %w", partition, err)
			}
		}
	}

	for {
		msg, err := k.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("kafka fetch: %w", err)
		}

		record := KafkaToRecord(msg)

		// Run deserializer if configured; on error drop the record.
		if k.cfg.deserializer != nil {
			parsed, err := k.cfg.deserializer.Deserialize(record.Value, record.Headers)
			if err != nil {
				continue
			}
			record.Parsed = parsed
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- record:
		}

		// Commit offsets.
		if k.cfg.commitBatch > 0 {
			k.pending = append(k.pending, msg)
			if len(k.pending) >= k.cfg.commitBatch {
				if err := k.flushCommits(ctx); err != nil {
					return err
				}
			}
		} else {
			if err := k.reader.CommitMessages(ctx, msg); err != nil {
				return fmt.Errorf("kafka commit: %w", err)
			}
		}
	}
}

// flushCommits commits all pending messages and clears the buffer.
func (k *KafkaSource) flushCommits(ctx context.Context) error {
	if len(k.pending) == 0 {
		return nil
	}
	if err := k.reader.CommitMessages(ctx, k.pending...); err != nil {
		return fmt.Errorf("kafka batch commit: %w", err)
	}
	k.pending = k.pending[:0]
	return nil
}

// CheckpointOffset returns the source's current position as JSON bytes.
// For Kafka, this is the last committed offset per partition (from reader stats).
func (k *KafkaSource) CheckpointOffset() ([]byte, error) {
	stats := k.reader.Stats()
	data := kafkaOffsetData{
		Topic:     stats.Topic,
		Partition: stats.Partition,
		Offset:    stats.Offset,
		Lag:       stats.Lag,
	}
	return json.Marshal(data)
}

// RestoreOffset seeks the reader to the position saved by CheckpointOffset.
// This is called during recovery before Run() starts.
func (k *KafkaSource) RestoreOffset(data []byte) error {
	var od kafkaOffsetData
	if err := json.Unmarshal(data, &od); err != nil {
		return fmt.Errorf("kafka restore offset: unmarshal: %w", err)
	}
	// We store the offset to apply at the start of Run(); the reader may not
	// be safe to seek before Run, so defer until then.
	if k.restoredOffsets == nil {
		k.restoredOffsets = make(map[int]int64)
	}
	// Partition in stats is a string like "0"; kafka-go reports it that way.
	var partInt int
	if _, err := fmt.Sscanf(od.Partition, "%d", &partInt); err == nil {
		k.restoredOffsets[partInt] = od.Offset
	}
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
// Headers are copied into a map. Parsed is left nil here; the deserializer
// (if configured) populates it in runOnce after this conversion.
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

// kafkaSourceRunner adapts KafkaSource to the Source interface for use
// inside a WatermarkSource, routing Run to runOnce (no watermark wrapping).
type kafkaSourceRunner struct{ inner *KafkaSource }

func (r *kafkaSourceRunner) Run(ctx context.Context, out chan<- types.Record) error {
	return r.inner.runOnce(ctx, out)
}

// Compile-time checks.
var (
	_ Source           = (*KafkaSource)(nil)
	_ CheckpointSource = (*KafkaSource)(nil)
	_ Source           = (*kafkaSourceRunner)(nil)
)
