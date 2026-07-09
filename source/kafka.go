package source

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/watermark"
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

	if len(k.restoredOffsets) > 0 {
		for partition, offset := range k.restoredOffsets {
			if err := k.reader.SetOffset(offset); err != nil {
				return fmt.Errorf("kafka restore offset (partition %d): %w", partition, err)
			}
		}
	}

	for {
		msg, err := k.fetchWithRetry(ctx)
		if err != nil {
			return err
		}

		record := KafkaToRecord(msg)

		if k.cfg.deserializer != nil {
			parsed, err := k.cfg.deserializer.Deserialize(record.Value, record.Headers)
			if err != nil {
				metrics.RecordsFailedTotal.Inc()
				switch k.cfg.deserFailPolicy {
				case DeserFailureDLQ:
					if k.cfg.deserDLQ != nil {
						failRecord := record
						failRecord = failRecord.WithHeader("_deser_error", []byte(err.Error()))
						if werr := k.cfg.deserDLQ.Write(ctx, failRecord); werr != nil {
							fmt.Printf("mailer/source: deserialization DLQ write error: %v\n", werr)
						}
					}
				case DeserFailureFail:
					return fmt.Errorf("deserialization failed: %w", err)
				default:
					// DeserFailureDrop — silently skip.
				}
				continue
			}
			record.Parsed = parsed
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- record:
		}

		if k.cfg.commitBatch > 0 {
			k.pending = append(k.pending, msg)
			if len(k.pending) >= k.cfg.commitBatch {
				if err := k.commitBatchWithRetry(ctx); err != nil {
					return err
				}
			}
		} else {
			if err := k.commitWithRetry(ctx, msg); err != nil {
				return err
			}
		}
	}
}

// fetchWithRetry retries FetchMessage up to fetchMaxRetries times
// with exponential backoff. If fetchMaxRetries is -1, retries forever.
// Returns an error on final failure.
func (k *KafkaSource) fetchWithRetry(ctx context.Context) (kafka.Message, error) {
	var lastErr error
	maxAttempts := k.cfg.fetchMaxRetries
	retryForever := maxAttempts < 0

	for attempt := 0; retryForever || attempt <= maxAttempts; attempt++ {
		msg, err := k.reader.FetchMessage(ctx)
		if err == nil {
			return msg, nil
		}
		if ctx.Err() != nil {
			return kafka.Message{}, ctx.Err()
		}
		lastErr = err
		if retryForever || attempt < maxAttempts {
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			select {
			case <-ctx.Done():
				return kafka.Message{}, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return kafka.Message{}, fmt.Errorf("kafka fetch failed after %d retries: %w", k.cfg.fetchMaxRetries, lastErr)
}

// commitWithRetry retries CommitMessages for a single message up to
// commitMaxRetries times. On final failure, the commit policy is applied.
func (k *KafkaSource) commitWithRetry(ctx context.Context, msgs ...kafka.Message) error {
	var lastErr error
	for attempt := 0; attempt <= k.cfg.commitMaxRetries; attempt++ {
		err := k.reader.CommitMessages(ctx, msgs...)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		lastErr = err
		if attempt < k.cfg.commitMaxRetries {
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	if k.cfg.commitFailPolicy == CommitPolicyFail {
		return fmt.Errorf("kafka commit failed after %d retries: %w", k.cfg.commitMaxRetries, lastErr)
	}
	fmt.Printf("mailer/source: kafka commit failed after %d retries (skipping): %v\n", k.cfg.commitMaxRetries, lastErr)
	return nil
}

// commitBatchWithRetry flushes and commits pending messages with retry.
// On final failure, the commit policy is applied (per-record).
func (k *KafkaSource) commitBatchWithRetry(ctx context.Context) error {
	if len(k.pending) == 0 {
		return nil
	}
	msgs := k.pending
	k.pending = k.pending[:0]
	return k.commitWithRetry(ctx, msgs...)
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
	_ Drainable        = (*KafkaSource)(nil)
	_ Describable      = (*KafkaSource)(nil)
	_ Source           = (*kafkaSourceRunner)(nil)
)

// Drain flushes pending offset commits. Called during graceful shutdown
// to commit offsets for records that were read but not yet committed.
func (k *KafkaSource) Drain(ctx context.Context) error {
	if len(k.pending) == 0 {
		return nil
	}
	return k.commitWithRetry(ctx, k.pending...)
}

// Describe returns metadata about this Kafka source for the dashboard.
func (k *KafkaSource) Describe() SourceInfo {
	props := map[string]string{
		"brokers": strings.Join(k.cfg.brokers, ","),
	}
	if len(k.cfg.topics) > 0 {
		props["topics"] = strings.Join(k.cfg.topics, ",")
	}
	if k.cfg.topic != "" {
		props["topic"] = k.cfg.topic
	}
	if k.cfg.groupID != "" {
		props["group_id"] = k.cfg.groupID
	}
	props["offset"] = k.cfg.offsetSpec.Display()
	if k.cfg.watermarkOutOfOrderness > 0 {
		props["watermark_out_of_orderness"] = k.cfg.watermarkOutOfOrderness.String()
	}
	if k.cfg.deserializer != nil {
		props["deserializer"] = fmt.Sprintf("%T", k.cfg.deserializer)
	}
	if k.cfg.commitBatch > 0 {
		props["commit_batch"] = fmt.Sprintf("%d", k.cfg.commitBatch)
	}
	if k.cfg.sasl != nil {
		props["sasl"] = string(k.cfg.sasl.Mechanism)
	}
	if k.cfg.tls != nil {
		props["tls"] = "enabled"
	}

	return SourceInfo{
		Type:  "Kafka",
		Props: props,
	}
}
