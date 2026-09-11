package source

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// KafkaSource reads records from one or more Kafka topics using a consumer group.
// It implements the Source interface for use in weibo pipelines,
// and the CheckpointSource interface for checkpoint/recovery support.
//
// KafkaSource is the top-level orchestrator. The work is split across small
// internal components:
//
//	KafkaSource
//	    ├── partitionDiscovery  (enumerate partitions in parallel mode)
//	    ├── readerSupervisor    (own readers, fan-out, cancellation, shutdown)
//	    ├── offsetTracker       (per-partition progress + checkpoint offsets)
//	    ├── deliveryCoordinator (convert, deserialize, emit)
//	    ├── watermarkTracker    (bounded-out-of-orderness watermarks)
//	    └── sourceMetrics       (metric updates)
//
// Configure a KafkaSource with functional options via NewKafkaSource:
//
//	src := source.NewKafkaSource(
//	    source.KafkaBrokers("localhost:9092"),
//	    source.KafkaTopic("orders"),
//	    source.KafkaGroupID("order-processor"),
//	    source.KafkaStartFrom(source.OffsetEarliest),
//	    source.KafkaParallel(),
//	    source.KafkaWithWatermarks(1*time.Second),
//	)
//
// Kafka message fields are mapped to weibo.Record as follows:
//   - Key     -> Record.Key
//   - Value   -> Record.Value
//   - Time    -> Record.Timestamp
//   - Offset  -> Record.Offset
//   - Headers -> Record.Headers
//
// If a Deserializer is configured (KafkaDeserialize), Record.Parsed is also set.
type KafkaSource struct {
	cfg kafkaSourceConfig

	// readers holds the single reader in consumer-group/serial mode; nil in
	// parallel mode, where partitions owns the dynamic reader set instead.
	readers  *readerSupervisor
	offsets  *offsetTracker
	delivery *deliveryCoordinator
	wm       watermarkTracker

	// partitions reconciles the running per-partition readers with the
	// desired partition set (parallel mode only). initialPartitions is the
	// set discovered at construction, used as the startup reconcile seed.
	partitions        *partitionManager
	initialPartitions []int

	// pending holds messages whose offsets have not yet been committed
	// when batch committing is enabled (single-reader mode only).
	pending []kafka.Message
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
		panic("weibo/source: KafkaSource requires KafkaBrokers(...)")
	}
	if len(cfg.topics) == 0 && cfg.topic == "" {
		panic("weibo/source: KafkaSource requires KafkaTopic or KafkaTopics")
	}

	ks := &KafkaSource{
		cfg:     cfg,
		offsets: newOffsetTracker(),
		delivery: &deliveryCoordinator{
			deserializer:    cfg.deserializer,
			deserFailPolicy: cfg.deserFailPolicy,
			deserDLQ:        cfg.deserDLQ,
		},
		wm: watermarkTracker{
			outOfOrderness: cfg.watermarkOutOfOrderness,
			interval:       cfg.watermarkInterval,
		},
	}

	if cfg.parallel {
		if cfg.groupID != "" {
			panic("weibo/source: KafkaParallel and KafkaGroupID are mutually exclusive")
		}
		if cfg.topic == "" || len(cfg.topics) > 0 {
			panic("weibo/source: KafkaParallel requires a single KafkaTopic (KafkaTopics is not supported)")
		}
		disc := partitionDiscovery{brokers: cfg.brokers, topic: cfg.topic}
		ids, err := disc.discover()
		if err != nil {
			panic(fmt.Sprintf("weibo/source: %v", err))
		}
		ks.initialPartitions = ids
		desired := func(context.Context) ([]int, error) { return disc.discover() }
		ks.partitions = newPartitionManager(cfg, ks.offsets, desired, cfg.watchPartitions)
	} else {
		ks.readers = newSerialReaderSupervisor(cfg)
	}

	return ks
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
	if k.wm.enabled() {
		return k.wm.wrap(&kafkaSourceRunner{k}).Run(ctx, out)
	}
	return k.runOnce(ctx, out)
}

// runOnce is the core fetch loop without watermark injection.
//
// In parallel mode the partitionManager owns the per-partition readers,
// reconciling them with the live partition set and supervising failures.
// Restore-seeking happens inside the manager as each reader is created (a
// no-op for serial mode, whose single reader is not partition-pinned).
func (k *KafkaSource) runOnce(ctx context.Context, out chan<- types.Record) error {
	if k.cfg.parallel {
		return k.partitions.run(ctx, k.initialPartitions, k.readerHandle(out))
	}
	defer k.readers.closeAll()
	if err := k.applyRestoredGroupOffsets(ctx); err != nil {
		return fmt.Errorf("kafka restore offsets: %w", err)
	}
	return k.runSerial(ctx, out)
}

// runSerial is the single-reader path.
func (k *KafkaSource) runSerial(ctx context.Context, out chan<- types.Record) error {
	r := k.readers.primary()
	for {
		msg, err := k.fetchWithRetry(ctx, r)
		if err != nil {
			return err
		}
		k.offsets.track(msg)
		record, err := k.delivery.toRecord(ctx, msg)
		if err != nil {
			return err
		}
		if record == nil {
			continue
		}
		if err := k.delivery.emit(ctx, out, *record); err != nil {
			return err
		}
		if k.cfg.exactlyOnce {
			// No eager commits: the checkpoint coordinator commits
			// offsets after each completed checkpoint (CommitOffsets).
			// Committing here would make offsets durable before the
			// sink's transaction — the data-loss window exactly-once
			// exists to close.
			continue
		}
		if k.cfg.commitBatch > 0 {
			k.pending = append(k.pending, msg)
			if len(k.pending) >= k.cfg.commitBatch {
				if err := k.commitBatchWithRetry(ctx, r); err != nil {
					return err
				}
			}
		} else {
			if err := k.commitWithRetry(ctx, r, msg); err != nil {
				return err
			}
		}
	}
}

// readerHandle returns the per-partition loop body run by the partitionManager
// in parallel mode. It fetches, tracks offsets and emits records for one
// reader until its context is cancelled or a fatal fetch error occurs.
//
// No CommitMessages here: parallel readers have no consumer group (GroupID and
// KafkaParallel are mutually exclusive), so broker-side commits are
// unavailable. Offset durability comes from CheckpointOffset/RestoreOffset via
// SetOffset instead.
func (k *KafkaSource) readerHandle(out chan<- types.Record) partitionLoop {
	return func(ctx context.Context, reader *kafka.Reader) error {
		for {
			msg, err := k.fetchWithRetry(ctx, reader)
			if err != nil {
				return err
			}
			k.offsets.track(msg)
			record, err := k.delivery.toRecord(ctx, msg)
			if err != nil {
				return err
			}
			if record == nil {
				continue
			}
			if err := k.delivery.emit(ctx, out, *record); err != nil {
				return err
			}
		}
	}
}

// fetchWithRetry retries FetchMessage up to fetchMaxRetries times
// with exponential backoff. If fetchMaxRetries is -1, retries forever.
func (k *KafkaSource) fetchWithRetry(ctx context.Context, r *kafka.Reader) (kafka.Message, error) {
	var lastErr error
	maxAttempts := k.cfg.fetchMaxRetries
	retryForever := maxAttempts < 0

	for attempt := 0; retryForever || attempt <= maxAttempts; attempt++ {
		msg, err := r.FetchMessage(ctx)
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
func (k *KafkaSource) commitWithRetry(ctx context.Context, r *kafka.Reader, msgs ...kafka.Message) error {
	var lastErr error
	for attempt := 0; attempt <= k.cfg.commitMaxRetries; attempt++ {
		err := r.CommitMessages(ctx, msgs...)
		if err == nil {
			k.offsets.markCommitted(msgs...)
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
	fmt.Printf("weibo/source: kafka commit failed after %d retries (skipping): %v\n", k.cfg.commitMaxRetries, lastErr)
	return nil
}

// commitBatchWithRetry flushes and commits pending messages with retry.
func (k *KafkaSource) commitBatchWithRetry(ctx context.Context, r *kafka.Reader) error {
	if len(k.pending) == 0 {
		return nil
	}
	msgs := k.pending
	k.pending = k.pending[:0]
	return k.commitWithRetry(ctx, r, msgs...)
}

// CheckpointOffset returns the source's current topic+partition positions in a
// versioned JSON envelope.
//
// This is the fallback offset source. The engine prefers barrier-aligned
// offsets captured at barrier injection (which reflect exactly the records
// processed before the barrier); CheckpointOffset reports the reader's fetch
// position, which may run ahead of the barrier. Either way it must cover
// every partition — the previous reader.Stats() implementation reported only
// one partition per reader, so a multi-partition consumer group lost all but
// one partition's offset from the checkpoint.
func (k *KafkaSource) CheckpointOffset() ([]byte, error) {
	return k.offsets.snapshot()
}

// CheckpointPosition preserves the Kafka topic during barrier-aligned tracking.
func (k *KafkaSource) CheckpointPosition(record types.Record) PositionKey {
	return PositionKey{Source: record.Source, Partition: record.Partition}
}

// OperationalState returns live per-topic/partition progress and lag.
func (k *KafkaSource) OperationalState() any { return k.offsets.operationalState() }

// RestoreOffset restores per-partition offsets from a checkpoint.
func (k *KafkaSource) RestoreOffset(data []byte) error {
	return k.offsets.restore(data, k.legacyCheckpointTopic())
}

func (k *KafkaSource) legacyCheckpointTopic() string {
	if k.cfg.topic != "" {
		return k.cfg.topic
	}
	if len(k.cfg.topics) == 1 {
		return k.cfg.topics[0]
	}
	return ""
}

func (k *KafkaSource) configuredTopics() []string {
	if k.cfg.topic != "" {
		return []string{k.cfg.topic}
	}
	return append([]string(nil), k.cfg.topics...)
}

// applyRestoredGroupOffsets makes checkpoint state authoritative over broker
// commits before the Reader joins for normal consumption. kafka-go's Reader
// cannot SetOffset in group mode, so a short-lived group generation resets the
// complete topic+partition map at the coordinator.
func (k *KafkaSource) applyRestoredGroupOffsets(ctx context.Context) error {
	positions := k.offsets.restoredPositions()
	if len(positions) == 0 {
		return nil
	}
	if k.cfg.groupID == "" {
		if len(positions) != 1 {
			return fmt.Errorf("non-group serial source cannot restore %d partitions", len(positions))
		}
		return k.readers.primary().SetOffset(positions[0].Offset)
	}

	cfg := kafka.ConsumerGroupConfig{
		ID:          k.cfg.groupID,
		Brokers:     k.cfg.brokers,
		Topics:      k.configuredTopics(),
		StartOffset: k.cfg.offsetSpec.toKafka(),
	}
	if k.cfg.sasl != nil || k.cfg.tls != nil {
		cfg.Dialer = buildDialer(k.cfg.sasl, k.cfg.tls)
	}
	group, err := kafka.NewConsumerGroup(cfg)
	if err != nil {
		return err
	}
	defer group.Close()
	generation, err := group.Next(ctx)
	if err != nil {
		return err
	}
	offsets := make(map[string]map[int]int64)
	for _, position := range positions {
		if offsets[position.Source] == nil {
			offsets[position.Source] = make(map[int]int64)
		}
		offsets[position.Source][position.Partition] = position.Offset
	}
	return generation.CommitOffsets(offsets)
}

// KafkaToRecord converts a kafka.Message to a weibo.Record.
// Headers are copied into a map. Parsed is left nil here; the deserializer
// (if configured) populates it in the delivery coordinator after this
// conversion.
func KafkaToRecord(msg kafka.Message) types.Record {
	// kafka-go reuses a message's Value/Key/Header byte buffers for
	// subsequent messages, so the record must own copies — otherwise a
	// record still in flight (buffered in a stage channel) is silently
	// overwritten, which turns downstream field reads into empty values.
	headers := make(map[string][]byte, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = bytes.Clone(h.Value)
	}

	return types.Record{
		Key:       bytes.Clone(msg.Key),
		Value:     bytes.Clone(msg.Value),
		Timestamp: msg.Time,
		Offset:    msg.Offset,
		Partition: msg.Partition,
		Source:    msg.Topic,
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
	_ Source                     = (*KafkaSource)(nil)
	_ CheckpointSource           = (*KafkaSource)(nil)
	_ PositionedCheckpointSource = (*KafkaSource)(nil)
	_ OffsetCommitter            = (*KafkaSource)(nil)
	_ Drainable                  = (*KafkaSource)(nil)
	_ Describable                = (*KafkaSource)(nil)
	_ Source                     = (*kafkaSourceRunner)(nil)
)

// Drain flushes pending offset commits. Called during graceful shutdown
// to commit offsets for records that were read but not yet committed.
func (k *KafkaSource) Drain(ctx context.Context) error {
	if k.cfg.parallel || k.cfg.exactlyOnce {
		return nil
	}
	if len(k.pending) == 0 {
		return nil
	}
	return k.commitWithRetry(ctx, k.readers.primary(), k.pending...)
}

// CommitOffsets implements source.OffsetCommitter: commits the given
// barrier-aligned offsets to the broker after a coordinated checkpoint
// completes. Advisory (consumer-lag visibility) — recovery reads the
// checkpoint file, never the broker. Only possible in consumer-group
// mode; parallel per-partition readers have no group to commit to.
func (k *KafkaSource) CommitOffsets(ctx context.Context, data []byte) error {
	if k.cfg.groupID == "" {
		return nil
	}
	positions, err := DecodePositions(data, k.legacyCheckpointTopic())
	if err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	msgs := make([]kafka.Message, 0, len(positions))
	for _, position := range positions {
		// CommitMessages commits msg.Offset+1 (the next offset to
		// read), matching Position.Offset.
		msgs = append(msgs, kafka.Message{
			Topic:     position.Source,
			Partition: position.Partition,
			Offset:    position.Offset - 1,
		})
	}
	if len(msgs) == 0 {
		return nil
	}
	return k.commitWithRetry(ctx, k.readers.primary(), msgs...)
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
