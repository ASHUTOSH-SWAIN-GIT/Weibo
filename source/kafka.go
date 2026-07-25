package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/watermark"
)

// KafkaSource reads records from one or more Kafka topics using a consumer group.
// It implements the Source interface for use in weibo pipelines,
// and the CheckpointSource interface for checkpoint/recovery support.
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
	cfg          kafkaSourceConfig
	readers      []*kafka.Reader
	partitionIDs []int

	// pending holds messages whose offsets have not yet been committed
	// when batch committing is enabled (single-reader mode only).
	pending []kafka.Message

	// restoredOffsets holds per-partition offsets to seek to on startup,
	// populated by RestoreOffset from a checkpoint.
	restoredOffsets map[int]int64

	// offsetMu guards consumedOffsets.
	offsetMu sync.Mutex
	// consumedOffsets tracks the next offset to read for every partition,
	// updated as each message is consumed. It is the source of truth for
	// CheckpointOffset: reader.Stats() surfaces only a single partition in
	// consumer-group mode, so a Stats-based checkpoint silently dropped
	// every other partition's progress — reprocessing (or, with a broker
	// commit racing ahead, losing) their records on restart.
	consumedOffsets map[int]int64
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

	ks := &KafkaSource{cfg: cfg}

	if cfg.parallel {
		if cfg.groupID != "" {
			panic("weibo/source: KafkaParallel and KafkaGroupID are mutually exclusive")
		}
		if cfg.topic == "" || len(cfg.topics) > 0 {
			panic("weibo/source: KafkaParallel requires a single KafkaTopic (KafkaTopics is not supported)")
		}
		ks.initParallelReaders()
	} else {
		rc := kafka.ReaderConfig{
			Brokers:     cfg.brokers,
			GroupID:     cfg.groupID,
			MinBytes:    cfg.minBytes,
			MaxBytes:    cfg.maxBytes,
			StartOffset: cfg.offsetSpec.toKafka(),
			Topic:       cfg.topic,
			GroupTopics: cfg.topics,
		}
		if cfg.exactlyOnce {
			rc.IsolationLevel = kafka.ReadCommitted
		}
		if cfg.sasl != nil || cfg.tls != nil {
			rc.Dialer = buildDialer(cfg.sasl, cfg.tls)
		}
		ks.readers = []*kafka.Reader{kafka.NewReader(rc)}
		ks.partitionIDs = []int{-1}
	}

	return ks
}

// initParallelReaders discovers partitions and creates one reader per partition.
func (k *KafkaSource) initParallelReaders() {
	conn, err := kafka.Dial("tcp", k.cfg.brokers[0])
	if err != nil {
		panic(fmt.Sprintf("weibo/source: cannot dial broker for partitions: %v", err))
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(k.cfg.topic)
	if err != nil {
		panic(fmt.Sprintf("weibo/source: cannot read partitions for %s: %v", k.cfg.topic, err))
	}

	for _, p := range partitions {
		rc := kafka.ReaderConfig{
			Brokers:     k.cfg.brokers,
			Topic:       k.cfg.topic,
			Partition:   p.ID,
			MinBytes:    k.cfg.minBytes,
			MaxBytes:    k.cfg.maxBytes,
			StartOffset: k.cfg.offsetSpec.toKafka(),
		}
		if k.cfg.exactlyOnce {
			rc.IsolationLevel = kafka.ReadCommitted
		}
		if k.cfg.sasl != nil || k.cfg.tls != nil {
			rc.Dialer = buildDialer(k.cfg.sasl, k.cfg.tls)
		}
		k.readers = append(k.readers, kafka.NewReader(rc))
		k.partitionIDs = append(k.partitionIDs, p.ID)
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
// In parallel mode, each partition gets its own goroutine.
func (k *KafkaSource) runOnce(ctx context.Context, out chan<- types.Record) error {
	defer k.closeAllReaders()

	if len(k.restoredOffsets) > 0 {
		for i, r := range k.readers {
			partInt := k.partitionIDs[i]
			if offset, ok := k.restoredOffsets[partInt]; ok {
				if err := r.SetOffset(offset); err != nil {
					fmt.Printf("weibo/source: restore offset partition %d: %v\n", partInt, err)
				}
			}
		}
	}

	if k.cfg.parallel && len(k.readers) > 1 {
		return k.runParallel(ctx, out)
	}
	return k.runSerial(ctx, out)
}

// runSerial is the single-reader path.
func (k *KafkaSource) runSerial(ctx context.Context, out chan<- types.Record) error {
	r := k.readers[0]
	for {
		msg, err := k.fetchWithRetry(ctx, r)
		if err != nil {
			return err
		}
		k.trackOffset(msg)
		record := k.processRecord(msg)
		if record == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- *record:
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

// runParallel starts one goroutine per partition reader and merges
// their output into a single channel.
func (k *KafkaSource) runParallel(ctx context.Context, out chan<- types.Record) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	perr := make(chan error, len(k.readers))

	for i, r := range k.readers {
		wg.Add(1)
		go func(idx int, reader *kafka.Reader) {
			defer wg.Done()
			for {
				msg, err := k.fetchWithRetry(ctx, reader)
				if err != nil {
					if ctx.Err() == nil {
						perr <- err
					}
					return
				}
				k.trackOffset(msg)
				record := k.processRecord(msg)
				if record == nil {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case out <- *record:
				}
				// No CommitMessages here: parallel readers have no consumer
				// group (GroupID and KafkaParallel are mutually exclusive), so
				// broker-side commits are unavailable. Offset durability comes
				// from CheckpointOffset/RestoreOffset via SetOffset instead.
			}
		}(i, r)
	}

	wg.Wait()
	select {
	case err := <-perr:
		return err
	default:
		return nil
	}
}

// processRecord runs deserialization and applies the deser failure policy.
// Returns nil if the record should be dropped.
func (k *KafkaSource) processRecord(msg kafka.Message) *types.Record {
	record := KafkaToRecord(msg)
	if k.cfg.deserializer != nil {
		parsed, err := k.cfg.deserializer.Deserialize(record.Value, record.Headers)
		if err != nil {
			metrics.RecordsFailedTotal.Inc()
			switch k.cfg.deserFailPolicy {
			case DeserFailureDLQ:
				if k.cfg.deserDLQ != nil {
					failRecord := record.WithHeader("_deser_error", []byte(err.Error()))
					if werr := k.cfg.deserDLQ.Write(context.Background(), failRecord); werr != nil {
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
	}
	return &record
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

// closeAllReaders closes all partition readers.
func (k *KafkaSource) closeAllReaders() {
	for _, r := range k.readers {
		r.Close()
	}
}

// trackOffset records progress past a consumed message so CheckpointOffset
// reports the next offset to read for every partition — not just the single
// partition reader.Stats() happens to surface in consumer-group mode.
func (k *KafkaSource) trackOffset(msg kafka.Message) {
	k.offsetMu.Lock()
	if k.consumedOffsets == nil {
		k.consumedOffsets = make(map[int]int64)
	}
	k.consumedOffsets[msg.Partition] = msg.Offset + 1
	k.offsetMu.Unlock()
}

// CheckpointOffset returns the source's current position as JSON bytes:
// {"<partition>": <nextOffsetToRead>} for every partition consumed so far.
//
// This is the fallback offset source. The engine prefers barrier-aligned
// offsets captured at barrier injection (which reflect exactly the records
// processed before the barrier); CheckpointOffset reports the reader's fetch
// position, which may run ahead of the barrier. Either way it must cover
// every partition — the previous reader.Stats() implementation reported only
// one partition per reader, so a multi-partition consumer group lost all but
// one partition's offset from the checkpoint.
//
// Keyed by partition only, matching RestoreOffset/CommitOffsets and the
// barrier-aligned offset map; a single consumer group spanning multiple
// topics with overlapping partition ids is not distinguished (documented
// limitation shared by the whole offset path).
func (k *KafkaSource) CheckpointOffset() ([]byte, error) {
	k.offsetMu.Lock()
	offsets := make(map[string]int64, len(k.consumedOffsets))
	for part, next := range k.consumedOffsets {
		offsets[strconv.Itoa(part)] = next
	}
	k.offsetMu.Unlock()
	return json.Marshal(offsets)
}

// RestoreOffset restores per-partition offsets from a checkpoint.
func (k *KafkaSource) RestoreOffset(data []byte) error {
	var offsets map[string]int64
	if err := json.Unmarshal(data, &offsets); err != nil {
		return fmt.Errorf("restore offset: unmarshal: %w", err)
	}
	if k.restoredOffsets == nil {
		k.restoredOffsets = make(map[int]int64)
	}
	k.offsetMu.Lock()
	if k.consumedOffsets == nil {
		k.consumedOffsets = make(map[int]int64)
	}
	for partStr, off := range offsets {
		var partInt int
		fmt.Sscanf(partStr, "%d", &partInt)
		k.restoredOffsets[partInt] = off
		// Seed consumedOffsets so a partition that receives no new message
		// this run still carries its restored position into the next
		// checkpoint — otherwise a quiet partition would be dropped and a
		// later restart would not resume it.
		k.consumedOffsets[partInt] = off
	}
	k.offsetMu.Unlock()
	return nil
}

// KafkaToRecord converts a kafka.Message to a weibo.Record.
// Headers are copied into a map. Parsed is left nil here; the deserializer
// (if configured) populates it in runOnce after this conversion.
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
	_ OffsetCommitter  = (*KafkaSource)(nil)
	_ Drainable        = (*KafkaSource)(nil)
	_ Describable      = (*KafkaSource)(nil)
	_ Source           = (*kafkaSourceRunner)(nil)
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
	return k.commitWithRetry(ctx, k.readers[0], k.pending...)
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
	var offsets map[string]int64
	if err := json.Unmarshal(data, &offsets); err != nil {
		return fmt.Errorf("commit offsets: unmarshal: %w", err)
	}
	msgs := make([]kafka.Message, 0, len(offsets))
	for partStr, next := range offsets {
		if next <= 0 {
			continue
		}
		var part int
		fmt.Sscanf(partStr, "%d", &part)
		// CommitMessages commits msg.Offset+1 (the next offset to
		// read), matching our stored {"partition": nextOffset} shape.
		msgs = append(msgs, kafka.Message{
			Topic:     k.cfg.topic,
			Partition: part,
			Offset:    next - 1,
		})
	}
	if len(msgs) == 0 {
		return nil
	}
	return k.commitWithRetry(ctx, k.readers[0], msgs...)
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
