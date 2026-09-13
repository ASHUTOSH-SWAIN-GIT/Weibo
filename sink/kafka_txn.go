package sink

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/auth"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// TxnKafkaSink writes records to Kafka inside transactions, one
// transaction per checkpoint interval, implementing CheckpointedSink
// for end-to-end exactly-once pipelines:
//
//	source offsets + operator state + sink output commit atomically.
//
// Built on franz-go (segmentio/kafka-go has no transactional
// producer). Every checkpoint transaction also carries a marker
// record in the marker topic (default "<topic>.checkpoints"); after a
// crash, the marker's visibility under read_committed proves whether
// the transaction committed — this resolves prepared-but-unconfirmed
// checkpoints without Kafka transaction resumption.
//
// IMPORTANT: downstream consumers of the output topic MUST use
// isolation.level=read_committed, or they will observe records from
// aborted transactions. Records become visible only when the
// checkpoint interval's transaction commits — the checkpoint interval
// is therefore also the output visibility latency.
//
// Usage:
//
//	sk := sink.NewTxnKafkaSink(
//	    sink.TxnKafkaBrokers("localhost:9092"),
//	    sink.TxnKafkaTopic("order-summary"),
//	    sink.TxnKafkaTransactionalID("order-pipeline-1"),
//	)
//
// The transactional ID must be unique per pipeline instance; a second
// instance with the same ID fences the first (Kafka zombie fencing).
type TxnKafkaSink struct {
	cfg txnKafkaConfig

	client     *kgo.Client
	onPrepared func(id string, err error)

	mu         sync.Mutex
	waiters    map[string]chan struct{}
	produceErr error
}

type txnKafkaConfig struct {
	brokers     []string
	topic       string
	txnID       string
	markerTopic string
	serializer  Serializer
	sasl        *auth.SASLConfig
	tls         *auth.TLSConfig
}

// TxnKafkaOption configures a TxnKafkaSink.
type TxnKafkaOption func(*txnKafkaConfig)

// TxnKafkaBrokers sets the Kafka bootstrap brokers.
func TxnKafkaBrokers(brokers ...string) TxnKafkaOption {
	return func(c *txnKafkaConfig) { c.brokers = brokers }
}

// TxnKafkaTopic sets the output topic.
func TxnKafkaTopic(topic string) TxnKafkaOption {
	return func(c *txnKafkaConfig) { c.topic = topic }
}

// TxnKafkaTransactionalID sets the Kafka transactional ID. Required.
// Must be stable across restarts of the same pipeline (fencing and
// marker attribution depend on it) and unique per pipeline instance.
func TxnKafkaTransactionalID(id string) TxnKafkaOption {
	return func(c *txnKafkaConfig) { c.txnID = id }
}

// TxnKafkaMarkerTopic overrides the transaction-marker topic
// (default "<topic>.checkpoints"). Should be compacted; deleting it
// breaks crash recovery of prepared checkpoints.
func TxnKafkaMarkerTopic(topic string) TxnKafkaOption {
	return func(c *txnKafkaConfig) { c.markerTopic = topic }
}

// TxnKafkaSerialize sets a serializer applied to record values.
func TxnKafkaSerialize(s Serializer) TxnKafkaOption {
	return func(c *txnKafkaConfig) { c.serializer = s }
}

// TxnKafkaSASL enables SASL authentication (PLAIN, SCRAM-SHA-256, or
// SCRAM-SHA-512) for hosted/secured clusters. Applies to both the
// transactional producer and the recovery marker-probe consumer.
func TxnKafkaSASL(cfg auth.SASLConfig) TxnKafkaOption {
	return func(c *txnKafkaConfig) { c.sasl = &cfg }
}

// TxnKafkaTLS enables TLS. An empty auth.TLSConfig turns TLS on with
// system root CAs (the common hosted-Kafka case); CAFile/CertFile/
// KeyFile configure private CAs and mutual TLS.
func TxnKafkaTLS(cfg auth.TLSConfig) TxnKafkaOption {
	return func(c *txnKafkaConfig) { c.tls = &cfg }
}

// NewTxnKafkaSink creates a transactional Kafka sink. Panics on
// missing required configuration (broker connection errors surface
// from Write instead).
func NewTxnKafkaSink(opts ...TxnKafkaOption) *TxnKafkaSink {
	cfg := txnKafkaConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if len(cfg.brokers) == 0 {
		panic("weibo/sink: TxnKafkaSink requires TxnKafkaBrokers(...)")
	}
	if cfg.topic == "" {
		panic("weibo/sink: TxnKafkaSink requires TxnKafkaTopic(...)")
	}
	if cfg.txnID == "" {
		panic("weibo/sink: TxnKafkaSink requires TxnKafkaTransactionalID(...)")
	}
	if cfg.markerTopic == "" {
		cfg.markerTopic = cfg.topic + ".checkpoints"
	}
	// Fail fast on unusable auth config (consistent with the other
	// Kafka connectors, which panic in their transport builders).
	if cfg.sasl != nil {
		if _, err := auth.BuildKgoSASL(*cfg.sasl); err != nil {
			panic(fmt.Sprintf("weibo/sink: TxnKafkaSink SASL: %v", err))
		}
	}
	if cfg.tls != nil {
		if _, err := auth.BuildTLSConfig(*cfg.tls); err != nil {
			panic(fmt.Sprintf("weibo/sink: TxnKafkaSink TLS: %v", err))
		}
	}
	return &TxnKafkaSink{
		cfg:     cfg,
		waiters: map[string]chan struct{}{},
	}
}

// baseOpts returns the kgo options shared by the transactional
// producer and the recovery marker-probe consumer: brokers plus the
// configured SASL/TLS. Both clients MUST authenticate identically —
// a probe that cannot reach the cluster would break crash recovery.
func (s *TxnKafkaSink) baseOpts() ([]kgo.Opt, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(s.cfg.brokers...)}
	if s.cfg.sasl != nil {
		mech, err := auth.BuildKgoSASL(*s.cfg.sasl)
		if err != nil {
			return nil, fmt.Errorf("sasl: %w", err)
		}
		opts = append(opts, kgo.SASL(mech))
	}
	if s.cfg.tls != nil {
		tlsConf, err := auth.BuildTLSConfig(*s.cfg.tls)
		if err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConf))
	}
	return opts, nil
}

// TransactionalID reports the configured transactional ID (recorded
// in checkpoint files for diagnostics).
func (s *TxnKafkaSink) TransactionalID() string { return s.cfg.txnID }

// SetOnPrepared implements CheckpointedSink.
func (s *TxnKafkaSink) SetOnPrepared(fn func(id string, err error)) { s.onPrepared = fn }

// Write consumes records, producing them into the currently open
// transaction. On a checkpoint barrier it flushes, writes the marker,
// notifies the coordinator, and blocks until Commit or Abort.
func (s *TxnKafkaSink) Write(ctx context.Context, in <-chan types.Record) error {
	opts, err := s.baseOpts()
	if err != nil {
		return fmt.Errorf("txn kafka sink: %w", err)
	}
	client, err := kgo.NewClient(append(opts,
		kgo.TransactionalID(s.cfg.txnID),
		kgo.DefaultProduceTopic(s.cfg.topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)...)
	if err != nil {
		return fmt.Errorf("txn kafka sink: client: %w", err)
	}
	s.mu.Lock()
	s.client = client
	s.mu.Unlock()
	defer client.Close()

	if err := client.BeginTransaction(); err != nil {
		return fmt.Errorf("txn kafka sink: begin: %w", err)
	}
	txnOpen := true
	defer func() {
		// Records produced after the last barrier belong to no
		// checkpoint: abort them. They stay invisible and are
		// replayed from the last checkpoint's offsets on restart.
		if txnOpen {
			abortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = client.AbortBufferedRecords(abortCtx)
			_ = client.EndTransaction(abortCtx, kgo.TryAbort)
		}
	}()

	for r := range in {
		if r.IsWatermark {
			continue
		}
		if r.IsBarrier {
			id := r.CheckpointID

			// Marker rides inside the same transaction: its
			// read_committed visibility after a crash proves the
			// transaction committed.
			s.produce(ctx, &kgo.Record{
				Topic: s.cfg.markerTopic,
				Key:   []byte(s.cfg.txnID),
				Value: []byte(id),
			})
			flushErr := client.Flush(ctx)

			s.mu.Lock()
			if flushErr == nil {
				flushErr = s.produceErr
			}
			ch := make(chan struct{})
			s.waiters[id] = ch
			s.mu.Unlock()

			s.onPrepared(id, flushErr)

			select {
			case <-ch: // Commit or Abort ran; next txn is open (or aborted)
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		value := r.Value
		if s.cfg.serializer != nil {
			v, serr := s.cfg.serializer.Serialize(r)
			if serr != nil {
				s.setProduceErr(fmt.Errorf("serialize: %w", serr))
				continue
			}
			value = v
		}
		headers := make([]kgo.RecordHeader, 0, len(r.Headers))
		for k, v := range r.Headers {
			headers = append(headers, kgo.RecordHeader{Key: k, Value: v})
		}
		s.produce(ctx, &kgo.Record{
			Key:       r.Key,
			Value:     value,
			Timestamp: r.Timestamp,
			Headers:   headers,
		})
	}
	return nil
}

func (s *TxnKafkaSink) produce(ctx context.Context, rec *kgo.Record) {
	s.client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		if err != nil {
			s.setProduceErr(err)
		}
	})
}

func (s *TxnKafkaSink) setProduceErr(err error) {
	s.mu.Lock()
	if s.produceErr == nil {
		s.produceErr = err
	}
	s.mu.Unlock()
}

// clearProduceErr resets the latched produce error once a transaction has
// ended. produceErr belongs to the transaction that just closed: the barrier
// already folded it into that checkpoint's flushErr (before onPrepared), and
// Flush drained every produce callback before the barrier read it, so no late
// callback from the old transaction can set it after this point. Without the
// reset, one transient produce error would fail every subsequent checkpoint.
func (s *TxnKafkaSink) clearProduceErr() {
	s.mu.Lock()
	s.produceErr = nil
	s.mu.Unlock()
}

// Commit implements CheckpointedSink: commits the transaction for
// checkpoint id, opens the next one, and unblocks Write.
func (s *TxnKafkaSink) Commit(ctx context.Context, id string) error {
	s.mu.Lock()
	client := s.client
	s.mu.Unlock()
	if client == nil {
		return fmt.Errorf("txn kafka sink: commit %s: sink not running", id)
	}
	if err := client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("txn kafka sink: commit %s: %w", id, err)
	}
	if err := client.BeginTransaction(); err != nil {
		return fmt.Errorf("txn kafka sink: begin after %s: %w", id, err)
	}
	s.clearProduceErr() // fresh transaction: don't carry the old one's error
	s.signal(id)
	return nil
}

// Abort implements CheckpointedSink: aborts the transaction for
// checkpoint id and unblocks Write. During recovery (sink not
// running) it is a no-op — producer fencing at the next InitProducerID
// aborts the dangling transaction broker-side.
func (s *TxnKafkaSink) Abort(ctx context.Context, id string) error {
	s.mu.Lock()
	client := s.client
	s.mu.Unlock()
	if client == nil {
		return nil // recovery path: fencing handles it
	}
	if err := client.AbortBufferedRecords(ctx); err != nil {
		return fmt.Errorf("txn kafka sink: abort %s: %w", id, err)
	}
	if err := client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		return fmt.Errorf("txn kafka sink: abort %s: %w", id, err)
	}
	if err := client.BeginTransaction(); err != nil {
		return fmt.Errorf("txn kafka sink: begin after abort %s: %w", id, err)
	}
	s.clearProduceErr() // fresh transaction: don't carry the old one's error
	s.signal(id)
	return nil
}

func (s *TxnKafkaSink) signal(id string) {
	s.mu.Lock()
	if ch, ok := s.waiters[id]; ok {
		close(ch)
		delete(s.waiters, id)
	}
	s.mu.Unlock()
}

// WasCommitted implements CheckpointedSink: reads the marker topic
// under read_committed isolation and reports whether checkpoint id's
// marker is visible — i.e. whether its transaction committed.
func (s *TxnKafkaSink) WasCommitted(ctx context.Context, id string) (bool, error) {
	opts, err := s.baseOpts()
	if err != nil {
		return false, fmt.Errorf("txn kafka sink: marker probe: %w", err)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return false, fmt.Errorf("txn kafka sink: marker probe client: %w", err)
	}
	defer cl.Close()

	// Snapshot the read-committed boundary for every marker partition. Unlike
	// the high watermark, this last stable offset excludes open transactions,
	// so reaching it proves that every currently decidable marker was scanned.
	adm := kadm.NewClient(cl)
	ends, err := adm.ListCommittedOffsets(ctx, s.cfg.markerTopic)
	if err != nil {
		return false, fmt.Errorf("txn kafka sink: marker probe stable offsets: %w", err)
	}
	boundaries := make(map[int32]int64)
	ends.Each(func(lo kadm.ListedOffset) {
		if lo.Err == nil && lo.Partition >= 0 {
			boundaries[lo.Partition] = lo.Offset
		}
	})
	for _, partition := range ends[s.cfg.markerTopic] {
		if partition.Err != nil {
			return false, fmt.Errorf("txn kafka sink: marker probe partition %d: %w", partition.Partition, partition.Err)
		}
	}
	if len(boundaries) == 0 {
		return false, fmt.Errorf("txn kafka sink: marker probe: topic %q has no readable partitions", s.cfg.markerTopic)
	}
	starts, err := adm.ListStartOffsets(ctx, s.cfg.markerTopic)
	if err != nil {
		return false, fmt.Errorf("txn kafka sink: marker probe start offsets: %w", err)
	}
	positions := make(map[int32]int64, len(boundaries))
	for partition := range boundaries {
		start, ok := starts.Lookup(s.cfg.markerTopic, partition)
		if !ok || start.Err != nil {
			return false, fmt.Errorf("txn kafka sink: marker probe start offset partition %d unavailable", partition)
		}
		positions[partition] = start.Offset
	}
	metadata, err := adm.Metadata(ctx, s.cfg.markerTopic)
	if err != nil {
		return false, fmt.Errorf("txn kafka sink: marker probe metadata: %w", err)
	}
	detail, ok := metadata.Topics[s.cfg.markerTopic]
	if !ok || detail.Err != nil {
		return false, fmt.Errorf("txn kafka sink: marker probe metadata unavailable for %q", s.cfg.markerTopic)
	}
	if allMarkerPartitionsDone(boundaries, positions) {
		return false, nil
	}

	for {
		req := kmsg.NewPtrFetchRequest()
		// v12 addresses topics by name. v13+ requires metadata topic IDs,
		// which add no value to this one-shot recovery scan.
		req.Version = 12
		req.IsolationLevel = 1
		req.MaxWaitMillis = 500
		req.MinBytes = 1
		req.MaxBytes = 50 << 20
		topic := kmsg.NewFetchRequestTopic()
		topic.Topic = s.cfg.markerTopic
		topic.TopicID = [16]byte(detail.ID)
		for partition, boundary := range boundaries {
			if positions[partition] >= boundary {
				continue
			}
			part := kmsg.NewFetchRequestTopicPartition()
			part.Partition, part.FetchOffset, part.PartitionMaxBytes = partition, positions[partition], 1<<20
			topic.Partitions = append(topic.Partitions, part)
		}
		req.Topics = append(req.Topics, topic)
		found := false
		progressed := false
		for _, shard := range cl.RequestSharded(ctx, req) {
			if shard.Err != nil {
				return false, fmt.Errorf("txn kafka sink: marker probe fetch: %w", shard.Err)
			}
			resp, ok := shard.Resp.(*kmsg.FetchResponse)
			if !ok {
				return false, fmt.Errorf("txn kafka sink: marker probe: unexpected fetch response %T", shard.Resp)
			}
			for _, responseTopic := range resp.Topics {
				for i := range responseTopic.Partitions {
					raw := &responseTopic.Partitions[i]
					position := positions[raw.Partition]
					part, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: position, IsolationLevel: kgo.ReadCommitted(), Topic: s.cfg.markerTopic, Partition: raw.Partition}, raw, nil, nil)
					if part.Err != nil {
						return false, fmt.Errorf("txn kafka sink: marker probe fetch partition %d: %w", raw.Partition, part.Err)
					}
					matched, _ := inspectMarkerPartition(part, boundaries[raw.Partition], s.cfg.txnID, id)
					if matched {
						found = true
					}
					if next > position {
						positions[raw.Partition], progressed = next, true
					}
				}
			}
		}
		if found {
			return true, nil
		}
		if allMarkerPartitionsDone(boundaries, positions) {
			return false, nil
		}
		if !progressed {
			return false, fmt.Errorf("txn kafka sink: marker probe made no progress before stable boundary")
		}
	}
}

func inspectMarkerPartition(part kgo.FetchPartition, boundary int64, txnID, checkpointID string) (bool, bool) {
	var lastOffset int64 = -1
	for _, rec := range part.Records {
		lastOffset = rec.Offset
		if string(rec.Key) == txnID && string(rec.Value) == checkpointID {
			return true, true
		}
	}
	// A visible record at boundary-1 reaches the snapshot directly. If the
	// tail contains only aborted/control batches, the next sequential fetch is
	// empty while reporting the same LSO; that also proves the boundary reached.
	reached := lastOffset+1 >= boundary || (len(part.Records) == 0 && part.LastStableOffset >= boundary)
	return false, reached
}

func allMarkerPartitionsDone(boundaries map[int32]int64, positions map[int32]int64) bool {
	for partition, boundary := range boundaries {
		if positions[partition] < boundary {
			return false
		}
	}
	return true
}

// SinkCapabilities declares the optional sink contracts TxnKafkaSink supports.
func (s *TxnKafkaSink) SinkCapabilities() Capabilities {
	return Capabilities{
		CoordinatedCheckpoints: true,
		Describe:               true,
	}
}

// Describe returns dashboard metadata.
func (s *TxnKafkaSink) Describe() SinkInfo {
	props := map[string]string{
		"topic":        s.cfg.topic,
		"marker_topic": s.cfg.markerTopic,
		"txn_id":       s.cfg.txnID,
		"exactly_once": "true",
	}
	if s.cfg.sasl != nil {
		props["sasl"] = string(s.cfg.sasl.Mechanism)
	}
	if s.cfg.tls != nil {
		props["tls"] = "enabled"
	}
	return SinkInfo{Type: "TxnKafka", Props: props}
}

// Compile-time checks.
var (
	_ Sink               = (*TxnKafkaSink)(nil)
	_ CheckpointedSink   = (*TxnKafkaSink)(nil)
	_ Describable        = (*TxnKafkaSink)(nil)
	_ CapabilityProvider = (*TxnKafkaSink)(nil)
)
