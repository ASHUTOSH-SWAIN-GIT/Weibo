package sink

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/auth"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
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
		panic("mailer/sink: TxnKafkaSink requires TxnKafkaBrokers(...)")
	}
	if cfg.topic == "" {
		panic("mailer/sink: TxnKafkaSink requires TxnKafkaTopic(...)")
	}
	if cfg.txnID == "" {
		panic("mailer/sink: TxnKafkaSink requires TxnKafkaTransactionalID(...)")
	}
	if cfg.markerTopic == "" {
		cfg.markerTopic = cfg.topic + ".checkpoints"
	}
	// Fail fast on unusable auth config (consistent with the other
	// Kafka connectors, which panic in their transport builders).
	if cfg.sasl != nil {
		if _, err := auth.BuildKgoSASL(*cfg.sasl); err != nil {
			panic(fmt.Sprintf("mailer/sink: TxnKafkaSink SASL: %v", err))
		}
	}
	if cfg.tls != nil {
		if _, err := auth.BuildTLSConfig(*cfg.tls); err != nil {
			panic(fmt.Sprintf("mailer/sink: TxnKafkaSink TLS: %v", err))
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
	cl, err := kgo.NewClient(append(opts,
		kgo.ConsumeTopics(s.cfg.markerTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)...)
	if err != nil {
		return false, fmt.Errorf("txn kafka sink: marker probe client: %w", err)
	}
	defer cl.Close()

	// Read the whole marker topic up to its current end offsets.
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, s.cfg.markerTopic)
	if err != nil {
		return false, fmt.Errorf("txn kafka sink: marker probe end offsets: %w", err)
	}
	remaining := map[int32]int64{}
	ends.Each(func(lo kadm.ListedOffset) {
		if lo.Offset > 0 {
			remaining[lo.Partition] = lo.Offset
		}
	})
	if len(remaining) == 0 {
		return false, nil // empty (or missing) marker topic: nothing committed
	}

	found := false
	for len(remaining) > 0 {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return false, fmt.Errorf("txn kafka sink: marker probe fetch: %v", errs[0].Err)
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if string(rec.Key) == s.cfg.txnID && string(rec.Value) == id {
				found = true
			}
			if end, ok := remaining[rec.Partition]; ok && rec.Offset+1 >= end {
				delete(remaining, rec.Partition)
			}
		})
	}
	return found, nil
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
	_ Sink             = (*TxnKafkaSink)(nil)
	_ CheckpointedSink = (*TxnKafkaSink)(nil)
	_ Describable      = (*TxnKafkaSink)(nil)
)
