package sink

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/auth"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"

	"github.com/segmentio/kafka-go"
)

// KafkaSink writes records to a single Kafka topic.
// It implements the Sink interface for use in mailer pipelines.
//
// Configure a KafkaSink with functional options via NewKafkaSink:
//
//	sink := sink.NewKafkaSink(
//	    sink.KafkaSinkBrokers("localhost:9092"),
//	    sink.KafkaSinkTopic("order-summary"),
//	    sink.KafkaSinkBatchSize(200),
//	    sink.KafkaSinkRequiredAcks(sink.AcksAll),
//	    sink.KafkaSinkSASL(auth.SASLConfig{Mechanism: auth.SASLPlain, Username: "u", Password: "p"}),
//	    sink.KafkaSinkSerialize(sink.NewJSONSerializer()),
//	)
//
// Records are written in batches for efficiency. On context cancellation,
// the sink drains remaining records for up to 5 seconds before flushing.
//
// mailer.Record fields are mapped to kafka.Message as follows:
//   - Key       -> Message.Key
//   - Value     -> Message.Value (or serializer output if configured)
//   - Timestamp -> Message.Time
//   - Headers   -> Message.Headers
type KafkaSink struct {
	cfg    kafkaSinkConfig
	writer *kafka.Writer
}

// NewKafkaSink creates a Sink that writes to a Kafka topic.
// Brokers and Topic are required; if missing, NewKafkaSink panics.
func NewKafkaSink(opts ...KafkaSinkOption) *KafkaSink {
	cfg := kafkaSinkConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if len(cfg.brokers) == 0 {
		panic("mailer/sink: KafkaSink requires KafkaSinkBrokers(...)")
	}
	if cfg.topic == "" {
		panic("mailer/sink: KafkaSink requires KafkaSinkTopic(...)")
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.brokers...),
		Topic:        cfg.topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    cfg.batchSize,
		BatchTimeout: cfg.batchTimeout,
		RequiredAcks: toKafkaAcks(cfg.acks),
		Async:        cfg.async,
	}

	// Wire SASL/TLS via Transport (kafka-go Writer uses Transport, not Dialer).
	if cfg.sasl != nil || cfg.tls != nil {
		w.Transport = buildTransport(cfg.sasl, cfg.tls)
	}

	return &KafkaSink{cfg: cfg, writer: w}
}

// toKafkaAcks maps the mailer AcksLevel enum to kafka-go's RequiredAcks value.
func toKafkaAcks(level AcksLevel) kafka.RequiredAcks {
	switch level {
	case AcksNone:
		return kafka.RequireNone
	case AcksAll:
		return kafka.RequireAll
	default:
		return kafka.RequireOne
	}
}

// buildTransport constructs a kafka-go Transport with SASL and/or TLS.
// This is the sink-side equivalent of the source's buildDialer.
func buildTransport(saslCfg *auth.SASLConfig, tlsCfg *auth.TLSConfig) *kafka.Transport {
	t := &kafka.Transport{}

	if saslCfg != nil {
		mechanism, err := auth.BuildSASLMechanism(*saslCfg)
		if err != nil {
			panic(fmt.Sprintf("mailer/sink: %v", err))
		}
		t.SASL = mechanism
	}

	if tlsCfg != nil {
		tlsConf, err := auth.BuildTLSConfig(*tlsCfg)
		if err != nil {
			panic(fmt.Sprintf("mailer/sink: %v", err))
		}
		t.TLS = tlsConf
	}

	return t
}

// kafkaBatchEntry holds a converted kafka.Message and the original Record
// so that failed writes can apply the failure policy to the original data.
type kafkaBatchEntry struct {
	msg    kafka.Message
	record types.Record
}

// Write reads records from the input channel and writes them to Kafka.
// It batches records for efficiency. On write failure, the batch is
// retried up to cfg.maxRetries times with exponential backoff.  After
// all retries are exhausted, the failure policy is applied to each
// record in the batch.
//
// On context cancellation, the sink drains remaining records for up to
// shutdownTimeout before performing a final flush.
func (k *KafkaSink) Write(ctx context.Context, in <-chan types.Record) error {
	defer k.writer.Close()

	const shutdownTimeout = 5 * time.Second
	flushThreshold := 100
	if k.cfg.batchSize > 0 {
		flushThreshold = k.cfg.batchSize
	}

	var (
		batch   []kafkaBatchEntry
		batchMu sync.Mutex
		wg      sync.WaitGroup
		errCh   = make(chan error, 1)
	)

	flush := func(flushCtx context.Context) {
		batchMu.Lock()
		if len(batch) == 0 {
			batchMu.Unlock()
			return
		}
		toWrite := batch
		batch = nil
		batchMu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			msgs := make([]kafka.Message, len(toWrite))
			for i, e := range toWrite {
				msgs[i] = e.msg
			}
			if err := k.writeWithRetry(flushCtx, msgs); err != nil {
				for _, e := range toWrite {
					if ferr := applyFailurePolicy(flushCtx, k.cfg.failurePolicy, k.cfg.dlq, e.record); ferr != nil {
						select {
						case errCh <- ferr:
						default:
						}
						return
					}
				}
			}
		}()
	}

	drain := func() {
		deadline := time.NewTimer(shutdownTimeout)
		defer deadline.Stop()
		for {
			select {
			case record, ok := <-in:
				if !ok {
					return
				}
				msg, rec := k.recordToKafka(record), record
				batchMu.Lock()
				batch = append(batch, kafkaBatchEntry{msg: msg, record: rec})
				batchMu.Unlock()
			case <-deadline.C:
				return
			}
		}
	}

	for {
		select {
		case err := <-errCh:
			wg.Wait()
			return err

		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			drain()
			flush(shutdownCtx)
			wg.Wait()
			cancel()
			return ctx.Err()

		case record, ok := <-in:
			if !ok {
				flush(ctx)
				wg.Wait()
				return nil
			}

			msg, rec := k.recordToKafka(record), record
			batchMu.Lock()
			batch = append(batch, kafkaBatchEntry{msg: msg, record: rec})
			full := len(batch) >= flushThreshold
			batchMu.Unlock()

			if full {
				flush(ctx)
			}
		}
	}
}

// writeWithRetry attempts to write the batch, retrying up to maxRetries
// times with exponential backoff. Returns nil on success.
func (k *KafkaSink) writeWithRetry(ctx context.Context, msgs []kafka.Message) error {
	var lastErr error
	for attempt := 0; attempt <= k.cfg.maxRetries; attempt++ {
		err := k.writer.WriteMessages(ctx, msgs...)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < k.cfg.maxRetries {
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return lastErr
}

// recordToKafka converts a mailer.Record to a kafka.Message.
// If a serializer is configured, it runs on the record to produce the
// message value; otherwise Record.Value is used directly.
func (k *KafkaSink) recordToKafka(r types.Record) kafka.Message {
	var value []byte = r.Value

	if k.cfg.serializer != nil {
		out, err := k.cfg.serializer.Serialize(r)
		if err != nil {
			fmt.Printf("mailer/sink: serialize error: %v\n", err)
		} else {
			value = out
		}
	}

	var ts time.Time
	if !r.Timestamp.IsZero() {
		ts = r.Timestamp
	}

	var headers []kafka.Header
	for k, v := range r.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: v})
	}

	return kafka.Message{
		Key:     r.Key,
		Value:   value,
		Time:    ts,
		Headers: headers,
	}
}

// RecordToKafka converts a mailer.Record to a kafka.Message.
// Exported for backwards compatibility and testing. Does not run any
// configured serializer — use the sink's internal recordToKafka for that.
func RecordToKafka(r types.Record) kafka.Message {
	var ts time.Time
	if !r.Timestamp.IsZero() {
		ts = r.Timestamp
	}

	var headers []kafka.Header
	for k, v := range r.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: v})
	}

	return kafka.Message{
		Key:     r.Key,
		Value:   r.Value,
		Time:    ts,
		Headers: headers,
	}
}

// Compile-time check.
var _ Sink = (*KafkaSink)(nil)

// Describe returns metadata about this Kafka sink for the dashboard.
func (k *KafkaSink) Describe() SinkInfo {
	props := map[string]string{
		"brokers": strings.Join(k.cfg.brokers, ","),
		"topic":   k.cfg.topic,
	}
	props["batch_size"] = fmt.Sprintf("%d", k.cfg.batchSize)
	props["batch_timeout"] = k.cfg.batchTimeout.String()
	props["acks"] = k.cfg.acks.Display()
	if k.cfg.async {
		props["async"] = "true"
	}
	if k.cfg.serializer != nil {
		props["serializer"] = fmt.Sprintf("%T", k.cfg.serializer)
	}
	if k.cfg.sasl != nil {
		props["sasl"] = string(k.cfg.sasl.Mechanism)
	}
	if k.cfg.tls != nil {
		props["tls"] = "enabled"
	}

	return SinkInfo{
		Type:  "Kafka",
		Props: props,
	}
}
