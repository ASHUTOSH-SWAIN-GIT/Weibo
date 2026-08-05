package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/auth"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"

	"github.com/segmentio/kafka-go"
)

// The DLQ implementations below satisfy sink.DLQ, source.RecordSink and
// operator.RecordSink — all three are the same one-method shape, so a
// single instance can absorb failures from a sink, a source's
// deserializer, and a Process operator at once.
//
// A DLQ write that fails returns an error, which the failure policy
// propagates to the pipeline. That is deliberate: if the DLQ is broken
// too, the record has nowhere left to go, and failing loudly beats
// discarding it silently.

// dlqEnvelope is the JSON form written by FileDLQ. It carries the
// routing fields needed to replay a record — partition and offset above
// all — plus when it was dead-lettered, so a file can be correlated with
// logs from the same window.
type dlqEnvelope struct {
	DLQTime   string            `json:"dlq_time"`
	Key       string            `json:"key,omitempty"`
	Value     json.RawMessage   `json:"value,omitempty"`
	Timestamp int64             `json:"timestamp"` // UnixNano, 0 when unset
	Offset    int64             `json:"offset"`
	Partition int               `json:"partition"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// newDLQEnvelope converts a record for durable inspection. A value that
// is already JSON is embedded as-is so the file stays greppable; anything
// else becomes a JSON string rather than Go's default base64 for []byte,
// which would make the output unreadable.
func newDLQEnvelope(r types.Record, now time.Time) dlqEnvelope {
	env := dlqEnvelope{
		DLQTime:   now.UTC().Format(time.RFC3339Nano),
		Key:       string(r.Key),
		Offset:    r.Offset,
		Partition: r.Partition,
	}
	if !r.Timestamp.IsZero() {
		env.Timestamp = r.Timestamp.UnixNano()
	}
	if len(r.Value) > 0 {
		if json.Valid(r.Value) {
			env.Value = json.RawMessage(r.Value)
		} else if quoted, err := json.Marshal(string(r.Value)); err == nil {
			env.Value = json.RawMessage(quoted)
		}
	}
	if len(r.Headers) > 0 {
		env.Headers = make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			env.Headers[k] = string(v)
		}
	}
	return env
}

// ---- FileDLQ ---------------------------------------------------------------

// FileDLQ appends failed records to a local file as JSON lines.
//
// It is the zero-infrastructure dead-letter target: nothing to provision,
// and the output is directly greppable. Each line is a self-contained
// envelope carrying the record plus the partition/offset needed to replay
// it from the source.
//
// Writes are serialized by a mutex, so one instance is safe to share
// across sinks, sources and operators concurrently. Records are fsynced
// by default — a dead-letter record that a crash erases is the one
// record you most needed to keep.
//
// The caller owns the lifetime: call Close when the pipeline is done.
type FileDLQ struct {
	mu     sync.Mutex
	file   *os.File
	sync   bool
	closed bool
	path   string
}

// FileDLQOption configures a FileDLQ.
type FileDLQOption func(*FileDLQ)

// FileDLQNoSync disables the fsync after each record. Faster, at the
// cost of losing recent dead-letter records if the process dies.
func FileDLQNoSync() FileDLQOption {
	return func(d *FileDLQ) { d.sync = false }
}

// NewFileDLQ opens (creating if needed) path for appending, creating any
// missing parent directories.
//
// Unlike the sink constructors, which panic, this returns an error: a
// DLQ is auxiliary, its failure modes are ordinary and recoverable
// (permissions, a read-only mount), and the caller is better placed to
// decide whether that is fatal.
func NewFileDLQ(path string, opts ...FileDLQOption) (*FileDLQ, error) {
	d := &FileDLQ{sync: true, path: path}
	for _, opt := range opts {
		opt(d)
	}

	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("weibo/sink: file DLQ: create dir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("weibo/sink: file DLQ: open %s: %w", path, err)
	}
	d.file = f
	return d, nil
}

// Write appends one record. It satisfies sink.DLQ, source.RecordSink and
// operator.RecordSink.
func (d *FileDLQ) Write(_ context.Context, r types.Record) error {
	line, err := json.Marshal(newDLQEnvelope(r, time.Now()))
	if err != nil {
		return fmt.Errorf("weibo/sink: file DLQ: encode: %w", err)
	}
	line = append(line, '\n')

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return fmt.Errorf("weibo/sink: file DLQ: write after close")
	}
	if _, err := d.file.Write(line); err != nil {
		return fmt.Errorf("weibo/sink: file DLQ: write: %w", err)
	}
	if d.sync {
		if err := d.file.Sync(); err != nil {
			return fmt.Errorf("weibo/sink: file DLQ: sync: %w", err)
		}
	}
	return nil
}

// Close flushes and closes the underlying file. It is idempotent.
func (d *FileDLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	return d.file.Close()
}

// Path returns the file being written to.
func (d *FileDLQ) Path() string { return d.path }

// ---- KafkaDLQ --------------------------------------------------------------

// KafkaDLQ publishes failed records to a Kafka topic.
//
// This is the replayable dead-letter target: the records land back on a
// topic, so recovery is a normal pipeline reading the DLQ topic rather
// than a bespoke import. The original key, headers and timestamp are
// preserved so a replay looks like the original traffic.
//
// Writes are synchronous — the caller learns whether the record was
// actually accepted, which is the point of a dead-letter path.
//
// The caller owns the lifetime: call Close when the pipeline is done.
type KafkaDLQ struct {
	writer     *kafka.Writer
	serializer Serializer
	topic      string
	brokers    []string
}

// kafkaDLQConfig holds resolved KafkaDLQ configuration.
type kafkaDLQConfig struct {
	brokers    []string
	topic      string
	serializer Serializer
	sasl       *auth.SASLConfig
	tls        *auth.TLSConfig
}

// KafkaDLQOption configures a KafkaDLQ. Brokers and Topic are required.
type KafkaDLQOption func(*kafkaDLQConfig)

// KafkaDLQBrokers sets the bootstrap brokers. Required.
func KafkaDLQBrokers(brokers ...string) KafkaDLQOption {
	return func(c *kafkaDLQConfig) { c.brokers = brokers }
}

// KafkaDLQTopic sets the dead-letter topic. Required.
func KafkaDLQTopic(topic string) KafkaDLQOption {
	return func(c *kafkaDLQConfig) { c.topic = topic }
}

// KafkaDLQSerialize sets how a record's value is serialized. Defaults to
// the record's raw Value bytes.
func KafkaDLQSerialize(s Serializer) KafkaDLQOption {
	return func(c *kafkaDLQConfig) { c.serializer = s }
}

// KafkaDLQSASL configures SASL authentication.
func KafkaDLQSASL(cfg auth.SASLConfig) KafkaDLQOption {
	return func(c *kafkaDLQConfig) { c.sasl = &cfg }
}

// KafkaDLQTLS configures TLS.
func KafkaDLQTLS(cfg auth.TLSConfig) KafkaDLQOption {
	return func(c *kafkaDLQConfig) { c.tls = &cfg }
}

// NewKafkaDLQ creates a DLQ that publishes to a Kafka topic. Brokers and
// topic are required; if missing, NewKafkaDLQ panics — matching the
// fail-fast construction of the Kafka sink.
func NewKafkaDLQ(opts ...KafkaDLQOption) *KafkaDLQ {
	cfg := kafkaDLQConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if len(cfg.brokers) == 0 {
		panic("weibo/sink: KafkaDLQ requires KafkaDLQBrokers(...)")
	}
	if cfg.topic == "" {
		panic("weibo/sink: KafkaDLQTopic(...) is required")
	}

	w := &kafka.Writer{
		Addr:     kafka.TCP(cfg.brokers...),
		Topic:    cfg.topic,
		Balancer: &kafka.Hash{},
		// One record at a time: a dead-letter write must be durable
		// before Write returns, so batching it would be wrong.
		BatchSize:    1,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	if cfg.sasl != nil || cfg.tls != nil {
		w.Transport = buildTransport(cfg.sasl, cfg.tls)
	}

	return &KafkaDLQ{
		writer:     w,
		serializer: cfg.serializer,
		topic:      cfg.topic,
		brokers:    cfg.brokers,
	}
}

// Write publishes one record. It satisfies sink.DLQ, source.RecordSink
// and operator.RecordSink.
func (d *KafkaDLQ) Write(ctx context.Context, r types.Record) error {
	value := r.Value
	if d.serializer != nil {
		out, err := d.serializer.Serialize(r)
		if err != nil {
			// Deliberately not falling back to the raw value: the DLQ
			// exists to preserve records faithfully, and quietly writing
			// a different payload than the serializer produced would
			// corrupt the replay path.
			return fmt.Errorf("weibo/sink: kafka DLQ: serialize: %w", err)
		}
		value = out
	}

	var headers []kafka.Header
	for k, v := range r.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: v})
	}

	msg := kafka.Message{Key: r.Key, Value: value, Headers: headers}
	if !r.Timestamp.IsZero() {
		msg.Time = r.Timestamp
	}

	if err := d.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("weibo/sink: kafka DLQ: write to %s: %w", d.topic, err)
	}
	return nil
}

// Close flushes and closes the underlying writer.
func (d *KafkaDLQ) Close() error { return d.writer.Close() }

// Topic returns the dead-letter topic.
func (d *KafkaDLQ) Topic() string { return d.topic }
