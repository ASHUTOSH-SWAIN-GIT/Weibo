package sink

import (
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/auth"
)

// AcksLevel controls how many broker acknowledgements the Kafka sink waits
// for before considering a write successful.
type AcksLevel int

const (
	// AcksNone does not wait for any acknowledgement (fire-and-forget).
	AcksNone AcksLevel = 0
	// AcksLeader waits for the leader broker to acknowledge the write.
	AcksLeader AcksLevel = 1
	// AcksAll waits for all in-sync replicas to acknowledge the write.
	AcksAll AcksLevel = -1
)

// Display returns a human-readable acks description for the dashboard.
func (a AcksLevel) Display() string {
	switch a {
	case AcksNone:
		return "none"
	case AcksAll:
		return "all"
	default:
		return "leader"
	}
}

// kafkaSinkConfig holds the resolved configuration for a KafkaSink.
// It is populated by KafkaSinkOption functions and read by NewKafkaSink.
type kafkaSinkConfig struct {
	brokers      []string
	topic        string
	batchSize    int
	batchTimeout time.Duration
	acks         AcksLevel
	acksSet      bool
	async        bool

	sasl       *auth.SASLConfig
	tls        *auth.TLSConfig
	serializer Serializer
}

// KafkaSinkOption configures a KafkaSink. Pass one or more to NewKafkaSink.
// Brokers and Topic are required.
type KafkaSinkOption func(*kafkaSinkConfig)

// KafkaSinkBrokers sets the Kafka bootstrap brokers.
// Required. Example: KafkaSinkBrokers("localhost:9092").
func KafkaSinkBrokers(brokers ...string) KafkaSinkOption {
	return func(c *kafkaSinkConfig) { c.brokers = brokers }
}

// KafkaSinkTopic sets the destination topic.
// Required.
func KafkaSinkTopic(topic string) KafkaSinkOption {
	return func(c *kafkaSinkConfig) { c.topic = topic }
}

// KafkaSinkBatchSize sets the max messages per batch (default 100).
// Larger batches improve throughput at the cost of latency.
func KafkaSinkBatchSize(n int) KafkaSinkOption {
	return func(c *kafkaSinkConfig) { c.batchSize = n }
}

// KafkaSinkBatchTimeout sets the max wait before flushing a partial batch
// (default 1s). This bounds latency even when the batch is not full.
func KafkaSinkBatchTimeout(d time.Duration) KafkaSinkOption {
	return func(c *kafkaSinkConfig) { c.batchTimeout = d }
}

// KafkaSinkRequiredAcks sets the acknowledgement level (default AcksLeader).
func KafkaSinkRequiredAcks(level AcksLevel) KafkaSinkOption {
	return func(c *kafkaSinkConfig) { c.acks = level; c.acksSet = true }
}

// KafkaSinkAsync enables asynchronous writes — WriteMessages returns
// immediately without waiting for acknowledgement. Improves throughput
// but offers no durability guarantee on failure.
func KafkaSinkAsync() KafkaSinkOption {
	return func(c *kafkaSinkConfig) { c.async = true }
}

// KafkaSinkSASL enables SASL authentication on the Kafka connection.
func KafkaSinkSASL(cfg auth.SASLConfig) KafkaSinkOption {
	return func(c *kafkaSinkConfig) { c.sasl = &cfg }
}

// KafkaSinkTLS enables TLS on the Kafka connection.
func KafkaSinkTLS(cfg auth.TLSConfig) KafkaSinkOption {
	return func(c *kafkaSinkConfig) { c.tls = &cfg }
}

// KafkaSinkSerialize sets a Serializer that runs on every record before it
// is written to Kafka. If Record.Parsed is set, the serializer receives it;
// otherwise it receives Record.Value. The serializer's output replaces
// Record.Value in the Kafka message.
func KafkaSinkSerialize(s Serializer) KafkaSinkOption {
	return func(c *kafkaSinkConfig) { c.serializer = s }
}

// applyDefaults fills in zero-value config fields with sensible defaults.
func (c *kafkaSinkConfig) applyDefaults() {
	if c.batchSize == 0 {
		c.batchSize = 100
	}
	if c.batchTimeout == 0 {
		c.batchTimeout = time.Second
	}
	if !c.acksSet {
		c.acks = AcksLeader
	}
}
