package source

import "time"

// kafkaSourceConfig holds the resolved configuration for a KafkaSource.
// It is populated by KafkaSourceOption functions and read by NewKafkaSource.
type kafkaSourceConfig struct {
	brokers     []string
	topic       string
	topics      []string
	groupID     string
	minBytes    int
	maxBytes    int
	offsetSpec  OffsetSpec
	commitBatch int // 0 = commit per message

	// Phase 2 (SASL/TLS) — populated by auth options.
	sasl *SASLConfig
	tls  *TLSConfig

	// Phase 3 (watermarks / deserialize).
	watermarkOutOfOrderness time.Duration
	watermarkInterval       time.Duration
	deserializer            Deserializer
}

// KafkaSourceOption configures a KafkaSource. Pass one or more to
// NewKafkaSource. Brokers and at least one topic are required.
type KafkaSourceOption func(*kafkaSourceConfig)

// KafkaBrokers sets the Kafka bootstrap brokers.
// Required. Example: KafkaBrokers("localhost:9092") or KafkaBrokers("a:9092", "b:9092").
func KafkaBrokers(brokers ...string) KafkaSourceOption {
	return func(c *kafkaSourceConfig) { c.brokers = brokers }
}

// KafkaTopic sets a single topic to consume from.
// Either KafkaTopic or KafkaTopics is required.
func KafkaTopic(topic string) KafkaSourceOption {
	return func(c *kafkaSourceConfig) { c.topic = topic }
}

// KafkaTopics sets multiple topics to consume from (consumer-group mode).
// Takes precedence over KafkaTopic.
func KafkaTopics(topics ...string) KafkaSourceOption {
	return func(c *kafkaSourceConfig) { c.topics = topics }
}

// KafkaGroupID sets the consumer group ID. When set, offsets are committed
// to Kafka and tracked by the broker. When empty, no commits happen.
func KafkaGroupID(id string) KafkaSourceOption {
	return func(c *kafkaSourceConfig) { c.groupID = id }
}

// KafkaStartFrom sets where consumption begins.
// Use OffsetEarliest or OffsetLatest. Defaults to OffsetEarliest.
func KafkaStartFrom(o Offset) KafkaSourceOption {
	return func(c *kafkaSourceConfig) { c.offsetSpec = OffsetSpec{Mode: o} }
}

// KafkaFetchBytes controls the min/max bytes returned in each fetch request.
// Defaults: min=1, max=10MB. Increase max for higher throughput on large messages.
func KafkaFetchBytes(min, max int) KafkaSourceOption {
	return func(c *kafkaSourceConfig) {
		c.minBytes = min
		c.maxBytes = max
	}
}

// KafkaCommitBatch batches offset commits every N messages instead of
// committing after every single message. Pass 0 (default) to commit
// per-message. Higher values improve throughput but risk reprocessing
// on failure within the batch window.
func KafkaCommitBatch(n int) KafkaSourceOption {
	return func(c *kafkaSourceConfig) { c.commitBatch = n }
}

// --- Phase 2 options (forward declarations; bodies in auth.go) ---

// KafkaSASL enables SASL authentication on the Kafka connection.
func KafkaSASL(cfg SASLConfig) KafkaSourceOption {
	return func(c *kafkaSourceConfig) { c.sasl = &cfg }
}

// KafkaTLS enables TLS on the Kafka connection.
func KafkaTLS(cfg TLSConfig) KafkaSourceOption {
	return func(c *kafkaSourceConfig) { c.tls = &cfg }
}

// --- Phase 3 options (forward declarations; bodies in deserialize.go / kafka.go) ---

// KafkaDeserialize sets a Deserializer that runs on every record before it
// is emitted. The deserialized value is stored in Record.Parsed.
// If the deserializer returns an error, the record is dropped.
func KafkaDeserialize(d Deserializer) KafkaSourceOption {
	return func(c *kafkaSourceConfig) { c.deserializer = d }
}

// KafkaWithWatermarks enables automatic watermark injection so that
// downstream windows fire without manually wrapping the source.
// maxOutOfOrderness is the maximum expected delay between events;
// the watermark is set to (maxTimestampSeen - maxOutOfOrderness).
func KafkaWithWatermarks(maxOutOfOrderness time.Duration) KafkaSourceOption {
	return func(c *kafkaSourceConfig) {
		c.watermarkOutOfOrderness = maxOutOfOrderness
		c.watermarkInterval = 500 * time.Millisecond
	}
}

// KafkaWatermarkInterval overrides the default 500ms watermark emission
// interval. Only meaningful when KafkaWithWatermarks is also set.
func KafkaWatermarkInterval(d time.Duration) KafkaSourceOption {
	return func(c *kafkaSourceConfig) { c.watermarkInterval = d }
}

// applyDefaults fills in zero-value config fields with sensible defaults.
func (c *kafkaSourceConfig) applyDefaults() {
	if c.minBytes == 0 {
		c.minBytes = 1
	}
	if c.maxBytes == 0 {
		c.maxBytes = 10 * 1024 * 1024
	}
	if c.offsetSpec.Mode == 0 {
		c.offsetSpec = OffsetSpec{Mode: OffsetEarliest}
	}
	if c.watermarkInterval == 0 {
		c.watermarkInterval = 500 * time.Millisecond
	}
}
