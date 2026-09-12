package source

import (
	"sync"

	"github.com/segmentio/kafka-go"
)

// readerSupervisor owns the single kafka.Reader used in consumer-group (or
// single-topic) mode. Parallel per-partition readers are owned by the
// partitionManager instead, which needs a dynamic reader set.
type readerSupervisor struct {
	mu           sync.Mutex
	cfg          kafkaSourceConfig
	readers      []*kafka.Reader
	partitionIDs []int
}

// newSerialReaderSupervisor builds the single reader used in consumer-group
// (or single-topic) mode. Partition id is -1 to mark "not a specific
// partition", matching the previous behaviour.
func newSerialReaderSupervisor(cfg kafkaSourceConfig) *readerSupervisor {
	return &readerSupervisor{cfg: cfg, partitionIDs: []int{-1}}
}

func buildSerialReader(cfg kafkaSourceConfig) *kafka.Reader {
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
	return kafka.NewReader(rc)
}

// primary returns the single reader, used by serial-mode fetch, commit and
// drain paths.
func (s *readerSupervisor) primary() *kafka.Reader {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.readers) == 0 {
		s.readers = []*kafka.Reader{buildSerialReader(s.cfg)}
	}
	return s.readers[0]
}

// closeAll closes every reader. Safe to call once during shutdown.
func (s *readerSupervisor) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.readers {
		r.Close()
	}
}

// buildPartitionReader constructs a reader pinned to a single partition, used
// by the partitionManager for parallel mode. Parallel readers have no consumer
// group (KafkaParallel and KafkaGroupID are mutually exclusive), so
// GroupID/GroupTopics are intentionally left unset.
func buildPartitionReader(cfg kafkaSourceConfig, partition int) *kafka.Reader {
	rc := kafka.ReaderConfig{
		Brokers:     cfg.brokers,
		Topic:       cfg.topic,
		Partition:   partition,
		MinBytes:    cfg.minBytes,
		MaxBytes:    cfg.maxBytes,
		StartOffset: cfg.offsetSpec.toKafka(),
	}
	if cfg.exactlyOnce {
		rc.IsolationLevel = kafka.ReadCommitted
	}
	if cfg.sasl != nil || cfg.tls != nil {
		rc.Dialer = buildDialer(cfg.sasl, cfg.tls)
	}
	return kafka.NewReader(rc)
}
