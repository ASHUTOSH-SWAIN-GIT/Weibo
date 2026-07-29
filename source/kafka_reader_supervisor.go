package source

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

// readerSupervisor owns the kafka.Reader instances for a KafkaSource and the
// goroutine orchestration for running them.
//
// It handles two shapes:
//   - a single reader (consumer-group / serial mode), partition id -1
//   - one reader per discovered partition (parallel mode)
//
// Reader creation, shared cancellation, fatal-error propagation and clean
// shutdown all live here; the per-message loop body is supplied by the caller.
type readerSupervisor struct {
	readers      []*kafka.Reader
	partitionIDs []int
}

// newSerialReaderSupervisor builds the single reader used in consumer-group
// (or single-topic) mode. Partition id is -1 to mark "not a specific
// partition", matching the previous behaviour.
func newSerialReaderSupervisor(cfg kafkaSourceConfig) *readerSupervisor {
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
	return &readerSupervisor{
		readers:      []*kafka.Reader{kafka.NewReader(rc)},
		partitionIDs: []int{-1},
	}
}

// newParallelReaderSupervisor builds one reader per partition id. Parallel
// readers have no consumer group (KafkaParallel and KafkaGroupID are mutually
// exclusive), so GroupID/GroupTopics are intentionally left unset.
func newParallelReaderSupervisor(cfg kafkaSourceConfig, partitionIDs []int) *readerSupervisor {
	s := &readerSupervisor{}
	for _, id := range partitionIDs {
		rc := kafka.ReaderConfig{
			Brokers:     cfg.brokers,
			Topic:       cfg.topic,
			Partition:   id,
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
		s.readers = append(s.readers, kafka.NewReader(rc))
		s.partitionIDs = append(s.partitionIDs, id)
	}
	return s
}

// primary returns the single/first reader, used by serial-mode fetch, commit
// and drain paths.
func (s *readerSupervisor) primary() *kafka.Reader {
	return s.readers[0]
}

// parallelMode reports whether more than one partition reader is running.
func (s *readerSupervisor) parallelMode() bool {
	return len(s.readers) > 1
}

// readerLoop is the per-reader loop body run by runParallel. idx is the index
// into the readers slice (also the partitionIDs slice).
type readerLoop func(ctx context.Context, idx int, r *kafka.Reader) error

// runParallel starts one goroutine per reader running loop and blocks until
// all return. A child context is derived so cancellation is shared. A loop
// that returns a non-nil error while the context is still live is treated as
// fatal and its (original) error is returned; errors observed after
// cancellation are ignored, exactly as before.
func (s *readerSupervisor) runParallel(ctx context.Context, loop readerLoop) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	perr := make(chan error, len(s.readers))

	for i, r := range s.readers {
		wg.Add(1)
		go func(idx int, reader *kafka.Reader) {
			defer wg.Done()
			if err := loop(ctx, idx, reader); err != nil && ctx.Err() == nil {
				perr <- err
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

// closeAll closes every partition reader. Safe to call once during shutdown.
func (s *readerSupervisor) closeAll() {
	for _, r := range s.readers {
		r.Close()
	}
}
