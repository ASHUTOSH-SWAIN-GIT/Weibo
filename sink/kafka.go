package sink

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"mailer/types"
)

// KafkaSink writes records to a single Kafka topic.
// It implements the Sink interface for use in mailer pipelines.
//
// Records are read from the input channel and written to Kafka in batches
// for efficiency. The sink respects context cancellation.
//
// mailer.Record fields are mapped to kafka.Message as follows:
//   - Key     → Message.Key
//   - Value   → Message.Value
//   - Timestamp → Message.Time
//   - Headers → Message.Headers
type KafkaSink struct {
	Writer *kafka.Writer
}

// KafkaConfig configures a Kafka producer.
type KafkaSinkConfig struct {
	Brokers      []string      // e.g. []string{"localhost:9092"}
	Topic        string        // destination topic
	BatchSize    int           // max messages per batch (default 100)
	BatchTimeout time.Duration // max time to wait before flushing partial batch (default 1s)
}

// NewKafkaSink creates a Sink that writes to a Kafka topic.
func NewKafkaSink(cfg KafkaSinkConfig) *KafkaSink {
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.BatchTimeout == 0 {
		cfg.BatchTimeout = time.Second
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{}, // route by key for partition affinity
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &KafkaSink{Writer: w}
}

// Write reads records from the input channel and writes them to Kafka.
// It uses a streaming batch writer to avoid materializing all records in memory.
//
// On context cancellation, the sink waits up to shutdownTimeout for the
// upstream chain to drain (so windowed/aggregated records can still be
// written) before performing a best-effort flush and returning.
func (k *KafkaSink) Write(ctx context.Context, in <-chan types.Record) error {
	defer k.Writer.Close()

	const shutdownTimeout = 5 * time.Second

	var (
		batch   []kafka.Message
		batchMu sync.Mutex
		wg      sync.WaitGroup
	)

	// flush writes the current batch using the given context.
	// Caller may pass a fresh context (e.g. on shutdown) to avoid
	// the write failing because ctx is already cancelled.
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
			if err := k.Writer.WriteMessages(flushCtx, toWrite...); err != nil {
				fmt.Printf("mailer/sink: kafka write error: %v\n", err)
			}
		}()
	}

	// drain keeps reading from `in` until either:
	//   - the channel closes (clean shutdown), or
	//   - shutdownTimeout elapses (forced shutdown).
	// It batches everything it reads so the subsequent flush writes it.
	drain := func() {
		deadline := time.NewTimer(shutdownTimeout)
		defer deadline.Stop()
		for {
			select {
			case record, ok := <-in:
				if !ok {
					return
				}
				batchMu.Lock()
				batch = append(batch, RecordToKafka(record))
				batchMu.Unlock()
			case <-deadline.C:
				return
			}
		}
	}

	for {
		select {
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

			batchMu.Lock()
			batch = append(batch, RecordToKafka(record))
			full := len(batch) >= 100
			batchMu.Unlock()

			if full {
				flush(ctx)
			}
		}
	}
}

// RecordToKafka converts a mailer.Record to a kafka.Message.
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
