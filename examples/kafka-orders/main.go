package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"mailer"
	"mailer/sink"
	"mailer/source"
	"mailer/types"
	"mailer/window"
)

// Order is the JSON-encoded message format on the input Kafka topic.
type Order struct {
	OrderID  string `json:"order_id"`
	Customer string `json:"customer"`
	Amount   uint64 `json:"amount"`
}

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	inputTopic := getenv("KAFKA_INPUT_TOPIC", "orders")
	outputTopic := getenv("KAFKA_OUTPUT_TOPIC", "order-summary")
	groupID := getenv("KAFKA_GROUP_ID", "order-processor")
	windowSize := getenvDuration("KAFKA_WINDOW_SIZE", 5*time.Minute)

	env := mailer.NewEnv()

	src := source.NewKafkaSource(
		source.KafkaBrokers(brokers),
		source.KafkaTopic(inputTopic),
		source.KafkaGroupID(groupID),
		source.KafkaStartFrom(source.OffsetEarliest),
		source.KafkaWithWatermarks(1*time.Second),
		source.KafkaDeserialize(source.NewJSONDeserializer[Order]()),
	)

	kafkaSink := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{brokers},
		Topic:   outputTopic,
	})

	env.
		FromSource(src).
		Map(parseOrder).
		KeyBy(func(r types.Record) []byte { return r.Key }).
		Window(window.NewTumbling(windowSize)).
		Reduce(sumAmount).
		Map(formatResult).
		ToSink(kafkaSink)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := env.Execute(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "pipeline error: %v\n", err)
		os.Exit(1)
	}
}

// parseOrder extracts the deserialized Order from record.Parsed and stores
// the amount as big-endian uint64 in Value, with Customer as Key.
// Invalid JSON is already dropped by the deserializer in the source.
func parseOrder(r types.Record) types.Record {
	o, ok := r.Parsed.(*Order)
	if !ok || o == nil {
		return types.Record{Key: nil, Value: nil, Timestamp: r.Timestamp, Offset: r.Offset}
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, o.Amount)
	return types.Record{
		Key:       []byte(o.Customer),
		Value:     buf,
		Timestamp: r.Timestamp,
		Offset:    r.Offset,
	}
}

// sumAmount sums order amounts per (customer, window).
func sumAmount(accum []byte, curr types.Record) []byte {
	total := uint64(0)
	if accum != nil {
		total = binary.BigEndian.Uint64(accum)
	}
	amount := binary.BigEndian.Uint64(curr.Value)
	total += amount

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, total)
	return buf
}

// formatResult encodes the reduce output as JSON for the output topic.
func formatResult(r types.Record) types.Record {
	total := binary.BigEndian.Uint64(r.Value)
	start := string(r.Headers["window_start"])
	end := string(r.Headers["window_end"])

	out, _ := json.Marshal(map[string]any{
		"customer":     string(r.Key),
		"total":        total,
		"window_start": start,
		"window_end":   end,
	})

	return types.Record{
		Key:       r.Key,
		Value:     out,
		Timestamp: r.Timestamp,
		Offset:    r.Offset,
		Headers:   r.Headers,
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getenvDuration reads a duration from an env var. Format: plain integer seconds
// (e.g. "10" = 10s) or a Go duration string (e.g. "5m", "500ms"). Returns fallback if unset/invalid.
func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second
	}
	fmt.Fprintf(os.Stderr, "invalid %s=%q, using fallback %s\n", key, v, fallback)
	return fallback
}
