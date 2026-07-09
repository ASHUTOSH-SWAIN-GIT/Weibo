package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	mailer "github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/dashboard"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/window"
)

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
	windowSize := getenvDuration("KAFKA_WINDOW_SIZE", 5*time.Second)
	dashAddr := getenv("DASHBOARD_ADDR", ":18080")

	env := mailer.NewEnv()

	src := source.NewKafkaSource(
		source.KafkaBrokers(brokers),
		source.KafkaTopic(inputTopic),
		source.KafkaGroupID(groupID),
		source.KafkaStartFrom(source.OffsetEarliest),
		source.KafkaWithWatermarks(1*time.Second),
		source.KafkaDeserialize(source.NewJSONDeserializer[Order]()),
	)

	kafkaSink := sink.NewKafkaSink(
		sink.KafkaSinkBrokers(brokers),
		sink.KafkaSinkTopic(outputTopic),
	)

	env.
		FromSource(src).
		Map(parseOrder, "parse-order").
		KeyBy(func(r types.Record) []byte { return r.Key }, "by-customer").
		Window(window.NewTumbling(windowSize), "tumbling-window").
		Reduce(sumAmount, "sum-amount").
		Map(formatResult, "format-json").
		ToSink(kafkaSink)

	dash := dashboard.NewServer(env, dashAddr)
	go dash.Start()
	dash.SetRunning(true)

	fmt.Printf("dashboard: http://localhost%s\n", dashAddr)
	fmt.Printf("pipeline: %s -> operators -> %s\n", inputTopic, outputTopic)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := env.Execute(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "pipeline error: %v\n", err)
		}
	}()

	<-ctx.Done()
	dash.SetRunning(false)
	time.Sleep(time.Second)
}

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

func sumAmount(accum []byte, curr types.Record) []byte {
	total := uint64(0)
	if accum != nil {
		total = binary.BigEndian.Uint64(accum)
	}
	total += binary.BigEndian.Uint64(curr.Value)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, total)
	return buf
}

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

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	return fallback
}