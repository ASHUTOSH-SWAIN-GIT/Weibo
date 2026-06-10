package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"mailer"
	"mailer/sink"
	"mailer/source"
	"mailer/types"
	"mailer/watermark"
	"mailer/window"
)

func main() {
	env := mailer.NewEnv()

	// Simulate order events arriving over time.
	// Each order has a timestamp (event time) and an amount.
	// Key: customer ID, Value: order amount as big-endian uint64.
	now := time.Now()
	records := []types.Record{
		newOrder("alice", 100, now.Add(0*time.Second)),      // t=0s
		newOrder("bob", 200, now.Add(1*time.Second)),        // t=1s
		newOrder("alice", 150, now.Add(2*time.Second)),      // t=2s
		newOrder("charlie", 300, now.Add(3*time.Second)),     // t=3s
		newOrder("alice", 50, now.Add(4*time.Second)),       // t=4s
		newOrder("bob", 100, now.Add(5*time.Second)),        // t=5s (triggers window [0-5s))
		newOrder("alice", 200, now.Add(10*time.Second)),     // t=10s (triggers window [5-10s))
		newOrder("bob", 50, now.Add(11*time.Second)),        // t=11s
	}

	src := source.NewGeneratorSource(records)

	// Wrap the source with watermark generation.
	// Allowed lateness = 1 second means windows close 1 second after their end time.
	wmSrc := source.NewWatermarkSource(
		src,
		watermark.NewBoundedOutOfOrderness(1*time.Second),
		500*time.Millisecond,
	)

	// Pipeline:
	//   Source → KeyBy(customer) → Window(5s tumbling) → Reduce(sum) → Sink(stdout)
	env.
		FromSource(wmSrc).
		KeyBy(func(r types.Record) []byte { return r.Key }).
		Window(window.NewTumbling(5 * time.Second)).
		Reduce(sumAmount).
		Map(func(r types.Record) types.Record {
			count := binary.BigEndian.Uint64(r.Value)
			start := r.Headers["window_start"]
			end := r.Headers["window_end"]
			fmt.Printf("customer=%s total=$%d window=[%s, %s)\n",
				string(r.Key), count, string(start), string(end))
			return r
		}).
		ToSink(sink.NewBlackholeSink())

	if err := env.Execute(context.Background()); err != nil {
		fmt.Printf("pipeline error: %v\n", err)
	}
}

func newOrder(customer string, amount uint64, ts time.Time) types.Record {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, amount)
	return types.Record{
		Key:       []byte(customer),
		Value:     buf,
		Timestamp: ts,
		Offset:    0,
	}
}

// sumAmount sums order amounts per key per window.
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