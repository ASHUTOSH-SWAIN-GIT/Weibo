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

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
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
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		dsn = "postgres://testuser:testpass@localhost:5433/mailertest?sslmode=disable"
	}

	records := make([]types.Record, 8)
	orders := []Order{
		{"o1", "alice", 100},
		{"o2", "bob", 200},
		{"o3", "alice", 150},
		{"o4", "charlie", 300},
		{"o5", "alice", 50},
		{"o6", "bob", 100},
		{"o7", "alice", 200},
		{"o8", "bob", 50},
	}
	for i, o := range orders {
		val, _ := json.Marshal(o)
		records[i] = types.Record{
			Key:       []byte(o.Customer),
			Value:     val,
			Timestamp: time.Now(),
			Offset:    int64(i),
		}
	}

	env := mailer.NewEnv()

	src := source.NewSliceSource(records)

	pgSink := sink.NewPostgresSink(
		sink.PostgresDSN(dsn),
		sink.PostgresMapper(func(r types.Record) (string, []string, []any) {
			total := binary.BigEndian.Uint64(r.Value)
			return "orders",
				[]string{"order_id", "customer", "amount"},
				[]any{fmt.Sprintf("agg-%s", string(r.Key)), string(r.Key), int64(total)}
		}),
		sink.PostgresBatchSize(8),
	)

	env.
		FromSource(src).
		Map(parseOrder, "parse-order").
		KeyBy(func(r types.Record) []byte { return r.Key }, "by-customer").
		Window(window.NewTumbling(5*time.Minute), "5min-window").
		Reduce(sumAmount, "sum-amount").
		ToSink(pgSink)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := env.Execute(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "pipeline error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("pipeline completed, checking table...")
}

func parseOrder(r types.Record) types.Record {
	var o Order
	if err := json.Unmarshal(r.Value, &o); err != nil {
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
