// Exactly-once pipeline: Kafka → keyed aggregation → transactional Kafka.
//
// Source offsets, operator state, and sink output commit as ONE
// coordinated checkpoint. After any crash, the pipeline restores the
// latest completed checkpoint and replays — committed output is never
// duplicated, uncommitted output was never visible.
//
// Requires a running Kafka broker (e.g. docker):
//
//	docker run -p 9092:9092 apache/kafka:latest
//	go run ./examples/exactly-once/
//
// IMPORTANT: consumers of the output topic must set
// isolation.level=read_committed, or they will see records from
// aborted transactions. Try:
//
//	kafka-console-consumer --bootstrap-server localhost:9092 \
//	    --topic order-totals --isolation-level read_committed
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

func main() {
	env := mailer.NewEnv().
		// The checkpoint interval is also the output visibility
		// latency: records become readable (read_committed) when
		// their interval's transaction commits.
		WithCheckpointing(5*time.Second, checkpoint.NewFileStorage("/tmp/mailer-eo-checkpoints"))

	// KafkaExactlyOnce(): no eager offset commits — offsets commit
	// with the checkpoint, after the sink transaction.
	src := source.NewKafkaSource(
		source.KafkaBrokers("localhost:9092"),
		source.KafkaTopic("orders"),
		source.KafkaGroupID("order-totals-pipeline"),
		source.KafkaStartFrom(source.OffsetEarliest),
		source.KafkaExactlyOnce(),
	)

	// TxnKafkaSink stages each checkpoint interval's output in a
	// Kafka transaction; the coordinator commits it atomically with
	// the checkpoint. The transactional ID must be stable across
	// restarts and unique per pipeline instance.
	snk := sink.NewTxnKafkaSink(
		sink.TxnKafkaBrokers("localhost:9092"),
		sink.TxnKafkaTopic("order-totals"),
		sink.TxnKafkaTransactionalID("order-totals-pipeline"),
	)

	env.
		FromSource(src).
		KeyBy(func(r types.Record) []byte { return r.Key }, "by-customer").WithPartitions(4).
		Reduce(sumAmounts, "running-total").
		ToSink(snk)

	// Ctrl-C triggers graceful shutdown: a final barrier flows through,
	// the last transaction commits, and the final checkpoint completes.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := env.Execute(ctx); err != nil {
		fmt.Printf("pipeline error: %v\n", err)
		os.Exit(1)
	}
}

// sumAmounts accumulates a per-customer total (8-byte big-endian).
func sumAmounts(accum []byte, curr types.Record) []byte {
	var total uint64
	if accum != nil {
		total = binary.BigEndian.Uint64(accum)
	}
	if len(curr.Value) == 8 {
		total += binary.BigEndian.Uint64(curr.Value)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, total)
	return buf
}
