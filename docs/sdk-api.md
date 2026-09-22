# SDK API

How to wire up a pipeline in Go. See [Core Concepts](concepts.md) first if
you haven't already — this doc assumes you know what a Source, Operator,
Sink, and Job are.

## Minimal pipeline

```go
package main

import (
    "context"
    "time"

    "github.com/ASHUTOSH-SWAIN-GIT/weibo"
    "github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
    "github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
    "github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
    "github.com/ASHUTOSH-SWAIN-GIT/weibo/window"
)

func main() {
    env := weibo.NewEnv()

    kafkaSource := source.NewKafkaSource(
        source.KafkaBrokers("localhost:9092"),
        source.KafkaTopic("orders"),
        source.KafkaGroupID("order-processor"),
        source.KafkaStartFrom(source.OffsetEarliest),
        source.KafkaWithWatermarks(1 * time.Second),
    )

    kafkaSink := sink.NewKafkaSink(
        sink.KafkaSinkBrokers("localhost:9092"),
        sink.KafkaSinkTopic("order-summary"),
    )

    env.
        FromSource(kafkaSource).
        KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(8).
        Window(window.NewTumbling(5 * time.Minute)).
        Reduce(func(accum []byte, curr types.Record) []byte {
            // accum is this key's running aggregate ([]byte, nil on
            // first record); return the updated aggregate.
            return addAmount(accum, curr)
        }).
        ToSink(kafkaSink)

    env.Execute(context.Background())
}
```

## Fluent chain

```go
stream.
    Map(parseOrder).                        // transform 1:1
    Filter(isValidOrder).                   // drop invalid
    KeyBy(customerKey).WithPartitions(8).   // partition by customer, 8 keyed workers
    Window(window.NewTumbling(5 * time.Minute)).
    Reduce(aggregateAmount).                // per-key aggregate ([]byte accumulator)
    ToSink(kafkaSink)
```

## Process (error-aware transform)

`Process` wraps a user function that may fail; the failure policy
decides what happens to the record (drop it, send it to a dead-letter
queue, or fail the pipeline):

```go
stream.Process(func(r types.Record) (types.Record, error) {
    if !isValid(r) {
        return r, fmt.Errorf("invalid order")
    }
    return enrich(r), nil
},
    operator.WithProcessFailurePolicy(operator.ProcFailureDLQ),
    operator.WithProcessDLQ(dlqSink),
)
```

Keyed state (per-key accumulators, window contents, and `ProcessKeyed`
state/timers) is managed by the engine and participates in checkpoints.

## Buffering and parallelism

```go
env := weibo.NewEnv().
    WithBufferSize(2048)          // edge capacity (default 1024)

stream.Map(cpuHeavyTransform).
    WithParallelism(4)            // worker pool for one stateless op
                                  // (order across workers not preserved)
```

See [Architecture — Execution Model](../ARCHITECTURE.md#view-2--runtime-dataflow-inside-one-job)
for what stages, edges, and backpressure mean underneath these calls.

## Declarative workflows (YAML)

Common pipelines can also be defined in YAML/JSON and run without Go
code. The declarative path supports built-in JSON-field filters,
projection/rename/set, key-by-field keyed state, count/sum reduce,
windows, Kafka/slice/generator/file sources, Kafka/Postgres/file/stdout/
blackhole sinks, state, checkpointing, and environment-backed secrets.

```sh
go run ./cmd/weibo-workflow --file examples/workflows/order-totals.yaml
go run ./cmd/weibo-workflow --file examples/workflows/order-totals.yaml --dry-run --describe
```

Secrets use `${VAR}` placeholders in sensitive fields:

```yaml
sink:
  type: postgres
  postgres:
    dsn: ${POSTGRES_DSN}
    table: customer_totals
    mapping:
      customer.id: customer_id
      sum: total_amount
    mode: upsert
    conflictColumns: [customer_id]
```

The runner resolves those placeholders at compile time and does not
include resolved values in summaries, errors, or pipeline descriptions.

A YAML job is also what the control-plane dashboard runs behind the
scenes for SDK-less deployments — see [Dashboard](dashboard.md) and
[Self-hosting](self-hosting.md).

---

Next: [Package Structure](package-structure.md) to find where each of these
lives in the repo.
