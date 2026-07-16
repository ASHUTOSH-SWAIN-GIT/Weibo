# Mailer

A stream processing engine in Go, inspired by Apache Flink.

Mailer reads unbounded data streams from sources like Apache Kafka, applies real-time transformations (map, filter, reduce, window), and writes results to sinks. It supports stateful processing, windowed aggregations, durable disk-backed state, and fault tolerance via checkpointing — up to end-to-end exactly-once for Kafka-to-Kafka pipelines.

---

## Why?

Apache Flink is powerful but Java-heavy and complex. There's no idiomatic Go stream processing engine that lets you:

- Consume Kafka topics as unbounded streams
- Apply stateful transformations with exactly-once semantics
- Window and aggregate events by time
- Recover from failures without data loss

Mailer fills this gap. It's a lightweight, embeddable Go library — not a cluster runtime. You import it, define your pipeline, and run it.

---

## Core Concepts

### Stream

An unbounded, ordered sequence of records. A stream is created from a Source and flows through a chain of Operators.

```
Source[Kafka] → Map → Filter → Window(5min) → Reduce → Sink[Postgres]
```

### Record

A single data item flowing through a stream. Every record carries:

| Field | Type | Description |
|-------|------|-------------|
| `Key` | `[]byte` | Partition key (used for keyed state and shuffling) |
| `Value` | `[]byte` | The actual data payload |
| `Timestamp` | `time.Time` | Event timestamp (for time-based operations) |
| `Offset` | `int64` | Source offset (for checkpointing and replay) |
| `Partition` | `int` | Source partition (for barrier-aligned offset tracking) |
| `Headers` | `map[string][]byte` | Optional metadata headers |

### Source

Where data enters the pipeline. A Source reads from an external system and emits Records into the stream.

| Source | Description |
|--------|-------------|
| `KafkaSource` | Consumes from one or more Kafka topics with consumer group support |
| `GeneratorSource` | Generates synthetic records for testing |
| `SliceSource` | Reads from an in-memory slice (for testing) |

### Sink

Where results leave the pipeline. A Sink receives processed Records and writes them to an external system.

| Sink | Description |
|--------|-------------|
| `KafkaSink` | Produces to a Kafka topic (SASL/TLS, batch writes, serializer); at-least-once |
| `TxnKafkaSink` | Transactional Kafka producer for end-to-end exactly-once (franz-go, per-checkpoint transactions) |
| `PostgresSink` | Batch inserts/upserts into Postgres tables (pgx, retry, full mapper) |
| `StdoutSink` | Prints records to stdout (debugging) |
| `BlackholeSink` | Discards everything (benchmarking) |

### Operator

A transformation applied to a stream. Each operator takes one or more input streams and produces one or more output streams.

| Operator | Signature | Description |
|----------|-----------|-------------|
| `Map` | `func(Record) Record` | Transform each record 1:1 |
| `FlatMap` | `func(Record) []Record` | Transform each record 1:many |
| `Filter` | `func(Record) bool` | Keep or drop records |
| `KeyBy` | `func(Record) []byte` | Partition stream by key (enables keyed state + parallelism) |
| `Reduce` | `func(accum []byte, curr Record) []byte` | Per-key aggregate: fold each record into a byte accumulator |
| `Window` | `WindowAssigner` | Group records into time-based windows |
| `Process` | `func(Record) (Record, error)` | Error-aware transform with failure policy (drop / DLQ / fail) |

### Keyed Stream

After `KeyBy`, the stream is partitioned by key. Each key gets its own state — this is how you do per-user counters, per-session windows, etc. State is always local to the key, no cross-key coordination needed.

```
stream.KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(8).
       Reduce(func(accum []byte, curr types.Record) []byte { /* fold */ })
```

### Window

Windows group an unbounded stream into finite chunks based on time. Without windows, aggregations like "count" or "sum" would never complete — the stream never ends.

| Window Type | Behavior | Example |
|-------------|----------|---------|
| **Tumbling** | Fixed-size, non-overlapping | 5-minute tumbling: [0-5), [5-10), [10-15) |
| **Sliding** | Fixed-size, overlapping with slide | 5-min window sliding every 1 min |
| **Session** | Gap-based, variable size | Inactivity gap of 30 seconds |

When a window closes, all records in that window are passed to the window function (Reduce, Process, etc.).

### Watermark

A watermark is a timestamp that says "no records with timestamp < X will arrive after this point." Watermarks are how Mailer decides when a window is complete. If a record arrives after the watermark has passed its timestamp, it's **late** — and can be dropped or handled separately.

```
Records:    e1(2)  e2(5)  e3(8)  ---watermark(6)---  e4(7)  e5(10)
                                            ↑
                                    e3 is on-time
                                    e4 is late (7 < watermark 6)
                                    e5 is on-time
```

Watermarks are generated by the Source. For Kafka, a simple strategy is: `watermark = max_timestamp_seen - allowed_lateness`.

### State

State is storage that persists across records (in RAM or on disk, depending on the backend). It's what makes a stream processor stateful.

| State Type | Use Case | Example |
|------------|----------|---------|
| **ValueState** | Single value per key | Current user score, Reduce accumulator |
| **ListState** | Ordered list per key | Recent login timestamps |

State is stored in a **State Backend**, chosen per pipeline via `WithStateBackend`:

```go
env.WithStateBackend(state.InMemory())          // default: RAM, serialized into checkpoints
env.WithStateBackend(state.Pebble("/var/lib/mailer/state")) // durable, disk-backed (LSM)
```

Each stateful operator instance (per keyed worker) gets its own isolated backend. With Pebble, checkpoints are **native**: the operator hard-links its LSM files at the barrier instead of serializing state, so checkpoint cost scales with *changed* data, not total state, and the checkpoint file stays small.

Measured at 5M keys (16-byte values): Pebble uses ~0.7 MB of heap vs 579 MB in-memory, checkpoints in ~75 ms vs ~3.3 s, and restores in ~58 ms vs ~2.9 s — at the cost of ~5.5 µs lookups (vs 0.3 µs) and ~25% pipeline throughput. Below ~100k keys the in-memory backend wins on everything except durability. Full numbers: `go test -bench . -benchtime=1x ./bench/`.

### Checkpoint

A checkpoint is a consistent snapshot of:
1. The current offset in each source partition
2. The state of all operators

If the pipeline crashes, it restarts from the last successful checkpoint: sources rewind to the saved offsets, state is restored, and processing continues. Operator state is always exactly-once. Output is **end-to-end exactly-once with `TxnKafkaSink`** (each checkpoint interval's output commits in a Kafka transaction, atomically with the checkpoint) and at-least-once with all other sinks — see Delivery Guarantees below.

Checkpointing is based on the Chandy-Lamport algorithm (barriers flow through the stream, operators snapshot state when they see a barrier).

```
Source ──[record][record][barrier]──→ Map ──[barrier]──→ Sink
                                  ↓                    ↓
                            snapshot state        snapshot state
                            snapshot offset       ack checkpoint
```

### Job

A Job is a complete pipeline definition: Source → Operator(s) → Sink. You submit a Job to the runtime, and it starts consuming, processing, and producing.

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                      Job                              │
│                                                      │
│  Source[Kafka]  →  Operator Chain  →  Sink[Postgres] │
│       │                  │                            │
│       │           ┌──────┴──────┐                     │
│       │           │   State     │                     │
│       │           │  Backend    │                     │
│       │           └──────┬──────┘                     │
│       │                  │                            │
│  ┌────┴──────────────────┴────┐                       │
│  │     Checkpoint Manager     │                       │
│  └────────────────────────────┘                       │
└──────────────────────────────────────────────────────┘
```

**Single-process, multi-goroutine.** No cluster runtime. Each source partition is consumed by a separate goroutine. Keyed state is partitioned by key across per-worker state backends (in-memory or Pebble). Checkpointing uses barrier alignment.

### Execution Model: Stages and Edges

At `Execute()`, the planner groups the operator chain into **execution
stages**. Inside a stage, operators run as direct function calls — no
channels, no goroutine hops. Bounded channels (**edges**, default
capacity 1024) exist only *between* stages:

```
[Source] →edge→ [Map→Filter] →edge→ [KeyBy: Window→Reduce ×N workers] →edge→ [Sink]
```

- Consecutive stateless operators (Map, Filter, FlatMap, Process) with
  the same parallelism share one stage.
- `KeyBy` starts a keyed stage: a router hash-dispatches records to N
  stateful workers (same key → same worker), each with cloned
  operators and isolated state.
- Checkpoint barriers and watermarks are broadcast to every worker of
  a parallel stage and re-aligned at its exit, so snapshots stay
  consistent at any parallelism.

**Backpressure** falls out of the bounded edges: sending to a full
edge blocks. A slow sink fills its input edge, which blocks the stage
before it, and so on back to the source — which simply stops fetching
from Kafka. Bounded memory, zero drops, no tuning required. Tune the
buffer/latency trade-off with:

```go
env := mailer.NewEnv().
    WithBufferSize(2048)          // edge capacity (default 1024)

stream.Map(cpuHeavyTransform).
    WithParallelism(4)            // worker pool for one stateless op
                                  // (order across workers not preserved)
```

**Shutdown is two-phase:** cancelling the context stops the source;
everything downstream drains through cascading channel closes (a final
checkpoint barrier rides the drain, so state is saved). Only if the
drain exceeds `WithShutdownTimeout` (default 30s) are blocked stages
forcibly aborted.

**Observability:** every edge and stage exports Prometheus metrics —
`mailer_edge_queue_size` / `_capacity` (an edge pinned at capacity
identifies the bottleneck stage right after it),
`mailer_stage_records_in_total` / `_out_total`,
`mailer_stage_send_block_seconds_total` (time spent blocked on
backpressure), `mailer_stage_workers`, and `mailer_stage_errors_total`,
alongside the existing per-operator and per-worker metrics.

---

## SDK API

```go
package main

import (
    "context"
    "time"

    "github.com/ASHUTOSH-SWAIN-GIT/mailer"
    "github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
    "github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
    "github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
    "github.com/ASHUTOSH-SWAIN-GIT/mailer/window"
)

func main() {
    env := mailer.NewEnv()

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

### Fluent Chain

```go
stream.
    Map(parseOrder).                        // transform 1:1
    Filter(isValidOrder).                   // drop invalid
    KeyBy(customerKey).WithPartitions(8).   // partition by customer, 8 keyed workers
    Window(window.NewTumbling(5 * time.Minute)).
    Reduce(aggregateAmount).                // per-key aggregate ([]byte accumulator)
    ToSink(kafkaSink)
```

### Process (error-aware transform)

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

Keyed state (per-key accumulators, window contents) is managed by the
engine inside `Reduce` and `Window` — there is no user-facing state
API yet; a Flink-style stateful ProcessFunction with direct state
access is on the roadmap.

## Declarative Workflows

Common pipelines can also be defined in YAML/JSON and run without Go
code. The declarative path supports built-in JSON-field filters,
projection/rename/set, key-by-field, count/sum reduce, windows, sources,
sinks, state, checkpointing, and environment-backed secrets.

```sh
go run ./cmd/mailer-workflow --file examples/workflows/order-totals.yaml
go run ./cmd/mailer-workflow --file examples/workflows/order-totals.yaml --dry-run --describe
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

---

## Package Structure

```
mailer/
├── mailer.go              # StreamExecutionEnv, NewEnv(), Execute(), checkpointing glue
├── stream.go              # Stream type (fluent chain builder)
├── metadata.go            # Pipeline description for the dashboard
├── types/
│   └── record.go          # Record type (data / watermark / barrier)
├── pipeline/              # Stage-based execution engine
│   ├── stage.go           # Stage interface, SourceStage, SinkStage, ChannelStage
│   ├── edge.go            # Bounded edges + blocking sends (backpressure)
│   ├── planner.go         # Groups operators into execution stages
│   ├── stateless_stage.go # Chained stateless operators, worker pool
│   ├── keyed_stage.go     # Router → keyed workers → aligning merger
│   ├── markers.go         # Barrier/watermark broadcast + alignment
│   └── metrics.go         # Per-stage / per-edge instrumentation
├── operator/
│   ├── operator.go        # Operator + optional interfaces: SingleProcessor, Cloneable,
│   │                      #   Parallel, StateConfigurable, Snapshotable, BarrierSnapshotter
│   ├── map.go             # Map (1:1)
│   ├── flatmap.go         # FlatMap (1:N)
│   ├── filter.go          # Filter
│   ├── process.go         # Process (error-aware, failure policy + DLQ)
│   ├── keyby.go           # KeyBy router (hash partitioning)
│   ├── reduce.go          # Reduce (keyed accumulator)
│   └── window.go          # Window operator (buffers, fires on watermark)
├── window/                # Window assigners: tumbling, sliding, session
├── watermark/             # Watermark strategies (bounded out-of-orderness)
├── state/                 # StateBackend interface; in-memory + Pebble (LSM) backends,
│                          #   ValueState/ListState, native hard-link checkpoints
├── source/                # Source interface; Kafka (multi-partition, SASL/TLS,
│                          #   deserializers, watermarks), slice/generator for tests
├── sink/                  # Sink interface; Kafka, Postgres (batch+retry),
│                          #   stdout, blackhole; serializers, failure policies, DLQ
├── checkpoint/            # Barrier-based checkpoint data, file storage (fsync,
│                          #   status pointers), two-phase exactly-once coordinator
├── auth/                  # SASL / TLS config shared by Kafka source & sink
├── observability/
│   ├── metrics/           # Prometheus registry: pipeline, operator, stage, edge
│   └── dashboard/         # Built-in web dashboard
├── bench/                 # State-backend scaling benchmarks (memory vs Pebble)
├── examples/
│   ├── wordcount/         # FlatMap → KeyBy → Reduce
│   ├── windowing/         # Event time, watermarks, tumbling windows
│   ├── backpressure/      # Fast source + slow sink, bounded edges
│   ├── exactly-once/      # Kafka → keyed reduce → transactional Kafka
│   ├── kafka-orders/      # Kafka → Window → Kafka
│   ├── kafka-dashboard/   # Kafka pipeline with the live dashboard
│   ├── pg-orders/         # Kafka → Postgres
│   └── dashboard-demo/    # Metrics + dashboard
└── test/unit_tests/       # Integration tests: checkpoint recovery, exactly-once
                           #   crash sweep, backpressure, shutdown, durable state,
                           #   per-package unit tests
```

---

## Implementation Status

All originally planned phases are implemented:

- ✅ **Core pipeline** — Record, fluent Stream API, Map/Filter/FlatMap/Process, sources and sinks.
- ✅ **Stateful processing** — per-key state, Reduce, `Process` with failure policies + DLQ.
- ✅ **Windowing & watermarks** — tumbling/sliding/session windows, bounded out-of-orderness watermarks, late-record dropping.
- ✅ **Kafka & Postgres connectors** — multi-partition Kafka source (consumer groups, SASL/TLS, deserializers, per-partition offset checkpointing), Kafka + Postgres sinks with batching, retries, serializers, and Postgres upserts.
- ✅ **Checkpointing & recovery** — barrier-based snapshots, file storage, restore of operator state + per-partition source offsets on restart.
- ✅ **Keyed parallelism** — `WithPartitions(n)`: router → N stateful workers with cloned operators and isolated state; barriers/watermarks broadcast and re-aligned so checkpoints stay consistent.
- ✅ **Stage-based execution & backpressure** — operators grouped into stages, direct function-call chaining inside a stage, bounded edges between stages, `WithParallelism(n)` for stateless workers, two-phase graceful shutdown.
- ✅ **Observability** — Prometheus metrics (pipeline, operator, worker, stage, edge) and a built-in dashboard.
- ✅ **End-to-end exactly-once (Kafka → Kafka)** — coordinated two-phase checkpoints: barrier-aligned source offsets, synchronous operator snapshots at barrier passage, transactional sink (`TxnKafkaSink` on franz-go) with per-checkpoint transaction markers for crash recovery. See Delivery Guarantees.
- ✅ **Durable state backend (Pebble)** — per-worker disk-backed LSM state selected via `WithStateBackend`; Reduce accumulators and Window records/watermark both live in the backend, so state is bounded by disk, not RAM. Native hard-link checkpoints make checkpoint cost scale with changed data, not total state. See State above.

Up next (roughly in order): allowed-lateness + side outputs for late data, multi-stream joins, typed `Stream[T]` API.

---

## Key Design Decisions

### Why single-process, not distributed?

Flink runs as a JobManager + TaskManager cluster. Mailer runs as a single Go process with goroutines. This makes it simple, embeddable, and easy to reason about. If you need multi-node parallelism, run multiple Mailer instances with different consumer group IDs (Kafka handles partitioning).

### Why barrier-based checkpointing?

The Chandy-Lamport approach (barriers flow through the dataflow graph, operators snapshot on barrier arrival) is well-proven in Flink. Barriers are special Records that flow in-band with data; stateful operators snapshot synchronously as the barrier passes through them, and at parallel stages barriers are broadcast to every worker and strictly re-aligned at the stage exit — no record can overtake a pending barrier. A snapshot is therefore always a consistent cut of the stream.

## Delivery Guarantees

| Configuration | Guarantee |
|---|---|
| `KafkaSource(KafkaExactlyOnce()) → … → TxnKafkaSink` + `WithCheckpointing` | **End-to-end exactly-once.** Source offsets, operator state, and sink output commit as one coordinated checkpoint (two-phase commit; the checkpoint file is the transaction log; a per-checkpoint transaction marker resolves crashes between sink commit and checkpoint completion). |
| Any source → any plain sink, `WithCheckpointing` on | Exactly-once **state**, at-least-once **output**: replay after recovery re-emits records the sink already wrote. |
| No checkpointing | At-most-once across restarts (processing restarts from the source's configured start offset). |

Requirements for the exactly-once configuration:

- **Consumers of the output topic must use `isolation.level=read_committed`** — otherwise they observe records from aborted transactions and all guarantees are void.
- The `TxnKafkaTransactionalID` must be stable across restarts and unique per pipeline instance (a second instance with the same ID fences the first).
- The marker topic (`<topic>.checkpoints` by default) must not be deleted — it is how recovery proves whether an unconfirmed transaction committed.
- Output visibility latency equals the checkpoint interval: records become readable when their interval's transaction commits.
- A checkpoint failure fails the pipeline (the aborted transaction's output must be replayed); restart recovers from the last completed checkpoint.

### Why not just use Kafka Streams?

Kafka Streams is Java-only. Mailer gives Go developers a native, embeddable stream processing library with similar semantics — without the JVM.

### Why segmentio/kafka-go over confluent-kafka-go?

`segmentio/kafka-go` is pure Go (no CGO), which simplifies builds and cross-compilation. If performance becomes critical, we can add a `confluent-kafka-go` source as an alternative.

---

## Comparison with Flink

| Feature | Apache Flink | Mailer |
|---------|-------------|--------|
| Language | Java | Go |
| Deployment | Cluster (JobManager + TaskManagers) | Single process, embeddable |
| API | DataStream API, Table API, SQL | DataStream API (fluent chain) |
| State Backends | RocksDB, Memory | Memory + Pebble (LSM), native hard-link checkpoints |
| Checkpointing | Barrier-based, aligned/unaligned | Barrier-based, aligned |
| Windowing | Tumbling, Sliding, Session, Global | Tumbling, Sliding, Session |
| Kafka Connector | Built-in, mature | Built-in (segmentio/kafka-go) |
| Exactly-once | End-to-end (with transactional sinks) | End-to-end (Kafka→Kafka via TxnKafkaSink); other sinks at-least-once |
| Backpressure | Credit-based network flow control | Bounded edges between stages |
| SQL | Yes | No (not planned for v1) |
| CEP (Complex Event Processing) | Yes | No (future) |

---

## Status

Actively developed. The engine (stage-based execution, keyed parallelism, checkpoint/recovery, backpressure, metrics) is implemented and covered by unit + integration tests, including crash-and-recovery and shutdown-under-load scenarios. See **Implementation Status** above for what's done and what's next.
