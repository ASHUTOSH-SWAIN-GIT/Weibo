# Core Concepts

The vocabulary used throughout the codebase and the rest of the docs.

## Stream

An unbounded, ordered sequence of records. A stream is created from a Source and flows through a chain of Operators.

```
Source[Kafka] → Map → Filter → Window(5min) → Reduce → Sink[Postgres]
```

## Record

A single data item flowing through a stream. Every record carries:

| Field | Type | Description |
|-------|------|-------------|
| `Key` | `[]byte` | Partition key (used for keyed state and shuffling) |
| `Value` | `[]byte` | The actual data payload |
| `Timestamp` | `time.Time` | Event timestamp (for time-based operations) |
| `Offset` | `int64` | Source offset (for checkpointing and replay) |
| `Partition` | `int` | Source partition (for barrier-aligned offset tracking) |
| `Source` | `string` | Stable source stream identity (Kafka topic) used with partition and offset |
| `Headers` | `map[string][]byte` | Optional metadata headers |

## Source

Where data enters the pipeline. A Source reads from an external system and emits Records into the stream.

| Source | Description |
|--------|-------------|
| `KafkaSource` | Consumes from one or more Kafka topics with consumer group support |
| `GeneratorSource` | Generates synthetic records for testing |
| `SliceSource` | Reads from an in-memory slice (for testing) |

## Sink

Where results leave the pipeline. A Sink receives processed Records and writes them to an external system.

| Sink | Description |
|--------|-------------|
| `KafkaSink` | Produces to a Kafka topic (SASL/TLS, batch writes, serializer); at-least-once |
| `TxnKafkaSink` | Transactional Kafka producer for end-to-end exactly-once (franz-go, per-checkpoint transactions) |
| `PostgresSink` | Batch inserts/upserts into Postgres tables (pgx, retry, full mapper) |
| `StdoutSink` | Prints records to stdout (debugging) |
| `BlackholeSink` | Discards everything (benchmarking) |

## Operator

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

## Keyed Stream

After `KeyBy`, the stream is partitioned by key. Each key gets its own state — this is how you do per-user counters, per-session windows, etc. State is always local to the key, no cross-key coordination needed.

```go
stream.KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(8).
       Reduce(func(accum []byte, curr types.Record) []byte { /* fold */ })
```

## Window

Windows group an unbounded stream into finite chunks based on time. Without windows, aggregations like "count" or "sum" would never complete — the stream never ends.

| Window Type | Behavior | Example |
|-------------|----------|---------|
| **Tumbling** | Fixed-size, non-overlapping | 5-minute tumbling: [0-5), [5-10), [10-15) |
| **Sliding** | Fixed-size, overlapping with slide | 5-min window sliding every 1 min |
| **Session** | Gap-based, variable size | Inactivity gap of 30 seconds |

When a window closes, all records in that window are passed to the window function (Reduce, Process, etc.).

**Windowed aggregation — use `WindowReduce`.** For a per-window aggregate
(sum/count/…), prefer `WindowReduce`, which folds each window's records and
emits **one** result per (key, window) at close:

```go
stream.KeyBy(customerKey).WithPartitions(8).
       WindowReduce(window.NewTumbling(5*time.Minute), sumAmount)
```

`Window(...).Reduce(...)` (two separate operators) still works but has
*incremental* semantics: the window emits every buffered record and the
streaming `Reduce` folds them, so you get several partial rows per window (the
last is the final total). Reduce evicts closed window accumulators once the
watermark proves the window plus any configured allowed lateness can no longer
receive records.

## Watermark

A watermark is a timestamp that says "no records with timestamp < X will arrive after this point." Watermarks are how Weibo decides when a window is complete. If a record arrives after the watermark has passed its timestamp, it's **late** — and can be dropped or handled separately.

```
Records:    e1(2)  e2(5)  e3(8)  ---watermark(6)---  e4(7)  e5(10)
                                            ↑
                                    e3 is on-time
                                    e4 is late (7 < watermark 6)
                                    e5 is on-time
```

Watermarks are generated by the Source. For Kafka, a simple strategy is:
`watermark = max_timestamp_seen - max_out_of_orderness`. Kafka watermarks are
global across the consumed stream: an idle partition does not hold back active
partitions, and source completion emits a final max watermark so pending windows
close. Window operators can additionally be configured with allowed lateness;
records older than `current_watermark - allowed_lateness` are dropped or sent to
a late-record side output.

## State

State is storage that persists across records (in RAM or on disk, depending on the backend). It's what makes a stream processor stateful.

| State Type | Use Case | Example |
|------------|----------|---------|
| **ValueState** | Single value per key | Current user score, Reduce accumulator |
| **ListState** | Ordered list per key | Recent login timestamps |

State is stored in a **State Backend**, chosen per pipeline via `WithStateBackend`:

```go
env.WithStateBackend(state.InMemory())          // default: RAM, serialized into checkpoints
env.WithStateBackend(state.Pebble("/var/lib/weibo/state")) // durable, disk-backed (LSM)
```

Each stateful operator instance (per keyed worker) gets its own isolated backend. With Pebble, checkpoints are **native**: the operator hard-links its LSM files at the barrier instead of serializing state, so checkpoint cost scales with *changed* data, not total state, and the checkpoint file stays small.

Measured at 5M keys (16-byte values): Pebble uses ~0.7 MB of heap vs 579 MB in-memory, checkpoints in ~75 ms vs ~3.3 s, and restores in ~58 ms vs ~2.9 s — at the cost of ~5.5 µs lookups (vs 0.3 µs) and ~25% pipeline throughput. Below ~100k keys the in-memory backend wins on everything except durability. Full numbers: `go test -bench . -benchtime=1x ./bench/`.

## Checkpoint

A checkpoint is a consistent snapshot of:
1. The current offset in each source partition
2. The state of all operators

If the pipeline crashes, it restarts from the last successful checkpoint: sources rewind to the saved offsets, state is restored, and processing continues. Operator state is always exactly-once. Output is **end-to-end exactly-once with `TxnKafkaSink`** (each checkpoint interval's output commits in a Kafka transaction, atomically with the checkpoint) and at-least-once with all other sinks — see [Delivery Guarantees](design-decisions.md#delivery-guarantees).

Checkpointing is based on the Chandy-Lamport algorithm (barriers flow through the stream, operators snapshot state when they see a barrier).

```
Source ──[record][record][barrier]──→ Map ──[barrier]──→ Sink
                                  ↓                    ↓
                            snapshot state        snapshot state
                            snapshot offset       ack checkpoint
```

See [checkpoints-and-api.md](checkpoints-and-api.md) for the on-disk schema and compatibility guarantees.

## Job

A Job is a complete pipeline definition: Source → Operator(s) → Sink. You submit a Job to the runtime, and it starts consuming, processing, and producing.

---

Next: [SDK API](sdk-api.md) for how to wire these concepts together in Go, or [Architecture](../ARCHITECTURE.md) for how the engine executes them.
