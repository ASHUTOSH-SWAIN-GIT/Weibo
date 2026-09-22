# Design Decisions, Guarantees, and Status

## Implementation Status

All originally planned phases are implemented:

- ✅ **Core pipeline** — Record, fluent Stream API, Map/Filter/FlatMap/Process, sources and sinks.
- ✅ **Stateful processing** — per-key state, Reduce, `Process` with failure policies + DLQ.
- ✅ **Windowing & watermarks** — tumbling/sliding/session windows, bounded out-of-orderness watermarks, allowed lateness, late-record side output hooks, final source-completion firing.
- ✅ **Production connectors** — multi-partition Kafka source (consumer groups, SASL/TLS, deserializers, per-partition offset checkpointing), checkpoint-aware file source, Kafka/Postgres/HTTP/S3/file sinks with batching/retries/serializers where appropriate, and transactional Kafka output.
- ✅ **Checkpointing & recovery** — barrier-based snapshots, file storage, restore of operator state + per-partition source offsets on restart.
- ✅ **Keyed parallelism** — `WithPartitions(n)`: router → N stateful workers with cloned operators and isolated state; barriers/watermarks broadcast and re-aligned so checkpoints stay consistent.
- ✅ **Stage-based execution & backpressure** — operators grouped into stages, direct function-call chaining inside a stage, bounded edges between stages, `WithParallelism(n)` for stateless workers, two-phase graceful shutdown.
- ✅ **Observability** — Prometheus metrics (pipeline, operator, worker, stage, edge) and a built-in dashboard.
- ✅ **End-to-end exactly-once (Kafka → Kafka)** — coordinated two-phase checkpoints: barrier-aligned source offsets, synchronous operator snapshots at barrier passage, transactional sink (`TxnKafkaSink` on franz-go) with per-checkpoint transaction markers for crash recovery. See Delivery Guarantees below.
- ✅ **Durable state backend (Pebble)** — per-worker disk-backed LSM state selected via `WithStateBackend`; Reduce accumulators and Window records/watermark both live in the backend, so state is bounded by disk, not RAM. Native hard-link checkpoints make checkpoint cost scale with changed data, not total state. See [Concepts — State](concepts.md#state).

Up next (roughly in order): multi-stream joins, typed `Stream[T]` API.

## Key Design Decisions

### Why single-process, not distributed?

Flink runs as a JobManager + TaskManager cluster. Weibo runs as a single Go process with goroutines. This makes it simple, embeddable, and easy to reason about. If you need multi-node parallelism, run multiple Weibo instances with different consumer group IDs (Kafka handles partitioning).

### Why barrier-based checkpointing?

The Chandy-Lamport approach (barriers flow through the dataflow graph, operators snapshot on barrier arrival) is well-proven in Flink. Barriers are special Records that flow in-band with data; stateful operators snapshot synchronously as the barrier passes through them, and at parallel stages barriers are broadcast to every worker and strictly re-aligned at the stage exit — no record can overtake a pending barrier. A snapshot is therefore always a consistent cut of the stream.

### Why not just use Kafka Streams?

Kafka Streams is Java-only. Weibo gives Go developers a native, embeddable stream processing library with similar semantics — without the JVM.

### Why segmentio/kafka-go over confluent-kafka-go?

`segmentio/kafka-go` is pure Go (no CGO), which simplifies builds and cross-compilation. If performance becomes critical, we can add a `confluent-kafka-go` source as an alternative.

## Delivery Guarantees

| Configuration | Guarantee |
|---|---|
| `KafkaSource(KafkaExactlyOnce()) → … → TxnKafkaSink` + `WithCheckpointing` | **End-to-end exactly-once.** Source offsets, operator state, and sink output commit as one coordinated checkpoint (two-phase commit; the checkpoint file is the transaction log; a per-checkpoint transaction marker resolves crashes between sink commit and checkpoint completion). |
| Any source → any plain sink, `WithCheckpointing` on | Exactly-once **state**, at-least-once **output**: replay after recovery re-emits records the sink already wrote. |
| No checkpointing | At-most-once across restarts (processing restarts from the source's configured start offset). |

Requirements for the exactly-once configuration:

- **Consumers of the output topic must use `isolation.level=read_committed`** — otherwise they observe records from aborted transactions and all guarantees are void.
- The `TxnKafkaTransactionalID` must be stable across restarts and unique per pipeline instance (a second instance with the same ID fences the first).
- The marker topic (`<topic>.checkpoints` by default) must not lose the latest marker while a prepared checkpoint may require recovery. Use compaction, durable replication, stable key partitioning, and grant the job read access; recovery scans every partition to its read-committed last-stable offset.
- Output visibility latency equals the checkpoint interval: records become readable when their interval's transaction commits.
- A checkpoint failure fails the pipeline (the aborted transaction's output must be replayed); restart recovers from the last completed checkpoint.

See [checkpoints-and-api.md](checkpoints-and-api.md) for the on-disk schema behind this.

## Comparison with Flink

| Feature | Apache Flink | Weibo |
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

Back to [Core Concepts](concepts.md), or see the full [Architecture](../ARCHITECTURE.md).
