# Weibo

A stream processing engine in Go, inspired by Apache Flink.

Weibo reads unbounded data streams from sources like Apache Kafka, applies real-time transformations (map, filter, reduce, window), and writes results to sinks. It supports stateful processing, windowed aggregations, durable disk-backed state, and fault tolerance via checkpointing — up to end-to-end exactly-once for Kafka-to-Kafka pipelines.

It's a lightweight, embeddable Go library — not a cluster runtime. You import it, define your pipeline, and run it. A separate, optional control plane gives you a web dashboard for running and watching jobs as containers.

![Dashboard overview](docs/images/dashboard-overview.png)

---

## Why?

Apache Flink is powerful but Java-heavy and complex. There's no idiomatic Go stream processing engine that lets you:

- Consume Kafka topics as unbounded streams
- Apply stateful transformations with exactly-once semantics
- Window and aggregate events by time
- Recover from failures without data loss

Weibo fills this gap.

---

## Quickstart

```sh
go get github.com/ASHUTOSH-SWAIN-GIT/weibo
go run ./examples/wordcount   # no external services required
```

```go
env := weibo.NewEnv()

env.
    FromSource(kafkaSource).
    KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(8).
    Window(window.NewTumbling(5 * time.Minute)).
    Reduce(aggregateAmount).
    ToSink(kafkaSink)

env.Execute(context.Background())
```

See [Getting Started](docs/getting-started.md) for a full walkthrough,
including running examples and the control-plane dashboard.

---

## Documentation

| Doc | Covers |
|---|---|
| [Getting Started](docs/getting-started.md) | Install, run an example, run the dashboard, run tests |
| [Core Concepts](docs/concepts.md) | Stream, Record, Source, Sink, Operator, Window, Watermark, State, Checkpoint, Job |
| [SDK API](docs/sdk-api.md) | Building pipelines in Go: the fluent chain, `Process`, and declarative YAML workflows |
| [Architecture](ARCHITECTURE.md) | How the engine executes: stages, edges, backpressure, checkpoint protocol, control plane |
| [Package Structure](docs/package-structure.md) | What lives where in the repo |
| [Design Decisions](docs/design-decisions.md) | Why single-process, why barrier checkpointing, delivery guarantees, comparison with Flink, implementation status |
| [Dashboard](docs/dashboard.md) | The control-plane web UI, with screenshots, and how to run a demo job against it |
| [Dashboard data contract](docs/dashboard-data-contract.md) | Source-of-truth inventory of dashboard endpoints/fields |
| [Benchmarks](docs/benchmarks.md) | Correctness under load with injected failures, recovery timings, microbenchmarks, and what the testing found |
| [Checkpoints & API compatibility](docs/checkpoints-and-api.md) | On-disk checkpoint schema, savepoints, API compatibility |
| [Self-hosting](docs/self-hosting.md) | Running the control plane on a VM: Docker vs Kubernetes backends, security checklist |

---

## Status

Actively developed. The engine (stage-based execution, keyed parallelism, checkpoint/recovery, backpressure, metrics) is implemented and covered by unit + integration tests, including crash-and-recovery and shutdown-under-load scenarios. See [Implementation Status](docs/design-decisions.md#implementation-status) for what's done and what's next.
