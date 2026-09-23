# Package Structure

```
weibo/
├── engine.go             # StreamExecutionEnv, NewEnv(), Execute(), checkpointing glue
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
├── control/               # Control plane: separate Go module, REST API + UI +
│                          #   job lifecycle. See ARCHITECTURE.md view 7.
├── workflow/               # YAML/JSON declarative pipeline parser, validator, compiler
├── jobagent/              # In-job control surface: serves /state, /metrics, /cancel and
│                          #   /savepoint, and supervises the pipeline run in a job container
├── sdk/                   # Harness for SDK (Go) jobs: sdk.Run / sdk.Serve
├── telemetry/             # Separate Go module: OpenTelemetry bridge (OTLP/HTTP tracing)
├── scripts/               # CI and quality scripts (coverage gate, static checks, fuzz smoke, demos)
├── plans/                 # Design docs and roadmaps
├── bench/                 # State-backend scaling benchmarks (memory vs Pebble)
├── examples/
│   ├── wordcount/         # FlatMap → KeyBy → Reduce
│   ├── windowing/         # Event time, watermarks, tumbling windows
│   ├── backpressure/      # Fast source + slow sink, bounded edges
│   ├── exactly-once/      # Kafka → keyed reduce → transactional Kafka
│   ├── kafka-orders/      # Kafka → Window → Kafka
│   ├── pg-orders/         # Kafka → Postgres
│   ├── stream-demo/       # Continuously-running SDK job used to exercise the
│   │                      #   dashboard end to end (see docs/dashboard.md)
│   ├── sdk-demo/          # Minimal SDK usage walkthrough
│   └── workflows/         # Sample YAML pipelines for cmd/weibo-workflow
└── test/unit_tests/       # Integration tests: checkpoint recovery, exactly-once
                           #   crash sweep, backpressure, shutdown, durable state,
                           #   per-package unit tests
```

Root-level commands:

| Path | Role |
|------|------|
| `cmd/weibo-runner` | Runs a YAML/JSON workflow document as a standalone job |
| `cmd/weibo-workflow` | CLI to parse/validate/dry-run a workflow file locally |
| `control/cmd/weibo` | Control-plane CLI (`weibo dashboard`, `deploy`, `jobs`, `logs`, …) |

---

Next: [Architecture](../ARCHITECTURE.md) for how these packages execute at
runtime, or [Dashboard](dashboard.md) for the control-plane UI.
