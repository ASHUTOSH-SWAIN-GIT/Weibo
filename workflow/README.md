# Workflow Format

Define a Mailer pipeline in YAML or JSON instead of Go. A workflow
document describes a pipeline's **shape and configuration**; it is
parsed, validated, compiled into the same SDK objects the fluent API
produces, and run by the same engine.

```
workflow.yaml → Parse → Validate → Compile → Execute
```

> Status: **Phase 2.1 (format definition + structural parsing)**.
> Validation, ref resolution, compilation, and execution are later
> phases and are not implemented yet.

## Logic by reference

Transformation logic — map/filter/reduce functions, key selectors,
Postgres row mappers — is Go code and cannot live in a config file. So
steps reference logic **by name** (`ref`), and those names are resolved
against a registry at compile time. The document carries only which
registered logic to wire where, plus pure configuration (brokers,
topics, window sizes, checkpoint dirs, partition counts, …).

```yaml
pipeline:
  - type: map
    map: { ref: parseOrder }   # a Go func registered under "parseOrder"
  - type: reduce
    reduce: { ref: builtin:count }
```

A `builtin:<name>` ref (e.g. `builtin:count`) selects a built-in
provided by the registry, so the simplest pipelines need no user Go.

## Top-level structure

```yaml
name: my-pipeline        # optional, for logs/metrics
version: "1"             # optional format version
env: { ... }             # optional; SDK defaults when omitted
source: { ... }          # required
pipeline: [ ... ]        # optional ordered operators
sink: { ... }            # required
```

## Durations

Any duration field is a Go duration string: `"30s"`, `"5m"`,
`"500ms"`, `"1h30m"`. Omitting a duration means the SDK default.

## `env`

| Field | Type | Default | SDK method |
|---|---|---|---|
| `bufferSize` | int | 1024 | `WithBufferSize` |
| `shutdownTimeout` | duration | 30s | `WithShutdownTimeout` |
| `checkpointing.interval` | duration | — | `WithCheckpointing` |
| `checkpointing.dir` | string | — | `checkpoint.NewFileStorage` |
| `state.backend` | `memory` \| `pebble` | memory | `WithStateBackend` |
| `state.dir` | string | — | Pebble root dir (required for pebble) |

## `source`

`type` is one of `kafka`, `slice`, `generator`.

```yaml
source:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: orders           # or topics: [a, b] for consumer-group
    groupID: my-group
    startFrom: earliest     # earliest | latest
    exactlyOnce: true       # pair with a txnKafka sink
    parallel: false         # per-partition readers (no group)
    deserialize: json       # "json" or a registered deserializer ref
    commitBatch: 0          # commit every N messages (0 = per message)
    fetchMinBytes: 1
    fetchMaxBytes: 10485760
    watermark:
      maxOutOfOrderness: 1s
      interval: 500ms
    sasl: { mechanism: plain, username: u, password: p }
    tls:  { caFile: ca.pem, insecureSkipVerify: false }
```

`slice`/`generator` sources carry inline test data:

```yaml
source:
  type: generator
  records:
    - { key: sentence, value: "hello world" }
```

## `pipeline`

An ordered list of operators. Each operator is a **discriminated
union**: `type` names the kind, and a matching **typed config block**
(named after the kind) carries its configuration. There is no shared
bag of fields — each kind decodes into its own struct, so a field that
belongs to another kind, or is misspelled, is rejected rather than
silently ignored. An optional `id` names the operator (must be unique).

| `type` | Config block | Uses `ref` | Fields |
|---|---|---|---|
| `map` | `map` | map fn | `ref`, `label`, `parallelism` |
| `filter` | `filter` | predicate | `ref`, `label`, `parallelism` |
| `flatMap` | `flatMap` | flatmap fn | `ref`, `label`, `parallelism` |
| `process` | `process` | fn returning error | `ref`, `parallelism`, `onError` (drop\|dlq\|fail), `dlq` |
| `keyBy` | `keyBy` | key selector | `ref`, `partitions` (default 16) |
| `reduce` | `reduce` | reduce fn | `ref`, `label` (after keyBy) |
| `window` | `window` | — | window fields (after keyBy) |

```yaml
pipeline:
  - type: map
    map:
      ref: parseOrder
      parallelism: 4        # order not preserved when > 1
  - type: keyBy
    keyBy:
      ref: byCustomer
      partitions: 8
  - type: window
    window:
      type: tumbling        # tumbling | sliding | session
      size: 5m              # tumbling/sliding
      slide: 1m             # sliding only (<= size)
      gap: 30s              # session only
      offset: 0s            # tumbling/sliding
      idleTimeout: 0s       # fire remaining windows after inactivity
  - type: reduce
    reduce:
      ref: sumAmount
```

## `sink`

`type` is one of `kafka`, `txnKafka`, `postgres`, `stdout`, `blackhole`.

```yaml
sink:
  type: txnKafka          # exactly-once (transactional)
  txnKafka:
    brokers: [localhost:9092]
    topic: order-totals
    transactionalID: order-totals-pipeline   # stable, unique per instance
    markerTopic: ""       # default "<topic>.checkpoints"
    serialize: json
```

```yaml
sink:
  type: kafka             # at-least-once
  kafka:
    brokers: [localhost:9092]
    topic: out
    batchSize: 100
    batchTimeout: 1s
    acks: leader          # none | leader | all
    async: false
    serialize: json
    maxRetries: 3
    onError: drop         # drop | dlq | fail
```

```yaml
sink:
  type: postgres
  postgres:
    dsn: postgres://user:pass@host:5432/db
    mapper: orderRowMapper   # registered RecordMapper ref
    batchSize: 100
    flushInterval: 5s
    maxRetries: 3
    onError: drop
```

## Parsing

```go
wf, err := workflow.Load("pipeline.yaml")   // dispatches on .yaml/.yml/.json
// or workflow.ParseYAML(bytes) / workflow.ParseJSON(bytes)
```

Parsing is **strict**: unknown keys are rejected (typo protection) and
durations are validated. It does not check that the document is a
runnable pipeline — required fields, `keyBy` ordering, exactly-once
consistency, and ref existence are checked in the validation phase.

See `testdata/` for complete example documents.
