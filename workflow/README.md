# Workflow Format

Define a Mailer pipeline in YAML or JSON instead of Go. A workflow
document describes a pipeline's **shape and configuration**; it is
parsed, validated, compiled into the same SDK objects the fluent API
produces, and run by the same engine.

```
workflow.yaml → Parse → Validate → Resolve Secrets → Compile → Execute
```

The workflow compiler and runner are implemented for the declarative
built-ins described below. Ref-based `map`/`flatMap`/`process` remain
reserved for a future function registry.

## No user code

The operators are **declarative built-ins** — filtering, field
projection/rename/set, key-by-field, and count/sum aggregation are all
expressed as configuration over the JSON record model, so a workflow
needs no Go functions to compile and run. The Postgres row mapping is
likewise declarative (fixed table + field→column map). Transforms that
genuinely need arbitrary code (`map`/`flatMap`/`process` with a `ref`)
are reserved for a future function registry and aren't compilable yet.

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
union**: a required unique `id`, a `type`, and a matching **typed config
block** (named after the type). Each block decodes into its own struct,
so a field that belongs to another kind, or is misspelled, is rejected.

The declarative operators need **no user code** — the compiler builds
them from config over the JSON record model:

| `type` | Config |
|---|---|
| `filter` | `{ field, operator, value }` — 9 comparison operators |
| `selectFields` | `{ fields: [...] }` — keep only these fields |
| `renameFields` | `{ renames: [{from, to}] }` |
| `setFields` | `{ sets: [{field, value}] }` |
| `keyBy` | `{ field, partitions }` — set the keyed-state key from a record field |
| `reduce` | `{ function: count\|sum, field }` — built-in aggregations |
| `window` | `{ type, size, slide, gap, offset, idleTimeout }` |

```yaml
pipeline:
  - id: completed-only
    type: filter
    filter: { field: status, operator: equals, value: completed }
  - id: project
    type: selectFields
    selectFields: { fields: [customer_id, amount] }
  - id: by-customer
    type: keyBy
    keyBy: { field: customer_id, partitions: 8 }
  - id: window
    type: window
    window: { type: tumbling, size: 5m }   # tumbling | sliding | session
  - id: totals
    type: reduce
    reduce: { function: sum, field: amount }
```

`map`, `flatMap`, and `process` exist in the schema as `ref`-based
operators (`{ ref, label, parallelism }`) for a future function
registry, but the declarative compiler cannot build them yet.

## Compiling and running

```go
c := &compiler.Compiler{BaseDataDir: "./data"}   // Secrets defaults to environment lookup
env, err := c.Compile(wf)                         // validate → resolve → source → env → operators → sink
// ... then env.Execute(ctx) to run
```

`Compile` produces a complete `*mailer.StreamExecutionEnv` **without
starting it** and without connecting (except a Postgres sink's pool).
`CompileWorkflow` additionally returns the pipeline graph and the
derived delivery guarantee (at-most-once / at-least-once / exactly-once).

For files, use the runner package or CLI:

```go
result, err := runner.RunFile(ctx, "workflow.yaml", runner.Options{BaseDataDir: "./data"})
```

```sh
go run ./cmd/mailer-workflow --file examples/workflows/order-totals.yaml
go run ./cmd/mailer-workflow --file examples/workflows/order-totals.yaml --dry-run --describe
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
    dsn: ${POSTGRES_DSN}     # ${VAR} resolved by the configured secret resolver
    table: customer_totals   # single fixed table (no per-record tables)
    mapping:                 # jsonField → column
      customer_id: customer_id
      payment.total: total_amount
    mode: upsert             # insert | upsert
    conflictColumns: [customer_id]
    updateColumns: [total_amount]  # optional; default = non-conflict mapped columns
    batchSize: 100
    flushInterval: 5s
    maxRetries: 3
    onError: drop
```

The Postgres row mapping is fully declarative: a single fixed `table`
and a `mapping` of JSON field paths to column names. A workflow cannot
generate arbitrary table names per record, and table/column names are
validated as safe SQL identifiers. Mapped numbers keep exact precision
(json.Number → int64/float64); a missing field becomes SQL NULL; a
nested object/array is stored as JSON. Constructing a Postgres sink
opens its connection pool (unlike Kafka/test sinks, which are lazy).
`mode: upsert` compiles to `INSERT ... ON CONFLICT ... DO UPDATE`;
conflict/update columns must be safe identifiers and present in the
declarative mapping.

## Parsing

```go
wf, err := workflow.Load("pipeline.yaml")   // dispatches on .yaml/.yml/.json
// or workflow.ParseYAML(bytes) / workflow.ParseJSON(bytes)
```

Parsing is **strict**: unknown keys are rejected (typo protection) and
durations are validated. It does not check that the document is a
runnable pipeline — that is validation.

## Validation

```go
if err := workflow.Validate(wf); err != nil {
    log.Fatal(err) // every problem, reported together
}
```

`Validate` runs entirely offline — it opens **no** Kafka, Postgres, or
Pebble connection (its only side effect is creating the configured
state/checkpoint directories to confirm they can be created). An
invalid workflow therefore never reaches the runtime. It accumulates
**all** problems into one error:

```
Workflow validation failed:
  - pipeline[2] "totals": reduce requires a keyBy before it
  - env.checkpointing.interval: checkpoint interval must be greater than zero
  - sink.txnKafka.transactionalID: a transactional id is required for transactional Kafka
```

Checks:

- **Structural** — supported version; valid, present workflow name;
  source and sink present and of supported type; operator ids present
  and unique; every operator's config block matches its type.
- **Configuration** — Kafka brokers non-empty and topic present
  (source and sink); window durations valid per kind (size/slide/gap);
  parallelism and keyBy partitions not negative; checkpoint interval
  positive; state/checkpoint directories creatable; a txnKafka sink has
  a transactional id.
- **Pipeline ordering** — `keyBy` before `reduce`/`window`; `window`
  before the aggregation that consumes it. (Only stateless operators
  *can* set `parallelism` — that's guaranteed by the schema, since the
  field exists only on stateless config blocks.)
- **Delivery guarantee** — if either the source declares
  `exactlyOnce: true` or the sink is `txnKafka`, the full set is
  required: exactly-once Kafka source **+** txnKafka sink **+**
  checkpointing enabled **+** a stable transactional id. A
  half-configured exactly-once pipeline is rejected before it can run.

See `testdata/` for complete example documents.
