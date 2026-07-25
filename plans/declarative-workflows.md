# Declarative Workflows (Phase 2)

Goal: define and run common Weibo pipelines from YAML/JSON instead of Go.

```
workflow.yaml → Parse → Validate → Compile → Execute
```

## The load-bearing design decision: declarative built-ins

The initial design considered logic-by-reference for every transform,
but the shipped compiler is intentionally narrower and more portable:
common operators are declarative built-ins over the JSON record model.
The workflow file expresses topology plus config, and the compiler turns
that config into ordinary Weibo SDK objects.

```yaml
pipeline:
  - type: keyBy
    keyBy: { field: customer.id, partitions: 8 }
  - type: reduce
    reduce: { function: sum, field: amount }
```

Each operator is a discriminated union: `type` + a matching **typed
config block**. No `map[string]any` anywhere — every component (source,
each operator, sink) decodes into its own typed struct, and the strict
decoder rejects fields that don't belong to that component.

Ref-based `map`/`flatMap`/`process` remain in the schema for a future
function registry, but the current declarative compiler rejects them.
Consequence: a workflow is portable config over a fixed set of built-in
building blocks, not arbitrary code.

## Phase breakdown

- **2.1 Define the format** (this phase): the typed schema (structs +
  yaml/json tags), a `Duration` type, `Parse`/`Load` (structural decode,
  strict on unknown fields), the spec doc, and example documents. No
  semantic validation, no compilation, no execution.
- **2.4 Validate** (DONE): offline semantic checks the schema can't
  express — structural (version/name/ids/types), configuration
  (brokers/topics/durations/dirs/positive intervals), pipeline ordering
  (keyBy before reduce/window), and delivery-guarantee (exactly-once
  requires the full source+sink+checkpoint+txn-id set). Accumulates all
  problems into one `ValidationErrors`; opens no external connections
  (only MkdirAll to confirm dirs are creatable). `workflow/validate.go`.
- **2.5 JSON record model** (DONE): `workflow/record` — `JSONRecord`
  (`map[string]any`) cached on `types.Record.Parsed`, with
  `DecodeJSON`/`EncodeJSON` and dotted-path `GetField`/`SetField`/
  `DeleteField`. Numbers decode as `json.Number` (exact, no float
  rounding). This is the data model built-in field operators read and
  modify.

- **2.6 Built-in stateless operators** (DONE): `workflow/operators` —
  declarative filter (9 comparison ops), select_fields, rename_fields,
  set_fields. Each `BuildX(cfg)` returns an ordinary Weibo function
  (`func(Record) bool` / `func(Record) Record`) over the JSON record
  model, so a YAML operator behaves like a hand-written SDK function.
  Robust numeric coercion (json.Number-aware) makes YAML-int and
  JSON-float configs behave identically.

- **2.8 Source compilation** (DONE): `workflow/compiler/source.go` —
  `CompileSource(SourceSpec) (source.Source, error)` maps Kafka config
  to the functional options (brokers/topic/groupID/startFrom/
  deserialize/watermarks/exactlyOnce/commitBatch/fetch/SASL/TLS) and
  builds slice/generator test sources from inline records. Constructs
  only, never connects: parallel Kafka (which dials at construction) is
  rejected; consumer-group readers are lazy. `json` format wires
  `record.DeserializeJSON`.

- **2.9 Sink compilation** (DONE): `workflow/compiler/sink.go` —
  `CompileSink(SinkSpec) (sink.Sink, error)` for kafka (acks/serialize/
  retries/failure-policy/SASL/TLS), transactional_kafka, postgres,
  stdout, blackhole. Postgres is fully declarative: fixed `table` +
  `field→column` mapping compiled into a `RecordMapper` (no per-record
  tables; table/column names validated as safe SQL identifiers; numbers
  coerced exactly; DSN `${VAR}` expanded; optional `mode: upsert` emits
  `INSERT ... ON CONFLICT ... DO UPDATE`). `json` format serializes the
  declaratively-modified record.

- **2.10 Runtime compilation** (DONE): `workflow/compiler/runtime.go` —
  `CompileRuntime(name, dataRoot, EnvSpec) (*weibo.StreamExecutionEnv, error)`
  applies bufferSize/shutdownTimeout/checkpointing/state-backend, and
  roots state + checkpoints in job-specific dirs
  (`<dataRoot>/<name>/{state,checkpoints}`, name sanitized against
  traversal) so two workflows never share a Pebble DB. Verified
  behaviorally: a real pipeline run populates the isolated dirs and
  writes a checkpoint.

- **2.11 Workflow compiler** (DONE): `workflow/compiler/compiler.go` —
  `Compiler{Connections, BaseDataDir}.Compile(*WorkflowSpec) →
  *weibo.StreamExecutionEnv` (and `CompileWorkflow → CompiledWorkflow`
  with graph + delivery guarantee). Order: validate → resolve
  connections (`${VAR}`) → source → runtime env → operators → sink.
  Operators are now **fully declarative** (no registry): filter
  (field/op/value), selectFields, renameFields, setFields, keyBy (by
  field), reduce (count/sum), window. Ref-based map/flatMap/process are
  rejected (no function registry). Produces a complete pipeline without
  starting it.

Note: the operator schema changed from ref-based (2.3) to declarative in
2.11, because the compiler API carries no function registry — logic must
be expressible as config. map/flatMap/process remain in the schema as
ref-based but are not compilable declaratively.
- **2.12 Secret and environment resolution** (DONE):
  `workflow/secrets` provides `SecretResolver` plus environment-backed
  `${VAR}` resolution for sensitive fields such as Postgres DSN and
  Kafka SASL credentials. Resolved secrets are kept out of errors,
  pipeline descriptions, and dashboard metadata.
- **2.13 Integration tests** (DONE): `test/workflow` verifies parse,
  validation, compilation, execution parity with SDK pipelines,
  memory/Pebble parity, window restore behavior, exactly-once
  validation, secret redaction, isolated state dirs, and fuzz targets.
- **2.14 Runner + CLI** (DONE): `workflow/runner` exposes
  `CompileFile`/`RunFile`, `cmd/weibo-workflow` runs YAML/JSON files
  with `--dry-run`, `--describe`, `--data-dir`, and examples live under
  `examples/workflows/`.

## Scope of 2.1 (this deliverable)

New package `workflow/`:
- `spec.go` — `Workflow` and nested spec structs, `Duration`.
- `parse.go` — `ParseYAML`, `ParseJSON`, `Load(path)` (extension
  dispatch), strict decoding.
- `format.md` / package doc — the format reference.
- `testdata/*.{yaml,json}` — examples that decode cleanly.
- `parse_test.go` — decode + round-trip + strictness tests.

Explicitly NOT in 2.1: validation semantics, the registry, compilation
to SDK objects, execution. Those are 2.2–2.4.
