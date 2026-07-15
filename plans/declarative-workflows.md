# Declarative Workflows (Phase 2)

Goal: define and run common Mailer pipelines from YAML/JSON instead of Go.

```
workflow.yaml → Parse → Validate → Compile → Execute
```

## The load-bearing design decision: logic-by-reference

Map/Filter/FlatMap/Process/Reduce and KeyBy selectors all take **Go
closures** (`func(Record) Record`, `func(accum []byte, curr Record) []byte`,
…). Closures cannot be serialized to YAML, and Mailer is a library, not
a DSL runtime. So the declarative format expresses **topology + config**
and references user logic **by name**:

```yaml
pipeline:
  - type: map
    map: { ref: parseOrder }        # a Go func registered under "parseOrder"
  - type: keyBy
    keyBy: { ref: byCustomer, partitions: 8 }
  - type: reduce
    reduce: { ref: sumAmount }
```

Each operator is a discriminated union: `type` + a matching **typed
config block**. No `map[string]any` anywhere — every component (source,
each operator, sink) decodes into its own typed struct, and the strict
decoder rejects fields that don't belong to that component.

The user registers those functions in Go (a `workflow.Registry`), then
loads + compiles + runs the YAML. The format never contains logic —
only which registered logic to wire where, plus everything that IS pure
config (brokers, topics, window sizes, checkpoint dirs, partition
counts, failure policies).

Consequence: a workflow is portable config over a fixed set of
registered building blocks — not arbitrary code. Pipelines that need
novel logic register a new named function; they don't edit a DSL.

A small set of **built-in refs** (e.g. `builtin:count`, `builtin:passthrough`,
key-by-record-key) will let the simplest pipelines run with zero
user Go — added in the compile phase, not the format.

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

- **2.6 Compile**: `Registry` (typed `RegisterMap`/`RegisterReduce`/… +
  built-ins that operate on `record.JSONRecord` fields) → resolve refs →
  build `source.Source`, `[]operator`, `sink.Sink`, and env config into
  a `*mailer.StreamExecutionEnv`.
- **2.4 Execute + tooling**: `workflow.Run(path, registry, ctx)`, a CLI
  entrypoint, round-trip example workflows for each connector.

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
