# Workflow Runner and CLI

Goal: make declarative workflows executable as a first-class user path,
not only as compiler/test fixtures.

```
workflow.yaml -> Load -> Validate -> Resolve Secrets -> Compile -> Execute
```

This is the follow-up to the declarative workflow work: the schema,
validation, compiler, secret resolution, and integration tests exist.
What is missing is a small public runner API and a CLI binary that users
can point at a YAML/JSON file.

## Principles

- Keep the library API small. The runner should compose existing
  `workflow.Load`, `workflow.Validate`, and `compiler.Compiler`; it
  should not reimplement compile logic.
- Use the standard library `flag` package for the first CLI. There is no
  existing CLI framework in the repo, and this should not add one.
- Do not print resolved secrets. CLI output, errors, graph summaries,
  and dry-run metadata must use the already-redacted compiled graph and
  `Describe()` behavior.
- Compile must still be non-starting. `dry-run` should stop after
  successful compilation and never execute sources/sinks.
- Execution should be cancellable by context and OS signals.

## Public API

Add `workflow/runner/run.go`.

The runner lives in a subpackage because `workflow/compiler` already
imports `workflow`; putting this API in `workflow` itself would create
an import cycle.

```go
package runner

import (
    "context"

    "github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/compiler"
    "github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/secrets"
)

type Options struct {
    BaseDataDir string
    Secrets     secrets.SecretResolver
}

type RunResult struct {
    Name     string
    Graph    compiler.PipelineGraph
    Delivery compiler.DeliveryGuarantee
}

func CompileFile(path string, opts Options) (*compiler.CompiledWorkflow, error)
func RunFile(ctx context.Context, path string, opts Options) (*RunResult, error)
```

Notes:

- `CompileFile` is useful for tests, dry-run, and embedding.
- `RunFile` calls `CompileFile`, then `CompiledWorkflow.Env.Execute(ctx)`.
- `RunResult` intentionally exposes only non-secret metadata.
- `Options.Secrets` defaults to `secrets.Environment`.
- `Options.BaseDataDir` passes through to `compiler.Compiler`.
- The API should not expose the resolved `Workflow` copy.

Error wrapping:

- Parse/load errors: `workflow runner: load <path>: ...`
- Validation errors: return the existing `ValidationErrors` wrapped once.
- Secret errors: preserve compiler's sanitized message.
- Execute errors: `workflow runner: execute <name>: ...`

## CLI

Add `cmd/weibo-workflow/main.go`.

Command:

```sh
go run ./cmd/weibo-workflow --file workflow.yaml
```

Flags:

| Flag | Default | Behavior |
|---|---|---|
| `--file` | required | Workflow YAML/JSON path. |
| `--data-dir` | `./data` via compiler default | Base dir for derived state/checkpoint dirs. |
| `--dry-run` | `false` | Load, validate, resolve secrets, compile, print summary, do not execute. |
| `--describe` | `false` | Print compiled pipeline description JSON. Works with or without execution. |
| `--quiet` | `false` | Suppress success summary. Errors still go to stderr. |

Exit codes:

- `0`: dry-run succeeded, or workflow executed cleanly.
- `1`: parse, validation, compile, secret resolution, or execution error.
- `2`: CLI usage error, such as missing `--file`.

Output:

Dry-run / startup summary should be short and stable:

```text
workflow: orders
delivery: at-least-once
source: kafka
operators: completed(filter), by-customer(keyBy), totals(reduce)
sink: kafka
```

`--describe` prints `CompiledWorkflow.Env.DescribeJSON()` after compile.
This must not contain:

- Postgres DSN
- Kafka SASL username/password
- resolved secret values
- raw environment variable values

Signal handling:

- Use `signal.NotifyContext(context.Background(), os.Interrupt,
  syscall.SIGTERM)`.
- Pass that context to `runner.RunFile` or the compiled environment.
- Let `StreamExecutionEnv.Execute` perform its existing graceful drain.
- If the context is cancelled and `Execute` returns `context.Canceled`,
  print a concise cancellation message and exit `1` unless the pipeline
  completed normally.

## Implementation Steps

### 1. Add runner API

Files:

- `workflow/runner/run.go`
- `workflow/runner/run_test.go`

Implementation:

1. `CompileFile(path, opts)`:
   - call `workflow.Load(path)`
   - instantiate `compiler.Compiler{BaseDataDir: opts.BaseDataDir,
     Secrets: opts.Secrets}`
   - call `CompileWorkflow`
   - return `CompiledWorkflow`
2. `RunFile(ctx, path, opts)`:
   - call `CompileFile`
   - call `cw.Env.Execute(ctx)`
   - return `RunResult{Name, Graph, Delivery}`

Tests:

- YAML file loads and executes over generator -> stdout/blackhole.
- Invalid workflow returns validation error.
- Missing secret returns sanitized error.
- Dry-run equivalent via `CompileFile` does not execute.

### 2. Add CLI binary

Files:

- `cmd/weibo-workflow/main.go`
- `cmd/weibo-workflow/main_test.go` if useful; otherwise cover via
  shell-style integration tests under `test/workflow`.

Implementation:

- Parse flags with `flag.NewFlagSet`.
- Validate required `--file`.
- Create signal-aware context.
- On `--dry-run`, call `runner.CompileFile`.
- Otherwise call `runner.RunFile`.
- Print summaries through helper functions that accept `io.Writer`.
- Keep `main()` tiny:
  - `os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr))`

Tests:

- Missing `--file` exits `2`.
- `--dry-run --file valid.yaml` exits `0`.
- invalid YAML exits `1`.
- `--describe` output does not include env secret values.

### 3. Add examples

Add declarative examples:

- `workflow/examples/wordcount.yaml` or `examples/workflows/wordcount.yaml`
- `examples/workflows/order-totals.yaml`
- `examples/workflows/kafka-to-kafka-exactly-once.yaml`
- `examples/workflows/postgres-sink.yaml`

Prefer `examples/workflows/` because these are runnable assets rather
than package testdata.

Each example should use `${...}` placeholders for secrets:

```yaml
sink:
  type: postgres
  postgres:
    dsn: ${POSTGRES_DSN}
```

### 4. Update docs

Files:

- `README.md`
- `workflow/README.md`
- `workflow/spec.go` package comment
- `plans/declarative-workflows.md`

Required updates:

- Remove stale "Phase 2.1 only" language.
- Replace registry-first language with current declarative built-ins.
- Add CLI usage:

```sh
export POSTGRES_DSN='postgres://...'
go run ./cmd/weibo-workflow --file examples/workflows/postgres-sink.yaml
go run ./cmd/weibo-workflow --file examples/workflows/postgres-sink.yaml --dry-run --describe
```

## Security Requirements

The runner and CLI must never include resolved secrets in:

- logs
- validation output
- pipeline descriptions
- dashboard metadata
- CLI summaries
- error messages

Concrete checks:

- CLI tests should set `KAFKA_PASSWORD=super-secret-value` and assert
  stdout/stderr do not contain it.
- `--describe` must not show Postgres DSN host/user/password. Current
  `PostgresSink.Describe()` already omits DSN-derived host metadata.
- Compiler errors must keep using sanitized secret-resolution errors.

## Testing Matrix

Unit tests:

- `go test ./workflow/...`
- `go test ./cmd/weibo-workflow`

Integration tests:

- Extend `test/workflow`:
  - runner executes valid YAML
  - CLI dry-run succeeds
  - CLI rejects invalid operator ordering
  - CLI output redacts secrets
  - CLI `--describe` emits valid JSON

Full suite:

```sh
go test ./...
```

Fuzz tests added in Phase 2.13 should continue to compile and run:

```sh
go test ./test/workflow -run=^$ -fuzz=FuzzParseYAML -fuzztime=10s
go test ./test/workflow -run=^$ -fuzz=FuzzFieldPaths -fuzztime=10s
go test ./test/workflow -run=^$ -fuzz=FuzzFilterComparisons -fuzztime=10s
go test ./test/workflow -run=^$ -fuzz=FuzzNumericConversions -fuzztime=10s
```

## Open Decisions

1. Binary name:
   - Recommended now: `weibo-workflow`
   - Future option: a broader `weibo` CLI with subcommands
     (`weibo workflow run ...`)
2. Whether `--describe` should print SDK `DescribeJSON()` or the
   compiler graph.
   - Recommended: print SDK `DescribeJSON()` because it is what the
     dashboard consumes.
3. Whether CLI should support `--validate-only`.
   - Recommended: skip initially; `--dry-run` is stronger because it
     validates and compiles without executing.

## Success Condition

- A user can run a declarative YAML/JSON workflow from the terminal with
  no Go code.
- `--dry-run` catches parse, validation, secret, and compile problems
  without starting the pipeline.
- CLI and runner output expose only non-secret graph/description data.
- Existing correctness tests and Phase 2.13 workflow integration tests
  remain green.
