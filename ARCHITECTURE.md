# Mailer — Architecture

Mailer is a **stream-processing engine in Go, inspired by Apache Flink**. It reads
unbounded streams (Kafka), applies stateful/windowed transformations, and writes
to sinks — with checkpointing, durable state, and up to end-to-end exactly-once.

Unlike Flink, it is **not a cluster runtime**. The engine is a single-process,
multi-goroutine library you import. A separate **control plane** (its own Go
module) then runs *many* such jobs, one container each, on Docker or Kubernetes.

---

## The three tiers

Mailer is layered. Read it top-to-bottom — each tier only depends on the ones
below it.

```
┌──────────────────────────────────────────────────────────────┐
│  CONTROL PLANE  (control/ — separate Go module)              │
│  submit → launch 1 container/job → reconcile to desired state│
│  SQLite store · Docker/Kubernetes backends · REST API · UI   │
└───────────────┬──────────────────────────────────────────────┘
                │ launches containers running…
┌───────────────▼──────────────────────────────────────────────┐
│  JOB HARNESS  (sdk/, jobagent/, cmd/, workflow/runner)       │
│  supervise ONE job in-process, expose control HTTP surface   │
│  (/state /metrics /cancel /savepoint), graceful drain        │
└───────────────┬──────────────────────────────────────────────┘
                │ supervises…
┌───────────────▼──────────────────────────────────────────────┐
│  CORE ENGINE  (mailer.go, pipeline/, operator/, state/, …)   │
│  Source → operator stages → Sink                             │
│  stage-based execution · keyed parallelism · checkpointing   │
└──────────────────────────────────────────────────────────────┘
```

- **Why the split?** The core engine stays dependency-light — `go get mailer`
  never pulls the Docker/Kubernetes/SQLite clients, which live only in the
  `control/` module.
- **One job = one container = one OS process.** There is no cross-node
  coordination inside a job; scale-out is "run more jobs" (Kafka partitions
  distribute the work across consumer-group members).

---

## Tier 1 — Core Engine

The dataflow engine. Entry point is `StreamExecutionEnv` (`mailer.go`), built
with a lazy fluent API (`stream.go`) and run with `Execute()`.

```go
env := mailer.NewEnv()
env.FromSource(kafkaSource).
    Map(parse).Filter(valid).
    KeyBy(customerKey).WithPartitions(8).
    Window(window.NewTumbling(5 * time.Minute)).
    Reduce(sumAmount).
    ToSink(kafkaSink)
env.Execute(ctx)
```

### Record — the unit of data (`types/record.go`)

Every item carries `Key`, `Value` (`[]byte`), `Timestamp`, `Offset`, `Partition`,
`Headers`. The same `Record` channel also carries **watermarks** and **checkpoint
barriers** in-band — control signals flow with the data, which is what makes
consistent snapshots possible.

### Sources & Sinks (`source/`, `sink/`)

- **Sources:** `KafkaSource` (multi-partition, consumer groups, SASL/TLS,
  deserializers, watermark generation), plus `SliceSource`/`GeneratorSource` for
  tests.
- **Sinks:** `KafkaSink` (at-least-once), `TxnKafkaSink` (transactional, for
  exactly-once), `PostgresSink` (batch upserts), `StdoutSink`, `BlackholeSink`.
- Both are small interfaces; `auth/` holds the SASL/TLS config shared by the
  Kafka connectors.

### Operators (`operator/`)

> ⚠️ `operator/` is the **dataflow operator library**, *not* the Kubernetes
> operator. The k8s integration lives in `control/backend/`.

`Operator` is the transform interface. Concrete ops: `Map` (1:1), `FlatMap`
(1:N), `Filter`, `Process` (error-aware with drop/DLQ/fail policy), `KeyBy`
(hash-partition by key), `Reduce` (per-key accumulator), `Window` (buffers into
time windows in the state backend, fires on watermark, drops late records).

Operators advertise *optional capability interfaces* the engine detects at plan
time — e.g. `SingleProcessor` (chain without channels), `Cloneable` (per-key
state isolation), `StateConfigurable` (receive a state backend),
`BarrierSnapshotter` (race-free snapshot as a barrier passes), `NativeSnapshotter`
(hard-link checkpoints).

### Execution model — stages & edges (`pipeline/`)

`Execute()` runs a **planner** (`planner.go`) that groups the operator chain into
**stages**:

```
[Source] →edge→ [Map→Filter] →edge→ [KeyBy: Window→Reduce ×N workers] →edge→ [Sink]
```

- **Inside a stage:** operators run as direct function calls — no channels, no
  goroutine hops. Consecutive stateless ops (Map/Filter/FlatMap/Process) with the
  same parallelism fuse into one stage.
- **`KeyBy` starts a keyed stage:** a router hash-dispatches records to N stateful
  workers (same key → same worker), each with cloned operators and isolated state.
- **Edges** (`edge.go`) are the only channels — bounded, default capacity 1024,
  *between* stages.
- **Backpressure is free:** a send to a full edge blocks. A slow sink fills its
  input edge → blocks the stage before it → … → the source stops fetching from
  Kafka. Bounded memory, zero drops, no tuning.
- **Barriers & watermarks** (`markers.go`) are broadcast to every worker of a
  parallel stage and re-aligned at the stage exit, so snapshots stay consistent
  at any parallelism.
- **Shutdown is two-phase:** cancel the context → source stops → downstream drains
  via cascading channel closes (a final barrier rides the drain so state is saved)
  → only a drain past `WithShutdownTimeout` is force-aborted.

### State (`state/`)

`StateBackend` interface with `ValueState` (one value/key) and `ListState`
(list/key). Two backends, chosen with `WithStateBackend`:

- **`InMemory()`** (default) — RAM; state serialized into checkpoints. Wins below
  ~100k keys.
- **`Pebble(dir)`** — disk-backed LSM. State bounded by disk, not RAM. Checkpoints
  are **native**: the operator hard-links its LSM files at the barrier instead of
  serializing, so checkpoint cost scales with *changed* data. (At 5M keys: ~0.7 MB
  heap vs 579 MB in-memory; benchmarks in `bench/`.)

Each keyed worker gets its own isolated backend.

### Checkpointing & delivery guarantees (`checkpoint/`)

Barrier-based (**Chandy-Lamport**): barriers flow through the graph, operators
snapshot on arrival. A checkpoint = source offsets + all operator state, written
to a file (fsync, status pointers).

The `coordinator.go` drives a **two-phase commit** for exactly-once: align source
offsets → snapshot operator state → commit the sink transaction → promote the
checkpoint file to `completed`. A per-checkpoint transaction marker resolves
crashes between sink commit and checkpoint completion. `savepoint.go` promotes a
checkpoint to a named, durable snapshot for upgrades/redeploys.

| Configuration | Guarantee |
|---|---|
| `KafkaSource(exactly-once)` → `TxnKafkaSink` + checkpointing | **End-to-end exactly-once** |
| Any source → plain sink, checkpointing on | Exactly-once **state**, at-least-once **output** |
| No checkpointing | At-most-once across restarts |

*(Exactly-once requires output consumers to use `read_committed`, a stable
`transactionalID`, and the marker topic to survive.)*

### Observability (`observability/`)

Prometheus metrics at every level — pipeline, operator, worker, stage, edge
(`mailer_edge_queue_size` pinned at capacity pinpoints the bottleneck) — plus a
built-in web dashboard. `metadata.go`'s `Describe()` emits the pipeline topology
as JSON for the dashboard.

---

## Tier 2 — Job Harness

Turns a configured `StreamExecutionEnv` into a **supervised, remotely
controllable job**. Two front doors (YAML and Go SDK) converge on one lifecycle.

### `jobagent/` — the per-job supervisor + control contract

`Agent` wraps one `StreamExecutionEnv`. `Run(ctx)` registers a checkpoint
listener, moves phase Starting→Running, calls `Execute`, and classifies the exit
(clean cancel = Finished, error = Failed). It serves an HTTP control surface
(`http.go`) that **every runner container embeds**:

| Route | Purpose |
|---|---|
| `GET /healthz` | Liveness + phase |
| `GET /state` | Lifecycle snapshot (phase, uptime, records in/out, last checkpoint) |
| `GET /describe` | Compiled pipeline topology |
| `GET /metrics` | Prometheus exposition |
| `POST /cancel` | Graceful drain |
| `POST /savepoint?label=` | Drain, final checkpoint, promote to named savepoint |

### `sdk/` — harness for hand-written Go jobs

`sdk.Run(build)` is the entrypoint for a Go pipeline (`func main(){ sdk.Run(Build) }`).
It configures the env from env vars (`DATA_DIR`, `PORT`, `SAVEPOINT_DIR`,
`CHECKPOINT_INTERVAL`, `RESTORE_SAVEPOINT`), then calls the shared `sdk.Serve` —
which restores any savepoint, starts a `jobagent`, runs to completion, and
promotes a final savepoint on request. **This is why SDK and YAML jobs behave
identically** — they share `Serve`.

### `workflow/` — declarative YAML/JSON pipelines (no user code)

Describe a pipeline as a document instead of Go:

```
workflow.yaml → Parse → Validate → Resolve Secrets → Compile → Execute
```

- **`spec.go`** — the whole schema as Go structs. Tagged unions for source
  (`kafka`/`slice`/`generator`), sink (`kafka`/`txnKafka`/`postgres`/`stdout`/
  `blackhole`), and operators.
- **`parse.go`** — strict decode (unknown fields rejected); `Load()` dispatches by
  extension.
- **`validate.go`** — exhaustive up-front validation (structural, per-field config,
  pipeline ordering — `reduce`/`window` require a preceding `keyBy` — and
  delivery-guarantee consistency). Invalid workflows never reach runtime.
- **`operators/`** — declarative built-ins (filter/select/rename/set/keyBy/
  count+sum reduce/window) implemented over a JSON record model (`record/`), so no
  Go functions are needed. `map`/`flatMap`/`process` refs are reserved for a
  future function registry and rejected today.
- **`secrets/`** — `${VAR}` placeholders expanded (only in sensitive fields like
  DSNs and SASL creds) from the environment; never echoed in errors.
- **`compiler/`** — turns a validated spec into the **same `StreamExecutionEnv`**
  the fluent API produces: `Validate → resolveSecrets → CompileSource →
  CompileRuntime → applyOperators → CompileSink`. Derives job-isolated
  `<dataRoot>/<name>/{state,checkpoints}` dirs. Opens **no network connections**
  except an eager Postgres pool (so an unreachable DB fails at submit). SQL
  identifiers are regex-guarded; `json.Number` used throughout for numeric
  fidelity.
- **`runner/`** — thin orchestration: `CompileFile` / `RunFile`.

### `cmd/` — binaries

- **`cmd/mailer-workflow`** — developer CLI: `--file`, `--dry-run`, `--describe`.
  Compiles and runs a workflow directly (no container).
- **`cmd/mailer-runner`** — the **generic entrypoint baked into the runner image**.
  Configured entirely by env vars (`WORKFLOW`, `DATA_DIR`, `PORT`, …), it compiles
  the mounted document and hands it to `sdk.Serve` under a jobagent. One prebuilt
  image runs *any* YAML job. (`Dockerfile.runner` builds it.)

---

## Tier 3 — Control Plane (`control/`, separate Go module)

The **JobManager equivalent** for the one-container-per-job model: submit a
workflow, it launches a runner container, tracks lifecycle, and continuously
reconciles running containers toward each job's desired state. Survives its own
crashes because the store is the source of truth.

| Package | Role |
|---|---|
| `store/` | **SQLite persistence** (jobs, runs, transitions). Source of truth. Never stores secrets. |
| `lifecycle/` | Coarse phase state machine + restart policy (bounded attempts, backoff). |
| `backend/` | `ContainerBackend` interface + **Docker** / **Kubernetes** / fake impls. |
| (root) | `Controller`: submit/cancel/restart/savepoint + the reconciler loop. |
| `api/` | REST server over the controller. |
| `ui/` | Single self-contained embedded HTML dashboard. |
| `cmd/mailer` | The `mailer` CLI (`mailer dashboard`). |

### Controller & reconciler

- **`Submit`** validates by dry-run compiling the workflow, persists the `Job`
  (spec with `${VAR}` intact), stores secrets **in memory only**, launches the
  first container. Supports `kind: yaml` (generic runner image) and `kind: sdk`
  (prebuilt Go image).
- **Reconciler** (`reconcile.go`) — a ~3s ticker: reads active runs, polls backend
  `Status`, drives phase transitions, applies the restart policy to crashed
  containers, marks clean exits Finished. On controller restart it re-reads active
  runs and **re-attaches** to live containers.
- **Exactly-once safeguards:** *single-live-run fencing* (never two containers for
  one job → two transactional producers with the same id never coexist) and a
  *stable transactional id* (`MAILER_JOB_ID` injected into container env, reused
  verbatim on restart).

### Backends — where containers run (same jobs/API/UI on both)

- **Docker** (default) — one container/job on the local daemon; per-job named
  volume for state+checkpoints, shared `mailer-savepoints` volume, workflow
  injected via tar copy.
- **Kubernetes** — one `batch/v1` **Job** per job (`backoffLimit: 0`,
  `RestartPolicyNever` — mailer's reconciler owns restarts, not k8s). Per job:
  a **PVC** (state+checkpoints, reused across restarts), a **ConfigMap** (workflow),
  an optional **Secret** (env), and a **ClusterIP Service** so the controller can
  reach the agent's control surface. Runs as distroless nonroot with `fsGroup`;
  `/healthz` liveness/readiness probes.

### REST API (`api/`)

`POST /jobs` (submit), `GET /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel|restart|savepoint`,
`GET /jobs/{id}/logs`. The live-state routes — `GET /jobs/{id}/state|metrics|describe`
— **proxy straight to the running container's jobagent**, closing the loop from
dashboard back down to the core engine.

---

## End-to-end flow (a YAML job, submitted to the control plane)

1. `POST /jobs` → **Controller.Submit** dry-run compiles (`workflow/compiler`),
   persists the `Job` to SQLite, holds secrets in memory, calls `backend.Launch`.
2. **Backend** (Docker/k8s) starts a **runner container** with the workflow doc +
   resolved env. Inside, `cmd/mailer-runner` compiles the doc into a
   `StreamExecutionEnv` and hands it to `sdk.Serve` → a **jobagent** runs it.
3. **Core engine** executes: Source → operator stages → Sink, checkpointing per
   the config.
4. **Reconciler** polls `backend.Status`, restarts failures per policy, records
   transitions.
5. **Dashboard/API** proxies `/state` · `/metrics` · `/describe` and issues
   `/cancel` · `/savepoint` to the in-container agent. Savepoint = drain + final
   checkpoint promoted to a named blob; the per-job volume/PVC preserves state
   across restarts.

---

## Package map

```
mailer/
├── mailer.go, stream.go, metadata.go   # core: env, fluent API, Describe()
├── types/          # Record (data / watermark / barrier)
├── pipeline/       # stage-based execution: planner, stages, edges, markers, metrics
├── operator/       # dataflow operators (Map/Filter/KeyBy/Reduce/Window/Process)
├── window/         # tumbling / sliding / session assigners
├── watermark/      # bounded out-of-orderness strategies
├── state/          # StateBackend: in-memory + Pebble (LSM), native checkpoints
├── source/         # Kafka + slice/generator
├── sink/           # Kafka, TxnKafka, Postgres, stdout, blackhole
├── checkpoint/     # barrier snapshots, file storage, 2-phase coordinator, savepoints
├── auth/           # SASL/TLS shared by Kafka source & sink
├── observability/  # Prometheus metrics + built-in dashboard
├── jobagent/       # per-job supervisor + control HTTP surface
├── sdk/            # harness for hand-written Go jobs (Run / Serve)
├── workflow/       # declarative YAML/JSON: spec, parse, validate, compiler, runner
├── cmd/            # mailer-workflow (CLI), mailer-runner (container entrypoint)
├── control/        # ⟵ SEPARATE MODULE: control plane
│   ├── store/ lifecycle/ backend/ api/ ui/ cmd/mailer
│   └── controller.go, reconcile.go
├── bench/          # state-backend scaling benchmarks
├── examples/       # wordcount, windowing, backpressure, exactly-once, kafka/pg …
└── test/           # integration: recovery, exactly-once crash sweep, shutdown
```

## Key design decisions

- **Single-process engine, multi-job control plane.** Simplicity and
  embeddability at the engine level; orchestration is a separate, optional module.
- **Barrier-based checkpointing** (Chandy-Lamport) — proven in Flink; a snapshot
  is always a consistent cut because barriers can't be overtaken.
- **Bounded edges for backpressure** instead of credit-based flow control —
  simpler, and the bottleneck is directly visible in `mailer_edge_queue_size`.
- **YAML and Go SDK jobs share one lifecycle** (`sdk.Serve` + `jobagent`), so the
  control plane treats them identically.
- **Store is the source of truth; secrets are never persisted** — the controller
  can crash and re-attach without losing jobs, and `${VAR}` placeholders keep
  credentials out of SQLite.
- **`segmentio/kafka-go`** (pure Go, no CGO) for simple builds/cross-compilation.
```
