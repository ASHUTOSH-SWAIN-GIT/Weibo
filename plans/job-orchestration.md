# Job Orchestration — Containerized Jobs, a Control Plane, and a Web UI

Status: PROPOSAL.

Goal: turn Mailer from "a library you embed" into "a platform you submit jobs
to." A user hands us a pipeline (a YAML workflow or Go SDK code + config); we
package it, run it in a container, track its lifecycle (submit / run / cancel /
restart / savepoint), and show everything in a web UI — the Apache Flink
experience, adapted to Mailer's single-process engine.

---

## 1. The core adaptation: Application Mode, not a cluster

Flink is a distributed runtime: a **JobManager** coordinates many **TaskManager**
nodes, splits an operator graph into parallel subtasks, and shuffles data across
the network. Mailer is deliberately **single-process** — parallelism is
goroutines and per-worker state inside one process, not operators spread across
nodes (see `internals/architecture`).

So we do **not** rebuild Flink's distributed scheduler. We borrow its *control
plane and lifecycle*, and run each job the way Flink's **Application Mode** does:
**one job = one self-contained container** that runs exactly one Mailer pipeline.
The platform is an **orchestrator of job containers**, not a distributed data
plane.

This is the single most important decision — it removes ~80% of Flink's
complexity (slot management, TaskManager registration, network stack, operator
placement) while keeping the parts users actually want: submit a job, watch it,
recover it, upgrade it.

### Flink → Mailer terminology map

| Flink | Mailer equivalent | Notes |
|---|---|---|
| JobManager / Dispatcher | **Controller** (new service) | REST API + lifecycle state machine + scheduler |
| TaskManager | **Job container** | Runs one pipeline; no distributed slots |
| Slot / parallelism across nodes | goroutine parallelism in one process | `WithParallelism` / `WithPartitions`; scale-out = more containers over Kafka partitions |
| JobGraph | `compiler.PipelineGraph` | Already exists (source, operators, sink) |
| ExecutionGraph | live stage/edge metrics | Already emitted (`mailer_stage_*`, `mailer_edge_*`) |
| Checkpoint Coordinator | per-job coordinator | Already implemented (`checkpoint/coordinator.go`) |
| Savepoint | **named checkpoint** | Promote a completed checkpoint dir to a named location |
| Web Dashboard (served by JobManager) | **Controller web UI** + per-job agent | New SPA + extend `observability/dashboard` |
| Application Mode | the default (and only) mode | One job per container |

---

## 2. Architecture

```
                            ┌─────────────────────────────────────────┐
   user / CI ──submit──────▶│              CONTROLLER                  │
   (YAML or SDK job)        │  REST API · lifecycle SM · scheduler     │
                            │  ┌───────────┐   ┌────────────────────┐  │
   browser ──HTTP/WS───────▶│  │ Job store │   │ ContainerBackend   │  │
                            │  │ (SQLite/  │   │  (Docker | K8s)    │  │
                            │  │  Postgres)│   └─────────┬──────────┘  │
                            └──┴───────────┴─────────────┼─────────────┘
                                    ▲ poll status/metrics │ launch / cancel
                                    │                      ▼
                    ┌───────────────┴───────────┐  ┌───────────────────────────┐
                    │   JOB CONTAINER (run A)    │  │   JOB CONTAINER (run B)    │
                    │  mailer engine (Execute)   │  │  mailer engine (Execute)   │
                    │  + control agent (HTTP):   │  │  + control agent (HTTP)    │
                    │    /healthz /describe       │  └───────────────────────────┘
                    │    /metrics  /state         │
                    │    POST /cancel /savepoint  │
                    └──────────┬──────────────────┘
                               │ checkpoints + state
                               ▼
                    ┌───────────────────────────┐
                    │ durable checkpoint storage │  local volume (dev)
                    │ (FileStorage | S3/GCS)     │  or object store (prod)
                    └───────────────────────────┘
```

Three new pieces:

1. **Control agent** — an in-process HTTP server inside every job container
   (extends the existing `dashboard.Server`). Exposes the job's own state and
   accepts control commands. This is how the controller talks to a running job.
2. **Controller** — the JobManager/Dispatcher equivalent: a long-lived service
   with a REST API, a persistent job store, a lifecycle state machine, and a
   pluggable container backend.
3. **Web UI** — a SPA the controller serves: list jobs, drill into one (graph,
   metrics, checkpoint history, logs), and submit/cancel/savepoint.

---

## 2.5 Job creation: how each input becomes a running job

There is **one runner harness** — the control agent + engine lifecycle (§4.1,
§4.3). Both input types produce a job through that same harness; they differ
only in *where the pipeline definition comes from* and *when it is bound in*.

### YAML job — read the document (at runtime)

The engine already knows how to turn a YAML document into a runnable pipeline:
`workflow.Load(path)` → `compiler.Compile(spec)` → `env.Execute(ctx)`. So a YAML
job needs **no build** — the definition is data the harness reads:

```
submit ──▶ Controller reads the YAML:
             workflow.Load → Validate → Compile (dry-run)
             ⇒ derive PipelineGraph + delivery guarantee, store the doc.
       ──▶ Launch the prebuilt `mailer-runner` image with the doc mounted
             + secrets as env + a checkpoint volume.
       ──▶ Inside the container, the harness reads the doc at startup:
             runner.CompileFile → Execute.  ← the engine reads the YAML
             and creates the job, exactly as asked.
```

The document is read twice, deliberately: once by the **controller at submit**
(validate, preview the graph/guarantee, reject bad jobs before any container
starts) and once by the **runner at start** (compile + execute). Same code path
(`workflow/runner`), same result.

### SDK job — compile the code (at build time)

Go is a **compiled** language: there is no way to "read" a user's `.go` file and
run its pipeline the way a YAML document is read, because a Map/Filter/Reduce is
an arbitrary Go closure — the whole reason to use the SDK. So "read the Go code
and create a job" means **compile the code into the job image**. The engine
reads (compiles) the code once, at build time; the resulting image then runs
like any other job.

To keep the harness generic, the user's code is a **library that declares a
pipeline**, not a standalone `main()`. It exports a builder against a fixed
contract, e.g.:

```go
// user's job.go — compiled into the runner image
package job

func Build(env *mailer.StreamExecutionEnv) *mailer.Stream {
    return env.FromSource(mySource).
        KeyBy(byCustomer).WithPartitions(4).
        Reduce(sumAmounts).
        ToSink(myTxnSink)
}
```

The platform owns `main()` (the harness): it applies runtime config, calls the
user's `Build`, injects the control agent, and runs `Execute` + graceful
shutdown. This mirrors the YAML runner (generic harness + supplied definition)
and Flink's application model (user declares the graph, the framework runs it).

```
submit (repo / build context with a mailer.yaml manifest naming the Build fn)
       ──▶ Controller reads the code by BUILDING it:
             render harness main.go → `go build` the user pkg + harness + module
             → produce a job image → push to the registry.
       ──▶ Launch that image (same lifecycle as a YAML job from here on).
```

Why not statically parse the Go to avoid a build? You could translate a *very*
restricted subset (only chained calls with literal args) into the same internal
form as YAML — but that subset is exactly what YAML already expresses, and it
cannot capture custom closures, which is the point of the SDK. So SDK ⇒ compile.
(If a user's pipeline is fully declarative, they should just write YAML.)

### The payoff: one harness, one lifecycle

Because both paths converge on the same runner harness and the same
`StreamExecutionEnv`, everything downstream — control agent, cancel = graceful
shutdown, checkpoint/recovery, savepoints, metrics, the web UI — is identical
regardless of how the job was authored. Only *job creation* differs: **read the
YAML** vs **compile the Go**.

---

## 3. Design decisions

- **D1 — One job per container (Application Mode).** No shared TaskManagers, no
  session cluster. The engine already isolates each workflow's state and
  checkpoint dirs by name (`compiler.CompileRuntime`); a container is just that
  isolation made physical. Scale-out for one logical job = run N containers,
  each a Kafka consumer-group member over a partition subset (documented as the
  scaling story, since Mailer can't split one operator across nodes).

- **D2 — Two job kinds, YAML first.** See §2.5 for exactly how each becomes a
  job. In short: a YAML job is **read at runtime** by a generic prebuilt image
  (no build); an SDK job must be **compiled at build time** into a job image
  (Go is compiled, not interpreted — you cannot "read" a `main.go` the way you
  read a YAML). YAML is 90% of the value for ~20% of the work — ship it first;
  the SDK build pipeline is the last phase.

- **D3 — Cancel = graceful two-phase shutdown, reused as-is.** The engine already
  does the right thing on context cancellation: stop the source, drain via
  cascading closes, inject a final checkpoint barrier, then hard-abort only past
  `shutdownTimeout` (`internals/execution-engine`). The control agent's
  `POST /cancel` cancels the run context; the platform gets a clean stop and a
  final checkpoint for free.

- **D4 — Checkpoints must outlive the container.** A container is ephemeral; its
  `/data` dir is not. Two storage tiers:
  - dev/Docker: a mounted host volume per job (`<host>/mailer-data/<job>`).
  - prod/K8s: a durable `checkpoint.Storage` backed by S3/GCS (new
    implementation of the existing interface) or a PersistentVolumeClaim.
  On restart the same location is remounted/reused, and the engine's existing
  recovery path restores offsets + state. **Exactly-once across restarts
  additionally requires a stable `transactionalID`** — pin it to the JobID.

- **D5 — Savepoints = promoted named checkpoints.** Flink's savepoint (portable
  snapshot for upgrade/rescale) maps to: trigger a graceful stop → the final
  completed checkpoint → copy/rename that checkpoint dir (+ Pebble state dirs) to
  `savepoints/<job>/<label>`. "Restart from savepoint" points a new run's
  checkpoint storage at it. Needs a small `SavepointStore` and a
  trigger-and-wait handshake through the control agent.

- **D6 — `ContainerBackend` interface, Docker first.**
  ```
  Launch(ctx, JobSpec) (RunHandle, error)   // create + start
  Stop(ctx, RunHandle, graceful bool) error
  Status(ctx, RunHandle) (RunState, error)
  Logs(ctx, RunHandle) (io.ReadCloser, error)
  ```
  Docker implementation (SDK) for local/dev; Kubernetes implementation (one
  Deployment or Job + Service + ConfigMap/Secret per job) for prod. The
  controller never assumes which — only the interface.

- **D7 — Job identity & lifecycle.** `JobID` (stable, user- or system-assigned)
  and `RunID` (one per attempt). Lifecycle state machine adapted from Flink:
  ```
  SUBMITTED → DEPLOYING → RUNNING → { FINISHED | FAILED | CANCELED }
                             │
                             ├─ FAILED → (restart policy) → RESTARTING → DEPLOYING
                             ├─ SAVEPOINTING → SUSPENDED
                             └─ CANCELLING → CANCELED
  ```
  Restart policy per job (none | fixed-delay(n, backoff) | failure-rate), mirrored
  from Flink. Delivery guarantee (from `compiler.DeliveryGuarantee`) decides
  whether a restart is safe/recoverable and is surfaced prominently in the UI.

- **D8 — Controller is stateful and HA-optional.** The job store (SQLite for
  single-node dev, Postgres for prod) is the source of truth for job/run state,
  so the controller can crash and reconcile running containers on restart
  (compare store vs backend, adopt orphans, restart the lost). Start
  single-instance; leader election is a later concern.

---

## 4. Components & interfaces

### 4.1 Control agent (in the job container)
Extend `observability/dashboard.Server` into a `jobagent` that wraps a
`compiler.CompiledWorkflow` (or an SDK-built env) and serves:
- `GET /healthz` — liveness/readiness.
- `GET /describe` — `env.DescribeJSON()` + graph + delivery guarantee.
- `GET /metrics` — the existing Prometheus registry (already wired).
- `GET /state` — structured job state: phase, records in/out, current
  checkpoint id, last checkpoint time, uptime, last error.
- `POST /cancel` — cancel the run context (graceful).
- `POST /savepoint?label=…` — trigger a savepoint, return its handle.
The agent owns the `Execute` goroutine and translates its outcome into a
terminal state the controller can read.

### 4.2 Controller service (`cmd/mailer-controller`)
- REST API: `POST /jobs` (submit), `GET /jobs`, `GET /jobs/{id}`,
  `POST /jobs/{id}/cancel`, `POST /jobs/{id}/savepoint`,
  `POST /jobs/{id}/restart?fromSavepoint=…`, `GET /jobs/{id}/logs` (stream),
  `GET /jobs/{id}/metrics` (proxy the agent).
- Job store, lifecycle state machine, restart policy, reconciler loop.
- `ContainerBackend` + `CheckpointStorage`/`SavepointStore` wiring.
- Serves the web UI.

### 4.3 Generic runner image (`mailer-runner`)
A minimal image: the `mailer-runner` binary (the control agent + engine),
`ENTRYPOINT` reads a `WORKFLOW` (path/inline) + `DATA_DIR` + secret env, compiles
via `workflow/runner.CompileFile`, starts the agent, runs `Execute`, exits with a
status the backend reports. Health/metrics/control on a fixed port.

### 4.4 Web UI (SPA served by the controller)
- **Jobs list**: name, state (color-coded), delivery guarantee, uptime,
  records/s, restart count. Submit button.
- **Job detail**: the pipeline graph (source → operators → sink, from
  `PipelineGraph`), live throughput + backpressure from `mailer_edge_*` /
  `mailer_stage_send_block_seconds_total` (an edge at capacity flags the
  bottleneck — Mailer already exposes this), checkpoint history + sizes,
  logs tail, and Cancel / Savepoint / Restart actions.
- **Submit**: paste/upload a workflow, set name + restart policy + secrets
  (names only; values via the controller's secret backend), dry-run
  (compile-only) preview showing the graph + guarantee before launch.

Take visual cues from Flink's dashboard (job graph, checkpoint timeline,
backpressure heat) but keep it Mailer-shaped.

---

## 5. Phases (locked)

Each phase is independently shippable, ends at a **gate** (a demoable
capability + green tests), and only depends on phases below it. Build order is
top-to-bottom; P4/P5 can proceed in parallel once P3 lands; P6/P7 are additive.

```
  P1 control agent ───┐
                      ├──► P3 controller (Docker) ──┬──► P4 durable ckpt + savepoints
  P2 runner image ────┘         │                   ├──► P5 web UI
                                │                   ├──► P6 Kubernetes backend
                                └───────────────────┴──► P7 SDK jobs
     (P1+P2+P3 = first usable slice: submit/run/manage YAML jobs locally)
```

### Module boundary (D9 — locked)

Keep the core engine dependency-light. Anyone doing `go get
github.com/ASHUTOSH-SWAIN-GIT/mailer` must **not** pull Docker/K8s/SQLite
clients. Therefore:

- **Core module** (existing `go.mod`): engine + `jobagent` (only `net/http` +
  engine) + `cmd/mailer-runner`. The runner needs the engine, nothing heavier.
- **Separate module** `control/` (own `go.mod`, in-repo): controller, container
  backends, job store, web UI, `cmd/mailer-controller`. This is where the heavy
  Docker/K8s/SQLite deps live, quarantined from library users.

---

### P1 — Control agent  *(no containers; pure Go, testable today)*

**Goal.** An in-process supervisor that owns one job's lifecycle and exposes it
over HTTP. This is the contract every runner (YAML or SDK) embeds.

**Deliverables** (core module, new pkg `jobagent/`):
- `agent.go` — `Agent` wraps a built `*mailer.StreamExecutionEnv`; `Run(ctx)`
  launches `Execute` in a goroutine and tracks lifecycle transitions.
- `state.go` — `State{Phase, StartedAt, RecordsIn, RecordsOut, CurrentCheckpointID,
  LastCheckpointAt, LastError, Uptime}`; `Phase` enum
  (`Starting|Running|Cancelling|Finished|Failed`).
- `http.go` — `GET /healthz`, `GET /describe` (`env.DescribeJSON()`),
  `GET /metrics` (reuse `promhttp`), `GET /state`, `POST /cancel`,
  `POST /savepoint` (stub in P1: returns 501 until P4).
- Small engine hook: an optional status listener the checkpoint coordinator
  calls on each completed checkpoint (feeds `CurrentCheckpointID` /
  `LastCheckpointAt`) — reuse the existing dashboard update path if present.

**Gate.** Unit test drives a real in-process workflow, polls `/state` mid-run,
`POST /cancel` triggers graceful shutdown, terminal `Phase` is correct. No
container involved.

---

### P2 — Generic runner image  *(YAML job in a container, by hand)*

**Goal.** A prebuilt image that turns a mounted YAML doc into a running,
supervised job — the "engine reads the YAML" path from §2.5.

**Deliverables** (core module):
- `cmd/mailer-runner/main.go` — reads env (`WORKFLOW` path, `DATA_DIR`, `PORT`,
  secrets via env), `runner.CompileFile` → `jobagent.Agent.Run`, `SIGTERM` →
  graceful cancel. State/checkpoint dirs derived under `DATA_DIR` (mounted vol).
- `Dockerfile.runner` — multi-stage: static build → distroless. Non-root.
- A short "run locally" recipe (`docker run -v … -e WORKFLOW=…`).

**Gate.** Run a YAML job in Docker by hand; `docker kill --signal=SIGTERM`;
restart with the **same volume**; confirm checkpoint recovery resumed (records
continue, not replay-from-zero).

---

### P3 — Controller, Docker backend  *(first usable slice)*

**Goal.** Submit / list / inspect / cancel / restart YAML jobs through one API.

**Deliverables** (new `control/` module):
- `store/` — `Store` interface + SQLite impl; tables `jobs`, `runs`,
  `transitions`. Persists the **submitted YAML doc**, derived
  `compiler.PipelineGraph`, and `compiler.DeliveryGuarantee` (never secrets).
- `backend/` — `ContainerBackend` interface (`Launch/Stop/Status/Logs`) +
  `docker.go` (Docker SDK): launches the P2 image with the doc as a
  config-mount + a per-job data volume + resolved-secret env.
- `lifecycle/` — state machine
  (`Submitted→Starting→Running→{Cancelling→Cancelled|Failed|Finished}`) +
  restart policy.
- `api/` — REST: `POST /jobs`, `GET /jobs`, `GET /jobs/{id}`,
  `POST /jobs/{id}/cancel`, `POST /jobs/{id}/restart`, `GET /jobs/{id}/logs`,
  `GET /jobs/{id}/metrics` (proxy to the agent). Submit path runs a dry-run
  compile first (`runner.CompileFile`) and rejects invalid jobs before any
  container starts.
- `reconcile.go` — loop reconciling store desired-state vs backend actual-state
  (adopt/restart/mark-failed).
- `cmd/mailer-controller/main.go`.

**Gate.** Full `submit → run → cancel → restart` of a YAML job through the API,
against local Docker; state survives a controller restart (store is source of
truth). Integration test gated on Docker availability.

---

### P4 — Durable checkpoints + savepoints  *(production data safety)* — DONE

**Goal.** Operator-triggered savepoints for safe stop/upgrade/restart; the
portable durability mechanic + fencing that keep exactly-once intact.

**Scope decision.** The durability **mechanic** (tar per-owner state dirs on
save / untar on restore) is built against an abstract `checkpoint.Blobstore`
with a filesystem impl. The actual **S3/GCS client is deferred to P6** — in the
local-Docker model checkpoints already survive on the per-job named volume, and
an AWS SDK in the core engine would break the dep-light-core rule (D9). The S3
adapter is a drop-in `Blobstore` implementation later.

**Delivered:**
- `checkpoint.Blobstore` + `FileBlobstore` (atomic writes, traversal guard) —
  the S3 drop-in point.
- `ArchiveCheckpoint` / `ExtractCheckpoint` — tar a checkpoint's JSON + Pebble
  state dirs into one portable stream and back (a real copy, not the local
  hard-link). `CreateSavepoint` / `RestoreSavepoint` / `ListSavepoints`.
- Stop-with-savepoint handshake: agent `POST /savepoint?label` (drain + final
  checkpoint) → runner promotes to `savepoints/<label>` in a shared blobstore
  volume; `RESTORE_SAVEPOINT` seeds a fresh run before Execute. Controller
  `Savepoint` + `RestartFromSavepoint`; API `POST /jobs/{id}/savepoint` and
  restart body `{"savepoint":"<label>"}`.
- Fencing: **single-live-run guard** in the controller (never two live
  transactional producers for one job); `MAILER_JOB_ID` injected so authors pin
  `transactionalID: ${MAILER_JOB_ID}`; the stored spec's id is reused verbatim
  on restart.
- `compiler.CheckpointDir` exposed on `CompiledWorkflow` so the runner seeds the
  right storage; engine checkpoint-listener already added in P1.

**Gate.** ✅ Savepoint → restart-from-savepoint round-trips state end-to-end on
Docker (restored reduce count exceeds a single run's total → carryover proven);
unit tests isolate savepoint restore into a *fresh* storage (the cross-job
case). Exactly-once fencing (single-live-run guard, stable txn id) is
unit-tested; full Kafka `cancel → restart` no-dup/no-loss is the same
Kafka-in-Docker deferral as P2 — the engine's exactly-once recovery is covered
by `test/unit_tests/exactly_once_test.go` + `recovery_test.go`.

---

### P5 — Web UI  *(operate from the browser)* — DONE

**Goal.** Everything the API does, visually — an industrial control-room
dashboard in the mailer "field-manual" identity.

**Scope decision.** Live updates use **polling** (list every 2s, detail every
1.6s) rather than SSE — simpler, self-contained, no server push code, and the
`/state` + `/logs` proxies already exist. SSE is a later upgrade if needed.

**Delivered** (in `control/ui`, one self-contained `index.html` embedded via
`embed.FS` — inline CSS/JS, no build step, no external assets):
- Jobs list with phase LEDs (running pulses), delivery badges, op count.
- Job detail: pipeline graph rendered from `PipelineGraph` (source/sink
  accented), **live panel** (phase, records in/out, uptime, current
  checkpoint) fed by the proxied agent `/state`, lifecycle transition log, and
  a tailing log panel. Cancel / restart / **savepoint** / restart-from-savepoint
  actions, disabled correctly on terminal jobs.
- Submit form with **dry-run preview**: `POST /validate` compiles without
  launching and shows the graph + delivery guarantee before Deploy.
- Backend additions: `Controller.Validate`, `POST /validate`, and `GET /jobs`
  enriched with each job's latest-run phase. UI served at `GET /{$}`.

**Gate.** ✅ Verified in a real browser: submitted a workflow via the form
(preview → deploy), watched it on the live list + detail, and drove
cancel/restart/savepoint — no curl. API-level tests cover `/validate`, the
enriched list, and UI serving; screenshots in `.playwright-mcp/shots/`.

---

### P6 — Kubernetes backend  *(cluster deployment)*

**Goal.** A second `ContainerBackend` so the same jobs run on K8s.

**Deliverables** (in `control/backend/kubernetes.go`):
- Per job: Deployment (or Job) + Service + ConfigMap (workflow doc) + Secret
  (resolved creds) + PVC (Pebble state + checkpoints on one volume).
- Agent `/healthz` wired to readiness/liveness probes; reconcile against the
  K8s API.
- Docs note: PVC placement + HPA caveat for partition-scaled jobs.

**Gate.** Submit/run/manage a job on kind/minikube through the same API.

---

### P7 — SDK jobs  *(the "engine reads the Go code" path, §2.5)*

**Goal.** Compile a user's Go pipeline into a job image, then manage it exactly
like a YAML job.

**Deliverables:**
- Builder contract: `func Build(env *mailer.StreamExecutionEnv) *mailer.Stream`
  + a `mailer.yaml` manifest (module path, builder symbol, runtime config).
- Build pipeline: render the platform-owned harness `main()` around the user's
  builder → `go build` user pkg + harness + module → image → push → run (same
  lifecycle thereafter).
- Supply-chain guardrails: pinned deps, sandboxed build, signed images. **v1
  may require a user-provided repo/Dockerfile** rather than compiling loose
  source.

**Gate.** Submit a Go SDK job (repo + `mailer.yaml`) → build → run → cancel →
restart, identical to a YAML job downstream.

---

**Sequence recap.** Ship **P1→P2→P3** as the first usable slice (submit/run/
manage YAML jobs locally). Then **P4 + P5** in parallel for production
credibility. Then **P6** (scale-out) and **P7** (SDK authoring) as additive
backends — neither blocks the other.

---

## 6. Risks & open questions

1. **SDK job builds (P7) are the hard part.** Compiling arbitrary user Go into a
   trusted image is a supply-chain surface (dependency pinning, sandboxed build,
   image signing). Consider constraining v1 to "user provides a Dockerfile/repo
   we build" rather than "we compile a loose main.go." YAML jobs sidestep this
   entirely — hence YAML-first.
2. **Scale-out is not Flink-style.** One logical job cannot spread one operator
   across nodes. Horizontal scale = N containers as Kafka consumer-group members
   over a partition subset. The UI and docs must state this plainly, or users
   will expect rescale-by-parallelism.
3. **Exactly-once across restart** needs *both* a persistent checkpoint location
   *and* a stable `transactionalID` pinned to the JobID — and a running-instance
   guard so a zombie old container doesn't double-produce (Kafka fencing helps,
   but the controller must not launch a second live run of the same job).
4. **Durable state (Pebble) in containers.** Pebble state dirs must be on the
   same persistent volume as checkpoints (native hard-link checkpoints require
   same-filesystem). K8s PVC placement matters; document it.
5. **Secret handling.** Secrets reach the job as env (`${VAR}` resolution
   already exists); the controller needs a secret backend (env / K8s Secret /
   Vault) and must never persist secret *values* in the job store or logs (the
   compiler already scrubs them from descriptions).
6. **Controller HA.** Single-instance first; the reconciler-from-store design
   makes it crash-safe, but concurrent controllers need leader election — out of
   scope for v1.
7. **Log aggregation.** v1 tails container stdout via the backend; centralized
   logging (Loki/ELK) is out of scope but the UI should not assume local logs
   forever.

---

## 7. Out of scope (v1)

- Distributed data plane / cross-node operator parallelism (contradicts the
  engine's single-process design).
- Multi-tenant auth/RBAC beyond a basic API token.
- Autoscaling logic (surface metrics; leave scaling decisions to the operator /
  HPA).
- A job DSL beyond the existing YAML workflow schema and Go SDK.
