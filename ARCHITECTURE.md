# Weibo — Architecture

Weibo is a **stream-processing engine in Go, inspired by Apache Flink**. It reads
unbounded streams (Kafka), applies stateful/windowed transformations, and writes
to sinks — with checkpointing, durable state, and up to end-to-end exactly-once.

Unlike Flink, it is **not a cluster runtime**. The engine is a single-process,
multi-goroutine library you import. A separate **control plane** (its own Go
module) then runs *many* such jobs, one container each, on Docker or Kubernetes.

This document is organized as a set of **views**, each meant to map onto one
diagram:

1. [Component view](#view-1--components-3-tiers) — the three tiers, static.
2. [Runtime dataflow](#view-2--runtime-dataflow-inside-one-job) — goroutines,
   channels, and the marker stream inside one running job.
3. [Stage internals](#view-3--stage-internals) — how a keyed / parallel stage
   fans out and re-aligns.
4. [Marker flow & checkpoint protocol](#view-4--marker-flow--the-checkpoint-barrier)
   — how a barrier snapshots a consistent cut.
5. [Exactly-once two-phase commit](#view-5--exactly-once-two-phase-commit-sequence)
   — the coordinator sequence.
6. [State & checkpoint storage](#view-6--state--checkpoint-storage-layout).
7. [Control plane](#view-7--control-plane-components--sequences) — components +
   submit / reconcile / proxy sequences.
8. [Recovery](#view-8--recovery-decision) — the restart decision table.

---

## View 1 — Components (3 tiers)

Layered; each tier depends only on the ones below it.

```
┌───────────────────────────────────────────────────────────────────────┐
│  TIER 3 · CONTROL PLANE            control/  (separate Go module)      │
│                                                                       │
│   cmd/weibo ──> Controller ──> ContainerBackend {Docker | K8s | fake}│
│      (CLI)          │  ▲              │ launches                       │
│                 api/│  │store/        ▼                                │
│              REST + UI  SQLite    one container per job                │
│                     └── reconcile loop (desired ⇄ actual)             │
└───────────────────────────┬───────────────────────────────────────────┘
                            │ starts container image, env-configured
┌───────────────────────────▼───────────────────────────────────────────┐
│  TIER 2 · JOB HARNESS     sdk/ · jobagent/ · workflow/ · cmd/          │
│                                                                       │
│   cmd/weibo-runner (YAML)  ─┐                                         │
│   any main(){sdk.Run(Build)} ─┼─> sdk.Serve ──> jobagent.Agent        │
│   (Go SDK job)                │                    │  supervises 1 env │
│                               │        HTTP control surface (:PORT):   │
│                               │        /healthz /state /describe       │
│                               │        /metrics /cancel /savepoint     │
│   workflow/{parse,validate,compiler} builds the env for the YAML path  │
└───────────────────────────┬───────────────────────────────────────────┘
                            │ StreamExecutionEnv (source+ops+sink wired)
┌───────────────────────────▼───────────────────────────────────────────┐
│  TIER 1 · CORE ENGINE     weibo.go · pipeline/ · operator/ · state/ … │
│                                                                       │
│   Source ─> [stage] ─edge─> [stage] ─edge─> … ─> Sink                 │
│   planner · edges (backpressure) · keyed parallelism · watermarks      │
│   checkpoint coordinator · state backends (memory | Pebble)            │
└───────────────────────────────────────────────────────────────────────┘
```

- **Why the split?** The core engine stays dependency-light — `go get weibo`
  never pulls Docker/Kubernetes/SQLite clients; those live only in `control/`.
- **One job = one container = one OS process.** No cross-node coordination inside
  a job. Scale-out = run more jobs (Kafka partitions distribute across the
  consumer group).

---

## View 2 — Runtime dataflow inside one job

When `env.Execute(ctx)` runs, the **planner** (`pipeline/planner.go`) groups the
operator chain into an ordered list of **stages**, wires **bounded edges**
between them, and starts **one goroutine per stage**. This is the diagram to draw
for "how a running pipeline actually looks."

```
                     ctx (graceful)          hardCtx (force-unwind on fatal error)
                         │                          │
  ┌───────────────┐  edge-0   ┌──────────────┐  (internal ch)   ┌───────────────┐
  │ SourceStage   │─────────▶ │ injectBarriers│───────────────▶ │ Stage 1       │
  │ goroutine     │  Records  │  goroutine    │  Records+Barrier │ stateless/keyed│
  │               │  + Water- │               │  + Watermarks    │ goroutine(s)  │
  │ source.Run()  │  marks    │ • per-partition│                 └──────┬────────┘
  │ (WatermarkSrc │           │   offset track │                        │ edge-1
  │  wraps Kafka) │           │ • barrier on    │                        ▼
  └───────────────┘           │   ticker        │                 ┌───────────────┐
        ▲                     └──────┬──────────┘                 │ Stage 2 …     │
        │ FetchMessage               │ registerAlignedOffsets     └──────┬────────┘
   Kafka broker                      ▼ (id → {partition:offset})         │ edge-(n-2)
                          ┌────────────────────┐                         ▼
                          │ Coordinator (E1)   │◀── OnStateSnapshot ┌───────────────┐
                          │ finalize goroutine │◀── OnSinkPrepared  │ SinkStage     │
                          └────────────────────┘   (barrierDetect)  │ goroutine     │
                                                                    │ sink.Write()  │
                                                                    └──────┬────────┘
                                                                           ▼
                                                                     Kafka / Postgres
```

Key facts for the diagram:

- **Nodes = goroutines.** Each stage is its own goroutine (`stage.Run(runCtx,
  hardCtx, in, out)`). `injectBarriers` and the coordinator's `finalize` are
  their own goroutines too. A keyed/parallel stage internally spawns *more*
  goroutines (View 3).
- **Edges = the only channels between stages** (`pipeline/edge.go`), bounded
  (default capacity 1024, `WithBufferSize`). `edge-i` connects stage `i` → stage
  `i+1`. A send to a full edge blocks → **backpressure** propagates upstream to
  the source, which stops calling `FetchMessage`. Zero drops, bounded memory.
- **Inside a single stage, operators are direct function calls** — no channels.
  Consecutive stateless ops (Map/Filter/FlatMap/Process) at the same parallelism
  fuse into one `StatelessStage`.
- **Two contexts:** `runCtx` = graceful stop (source stops, everything drains);
  `hardCtx` = force-unwind, cancelled on any fatal stage/coordinator error or
  when the `WithShutdownTimeout` drain deadline passes.
- **`injectBarriers` sits between the source and stage 1** (`weibo.go`). It reads
  the source's output, tracks `offsets[partition] = record.Offset+1` for every
  data record (the *barrier-aligned* offset map), and on each checkpoint tick
  injects a **barrier** record after registering that offset snapshot.

### The record stream carries three kinds of item (`types/record.go`)

Everything flows through the same `chan types.Record`:

| Kind | Flag | Origin | Consumed by |
|------|------|--------|-------------|
| **Data** | (none) | Source | operators |
| **Watermark** | `IsWatermark` | `WatermarkSource` at the source | Window (fires closed windows), passes through |
| **Barrier** | `IsBarrier` + `CheckpointID` | `injectBarriers` goroutine | stateful ops (snapshot), sink (commit), coordinator |

A data `Record` = `Key []byte`, `Value []byte`, `Timestamp`, `Offset`,
`Partition`, `Headers map[string][]byte`.

---

## View 3 — Stage internals

Two stage types fan out; draw each as a sub-diagram.

### Keyed stage (`pipeline/keyed_stage.go`) — starts at `KeyBy`

```
                       ┌── workerIn[0] ─▶ Worker 0: Op₀ᶜˡᵒⁿᵉ→Op₁ᶜˡᵒⁿᵉ ─▶ workerOut[0] ──┐
   in ──▶ Router ──────┼── workerIn[1] ─▶ Worker 1: Op₀ᶜˡᵒⁿᵉ→Op₁ᶜˡᵒⁿᵉ ─▶ workerOut[1] ──┼─▶ alignedMerge ──▶ out
 (goroutine)  │ hash   └── workerIn[N] ─▶ Worker N: Op₀ᶜˡᵒⁿᵉ→Op₁ᶜˡᵒⁿᵉ ─▶ workerOut[N] ──┘   (goroutine)
              │ key→worker                (each a goroutine,                              emits each
              │                            isolated state backend)                        marker ONCE
              └ broadcast barriers+watermarks to ALL workers
```

- **Router** hash-dispatches *data* by key (`same key → same worker`) and
  **broadcasts** barriers + watermarks to every worker.
- **Workers** each own a *clone* of every stateful operator (`Cloneable`) with its
  own `StateBackend`, registered under the checkpoint owner id `worker-<idx>`.
- **`alignedMerge`** re-serializes worker outputs and emits each marker
  **exactly once**, only after *all* N workers have delivered it — so a barrier
  can never be overtaken and the snapshot is a consistent cut at any parallelism.

### Parallel stateless stage (`pipeline/stateless_stage.go`) — `WithParallelism(n)`

Same shape, but the dispatcher is **round-robin** (stateless, order across
workers not preserved) and workers hold no keyed state. `Parallelism == 1` is the
degenerate case: `in → runWorker → out` with no fan-out.

---

## View 4 — Marker flow & the checkpoint barrier

A **checkpoint** is a consistent snapshot of *(all source-partition offsets)* +
*(all operator state)*. It is taken by flowing a **barrier** through the graph
(Chandy-Lamport). Draw this as a swimlane: barrier position over time.

```
 injectBarriers        Stage 1            Keyed stage             barrierDetect        SinkStage
 ──────────────        ───────            ───────────             ─────────────        ─────────
 tick →                                                                               
 registerAligned                                                                      
   Offsets(id) ──┐                                                                     
 emit ▮barrier   └▶ …data… ▮ ─▶ snapshot ▮ ─▶ broadcast ▮ to workers                   
                            op state       each worker snapshots (worker-<idx>)        
                            (op-<i>)        alignedMerge emits ▮ once                   
                                                     └────────────▶ collectSnapshots(id)
                                                                    → coord.OnState-    
                                                                       Snapshot ──┐     
                                                                                  └▶ ▮ reaches sink
                                                                                     → produce marker
                                                                                       in open txn,
                                                                                       Flush, onPrepared
```

Mechanics:

- A **stateful operator** implements `BarrierSnapshotter`: the instant the barrier
  passes through its `Process` loop (between two data records — the race-free
  point), it snapshots its state synchronously and hands it back via a callback,
  keyed `op-<i>` (top-level) or `worker-<idx>` (keyed clone).
- **Native vs inline snapshot:** if the operator's backend is `Checkpointable`
  (Pebble), it hard-links its LSM files and returns a small `state_ref` marker
  (checkpoint cost ∝ *changed* data); otherwise it serializes state to JSON
  inline.
- **Offsets are barrier-aligned:** `injectBarriers` registered the exact
  per-partition offset map *before* emitting the barrier, so the snapshot covers
  precisely the records ahead of the barrier. (`CheckpointOffset()` on the source
  is only a fallback, and tracks per-partition consumed offsets — never
  `reader.Stats()`, which would drop all-but-one partition in a consumer group.)

---

## View 5 — Exactly-once two-phase commit (sequence)

Only when `KafkaSource(exactly-once) → TxnKafkaSink + WithCheckpointing`. The
`checkpoint.Coordinator` (`checkpoint/coordinator.go`) drives it. Draw as a
sequence diagram across: **injectBarriers · operators · TxnKafkaSink · Coordinator
· Storage · Kafka broker**.

```
 injectBarriers   operators        TxnKafkaSink          Coordinator            Storage        Broker
 ─────────────    ─────────        ────────────          ───────────            ───────        ──────
 OnBarrierInjected(id, offsets) ─────────────────────────▶ pending[id].offsets                 
   emit ▮ ──▶ snapshot ▮ …                                                                      
                        collectSnapshots(id) ────────────▶ OnStateSnapshot(id, state, dirs)     
   ▮ ─────────────────────────────▶ produce marker (in txn)                                      
                                    Flush() ──────────────────────────────────────────────────▶ (staged, invisible)
                                    OnSinkPrepared(id,err)▶ events<-id                            
                                                          finalize(id):                          
                                                          1. Save(prepared) ─────▶ fsync file    
                                                          2. CommitSink(id) ──▶ EndTxn(TryCommit) ▶ (marker+data visible)
                                                          3. UpdateStatus(completed) ▶ file       
                                                          4. CommitOffsets ──────────────────────▶ (advisory group commit)
                                                          signal ▶ open next txn
```

- **The checkpoint file is the transaction log.** `prepared` is written *before*
  the sink commit; `completed` *after*. A crash between step 2 and step 3 is
  resolved on recovery by the **marker probe** (`WasCommitted`): the marker record
  rides *inside* the transaction, so its `read_committed` visibility proves the
  transaction committed.
- **Concurrency safety:** `OnSinkPrepared` runs on the sink's write goroutine and
  hands the id to the coordinator's `finalize` goroutine via a channel; a
  send-WaitGroup makes the `Stop`/`close` race-free. A per-transaction
  `produceErr` is reset on Commit/Abort so one transient error can't poison later
  checkpoints.
- **Uncoordinated mode** (any other sink, checkpointing on): no coordinator. The
  `SinkStage.OnBarrier = saveCheckpoint` fires *after* the sink drains everything
  ahead of the barrier → exactly-once **state**, at-least-once **output**.

| Configuration | Guarantee |
|---|---|
| `KafkaSource(exactly-once)` → `TxnKafkaSink` + checkpointing | **End-to-end exactly-once** |
| Any source → plain sink, checkpointing on | Exactly-once **state**, at-least-once **output** |
| No checkpointing | At-most-once across restarts |

---

## View 6 — State & checkpoint storage layout

```
StateBackend (per stateful operator / per keyed worker)
├── InMemory()  — map[namespace]map[key][]byte in RAM; serialized into checkpoints
└── Pebble(dir) — LSM on disk; state bounded by disk, not heap
        ValueState(namespace)  key → []byte        (Reduce accumulator, watermark)
        ListState(namespace)   key → [][]byte      (Window buffered records)

Checkpoint file storage (checkpoint/, FileStorage)
  <dir>/checkpoint-<id>.json          # {offsets, operator snapshots|state_refs, status}
  <dir>/checkpoint-<id>.state/        # native hard-link dir (Pebble), per owner
        op-<i>/ | worker-<idx>/
  status pointer: prepared → completed   (atomic write+rename, fsync)
  savepoints/<label>                   # promoted checkpoint (shared volume / S3-ready)
```

- **Windowing** stores records in `ListState` keyed by `"<recordKey>/<start>/<end>"`
  and the watermark in `ValueState`; the set of open windows is exactly the key
  set. `WindowReduce` folds a window's records at fire and emits one aggregate,
  evicting the records (memory bounded by *open* windows).
- **Recovery restores symmetrically:** both `op-<i>` (top-level ops, incl.
  non-keyed Window/Reduce) and `worker-<idx>` (keyed clones), each via native
  `RestoreFrom` when the backend is `Checkpointable`, else inline `Restore`.

---

## View 7 — Control plane: components & sequences

### Components (`control/`, separate module)

```
        HTTP (REST + embedded SPA)
   client ──▶ api/  ──▶  Controller  ──▶  store/  (SQLite: jobs, runs, transitions)
                (root)      │  ▲                    source of truth; NO secrets
                           │  └── secrets: in-memory map[jobID]→env only
                           ▼
                     ContainerBackend  (backend/)
                      ├── Docker      one container/job, per-job volume
                      ├── Kubernetes  one batch/v1 Job + PVC + ConfigMap + Secret + Service
                      └── fake        in-memory (tests)
                     lifecycle/  phase state machine + restart policy
```

### Submit sequence (`POST /jobs`)

```
client ─POST /jobs (YAML|manifest)─▶ api ─▶ Controller.Submit
  1. dry-run compile (workflow/compiler)   ── validates; Postgres sink opens a pool
  2. store.CreateJob (spec keeps ${VAR})   ── secrets held in memory only
  3. backend.Launch(LaunchSpec{image, workflowDoc, env, WEIBO_JOB_ID})
        └─▶ container starts: weibo-runner|sdk.Run → jobagent serves :PORT
  4. return {id, graph, delivery}
```

### Reconcile loop (`control/reconcile.go`, ~3s ticker)

```
every tick:
  for run in store.ActiveRuns():
     st = backend.Status(run)          # running | exited | gone
     desired == stopped  → Stop + finishRun(Cancelled)
     exited(0)           → Finished
     exited(≠0)/gone     → restart policy (bounded attempts + backoff) → new run | Failed
  (on controller restart: re-read ActiveRuns, RE-ATTACH to live containers)
```

### Live-state proxy (dashboard ⇄ running job)

```
GET /jobs/{id}/state|metrics|describe ─▶ Controller.ControlAddress(id) ─▶ jobagent :PORT
POST /jobs/{id}/cancel|restart|savepoint ─▶ same proxy (savepoint drains + final checkpoint)
```

- **Exactly-once across restarts** is protected by two rules: *single-live-run
  fencing* (never two containers for one job, so two transactional producers with
  the same id never coexist) and a *stable transactional id* pinned to
  `WEIBO_JOB_ID`.

---

## View 8 — Recovery decision

On startup with checkpointing, `resolveCoordinatedRecovery` picks the state to
restore:

```
latest checkpoint on disk?
├── completed              → restore it
├── prepared + marker visible (WasCommitted == true)  → promote to completed, restore
├── prepared + marker absent (WasCommitted == false)  → abort txn, restore previous completed
└── none                   → start from the source's configured offset
then: restore operator state (op-<i> + worker-<idx>) and seek source offsets
```

`WasCommitted` drains the marker topic under `read_committed`: it returns *true*
immediately on seeing the marker (definitive), and only concludes *absent* after
two consecutive empty polls — never a premature false negative that would replay
a committed transaction.

---

## Package map

```
weibo/
├── weibo.go, stream.go, metadata.go   # env, fluent API, Execute wiring, Describe()
├── types/          # Record (data / watermark / barrier), NewWatermark/NewBarrier
├── pipeline/       # planner, stages (Source/Stateless/Keyed/Sink), edges, markers, metrics
├── operator/       # Map/Filter/FlatMap/Process/KeyBy/Reduce/Window(+WindowReduce)
├── window/         # tumbling / sliding / session assigners
├── watermark/      # bounded out-of-orderness generator
├── state/          # StateBackend: InMemory + Pebble(LSM); Value/ListState; Checkpointable
├── source/         # KafkaSource (+ watermark wrap, offset tracking) · slice/generator
├── sink/           # Kafka · TxnKafka (2PC) · Postgres · stdout · blackhole · failure policy/DLQ
├── checkpoint/     # CheckpointData, FileStorage, Coordinator (2PC), savepoints, archive
├── auth/           # SASL/TLS shared by Kafka source & sink
├── observability/  # Prometheus registry (pipeline/op/worker/stage/edge) + dashboard assets
├── jobagent/       # per-job supervisor (Agent) + HTTP control surface
├── sdk/            # sdk.Run / sdk.Serve — harness shared by SDK and YAML jobs
├── workflow/       # spec · parse · validate · secrets · compiler · operators · record · runner
├── cmd/            # weibo-workflow (dev CLI) · weibo-runner (container entrypoint)
├── control/        # ⟵ SEPARATE MODULE: Controller, reconcile, store, lifecycle, backend, api, ui
├── bench/          # state-backend scaling benchmarks (memory vs Pebble)
├── examples/       # wordcount, windowing, backpressure, exactly-once, kafka-orders, pg-orders …
└── test/           # integration: recovery, exactly-once crash sweep, shutdown-under-load
```

## Key design decisions

- **Single-process engine, multi-job control plane** — embeddable core; optional
  orchestration in a separate module.
- **Barrier-based checkpointing** (Chandy-Lamport) — a snapshot is always a
  consistent cut because a barrier can't be overtaken (aligned merge).
- **Bounded edges for backpressure** instead of credit-based flow control — the
  bottleneck is directly visible in `weibo_edge_queue_size`.
- **Barrier-aligned offsets are authoritative**; the source's `CheckpointOffset`
  is a per-partition fallback.
- **YAML and Go SDK jobs share one lifecycle** (`sdk.Serve` + `jobagent`), so the
  control plane treats them identically.
- **Store is the source of truth; secrets are never persisted** — the controller
  re-attaches after a crash; `${VAR}` placeholders keep credentials out of SQLite.
- **`segmentio/kafka-go`** (source) and **`franz-go`** (transactional sink) — pure
  Go, no CGO.
```
