# P0 — Correctness Work

Two items, in order. Item 1 is a standalone bug fix that ships first — it
repairs checkpoint consistency in the code as it exists today. Item 2 is the
execution-model refactor that everything else (joins, typed streams, durable
state) will sit on.

---

# P0-1: Barrier Broadcast + Alignment in the Keyed Stage

## The bug

Checkpoints taken with `partitions > 1` are **silently inconsistent**.

Barriers are keyless records. The router in `wireKeyedStage` treats them like
data:

```go
// weibo.go — router goroutine
w := kb.Route(r)
workerIns[w] <- r
```

and `Route()` sends every keyless record to worker 0:

```go
// operator/keyby.go:69
if len(key) == 0 || op.Partitions <= 1 {
    return 0
}
```

So a barrier only ever flows through **worker 0**. When it reaches the end of
the chain, `barrierDetect` fires `saveCheckpoint`, which snapshots **all**
worker operators (`env.workerOps`) — including workers 1..N that never saw the
barrier and still have pre-barrier records in flight. Restore from such a
checkpoint and those workers' state is from an arbitrary point in time
relative to the source offset.

## The fix

Chandy-Lamport alignment: broadcast at entry, align at exit.

**Router** — detect barriers (and watermarks, same problem) and copy to every
worker instead of routing:

```go
case r, ok := <-in:
    if !ok { return }
    if r.IsBarrier || r.IsWatermark {
        for _, ch := range workerIns { ch <- r }   // broadcast
        continue
    }
    workerIns[kb.Route(r)] <- r
```

**Merger** — currently N fan-in goroutines dumping into one channel, which
would forward N copies of each barrier in arbitrary positions. It must emit
each barrier exactly once, only after ALL workers have delivered it:

```go
barrierSeen := map[string]int{}            // CheckpointID → workers delivered
for r := range fanIn {                     // fan-in of all workerOuts
    if r.IsBarrier {
        barrierSeen[r.CheckpointID]++
        if barrierSeen[r.CheckpointID] == n {
            delete(barrierSeen, r.CheckpointID)
            merged <- r                    // emit once — all workers aligned
        }
        continue
    }
    if r.IsWatermark { /* same counting, emit once */ continue }
    merged <- r
}
```

Why this is sufficient: each worker owns a disjoint key set, and a worker
forwards the barrier only after processing everything that arrived before it.
When the merger has collected the barrier from all N workers, every worker's
state reflects exactly the pre-barrier stream — so the snapshot that fires
downstream at `barrierDetect` is consistent. Post-barrier records from
already-aligned workers may pass the held barrier in the merged stream; that
is safe because their state was already captured at their own barrier — no
record buffering needed.

Note the merger restructure: today each worker output has its own goroutine
writing directly to `merged`. The counting logic needs a single serialization
point — either one goroutine doing a select-based fan-in, or keep the N
goroutines writing to an intermediate channel and do the counting in one
consumer loop between it and `merged`.

## Tests

- 4-partition keyed stage: every worker receives every barrier.
- Merged output contains each `CheckpointID` exactly once, and only after all
  4 workers emitted it.
- Kill/restore under load: restored state + source offset replay produces the
  same results as an uninterrupted run (this is the test that fails today).
- Watermarks: emitted once, windows fire identically with partitions 1 vs 4.

## Files

| File | Change |
|------|--------|
| `weibo.go` | Router broadcast; merger alignment counting |
| `weibo_test.go` (or `test/`) | Alignment + restore-consistency tests |

Ship this on its own commit/PR before starting P0-2. The refactor migrates
this exact logic into `pipeline/keyed_stage.go`, so nothing is thrown away.

---

# P0-2: Stage-Based Backpressure

## Current architecture

Every operator gets a bounded 256-cap channel (`weibo.go:160`), and the
`countedRead`/`timedRead` metric wrappers each add another 256-buffer channel
— so one operator hop buffers up to ~768 records.

Problems:
- Hidden buffering (~768 records/hop) adds memory and latency.
- No concept of "execution stage" — operators that should run together are
  artificially separated.
- Backpressure granularity is per-operator, not per-logical-group.
- Metrics wrappers ARE channels — observability and dataflow are entangled.

## Target architecture

Group operators into execution stages; bounded channels only between stages:

```
Stage 1: Source        → [edge 1024] →
Stage 2: Map→Filter    → [edge 1024] →
Stage 3: KeyBy→Win→Red → [edge 1024] →
Stage 4: Map (format)  → [edge 1024] →
Stage 5: Sink
```

Operators within a stage execute as direct function calls — no channels.

## Design constraints

These shape every phase below; violating any of them reintroduces a bug.

- **C1 — Barrier alignment at parallel stages.** Any stage with N > 1 workers
  must broadcast barriers/watermarks to all workers at entry and emit each one
  exactly once at exit, after all N deliver it. This is the P0-1 mechanism,
  applied uniformly (keyed stage AND parallel stateless stages).
- **C2 — Single close owner.** The stage that writes to an edge closes it,
  inside `Run` (`defer close(out)`). The wiring loop never closes edges.
- **C3 — Two-phase shutdown.** Two contexts: `runCtx` (caller's) only stops
  the source; drain proceeds via cascading channel closes. `hardCtx`
  (`WithTimeout(Background, shutdownTimeout)`, started when runCtx cancels) is
  what `sendRecord` selects on — so blocked sends survive graceful drain and
  only abort if drain exceeds the timeout. Never abort sends on `runCtx.Done()`
  or the final shutdown checkpoint loses records.
- **C4 — Stateless parallelism needs an API.** Add
  `stream.Map(fn).WithParallelism(4)` (optional `Parallel { Parallelism() int }`
  interface on operators, default 1). Document that N > 1 does not preserve
  order. Without this, the planner's "parallelism change → new stage" rule has
  nothing to read.
- **C5 — Metrics must survive the wrapper deletion.**
  `RecordsProcessedTotal`, `OperatorLatencySeconds`, and the per-worker
  variants are currently implemented by the wrapper channels this refactor
  deletes. Reimplement them as inline increments in the stage worker loops
  (same names/labels — the dashboard depends on them) before deleting the
  wrappers.

## Phase 1 — `Stage` and `Edge` abstractions

New `pipeline/` package at repo root.

**`pipeline/stage.go`:**

```go
type Stage interface {
    Name() string
    // Run consumes in until closed, writes to out, and closes out before
    // returning (C2). hardCtx only unblocks stuck sends after
    // shutdownTimeout (C3). Returns the first fatal error.
    Run(hardCtx context.Context, in <-chan types.Record, out chan<- types.Record) error
}
```

Concrete types: `SourceStage` (wraps `source.Source`; ignores `in`; watches
`runCtx` from construction), `StatelessStage` (Phase 5), `KeyedStage`
(Phase 6), `SinkStage` (wraps `sink.Sink`; `out` is nil).

**`pipeline/edge.go`:**

```go
type Edge struct {
    Name string
    Ch   chan types.Record
}
func NewEdge(name string, capacity int) *Edge
```

Keep Edge a thin struct — all sends go through one `sendRecord` helper
(Phase 4). Edge exists to carry the name for metrics.

## Phase 2 — Execution planner

**`pipeline/planner.go`:**

```go
func BuildPlan(src source.Source, ops []operator.Operator, snk sink.Sink) ([]Stage, error)
```

Walk the operator list once:

| Rule | Action |
|------|--------|
| Always | Stage 1 is SourceStage |
| Stateless op (Map, Filter, FlatMap, Process) | Append to current StatelessStage if parallelism matches, else new one |
| Parallelism differs (C4) | New StatelessStage |
| `KeyByOperator.IsRouter()` | New KeyedStage; consume following `Cloneable` ops into it (reuse `takeStateful`, `weibo.go:253`) |
| Stateless op after keyed stage | New StatelessStage |
| Always | Last stage is SinkStage |

Validate (error, not panic): `parallelism > 0`, `partitions > 0`, every op
lands in a stage.

Example — `Source → Map → Filter → KeyBy(4) → Window → Reduce → Map(par=4) → Sink`:

```go
[]Stage{
    SourceStage{source},
    StatelessStage{ops: [Map, Filter], parallelism: 1},
    KeyedStage{keyBy, partitions: 4, ops: [Window, Reduce]},
    StatelessStage{ops: [Map], parallelism: 4},
    SinkStage{sink},
}
// edges: len(stages) - 1 = 4
```

## Phase 3 — Wiring in `Execute()`

**File: `weibo.go` at the repo root** (the package lives at the root — there
is no `weibo/` subdir). Replace the per-operator loop (`weibo.go:149-171`):

```go
plan, err := pipeline.BuildPlan(env.source, env.operators, env.sink)
if err != nil { return err }

edges := make([]*pipeline.Edge, len(plan)-1)
for i := range edges {
    edges[i] = pipeline.NewEdge(fmt.Sprintf("edge-%d", i), env.edgeCapacity)
}

hardCtx := ... // C3

var wg sync.WaitGroup
errs := make([]error, len(plan))
for i, stage := range plan {
    var in <-chan types.Record
    if i > 0 { in = edges[i-1].Ch }
    var out chan<- types.Record
    if i < len(edges) { out = edges[i].Ch }

    wg.Add(1)
    go func(i int, st pipeline.Stage, in <-chan types.Record, out chan<- types.Record) {
        defer wg.Done()
        errs[i] = st.Run(hardCtx, in, out)   // Run closes out (C2)
    }(i, stage, in, out)
}
wg.Wait()
return firstNonNil(errs)
```

- No `close(out)` here — Run owns it (C2).
- `wg.Wait()` + per-stage error collection — Execute must not return while
  stages run; first fatal error cancels `hardCtx` so everything unwinds
  (replaces `stageCancel` + `env.stageErr`).
- `injectBarriers` and `barrierDetect`/`saveCheckpoint` stay: inject between
  SourceStage and edge 0; detect on the last edge before SinkStage. Thin
  wrappers inserted by the wiring, not stages.

## Phase 4 — Blocking, context-aware sends

```go
// pipeline/edge.go
func sendRecord(hardCtx context.Context, out chan<- types.Record, r types.Record) error {
    select {
    case out <- r:
        return nil
    case <-hardCtx.Done():
        return hardCtx.Err()
    }
}
```

Full channel = downstream slower = upstream blocks. No `default` branch. Takes
**hardCtx** (C3): graceful shutdown drains; only timeout expiry aborts.

## Phase 5 — Stateless stage execution

First, direct invocation on operators:

```go
// operator/operator.go
type SingleProcessor interface {
    // ProcessOne applies the operator to one record.
    // Returns output records (0 = dropped, 1 = mapped, N = flatmap).
    // Barriers/watermarks are NOT passed here — the stage forwards them.
    ProcessOne(r types.Record) []types.Record
}
```

Implement on Map (`[]types.Record{fn(r)}`), Filter (`nil` or `[r]`), FlatMap
(`fn(r)`), Process. Keep channel-based `Process` until the old path is deleted.

**`pipeline/stateless_stage.go`:**

```go
func (s *StatelessStage) Run(hardCtx context.Context, in <-chan types.Record, out chan<- types.Record) error {
    defer close(out)                                  // C2

    barrierIn := in
    if s.Parallelism > 1 {
        barrierIn = s.dispatch(in)                    // C1: broadcast barriers/watermarks
    }

    var wg sync.WaitGroup
    for w := 0; w < s.Parallelism; w++ {
        wg.Add(1)
        go func(workerIn <-chan types.Record) {
            defer wg.Done()
            for r := range workerIn {
                if r.IsBarrier || r.IsWatermark {
                    s.merger.deliver(r)               // aligned emit (C1)
                    continue
                }
                outs := []types.Record{r}
                for _, op := range s.Operators {
                    var next []types.Record
                    for _, rec := range outs {
                        next = append(next, op.(operator.SingleProcessor).ProcessOne(rec)...)
                        // C5: increment RecordsProcessedTotal / observe latency here
                    }
                    outs = next
                    if len(outs) == 0 { break }       // dropped — emit nothing
                }
                for _, rec := range outs {
                    if err := sendRecord(hardCtx, out, rec); err != nil { return }
                }
            }
        }(...)
    }
    wg.Wait()
    return nil
}
```

Two correctness details baked in: a Filter drop yields `len(outs) == 0` and
nothing is sent; FlatMap fan-out is a slice loop, not a single result
variable. Fast path: `Parallelism == 1` skips dispatcher/merger entirely —
barriers flow inline in order.

## Phase 6 — Keyed stage execution

**`pipeline/keyed_stage.go`** — migrate `wireKeyedStage` (`weibo.go:274`)
into a `KeyedStage` implementing `Stage`. Structure unchanged (router → N
workers with cloned operators → merger); the router broadcast and merger
alignment are exactly the P0-1 fix, carried over. Keep: per-worker `Clone()`,
panic→error recovery, `env.workerOps` registration for checkpointing,
per-worker metrics inlined (C5).

## Phase 7 — Backpressure propagation

No code — a property to verify:

```
Slow sink → edge 4 fills → stage 4 blocks in sendRecord → edge 3 fills
→ ... → edge 0 fills → SourceStage blocks → Kafka source stops fetching
```

Kafka is the durable backlog: no drops, bounded memory. Confirm the Kafka
source's `Run` blocks on channel send (it writes to a bounded channel, so it
should) with the fast-source/slow-sink test below.

## Phase 8 — Configuration

```go
func (env *StreamExecutionEnv) WithBufferSize(n int) *StreamExecutionEnv  // edge capacity, default 1024
func (s *Stream) WithParallelism(n int) *Stream                           // C4, default 1
```

Validate `n > 0` in Execute. Document on `WithParallelism`: order not
preserved across workers.

## Phase 9 — Graceful shutdown (two-phase, C3)

1. Caller cancels `runCtx`.
2. SourceStage stops producing; `injectBarriers` emits the final shutdown
   barrier; source channel closes.
3. Channel closes cascade: each stage's `for range in` ends, workers drain,
   `Run` closes `out` (C2), next stage sees EOF.
4. The shutdown barrier — broadcast and aligned like any other (C1) — reaches
   `barrierDetect`; the final checkpoint captures fully-drained state.
5. Sink drains and returns; `wg.Wait()` returns.
6. Only if 3–5 exceed `shutdownTimeout` does `hardCtx` fire and abort blocked
   sends. Only this path loses records.

## Phase 10 — Checkpoint barriers via edges

Barriers are `types.Record{IsBarrier: true}` and flow through the same edges
as data — no separate control channel. The ordering guarantee comes from C1:
serial stages forward barriers inline (all operators already do); parallel
stages broadcast at entry and align at exit. A barrier emitted downstream
means every upstream worker has processed all pre-barrier records for its
partition — which is what `saveCheckpoint` snapshotting `env.workerOps`
requires. `barrierDetect` + `saveCheckpoint` stay as-is.

## Phase 11 — Observability

Per-edge (sampled by a 1s ticker):

```
weibo_edge_queue_size{edge}          = len(edge.Ch)
weibo_edge_queue_capacity{edge}      = cap(edge.Ch)
weibo_edge_send_block_seconds{edge}  — time blocked in sendRecord
```

Per-stage:

```
weibo_stage_records_in_total{stage,type}
weibo_stage_records_out_total{stage,type}
weibo_stage_errors_total{stage}
weibo_stage_workers{stage}
```

Existing per-operator metrics: reimplemented inline (C5), same names/labels.
Delete `countedRead`/`timedRead`/`workerCountedRead`/`workerTimedRead` only
after that lands.

## Phase 12 — Tests

- **Planning:** Map+Filter → one stage; KeyBy starts KeyedStage; Window+Reduce
  join it; op after keyed stage → new stage; parallelism change splits stages.
- **Backpressure:** fast source + slow sink → edges fill, memory bounded, no
  drops, throughput ≈ sink rate.
- **Barrier alignment (C1):** stateless parallelism=4 — barrier reaches all
  workers, emitted once; checkpoint under load is consistent (replay test).
- **Concurrency:** same key → same worker, same-key order preserved,
  different keys concurrent.
- **Shutdown (C3):** cancel under load — records ahead of shutdown barrier
  not lost, final checkpoint complete; drain past shutdownTimeout — sends
  abort, no goroutine leaks (`runtime.NumGoroutine()`); edges closed once (C2).
- **Failure:** worker panic → stage error → pipeline cancels; full edges are
  backpressure, never errors.

## Files

| File | Action |
|------|--------|
| `pipeline/stage.go` | New — Stage interface, SourceStage, SinkStage |
| `pipeline/edge.go` | New — Edge + sendRecord |
| `pipeline/planner.go` | New — BuildPlan |
| `pipeline/stateless_stage.go` | New — worker pool + C1 alignment |
| `pipeline/keyed_stage.go` | New — migrated wireKeyedStage (with P0-1 fix) |
| `pipeline/metrics.go` | New — edge/stage metrics |
| `weibo.go` (repo root) | Modify — plan + wire + two-phase shutdown |
| `stream.go` | Modify — WithParallelism |
| `operator/operator.go` | Add — SingleProcessor, Parallel interfaces |
| `operator/map.go`, `filter.go`, `flatmap.go`, `process.go` | Add — ProcessOne |
| `observability/metrics/metrics.go` | Add — edge/stage metrics; keep existing names |

## Implementation order

1. **P0-1 first** — barrier fix in current `wireKeyedStage`, own PR.
2. `operator`: `ProcessOne` on all stateless operators + unit tests.
3. `pipeline/edge.go` + `pipeline/stage.go` — abstractions, sendRecord.
4. `pipeline/planner.go` + planning tests.
5. `pipeline/stateless_stage.go` — serial path first, then parallel with C1.
6. `pipeline/keyed_stage.go` — migrate wireKeyedStage (P0-1 logic carries over).
7. `weibo.go` — wire plan + edges, two-phase shutdown (C3), delete old loop.
8. Metrics migration (C5) — inline counters, delete wrapper channels.
9. Config (`WithBufferSize`, `WithParallelism`) + validation.
10. Backpressure / alignment / shutdown tests; update examples.
