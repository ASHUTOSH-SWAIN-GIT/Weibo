# Keyed Parallel Execution for Stateful Operators

## Current Architecture

```
Source → KeyBy(internal partitions, merged output) → Window → Reduce → Sink
                                    ↑ single shared state backend
```

KeyBy has N internal partition goroutines but **merges output back into a single channel**. Downstream Window and Reduce share one state backend instance — no per-key isolation.

## Target Architecture

```
                        ┌── Worker 0: Window₀ → Reduce₀ ──┐
Source → KeyBy Router ──┼── Worker 1: Window₁ → Reduce₁ ──┼── Merge → Sink
                        ├── Worker 2: Window₂ → Reduce₂ ──┤
                        └── Worker 3: Window₃ → Reduce₃ ──┘
                         ↑ each has isolated state backend
```

KeyBy becomes a **router** that hash-dispatches records to worker channels. Each worker runs its own chain of stateful operators with isolated state. A merger combines worker outputs back into a single channel for downstream stateless operators and the sink.

---

## Phase 1 — Key Selector Abstraction

- `KeySelector func(types.Record) []byte` — user-provided key extraction function
- `KeyBy(fn)` stores the selector for pipeline routing
- Returns `[]byte`, independent of Kafka message key

---

## Phase 2 — KeyBy as Router (Rewrite `operator/keyby.go`)

KeyByOperator transforms from a processing operator into a router:

- Remove internal partition goroutines
- In `Execute()`: when KeyBy is encountered, create the worker topology
- Router goroutine: `hash(key) % partitions → send to worker[workerID]`
- Uses FNV-1a hash (already deterministic)

## Phase 3 — Worker-level Operator Instances

In `Execute()`, when a KeyBy is followed by stateful operators:

1. Identify the KeyBy operator and its partition count
2. Identify the downstream stateful chain (Window → Reduce → ...)
3. For each worker (0..N-1):
   - Create a dedicated input channel
   - Instantiate fresh copies of each stateful operator in the chain
   - Run them sequentially in a goroutine
4. Each operator instance gets its own `MemoryBackend` (state isolation)

## Phase 4 — Consistent Window → Reduce Routing

Within a worker, operators run sequentially:

```
Worker 2: [input chan] → Window₂ → Reduce₂ → [output chan]
```

Records for key "Alice" always enter Worker 2, so Window₂ and Reduce₂ always see Alice's records in order. No redistribution between stateful operators.

## Phase 5 — Configuration

- `KeyBy(fn, operator.WithPartitions(8))` — keyed parallelism
- `Workers(4)` — stateless operator parallelism (separate, already designed)
- Validation: `Workers(N)` before stateful ops returns pipeline error

## Phase 6 — Checkpointing Per Worker

- `CheckpointData.Operators` keyed as `"Window/worker-0"`, `"Reduce/worker-0"`, etc.
- Each worker's operator snapshots independently via `Snapshot()`
- On restore: recreate N workers, restore each worker's state
- Same deterministic hash ensures same keys route to same worker indices

**Status:** Not yet implemented.  The worker instances created by `wireKeyedStage`
are local variables — they are not reachable by `saveCheckpoint()` which
traverses `env.operators` (the original template operators).  A worker
registry needs to be added so checkpoints capture per-worker state.

## Phase 7 — Metrics Per Operator Per Worker

```
mailer_operator_worker_records_total{operator="Window",worker="0"}
mailer_operator_worker_records_total{operator="Reduce",worker="0"}
mailer_operator_worker_latency_seconds{operator="Window",worker="0"}
```

## Phase 8 — Failure Handling

- Shared child context per keyed stage
- Buffered error channel (cap = N workers)
- Worker failure → cancel shared context → stop all workers
- `FailurePolicyFail` propagates to pipeline runtime
- Bad-record errors: continue using existing ProcessOperator failure policy

## Phase 9 — Graceful Shutdown

1. Stop accepting new records (ctx cancelled)
2. Close router input
3. Workers drain their channels
4. Wait for all worker goroutines
5. Merger drains all worker outputs
6. All channels closed exactly once

## Phase 10 — Tests

| Test | Description |
|------|-------------|
| Same-key routing | Two records with same key → same worker |
| Different-key routing | Different keys can reach different workers |
| Same-key ordering | Records for one key arrive in order at the worker |
| State isolation | Workers do not share state |
| Cancellation | Shared context stops all workers, no goroutine leaks |
| Checkpoint/restore | Restored workers receive same keys as before |
| Failure policy | Processing errors follow configured Drop/DLQ/Fail |
| Fatal error | Worker crash stops entire keyed stage |
