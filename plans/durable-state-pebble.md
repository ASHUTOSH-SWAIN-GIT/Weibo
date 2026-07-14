# Durable State Backend (Pebble)

Status: P1–P6 + PW IMPLEMENTED (2026-07-14). Durable state complete.

## PW — Window records on the backend (done)

`WindowOperator` no longer keeps a private `map[windowKey]*windowState`
in the Go heap. Its records and watermark now live in the injected
state backend, so a window buffering millions of records uses disk
(Pebble) instead of RAM, and native checkpoints hard-link window
contents just like Reduce state.

Design as built:
- **Records** → `ListState("window_records")`, keyed by
  `windowKey.String()` = `"<recordKey>/<start>/<end>"`. Each entry is
  one JSON-encoded record. Window bounds are recovered by parsing the
  key, so the set of open windows is exactly `ListState.Keys()` — no
  separate index.
- **Watermark** → `ValueState("window_wm")["wm"]` (8-byte UnixNano),
  with a RAM working copy (`currentWatermark`) for the per-record
  late-drop check (no backend read on the hot path). Updates
  write-through; `Process` loads it at startup so it survives a native
  restore (where the operator's `Restore([]byte)` is never called).
- **New capability**: `ListState.Keys()` (added to the interface, both
  backends) — the one thing Window needs that Reduce didn't:
  enumerate open windows to fire the elapsed ones on a watermark.
  Pebble extracts the key positionally from `l\x00<ns>\x00<key>\x00<seq8>`
  (robust to `\x00`/`/` inside keys).
- **Native checkpoint**: Window implements `NativeSnapshotter` +
  `StateConfigurable`, so the planner injects a backend and wires
  hard-link snapshots automatically — identical to Reduce.

Trade-off documented in code: **session merges do a list-move**
(get+clear+re-append) when two sessions merge and the window key
changes. Same iteration complexity as the old map, but the record move
is new cost; only pathological for huge sessions that merge
repeatedly. Tumbling/sliding just append.

Latent bug fixed in passing: the old `Clone()` dropped `IdleTimeout`
(keyed windows lost their idle timeout in worker clones); the rewrite
copies it.

Tests: `test/unit_tests/window_durable_test.go` — records physically in
the backend, JSON snapshot/restore over both backends, and native
hard-link checkpoint restore of both window records and the watermark.
All existing window tests pass unchanged on the rewrite.

## Phase 6 results (Apple Silicon, macOS, 16-byte values, 4 keyed workers)

## Phase 6 results (Apple Silicon, macOS, 16-byte values, 4 keyed workers)

Checkpoint duration — the success metric (full = all keys changed,
incr = 1,000 keys changed since last checkpoint):

| keys | memory full/incr | pebble-compat full/incr | **pebble-native full/incr** |
|------|-----------------|------------------------|----------------------------|
| 1k   | ~0 / 0.3 ms | ~0 / 0.3 ms | 39 / 46 ms |
| 100k | 32 / 26 ms  | 37 / 35 ms | 42 / 48 ms |
| 5M   | 3,340 / 3,150 ms | 3,826 / 3,589 ms | **72 / 77 ms** |

**Success condition MET**: native checkpoint cost is flat (~40–77 ms,
dominated by fixed flush/fsync overhead) across three orders of
magnitude of state — it scales with changed data. Both serialization
modes scale with TOTAL state (3.3–3.8 s barrier stalls at 5M, even
when only 1k keys changed, because SnapshotAll can't know what changed).

Other measurements at 5M keys:

| metric | memory | pebble-compat | pebble-native |
|---|---|---|---|
| Go heap for state | 579 MB | 0.8 MB | 0.7 MB |
| live disk | — | 56 MB | 56 MB |
| checkpoint artifact | 205 MB JSON | 205 MB JSON | 46 MB (hard-linked SSTs) |
| lookup latency | 0.3 µs | 5.5 µs | 5.4 µs |
| restore (state part of recovery) | 2.9 s | 4.2 s | **58 ms** |
| backend write rate | 3.1 M/s | 1.2 M/s | 1.3 M/s |
| pipeline throughput | 1.38 M rec/s | 1.03 M rec/s | 1.03 M rec/s |

Trade-off summary: Pebble costs ~17× lookup latency and ~25% pipeline
throughput at 5M keys, and buys ~700× less RAM, ~46× faster barriers,
and ~50× faster recovery. At small state (≤100k keys) memory wins
everything except durability; native's fixed ~40 ms flush makes it
slower than serialization below ~1M keys.

Three bugs found and fixed by Phase 6 verification:
0. At-least-once checkpoints could become durable for records the sink
   never received (barrier saved when it entered the sink's 256-slot
   handoff buffer, not when the sink drained it). Uncoordinated saves
   now happen in SinkStage, only after the sink has dequeued
   everything ahead of the barrier. (Pre-existing; Pebble's timing
   made the recovery test catch it.)
1. Native checkpoints were dead code for keyed workers — barrier-time
   full-serialization snapshots always preempted them. Fixed by
   `NativeSnapshotter`: Checkpointable backends now hard-link at the
   barrier inside the operator, and (also fixed) `Save`/restore
   disagreed on the StateDirs path convention.
2. `CheckpointTo` hard-linked an unflushed WAL: with `Sync: false`
   writes, small states checkpointed EMPTY. `CheckpointTo` now flushes
   the memtable first (cost ∝ data since last flush — preserves the
   incremental property). Also added: a lifecycle guard so straggler
   goroutines from force-aborted runs can't panic on a closed DB.

Benchmarks live in `bench/state_scale_test.go`:
`go test -bench . -benchtime=1x -timeout 30m ./bench/` (add `-short`
to skip the 5M tier). Barrier pause == checkpoint duration (the
snapshot runs synchronously in the operator at the barrier).

Goal: keyed operator state that (1) is not bounded by RAM, (2) survives
without re-serializing everything on every checkpoint, and (3) keeps
the exactly-once guarantees intact. Backend: `cockroachdb/pebble`
(pure-Go LSM, the RocksDB equivalent Flink uses for the same job).

---

## 1. Current state of state (audited)

| Fact | Consequence |
|------|-------------|
| `state.StateBackend` interface exists (`ValueState`, `ListState`) with only `MemoryBackend` | The interface seam is already there — good. |
| `Reduce()` constructs `NewMemoryBackend()` **inside its own constructor and inside `Clone()`** (`operator/reduce.go`) | There is NO injection point. Nothing can swap the backend today. This is the first thing to fix. |
| `WindowOperator` does NOT use the state backend at all — windows live in a private `map[windowKey]*windowState` | Pebble v1 will not make window buffers durable/spillable. Migrating Window onto `ListState` is its own phase (last). |
| Checkpointing calls `Snapshot()` → `ValueState.SnapshotAll()` → full copy of every key, JSON-marshaled into the checkpoint file | O(total state) serialization per checkpoint. Fine for KBs, fatal for GBs. A durable backend must eventually bypass this (Phase 4), not just implement it. |
| Barrier-time snapshots (`BarrierSnapshotter`) fire synchronously inside each operator's Process loop, per worker, at different wall-clock times | Any snapshot mechanism must be **per-worker-consistent at its own barrier**. A single shared DB cannot be checkpointed once globally — workers hit the barrier at different times and post-barrier writes from early workers would leak in. |

## 2. Key design decisions

### D1 — One Pebble DB per state owner, not one shared DB

"Owner" = a stateful operator instance: top-level op (`op-<i>`) or
keyed-worker clone (`worker-<idx>`) — the same stable IDs the
checkpoint format already uses.

Why not shared: barrier-time consistency (see audit table). Each owner
snapshots at *its* barrier passage; with per-owner DBs, `pebble.Checkpoint`
of that DB at that moment is exactly the pre-barrier state. A shared DB
would need a global write pause across workers — needless coordination.

Cost: `partitions × stateful ops` small DBs (e.g. 16×2 = 32 dirs).
Acceptable; documented; partitions are bounded by config.

Layout: `<stateDir>/live/<ownerID>/` per DB. Inside a DB, keys are
`<stateName> 0x00 <userKey>` (value states) and
`l 0x00 <stateName> 0x00 <userKey> 0x00 <seq>` (list entries).

### D2 — Backend injection via factory, memory stays the default

- `mailer.WithStateBackend(factory)` on the env, where
  `factory: func(ownerID string) (state.StateBackend, error)`.
- `state.Pebble(dir, opts...)` returns such a factory;
  `state.InMemory()` is the default (current behavior, zero change for
  existing users).
- New optional interface `operator.StateConfigurable
  { SetStateBackend(state.StateBackend) }`, implemented by Reduce (and
  later Window). The planner assigns backends: top-level ops get
  `op-<i>`, keyed clones get `worker-<idx>` at `NewKeyedStage` clone
  time — the same place `OnClone` already assigns those indices.
- `Clone()` stops hard-coding `NewMemoryBackend()`; clones receive
  their backend from the planner (each clone = own owner = own DB —
  keyed state isolation preserved).
- Lifecycle: backends that implement `io.Closer` are tracked by the
  env and closed after `wg.Wait()` in Execute. Double-run of an env
  with Pebble = error (single-writer lock per dir; Pebble enforces
  this natively — surface the error clearly).

### D3 — Durability model: the working DB is DISPOSABLE, checkpoints are the truth

Between checkpoints, crash-consistency of the live DB is worthless —
recovery always restores from the last completed checkpoint and
replays. Therefore:

- Pebble runs with `DisableWAL: true` and non-sync writes (fast path);
- on restore, the live dir is **wiped** and rebuilt from the
  checkpoint's state snapshot;
- durability is provided exclusively by the checkpoint mechanism.

This mirrors Flink+RocksDB and keeps the write path at memory-like
speed.

### D4 — Two checkpoint modes, shipped in this order

**Phase 3 (compatible mode):** Pebble implements the existing
`SnapshotAll`/`RestoreAll` contract by prefix iteration. State bytes
still travel inside the checkpoint file exactly as today. Correct for
all existing paths (at-least-once AND the exactly-once coordinator)
with zero checkpoint-format changes — but O(state) per checkpoint.
This makes Pebble a drop-in from day one and keeps the diff reviewable.

**Phase 4 (scalable mode):** native Pebble checkpoints.
- New optional backend interface:
  `state.Checkpointable { CheckpointTo(dir string) error; RestoreFrom(dir string) error }`.
  `pebble.DB.Checkpoint()` hard-links SSTs — cost is proportional to
  *changed* data, not total state (incremental in practice).
- `Reduce.Snapshot()` on a Checkpointable backend returns a small JSON
  **reference** (`{"state_ref": "<ownerID>"}`) instead of the data;
  the actual snapshot goes to
  `<checkpointDir>/checkpoint-<id>.state/<ownerID>/`.
- The barrier-time snapshot callback already carries the checkpoint
  ID; the backend derives the target dir from (baseDir, id, ownerID).
  Target dir must be on the same filesystem as the live DB for
  hard-links — validated at startup.
- `checkpoint.CheckpointData` gains `StateDirs map[string]string`
  (owner → relative path). `FileStorage` owns the directory layout,
  fsyncs the state dirs before the checkpoint JSON (the JSON is the
  commit point, same rule as the exactly-once protocol), and its
  retention/GC deletes state dirs together with superseded checkpoint
  files — orphaned `*.state` dirs from crashed checkpoints are swept
  on startup.
- Restore: wipe live dir, `RestoreFrom` copies (or re-hard-links) the
  checkpoint state dir, open.

### D5 — Exactly-once composes without protocol changes

The coordinator doesn't care whether `Operators[key]` holds state
bytes or a state ref — the two-phase protocol, marker probe, and
recovery decision table are untouched. The only addition: a `prepared`
checkpoint that gets discarded during recovery must also have its
state dir deleted (hook into the existing abort/fallback path).

## 3. What this does NOT cover (explicit)

- **Window state stays in RAM** until the final phase: `WindowOperator`
  buffers records in its own map, not in the backend. Large-window
  spill-to-disk requires rewriting Window on `ListState` — Phase W
  below, separately shippable.
- Cross-machine state (still single-process).
- State TTL / expiry (note as future knob on the backend).

## 4. Risks / assumptions

1. **Dependency weight**: `cockroachdb/pebble` is a heavy module.
   Sink/source users who don't need it still compile it (no build
   tags in v1 — revisit if binary size becomes a complaint).
2. **Barrier latency**: `pebble.Checkpoint` at every barrier per owner.
   Hard-link based, typically ms — but budget it: measure with the
   benchmark in Phase 5; if needed, checkpoint only owners whose state
   changed since the last barrier (pebble metrics expose this cheaply).
3. **Same-filesystem requirement** for hard-links (live dir vs
   checkpoint dir). Validate at startup, fail fast with a clear error.
4. **List semantics on LSM**: `ListState.Append` needs a per-key
   sequence suffix; `Clear` is a range delete. Straightforward but the
   conformance suite (Phase 2) must nail ordering.
5. **SnapshotAll on huge state** (compatible mode) is still O(state) —
   Phase 3 is explicitly an intermediate step; docs must say "use
   scalable mode (default once Phase 4 lands) for large state".
6. **Restore-into-fresh-clone path**: `RestoreAll`/`RestoreFrom` runs
   before stages start (Phase B in Execute) — already single-threaded,
   no ordering change needed. Verified by existing recovery tests once
   the factory is wired in.

## 5. Phases

```
P1 injection ──► P2 pebble backend ──► P3 drop-in (SnapshotAll) ──► P4 native checkpoints ──► P5 EO + perf ──► P6 docs
                                                                                                └─► PW window-on-ListState (independent after P2)
```

**P1 — Backend injection (no Pebble yet).** ✅ DONE
`WithStateBackend(factory)`, `operator.StateConfigurable`, planner
assigns owner IDs, Reduce/Clone take injected backends, env closes
Closers. Default factory = memory; all existing tests must pass
unchanged. *Ships alone.*
Note: implemented via `pipeline.StageHooks` (OnClone/OnSnapshot/
StateBackendFor bundled); prototypes of keyed operators keep their
default memory backend — only clones (the instances that process
records) get injected backends.

**P2 — PebbleBackend.**
`state/pebble.go`: `ValueState` + `ListState` on the D1 key layout,
`DisableWAL`, Close, single-writer errors. A **conformance test suite**
run against both MemoryBackend and PebbleBackend (same behavioral
spec: get/set/clear, key scoping, list ordering, snapshot/restore
roundtrip, concurrent owners).

**Status:** ✅ DONE.

**P3 — Drop-in durability (compatible mode).**
Nothing new to build beyond P2's `SnapshotAll`/`RestoreAll`; the work
is verification: run the entire recovery + exactly-once test matrix
(crash sweep, keyed multi-partition, persist-failure) parameterized
over both factories. Gate: full suite green with Pebble under `-race`.

**Status:** ✅ DONE.  Conformance suite (14 tests × 2 backends), recovery
tests (2 tests × 2 backends), exactly-once tests (7 tests × 2 backends)
all pass with `-race`.

**P4 — Native Pebble checkpoints (scalable mode).**
`Checkpointable`, state refs in `CheckpointData.StateDirs`, FileStorage
dir layout + fsync order + GC + orphan sweep, restore-from-dir, wipe
live dir on restore, prepared-checkpoint state-dir cleanup in the
recovery fallback.

**Status:** ✅ DONE.

- `state.Checkpointable` interface: `CheckpointTo(dir)`, `RestoreFrom(dir)`
- `PebbleBackend` implements Checkpointable via `pebble.DB.Checkpoint()` (hard-links)
- `CheckpointData.StateDirs` maps ownerID → state dir key
- `collectSnapshots` routes to `CheckpointTo` for Pebble backends, stores state-ref marker
- `extractStateDirs` populates `StateDirs` from snapshot markers, clears inline state bytes
- `restoreWorkersFromCheckpoint` uses `RestoreFrom` when `StateDirs` exists
- `FileStorage.Save()` fsyncs state dirs before writing commit-point JSON
- `FileStorage.StateDir(id)`, `SweepOrphans()`, `DeleteStateDirs()` for lifecycle
- `syncDir()` ensures newly created state subdirectories are durable

**P5 — Exactly-once integration.**

- `collectSnapshots` returns both operator snaps AND native state dirs.
- Uncoordinated path: `saveCheckpoint` stores `StateDirs` directly.
- Coordinated path: `OnStateSnapshot` accepts `stateDirs`, coordinator
  `finalize` includes them in the prepared checkpoint data.
- `pendingCheckpoint` tracks `stateDirs` alongside offsets/state.
- `abortFatal` calls `cleanupStateDirs` — failed/aborted checkpoint
  state directories are deleted so they don't accumulate.
- `restoreWorkersFromCheckpoint` uses `RestoreFrom` when `StateDirs`
  exists for a worker.

Crash windows tested (via existing parameterized EO suite):
- Before state snapshot, during, after.
- Before checkpoint JSON commit, after completion.
- During sink/source commit.

**Status:** ✅ DONE.  Full EO test matrix (7 test classes × 2 backends ×
multiple crash points) passes with `-race`.

**P6 — Metrics + docs + example.**
`mailer_state_size_bytes{owner}`, `mailer_state_checkpoint_duration_seconds`,
pebble compaction gauges. README: state section rewrite (backend
choice table, durability model D3, same-filesystem note).
`examples/durable-state/`: kill -9 mid-run, restart, counts continue —
the demo that sells it.

**PW — Window on ListState (follow-up, independent).**
Rewrite `WindowOperator` buffers onto `ListState` (per key+window
lists, window-index value state, watermark in value state), delete its
private map, inherit the injected backend. This is what makes
million-record windows possible; it only depends on P1+P2.

## 6. File map

| File | Action |
|------|--------|
| `state/pebble.go` | New — PebbleBackend (P2), Checkpointable (P4) |
| `state/factory.go` | New — `InMemory()`, `Pebble(dir, ...)` factories (P1/P2) |
| `state/conformance_test.go` | New — behavioral spec vs both backends (P2) |
| `operator/operator.go` | Add — `StateConfigurable` (P1) |
| `operator/reduce.go` | Modify — injected backend, Clone inherits (P1); ref-based Snapshot (P4) |
| `pipeline/planner.go`, `keyed_stage.go` | Modify — assign backends by owner ID at plan/clone time (P1) |
| `mailer.go` | Add — `WithStateBackend`, Closer lifecycle (P1); restore-from-dir path (P4) |
| `checkpoint/checkpoint.go` | Add — `StateDirs`, dir layout + fsync order + GC (P4) |
| `test/unit_tests/` | Parameterize recovery + EO suites over backend factories (P3/P5) |
| `operator/window.go` | Rewrite buffers onto ListState (PW) |
```
