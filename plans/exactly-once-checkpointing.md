# Exactly-Once Checkpoint Coordination

Status: **IMPLEMENTED** (2026-07-14). All phases landed; the crash-point
sweep and multi-partition suites pass under `-race`. Two protocol bugs
were found and fixed during implementation beyond the plan:
(1) operator snapshots must be taken synchronously inside the operator
when the barrier passes (snapshotting at the pre-sink tap races with
post-barrier processing and can persist empty/corrupt state);
(2) the aligned merge must PARK workers at markers — letting post-barrier
records overtake a held barrier leaks them into the wrong sink
transaction and duplicates them on replay. Checkpoint IDs also gained a
per-run nonce so a stale marker can never prove a different run's
checkpoint. Real-broker (dockerized) tests remain a follow-up.

Goal: coordinate (1) source offsets, (2) operator state snapshots, and
(3) sink output as one atomic checkpoint, so that after any crash +
recovery the *visible* output of the pipeline contains every input
record's effect exactly once.

Initial scope: `Kafka source → operators → Kafka transactional sink`.
All other sinks keep their current at-least-once semantics and must
never be advertised as exactly-once.

---

## 1. Audit of current behavior

### Where source offsets are committed today

| Site | Behavior | Problem |
|------|----------|---------|
| `source/kafka.go` `runSerial` (lines ~191–202) | Commits each message (or batch of `commitBatch`) to the broker **immediately after** pushing the record downstream | **Loss window**: offset is durable before the sink ever sees the record. Crash ⇒ record skipped on restart. |
| `source/kafka.go` `Drain` | Flushes pending batch commits on graceful shutdown | Same semantics, shutdown-time. |
| `source/kafka.go` parallel mode | Never commits to the broker; durability only via `CheckpointOffset`/`RestoreOffset` + `SetOffset` | Broker lag metrics lie; two sources of truth. |
| `weibo.go` `saveCheckpoint` → `CheckpointOffset` | Stores `reader.Stats().Offset` per partition into the checkpoint file | **Not barrier-aligned** (next section). |

### Where checkpoints are saved today

`injectBarriers` (weibo.go) puts a barrier into the stream after the
source stage on a ticker. When the barrier passes `barrierDetect`
(wired just **before** the sink stage), `saveCheckpoint` synchronously
snapshots operator/worker state + source offsets and writes the file.

Two defects for exactly-once purposes:

1. **Offsets are captured at save time, not at the barrier's stream
   position.** There are buffered channels (≈256 slots each) between
   `source.Run` and the injector. Records already counted in
   `reader.Stats().Offset` can still be *behind* the barrier in those
   buffers. On restore, those records are neither in the state snapshot
   nor replayed ⇒ **silent loss**. (Same reasoning applies at save
   time: the source has raced ahead of the barrier.)
2. **The sink is not part of the checkpoint at all.** The checkpoint is
   "complete" while the sink's pre-barrier output may still be sitting
   in buffers. Crash ⇒ that output is gone but the offsets say it
   happened (loss), or the output was written but a *previous*
   checkpoint is restored (duplicates).

### Duplicate window (current)

Sink writes record R durably → crash before the next checkpoint saves
→ restart from older checkpoint → R replayed → written again. Classic
at-least-once; expected today, to be eliminated for the transactional
sink.

---

## 2. Design overview

The coordinator implements a **two-phase commit where the checkpoint
file is the transaction log**, with one non-standard addition — a
**transaction marker record** — that closes the "committed but not
recorded" window without needing Kafka transaction resumption (which
stock Go clients cannot do).

```
                     ┌──────────────── CheckpointCoordinator ────────────────┐
                     │  id, aligned offsets, state snapshots, txn status     │
                     └───────────┬────────────────────────────┬──────────────┘
                     (2) barrier + offset snapshot        (5..9) prepare/commit
                                 │                            │
Kafka ──► SourceStage ──► [offset tracker + injector] ──► stages... ──► [state snapshot] ──► TxnKafkaSink ──► Kafka (txn)
                                                                                      (4) flush + PreCommit + marker
```

### The protocol, step by step (maps to the requested flow)

1. **Trigger.** Ticker fires (or shutdown begins). Coordinator
   allocates `id = cp-N`.
2. **Barrier injection with aligned offsets.** The injector sits
   directly after the source stage and maintains an **in-band offset
   map**: for every data record passing through, it records
   `partition → offset+1` (records will carry their partition — see
   §4). When the barrier is injected, the current map *is* the exact
   set of offsets of records ahead of the barrier — buffers can't
   desynchronize it, because it is derived from the records that
   actually passed this point, not from live reader stats. The map is
   registered with the coordinator under `id`.
   *(This replaces "the source snapshots its offsets": snapshotting at
   the injector is provably aligned; snapshotting inside the source is
   not, because of the buffers between them.)*
3. **Operators snapshot.** Barrier flows through stages with the
   existing broadcast + alignment machinery (unchanged — it is already
   correct). At the pre-sink tap (today's `barrierDetect`), operator
   and keyed-worker state is snapshotted and handed to the coordinator
   under `id`. Nothing is persisted yet.
4. **Sink prepares.** The barrier continues *into* the sink stage
   in-band. The transactional sink's `Write` loop sees it after all
   pre-barrier records, and:
   - flushes every buffered pre-barrier record into the currently open
     Kafka transaction,
   - produces a **marker record** `{checkpoint_id}` into a compacted
     `<topic>.checkpoints` marker topic *inside the same transaction*,
   - calls `PreCommit(id)` semantics: all output for this interval is
     now in the broker, invisible (transaction still open),
   - acknowledges `id` to the coordinator.
   Records arriving *after* the barrier are buffered (bounded — the
   edge provides backpressure) until step 6 opens the next transaction.
5. **Persist `prepared`.** Coordinator writes the checkpoint file:
   `{id, offsets, operator state, txn_id, status: prepared}` + fsync.
   This is the 2PC "commit decision is logged" point.
6. **Commit sink.** Coordinator calls `Commit(id)`: sink issues
   `EndTxn(commit)` and immediately `Begin`s the next transaction
   (unblocking post-barrier records).
7. **Persist `completed`.** Coordinator rewrites the checkpoint status
   to `completed` and updates the `latest-completed` pointer.
8. **Commit source offsets to the broker.** `CommitOffsets(ctx, data)`
   on the source — **advisory only** (consumer-lag visibility). The
   checkpoint file is the source of truth for recovery; a crash before
   this step is harmless.
9. Done. Metrics: checkpoint duration, size, status.

### Failure handling during a checkpoint

Any failure in steps 2–6 ⇒ coordinator calls `Abort(id)` on the sink
(abort the open transaction), discards the pending snapshot, and
**fails the pipeline** (returns error from Execute). It must NOT
"skip this checkpoint and continue" the way Flink does: the aborted
transaction contained real output for records the stream has already
consumed — continuing would lose that output forever. Restart +
restore from the latest completed checkpoint replays that interval
into a fresh transaction.

### Recovery decision table

On startup, load the newest checkpoint file:

| Latest checkpoint status | Marker `{id}` visible in marker topic (read_committed)? | Action |
|---|---|---|
| `completed` | — | Restore state + offsets from it. Init producer (fences/aborts any zombie txn from the crashed run). Run. |
| `prepared` | **yes** | The transaction actually committed before the crash (we died between steps 6 and 7). Promote to `completed`, restore from it. **No duplicates.** |
| `prepared` | **no** | The transaction never committed (died between 5 and 6, or during 6). Producer init fences/aborts it — its output was never visible. Discard this checkpoint, restore from the previous `completed`. Replay re-produces the interval in a new transaction. **No loss, no duplicates.** |
| none | — | Fresh start from configured start offset. |

The marker probe is what makes this exactly-once *without* transaction
resumption: it converts "did EndTxn happen?" — which Kafka won't tell
you — into a read-committed visibility check.

---

## 3. New abstractions

### `source` package

The existing `CheckpointSource` (`CheckpointOffset`/`RestoreOffset`)
stays for at-least-once compatibility. Exactly-once adds one optional
interface — offsets are *snapshotted* by the injector (in-band, §2
step 2), so the source only needs commit + restore:

```go
// OffsetCommitter is implemented by sources whose offsets should be
// committed externally after a checkpoint completes. Advisory: the
// checkpoint file remains the source of truth for recovery.
type OffsetCommitter interface {
    CommitOffsets(ctx context.Context, offsets []byte) error // same JSON shape as CheckpointOffset
}
```

*(Deviation from the proposed `SnapshotOffsets` on the source: a
source-side snapshot cannot be barrier-aligned through the buffers
between source and injector — see audit. The injector-side in-band
tracker is exact by construction.)*

New `KafkaSource` behavior behind a `KafkaExactlyOnce()` option:
- **All eager broker commits in `runSerial`/`Drain` are disabled.**
  Commits happen only via `CommitOffsets` after checkpoint completion.
- `CommitOffsets` maps the JSON offsets back to `CommitMessages` /
  per-partition commit.

### `sink` package

```go
// CheckpointedSink participates in coordinated checkpoints. Output
// between two checkpoints is staged (e.g. in a Kafka transaction) and
// becomes visible only on Commit.
type CheckpointedSink interface {
    Sink
    BeginCheckpoint(ctx context.Context, id string) error // open txn for the interval ending at id
    PreCommit(ctx context.Context, id string) error       // flush all pre-barrier output + marker; keep txn open
    Commit(ctx context.Context, id string) error          // EndTxn(commit); Begin next txn
    Abort(ctx context.Context, id string) error           // EndTxn(abort)
}
```

New `TxnKafkaSink` (file `sink/kafka_txn.go`) implementing it on
**franz-go** (`github.com/twmb/franz-go/pkg/kgo`) — see §5 for why a
new dependency is unavoidable. Config: brokers, topic, transactional
ID, marker topic (default `<topic>.checkpoints`), serializer, SASL/TLS
reusing `auth`.

Barrier handling: `Write` sees barriers in-band (they already flow to
the sink); on barrier it performs the PreCommit sequence and notifies
the coordinator through a callback the engine registers:

```go
type CheckpointNotifier interface {
    SetOnPrepared(func(id string, err error))
}
```

### `checkpoint` package

```go
type Status string // "prepared" | "completed"

type CheckpointData struct { // extended
    ID        string
    Timestamp time.Time
    Operators map[string][]byte
    Source    map[string][]byte
    Status    Status // NEW
    TxnID     string // NEW: sink transactional id, for diagnostics
}
```

- Storage keeps every checkpoint file (bounded retention, e.g. last 5)
  plus **two pointers**: `latest.json` (any status, for the recovery
  decision table) and `latest-completed.json`.
- `Coordinator` (new file `checkpoint/coordinator.go`): owns the ticker,
  ID allocation, the pending-checkpoint state machine
  (`offsets → state → sink-ack → prepared → committed → completed`),
  timeouts (a checkpoint that doesn't complete within N seconds ⇒
  abort + pipeline error), and the recovery decision table.

### Engine wiring (`weibo.go` / `pipeline`)

- `types.Record` gains a `Partition int` field; `KafkaToRecord` sets it
  from `kafka.Message.Partition` (needed by the in-band offset tracker;
  `replaySource` in tests sets it too).
- The injector (today `injectBarriers`) becomes coordinator-driven and
  maintains the aligned offset map.
- `barrierDetect` becomes "state tap": hands snapshots to the
  coordinator instead of writing the file itself.
- Execute detects the sink type:
  - `CheckpointedSink` ⇒ coordinated mode (full protocol).
  - plain `Sink` ⇒ current behavior, unchanged (at-least-once), even
    when checkpointing is on.
- Mode validation: coordinated mode requires a `CheckpointedSink` AND
  (if the source is Kafka) the `KafkaExactlyOnce()` source option;
  Execute returns a configuration error otherwise, so exactly-once is
  never silently half-configured.

---

## 4. Ordering rules that make it correct

1. Offsets are snapshotted **in-band at the injector** — aligned by
   construction, immune to buffering.
2. Operator snapshots are taken when the barrier reaches the pre-sink
   tap — after the existing broadcast/alignment at parallel stages
   (already correct, tested).
3. `prepared` is persisted **before** `EndTxn(commit)` — the commit
   decision is logged first (2PC rule).
4. The marker record makes commit **detectable** after a crash, so a
   `prepared` checkpoint can be safely rolled forward (marker visible)
   or safely discarded (marker absent — output was never visible).
5. Broker offset commits happen **last** and are advisory. The
   requested test "crash after sink commit but before source offset
   commit" is safe because recovery never reads broker offsets — it
   reads the checkpoint file.
6. One open transaction at a time: post-barrier records wait between
   `PreCommit` and `Commit`. Backpressure (bounded edges) absorbs the
   pause. (A producer pool à la Flink is a later optimization.)

---

## 5. Assumptions and correctness risks

1. **`segmentio/kafka-go` cannot do transactions.** Its `Writer` has no
   transactional producer (no InitProducerID/EndTxn flow exposed). The
   transactional sink therefore requires **franz-go** as a new
   dependency (sink only; the source can stay on kafka-go).
   Alternative rejected: hand-rolling the transaction protocol on
   kafka-go's low-level messages — large, error-prone surface.
2. **Downstream consumers must read with `isolation.level =
   read_committed`.** Otherwise they see aborted transactions and all
   guarantees are void. Must be documented loudly; our own KafkaSource
   should default to ReadCommitted (kafka-go `ReaderConfig.IsolationLevel`
   supports it).
3. **Single writer per transactional ID.** Two pipeline instances with
   the same txn ID fence each other (that's desirable — zombie
   fencing — but must be documented for people running replicas).
4. **The marker topic is part of the correctness story.** If an
   operator deletes it or it has aggressive retention, a `prepared`
   checkpoint after a crash could be mis-resolved. Mitigation: compacted
   topic, markers keyed by pipeline ID, retention documented.
5. **Checkpoint failure now fails the pipeline** (restart-to-recover),
   where today it just logs. This is required for correctness (§2,
   failure handling) but is a behavior change to document.
6. **Latency coupling**: output visibility latency == checkpoint
   interval. Records are invisible to read_committed consumers until
   the interval's transaction commits. Interval choice becomes a
   latency knob, not just a durability knob.
7. **State backend fsync**: exactly-once state assumes the checkpoint
   file write is durable (`fsync` before `prepared` is acknowledged —
   FileStorage currently renames without explicit fsync; add it).
8. **Barriers require a live source** (existing quirk, found during
   testing): a finite source that closes immediately never gets a
   mid-run checkpoint. Irrelevant for Kafka (infinite) but tests must
   use paced sources.
9. **Existing sinks**: Postgres/stdout/blackhole remain at-least-once.
   Postgres can later get an idempotent-upsert mode (`ON CONFLICT`)
   keyed by (partition, offset) as a cheaper alternative to 2PC — out
   of scope here.
10. **Windows/timers**: window contents and watermarks are part of
    operator snapshots already; replay after restore re-fires windows
    deterministically only if event time is used (processing-time
    windows are inherently non-deterministic across replay — document).

---

## 6. Test plan

Test doubles: `replaySource` (already exists — gains `Partition` on
records and `OffsetCommitter`, recording every `CommitOffsets` call)
and a new `fakeTxnSink` implementing `CheckpointedSink` fully in
memory: per-txn staging buffers, a "visible output" log appended only
on `Commit`, a fake marker set, and injectable failures per step.
Crash points are driven through coordinator hooks (test-only callback
`onStep(step, id) error`) so each window is hit deterministically, not
by timing luck.

| # | Requested scenario | Test |
|---|---|---|
| 1 | Crash before sink commit | Fail after `prepared` persisted, before `Commit`. Recovery: marker absent ⇒ previous completed checkpoint restored; staged output never visible; replay re-produces interval. Assert visible output == exactly-once set. |
| 2 | Crash after sink commit, before source offset commit | Fail between `Commit` and `CommitOffsets`. Recovery: marker present ⇒ prepared promoted to completed; no replay of the interval. Assert zero duplicates and `CommitOffsets` re-issued (or skipped) harmlessly. |
| 3 | Recovery from latest completed | Several completed checkpoints + one dangling prepared; assert restore picks the right one per the decision table (both marker-present and marker-absent variants). |
| 4 | No lost or duplicate output after replay | Sweep every crash point (each protocol step × 2 checkpoint cycles) in a loop; after final recovery run to completion, assert visible output multiset == input effect set, exactly once. This is the umbrella property test. |
| 5 | Checkpoint failure ⇒ sink abort | Make `prepared` persist fail (storage error hook). Assert `Abort(id)` called, staged output discarded, Execute returns error, and a subsequent run recovers cleanly from the last completed checkpoint. |
| 6 | Multiple Kafka partitions | All of the above run with 3 partitions; plus: aligned offset map in the checkpoint equals per-partition positions of records ahead of the barrier (assert against the injector-tracked map, including under backpressure with tiny edges — the case that breaks stats-based capture today). |

Optional (follow-up, behind a build tag / docker-compose): the same
scenarios against real Kafka with franz-go, kill -9 style process
crashes, read_committed consumer verification.

---

## 7. Implementation phases — dependency diagram

```
                ENGINE TRACK                          CONNECTOR TRACK
                ════════════                          ═══════════════

┌─────────────────────────────────────┐
│ PHASE 1  Aligned offsets            │  ◄── ships alone: fixes the existing
│  • types.Record.Partition           │      offset-misalignment bug even for
│  • in-band offset tracker at        │      today's at-least-once checkpoints
│    the barrier injector             │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ PHASE 2  Checkpoint v2              │
│  • CheckpointData.Status/TxnID      │
│  • per-checkpoint files + fsync     │
│  • latest / latest-completed ptrs   │
│  • Coordinator state machine        │
│    (no sink participation yet)      │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│ PHASE 3  Interfaces + wiring        │
│  • sink.CheckpointedSink + notifier │
│  • source.OffsetCommitter           │
│  • Execute: coordinated mode        │
│    detection + config validation    │
└───────┬──────────────────┬──────────┘
        │                  │
        ▼                  ├────────────────────┬─────────────────────┐
┌───────────────────────┐  │                    ▼                     ▼
│ PHASE 4  Protocol     │  │   ┌───────────────────────────┐  ┌─────────────────────────┐
│ tests on fakes        │  │   │ PHASE 5  TxnKafkaSink     │  │ PHASE 6  KafkaSource EO │
│  • fakeTxnSink        │  │   │  • franz-go transactions  │  │  • KafkaExactlyOnce()   │
│    (staged/visible/   │  │   │  • marker topic write     │  │    (no eager commits)   │
│    marker buffers)    │  │   │  • markerVisible(id)      │  │  • CommitOffsets impl   │
│  • crash-point sweep  │  │   │    recovery probe         │  │  • ReadCommitted default│
│    tests 1–6 (§6)     │  │   └─────────────┬─────────────┘  └────────────┬────────────┘
└───────────┬───────────┘  │                 │                             │
            │              │                 └──────────────┬──────────────┘
            │   GATE ✋: protocol proven                     │
            │   exactly-once on fakes                       ▼
            │   before touching Kafka       ┌─────────────────────────────────┐
            └──────────────────────────────►│ PHASE 7  Recovery decision      │
                                            │ table in Execute startup        │
                                            │  • completed → restore          │
                                            │  • prepared + marker → promote  │
                                            │  • prepared, no marker → back   │
                                            └───────────────┬─────────────────┘
                                                            │
                                                            ▼
                                            ┌─────────────────────────────────┐
                                            │ PHASE 8  Docs + example         │
                                            │  • README guarantees table     │
                                            │  • read_committed warning       │
                                            │  • examples/exactly-once/       │
                                            │  • (optional) dockerized        │
                                            │    real-Kafka test suite        │
                                            └─────────────────────────────────┘

WHAT'S SAFE TO MERGE AT EACH POINT
──────────────────────────────────
after 1     bug fix, better at-least-once           ── main, immediately
after 2–3   inert plumbing (no behavior change      ── main, behind sink-type
            unless sink is CheckpointedSink)            detection
after 4     exactly-once PROVEN on fakes            ── gate for phases 5–7
after 5–7   end-to-end exactly-once (Kafka→Kafka)   ── announce the guarantee
after 8     documented + demoed                     ── done
```

Reading the tracks:
- **Engine track (1→2→3→4)** is strictly sequential — each phase builds
  on the previous one's types and wiring.
- **Connector track (5, 6)** starts only after Phase 3 freezes the
  interfaces, and 5 ∥ 6 are independent of each other (sink is
  franz-go, source is kafka-go — no shared code).
- **Phase 4 is the gate**: the full crash-point sweep must be green on
  fakes before the Kafka implementations land, so protocol bugs are
  never debugged through a broker.
- **Phase 7** needs both the coordinator (2) and the marker probe (5).

### Step-by-step detail

1. `types.Record.Partition` + in-band aligned offset tracker at the
   injector. **Ship independently — it fixes the existing offset
   misalignment bug for at-least-once checkpointing too.**
2. `checkpoint`: `Status`, per-checkpoint files + `latest-completed`
   pointer, fsync, `Coordinator` state machine (no sink yet — behaves
   like today when the sink isn't checkpointed).
3. Interfaces: `sink.CheckpointedSink` (+ notifier), `source.OffsetCommitter`;
   Execute wiring + mode validation.
4. `fakeTxnSink` + tests 1–6 against the coordinator (fakes first —
   the protocol is fully testable without Kafka).
5. `TxnKafkaSink` on franz-go: transactions, marker topic, recovery
   probe (`markerVisible(id)`).
6. `KafkaSource`: `KafkaExactlyOnce()` option (disable eager commits),
   `CommitOffsets`, ReadCommitted default.
7. Recovery decision table in Execute startup path.
8. Docs: README guarantees section (state EO ✅, end-to-end EO with
   TxnKafkaSink ✅ + read_committed requirement, everything else
   at-least-once), example `examples/exactly-once/`.

---

## 8. Explicitly out of scope

- Exactly-once for Postgres/stdout/blackhole sinks (Postgres gets
  idempotent upsert later; never claim EO for the rest).
- Producer pools / concurrent transactions (latency optimization).
- KIP-939 (broker-side 2PC) — not broadly deployed.
- Multi-instance / distributed coordination.
