# Weibo — Roadmap

What to work on next, prioritized. Compiled from a dual (Claude + Codex) code
audit and a production-style Kafka load test (continuous stream → window → sink,
with Prometheus/Grafana and the control-plane dashboard).

Ordering principle: **correctness before features.** A stream engine's whole
value proposition is "no data loss, exactly-once." Bugs that undermine that come
first; new capabilities come after the guarantees are solid.

> Already shipped in the last pass (not repeated below): ~20 small fixes
> (state-copy on `Get`, `WithHeader` map clone, watermark ctx-leak, tar
> path-traversal guard, Postgres schema-qualified quoting, Kafka `batchSize`,
> API 400 on bad JSON, validation-message fixes, doc drift), the Grafana
> compose fix, the weibo-test observability stack, and the dashboard's built-in
> **Metrics** page.

---

## Tier 1 — Correctness (do first; these break the core guarantees)

These are real bugs found by **both** auditors independently, or that directly
threaten exactly-once. Each needs a fix **plus a regression test**.

1. **Multi-partition / multi-topic Kafka checkpoint offsets** — `source/kafka.go`
   (`CheckpointOffset`, ~line 376–400). Offsets are derived from
   `reader.Stats()`, which in consumer-group mode reports a single partition, and
   the checkpoint key is partition-only (topics collide). **This silently breaks
   exactly-once on any multi-partition source** — the most important item on this
   list. Fix: track committed offset per `(topic, partition)` from consumed
   messages, not aggregate reader stats.

2. **Non-keyed stateful state lost on restart** — `weibo.go` (~line 854).
   A `Window`/`Reduce` used *without* `KeyBy` is snapshotted under `op-<i>` keys
   but recovery only restores `worker-<idx>`, so its state silently resets on
   restart — contradicting the "restore all stateful operators" contract. Fix:
   add an `op-<i>` restore loop mirroring the worker loop.

3. **Checkpoint coordinator send-on-closed race** — `checkpoint/coordinator.go`
   (~line 164). `OnSinkPrepared` can send on `c.events` after `Stop()` closed it
   → panic. Fix: guard send/close mutual exclusion; add a `-race` test.

4. **Transactional sink error latching** — `sink/kafka_txn.go` (~line 236).
   `produceErr` is set once and never cleared, so one transient produce error
   fails **every** subsequent checkpoint. Fix: reset on commit/abort.

5. **`WasCommitted` can block until ctx timeout** — `sink/kafka_txn.go` (~line
   378). If the trailing records before the target offset are transaction-control
   / aborted (not surfaced under `read_committed`), the recovery probe never
   drains. Fix: track progress by high-water offset, not data-record offsets.

---

## Tier 2 — Windowing semantics (surfaced by the production test)

The load test exposed how `Window → Reduce` actually behaves, and it's the
biggest usability gap for real analytics.

6. **Windowed aggregation emits partials, not one result per window.**
   `Window` flushes every buffered record at close, and `Reduce` emits a running
   accumulator per record (`operator/reduce.go:137`), so a 5s window produces N
   rows per key with growing totals — the *last* is the real total. This is
   documented, but it's not what "windowed sum" should mean. **Add a
   window-close-only emission** (one final aggregate per key per window), e.g. a
   `WindowedReduce` / an "emit on fire" flag, keeping the incremental mode
   available. Highest-value feature-correctness item.

7. **Per-`(key, window)` reduce state is never evicted** — `operator/reduce.go`
   (`StateKey`). Every distinct window creates a new state entry that's never
   removed, so a long-running windowed pipeline grows memory without bound.
   Fix: drop a window's reduce state when the window fires / is GC'd past
   allowed-lateness.

8. **Session-window multi-merge** — `operator/window.go` (~line 217) /
   `window/session.go`. A record bridging two sessions merges only the first
   overlap, leaving fragments. Cross-confirmed by both auditors. Fix: coalesce
   all overlapping sessions.

---

## Tier 3 — Robustness & failure handling

9. **Failure policies not honored.** `DeserFailureFail` drops instead of failing
   (`source/kafka.go:284`); source errors are logged then swallowed so `Execute`
   can report success after a fatal source error (`pipeline/stage.go:58`); a
   serializer error logs then writes the raw record (`sink/kafka.go:253`); the
   batch timeout doesn't flush partial batches (`sink/kafka.go:197`).

10. **Reconciler blocks the whole loop on backoff** — `control/reconcile.go:99`.
    `maybeRestart` sleeps synchronously in the single-threaded reconcile loop, so
    one crash-looping job stalls every other job. Fix: record a
    "restart-not-before" timestamp and skip until it passes.

11. **K8s backend `Stop` ignores the timeout** — `control/backend/kubernetes.go`
    (~line 313). No graceful-termination wait; set `GracePeriodSeconds` and poll.

---

## Tier 4 — Observability & operability (build on what we just added)

The production test showed these gaps directly.

12. **Metrics/health server by default.** The standalone pipeline needed a
    hand-written `:18080` server. Bake a standard `/metrics` + `/healthz` server
    into `sdk.Run` and the runner (opt-out via env), so *every* job is scrapable
    with zero boilerplate. The dashboard-managed jobs already get this via the
    jobagent — unify the two paths.

13. **Consumer-group lag & offset visibility.** The "0 records read" confusion
    was a stale consumer group resuming from a committed offset. Surface
    per-partition offset/lag in `/state` and the dashboard, and document/flag the
    "fresh group replays from earliest" behavior.

14. **Dashboard metrics: history + Grafana deep-link.** The new in-app Metrics
    page is point-in-time. Add (a) small sparklines / a short rolling window, and
    (b) an optional controller flag (`--grafana-url`) that, when set, adds an
    "Open in Grafana" link per job. Aggregate/all-jobs metrics view.

15. **Wire dashboard-managed jobs into Prometheus automatically.** Today the
    scrape target is pinned to a job id. Add a `/metrics` federation or
    service-discovery endpoint on the controller that lists all live jobs, so
    Prometheus discovers them without hand-editing `prometheus.yml`.

---

## Tier 5 — Features (from README "Up next" + plans/)

16. **Allowed-lateness + side outputs** for late data (pairs with #7 eviction).
17. **Multi-stream joins.**
18. **Typed `Stream[T]` API** — replace raw `[]byte` payloads with generics.
19. **User-facing keyed-state API** — a Flink-style `ProcessFunction` with direct
    state access (today state is engine-internal to Reduce/Window).
20. **S3 blobstore for savepoints** — cross-host / cross-job savepoint restore
    (the control-plane README and plans already scope this).
21. **Function registry** for `map`/`flatMap`/`process` refs in declarative
    workflows (currently rejected by the compiler).

---

## Tier 6 — CI / DevEx (cheap, prevents regressions)

22. **CI covers only the root module.** `go vet ./...` and tests run from the
    repo root; the `control/` module isn't vetted/tested in CI, and the `fmt`
    job already caught unformatted files. Add `control/` to the CI matrix and a
    `gofmt` pre-commit hook.
23. **SDK-job Docker build ergonomics.** The `replace => ../weibo` can't resolve
    inside a Docker build; the weibo-test workaround compiles the binary on the
    host first. Document this as the supported pattern (or provide a
    multi-module build context) for anyone shipping SDK jobs.
24. **Node 20 → 24 deprecation warnings** in the GitHub Actions run — bump the
    action versions (`checkout`, `setup-go`, `upload-artifact`).

---

## Suggested sequencing

- **Sprint 1 (correctness):** Tier 1 (#1 first — it's the exactly-once bug), then
  Tier 2 #6–#7 (windowed final-emit + state eviction go together).
- **Sprint 2 (hardening + ops):** Tier 3, plus Tier 4 #12–#13 while the testing
  context is fresh.
- **Sprint 3+ (features):** Tier 5, starting with allowed-lateness (#16), which
  the windowing work in Sprint 1 sets up.
- **Continuous:** Tier 6 alongside everything.

The single highest-leverage item is **#1 (multi-partition checkpoint offsets)** —
it's the difference between "claims exactly-once" and "is exactly-once" on any
realistic multi-partition topic.
