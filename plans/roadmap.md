# Weibo — Repository Audit Roadmap

Fresh audit: 2026-09-12. This supersedes the old roadmap, which mixed completed
work with stale findings.

## Current baseline

Weibo already has bounded backpressure, keyed parallel stages with marker
alignment, memory/Pebble state, coordinated checkpoints, transactional Kafka
output, declarative and SDK jobs, Docker/Kubernetes control backends, and broad
CI coverage.

The ordering below is **correctness → durability → control-plane safety →
operability → features**. Every item includes tests, documentation, and
observable failure behavior.

---

## P0 — Checkpoint and delivery correctness

### 1. Make source positions topic-aware end to end — ✅ DONE

**Finding:** Kafka operational state is keyed by `(topic, partition)`, but
`offsetTracker.consumed` and the barrier injector in the execution engine used only
`partition`. `CheckpointOffset`, `RestoreOffset`, and `CommitOffsets` serialize
`{"<partition>": offset}`. Two topics using partition `0` can overwrite each
other in one checkpoint.

**Shipped:** source positions now use a versioned topic/partition/next-offset
envelope. Kafka records carry topic identity; barrier tracking, fallback
snapshots, restore, broker commits, and `/state` share that identity. Existing
single-topic partition-only checkpoints remain readable; ambiguous legacy
multi-topic restore fails explicitly. Consumer-group recovery resets broker
positions from the checkpoint before normal reading begins.

**Exit criteria:** collision unit tests, restart tests, and a real Kafka
multi-topic exactly-once E2E test.

### 2. Complete filesystem crash durability and retention — ✅ DONE

**Finding:** checkpoint/blob files are fsynced before rename, but parent
directories are not fsynced after renames. `SweepOrphans` and state-dir deletion
exist but are not wired into normal startup/retention.

**Change:** fsync parent directories; enforce retention for completed/prepared
checkpoints; sweep orphans at startup; preserve unresolved prepared
transactions; recover when latest pointers or files are damaged.

**Shipped:** atomic checkpoint and blob renames now fsync their parent
directories. File storage performs startup pointer repair and orphan sweeping,
retains a configurable number of completed recovery points (three by default),
never garbage-collects prepared commit decisions, and removes matching native
state directories with expired checkpoint metadata. YAML exposes
`checkpointing.retainCompleted`; SDK jobs expose `CHECKPOINT_RETENTION`.

**Exit criteria:** fault-injection tests for partial files, missing pointers,
orphan directories, prepared checkpoints, and retention boundaries.

### 3. Replace timeout-based transaction-marker absence detection — ✅ DONE

`TxnKafkaSink.WasCommitted` currently uses two empty five-second polls to infer
absence. Read marker partitions explicitly and terminate from consumer position
versus the read-committed boundary. Document marker-topic partitioning,
compaction, and retention.

**Shipped:** recovery snapshots Kafka's last-stable offset for every marker
partition, consumes under `read_committed`, and concludes absence only once all
partitions reach those boundaries. Timeouts are no longer evidence of absence;
broker failures and cancellation propagate as errors. Unit boundary tests and a
real-Kafka committed/aborted/absent integration test cover the protocol.

### 4. Expand the crash-consistency matrix — ✅ DONE

Cover failures around barrier injection, operator snapshot, prepared-file
persistence, sink commit, completed promotion, and advisory offset commit.
Run it with memory/Pebble state and single-/multi-partition Kafka.

**Shipped:** the test hook now exposes barrier capture, completed operator
snapshot, and sink preparation in addition to every durable coordinator step.
The matrix crashes and recovers a keyed pipeline at all seven boundaries with
one and three source partitions against both memory and Pebble backends,
asserting one visible output per input and exact restored counts. Unknown marker
outcomes refuse recovery. The matrix also found and fixed stale Pebble working
state: startup now resets state not represented by the selected checkpoint and
propagates reset/restore failures instead of processing inconsistent state.

---

## P1 — Control-plane consistency and recovery

### 5. Make launch and lifecycle bookkeeping atomic

**Finding:** a backend resource is launched before `CreateRun` succeeds, so a
store error can leave an untracked live container. Several store/transition
errors are discarded. Concurrent restart/reconcile calls have no per-job lock
or database constraint preventing two active runs.

**Change:** serialize operations per job; enforce one active run in the store;
persist a starting run before launch and attach the backend ID afterward;
compensate by removing resources after persistence failures; transact run and
transition updates; propagate all store failures.

**Exit criteria:** concurrent-operation tests, injected store failures at every
launch step, and orphan-resource reconciliation tests.

### 6. Recover initial launch failures

Model transient first-launch failures as restartable with persisted backoff.
Separate permanent validation/image failures from backend failures and expose
the next retry time.

### 7. Define durable secret recovery

**Finding:** API-supplied secrets live only in controller memory. After a
controller restart, an automatic relaunch can start without required secrets.

Add a `SecretProvider` abstraction with environment and Kubernetes Secret
references. Never return resolved values. If references cannot resolve, place
the job in an explicit blocked state instead of launching incomplete.

### 8. Add backend resource garbage collection — ✅ DONE

**Finding:** restarts stopped the old container without removing it (one
leaked exited container / Job+Service+ConfigMap+Secret set per attempt),
durable volumes/PVCs had no deletion path at all, terminal-run rows grew
without bound, and labeled backend resources with no store reference were
never reclaimed.

**Shipped:** restarts remove the previous attempt's backend resource;
`DELETE /jobs/{id}` gains `?deleteData=true` (plus `weibo delete
-delete-data` and `Controller.DeleteWithOptions`) that wipes the Docker
volume / K8s PVC — default deletion still preserves durable state;
terminal-run history is bounded (`TerminalRunRetention`, default 5 newest,
backend container + rows + transitions pruned via
`store.PruneTerminalRuns`); `Controller.SweepOrphans` removes exited
managed resources the store no longer references at dashboard startup
(running unknowns are reported, never removed).

**Exit criteria:** restart-leak, preserve-by-default/explicit-wipe,
retention-bound, and orphan-sweep unit tests; k8s-tagged backend suite green.

### 9. Make controller health dependency-aware

Add controller `/livez` and `/readyz`. Readiness should check the store and
selected backend; health should report degraded dependencies without exposing
credentials or internal network details.

---

## P2 — Runtime API and failure semantics

### 10. Remove panic as the operator/sink error channel

Invalid connector construction, Pebble mutations, and `Process` failure
policies currently communicate through panic. Add error-returning constructors
and an error-aware processor contract; retain panic wrappers only for backward
compatibility and preserve operator/record context in structured errors.

### 11. Separate compile-time validation from live resources

**Finding:** compiling a Postgres sink creates a pool, while dry-run validation
discards the compiled environment without a close lifecycle. Documentation
claims this proves reachability, although `pgxpool.New` does not connect.

Make compilation side-effect free, add explicit runtime `Open/Close` hooks, and
provide a separate opt-in connectivity check with bounded timeouts.

### 12. Finish window lifecycle semantics

- evict the final closed window on source completion;
- implement allowed lateness and late-record side outputs;
- delay reduce-state eviction by the lateness bound;
- define idle-partition behavior for multi-partition watermarks.

### 13. Formalize source and sink capabilities

Replace scattered type assertions with declared capabilities for checkpointing,
draining, offset commits, transactions, operational state, and lifecycle.
Reject incompatible combinations before execution.

---

## P3 — Observability and operations

### 14. Add controller-native metrics aggregation and discovery — ✅ DONE

**Shipped:** `ControllerMetrics` on a private registry (process + Go
collectors, reconcile count/duration, launch outcomes
`success|transient|permanent|blocked|record_failed`, live job/run
inventory gauges read from the store per scrape, sweep counters, per-route
API counts/latency) served at public `GET /metrics`; `GET /targets`
(auth-gated) lists live job agents in Prometheus http_sd format with
stable `weibo_job_id`/`weibo_job_name` labels. Metric labels carry only
small enumerations (mux route templates, never raw IDs); discovery runs at
most 8 concurrent backend probes under a 15s deadline. Includes
`control/kubernetes-servicemonitor.yaml` (ServiceMonitor + commented
`weibo-jobs` scrape job).

**Exit criteria:** endpoint, route-normalization, cardinality,
launch-kind, and discovery unit tests; k8s-tagged backend suite green.

### 15. Add short metrics history and Grafana links — ✅ DONE

**Shipped:** the controller samples every live job's agent (`/state` plus
edge-queue/error counters parsed from agent `/metrics`) on a tick
(`--history-interval`, default 15s) into a bounded in-memory ring (240
samples/job, ~1h; process memory only, never SQLite; dropped on job
deletion). `GET /jobs/{id}/history` and bulk `GET /jobs/history` serve
downsampled series; the dashboard renders a fleet-throughput tile, a
per-job trend column, and a detail throughput chart with lag/queue/error
tiles (rates derived client-side from counter deltas). `--grafana-url`
(env `WEIBO_GRAFANA_URL`, exposed via `GET /config`) adds per-job deep
links to a `weibo-job` dashboard (`var-job`/`var-run`).

**Exit criteria:** ring/downsample/drop, sampler success/skip, lag and
exposition-parser, history/config API, and dashboard-hook unit tests;
sampling reuses the bounded discovery probes (8 concurrent, 15s deadline,
5s per-target timeout).

### 16. Improve diagnostic APIs — ✅ DONE

**Shipped:** engine `CheckpointReport` observer (barrier injection →
completion duration plus inline snapshot bytes on both completion paths;
agent serves duration/size per checkpoint) with the legacy ID listener
kept; `GET /jobs/{id}/diagnostics` assembling failure classification +
hint, last activity (rolling history), live restart countdown, and latest
checkpoint duration/size; `GET /jobs/{id}/runs` + `/runs/{runId}` (+
`/logs` with 404 unknown / 410 container-removed) for previous-run
selection; cursor-paged `GET /jobs/{id}/transitions` (`limit` default 50,
max 200); SSE `GET /jobs/{id}/logs/stream` (tail burst, 2s suffix polls,
capped deltas, heartbeat). Dashboard: Diagnostics card, Runs tab, Follow
toggle (fetch streaming, so the bearer token still applies), audit Older
button. CLI: `weibo runs`, `weibo logs -follow`.

**Exit criteria:** observer duration/size, paged-transition, diagnosis,
restart-countdown, run-logs 404/410, paging cursor, SSE burst, sampler
checkpoint-stats, and dashboard-hook unit tests; suites green.

### 17. Add tracing and structured logging — ✅ DONE

**Shipped:** stdlib `observability/log` (levels, text/JSON, `Secret`
redaction type, env-key-names helper) and `observability/trace`
(coarse-span contracts, no-op default, log correlation) with zero new
engine dependencies; engine/coordinator/agent log structured lines and
span recovery, checkpoint saves/finalizes, and job runs; Postgres/Kafka
sinks log batch failures and span flushes (DSN/SASL never logged);
controller moved from `Logf` to `Logger`/`Tracer` options with spans on
launch/reconcile/sweep/savepoint; OTLP export lives in the separate
`telemetry/` module (workspace member) wired by `--otel-endpoint` on the
dashboard and `OTEL_*` env on the runner, so library users pull no
tracing clients. Secrets and the bearer token never enter logs, metrics,
or API bodies (tested).

**Exit criteria:** redaction, log/trace contract, OTLP-to-local-collector,
coordinator/agent/sink/adapter/controller-span, and API-bodies unit
tests; engine, control (both tag sets), and telemetry suites green.

---

## P4 — Kubernetes and production hardening

### 18. Ship deployable controller manifests — ✅ DONE

**Shipped:** `control/kubernetes-controller.yaml` provides a controller
ServiceAccount/Role/RoleBinding, ConfigMap, SQLite PVC, single-replica
`Deployment` (`Recreate`, `weibo dashboard -backend kubernetes`), Service,
`/livez` and `/readyz` probes, non-root/read-only-root filesystem defaults, and
PodDisruptionBudget. Docs now cover token creation, replacing the placeholder
controller image, setting `WEIBO_RUNNER_IMAGE`, and the SQLite single-replica
constraint until leader election/shared storage exists. A manifest test checks
the production basics.

### 19. Harden job isolation

Add least-privilege service accounts, NetworkPolicies, pod/node placement
options, priority/runtime classes, ephemeral-storage limits, PVC quotas, and
safe per-job overrides.

### 20. Improve Kubernetes operability

Use watches/informers instead of repeated polling, expose Kubernetes events,
define cleanup finalizers/TTL behavior, handle PVC resize failures, and run a
scheduled real-kind lifecycle suite.

### 21. Add object-store savepoints/checkpoints

Implement S3-compatible storage with encryption, integrity metadata, multipart
uploads, retries, and lifecycle guidance for cross-node/cross-job restore.

---

## P5 — Security

### 22. Strengthen controller authentication

Keep shared-token mode locally; add hashed tokens or OIDC, scoped roles,
rotation, and mutation auditing. Warn or fail on insecure public binding unless
explicitly acknowledged.

### 23. Bound and validate API inputs

Reject oversized bodies rather than silently truncating them, bound log tails,
configure HTTP server timeouts, rate-limit mutations, and normalize HTTP status
mapping.

### 24. Automate dependency and artifact security

Add `govulncheck`, dependency updates, SBOMs, image/binary provenance and
signing, container scanning, minimal workflow permissions, and immutable pins
for third-party CI actions.

---

## P6 — Test, CI, and developer experience

### 25. Keep local and hosted CI equivalent

**Finding:** the Makefile still tests only `test/unit_tests/...`, while hosted
CI tests all root, control, and Kubernetes-tagged packages.

Update local test/race/coverage/CI targets for both modules and Kubernetes tags.
Add checks for workflows, Dockerfiles, shell, YAML, and documentation links.

### 26. Add missing integration tiers

- Kafka: multi-topic recovery, transactions, auth/TLS, rebalance, broker loss,
  and partition expansion;
- Postgres: retry, upsert, disconnect, and shutdown flush;
- HTTP/S3: retry/idempotency and savepoint round trips;
- Kubernetes: kind submit/readiness/state/savepoint/restart/delete;
- browser: dashboard lifecycle, auth, and metrics rendering.

Keep fake-client suites on every pull request; run fault/cluster suites on merge
or nightly schedules.

### 27. Add fuzzing and quality gates

Fuzz workflow parsing, record paths, checkpoint/archive inputs, and API request
decoding. Publish root/control coverage separately and gate changed-package
regressions before adopting a global threshold.

### 28. Clean naming and documentation drift — ✅ DONE

**Shipped:** renamed `mailer.go` to `engine.go`; documented controller/job
`/livez` versus `/readyz` probes and kept `/healthz` as compatibility; corrected
stale Postgres dry-run wording to describe side-effect-free validation; narrowed
savepoint-storage claims to the storage namespace actually shared by each
backend; added `docs/checkpoints-and-api.md` for checkpoint schema and public
API compatibility; removed tracked generated binaries (`kafka-orders`,
`s3-demo`) and ignored them going forward.

---

## P7 — Product features (after P0–P3)

29. Multi-stream joins with watermark alignment and checkpointed join state.
30. Typed `Stream[T]` API with a migration path from `[]byte` records.
31. User-facing keyed state/process functions with timers.
32. Declarative function registry for map/flatMap/process references.
33. New production connectors after capability/lifecycle contracts stabilize.

---

## Recommended execution sequence

1. **Sprint A:** ~~topic-aware positions~~, filesystem durability,
   deterministic transaction recovery, crash matrix (#1–#4).
2. **Sprint B:** atomic controller lifecycle, launch recovery, durable secrets,
   garbage collection, controller health (#5–#9).
3. **Sprint C:** error/lifecycle contracts and window completion (#10–#13).
4. **Sprint D:** metrics, diagnostics, Kubernetes productionization, security,
   and CI hardening (#14–#28).
5. Begin product expansion only after those foundations (#29–#33).

**Next task:** #19, harden job isolation.
