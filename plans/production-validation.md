# Production-style validation of Weibo

Status (2026-09-24): partly carried out. Phase 0 done; Phase 1 infra built. A real AWS deploy
(Phase 5) and a 10,000 events/s load test with injected failures were run on EC2; their results
and the bugs they found are recorded in [docs/benchmarks.md](../docs/benchmarks.md). Phases 2-4
(source-path fidelity, the full failure-injection drill list, and a load/soak baseline) were not
carried out as written below. Owner: @ASHUTOSH-SWAIN-GIT.

## Context

Weibo is a Flink-inspired Go stream processor plus a control plane (`weibo dashboard`) that runs
one Docker container per job. The engine, the declarative workflow layer, the SDK, the Kubernetes
backend, CI gates and the dashboard revamp are all built and marked done.

What has **never happened** is running the thing the way an operator would run it. Four findings
from a full read of the tree all point the same direction:

1. **The flagship end-to-end exerciser is not real.** `control/scripts/dashboard-stream-e2e.sh`
   plus `examples/stream-demo/` are the intended full-system test, but they are untracked, absent
   from `Makefile` and `.github/workflows/`, and — per `control/.playwright-shots/04-sources.png` —
   demo the **degraded** path. The job uses a custom in-process `liveOrderSource`, so `/describe`
   reports `SOURCE · UNKNOWN`, `LAG: not reported`, `POSITION: source does not expose progress`.
   The Sources section that the whole dashboard revamp was anchored on has no live proof, only
   httptest fixtures.

2. **No performance or resilience validation exists at all.** A repo-wide grep for
   `soak|chaos|SLO|load test` returns zero matches. Five micro-benchmarks exist
   (`bench/state_scale_test.go`, `pipeline/stateless_stage_bench_test.go`,
   `operator/keyby_route_test.go`); none is run by any Makefile target or workflow, and none has a
   recorded baseline.

3. **`plans/roadmap.md` is stale in both directions and cannot be trusted as a gap list.** Verified
   against the code:

   | Roadmap item | Roadmap says | Code actually says |
   |---|---|---|
   | #5 atomic launch bookkeeping | open | **fixed** — `startRun` calls `CreateRunWithTransition` (phase `starting`) *before* `backend.Launch`, attaches `ContainerID` after, and on a post-launch store failure calls `backend.Remove` + `markLaunchRecordFailure` (`control/controller.go:690-815`) |
   | #6 launch-failure recovery | open | **largely done** — `FailureLaunchTransient`/`Permanent` split, persisted `run.RestartAt`, `scheduleRestartFrom`, next retry surfaced at `diagnostics.restart.inSeconds` |
   | #7 durable secret recovery | open | **largely done** — `SecretProvider`, `store.SecretRef`, `blockRun` → phase `blocked` + `FailureSecretBlocked`, `maybeUnblock` retries (`control/secrets.go`) |
   | #9 dependency-aware health | open | **done** — `Controller.Ready` checks `store.ListJobs` then `backend.Capacity` (`control/controller.go:600-615`) |
   | #10 panic as error channel | open | **partial** — error-returning `New*SinkE` constructors exist, but `panic` is still live in `operator/keyed_process.go:101,172` and `operator/process.go:80,125` |
   | #29 multi-stream joins | "Next task" (line 474) | **done** — shipped in `be70785` |

4. **The real gaps are ones the roadmap does not list.** Each verified by reading the code:
   - **Paused containers read as healthy.** `Docker.Status` branches only on `info.State.Running`,
     which is `true` for a paused container; nothing checks `State.Paused`, and the reconciler never
     probes the agent's `/livez`. A frozen job reports `phase: running` indefinitely.
     (`control/backend/docker.go:301-318`)
   - **Reconcile head-of-line blocking.** `reconcile` has five `return err` paths inside its loop
     over `ActiveRuns()`. One job with a store error abandons the rest of the pass, so jobs later in
     the ordering never get reconciled and their restarts never fire. (`control/reconcile.go:30-88`)
   - **No checkpoint metric exists.** `observability/metrics/metrics.go` has no checkpoint
     instrument at all. Duration is only reachable as `jobagent.Checkpoint.DurationMs` via
     `GET /jobs/{id}/state` and `control.Sample.checkpointDurationMs` via `/jobs/{id}/history`.
   - **Single-live-run fencing is in-process only.** `startRun` guards with a `sync.Mutex` map plus a
     non-transactional `ListRuns` scan. Two controller processes on one SQLite file can both pass the
     scan and double-launch — which with `sink/kafka_txn.go` means two live producers sharing a
     transactional ID.
   - **Prometheus scrapes a hardcoded target.** `observability/docker/prometheus.yml` points at
     `host.docker.internal:18080` and ignores the `GET /targets` http_sd endpoint that
     `control/discovery.go` already serves. Multi-job observation is impossible until this changes.

**Intended outcome:** exercise Weibo as a production system — real Kafka/Postgres/S3, TLS, token
auth, registry pull — then deliberately break it and write down what happens. Success is not
"it works". Success is **a recorded performance baseline plus a list of confirmed real failure
modes**, neither of which exists today.

**Decisions taken:** prod-like local stack first, then a real AWS free-tier deploy. All four backing
services real (Kafka, MinIO, Postgres, Prometheus+Grafana). Zero spend.

## Environment (verified)

| | |
|---|---|
| Docker | 29.2.1, compose v5.0.2, **10 CPU / 8 GB RAM** — the binding constraint |
| Go / Node | 1.26.6 / v25.9.0 |
| kubectl | installed, **no cluster**; no kind, no minikube |
| Load tools | **none** — no k6, vegeta or hey. Avoid new installs; prefer Go-native or containers |
| AWS | account `492351590994`, IAM **user** `ashutosh-dev`, ECR in `ap-south-1` (`weibo-sdk-demo`) |
| Docker auth | `credsStore: desktop` — **no `ecr-login` credHelper**, which an EC2 host will need |
| Already built | `weibo-runner:ci`, `weibo-stream-demo:dev`, `weibo-ec2-continuous-test:0.1.0` |

## Reusable contract — do not reinvent

`.github/workflows/integration.yml` job `tiers-live-services` already pins the env contract the
`TestLive_*` tests read. The local stack must expose **exactly these names** so
`make test-integration-live` works unchanged:

```
KAFKA_BROKERS=localhost:9092
POSTGRES_DSN=postgres://weibo:weibo@localhost:5432/weibo?sslmode=disable
SAVEPOINT_S3_BUCKET / SAVEPOINT_S3_ENDPOINT / SAVEPOINT_S3_PATH_STYLE=true
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION
```

Test convention is **name prefixes, not build tags**: `TestTier_*` = offline/fake, `TestLive_*` =
needs real backends. There is no `//go:build integration` anywhere.

Also reuse rather than rewrite:
- `observability/docker/docker-compose.yml` — Prometheus + Grafana + provisioned `weibo.json`.
- `control/scripts/dashboard-stream-e2e.sh` — already solves submit/poll/assert/cleanup and asserts
  five read-path invariants. Fork it for the drill and load harnesses.
- `scripts/test-kafka.sh` — existing topic-create / produce / assert-windowed-totals pattern.
- `Makefile` targets `test-integration-live`, `kafka-test`, `ci`.
- `docs/self-hosting.md` — the runbook to execute verbatim in Phase 5.

---

## Phase 0 — Stop losing the harness (~20 min) — DONE

The end-to-end harness was already tracked by `0c1003e`. Remaining work, now complete:

- `.gitignore`: added `/stream-demo` (**anchored** — an unanchored `stream-demo` would also ignore the
  tracked `examples/stream-demo/` source) and `control/.playwright-shots/`.
- `git rm --cached` the 37 MB `stream-demo` binary and the four Playwright PNGs that `0c1003e`
  committed by mistake. Files stay on disk.
- Added `make dashboard-e2e` (wraps `control/scripts/dashboard-stream-e2e.sh --ci`) and fixed the
  `make help` regex, which silently hid any target name containing a digit.
- Baseline smoke test: `make dashboard-e2e` **passes in ~24 s**. Note its own output prints
  `describe: source=Unknown` — the script asserts source/sink/operators are *exposed*, not that the
  source is *identified*, so the degraded path passes. Phase 2 tightens this.

**Side effect to know about:** `0c1003e` is what the published `v1.0.0` engine tag points at, so the
Go module zip for `v1.0.0` contains the 37 MB binary, and it stays in git history. The Go proxy never
forgets a fetched version, so this is fixed only going forward (`v1.0.1`), not retroactively.

## Phase 1 — Prod-like local stack — infra DONE, gate partially red (real findings)

Goal: every backing service real, controller reachable only over TLS with a token — the
`docs/self-hosting.md` security checklist actually satisfied, on one machine.

**Built:** `deploy/compose/docker-compose.prod-like.yml`, `deploy/compose/Caddyfile`,
`deploy/compose/.env.example`, `deploy/compose/README.md`, `make prod-like-up`/`prod-like-down`.
All six services (Kafka, Postgres, MinIO, Prometheus, Grafana, Caddy) come up healthy and were
verified end to end: `weibo dashboard` on the host, reachable only via `https://weibo.localhost`
through Caddy, 401 with no token, 401 with a wrong token, 200 with the right one.

**Host-specific port conflicts found and worked around** (this machine already runs a native
Kafka broker on 9092 and a native Postgres on 5432 — the latter silently wins over Docker's
`0.0.0.0` publish because macOS prefers the more specific `127.0.0.1` bind, so clients connecting
to "localhost" reached the wrong server with no error). Kafka now uses host ports 39092/39093,
Postgres 54320. Ports are machine-specific; another host may need different ones — check first.

**Real bug found and fixed in the compose config itself:** the first listener topology advertised
the external host address (`localhost:39092`) as the *inter-broker* listener. The broker could
accept client connections fine but could not dial **itself** for transaction-coordinator and
marker-propagation traffic (nothing inside the container listens on the host-mapped port), so
`TestLive_KafkaTransactions` hung for a full 60 s timeout. Fixed by giving inter-broker traffic
its own `BROKER://kafka:9092` listener, separate from the `EXTERNAL`/`INTERNAL` ones clients use.

**Gate result running `make test-integration-live` for real, for the first time, against this
stack** — 12 of 16 live/tier tests green; four failures are genuine product/test gaps, not stack
misconfiguration (confirmed by rerunning each in isolation against a healthy stack):
- `TestLive_PostgresUpsert`, `TestLive_PostgresDisconnectAndShutdownFlush` — both write fewer
  records than the batch size and expect a flush anyway; both land 0 rows. Points at
  `sink.PostgresSink` having no flush-on-close / flush-interval path that actually fires before
  `Write` returns, only a batch-size-boundary flush (the 50-row `TestLive_PostgresRetryInsert`,
  which always hits full batches of 10, passes). Needs a code fix in `sink/`, not infra.
- `TestLive_KafkaPartitionExpansion`, `TestLive_KafkaRebalance` — intermittent
  `[3] Unknown Topic Or Partition` immediately after `createLiveTopics` + `waitLiveLeader`.
  `waitLiveLeader` (`test/integration/kafka_tiers_test.go:162`) only confirms partition **0**'s
  leader is ready before the test produces to a topic with 2 partitions — a real gap in the test
  helper itself, exposed only by actually running against a live broker under load.

These four are unresolved — left for a follow-up pass, per this plan's own rule not to fix drill
findings inline. Re-run `make test-integration-live` against `deploy/compose/` after any fix to
confirm.

**New file `deploy/compose/docker-compose.prod-like.yml`** (a dedicated stack, not a change to the
observability compose):

| Service | Image | Notes |
|---|---|---|
| `kafka` | `apache/kafka:3.7.0` | KRaft single node, same env block as CI for parity. Cap with `KAFKA_HEAP_OPTS=-Xmx512m` |
| `postgres` | `postgres:16` | user/db `weibo`, `pg_isready` healthcheck |
| `minio` + `minio-init` | `minio/minio` + `minio/mc` | init container creates the savepoint bucket, replacing CI's manual `mc` steps |
| `prometheus` | reuse `observability/docker/prometheus.yml` | **must** be retargeted — see Phase 4 |
| `grafana` | anon admin, provisioned `weibo.json` | |
| `caddy` | `caddy:2` | **TLS terminator** in front of the controller — the one checklist item untestable without a proxy |

Set compose `mem_limit`s on everything so a job OOM is the job's fault, not collateral.

The controller runs **on the host**, not containerized — the Docker backend needs the daemon socket,
and this is how `self-hosting.md` describes it:

```sh
export WEIBO_AUTH_TOKEN="$(openssl rand -hex 32)"
cd control && go run ./cmd/weibo dashboard \
  -addr 127.0.0.1:9000 -image weibo-runner:dev \
  -db /var/lib/weibo/control.db -no-open
```

Caddy then proxies `https://weibo.localhost` → `127.0.0.1:9000`, and the CLI targets the HTTPS URL so
the token never crosses in cleartext.

**Settle before writing the compose file:** the Kafka advertised-listener problem. The host reaches
the broker at `localhost:9092`, but a job *container* cannot. Either add a second listener
advertising `host.docker.internal:9092` (the pattern the observability compose already uses via
`extra_hosts`), or attach job containers to the compose network — check whether
`control/backend/docker.go` supports joining an external network. If it does not, the dual-listener
route is the only option.

**Gate:**
- `make test-integration-live` green (`WEIBO_LIVE=1`) — first time `TestLive_Kafka*`,
  `TestLive_Postgres*`, `TestLive_S3*` run outside CI.
- `scripts/test-kafka.sh` green against the real broker.
- `weibo jobs` works through Caddy over HTTPS; a wrong token gets 401, a `readonly:` hashed token
  gets 403 on mutations.

## Phase 2 — Make the demo exercise the *real* source path

Highest-value single change, and a genuine bug hunt rather than plumbing. Today the dashboard's
Sources section is only ever demoed in its "not reported" degraded path.

- Add a Kafka-backed variant of the stream-demo (new manifest + env switch, or promote
  `examples/kafka-orders/`) so the source is a real `source/kafka.go` with partitions, offsets and a
  watermark tracker.
- Confirm Sources renders **real** identity, partition/offset position and lag. **If it still shows
  `Unknown`, that is a real dashboard bug and the first finding of this exercise.**
- Point checkpoints at MinIO (`SAVEPOINT_S3_*`) so `checkpoint/s3_blobstore.go` runs against real
  object storage; take a savepoint and restart from it.
- Use `sink/kafka_txn.go` (Kafka → Kafka) so the exactly-once claim in `README.md:432` is under test.

**Gate:** extend `dashboard-stream-e2e.sh` with assertions for non-empty source identity and a
numeric lag, so the degraded path can never silently return.

---

## Phase 3 — Failure-injection drills

Shorthand used throughout. Dependencies in a compose project `weibodrill`; controller on the host.

```bash
J=<jobId>; B=https://weibo.localhost
CID()   { docker ps -aq --filter "label=weibo.job=$J"; }
PHASE() { curl -s $B/jobs/$J | jq -r .latestRun.phase; }
KIND()  { curl -s $B/jobs/$J | jq -r '.latestRun.failureKind // "-"'; }
```

Run in three blocks so failures do not contaminate each other:
**(a)** no-infra data-correctness — D5, D6; **(b)** single-job faults — D1, D2, D11, D12, D13, D7,
D8; **(c)** control-plane, destroys controller state, do last — D3, D4, D9.

### The five highest-value drills

**D5 — Silent late-record and final-window loss.** *(gap: window lifecycle)*
No infra fault; pure data assertion, so it is the cheapest and most likely to find real data loss.
Produce a known keyed corpus including one record 30 s behind the watermark, then
`POST $B/jobs/$J/cancel` mid-window. Compare output sum against input sum, and watch
`weibo_records_failed_total`.
*Predicted:* **silent loss on both counts.** No `lateness` handling exists in `operator/` (the term
appears only in `watermark/`, `workflow/validate.go`, `stream.go`), and there is no final-window
eviction on drain. The loss is invisible — `weibo_records_failed_total` stays 0.

**D2 — Wedged job reads as healthy.** *(gap: `Docker.Status` ignores `State.Paused`)*
```bash
docker pause $(CID)   # hold 120s
```
*Assert:* `PHASE` stays `running`; `GET /jobs/$J/state` **times out**; Prometheus
`up{weibo_job_id="$J"} == 0`; `weibo_edge_queue_size` frozen at its last scrape.
*Predicted:* **indefinite false-healthy.** Dashboard shows green while throughput is zero. This is
the drill the `up{job="weibo-jobs"}` Grafana panel (Phase 4, item 7) exists to catch.

**D3 — Concurrent restart / double active run.** *(gap: in-process-only fencing)*
Ten parallel `POST $B/jobs/$J/restart`, then the real test: two controller processes against the
same `control.db`, each issuing a restart.
*Assert:* `docker ps -q --filter label=weibo.job=$J | wc -l` must be `1`;
`curl -s $B/jobs/$J/runs | jq '[.runs[]|select(.stoppedAt==null)]|length'` must be `1`.
*Predicted:* single-process passes (`lockJob` + scan); **two-process double-launches**, because the
mutex is per-process and the `ListRuns` scan is not transactional. With `sink/kafka_txn.go` that is
two live producers on one transactional ID — exactly-once silently broken.

**D4 — Secret loss across controller restart.** *(#7)*
`kill -9` the controller; while down `docker kill $(CID)`; restart it *without* the submit-time env.
*Assert:* `PHASE` → `blocked`; `KIND` → `secret_blocked`; `diagnostics.failure.title` ==
`"Blocked on secrets"`.
*Predicted:* the block itself works. **Two residual bugs to check:** (a) `secretRefsFromEnv` turns
every submit-supplied key into `{provider:"env", Name:key}`, so resolution silently falls back to the
*controller's own* env — if the operator happens to export the same var name, the job relaunches with
the **wrong value and no signal**; (b) `maybeUnblock` does `finishRun(Blocked→Failed)` then
`launchLocked(attempt+1)`, so each unblock burns a restart attempt and a job blocked 5× is
permanently dead at `MaxAttempts:5`.

**D1 — Hard job crash and checkpoint recovery.** *(restart policy, #6)*
`docker kill -s KILL $(CID)`.
*Assert:* phase `running`→`restarting`→`running`; `latestRun.attempt` +1;
`GET /jobs/$J/state .currentCheckpointId` non-empty and `.recordsIn` resumes near the pre-kill
checkpoint, not 0; `weibo_controller_launches_total{result="success"}` +1.
*Predicted:* state recovery passes, but **every healthy auto-restart records a `failed` run** —
`maybeRestart` calls `finishRun(..., Failed, "restart backoff elapsed")` (`control/reconcile.go:243`),
so a self-healing job accrues `failed` runs in the UI history. Cosmetic but alarming.

### Remaining drills

| # | Probes | Injection | Predicted |
|---|---|---|---|
| D6 | Validation creates live resources (#11) | Stop Postgres, then 200× `POST /validate` with a pg-sink workflow | **Monotonic `go_goroutines` and `process_open_fds` growth** — `pgxpool.New` spawns health-check goroutines and `Validate` discards the env without `Close`. Also returns `200 OK` against a *stopped* Postgres, so "validation proves reachability" is false. Cheapest drill in the set — promote if time is short |
| D7 | Broker loss mid-stream (#10) | `docker stop` Kafka 60 s, then start | **Process death via panic**, not a returned error (`operator/process.go:80`). Exit code 2 with a Go trace in `/jobs/$J/logs`, indistinguishable from a bug |
| D8 | Broker loss inside the 2PC window | Kill Kafka within ~1 s of a checkpoint boundary, then kill the job | Expect pass. Real risk: recovery **refuses to start on an unknown marker outcome** → run fails permanently and burns attempts. Assert on `.latestRun.error` |
| D9 | Store unavailable + head-of-line blocking | Hold `BEGIN EXCLUSIVE;` on the SQLite file 60 s with 4 jobs running | `/readyz` → 503 correctly. But **one failing job aborts the whole reconcile pass**; jobs later in `ActiveRuns()` order never reconcile and their restarts never fire. No metric distinguishes "reconciled 4/4" from "1/4" |
| D11 | OOM kill | `docker update --memory 64m --memory-swap 64m $(CID)` | Functionally restarts, but **no distinct signal**: `handleExit` treats OOM like any crash, `failureKind` stays empty, `diagnostics.failure` is `null`. After `MaxAttempts:5` the job is silently dead |
| D12 | Checkpoint storage exhausted | Pre-create the job volume as a 24 MB tmpfs before first launch (the backend reuses it — `docker.go:188`) | Durability half should pass; **predict the error surfaces as a panic** rather than a `stage_errors` increment. This is the correct way to test disk-full on Docker Desktop — do not fill the shared VM disk |
| D13 | Corrupted checkpoint | `dd` random bytes over the newest checkpoint file from a helper container | Expect a clean refusal. **Assert the negative hard:** `.recordsIn` starting at 0 with `phase: running` is a *failure*, not a recovery |
| D14 | Network degradation | netns sidecar `tc qdisc add dev eth0 root netem delay 400ms loss 5%` | Expect bounded backpressure. Watch `GET /cluster .memoryUsedBytes` — if RSS climbs while read rate drops, buffering is leaking off the metered edges |
| D15 | Savepoint blob store loss | Stop MinIO, then `POST /jobs/$J/savepoint` | **Confusing `202`-then-death:** savepoint is fire-and-forget and stop-with-savepoint drains the job, so a failed upload likely ends the run with no savepoint — `accepted` reported for an operation that lost the job |

### Exactly-once verification (used by D3, D7, D8)

Drain the output topic under `read_committed`, then assert:
- **No duplicates:** `cut -f1 out.tsv | sort | uniq -d` empty. Run `read_uncommitted` too and diff —
  duplicates visible only there proves the transaction boundary is doing the work.
- **No loss:** output key count equals input key count; `comm -23` of the key sets is empty.
- **Per-key aggregate exactness:** compare per-key sums, not just key presence — a duplicate *within*
  a window inflates a sum without producing a duplicate key.
- **Offset monotonicity:** `GET /jobs/$J/state .source` never regresses below the last completed
  checkpoint's offset.

---

## Phase 4 — Load and soak baseline

### 4.1 Prerequisites (do these first — the methodology is blocked on them)

1. **Add checkpoint metrics.** `observability/metrics/metrics.go` has none. Add
   `weibo_checkpoint_duration_seconds` (histogram), `weibo_checkpoint_size_bytes`,
   `weibo_checkpoints_completed_total`, `weibo_checkpoints_failed_total`. Until then, sample
   `checkpointDurationMs` from `GET /jobs/$J/history?points=120`.
2. **Retarget Prometheus** — `observability/docker/prometheus.yml` scrapes a hardcoded
   `host.docker.internal:18080` and ignores the existing `/targets` http_sd endpoint:

```yaml
scrape_configs:
  - job_name: weibo-jobs
    scrape_interval: 5s
    http_sd_configs:
      - url: http://host.docker.internal:9000/targets
        refresh_interval: 10s
        # authorization: { credentials: "<token>" }   # /targets is auth-gated
    metric_relabel_configs:
      # weibo_run_phase is a MUTABLE target label: keeping it forks a new series
      # on every phase change and breaks rate() across a restart.
      - action: labeldrop
        regex: weibo_run_phase
  - job_name: weibo-controller
    scrape_interval: 15s
    static_configs: [{ targets: ["host.docker.internal:9000"] }]
```

### 4.2 Load generator

**Build `examples/loadbench/` — do not load-test `stream-demo` as-is.** Three defects in
`examples/stream-demo/main.go` cap it far below the engine's ceiling and would measure the wrong
thing:
- one `time.Ticker` tick **per record** (`interval = time.Second / perSecond`) — above a few thousand
  rps the ticker coalesces and you silently get less load than requested, so `RECORDS_PER_SECOND`
  stops being the independent variable;
- a `fmt.Printf` per output record plus `NewStdoutSink()` — the Docker `json-file` log driver becomes
  the bottleneck and pollutes latency;
- **three distinct keys** across `WithPartitions(4)` — one partition is permanently idle and there is
  no cardinality or skew control.

`loadbench` is a near-copy with four changes: batch emission (one tick per ms emitting `rate/1000`);
`KEY_CARDINALITY` (default 1000) with optional Zipf skew; `SINK=blackhole|kafka` using the existing
`sink/blackhole.go`; no per-record `Printf`. Keep the same filter → keyBy → tumbling → reduce shape so
numbers stay comparable. Make `WithPartitions(N)` env-driven as a second ramp axis.

Rejected alternatives: a Kafka producer as *primary* puts the broker inside the measurement and eats
the headroom being measured on a 10-CPU box; N concurrent jobs measures the controller and Docker
scheduling, not stream throughput (keep it as a separate axis). k6/vegeta/hey are HTTP tools and are
the wrong shape — the load is records into a source, not requests.

For the Kafka path (needed by D5/D7/D8): `kafka-producer-perf-test.sh` via `docker exec` for
throughput, plus a ~60-line Go producer at `bench/cmd/loadgen/` for the **correctness corpus** —
`kafka-producer-perf-test.sh` emits random unkeyed payloads, which makes the duplicate/loss
assertions impossible.

### 4.3 Ramp schedule

One job, `WithPartitions(4)`, `KEY_CARDINALITY=1000`, `CHECKPOINT_INTERVAL=5s`, resources `cpu: "1"` /
`memory: "1Gi"` (up from the demo's `250m`/`256Mi`).

| Step | `RECORDS_PER_SECOND` | Hold |
|---|---|---|
| 0 | 1 000 | 3 min warm-up — **discard** |
| 1 | 5 000 | 5 min |
| 2 | 10 000 | 5 min |
| 3 | 20 000 | 5 min |
| 4 | 40 000 | 5 min |
| 5 | 80 000 | 5 min |
| 6 | ×2 from last passing step | 5 min, repeat until stop |

Score each step on its **last 3 minutes only**. Max sustainable throughput = achieved throughput of
the last fully passing step. A step fails if any of:

1. **Achieved < 95 % of offered** — `rate(weibo_records_read_total[1m]) < 0.95 * RPS`. Distinguishes
   generator saturation from engine saturation: if the generator is the limiter, edge queues are
   *empty*, not full.
2. **Sustained backpressure** — `max(weibo_edge_queue_size / weibo_edge_queue_capacity) > 0.9` for
   > 60 s continuous.
3. **Latency cliff** — sink or operator p99 > 3× the step-1 baseline.
4. **Checkpoint cadence lost** — checkpoint duration exceeds `CHECKPOINT_INTERVAL`.
5. **Any nonzero error rate** — `rate(weibo_records_failed_total[1m]) > 0` or
   `rate(weibo_stage_errors_total[1m]) > 0`.
6. **Memory pressure** — `memoryPercent > 85` from `GET /cluster`, or an `OOMKilled` exit.

Also require `latestRun.phase == running` and `latestRun.attempt` unchanged for the entire ramp — a
silent restart invalidates the whole run.

### 4.4 Metric set (PromQL)

| Quantity | Query |
|---|---|
| Read rate (offered vs achieved) | `rate(weibo_records_read_total[1m])` |
| Processed per operator | `sum by (operator) (rate(weibo_records_processed_total[1m]))` |
| Goodput | `rate(weibo_records_written_total[1m])` |
| Operator p50/95/99 | `histogram_quantile(0.99, sum by (le, operator) (rate(weibo_operator_latency_seconds_bucket[1m])))` |
| Sink p50/95/99 | `histogram_quantile(0.99, sum by (le) (rate(weibo_sink_write_latency_seconds_bucket[1m])))` |
| Per-worker skew | `histogram_quantile(0.99, sum by (le, operator, worker) (rate(weibo_operator_worker_processing_duration_seconds_bucket[1m])))` |
| **Backpressure: fill ratio** | `max by (edge) (weibo_edge_queue_size / weibo_edge_queue_capacity)` |
| **Backpressure: block fraction** | `rate(weibo_stage_send_block_seconds_total[1m])` — near 1.0 means that stage is blocked ~100 % of wall time; **the highest-value `stage` is the bottleneck** |
| Stage imbalance | `rate(weibo_stage_records_in_total[1m]) - rate(weibo_stage_records_out_total[1m])` |
| Errors | `rate(weibo_records_failed_total[1m])`, `rate(weibo_source_errors_total[1m])`, `rate(weibo_sink_errors_total[1m])`, `sum by (stage) (rate(weibo_stage_errors_total[1m]))` |
| Controller overhead | `rate(weibo_controller_reconciles_total[5m])`, `histogram_quantile(0.99, rate(weibo_controller_reconcile_duration_seconds_bucket[5m]))`, `process_resident_memory_bytes{job="weibo-controller"}`, `go_goroutines{job="weibo-controller"}` |

**Container RSS/CPU — no cAdvisor, and do not add one** (unreliable on Docker Desktop macOS, ~200 MB
of a 8 GB budget). Use the control plane's own stats, which already wrap `docker stats` and subtract
page cache (`control/backend/docker.go:536-538`) — exactly what a leak slope needs:

```bash
while :; do
  curl -s "$B/cluster" | jq -r --arg t "$(date -u +%FT%TZ)" \
    '.containers[] | select(.managed) |
     [$t,.jobId,.cpuPercent,.memoryUsedBytes,.memoryPercent,.pids,
      .networkRxBytes,.networkTxBytes,.blockWriteBytes] | @tsv'
  sleep 10
done >> bench/results/rss-$(git rev-parse --short HEAD).tsv
```

### 4.5 Soak

**12 hours at 70 % of max sustainable rate**, `CHECKPOINT_INTERVAL=5s`, `CHECKPOINT_RETENTION=3`.
Covers ≥ 4300 checkpoints and multiple retention-pruning cycles, and fits an overnight.

| Failure condition | Measurement | Threshold |
|---|---|---|
| Job memory leak | slope of `memoryUsedBytes`, hours 2–12 (discard hour 1) | > 2 MB/h sustained, or > 15 % total growth. Must plateau, not trend |
| Controller leak | `deriv(process_resident_memory_bytes{job="weibo-controller"}[6h])`, `deriv(go_goroutines{...}[6h])` | RSS > 1 MB/h; **`go_goroutines` slope > 0 at all** — any trend is a leak, and D6 predicts one |
| Throughput decay | `avg_over_time(rate(weibo_records_written_total[5m])[1h:])` at h12 vs h2 | > 5 % decay |
| Lag divergence | in-process: read-rate minus write-rate must oscillate around 0. Kafka: `.kafkaLag` from `/history` | **any monotonic increase over a 1 h window**. A bounded sawtooth (grows during checkpoint, drains after) passes |
| Checkpoint drift | `checkpointDurationMs` p99, h12 vs h2 | > 2× growth, or any value > `CHECKPOINT_INTERVAL` |
| State dir growth | hourly `du -sm` on the job volume | must plateau — unbounded growth means retention is not pruning |
| Stability | `weibo_pipeline_running`, `changes(weibo_controller_launches_total[12h])` | `== 1` for the full 12 h; **zero** restarts |
| Errors | `increase(weibo_records_failed_total[12h])` | exactly `0` |

### 4.6 Baseline artifact

Two files under a new `bench/results/`. Greppable, diffable, no tooling needed to read them.

**`bench/results/BASELINES.tsv`** — append-only, one row per (SHA, scenario, rate step), `#` header:

```
# sha	date	host	scenario	partitions	keys	offered_rps	achieved_rps	written_rps	op_p99_ms	sink_p99_ms	edge_fill_max	block_frac_max	ckpt_p99_ms	rss_mb_p95	cpu_pct_p95	failed	verdict
a1b2c3d	2026-09-21	mac-m-10c-8g	loadbench-blackhole	4	1000	5000	5000	4998	2.3	0.8	0.11	0.02	180	210	38	0	pass
a1b2c3d	2026-09-21	mac-m-10c-8g	loadbench-blackhole	4	1000	40000	39900	39880	14.1	2.4	0.78	0.41	820	515	291	0	pass
a1b2c3d	2026-09-21	mac-m-10c-8g	loadbench-blackhole	4	1000	80000	61200	61150	95.0	9.7	1.00	0.93	5400	690	870	0	fail-backpressure
```

**`bench/results/README.md`** — reproduce recipe (exact compose file, submit command with every env
var, the ramp table, the six stop conditions, the PromQL verbatim), plus the first **SLO** the repo
has ever had, derived from the measured baseline:

> SLO v0 (single job, 4 partitions, 1000 keys, blackhole sink, 10 CPU / 8 GB host): sustain
> ≥ 40 000 rec/s with sink p99 ≤ 5 ms, operator p99 ≤ 20 ms, edge fill ratio ≤ 0.8, checkpoint p99
> ≤ 1 s, zero failed records.

**Regression rule — advisory first.** A PR regresses if `achieved_rps` at the reference step drops
> 10 % or any p99 grows > 25 % vs the newest `pass` row for that scenario. Enforce **ratios against a
same-run reference step**, never absolutes — a 10-CPU laptop and a CI runner are not comparable. This
is also where the five orphaned micro-benchmarks should get committed baselines, same file, with
`scenario` set to the benchmark name, so there is exactly one place to look.

### 4.7 Grafana gaps

`observability/docker/weibo.json` has 8 panels covering **throughput and latency only**. Missing the
entire backpressure, checkpoint, resource and control-plane story:

1. **Edge fill ratio** — `max by (edge) (weibo_edge_queue_size / weibo_edge_queue_capacity)`, 0–1 axis,
   thresholds 0.8/0.9. The single most important missing panel; without it "latency went up" has no
   explanation.
2. **Block fraction by stage** — `rate(weibo_stage_send_block_seconds_total[1m])`. The
   bottleneck-identifier; the whole `weibo_stage_*` family is absent from the dashboard today.
3. **Stage in/out rates** by `stage`.
4. **Per-worker skew** + `weibo_stage_workers` — shows whether `WithPartitions(4)` is balanced.
5. **Checkpoint duration + size** — blocked on 4.1.
6. **Container RSS/CPU** — either export the `ContainerStats` the controller already collects (~20
   lines next to `inventoryCollector` in `control/metrics.go`) or keep RSS in the TSV only.
7. **Target health** — `up{job="weibo-jobs"}` and `count(up{job="weibo-jobs"} == 0)`. **This is the
   panel that catches D2.**
8. **A controller row** — `weibo_controller_runs` by phase, `rate(weibo_controller_launches_total[5m])`
   by `result` (the `record_failed` and `blocked` series are the D3/D4 signals), reconcile p99,
   `go_goroutines` (the D6 leak signal).
9. **Global fix:** every existing panel uses `rate(...[15s])`, shorter than 4× the 5 s scrape interval
   and therefore gappy — move to `[1m]`. Add a `weibo_job_id` template variable, or once http_sd is
   wired all jobs overlay into one unreadable chart.

Split: keep `weibo.json` as the single-job engine view (1–5, 9); add `weibo-control.json` for item 8
so the control-plane view survives a job going away.

---

## Phase 5 — Real cloud deploy (AWS free tier)

Execute `docs/self-hosting.md` verbatim on a real VM. Validates what a local stack structurally
cannot: registry pull by a remote host, systemd supervision, IAM, real TLS.

**Honest constraint:** free-tier `t3.micro` is 1 GB RAM. Kafka will not fit alongside the controller
and jobs. So the cloud phase runs the **generator-based** job and points checkpoints at **real S3**
(5 GB free tier) instead of MinIO. Kafka validation stays local. Check whether the `t4g.small` ARM
promo still applies — 2 GB would be far more comfortable.

1. Push the runner image to ECR in `ap-south-1` (create `weibo-runner`; only `weibo-sdk-demo` exists).
2. Launch with an **IAM instance role** carrying `AmazonEC2ContainerRegistryReadOnly` plus write
   access to one S3 savepoint bucket. Local uses an IAM *user* with `credsStore: desktop`; on EC2 the
   `amazon-ecr-credential-helper` + `credHelpers` block from `self-hosting.md:184-190` is required
   instead. **This is the step most likely to fail first.**
3. Install Docker and the ECR helper; verify `docker pull` **as the service user**.
4. Install the `weibo` binary — `control-release.yml` already cross-builds linux/arm64; reuse it
   rather than building on a 1 GB box.
5. systemd unit + `/etc/weibo/weibo.env` (chmod 600) exactly as documented;
   `useradd -r -G docker weibo`.
6. Caddy in front for real TLS; controller stays on `127.0.0.1:9000`.
7. Deploy from this Mac: `WEIBO_CONTROLLER=https://<host>` + `WEIBO_TOKEN`.
8. Re-run the cheap drills remotely — D1, D4, D13 — these behave differently under systemd.

**Teardown is part of the plan:** terminate the instance, delete the bucket and ECR images. Free tier
is 750 h/month for 12 months; an instance left running past that bills.

## Risks

- **Kafka reachability from job containers** — settle before writing the compose file (Phase 1).
- **8 GB RAM ceiling** — Kafka + Postgres + MinIO + Prometheus + Grafana + controller + jobs is close
  to the limit. Cap heaps, `mem_limit` everything, run the load phase with no extras.
- **`self-hosting.md` has never been executed** — treat every deviation as a finding and fix the doc.
- **Drills will find real bugs.** D2, D3 and D6 are near-certain. Decide up front: this plan's job is
  to *record* them, not fix them inline, or Phase 3 never finishes.

## Follow-ups found during exploration (separate pass)

- `plans/roadmap.md` is stale — items #5/#6/#7/#9 are done in code but unticked; line 474 still reads
  "Next task: #29" though #29 shipped in `be70785`. Reconcile the whole file against the code.
- `plans/dashboard-revamp-roadmap.md` uses D7/D8/D9 twice each; a Sinks exit-criteria block is
  misfiled under the first D9.
- Stale text: `docs/dashboard.md:26` and `plans/dashboard-revamp-roadmap.md:402-403,666-674` still say
  the UI is "reduced to a single Sources section".
- Theme drift: the roadmap mandates a dark UI; shipped UI is light content + dark sidebar.
- D9's "browser-tier tests" deliverable was substituted with httptest HTML assertions plus manual
  Playwright screenshots.
