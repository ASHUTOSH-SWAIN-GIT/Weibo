# weibo control plane

The Weibo job control plane (job-orchestration plan, phase P3): submit a
workflow, and the controller launches one runner container for it, tracks its
lifecycle, and keeps it converging on your desired state — the JobManager
equivalent for the single-container-per-job model.

This is a **separate Go module** (`control/`) so the core engine stays
dependency-light: `go get github.com/ASHUTOSH-SWAIN-GIT/weibo` never pulls the
Docker/SQLite clients that live here.

## Layout

| Package                    | Role |
| -------------------------- | ---- |
| `store`                    | SQLite persistence (jobs, runs, transitions). Source of truth; never stores secrets. |
| `lifecycle`                | Phases, legal transitions, restart policy. |
| `backend`                  | `ContainerBackend` interface + Docker + Kubernetes impls + in-memory fake. |
| (root) `control`           | `Controller`: submit/cancel/restart + the reconciler loop. |
| `api`                      | REST server over the controller. |
| `cmd/weibo`               | The `weibo` CLI (`weibo dashboard`). |

## Run

Build the runner image once (from the repo root — see
`cmd/weibo-runner/README.md`), then launch the dashboard:

```sh
docker build -f Dockerfile.runner -t weibo-runner:dev .   # repo root
cd control && go run ./cmd/weibo dashboard                # starts controller + opens the UI
```

`weibo dashboard` boots the controller and opens the web UI in your browser.
Add `-no-open` to run it headless (e.g. on a server), or `-addr :9000` to
change the port. Everything — submit, watch, cancel, restart, savepoint —
happens in that one UI.

## CLI

`weibo dashboard` is the server; the rest are thin REST clients that talk to a
running controller (local or remote) over `WEIBO_CONTROLLER` (default
`http://localhost:9000`) and `WEIBO_TOKEN`:

```sh
weibo deploy -file weibo.yaml   # build (SDK) + push + submit a job
weibo jobs                       # list jobs
weibo status <id>                # detail + latest run + history
weibo logs <id> [-tail N]        # container logs
weibo cancel <id>                # graceful stop
weibo restart <id> [-savepoint L]
weibo savepoint <id> -label L    # stop-with-savepoint
```

### `weibo deploy`

For an **SDK job** (`kind: sdk` manifest), `deploy` is one command from source to
a running job: it **builds** the image, **pushes** it to a registry, and
**submits** the manifest — all as a clean abstraction (the raw `docker
build`/`push` output is hidden behind progress lines; the underlying output is
surfaced only on failure). A plain YAML workflow has no image, so `deploy` just
submits it.

```sh
# -registry (env WEIBO_REGISTRY) prefixes a bare manifest image and pushes there:
#   manifest `image: orders:1.0`  +  -registry docker.io/you
#   -> builds & pushes docker.io/you/orders:1.0, and the submitted manifest
#      is rewritten to that same ref so the controller runs exactly what shipped.
weibo deploy -registry docker.io/you -file weibo.yaml
```

Flags: `-file` (manifest, default `weibo.yaml`), `-registry` (env
`WEIBO_REGISTRY`), `-dockerfile` (default `Dockerfile`), `-context` (default
`.`), `-env KEY=VAL` (repeatable), `-no-build`, `-no-push`. An image that is
already fully qualified (contains a `/`) is pushed verbatim; a bare name is
prefixed with `-registry`.

> **Build arch:** `deploy` builds for the host's architecture. Building on an
> arm64 machine (e.g. Apple Silicon) produces an arm64 image that won't run on
> an amd64 controller host — deploy against a matching-arch controller, or build
> on the target arch.

For a production single-VM deployment (registry auth, bearer-token auth, TLS,
resource limits, systemd), see **[docs/self-hosting.md](../docs/self-hosting.md)**.

## Backends: Docker or Kubernetes

The controller drives one job per container through a `ContainerBackend`. The
same jobs, API, and UI work on either backend — only *where* the containers run
changes.

**Docker** (default) — one container per job on the local daemon:

```sh
weibo dashboard                         # -backend docker is the default
```

**Kubernetes** — one `batch/v1` Job per job on a cluster (a `Job`, not a
Deployment, so a completed job isn't auto-restarted — weibo's reconciler owns
restarts). Each job gets a per-job PVC (state + checkpoints), a ConfigMap (the
workflow), an optional Secret (env), and a ClusterIP Service, with `/healthz`
liveness/readiness probes and `fsGroup` so the non-root runner can write the
volume.

```sh
# The image must be pullable by the cluster — push it, or for kind:
kind load docker-image weibo-runner:dev
weibo dashboard -backend kubernetes -namespace default -image weibo-runner:dev
```

Notes for the Kubernetes backend:

- **Image:** `weibo-runner:dev` is local; a real cluster needs it in a registry
  (`-image <registry>/weibo-runner:tag`), or loaded into kind.
- **Live state proxy:** the dashboard reaches a job's `/state` and `/metrics`
  via the ClusterIP Service DNS, so those work when the controller runs
  **in-cluster**. Run the controller on the host (against a remote cluster) and
  job *lifecycle* (submit/status/logs/cancel/restart) still works via the API,
  but the live-metrics proxy needs in-cluster networking (or a port-forward).
- **Savepoints** live under the per-job PVC (`/data/savepoints`), so same-job
  restart-from-savepoint works. Cross-host / cross-job savepoints need an object
  store (an S3 `Blobstore` adapter) — a planned follow-up.

## API

| Method + path                 | Purpose |
| ----------------------------- | ------- |
| `POST /jobs`                  | Submit a workflow. Body: raw YAML, or JSON `{"workflow": "...", "env": {...}, "envRefs": {...}}` to pass launch env and durable secret refs. Validated (dry-run compile) before launch. |
| `GET  /jobs`                  | List jobs. |
| `GET  /jobs/{id}`             | Job detail: job + latest run + transition log. |
| `DELETE /jobs/{id}`           | Stop/remove known backend run resources, then delete job/run/history rows. Durable state volumes/savepoints are preserved unless `?deleteData=true` (wipes the Docker volume / K8s PVC irreversibly). |
| `POST /jobs/{id}/cancel`      | Graceful stop; desired state → stopped. |
| `POST /jobs/{id}/restart`     | Stop any live run and launch a fresh one. Body `{"savepoint":"<label>"}` resumes from a savepoint. |
| `POST /jobs/{id}/savepoint`   | Stop-with-savepoint. Label via `?label=` or body `{"label":"..."}`. |
| `GET  /jobs/{id}/logs?tail=N` | Container logs (`tail=0` for all). |
| `GET  /jobs/{id}/state`       | Proxy to the job's live agent `/state`. |
| `GET  /jobs/{id}/metrics`     | Proxy to the job's live agent `/metrics`. |
| `POST /auth`                  | Returns 200 iff the bearer token is valid (UI token check). |
| `GET  /livez`                 | Process liveness. |
| `GET  /readyz`                | Store/backend readiness; returns 503 when dependencies are unavailable. |
| `GET  /metrics`               | Controller Prometheus metrics (public; aggregate counters only). |
| `GET  /targets`               | Prometheus http_sd discovery for live job agents (auth-gated). |
| `GET  /jobs/{id}/history?points=N` | Rolling history samples for one job (default 120), oldest first. |
| `GET  /jobs/history?points=N` | Downsampled series for every job with history (default 30) — one request for the all-jobs view. |
| `GET  /config`                | UI configuration: `{"grafanaUrl": "..."}` ("" when deep links are disabled). |
| `GET  /jobs/{id}/diagnostics` | Assembled health view: failure classification + hint, last activity, restart countdown, latest checkpoint duration/size. |
| `GET  /jobs/{id}/runs`        | Every recorded attempt, newest first (retention-bounded). |
| `GET  /jobs/{id}/runs/{runId}` | One attempt with its audit transitions and restart countdown. |
| `GET  /jobs/{id}/runs/{runId}/logs?tail=N` | That attempt's container logs (404 unknown run, 410 container removed). |
| `GET  /jobs/{id}/transitions?limit=N&before=<id>` | Paged audit log, newest first (default 50, max 200; `nextBefore` cursor, 0 when exhausted). |
| `GET  /jobs/{id}/logs/stream?tail=N` | Follow the latest container's logs over server-sent events (initial burst, then new output every 2s). |

**Auth:** start the controller with `-auth-token <secret>` (env
`WEIBO_AUTH_TOKEN`) to require `Authorization: Bearer <secret>` on every route
except `GET /` and `GET /healthz`. The CLI sends it via `-token` /
`WEIBO_TOKEN`; the UI prompts and stores it. No token = open API (the default).

### Example

```sh
curl -X POST --data-binary @examples/workflows/order-totals.yaml \
  -H 'content-type: application/yaml' localhost:9000/jobs
curl localhost:9000/jobs
curl localhost:9000/jobs/<id>/state
curl -X POST localhost:9000/jobs/<id>/cancel
```

## Metrics and discovery

- `GET /metrics` (no auth, like `/healthz`) — controller-native Prometheus
  metrics: process/Go runtime, `weibo_controller_reconciles_total` +
  `weibo_controller_reconcile_duration_seconds`, `weibo_controller_launches_total{result}`
  (`success|transient|permanent|blocked|record_failed`),
  `weibo_controller_jobs{desired}` and `weibo_controller_runs{phase}`
  inventory gauges (read live from the store on each scrape), sweep
  counters, and `weibo_controller_api_requests_total{method,route,status}`.
- `GET /targets` (same auth as the API) — Prometheus `http_sd` discovery:
  one target per job with a reachable live agent, labeled `weibo_job_id`,
  `weibo_job_name`, `weibo_run_phase`. Stopped jobs disappear; unreachable
  ones are skipped.

Cardinality is bounded by design: metric labels carry only small
enumerations (route templates, never raw paths or IDs — `/jobs/{id}`, not
`/jobs/abc123`). Per-job addressing lives in `/targets`, and discovery
resolves at most 8 backend probes concurrently under a 15s deadline.
`control/kubernetes-servicemonitor.yaml` is a ready ServiceMonitor plus a
commented `weibo-jobs` scrape job using `/targets`.

## Rolling history and Grafana links

The controller samples every live job's agent (`/state` plus queue/error
counters from `/metrics`) every `--history-interval` (default 15s) into a
bounded in-memory ring (`DefaultHistorySamples` = 240 per job, about an
hour). It is process memory only — deliberately never SQLite — so a
controller restart starts history fresh, and job deletion drops its series.
The dashboard renders throughput sparklines from it (fleet total on the
Overview, per-job trend column, and a larger chart with lag/queue/error
tiles on each job's detail page); readers derive records/sec from counter
deltas, so a missed tick widens one interval instead of corrupting the
series.

With `--grafana-url https://grafana…` (env `WEIBO_GRAFANA_URL`), each job
page gains a Grafana button linking to
`{url}/d/weibo-job?var-job=<id>&var-run=<run>&from=now-1h&to=now` —
provision a dashboard with UID `weibo-job` and `job`/`run` variables to
receive it.

## Diagnostics

When a job misbehaves, `GET /jobs/{id}/diagnostics` assembles the answer
in one call: the failure category (`launch_transient/permanent`,
`launch_record`, `secret_blocked`, `restarting`, `run_failed`) with an
operator hint, the last reported activity (when the agent last checked in
plus record counters), a live restart countdown when a retry is scheduled,
and the latest checkpoint's duration and inline size (exact for in-memory
state, a lower bound with native Pebble state — see the engine's
`CheckpointReport`). The dashboard renders this as a Diagnostics card, a
Runs tab for inspecting previous attempts (audit + logs each), a Follow
button that streams logs over server-sent events, and an Older button that
pages the audit log. The CLI mirrors it: `weibo runs <job-id>` and
`weibo logs <job-id> -follow`.

## Savepoints (stop-with-savepoint)

A savepoint is a named, durable snapshot you can restart from — for upgrades
or redeploys:

```sh
curl -X POST 'localhost:9000/jobs/<id>/savepoint?label=before-upgrade'
# ...deploy new code...
curl -X POST localhost:9000/jobs/<id>/restart \
  -H 'content-type: application/json' -d '{"savepoint":"before-upgrade"}'
```

The job drains, writes a final checkpoint, and the runner promotes it to a
blob under `savepoints/<label>` in a shared volume visible to every job (the
same namespace an S3 bucket gives across hosts — an S3 blobstore drops in for
P6 without touching the savepoint code). The workflow must have checkpointing
enabled (`env.checkpointing`).

## Launch failures and retries

Submit validates the workflow before any backend resource is created. If the
spec or SDK manifest is invalid, `POST /jobs` returns `400` and no job is
recorded.

If validation succeeds but the backend cannot start the first container, the
job is recorded and the response is `202 Accepted` with a warning. The latest
run explains what happens next:

- `phase: "restarting"` with `failureKind: "launch_transient"` and `restartAt`
  means the controller persisted a retry time and the reconciler will launch a
  fresh run after backoff.
- `phase: "failed"` with `failureKind: "launch_permanent"` means the failure is
  not expected to recover automatically, such as an invalid or inaccessible
  image reference.
- `phase: "blocked"` with `failureKind: "secret_blocked"` means durable secret
  references could not be resolved; no container is launched until they resolve.

The controller persists this state, so restart/backoff decisions survive a
controller process restart.

## Exactly-once across restarts

Two safeguards keep exactly-once intact when a job restarts:

- **Single-live-run fencing** — the controller refuses to launch a second
  container while one is live, so two transactional producers with the same id
  never coexist.
- **Stable transactional id** — the job's spec (with its `transactionalID`) is
  stored and reused verbatim on every restart. Pin it to the job by referencing
  the injected `WEIBO_JOB_ID` (`transactionalID: ${WEIBO_JOB_ID}`).

## Design notes

- **Store is the source of truth.** On restart the controller's reconciler
  re-reads active runs and re-attaches to their containers — a controller crash
  never loses track of a running job.
- **Secrets are never persisted.** The workflow doc is stored with its `${VAR}`
  placeholders intact. Resolved values are passed to the container and held in
  process memory only; durable recovery stores only references such as
  `{"provider":"env","name":"API_KEY"}`. If a reference cannot resolve during a
  restart/relaunch, the run enters `blocked` instead of launching with missing
  env, and the reconciler retries once the reference becomes resolvable.
- **Reconciler** enforces desired state, applies the restart policy to crashed
  containers (bounded attempts + backoff), and marks clean exits Finished.
- **Deletion cleans run resources first.** `DELETE /jobs/{id}` stops/removes
  every known backend run resource before deleting store history. Docker
  containers and Kubernetes per-run Jobs/Services/ConfigMaps/Secrets are
  removed; durable volumes/PVCs/savepoints are kept unless
  `?deleteData=true`. Restarts remove the previous attempt's backend
  resource, terminal-run history is retained bounded (newest 5 by default),
  and a startup orphan sweep removes exited backend resources the store no
  longer references (running unknowns are reported, never removed).
- **Health checks are dependency-aware.** `/livez` reports the controller
  process is alive. `/readyz` verifies the store and selected backend are
  reachable without exposing credentials or internal addresses.
- Submit-time validation compiles the workflow in a throwaway data dir without
  side effects: no pools are opened and no connections are made, so an
  unreachable database does not fail the submit (connectivity is a runtime
  concern, surfaced in run state).
