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
| `POST /jobs`                  | Submit a workflow. Body: raw YAML, or JSON `{"workflow": "...", "env": {...}}` to pass secrets. Validated (dry-run compile) before launch. |
| `GET  /jobs`                  | List jobs. |
| `GET  /jobs/{id}`             | Job detail: job + latest run + transition log. |
| `POST /jobs/{id}/cancel`      | Graceful stop; desired state → stopped. |
| `POST /jobs/{id}/restart`     | Stop any live run and launch a fresh one. Body `{"savepoint":"<label>"}` resumes from a savepoint. |
| `POST /jobs/{id}/savepoint`   | Stop-with-savepoint. Label via `?label=` or body `{"label":"..."}`. |
| `GET  /jobs/{id}/logs?tail=N` | Container logs (`tail=0` for all). |
| `GET  /jobs/{id}/state`       | Proxy to the job's live agent `/state`. |
| `GET  /jobs/{id}/metrics`     | Proxy to the job's live agent `/metrics`. |
| `POST /auth`                  | Returns 200 iff the bearer token is valid (UI token check). |

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
  placeholders intact; resolved values are passed to the container and held in
  process memory only. A job that needs secrets cannot be *relaunched* after a
  controller restart without re-supplying them (real secret management is a
  later concern); already-running containers are unaffected.
- **Reconciler** enforces desired state, applies the restart policy to crashed
  containers (bounded attempts + backoff), and marks clean exits Finished.
- Submit-time validation compiles the workflow in a throwaway data dir, so a
  Postgres sink is checked by opening its pool — an unreachable database fails
  the submit.
