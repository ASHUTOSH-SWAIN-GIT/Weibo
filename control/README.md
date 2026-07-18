# mailer control plane

The Mailer job control plane (job-orchestration plan, phase P3): submit a
workflow, and the controller launches one runner container for it, tracks its
lifecycle, and keeps it converging on your desired state — the JobManager
equivalent for the single-container-per-job model.

This is a **separate Go module** (`control/`) so the core engine stays
dependency-light: `go get github.com/ASHUTOSH-SWAIN-GIT/mailer` never pulls the
Docker/SQLite clients that live here.

## Layout

| Package                    | Role |
| -------------------------- | ---- |
| `store`                    | SQLite persistence (jobs, runs, transitions). Source of truth; never stores secrets. |
| `lifecycle`                | Phases, legal transitions, restart policy. |
| `backend`                  | `ContainerBackend` interface + Docker impl + in-memory fake. |
| (root) `control`           | `Controller`: submit/cancel/restart + the reconciler loop. |
| `api`                      | REST server over the controller. |
| `cmd/mailer-controller`    | The controller binary. |

## Run

Build the runner image first (from the repo root — see
`cmd/mailer-runner/README.md`), then start the controller:

```sh
docker build -f Dockerfile.runner -t mailer-runner:dev .   # repo root
cd control && go run ./cmd/mailer-controller -addr :9000 -image mailer-runner:dev
```

## API

| Method + path                 | Purpose |
| ----------------------------- | ------- |
| `POST /jobs`                  | Submit a workflow. Body: raw YAML, or JSON `{"workflow": "...", "env": {...}}` to pass secrets. Validated (dry-run compile) before launch. |
| `GET  /jobs`                  | List jobs. |
| `GET  /jobs/{id}`             | Job detail: job + latest run + transition log. |
| `POST /jobs/{id}/cancel`      | Graceful stop; desired state → stopped. |
| `POST /jobs/{id}/restart`     | Stop any live run and launch a fresh one. |
| `GET  /jobs/{id}/logs?tail=N` | Container logs. |
| `GET  /jobs/{id}/state`       | Proxy to the job's live agent `/state`. |
| `GET  /jobs/{id}/metrics`     | Proxy to the job's live agent `/metrics`. |

### Example

```sh
curl -X POST --data-binary @examples/workflows/order-totals.yaml \
  -H 'content-type: application/yaml' localhost:9000/jobs
curl localhost:9000/jobs
curl localhost:9000/jobs/<id>/state
curl -X POST localhost:9000/jobs/<id>/cancel
```

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
