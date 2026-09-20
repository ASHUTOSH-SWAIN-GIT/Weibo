# Dashboard data contract

This is the dashboard revamp source-of-truth inventory. The UI should render
from these endpoints through normalization helpers in `control/ui/index.html`;
when a field or endpoint is missing, it must show a clear "not reported" or
"unavailable" state instead of guessing.

## Endpoint inventory

| Endpoint | Source of truth | Important fields | Dashboard use | Missing-data behavior |
| --- | --- | --- | --- | --- |
| `GET /jobs` | controller store + latest run projection | `jobs[].id`, `name`, `kind`, `delivery`, `desiredState`, `createdAt`, `updatedAt`, `phase` | fleet overview, Active/History lists, status counts | empty list renders the "No jobs" state |
| `GET /jobs/{id}` | controller store + latest run + transition audit | `job`, `latestRun`, `transitions` | job metadata strip, current attempt, lifecycle preview, spec pane | 404 renders "Job not found"; absent latest run renders unknown/not-started fields |
| `GET /jobs/{id}/state` | live job agent `/state` proxy | `phase`, `startedAt`, `uptime`, `recordsIn`, `recordsOut`, `ready`, `source`, `currentCheckpointId`, `lastCheckpointAt`, `checkpoints[]`, `lastError` | live state card, source positions, checkpoint tab, checkpointed source positions | non-200/null is expected for terminal or unreachable jobs; UI keeps page functional and labels state unavailable |
| `GET /jobs/{id}/describe` | live SDK job agent topology | `source.type`, `source.props`, `sink.type`, `sink.props`, `operators[]`, `checkpoint` | Sources/Sinks/Operators identity, redacted connector config, checkpoint config, delivery derivation | unavailable while job is stopped or no agent exists; UI renders pending describe / metadata unavailable |
| `GET /jobs/{id}/plan` | live SDK execution plan | `stages[]`, `edges[]`, stage names/types/operators/parallelism | runtime stage table and backpressure joins | unavailable while job is stopped or no agent exists; UI renders no runtime plan |
| `GET /jobs/{id}/diagnostics` | controller diagnostics assembly | `jobId`, `phase`, `desired`, `failure`, `activity`, `restart`, `checkpoint`, `runId`, `attempt` | grouped diagnostics on Overview | unavailable only for unknown jobs; missing sub-objects render "none" / "not reported" |
| `GET /jobs/{id}/runs` | controller store run history | `runs[]` with `id`, `jobId`, `containerId`, `hostPort`, `phase`, `attempt`, `error`, `failureKind`, `startedAt`, `stoppedAt`, `restartAt` | Runs tab attempt table and log selector | empty list renders "No runs recorded" |
| `GET /jobs/{id}/runs/{runId}` | controller store + transition audit | `run`, `transitions`, `restart` | selected attempt detail in Runs tab | 404 renders empty/unavailable selected attempt state |
| `GET /jobs/{id}/runs/{runId}/logs?tail=N` | backend container log store for a historical run | plain text | Attempt logs and previous-run Logs tab | 404/410 renders no output/container removed wording |
| `GET /jobs/{id}/logs?tail=N` | latest backend container logs | plain text | live/latest Logs tab | unavailable logs render "No output" |
| `GET /jobs/{id}/logs/stream?tail=N` | latest backend container logs via SSE | `data:` events | live follow/pause | disabled for terminal jobs and historical attempts |
| `GET /jobs/{id}/history?points=N` | in-memory controller history ring | `jobId`, `points[]`, `samples` | job detail sparklines and trend tiles | empty points render no trend; rates are never guessed |
| `GET /jobs/history?points=N` | in-memory controller history ring | `histories` map of job ID to points | fleet throughput sparklines | missing job history is treated as no samples |
| `GET /jobs/{id}/metrics` | live job agent Prometheus text | counters/gauges such as `weibo_records_*`, `weibo_stage_*`, `weibo_edge_*`, source/sink errors | source/sink/operator/stage metrics and freshness badge | parse failure or non-200 renders "metrics not reported" |
| `GET /config` | controller UI config | `grafanaUrl` | optional Grafana deep link | empty URL hides the link |
| `POST /auth` | API auth middleware | `status`, `role` (`open`, `readonly`, `readwrite`) | token prompt validation and read-only UI disabling | failed auth drops back to token prompt |

## Field notes

### Store-backed fields

Store-backed data is durable across controller restarts and is safe for
terminal jobs:

- `store.Job`: `id`, `name`, `kind`, `spec`, `image`, `secrets`, `delivery`,
  `graph`, `desiredState`, `createdAt`, `updatedAt`.
- `store.Run`: `id`, `jobId`, `containerId`, `hostPort`, `phase`, `attempt`,
  `error`, `failureKind`, `startedAt`, `stoppedAt`, `restartAt`.
- `store.Transition`: `id`, `jobId`, `runId`, `from`, `to`, `reason`, `at`.

Secrets are references only. Resolved secret values must never be returned by
dashboard APIs.

### Live-agent fields

Live-agent data is best-effort. It exists only while a job exposes a reachable
control surface:

- `/state` carries live phase, record totals, readiness, connector source
  position, recent checkpoints, and terminal `lastError`.
- `/describe` carries connector/operator/checkpoint metadata used for labels
  and redacted configuration display.
- `/plan` carries runtime stages and edges used for the stage/backpressure
  table.
- `/metrics` is Prometheus text. The browser parser must tolerate missing
  metrics and label drift.

### In-memory controller history

History endpoints are process memory only. They are useful for trend and
diagnostic context but not a durable audit log. Rates are derived from
counter deltas between samples; the UI should not invent rates from a single
sample.

## Normalization rules

- Source identity comes from `/describe`; source position comes from `/state`;
  source counters/errors come from `/metrics`.
- Sink identity comes from `/describe`; sink counters/errors come from
  `/metrics`.
- Logical operators come from `/describe`; runtime stages and edges come from
  `/plan`; stage/edge health comes from `/metrics`.
- Checkpoint config comes from `/describe`; checkpoint progress/history comes
  from `/state`; checkpoint duration/size can also appear in diagnostics and
  history.
- Delivery is derived from checkpointing plus coordinated sink capability. The
  UI must not display exactly-once solely because a raw spec string says so.
- Attempt history comes from `/runs` and `/runs/{runId}`, not from the current
  latest run alone.
- Mutation affordances are disabled for `POST /auth` role `readonly`, while the
  backend remains the final enforcement layer.

## Regression coverage

The D9 dashboard accuracy tests in `control/api/dashboard_accuracy_test.go`
cover the key contracts here:

- app shell and all job-detail tabs;
- normalization helper hooks and freshness labels;
- `/jobs`, `/jobs/{id}`, runs, transitions, diagnostics, history, and config
  envelopes;
- graceful degradation for missing live-agent endpoints;
- Kafka/file/source/sink/operator/checkpoint text hooks;
- read-only auth UI and backend 403 enforcement;
- absence of forbidden secret values from dashboard API surfaces.
