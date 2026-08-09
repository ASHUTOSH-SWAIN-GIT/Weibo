# Weibo Dashboard

The Weibo control-plane web UI. A single self-contained HTML page (inline CSS/JS, no external assets) embedded in the controller binary via `//go:embed` — no build step or static file deployment needed. This doc tracks what's been implemented so far and is updated as the dashboard evolves.

## Run

```sh
cd control && go run ./cmd/weibo dashboard        # starts controller + opens UI at :9000
go run ./cmd/weibo dashboard -no-open             # headless
```

Dashboard: http://localhost:9000 — API auth via `-auth-token` (env `WEIBO_AUTH_TOKEN`); empty = open.

## Code layout

| Path | Role |
| ---- | ---- |
| `control/ui/ui.go`       | `//go:embed index.html logo.png`; serves the SPA at `/` and the logo |
| `control/ui/index.html`  | The whole dashboard (SPA, inline CSS/JS) |
| `control/api/api.go`     | REST server; dashboard talks to `/jobs`, `/jobs/{id}/...`, `/validate` |

The dashboard is a plain-JS SPA with a hash router (`#/overview`, `#/job-manager`, `#/running`, `#/completed`, `#/submit`, `#/job/{id}`). It polls the API on an interval (Overview/Infrastructure/Active/History: 3s, job detail: 2s). All rendering is self-contained in the embedded page.

## Implemented so far

### Layout & navigation
- **Sidebar** — compact dark navigation with Overview, Infrastructure, Active, History, and Deploy. Connection status appears in the footer.
- **Token auth** — on a 401 the UI drops to a token prompt, validates it via `POST /auth`, and stores it in `localStorage`.

### Overview
- Stat tiles: Available Task Slots (jobs × 2), Running, Finished, Failed.
- Running Jobs + Completed Jobs tables (name/id, start/end time, status phase dot). Rows navigate to the job detail page.

### Jobs lists
- **Running Jobs** — filter to `phase=running`, count subheading.
- **Completed Jobs** — terminal phases only, Finished/Failed stat tiles.

### Infrastructure
- **Host machine** — hostname, operating system, architecture, CPU, memory, load average, Docker version, and container counts.
- **Container inventory** — every Docker container with image reference/ID, state, live CPU and memory, network, disk I/O, process count, and start time.

### Job detail
- **Metadata strip** — Job Name, Job ID, Status, Type, Kind, Delivery, Created/Updated, Attempt, Started/Stopped, Control Port.
- **Actions** — Savepoint (prompt for label), Restart (prompt for savepoint label, blank = last checkpoint), Cancel.
- **Pipeline graph (DAG)** — source → operators → sink nodes, color-coded by kind (source blue, sink green, keyBy/reduce accent, window amber), parallelism badge (`×N`) when present. Rendered as inline SVG.
- **Tabs**:
  - **Overview** — pipeline DAG, Live State card (phase, uptime, records in/out) via `/jobs/{id}/state`, Lifecycle transition log.
  - **Metrics** — stat tiles (Records read/written, Processed, Failed, Source/Sink errors) + stages table (records in/out per stage) + operators table (records processed per operator), parsed from the Prometheus exposition format served by `/jobs/{id}/metrics`.
  - **Checkpoints** — count, last checkpoint time, current ID, checkpoint history table (from `/state`).
  - **Logs** — container logs (`/jobs/{id}/logs?tail=200`), auto-scrolled.
  - **Spec** — raw job spec YAML.

### Deploy
- Accepts an SDK image manifest with name, image reference, and optional resource limits.
- Submits via `POST /jobs`, then navigates to the new job's detail page.

## How to update this doc

Append new items under "Implemented so far" whenever a change lands. Keep it brief — one line per feature.
