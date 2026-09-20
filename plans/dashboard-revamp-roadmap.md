# Weibo Dashboard Revamp Roadmap

Goal: rebuild the controller web UI into a minimal, accurate operating console
for Weibo jobs. The dashboard should answer four questions quickly:

1. Is the job healthy?
2. Where is data coming from?
3. What operators are running and where is time/backpressure/state accumulating?
4. Where is data going, and what delivery guarantee applies?

Design constraint: keep the current deployment shape — one embedded
`control/ui/index.html` SPA with inline CSS/JS and no frontend build step —
unless a later phase explicitly justifies splitting files.

---

## Product principles

- **Accuracy over decoration.** Every number must cite one source of truth:
  controller store, job `/state`, job `/metrics`, `/describe`, `/plan`, logs, or
  diagnostics. If data is missing, show "not reported" instead of guessing.
- **Minimal visual language.** Neutral dark UI, restrained color, compact cards,
  dense tables, no novelty charts unless they clarify state.
- **Sections mirror the operator's question.** Overview, Sources, Pipeline,
  Sinks, and Reliability — not every internal subsystem as its own page.
- **Progressive detail.** The first screen shows health and bottleneck hints; the
  detail page expands exact source/sink/operator data.
- **Stable for operators.** Avoid noisy reflow while polling. Preserve selected
  tab, selected run, scroll position, and log-follow state.
- **No secret leakage.** Redact DSNs, SASL credentials, bearer tokens, env values,
  registry credentials, and filesystem paths only when they are explicitly marked
  sensitive. Show safe identifiers such as topic, table, bucket, file basename,
  operator label, checkpoint ID, and container image.

---

## Current state

The dashboard currently provides:

- Embedded plain-JS SPA in `control/ui/index.html`.
- A deliberately reduced visible route: Sources.
- Source list and source detail as the first section-design pass.
- Existing normalization helpers in code for sources, sinks, stages/operators,
  checkpoints, and delivery derivation; these will be reused as sections are
  reintroduced.
- API routes already available:
  - `GET /jobs`
  - `GET /jobs/{id}`
  - `GET /jobs/{id}/state`
  - `GET /jobs/{id}/metrics`
  - `GET /jobs/{id}/describe`
  - `GET /jobs/{id}/plan`
  - `GET /jobs/{id}/diagnostics`
  - `GET /jobs/{id}/runs`
  - `GET /jobs/{id}/transitions`
  - `GET /jobs/{id}/logs`

Known gaps:

- Sources is still only a first draft and needs focused design.
- Sinks is not visible yet.
- Pipeline needs to combine logical operators and runtime stages into one view.
- Reliability needs to group checkpoints, runs, diagnostics, and logs.
- Overview should be rebuilt last from summaries of the other sections.

---

## Target information architecture

The dashboard should answer one operator question first:

> What is happening in my streaming system right now, and where is the problem?

Use five sections only:

```text
Overview
Sources
Pipeline
Sinks
Reliability
```

The global dashboard and job detail view should use the same model. When a user
clicks a job, the detail tabs should be:

```text
Overview | Sources | Pipeline | Sinks | Reliability
```

Avoid exposing implementation boundaries as top-level UI. Operators, stages,
checkpoints, runs, diagnostics, logs, spec, infrastructure, and deploy are
important capabilities, but they should be grouped under the five sections
unless a later design pass proves they deserve their own surface.

### 1. Overview

Purpose: quick health check.

Show only what helps triage:

- running jobs;
- failed jobs;
- fleet throughput;
- total source lag;
- jobs needing attention;
- compact jobs table:
  - job name;
  - status;
  - source → sink;
  - records/sec;
  - lag;
  - last checkpoint age.

Answers:

- Is the system healthy?
- Is data flowing?
- Which job needs attention first?

Accuracy rules:

- Phase comes from controller run/job state.
- Throughput comes from history points or Prometheus counter deltas.
- Never invent rates from total counters without timestamps.
- If source/sink summary is unavailable until a job starts, display "pending
  describe".

### 2. Sources

Purpose: understand where data enters Weibo.

Show:

- source type: Kafka, file, generator, slice, custom SDK, unknown;
- source identity:
  - Kafka topic/group;
  - file path/basename;
  - generator/source name;
- read rate;
- current offset/position;
- checkpointed offset/position;
- lag;
- source errors/deserialization errors;
- whether the source participates in checkpointing.

For Kafka, show partition detail only after expanding or selecting the source.

Answers:

- Are we reading correctly?
- Are we falling behind at ingress?
- Do we have enough source state for recovery?

Data sources:

- `/describe` for static connector metadata.
- `/state` for operational state and checkpoint progress.
- `/metrics` for rates/errors.
- `/plan` only when needed to map source to runtime stage.

Implementation notes:

- Keep `normalizeSources(job, describe, plan, state, metrics)` as the main data
  boundary.
- Avoid Kafka-only labels in the generic UI.
- Show "source does not expose progress" for connectors without operational
  state.

### 3. Pipeline

Purpose: understand what happens between source and sink.

This combines the old Operators and Runtime Stages ideas. The UI should explain
the processing path without forcing the user to understand internal scheduler
objects first.

Show:

- logical operators:
  - map;
  - filter;
  - flatMap/process;
  - keyBy;
  - window;
  - reduce;
  - join;
  - custom;
- runtime stages:
  - stage name;
  - stage type;
  - workers;
  - records in/out;
  - queue size/capacity;
  - send-block time;
- backpressure indicators;
- stateful operator indicators;
- checkpoint participation when exposed.

Answers:

- Where is processing slow?
- Which operation is stateful?
- Which stage or edge is causing backpressure?

Data sources:

- `/describe` for logical graph/operators.
- `/plan` for runtime stages/edges.
- `/metrics` for operator/stage counters and queue/send-block metrics.
- `/state` for checkpoint/state participation when exposed.

Accuracy rules:

- Do not conflate logical operators with runtime stages.
- Counter totals are totals; rates require history/delta.
- If metric labels change, render "metric unavailable" instead of zero.

### 4. Sinks

Purpose: understand where data leaves Weibo.

Show:

- sink type: Kafka, transactional Kafka, file, Postgres, HTTP, S3, stdout,
  blackhole, custom SDK, unknown;
- destination:
  - Kafka topic;
  - table;
  - URL host/path with query redacted;
  - bucket/path;
  - file path/basename;
- records written;
- sink errors;
- delivery guarantee:
  - at-most-once;
  - at-least-once;
  - exactly-once;
- transaction/checkpoint participation when relevant.

Answers:

- Are we writing correctly?
- Is the sink failing?
- What delivery guarantee actually applies?

Data sources:

- `/describe` for static sink metadata.
- `/metrics` for records/errors.
- `/state` and checkpoint state for transactional/checkpoint detail.

Accuracy rules:

- Delivery guarantee is derived from actual source/sink capabilities and
  checkpointing config, not from connector names alone.
- If transactional metadata is missing, show "coordinated sink detected;
  transaction detail unavailable".

### 5. Reliability

Purpose: debug and recover.

This combines the old Checkpoints, Runs, Diagnostics, and Logs sections. Those
are all reliability concerns, so keep them together until the UI proves a split
is necessary.

Show:

- current job state;
- latest checkpoint;
- checkpoint age;
- checkpoint history;
- savepoint action;
- restart from checkpoint/savepoint;
- attempts/runs;
- lifecycle transitions;
- failure reason;
- logs;
- restart countdown when available.

Answers:

- If something broke, why?
- What happened before the failure?
- Can I recover from a checkpoint or savepoint?

Data sources:

- `/state` for checkpoint state.
- `/diagnostics` for grouped failure/activity/restart/checkpoint status.
- `/runs` and `/runs/{runId}` for attempt history.
- `/transitions` for lifecycle audit.
- `/logs`, `/logs/stream`, and `/runs/{runId}/logs` for logs.

Accuracy rules:

- Attempt details come from `/runs`, not reconstructed from the current job.
- Terminal jobs should not poll live-agent endpoints forever.
- Streaming logs must use bearer auth.
- If logs are unavailable because the backend removed the resource, say that
  clearly.

---

## Current section-by-section build plan

The current product direction is intentionally simpler than the earlier
eight-pane prototype. Treat the older D-phases below as data/model groundwork,
not the final navigation.

Build in this order:

1. **Sources** — ✅ live inventory pass complete; visible section.
2. **Sinks** — ✅ live inventory and delivery guarantee pass complete; visible section.
3. **Pipeline** — ✅ live operator/stage/backpressure pass complete; visible section.
4. **Reliability** — ✅ checkpoint/run/diagnostic recovery pass complete; visible section.
5. **Overview** — ✅ default fleet summary rebuilt from section summaries.

Each section should be designed, implemented, and tested before the next section
is exposed in navigation. Dashboard revamp is complete; remaining work is
validation/polish only.

---

## Data-contract roadmap

### Phase D1 — Inventory the current truth sources — ✅ DONE

Status:

- Added `docs/dashboard-data-contract.md` with endpoint-by-endpoint source of
  truth, key fields, dashboard use, and missing-data behavior.
- D9 accuracy tests now cover the contract surfaces that matter for the
  dashboard: app shell, tabs, live-agent degradation, source/sink/operator
  hooks, auth roles, and secret redaction.

Deliverables:

- Document every field currently returned by:
  - `/jobs`;
  - `/jobs/{id}`;
  - `/jobs/{id}/state`;
  - `/jobs/{id}/describe`;
  - `/jobs/{id}/plan`;
  - `/jobs/{id}/diagnostics`;
  - `/jobs/{id}/runs`;
  - `/jobs/{id}/metrics`.
- Add frontend comments or helper names that identify source-of-truth for every
  displayed metric.
- Add fixtures under `control/api` or `control/ui` tests representing:
  - running Kafka → transactional Kafka;
  - file → file;
  - Postgres sink;
  - failed job with no live agent;
  - SDK job with `/describe` available only while running.

Exit criteria:

- No dashboard section uses guessed data when an API field is missing.
- Missing live-agent endpoints render clearly instead of failing the whole page.

### Phase D2 — Normalize dashboard models in JS — ✅ DONE

Status:

- Implemented in `control/ui/index.html` with pure normalization helpers for
  sources, sinks, stages, operators, checkpoints, and delivery derivation.
- Rendering consumes normalized models instead of directly mixing raw endpoint
  payloads.

Deliverables:

- Add pure helpers:
  - `normalizeJobSummary(job, history)`;
  - `normalizeSources(job, describe, plan, state, metrics)`;
  - `normalizeOperators(job, describe, plan, state, metrics)`;
  - `normalizeStages(plan, metrics)`;
  - `normalizeSinks(job, describe, plan, state, metrics)`;
  - `normalizeCheckpoints(state, job)`;
  - `deriveDelivery(job, sourceModel, sinkModel, checkpoints)`.
- Add minimal browser-free unit coverage if existing test harness allows; else
  add table-driven Go/API tests for backend payload stability and keep JS
  helpers pure enough to smoke-test manually.

Exit criteria:

- Rendering functions consume normalized models, not raw mixed payloads.
- Every model includes `fresh`, `source`, and `missingReason` fields where
  appropriate.

### Phase D3 — Minimal shell and visual system — ✅ DONE

Status:

- The visible dashboard is intentionally reduced to a single Sources section
  while the UI is redesigned section-by-section.
- Earlier multi-section helpers are retained in code for reuse, but Overview,
  Operators, Sinks, Checkpoints, Runs, Logs, Spec, Infrastructure, and Deploy
  are not exposed in the current navigation.

Deliverables:

- Keep dark theme but reduce visual noise:
  - one accent color;
  - semantic status colors only;
  - compact cards;
  - clear table typography;
  - fewer borders/shadows.
- Restructure job detail into fixed tabs:
  - Overview;
  - Sources;
  - Operators;
  - Sinks;
  - Checkpoints;
  - Runs;
  - Logs;
  - Spec.
- Add "data freshness" indicator in job detail header.

Exit criteria:

- Design stays usable at 1280px width.
- No horizontal scrolling except for log/code blocks and large tables.

### Phase D4 — Sources section — ✅ DONE

Status:

- Sources render through generic source cards.
- Kafka partition progress remains available under Kafka-specific detail.
- File, generator/slice, and custom sources degrade to reported fields only.

Deliverables:

- Generic source cards.
- Kafka partition progress table retained but nested under Kafka-specific detail.
- File/source/custom fallback rendering.
- Source errors and deserialization errors from metrics.
- Source lag/position health badges.

Exit criteria:

- Kafka, file, slice/generator, and unknown sources all render accurately.
- If `/state.source` is absent, the UI says so and keeps the rest of the page
  functional.

### Phase D5 — Operators and stages section — ✅ DONE

Status:

- Logical operators and runtime stages render separately.
- Stage table includes throughput, worker, queue, and send-block/backpressure
  signals when metrics are available.

Deliverables:

- Logical operator table from `/describe`/job graph.
- Runtime stage table from `/plan` + stage metrics.
- Backpressure cards:
  - queue size/capacity;
  - send-block seconds;
  - high utilization badge.
- Stateful operator indicators:
  - keyBy/reduce/window/join/keyedProcess;
  - partition count;
  - checkpoint participation when exposed.

Exit criteria:

- Logical operators and runtime stages are not conflated.
- A slow sink/backpressure demo clearly shows which edge/stage is blocking.

### Phase D6 — Sinks section — ✅ DONE

Status:

- Sinks render through generic sink cards with redacted destination details.
- Delivery guarantee is derived from checkpointing plus coordinated sink
  capability instead of trusting the raw spec string.

Deliverables:

- Generic sink cards.
- Destination details with redaction.
- Records written and sink error tiles.
- Delivery guarantee panel.
- Transactional Kafka details when available.
- File/HTTP/S3/Postgres/Kafka-specific detail rows.

---
### Phase D7 — Pipeline section — ✅ DONE

Status:

- Pipeline is exposed as its own minimal section.
- The list view combines logical operators, runtime stages, throughput totals,
  stateful operator count, worker count, and backpressure signals.
- Job detail opens directly to logical operators and runtime stages when entered
  from Pipeline.

Deliverables:

- Pipeline inventory loader.
- Pipeline summary rows.
- Runtime stage and logical operator detail pane.
- Accuracy tests for Pipeline navigation and detail rendering.

---
### Phase D8 — Reliability section — ✅ DONE

Status:

- Reliability is exposed as its own minimal section.
- The list view combines checkpoint health, attempts, failures, restart
  countdowns, last activity, and current job status.
- Job detail opens directly to diagnostics, checkpoint health, attempt history,
  and checkpoint history when entered from Reliability.

Deliverables:

- Reliability inventory loader.
- Reliability summary rows.
- Checkpoint/run/diagnostic detail pane.
- Accuracy tests for Reliability navigation and detail rendering.

---
### Phase D9 — Overview section — ✅ DONE

Status:

- Overview is the default landing page.
- It summarizes job count, source lag, sink errors, pipeline backpressure, and
  reliability attention using the same section inventory loaders as the detail
  pages.
- It links directly into Sources, Sinks, Pipeline, and Reliability without
  reintroducing the old running/completed/deploy navigation.

Deliverables:

- Overview summary rows.
- Default route switched to Overview.
- Accuracy tests for Overview navigation and shell content.

Exit criteria:

- Plain sinks show at-least-once output when checkpointing is enabled.
- Transactional Kafka shows exactly-once only when paired with checkpointing and
  compatible source settings.

### Phase D7 — Checkpoints, state, and savepoints — ✅ DONE

Status:

- Checkpoint tab now shows health, stale/disabled/unavailable states, backend
  config, savepoint/restore actions, richer checkpoint history, and
  checkpointed source positions when reported.

Deliverables:

- Checkpoint health card.
- History table with age/duration/status when fields exist.
- State backend card.
- Savepoint action moved into this section, still available in header actions.
- Restart-from-savepoint affordance with clear wording.

Exit criteria:

- Running jobs with stale checkpoints are visibly flagged.
- Disabled checkpointing is displayed as a configured state, not an error.

### Phase D8 — Runs, diagnostics, and logs polish — ✅ DONE

Status:

- Runs is now the canonical attempt explorer with selected attempt detail,
  transitions, logs, and full lifecycle history.
- Diagnostics are grouped into failure, activity, restart, and checkpoint signal
  panels.
- Logs support tail size, copy, follow/pause for live logs, and previous run
  selection.

Deliverables:

- Runs tab becomes the canonical attempt explorer.
- Diagnostics grouped and de-duplicated.
- Logs tab supports tail size, follow/pause, copy, previous run selector.
- Lifecycle transitions visible on Overview and full list in Runs.

Exit criteria:

- A failed job can be debugged without leaving the dashboard.
- Terminal jobs do not attempt live-agent polling forever.

### Phase D9 — Accuracy tests and regression gates — ✅ DONE

Status:

- `control/api/dashboard_accuracy_test.go` pins the dashboard contract via
  httptest (no browser required, runs in CI `test` job):
  - current Sources-only shell;
  - source detail rendering + normalization helpers;
  - backend contract stability for `/jobs`, `/jobs/{id}`, `/runs`,
    `/transitions`, `/diagnostics`, `/history`, `/config`;
  - graceful degradation when `/state`, `/metrics`, `/describe`, `/plan`
    have no live agent;
  - Kafka/file source, sink, operator/stage, checkpoint, and delivery text;
  - readonly 403s on every mutation, `POST /auth` role reporting, and
    readonly-disabled mutation buttons (`userRole`/`canMutate`/`mutAttr`);
  - secret values absent from every API surface plus `redactProps` coverage.
- `POST /auth` now returns `{status, role}` (`open`/`readonly`/`readwrite`)
  so the dashboard disables Cancel/Restart/Savepoint/Deploy for readonly
  tokens; the backend still enforces 403.

Deliverables:

- API tests for new/changed fields if backend contracts are added.
- Browser-tier tests for:
  - overview renders;
  - job detail tabs render with fixture data;
  - readonly auth hides/disables mutation actions;
  - missing `/state` and `/metrics` degrade gracefully;
  - sources/operators/sinks sections show correct text for Kafka and file jobs.
- Static check for forbidden secret strings in rendered known fixtures.

Exit criteria:

- CI covers dashboard rendering enough that future backend changes cannot silently
  break source/sink/operator sections.

---

## Backend/API additions likely needed

Keep these small and only add them when frontend normalization cannot derive
accurate data from existing endpoints.

1. `/describe` should consistently include:
   - source list;
   - sink list;
   - logical operators;
   - delivery/capability summary;
   - redacted config summaries.
2. `/plan` should consistently include:
   - runtime stages;
   - stage type;
   - operators inside each stage;
   - edge IDs between stages.
3. `/state` should expose connector state generically:
   - `sources: []`;
   - `sinks: []`;
   - `checkpoints: []`;
   - `stateBackend`.
4. `/metrics` remains Prometheus text, but the UI parser should handle missing
   metrics and label drift safely.

Do not add a broad "dashboard blob" endpoint until the normalized frontend model
proves stable. Prefer small, typed additions to existing operational endpoints.

---

## Minimal layout sketch

```text
Sidebar
Sources

Later:
Overview
Sources
Pipeline
Sinks
Reliability

Job detail
order-totals                         running · metrics 3s ago

Overview | Sources | Pipeline | Sinks | Reliability

Sources
Kafka: orders
group order-processor · read_committed
records/s 1.2k · lag 240 · checkpointed yes
[expand partition progress]

Pipeline
operators + runtime stages in one processing view

Sinks
Txn Kafka: customer-totals
exactly-once · records written 1.8M · errors 0

Reliability
latest checkpoint · attempts · diagnostics · logs
```

---

## Suggested implementation order

Build section-by-section:

1. **Sources** — make ingress accurate and calm first.
2. **Sinks** — add egress and delivery semantics next.
3. **Pipeline** — combine operators/stages/backpressure into one processing
   view.
4. **Reliability** — add checkpoints, runs, diagnostics, and logs as one
   recovery/debugging section.
5. **Overview** — build last from summaries produced by the other sections.

Overview is intentionally last: if built first, it will guess. Once Sources,
Sinks, Pipeline, and Reliability are accurate, Overview can become a compact
summary instead of a noisy dashboard-card grid.
