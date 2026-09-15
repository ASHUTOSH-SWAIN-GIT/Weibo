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
- **Sections mirror the runtime.** Sources, operators/stages, sinks,
  checkpoints/state, runs/lifecycle, logs/diagnostics.
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

The dashboard already provides:

- Embedded plain-JS SPA in `control/ui/index.html`.
- Routes: overview, infrastructure, running, completed, submit, job detail.
- Job metadata/action strip.
- Inline SVG pipeline graph.
- Metrics tab with stage/operator tables parsed from Prometheus text.
- Checkpoints, runs, logs, spec, diagnostics.
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

- Source/sink/operator data is scattered across Pipeline, Metrics, and State.
- Graph is visually useful but not operationally rich.
- Metrics parsing is ad hoc and lacks an explicit "freshness" or source
  attribution model.
- Operator/stage/backpressure/state are not presented as one coherent runtime
  picture.
- Sources are Kafka-biased; file/slice/generator and future connectors need a
  generic model.
- Sinks are underrepresented; delivery semantics and error/retry behavior should
  be visible.

---

## Target information architecture

### 1. Fleet overview

Purpose: fast triage across all jobs.

Show:

- Header stats: running, failed, finished, cancelled, restarting, total.
- Throughput total: latest fleet records/sec from `/jobs/history`.
- Attention list:
  - failed jobs;
  - running jobs with source lag growing;
  - jobs with sink errors;
  - jobs with checkpoint age above threshold;
  - jobs with no metrics heartbeat.
- Jobs table:
  - name/id;
  - phase;
  - kind/delivery;
  - source → sink summary;
  - records out/sec;
  - latest checkpoint age;
  - last updated.

Accuracy rules:

- Phase comes from controller run/job state, not CSS-only interpretation.
- Throughput comes from history points or Prometheus counter delta; never invent
  rates from total counters without timestamps.
- If source/sink summary is unavailable until job starts, display "pending
  describe".

### 2. Job detail overview

Purpose: one screen for "what is this job doing?"

Sections:

- **Status strip**
  - phase;
  - attempt;
  - uptime;
  - delivery guarantee;
  - latest checkpoint;
  - metrics freshness;
  - action buttons.
- **Dataflow summary**
  - source cards on the left;
  - operator/stage path in the middle;
  - sink cards on the right;
  - minimal arrows; compact labels.
- **Health summary**
  - source lag;
  - records in/out rate;
  - failed records;
  - sink errors;
  - checkpoint age;
  - restart count;
  - diagnostics severity.
- **Recent events**
  - lifecycle transitions;
  - latest diagnostics;
  - latest checkpoint;
  - latest error log excerpt.

Accuracy rules:

- Metrics freshness must be based on fetch time + successful parse time.
- If a job is terminal, freeze live sections and prefer latest run snapshots/logs
  when available.

### 3. Sources section

Purpose: explain ingress and read progress.

For every source, show:

- connector type: Kafka, file, slice, generator, custom SDK, unknown;
- identity: topic(s), file path/basename, source name, or logical source ID;
- delivery capability:
  - checkpoint offsets;
  - positioned checkpoints;
  - external offset commit;
  - operational state available;
- current position:
  - Kafka: topic, partition, current offset, checkpoint offset, high watermark,
    lag;
  - file: source name, next line/offset when reported;
  - custom: display reported fields only;
- read rate and records emitted;
- source errors/deserialization failures;
- watermark status when available:
  - latest watermark;
  - idle or active;
  - max out-of-orderness when described.

Data sources:

- `/describe` for static connector metadata.
- `/state` for operational state and checkpoint progress.
- `/metrics` for rates/errors.
- `/plan` for source/stage mapping when available.

Implementation notes:

- Build a connector-normalization layer in JS:
  `normalizeSources(job, describe, plan, state, metrics)`.
- Avoid Kafka-only table titles. Use generic "Source progress"; render Kafka
  partition table only for Kafka.
- Show "source does not expose progress" for connectors without operational
  state.

### 4. Operators section

Purpose: make transformations and bottlenecks inspectable.

Show two views:

1. **Logical operators**
   - ID/label;
   - type: map, filter, flatMap, process, keyBy, reduce, window, join,
     keyedProcess, custom;
   - parallelism/partitions;
   - records processed;
   - failures/DLQ count when available;
   - stateful yes/no;
   - checkpoint participation.

2. **Runtime stages**
   - stage ID;
   - type: source/stateless/keyed/sink/join;
   - records in/out;
   - worker count;
   - send-block seconds;
   - edge queue size/capacity before/after;
   - bottleneck badge when queue stays high or send-block increases.

Data sources:

- `/plan` for runtime stage layout.
- `/describe` for logical graph.
- `/metrics` for operator/stage counters.
- `/state` for state/checkpoint info where exposed.

Accuracy rules:

- Do not equate logical operators with runtime stages. Show both because one
  stage can contain multiple stateless operators.
- Counter totals are totals; rates require history/delta.
- Use exact Prometheus labels; if labels change, render "metric unavailable" not
  zero.

### 5. Sinks section

Purpose: explain egress, write progress, and delivery semantics.

For every sink, show:

- connector type: Kafka, transactional Kafka, Postgres, HTTP, S3, file, stdout,
  blackhole, custom SDK, unknown;
- destination identity:
  - Kafka topic;
  - Postgres table;
  - HTTP URL host/path with query redacted;
  - S3 bucket/prefix;
  - file path/basename;
  - stdout/blackhole;
- delivery guarantee:
  - exactly-once coordinated;
  - at-least-once;
  - at-most-once/no checkpointing;
- batching/retry config when described;
- records written;
- sink errors;
- last successful flush/commit when available;
- transactional state for `TxnKafkaSink`:
  - transaction ID;
  - marker topic;
  - prepared/committed checkpoint status if exposed.

Data sources:

- `/describe` for static sink metadata.
- `/metrics` for records/errors.
- `/state` and checkpoint state for transactional/checkpoint information.

Accuracy rules:

- Delivery guarantee is derived from actual source/sink capabilities and
  checkpointing config, not from connector names alone.
- If transactional sink metadata is missing, show "coordinated sink detected;
  transaction detail unavailable".

### 6. Checkpoints & state section

Purpose: make fault tolerance understandable.

Show:

- checkpointing enabled/disabled;
- interval;
- storage backend/path/bucket where safe;
- state backend: memory/Pebble/custom;
- latest completed checkpoint ID/time/age;
- in-progress/prepared checkpoint if exposed;
- checkpoint history table;
- source positions stored in latest checkpoint;
- operator/native state backend summary when available;
- savepoint actions and restart-from-savepoint path.

Accuracy rules:

- "Healthy checkpointing" requires a recent completed checkpoint for running
  jobs with checkpointing enabled; otherwise show stale/disabled explicitly.
- Never show "exactly-once" unless a coordinated sink and checkpointing are both
  actually configured.

### 7. Runs, lifecycle, and diagnostics

Purpose: debug lifecycle issues.

Show:

- attempt list with start/stop time, phase, error.
- transition log with reason and actor if available.
- diagnostics grouped by severity:
  - validation;
  - backend launch;
  - runtime;
  - resource cleanup;
  - API/proxy.
- restart policy and next restart countdown when available.

Accuracy rules:

- Attempt details should come from `/runs`, not reconstructed from current job.
- Transition order must be monotonic by server timestamp.

### 8. Logs section

Purpose: low-friction debugging.

Show:

- current run logs by default;
- previous run selector;
- follow/pause;
- tail size selector: 100/200/500/1000;
- copy button;
- error highlighting that is purely visual and never hides lines.

Accuracy rules:

- Streaming logs must use bearer auth.
- If logs are unavailable because the backend removed the resource, show the
  stored run log route when available.

---

## Data-contract roadmap

### Phase D1 — Inventory the current truth sources

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

- Job detail is split into Overview, Sources, Operators, Sinks, Checkpoints,
  Runs, Logs, and Spec tabs.
- Header includes a freshness badge based on live fetch/parse results.

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

### Phase D8 — Runs, diagnostics, and logs polish

Deliverables:

- Runs tab becomes the canonical attempt explorer.
- Diagnostics grouped and de-duplicated.
- Logs tab supports tail size, follow/pause, copy, previous run selector.
- Lifecycle transitions visible on Overview and full list in Runs.

Exit criteria:

- A failed job can be debugged without leaving the dashboard.
- Terminal jobs do not attempt live-agent polling forever.

### Phase D9 — Accuracy tests and regression gates

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
Job: order-totals                         running · metrics 3s ago
[Cancel] [Savepoint] [Restart] [Grafana]

Health
records out/s | source lag | sink errors | checkpoint age | restarts

Dataflow
[Source: Kafka orders ×12 partitions] → [Operators: 5 logical / 4 stages] → [Sink: Txn Kafka totals]

Tabs
Overview | Sources | Operators | Sinks | Checkpoints | Runs | Logs | Spec

Sources
┌ Kafka: orders ───────────────────────────────┐
│ group order-processor · read_committed        │
│ records/s 1.2k · lag 240 · checkpointed yes   │
│ partitions table...                           │
└───────────────────────────────────────────────┘

Operators
Logical operators table
Runtime stages table + edge backpressure

Sinks
┌ Txn Kafka: customer-totals ───────────────────┐
│ exactly-once · txn id customer-totals-v1      │
│ records written 1.8M · errors 0               │
└───────────────────────────────────────────────┘
```

---

## Suggested implementation order

1. D1 + D2 first: data inventory and normalization helpers.
2. D3: visual shell and tabs.
3. D4/D6 together enough to show source → sink accurately.
4. D5: operators/stages/backpressure.
5. D7/D8: operational depth.
6. D9: tests and CI hardening.

This order keeps the revamp honest: data model first, UI second, polish last.
