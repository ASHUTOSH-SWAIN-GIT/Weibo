# mailer-runner

The generic job entrypoint baked into the runner container image (job
orchestration, phase P2). One prebuilt image runs **any** YAML/JSON workflow:
mount the document, point `WORKFLOW` at it, mount a volume for durable state.

## Configuration (environment)

| Var        | Required | Default | Purpose |
| ---------- | -------- | ------- | ------- |
| `WORKFLOW`           | yes | —            | Path to the mounted workflow file. |
| `DATA_DIR`           | no  | `/data`      | Base dir; the engine derives `<name>/state` and `<name>/checkpoints` under it. Mount a volume here for durability. |
| `SAVEPOINT_DIR`      | no  | `/savepoints`| Shared blobstore for savepoints. Mount a shared volume so any job can restore any savepoint. |
| `RESTORE_SAVEPOINT`  | no  | —            | Name of a savepoint to seed state from before starting. |
| `PORT`               | no  | `8080`       | Agent HTTP control port. |
| `MAILER_JOB_ID`      | no  | —            | Injected by the controller; reference it (`transactionalID: ${MAILER_JOB_ID}`) to pin a stable exactly-once id across restarts. |

Secret placeholders (`${VAR}`) in the workflow resolve from the process
environment at compile time — pass them with `-e`.

## Control surface

The runner supervises the job with a `jobagent` and serves it on `PORT`:

| Route            | Purpose |
| ---------------- | ------- |
| `GET /healthz`   | Liveness + current phase. |
| `GET /state`     | Lifecycle snapshot (phase, uptime, records in/out, last checkpoint). |
| `GET /describe`  | Compiled pipeline topology (JSON). |
| `GET /metrics`   | Prometheus exposition. |
| `POST /cancel`   | Request graceful shutdown. |
| `POST /savepoint?label=<name>` | Stop-with-savepoint: drain, write a final checkpoint, and promote it to a named savepoint. |

On `SIGTERM`/`SIGINT` the job drains in-flight records and takes a final
checkpoint before the process exits.

## Build

```sh
docker build -f Dockerfile.runner -t mailer-runner:dev .
```

## Run

```sh
docker run -d -p 8080:8080 \
  -v "$PWD/examples/workflows/order-totals.yaml:/wf/job.yaml:ro" \
  -v mailer-job-data:/data \
  -e WORKFLOW=/wf/job.yaml \
  mailer-runner:dev

curl localhost:8080/state
docker stop <container>          # SIGTERM → graceful drain
```

## Recovery across restart

Restarting with the **same** `/data` volume lets the engine load the latest
checkpoint on startup and resume instead of replaying from zero. A meaningful
cross-restart resume needs a *resumable* source that tracks offsets (Kafka) —
the bundled `generator` source emits a fixed batch and finishes, so it has no
offset to resume from. End-to-end offset-resume in a container is exercised as
part of phase P4 (durable checkpoints); the engine recovery path itself is
covered by the in-process recovery tests (`test/unit_tests/recovery_test.go`,
`e2e_checkpoint_test.go`).
