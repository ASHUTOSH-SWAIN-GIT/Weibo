# weibo-runner

The generic job entrypoint baked into the runner container image (job
orchestration, phase P2). One prebuilt image runs **any** YAML/JSON workflow:
mount the document, point `WORKFLOW` at it, mount a volume for durable state.

## Configuration (environment)

| Var        | Required | Default | Purpose |
| ---------- | -------- | ------- | ------- |
| `WORKFLOW`           | yes | —            | Path to the mounted workflow file. |
| `DATA_DIR`           | no  | `/data`      | Base dir; the engine derives `<name>/state` and `<name>/checkpoints` under it. Mount a volume here for durability. |
| `SAVEPOINT_DIR`      | no  | `/savepoints`| Directory where the runner promotes named savepoints. It is portable only across jobs that can read the same storage namespace. |
| `SAVEPOINT_S3_BUCKET`| no  | —            | Use S3-compatible object storage for savepoints instead of `SAVEPOINT_DIR`. |
| `SAVEPOINT_S3_PREFIX`| no  | —            | Key prefix for S3 savepoint objects. |
| `SAVEPOINT_S3_ENDPOINT` | no | —          | S3-compatible endpoint override. |
| `SAVEPOINT_S3_PATH_STYLE` | no | `false`  | Use path-style bucket addressing for S3-compatible stores. |
| `SAVEPOINT_S3_SSE`   | no  | —            | Server-side encryption mode, e.g. `AES256` or `aws:kms`. |
| `SAVEPOINT_S3_KMS_KEY_ID` | no | —        | KMS key ID when using KMS encryption. |
| `RESTORE_SAVEPOINT`  | no  | —            | Name of a savepoint to seed state from before starting. |
| `PORT`               | no  | `8080`       | Agent HTTP control port. |
| `WEIBO_JOB_ID`      | no  | —            | Injected by the controller; reference it (`transactionalID: ${WEIBO_JOB_ID}`) to pin a stable exactly-once id across restarts. |

Secret placeholders (`${VAR}`) in the workflow resolve from the process
environment at compile time — pass them with `-e`.

S3 savepoints use the standard AWS credential chain. Set `AWS_REGION`,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` as needed,
or rely on the pod/instance role.

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
docker build -f Dockerfile.runner -t weibo-runner:dev .
```

## Run

```sh
docker run -d -p 8080:8080 \
  -v "$PWD/examples/workflows/order-totals.yaml:/wf/job.yaml:ro" \
  -v weibo-job-data:/data \
  -e WORKFLOW=/wf/job.yaml \
  weibo-runner:dev

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
