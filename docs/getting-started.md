# Getting Started

## Install

Weibo is a Go library, not a binary you install — you import it into your
own module:

```sh
go get github.com/ASHUTOSH-SWAIN-GIT/weibo
```

Requires Go 1.26+ (see `go.mod`).

## Run an example

The fastest way to see a pipeline run is the in-memory word count example —
no Kafka, no Postgres, nothing external required:

```sh
go run ./examples/wordcount
```

Other examples build on that, each isolating one concept:

| Example | What it shows |
|---|---|
| `examples/wordcount` | `FlatMap → KeyBy → Reduce`, the smallest complete pipeline |
| `examples/windowing` | Event time, watermarks, tumbling windows |
| `examples/backpressure` | Fast source + slow sink, bounded edges |
| `examples/exactly-once` | Kafka → keyed reduce → transactional Kafka |
| `examples/kafka-orders` | Kafka → Window → Kafka |
| `examples/pg-orders` | Kafka → Postgres |
| `examples/sdk-demo` | Minimal SDK usage walkthrough |
| `examples/stream-demo` | Continuously-running job used to exercise the [dashboard](dashboard.md) |
| `examples/workflows` | Sample YAML pipelines for the declarative runner |

Kafka/Postgres-backed examples need those services reachable — see each
example's own source for the expected connection settings, or
`deploy/compose` for a local stack.

## Write your own pipeline

Once you've run an example, see [SDK API](sdk-api.md) for the fluent chain
you'll actually write, and [Core Concepts](concepts.md) for what each piece
(Source, Operator, State, Checkpoint, …) means.

## Run the control plane + dashboard

If you want to see a job's live state (records/sec, checkpoints, lag) in a
UI instead of stdout logs, boot the control plane:

```sh
make dashboard-demo    # builds a demo job image, boots the dashboard, submits it
```

See [Dashboard](dashboard.md) for screenshots and [Self-hosting](self-hosting.md)
for running the control plane for real, beyond a local demo.

## Run the test suite

```sh
make test        # unit + control-plane + telemetry tests
make ci          # the same checks hosted CI runs
```

`make help` lists every available target.

---

Next: [Core Concepts](concepts.md).
