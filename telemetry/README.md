# telemetry — OpenTelemetry bridge for Weibo

A separate Go module (see `go.work`) so the engine never depends on
tracing clients. It implements Weibo's tracing contract
(`weibo/observability/trace`) over the OpenTelemetry SDK with OTLP/HTTP
export.

## Use

```go
prov, shutdown, err := trace.Configure(ctx, trace.Options{
    Endpoint: "http://collector:4318", // "" disables export (no-op)
    ServiceName: "weibo-controller",
})
if err != nil { ... }
defer shutdown(ctx) // flush before exit
```

Standard environment variables fill anything unset:
`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_INSECURE`,
`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_SERVICE_NAME`.

## Wiring

The method sets mirror the engine contract, but Go treats the two
`Attribute` structs as distinct types, so each consumer adapts explicitly:

- controller → `control/trace.Adapt`
- job runner → `cmd/weibo-runner` local adapter

Both are ~25 lines and covered by tests that export to a local collector.
