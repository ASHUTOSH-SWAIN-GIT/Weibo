package main

import (
	"context"
	"log/slog"
	"time"

	wlog "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/log"
	wtrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
	teltrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/telemetry/trace"
)

// adapter converts the telemetry tracer to the engine contract (same
// {Key, Value} attribute shape; see control/trace for the twin).
type adapter struct{ t teltrace.Tracer }

func (a adapter) Start(ctx context.Context, name string, attrs ...wtrace.Attribute) (context.Context, wtrace.Span) {
	conv := make([]teltrace.Attribute, len(attrs))
	for i, attr := range attrs {
		conv[i] = teltrace.Attribute{Key: attr.Key, Value: attr.Value}
	}
	ctx, span := a.t.Start(ctx, name, conv...)
	return wtrace.ContextWithSpan(ctx, spanAdapter{span: span}), spanAdapter{span: span}
}

type spanAdapter struct{ span teltrace.Span }

func (s spanAdapter) End() { s.span.End() }

func (s spanAdapter) RecordError(err error) { s.span.RecordError(err) }

func (s spanAdapter) SetAttributes(attrs ...wtrace.Attribute) {
	conv := make([]teltrace.Attribute, len(attrs))
	for i, attr := range attrs {
		conv[i] = teltrace.Attribute{Key: attr.Key, Value: attr.Value}
	}
	s.span.SetAttributes(conv...)
}

func (s spanAdapter) TraceID() string { return s.span.TraceID() }

func (s spanAdapter) SpanID() string { return s.span.SpanID() }

// setupTelemetry builds the runner logger and optional tracer from the
// environment:
//
//	LOG_LEVEL  debug|info|warn|error            (default info)
//	LOG_FORMAT text|json                         (default json: containers)
//	OTEL_EXPORTER_OTLP_ENDPOINT                  (unset disables tracing)
//	OTEL_SERVICE_NAME                            (default weibo-runner)
//
// It returns the logger, the engine tracer (no-op when disabled), and a
// flush function to defer before process exit.
func setupTelemetry(getenv func(string) string, serviceName string) (*slog.Logger, wtrace.Tracer, func()) {
	level, err := wlog.ParseLevel(getenv("LOG_LEVEL"))
	if err != nil {
		level = slog.LevelInfo
	}
	format := getenv("LOG_FORMAT")
	if format == "" {
		format = "json"
	}
	logger := wlog.New(level, format)

	name := getenv("OTEL_SERVICE_NAME")
	if name == "" {
		name = serviceName
	}
	tel, shutdown, err := teltrace.Configure(context.Background(), teltrace.Options{ServiceName: name})
	if err != nil {
		logger.Warn("tracing disabled: configure failed", "error", err)
		return logger, wtrace.Noop(), func() {}
	}
	if shutdown == nil {
		return logger, wtrace.Noop(), func() {}
	}
	flush := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	}
	return logger, adapter{t: tel}, flush
}
