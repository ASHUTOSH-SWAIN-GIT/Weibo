// Package trace adapts the telemetry module's OpenTelemetry tracer to
// the engine's tracing contract (weibo/observability/trace). The two
// method sets match structurally ({Key, Value} attributes, End /
// RecordError / SetAttributes / TraceID / SpanID spans); this adapter
// only converts the attribute slices so neither the engine nor this
// module needs the other's imports.
package trace

import (
	"context"

	wtrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
	teltrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/telemetry/trace"
)

// Adapt wraps an OpenTelemetry-backed tracer as an engine tracer. A nil
// provider yields the no-op tracer.
func Adapt(t teltrace.Tracer) wtrace.Tracer {
	if t == nil {
		return wtrace.Noop()
	}
	return adapter{t: t}
}

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
