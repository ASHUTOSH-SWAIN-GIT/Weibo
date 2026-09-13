// Package trace is Weibo's tracing contract: Tracer/Span interfaces over
// coarse operations (launches, reconciles, checkpoint finalizes, sink
// flushes — never per-record work). It depends only on the standard
// library, so instrumented code stays free of tracing clients.
//
// Any backend whose types match these method sets plugs in with zero
// imports — the OpenTelemetry bridge lives in the separate telemetry
// module and is verified compatible by the control module's tests.
// Unconfigured code uses Noop, which costs nothing and is safe to leave
// in hot paths.
package trace

import "context"

// Attribute is one span/log attribute. Values should be strings, ints,
// bools, floats, or durations — the bridge converts the rest with %v.
type Attribute struct {
	Key   string
	Value any
}

// String builds a string attribute.
func String(key, value string) Attribute { return Attribute{Key: key, Value: value} }

// Int64 builds an integer attribute.
func Int64(key string, value int64) Attribute { return Attribute{Key: key, Value: value} }

// Int builds an integer attribute.
func Int(key string, value int) Attribute { return Attribute{Key: key, Value: int64(value)} }

// Bool builds a boolean attribute.
func Bool(key string, value bool) Attribute { return Attribute{Key: key, Value: value} }

// Span is one coarse operation. End must be called exactly once;
// RecordError marks the span failed without ending it.
type Span interface {
	// End finishes the span.
	End()
	// RecordError marks the span failed with err (nil is a no-op).
	RecordError(err error)
	// SetAttributes adds attributes to the span.
	SetAttributes(attrs ...Attribute)
	// TraceID/SpanID identify the span for log correlation (""
	// when the backend has none).
	TraceID() string
	SpanID() string
}

// Tracer starts spans. Implementations must be safe for concurrent use.
type Tracer interface {
	// Start begins a span and returns a context carrying it (see
	// ContextWithSpan). The caller must call Span.End.
	Start(ctx context.Context, name string, attrs ...Attribute) (context.Context, Span)
}

// ctxKey carries the current Span.
type ctxKey struct{}

// Noop returns a tracer that records nothing.
func Noop() Tracer { return noopTracer{} }

type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _ string, _ ...Attribute) (context.Context, Span) {
	return ctx, noopSpan{}
}

type noopSpan struct{}

func (noopSpan) End()                       {}
func (noopSpan) RecordError(error)          {}
func (noopSpan) SetAttributes(...Attribute) {}
func (noopSpan) TraceID() string            { return "" }
func (noopSpan) SpanID() string             { return "" }

// ContextWithSpan returns a context carrying span.
func ContextWithSpan(ctx context.Context, span Span) context.Context {
	return context.WithValue(ctx, ctxKey{}, span)
}

// SpanFromContext returns the span carried by ctx (nil when none).
func SpanFromContext(ctx context.Context) Span {
	if span, ok := ctx.Value(ctxKey{}).(Span); ok {
		return span
	}
	return nil
}

// IDs returns the trace/span IDs carried by ctx for log correlation.
// ok is false when no span is present (or it carries no IDs).
func IDs(ctx context.Context) (traceID, spanID string, ok bool) {
	span := SpanFromContext(ctx)
	if span == nil {
		return "", "", false
	}
	traceID, spanID = span.TraceID(), span.SpanID()
	if traceID == "" {
		return "", "", false
	}
	return traceID, spanID, true
}

// Attrs returns trace correlation attributes for a log call (empty when
// no span is present), so logs join traces with no extra plumbing.
func Attrs(ctx context.Context) []any {
	traceID, spanID, ok := IDs(ctx)
	if !ok {
		return nil
	}
	return []any{"trace_id", traceID, "span_id", spanID}
}
