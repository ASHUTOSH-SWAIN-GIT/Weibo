// Package trace is the OpenTelemetry backend for Weibo's tracing
// contract (weibo/observability/trace). It is a separate module so the
// engine never depends on OpenTelemetry: the Tracer/Span/Attribute method
// sets mirror the engine contract exactly, and the thin adapters in
// control/trace and cmd/weibo-runner convert the attribute slices (same
// {Key, Value} shape) when handing a Provider to engine code.
//
// Tracing is optional end to end: an empty OTLP endpoint yields a no-op
// tracer, so binaries run identically with no collector configured.
package trace

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// Attribute mirrors weibo/observability/trace.Attribute.
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

// Span mirrors weibo/observability/trace.Span.
type Span interface {
	End()
	RecordError(err error)
	SetAttributes(attrs ...Attribute)
	TraceID() string
	SpanID() string
}

// Tracer mirrors weibo/observability/trace.Tracer.
type Tracer interface {
	Start(ctx context.Context, name string, attrs ...Attribute) (context.Context, Span)
}

// Options configures the OTLP provider. Zero Options (in particular an
// empty Endpoint) selects the no-op tracer.
type Options struct {
	// Endpoint is the OTLP/HTTP collector endpoint, e.g.
	// "http://collector:4318" (insecure) or "https://collector:4318".
	// It defaults from OTEL_EXPORTER_OTLP_ENDPOINT (+ "/v1/traces" is
	// appended by the exporter when missing); empty disables export.
	Endpoint string
	// Insecure uses http instead of https. Defaults from
	// OTEL_EXPORTER_OTLP_INSECURE ("true"/"1").
	Insecure bool
	// Headers are sent on every OTLP request (e.g. authentication).
	// Defaults from OTEL_EXPORTER_OTLP_HEADERS ("k=v,k2=v2").
	Headers map[string]string
	// ServiceName/Version identify this process. ServiceName defaults
	// from OTEL_SERVICE_NAME, then "weibo".
	ServiceName    string
	ServiceVersion string
	// SampleRatio is the parent-based ratio, 0..1 (default 1).
	SampleRatio float64
	// Timeout bounds each export; default 10s.
	Timeout time.Duration
}

// Provider is an SDK-backed Tracer. Shut it down (with a timeout
// context) before process exit to flush pending spans.
type Provider struct {
	tracer oteltrace.Tracer
	tp     *sdktrace.TracerProvider
}

// Configure builds a Provider from opts, reading standard OTEL_*
// environment variables for anything unset. An empty endpoint returns a
// no-op Provider (nil shutdown) — tracing stays off.
func Configure(ctx context.Context, opts Options) (*Provider, func(context.Context) error, error) {
	opts = withEnvDefaults(opts)
	if opts.Endpoint == "" {
		return &Provider{}, nil, nil
	}
	endpointOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(opts.Endpoint),
		otlptracehttp.WithTimeout(defaultTimeout(opts.Timeout)),
	}
	if opts.Insecure {
		endpointOpts = append(endpointOpts, otlptracehttp.WithInsecure())
	}
	if len(opts.Headers) > 0 {
		endpointOpts = append(endpointOpts, otlptracehttp.WithHeaders(opts.Headers))
	}
	exp, err := otlptracehttp.New(ctx, endpointOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry: otlp exporter: %w", err)
	}
	ratio := opts.SampleRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 1
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithResource(resource.NewSchemaless(
			semconv.ServiceNameKey.String(serviceName(opts.ServiceName)),
			semconv.ServiceVersionKey.String(opts.ServiceVersion),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return &Provider{tracer: tp.Tracer("weibo"), tp: tp}, tp.Shutdown, nil
}

// Start begins a span, storing it in ctx for nested operations.
func (p *Provider) Start(ctx context.Context, name string, attrs ...Attribute) (context.Context, Span) {
	if p.tracer == nil {
		return ctx, noopSpan{}
	}
	ctx, span := p.tracer.Start(ctx, name, oteltrace.WithAttributes(toOtel(attrs)...))
	return ctx, &otelSpan{span: span}
}

func toOtel(attrs []Attribute) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		switch v := a.Value.(type) {
		case string:
			out = append(out, attribute.String(a.Key, v))
		case bool:
			out = append(out, attribute.Bool(a.Key, v))
		case int:
			out = append(out, attribute.Int(a.Key, v))
		case int64:
			out = append(out, attribute.Int64(a.Key, v))
		case float64:
			out = append(out, attribute.Float64(a.Key, v))
		case time.Duration:
			out = append(out, attribute.Int64(a.Key, int64(v)))
		case nil:
		default:
			out = append(out, attribute.String(a.Key, fmt.Sprintf("%v", v)))
		}
	}
	return out
}

type otelSpan struct{ span oteltrace.Span }

func (s *otelSpan) End() { s.span.End() }

func (s *otelSpan) RecordError(err error) {
	if err != nil {
		s.span.RecordError(err)
	}
}

func (s *otelSpan) SetAttributes(attrs ...Attribute) { s.span.SetAttributes(toOtel(attrs)...) }

func (s *otelSpan) TraceID() string { return s.span.SpanContext().TraceID().String() }

func (s *otelSpan) SpanID() string { return s.span.SpanContext().SpanID().String() }

type noopSpan struct{}

func (noopSpan) End()                       {}
func (noopSpan) RecordError(error)          {}
func (noopSpan) SetAttributes(...Attribute) {}
func (noopSpan) TraceID() string            { return "" }
func (noopSpan) SpanID() string             { return "" }

func defaultTimeout(d time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return 10 * time.Second
}

func serviceName(s string) string {
	if s != "" {
		return s
	}
	return "weibo"
}
