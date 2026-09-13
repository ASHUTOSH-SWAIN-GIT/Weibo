package main

import (
	"context"
	"testing"

	wtrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
	teltrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/telemetry/trace"
)

func TestSetupTelemetryDefaults(t *testing.T) {
	env := map[string]string{}
	logger, tracer, flush := setupTelemetry(func(k string) string { return env[k] }, "weibo-runner")
	if logger == nil || tracer == nil || flush == nil {
		t.Fatal("setup must always return usable values")
	}
	ctx, span := tracer.Start(context.Background(), "op", wtrace.String("k", "v"))
	span.End()
	if span.TraceID() != "" {
		t.Error("no endpoint configured: tracer must be no-op")
	}
	_ = ctx
	flush() // no-op without an endpoint; must not hang or panic
}

// fakeProvider records spans without OpenTelemetry.
type fakeProvider struct {
	names []string
	attrs [][]teltrace.Attribute
}

type fakeSpan struct{ ended *bool }

func (f *fakeProvider) Start(_ context.Context, name string, attrs ...teltrace.Attribute) (context.Context, teltrace.Span) {
	f.names = append(f.names, name)
	f.attrs = append(f.attrs, attrs)
	ended := false
	return context.Background(), &fakeSpan{ended: &ended}
}

func (s *fakeSpan) End()                                {}
func (s *fakeSpan) RecordError(error)                   {}
func (s *fakeSpan) SetAttributes(...teltrace.Attribute) {}
func (s *fakeSpan) TraceID() string                     { return "" }
func (s *fakeSpan) SpanID() string                      { return "" }

func TestAdapterConvertsAttributes(t *testing.T) {
	fake := &fakeProvider{}
	tr := adapter{t: fake}
	ctx, span := tr.Start(context.Background(), "agent.run",
		wtrace.String("job", "j1"), wtrace.Int("attempt", 2), wtrace.Bool("ok", true))
	span.SetAttributes(wtrace.Int64("n", 3))
	span.End()
	_ = ctx
	if len(fake.names) != 1 || fake.names[0] != "agent.run" {
		t.Fatalf("names=%v", fake.names)
	}
	got := map[string]any{}
	for _, a := range fake.attrs[0] {
		got[a.Key] = a.Value
	}
	if got["job"] != "j1" || got["attempt"] != int64(2) || got["ok"] != true {
		t.Fatalf("start attrs lost: %v", got)
	}
	if len(fake.attrs) != 1 {
		t.Fatalf("attrs recorded %d times: %v", len(fake.attrs), fake.attrs)
	}
}
