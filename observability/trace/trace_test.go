package trace_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
)

// stub records calls so backends can prove the contract.
type stubSpan struct {
	ended int
	errs  []error
	attrs []trace.Attribute
	tid   string
	sid   string
}

func (s *stubSpan) End()                               { s.ended++ }
func (s *stubSpan) RecordError(err error)              { s.errs = append(s.errs, err) }
func (s *stubSpan) SetAttributes(a ...trace.Attribute) { s.attrs = append(s.attrs, a...) }
func (s *stubSpan) TraceID() string                    { return s.tid }
func (s *stubSpan) SpanID() string                     { return s.sid }

type stubTracer struct{ last *stubSpan }

func (t *stubTracer) Start(ctx context.Context, _ string, attrs ...trace.Attribute) (context.Context, trace.Span) {
	t.last = &stubSpan{tid: "trace-1", sid: "span-1"}
	t.last.SetAttributes(attrs...)
	return trace.ContextWithSpan(ctx, t.last), t.last
}

func TestNoopIsSilent(t *testing.T) {
	ctx, span := trace.Noop().Start(context.Background(), "op", trace.String("k", "v"))
	span.RecordError(errors.New("x"))
	span.SetAttributes(trace.Int("n", 1))
	span.End()
	if trace.SpanFromContext(ctx) != nil {
		t.Error("noop must not stash a span")
	}
	if _, _, ok := trace.IDs(ctx); ok {
		t.Error("noop must not correlate")
	}
	if attrs := trace.Attrs(ctx); len(attrs) != 0 {
		t.Error("noop must yield no log attrs")
	}
}

func TestPropagationAndCorrelation(t *testing.T) {
	tr := &stubTracer{}
	ctx, span := tr.Start(context.Background(), "launch", trace.String("job", "j1"), trace.Int("attempt", 2))
	defer span.End()
	if trace.SpanFromContext(ctx) == nil {
		t.Fatal("span missing from context")
	}
	tid, sid, ok := trace.IDs(ctx)
	if !ok || tid != "trace-1" || sid != "span-1" {
		t.Fatalf("ids=%q,%q ok=%v", tid, sid, ok)
	}
	attrs := trace.Attrs(ctx)
	if len(attrs) != 4 || attrs[0] != "trace_id" || attrs[1] != "trace-1" {
		t.Fatalf("attrs=%v", attrs)
	}
	if len(tr.last.attrs) != 2 {
		t.Fatalf("start attrs lost: %+v", tr.last.attrs)
	}
}
