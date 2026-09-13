package control_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/lifecycle"
	wtrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
)

// stubTracer records spans for controller observability assertions.
type stubTracer struct {
	mu    sync.Mutex
	spans []*stubSpan
}

type stubSpan struct {
	name  string
	attrs []wtrace.Attribute
	ended bool
	errs  []error
}

func (t *stubTracer) Start(ctx context.Context, name string, attrs ...wtrace.Attribute) (context.Context, wtrace.Span) {
	s := &stubSpan{name: name}
	s.attrs = append(s.attrs, attrs...)
	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.mu.Unlock()
	return wtrace.ContextWithSpan(ctx, s), s
}

func (s *stubSpan) End()                                { s.ended = true }
func (s *stubSpan) RecordError(err error)               { s.errs = append(s.errs, err) }
func (s *stubSpan) SetAttributes(a ...wtrace.Attribute) { s.attrs = append(s.attrs, a...) }
func (s *stubSpan) TraceID() string                     { return "" }
func (s *stubSpan) SpanID() string                      { return "" }

func (t *stubTracer) names() map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]int{}
	for _, s := range t.spans {
		out[s.name]++
	}
	return out
}

type captureLogs struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (c *captureLogs) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.b.Write(p)
}

func (c *captureLogs) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.b.String()
}

func TestControllerStructuredLogsRedactSecrets(t *testing.T) {
	logs := &captureLogs{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	fake := backend.NewFake()
	c := newControllerWith(t, fake, control.Options{Logger: logger})

	const secret = "s3cr3t-value-xyz"
	if _, err := c.Submit(context.Background(), []byte(validSDKManifest), map[string]string{"API_KEY": secret}); err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if strings.Contains(out, secret) {
		t.Errorf("secret value leaked into controller logs:\n%s", out)
	}
	if !strings.Contains(out, "job submitted") || !strings.Contains(out, "API_KEY") {
		t.Errorf("expected key-names-only submit log:\n%s", out)
	}
}

func TestControllerLaunchAndReconcileSpans(t *testing.T) {
	tr := &stubTracer{}
	fake := backend.NewFake()
	c := newControllerWith(t, fake, control.Options{Tracer: tr})

	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	names := tr.names()
	if names["controller.launch"] != 1 {
		t.Errorf("launch spans: %v", names)
	}
	if names["controller.reconcile"] != 1 {
		t.Errorf("reconcile spans: %v", names)
	}
	// The launch span carries the job ID for filtering.
	tr.mu.Lock()
	defer tr.mu.Unlock()
	found := false
	for _, s := range tr.spans {
		if s.name != "controller.launch" {
			continue
		}
		for _, a := range s.attrs {
			if a.Key == "job" && a.Value == job.ID {
				found = true
			}
		}
	}
	if !found {
		t.Error("launch span missing job attribute")
	}
}

func TestControllerFailedLaunchRecordsSpanError(t *testing.T) {
	tr := &stubTracer{}
	fake := backend.NewFake()
	fake.LaunchErr = backend.TransientLaunchErrorf("temporary backend unavailable")
	c := newControllerWith(t, fake, control.Options{Tracer: tr, Restart: lifecycle.RestartPolicy{MaxAttempts: 1}})
	if _, err := c.Submit(context.Background(), []byte(validSDKManifest), nil); err == nil {
		t.Fatal("expected launch failure")
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.spans) != 1 || len(tr.spans[0].errs) != 1 || !tr.spans[0].ended {
		t.Fatalf("failed launch should end its span with an error: %+v", tr.spans)
	}
}

func newControllerWith(t *testing.T, fake *backend.Fake, extra control.Options) *control.Controller {
	t.Helper()
	extra.Store = openStore(t)
	extra.Backend = fake
	extra.Image = "unused-default-image:test"
	extra.StopTimeout = time.Second
	if extra.Restart == (lifecycle.RestartPolicy{}) {
		extra.Restart = lifecycle.DefaultRestartPolicy()
	}
	return control.New(extra)
}
