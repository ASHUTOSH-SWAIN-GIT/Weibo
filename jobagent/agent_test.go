package jobagent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/jobagent"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// syncBuffer is a goroutine-safe bytes.Buffer for log capture.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// stubTracer records spans for observability assertions.
type stubTracer struct {
	mu    sync.Mutex
	spans []*stubSpan
}

type stubSpan struct {
	name  string
	attrs []trace.Attribute
	ended bool
	errs  []error
}

func (t *stubTracer) Start(ctx context.Context, name string, attrs ...trace.Attribute) (context.Context, trace.Span) {
	s := &stubSpan{name: name}
	s.attrs = append(s.attrs, attrs...)
	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.mu.Unlock()
	return trace.ContextWithSpan(ctx, s), s
}

func (s *stubSpan) End()                               { s.ended = true }
func (s *stubSpan) RecordError(err error)              { s.errs = append(s.errs, err) }
func (s *stubSpan) SetAttributes(a ...trace.Attribute) { s.attrs = append(s.attrs, a...) }
func (s *stubSpan) TraceID() string                    { return "" }
func (s *stubSpan) SpanID() string                     { return "" }

// blockingSource emits a few records, signals that it is live, then
// blocks until the context is cancelled. It keeps a job in the Running
// phase so tests can observe /state and exercise graceful cancel.
type blockingSource struct{ live chan struct{} }

func (s *blockingSource) Run(ctx context.Context, out chan<- types.Record) error {
	for i := range 3 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- types.Record{Key: []byte("k"), Value: []byte("v"), Offset: int64(i)}:
		}
	}
	close(s.live) // records are flowing; the job is running
	<-ctx.Done()
	return ctx.Err()
}

// A bounded job runs to completion on its own; terminal phase is Finished.
func TestAgent_RunToCompletion(t *testing.T) {
	env := weibo.NewEnv().
		FromSource(source.FromSlices([]string{"a", "b"}, []string{"1", "2"})).
		ToSink(sink.NewBlackholeSink())

	a := jobagent.New(env)
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := a.State().Phase; got != jobagent.PhaseFinished {
		t.Fatalf("terminal phase: got %q, want finished", got)
	}
}

// The gate: observe Running via /state mid-run, then POST /cancel and
// confirm the job drains to a clean terminal phase.
func TestAgent_StateAndCancel(t *testing.T) {
	src := &blockingSource{live: make(chan struct{})}
	env := weibo.NewEnv().FromSource(src).ToSink(sink.NewBlackholeSink())
	a := jobagent.New(env)

	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()

	// Wait until the source reports records flowing, then /state must
	// show Running.
	select {
	case <-src.live:
	case <-time.After(5 * time.Second):
		t.Fatal("source never went live")
	}
	if st := getState(t, srv.URL); st.Phase != jobagent.PhaseRunning {
		t.Fatalf("mid-run phase: got %q, want running", st.Phase)
	}

	// POST /cancel triggers graceful shutdown.
	resp, err := http.Post(srv.URL+"/cancel", "", nil)
	if err != nil {
		t.Fatalf("POST /cancel: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel status: got %d, want 202", resp.StatusCode)
	}

	select {
	case err := <-done:
		if err != nil && !isCanceled(err) {
			t.Fatalf("Run after cancel returned unexpected error: %v", err)
		}
	case <-time.After(35 * time.Second): // shutdownTimeout is 30s
		t.Fatal("job did not drain after cancel")
	}
	if got := a.State().Phase; got != jobagent.PhaseFinished {
		t.Fatalf("phase after cancel: got %q, want finished", got)
	}
}

// POST /savepoint?label triggers a stop-with-savepoint: the job drains
// and the label is recorded for the runner to promote.
func TestAgent_SavepointRequest(t *testing.T) {
	src := &blockingSource{live: make(chan struct{})}
	env := weibo.NewEnv().FromSource(src).ToSink(sink.NewBlackholeSink())
	a := jobagent.New(env)

	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	select {
	case <-src.live:
	case <-time.After(5 * time.Second):
		t.Fatal("source never went live")
	}

	resp, err := http.Post(srv.URL+"/savepoint?label=before-upgrade", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("savepoint: got %d, want 202", resp.StatusCode)
	}

	select {
	case <-done:
	case <-time.After(35 * time.Second):
		t.Fatal("job did not drain after savepoint request")
	}
	if label, ok := a.SavepointRequest(); !ok || label != "before-upgrade" {
		t.Fatalf("savepoint request: label=%q ok=%v", label, ok)
	}
	if a.State().Phase != jobagent.PhaseFinished {
		t.Errorf("phase after savepoint: %q", a.State().Phase)
	}
}

// A checkpointed run records duration and inline size on every completed
// checkpoint, in both the agent state and the served /state.
func TestAgent_CheckpointReport(t *testing.T) {
	env := weibo.NewEnv().
		FromSource(source.FromSlices([]string{"a", "b", "c"}, []string{"1", "2", "3"})).
		ToSink(sink.NewBlackholeSink()).
		WithCheckpointing(10*time.Millisecond, checkpoint.NewFileStorage(t.TempDir()))

	a := jobagent.New(env)
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	st := a.State()
	if len(st.Checkpoints) == 0 {
		t.Fatal("expected completed checkpoints")
	}
	for _, cp := range st.Checkpoints {
		if cp.DurationMs < 0 {
			t.Errorf("checkpoint %s has negative duration: %d", cp.ID, cp.DurationMs)
		}
		if cp.SizeBytes <= 0 {
			t.Errorf("checkpoint %s has no inline size: %d", cp.ID, cp.SizeBytes)
		}
	}
}

// Structured logging and tracing ride the whole run: the agent logs
// start/finish, the engine checkpoint save is spanned, and the agent's
// job-run span ends cleanly.
func TestAgent_StructuredLoggingAndTracing(t *testing.T) {
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	tr := &stubTracer{}
	env := weibo.NewEnv().
		FromSource(source.FromSlices([]string{"a", "b"}, []string{"1", "2"})).
		ToSink(sink.NewBlackholeSink()).
		WithCheckpointing(10*time.Millisecond, checkpoint.NewFileStorage(t.TempDir())).
		WithLogger(logger).
		WithTracer(tr)

	a := jobagent.New(env).SetLogger(logger).SetTracer(tr)
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := logs.String()
	for _, want := range []string{"job run started", "job run finished", "checkpoint="} {
		if !strings.Contains(out, want) {
			t.Errorf("logs missing %q:\n%s", want, out)
		}
	}
	names := map[string]bool{}
	ended := 0
	tr.mu.Lock()
	for _, s := range tr.spans {
		names[s.name] = true
		if s.ended {
			ended++
		}
		if len(s.errs) != 0 {
			t.Errorf("span %s errors: %v", s.name, s.errs)
		}
	}
	total := len(tr.spans)
	tr.mu.Unlock()
	if total == 0 || ended != total {
		t.Errorf("spans=%d ended=%d", total, ended)
	}
	for _, want := range []string{"agent.run", "checkpoint.save"} {
		if !names[want] {
			t.Errorf("missing span %q (have %v)", want, names)
		}
	}
}

// A misconfigured env (no sink) fails fast; the agent records it.
// checkpointableSource is a minimal source.CheckpointSource: enough for
// engine.go's coordinated (exactly-once) mode to accept it, emitting one
// record per checkpoint tick so a barrier keeps finding new work.
type checkpointableSource struct{ n int }

func (s *checkpointableSource) Run(ctx context.Context, out chan<- types.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- types.Record{Key: []byte("k"), Value: []byte("v"), Offset: int64(s.n)}:
			s.n++
			time.Sleep(time.Millisecond)
		}
	}
}
func (s *checkpointableSource) CheckpointOffset() ([]byte, error) { return []byte("0"), nil }
func (s *checkpointableSource) RestoreOffset([]byte) error        { return nil }

// fatalCommitSink is a minimal sink.CheckpointedSink whose Commit always
// fails with context.Canceled — simulating the exact shape of error found
// live: a coordinator-internal operation returns context.Canceled because
// an unrelated internal force-unwind already cancelled its context, NOT
// because anything asked the job to stop.
type fatalCommitSink struct{ onPrepared func(id string, err error) }

func (s *fatalCommitSink) Write(ctx context.Context, in <-chan types.Record) error {
	for r := range in {
		if r.IsBarrier {
			s.onPrepared(r.CheckpointID, nil)
		}
	}
	return nil
}
func (s *fatalCommitSink) SetOnPrepared(fn func(id string, err error)) { s.onPrepared = fn }
func (s *fatalCommitSink) Commit(ctx context.Context, id string) error { return context.Canceled }
func (s *fatalCommitSink) Abort(ctx context.Context, id string) error  { return nil }
func (s *fatalCommitSink) WasCommitted(ctx context.Context, id string) (bool, error) {
	return false, nil
}
func (s *fatalCommitSink) TransactionalID() string { return "fatal-commit-test" }

// TestAgent_ContextCanceledErrorWithoutShutdownRequestIsAFailure guards
// against the exact bug found live against a real Kafka outage: Execute
// can return a context.Canceled-wrapped error from an internal force-unwind
// that nothing outside the job ever requested. Run must not treat that as
// PhaseFinished just because the error "Is" context.Canceled — only an
// actual shutdown request (ctx cancelled, or Cancel() called) earns that.
// The ctx passed to Run here is never cancelled and Cancel() is never
// called, so this run must land on PhaseFailed.
func TestAgent_ContextCanceledErrorWithoutShutdownRequestIsAFailure(t *testing.T) {
	env := weibo.NewEnv().
		WithCheckpointing(5*time.Millisecond, checkpoint.NewFileStorage(t.TempDir())).
		WithShutdownTimeout(200 * time.Millisecond)
	env.FromSource(&checkpointableSource{}).ToSink(&fatalCommitSink{})

	a := jobagent.New(env)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := a.Run(ctx)
	if err == nil {
		t.Fatal("expected Run to report the fatal commit error, got nil")
	}
	if got := a.State().Phase; got != jobagent.PhaseFailed {
		t.Fatalf("phase: got %q, want failed (this job was never asked to stop)", got)
	}
	if a.State().LastError == "" {
		t.Fatal("expected LastError to be set")
	}
}

func TestAgent_Failed(t *testing.T) {
	env := weibo.NewEnv() // no source and no sink configured

	a := jobagent.New(env)
	if err := a.Run(context.Background()); err == nil {
		t.Fatal("expected Run error for missing sink")
	}
	st := a.State()
	if st.Phase != jobagent.PhaseFailed {
		t.Fatalf("phase: got %q, want failed", st.Phase)
	}
	if st.LastError == "" {
		t.Fatal("expected LastError to be set on failure")
	}
}

// MarkSavepointFailed must flip an already-finished run to Failed: a
// savepoint promotion that fails after Run returns cleanly is otherwise
// indistinguishable from a real clean stop, which is exactly the bug found
// live (see sdk.Serve's savepoint promotion step).
func TestAgent_MarkSavepointFailed(t *testing.T) {
	env := weibo.NewEnv().
		FromSource(source.FromSlices([]string{"a"}, []string{"1"})).
		ToSink(sink.NewBlackholeSink())
	a := jobagent.New(env)
	if err := a.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if a.State().Phase != jobagent.PhaseFinished {
		t.Fatalf("phase after Run: got %q, want finished", a.State().Phase)
	}

	a.MarkSavepointFailed("before-upgrade", errors.New("blobstore unreachable"))

	st := a.State()
	if st.Phase != jobagent.PhaseFailed {
		t.Fatalf("phase after MarkSavepointFailed: got %q, want failed", st.Phase)
	}
	if st.LastError == "" {
		t.Fatal("expected LastError to be set")
	}
}

func TestAgent_HTTPSurface(t *testing.T) {
	env := weibo.NewEnv().
		FromSource(source.FromSlices([]string{"a"}, []string{"1"})).
		ToSink(sink.NewBlackholeSink())
	srv := httptest.NewServer(jobagent.New(env).Handler())
	defer srv.Close()

	cases := []struct {
		method, path string
		want         int
	}{
		{"GET", "/livez", 200},
		{"GET", "/readyz", 503}, // agent has not started yet
		{"GET", "/healthz", 200},
		{"GET", "/state", 200},
		{"GET", "/describe", 200},
		{"GET", "/metrics", 200},
		{"POST", "/savepoint", 400}, // missing ?label
		{"POST", "/state", 405},     // wrong method
	}
	for _, c := range cases {
		req, _ := http.NewRequest(c.method, srv.URL+c.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", c.method, c.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s %s: got %d, want %d", c.method, c.path, resp.StatusCode, c.want)
		}
	}
}

func getState(t *testing.T, base string) jobagent.State {
	t.Helper()
	resp, err := http.Get(base + "/state")
	if err != nil {
		t.Fatalf("GET /state: %v", err)
	}
	defer resp.Body.Close()
	var st jobagent.State
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	return st
}

func isCanceled(err error) bool {
	return err == context.Canceled || err.Error() == context.Canceled.Error()
}
