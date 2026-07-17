package jobagent_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/jobagent"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

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
	env := mailer.NewEnv().
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
	env := mailer.NewEnv().FromSource(src).ToSink(sink.NewBlackholeSink())
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

// A misconfigured env (no sink) fails fast; the agent records it.
func TestAgent_Failed(t *testing.T) {
	env := mailer.NewEnv() // no source and no sink configured

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

func TestAgent_HTTPSurface(t *testing.T) {
	env := mailer.NewEnv().
		FromSource(source.FromSlices([]string{"a"}, []string{"1"})).
		ToSink(sink.NewBlackholeSink())
	srv := httptest.NewServer(jobagent.New(env).Handler())
	defer srv.Close()

	cases := []struct {
		method, path string
		want         int
	}{
		{"GET", "/healthz", 200},
		{"GET", "/state", 200},
		{"GET", "/describe", 200},
		{"GET", "/metrics", 200},
		{"POST", "/savepoint", 501},
		{"POST", "/state", 405}, // wrong method
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
