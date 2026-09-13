package checkpoint

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
)

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
	tid   string
	sid   string
}

func (t *stubTracer) Start(ctx context.Context, name string, attrs ...trace.Attribute) (context.Context, trace.Span) {
	s := &stubSpan{name: name, tid: "trace-1", sid: "span-1"}
	s.attrs = append(s.attrs, attrs...)
	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.mu.Unlock()
	return trace.ContextWithSpan(ctx, s), s
}

func (s *stubSpan) End()                               { s.ended = true }
func (s *stubSpan) RecordError(err error)              { s.errs = append(s.errs, err) }
func (s *stubSpan) SetAttributes(a ...trace.Attribute) { s.attrs = append(s.attrs, a...) }
func (s *stubSpan) TraceID() string                    { return s.tid }
func (s *stubSpan) SpanID() string                     { return s.sid }

// A full finalize records one ended, error-free span carrying the
// checkpoint ID.
func TestCoordinator_FinalizeSpan(t *testing.T) {
	tr := &stubTracer{}
	c := NewCoordinator(NewFileStorage(t.TempDir()), "txn")
	c.Tracer = tr
	c.CommitSink = func(context.Context, string) error { return nil }
	c.AbortSink = func(context.Context, string) error { return nil }
	c.Start(context.Background())
	defer c.Stop()

	c.OnBarrierInjected("cp-1", []byte(`{}`))
	c.OnStateSnapshot("cp-1", map[string][]byte{"op-0": []byte("s")}, nil)
	c.OnSinkPrepared("cp-1", nil)

	deadline := time.Now().Add(5 * time.Second)
	for {
		tr.mu.Lock()
		done := len(tr.spans) > 0 && tr.spans[0].ended
		tr.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("finalize span never ended")
		}
		time.Sleep(5 * time.Millisecond)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	s := tr.spans[0]
	if s.name != "checkpoint.finalize" {
		t.Errorf("span name=%q", s.name)
	}
	if len(s.errs) != 0 {
		t.Errorf("span errors=%v", s.errs)
	}
	found := false
	for _, a := range s.attrs {
		if a.Key == "checkpoint" && a.Value == "cp-1" {
			found = true
		}
	}
	if !found {
		t.Errorf("span attrs=%+v", s.attrs)
	}
}

// TestCoordinator_StopRaceWithOnSinkPrepared stresses the window where a
// sink's OnSinkPrepared send races Stop closing the events channel. Before
// the sendWg guard this panicked with "send on closed channel" (and the race
// detector flagged the halted/close ordering). Run with -race.
func TestCoordinator_StopRaceWithOnSinkPrepared(t *testing.T) {
	for iter := 0; iter < 40; iter++ {
		c := NewCoordinator(NewFileStorage(t.TempDir()), "txn")
		c.CommitSink = func(context.Context, string) error { return nil }
		c.AbortSink = func(context.Context, string) error { return nil }
		c.Start(context.Background())

		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				c.OnSinkPrepared(fmt.Sprintf("cp-%d", i), nil)
			}(i)
		}
		// Stop concurrently with the in-flight prepares.
		go c.Stop()

		wg.Wait()
		c.Stop() // idempotent; also blocks until the first Stop finished
	}
}
