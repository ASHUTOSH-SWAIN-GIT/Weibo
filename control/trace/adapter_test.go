package trace_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	wtrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
	teltrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/telemetry/trace"
	ctrace "github.com/ASHUTOSH-SWAIN-GIT/weibo/control/trace"
)

func TestAdaptNilIsNoop(t *testing.T) {
	tr := ctrace.Adapt(nil)
	ctx, span := tr.Start(context.Background(), "op", wtrace.String("k", "v"))
	span.End()
	if span.TraceID() != "" {
		t.Error("nil provider must adapt to no-op")
	}
	_ = ctx
}

func TestAdaptForwardsToOTel(t *testing.T) {
	var mu sync.Mutex
	var got int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		if len(body) > 0 {
			got++
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prov, shutdown, err := teltrace.Configure(ctx, teltrace.Options{
		Endpoint: srv.URL, Insecure: true, ServiceName: "weibo-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	tr := ctrace.Adapt(prov)
	sctx, span := tr.Start(ctx, "controller.launch",
		wtrace.String("job", "j1"), wtrace.Int("attempt", 2), wtrace.Bool("ok", true))
	span.SetAttributes(wtrace.Int64("n", 3))
	if span.TraceID() == "" || span.SpanID() == "" {
		t.Fatal("adapted span should carry real IDs")
	}
	if _, _, ok := wtrace.IDs(sctx); !ok {
		t.Fatal("adapted span should propagate for log correlation")
	}
	span.End()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got == 0 {
		t.Error("collector received no spans")
	}
}
