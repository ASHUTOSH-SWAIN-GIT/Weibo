package trace_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	telemetry "github.com/ASHUTOSH-SWAIN-GIT/weibo/telemetry/trace"
)

func TestEmptyEndpointIsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	p, shutdown, err := telemetry.Configure(context.Background(), telemetry.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if shutdown != nil {
		t.Error("noop provider should have nil shutdown")
	}
	ctx, span := p.Start(context.Background(), "op", telemetry.String("k", "v"))
	span.RecordError(errors.New("x"))
	span.SetAttributes(telemetry.Int("n", 1))
	span.End()
	if span.TraceID() != "" || span.SpanID() != "" {
		t.Error("noop span must carry no IDs")
	}
	_ = ctx
}

func TestOTLPExport(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	p, shutdown, err := telemetry.Configure(ctx, telemetry.Options{
		Endpoint:    srv.URL,
		Insecure:    true,
		ServiceName: "weibo-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	spanCtx, span := p.Start(ctx, "controller.launch",
		telemetry.String("job", "j1"), telemetry.Int("attempt", 2), telemetry.Bool("ok", true))
	span.RecordError(errors.New("boom"))
	span.End()
	_ = spanCtx

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) == 0 {
		t.Fatal("collector received no export")
	}
	total := 0
	for _, b := range bodies {
		total += len(b)
	}
	if total == 0 {
		t.Error("export body was empty")
	}
}

func TestParseSampleRatio(t *testing.T) {
	if got := telemetry.ParseSampleRatio("0.25"); got != 0.25 {
		t.Errorf("got %v", got)
	}
	for _, bad := range []string{"", "0", "-1", "2", "nan", "abc"} {
		if got := telemetry.ParseSampleRatio(bad); got != 1 {
			t.Errorf("ParseSampleRatio(%q)=%v, want 1", bad, got)
		}
	}
}
