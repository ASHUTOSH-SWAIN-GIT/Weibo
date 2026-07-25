package jobagent

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler returns the agent's HTTP surface. Routes use Go 1.22+ method
// patterns so wrong-method requests get 405 automatically.
//
//	GET  /healthz    liveness + current phase
//	GET  /state      lifecycle snapshot (JSON State)
//	GET  /describe   pipeline topology (env.DescribeJSON)
//	GET  /metrics    Prometheus exposition
//	POST /cancel     request graceful shutdown
//	POST /savepoint  stop-with-savepoint (?label=<name>)
func (a *Agent) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "ok",
			"phase":  a.State().Phase,
		})
	})

	mux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, a.State())
	})

	mux.HandleFunc("GET /describe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(a.DescribeJSON()))
	})

	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))

	mux.HandleFunc("POST /cancel", func(w http.ResponseWriter, r *http.Request) {
		a.Cancel()
		writeJSON(w, http.StatusAccepted, map[string]any{
			"phase": a.State().Phase,
		})
	})

	mux.HandleFunc("POST /savepoint", func(w http.ResponseWriter, r *http.Request) {
		label := r.URL.Query().Get("label")
		if label == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing ?label"})
			return
		}
		if !a.RequestSavepoint(label) {
			writeJSON(w, http.StatusConflict, map[string]any{"error": "job is not running"})
			return
		}
		// Stop-with-savepoint: the job now drains and writes its final
		// checkpoint; the runner promotes it to the named savepoint as the
		// process winds down.
		writeJSON(w, http.StatusAccepted, map[string]any{"label": label, "phase": a.State().Phase})
	})

	return mux
}

// Serve runs the agent's HTTP server on addr until ctx is cancelled, then
// shuts it down gracefully. It blocks; run it in its own goroutine
// alongside Run. Returns nil on a clean ctx-driven shutdown.
func (a *Agent) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: a.Handler()}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
