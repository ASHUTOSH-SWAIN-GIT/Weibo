// Package api exposes the Controller over HTTP: submit, list, inspect,
// cancel, restart, and log/metric access for jobs. Live control-surface
// calls (/state, /metrics) are proxied to the job's container agent.
package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/store"
)

// Server adapts a Controller to an http.Handler.
type Server struct {
	ctrl   *control.Controller
	client *http.Client
}

// NewServer builds the API server.
func NewServer(ctrl *control.Controller) *Server {
	return &Server{ctrl: ctrl, client: &http.Client{Timeout: 5 * time.Second}}
}

// Handler returns the routed API. Method patterns give automatic 405s.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /jobs", s.submit)
	mux.HandleFunc("GET /jobs", s.list)
	mux.HandleFunc("GET /jobs/{id}", s.get)
	mux.HandleFunc("POST /jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("POST /jobs/{id}/restart", s.restart)
	mux.HandleFunc("POST /jobs/{id}/savepoint", s.savepoint)
	mux.HandleFunc("GET /jobs/{id}/logs", s.logs)
	mux.HandleFunc("GET /jobs/{id}/state", s.proxy("/state"))
	mux.HandleFunc("GET /jobs/{id}/metrics", s.proxy("/metrics"))
	return mux
}

// submitRequest is the JSON envelope for POST /jobs. A raw (non-JSON)
// body is treated as the workflow document itself, with no env.
type submitRequest struct {
	Workflow string            `json:"workflow"`
	Env      map[string]string `json:"env,omitempty"`
}

// jobDetail is the GET /jobs/{id} response.
type jobDetail struct {
	Job         *store.Job          `json:"job"`
	LatestRun   *store.Run          `json:"latestRun,omitempty"`
	Transitions []*store.Transition `json:"transitions,omitempty"`
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20)) // 4 MiB cap
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	var doc []byte
	var env map[string]string
	if isJSON(r) {
		var req submitRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		doc, env = []byte(req.Workflow), req.Env
	} else {
		doc = body
	}
	if len(doc) == 0 {
		writeErr(w, http.StatusBadRequest, "empty workflow")
		return
	}

	job, err := s.ctrl.Submit(r.Context(), doc, env)
	if err != nil {
		// A validation failure is a client error; a launch failure after a
		// valid spec is a server/infra error but the job is recorded.
		if job == nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"job": job, "warning": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.ctrl.ListJobs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.ctrl.GetJob(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	run, _ := s.ctrl.LatestRun(id)
	trans, _ := s.ctrl.Transitions(id)
	writeJSON(w, http.StatusOK, jobDetail{Job: job, LatestRun: run, Transitions: trans})
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	if err := s.ctrl.Cancel(r.Context(), r.PathValue("id")); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "cancelling"})
}

// restart optionally accepts JSON body {"savepoint": "<label>"} to resume
// from a savepoint instead of the last automatic checkpoint.
func (s *Server) restart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	label := savepointLabel(r)

	var job *store.Job
	var err error
	if label != "" {
		job, err = s.ctrl.RestartFromSavepoint(r.Context(), id, label)
	} else {
		job, err = s.ctrl.Restart(r.Context(), id)
	}
	if err != nil {
		if job == nil {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

// savepoint triggers a stop-with-savepoint. Label comes from ?label= or a
// JSON body {"label": "..."}.
func (s *Server) savepoint(w http.ResponseWriter, r *http.Request) {
	label := savepointLabel(r)
	if label == "" {
		writeErr(w, http.StatusBadRequest, "missing savepoint label (?label= or {\"label\":...})")
		return
	}
	if err := s.ctrl.Savepoint(r.Context(), r.PathValue("id"), label); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"savepoint": label, "status": "stopping"})
}

// savepointLabel reads a savepoint label from the ?label query param, then
// falls back to a JSON body {"label" | "savepoint": "..."}.
func savepointLabel(r *http.Request) string {
	if l := r.URL.Query().Get("label"); l != "" {
		return l
	}
	if r.Body == nil {
		return ""
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	if len(body) == 0 {
		return ""
	}
	var req struct {
		Label     string `json:"label"`
		Savepoint string `json:"savepoint"`
	}
	_ = json.Unmarshal(body, &req)
	if req.Label != "" {
		return req.Label
	}
	return req.Savepoint
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	tail := 200
	if t := r.URL.Query().Get("tail"); t != "" {
		if n, err := strconv.Atoi(t); err == nil {
			tail = n
		}
	}
	out, err := s.ctrl.Logs(r.Context(), r.PathValue("id"), tail)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, out)
}

// proxy forwards to a path on the job's live container control surface.
func (s *Server) proxy(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		addr, err := s.ctrl.ControlAddress(r.Context(), r.PathValue("id"))
		if err != nil {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		if addr == "" {
			writeErr(w, http.StatusServiceUnavailable, "job has no running control surface")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
		resp, err := s.client.Do(req)
		if err != nil {
			writeErr(w, http.StatusBadGateway, "control surface unreachable: "+err.Error())
			return
		}
		defer resp.Body.Close()
		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}
}

func isJSON(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/json")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
