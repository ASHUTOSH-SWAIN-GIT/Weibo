// Package api exposes the Controller over HTTP: submit, list, inspect,
// cancel, restart, and log/metric access for jobs. Live control-surface
// calls (/state, /metrics) are proxied to the job's container agent.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/ui"
)

// Server adapts a Controller to an http.Handler.
type Server struct {
	ctrl       *control.Controller
	client     *http.Client
	authConfig AuthConfig
	limit      *mutationLimiter
}

// Role is an API token scope. Read-only tokens may inspect jobs and cluster
// state; read-write tokens may also submit, cancel, restart, savepoint and
// delete jobs.
type Role string

const (
	RoleReadOnly  Role = "readonly"
	RoleReadWrite Role = "readwrite"
)

// AuthConfig configures bearer-token auth. Token preserves the original local
// plaintext-token mode. TokenSHA256 accepts one or more hex SHA-256 digests of
// raw bearer tokens; prefix a digest with "readonly:" or "readwrite:" to scope
// it. Unprefixed hashes are read-write for compatibility and rotation.
type AuthConfig struct {
	Token       string
	TokenSHA256 []string
}

type tokenHash struct {
	role Role
	sum  []byte
}

// NewServer builds the API server. An empty token leaves the API open;
// a non-empty token gates every route behind Authorization: Bearer <token>.
func NewServer(ctrl *control.Controller, token string) *Server {
	return NewServerWithAuth(ctrl, AuthConfig{Token: token})
}

// NewServerWithAuth builds the API server with plaintext and/or hashed tokens.
func NewServerWithAuth(ctrl *control.Controller, auth AuthConfig) *Server {
	auth.TokenSHA256 = normalizeTokenHashes(auth.TokenSHA256)
	return &Server{
		ctrl:       ctrl,
		client:     &http.Client{Timeout: 5 * time.Second},
		authConfig: auth,
		limit:      newMutationLimiter(30, time.Minute),
	}
}

// Handler returns the routed API. Method patterns give automatic 405s.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /{$}", ui.Index()) // dashboard at the app root
	mux.Handle("GET /logo.png", ui.Logo())
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /livez", s.livez)
	mux.HandleFunc("GET /readyz", s.readyz)
	// Controller-native Prometheus metrics (process, reconcile, launches,
	// inventory, API). Aggregate counters only — no job/run/container IDs,
	// so this is safe to scrape without auth, like /healthz.
	mux.Handle("GET /metrics", s.ctrl.Metrics().Handler())
	// Prometheus http_sd discovery for live job agents. Gated by the same
	// auth as the rest of the API: it exposes internal control addresses.
	mux.HandleFunc("GET /targets", s.targets)
	mux.HandleFunc("POST /auth", s.authCheck)
	mux.HandleFunc("GET /cluster", s.cluster)
	mux.HandleFunc("POST /validate", s.validate)
	mux.HandleFunc("POST /jobs", s.submit)
	mux.HandleFunc("GET /jobs", s.list)
	mux.HandleFunc("GET /jobs/{id}", s.get)
	mux.HandleFunc("DELETE /jobs/{id}", s.delete)
	mux.HandleFunc("POST /jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("POST /jobs/{id}/restart", s.restart)
	mux.HandleFunc("POST /jobs/{id}/savepoint", s.savepoint)
	mux.HandleFunc("GET /jobs/{id}/logs", s.logs)
	mux.HandleFunc("GET /jobs/{id}/state", s.proxy("/state"))
	mux.HandleFunc("GET /jobs/{id}/metrics", s.proxy("/metrics"))
	mux.HandleFunc("GET /jobs/{id}/describe", s.proxy("/describe"))
	mux.HandleFunc("GET /jobs/{id}/plan", s.proxy("/plan"))
	mux.HandleFunc("GET /jobs/{id}/history", s.history)
	mux.HandleFunc("GET /jobs/history", s.bulkHistory)
	mux.HandleFunc("GET /config", s.config)
	mux.HandleFunc("GET /jobs/{id}/diagnostics", s.diagnostics)
	mux.HandleFunc("GET /jobs/{id}/runs", s.runs)
	mux.HandleFunc("GET /jobs/{id}/runs/{runId}", s.runDetail)
	mux.HandleFunc("GET /jobs/{id}/runs/{runId}/logs", s.runLogs)
	mux.HandleFunc("GET /jobs/{id}/transitions", s.transitions)
	mux.HandleFunc("GET /jobs/{id}/logs/stream", s.logsStream)
	return s.auth(s.auditMutations(s.rateLimitMutations(s.instrument(mux))))
}

// instrument records per-request API metrics. The route label is the
// matched mux template (e.g. "GET /jobs/{id}"), never the raw path, so
// job IDs cannot explode cardinality.
func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		route := r.Pattern
		if route == "" {
			route = r.Method + " unknown"
		}
		s.ctrl.Metrics().ObserveAPI(r.Method, route, strconv.Itoa(rw.status), time.Since(start))
	})
}

// statusRecorder captures the status code for metrics. It forwards Flush
// so streaming handlers (SSE log follow) keep working behind the metrics
// middleware.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

type mutationLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	hits   map[string][]time.Time
}

func newMutationLimiter(limit int, window time.Duration) *mutationLimiter {
	return &mutationLimiter{limit: limit, window: window, hits: map[string][]time.Time{}}
}

func (l *mutationLimiter) Allow(key string) bool {
	now := time.Now()
	cutoff := now.Add(-l.window)
	l.mu.Lock()
	defer l.mu.Unlock()
	hits := l.hits[key]
	keep := hits[:0]
	for _, ts := range hits {
		if ts.After(cutoff) {
			keep = append(keep, ts)
		}
	}
	if len(keep) >= l.limit {
		l.hits[key] = keep
		return false
	}
	keep = append(keep, now)
	l.hits[key] = keep
	return true
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}
	return "unknown"
}

type authRoleKey struct{}

// auth wraps h with shared-bearer-token enforcement. With no token
// configured it is a pass-through (open API). Otherwise every request must
// carry Authorization: Bearer <token>, except the two public routes: the
// health check and the HTML shell (which must load so the browser can
// prompt for a token).
func (s *Server) auth(h http.Handler) http.Handler {
	if !s.authConfigured() {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if publicRoute(r) {
			h.ServeHTTP(w, r)
			return
		}
		role, ok := s.authenticate(r.Header.Get("Authorization"))
		if !ok {
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if role == RoleReadOnly && mutationRoute(r) {
			writeErr(w, http.StatusForbidden, "read-only token cannot mutate jobs")
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), authRoleKey{}, role))
		h.ServeHTTP(w, r)
	})
}

func (s *Server) authConfigured() bool {
	return s.authConfig.Token != "" || len(s.authConfig.TokenSHA256) > 0
}

func (s *Server) authenticate(header string) (Role, bool) {
	const prefix = "Bearer "
	token, ok := strings.CutPrefix(header, prefix)
	if !ok || token == "" {
		return "", false
	}
	if s.authConfig.Token != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.authConfig.Token)) == 1 {
		return RoleReadWrite, true
	}
	sum := sha256.Sum256([]byte(token))
	got := make([]byte, hex.EncodedLen(len(sum)))
	hex.Encode(got, sum[:])
	for _, raw := range s.authConfig.TokenSHA256 {
		role, want, ok := parseTokenHash(raw)
		if !ok {
			continue
		}
		if subtle.ConstantTimeCompare(got, want) == 1 {
			return role, true
		}
	}
	return "", false
}

func normalizeTokenHashes(in []string) []string {
	var out []string
	for _, part := range in {
		for _, item := range strings.Split(part, ",") {
			if item = strings.TrimSpace(item); item != "" {
				out = append(out, item)
			}
		}
	}
	return out
}

func parseTokenHash(raw string) (Role, []byte, bool) {
	role := RoleReadWrite
	body := strings.TrimSpace(raw)
	if before, after, ok := strings.Cut(body, ":"); ok {
		switch Role(strings.ToLower(strings.TrimSpace(before))) {
		case RoleReadOnly:
			role = RoleReadOnly
		case RoleReadWrite:
			role = RoleReadWrite
		default:
			return "", nil, false
		}
		body = strings.TrimSpace(after)
	}
	body = strings.ToLower(body)
	if len(body) != sha256.Size*2 {
		return "", nil, false
	}
	decoded, err := hex.DecodeString(body)
	if err != nil {
		return "", nil, false
	}
	return role, []byte(hex.EncodeToString(decoded)), true
}

// publicRoute reports whether r may bypass auth: the HTML shell at "/" and
// the health check, both GET-only.
func publicRoute(r *http.Request) bool {
	return r.Method == http.MethodGet && (r.URL.Path == "/" || r.URL.Path == "/healthz" || r.URL.Path == "/livez" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics")
}

func mutationRoute(r *http.Request) bool {
	switch r.Method {
	case http.MethodPost, http.MethodDelete, http.MethodPut, http.MethodPatch:
		return r.URL.Path != "/auth" && r.URL.Path != "/validate"
	default:
		return false
	}
}

func (s *Server) rateLimitMutations(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mutationRoute(r) && s.limit != nil && !s.limit.Allow(clientIP(r)) {
			writeErr(w, http.StatusTooManyRequests, "too many mutation requests")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) auditMutations(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !mutationRoute(r) {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		role := Role("anonymous")
		if v, ok := r.Context().Value(authRoleKey{}).(Role); ok && v != "" {
			role = v
		}
		route := r.Pattern
		if route == "" {
			route = r.Method + " unknown"
		}
		s.ctrl.AuditMutation(r.Method, route, r.PathValue("id"), string(role), clientIP(r), rw.status, time.Since(start))
	})
}

// authCheck returns 200 once a request reaches it — the auth middleware has
// already validated (or the API is open). It lets the UI verify a token
// before storing it, without listing jobs. The role field lets the
// dashboard disable mutation actions for read-only tokens (D9): "open"
// when no token is configured, otherwise the authenticated token's scope.
func (s *Server) authCheck(w http.ResponseWriter, r *http.Request) {
	role := "open"
	if s.authConfigured() {
		role = "unknown"
		if v, ok := r.Context().Value(authRoleKey{}).(Role); ok && v != "" {
			role = string(v)
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "role": role})
}

func (s *Server) cluster(w http.ResponseWriter, r *http.Request) {
	snap, err := s.ctrl.Cluster(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// submitRequest is the JSON envelope for POST /jobs. A raw (non-JSON)
// body is treated as the workflow document itself, with no env.
type submitRequest struct {
	Workflow string                     `json:"workflow"`
	Env      map[string]string          `json:"env,omitempty"`
	EnvRefs  map[string]store.SecretRef `json:"envRefs,omitempty"`
}

// jobDetail is the GET /jobs/{id} response.
type jobDetail struct {
	Job         *store.Job          `json:"job"`
	LatestRun   *store.Run          `json:"latestRun,omitempty"`
	Transitions []*store.Transition `json:"transitions,omitempty"`
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	s.readyz(w, r)
}

func (s *Server) livez(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if err := s.ctrl.Ready(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "reason": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// targets serves Prometheus http_sd discovery: one target per job with a
// reachable live control surface, so Prometheus can scrape agent /metrics
// directly. Unreachable jobs are skipped; an empty list encodes as [].
func (s *Server) targets(w http.ResponseWriter, r *http.Request) {
	targets, err := s.ctrl.DiscoveryTargets(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if targets == nil {
		targets = []control.DiscoveryTarget{}
	}
	writeJSON(w, http.StatusOK, targets)
}

// history serves one job's rolling metrics history (downsampled to at
// most ?points=N, default 120) for sparklines: raw cumulative counters
// plus checkpoint/lag/queue/error state per point; readers derive rates
// from counter deltas.
func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.ctrl.GetJob(id); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"jobId":   id,
		"points":  s.ctrl.History().Series(id, historyPoints(r, 120)),
		"samples": control.DefaultHistorySamples,
	})
}

// bulkHistory serves downsampled series for every job that has history
// (at most ?points=N each, default 30) — one request for the all-jobs
// view instead of one per job.
func (s *Server) bulkHistory(w http.ResponseWriter, r *http.Request) {
	points := historyPoints(r, 30)
	out := map[string]any{}
	for _, id := range s.ctrl.History().JobsWithHistory() {
		out[id] = s.ctrl.History().Series(id, points)
	}
	writeJSON(w, http.StatusOK, map[string]any{"histories": out})
}

// historyPoints parses ?points=N (def when absent); 0 means "full
// series", and anything unparsable falls back to def.
func historyPoints(r *http.Request, def int) int {
	if q := r.URL.Query().Get("points"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

// config exposes controller-level UI configuration: currently just the
// external Grafana base URL ("" when deep links are disabled).
func (s *Server) config(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"grafanaUrl": s.ctrl.GrafanaURL()})
}

// diagnostics serves the assembled "why is my job (un)healthy" view:
// failure classification, last activity, restart countdown, and latest
// checkpoint duration/size.
func (s *Server) diagnostics(w http.ResponseWriter, r *http.Request) {
	d, err := s.ctrl.Diagnostics(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, d)
}

// runs lists every recorded attempt for a job, newest first. Terminal
// history is retention-bounded; the live attempt is always present.
func (s *Server) runs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.ctrl.GetJob(id); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	runs, err := s.ctrl.Runs(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if runs == nil {
		runs = []*store.Run{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

// runDetail serves one attempt with its audit transitions and restart
// countdown.
func (s *Server) runDetail(w http.ResponseWriter, r *http.Request) {
	d, err := s.ctrl.RunDetail(r.PathValue("id"), r.PathValue("runId"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, d)
}

// runLogs serves one attempt's container logs. 404 for an unknown run,
// 410 when the container is gone (pruned by retention, or never
// recorded) — the run row itself remains for audit.
func (s *Server) runLogs(w http.ResponseWriter, r *http.Request) {
	out, status, err := s.ctrl.RunLogs(r.Context(), r.PathValue("id"), r.PathValue("runId"), logTail(r))
	if err != nil {
		writeErr(w, status, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, out)
}

// transitions serves the paged audit log, newest first:
// ?limit=N (default 50, max 200) and ?before=<id> (exclusive cursor).
// The response carries nextBefore (0 when exhausted) for the More button.
func (s *Server) transitions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.ctrl.GetJob(id); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	var before int64
	if q := r.URL.Query().Get("before"); q != "" {
		if n, err := strconv.ParseInt(q, 10, 64); err == nil && n > 0 {
			before = n
		}
	}
	limit := store.DefaultTransitionLimit
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}
	ts, err := s.ctrl.TransitionsPaged(id, before, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if ts == nil {
		ts = []*store.Transition{}
	}
	var nextBefore int64
	if len(ts) > 0 {
		nextBefore = ts[len(ts)-1].ID
	}
	writeJSON(w, http.StatusOK, map[string]any{"transitions": ts, "nextBefore": nextBefore})
}

// logsStream follows a job's latest container logs over server-sent
// events: an initial ?tail= burst, then only new output every 2s, plus
// heartbeat comments. It ends when the client disconnects. Deltas are
// capped per event so one chatty poll cannot balloon memory. Disabled for
// terminal jobs — there is nothing left to follow, and the container may
// already be gone.
func (s *Server) logsStream(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.ctrl.LatestRun(id)
	if err != nil || run == nil || run.ContainerID == "" {
		writeErr(w, http.StatusNotFound, "no container recorded for job")
		return
	}
	if lifecycle.Phase(run.Phase).Terminal() {
		writeErr(w, http.StatusConflict, "job is terminal; nothing to follow")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	send := func(text string) {
		if len(text) > 64<<10 {
			text = "[…truncated…]\n" + text[len(text)-64<<10:]
		}
		for _, line := range strings.Split(text, "\n") {
			_, _ = io.WriteString(w, "data: "+line+"\n")
		}
		_, _ = io.WriteString(w, "\n")
		flusher.Flush()
	}
	ctx := r.Context()
	last, err := s.ctrl.Logs(ctx, id, logTail(r))
	if err != nil {
		send("log stream unavailable: " + err.Error())
		return
	}
	send(last)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Bounded, not unbounded (tail=0): an unbounded fetch re-reads
			// and re-buffers the container's entire accumulated log on
			// every tick for the life of the connection. maxLogTail keeps
			// this call's cost flat regardless of how long the job runs;
			// once the log outgrows it, the "resend the window" branch
			// below (len(cur)!=len(last)) kicks in instead of the plain
			// suffix diff, which is still correct, just coarser.
			cur, err := s.ctrl.Logs(ctx, id, maxLogTail)
			if err != nil {
				_, _ = io.WriteString(w, ": backend error: "+singleLine(err.Error())+"\n\n")
				flusher.Flush()
				continue
			}
			switch {
			case len(cur) > len(last) && strings.HasPrefix(cur, last):
				send(cur[len(last):])
			case len(cur) != len(last):
				// Log rotated or container replaced: resend the window.
				send(cur)
			default:
				_, _ = io.WriteString(w, ": ping\n\n")
				flusher.Flush()
			}
			last = cur
		}
	}
}

func singleLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// maxLogTail bounds every log read this server issues to the backend, so
// no single call (an explicit ?tail= or a periodic stream refetch) can
// force an unbounded read of a container's full accumulated log.
const maxLogTail = 5000

// logTail parses ?tail=N for the log endpoints (default 200); values < 0
// mean "all".
func logTail(r *http.Request) int {
	tail := 200
	if t := r.URL.Query().Get("tail"); t != "" {
		if n, err := strconv.Atoi(t); err == nil {
			tail = n
		}
	}
	if tail < 0 {
		return 0
	}
	if tail > maxLogTail {
		return maxLogTail
	}
	return tail
}

const (
	maxWorkflowBody  = 4 << 20
	maxSavepointBody = 1 << 16
)

// readWorkflow extracts a workflow doc (+ optional env) from a request:
// a JSON envelope {"workflow","env"} or a raw workflow body.
func readWorkflow(r *http.Request) (doc []byte, env map[string]string, refs map[string]store.SecretRef, err error) {
	body, err := readLimited(r.Body, maxWorkflowBody)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read body: %w", err)
	}
	if isJSON(r) {
		var req submitRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, nil, nil, fmt.Errorf("invalid JSON: %w", err)
		}
		return []byte(req.Workflow), req.Env, req.EnvRefs, nil
	}
	return body, nil, nil, nil
}

// validate is the dry-run preview: compile without launching, return the
// name, delivery guarantee, and graph the submit would produce.
func (s *Server) validate(w http.ResponseWriter, r *http.Request) {
	doc, env, _, err := readWorkflow(r)
	if err != nil {
		writeErr(w, statusForError(err), err.Error())
		return
	}
	if len(doc) == 0 {
		writeErr(w, http.StatusBadRequest, "empty workflow")
		return
	}
	name, delivery, graph, err := s.ctrl.Validate(doc, env)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"name": name, "delivery": delivery, "graph": graph,
	})
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	doc, env, refs, err := readWorkflow(r)
	if err != nil {
		writeErr(w, statusForError(err), err.Error())
		return
	}
	if len(doc) == 0 {
		writeErr(w, http.StatusBadRequest, "empty workflow")
		return
	}

	job, err := s.ctrl.SubmitWithSecretRefs(r.Context(), doc, env, refs)
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

// jobListItem is a job plus its latest-run phase, for the list view.
type jobListItem struct {
	*store.Job
	Phase string `json:"phase"`
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.ctrl.ListJobs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	items := make([]jobListItem, len(jobs))
	for i, j := range jobs {
		phase := "—"
		if run, _ := s.ctrl.LatestRun(j.ID); run != nil {
			phase = run.Phase
		}
		items[i] = jobListItem{Job: j, Phase: phase}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": items})
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

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	opts := control.DeleteOptions{DeleteData: r.URL.Query().Get("deleteData") == "true"}
	if err := s.ctrl.DeleteWithOptions(r.Context(), r.PathValue("id"), opts); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "deleted"})
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
	label, err := savepointLabel(r)
	if err != nil {
		writeErr(w, statusForError(err), err.Error())
		return
	}

	var job *store.Job
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
	label, err := savepointLabel(r)
	if err != nil {
		writeErr(w, statusForError(err), err.Error())
		return
	}
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
func savepointLabel(r *http.Request) (string, error) {
	if l := r.URL.Query().Get("label"); l != "" {
		return l, nil
	}
	if r.Body == nil {
		return "", nil
	}
	body, err := readLimited(r.Body, maxSavepointBody)
	if err != nil {
		return "", err
	}
	if len(body) == 0 {
		return "", nil
	}
	var req struct {
		Label     string `json:"label"`
		Savepoint string `json:"savepoint"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return "", err
	}
	if req.Label != "" {
		return req.Label, nil
	}
	return req.Savepoint, nil
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	tail := logTail(r)
	out, err := s.ctrl.Logs(r.Context(), r.PathValue("id"), tail)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, out)
}

var errBodyTooLarge = errors.New("body too large")

func readLimited(r io.Reader, max int64) ([]byte, error) {
	lr := io.LimitReader(r, max+1)
	body, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > max {
		return nil, errBodyTooLarge
	}
	return body, nil
}

// proxy forwards to a path on the job's live container control surface.
func (s *Server) proxy(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		// ControlAddress returns ("", nil) both for an unknown job and for a
		// real job with no current control surface; check the job exists
		// first so a bogus ID 404s instead of the misleading 503 below.
		if _, err := s.ctrl.GetJob(id); err != nil {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		addr, err := s.ctrl.ControlAddress(r.Context(), id)
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

func statusForError(err error) int {
	if errors.Is(err, errBodyTooLarge) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusBadRequest
}
