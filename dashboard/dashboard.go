package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"mailer"
)

//go:embed static/*
var staticFS embed.FS

// upgrader is used to promote HTTP connections to WebSocket connections.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Server is the dashboard HTTP server. It serves the pipeline dashboard
// UI and a WebSocket endpoint for live status updates.
//
// Create one with NewServer, then call Start() to run it in a goroutine:
//
//	dash := dashboard.NewServer(env, ":8080")
//	go dash.Start()
type Server struct {
	env    *mailer.StreamExecutionEnv
	addr   string
	server *http.Server

	// status tracks the pipeline runtime state.
	mu      sync.Mutex
	status  Status
	started time.Time
}

// Status is pushed to the dashboard via WebSocket.
type Status struct {
	Running    bool   `json:"running"`
	Uptime     string `json:"uptime"`
	RecordsIn  int64  `json:"records_in"`
	RecordsOut int64  `json:"records_out"`
	Error      string `json:"error,omitempty"`
}

// NewServer creates a dashboard server that serves the pipeline UI
// and live status updates. It does not start the HTTP server — call
// Start() to begin serving.
//
// The env should be configured (FromSource + ToSink) before creating
// the server so the dashboard can display the full pipeline graph.
func NewServer(env *mailer.StreamExecutionEnv, addr string) *Server {
	return &Server{
		env:     env,
		addr:    addr,
		started: time.Now(),
	}
}

// Start launches the HTTP server and blocks until it is shut down.
// Typically called in a goroutine alongside env.Execute().
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Serve embedded static files.
	mux.HandleFunc("/", s.serveIndex)
	mux.Handle("/static/", http.FileServer(http.FS(staticFS)))

	// API endpoints.
	mux.HandleFunc("/api/pipeline", s.handlePipeline)
	mux.HandleFunc("/ws", s.handleWebSocket)

	s.server = &http.Server{Addr: s.addr, Handler: mux}

	fmt.Printf("mailer/dashboard: serving on http://localhost%s\n", s.addr)
	return s.server.ListenAndServe()
}

// Shutdown gracefully stops the dashboard server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// SetRunning updates the pipeline status. Called by the user or the
// execution environment when the pipeline starts or stops.
func (s *Server) SetRunning(running bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status.Running = running
}

// SetError records a pipeline error in the status.
func (s *Server) SetError(err string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status.Error = err
}

// UpdateCounts updates the record counters. Called by the pipeline
// to report throughput.
func (s *Server) UpdateCounts(in, out int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status.RecordsIn = in
	s.status.RecordsOut = out
}

// serveIndex serves the embedded index.html at the root path.
func (s *Server) serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, "dashboard: index.html not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}

// handlePipeline returns the pipeline structure as JSON.
func (s *Server) handlePipeline(w http.ResponseWriter, r *http.Request) {
	info := s.env.Describe()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

// handleWebSocket upgrades to a WebSocket connection and pushes status
// updates every second.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		s.mu.Lock()
		status := s.status
		status.Uptime = time.Since(s.started).Round(time.Second).String()
		s.mu.Unlock()

		if err := conn.WriteJSON(status); err != nil {
			return
		}

		<-ticker.C
	}
}
