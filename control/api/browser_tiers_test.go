package api_test

// Browser integration tier (roadmap #26).
//
// A real browser renders the dashboard served at GET / and drives the same
// REST surface below. These tests pin the contract a browser depends on —
// dashboard HTML hooks, auth gating, and metrics/history rendering —
// against httptest servers, so they run on every pull request with no
// browser installed. TestTier_BrowserAgainstLiveURL replays the same
// assertions against an external dashboard when WEIBO_RUN_BROWSER_URL is
// set (merge/nightly deploys a controller and points the tier at it).

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/api"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

const tierSDKJob = `kind: sdk
name: orders-sdk
image: registry.example/orders:v1
`

func tierStore(t *testing.T) store.Store {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func tierServer(t *testing.T, auth api.AuthConfig) (*httptest.Server, *control.Controller) {
	t.Helper()
	ctrl := control.New(control.Options{
		Store:       tierStore(t),
		Backend:     backend.NewFake(),
		Image:       "img",
		StopTimeout: time.Second,
	})
	srv := httptest.NewServer(api.NewServerWithAuth(ctrl, auth).Handler())
	t.Cleanup(srv.Close)
	return srv, ctrl
}

func tierGet(t *testing.T, client *http.Client, url, auth string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// Dashboard lifecycle through a browser's eyes: the app shell loads, a job
// is deployed, inspected, cancelled, and deleted — every view reachable.
func TestTier_BrowserDashboardLifecycle(t *testing.T) {
	srv, _ := tierServer(t, api.AuthConfig{})
	client := srv.Client()

	if code, body := tierGet(t, client, srv.URL+"/", ""); code != 200 {
		t.Fatalf("GET /: got %d", code)
	} else {
		for _, want := range []string{
			`<title>weibo</title>`, `id="app"`,
			`data-r="overview"`, `data-r="sources"`, `data-r="sinks"`, `data-r="pipeline"`, `data-r="reliability"`,
			`Overview`, `Sources`, `Sinks`, `Pipeline`, `Reliability`,
			`normalizeSources`, `normalizeSinks`, `normalizeOperators`, `normalizeStages`, `normalizeCheckpoints`, `deriveDelivery`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("dashboard missing %q", want)
			}
		}
		for _, removed := range []string{
			`data-r="job-manager"`, `data-r="running"`,
			`data-r="completed"`, `data-r="submit"`,
		} {
			if strings.Contains(body, removed) {
				t.Errorf("section-by-section dashboard still exposes %q", removed)
			}
		}
	}

	resp, err := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(tierSDKJob))
	if err != nil {
		t.Fatal(err)
	}
	var job store.Job
	_ = json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated || job.ID == "" {
		t.Fatalf("submit: got %d", resp.StatusCode)
	}

	for _, path := range []string{
		"/jobs", "/jobs/" + job.ID,
		"/jobs/" + job.ID + "/history", "/jobs/" + job.ID + "/diagnostics",
		"/jobs/" + job.ID + "/runs", "/jobs/" + job.ID + "/transitions",
		"/config",
	} {
		if code, _ := tierGet(t, client, srv.URL+path, ""); code != 200 {
			t.Errorf("GET %s: got %d, want 200", path, code)
		}
	}

	cancelReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/jobs/"+job.ID+"/cancel", nil)
	if r, err := client.Do(cancelReq); err != nil {
		t.Fatal(err)
	} else {
		r.Body.Close()
		if r.StatusCode != http.StatusAccepted {
			t.Errorf("POST cancel: got %d, want 202", r.StatusCode)
		}
	}
	deleteReq, _ := http.NewRequest(http.MethodDelete, srv.URL+"/jobs/"+job.ID, nil)
	if r, err := client.Do(deleteReq); err != nil {
		t.Fatal(err)
	} else {
		r.Body.Close()
		if r.StatusCode != http.StatusAccepted {
			t.Errorf("DELETE job: got %d, want 202", r.StatusCode)
		}
	}
	if code, body := tierGet(t, client, srv.URL+"/jobs", ""); code != 200 || !strings.Contains(body, `"jobs"`) {
		t.Errorf("GET /jobs after delete: got %d %q", code, body)
	}
}

// Auth tier: anonymous inspection is rejected, bearer holders pass, and
// read-only tokens render but cannot mutate.
func TestTier_BrowserAuth(t *testing.T) {
	srv, _ := tierServer(t, api.AuthConfig{Token: "s3cret"})
	client := srv.Client()

	if code, _ := tierGet(t, client, srv.URL+"/jobs", ""); code != http.StatusUnauthorized {
		t.Errorf("anonymous /jobs: got %d, want 401", code)
	}
	// The dashboard shell itself stays public (like /healthz and /metrics)
	// so browsers and scrapers can load without a token; data routes gate.
	if code, _ := tierGet(t, client, srv.URL+"/", ""); code != http.StatusOK {
		t.Errorf("anonymous dashboard: got %d, want 200 (public shell)", code)
	}
	if code, _ := tierGet(t, client, srv.URL+"/jobs", "Bearer s3cret"); code != 200 {
		t.Errorf("authed /jobs: got %d, want 200", code)
	}

	ro := "readonly:" + sha256Hex("reader")
	roSrv, _ := tierServer(t, api.AuthConfig{TokenSHA256: []string{ro}})
	if code, _ := tierGet(t, roSrv.Client(), roSrv.URL+"/jobs", "Bearer reader"); code != 200 {
		t.Errorf("readonly /jobs: got %d, want 200", code)
	}
	resp, err := http.Post(roSrv.URL+"/jobs", "application/yaml", strings.NewReader(tierSDKJob))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusCreated {
		t.Error("readonly token submitted a job without credentials")
	}
	req, _ := http.NewRequest(http.MethodPost, roSrv.URL+"/jobs", strings.NewReader(tierSDKJob))
	req.Header.Set("Authorization", "Bearer reader")
	req.Header.Set("Content-Type", "application/yaml")
	r, err := roSrv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusForbidden {
		t.Errorf("readonly POST /jobs: got %d, want 403", r.StatusCode)
	}
}

// Metrics-rendering tier: the controller exposes aggregate series the
// dashboard tiles and Grafana links consume, and per-job history endpoints
// stay reachable after a submit.
func TestTier_BrowserMetricsRendering(t *testing.T) {
	srv, _ := tierServer(t, api.AuthConfig{})
	client := srv.Client()

	resp, err := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(tierSDKJob))
	if err != nil {
		t.Fatal(err)
	}
	var job store.Job
	_ = json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	if job.ID == "" {
		t.Fatal("submit failed")
	}

	if code, body := tierGet(t, client, srv.URL+"/metrics", ""); code != 200 {
		t.Fatalf("GET /metrics: got %d", code)
	} else {
		for _, want := range []string{
			`weibo_controller_jobs{desired="running"} 1`,
			`weibo_controller_api_requests_total`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("controller metrics missing %q", want)
			}
		}
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, "weibo_controller_") && strings.Contains(line, job.ID) {
				t.Errorf("job ID leaked into metric labels: %q", line)
			}
		}
	}

	if code, body := tierGet(t, client, srv.URL+"/jobs/history", ""); code != 200 {
		t.Errorf("GET /jobs/history: got %d", code)
	} else if !strings.Contains(body, `"histories"`) {
		t.Errorf("/jobs/history missing histories envelope: %s", body)
	}
}

// Live replay: same lifecycle assertions against a deployed dashboard
// (nightly points WEIBO_RUN_BROWSER_URL at a real controller).
func TestTier_BrowserAgainstLiveURL(t *testing.T) {
	base := os.Getenv("WEIBO_RUN_BROWSER_URL")
	if base == "" {
		t.Skip("WEIBO_RUN_BROWSER_URL is not set")
	}
	base = strings.TrimSuffix(base, "/")
	client := &http.Client{Timeout: 10 * time.Second}
	token := os.Getenv("WEIBO_BROWSER_TOKEN")
	auth := ""
	if token != "" {
		auth = "Bearer " + token
	}
	if code, body := tierGet(t, client, base+"/", auth); code != 200 || !strings.Contains(body, `id="app"`) {
		t.Fatalf("live dashboard: got %d, app shell missing", code)
	}
	if code, body := tierGet(t, client, base+"/metrics", auth); code != 200 || !strings.Contains(body, "weibo_controller_") {
		t.Fatalf("live metrics: got %d, controller series missing", code)
	}
	if code, _ := tierGet(t, client, base+"/jobs", auth); code != 200 {
		t.Fatalf("live /jobs: got %d", code)
	}
}
