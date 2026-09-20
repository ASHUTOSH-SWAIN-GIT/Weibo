package api_test

// Phase D9 — Accuracy tests and regression gates.
//
// These tests pin the dashboard contract in plans/dashboard-revamp-roadmap.md
// so future backend changes cannot silently break the source/sink/operator
// sections. They run against httptest servers (no browser installed), the
// same harness as browser_tiers_test.go, and therefore run on every PR via
// the existing CI test job.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/api"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

func d9Server(t *testing.T, auth api.AuthConfig) *httptest.Server {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ctrl := control.New(control.Options{
		Store:       st,
		Backend:     backend.NewFake(),
		Image:       "img",
		StopTimeout: time.Second,
	})
	srv := httptest.NewServer(api.NewServerWithAuth(ctrl, auth).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func d9Get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func d9Submit(t *testing.T, srv *httptest.Server, body, ctype string) store.Job {
	t.Helper()
	resp, err := http.Post(srv.URL+"/jobs", ctype, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("submit: got %d: %s", resp.StatusCode, data)
	}
	var job store.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	if job.ID == "" {
		t.Fatal("submit returned empty job ID")
	}
	return job
}

// Sources renders: the dashboard is intentionally reduced to one visible
// section while the source UX is designed.
func TestDashboard_SourcesOnlyShellRenders(t *testing.T) {
	srv := d9Server(t, api.AuthConfig{})
	code, html := d9Get(t, srv.URL+"/")
	if code != 200 {
		t.Fatalf("GET /: got %d", code)
	}
	for _, want := range []string{
		`id="app"`, `data-r="sources"`, `Sources`,
		`Only source inventory is shown for now`,
		`sourceRows`, `sourceFallback`, `normalizeSources`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("sources shell missing %q", want)
		}
	}
	for _, removed := range []string{
		`data-r="overview"`, `data-r="job-manager"`, `data-r="running"`,
		`data-r="completed"`, `data-r="submit"`,
	} {
		if strings.Contains(html, removed) {
			t.Errorf("sources-only dashboard still exposes %q", removed)
		}
	}
}

// Job detail renders only source detail for now; the other panes stay out of
// the visible UI until they are designed section-by-section.
func TestDashboard_JobDetailSourcesOnly(t *testing.T) {
	srv := d9Server(t, api.AuthConfig{})
	_, html := d9Get(t, srv.URL+"/")
	for _, want := range []string{
		`data-pane="sources"`, `normalizeSources`,
		`Source · Kafka`, `Source · File`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("source detail shell missing %q", want)
		}
	}
	for _, removed := range []string{
		`data-tab="overview"`, `data-tab="operators"`, `data-tab="sinks"`,
		`data-tab="checkpoints"`, `data-tab="runs"`, `data-tab="logs"`,
		`data-tab="spec"`,
	} {
		if strings.Contains(html, removed) {
			t.Errorf("source detail still exposes removed tab %q", removed)
		}
	}
}

// Backend contracts the normalized models depend on stay stable.
func TestDashboard_APIContractsStable(t *testing.T) {
	srv := d9Server(t, api.AuthConfig{})
	job := d9Submit(t, srv, tierSDKJob, "application/yaml")

	// /jobs list carries phase for the fleet view.
	resp, err := http.Get(srv.URL + "/jobs")
	if err != nil {
		t.Fatal(err)
	}
	var listed struct {
		Jobs []struct {
			ID    string `json:"id"`
			Phase string `json:"phase"`
		} `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(listed.Jobs) != 1 || listed.Jobs[0].Phase == "" {
		t.Fatalf("list phase contract broken: %+v", listed.Jobs)
	}

	// /jobs/{id} carries job + latestRun + transitions.
	resp, err = http.Get(srv.URL + "/jobs/" + job.ID)
	if err != nil {
		t.Fatal(err)
	}
	var detail struct {
		Job         *store.Job `json:"job"`
		LatestRun   *store.Run `json:"latestRun"`
		Transitions []any      `json:"transitions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if detail.Job == nil || detail.LatestRun == nil {
		t.Fatalf("detail contract broken: %+v", detail)
	}

	// /runs envelope + run detail with transitions.
	resp, err = http.Get(srv.URL + "/jobs/" + job.ID + "/runs")
	if err != nil {
		t.Fatal(err)
	}
	var runs struct {
		Runs []store.Run `json:"runs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(runs.Runs) == 0 {
		t.Fatal("runs envelope empty")
	}
	resp, err = http.Get(srv.URL + "/jobs/" + job.ID + "/runs/" + runs.Runs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	var runDetail struct {
		Run         store.Run `json:"run"`
		Transitions []any     `json:"transitions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&runDetail); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if runDetail.Run.ID == "" || len(runDetail.Transitions) == 0 {
		t.Fatalf("run detail contract broken: %+v", runDetail)
	}

	// Paged transitions carry the nextBefore cursor.
	resp, err = http.Get(srv.URL + "/jobs/" + job.ID + "/transitions?limit=2")
	if err != nil {
		t.Fatal(err)
	}
	var trs struct {
		Transitions []any `json:"transitions"`
		NextBefore  int64 `json:"nextBefore"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&trs); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(trs.Transitions) == 0 {
		t.Fatal("transitions envelope empty")
	}

	// Diagnostics, history, bulk history, config envelopes.
	for _, path := range []string{
		"/jobs/" + job.ID + "/diagnostics",
		"/jobs/" + job.ID + "/history?points=2",
		"/jobs/history?points=2",
		"/config",
	} {
		if code, _ := d9Get(t, srv.URL+path); code != 200 {
			t.Errorf("GET %s: got %d, want 200", path, code)
		}
	}
	if _, body := d9Get(t, srv.URL+"/jobs/history?points=2"); !strings.Contains(body, `"histories"`) {
		t.Error("bulk history missing histories envelope")
	}
	if _, body := d9Get(t, srv.URL+"/config"); !strings.Contains(body, "grafanaUrl") {
		t.Error("config missing grafanaUrl")
	}
}

// Missing live-agent endpoints degrade gracefully: the fake backend has no
// reachable control surface, so /state /metrics /describe /plan must not be
// 200 — and the shell must still render with "not reported" fallbacks
// instead of failing the whole page.
func TestDashboard_DegradesWithoutLiveAgent(t *testing.T) {
	srv := d9Server(t, api.AuthConfig{})
	job := d9Submit(t, srv, tierSDKJob, "application/yaml")

	for _, path := range []string{"/state", "/metrics", "/describe", "/plan"} {
		code, _ := d9Get(t, srv.URL+"/jobs/"+job.ID+path)
		if code == 200 {
			t.Errorf("GET %s: got 200, want non-200 without a live agent", path)
		}
	}

	_, html := d9Get(t, srv.URL+"/")
	for _, want := range []string{
		`not reported`, `pending describe`, `metrics not reported`,
		`Not running`, `Diagnostics are unavailable`,
		`source does not expose`, `describe unavailable`,
		`plan unavailable`, `No output`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("graceful-degradation shell missing %q", want)
		}
	}

	// The page stays functional: diagnostics/runs/transitions still serve.
	for _, path := range []string{
		"/jobs/" + job.ID + "/diagnostics",
		"/jobs/" + job.ID + "/runs",
		"/jobs/" + job.ID + "/transitions?limit=2",
	} {
		if code, _ := d9Get(t, srv.URL+path); code != 200 {
			t.Errorf("GET %s after degraded live agent: got %d, want 200", path, code)
		}
	}
}

// Sources/operators/sinks sections show the correct text for Kafka and file
// jobs: generic cards, Kafka partition detail, delivery derivation.
func TestDashboard_SourceSinkOperatorFixtures(t *testing.T) {
	srv := d9Server(t, api.AuthConfig{})
	_, html := d9Get(t, srv.URL+"/")
	for _, want := range []string{
		`Source · Kafka`, `Source · File`, `Sink ·`,
		`Logical operators`, `Runtime stages`,
		`Delivery guarantee`, `exactly-once`, `at-least-once`, `at-most-once`,
		`normalizeSources`, `normalizeSinks`, `normalizeStages`,
		`normalizeOperators`, `deriveDelivery`, `checkpointPositions`,
		`<th>Topic</th>`, `High wm`, `Lag`,
		`bottleneck`, `stateful`,
		`Checkpoint health`, `State backend`, `Checkpoint history`,
		`Checkpointed source positions`,
		`Attempts`, `Full lifecycle`, `Attempt logs`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("source/sink/operator shell missing %q", want)
		}
	}
}

// Read-only auth hides/disables mutation actions: the backend 403s every
// mutation, /auth reports the role, and the shell renders disabled
// mutation buttons for readonly tokens.
func TestDashboard_ReadonlyDisablesMutations(t *testing.T) {
	ro := "readonly:" + sha256Hex("reader")
	rw := "readwrite:" + sha256Hex("writer")
	srv := d9Server(t, api.AuthConfig{TokenSHA256: []string{ro, rw}})
	client := srv.Client()
	authGet := func(path, token string) (int, string) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	authMut := func(method, path, token, body string) int {
		var rdr *strings.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		} else {
			rdr = strings.NewReader("")
		}
		req, _ := http.NewRequest(method, srv.URL+path, rdr)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/yaml")
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	// /auth reports the token scope without listing jobs.
	for token, wantRole := range map[string]string{"reader": "readonly", "writer": "readwrite"} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/auth", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Status string `json:"status"`
			Role   string `json:"role"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if out.Status != "ok" || out.Role != wantRole {
			t.Errorf("POST /auth %s: got %+v, want role %q", token, out, wantRole)
		}
	}

	// Read-only inspects but every mutation is 403.
	if code, _ := authGet("/jobs", "reader"); code != 200 {
		t.Errorf("readonly GET /jobs: got %d, want 200", code)
	}
	job := func() store.Job {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/jobs", strings.NewReader(tierSDKJob))
		req.Header.Set("Authorization", "Bearer writer")
		req.Header.Set("Content-Type", "application/yaml")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var j store.Job
		_ = json.NewDecoder(resp.Body).Decode(&j)
		return j
	}()
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/jobs"},
		{http.MethodPost, "/jobs/" + job.ID + "/cancel"},
		{http.MethodPost, "/jobs/" + job.ID + "/restart"},
		{http.MethodPost, "/jobs/" + job.ID + "/savepoint?label=x"},
		{http.MethodDelete, "/jobs/" + job.ID},
	} {
		if got := authMut(tc.method, tc.path, "reader", map[bool]string{true: tierSDKJob}[tc.path == "/jobs"]); got != http.StatusForbidden {
			t.Errorf("readonly %s %s: got %d, want 403", tc.method, tc.path, got)
		}
	}

	// The shell disables mutation actions for readonly tokens.
	_, html := d9Get(t, srv.URL+"/")
	for _, want := range []string{
		`userRole`, `canMutate`, `mutAttr`, `refreshRole`,
		`read-only token`, `Read-only token cannot`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("readonly shell missing %q", want)
		}
	}
}

// Forbidden secret strings never render: secret env values stay out of every
// API surface, and the dashboard redacts sensitive props before display.
func TestDashboard_NoSecretLeak(t *testing.T) {
	srv := d9Server(t, api.AuthConfig{})
	const secret = "d9-s3cr3t-value-xyz"
	body, _ := json.Marshal(map[string]any{"workflow": tierSDKJob, "env": map[string]string{"API_KEY": secret}})
	resp, err := http.Post(srv.URL+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var job store.Job
	_ = json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	if job.ID == "" {
		t.Fatal("submit failed")
	}

	for _, path := range []string{
		"/jobs/" + job.ID,
		"/jobs/" + job.ID + "/diagnostics",
		"/jobs/" + job.ID + "/transitions?limit=100",
		"/jobs/" + job.ID + "/runs",
		"/jobs/history?points=10",
		"/metrics",
		"/targets",
		"/",
	} {
		_, data := d9Get(t, srv.URL+path)
		if strings.Contains(data, secret) {
			t.Errorf("secret value leaked in %s", path)
		}
	}

	_, html := d9Get(t, srv.URL+"/")
	for _, want := range []string{
		`SENSITIVE_RE`, `redactProps`, `• redacted •`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("redaction shell missing %q", want)
		}
	}
}
