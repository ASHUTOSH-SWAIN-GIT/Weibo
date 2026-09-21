package api_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/api"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

const sdkJob = `kind: sdk
name: orders-sdk
image: registry.example/orders:v1
`

func newAPI(t *testing.T) *httptest.Server {
	t.Helper()
	return newAPIWithController(t, control.New(control.Options{
		Store:       mustStore(t),
		Backend:     backend.NewFake(),
		Image:       "img",
		StopTimeout: time.Second,
	}))
}

func mustStore(t *testing.T) store.Store {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func newAPIWithController(t *testing.T, ctrl *control.Controller) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(api.NewServer(ctrl, "").Handler())
	t.Cleanup(srv.Close)
	return srv
}

// newAuthAPI builds a server that requires the given bearer token.
func newAuthAPI(t *testing.T, token string) *httptest.Server {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ctrl := control.New(control.Options{
		Store: st, Backend: backend.NewFake(), Image: "img", StopTimeout: time.Second,
	})
	srv := httptest.NewServer(api.NewServer(ctrl, token).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func newAuthHashAPI(t *testing.T, hashes ...string) *httptest.Server {
	t.Helper()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: backend.NewFake(), Image: "img", StopTimeout: time.Second,
	})
	srv := httptest.NewServer(api.NewServerWithAuth(ctrl, api.AuthConfig{TokenSHA256: hashes}).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestAuth_TokenGating(t *testing.T) {
	srv := newAuthAPI(t, "s3cret")
	client := srv.Client()

	do := func(method, path, auth string) int {
		req, _ := http.NewRequest(method, srv.URL+path, nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	// Protected route: no header / wrong token → 401; correct token → 200.
	if got := do(http.MethodGet, "/jobs", ""); got != http.StatusUnauthorized {
		t.Errorf("/jobs no token: got %d, want 401", got)
	}
	if got := do(http.MethodGet, "/jobs", "Bearer wrong"); got != http.StatusUnauthorized {
		t.Errorf("/jobs wrong token: got %d, want 401", got)
	}
	if got := do(http.MethodGet, "/jobs", "Bearer s3cret"); got != http.StatusOK {
		t.Errorf("/jobs correct token: got %d, want 200", got)
	}
	if got := do(http.MethodGet, "/cluster", ""); got != http.StatusUnauthorized {
		t.Errorf("/cluster no token: got %d, want 401", got)
	}
	if got := do(http.MethodGet, "/cluster", "Bearer s3cret"); got != http.StatusOK {
		t.Errorf("/cluster correct token: got %d, want 200", got)
	}

	// Public routes need no token so the browser can load and prompt.
	if got := do(http.MethodGet, "/", ""); got != http.StatusOK {
		t.Errorf("GET / no token: got %d, want 200", got)
	}
	if got := do(http.MethodGet, "/healthz", ""); got != http.StatusOK {
		t.Errorf("/healthz no token: got %d, want 200", got)
	}
	if got := do(http.MethodGet, "/readyz", ""); got != http.StatusOK {
		t.Errorf("/readyz no token: got %d, want 200", got)
	}

	// /auth verifies a token: 401 without, 200 with.
	if got := do(http.MethodPost, "/auth", ""); got != http.StatusUnauthorized {
		t.Errorf("/auth no token: got %d, want 401", got)
	}
	if got := do(http.MethodPost, "/auth", "Bearer s3cret"); got != http.StatusOK {
		t.Errorf("/auth correct token: got %d, want 200", got)
	}
}

func TestAuth_OpenWhenNoToken(t *testing.T) {
	srv := newAPI(t) // token ""
	// With no token configured, protected routes stay open.
	resp, err := http.Get(srv.URL + "/jobs")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("open /jobs: got %d, want 200", resp.StatusCode)
	}
}

func TestAuth_SHA256HashesAndReadonlyScope(t *testing.T) {
	srv := newAuthHashAPI(t, "readonly:"+sha256Hex("reader"), "readwrite:"+sha256Hex("writer"))
	client := srv.Client()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/jobs", nil)
	req.Header.Set("Authorization", "Bearer reader")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readonly GET /jobs: got %d, want 200", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/jobs", strings.NewReader(sdkJob))
	req.Header.Set("Authorization", "Bearer reader")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("readonly POST /jobs: got %d, want 403", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/jobs", strings.NewReader(sdkJob))
	req.Header.Set("Authorization", "Bearer writer")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("readwrite POST /jobs: got %d, want 201", resp.StatusCode)
	}
}

func TestSubmitRejectsOversizedBody(t *testing.T) {
	srv := newAPI(t)
	resp, err := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(strings.Repeat("x", (4<<20)+1)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized submit: got %d, want 413", resp.StatusCode)
	}
}

func TestMutationRateLimit(t *testing.T) {
	srv := newAPI(t)
	client := srv.Client()
	form := url.Values{"deleteData": {"false"}}
	var got int
	for i := 0; i < 31; i++ {
		req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/jobs/missing?"+form.Encode(), nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		got = resp.StatusCode
		resp.Body.Close()
	}
	if got != http.StatusTooManyRequests {
		t.Fatalf("31st mutation: got %d, want 429", got)
	}
}

func TestClusterIncludesHostAndSDKContainers(t *testing.T) {
	srv := newAPI(t)
	body, _ := json.Marshal(map[string]any{"workflow": "kind: sdk\nname: orders\nimage: registry/orders:v2\n"})
	resp, err := http.Post(srv.URL+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	resp, err = http.Get(srv.URL + "/cluster")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /cluster: %d", resp.StatusCode)
	}
	var got backend.CapacitySnapshot
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Host == nil || got.Host.Hostname != "fake-host" {
		t.Fatalf("host missing: %+v", got.Host)
	}
	if len(got.Containers) != 1 || got.Containers[0].Image != "registry/orders:v2" || !got.Containers[0].Managed {
		t.Fatalf("containers missing: %+v", got.Containers)
	}
}

func TestSubmitSDKManifestThenListAndGet(t *testing.T) {
	srv := newAPI(t)

	resp, err := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit: got %d", resp.StatusCode)
	}
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	if job.ID == "" || job.Name != "orders-sdk" || job.Kind != store.KindSDK {
		t.Fatalf("job: %+v", job)
	}

	// List.
	resp, err = http.Get(srv.URL + "/jobs")
	if err != nil {
		t.Fatal(err)
	}
	var listed struct {
		Jobs []store.Job `json:"jobs"`
	}
	json.NewDecoder(resp.Body).Decode(&listed)
	resp.Body.Close()
	if len(listed.Jobs) != 1 {
		t.Fatalf("list: got %d jobs", len(listed.Jobs))
	}

	// Get detail.
	resp, err = http.Get(srv.URL + "/jobs/" + job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("get: %d", resp.StatusCode)
	}
	var detail struct {
		Job       store.Job  `json:"job"`
		LatestRun *store.Run `json:"latestRun"`
	}
	json.NewDecoder(resp.Body).Decode(&detail)
	resp.Body.Close()
	if detail.LatestRun == nil || detail.LatestRun.Phase != "running" {
		t.Fatalf("latest run: %+v", detail.LatestRun)
	}
}

func TestSubmitJSONEnvelopeWithEnv(t *testing.T) {
	srv := newAPI(t)
	body, _ := json.Marshal(map[string]any{"workflow": sdkJob, "env": map[string]string{"K": "v"}})
	resp, err := http.Post(srv.URL+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit json: got %d", resp.StatusCode)
	}
}

func TestSubmitJSONEnvelopeWithEnvRefs(t *testing.T) {
	t.Setenv("API_KEY", "from-env-ref")
	srv := newAPI(t)
	body, _ := json.Marshal(map[string]any{
		"workflow": sdkJob,
		"envRefs":  map[string]store.SecretRef{"API_KEY": {Provider: "env", Name: "API_KEY"}},
	})
	resp, err := http.Post(srv.URL+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit json envRefs: got %d", resp.StatusCode)
	}
}

func TestSubmitLaunchFailureExposesRetryMetadata(t *testing.T) {
	fake := backend.NewFake()
	fake.LaunchErr = backend.TransientLaunchErrorf("temporary backend unavailable")
	ctrl := control.New(control.Options{
		Store:       mustStore(t),
		Backend:     fake,
		Image:       "img",
		Restart:     lifecycle.RestartPolicy{MaxAttempts: 2, BaseBackoff: time.Minute},
		StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	resp, err := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit with transient launch failure: got %d, want 202", resp.StatusCode)
	}
	var accepted struct {
		Job     store.Job `json:"job"`
		Warning string    `json:"warning"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&accepted); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if accepted.Job.ID == "" || !strings.Contains(accepted.Warning, "launch") {
		t.Fatalf("accepted response missing job/warning: %+v", accepted)
	}

	resp, err = http.Get(srv.URL + "/jobs/" + accepted.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var detail struct {
		LatestRun *store.Run `json:"latestRun"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatal(err)
	}
	if detail.LatestRun == nil {
		t.Fatal("missing latestRun")
	}
	if detail.LatestRun.Phase != string(lifecycle.Restarting) ||
		detail.LatestRun.FailureKind != store.FailureLaunchTransient ||
		detail.LatestRun.RestartAt == nil {
		t.Fatalf("retry metadata not exposed: %+v", detail.LatestRun)
	}
}

func TestSubmitInvalidSDKRejected(t *testing.T) {
	srv := newAPI(t)
	resp, err := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader("kind: sdk\nname: missing-image\n"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid SDK manifest, got %d", resp.StatusCode)
	}
}

func TestCancelAndRestart(t *testing.T) {
	srv := newAPI(t)
	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()

	// Cancel.
	resp, err := http.Post(srv.URL+"/jobs/"+job.ID+"/cancel", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Restart.
	resp, err = http.Post(srv.URL+"/jobs/"+job.ID+"/restart", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("restart: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestDeleteJob(t *testing.T) {
	srv := newAPI(t)
	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/jobs/"+job.ID, nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("delete: got %d, want 202", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/jobs/" + job.ID)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("deleted job should 404, got %d", resp.StatusCode)
	}
}

func TestDeleteJobWithDeleteDataFlag(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)
	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/jobs/"+job.ID+"?deleteData=true", nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("delete: got %d, want 202", resp.StatusCode)
	}
	if !fake.DataDeleted(job.ID) {
		t.Error("?deleteData=true should wipe durable state")
	}
}

func TestMetricsEndpointAndRouteNormalization(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	resp, _ = http.Get(srv.URL + "/jobs/" + job.ID)
	resp.Body.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	text := string(body)
	if !strings.Contains(text, `weibo_controller_jobs{desired="running"} 1`) {
		t.Error("metrics should report the submitted job")
	}
	// Route labels use mux templates, never raw IDs.
	if !strings.Contains(text, `route="GET /jobs/{id}"`) {
		t.Error("api requests should be labeled by route template")
	}
	if strings.Contains(text, job.ID) {
		t.Error("raw job ID must not appear in metric labels")
	}
}

func TestTargetsDiscovery(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()

	resp, err := http.Get(srv.URL + "/targets")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var targets []struct {
		Targets []string          `json:"targets"`
		Labels  map[string]string `json:"labels"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&targets); err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].Labels["weibo_job_id"] != job.ID {
		t.Fatalf("targets=%+v, want one for job %s", targets, job.ID)
	}
	if len(targets[0].Targets) != 1 || targets[0].Targets[0] == "" {
		t.Fatalf("target address missing: %+v", targets)
	}
}

func TestJobHistoryDownsamples(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()

	for i := 0; i < 5; i++ {
		ctrl.History().Add(job.ID, sampleAt(i))
	}
	resp, err := http.Get(srv.URL + "/jobs/" + job.ID + "/history?points=2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		JobID  string `json:"jobId"`
		Points []struct {
			RecordsOut int64 `json:"recordsOut"`
		} `json:"points"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.JobID != job.ID || len(out.Points) != 2 {
		t.Fatalf("history=%+v", out)
	}
	if out.Points[0].RecordsOut != 0 || out.Points[1].RecordsOut != 400 {
		t.Fatalf("endpoints not preserved: %+v", out.Points)
	}

	resp404, err := http.Get(srv.URL + "/jobs/nope/history")
	if err != nil {
		t.Fatal(err)
	}
	resp404.Body.Close()
	if resp404.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown job history: got %d, want 404", resp404.StatusCode)
	}
}

func sampleAt(i int) control.Sample {
	return control.Sample{
		At:         time.Now().UTC().Add(time.Duration(i) * time.Second),
		Phase:      "running",
		RecordsIn:  int64(i * 10),
		RecordsOut: int64(i * 100),
	}
}

func TestBulkHistoryAndConfig(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
		GrafanaURL: "https://grafana.example",
	})
	srv := newAPIWithController(t, ctrl)

	var ids []string
	for range 2 {
		resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
		var job store.Job
		json.NewDecoder(resp.Body).Decode(&job)
		resp.Body.Close()
		ids = append(ids, job.ID)
		ctrl.History().Add(job.ID, sampleAt(0))
	}
	resp, err := http.Get(srv.URL + "/jobs/history?points=10")
	if err != nil {
		t.Fatal(err)
	}
	var bulk struct {
		Histories map[string][]any `json:"histories"`
	}
	json.NewDecoder(resp.Body).Decode(&bulk)
	resp.Body.Close()
	if len(bulk.Histories) != 2 {
		t.Fatalf("bulk=%+v", bulk)
	}

	resp, err = http.Get(srv.URL + "/config")
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		GrafanaURL string `json:"grafanaUrl"`
	}
	json.NewDecoder(resp.Body).Decode(&cfg)
	resp.Body.Close()
	if cfg.GrafanaURL != "https://grafana.example" {
		t.Fatalf("config=%+v", cfg)
	}
}

func TestUIServesHistoryHooks(t *testing.T) {
	srv := newAPI(t)
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	html := string(body)
	for _, want := range []string{"grafanaLink", "/config", "Source Lag"} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard missing history hook %q", want)
		}
	}
}

func TestUIServesDiagnosticsHooks(t *testing.T) {
	srv := newAPI(t)
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	html := string(body)
	for _, want := range []string{"diagnostics", "auditMore", "Older", "Attempts"} {
		if !strings.Contains(html, want) {
			t.Errorf("dashboard missing diagnostics hook %q", want)
		}
	}
}

func TestDiagnosticsEndpoint(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	ctrl.History().Add(job.ID, control.Sample{
		At: time.Now().UTC(), Phase: "running", RecordsOut: 8,
		CheckpointID: "cp-1", CheckpointDurationMs: 250, CheckpointSizeBytes: 1024,
	})

	resp, err := http.Get(srv.URL + "/jobs/" + job.ID + "/diagnostics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var d struct {
		Phase      string `json:"phase"`
		Checkpoint *struct {
			ID         string `json:"id"`
			DurationMs int64  `json:"durationMs"`
			SizeBytes  int64  `json:"sizeBytes"`
		} `json:"checkpoint"`
		Activity *struct {
			RecordsOut int64 `json:"recordsOut"`
		} `json:"activity"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		t.Fatal(err)
	}
	if d.Phase != "running" || d.Checkpoint == nil || d.Checkpoint.DurationMs != 250 || d.Activity == nil {
		t.Fatalf("diagnostics=%+v", d)
	}

	resp404, err := http.Get(srv.URL + "/jobs/nope/diagnostics")
	if err != nil {
		t.Fatal(err)
	}
	resp404.Body.Close()
	if resp404.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown job diagnostics: got %d, want 404", resp404.StatusCode)
	}
}

func TestRunsEndpoints(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	first := latestRun(t, srv, job.ID)
	if _, err := http.Post(srv.URL+"/jobs/"+job.ID+"/restart", "", nil); err != nil {
		t.Fatal(err)
	}

	// List: newest first.
	resp, err := http.Get(srv.URL + "/jobs/" + job.ID + "/runs")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Runs []store.Run `json:"runs"`
	}
	json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	if len(list.Runs) != 2 || list.Runs[0].ID == first.ID {
		t.Fatalf("runs=%+v", list.Runs)
	}

	// Get: run detail with audit transitions.
	resp, err = http.Get(srv.URL + "/jobs/" + job.ID + "/runs/" + first.ID)
	if err != nil {
		t.Fatal(err)
	}
	var detail struct {
		Run         store.Run `json:"run"`
		Transitions []any     `json:"transitions"`
	}
	json.NewDecoder(resp.Body).Decode(&detail)
	resp.Body.Close()
	if detail.Run.ID != first.ID || len(detail.Transitions) == 0 {
		t.Fatalf("run detail=%+v", detail)
	}

	// Unknown run is 404, even with a valid job.
	resp404, err := http.Get(srv.URL + "/jobs/" + job.ID + "/runs/nope")
	if err != nil {
		t.Fatal(err)
	}
	resp404.Body.Close()
	if resp404.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown run: got %d, want 404", resp404.StatusCode)
	}
}

func latestRun(t *testing.T, srv *httptest.Server, jobID string) store.Run {
	t.Helper()
	resp, err := http.Get(srv.URL + "/jobs/" + jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var d struct {
		LatestRun *store.Run `json:"latestRun"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		t.Fatal(err)
	}
	return *d.LatestRun
}

func TestTransitionsPagingEndpoint(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	if _, err := http.Post(srv.URL+"/jobs/"+job.ID+"/restart", "", nil); err != nil {
		t.Fatal(err)
	}

	get := func(q string) (int, int64) {
		resp, err := http.Get(srv.URL + "/jobs/" + job.ID + "/transitions" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Transitions []struct {
				ID int64 `json:"id"`
			} `json:"transitions"`
			NextBefore int64 `json:"nextBefore"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return len(out.Transitions), out.NextBefore
	}
	n, next := get("?limit=2")
	if n != 2 || next == 0 {
		t.Fatalf("page1: n=%d next=%d", n, next)
	}
	n2, next2 := get("?limit=100&before=" + strconv.FormatInt(next, 10))
	if n2 == 0 {
		t.Fatalf("page2: n=%d next=%d", n2, next2)
	}
	n3, _ := get("?limit=100&before=" + strconv.FormatInt(next2, 10))
	if n3 != 0 {
		t.Fatalf("page3 should be exhausted, got %d", n3)
	}
	if total := n + n2 + n3; total < 4 {
		t.Fatalf("paged walk covered %d transitions, want >= 4", total)
	}
}

func TestLogsStream(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	run := latestRun(t, srv, job.ID)
	fake.SetLogs(run.ContainerID, "line one\nline two\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/jobs/"+job.ID+"/logs/stream?tail=200", nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type=%q", ct)
	}
	// The initial burst ("data: line one", "data: line two", blank) is
	// sent immediately; read exactly it, then cancel so the handler's
	// 2s poll loop exits.
	br := bufio.NewReader(resp.Body)
	var burst []string
	for len(burst) < 3 {
		line, err := br.ReadString('\n')
		burst = append(burst, line)
		if err != nil {
			break
		}
	}
	cancel()
	text := strings.Join(burst, "")
	if !strings.Contains(text, "data: line one") || !strings.Contains(text, "data: line two") {
		t.Fatalf("stream missing initial burst: %q", text)
	}
}

// A terminal job has nothing left to follow — the stream must reject it
// server-side rather than only disabling the button client-side.
func TestLogsStreamRejectsTerminalJob(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	run := latestRun(t, srv, job.ID)

	// Exit code 0 goes straight to Finished without consulting the restart
	// policy (only a nonzero exit ever triggers a restart), so this can't
	// flake into "restarting" under the controller's default retry policy.
	fake.SetPhase(run.ContainerID, backend.PhaseExited, 0)
	if err := ctrl.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	resp2, err := http.Get(srv.URL + "/jobs/" + job.ID + "/logs/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode == http.StatusOK {
		t.Fatalf("terminal job logs/stream: got 200, want a rejection")
	}
}

func TestSecretValuesNeverReachAPI(t *testing.T) {
	fake := backend.NewFake()
	ctrl := control.New(control.Options{
		Store: mustStore(t), Backend: fake, Image: "img", StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	const secret = "s3cr3t-value-xyz"
	body, _ := json.Marshal(map[string]any{"workflow": sdkJob, "env": map[string]string{"API_KEY": secret}})
	resp, err := http.Post(srv.URL+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()

	for _, path := range []string{
		"/jobs/" + job.ID,
		"/jobs/" + job.ID + "/diagnostics",
		"/jobs/" + job.ID + "/transitions?limit=100",
		"/jobs/" + job.ID + "/runs",
		"/jobs/history?points=10",
		"/metrics",
		"/targets",
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if strings.Contains(string(data), secret) {
			t.Errorf("secret value leaked in %s", path)
		}
	}
}

// A job ID that never existed must 404 on the live-agent proxy endpoints,
// not the 503 reserved for a real job with no current control surface.
func TestProxyUnknownJob404s(t *testing.T) {
	srv := newAPI(t)
	for _, path := range []string{"/state", "/describe", "/plan", "/metrics"} {
		resp, err := http.Get(srv.URL + "/jobs/does-not-exist" + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s for unknown job: got %d, want 404", path, resp.StatusCode)
		}
	}
}

func TestReadinessReportsBackendOutage(t *testing.T) {
	ctrl := control.New(control.Options{
		Store:       mustStore(t),
		Backend:     outageBackend{ContainerBackend: backend.NewFake()},
		Image:       "img",
		StopTimeout: time.Second,
	})
	srv := newAPIWithController(t, ctrl)

	resp, err := http.Get(srv.URL + "/livez")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("livez should stay OK during dependency outage, got %d", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz: got %d, want 503", resp.StatusCode)
	}
}

type outageBackend struct {
	backend.ContainerBackend
}

func (outageBackend) Capacity(ctx context.Context, cfg backend.CapacityConfig) (backend.CapacitySnapshot, error) {
	return backend.CapacitySnapshot{Backend: "fake", Health: "unreachable", Reason: "simulated outage"}, nil
}

func TestListIncludesPhase(t *testing.T) {
	srv := newAPI(t)
	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(sdkJob))
	resp.Body.Close()

	resp, err := http.Get(srv.URL + "/jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var listed struct {
		Jobs []struct {
			Phase string `json:"phase"`
		} `json:"jobs"`
	}
	json.NewDecoder(resp.Body).Decode(&listed)
	if len(listed.Jobs) != 1 || listed.Jobs[0].Phase != "running" {
		t.Fatalf("list phase: %+v", listed.Jobs)
	}
}

func TestServesUI(t *testing.T) {
	srv := newAPI(t)
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GET /: %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("content-type: %q", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	html := string(body)
	if !strings.Contains(strings.ToLower(html), "weibo") {
		t.Fatal("index.html not served")
	}
	// NOTE: "Grafana ↗" was once removed UI, but roadmap #15 reintroduced
	// it deliberately as the external-Grafana deep link (see
	// TestUIServesHistoryHooks), so it is no longer in this list.
	//
	// Infrastructure/Host machine/Containers (the Job Manager page) and
	// Deploy a job (the Submit page) were dead, unreachable code — no
	// route ever led to them — removed along with the rest of the
	// disused tab system; see TestDashboard_MinimalSectionsShellRenders
	// for the routes that must stay absent.
	for _, removed := range []string{
		"Weibo Resource Model", "Submit New Job", "type: generator",
		"Infrastructure", "Host machine", "Containers", "Deploy a job",
	} {
		if strings.Contains(html, removed) {
			t.Errorf("dashboard still contains removed UI %q", removed)
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	srv := newAPI(t)
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/jobs", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}
