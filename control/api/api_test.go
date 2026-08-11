package api_test

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

const sdkJob = `kind: sdk
name: orders-sdk
image: registry.example/orders:v1
`

func newAPI(t *testing.T) *httptest.Server {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ctrl := control.New(control.Options{
		Store: st, Backend: backend.NewFake(), Image: "img", StopTimeout: time.Second,
	})
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
	for _, required := range []string{"Infrastructure", "Host machine", "Containers", "Deploy a job", "kind: sdk", "imageId"} {
		if !strings.Contains(html, required) {
			t.Errorf("dashboard missing current contract %q", required)
		}
	}
	for _, removed := range []string{"Weibo Resource Model", "Grafana ↗", "Submit New Job", "type: generator"} {
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
