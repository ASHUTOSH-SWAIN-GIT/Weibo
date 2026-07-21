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

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/api"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/store"
)

const wf = `name: wordcount
version: "1"
source:
  type: generator
  records: [{key: hello, value: '{"word":"hello"}'}]
pipeline:
  - {id: by-word, type: keyBy, keyBy: {field: word, partitions: 1}}
  - {id: count, type: reduce, reduce: {function: count}}
sink: {type: stdout}
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
	srv := httptest.NewServer(api.NewServer(ctrl).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func TestSubmitRawYAMLThenListAndGet(t *testing.T) {
	srv := newAPI(t)

	// Submit raw YAML.
	resp, err := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(wf))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit: got %d", resp.StatusCode)
	}
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	if job.ID == "" || job.Name != "wordcount" {
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
	body, _ := json.Marshal(map[string]any{"workflow": wf, "env": map[string]string{"K": "v"}})
	resp, err := http.Post(srv.URL+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit json: got %d", resp.StatusCode)
	}
}

func TestSubmitInvalidRejected(t *testing.T) {
	srv := newAPI(t)
	resp, err := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader("::: not valid :::"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid workflow, got %d", resp.StatusCode)
	}
}

func TestCancelAndRestart(t *testing.T) {
	srv := newAPI(t)
	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(wf))
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

func TestValidatePreview(t *testing.T) {
	srv := newAPI(t)
	body, _ := json.Marshal(map[string]any{"workflow": wf})
	resp, err := http.Post(srv.URL+"/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("validate: got %d", resp.StatusCode)
	}
	var out struct {
		Name     string `json:"name"`
		Delivery string `json:"delivery"`
		Graph    struct {
			Source string `json:"Source"`
			Sink   string `json:"Sink"`
		} `json:"graph"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if out.Name != "wordcount" || out.Graph.Source != "generator" || out.Graph.Sink != "stdout" {
		t.Fatalf("preview wrong: %+v", out)
	}

	// Validate must NOT create a job.
	resp2, _ := http.Get(srv.URL + "/jobs")
	var listed struct {
		Jobs []any `json:"jobs"`
	}
	json.NewDecoder(resp2.Body).Decode(&listed)
	resp2.Body.Close()
	if len(listed.Jobs) != 0 {
		t.Fatalf("validate must not launch a job, found %d", len(listed.Jobs))
	}
}

func TestListIncludesPhase(t *testing.T) {
	srv := newAPI(t)
	resp, _ := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(wf))
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
	if !strings.Contains(strings.ToLower(string(body)), "mailer") {
		t.Fatal("index.html not served")
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
