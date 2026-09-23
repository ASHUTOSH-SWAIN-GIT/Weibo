package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordedReq is one request the fake controller saw.
type recordedReq struct {
	method, path, rawPath, query, auth, contentType, body string
}

// fakeController is an httptest stand-in for the controller API. It records
// every request, then delegates to handler for the response.
type fakeController struct {
	URL  string
	mu   sync.Mutex
	reqs []recordedReq
}

func newFakeController(t *testing.T, handler http.HandlerFunc) *fakeController {
	t.Helper()
	fc := &fakeController{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fc.mu.Lock()
		fc.reqs = append(fc.reqs, recordedReq{
			method: r.Method, path: r.URL.Path, rawPath: r.URL.EscapedPath(), query: r.URL.RawQuery,
			auth: r.Header.Get("Authorization"), contentType: r.Header.Get("Content-Type"), body: string(body),
		})
		fc.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	fc.URL = srv.URL
	return fc
}

// last returns the most recent request, failing the test if there was none.
func (fc *fakeController) last(t *testing.T) recordedReq {
	t.Helper()
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.reqs) == 0 {
		t.Fatal("controller saw no requests")
	}
	return fc.reqs[len(fc.reqs)-1]
}

func (fc *fakeController) count() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.reqs)
}

func writeJSONResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// capture runs fn with os.Stdout/os.Stderr redirected, returning fn's exit code
// and everything it printed. The subcommands print straight to the process
// streams, so this is how their output is asserted. Tests using it must not
// run in parallel.
func capture(t *testing.T, fn func() int) (code int, stdout, stderr string) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	ro, wo, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	re, we, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout, os.Stderr = wo, we
	outC, errC := make(chan string, 1), make(chan string, 1)
	go func() { b, _ := io.ReadAll(ro); outC <- string(b) }()
	go func() { b, _ := io.ReadAll(re); errC <- string(b) }()

	code = fn()

	_ = wo.Close()
	_ = we.Close()
	os.Stdout, os.Stderr = oldOut, oldErr
	return code, <-outC, <-errC
}

var testJob = map[string]any{
	"id": "job1", "name": "orders", "kind": "sdk", "image": "me/orders:1",
	"delivery": "at-least-once", "desiredState": "running",
	"createdAt": "2026-01-02T03:04:05Z", "updatedAt": "2026-01-02T03:04:05Z",
}

// --- client ---

func TestClient_ErrorEnvelopeAndStatus(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/jobs/missing" {
			writeJSONResp(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}
		http.Error(w, "<html>boom</html>", http.StatusInternalServerError)
	})
	c := newClient(fc.URL, "")

	_, err := c.getJob(context.Background(), "missing")
	if err == nil || !strings.Contains(err.Error(), "job not found") || !strings.Contains(err.Error(), "404") {
		t.Fatalf("API error envelope not surfaced, got %v", err)
	}
	// A non-JSON error body falls back to the bare status.
	_, err = c.getJob(context.Background(), "other")
	if err == nil || !strings.Contains(err.Error(), "controller returned 500") {
		t.Fatalf("expected bare-status error, got %v", err)
	}
}

func TestClient_UnreachableController(t *testing.T) {
	c := newClient("http://127.0.0.1:1", "")
	_, err := c.listJobs(context.Background())
	if err == nil || !strings.Contains(err.Error(), "contacting controller at http://127.0.0.1:1") {
		t.Fatalf("expected a contacting-controller error, got %v", err)
	}
}

func TestClient_TokenAndBaseURLNormalized(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, 200, map[string]any{"jobs": []any{}})
	})
	c := newClient(fc.URL+"///", "  secret-token \n") // trailing slashes and whitespace are trimmed
	if _, err := c.listJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fc.last(t); got.auth != "Bearer secret-token" || got.path != "/jobs" {
		t.Fatalf("got auth=%q path=%q, want trimmed token and clean path", got.auth, got.path)
	}
}

func TestClient_NoTokenSendsNoAuthHeader(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, 200, map[string]any{"jobs": []any{}})
	})
	if _, err := newClient(fc.URL, "").listJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fc.last(t).auth; got != "" {
		t.Fatalf("Authorization = %q, want none", got)
	}
}

func TestClient_ListGetRuns(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/jobs":
			row := map[string]any{"phase": "running"}
			maps.Copy(row, testJob)
			writeJSONResp(w, 200, map[string]any{"jobs": []any{row}})
		case "/jobs/job1":
			writeJSONResp(w, 200, map[string]any{
				"job":         testJob,
				"latestRun":   map[string]any{"id": "r1", "phase": "running", "attempt": 2, "startedAt": "2026-01-02T03:04:05Z"},
				"transitions": []any{map[string]any{"from": "a", "to": "b", "at": "2026-01-02T03:04:05Z"}},
			})
		case "/jobs/job1/runs":
			writeJSONResp(w, 200, map[string]any{"runs": []any{map[string]any{"id": "r1", "attempt": 1, "phase": "failed", "startedAt": "2026-01-02T03:04:05Z"}}})
		default:
			http.NotFound(w, r)
		}
	})
	c := newClient(fc.URL, "")
	ctx := context.Background()

	rows, err := c.listJobs(ctx)
	if err != nil || len(rows) != 1 || rows[0].ID != "job1" || rows[0].Phase != "running" {
		t.Fatalf("listJobs = %+v, %v", rows, err)
	}
	d, err := c.getJob(ctx, "job1")
	if err != nil || d.Job.Name != "orders" || d.LatestRun.Attempt != 2 || len(d.Transitions) != 1 {
		t.Fatalf("getJob = %+v, %v", d, err)
	}
	runs, err := c.runs(ctx, "job1")
	if err != nil || len(runs) != 1 || runs[0].Phase != "failed" {
		t.Fatalf("runs = %+v, %v", runs, err)
	}
}

func TestClient_DecodeErrors(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("not json")) })
	c := newClient(fc.URL, "")
	ctx := context.Background()
	if _, err := c.listJobs(ctx); err == nil {
		t.Error("listJobs: want decode error")
	}
	if _, err := c.getJob(ctx, "x"); err == nil {
		t.Error("getJob: want decode error")
	}
	if _, err := c.runs(ctx, "x"); err == nil {
		t.Error("runs: want decode error")
	}
	if _, err := c.restart(ctx, "x", ""); err == nil {
		t.Error("restart: want decode error")
	}
	if _, _, err := c.submit(ctx, []byte("kind: sdk"), nil); err == nil {
		t.Error("submit: want decode error")
	}
}

func TestClient_JobIDIsPathEscaped(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { writeJSONResp(w, 200, map[string]any{"job": testJob}) })
	if _, err := newClient(fc.URL, "").getJob(context.Background(), "a/b c"); err != nil {
		t.Fatal(err)
	}
	// Assert the ON-THE-WIRE form: decoded, "a/b c" would look identical whether or
	// not the client escaped it. The slash must travel as %2F so it can't split the path.
	if got := fc.last(t).rawPath; got != "/jobs/a%2Fb%20c" {
		t.Fatalf("escaped path = %q, want /jobs/a%%2Fb%%20c", got)
	}
}

func TestClient_SubmitCreatedAndAccepted(t *testing.T) {
	status := http.StatusCreated
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if status == http.StatusAccepted {
			writeJSONResp(w, status, map[string]any{"job": testJob, "warning": "image pull failed"})
			return
		}
		writeJSONResp(w, status, testJob) // 201 returns the bare job
	})
	c := newClient(fc.URL, "")

	job, warn, err := c.submit(context.Background(), []byte("kind: sdk\nname: orders"), map[string]string{"K": "V"})
	if err != nil || job.ID != "job1" || warn != "" {
		t.Fatalf("201 submit = %+v, %q, %v", job, warn, err)
	}
	req := fc.last(t)
	if req.method != http.MethodPost || req.path != "/jobs" || req.contentType != "application/json" {
		t.Fatalf("submit request = %+v", req)
	}
	var sent struct {
		Workflow string            `json:"workflow"`
		Env      map[string]string `json:"env"`
	}
	if err := json.Unmarshal([]byte(req.body), &sent); err != nil || sent.Workflow != "kind: sdk\nname: orders" || sent.Env["K"] != "V" {
		t.Fatalf("submit body = %q (%v)", req.body, err)
	}

	status = http.StatusAccepted
	job, warn, err = c.submit(context.Background(), []byte("x"), nil)
	if err != nil || job.ID != "job1" || warn != "image pull failed" {
		t.Fatalf("202 submit = %+v, %q, %v", job, warn, err)
	}
}

func TestClient_RestartBodyOnlyWithSavepoint(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { writeJSONResp(w, 200, testJob) })
	c := newClient(fc.URL, "")

	if _, err := c.restart(context.Background(), "job1", ""); err != nil {
		t.Fatal(err)
	}
	if got := fc.last(t); got.method != http.MethodPost || got.path != "/jobs/job1/restart" || got.body != "" || got.contentType != "" {
		t.Fatalf("plain restart = %+v", got)
	}
	if _, err := c.restart(context.Background(), "job1", "before-upgrade"); err != nil {
		t.Fatal(err)
	}
	if got := fc.last(t); got.contentType != "application/json" || got.body != `{"savepoint":"before-upgrade"}` {
		t.Fatalf("savepoint restart = %+v", got)
	}
}

func TestClient_CancelSavepointDelete(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	c := newClient(fc.URL, "")
	ctx := context.Background()

	if err := c.cancel(ctx, "job1"); err != nil {
		t.Fatal(err)
	}
	if got := fc.last(t); got.method != http.MethodPost || got.path != "/jobs/job1/cancel" {
		t.Fatalf("cancel = %+v", got)
	}

	if err := c.savepoint(ctx, "job1", "a b&c"); err != nil {
		t.Fatal(err)
	}
	if got := fc.last(t); got.method != http.MethodPost || got.path != "/jobs/job1/savepoint" || got.query != "label=a+b%26c" {
		t.Fatalf("savepoint = %+v", got)
	}

	if err := c.deleteJob(ctx, "job1", false); err != nil {
		t.Fatal(err)
	}
	if got := fc.last(t); got.method != http.MethodDelete || got.path != "/jobs/job1" || got.query != "" {
		t.Fatalf("delete = %+v", got)
	}
	if err := c.deleteJob(ctx, "job1", true); err != nil {
		t.Fatal(err)
	}
	if got := fc.last(t); got.query != "deleteData=true" {
		t.Fatalf("delete -delete-data query = %q", got.query)
	}
}

func TestClient_Logs(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("line1\nline2\n")) })
	out, err := newClient(fc.URL, "tok").logs(context.Background(), "job1", 50)
	if err != nil || out != "line1\nline2\n" {
		t.Fatalf("logs = %q, %v", out, err)
	}
	if got := fc.last(t); got.path != "/jobs/job1/logs" || got.query != "tail=50" || got.auth != "Bearer tok" {
		t.Fatalf("logs request = %+v", got)
	}
}

func TestClient_FollowLogsParsesSSE(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, ": heartbeat\n\n")  // comment: skipped
		fmt.Fprint(w, "data: hello\n\n")  // delivered
		fmt.Fprint(w, "data:\n\n")        // empty data line: delivered as ""
		fmt.Fprint(w, "event: ignored\n") // non-data field: skipped
		fmt.Fprint(w, "data: world\n\n")  // delivered
	})
	var got []string
	err := newClient(fc.URL, "tok").followLogs(context.Background(), "job1", 10, func(l string) { got = append(got, l) })
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"hello", "", "world"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("lines = %q, want %q", got, want)
	}
	if req := fc.last(t); req.path != "/jobs/job1/logs/stream" || req.query != "tail=10" || req.auth != "Bearer tok" {
		t.Fatalf("follow request = %+v", req)
	}
}

func TestClient_FollowLogsErrors(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("tail") == "1" {
			writeJSONResp(w, http.StatusNotFound, map[string]string{"error": "no such job"})
			return
		}
		http.Error(w, "x", http.StatusBadGateway)
	})
	c := newClient(fc.URL, "")
	if err := c.followLogs(context.Background(), "j", 1, func(string) {}); err == nil || !strings.Contains(err.Error(), "no such job") {
		t.Errorf("API error not surfaced: %v", err)
	}
	if err := c.followLogs(context.Background(), "j", 2, func(string) {}); err == nil || !strings.Contains(err.Error(), "controller returned 502") {
		t.Errorf("bare status not surfaced: %v", err)
	}
	if err := newClient("http://127.0.0.1:1", "").followLogs(context.Background(), "j", 1, func(string) {}); err == nil || !strings.Contains(err.Error(), "contacting controller") {
		t.Errorf("unreachable controller not surfaced: %v", err)
	}
}

// --- subcommands ---

func TestRunJobs(t *testing.T) {
	var jobs []any
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, 200, map[string]any{"jobs": jobs})
	})

	code, out, _ := capture(t, func() int { return runJobs([]string{"-controller", fc.URL}) })
	if code != 0 || strings.TrimSpace(out) != "no jobs" {
		t.Fatalf("empty: code=%d out=%q", code, out)
	}

	row := map[string]any{"phase": "running", "id": "abc123", "name": "orders", "kind": "", "desiredState": "running", "updatedAt": time.Now().UTC().Format(time.RFC3339)}
	jobs = []any{row}
	code, out, _ = capture(t, func() int { return runJobs([]string{"-controller", fc.URL}) })
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
	for _, want := range []string{"ID", "PHASE", "abc123", "orders", "yaml", "running", "just now"} { // empty kind renders as yaml
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestRunJobs_ControllerErrorAndBadFlag(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, http.StatusUnauthorized, map[string]string{"error": "bad token"})
	})
	code, _, errOut := capture(t, func() int { return runJobs([]string{"-controller", fc.URL}) })
	if code != 1 || !strings.Contains(errOut, "weibo: bad token") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
	if code, _, _ := capture(t, func() int { return runJobs([]string{"-nope"}) }); code != 2 {
		t.Fatalf("bad flag exit code = %d, want 2", code)
	}
}

func TestRunStatus(t *testing.T) {
	transitions := make([]any, 10)
	for i := range transitions {
		transitions[i] = map[string]any{"from": "a", "to": "b", "reason": fmt.Sprintf("r%d", i), "at": "2026-01-02T03:04:05Z"}
	}
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, 200, map[string]any{
			"job":         testJob,
			"latestRun":   map[string]any{"id": "r1", "phase": "failed", "attempt": 3, "error": "image pull failed", "startedAt": "2026-01-02T03:04:05Z"},
			"transitions": transitions,
		})
	})

	code, out, _ := capture(t, func() int { return runStatus([]string{"-controller", fc.URL, "job1"}) })
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
	for _, want := range []string{"ID:        job1", "Name:      orders", "Kind:      sdk", "Image:     me/orders:1", "Phase:   failed (attempt 3)", "Error:   image pull failed", "Recent transitions:"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// Only the last 8 of 10 transitions are shown, oldest first: r2..r9.
	for _, gone := range []string{"r0", "r1"} {
		if strings.Contains(out, gone) {
			t.Errorf("transition %s should have been trimmed:\n%s", gone, out)
		}
	}
	for _, kept := range []string{"r2", "r9"} {
		if !strings.Contains(out, kept) {
			t.Errorf("transition %s missing:\n%s", kept, out)
		}
	}
}

func TestRunStatus_MinimalJobAndErrors(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/jobs/gone" {
			writeJSONResp(w, 404, map[string]string{"error": "job not found"})
			return
		}
		// A yaml job with no image, no run and no transitions.
		writeJSONResp(w, 200, map[string]any{"job": map[string]any{"id": "y1", "name": "wf", "desiredState": "running", "createdAt": "2026-01-02T03:04:05Z"}})
	})
	code, out, _ := capture(t, func() int { return runStatus([]string{"-controller", fc.URL, "y1"}) })
	if code != 0 || !strings.Contains(out, "Kind:      yaml") || strings.Contains(out, "Image:") || strings.Contains(out, "Latest run") {
		t.Fatalf("minimal status: code=%d out=%q", code, out)
	}
	if code, _, errOut := capture(t, func() int { return runStatus([]string{"-controller", fc.URL, "gone"}) }); code != 1 || !strings.Contains(errOut, "job not found") {
		t.Fatalf("missing job: code=%d stderr=%q", code, errOut)
	}
	if code, _, errOut := capture(t, func() int { return runStatus([]string{"-controller", fc.URL}) }); code != 2 || !strings.Contains(errOut, "usage: weibo status") {
		t.Fatalf("no id: code=%d stderr=%q", code, errOut)
	}
	if code, _, _ := capture(t, func() int { return runStatus([]string{"-nope"}) }); code != 2 {
		t.Fatalf("bad flag exit code = %d, want 2", code)
	}
}

func TestRunLogs(t *testing.T) {
	body := "one\ntwo" // no trailing newline: the CLI adds one
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) })

	code, out, _ := capture(t, func() int { return runLogs([]string{"-controller", fc.URL, "-tail", "5", "job1"}) })
	if code != 0 || out != "one\ntwo\n" {
		t.Fatalf("code=%d out=%q", code, out)
	}
	if got := fc.last(t).query; got != "tail=5" {
		t.Fatalf("query = %q", got)
	}

	body = "already-terminated\n"
	if _, out, _ := capture(t, func() int { return runLogs([]string{"-controller", fc.URL, "job1"}) }); out != "already-terminated\n" {
		t.Fatalf("out = %q (must not add a second newline)", out)
	}
	if got := fc.last(t).query; got != "tail=200" {
		t.Fatalf("default tail query = %q", got)
	}

	if code, _, errOut := capture(t, func() int { return runLogs([]string{"-controller", fc.URL}) }); code != 2 || !strings.Contains(errOut, "usage: weibo logs") {
		t.Fatalf("no id: code=%d stderr=%q", code, errOut)
	}
	if code, _, _ := capture(t, func() int { return runLogs([]string{"-nope"}) }); code != 2 {
		t.Fatalf("bad flag exit code = %d, want 2", code)
	}
}

func TestRunLogs_ControllerErrorAndFollow(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/stream") {
			fmt.Fprint(w, "data: streamed line\n\n")
			return
		}
		writeJSONResp(w, 404, map[string]string{"error": "job not found"})
	})
	if code, _, errOut := capture(t, func() int { return runLogs([]string{"-controller", fc.URL, "job1"}) }); code != 1 || !strings.Contains(errOut, "job not found") {
		t.Fatalf("error path: code=%d stderr=%q", code, errOut)
	}
	// -follow ends cleanly when the server closes the stream.
	code, out, _ := capture(t, func() int { return runLogs([]string{"-controller", fc.URL, "-follow", "job1"}) })
	if code != 0 || out != "streamed line\n" {
		t.Fatalf("follow: code=%d out=%q", code, out)
	}
	// A failing follow (unreachable controller) is reported, not swallowed.
	if code, _, errOut := capture(t, func() int { return runLogs([]string{"-controller", "http://127.0.0.1:1", "-follow", "job1"}) }); code != 1 || !strings.Contains(errOut, "contacting controller") {
		t.Fatalf("follow error: code=%d stderr=%q", code, errOut)
	}
}

func TestRunRuns(t *testing.T) {
	var runs []any
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { writeJSONResp(w, 200, map[string]any{"runs": runs}) })

	code, out, _ := capture(t, func() int { return runRuns([]string{"-controller", fc.URL, "job1"}) })
	if code != 0 || strings.TrimSpace(out) != "no runs" {
		t.Fatalf("empty: code=%d out=%q", code, out)
	}

	runs = []any{
		map[string]any{"id": "run-b", "attempt": 2, "phase": "running", "startedAt": "2026-01-02T03:04:05Z"},
		map[string]any{"id": "run-a", "attempt": 1, "phase": "failed", "error": "oom", "startedAt": "2026-01-01T03:04:05Z", "stoppedAt": "2026-01-01T03:09:05Z"},
	}
	code, out, _ = capture(t, func() int { return runRuns([]string{"-controller", fc.URL, "job1"}) })
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
	for _, want := range []string{"ATTEMPT", "run-b", "run-a", "oom", "—"} { // "—" is the still-running run's STOPPED cell
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if code, _, errOut := capture(t, func() int { return runRuns([]string{"-controller", fc.URL}) }); code != 2 || !strings.Contains(errOut, "usage: weibo runs") {
		t.Fatalf("no id: code=%d stderr=%q", code, errOut)
	}
	if code, _, _ := capture(t, func() int { return runRuns([]string{"-nope"}) }); code != 2 {
		t.Fatalf("bad flag exit code = %d, want 2", code)
	}
}

func TestRunRuns_ControllerError(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, 500, map[string]string{"error": "store down"})
	})
	if code, _, errOut := capture(t, func() int { return runRuns([]string{"-controller", fc.URL, "job1"}) }); code != 1 || !strings.Contains(errOut, "store down") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestRunCancel(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	code, out, _ := capture(t, func() int { return runCancel([]string{"-controller", fc.URL, "job1"}) })
	if code != 0 || strings.TrimSpace(out) != "cancelling job1" {
		t.Fatalf("code=%d out=%q", code, out)
	}
	if got := fc.last(t); got.method != http.MethodPost || got.path != "/jobs/job1/cancel" {
		t.Fatalf("request = %+v", got)
	}
	if code, _, errOut := capture(t, func() int { return runCancel([]string{"-controller", fc.URL}) }); code != 2 || !strings.Contains(errOut, "usage: weibo cancel") {
		t.Fatalf("no id: code=%d stderr=%q", code, errOut)
	}
	if code, _, _ := capture(t, func() int { return runCancel([]string{"-nope"}) }); code != 2 {
		t.Fatalf("bad flag exit code = %d, want 2", code)
	}
}

func TestRunCancel_ControllerError(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, 409, map[string]string{"error": "already stopped"})
	})
	if code, _, errOut := capture(t, func() int { return runCancel([]string{"-controller", fc.URL, "job1"}) }); code != 1 || !strings.Contains(errOut, "already stopped") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestRunRestart(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { writeJSONResp(w, 200, testJob) })

	code, out, _ := capture(t, func() int { return runRestart([]string{"-controller", fc.URL, "job1"}) })
	if code != 0 || strings.TrimSpace(out) != "restarting job1 from last checkpoint" {
		t.Fatalf("plain: code=%d out=%q", code, out)
	}
	code, out, _ = capture(t, func() int { return runRestart([]string{"-controller", fc.URL, "-savepoint", "sp1", "job1"}) })
	if code != 0 || strings.TrimSpace(out) != `restarting job1 from savepoint "sp1"` {
		t.Fatalf("savepoint: code=%d out=%q", code, out)
	}
	if got := fc.last(t); got.body != `{"savepoint":"sp1"}` {
		t.Fatalf("body = %q", got.body)
	}
	if code, _, errOut := capture(t, func() int { return runRestart([]string{"-controller", fc.URL}) }); code != 2 || !strings.Contains(errOut, "usage: weibo restart") {
		t.Fatalf("no id: code=%d stderr=%q", code, errOut)
	}
	if code, _, _ := capture(t, func() int { return runRestart([]string{"-nope"}) }); code != 2 {
		t.Fatalf("bad flag exit code = %d, want 2", code)
	}
}

func TestRunRestart_ControllerError(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, 404, map[string]string{"error": "no such savepoint"})
	})
	if code, _, errOut := capture(t, func() int { return runRestart([]string{"-controller", fc.URL, "job1"}) }); code != 1 || !strings.Contains(errOut, "no such savepoint") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestRunDelete(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })

	code, out, _ := capture(t, func() int { return runDelete([]string{"-controller", fc.URL, "job1"}) })
	if code != 0 || strings.TrimSpace(out) != "deleted job1" {
		t.Fatalf("code=%d out=%q", code, out)
	}
	if got := fc.last(t); got.method != http.MethodDelete || got.query != "" {
		t.Fatalf("plain delete = %+v (must not wipe durable state)", got)
	}
	if code, _, _ := capture(t, func() int { return runDelete([]string{"-controller", fc.URL, "-delete-data", "job1"}) }); code != 0 {
		t.Fatalf("code=%d", code)
	}
	if got := fc.last(t); got.query != "deleteData=true" {
		t.Fatalf("query = %q", got.query)
	}
	if code, _, errOut := capture(t, func() int { return runDelete([]string{"-controller", fc.URL}) }); code != 2 || !strings.Contains(errOut, "usage: weibo delete") {
		t.Fatalf("no id: code=%d stderr=%q", code, errOut)
	}
	if code, _, _ := capture(t, func() int { return runDelete([]string{"-nope"}) }); code != 2 {
		t.Fatalf("bad flag exit code = %d, want 2", code)
	}
}

func TestRunDelete_ControllerError(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, 403, map[string]string{"error": "read-only token"})
	})
	if code, _, errOut := capture(t, func() int { return runDelete([]string{"-controller", fc.URL, "job1"}) }); code != 1 || !strings.Contains(errOut, "read-only token") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

func TestRunSavepoint(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusAccepted) })

	code, out, _ := capture(t, func() int { return runSavepoint([]string{"-controller", fc.URL, "-label", "pre-upgrade", "job1"}) })
	if code != 0 || strings.TrimSpace(out) != `savepoint "pre-upgrade": stopping job1` {
		t.Fatalf("code=%d out=%q", code, out)
	}
	if got := fc.last(t); got.path != "/jobs/job1/savepoint" || got.query != "label=pre-upgrade" {
		t.Fatalf("request = %+v", got)
	}
	n := fc.count()
	// Both the job id and -label are required; nothing may reach the controller without them.
	for _, args := range [][]string{{"-controller", fc.URL, "job1"}, {"-controller", fc.URL, "-label", "x"}} {
		if code, _, errOut := capture(t, func() int { return runSavepoint(args) }); code != 2 || !strings.Contains(errOut, "usage: weibo savepoint") {
			t.Fatalf("args %v: code=%d stderr=%q", args, code, errOut)
		}
	}
	if fc.count() != n {
		t.Fatal("a request was sent despite missing required arguments")
	}
	if code, _, _ := capture(t, func() int { return runSavepoint([]string{"-nope"}) }); code != 2 {
		t.Fatalf("bad flag exit code = %d, want 2", code)
	}
}

func TestRunSavepoint_ControllerError(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, 409, map[string]string{"error": "job not running"})
	})
	if code, _, errOut := capture(t, func() int { return runSavepoint([]string{"-controller", fc.URL, "-label", "x", "job1"}) }); code != 1 || !strings.Contains(errOut, "job not running") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}

// --- shared helpers ---

func TestControllerFlagsDefaultFromEnv(t *testing.T) {
	fc := newFakeController(t, func(w http.ResponseWriter, r *http.Request) { writeJSONResp(w, 200, map[string]any{"jobs": []any{}}) })
	t.Setenv("WEIBO_CONTROLLER", fc.URL)
	t.Setenv("WEIBO_TOKEN", "env-token")

	// No -controller/-token flags: both come from the environment.
	if code, _, _ := capture(t, func() int { return runJobs(nil) }); code != 0 {
		t.Fatalf("code=%d", code)
	}
	if got := fc.last(t).auth; got != "Bearer env-token" {
		t.Fatalf("auth = %q, want the WEIBO_TOKEN env value", got)
	}
}

func TestEnvOr(t *testing.T) {
	t.Setenv("WEIBO_TEST_ENVOR", "")
	if got := envOr("WEIBO_TEST_ENVOR", "def"); got != "def" {
		t.Errorf("unset/empty = %q, want the default", got)
	}
	t.Setenv("WEIBO_TEST_ENVOR", "val")
	if got := envOr("WEIBO_TEST_ENVOR", "def"); got != "val" {
		t.Errorf("set = %q, want val", got)
	}
}

func TestKindOr(t *testing.T) {
	if kindOr("") != "yaml" || kindOr("sdk") != "sdk" {
		t.Fatal("empty kind must render as yaml; other kinds pass through")
	}
}

func TestAgo(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		t    time.Time
		want string
	}{
		{"zero", time.Time{}, "—"},
		{"seconds", now.Add(-10 * time.Second), "just now"},
		{"minutes", now.Add(-5*time.Minute - time.Second), "5m"},
		{"hours", now.Add(-3*time.Hour - time.Minute), "3h"},
		{"days", now.Add(-50 * time.Hour), "2d"},
	}
	for _, tc := range tests {
		if got := ago(tc.t); got != tc.want {
			t.Errorf("%s: ago = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestFail(t *testing.T) {
	code, _, errOut := capture(t, func() int { return fail(fmt.Errorf("boom")) })
	if code != 1 || errOut != "weibo: boom\n" {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
}
