package control_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/api"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/store"
)

const runnerImage = "mailer-runner:dev"

// bigWorkflow builds a generator→stdout job with enough records to stay
// running for a few seconds, so we can observe it live and cancel it.
// Kept under the API's 4 MiB submit cap.
func bigWorkflow(records int) string {
	var b strings.Builder
	b.WriteString("name: itest\nversion: \"1\"\nsource:\n  type: generator\n  records:\n")
	for range records {
		b.WriteString("    - {key: h, value: '{\"word\":\"h\"}'}\n")
	}
	b.WriteString("pipeline:\n  - {id: by-word, type: keyBy, keyBy: {field: word, partitions: 1}}\n")
	b.WriteString("  - {id: count, type: reduce, reduce: {function: count}}\nsink: {type: stdout}\n")
	return b.String()
}

// dockerReady skips the test unless a Docker daemon and the runner image
// are both available.
func dockerReady(t *testing.T) *backend.Docker {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Docker integration test in -short mode")
	}
	d, err := backend.NewDocker(runnerImage)
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := d.Ping(ctx); err != nil {
		t.Skipf("docker daemon unreachable: %v", err)
	}
	return d
}

// The P3 gate: submit → run → cancel → restart a real YAML job through the
// API against local Docker, and confirm the store survives a controller
// restart (a fresh controller re-attaches to the running container).
func TestIntegration_SubmitRunCancelRestart(t *testing.T) {
	docker := dockerReady(t)
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "control.db")
	st, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	ctrl := control.New(control.Options{Store: st, Backend: docker, Image: runnerImage})
	srv := httptest.NewServer(api.NewServer(ctrl, "").Handler())
	defer srv.Close()

	// Clean up any containers we launch, whatever the outcome.
	var jobID string
	defer func() {
		if jobID != "" {
			cleanupJob(docker, st, jobID)
		}
	}()

	// --- submit ---
	resp, err := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(bigWorkflow(100_000)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit: got %d", resp.StatusCode)
	}
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	jobID = job.ID
	t.Logf("submitted job %s (%s)", job.ID, job.Name)

	// --- run: observe the live control surface via the proxy ---
	if !waitFor(4*time.Second, func() bool {
		return proxyPhase(srv.URL, job.ID) == "running"
	}) {
		// The job may have finished quickly; that's still a valid run, but
		// on a fast machine we expect to catch it running at least once.
		t.Logf("did not observe running phase (job may have finished fast)")
	}

	// --- cancel ---
	post(t, srv.URL+"/jobs/"+job.ID+"/cancel")
	got, _ := ctrl.GetJob(job.ID)
	if got.Desired != store.DesiredStopped {
		t.Fatalf("after cancel, desired=%q", got.Desired)
	}
	// Reconcile until the run reaches a terminal phase.
	if !waitFor(35*time.Second, func() bool {
		_ = ctrl.Reconcile(ctx)
		run, _ := ctrl.LatestRun(job.ID)
		return run != nil && run.Stopped != nil
	}) {
		t.Fatal("run did not reach terminal phase after cancel")
	}

	// --- restart ---
	before, _ := ctrl.LatestRun(job.ID)
	post(t, srv.URL+"/jobs/"+job.ID+"/restart")
	after, _ := ctrl.LatestRun(job.ID)
	if after.ID == before.ID {
		t.Fatal("restart did not create a new run")
	}
	if after.ContainerID == "" {
		t.Fatal("restart produced no container")
	}
	t.Logf("restarted: new run %s container %s", after.ID, after.ContainerID)

	// --- controller restart: fresh controller, same DB, re-attaches ---
	ctrl2 := control.New(control.Options{Store: st, Backend: docker, Image: runnerImage})
	if err := ctrl2.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	jobs, _ := ctrl2.ListJobs()
	if len(jobs) != 1 || jobs[0].ID != job.ID {
		t.Fatalf("job did not survive controller restart: %+v", jobs)
	}
	t.Log("fresh controller re-attached to the job from the store")
}

// statefulWorkflow builds a checkpointing, Pebble-backed counting job:
// every record shares one key, so the reduce count accumulates and its
// value is a direct witness of restored state.
func statefulWorkflow(records int) string {
	var b strings.Builder
	b.WriteString("name: sp-itest\nversion: \"1\"\n")
	b.WriteString("env:\n  checkpointing:\n    interval: 300ms\n  state:\n    backend: pebble\n")
	b.WriteString("source:\n  type: generator\n  records:\n")
	for range records {
		b.WriteString("    - {key: h, value: '{\"word\":\"h\"}'}\n")
	}
	b.WriteString("pipeline:\n  - {id: by-word, type: keyBy, keyBy: {field: word, partitions: 1}}\n")
	b.WriteString("  - {id: count, type: reduce, reduce: {function: count}}\nsink: {type: stdout}\n")
	return b.String()
}

// The savepoint half of the P4 gate: submit a stateful job → savepoint →
// restart-from-savepoint, and confirm the reduce state carried over
// (the restarted run's count exceeds a single run's record total).
func TestIntegration_SavepointAndRestore(t *testing.T) {
	docker := dockerReady(t)
	ctx := context.Background()
	const records = 80_000

	st, err := store.OpenSQLite(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctrl := control.New(control.Options{Store: st, Backend: docker, Image: runnerImage})
	srv := httptest.NewServer(api.NewServer(ctrl, "").Handler())
	defer srv.Close()

	var jobID string
	defer func() {
		if jobID != "" {
			cleanupJob(docker, st, jobID)
		}
	}()

	// Submit and wait until it is actually processing.
	resp, err := http.Post(srv.URL+"/jobs", "application/yaml", strings.NewReader(statefulWorkflow(records)))
	if err != nil {
		t.Fatal(err)
	}
	var job store.Job
	json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	jobID = job.ID
	if !waitFor(6*time.Second, func() bool { return proxyPhase(srv.URL, job.ID) == "running" }) {
		t.Fatal("job never reached running")
	}

	// Take a savepoint (stop-with-savepoint).
	post(t, srv.URL+"/jobs/"+job.ID+"/savepoint?label=sp1")
	if !waitFor(35*time.Second, func() bool {
		_ = ctrl.Reconcile(ctx)
		run, _ := ctrl.LatestRun(job.ID)
		return run != nil && run.Stopped != nil
	}) {
		t.Fatal("run did not stop after savepoint")
	}
	// Run 1's logs must show the savepoint was promoted.
	logs1 := logsOf(t, srv.URL, job.ID)
	if !strings.Contains(logs1, `savepoint "sp1" created`) {
		t.Fatalf("run 1 did not create the savepoint; logs:\n%s", tailStr(logs1, 600))
	}

	// Restart from the savepoint and let it run to completion.
	restartFromSavepoint(t, srv.URL, job.ID, "sp1")
	if !waitFor(40*time.Second, func() bool {
		_ = ctrl.Reconcile(ctx)
		run, _ := ctrl.LatestRun(job.ID)
		return run != nil && run.Stopped != nil && run.Phase == "finished"
	}) {
		t.Fatal("restored run did not finish")
	}

	logs2 := logsOf(t, srv.URL, job.ID)
	if !strings.Contains(logs2, `restored from savepoint "sp1"`) {
		t.Fatalf("restored run did not seed from savepoint; logs:\n%s", tailStr(logs2, 600))
	}
	// State carried over: replaying `records` on top of the savepoint's
	// count must exceed a single run's total. A fresh run would cap at
	// `records`.
	if got := maxCount(logs2); got <= records {
		t.Fatalf("state did not carry over: max count %d, want > %d", got, records)
	} else {
		t.Logf("restored run reached count %d (savepoint carryover proven)", got)
	}
}

func logsOf(t *testing.T, base, id string) string {
	t.Helper()
	// tail=0 → all lines: the "restored from savepoint" banner prints at
	// container startup, before the run's output, so a tail would miss it.
	resp, err := http.Get(base + "/jobs/" + id + "/logs?tail=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func restartFromSavepoint(t *testing.T, base, id, label string) {
	t.Helper()
	body := strings.NewReader(`{"savepoint":"` + label + `"}`)
	resp, err := http.Post(base+"/jobs/"+id+"/restart", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("restart-from-savepoint: got %d", resp.StatusCode)
	}
}

var countRe = regexp.MustCompile(`"count":(\d+)`)

func maxCount(logs string) int {
	max := 0
	for _, m := range countRe.FindAllStringSubmatch(logs, -1) {
		if n, err := strconv.Atoi(m[1]); err == nil && n > max {
			max = n
		}
	}
	return max
}

func tailStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// --- helpers ---

func proxyPhase(base, id string) string {
	resp, err := http.Get(base + "/jobs/" + id + "/state")
	if err != nil || resp.StatusCode != 200 {
		if resp != nil {
			resp.Body.Close()
		}
		return ""
	}
	defer resp.Body.Close()
	var st struct {
		Phase string `json:"phase"`
	}
	json.NewDecoder(resp.Body).Decode(&st)
	return st.Phase
}

func post(t *testing.T, url string) {
	t.Helper()
	resp, err := http.Post(url, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST %s: got %d", url, resp.StatusCode)
	}
}

func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(150 * time.Millisecond)
	}
	return false
}

func cleanupJob(d *backend.Docker, st store.Store, jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runs, _ := st.ListRuns(jobID)
	for _, r := range runs {
		if r.ContainerID != "" {
			_ = d.Stop(ctx, r.ContainerID, 2*time.Second)
			_ = d.Remove(ctx, r.ContainerID)
		}
	}
	fmt.Printf("cleaned up job %s (%d runs)\n", jobID, len(runs))
}
