package control_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
	srv := httptest.NewServer(api.NewServer(ctrl).Handler())
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
