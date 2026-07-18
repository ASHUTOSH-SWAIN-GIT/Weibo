package control_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/store"
)

const validWorkflow = `name: wordcount
version: "1"
source:
  type: generator
  records:
    - {key: hello, value: '{"word":"hello"}'}
pipeline:
  - {id: by-word, type: keyBy, keyBy: {field: word, partitions: 1}}
  - {id: count, type: reduce, reduce: {function: count}}
sink: {type: stdout}
`

func newController(t *testing.T, fake *backend.Fake, restart lifecycle.RestartPolicy) (*control.Controller, store.Store) {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	c := control.New(control.Options{
		Store:       st,
		Backend:     fake,
		Image:       "mailer-runner:test",
		Restart:     restart,
		StopTimeout: time.Second,
	})
	return c, st
}

func TestSubmitLaunchesJob(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())

	job, err := c.Submit(context.Background(), []byte(validWorkflow), nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if job.Name != "wordcount" || job.Desired != store.DesiredRunning {
		t.Fatalf("job: %+v", job)
	}
	if fake.Launched() != 1 {
		t.Fatalf("expected 1 launch, got %d", fake.Launched())
	}
	run, _ := c.LatestRun(job.ID)
	if run == nil || run.Phase != string(lifecycle.Running) || run.ContainerID == "" {
		t.Fatalf("run not launched: %+v", run)
	}
	if run.HostPort == 0 {
		t.Error("expected a mapped host port")
	}
}

func TestSubmitRejectsInvalidWorkflow(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())

	_, err := c.Submit(context.Background(), []byte("not: a valid: workflow: ["), nil)
	if err == nil {
		t.Fatal("expected submit to reject invalid workflow")
	}
	if fake.Launched() != 0 {
		t.Error("no container should launch for an invalid workflow")
	}
}

func TestSecretsPassedbutNotPersisted(t *testing.T) {
	fake := backend.NewFake()
	c, st := newController(t, fake, lifecycle.DefaultRestartPolicy())

	job, err := c.Submit(context.Background(), []byte(validWorkflow), map[string]string{"API_KEY": "s3cr3t"})
	if err != nil {
		t.Fatal(err)
	}
	run, _ := c.LatestRun(job.ID)
	if env := fake.LastEnv(run.ContainerID); env["API_KEY"] != "s3cr3t" {
		t.Errorf("secret not passed to container: %v", env)
	}
	// The persisted job spec must not contain the resolved secret value.
	stored, _ := st.GetJob(job.ID)
	if strings.Contains(stored.Spec, "s3cr3t") {
		t.Error("secret leaked into persisted job spec")
	}
}

func TestCancelStopsJob(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())
	job, _ := c.Submit(context.Background(), []byte(validWorkflow), nil)

	if err := c.Cancel(context.Background(), job.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	got, _ := c.GetJob(job.ID)
	if got.Desired != store.DesiredStopped {
		t.Errorf("desired: got %q", got.Desired)
	}
	run, _ := c.LatestRun(job.ID)
	if run.Phase != string(lifecycle.Cancelled) || run.Stopped == nil {
		t.Errorf("run not cancelled: %+v", run)
	}
}

func TestRestartLaunchesNewRun(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())
	job, _ := c.Submit(context.Background(), []byte(validWorkflow), nil)
	first, _ := c.LatestRun(job.ID)

	if _, err := c.Restart(context.Background(), job.ID); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	second, _ := c.LatestRun(job.ID)
	if second.ID == first.ID {
		t.Fatal("restart did not create a new run")
	}
	if fake.Launched() != 2 {
		t.Errorf("expected 2 launches, got %d", fake.Launched())
	}
}

// A crashed container (nonzero exit) is restarted by the reconciler while
// the policy has attempts left, then given up on.
func TestReconcileRestartsOnCrash(t *testing.T) {
	fake := backend.NewFake()
	// Zero backoff so the test doesn't sleep.
	c, _ := newController(t, fake, lifecycle.RestartPolicy{MaxAttempts: 2, BaseBackoff: 0})
	job, _ := c.Submit(context.Background(), []byte(validWorkflow), nil)

	run, _ := c.LatestRun(job.ID)
	fake.SetPhase(run.ContainerID, backend.PhaseExited, 1) // crash

	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	// attempt 1 failed → a new run (attempt 2) launched.
	if fake.Launched() != 2 {
		t.Fatalf("expected a restart (2 launches), got %d", fake.Launched())
	}
	run2, _ := c.LatestRun(job.ID)
	if run2.Attempt != 2 {
		t.Errorf("attempt: got %d, want 2", run2.Attempt)
	}

	// Crash again → policy exhausted (attempt == MaxAttempts), no restart.
	fake.SetPhase(run2.ContainerID, backend.PhaseExited, 1)
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.Launched() != 2 {
		t.Errorf("policy should be exhausted, got %d launches", fake.Launched())
	}
	final, _ := c.LatestRun(job.ID)
	if final.Phase != string(lifecycle.Failed) {
		t.Errorf("final phase: got %q, want failed", final.Phase)
	}
}

// The store is the source of truth: a fresh Controller over the same store
// re-attaches to a still-running container via Reconcile — no state lost
// across a controller restart.
func TestControllerRestartReattaches(t *testing.T) {
	fake := backend.NewFake()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	c1 := control.New(control.Options{Store: st, Backend: fake, Image: "img", StopTimeout: time.Second})
	job, _ := c1.Submit(context.Background(), []byte(validWorkflow), nil)
	run, _ := c1.LatestRun(job.ID)

	// New controller instance, same store + same (still-running) backend.
	c2 := control.New(control.Options{Store: st, Backend: fake, Image: "img", StopTimeout: time.Second})
	if err := c2.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := c2.LatestRun(job.ID)
	if got.ContainerID != run.ContainerID || got.Phase != string(lifecycle.Running) {
		t.Fatalf("new controller lost the running job: %+v", got)
	}
	jobs, _ := c2.ListJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected the job to survive, got %d", len(jobs))
	}
}
