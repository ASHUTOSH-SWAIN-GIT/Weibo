package control_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

const validSDKManifest = `kind: sdk
name: orders-sdk
image: my-registry/orders-sdk:v1
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
		Image:       "unused-default-image:test",
		Restart:     restart,
		StopTimeout: time.Second,
	})
	return c, st
}

func TestSubmitSDKLaunchesJob(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())

	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if job.Name != "orders-sdk" || job.Kind != store.KindSDK || job.Desired != store.DesiredRunning {
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

// An SDK manifest is auto-detected, stored as an sdk job, and launched
// with its own image (no workflow compilation).
func TestSubmitSDK_DetectedAndLaunched(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())

	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatalf("Submit(sdk): %v", err)
	}
	if job.Kind != store.KindSDK || job.Image != "my-registry/orders-sdk:v1" || job.Name != "orders-sdk" {
		t.Fatalf("sdk job wrong: %+v", job)
	}
	if fake.Launched() != 1 {
		t.Fatalf("expected launch, got %d", fake.Launched())
	}
	// The container runs the SDK image with NO workflow doc injected.
	run, _ := c.LatestRun(job.ID)
	if got := fake.LastImage(run.ContainerID); got != "my-registry/orders-sdk:v1" {
		t.Errorf("launched image: got %q", got)
	}
	if doc := fake.LastWorkflowDoc(run.ContainerID); len(doc) != 0 {
		t.Errorf("sdk job must not inject a workflow doc, got %q", doc)
	}
}

// An SDK manifest's env and resources flow through to the LaunchSpec, and
// API-supplied secret env overrides manifest env.
func TestSubmitSDK_EnvAndResources(t *testing.T) {
	const doc = `kind: sdk
name: orders-sdk
image: my-registry/orders-sdk:v1
env:
  LOG_LEVEL: info
  DB_HOST: manifest-db
resources:
  cpu: 500m
  memory: 256Mi
`
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())

	// A secret supplied at submit shadows the manifest's DB_HOST.
	job, err := c.Submit(context.Background(), []byte(doc), map[string]string{"DB_HOST": "secret-db"})
	if err != nil {
		t.Fatalf("Submit(sdk): %v", err)
	}
	run, _ := c.LatestRun(job.ID)

	res := fake.LastResources(run.ContainerID)
	if res == nil || res.CPU != "500m" || res.Memory != "256Mi" {
		t.Fatalf("resources not threaded: %+v", res)
	}
	env := fake.LastEnv(run.ContainerID)
	if env["LOG_LEVEL"] != "info" {
		t.Errorf("manifest env lost: LOG_LEVEL=%q", env["LOG_LEVEL"])
	}
	if env["DB_HOST"] != "secret-db" {
		t.Errorf("secret env should override manifest: DB_HOST=%q, want secret-db", env["DB_HOST"])
	}
}

// A malformed resource quantity is rejected at submit — no launch.
func TestSubmitSDK_InvalidResources(t *testing.T) {
	const doc = `kind: sdk
name: bad
image: my-registry/bad:v1
resources:
  cpu: not-a-cpu
`
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())
	_, err := c.Submit(context.Background(), []byte(doc), nil)
	if err == nil {
		t.Fatal("expected error for invalid resources.cpu")
	}
	if !strings.Contains(err.Error(), "resources.cpu") {
		t.Errorf("error should name the bad field: %v", err)
	}
	if fake.Launched() != 0 {
		t.Errorf("must not launch on invalid resources, launched %d", fake.Launched())
	}
}

func TestSubmitSDK_MissingImage(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())
	_, err := c.Submit(context.Background(), []byte("kind: sdk\nname: x\n"), nil)
	if err == nil {
		t.Fatal("expected error for sdk manifest without image")
	}
	if fake.Launched() != 0 {
		t.Error("nothing should launch for an invalid sdk manifest")
	}
}

func TestSecretsPassedbutNotPersisted(t *testing.T) {
	fake := backend.NewFake()
	c, st := newController(t, fake, lifecycle.DefaultRestartPolicy())

	job, err := c.Submit(context.Background(), []byte(validSDKManifest), map[string]string{"API_KEY": "s3cr3t"})
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
	if got := stored.Secrets["API_KEY"]; got.Provider != "env" || got.Name != "API_KEY" {
		t.Fatalf("secret ref not persisted: %+v", stored.Secrets)
	}
	if strings.Contains(stored.Secrets["API_KEY"].Name, "s3cr3t") {
		t.Error("resolved secret leaked into persisted refs")
	}
}

func TestSecretRefsBlockRelaunchAfterControllerRestartUntilResolvable(t *testing.T) {
	fake := backend.NewFake()
	st := openStore(t)
	c1 := control.New(control.Options{Store: st, Backend: fake, Image: "img", StopTimeout: time.Second})
	job, err := c1.Submit(context.Background(), []byte(validSDKManifest), map[string]string{"API_KEY": "submit-only"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := c1.LatestRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fake.LastEnv(first.ContainerID)["API_KEY"] != "submit-only" {
		t.Fatalf("initial launch did not use submit env: %v", fake.LastEnv(first.ContainerID))
	}

	c2 := control.New(control.Options{Store: st, Backend: fake, Image: "img", StopTimeout: time.Second})
	_, err = c2.Restart(context.Background(), job.ID)
	if err == nil {
		t.Fatal("expected restart to block when durable secret ref cannot resolve")
	}
	blocked, err := c2.LatestRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Phase != string(lifecycle.Blocked) || blocked.ContainerID != "" || blocked.Stopped != nil {
		t.Fatalf("missing secret should leave an active blocked run: %+v", blocked)
	}
	if strings.Contains(blocked.Error, "submit-only") {
		t.Fatalf("blocked error leaked secret value: %q", blocked.Error)
	}

	t.Setenv("API_KEY", "from-provider")
	if err := c2.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, err := c2.LatestRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Phase != string(lifecycle.Running) || recovered.ContainerID == "" {
		t.Fatalf("resolved secret ref did not relaunch job: %+v", recovered)
	}
	if fake.LastEnv(recovered.ContainerID)["API_KEY"] != "from-provider" {
		t.Fatalf("relaunch did not use provider secret: %v", fake.LastEnv(recovered.ContainerID))
	}
}

func TestSubmitSDKWithUnresolvedSecretRefsBlocksThenRecovers(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.RestartPolicy{MaxAttempts: 2, BaseBackoff: 0})
	job, err := c.SubmitWithSecretRefs(context.Background(), []byte(validSDKManifest), nil, map[string]store.SecretRef{
		"API_KEY": {Provider: "env", Name: "API_KEY"},
	})
	if err == nil {
		t.Fatal("expected launch to block on missing secret ref")
	}
	if job == nil {
		t.Fatal("job should be persisted before blocked launch is reported")
	}
	blocked, err := c.LatestRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Phase != string(lifecycle.Blocked) || fake.Launched() != 0 {
		t.Fatalf("missing secret ref should block without launch: run=%+v launches=%d", blocked, fake.Launched())
	}

	t.Setenv("API_KEY", "resolved")
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	run, err := c.LatestRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Phase != string(lifecycle.Running) || fake.LastEnv(run.ContainerID)["API_KEY"] != "resolved" {
		t.Fatalf("resolved secret ref did not launch with env: run=%+v env=%v", run, fake.LastEnv(run.ContainerID))
	}
}

func TestCancelStopsJob(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())
	job, _ := c.Submit(context.Background(), []byte(validSDKManifest), nil)

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

// Fencing: the controller refuses to launch a second container while a
// run is live — two transactional producers with the same id would break
// exactly-once. (A restart stops the old run first, so it is allowed.)
func TestSingleLiveRunGuard(t *testing.T) {
	fake := backend.NewFake()
	c, st := newController(t, fake, lifecycle.DefaultRestartPolicy())
	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}

	// The container carries the stable job id for txn-id pinning.
	run, _ := c.LatestRun(job.ID)
	if fake.LastEnv(run.ContainerID)["WEIBO_JOB_ID"] != job.ID {
		t.Errorf("WEIBO_JOB_ID not injected: %v", fake.LastEnv(run.ContainerID))
	}

	// Restart while the run is still live must stop the old one first, so
	// there is never more than one active run.
	if _, err := c.Restart(context.Background(), job.ID); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	active := 0
	runs, _ := st.ListRuns(job.ID)
	for _, r := range runs {
		if r.Stopped == nil {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("expected exactly 1 active run, got %d", active)
	}
}

func TestConcurrentRestartsKeepOneActiveRun(t *testing.T) {
	fake := backend.NewFake()
	c, st := newController(t, fake, lifecycle.DefaultRestartPolicy())
	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Restart(context.Background(), job.ID)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Restart: %v", err)
		}
	}

	active := 0
	runs, err := st.ListRuns(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range runs {
		if r.Stopped == nil {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("expected exactly 1 active run after concurrent restarts, got %d", active)
	}
}

func TestLaunchRemovesBackendResourceWhenPersistingContainerFails(t *testing.T) {
	fake := backend.NewFake()
	base := openStore(t)
	st := &failUpdateRunWithTransitionStore{Store: base, err: errors.New("store unavailable")}
	c := control.New(control.Options{
		Store:       st,
		Backend:     fake,
		Image:       "unused-default-image:test",
		StopTimeout: time.Second,
	})

	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err == nil {
		t.Fatal("expected submit to fail when launched run cannot be persisted")
	}
	if job == nil {
		t.Fatal("job should be returned even when launch persistence fails")
	}
	if fake.Launched() != 1 {
		t.Fatalf("expected backend launch before injected store failure, got %d", fake.Launched())
	}
	capacity, err := fake.Capacity(context.Background(), backend.CapacityConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if capacity.RunningContainers != 0 {
		t.Fatalf("launched container was not removed after persistence failure: %+v", capacity)
	}
	run, err := base.LatestRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run == nil || run.Phase != string(lifecycle.Starting) || run.ContainerID != "" {
		t.Fatalf("expected only the pre-launch starting run to remain, got %+v", run)
	}
}

func TestReconcileReattachesUnrecordedBackendRun(t *testing.T) {
	fake := backend.NewFake()
	st := openStore(t)
	now := time.Now().UTC()
	job := &store.Job{
		ID:      "job-reattach",
		Name:    "orders-sdk",
		Kind:    store.KindSDK,
		Image:   "my-registry/orders-sdk:v1",
		Spec:    validSDKManifest,
		Desired: store.DesiredRunning,
		Created: now,
		Updated: now,
	}
	if err := st.CreateJob(job); err != nil {
		t.Fatal(err)
	}
	run := &store.Run{ID: "run-reattach", JobID: job.ID, Phase: string(lifecycle.Starting), Attempt: 1, Started: now}
	if err := st.CreateRun(run); err != nil {
		t.Fatal(err)
	}
	containerID, err := fake.Launch(context.Background(), backend.LaunchSpec{JobID: job.ID, Name: job.Name, Image: job.Image, ControlPort: 8080})
	if err != nil {
		t.Fatal(err)
	}

	c := control.New(control.Options{
		Store:       st,
		Backend:     fake,
		Image:       "unused-default-image:test",
		StopTimeout: time.Second,
	})
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ContainerID != containerID || got.Phase != string(lifecycle.Running) || got.HostPort == 0 {
		t.Fatalf("run was not reattached: %+v", got)
	}
}

func openStore(t *testing.T) *store.SQLite {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

type failUpdateRunWithTransitionStore struct {
	store.Store
	err error
}

func (s *failUpdateRunWithTransitionStore) UpdateRunWithTransition(*store.Run, *store.Transition) error {
	return s.err
}

func TestRestartLaunchesNewRun(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())
	job, _ := c.Submit(context.Background(), []byte(validSDKManifest), nil)
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
	job, _ := c.Submit(context.Background(), []byte(validSDKManifest), nil)

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

func TestInitialLaunchFailureRetriesOnReconcile(t *testing.T) {
	fake := backend.NewFake()
	fake.LaunchErr = backend.TransientLaunchErrorf("temporary backend unavailable")
	c, _ := newController(t, fake, lifecycle.RestartPolicy{MaxAttempts: 2, BaseBackoff: 0})

	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err == nil {
		t.Fatal("expected submit to report the launch failure")
	}
	run, err := c.LatestRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Phase != string(lifecycle.Restarting) || run.RestartAt == nil || run.Stopped != nil {
		t.Fatalf("initial launch failure was not scheduled for retry: %+v", run)
	}
	if fake.Launched() != 0 {
		t.Fatalf("failed launch should not create a container, got %d", fake.Launched())
	}

	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	retried, err := c.LatestRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Phase != string(lifecycle.Running) || retried.Attempt != 2 || retried.ContainerID == "" {
		t.Fatalf("scheduled launch retry did not start a new run: %+v", retried)
	}
	if fake.Launched() != 1 {
		t.Fatalf("expected one successful backend launch, got %d", fake.Launched())
	}
}

func TestInitialPermanentLaunchFailureDoesNotRetry(t *testing.T) {
	fake := backend.NewFake()
	fake.LaunchErr = backend.PermanentLaunchErrorf("pull access denied for image")
	c, _ := newController(t, fake, lifecycle.RestartPolicy{MaxAttempts: 2, BaseBackoff: 0})

	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err == nil {
		t.Fatal("expected submit to report the launch failure")
	}
	run, err := c.LatestRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Phase != string(lifecycle.Failed) || run.RestartAt != nil || run.Stopped == nil {
		t.Fatalf("permanent launch failure should be terminal: %+v", run)
	}
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.Launched() != 0 {
		t.Fatalf("permanent launch failure should not retry, got %d launches", fake.Launched())
	}
}

func TestInitialLaunchFailureExhaustsRetryPolicy(t *testing.T) {
	fake := backend.NewFake()
	fake.LaunchErr = backend.TransientLaunchErrorf("temporary backend unavailable")
	c, _ := newController(t, fake, lifecycle.RestartPolicy{MaxAttempts: 1, BaseBackoff: 0})

	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err == nil {
		t.Fatal("expected submit to report the launch failure")
	}
	run, err := c.LatestRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Phase != string(lifecycle.Failed) || run.RestartAt != nil || run.Stopped == nil {
		t.Fatalf("exhausted launch failure should be terminal: %+v", run)
	}
}

func TestReconcileBackoffDoesNotBlockOtherJobs(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.RestartPolicy{MaxAttempts: 2, BaseBackoff: 250 * time.Millisecond})
	job1, _ := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	job2, _ := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	run1, _ := c.LatestRun(job1.ID)
	run2, _ := c.LatestRun(job2.ID)
	fake.SetPhase(run1.ContainerID, backend.PhaseExited, 1)

	started := time.Now()
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= 100*time.Millisecond {
		t.Fatalf("Reconcile blocked for restart backoff: %v", elapsed)
	}
	if fake.Launched() != 2 {
		t.Fatalf("restart launched before backoff elapsed: launches=%d", fake.Launched())
	}
	pending, _ := c.LatestRun(job1.ID)
	if pending.Phase != string(lifecycle.Restarting) || pending.RestartAt == nil {
		t.Fatalf("failed run was not durably scheduled: %+v", pending)
	}
	healthy, _ := c.LatestRun(job2.ID)
	if healthy.ID != run2.ID || healthy.Phase != string(lifecycle.Running) {
		t.Fatalf("healthy job was not reconciled: %+v", healthy)
	}
}

func TestScheduledRestartSurvivesControllerRestart(t *testing.T) {
	fake := backend.NewFake()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	policy := lifecycle.RestartPolicy{MaxAttempts: 2, BaseBackoff: 20 * time.Millisecond}
	c1 := control.New(control.Options{Store: st, Backend: fake, Restart: policy, StopTimeout: time.Second})
	job, _ := c1.Submit(context.Background(), []byte(validSDKManifest), nil)
	run, _ := c1.LatestRun(job.ID)
	fake.SetPhase(run.ContainerID, backend.PhaseExited, 1)
	if err := c1.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	time.Sleep(30 * time.Millisecond)
	c2 := control.New(control.Options{Store: st, Backend: fake, Restart: policy, StopTimeout: time.Second})
	if err := c2.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.Launched() != 2 {
		t.Fatalf("scheduled restart was lost across controller restart: launches=%d", fake.Launched())
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
	job, _ := c1.Submit(context.Background(), []byte(validSDKManifest), nil)
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
