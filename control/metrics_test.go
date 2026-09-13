package control_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

func scrapeMetrics(t *testing.T, c *control.Controller) string {
	t.Helper()
	srv := httptest.NewServer(c.Metrics().Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestControllerMetricsJobsLaunchesAndRuns(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())

	if _, err := c.Submit(context.Background(), []byte(validSDKManifest), nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	body := scrapeMetrics(t, c)
	for _, want := range []string{
		`weibo_controller_jobs{desired="running"} 1`,
		`weibo_controller_runs{phase="running"} 1`,
		`weibo_controller_launches_total{result="success"} 1`,
		`weibo_controller_reconciles_total{result="success"} 1`,
		`weibo_controller_reconcile_duration_seconds_count 1`,
		`process_cpu_seconds_total`,
		`go_goroutines`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestControllerMetricsLaunchFailureKinds(t *testing.T) {
	fake := backend.NewFake()
	fake.LaunchErr = backend.TransientLaunchErrorf("temporary backend unavailable")
	c, _ := newController(t, fake, lifecycle.RestartPolicy{MaxAttempts: 2, BaseBackoff: 0})
	if _, err := c.Submit(context.Background(), []byte(validSDKManifest), nil); err == nil {
		t.Fatal("expected launch failure")
	}

	fake2 := backend.NewFake()
	c2, _ := newController(t, fake2, lifecycle.DefaultRestartPolicy())
	if _, err := c2.SubmitWithSecretRefs(context.Background(), []byte(validSDKManifest), nil,
		map[string]store.SecretRef{"API_KEY": {Provider: "env", Name: "API_KEY"}}); err == nil {
		t.Fatal("expected blocked launch")
	}

	if body := scrapeMetrics(t, c); !strings.Contains(body, `weibo_controller_launches_total{result="transient"} 1`) {
		t.Errorf("missing transient launch counter:\n%s", body)
	}
	if body := scrapeMetrics(t, c2); !strings.Contains(body, `weibo_controller_launches_total{result="blocked"} 1`) {
		t.Errorf("missing blocked launch counter:\n%s", body)
	}
}

// Controller metric labels must stay low-cardinality: no job, run, or
// container IDs may appear as label values.
func TestControllerMetricsNoUnboundedLabels(t *testing.T) {
	fake := backend.NewFake()
	c, _ := newController(t, fake, lifecycle.DefaultRestartPolicy())
	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}
	run, _ := c.LatestRun(job.ID)
	body := scrapeMetrics(t, c)
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "weibo_controller_") || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.Contains(line, job.ID) || strings.Contains(line, run.ID) || strings.Contains(line, run.ContainerID) {
			t.Errorf("unbounded ID in metric labels: %q", line)
		}
	}
}

func TestDiscoveryTargetsListsLiveJobs(t *testing.T) {
	fake := backend.NewFake()
	st := openStore(t)
	c := control.New(control.Options{Store: st, Backend: fake, Image: "img", StopTimeout: time.Second})
	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}

	targets, err := c.DiscoveryTargets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 {
		t.Fatalf("targets=%+v, want 1", targets)
	}
	run, _ := c.LatestRun(job.ID)
	addr := fakeLastAddress(t, fake, run.ContainerID)
	if len(targets[0].Targets) != 1 || targets[0].Targets[0] != addr {
		t.Errorf("target address=%v, want [%s]", targets[0].Targets, addr)
	}
	if targets[0].Labels["weibo_job_id"] != job.ID || targets[0].Labels["weibo_job_name"] != "orders-sdk" {
		t.Errorf("target labels=%v", targets[0].Labels)
	}

	if err := c.Cancel(context.Background(), job.ID); err != nil {
		t.Fatal(err)
	}
	targets, err = c.DiscoveryTargets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 0 {
		t.Fatalf("stopped job should have no targets, got %+v", targets)
	}
}

func fakeLastAddress(t *testing.T, fake *backend.Fake, containerID string) string {
	t.Helper()
	st, err := fake.Status(context.Background(), containerID)
	if err != nil {
		t.Fatal(err)
	}
	return st.Address
}
