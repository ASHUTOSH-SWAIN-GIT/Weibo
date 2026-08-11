package control_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

const defaultSDKTestImage = "weibo-sdk-demo:test"

func sdkTestImage() string {
	if image := os.Getenv("WEIBO_SDK_TEST_IMAGE"); image != "" {
		return image
	}
	return defaultSDKTestImage
}

// dockerReady skips unless Docker and the prebuilt SDK integration image are
// available. CI builds examples/sdk-demo as weibo-sdk-demo:test first.
func dockerReady(t *testing.T) (*backend.Docker, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Docker integration test in -short mode")
	}
	image := sdkTestImage()
	d, err := backend.NewDocker(image)
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := d.Ping(ctx); err != nil {
		t.Skipf("docker daemon unreachable: %v", err)
	}
	if ok, err := d.HasImage(ctx, image); err != nil || !ok {
		t.Skipf("SDK integration image %q is not built", image)
	}
	return d, image
}

// TestIntegration_SDKImageLifecycle is the product-level Docker gate:
// submit an SDK image, launch it, expose it in infrastructure inventory,
// reconcile completion, restart it, and preserve the job across a controller
// restart. No generic runner or declarative workflow is involved.
func TestIntegration_SDKImageLifecycle(t *testing.T) {
	docker, image := dockerReady(t)
	ctx := context.Background()
	st, err := store.OpenSQLite(t.TempDir() + "/control.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	ctrl := control.New(control.Options{Store: st, Backend: docker})
	manifest := fmt.Sprintf("kind: sdk\nname: integration-sdk\nimage: %s\n", image)
	job, err := ctrl.Submit(ctx, []byte(manifest), map[string]string{"TEST_RUN": "1"})
	if err != nil {
		t.Fatalf("submit SDK image: %v", err)
	}
	defer cleanupSDKJob(docker, st, job.ID)
	if job.Kind != store.KindSDK || job.Image != image {
		t.Fatalf("wrong persisted SDK job: %+v", job)
	}

	first, _ := ctrl.LatestRun(job.ID)
	if first == nil || first.ContainerID == "" {
		t.Fatal("SDK submission did not launch a container")
	}

	snap, err := docker.Capacity(ctx, backend.CapacityConfig{DefaultJobCPU: "1", DefaultJobMemory: "1Gi"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range snap.Containers {
		if c.JobID == job.ID {
			found = true
			if c.Image != image || !c.Managed || c.ImageID == "" {
				t.Fatalf("incomplete infrastructure record: %+v", c)
			}
		}
	}
	if !found {
		t.Fatalf("launched SDK container absent from infrastructure snapshot: %+v", snap.Containers)
	}

	if !waitFor(20*time.Second, func() bool {
		_ = ctrl.Reconcile(ctx)
		run, _ := ctrl.LatestRun(job.ID)
		return run != nil && run.Stopped != nil
	}) {
		t.Fatal("SDK container did not reach a terminal state")
	}

	if _, err := ctrl.Restart(ctx, job.ID); err != nil {
		t.Fatalf("restart SDK job: %v", err)
	}
	second, _ := ctrl.LatestRun(job.ID)
	if second == nil || second.ID == first.ID || second.ContainerID == first.ContainerID {
		t.Fatalf("restart did not create a new SDK run: first=%+v second=%+v", first, second)
	}

	ctrl2 := control.New(control.Options{Store: st, Backend: docker})
	if err := ctrl2.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	jobs, _ := ctrl2.ListJobs()
	if len(jobs) != 1 || jobs[0].ID != job.ID || jobs[0].Kind != store.KindSDK {
		t.Fatalf("controller restart lost SDK job: %+v", jobs)
	}
}

func waitFor(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(150 * time.Millisecond)
	}
	return false
}

func cleanupSDKJob(d *backend.Docker, st store.Store, jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runs, _ := st.ListRuns(jobID)
	for _, run := range runs {
		if strings.TrimSpace(run.ContainerID) == "" {
			continue
		}
		_ = d.Stop(ctx, run.ContainerID, time.Second)
		_ = d.Remove(ctx, run.ContainerID)
	}
}
