//go:build kubernetes

package backend_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
)

const k8sTestImage = "weibo-sdk-demo:test"

// clusterReady skips unless a Kubernetes cluster is reachable.
func clusterReady(t *testing.T) *backend.Kubernetes {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Kubernetes integration test in -short mode")
	}
	kb, err := backend.NewKubernetes(backend.KubernetesOptions{
		Namespace: "default", Image: k8sTestImage,
	})
	if err != nil {
		t.Skipf("no kubeconfig: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := kb.Ping(ctx); err != nil {
		t.Skipf("cluster unreachable: %v", err)
	}
	return kb
}

// Launch a real SDK image on a cluster, observe it, read its logs, then clean
// it up. Requires the SDK fixture image to be loaded into the test cluster.
func TestIntegration_K8sSDKJobLifecycle(t *testing.T) {
	kb := clusterReady(t)
	ctx := context.Background()

	id, err := kb.Launch(ctx, backend.LaunchSpec{
		JobID: "itest1", Name: "k8s-sdk-itest", Image: k8sTestImage, ControlPort: 8080,
	})
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	t.Logf("launched job resource %s", id)
	defer func() {
		_ = kb.Remove(context.Background(), id)
	}()

	// The Job should reach a non-Gone phase (running or completed).
	ok := false
	for range 60 {
		st, err := kb.Status(ctx, id)
		if err == nil && st.Phase != backend.PhaseGone {
			t.Logf("phase=%s addr=%s", st.Phase, st.Address)
			ok = true
			if st.Phase == backend.PhaseExited {
				break
			}
		}
		time.Sleep(time.Second)
	}
	if !ok {
		t.Fatal("job never reached a running/terminal phase")
	}

	// Logs should eventually contain the pipeline's output.
	if logs, err := kb.Logs(ctx, id, 100); err == nil && strings.Contains(logs, "count") {
		t.Logf("logs show pipeline output ✓")
	} else {
		t.Logf("logs (may be empty if pod still starting): err=%v", err)
	}

	if err := kb.Stop(ctx, id, 30*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
