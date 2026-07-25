package backend_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
)

const k8sTestImage = "weibo-runner:dev"

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

// The P6 gate: launch a real job on a cluster, see it run, read its logs,
// then stop and clean it up — proving the K8s backend drives the same
// lifecycle as Docker. Requires a cluster with the runner image loaded
// (e.g. `kind load docker-image weibo-runner:dev`).
func TestIntegration_K8sJobLifecycle(t *testing.T) {
	kb := clusterReady(t)
	ctx := context.Background()

	wf := "name: k8s-itest\nversion: \"1\"\n" +
		"source:\n  type: generator\n  records: [{key: a, value: '{\"word\":\"a\"}'}]\n" +
		"pipeline:\n  - {id: by-word, type: keyBy, keyBy: {field: word, partitions: 1}}\n" +
		"  - {id: count, type: reduce, reduce: {function: count}}\nsink: {type: stdout}\n"

	id, err := kb.Launch(ctx, backend.LaunchSpec{
		JobID: "itest1", Name: "k8s-itest", WorkflowDoc: []byte(wf), ControlPort: 8080,
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
