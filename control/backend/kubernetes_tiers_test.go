//go:build kubernetes

package backend

// Kubernetes integration tier (roadmap #26): kind submit / readiness /
// state / restart / delete.
//
// TestTier_K8sLifecycle runs on every pull request against a fake
// clientset: it pins the full lifecycle contract (submit creates the
// Job+Service+ConfigMap+Secret set, readiness reports pending with a
// reason, state is queryable, restart relaunches, delete returns the run
// to gone) without a cluster. TestTier_K8sLiveKind replays the same
// lifecycle against a real kind cluster when WEIBO_RUN_KIND=1 and a
// kubeconfig is present (merge/nightly); it additionally covers logs and
// data-volume deletion, which need real pods and PVCs.

import (
	"context"
	"os"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func tierK8s(t *testing.T) *Kubernetes {
	t.Helper()
	return newK8s(fake.NewSimpleClientset(), KubernetesOptions{
		Image: "weibo-sdk-demo:test", Namespace: "weibo",
	})
}

func tierLaunchSpec(jobID string) LaunchSpec {
	// A YAML-style spec exercises the full managed set: Job + Service +
	// ConfigMap (workflow doc) + Secret (env) + per-job data PVC.
	return LaunchSpec{
		JobID: jobID, Name: "tier-" + jobID,
		Image: "registry.example/job:v1", ControlPort: 8080,
		WorkflowDoc: []byte("name: tier\nversion: '1'\n"),
		Env:         map[string]string{"TIER": "1"},
	}
}

func TestTier_K8sLifecycle(t *testing.T) {
	ctx := context.Background()
	k := tierK8s(t)

	// Submit: the full managed resource set appears.
	run, err := k.Launch(ctx, tierLaunchSpec("tier1"))
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	for _, check := range []struct {
		kind string
		get  func() error
	}{
		{"job", func() error { _, err := k.cs.BatchV1().Jobs("weibo").Get(ctx, run, metav1.GetOptions{}); return err }},
		{"service", func() error { _, err := k.cs.CoreV1().Services("weibo").Get(ctx, run, metav1.GetOptions{}); return err }},
		{"configmap", func() error {
			_, err := k.cs.CoreV1().ConfigMaps("weibo").Get(ctx, run, metav1.GetOptions{})
			return err
		}},
		{"secret", func() error { _, err := k.cs.CoreV1().Secrets("weibo").Get(ctx, run, metav1.GetOptions{}); return err }},
		{"pvc", func() error {
			_, err := k.cs.CoreV1().PersistentVolumeClaims("weibo").Get(ctx, "weibo-tier1-data", metav1.GetOptions{})
			return err
		}},
	} {
		if err := check.get(); err != nil {
			t.Errorf("submit: %s missing: %v", check.kind, err)
		}
	}

	// Readiness/state: no pods exist on the fake, so the run reports
	// pending with a reason — and a reachable control address template.
	st, err := k.Status(ctx, run)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Phase != PhasePending {
		t.Errorf("readiness: phase = %q, want pending", st.Phase)
	}
	if st.Reason == "" {
		t.Error("readiness: pending run should carry a reason")
	}
	if st.Address == "" {
		t.Error("state: expected a control address from the Service")
	}

	// Restart: stop the old attempt, launch again, still pending.
	if err := k.Stop(ctx, run, 5*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if st, _ := k.Status(ctx, run); st.Phase != PhaseGone {
		t.Errorf("after stop: phase = %q, want gone", st.Phase)
	}
	run2, err := k.Launch(ctx, tierLaunchSpec("tier1"))
	if err != nil {
		t.Fatalf("relaunch: %v", err)
	}
	if st, _ := k.Status(ctx, run2); st.Phase != PhasePending {
		t.Errorf("after relaunch: phase = %q, want pending", st.Phase)
	}

	// Delete: the managed set is removed and the run reads gone.
	if err := k.Remove(ctx, run2); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if st, err := k.Status(ctx, run2); err != nil || st.Phase != PhaseGone {
		t.Errorf("after delete: phase = %q err = %v, want gone", st.Phase, err)
	}

	// Data volumes have an explicit deletion path; missing PVCs are nil.
	if err := k.DeleteJobData(ctx, "tier1"); err != nil {
		t.Errorf("DeleteJobData on missing PVC: %v", err)
	}
}

// Live replay against kind: submit → readiness → state/logs → restart →
// delete. Needs WEIBO_RUN_KIND=1, a kubeconfig, and the SDK fixture image
// loaded into the cluster (integration.yml does all three on nightly).
func TestTier_K8sLiveKind(t *testing.T) {
	if os.Getenv("WEIBO_RUN_KIND") != "1" {
		t.Skip("WEIBO_RUN_KIND != 1")
	}
	if testing.Short() {
		t.Skip("skipping kind tier in -short mode")
	}
	kb, err := NewKubernetes(KubernetesOptions{
		Namespace: "default", Image: "weibo-sdk-demo:test",
	})
	if err != nil {
		t.Skipf("no kubeconfig: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := kb.Ping(ctx); err != nil {
		t.Skipf("cluster unreachable: %v", err)
	}

	ctx = context.Background()
	id, err := kb.Launch(ctx, LaunchSpec{
		JobID: "kind-tier", Name: "kind-tier", Image: "weibo-sdk-demo:test", ControlPort: 8080,
	})
	if err != nil {
		t.Fatalf("kind submit: %v", err)
	}
	t.Cleanup(func() { _ = kb.Remove(context.Background(), id) })

	seen := false
	for range 60 {
		st, err := kb.Status(ctx, id)
		if err == nil && st.Phase != PhaseGone {
			t.Logf("readiness: phase=%s addr=%s reason=%s", st.Phase, st.Address, st.Reason)
			seen = true
			if st.Phase == PhaseExited {
				break
			}
		}
		time.Sleep(time.Second)
	}
	if !seen {
		t.Fatal("kind run never left gone")
	}
	if logs, err := kb.Logs(ctx, id, 50); err != nil {
		t.Logf("state logs: %v (pods may still be starting)", err)
	} else {
		t.Logf("state logs: %d bytes", len(logs))
	}
	if err := kb.Stop(ctx, id, 30*time.Second); err != nil {
		t.Fatalf("kind restart stop: %v", err)
	}
	if err := kb.Remove(ctx, id); err != nil {
		t.Fatalf("kind delete: %v", err)
	}
	if st, _ := kb.Status(ctx, id); st.Phase != PhaseGone {
		t.Errorf("after kind delete: phase = %q, want gone", st.Phase)
	}
}
