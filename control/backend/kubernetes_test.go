//go:build kubernetes

package backend

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func k8sBackend(t *testing.T) *Kubernetes {
	t.Helper()
	return newK8s(fake.NewSimpleClientset(), KubernetesOptions{Image: "weibo-sdk-demo:test", Namespace: "weibo"})
}

// Launch must create the full object set with correct config.
func TestK8sLaunch_CreatesObjects(t *testing.T) {
	ctx := context.Background()
	k := k8sBackend(t)

	id, err := k.Launch(ctx, LaunchSpec{
		JobID:            "abc123",
		Name:             "orders-sdk",
		Image:            "registry/orders:v2",
		Env:              map[string]string{"WEIBO_JOB_ID": "abc123", "API_KEY": "s3cr3t"},
		ControlPort:      8080,
		RestoreSavepoint: "before-upgrade",
	})
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}

	// PVC (per job).
	if _, err := k.cs.CoreV1().PersistentVolumeClaims("weibo").Get(ctx, "weibo-abc123-data", metav1.GetOptions{}); err != nil {
		t.Fatalf("pvc not created: %v", err)
	}
	// SDK images carry their pipeline and do not create a workflow ConfigMap.
	if _, err := k.cs.CoreV1().ConfigMaps("weibo").Get(ctx, id, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("SDK launch should not create a configmap: %v", err)
	}
	// Secret holds the env (never inline in the pod spec).
	sec, err := k.cs.CoreV1().Secrets("weibo").Get(ctx, id, metav1.GetOptions{})
	if err != nil || sec.StringData["API_KEY"] != "s3cr3t" {
		t.Fatalf("secret wrong: %v", err)
	}
	// Service for the control surface.
	if _, err := k.cs.CoreV1().Services("weibo").Get(ctx, id, metav1.GetOptions{}); err != nil {
		t.Fatalf("service not created: %v", err)
	}

	// The Job's pod spec.
	job, err := k.cs.BatchV1().Jobs("weibo").Get(ctx, id, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("job not created: %v", err)
	}
	if bl := job.Spec.BackoffLimit; bl == nil || *bl != 0 {
		t.Errorf("backoffLimit must be 0 (weibo owns restarts), got %v", bl)
	}
	pod := job.Spec.Template.Spec
	if pod.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy: got %q", pod.RestartPolicy)
	}
	// fsGroup lets nonroot write the PVC.
	if sc := pod.SecurityContext; sc == nil || sc.FSGroup == nil || *sc.FSGroup != nonRootUID {
		t.Errorf("fsGroup not set to nonroot: %+v", pod.SecurityContext)
	}
	c := pod.Containers[0]
	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	if env["WORKFLOW"] != "" || env["DATA_DIR"] != k8sDataDir ||
		env["SAVEPOINT_DIR"] != k8sSavepointDir || env["PORT"] != "8080" {
		t.Errorf("fixed env wrong: %v", env)
	}
	if env["RESTORE_SAVEPOINT"] != "before-upgrade" {
		t.Errorf("RESTORE_SAVEPOINT not wired: %v", env)
	}
	if len(c.EnvFrom) != 1 || c.EnvFrom[0].SecretRef == nil || c.EnvFrom[0].SecretRef.Name != id {
		t.Errorf("envFrom secret not wired: %+v", c.EnvFrom)
	}
	if c.LivenessProbe == nil || c.LivenessProbe.HTTPGet == nil || c.LivenessProbe.HTTPGet.Path != "/healthz" {
		t.Errorf("liveness probe not on /healthz: %+v", c.LivenessProbe)
	}
	if len(c.VolumeMounts) != 1 {
		t.Errorf("expected only the data mount for SDK jobs, got %d", len(c.VolumeMounts))
	}
}

// Env is optional: no Secret and no envFrom when Env is empty.
func TestK8sLaunch_NoEnvNoSecret(t *testing.T) {
	ctx := context.Background()
	k := k8sBackend(t)
	id, err := k.Launch(ctx, LaunchSpec{JobID: "j1", Image: "registry/job:v1", ControlPort: 8080})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.cs.CoreV1().Secrets("weibo").Get(ctx, id, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("no secret expected when env empty, got %v", err)
	}
	job, _ := k.cs.BatchV1().Jobs("weibo").Get(ctx, id, metav1.GetOptions{})
	if len(job.Spec.Template.Spec.Containers[0].EnvFrom) != 0 {
		t.Error("no envFrom expected when env empty")
	}
}

func TestK8sStatus_Phases(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		js   batchv1.JobStatus
		want Phase
	}{
		{"active", batchv1.JobStatus{Active: 1}, PhaseRunning},
		{"succeeded", batchv1.JobStatus{Succeeded: 1}, PhaseExited},
		{"failed", batchv1.JobStatus{Failed: 1}, PhaseExited},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cs := fake.NewSimpleClientset(&batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "weibo-x", Namespace: "weibo"},
				Status:     c.js,
			})
			k := newK8s(cs, KubernetesOptions{Namespace: "weibo"})
			st, err := k.Status(ctx, "weibo-x")
			if err != nil {
				t.Fatal(err)
			}
			if st.Phase != c.want {
				t.Errorf("phase: got %q want %q", st.Phase, c.want)
			}
		})
	}
}

func TestK8sStatus_Gone(t *testing.T) {
	k := k8sBackend(t)
	st, err := k.Status(context.Background(), "does-not-exist")
	if err != nil || st.Phase != PhaseGone {
		t.Fatalf("expected gone, got %q err=%v", st.Phase, err)
	}
}

func TestK8sStop_DeletesJob(t *testing.T) {
	ctx := context.Background()
	k := k8sBackend(t)
	id, _ := k.Launch(ctx, LaunchSpec{JobID: "j2", Image: "registry/job:v1", ControlPort: 8080})
	if err := k.Stop(ctx, id, 30*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := k.cs.BatchV1().Jobs("weibo").Get(ctx, id, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("job should be deleted, got %v", err)
	}
	// Stop on an already-gone job is not an error.
	if err := k.Stop(ctx, id, time.Second); err != nil {
		t.Errorf("Stop on missing job: %v", err)
	}
}

// Remove cleans up run resources but keeps the per-job PVC.
func TestK8sRemove_KeepsPVC(t *testing.T) {
	ctx := context.Background()
	k := k8sBackend(t)
	id, _ := k.Launch(ctx, LaunchSpec{JobID: "j3", Image: "registry/job:v1", Env: map[string]string{"A": "b"}, ControlPort: 8080})

	if err := k.Remove(ctx, id); err != nil {
		t.Fatal(err)
	}
	for _, check := range []func() error{
		func() error { _, e := k.cs.BatchV1().Jobs("weibo").Get(ctx, id, metav1.GetOptions{}); return e },
		func() error { _, e := k.cs.CoreV1().Services("weibo").Get(ctx, id, metav1.GetOptions{}); return e },
		func() error { _, e := k.cs.CoreV1().ConfigMaps("weibo").Get(ctx, id, metav1.GetOptions{}); return e },
		func() error { _, e := k.cs.CoreV1().Secrets("weibo").Get(ctx, id, metav1.GetOptions{}); return e },
	} {
		if err := check(); !apierrors.IsNotFound(err) {
			t.Errorf("resource should be removed, got %v", err)
		}
	}
	// PVC survives.
	if _, err := k.cs.CoreV1().PersistentVolumeClaims("weibo").Get(ctx, "weibo-j3-data", metav1.GetOptions{}); err != nil {
		t.Errorf("PVC must survive Remove: %v", err)
	}
}

// The PVC is reused across launches of the same job (restart recovery).
func TestK8sLaunch_ReusesPVC(t *testing.T) {
	ctx := context.Background()
	k := k8sBackend(t)
	if _, err := k.Launch(ctx, LaunchSpec{JobID: "j4", Image: "registry/job:v1", ControlPort: 8080}); err != nil {
		t.Fatal(err)
	}
	// Second launch of the same job must not fail on the existing PVC.
	if _, err := k.Launch(ctx, LaunchSpec{JobID: "j4", Image: "registry/job:v1", ControlPort: 8080}); err != nil {
		t.Fatalf("second Launch (PVC reuse): %v", err)
	}
}
