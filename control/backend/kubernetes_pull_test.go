//go:build kubernetes

package backend

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// TestK8sPullPolicy maps LaunchSpec pull policies to k8s policies; unknown/empty
// leaves it to the cluster default.
func TestK8sPullPolicy(t *testing.T) {
	cases := map[string]corev1.PullPolicy{
		PullAlways:       corev1.PullAlways,
		PullNever:        corev1.PullNever,
		PullIfNotPresent: corev1.PullIfNotPresent,
		"":               "",
		"bogus":          "",
	}
	for in, want := range cases {
		if got := k8sPullPolicy(in); got != want {
			t.Errorf("k8sPullPolicy(%q)=%q, want %q", in, got, want)
		}
	}
}

// TestBuildJob_PullSecretsAndPolicy verifies imagePullSecrets and the container
// ImagePullPolicy are set from the backend config / spec.
func TestBuildJob_PullSecretsAndPolicy(t *testing.T) {
	k := &Kubernetes{namespace: "default", imagePullSecrets: []string{"reg-a", "reg-b"}}
	job := k.buildJob("run1", "job1", "pvc1", "", "", "myreg/img:v1", 8080, "", PullAlways, nil)
	pod := job.Spec.Template.Spec

	if len(pod.ImagePullSecrets) != 2 ||
		pod.ImagePullSecrets[0].Name != "reg-a" || pod.ImagePullSecrets[1].Name != "reg-b" {
		t.Errorf("imagePullSecrets: %+v", pod.ImagePullSecrets)
	}
	if got := pod.Containers[0].ImagePullPolicy; got != corev1.PullAlways {
		t.Errorf("container ImagePullPolicy: got %q, want %q", got, corev1.PullAlways)
	}
}
