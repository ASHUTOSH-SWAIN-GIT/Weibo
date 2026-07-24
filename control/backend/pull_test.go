package backend

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// TestResolveRegistryAuth checks that Docker credentials are resolved from the
// host's config for the image's registry (honoring a `docker login`'d host),
// including the Docker Hub → index-URL key mapping, with no auth for unknown
// registries or unparseable refs.
func TestResolveRegistryAuth(t *testing.T) {
	dir := t.TempDir()
	enc := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	cfg := `{"auths":{` +
		`"myreg.example.com":{"auth":"` + enc("user:pass") + `"},` +
		`"https://index.docker.io/v1/":{"auth":"` + enc("hub:secret") + `"}` +
		`}}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	// Override the config dir directly: docker/cli caches the default dir on
	// first use, so a DOCKER_CONFIG env change wouldn't take effect if another
	// test already resolved credentials.
	old := dockerConfigDir
	dockerConfigDir = dir
	t.Cleanup(func() { dockerConfigDir = old })

	d := &Docker{}
	tests := []struct {
		name    string
		ref     string
		wantSet bool
	}{
		{"private registry", "myreg.example.com/team/job:v1", true},
		{"docker hub (index-url key)", "alice/job:latest", true},
		{"unknown registry", "ghcr.io/nobody/job:v1", false},
		{"unparseable ref", "::::", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := d.resolveRegistryAuth(tc.ref)
			if (got != "") != tc.wantSet {
				t.Errorf("resolveRegistryAuth(%q): set=%v, want %v (got %q)", tc.ref, got != "", tc.wantSet, got)
			}
		})
	}
}

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
	job := k.buildJob("run1", "job1", "pvc1", "", "", "myreg/img:v1", 8080, "", PullAlways)
	pod := job.Spec.Template.Spec

	if len(pod.ImagePullSecrets) != 2 ||
		pod.ImagePullSecrets[0].Name != "reg-a" || pod.ImagePullSecrets[1].Name != "reg-b" {
		t.Errorf("imagePullSecrets: %+v", pod.ImagePullSecrets)
	}
	if got := pod.Containers[0].ImagePullPolicy; got != corev1.PullAlways {
		t.Errorf("container ImagePullPolicy: got %q, want %q", got, corev1.PullAlways)
	}
}
