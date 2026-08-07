package backend

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
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
