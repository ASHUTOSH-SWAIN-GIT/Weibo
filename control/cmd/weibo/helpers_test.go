package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// --- makeBackend: the docker preflight, against a fake Docker daemon ---

// fakeDockerDaemon points DOCKER_HOST at an httptest server that answers just
// enough of the Docker API for makeBackend's preflight: /_ping (API version
// negotiation + Ping) and /images/json (HasImage). It returns the image-list
// query strings it was asked for.
func fakeDockerDaemon(t *testing.T, images []any) func() []string {
	t.Helper()
	var (
		mu      sync.Mutex
		queries []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/_ping"):
			w.Header().Set("API-Version", "1.47")
			w.Header().Set("OSType", "linux")
			w.WriteHeader(http.StatusOK)
			if r.Method != http.MethodHead {
				_, _ = w.Write([]byte("OK"))
			}
		case strings.HasSuffix(r.URL.Path, "/images/json"):
			mu.Lock()
			queries = append(queries, r.URL.RawQuery)
			mu.Unlock()
			writeJSONResp(w, http.StatusOK, images)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("DOCKER_HOST", "tcp://"+strings.TrimPrefix(srv.URL, "http://"))
	for _, k := range []string{"DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "DOCKER_API_VERSION"} {
		t.Setenv(k, "")
	}
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), queries...)
	}
}

func runMakeBackend(kind, image string) (gotBackend bool, code int) {
	be, code := makeBackend(context.Background(), kind, image, "", "", nil, "", "", "", "", "", nil, nil, nil, "")
	return be != nil, code
}

// A missing default runner image must NOT stop the controller from starting:
// it is only used by plain YAML workflow jobs, and an SDK-only host never
// builds it. (It used to be fatal, which blocked SDK-only deployments.)
func TestMakeBackend_MissingRunnerImageWarnsButStarts(t *testing.T) {
	queries := fakeDockerDaemon(t, []any{}) // daemon reports no such image

	var ok bool
	code, _, stderr := capture(t, func() int {
		var c int
		ok, c = runMakeBackend("docker", "weibo-runner:test")
		return c
	})
	if code != 0 || !ok {
		t.Fatalf("code=%d backend=%v; a missing runner image must not abort startup (stderr: %s)", code, ok, stderr)
	}
	for _, want := range []string{`runner image "weibo-runner:test" not found`, "plain YAML workflow jobs will fail", "docker build -f Dockerfile.runner -t weibo-runner:test ."} {
		if !strings.Contains(stderr, want) {
			t.Errorf("warning missing %q; stderr:\n%s", want, stderr)
		}
	}
	// The preflight asked the daemon about exactly this image.
	if q := queries(); len(q) == 0 || !strings.Contains(q[0], "weibo-runner") {
		t.Errorf("image-list queries = %v, want one filtering on the runner image", q)
	}
}

func TestMakeBackend_PresentRunnerImageIsSilent(t *testing.T) {
	fakeDockerDaemon(t, []any{map[string]any{"Id": "sha256:abc", "RepoTags": []string{"weibo-runner:test"}}})

	var ok bool
	code, _, stderr := capture(t, func() int {
		var c int
		ok, c = runMakeBackend("docker", "weibo-runner:test")
		return c
	})
	if code != 0 || !ok || strings.TrimSpace(stderr) != "" {
		t.Fatalf("code=%d backend=%v stderr=%q, want a clean start", code, ok, stderr)
	}
}

func TestMakeBackend_DockerDaemonUnreachableStillFails(t *testing.T) {
	// A dead daemon IS fatal (unlike a missing image): nothing can launch.
	t.Setenv("DOCKER_HOST", "tcp://127.0.0.1:1")
	for _, k := range []string{"DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "DOCKER_API_VERSION"} {
		t.Setenv(k, "")
	}
	var ok bool
	code, _, stderr := capture(t, func() int {
		var c int
		ok, c = runMakeBackend("docker", "weibo-runner:test")
		return c
	})
	if code != 1 || ok || !strings.Contains(stderr, "Docker daemon not reachable") {
		t.Fatalf("code=%d backend=%v stderr=%q", code, ok, stderr)
	}
}

func TestMakeBackend_UnknownBackend(t *testing.T) {
	var ok bool
	code, _, stderr := capture(t, func() int {
		var c int
		ok, c = runMakeBackend("nomad", "img")
		return c
	})
	if code != 2 || ok || !strings.Contains(stderr, `unknown backend "nomad"`) {
		t.Fatalf("code=%d backend=%v stderr=%q", code, ok, stderr)
	}
}

// --- main.go helpers ---

func TestEnvBool(t *testing.T) {
	for _, v := range []string{"1", "true", "TRUE", "t", "yes", "Y", "on", "  on  "} {
		t.Setenv("WEIBO_TEST_BOOL", v)
		if !envBool("WEIBO_TEST_BOOL") {
			t.Errorf("envBool(%q) = false, want true", v)
		}
	}
	for _, v := range []string{"", "0", "false", "no", "off", "maybe"} {
		t.Setenv("WEIBO_TEST_BOOL", v)
		if envBool("WEIBO_TEST_BOOL") {
			t.Errorf("envBool(%q) = true, want false", v)
		}
	}
}

func TestIsPublicBind(t *testing.T) {
	tests := map[string]bool{
		":9000":          true, // all interfaces
		"0.0.0.0:9000":   true,
		"[::]:9000":      true,
		"127.0.0.1:9000": false,
		"localhost:9000": false,
		"[::1]:9000":     false,
		"10.0.0.5:9000":  false, // a specific interface is a deliberate choice
		"9000":           false, // not host:port at all
	}
	for addr, want := range tests {
		if got := isPublicBind(addr); got != want {
			t.Errorf("isPublicBind(%q) = %v, want %v", addr, got, want)
		}
	}
}

func TestBrowserURL(t *testing.T) {
	tests := map[string]string{
		":9000":          "http://localhost:9000",
		"0.0.0.0:9000":   "http://localhost:9000",
		"[::]:9000":      "http://localhost:9000",
		"127.0.0.1:8080": "http://127.0.0.1:8080",
		"myhost:80":      "http://myhost:80",
	}
	for addr, want := range tests {
		if got := browserURL(addr); got != want {
			t.Errorf("browserURL(%q) = %q, want %q", addr, got, want)
		}
	}
}

func TestInt32PtrFromNonNegative(t *testing.T) {
	if got := int32PtrFromNonNegative(-1); got != nil {
		t.Errorf("-1 must mean unset (nil), got %d", *got)
	}
	if got := int32PtrFromNonNegative(0); got == nil || *got != 0 {
		t.Errorf("0 is a valid TTL, got %v", got)
	}
	if got := int32PtrFromNonNegative(300); got == nil || *got != 300 {
		t.Errorf("300 -> %v", got)
	}
}

func TestUsagePrintsCommands(t *testing.T) {
	_, _, stderr := capture(t, func() int { usage(); return 0 })
	for _, cmd := range []string{"weibo dashboard", "weibo deploy", "weibo jobs", "weibo status", "weibo logs", "weibo cancel", "weibo restart", "weibo savepoint", "weibo delete", "weibo runs"} {
		if !strings.Contains(stderr, cmd) {
			t.Errorf("usage missing %q", cmd)
		}
	}
}

// --- deploy.go helpers ---

func TestParseEnv(t *testing.T) {
	if got, err := parseEnv(nil); got != nil || err != nil {
		t.Fatalf("no pairs = %v, %v; want nil, nil", got, err)
	}
	got, err := parseEnv([]string{"A=1", "B=x=y", "C="})
	if err != nil {
		t.Fatal(err)
	}
	if got["A"] != "1" || got["B"] != "x=y" || got["C"] != "" || len(got) != 3 {
		t.Fatalf("parseEnv = %v (only the first '=' splits; empty values are allowed)", got)
	}
	for _, bad := range []string{"NOEQUALS", "=novalue"} {
		if _, err := parseEnv([]string{bad}); err == nil || !strings.Contains(err.Error(), "want KEY=VAL") {
			t.Errorf("parseEnv(%q) err = %v, want a KEY=VAL error", bad, err)
		}
	}
}

func TestMultiFlag(t *testing.T) {
	var m multiFlag
	for _, v := range []string{"A=1", "B=2"} {
		if err := m.Set(v); err != nil {
			t.Fatal(err)
		}
	}
	if m.String() != "A=1,B=2" || len(m) != 2 {
		t.Fatalf("multiFlag = %v / %q", []string(m), m.String())
	}
}

func TestIndent(t *testing.T) {
	if indent("") != "" {
		t.Error("empty input must stay empty")
	}
	if got := indent("a\nb\n\n"); got != "  a\n  b\n" {
		t.Errorf("indent = %q (trailing blank lines are dropped)", got)
	}
}

func TestIsTerminal(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "notatty")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if isTerminal(f) {
		t.Error("a regular file is not a terminal")
	}
	if runtime.GOOS != "windows" {
		null, err := os.Open(os.DevNull) // a character device, like a TTY
		if err != nil {
			t.Fatal(err)
		}
		defer null.Close()
		if !isTerminal(null) {
			t.Error("a character device should count as a terminal")
		}
	}
	closed, _ := os.CreateTemp(t.TempDir(), "closed")
	closed.Close()
	if isTerminal(closed) {
		t.Error("a closed file (Stat fails) must not count as a terminal")
	}
}

func TestStepAndOkWriteToStderr(t *testing.T) {
	_, stdout, stderr := capture(t, func() int {
		step("building %s", "img")
		ok("built %s", "img")
		return 0
	})
	if stdout != "" {
		t.Errorf("stdout must stay clean for machine-readable output, got %q", stdout)
	}
	if !strings.Contains(stderr, "→ building img") || !strings.Contains(stderr, "✓ built img") {
		t.Errorf("stderr = %q", stderr)
	}
}
