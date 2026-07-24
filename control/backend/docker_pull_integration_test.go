package backend

import (
	"context"
	"testing"
	"time"
)

// dockerReady skips unless a local Docker daemon is reachable.
func dockerReady(t *testing.T) *Docker {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Docker integration test in -short mode")
	}
	d, err := NewDocker("")
	if err != nil {
		t.Skipf("no docker client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := d.Ping(ctx); err != nil {
		t.Skipf("docker daemon not reachable: %v", err)
	}
	return d
}

// TestPullImage_Live exercises the real pull path against a running daemon:
// an anonymous pull of a small public image, the ifnotpresent no-op once it is
// local, and the never no-op for an absent image.
func TestPullImage_Live(t *testing.T) {
	d := dockerReady(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const img = "busybox:latest"
	if err := d.pullImage(ctx, img, PullAlways); err != nil {
		t.Fatalf("PullAlways %s: %v", img, err)
	}
	// Now present locally → ifnotpresent must be a no-op, not an error.
	if err := d.pullImage(ctx, img, PullIfNotPresent); err != nil {
		t.Fatalf("PullIfNotPresent %s (present): %v", img, err)
	}
	// Never must not pull or error, even for an absent image.
	if err := d.pullImage(ctx, "example.invalid/nope:v1", PullNever); err != nil {
		t.Fatalf("PullNever absent: %v", err)
	}
}
