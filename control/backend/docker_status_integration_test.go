package backend

import (
	"context"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
)

// TestDockerStatus_Paused verifies that Status() distinguishes a
// `docker pause`d container from a healthy running one (issue: paused
// containers were reporting as PhaseRunning indefinitely).
func TestDockerStatus_Paused(t *testing.T) {
	d := dockerReady(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const img = "busybox:latest"
	if err := d.pullImage(ctx, img, PullIfNotPresent); err != nil {
		t.Fatalf("pull %s: %v", img, err)
	}

	created, err := d.cli.ContainerCreate(ctx,
		&container.Config{Image: img, Cmd: []string{"sleep", "60"}},
		&container.HostConfig{}, nil, nil, "")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() {
		_ = d.cli.ContainerRemove(context.Background(), created.ID, container.RemoveOptions{Force: true})
	})
	if err := d.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		t.Fatalf("start: %v", err)
	}

	st, err := d.Status(ctx, created.ID)
	if err != nil {
		t.Fatalf("status (running): %v", err)
	}
	if st.Phase != PhaseRunning {
		t.Fatalf("expected PhaseRunning before pause, got %q", st.Phase)
	}

	if err := d.cli.ContainerPause(ctx, created.ID); err != nil {
		t.Fatalf("pause: %v", err)
	}
	st, err = d.Status(ctx, created.ID)
	if err != nil {
		t.Fatalf("status (paused): %v", err)
	}
	if st.Phase != PhaseUnhealthy {
		t.Fatalf("expected PhaseUnhealthy for a paused container, got %q (reason %q)", st.Phase, st.Reason)
	}
	if st.Reason == "" {
		t.Error("expected a non-empty reason for the unhealthy status")
	}

	if err := d.cli.ContainerUnpause(ctx, created.ID); err != nil {
		t.Fatalf("unpause: %v", err)
	}
	st, err = d.Status(ctx, created.ID)
	if err != nil {
		t.Fatalf("status (unpaused): %v", err)
	}
	if st.Phase != PhaseRunning {
		t.Fatalf("expected PhaseRunning after unpause, got %q", st.Phase)
	}
}
