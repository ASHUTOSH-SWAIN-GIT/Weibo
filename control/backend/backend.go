// Package backend abstracts where a job's container actually runs. The
// controller speaks only to this interface, so the same submit/run/cancel
// logic drives local Docker today (docker.go) and Kubernetes later (P6)
// without change.
package backend

import (
	"context"
	"time"
)

// Phase is the coarse container lifecycle as the backend sees it —
// distinct from the richer job lifecycle the controller tracks.
type Phase string

const (
	// PhaseRunning: the container process is up.
	PhaseRunning Phase = "running"
	// PhaseExited: the container process has stopped (see ExitCode).
	PhaseExited Phase = "exited"
	// PhaseGone: no such container (never launched, or removed).
	PhaseGone Phase = "gone"
)

// LaunchSpec is everything the backend needs to start one job container.
type LaunchSpec struct {
	JobID string // used to name the container and its data volume
	Name  string // human-readable workflow name
	Image string // runner image, e.g. mailer-runner:dev

	// WorkflowDoc is the raw workflow file content. The backend makes it
	// available to the container and points WORKFLOW at it.
	WorkflowDoc []byte

	// Env is the resolved runtime environment (including any secrets).
	// The backend passes it to the container; it is never persisted.
	Env map[string]string

	// ControlPort is the container port the jobagent listens on. The
	// backend publishes it and reports the reachable host port in Status.
	ControlPort int

	// RestoreSavepoint, if set, names a savepoint the runner seeds from
	// before starting (RESTORE_SAVEPOINT). Empty means a fresh start.
	RestoreSavepoint string
}

// Status is a point-in-time container status.
type Status struct {
	Phase    Phase
	ExitCode int    // meaningful when Phase == PhaseExited
	HostPort int    // reachable host port mapped to ControlPort, 0 if none
	Address  string // host:port for the control surface, empty if unreachable
}

// ContainerBackend launches and manages one container per job.
type ContainerBackend interface {
	// Launch starts a container and returns its backend-specific ID. The
	// data volume is reused across launches of the same JobID so state
	// and checkpoints survive a restart.
	Launch(ctx context.Context, spec LaunchSpec) (containerID string, err error)
	// Stop requests a graceful stop (SIGTERM), waiting up to timeout
	// before killing. Safe to call on an already-stopped container.
	Stop(ctx context.Context, containerID string, timeout time.Duration) error
	// Status reports the container's current phase and control address.
	Status(ctx context.Context, containerID string) (Status, error)
	// Logs returns up to tail lines of the container's stdout+stderr.
	Logs(ctx context.Context, containerID string, tail int) (string, error)
	// Remove deletes the container (not its data volume).
	Remove(ctx context.Context, containerID string) error
}
