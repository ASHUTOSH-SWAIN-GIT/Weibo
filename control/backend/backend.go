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
	Image string // runner image, e.g. weibo-runner:dev

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

	// PullPolicy controls whether the backend pulls Image before launch:
	// PullAlways, PullNever, or PullIfNotPresent (the default when empty).
	PullPolicy string

	// Resources caps the container's CPU/memory. Nil means unlimited
	// (today's behavior); each backend maps it to its native limits.
	Resources *ResourceLimits
}

// ResourceLimits are CPU/memory caps expressed as Kubernetes quantity
// strings (e.g. CPU "500m" or "2", Memory "512Mi" or "1Gi"). An empty
// field means "no limit for this dimension". The controller validates the
// strings before launch, so backends can assume they parse.
type ResourceLimits struct {
	CPU    string
	Memory string
}

// CapacityConfig describes Weibo's scheduling policy for converting backend
// resources into user-facing job slots.
type CapacityConfig struct {
	MaxJobs          int
	DefaultJobCPU    string
	DefaultJobMemory string
}

// CapacitySnapshot is a backend-normalized view of how many Weibo job
// containers can run now. Nil slot values mean the backend cannot determine
// them accurately.
type CapacitySnapshot struct {
	Backend string    `json:"backend"`
	Health  string    `json:"health"`
	Source  string    `json:"source,omitempty"`
	Reason  string    `json:"reason,omitempty"`
	At      time.Time `json:"observedAt"`

	TotalSlots     *int `json:"totalSlots,omitempty"`
	UsedSlots      int  `json:"usedSlots"`
	AvailableSlots *int `json:"availableSlots,omitempty"`
	MaxJobs        int  `json:"maxJobs,omitempty"`

	CPUTotalMilli     int64 `json:"cpuTotalMilli,omitempty"`
	CPUReservedMilli  int64 `json:"cpuReservedMilli,omitempty"`
	CPUAvailableMilli int64 `json:"cpuAvailableMilli,omitempty"`

	MemoryTotalBytes     int64 `json:"memoryTotalBytes,omitempty"`
	MemoryReservedBytes  int64 `json:"memoryReservedBytes,omitempty"`
	MemoryAvailableBytes int64 `json:"memoryAvailableBytes,omitempty"`

	DefaultJobCPUMilli    int64            `json:"defaultJobCPUMilli,omitempty"`
	DefaultJobMemoryBytes int64            `json:"defaultJobMemoryBytes,omitempty"`
	RunningContainers     int              `json:"runningContainers"`
	StartingContainers    int              `json:"startingContainers"`
	ExitedContainers      int              `json:"exitedContainers"`
	UnhealthyContainers   int              `json:"unhealthyContainers"`
	Unsupported           bool             `json:"unsupported,omitempty"`
	Host                  *HostStats       `json:"host,omitempty"`
	Containers            []ContainerStats `json:"containers,omitempty"`
}

// HostStats is live utilization for the machine running the Docker daemon.
type HostStats struct {
	Hostname         string  `json:"hostname,omitempty"`
	OperatingSystem  string  `json:"operatingSystem,omitempty"`
	Architecture     string  `json:"architecture,omitempty"`
	KernelVersion    string  `json:"kernelVersion,omitempty"`
	DockerVersion    string  `json:"dockerVersion,omitempty"`
	CPUCores         int     `json:"cpuCores"`
	CPUPercent       float64 `json:"cpuPercent"`
	MemoryUsedBytes  int64   `json:"memoryUsedBytes"`
	MemoryTotalBytes int64   `json:"memoryTotalBytes"`
	MemoryPercent    float64 `json:"memoryPercent"`
	Load1            float64 `json:"load1"`
	Load5            float64 `json:"load5"`
	Load15           float64 `json:"load15"`
}

// ContainerStats is a live compute snapshot for one Weibo-managed container.
type ContainerStats struct {
	ID               string  `json:"id"`
	Name             string  `json:"name"`
	JobID            string  `json:"jobId,omitempty"`
	Managed          bool    `json:"managed"`
	Image            string  `json:"image,omitempty"`
	ImageID          string  `json:"imageId,omitempty"`
	State            string  `json:"state"`
	CPUPercent       float64 `json:"cpuPercent"`
	MemoryUsedBytes  uint64  `json:"memoryUsedBytes"`
	MemoryLimitBytes uint64  `json:"memoryLimitBytes"`
	MemoryPercent    float64 `json:"memoryPercent"`
	NetworkRxBytes   uint64  `json:"networkRxBytes"`
	NetworkTxBytes   uint64  `json:"networkTxBytes"`
	BlockReadBytes   uint64  `json:"blockReadBytes"`
	BlockWriteBytes  uint64  `json:"blockWriteBytes"`
	PIDs             uint64  `json:"pids"`
	StartedAt        int64   `json:"startedAt,omitempty"`
}

// Pull policies for LaunchSpec.PullPolicy.
const (
	// PullIfNotPresent pulls only when the image is absent locally. Default.
	// Local-only images (e.g. a dev-built runner tag) are never pulled.
	PullIfNotPresent = "ifnotpresent"
	// PullAlways always attempts a pull before launch.
	PullAlways = "always"
	// PullNever never pulls; the image must already be present.
	PullNever = "never"
)

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
	// Capacity reports backend health and job-slot capacity.
	Capacity(ctx context.Context, cfg CapacityConfig) (CapacitySnapshot, error)
}
