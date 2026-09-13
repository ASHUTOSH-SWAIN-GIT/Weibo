// Package store persists the control plane's durable state: the jobs a
// user submitted and the container runs launched for them. The store is
// the source of truth — on a controller restart the reconciler rebuilds
// its view from here, so a crash never loses track of a running job.
//
// Secrets are never stored. A job's spec is persisted with its ${VAR}
// placeholders intact; durable recovery stores only secret references
// (provider/name), never resolved values.
package store

import (
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/compiler"
)

// DesiredState is the operator's intent for a job, independent of what
// any individual container is doing right now. The reconciler drives the
// actual runs toward this.
type DesiredState string

const (
	// DesiredRunning: the job should have a live container.
	DesiredRunning DesiredState = "running"
	// DesiredStopped: the job should be stopped and stay stopped.
	DesiredStopped DesiredState = "stopped"
)

// Job is a submitted workflow and the operator's intent for it.
type Job struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Kind is "yaml" (a declarative workflow) or "sdk" (a prebuilt Go
	// pipeline image). Empty is treated as "yaml".
	Kind string `json:"kind,omitempty"`
	Spec string `json:"spec"` // workflow doc (yaml) or manifest (sdk), secrets unresolved
	// Image is the container image to run. Empty for yaml jobs (the
	// controller's generic runner image is used); set for sdk jobs.
	Image    string                     `json:"image,omitempty"`
	Secrets  map[string]SecretRef       `json:"secrets,omitempty"`
	Delivery compiler.DeliveryGuarantee `json:"delivery"`
	Graph    compiler.PipelineGraph     `json:"graph"`
	Desired  DesiredState               `json:"desiredState"`
	Created  time.Time                  `json:"createdAt"`
	Updated  time.Time                  `json:"updatedAt"`
}

// SecretRef is a durable pointer to a secret value. Name is provider-specific
// and must not contain the secret's resolved value.
type SecretRef struct {
	Provider string `json:"provider"`
	Name     string `json:"name"`
	Key      string `json:"key,omitempty"`
}

// KindSDK / KindYAML are the Job.Kind values.
const (
	KindYAML = "yaml"
	KindSDK  = "sdk"
)

// Run is one container launched for a job. A job accumulates runs across
// restarts; at most one is non-terminal at a time.
type Run struct {
	ID          string     `json:"id"`
	JobID       string     `json:"jobId"`
	ContainerID string     `json:"containerId,omitempty"`
	HostPort    int        `json:"hostPort,omitempty"` // mapped control-surface port
	Phase       string     `json:"phase"`
	Attempt     int        `json:"attempt"`
	Error       string     `json:"error,omitempty"`
	FailureKind string     `json:"failureKind,omitempty"`
	Started     time.Time  `json:"startedAt"`
	Stopped     *time.Time `json:"stoppedAt,omitempty"`
	RestartAt   *time.Time `json:"restartAt,omitempty"`
}

// Run failure categories exposed through the API. These are intentionally
// coarse and non-secret: callers can decide whether a run is retrying because
// infrastructure is unavailable, failed permanently because the request/image
// is invalid, or was blocked waiting for external configuration.
const (
	FailureLaunchTransient = "launch_transient"
	FailureLaunchPermanent = "launch_permanent"
	FailureLaunchRecord    = "launch_record"
	FailureSecretBlocked   = "secret_blocked"
)

// Transition is an append-only lifecycle audit record.
type Transition struct {
	ID     int64     `json:"id"`
	JobID  string    `json:"jobId"`
	RunID  string    `json:"runId,omitempty"`
	From   string    `json:"from"`
	To     string    `json:"to"`
	Reason string    `json:"reason,omitempty"`
	At     time.Time `json:"at"`
}

// Store is the persistence contract. Implementations must be safe for
// concurrent use (the API server and reconciler share one).
type Store interface {
	CreateJob(j *Job) error
	GetJob(id string) (*Job, error)
	ListJobs() ([]*Job, error)
	SetDesired(jobID string, d DesiredState) error
	DeleteJob(id string) error

	CreateRun(r *Run) error
	CreateRunWithTransition(r *Run, t *Transition) error
	UpdateRun(r *Run) error
	UpdateRunWithTransition(r *Run, t *Transition) error
	GetRun(id string) (*Run, error)
	// LatestRun returns the most recent run for a job, or (nil, nil) if
	// the job has never been launched.
	LatestRun(jobID string) (*Run, error)
	ListRuns(jobID string) ([]*Run, error)
	// PruneTerminalRuns deletes terminal runs (stopped_at NOT NULL) for a
	// job, keeping the newest keep rows plus every active run (which is
	// never deleted). Transitions belonging to pruned runs are deleted
	// too. keep <= 0 keeps all terminal runs. It returns the number of
	// runs deleted.
	PruneTerminalRuns(jobID string, keep int) (int64, error)
	// ActiveRuns returns every non-terminal run across all jobs — the set
	// the reconciler must watch.
	ActiveRuns() ([]*Run, error)

	AppendTransition(t *Transition) error
	ListTransitions(jobID string) ([]*Transition, error)

	Close() error
}
