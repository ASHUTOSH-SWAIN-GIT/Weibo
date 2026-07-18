// Package store persists the control plane's durable state: the jobs a
// user submitted and the container runs launched for them. The store is
// the source of truth — on a controller restart the reconciler rebuilds
// its view from here, so a crash never loses track of a running job.
//
// Secrets are never stored. A job's spec is persisted with its ${VAR}
// placeholders intact; resolved secret values live only in process memory
// (see the controller) and in the launched container's environment.
package store

import (
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/compiler"
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
	ID       string                       `json:"id"`
	Name     string                       `json:"name"`
	Spec     string                       `json:"spec"` // raw workflow doc, secrets unresolved
	Delivery compiler.DeliveryGuarantee   `json:"delivery"`
	Graph    compiler.PipelineGraph       `json:"graph"`
	Desired  DesiredState                 `json:"desiredState"`
	Created  time.Time                    `json:"createdAt"`
	Updated  time.Time                    `json:"updatedAt"`
}

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
	Started     time.Time  `json:"startedAt"`
	Stopped     *time.Time `json:"stoppedAt,omitempty"`
}

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
	UpdateRun(r *Run) error
	GetRun(id string) (*Run, error)
	// LatestRun returns the most recent run for a job, or (nil, nil) if
	// the job has never been launched.
	LatestRun(jobID string) (*Run, error)
	ListRuns(jobID string) ([]*Run, error)
	// ActiveRuns returns every non-terminal run across all jobs — the set
	// the reconciler must watch.
	ActiveRuns() ([]*Run, error)

	AppendTransition(t *Transition) error
	ListTransitions(jobID string) ([]*Transition, error)

	Close() error
}
