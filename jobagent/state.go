// Package jobagent supervises a single Weibo job in-process and exposes
// its lifecycle over HTTP. One agent owns one *weibo.StreamExecutionEnv:
// it runs Execute in a goroutine, tracks lifecycle transitions, serves
// /state, /describe, /metrics, /healthz, and translates POST /cancel into
// a graceful context cancellation.
//
// This is the control contract every runner embeds (see the weibo-runner
// binary and the job-orchestration plan, phase P1). It adds no container
// or scheduling concerns — those live in the separate control plane.
package jobagent

import "time"

// Phase is the coarse lifecycle state of a supervised job. It only ever
// moves forward: Starting → Running → (Cancelling) → Finished | Failed.
type Phase string

const (
	// PhaseStarting is set from construction until Execute is entered.
	PhaseStarting Phase = "starting"
	// PhaseRunning means Execute is in progress.
	PhaseRunning Phase = "running"
	// PhaseCancelling means a cancel was requested and the pipeline is
	// draining; it still ends in Finished (clean drain) or Failed.
	PhaseCancelling Phase = "cancelling"
	// PhaseFinished is a clean terminal state: the source drained or a
	// cancel completed its graceful shutdown without error.
	PhaseFinished Phase = "finished"
	// PhaseFailed is a terminal state: Execute returned a non-cancel error.
	PhaseFailed Phase = "failed"
)

// Terminal reports whether the phase is an end state.
func (p Phase) Terminal() bool {
	return p == PhaseFinished || p == PhaseFailed
}

// State is a point-in-time snapshot of a job, returned by GET /state.
// Record counts are read from the process Prometheus counters at snapshot
// time (one job per process, so the cumulative counters are the job's
// totals); checkpoint fields are populated by the engine checkpoint
// listener.
type State struct {
	Phase      Phase     `json:"phase"`
	StartedAt  time.Time `json:"startedAt"`
	Uptime     string    `json:"uptime"`
	RecordsIn  int64     `json:"recordsIn"`
	RecordsOut int64     `json:"recordsOut"`

	// CurrentCheckpointID is the ID of the most recently completed
	// checkpoint, empty until the first one completes.
	CurrentCheckpointID string `json:"currentCheckpointId,omitempty"`
	// LastCheckpointAt is when the agent observed that checkpoint
	// complete (nil until the first one).
	LastCheckpointAt *time.Time `json:"lastCheckpointAt,omitempty"`

	// LastError is the terminal error message when Phase is Failed.
	LastError string `json:"lastError,omitempty"`
}
