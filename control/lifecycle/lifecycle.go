// Package lifecycle defines the controller's view of a job's phases and
// the rules that move a job between them. It is deliberately small: the
// jobagent inside each container owns the fine-grained execution state;
// the controller only needs a coarse phase per run plus a restart policy.
package lifecycle

import (
	"slices"
	"time"
)

// Phase is the controller-level state of a run. It is a superset of the
// jobagent phases: the controller adds Submitted (accepted but not yet
// launched) and Cancelled (a user-requested stop, distinct from a job
// that finished on its own).
type Phase string

const (
	Submitted  Phase = "submitted"
	Starting   Phase = "starting"
	Running    Phase = "running"
	Cancelling Phase = "cancelling"
	Cancelled  Phase = "cancelled"
	Finished   Phase = "finished"
	Failed     Phase = "failed"
)

// Terminal reports whether the phase is an end state (no container is or
// should be running).
func (p Phase) Terminal() bool {
	switch p {
	case Cancelled, Finished, Failed:
		return true
	}
	return false
}

// valid maps each phase to the phases it may move to. Used to guard and
// audit transitions; an unknown edge is a programming error, not user
// input, so callers may log rather than hard-fail.
var valid = map[Phase][]Phase{
	Submitted:  {Starting, Failed, Cancelled},
	Starting:   {Running, Failed, Cancelling},
	Running:    {Cancelling, Finished, Failed},
	Cancelling: {Cancelled, Finished, Failed},
	// terminal states may only re-enter Starting via an explicit restart,
	// which creates a NEW run rather than transitioning the old one.
}

// CanTransition reports whether from → to is a legal edge.
func CanTransition(from, to Phase) bool {
	return slices.Contains(valid[from], to)
}

// FromAgent maps a jobagent phase string to a controller Phase. Unknown
// values map to Failed so a garbled report never looks healthy.
func FromAgent(agentPhase string) Phase {
	switch Phase(agentPhase) {
	case Starting:
		return Starting
	case Running:
		return Running
	case Cancelling:
		return Cancelling
	case Finished:
		return Finished
	case Failed:
		return Failed
	default:
		return Failed
	}
}

// RestartPolicy bounds automatic restarts of a job whose desired state is
// running but whose container exited unexpectedly.
type RestartPolicy struct {
	MaxAttempts int           // 0 disables automatic restart
	BaseBackoff time.Duration // backoff = BaseBackoff * 2^(attempt-1), capped
	MaxBackoff  time.Duration
}

// DefaultRestartPolicy is a conservative default: a few tries with
// exponential backoff.
func DefaultRestartPolicy() RestartPolicy {
	return RestartPolicy{MaxAttempts: 5, BaseBackoff: 2 * time.Second, MaxBackoff: time.Minute}
}

// ShouldRestart reports whether a run that just reached `phase` on its
// `attempt` should be restarted. Only unexpected failures restart; a
// clean Finished or a user Cancelled does not.
func (p RestartPolicy) ShouldRestart(phase Phase, attempt int) bool {
	if p.MaxAttempts <= 0 {
		return false
	}
	return phase == Failed && attempt < p.MaxAttempts
}

// Backoff returns how long to wait before the given attempt number
// (1-based). Exponential, capped at MaxBackoff.
func (p RestartPolicy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.BaseBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if p.MaxBackoff > 0 && d >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	return d
}
