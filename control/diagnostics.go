package control

import (
	"context"
	"fmt"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

// Runs returns every recorded attempt for a job, newest first.
func (c *Controller) Runs(jobID string) ([]*store.Run, error) {
	return c.store.ListRuns(jobID)
}

// TransitionsPaged returns one audit page, newest first (see
// store.ListTransitionsPaged).
func (c *Controller) TransitionsPaged(jobID string, beforeID int64, limit int) ([]*store.Transition, error) {
	return c.store.ListTransitionsPaged(jobID, beforeID, limit)
}

// RunLogs returns one attempt's container logs. It returns 404 for an
// unknown run and 410 when the container is gone (pruned by retention,
// or never recorded) — the run row itself remains for audit.
func (c *Controller) RunLogs(ctx context.Context, jobID, runID string, tail int) (string, int, error) {
	run, err := c.store.GetRun(runID)
	if err != nil || run.JobID != jobID {
		return "", 404, fmt.Errorf("run not found")
	}
	if run.ContainerID == "" {
		return "", 410, fmt.Errorf("logs unavailable: no container recorded for run")
	}
	st, err := c.backend.Status(ctx, run.ContainerID)
	if err != nil {
		return "", 502, err
	}
	if st.Phase == backend.PhaseGone {
		return "", 410, fmt.Errorf("logs unavailable: container removed")
	}
	out, err := c.backend.Logs(ctx, run.ContainerID, tail)
	if err != nil {
		return "", 502, err
	}
	return out, 200, nil
}

// FailureDiagnosis is a structured, human-readable classification of why
// a job is (or was) unhealthy: a stable machine kind, a short title, an
// operator hint, and the raw message. Nil means "no failure".
type FailureDiagnosis struct {
	Kind    string `json:"kind"`
	Title   string `json:"title"`
	Hint    string `json:"hint"`
	Message string `json:"message,omitempty"`
}

// failureHints maps store failure kinds to operator guidance. Unknown
// kinds fall back to a generic title so new kinds never render blank.
var failureHints = map[string][2]string{
	store.FailureLaunchTransient: {"Temporary launch failure", "The backend hiccupped — the reconciler retries automatically with backoff."},
	store.FailureLaunchPermanent: {"Permanent launch failure", "Needs operator action: check the image reference, manifest, and backend credentials."},
	store.FailureLaunchRecord:    {"Launch bookkeeping failure", "The container started but recording it failed — check controller logs; orphan cleanup ran."},
	store.FailureSecretBlocked:   {"Blocked on secrets", "A durable secret reference cannot resolve — set the missing value and the reconciler retries."},
	"restarting":                 {"Restart scheduled", "The last attempt failed — retrying automatically with backoff."},
}

// DiagnoseFailure classifies a run's health for display. It returns nil
// for runs with nothing to explain (running/finished/cancelled/blocked
// without a recorded failure kind).
func DiagnoseFailure(run *store.Run) *FailureDiagnosis {
	if run == nil {
		return nil
	}
	kind := run.FailureKind
	if kind == "" {
		switch lifecycle.Phase(run.Phase) {
		case lifecycle.Failed:
			kind = "run_failed"
		case lifecycle.Restarting:
			kind = "restarting"
		default:
			return nil
		}
	}
	title, hint := "Run failed", "The container exited unexpectedly — check its logs and transitions."
	if th, ok := failureHints[kind]; ok {
		title, hint = th[0], th[1]
	}
	return &FailureDiagnosis{Kind: kind, Title: title, Hint: hint, Message: run.Error}
}

// Activity is the freshest progress signal for a job: when its agent
// last reported, and the record counters then. Null when the job never
// reported (never launched, or history expired).
type Activity struct {
	At         time.Time `json:"at"`
	RecordsIn  int64     `json:"recordsIn"`
	RecordsOut int64     `json:"recordsOut"`
}

// RestartStatus is a live "retrying in N seconds" view over a run that
// the restart policy parked. Null unless the latest run is restarting
// with a scheduled time.
type RestartStatus struct {
	ScheduledAt time.Time `json:"scheduledAt"`
	InSeconds   int64     `json:"inSeconds"`
}

// CheckpointStatus is the latest checkpoint the job reported, with the
// duration/size diagnostics from the engine observer. Null until the
// first checkpoint completes.
type CheckpointStatus struct {
	ID          string     `json:"id"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	DurationMs  int64      `json:"durationMs,omitempty"`
	SizeBytes   int64      `json:"sizeBytes,omitempty"`
}

// Diagnostics is the assembled "why is my job (un)healthy" view behind
// GET /jobs/{id}/diagnostics.
type Diagnostics struct {
	JobID      string             `json:"jobId"`
	Phase      string             `json:"phase"`
	Desired    store.DesiredState `json:"desired"`
	Failure    *FailureDiagnosis  `json:"failure,omitempty"`
	Activity   *Activity          `json:"activity,omitempty"`
	Restart    *RestartStatus     `json:"restart,omitempty"`
	Checkpoint *CheckpointStatus  `json:"checkpoint,omitempty"`
	RunID      string             `json:"runId,omitempty"`
	Attempt    int                `json:"attempt,omitempty"`
}

// Diagnostics assembles the diagnostic view for a job: failure
// classification from the latest run, last activity + checkpoint state
// from the rolling history, and a live restart countdown when one is
// scheduled.
func (c *Controller) Diagnostics(jobID string) (*Diagnostics, error) {
	job, err := c.store.GetJob(jobID)
	if err != nil {
		return nil, err
	}
	d := &Diagnostics{JobID: job.ID, Desired: job.Desired}
	run, err := c.store.LatestRun(jobID)
	if err != nil {
		return nil, err
	}
	if run != nil {
		d.Phase = run.Phase
		d.RunID = run.ID
		d.Attempt = run.Attempt
		if lifecycle.Phase(run.Phase) == lifecycle.Restarting && run.RestartAt != nil {
			in := int64(time.Until(*run.RestartAt).Seconds() + 0.5)
			if in < 0 {
				in = 0
			}
			d.Restart = &RestartStatus{ScheduledAt: *run.RestartAt, InSeconds: in}
		}
		d.Failure = DiagnoseFailure(run)
	}
	if sample, ok := c.history.Latest(jobID); ok {
		d.Activity = &Activity{At: sample.At, RecordsIn: sample.RecordsIn, RecordsOut: sample.RecordsOut}
		if sample.CheckpointID != "" {
			d.Checkpoint = &CheckpointStatus{
				ID:          sample.CheckpointID,
				CompletedAt: sample.LastCheckpointAt,
				DurationMs:  sample.CheckpointDurationMs,
				SizeBytes:   sample.CheckpointSizeBytes,
			}
		}
	}
	return d, nil
}

// RunDetail is one attempt with its audit slice and, when scheduled, its
// restart countdown — behind GET /jobs/{id}/runs/{runId}.
type RunDetail struct {
	Run         *store.Run          `json:"run"`
	Transitions []*store.Transition `json:"transitions"`
	Restart     *RestartStatus      `json:"restart,omitempty"`
}

// RunDetail returns one run of a job (404 when the run is unknown or
// belongs to another job) with that run's transitions, oldest first.
func (c *Controller) RunDetail(jobID, runID string) (*RunDetail, error) {
	if _, err := c.store.GetJob(jobID); err != nil {
		return nil, err
	}
	run, err := c.store.GetRun(runID)
	if err != nil {
		return nil, err
	}
	if run.JobID != jobID {
		return nil, fmt.Errorf("store: run not found")
	}
	all, err := c.store.ListTransitions(jobID)
	if err != nil {
		return nil, err
	}
	detail := &RunDetail{Run: run}
	for _, t := range all {
		if t.RunID == runID {
			detail.Transitions = append(detail.Transitions, t)
		}
	}
	if detail.Transitions == nil {
		detail.Transitions = []*store.Transition{}
	}
	if lifecycle.Phase(run.Phase) == lifecycle.Restarting && run.RestartAt != nil {
		in := int64(time.Until(*run.RestartAt).Seconds() + 0.5)
		if in < 0 {
			in = 0
		}
		detail.Restart = &RestartStatus{ScheduledAt: *run.RestartAt, InSeconds: in}
	}
	return detail, nil
}
