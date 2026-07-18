package control

import (
	"context"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/store"
)

// Reconcile drives every active run toward its job's desired state once.
// It is the single place that observes real container status and updates
// the store — so after a controller restart, calling Reconcile re-attaches
// to the containers the store still knows about (the store is the source
// of truth). Safe to call repeatedly.
func (c *Controller) Reconcile(ctx context.Context) error {
	active, err := c.store.ActiveRuns()
	if err != nil {
		return err
	}
	for _, run := range active {
		job, err := c.store.GetJob(run.JobID)
		if err != nil {
			continue // job deleted out from under a run; skip
		}
		st, err := c.backend.Status(ctx, run.ContainerID)
		if err != nil {
			c.logf("reconcile: status %s: %v", run.ContainerID, err)
			continue
		}
		c.reconcileRun(ctx, job, run, st)
	}
	return nil
}

func (c *Controller) reconcileRun(ctx context.Context, job *store.Job, run *store.Run, st backend.Status) {
	switch st.Phase {
	case backend.PhaseRunning:
		// If the operator asked it to stop, stop it.
		if job.Desired == store.DesiredStopped {
			_ = c.backend.Stop(ctx, run.ContainerID, c.stopTimeout)
			c.finishRun(run, lifecycle.Running, lifecycle.Cancelled, "desired stopped")
			return
		}
		// Keep the live host port fresh (e.g. after a controller restart).
		if run.Phase != string(lifecycle.Running) || (st.HostPort != 0 && run.HostPort != st.HostPort) {
			run.Phase = string(lifecycle.Running)
			if st.HostPort != 0 {
				run.HostPort = st.HostPort
			}
			_ = c.store.UpdateRun(run)
		}

	case backend.PhaseExited:
		c.handleExit(ctx, job, run, st.ExitCode)

	case backend.PhaseGone:
		// The container vanished (e.g. host reboot removed it).
		if job.Desired == store.DesiredStopped {
			c.finishRun(run, lifecycle.Running, lifecycle.Cancelled, "container gone; desired stopped")
			return
		}
		c.finishRun(run, lifecycle.Running, lifecycle.Failed, "container gone")
		c.maybeRestart(ctx, job, run)
	}
}

// handleExit records a stopped container's terminal phase and applies the
// restart policy when the job should still be running.
func (c *Controller) handleExit(ctx context.Context, job *store.Job, run *store.Run, exitCode int) {
	var to lifecycle.Phase
	reason := "container exited"
	switch {
	case job.Desired == store.DesiredStopped:
		to = lifecycle.Cancelled
		reason = "stopped by request"
	case exitCode == 0:
		to = lifecycle.Finished
	default:
		to = lifecycle.Failed
		reason = "nonzero exit"
	}
	c.finishRun(run, lifecycle.Running, to, reason)
	if to == lifecycle.Failed {
		c.maybeRestart(ctx, job, run)
	}
}

// maybeRestart launches a fresh run if the restart policy allows it.
func (c *Controller) maybeRestart(ctx context.Context, job *store.Job, run *store.Run) {
	if job.Desired != store.DesiredRunning {
		return
	}
	if !c.restart.ShouldRestart(lifecycle.Failed, run.Attempt) {
		c.logf("job %s: not restarting (attempt %d, policy exhausted)", job.ID, run.Attempt)
		return
	}
	// Backoff is best-effort here; a scheduled backoff belongs to the
	// loop, but a short sleep keeps a crash-looping job from hammering.
	if d := c.restart.Backoff(run.Attempt); d > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
	if err := c.launch(ctx, job, run.Attempt+1); err != nil {
		c.logf("job %s: restart launch failed: %v", job.ID, err)
	}
}

// RunReconciler runs Reconcile on a ticker until ctx is cancelled. Call it
// in a goroutine from the controller process.
func (c *Controller) RunReconciler(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.Reconcile(ctx); err != nil {
				c.logf("reconcile: %v", err)
			}
		}
	}
}
