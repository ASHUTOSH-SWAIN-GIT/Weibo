package control

import (
	"context"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
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
		if lifecycle.Phase(run.Phase) == lifecycle.Restarting {
			c.maybeRestart(ctx, job, run)
			continue
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
	case backend.PhasePending:
		if job.Desired == store.DesiredStopped {
			if err := c.backend.Stop(ctx, run.ContainerID, c.stopTimeout); err != nil {
				run.Error = err.Error()
				_ = c.store.UpdateRun(run)
				return
			}
			c.finishRun(run, lifecycle.Starting, lifecycle.Cancelled, "desired stopped")
			return
		}
		if run.Phase != string(lifecycle.Starting) || run.Error != st.Reason {
			run.Phase, run.Error = string(lifecycle.Starting), st.Reason
			_ = c.store.UpdateRun(run)
		}
	case backend.PhaseRunning:
		// If the operator asked it to stop, stop it.
		if job.Desired == store.DesiredStopped {
			if err := c.backend.Stop(ctx, run.ContainerID, c.stopTimeout); err != nil {
				run.Error = err.Error()
				_ = c.store.UpdateRun(run)
				return
			}
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
		if c.restart.ShouldRestart(lifecycle.Failed, run.Attempt) {
			c.scheduleRestart(job, run, "container gone")
			c.maybeRestart(ctx, job, run)
		} else {
			c.finishRun(run, lifecycle.Running, lifecycle.Failed, "container gone")
		}
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
	if to == lifecycle.Failed && c.restart.ShouldRestart(lifecycle.Failed, run.Attempt) {
		c.scheduleRestart(job, run, reason)
		c.maybeRestart(ctx, job, run)
		return
	}
	c.finishRun(run, lifecycle.Running, to, reason)
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
	if run.RestartAt != nil && time.Now().UTC().Before(*run.RestartAt) {
		return
	}
	c.finishRun(run, lifecycle.Restarting, lifecycle.Failed, "restart backoff elapsed")
	if err := c.launch(ctx, job, run.Attempt+1, ""); err != nil {
		c.logf("job %s: restart launch failed: %v", job.ID, err)
	}
}

func (c *Controller) scheduleRestart(job *store.Job, run *store.Run, reason string) {
	at := time.Now().UTC().Add(c.restart.Backoff(run.Attempt))
	run.Phase = string(lifecycle.Restarting)
	run.Error = reason
	run.RestartAt = &at
	_ = c.store.UpdateRun(run)
	c.transition(job.ID, run.ID, lifecycle.Running, lifecycle.Restarting, reason)
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
