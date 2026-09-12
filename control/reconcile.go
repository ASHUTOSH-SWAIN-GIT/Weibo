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
		unlock := c.lockJob(run.JobID)
		current, err := c.store.GetRun(run.ID)
		if err != nil || current.Stopped != nil {
			unlock()
			if err != nil {
				return err
			}
			continue
		}
		run = current
		job, err := c.store.GetJob(run.JobID)
		if err != nil {
			unlock()
			continue // job deleted out from under a run; skip
		}
		if lifecycle.Phase(run.Phase) == lifecycle.Restarting {
			err := c.maybeRestart(ctx, job, run)
			unlock()
			if err != nil {
				return err
			}
			continue
		}
		if run.ContainerID == "" {
			err := c.reattachUnrecordedBackendRun(ctx, job, run)
			unlock()
			if err != nil {
				return err
			}
			continue
		}
		st, err := c.backend.Status(ctx, run.ContainerID)
		if err != nil {
			c.logf("reconcile: status %s: %v", run.ContainerID, err)
			unlock()
			continue
		}
		err = c.reconcileRun(ctx, job, run, st)
		unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) reattachUnrecordedBackendRun(ctx context.Context, job *store.Job, run *store.Run) error {
	container, ok, err := c.findManagedContainer(ctx, job.ID)
	if err != nil || !ok {
		return err
	}
	st, err := c.backend.Status(ctx, container.ID)
	if err != nil {
		return err
	}
	if st.Phase == backend.PhaseGone {
		return nil
	}
	run.ContainerID = container.ID
	if st.HostPort != 0 {
		run.HostPort = st.HostPort
	}
	switch st.Phase {
	case backend.PhaseRunning:
		from := lifecycle.Phase(run.Phase)
		run.Phase = string(lifecycle.Running)
		return c.store.UpdateRunWithTransition(run, transitionRecord(job.ID, run.ID, from, lifecycle.Running, "reattached backend resource"))
	case backend.PhasePending:
		run.Phase = string(lifecycle.Starting)
		run.Error = st.Reason
	default:
		return c.reconcileRun(ctx, job, run, st)
	}
	return c.store.UpdateRun(run)
}

func (c *Controller) findManagedContainer(ctx context.Context, jobID string) (backend.ContainerStats, bool, error) {
	snap, err := c.backend.Capacity(ctx, c.capacity)
	if err != nil {
		return backend.ContainerStats{}, false, err
	}
	var best backend.ContainerStats
	found := false
	for _, container := range snap.Containers {
		if !container.Managed || container.JobID != jobID || container.ID == "" {
			continue
		}
		if !found || container.StartedAt > best.StartedAt {
			best = container
			found = true
		}
	}
	return best, found, nil
}

func (c *Controller) reconcileRun(ctx context.Context, job *store.Job, run *store.Run, st backend.Status) error {
	switch st.Phase {
	case backend.PhasePending:
		if job.Desired == store.DesiredStopped {
			if err := c.backend.Stop(ctx, run.ContainerID, c.stopTimeout); err != nil {
				run.Error = err.Error()
				if updateErr := c.store.UpdateRun(run); updateErr != nil {
					return updateErr
				}
				return nil
			}
			return c.finishRun(run, lifecycle.Starting, lifecycle.Cancelled, "desired stopped")
		}
		if run.Phase != string(lifecycle.Starting) || run.Error != st.Reason {
			run.Phase, run.Error = string(lifecycle.Starting), st.Reason
			if err := c.store.UpdateRun(run); err != nil {
				return err
			}
		}
	case backend.PhaseRunning:
		// If the operator asked it to stop, stop it.
		if job.Desired == store.DesiredStopped {
			if err := c.backend.Stop(ctx, run.ContainerID, c.stopTimeout); err != nil {
				run.Error = err.Error()
				if updateErr := c.store.UpdateRun(run); updateErr != nil {
					return updateErr
				}
				return nil
			}
			return c.finishRun(run, lifecycle.Running, lifecycle.Cancelled, "desired stopped")
		}
		// Keep the live host port fresh (e.g. after a controller restart).
		if run.Phase != string(lifecycle.Running) || (st.HostPort != 0 && run.HostPort != st.HostPort) {
			run.Phase = string(lifecycle.Running)
			if st.HostPort != 0 {
				run.HostPort = st.HostPort
			}
			if err := c.store.UpdateRun(run); err != nil {
				return err
			}
		}

	case backend.PhaseExited:
		return c.handleExit(ctx, job, run, st.ExitCode)

	case backend.PhaseGone:
		// The container vanished (e.g. host reboot removed it).
		if job.Desired == store.DesiredStopped {
			return c.finishRun(run, lifecycle.Running, lifecycle.Cancelled, "container gone; desired stopped")
		}
		if c.restart.ShouldRestart(lifecycle.Failed, run.Attempt) {
			if err := c.scheduleRestart(job, run, "container gone"); err != nil {
				return err
			}
			return c.maybeRestart(ctx, job, run)
		} else {
			return c.finishRun(run, lifecycle.Running, lifecycle.Failed, "container gone")
		}
	}
	return nil
}

// handleExit records a stopped container's terminal phase and applies the
// restart policy when the job should still be running.
func (c *Controller) handleExit(ctx context.Context, job *store.Job, run *store.Run, exitCode int) error {
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
		if err := c.scheduleRestart(job, run, reason); err != nil {
			return err
		}
		return c.maybeRestart(ctx, job, run)
	}
	return c.finishRun(run, lifecycle.Running, to, reason)
}

// maybeRestart launches a fresh run if the restart policy allows it.
func (c *Controller) maybeRestart(ctx context.Context, job *store.Job, run *store.Run) error {
	if job.Desired != store.DesiredRunning {
		return nil
	}
	if !c.restart.ShouldRestart(lifecycle.Failed, run.Attempt) {
		c.logf("job %s: not restarting (attempt %d, policy exhausted)", job.ID, run.Attempt)
		return nil
	}
	if run.RestartAt != nil && time.Now().UTC().Before(*run.RestartAt) {
		return nil
	}
	if err := c.finishRun(run, lifecycle.Restarting, lifecycle.Failed, "restart backoff elapsed"); err != nil {
		return err
	}
	if err := c.launchLocked(ctx, job, run.Attempt+1, ""); err != nil {
		c.logf("job %s: restart launch failed: %v", job.ID, err)
		return err
	}
	return nil
}

func (c *Controller) scheduleRestart(job *store.Job, run *store.Run, reason string) error {
	return c.scheduleRestartFrom(job, run, lifecycle.Running, reason)
}

func (c *Controller) scheduleRestartFrom(job *store.Job, run *store.Run, from lifecycle.Phase, reason string) error {
	at := time.Now().UTC().Add(c.restart.Backoff(run.Attempt))
	run.Phase = string(lifecycle.Restarting)
	run.Error = reason
	run.RestartAt = &at
	return c.store.UpdateRunWithTransition(run, transitionRecord(job.ID, run.ID, from, lifecycle.Restarting, reason))
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
