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
func (c *Controller) Reconcile(ctx context.Context) (err error) {
	start := time.Now()
	ctx, span := c.tracing().Start(ctx, "controller.reconcile")
	defer func() {
		if err != nil {
			span.RecordError(err)
		}
		span.End()
		c.metrics.ObserveReconcile(time.Since(start), err)
	}()
	return c.reconcile(ctx)
}

// reconcile drives every active run toward its desired state once. A
// per-run error (a store hiccup, a transient status probe failure, ...) is
// logged and skipped rather than aborting the pass: one bad row must not
// stall restart/recovery for every other job in the fleet. Only a failure
// of ActiveRuns() itself — which means the run list can't be trusted at
// all — aborts the whole pass.
func (c *Controller) reconcile(ctx context.Context) error {
	active, err := c.store.ActiveRuns()
	if err != nil {
		return err
	}
	for _, run := range active {
		unlock := c.lockJob(run.JobID)
		current, err := c.store.GetRun(run.ID)
		if err != nil {
			c.log().Warn("reconcile: get run", "run", run.ID, "job", run.JobID, "error", err)
			unlock()
			continue
		}
		if current.Stopped != nil {
			unlock()
			continue
		}
		run = current
		job, err := c.store.GetJob(run.JobID)
		if err != nil {
			unlock()
			continue // job deleted out from under a run; skip
		}
		if lifecycle.Phase(run.Phase) == lifecycle.Blocked {
			if err := c.maybeUnblock(ctx, job, run); err != nil {
				c.log().Warn("reconcile: unblock", "run", run.ID, "job", job.ID, "error", err)
			}
			unlock()
			continue
		}
		if lifecycle.Phase(run.Phase) == lifecycle.Restarting {
			if err := c.maybeRestart(ctx, job, run); err != nil {
				c.log().Warn("reconcile: restart", "run", run.ID, "job", job.ID, "error", err)
			}
			unlock()
			continue
		}
		if run.ContainerID == "" {
			if err := c.reattachUnrecordedBackendRun(ctx, job, run); err != nil {
				c.log().Warn("reconcile: reattach", "run", run.ID, "job", job.ID, "error", err)
			}
			unlock()
			continue
		}
		st, err := c.backend.Status(ctx, run.ContainerID)
		if err != nil {
			c.log().Warn("reconcile status probe", "container", run.ContainerID, "error", err)
			unlock()
			continue
		}
		if err := c.reconcileRun(ctx, job, run, st); err != nil {
			c.log().Warn("reconcile: run", "run", run.ID, "job", job.ID, "error", err)
		}
		unlock()
	}
	return nil
}

func (c *Controller) maybeUnblock(ctx context.Context, job *store.Job, run *store.Run) error {
	if job.Desired != store.DesiredRunning {
		return nil
	}
	if _, err := c.launchEnv(job); err != nil {
		if run.Error != err.Error() {
			run.Error = err.Error()
			return c.store.UpdateRun(run)
		}
		return nil
	}
	if err := c.finishRun(run, lifecycle.Blocked, lifecycle.Failed, "secret references resolved; retrying launch"); err != nil {
		return err
	}
	return c.launchLocked(ctx, job, run.Attempt+1, "")
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
		// Keep the live host port fresh (e.g. after a controller restart),
		// and clear an "unhealthy" mark left by a prior pause once the
		// container is confirmed running again.
		recovered := run.FailureKind == store.FailureContainerUnhealthy
		if run.Phase != string(lifecycle.Running) || (st.HostPort != 0 && run.HostPort != st.HostPort) || recovered {
			run.Phase = string(lifecycle.Running)
			if st.HostPort != 0 {
				run.HostPort = st.HostPort
			}
			if recovered {
				run.Error = ""
				run.FailureKind = ""
			}
			if err := c.store.UpdateRun(run); err != nil {
				return err
			}
		}

	case backend.PhaseUnhealthy:
		// A frozen container (e.g. Docker-paused) is not making progress
		// but hasn't exited, so it isn't safe to auto-restart on the
		// operator's behalf — surface it distinctly instead of leaving
		// the run silently reporting "running".
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
		if run.Error != st.Reason || run.FailureKind != store.FailureContainerUnhealthy {
			c.log().Warn("reconcile: container unhealthy", "job", job.ID, "run", run.ID, "container", run.ContainerID, "reason", st.Reason)
			run.Error = st.Reason
			run.FailureKind = store.FailureContainerUnhealthy
			if err := c.store.UpdateRun(run); err != nil {
				return err
			}
		}

	case backend.PhaseExited:
		return c.handleExit(ctx, job, run, st.ExitCode, st.OOMKilled)

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
func (c *Controller) handleExit(ctx context.Context, job *store.Job, run *store.Run, exitCode int, oomKilled bool) error {
	var to lifecycle.Phase
	reason := "container exited"
	failureKind := ""
	switch {
	case job.Desired == store.DesiredStopped:
		to = lifecycle.Cancelled
		reason = "stopped by request"
	case exitCode == 0:
		to = lifecycle.Finished
	case oomKilled:
		// Distinct from a generic nonzero exit: an operator needs "raise
		// the memory limit", not "check the logs for a bug" — and without
		// this, OOM kills are indistinguishable from any other crash and
		// the job goes silently dead once restart attempts run out.
		to = lifecycle.Failed
		reason = "out of memory"
		failureKind = store.FailureOOMKilled
	default:
		to = lifecycle.Failed
		reason = "nonzero exit"
	}
	if to == lifecycle.Failed {
		run.FailureKind = failureKind
		run.Error = reason
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
		c.log().Info("restart policy exhausted", "job", job.ID, "attempt", run.Attempt)
		return nil
	}
	if run.RestartAt != nil && time.Now().UTC().Before(*run.RestartAt) {
		return nil
	}
	if err := c.finishRun(run, lifecycle.Restarting, lifecycle.Failed, "restart backoff elapsed"); err != nil {
		return err
	}
	if err := c.launchLocked(ctx, job, run.Attempt+1, ""); err != nil {
		c.log().Warn("restart launch failed", "job", job.ID, "error", err)
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
				c.log().Error("reconcile pass failed", "error", err)
			}
		}
	}
}
