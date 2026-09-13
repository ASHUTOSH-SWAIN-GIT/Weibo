package control

import (
	"context"
	"strings"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
)

// SweepReport describes one SweepOrphans pass: backend resources the store
// no longer references (deleted jobs, failed removes) that were removed,
// plus running unknowns left alone for an operator to inspect.
type SweepReport struct {
	Removed []string `json:"removed"`
	// RunningOrphans are managed containers with no store reference that
	// are still running. They are reported, never removed: deleting a
	// live unknown could kill a job another controller owns.
	RunningOrphans []string `json:"runningOrphans,omitempty"`
	Errors         []string `json:"errors,omitempty"`
}

// SweepOrphans removes labeled backend resources the store no longer
// references and reports what it did. Call it at controller startup (and
// periodically, if desired) so crashed deletes, failed removes, and
// out-of-band backend state cannot accumulate forever.
//
// A managed container is an orphan when its job ID has no job row, or when
// no run of its job references its backend ID (e.g. a restart's Remove
// failed after the new run was recorded). Exited/gone orphans are removed;
// running orphans are only reported.
func (c *Controller) SweepOrphans(ctx context.Context) (SweepReport, error) {
	var rep SweepReport
	snap, err := c.backend.Capacity(ctx, c.capacity)
	if err != nil {
		return rep, err
	}
	jobs, err := c.store.ListJobs()
	if err != nil {
		return rep, err
	}
	knownJobs := make(map[string]bool, len(jobs))
	for _, j := range jobs {
		knownJobs[j.ID] = true
	}
	// known containers per job: every run ever recorded (active or
	// terminal — a stopped run's container is kept for logs until
	// retention pruning or job deletion removes it).
	known := map[string]map[string]bool{}
	for _, j := range jobs {
		runs, err := c.store.ListRuns(j.ID)
		if err != nil {
			return rep, err
		}
		m := make(map[string]bool, len(runs))
		for _, r := range runs {
			if r.ContainerID != "" {
				m[r.ContainerID] = true
			}
		}
		known[j.ID] = m
	}
	for _, ctr := range snap.Containers {
		if !ctr.Managed {
			continue
		}
		if ctr.ID == "" {
			continue
		}
		refs, ok := known[ctr.JobID]
		if !ok || !referenced(refs, ctr.ID) {
			st, err := c.backend.Status(ctx, ctr.ID)
			if err != nil {
				rep.Errors = append(rep.Errors, ctr.ID+": status: "+err.Error())
				continue
			}
			switch st.Phase {
			case backend.PhaseRunning, backend.PhasePending:
				rep.RunningOrphans = append(rep.RunningOrphans, ctr.ID)
				continue
			}
			if err := c.backend.Remove(ctx, ctr.ID); err != nil {
				rep.Errors = append(rep.Errors, ctr.ID+": remove: "+err.Error())
				continue
			}
			rep.Removed = append(rep.Removed, ctr.ID)
		}
	}
	return rep, nil
}

// referenced reports whether backendID is one of the container IDs the
// store knows for a job. Docker's Capacity snapshot truncates IDs to 12
// characters, so a stored full ID with the snapshot as a prefix counts.
func referenced(known map[string]bool, backendID string) bool {
	if known[backendID] {
		return true
	}
	for id := range known {
		if strings.HasPrefix(id, backendID) {
			return true
		}
	}
	return false
}
