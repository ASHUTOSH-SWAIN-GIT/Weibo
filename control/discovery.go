package control

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
)

// maxDiscoveryProbes bounds how many backend status probes run
// concurrently while resolving live control addresses.
const maxDiscoveryProbes = 8

// DiscoveryTarget is one Prometheus http_sd target: the job agent's
// address plus stable job labels. Run/container IDs are deliberately
// absent so targets stay stable across restarts (only the address
// changes); the run phase is informational.
type DiscoveryTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// DiscoveryTargets lists every job with a reachable live control surface
// in Prometheus http_sd format, for scraping agent /metrics directly.
// Resolution is bounded: at most maxDiscoveryProbes concurrent backend
// probes under an overall 15s deadline; unreachable jobs are skipped, a
// backend/store failure aborts the listing with an error.
func (c *Controller) DiscoveryTargets(ctx context.Context) ([]DiscoveryTarget, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	runs, err := c.store.ActiveRuns()
	if err != nil {
		return nil, err
	}
	sem := make(chan struct{}, maxDiscoveryProbes)
	var mu sync.Mutex
	var wg sync.WaitGroup
	out := []DiscoveryTarget{}
	for _, run := range runs {
		if run.ContainerID == "" {
			continue
		}
		wg.Add(1)
		go func(jobID, containerID string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			job, err := c.store.GetJob(jobID)
			if err != nil {
				return
			}
			st, err := c.backend.Status(ctx, containerID)
			if err != nil || st.Address == "" {
				return
			}
			if st.Phase != backend.PhaseRunning && st.Phase != backend.PhasePending {
				return
			}
			mu.Lock()
			out = append(out, DiscoveryTarget{
				Targets: []string{st.Address},
				Labels: map[string]string{
					"weibo_job_id":    job.ID,
					"weibo_job_name":  job.Name,
					"weibo_run_phase": string(st.Phase),
				},
			})
			mu.Unlock()
		}(run.JobID, run.ContainerID)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil && len(out) == 0 {
		return out, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Targets[0] < out[j].Targets[0] })
	return out, nil
}
