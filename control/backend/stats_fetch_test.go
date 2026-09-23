package backend

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

// newTestDocker points a real *client.Client at an httptest server, so
// fetchContainerStats can be exercised against canned HTTP responses instead
// of a live Docker daemon.
func newTestDocker(t *testing.T, handler http.HandlerFunc) *Docker {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	cli, err := client.NewClientWithOpts(
		client.WithHost(srv.URL),
		client.WithHTTPClient(srv.Client()),
		client.WithVersion("1.44"), // pin the API version: skip the negotiation ping
	)
	if err != nil {
		t.Fatalf("new docker client: %v", err)
	}
	return &Docker{cli: cli}
}

func fakeContainers(n int) []types.Container {
	list := make([]types.Container, n)
	for i := range list {
		list[i] = types.Container{ID: fmt.Sprintf("c%d", i), State: "running"}
	}
	return list
}

// TestFetchContainerStats_Concurrent verifies stats are fetched in parallel,
// not one request at a time: 20 containers each held open by the fake
// daemon for 100ms complete in well under 20*100ms.
func TestFetchContainerStats_Concurrent(t *testing.T) {
	const n = 20
	var inFlight, maxInFlight atomic.Int64
	d := newTestDocker(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/stats") {
			http.NotFound(w, r)
			return
		}
		cur := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			m := maxInFlight.Load()
			if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{})
	})

	start := time.Now()
	results := d.fetchContainerStats(t.Context(), fakeContainers(n))
	elapsed := time.Since(start)

	if len(results) != n {
		t.Fatalf("got %d results, want %d", len(results), n)
	}
	for id, res := range results {
		if res.err != nil {
			t.Errorf("container %s: unexpected error: %v", id, res.err)
		}
		if res.stats == nil {
			t.Errorf("container %s: nil stats", id)
		}
	}
	// Serial would take n*100ms = 2s; bounded-concurrent (cap 8) takes
	// ceil(n/8)*100ms = 300ms. Assert well below serial to catch a
	// regression back to one-at-a-time without being flaky on a slow CI box.
	if elapsed >= 1*time.Second {
		t.Errorf("fetchContainerStats took %v for %d containers (cap %d) — looks serial, not concurrent",
			elapsed, n, maxConcurrentStatsFetches)
	}
	if got := maxInFlight.Load(); got < 2 {
		t.Errorf("max concurrent in-flight requests = %d, want > 1 (no parallelism observed)", got)
	}
	if got := maxInFlight.Load(); got > int64(maxConcurrentStatsFetches) {
		t.Errorf("max concurrent in-flight requests = %d, want <= %d (pool cap not enforced)", got, maxConcurrentStatsFetches)
	}
}

// TestFetchContainerStats_PerContainerError verifies one failing container
// doesn't fail the others, and skips non-running containers entirely (no
// stats call is made for them).
func TestFetchContainerStats_PerContainerError(t *testing.T) {
	d := newTestDocker(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/containers/bad/stats"):
			http.Error(w, "boom", http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/stats"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{})
		default:
			http.NotFound(w, r)
		}
	})

	list := []types.Container{
		{ID: "good", State: "running"},
		{ID: "bad", State: "running"},
		{ID: "stopped", State: "exited"}, // must be skipped: no HTTP call for it
	}
	results := d.fetchContainerStats(t.Context(), list)

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2 (running containers only): %v", len(results), results)
	}
	if res, ok := results["good"]; !ok || res.err != nil || res.stats == nil {
		t.Errorf("good: got %+v, want stats with no error", res)
	}
	if res, ok := results["bad"]; !ok || res.err == nil {
		t.Errorf("bad: got %+v, want an error", res)
	}
	if _, ok := results["stopped"]; ok {
		t.Errorf("stopped container should not have been fetched")
	}
}

// fakeDaemonWithContainers serves the slice of the Docker API Capacity uses:
// /info, the container list, per-container stats (each held for statsDelay),
// and per-container inspect (managed jobs reserve 500m CPU / 256 MiB).
func fakeDaemonWithContainers(t *testing.T, n int, statsDelay time.Duration, failStatsFor string) *Docker {
	t.Helper()
	return newTestDocker(t, func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(p, "/info"):
			_ = json.NewEncoder(w).Encode(map[string]any{"NCPU": 4, "MemTotal": int64(8 << 30), "Name": "test-host"})
		case strings.HasSuffix(p, "/containers/json"):
			list := make([]map[string]any, n)
			for i := range list {
				c := map[string]any{"Id": fmt.Sprintf("c%d", i), "Names": []string{fmt.Sprintf("/c%d", i)}, "State": "running", "Labels": map[string]string{}}
				if i%2 == 0 { // every other container is a Weibo-managed job
					c["Labels"] = map[string]string{"weibo.job": fmt.Sprintf("job%d", i)}
				}
				list[i] = c
			}
			_ = json.NewEncoder(w).Encode(list)
		case strings.HasSuffix(p, "/stats"):
			if failStatsFor != "" && strings.Contains(p, "/"+failStatsFor+"/") {
				http.Error(w, "stats unavailable", http.StatusInternalServerError)
				return
			}
			time.Sleep(statsDelay)
			_ = json.NewEncoder(w).Encode(map[string]any{})
		case strings.HasSuffix(p, "/json"): // /containers/{id}/json = inspect
			_ = json.NewEncoder(w).Encode(map[string]any{"HostConfig": map[string]any{"NanoCPUs": int64(500_000_000), "Memory": int64(256 << 20)}})
		default:
			http.NotFound(w, r)
		}
	})
}

// TestDockerCapacity_StatsAreConcurrent is the regression test for slow
// /readyz and /healthz: Capacity backs both, and it used to fetch container
// stats one at a time, so latency grew linearly with the container count.
func TestDockerCapacity_StatsAreConcurrent(t *testing.T) {
	const n = 20
	d := fakeDaemonWithContainers(t, n, 100*time.Millisecond, "")

	start := time.Now()
	snap, err := d.Capacity(t.Context(), CapacityConfig{})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}

	// Serial: 20 x 100ms = 2s. Concurrent (cap 8): ~300ms.
	if elapsed >= 1*time.Second {
		t.Errorf("Capacity took %v for %d containers — stats look serial again", elapsed, n)
	}
	if snap.Health != "healthy" || snap.Reason != "" {
		t.Errorf("health = %q (%s), want healthy", snap.Health, snap.Reason)
	}
	if snap.RunningContainers != n || len(snap.Containers) != n {
		t.Errorf("running=%d listed=%d, want %d", snap.RunningContainers, len(snap.Containers), n)
	}
	// 10 of the 20 are managed jobs, each reserving 500m CPU / 256 MiB.
	if snap.UsedSlots != 10 || snap.CPUReservedMilli != 5000 || snap.MemoryReservedBytes != 10*(256<<20) {
		t.Errorf("used=%d cpuReserved=%d memReserved=%d, want 10 / 5000 / %d", snap.UsedSlots, snap.CPUReservedMilli, snap.MemoryReservedBytes, int64(10*(256<<20)))
	}
	if snap.CPUTotalMilli != 4000 || snap.CPUAvailableMilli != 0 { // reservations exceed the 4-CPU host: clamped to 0
		t.Errorf("cpuTotal=%d cpuAvailable=%d, want 4000 / 0", snap.CPUTotalMilli, snap.CPUAvailableMilli)
	}
	if snap.AvailableSlots == nil || snap.TotalSlots == nil || *snap.AvailableSlots != 0 || *snap.TotalSlots != 10 {
		t.Errorf("slots available=%v total=%v, want 0 / 10", snap.AvailableSlots, snap.TotalSlots)
	}
}

// One container whose stats call fails degrades the snapshot (with the
// container named in the reason) instead of failing the whole health check.
func TestDockerCapacity_OneStatsFailureDegradesNotFails(t *testing.T) {
	d := fakeDaemonWithContainers(t, 4, 0, "c1")

	snap, err := d.Capacity(t.Context(), CapacityConfig{})
	if err != nil {
		t.Fatalf("Capacity must not error on a single stats failure: %v", err)
	}
	if snap.Health != "degraded" || !strings.Contains(snap.Reason, "stats c1") {
		t.Errorf("health=%q reason=%q, want degraded naming c1", snap.Health, snap.Reason)
	}
	if snap.RunningContainers != 4 {
		t.Errorf("running = %d, want all 4 still counted", snap.RunningContainers)
	}
}

func TestDockerCapacity_DaemonUnreachableReportsUnreachable(t *testing.T) {
	d := newTestDocker(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "down", http.StatusInternalServerError) })
	snap, err := d.Capacity(t.Context(), CapacityConfig{})
	if err != nil || snap.Health != "unreachable" || snap.Reason == "" {
		t.Fatalf("snap = %+v, err = %v; want an unreachable snapshot with a reason", snap, err)
	}
}
