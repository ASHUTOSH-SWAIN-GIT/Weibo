package control

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultHistorySamples bounds the per-job rolling history: at the default
// 15s scrape interval this is about one hour. History is process memory
// only — never the store — so a controller restart starts it fresh.
const DefaultHistorySamples = 240

// Sample is one history point for a job: raw cumulative counters plus the
// agent's own checkpoint/error/lag state. Rates (records/sec) are derived
// by readers from counter deltas, so a missed tick just widens one
// interval instead of corrupting the series.
type Sample struct {
	At               time.Time  `json:"at"`
	Phase            string     `json:"phase"`
	RecordsIn        int64      `json:"recordsIn"`
	RecordsOut       int64      `json:"recordsOut"`
	CheckpointID     string     `json:"checkpointId,omitempty"`
	LastCheckpointAt *time.Time `json:"lastCheckpointAt,omitempty"`
	// CheckpointDurationMs is barrier injection → completion for the
	// latest checkpoint; CheckpointSizeBytes its inline snapshot bytes.
	CheckpointDurationMs int64  `json:"checkpointDurationMs,omitempty"`
	CheckpointSizeBytes  int64  `json:"checkpointSizeBytes,omitempty"`
	KafkaLag             int64  `json:"kafkaLag,omitempty"`
	EdgeQueued           int64  `json:"edgeQueued,omitempty"`
	Errors               int64  `json:"errors,omitempty"`
	LastError            string `json:"lastError,omitempty"`
}

// History is a bounded in-memory rolling history per job (append-only ring
// per job ID, oldest dropped past the cap). Safe for concurrent use.
type History struct {
	mu     sync.Mutex
	max    int
	series map[string][]Sample
}

// NewHistory builds a history keeping at most max samples per job.
// max <= 0 falls back to DefaultHistorySamples.
func NewHistory(max int) *History {
	if max <= 0 {
		max = DefaultHistorySamples
	}
	return &History{max: max, series: map[string][]Sample{}}
}

// Add appends a sample, dropping the oldest past the cap.
func (h *History) Add(jobID string, s Sample) {
	h.mu.Lock()
	defer h.mu.Unlock()
	series := append(h.series[jobID], s)
	if len(series) > h.max {
		series = series[len(series)-h.max:]
	}
	h.series[jobID] = series
}

// Series returns up to maxPoints evenly spaced samples, oldest first (a
// copy). maxPoints <= 0 returns the full series.
func (h *History) Series(jobID string, maxPoints int) []Sample {
	h.mu.Lock()
	defer h.mu.Unlock()
	series := h.series[jobID]
	if maxPoints <= 0 || len(series) <= maxPoints {
		return append([]Sample(nil), series...)
	}
	out := make([]Sample, 0, maxPoints)
	stride := float64(len(series)-1) / float64(maxPoints-1)
	for i := 0; i < maxPoints; i++ {
		out = append(out, series[int(float64(i)*stride)])
	}
	return out
}

// Drop forgets a job's series (called on job deletion).
func (h *History) Drop(jobID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.series, jobID)
}

// Latest returns the newest sample for a job (false when none). It is
// the "last activity" signal behind the diagnostics endpoint.
func (h *History) Latest(jobID string) (Sample, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	series := h.series[jobID]
	if len(series) == 0 {
		return Sample{}, false
	}
	return series[len(series)-1], true
}

// JobsWithHistory returns the IDs that currently have samples.
func (h *History) JobsWithHistory() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.series))
	for id, series := range h.series {
		if len(series) > 0 {
			out = append(out, id)
		}
	}
	return out
}

// HistoryRecorder samples every live job's agent on a ticker into a
// History. It reuses DiscoveryTargets (bounded probes, 15s deadline) and
// fetches /state plus /metrics per target with bounded concurrency.
type HistoryRecorder struct {
	controller *Controller
	history    *History
	httpc      *http.Client
	// fetch is the per-target scrape; swapped in tests.
	fetch func(ctx context.Context, addr string) (Sample, error)
}

// newHistoryRecorder builds the sampler; fetch defaults to live HTTP.
func newHistoryRecorder(c *Controller, h *History) *HistoryRecorder {
	r := &HistoryRecorder{
		controller: c,
		history:    h,
		httpc:      &http.Client{Timeout: 5 * time.Second},
	}
	r.fetch = r.scrapeTarget
	return r
}

// Run samples on every tick until ctx is cancelled. One slow job never
// blocks the others: targets resolve bounded (see DiscoveryTargets) and
// each scrape has its own timeout.
func (r *HistoryRecorder) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sampleOnce(ctx)
		}
	}
}

func (r *HistoryRecorder) sampleOnce(ctx context.Context) {
	targets, err := r.controller.DiscoveryTargets(ctx)
	if err != nil || len(targets) == 0 {
		return
	}
	sem := make(chan struct{}, maxDiscoveryProbes)
	var wg sync.WaitGroup
	for _, target := range targets {
		jobID, _ := target.Labels["weibo_job_id"]
		addr := ""
		if len(target.Targets) > 0 {
			addr = target.Targets[0]
		}
		if jobID == "" || addr == "" {
			continue
		}
		wg.Add(1)
		go func(jobID, addr string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			sample, err := r.fetch(sctx, addr)
			if err != nil {
				return
			}
			r.history.Add(jobID, sample)
		}(jobID, addr)
	}
	wg.Wait()
}

// agentCheckpoint is one entry of the agent's checkpoint list; only the
// newest (index 0) is sampled.
type agentCheckpoint struct {
	DurationMs int64 `json:"durationMs"`
	SizeBytes  int64 `json:"sizeBytes"`
}

// agentState is the subset of the job agent's /state the sampler keeps.
// Source carries connector operational state (Kafka partition progress
// with lag); its shape varies by connector, so it stays decoded.
type agentState struct {
	Phase            string            `json:"phase"`
	RecordsIn        int64             `json:"recordsIn"`
	RecordsOut       int64             `json:"recordsOut"`
	CheckpointID     string            `json:"currentCheckpointId"`
	LastCheckpointAt *time.Time        `json:"lastCheckpointAt"`
	LastError        string            `json:"lastError"`
	Source           any               `json:"source"`
	Checkpoints      []agentCheckpoint `json:"checkpoints"`
}

// scrapeTarget reads one agent's /state and /metrics into a Sample.
// Either fetch may fail independently: a missing /metrics still yields a
// sample (counters starved of resource detail), a missing /state yields
// nothing.
func (r *HistoryRecorder) scrapeTarget(ctx context.Context, addr string) (Sample, error) {
	sample := Sample{At: time.Now().UTC()}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/state", nil)
	resp, err := r.httpc.Do(req)
	if err != nil {
		return sample, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return sample, &httpError{status: resp.StatusCode}
	}
	var st agentState
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return sample, err
	}
	sample.Phase = st.Phase
	sample.RecordsIn = st.RecordsIn
	sample.RecordsOut = st.RecordsOut
	sample.CheckpointID = st.CheckpointID
	sample.LastCheckpointAt = st.LastCheckpointAt
	sample.LastError = st.LastError
	if len(st.Checkpoints) > 0 {
		sample.CheckpointDurationMs = st.Checkpoints[0].DurationMs
		sample.CheckpointSizeBytes = st.Checkpoints[0].SizeBytes
	}
	sample.KafkaLag = sumLag(st.Source)

	// Resource detail is best effort: a missing /metrics still yields a
	// usable sample (zero queue/errors), never an error.
	sample.EdgeQueued, sample.Errors = r.scrapeAgentMetrics(ctx, addr)
	return sample, nil
}

type httpError struct{ status int }

func (e *httpError) Error() string { return "agent returned " + strconv.Itoa(e.status) }

// scrapeAgentMetrics sums edge queue depth and error counters from an
// agent's Prometheus exposition. Unknown/missing series contribute zero.
func (r *HistoryRecorder) scrapeAgentMetrics(ctx context.Context, addr string) (queued, errs int64) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/metrics", nil)
	resp, err := r.httpc.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return 0, 0
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(body), "\n") {
		name, value, ok := promSample(line)
		if !ok {
			continue
		}
		switch name {
		case "weibo_edge_queue_size":
			queued += value
		case "weibo_stage_errors_total",
			"weibo_operator_worker_errors_total",
			"weibo_source_errors_total",
			"weibo_sink_errors_total",
			"weibo_records_failed_total":
			errs += value
		}
	}
	return queued, errs
}

// promSample parses one exposition line into (metric name, int value).
func promSample(line string) (string, int64, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", 0, false
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", 0, false
	}
	name := fields[0]
	if i := strings.IndexByte(name, '{'); i >= 0 {
		name = name[:i]
	}
	f, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return "", 0, false
	}
	return name, int64(f), true
}

// sumLag totals Kafka consumer lag from connector operational state:
// an array of objects carrying a numeric "lag" field. Any other shape
// (nil, non-Kafka sources) contributes zero.
func sumLag(source any) int64 {
	items, ok := source.([]any)
	if !ok {
		return 0
	}
	var total int64
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if lag, ok := m["lag"].(float64); ok && lag > 0 {
			total += int64(lag)
		}
	}
	return total
}
