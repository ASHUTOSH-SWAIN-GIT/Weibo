package pipeline

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// stageMetrics holds a stage's pre-resolved metric handles so the hot
// path never does label lookups.
type stageMetrics struct {
	in      prometheus.Counter
	out     prometheus.Counter
	blocked prometheus.Counter
	workers prometheus.Gauge
}

func newStageMetrics(stage, typ string) *stageMetrics {
	return &stageMetrics{
		in:      metrics.StageRecordsInTotal.WithLabelValues(stage, typ),
		out:     metrics.StageRecordsOutTotal.WithLabelValues(stage, typ),
		blocked: metrics.StageSendBlockSeconds.WithLabelValues(stage),
		workers: metrics.StageWorkers.WithLabelValues(stage),
	}
}

// countIn records a consumed data record. Markers are not counted —
// in/out reflect throughput, not control flow.
func (m *stageMetrics) countIn(r types.Record) {
	if !r.IsBarrier && !r.IsWatermark {
		m.in.Inc()
	}
}

// countOut records an emitted data record for sends that bypass send()
// (e.g. the sink pump's multi-way select).
func (m *stageMetrics) countOut(r types.Record) {
	if !r.IsBarrier && !r.IsWatermark {
		m.out.Inc()
	}
}

// send forwards r to out like sendRecord, additionally counting data
// records and the time spent blocked on a full edge. The fast path
// (channel has room) costs one extra select and no clock reads.
func (m *stageMetrics) send(hardCtx context.Context, out chan<- types.Record, r types.Record) error {
	isData := !r.IsBarrier && !r.IsWatermark
	select {
	case out <- r:
		if isData {
			m.out.Inc()
		}
		return nil
	default:
	}
	start := time.Now()
	err := sendRecord(hardCtx, out, r)
	m.blocked.Add(time.Since(start).Seconds())
	if err == nil && isData {
		m.out.Inc()
	}
	return err
}

// setWorkers publishes the stage's live worker count for the duration
// of Run. Call the returned func on exit.
func (m *stageMetrics) setWorkers(n int) func() {
	m.workers.Set(float64(n))
	return func() { m.workers.Set(0) }
}

// sampleEdges publishes queue size/capacity gauges for every edge once
// per second until stop is closed. Size pinned at capacity identifies
// the stage downstream of that edge as the bottleneck.
func sampleEdges(stop <-chan struct{}, edges []*Edge) {
	for _, e := range edges {
		metrics.EdgeQueueCapacity.WithLabelValues(e.Name).Set(float64(cap(e.Ch)))
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			for _, e := range edges {
				metrics.EdgeQueueSize.WithLabelValues(e.Name).Set(float64(len(e.Ch)))
			}
		}
	}
}

// SampleEdges starts the edge gauge sampler in a goroutine; it stops
// when stop is closed.
func SampleEdges(stop <-chan struct{}, edges []*Edge) {
	if len(edges) == 0 {
		return
	}
	go sampleEdges(stop, edges)
}
