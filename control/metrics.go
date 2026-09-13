package control

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

// ControllerMetrics is the controller's Prometheus surface: process
// health, reconcile loop, launch outcomes, job/run inventory, orphan
// sweeps, and API traffic. It owns a private registry (never the global
// default) so tests and embedded use can run several controllers in one
// process without duplicate registrations.
//
// Cardinality is bounded by construction: labels carry only small
// enumerations (desired state, phase, result, method, route template,
// status). Job/run/container IDs never appear as metric labels —
// per-job addressing lives in the /targets discovery endpoint instead.
type ControllerMetrics struct {
	reg *prometheus.Registry

	launches           *prometheus.CounterVec // result: success|transient|permanent|blocked|record_failed
	reconciles         *prometheus.CounterVec // result: success|error
	reconcileDuration  prometheus.Histogram
	sweepRemoved       prometheus.Counter
	sweepRunningSeen   prometheus.Counter
	apiRequests        *prometheus.CounterVec   // method, route, status
	apiDuration        *prometheus.HistogramVec // method, route
}

// NewControllerMetrics builds the registry, process/go collectors, and
// all controller instruments. st backs the inventory collector (jobs by
// desired state, active runs by phase), read fresh on every scrape.
func NewControllerMetrics(st store.Store) *ControllerMetrics {
	m := &ControllerMetrics{
		reg: prometheus.NewRegistry(),
		launches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "weibo_controller_launches_total",
			Help: "Container launch attempts by outcome.",
		}, []string{"result"}),
		reconciles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "weibo_controller_reconciles_total",
			Help: "Reconcile passes by outcome.",
		}, []string{"result"}),
		reconcileDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "weibo_controller_reconcile_duration_seconds",
			Help:    "Time spent in one reconcile pass.",
			Buckets: prometheus.DefBuckets,
		}),
		sweepRemoved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "weibo_controller_sweep_orphans_removed_total",
			Help: "Orphaned backend resources removed by startup/GC sweeps.",
		}),
		sweepRunningSeen: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "weibo_controller_sweep_running_orphans_total",
			Help: "Running backend resources with no store reference seen by sweeps (reported, never removed).",
		}),
		apiRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "weibo_controller_api_requests_total",
			Help: "API requests by method, route template, and status code.",
		}, []string{"method", "route", "status"}),
		apiDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "weibo_controller_api_duration_seconds",
			Help:    "API request latency by method and route template.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}
	m.reg.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
		m.launches, m.reconciles, m.reconcileDuration,
		m.sweepRemoved, m.sweepRunningSeen,
		m.apiRequests, m.apiDuration,
		&inventoryCollector{store: st},
	)
	return m
}

// Handler serves the registry in Prometheus exposition format.
func (m *ControllerMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// ObserveLaunch records one launch attempt's outcome.
func (m *ControllerMetrics) ObserveLaunch(result string) {
	m.launches.WithLabelValues(result).Inc()
}

// ObserveReconcile records one reconcile pass.
func (m *ControllerMetrics) ObserveReconcile(d time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	m.reconciles.WithLabelValues(result).Inc()
	m.reconcileDuration.Observe(d.Seconds())
}

// AddSweepRemoved records orphaned resources a sweep removed.
func (m *ControllerMetrics) AddSweepRemoved(n int) {
	m.sweepRemoved.Add(float64(n))
}

// AddSweepRunningSeen records running unknowns a sweep reported.
func (m *ControllerMetrics) AddSweepRunningSeen(n int) {
	m.sweepRunningSeen.Add(float64(n))
}

// ObserveAPI records one API request. route must be the matched route
// template (e.g. "/jobs/{id}"), never a raw path with IDs.
func (m *ControllerMetrics) ObserveAPI(method, route, status string, d time.Duration) {
	m.apiRequests.WithLabelValues(method, route, status).Inc()
	m.apiDuration.WithLabelValues(method, route).Observe(d.Seconds())
}

var (
	inventoryJobsDesc = prometheus.NewDesc(
		"weibo_controller_jobs",
		"Jobs in the store by desired state.",
		[]string{"desired"}, nil,
	)
	inventoryRunsDesc = prometheus.NewDesc(
		"weibo_controller_runs",
		"Active (non-terminal) runs by phase.",
		[]string{"phase"}, nil,
	)
)

// inventoryCollector reports job/run inventory gauges, read live from the
// store on every scrape so the numbers never go stale.
type inventoryCollector struct {
	store store.Store
}

func (c *inventoryCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- inventoryJobsDesc
	ch <- inventoryRunsDesc
}

func (c *inventoryCollector) Collect(ch chan<- prometheus.Metric) {
	if jobs, err := c.store.ListJobs(); err == nil {
		byDesired := map[string]int{}
		for _, j := range jobs {
			byDesired[string(j.Desired)]++
		}
		for desired, n := range byDesired {
			ch <- prometheus.MustNewConstMetric(inventoryJobsDesc, prometheus.GaugeValue, float64(n), desired)
		}
	}
	if runs, err := c.store.ActiveRuns(); err == nil {
		byPhase := map[string]int{}
		for _, r := range runs {
			byPhase[r.Phase]++
		}
		for phase, n := range byPhase {
			ch <- prometheus.MustNewConstMetric(inventoryRunsDesc, prometheus.GaugeValue, float64(n), phase)
		}
	}
}
