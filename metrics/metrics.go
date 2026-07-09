package metrics

import "github.com/prometheus/client_golang/prometheus"

var Registry = prometheus.NewRegistry()

func init() {
	Registry.MustRegister(All()...)
}

var (
	RecordsReadTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mailer_records_read_total",
		Help: "Total number of records read from all sources.",
	})

	RecordsProcessedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mailer_records_processed_total",
		Help: "Total number of records processed, partitioned by operator.",
	}, []string{"operator"})

	RecordsWrittenTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mailer_records_written_total",
		Help: "Total number of records written to all sinks.",
	})

	RecordsFailedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mailer_records_failed_total",
		Help: "Total number of records that failed during processing.",
	})

	PipelineRunning = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mailer_pipeline_running",
		Help: "1 if the pipeline is running, 0 otherwise.",
	})

	SourceErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mailer_source_errors_total",
		Help: "Total number of source errors.",
	})

	SinkErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mailer_sink_errors_total",
		Help: "Total number of sink errors.",
	})

	OperatorLatencySeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mailer_operator_latency_seconds",
		Help:    "Per-operator processing latency in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms to ~8s
	}, []string{"operator"})

	SinkWriteLatencySeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "mailer_sink_write_latency_seconds",
		Help:    "Sink write latency in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})
)

// All returns all registered metrics for use with a custom registry
// or mustRegister-style setup.
func All() []prometheus.Collector {
	return []prometheus.Collector{
		RecordsReadTotal,
		RecordsProcessedTotal,
		RecordsWrittenTotal,
		RecordsFailedTotal,
		PipelineRunning,
		SourceErrorsTotal,
		SinkErrorsTotal,
		OperatorLatencySeconds,
		SinkWriteLatencySeconds,
	}
}
