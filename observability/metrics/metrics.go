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

	OperatorWorkerRecordsIn = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mailer_operator_worker_records_in_total",
		Help: "Total records entering a keyed operator worker.",
	}, []string{"operator", "worker"})

	OperatorWorkerRecordsOut = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mailer_operator_worker_records_out_total",
		Help: "Total records emitted by a keyed operator worker.",
	}, []string{"operator", "worker"})

	OperatorWorkerErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mailer_operator_worker_errors_total",
		Help: "Total errors in a keyed operator worker.",
	}, []string{"operator", "worker"})

	OperatorWorkerLatencySeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mailer_operator_worker_processing_duration_seconds",
		Help:    "Per-worker processing latency in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"operator", "worker"})

	// Edge metrics: the bounded channels between execution stages.
	// A queue size pinned at capacity means the downstream stage is
	// the bottleneck — that's where backpressure originates.

	EdgeQueueSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mailer_edge_queue_size",
		Help: "Records currently buffered in an inter-stage edge.",
	}, []string{"edge"})

	EdgeQueueCapacity = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mailer_edge_queue_capacity",
		Help: "Capacity of an inter-stage edge.",
	}, []string{"edge"})

	// Stage metrics: one execution stage = source, a group of chained
	// stateless operators, a keyed worker pool, or the sink.

	StageRecordsInTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mailer_stage_records_in_total",
		Help: "Data records consumed by a stage (markers excluded).",
	}, []string{"stage", "type"})

	StageRecordsOutTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mailer_stage_records_out_total",
		Help: "Data records emitted by a stage (markers excluded).",
	}, []string{"stage", "type"})

	StageErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mailer_stage_errors_total",
		Help: "Fatal errors returned by a stage.",
	}, []string{"stage"})

	StageWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mailer_stage_workers",
		Help: "Number of worker goroutines currently running in a stage.",
	}, []string{"stage"})

	StageSendBlockSeconds = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mailer_stage_send_block_seconds_total",
		Help: "Cumulative time a stage spent blocked sending to its output edge (backpressure wait).",
	}, []string{"stage"})
)

// All returns all registered metrics for use with a custom registry
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
		OperatorWorkerRecordsIn,
		OperatorWorkerRecordsOut,
		OperatorWorkerErrors,
		OperatorWorkerLatencySeconds,
		EdgeQueueSize,
		EdgeQueueCapacity,
		StageRecordsInTotal,
		StageRecordsOutTotal,
		StageErrorsTotal,
		StageWorkers,
		StageSendBlockSeconds,
	}
}
