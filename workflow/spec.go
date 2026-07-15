// Package workflow defines a declarative format for Mailer pipelines.
//
// A workflow document (YAML or JSON) describes a pipeline's shape and
// configuration — source, ordered operators, sink, and environment
// settings — mirroring the fluent SDK in mailer/stream.go. It is
// compiled into the same SDK objects and run by the same engine.
//
// Logic by reference. Transformation logic (map/filter/reduce
// functions, key selectors) is Go code and cannot live in YAML. Steps
// therefore reference logic by name (the Ref field); the names are
// resolved against a registry at compile time (a later phase). The
// document itself carries only topology and pure configuration.
//
// Phase 2.1 provides the schema and structural parsing only. Semantic
// validation, ref resolution, compilation to SDK objects, and
// execution are separate phases.
package workflow

// Workflow is a complete declarative pipeline definition: the root of a
// workflow YAML/JSON document.
type Workflow struct {
	// Name identifies the pipeline (used in logs, metrics labels, and
	// as a default transactional-id prefix). Optional.
	Name string `yaml:"name,omitempty" json:"name,omitempty"`

	// Version is the workflow-format version. "1" (or empty) today.
	Version string `yaml:"version,omitempty" json:"version,omitempty"`

	// Env configures the execution environment (buffer size, shutdown
	// timeout, checkpointing, state backend). Optional; SDK defaults
	// apply when omitted.
	Env *EnvSpec `yaml:"env,omitempty" json:"env,omitempty"`

	// Source is where records enter the pipeline. Required.
	Source SourceSpec `yaml:"source" json:"source"`

	// Pipeline is the ordered list of operators between source and
	// sink. May be empty (source → sink passthrough).
	Pipeline []StepSpec `yaml:"pipeline,omitempty" json:"pipeline,omitempty"`

	// Sink is where results leave the pipeline. Required.
	Sink SinkSpec `yaml:"sink" json:"sink"`
}

// EnvSpec mirrors the StreamExecutionEnv Configuration methods.
type EnvSpec struct {
	// BufferSize is the bounded edge capacity between stages
	// (WithBufferSize). 0 = SDK default (1024).
	BufferSize int `yaml:"bufferSize,omitempty" json:"bufferSize,omitempty"`

	// ShutdownTimeout is the drain wait before forced shutdown
	// (WithShutdownTimeout). 0 = SDK default (30s).
	ShutdownTimeout Duration `yaml:"shutdownTimeout,omitempty" json:"shutdownTimeout,omitempty"`

	// Checkpointing enables barrier checkpointing (WithCheckpointing).
	// Omitted = disabled.
	Checkpointing *CheckpointSpec `yaml:"checkpointing,omitempty" json:"checkpointing,omitempty"`

	// State selects the state backend (WithStateBackend). Omitted =
	// in-memory.
	State *StateSpec `yaml:"state,omitempty" json:"state,omitempty"`
}

// CheckpointSpec configures barrier checkpointing with file storage.
type CheckpointSpec struct {
	// Interval between injected checkpoint barriers. Required when
	// checkpointing is present.
	Interval Duration `yaml:"interval" json:"interval"`

	// Dir is the checkpoint file-storage directory
	// (checkpoint.NewFileStorage). Required.
	Dir string `yaml:"dir" json:"dir"`
}

// StateSpec selects and configures a state backend.
type StateSpec struct {
	// Backend is "memory" or "pebble".
	Backend string `yaml:"backend" json:"backend"`

	// Dir is the Pebble root directory (one DB per operator owner
	// underneath). Required when Backend is "pebble".
	Dir string `yaml:"dir,omitempty" json:"dir,omitempty"`
}

// ---- Source ----------------------------------------------------------------

// SourceSpec is a tagged union over source types. Exactly one of the
// per-type sub-specs is set, matching Type.
type SourceSpec struct {
	// Type is "kafka", "slice", or "generator".
	Type string `yaml:"type" json:"type"`

	Kafka *KafkaSourceSpec `yaml:"kafka,omitempty" json:"kafka,omitempty"`

	// Slice / Generator carry test data inline (mainly for examples
	// and tests). Records are raw string key/value pairs.
	Records []RecordSpec `yaml:"records,omitempty" json:"records,omitempty"`
}

// RecordSpec is an inline record for slice/generator sources.
type RecordSpec struct {
	Key   string `yaml:"key,omitempty" json:"key,omitempty"`
	Value string `yaml:"value" json:"value"`
}

// KafkaSourceSpec mirrors the KafkaSource functional options.
type KafkaSourceSpec struct {
	Brokers []string `yaml:"brokers" json:"brokers"`

	// Topic or Topics (consumer-group). Exactly one is used.
	Topic  string   `yaml:"topic,omitempty" json:"topic,omitempty"`
	Topics []string `yaml:"topics,omitempty" json:"topics,omitempty"`

	GroupID string `yaml:"groupID,omitempty" json:"groupID,omitempty"`

	// StartFrom is "earliest" or "latest". Empty = earliest.
	StartFrom string `yaml:"startFrom,omitempty" json:"startFrom,omitempty"`

	// ExactlyOnce disables eager commits + enables read_committed
	// (pair with a transactional sink).
	ExactlyOnce bool `yaml:"exactlyOnce,omitempty" json:"exactlyOnce,omitempty"`

	// Parallel uses one reader per partition (no consumer group;
	// mutually exclusive with GroupID).
	Parallel bool `yaml:"parallel,omitempty" json:"parallel,omitempty"`

	// Watermark enables event-time watermark generation.
	Watermark *WatermarkSpec `yaml:"watermark,omitempty" json:"watermark,omitempty"`

	// Deserialize names a registered deserializer, or "json" for the
	// built-in JSON deserializer. Empty = raw bytes.
	Deserialize string `yaml:"deserialize,omitempty" json:"deserialize,omitempty"`

	// CommitBatch commits offsets every N messages (0 = per message).
	CommitBatch int `yaml:"commitBatch,omitempty" json:"commitBatch,omitempty"`

	// FetchMinBytes / FetchMaxBytes tune the fetch request.
	FetchMinBytes int `yaml:"fetchMinBytes,omitempty" json:"fetchMinBytes,omitempty"`
	FetchMaxBytes int `yaml:"fetchMaxBytes,omitempty" json:"fetchMaxBytes,omitempty"`

	SASL *SASLSpec `yaml:"sasl,omitempty" json:"sasl,omitempty"`
	TLS  *TLSSpec  `yaml:"tls,omitempty" json:"tls,omitempty"`
}

// WatermarkSpec configures bounded-out-of-orderness watermarks.
type WatermarkSpec struct {
	// MaxOutOfOrderness is the allowed lateness (watermark =
	// max-seen-timestamp − this). Required.
	MaxOutOfOrderness Duration `yaml:"maxOutOfOrderness" json:"maxOutOfOrderness"`

	// Interval between emitted watermarks. 0 = SDK default (500ms).
	Interval Duration `yaml:"interval,omitempty" json:"interval,omitempty"`
}

// SASLSpec mirrors auth.SASLConfig (shared by Kafka source and sink).
type SASLSpec struct {
	Mechanism string `yaml:"mechanism" json:"mechanism"` // e.g. "plain", "scram-sha-256"
	Username  string `yaml:"username" json:"username"`
	Password  string `yaml:"password" json:"password"`
}

// TLSSpec mirrors auth.TLSConfig.
type TLSSpec struct {
	CAFile             string `yaml:"caFile,omitempty" json:"caFile,omitempty"`
	CertFile           string `yaml:"certFile,omitempty" json:"certFile,omitempty"`
	KeyFile            string `yaml:"keyFile,omitempty" json:"keyFile,omitempty"`
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify,omitempty" json:"insecureSkipVerify,omitempty"`
}

// ---- Pipeline steps --------------------------------------------------------

// StepSpec is one operator in the pipeline. Type selects the operator
// kind; the remaining fields carry that kind's configuration. Fields
// not relevant to a given Type are ignored (validation flags misuse in
// a later phase).
type StepSpec struct {
	// Type is one of: map, filter, flatMap, process, keyBy, reduce, window.
	Type string `yaml:"type" json:"type"`

	// Ref names the registered function implementing this operator's
	// logic (map/filter/flatMap/process/reduce fn, or keyBy selector).
	// A "builtin:<name>" ref selects a built-in provided by the
	// registry. Not used by window.
	Ref string `yaml:"ref,omitempty" json:"ref,omitempty"`

	// Label is the dashboard/metrics label (defaults to the operator
	// type name).
	Label string `yaml:"label,omitempty" json:"label,omitempty"`

	// Partitions is the keyed-worker count for keyBy (SDK default 16).
	Partitions int `yaml:"partitions,omitempty" json:"partitions,omitempty"`

	// Parallelism is the worker count for a stateless operator
	// (map/filter/flatMap/process). Order is not preserved when > 1.
	Parallelism int `yaml:"parallelism,omitempty" json:"parallelism,omitempty"`

	// Window configures a window step (Type == "window").
	Window *WindowSpec `yaml:"window,omitempty" json:"window,omitempty"`

	// OnError is the process-operator failure policy: "drop" (default),
	// "dlq", or "fail". Only used by Type == "process".
	OnError string `yaml:"onError,omitempty" json:"onError,omitempty"`

	// DLQ is the dead-letter sink used when OnError == "dlq".
	DLQ *SinkSpec `yaml:"dlq,omitempty" json:"dlq,omitempty"`
}

// WindowSpec configures a window operator.
type WindowSpec struct {
	// Type is "tumbling", "sliding", or "session".
	Type string `yaml:"type" json:"type"`

	// Size is the window length (tumbling, sliding). Required for those.
	Size Duration `yaml:"size,omitempty" json:"size,omitempty"`

	// Slide is the slide interval (sliding only; must be <= Size).
	Slide Duration `yaml:"slide,omitempty" json:"slide,omitempty"`

	// Gap is the inactivity gap (session only).
	Gap Duration `yaml:"gap,omitempty" json:"gap,omitempty"`

	// Offset shifts window boundaries off the epoch (tumbling, sliding).
	Offset Duration `yaml:"offset,omitempty" json:"offset,omitempty"`

	// IdleTimeout fires remaining windows after this much inactivity
	// (WindowWithIdleTimeout). 0 = disabled.
	IdleTimeout Duration `yaml:"idleTimeout,omitempty" json:"idleTimeout,omitempty"`
}

// ---- Sink ------------------------------------------------------------------

// SinkSpec is a tagged union over sink types. Exactly one per-type
// sub-spec is set, matching Type.
type SinkSpec struct {
	// Type is "kafka", "txnKafka", "postgres", "stdout", or "blackhole".
	Type string `yaml:"type" json:"type"`

	Kafka    *KafkaSinkSpec    `yaml:"kafka,omitempty" json:"kafka,omitempty"`
	TxnKafka *TxnKafkaSinkSpec `yaml:"txnKafka,omitempty" json:"txnKafka,omitempty"`
	Postgres *PostgresSinkSpec `yaml:"postgres,omitempty" json:"postgres,omitempty"`
}

// KafkaSinkSpec mirrors the KafkaSink functional options (at-least-once).
type KafkaSinkSpec struct {
	Brokers []string `yaml:"brokers" json:"brokers"`
	Topic   string   `yaml:"topic" json:"topic"`

	BatchSize    int      `yaml:"batchSize,omitempty" json:"batchSize,omitempty"`
	BatchTimeout Duration `yaml:"batchTimeout,omitempty" json:"batchTimeout,omitempty"`

	// Acks is "none", "leader", or "all". Empty = leader.
	Acks  string `yaml:"acks,omitempty" json:"acks,omitempty"`
	Async bool   `yaml:"async,omitempty" json:"async,omitempty"`

	// Serialize names a registered serializer, or "json". Empty = raw.
	Serialize string `yaml:"serialize,omitempty" json:"serialize,omitempty"`

	MaxRetries int    `yaml:"maxRetries,omitempty" json:"maxRetries,omitempty"`
	OnError    string `yaml:"onError,omitempty" json:"onError,omitempty"` // drop | dlq | fail

	SASL *SASLSpec `yaml:"sasl,omitempty" json:"sasl,omitempty"`
	TLS  *TLSSpec  `yaml:"tls,omitempty" json:"tls,omitempty"`
}

// TxnKafkaSinkSpec mirrors the TxnKafkaSink options (exactly-once).
type TxnKafkaSinkSpec struct {
	Brokers []string `yaml:"brokers" json:"brokers"`
	Topic   string   `yaml:"topic" json:"topic"`

	// TransactionalID must be stable across restarts and unique per
	// pipeline instance. Required.
	TransactionalID string `yaml:"transactionalID" json:"transactionalID"`

	// MarkerTopic overrides the recovery-marker topic
	// (default "<topic>.checkpoints").
	MarkerTopic string `yaml:"markerTopic,omitempty" json:"markerTopic,omitempty"`

	Serialize string `yaml:"serialize,omitempty" json:"serialize,omitempty"`
}

// PostgresSinkSpec mirrors the PostgresSink options. The record→row
// mapping is logic, so Mapper is a registered ref.
type PostgresSinkSpec struct {
	DSN string `yaml:"dsn" json:"dsn"`

	// Mapper names a registered RecordMapper. Required.
	Mapper string `yaml:"mapper" json:"mapper"`

	BatchSize     int      `yaml:"batchSize,omitempty" json:"batchSize,omitempty"`
	FlushInterval Duration `yaml:"flushInterval,omitempty" json:"flushInterval,omitempty"`
	MaxRetries    int      `yaml:"maxRetries,omitempty" json:"maxRetries,omitempty"`
	OnError       string   `yaml:"onError,omitempty" json:"onError,omitempty"` // drop | dlq | fail
}
