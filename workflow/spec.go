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
	Pipeline []Operator `yaml:"pipeline,omitempty" json:"pipeline,omitempty"`

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

// ---- Pipeline operators ----------------------------------------------------

// Operator is one operator in the pipeline: a discriminated union.
// Type names the operator kind and exactly one matching typed config
// block carries its configuration. There is no shared bag of fields —
// each kind decodes into its own typed struct, so a field that belongs
// to another kind (or is misspelled) is rejected by the strict decoder
// rather than silently ignored.
//
//	pipeline:
//	  - type: map
//	    map: { ref: parseOrder }
//	  - type: keyBy
//	    keyBy: { ref: byCustomer, partitions: 8 }
//	  - type: window
//	    window: { type: tumbling, size: 5m }
type Operator struct {
	// ID optionally names this operator (for cross-references and
	// duplicate detection during validation). Optional and unique.
	ID string `yaml:"id,omitempty" json:"id,omitempty"`

	// Type is one of: map, filter, flatMap, process, keyBy, reduce, window.
	// It must match the single config block that is set.
	Type string `yaml:"type" json:"type"`

	Map     *MapConfig     `yaml:"map,omitempty" json:"map,omitempty"`
	Filter  *FilterConfig  `yaml:"filter,omitempty" json:"filter,omitempty"`
	FlatMap *FlatMapConfig `yaml:"flatMap,omitempty" json:"flatMap,omitempty"`
	Process *ProcessConfig `yaml:"process,omitempty" json:"process,omitempty"`
	KeyBy   *KeyByConfig   `yaml:"keyBy,omitempty" json:"keyBy,omitempty"`
	Reduce  *ReduceConfig  `yaml:"reduce,omitempty" json:"reduce,omitempty"`
	Window  *WindowConfig  `yaml:"window,omitempty" json:"window,omitempty"`
}

// MapConfig configures a map operator (1:1 transform).
type MapConfig struct {
	// Ref names the registered map function. "builtin:<name>" selects
	// a built-in. Required.
	Ref string `yaml:"ref" json:"ref"`
	// Label is the metrics/dashboard label (defaults to "Map").
	Label string `yaml:"label,omitempty" json:"label,omitempty"`
	// Parallelism is the stateless worker count (order not preserved
	// when > 1). 0 = 1 worker.
	Parallelism int `yaml:"parallelism,omitempty" json:"parallelism,omitempty"`
}

// FilterConfig configures a filter operator (predicate).
type FilterConfig struct {
	Ref         string `yaml:"ref" json:"ref"`
	Label       string `yaml:"label,omitempty" json:"label,omitempty"`
	Parallelism int    `yaml:"parallelism,omitempty" json:"parallelism,omitempty"`
}

// FlatMapConfig configures a flatMap operator (1:many).
type FlatMapConfig struct {
	Ref         string `yaml:"ref" json:"ref"`
	Label       string `yaml:"label,omitempty" json:"label,omitempty"`
	Parallelism int    `yaml:"parallelism,omitempty" json:"parallelism,omitempty"`
}

// ProcessConfig configures a process operator (error-aware transform).
type ProcessConfig struct {
	Ref         string `yaml:"ref" json:"ref"`
	Label       string `yaml:"label,omitempty" json:"label,omitempty"`
	Parallelism int    `yaml:"parallelism,omitempty" json:"parallelism,omitempty"`
	// OnError is the failure policy: "drop" (default), "dlq", or "fail".
	OnError string `yaml:"onError,omitempty" json:"onError,omitempty"`
	// DLQ is the dead-letter sink, used only when OnError == "dlq".
	DLQ *SinkSpec `yaml:"dlq,omitempty" json:"dlq,omitempty"`
}

// KeyByConfig configures a keyBy operator (partition by key selector).
type KeyByConfig struct {
	// Ref names the registered key selector. Required.
	Ref string `yaml:"ref" json:"ref"`
	// Partitions is the keyed-worker count. 0 = SDK default (16).
	Partitions int    `yaml:"partitions,omitempty" json:"partitions,omitempty"`
	Label      string `yaml:"label,omitempty" json:"label,omitempty"`
}

// ReduceConfig configures a reduce operator (keyed aggregation).
type ReduceConfig struct {
	// Ref names the registered reduce function. Required (after keyBy).
	Ref   string `yaml:"ref" json:"ref"`
	Label string `yaml:"label,omitempty" json:"label,omitempty"`
}

// WindowConfig configures a window operator (after keyBy).
type WindowConfig struct {
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

	Label string `yaml:"label,omitempty" json:"label,omitempty"`
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
