package compiler

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/record"
)

// identRe is a conservative SQL identifier: a table name may be
// schema-qualified (one dot), a column name may not. This blocks
// injection through table/column names supplied by config.
var (
	tableRe  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)
	columnRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// CompileSink builds a sink.Sink from a sink spec. Kafka, transactional
// Kafka, stdout, and blackhole sinks construct without connecting;
// constructing a Postgres sink opens its connection pool (SDK behavior),
// so build it last, after the rest of a workflow validates.
func CompileSink(spec workflow.SinkSpec) (sink.Sink, error) {
	switch spec.Type {
	case "":
		return nil, fmt.Errorf("compiler: sink type is required")
	case "kafka":
		return compileKafkaSink(spec.Kafka)
	case "txnKafka", "transactional_kafka":
		return compileTxnKafkaSink(spec.TxnKafka)
	case "postgres":
		return compilePostgresSink(spec.Postgres)
	case "stdout":
		return sink.NewStdoutSink(), nil
	case "blackhole":
		return sink.NewBlackholeSink(), nil
	default:
		return nil, fmt.Errorf("compiler: unsupported sink type %q", spec.Type)
	}
}

func compileKafkaSink(k *workflow.KafkaSinkSpec) (sink.Sink, error) {
	if k == nil {
		return nil, fmt.Errorf("compiler: kafka sink configuration is required")
	}
	if len(k.Brokers) == 0 {
		return nil, fmt.Errorf("compiler: kafka sink requires at least one broker")
	}
	if k.Topic == "" {
		return nil, fmt.Errorf("compiler: kafka sink requires a topic")
	}

	opts := []sink.KafkaSinkOption{
		sink.KafkaSinkBrokers(k.Brokers...),
		sink.KafkaSinkTopic(k.Topic),
	}
	if k.BatchSize > 0 {
		opts = append(opts, sink.KafkaSinkBatchSize(k.BatchSize))
	}
	if k.BatchTimeout > 0 {
		opts = append(opts, sink.KafkaSinkBatchTimeout(k.BatchTimeout.Std()))
	}
	acks, err := compileAcks(k.Acks)
	if err != nil {
		return nil, err
	}
	if k.Acks != "" {
		opts = append(opts, sink.KafkaSinkRequiredAcks(acks))
	}
	if k.Async {
		opts = append(opts, sink.KafkaSinkAsync())
	}
	ser, err := compileSerializer(k.Serialize)
	if err != nil {
		return nil, err
	}
	if ser != nil {
		opts = append(opts, sink.KafkaSinkSerialize(ser))
	}
	if k.MaxRetries > 0 {
		opts = append(opts, sink.KafkaSinkMaxRetries(k.MaxRetries))
	}
	pol, err := compileFailurePolicy(k.OnError)
	if err != nil {
		return nil, err
	}
	opts = append(opts, sink.KafkaSinkFailurePolicy(pol))
	if k.SASL != nil {
		cfg, err := compileSASL(k.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sink.KafkaSinkSASL(cfg))
	}
	if k.TLS != nil {
		opts = append(opts, sink.KafkaSinkTLS(compileTLS(k.TLS)))
	}
	return sink.NewKafkaSink(opts...), nil
}

func compileTxnKafkaSink(t *workflow.TxnKafkaSinkSpec) (sink.Sink, error) {
	if t == nil {
		return nil, fmt.Errorf("compiler: transactional kafka sink configuration is required")
	}
	if len(t.Brokers) == 0 {
		return nil, fmt.Errorf("compiler: transactional kafka sink requires at least one broker")
	}
	if t.Topic == "" {
		return nil, fmt.Errorf("compiler: transactional kafka sink requires a topic")
	}
	if t.TransactionalID == "" {
		return nil, fmt.Errorf("compiler: transactional kafka sink requires a transactionalID")
	}
	opts := []sink.TxnKafkaOption{
		sink.TxnKafkaBrokers(t.Brokers...),
		sink.TxnKafkaTopic(t.Topic),
		sink.TxnKafkaTransactionalID(t.TransactionalID),
	}
	if t.MarkerTopic != "" {
		opts = append(opts, sink.TxnKafkaMarkerTopic(t.MarkerTopic))
	}
	ser, err := compileSerializer(t.Serialize)
	if err != nil {
		return nil, err
	}
	if ser != nil {
		opts = append(opts, sink.TxnKafkaSerialize(ser))
	}
	return sink.NewTxnKafkaSink(opts...), nil
}

func compilePostgresSink(p *workflow.PostgresSinkSpec) (sink.Sink, error) {
	if p == nil {
		return nil, fmt.Errorf("compiler: postgres sink configuration is required")
	}
	dsn := os.ExpandEnv(p.DSN)
	if dsn == "" {
		return nil, fmt.Errorf("compiler: postgres sink requires a dsn")
	}
	if p.Table == "" {
		return nil, fmt.Errorf("compiler: postgres sink requires a table")
	}
	if !tableRe.MatchString(p.Table) {
		return nil, fmt.Errorf("compiler: unsafe postgres table name %q", p.Table)
	}
	if len(p.Mapping) == 0 {
		return nil, fmt.Errorf("compiler: postgres sink requires a non-empty field→column mapping")
	}
	mapper, err := BuildPostgresMapper(p.Table, p.Mapping)
	if err != nil {
		return nil, err
	}

	opts := []sink.PostgresSinkOption{
		sink.PostgresDSN(dsn),
		sink.PostgresMapper(mapper),
	}
	if p.BatchSize > 0 {
		opts = append(opts, sink.PostgresBatchSize(p.BatchSize))
	}
	if p.FlushInterval > 0 {
		opts = append(opts, sink.PostgresFlushInterval(p.FlushInterval.Std()))
	}
	if p.MaxRetries > 0 {
		opts = append(opts, sink.PostgresMaxRetries(p.MaxRetries))
	}
	pol, err := compileFailurePolicy(p.OnError)
	if err != nil {
		return nil, err
	}
	opts = append(opts, sink.PostgresFailurePolicy(pol))

	// NewPostgresSink opens the connection pool eagerly (SDK behavior).
	return sink.NewPostgresSink(opts...), nil
}

// BuildPostgresMapper builds a fixed-table RecordMapper from a
// field→column mapping. Every record maps to the SAME table (the name
// is a config constant, never derived from record data) and the mapped
// JSON fields become the columns. Column order is deterministic (sorted
// by column) so batched INSERTs are stable. A JSON field absent from a
// record yields a SQL NULL; json.Number values are coerced to int64 or
// float64 for correct numeric columns; nested objects/arrays are stored
// as JSON bytes.
func BuildPostgresMapper(table string, mapping map[string]string) (sink.RecordMapper, error) {
	if !tableRe.MatchString(table) {
		return nil, fmt.Errorf("compiler: unsafe postgres table name %q", table)
	}
	type fieldCol struct{ field, col string }
	pairs := make([]fieldCol, 0, len(mapping))
	for field, col := range mapping {
		if field == "" {
			return nil, fmt.Errorf("compiler: postgres mapping has an empty field name")
		}
		if !columnRe.MatchString(col) {
			return nil, fmt.Errorf("compiler: unsafe postgres column name %q", col)
		}
		pairs = append(pairs, fieldCol{field, col})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].col < pairs[j].col })

	return func(r types.Record) (string, []string, []any) {
		jr, _ := record.DecodeJSON(r)
		cols := make([]string, len(pairs))
		vals := make([]any, len(pairs))
		for i, p := range pairs {
			cols[i] = p.col
			v, ok := record.GetField(jr, p.field)
			vals[i] = coercePGValue(v, ok)
		}
		return table, cols, vals
	}, nil
}

func coercePGValue(v any, present bool) any {
	if !present {
		return nil
	}
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
		if f, err := n.Float64(); err == nil {
			return f
		}
		return string(n)
	case map[string]any, []any:
		b, _ := json.Marshal(v)
		return b
	default:
		return v
	}
}

// compileAcks maps a required-acks string to the SDK enum.
func compileAcks(acks string) (sink.AcksLevel, error) {
	switch strings.ToLower(acks) {
	case "", "leader", "one":
		return sink.AcksLeader, nil
	case "none":
		return sink.AcksNone, nil
	case "all":
		return sink.AcksAll, nil
	default:
		return 0, fmt.Errorf("compiler: unknown requiredAcks %q (none, leader, all)", acks)
	}
}

// compileSerializer resolves a sink format. Only "json" is built in
// (serializes the record's Parsed JSONRecord — i.e. the declaratively
// modified record — or its raw Value); empty means raw bytes.
func compileSerializer(format string) (sink.Serializer, error) {
	switch strings.ToLower(format) {
	case "":
		return nil, nil
	case "json":
		return sink.NewJSONSerializer(), nil
	default:
		return nil, fmt.Errorf("compiler: unknown serialize format %q (only \"json\" is built in)", format)
	}
}

// compileFailurePolicy maps an onError string to the SDK policy. "dlq"
// needs a dead-letter sink not expressible declaratively yet.
func compileFailurePolicy(onError string) (sink.FailurePolicy, error) {
	switch strings.ToLower(onError) {
	case "", "drop":
		return sink.FailurePolicyDrop, nil
	case "fail":
		return sink.FailurePolicyFail, nil
	case "dlq":
		return 0, fmt.Errorf("compiler: onError \"dlq\" is not supported declaratively yet")
	default:
		return 0, fmt.Errorf("compiler: unknown onError %q (drop or fail)", onError)
	}
}
