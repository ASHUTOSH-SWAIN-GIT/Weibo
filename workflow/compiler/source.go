// Package compiler turns a parsed, validated workflow spec into the
// existing Mailer SDK objects (sources, operators, sinks, environment).
//
// Compilation constructs objects only — it never opens a network
// connection. A Kafka consumer-group source builds a lazy reader
// (connects on Run, not construction); the parallel per-partition mode,
// which the SDK dials at construction to discover partitions, is
// therefore rejected here. Any liveness/connection check is a separate
// operation, not part of compile or validation.
package compiler

import (
	"fmt"
	"strings"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/auth"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/record"
)

// CompileSource builds a source.Source from a source spec. It returns
// an error for unsupported or invalid configuration but never connects.
func CompileSource(spec workflow.SourceSpec) (source.Source, error) {
	switch spec.Type {
	case "":
		return nil, fmt.Errorf("compiler: source type is required")
	case "kafka":
		return compileKafkaSource(spec.Kafka)
	case "slice":
		return source.NewSliceSource(compileRecords(spec.Records)), nil
	case "generator":
		return source.NewGeneratorSource(compileRecords(spec.Records)), nil
	default:
		return nil, fmt.Errorf("compiler: unsupported source type %q", spec.Type)
	}
}

func compileKafkaSource(k *workflow.KafkaSourceSpec) (source.Source, error) {
	if k == nil {
		return nil, fmt.Errorf("compiler: kafka source configuration is required")
	}
	if len(k.Brokers) == 0 {
		return nil, fmt.Errorf("compiler: kafka source requires at least one broker")
	}
	if k.Topic == "" && len(k.Topics) == 0 {
		return nil, fmt.Errorf("compiler: kafka source requires a topic or topics")
	}
	if k.Parallel {
		// Parallel mode discovers partitions by dialing the broker at
		// construction, which would connect during compile.
		return nil, fmt.Errorf("compiler: parallel kafka sources are not supported declaratively (they connect at construction); use a consumer group (groupID)")
	}
	if k.GroupID == "" {
		return nil, fmt.Errorf("compiler: kafka source requires a groupID (consumer-group mode)")
	}

	opts := []source.KafkaSourceOption{
		source.KafkaBrokers(k.Brokers...),
		source.KafkaGroupID(k.GroupID),
	}
	if k.Topic != "" {
		opts = append(opts, source.KafkaTopic(k.Topic))
	}
	if len(k.Topics) > 0 {
		opts = append(opts, source.KafkaTopics(k.Topics...))
	}

	switch strings.ToLower(k.StartFrom) {
	case "", "earliest":
		opts = append(opts, source.KafkaStartFrom(source.OffsetEarliest))
	case "latest":
		opts = append(opts, source.KafkaStartFrom(source.OffsetLatest))
	default:
		return nil, fmt.Errorf("compiler: unknown startFrom %q (earliest or latest)", k.StartFrom)
	}

	des, err := compileDeserializer(k.Deserialize)
	if err != nil {
		return nil, err
	}
	if des != nil {
		opts = append(opts, source.KafkaDeserialize(des))
	}

	if k.ExactlyOnce {
		opts = append(opts, source.KafkaExactlyOnce())
	}
	if k.CommitBatch > 0 {
		opts = append(opts, source.KafkaCommitBatch(k.CommitBatch))
	}
	if k.FetchMinBytes > 0 || k.FetchMaxBytes > 0 {
		opts = append(opts, source.KafkaFetchBytes(k.FetchMinBytes, k.FetchMaxBytes))
	}
	if k.Watermark != nil {
		if k.Watermark.MaxOutOfOrderness <= 0 {
			return nil, fmt.Errorf("compiler: watermark maxOutOfOrderness must be greater than zero")
		}
		opts = append(opts, source.KafkaWithWatermarks(k.Watermark.MaxOutOfOrderness.Std()))
		if k.Watermark.Interval > 0 {
			opts = append(opts, source.KafkaWatermarkInterval(k.Watermark.Interval.Std()))
		}
	}
	if k.SASL != nil {
		cfg, err := compileSASL(k.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, source.KafkaSASL(cfg))
	}
	if k.TLS != nil {
		opts = append(opts, source.KafkaTLS(compileTLS(k.TLS)))
	}

	return source.NewKafkaSource(opts...), nil
}

// compileDeserializer resolves the source format. Only "json" is
// built in (decoding to a JSONRecord with json.Number); an empty format
// means records are left as raw bytes (operators decode lazily).
func compileDeserializer(format string) (source.Deserializer, error) {
	switch strings.ToLower(format) {
	case "":
		return nil, nil
	case "json":
		return source.DeserializerFunc(record.DeserializeJSON), nil
	default:
		return nil, fmt.Errorf("compiler: unknown deserialize format %q (only \"json\" is built in)", format)
	}
}

// compileSASL maps a SASL spec to auth.SASLConfig, validating the
// mechanism up front so construction cannot panic on an unknown one.
func compileSASL(s *workflow.SASLSpec) (auth.SASLConfig, error) {
	var mech auth.SASLMechanism
	switch strings.ToUpper(strings.TrimSpace(s.Mechanism)) {
	case string(auth.SASLPlain):
		mech = auth.SASLPlain
	case string(auth.SASLScramSHA256):
		mech = auth.SASLScramSHA256
	case string(auth.SASLScramSHA512):
		mech = auth.SASLScramSHA512
	default:
		return auth.SASLConfig{}, fmt.Errorf("compiler: unsupported SASL mechanism %q (plain, scram-sha-256, scram-sha-512)", s.Mechanism)
	}
	return auth.SASLConfig{Mechanism: mech, Username: s.Username, Password: s.Password}, nil
}

func compileTLS(t *workflow.TLSSpec) auth.TLSConfig {
	return auth.TLSConfig{
		CertFile:           t.CertFile,
		KeyFile:            t.KeyFile,
		CAFile:             t.CAFile,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

// compileRecords turns inline record specs into types.Records for the
// slice/generator test sources.
func compileRecords(recs []workflow.RecordSpec) []types.Record {
	out := make([]types.Record, len(recs))
	for i, r := range recs {
		out[i] = types.Record{Key: []byte(r.Key), Value: []byte(r.Value)}
	}
	return out
}
