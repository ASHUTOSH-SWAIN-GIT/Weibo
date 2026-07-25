package compiler_test

import (
	"context"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/compiler"
)

// TestCompileSource_Kafka constructs a full Kafka source from config and
// inspects it via Describe(). No broker is running, so completing this
// test at all proves construction does not connect.
func TestCompileSource_Kafka(t *testing.T) {
	spec := workflow.SourceSpec{
		Type: "kafka",
		Kafka: &workflow.KafkaSourceSpec{
			Brokers:     []string{"localhost:9092"},
			Topic:       "orders",
			GroupID:     "order-processor",
			StartFrom:   "earliest",
			Deserialize: "json",
			ExactlyOnce: true,
			Watermark: &workflow.WatermarkSpec{
				MaxOutOfOrderness: workflow.Duration(2 * time.Second),
				Interval:          workflow.Duration(500 * time.Millisecond),
			},
		},
	}

	src, err := compiler.CompileSource(spec)
	if err != nil {
		t.Fatalf("CompileSource: %v", err)
	}
	if src == nil {
		t.Fatal("expected a source")
	}
	if _, ok := src.(*source.KafkaSource); !ok {
		t.Fatalf("expected *source.KafkaSource, got %T", src)
	}

	d, ok := src.(source.Describable)
	if !ok {
		t.Fatal("KafkaSource should implement Describable")
	}
	info := d.Describe()
	if info.Type != "Kafka" {
		t.Errorf("type: got %q", info.Type)
	}
	wantProps := map[string]string{
		"brokers":  "localhost:9092",
		"topic":    "orders",
		"group_id": "order-processor",
		"offset":   "earliest",
	}
	for k, want := range wantProps {
		if got := info.Props[k]; got != want {
			t.Errorf("prop %q: got %q, want %q", k, got, want)
		}
	}
	if info.Props["watermark_out_of_orderness"] == "" {
		t.Error("expected watermark prop to be set")
	}
}

func TestCompileSource_KafkaLatestAndSASL(t *testing.T) {
	spec := workflow.SourceSpec{
		Type: "kafka",
		Kafka: &workflow.KafkaSourceSpec{
			Brokers:   []string{"b1:9092", "b2:9092"},
			Topic:     "t",
			GroupID:   "g",
			StartFrom: "latest",
			SASL:      &workflow.SASLSpec{Mechanism: "scram-sha-256", Username: "u", Password: "p"},
			TLS:       &workflow.TLSSpec{InsecureSkipVerify: true},
		},
	}
	src, err := compiler.CompileSource(spec)
	if err != nil {
		t.Fatalf("CompileSource: %v", err)
	}
	info := src.(source.Describable).Describe()
	if info.Props["offset"] != "latest" {
		t.Errorf("offset: got %q", info.Props["offset"])
	}
}

// TestCompileSource_Slice constructs a test source from inline records
// and runs it to completion (no external connection).
func TestCompileSource_Slice(t *testing.T) {
	spec := workflow.SourceSpec{
		Type: "slice",
		Records: []workflow.RecordSpec{
			{Key: "k1", Value: `{"a":1}`},
			{Key: "k2", Value: `{"a":2}`},
		},
	}
	src, err := compiler.CompileSource(spec)
	if err != nil {
		t.Fatalf("CompileSource: %v", err)
	}

	out := make(chan types.Record, 4)
	go func() {
		defer close(out)
		_ = src.Run(context.Background(), out)
	}()

	var got []types.Record
	for r := range out {
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 records, got %d", len(got))
	}
	if string(got[0].Key) != "k1" || string(got[0].Value) != `{"a":1}` {
		t.Errorf("record 0: %+v", got[0])
	}
	if string(got[1].Value) != `{"a":2}` {
		t.Errorf("record 1: %+v", got[1])
	}
}

func TestCompileSource_Generator(t *testing.T) {
	spec := workflow.SourceSpec{
		Type:    "generator",
		Records: []workflow.RecordSpec{{Value: "hello"}},
	}
	src, err := compiler.CompileSource(spec)
	if err != nil {
		t.Fatalf("CompileSource: %v", err)
	}
	if _, ok := src.(*source.GeneratorSource); !ok {
		t.Fatalf("expected *source.GeneratorSource, got %T", src)
	}
}

func TestCompileSource_Errors(t *testing.T) {
	cases := []struct {
		name string
		spec workflow.SourceSpec
	}{
		{"no type", workflow.SourceSpec{}},
		{"unsupported type", workflow.SourceSpec{Type: "rabbitmq"}},
		{"kafka nil config", workflow.SourceSpec{Type: "kafka"}},
		{"kafka no brokers", workflow.SourceSpec{Type: "kafka", Kafka: &workflow.KafkaSourceSpec{Topic: "t", GroupID: "g"}}},
		{"kafka no topic", workflow.SourceSpec{Type: "kafka", Kafka: &workflow.KafkaSourceSpec{Brokers: []string{"b"}, GroupID: "g"}}},
		{"kafka no groupID", workflow.SourceSpec{Type: "kafka", Kafka: &workflow.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t"}}},
		{"kafka parallel rejected", workflow.SourceSpec{Type: "kafka", Kafka: &workflow.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t", Parallel: true}}},
		{"kafka bad startFrom", workflow.SourceSpec{Type: "kafka", Kafka: &workflow.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t", GroupID: "g", StartFrom: "somewhere"}}},
		{"kafka bad format", workflow.SourceSpec{Type: "kafka", Kafka: &workflow.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t", GroupID: "g", Deserialize: "protobuf"}}},
		{"kafka bad sasl", workflow.SourceSpec{Type: "kafka", Kafka: &workflow.KafkaSourceSpec{Brokers: []string{"b"}, Topic: "t", GroupID: "g", SASL: &workflow.SASLSpec{Mechanism: "kerberos"}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := compiler.CompileSource(c.spec); err == nil {
				t.Errorf("expected error for %s", c.name)
			}
		})
	}
}

// The json format wires a deserializer that decodes into the record
// model (json.Number), which built-in operators then read.
func TestCompileSource_JSONDeserializerWired(t *testing.T) {
	spec := workflow.SourceSpec{
		Type: "kafka",
		Kafka: &workflow.KafkaSourceSpec{
			Brokers: []string{"b"}, Topic: "t", GroupID: "g", Deserialize: "json",
		},
	}
	src, err := compiler.CompileSource(spec)
	if err != nil {
		t.Fatalf("CompileSource: %v", err)
	}
	if info := src.(source.Describable).Describe(); info.Props["deserializer"] == "" {
		t.Error("expected a deserializer to be wired for format json")
	}
}
