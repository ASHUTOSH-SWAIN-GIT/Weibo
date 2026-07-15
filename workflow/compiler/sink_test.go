package compiler_test

import (
	"encoding/json"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/compiler"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/operators"
)

func TestCompileSink_Kafka(t *testing.T) {
	s, err := compiler.CompileSink(workflow.SinkSpec{
		Type: "kafka",
		Kafka: &workflow.KafkaSinkSpec{
			Brokers: []string{"localhost:9092"}, Topic: "customer-totals",
			Serialize: "json", Acks: "all",
		},
	})
	if err != nil {
		t.Fatalf("CompileSink: %v", err)
	}
	if _, ok := s.(*sink.KafkaSink); !ok {
		t.Fatalf("expected *sink.KafkaSink, got %T", s)
	}
}

func TestCompileSink_TxnKafka(t *testing.T) {
	// Accepts both the schema type and the phase's "transactional_kafka".
	for _, typ := range []string{"txnKafka", "transactional_kafka"} {
		s, err := compiler.CompileSink(workflow.SinkSpec{
			Type: typ,
			TxnKafka: &workflow.TxnKafkaSinkSpec{
				Brokers: []string{"localhost:9092"}, Topic: "customer-totals",
				TransactionalID: "customer-totals-v1",
			},
		})
		if err != nil {
			t.Fatalf("CompileSink(%s): %v", typ, err)
		}
		if _, ok := s.(*sink.TxnKafkaSink); !ok {
			t.Fatalf("expected *sink.TxnKafkaSink, got %T", s)
		}
	}
}

func TestCompileSink_StdoutBlackhole(t *testing.T) {
	if s, err := compiler.CompileSink(workflow.SinkSpec{Type: "stdout"}); err != nil || s == nil {
		t.Fatalf("stdout: %v", err)
	}
	if s, err := compiler.CompileSink(workflow.SinkSpec{Type: "blackhole"}); err != nil || s == nil {
		t.Fatalf("blackhole: %v", err)
	}
}

func TestCompileSink_Errors(t *testing.T) {
	cases := []struct {
		name string
		spec workflow.SinkSpec
	}{
		{"no type", workflow.SinkSpec{}},
		{"unsupported", workflow.SinkSpec{Type: "s3"}},
		{"kafka no brokers", workflow.SinkSpec{Type: "kafka", Kafka: &workflow.KafkaSinkSpec{Topic: "t"}}},
		{"kafka no topic", workflow.SinkSpec{Type: "kafka", Kafka: &workflow.KafkaSinkSpec{Brokers: []string{"b"}}}},
		{"kafka bad acks", workflow.SinkSpec{Type: "kafka", Kafka: &workflow.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t", Acks: "quorum"}}},
		{"kafka bad format", workflow.SinkSpec{Type: "kafka", Kafka: &workflow.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t", Serialize: "avro"}}},
		{"kafka dlq unsupported", workflow.SinkSpec{Type: "kafka", Kafka: &workflow.KafkaSinkSpec{Brokers: []string{"b"}, Topic: "t", OnError: "dlq"}}},
		{"txn no txnID", workflow.SinkSpec{Type: "txnKafka", TxnKafka: &workflow.TxnKafkaSinkSpec{Brokers: []string{"b"}, Topic: "t"}}},
		{"pg no dsn", workflow.SinkSpec{Type: "postgres", Postgres: &workflow.PostgresSinkSpec{Table: "t", Mapping: map[string]string{"a": "a"}}}},
		{"pg unset dsn var", workflow.SinkSpec{Type: "postgres", Postgres: &workflow.PostgresSinkSpec{DSN: "${MAILER_TEST_UNSET_DSN}", Table: "t", Mapping: map[string]string{"a": "a"}}}},
		{"pg no table", workflow.SinkSpec{Type: "postgres", Postgres: &workflow.PostgresSinkSpec{DSN: "x", Mapping: map[string]string{"a": "a"}}}},
		{"pg unsafe table", workflow.SinkSpec{Type: "postgres", Postgres: &workflow.PostgresSinkSpec{DSN: "x", Table: "t; DROP TABLE u", Mapping: map[string]string{"a": "a"}}}},
		{"pg no mapping", workflow.SinkSpec{Type: "postgres", Postgres: &workflow.PostgresSinkSpec{DSN: "x", Table: "t"}}},
		{"pg unsafe column", workflow.SinkSpec{Type: "postgres", Postgres: &workflow.PostgresSinkSpec{DSN: "x", Table: "t", Mapping: map[string]string{"a": "a; DROP"}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := compiler.CompileSink(c.spec); err == nil {
				t.Errorf("expected error for %s", c.name)
			}
		})
	}
}

// The Postgres success condition: a declarative field→column mapping
// turns a JSON record into a fixed-table row with the right columns and
// values — including exact numeric coercion and a fixed table name.
func TestPostgresMapper_Correctness(t *testing.T) {
	mapper, err := compiler.BuildPostgresMapper("customer_totals", map[string]string{
		"customer_id":         "customer_id",
		"payment.total":       "total_amount",
		"missing_field":       "note",
		"nested.obj":          "meta",
	})
	if err != nil {
		t.Fatal(err)
	}

	r := types.Record{Value: []byte(`{
		"customer_id": "c-1",
		"payment": {"total": 999999999999},
		"nested": {"obj": {"k": 1}}
	}`)}

	table, cols, vals := mapper(r)

	// Fixed table, never derived from the record.
	if table != "customer_totals" {
		t.Errorf("table: got %q", table)
	}
	// Columns are deterministic (sorted): customer_id, meta, note, total_amount
	wantCols := []string{"customer_id", "meta", "note", "total_amount"}
	if len(cols) != len(wantCols) {
		t.Fatalf("columns: got %v, want %v", cols, wantCols)
	}
	for i := range wantCols {
		if cols[i] != wantCols[i] {
			t.Fatalf("columns: got %v, want %v", cols, wantCols)
		}
	}
	byCol := map[string]any{}
	for i, c := range cols {
		byCol[c] = vals[i]
	}
	if byCol["customer_id"] != "c-1" {
		t.Errorf("customer_id: got %v", byCol["customer_id"])
	}
	// json.Number → int64 (exact, no float rounding).
	if got, ok := byCol["total_amount"].(int64); !ok || got != 999999999999 {
		t.Errorf("total_amount: got %v (%T), want int64 999999999999", byCol["total_amount"], byCol["total_amount"])
	}
	// Missing field → SQL NULL.
	if byCol["note"] != nil {
		t.Errorf("note (missing): got %v, want nil", byCol["note"])
	}
	// Nested object → JSON bytes.
	if b, ok := byCol["meta"].([]byte); !ok || string(b) != `{"k":1}` {
		t.Errorf("meta (nested): got %v, want json bytes", byCol["meta"])
	}
}

// The Kafka success condition: a record modified by the declarative
// operators (Parsed = JSONRecord) is serialized correctly by the json
// serializer the compiler wires.
func TestKafkaJSONSerializesDeclarativeRecord(t *testing.T) {
	in := types.Record{Value: []byte(`{"customer_id":"c-1","secret":"x"}`)}
	// A declarative pipeline: drop a field, add a derived one.
	sel := operators.BuildSelect(operators.SelectConfig{Fields: []string{"customer_id"}})
	set := operators.BuildSet(operators.SetConfig{Sets: []operators.FieldSet{{Field: "flagged", Value: true}}})
	out := set(sel(in))

	ser := sink.NewJSONSerializer() // the serializer compileSerializer("json") wires
	b, err := ser.Serialize(out)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	json.Unmarshal(b, &got)
	if got["customer_id"] != "c-1" || got["flagged"] != true {
		t.Errorf("declarative record serialized wrong: %s", b)
	}
	if _, ok := got["secret"]; ok {
		t.Errorf("dropped field present in serialized output: %s", b)
	}
}
