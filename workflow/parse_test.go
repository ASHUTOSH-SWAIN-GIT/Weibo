package workflow_test

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
)

func marshalYAML(wf *workflow.Workflow) ([]byte, error) { return yaml.Marshal(wf) }
func marshalJSON(wf *workflow.Workflow) ([]byte, error) { return json.Marshal(wf) }

func TestLoad_YAML_OrderTotals(t *testing.T) {
	wf, err := workflow.Load("testdata/order-totals.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if wf.Name != "order-totals" {
		t.Errorf("name: got %q", wf.Name)
	}
	if wf.Env == nil || wf.Env.BufferSize != 2048 {
		t.Fatalf("env.bufferSize: got %+v", wf.Env)
	}
	if wf.Env.ShutdownTimeout.Std() != 30*time.Second {
		t.Errorf("shutdownTimeout: got %s", wf.Env.ShutdownTimeout)
	}
	if wf.Env.Checkpointing == nil || wf.Env.Checkpointing.Interval.Std() != 30*time.Second {
		t.Errorf("checkpointing: got %+v", wf.Env.Checkpointing)
	}
	if wf.Env.State == nil || wf.Env.State.Backend != "pebble" || wf.Env.State.Dir != "/var/lib/mailer/state" {
		t.Errorf("state: got %+v", wf.Env.State)
	}

	if wf.Source.Type != "kafka" || wf.Source.Kafka == nil {
		t.Fatalf("source: got %+v", wf.Source)
	}
	k := wf.Source.Kafka
	if len(k.Brokers) != 1 || k.Brokers[0] != "localhost:9092" || k.Topic != "orders" {
		t.Errorf("kafka source: got %+v", k)
	}
	if !k.ExactlyOnce {
		t.Error("expected exactlyOnce true")
	}
	if k.Watermark == nil || k.Watermark.MaxOutOfOrderness.Std() != time.Second ||
		k.Watermark.Interval.Std() != 500*time.Millisecond {
		t.Errorf("watermark: got %+v", k.Watermark)
	}

	wantSteps := []struct {
		typ, ref string
	}{
		{"map", "parseOrder"},
		{"filter", "isValidOrder"},
		{"keyBy", "byCustomer"},
		{"window", ""},
		{"reduce", "sumAmount"},
	}
	if len(wf.Pipeline) != len(wantSteps) {
		t.Fatalf("pipeline: got %d steps, want %d", len(wf.Pipeline), len(wantSteps))
	}
	for i, w := range wantSteps {
		if wf.Pipeline[i].Type != w.typ || wf.Pipeline[i].Ref != w.ref {
			t.Errorf("step %d: got {%s %s}, want {%s %s}",
				i, wf.Pipeline[i].Type, wf.Pipeline[i].Ref, w.typ, w.ref)
		}
	}
	if p := wf.Pipeline[2]; p.Partitions != 8 {
		t.Errorf("keyBy partitions: got %d", p.Partitions)
	}
	win := wf.Pipeline[3].Window
	if win == nil || win.Type != "tumbling" || win.Size.Std() != 5*time.Minute {
		t.Errorf("window: got %+v", win)
	}

	if wf.Sink.Type != "txnKafka" || wf.Sink.TxnKafka == nil ||
		wf.Sink.TxnKafka.TransactionalID != "order-totals-pipeline" {
		t.Errorf("sink: got %+v", wf.Sink)
	}
}

func TestLoad_JSON_Wordcount(t *testing.T) {
	wf, err := workflow.Load("testdata/wordcount.json")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if wf.Name != "wordcount" {
		t.Errorf("name: got %q", wf.Name)
	}
	if wf.Source.Type != "generator" || len(wf.Source.Records) != 2 {
		t.Fatalf("source: got %+v", wf.Source)
	}
	if wf.Source.Records[0].Value != "hello world" {
		t.Errorf("record 0: got %+v", wf.Source.Records[0])
	}
	if len(wf.Pipeline) != 3 || wf.Pipeline[2].Ref != "builtin:count" {
		t.Errorf("pipeline: got %+v", wf.Pipeline)
	}
	if wf.Sink.Type != "stdout" {
		t.Errorf("sink: got %+v", wf.Sink)
	}
}

func TestParse_RejectsUnknownFields(t *testing.T) {
	yamlDoc := `
name: bad
source:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: t
    notAField: oops
sink:
  type: stdout
`
	if _, err := workflow.ParseYAML([]byte(yamlDoc)); err == nil {
		t.Error("expected YAML parse to reject unknown field 'notAField'")
	}

	jsonDoc := `{"name":"bad","typo":1,"source":{"type":"stdout"},"sink":{"type":"stdout"}}`
	if _, err := workflow.ParseJSON([]byte(jsonDoc)); err == nil {
		t.Error("expected JSON parse to reject unknown field 'typo'")
	}
}

func TestParse_InvalidDuration(t *testing.T) {
	doc := `
name: bad
env:
  shutdownTimeout: 30apples
source:
  type: stdout
sink:
  type: stdout
`
	_, err := workflow.ParseYAML([]byte(doc))
	if err == nil {
		t.Fatal("expected an error for an invalid duration")
	}
}

func TestParse_MissingRequiredIsStructurallyOK(t *testing.T) {
	// Parsing is structural only: a document missing required fields
	// still decodes (validation is a separate phase).
	doc := `source: {type: kafka}` + "\n" + `sink: {type: stdout}`
	wf, err := workflow.ParseYAML([]byte(doc))
	if err != nil {
		t.Fatalf("parse should succeed structurally: %v", err)
	}
	if wf.Source.Type != "kafka" || wf.Sink.Type != "stdout" {
		t.Errorf("got %+v", wf)
	}
}

func TestLoad_UnsupportedExtension(t *testing.T) {
	if _, err := workflow.Load("testdata/whatever.txt"); err == nil {
		t.Error("expected error for unsupported extension")
	}
}

// Round-trip: a decoded workflow re-marshals to YAML and JSON and
// decodes back to an equivalent document (durations survive as strings).
func TestRoundTrip_YAMLAndJSON(t *testing.T) {
	orig, err := workflow.Load("testdata/order-totals.yaml")
	if err != nil {
		t.Fatal(err)
	}

	yml, err := marshalYAML(orig)
	if err != nil {
		t.Fatalf("marshal yaml: %v", err)
	}
	fromYAML, err := workflow.ParseYAML(yml)
	if err != nil {
		t.Fatalf("re-parse yaml: %v", err)
	}
	if fromYAML.Env.Checkpointing.Interval.Std() != 30*time.Second {
		t.Errorf("yaml round-trip lost duration: %s", fromYAML.Env.Checkpointing.Interval)
	}

	js, err := marshalJSON(orig)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	fromJSON, err := workflow.ParseJSON(js)
	if err != nil {
		t.Fatalf("re-parse json: %v", err)
	}
	if fromJSON.Pipeline[3].Window.Size.Std() != 5*time.Minute {
		t.Errorf("json round-trip lost window size: %s", fromJSON.Pipeline[3].Window.Size)
	}
}
