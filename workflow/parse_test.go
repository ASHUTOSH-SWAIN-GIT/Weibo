package workflow_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow"
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
	if wf.Env.State == nil || wf.Env.State.Backend != "pebble" || wf.Env.State.Dir != "/var/lib/weibo/state" {
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

	if len(wf.Pipeline) != 4 {
		t.Fatalf("pipeline: got %d operators, want 4", len(wf.Pipeline))
	}
	// Each operator decodes into its own typed config block.
	if f := wf.Pipeline[0]; f.Type != "filter" || f.Filter == nil || f.Filter.Field != "status" ||
		f.Filter.Operator != "equals" || f.Filter.Value != "completed" {
		t.Errorf("filter op: got %+v", f.Filter)
	}
	if k := wf.Pipeline[1]; k.Type != "keyBy" || k.KeyBy == nil || k.KeyBy.Field != "customer_id" || k.KeyBy.Partitions != 8 {
		t.Errorf("keyBy op: got %+v", k)
	}
	if w := wf.Pipeline[2]; w.Type != "window" || w.Window == nil || w.Window.Type != "tumbling" || w.Window.Size.Std() != 5*time.Minute {
		t.Errorf("window op: got %+v", w)
	}
	if r := wf.Pipeline[3]; r.Type != "reduce" || r.Reduce == nil || r.Reduce.Function != "sum" || r.Reduce.Field != "amount" {
		t.Errorf("reduce op: got %+v", r)
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
	if len(wf.Pipeline) != 2 || wf.Pipeline[1].Reduce == nil || wf.Pipeline[1].Reduce.Function != "count" {
		t.Errorf("pipeline: got %+v", wf.Pipeline)
	}
	if wf.Pipeline[0].KeyBy == nil || wf.Pipeline[0].KeyBy.Field != "word" {
		t.Errorf("keyBy op: got %+v", wf.Pipeline[0])
	}
	if wf.Sink.Type != "stdout" {
		t.Errorf("sink: got %+v", wf.Sink)
	}
}

// Success condition for typed component configs: the same workflow,
// written once as YAML and once as JSON, decodes into byte-for-byte
// identical typed specs.
func TestParse_YAMLAndJSON_IdenticalSpec(t *testing.T) {
	yamlDoc := `
name: totals
env:
  bufferSize: 512
  checkpointing: { interval: 10s, dir: /ckpt }
source:
  type: kafka
  kafka:
    brokers: [b1:9092, b2:9092]
    topic: in
    exactlyOnce: true
    watermark: { maxOutOfOrderness: 2s }
pipeline:
  - type: map
    map: { ref: parse, parallelism: 4 }
  - type: keyBy
    keyBy: { field: k, partitions: 8 }
  - type: window
    window: { type: sliding, size: 10m, slide: 1m }
  - type: reduce
    reduce: { function: sum, field: amount }
sink:
  type: txnKafka
  txnKafka: { brokers: [b1:9092], topic: out, transactionalID: t1 }
`
	jsonDoc := `{
  "name": "totals",
  "env": {
    "bufferSize": 512,
    "checkpointing": { "interval": "10s", "dir": "/ckpt" }
  },
  "source": {
    "type": "kafka",
    "kafka": {
      "brokers": ["b1:9092", "b2:9092"],
      "topic": "in",
      "exactlyOnce": true,
      "watermark": { "maxOutOfOrderness": "2s" }
    }
  },
  "pipeline": [
    { "type": "map", "map": { "ref": "parse", "parallelism": 4 } },
    { "type": "keyBy", "keyBy": { "field": "k", "partitions": 8 } },
    { "type": "window", "window": { "type": "sliding", "size": "10m", "slide": "1m" } },
    { "type": "reduce", "reduce": { "function": "sum", "field": "amount" } }
  ],
  "sink": {
    "type": "txnKafka",
    "txnKafka": { "brokers": ["b1:9092"], "topic": "out", "transactionalID": "t1" }
  }
}`

	fromYAML, err := workflow.ParseYAML([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("ParseYAML: %v", err)
	}
	fromJSON, err := workflow.ParseJSON([]byte(jsonDoc))
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	if !reflect.DeepEqual(fromYAML, fromJSON) {
		t.Errorf("YAML and JSON produced different specs:\nYAML: %+v\nJSON: %+v", fromYAML, fromJSON)
	}
}

// A field that belongs to a different operator kind (or is misspelled)
// inside a component's typed config block is rejected, not ignored.
func TestParse_RejectsMisplacedComponentField(t *testing.T) {
	// "partitions" belongs to keyBy, not map — map's typed config has
	// no such field, so it must be rejected.
	misplaced := `
source: { type: stdout }
pipeline:
  - type: map
    map: { ref: parse, partitions: 8 }
sink: { type: stdout }
`
	if _, err := workflow.ParseYAML([]byte(misplaced)); err == nil {
		t.Error("expected rejection of 'partitions' inside a map config")
	}

	// Misspelled field inside a typed config block.
	typo := `
source: { type: stdout }
pipeline:
  - type: window
    window: { type: tumbling, siZe: 5m }
sink: { type: stdout }
`
	if _, err := workflow.ParseYAML([]byte(typo)); err == nil {
		t.Error("expected rejection of misspelled 'siZe' inside a window config")
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
	if fromJSON.Pipeline[2].Window.Size.Std() != 5*time.Minute {
		t.Errorf("json round-trip lost window size: %s", fromJSON.Pipeline[2].Window.Size)
	}
}
