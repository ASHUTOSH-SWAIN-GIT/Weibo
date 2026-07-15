package workflow_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
)

func writeFile(path, content string) error { return os.WriteFile(path, []byte(content), 0o644) }

// validEO builds a fully valid exactly-once workflow rooted at a temp
// dir (so directory-createability checks pass without prod paths).
func validEO(t *testing.T) *workflow.Workflow {
	t.Helper()
	root := t.TempDir()
	return &workflow.Workflow{
		Name:    "order-totals",
		Version: "1",
		Env: &workflow.EnvSpec{
			Checkpointing: &workflow.CheckpointSpec{
				Interval: workflow.Duration(30e9), // 30s
				Dir:      filepath.Join(root, "ckpt"),
			},
			State: &workflow.StateSpec{Backend: "pebble", Dir: filepath.Join(root, "state")},
		},
		Source: workflow.SourceSpec{
			Type: "kafka",
			Kafka: &workflow.KafkaSourceSpec{
				Brokers:     []string{"localhost:9092"},
				Topic:       "orders",
				GroupID:     "g",
				ExactlyOnce: true,
			},
		},
		Pipeline: []workflow.Operator{
			{ID: "filter", Type: "filter", Filter: &workflow.FilterConfig{Field: "status", Operator: "equals", Value: "completed"}},
			{ID: "key", Type: "keyBy", KeyBy: &workflow.KeyByConfig{Field: "customer_id", Partitions: 8}},
			{ID: "win", Type: "window", Window: &workflow.WindowConfig{Type: "tumbling", Size: workflow.Duration(300e9)}},
			{ID: "totals", Type: "reduce", Reduce: &workflow.ReduceConfig{Function: "sum", Field: "amount"}},
		},
		Sink: workflow.SinkSpec{
			Type: "txnKafka",
			TxnKafka: &workflow.TxnKafkaSinkSpec{
				Brokers:         []string{"localhost:9092"},
				Topic:           "order-totals",
				TransactionalID: "order-totals-pipeline",
			},
		},
	}
}

func TestValidate_ValidWorkflowPasses(t *testing.T) {
	if err := workflow.Validate(validEO(t)); err != nil {
		t.Fatalf("expected valid workflow, got:\n%v", err)
	}
}

// errmsg runs Validate and returns the aggregated message, failing if
// the workflow unexpectedly validated.
func errmsg(t *testing.T, wf *workflow.Workflow) string {
	t.Helper()
	err := workflow.Validate(wf)
	if err == nil {
		t.Fatal("expected validation to fail")
	}
	return err.Error()
}

func mustContain(t *testing.T, msg string, subs ...string) {
	t.Helper()
	for _, s := range subs {
		if !strings.Contains(msg, s) {
			t.Errorf("validation message missing %q; full message:\n%s", s, msg)
		}
	}
}

func TestValidate_Structural(t *testing.T) {
	wf := validEO(t)
	wf.Version = "9"
	wf.Name = ""
	msg := errmsg(t, wf)
	mustContain(t, msg, "Workflow validation failed:", "unsupported workflow version", "name is required")
}

func TestValidate_OperatorIDsRequiredAndUnique(t *testing.T) {
	wf := validEO(t)
	wf.Pipeline[0].ID = ""       // missing id
	wf.Pipeline[3].ID = "key"    // duplicate of pipeline[1]
	msg := errmsg(t, wf)
	mustContain(t, msg, "operator id is required", `duplicate operator id "key"`)
}

func TestValidate_UnsupportedTypes(t *testing.T) {
	wf := validEO(t)
	wf.Source.Type = "rabbitmq"
	wf.Sink.Type = "s3"
	wf.Pipeline[0].Type = "transmogrify"
	msg := errmsg(t, wf)
	mustContain(t, msg, "unsupported source type", "unsupported sink type", "unsupported operator type")
}

func TestValidate_TypeConfigMismatch(t *testing.T) {
	wf := validEO(t)
	// type says map, but a keyBy block is set instead.
	wf.Pipeline[0] = workflow.Operator{ID: "x", Type: "map", KeyBy: &workflow.KeyByConfig{Field: "k"}}
	msg := errmsg(t, wf)
	mustContain(t, msg, "does not match config block")
}

func TestValidate_KafkaConfig(t *testing.T) {
	wf := validEO(t)
	wf.Source.Kafka.Brokers = nil
	wf.Source.Kafka.Topic = ""
	msg := errmsg(t, wf)
	mustContain(t, msg, "source.kafka.brokers", "at least one broker is required", "a topic (or topics) is required")
}

func TestValidate_WindowDurations(t *testing.T) {
	wf := validEO(t)
	wf.Pipeline[2].Window = &workflow.WindowConfig{Type: "sliding", Size: workflow.Duration(60e9), Slide: workflow.Duration(120e9)}
	msg := errmsg(t, wf)
	mustContain(t, msg, "slide must be less than or equal to size")

	wf2 := validEO(t)
	wf2.Pipeline[2].Window = &workflow.WindowConfig{Type: "tumbling"} // size 0
	mustContain(t, errmsg(t, wf2), "window size must be greater than zero")
}

func TestValidate_Partitions(t *testing.T) {
	wf := validEO(t)
	wf.Pipeline[1].KeyBy.Partitions = -1
	msg := errmsg(t, wf)
	mustContain(t, msg, "partitions must be greater than zero")
}

func TestValidate_CheckpointInterval(t *testing.T) {
	wf := validEO(t)
	wf.Env.Checkpointing.Interval = 0
	msg := errmsg(t, wf)
	mustContain(t, msg, "env.checkpointing.interval", "must be greater than zero")
}

func TestValidate_PebbleDirCreatable(t *testing.T) {
	wf := validEO(t)
	// A path whose parent is a file cannot be created.
	root := t.TempDir()
	fileAsParent := filepath.Join(root, "afile")
	if err := writeFile(fileAsParent, "x"); err != nil {
		t.Fatal(err)
	}
	wf.Env.State.Dir = filepath.Join(fileAsParent, "state")
	msg := errmsg(t, wf)
	mustContain(t, msg, "cannot be created")
}

func TestValidate_PipelineOrdering_KeyByBeforeReduce(t *testing.T) {
	wf := validEO(t)
	// Reorder so reduce comes before any keyBy.
	wf.Pipeline = []workflow.Operator{
		{ID: "totals", Type: "reduce", Reduce: &workflow.ReduceConfig{Function: "count"}},
		{ID: "key", Type: "keyBy", KeyBy: &workflow.KeyByConfig{Field: "k"}},
	}
	msg := errmsg(t, wf)
	mustContain(t, msg, `pipeline[0] "totals"`, "reduce requires a keyBy before it")
}

func TestValidate_PipelineOrdering_WindowBeforeReduce(t *testing.T) {
	wf := validEO(t)
	wf.Pipeline = []workflow.Operator{
		{ID: "key", Type: "keyBy", KeyBy: &workflow.KeyByConfig{Field: "k"}},
		{ID: "totals", Type: "reduce", Reduce: &workflow.ReduceConfig{Function: "count"}},
		{ID: "win", Type: "window", Window: &workflow.WindowConfig{Type: "tumbling", Size: workflow.Duration(60e9)}},
	}
	msg := errmsg(t, wf)
	mustContain(t, msg, "window must appear before the aggregation that consumes it")
}

func TestValidate_ExactlyOnce_RequiresFullSet(t *testing.T) {
	// Source asks for exactly-once but sink is stdout and no checkpointing.
	wf := validEO(t)
	wf.Sink = workflow.SinkSpec{Type: "stdout"}
	wf.Env.Checkpointing = nil
	msg := errmsg(t, wf)
	mustContain(t, msg,
		"exactly-once requires a txnKafka",
		"exactly-once requires checkpointing")
}

func TestValidate_TxnSink_RequiresTransactionalID(t *testing.T) {
	wf := validEO(t)
	wf.Sink.TxnKafka.TransactionalID = ""
	msg := errmsg(t, wf)
	mustContain(t, msg, "sink.txnKafka.transactionalID", "transactional id is required for transactional Kafka")
}

// The headline: several unrelated problems are reported together.
func TestValidate_ReportsMultipleErrorsTogether(t *testing.T) {
	wf := validEO(t)
	wf.Env.Checkpointing.Interval = 0                  // checkpoint interval
	wf.Sink.TxnKafka.TransactionalID = ""              // txn id
	wf.Pipeline = []workflow.Operator{                 // reduce before keyBy
		{ID: "totals", Type: "reduce", Reduce: &workflow.ReduceConfig{Function: "count"}},
	}

	msg := errmsg(t, wf)
	// Header + all three distinct problems in one aggregated error.
	mustContain(t, msg,
		"Workflow validation failed:",
		`pipeline[0] "totals": reduce requires a keyBy before it`,
		"env.checkpointing.interval: checkpoint interval must be greater than zero",
		"sink.txnKafka.transactionalID: a transactional id is required for transactional Kafka",
	)
	if got := strings.Count(msg, "\n  - "); got < 3 {
		t.Errorf("expected at least 3 bulleted errors, got %d:\n%s", got, msg)
	}
}
