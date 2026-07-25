package compiler_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/compiler"
)

// declarativeWF builds a fully declarative workflow (no user code):
// generator source → filter → keyBy → reduce(count) → stdout.
func declarativeWF() *workflow.WorkflowSpec {
	return &workflow.Workflow{
		Name:    "orders",
		Version: "1",
		Source: workflow.SourceSpec{
			Type: "generator",
			Records: []workflow.RecordSpec{
				{Key: "a", Value: `{"status":"completed","customer_id":"c1","amount":10}`},
				{Key: "b", Value: `{"status":"pending","customer_id":"c2","amount":20}`},
				{Key: "c", Value: `{"status":"completed","customer_id":"c1","amount":30}`},
			},
		},
		Pipeline: []workflow.Operator{
			{ID: "completed", Type: "filter", Filter: &workflow.FilterConfig{Field: "status", Operator: "equals", Value: "completed"}},
			{ID: "drop-status", Type: "selectFields", SelectFields: &workflow.SelectConfig{Fields: []string{"customer_id", "amount"}}},
			{ID: "by-customer", Type: "keyBy", KeyBy: &workflow.KeyByConfig{Field: "customer_id", Partitions: 2}},
			{ID: "totals", Type: "reduce", Reduce: &workflow.ReduceConfig{Function: "sum", Field: "amount"}},
		},
		Sink: workflow.SinkSpec{Type: "blackhole"},
	}
}

// The success condition: a workflow compiles into a complete, runnable
// Weibo pipeline without starting it.
func TestCompile_CompletePipelineWithoutStarting(t *testing.T) {
	c := &compiler.Compiler{BaseDataDir: t.TempDir()}

	env, err := c.Compile(declarativeWF())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if env == nil {
		t.Fatal("expected an executable environment")
	}

	// The pipeline was not started — nothing ran. We can now execute it
	// on demand, and it completes over the in-memory generator source.
	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute the compiled pipeline: %v", err)
	}
}

func TestCompileWorkflow_GraphAndDelivery(t *testing.T) {
	c := &compiler.Compiler{BaseDataDir: t.TempDir()}
	cw, err := c.CompileWorkflow(declarativeWF())
	if err != nil {
		t.Fatalf("CompileWorkflow: %v", err)
	}
	if cw.Name != "orders" {
		t.Errorf("name: got %q", cw.Name)
	}
	if cw.Graph.Source != "generator" || cw.Graph.Sink != "blackhole" {
		t.Errorf("graph endpoints: %+v", cw.Graph)
	}
	wantOps := []string{"completed", "drop-status", "by-customer", "totals"}
	if len(cw.Graph.Operators) != len(wantOps) {
		t.Fatalf("graph ops: got %d, want %d", len(cw.Graph.Operators), len(wantOps))
	}
	for i, id := range wantOps {
		if cw.Graph.Operators[i].ID != id {
			t.Errorf("graph op %d: got %q, want %q", i, cw.Graph.Operators[i].ID, id)
		}
	}
	// No checkpointing → at-most-once.
	if cw.Delivery != compiler.AtMostOnce {
		t.Errorf("delivery: got %q, want at-most-once", cw.Delivery)
	}
}

func TestCompile_DeliveryGuarantees(t *testing.T) {
	base := t.TempDir()

	// at-least-once: checkpointing on, plain sink.
	al := declarativeWF()
	al.Env = &workflow.EnvSpec{
		Checkpointing: &workflow.CheckpointSpec{Interval: workflow.Duration(1e9)},
	}
	cw, err := (&compiler.Compiler{BaseDataDir: base}).CompileWorkflow(al)
	if err != nil {
		t.Fatalf("at-least-once compile: %v", err)
	}
	if cw.Delivery != compiler.AtLeastOnce {
		t.Errorf("delivery: got %q, want at-least-once", cw.Delivery)
	}
}

// An invalid workflow must not compile (never reaches the runtime).
func TestCompile_InvalidWorkflowRejected(t *testing.T) {
	c := &compiler.Compiler{BaseDataDir: t.TempDir()}
	wf := declarativeWF()
	wf.Pipeline[3].ID = "" // missing operator id → validation error
	if _, err := c.Compile(wf); err == nil {
		t.Fatal("expected an invalid workflow to be rejected before compilation")
	}
}

// Ref-based operators cannot be compiled without a function registry.
func TestCompile_RefOperatorRejected(t *testing.T) {
	c := &compiler.Compiler{BaseDataDir: t.TempDir()}
	wf := declarativeWF()
	wf.Pipeline = []workflow.Operator{
		{ID: "x", Type: "map", Map: &workflow.RefConfig{Ref: "enrich"}},
	}
	if _, err := c.Compile(wf); err == nil {
		t.Fatal("expected a ref-based operator to be rejected")
	}
}

// Connection resolution: an unset ${VAR} in a DSN fails compilation
// (without connecting).
func TestCompile_UnresolvedConnectionFails(t *testing.T) {
	c := &compiler.Compiler{BaseDataDir: t.TempDir()}
	wf := declarativeWF()
	wf.Sink = workflow.SinkSpec{
		Type: "postgres",
		Postgres: &workflow.PostgresSinkSpec{
			DSN: "${WEIBO_TEST_MISSING_DSN}", Table: "t", Mapping: map[string]string{"amount": "amount"},
		},
	}
	if _, err := c.Compile(wf); err == nil {
		t.Fatal("expected unresolved ${VAR} in DSN to fail compilation")
	}
}

func TestCompileWorkflow_SecretsDoNotLeakToDescriptions(t *testing.T) {
	t.Setenv("KAFKA_USERNAME", "actual-user")
	t.Setenv("KAFKA_PASSWORD", "actual-password")

	wf := &workflow.Workflow{
		Name:    "secret-kafka",
		Version: "1",
		Source: workflow.SourceSpec{
			Type: "kafka",
			Kafka: &workflow.KafkaSourceSpec{
				Brokers: []string{"localhost:9092"},
				Topic:   "orders",
				GroupID: "secret-kafka",
				SASL: &workflow.SASLSpec{
					Mechanism: "plain",
					Username:  "${KAFKA_USERNAME}",
					Password:  "${KAFKA_PASSWORD}",
				},
			},
		},
		Pipeline: []workflow.Operator{
			{ID: "by-key", Type: "keyBy", KeyBy: &workflow.KeyByConfig{Field: "customer_id"}},
			{ID: "count", Type: "reduce", Reduce: &workflow.ReduceConfig{Function: "count"}},
		},
		Sink: workflow.SinkSpec{
			Type: "kafka",
			Kafka: &workflow.KafkaSinkSpec{
				Brokers: []string{"localhost:9092"},
				Topic:   "counts",
				SASL: &workflow.SASLSpec{
					Mechanism: "plain",
					Username:  "${KAFKA_USERNAME}",
					Password:  "${KAFKA_PASSWORD}",
				},
			},
		},
	}

	cw, err := (&compiler.Compiler{BaseDataDir: t.TempDir()}).CompileWorkflow(wf)
	if err != nil {
		t.Fatalf("CompileWorkflow: %v", err)
	}

	graph := cw.Graph
	if strings.Contains(graph.Source, "actual-user") || strings.Contains(graph.Sink, "actual-password") {
		t.Fatalf("compiled graph leaked a secret: %+v", graph)
	}

	desc := cw.Env.DescribeJSON()
	for _, secret := range []string{"actual-user", "actual-password"} {
		if strings.Contains(desc, secret) {
			t.Fatalf("pipeline description leaked %q: %s", secret, desc)
		}
	}
}
