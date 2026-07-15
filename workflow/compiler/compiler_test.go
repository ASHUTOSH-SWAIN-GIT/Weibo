package compiler_test

import (
	"context"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/compiler"
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
// Mailer pipeline without starting it.
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
			DSN: "${MAILER_TEST_MISSING_DSN}", Table: "t", Mapping: map[string]string{"amount": "amount"},
		},
	}
	if _, err := c.Compile(wf); err == nil {
		t.Fatal("expected unresolved ${VAR} in DSN to fail compilation")
	}
}
