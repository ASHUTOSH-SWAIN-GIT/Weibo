package workflow_test

import (
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/record"
)

func TestInvalidOperatorOrderingRejected(t *testing.T) {
	doc := `name: bad-order
source: {type: generator, records: [{value: '{"amount":1}'}]}
pipeline:
  - id: sum
    type: reduce
    reduce: {function: sum, field: amount}
sink: {type: stdout}
`
	wf, err := workflow.ParseYAML([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	err = workflow.Validate(wf)
	if err == nil || !strings.Contains(err.Error(), "reduce requires a keyBy before it") {
		t.Fatalf("expected ordering rejection, got %v", err)
	}
}

func TestUnsupportedFieldsRejected(t *testing.T) {
	doc := `name: typo
source:
  type: generator
  records: [{value: "{}"}]
sink:
  type: stdout
  extra: nope
`
	if _, err := workflow.ParseYAML([]byte(doc)); err == nil {
		t.Fatal("expected strict parser to reject unsupported field")
	}
}

func FuzzFieldPaths(f *testing.F) {
	f.Add("customer.id")
	f.Add("payment.total")
	f.Add("")
	f.Add("a..b")

	f.Fuzz(func(t *testing.T, path string) {
		doc := record.JSONRecord{"customer": map[string]any{"id": "c1"}}
		_, _ = record.GetField(doc, path)
		_ = record.SetField(doc, path, "x")
		_ = record.DeleteField(doc, path)
	})
}
