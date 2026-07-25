package workflow_test

import (
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/compiler"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/operators"
)

func TestCompilationBuildsGraphWithoutSecrets(t *testing.T) {
	t.Setenv("KAFKA_USERNAME", "compiled-user")
	t.Setenv("KAFKA_PASSWORD", "compiled-password")

	doc := `name: secret-pipeline
source:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: in
    groupID: g
    sasl: {mechanism: plain, username: "${KAFKA_USERNAME}", password: "${KAFKA_PASSWORD}"}
pipeline:
  - id: by-customer
    type: keyBy
    keyBy: {field: customer.id, partitions: 1}
  - id: count
    type: reduce
    reduce: {function: count}
sink:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: out
    sasl: {mechanism: plain, username: "${KAFKA_USERNAME}", password: "${KAFKA_PASSWORD}"}
`
	wf, err := workflow.ParseYAML([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	cw, err := (&compiler.Compiler{BaseDataDir: t.TempDir()}).CompileWorkflow(wf)
	if err != nil {
		t.Fatalf("CompileWorkflow: %v", err)
	}
	if cw.Graph.Source != "kafka" || cw.Graph.Sink != "kafka" || len(cw.Graph.Operators) != 2 {
		t.Fatalf("unexpected graph: %+v", cw.Graph)
	}
	desc := cw.Env.DescribeJSON()
	for _, secret := range []string{"compiled-user", "compiled-password"} {
		if strings.Contains(desc, secret) {
			t.Fatalf("description leaked secret %q: %s", secret, desc)
		}
	}
}

func FuzzFilterComparisons(f *testing.F) {
	f.Add("equals", "status", "completed")
	f.Add("greater_than", "amount", "10")
	f.Add("contains", "note", "x")

	f.Fuzz(func(t *testing.T, op, field, value string) {
		filter := operators.BuildFilter(operators.FilterConfig{Field: field, Operator: op, Value: value})
		_ = filter
		_, _ = operators.Compare(operators.CompareOp(op), value, true, value)
	})
}
