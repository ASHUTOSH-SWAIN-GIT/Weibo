package workflow_test

import (
	"encoding/json"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/operators"
)

func TestYAMLAndSDKPipelinesProduceIdenticalResults(t *testing.T) {
	baseDir := t.TempDir()
	workflowOut := runWorkflowYAML(t, baseWorkflowYAML("memory", "", ""), baseDir)
	sdkOut := runSDKOrders(t, "memory", "", "")
	assertSameValues(t, workflowOut, sdkOut)
}

func TestNestedJSONFieldsAndNumericAggregation(t *testing.T) {
	out := runWorkflowYAML(t, baseWorkflowYAML("memory", "", ""), t.TempDir())
	assertSameValues(t, out, []string{`{"sum":10}`, `{"sum":15}`})
}

func TestMemoryAndPebbleStateProduceIdenticalResults(t *testing.T) {
	mem := runWorkflowYAML(t, baseWorkflowYAML("memory", "", ""), t.TempDir())
	pebbleRoot := t.TempDir()
	pebble := runWorkflowYAML(t, baseWorkflowYAML("pebble", pebbleRoot, ""), t.TempDir())
	assertSameValues(t, pebble, mem)
}

func FuzzNumericConversions(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(42))
	f.Add(int64(900719925474099))

	f.Fuzz(func(t *testing.T, n int64) {
		rec := types.Record{Value: []byte(`{"amount":` + json.Number(stringInt(n)).String() + `}`)}
		yamlLike := operators.BuildFilter(operators.FilterConfig{Field: "amount", Operator: "equals", Value: n})
		jsonLike := operators.BuildFilter(operators.FilterConfig{Field: "amount", Operator: "equals", Value: float64(n)})
		if n > -9_007_199_254_740_992 && n < 9_007_199_254_740_992 && yamlLike(rec) != jsonLike(rec) {
			t.Fatalf("small numeric equality diverged for %d", n)
		}
	})
}

func stringInt(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}
