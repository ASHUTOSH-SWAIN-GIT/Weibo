package workflow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/window"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/compiler"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/record"
)

func TestParseYAMLAndJSON(t *testing.T) {
	yamlDoc := baseWorkflowYAML("memory", "", "")
	wf, err := workflow.ParseYAML([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("ParseYAML: %v", err)
	}
	if wf.Name != "orders" || wf.Source.Type != "generator" || wf.Sink.Type != "stdout" {
		t.Fatalf("decoded wrong workflow: %+v", wf)
	}

	jsonDoc := `{
		"name": "orders-json",
		"version": "1",
		"source": {"type": "generator", "records": [{"key": "a", "value": "{\"customer\":{\"id\":\"c1\"},\"amount\":1}"}]},
		"pipeline": [
			{"id": "by-customer", "type": "keyBy", "keyBy": {"field": "customer.id", "partitions": 1}},
			{"id": "sum", "type": "reduce", "reduce": {"function": "sum", "field": "amount"}}
		],
		"sink": {"type": "stdout"}
	}`
	if _, err := workflow.ParseJSON([]byte(jsonDoc)); err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
}

func FuzzParseYAML(f *testing.F) {
	f.Add(baseWorkflowYAML("memory", "", ""))
	f.Add("name: bad\nsource:\n  type: nope\n")
	f.Add("pipeline:\n  - type: filter\n    filter:\n      field: x\n")

	f.Fuzz(func(t *testing.T, doc string) {
		wf, err := workflow.ParseYAML([]byte(doc))
		if err == nil {
			_ = workflow.Validate(wf)
		}
	})
}

func baseWorkflowYAML(stateBackend, stateDir, checkpointDir string) string {
	env := ""
	if stateBackend != "" || checkpointDir != "" {
		env = "env:\n"
		if checkpointDir != "" {
			env += fmt.Sprintf("  checkpointing:\n    interval: 5ms\n    dir: %s\n", checkpointDir)
		}
		if stateBackend != "" {
			env += fmt.Sprintf("  state:\n    backend: %s\n", stateBackend)
			if stateDir != "" {
				env += fmt.Sprintf("    dir: %s\n", stateDir)
			}
		}
	}
	return fmt.Sprintf(`name: orders
version: "1"
%ssource:
  type: generator
  records:
    - key: a
      value: '{"customer":{"id":"c1"},"amount":10,"status":"completed"}'
    - key: b
      value: '{"customer":{"id":"c1"},"amount":15,"status":"pending"}'
    - key: c
      value: '{"customer":{"id":"c1"},"amount":5,"status":"completed"}'
pipeline:
  - id: completed
    type: filter
    filter: {field: status, operator: equals, value: completed}
  - id: by-customer
    type: keyBy
    keyBy: {field: customer.id, partitions: 1}
  - id: sum
    type: reduce
    reduce: {function: sum, field: amount}
sink:
  type: stdout
`, env)
}

func runWorkflowYAML(t *testing.T, yamlDoc string, baseDir string) []string {
	t.Helper()
	wf, err := workflow.ParseYAML([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("ParseYAML: %v", err)
	}
	env, err := (&compiler.Compiler{BaseDataDir: baseDir}).Compile(wf)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return stdoutValues(t, func() error { return env.Execute(context.Background()) })
}

func runSDKOrders(t *testing.T, stateBackend, stateDir, checkpointDir string) []string {
	t.Helper()
	env := mailer.NewEnv()
	if stateBackend == "pebble" {
		env, _ = compiler.CompileRuntime("orders-sdk", t.TempDir(), &workflow.EnvSpec{
			State: &workflow.StateSpec{Backend: "pebble", Dir: stateDir},
		})
	}
	if checkpointDir != "" {
		env, _ = compiler.CompileRuntime("orders-sdk", t.TempDir(), &workflow.EnvSpec{
			Checkpointing: &workflow.CheckpointSpec{Interval: workflow.Duration(5_000_000), Dir: checkpointDir},
			State:         &workflow.StateSpec{Backend: stateBackend, Dir: stateDir},
		})
	}
	records := []types.Record{
		{Key: []byte("a"), Value: []byte(`{"customer":{"id":"c1"},"amount":10,"status":"completed"}`)},
		{Key: []byte("b"), Value: []byte(`{"customer":{"id":"c1"},"amount":15,"status":"pending"}`)},
		{Key: []byte("c"), Value: []byte(`{"customer":{"id":"c1"},"amount":5,"status":"completed"}`)},
	}
	env.FromSource(source.NewGeneratorSource(records)).
		Filter(func(r types.Record) bool {
			v, ok := field(r, "status")
			return ok && v == "completed"
		}, "completed").
		KeyBy(func(r types.Record) []byte {
			v, _ := field(r, "customer.id")
			return []byte(fmt.Sprint(v))
		}, "by-customer").WithPartitions(1).
		Reduce(sumReducer("amount"), "sum").
		ToSink(sink.NewStdoutSink())
	return stdoutValues(t, func() error { return env.Execute(context.Background()) })
}

func runWorkflowWindow(t *testing.T, stateBackend, stateDir, checkpointDir string) []string {
	t.Helper()
	envSpec := ""
	if stateBackend != "" || checkpointDir != "" {
		envSpec = "env:\n"
		if checkpointDir != "" {
			envSpec += fmt.Sprintf("  checkpointing:\n    interval: 5ms\n    dir: %s\n", checkpointDir)
		}
		if stateBackend != "" {
			envSpec += fmt.Sprintf("  state:\n    backend: %s\n", stateBackend)
			if stateDir != "" {
				envSpec += fmt.Sprintf("    dir: %s\n", stateDir)
			}
		}
	}
	doc := fmt.Sprintf(`name: windowed
version: "1"
%ssource:
  type: generator
  records:
    - {key: a, value: '{"customer":{"id":"c1"},"amount":2}'}
    - {key: b, value: '{"customer":{"id":"c1"},"amount":3}'}
pipeline:
  - id: by-customer
    type: keyBy
    keyBy: {field: customer.id, partitions: 1}
  - id: window
    type: window
    window: {type: tumbling, size: 1s}
  - id: sum
    type: reduce
    reduce: {function: sum, field: amount}
sink:
  type: stdout
`, envSpec)
	return runWorkflowYAML(t, doc, t.TempDir())
}

func runSDKWindow(t *testing.T, stateBackend, stateDir, checkpointDir string) []string {
	t.Helper()
	env, err := compiler.CompileRuntime("windowed-sdk", t.TempDir(), &workflow.EnvSpec{
		Checkpointing: checkpointSpec(checkpointDir),
		State:         &workflow.StateSpec{Backend: stateBackend, Dir: stateDir},
	})
	if err != nil {
		t.Fatalf("CompileRuntime: %v", err)
	}
	records := []types.Record{
		{Key: []byte("a"), Value: []byte(`{"customer":{"id":"c1"},"amount":2}`)},
		{Key: []byte("b"), Value: []byte(`{"customer":{"id":"c1"},"amount":3}`)},
	}
	env.FromSource(source.NewGeneratorSource(records)).
		KeyBy(func(r types.Record) []byte {
			v, _ := field(r, "customer.id")
			return []byte(fmt.Sprint(v))
		}, "by-customer").WithPartitions(1).
		Window(window.NewTumbling(1), "window").
		Reduce(sumReducer("amount"), "sum").
		ToSink(sink.NewStdoutSink())
	return stdoutValues(t, func() error { return env.Execute(context.Background()) })
}

func checkpointSpec(dir string) *workflow.CheckpointSpec {
	if dir == "" {
		return nil
	}
	return &workflow.CheckpointSpec{Interval: workflow.Duration(5_000_000), Dir: dir}
}

func stdoutValues(t *testing.T, run func() error) []string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	runErr := run()
	_ = w.Close()
	os.Stdout = orig
	out, readErr := io.ReadAll(r)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if runErr != nil {
		t.Fatalf("Execute: %v\nstdout:\n%s", runErr, out)
	}
	return parseStdoutValues(string(out))
}

func parseStdoutValues(out string) []string {
	var values []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		start := strings.Index(line, "value=")
		end := strings.Index(line, " timestamp=")
		if start == -1 || end == -1 || end < start {
			continue
		}
		value := line[start+len("value=") : end]
		if value == "" {
			continue
		}
		values = append(values, value)
	}
	return values
}

func canonicalValues(values []string) []string {
	out := make([]string, len(values))
	for i, v := range values {
		var decoded any
		if err := json.Unmarshal([]byte(v), &decoded); err == nil {
			b, _ := json.Marshal(decoded)
			out[i] = string(b)
		} else {
			out[i] = v
		}
	}
	sort.Strings(out)
	return out
}

func assertSameValues(t *testing.T, got, want []string) {
	t.Helper()
	g := canonicalValues(got)
	w := canonicalValues(want)
	if strings.Join(g, "\n") != strings.Join(w, "\n") {
		t.Fatalf("values differ\ngot:\n%s\nwant:\n%s", strings.Join(g, "\n"), strings.Join(w, "\n"))
	}
}

func field(r types.Record, path string) (any, bool) {
	jr, err := record.DecodeJSON(r)
	if err != nil {
		return nil, false
	}
	return record.GetField(jr, path)
}

func sumReducer(path string) func([]byte, types.Record) []byte {
	return func(accum []byte, r types.Record) []byte {
		total := 0.0
		if len(accum) > 0 {
			var m map[string]json.Number
			_ = json.Unmarshal(accum, &m)
			total, _ = m["sum"].Float64()
		}
		if v, ok := field(r, path); ok {
			switch n := v.(type) {
			case json.Number:
				f, _ := n.Float64()
				total += f
			case float64:
				total += n
			case int:
				total += float64(n)
			}
		}
		if total == float64(int64(total)) {
			return []byte(fmt.Sprintf(`{"sum":%d}`, int64(total)))
		}
		b, _ := json.Marshal(map[string]float64{"sum": total})
		return bytes.TrimSpace(b)
	}
}
