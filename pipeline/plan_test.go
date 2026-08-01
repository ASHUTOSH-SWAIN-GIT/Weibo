package pipeline_test

import (
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/pipeline"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/window"
)

// TestDescribePlan_NodesAndEdgesMatchMetricLabels locks the plan-graph
// contract the dashboard depends on: node names equal the weibo_stage_*
// `stage` labels, node types equal the newStageMetrics type, edges are
// named edge-<i> (the weibo_edge_queue `edge` label), and keyed
// parallelism reflects the KeyBy partition count.
func TestDescribePlan_NodesAndEdgesMatchMetricLabels(t *testing.T) {
	plan := buildPlan(t,
		operator.Map(func(r types.Record) types.Record { return r }),
		operator.KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(8),
		operator.Window(window.NewTumbling(time.Second)),
		operator.Reduce(func(a []byte, c types.Record) []byte { return a }),
	)
	g := pipeline.DescribePlan(plan)

	// Stages: source, stateless-0 (Map), keyed-0 (Window+Reduce), sink.
	wantNames := []string{"source", "stateless-0", "keyed-0", "sink"}
	wantTypes := []string{"source", "stateless", "keyed", "sink"}
	if len(g.Stages) != len(wantNames) {
		t.Fatalf("stage count: got %d (%+v), want %d", len(g.Stages), g.Stages, len(wantNames))
	}
	for i, n := range wantNames {
		if g.Stages[i].Name != n {
			t.Errorf("stage %d name: got %q, want %q", i, g.Stages[i].Name, n)
		}
		if g.Stages[i].Type != wantTypes[i] {
			t.Errorf("stage %d type: got %q, want %q", i, g.Stages[i].Type, wantTypes[i])
		}
	}

	// Keyed stage carries the partition count as parallelism.
	if p := g.Stages[2].Parallelism; p != 8 {
		t.Errorf("keyed parallelism: got %d, want 8", p)
	}

	// Edges: edge-0..edge-2, wiring consecutive stages.
	if len(g.Edges) != 3 {
		t.Fatalf("edge count: got %d (%+v), want 3", len(g.Edges), g.Edges)
	}
	for i, e := range g.Edges {
		if e.Name != "edge-"+itoa(i) {
			t.Errorf("edge %d name: got %q, want edge-%d", i, e.Name, i)
		}
		if e.From != wantNames[i] || e.To != wantNames[i+1] {
			t.Errorf("edge %d: got %s->%s, want %s->%s", i, e.From, e.To, wantNames[i], wantNames[i+1])
		}
	}
}

func itoa(i int) string { return string(rune('0' + i)) }
