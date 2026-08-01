package pipeline

import (
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
)

// PlanNode is one execution stage, described for the dashboard DAG.
// Name matches the `stage` label on the weibo_stage_* metrics, so the
// UI can overlay live throughput/worker counts onto each node.
type PlanNode struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"` // source | stateless | keyed | channel | sink
	Parallelism int      `json:"parallelism"`
	Operators   []string `json:"operators,omitempty"`
}

// PlanEdge is a bounded edge between two consecutive stages. Name matches
// the `edge` label on the weibo_edge_queue_* metrics (edge-0, edge-1, …),
// so the UI can color each edge by its live queue fill.
type PlanEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Name string `json:"name"`
}

// PlanGraph is the executed stage topology: the nodes the planner built
// and the edges wiring them, keyed so metric labels line up exactly.
type PlanGraph struct {
	Stages []PlanNode `json:"stages"`
	Edges  []PlanEdge `json:"edges"`
}

// DescribePlan turns a built stage list into a PlanGraph. Edges are named
// edge-<i> to match the names mailer.go assigns (one bounded edge between
// each consecutive stage pair) and the weibo_edge_queue metric labels.
func DescribePlan(stages []Stage) PlanGraph {
	g := PlanGraph{}
	for _, st := range stages {
		g.Stages = append(g.Stages, planNode(st))
	}
	for i := 0; i+1 < len(stages); i++ {
		g.Edges = append(g.Edges, PlanEdge{
			From: stages[i].Name(),
			To:   stages[i+1].Name(),
			Name: fmt.Sprintf("edge-%d", i),
		})
	}
	return g
}

// planNode reads the type, parallelism and member operators off a concrete
// stage. Types mirror the newStageMetrics "typ" argument each stage passes.
func planNode(st Stage) PlanNode {
	switch s := st.(type) {
	case *SourceStage:
		return PlanNode{Name: s.Name(), Type: "source", Parallelism: 1}
	case *StatelessStage:
		par := s.Parallelism
		if par < 1 {
			par = 1
		}
		return PlanNode{Name: s.StageName, Type: "stateless", Parallelism: par, Operators: opNames(s.Ops)}
	case *KeyedStage:
		ops := []string{s.KeyBy.Name()}
		if len(s.workers) > 0 {
			ops = append(ops, opNames(s.workers[0])...)
		}
		return PlanNode{Name: s.Name(), Type: "keyed", Parallelism: len(s.workers), Operators: ops}
	case *ChannelStage:
		return PlanNode{Name: s.Name(), Type: "channel", Parallelism: 1, Operators: []string{s.Op.Name()}}
	case *SinkStage:
		return PlanNode{Name: s.Name(), Type: "sink", Parallelism: 1}
	default:
		return PlanNode{Name: st.Name(), Type: "stage", Parallelism: 1}
	}
}

func opNames(ops []operator.Operator) []string {
	out := make([]string, len(ops))
	for i, o := range ops {
		out[i] = o.Name()
	}
	return out
}
