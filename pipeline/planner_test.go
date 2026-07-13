package pipeline_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/pipeline"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/window"
)

func buildPlan(t *testing.T, ops ...operator.Operator) []pipeline.Stage {
	t.Helper()
	labels := make([]string, len(ops))
	for i, op := range ops {
		labels[i] = fmt.Sprintf("%s-%d", op.Name(), i)
	}
	plan, err := pipeline.BuildPlan(pipeline.PlanConfig{
		Source:    source.NewSliceSource(nil),
		Operators: ops,
		Labels:    labels,
		Sink:      sink.NewBlackholeSink(),
	})
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}
	return plan
}

func stageNames(plan []pipeline.Stage) []string {
	names := make([]string, len(plan))
	for i, s := range plan {
		names[i] = s.Name()
	}
	return names
}

func TestPlanner_ConsecutiveStatelessShareOneStage(t *testing.T) {
	plan := buildPlan(t,
		operator.Map(func(r types.Record) types.Record { return r }),
		operator.Filter(func(r types.Record) bool { return true }),
	)
	// source, stateless-0 (Map+Filter), sink
	if len(plan) != 3 {
		t.Fatalf("expected 3 stages, got %d: %v", len(plan), stageNames(plan))
	}
	if plan[1].Name() != "stateless-0" {
		t.Errorf("expected stateless-0, got %s", plan[1].Name())
	}
}

func TestPlanner_KeyByStartsKeyedStageConsumingStateful(t *testing.T) {
	plan := buildPlan(t,
		operator.Map(func(r types.Record) types.Record { return r }),
		operator.KeyBy(func(r types.Record) []byte { return r.Key }),
		operator.Window(window.NewTumbling(time.Second)),
		operator.Reduce(func(a []byte, c types.Record) []byte { return a }),
		operator.Map(func(r types.Record) types.Record { return r }),
	)
	// source, stateless-0 (Map), keyed-0 (Window+Reduce), stateless-1 (Map), sink
	want := []string{"source", "stateless-0", "keyed-0", "stateless-1", "sink"}
	got := stageNames(plan)
	if len(got) != len(want) {
		t.Fatalf("expected stages %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected stages %v, got %v", want, got)
		}
	}
}

func TestPlanner_ParallelismChangeSplitsStage(t *testing.T) {
	m1 := operator.Map(func(r types.Record) types.Record { return r })
	m2 := operator.Map(func(r types.Record) types.Record { return r })
	m2.SetParallelism(4)
	m3 := operator.Map(func(r types.Record) types.Record { return r })
	m3.SetParallelism(4)

	plan := buildPlan(t, m1, m2, m3)
	// source, stateless-0 (m1, par 1), stateless-1 (m2+m3, par 4), sink
	if len(plan) != 4 {
		t.Fatalf("expected 4 stages, got %d: %v", len(plan), stageNames(plan))
	}
}

func TestPlanner_StatefulWithoutKeyByGetsChannelStage(t *testing.T) {
	plan := buildPlan(t,
		operator.Window(window.NewTumbling(time.Second)),
	)
	// source, op-Window-0, sink
	if len(plan) != 3 {
		t.Fatalf("expected 3 stages, got %d: %v", len(plan), stageNames(plan))
	}
	if plan[1].Name() != "op-Window-0" {
		t.Errorf("expected op-Window-0 channel stage, got %s", plan[1].Name())
	}
}

func TestPlanner_RejectsInvalidPartitions(t *testing.T) {
	kb := operator.KeyBy(func(r types.Record) []byte { return r.Key })
	kb.Partitions = 0
	_, err := pipeline.BuildPlan(pipeline.PlanConfig{
		Source:    source.NewSliceSource(nil),
		Operators: []operator.Operator{kb},
		Labels:    []string{"keyby"},
		Sink:      sink.NewBlackholeSink(),
	})
	if err == nil {
		t.Fatal("expected error for partitions=0, got nil")
	}
}

func TestPlanner_NoOperators(t *testing.T) {
	plan := buildPlan(t)
	// source, sink — still a valid pipeline
	if len(plan) != 2 {
		t.Fatalf("expected 2 stages, got %d: %v", len(plan), stageNames(plan))
	}
}
