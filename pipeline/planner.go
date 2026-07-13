package pipeline

import (
	"fmt"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
)

// PlanConfig is the input to BuildPlan.
type PlanConfig struct {
	Source    source.Source
	Operators []operator.Operator
	Labels    []string // metric label per operator, aligned with Operators
	Sink      sink.Sink

	// DrainTimeout bounds the source offset flush on shutdown.
	DrainTimeout time.Duration

	// OnClone is called for every stateful operator clone a keyed
	// stage creates, so the caller can register it for checkpointing.
	OnClone func(operator.Operator)
}

// BuildPlan groups the flat operator list into execution stages:
//
//   - consecutive stateless operators (SingleProcessor) with the same
//     parallelism share one StatelessStage
//   - a KeyBy router starts a KeyedStage that consumes the following
//     Cloneable (stateful) operators
//   - any other channel-based operator gets its own ChannelStage
//   - SourceStage and SinkStage bracket the plan
//
// The returned stages are wired with one bounded edge between each
// consecutive pair.
func BuildPlan(cfg PlanConfig) ([]Stage, error) {
	if cfg.Source == nil {
		return nil, fmt.Errorf("pipeline: plan has no source")
	}
	if cfg.Sink == nil {
		return nil, fmt.Errorf("pipeline: plan has no sink")
	}
	if len(cfg.Labels) != len(cfg.Operators) {
		return nil, fmt.Errorf("pipeline: %d labels for %d operators", len(cfg.Labels), len(cfg.Operators))
	}

	stages := []Stage{&SourceStage{Source: cfg.Source, DrainTimeout: cfg.DrainTimeout}}

	var group []operator.Operator
	var groupLabels []string
	groupPar := 1
	statelessCount := 0
	keyedCount := 0
	channelCount := 0

	flush := func() {
		if len(group) == 0 {
			return
		}
		stages = append(stages, &StatelessStage{
			StageName:   fmt.Sprintf("stateless-%d", statelessCount),
			Ops:         group,
			Labels:      groupLabels,
			Parallelism: groupPar,
		})
		statelessCount++
		group = nil
		groupLabels = nil
	}

	for i := 0; i < len(cfg.Operators); i++ {
		op := cfg.Operators[i]

		if kb, ok := op.(*operator.KeyByOperator); ok && kb.IsRouter() {
			if kb.Partitions <= 0 {
				return nil, fmt.Errorf("pipeline: KeyBy partitions must be > 0, got %d", kb.Partitions)
			}
			flush()
			stateful, skip := takeStateful(cfg.Operators[i+1:])
			i += skip
			ks := NewKeyedStage(kb, stateful, cfg.OnClone)
			ks.StageName = fmt.Sprintf("keyed-%d", keyedCount)
			keyedCount++
			stages = append(stages, ks)
			continue
		}

		if _, ok := op.(operator.SingleProcessor); ok {
			par := 1
			if p, ok := op.(operator.Parallel); ok {
				par = p.Parallelism()
			}
			if len(group) > 0 && par != groupPar {
				flush()
			}
			groupPar = par
			group = append(group, op)
			groupLabels = append(groupLabels, cfg.Labels[i])
			continue
		}

		// Channel-based operator outside a keyed stage (e.g. Window
		// or Reduce without KeyBy): runs alone with the old model.
		flush()
		stages = append(stages, &ChannelStage{
			Op:        op,
			Label:     cfg.Labels[i],
			StageName: fmt.Sprintf("op-%s-%d", op.Name(), channelCount),
		})
		channelCount++
	}
	flush()

	stages = append(stages, &SinkStage{Sink: cfg.Sink})
	return stages, nil
}

// takeStateful collects consecutive Cloneable (stateful) operators
// from the given slice. Returns the collected operators and the
// number of operators consumed.
func takeStateful(ops []operator.Operator) ([]operator.Operator, int) {
	var stateful []operator.Operator
	for _, op := range ops {
		if _, ok := op.(operator.Cloneable); ok {
			stateful = append(stateful, op)
		} else {
			return stateful, len(stateful)
		}
	}
	return stateful, len(stateful)
}
