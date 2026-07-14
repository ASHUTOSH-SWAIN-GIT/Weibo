package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/state"
)

// PlanConfig is the input to BuildPlan.
type PlanConfig struct {
	Source    source.Source
	Operators []operator.Operator
	Labels    []string // metric label per operator, aligned with Operators
	Sink      sink.Sink

	// DrainTimeout bounds the source offset flush on shutdown.
	DrainTimeout time.Duration

	// StageHooks are forwarded to the stages the planner builds.
	StageHooks
}

// StageHooks are engine callbacks wired into stateful stages.
type StageHooks struct {
	// OnClone is called for every stateful operator clone a keyed
	// stage creates, so the caller can register it for checkpointing.
	// It returns the clone's global worker index ("worker-<idx>" in
	// checkpoint data).
	OnClone func(operator.Operator) int

	// OnSnapshot receives operator state captured synchronously when
	// a barrier passes through a stateful operator (the race-free
	// snapshot point). key is the checkpoint-data key ("worker-<idx>"
	// or "op-<idx>").
	OnSnapshot func(checkpointID, key string, snapshot []byte)

	// StateBackendFor creates the state backend for a stateful
	// operator instance, keyed by its owner ID ("op-<i>" or
	// "worker-<idx>"). Nil means operators keep their self-created
	// default (in-memory) backends.
	StateBackendFor func(ownerID string) (state.StateBackend, error)

	// NativeStateDir returns the directory a Checkpointable backend
	// should hard-link its barrier-time checkpoint into, for a given
	// (checkpoint ID, owner). Nil when checkpointing is off or the
	// storage has no state directory support.
	NativeStateDir func(checkpointID, ownerID string) string
}

// wireNativeSnapshot injects a native barrier-time snapshot into ops
// whose backend supports Checkpointable: the operator hard-links its
// state into the checkpoint's state dir and reports a state-ref
// marker instead of serialized bytes. Must run AFTER assignBackend.
func (h StageHooks) wireNativeSnapshot(op operator.Operator, ownerID string) {
	if h.NativeStateDir == nil {
		return
	}
	ns, ok := op.(operator.NativeSnapshotter)
	if !ok {
		return
	}
	cp, ok := ns.Backend().(state.Checkpointable)
	if !ok {
		return
	}
	dirFor := h.NativeStateDir
	ns.SetNativeSnapshot(func(checkpointID string) ([]byte, error) {
		if err := cp.CheckpointTo(dirFor(checkpointID, ownerID)); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"state_ref": ownerID})
	})
}

// assignBackend injects an engine-created state backend into op when
// both the operator and the configuration support it.
func (h StageHooks) assignBackend(op operator.Operator, ownerID string) error {
	sc, ok := op.(operator.StateConfigurable)
	if !ok || h.StateBackendFor == nil {
		return nil
	}
	b, err := h.StateBackendFor(ownerID)
	if err != nil {
		return fmt.Errorf("pipeline: state backend for %s: %w", ownerID, err)
	}
	sc.SetStateBackend(b)
	return nil
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
			ks, err := NewKeyedStage(kb, stateful, cfg.StageHooks)
			if err != nil {
				return nil, err
			}
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
		if err := cfg.assignBackend(op, fmt.Sprintf("op-%d", i)); err != nil {
			return nil, err
		}
		cfg.wireNativeSnapshot(op, fmt.Sprintf("op-%d", i))
		// Wire the race-free barrier-time snapshot for top-level
		// stateful operators too ("op-<idx>" in checkpoint data).
		if bs, ok := op.(operator.BarrierSnapshotter); ok && cfg.OnSnapshot != nil {
			key := fmt.Sprintf("op-%d", i)
			onSnap := cfg.OnSnapshot
			bs.SetBarrierSnapshot(func(id string, snap []byte, err error) {
				if err != nil {
					fmt.Printf("mailer: barrier snapshot failed for %s: %v\n", key, err)
					return
				}
				onSnap(id, key, snap)
			})
		}
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
