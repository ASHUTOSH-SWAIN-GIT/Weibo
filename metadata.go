package weibo

import (
	"encoding/json"
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
)

// PipelineInfo is the JSON-serializable description of a pipeline.
// It is returned by Describe() and consumed by the dashboard.
type PipelineInfo struct {
	Source     source.SourceInfo `json:"source"`
	Operators  []OperatorInfo    `json:"operators"`
	Sink       sink.SinkInfo     `json:"sink"`
	Checkpoint *CheckpointInfo   `json:"checkpoint,omitempty"`
}

// OperatorInfo describes a single operator in the pipeline chain.
type OperatorInfo struct {
	Type   string            `json:"type"`
	Label  string            `json:"label,omitempty"`
	Config map[string]string `json:"config,omitempty"`
}

// CheckpointInfo describes the checkpointing configuration.
type CheckpointInfo struct {
	Interval string `json:"interval"`
	Storage  string `json:"storage"`
}

// Describe returns a serializable description of the pipeline:
// source, operator chain, sink, and checkpoint config.
// Call this before Execute() to get the pipeline graph for the dashboard.
func (env *StreamExecutionEnv) Describe() PipelineInfo {
	info := PipelineInfo{}

	if d, ok := env.source.(source.Describable); ok {
		info.Source = d.Describe()
	} else {
		info.Source = source.SourceInfo{Type: "Unknown", Props: map[string]string{}}
	}

	for _, op := range env.operators {
		oi := OperatorInfo{Type: op.Name()}
		if labeled, ok := op.(operator.Labeled); ok {
			oi.Label = labeled.GetLabel()
		}
		if describable, ok := op.(operator.DescribableOperator); ok {
			meta := describable.DescribeOp()
			oi.Config = meta.Config
		}
		info.Operators = append(info.Operators, oi)
	}

	if d, ok := env.sink.(sink.Describable); ok {
		info.Sink = d.Describe()
	} else {
		info.Sink = sink.SinkInfo{Type: "Unknown", Props: map[string]string{}}
	}

	if env.checkpointInterval > 0 {
		info.Checkpoint = &CheckpointInfo{
			Interval: fmt.Sprintf("%v", env.checkpointInterval),
			Storage:  fmt.Sprintf("%T", env.checkpointStorage),
		}
	}

	return info
}

// DescribeJSON returns the pipeline description as indented JSON.
func (env *StreamExecutionEnv) DescribeJSON() string {
	info := env.Describe()
	data, _ := json.MarshalIndent(info, "", "  ")
	return string(data)
}
