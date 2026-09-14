package compiler

import (
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/secrets"
)

// Compiler turns a declarative workflow into an executable Weibo
// environment. It does not start the pipeline and, apart from creating
// job directories and (for a Postgres sink) opening a connection pool,
// establishes no connections.
type Compiler struct {
	// Secrets resolves ${VAR} references in sensitive connection fields.
	// Defaults to secrets.Environment.
	Secrets secrets.SecretResolver

	// BaseDataDir is the root for per-workflow state/checkpoint
	// directories. Defaults to DefaultDataRoot ("./data").
	BaseDataDir string

	// Functions resolves ref-based map/flatMap/process operators. Nil means
	// ref-based operators are rejected and only built-in declarative operators
	// can compile.
	Functions *FunctionRegistry
}

// DeliveryGuarantee is the end-to-end guarantee a compiled workflow provides.
type DeliveryGuarantee string

const (
	AtMostOnce  DeliveryGuarantee = "at-most-once"
	AtLeastOnce DeliveryGuarantee = "at-least-once"
	ExactlyOnce DeliveryGuarantee = "exactly-once"
)

// GraphNode is one operator in the compiled pipeline graph.
type GraphNode struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// PipelineGraph is a static description of the compiled pipeline.
type PipelineGraph struct {
	Source    string      `json:"source"`
	Operators []GraphNode `json:"operators"`
	Sink      string      `json:"sink"`
}

// CompiledWorkflow bundles the executable environment with a static
// description of what was compiled.
type CompiledWorkflow struct {
	Env      *weibo.StreamExecutionEnv
	Name     string
	Graph    PipelineGraph
	Delivery DeliveryGuarantee
	// CheckpointDir is the on-disk directory holding this workflow's
	// checkpoints, or "" when checkpointing is disabled. Exposed so the
	// runner can seed a restored savepoint into the same storage.
	CheckpointDir string
}

// Compile validates and compiles a workflow into an executable Weibo
// environment, ready to Execute. It returns an error without producing
// an environment if the workflow is invalid or references anything the
// declarative compiler cannot build.
func (c *Compiler) Compile(spec *workflow.WorkflowSpec) (*weibo.StreamExecutionEnv, error) {
	cw, err := c.CompileWorkflow(spec)
	if err != nil {
		return nil, err
	}
	return cw.Env, nil
}

// CompileWorkflow is Compile with the full compiled description.
//
// Order: validate → resolve secrets → create source → create env
// (runtime config) → apply operators → create sink → return.
func (c *Compiler) CompileWorkflow(spec *workflow.WorkflowSpec) (*CompiledWorkflow, error) {
	if spec == nil {
		return nil, fmt.Errorf("compiler: nil workflow")
	}

	// 1. Validate — an invalid workflow never reaches the runtime.
	if err := workflow.Validate(spec); err != nil {
		return nil, err
	}

	// 2. Resolve secret references into a working copy.
	resolver := c.Secrets
	if resolver == nil {
		resolver = secrets.Environment{}
	}
	resolved, err := resolveSecrets(spec, resolver)
	if err != nil {
		return nil, fmt.Errorf("compiler: resolve secrets: %w", err)
	}

	// 3. Create env with runtime config (job-isolated state/checkpoints).
	env, err := CompileRuntime(resolved.Name, c.BaseDataDir, resolved.Env)
	if err != nil {
		return nil, err
	}

	// 4. Create source(s) and apply operators.
	stream, err := c.compileStream(env, resolved)
	if err != nil {
		return nil, err
	}

	// 5. Create sink and terminate the pipeline.
	snk, err := CompileSink(resolved.Sink)
	if err != nil {
		return nil, err
	}
	stream.ToSink(snk)

	// 6. Return the executable environment + description.
	return &CompiledWorkflow{
		Env:           env,
		Name:          resolved.Name,
		Graph:         buildGraph(resolved),
		Delivery:      deliveryGuarantee(resolved),
		CheckpointDir: CheckpointDir(resolved.Name, c.BaseDataDir, resolved.Env),
	}, nil
}

func (c *Compiler) compileStream(env *weibo.StreamExecutionEnv, wf *workflow.Workflow) (*weibo.Stream, error) {
	if len(wf.Sources) == 0 {
		src, err := CompileSource(wf.Source)
		if err != nil {
			return nil, err
		}
		return applyOperators(env, src, wf.Pipeline, c.Functions)
	}
	if len(wf.Pipeline) == 0 || wf.Pipeline[0].Join == nil {
		return nil, fmt.Errorf("compiler: multi-source workflows must start with a join operator")
	}
	named := make(map[string]source.Source, len(wf.Sources))
	for _, srcSpec := range wf.Sources {
		src, err := CompileSource(srcSpec.Source)
		if err != nil {
			return nil, fmt.Errorf("compiler: source %q: %w", srcSpec.Name, err)
		}
		named[srcSpec.Name] = src
	}
	j := wf.Pipeline[0].Join
	left := named[j.LeftSource]
	right := named[j.RightSource]
	if left == nil || right == nil {
		return nil, fmt.Errorf("compiler: join references unknown source(s)")
	}
	var stream *weibo.Stream
	if j.Within > 0 {
		stream = env.JoinSourcesWithin(j.LeftSource, left, j.RightSource, right, j.Within.Std(), nil, wf.Pipeline[0].ID)
	} else {
		stream = env.JoinSources(j.LeftSource, left, j.RightSource, right, j.Before.Std(), j.After.Std(), nil, wf.Pipeline[0].ID)
	}
	return applyOperatorsToStream(stream, wf.Pipeline[1:], c.Functions)
}

func buildGraph(wf *workflow.Workflow) PipelineGraph {
	nodes := make([]GraphNode, len(wf.Pipeline))
	for i, op := range wf.Pipeline {
		nodes[i] = GraphNode{ID: op.ID, Type: op.Type}
	}
	return PipelineGraph{
		Source:    graphSource(wf),
		Operators: nodes,
		Sink:      wf.Sink.Type,
	}
}

func graphSource(wf *workflow.Workflow) string {
	if len(wf.Sources) == 0 {
		return wf.Source.Type
	}
	return "multi-source"
}

func deliveryGuarantee(wf *workflow.Workflow) DeliveryGuarantee {
	srcEO := wf.Source.Type == "kafka" && wf.Source.Kafka != nil && wf.Source.Kafka.ExactlyOnce
	sinkTxn := wf.Sink.Type == "txnKafka" || wf.Sink.Type == "transactional_kafka"
	ckpt := wf.Env != nil && wf.Env.Checkpointing != nil && wf.Env.Checkpointing.Interval > 0

	switch {
	case srcEO && sinkTxn && ckpt:
		return ExactlyOnce
	case ckpt:
		return AtLeastOnce
	default:
		return AtMostOnce
	}
}
