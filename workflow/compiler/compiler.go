package compiler

import (
	"fmt"
	"os"
	"strings"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
)

// ConnectionResolver expands connection/secret references in string
// configuration values (broker lists, DSNs, credentials). The default
// resolves ${VAR}/$VAR from the OS environment.
type ConnectionResolver interface {
	Resolve(value string) (string, error)
}

// EnvResolver resolves ${VAR}/$VAR from the process environment and
// fails if any referenced variable is unset.
type EnvResolver struct{}

func (EnvResolver) Resolve(value string) (string, error) {
	var missing []string
	out := os.Expand(value, func(k string) string {
		v, ok := os.LookupEnv(k)
		if !ok {
			missing = append(missing, k)
			return ""
		}
		return v
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("unresolved environment variable(s): %s", strings.Join(missing, ", "))
	}
	return out, nil
}

// Compiler turns a declarative workflow into an executable Mailer
// environment. It does not start the pipeline and, apart from creating
// job directories and (for a Postgres sink) opening a connection pool,
// establishes no connections.
type Compiler struct {
	// Connections resolves ${VAR} references in connection strings.
	// Defaults to EnvResolver.
	Connections ConnectionResolver

	// BaseDataDir is the root for per-workflow state/checkpoint
	// directories. Defaults to DefaultDataRoot ("./data").
	BaseDataDir string
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
	ID   string
	Type string
}

// PipelineGraph is a static description of the compiled pipeline.
type PipelineGraph struct {
	Source    string
	Operators []GraphNode
	Sink      string
}

// CompiledWorkflow bundles the executable environment with a static
// description of what was compiled.
type CompiledWorkflow struct {
	Env      *mailer.StreamExecutionEnv
	Name     string
	Graph    PipelineGraph
	Delivery DeliveryGuarantee
}

// Compile validates and compiles a workflow into an executable Mailer
// environment, ready to Execute. It returns an error without producing
// an environment if the workflow is invalid or references anything the
// declarative compiler cannot build.
func (c *Compiler) Compile(spec *workflow.WorkflowSpec) (*mailer.StreamExecutionEnv, error) {
	cw, err := c.CompileWorkflow(spec)
	if err != nil {
		return nil, err
	}
	return cw.Env, nil
}

// CompileWorkflow is Compile with the full compiled description.
//
// Order: validate → resolve connections → create source → create env
// (runtime config) → apply operators → create sink → return.
func (c *Compiler) CompileWorkflow(spec *workflow.WorkflowSpec) (*CompiledWorkflow, error) {
	if spec == nil {
		return nil, fmt.Errorf("compiler: nil workflow")
	}

	// 1. Validate — an invalid workflow never reaches the runtime.
	if err := workflow.Validate(spec); err != nil {
		return nil, err
	}

	// 2. Resolve connections/env vars into a working copy.
	resolver := c.Connections
	if resolver == nil {
		resolver = EnvResolver{}
	}
	resolved, err := resolveConnections(spec, resolver)
	if err != nil {
		return nil, fmt.Errorf("compiler: resolve connections: %w", err)
	}

	// 3. Create source (no connection for consumer-group Kafka / test sources).
	src, err := CompileSource(resolved.Source)
	if err != nil {
		return nil, err
	}

	// 4. Create env with runtime config (job-isolated state/checkpoints).
	env, err := CompileRuntime(resolved.Name, c.BaseDataDir, resolved.Env)
	if err != nil {
		return nil, err
	}

	// 5. Apply operators in order onto the source stream.
	stream, err := applyOperators(env, src, resolved.Pipeline)
	if err != nil {
		return nil, err
	}

	// 6. Create sink and terminate the pipeline.
	snk, err := CompileSink(resolved.Sink)
	if err != nil {
		return nil, err
	}
	stream.ToSink(snk)

	// 7. Return the executable environment + description.
	return &CompiledWorkflow{
		Env:      env,
		Name:     resolved.Name,
		Graph:    buildGraph(resolved),
		Delivery: deliveryGuarantee(resolved),
	}, nil
}

func buildGraph(wf *workflow.Workflow) PipelineGraph {
	nodes := make([]GraphNode, len(wf.Pipeline))
	for i, op := range wf.Pipeline {
		nodes[i] = GraphNode{ID: op.ID, Type: op.Type}
	}
	return PipelineGraph{
		Source:    wf.Source.Type,
		Operators: nodes,
		Sink:      wf.Sink.Type,
	}
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
