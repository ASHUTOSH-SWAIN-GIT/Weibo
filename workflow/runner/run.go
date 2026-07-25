package runner

import (
	"context"
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/compiler"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/secrets"
)

// Options configures workflow compilation and execution.
type Options struct {
	// BaseDataDir is passed to compiler.Compiler for derived
	// state/checkpoint directories.
	BaseDataDir string

	// Secrets resolves ${VAR} references in sensitive connection fields.
	// Defaults to secrets.Environment.
	Secrets secrets.SecretResolver
}

// RunResult is the non-secret metadata for a completed workflow run.
type RunResult struct {
	Name     string
	Graph    compiler.PipelineGraph
	Delivery compiler.DeliveryGuarantee
}

// CompileFile loads, validates, resolves secrets, and compiles a
// workflow file without starting it.
func CompileFile(path string, opts Options) (*compiler.CompiledWorkflow, error) {
	spec, err := workflow.Load(path)
	if err != nil {
		return nil, fmt.Errorf("workflow runner: load %s: %w", path, err)
	}

	c := &compiler.Compiler{
		BaseDataDir: opts.BaseDataDir,
		Secrets:     opts.Secrets,
	}
	cw, err := c.CompileWorkflow(spec)
	if err != nil {
		return nil, fmt.Errorf("workflow runner: compile %s: %w", path, err)
	}
	return cw, nil
}

// RunFile compiles and executes a workflow file.
func RunFile(ctx context.Context, path string, opts Options) (*RunResult, error) {
	cw, err := CompileFile(path, opts)
	if err != nil {
		return nil, err
	}
	if err := cw.Env.Execute(ctx); err != nil {
		return nil, fmt.Errorf("workflow runner: execute %s: %w", cw.Name, err)
	}
	return &RunResult{
		Name:     cw.Name,
		Graph:    cw.Graph,
		Delivery: cw.Delivery,
	}, nil
}
