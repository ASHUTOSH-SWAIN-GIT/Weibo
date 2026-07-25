package compiler

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/state"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow"
)

// DefaultDataRoot is where per-workflow state and checkpoints live when
// no directory is configured.
const DefaultDataRoot = "./data"

// CompileRuntime builds a *weibo.StreamExecutionEnv configured from a
// workflow's runtime settings (buffer size, shutdown timeout,
// checkpointing, state backend). The returned env has no source or sink
// yet — the caller wires those with FromSource/…/ToSink.
//
// State and checkpoint directories are made job-specific so two
// workflows can never share the same Pebble database:
//
//	<dataRoot>/<workflow-name>/state
//	<dataRoot>/<workflow-name>/checkpoints
//
// dataRoot defaults to DefaultDataRoot ("./data"). A directory
// explicitly set in the spec is honored as that resource's root, with
// the workflow name nested under it — so isolation holds either way.
func CompileRuntime(workflowName, dataRoot string, rt *workflow.EnvSpec) (*weibo.StreamExecutionEnv, error) {
	if dataRoot == "" {
		dataRoot = DefaultDataRoot
	}
	name := sanitizeName(workflowName)

	env := weibo.NewEnv()
	if rt == nil {
		return env, nil
	}

	if rt.BufferSize < 0 {
		return nil, fmt.Errorf("compiler: runtime bufferSize must not be negative")
	}
	if rt.BufferSize > 0 {
		env.WithBufferSize(rt.BufferSize)
	}
	if rt.ShutdownTimeout > 0 {
		env.WithShutdownTimeout(rt.ShutdownTimeout.Std())
	}

	if rt.Checkpointing != nil {
		interval := rt.Checkpointing.Interval.Std()
		if interval <= 0 {
			return nil, fmt.Errorf("compiler: checkpoint interval must be greater than zero")
		}
		ckptDir := jobDir(rt.Checkpointing.Dir, dataRoot, name, "checkpoints")
		if err := os.MkdirAll(ckptDir, 0o755); err != nil {
			return nil, fmt.Errorf("compiler: create checkpoint dir %q: %w", ckptDir, err)
		}
		env.WithCheckpointing(interval, checkpoint.NewFileStorage(ckptDir))
	}

	if rt.State != nil {
		switch rt.State.Backend {
		case "", "memory":
			env.WithStateBackend(state.InMemory())
		case "pebble":
			stateDir := jobDir(rt.State.Dir, dataRoot, name, "state")
			if err := os.MkdirAll(stateDir, 0o755); err != nil {
				return nil, fmt.Errorf("compiler: create state dir %q: %w", stateDir, err)
			}
			env.WithStateBackend(state.Pebble(stateDir))
		default:
			return nil, fmt.Errorf("compiler: unsupported state backend %q (memory or pebble)", rt.State.Backend)
		}
	}

	return env, nil
}

// CheckpointDir returns the directory a compiled workflow uses for its
// checkpoints, or "" when checkpointing is disabled. It mirrors the path
// CompileRuntime derives, so callers that must target the same storage
// without holding the env — e.g. the runner seeding a restored savepoint
// before Execute — can compute it identically.
func CheckpointDir(workflowName, dataRoot string, rt *workflow.EnvSpec) string {
	if rt == nil || rt.Checkpointing == nil || rt.Checkpointing.Interval.Std() <= 0 {
		return ""
	}
	if dataRoot == "" {
		dataRoot = DefaultDataRoot
	}
	return jobDir(rt.Checkpointing.Dir, dataRoot, sanitizeName(workflowName), "checkpoints")
}

// jobDir returns a per-workflow directory. When a directory is
// configured it is used as the resource root with the workflow name
// nested under it (<configured>/<name>); otherwise the default layout
// <dataRoot>/<name>/<resource> is used. Either way the workflow name is
// in the path, so distinct workflows get distinct Pebble databases.
func jobDir(configured, dataRoot, name, resource string) string {
	if configured != "" {
		return filepath.Join(configured, name)
	}
	return filepath.Join(dataRoot, name, resource)
}

// sanitizeName turns a workflow name into a safe single path segment,
// guarding against empty names and path traversal.
func sanitizeName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." {
		return "default"
	}
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '_' || r == '-' || r == '.':
			return r
		default:
			return '_'
		}
	}, name)
	if safe == "" || safe == "." || safe == ".." {
		return "default"
	}
	return safe
}
