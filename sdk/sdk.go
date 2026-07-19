// Package sdk is the harness for SDK (Go) jobs run through the mailer
// control plane. A user writes a pipeline builder and calls Run:
//
//	func main() { sdk.Run(Build) }
//	func Build(env *mailer.StreamExecutionEnv) {
//	    env.FromSource(src).KeyBy(key).Reduce(sum).ToSink(out)
//	}
//
// The harness supervises the pipeline exactly like the YAML runner: it
// serves the control surface (/state, /metrics, /cancel, /savepoint) for
// the dashboard, drains gracefully on SIGTERM, and — when checkpointing is
// enabled (CHECKPOINT_INTERVAL) — supports savepoints and recovery. So an
// SDK job is managed identically to a YAML job.
//
// Configuration (environment):
//
//	DATA_DIR             state/checkpoint root                (default /data)
//	PORT                 control port                          (default 8080)
//	SAVEPOINT_DIR        savepoint blobstore                   (default /savepoints)
//	CHECKPOINT_INTERVAL  enable durable checkpointing, e.g. 5s (default off)
//	RESTORE_SAVEPOINT    savepoint label to resume from        (default none)
//	JOB_NAME             human-readable name for logs
package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/jobagent"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/state"
)

const (
	defaultDataDir      = "/data"
	defaultPort         = "8080"
	defaultSavepointDir = "/savepoints"
)

// Builder wires a pipeline onto env: env.FromSource(...)....ToSink(...).
type Builder func(env *mailer.StreamExecutionEnv)

// Run is the SDK job entrypoint. It configures the environment from env
// vars, invokes build to wire the pipeline, and supervises it to
// completion, exiting the process with an appropriate status code.
func Run(build Builder) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(runBuild(ctx, build, os.Getenv, os.Stdout, os.Stderr))
}

// runBuild is Run's testable core.
func runBuild(ctx context.Context, build Builder, getenv func(string) string, stdout, stderr io.Writer) int {
	dataDir := orDefault(getenv("DATA_DIR"), defaultDataDir)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		fmt.Fprintf(stderr, "sdk: create data dir %s: %v\n", dataDir, err)
		return 1
	}

	env := mailer.NewEnv()
	checkpointDir := ""
	// Opt-in durable checkpointing: the harness owns the storage layout so
	// savepoints/recovery work identically to YAML jobs.
	if iv := getenv("CHECKPOINT_INTERVAL"); iv != "" {
		d, err := time.ParseDuration(iv)
		if err != nil || d <= 0 {
			fmt.Fprintf(stderr, "sdk: invalid CHECKPOINT_INTERVAL %q\n", iv)
			return 2
		}
		checkpointDir = filepath.Join(dataDir, "checkpoints")
		stateDir := filepath.Join(dataDir, "state")
		if err := os.MkdirAll(checkpointDir, 0o755); err != nil {
			fmt.Fprintf(stderr, "sdk: create checkpoint dir: %v\n", err)
			return 1
		}
		if err := os.MkdirAll(stateDir, 0o755); err != nil {
			fmt.Fprintf(stderr, "sdk: create state dir: %v\n", err)
			return 1
		}
		env.WithCheckpointing(d, checkpoint.NewFileStorage(checkpointDir))
		env.WithStateBackend(state.Pebble(stateDir))
	}

	build(env)

	return Serve(ctx, env, ServeOptions{
		Name:             orDefault(getenv("JOB_NAME"), "sdk-job"),
		Port:             getenv("PORT"),
		CheckpointDir:    checkpointDir,
		SavepointDir:     getenv("SAVEPOINT_DIR"),
		RestoreSavepoint: getenv("RESTORE_SAVEPOINT"),
		Stdout:           stdout,
		Stderr:           stderr,
	})
}

// ServeOptions configures Serve.
type ServeOptions struct {
	Name             string // for log lines
	Port             string // control port; default 8080
	CheckpointDir    string // "" when checkpointing is disabled
	SavepointDir     string // savepoint blobstore; default /savepoints
	RestoreSavepoint string // savepoint label to seed from, or ""
	Stdout, Stderr   io.Writer
}

// Serve supervises a configured env under a jobagent: it restores a
// savepoint if asked, serves the control surface, runs to completion, and
// promotes a savepoint on a stop-with-savepoint request. It returns a
// process exit code. This is the shared lifecycle behind both the YAML
// runner and SDK jobs, so they behave identically.
func Serve(ctx context.Context, env *mailer.StreamExecutionEnv, opts ServeOptions) int {
	stdout := orWriter(opts.Stdout, os.Stdout)
	stderr := orWriter(opts.Stderr, os.Stderr)
	port := orDefault(opts.Port, defaultPort)
	blobs := checkpoint.NewFileBlobstore(orDefault(opts.SavepointDir, defaultSavepointDir))

	if opts.RestoreSavepoint != "" {
		if opts.CheckpointDir == "" {
			fmt.Fprintln(stderr, "sdk: RESTORE_SAVEPOINT set but checkpointing is disabled")
			return 1
		}
		id, err := checkpoint.RestoreSavepoint(checkpoint.NewFileStorage(opts.CheckpointDir), blobs, opts.RestoreSavepoint)
		if err != nil {
			fmt.Fprintf(stderr, "sdk: restore savepoint %q: %v\n", opts.RestoreSavepoint, err)
			return 1
		}
		fmt.Fprintf(stdout, "sdk: restored from savepoint %q (checkpoint %s)\n", opts.RestoreSavepoint, id)
	}

	agent := jobagent.New(env)

	// Serve the control surface alongside the job; its own context tears
	// it down when the job finishes on its own, not only on SIGTERM.
	srvCtx, stopSrv := context.WithCancel(ctx)
	defer stopSrv()
	serveErr := make(chan error, 1)
	go func() { serveErr <- agent.Serve(srvCtx, ":"+port) }()

	runErr := agent.Run(ctx)
	stopSrv()

	// Stop-with-savepoint: promote the final checkpoint now.
	if label, ok := agent.SavepointRequest(); ok {
		if opts.CheckpointDir == "" {
			fmt.Fprintln(stderr, "sdk: savepoint requested but checkpointing is disabled")
		} else if id, err := checkpoint.CreateSavepoint(checkpoint.NewFileStorage(opts.CheckpointDir), blobs, label); err != nil {
			fmt.Fprintf(stderr, "sdk: create savepoint %q: %v\n", label, err)
		} else {
			fmt.Fprintf(stdout, "sdk: savepoint %q created from checkpoint %s\n", label, id)
		}
	}

	if err := <-serveErr; err != nil {
		fmt.Fprintf(stderr, "sdk: control server: %v\n", err)
	}

	switch {
	case runErr == nil || errors.Is(runErr, context.Canceled):
		fmt.Fprintf(stdout, "sdk: job=%s %s\n", opts.Name, agent.State().Phase)
		return 0
	default:
		fmt.Fprintf(stderr, "sdk: job=%s failed: %v\n", opts.Name, runErr)
		return 1
	}
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func orWriter(w, def io.Writer) io.Writer {
	if w == nil {
		return def
	}
	return w
}
