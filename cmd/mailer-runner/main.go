// Command mailer-runner is the generic job entrypoint baked into the
// runner container image. It reads a mounted workflow document, compiles
// it, and runs it under a jobagent supervisor that exposes the control
// surface (/state, /cancel, /metrics, ...) on an HTTP port.
//
// Configuration is entirely by environment variable so one prebuilt image
// runs any YAML/JSON job (see the job-orchestration plan, phase P2):
//
//	WORKFLOW   path to the mounted workflow file        (required)
//	DATA_DIR   base dir for derived state/checkpoints    (default /data)
//	PORT       agent HTTP control port                   (default 8080)
//
// Secret placeholders (${VAR}) in the workflow resolve from this process's
// environment at compile time. On SIGTERM/SIGINT the job drains
// gracefully and takes a final checkpoint before the process exits.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sdk"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/runner"
)

const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2

	defaultDataDir = "/data"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Getenv, os.Stdout, os.Stderr))
}

// run holds the whole entrypoint so it is testable without a real process
// or container: getenv supplies configuration and ctx drives shutdown.
func run(ctx context.Context, getenv func(string) string, stdout, stderr io.Writer) int {
	workflowPath := getenv("WORKFLOW")
	if workflowPath == "" {
		fmt.Fprintln(stderr, "mailer-runner: WORKFLOW is required (path to the workflow file)")
		return exitUsage
	}
	dataDir := getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = defaultDataDir
	}

	// The engine derives per-job state/checkpoint dirs under DATA_DIR;
	// make sure the mounted base exists.
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		fmt.Fprintf(stderr, "mailer-runner: create data dir %s: %v\n", dataDir, err)
		return exitError
	}

	// Compile the mounted document. Secrets resolve from this process's
	// environment (runner.Options default resolver).
	cw, err := runner.CompileFile(workflowPath, runner.Options{BaseDataDir: dataDir})
	if err != nil {
		fmt.Fprintf(stderr, "mailer-runner: compile %s: %v\n", workflowPath, err)
		return exitError
	}
	fmt.Fprintf(stdout, "mailer-runner: job=%s delivery=%s data=%s\n", cw.Name, cw.Delivery, dataDir)

	// The lifecycle (agent, serve, savepoints, graceful shutdown) is shared
	// with SDK jobs so both behave identically.
	return sdk.Serve(ctx, cw.Env, sdk.ServeOptions{
		Name:             cw.Name,
		Port:             getenv("PORT"),
		CheckpointDir:    cw.CheckpointDir,
		SavepointDir:     getenv("SAVEPOINT_DIR"),
		RestoreSavepoint: getenv("RESTORE_SAVEPOINT"),
		Stdout:           stdout,
		Stderr:           stderr,
	})
}
