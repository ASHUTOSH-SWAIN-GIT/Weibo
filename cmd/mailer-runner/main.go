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
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/jobagent"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/runner"
)

const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2

	defaultDataDir = "/data"
	defaultPort    = "8080"
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
	dataDir := orDefault(getenv("DATA_DIR"), defaultDataDir)
	port := orDefault(getenv("PORT"), defaultPort)

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
	fmt.Fprintf(stdout, "mailer-runner: job=%s delivery=%s data=%s port=%s\n",
		cw.Name, cw.Delivery, dataDir, port)

	agent := jobagent.New(cw.Env)

	// Serve the control surface alongside the job. A serve failure (e.g.
	// port already bound) is logged but does not abort the job — the
	// pipeline is the primary workload. The server has its own context so
	// it shuts down when the job finishes on its own, not only on SIGTERM.
	srvCtx, stopSrv := context.WithCancel(ctx)
	defer stopSrv()
	serveErr := make(chan error, 1)
	go func() { serveErr <- agent.Serve(srvCtx, ":"+port) }()

	runErr := agent.Run(ctx)
	stopSrv() // job is done; tear down the control server

	if err := <-serveErr; err != nil {
		fmt.Fprintf(stderr, "mailer-runner: control server: %v\n", err)
	}

	switch {
	case runErr == nil || errors.Is(runErr, context.Canceled):
		fmt.Fprintf(stdout, "mailer-runner: job=%s %s\n", cw.Name, agent.State().Phase)
		return exitOK
	default:
		fmt.Fprintf(stderr, "mailer-runner: job=%s failed: %v\n", cw.Name, runErr)
		return exitError
	}
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
