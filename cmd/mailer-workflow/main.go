package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/compiler"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/runner"
)

const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

type cliConfig struct {
	file     string
	dataDir  string
	dryRun   bool
	describe bool
	quiet    bool
}

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr))
}

func runCLI(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseFlags(args, stderr)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := runner.Options{BaseDataDir: cfg.dataDir}
	if cfg.dryRun {
		cw, err := runner.CompileFile(cfg.file, opts)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return exitError
		}
		if !cfg.quiet {
			printSummary(stdout, cw.Name, cw.Graph, cw.Delivery)
		}
		if cfg.describe {
			fmt.Fprintln(stdout, cw.Env.DescribeJSON())
		}
		return exitOK
	}

	cw, err := runner.CompileFile(cfg.file, opts)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitError
	}
	if !cfg.quiet {
		printSummary(stdout, cw.Name, cw.Graph, cw.Delivery)
	}
	if cfg.describe {
		fmt.Fprintln(stdout, cw.Env.DescribeJSON())
	}

	if err := cw.Env.Execute(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(stderr, "workflow cancelled")
		} else {
			fmt.Fprintf(stderr, "workflow runner: execute %s: %v\n", cw.Name, err)
		}
		return exitError
	}
	return exitOK
}

func parseFlags(args []string, stderr io.Writer) (cliConfig, error) {
	var cfg cliConfig
	fs := flag.NewFlagSet("mailer-workflow", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.file, "file", "", "workflow YAML/JSON path")
	fs.StringVar(&cfg.dataDir, "data-dir", "", "base directory for derived state/checkpoint dirs")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "load, validate, resolve secrets, and compile without executing")
	fs.BoolVar(&cfg.describe, "describe", false, "print compiled pipeline description JSON")
	fs.BoolVar(&cfg.quiet, "quiet", false, "suppress success summary")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.file == "" {
		return cfg, fmt.Errorf("missing required --file")
	}
	return cfg, nil
}

func printSummary(w io.Writer, name string, graph compiler.PipelineGraph, delivery compiler.DeliveryGuarantee) {
	fmt.Fprintf(w, "workflow: %s\n", name)
	fmt.Fprintf(w, "delivery: %s\n", delivery)
	fmt.Fprintf(w, "source: %s\n", graph.Source)
	fmt.Fprintf(w, "operators: %s\n", formatOperators(graph.Operators))
	fmt.Fprintf(w, "sink: %s\n", graph.Sink)
}

func formatOperators(ops []compiler.GraphNode) string {
	if len(ops) == 0 {
		return "(none)"
	}
	parts := make([]string, len(ops))
	for i, op := range ops {
		parts[i] = fmt.Sprintf("%s(%s)", op.ID, op.Type)
	}
	return strings.Join(parts, ", ")
}
