package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"
)

// controllerFlags registers the -controller and -token flags shared by every
// management subcommand, defaulting to the WEIBO_CONTROLLER / WEIBO_TOKEN
// environment so remote use needs no repeated flags.
func controllerFlags(fs *flag.FlagSet) (controller, token *string) {
	base := envOr("WEIBO_CONTROLLER", "http://localhost:9000")
	controller = fs.String("controller", base, "controller base URL (env WEIBO_CONTROLLER)")
	token = fs.String("token", os.Getenv("WEIBO_TOKEN"), "bearer token, if the controller requires one (env WEIBO_TOKEN)")
	return controller, token
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// runJobs lists all jobs as a compact table.
func runJobs(args []string) int {
	fs := flag.NewFlagSet("jobs", flag.ContinueOnError)
	controller, token := controllerFlags(fs)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rows, err := newClient(*controller, *token).listJobs(context.Background())
	if err != nil {
		return fail(err)
	}
	if len(rows) == 0 {
		fmt.Println("no jobs")
		return 0
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tNAME\tKIND\tPHASE\tDESIRED\tUPDATED")
	for _, j := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			j.ID, j.Name, kindOr(j.Kind), j.Phase, j.Desired, ago(j.Updated))
	}
	tw.Flush()
	return 0
}

// runStatus prints the detail of one job: identity, phase, latest run, and
// recent lifecycle transitions.
func runStatus(args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	controller, token := controllerFlags(fs)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	id := fs.Arg(0)
	if id == "" {
		fmt.Fprintln(os.Stderr, "usage: weibo status <job-id> [flags]")
		return 2
	}
	d, err := newClient(*controller, *token).getJob(context.Background(), id)
	if err != nil {
		return fail(err)
	}
	j := d.Job
	fmt.Printf("ID:        %s\n", j.ID)
	fmt.Printf("Name:      %s\n", j.Name)
	fmt.Printf("Kind:      %s\n", kindOr(j.Kind))
	if j.Image != "" {
		fmt.Printf("Image:     %s\n", j.Image)
	}
	fmt.Printf("Delivery:  %s\n", j.Delivery)
	fmt.Printf("Desired:   %s\n", j.Desired)
	fmt.Printf("Created:   %s\n", j.Created.Local().Format(time.RFC3339))
	if d.LatestRun != nil {
		r := d.LatestRun
		fmt.Printf("\nLatest run:\n")
		fmt.Printf("  Phase:   %s (attempt %d)\n", r.Phase, r.Attempt)
		if r.Error != "" {
			fmt.Printf("  Error:   %s\n", r.Error)
		}
		fmt.Printf("  Started: %s\n", r.Started.Local().Format(time.RFC3339))
	}
	if len(d.Transitions) > 0 {
		fmt.Printf("\nRecent transitions:\n")
		n := len(d.Transitions)
		start := 0
		if n > 8 {
			start = n - 8 // last 8, oldest first
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		for _, t := range d.Transitions[start:] {
			fmt.Fprintf(tw, "  %s\t%s -> %s\t%s\n", t.At.Local().Format("15:04:05"), t.From, t.To, t.Reason)
		}
		tw.Flush()
	}
	return 0
}

// runLogs prints the tail of a job's container logs.
func runLogs(args []string) int {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	controller, token := controllerFlags(fs)
	tail := fs.Int("tail", 200, "number of trailing log lines")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	id := fs.Arg(0)
	if id == "" {
		fmt.Fprintln(os.Stderr, "usage: weibo logs <job-id> [-tail N] [flags]")
		return 2
	}
	out, err := newClient(*controller, *token).logs(context.Background(), id, *tail)
	if err != nil {
		return fail(err)
	}
	fmt.Print(out)
	if len(out) > 0 && out[len(out)-1] != '\n' {
		fmt.Println()
	}
	return 0
}

// runCancel requests a graceful stop.
func runCancel(args []string) int {
	fs := flag.NewFlagSet("cancel", flag.ContinueOnError)
	controller, token := controllerFlags(fs)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	id := fs.Arg(0)
	if id == "" {
		fmt.Fprintln(os.Stderr, "usage: weibo cancel <job-id> [flags]")
		return 2
	}
	if err := newClient(*controller, *token).cancel(context.Background(), id); err != nil {
		return fail(err)
	}
	fmt.Printf("cancelling %s\n", id)
	return 0
}

// runRestart resumes a job, optionally from a named savepoint.
func runRestart(args []string) int {
	fs := flag.NewFlagSet("restart", flag.ContinueOnError)
	controller, token := controllerFlags(fs)
	savepoint := fs.String("savepoint", "", "resume from this savepoint label (default: last checkpoint)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	id := fs.Arg(0)
	if id == "" {
		fmt.Fprintln(os.Stderr, "usage: weibo restart <job-id> [-savepoint label] [flags]")
		return 2
	}
	job, err := newClient(*controller, *token).restart(context.Background(), id, *savepoint)
	if err != nil {
		return fail(err)
	}
	if *savepoint != "" {
		fmt.Printf("restarting %s from savepoint %q\n", job.ID, *savepoint)
	} else {
		fmt.Printf("restarting %s from last checkpoint\n", job.ID)
	}
	return 0
}

// runSavepoint triggers a stop-with-savepoint.
func runSavepoint(args []string) int {
	fs := flag.NewFlagSet("savepoint", flag.ContinueOnError)
	controller, token := controllerFlags(fs)
	label := fs.String("label", "", "savepoint label (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	id := fs.Arg(0)
	if id == "" || *label == "" {
		fmt.Fprintln(os.Stderr, "usage: weibo savepoint <job-id> -label <name> [flags]")
		return 2
	}
	if err := newClient(*controller, *token).savepoint(context.Background(), id, *label); err != nil {
		return fail(err)
	}
	fmt.Printf("savepoint %q: stopping %s\n", *label, id)
	return 0
}

// --- small shared helpers ---

func fail(err error) int {
	fmt.Fprintf(os.Stderr, "weibo: %v\n", err)
	return 1
}

func kindOr(k string) string {
	if k == "" {
		return "yaml"
	}
	return k
}

// ago renders a timestamp as a short relative age (e.g. "3m", "2h", "5d").
func ago(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
