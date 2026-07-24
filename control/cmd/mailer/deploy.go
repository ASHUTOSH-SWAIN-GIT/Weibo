package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"gopkg.in/yaml.v3"
)

// runDeploy is the one-command path from source to a running job: build the
// image from a Dockerfile, push it to its registry, then submit the manifest
// to the controller. Build/push shell out to the user's docker CLI (reusing
// their buildkit + registry auth); submission is pure REST, so the manifest
// is forwarded as-is and stays agnostic to future manifest fields.
func runDeploy(args []string) int {
	fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
	controller, token := controllerFlags(fs)
	file := fs.String("file", "mailer.yaml", "job manifest to deploy")
	dockerfile := fs.String("dockerfile", "Dockerfile", "Dockerfile for the image build")
	buildCtx := fs.String("context", ".", "docker build context directory")
	noBuild := fs.Bool("no-build", false, "skip docker build (use an already-built image)")
	noPush := fs.Bool("no-push", false, "skip docker push (e.g. a local-only backend)")
	var envs multiFlag
	fs.Var(&envs, "env", "env var KEY=VAL to pass to the job (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	// Read the manifest and pull out the image reference to build/push.
	doc, err := os.ReadFile(*file)
	if err != nil {
		return fail(fmt.Errorf("read manifest: %w", err))
	}
	var m struct {
		Kind  string `yaml:"kind"`
		Name  string `yaml:"name"`
		Image string `yaml:"image"`
	}
	if err := yaml.Unmarshal(doc, &m); err != nil {
		return fail(fmt.Errorf("parse manifest %s: %w", *file, err))
	}

	env, err := parseEnv(envs)
	if err != nil {
		return fail(err)
	}

	// Build + push only make sense for an image-backed (SDK) manifest. A
	// plain YAML workflow runs the generic runner image, so there is nothing
	// to build — we just submit it.
	isImageJob := strings.EqualFold(m.Kind, "sdk") && m.Image != ""
	if isImageJob {
		if !*noBuild {
			if err := runDocker("build", "-f", *dockerfile, "-t", m.Image, *buildCtx); err != nil {
				return fail(fmt.Errorf("docker build: %w", err))
			}
		}
		if !*noPush {
			if err := runDocker("push", m.Image); err != nil {
				return fail(fmt.Errorf("docker push: %w", err))
			}
		}
	} else if !*noBuild {
		fmt.Fprintf(os.Stderr, "mailer: %s is not an SDK image manifest (kind: sdk with image:); submitting without build/push\n", *file)
	}

	job, warning, err := newClient(*controller, *token).submit(context.Background(), doc, env)
	if err != nil {
		return fail(fmt.Errorf("submit: %w", err))
	}
	fmt.Printf("deployed job %s (%s)\n", job.ID, job.Name)
	if warning != "" {
		fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
	}
	fmt.Printf("  mailer status %s\n  mailer logs %s\n", job.ID, job.ID)
	return 0
}

// runDocker runs a docker CLI command, streaming its output to the terminal
// so the user sees build/push progress live.
func runDocker(args ...string) error {
	fmt.Fprintf(os.Stderr, "+ docker %s\n", strings.Join(args, " "))
	cmd := exec.Command("docker", args...)
	cmd.Stdout = os.Stderr // keep stdout clean for the final job line
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// parseEnv turns KEY=VAL pairs into a map, rejecting malformed entries.
func parseEnv(pairs []string) (map[string]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	env := make(map[string]string, len(pairs))
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid -env %q (want KEY=VAL)", p)
		}
		env[k] = v
	}
	return env, nil
}

// multiFlag collects a repeatable string flag (-env A=1 -env B=2).
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}
