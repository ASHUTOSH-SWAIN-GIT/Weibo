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
	file := fs.String("file", "weibo.yaml", "job manifest to deploy")
	dockerfile := fs.String("dockerfile", "Dockerfile", "Dockerfile for the image build")
	buildCtx := fs.String("context", ".", "docker build context directory")
	registry := fs.String("registry", os.Getenv("WEIBO_REGISTRY"), "registry/namespace to push to, e.g. docker.io/<user> (env WEIBO_REGISTRY); a bare manifest image is prefixed with it")
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
		// Resolve the final image ref. With -registry set, a bare manifest
		// image (no "/") is prefixed so `image: orders:1.0` becomes
		// `docker.io/<user>/orders:1.0`. The submitted doc is rewritten to
		// the same ref, so the controller runs exactly what we pushed.
		image := fullImageRef(*registry, m.Image)
		if image != m.Image {
			doc, err = rewriteManifestImage(doc, image)
			if err != nil {
				return fail(fmt.Errorf("rewrite image to %q: %w", image, err))
			}
			fmt.Fprintf(os.Stderr, "weibo: using image %s\n", image)
		}
		if !*noBuild {
			step("building image %s", image)
			if err := runDocker("build", "-f", *dockerfile, "-t", image, *buildCtx); err != nil {
				return fail(fmt.Errorf("build failed for %s: %w", image, err))
			}
			ok("built %s", image)
		}
		if !*noPush {
			step("pushing %s", image)
			if err := runDocker("push", image); err != nil {
				return fail(fmt.Errorf("push failed for %s: %w (is `docker login` done for this registry?)", image, err))
			}
			ok("pushed %s", image)
		}
	} else if !*noBuild {
		fmt.Fprintf(os.Stderr, "weibo: %s is not an SDK image manifest (kind: sdk with image:); submitting without build/push\n", *file)
	}

	step("submitting to controller %s", *controller)
	job, warning, err := newClient(*controller, *token).submit(context.Background(), doc, env)
	if err != nil {
		return fail(fmt.Errorf("submit failed: %w", err))
	}
	if warning != "" {
		// The job was recorded but its launch reported a problem (e.g. the
		// image could not be pulled) — surface it clearly, not as success.
		fmt.Fprintf(os.Stderr, "weibo: ! launch warning: %s\n", warning)
	} else {
		ok("submitted to controller")
	}
	// Final result on stdout (kept clean so scripts can capture the job id).
	fmt.Printf("deployed job %s (%s)\n", job.ID, job.Name)
	fmt.Printf("  weibo status %s\n  weibo logs %s\n", job.ID, job.ID)
	return 0
}

// step logs the start of a deploy phase; ok logs its success. Both go to
// stderr so stdout stays reserved for the final machine-friendly job line.
func step(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "weibo: → "+format+"\n", a...)
}

func ok(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "weibo: ✓ "+format+"\n", a...)
}

// fullImageRef resolves the image to build/push given an optional registry.
// A bare manifest image (no "/") is prefixed with the registry, so
// `image: orders:1.0` + registry `docker.io/user` -> `docker.io/user/orders:1.0`.
// An already-qualified image (contains "/") is respected verbatim, and an
// empty registry leaves the image unchanged.
func fullImageRef(registry, image string) string {
	registry = strings.TrimRight(registry, "/")
	if registry == "" || strings.Contains(image, "/") {
		return image
	}
	return registry + "/" + image
}

// rewriteManifestImage returns doc with its top-level `image:` value set to
// newImage, preserving the rest of the document (order and comments). This
// keeps the submitted manifest in sync with the pushed image so the
// controller runs exactly what deploy pushed.
func rewriteManifestImage(doc []byte, newImage string) ([]byte, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(doc, &root); err != nil {
		return nil, err
	}
	if len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("manifest is not a YAML mapping")
	}
	m := root.Content[0]
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == "image" {
			v := m.Content[i+1]
			v.Kind = yaml.ScalarNode
			v.Tag = "!!str"
			v.Value = newImage
			v.Style = 0
			return yaml.Marshal(&root)
		}
	}
	return nil, fmt.Errorf("no top-level image field to rewrite")
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
