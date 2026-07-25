package backend

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/distribution/reference"
	"github.com/docker/cli/cli/config"
	"github.com/docker/cli/cli/config/configfile"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"k8s.io/apimachinery/pkg/api/resource"
)

// dockerConfigDir, when non-empty, overrides the directory registry
// credentials are read from. Empty uses Docker's default (~/.docker or
// $DOCKER_CONFIG). Set only in tests — docker/cli caches the default config
// dir on first use, which would otherwise make credential tests order-dependent.
var dockerConfigDir string

// Docker runs each job as a local Docker container using the runner image.
// The workflow document is copied into the container before start (so it
// works regardless of where the daemon runs), and /data is backed by a
// named volume per job so state and checkpoints survive restarts.
type Docker struct {
	cli   *client.Client
	image string
}

// NewDocker connects to the Docker daemon from the environment. image is
// the runner image tag to launch (e.g. "weibo-runner:dev").
func NewDocker(image string) (*Docker, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker: connect: %w", err)
	}
	return &Docker{cli: cli, image: image}, nil
}

// Ping verifies the daemon is reachable — used at controller startup and
// to gate integration tests.
func (d *Docker) Ping(ctx context.Context) error {
	_, err := d.cli.Ping(ctx)
	return err
}

// HasImage reports whether the given image reference is present locally,
// so the CLI can fail fast with a build hint instead of a launch error.
func (d *Docker) HasImage(ctx context.Context, ref string) (bool, error) {
	list, err := d.cli.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", ref)),
	})
	if err != nil {
		return false, err
	}
	return len(list) > 0, nil
}

// pullImage pulls ref according to policy, resolving registry credentials from
// the host's Docker config. It is a no-op for PullNever, and for
// PullIfNotPresent when the image is already present locally.
func (d *Docker) pullImage(ctx context.Context, ref, policy string) error {
	if policy == "" {
		policy = PullIfNotPresent
	}
	if policy == PullNever {
		return nil
	}
	if policy == PullIfNotPresent {
		if ok, err := d.HasImage(ctx, ref); err == nil && ok {
			return nil // already local; skip the pull
		}
	}
	rc, err := d.cli.ImagePull(ctx, ref, image.PullOptions{RegistryAuth: d.resolveRegistryAuth(ref)})
	if err != nil {
		return err
	}
	defer rc.Close()
	// Draining the stream to EOF is what makes the pull actually complete.
	_, err = io.Copy(io.Discard, rc)
	return err
}

// resolveRegistryAuth returns the base64 X-Registry-Auth string for pulling
// ref, resolved from the host's Docker credentials — ~/.docker/config.json
// and any configured credential helper (e.g. docker-credential-ecr-login fed
// by node IAM). It honors a host that has run `docker login`. The docker Go
// SDK's ImagePull does NOT read the Docker config itself, so we resolve it
// here. Returns "" when no credentials are configured for the image's
// registry, so public images still pull anonymously; credential-resolution
// errors are non-fatal and degrade to an anonymous pull.
func (d *Docker) resolveRegistryAuth(ref string) string {
	named, err := reference.ParseNormalizedNamed(ref)
	if err != nil {
		return ""
	}
	// Docker Hub credentials are stored under the legacy index URL key.
	key := reference.Domain(named)
	if key == "docker.io" {
		key = "https://index.docker.io/v1/"
	}

	var cfg *configfile.ConfigFile
	if dockerConfigDir != "" { // test override; empty uses the default location
		if cfg, err = config.Load(dockerConfigDir); err != nil {
			return ""
		}
	} else {
		cfg = config.LoadDefaultConfigFile(io.Discard)
	}
	authCfg, err := cfg.GetAuthConfig(key)
	if err != nil {
		return ""
	}
	if authCfg.Username == "" && authCfg.Password == "" &&
		authCfg.IdentityToken == "" && authCfg.RegistryToken == "" {
		return "" // no credentials for this registry
	}
	encoded, err := registry.EncodeAuthConfig(registry.AuthConfig{
		Username:      authCfg.Username,
		Password:      authCfg.Password,
		Auth:          authCfg.Auth,
		ServerAddress: authCfg.ServerAddress,
		IdentityToken: authCfg.IdentityToken,
		RegistryToken: authCfg.RegistryToken,
	})
	if err != nil {
		return ""
	}
	return encoded
}

const (
	containerWorkflowPath = "/wf/workflow.yaml"
	containerDataDir      = "/data"
	containerSavepointDir = "/savepoints"
	// sharedSavepointVolume is mounted into every job, so a savepoint
	// written by one job is visible to another — a shared namespace like
	// an object-store bucket, which an S3 backend replaces later (P6).
	sharedSavepointVolume = "weibo-savepoints"
)

func (d *Docker) Launch(ctx context.Context, spec LaunchSpec) (string, error) {
	image := spec.Image
	if image == "" {
		image = d.image
	}
	port := spec.ControlPort
	if port == 0 {
		port = 8080
	}
	portSpec := nat.Port(strconv.Itoa(port) + "/tcp")

	// Pull the image (per policy) so registry-hosted images work without a
	// manual `docker pull`. Fall back to a locally-present image so an
	// air-gapped or locally-built image still launches.
	if err := d.pullImage(ctx, image, spec.PullPolicy); err != nil {
		if ok, herr := d.HasImage(ctx, image); herr == nil && ok {
			log.Printf("docker: pull %s failed (%v); using local image", image, err)
		} else {
			return "", fmt.Errorf("docker: pull image %s: %w", image, err)
		}
	}

	// Reuse a per-job named volume so restarts recover from checkpoints,
	// plus the shared savepoints volume visible to every job.
	volName := "weibo-" + spec.JobID
	for _, v := range []string{volName, sharedSavepointVolume} {
		if _, err := d.cli.VolumeCreate(ctx, volume.CreateOptions{Name: v}); err != nil {
			return "", fmt.Errorf("docker: create volume %s: %w", v, err)
		}
	}

	// An SDK job's image has the pipeline compiled in — no workflow doc to
	// inject, and WORKFLOW is left unset.
	sdkJob := len(spec.WorkflowDoc) == 0

	env := []string{
		"DATA_DIR=" + containerDataDir,
		"SAVEPOINT_DIR=" + containerSavepointDir,
		"PORT=" + strconv.Itoa(port),
	}
	if !sdkJob {
		env = append(env, "WORKFLOW="+containerWorkflowPath)
	}
	if spec.RestoreSavepoint != "" {
		env = append(env, "RESTORE_SAVEPOINT="+spec.RestoreSavepoint)
	}
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}

	cfg := &container.Config{
		Image:        image,
		Env:          env,
		ExposedPorts: nat.PortSet{portSpec: struct{}{}},
		Labels:       map[string]string{"weibo.job": spec.JobID, "weibo.name": spec.Name},
	}
	host := &container.HostConfig{
		Binds: []string{
			volName + ":" + containerDataDir,
			sharedSavepointVolume + ":" + containerSavepointDir,
		},
		PortBindings: nat.PortMap{
			portSpec: []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: ""}},
		},
	}
	if err := applyResources(host, spec.Resources); err != nil {
		return "", fmt.Errorf("docker: %w", err)
	}
	name := fmt.Sprintf("weibo-%s-%d", spec.JobID, time.Now().UnixNano())

	created, err := d.cli.ContainerCreate(ctx, cfg, host, nil, nil, name)
	if err != nil {
		return "", fmt.Errorf("docker: create container: %w", err)
	}

	// Inject the workflow document before the process starts (YAML jobs).
	if !sdkJob {
		archive, err := tarFile("wf/workflow.yaml", spec.WorkflowDoc)
		if err != nil {
			return "", err
		}
		if err := d.cli.CopyToContainer(ctx, created.ID, "/", archive, container.CopyToContainerOptions{}); err != nil {
			_ = d.cli.ContainerRemove(ctx, created.ID, container.RemoveOptions{Force: true})
			return "", fmt.Errorf("docker: copy workflow: %w", err)
		}
	}

	if err := d.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		_ = d.cli.ContainerRemove(ctx, created.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("docker: start container: %w", err)
	}
	return created.ID, nil
}

// applyResources maps ResourceLimits onto the Docker HostConfig. A nil
// limit or empty field leaves that dimension unconstrained. It parses the
// same Kubernetes quantity strings the controller validates and the K8s
// backend uses, so all three agree on what "500m"/"1Gi" mean.
func applyResources(host *container.HostConfig, r *ResourceLimits) error {
	if r == nil {
		return nil
	}
	if r.CPU != "" {
		nano, err := cpuToNanoCPUs(r.CPU)
		if err != nil {
			return fmt.Errorf("cpu %q: %w", r.CPU, err)
		}
		host.NanoCPUs = nano
	}
	if r.Memory != "" {
		q, err := resource.ParseQuantity(r.Memory)
		if err != nil {
			return fmt.Errorf("memory %q: %w", r.Memory, err)
		}
		host.Memory = q.Value()
	}
	return nil
}

// cpuToNanoCPUs converts a Kubernetes CPU quantity to Docker NanoCPUs
// (1 CPU = 1e9). Millicores map directly: "500m" → 5e8, "2" → 2e9.
func cpuToNanoCPUs(s string) (int64, error) {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, err
	}
	return q.MilliValue() * 1_000_000, nil // millicores → nanocores
}

func (d *Docker) Stop(ctx context.Context, id string, timeout time.Duration) error {
	secs := int(timeout.Seconds())
	err := d.cli.ContainerStop(ctx, id, container.StopOptions{Timeout: &secs})
	if err != nil && client.IsErrNotFound(err) {
		return nil // already gone
	}
	return err
}

func (d *Docker) Status(ctx context.Context, id string) (Status, error) {
	info, err := d.cli.ContainerInspect(ctx, id)
	if err != nil {
		if client.IsErrNotFound(err) {
			return Status{Phase: PhaseGone}, nil
		}
		return Status{}, err
	}
	st := Status{}
	switch {
	case info.State != nil && info.State.Running:
		st.Phase = PhaseRunning
	default:
		st.Phase = PhaseExited
		if info.State != nil {
			st.ExitCode = info.State.ExitCode
		}
	}
	// Resolve the published control port.
	for _, portMap := range info.NetworkSettings.Ports {
		for _, b := range portMap {
			if b.HostPort != "" {
				if p, err := strconv.Atoi(b.HostPort); err == nil {
					st.HostPort = p
					host := b.HostIP
					if host == "" || host == "0.0.0.0" {
						host = "127.0.0.1"
					}
					st.Address = host + ":" + b.HostPort
				}
			}
		}
	}
	return st, nil
}

func (d *Docker) Logs(ctx context.Context, id string, tail int) (string, error) {
	opts := container.LogsOptions{ShowStdout: true, ShowStderr: true}
	if tail > 0 {
		opts.Tail = strconv.Itoa(tail)
	}
	rc, err := d.cli.ContainerLogs(ctx, id, opts)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	// Container logs are a multiplexed stream; demux stdout+stderr.
	var out bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &out, rc); err != nil {
		return out.String(), err
	}
	return out.String(), nil
}

func (d *Docker) Remove(ctx context.Context, id string) error {
	err := d.cli.ContainerRemove(ctx, id, container.RemoveOptions{Force: true})
	if err != nil && client.IsErrNotFound(err) {
		return nil
	}
	return err
}

// tarFile builds a one-file tar archive for CopyToContainer.
func tarFile(name string, content []byte) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := &tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, err
	}
	if _, err := tw.Write(content); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}
