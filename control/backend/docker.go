package backend

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
)

// Docker runs each job as a local Docker container using the runner image.
// The workflow document is copied into the container before start (so it
// works regardless of where the daemon runs), and /data is backed by a
// named volume per job so state and checkpoints survive restarts.
type Docker struct {
	cli   *client.Client
	image string
}

// NewDocker connects to the Docker daemon from the environment. image is
// the runner image tag to launch (e.g. "mailer-runner:dev").
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

const (
	containerWorkflowPath = "/wf/workflow.yaml"
	containerDataDir      = "/data"
	containerSavepointDir = "/savepoints"
	// sharedSavepointVolume is mounted into every job, so a savepoint
	// written by one job is visible to another — a shared namespace like
	// an object-store bucket, which an S3 backend replaces later (P6).
	sharedSavepointVolume = "mailer-savepoints"
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

	// Reuse a per-job named volume so restarts recover from checkpoints,
	// plus the shared savepoints volume visible to every job.
	volName := "mailer-" + spec.JobID
	for _, v := range []string{volName, sharedSavepointVolume} {
		if _, err := d.cli.VolumeCreate(ctx, volume.CreateOptions{Name: v}); err != nil {
			return "", fmt.Errorf("docker: create volume %s: %w", v, err)
		}
	}

	env := []string{
		"WORKFLOW=" + containerWorkflowPath,
		"DATA_DIR=" + containerDataDir,
		"SAVEPOINT_DIR=" + containerSavepointDir,
		"PORT=" + strconv.Itoa(port),
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
		Labels:       map[string]string{"mailer.job": spec.JobID, "mailer.name": spec.Name},
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
	name := fmt.Sprintf("mailer-%s-%d", spec.JobID, time.Now().UnixNano())

	created, err := d.cli.ContainerCreate(ctx, cfg, host, nil, nil, name)
	if err != nil {
		return "", fmt.Errorf("docker: create container: %w", err)
	}

	// Inject the workflow document before the process starts.
	archive, err := tarFile("wf/workflow.yaml", spec.WorkflowDoc)
	if err != nil {
		return "", err
	}
	if err := d.cli.CopyToContainer(ctx, created.ID, "/", archive, container.CopyToContainerOptions{}); err != nil {
		_ = d.cli.ContainerRemove(ctx, created.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("docker: copy workflow: %w", err)
	}

	if err := d.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		_ = d.cli.ContainerRemove(ctx, created.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("docker: start container: %w", err)
	}
	return created.ID, nil
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
