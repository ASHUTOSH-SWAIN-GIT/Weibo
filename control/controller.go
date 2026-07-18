// Package control is the Mailer job control plane: it accepts workflow
// submissions, launches one container per job through a ContainerBackend,
// persists everything to a Store, and reconciles the running containers
// toward each job's desired state. It is the JobManager equivalent for
// the single-container-per-job model (see the job-orchestration plan, P3).
package control

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/store"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/compiler"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/secrets"
)

// Options configures a Controller.
type Options struct {
	Store       store.Store
	Backend     backend.ContainerBackend
	Image       string                  // runner image tag
	ControlPort int                     // container control port (default 8080)
	Restart     lifecycle.RestartPolicy // default: lifecycle.DefaultRestartPolicy()
	StopTimeout time.Duration           // graceful stop wait (default 30s)
	NewID       func() string           // override for deterministic tests
	Logf        func(string, ...any)    // optional logger
}

// Controller ties the store, backend, and lifecycle rules together.
type Controller struct {
	store       store.Store
	backend     backend.ContainerBackend
	image       string
	port        int
	restart     lifecycle.RestartPolicy
	stopTimeout time.Duration
	newID       func() string
	logf        func(string, ...any)

	mu      sync.Mutex
	secrets map[string]map[string]string // jobID → env; process-memory only, never persisted
}

// New builds a Controller, applying defaults.
func New(opts Options) *Controller {
	c := &Controller{
		store:       opts.Store,
		backend:     opts.Backend,
		image:       opts.Image,
		port:        opts.ControlPort,
		restart:     opts.Restart,
		stopTimeout: opts.StopTimeout,
		newID:       opts.NewID,
		logf:        opts.Logf,
		secrets:     map[string]map[string]string{},
	}
	if c.port == 0 {
		c.port = 8080
	}
	if c.restart == (lifecycle.RestartPolicy{}) {
		c.restart = lifecycle.DefaultRestartPolicy()
	}
	if c.stopTimeout == 0 {
		c.stopTimeout = 30 * time.Second
	}
	if c.newID == nil {
		c.newID = randomID
	}
	if c.logf == nil {
		c.logf = func(string, ...any) {}
	}
	return c
}

// Submit validates a workflow document, records the job (desired:
// running), and launches its first container. The env carries any
// secrets; it is used for validation and passed to the container but
// never persisted. A validation failure rejects the job before any
// container starts.
func (c *Controller) Submit(ctx context.Context, doc []byte, env map[string]string) (*store.Job, error) {
	name, delivery, graph, err := c.validate(doc, env)
	if err != nil {
		return nil, fmt.Errorf("submit: invalid workflow: %w", err)
	}

	now := time.Now().UTC()
	job := &store.Job{
		ID:       c.newID(),
		Name:     name,
		Spec:     string(doc),
		Delivery: delivery,
		Graph:    graph,
		Desired:  store.DesiredRunning,
		Created:  now,
		Updated:  now,
	}
	if err := c.store.CreateJob(job); err != nil {
		return nil, err
	}
	c.setSecrets(job.ID, env)
	c.transition(job.ID, "", "", lifecycle.Submitted, "submitted")

	if err := c.launch(ctx, job, 1); err != nil {
		// The job is recorded; the reconciler will not retry a launch
		// failure automatically, so surface it to the caller.
		return job, fmt.Errorf("submit: launch: %w", err)
	}
	return job, nil
}

// Cancel sets the job's desired state to stopped and stops its live
// container gracefully. The container is kept (not removed) so logs stay
// available; a later Restart or GC removes it.
func (c *Controller) Cancel(ctx context.Context, jobID string) error {
	if _, err := c.store.GetJob(jobID); err != nil {
		return err
	}
	if err := c.store.SetDesired(jobID, store.DesiredStopped); err != nil {
		return err
	}
	run, err := c.store.LatestRun(jobID)
	if err != nil {
		return err
	}
	if run == nil || run.Stopped != nil {
		return nil // nothing live to stop
	}
	_ = c.backend.Stop(ctx, run.ContainerID, c.stopTimeout)
	c.finishRun(run, lifecycle.Cancelling, lifecycle.Cancelled, "user cancel")
	return nil
}

// Restart stops any live run and launches a fresh one, resetting the
// desired state to running and the attempt counter.
func (c *Controller) Restart(ctx context.Context, jobID string) (*store.Job, error) {
	job, err := c.store.GetJob(jobID)
	if err != nil {
		return nil, err
	}
	if run, _ := c.store.LatestRun(jobID); run != nil && run.Stopped == nil {
		_ = c.backend.Stop(ctx, run.ContainerID, c.stopTimeout)
		c.finishRun(run, lifecycle.Cancelling, lifecycle.Cancelled, "restart")
	}
	if err := c.store.SetDesired(jobID, store.DesiredRunning); err != nil {
		return nil, err
	}
	if err := c.launch(ctx, job, 1); err != nil {
		return job, fmt.Errorf("restart: launch: %w", err)
	}
	return job, nil
}

// ListJobs returns all jobs, newest first.
func (c *Controller) ListJobs() ([]*store.Job, error) { return c.store.ListJobs() }

// GetJob returns one job.
func (c *Controller) GetJob(id string) (*store.Job, error) { return c.store.GetJob(id) }

// LatestRun returns the most recent run for a job (nil if never launched).
func (c *Controller) LatestRun(jobID string) (*store.Run, error) { return c.store.LatestRun(jobID) }

// Transitions returns a job's lifecycle audit log.
func (c *Controller) Transitions(jobID string) ([]*store.Transition, error) {
	return c.store.ListTransitions(jobID)
}

// Logs returns up to tail lines from a job's latest container.
func (c *Controller) Logs(ctx context.Context, jobID string, tail int) (string, error) {
	run, err := c.store.LatestRun(jobID)
	if err != nil {
		return "", err
	}
	if run == nil || run.ContainerID == "" {
		return "", nil
	}
	return c.backend.Logs(ctx, run.ContainerID, tail)
}

// ControlAddress returns the host:port of a job's live control surface,
// or "" if the job has no reachable running container.
func (c *Controller) ControlAddress(ctx context.Context, jobID string) (string, error) {
	run, err := c.store.LatestRun(jobID)
	if err != nil || run == nil || run.ContainerID == "" || run.Stopped != nil {
		return "", err
	}
	st, err := c.backend.Status(ctx, run.ContainerID)
	if err != nil {
		return "", err
	}
	return st.Address, nil
}

// --- internal ---

// validate parses and compiles the document to confirm it is a runnable
// workflow, returning the non-secret metadata to persist. It compiles in
// a throwaway data dir so validation never touches real job state.
func (c *Controller) validate(doc []byte, env map[string]string) (string, compiler.DeliveryGuarantee, compiler.PipelineGraph, error) {
	spec, err := workflow.ParseYAML(doc)
	if err != nil {
		return "", "", compiler.PipelineGraph{}, err
	}
	tmp, err := os.MkdirTemp("", "mailer-validate-")
	if err != nil {
		return "", "", compiler.PipelineGraph{}, err
	}
	defer os.RemoveAll(tmp)

	comp := &compiler.Compiler{
		BaseDataDir: tmp,
		Secrets:     chainResolver{env: env},
	}
	cw, err := comp.CompileWorkflow(spec)
	if err != nil {
		return "", "", compiler.PipelineGraph{}, err
	}
	return cw.Name, cw.Delivery, cw.Graph, nil
}

// launch starts a container for the job and records the run.
func (c *Controller) launch(ctx context.Context, job *store.Job, attempt int) error {
	now := time.Now().UTC()
	run := &store.Run{
		ID:      c.newID(),
		JobID:   job.ID,
		Phase:   string(lifecycle.Starting),
		Attempt: attempt,
		Started: now,
	}

	id, err := c.backend.Launch(ctx, backend.LaunchSpec{
		JobID:       job.ID,
		Name:        job.Name,
		Image:       c.image,
		WorkflowDoc: []byte(job.Spec),
		Env:         c.getSecrets(job.ID),
		ControlPort: c.port,
	})
	if err != nil {
		run.Phase = string(lifecycle.Failed)
		run.Error = err.Error()
		run.Stopped = &now
		_ = c.store.CreateRun(run)
		c.transition(job.ID, run.ID, lifecycle.Starting, lifecycle.Failed, err.Error())
		return err
	}

	run.ContainerID = id
	run.Phase = string(lifecycle.Running)
	if st, err := c.backend.Status(ctx, id); err == nil {
		run.HostPort = st.HostPort
	}
	if err := c.store.CreateRun(run); err != nil {
		return err
	}
	c.transition(job.ID, run.ID, lifecycle.Submitted, lifecycle.Running, "launched")
	c.logf("job %s: launched run %s (container %s, attempt %d)", job.ID, run.ID, id, attempt)
	return nil
}

// finishRun marks a run terminal in the store and logs the transition.
func (c *Controller) finishRun(run *store.Run, from, to lifecycle.Phase, reason string) {
	now := time.Now().UTC()
	run.Phase = string(to)
	run.Stopped = &now
	_ = c.store.UpdateRun(run)
	c.transition(run.JobID, run.ID, from, to, reason)
}

func (c *Controller) transition(jobID, runID string, from, to lifecycle.Phase, reason string) {
	_ = c.store.AppendTransition(&store.Transition{
		JobID: jobID, RunID: runID,
		From: string(from), To: string(to),
		Reason: reason, At: time.Now().UTC(),
	})
}

func (c *Controller) setSecrets(jobID string, env map[string]string) {
	if len(env) == 0 {
		return
	}
	cp := make(map[string]string, len(env))
	maps.Copy(cp, env)
	c.mu.Lock()
	c.secrets[jobID] = cp
	c.mu.Unlock()
}

func (c *Controller) getSecrets(jobID string) map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.secrets[jobID]
}

func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// chainResolver resolves ${VAR} from the submit-supplied env first, then
// falls back to the controller's process environment. It never includes
// resolved values in errors.
type chainResolver struct{ env map[string]string }

func (r chainResolver) Resolve(name string) (string, error) {
	if v, ok := r.env[name]; ok {
		return v, nil
	}
	return secrets.Environment{}.Resolve(name)
}
