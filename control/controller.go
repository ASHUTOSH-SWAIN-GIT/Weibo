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
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

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
	httpc       *http.Client // talks to job agents (savepoint trigger)

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
		httpc:       &http.Client{Timeout: 10 * time.Second},
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
	// Auto-detect the kind: a manifest with `kind: sdk` is a prebuilt Go
	// pipeline image; anything else is a declarative YAML workflow.
	if m, ok := parseSDKManifest(doc); ok {
		return c.submitSDK(ctx, doc, m, env)
	}

	name, delivery, graph, err := c.validate(doc, env)
	if err != nil {
		return nil, fmt.Errorf("submit: invalid workflow: %w", err)
	}

	now := time.Now().UTC()
	job := &store.Job{
		ID:       c.newID(),
		Name:     name,
		Kind:     store.KindYAML,
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

	if err := c.launch(ctx, job, 1, ""); err != nil {
		// The job is recorded; the reconciler will not retry a launch
		// failure automatically, so surface it to the caller.
		return job, fmt.Errorf("submit: launch: %w", err)
	}
	return job, nil
}

// sdkManifest is the submission for a prebuilt SDK job image.
type sdkManifest struct {
	Kind  string `yaml:"kind"`
	Name  string `yaml:"name"`
	Image string `yaml:"image"`
}

// parseSDKManifest reports whether doc is an SDK job manifest (kind: sdk).
// A YAML workflow has no `kind`, so it never matches.
func parseSDKManifest(doc []byte) (sdkManifest, bool) {
	var m sdkManifest
	if err := yaml.Unmarshal(doc, &m); err != nil {
		return sdkManifest{}, false
	}
	return m, strings.EqualFold(m.Kind, store.KindSDK)
}

// submitSDK records and launches a prebuilt SDK job image. There is no
// workflow to compile; the pipeline lives in the image. The topology is
// discovered at runtime from the agent's /describe.
func (c *Controller) submitSDK(ctx context.Context, doc []byte, m sdkManifest, env map[string]string) (*store.Job, error) {
	if m.Name == "" {
		return nil, fmt.Errorf("submit: sdk manifest missing 'name'")
	}
	if m.Image == "" {
		return nil, fmt.Errorf("submit: sdk manifest missing 'image'")
	}

	now := time.Now().UTC()
	job := &store.Job{
		ID:      c.newID(),
		Name:    m.Name,
		Kind:    store.KindSDK,
		Image:   m.Image,
		Spec:    string(doc),
		Desired: store.DesiredRunning,
		Created: now,
		Updated: now,
	}
	if err := c.store.CreateJob(job); err != nil {
		return nil, err
	}
	c.setSecrets(job.ID, env)
	c.transition(job.ID, "", "", lifecycle.Submitted, "submitted (sdk)")

	if err := c.launch(ctx, job, 1, ""); err != nil {
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
	c.finishRun(run, lifecycle.Running, lifecycle.Cancelled, "user cancel")
	return nil
}

// Restart stops any live run and launches a fresh one, resetting the
// desired state to running and the attempt counter.
func (c *Controller) Restart(ctx context.Context, jobID string) (*store.Job, error) {
	return c.doRestart(ctx, jobID, "")
}

// RestartFromSavepoint restarts the job, seeding the new run's state from
// the named savepoint instead of the last automatic checkpoint.
func (c *Controller) RestartFromSavepoint(ctx context.Context, jobID, label string) (*store.Job, error) {
	if label == "" {
		return nil, fmt.Errorf("restart: empty savepoint label")
	}
	return c.doRestart(ctx, jobID, label)
}

func (c *Controller) doRestart(ctx context.Context, jobID, restore string) (*store.Job, error) {
	job, err := c.store.GetJob(jobID)
	if err != nil {
		return nil, err
	}
	if run, _ := c.store.LatestRun(jobID); run != nil && run.Stopped == nil {
		_ = c.backend.Stop(ctx, run.ContainerID, c.stopTimeout)
		c.finishRun(run, lifecycle.Running, lifecycle.Cancelled, "restart")
	}
	if err := c.store.SetDesired(jobID, store.DesiredRunning); err != nil {
		return nil, err
	}
	if err := c.launch(ctx, job, 1, restore); err != nil {
		return job, fmt.Errorf("restart: launch: %w", err)
	}
	return job, nil
}

// Savepoint triggers a stop-with-savepoint on the job's live container:
// the job drains, writes its final checkpoint, and the runner promotes it
// to a named savepoint. The job's desired state becomes stopped.
func (c *Controller) Savepoint(ctx context.Context, jobID, label string) error {
	if label == "" {
		return fmt.Errorf("savepoint: empty label")
	}
	addr, err := c.ControlAddress(ctx, jobID)
	if err != nil {
		return err
	}
	if addr == "" {
		return fmt.Errorf("savepoint: job %s has no running control surface", jobID)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+addr+"/savepoint?label="+url.QueryEscape(label), nil)
	resp, err := c.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("savepoint: contact agent: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("savepoint: agent returned %d", resp.StatusCode)
	}
	// The job is stopping; record intent and audit it.
	if err := c.store.SetDesired(jobID, store.DesiredStopped); err != nil {
		return err
	}
	c.transition(jobID, "", lifecycle.Running, lifecycle.Cancelling, "savepoint: "+label)
	return nil
}

// Validate compiles a workflow without launching it — the dry-run preview
// behind the submit form. Returns the name, delivery guarantee, and graph
// a submit would produce, or an error if the workflow is invalid.
func (c *Controller) Validate(doc []byte, env map[string]string) (string, compiler.DeliveryGuarantee, compiler.PipelineGraph, error) {
	return c.validate(doc, env)
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

// launch starts a container for the job and records the run. restore, if
// non-empty, names a savepoint the new run seeds from.
func (c *Controller) launch(ctx context.Context, job *store.Job, attempt int, restore string) error {
	// Single-live-run fencing: never run two containers for one job at
	// once — two live transactional producers with the same id would
	// break exactly-once. Callers stop the prior run before relaunching.
	if prev, _ := c.store.LatestRun(job.ID); prev != nil && prev.Stopped == nil {
		return fmt.Errorf("job %s already has an active run %s", job.ID, prev.ID)
	}

	now := time.Now().UTC()
	run := &store.Run{
		ID:      c.newID(),
		JobID:   job.ID,
		Phase:   string(lifecycle.Starting),
		Attempt: attempt,
		Started: now,
	}

	// SDK jobs run their own prebuilt image with the pipeline compiled in;
	// YAML jobs run the generic runner image with the workflow document
	// injected.
	img := c.image
	var doc []byte
	if job.Kind == store.KindSDK {
		img = job.Image
	} else {
		doc = []byte(job.Spec)
	}

	id, err := c.backend.Launch(ctx, backend.LaunchSpec{
		JobID:            job.ID,
		Name:             job.Name,
		Image:            img,
		WorkflowDoc:      doc,
		Env:              c.launchEnv(job.ID),
		ControlPort:      c.port,
		RestoreSavepoint: restore,
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

// launchEnv builds the container environment: the job's secrets plus
// MAILER_JOB_ID. Authors can pin a stable transactional id across
// restarts by referencing it (e.g. transactionalID: ${MAILER_JOB_ID}),
// which — with single-live-run fencing — keeps exactly-once safe.
func (c *Controller) launchEnv(jobID string) map[string]string {
	env := map[string]string{"MAILER_JOB_ID": jobID}
	maps.Copy(env, c.getSecrets(jobID))
	return env
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
