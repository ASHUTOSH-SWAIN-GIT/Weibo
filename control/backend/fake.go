package backend

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Fake is an in-memory ContainerBackend for controller unit tests. It
// records launches and lets a test drive each container's phase without
// Docker. Safe for concurrent use.
type Fake struct {
	mu         sync.Mutex
	seq        int
	containers map[string]*fakeContainer
	// LaunchErr, if set, makes the next Launch fail (then clears).
	LaunchErr error
}

type fakeContainer struct {
	spec     LaunchSpec
	status   Status
	logs     string
	removed  bool
	launched time.Time
}

// NewFake returns a ready fake backend.
func NewFake() *Fake {
	return &Fake{containers: map[string]*fakeContainer{}}
}

func (f *Fake) Launch(ctx context.Context, spec LaunchSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.LaunchErr != nil {
		err := f.LaunchErr
		f.LaunchErr = nil
		return "", err
	}
	f.seq++
	id := fmt.Sprintf("fake-%s-%d", spec.JobID, f.seq)
	f.containers[id] = &fakeContainer{
		spec:     spec,
		status:   Status{Phase: PhaseRunning, HostPort: 30000 + f.seq, Address: fmt.Sprintf("127.0.0.1:%d", 30000+f.seq)},
		launched: time.Now(),
	}
	return id, nil
}

func (f *Fake) Stop(ctx context.Context, id string, timeout time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[id]; ok {
		c.status.Phase = PhaseExited
		c.status.ExitCode = 0
	}
	return nil
}

func (f *Fake) Status(ctx context.Context, id string) (Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[id]
	if !ok || c.removed {
		return Status{Phase: PhaseGone}, nil
	}
	return c.status, nil
}

func (f *Fake) Logs(ctx context.Context, id string, tail int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[id]; ok {
		return c.logs, nil
	}
	return "", nil
}

func (f *Fake) Remove(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[id]; ok {
		c.removed = true
	}
	return nil
}

// --- test controls (not part of ContainerBackend) ---

// SetPhase forces a container's phase, simulating an exit or crash.
func (f *Fake) SetPhase(id string, phase Phase, exitCode int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[id]; ok {
		c.status.Phase = phase
		c.status.ExitCode = exitCode
	}
}

// SetLogs sets the log text a container returns.
func (f *Fake) SetLogs(id, logs string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[id]; ok {
		c.logs = logs
	}
}

// LastEnv returns the env a container was launched with (for assertions).
func (f *Fake) LastEnv(id string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[id]; ok {
		return c.spec.Env
	}
	return nil
}

// Launched reports how many containers were ever launched.
func (f *Fake) Launched() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seq
}

// compile-time check.
var _ ContainerBackend = (*Fake)(nil)
