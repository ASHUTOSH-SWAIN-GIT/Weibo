package checkpoint

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Step identifies a point in the coordinated checkpoint protocol.
// The test hook fires AFTER each step succeeds, which is how the
// crash-window tests halt the coordinator at exact protocol positions.
type Step string

const (
	StepPersistPrepared  Step = "persist-prepared"  // checkpoint file written with status=prepared
	StepSinkCommitted    Step = "sink-committed"    // sink transaction committed (EndTxn)
	StepPersistCompleted Step = "persist-completed" // status promoted to completed
	StepOffsetsCommitted Step = "offsets-committed" // advisory broker offset commit
)

// HookAction is what a test hook tells the coordinator to do after a step.
type HookAction int

const (
	// HookContinue proceeds normally.
	HookContinue HookAction = iota
	// HookFail injects a failure at this step: the coordinator aborts
	// the sink transaction and reports a fatal error.
	HookFail
	// HookHalt simulates a process crash: the coordinator stops doing
	// anything, silently, forever. The pipeline is left to be
	// cancelled by the test.
	HookHalt
)

// Coordinator drives the two-phase checkpoint protocol:
//
//	offsets (barrier injection) ─┐
//	operator state (barrier tap) ─┼─► sink prepared ─► persist(prepared)
//	sink ack (PreCommit done)    ─┘        │
//	                                       ▼
//	              sink.Commit ─► persist(completed) ─► source.CommitOffsets
//
// Any failure before the sink commit aborts the sink transaction and
// fails the pipeline; recovery restores the latest completed
// checkpoint (or promotes a prepared one whose transaction provably
// committed — see the engine's recovery decision table).
type Coordinator struct {
	Storage Storage
	TxnID   string

	// CommitSink / AbortSink are the sink's transaction controls.
	CommitSink func(ctx context.Context, id string) error
	AbortSink  func(ctx context.Context, id string) error

	// CommitOffsets commits source offsets externally after a
	// checkpoint completes. Advisory: errors are logged, not fatal.
	// Nil when the source has no external commit.
	CommitOffsets func(ctx context.Context, offsets []byte) error

	// Hook is a test-only seam fired after each protocol step.
	Hook func(step Step, id string) HookAction

	mu      sync.Mutex
	pending map[string]*pendingCheckpoint
	halted  bool

	events   chan string // checkpoint IDs ready to finalize
	fatal    chan error
	wg       sync.WaitGroup
	stopOnce sync.Once
}

type pendingCheckpoint struct {
	offsets   []byte
	state     map[string][]byte
	stateDirs map[string]string
	prepared  bool
	sinkErr   error
}

// NewCoordinator creates a coordinator. Call Start before wiring it
// into a pipeline and Stop after the pipeline finishes.
func NewCoordinator(storage Storage, txnID string) *Coordinator {
	return &Coordinator{
		Storage: storage,
		TxnID:   txnID,
		pending: make(map[string]*pendingCheckpoint),
		events:  make(chan string, 16),
		fatal:   make(chan error, 1),
	}
}

// Start launches the finalize worker. It runs until Stop is called;
// ctx bounds the sink/offset calls made while finalizing.
func (c *Coordinator) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for id := range c.events {
			c.finalize(ctx, id)
		}
	}()
}

// Stop closes the event stream and waits for in-flight finalization.
// Idempotent.
func (c *Coordinator) Stop() {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.halted = true // no further OnSinkPrepared may enqueue
		c.mu.Unlock()
		close(c.events)
	})
	c.wg.Wait()
}

// Fatal returns a channel that receives the first unrecoverable
// coordination error (buffered; never closed).
func (c *Coordinator) Fatal() <-chan error {
	return c.fatal
}

// OnBarrierInjected records the barrier-aligned source offsets for a
// checkpoint at the moment its barrier entered the stream.
func (c *Coordinator) OnBarrierInjected(id string, offsets []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensure(id).offsets = offsets
}

// OnStateSnapshot records operator/worker state captured when the
// barrier reached the pre-sink tap.  stateDirs carries native
// (hard-link) state directory references for Checkpointable backends.
func (c *Coordinator) OnStateSnapshot(id string, state map[string][]byte, stateDirs map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.ensure(id)
	p.state = state
	p.stateDirs = stateDirs
}

// OnSinkPrepared is called by the sink (via its notifier) once all
// pre-barrier output for this checkpoint is staged in the open
// transaction. It queues finalization on the coordinator worker —
// never finalize inline: this runs on the sink's Write goroutine,
// which must stay free to be unblocked by CommitSink.
func (c *Coordinator) OnSinkPrepared(id string, sinkErr error) {
	c.mu.Lock()
	p := c.ensure(id)
	p.prepared = true
	p.sinkErr = sinkErr
	halted := c.halted
	c.mu.Unlock()
	if halted {
		return
	}
	c.events <- id
}

func (c *Coordinator) ensure(id string) *pendingCheckpoint {
	p, ok := c.pending[id]
	if !ok {
		p = &pendingCheckpoint{}
		c.pending[id] = p
	}
	return p
}

func (c *Coordinator) finalize(ctx context.Context, id string) {
	c.mu.Lock()
	if c.halted {
		c.mu.Unlock()
		return
	}
	p := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	if p == nil {
		return
	}

	if p.sinkErr != nil {
		c.abortFatal(ctx, id, fmt.Errorf("checkpoint %s: sink prepare failed: %w", id, p.sinkErr))
		return
	}

	// Step: persist the commit decision (status=prepared, fsynced).
	data := &CheckpointData{
		ID:        id,
		Timestamp: time.Now().UTC(),
		Operators: p.state,
		Source:    make(map[string][]byte),
		Status:    StatusPrepared,
		TxnID:     c.TxnID,
		StateDirs: p.stateDirs,
	}
	if p.offsets != nil {
		data.Source["offset"] = p.offsets
	}
	if err := c.Storage.Save(data); err != nil {
		c.abortFatal(ctx, id, fmt.Errorf("checkpoint %s: persist prepared: %w", id, err))
		return
	}
	if !c.step(ctx, StepPersistPrepared, id) {
		return
	}

	// Step: commit the sink transaction. From here on the output is
	// (or may be) visible — failures can no longer abort.
	if err := c.CommitSink(ctx, id); err != nil {
		// Commit outcome unknown. Recovery resolves it via the
		// transaction marker probe; here we only report.
		c.reportFatal(fmt.Errorf("checkpoint %s: sink commit: %w", id, err))
		return
	}
	if !c.step(ctx, StepSinkCommitted, id) {
		return
	}

	// Step: promote to completed.
	if err := c.Storage.UpdateStatus(id, StatusCompleted); err != nil {
		// Sink already committed: the marker makes this recoverable
		// (prepared + marker visible → promoted on restart).
		c.reportFatal(fmt.Errorf("checkpoint %s: persist completed: %w", id, err))
		return
	}
	if !c.step(ctx, StepPersistCompleted, id) {
		return
	}

	// Step: advisory external offset commit (lag visibility only —
	// the checkpoint file is the recovery source of truth).
	if c.CommitOffsets != nil && p.offsets != nil {
		if err := c.CommitOffsets(ctx, p.offsets); err != nil {
			fmt.Printf("mailer: checkpoint %s: advisory offset commit failed: %v\n", id, err)
		}
	}
	c.step(ctx, StepOffsetsCommitted, id)
}

// step fires the test hook after a successful protocol step. Returns
// false if finalization must stop (injected failure or simulated crash).
func (c *Coordinator) step(ctx context.Context, s Step, id string) bool {
	if c.Hook == nil {
		return true
	}
	switch c.Hook(s, id) {
	case HookFail:
		c.abortFatal(ctx, id, fmt.Errorf("checkpoint %s: injected failure after %s", id, s))
		return false
	case HookHalt:
		c.mu.Lock()
		c.halted = true
		c.mu.Unlock()
		return false
	default:
		return true
	}
}

// abortFatal aborts the sink transaction (pre-commit failures only),
// deletes any native state directories, and reports the pipeline-fatal error.
func (c *Coordinator) abortFatal(ctx context.Context, id string, err error) {
	if c.AbortSink != nil {
		if aerr := c.AbortSink(ctx, id); aerr != nil {
			fmt.Printf("mailer: checkpoint %s: sink abort failed: %v\n", id, aerr)
		}
	}
	c.cleanupStateDirs(id)
	c.reportFatal(err)
}

// cleanupStateDirs deletes the native state directories for a
// checkpoint that will never complete (aborted / failed).
func (c *Coordinator) cleanupStateDirs(id string) {
	if fs, ok := c.Storage.(*FileStorage); ok {
		fs.DeleteStateDirs(id)
	}
}

func (c *Coordinator) reportFatal(err error) {
	select {
	case c.fatal <- err:
	default:
	}
}
