package jobagent

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Agent supervises one job. It is safe for concurrent use: State/Cancel
// may be called from HTTP handlers while Run executes on another
// goroutine.
type Agent struct {
	env *weibo.StreamExecutionEnv

	mu     sync.Mutex
	st     State
	cancel context.CancelFunc

	logger *slog.Logger
	tracer trace.Tracer

	// savepoint request (stop-with-savepoint). When set, the runner
	// promotes the final checkpoint to a savepoint after Run returns.
	savepointReq   bool
	savepointLabel string
}

// New creates an agent that will run the given configured environment.
// The env must already have its source, sink, and any operators wired
// (e.g. from the workflow compiler); the agent does not build pipelines.
func New(env *weibo.StreamExecutionEnv) *Agent {
	return &Agent{
		env: env,
		st:  State{Phase: PhaseStarting},
	}
}

// SetLogger sets the structured logger for agent lifecycle events (run
// start/finish/failure). Chainable; nil restores the default.
func (a *Agent) SetLogger(l *slog.Logger) *Agent {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.logger = l
	return a
}

// SetTracer sets the tracer for the agent's job-run span. Chainable;
// nil restores the no-op tracer.
func (a *Agent) SetTracer(t trace.Tracer) *Agent {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tracer = t
	return a
}

// log returns the agent logger, defaulting to a component-tagged slog
// default.
func (a *Agent) log() *slog.Logger {
	if a.logger != nil {
		return a.logger
	}
	return slog.Default().With("component", "agent")
}

// tracing returns the agent tracer, defaulting to no-op.
func (a *Agent) tracing() trace.Tracer {
	if a.tracer != nil {
		return a.tracer
	}
	return trace.Noop()
}

// Run executes the job to completion, blocking until it finishes, fails,
// or a cancel drains it. It registers the checkpoint listener, moves the
// phase to Running, then calls Execute. The returned error is Execute's
// error (nil on a clean finish or a completed cancel). Run is intended to
// be called once.
func (a *Agent) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)

	a.mu.Lock()
	a.cancel = cancel
	a.st.StartedAt = time.Now()
	a.st.Phase = PhaseRunning
	a.mu.Unlock()

	// Surface checkpoint progress in /state (both delivery modes).
	a.env.WithCheckpointObserver(a.onCheckpointReport)

	a.log().Info("job run started")
	rctx, runSpan := a.tracing().Start(runCtx, "agent.run")
	defer runSpan.End()

	err := a.env.Execute(rctx)
	// Capture whether shutdown was actually requested (SIGTERM propagating
	// into ctx, or an explicit Cancel() call, which cancels runCtx
	// directly) BEFORE the unconditional cancel() below — which would
	// otherwise make runCtx.Err() always non-nil from here on and defeat
	// the point of checking it.
	shutdownRequested := runCtx.Err() != nil
	cancel() // release the context; harmless if already cancelled

	a.mu.Lock()
	defer a.mu.Unlock()
	// A requested cancel drains gracefully; Execute may return nil or a
	// context-cancellation error, and either is a clean finish. But a
	// context.Canceled error alone does NOT mean that: Execute's internal
	// two-phase shutdown also produces context.Canceled from a completely
	// unrelated, unrequested internal fatal error (e.g. the sink can't
	// reach a dead Kafka broker triggers its own force-unwind, and every
	// other stage then reports context.Canceled too). Treating any
	// context.Canceled as "clean" made a real infrastructure failure
	// report as PhaseFinished — found live. shutdownRequested is exactly
	// the missing check: it is false in that scenario, since nothing
	// outside Execute ever asked this job to stop.
	if err != nil && !(errors.Is(err, context.Canceled) && shutdownRequested) {
		a.st.Phase = PhaseFailed
		a.st.LastError = err.Error()
		runSpan.RecordError(err)
		a.log().Error("job run failed", append(trace.Attrs(rctx), "error", err)...)
	} else {
		a.st.Phase = PhaseFinished
		a.log().Info("job run finished")
	}
	return err
}

// Cancel requests a graceful shutdown. It is idempotent and a no-op once
// the job has reached a terminal phase. The pipeline drains in-flight
// records and takes a final checkpoint before Run returns.
func (a *Agent) Cancel() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cancel == nil || a.st.Phase.Terminal() {
		return
	}
	a.st.Phase = PhaseCancelling
	a.cancel()
}

// RequestSavepoint asks for a stop-with-savepoint: the job drains and
// writes its final checkpoint (like Cancel), and the label is recorded so
// the runner can promote that checkpoint to a named savepoint once Run
// returns. It is a no-op once the job is terminal; returns whether the
// request was accepted.
func (a *Agent) RequestSavepoint(label string) bool {
	a.mu.Lock()
	if a.cancel == nil || a.st.Phase.Terminal() {
		a.mu.Unlock()
		return false
	}
	a.savepointReq = true
	a.savepointLabel = label
	a.mu.Unlock()
	a.Cancel()
	return true
}

// SavepointRequest reports whether a savepoint was requested and its
// label. The runner reads this after Run returns to decide whether to
// promote the final checkpoint.
func (a *Agent) SavepointRequest() (label string, requested bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.savepointLabel, a.savepointReq
}

// State returns a snapshot, refreshing the live-computed fields (uptime
// and record counts) at call time.
func (a *Agent) State() State {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := a.st
	if !st.StartedAt.IsZero() {
		st.Uptime = time.Since(st.StartedAt).Round(time.Second).String()
	}
	st.RecordsIn = counterValue(metrics.RecordsReadTotal)
	st.RecordsOut = counterValue(metrics.RecordsWrittenTotal)
	st.Ready = st.Phase == PhaseRunning
	if a.env != nil {
		st.Source = a.env.SourceOperationalState()
	}
	return st
}

// DescribeJSON returns the pipeline topology as indented JSON.
func (a *Agent) DescribeJSON() string {
	return a.env.DescribeJSON()
}

// PlanJSON returns the executed stage topology (DAG nodes + edges) as
// indented JSON, for the dashboard's live pipeline graph.
func (a *Agent) PlanJSON() string {
	return a.env.PlanJSON()
}

// onCheckpointReport records the latest completed checkpoint. Called from
// the engine (coordinator finalize goroutine or the uncoordinated save
// path); must be cheap and non-blocking.
func (a *Agent) onCheckpointReport(rep weibo.CheckpointReport) {
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.st.CurrentCheckpointID = rep.ID
	a.st.LastCheckpointAt = &now
	// Keep a bounded history, newest first, so /state can show a
	// checkpoints section without unbounded memory growth.
	cp := Checkpoint{ID: rep.ID, CompletedAt: now, DurationMs: rep.Duration.Milliseconds(), SizeBytes: rep.InlineBytes}
	a.st.Checkpoints = append([]Checkpoint{cp}, a.st.Checkpoints...)
	metrics.CheckpointDurationSeconds.Observe(rep.Duration.Seconds())
	metrics.CheckpointSizeBytes.Observe(float64(rep.InlineBytes))
	const maxCheckpoints = 10
	if len(a.st.Checkpoints) > maxCheckpoints {
		a.st.Checkpoints = a.st.Checkpoints[:maxCheckpoints]
	}
	a.log().Debug("checkpoint observed",
		"checkpoint", rep.ID,
		"duration_ms", rep.Duration.Milliseconds(),
		"size_bytes", rep.InlineBytes)
}

// counterValue reads the current value of a Prometheus counter without
// scraping the whole registry. Counters are cumulative and process-wide,
// which — with one job per process — is exactly this job's total.
func counterValue(c prometheus.Counter) int64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return 0
	}
	return int64(m.GetCounter().GetValue())
}
