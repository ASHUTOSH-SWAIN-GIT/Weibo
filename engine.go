package weibo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/pipeline"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/state"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// DefaultEdgeCapacity is the default buffer size of the bounded edges
// between execution stages. Override with WithBufferSize.
const DefaultEdgeCapacity = 1024

// StreamExecutionEnv is the entry point for building and running stream pipelines.
// Create one with NewEnv(), define your pipeline using FromSource/ToSink,
// then call Execute() to run it.
//
//	env := weibo.NewEnv()
//	env.FromSource(src).Map(fn).Filter(fn).ToSink(stdout)
//	env.Execute(ctx)
type StreamExecutionEnv struct {
	source    source.Source
	sink      sink.Sink
	operators []operator.Operator

	checkpointInterval time.Duration
	checkpointStorage  checkpoint.Storage

	shutdownTimeout time.Duration
	edgeCapacity    int

	workerOps []operator.Operator
	workerMu  sync.Mutex

	// pendingOffsets maps a checkpoint ID to the barrier-aligned
	// source offsets captured when that barrier was injected. Aligned
	// by construction: the map is built from records that actually
	// passed the injector, so channel buffering between the source
	// and the injector cannot desynchronize it (unlike reading live
	// reader stats at save time).
	pendingOffsets map[string][]byte
	offsetsMu      sync.Mutex

	// coord drives the two-phase checkpoint protocol when the sink is
	// a CheckpointedSink (exactly-once mode). Nil otherwise.
	coord *checkpoint.Coordinator

	// barrierSnaps holds operator state captured synchronously as
	// barriers pass through stateful operators (race-free snapshot
	// point), keyed by checkpoint ID then checkpoint-data key.
	barrierSnaps map[string]map[string][]byte
	snapsMu      sync.Mutex

	// checkpointHook is a test-only seam forwarded to the coordinator
	// to halt or fail the protocol at exact steps.
	checkpointHook func(checkpoint.Step, string) checkpoint.HookAction

	// checkpointListener, if set, is called with the checkpoint ID each
	// time a checkpoint completes — in both the uncoordinated
	// (at-least-once) and coordinated (exactly-once) paths. Progress
	// signal for supervisors (see the jobagent package); must not block
	// and must not mutate pipeline state.
	checkpointListener func(id string)

	// checkpointObserver, if set, is called with a full CheckpointReport
	// (duration + inline size) on the same completions. Prefer it over
	// checkpointListener for new supervisors.
	checkpointObserver func(CheckpointReport)

	// checkpointMeta tracks per-checkpoint diagnostics: barrier-injection
	// time (for duration) and inline snapshot bytes (for size), keyed by
	// checkpoint ID and consumed on completion. Bounded like barrierSnaps.
	checkpointMeta   map[string]checkpointMeta
	checkpointMetaMu sync.Mutex

	// stateFactory creates per-owner state backends for stateful
	// operators (WithStateBackend). Nil = operators keep their
	// self-created in-memory backends.
	stateFactory state.BackendFactory

	// stateClosers are factory-created backends that need closing
	// when the run finishes (e.g. on-disk backends).
	stateClosers []io.Closer

	// planGraph is the executed stage topology, captured once BuildPlan
	// runs in Execute. Exposed via PlanJSON for the dashboard DAG; nil
	// until the job starts running.
	planGraph *pipeline.PlanGraph
	planMu    sync.Mutex

	// logger receives structured engine logs (checkpoints, recovery,
	// state lifecycle). Nil uses a component-tagged slog default.
	logger *slog.Logger
	// tracer starts coarse spans (recovery, checkpoint saves). Nil
	// behaves as a no-op tracer.
	tracer trace.Tracer
}

// NewEnv creates a new StreamExecutionEnv.
func NewEnv() *StreamExecutionEnv {
	return &StreamExecutionEnv{
		shutdownTimeout: 30 * time.Second,
		edgeCapacity:    DefaultEdgeCapacity,
	}
}

// WithShutdownTimeout sets how long the pipeline waits for in-flight
// records to drain before forcing shutdown (default 30s).
func (env *StreamExecutionEnv) WithShutdownTimeout(d time.Duration) *StreamExecutionEnv {
	env.shutdownTimeout = d
	return env
}

// WithBufferSize sets the capacity of the bounded edges between
// execution stages (default 1024). Larger buffers absorb bursts;
// smaller buffers propagate backpressure to the source sooner.
// Values < 1 are ignored.
func (env *StreamExecutionEnv) WithBufferSize(n int) *StreamExecutionEnv {
	if n > 0 {
		env.edgeCapacity = n
	}
	return env
}

// WithLogger sets the structured logger for engine operations
// (checkpoints, recovery, state lifecycle). Nil restores the default.
func (env *StreamExecutionEnv) WithLogger(l *slog.Logger) *StreamExecutionEnv {
	env.logger = l
	return env
}

// WithTracer sets the tracer for coarse engine spans (recovery,
// checkpoint saves). Nil restores the no-op tracer.
func (env *StreamExecutionEnv) WithTracer(t trace.Tracer) *StreamExecutionEnv {
	env.tracer = t
	return env
}

// log returns the engine logger, defaulting to a component-tagged slog
// default so unconfigured pipelines keep working unchanged.
func (env *StreamExecutionEnv) log() *slog.Logger {
	if env.logger != nil {
		return env.logger
	}
	return slog.Default().With("component", "engine")
}

// tracing returns the engine tracer, defaulting to no-op.
func (env *StreamExecutionEnv) tracing() trace.Tracer {
	if env.tracer != nil {
		return env.tracer
	}
	return trace.Noop()
}

// WithCheckpointing enables periodic checkpointing with the given interval
// and storage backend. Barriers are injected into the stream at the specified
// interval; when a barrier passes through all operators and reaches the sink,
// the checkpoint is complete.
//
// On recovery, Execute() will load the latest checkpoint, restore all stateful
// operators, and resume from the saved source offset.
//
// Example:
//
//	env := weibo.NewEnv()
//	env.WithCheckpointing(30*time.Second, checkpoint.NewFileStorage("/tmp/checkpoints"))
func (env *StreamExecutionEnv) WithCheckpointing(interval time.Duration, storage checkpoint.Storage) *StreamExecutionEnv {
	env.checkpointInterval = interval
	env.checkpointStorage = storage
	return env
}

// WithStateBackend sets the factory used to create state backends for
// stateful operators (Reduce, and any operator implementing
// operator.StateConfigurable). The factory is called once per state
// owner while the execution plan is built — "op-<i>" for top-level
// operators, "worker-<idx>" for keyed-worker clones — so every owner
// gets isolated state. Backends implementing io.Closer are closed
// when Execute returns.
//
// Default: state.InMemory() semantics (each operator keeps its own
// in-memory backend).
func (env *StreamExecutionEnv) WithStateBackend(f state.BackendFactory) *StreamExecutionEnv {
	env.stateFactory = f
	return env
}

// WithCheckpointHook installs a test-only hook fired after each
// coordinated checkpoint protocol step. Used by crash-window tests to
// halt or fail the coordinator at exact positions. Not for production.
func (env *StreamExecutionEnv) WithCheckpointHook(fn func(checkpoint.Step, string) checkpoint.HookAction) *StreamExecutionEnv {
	env.checkpointHook = fn
	return env
}

// CheckpointReport describes one completed checkpoint for diagnostics:
// how long barrier injection → completion took, and how many inline
// snapshot bytes the checkpoint carries. InlineBytes counts serialized
// operator + source payloads only — native state directories (Pebble
// hard-links) and sink transaction payloads are excluded, so it is exact
// for in-memory state and a lower bound otherwise.
type CheckpointReport struct {
	ID          string
	Duration    time.Duration
	InlineBytes int64
}

// checkpointMeta is the per-checkpoint diagnostic state behind a
// CheckpointReport: when the barrier was injected, and the inline
// snapshot bytes assembled at completion.
type checkpointMeta struct {
	startedAt   time.Time
	inlineBytes int64
	haveBytes   bool
}

// WithCheckpointListener registers an observer called with the
// checkpoint ID whenever a checkpoint completes — covering both the
// uncoordinated (at-least-once) and coordinated (exactly-once) paths.
// It is a progress signal for supervisors such as the job agent; the
// callback must not block and must not mutate pipeline state.
func (env *StreamExecutionEnv) WithCheckpointListener(fn func(id string)) *StreamExecutionEnv {
	env.checkpointListener = fn
	return env
}

// WithCheckpointObserver registers an observer called with a full
// CheckpointReport whenever a checkpoint completes (same coverage as
// WithCheckpointListener, plus duration and inline size). Prefer it for
// new supervisors; the callback must not block and must not mutate
// pipeline state.
func (env *StreamExecutionEnv) WithCheckpointObserver(fn func(CheckpointReport)) *StreamExecutionEnv {
	env.checkpointObserver = fn
	return env
}

// noteCheckpointStart records a barrier-injection timestamp for a
// checkpoint ID. Called on every injection path (periodic, shutdown,
// end-of-stream).
func (env *StreamExecutionEnv) noteCheckpointStart(id string) {
	env.checkpointMetaMu.Lock()
	defer env.checkpointMetaMu.Unlock()
	if env.checkpointMeta == nil {
		env.checkpointMeta = make(map[string]checkpointMeta)
	}
	if len(env.checkpointMeta) > 16 {
		for stale := range env.checkpointMeta {
			if stale != id {
				delete(env.checkpointMeta, stale)
				break
			}
		}
	}
	m := env.checkpointMeta[id]
	m.startedAt = time.Now()
	env.checkpointMeta[id] = m
}

// addCheckpointBytes accumulates inline snapshot bytes for a checkpoint
// ID: source offset payloads (at injection) plus operator snapshots (at
// completion). Both completion paths contribute, so the total covers the
// full inline payload.
func (env *StreamExecutionEnv) addCheckpointBytes(id string, n int64) {
	env.checkpointMetaMu.Lock()
	defer env.checkpointMetaMu.Unlock()
	if env.checkpointMeta == nil {
		env.checkpointMeta = make(map[string]checkpointMeta)
	}
	m := env.checkpointMeta[id]
	m.inlineBytes += n
	m.haveBytes = true
	env.checkpointMeta[id] = m
}

// snapBytes totals the payload bytes of one operator-snapshot map.
func snapBytes(snaps map[string][]byte) int64 {
	var total int64
	for _, b := range snaps {
		total += int64(len(b))
	}
	return total
}

// notifyCheckpoint fires the optional checkpoint observers. A no-op when
// none is registered.
func (env *StreamExecutionEnv) notifyCheckpoint(id string) {
	if env.checkpointObserver != nil || env.checkpointListener != nil {
		report := CheckpointReport{ID: id}
		env.checkpointMetaMu.Lock()
		if m, ok := env.checkpointMeta[id]; ok {
			delete(env.checkpointMeta, id)
			if !m.startedAt.IsZero() {
				report.Duration = time.Since(m.startedAt)
			}
			if m.haveBytes {
				report.InlineBytes = m.inlineBytes
			}
		}
		env.checkpointMetaMu.Unlock()
		if env.checkpointObserver != nil {
			env.checkpointObserver(report)
		}
		if env.checkpointListener != nil {
			env.checkpointListener(id)
		}
		return
	}
	// No observers: still drop the metadata so it cannot accumulate.
	env.checkpointMetaMu.Lock()
	delete(env.checkpointMeta, id)
	env.checkpointMetaMu.Unlock()
}

// FromSource sets the data source for the pipeline and returns a Stream
// that you can chain operators on.
func (env *StreamExecutionEnv) FromSource(src source.Source) *Stream {
	env.source = src
	return &Stream{env: env}
}

// SourceOperationalState returns connector-specific, read-only live state for
// the job agent. Nil means the configured source does not expose it.
func (env *StreamExecutionEnv) SourceOperationalState() any {
	if p, ok := env.source.(source.OperationalStateProvider); ok {
		return p.OperationalState()
	}
	return nil
}

// Execute runs the pipeline. Operators are grouped into execution
// stages (see the pipeline package); stages run concurrently,
// connected by bounded edges. A full edge blocks the upstream stage —
// backpressure propagates stage by stage back to the source, so a slow
// sink throttles ingestion instead of growing memory.
//
// Graceful shutdown (on context cancellation, C3 two-phase):
//  1. The source stops producing and flushes pending offset commits.
//  2. A final checkpoint barrier is injected (if checkpointing is on).
//  3. Channel closes cascade downstream; every stage drains in-flight
//     records — nothing is dropped.
//  4. The sink drains and the final checkpoint is saved.
//  5. Only if draining exceeds shutdownTimeout are blocked stages
//     forcibly aborted.
//
// Prometheus metrics are collected automatically during execution.
func (env *StreamExecutionEnv) Execute(ctx context.Context) error {
	if env.source == nil {
		return fmt.Errorf("weibo: no source configured, use FromSource()")
	}
	if env.sink == nil {
		return fmt.Errorf("weibo: no sink configured, use ToSink()")
	}

	metrics.PipelineRunning.Set(1)
	defer metrics.PipelineRunning.Set(0)

	// Coordinated (exactly-once) mode is active when the sink stages
	// its output transactionally. It requires checkpointing and a
	// resumable source — refuse half-configured setups.
	sourceCaps := source.CapabilitiesOf(env.source)
	sinkCaps := sink.CapabilitiesOf(env.sink)
	coordinatedSink, coordinated := env.sink.(sink.CheckpointedSink)
	if sinkCaps.CoordinatedCheckpoints {
		if !coordinated {
			return fmt.Errorf("weibo: sink declares coordinated checkpoints but does not implement sink.CheckpointedSink")
		}
		if env.checkpointStorage == nil || env.checkpointInterval <= 0 {
			return fmt.Errorf("weibo: a CheckpointedSink requires WithCheckpointing(interval, storage)")
		}
		if !sourceCaps.CheckpointOffsets {
			return fmt.Errorf("weibo: exactly-once requires a source with CheckpointOffsets capability (source.CheckpointSource)")
		}
		if _, ok := env.source.(source.CheckpointSource); !ok {
			return fmt.Errorf("weibo: source declares CheckpointOffsets but does not implement source.CheckpointSource")
		}
	}

	// Phase A: restore the source offset before wiring, so the reader
	// knows where to resume.
	var savedCheckpoint *checkpoint.CheckpointData
	if env.checkpointStorage != nil {
		if coordinated {
			rctx, rspan := env.tracing().Start(ctx, "checkpoint.recovery")
			data, err := env.resolveCoordinatedRecovery(rctx, coordinatedSink)
			if err != nil {
				rspan.RecordError(err)
				rspan.End()
				// Guessing here risks duplicates or loss — refuse to start.
				return fmt.Errorf("weibo: recovery: %w", err)
			}
			rspan.End()
			if data != nil {
				env.restoreSourceOffset(data)
				savedCheckpoint = data
			}
		} else {
			data, err := env.checkpointStorage.Load()
			if err != nil {
				env.log().Warn("checkpoint load failed, starting fresh", "error", err)
			} else if data != nil {
				env.restoreSourceOffset(data)
				savedCheckpoint = data
			}
		}
	}

	// Per-owner state backends: wrap the factory so every backend it
	// creates is tracked for closing when the run ends.
	env.stateClosers = nil
	var backendFor func(ownerID string) (state.StateBackend, error)
	if env.stateFactory != nil {
		backendFor = func(ownerID string) (state.StateBackend, error) {
			b, err := env.stateFactory(ownerID)
			if err != nil {
				return nil, err
			}
			if c, ok := b.(io.Closer); ok {
				env.stateClosers = append(env.stateClosers, c)
			}
			return b, nil
		}
	}
	defer func() {
		for _, c := range env.stateClosers {
			if cerr := c.Close(); cerr != nil {
				env.log().Warn("closing state backend", "error", cerr)
			}
		}
	}()

	plan, err := pipeline.BuildPlan(pipeline.PlanConfig{
		Source:       env.source,
		Operators:    env.operators,
		Labels:       env.operatorLabels(),
		Sink:         env.sink,
		DrainTimeout: env.shutdownTimeout,
		StageHooks: pipeline.StageHooks{
			OnClone: func(op operator.Operator) int {
				env.workerMu.Lock()
				defer env.workerMu.Unlock()
				env.workerOps = append(env.workerOps, op)
				return len(env.workerOps) - 1
			},
			OnSnapshot:      env.addBarrierSnapshot,
			StateBackendFor: backendFor,
			NativeStateDir:  env.nativeStateDir(),
		},
	})
	if err != nil {
		return err
	}

	// Capture the executed stage topology for the dashboard DAG. Names
	// line up with the weibo_stage_*/weibo_edge_queue metric labels so
	// the UI can overlay live throughput and backpressure per node/edge.
	pg := pipeline.DescribePlan(plan)
	env.planMu.Lock()
	env.planGraph = &pg
	env.planMu.Unlock()

	// Phase B: restore per-worker operator state. Keyed stages clone
	// their operators at plan time, so every clone exists before any
	// stage starts processing.
	if savedCheckpoint != nil {
		if err := env.restoreWorkersFromCheckpoint(savedCheckpoint); err != nil {
			return fmt.Errorf("weibo: restore operator state: %w", err)
		}
	} else {
		if err := env.resetWorkingState(); err != nil {
			return fmt.Errorf("weibo: reset working state: %w", err)
		}
	}

	// Coordinator lifecycle (exactly-once mode only).
	var coordErrCh chan error
	if coordinated {
		txnID := ""
		if t, ok := env.sink.(interface{ TransactionalID() string }); ok {
			txnID = t.TransactionalID()
		}
		env.coord = checkpoint.NewCoordinator(env.checkpointStorage, txnID)
		env.coord.Logger = env.log()
		env.coord.Tracer = env.tracing()
		env.coord.CommitSink = coordinatedSink.Commit
		env.coord.AbortSink = coordinatedSink.Abort
		env.coord.Hook = env.checkpointHook
		env.coord.OnCompleted = env.notifyCheckpoint
		if oc, ok := env.source.(source.OffsetCommitter); ok {
			env.coord.CommitOffsets = oc.CommitOffsets
		}
		coordinatedSink.SetOnPrepared(env.coord.OnSinkPrepared)
		coordErrCh = make(chan error, 1)
	}

	// Two-phase shutdown (C3). Cancelling ctx only stops the source;
	// the pipeline drains through cascading channel closes. hardCtx is
	// what unblocks stuck sends — it fires shutdownTimeout after ctx
	// is cancelled, or immediately on a fatal stage error.
	hardCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()
	pipelineDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			t := time.NewTimer(env.shutdownTimeout)
			defer t.Stop()
			select {
			case <-t.C:
				hardCancel()
			case <-pipelineDone:
			}
		case <-pipelineDone:
		}
	}()

	if coordinated {
		env.coord.Start(hardCtx)
		defer env.coord.Stop()
		// A coordination failure (persist error, sink commit error,
		// injected test failure) is pipeline-fatal: capture it and
		// force-unwind.
		go func() {
			select {
			case err := <-env.coord.Fatal():
				coordErrCh <- err
				hardCancel()
			case <-pipelineDone:
			}
		}()
	}

	nStages := len(plan)
	edges := make([]*pipeline.Edge, nStages-1)
	for i := range edges {
		edges[i] = pipeline.NewEdge(fmt.Sprintf("edge-%d", i), env.edgeCapacity)
	}
	// Publish per-edge queue gauges: an edge pinned at capacity means
	// the stage downstream of it is the bottleneck.
	pipeline.SampleEdges(pipelineDone, edges)

	var wg sync.WaitGroup
	errs := make([]error, nStages)
	for i, stage := range plan {
		var in <-chan types.Record
		if i > 0 {
			in = edges[i-1].Ch
		}
		// Checkpoint barriers enter the stream right after the source
		// stage and are detected right before the sink stage, when
		// every operator upstream has forwarded them.
		if i == 1 && env.checkpointInterval > 0 {
			in = env.injectBarriers(ctx, hardCtx, in)
		}
		if i == nStages-1 {
			if coordinated {
				// Coordinated mode: register the state snapshot with
				// the coordinator BEFORE the barrier reaches the sink
				// — the sink's prepare ack may trigger finalization
				// immediately, and the snapshot must already be there.
				in = barrierDetect(hardCtx, in, func(id string) {
					snaps, dirs := env.collectSnapshots(id)
					env.coord.OnStateSnapshot(id, snaps, dirs)
				})
			} else if env.checkpointInterval > 0 {
				// Uncoordinated mode: persist only after the sink has
				// drained everything ahead of the barrier (SinkStage
				// waits for its handoff buffer to empty) — offsets
				// must never be durable for records the sink never
				// received.
				if ss, ok := stage.(*pipeline.SinkStage); ok {
					ss.OnBarrier = env.saveCheckpoint
				}
			}
		}
		var out chan<- types.Record
		if i < len(edges) {
			out = edges[i].Ch
		}

		wg.Add(1)
		go func(i int, st pipeline.Stage, in <-chan types.Record, out chan<- types.Record) {
			defer wg.Done()
			if err := st.Run(ctx, hardCtx, in, out); err != nil {
				errs[i] = err
				metrics.StageErrorsTotal.WithLabelValues(st.Name()).Inc()
				hardCancel() // fatal stage error: unwind the whole pipeline
			}
		}(i, stage, in, out)
	}
	wg.Wait()
	close(pipelineDone)

	if coordinated {
		env.coord.Stop() // idempotent; waits for in-flight finalization
		var coordErr error
		select {
		case coordErr = <-coordErrCh: // relayed by the watcher
		default:
			select {
			case coordErr = <-env.coord.Fatal(): // watcher hadn't relayed yet
			default:
			}
		}
		if coordErr != nil && !(errors.Is(coordErr, context.Canceled) && ctx.Err() != nil) {
			return coordErr
		}
	}

	for _, err := range errs {
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		return err
	}
	return nil
}

// operatorLabels returns a label string for each operator in the chain.
// Uses the user-provided label if set, otherwise the operator type name.
func (env *StreamExecutionEnv) operatorLabels() []string {
	labels := make([]string, len(env.operators))
	for i, op := range env.operators {
		if lab, ok := op.(operator.Labeled); ok && lab.GetLabel() != "" {
			labels[i] = lab.GetLabel()
		} else {
			labels[i] = op.Name()
		}
	}
	return labels
}

// barrierDetect wraps a read channel and calls saveCheckpoint
// whenever a barrier record passes through.  Barriers reach this
// wrapper only after every operator upstream has forwarded them,
// so operator state snapshots capture the correct point-in-time
// state (post-barrier Chandy-Lamport alignment).
func barrierDetect(hardCtx context.Context, in <-chan types.Record, save func(id string)) <-chan types.Record {
	out := make(chan types.Record, 256)
	go func() {
		defer close(out)
		for r := range in {
			// Register state BEFORE forwarding — once the barrier
			// reaches the sink, the coordinator may finalize, and the
			// snapshot must already be there.
			if r.IsBarrier {
				save(r.CheckpointID)
			}
			select {
			case out <- r:
			case <-hardCtx.Done():
				return // forced shutdown: downstream is gone
			}
		}
	}()
	return out
}

// injectBarriers wraps a source channel and periodically injects checkpoint
// barriers into the stream. When a barrier reaches the end of the pipeline,
// all stateful operators snapshot their state and the checkpoint is saved.
func (env *StreamExecutionEnv) injectBarriers(ctx, hardCtx context.Context, sourceCh <-chan types.Record) <-chan types.Record {
	out := make(chan types.Record, 256)
	go func() {
		defer close(out)

		ticker := time.NewTicker(env.checkpointInterval)
		defer ticker.Stop()

		// Run-unique ID prefix: checkpoint IDs must never collide
		// across restarts — the recovery marker probe matches on ID,
		// and a stale marker from a previous run must not "prove" a
		// different run's checkpoint committed.
		runNonce := time.Now().UnixNano()
		checkpointID := 0
		mkID := func(suffix string) string {
			return fmt.Sprintf("cp-%d-%d%s", runNonce, checkpointID, suffix)
		}

		// Barrier-aligned offset tracking: every data record that
		// passes this point advances its partition's position. A
		// barrier injected here is therefore preceded by exactly the
		// records reflected in the map — the alignment invariant.
		offsets := make(map[source.PositionKey]int64)
		positionKey := func(record types.Record) source.PositionKey {
			if positioned, ok := env.source.(source.PositionedCheckpointSource); ok {
				return positioned.CheckpointPosition(record)
			}
			return source.PositionKey{Partition: record.Partition}
		}

		// forward blocks until downstream accepts r; it gives up only
		// on hardCtx so this goroutine can't leak when the pipeline is
		// forcibly unwound with full edges.
		forward := func(r types.Record) bool {
			select {
			case out <- r:
				return true
			case <-hardCtx.Done():
				return false
			}
		}

		// barrier snapshots the aligned offsets under the new
		// checkpoint ID, then injects the barrier record.
		barrier := func(id string) bool {
			env.registerAlignedOffsets(id, offsets)
			return forward(types.NewBarrier(id))
		}

		for {
			select {
			case <-ctx.Done():
				// Inject a final checkpoint barrier before draining so
				// state is saved on graceful shutdown.
				checkpointID++
				if !barrier(mkID("-shutdown")) {
					return
				}
				for record := range sourceCh {
					if !record.IsWatermark && !record.IsBarrier {
						offsets[positionKey(record)] = record.Offset + 1
					}
					if !forward(record) {
						return
					}
				}
				return

			case record, ok := <-sourceCh:
				if !ok {
					// End of stream: one final barrier so every record
					// is covered by a checkpoint (and, in coordinated
					// mode, committed by the sink transaction).
					checkpointID++
					barrier(mkID("-final"))
					return
				}
				if !record.IsWatermark && !record.IsBarrier {
					offsets[positionKey(record)] = record.Offset + 1
				}
				if !forward(record) {
					return
				}

			case <-ticker.C:
				checkpointID++
				id := mkID("")

				// Inject barrier into the stream. The barrier flows
				// through all operators. When it reaches the end of the
				// operator chain, saveCheckpoint is triggered (see
				// barrierDetect). This ensures operator state is
				// captured AFTER all pre-barrier records are processed.
				if !barrier(id) {
					return
				}
			}
		}
	}()

	return out
}

// registerAlignedOffsets stores a JSON snapshot of the injector's
// aligned offset map under the given checkpoint ID, using the shared versioned
// source-position format.
func (env *StreamExecutionEnv) registerAlignedOffsets(id string, offsets map[source.PositionKey]int64) {
	// Every injection funnels through here (periodic, shutdown,
	// end-of-stream), so it is also the duration start point.
	env.noteCheckpointStart(id)
	var data []byte
	var err error
	if _, topicAware := env.source.(source.PositionedCheckpointSource); topicAware {
		positions := make([]source.Position, 0, len(offsets))
		for key, off := range offsets {
			positions = append(positions, source.Position{Source: key.Source, Partition: key.Partition, Offset: off})
		}
		data, err = source.EncodePositions(positions)
	} else {
		legacy := make(map[string]int64, len(offsets))
		for key, off := range offsets {
			legacy[strconv.Itoa(key.Partition)] = off
		}
		data, err = json.Marshal(legacy)
	}
	if err != nil {
		return
	}
	env.addCheckpointBytes(id, int64(len(data)))
	// Coordinated mode: the coordinator owns pending checkpoints.
	if env.coord != nil {
		env.coord.OnBarrierInjected(id, data)
		return
	}
	env.offsetsMu.Lock()
	if env.pendingOffsets == nil {
		env.pendingOffsets = make(map[string][]byte)
	}
	env.pendingOffsets[id] = data
	env.offsetsMu.Unlock()
}

// takeAlignedOffsets returns and removes the aligned offsets captured
// at barrier injection for the given checkpoint ID.
func (env *StreamExecutionEnv) takeAlignedOffsets(id string) ([]byte, bool) {
	env.offsetsMu.Lock()
	defer env.offsetsMu.Unlock()
	data, ok := env.pendingOffsets[id]
	if ok {
		delete(env.pendingOffsets, id)
	}
	return data, ok
}

// nativeStateDir returns the hook Checkpointable backends use to
// place barrier-time native checkpoints, or nil when checkpointing is
// off. The directory root is created eagerly so the backend's
// hard-link call only has to create its own leaf dir.
func (env *StreamExecutionEnv) nativeStateDir() func(checkpointID, ownerID string) string {
	if env.checkpointStorage == nil || env.checkpointInterval <= 0 {
		return nil
	}
	return func(checkpointID, ownerID string) string {
		root := env.checkpointStorage.StateDir(checkpointID)
		if err := os.MkdirAll(root, 0o755); err != nil {
			env.log().Warn("create state dir", "dir", root, "checkpoint", checkpointID, "error", err)
		}
		return filepath.Join(root, ownerID)
	}
}

// addBarrierSnapshot stores state captured when a barrier passed
// through a stateful operator, until the barrier reaches the end of
// the pipeline and the checkpoint is assembled.
func (env *StreamExecutionEnv) addBarrierSnapshot(checkpointID, key string, snapshot []byte) {
	env.snapsMu.Lock()
	defer env.snapsMu.Unlock()
	if env.barrierSnaps == nil {
		env.barrierSnaps = make(map[string]map[string][]byte)
	}
	if env.barrierSnaps[checkpointID] == nil {
		// Bound the map: a checkpoint whose barrier never reaches the
		// end of the pipeline would otherwise leak its snapshots.
		if len(env.barrierSnaps) > 8 {
			for stale := range env.barrierSnaps {
				if stale != checkpointID {
					delete(env.barrierSnaps, stale)
					break
				}
			}
		}
		env.barrierSnaps[checkpointID] = make(map[string][]byte)
	}
	env.barrierSnaps[checkpointID][key] = snapshot
}

// collectSnapshots assembles the state for one checkpoint. Stateful
// operators that implement BarrierSnapshotter delivered their state
// when the barrier passed through them (race-free); anything else is
// snapshotted here as a legacy fallback.
func (env *StreamExecutionEnv) collectSnapshots(checkpointID string) (map[string][]byte, map[string]string) {
	env.snapsMu.Lock()
	snaps := env.barrierSnaps[checkpointID]
	delete(env.barrierSnaps, checkpointID)
	env.snapsMu.Unlock()
	if snaps == nil {
		snaps = make(map[string][]byte)
	}

	stateRoot := env.checkpointStorage.StateDir(checkpointID)
	for i, op := range env.operators {
		key := fmt.Sprintf("op-%d", i)
		if _, done := snaps[key]; done {
			continue
		}
		snaps[key] = env.snapshotOrCheckpoint(stateRoot, key, op)
	}

	env.workerMu.Lock()
	for i, op := range env.workerOps {
		key := fmt.Sprintf("worker-%d", i)
		if _, done := snaps[key]; done {
			continue
		}
		snaps[key] = env.snapshotOrCheckpoint(stateRoot, key, op)
	}
	env.workerMu.Unlock()

	dirs := extractStateDirs(snaps)
	env.addCheckpointBytes(checkpointID, snapBytes(snaps))
	return snaps, dirs
}

// extractStateDirs reads state-ref markers from snapshot entries and
// returns the StateDirs map. Entries containing a state_ref are
// replaced with nil so the operator processor ignores them.
func extractStateDirs(snaps map[string][]byte) map[string]string {
	dirs := make(map[string]string)
	for key, val := range snaps {
		var ref struct {
			StateRef string `json:"state_ref"`
		}
		if json.Unmarshal(val, &ref) == nil && ref.StateRef != "" {
			dirs[ref.StateRef] = key
			snaps[key] = nil // signal: op has no inline state
		}
	}
	if len(dirs) == 0 {
		return nil
	}
	return dirs
}

// snapshotOrCheckpoint returns serialized state bytes for an operator,
// or a state-ref marker if the backend supports native checkpointing
// (Pebble hard-links).
func (env *StreamExecutionEnv) snapshotOrCheckpoint(stateRoot, ownerID string, op operator.Operator) []byte {
	// Check for native checkpointable backend.
	if rop, ok := op.(*operator.ReduceOperator); ok {
		if cp, ok := rop.Backend().(state.Checkpointable); ok {
			ownerDir := filepath.Join(stateRoot, ownerID)
			if err := cp.CheckpointTo(ownerDir); err != nil {
				env.log().Warn("checkpoint native snapshot failed", "owner", ownerID, "error", err)
				return nil
			}
			ref, _ := json.Marshal(map[string]string{"state_ref": ownerID})
			return ref
		}
	}

	if snap, ok := op.(operator.Snapshotable); ok {
		snapshot, err := snap.Snapshot()
		if err != nil {
			env.log().Warn("checkpoint snapshot failed", "owner", ownerID, "error", err)
			return nil
		}
		return snapshot
	}
	return nil
}

// resolveCoordinatedRecovery implements the exactly-once recovery
// decision table:
//
//	latest completed              → restore from it
//	latest prepared, txn committed → promote to completed, restore
//	latest prepared, txn absent    → abort txn, restore previous completed
//
// The "did the transaction commit" question is answered by the sink's
// marker probe (WasCommitted) — the output of an uncommitted
// transaction was never visible, so falling back and replaying it
// cannot duplicate.
func (env *StreamExecutionEnv) resolveCoordinatedRecovery(ctx context.Context, cs sink.CheckpointedSink) (*checkpoint.CheckpointData, error) {
	latest, err := env.checkpointStorage.Load()
	if err != nil {
		return nil, fmt.Errorf("load latest checkpoint: %w", err)
	}
	if latest == nil {
		return nil, nil // fresh start
	}
	if latest.Completed() {
		return latest, nil
	}

	committed, err := cs.WasCommitted(ctx, latest.ID)
	if err != nil {
		return nil, fmt.Errorf("cannot resolve prepared checkpoint %s: %w", latest.ID, err)
	}
	if committed {
		if err := env.checkpointStorage.UpdateStatus(latest.ID, checkpoint.StatusCompleted); err != nil {
			return nil, fmt.Errorf("promote checkpoint %s: %w", latest.ID, err)
		}
		env.log().Info("recovery: prepared transaction had committed, promoted to completed", "checkpoint", latest.ID)
		latest.Status = checkpoint.StatusCompleted
		return latest, nil
	}

	// Never committed: its output was never visible. Abort (best
	// effort — producer fencing handles it too) and fall back.
	if err := cs.Abort(ctx, latest.ID); err != nil {
		env.log().Warn("recovery: abort dangling transaction", "checkpoint", latest.ID, "error", err)
	}
	env.log().Info("recovery: discarding uncommitted checkpoint", "checkpoint", latest.ID)
	return env.checkpointStorage.LoadLatestCompleted()
}

// saveCheckpoint captures a snapshot from all stateful operators
// and writes it to the checkpoint storage (uncoordinated path).
func (env *StreamExecutionEnv) saveCheckpoint(id string) {
	_, span := env.tracing().Start(context.Background(), "checkpoint.save",
		trace.String("checkpoint", id))
	defer span.End()
	snaps, dirs := env.collectSnapshots(id)
	data := &checkpoint.CheckpointData{
		ID:        id,
		Timestamp: time.Now().UTC(),
		Operators: snaps,
		Source:    make(map[string][]byte),
		Status:    checkpoint.StatusCompleted,
		StateDirs: dirs,
	}

	if _, ok := env.source.(source.CheckpointSource); ok {
		// Prefer the barrier-aligned offsets captured at injection;
		// fall back to the source's live position only if the barrier
		// predates offset tracking (shouldn't happen in practice).
		if aligned, ok := env.takeAlignedOffsets(id); ok {
			data.Source["offset"] = aligned
		} else if cps, ok := env.source.(source.CheckpointSource); ok {
			offset, err := cps.CheckpointOffset()
			if err != nil {
				env.log().Warn("checkpoint source offset failed", "checkpoint", id, "error", err)
			} else {
				data.Source["offset"] = offset
			}
		}
	}

	if err := env.checkpointStorage.Save(data); err != nil {
		span.RecordError(err)
		env.log().Error("checkpoint save failed", "checkpoint", id, "error", err)
		return
	}
	env.log().Debug("checkpoint saved", "checkpoint", id, "owners", len(snaps))
	env.notifyCheckpoint(id)
}

// restoreSourceOffset restores the source offset from a checkpoint.
// Called before wiring so the Kafka reader knows where to resume.
func (env *StreamExecutionEnv) restoreSourceOffset(data *checkpoint.CheckpointData) {
	if data == nil {
		return
	}
	if cps, ok := env.source.(source.CheckpointSource); ok {
		if offsetData, exists := data.Source["offset"]; exists {
			if err := cps.RestoreOffset(offsetData); err != nil {
				env.log().Warn("restore source offset failed", "checkpoint", data.ID, "error", err)
			}
		}
	}
	env.log().Info("restored from checkpoint", "checkpoint", data.ID)
}

// restoreWorkersFromCheckpoint restores per-worker operator state for
// operator instances created by keyed stages. Called after the plan is
// built (which creates the worker clones) and before stages start.
func (env *StreamExecutionEnv) restoreWorkersFromCheckpoint(data *checkpoint.CheckpointData) error {
	if data == nil {
		return nil
	}

	// Top-level operators are snapshotted under "op-<i>" in collectSnapshots
	// and must be restored symmetrically. For a non-keyed stateful pipeline
	// (a Window/Reduce used without KeyBy) these ARE the live operators, so
	// skipping them silently reset their state on every restart. For keyed
	// pipelines they are unused templates and the restore is a harmless
	// no-op (no matching snapshot / empty state).
	for i, op := range env.operators {
		if err := env.restoreOperatorState(data, fmt.Sprintf("op-%d", i), op); err != nil {
			return err
		}
	}

	env.workerMu.Lock()
	defer env.workerMu.Unlock()
	for i, op := range env.workerOps {
		if err := env.restoreOperatorState(data, fmt.Sprintf("worker-%d", i), op); err != nil {
			return err
		}
	}
	return nil
}

// restoreOperatorState restores a single operator's state from a checkpoint
// under the given owner key, using native (Pebble hard-link) restore when
// available and falling back to inline snapshot bytes otherwise.
func (env *StreamExecutionEnv) restoreOperatorState(data *checkpoint.CheckpointData, key string, op operator.Operator) error {
	// Native checkpoint restore (Pebble hard-links). Any operator that
	// exposes a checkpointable backend uses this path — not only Reduce.
	// A Pebble-backed Window/WindowReduce snapshots natively (a state_ref
	// in StateDirs), so restricting native restore to *ReduceOperator would
	// drop it to the inline path, find no inline bytes, and lose its
	// buffered windows and watermark.
	if stateDir, ok := data.StateDirs[key]; ok {
		if b, ok := op.(interface {
			Backend() state.StateBackend
		}); ok {
			if cp, ok := b.Backend().(state.Checkpointable); ok {
				absPath := filepath.Join(env.checkpointStorage.StateDir(data.ID), stateDir)
				if err := cp.RestoreFrom(absPath); err != nil {
					return fmt.Errorf("restore %s from native state: %w", key, err)
				}
				return nil
			}
		}
	}

	// Inline state restore (memory / compatible Pebble).
	if snap, ok := op.(operator.Snapshotable); ok {
		if stateData, exists := data.Operators[key]; exists && len(stateData) > 0 {
			if err := snap.Restore(stateData); err != nil {
				return fmt.Errorf("restore %s: %w", key, err)
			}
			return nil
		}
	}
	return env.resetOperatorState(key, op)
}

func (env *StreamExecutionEnv) resetWorkingState() error {
	for i, op := range env.operators {
		if err := env.resetOperatorState(fmt.Sprintf("op-%d", i), op); err != nil {
			return err
		}
	}
	env.workerMu.Lock()
	defer env.workerMu.Unlock()
	for i, op := range env.workerOps {
		if err := env.resetOperatorState(fmt.Sprintf("worker-%d", i), op); err != nil {
			return err
		}
	}
	return nil
}

func (env *StreamExecutionEnv) resetOperatorState(key string, op operator.Operator) error {
	backendOwner, ok := op.(interface{ Backend() state.StateBackend })
	if !ok {
		return nil
	}
	reset, ok := backendOwner.Backend().(state.Resettable)
	if !ok {
		return nil
	}
	if err := reset.Reset(); err != nil {
		return fmt.Errorf("reset uncheckpointed state %s: %w", key, err)
	}
	return nil
}
