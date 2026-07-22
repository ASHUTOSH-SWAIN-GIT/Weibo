package mailer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/pipeline"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/state"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// DefaultEdgeCapacity is the default buffer size of the bounded edges
// between execution stages. Override with WithBufferSize.
const DefaultEdgeCapacity = 1024

// StreamExecutionEnv is the entry point for building and running stream pipelines.
// Create one with NewEnv(), define your pipeline using FromSource/ToSink,
// then call Execute() to run it.
//
//	env := mailer.NewEnv()
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

	// stateFactory creates per-owner state backends for stateful
	// operators (WithStateBackend). Nil = operators keep their
	// self-created in-memory backends.
	stateFactory state.BackendFactory

	// stateClosers are factory-created backends that need closing
	// when the run finishes (e.g. on-disk backends).
	stateClosers []io.Closer
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
//	env := mailer.NewEnv()
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

// WithCheckpointListener registers an observer called with the
// checkpoint ID whenever a checkpoint completes — covering both the
// uncoordinated (at-least-once) and coordinated (exactly-once) paths.
// It is a progress signal for supervisors such as the job agent; the
// callback must not block and must not mutate pipeline state.
func (env *StreamExecutionEnv) WithCheckpointListener(fn func(id string)) *StreamExecutionEnv {
	env.checkpointListener = fn
	return env
}

// notifyCheckpoint fires the optional checkpoint listener. A no-op when
// none is registered.
func (env *StreamExecutionEnv) notifyCheckpoint(id string) {
	if env.checkpointListener != nil {
		env.checkpointListener(id)
	}
}

// FromSource sets the data source for the pipeline and returns a Stream
// that you can chain operators on.
func (env *StreamExecutionEnv) FromSource(src source.Source) *Stream {
	env.source = src
	return &Stream{env: env}
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
		return fmt.Errorf("mailer: no source configured, use FromSource()")
	}
	if env.sink == nil {
		return fmt.Errorf("mailer: no sink configured, use ToSink()")
	}

	metrics.PipelineRunning.Set(1)
	defer metrics.PipelineRunning.Set(0)

	// Coordinated (exactly-once) mode is active when the sink stages
	// its output transactionally. It requires checkpointing and a
	// resumable source — refuse half-configured setups.
	coordinatedSink, coordinated := env.sink.(sink.CheckpointedSink)
	if coordinated {
		if env.checkpointStorage == nil || env.checkpointInterval <= 0 {
			return fmt.Errorf("mailer: a CheckpointedSink requires WithCheckpointing(interval, storage)")
		}
		if _, ok := env.source.(source.CheckpointSource); !ok {
			return fmt.Errorf("mailer: exactly-once requires a source that supports offset checkpointing (source.CheckpointSource)")
		}
	}

	// Phase A: restore the source offset before wiring, so the reader
	// knows where to resume.
	var savedCheckpoint *checkpoint.CheckpointData
	if env.checkpointStorage != nil {
		if coordinated {
			data, err := env.resolveCoordinatedRecovery(ctx, coordinatedSink)
			if err != nil {
				// Guessing here risks duplicates or loss — refuse to start.
				return fmt.Errorf("mailer: recovery: %w", err)
			}
			if data != nil {
				env.restoreSourceOffset(data)
				savedCheckpoint = data
			}
		} else {
			data, err := env.checkpointStorage.Load()
			if err != nil {
				fmt.Printf("mailer: checkpoint load failed (starting fresh): %v\n", err)
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
				fmt.Printf("mailer: closing state backend: %v\n", cerr)
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

	// Phase B: restore per-worker operator state. Keyed stages clone
	// their operators at plan time, so every clone exists before any
	// stage starts processing.
	if savedCheckpoint != nil {
		env.restoreWorkersFromCheckpoint(savedCheckpoint)
	}

	// Coordinator lifecycle (exactly-once mode only).
	var coordErrCh chan error
	if coordinated {
		txnID := ""
		if t, ok := env.sink.(interface{ TransactionalID() string }); ok {
			txnID = t.TransactionalID()
		}
		env.coord = checkpoint.NewCoordinator(env.checkpointStorage, txnID)
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

	for i, err := range errs {
		if err == nil || errors.Is(err, context.Canceled) {
			continue
		}
		if i == nStages-1 && ctx.Err() != nil {
			continue // sink error during graceful shutdown
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
		offsets := make(map[int]int64)

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
						offsets[record.Partition] = record.Offset + 1
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
					offsets[record.Partition] = record.Offset + 1
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
// aligned offset map under the given checkpoint ID, in the same
// {"partition": nextOffset} shape as source.CheckpointSource.
func (env *StreamExecutionEnv) registerAlignedOffsets(id string, offsets map[int]int64) {
	snapshot := make(map[string]int64, len(offsets))
	for p, off := range offsets {
		snapshot[strconv.Itoa(p)] = off
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return
	}
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
			fmt.Printf("mailer: create state dir %s: %v\n", root, err)
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
				fmt.Printf("mailer: checkpoint native snapshot %s failed: %v\n", ownerID, err)
				return nil
			}
			ref, _ := json.Marshal(map[string]string{"state_ref": ownerID})
			return ref
		}
	}

	if snap, ok := op.(operator.Snapshotable); ok {
		snapshot, err := snap.Snapshot()
		if err != nil {
			fmt.Printf("mailer: checkpoint snapshot failed for %s: %v\n", ownerID, err)
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
		fmt.Printf("mailer: recovery: checkpoint %s transaction had committed — promoted to completed\n", latest.ID)
		latest.Status = checkpoint.StatusCompleted
		return latest, nil
	}

	// Never committed: its output was never visible. Abort (best
	// effort — producer fencing handles it too) and fall back.
	if err := cs.Abort(ctx, latest.ID); err != nil {
		fmt.Printf("mailer: recovery: abort dangling transaction %s: %v\n", latest.ID, err)
	}
	fmt.Printf("mailer: recovery: discarding uncommitted checkpoint %s\n", latest.ID)
	return env.checkpointStorage.LoadLatestCompleted()
}

// saveCheckpoint captures a snapshot from all stateful operators
// and writes it to the checkpoint storage (uncoordinated path).
func (env *StreamExecutionEnv) saveCheckpoint(id string) {
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
				fmt.Printf("mailer: checkpoint source offset failed: %v\n", err)
			} else {
				data.Source["offset"] = offset
			}
		}
	}

	if err := env.checkpointStorage.Save(data); err != nil {
		fmt.Printf("mailer: checkpoint save failed: %v\n", err)
		return
	}
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
				fmt.Printf("mailer: restore source offset failed: %v\n", err)
			}
		}
	}
	fmt.Printf("mailer: restored from checkpoint %s\n", data.ID)
}

// restoreWorkersFromCheckpoint restores per-worker operator state for
// operator instances created by keyed stages. Called after the plan is
// built (which creates the worker clones) and before stages start.
func (env *StreamExecutionEnv) restoreWorkersFromCheckpoint(data *checkpoint.CheckpointData) {
	if data == nil {
		return
	}

	// Top-level operators are snapshotted under "op-<i>" in collectSnapshots
	// and must be restored symmetrically. For a non-keyed stateful pipeline
	// (a Window/Reduce used without KeyBy) these ARE the live operators, so
	// skipping them silently reset their state on every restart. For keyed
	// pipelines they are unused templates and the restore is a harmless
	// no-op (no matching snapshot / empty state).
	for i, op := range env.operators {
		env.restoreOperatorState(data, fmt.Sprintf("op-%d", i), op)
	}

	env.workerMu.Lock()
	defer env.workerMu.Unlock()
	for i, op := range env.workerOps {
		env.restoreOperatorState(data, fmt.Sprintf("worker-%d", i), op)
	}
}

// restoreOperatorState restores a single operator's state from a checkpoint
// under the given owner key, using native (Pebble hard-link) restore when
// available and falling back to inline snapshot bytes otherwise.
func (env *StreamExecutionEnv) restoreOperatorState(data *checkpoint.CheckpointData, key string, op operator.Operator) {
	// Native checkpoint restore (Pebble hard-links).
	if stateDir, ok := data.StateDirs[key]; ok {
		if rop, ok := op.(*operator.ReduceOperator); ok {
			if cp, ok := rop.Backend().(state.Checkpointable); ok {
				absPath := filepath.Join(env.checkpointStorage.StateDir(data.ID), stateDir)
				if err := cp.RestoreFrom(absPath); err != nil {
					fmt.Printf("mailer: restore %s from native state failed: %v\n", key, err)
				}
				return
			}
		}
	}

	// Inline state restore (memory / compatible Pebble).
	if snap, ok := op.(operator.Snapshotable); ok {
		if stateData, exists := data.Operators[key]; exists && len(stateData) > 0 {
			if err := snap.Restore(stateData); err != nil {
				fmt.Printf("mailer: restore %s failed: %v\n", key, err)
			}
		}
	}
}
