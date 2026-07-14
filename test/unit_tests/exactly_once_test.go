package mailer_test

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// ---------------------------------------------------------------------------
// fakeTxnSink — an in-memory CheckpointedSink modeling a transactional
// broker: staged output becomes visible only on Commit; the marker set
// models the in-transaction checkpoint marker; both survive "restarts"
// because tests reuse the same instance across runs (like a broker).
// ---------------------------------------------------------------------------

type fakeTxnSink struct {
	mu         sync.Mutex
	staged     []types.Record
	pending    map[string][]types.Record // prepared, awaiting commit/abort
	visible    []types.Record            // committed (read_committed view)
	markers    map[string]bool           // committed checkpoint markers
	aborted    map[string]bool
	waiters    map[string]chan struct{}
	onPrepared func(id string, err error)
}

func newFakeTxnSink() *fakeTxnSink {
	return &fakeTxnSink{
		pending: map[string][]types.Record{},
		markers: map[string]bool{},
		aborted: map[string]bool{},
		waiters: map[string]chan struct{}{},
	}
}

func (s *fakeTxnSink) SetOnPrepared(fn func(id string, err error)) { s.onPrepared = fn }

func (s *fakeTxnSink) Write(ctx context.Context, in <-chan types.Record) error {
	s.mu.Lock()
	s.staged = nil // a restart abandons any staged (uncommitted) output
	s.mu.Unlock()

	for r := range in {
		if r.IsWatermark {
			continue
		}
		if r.IsBarrier {
			id := r.CheckpointID
			s.mu.Lock()
			s.pending[id] = s.staged
			s.staged = nil
			ch := make(chan struct{})
			s.waiters[id] = ch
			cb := s.onPrepared
			s.mu.Unlock()

			cb(id, nil) // all pre-barrier output staged in the open txn

			// Block until the coordinator commits or aborts — a real
			// transactional producer can't write the next interval
			// into a still-pending transaction.
			select {
			case <-ch:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		s.mu.Lock()
		s.staged = append(s.staged, r)
		s.mu.Unlock()
	}
	return nil
}

func (s *fakeTxnSink) Commit(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.visible = append(s.visible, s.pending[id]...)
	s.markers[id] = true // the marker commits atomically with the data
	delete(s.pending, id)
	s.signal(id)
	return nil
}

func (s *fakeTxnSink) Abort(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, id)
	s.aborted[id] = true
	s.signal(id)
	return nil
}

func (s *fakeTxnSink) signal(id string) {
	if ch, ok := s.waiters[id]; ok {
		close(ch)
		delete(s.waiters, id)
	}
}

func (s *fakeTxnSink) WasCommitted(ctx context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.markers[id], nil
}

func (s *fakeTxnSink) TransactionalID() string { return "fake-txn" }

// visibleIDs returns how many times each record identity
// ("partition/offset") is visible in committed output.
func (s *fakeTxnSink) visibleIDs() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for _, r := range s.visible {
		out[strconv.Itoa(r.Partition)+"/"+strconv.FormatInt(r.Offset, 10)]++
	}
	return out
}

func (s *fakeTxnSink) visibleLastByKey() map[string][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]byte{}
	for _, r := range s.visible {
		out[string(r.Key)] = r.Value
	}
	return out
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const (
	eoParts   = 3
	eoPerPart = 40
)

func eoParts3() [][]types.Record {
	parts := make([][]types.Record, eoParts)
	for p := range parts {
		for i := 0; i < eoPerPart; i++ {
			parts[p] = append(parts[p], types.NewRecord(
				[]byte("key-"+strconv.Itoa(p)), []byte("v"+strconv.Itoa(i))))
		}
	}
	return parts
}

// runEO executes one pipeline run: replay source → Map → fakeTxnSink,
// coordinated checkpoints every 5ms. If haltStep is non-empty, the
// coordinator halts (simulated crash) at the haltOccurrence-th time
// that step completes, and the run is cancelled shortly after.
func runEO(t *testing.T, sk *fakeTxnSink, storage checkpoint.Storage, haltStep checkpoint.Step, haltOccurrence int) error {
	t.Helper()

	src := newReplaySource(eoParts3())
	src.emitDelay = 500 * time.Microsecond

	env := mailer.NewEnv().
		WithBufferSize(16).
		WithShutdownTimeout(300*time.Millisecond).
		WithCheckpointing(5*time.Millisecond, storage)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if haltStep != "" {
		occurrences := 0
		var once sync.Once
		env.WithCheckpointHook(func(step checkpoint.Step, id string) checkpoint.HookAction {
			if step != haltStep {
				return checkpoint.HookContinue
			}
			occurrences++
			if occurrences < haltOccurrence {
				return checkpoint.HookContinue
			}
			once.Do(func() {
				go func() {
					time.Sleep(20 * time.Millisecond) // let the halt settle
					cancel()
				}()
			})
			return checkpoint.HookHalt
		})
	}

	env.FromSource(src).
		Map(func(r types.Record) types.Record { return r }, "pass").
		ToSink(sk)

	return env.Execute(ctx)
}

func assertExactlyOnce(t *testing.T, sk *fakeTxnSink) {
	t.Helper()
	seen := sk.visibleIDs()
	for p := 0; p < eoParts; p++ {
		for i := 0; i < eoPerPart; i++ {
			id := strconv.Itoa(p) + "/" + strconv.Itoa(i)
			switch seen[id] {
			case 1:
			case 0:
				t.Errorf("record %s LOST: not visible in committed output", id)
			default:
				t.Errorf("record %s DUPLICATED: visible %d times", id, seen[id])
			}
			delete(seen, id)
		}
	}
	for id, n := range seen {
		t.Errorf("unexpected record %s visible %d times", id, n)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Crash after the commit decision was logged (prepared) but before the
// sink transaction committed. Recovery must see no marker, abort the
// dangling transaction, fall back to the previous completed
// checkpoint, and replay — its output was never visible, so the replay
// cannot duplicate.
func TestExactlyOnce_CrashBeforeSinkCommit(t *testing.T) {
	sk := newFakeTxnSink()
	storage := checkpoint.NewFileStorage(t.TempDir())

	runEO(t, sk, storage, checkpoint.StepPersistPrepared, 2) // crash run

	if err := runEO(t, sk, storage, "", 0); err != nil { // recovery run
		t.Fatalf("recovery run failed: %v", err)
	}
	assertExactlyOnce(t, sk)

	latest, _ := storage.Load()
	if latest == nil || !latest.Completed() {
		t.Error("expected final checkpoint to be completed after recovery run")
	}
}

// Crash after the sink transaction committed but before the checkpoint
// was marked completed (and before the advisory source offset commit).
// Recovery must find the marker, promote the prepared checkpoint, and
// NOT replay its interval — that would duplicate.
func TestExactlyOnce_CrashAfterSinkCommitBeforeCompleted(t *testing.T) {
	sk := newFakeTxnSink()
	storage := checkpoint.NewFileStorage(t.TempDir())

	runEO(t, sk, storage, checkpoint.StepSinkCommitted, 2)

	if err := runEO(t, sk, storage, "", 0); err != nil {
		t.Fatalf("recovery run failed: %v", err)
	}
	assertExactlyOnce(t, sk)
}

// Crash on the very first checkpoint, before anything ever completed:
// recovery falls back to a fresh start and the aborted transaction's
// output must not surface.
func TestExactlyOnce_CrashWithNoCompletedCheckpoint(t *testing.T) {
	sk := newFakeTxnSink()
	storage := checkpoint.NewFileStorage(t.TempDir())

	runEO(t, sk, storage, checkpoint.StepPersistPrepared, 1)

	if err := runEO(t, sk, storage, "", 0); err != nil {
		t.Fatalf("recovery run failed: %v", err)
	}
	assertExactlyOnce(t, sk)
}

// The umbrella property: crash at EVERY protocol step, recover, and
// verify no loss and no duplication each time.
func TestExactlyOnce_CrashSweepAllProtocolSteps(t *testing.T) {
	steps := []checkpoint.Step{
		checkpoint.StepPersistPrepared,
		checkpoint.StepSinkCommitted,
		checkpoint.StepPersistCompleted,
		checkpoint.StepOffsetsCommitted,
	}
	for _, step := range steps {
		t.Run(string(step), func(t *testing.T) {
			sk := newFakeTxnSink()
			storage := checkpoint.NewFileStorage(t.TempDir())

			runEO(t, sk, storage, step, 2)

			if err := runEO(t, sk, storage, "", 0); err != nil {
				t.Fatalf("recovery run failed: %v", err)
			}
			assertExactlyOnce(t, sk)
		})
	}
}

// failingStorage fails the first Save of a prepared checkpoint,
// simulating a checkpoint-persist failure mid-protocol.
type failingStorage struct {
	checkpoint.Storage
	mu     sync.Mutex
	failed bool
}

func (f *failingStorage) Save(data *checkpoint.CheckpointData) error {
	f.mu.Lock()
	shouldFail := data.Status == checkpoint.StatusPrepared && !f.failed
	if shouldFail {
		f.failed = true
	}
	f.mu.Unlock()
	if shouldFail {
		return errors.New("injected storage failure")
	}
	return f.Storage.Save(data)
}

// A checkpoint persist failure must abort the sink transaction, keep
// its output invisible, and fail the pipeline; the next run recovers
// and completes exactly-once.
func TestExactlyOnce_PersistFailureAbortsSinkTxn(t *testing.T) {
	sk := newFakeTxnSink()
	fs := &failingStorage{Storage: checkpoint.NewFileStorage(t.TempDir())}

	err := runEO(t, sk, fs, "", 0)
	if err == nil {
		t.Fatal("expected pipeline to fail on injected storage failure")
	}

	sk.mu.Lock()
	abortedCount := len(sk.aborted)
	sk.mu.Unlock()
	if abortedCount == 0 {
		t.Error("expected the sink transaction to be aborted on persist failure")
	}

	if err := runEO(t, sk, fs, "", 0); err != nil {
		t.Fatalf("recovery run failed: %v", err)
	}
	assertExactlyOnce(t, sk)
}

// Keyed stateful pipeline across a crash: Reduce state, keyed-worker
// snapshots, multi-partition offsets, and transactional output must
// all line up — final per-key counts equal an uninterrupted run.
func TestExactlyOnce_KeyedStateMultiPartition(t *testing.T) {
	sk := newFakeTxnSink()
	storage := checkpoint.NewFileStorage(t.TempDir())

	run := func(halt checkpoint.Step, occ int) error {
		src := newReplaySource(eoParts3())
		src.emitDelay = 500 * time.Microsecond

		env := mailer.NewEnv().
			WithBufferSize(16).
			WithShutdownTimeout(300*time.Millisecond).
			WithCheckpointing(5*time.Millisecond, storage)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if halt != "" {
			occurrences := 0
			var once sync.Once
			env.WithCheckpointHook(func(step checkpoint.Step, id string) checkpoint.HookAction {
				if step != halt {
					return checkpoint.HookContinue
				}
				occurrences++
				if occurrences < occ {
					return checkpoint.HookContinue
				}
				once.Do(func() {
					go func() { time.Sleep(20 * time.Millisecond); cancel() }()
				})
				return checkpoint.HookHalt
			})
		}
		env.FromSource(src).
			KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(4).
			Reduce(countReduceFn).
			ToSink(sk)
		return env.Execute(ctx)
	}

	run(checkpoint.StepPersistPrepared, 2) // crash
	if err := run("", 0); err != nil {     // recover + finish
		t.Fatalf("recovery run failed: %v", err)
	}

	// Each partition emits eoPerPart records for its single key; the
	// last visible Reduce emission per key must be exactly that count.
	last := sk.visibleLastByKey()
	for p := 0; p < eoParts; p++ {
		key := "key-" + strconv.Itoa(p)
		v, ok := last[key]
		if !ok {
			t.Errorf("key %s: no committed output", key)
			continue
		}
		if got := binary.BigEndian.Uint64(v); got != eoPerPart {
			t.Errorf("key %s: final count %d, want %d (state/offset/output misaligned)", key, got, eoPerPart)
		}
	}

	// The completed checkpoint tracks every partition individually.
	final, err := storage.LoadLatestCompleted()
	if err != nil || final == nil {
		t.Fatalf("no completed checkpoint after recovery: %v", err)
	}
	offs := checkpointOffsets(storage)
	if len(offs) != eoParts {
		t.Errorf("expected offsets for %d partitions, got %v", eoParts, offs)
	}
	for p := 0; p < eoParts; p++ {
		if offs[strconv.Itoa(p)] != eoPerPart {
			t.Errorf("partition %d: final checkpointed offset %d, want %d", p, offs[strconv.Itoa(p)], eoPerPart)
		}
	}
}

// A CheckpointedSink without checkpointing configured must be refused
// — exactly-once cannot be silently half-configured.
func TestExactlyOnce_RequiresCheckpointing(t *testing.T) {
	sk := newFakeTxnSink()
	env := mailer.NewEnv()
	env.FromSource(newReplaySource(eoParts3())).
		Map(func(r types.Record) types.Record { return r }, "pass").
		ToSink(sk)

	if err := env.Execute(context.Background()); err == nil {
		t.Fatal("expected configuration error for CheckpointedSink without WithCheckpointing")
	}
}
