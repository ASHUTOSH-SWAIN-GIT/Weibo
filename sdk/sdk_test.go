package sdk

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/jobagent"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// checkpointableSource is a minimal source.CheckpointSource, just enough
// for engine.go's coordinated (exactly-once) mode to accept it.
type checkpointableSource struct{ n int }

func (s *checkpointableSource) Run(ctx context.Context, out chan<- types.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- types.Record{Key: []byte("k"), Value: []byte("v"), Offset: int64(s.n)}:
			s.n++
			time.Sleep(time.Millisecond)
		}
	}
}
func (s *checkpointableSource) CheckpointOffset() ([]byte, error) { return []byte("0"), nil }
func (s *checkpointableSource) RestoreOffset([]byte) error        { return nil }

// fatalCommitSink is a minimal sink.CheckpointedSink whose Commit always
// fails with context.Canceled — the exact shape found live: a coordinator
// error that "Is" context.Canceled from an unrelated internal force-unwind,
// not from anything asking the job to stop.
type fatalCommitSink struct{ onPrepared func(id string, err error) }

func (s *fatalCommitSink) Write(ctx context.Context, in <-chan types.Record) error {
	for r := range in {
		if r.IsBarrier {
			s.onPrepared(r.CheckpointID, nil)
		}
	}
	return nil
}
func (s *fatalCommitSink) SetOnPrepared(fn func(id string, err error)) { s.onPrepared = fn }
func (s *fatalCommitSink) Commit(ctx context.Context, id string) error { return context.Canceled }
func (s *fatalCommitSink) Abort(ctx context.Context, id string) error  { return nil }
func (s *fatalCommitSink) WasCommitted(ctx context.Context, id string) (bool, error) {
	return false, nil
}
func (s *fatalCommitSink) TransactionalID() string { return "fatal-commit-test" }

// TestRunBuild_ContextCanceledErrorWithoutShutdownRequestExitsNonZero guards
// the process-exit-code layer of the same bug fixed in jobagent.Agent.Run:
// runBuild must not exit 0 just because the run error "Is" context.Canceled.
// It now defers to agent.State().Phase (set correctly by the Agent.Run fix)
// instead of re-deriving the same fragile check a second time.
func TestRunBuild_ContextCanceledErrorWithoutShutdownRequestExitsNonZero(t *testing.T) {
	build := func(env *weibo.StreamExecutionEnv) {
		env.FromSource(&checkpointableSource{}).ToSink(&fatalCommitSink{})
	}
	env := map[string]string{"DATA_DIR": t.TempDir(), "PORT": "0", "CHECKPOINT_INTERVAL": "5ms"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	code := runBuild(ctx, build, func(k string) string { return env[k] }, io.Discard, io.Discard)
	if code != 1 {
		t.Fatalf("exit code: got %d, want 1 (this job was never asked to stop)", code)
	}
}

// The harness wires the builder's pipeline and runs it to completion.
func TestRunBuild_Completes(t *testing.T) {
	build := func(env *weibo.StreamExecutionEnv) {
		env.FromSource(source.FromSlices([]string{"a", "b"}, []string{"1", "2"})).
			ToSink(sink.NewBlackholeSink())
	}
	env := map[string]string{"DATA_DIR": t.TempDir(), "PORT": "0"}
	code := runBuild(context.Background(), build, func(k string) string { return env[k] }, io.Discard, io.Discard)
	if code != 0 {
		t.Fatalf("exit code: got %d, want 0", code)
	}
}

// With checkpointing enabled the harness configures storage without error.
func TestRunBuild_Checkpointing(t *testing.T) {
	build := func(env *weibo.StreamExecutionEnv) {
		env.FromSource(source.FromSlices([]string{"a"}, []string{"1"})).
			ToSink(sink.NewBlackholeSink())
	}
	env := map[string]string{"DATA_DIR": t.TempDir(), "PORT": "0", "CHECKPOINT_INTERVAL": "200ms"}
	code := runBuild(context.Background(), build, func(k string) string { return env[k] }, io.Discard, io.Discard)
	if code != 0 {
		t.Fatalf("exit code: got %d, want 0", code)
	}
}

// A bad CHECKPOINT_INTERVAL is a usage error.
func TestRunBuild_BadInterval(t *testing.T) {
	env := map[string]string{"DATA_DIR": t.TempDir(), "CHECKPOINT_INTERVAL": "nonsense"}
	code := runBuild(context.Background(), func(*weibo.StreamExecutionEnv) {}, func(k string) string { return env[k] }, io.Discard, io.Discard)
	if code != 2 {
		t.Fatalf("exit code: got %d, want 2", code)
	}
}

// requestSavepointOnceRunning polls RequestSavepoint until it is accepted
// (it requires Run to have installed its cancel func first) or t fails.
func requestSavepointOnceRunning(t *testing.T, a *jobagent.Agent, label string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a.RequestSavepoint(label) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("savepoint request %q never accepted", label)
}

// TestPromoteSavepoint_PromotionFailureMarksRunFailed guards the exact bug
// found live: a stop-with-savepoint whose promotion fails (here: no
// checkpoint had completed yet to promote) must not leave the run looking
// like a clean finish. Before this fix, a failed promoteSavepoint left
// Run's own Phase decision (Finished) untouched.
func TestPromoteSavepoint_PromotionFailureMarksRunFailed(t *testing.T) {
	env := weibo.NewEnv().
		FromSource(&checkpointableSource{}).
		ToSink(sink.NewBlackholeSink())
	// No WithCheckpointing: nothing ever completes a checkpoint, so
	// CreateSavepoint below has nothing to promote and must fail.
	a := jobagent.New(env)

	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	requestSavepointOnceRunning(t, a, "v1")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not drain after savepoint request")
	}
	if a.State().Phase != jobagent.PhaseFinished {
		t.Fatalf("phase before promotion: got %q, want finished", a.State().Phase)
	}

	blobs := checkpoint.NewFileBlobstore(t.TempDir())
	promoteSavepoint(a, t.TempDir(), blobs, io.Discard, io.Discard)

	st := a.State()
	if st.Phase != jobagent.PhaseFailed {
		t.Fatalf("phase after failed promotion: got %q, want failed", st.Phase)
	}
	if st.LastError == "" {
		t.Fatal("expected LastError to explain the promotion failure")
	}
}

// The success path leaves the run's Finished phase untouched.
func TestPromoteSavepoint_SuccessLeavesRunFinished(t *testing.T) {
	ckptDir := t.TempDir()
	env := weibo.NewEnv().
		FromSource(&checkpointableSource{}).
		ToSink(sink.NewBlackholeSink()).
		WithCheckpointing(5*time.Millisecond, checkpoint.NewFileStorage(ckptDir))
	a := jobagent.New(env)

	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	// Give at least one checkpoint a chance to complete before stopping.
	time.Sleep(50 * time.Millisecond)
	requestSavepointOnceRunning(t, a, "v1")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not drain after savepoint request")
	}

	blobs := checkpoint.NewFileBlobstore(t.TempDir())
	promoteSavepoint(a, ckptDir, blobs, io.Discard, io.Discard)

	if got := a.State().Phase; got != jobagent.PhaseFinished {
		t.Fatalf("phase after successful promotion: got %q, want finished", got)
	}
}
