package sdk

import (
	"context"
	"io"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
)

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
