package mailer_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/state"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

func stateBackends(t *testing.T) []struct {
	Name    string
	Factory state.BackendFactory
} {
	return []struct {
		Name    string
		Factory state.BackendFactory
	}{
		{"Memory", state.InMemory()},
		{"Pebble", state.Pebble(t.TempDir())},
	}
}

// ---------------------------------------------------------------------------
// Test doubles that mimic the Kafka contract without a broker.
// ---------------------------------------------------------------------------

// replaySource mimics Kafka semantics: a fixed append-only log per
// partition, consumable from per-partition offsets. It implements
// source.CheckpointSource, so Execute checkpoints and restores its
// offsets exactly like it would a real KafkaSource.
type replaySource struct {
	parts [][]types.Record

	// pauseAfter > 0: after emitting that many records (across all
	// partitions), poll resume() until it returns true, then stop if
	// stopAfterPause is set. Used to make checkpoint timing deterministic.
	pauseAfter     int
	resume         func() bool
	stopAfterPause bool

	// emitDelay throttles emission like a live stream. Checkpoint
	// barriers are only injected while the source is running, so a
	// source that dumps everything instantly and closes never gets
	// checkpointed mid-run.
	emitDelay time.Duration

	mu       sync.Mutex
	pos      []int64
	startPos []int64 // positions at the moment Run started (for assertions)
}

// newReplaySource builds a partitioned log. Record offsets are set to
// the position within their partition; the partition id is stored in a
// header so sinks can identify records.
func newReplaySource(parts [][]types.Record) *replaySource {
	for p := range parts {
		for i := range parts[p] {
			parts[p][i].Offset = int64(i)
			parts[p][i].Partition = p
			parts[p][i] = parts[p][i].WithHeader("part", []byte(strconv.Itoa(p)))
		}
	}
	return &replaySource{parts: parts}
}

func (s *replaySource) Run(ctx context.Context, out chan<- types.Record) error {
	s.mu.Lock()
	if s.pos == nil {
		s.pos = make([]int64, len(s.parts))
	}
	s.startPos = append([]int64(nil), s.pos...)
	s.mu.Unlock()

	emitted := 0
	for {
		progress := false
		for p := range s.parts {
			s.mu.Lock()
			i := s.pos[p]
			s.mu.Unlock()
			if int(i) >= len(s.parts[p]) {
				continue
			}
			if s.emitDelay > 0 {
				time.Sleep(s.emitDelay)
			}
			select {
			case out <- s.parts[p][i]:
			case <-ctx.Done():
				return nil
			}
			s.mu.Lock()
			s.pos[p]++
			s.mu.Unlock()
			progress = true
			emitted++

			if s.pauseAfter > 0 && emitted == s.pauseAfter {
				for !s.resume() {
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(5 * time.Millisecond):
					}
				}
				if s.stopAfterPause {
					return nil
				}
			}
		}
		if !progress {
			return nil
		}
	}
}

// CheckpointOffset returns per-partition positions as JSON, exactly
// like KafkaSource.CheckpointOffset.
func (s *replaySource) CheckpointOffset() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	offsets := make(map[string]int64, len(s.pos))
	for p, off := range s.pos {
		offsets[strconv.Itoa(p)] = off
	}
	return json.Marshal(offsets)
}

// RestoreOffset seeks each partition to its checkpointed position.
func (s *replaySource) RestoreOffset(data []byte) error {
	var offsets map[string]int64
	if err := json.Unmarshal(data, &offsets); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pos == nil {
		s.pos = make([]int64, len(s.parts))
	}
	for pStr, off := range offsets {
		p, err := strconv.Atoi(pStr)
		if err != nil || p < 0 || p >= len(s.parts) {
			return fmt.Errorf("bad partition %q in checkpoint", pStr)
		}
		s.pos[p] = off
	}
	return nil
}

func (s *replaySource) startPositions() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.startPos...)
}

// captureSink records everything it receives. failWhen (optional)
// simulates a sink crash: when it returns true for the current record
// count, Write returns an error and the pipeline dies mid-flight.
// delay (optional) slows consumption so periodic checkpoints have time
// to fire during a run.
type captureSink struct {
	failWhen func(seen int) bool
	delay    time.Duration

	mu        sync.Mutex
	seen      map[string]bool // "part/offset" identity of data records
	lastByKey map[string][]byte
	count     int
}

func newCaptureSink() *captureSink {
	return &captureSink{seen: map[string]bool{}, lastByKey: map[string][]byte{}}
}

func (s *captureSink) Write(ctx context.Context, in <-chan types.Record) error {
	for r := range in {
		if r.IsBarrier || r.IsWatermark {
			continue
		}
		if s.delay > 0 {
			time.Sleep(s.delay)
		}
		s.mu.Lock()
		s.seen[string(r.Headers["part"])+"/"+strconv.FormatInt(r.Offset, 10)] = true
		s.lastByKey[string(r.Key)] = r.Value
		s.count++
		n := s.count
		s.mu.Unlock()
		if s.failWhen != nil && s.failWhen(n) {
			return errors.New("injected sink crash")
		}
	}
	return nil
}

// checkpointOffsets loads the latest checkpoint and returns the saved
// per-partition source offsets (nil if no checkpoint yet).
func checkpointOffsets(storage checkpoint.Storage) map[string]int64 {
	data, err := storage.Load()
	if err != nil || data == nil {
		return nil
	}
	raw, ok := data.Source["offset"]
	if !ok {
		return nil
	}
	var offsets map[string]int64
	if json.Unmarshal(raw, &offsets) != nil {
		return nil
	}
	return offsets
}

// ---------------------------------------------------------------------------
// Crash and recovery
// ---------------------------------------------------------------------------

// TestRecovery_CrashResumeFromMultiPartitionOffsets: hard-crash the
// pipeline via a failing sink, then restart it against the same
// checkpoint storage. The restarted run must resume every partition
// from its own checkpointed offset (not from zero, not from a global
// position), and must deliver the entire post-checkpoint suffix.
func TestRecovery_CrashResumeFromMultiPartitionOffsets(t *testing.T) {
	for _, bk := range stateBackends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			const perPart = 40
			dir := t.TempDir()
			storage := checkpoint.NewFileStorage(dir)

			mkParts := func() [][]types.Record {
				parts := make([][]types.Record, 3)
				for p := range parts {
					for i := 0; i < perPart; i++ {
						parts[p] = append(parts[p], types.NewRecord(
							[]byte("key-"+strconv.Itoa(p)), []byte("v"+strconv.Itoa(i))))
					}
				}
				return parts
			}

			src1 := newReplaySource(mkParts())
			src1.emitDelay = time.Millisecond
			sink1 := newCaptureSink()
			sink1.failWhen = func(seen int) bool {
				return seen >= 30 && checkpointOffsets(storage) != nil
			}

			env1 := mailer.NewEnv().WithBufferSize(16).
				WithCheckpointing(5*time.Millisecond, storage).
				WithStateBackend(bk.Factory)
			env1.FromSource(src1).
				Map(func(r types.Record) types.Record { return r }, "pass").
				ToSink(sink1)

			if err := env1.Execute(context.Background()); err == nil {
				t.Fatal("run 1: expected pipeline to fail from injected sink crash, got nil")
			}

			saved := checkpointOffsets(storage)
			if saved == nil {
				t.Fatal("run 1: no checkpoint was saved before the crash")
			}
			if len(saved) != 3 {
				t.Fatalf("checkpoint should hold offsets for all 3 partitions, got %v", saved)
			}

			src2 := newReplaySource(mkParts())
			sink2 := newCaptureSink()
			env2 := mailer.NewEnv().WithBufferSize(16).
				WithCheckpointing(5*time.Millisecond, storage).
				WithStateBackend(bk.Factory)
			env2.FromSource(src2).
				Map(func(r types.Record) types.Record { return r }, "pass").
				ToSink(sink2)

			if err := env2.Execute(context.Background()); err != nil {
				t.Fatalf("run 2: Execute failed: %v", err)
			}

			start := src2.startPositions()
			for p := 0; p < 3; p++ {
				want := saved[strconv.Itoa(p)]
				if start[p] != want {
					t.Errorf("partition %d resumed at %d, checkpoint says %d", p, start[p], want)
				}
			}

			sink2.mu.Lock()
			defer sink2.mu.Unlock()
			for p := 0; p < 3; p++ {
				for off := saved[strconv.Itoa(p)]; off < perPart; off++ {
					id := strconv.Itoa(p) + "/" + strconv.FormatInt(off, 10)
					if !sink2.seen[id] {
						t.Errorf("record %s (post-checkpoint) never delivered after recovery", id)
					}
				}
			}
		})
	}
}

// TestRecovery_KeyedStateRestoredExactly: run 1 processes exactly half
// the log, waits until a checkpoint covering all of it is on disk, and
// stops. Run 2 restores keyed Reduce state + per-partition offsets and
// processes the rest. Final per-key counts must equal an uninterrupted
// run — state and offsets were captured consistently.
func TestRecovery_KeyedStateRestoredExactly(t *testing.T) {
	for _, bk := range stateBackends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			const perPart = 50
			dir := t.TempDir()
			storage := checkpoint.NewFileStorage(dir)

			mkParts := func() [][]types.Record {
				keysByPart := [][]string{{"a", "b"}, {"c", "d"}}
				parts := make([][]types.Record, 2)
				for p := range parts {
					for i := 0; i < perPart; i++ {
						parts[p] = append(parts[p],
							types.NewRecord([]byte(keysByPart[p][i%2]), []byte("v")))
					}
				}
				return parts
			}

			src1 := newReplaySource(mkParts())
			src1.pauseAfter = 50
			src1.stopAfterPause = true
			src1.resume = func() bool {
				offs := checkpointOffsets(storage)
				return offs != nil && offs["0"]+offs["1"] == 50
			}

			sink1 := newCaptureSink()
			env1 := mailer.NewEnv().
				WithCheckpointing(5*time.Millisecond, storage).
				WithStateBackend(bk.Factory)
			env1.FromSource(src1).
				KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(4).
				Reduce(countReduceFn).
				ToSink(sink1)

			if err := env1.Execute(context.Background()); err != nil {
				t.Fatalf("run 1: Execute failed: %v", err)
			}

			saved := checkpointOffsets(storage)
			if saved["0"] != 25 || saved["1"] != 25 {
				t.Fatalf("expected per-partition offsets {0:25 1:25}, got %v", saved)
			}

			src2 := newReplaySource(mkParts())
			sink2 := newCaptureSink()
			env2 := mailer.NewEnv().
				WithCheckpointing(5*time.Millisecond, storage).
				WithStateBackend(bk.Factory)
			env2.FromSource(src2).
				KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(4).
				Reduce(countReduceFn).
				ToSink(sink2)

			if err := env2.Execute(context.Background()); err != nil {
				t.Fatalf("run 2: Execute failed: %v", err)
			}

			if start := src2.startPositions(); start[0] != 25 || start[1] != 25 {
				t.Fatalf("run 2 should resume both partitions at 25, got %v", start)
			}

			sink2.mu.Lock()
			defer sink2.mu.Unlock()
			for _, key := range []string{"a", "b", "c", "d"} {
				v, ok := sink2.lastByKey[key]
				if !ok {
					t.Errorf("key %s: no output in run 2", key)
					continue
				}
				if got := binary.BigEndian.Uint64(v); got != 25 {
					t.Errorf("key %s: final count %d, want 25", key, got)
				}
			}
		})
	}
}

// TestRecovery_NonKeyedStateRestored: a Reduce used WITHOUT KeyBy is a
// top-level operator, snapshotted under "op-<i>". Run 1 counts the first
// half and checkpoints; run 2 must restore that op-<i> state and continue,
// so the final per-key counts match an uninterrupted run. Regression test
// for op-<i> state being snapshotted but never restored.
func TestRecovery_NonKeyedStateRestored(t *testing.T) {
	for _, bk := range stateBackends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			const total = 50
			dir := t.TempDir()
			storage := checkpoint.NewFileStorage(dir)

			mkParts := func() [][]types.Record {
				recs := make([]types.Record, total)
				for i := 0; i < total; i++ {
					key := "a"
					if i%2 == 1 {
						key = "b"
					}
					recs[i] = types.NewRecord([]byte(key), []byte("v"))
				}
				return [][]types.Record{recs}
			}

			src1 := newReplaySource(mkParts())
			src1.pauseAfter = 25
			src1.stopAfterPause = true
			src1.resume = func() bool {
				offs := checkpointOffsets(storage)
				return offs != nil && offs["0"] == 25
			}
			sink1 := newCaptureSink()
			env1 := mailer.NewEnv().
				WithCheckpointing(5*time.Millisecond, storage).
				WithStateBackend(bk.Factory)
			env1.FromSource(src1).Reduce(countReduceFn).ToSink(sink1) // no KeyBy
			if err := env1.Execute(context.Background()); err != nil {
				t.Fatalf("run 1: Execute failed: %v", err)
			}
			if saved := checkpointOffsets(storage); saved["0"] != 25 {
				t.Fatalf("expected offset {0:25}, got %v", saved)
			}

			src2 := newReplaySource(mkParts())
			sink2 := newCaptureSink()
			env2 := mailer.NewEnv().
				WithCheckpointing(5*time.Millisecond, storage).
				WithStateBackend(bk.Factory)
			env2.FromSource(src2).Reduce(countReduceFn).ToSink(sink2) // no KeyBy
			if err := env2.Execute(context.Background()); err != nil {
				t.Fatalf("run 2: Execute failed: %v", err)
			}

			sink2.mu.Lock()
			defer sink2.mu.Unlock()
			for _, key := range []string{"a", "b"} {
				v, ok := sink2.lastByKey[key]
				if !ok {
					t.Errorf("key %s: no output in run 2", key)
					continue
				}
				// 25 of each across the full log; run 2 must continue the
				// pre-crash count, not restart it. Without op-<i> restore
				// this comes out ~12–13.
				if got := binary.BigEndian.Uint64(v); got != 25 {
					t.Errorf("key %s: final count %d, want 25 (state not restored?)", key, got)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Cancellation with completely full edges
// ---------------------------------------------------------------------------

// blockedSink never consumes a record — it just waits for its context.
// With it attached, every edge in the pipeline fills to capacity and
// every upstream stage ends up blocked in a send.
type blockedSink struct{}

func (blockedSink) Write(ctx context.Context, in <-chan types.Record) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestShutdown_CancelWhileEdgesFull: with all edges full and every
// stage blocked mid-send, cancellation cannot drain — the shutdown
// timeout must fire, force-unwind every stage type (source, stateless
// parallel, keyed, sink), and Execute must return without deadlock or
// goroutine leaks.
func TestShutdown_CancelWhileEdgesFull(t *testing.T) {
	before := runtime.NumGoroutine()

	parts := [][]types.Record{make([]types.Record, 0, 100000)}
	for i := 0; i < 100000; i++ {
		parts[0] = append(parts[0], types.NewRecord([]byte(strconv.Itoa(i%8)), []byte("v")))
	}
	src := newReplaySource(parts)

	env := mailer.NewEnv().WithBufferSize(4).WithShutdownTimeout(500 * time.Millisecond)
	env.FromSource(src).
		Map(func(r types.Record) types.Record { return r }, "m1").WithParallelism(3).
		KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(4).
		Reduce(countReduceFn).
		Map(func(r types.Record) types.Record { return r }, "m2").
		ToSink(blockedSink{})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Give the edges time to fill completely before cancelling.
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() { done <- env.Execute(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil from forced shutdown under cancellation, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Execute deadlocked: cancellation with full edges never completed")
	}

	// All stage goroutines must unwind. Poll: fan-in helpers and the
	// hardCtx watcher need a moment after Execute returns.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if g := runtime.NumGoroutine(); g <= before+3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak after forced shutdown: before=%d after=%d", before, runtime.NumGoroutine())
		}
		time.Sleep(50 * time.Millisecond)
	}
}
