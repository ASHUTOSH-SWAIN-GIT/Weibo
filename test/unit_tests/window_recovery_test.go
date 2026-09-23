package weibo_test

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/watermark"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/window"
)

// TestRecovery_WindowBufferSurvivesRestart is the regression test for records
// silently vanishing from a window across a restart.
//
// Run 1 buffers the first half of a window's records and checkpoints. Run 2
// restores that state and processes the second half. The window's final count
// must be the full set. With the Pebble backend, the list-append counter used to
// restart at 0 after a restore, so the second half overwrote the first half in
// place and the window fired with only half its records.
func TestRecovery_WindowBufferSurvivesRestart(t *testing.T) {
	for _, bk := range stateBackends(t) {
		t.Run(bk.Name, func(t *testing.T) {
			const perPart = 40 // records per partition; all in one tumbling window
			base := time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC)
			dir := t.TempDir()
			storage := checkpoint.NewFileStorage(dir)

			mkParts := func() [][]types.Record {
				keysByPart := [][]string{{"a", "b"}, {"c", "d"}}
				parts := make([][]types.Record, 2)
				for p := range parts {
					for i := 0; i < perPart; i++ {
						r := types.NewRecord([]byte(keysByPart[p][i%2]), []byte("v"))
						r.Timestamp = base.Add(time.Duration(i) * time.Second)
						parts[p] = append(parts[p], r)
					}
				}
				return parts
			}
			build := func(env *weibo.StreamExecutionEnv, src source.Source, sink *captureSink) {
				env.FromSource(src).
					KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(2).
					Window(window.NewTumbling(time.Hour)).
					Reduce(countReduceFn).
					ToSink(sink)
			}

			// Run 1: process half of each partition, wait for a checkpoint
			// covering exactly that, stop. Nothing has fired yet (no watermark).
			src1 := newReplaySource(mkParts())
			src1.pauseAfter = perPart // half of the 2*perPart records
			src1.stopAfterPause = true
			src1.resume = func() bool {
				offs := checkpointOffsets(storage)
				return offs != nil && offs["0"]+offs["1"] == perPart
			}
			env1 := weibo.NewEnv().WithCheckpointing(5*time.Millisecond, storage).WithStateBackend(bk.Factory)
			build(env1, src1, newCaptureSink())
			if err := env1.Execute(context.Background()); err != nil {
				t.Fatalf("run 1: %v", err)
			}
			if saved := checkpointOffsets(storage); saved["0"]+saved["1"] != perPart {
				t.Fatalf("checkpoint offsets %v, want them to sum to %d", saved, perPart)
			}

			// Run 2: restore and process the rest; the window flushes at end of stream.
			src2 := newReplaySource(mkParts())
			sink2 := newCaptureSink()
			env2 := weibo.NewEnv().WithCheckpointing(5*time.Millisecond, storage).WithStateBackend(bk.Factory)
			build(env2, src2, sink2)
			if err := env2.Execute(context.Background()); err != nil {
				t.Fatalf("run 2: %v", err)
			}

			sink2.mu.Lock()
			defer sink2.mu.Unlock()
			for _, key := range []string{"a", "b", "c", "d"} {
				v, ok := sink2.lastByKey[key]
				if !ok {
					t.Errorf("key %s: no output", key)
					continue
				}
				// perPart records per partition alternate between 2 keys.
				if got, want := binary.BigEndian.Uint64(v), uint64(perPart/2); got != want {
					t.Errorf("key %s: window counted %d records, want %d (records buffered before the restart were lost)", key, got, want)
				}
			}
		})
	}
}

// skewedSource models a Kafka source whose partition readers run at different
// speeds: partition 1 delivers one record, then partition 0 races far ahead in
// event time, then partition 1 delivers the rest. Sleeps between phases give the
// watermark generator's ticker time to fire in between, like a real fast reader.
type skewedSource struct {
	lateRecords int // records partition 1 delivers after partition 0's burst
}

func (s *skewedSource) Run(ctx context.Context, out chan<- types.Record) error {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	send := func(part int, key string, ts time.Time) bool {
		r := types.NewRecord([]byte(key), []byte("v"))
		r.Timestamp, r.Partition, r.Source = ts, part, "orders"
		select {
		case out <- r:
			return true
		case <-ctx.Done():
			return false
		}
	}
	// Partition 1, first record (so it is known before partition 0 runs ahead).
	if !send(1, "slow", base) {
		return nil
	}
	// Partition 0 races three windows ahead.
	for i := 0; i < 50; i++ {
		if !send(0, "fast", base.Add(3*time.Hour+time.Duration(i)*time.Second)) {
			return nil
		}
	}
	time.Sleep(60 * time.Millisecond) // let several watermark ticks fire
	// Partition 1 catches up with records that belong to the first window.
	for i := 1; i <= s.lateRecords; i++ {
		if !send(1, "slow", base.Add(time.Duration(i)*time.Second)) {
			return nil
		}
	}
	return nil
}

// TestWatermark_SkewedPartitionsLoseNothing: a partition that runs ahead must not
// advance the watermark past records that a slower partition has yet to deliver.
// With one shared generator those records were dropped as late, silently.
func TestWatermark_SkewedPartitionsLoseNothing(t *testing.T) {
	const late = 20
	newGen := func() watermark.WatermarkGenerator { return watermark.NewBoundedOutOfOrderness(time.Second) }
	src := &source.WatermarkSource{
		Source:       &skewedSource{lateRecords: late},
		Generator:    newGen(),
		NewGenerator: newGen,
		Interval:     5 * time.Millisecond,
	}

	var mu sync.Mutex
	counts := map[string]uint64{}
	sink := &collectSink{onRecord: func(r types.Record) {
		mu.Lock()
		counts[string(r.Key)] = binary.BigEndian.Uint64(r.Value)
		mu.Unlock()
	}}
	env := weibo.NewEnv()
	env.FromSource(src).
		KeyBy(func(r types.Record) []byte { return r.Key }).
		Window(window.NewTumbling(time.Hour)).
		Reduce(countReduceFn).
		ToSink(sink)
	if err := env.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := counts["slow"], uint64(late+1); got != want {
		t.Errorf("slow partition counted %d records, want %d: the fast partition's watermark dropped the rest as late", got, want)
	}
	if got, want := counts["fast"], uint64(50); got != want {
		t.Errorf("fast partition counted %d records, want %d", got, want)
	}
}

// collectSink calls onRecord for every data record.
type collectSink struct{ onRecord func(types.Record) }

func (s *collectSink) Write(ctx context.Context, in <-chan types.Record) error {
	for r := range in {
		if r.IsBarrier || r.IsWatermark {
			continue
		}
		s.onRecord(r)
	}
	return nil
}
