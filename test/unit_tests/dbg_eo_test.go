package mailer_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

func TestDebugEO(t *testing.T) {
	sk := newFakeTxnSink()
	storage := checkpoint.NewFileStorage(t.TempDir())

	run := func(halt checkpoint.Step, occ int) error {
		src := newReplaySource(eoParts3())
		src.emitDelay = 500 * time.Microsecond
		env := mailer.NewEnv().WithBufferSize(16).
			WithShutdownTimeout(300*time.Millisecond).
			WithCheckpointing(5*time.Millisecond, storage)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if halt != "" {
			n := 0
			var once sync.Once
			env.WithCheckpointHook(func(step checkpoint.Step, id string) checkpoint.HookAction {
				if step != halt {
					return checkpoint.HookContinue
				}
				n++
				if n < occ {
					return checkpoint.HookContinue
				}
				once.Do(func() { go func() { time.Sleep(20 * time.Millisecond); cancel() }() })
				return checkpoint.HookHalt
			})
		}
		env.FromSource(src).
			KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(4).
			Reduce(countReduceFn).
			ToSink(sk)
		return env.Execute(ctx)
	}

	run(checkpoint.StepPersistPrepared, 2)

	data, err := storage.LoadLatestCompleted()
	if err != nil || data == nil {
		t.Fatalf("no completed checkpoint: %v", err)
	}
	var offs map[string]int64
	json.Unmarshal(data.Source["offset"], &offs)
	fmt.Printf("DBG cp=%s offsets=%v\n", data.ID, offs)
	var total int64
	for _, v := range offs {
		total += v
	}
	var stateTotal uint64
	for key, snap := range data.Operators {
		var st struct {
			Entries map[string][]byte `json:"entries"`
		}
		if json.Unmarshal(snap, &st) == nil {
			for k, v := range st.Entries {
				if len(v) == 8 {
					c := binary.BigEndian.Uint64(v)
					stateTotal += c
					fmt.Printf("DBG %s state[%s]=%d\n", key, k, c)
				}
			}
		}
	}
	fmt.Printf("DBG offsetsTotal=%d stateTotal=%d visible=%d\n", total, stateTotal, len(sk.visible))
}

func TestDebugEO2(t *testing.T) {
	sk := newFakeTxnSink()
	storage := checkpoint.NewFileStorage(t.TempDir())

	run := func(halt checkpoint.Step, occ int) error {
		src := newReplaySource(eoParts3())
		src.emitDelay = 500 * time.Microsecond
		env := mailer.NewEnv().WithBufferSize(16).
			WithShutdownTimeout(300*time.Millisecond).
			WithCheckpointing(5*time.Millisecond, storage)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if halt != "" {
			n := 0
			var once sync.Once
			env.WithCheckpointHook(func(step checkpoint.Step, id string) checkpoint.HookAction {
				if step != halt {
					return checkpoint.HookContinue
				}
				n++
				if n < occ {
					return checkpoint.HookContinue
				}
				once.Do(func() { go func() { time.Sleep(20 * time.Millisecond); cancel() }() })
				return checkpoint.HookHalt
			})
		}
		env.FromSource(src).
			KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(4).
			Reduce(countReduceFn).
			ToSink(sk)
		return env.Execute(ctx)
	}

	run(checkpoint.StepPersistPrepared, 2)
	nVisibleAfterCrash := len(sk.visible)
	if err := run("", 0); err != nil {
		t.Fatalf("run2: %v", err)
	}
	sk.mu.Lock()
	fmt.Printf("DBG2 visibleAfterCrash=%d visibleFinal=%d\n", nVisibleAfterCrash, len(sk.visible))
	// print the sequence of counts for key-0 in visible order
	var seq []uint64
	for _, r := range sk.visible {
		if string(r.Key) == "key-0" && len(r.Value) == 8 {
			seq = append(seq, binary.BigEndian.Uint64(r.Value))
		}
	}
	sk.mu.Unlock()
	fmt.Printf("DBG2 key-0 emission sequence: %v\n", seq)
}
