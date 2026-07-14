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
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/state"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// trackingBackend wraps a MemoryBackend to observe injection and
// lifecycle: which owner it was created for, and whether the engine
// closed it after the run.
type trackingBackend struct {
	state.StateBackend
	owner  string
	mu     sync.Mutex
	closed bool
}

func (b *trackingBackend) Close() error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	return nil
}

// trackingFactory records every owner ID it is asked for.
type trackingFactory struct {
	mu       sync.Mutex
	backends map[string]*trackingBackend
}

func newTrackingFactory() *trackingFactory {
	return &trackingFactory{backends: map[string]*trackingBackend{}}
}

func (f *trackingFactory) factory(ownerID string) (state.StateBackend, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b := &trackingBackend{StateBackend: state.NewMemoryBackend(), owner: ownerID}
	f.backends[ownerID] = b
	return b, nil
}

func (f *trackingFactory) owners() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.backends))
	for o := range f.backends {
		out = append(out, o)
	}
	return out
}

func TestStateBackend_InjectedPerKeyedWorker(t *testing.T) {
	tf := newTrackingFactory()
	sk := newCaptureSink()

	env := mailer.NewEnv().WithStateBackend(tf.factory)
	env.FromSource(newReplaySource(eoParts3())).
		KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(3).
		Reduce(countReduceFn).
		ToSink(sk)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// One backend per keyed-worker clone, named by the checkpoint
	// owner ID convention.
	owners := tf.owners()
	if len(owners) != 3 {
		t.Fatalf("expected 3 injected backends (one per worker), got %v", owners)
	}
	for w := 0; w < 3; w++ {
		key := "worker-" + strconv.Itoa(w)
		if _, ok := tf.backends[key]; !ok {
			t.Errorf("expected a backend for owner %s, got %v", key, owners)
		}
	}

	// The injected backends were actually used: the pipeline computed
	// correct per-key counts through them.
	sk.mu.Lock()
	defer sk.mu.Unlock()
	for p := 0; p < eoParts; p++ {
		key := "key-" + strconv.Itoa(p)
		v, ok := sk.lastByKey[key]
		if !ok || binary.BigEndian.Uint64(v) != eoPerPart {
			t.Errorf("key %s: expected final count %d via injected backend", key, eoPerPart)
		}
	}

	// io.Closer backends are closed when the run ends.
	for owner, b := range tf.backends {
		b.mu.Lock()
		closed := b.closed
		b.mu.Unlock()
		if !closed {
			t.Errorf("backend %s not closed after Execute", owner)
		}
	}
}

func TestStateBackend_InjectedForTopLevelOperator(t *testing.T) {
	tf := newTrackingFactory()
	sk := newCaptureSink()

	// Reduce WITHOUT KeyBy runs as a standalone channel stage; its
	// backend owner is the operator index.
	env := mailer.NewEnv().WithStateBackend(tf.factory)
	env.FromSource(newReplaySource([][]types.Record{dataRows("a", 10)})).
		Reduce(countReduceFn).
		ToSink(sk)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if _, ok := tf.backends["op-0"]; !ok {
		t.Errorf("expected backend for owner op-0, got %v", tf.owners())
	}
}

func TestStateBackend_FactoryErrorFailsExecute(t *testing.T) {
	env := mailer.NewEnv().WithStateBackend(func(ownerID string) (state.StateBackend, error) {
		return nil, errors.New("disk on fire")
	})
	env.FromSource(newReplaySource(eoParts3())).
		KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(2).
		Reduce(countReduceFn).
		ToSink(newCaptureSink())

	err := env.Execute(context.Background())
	if err == nil {
		t.Fatal("expected Execute to fail when the state backend factory errors")
	}
}

// Crash + recovery must work identically with injected backends:
// checkpointed state is restored into the factory-created backends of
// the new run.
func TestStateBackend_RecoveryWithInjectedBackends(t *testing.T) {
	storage := checkpoint.NewFileStorage(t.TempDir())

	run := func(pauseAt int) error {
		tf := newTrackingFactory()
		src := newReplaySource(twoPartAlternating())
		if pauseAt > 0 {
			src.pauseAfter = pauseAt
			src.stopAfterPause = true
			src.resume = func() bool {
				offs := checkpointOffsets(storage)
				return offs != nil && offs["0"]+offs["1"] == int64(pauseAt)
			}
		}
		sk := newCaptureSink()
		env := mailer.NewEnv().
			WithStateBackend(tf.factory).
			WithCheckpointing(5*time.Millisecond, storage)
		env.FromSource(src).
			KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(4).
			Reduce(countReduceFn).
			ToSink(sk)
		if err := env.Execute(context.Background()); err != nil {
			return err
		}
		if pauseAt == 0 {
			// Full-log totals: every key appears 25 times.
			sk.mu.Lock()
			defer sk.mu.Unlock()
			for _, key := range []string{"a", "b", "c", "d"} {
				v, ok := sk.lastByKey[key]
				if !ok || binary.BigEndian.Uint64(v) != 25 {
					t.Errorf("key %s: state not restored into injected backend (got %v)", key, v)
				}
			}
		}
		return nil
	}

	if err := run(50); err != nil { // first half, checkpoint, stop
		t.Fatalf("run 1: %v", err)
	}
	if err := run(0); err != nil { // restore into fresh injected backends, finish
		t.Fatalf("run 2: %v", err)
	}
}

// dataRows builds a single-partition log of n records with one key.
func dataRows(key string, n int) []types.Record {
	recs := make([]types.Record, n)
	for i := range recs {
		recs[i] = types.NewRecord([]byte(key), []byte("v"))
	}
	return recs
}

// twoPartAlternating mirrors the layout used by the keyed recovery
// test: partition 0 alternates keys a/b, partition 1 alternates c/d,
// 50 records each.
func twoPartAlternating() [][]types.Record {
	keysByPart := [][]string{{"a", "b"}, {"c", "d"}}
	parts := make([][]types.Record, 2)
	for p := range parts {
		for i := 0; i < 50; i++ {
			parts[p] = append(parts[p],
				types.NewRecord([]byte(keysByPart[p][i%2]), []byte("v")))
		}
	}
	return parts
}
