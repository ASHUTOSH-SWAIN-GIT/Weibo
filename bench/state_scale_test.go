// Package bench measures how state backends scale (durable-state plan,
// Phase 6). Run with:
//
//	go test -bench . -benchtime=1x -timeout 30m ./bench/
//
// Add -short to skip the 5M-key tier. Each sub-benchmark loads K keys
// into a backend and reports, via custom metrics:
//
//	load-rec/s        write throughput while building K keys of state
//	lookup-ns         average Get latency over random existing keys
//	full-ckpt-ms      checkpoint duration with all K keys "changed"
//	incr-ckpt-ms      checkpoint duration after touching only 1,000 keys
//	restore-ms        time to rebuild a fresh backend from the checkpoint
//	ckpt-bytes        size of the checkpoint artifact (JSON or state dir)
//	heap-mb           Go heap growth attributable to the loaded state
//	disk-mb           live on-disk footprint (Pebble only)
//
// Modes:
//
//	memory          MemoryBackend, checkpoint = SnapshotAll → JSON
//	pebble-compat   PebbleBackend, checkpoint = SnapshotAll → JSON
//	pebble-native   PebbleBackend, checkpoint = CheckpointTo (flush + hard-links)
//
// The success condition for native checkpoints: incr-ckpt-ms must stay
// roughly flat across key counts (cost ∝ changed data), while full
// serialization's checkpoint cost grows with total state.
//
// Barrier-pause note: a checkpoint runs synchronously inside the
// operator as the barrier passes, so full-ckpt-ms / incr-ckpt-ms IS
// the per-barrier pipeline pause for that operator. restore-ms is the
// state-restoration component of recovery time.
package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/state"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

const (
	valueSize   = 16
	touchedKeys = 1_000 // "changed data" between incremental checkpoints
)

var sizes = []struct {
	name string
	keys int
	big  bool // skipped with -short
}{
	{"1k", 1_000, false},
	{"100k", 100_000, false},
	{"5m", 5_000_000, true},
}

func keyOf(i int) string { return fmt.Sprintf("key-%09d", i) }

func valOf(i int) []byte {
	v := make([]byte, valueSize)
	copy(v, fmt.Sprintf("v%014d", i))
	return v
}

// jsonSnapshot mirrors ReduceOperator.Snapshot: SnapshotAll + marshal.
// This is the "full serialization" checkpoint path.
func jsonSnapshot(vs state.ValueState) []byte {
	entries := vs.SnapshotAll()
	b, _ := json.Marshal(struct {
		Entries map[string][]byte `json:"entries"`
	}{entries})
	return b
}

func dirSize(root string) int64 {
	var total int64
	filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

func heapInUse() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapInuse)
}

// BenchmarkStateScale is the core backend-level matrix. Use
// -benchtime=1x: each iteration is a full load + measurement pass.
func BenchmarkStateScale(b *testing.B) {
	modes := []string{"memory", "pebble-compat", "pebble-native"}
	for _, mode := range modes {
		for _, size := range sizes {
			b.Run(mode+"/"+size.name, func(b *testing.B) {
				if size.big && testing.Short() {
					b.Skip("5m tier skipped with -short")
				}
				for i := 0; i < b.N; i++ {
					runScale(b, mode, size.keys)
				}
			})
		}
	}
}

func runScale(b *testing.B, mode string, keys int) {
	base := b.TempDir()

	newBackend := func(dir string) state.StateBackend {
		if mode == "memory" {
			return state.NewMemoryBackend()
		}
		p, err := state.OpenPebble(dir)
		if err != nil {
			b.Fatal(err)
		}
		return p
	}
	closeBackend := func(sb state.StateBackend) {
		if c, ok := sb.(interface{ Close() error }); ok {
			c.Close()
		}
	}

	heapBefore := heapInUse()

	// ---- Load K keys --------------------------------------------------------
	live := filepath.Join(base, "live")
	backend := newBackend(live)
	defer closeBackend(backend)
	vs := backend.ValueState("reduce")

	start := time.Now()
	for i := 0; i < keys; i++ {
		vs.SetKey(keyOf(i))
		vs.Set(valOf(i))
	}
	loadDur := time.Since(start)
	b.ReportMetric(float64(keys)/loadDur.Seconds(), "load-rec/s")
	b.ReportMetric((heapInUse()-heapBefore)/(1<<20), "heap-mb")
	if mode != "memory" {
		b.ReportMetric(float64(dirSize(live))/(1<<20), "disk-mb")
	}

	// ---- Lookup latency ------------------------------------------------------
	rng := rand.New(rand.NewSource(42))
	lookups := 10_000
	start = time.Now()
	for i := 0; i < lookups; i++ {
		vs.SetKey(keyOf(rng.Intn(keys)))
		if vs.Get() == nil {
			b.Fatal("lookup miss on existing key")
		}
	}
	b.ReportMetric(float64(time.Since(start).Nanoseconds())/float64(lookups), "lookup-ns")

	// ---- Full checkpoint (all K keys are "new" since the last one) ----------
	var fullArtifact []byte
	fullDir := filepath.Join(base, "ckpt-full")
	start = time.Now()
	if mode == "pebble-native" {
		if err := backend.(*state.PebbleBackend).CheckpointTo(fullDir); err != nil {
			b.Fatal(err)
		}
	} else {
		fullArtifact = jsonSnapshot(vs)
	}
	fullDur := time.Since(start)
	b.ReportMetric(float64(fullDur.Milliseconds()), "full-ckpt-ms")
	if mode == "pebble-native" {
		b.ReportMetric(float64(dirSize(fullDir))/(1<<20), "ckpt-mb")
	} else {
		b.ReportMetric(float64(len(fullArtifact))/(1<<20), "ckpt-mb")
	}

	// ---- Incremental checkpoint (only touchedKeys changed) -------------------
	touched := touchedKeys
	if touched > keys {
		touched = keys
	}
	for i := 0; i < touched; i++ {
		vs.SetKey(keyOf(i))
		vs.Set(valOf(i + 1))
	}
	incrDir := filepath.Join(base, "ckpt-incr")
	var incrArtifact []byte
	start = time.Now()
	if mode == "pebble-native" {
		if err := backend.(*state.PebbleBackend).CheckpointTo(incrDir); err != nil {
			b.Fatal(err)
		}
	} else {
		incrArtifact = jsonSnapshot(vs)
	}
	incrDur := time.Since(start)
	_ = incrArtifact
	b.ReportMetric(float64(incrDur.Microseconds())/1000.0, "incr-ckpt-ms")

	// ---- Restore (the state part of recovery time) ---------------------------
	restoreLive := filepath.Join(base, "restore-live")
	fresh := newBackend(restoreLive)
	defer closeBackend(fresh)
	start = time.Now()
	if mode == "pebble-native" {
		if err := fresh.(*state.PebbleBackend).RestoreFrom(fullDir); err != nil {
			b.Fatal(err)
		}
	} else {
		var snap struct {
			Entries map[string][]byte `json:"entries"`
		}
		if err := json.Unmarshal(fullArtifact, &snap); err != nil {
			b.Fatal(err)
		}
		if err := fresh.ValueState("reduce").RestoreAll(snap.Entries); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(time.Since(start).Milliseconds()), "restore-ms")

	// Spot-check the restore actually carried data.
	rv := fresh.ValueState("reduce")
	rv.SetKey(keyOf(keys - 1))
	if rv.Get() == nil {
		b.Fatal("restored backend missing data")
	}
}

// BenchmarkPipelineThroughput runs a real pipeline (KeyBy ×4 → Reduce →
// blackhole) over K records with K distinct keys and reports
// end-to-end records/s per backend. The write path is identical for
// pebble-compat and pebble-native, so only memory vs pebble is
// compared here.
func BenchmarkPipelineThroughput(b *testing.B) {
	backends := []struct {
		name    string
		factory func(dir string) state.BackendFactory
	}{
		{"memory", func(string) state.BackendFactory { return state.InMemory() }},
		{"pebble", func(dir string) state.BackendFactory { return state.Pebble(dir) }},
	}
	for _, bk := range backends {
		for _, size := range sizes {
			b.Run(bk.name+"/"+size.name, func(b *testing.B) {
				if size.big && testing.Short() {
					b.Skip("5m tier skipped with -short")
				}
				for i := 0; i < b.N; i++ {
					records := make([]types.Record, size.keys)
					for j := range records {
						records[j] = types.NewRecord([]byte(keyOf(j)), valOf(j))
					}

					env := mailer.NewEnv().WithStateBackend(bk.factory(b.TempDir()))
					env.FromSource(source.NewSliceSource(records)).
						KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(4).
						Reduce(func(accum []byte, curr types.Record) []byte { return curr.Value }).
						ToSink(sink.NewBlackholeSink())

					start := time.Now()
					if err := env.Execute(context.Background()); err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(size.keys)/time.Since(start).Seconds(), "rec/s")
				}
			})
		}
	}
}

// TestBenchCompiles keeps the bench package honest in normal test runs
// (a benchmark that doesn't compile helps no one).
func TestBenchCompiles(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("bench package is compile-checked only in CI")
	}
}
