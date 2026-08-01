package operator

import (
	"hash/fnv"
	"strconv"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// routeKeyReference is the pre-optimization implementation: a fresh
// fnv.New32a hasher per call. RouteKey must match it exactly, or keys would
// re-route to different workers and break state locality across restarts.
func routeKeyReference(key []byte, partitions int) int {
	if len(key) == 0 || partitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % partitions
}

func TestRouteKey_MatchesFNVReference(t *testing.T) {
	for _, partitions := range []int{1, 2, 3, 16, 17, 64} {
		op := &KeyByOperator{Partitions: partitions}
		for i := 0; i < 5000; i++ {
			key := []byte("customer-" + strconv.Itoa(i*7))
			if got, want := op.RouteKey(key), routeKeyReference(key, partitions); got != want {
				t.Fatalf("partitions=%d key=%q: RouteKey=%d, fnv reference=%d", partitions, key, got, want)
			}
		}
		// Edge cases: empty key and single-partition both pin to worker 0.
		if op.RouteKey(nil) != 0 {
			t.Errorf("partitions=%d: empty key must route to 0", partitions)
		}
	}
}

func BenchmarkRouteKey(b *testing.B) {
	op := &KeyByOperator{Partitions: 16}
	key := []byte("customer-4815162342")
	b.ReportAllocs()
	b.ResetTimer()
	var sink int
	for i := 0; i < b.N; i++ {
		sink = op.RouteKey(key)
	}
	_ = sink
}

// SelectKey + RouteKey together model one router dispatch decision.
func BenchmarkSelectAndRoute(b *testing.B) {
	op := &KeyByOperator{
		Partitions:  16,
		KeySelector: func(r types.Record) []byte { return r.Key },
	}
	r := types.Record{Key: []byte("customer-4815162342")}
	b.ReportAllocs()
	b.ResetTimer()
	var sink int
	for i := 0; i < b.N; i++ {
		sink = op.RouteKey(op.SelectKey(r))
	}
	_ = sink
}
