package operator_test

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/window"
)

func u64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// TestWindowReduce_OneResultPerWindow verifies that a window with a Reducer
// emits exactly ONE aggregate per (key, window) at close — not a running
// partial per record (the incremental Window+Reduce behavior). Covers #6.
func TestWindowReduce_OneResultPerWindow(t *testing.T) {
	sum := func(accum []byte, curr types.Record) []byte {
		var total uint64
		if len(accum) == 8 {
			total = binary.BigEndian.Uint64(accum)
		}
		if len(curr.Value) == 8 {
			total += binary.BigEndian.Uint64(curr.Value)
		}
		return u64(total)
	}

	op := operator.Window(window.NewTumbling(10 * time.Second))
	op.Reducer = sum

	in := make(chan types.Record, 32)
	out := make(chan types.Record, 32)
	go op.Process(in, out)

	// k1 window [0,10): 1+2+3 = 6
	in <- types.Record{Key: []byte("k1"), Value: u64(1), Timestamp: time.Unix(1, 0).UTC()}
	in <- types.Record{Key: []byte("k1"), Value: u64(2), Timestamp: time.Unix(2, 0).UTC()}
	in <- types.Record{Key: []byte("k1"), Value: u64(3), Timestamp: time.Unix(5, 0).UTC()}
	// k1 window [10,20): 10
	in <- types.Record{Key: []byte("k1"), Value: u64(10), Timestamp: time.Unix(12, 0).UTC()}
	// k2 window [0,10): 100
	in <- types.Record{Key: []byte("k2"), Value: u64(100), Timestamp: time.Unix(3, 0).UTC()}
	in <- types.NewWatermark(time.Unix(25, 0).UTC())
	close(in)

	type key struct{ k, start string }
	got := map[key]uint64{}
	n := 0
	for r := range out {
		if r.IsWatermark || r.IsBarrier {
			continue
		}
		n++
		got[key{string(r.Key), string(r.Headers["window_start"])}] = binary.BigEndian.Uint64(r.Value)
	}

	if n != 3 {
		t.Fatalf("expected exactly 3 aggregates (one per key+window), got %d", n)
	}
	want := map[key]uint64{
		{"k1", "1970-01-01T00:00:00Z"}: 6,
		{"k1", "1970-01-01T00:00:10Z"}: 10,
		{"k2", "1970-01-01T00:00:00Z"}: 100,
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("(%s, win %s): got sum %d, want %d", k.k, k.start, got[k], w)
		}
	}
}
