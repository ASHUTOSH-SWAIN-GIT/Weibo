package operator

import (
	"fmt"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func TestIntervalJoinMatchesByKeyAndTime(t *testing.T) {
	base := time.Unix(100, 0).UTC()
	op := JoinWithin("orders", "payments", 5*time.Second, func(left, right types.Record) types.Record {
		return types.Record{
			Key:       left.Key,
			Value:     []byte(fmt.Sprintf("%s=%s", left.Value, right.Value)),
			Timestamp: right.Timestamp,
		}
	})

	got := runJoin(op,
		rec("orders", "o-1", "order-1", base),
		rec("payments", "o-1", "payment-1", base.Add(3*time.Second)),
		rec("payments", "o-2", "payment-2", base.Add(3*time.Second)),
		rec("payments", "o-1", "too-late", base.Add(10*time.Second)),
	)

	if len(got.data) != 1 {
		t.Fatalf("joined records=%d, want 1: %+v", len(got.data), got.data)
	}
	if string(got.data[0].Key) != "o-1" || string(got.data[0].Value) != "order-1=payment-1" {
		t.Fatalf("join output=%q/%q", got.data[0].Key, got.data[0].Value)
	}
}

func TestIntervalJoinAlignsWatermarksAndEvictsState(t *testing.T) {
	base := time.Unix(200, 0).UTC()
	op := JoinWithin("left", "right", 5*time.Second, nil)

	got := runJoin(op,
		rec("left", "k", "old-left", base),
		wm("left", base.Add(30*time.Second)),
		wm("right", base.Add(3*time.Second)),
		rec("right", "k", "still-matches", base.Add(4*time.Second)),
		wm("right", base.Add(30*time.Second)),
		rec("right", "k", "after-evict", base.Add(4*time.Second)),
	)

	if len(got.watermarks) != 2 {
		t.Fatalf("watermarks=%d, want 2: %+v", len(got.watermarks), got.watermarks)
	}
	if !got.watermarks[0].Equal(base.Add(3 * time.Second)) {
		t.Fatalf("first aligned watermark=%s", got.watermarks[0])
	}
	if !got.watermarks[1].Equal(base.Add(30 * time.Second)) {
		t.Fatalf("second aligned watermark=%s", got.watermarks[1])
	}
	if len(got.data) != 1 {
		t.Fatalf("joined records=%d, want 1", len(got.data))
	}
}

func TestIntervalJoinSnapshotRestore(t *testing.T) {
	base := time.Unix(300, 0).UTC()
	op := JoinWithin("left", "right", time.Minute, func(left, right types.Record) types.Record {
		return types.Record{Key: left.Key, Value: []byte(string(left.Value) + "+" + string(right.Value))}
	})

	first := runJoin(op, rec("left", "k", "L", base), types.NewBarrier("cp-1"))
	if len(first.data) != 0 {
		t.Fatalf("pre-restore joined records=%d, want 0", len(first.data))
	}
	snap, err := op.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	restored := JoinWithin("left", "right", time.Minute, op.Fn)
	if err := restored.Restore(snap); err != nil {
		t.Fatal(err)
	}
	second := runJoin(restored, rec("right", "k", "R", base.Add(time.Second)))
	if len(second.data) != 1 {
		t.Fatalf("post-restore joined records=%d, want 1", len(second.data))
	}
	if string(second.data[0].Value) != "L+R" {
		t.Fatalf("post-restore output=%q", second.data[0].Value)
	}
}

type joinRunOutput struct {
	data       []types.Record
	watermarks []time.Time
}

func runJoin(op *IntervalJoinOperator, inputs ...types.Record) joinRunOutput {
	in := make(chan types.Record, len(inputs))
	out := make(chan types.Record, len(inputs)+8)
	for _, r := range inputs {
		in <- r
	}
	close(in)
	op.Process(in, out)

	var got joinRunOutput
	for r := range out {
		switch {
		case r.IsWatermark:
			got.watermarks = append(got.watermarks, r.Timestamp)
		case r.IsBarrier:
		default:
			got.data = append(got.data, r)
		}
	}
	return got
}

func rec(source, key, value string, ts time.Time) types.Record {
	return types.Record{Source: source, Key: []byte(key), Value: []byte(value), Timestamp: ts}
}

func wm(source string, ts time.Time) types.Record {
	r := types.NewWatermark(ts)
	r.Source = source
	return r
}
