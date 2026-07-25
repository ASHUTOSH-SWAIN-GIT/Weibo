package operator_test

import (
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/window"
)

func TestKeyBy_PassesThroughBarrier(t *testing.T) {
	kb := operator.KeyBy(func(r types.Record) []byte { return r.Key })

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go kb.Process(in, out)

	in <- types.Record{Key: []byte("a"), Value: []byte("data1")}
	in <- types.NewBarrier("cp-1")
	in <- types.Record{Key: []byte("b"), Value: []byte("data2")}
	close(in)

	var results []types.Record
	for r := range out {
		results = append(results, r)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 records (2 data + 1 barrier), got %d", len(results))
	}
	if !results[1].IsBarrier {
		t.Error("expected barrier in original position")
	}
	if results[1].CheckpointID != "cp-1" {
		t.Errorf("barrier ID: got %q, want cp-1", results[1].CheckpointID)
	}
}

func TestReduce_PassesThroughBarrier(t *testing.T) {
	countFn := func(accum []byte, curr types.Record) []byte {
		return []byte("x")
	}
	op := operator.Reduce(countFn)

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("a")}
	<-out // drain reduce output

	in <- types.NewBarrier("cp-1")
	close(in)

	var gotBarrier bool
	for r := range out {
		if r.IsBarrier {
			gotBarrier = true
			if r.CheckpointID != "cp-1" {
				t.Errorf("barrier ID: got %q, want %q", r.CheckpointID, "cp-1")
			}
		}
	}

	if !gotBarrier {
		t.Error("expected barrier to pass through Reduce")
	}
}

func TestWindow_DropsBarrier(t *testing.T) {
	// Window operator should forward barriers without processing them.
	op := operator.Window(window.NewTumbling(5 * time.Second))

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	in <- types.NewBarrier("cp-1")
	close(in)

	gotBarrier := false
	for r := range out {
		if r.IsBarrier {
			gotBarrier = true
		}
	}

	// Window should forward barriers.
	if !gotBarrier {
		t.Error("expected barrier to pass through Window operator")
	}
}
