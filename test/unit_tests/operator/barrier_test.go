package operator_test

import (
	"testing"
	"time"

	"mailer/operator"
	"mailer/types"
	"mailer/window"
)

func TestKeyBy_BarrierHoldAndForward(t *testing.T) {
	op := operator.KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(2)

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	// Send data, then a barrier, then more data.
	in <- types.Record{Key: []byte("a"), Value: []byte("data1")}
	in <- types.NewBarrier("cp-1")
	in <- types.Record{Key: []byte("b"), Value: []byte("data2")}
	close(in)

	var results []types.Record
	for r := range out {
		results = append(results, r)
	}

	// Barrier should come after all data records (held back until input closes).
	var dataBeforeBarrier int
	var dataAfterBarrier int
	var barrierCount int
	var barrierIdx int

	for i, r := range results {
		if r.IsBarrier {
			barrierCount++
			barrierIdx = i
		}
	}

	for _, r := range results {
		if !r.IsBarrier && !r.IsWatermark {
			dataBeforeBarrier++
		}
	}

	// Fix: count data before and after the barrier.
	dataBeforeBarrier = 0
	dataAfterBarrier = 0
	for i, r := range results {
		if !r.IsBarrier && !r.IsWatermark {
			if i < barrierIdx {
				dataBeforeBarrier++
			} else {
				dataAfterBarrier++
			}
		}
	}

	if barrierCount != 1 {
		t.Errorf("expected 1 barrier, got %d", barrierCount)
	}
	// KeyBy holds barriers until all data drains, so the barrier should come
	// after all data records.
	if dataAfterBarrier != 0 {
		t.Errorf("all data should come before the barrier, but %d records after barrier", dataAfterBarrier)
	}
	totalData := dataBeforeBarrier + dataAfterBarrier
	if totalData != 2 {
		t.Errorf("expected 2 data records, got %d", totalData)
	}
}

func TestKeyBy_MultipleBarriers(t *testing.T) {
	op := operator.KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(2)

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("a"), Value: []byte("d1")}
	in <- types.NewBarrier("cp-1")
	in <- types.Record{Key: []byte("b"), Value: []byte("d2")}
	in <- types.NewBarrier("cp-2")
	close(in)

	var results []types.Record
	for r := range out {
		results = append(results, r)
	}

	// Both barriers should be forwarded in order after all data.
	var barriers []string
	for _, r := range results {
		if r.IsBarrier {
			barriers = append(barriers, r.CheckpointID)
		}
	}

	if len(barriers) != 2 {
		t.Fatalf("expected 2 barriers, got %d", len(barriers))
	}
	if barriers[0] != "cp-1" {
		t.Errorf("first barrier: got %q, want %q", barriers[0], "cp-1")
	}
	if barriers[1] != "cp-2" {
		t.Errorf("second barrier: got %q, want %q", barriers[1], "cp-2")
	}

	// All data should come before the first barrier.
	dataBeforeFirstBarrier := 0
	for _, r := range results {
		if !r.IsBarrier && !r.IsWatermark {
			dataBeforeFirstBarrier++
		}
	}
	if dataBeforeFirstBarrier != 2 {
		t.Errorf("expected 2 data records before barriers, got %d", dataBeforeFirstBarrier)
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
