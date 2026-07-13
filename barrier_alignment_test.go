package mailer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// alignTestOp is a Cloneable operator that records the barriers each clone
// sees and optionally delays data records to simulate slow workers.
type alignTestOp struct {
	delay time.Duration

	mu       sync.Mutex
	barriers []string       // barrier IDs seen by this clone
	clones   []*alignTestOp // populated on the prototype only
}

func (o *alignTestOp) Name() string { return "AlignTest" }

func (o *alignTestOp) Clone() operator.Operator {
	c := &alignTestOp{delay: o.delay}
	o.mu.Lock()
	o.clones = append(o.clones, c)
	o.mu.Unlock()
	return c
}

func (o *alignTestOp) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)
	for r := range in {
		if r.IsBarrier {
			o.mu.Lock()
			o.barriers = append(o.barriers, r.CheckpointID)
			o.mu.Unlock()
			out <- r
			continue
		}
		if r.IsWatermark {
			out <- r
			continue
		}
		if o.delay > 0 {
			time.Sleep(o.delay)
		}
		out <- r
	}
}

func (o *alignTestOp) seenBarriers() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.barriers...)
}

// runKeyedStage wires a keyed stage with the given partitions and prototype
// operator, feeds it the records, and returns the merged output.
func runKeyedStage(t *testing.T, partitions int, proto *alignTestOp, records []types.Record) []types.Record {
	t.Helper()

	env := NewEnv()
	kb := operator.KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(partitions)

	in := make(chan types.Record, len(records))
	for _, r := range records {
		in <- r
	}
	close(in)

	merged := env.wireKeyedStage(context.Background(), kb, []operator.Operator{proto}, in)

	var out []types.Record
	done := make(chan struct{})
	go func() {
		defer close(done)
		for r := range merged {
			out = append(out, r)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out draining keyed stage output")
	}
	return out
}

func dataRecords(n int) []types.Record {
	recs := make([]types.Record, n)
	for i := range recs {
		recs[i] = types.Record{Key: []byte{byte('a' + i)}, Value: []byte("v")}
	}
	return recs
}

func TestKeyedStage_BarrierBroadcastToAllWorkers(t *testing.T) {
	proto := &alignTestOp{}
	recs := append(dataRecords(8), types.NewBarrier("cp-1"))

	runKeyedStage(t, 4, proto, recs)

	if got := len(proto.clones); got != 4 {
		t.Fatalf("expected 4 worker clones, got %d", got)
	}
	for i, c := range proto.clones {
		if seen := c.seenBarriers(); len(seen) != 1 || seen[0] != "cp-1" {
			t.Errorf("worker %d: expected to see barrier cp-1 exactly once, saw %v", i, seen)
		}
	}
}

func TestKeyedStage_BarrierEmittedExactlyOnce(t *testing.T) {
	proto := &alignTestOp{}
	recs := append(dataRecords(8), types.NewBarrier("cp-1"))

	out := runKeyedStage(t, 4, proto, recs)

	var barriers, data int
	for _, r := range out {
		if r.IsBarrier {
			barriers++
		} else {
			data++
		}
	}
	if barriers != 1 {
		t.Errorf("expected barrier emitted exactly once downstream, got %d", barriers)
	}
	if data != 8 {
		t.Errorf("expected 8 data records downstream, got %d", data)
	}
}

func TestKeyedStage_BarrierAlignsBehindSlowWorkers(t *testing.T) {
	// Every worker delays each data record; the barrier must still come
	// out AFTER all pre-barrier data records, because the merger holds it
	// until every worker has drained its backlog and forwarded its copy.
	proto := &alignTestOp{delay: 20 * time.Millisecond}
	recs := append(dataRecords(8), types.NewBarrier("cp-1"))

	out := runKeyedStage(t, 4, proto, recs)

	barrierIdx := -1
	dataBefore := 0
	for i, r := range out {
		if r.IsBarrier {
			barrierIdx = i
			break
		}
		dataBefore++
	}
	if barrierIdx == -1 {
		t.Fatal("barrier never emitted downstream")
	}
	if dataBefore != 8 {
		t.Errorf("barrier emitted after %d of 8 pre-barrier records — checkpoint would be inconsistent", dataBefore)
	}
}

func TestKeyedStage_MultipleBarriersOrderedAndDeduped(t *testing.T) {
	proto := &alignTestOp{}
	recs := append(dataRecords(4), types.NewBarrier("cp-1"))
	recs = append(recs, dataRecords(4)...)
	recs = append(recs, types.NewBarrier("cp-2"))

	out := runKeyedStage(t, 4, proto, recs)

	var ids []string
	for _, r := range out {
		if r.IsBarrier {
			ids = append(ids, r.CheckpointID)
		}
	}
	if len(ids) != 2 || ids[0] != "cp-1" || ids[1] != "cp-2" {
		t.Errorf("expected barriers [cp-1 cp-2] exactly once each in order, got %v", ids)
	}
}

func TestKeyedStage_WatermarkEmittedExactlyOnce(t *testing.T) {
	proto := &alignTestOp{}
	recs := append(dataRecords(8), types.NewWatermark(time.Unix(100, 0)))

	out := runKeyedStage(t, 4, proto, recs)

	var watermarks int
	for _, r := range out {
		if r.IsWatermark {
			watermarks++
		}
	}
	if watermarks != 1 {
		t.Errorf("expected watermark emitted exactly once downstream, got %d", watermarks)
	}
}

func TestKeyedStage_SinglePartitionStillWorks(t *testing.T) {
	proto := &alignTestOp{}
	recs := append(dataRecords(4), types.NewBarrier("cp-1"))

	out := runKeyedStage(t, 1, proto, recs)

	var barriers int
	for _, r := range out {
		if r.IsBarrier {
			barriers++
		}
	}
	if barriers != 1 {
		t.Errorf("expected 1 barrier with a single partition, got %d", barriers)
	}
}
