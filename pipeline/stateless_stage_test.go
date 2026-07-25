package pipeline_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/pipeline"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func runStatelessStage(t *testing.T, stage *pipeline.StatelessStage, records []types.Record) []types.Record {
	t.Helper()

	in := make(chan types.Record, len(records))
	for _, r := range records {
		in <- r
	}
	close(in)
	out := make(chan types.Record, len(records)*4+16)

	errc := make(chan error, 1)
	go func() { errc <- stage.Run(context.Background(), context.Background(), in, out) }()

	var result []types.Record
	timeout := time.After(10 * time.Second)
	for {
		select {
		case r, ok := <-out:
			if !ok {
				if err := <-errc; err != nil {
					t.Fatalf("stage.Run returned error: %v", err)
				}
				return result
			}
			result = append(result, r)
		case <-timeout:
			t.Fatal("timed out draining stateless stage output")
		}
	}
}

func TestStatelessStage_ChainsOperatorsDirectly(t *testing.T) {
	stage := &pipeline.StatelessStage{
		StageName: "test",
		Ops: []operator.Operator{
			operator.Map(func(r types.Record) types.Record {
				r.Value = append(r.Value, '!')
				return r
			}),
			operator.Filter(func(r types.Record) bool { return string(r.Key) != "drop" }),
			operator.FlatMap(func(r types.Record) []types.Record { return []types.Record{r, r} }),
		},
		Labels:      []string{"map", "filter", "flatmap"},
		Parallelism: 1,
	}

	recs := []types.Record{
		{Key: []byte("keep"), Value: []byte("v")},
		{Key: []byte("drop"), Value: []byte("v")},
	}
	out := runStatelessStage(t, stage, recs)

	// "keep" passes filter and is duplicated by flatmap; "drop" vanishes.
	if len(out) != 2 {
		t.Fatalf("expected 2 records (keep duplicated, drop filtered), got %d", len(out))
	}
	for _, r := range out {
		if string(r.Key) != "keep" || string(r.Value) != "v!" {
			t.Errorf("unexpected record key=%s value=%s", r.Key, r.Value)
		}
	}
}

func TestStatelessStage_FilterDropEmitsNothing(t *testing.T) {
	stage := &pipeline.StatelessStage{
		StageName:   "test",
		Ops:         []operator.Operator{operator.Filter(func(r types.Record) bool { return false })},
		Labels:      []string{"filter"},
		Parallelism: 1,
	}
	out := runStatelessStage(t, stage, dataRecords(5))
	if len(out) != 0 {
		t.Errorf("expected all records dropped, got %d", len(out))
	}
}

func TestStatelessStage_MarkersForwardedInOrderWhenSerial(t *testing.T) {
	stage := &pipeline.StatelessStage{
		StageName:   "test",
		Ops:         []operator.Operator{operator.Map(func(r types.Record) types.Record { return r })},
		Labels:      []string{"map"},
		Parallelism: 1,
	}
	recs := []types.Record{
		{Key: []byte("a"), Value: []byte("1")},
		types.NewBarrier("cp-1"),
		{Key: []byte("b"), Value: []byte("2")},
	}
	out := runStatelessStage(t, stage, recs)
	if len(out) != 3 || !out[1].IsBarrier {
		t.Fatalf("expected [data barrier data] preserved in order, got %d records", len(out))
	}
}

func TestStatelessStage_ParallelProcessesAllRecords(t *testing.T) {
	var processed int64
	stage := &pipeline.StatelessStage{
		StageName: "test",
		Ops: []operator.Operator{operator.Map(func(r types.Record) types.Record {
			atomic.AddInt64(&processed, 1)
			return r
		})},
		Labels:      []string{"map"},
		Parallelism: 4,
	}

	n := 200
	recs := make([]types.Record, n)
	for i := range recs {
		recs[i] = types.Record{Key: []byte(strconv.Itoa(i)), Value: []byte("v")}
	}
	out := runStatelessStage(t, stage, recs)

	var data int
	for _, r := range out {
		if !r.IsBarrier && !r.IsWatermark {
			data++
		}
	}
	if data != n {
		t.Errorf("expected %d records out, got %d", n, data)
	}
	if atomic.LoadInt64(&processed) != int64(n) {
		t.Errorf("expected %d records processed, got %d", n, processed)
	}
}

func TestStatelessStage_ParallelBarrierAlignment(t *testing.T) {
	// Slow map so all 4 workers have backlogs when the barrier arrives.
	stage := &pipeline.StatelessStage{
		StageName: "test",
		Ops: []operator.Operator{operator.Map(func(r types.Record) types.Record {
			time.Sleep(5 * time.Millisecond)
			return r
		})},
		Labels:      []string{"map"},
		Parallelism: 4,
	}

	n := 40
	recs := make([]types.Record, 0, n+1)
	for i := 0; i < n; i++ {
		recs = append(recs, types.Record{Key: []byte(strconv.Itoa(i)), Value: []byte("v")})
	}
	recs = append(recs, types.NewBarrier("cp-1"))

	out := runStatelessStage(t, stage, recs)

	barriers := 0
	dataBefore := 0
	for _, r := range out {
		if r.IsBarrier {
			barriers++
			continue
		}
		if barriers == 0 {
			dataBefore++
		}
	}
	if barriers != 1 {
		t.Fatalf("expected barrier emitted exactly once, got %d", barriers)
	}
	if dataBefore != n {
		t.Errorf("barrier emitted after %d of %d pre-barrier records — alignment broken", dataBefore, n)
	}
}

func TestStatelessStage_ParallelWatermarkEmittedOnce(t *testing.T) {
	stage := &pipeline.StatelessStage{
		StageName:   "test",
		Ops:         []operator.Operator{operator.Map(func(r types.Record) types.Record { return r })},
		Labels:      []string{"map"},
		Parallelism: 4,
	}
	recs := append(dataRecords(20), types.NewWatermark(time.Unix(50, 0)))
	out := runStatelessStage(t, stage, recs)

	watermarks := 0
	for _, r := range out {
		if r.IsWatermark {
			watermarks++
		}
	}
	if watermarks != 1 {
		t.Errorf("expected watermark emitted exactly once, got %d", watermarks)
	}
}

func TestStatelessStage_OperatorPanicReturnsError(t *testing.T) {
	stage := &pipeline.StatelessStage{
		StageName:   "test",
		Ops:         []operator.Operator{operator.Map(func(r types.Record) types.Record { panic("boom") })},
		Labels:      []string{"map"},
		Parallelism: 2,
	}

	in := make(chan types.Record, 4)
	in <- types.Record{Key: []byte("a")}
	in <- types.Record{Key: []byte("b")}
	close(in)
	out := make(chan types.Record, 16)

	errc := make(chan error, 1)
	go func() { errc <- stage.Run(context.Background(), context.Background(), in, out) }()
	for range out {
	}

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("expected error from panicking operator, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stage.Run did not return after operator panic")
	}
}
