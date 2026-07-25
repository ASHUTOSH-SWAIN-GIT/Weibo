package weibo_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// slowSink consumes records with a fixed per-record delay and counts them.
type slowSink struct {
	delay time.Duration
	count int64
}

func (s *slowSink) Write(ctx context.Context, in <-chan types.Record) error {
	for range in {
		if s.delay > 0 {
			time.Sleep(s.delay)
		}
		atomic.AddInt64(&s.count, 1)
	}
	return nil
}

func makeRecords(n int) []types.Record {
	recs := make([]types.Record, n)
	for i := range recs {
		recs[i] = types.NewRecord([]byte(strconv.Itoa(i%16)), []byte("v"))
	}
	return recs
}

// TestBackpressure_FastSourceSlowSink_NoDrops: with tiny edges and a
// slow sink, the source must be throttled by backpressure — and every
// record must still arrive.
func TestBackpressure_FastSourceSlowSink_NoDrops(t *testing.T) {
	const n = 2000
	sk := &slowSink{delay: 50 * time.Microsecond}

	env := weibo.NewEnv().WithBufferSize(8)
	env.FromSource(source.NewSliceSource(makeRecords(n))).
		Map(func(r types.Record) types.Record { return r }, "noop").
		Filter(func(r types.Record) bool { return true }, "keep-all").
		ToSink(sk)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if got := atomic.LoadInt64(&sk.count); got != n {
		t.Errorf("expected %d records at sink, got %d — records were dropped", n, got)
	}
}

// TestBackpressure_ParallelStatelessDelivery: WithParallelism fans a
// stateless operator across workers; every record must still arrive.
func TestBackpressure_ParallelStatelessDelivery(t *testing.T) {
	const n = 1000
	sk := &slowSink{}

	env := weibo.NewEnv().WithBufferSize(64)
	env.FromSource(source.NewSliceSource(makeRecords(n))).
		Map(func(r types.Record) types.Record { return r }, "par-map").WithParallelism(4).
		ToSink(sk)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if got := atomic.LoadInt64(&sk.count); got != n {
		t.Errorf("expected %d records at sink, got %d", n, got)
	}
}

// TestObservability_StageAndEdgeMetrics: running a pipeline must
// populate the per-stage throughput counters and per-edge capacity
// gauges introduced with stage-based execution.
func TestObservability_StageAndEdgeMetrics(t *testing.T) {
	const n = 500
	sk := &slowSink{}

	sourceInBefore := testutil.ToFloat64(metrics.StageRecordsInTotal.WithLabelValues("source", "source"))
	sinkInBefore := testutil.ToFloat64(metrics.StageRecordsInTotal.WithLabelValues("sink", "sink"))

	env := weibo.NewEnv().WithBufferSize(32)
	env.FromSource(source.NewSliceSource(makeRecords(n))).
		Map(func(r types.Record) types.Record { return r }, "noop").
		ToSink(sk)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if got := testutil.ToFloat64(metrics.StageRecordsInTotal.WithLabelValues("source", "source")) - sourceInBefore; got != n {
		t.Errorf("source stage records_in: expected %d, got %v", n, got)
	}
	if got := testutil.ToFloat64(metrics.StageRecordsInTotal.WithLabelValues("sink", "sink")) - sinkInBefore; got != n {
		t.Errorf("sink stage records_in: expected %d, got %v", n, got)
	}
	// Plan is source → stateless-0 → sink: two edges, capacity 32.
	for _, edge := range []string{"edge-0", "edge-1"} {
		if got := testutil.ToFloat64(metrics.EdgeQueueCapacity.WithLabelValues(edge)); got != 32 {
			t.Errorf("edge %s capacity gauge: expected 32, got %v", edge, got)
		}
	}
}

// TestShutdown_GracefulDrainOnCancel: cancelling the context mid-run
// must stop the source, drain in-flight records, and return promptly
// without error (graceful shutdown).
func TestShutdown_GracefulDrainOnCancel(t *testing.T) {
	sk := &slowSink{delay: time.Millisecond}

	env := weibo.NewEnv().WithBufferSize(16).WithShutdownTimeout(5 * time.Second)
	env.FromSource(source.NewSliceSource(makeRecords(10000))).
		Map(func(r types.Record) types.Record { return r }, "noop").
		ToSink(sk)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() { done <- env.Execute(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected graceful shutdown to return nil, got %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Execute did not return after cancellation")
	}
	if atomic.LoadInt64(&sk.count) == 0 {
		t.Error("expected some records processed before shutdown")
	}
}
