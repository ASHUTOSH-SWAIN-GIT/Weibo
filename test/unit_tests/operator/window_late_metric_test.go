package operator_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/metrics"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/window"
)

type lateCollector struct {
	mu   sync.Mutex
	recs []types.Record
}

func (c *lateCollector) Write(_ context.Context, r types.Record) error {
	c.mu.Lock()
	c.recs = append(c.recs, r)
	c.mu.Unlock()
	return nil
}

func runWindowWith(op *operator.WindowOperator, in ...types.Record) []types.Record {
	inCh := make(chan types.Record, len(in)+1)
	out := make(chan types.Record, len(in)+8)
	for _, r := range in {
		inCh <- r
	}
	close(inCh)
	go op.Process(inCh, out)
	var got []types.Record
	for r := range out {
		got = append(got, r)
	}
	return got
}

// A record behind the watermark used to vanish with no error, log or metric,
// which is how a watermark racing ahead of a slow partition lost data unseen.
func TestWindow_DroppedLateRecordsAreCounted(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	dropped := metrics.WindowLateRecordsTotal.WithLabelValues("dropped")
	before := testutil.ToFloat64(dropped)

	late := types.NewRecord([]byte("k"), []byte("v"))
	late.Timestamp = base // watermark below is 10 minutes ahead
	fresh := types.NewRecord([]byte("k"), []byte("v"))
	fresh.Timestamp = base.Add(11 * time.Minute)

	runWindowWith(operator.Window(window.NewTumbling(time.Minute)),
		types.NewWatermark(base.Add(10*time.Minute)), late, fresh, late)

	if got := testutil.ToFloat64(dropped) - before; got != 2 {
		t.Errorf("dropped late records counted = %v, want 2 (the fresh record must not count)", got)
	}
}

func TestWindow_SideOutputLateRecordsAreCountedSeparately(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	dropped := metrics.WindowLateRecordsTotal.WithLabelValues("dropped")
	side := metrics.WindowLateRecordsTotal.WithLabelValues("side_output")
	dBefore, sBefore := testutil.ToFloat64(dropped), testutil.ToFloat64(side)

	sink := &lateCollector{}
	late := types.NewRecord([]byte("k"), []byte("v"))
	late.Timestamp = base
	runWindowWith(operator.Window(window.NewTumbling(time.Minute)).WithLateSink(sink),
		types.NewWatermark(base.Add(10*time.Minute)), late)

	if got := testutil.ToFloat64(side) - sBefore; got != 1 {
		t.Errorf("side_output = %v, want 1", got)
	}
	if got := testutil.ToFloat64(dropped) - dBefore; got != 0 {
		t.Errorf("dropped = %v, want 0: a record captured by a LateSink is not lost", got)
	}
	if len(sink.recs) != 1 {
		t.Errorf("late sink got %d records, want 1", len(sink.recs))
	}
}
