package source

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/watermark"
)

// scriptedSource emits a fixed list of records, then blocks until ctx ends.
type scriptedSource struct {
	records []types.Record
	// gate, if set, is called between records so a test can advance a fake clock.
	gate func(i int)
}

func (s *scriptedSource) Run(ctx context.Context, out chan<- types.Record) error {
	for i, r := range s.records {
		if s.gate != nil {
			s.gate(i)
		}
		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func rec(topic string, part int, ts time.Time) types.Record {
	return types.Record{Source: topic, Partition: part, Timestamp: ts}
}

// collectWatermarks runs ws until it has forwarded wantRecords data records,
// waits for at least one tick after that, and returns every watermark emitted.
func collectWatermarks(t *testing.T, ws *WatermarkSource, wantRecords int) []time.Time {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan types.Record, 1024)
	done := make(chan struct{})
	go func() { _ = ws.Run(ctx, out); close(done) }()

	var (
		mu  sync.Mutex
		wms []time.Time
	)
	seen := 0
	deadline := time.After(3 * time.Second)
	settled := time.Time{}
	for {
		select {
		case r := <-out:
			mu.Lock()
			if r.IsWatermark {
				if !r.Timestamp.Equal(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)) { // ignore the shutdown flush
					wms = append(wms, r.Timestamp)
				}
			} else {
				seen++
				if seen == wantRecords {
					settled = time.Now()
				}
			}
			mu.Unlock()
		case <-time.After(20 * time.Millisecond):
			// keep polling
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("timed out (saw %d/%d records)", seen, wantRecords)
		}
		mu.Lock()
		enough := seen >= wantRecords && !settled.IsZero() && time.Since(settled) > 120*time.Millisecond
		mu.Unlock()
		if enough {
			break
		}
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	return append([]time.Time(nil), wms...)
}

func maxTime(ts []time.Time) time.Time {
	var m time.Time
	for _, x := range ts {
		if x.After(m) {
			m = x
		}
	}
	return m
}

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func perPartition(src Source, outOfOrderness time.Duration) *WatermarkSource {
	return &WatermarkSource{
		Source:       src,
		Generator:    watermark.NewBoundedOutOfOrderness(outOfOrderness),
		NewGenerator: func() watermark.WatermarkGenerator { return watermark.NewBoundedOutOfOrderness(outOfOrderness) },
		Interval:     10 * time.Millisecond,
	}
}

// The bug: with one generator over all partitions, a partition that races ahead
// drags the watermark past records of slower partitions, which the window
// operator then drops as late — silently. With per-partition generators the
// watermark is the MINIMUM across partitions.
func TestWatermarkSource_PerPartitionWatermarkIsMinAcrossPartitions(t *testing.T) {
	src := &scriptedSource{records: []types.Record{
		rec("orders", 0, t0.Add(100*time.Second)), // partition 0 is far ahead
		rec("orders", 1, t0),                      // partition 1 is behind
		rec("orders", 2, t0.Add(5*time.Second)),
	}}
	wms := collectWatermarks(t, perPartition(src, 2*time.Second), 3)
	if len(wms) == 0 {
		t.Fatal("no watermarks emitted")
	}
	// Slowest partition is at t0 -> watermark must not exceed t0 - 2s.
	if got, limit := maxTime(wms), t0.Add(-2*time.Second); got.After(limit) {
		t.Errorf("watermark reached %v, want <= %v (a fast partition must not advance it past a slow one)", got, limit)
	}
}

// Same input through the legacy single-generator path: documents the old
// behaviour the per-partition mode replaces, and guards that it is unchanged
// for sources that don't opt in.
func TestWatermarkSource_LegacySingleGeneratorFollowsFastestPartition(t *testing.T) {
	src := &scriptedSource{records: []types.Record{
		rec("orders", 0, t0.Add(100*time.Second)),
		rec("orders", 1, t0),
	}}
	ws := &WatermarkSource{Source: src, Generator: watermark.NewBoundedOutOfOrderness(2 * time.Second), Interval: 10 * time.Millisecond}
	wms := collectWatermarks(t, ws, 2)
	if got, want := maxTime(wms), t0.Add(98*time.Second); !got.Equal(want) {
		t.Errorf("legacy watermark = %v, want %v", got, want)
	}
}

func TestWatermarkSource_PerPartitionAdvancesWhenAllPartitionsAdvance(t *testing.T) {
	src := &scriptedSource{records: []types.Record{
		rec("orders", 0, t0.Add(60*time.Second)),
		rec("orders", 1, t0.Add(70*time.Second)),
	}}
	wms := collectWatermarks(t, perPartition(src, 2*time.Second), 2)
	if got, want := maxTime(wms), t0.Add(58*time.Second); !got.Equal(want) {
		t.Errorf("watermark = %v, want %v (min of 58s and 68s)", got, want)
	}
}

// Partitions are identified by topic AND partition number.
func TestWatermarkSource_PerPartitionSeparatesTopics(t *testing.T) {
	src := &scriptedSource{records: []types.Record{
		rec("a", 0, t0.Add(100*time.Second)),
		rec("b", 0, t0),
	}}
	wms := collectWatermarks(t, perPartition(src, 0), 2)
	if got := maxTime(wms); got.After(t0) {
		t.Errorf("watermark = %v, want <= %v: topic b partition 0 is behind topic a partition 0", got, t0)
	}
}

// The watermark never moves backwards, even when a new (behind) partition
// shows up after the watermark has advanced.
func TestWatermarkSource_PerPartitionIsMonotonic(t *testing.T) {
	src := &scriptedSource{
		records: []types.Record{
			rec("orders", 0, t0.Add(50*time.Second)),
			rec("orders", 1, t0.Add(-40*time.Second)), // a late-joining, far-behind partition
		},
		gate: func(i int) {
			if i == 1 {
				time.Sleep(80 * time.Millisecond) // let a watermark be emitted first
			}
		},
	}
	wms := collectWatermarks(t, perPartition(src, 0), 2)
	for i := 1; i < len(wms); i++ {
		if wms[i].Before(wms[i-1]) {
			t.Fatalf("watermark went backwards: %v -> %v", wms[i-1], wms[i])
		}
	}
	if !maxTime(wms).Equal(t0.Add(50 * time.Second)) {
		t.Errorf("max watermark = %v, want %v", maxTime(wms), t0.Add(50*time.Second))
	}
}

// A partition that stops receiving data must not hold the watermark back
// forever (empty or dead partitions are common).
func TestWatermarkSource_IdlePartitionIsExcluded(t *testing.T) {
	var mu sync.Mutex
	clock := time.Unix(1_000_000, 0)
	now := func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	advance := func(d time.Duration) { mu.Lock(); clock = clock.Add(d); mu.Unlock() }

	src := &scriptedSource{
		records: []types.Record{
			rec("orders", 1, t0), // partition 1 speaks once, then goes quiet
			rec("orders", 0, t0.Add(10*time.Second)),
			rec("orders", 0, t0.Add(90*time.Second)),
		},
		gate: func(i int) {
			if i == 2 {
				// Records 0 and 1 sit in a buffered channel: let the source
				// process (and timestamp) them BEFORE the clock moves, or they
				// would all be stamped with the advanced time.
				time.Sleep(60 * time.Millisecond)
				advance(time.Minute) // > idle timeout of 30s: partition 1 is now idle
				time.Sleep(60 * time.Millisecond)
			}
		},
	}
	ws := perPartition(src, 0)
	ws.PartitionIdleTimeout = 30 * time.Second
	ws.now = now
	wms := collectWatermarks(t, ws, 3)
	if got, want := maxTime(wms), t0.Add(90*time.Second); !got.Equal(want) {
		t.Errorf("watermark = %v, want %v: the idle partition should no longer hold it back", got, want)
	}
}

func TestWatermarkSource_NegativeIdleTimeoutNeverExcludes(t *testing.T) {
	var mu sync.Mutex
	clock := time.Unix(1_000_000, 0)
	now := func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	src := &scriptedSource{
		records: []types.Record{rec("orders", 1, t0), rec("orders", 0, t0.Add(90*time.Second))},
		gate: func(i int) {
			if i == 1 {
				time.Sleep(60 * time.Millisecond) // let record 0 be timestamped first
				mu.Lock()
				clock = clock.Add(24 * time.Hour)
				mu.Unlock()
				time.Sleep(60 * time.Millisecond)
			}
		},
	}
	ws := perPartition(src, 0)
	ws.PartitionIdleTimeout = -1
	ws.now = now
	wms := collectWatermarks(t, ws, 2)
	if got := maxTime(wms); got.After(t0) {
		t.Errorf("watermark = %v, want <= %v with idleness disabled", got, t0)
	}
}
