// Backpressure demo: a fast source feeding a deliberately slow sink
// through tiny edges. The bounded edges fill up, block the upstream
// stages, and throttle the source to the sink's pace — memory stays
// bounded and no record is ever dropped, with zero tuning.
//
// Run with:
//
//	go run ./examples/backpressure/
package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

const total = 5000

// slowSink simulates a slow downstream system (e.g. a rate-limited
// API or a struggling database): 200µs per record ≈ 5k records/s max.
type slowSink struct {
	count int
	start time.Time
}

func (s *slowSink) Write(ctx context.Context, in <-chan types.Record) error {
	s.start = time.Now()
	for range in {
		time.Sleep(200 * time.Microsecond)
		s.count++
	}
	elapsed := time.Since(s.start)
	fmt.Printf("sink consumed %d/%d records in %s (%.0f rec/s) — nothing dropped\n",
		s.count, total, elapsed.Round(time.Millisecond), float64(s.count)/elapsed.Seconds())
	return nil
}

func main() {
	records := make([]types.Record, total)
	for i := range records {
		records[i] = types.NewRecord([]byte(strconv.Itoa(i%64)), []byte("payload"))
	}

	// WithBufferSize(32): tiny edges so backpressure kicks in almost
	// immediately. The source can emit millions/s, but it will be
	// blocked to the sink's ~5k/s — watch mailer_edge_queue_size sit
	// at capacity and mailer_stage_send_block_seconds_total grow if
	// you scrape the Prometheus registry.
	env := mailer.NewEnv().WithBufferSize(32)

	env.
		FromSource(source.NewSliceSource(records)).
		// WithParallelism(4): fan a stateless transform across 4
		// workers. Order across workers is not preserved — fine here,
		// each record is independent.
		Map(func(r types.Record) types.Record {
			r.Value = append(r.Value, '!')
			return r
		}, "enrich").WithParallelism(4).
		ToSink(&slowSink{})

	if err := env.Execute(context.Background()); err != nil {
		fmt.Printf("pipeline error: %v\n", err)
	}
}
