// Command stream-demo is a continuously-running Weibo SDK job used to
// exercise the control-plane dashboard end to end: a live source feeds a
// real operator graph (filter → keyBy → tumbling window → reduce), results
// are printed, and the job agent exposes /state and /metrics the whole time.
//
// Unlike examples/sdk-demo (which is a short fixture), this job never
// exhausts its source, so the dashboard shows live record counters,
// throughput sparklines, source positions, and checkpoint progress while it
// runs. Stop it with cancel/savepoint from the UI or CLI.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sdk"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/watermark"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/window"
)

// order is the JSON payload emitted by the live source.
type order struct {
	Product string `json:"product"`
	Region  string `json:"region"`
	Amount  int64  `json:"amount"`
}

func main() {
	sdk.Run(func(env *weibo.StreamExecutionEnv) {
		rate := 20
		if v := os.Getenv("RECORDS_PER_SECOND"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				rate = n
			}
		}

		// A bounded-out-of-orderness watermark lets the tumbling window close
		// on event time as records flow.
		src := source.NewWatermarkSource(
			&liveOrderSource{perSecond: rate},
			watermark.NewBoundedOutOfOrderness(2*time.Second),
			500*time.Millisecond,
		)

		env.
			FromSource(src).
			// operator 1: drop invalid (non-positive) orders
			Filter(func(r types.Record) bool {
				var o order
				return json.Unmarshal(r.Value, &o) == nil && o.Amount > 0
			}, "valid-orders").
			// operator 2: partition by product so each product's state is local
			KeyBy(func(r types.Record) []byte { return r.Key }, "by-product").
			WithPartitions(4).
			// operator 3: tumbling event-time window
			Window(window.NewTumbling(10*time.Second), "per-10s").
			// operator 4: reduce to a per-window sum
			Reduce(sumAmount, "totals").
			// terminal map: emit a readable result line to the job logs
			Map(func(r types.Record) types.Record {
				fmt.Printf("product=%s total=%d window=[%s, %s)\n",
					string(r.Key), binary.BigEndian.Uint64(r.Value),
					string(r.Headers["window_start"]), string(r.Headers["window_end"]))
				return r
			}, "print").
			ToSink(sink.NewStdoutSink())
	})
}

// liveOrderSource emits synthetic orders at a steady rate until the context
// is cancelled — a stand-in for a Kafka topic so the demo runs anywhere.
type liveOrderSource struct {
	perSecond int
}

func (s *liveOrderSource) Run(ctx context.Context, out chan<- types.Record) error {
	products := []string{"widget", "gadget", "doohickey"}
	regions := []string{"us-east", "eu-west", "ap-south"}
	interval := time.Second / time.Duration(s.perSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for i := uint64(0); ; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case t := <-ticker.C:
			product := products[i%uint64(len(products))]
			// Every 9th record is intentionally invalid so the filter has work.
			amount := int64((i*37)%100) + 1
			if i%9 == 0 {
				amount = 0
			}
			payload, _ := json.Marshal(order{
				Product: product,
				Region:  regions[i%uint64(len(regions))],
				Amount:  amount,
			})
			rec := types.Record{
				Key:       []byte(product),
				Value:     payload,
				Timestamp: t,
			}
			select {
			case out <- rec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// sumAmount accumulates a big-endian uint64 sum of the order amounts in a
// window for one product key.
func sumAmount(accum []byte, curr types.Record) []byte {
	total := uint64(0)
	if accum != nil {
		total = binary.BigEndian.Uint64(accum)
	}
	var o order
	if json.Unmarshal(curr.Value, &o) == nil && o.Amount > 0 {
		total += uint64(o.Amount)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, total)
	return buf
}
