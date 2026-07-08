package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/dashboard"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/source"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/window"
)

func main() {
	env := mailer.NewEnv()

	src := source.NewSliceSource([]types.Record{
		{Key: []byte("a"), Value: []byte("1"), Timestamp: time.Now()},
		{Key: []byte("b"), Value: []byte("2"), Timestamp: time.Now()},
	})

	sk := sink.NewStdoutSink()

	env.
		FromSource(src).
		Map(func(r types.Record) types.Record { return r }, "parse-order").
		Filter(func(r types.Record) bool { return len(r.Key) > 0 }, "drop-invalid").
		KeyBy(func(r types.Record) []byte { return r.Key }, "by-customer").
		Window(window.NewTumbling(5*time.Minute), "5min-window").
		Reduce(func(accum []byte, curr types.Record) []byte {
			if accum == nil {
				return curr.Value
			}
			return accum
		}, "sum-amount").
		ToSink(sk)

	dash := dashboard.NewServer(env, ":18080")
	go dash.Start()

	dash.SetRunning(true)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	<-ctx.Done()
	dash.SetRunning(false)
}
