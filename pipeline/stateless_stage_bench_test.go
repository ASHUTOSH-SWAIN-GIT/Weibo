package pipeline

import (
	"context"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// BenchmarkStatelessChain drives a 2-op Map→Map chain through runWorker to
// exercise the hot loop: type assertions, metric adders and output slices are
// all resolved/reused once, so the only remaining per-record allocation is
// each op's ProcessOne return slice.
func BenchmarkStatelessChain(b *testing.B) {
	identity := func(r types.Record) types.Record { return r }
	st := &StatelessStage{
		StageName:   "bench",
		Ops:         []operator.Operator{operator.Map(identity), operator.Map(identity)},
		Labels:      []string{"m1", "m2"},
		Parallelism: 1,
	}

	in := make(chan types.Record, b.N+1)
	for i := 0; i < b.N; i++ {
		in <- types.Record{Value: []byte("payload")}
	}
	close(in)
	out := make(chan types.Record, b.N+1)

	b.ReportAllocs()
	b.ResetTimer()
	if err := st.runWorker(context.Background(), in, out, nil); err != nil {
		b.Fatalf("runWorker: %v", err)
	}
}
