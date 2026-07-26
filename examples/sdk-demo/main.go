// Command sdk-demo is a self-contained Weibo SDK job used to test the
// deploy → build → push → run flow. It depends on the released Weibo SDK
// (see go.mod), so building it exercises exactly what a real customer job
// does. The pipeline is a word count over an in-memory source (no Kafka),
// so it runs anywhere and prints its results to the job logs.
package main

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sdk"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func main() {
	// sdk.Run wires the pipeline, then serves the jobagent control surface
	// (/healthz, /state, /metrics) the controller talks to, and runs to
	// completion — the same lifecycle as a YAML runner job.
	sdk.Run(func(env *weibo.StreamExecutionEnv) {
		sentences := []string{
			"hello world",
			"hello weibo",
			"world of stream processing",
			"hello stream processing",
		}
		keys := make([]string, len(sentences))
		for i := range keys {
			keys[i] = "sentence"
		}

		env.
			FromSource(source.FromSlices(keys, sentences)).
			FlatMap(func(r types.Record) []types.Record {
				out := make([]types.Record, 0)
				for _, w := range strings.Fields(string(r.Value)) {
					out = append(out, types.NewRecord([]byte(w), []byte(w)))
				}
				return out
			}).
			KeyBy(func(r types.Record) []byte { return r.Value }).
			Reduce(operator.ReduceFn(countWords)).
			Map(func(r types.Record) types.Record {
				fmt.Printf("word=%s count=%d\n", string(r.Key), binary.BigEndian.Uint64(r.Value))
				return r
			}).
			ToSink(sink.NewBlackholeSink())
	})
}

// countWords accumulates an 8-byte big-endian count per word (key).
func countWords(accum []byte, _ types.Record) []byte {
	n := uint64(0)
	if accum != nil {
		n = binary.BigEndian.Uint64(accum)
	}
	n++
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}
